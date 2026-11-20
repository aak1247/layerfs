//! One Store-authenticated catalog binding prepared under its original lease.
use super::{
    bind::checked_base,
    conflict_paths::{ConflictDecoder, ConflictDescription},
    BindError, BindPhase, BindRefusal, BindRequest, BindSuccess, BoundWorkspace, Store,
};
use crate::{Owner, OwnerClient, OwnerError, ServiceClass};
use layerfs_content::ObjectId;
use layerfs_history::{BranchSnapshot, HistoryError};
use layerfs_overlay::{
    OrphanRecoveryRecord, Overlay, OverlayCatalogRecovery, ProfileConfig, ReadyRecord,
    SettledConflict,
};
use layerfs_workspace::Workspace;
use std::{
    fmt,
    path::Path,
    sync::{atomic::AtomicBool, Arc, Mutex},
};

/// A refusal retains a successfully opened local lease for its caller.
#[derive(Debug)]
pub enum CatalogPreparationFailure {
    Open(OwnerError),
    Retained {
        cause: BindError,
        custody: RetainedCatalogRecovery,
    },
}

/// A rejected local observation retains its lease without exposing a route.
pub struct RetainedCatalogRecovery {
    _pending: Box<OverlayCatalogRecovery>,
}
impl fmt::Debug for RetainedCatalogRecovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original catalog recovery retained")
    }
}

/// A catalog recheck refusal retains its prepared lease without activation.
pub struct RetainedCatalogPreparation {
    _prepared: Box<PreparedCatalogRecovery>,
}
impl fmt::Debug for RetainedCatalogPreparation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original catalog preparation retained")
    }
}

/// A failed namespace/bind step retains its exact unadmitted database.
pub struct CatalogActivationFailure {
    pub cause: BindError,
    _pending: Option<Box<OverlayCatalogRecovery>>,
    _opened: Option<Box<Overlay>>,
}
impl fmt::Debug for CatalogActivationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogActivationFailure")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}

enum LocalRecord {
    Ready(ReadyRecord),
    Conflict(SettledConflict),
    Checkpoint(Box<super::checkpoint_recovery::PreparedCheckpoint>),
    Orphans(OrphanRecoveryRecord, u64),
}

/// Private fields can only be produced from the actual Store and the original
/// leased Overlay. No route exists while this preparation is held.
pub struct PreparedCatalogRecovery {
    store: Arc<Store>,
    request: BindRequest,
    pending: OverlayCatalogRecovery,
    record: LocalRecord,
    snapshot: BranchSnapshot,
    conflict: Option<Arc<super::commit_merge::PendingProjection>>,
}
impl fmt::Debug for PreparedCatalogRecovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedCatalogRecovery")
            .field("request", &self.request)
            .finish_non_exhaustive()
    }
}

/// A known namespace and the exact validated projection for the one bind.
pub struct RecoveredCatalogBinding {
    store: Arc<Store>,
    owner: OwnerClient,
    request: BindRequest,
    snapshot: BranchSnapshot,
    namespace: i64,
    conflict: Option<(SettledConflict, Arc<super::commit_merge::PendingProjection>)>,
}
impl RecoveredCatalogBinding {
    pub fn snapshot(&self) -> &BranchSnapshot {
        &self.snapshot
    }
}

fn identity_error() -> BindError {
    BindError::History(HistoryError::Integrity("catalog recovery identity"))
}

impl Store {
    /// Prepare one recorded catalog entry on the control reader while retaining
    /// the original Overlay lease. The trusted caller fences old consumers.
    pub fn prepare_catalog_recovery(
        self: &Arc<Self>,
        path: &Path,
        profile: ProfileConfig,
        request: BindRequest,
    ) -> Result<PreparedCatalogRecovery, Box<CatalogPreparationFailure>> {
        let mut pending = OverlayCatalogRecovery::open(path, profile, request.workspace.to_bytes())
            .map_err(|error| {
                Box::new(CatalogPreparationFailure::Open(OwnerError::Overlay(error)))
            })?;
        let prepared = if let OverlayCatalogRecovery::PendingCheckpoint(value) = &mut pending {
            self.prepare_checkpoint_recovery(value, &request)
                .map(|plan| {
                    let snapshot = plan.snapshot.clone();
                    (LocalRecord::Checkpoint(Box::new(plan)), snapshot, None)
                })
        } else if let OverlayCatalogRecovery::PendingOrphans(value) = &mut pending {
            self.prepare_orphan_recovery(value, &request)
                .map(|(record, snapshot, serial)| {
                    (LocalRecord::Orphans(record, serial), snapshot, None)
                })
        } else {
            (|| -> Result<(LocalRecord,BranchSnapshot,Option<Arc<super::commit_merge::PendingProjection>>),BindError> {
            let (bytes,root,record)=match &pending {
                OverlayCatalogRecovery::Ready(value)=>{
                    let observed=value.observed().clone();
                    (observed.snapshot.clone(),observed.root,LocalRecord::Ready(observed))
                }
                OverlayCatalogRecovery::SettledConflict(value)=>{
                    let observed=value.observed().clone();
                    if !self.has_durable_writer() || observed.catalog!=self.history().catalog_id().to_bytes()
                        || observed.incarnation!=self.history().incarnation() {
                        return Err(identity_error());
                    }
                    self.history().require_checkpoint_receipts().map_err(BindError::History)?;
                    (observed.target_snapshot.clone(),observed.marker_root,LocalRecord::Conflict(observed))
                }
                OverlayCatalogRecovery::PendingCheckpoint(_) => unreachable!("checkpoint prepared separately"),
                OverlayCatalogRecovery::PendingOrphans(_) => unreachable!("orphans prepared separately"),
            };
            let snapshot=layerfs_bridge::snapshot::decode(&bytes).map_err(|e|BindError::Owner(OwnerError::BindingCodec(e)))?;
            if snapshot.branch.id!=request.branch { return Err(identity_error()); }
            self.published(|history|super::durable_bind::validate_snapshot(history,&snapshot)).map_err(BindError::Read)?;
            let ports=self.ports(snapshot.scope);
            let history_base=checked_base(ports.client(),&snapshot).map_err(|error|BindError::Content { error,provider:ports.failure().unwrap_or_else(|e|Some(Arc::new(e))) })?;
            let conflict=match (&record,&pending) {
                (LocalRecord::Ready(_),_)=>{
                    if root!=snapshot.effective_root.to_bytes() { return Err(identity_error()); }
                    None
                }
                (LocalRecord::Conflict(observed),OverlayCatalogRecovery::SettledConflict(value))=>{
                    if observed.workspace!=request.workspace.to_bytes() { return Err(identity_error()); }
                    let marker=ObjectId::from_bytes(&root).map_err(|error|BindError::Content{error,provider:None})?;
                    let mut marker_snapshot=snapshot.clone(); marker_snapshot.effective_root=marker;
                    let marker_base=checked_base(ports.client(),&marker_snapshot).map_err(|error|BindError::Content {error,provider:ports.failure().unwrap_or_else(|e|Some(Arc::new(e)))})?;
                    if marker_base.root().root_inode()!=history_base.root().root_inode() { return Err(identity_error()); }
                    let mut decoder=ConflictDecoder::new(ConflictDescription {paths:observed.paths_count,bytes:observed.byte_count,digest:observed.final_digest})
                        .map_err(|message|BindError::History(HistoryError::Integrity(message)))?;
                    for number in 0..observed.page_count {
                        let bytes=value.page(number).map_err(|e|BindError::Owner(OwnerError::Overlay(e)))?;
                        decoder.push(number,&bytes).map_err(|message|BindError::History(HistoryError::Integrity(message)))?;
                    }
                    let conflicted=decoder.finish().map_err(|message|BindError::History(HistoryError::Integrity(message)))?;
                    Some(Arc::new(super::commit_merge::PendingProjection {
                        merge: super::commit_merge::PendingMerge { local_root:marker, conflicted },
                        descriptor: Some(observed.descriptor),
                    }))
                }
                _=>return Err(identity_error()),
            };
            Ok((record,snapshot,conflict))
        })()
        };
        match prepared {
            Ok((record, snapshot, conflict)) => Ok(PreparedCatalogRecovery {
                store: self.clone(),
                request,
                pending,
                record,
                snapshot,
                conflict,
            }),
            Err(cause) => Err(Box::new(CatalogPreparationFailure::Retained {
                cause,
                custody: RetainedCatalogRecovery {
                    _pending: Box::new(pending),
                },
            })),
        }
    }
}

impl PreparedCatalogRecovery {
    /// Retain a preparation whose original catalog recheck failed, without
    /// permitting that rejected preparation to be activated later.
    pub fn retain(self) -> RetainedCatalogPreparation {
        RetainedCatalogPreparation {
            _prepared: Box::new(self),
        }
    }
    /// Consumes the locally checked lease once, after the caller rechecks its
    /// original catalog. Namespace publication follows the Store validation.
    pub fn activate(
        self,
        minimum: i64,
    ) -> Result<(Owner, RecoveredCatalogBinding), Box<CatalogActivationFailure>> {
        let Self {
            store,
            request,
            pending,
            record,
            mut snapshot,
            conflict,
        } = self;
        let previous = match &record {
            LocalRecord::Ready(value) => value.previous_namespace,
            LocalRecord::Conflict(value) => value.previous_namespace,
            LocalRecord::Checkpoint(value) => value.previous_namespace(),
            LocalRecord::Orphans(value, _) => value.previous_namespace,
        };
        let namespace = match previous.checked_add(1) {
            Some(value) if minimum > 0 && value.max(minimum) < i64::MAX => value.max(minimum),
            _ => {
                return Err(Box::new(CatalogActivationFailure {
                    cause: BindError::Owner(OwnerError::IdentityExhausted),
                    _pending: Some(Box::new(pending)),
                    _opened: None,
                }))
            }
        };
        let incarnation = match crate::overlay::durable_owner::catalog_incarnation(
            namespace,
            request.workspace.to_bytes(),
        ) {
            Ok(value) => value,
            Err(cause) => {
                return Err(Box::new(CatalogActivationFailure {
                    cause: BindError::Owner(cause),
                    _pending: Some(Box::new(pending)),
                    _opened: None,
                }))
            }
        };
        let db = match (pending, &record) {
            (OverlayCatalogRecovery::Ready(value), LocalRecord::Ready(expected)) => {
                match value.complete(expected) {
                    Ok(db) => db,
                    Err((error, value)) => {
                        return Err(Box::new(CatalogActivationFailure {
                            cause: BindError::Owner(OwnerError::Overlay(error)),
                            _pending: Some(Box::new(OverlayCatalogRecovery::Ready(value))),
                            _opened: None,
                        }))
                    }
                }
            }
            (OverlayCatalogRecovery::SettledConflict(value), LocalRecord::Conflict(expected)) => {
                match value.complete_settled_active(expected, previous) {
                    Ok(db) => db,
                    Err((error, value)) => {
                        return Err(Box::new(CatalogActivationFailure {
                            cause: BindError::Owner(OwnerError::Overlay(error)),
                            _pending: Some(Box::new(OverlayCatalogRecovery::SettledConflict(
                                value,
                            ))),
                            _opened: None,
                        }))
                    }
                }
            }
            (
                OverlayCatalogRecovery::PendingCheckpoint(mut value),
                LocalRecord::Checkpoint(plan),
            ) => {
                let settled = match store.settle_checkpoint_recovery(&mut value, plan) {
                    Ok(settled) => settled,
                    Err(cause) => {
                        return Err(Box::new(CatalogActivationFailure {
                            cause,
                            _pending: Some(Box::new(OverlayCatalogRecovery::PendingCheckpoint(
                                value,
                            ))),
                            _opened: None,
                        }))
                    }
                };
                let installed = settled
                    .installed
                    .as_ref()
                    .map(|(bytes, outcome)| (bytes.as_slice(), *outcome));
                match value.complete_settled(plan.request(), installed) {
                    Ok(db) => {
                        snapshot = settled.snapshot;
                        db
                    }
                    Err((error, value)) => {
                        return Err(Box::new(CatalogActivationFailure {
                            cause: BindError::Owner(OwnerError::Overlay(error)),
                            _pending: Some(Box::new(OverlayCatalogRecovery::PendingCheckpoint(
                                value,
                            ))),
                            _opened: None,
                        }))
                    }
                }
            }
            (
                OverlayCatalogRecovery::PendingOrphans(mut value),
                LocalRecord::Orphans(expected, root_serial),
            ) => {
                if let Err(cause) = store.settle_orphan_recovery(&mut value, *root_serial) {
                    return Err(Box::new(CatalogActivationFailure {
                        cause,
                        _pending: Some(Box::new(OverlayCatalogRecovery::PendingOrphans(value))),
                        _opened: None,
                    }));
                }
                match value.complete(expected) {
                    Ok(db) => db,
                    Err((error, value)) => {
                        return Err(Box::new(CatalogActivationFailure {
                            cause: BindError::Owner(OwnerError::Overlay(error)),
                            _pending: Some(Box::new(OverlayCatalogRecovery::PendingOrphans(value))),
                            _opened: None,
                        }))
                    }
                }
            }
            (pending, _) => {
                return Err(Box::new(CatalogActivationFailure {
                    cause: BindError::Owner(OwnerError::InvalidAdmission),
                    _pending: Some(Box::new(pending)),
                    _opened: None,
                }))
            }
        };
        if let Err(error) = db.advance_durable_namespace(namespace) {
            return Err(Box::new(CatalogActivationFailure {
                cause: BindError::Owner(OwnerError::Overlay(error)),
                _pending: None,
                _opened: Some(Box::new(db)),
            }));
        }
        let owner = match crate::overlay::durable_owner::bind_catalog_overlay(
            db,
            namespace,
            request.workspace.to_bytes(),
            incarnation,
        ) {
            Ok(owner) => owner,
            Err((cause, db)) => {
                return Err(Box::new(CatalogActivationFailure {
                    cause: BindError::Owner(cause),
                    _pending: None,
                    _opened: Some(Box::new(db)),
                }))
            }
        };
        let conflict = match (record, conflict) {
            (LocalRecord::Ready(_), None) => None,
            (LocalRecord::Conflict(record), Some(value)) => Some((record, value)),
            (LocalRecord::Checkpoint(_), None) => None,
            (LocalRecord::Orphans(_, _), None) => None,
            _ => unreachable!("prepared recovery projection"),
        };
        let exact_owner = owner.client();
        Ok((
            owner,
            RecoveredCatalogBinding {
                store,
                owner: exact_owner,
                request,
                snapshot,
                namespace,
                conflict,
            },
        ))
    }
}

impl Store {
    /// Consumes the one Store-verified recovery projection with its adopted
    /// owner. A normal binding keeps the original bind path unchanged.
    #[allow(clippy::result_large_err)]
    pub fn bind_catalog_recovery(
        self: &Arc<Self>,
        owner: OwnerClient,
        request: BindRequest,
        proof: RecoveredCatalogBinding,
    ) -> Result<BindSuccess, BindRefusal> {
        let snapshot = proof.snapshot.clone();
        let refusal = |phase, error| BindRefusal {
            phase,
            request: request.clone(),
            snapshot: Some(snapshot.clone()),
            error,
        };
        if !Arc::ptr_eq(self, &proof.store)
            || !owner.same_workspace(&proof.owner)
            || request.workspace != proof.request.workspace
            || request.branch != proof.request.branch
        {
            return Err(refusal(BindPhase::Snapshot, identity_error()));
        }
        let Some((mut expected, pending)) = proof.conflict else {
            return self.bind_durable(owner, request, snapshot);
        };
        let identity = owner
            .durable_identity()
            .map_err(|error| refusal(BindPhase::Snapshot, BindError::Owner(error)))?;
        let route = owner
            .bound_route()
            .map_err(|error| refusal(BindPhase::Snapshot, BindError::Owner(error)))?;
        if identity != Some(request.workspace.to_bytes()) || route.namespace() != proof.namespace {
            return Err(refusal(BindPhase::Snapshot, identity_error()));
        }
        expected.previous_namespace = proof.namespace;
        let local = owner
            .call(route, ServiceClass::General, None, |db| {
                db.settled_conflict()
            })
            .map_err(|error| refusal(BindPhase::Snapshot, BindError::Owner(error)))?;
        if local.as_ref() != Some(&expected)
            || expected.catalog != self.history().catalog_id().to_bytes()
            || expected.incarnation != self.history().incarnation()
            || pending.local_root.to_bytes() != expected.marker_root
        {
            return Err(refusal(BindPhase::Snapshot, identity_error()));
        }
        self.readers
            .mount(request.workspace, false)
            .map_err(|error| refusal(BindPhase::Snapshot, BindError::Read(error)))?;
        let result = (|| {
            let reader = self.reader(Some(request.workspace)).map_err(|error| {
                BindError::Read(Arc::new(super::PortError::ReadAdmission(error)))
            })?;
            reader
                .published(|history| super::durable_bind::validate_snapshot(history, &snapshot))
                .map_err(BindError::Read)?;
            let ports = self.ports_for(snapshot.scope, request.workspace);
            let history_base =
                checked_base(ports.client(), &snapshot).map_err(|error| BindError::Content {
                    error,
                    provider: ports.failure().unwrap_or_else(|e| Some(Arc::new(e))),
                })?;
            let mut marker_snapshot = snapshot.clone();
            marker_snapshot.effective_root = pending.local_root;
            let base = checked_base(ports.client(), &marker_snapshot).map_err(|error| {
                BindError::Content {
                    error,
                    provider: ports.failure().unwrap_or_else(|e| Some(Arc::new(e))),
                }
            })?;
            if base.root().root_inode() != history_base.root().root_inode() {
                return Err(identity_error());
            }
            owner
                .ensure_durable_snapshot(&snapshot)
                .map_err(BindError::Owner)?;
            owner.claim_durable_binding().map_err(BindError::Owner)?;
            Ok(BindSuccess {
                workspace: BoundWorkspace {
                    store: self.clone(),
                    owner,
                    snapshot: Mutex::new(snapshot.clone()),
                    identity: request.workspace,
                    workspace: Arc::new(Workspace::bind(route, base)),
                    committing: AtomicBool::new(false),
                    producer: Default::default(),
                    pending: Mutex::new(Some(pending)),
                },
            })
        })();
        result.map_err(|error| {
            let error = match self.close_reader(request.workspace) {
                Ok(()) => error,
                Err(cause) => BindError::Cleanup {
                    original: Box::new(error),
                    cause,
                },
            };
            refusal(BindPhase::Open, error)
        })
    }
}
