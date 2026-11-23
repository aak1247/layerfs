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
#[cfg(target_os = "linux")]
use layerfs_overlay::{ReadyRecovery, SettledConflictRecovery};
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

pub(super) enum LocalRecord {
    Ready(ReadyRecord),
    Conflict(SettledConflict),
    Checkpoint(Box<super::checkpoint_recovery::PreparedCheckpoint>),
    Orphans(OrphanRecoveryRecord, u64),
    ConflictOrphans(OrphanRecoveryRecord, u64, SettledConflict),
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

/// A locally ready original lease authenticated against one old Store. This
/// value holds no Store provider, reader, route or Workspace.
#[cfg(target_os = "linux")]
pub struct PreparedLegacyRecovery {
    request: BindRequest,
    local: PreparedLegacyLocal,
    snapshot: BranchSnapshot,
    catalog: [u8; 32],
    incarnation: u64,
    version: i64,
    physical: layerfs_persistence::StorePhysicalIdentity,
}

#[cfg(target_os = "linux")]
enum PreparedLegacyLocal {
    Ready(ReadyRecovery, ReadyRecord),
    Conflict(
        SettledConflictRecovery,
        SettledConflict,
        Arc<super::commit_merge::PendingProjection>,
    ),
}

/// The original locally settled binding retained by maintenance.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintainedDisposition {
    Ready,
    SettledConflict,
}

/// A migrated, locally ready Workspace with its original lease but no open
/// SQLite connection or Store handle. It cannot create a route or namespace.
#[cfg(target_os = "linux")]
pub struct MaintainedCatalogRecovery {
    request: BindRequest,
    previous_namespace: i64,
    source_version: i64,
    disposition: MaintainedDisposition,
    _closed: layerfs_overlay::ClosedFile,
}

#[cfg(target_os = "linux")]
impl MaintainedCatalogRecovery {
    pub fn request(&self) -> &BindRequest {
        &self.request
    }

    pub fn previous_namespace(&self) -> i64 {
        self.previous_namespace
    }

    pub fn source_version(&self) -> i64 {
        self.source_version
    }

    pub fn disposition(&self) -> MaintainedDisposition {
        self.disposition
    }

    pub fn verify_original(&self) -> layerfs_overlay::OverlayResult<()> {
        self._closed.verify_durable_identity()
    }
}
#[cfg(target_os = "linux")]
impl PreparedLegacyRecovery {
    /// The original namespace while its local lease remains held.
    pub fn previous_namespace(&self) -> i64 {
        match &self.local {
            PreparedLegacyLocal::Ready(_, observed) => observed.previous_namespace,
            PreparedLegacyLocal::Conflict(_, observed, _) => observed.previous_namespace,
        }
    }

    pub fn verify_original(&self) -> layerfs_overlay::OverlayResult<()> {
        match &self.local {
            PreparedLegacyLocal::Ready(ready, _) => ready.verify_original(),
            PreparedLegacyLocal::Conflict(conflict, _, _) => conflict.verify_original(),
        }
    }
}

/// A refusal retains its non-routable local custody and original cause.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub enum CatalogMigrationFailure {
    Preparation(Box<CatalogPreparationFailure>),
    Refused {
        cause: BindError,
        retained: RetainedCatalogPreparation,
    },
    Source {
        cause: layerfs_storage::port::PersistenceError,
        retained: RetainedCatalogPreparation,
    },
    Upgrade {
        cause: BindError,
        custody: RetainedCatalogRecovery,
    },
    Physical {
        cause: layerfs_storage::port::PersistenceError,
        custody: RetainedCatalogRecovery,
    },
    Close {
        cause: layerfs_overlay::OverlayError,
    },
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

#[derive(Clone, Copy)]
enum CatalogPreparationPurpose {
    Recovery,
    #[cfg(target_os = "linux")]
    Maintenance,
}
impl CatalogPreparationPurpose {
    fn permits_readonly_conflict(self) -> bool {
        match self {
            Self::Recovery => false,
            #[cfg(target_os = "linux")]
            Self::Maintenance => true,
        }
    }
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
        let pending = OverlayCatalogRecovery::open(path, profile, request.workspace.to_bytes())
            .map_err(|error| {
                Box::new(CatalogPreparationFailure::Open(OwnerError::Overlay(error)))
            })?;
        self.prepare_opened_catalog_recovery(pending, request)
    }

    /// Authenticate an already leased, non-routable Overlay against this
    /// Store. The trusted caller retains its original roster and fence.
    pub fn prepare_opened_catalog_recovery(
        self: &Arc<Self>,
        pending: OverlayCatalogRecovery,
        request: BindRequest,
    ) -> Result<PreparedCatalogRecovery, Box<CatalogPreparationFailure>> {
        self.prepare_opened_for(pending, request, CatalogPreparationPurpose::Recovery)
    }

    fn prepare_opened_for(
        self: &Arc<Self>,
        mut pending: OverlayCatalogRecovery,
        request: BindRequest,
        purpose: CatalogPreparationPurpose,
    ) -> Result<PreparedCatalogRecovery, Box<CatalogPreparationFailure>> {
        if purpose.permits_readonly_conflict()
            && !matches!(
                &pending,
                OverlayCatalogRecovery::Ready(_) | OverlayCatalogRecovery::SettledConflict(_)
            )
        {
            return Err(Box::new(CatalogPreparationFailure::Retained {
                cause: identity_error(),
                custody: RetainedCatalogRecovery {
                    _pending: Box::new(pending),
                },
            }));
        }
        let prepared = if let OverlayCatalogRecovery::PendingCheckpoint(value) = &mut pending {
            self.prepare_checkpoint_recovery(value, &request)
                .map(|plan| {
                    let snapshot = plan.snapshot.clone();
                    (LocalRecord::Checkpoint(Box::new(plan)), snapshot, None)
                })
        } else if let OverlayCatalogRecovery::PendingOrphans(value) = &mut pending {
            self.prepare_orphans(value, &request)
        } else {
            (|| -> Result<(LocalRecord,BranchSnapshot,Option<Arc<super::commit_merge::PendingProjection>>),BindError> {
            let (bytes,root,record)=match &pending {
                OverlayCatalogRecovery::Ready(value)=>{
                    let observed=value.observed().clone();
                    if observed.workspace!=request.workspace.to_bytes() { return Err(identity_error()); }
                    (observed.snapshot.clone(),observed.root,LocalRecord::Ready(observed))
                }
                OverlayCatalogRecovery::SettledConflict(value)=>{
                    let observed=value.observed().clone();
                    if (!purpose.permits_readonly_conflict() && !self.has_durable_writer()) || observed.catalog!=self.history().catalog_id().to_bytes()
                        || observed.incarnation!=self.history().incarnation() {
                        return Err(identity_error());
                    }
                    if purpose.permits_readonly_conflict() {
                        self.history().checkpoint_receipt(layerfs_history::CheckpointAttemptId::from_bytes([0; 32])).map_err(BindError::History)?;
                    } else {
                        self.history().require_checkpoint_receipts().map_err(BindError::History)?;
                    }
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

    /// Prepare an opaque maintenance-only local binding against the original
    /// read-only Store. The caller retains the Store-wide external fence.
    #[cfg(target_os = "linux")]
    pub fn prepare_catalog_for_maintenance(
        self: &Arc<Self>,
        pending: OverlayCatalogRecovery,
        request: BindRequest,
        source: &Arc<layerfs_persistence::StorageProvider>,
        original: &std::fs::File,
    ) -> Result<PreparedLegacyRecovery, Box<CatalogMigrationFailure>> {
        self.prepare_opened_for(pending, request, CatalogPreparationPurpose::Maintenance)
            .map_err(|failure| Box::new(CatalogMigrationFailure::Preparation(failure)))?
            .into_legacy(source, original)
    }
}

impl PreparedCatalogRecovery {
    /// Retain an authenticated settled predecessor without retaining the old
    /// Store. The caller must still hold the original roster and full fence.
    #[cfg(target_os = "linux")]
    pub fn into_legacy(
        self,
        source: &Arc<layerfs_persistence::StorageProvider>,
        original: &std::fs::File,
    ) -> Result<PreparedLegacyRecovery, Box<CatalogMigrationFailure>> {
        let provider: Arc<dyn layerfs_storage::port::PackPersistence> = source.clone();
        if !Arc::ptr_eq(&self.store.writer, &provider) {
            return Err(Box::new(CatalogMigrationFailure::Refused {
                cause: identity_error(),
                retained: self.retain(),
            }));
        }
        let physical = match source.observe_original_store(original) {
            Ok(value) => value,
            Err(cause) => {
                return Err(Box::new(CatalogMigrationFailure::Source {
                    cause,
                    retained: self.retain(),
                }))
            }
        };
        let local = match (&self.pending, &self.record, &self.conflict) {
            (OverlayCatalogRecovery::Ready(ready), LocalRecord::Ready(observed), None) => ready
                .preflight_upgrade(observed)
                .map(|version| (version, false))
                .map_err(|error| BindError::Owner(OwnerError::Overlay(error))),
            (
                OverlayCatalogRecovery::SettledConflict(conflict),
                LocalRecord::Conflict(observed),
                Some(_),
            ) => conflict
                .preflight_upgrade(observed)
                .map(|version| (version, true))
                .map_err(|error| BindError::Owner(OwnerError::Overlay(error))),
            _ => Err(BindError::History(HistoryError::Integrity(
                "legacy migration requires a settled Workspace",
            ))),
        };
        let (version, has_conflict) = match local {
            Ok(value) => value,
            Err(cause) => {
                return Err(Box::new(CatalogMigrationFailure::Refused {
                    cause,
                    retained: self.retain(),
                }))
            }
        };
        if matches!(version, 45 | 46) || has_conflict {
            // Both exact old and receipt-capable Store formats can contain a
            // locally ready Workspace. An unsupported receipt operation proves
            // the former only when it is the provider's explicit capability
            // result; no publication or missing receipt is inferred from it.
            match self
                .store
                .history()
                .checkpoint_receipt(layerfs_history::CheckpointAttemptId::from_bytes([0; 32]))
            {
                Ok(_) => {}
                Err(HistoryError::Unsupported("checkpoint receipt")) if !has_conflict => {}
                Err(error) => {
                    return Err(Box::new(CatalogMigrationFailure::Refused {
                        cause: BindError::History(error),
                        retained: self.retain(),
                    }))
                }
            }
        }
        let catalog = self.store.history().catalog_id().to_bytes();
        let incarnation = self.store.history().incarnation();
        let Self {
            request,
            pending,
            record,
            snapshot,
            conflict,
            ..
        } = self;
        let local = match (pending, record, conflict) {
            (OverlayCatalogRecovery::Ready(ready), LocalRecord::Ready(observed), None) => {
                PreparedLegacyLocal::Ready(ready, observed)
            }
            (
                OverlayCatalogRecovery::SettledConflict(value),
                LocalRecord::Conflict(observed),
                Some(projection),
            ) => PreparedLegacyLocal::Conflict(value, observed, projection),
            _ => unreachable!("legacy settled state checked"),
        };
        Ok(PreparedLegacyRecovery {
            request,
            local,
            snapshot,
            catalog,
            incarnation,
            version,
            physical,
        })
    }

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
            LocalRecord::ConflictOrphans(value, _, _) => value.previous_namespace,
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
                LocalRecord::Orphans(expected, root_serial)
                | LocalRecord::ConflictOrphans(expected, root_serial, _),
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
            (LocalRecord::ConflictOrphans(_, _, record), Some(value)) => Some((record, value)),
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
    /// Reauthenticate a locally retained legacy Workspace with this newly
    /// opened original Store before its same-lease schema transition.
    #[cfg(target_os = "linux")]
    pub fn prepare_migrated_catalog_recovery(
        self: &Arc<Self>,
        original: PreparedLegacyRecovery,
    ) -> Result<PreparedCatalogRecovery, Box<CatalogMigrationFailure>> {
        let PreparedLegacyRecovery {
            request,
            local,
            snapshot,
            catalog,
            incarnation,
            version,
            physical,
        } = original;
        let (pending, expected, expected_conflict) = match local {
            PreparedLegacyLocal::Ready(ready, observed) => (
                OverlayCatalogRecovery::Ready(ready),
                LocalRecord::Ready(observed),
                None,
            ),
            PreparedLegacyLocal::Conflict(value, observed, projection) => (
                OverlayCatalogRecovery::SettledConflict(value),
                LocalRecord::Conflict(observed),
                Some(projection),
            ),
        };
        if let Some(writer) = &self.durable_writer {
            if let Err(cause) = writer.verify_observed_store(&physical) {
                return Err(Box::new(CatalogMigrationFailure::Physical {
                    cause,
                    custody: RetainedCatalogRecovery {
                        _pending: Box::new(pending),
                    },
                }));
            }
        }
        let identity = self.history().catalog_id().to_bytes() == catalog
            && self.history().incarnation() == incarnation
            && self.has_durable_writer();
        let receipts = self.history().require_checkpoint_receipts();
        if !identity || receipts.is_err() {
            return Err(Box::new(CatalogMigrationFailure::Upgrade {
                cause: if identity {
                    BindError::History(receipts.expect_err("receipt check failed"))
                } else {
                    identity_error()
                },
                custody: RetainedCatalogRecovery {
                    _pending: Box::new(pending),
                },
            }));
        }
        let prepared = self
            .prepare_opened_catalog_recovery(pending, request)
            .map_err(|failure| Box::new(CatalogMigrationFailure::Preparation(failure)))?;
        let records_match = match (&prepared.record, &expected) {
            (LocalRecord::Ready(actual), LocalRecord::Ready(original)) => actual == original,
            (LocalRecord::Conflict(actual), LocalRecord::Conflict(original)) => actual == original,
            _ => false,
        };
        let conflicts_match = match (&prepared.conflict, &expected_conflict) {
            (None, None) => true,
            (Some(actual), Some(original)) => {
                actual.descriptor == original.descriptor
                    && actual.merge.local_root == original.merge.local_root
                    && actual.merge.conflicted == original.merge.conflicted
            }
            _ => false,
        };
        let matches = records_match && conflicts_match && prepared.snapshot == snapshot;
        if !matches {
            return Err(Box::new(CatalogMigrationFailure::Refused {
                cause: identity_error(),
                retained: prepared.retain(),
            }));
        }
        if matches!(version, 45 | 46) {
            return Ok(prepared);
        }
        let PreparedCatalogRecovery {
            store,
            request,
            pending,
            record,
            snapshot,
            conflict,
        } = prepared;
        let pending = match (pending, &record) {
            (OverlayCatalogRecovery::Ready(ready), LocalRecord::Ready(observed)) => ready
                .upgrade_legacy(observed)
                .map(OverlayCatalogRecovery::Ready)
                .map_err(|(error, ready)| {
                    Box::new(CatalogMigrationFailure::Upgrade {
                        cause: BindError::Owner(OwnerError::Overlay(error)),
                        custody: RetainedCatalogRecovery {
                            _pending: Box::new(OverlayCatalogRecovery::Ready(ready)),
                        },
                    })
                })?,
            (OverlayCatalogRecovery::SettledConflict(value), LocalRecord::Conflict(observed)) => {
                value
                    .upgrade_legacy(observed)
                    .map(OverlayCatalogRecovery::SettledConflict)
                    .map_err(|(error, value)| {
                        Box::new(CatalogMigrationFailure::Upgrade {
                            cause: BindError::Owner(OwnerError::Overlay(error)),
                            custody: RetainedCatalogRecovery {
                                _pending: Box::new(OverlayCatalogRecovery::SettledConflict(value)),
                            },
                        })
                    })?
            }
            _ => unreachable!("legacy settled state checked"),
        };
        Ok(PreparedCatalogRecovery {
            store,
            request,
            pending,
            record,
            snapshot,
            conflict,
        })
    }

    /// Migrate and checked-close one already authenticated Workspace for a
    /// full-Store maintenance batch. The caller must retain its original
    /// Store-wide writer and consumer fence and the original roster throughout.
    /// This does not advance a namespace, register a Workspace or publish.
    #[cfg(target_os = "linux")]
    pub fn migrate_catalog_for_maintenance(
        self: &Arc<Self>,
        original: PreparedLegacyRecovery,
    ) -> Result<MaintainedCatalogRecovery, Box<CatalogMigrationFailure>> {
        let source_version = original.version;
        let prepared = self.prepare_migrated_catalog_recovery(original)?;
        let PreparedCatalogRecovery {
            pending,
            record,
            request,
            ..
        } = prepared;
        let verify_version = |result: layerfs_overlay::OverlayResult<i64>| {
            result.and_then(|version| {
                if matches!(version, 45 | 46) {
                    Ok(())
                } else {
                    Err(layerfs_overlay::OverlayError::Invalid(
                        "maintenance migration schema",
                    ))
                }
            })
        };
        let (db, previous_namespace, disposition) = match (pending, record) {
            (OverlayCatalogRecovery::Ready(ready), LocalRecord::Ready(observed)) => {
                if let Err(cause) = verify_version(ready.preflight_upgrade(&observed)) {
                    return Err(Box::new(CatalogMigrationFailure::Upgrade {
                        cause: BindError::Owner(OwnerError::Overlay(cause)),
                        custody: RetainedCatalogRecovery {
                            _pending: Box::new(OverlayCatalogRecovery::Ready(ready)),
                        },
                    }));
                }
                let db = ready.complete(&observed).map_err(|(cause, ready)| {
                    Box::new(CatalogMigrationFailure::Upgrade {
                        cause: BindError::Owner(OwnerError::Overlay(cause)),
                        custody: RetainedCatalogRecovery {
                            _pending: Box::new(OverlayCatalogRecovery::Ready(ready)),
                        },
                    })
                })?;
                (
                    db,
                    observed.previous_namespace,
                    MaintainedDisposition::Ready,
                )
            }
            (OverlayCatalogRecovery::SettledConflict(value), LocalRecord::Conflict(observed)) => {
                if let Err(cause) = verify_version(value.preflight_upgrade(&observed)) {
                    return Err(Box::new(CatalogMigrationFailure::Upgrade {
                        cause: BindError::Owner(OwnerError::Overlay(cause)),
                        custody: RetainedCatalogRecovery {
                            _pending: Box::new(OverlayCatalogRecovery::SettledConflict(value)),
                        },
                    }));
                }
                let namespace = observed.previous_namespace;
                let db = value
                    .complete_settled_active(&observed, namespace)
                    .map_err(|(cause, value)| {
                        Box::new(CatalogMigrationFailure::Upgrade {
                            cause: BindError::Owner(OwnerError::Overlay(cause)),
                            custody: RetainedCatalogRecovery {
                                _pending: Box::new(OverlayCatalogRecovery::SettledConflict(value)),
                            },
                        })
                    })?;
                (db, namespace, MaintainedDisposition::SettledConflict)
            }
            _ => unreachable!("legacy migration prepared a settled Workspace"),
        };
        let closed = db
            .close_database_file()
            .map_err(|cause| Box::new(CatalogMigrationFailure::Close { cause }))?;
        Ok(MaintainedCatalogRecovery {
            request,
            previous_namespace,
            source_version,
            disposition,
            _closed: closed,
        })
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
