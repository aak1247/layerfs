//! One catalog-backed Workspace creation with before-effect capacity admission.
use super::{
    registry::{Binding, Entry},
    types::KernelFailure,
    Failure, Service, Success,
};
use crate::{
    durable_roster::{Authority, Roster, RosterError, WorkspaceRecord},
    store::{BindRefusal, BindRequest, BoundWorkspace},
    Owner, OwnerClient, OwnerError,
};
use layerfs_bridge::control::{Activity, ControlCode, Reply, WorkspaceToken};
use layerfs_history::{BranchId, BranchSnapshot, HistoryError, WorkspaceId};
use layerfs_overlay::ProfileConfig;
use layerfs_store_service::{DurableCreationFailure, DurableReleaseFailure, DurableReservation};
use std::{fmt, path::Path, sync::Arc};

/// Authorized identities and durable resource profile for one creation.
pub struct RecordedMountRequest {
    pub sandbox: [u8; 32],
    pub profile: ProfileConfig,
    pub workspace: WorkspaceId,
    pub branch: BranchId,
}

/// Retained admission when recording an intent had an uncertain result.
pub struct RecordedMountReservation(Option<DurableReservation>);
impl RecordedMountReservation {
    pub fn namespace(&self) -> i64 {
        self.0.as_ref().expect("reservation retained").namespace()
    }
}
impl fmt::Debug for RecordedMountReservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordedMountReservation")
            .field("namespace", &self.namespace())
            .finish()
    }
}
impl Drop for RecordedMountReservation {
    fn drop(&mut self) {
        if let Some(value) = self.0.take() {
            std::mem::forget(value);
        }
    }
}

/// Exact failure and any owner or reservation that must remain in custody.
pub enum RecordedMountFailure {
    Invalid(&'static str),
    Snapshot(HistoryError),
    MissingBranch(BranchId),
    Before(Failure),
    Reservation(OwnerError),
    Discovery(RosterError),
    PreflightCleanup {
        original: Box<RecordedMountFailure>,
        release: Option<Box<DurableReleaseFailure>>,
        registry: Option<Failure>,
    },
    Intent {
        record: WorkspaceRecord,
        snapshot: Box<BranchSnapshot>,
        cause: RosterError,
        reservation: RecordedMountReservation,
    },
    Creation {
        record: WorkspaceRecord,
        snapshot: Box<BranchSnapshot>,
        failure: Box<DurableCreationFailure>,
    },
    Owner {
        record: WorkspaceRecord,
        snapshot: Box<BranchSnapshot>,
        cause: OwnerError,
        owner: OwnerClient,
    },
    DiscoveryAfterCreation {
        record: WorkspaceRecord,
        snapshot: Box<BranchSnapshot>,
        cause: RosterError,
        owner: OwnerClient,
    },
    Bind {
        record: WorkspaceRecord,
        failure: Box<BindRefusal>,
        owner: OwnerClient,
    },
    Install {
        record: WorkspaceRecord,
        cause: Failure,
        workspace: Box<BoundWorkspace>,
    },
}
impl fmt::Debug for RecordedMountFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(cause) => f.debug_tuple("Invalid").field(cause).finish(),
            Self::Snapshot(cause) => f.debug_tuple("Snapshot").field(cause).finish(),
            Self::MissingBranch(id) => f.debug_tuple("MissingBranch").field(id).finish(),
            Self::Before(cause) => f.debug_tuple("Before").field(cause).finish(),
            Self::Reservation(cause) => f.debug_tuple("Reservation").field(cause).finish(),
            Self::Discovery(cause) => f.debug_tuple("Discovery").field(cause).finish(),
            Self::PreflightCleanup {
                original,
                release,
                registry,
            } => f
                .debug_struct("PreflightCleanup")
                .field("original", original)
                .field("release", release)
                .field("registry", registry)
                .finish(),
            Self::Intent {
                record,
                cause,
                reservation,
                ..
            } => f
                .debug_struct("Intent")
                .field("record", record)
                .field("cause", cause)
                .field("reservation", reservation)
                .finish_non_exhaustive(),
            Self::Creation {
                record, failure, ..
            } => f
                .debug_struct("Creation")
                .field("record", record)
                .field("failure", failure)
                .finish_non_exhaustive(),
            Self::Owner { record, cause, .. } => f
                .debug_struct("Owner")
                .field("record", record)
                .field("cause", cause)
                .finish_non_exhaustive(),
            Self::DiscoveryAfterCreation { record, cause, .. } => f
                .debug_struct("DiscoveryAfterCreation")
                .field("record", record)
                .field("cause", cause)
                .finish_non_exhaustive(),
            Self::Bind {
                record, failure, ..
            } => f
                .debug_struct("Bind")
                .field("record", record)
                .field("failure", failure)
                .finish_non_exhaustive(),
            Self::Install { record, cause, .. } => f
                .debug_struct("Install")
                .field("record", record)
                .field("cause", cause)
                .finish_non_exhaustive(),
        }
    }
}
impl fmt::Display for RecordedMountFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "recorded Workspace mount: {self:?}")
    }
}
impl std::error::Error for RecordedMountFailure {}

impl Service {
    /// Create and bind one catalog-backed Workspace. The caller serializes the
    /// catalog and authenticates the Sandbox, Store and Branch. On any failure
    /// after recording the intent it keeps this Service, Owner and original
    /// failure; no creation, binding or external effect is automatically retried.
    pub fn mount_recorded(
        &self,
        catalog: &mut Roster,
        owner: &Owner,
        overlay_directory: &Path,
        request: RecordedMountRequest,
    ) -> Result<Success, Box<RecordedMountFailure>> {
        let RecordedMountRequest {
            sandbox,
            profile,
            workspace,
            branch,
        } = request;
        if !self.owner.same_roster(&owner.client())
            || profile.pager_kib == 0
            || profile.max_pages.is_some()
        {
            return Err(Box::new(RecordedMountFailure::Invalid(
                "recorded mount owner or profile",
            )));
        }
        let authority = Authority::for_catalog(sandbox, self.store.history())
            .map_err(|cause| Box::new(RecordedMountFailure::Discovery(cause)))?;
        if authority != catalog.authority() {
            return Err(Box::new(RecordedMountFailure::Invalid(
                "recorded mount authority",
            )));
        }
        let snapshot = self
            .store
            .history()
            .branch_snapshot(branch)
            .map_err(|cause| Box::new(RecordedMountFailure::Snapshot(cause)))?
            .ok_or_else(|| Box::new(RecordedMountFailure::MissingBranch(branch)))?;
        self.reserve_recorded_binding(workspace)
            .map_err(|cause| Box::new(RecordedMountFailure::Before(cause)))?;
        let reservation = match owner.reserve_durable(workspace.to_bytes()) {
            Ok(reservation) => reservation,
            Err(cause) => {
                return Err(self.cleanup_recorded_preflight(
                    workspace,
                    None,
                    RecordedMountFailure::Reservation(cause),
                ))
            }
        };
        let record = WorkspaceRecord { workspace, branch };
        if let Err(cause) = catalog.record(record) {
            if matches!(
                &cause,
                RosterError::AlreadyExists | RosterError::PendingClose(_) | RosterError::Invalid(_)
            ) {
                return Err(self.cleanup_recorded_preflight(
                    workspace,
                    Some(reservation),
                    RecordedMountFailure::Discovery(cause),
                ));
            }
            return Err(Box::new(RecordedMountFailure::Intent {
                record,
                snapshot: Box::new(snapshot),
                cause,
                reservation: RecordedMountReservation(Some(reservation)),
            }));
        }
        if let Err(cause) = catalog.check_live() {
            return Err(Box::new(RecordedMountFailure::Intent {
                record,
                snapshot: Box::new(snapshot),
                cause,
                reservation: RecordedMountReservation(Some(reservation)),
            }));
        }
        let path = overlay_directory.join(record.overlay_name());
        let client = reservation
            .create(&path, profile, snapshot.effective_root.to_bytes())
            .map_err(|failure| {
                Box::new(RecordedMountFailure::Creation {
                    record,
                    snapshot: Box::new(snapshot.clone()),
                    failure,
                })
            })?;
        let route = client.bound_route().map_err(|cause| {
            Box::new(RecordedMountFailure::Owner {
                record,
                snapshot: Box::new(snapshot.clone()),
                cause,
                owner: client.clone(),
            })
        })?;
        if let Err(cause) = catalog.check_live() {
            return Err(Box::new(RecordedMountFailure::DiscoveryAfterCreation {
                record,
                snapshot: Box::new(snapshot),
                cause,
                owner: client,
            }));
        }
        let binding = self
            .store
            .bind_durable(
                client.clone(),
                BindRequest { workspace, branch },
                snapshot.clone(),
            )
            .map_err(|failure| {
                Box::new(RecordedMountFailure::Bind {
                    record,
                    failure: Box::new(failure),
                    owner: client,
                })
            })?
            .workspace;
        let token = WorkspaceToken {
            workspace,
            namespace: route.namespace(),
        };
        let mut entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(_) => {
                return Err(Box::new(RecordedMountFailure::Install {
                    record,
                    cause: Failure::Poisoned,
                    workspace: Box::new(binding),
                }))
            }
        };
        if !matches!(entries.get(&workspace), Some(Entry::Binding)) {
            return Err(Box::new(RecordedMountFailure::Install {
                record,
                cause: Failure::Rejected(ControlCode::Unknown, "recorded mount reservation lost"),
                workspace: Box::new(binding),
            }));
        }
        entries.insert(
            workspace,
            Entry::Bound(Box::new(Binding {
                kernel: super::kernel::Kernel::Unattached,
                workspace: Arc::new(binding),
                snapshot: snapshot.clone(),
                activity: Activity::Idle,
                recorded: true,
                epoch: 1,
                published: None,
            })),
        );
        Ok(Success::reply(Reply::Bound {
            token,
            binding: snapshot,
        }))
    }

    fn reserve_recorded_binding(&self, workspace: WorkspaceId) -> Result<(), Failure> {
        let mut entries = self.entries.lock().map_err(|_| Failure::Poisoned)?;
        if entries.contains_key(&workspace) {
            return Err(Failure::Rejected(
                ControlCode::Busy,
                "Workspace incarnation already admitted",
            ));
        }
        if entries.len() >= self.capacity {
            return Err(Failure::Rejected(
                ControlCode::Capacity,
                "daemon Workspace capacity",
            ));
        }
        if let Some(stopped) = self.owner.maintenance_failure().map_err(Failure::Owner)? {
            return Err(Failure::Kernel(Box::new(KernelFailure {
                code: ControlCode::Capacity,
                phase: "mount:debt",
                detail: stopped.to_string(),
                evidence: Some(Box::new(stopped)),
            })));
        }
        entries.insert(workspace, Entry::Binding);
        Ok(())
    }

    fn cleanup_recorded_preflight(
        &self,
        workspace: WorkspaceId,
        reservation: Option<DurableReservation>,
        original: RecordedMountFailure,
    ) -> Box<RecordedMountFailure> {
        if let Some(reservation) = reservation {
            if let Err(release) = reservation.release() {
                return Box::new(RecordedMountFailure::PreflightCleanup {
                    original: Box::new(original),
                    release: Some(release),
                    registry: None,
                });
            }
        }
        let registry = match self.entries.lock() {
            Ok(mut entries) if matches!(entries.get(&workspace), Some(Entry::Binding)) => {
                entries.remove(&workspace);
                None
            }
            Ok(_) => Some(Failure::Rejected(
                ControlCode::Unknown,
                "recorded mount reservation lost",
            )),
            Err(_) => Some(Failure::Poisoned),
        };
        match registry {
            None => Box::new(original),
            Some(cause) => Box::new(RecordedMountFailure::PreflightCleanup {
                original: Box::new(original),
                release: None,
                registry: Some(cause),
            }),
        }
    }
}
