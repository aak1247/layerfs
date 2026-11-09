//! Explicit installation of one independently fenced durable control catalog.
use super::{
    durable_root::DurableRoot, CatalogRecoveryFailure, Failure, RecordedMountRequest, Service,
    Success,
};
use crate::{durable_roster::Roster, Owner};
use layerfs_bridge::control::{UnmountControl, WorkspaceToken};
use layerfs_history::{BranchId, WorkspaceId};
use layerfs_overlay::ProfileConfig;
use std::{
    fmt, io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// Previously authenticated catalog and its original durable storage location.
pub struct DurableControlConfig {
    pub catalog: Roster,
    pub overlay_directory: PathBuf,
    pub sandbox: [u8; 32],
    pub profile: ProfileConfig,
    pub minimum_namespace: i64,
}

pub(super) struct DurableControl {
    state: Mutex<DurableState>,
    sandbox: [u8; 32],
    profile: ProfileConfig,
}
struct DurableState {
    catalog: Roster,
    root: DurableRoot,
    catalog_root: Option<DurableRoot>,
    failure: Option<Arc<io::Error>>,
}
impl DurableState {
    fn check(&mut self) -> Result<(), Failure> {
        if let Some(cause) = &self.failure {
            return Err(Failure::DurableRoot(cause.clone()));
        }
        let verified = self.root.verify().and_then(|()| match &self.catalog_root {
            Some(root) => root.verify(),
            None => Ok(()),
        });
        if let Err(error) = verified {
            let cause = Arc::new(error);
            self.root.quarantine();
            if let Some(root) = &mut self.catalog_root {
                root.quarantine();
            }
            self.catalog
                .quarantine("durable directory identity changed");
            self.failure = Some(cause.clone());
            return Err(Failure::DurableRoot(cause));
        }
        Ok(())
    }
}

/// The original setup refusal and any recovery already completed.
#[derive(Debug)]
pub enum DurableControlCause {
    Invalid(&'static str),
    Directory(io::Error),
    Recovery(Box<CatalogRecoveryFailure>),
    DirectoryAfterRecovery {
        cause: Arc<io::Error>,
        completed: Vec<WorkspaceToken>,
    },
    RecoveryAndDirectory {
        recovery: Box<CatalogRecoveryFailure>,
        cause: Arc<io::Error>,
    },
}
/// Original setup cause, catalog and, if opened, the original directory handle.
pub struct DurableControlFailure {
    pub cause: DurableControlCause,
    pub config: Box<DurableControlConfig>,
    _root: Option<DurableRoot>,
    _catalog_root: Option<DurableRoot>,
}
impl fmt::Debug for DurableControlFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableControlFailure")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for DurableControlFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "durable control setup: {:?}", self.cause)
    }
}
impl std::error::Error for DurableControlFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            DurableControlCause::Invalid(_) => None,
            DurableControlCause::Directory(cause) => Some(cause),
            DurableControlCause::Recovery(cause) => Some(cause.as_ref()),
            DurableControlCause::DirectoryAfterRecovery { cause, .. } => Some(cause.as_ref()),
            DurableControlCause::RecoveryAndDirectory { recovery, .. } => Some(recovery.as_ref()),
        }
    }
}

fn checked_result(
    result: Result<Success, Failure>,
    check: Result<(), Failure>,
) -> Result<Success, Failure> {
    match (result, check) {
        (result, Ok(())) => result,
        (Ok(original), Err(cause)) => Err(Failure::After {
            cause: Box::new(cause),
            original: Box::new(original),
        }),
        (Err(original), Err(cause)) => Err(Failure::AfterFailure {
            cause: Box::new(cause),
            original: Box::new(original),
        }),
    }
}

impl Service {
    pub(super) fn install_fresh_control(
        &mut self,
        catalog: Roster,
        root: DurableRoot,
        catalog_root: DurableRoot,
        sandbox: [u8; 32],
        profile: ProfileConfig,
    ) {
        self.durable = Some(DurableControl {
            state: Mutex::new(DurableState {
                catalog,
                root,
                catalog_root: Some(catalog_root),
                failure: None,
            }),
            sandbox,
            profile,
        });
    }

    pub(super) fn check_durable_state(&self) -> Result<(), Failure> {
        if self.fresh_failed {
            return Err(Failure::Rejected(
                layerfs_bridge::control::ControlCode::Unknown,
                "fresh durable control initialization failed",
            ));
        }
        match &self.durable {
            None => Ok(()),
            Some(control) => control.state.lock().map_err(|_| Failure::Poisoned)?.check(),
        }
    }

    pub(super) fn durable_mount(
        &self,
        workspace: WorkspaceId,
        branch: BranchId,
    ) -> Option<Result<Success, Failure>> {
        let control = self.durable.as_ref()?;
        let mut state = match control.state.lock() {
            Ok(state) => state,
            Err(_) => return Some(Err(Failure::Poisoned)),
        };
        if let Err(cause) = state.check() {
            return Some(Err(cause));
        }
        let path = state.root.retained_path();
        let result = self
            .mount_recorded_on_roster(
                &mut state.catalog,
                &path,
                RecordedMountRequest {
                    sandbox: control.sandbox,
                    profile: control.profile,
                    workspace,
                    branch,
                },
            )
            .map_err(Failure::RecordedMount);
        let check = state.check();
        Some(checked_result(result, check))
    }

    pub(super) fn durable_unmount(
        &self,
        request: UnmountControl,
    ) -> Option<Result<Success, Failure>> {
        let control = self.durable.as_ref()?;
        let mut state = match control.state.lock() {
            Ok(state) => state,
            Err(_) => return Some(Err(Failure::Poisoned)),
        };
        if let Err(cause) = state.check() {
            return Some(Err(cause));
        }
        let result = self
            .unmount_recorded_on_catalog(&mut state.catalog, request)
            .map_err(|error| Failure::RecordedClose(Box::new(error)));
        let check = state.check();
        Some(checked_result(result, check))
    }

    /// Installs the catalog only after one complete recovery without mounts.
    /// The trusted caller must authenticate the Sandbox and physical Store,
    /// fence old processes/mounts/consumers, and reconcile unknown operations.
    /// A failure returns the original config and completed tokens while this
    /// Service and its Owner remain with the caller; do not replay it.
    pub fn activate_durable_control(
        &mut self,
        owner: &Owner,
        config: DurableControlConfig,
    ) -> Result<Vec<WorkspaceToken>, Box<DurableControlFailure>> {
        let invalid = if self.durable.is_some() || self.fresh_failed {
            Some("durable control already installed")
        } else if !self.store.has_durable_writer() {
            Some("durable control requires a Durable Store")
        } else {
            None
        };
        if let Some(detail) = invalid {
            return Err(Box::new(DurableControlFailure {
                cause: DurableControlCause::Invalid(detail),
                config: Box::new(config),
                _root: None,
                _catalog_root: None,
            }));
        }
        let root = match DurableRoot::open(&config.overlay_directory) {
            Ok(root) => root,
            Err(error) => {
                return Err(Box::new(DurableControlFailure {
                    cause: DurableControlCause::Directory(error),
                    config: Box::new(config),
                    _root: None,
                    _catalog_root: None,
                }))
            }
        };
        self.activate_durable_with_roots(owner, config, root, None)
    }

    pub(super) fn activate_durable_with_roots(
        &mut self,
        owner: &Owner,
        mut config: DurableControlConfig,
        mut root: DurableRoot,
        mut catalog_root: Option<DurableRoot>,
    ) -> Result<Vec<WorkspaceToken>, Box<DurableControlFailure>> {
        let path = root.retained_path();
        let recovery = self.recover_catalog(
            &mut config.catalog,
            owner,
            &path,
            config.sandbox,
            config.profile,
            config.minimum_namespace,
        );
        let checked = root.verify().and_then(|()| match &catalog_root {
            Some(root) => root.verify(),
            None => Ok(()),
        });
        let completed = match (recovery, checked) {
            (Ok(tokens), Ok(())) => tokens,
            (Err(cause), Ok(())) => {
                return Err(Box::new(DurableControlFailure {
                    cause: DurableControlCause::Recovery(cause),
                    config: Box::new(config),
                    _root: Some(root),
                    _catalog_root: catalog_root,
                }))
            }
            (result, Err(error)) => {
                let cause = Arc::new(error);
                root.quarantine();
                if let Some(root) = &mut catalog_root {
                    root.quarantine();
                }
                config
                    .catalog
                    .quarantine("durable directory changed during recovery");
                let cause = match result {
                    Ok(completed) => {
                        DurableControlCause::DirectoryAfterRecovery { cause, completed }
                    }
                    Err(recovery) => DurableControlCause::RecoveryAndDirectory { recovery, cause },
                };
                return Err(Box::new(DurableControlFailure {
                    cause,
                    config: Box::new(config),
                    _root: Some(root),
                    _catalog_root: catalog_root,
                }));
            }
        };
        self.durable = Some(DurableControl {
            state: Mutex::new(DurableState {
                catalog: config.catalog,
                root,
                catalog_root,
                failure: None,
            }),
            sandbox: config.sandbox,
            profile: config.profile,
        });
        Ok(completed)
    }
}
