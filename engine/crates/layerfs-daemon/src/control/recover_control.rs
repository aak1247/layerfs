//! Recovery through pinned, same-device catalog and Overlay directories.
use super::{durable_root::DurableRoot, DurableControlConfig, DurableControlFailure, Service};
use crate::{
    durable_roster::{Authority, Roster, RosterError},
    Owner,
};
use layerfs_bridge::control::WorkspaceToken;
use layerfs_overlay::ProfileConfig;
use std::{fmt, io, path::PathBuf};

/// Original durable directories and authority supplied by a trusted caller.
/// `store_device` must come from the independently authenticated Store.
#[derive(Clone, Debug)]
pub struct RecoverControlConfig {
    pub catalog_directory: PathBuf,
    pub overlay_directory: PathBuf,
    pub sandbox: [u8; 32],
    pub profile: ProfileConfig,
    pub minimum_namespace: i64,
    pub store_device: u64,
}

/// Exact boundary that stopped the original recovery attempt.
#[derive(Debug)]
pub enum RecoverControlCause {
    Invalid(&'static str),
    Directory(io::Error),
    Catalog(RosterError),
    Control(Box<DurableControlFailure>),
}

/// Retains the original inputs and any acquired directory or catalog owners.
pub struct RecoverControlFailure {
    pub cause: RecoverControlCause,
    pub config: Box<RecoverControlConfig>,
    _catalog_root: Option<DurableRoot>,
    _overlay_root: Option<DurableRoot>,
    _catalog: Option<Roster>,
}
impl fmt::Debug for RecoverControlFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoverControlFailure")
            .field("cause", &self.cause)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RecoverControlFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "durable recovery setup: {:?}", self.cause)
    }
}
impl std::error::Error for RecoverControlFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            RecoverControlCause::Invalid(_) => None,
            RecoverControlCause::Directory(cause) => Some(cause),
            RecoverControlCause::Catalog(cause) => Some(cause),
            RecoverControlCause::Control(cause) => Some(cause.as_ref()),
        }
    }
}

impl Service {
    /// Opens one existing catalog and recovers it without mounting.
    ///
    /// The caller must authenticate the original Sandbox and physical Store,
    /// fence old processes, mounts and consumers, and reconcile all unknown
    /// effects before this call. This method pins both private directories,
    /// requires their device to match the authenticated Store device, and
    /// preserves the Service, Owner and original failure on an uncertain
    /// result. It is not an authorization to replay a failed attempt.
    pub fn recover_durable_control(
        &mut self,
        owner: &Owner,
        config: RecoverControlConfig,
    ) -> Result<Vec<WorkspaceToken>, Box<RecoverControlFailure>> {
        let mut catalog_root = None;
        let mut overlay_root = None;
        let mut catalog = None;
        let prepared = (|| {
            if self.durable.is_some()
                || self.fresh_failed
                || !self.owner.same_roster(&owner.client())
                || self.workspaces() != Some(0)
                || !self.store.has_durable_writer()
                || config.profile.pager_kib == 0
                || config.profile.max_pages.is_some()
                || config.minimum_namespace <= 0
                || config.minimum_namespace == i64::MAX
            {
                return Err(RecoverControlCause::Invalid("recovery control admission"));
            }
            let authority = Authority::for_catalog(config.sandbox, self.store.history())
                .map_err(RecoverControlCause::Catalog)?;
            catalog_root = Some(
                DurableRoot::open(&config.catalog_directory)
                    .map_err(RecoverControlCause::Directory)?,
            );
            overlay_root = Some(
                DurableRoot::open(&config.overlay_directory)
                    .map_err(RecoverControlCause::Directory)?,
            );
            let catalog_dir = catalog_root.as_ref().expect("catalog directory held");
            let overlay_dir = overlay_root.as_ref().expect("overlay directory held");
            let device = catalog_dir
                .device()
                .map_err(RecoverControlCause::Directory)?;
            if catalog_dir.path.starts_with(&overlay_dir.path)
                || overlay_dir.path.starts_with(&catalog_dir.path)
                || device
                    != overlay_dir
                        .device()
                        .map_err(RecoverControlCause::Directory)?
                || device != config.store_device
                || catalog_dir.mount_id() != overlay_dir.mount_id()
                || catalog_dir
                    .inode()
                    .map_err(RecoverControlCause::Directory)?
                    == overlay_dir
                        .inode()
                        .map_err(RecoverControlCause::Directory)?
            {
                return Err(RecoverControlCause::Invalid(
                    "separate local durable directories",
                ));
            }
            let directory = catalog_dir
                .duplicate()
                .map_err(RecoverControlCause::Directory)?;
            catalog = Some(
                Roster::open_opened(catalog_dir.path.clone(), directory, authority)
                    .map_err(RecoverControlCause::Catalog)?,
            );
            catalog_dir
                .verify()
                .map_err(RecoverControlCause::Directory)?;
            overlay_dir
                .verify()
                .map_err(RecoverControlCause::Directory)?;
            Ok(())
        })();
        if let Err(cause) = prepared {
            if let Some(catalog) = &mut catalog {
                catalog.quarantine("durable directory changed before recovery");
                if let Some(root) = &mut catalog_root {
                    root.quarantine();
                }
                if let Some(root) = &mut overlay_root {
                    root.quarantine();
                }
            }
            return Err(Box::new(RecoverControlFailure {
                cause,
                config: Box::new(config),
                _catalog_root: catalog_root,
                _overlay_root: overlay_root,
                _catalog: catalog,
            }));
        }
        let original = config.clone();
        let control = DurableControlConfig {
            catalog: catalog.expect("catalog opened"),
            overlay_directory: config.overlay_directory,
            sandbox: config.sandbox,
            profile: config.profile,
            minimum_namespace: config.minimum_namespace,
        };
        self.activate_durable_with_roots(
            owner,
            control,
            overlay_root.expect("overlay directory held"),
            catalog_root,
        )
        .map_err(|cause| {
            Box::new(RecoverControlFailure {
                cause: RecoverControlCause::Control(cause),
                config: Box::new(original),
                _catalog_root: None,
                _overlay_root: None,
                _catalog: None,
            })
        })
    }
}
