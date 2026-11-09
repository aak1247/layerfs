//! Creation of a new durable catalog, never recovery of an existing Sandbox.
use super::{durable_root::DurableRoot, Service};
use crate::{
    durable_roster::{Authority, Roster, RosterError},
    Owner, OwnerError,
};
use layerfs_overlay::ProfileConfig;
use std::{fmt, fs, io, path::PathBuf};

/// Dedicated, already created private directories and a new Sandbox identity.
/// The caller authenticates the physical Store and supplies a globally fresh
/// Sandbox identity; this is not a recovery or an old-mount fence.
#[derive(Clone, Debug)]
pub struct FreshControlConfig {
    pub catalog_directory: PathBuf,
    pub overlay_directory: PathBuf,
    pub sandbox: [u8; 32],
    pub profile: ProfileConfig,
}

/// The original initialization boundary and cause.
#[derive(Debug)]
pub enum FreshControlCause {
    Invalid(&'static str),
    Directory(io::Error),
    Owner(OwnerError),
    Catalog(RosterError),
}

/// Original input and any catalog/directory descriptors remain in custody.
pub struct FreshControlFailure {
    pub cause: FreshControlCause,
    pub config: Box<FreshControlConfig>,
    _catalog: Option<Roster>,
    _catalog_root: Option<DurableRoot>,
    _overlay_root: Option<DurableRoot>,
}
impl fmt::Debug for FreshControlFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FreshControlFailure")
            .field("cause", &self.cause)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for FreshControlFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "new durable control: {:?}", self.cause)
    }
}
impl std::error::Error for FreshControlFailure {}

impl Service {
    /// Creates exactly one empty catalog and installs durable control. Existing
    /// data is refused, never reopened or deleted. Directory and parent syncs
    /// precede catalog creation; all work uses pinned directory descriptors.
    /// The Service and Owner must remain private until this completes. On any
    /// failure keep this Service, the Owner and the returned original receipt.
    pub fn activate_fresh_control(
        &mut self,
        owner: &Owner,
        config: FreshControlConfig,
    ) -> Result<(), Box<FreshControlFailure>> {
        let mut catalog_root = None;
        let mut overlay_root = None;
        let mut catalog = None;
        let mut catalog_attempted = false;
        let mut admitted = false;
        let result = (|| {
            if self.durable.is_some()
                || self.fresh_failed
                || !self.owner.same_roster(&owner.client())
                || self.workspaces() != Some(0)
            {
                return Err(FreshControlCause::Invalid("fresh control admission"));
            }
            owner
                .vacant_namespace_floor()
                .map_err(FreshControlCause::Owner)?;
            admitted = true;
            if !self.store.has_durable_writer()
                || config.profile.pager_kib == 0
                || config.profile.max_pages.is_some()
            {
                return Err(FreshControlCause::Invalid("fresh control admission"));
            }
            let authority = Authority::for_catalog(config.sandbox, self.store.history())
                .map_err(FreshControlCause::Catalog)?;
            catalog_root = Some(
                DurableRoot::open(&config.catalog_directory)
                    .map_err(FreshControlCause::Directory)?,
            );
            overlay_root = Some(
                DurableRoot::open(&config.overlay_directory)
                    .map_err(FreshControlCause::Directory)?,
            );
            let catalog_dir = catalog_root.as_ref().expect("catalog directory held");
            let overlay_dir = overlay_root.as_ref().expect("overlay directory held");
            if catalog_dir.path.starts_with(&overlay_dir.path)
                || overlay_dir.path.starts_with(&catalog_dir.path)
                || catalog_dir.device().map_err(FreshControlCause::Directory)?
                    != overlay_dir.device().map_err(FreshControlCause::Directory)?
            {
                return Err(FreshControlCause::Invalid(
                    "separate local durable directories",
                ));
            }
            catalog_dir
                .sync_creation()
                .map_err(FreshControlCause::Directory)?;
            overlay_dir
                .sync_creation()
                .map_err(FreshControlCause::Directory)?;
            if fs::read_dir(overlay_dir.retained_path())
                .map_err(FreshControlCause::Directory)?
                .next()
                .transpose()
                .map_err(FreshControlCause::Directory)?
                .is_some()
            {
                return Err(FreshControlCause::Invalid("overlay directory is not empty"));
            }
            let directory = catalog_dir
                .duplicate()
                .map_err(FreshControlCause::Directory)?;
            catalog_attempted = true;
            catalog = Some(
                Roster::create_opened(catalog_dir.path.clone(), directory, authority)
                    .map_err(FreshControlCause::Catalog)?,
            );
            catalog_dir.verify().map_err(FreshControlCause::Directory)?;
            overlay_dir.verify().map_err(FreshControlCause::Directory)?;
            Ok(())
        })();
        if let Err(cause) = result {
            if admitted {
                self.fresh_failed = true;
            }
            if catalog_attempted {
                if let Some(catalog) = &mut catalog {
                    catalog.quarantine("fresh control initialization failed");
                }
                if let Some(root) = &mut catalog_root {
                    root.quarantine();
                }
                if let Some(root) = &mut overlay_root {
                    root.quarantine();
                }
            }
            return Err(Box::new(FreshControlFailure {
                cause,
                config: Box::new(config),
                _catalog: catalog,
                _catalog_root: catalog_root,
                _overlay_root: overlay_root,
            }));
        }
        self.install_fresh_control(
            catalog.expect("fresh catalog created"),
            overlay_root.expect("overlay directory held"),
            config.sandbox,
            config.profile,
        );
        Ok(())
    }
}
