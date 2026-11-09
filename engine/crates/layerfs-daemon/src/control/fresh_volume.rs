//! One fresh Sandbox on a durable volume, from directory creation to control.
use super::{FreshControlConfig, FreshControlFailure, Service};
use crate::{
    durable_directories::{create_fresh_directories, DirectoryFailure, FreshDirectories},
    Owner, OwnerError,
};
use layerfs_overlay::ProfileConfig;
use std::{fmt, io, path::PathBuf};

/// The caller authenticates this volume and Store and supplies a fresh identity.
#[derive(Clone, Debug)]
pub struct FreshVolumeConfig {
    pub volume: PathBuf,
    pub sandbox: [u8; 32],
    pub profile: ProfileConfig,
    pub store_device: u64,
}
/// Original cause, including both results if a final verification also failed.
#[derive(Debug)]
pub enum FreshVolumeCause {
    Invalid(&'static str),
    Owner(OwnerError),
    Directories(Box<DirectoryFailure>),
    Activation(Box<FreshControlFailure>),
    Verification {
        cause: io::Error,
        activation: Option<Box<FreshControlFailure>>,
        activated: bool,
    },
}
/// The original input, failure and all created directory owners stay together.
pub struct FreshVolumeFailure {
    pub cause: FreshVolumeCause,
    pub config: Box<FreshVolumeConfig>,
    _directories: Option<FreshDirectories>,
}
impl fmt::Debug for FreshVolumeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FreshVolumeFailure")
            .field("cause", &self.cause)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for FreshVolumeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fresh durable volume: {:?}", self.cause)
    }
}
impl std::error::Error for FreshVolumeFailure {}

impl Service {
    /// Creates one fresh identity and starts durable control without recovery.
    /// The caller must first persist the intent and prove the physical volume.
    /// The original directory handles address the catalog even during a rename.
    /// On failure keep this Service, Owner and returned receipt; do not replay.
    pub fn activate_fresh_volume(
        &mut self,
        owner: &Owner,
        config: FreshVolumeConfig,
    ) -> Result<(), Box<FreshVolumeFailure>> {
        let fail = |cause, directories| {
            Box::new(FreshVolumeFailure {
                cause,
                config: Box::new(config.clone()),
                _directories: directories,
            })
        };
        if self.durable.is_some()
            || self.fresh_failed
            || !self.owner.same_roster(&owner.client())
            || self.workspaces() != Some(0)
        {
            return Err(fail(
                FreshVolumeCause::Invalid("fresh volume admission"),
                None,
            ));
        }
        owner
            .vacant_namespace_floor()
            .map_err(|cause| fail(FreshVolumeCause::Owner(cause), None))?;
        if !self.store.has_durable_writer()
            || config.profile.pager_kib == 0
            || config.profile.max_pages.is_some()
        {
            self.fresh_failed = true;
            return Err(fail(
                FreshVolumeCause::Invalid("fresh volume admission"),
                None,
            ));
        }
        let directories =
            match create_fresh_directories(&config.volume, config.sandbox, config.store_device) {
                Ok(directories) => directories,
                Err(cause) => {
                    self.fresh_failed = true;
                    return Err(fail(FreshVolumeCause::Directories(cause), None));
                }
            };
        let activation = self.activate_fresh_control(
            owner,
            FreshControlConfig {
                catalog_directory: directories.catalog_descriptor_path(),
                overlay_directory: directories.overlay_descriptor_path(),
                sandbox: config.sandbox,
                profile: config.profile,
                store_device: config.store_device,
            },
        );
        match (activation, directories.verify()) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(cause), Ok(())) => {
                Err(fail(FreshVolumeCause::Activation(cause), Some(directories)))
            }
            (activation, Err(cause)) => {
                self.fresh_failed = true;
                let (activated, activation) = match activation {
                    Ok(()) => (true, None),
                    Err(error) => (false, Some(error)),
                };
                Err(fail(
                    FreshVolumeCause::Verification {
                        cause,
                        activation,
                        activated,
                    },
                    Some(directories),
                ))
            }
        }
    }
}
