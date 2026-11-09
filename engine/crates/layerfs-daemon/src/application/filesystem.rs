//! Kernel serving assembly over the daemon's one Store and one overlay owner.
use super::{ServiceStartCause, ServiceStartFailure};
use crate::{
    control::{KernelServing, Service},
    store::Store,
    Owner,
};
use layerfs_bridge::daemon_setup::DaemonSetup;
use std::sync::Arc;

/// Control and kernel serving share the same Store handle and overlay owner;
/// the Service's registry stays the sole authority for every mount. The
/// Store takes its explicit serial low-water here, before any Workspace.
pub(super) fn service(
    store: Arc<Store>,
    owner: &Owner,
    kernel: Option<&Arc<KernelServing>>,
    setup: &DaemonSetup,
) -> Result<Arc<Service>, Box<ServiceStartFailure>> {
    store.set_serial_low_water(setup.limits.serial_low_water);
    let mut service = match kernel {
        Some(kernel) => Service::with_kernel(store, owner, kernel.clone()),
        None => Service::new(store, owner),
    };
    if let Err(cause) = configure(&mut service, owner, setup) {
        return Err(Box::new(ServiceStartFailure {
            cause,
            service: Box::new(service),
            opened: None,
            diagnostic_error: None,
        }));
    }
    Ok(Arc::new(service))
}

#[cfg(target_os = "linux")]
fn configure(
    service: &mut Service,
    owner: &Owner,
    setup: &DaemonSetup,
) -> Result<(), ServiceStartCause> {
    use std::os::unix::fs::MetadataExt;
    let Some(fresh) = &setup.fresh_workspace else {
        return Ok(());
    };
    let metadata = std::fs::symlink_metadata(&setup.store).map_err(ServiceStartCause::Io)?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        return Err(ServiceStartCause::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "durable Store identity or privacy",
        )));
    }
    let profile = layerfs_overlay::ProfileConfig {
        pager_kib: setup.limits.pager_kib,
        max_pages: None,
    };
    let creation_volume = fresh.creation_volume().map_err(|cause| {
        ServiceStartCause::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, cause))
    })?;
    if let Some(volume) = creation_volume {
        return service
            .activate_fresh_volume(
                owner,
                crate::control::FreshVolumeConfig {
                    volume,
                    sandbox: fresh.sandbox,
                    profile,
                    store_device: metadata.dev(),
                },
            )
            .map_err(ServiceStartCause::FreshVolume);
    }
    service
        .activate_fresh_control(
            owner,
            crate::control::FreshControlConfig {
                catalog_directory: fresh.catalog_directory.clone().into(),
                overlay_directory: fresh.overlay_directory.clone().into(),
                sandbox: fresh.sandbox,
                profile,
                store_device: metadata.dev(),
            },
        )
        .map_err(ServiceStartCause::Fresh)
}

#[cfg(not(target_os = "linux"))]
fn configure(
    _service: &mut Service,
    _owner: &Owner,
    setup: &DaemonSetup,
) -> Result<(), ServiceStartCause> {
    if setup.fresh_workspace.is_some() {
        Err(ServiceStartCause::Unsupported)
    } else {
        Ok(())
    }
}
/// Kernel serving is a Linux capability; elsewhere control alone is assembled.
#[cfg(not(target_os = "linux"))]
pub(super) fn assemble(
    _setup: &layerfs_bridge::daemon_setup::DaemonSetup,
) -> std::io::Result<Option<Arc<KernelServing>>> {
    Ok(None)
}
#[cfg(target_os = "linux")]
pub(super) use linux::assemble;
#[cfg(target_os = "linux")]
mod linux {
    use crate::control::{ContainerExportRoot, KernelConfig, KernelServing};
    use layerfs_bridge::daemon_setup::DaemonSetup;
    use nix::{
        fcntl::OFlag,
        unistd::{Gid, Uid},
    };
    use std::{
        fs::{self, OpenOptions, Permissions},
        io::{self, Write},
        os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        path::Path,
        sync::Arc,
        time::Duration,
    };

    /// Observation deadline for one Attach's all-loop serving evidence.
    const READY_WAIT: Duration = Duration::from_secs(5);
    /// Observation deadline for one unmount's loop joins and request drain.
    /// Expiry keeps the exact owners Retained; it cancels and drops nothing.
    const DRAIN_WAIT: Duration = Duration::from_secs(5);

    /// Starts the fixed shared workers once, before control readiness. No
    /// device is opened and nothing is mounted until an explicit Attach.
    pub(in crate::application) fn assemble(
        setup: &DaemonSetup,
        host_export: Option<ContainerExportRoot>,
    ) -> io::Result<Option<Arc<KernelServing>>> {
        let mounts = Path::new(&setup.mounts);
        if let Some(export) = &host_export {
            export.verify()?;
        }
        device()?;
        match &host_export {
            Some(export) => export.verify()?,
            None => home(mounts)?,
        }
        // Beside the Overlay file, in the directory configuration checked
        // private, so no command identity can reach another connection.
        // Best effort: where the mount is refused (a user namespace refuses
        // it, though not FUSE mounts) the shared tree, unmounted there, binds
        // nothing and forced unmount is refused before any effect, as it was
        // before this mount existed. The refusal is written once to stderr.
        let overlay = Path::new(&setup.overlay).parent();
        let private = overlay.ok_or_else(|| invalid("Overlay parent"))?;
        let control = match layerfs_fuse::mount::control_root(&private.join("fusectl")) {
            Ok(control) => control,
            Err(cause) => {
                let _ = writeln!(io::stderr().lock(), "LAYERFS_DAEMON_ABORT_UNBOUND {cause}");
                layerfs_fuse::mount::CONTROL.into()
            }
        };
        let config = KernelConfig {
            mounts: mounts.to_owned(),
            owner_uid: Uid::effective().as_raw(),
            owner_gid: Gid::effective().as_raw(),
            command_uid: setup.command_uid,
            command_gid: setup.command_gid,
            ready_wait: READY_WAIT,
            drain_wait: DRAIN_WAIT,
            control,
        };
        match host_export {
            Some(export) => KernelServing::start_with_export(config, export),
            None => KernelServing::start(config),
        }
        .map(|serving| Some(Arc::new(serving)))
        .map_err(|failure| io::Error::other(failure.to_string()))
    }
    /// The command identity must never open the FUSE device. The deployment
    /// supplies the daemon's own device node; one attempt narrows it to its
    /// owner before control readiness. A node the daemon does not own, or one
    /// that stays reachable, refuses startup rather than serving unprotected.
    fn device() -> io::Result<()> {
        let path = Path::new("/dev/fuse");
        let protected = |metadata: &fs::Metadata| {
            metadata.file_type().is_char_device() && metadata.uid() == Uid::effective().as_raw()
        };
        let metadata = fs::symlink_metadata(path)?;
        if !protected(&metadata) {
            return Err(invalid("FUSE device ownership/type"));
        }
        if metadata.mode() & 0o077 != 0 {
            fs::set_permissions(path, Permissions::from_mode(0o600))?;
            let narrowed = fs::symlink_metadata(path)?;
            if !protected(&narrowed) || narrowed.mode() & 0o077 != 0 {
                return Err(invalid("FUSE device remains reachable"));
            }
        }
        Ok(())
    }
    /// The mount home is daemon-owned, writable by the daemon alone and
    /// traversable by the command identity. One create attempt; an existing
    /// path is checked, never repaired.
    fn home(path: &Path) -> io::Result<()> {
        if !path.is_absolute() {
            return Err(invalid("mount home must be absolute"));
        }
        match fs::symlink_metadata(path) {
            Ok(_) => (),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::DirBuilder::new().mode(0o755).create(path)?;
                // The protected process umask narrows the create mode.
                fs::set_permissions(path, Permissions::from_mode(0o755))?;
            }
            Err(error) => return Err(error),
        }
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags((OFlag::O_NOFOLLOW | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC).bits())
            .open(path)?;
        let metadata = directory.metadata()?;
        if !metadata.is_dir()
            || metadata.uid() != Uid::effective().as_raw()
            || metadata.mode() & 0o022 != 0
            || metadata.mode() & 0o005 != 0o005
        {
            return Err(invalid("mount home ownership/mode/type"));
        }
        Ok(())
    }
    fn invalid(message: &'static str) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, message)
    }
}
