//! Actual protected configuration descriptors and process/placement checks.
use layerfs_bridge::{
    daemon_recovery_setup::{RecoveryDaemonSetup, RecoveryWorkspaceSetup},
    daemon_setup::{DaemonSetup, MAX_DAEMON_SETUP_BYTES},
};
use nix::{fcntl::OFlag, unistd::Uid};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::Path,
};

pub(super) struct StartupConfig {
    pub daemon: DaemonSetup,
    pub recovery: Option<RecoveryWorkspaceSetup>,
    pub recovery_anchor: Option<File>,
}

pub(super) fn read(path: &Path) -> io::Result<StartupConfig> {
    secure_process()?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags((OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC).bits())
        .open(path)?;
    let metadata = file.metadata()?;
    let uid = Uid::effective().as_raw();
    if !metadata.is_file() || metadata.uid() != uid || metadata.mode() & 0o077 != 0 {
        return Err(invalid("private config ownership/mode/type"));
    }
    private_directory(
        path.parent().ok_or_else(|| invalid("config parent"))?,
        false,
    )?;
    let limit = MAX_DAEMON_SETUP_BYTES + 1;
    let mut bytes = Vec::with_capacity(limit);
    (&mut file).take(limit as u64).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_DAEMON_SETUP_BYTES {
        return Err(invalid("private config record size"));
    }
    let (config, recovery) = if RecoveryDaemonSetup::is_encoded(&bytes) {
        let selected = RecoveryDaemonSetup::decode(&bytes)
            .map_err(|_| invalid("private recovery config fields"))?;
        (selected.daemon, Some(selected.workspace))
    } else {
        (
            DaemonSetup::decode(&bytes).map_err(|_| invalid("private config fields"))?,
            None,
        )
    };
    if config.command_uid == uid {
        return Err(invalid("command and daemon identities must differ"));
    }
    // One early reservation restores any value below the refill window; a
    // larger one would have consecutive creates each attempt a reservation.
    if config.limits.serial_low_water >= layerfs_workspace::SERIAL_REFILL {
        return Err(invalid("serial low-water must be below the refill window"));
    }
    let store = Path::new(&config.store);
    let overlay = Path::new(&config.overlay);
    let mounts = Path::new(&config.mounts);
    if store == overlay
        || store.starts_with(mounts)
        || overlay.starts_with(mounts)
        || path.starts_with(mounts)
    {
        return Err(invalid("separate protected backing and mount paths"));
    }
    private_directory(
        overlay.parent().ok_or_else(|| invalid("Overlay parent"))?,
        true,
    )?;
    private_directory(
        store.parent().ok_or_else(|| invalid("Store parent"))?,
        config.existing_store.is_none(),
    )?;
    if let Some(fresh) = &config.fresh_workspace {
        for directory in [&fresh.catalog_directory, &fresh.overlay_directory] {
            let directory = Path::new(directory);
            if path.starts_with(directory) || directory.starts_with(path) {
                return Err(invalid("private config overlaps durable Workspace"));
            }
            if !fresh.create_directories {
                private_directory(directory, false)?;
                private_directory(
                    directory
                        .parent()
                        .ok_or_else(|| invalid("durable directory parent"))?,
                    false,
                )?;
            }
        }
    }
    if let Some(manifest) = &config.existing_store {
        if manifest.daemon_sqlite.is_none() {
            return Err(invalid("existing installation metadata"));
        }
        if recovery.is_none() {
            protected_file(store)?;
        }
    }
    let recovery_anchor = if let Some(recovery) = &recovery {
        for directory in [&recovery.catalog_directory, &recovery.overlay_directory] {
            let directory = Path::new(directory);
            if path.starts_with(directory) || directory.starts_with(path) {
                return Err(invalid("private config overlaps durable Workspace"));
            }
            private_directory(directory, false)?;
            private_directory(
                directory
                    .parent()
                    .ok_or_else(|| invalid("durable directory parent"))?,
                false,
            )?;
        }
        Some(open_recovery_store(&config, recovery)?)
    } else {
        None
    };
    Ok(StartupConfig {
        daemon: config,
        recovery,
        recovery_anchor,
    })
}

pub(super) fn open_recovery_store(
    config: &DaemonSetup,
    recovery: &RecoveryWorkspaceSetup,
) -> io::Result<File> {
    let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC).bits())
        .open(&config.store)?;
    let opened = file.metadata()?;
    let named = fs::symlink_metadata(&config.store)?;
    let uid = Uid::effective().as_raw();
    if boot.trim() != recovery.boot_id
        || !opened.is_file()
        || !named.is_file()
        || opened.uid() != uid
        || named.uid() != uid
        || opened.mode() & 0o077 != 0
        || named.mode() & 0o077 != 0
        || opened.nlink() != 1
        || named.nlink() != 1
        || opened.dev() != recovery.store_device
        || named.dev() != recovery.store_device
        || opened.ino() != recovery.store_inode
        || named.ino() != recovery.store_inode
    {
        return Err(invalid("recovery Store identity or privacy changed"));
    }
    #[cfg(target_os = "linux")]
    {
        // O_PATH preserves this identity without closing another ordinary
        // descriptor after SQLite has acquired process-scoped locks. The
        // O_RDONLY open above already checked the effective read permission.
        let anchor = OpenOptions::new()
            .read(true)
            .custom_flags((OFlag::O_PATH | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC).bits())
            .open(&config.store)?;
        let anchored = anchor.metadata()?;
        if !anchored.is_file()
            || (
                anchored.dev(),
                anchored.ino(),
                anchored.uid(),
                anchored.mode(),
                anchored.nlink(),
            ) != (
                opened.dev(),
                opened.ino(),
                opened.uid(),
                opened.mode(),
                opened.nlink(),
            )
        {
            return Err(invalid("recovery Store anchor identity changed"));
        }
        drop(file);
        Ok(anchor)
    }
    #[cfg(not(target_os = "linux"))]
    Ok(file)
}

/// Repeat the original effective read-permission check before any SQLite
/// connection exists, and retain the first O_PATH identity across that check.
#[cfg(target_os = "linux")]
pub(super) fn verify_recovery_access(
    config: &DaemonSetup,
    recovery: &RecoveryWorkspaceSetup,
    anchor: &File,
) -> io::Result<()> {
    let checked = open_recovery_store(config, recovery)?;
    let first = anchor.metadata()?;
    let current = checked.metadata()?;
    if (
        first.dev(),
        first.ino(),
        first.uid(),
        first.mode(),
        first.nlink(),
    ) != (
        current.dev(),
        current.ino(),
        current.uid(),
        current.mode(),
        current.nlink(),
    ) {
        return Err(invalid("recovery Store anchor identity changed"));
    }
    Ok(())
}
pub(super) fn protected_file(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC).bits())
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != Uid::effective().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        return Err(invalid("protected file ownership/mode/type"));
    }
    Ok(())
}
fn private_directory(path: &Path, create: bool) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => (),
        Err(error) if create && error.kind() == io::ErrorKind::NotFound => {
            // Exactly one create attempt. A race/failure is not an open fallback.
            fs::DirBuilder::new().mode(0o700).create(path)?;
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
        || metadata.mode() & 0o077 != 0
    {
        return Err(invalid("private directory ownership/mode/type"));
    }
    Ok(())
}
#[cfg(target_os = "linux")]
fn secure_process() -> io::Result<()> {
    nix::sys::stat::umask(nix::sys::stat::Mode::from_bits_truncate(0o077));
    nix::sys::prctl::set_dumpable(false).map_err(io::Error::from)?;
    if nix::sys::prctl::get_dumpable().map_err(io::Error::from)? {
        return Err(invalid("daemon remains dumpable"));
    }
    Ok(())
}
#[cfg(not(target_os = "linux"))]
fn secure_process() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "production daemon requires Linux process protection",
    ))
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
