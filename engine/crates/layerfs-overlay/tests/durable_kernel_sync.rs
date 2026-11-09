//! The overlay barrier fences exact descriptors and retains uncertain owners.
#![cfg(target_os = "linux")]
use layerfs_overlay::{
    Inode, InodeKind, KernelDecision, KernelMount, Overlay, OverlayError, ProfileConfig,
};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

const ID: [u8; 32] = [63; 32];
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "layerfs-durable-kernel-sync-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
fn inode(serial: u64, kind: InodeKind) -> Inode {
    Inode {
        serial,
        kind,
        mode: 0o755,
        mtime_seconds: 0,
        mtime_nanoseconds: 0,
        nlink: 1,
        size: 0,
        inherited_cutoff: 0,
        born: 0,
        entries: 0,
        subdirs: 0,
    }
}
fn attach(db: &Overlay) -> (KernelMount, u64, u64) {
    db.open_workspace([7; 32]).unwrap();
    let mount = db.create_kernel_mount(1).unwrap();
    let found = db.observe_kernel_visit(mount, 1, None, true, |_, _| {
        Ok(KernelDecision::Finished {
            inode: Some(inode(2, InodeKind::File)),
            value: (),
        })
    });
    found.result.unwrap();
    let file = db.open_kernel_visit(mount, 10, 2, true, |_, _| {
        Ok(KernelDecision::Finished {
            inode: Some(inode(2, InodeKind::File)),
            value: (),
        })
    });
    file.result.unwrap();
    let directory = db.opendir_kernel_visit(mount, 11, 1, |_, _| {
        Ok(KernelDecision::Finished {
            inode: Some(inode(1, InodeKind::Directory)),
            value: (),
        })
    });
    directory.result.unwrap();
    (
        mount,
        file.open_candidate.unwrap().owner_id(),
        directory.directory_candidate.unwrap().owner_id(),
    )
}

#[test]
fn barrier_accepts_only_its_own_live_file_or_directory() {
    let fixture = Fixture::new();
    let path = fixture.0.join("overlay.sqlite");
    let db = Overlay::create_durable(&path, ProfileConfig::default(), ID).unwrap();
    let (mount, file, dir) = attach(&db);
    db.synchronize_kernel(mount, 2, file, false).unwrap();
    db.synchronize_kernel(mount, 1, dir, true).unwrap();
    let nested = db.observe_kernel_visit(mount, 2, Some(file), false, |_, _| {
        assert!(matches!(
            db.synchronize_kernel(mount, 2, file, false),
            Err(OverlayError::Invalid("synchronization inside atomic job"))
        ));
        Ok(KernelDecision::Finished {
            inode: None,
            value: (),
        })
    });
    nested.result.unwrap();
    assert!(matches!(
        db.synchronize_kernel(mount, 1, file, false),
        Err(OverlayError::Stale)
    ));
    assert!(matches!(
        db.synchronize_kernel(mount, 2, file, true),
        Err(OverlayError::Stale)
    ));
    assert!(matches!(
        db.synchronize_kernel(mount, 1, dir, false),
        Err(OverlayError::Stale)
    ));
    db.kernel_close_file(mount, 2, file).unwrap();
    assert!(matches!(
        db.synchronize_kernel(mount, 2, file, false),
        Err(OverlayError::Stale)
    ));
    db.close_kernel_directory(mount, 1, dir).unwrap();
    db.revoke_kernel_mount(mount).unwrap();
    assert!(matches!(
        db.synchronize_kernel(mount, 1, dir, true),
        Err(OverlayError::Stale)
    ));
    db.close_database().unwrap();
}

#[test]
fn disposable_overlay_cannot_claim_a_durable_barrier() {
    let fixture = Fixture::new();
    let db = Overlay::create(&fixture.0.join("overlay.sqlite"), ProfileConfig::default()).unwrap();
    let (mount, file, _) = attach(&db);
    assert!(matches!(
        db.synchronize_kernel(mount, 2, file, false),
        Err(OverlayError::Invalid("durable overlay required"))
    ));
    db.revoke_kernel_mount(mount).unwrap();
    db.close_database().unwrap();
}

#[test]
fn failed_barrier_retains_connection_and_lease() {
    const CHILD: &str = "LAYERFS_OVERLAY_SYNC_BARRIER_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let path = directory.join("overlay.sqlite");
        let db = Overlay::create_durable(&path, ProfileConfig::default(), ID).unwrap();
        let (mount, file, _) = attach(&db);
        fs::write(directory.join("inject"), b"1").unwrap();
        assert!(matches!(
            db.synchronize_kernel(mount, 2, file, false),
            Err(OverlayError::Uncertain { .. })
        ));
        assert!(matches!(
            db.synchronize_kernel(mount, 2, file, false),
            Err(OverlayError::Quarantined)
        ));
        drop(db);
        assert!(Overlay::recover_durable(&path, ProfileConfig::default(), ID).is_err());
        return;
    }
    let fixture = Fixture::new();
    let shim = fixture.0.join("failure.so");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/durable_dir_sync_failure.c"))
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    for mode in ["file", "directory"] {
        let directory = fixture.0.join(mode);
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let canonical = fs::canonicalize(&directory).unwrap();
        let target = if mode == "file" {
            canonical.join("overlay.sqlite")
        } else {
            canonical
        };
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "failed_barrier_retains_connection_and_lease"])
            .env(CHILD, &directory)
            .env("LD_PRELOAD", &shim)
            .env("LAYERFS_TEST_DIR_SYNC_TRIGGER", directory.join("inject"))
            .env("LAYERFS_TEST_DIR_SYNC_TARGET", target)
            .env("LAYERFS_TEST_DIR_SYNC_RECEIPT", directory.join("failed"))
            .status()
            .unwrap();
        assert!(status.success());
        assert!(!fs::read(directory.join("failed")).unwrap().is_empty());
        let db = Overlay::recover_durable(
            &directory.join("overlay.sqlite"),
            ProfileConfig::default(),
            ID,
        )
        .unwrap();
        db.close_database().unwrap();
    }
}
