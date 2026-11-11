//! The validator is read-only; privileged mount setup belongs to the caller.
#![cfg(target_os = "linux")]

use layerfs_sandbox::{
    export_lease::HostExportLease,
    host_export::{ExportIdentity, HostExportRoot},
};
use std::path::Path;

#[test]
fn refuses_relative_root_and_symlink_paths() {
    for path in ["relative/export", "/", "/proc/self"] {
        assert!(
            HostExportRoot::open_fresh(Path::new(path)).is_err(),
            "accepted {path}"
        );
    }
}

#[test]
fn refuses_a_path_under_a_world_writable_ancestor() {
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        time::{SystemTime, UNIX_EPOCH},
    };
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("layerfs-export-{}-{id}", std::process::id()));
    fs::create_dir(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o777)).unwrap();
    let refused = HostExportRoot::open_fresh(&path).is_err();
    fs::remove_dir(&path).unwrap();
    assert!(refused);
}

#[test]
#[ignore = "requires an explicitly provisioned root-owned, empty, isolated shared mount"]
fn validates_exact_owned_shared_mount() {
    let path = std::env::var_os("LAYERFS_EXPORT_ROOT").expect("owned export root");
    let path = Path::new(&path);
    let guard = HostExportRoot::open_fresh(path).expect("fresh isolated shared root");
    assert_eq!(guard.path(), path);
    assert_ne!(guard.identity().mount_id, 0);
    assert_ne!(guard.identity().shared_peer, 0);
    guard.verify().unwrap();
    let identity = guard.identity().clone();
    HostExportRoot::reopen(path, &identity)
        .unwrap()
        .verify()
        .unwrap();
    let altered = ExportIdentity {
        inode: identity.inode.wrapping_add(1),
        ..identity
    };
    assert!(HostExportRoot::reopen(path, &altered).is_err());
}

#[test]
#[ignore = "requires a caller-owned empty shared root for a cross-process lock"]
fn live_export_lease_excludes_another_process() {
    let path = std::env::var_os("LAYERFS_EXPORT_ROOT").expect("owned export root");
    let root = HostExportRoot::open_fresh(Path::new(&path)).unwrap();
    if std::env::var_os("LAYERFS_EXPORT_LEASE_CHILD").is_some() {
        let error = HostExportLease::try_acquire(root).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        return;
    }
    let lease = HostExportLease::try_acquire(root).unwrap();
    lease.verify().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .env("LAYERFS_EXPORT_LEASE_CHILD", "1")
        .arg("--exact")
        .arg("live_export_lease_excludes_another_process")
        .arg("--ignored")
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    drop(lease);
    let root = HostExportRoot::open_fresh(Path::new(&path)).unwrap();
    HostExportLease::try_acquire(root)
        .unwrap()
        .verify()
        .unwrap();
}

#[test]
#[ignore = "requires root and a caller-owned empty shared export"]
fn lease_rechecks_freshness_after_the_original_observation() {
    use std::{fs::OpenOptions, io::Write};
    let path = std::env::var_os("LAYERFS_EXPORT_ROOT").expect("owned export root");
    let root = HostExportRoot::open_fresh(Path::new(&path)).unwrap();
    let marker = Path::new(&path).join(format!("owned-marker-{}", std::process::id()));
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
        .unwrap()
        .write_all(b"owned fixture")
        .unwrap();
    assert!(HostExportLease::try_acquire(root).is_err());
    std::fs::remove_file(marker).unwrap();
}

#[test]
#[ignore = "requires root and a caller-owned empty shared export"]
fn reattach_keeps_nonempty_export_and_excludes_fresh_creation() {
    use std::fs;
    let path = std::env::var_os("LAYERFS_EXPORT_ROOT").expect("owned export root");
    let path = Path::new(&path);
    let root = HostExportRoot::open_fresh(path).unwrap();
    let identity = root.identity().clone();
    let original = HostExportLease::try_acquire(root).unwrap();
    let second = HostExportRoot::reopen(path, &identity).unwrap();
    assert!(HostExportLease::reattach(second).is_err());
    let marker = path.join(format!("reattach-owned-marker-{}", std::process::id()));
    fs::write(&marker, b"original persistent mount contents").unwrap();
    drop(original);
    assert!(HostExportRoot::open_fresh(path).is_err());
    let recovered =
        HostExportLease::reattach(HostExportRoot::reopen(path, &identity).unwrap()).unwrap();
    recovered.verify().unwrap();
    assert_eq!(
        fs::read(&marker).unwrap(),
        b"original persistent mount contents"
    );
    let second = HostExportRoot::reopen(path, &identity).unwrap();
    assert!(HostExportLease::reattach(second).is_err());
    fs::remove_file(marker).unwrap();
}

#[test]
#[ignore = "requires root and a caller-owned group-restricted export"]
fn held_lease_refuses_permission_widening() {
    use std::{fs, os::unix::fs::PermissionsExt};
    let path = std::env::var_os("LAYERFS_EXPORT_ROOT").expect("owned export root");
    let root = HostExportRoot::open_fresh(Path::new(&path)).unwrap();
    let lease = HostExportLease::try_acquire(root).unwrap();
    let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode & 0o007, 0);
    fs::set_permissions(&path, fs::Permissions::from_mode(mode | 0o005)).unwrap();
    let refused = lease.verify().is_err();
    fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
    assert!(refused, "held export must refuse newly granted access");
}

#[test]
#[ignore = "requires a separate root-owned test mount in a parent propagation group"]
fn refuses_parent_propagation_group() {
    let path = std::env::var_os("LAYERFS_EXPORT_BAD_GROUP").expect("owned bad-group root");
    assert!(HostExportRoot::open_fresh(Path::new(&path)).is_err());
}

#[test]
#[ignore = "requires a separate root-owned test mount containing a file"]
fn refuses_nonempty_fresh_root() {
    let path = std::env::var_os("LAYERFS_EXPORT_NONEMPTY").expect("owned nonempty root");
    assert!(HostExportRoot::open_fresh(Path::new(&path)).is_err());
}

#[test]
#[ignore = "requires a dedicated shared root and an external mount-change controller"]
fn refuses_propagation_change_while_root_is_held() {
    use std::{
        fs, thread,
        time::{Duration, Instant},
    };
    let path = std::env::var_os("LAYERFS_EXPORT_MUTABLE").expect("owned mutable root");
    let proof =
        std::path::PathBuf::from(std::env::var_os("LAYERFS_EXPORT_PROOF").expect("owned proof"));
    let guard = HostExportRoot::open_fresh(Path::new(&path)).unwrap();
    fs::write(proof.join("guard-ready"), b"ready\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !proof.join("guard-changed").exists() {
        assert!(
            Instant::now() < deadline,
            "controller did not change the owned mount"
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        guard.verify().is_err(),
        "changed propagation must invalidate the held root"
    );
}

#[test]
#[ignore = "requires a dedicated shared root and an external ancestor-mode controller"]
fn refuses_ancestor_mode_change_while_root_is_held() {
    use std::{
        fs, thread,
        time::{Duration, Instant},
    };
    let path = std::env::var_os("LAYERFS_EXPORT_MUTABLE").expect("owned mutable root");
    let proof =
        std::path::PathBuf::from(std::env::var_os("LAYERFS_EXPORT_PROOF").expect("owned proof"));
    let guard = HostExportRoot::open_fresh(Path::new(&path)).unwrap();
    fs::write(proof.join("ancestor-ready"), b"ready\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !proof.join("ancestor-changed").exists() {
        assert!(
            Instant::now() < deadline,
            "controller did not change the ancestor"
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        guard.verify().is_err(),
        "writable ancestor must invalidate the held root"
    );
}
