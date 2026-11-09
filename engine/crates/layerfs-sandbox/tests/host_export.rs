//! The validator is read-only; privileged mount setup belongs to the caller.
#![cfg(target_os = "linux")]

use layerfs_sandbox::host_export::{ExportIdentity, HostExportRoot};
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
