//! One-shot volume directory creation preserves old or uncertain data.
#![cfg(target_os = "linux")]

use layerfs_daemon::durable_directories::{create_fresh_directories, DirectoryPhase};
use std::{
    fs,
    io::ErrorKind,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Barrier},
};

fn root(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "layerfs-durable-directories-{label}-{}",
        std::process::id()
    ));
    fs::create_dir(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path.canonicalize().unwrap()
}
fn name(identity: u8) -> String {
    format!("{identity:02x}").repeat(32)
}

#[test]
fn new_identities_are_private_and_existing_identity_is_never_reused() {
    let root = root("distinct");
    let device = fs::metadata(&root).unwrap().dev();
    let first = create_fresh_directories(&root, [11; 32], device).unwrap();
    let second = create_fresh_directories(&root, [12; 32], device).unwrap();
    first.verify().unwrap();
    second.verify().unwrap();
    assert_ne!(first.overlay_path(), second.overlay_path());
    assert_eq!(
        fs::metadata(first.catalog_path()).unwrap().mode() & 0o777,
        0o700
    );
    let original = first.overlay_path().join("original");
    fs::write(&original, b"user data").unwrap();
    let failure = match create_fresh_directories(&root, [11; 32], device) {
        Ok(_) => panic!("old Sandbox was adopted"),
        Err(failure) => failure,
    };
    assert_eq!(failure.phase, DirectoryPhase::SandboxCreate);
    assert_eq!(failure.cause.kind(), ErrorKind::AlreadyExists);
    assert!(!failure.sandbox_created);
    assert_eq!(fs::read(original).unwrap(), b"user data");
    drop(failure);
    drop(first);
    drop(second);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn simultaneous_creators_cannot_adopt_one_sandbox_identity() {
    let root = root("contended");
    let device = fs::metadata(&root).unwrap().dev();
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let root = root.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                create_fresh_directories(&root, [13; 32], device)
            })
        })
        .collect();
    let mut admitted = Vec::new();
    let mut refused = 0;
    for thread in threads {
        match thread.join().unwrap() {
            Ok(owner) => admitted.push(owner),
            Err(failure) => {
                assert_eq!(failure.phase, DirectoryPhase::SandboxCreate);
                assert_eq!(failure.cause.kind(), ErrorKind::AlreadyExists);
                refused += 1;
            }
        }
    }
    assert_eq!(admitted.len(), 1);
    assert_eq!(refused, 7);
    admitted[0].verify().unwrap();
    drop(admitted);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn mismatched_device_and_public_writable_volume_are_rejected() {
    let root = root("invalid");
    let device = fs::metadata(&root).unwrap().dev();
    let error = match create_fresh_directories(&root, [14; 32], device ^ 1) {
        Ok(_) => panic!("different device accepted"),
        Err(error) => error,
    };
    assert_eq!(error.phase, DirectoryPhase::OpenRoot);
    assert!(!root.join("layerfs-durable").exists());
    let alias = root.with_extension("alias");
    std::os::unix::fs::symlink(&root, &alias).unwrap();
    assert!(create_fresh_directories(&alias, [14; 32], device).is_err());
    fs::remove_file(alias).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o777)).unwrap();
    assert!(create_fresh_directories(&root, [14; 32], device).is_err());
    assert!(!root.join("layerfs-durable").exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_directory_sync_keeps_the_original_created_directories() {
    const CHILD: &str = "LAYERFS_TEST_VOLUME_SYNC_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let device = fs::metadata(&root).unwrap().dev();
        let error = match create_fresh_directories(&root, [15; 32], device) {
            Ok(_) => panic!("injected sync succeeded"),
            Err(error) => error,
        };
        assert_eq!(error.phase, DirectoryPhase::DirectoriesSync);
        assert_eq!(error.cause.raw_os_error(), Some(5));
        assert!(error.sandbox_created && error.catalog_created && error.overlay_created);
        let original = root.join("layerfs-durable").join(name(15));
        assert!(original.join("catalog").is_dir() && original.join("overlays").is_dir());
        let retry = match create_fresh_directories(&root, [15; 32], device) {
            Ok(_) => panic!("uncertain creation was adopted"),
            Err(error) => error,
        };
        assert_eq!(retry.cause.kind(), ErrorKind::AlreadyExists);
        fs::write(root.join("ack"), b"original retained").unwrap();
        return;
    }
    let root = root("sync");
    let shim = compile_shim(&root, "../layerfs-overlay/tests/durable_dir_sync_failure.c");
    fs::write(root.join("inject"), b"1").unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "failed_directory_sync_keeps_the_original_created_directories",
        ])
        .env(CHILD, &root)
        .env("LD_PRELOAD", shim)
        .env(
            "LAYERFS_TEST_DIR_SYNC_TARGET",
            root.join("layerfs-durable").join(name(15)),
        )
        .env("LAYERFS_TEST_DIR_SYNC_TRIGGER", root.join("inject"))
        .env("LAYERFS_TEST_DIR_SYNC_RECEIPT", root.join("failed"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fs::read(root.join("failed")).unwrap(), b"EIO\n");
    assert!(root.join("ack").is_file());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn ancestor_swap_never_redirects_created_directories() {
    const CHILD: &str = "LAYERFS_TEST_VOLUME_SWAP_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let device = fs::metadata(&root).unwrap().dev();
        let error = match create_fresh_directories(&root, [16; 32], device) {
            Ok(_) => panic!("replaced Sandbox directory accepted"),
            Err(error) => error,
        };
        assert_eq!(error.phase, DirectoryPhase::Verify);
        assert!(error.sandbox_created && error.catalog_created && error.overlay_created);
        assert_eq!(
            fs::read_dir(root.join("layerfs-durable").join(name(16)))
                .unwrap()
                .count(),
            0
        );
        assert!(root.join("original/catalog").is_dir());
        assert!(root.join("original/overlays").is_dir());
        fs::write(root.join("ack"), b"original retained").unwrap();
        return;
    }
    let root = root("swap");
    let shim = compile_shim(&root, "tests/durable_root_swap.c");
    let sandbox = root.join("layerfs-durable").join(name(16));
    fs::write(root.join("inject"), b"1").unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "ancestor_swap_never_redirects_created_directories",
        ])
        .env(CHILD, &root)
        .env("LD_PRELOAD", shim)
        .env("LAYERFS_TEST_SWAP_TARGET", sandbox.join("catalog"))
        .env("LAYERFS_TEST_SWAP_SOURCE", &sandbox)
        .env("LAYERFS_TEST_SWAP_BACKUP", root.join("original"))
        .env("LAYERFS_TEST_SWAP_TRIGGER", root.join("inject"))
        .env("LAYERFS_TEST_SWAP_RECEIPT", root.join("swapped"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fs::read(root.join("swapped")).unwrap(), b"swapped\n");
    assert!(root.join("ack").is_file());
    fs::remove_dir_all(root).unwrap();
}

fn compile_shim(root: &Path, source: &str) -> PathBuf {
    let shim = root.join("shim.so");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join(source))
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    shim
}
