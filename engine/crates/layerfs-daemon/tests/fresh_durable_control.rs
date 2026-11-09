//! Fresh initialization never adopts or overwrites an existing durable Sandbox.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{Reply, Request, UnmountControl};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{Failure, FreshControlCause, FreshControlConfig, Service},
    durable_roster::{Authority, Roster, WorkspaceRecord},
    store::Store,
    Owner, OwnerConfig,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
use layerfs_storage::ReservationBlocks;
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::PathBuf,
    process::Command,
    sync::Arc,
};

fn setup(fixture: &support::Fixture) -> (Arc<Store>, Owner, FreshControlConfig) {
    fs::set_permissions(&fixture.directory, fs::Permissions::from_mode(0o700)).unwrap();
    let catalog = fixture.directory.join("catalog");
    let overlays = fixture.directory.join("overlays");
    for directory in [&catalog, &overlays] {
        fs::create_dir(directory).unwrap();
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        1,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let owner = Owner::start(
        &fixture.directory.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 1 },
    )
    .unwrap();
    let config = FreshControlConfig {
        catalog_directory: catalog,
        overlay_directory: overlays,
        sandbox: [8; 32],
        profile: ProfileConfig::default(),
        store_device: fs::metadata(&fixture.config.path).unwrap().dev(),
    };
    (store, owner, config)
}

#[test]
fn new_catalog_uses_durable_bindings_and_cannot_be_created_twice() {
    let fixture = support::Fixture::new_durable(1, "fresh-control");
    let (store, owner, config) = setup(&fixture);
    let authority = Authority::for_catalog(config.sandbox, store.history()).unwrap();
    let mut service = Service::new(store.clone(), &owner);
    service
        .activate_fresh_control(&owner, config.clone())
        .unwrap();
    let workspace = WorkspaceId::from_authority([31; 32]).unwrap();
    let record = WorkspaceRecord {
        workspace,
        branch: fixture.branch,
    };
    let success = service
        .execute_control(&Request::Mount {
            workspace,
            branch: fixture.branch,
        })
        .unwrap();
    let Reply::Bound { token, .. } = success.reply else {
        panic!("missing binding")
    };
    assert!(config
        .overlay_directory
        .join(record.overlay_name())
        .is_file());
    assert_eq!(
        service
            .execute_control(&Request::Unmount(UnmountControl {
                token,
                discard: false
            }))
            .unwrap()
            .reply,
        Reply::Unmounted(token)
    );
    drop(service);
    let mut reopened = Roster::open(&config.catalog_directory, authority).unwrap();
    assert!(reopened.lookup(workspace).unwrap().is_none());
    drop(reopened);
    assert!(config
        .overlay_directory
        .join(record.overlay_name())
        .is_file());
    let mut another = Service::new(store.clone(), &owner);
    let error = another.activate_fresh_control(&owner, config).unwrap_err();
    assert!(matches!(
        error.cause,
        FreshControlCause::Invalid("overlay directory is not empty")
    ));
    drop(error);
    drop(another);
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}

#[test]
fn preexisting_overlay_is_preserved_and_catalog_is_not_created() {
    let fixture = support::Fixture::new_durable(1, "fresh-refusal");
    let (store, owner, config) = setup(&fixture);
    let existing = config.overlay_directory.join("original");
    fs::write(&existing, b"user data").unwrap();
    let mut service = Service::new(store.clone(), &owner);
    let error = service
        .activate_fresh_control(&owner, config.clone())
        .unwrap_err();
    assert!(matches!(
        error.cause,
        FreshControlCause::Invalid("overlay directory is not empty")
    ));
    assert_eq!(fs::read(existing).unwrap(), b"user data");
    assert_eq!(fs::read_dir(&config.catalog_directory).unwrap().count(), 0);
    assert!(matches!(
        service.execute_control(&Request::Mount {
            workspace: WorkspaceId::from_authority([37; 32]).unwrap(),
            branch: fixture.branch,
        }),
        Err(Failure::Rejected(
            layerfs_bridge::control::ControlCode::Unknown,
            _
        ))
    ));
    assert!(service.activate_fresh_control(&owner, config).is_err());
    drop(error);
    drop(service);
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}

#[test]
fn disposable_store_is_rejected_before_initializing_a_catalog() {
    let fixture = support::Fixture::new(1, "fresh-disposable");
    let (store, owner, config) = setup(&fixture);
    let mut service = Service::new(store.clone(), &owner);
    let error = service
        .activate_fresh_control(&owner, config.clone())
        .unwrap_err();
    assert!(matches!(
        error.cause,
        FreshControlCause::Invalid("fresh control admission")
    ));
    assert!(matches!(
        service.execute_control(&Request::Mount {
            workspace: WorkspaceId::from_authority([38; 32]).unwrap(),
            branch: fixture.branch,
        }),
        Err(Failure::Rejected(
            layerfs_bridge::control::ControlCode::Unknown,
            _
        ))
    ));
    assert_eq!(fs::read_dir(config.catalog_directory).unwrap().count(), 0);
    drop(error);
    drop(service);
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}

#[test]
fn different_store_device_is_rejected_without_publishing_a_catalog() {
    let fixture = support::Fixture::new_durable(1, "fresh-device");
    let (store, owner, mut config) = setup(&fixture);
    config.store_device ^= 1;
    let mut service = Service::new(store.clone(), &owner);
    let error = service
        .activate_fresh_control(&owner, config.clone())
        .unwrap_err();
    assert!(matches!(
        error.cause,
        FreshControlCause::Invalid("separate local durable directories")
    ));
    assert_eq!(fs::read_dir(config.catalog_directory).unwrap().count(), 0);
    drop(error);
    drop(service);
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}

#[test]
fn directory_sync_failure_does_not_publish_a_catalog() {
    const CHILD: &str = "LAYERFS_TEST_FRESH_SYNC_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let fixture = support::Fixture::new_durable(1, "fresh-sync");
        let (store, owner, mut config) = setup(&fixture);
        config.catalog_directory = root.join("catalog");
        config.overlay_directory = root.join("overlays");
        let mut service = Service::new(store, &owner);
        fs::write(root.join("inject"), b"1").unwrap();
        let error = service.activate_fresh_control(&owner, config).unwrap_err();
        assert!(
            matches!(error.cause, FreshControlCause::Directory(ref error) if error.raw_os_error() == Some(5))
        );
        assert_eq!(fs::read_dir(root.join("catalog")).unwrap().count(), 0);
        assert_eq!(fs::read_dir(root.join("overlays")).unwrap().count(), 0);
        fs::write(root.join("ack"), b"sync failure retained").unwrap();
        return;
    }
    let root = test_root("sync");
    let shim = compile_shim(&root, "../layerfs-overlay/tests/durable_dir_sync_failure.c");
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "directory_sync_failure_does_not_publish_a_catalog",
        ])
        .env(CHILD, &root)
        .env("TMPDIR", &root)
        .env("LD_PRELOAD", shim)
        .env("LAYERFS_TEST_DIR_SYNC_TARGET", root.join("catalog"))
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
fn catalog_swap_keeps_creation_on_the_original_directory() {
    const CHILD: &str = "LAYERFS_TEST_FRESH_SWAP_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let fixture = support::Fixture::new_durable(1, "fresh-swap");
        let (store, owner, mut config) = setup(&fixture);
        config.catalog_directory = root.join("catalog");
        config.overlay_directory = root.join("overlays");
        let authority = Authority::for_catalog(config.sandbox, store.history()).unwrap();
        let mut service = Service::new(store, &owner);
        fs::write(root.join("inject"), b"1").unwrap();
        let error = service.activate_fresh_control(&owner, config).unwrap_err();
        assert!(matches!(error.cause, FreshControlCause::Catalog(_)));
        assert_eq!(fs::read_dir(root.join("catalog")).unwrap().count(), 0);
        assert!(root.join("original/roster.owner").is_file());
        assert!(root.join("original/roster.ledger").is_file());
        assert!(Roster::open(&root.join("original"), authority).is_err());
        fs::write(root.join("ack"), b"original catalog locked").unwrap();
        return;
    }
    let root = test_root("swap");
    let shim = compile_shim(&root, "tests/durable_root_swap.c");
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "catalog_swap_keeps_creation_on_the_original_directory",
        ])
        .env(CHILD, &root)
        .env("TMPDIR", &root)
        .env("LD_PRELOAD", shim)
        .env(
            "LAYERFS_TEST_SWAP_TARGET",
            root.join("catalog/roster.ledger"),
        )
        .env("LAYERFS_TEST_SWAP_SOURCE", root.join("catalog"))
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

fn test_root(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "layerfs-fresh-control-{label}-{}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["catalog", "overlays"] {
        fs::create_dir(root.join(name)).unwrap();
        fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o700)).unwrap();
    }
    root.canonicalize().unwrap()
}

fn compile_shim(root: &std::path::Path, source: &str) -> PathBuf {
    let shim = root.join("shim.so");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(source))
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    shim
}
