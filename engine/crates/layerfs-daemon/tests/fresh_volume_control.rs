//! Fresh volume creation and catalog activation remain one owned attempt.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{ControlCode, Reply, Request, UnmountControl};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{Failure, FreshVolumeCause, FreshVolumeConfig, Service},
    durable_directories::DirectoryPhase,
    durable_roster::WorkspaceRecord,
    Owner, OwnerConfig,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
use layerfs_storage::ReservationBlocks;
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
};

fn config(volume: &Path, store: &Path) -> FreshVolumeConfig {
    FreshVolumeConfig {
        volume: volume.to_owned(),
        sandbox: [8; 32],
        profile: ProfileConfig::default(),
        store_device: fs::metadata(store).unwrap().dev(),
    }
}

#[test]
fn fresh_volume_routes_control_and_refuses_an_existing_identity() {
    let fixture = support::Fixture::new_durable(1, "fresh-volume");
    fs::set_permissions(&fixture.directory, fs::Permissions::from_mode(0o700)).unwrap();
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
    let config = config(&fixture.directory, &fixture.config.path);
    let mut service = Service::new(store.clone(), &owner);
    service
        .activate_fresh_volume(&owner, config.clone())
        .unwrap();
    let workspace = WorkspaceId::from_authority([31; 32]).unwrap();
    let record = WorkspaceRecord {
        workspace,
        branch: fixture.branch,
    };
    let Reply::Bound { token, .. } = service
        .execute_control(&Request::Mount {
            workspace,
            branch: fixture.branch,
        })
        .unwrap()
        .reply
    else {
        panic!("missing binding")
    };
    let overlay = fixture
        .directory
        .join("layerfs-durable")
        .join("08".repeat(32))
        .join("overlays")
        .join(record.overlay_name());
    assert!(overlay.is_file());
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
    let mut another = Service::new(store.clone(), &owner);
    let error = another.activate_fresh_volume(&owner, config).unwrap_err();
    assert!(
        matches!(error.cause, FreshVolumeCause::Directories(ref cause) if cause.phase == DirectoryPhase::SandboxCreate && cause.cause.kind() == std::io::ErrorKind::AlreadyExists)
    );
    assert!(matches!(
        another.execute_control(&Request::Mount {
            workspace,
            branch: fixture.branch
        }),
        Err(Failure::Rejected(ControlCode::Unknown, _))
    ));
    assert!(overlay.is_file());
    drop(error);
    drop(another);
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}

#[test]
fn swapped_ancestor_retains_both_activation_and_verification_failures() {
    const CHILD: &str = "LAYERFS_TEST_FRESH_VOLUME_SWAP_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let fixture = support::Fixture::new_durable(1, "fresh-volume-swap");
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
        let mut service = Service::new(store, &owner);
        let error = service
            .activate_fresh_volume(&owner, config(&root, &fixture.config.path))
            .unwrap_err();
        assert!(matches!(
            error.cause,
            FreshVolumeCause::Verification {
                activation: Some(_),
                activated: false,
                ..
            }
        ));
        assert!(matches!(
            service.execute_control(&Request::Mount {
                workspace: WorkspaceId::from_authority([33; 32]).unwrap(),
                branch: fixture.branch
            }),
            Err(Failure::Rejected(ControlCode::Unknown, _))
        ));
        assert_eq!(
            fs::read_dir(root.join("layerfs-durable").join("08".repeat(32)))
                .unwrap()
                .count(),
            0
        );
        assert!(root.join("original/catalog/roster.owner").is_file());
        assert!(root.join("original/catalog/roster.ledger").is_file());
        fs::write(root.join("ack"), b"original and both errors retained").unwrap();
        return;
    }
    let root =
        std::env::temp_dir().join(format!("layerfs-fresh-volume-swap-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let root = root.canonicalize().unwrap();
    let shim = root.join("shim.so");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/durable_root_swap.c"))
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    let sandbox = root.join("layerfs-durable").join("08".repeat(32));
    fs::write(root.join("inject"), b"1").unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "swapped_ancestor_retains_both_activation_and_verification_failures",
        ])
        .env(CHILD, &root)
        .env("TMPDIR", &root)
        .env("LD_PRELOAD", shim)
        .env(
            "LAYERFS_TEST_SWAP_TARGET",
            sandbox.join("catalog/roster.ledger"),
        )
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
