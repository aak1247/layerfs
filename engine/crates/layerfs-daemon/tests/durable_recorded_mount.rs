//! Catalog-backed creation reserves capacity before writing any intent.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{ControlCode, Reply, Request, UnmountControl, WorkspaceToken};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{Failure, RecordedMountFailure, RecordedMountRequest, Service},
    durable_roster::{Authority, Roster, RosterError, WorkspaceRecord},
    Owner, OwnerConfig, OwnerError,
};
use layerfs_history::{BranchId, WorkspaceId};
use layerfs_overlay::ProfileConfig;
use layerfs_storage::ReservationBlocks;
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf, process::Command};

fn request(identity: u8, branch: BranchId) -> RecordedMountRequest {
    RecordedMountRequest {
        sandbox: [8; 32],
        profile: ProfileConfig::default(),
        workspace: WorkspaceId::from_authority([identity; 32]).unwrap(),
        branch,
    }
}
fn token(reply: Reply) -> WorkspaceToken {
    match reply {
        Reply::Bound { token, .. } => token,
        other => panic!("expected binding: {other:?}"),
    }
}

#[test]
fn creation_admission_checks_both_capacity_owners_before_recording() {
    let fixture = support::Fixture::new(1, "recorded-mount-capacity");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        2,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let catalog = fixture.directory.join("catalog");
    let overlays = fixture.directory.join("durable");
    for path in [&catalog, &overlays] {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::create(&catalog, authority).unwrap();
    let owner = Owner::start(
        &fixture.directory.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let service = Service::new(store.clone(), &owner);

    let missing = service
        .mount_recorded(
            &mut discovery,
            &owner,
            &overlays,
            request(1, BranchId::from_authority([99; 16])),
        )
        .unwrap_err();
    assert!(matches!(*missing, RecordedMountFailure::MissingBranch(_)));
    assert_eq!(service.workspaces(), Some(0));
    let first = token(
        service
            .mount_recorded(
                &mut discovery,
                &owner,
                &overlays,
                request(1, fixture.branch),
            )
            .unwrap()
            .reply,
    );
    assert_eq!(first.namespace, 1);

    let held = owner.reserve_durable([90; 32]).unwrap();
    assert_eq!(held.namespace(), 2);
    let refused = service
        .mount_recorded(
            &mut discovery,
            &owner,
            &overlays,
            request(2, fixture.branch),
        )
        .unwrap_err();
    assert!(matches!(
        *refused,
        RecordedMountFailure::Reservation(OwnerError::AdmissionFull)
    ));
    assert_eq!(service.workspaces(), Some(1));
    assert_eq!(
        discovery
            .lookup(request(2, fixture.branch).workspace)
            .unwrap(),
        None
    );
    held.release().unwrap();

    let second = token(
        service
            .mount_recorded(
                &mut discovery,
                &owner,
                &overlays,
                request(2, fixture.branch),
            )
            .unwrap()
            .reply,
    );
    assert_eq!(second.namespace, 3);
    let full = service
        .mount_recorded(
            &mut discovery,
            &owner,
            &overlays,
            request(3, fixture.branch),
        )
        .unwrap_err();
    assert!(matches!(
        *full,
        RecordedMountFailure::Before(Failure::Rejected(ControlCode::Capacity, _))
    ));
    assert_eq!(
        discovery
            .lookup(request(3, fixture.branch).workspace)
            .unwrap(),
        None
    );
    assert!(matches!(
        service
            .execute_control(&Request::Status(second))
            .unwrap()
            .reply,
        Reply::Status(_)
    ));

    assert_eq!(
        service
            .unmount_recorded(
                &mut discovery,
                UnmountControl {
                    token: first,
                    discard: false
                }
            )
            .unwrap()
            .reply,
        Reply::Unmounted(first)
    );
    let duplicate = service
        .mount_recorded(
            &mut discovery,
            &owner,
            &overlays,
            request(1, fixture.branch),
        )
        .unwrap_err();
    assert!(matches!(
        *duplicate,
        RecordedMountFailure::Discovery(RosterError::AlreadyExists)
    ));
    assert_eq!(service.workspaces(), Some(1));
    let third = token(
        service
            .mount_recorded(
                &mut discovery,
                &owner,
                &overlays,
                request(3, fixture.branch),
            )
            .unwrap()
            .reply,
    );
    assert_eq!(third.namespace, 5);
    for token in [second, third] {
        assert_eq!(
            service
                .unmount_recorded(
                    &mut discovery,
                    UnmountControl {
                        token,
                        discard: false
                    }
                )
                .unwrap()
                .reply,
            Reply::Unmounted(token)
        );
    }
    let mut active = Vec::new();
    discovery.visit(|record| active.push(record)).unwrap();
    assert!(active.is_empty());
    drop(service);
    owner.stop().unwrap();
    drop(discovery);
    drop(store);
    fixture.cleanup();
}

#[test]
fn failed_overlay_creation_keeps_record_slot_and_existing_file() {
    const CHILD: &str = "LAYERFS_RECORDED_MOUNT_FAILURE_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let fixture = support::Fixture::new(1, "recorded-mount-failure");
        fs::write(
            root.join("fixture-path"),
            fixture.directory.to_str().unwrap(),
        )
        .unwrap();
        let store = open_store(
            fixture.config.clone(),
            support::BINDING,
            support::CURSOR,
            1,
            2 * 1024 * 1024,
            ReservationBlocks::default(),
        )
        .unwrap();
        let catalog = fixture.directory.join("catalog");
        let overlays = fixture.directory.join("durable");
        for path in [&catalog, &overlays] {
            fs::create_dir(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
        let mut discovery = Roster::create(&catalog, authority).unwrap();
        let owner = Owner::start(
            &fixture.directory.join("template.sqlite"),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 1 },
        )
        .unwrap();
        let service = Service::new(store, &owner);
        let record = WorkspaceRecord {
            workspace: request(7, fixture.branch).workspace,
            branch: fixture.branch,
        };
        let path = overlays.join(record.overlay_name());
        fs::write(&path, b"original user data").unwrap();
        let failure = service
            .mount_recorded(
                &mut discovery,
                &owner,
                &overlays,
                request(7, fixture.branch),
            )
            .unwrap_err();
        match *failure {
            RecordedMountFailure::Creation {
                record: saved,
                failure,
                ..
            } => {
                assert_eq!(saved, record);
                assert_eq!(failure.custody.namespace(), 1);
                assert_eq!(failure.custody.identity(), [7; 32]);
            }
            other => panic!("expected creation custody: {other:?}"),
        }
        assert_eq!(service.workspaces(), Some(1));
        assert!(matches!(
            owner.reserve_durable([8; 32]),
            Err(OwnerError::AdmissionFull)
        ));
        assert_eq!(discovery.lookup(record.workspace).unwrap(), Some(record));
        assert_eq!(fs::read(path).unwrap(), b"original user data");
        return;
    }
    let root = std::env::temp_dir().join(format!(
        "layerfs-recorded-mount-failure-{}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "failed_overlay_creation_keeps_record_slot_and_existing_file",
        ])
        .env(CHILD, &root)
        .env("TMPDIR", &root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let fixture = PathBuf::from(fs::read_to_string(root.join("fixture-path")).unwrap());
    let record = WorkspaceRecord {
        workspace: request(7, BranchId::from_authority([24; 16])).workspace,
        branch: BranchId::from_authority([24; 16]),
    };
    assert_eq!(
        fs::read(fixture.join("durable").join(record.overlay_name())).unwrap(),
        b"original user data"
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn uncertain_discovery_sync_retains_reservation_before_overlay_creation() {
    const CHILD: &str = "LAYERFS_RECORDED_MOUNT_SYNC_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let fixture = support::Fixture::new(1, "recorded-mount-sync");
        fs::write(
            root.join("fixture-path"),
            fixture.directory.to_str().unwrap(),
        )
        .unwrap();
        let store = open_store(
            fixture.config.clone(),
            support::BINDING,
            support::CURSOR,
            1,
            2 * 1024 * 1024,
            ReservationBlocks::default(),
        )
        .unwrap();
        let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
        fs::write(root.join("authority"), authority.store).unwrap();
        let mut discovery = Roster::create(&root.join("catalog"), authority).unwrap();
        let overlays = fixture.directory.join("durable");
        fs::create_dir(&overlays).unwrap();
        fs::set_permissions(&overlays, fs::Permissions::from_mode(0o700)).unwrap();
        let owner = Owner::start(
            &fixture.directory.join("template.sqlite"),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 1 },
        )
        .unwrap();
        let service = Service::new(store, &owner);
        let record = WorkspaceRecord {
            workspace: request(8, fixture.branch).workspace,
            branch: fixture.branch,
        };
        fs::write(root.join("inject"), b"1").unwrap();
        let failure = service
            .mount_recorded(
                &mut discovery,
                &owner,
                &overlays,
                request(8, fixture.branch),
            )
            .unwrap_err();
        assert!(
            matches!(*failure, RecordedMountFailure::Intent { record: saved, cause: RosterError::Uncertain(_), ref reservation, .. }
            if saved == record && reservation.namespace() == 1)
        );
        assert_eq!(service.workspaces(), Some(1));
        assert!(matches!(
            owner.reserve_durable([9; 32]),
            Err(OwnerError::AdmissionFull)
        ));
        assert!(!overlays.join(record.overlay_name()).exists());
        drop(failure);
        drop(discovery);
        assert!(Roster::open(&root.join("catalog"), authority).is_err());
        return;
    }
    let root = std::env::temp_dir().join(format!(
        "layerfs-recorded-mount-sync-{}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    let catalog = root.join("catalog");
    fs::create_dir(&catalog).unwrap();
    fs::set_permissions(&catalog, fs::Permissions::from_mode(0o700)).unwrap();
    let shim = root.join("failure.so");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../layerfs-overlay/tests/durable_dir_sync_failure.c")
        )
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "uncertain_discovery_sync_retains_reservation_before_overlay_creation",
        ])
        .env(CHILD, &root)
        .env("TMPDIR", &root)
        .env("LD_PRELOAD", &shim)
        .env(
            "LAYERFS_TEST_DIR_SYNC_TARGET",
            fs::canonicalize(&catalog).unwrap(),
        )
        .env("LAYERFS_TEST_DIR_SYNC_TRIGGER", root.join("inject"))
        .env("LAYERFS_TEST_DIR_SYNC_RECEIPT", root.join("sync-failed"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!fs::read(root.join("sync-failed")).unwrap().is_empty());
    let authority = Authority {
        sandbox: [8; 32],
        store: fs::read(root.join("authority"))
            .unwrap()
            .try_into()
            .unwrap(),
    };
    let mut discovery = Roster::open(&catalog, authority).unwrap();
    let record = WorkspaceRecord {
        workspace: request(8, BranchId::from_authority([24; 16])).workspace,
        branch: BranchId::from_authority([24; 16]),
    };
    assert_eq!(discovery.lookup(record.workspace).unwrap(), Some(record));
    let fixture = PathBuf::from(fs::read_to_string(root.join("fixture-path")).unwrap());
    assert!(!fixture.join("durable").join(record.overlay_name()).exists());
    drop(discovery);
    fs::remove_dir_all(root).unwrap();
}
