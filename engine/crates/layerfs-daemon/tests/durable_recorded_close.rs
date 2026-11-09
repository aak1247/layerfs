//! Catalog-aware close keeps its original control receipt when sync is uncertain.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{Activity, Reply, Request, UnmountControl};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{RecordedCloseFailure, Service},
    durable_roster::{Authority, Roster, RosterError, WorkspaceRecord},
    store::BindRequest,
    Owner, OwnerConfig,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
use layerfs_storage::ReservationBlocks;
use layerfs_store_service::DurableWorkspace;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

#[test]
fn failed_intent_and_terminal_sync_keep_the_original_custody() {
    const CHILD: &str = "LAYERFS_RECORDED_CLOSE_FAILURE_CHILD";
    const MODE: &str = "LAYERFS_RECORDED_CLOSE_FAILURE_MODE";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let mode = std::env::var(MODE).unwrap();
        let fixture = support::Fixture::new(1, "recorded-close-failure");
        let store = open_store(
            fixture.config.clone(),
            support::BINDING,
            support::CURSOR,
            1,
            2 * 1024 * 1024,
            ReservationBlocks::default(),
        )
        .unwrap();
        let snapshot = store
            .history()
            .branch_snapshot(fixture.branch)
            .unwrap()
            .unwrap();
        let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
        fs::write(root.join("authority"), authority.store).unwrap();
        let mut discovery = Roster::create(&root.join("catalog"), authority).unwrap();
        let workspace = WorkspaceId::from_authority([101; 32]).unwrap();
        let record = WorkspaceRecord {
            workspace,
            branch: fixture.branch,
        };
        discovery.record(record).unwrap();
        let owner = Owner::start(
            &fixture.directory.join("template.sqlite"),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 1 },
        )
        .unwrap();
        let overlays = fixture.directory.join("durable");
        fs::create_dir(&overlays).unwrap();
        fs::set_permissions(&overlays, fs::Permissions::from_mode(0o700)).unwrap();
        let durable = Owner::create_durable(DurableWorkspace {
            path: &overlays.join("overlay.sqlite"),
            profile: ProfileConfig::default(),
            identity: [101; 32],
            namespace: 1,
            expected_base_root: snapshot.effective_root.to_bytes(),
        })
        .unwrap();
        let client = owner.adopt_durable(durable).unwrap();
        let binding = store
            .bind_durable(
                client,
                BindRequest {
                    workspace,
                    branch: fixture.branch,
                },
                snapshot,
            )
            .unwrap()
            .workspace;
        let service = Service::new(store.clone(), &owner);
        let token = service.register_recorded(&mut discovery, binding).unwrap();
        if mode == "intent" {
            fs::write(root.join("inject"), b"1").unwrap();
        }
        let failure = service
            .unmount_recorded(
                &mut discovery,
                UnmountControl {
                    token,
                    discard: true,
                },
            )
            .unwrap_err();
        if mode == "intent" {
            assert!(matches!(
                failure,
                RecordedCloseFailure::Intent {
                    cause: RosterError::Uncertain(_),
                    reservation_retained: true
                }
            ));
            let Reply::Status(status) = service
                .execute_control(&Request::Status(token))
                .unwrap()
                .reply
            else {
                panic!("missing status")
            };
            assert_eq!(status.activity, Activity::Closing);
            assert!(service.operation(token).is_err());
            assert!(service
                .execute_control(&Request::Unmount(UnmountControl {
                    token,
                    discard: true
                }))
                .is_err());
            assert_eq!(service.workspaces(), Some(1));
        } else {
            match failure {
                RecordedCloseFailure::Completion {
                    cause: RosterError::Uncertain(_),
                    original,
                } => {
                    assert_eq!(original.reply, Reply::Unmounted(token));
                }
                other => panic!("expected exact terminal receipt: {other:?}"),
            }
            assert_eq!(service.workspaces(), Some(0));
        }
        assert!(matches!(
            discovery.pending_close(),
            Err(RosterError::Retained(_))
        ));
        drop(discovery);
        assert!(Roster::open(&root.join("catalog"), authority).is_err());
        drop(service);
        if mode == "intent" {
            store.close_reader(workspace).unwrap();
        }
        owner.stop().unwrap();
        drop(store);
        fixture.cleanup();
        return;
    }

    let root = std::env::temp_dir().join(format!(
        "layerfs-recorded-close-failure-{}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    let shim = root.join("failure.so");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../layerfs-overlay/tests/durable_dir_sync_failure.c")
        )
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    let workspace = WorkspaceId::from_authority([101; 32]).unwrap();
    let record = WorkspaceRecord {
        workspace,
        branch: layerfs_history::BranchId::from_authority([24; 16]),
    };
    for mode in ["intent", "completion"] {
        let directory = root.join(mode);
        let catalog = directory.join("catalog");
        fs::create_dir(&directory).unwrap();
        fs::create_dir(&catalog).unwrap();
        fs::set_permissions(&catalog, fs::Permissions::from_mode(0o700)).unwrap();
        let trigger = if mode == "intent" {
            directory.join("inject")
        } else {
            catalog.join(record.overlay_name().replace(".sqlite", ".closed"))
        };
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "failed_intent_and_terminal_sync_keep_the_original_custody",
            ])
            .env(CHILD, &directory)
            .env(MODE, mode)
            .env("LD_PRELOAD", &shim)
            .env(
                "LAYERFS_TEST_DIR_SYNC_TARGET",
                fs::canonicalize(&catalog).unwrap(),
            )
            .env("LAYERFS_TEST_DIR_SYNC_TRIGGER", trigger)
            .env(
                "LAYERFS_TEST_DIR_SYNC_RECEIPT",
                directory.join("sync-failed"),
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "failed mode {mode}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!fs::read(directory.join("sync-failed")).unwrap().is_empty());
        let authority = Authority {
            sandbox: [8; 32],
            store: fs::read(directory.join("authority"))
                .unwrap()
                .try_into()
                .unwrap(),
        };
        let mut discovery = Roster::open(&catalog, authority).unwrap();
        if mode == "intent" {
            assert_eq!(
                discovery.pending_close().unwrap().unwrap().token.workspace,
                workspace
            );
            assert!(matches!(
                discovery.lookup(workspace),
                Err(RosterError::PendingClose(_))
            ));
        } else {
            assert_eq!(discovery.pending_close().unwrap(), None);
            assert_eq!(discovery.lookup(workspace).unwrap(), None);
        }
    }
    fs::remove_dir_all(root).unwrap();
}
