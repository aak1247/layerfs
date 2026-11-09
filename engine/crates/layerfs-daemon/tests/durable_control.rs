//! Existing control requests can use one fenced, catalog-backed durable owner.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::{
    control::{Answer, Call, ControlCode, Reply, Request, UnmountControl, WorkspaceToken},
    duplex,
};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{
        CatalogRecoveryCause, DurableControlCause, DurableControlConfig, Failure,
        RecordedCloseFailure, RecordedMountFailure, RecordedMountRequest, RecoverControlCause,
        RecoverControlConfig, Service,
    },
    durable_roster::{Authority, Roster, RosterError, WorkspaceRecord},
    Owner, OwnerConfig,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
use layerfs_storage::ReservationBlocks;
use std::{
    fs,
    net::{TcpListener, TcpStream},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::PathBuf,
    process::Command,
    sync::Arc,
    time::Duration,
};

fn directories(fixture: &support::Fixture) -> (PathBuf, PathBuf) {
    let catalog = fixture.directory.join("catalog");
    let overlays = fixture.directory.join("durable");
    for path in [&catalog, &overlays] {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    (catalog, overlays)
}
fn config(catalog: Roster, overlay_directory: PathBuf) -> DurableControlConfig {
    DurableControlConfig {
        catalog,
        overlay_directory,
        sandbox: [8; 32],
        profile: ProfileConfig::default(),
        minimum_namespace: 1,
    }
}

fn call(connection: &mut duplex::Connection, id: u64, request: Request) -> Reply {
    connection
        .send
        .send(&Call { id, request }.encode().unwrap())
        .unwrap();
    let answer = Answer::decode(connection.receive.receive().unwrap()).unwrap();
    assert_eq!(answer.id, id);
    answer.reply
}

#[test]
fn durable_control_rejects_disposable_store_before_recovery() {
    let fixture = support::Fixture::new(1, "control-disposable-rejection");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        1,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let (catalog, overlays) = directories(&fixture);
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let discovery = Roster::create(&catalog, authority).unwrap();
    let owner = Owner::start(
        &fixture.directory.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 1 },
    )
    .unwrap();
    let mut service = Service::new(store.clone(), &owner);
    let failure = service
        .activate_durable_control(&owner, config(discovery, overlays))
        .unwrap_err();
    assert!(matches!(
        failure.cause,
        DurableControlCause::Invalid("durable control requires a Durable Store")
    ));
    assert_eq!(service.workspaces(), Some(0));
    drop(failure);
    drop(service);
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}

#[test]
fn fenced_recovery_pins_separate_directories_on_the_store_device() {
    let fixture = support::Fixture::new_durable(1, "recovery-control-directories");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        1,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let (catalog, overlays) = directories(&fixture);
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    drop(Roster::create(&catalog, authority).unwrap());
    let owner = Owner::start(
        &fixture.directory.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 1 },
    )
    .unwrap();
    let mut service = Service::new(store.clone(), &owner);
    let config = RecoverControlConfig {
        catalog_directory: catalog.clone(),
        overlay_directory: overlays,
        sandbox: [8; 32],
        profile: ProfileConfig::default(),
        minimum_namespace: 1,
        store_device: fs::metadata(&fixture.config.path).unwrap().dev(),
    };
    let mut wrong = config.clone();
    wrong.store_device ^= 1;
    let error = service.recover_durable_control(&owner, wrong).unwrap_err();
    assert!(matches!(
        error.cause,
        RecoverControlCause::Invalid("separate local durable directories")
    ));
    drop(error);
    drop(Roster::open(&catalog, authority).unwrap());
    let mut overlap = config.clone();
    overlap.overlay_directory = catalog.clone();
    let error = service
        .recover_durable_control(&owner, overlap)
        .unwrap_err();
    assert!(matches!(error.cause, RecoverControlCause::Invalid(_)));
    drop(error);
    assert!(service
        .recover_durable_control(&owner, config)
        .unwrap()
        .is_empty());
    assert!(Roster::open(&catalog, authority).is_err());
    assert_eq!(service.workspaces(), Some(0));
    drop(service);
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}

#[test]
fn fenced_recovery_preserves_an_unresolved_close_intent() {
    let fixture = support::Fixture::new_durable(1, "recovery-pending-close");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        1,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let (catalog, overlays) = directories(&fixture);
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::create(&catalog, authority).unwrap();
    let workspace = WorkspaceId::from_authority([70; 32]).unwrap();
    discovery
        .record(WorkspaceRecord {
            workspace,
            branch: fixture.branch,
        })
        .unwrap();
    let closing = discovery
        .begin_close(WorkspaceToken {
            workspace,
            namespace: 1,
        })
        .unwrap();
    drop(discovery);
    let owner = Owner::start(
        &fixture.directory.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 1 },
    )
    .unwrap();
    let mut service = Service::new(store.clone(), &owner);
    let mut failure = service
        .recover_durable_control(
            &owner,
            RecoverControlConfig {
                catalog_directory: catalog.clone(),
                overlay_directory: overlays,
                sandbox: [8; 32],
                profile: ProfileConfig::default(),
                minimum_namespace: 2,
                store_device: fs::metadata(&fixture.config.path).unwrap().dev(),
            },
        )
        .unwrap_err();
    let RecoverControlCause::Control(control) = &mut failure.cause else {
        panic!("original recovery failure was not retained")
    };
    assert!(matches!(
        &control.cause,
        DurableControlCause::Recovery(cause)
            if matches!(&cause.cause, CatalogRecoveryCause::Discovery(RosterError::PendingClose(value)) if *value == closing)
    ));
    assert_eq!(
        control.config.catalog.pending_close().unwrap(),
        Some(closing)
    );
    assert_eq!(service.workspaces(), Some(0));
    drop(failure);
    let mut discovery = Roster::open(&catalog, authority).unwrap();
    assert_eq!(discovery.pending_close().unwrap(), Some(closing));
    drop(discovery);
    drop(service);
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}

#[test]
#[ignore = "requires CAP_SYS_ADMIN and an owned ext4 test directory"]
fn same_inode_bind_mount_is_not_accepted_as_the_original_directory() {
    use nix::mount::{mount, umount, MsFlags};
    for which in ["catalog", "overlays"] {
        let fixture = support::Fixture::new_durable(1, &format!("recovery-control-bind-{which}"));
        let store = open_store(
            fixture.config.clone(),
            support::BINDING,
            support::CURSOR,
            1,
            2 * 1024 * 1024,
            ReservationBlocks::default(),
        )
        .unwrap();
        let (catalog, overlays) = directories(&fixture);
        let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
        drop(Roster::create(&catalog, authority).unwrap());
        let owner = Owner::start(
            &fixture.directory.join("template.sqlite"),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 1 },
        )
        .unwrap();
        let mut service = Service::new(store.clone(), &owner);
        let config = RecoverControlConfig {
            catalog_directory: catalog.clone(),
            overlay_directory: overlays.clone(),
            sandbox: [8; 32],
            profile: ProfileConfig::default(),
            minimum_namespace: 1,
            store_device: fs::metadata(&fixture.config.path).unwrap().dev(),
        };
        let target = if which == "catalog" {
            &catalog
        } else {
            &overlays
        };
        let bind = || {
            mount(
                Some(target.as_path()),
                target.as_path(),
                None::<&str>,
                MsFlags::MS_BIND,
                None::<&str>,
            )
            .unwrap();
        };
        bind();
        let refused = service
            .recover_durable_control(&owner, config.clone())
            .unwrap_err();
        assert!(matches!(refused.cause, RecoverControlCause::Invalid(_)));
        drop(refused);
        umount(target.as_path()).unwrap();
        service.recover_durable_control(&owner, config).unwrap();
        bind();
        let request = Request::Mount {
            workspace: WorkspaceId::from_authority([70; 32]).unwrap(),
            branch: fixture.branch,
        };
        assert!(matches!(
            service.execute_control(&request),
            Err(Failure::DurableRoot(_))
        ));
        umount(target.as_path()).unwrap();
        assert!(matches!(
            service.execute_control(&request),
            Err(Failure::DurableRoot(_))
        ));
        drop(service);
        assert!(Roster::open(&catalog, authority).is_err());
        owner.stop().unwrap();
        drop(store);
        println!("target={which} same_inode_bind_refused=true held_root_quarantined=true");
        println!("owned_fixture={}", fixture.directory.display());
        // The quarantine intentionally retains descriptors until this test process exits.
    }
}

#[test]
fn existing_control_requests_create_and_close_a_durable_workspace() {
    let fixture = support::Fixture::new_durable(1, "control-durable-requests");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        1,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let (catalog, overlays) = directories(&fixture);
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let discovery = Roster::create(&catalog, authority).unwrap();
    let owner = Owner::start(
        &fixture.directory.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 1 },
    )
    .unwrap();
    let mut service = Service::new(store.clone(), &owner);
    assert!(service
        .activate_durable_control(&owner, config(discovery, overlays.clone()))
        .unwrap()
        .is_empty());
    let other_path = fixture.directory.join("other-catalog");
    fs::create_dir(&other_path).unwrap();
    fs::set_permissions(&other_path, fs::Permissions::from_mode(0o700)).unwrap();
    let mut other = Roster::create(&other_path, authority).unwrap();
    let refused = service
        .mount_recorded(
            &mut other,
            &owner,
            &overlays,
            RecordedMountRequest {
                sandbox: [8; 32],
                profile: ProfileConfig::default(),
                workspace: WorkspaceId::from_authority([70; 32]).unwrap(),
                branch: fixture.branch,
            },
        )
        .unwrap_err();
    assert!(matches!(
        *refused,
        RecordedMountFailure::Invalid("durable control owns admission")
    ));
    let refused = service
        .recover_catalog(
            &mut other,
            &owner,
            &overlays,
            [8; 32],
            ProfileConfig::default(),
            1,
        )
        .unwrap_err();
    assert!(matches!(
        refused.cause,
        CatalogRecoveryCause::Invalid("durable control owns admission")
    ));
    let service = Arc::new(service);
    let server = service.clone();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server_key = [91; 32];
    let client_key = [92; 32];
    let worker = std::thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut connection = duplex::accept(
            socket,
            &server_key,
            duplex::public_key(&client_key).unwrap(),
        )
        .unwrap();
        for _ in 0..3 {
            server.serve_one_control(&mut connection).unwrap();
        }
    });
    let socket = TcpStream::connect_timeout(&address, Duration::from_secs(10)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut connection = duplex::initiate(
        socket,
        &client_key,
        duplex::public_key(&server_key).unwrap(),
    )
    .unwrap();
    let workspace = WorkspaceId::from_authority([71; 32]).unwrap();
    let Reply::Bound { token, .. } = call(
        &mut connection,
        1,
        Request::Mount {
            workspace,
            branch: fixture.branch,
        },
    ) else {
        panic!("missing bound reply")
    };
    assert_eq!(token.workspace, workspace);
    assert_eq!(token.namespace, 1);
    assert!(matches!(
        service.unmount_recorded(
            &mut other,
            UnmountControl {
                token,
                discard: true
            }
        ),
        Err(RecordedCloseFailure::Invalid(
            "durable control owns admission"
        ))
    ));
    let full = call(
        &mut connection,
        2,
        Request::Mount {
            workspace: WorkspaceId::from_authority([72; 32]).unwrap(),
            branch: fixture.branch,
        },
    );
    assert!(matches!(full, Reply::Refused(ref refusal) if refusal.code == ControlCode::Capacity));
    assert_eq!(
        call(
            &mut connection,
            3,
            Request::Unmount(UnmountControl {
                token,
                discard: false
            })
        ),
        Reply::Unmounted(token)
    );
    drop(connection);
    worker.join().unwrap();
    drop(Arc::try_unwrap(service).ok().unwrap());
    drop(other);
    let mut discovery = Roster::open(&catalog, authority).unwrap();
    assert_eq!(discovery.lookup(workspace).unwrap(), None);
    drop(discovery);
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}

#[test]
fn replacing_overlay_directory_quarantines_catalog_and_original_root() {
    const CHILD: &str = "LAYERFS_DURABLE_CONTROL_ROOT_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let fixture = support::Fixture::new_durable(1, "control-root-replacement");
        let store = open_store(
            fixture.config.clone(),
            support::BINDING,
            support::CURSOR,
            1,
            2 * 1024 * 1024,
            ReservationBlocks::default(),
        )
        .unwrap();
        let (catalog, overlays) = directories(&fixture);
        let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
        let discovery = Roster::create(&catalog, authority).unwrap();
        let owner = Owner::start(
            &fixture.directory.join("template.sqlite"),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 1 },
        )
        .unwrap();
        let mut service = Service::new(store.clone(), &owner);
        service
            .activate_durable_control(&owner, config(discovery, overlays.clone()))
            .unwrap();
        let original = fixture.directory.join("original-durable");
        let replacement = fixture.directory.join("replacement-durable");
        fs::rename(&overlays, &original).unwrap();
        fs::create_dir(&overlays).unwrap();
        fs::set_permissions(&overlays, fs::Permissions::from_mode(0o700)).unwrap();
        let request = Request::Mount {
            workspace: WorkspaceId::from_authority([73; 32]).unwrap(),
            branch: fixture.branch,
        };
        assert!(matches!(
            service.execute_control(&request),
            Err(Failure::DurableRoot(_))
        ));
        assert_eq!(service.workspaces(), Some(0));
        fs::rename(&overlays, &replacement).unwrap();
        fs::rename(&original, &overlays).unwrap();
        assert!(matches!(
            service.execute_control(&request),
            Err(Failure::DurableRoot(_))
        ));
        drop(service);
        assert!(Roster::open(&catalog, authority).is_err());
        assert!(fs::read_dir(&overlays).unwrap().next().is_none());
        fs::write(root.join("ack"), b"original lock and root retained").unwrap();
        drop(store);
        return;
    }
    let root = std::env::temp_dir().join(format!("layerfs-control-root-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "replacing_overlay_directory_quarantines_catalog_and_original_root",
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
    assert!(root.join("ack").is_file());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn uncertain_mount_reply_keeps_original_custody_after_wire_delivery() {
    const CHILD: &str = "LAYERFS_DURABLE_CONTROL_SYNC_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let fixture = support::Fixture::new_durable(1, "control-sync-failure");
        let store = open_store(
            fixture.config.clone(),
            support::BINDING,
            support::CURSOR,
            1,
            2 * 1024 * 1024,
            ReservationBlocks::default(),
        )
        .unwrap();
        let overlays = fixture.directory.join("durable");
        fs::create_dir(&overlays).unwrap();
        fs::set_permissions(&overlays, fs::Permissions::from_mode(0o700)).unwrap();
        let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
        let discovery = Roster::create(&root.join("catalog"), authority).unwrap();
        let owner = Owner::start(
            &fixture.directory.join("template.sqlite"),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 1 },
        )
        .unwrap();
        let mut service = Service::new(store.clone(), &owner);
        service
            .activate_durable_control(&owner, config(discovery, overlays.clone()))
            .unwrap();
        let service = Arc::new(service);
        let server = service.clone();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_key = [91; 32];
        let client_key = [92; 32];
        fs::write(root.join("inject"), b"1").unwrap();
        let worker = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut connection = duplex::accept(
                socket,
                &server_key,
                duplex::public_key(&client_key).unwrap(),
            )
            .unwrap();
            server.serve_one_control(&mut connection).unwrap()
        });
        let socket = TcpStream::connect_timeout(&address, Duration::from_secs(10)).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut connection = duplex::initiate(
            socket,
            &client_key,
            duplex::public_key(&server_key).unwrap(),
        )
        .unwrap();
        let workspace = WorkspaceId::from_authority([74; 32]).unwrap();
        let reply = call(
            &mut connection,
            1,
            Request::Mount {
                workspace,
                branch: fixture.branch,
            },
        );
        assert!(
            matches!(reply, Reply::Refused(ref refusal) if refusal.code == ControlCode::Unknown)
        );
        drop(connection);
        let served = worker.join().unwrap();
        assert!(
            matches!(served.outcome, Err(Failure::RecordedMount(ref cause))
            if matches!(cause.as_ref(), layerfs_daemon::control::RecordedMountFailure::Intent { cause: layerfs_daemon::durable_roster::RosterError::Uncertain(_), .. }))
        );
        assert_eq!(service.workspaces(), Some(1));
        assert!(fs::read_dir(&overlays).unwrap().next().is_none());
        drop(served);
        drop(Arc::try_unwrap(service).ok().unwrap());
        assert!(Roster::open(&root.join("catalog"), authority).is_err());
        fs::write(
            root.join("ack"),
            b"unknown reply and original custody retained",
        )
        .unwrap();
        return;
    }
    let root = std::env::temp_dir().join(format!("layerfs-control-sync-{}", std::process::id()));
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
            "uncertain_mount_reply_keeps_original_custody_after_wire_delivery",
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
    assert!(root.join("ack").is_file());
    assert!(!fs::read(root.join("sync-failed")).unwrap().is_empty());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn root_replacement_during_mount_retains_each_original_outcome() {
    const CHILD: &str = "LAYERFS_DURABLE_CONTROL_ROOT_SWAP_CHILD";
    const DENY: &str = "LAYERFS_TEST_SWAP_DENY";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let fixture = support::Fixture::new_durable(1, "control-root-swap");
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
        let discovery = Roster::create(&root.join("catalog"), authority).unwrap();
        let owner = Owner::start(
            &fixture.directory.join("template.sqlite"),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 1 },
        )
        .unwrap();
        let mut service = Service::new(store, &owner);
        service
            .activate_durable_control(&owner, config(discovery, root.join("overlays")))
            .unwrap();
        fs::write(root.join("inject"), b"1").unwrap();
        let workspace = WorkspaceId::from_authority([75; 32]).unwrap();
        let failure = service
            .execute_control(&Request::Mount {
                workspace,
                branch: fixture.branch,
            })
            .unwrap_err();
        let denied = std::env::var_os(DENY).is_some();
        let token = match failure {
            Failure::After { cause, original } => {
                assert!(!denied);
                assert!(matches!(*cause, Failure::DurableRoot(_)));
                let Reply::Bound { token, .. } = original.reply else {
                    panic!("original binding receipt missing")
                };
                token
            }
            Failure::AfterFailure { cause, original } => {
                assert!(denied);
                assert!(matches!(*cause, Failure::DurableRoot(_)));
                assert!(matches!(*original, Failure::RecordedMount(ref failure)
                    if matches!(failure.as_ref(), layerfs_daemon::control::RecordedMountFailure::Creation { .. })));
                layerfs_bridge::control::WorkspaceToken {
                    workspace,
                    namespace: 1,
                }
            }
            other => panic!("expected retained original result: {other:?}"),
        };
        assert_eq!(token.workspace, workspace);
        assert_eq!(service.workspaces(), Some(1));
        assert!(matches!(
            service.execute_control(&Request::Attach(token)),
            Err(Failure::DurableRoot(_))
        ));
        assert!(matches!(
            service.operation(token),
            Err(Failure::DurableRoot(_))
        ));
        let commit = Request::Commit(layerfs_bridge::control::CommitControl {
            token,
            force: false,
            message: None,
        });
        assert!(matches!(
            service.execute_control(&commit),
            Err(Failure::DurableRoot(_))
        ));
        assert!(matches!(
            service.execute(&commit, |_, _, _| panic!(
                "a quarantined Commit must not start"
            )),
            Err(Failure::DurableRoot(_))
        ));
        assert!(matches!(
            service.execute_control(&Request::Unmount(UnmountControl {
                token,
                discard: true
            })),
            Err(Failure::DurableRoot(_))
        ));
        fs::rename(root.join("overlays"), root.join("replacement")).unwrap();
        fs::rename(root.join("original"), root.join("overlays")).unwrap();
        if denied {
            fs::set_permissions(root.join("overlays"), fs::Permissions::from_mode(0o700)).unwrap();
        }
        assert!(matches!(
            service.execute_control(&Request::Attach(token)),
            Err(Failure::DurableRoot(_))
        ));
        drop(service);
        assert!(Roster::open(&root.join("catalog"), authority).is_err());
        assert_eq!(
            fs::read_dir(root.join("overlays"))
                .unwrap()
                .next()
                .is_none(),
            denied
        );
        assert!(fs::read_dir(root.join("replacement"))
            .unwrap()
            .next()
            .is_none());
        fs::write(
            root.join("ack"),
            b"original result, directory and catalog retained",
        )
        .unwrap();
        return;
    }
    let root =
        std::env::temp_dir().join(format!("layerfs-control-root-swap-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let shim = root.join("swap.so");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/durable_root_swap.c"))
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    for mode in ["success", "denied"] {
        let directory = root.join(mode);
        fs::create_dir(&directory).unwrap();
        for name in ["catalog", "overlays"] {
            fs::create_dir(directory.join(name)).unwrap();
            fs::set_permissions(directory.join(name), fs::Permissions::from_mode(0o700)).unwrap();
        }
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "root_replacement_during_mount_retains_each_original_outcome",
            ])
            .env(CHILD, &directory)
            .env("TMPDIR", &directory)
            .env("LD_PRELOAD", &shim)
            .env(
                "LAYERFS_TEST_SWAP_TARGET",
                fs::canonicalize(directory.join("catalog")).unwrap(),
            )
            .env("LAYERFS_TEST_SWAP_SOURCE", directory.join("overlays"))
            .env("LAYERFS_TEST_SWAP_BACKUP", directory.join("original"))
            .env("LAYERFS_TEST_SWAP_TRIGGER", directory.join("inject"))
            .env("LAYERFS_TEST_SWAP_RECEIPT", directory.join("swapped"));
        if mode == "denied" {
            command.env(DENY, "1");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{mode}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::read(directory.join("swapped")).unwrap(), b"swapped\n");
        assert!(directory.join("ack").is_file());
    }
    fs::remove_dir_all(root).unwrap();
}
