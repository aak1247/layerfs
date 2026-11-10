//! Explicit recovery of one fenced, host-exported durable SDK fixture.
#![cfg(target_os = "linux")]

use layerfs_bridge::{
    control::{DaemonPhase, HelloRequest, Reply, Request, UnmountControl, WorkspaceToken},
    daemon_recovery_setup::{RecoveryDaemonSetup, RecoveryWorkspaceSetup},
    daemon_setup::{DaemonSetup, HostExportIdentity},
    duplex,
};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{ContainerExportRoot, KernelConfig, KernelServing, RecoverControlConfig, Service},
    Owner, OwnerConfig,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
use layerfs_persistence::{PersistenceConfig, SqlitePersistenceProfile};
use layerfs_storage::ReservationBlocks;
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpStream},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

#[test]
#[ignore = "requires a caller-fenced owned Docker fixture and a fresh shared export"]
fn recover_synced_host_workspace_after_original_owner_exit() {
    let proof = PathBuf::from(std::env::var_os("LAYERFS_CRASH_PROOF").unwrap());
    assert!(proof.join("original-owner-fenced").is_file());
    let setup = DaemonSetup::decode(&fs::read(proof.join("daemon.setup")).unwrap()).unwrap();
    let receipt = fs::read_to_string(proof.join("original.receipt")).unwrap();
    let old_namespace: i64 = receipt.lines().next().unwrap().parse().unwrap();
    let fresh = setup.fresh_workspace.as_ref().unwrap();
    let manifest = setup.existing_store.as_ref().unwrap();
    let number = |name| std::env::var(name).unwrap().parse().unwrap();
    let identity = HostExportIdentity {
        boot_id: std::env::var("LAYERFS_TEST_EXPORT_BOOT").unwrap(),
        device: number("LAYERFS_TEST_EXPORT_DEVICE"),
        inode: number("LAYERFS_TEST_EXPORT_INODE"),
        shared_peer: number("LAYERFS_TEST_EXPORT_PEER"),
    };
    let mut guard = ContainerExportRoot::open_fresh(Path::new("/workspaces"), &identity).unwrap();
    guard
        .claim_fresh(duplex::public_key(&[9; 32]).unwrap())
        .unwrap();
    let store = open_store(
        PersistenceConfig::sqlite(&setup.store)
            .with_sqlite_profile(SqlitePersistenceProfile::Durable),
        &manifest.binding,
        manifest.cursor_key,
        16,
        8 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let recovery = PathBuf::from("/tmp/layerfs-owned-recovery");
    fs::create_dir(&recovery).unwrap();
    let owner = Owner::start(
        &recovery.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 16 },
    )
    .unwrap();
    let kernel = Arc::new(
        KernelServing::start_with_export(
            KernelConfig {
                mounts: "/workspaces".into(),
                owner_uid: 0,
                owner_gid: 0,
                command_uid: u32::try_from(number("LAYERFS_TEST_UID")).unwrap(),
                command_gid: u32::try_from(number("LAYERFS_TEST_GID")).unwrap(),
                ready_wait: Duration::from_secs(10),
                drain_wait: Duration::from_secs(10),
                control: recovery.join("unused-fusectl"),
            },
            guard,
        )
        .unwrap(),
    );
    let mut service = Service::with_kernel(store, &owner, kernel);
    let tokens = service
        .recover_durable_control(
            &owner,
            RecoverControlConfig {
                catalog_directory: fresh.catalog_directory.clone().into(),
                overlay_directory: fresh.overlay_directory.clone().into(),
                sandbox: fresh.sandbox,
                profile: ProfileConfig::default(),
                minimum_namespace: old_namespace + 1,
                store_device: fs::metadata(&setup.store).unwrap().dev(),
            },
        )
        .unwrap();
    let workspace = WorkspaceId::from_authority([71; 32]).unwrap();
    assert_eq!(tokens.len(), 1);
    let token = tokens[0];
    assert_eq!(token.workspace, workspace);
    assert!(token.namespace > old_namespace);
    assert!(service
        .execute_control(&Request::Status(WorkspaceToken {
            workspace,
            namespace: old_namespace,
        }))
        .is_err());
    let Reply::Ready(ready) = service
        .execute_control(&Request::Attach(token))
        .unwrap()
        .reply
    else {
        panic!("recovered Workspace lacks a Ready receipt")
    };
    assert_eq!(
        fs::read(Path::new(&ready.directory).join("durable-host-proof")).unwrap(),
        b"host durable acknowledgement"
    );
    fs::write(
        proof.join("recovery-ready"),
        format!("{}\n{}\n", token.namespace, ready.receipt.device_minor),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(45);
    while !proof.join("host-read").exists() {
        assert!(
            Instant::now() < deadline,
            "host did not verify recovered bytes"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        service
            .execute_control(&Request::Unmount(UnmountControl {
                token,
                discard: true
            }))
            .unwrap()
            .reply,
        Reply::Unmounted(token)
    );
    println!("RECOVERED_HOST_WORKSPACE original_bytes=true old_token_refused=true new_namespace=true closed=true");
}

#[test]
#[ignore = "requires a caller-fenced owned Docker fixture, daemon binary and fresh shared export"]
fn recover_through_daemon_startup_after_original_owner_exit() {
    let proof = PathBuf::from(std::env::var_os("LAYERFS_CRASH_PROOF").unwrap());
    assert!(proof.join("original-owner-fenced").is_file());
    let mut setup = DaemonSetup::decode(&fs::read(proof.join("daemon.setup")).unwrap()).unwrap();
    let receipt = fs::read_to_string(proof.join("original.receipt")).unwrap();
    let old_namespace: i64 = receipt.lines().next().unwrap().parse().unwrap();
    let fresh = setup.fresh_workspace.take().unwrap();
    let number = |name| std::env::var(name).unwrap().parse().unwrap();
    let export = HostExportIdentity {
        boot_id: std::env::var("LAYERFS_TEST_EXPORT_BOOT").unwrap(),
        device: number("LAYERFS_TEST_EXPORT_DEVICE"),
        inode: number("LAYERFS_TEST_EXPORT_INODE"),
        shared_peer: number("LAYERFS_TEST_EXPORT_PEER"),
    };
    let store = fs::metadata(&setup.store).unwrap();
    let root = PathBuf::from("/tmp/layerfs-recovery-daemon");
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    setup.listen = "127.0.0.1:0".into();
    setup.private_key = [8; 32];
    setup.control_peer = duplex::public_key(&[9; 32]).unwrap();
    setup.overlay = root.join("template.sqlite").to_str().unwrap().into();
    setup.host_export = Some(export.clone());
    let recover = RecoveryDaemonSetup {
        daemon: setup,
        workspace: RecoveryWorkspaceSetup {
            sandbox: fresh.sandbox,
            catalog_directory: fresh.catalog_directory,
            overlay_directory: fresh.overlay_directory,
            minimum_namespace: old_namespace + 1,
            boot_id: export.boot_id,
            store_device: store.dev(),
            store_inode: store.ino(),
        },
    };
    let write_config = |path: &Path, bytes: &[u8]| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    };
    let mut wrong = recover.clone();
    wrong.workspace.store_inode = if store.ino() == 1 { 2 } else { store.ino() - 1 };
    let wrong_path = root.join("wrong.setup");
    write_config(&wrong_path, &wrong.encode().unwrap());
    let rejected = Command::new("/layerfs-daemon")
        .arg("--config")
        .arg(&wrong_path)
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr)
        .contains("recovery Store identity or privacy changed"));
    assert!(fs::read_dir("/workspaces").unwrap().next().is_none());
    assert!(!root.join("template.sqlite").exists());

    let config_path = root.join("daemon.setup");
    write_config(&config_path, &recover.encode().unwrap());
    let mut daemon = Command::new("/layerfs-daemon")
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(daemon.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let address: SocketAddr = line
        .strip_prefix("LAYERFS_DAEMON_LISTEN ")
        .expect("recovery daemon listener")
        .trim()
        .parse()
        .unwrap();
    let socket = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    let connection =
        duplex::initiate(socket, &[9; 32], duplex::public_key(&[8; 32]).unwrap()).unwrap();
    let mut control = layerfs_api::internal::Control::new(connection);
    assert!(matches!(
        control
            .call(Request::Hello(HelloRequest {
                expected_instance: None,
                wait_for_store: true,
            }))
            .unwrap(),
        Reply::Hello(status) if status.phase == DaemonPhase::ControlReady
    ));
    let workspace = WorkspaceId::from_authority([71; 32]).unwrap();
    let Reply::Located(status) = control.call(Request::Locate(workspace)).unwrap() else {
        panic!("recovered Workspace was not discoverable")
    };
    let token = status.token;
    assert!(token.namespace > old_namespace);
    assert!(matches!(
        control
            .call(Request::Status(WorkspaceToken {
                workspace,
                namespace: old_namespace,
            }))
            .unwrap(),
        Reply::Refused(_)
    ));
    let Reply::Ready(ready) = control.call(Request::Attach(token)).unwrap() else {
        panic!("recovered Workspace lacks a Ready receipt")
    };
    assert_eq!(
        fs::read(Path::new(&ready.directory).join("durable-host-proof")).unwrap(),
        b"host durable acknowledgement"
    );
    fs::write(
        proof.join("recovery-ready"),
        format!("{}\n{}\n", token.namespace, ready.receipt.device_minor),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(45);
    while !proof.join("host-read").exists() {
        assert!(
            Instant::now() < deadline,
            "host did not verify recovered bytes"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        control
            .call(Request::Unmount(UnmountControl {
                token,
                discard: true
            }))
            .unwrap(),
        Reply::Unmounted(token)
    );
    assert!(matches!(
        control.call(Request::EndSession).unwrap(),
        Reply::SessionEnded
    ));
    daemon.kill().unwrap();
    daemon.wait().unwrap();
    println!("DAEMON_RECOVERED_HOST_WORKSPACE original_bytes=true old_token_refused=true new_namespace=true closed=true wrong_store_refused=true");
}
