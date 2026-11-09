//! Explicit recovery of one fenced, host-exported durable SDK fixture.
#![cfg(target_os = "linux")]

use layerfs_bridge::{
    control::{Reply, Request, UnmountControl, WorkspaceToken},
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
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
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
