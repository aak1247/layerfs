//! Explicit two-namespace visibility proof; run only with the dedicated harness.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::{
    control::{Reply, Request, TeardownStage, UnmountControl},
    daemon_setup::HostExportIdentity,
};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{
        ContainerExportRoot, DurableControlConfig, Failure, KernelConfig, KernelServing,
        RecordedCloseFailure, Service,
    },
    durable_roster::{Authority, Roster, RosterError},
    Owner, OwnerConfig,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
use layerfs_storage::ReservationBlocks;
use nix::unistd::{chown, Gid, Uid};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

#[test]
#[ignore = "requires an owned shared mount root and independently controlled host/container consumers"]
fn owned_mounts_reach_host_and_restricted_consumer() {
    let exported =
        PathBuf::from(std::env::var_os("LAYERFS_VISIBILITY_EXPORT").expect("owned mount root"));
    let proof =
        PathBuf::from(std::env::var_os("LAYERFS_VISIBILITY_PROOF").expect("owned proof directory"));
    let uid: u32 = std::env::var("LAYERFS_VISIBILITY_UID")
        .unwrap()
        .parse()
        .unwrap();
    let gid: u32 = std::env::var("LAYERFS_VISIBILITY_GID")
        .unwrap()
        .parse()
        .unwrap();
    let fixture = support::Fixture::new_durable(1, "export-visibility");
    for name in ["durable", "catalog"] {
        let path = fixture.directory.join(name);
        fs::create_dir(&path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        2,
        8 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let owner = Owner::start(
        &fixture.directory.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let authority = Authority::for_catalog([9; 32], store.history()).unwrap();
    let roster = Roster::create(&fixture.directory.join("catalog"), authority).unwrap();
    let config = KernelConfig {
        mounts: exported.clone(),
        owner_uid: 0,
        owner_gid: 0,
        command_uid: uid,
        command_gid: gid,
        ready_wait: Duration::from_secs(10),
        drain_wait: Duration::from_secs(10),
        control: fixture.directory.join("unused-fusectl"),
    };
    let kernel = Arc::new(match std::env::var("LAYERFS_TEST_EXPORT_BOOT") {
        Ok(boot_id) => {
            let number = |name| std::env::var(name).unwrap().parse().unwrap();
            let identity = HostExportIdentity {
                boot_id,
                device: number("LAYERFS_TEST_EXPORT_DEVICE"),
                inode: number("LAYERFS_TEST_EXPORT_INODE"),
                shared_peer: number("LAYERFS_TEST_EXPORT_PEER"),
            };
            let mut guard = ContainerExportRoot::open_fresh(&exported, &identity).unwrap();
            guard
                .claim_fresh(layerfs_bridge::duplex::public_key(&[9; 32]).unwrap())
                .unwrap();
            KernelServing::start_with_export(config, guard).unwrap()
        }
        Err(std::env::VarError::NotPresent) => KernelServing::start(config).unwrap(),
        Err(error) => panic!("invalid host export proof identity: {error}"),
    });
    let mut service = Service::with_kernel(store, &owner, kernel);
    assert!(service
        .activate_durable_control(
            &owner,
            DurableControlConfig {
                catalog: roster,
                overlay_directory: fixture.directory.join("durable"),
                sandbox: [9; 32],
                profile: ProfileConfig::default(),
                minimum_namespace: 1,
            }
        )
        .unwrap()
        .is_empty());
    let mut tokens = Vec::new();
    let mut dirs = Vec::new();
    for id in [1, 2] {
        let Reply::Bound { token, .. } = service
            .execute_control(&Request::Mount {
                workspace: WorkspaceId::from_authority([id; 32]).unwrap(),
                branch: fixture.branch,
            })
            .unwrap()
            .reply
        else {
            panic!("missing Bound")
        };
        let Reply::Ready(ready) = service
            .execute_control(&Request::Attach(token))
            .unwrap()
            .reply
        else {
            panic!("missing Ready")
        };
        let dir = PathBuf::from(ready.directory);
        let relative = dir.strip_prefix(&exported).unwrap();
        assert_eq!(relative.components().count(), 1);
        chown(&dir, Some(Uid::from_raw(uid)), Some(Gid::from_raw(gid))).unwrap();
        tokens.push(token);
        dirs.push(dir);
    }
    fs::write(dirs[0].join("from-daemon"), b"daemon\n").unwrap();
    fs::write(dirs[1].join("sibling-only"), b"sibling\n").unwrap();
    fs::write(
        proof.join("ready"),
        dirs[0].file_name().unwrap().as_encoded_bytes(),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(45);
    while !proof.join("consumers-done").exists() {
        assert!(
            !proof.join("consumer-failed").exists(),
            "external consumer failed"
        );
        assert!(
            Instant::now() < deadline,
            "external consumers did not finish"
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(fs::read(dirs[0].join("from-host")).unwrap(), b"host\n");
    assert_eq!(
        fs::read(dirs[0].join("from-container")).unwrap(),
        b"container\n"
    );
    assert!(!dirs[0].join("sibling-only").exists());
    assert!(!dirs[1].join("from-host").exists());
    if std::env::var_os("LAYERFS_VISIBILITY_EXPECT_EXPORT_CHANGE").is_some() {
        let close = |token| {
            service
                .execute_control(&Request::Unmount(UnmountControl {
                    token,
                    discard: true,
                }))
                .unwrap_err()
        };
        let Failure::RecordedClose(first) = close(tokens[0]) else {
            panic!("expected retained recorded close")
        };
        let RecordedCloseFailure::Control {
            cause: Failure::Retained(custody),
            ..
        } = *first
        else {
            panic!("expected retained original connection: {first:?}")
        };
        assert_eq!(custody.stage, TeardownStage::Detach);
        assert!(!custody.detached);
        let Failure::RecordedClose(second) = close(tokens[1]) else {
            panic!("expected durable close refusal")
        };
        let RecordedCloseFailure::Discovery(RosterError::PendingClose(original)) = *second else {
            panic!("expected original pending close: {second:?}")
        };
        assert_eq!(original.token, tokens[0]);
        fs::write(
            proof.join("closed"),
            b"original connection retained; next close refused by original intent\n",
        )
        .unwrap();
        return;
    }
    for token in tokens {
        assert_eq!(
            service
                .execute_control(&Request::Unmount(UnmountControl {
                    token,
                    discard: true,
                }))
                .unwrap()
                .reply,
            Reply::Unmounted(token)
        );
    }
    fs::write(proof.join("closed"), b"both normal unmounts acknowledged\n").unwrap();
}
