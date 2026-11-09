//! Explicit recovery proof for the isolated Node SDK Docker fixture.
#![cfg(target_os = "linux")]

use layerfs_bridge::{
    control::{Answer, Reply, Request, SdkValue},
    daemon_setup::DaemonSetup,
};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{DurableControlConfig, KernelConfig, KernelServing, Service},
    durable_roster::{Authority, Roster},
    Owner, OwnerConfig,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
use layerfs_persistence::{PersistenceConfig, SqlitePersistenceProfile};
use layerfs_storage::ReservationBlocks;
use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc, time::Duration};

struct Mounts(Vec<PathBuf>);
impl Drop for Mounts {
    fn drop(&mut self) {
        for mount in &self.0 {
            let _ = nix::mount::umount2(mount, nix::mount::MntFlags::MNT_DETACH);
        }
    }
}

#[test]
#[ignore = "requires the explicitly fenced SDK Docker proof, its original volume and a dedicated FUSE container"]
fn recover_exact_sdk_workspaces_after_owner_exit() {
    let proof =
        PathBuf::from(std::env::var_os("LAYERFS_SDK_PROOF_DIR").expect("owned proof directory"));
    assert!(
        proof.join("old-owner-fenced.txt").is_file(),
        "the test owner must record the original process and mount fence first"
    );
    let setup = DaemonSetup::decode(&fs::read(proof.join("daemon.setup")).unwrap()).unwrap();
    let fresh = setup
        .fresh_workspace
        .expect("original durable Sandbox setup");
    let manifest = setup
        .existing_store
        .expect("original installed Durable Store");
    let records = proof.join("home/lfs-sdk-proof-sstqc67g/sandboxes/1");
    let mut expected = BTreeMap::new();
    for entry in fs::read_dir(records).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("workspace-") || !name.ends_with(".record") {
            continue;
        }
        let Answer {
            reply: Reply::Sdk(record),
            ..
        } = Answer::decode(&fs::read(entry.path()).unwrap()).unwrap()
        else {
            panic!("Workspace discovery record")
        };
        let Some(SdkValue::Bytes(workspace)) = record.get("workspace") else {
            panic!("Workspace identity")
        };
        let id = WorkspaceId::from_slice(workspace).unwrap();
        let namespace = record.get_int("namespace").expect("original namespace");
        let branch = record
            .get_text("branch")
            .expect("original branch")
            .to_owned();
        assert!(expected.insert(id, (namespace, branch)).is_none());
    }
    assert_eq!(expected.len(), 2);
    let minimum = expected
        .values()
        .map(|(namespace, _)| *namespace)
        .max()
        .unwrap()
        + 1;
    let recovery = proof.join("recovery");
    let mounts = recovery.join("mounts");
    fs::create_dir_all(&mounts).unwrap();
    let store = open_store(
        PersistenceConfig::sqlite(&setup.store)
            .with_sqlite_profile(SqlitePersistenceProfile::Durable),
        &manifest.binding,
        manifest.cursor_key,
        32,
        8 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let owner = Owner::start(
        &recovery.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 32 },
    )
    .unwrap();
    let authority = Authority::for_catalog(fresh.sandbox, store.history()).unwrap();
    let roster = Roster::open(&PathBuf::from(&fresh.catalog_directory), authority).unwrap();
    let kernel = Arc::new(
        KernelServing::start(KernelConfig {
            mounts,
            owner_uid: 0,
            owner_gid: 0,
            command_uid: 501,
            command_gid: 20,
            ready_wait: Duration::from_secs(10),
            drain_wait: Duration::from_secs(10),
            control: recovery.join("fusectl"),
        })
        .unwrap(),
    );
    let mut service = Service::with_kernel(store, &owner, kernel);
    let tokens = service
        .activate_durable_control(
            &owner,
            DurableControlConfig {
                catalog: roster,
                overlay_directory: fresh.overlay_directory.into(),
                sandbox: fresh.sandbox,
                profile: ProfileConfig::default(),
                minimum_namespace: minimum,
            },
        )
        .unwrap();
    assert_eq!(tokens.len(), expected.len());
    let mut mounted = Mounts(Vec::new());
    for token in tokens {
        let (old_namespace, branch) = expected
            .remove(&token.workspace)
            .expect("original Workspace");
        assert!(
            token.namespace > old_namespace,
            "the old namespace cannot be reused"
        );
        let Reply::Ready(ready) = service
            .execute_control(&Request::Attach(token))
            .unwrap()
            .reply
        else {
            panic!("recovered Ready receipt")
        };
        let directory = PathBuf::from(ready.directory);
        mounted.0.push(directory.clone());
        let actual = fs::read(directory.join("durable.txt")).unwrap();
        let want = match branch.as_str() {
            "branch-one" => b"durable-first\n".as_slice(),
            "branch-two" => b"durable-second\n".as_slice(),
            _ => panic!("unexpected original Branch"),
        };
        assert_eq!(actual, want);
    }
    assert!(expected.is_empty());
    for directory in &mounted.0 {
        nix::mount::umount2(directory, nix::mount::MntFlags::MNT_DETACH).unwrap();
    }
    mounted.0.clear();
    println!(
        "recovered both exact SDK Workspaces with fresh namespaces and their original synced bytes"
    );
}
