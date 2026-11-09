//! Real FUSE capacity for durable Workspaces on one manually assembled Service.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{ControlCode, Reply, Request, UnmountControl};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{
        DurableControlConfig, Failure, KernelConfig, KernelServing, RecordedMountFailure, Service,
    },
    durable_roster::{Authority, Roster},
    Owner, OwnerConfig,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
use layerfs_storage::ReservationBlocks;
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf, sync::Arc, time::Duration};

struct Mounts(Vec<PathBuf>);
impl Drop for Mounts {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = nix::mount::umount2(path, nix::mount::MntFlags::MNT_DETACH);
        }
    }
}

#[test]
#[ignore = "requires an isolated FUSE container and a private writable fusectl"]
fn thirty_two_durable_fuse_workspaces_share_one_capacity() {
    let fixture = support::Fixture::new_durable(1, "durable-capacity-fuse");
    let directory = &fixture.directory;
    for name in ["durable", "catalog", "mounts"] {
        let path = directory.join(name);
        fs::create_dir(&path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        32,
        8 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let owner = Owner::start(
        &directory.join("capacity-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 32 },
    )
    .unwrap();
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let roster = Roster::create(&directory.join("catalog"), authority).unwrap();
    let control = PathBuf::from(std::env::var_os("LAYERFS_TEST_FUSECTL").expect("private fusectl"));
    let kernel = Arc::new(
        KernelServing::start(KernelConfig {
            mounts: directory.join("mounts"),
            owner_uid: 0,
            owner_gid: 0,
            command_uid: 65534,
            command_gid: 65534,
            ready_wait: Duration::from_secs(10),
            drain_wait: Duration::from_secs(10),
            control,
        })
        .unwrap(),
    );
    let mut service = Service::with_kernel(store, &owner, kernel);
    assert!(service
        .activate_durable_control(
            &owner,
            DurableControlConfig {
                catalog: roster,
                overlay_directory: directory.join("durable"),
                sandbox: [8; 32],
                profile: ProfileConfig::default(),
                minimum_namespace: 1,
            }
        )
        .unwrap()
        .is_empty());
    let mut mounted = Mounts(Vec::new());
    let mut tokens = Vec::new();
    for index in 1..=32u8 {
        let workspace = WorkspaceId::from_authority([index; 32]).unwrap();
        let Reply::Bound { token, .. } = service
            .execute_control(&Request::Mount {
                workspace,
                branch: fixture.branch,
            })
            .unwrap()
            .reply
        else {
            panic!("missing bound Workspace")
        };
        let Reply::Ready(ready) = service
            .execute_control(&Request::Attach(token))
            .unwrap()
            .reply
        else {
            panic!("missing Ready receipt")
        };
        mounted.0.push(ready.directory.into());
        tokens.push(token);
    }
    let overflow = service.execute_control(&Request::Mount {
        workspace: WorkspaceId::from_authority([33; 32]).unwrap(),
        branch: fixture.branch,
    });
    let Err(Failure::RecordedMount(failure)) = overflow else {
        panic!("capacity must retain its recorded-mount refusal");
    };
    assert!(matches!(
        *failure,
        RecordedMountFailure::Before(Failure::Rejected(ControlCode::Capacity, _))
    ));
    assert_eq!(mounted.0.len(), 32);
    for token in tokens {
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
    }
    mounted.0.clear();
    println!("32 durable FUSE Workspaces mounted, the 33rd was refused, and all 32 closed");
}
