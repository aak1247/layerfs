//! Discovery evidence gates registration while retaining the original owner.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{Reply, Request, UnmountControl};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{RecordedRegistrationError, Service},
    durable_roster::{Authority, Roster, WorkspaceRecord},
    store::BindRequest,
    Owner, OwnerConfig,
};
use layerfs_history::{BranchId, WorkspaceId};
use layerfs_overlay::ProfileConfig;
use layerfs_storage::ReservationBlocks;
use layerfs_store_service::DurableWorkspace;
use std::{fs, os::unix::fs::PermissionsExt};

#[test]
fn discovery_identity_and_branch_gate_registration() {
    let fixture = support::Fixture::new(1, "recorded-registration");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        2,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let snapshot = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let catalog = fixture.directory.join("catalog");
    let wrong_catalog = fixture.directory.join("wrong-catalog");
    let overlays = fixture.directory.join("durable");
    for path in [&catalog, &wrong_catalog, &overlays] {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::create(&catalog, authority).unwrap();
    let mut wrong = Roster::create(
        &wrong_catalog,
        Authority {
            sandbox: [8; 32],
            store: [9; 32],
        },
    )
    .unwrap();
    let owner = Owner::start(
        &fixture.directory.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let service = Service::new(store.clone(), &owner);
    let bind = |identity: u8, namespace: i64| {
        let path = overlays.join(format!("{identity}.sqlite"));
        let workspace_id = WorkspaceId::from_authority([identity; 32]).unwrap();
        let workspace_owner = Owner::create_durable(DurableWorkspace {
            path: &path,
            profile: ProfileConfig::default(),
            identity: [identity; 32],
            namespace,
            expected_base_root: snapshot.effective_root.to_bytes(),
        })
        .unwrap();
        let client = owner.adopt_durable(workspace_owner).unwrap();
        store
            .bind_durable(
                client,
                BindRequest {
                    workspace: workspace_id,
                    branch: fixture.branch,
                },
                snapshot.clone(),
            )
            .unwrap()
            .workspace
    };

    let mismatch_store = service
        .register_recorded(&mut wrong, bind(61, 1))
        .unwrap_err();
    assert!(matches!(
        &mismatch_store.cause,
        RecordedRegistrationError::StoreMismatch
    ));
    let missing = service
        .register_recorded(&mut discovery, *mismatch_store.workspace)
        .unwrap_err();
    assert!(matches!(&missing.cause, RecordedRegistrationError::Missing));
    assert_eq!(service.workspaces(), Some(0));
    let first_id = WorkspaceId::from_authority([61; 32]).unwrap();
    discovery
        .record(WorkspaceRecord {
            workspace: first_id,
            branch: fixture.branch,
        })
        .unwrap();
    let token = service
        .register_recorded(&mut discovery, *missing.workspace)
        .unwrap();

    let second_id = WorkspaceId::from_authority([62; 32]).unwrap();
    discovery
        .record(WorkspaceRecord {
            workspace: second_id,
            branch: BranchId::from_authority([99; 16]),
        })
        .unwrap();
    let mismatch = service
        .register_recorded(&mut discovery, bind(62, 2))
        .unwrap_err();
    assert!(matches!(
        &mismatch.cause,
        RecordedRegistrationError::BranchMismatch
    ));
    assert_eq!(mismatch.workspace.identity(), second_id);
    assert_eq!(service.workspaces(), Some(1));
    mismatch.workspace.close().unwrap();
    drop(mismatch);
    owner.client().retire_namespace(2).unwrap();
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
    owner.stop().unwrap();
    drop(discovery);
    drop(wrong);
    drop(store);
    fixture.cleanup();
}
