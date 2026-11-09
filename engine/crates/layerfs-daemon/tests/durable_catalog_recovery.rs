//! A catalog recovery retains partial ownership and never skips missing files.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{Reply, Request, UnmountControl, WorkspaceToken};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{CatalogRecoveryCause, Service},
    durable_roster::{Authority, Roster, WorkspaceRecord},
    store::BindRequest,
    Owner, OwnerConfig, OwnerError,
};
use layerfs_history::{BranchId, WorkspaceId};
use layerfs_overlay::{Overlay, ProfileConfig};
use layerfs_storage::ReservationBlocks;
use layerfs_store_service::DurableWorkspace;
use std::{fs, os::unix::fs::PermissionsExt};

#[test]
fn capacity_preflight_and_partial_failure_keep_original_recovery_custody() {
    let fixture = support::Fixture::new(1, "catalog-recovery");
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
    let overlays = fixture.directory.join("durable");
    for path in [&catalog, &overlays] {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::create(&catalog, authority).unwrap();
    let writer = Owner::start(
        &fixture.directory.join("writer-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let mut records = Vec::new();
    for (identity, namespace) in [(71, 1), (72, 2)] {
        let workspace = WorkspaceId::from_authority([identity; 32]).unwrap();
        let record = WorkspaceRecord {
            workspace,
            branch: fixture.branch,
        };
        discovery.record(record).unwrap();
        let path = overlays.join(record.overlay_name());
        let owner = Owner::create_durable(DurableWorkspace {
            path: &path,
            profile: ProfileConfig::default(),
            identity: [identity; 32],
            namespace,
            expected_base_root: snapshot.effective_root.to_bytes(),
        })
        .unwrap();
        let client = writer.adopt_durable(owner).unwrap();
        let binding = store
            .bind_durable(
                client,
                BindRequest {
                    workspace,
                    branch: fixture.branch,
                },
                snapshot.clone(),
            )
            .unwrap()
            .workspace;
        drop(binding);
        store.close_reader(workspace).unwrap();
        records.push(record);
    }
    writer.stop().unwrap();

    let small = Owner::start(
        &fixture.directory.join("small-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 1 },
    )
    .unwrap();
    let service = Service::new(store.clone(), &small);
    let refused = service
        .recover_catalog(
            &mut discovery,
            &small,
            &overlays,
            [8; 32],
            ProfileConfig::default(),
            3,
        )
        .unwrap_err();
    assert!(matches!(
        refused.cause,
        CatalogRecoveryCause::Capacity {
            requested: 2,
            capacity: 1
        }
    ));
    assert!(refused.completed.is_empty());
    let first_path = overlays.join(records[0].overlay_name());
    let db = Overlay::recover_durable(&first_path, ProfileConfig::default(), [71; 32]).unwrap();
    assert_eq!(db.durable_namespace().unwrap(), Some(1));
    db.close_database().unwrap();
    drop(service);
    small.stop().unwrap();

    let occupied = Owner::start(
        &fixture.directory.join("occupied-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let occupied_route = occupied
        .client()
        .open(None, [73; 32], snapshot.effective_root.to_bytes())
        .unwrap();
    let service = Service::new(store.clone(), &occupied);
    let refused = service
        .recover_catalog(
            &mut discovery,
            &occupied,
            &overlays,
            [8; 32],
            ProfileConfig::default(),
            3,
        )
        .unwrap_err();
    assert!(matches!(
        refused.cause,
        CatalogRecoveryCause::Roster(OwnerError::InvalidAdmission)
    ));
    assert!(refused.completed.is_empty());
    occupied.client().close(occupied_route).unwrap();
    occupied
        .client()
        .retire_namespace(occupied_route.namespace())
        .unwrap();
    drop(service);
    occupied.stop().unwrap();

    let second_path = overlays.join(records[1].overlay_name());
    let saved_path = second_path.with_extension("saved");
    fs::rename(&second_path, &saved_path).unwrap();
    let owner = Owner::start(
        &fixture.directory.join("recovery-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let service = Service::new(store.clone(), &owner);
    let wrong = service
        .recover_catalog(
            &mut discovery,
            &owner,
            &overlays,
            [9; 32],
            ProfileConfig::default(),
            3,
        )
        .unwrap_err();
    assert!(matches!(
        wrong.cause,
        CatalogRecoveryCause::Invalid("recovery catalog authority")
    ));
    assert!(wrong.completed.is_empty());
    let failed = service
        .recover_catalog(
            &mut discovery,
            &owner,
            &overlays,
            [8; 32],
            ProfileConfig::default(),
            3,
        )
        .unwrap_err();
    assert!(
        matches!(failed.cause, CatalogRecoveryCause::Open { record, .. } if record == records[1])
    );
    assert_eq!(failed.completed.len(), 1);
    let token = failed.completed[0];
    assert_eq!(token.workspace, records[0].workspace);
    assert_eq!(token.namespace, 3);
    assert_eq!(service.workspaces(), Some(1));
    assert!(matches!(
        service
            .execute_control(&Request::Status(token))
            .unwrap()
            .reply,
        Reply::Status(_)
    ));
    assert!(service
        .execute_control(&Request::Status(WorkspaceToken {
            workspace: token.workspace,
            namespace: 1
        }))
        .is_err());
    assert!(saved_path.is_file());
    let repeated = service
        .recover_catalog(
            &mut discovery,
            &owner,
            &overlays,
            [8; 32],
            ProfileConfig::default(),
            3,
        )
        .unwrap_err();
    assert!(matches!(
        repeated.cause,
        CatalogRecoveryCause::Invalid("recovery owner or nonempty Service")
    ));
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
    drop(store);
    fixture.cleanup();
}

#[test]
fn one_catalog_recovers_sixty_four_independent_workspace_bindings() {
    let fixture = support::Fixture::new(1, "catalog-recovery-sixty-four");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        64,
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
    let overlays = fixture.directory.join("durable");
    for path in [&catalog, &overlays] {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::create(&catalog, authority).unwrap();
    let writer = Owner::start(
        &fixture.directory.join("writer-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 64 },
    )
    .unwrap();
    for identity in 1..=64u8 {
        let workspace = WorkspaceId::from_authority([identity; 32]).unwrap();
        let record = WorkspaceRecord {
            workspace,
            branch: fixture.branch,
        };
        discovery.record(record).unwrap();
        let path = overlays.join(record.overlay_name());
        let owner = Owner::create_durable(DurableWorkspace {
            path: &path,
            profile: ProfileConfig::default(),
            identity: [identity; 32],
            namespace: i64::from(identity),
            expected_base_root: snapshot.effective_root.to_bytes(),
        })
        .unwrap();
        let client = writer.adopt_durable(owner).unwrap();
        let binding = store
            .bind_durable(
                client,
                BindRequest {
                    workspace,
                    branch: fixture.branch,
                },
                snapshot.clone(),
            )
            .unwrap()
            .workspace;
        drop(binding);
        store.close_reader(workspace).unwrap();
    }
    writer.stop().unwrap();
    let owner = Owner::start(
        &fixture.directory.join("recovery-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 64 },
    )
    .unwrap();
    let service = Service::new(store.clone(), &owner);
    let tokens = service
        .recover_catalog(
            &mut discovery,
            &owner,
            &overlays,
            [8; 32],
            ProfileConfig::default(),
            65,
        )
        .unwrap();
    assert_eq!(tokens.len(), 64);
    assert_eq!(service.workspaces(), Some(64));
    for (index, token) in tokens.iter().enumerate() {
        assert_eq!(token.namespace, 65 + index as i64);
        assert_eq!(token.workspace.to_bytes()[0], index as u8 + 1);
        assert!(matches!(
            service
                .execute_control(&Request::Status(*token))
                .unwrap()
                .reply,
            Reply::Status(_)
        ));
    }
    for token in tokens {
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
    }
    drop(service);
    owner.stop().unwrap();
    drop(discovery);
    drop(store);
    fixture.cleanup();
}

#[test]
fn branch_mismatch_returns_the_recovered_owner_and_snapshot() {
    let fixture = support::Fixture::new(1, "catalog-recovery-branch-mismatch");
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
    let catalog = fixture.directory.join("catalog");
    let overlays = fixture.directory.join("durable");
    for path in [&catalog, &overlays] {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::create(&catalog, authority).unwrap();
    let workspace = WorkspaceId::from_authority([81; 32]).unwrap();
    let record = WorkspaceRecord {
        workspace,
        branch: BranchId::from_authority([99; 16]),
    };
    discovery.record(record).unwrap();
    let path = overlays.join(record.overlay_name());
    let original = Owner::create_durable(DurableWorkspace {
        path: &path,
        profile: ProfileConfig::default(),
        identity: [81; 32],
        namespace: 1,
        expected_base_root: snapshot.effective_root.to_bytes(),
    })
    .unwrap();
    let binding = store
        .bind_durable(
            original.client(),
            BindRequest {
                workspace,
                branch: fixture.branch,
            },
            snapshot.clone(),
        )
        .unwrap()
        .workspace;
    drop(binding);
    store.close_reader(workspace).unwrap();
    original.stop().unwrap();
    let owner = Owner::start(
        &fixture.directory.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 1 },
    )
    .unwrap();
    let service = Service::new(store.clone(), &owner);
    let failed = service
        .recover_catalog(
            &mut discovery,
            &owner,
            &overlays,
            [8; 32],
            ProfileConfig::default(),
            2,
        )
        .unwrap_err();
    assert!(failed.completed.is_empty());
    let CatalogRecoveryCause::Unadmitted {
        record: failed_record,
        reason,
        failure,
        snapshot: returned,
    } = failed.cause
    else {
        panic!("expected original owner custody");
    };
    assert_eq!(failed_record, record);
    assert_eq!(reason, "recorded Branch mismatch");
    assert_eq!(*returned, snapshot);
    assert_eq!(failure.owner.client().bound_route().unwrap().namespace(), 2);
    assert!(Owner::recover_recorded_next(&path, ProfileConfig::default(), [81; 32], 3).is_err());
    assert_eq!(service.workspaces(), Some(0));
    failure.owner.stop().unwrap();
    drop(service);
    owner.stop().unwrap();
    drop(discovery);
    drop(store);
    fixture.cleanup();
}
