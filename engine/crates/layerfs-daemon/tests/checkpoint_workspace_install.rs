//! A known checkpoint install updates the Overlay and Workspace base together.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::snapshot;
use layerfs_content::filesystem::{FilesystemRootId, PathName};
use layerfs_daemon::{
    bootstrap::open_store,
    store::{BindRequest, BoundWorkspace, CommitOptions, OriginalCheckpoint},
    Owner, OwnerConfig, ServiceClass,
};
use layerfs_history::{
    checkpoint_request_digest, encode_checkpoint_request, CheckpointAttemptId, CommitMeta,
    CommitOutcome, CommitRequest, CommitVia, WorkspaceId,
};
use layerfs_overlay::{CheckpointOutcome, OverlayError, ProfileConfig};
use layerfs_storage::ReservationBlocks;
use layerfs_store_service::DurableWorkspace;
use layerfs_workspace::{CheckpointInstall, Operation, Outcome, Position, Time, WorkspaceError};
use std::{fs, os::unix::fs::PermissionsExt};

fn read(workspace: &BoundWorkspace) -> Vec<u8> {
    let operation = workspace.operation().unwrap();
    let source = operation
        .overlay()
        .acquire_base_source(workspace.route(), 7)
        .unwrap();
    let view = operation.workspace().view_for_source(source).unwrap();
    let file = view
        .lookup(
            operation.overlay(),
            view.root_serial(),
            &PathName::new("file-000000").unwrap(),
        )
        .unwrap();
    let mut bytes = Vec::new();
    view.read(operation.overlay(), file.serial, 0, 1024, &mut bytes)
        .unwrap();
    drop(view);
    operation
        .overlay()
        .release_base_source(workspace.route(), source)
        .unwrap();
    bytes
}

fn write(workspace: &BoundWorkspace, bytes: &[u8]) {
    let operation = workspace.operation().unwrap();
    let source = operation
        .overlay()
        .acquire_base_source(workspace.route(), 8)
        .unwrap();
    let view = operation.workspace().view_for_source(source).unwrap();
    let file = view
        .lookup(
            operation.overlay(),
            view.root_serial(),
            &PathName::new("file-000000").unwrap(),
        )
        .unwrap();
    let result = operation
        .workspace()
        .mutate(
            operation.overlay(),
            operation.ports(),
            &view,
            Operation::Write {
                serial: file.serial,
                position: Position::At(0),
                data: bytes.into(),
            },
            Time {
                seconds: file.metadata.mtime_seconds,
                nanoseconds: file.metadata.mtime_nanoseconds,
            },
        )
        .unwrap();
    let Outcome::Applied { publication, .. } = result else {
        panic!("write was not applied")
    };
    operation.overlay().reply_attempted(publication).unwrap();
    drop(view);
    operation
        .overlay()
        .release_base_source(workspace.route(), source)
        .unwrap();
}

#[test]
fn recorded_install_keeps_workspace_base_paired_with_original_overlay() {
    let fixture = support::Fixture::new_durable(1, "checkpoint-paired");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        4,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    assert!(store.has_durable_writer());
    let target = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let directory = fixture.directory.join("durable");
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    let identity = WorkspaceId::from_authority([42; 32]).unwrap();
    let owner = Owner::create_durable(DurableWorkspace {
        path: &directory.join("workspace.sqlite"),
        profile: ProfileConfig::default(),
        identity: identity.to_bytes(),
        namespace: 1,
        expected_base_root: target.effective_root.to_bytes(),
    })
    .unwrap();
    let original = store
        .bind_durable(
            owner.client(),
            BindRequest {
                workspace: identity,
                branch: fixture.branch,
            },
            target.clone(),
        )
        .unwrap()
        .workspace;
    let client = owner.client();
    let route = original.route();
    let target_bytes = snapshot::encode(&target).unwrap();
    let catalog = store.history().catalog_id();
    let incarnation = store.history().incarnation();
    assert!(client
        .capture_checkpoint(
            route,
            [99; 32],
            catalog.to_bytes(),
            incarnation,
            target_bytes.clone()
        )
        .is_err());
    assert!(client.state(route).unwrap().captured.is_none());
    let recorded = client
        .capture_checkpoint(
            route,
            identity.to_bytes(),
            catalog.to_bytes(),
            incarnation,
            target_bytes.clone(),
        )
        .unwrap();
    let generation = u64::try_from(recorded.capture().generation.number()).unwrap();
    let reader = client
        .acquire_recorded_captured_reader(route, recorded, generation)
        .unwrap();
    let owner_token = client
        .acquire_recorded_captured_operation(route, recorded, generation, reader)
        .unwrap();
    let mut later = support::bytes(0);
    later[0] = b'L';
    write(&original, &later);

    let other_owner = Owner::start(
        &fixture.directory.join("other.sqlite"),
        ProfileConfig::default(),
        OwnerConfig::default(),
    )
    .unwrap();
    let other = store
        .bind(
            other_owner.client(),
            BindRequest {
                workspace: WorkspaceId::from_authority([43; 32]).unwrap(),
                branch: fixture.branch,
            },
        )
        .unwrap()
        .workspace;
    let mut candidate_bytes = support::bytes(0);
    candidate_bytes[0] = b'C';
    write(&other, &candidate_bytes);
    other.commit_captured(&CommitOptions::default()).unwrap();
    let current = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let request = CommitRequest {
        branch: target.branch.id,
        expected_head: target.branch.head_commit,
        expected_base: target.branch.base_layer,
        expected_root: target.effective_root,
        candidate_root: current.effective_root,
        profile: target.profile,
        scope: target.scope,
        second_parent: None,
        meta: CommitMeta::new(CommitVia::Workspace),
        force: true,
    };
    let request_bytes = encode_checkpoint_request(&request).unwrap();
    let digest = checkpoint_request_digest(&request_bytes);
    let candidate_root = request.candidate_root.to_bytes();
    client
        .prepare_checkpoint_publication(
            route,
            recorded,
            request_bytes.clone(),
            digest,
            candidate_root,
            target_bytes.clone(),
        )
        .unwrap();
    let attempt = CheckpointAttemptId::from_bytes(recorded.attempt());
    let outcome = store
        .history()
        .commit_checkpoint(attempt, &request)
        .unwrap();
    let head = match outcome {
        CommitOutcome::UpToDate { head: Some(id), .. } => id,
        other => panic!("unexpected outcome: {other:?}"),
    };
    let observed = store
        .observe_original_checkpoint(OriginalCheckpoint {
            catalog,
            incarnation,
            attempt,
            request: &request_bytes,
            request_digest: digest,
            candidate_root: request.candidate_root,
            target: &target,
        })
        .unwrap()
        .unwrap();
    assert_eq!(observed.installed, current);
    let installed = snapshot::encode(&observed.installed).unwrap();
    let outcome = CheckpointOutcome::UpToDate {
        head: Some(head.to_bytes()),
    };
    let operation = original.operation().unwrap();
    let prepared = operation
        .workspace()
        .prepare_base_install(recorded.capture(), FilesystemRootId(current.effective_root))
        .unwrap();
    let scoped = operation
        .workspace()
        .scoped(operation.client_handle())
        .unwrap();
    drop(operation);
    let installed_copy = installed.clone();
    let result = client
        .call(route, ServiceClass::General, None, move |db| {
            Ok(scoped.install_prepared_checkpoint(
                db,
                prepared,
                CheckpointInstall {
                    recorded,
                    request_digest: digest,
                    target_snapshot: b"wrong target",
                    installed_snapshot: &installed_copy,
                    outcome,
                },
            ))
        })
        .unwrap();
    let (error, prepared) = result.unwrap_err();
    assert!(matches!(
        error,
        WorkspaceError::Overlay(OverlayError::Invalid("checkpoint install identity"))
    ));
    assert_eq!(
        original
            .operation()
            .unwrap()
            .workspace()
            .base()
            .unwrap()
            .identity()
            .0,
        target.effective_root
    );
    assert_eq!(client.durable_snapshot().unwrap(), target);
    assert_eq!(read(&original), later);

    let operation = original.operation().unwrap();
    let scoped = operation
        .workspace()
        .scoped(operation.client_handle())
        .unwrap();
    drop(operation);
    let target_copy = target_bytes.clone();
    let installed_copy = installed.clone();
    client
        .call(route, ServiceClass::General, None, move |db| {
            Ok(scoped.install_prepared_checkpoint(
                db,
                prepared,
                CheckpointInstall {
                    recorded,
                    request_digest: digest,
                    target_snapshot: &target_copy,
                    installed_snapshot: &installed_copy,
                    outcome,
                },
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        original
            .operation()
            .unwrap()
            .workspace()
            .base()
            .unwrap()
            .identity()
            .0,
        current.effective_root
    );
    assert_eq!(client.durable_snapshot().unwrap(), observed.installed);
    assert_eq!(original.snapshot().unwrap(), target); // The Commit driver owns this later update.
    assert_eq!(read(&original), later);
    client.release_captured_reader(route, reader).unwrap();
    client.release_operation(route, owner_token).unwrap();

    other.close().unwrap();
    drop(other);
    other_owner.stop().unwrap();
    drop(original);
    store.close_reader(identity).unwrap();
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}
