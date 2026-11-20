//! Durable service binding keeps the original Branch even after it moves.
#![cfg(target_os = "linux")]

#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{Activity, CommitControl, Reply, Request};
use layerfs_content::filesystem::PathName;
use layerfs_daemon::{
    bootstrap::open_store,
    control::{Failure, Service},
    store::{BindRequest, BoundWorkspace, CommitError, CommitOptions},
    Owner, OwnerConfig, OwnerError,
};
use layerfs_history::{CommitOutcome, HistoryError, WorkspaceId};
use layerfs_overlay::{OverlayError, ProfileConfig};
use layerfs_storage::ReservationBlocks;
use layerfs_store_service::DurableWorkspace;
use layerfs_workspace::{Operation, Outcome, Position, Time};
use std::{os::unix::fs::PermissionsExt, path::Path};

fn spec(path: &Path, root: [u8; 32]) -> DurableWorkspace<'_> {
    DurableWorkspace {
        path,
        profile: ProfileConfig::default(),
        identity: [42; 32],
        namespace: 1,
        expected_base_root: root,
    }
}
fn access<T>(
    workspace: &BoundWorkspace,
    action: impl FnOnce(&layerfs_daemon::store::StoreOperation, &layerfs_workspace::SourceView) -> T,
) -> T {
    let operation = workspace.operation().unwrap();
    let overlay = operation.overlay();
    let route = workspace.route();
    let source = overlay.acquire_base_source(route, 7).unwrap();
    let view = operation.workspace().view_for_source(source).unwrap();
    let result = action(&operation, &view);
    drop(view);
    overlay.release_base_source(route, source).unwrap();
    result
}
fn read(workspace: &BoundWorkspace, name: &str) -> Vec<u8> {
    access(workspace, |operation, view| {
        let file = view
            .lookup(
                operation.overlay(),
                view.root_serial(),
                &PathName::new(name).unwrap(),
            )
            .unwrap();
        let mut bytes = Vec::new();
        view.read(operation.overlay(), file.serial, 0, 1024, &mut bytes)
            .unwrap();
        bytes
    })
}
fn write(workspace: &BoundWorkspace, name: &str, bytes: &[u8]) {
    access(workspace, |operation, view| {
        let file = view
            .lookup(
                operation.overlay(),
                view.root_serial(),
                &PathName::new(name).unwrap(),
            )
            .unwrap();
        let result = operation
            .workspace()
            .mutate(
                operation.overlay(),
                operation.ports(),
                view,
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
    });
}

#[test]
fn recovery_rebinds_recorded_base_and_merges_a_moved_branch() {
    let fixture = support::Fixture::new_durable(2, "durable-bind");
    assert_eq!(fixture.count, 2);
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        16,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let initial = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let directory = fixture.directory.join("durable");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.join("workspace.sqlite");
    let identity = WorkspaceId::from_authority([42; 32]).unwrap();
    let request = BindRequest {
        workspace: identity,
        branch: fixture.branch,
    };
    let owner = Owner::create_durable(spec(&path, initial.effective_root.to_bytes())).unwrap();
    let original = store
        .bind_durable(owner.client(), request.clone(), initial.clone())
        .unwrap()
        .workspace;
    assert_eq!(owner.client().durable_snapshot().unwrap(), initial);
    let mut ours = support::bytes(0);
    ours[0] = b'X';
    write(&original, "file-000000", &ours);
    assert_eq!(read(&original, "file-000000"), ours);

    // The runtime fences consumers and releases its reader; it does not
    // terminally close or discard the Workspace before owner process loss.
    store.close_reader(identity).unwrap();
    assert!(
        store
            .bind_durable(owner.client(), request.clone(), initial.clone())
            .is_err(),
        "the same owner must never have two logical bindings"
    );
    drop(original);
    owner.stop().unwrap();

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
    let mut theirs = support::bytes(1);
    theirs[0] = b'Y';
    write(&other, "file-000001", &theirs);
    other.commit_captured(&CommitOptions::default()).unwrap();
    let advanced = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    assert_ne!(advanced, initial);
    other.close().unwrap();
    drop(other);
    other_owner.stop().unwrap();

    assert!(Owner::recover_durable(spec(&path, initial.effective_root.to_bytes())).is_err());
    let mut recovered_spec = spec(&path, initial.effective_root.to_bytes());
    recovered_spec.namespace = 2;
    let recovered_owner = Owner::recover_durable(recovered_spec).unwrap();
    assert_eq!(
        recovered_owner.client().durable_snapshot().unwrap(),
        initial
    );
    let mut wrong = request.clone();
    wrong.workspace = WorkspaceId::from_authority([44; 32]).unwrap();
    assert!(store
        .bind_durable(recovered_owner.client(), wrong, initial.clone())
        .is_err());
    let mut forged = initial.clone();
    forged.head_root = Some(initial.base_root);
    assert!(store
        .bind_durable(recovered_owner.client(), request.clone(), forged)
        .is_err());
    assert!(
        store
            .bind_durable(recovered_owner.client(), request.clone(), advanced)
            .is_err(),
        "a valid newer root does not replace the recorded base"
    );
    let recovered = store
        .bind_durable(recovered_owner.client(), request.clone(), initial.clone())
        .unwrap()
        .workspace;
    assert_eq!(recovered.snapshot().unwrap(), initial);
    assert_eq!(read(&recovered, "file-000000"), ours);
    assert_eq!(read(&recovered, "file-000001"), support::bytes(1));
    recovered
        .commit_captured(&CommitOptions::default())
        .unwrap();
    assert_eq!(read(&recovered, "file-000000"), ours);
    assert_eq!(read(&recovered, "file-000001"), theirs);
    let installed = recovered.snapshot().unwrap();
    assert_eq!(
        recovered_owner.client().durable_snapshot().unwrap(),
        installed
    );
    drop(recovered);
    store.close_reader(identity).unwrap();
    recovered_owner.stop().unwrap();
    let (reopened_owner, recorded) =
        Owner::recover_recorded(&path, ProfileConfig::default(), [42; 32], 3).unwrap();
    assert_eq!(recorded, installed);
    let reopened = store
        .bind_durable(reopened_owner.client(), request.clone(), recorded)
        .unwrap()
        .workspace;
    assert_eq!(read(&reopened, "file-000000"), ours);
    assert_eq!(read(&reopened, "file-000001"), theirs);
    drop(reopened);
    store.close_reader(identity).unwrap();
    reopened_owner.stop().unwrap();
    assert!(Owner::recover_recorded_next(&path, ProfileConfig::default(), [42; 32], 0).is_err());
    let (later, snapshot) =
        Owner::recover_recorded_next(&path, ProfileConfig::default(), [42; 32], 10).unwrap();
    assert_eq!(later.client().bound_route().unwrap().namespace(), 10);
    assert_eq!(snapshot, installed);
    later.stop().unwrap();
    let (last, snapshot) =
        Owner::recover_recorded_next(&path, ProfileConfig::default(), [42; 32], 1).unwrap();
    assert_eq!(last.client().bound_route().unwrap().namespace(), 11);
    let final_binding = store
        .bind_durable(last.client(), request, snapshot)
        .unwrap()
        .workspace;
    assert_eq!(read(&final_binding, "file-000000"), ours);
    final_binding.close().unwrap();
    drop(final_binding);
    last.stop().unwrap();
    drop(store);
    fixture.cleanup();
}

#[test]
fn recorded_live_commits_publish_once_and_finish_original_custody() {
    let fixture = support::Fixture::new_durable(1, "recorded-live-commit");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        16,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let initial = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let directory = fixture.directory.join("durable");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.join("workspace.sqlite");
    let identity = WorkspaceId::from_authority([42; 32]).unwrap();
    let owner = Owner::create_durable(spec(&path, initial.effective_root.to_bytes())).unwrap();
    let workspace = store
        .bind_durable(
            owner.client(),
            BindRequest {
                workspace: identity,
                branch: fixture.branch,
            },
            initial,
        )
        .unwrap()
        .workspace;
    for byte in [b'X', b'Y'] {
        let mut content = support::bytes(0);
        content[0] = byte;
        write(&workspace, "file-000000", &content);
        let result = workspace
            .commit_captured(&CommitOptions::default())
            .unwrap();
        assert!(matches!(
            result.history,
            Some(CommitOutcome::Committed { .. })
        ));
        let namespace = result.namespace.as_ref().unwrap();
        assert!(!namespace.retained());
        let snapshot = workspace.snapshot().unwrap();
        assert_eq!(owner.client().durable_snapshot().unwrap(), snapshot);
        assert_eq!(read(&workspace, "file-000000"), content);
    }
    let unchanged = workspace
        .commit_captured(&CommitOptions::default())
        .unwrap();
    assert!(matches!(
        unchanged.history,
        Some(CommitOutcome::UpToDate { .. })
    ));
    assert!(unchanged.capture.is_none());
    drop(workspace);
    store.close_reader(identity).unwrap();
    owner.stop().unwrap();
    drop(store);
    let history = rusqlite::Connection::open(&fixture.config.path).unwrap();
    let receipts: (i64, i64) = history
        .query_row(
            "SELECT count(*),count(DISTINCT attempt_id) FROM checkpoint_receipt",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(receipts, (2, 2));
    drop(history);
    let overlay = rusqlite::Connection::open(path).unwrap();
    let pending: i64 = overlay
        .query_row(
            "SELECT count(*) FROM durable_checkpoint_attempt",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 0);
    drop(overlay);
    fixture.cleanup();
}

#[test]
fn recorded_capture_rejects_store_without_receipts_before_any_capture() {
    let fixture = support::Fixture::new(1, "recorded-unsupported-store");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        16,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let initial = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let directory = fixture.directory.join("durable");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.join("workspace.sqlite");
    let identity = WorkspaceId::from_authority([42; 32]).unwrap();
    let owner = Owner::create_durable(spec(&path, initial.effective_root.to_bytes())).unwrap();
    let workspace = store
        .bind_durable(
            owner.client(),
            BindRequest {
                workspace: identity,
                branch: fixture.branch,
            },
            initial.clone(),
        )
        .unwrap()
        .workspace;
    let mut content = support::bytes(0);
    content[0] = b'X';
    write(&workspace, "file-000000", &content);
    let before = owner.client().state(workspace.route()).unwrap();
    let failure = workspace
        .commit_captured(&CommitOptions::default())
        .unwrap_err();
    assert!(matches!(
        failure.error,
        CommitError::History(HistoryError::Unsupported(_))
    ));
    assert!(failure.capture.is_none());
    assert!(failure.recorded.is_none());
    assert_eq!(owner.client().state(workspace.route()).unwrap(), before);
    assert_eq!(workspace.snapshot().unwrap(), initial);
    assert_eq!(read(&workspace, "file-000000"), content);
    drop(workspace);
    store.close_reader(identity).unwrap();
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}

#[test]
fn installed_merge_with_failed_settlement_keeps_original_control_projection() {
    let fixture = support::Fixture::new_durable(2, "recorded-settlement-refusal");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        16,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let initial = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let directory = fixture.directory.join("durable");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.join("workspace.sqlite");
    let identity = WorkspaceId::from_authority([42; 32]).unwrap();
    let request = BindRequest {
        workspace: identity,
        branch: fixture.branch,
    };
    let owner = Owner::create_durable(spec(&path, initial.effective_root.to_bytes())).unwrap();
    let original = store
        .bind_durable(owner.client(), request.clone(), initial.clone())
        .unwrap()
        .workspace;
    drop(original);
    store.close_reader(identity).unwrap();
    owner.stop().unwrap();
    let overlay = rusqlite::Connection::open(&path).unwrap();
    overlay
        .execute_batch(
            "CREATE TRIGGER fail_checkpoint_finish BEFORE DELETE ON durable_checkpoint_attempt \
         BEGIN SELECT RAISE(ABORT, 'injected finish failure'); END;",
        )
        .unwrap();
    drop(overlay);
    let (owner, recorded) =
        Owner::recover_recorded(&path, ProfileConfig::default(), [42; 32], 2).unwrap();
    let original = store
        .bind_durable(owner.client(), request, recorded)
        .unwrap()
        .workspace;
    let mut ours = support::bytes(0);
    ours[0] = b'X';
    write(&original, "file-000000", &ours);

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
    let mut theirs = support::bytes(1);
    theirs[0] = b'Y';
    write(&other, "file-000001", &theirs);
    other.commit_captured(&CommitOptions::default()).unwrap();
    other.close().unwrap();
    drop(other);
    other_owner.stop().unwrap();

    let service = Service::new(store.clone(), &owner);
    let token = service.register_durable(original).unwrap();
    let command = Request::Commit(CommitControl {
        token,
        force: false,
        message: None,
    });
    let failure = service.execute_control(&command).unwrap_err();
    let Failure::After { cause, original } = failure else {
        panic!("expected known local install and settlement refusal")
    };
    let Failure::Commit(failed) = *cause else {
        panic!("expected original Commit settlement failure")
    };
    let CommitError::Owner(OwnerError::Attempted(attempted)) = &failed.error else {
        panic!("expected attempted owner failure: {:?}", failed.error)
    };
    let OwnerError::Overlay(OverlayError::Sql(rusqlite::Error::SqliteFailure(
        error,
        Some(message),
    ))) = attempted.cause.as_ref()
    else {
        panic!(
            "expected injected SQLite trigger failure: {:?}",
            attempted.cause
        )
    };
    assert_eq!(
        error.extended_code,
        rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER
    );
    assert_eq!(message, "injected finish failure");
    let commit = original.commit.expect("original installed Commit");
    assert!(commit.replaced.is_some());
    assert!(matches!(
        commit.history,
        Some(CommitOutcome::Committed { .. })
    ));
    let status = service.execute_control(&Request::Status(token)).unwrap();
    let Reply::Status(status) = status.reply else {
        panic!("expected status")
    };
    assert_eq!(status.activity, Activity::LocalFailure);
    assert_eq!(
        status.binding,
        store
            .history()
            .branch_snapshot(fixture.branch)
            .unwrap()
            .unwrap()
    );
    assert_eq!(status.binding, owner.client().durable_snapshot().unwrap());
    assert!(status.published.is_some());
    assert!(service.execute_control(&command).is_err());
    drop(service);
    store.close_reader(identity).unwrap();
    owner.stop().unwrap();
    drop(store);
    let overlay = rusqlite::Connection::open(path).unwrap();
    let phase: i64 = overlay
        .query_row("SELECT phase FROM durable_checkpoint_attempt", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(phase, 4);
    drop(overlay);
    let history = rusqlite::Connection::open(&fixture.config.path).unwrap();
    let receipts: i64 = history
        .query_row("SELECT count(*) FROM checkpoint_receipt", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(receipts, 1);
    drop(history);
    fixture.cleanup();
}

#[test]
fn recorded_construction_failure_folds_before_next_new_commit() {
    let fixture = support::Fixture::new_durable(1, "recorded-prepublication-failure");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        16,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let initial = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let directory = fixture.directory.join("durable");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.join("workspace.sqlite");
    let identity = WorkspaceId::from_authority([42; 32]).unwrap();
    let owner = Owner::create_durable(spec(&path, initial.effective_root.to_bytes())).unwrap();
    let workspace = store
        .bind_durable(
            owner.client(),
            BindRequest {
                workspace: identity,
                branch: fixture.branch,
            },
            initial.clone(),
        )
        .unwrap()
        .workspace;
    let mut content = support::bytes(0);
    content[0] = b'X';
    write(&workspace, "file-000000", &content);
    let failed = workspace
        .commit(|_, _, _| Err(CommitError::Context("original construction refusal")))
        .unwrap_err();
    assert!(matches!(
        failed.error,
        CommitError::Context("original construction refusal")
    ));
    assert!(failed.recorded.is_some());
    assert!(!failed.publication_prepared);
    assert!(failed.published.is_none());
    assert!(failed.locally_settled);
    assert_eq!(workspace.snapshot().unwrap(), initial);
    assert_eq!(read(&workspace, "file-000000"), content);
    assert!(matches!(
        workspace
            .commit_captured(&CommitOptions::default())
            .unwrap()
            .history,
        Some(CommitOutcome::Committed { .. })
    ));
    drop(workspace);
    store.close_reader(identity).unwrap();
    owner.stop().unwrap();
    drop(store);
    let history = rusqlite::Connection::open(&fixture.config.path).unwrap();
    let receipts: i64 = history
        .query_row("SELECT count(*) FROM checkpoint_receipt", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(receipts, 1);
    drop(history);
    fixture.cleanup();
}

#[test]
fn legacy_binding_requires_external_namespace_freshness() {
    let fixture = support::Fixture::new(1, "durable-bind-legacy");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        16,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let snapshot = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let directory = fixture.directory.join("durable");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.join("workspace.sqlite");
    let identity = WorkspaceId::from_authority([42; 32]).unwrap();
    let owner = Owner::create_durable(spec(&path, snapshot.effective_root.to_bytes())).unwrap();
    let binding = store
        .bind_durable(
            owner.client(),
            BindRequest {
                workspace: identity,
                branch: fixture.branch,
            },
            snapshot.clone(),
        )
        .unwrap()
        .workspace;
    drop(binding);
    store.close_reader(identity).unwrap();
    owner.stop().unwrap();

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch("DROP TABLE durable_route; DROP TABLE durable_kept_live; DROP TABLE durable_custody; DROP TABLE durable_checkpoint_attempt; DROP TABLE durable_operation_owner; DROP TABLE durable_orphan_owner; DROP TABLE durable_owner_counts; PRAGMA user_version=40;")
        .unwrap();
    drop(connection);
    assert!(matches!(
        Owner::recover_recorded_next(&path, ProfileConfig::default(), [42; 32], 2),
        Err(OwnerError::Overlay(OverlayError::Invalid(
            "durable namespace freshness is not recorded"
        )))
    ));
    let (legacy, recovered) =
        Owner::recover_recorded(&path, ProfileConfig::default(), [42; 32], 2).unwrap();
    assert_eq!(recovered, snapshot);
    assert_eq!(legacy.client().bound_route().unwrap().namespace(), 2);
    legacy.stop().unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 40);
    drop(connection);
    drop(store);
    fixture.cleanup();
}
