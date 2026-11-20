//! Durable service binding keeps the original Branch even after it moves.
#![cfg(target_os = "linux")]

#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{
    Activity, CommitControl, Reply, Request, UnmountControl, WorkspaceToken,
};
use layerfs_content::filesystem::PathName;
use layerfs_daemon::{
    bootstrap::open_store,
    control::{Failure, Service},
    durable_roster::{Authority, Roster, WorkspaceRecord},
    store::{BindRequest, BoundWorkspace, CommitError, CommitOptions},
    Owner, OwnerConfig, OwnerError,
};
use layerfs_history::{CommitOutcome, HistoryError, WorkspaceId};
use layerfs_overlay::{
    conflict_page_digest, ConflictCompletion, ConflictStageInput, MaintenanceCursor, Overlay,
    OverlayError, ProfileConfig, CONFLICT_PAGE_BYTES,
};
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
    access_operation(&operation, action)
}
fn access_operation<T>(
    operation: &layerfs_daemon::store::StoreOperation,
    action: impl FnOnce(&layerfs_daemon::store::StoreOperation, &layerfs_workspace::SourceView) -> T,
) -> T {
    let overlay = operation.overlay();
    let route = operation.workspace().route();
    let source = overlay.acquire_base_source(route, 7).unwrap();
    let view = operation.workspace().view_for_source(source).unwrap();
    let result = action(operation, &view);
    drop(view);
    overlay.release_base_source(route, source).unwrap();
    result
}

#[test]
fn settled_conflict_recovers_original_catalog_workspace_and_resolves_once() {
    let fixture = support::Fixture::new_durable(1, "recorded-cold-conflict");
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
    let catalog = fixture.directory.join("cold-conflict-catalog");
    let overlays = fixture.directory.join("cold-conflict-overlays");
    for path in [&catalog, &overlays] {
        std::fs::create_dir(path).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let id1 = WorkspaceId::from_authority([42; 32]).unwrap();
    let id2 = WorkspaceId::from_authority([43; 32]).unwrap();
    let record = WorkspaceRecord {
        workspace: id2,
        branch: fixture.branch,
    };
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut roster = Roster::create(&catalog, authority).unwrap();
    roster.record(record).unwrap();
    let first_path = fixture.directory.join("cold-conflict-first.sqlite");
    let second_path = overlays.join(record.overlay_name());
    let owner1 =
        Owner::create_durable(spec(&first_path, initial.effective_root.to_bytes())).unwrap();
    let mut second_spec = spec(&second_path, initial.effective_root.to_bytes());
    second_spec.identity = [43; 32];
    second_spec.namespace = 2;
    let owner2 = Owner::create_durable(second_spec).unwrap();
    let first = store
        .bind_durable(
            owner1.client(),
            BindRequest {
                workspace: id1,
                branch: fixture.branch,
            },
            initial.clone(),
        )
        .unwrap()
        .workspace;
    let second = store
        .bind_durable(
            owner2.client(),
            BindRequest {
                workspace: id2,
                branch: fixture.branch,
            },
            initial,
        )
        .unwrap()
        .workspace;
    let mut ours = support::bytes(0);
    ours[0] = b'X';
    let mut theirs = support::bytes(0);
    theirs[0] = b'Y';
    write(&first, "file-000000", &ours);
    write(&second, "file-000000", &theirs);
    first.commit_captured(&CommitOptions::default()).unwrap();
    let winning = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let conflict = second.commit_captured(&CommitOptions::default()).unwrap();
    assert!(conflict.history.is_none());
    assert_eq!(
        second.pending_conflicts().unwrap(),
        Some(vec!["/file-000000".into()])
    );
    let marker = read(&second, "file-000000");
    assert!(String::from_utf8(marker.clone())
        .unwrap()
        .contains("<<<<<<< workspace"));
    drop(first);
    drop(second);
    store.close_reader(id1).unwrap();
    store.close_reader(id2).unwrap();
    owner1.stop().unwrap();
    owner2.stop().unwrap();

    let recovered_owner = Owner::start(
        &fixture.directory.join("cold-conflict-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let service = Service::new(store.clone(), &recovered_owner);
    let recovered = service
        .recover_catalog(
            &mut roster,
            &recovered_owner,
            &overlays,
            [8; 32],
            ProfileConfig::default(),
            3,
        )
        .unwrap();
    assert_eq!(recovered.len(), 1);
    let token = recovered[0];
    assert_eq!(token.workspace, id2);
    assert!(token.namespace > 2);
    assert!(service
        .operation(WorkspaceToken {
            workspace: id2,
            namespace: 2
        })
        .is_err());
    let operation = service.operation(token).unwrap();
    let readback = access_operation(&operation, |operation, view| {
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
        bytes
    });
    assert_eq!(readback, marker);
    let dirty = service
        .unmount_recorded(
            &mut roster,
            UnmountControl {
                token,
                discard: false,
            },
        )
        .unwrap()
        .reply;
    assert!(
        matches!(dirty, Reply::Dirty(report) if report.pending_merge && report.paths.contains(&"/file-000000".to_string()))
    );
    let refused = service
        .execute_control(&Request::Commit(CommitControl {
            token,
            force: false,
            message: None,
        }))
        .unwrap()
        .reply;
    assert!(
        matches!(refused, Reply::Conflict(report) if report.markers && report.conflicts.iter().any(|c| c.path == "/file-000000"))
    );
    assert_eq!(
        store
            .history()
            .branch_snapshot(fixture.branch)
            .unwrap()
            .unwrap(),
        winning
    );
    access_operation(&operation, |operation, view| {
        let file = view
            .lookup(
                operation.overlay(),
                view.root_serial(),
                &PathName::new("file-000000").unwrap(),
            )
            .unwrap();
        let now = Time {
            seconds: file.metadata.mtime_seconds,
            nanoseconds: file.metadata.mtime_nanoseconds,
        };
        for change in [
            Operation::SetAttributes {
                serial: file.serial,
                mode: None,
                mtime: None,
                size: Some(0),
            },
            Operation::Write {
                serial: file.serial,
                position: Position::At(0),
                data: b"resolved\n".as_slice().into(),
            },
        ] {
            let Outcome::Applied { publication, .. } = operation
                .workspace()
                .mutate(operation.overlay(), operation.ports(), view, change, now)
                .unwrap()
            else {
                panic!("resolution was not applied")
            };
            operation.overlay().reply_attempted(publication).unwrap();
        }
    });
    drop(operation);
    let published = service
        .execute_control(&Request::Commit(CommitControl {
            token,
            force: false,
            message: None,
        }))
        .unwrap()
        .reply;
    assert!(matches!(
        published,
        Reply::Committed(_) | Reply::MergeCommitted(_)
    ));
    drop(service);
    store.close_reader(id2).unwrap();
    recovered_owner.stop().unwrap();
    drop(roster);
    drop(store);
    let history = rusqlite::Connection::open(&fixture.config.path).unwrap();
    let receipts: (i64, i64) = history
        .query_row(
            "SELECT count(*),count(DISTINCT attempt_id) FROM checkpoint_receipt",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(receipts, (2, 2));
    drop(history);
    fixture.cleanup();
}

#[test]
fn catalog_conflict_paths_preserve_cross_page_utf8_order_and_duplicates() {
    let fixture = support::Fixture::new_durable(1, "catalog-conflict-pages");
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
    let id = WorkspaceId::from_authority([44; 32]).unwrap();
    let record = WorkspaceRecord {
        workspace: id,
        branch: fixture.branch,
    };
    let catalog = fixture.directory.join("pages-catalog");
    let overlays = fixture.directory.join("pages-overlays");
    for path in [&catalog, &overlays] {
        std::fs::create_dir(path).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let mut roster = Roster::create(
        &catalog,
        Authority::for_catalog([8; 32], store.history()).unwrap(),
    )
    .unwrap();
    roster.record(record).unwrap();
    let path = overlays.join(record.overlay_name());
    let db = Overlay::create_durable(&path, ProfileConfig::default(), id.to_bytes()).unwrap();
    let root = snapshot.effective_root.to_bytes();
    let original = layerfs_bridge::snapshot::encode(&snapshot).unwrap();
    db.open_workspace(root).unwrap();
    db.initialize_durable_binding(root, &original).unwrap();
    db.initialize_durable_namespace(1).unwrap();
    let recorded = db
        .capture_recorded(
            id.to_bytes(),
            store.history().catalog_id().to_bytes(),
            store.history().incarnation(),
            &original,
        )
        .unwrap();
    // The first length prefix and a UTF-8 code point each cross a page boundary.
    let first = format!("/{}", "a".repeat(CONFLICT_PAGE_BYTES - 18));
    let second = format!("{}中", "b".repeat(CONFLICT_PAGE_BYTES - 5));
    let paths = vec![first.clone(), second, first];
    let mut bytes = b"LFCM\x01".to_vec();
    for value in &paths {
        bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    assert_eq!(
        (5 + 8 + paths[0].len()) % CONFLICT_PAGE_BYTES,
        CONFLICT_PAGE_BYTES - 4
    );
    assert_eq!(
        (5 + 8 + paths[0].len() + 8 + paths[1].len() - "中".len()) % CONFLICT_PAGE_BYTES,
        CONFLICT_PAGE_BYTES - 1
    );
    let mut digest = [0; 32];
    for (number, page) in bytes.chunks(CONFLICT_PAGE_BYTES).enumerate() {
        digest = conflict_page_digest(digest, number as u64, page).unwrap();
    }
    let stage = db
        .begin_conflict_stage(
            recorded,
            ConflictStageInput {
                local_root: root,
                target_snapshot: &original,
                paths_count: paths.len() as u64,
                byte_count: bytes.len() as u64,
                final_digest: digest,
            },
        )
        .unwrap();
    for (number, page) in bytes.chunks(CONFLICT_PAGE_BYTES).enumerate() {
        db.append_conflict_page(stage, number as u64, page).unwrap();
    }
    db.install_recorded_conflict(stage, root, &original)
        .unwrap();
    let completion = ConflictCompletion {
        stage,
        marker_root: root,
        target_snapshot: &original,
        reader: None,
        operation: None,
    };
    let mut cursor = MaintenanceCursor::default();
    for _ in 0..512 {
        if db.maintenance_idle().unwrap() {
            break;
        }
        cursor = db
            .maintain(cursor)
            .unwrap()
            .expect("maintenance stalled")
            .cursor;
    }
    assert!(db.maintenance_idle().unwrap());
    assert!(db.conflict_checkpoint_cleanup_ready(completion).unwrap());
    db.finish_conflict_checkpoint_cleanup(completion).unwrap();
    drop(db);

    let mut records = Vec::new();
    roster.visit(|entry| records.push(entry)).unwrap();
    assert_eq!(records, [record]);
    let request = BindRequest {
        workspace: id,
        branch: fixture.branch,
    };
    let prepared = store
        .prepare_catalog_recovery(&path, ProfileConfig::default(), request.clone())
        .unwrap();
    records.clear();
    roster.visit(|entry| records.push(entry)).unwrap();
    assert_eq!(records, [record]);
    let (recovered, proof) = prepared.activate(2).unwrap();
    let owner = Owner::start(
        &fixture.directory.join("pages-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 1 },
    )
    .unwrap();
    let client = owner.adopt_durable(recovered).unwrap();
    let bound = store
        .bind_catalog_recovery(client, request, proof)
        .unwrap()
        .workspace;
    assert_eq!(bound.identity(), id);
    assert!(bound.route().namespace() > 1);
    assert_eq!(bound.pending_conflicts().unwrap(), Some(paths));
    drop(bound);
    store.close_reader(id).unwrap();
    owner.stop().unwrap();
    drop(roster);
    drop(store);
    fixture.cleanup();
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

fn truncate(workspace: &BoundWorkspace, name: &str) {
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
                Operation::SetAttributes {
                    serial: file.serial,
                    mode: None,
                    mtime: None,
                    size: Some(0),
                },
                Time {
                    seconds: file.metadata.mtime_seconds,
                    nanoseconds: file.metadata.mtime_nanoseconds,
                },
            )
            .unwrap();
        let Outcome::Applied { publication, .. } = result else {
            panic!("truncate was not applied")
        };
        operation.overlay().reply_attempted(publication).unwrap();
    });
}

#[test]
fn recorded_conflict_keeps_markers_until_resolved_then_publishes_once() {
    let fixture = support::Fixture::new_durable(1, "recorded-live-conflict");
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
    let directory = fixture.directory.join("conflict-durable");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let one = directory.join("one.sqlite");
    let two = directory.join("two.sqlite");
    let id1 = WorkspaceId::from_authority([42; 32]).unwrap();
    let id2 = WorkspaceId::from_authority([43; 32]).unwrap();
    let owner1 = Owner::create_durable(spec(&one, initial.effective_root.to_bytes())).unwrap();
    let mut second_spec = spec(&two, initial.effective_root.to_bytes());
    second_spec.identity = [43; 32];
    second_spec.namespace = 2;
    let owner2 = Owner::create_durable(second_spec).unwrap();
    let first = store
        .bind_durable(
            owner1.client(),
            BindRequest {
                workspace: id1,
                branch: fixture.branch,
            },
            initial.clone(),
        )
        .unwrap()
        .workspace;
    let second = store
        .bind_durable(
            owner2.client(),
            BindRequest {
                workspace: id2,
                branch: fixture.branch,
            },
            initial,
        )
        .unwrap()
        .workspace;
    let mut ours = support::bytes(0);
    ours[0] = b'X';
    let mut theirs = support::bytes(0);
    theirs[0] = b'Y';
    write(&first, "file-000000", &ours);
    write(&second, "file-000000", &theirs);
    let first_result = first.commit_captured(&CommitOptions::default()).unwrap();
    assert!(matches!(
        first_result.history,
        Some(CommitOutcome::Committed { .. })
    ));
    let winning = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let conflicted = second.commit_captured(&CommitOptions::default()).unwrap();
    assert!(conflicted.history.is_none());
    let report = conflicted.merge.as_ref().unwrap();
    let paths: Vec<_> = report
        .conflicts
        .iter()
        .map(|entry| entry.path.clone())
        .collect();
    assert_eq!(paths, ["/file-000000"]);
    assert_eq!(second.pending_conflicts().unwrap(), Some(paths));
    assert_eq!(second.snapshot().unwrap(), winning);
    assert_eq!(
        store
            .history()
            .branch_snapshot(fixture.branch)
            .unwrap()
            .unwrap(),
        winning
    );
    assert!(String::from_utf8(read(&second, "file-000000"))
        .unwrap()
        .contains("<<<<<<< workspace"));
    assert_eq!(read(&first, "file-000000"), ours);
    let refused = second
        .commit_captured(&CommitOptions::default())
        .unwrap_err();
    assert!(matches!(refused.error, CommitError::Markers(_)));
    assert!(refused.locally_settled);
    assert_eq!(
        store
            .history()
            .branch_snapshot(fixture.branch)
            .unwrap()
            .unwrap(),
        winning
    );
    truncate(&second, "file-000000");
    write(&second, "file-000000", b"resolved\n");
    let resolved = second.commit_captured(&CommitOptions::default()).unwrap();
    assert!(matches!(
        resolved.history,
        Some(CommitOutcome::Committed { .. })
    ));
    assert!(second.pending_conflicts().unwrap().is_none());
    assert_eq!(read(&second, "file-000000"), b"resolved\n");
    drop(first);
    drop(second);
    store.close_reader(id1).unwrap();
    store.close_reader(id2).unwrap();
    owner1.stop().unwrap();
    owner2.stop().unwrap();
    drop(store);
    let history = rusqlite::Connection::open(&fixture.config.path).unwrap();
    let receipts: (i64, i64) = history
        .query_row(
            "SELECT count(*),count(DISTINCT attempt_id) FROM checkpoint_receipt",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(receipts, (2, 2));
    drop(history);
    fixture.cleanup();
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
