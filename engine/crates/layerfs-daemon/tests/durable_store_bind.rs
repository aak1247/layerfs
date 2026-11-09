//! Durable service binding keeps the original Branch even after it moves.
#![cfg(target_os = "linux")]

#[path = "support/installed_store.rs"]
mod support;

use layerfs_content::filesystem::PathName;
use layerfs_daemon::{
    bootstrap::open_store,
    store::{BindRequest, BoundWorkspace, CommitOptions},
    Owner, OwnerConfig,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
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
    let fixture = support::Fixture::new(2, "durable-bind");
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

    let recovered_owner =
        Owner::recover_durable(spec(&path, initial.effective_root.to_bytes())).unwrap();
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
        .bind_durable(recovered_owner.client(), request, initial.clone())
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
    recovered.close().unwrap();
    drop(recovered);
    recovered_owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}
