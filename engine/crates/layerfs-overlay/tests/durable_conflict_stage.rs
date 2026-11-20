//! Original conflict staging is bounded and mutually exclusive with sending.
#![cfg(target_os = "linux")]

use layerfs_overlay::{
    conflict_page_digest, Changes, CheckpointCompletion, CheckpointOutcome, ConflictCompletion,
    ConflictStageInput, Inode, InodeKind, MaintenanceCursor, Overlay, OverlayError, PayloadWrite,
    ProfileConfig, CONFLICT_PAGE_BYTES,
};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

const WORKSPACE: [u8; 32] = [43; 32];
const CATALOG: [u8; 32] = [27; 32];
const ROOT: [u8; 32] = [17; 32];
const SNAPSHOT: &[u8] = b"authenticated branch snapshot";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "layerfs-conflict-stage-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
    fn path(&self) -> PathBuf {
        self.0.join("overlay.sqlite")
    }
    fn create(&self) -> Overlay {
        let db =
            Overlay::create_durable(&self.path(), ProfileConfig::default(), WORKSPACE).unwrap();
        db.open_workspace(ROOT).unwrap();
        db.initialize_durable_binding(ROOT, SNAPSHOT).unwrap();
        db.initialize_durable_namespace(7).unwrap();
        db
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_CONFLICT_STAGE_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
fn write(db: &Overlay, bytes: &[u8]) {
    let source = db.acquire_base_source(60).unwrap();
    let publication = db
        .apply(
            source,
            &Changes {
                inodes: vec![Inode {
                    serial: 60,
                    kind: InodeKind::File,
                    mode: 0o644,
                    mtime_seconds: 17,
                    mtime_nanoseconds: 123_456_789,
                    nlink: 1,
                    size: bytes.len() as u64,
                    inherited_cutoff: 0,
                    born: 0,
                    entries: 0,
                    subdirs: 0,
                }],
                write: Some(PayloadWrite {
                    serial: 60,
                    offset: 0,
                    data: Arc::from(bytes),
                }),
                ..Changes::default()
            },
        )
        .unwrap();
    db.reply_attempted(publication).unwrap();
    db.release_base_source(source).unwrap();
}
fn read(db: &Overlay, expected: &[u8]) {
    let source = db.acquire_base_source(160).unwrap();
    let inode = db.source_inode(source, 60).unwrap().unwrap();
    assert_eq!(inode.mtime_nanoseconds, 123_456_789);
    assert_eq!(
        db.source_read(source, 60, 0, expected.len() as u32)
            .unwrap()
            .unwrap()
            .data,
        expected
    );
    db.release_base_source(source).unwrap();
}
fn drain(db: &Overlay) -> u64 {
    let mut cursor = MaintenanceCursor::default();
    let mut bytes = 0;
    for _ in 0..512 {
        if db.maintenance_idle().unwrap() {
            return bytes;
        }
        let step = db.maintain(cursor).unwrap().expect("maintenance stalled");
        cursor = step.cursor;
        bytes += step.work.data_bytes;
    }
    panic!("bounded maintenance did not finish");
}
fn input<'a>(bytes: u64, final_digest: [u8; 32]) -> ConflictStageInput<'a> {
    ConflictStageInput {
        local_root: [18; 32],
        target_snapshot: b"original merge target",
        paths_count: 2,
        byte_count: bytes,
        final_digest,
    }
}
fn request_digest(request: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs/checkpoint-request/v1\0");
    hash.update(request);
    *hash.finalize().as_bytes()
}

#[test]
fn failed_final_page_folds_and_reclaims_only_the_written_prefix() {
    let fixture = Fixture::new();
    let db = fixture.create();
    write(&db, b"original");
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    write(&db, b"later");
    let first = vec![3; CONFLICT_PAGE_BYTES];
    let last = b"last";
    let first_hash = conflict_page_digest([0; 32], 0, &first).unwrap();
    let final_hash = conflict_page_digest(first_hash, 1, last).unwrap();
    let stage = db
        .begin_conflict_stage(
            recorded,
            input((first.len() + last.len()) as u64, final_hash),
        )
        .unwrap();
    let request = b"LFCP\x01unpublished";
    assert!(matches!(
        db.prepare_checkpoint_publication(
            recorded,
            request,
            request_digest(request),
            ROOT,
            SNAPSHOT
        ),
        Err(OverlayError::Invalid("checkpoint has conflict staging"))
    ));
    assert!(matches!(
        db.append_conflict_page(stage, 1, last),
        Err(OverlayError::Invalid("conflict page predecessor"))
    ));
    db.append_conflict_page(stage, 0, &first).unwrap();
    let before = db.state().unwrap();
    assert!(matches!(
        db.append_conflict_page(stage, 1, b"oops"),
        Err(OverlayError::Invalid("conflict final digest"))
    ));
    assert_eq!(db.state().unwrap(), before);
    db.fold_unpublished_checkpoint(recorded).unwrap();
    assert!(!db.folded_checkpoint_cleanup_ready(recorded).unwrap());
    let reclaimed = drain(&db);
    assert!(reclaimed >= CONFLICT_PAGE_BYTES as u64);
    assert!(db.folded_checkpoint_cleanup_ready(recorded).unwrap());
    db.finish_folded_checkpoint_cleanup(recorded).unwrap();
    read(&db, b"later");
    let next = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    db.fold_unpublished_checkpoint(next).unwrap();
    drain(&db);
    db.finish_folded_checkpoint_cleanup(next).unwrap();
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let counts:(i64,i64,i64)=sql.query_row("SELECT (SELECT count(*) FROM durable_conflict_plan),(SELECT count(*) FROM durable_conflict_page),(SELECT count(*) FROM cleanup_task WHERE kind=13)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(counts, (0, 0, 0));
}

#[test]
fn complete_stage_is_one_ordered_chain_and_cannot_follow_publication() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let first = vec![7; CONFLICT_PAGE_BYTES];
    let last = b"tail";
    let final_hash =
        conflict_page_digest(conflict_page_digest([0; 32], 0, &first).unwrap(), 1, last).unwrap();
    let stage = db
        .begin_conflict_stage(
            recorded,
            input((first.len() + last.len()) as u64, final_hash),
        )
        .unwrap();
    assert!(matches!(
        db.append_conflict_page(stage, 0, b"short"),
        Err(OverlayError::Invalid("conflict page length"))
    ));
    db.append_conflict_page(stage, 0, &first).unwrap();
    assert!(matches!(
        db.append_conflict_page(stage, 0, &first),
        Err(OverlayError::Invalid("conflict page predecessor"))
    ));
    db.append_conflict_page(stage, 1, last).unwrap();
    db.fold_unpublished_checkpoint(recorded).unwrap();
    drain(&db);
    db.finish_folded_checkpoint_cleanup(recorded).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = b"LFCP\x01publication";
    db.prepare_checkpoint_publication(recorded, request, request_digest(request), ROOT, SNAPSHOT)
        .unwrap();
    assert!(matches!(
        db.begin_conflict_stage(recorded, input(1, [0; 32])),
        Err(OverlayError::Invalid("checkpoint publication predecessor"))
    ));
}

#[test]
fn empty_staging_is_retired_without_a_page() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    db.begin_conflict_stage(recorded, input(5, [19; 32]))
        .unwrap();
    db.fold_unpublished_checkpoint(recorded).unwrap();
    assert!(!db.folded_checkpoint_cleanup_ready(recorded).unwrap());
    drain(&db);
    assert!(db.folded_checkpoint_cleanup_ready(recorded).unwrap());
    db.finish_folded_checkpoint_cleanup(recorded).unwrap();
    db.require_recorded_recovery_ready().unwrap();
}

#[test]
fn a_missing_written_page_keeps_the_attempt_and_cleanup_pending() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch(
        "CREATE TRIGGER lose_staged_page AFTER UPDATE OF state ON durable_conflict_plan \
         WHEN NEW.state=2 BEGIN DELETE FROM durable_conflict_page \
         WHERE generation=NEW.generation AND page_no=0; END;",
    )
    .unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let data = b"written";
    let digest = conflict_page_digest([0; 32], 0, data).unwrap();
    let stage = db
        .begin_conflict_stage(recorded, input(data.len() as u64, digest))
        .unwrap();
    db.append_conflict_page(stage, 0, data).unwrap();
    db.fold_unpublished_checkpoint(recorded).unwrap();
    let mut cursor = MaintenanceCursor::default();
    let mut refused = false;
    for _ in 0..256 {
        match db.maintain(cursor) {
            Ok(Some(step)) => cursor = step.cursor,
            Err(OverlayError::Invalid("conflict cleanup page")) => {
                refused = true;
                break;
            }
            other => panic!("unexpected maintenance result: {other:?}"),
        }
    }
    assert!(refused);
    assert!(!db.folded_checkpoint_cleanup_ready(recorded).unwrap());
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let (phase,completed,state,queued):(i64,i64,i64,i64)=sql.query_row(
        "SELECT a.phase,a.conflict_cleanup_completed,p.state,(SELECT count(*) FROM cleanup_task WHERE kind=13 AND resource=0 AND target=p.generation) FROM durable_checkpoint_attempt a JOIN durable_conflict_plan p ON p.attempt=a.attempt",[],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!((phase, completed, state, queued), (3, 0, 2, 1));
}

#[test]
fn invalid_target_is_rejected_before_the_capture_changes() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let before = db.state().unwrap();
    for target in [&[][..], &[7_u8; 4097][..]] {
        let bad = ConflictStageInput {
            target_snapshot: target,
            ..input(1, [0; 32])
        };
        assert!(matches!(
            db.begin_conflict_stage(recorded, bad),
            Err(OverlayError::Invalid("conflict target snapshot"))
        ));
        assert_eq!(db.state().unwrap(), before);
    }
    let request = b"LFCP\x01publication";
    db.prepare_checkpoint_publication(recorded, request, request_digest(request), ROOT, SNAPSHOT)
        .unwrap();
}

#[test]
fn an_unowned_cleanup_item_blocks_stage_and_publication() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER orphan_cleanup AFTER INSERT ON durable_checkpoint_attempt \
        BEGIN INSERT INTO cleanup_task(kind,resource,target,cursor) VALUES(13,0,NEW.generation,1); END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let before = db.state().unwrap();
    assert!(matches!(
        db.begin_conflict_stage(recorded, input(1, [0; 32])),
        Err(OverlayError::Invalid("checkpoint has conflict staging"))
    ));
    let request = b"LFCP\x01publication";
    assert!(matches!(
        db.prepare_checkpoint_publication(
            recorded,
            request,
            request_digest(request),
            ROOT,
            SNAPSHOT
        ),
        Err(OverlayError::Invalid("checkpoint has conflict staging"))
    ));
    assert!(matches!(
        db.fold_unpublished_checkpoint(recorded),
        Err(OverlayError::Invalid("conflict retirement predecessor"))
    ));
    assert_eq!(db.state().unwrap(), before);
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let evidence:(i64,i64,i64)=sql.query_row("SELECT phase,(SELECT count(*) FROM durable_conflict_plan),(SELECT cursor FROM cleanup_task WHERE kind=13 AND resource=0 AND target=durable_checkpoint_attempt.generation) FROM durable_checkpoint_attempt",[],|r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(evidence, (1, 0, 1));
}

#[test]
fn retiring_a_stage_does_not_adopt_an_existing_cleanup_cursor() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER competing_cleanup AFTER UPDATE OF conflict_generation ON durable_checkpoint_attempt \
        WHEN NEW.conflict_generation IS NOT NULL BEGIN \
        INSERT INTO cleanup_task(kind,resource,target,cursor) VALUES(13,0,NEW.generation,1); END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    db.begin_conflict_stage(recorded, input(1, [0; 32]))
        .unwrap();
    let before = db.state().unwrap();
    assert!(matches!(
        db.fold_unpublished_checkpoint(recorded),
        Err(OverlayError::Invalid("conflict retirement predecessor"))
    ));
    assert_eq!(db.state().unwrap(), before);
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let evidence:(i64,i64,i64)=sql.query_row("SELECT a.phase,p.state,(SELECT cursor FROM cleanup_task WHERE kind=13 AND resource=0 AND target=p.generation) FROM durable_checkpoint_attempt a JOIN durable_conflict_plan p ON p.attempt=a.attempt",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(evidence, (1, 0, 1));
}

#[test]
fn conflict_binding_survives_capture_and_is_retired_by_a_later_install() {
    let fixture = Fixture::new();
    let db = fixture.create();
    write(&db, b"original");
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request_id = recorded.capture().generation.number() as u64;
    let reader = db
        .acquire_recorded_captured_reader(recorded, request_id)
        .unwrap();
    let operation = db
        .acquire_recorded_captured_operation(recorded, request_id, reader)
        .unwrap();
    let paths = b"a conflict";
    let digest = conflict_page_digest([0; 32], 0, paths).unwrap();
    let stage = db
        .begin_conflict_stage(recorded, input(paths.len() as u64, digest))
        .unwrap();
    db.append_conflict_page(stage, 0, paths).unwrap();
    let target = b"original merge target";
    db.install_recorded_conflict(stage, [18; 32], target)
        .unwrap();
    assert_eq!(db.state().unwrap().base_root, [18; 32]);
    assert_eq!(
        db.durable_binding().unwrap().as_deref(),
        Some(target.as_slice())
    );
    db.release_captured_reader(reader).unwrap();
    db.release_operation(operation).unwrap();
    let conflict = ConflictCompletion {
        stage,
        marker_root: [18; 32],
        target_snapshot: target,
        reader: Some(reader),
        operation: Some(operation),
    };
    drain(&db);
    assert!(db.conflict_checkpoint_cleanup_ready(conflict).unwrap());
    db.finish_conflict_checkpoint_cleanup(conflict).unwrap();
    write(&db, b"resolved");
    let before = db.state().unwrap();
    assert!(matches!(
        db.capture_recorded(WORKSPACE, [9; 32], 12, target),
        Err(OverlayError::Invalid("recorded conflict context"))
    ));
    assert_eq!(db.state().unwrap(), before);
    let next = db.capture_recorded(WORKSPACE, CATALOG, 12, target).unwrap();
    write(&db, b"later");
    let request = b"LFCP\x01resolved";
    let digest = request_digest(request);
    let candidate = [19; 32];
    let installed = b"published snapshot";
    db.prepare_checkpoint_publication(next, request, digest, candidate, target)
        .unwrap();
    let outcome = CheckpointOutcome::UpToDate { head: None };
    db.install_recorded_checkpoint(next, digest, candidate, target, installed, outcome)
        .unwrap();
    assert_eq!(
        db.durable_binding().unwrap().as_deref(),
        Some(installed.as_slice())
    );
    drain(&db);
    let complete = CheckpointCompletion {
        recorded: next,
        request,
        request_digest: digest,
        candidate_root: candidate,
        target_snapshot: target,
        installed_snapshot: installed,
        outcome,
        reader: None,
        operation: None,
    };
    assert!(db.installed_checkpoint_cleanup_ready(complete).unwrap());
    db.finish_installed_checkpoint_cleanup(complete).unwrap();
    read(&db, b"later");
    drain(&db);
    db.close_database().unwrap();
    let recovered =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    assert_eq!(
        recovered.durable_binding().unwrap().as_deref(),
        Some(installed.as_slice())
    );
    recovered.close_database().unwrap();
}

#[test]
fn a_successor_conflict_retires_only_the_previous_active_plan() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let first = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let data = b"first";
    let stage = db
        .begin_conflict_stage(
            first,
            input(
                data.len() as u64,
                conflict_page_digest([0; 32], 0, data).unwrap(),
            ),
        )
        .unwrap();
    db.append_conflict_page(stage, 0, data).unwrap();
    let target = b"original merge target";
    db.install_recorded_conflict(stage, [18; 32], target)
        .unwrap();
    let done = ConflictCompletion {
        stage,
        marker_root: [18; 32],
        target_snapshot: target,
        reader: None,
        operation: None,
    };
    drain(&db);
    db.finish_conflict_checkpoint_cleanup(done).unwrap();
    let second = db.capture_recorded(WORKSPACE, CATALOG, 12, target).unwrap();
    let data = b"second";
    let target2 = b"second merge target";
    let plan = ConflictStageInput {
        local_root: [20; 32],
        target_snapshot: target2,
        paths_count: 1,
        byte_count: data.len() as u64,
        final_digest: conflict_page_digest([0; 32], 0, data).unwrap(),
    };
    let stage2 = db.begin_conflict_stage(second, plan).unwrap();
    db.append_conflict_page(stage2, 0, data).unwrap();
    db.install_recorded_conflict(stage2, [20; 32], target2)
        .unwrap();
    drain(&db);
    let done2 = ConflictCompletion {
        stage: stage2,
        marker_root: [20; 32],
        target_snapshot: target2,
        reader: None,
        operation: None,
    };
    db.finish_conflict_checkpoint_cleanup(done2).unwrap();
    assert_eq!(
        db.durable_binding().unwrap().as_deref(),
        Some(target2.as_slice())
    );
    assert!(matches!(
        db.require_recorded_recovery_ready(),
        Err(OverlayError::Invalid(
            "durable conflict requires reconciliation"
        ))
    ));
}

#[test]
fn conflict_install_rejects_a_newly_occupied_cleanup_key_before_switching() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER occupied_install AFTER UPDATE OF next_page ON durable_conflict_plan \
       WHEN NEW.next_page=NEW.page_count BEGIN INSERT INTO cleanup_task(kind,resource,target,cursor) VALUES(13,0,NEW.generation,1); END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let data = b"page";
    let stage = db
        .begin_conflict_stage(
            recorded,
            input(
                data.len() as u64,
                conflict_page_digest([0; 32], 0, data).unwrap(),
            ),
        )
        .unwrap();
    db.append_conflict_page(stage, 0, data).unwrap();
    let before = db.state().unwrap();
    assert!(matches!(
        db.install_recorded_conflict(stage, [18; 32], b"original merge target"),
        Err(OverlayError::Invalid("conflict install predecessor"))
    ));
    assert_eq!(db.state().unwrap(), before);
    assert_eq!(db.durable_binding().unwrap().as_deref(), Some(SNAPSHOT));
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let evidence:(i64,i64,i64)=sql.query_row("SELECT a.phase,p.state,(SELECT cursor FROM cleanup_task WHERE kind=13 AND resource=0 AND target=p.generation) FROM durable_checkpoint_attempt a JOIN durable_conflict_plan p ON p.attempt=a.attempt",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(evidence, (1, 0, 1));
}

#[test]
fn retired_conflict_rejects_a_different_successor_without_deleting_pages() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER corrupt_successor AFTER INSERT ON cleanup_task WHEN NEW.kind=13 AND NEW.phase=1 BEGIN \
       UPDATE cleanup_task SET name=CAST(substr(name,1,32)||zeroblob(32) AS BLOB) \
       WHERE kind=NEW.kind AND resource=NEW.resource AND target=NEW.target; END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let first = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let data = b"first";
    let stage = db
        .begin_conflict_stage(
            first,
            input(
                data.len() as u64,
                conflict_page_digest([0; 32], 0, data).unwrap(),
            ),
        )
        .unwrap();
    db.append_conflict_page(stage, 0, data).unwrap();
    let target = b"original merge target";
    db.install_recorded_conflict(stage, [18; 32], target)
        .unwrap();
    let done = ConflictCompletion {
        stage,
        marker_root: [18; 32],
        target_snapshot: target,
        reader: None,
        operation: None,
    };
    drain(&db);
    db.finish_conflict_checkpoint_cleanup(done).unwrap();
    let next = db.capture_recorded(WORKSPACE, CATALOG, 12, target).unwrap();
    let request = b"LFCP\x01resolved";
    let digest = request_digest(request);
    let candidate = [19; 32];
    let installed = b"published snapshot";
    db.prepare_checkpoint_publication(next, request, digest, candidate, target)
        .unwrap();
    db.install_recorded_checkpoint(
        next,
        digest,
        candidate,
        target,
        installed,
        CheckpointOutcome::UpToDate { head: None },
    )
    .unwrap();
    let mut cursor = MaintenanceCursor::default();
    let mut refused = false;
    for _ in 0..256 {
        match db.maintain(cursor) {
            Ok(Some(step)) => cursor = step.cursor,
            Err(OverlayError::Invalid("conflict retired identity")) => {
                refused = true;
                break;
            }
            other => panic!("unexpected maintenance result: {other:?}"),
        }
    }
    assert!(refused);
    assert_eq!(
        db.durable_binding().unwrap().as_deref(),
        Some(installed.as_slice())
    );
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let evidence:(i64,i64,i64,i64)=sql.query_row("SELECT a.phase,p.state,(SELECT count(*) FROM durable_conflict_page WHERE generation=p.generation),(SELECT count(*) FROM cleanup_task WHERE kind=13 AND resource=0 AND target=p.generation) FROM durable_checkpoint_attempt a JOIN durable_conflict_plan p ON p.state=2",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(evidence, (4, 2, 1, 1));
}
