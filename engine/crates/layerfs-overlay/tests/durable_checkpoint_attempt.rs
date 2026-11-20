//! The original capture and its prepublication identity share one transaction.
#![cfg(target_os = "linux")]

use layerfs_overlay::{
    Changes, CheckpointOutcome, CleanupState, Inode, InodeKind, MaintenanceCursor, Overlay,
    OverlayError, ProfileConfig,
};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const WORKSPACE: [u8; 32] = [43; 32];
const CATALOG: [u8; 32] = [27; 32];
const ROOT: [u8; 32] = [17; 32];
const SNAPSHOT: &[u8] = b"authenticated original branch snapshot";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "layerfs-checkpoint-attempt-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        Self(root)
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
        let source = db.acquire_base_source(1).unwrap();
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
                        size: 0,
                        inherited_cutoff: 0,
                        born: 0,
                        entries: 0,
                        subdirs: 0,
                    }],
                    ..Changes::default()
                },
            )
            .unwrap();
        db.reply_attempted(publication).unwrap();
        db.release_base_source(source).unwrap();
        db
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_CHECKPOINT_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

#[test]
fn capture_and_original_attempt_are_persisted_together_without_granting_recovery() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let original = db.state().unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let capture = recorded.capture();
    assert_eq!(capture.generation, original.active);
    assert_eq!(capture.revision, original.revision);
    assert_eq!(capture.base_root, ROOT);
    assert_ne!(recorded.attempt(), [0; 32]);
    assert!(matches!(
        db.resolve_failed_capture(capture),
        Err(OverlayError::Invalid(
            "recorded capture requires disposition"
        ))
    ));
    assert!(matches!(
        db.install_recorded(capture, ROOT, SNAPSHOT),
        Err(OverlayError::Invalid(
            "recorded capture requires disposition"
        ))
    ));
    db.close().unwrap();
    assert!(matches!(
        db.release_closed_capture(capture),
        Err(OverlayError::Invalid(
            "recorded capture requires disposition"
        ))
    ));
    assert_eq!(db.cleanup_state().unwrap(), CleanupState::Held);
    drop(db);

    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (Vec<u8>, Vec<u8>, Vec<u8>, i64, i64, i64, i64, Vec<u8>, Vec<u8>, i64) = connection.query_row(
        "SELECT attempt,workspace,catalog,incarnation,namespace,generation,revision,base_root,snapshot,phase FROM durable_checkpoint_attempt WHERE id=1",
        [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?)),
    ).unwrap();
    assert_eq!(row.0, recorded.attempt());
    assert_eq!(row.1, WORKSPACE);
    assert_eq!(row.2, CATALOG);
    assert_eq!((row.3, row.4), (12, 7));
    assert_eq!(
        (row.5, row.6),
        (capture.generation.number(), capture.revision)
    );
    assert_eq!(row.7, ROOT);
    assert_eq!(row.8, SNAPSHOT);
    assert_eq!(row.9, 1);
    drop(connection);
    let before = fs::read(fixture.path()).unwrap();
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE),
        Err(OverlayError::Invalid(
            "durable overlay requires reconciliation"
        ))
    ));
    assert_eq!(fs::read(fixture.path()).unwrap(), before);
}

#[test]
fn bad_identity_or_failed_attempt_insert_rolls_back_the_capture() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let before = db.state().unwrap();
    assert!(db
        .capture_recorded([42; 32], CATALOG, 12, SNAPSHOT)
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    assert!(db
        .capture_recorded(WORKSPACE, CATALOG, 12, b"wrong snapshot")
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    db.close_database().unwrap();

    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_attempt BEFORE INSERT ON durable_checkpoint_attempt BEGIN SELECT RAISE(ABORT, 'injected attempt failure'); END;").unwrap();
    drop(connection);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    assert!(db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    assert_eq!(db.active_inodes(0).unwrap()[0].serial, 60);
    db.close_database().unwrap();
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM durable_checkpoint_attempt", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    let persisted: (Option<i64>, i64) = connection
        .query_row(
            "SELECT captured,dirty_inodes FROM durable_state WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(persisted, (None, 1));
}

#[test]
fn catalog_identity_preserves_the_full_history_domain() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, [0; 32], 12, SNAPSHOT)
        .unwrap();
    assert_ne!(recorded.attempt(), [0; 32]);
    drop(db);
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    let catalog: Vec<u8> = connection
        .query_row(
            "SELECT catalog FROM durable_checkpoint_attempt WHERE id=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(catalog, [0; 32]);
}

fn request_digest(request: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs/checkpoint-request/v1\0");
    hash.update(request);
    *hash.finalize().as_bytes()
}

#[test]
fn publication_intent_is_frozen_once_for_the_original_capture() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let capture = recorded.capture();
    let request = b"LFCP\x01original complete request";
    let target = b"authenticated target branch snapshot";
    let candidate = [18; 32];
    assert!(db
        .prepare_checkpoint_publication(recorded, request, [0; 32], candidate, target)
        .is_err());
    assert!(db
        .prepare_checkpoint_publication(recorded, request, request_digest(request), candidate, b"")
        .is_err());
    assert_eq!(db.state().unwrap().captured, Some(capture.generation));
    db.prepare_checkpoint_publication(
        recorded,
        request,
        request_digest(request),
        candidate,
        target,
    )
    .unwrap();
    assert!(matches!(
        db.prepare_checkpoint_publication(
            recorded,
            request,
            request_digest(request),
            candidate,
            target
        ),
        Err(OverlayError::Invalid("checkpoint publication predecessor"))
    ));
    assert!(db.install_recorded(capture, candidate, target).is_err());
    assert!(db.resolve_failed_capture(capture).is_err());
    assert!(db.fold_unpublished_checkpoint(recorded).is_err());
    drop(db);

    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (Vec<u8>, i64, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) = sql.query_row(
        "SELECT attempt,phase,request,request_digest,candidate_root,target_snapshot FROM durable_checkpoint_attempt WHERE id=1",
        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
    ).unwrap();
    assert_eq!(
        row,
        (
            recorded.attempt().to_vec(),
            2,
            request.to_vec(),
            request_digest(request).to_vec(),
            candidate.to_vec(),
            target.to_vec()
        )
    );
    drop(sql);
    let before = fs::read(fixture.path()).unwrap();
    assert!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).is_err()
    );
    assert_eq!(fs::read(fixture.path()).unwrap(), before);
}

#[test]
fn failed_publication_intent_write_keeps_original_capture_and_phase() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_publication_intent BEFORE UPDATE ON durable_checkpoint_attempt BEGIN SELECT RAISE(ABORT, 'intent failure'); END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let before = db.state().unwrap();
    let request = b"LFCP\x01request";
    assert!(db
        .prepare_checkpoint_publication(
            recorded,
            request,
            request_digest(request),
            [18; 32],
            SNAPSHOT
        )
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (i64, Option<Vec<u8>>, Option<Vec<u8>>) = sql
        .query_row(
            "SELECT phase,request,request_digest FROM durable_checkpoint_attempt WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(row, (1, None, None));
}

#[test]
fn changed_original_attempt_context_cannot_be_frozen_for_publication() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER alter_capture_context AFTER INSERT ON durable_checkpoint_attempt BEGIN UPDATE durable_checkpoint_attempt SET catalog=zeroblob(32) WHERE id=1; END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let before = db.state().unwrap();
    let request = b"LFCP\x01request";
    assert!(matches!(
        db.prepare_checkpoint_publication(
            recorded,
            request,
            request_digest(request),
            [18; 32],
            SNAPSHOT
        ),
        Err(OverlayError::Invalid("checkpoint original identity"))
    ));
    assert_eq!(db.state().unwrap(), before);
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let (phase, request): (i64, Option<Vec<u8>>) = sql
        .query_row(
            "SELECT phase,request FROM durable_checkpoint_attempt WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((phase, request), (1, None));
}

#[test]
fn recorded_reader_identity_survives_before_any_operation_is_acquired() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(recorded.capture().generation.number()).unwrap();
    assert!(db
        .acquire_captured_reader(recorded.capture(), request)
        .is_err());
    let reader = db
        .acquire_recorded_captured_reader(recorded, request)
        .unwrap();
    assert!(db.acquire_captured_operation(request, reader).is_err());
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (i64, i64, i64, i64, Option<i64>, Option<i64>) = sql.query_row(
        "SELECT reader_owner,reader_request,reader_floor,reader_released,operation_owner,operation_released FROM durable_checkpoint_attempt WHERE id=1",
        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
    ).unwrap();
    assert_eq!(
        row,
        (
            reader.owner_id() as i64,
            request as i64,
            reader.installed_floor() as i64,
            0,
            None,
            None
        )
    );
    drop(sql);
    assert!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).is_err()
    );
}

#[test]
fn recorded_owner_release_keeps_exact_ids_through_existing_cleanup_transfer() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(recorded.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(recorded, request)
        .unwrap();
    let operation = db
        .acquire_recorded_captured_operation(recorded, request, reader)
        .unwrap();
    assert!(db
        .acquire_recorded_captured_reader(recorded, request)
        .is_err());
    db.release_captured_reader(reader).unwrap();
    db.release_operation(operation).unwrap();
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (i64, i64, i64, i64) = sql.query_row(
        "SELECT reader_owner,reader_released,operation_owner,operation_released FROM durable_checkpoint_attempt WHERE id=1",
        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    ).unwrap();
    assert_eq!(row.0, reader.owner_id() as i64);
    assert_eq!((row.1, row.3), (1, 1));
    assert!(row.2 > 0);
    let cleanup: i64 = sql
        .query_row(
            "SELECT count(*) FROM cleanup_task WHERE kind=6 AND resource=?1",
            [row.2],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cleanup, 1);
    let owners: i64 = sql
        .query_row(
            "SELECT operation_count FROM durable_owner_counts WHERE id=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(owners, 0);
}

#[test]
fn failed_recorded_acquisitions_roll_back_original_owner_maps() {
    for operation_failure in [false, true] {
        let fixture = Fixture::new();
        fixture.create().close_database().unwrap();
        let sql = rusqlite::Connection::open(fixture.path()).unwrap();
        let guard = if operation_failure {
            "NEW.operation_owner IS NOT NULL AND OLD.operation_owner IS NULL"
        } else {
            "NEW.reader_owner IS NOT NULL AND OLD.reader_owner IS NULL"
        };
        sql.execute_batch(&format!("CREATE TRIGGER fail_recorded_owner BEFORE UPDATE ON durable_checkpoint_attempt WHEN {guard} BEGIN SELECT RAISE(ABORT, 'owner record failure'); END;")).unwrap();
        drop(sql);
        let db =
            Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
        let recorded = db
            .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
            .unwrap();
        let request = u64::try_from(recorded.capture().generation.number()).unwrap();
        if operation_failure {
            let reader = db
                .acquire_recorded_captured_reader(recorded, request)
                .unwrap();
            assert!(db
                .acquire_recorded_captured_operation(recorded, request, reader)
                .is_err());
            assert_eq!(db.retained_captured_reader(request).unwrap(), Some(reader));
            assert!(db.retained_operation(request).unwrap().is_none());
        } else {
            assert!(db
                .acquire_recorded_captured_reader(recorded, request)
                .is_err());
            assert!(db.retained_captured_reader(request).unwrap().is_none());
        }
        drop(db);
        let sql = rusqlite::Connection::open(fixture.path()).unwrap();
        let (reader, operation): (Option<i64>, Option<i64>) = sql
            .query_row(
                "SELECT reader_owner,operation_owner FROM durable_checkpoint_attempt WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(reader.is_some(), operation_failure);
        assert_eq!(operation, None);
        let owners: i64 = sql
            .query_row(
                "SELECT operation_count FROM durable_owner_counts WHERE id=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(owners, 0);
    }
}

#[test]
fn an_older_generic_reader_can_release_during_a_new_recorded_capture() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let older = db.capture().unwrap();
    let old_request = u64::try_from(older.generation.number()).unwrap();
    let old_reader = db.acquire_captured_reader(older, old_request).unwrap();
    db.install_recorded(older, ROOT, SNAPSHOT).unwrap();
    let current = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    assert_ne!(older.generation, current.capture().generation);
    db.release_captured_reader(old_reader).unwrap();
    assert!(db.retained_captured_reader(old_request).unwrap().is_none());
    let request = u64::try_from(current.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(current, request)
        .unwrap();
    assert_eq!(reader.capture(), current.capture());
}

#[test]
fn changed_operation_link_retains_the_original_owner_and_cleanup() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER alter_operation_link AFTER UPDATE ON durable_checkpoint_attempt WHEN NEW.operation_owner IS NOT NULL AND OLD.operation_owner IS NULL BEGIN UPDATE durable_checkpoint_attempt SET operation_owner=NEW.operation_owner+100 WHERE id=1; END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(recorded.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(recorded, request)
        .unwrap();
    let operation = db
        .acquire_recorded_captured_operation(recorded, request, reader)
        .unwrap();
    db.release_captured_reader(reader).unwrap();
    assert!(matches!(
        db.release_operation(operation),
        Err(OverlayError::Invalid(
            "checkpoint operation release identity"
        ))
    ));
    assert_eq!(db.retained_operation(request).unwrap(), Some(operation));
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let (released, owners, cleanup): (i64, i64, i64) = sql.query_row(
        "SELECT operation_released,(SELECT operation_count FROM durable_owner_counts WHERE id=1),(SELECT count(*) FROM cleanup_task WHERE kind=6) FROM durable_checkpoint_attempt WHERE id=1",
        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ).unwrap();
    assert_eq!((released, owners, cleanup), (0, 1, 0));
}

#[test]
fn prepublication_fold_retains_later_writes_and_original_custody() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(recorded.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(recorded, request)
        .unwrap();
    let operation = db
        .acquire_recorded_captured_operation(recorded, request, reader)
        .unwrap();
    let source = db.acquire_base_source(99).unwrap();
    let publication = db
        .apply(
            source,
            &Changes {
                inodes: vec![Inode {
                    serial: 61,
                    kind: InodeKind::File,
                    mode: 0o600,
                    mtime_seconds: 20,
                    mtime_nanoseconds: 987_654_321,
                    nlink: 1,
                    size: 0,
                    inherited_cutoff: 0,
                    born: 0,
                    entries: 0,
                    subdirs: 0,
                }],
                ..Changes::default()
            },
        )
        .unwrap();
    db.reply_attempted(publication).unwrap();
    db.release_base_source(source).unwrap();
    db.fold_unpublished_checkpoint(recorded).unwrap();
    assert!(db.capture().is_err());
    assert!(db.fold_unpublished_checkpoint(recorded).is_err());
    db.release_captured_reader(reader).unwrap();
    db.release_operation(operation).unwrap();
    let mut cursor = MaintenanceCursor::default();
    for _ in 0..256 {
        if db.maintenance_idle().unwrap() {
            break;
        }
        cursor = db
            .maintain(cursor)
            .unwrap()
            .expect("ready maintenance stalled")
            .cursor;
    }
    assert!(db.maintenance_idle().unwrap());
    let inodes = db.active_inodes(0).unwrap();
    assert_eq!(
        inodes.iter().map(|inode| inode.serial).collect::<Vec<_>>(),
        vec![60, 61]
    );
    assert_eq!(inodes[1].mtime_nanoseconds, 987_654_321);
    assert!(db.capture().is_err());
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (i64, i64, i64, i64, i64, i64) = sql.query_row(
        "SELECT phase,reader_released,operation_released,fold_completed,retire_completed,operation_cleanup_completed FROM durable_checkpoint_attempt WHERE id=1", [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
    ).unwrap();
    assert_eq!(row, (3, 1, 1, 1, 1, 1));
    drop(sql);
    assert!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).is_err()
    );
}

#[test]
fn failed_fold_schedule_rolls_back_phase_and_original_capture() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_checkpoint_fold BEFORE INSERT ON cleanup_task WHEN NEW.kind=1 BEGIN SELECT RAISE(ABORT, 'fold failure'); END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let before = db.state().unwrap();
    assert!(db.fold_unpublished_checkpoint(recorded).is_err());
    assert_eq!(db.state().unwrap(), before);
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let (phase, folds): (i64, i64) = sql.query_row(
        "SELECT phase,(SELECT count(*) FROM cleanup_task WHERE kind=1) FROM durable_checkpoint_attempt WHERE id=1",
        [], |r| Ok((r.get(0)?, r.get(1)?)),
    ).unwrap();
    assert_eq!((phase, folds), (1, 0));
}

#[test]
fn changed_reader_floor_cannot_release_during_scheduled_fold() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER alter_fold_floor AFTER UPDATE OF phase ON durable_checkpoint_attempt WHEN NEW.phase=3 BEGIN UPDATE durable_checkpoint_attempt SET reader_floor=0 WHERE id=1; END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let earlier = db.capture().unwrap();
    db.install_recorded(earlier, ROOT, SNAPSHOT).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(recorded.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(recorded, request)
        .unwrap();
    assert_eq!(reader.installed_floor(), 1);
    db.fold_unpublished_checkpoint(recorded).unwrap();
    assert!(matches!(
        db.release_captured_reader(reader),
        Err(OverlayError::Invalid("checkpoint fold state"))
    ));
    assert_eq!(db.retained_captured_reader(request).unwrap(), Some(reader));
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (i64, i64, i64) = sql
        .query_row(
            "SELECT phase,reader_floor,reader_released FROM durable_checkpoint_attempt WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(row, (3, 0, 0));
}

fn commit_id(tag: u8) -> [u8; 33] {
    let mut id = [tag; 33];
    id[0] = 0x12;
    id
}

#[test]
fn known_checkpoint_install_retains_original_outcome_and_later_writes() {
    for outcome in [
        CheckpointOutcome::Committed {
            head: commit_id(1),
            displaced: Some(commit_id(2)),
        },
        CheckpointOutcome::UpToDate { head: None },
    ] {
        let fixture = Fixture::new();
        let db = fixture.create();
        let recorded = db
            .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
            .unwrap();
        let request_id = u64::try_from(recorded.capture().generation.number()).unwrap();
        let reader = db
            .acquire_recorded_captured_reader(recorded, request_id)
            .unwrap();
        let operation = db
            .acquire_recorded_captured_operation(recorded, request_id, reader)
            .unwrap();
        let source = db.acquire_base_source(99).unwrap();
        let publication = db
            .apply(
                source,
                &Changes {
                    inodes: vec![Inode {
                        serial: 61,
                        kind: InodeKind::File,
                        mode: 0o600,
                        mtime_seconds: 20,
                        mtime_nanoseconds: 987_654_321,
                        nlink: 1,
                        size: 0,
                        inherited_cutoff: 0,
                        born: 0,
                        entries: 0,
                        subdirs: 0,
                    }],
                    ..Changes::default()
                },
            )
            .unwrap();
        db.reply_attempted(publication).unwrap();
        db.release_base_source(source).unwrap();
        let request = b"LFCP\x01known publication";
        let digest = request_digest(request);
        let candidate = [18; 32];
        let target = b"target snapshot";
        let installed = b"verified installed branch snapshot";
        db.prepare_checkpoint_publication(recorded, request, digest, candidate, target)
            .unwrap();
        let before = db.state().unwrap();
        assert!(db
            .install_recorded_checkpoint(recorded, [0; 32], candidate, target, installed, outcome)
            .is_err());
        assert!(db
            .install_recorded_checkpoint(recorded, digest, [19; 32], target, installed, outcome)
            .is_err());
        assert_eq!(db.state().unwrap(), before);
        db.install_recorded_checkpoint(recorded, digest, candidate, target, installed, outcome)
            .unwrap();
        let state = db.state().unwrap();
        assert_eq!(state.base_root, candidate);
        assert_eq!(state.dirty_inodes, 1);
        assert_eq!(
            db.durable_binding().unwrap().as_deref(),
            Some(installed.as_slice())
        );
        assert_eq!(db.active_inodes(0).unwrap()[0].serial, 61);
        assert!(db
            .install_recorded_checkpoint(recorded, digest, candidate, target, installed, outcome)
            .is_err());
        db.release_captured_reader(reader).unwrap();
        db.release_operation(operation).unwrap();
        let mut cursor = MaintenanceCursor::default();
        for _ in 0..256 {
            if db.maintenance_idle().unwrap() {
                break;
            }
            cursor = db
                .maintain(cursor)
                .unwrap()
                .expect("ready maintenance stalled")
                .cursor;
        }
        assert!(db.maintenance_idle().unwrap());
        assert!(db.capture().is_err());
        drop(db);
        let sql = rusqlite::Connection::open(fixture.path()).unwrap();
        let row: (i64, i64, Option<Vec<u8>>, Option<Vec<u8>>, Vec<u8>, i64, i64) = sql.query_row(
            "SELECT phase,outcome_kind,outcome_head,outcome_displaced,installed_snapshot,reader_released,operation_released FROM durable_checkpoint_attempt WHERE id=1",
            [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)),
        ).unwrap();
        let (kind, head, displaced) = match outcome {
            CheckpointOutcome::Committed { head, displaced } => {
                (1, Some(head.to_vec()), displaced.map(|id| id.to_vec()))
            }
            CheckpointOutcome::UpToDate { head } => (2, head.map(|id| id.to_vec()), None),
        };
        assert_eq!(row, (4, kind, head, displaced, installed.to_vec(), 1, 1));
        let completed: (i64, i64) = sql.query_row(
            "SELECT retire_completed,operation_cleanup_completed FROM durable_checkpoint_attempt WHERE id=1", [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ).unwrap();
        assert_eq!(completed, (1, 1));
        drop(sql);
        assert!(
            Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).is_err()
        );
    }
}

#[test]
fn failed_checkpoint_install_retains_original_pending_attempt_and_binding() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_checkpoint_retire BEFORE INSERT ON cleanup_task WHEN NEW.kind=3 BEGIN SELECT RAISE(ABORT, 'retire failure'); END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = b"LFCP\x01request";
    let digest = request_digest(request);
    db.prepare_checkpoint_publication(recorded, request, digest, [18; 32], SNAPSHOT)
        .unwrap();
    let before = db.state().unwrap();
    assert!(db
        .install_recorded_checkpoint(
            recorded,
            digest,
            [18; 32],
            SNAPSHOT,
            b"installed",
            CheckpointOutcome::UpToDate { head: None }
        )
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    assert_eq!(db.durable_binding().unwrap().as_deref(), Some(SNAPSHOT));
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let (phase, outcome, retire): (i64, Option<i64>, i64) = sql.query_row(
        "SELECT phase,outcome_kind,(SELECT count(*) FROM cleanup_task WHERE kind=3) FROM durable_checkpoint_attempt WHERE id=1", [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ).unwrap();
    assert_eq!((phase, outcome, retire), (2, None, 0));
}

#[test]
fn different_target_snapshot_keeps_the_pending_attempt_and_binding() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = b"LFCP\x01request";
    let digest = request_digest(request);
    db.prepare_checkpoint_publication(recorded, request, digest, [18; 32], b"target snapshot")
        .unwrap();
    let before = db.state().unwrap();
    assert!(db
        .install_recorded_checkpoint(
            recorded,
            digest,
            [18; 32],
            b"forged snapshot",
            b"installed",
            CheckpointOutcome::UpToDate { head: None }
        )
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    assert_eq!(db.durable_binding().unwrap().as_deref(), Some(SNAPSHOT));
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (i64, Vec<u8>, i64) = sql.query_row(
        "SELECT phase,target_snapshot,(SELECT count(*) FROM cleanup_task WHERE kind=3) FROM durable_checkpoint_attempt WHERE id=1", [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ).unwrap();
    assert_eq!(row, (2, b"target snapshot".to_vec(), 0));
}

#[test]
fn maintenance_completion_failure_retains_the_exact_original_item() {
    for kind in ["fold", "retire", "operation"] {
        let fixture = Fixture::new();
        fixture.create().close_database().unwrap();
        let sql = rusqlite::Connection::open(fixture.path()).unwrap();
        let column = match kind {
            "fold" => "fold_completed",
            "retire" => "retire_completed",
            _ => "operation_cleanup_completed",
        };
        sql.execute_batch(&format!("CREATE TRIGGER fail_completion BEFORE UPDATE OF {column} ON durable_checkpoint_attempt WHEN NEW.{column}=1 BEGIN SELECT RAISE(ABORT, 'completion failure'); END;")).unwrap();
        drop(sql);
        let db =
            Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
        let recorded = db
            .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
            .unwrap();
        let generation = recorded.capture().generation.number();
        if kind == "operation" {
            let request = u64::try_from(recorded.capture().generation.number()).unwrap();
            let reader = db
                .acquire_recorded_captured_reader(recorded, request)
                .unwrap();
            let operation = db
                .acquire_recorded_captured_operation(recorded, request, reader)
                .unwrap();
            db.release_captured_reader(reader).unwrap();
            db.release_operation(operation).unwrap();
        } else {
            db.fold_unpublished_checkpoint(recorded).unwrap();
        }
        let mut cursor = MaintenanceCursor::default();
        let mut failed = false;
        for _ in 0..256 {
            match db.maintain(cursor) {
                Err(OverlayError::Sql(rusqlite::Error::SqliteFailure(error, Some(message)))) => {
                    assert_eq!(
                        error.extended_code,
                        rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER
                    );
                    assert_eq!(message, "completion failure");
                    failed = true;
                    break;
                }
                Err(error) => panic!("unexpected maintenance failure: {error:?}"),
                Ok(Some(step)) => cursor = step.cursor,
                Ok(None) => panic!("maintenance ended before the injected failure"),
            }
        }
        assert!(failed);
        drop(db);
        let sql = rusqlite::Connection::open(fixture.path()).unwrap();
        let query =
            format!("SELECT {column},operation_owner FROM durable_checkpoint_attempt WHERE id=1");
        let task_kind = match kind {
            "fold" => 1,
            "retire" => 3,
            _ => 6,
        };
        let (completed, owner): (i64, Option<i64>) = sql
            .query_row(&query, [], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap();
        let (resource, target) = if kind == "operation" {
            (owner.expect("original operation owner"), 2)
        } else {
            (0, generation)
        };
        let task: i64 = sql
            .query_row(
                "SELECT count(*) FROM cleanup_task WHERE kind=?1 AND resource=?2 AND target=?3",
                rusqlite::params![task_kind, resource, target],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!((completed, task), (0, 1), "{kind}");
    }
}

#[test]
fn operation_cleanup_cannot_complete_before_owner_release() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER forge_early_cleanup AFTER UPDATE OF operation_owner ON durable_checkpoint_attempt WHEN NEW.operation_owner IS NOT NULL BEGIN UPDATE durable_checkpoint_attempt SET operation_cleanup_completed=1 WHERE id=1; END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(recorded.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(recorded, request)
        .unwrap();
    assert!(matches!(
        db.acquire_recorded_captured_operation(recorded, request, reader),
        Err(OverlayError::Sql(rusqlite::Error::SqliteFailure(error, _)))
            if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_CHECK
    ));
    assert!(db.retained_operation(request).unwrap().is_none());
    assert_eq!(db.retained_captured_reader(request).unwrap(), Some(reader));
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let (owner, count): (Option<i64>, i64) = sql.query_row(
        "SELECT operation_owner,(SELECT operation_count FROM durable_owner_counts WHERE id=1) FROM durable_checkpoint_attempt WHERE id=1",
        [], |r| Ok((r.get(0)?, r.get(1)?)),
    ).unwrap();
    assert_eq!((owner, count), (None, 0));
}
