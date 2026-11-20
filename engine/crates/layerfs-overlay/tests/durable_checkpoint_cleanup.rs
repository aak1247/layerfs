//! Exact local cleanup after a definitely unpublished durable checkpoint.
#![cfg(target_os = "linux")]

use layerfs_overlay::{
    Changes, CheckpointOutcome, Inode, InodeKind, MaintenanceCursor, Overlay, OverlayError,
    PayloadWrite, ProfileConfig,
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
const SNAPSHOT: &[u8] = b"authenticated original branch snapshot";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "layerfs-checkpoint-cleanup-{}-{}",
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
        write(&db, 60, b"before", 123_456_789);
        db
    }
    fn reopen(&self) -> Overlay {
        Overlay::recover_durable(&self.path(), ProfileConfig::default(), WORKSPACE).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_CHECKPOINT_CLEANUP_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn write(db: &Overlay, serial: u64, bytes: &[u8], nanos: u32) {
    let source = db.acquire_base_source(serial).unwrap();
    let publication = db
        .apply(
            source,
            &Changes {
                inodes: vec![Inode {
                    serial,
                    kind: InodeKind::File,
                    mode: 0o644,
                    mtime_seconds: 17,
                    mtime_nanoseconds: nanos,
                    nlink: 1,
                    size: bytes.len() as u64,
                    inherited_cutoff: 0,
                    born: 0,
                    entries: 0,
                    subdirs: 0,
                }],
                write: Some(PayloadWrite {
                    serial,
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

fn drain(db: &Overlay) {
    let mut cursor = MaintenanceCursor::default();
    for _ in 0..256 {
        if db.maintenance_idle().unwrap() {
            return;
        }
        cursor = db
            .maintain(cursor)
            .unwrap()
            .expect("ready maintenance stalled")
            .cursor;
    }
    panic!("bounded maintenance did not finish");
}

fn read(db: &Overlay, serial: u64, expected: &[u8], nanos: u32) {
    let source = db.acquire_base_source(serial + 1000).unwrap();
    let inode = db.source_inode(source, serial).unwrap().unwrap();
    assert_eq!(inode.mtime_nanoseconds, nanos);
    assert_eq!(
        db.source_read(source, serial, 0, expected.len() as u32)
            .unwrap()
            .unwrap()
            .data,
        expected
    );
    db.release_base_source(source).unwrap();
}

#[test]
fn folded_attempt_finishes_once_without_draining_unrelated_work() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let original = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(original.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(original, request)
        .unwrap();
    let operation = db
        .acquire_recorded_captured_operation(original, request, reader)
        .unwrap();
    write(&db, 61, b"after", 987_654_321);
    db.fold_unpublished_checkpoint(original).unwrap();
    assert!(!db.folded_checkpoint_cleanup_ready(original).unwrap());
    assert!(matches!(
        db.finish_folded_checkpoint_cleanup(original),
        Err(OverlayError::Invalid("checkpoint fold cleanup pending"))
    ));
    assert_eq!(db.retained_captured_reader(request).unwrap(), Some(reader));
    assert_eq!(db.retained_operation(request).unwrap(), Some(operation));
    db.release_captured_reader(reader).unwrap();
    db.release_operation(operation).unwrap();
    assert!(!db.folded_checkpoint_cleanup_ready(original).unwrap());
    drain(&db);
    let unrelated = db.acquire_operation(900).unwrap();
    db.release_operation(unrelated).unwrap();
    assert!(!db.maintenance_idle().unwrap());
    assert!(db.folded_checkpoint_cleanup_ready(original).unwrap());
    db.finish_folded_checkpoint_cleanup(original).unwrap();
    assert!(!db.maintenance_idle().unwrap());
    assert!(db.finish_folded_checkpoint_cleanup(original).is_err());
    drain(&db);
    read(&db, 60, b"before", 123_456_789);
    read(&db, 61, b"after", 987_654_321);
    let next = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    assert_ne!(original.attempt(), next.attempt());
    assert!(db.finish_folded_checkpoint_cleanup(original).is_err());
    assert_eq!(db.retained_capture().unwrap(), Some(next.capture()));
    db.fold_unpublished_checkpoint(next).unwrap();
    drain(&db);
    db.finish_folded_checkpoint_cleanup(next).unwrap();
    db.close_database().unwrap();

    let db = fixture.reopen();
    read(&db, 60, b"before", 123_456_789);
    read(&db, 61, b"after", 987_654_321);
    assert!(db.finish_folded_checkpoint_cleanup(original).is_err());
}

#[test]
fn operation_cleanup_completed_before_fold_remains_part_of_that_attempt() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let original = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(original.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(original, request)
        .unwrap();
    let operation = db
        .acquire_recorded_captured_operation(original, request, reader)
        .unwrap();
    db.release_captured_reader(reader).unwrap();
    db.release_operation(operation).unwrap();
    drain(&db);
    db.fold_unpublished_checkpoint(original).unwrap();
    assert!(!db.folded_checkpoint_cleanup_ready(original).unwrap());
    drain(&db);
    assert!(db.folded_checkpoint_cleanup_ready(original).unwrap());
    db.finish_folded_checkpoint_cleanup(original).unwrap();
    db.close_database().unwrap();
    let db = fixture.reopen();
    read(&db, 60, b"before", 123_456_789);
}

#[test]
fn failed_attempt_deletion_retains_all_original_completion_facts() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_checkpoint_finish BEFORE DELETE ON durable_checkpoint_attempt BEGIN SELECT RAISE(ABORT, 'finish failure'); END;").unwrap();
    drop(sql);
    let db = fixture.reopen();
    let original = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(original.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(original, request)
        .unwrap();
    let operation = db
        .acquire_recorded_captured_operation(original, request, reader)
        .unwrap();
    db.fold_unpublished_checkpoint(original).unwrap();
    db.release_captured_reader(reader).unwrap();
    db.release_operation(operation).unwrap();
    drain(&db);
    assert!(db.folded_checkpoint_cleanup_ready(original).unwrap());
    match db.finish_folded_checkpoint_cleanup(original) {
        Err(OverlayError::Sql(rusqlite::Error::SqliteFailure(error, Some(message)))) => {
            assert_eq!(
                error.extended_code,
                rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER
            );
            assert_eq!(message, "finish failure");
        }
        other => panic!("unexpected cleanup result: {other:?}"),
    }
    assert!(db.folded_checkpoint_cleanup_ready(original).unwrap());
    assert!(db.capture().is_err());
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (Vec<u8>, i64, i64, i64, i64, i64, i64) = sql.query_row(
        "SELECT attempt,phase,reader_released,operation_released,fold_completed,retire_completed,operation_cleanup_completed FROM durable_checkpoint_attempt WHERE id=1", [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
    ).unwrap();
    assert_eq!(row, (original.attempt().to_vec(), 3, 1, 1, 1, 1, 1));
}

#[test]
fn installed_attempt_cannot_use_unpublished_cleanup() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let original = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = b"LFCP\x01request";
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs/checkpoint-request/v1\0");
    hash.update(request);
    let digest = *hash.finalize().as_bytes();
    db.prepare_checkpoint_publication(original, request, digest, [18; 32], SNAPSHOT)
        .unwrap();
    db.install_recorded_checkpoint(
        original,
        digest,
        [18; 32],
        SNAPSHOT,
        b"installed",
        CheckpointOutcome::UpToDate { head: None },
    )
    .unwrap();
    assert!(db.folded_checkpoint_cleanup_ready(original).is_err());
    assert!(db.finish_folded_checkpoint_cleanup(original).is_err());
    assert_eq!(
        db.durable_binding().unwrap().as_deref(),
        Some(b"installed".as_slice())
    );
}

#[test]
fn missing_operation_link_cannot_hide_the_original_live_owner() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    // Model a database whose owner-immutability guard is missing as well as a
    // lost link; the independent token and durable owner must still block it.
    sql.execute_batch("DROP TRIGGER durable_checkpoint_attempt_owner_immutable; CREATE TRIGGER erase_owner AFTER UPDATE OF phase ON durable_checkpoint_attempt WHEN NEW.phase=3 BEGIN UPDATE durable_checkpoint_attempt SET operation_owner=NULL,operation_released=NULL,operation_cleanup_completed=NULL WHERE id=1; END;").unwrap();
    drop(sql);
    let db = fixture.reopen();
    let original = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(original.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(original, request)
        .unwrap();
    let operation = db
        .acquire_recorded_captured_operation(original, request, reader)
        .unwrap();
    db.release_captured_reader(reader).unwrap();
    db.fold_unpublished_checkpoint(original).unwrap();
    drain(&db);
    assert!(matches!(
        db.folded_checkpoint_cleanup_ready(original),
        Err(OverlayError::Invalid(
            "checkpoint operation cleanup identity"
        ))
    ));
    assert!(db.finish_folded_checkpoint_cleanup(original).is_err());
    assert_eq!(db.retained_operation(request).unwrap(), Some(operation));
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (i64, Option<i64>, i64) = sql.query_row(
        "SELECT phase,operation_owner,(SELECT count(*) FROM durable_operation_owner) FROM durable_checkpoint_attempt WHERE id=1", [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ).unwrap();
    assert_eq!(row, (3, None, 1));
}

#[test]
fn released_operation_identity_cannot_disappear_during_cleanup() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER erase_released_owner AFTER UPDATE OF operation_cleanup_completed ON durable_checkpoint_attempt WHEN NEW.operation_cleanup_completed=1 BEGIN UPDATE durable_checkpoint_attempt SET operation_owner=NULL,operation_released=NULL,operation_cleanup_completed=NULL WHERE id=1; END;").unwrap();
    drop(sql);
    let db = fixture.reopen();
    let original = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(original.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(original, request)
        .unwrap();
    let operation = db
        .acquire_recorded_captured_operation(original, request, reader)
        .unwrap();
    db.release_captured_reader(reader).unwrap();
    db.release_operation(operation).unwrap();
    let mut cursor = MaintenanceCursor::default();
    let mut failed = false;
    for _ in 0..256 {
        match db.maintain(cursor) {
            Err(OverlayError::Sql(rusqlite::Error::SqliteFailure(error, Some(message)))) => {
                assert_eq!(
                    error.extended_code,
                    rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER
                );
                assert_eq!(message, "checkpoint owner identity is immutable");
                failed = true;
                break;
            }
            Err(error) => panic!("unexpected maintenance failure: {error:?}"),
            Ok(Some(step)) => cursor = step.cursor,
            Ok(None) => panic!("maintenance ended before identity mutation"),
        }
    }
    assert!(failed);
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (i64, i64, i64, i64) = sql.query_row(
        "SELECT operation_owner,operation_released,operation_cleanup_completed,(SELECT count(*) FROM cleanup_task WHERE kind=6 AND resource=durable_checkpoint_attempt.operation_owner AND target=2) FROM durable_checkpoint_attempt WHERE id=1", [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    ).unwrap();
    assert!(row.0 > 0);
    assert_eq!((row.1, row.2, row.3), (1, 0, 1));
}

#[test]
fn unrelated_generic_operation_with_same_request_does_not_block_finish() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let original = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(original.capture().generation.number()).unwrap();
    db.fold_unpublished_checkpoint(original).unwrap();
    drain(&db);
    let generic = db.acquire_operation(request).unwrap();
    assert!(db.folded_checkpoint_cleanup_ready(original).unwrap());
    db.finish_folded_checkpoint_cleanup(original).unwrap();
    assert_eq!(db.retained_operation(request).unwrap(), Some(generic));
    db.release_operation(generic).unwrap();
    drain(&db);
}
