//! New durable conflict backing preserves old schema and does not grant recovery.
#![cfg(target_os = "linux")]

#[path = "support/durable_schema43.rs"]
mod durable_schema43;

use layerfs_overlay::{MaintenanceCursor, Overlay, OverlayError, ProfileConfig};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const WORKSPACE: [u8; 32] = [43; 32];
const ROOT: [u8; 32] = [17; 32];
const SNAPSHOT: &[u8] = b"original historical snapshot";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "layerfs-conflict-format-{}-{}",
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
        db.initialize_durable_namespace(1).unwrap();
        db
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_CONFLICT_FORMAT_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
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
            .expect("maintenance stalled")
            .cursor;
    }
    panic!("bounded maintenance did not finish");
}

#[test]
fn current_schema_records_clean_checkpoint_and_reopens_without_conflict() {
    let fixture = Fixture::new();
    let db = fixture.create();
    assert_eq!(db.profile().schema_version, 45);
    let capture = db
        .capture_recorded(WORKSPACE, [0; 32], 1, SNAPSHOT)
        .unwrap();
    db.fold_unpublished_checkpoint(capture).unwrap();
    drain(&db);
    db.finish_folded_checkpoint_cleanup(capture).unwrap();
    db.require_recorded_recovery_ready().unwrap();
    db.close_database().unwrap();
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    assert_eq!(db.profile().schema_version, 45);
    assert_eq!(db.durable_binding().unwrap().as_deref(), Some(SNAPSHOT));
    db.close_database().unwrap();
}

#[test]
fn unqualified_conflict_records_block_recovery_and_leave_original_bytes() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute(
        "INSERT INTO durable_conflict_plan(generation,attempt,workspace,catalog,incarnation,namespace,local_root,target_snapshot,paths_count,byte_count,page_count,final_digest,descriptor_digest,next_page,written_bytes,chain,state) VALUES(1,?1,?2,?3,1,1,?4,?5,1,1,1,?3,?3,0,0,?3,0)",
        rusqlite::params![&[1_u8;32], &WORKSPACE, &[0_u8;32], &ROOT, SNAPSHOT],
    ).unwrap();
    drop(sql);
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
fn conflict_cleanup_key_is_exact_and_unqualified_work_blocks_recovery() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    for (resource, target) in [(1, 1), (0, -1)] {
        let error = sql
            .execute(
                "INSERT INTO cleanup_task(kind,resource,target) VALUES(13,?1,?2)",
                [resource, target],
            )
            .unwrap_err();
        assert!(
            matches!(error, rusqlite::Error::SqliteFailure(error, _) if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_CHECK)
        );
    }
    sql.execute(
        "INSERT INTO cleanup_task(kind,resource,target) VALUES(13,0,1)",
        [],
    )
    .unwrap();
    drop(sql);
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
fn altered_required_trigger_blocks_open_without_changing_original_database() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch(
        "DROP TRIGGER durable_conflict_page_update; \
         CREATE TRIGGER durable_conflict_page_update BEFORE UPDATE ON durable_conflict_page BEGIN SELECT 1; END;",
    ).unwrap();
    drop(sql);
    let before = fs::read(fixture.path()).unwrap();
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE),
        Err(OverlayError::Invalid("durable conflict schema identity"))
    ));
    assert_eq!(fs::read(fixture.path()).unwrap(), before);
}

#[test]
fn schema44_reader_floor_cannot_change_after_acquisition() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch(
        "CREATE TRIGGER alter_floor AFTER UPDATE OF phase ON durable_checkpoint_attempt \
         WHEN NEW.phase=3 BEGIN UPDATE durable_checkpoint_attempt SET reader_floor=0 WHERE id=1; END;",
    ).unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let earlier = db.capture().unwrap();
    db.install_recorded(earlier, ROOT, SNAPSHOT).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, [0; 32], 1, SNAPSHOT)
        .unwrap();
    let request = u64::try_from(recorded.capture().generation.number()).unwrap();
    let reader = db
        .acquire_recorded_captured_reader(recorded, request)
        .unwrap();
    assert_eq!(reader.installed_floor(), 1);
    let before = db.state().unwrap();
    let error = db.fold_unpublished_checkpoint(recorded).unwrap_err();
    assert!(
        matches!(error, OverlayError::Sql(rusqlite::Error::SqliteFailure(error, Some(message)))
        if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER
        && message == "checkpoint owner identity is immutable")
    );
    assert_eq!(db.state().unwrap(), before);
    assert_eq!(db.retained_captured_reader(request).unwrap(), Some(reader));
}

#[test]
fn conflict_plan_pages_and_binding_have_one_monotonic_predecessor() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let insert = "INSERT INTO durable_conflict_plan(generation,attempt,workspace,catalog,incarnation,namespace,local_root,target_snapshot,paths_count,byte_count,page_count,final_digest,descriptor_digest,next_page,written_bytes,chain,state) VALUES(1,?1,?2,?1,1,1,?3,?4,1,1,1,?1,?1,0,0,?1,?5)";
    let zeros = [0_u8; 32];
    let error = sql
        .execute(
            insert,
            rusqlite::params![&zeros, &WORKSPACE, &ROOT, SNAPSHOT, 1],
        )
        .unwrap_err();
    assert!(
        matches!(error, rusqlite::Error::SqliteFailure(error, Some(message))
        if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER
        && message == "invalid conflict plan predecessor")
    );
    sql.execute(
        insert,
        rusqlite::params![&zeros, &WORKSPACE, &ROOT, SNAPSHOT, 0],
    )
    .unwrap();
    let error = sql
        .execute(
            "UPDATE durable_conflict_plan SET next_page=1,written_bytes=1,chain=zeroblob(32)",
            [],
        )
        .unwrap_err();
    assert!(
        matches!(error, rusqlite::Error::SqliteFailure(error, Some(message))
        if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER
        && message == "invalid conflict page progress")
    );
    sql.execute(
        "INSERT INTO durable_conflict_page(generation,page_no,data) VALUES(1,0,X'01')",
        [],
    )
    .unwrap();
    sql.execute(
        "UPDATE durable_conflict_plan SET next_page=1,written_bytes=1,chain=zeroblob(32)",
        [],
    )
    .unwrap();
    sql.execute("UPDATE durable_conflict_plan SET state=1", [])
        .unwrap();
    assert!(sql
        .execute("UPDATE durable_conflict_page SET data=X'02'", [])
        .is_err());
    assert!(sql
        .execute("DELETE FROM durable_conflict_page", [])
        .is_err());
    sql.execute(
        "UPDATE durable_binding SET conflict_generation=1,conflict_digest=zeroblob(32)",
        [],
    )
    .unwrap();
    assert!(sql
        .execute("UPDATE durable_binding SET root=zeroblob(32)", [])
        .is_err());
    assert!(sql
        .execute("UPDATE durable_conflict_plan SET state=2", [])
        .is_err());
    sql.execute(
        "UPDATE durable_binding SET conflict_generation=NULL,conflict_digest=NULL",
        [],
    )
    .unwrap();
    sql.execute("UPDATE durable_conflict_plan SET state=2", [])
        .unwrap();
    sql.execute("DELETE FROM durable_conflict_page", [])
        .unwrap();
    sql.execute("DELETE FROM durable_conflict_plan", [])
        .unwrap();
}

#[test]
fn prepared_request_cannot_be_erased_or_changed_by_a_phase_transition() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, [0; 32], 1, SNAPSHOT)
        .unwrap();
    let request = b"LFCP\x01original";
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs/checkpoint-request/v1\0");
    hash.update(request);
    let digest = *hash.finalize().as_bytes();
    db.prepare_checkpoint_publication(recorded, request, digest, ROOT, SNAPSHOT)
        .unwrap();
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let error = sql.execute(
        "UPDATE durable_checkpoint_attempt SET phase=6,fold_completed=0,retire_completed=0,request=X'00'", [],
    ).unwrap_err();
    assert!(
        matches!(error, rusqlite::Error::SqliteFailure(error, Some(message))
        if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER
        && message == "checkpoint request is immutable")
    );
    assert!(sql
        .execute(
            "UPDATE durable_checkpoint_attempt SET phase=3,fold_completed=0,retire_completed=0",
            [],
        )
        .is_err());
    let row: (i64, Vec<u8>, Vec<u8>) = sql
        .query_row(
            "SELECT phase,request,request_digest FROM durable_checkpoint_attempt",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(row, (2, request.to_vec(), digest.to_vec()));
}

#[test]
fn original_schema43_reopens_without_conversion_and_keeps_recorded_capture() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    durable_schema43::convert(&fixture.path());
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    assert_eq!(db.profile().schema_version, 43);
    assert_eq!(db.durable_binding().unwrap().as_deref(), Some(SNAPSHOT));
    let capture = db
        .capture_recorded(WORKSPACE, [0; 32], 1, SNAPSHOT)
        .unwrap();
    db.fold_unpublished_checkpoint(capture).unwrap();
    drain(&db);
    db.finish_folded_checkpoint_cleanup(capture).unwrap();
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let version: i64 = sql
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 43);
    let count: i64 = sql.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE name IN ('durable_conflict_plan','durable_conflict_page')",
        [], |row| row.get(0),
    ).unwrap();
    assert_eq!(count, 0);
}
