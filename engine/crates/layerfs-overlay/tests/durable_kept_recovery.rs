//! Durable carried rows remain distinct from unsafe process-local custody.
#![cfg(target_os = "linux")]

use layerfs_overlay::{
    Changes, Inode, InodeKind, MaintenanceCursor, Overlay, OverlayError, ProfileConfig,
};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const IDENTITY: [u8; 32] = [43; 32];
const ROOT: [u8; 32] = [17; 32];
const SERIAL: u64 = 60;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "layerfs-durable-kept-{}-{}",
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
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_DURABLE_OVERLAY_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn inode(serial: u64) -> Inode {
    Inode {
        serial,
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
    }
}

fn apply(db: &Overlay, request: u64, changes: &Changes) -> layerfs_overlay::OverlayResult<()> {
    let source = db.acquire_base_source(request).unwrap();
    let result = db
        .apply(source, changes)
        .map(|publication| db.reply_attempted(publication).unwrap());
    db.release_base_source(source).unwrap();
    result
}

fn settle_maintenance(db: &Overlay) {
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
    panic!("bounded fixture maintenance did not finish");
}

fn with_kept(fixture: &Fixture) -> Overlay {
    let db = Overlay::create_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    assert_eq!(db.profile().schema_version, 45);
    db.open_workspace(ROOT).unwrap();
    db.initialize_durable_binding(ROOT, b"original snapshot")
        .unwrap();
    db.initialize_durable_namespace(1).unwrap();
    apply(
        &db,
        1,
        &Changes {
            inodes: vec![inode(SERIAL)],
            ..Changes::default()
        },
    )
    .unwrap();
    let capture = db.capture().unwrap();
    let reader = db.acquire_captured_reader(capture, 1).unwrap();
    assert!(db.keep_live_row(reader, SERIAL).unwrap());
    assert!(!db.keep_live_row(reader, SERIAL).unwrap());
    db.release_captured_reader(reader).unwrap();
    db.install_recorded(capture, ROOT, b"installed snapshot")
        .unwrap();
    settle_maintenance(&db);
    assert_eq!(db.state().unwrap().dirty_inodes, 0);
    db
}

#[test]
fn carried_row_recovers_and_the_first_writer_counts_it_once() {
    let fixture = Fixture::new();
    let db = with_kept(&fixture);
    db.require_recorded_recovery_ready().unwrap();
    drop(db);
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    assert_eq!(db.durable_namespace().unwrap(), Some(1));
    db.advance_durable_namespace(2).unwrap();
    assert_eq!(
        db.inode(SERIAL).unwrap().unwrap().mtime_nanoseconds,
        123_456_789
    );
    assert_eq!(db.state().unwrap().dirty_inodes, 0);
    let mut changed = inode(SERIAL);
    changed.mode = 0o600;
    apply(
        &db,
        2,
        &Changes {
            inodes: vec![changed.clone()],
            ..Changes::default()
        },
    )
    .unwrap();
    assert_eq!(db.state().unwrap().dirty_inodes, 1);
    apply(
        &db,
        3,
        &Changes {
            inodes: vec![changed],
            ..Changes::default()
        },
    )
    .unwrap();
    assert_eq!(db.state().unwrap().dirty_inodes, 1);
    let capture = db.capture().unwrap();
    assert!(db
        .captured_inodes(capture, 0)
        .unwrap()
        .iter()
        .any(|row| row.serial == SERIAL && row.mtime_nanoseconds == 123_456_789));
    let reader = db.acquire_captured_reader(capture, 2).unwrap();
    assert!(db.keep_live_row(reader, SERIAL).unwrap());
    db.release_captured_reader(reader).unwrap();
    db.install_recorded(capture, ROOT, b"second snapshot")
        .unwrap();
    settle_maintenance(&db);
    drop(db);
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    assert_eq!(db.inode(SERIAL).unwrap().unwrap().mode, 0o600);
    assert_eq!(db.state().unwrap().dirty_inodes, 0);
    db.close_database().unwrap();
}

#[test]
fn failed_write_rolls_back_the_carried_row_and_its_durable_record() {
    let fixture = Fixture::new();
    let db = with_kept(&fixture);
    apply(
        &db,
        2,
        &Changes {
            created: Some(61),
            inodes: vec![inode(61)],
            ..Changes::default()
        },
    )
    .unwrap();
    let mut changed = inode(SERIAL);
    changed.mode = 0o600;
    let failing = Changes {
        created: Some(61),
        inodes: vec![changed.clone(), inode(61)],
        ..Changes::default()
    };
    assert!(apply(&db, 3, &failing).is_err());
    assert_eq!(db.state().unwrap().dirty_inodes, 1);
    drop(db);
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    apply(
        &db,
        4,
        &Changes {
            inodes: vec![changed],
            ..Changes::default()
        },
    )
    .unwrap();
    assert_eq!(db.state().unwrap().dirty_inodes, 2);
    db.close_database().unwrap();
}

#[test]
fn kept_rows_do_not_hide_unresolved_operation_or_legacy_custody() {
    let fixture = Fixture::new();
    let db = with_kept(&fixture);
    let _operation = db.acquire_operation(7).unwrap();
    drop(db);
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY),
        Err(OverlayError::Invalid(
            "durable overlay requires reconciliation"
        ))
    ));
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    let (orphan, operation): (bool, bool) = connection
        .query_row(
            "SELECT orphan_owners,operation_owners FROM durable_custody WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(!orphan && operation);
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM durable_kept_live", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    connection
        .execute_batch(
            "DROP TABLE durable_checkpoint_attempt; DROP TABLE durable_operation_owner; DROP TABLE durable_orphan_owner; DROP TABLE durable_owner_counts; DROP TABLE durable_kept_live; DROP TABLE durable_custody; PRAGMA user_version=41;",
        )
        .unwrap();
    drop(connection);
    let before = fs::read(fixture.path()).unwrap();
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY),
        Err(OverlayError::Invalid(
            "legacy durable volatile custody unresolved"
        ))
    ));
    assert_eq!(fs::read(fixture.path()).unwrap(), before);
}

#[test]
fn missing_or_mismatched_kept_rows_require_reconciliation_without_changes() {
    for corruption in [
        "DELETE FROM durable_kept_live",
        "UPDATE durable_kept_live SET generation=generation+1",
        "DELETE FROM fs_inode WHERE inode=60 AND generation=(SELECT active FROM durable_state WHERE id=1)",
    ] {
        let fixture = Fixture::new();
        drop(with_kept(&fixture));
        let connection = rusqlite::Connection::open(fixture.path()).unwrap();
        assert_eq!(connection.execute(corruption, []).unwrap(), 1);
        drop(connection);
        let before = fs::read(fixture.path()).unwrap();
        assert!(matches!(
            Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY),
            Err(OverlayError::Invalid("durable kept count requires reconciliation"))
                | Err(OverlayError::Invalid("durable kept row requires reconciliation"))
        ));
        assert_eq!(fs::read(fixture.path()).unwrap(), before);
    }
}

#[test]
fn clean_legacy_41_reopens_without_implicit_conversion() {
    let fixture = Fixture::new();
    let db = Overlay::create_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    db.open_workspace(ROOT).unwrap();
    db.initialize_durable_binding(ROOT, b"original snapshot")
        .unwrap();
    db.initialize_durable_namespace(1).unwrap();
    db.close_database().unwrap();
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    connection
        .execute_batch(
            "DROP TABLE durable_checkpoint_attempt; DROP TABLE durable_operation_owner; DROP TABLE durable_orphan_owner; DROP TABLE durable_owner_counts; DROP TABLE durable_kept_live; DROP TABLE durable_custody; PRAGMA user_version=41;",
        )
        .unwrap();
    drop(connection);
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    assert_eq!(db.profile().schema_version, 41);
    assert_eq!(db.durable_namespace().unwrap(), Some(1));
    db.close_database().unwrap();
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        41
    );
}

#[test]
fn legacy_42_reconstructs_kept_rows_without_implicit_conversion() {
    let fixture = Fixture::new();
    drop(with_kept(&fixture));
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    connection.execute_batch("DROP TABLE durable_checkpoint_attempt; DROP TABLE durable_operation_owner; DROP TABLE durable_orphan_owner; DROP TABLE durable_owner_counts; PRAGMA user_version=42;").unwrap();
    drop(connection);
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    assert_eq!(db.profile().schema_version, 42);
    assert_eq!(db.state().unwrap().dirty_inodes, 0);
    let mut changed = inode(SERIAL);
    changed.mode = 0o600;
    apply(
        &db,
        2,
        &Changes {
            inodes: vec![changed],
            ..Changes::default()
        },
    )
    .unwrap();
    assert_eq!(db.state().unwrap().dirty_inodes, 1);
    db.close_database().unwrap();
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        42
    );
}

#[test]
fn a_failed_kept_insert_rolls_back_the_inode_and_custody() {
    let fixture = Fixture::new();
    let db = Overlay::create_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    db.open_workspace(ROOT).unwrap();
    db.initialize_durable_binding(ROOT, b"original snapshot")
        .unwrap();
    db.initialize_durable_namespace(1).unwrap();
    apply(
        &db,
        1,
        &Changes {
            inodes: vec![inode(SERIAL)],
            ..Changes::default()
        },
    )
    .unwrap();
    db.close_database().unwrap();
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_keep BEFORE INSERT ON durable_kept_live BEGIN SELECT RAISE(ABORT, 'injected keep failure'); END;").unwrap();
    drop(connection);
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    let capture = db.capture().unwrap();
    let reader = db.acquire_captured_reader(capture, 1).unwrap();
    assert!(db.keep_live_row(reader, SERIAL).is_err());
    assert!(db.active_inodes(0).unwrap().is_empty());
    db.release_captured_reader(reader).unwrap();
    drop(db);
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    let (rows, count): (i64, i64) = connection.query_row(
        "SELECT (SELECT count(*) FROM durable_kept_live), kept_count FROM durable_custody WHERE id=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap();
    assert_eq!((rows, count), (0, 0));
}

#[test]
fn a_failed_capture_preserves_the_kept_set_and_dirty_accounting() {
    let fixture = Fixture::new();
    drop(with_kept(&fixture));
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_capture BEFORE DELETE ON durable_kept_live BEGIN SELECT RAISE(ABORT, 'injected capture failure'); END;").unwrap();
    drop(connection);
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    let before = db.state().unwrap();
    assert!(db.capture().is_err());
    let after = db.state().unwrap();
    assert_eq!(after.active, before.active);
    assert_eq!(after.captured, before.captured);
    assert_eq!(after.dirty_inodes, 0);
    db.close_database().unwrap();
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    connection
        .execute_batch("DROP TRIGGER fail_capture")
        .unwrap();
    drop(connection);
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    let mut changed = inode(SERIAL);
    changed.mode = 0o600;
    apply(
        &db,
        2,
        &Changes {
            inodes: vec![changed],
            ..Changes::default()
        },
    )
    .unwrap();
    assert_eq!(db.state().unwrap().dirty_inodes, 1);
    db.close_database().unwrap();
}
