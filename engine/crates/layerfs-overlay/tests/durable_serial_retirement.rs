//! New durable retirement provenance records facts without admitting recovery.
#![cfg(target_os = "linux")]

mod payload_support;

use layerfs_overlay::{
    Changes, MaintenanceCursor, Overlay, OverlayCatalogRecovery, OverlayError, ProfileConfig,
};
use payload_support::File;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const WORKSPACE: [u8; 32] = [43; 32];
const ROOT: [u8; 32] = [17; 32];
const SNAPSHOT: &[u8] = b"original snapshot";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "layerfs-retirement-origin-{}-{}",
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
        assert_eq!(db.profile().schema_version, 45);
        db.open_workspace(ROOT).unwrap();
        db.initialize_durable_binding(ROOT, SNAPSHOT).unwrap();
        db.initialize_durable_namespace(7).unwrap();
        db
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_SERIAL_RETIREMENT_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn number(bytes: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn original_origin(sql: &rusqlite::Connection, kind: u8) -> Vec<u8> {
    let bytes: Vec<u8> = sql
        .query_row(
            "SELECT name FROM cleanup_task WHERE kind=7 AND resource=9 AND target=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bytes.len(), 134);
    assert_eq!(&bytes[..6], &[b'L', b'F', b'S', b'R', 1, kind]);
    assert_eq!(&bytes[6..38], WORKSPACE);
    assert_eq!(&bytes[38..70], ROOT);
    assert_eq!(
        (number(&bytes, 70), number(&bytes, 78), number(&bytes, 86)),
        (7, 9, 1)
    );
    bytes
}

#[test]
fn last_unlink_records_immutable_original_origin_and_old_open_stays_fenced() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let source = db.acquire_base_source(1).unwrap();
    let mut file = File::new(&db, source, 9, Vec::new());
    file.write(0, &[7; 131072]);
    let revision = db.state().unwrap().revision;
    let mut removed = file.inode(file.expect.len() as u64);
    removed.nlink = 0;
    let publication = db
        .apply(
            source,
            &Changes {
                inodes: vec![removed],
                ..Changes::default()
            },
        )
        .unwrap();
    db.reply_attempted(publication).unwrap();
    drop(file);
    db.release_base_source(source).unwrap();
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let bytes = original_origin(&sql, 1);
    assert_eq!(
        (
            number(&bytes, 94),
            number(&bytes, 102),
            number(&bytes, 110),
            number(&bytes, 118),
            number(&bytes, 126)
        ),
        (0, 1, 1, 1, revision)
    );
    let error = sql
        .execute(
            "UPDATE cleanup_task SET name=zeroblob(134) WHERE kind=7",
            [],
        )
        .unwrap_err();
    assert!(
        matches!(error, rusqlite::Error::SqliteFailure(code, Some(ref message)) if code.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER && message == "serial retirement origin is immutable")
    );
    assert_eq!(original_origin(&sql, 1), bytes);
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
fn released_layer_origin_survives_original_orphan_removal() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let source = db.acquire_base_source(1).unwrap();
    let mut file = File::new(&db, source, 9, Vec::new());
    file.write(0, &[8; 131072]);
    file.write(131072, &[8; 131072]);
    let handle = db
        .open_file(source, 2, &file.inode(file.expect.len() as u64), true)
        .unwrap();
    let mut removed = file.inode(file.expect.len() as u64);
    removed.nlink = 0;
    let publication = db
        .apply(
            source,
            &Changes {
                inodes: vec![removed],
                ..Changes::default()
            },
        )
        .unwrap();
    db.reply_attempted(publication).unwrap();
    assert!(db.check_file(handle, false).is_ok());
    db.close_file(handle).unwrap();
    // Repeatedly take the earliest ready item, allowing the orphan job to
    // finish while its independently queued serial retirement remains.
    db.maintain(MaintenanceCursor::default()).unwrap().unwrap();
    drop(file);
    db.release_base_source(source).unwrap();
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let bytes = original_origin(&sql, 2);
    assert_eq!(
        (
            number(&bytes, 94),
            number(&bytes, 102),
            number(&bytes, 110),
            number(&bytes, 118)
        ),
        (0, 1, 0, 1)
    );
    let orphan: i64 = sql
        .query_row(
            "SELECT count(*) FROM durable_orphan_owner WHERE serial=9",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(orphan, 0);
}

#[test]
fn background_migration_records_the_layer_before_advancing_its_owner() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let source = db.acquire_base_source(1).unwrap();
    let mut file = File::new(&db, source, 9, Vec::new());
    file.write(0, b"x");
    let handle = db.open_file(source, 2, &file.inode(1), true).unwrap();
    let mut removed = file.inode(1);
    removed.nlink = 0;
    let publication = db
        .apply(
            source,
            &Changes {
                inodes: vec![removed],
                ..Changes::default()
            },
        )
        .unwrap();
    db.reply_attempted(publication).unwrap();
    for _ in 0..2 {
        db.maintain(MaintenanceCursor::default()).unwrap().unwrap();
    }
    assert!(db.check_file(handle, false).is_ok());
    drop(file);
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let bytes = original_origin(&sql, 2);
    assert_eq!(
        (number(&bytes, 94), number(&bytes, 102), number(&bytes, 110)),
        (0, 1, 0)
    );
    let top: i64 = sql
        .query_row(
            "SELECT top FROM durable_orphan_owner WHERE serial=9",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(top, 0);
}

#[test]
fn origin_without_namespace_is_not_promoted_to_a_recovery_identity() {
    let fixture = Fixture::new();
    let db = Overlay::create_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    db.open_workspace(ROOT).unwrap();
    let source = db.acquire_base_source(1).unwrap();
    let mut file = File::new(&db, source, 9, Vec::new());
    file.write(0, b"x");
    let mut removed = file.inode(1);
    removed.nlink = 0;
    let publication = db
        .apply(
            source,
            &Changes {
                inodes: vec![removed],
                ..Changes::default()
            },
        )
        .unwrap();
    db.reply_attempted(publication).unwrap();
    drop(file);
    db.release_base_source(source).unwrap();
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let bytes: Vec<u8> = sql
        .query_row("SELECT name FROM cleanup_task WHERE kind=7", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!((bytes.len(), number(&bytes, 70)), (134, 0));
    drop(sql);
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE),
        Err(OverlayError::Invalid(
            "durable overlay requires reconciliation"
        ))
    ));
}

#[test]
fn failed_origin_insert_rolls_back_the_unlink() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_retirement BEFORE INSERT ON cleanup_task WHEN NEW.kind=7 BEGIN SELECT RAISE(ABORT,'injected origin failure'); END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let source = db.acquire_base_source(1).unwrap();
    let mut file = File::new(&db, source, 9, Vec::new());
    file.write(0, b"original contents");
    let mut removed = file.inode(file.expect.len() as u64);
    removed.nlink = 0;
    let error = db
        .apply(
            source,
            &Changes {
                inodes: vec![removed],
                ..Changes::default()
            },
        )
        .unwrap_err();
    assert!(
        matches!(error, OverlayError::Sql(rusqlite::Error::SqliteFailure(code, Some(ref message))) if code.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER && message == "injected origin failure")
    );
    assert_eq!(db.inode(9).unwrap().unwrap().nlink, 1);
    assert_eq!(file.read(0, 17), b"original contents");
    drop(file);
    db.release_base_source(source).unwrap();
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    assert_eq!(
        sql.query_row(
            "SELECT count(*) FROM cleanup_task WHERE kind=7",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}

#[test]
fn different_origin_for_the_same_serial_layer_rolls_back_the_unlink() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch(
        "CREATE TRIGGER conflicting_retirement BEFORE INSERT ON cleanup_task \
         WHEN NEW.kind=7 AND NEW.name<>zeroblob(134) BEGIN \
         INSERT INTO cleanup_task(kind,resource,target,name) \
         VALUES(7,NEW.resource,NEW.target,zeroblob(134)); END;",
    )
    .unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let source = db.acquire_base_source(1).unwrap();
    let mut file = File::new(&db, source, 9, Vec::new());
    file.write(0, b"original contents");
    let mut removed = file.inode(file.expect.len() as u64);
    removed.nlink = 0;
    let error = db
        .apply(
            source,
            &Changes {
                inodes: vec![removed],
                ..Changes::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        OverlayError::Invalid("serial retirement origin changed")
    ));
    assert_eq!(db.inode(9).unwrap().unwrap().nlink, 1);
    assert_eq!(file.read(0, 17), b"original contents");
    drop(file);
    db.release_base_source(source).unwrap();
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    assert_eq!(
        sql.query_row(
            "SELECT count(*) FROM cleanup_task WHERE kind=7",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}

#[test]
fn altered_retirement_trigger_refuses_reopen_without_changing_the_database() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch(
        "DROP TRIGGER durable_serial_retirement_identity; \
         CREATE TRIGGER durable_serial_retirement_identity BEFORE UPDATE ON cleanup_task \
         BEGIN SELECT 1; END;",
    )
    .unwrap();
    drop(sql);
    let before = fs::read(fixture.path()).unwrap();
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE),
        Err(OverlayError::Invalid(
            "durable serial retirement schema identity"
        ))
    ));
    assert_eq!(fs::read(fixture.path()).unwrap(), before);
}

#[test]
fn wake_only_interruption_can_finish_without_minting_an_owner() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute(
        "INSERT INTO cleanup_task(kind,resource,target) VALUES(8,0,1)",
        [],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO cleanup_task(kind,resource,target) VALUES(9,0,1)",
        [],
    )
    .unwrap();
    drop(sql);
    let recovery =
        OverlayCatalogRecovery::open(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let OverlayCatalogRecovery::PendingOrphans(mut pending) = recovery else {
        panic!("wake-only work must remain under the original pending lease")
    };
    let record = pending.observed().clone();
    assert_eq!(record.previous_namespace, 7);
    assert!(pending.owner_roots(0).unwrap().is_empty());
    assert!(pending.retirement_roots((0, 0)).unwrap().is_empty());
    pending.schedule(1).unwrap();
    let mut cursor = MaintenanceCursor::default();
    for _ in 0..8 {
        if pending.ready().unwrap() {
            break;
        }
        cursor = pending
            .maintain(cursor)
            .unwrap()
            .expect("wake work stalled")
            .cursor;
    }
    assert!(pending.ready().unwrap());
    let db = pending.complete(&record).unwrap();
    assert_eq!(db.durable_namespace().unwrap(), Some(7));
    db.advance_durable_namespace(8).unwrap();
    assert_eq!(db.durable_namespace().unwrap(), Some(8));
    db.close_database().unwrap();
}

#[test]
fn inconsistent_orphan_domain_or_floor_is_refused_before_any_write() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let source = db.acquire_base_source(1).unwrap();
    let mut file = File::new(&db, source, 9, Vec::new());
    file.write(0, b"held bytes");
    let _handle = db
        .open_file(source, 2, &file.inode(file.expect.len() as u64), true)
        .unwrap();
    let mut removed = file.inode(file.expect.len() as u64);
    removed.nlink = 0;
    let publication = db
        .apply(
            source,
            &Changes {
                inodes: vec![removed],
                ..Changes::default()
            },
        )
        .unwrap();
    db.reply_attempted(publication).unwrap();
    drop(file);
    db.release_base_source(source).unwrap();
    drop(db);

    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute(
        "INSERT INTO fs_data(inode,generation,cell_offset,epoch,data) VALUES(10,-1,0,0,X'01')",
        [],
    )
    .unwrap();
    drop(sql);
    let before = fs::read(fixture.path()).unwrap();
    assert!(matches!(
        OverlayCatalogRecovery::open(&fixture.path(), ProfileConfig::default(), WORKSPACE),
        Err(OverlayError::Invalid("orphan recovery unowned row"))
    ));
    assert_eq!(fs::read(fixture.path()).unwrap(), before);

    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute("DELETE FROM fs_data WHERE inode=10 AND generation=-1", [])
        .unwrap();
    assert_eq!(
        sql.execute("UPDATE durable_orphan_owner SET floor=1 WHERE serial=9", [])
            .unwrap(),
        1
    );
    drop(sql);
    let before = fs::read(fixture.path()).unwrap();
    assert!(matches!(
        OverlayCatalogRecovery::open(&fixture.path(), ProfileConfig::default(), WORKSPACE),
        Err(OverlayError::Invalid("orphan recovery owner"))
    ));
    assert_eq!(fs::read(fixture.path()).unwrap(), before);
}

#[test]
fn reconstructed_schema44_keeps_legacy_behavior_without_silent_upgrade() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("DROP TRIGGER durable_serial_retirement_insert; DROP TRIGGER durable_serial_retirement_identity; PRAGMA user_version=44;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    assert_eq!(db.profile().schema_version, 44);
    let source = db.acquire_base_source(1).unwrap();
    let mut file = File::new(&db, source, 9, Vec::new());
    file.write(0, b"old format");
    let mut removed = file.inode(file.expect.len() as u64);
    removed.nlink = 0;
    let publication = db
        .apply(
            source,
            &Changes {
                inodes: vec![removed],
                ..Changes::default()
            },
        )
        .unwrap();
    db.reply_attempted(publication).unwrap();
    drop(file);
    db.release_base_source(source).unwrap();
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let (version, name): (i64, Vec<u8>) = sql.query_row("SELECT (SELECT user_version FROM pragma_user_version),name FROM cleanup_task WHERE kind=7", [], |row| Ok((row.get(0)?,row.get(1)?))).unwrap();
    assert_eq!((version, name), (44, Vec::new()));
    drop(sql);
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE),
        Err(OverlayError::Invalid(
            "durable overlay requires reconciliation"
        ))
    ));
}
