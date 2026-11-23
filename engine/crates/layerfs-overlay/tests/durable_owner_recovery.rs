//! Durable owner records preserve custody without guessing an interrupted outcome.
#![cfg(target_os = "linux")]

#[path = "support/durable_schema45.rs"]
mod durable_schema45;

mod payload_support;

use layerfs_overlay::{Changes, Overlay, OverlayError, ProfileConfig};
use payload_support::File;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const IDENTITY: [u8; 32] = [43; 32];
const ROOT: [u8; 32] = [17; 32];

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "layerfs-durable-owner-{}-{}",
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
    fn create(&self, namespace: bool) -> Overlay {
        let db = Overlay::create_durable(&self.path(), ProfileConfig::default(), IDENTITY).unwrap();
        assert_eq!(db.profile().schema_version, 46);
        db.open_workspace(ROOT).unwrap();
        db.initialize_durable_binding(ROOT, b"original snapshot")
            .unwrap();
        if namespace {
            db.initialize_durable_namespace(1).unwrap();
        }
        db
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_DURABLE_OWNER_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

#[test]
fn captured_owner_records_the_exact_reader_but_does_not_authorize_recovery() {
    let fixture = Fixture::new();
    let db = fixture.create(true);
    let capture = db.capture().unwrap();
    let reader = db.acquire_captured_reader(capture, 9).unwrap();
    let request = u64::try_from(capture.generation.number()).unwrap();
    assert!(db.acquire_captured_operation(request + 1, reader).is_err());
    assert!(db.retained_operation(request + 1).unwrap().is_none());
    let _owner = db.acquire_captured_operation(request, reader).unwrap();
    drop(db);

    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (i64, i64, i64, i64, Vec<u8>, i64, i64) = connection
        .query_row(
            "SELECT namespace,request,purpose,capture_generation,capture_root,capture_floor,reader_owner FROM durable_operation_owner",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
        )
        .unwrap();
    assert_eq!(row.0, 1);
    assert_eq!(row.1, i64::try_from(request).unwrap());
    assert_eq!(row.2, 2);
    assert_eq!(row.3, capture.generation.number());
    assert_eq!(row.4, capture.base_root);
    assert_eq!(row.5, i64::try_from(reader.installed_floor()).unwrap());
    assert_eq!(row.6, i64::try_from(reader.owner_id()).unwrap());
    assert_eq!(
        connection
            .query_row(
                "SELECT operation_count FROM durable_owner_counts",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    drop(connection);
    let before = fs::read(fixture.path()).unwrap();
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY),
        Err(OverlayError::Invalid(
            "durable overlay requires reconciliation"
        ))
    ));
    assert_eq!(fs::read(fixture.path()).unwrap(), before);
}

#[test]
fn failed_owner_record_insert_rolls_back_sql_and_memory_custody() {
    let fixture = Fixture::new();
    fixture.create(true).close_database().unwrap();
    durable_schema45::convert(&fixture.path());
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_owner BEFORE INSERT ON durable_operation_owner BEGIN SELECT RAISE(ABORT, 'injected owner failure'); END;").unwrap();
    drop(connection);

    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    assert!(db.acquire_operation(77).is_err());
    assert!(db.retained_operation(77).unwrap().is_none());
    db.close_database().unwrap();
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    let state: (i64, i64, i64) = connection.query_row(
        "SELECT (SELECT count(*) FROM durable_operation_owner),operation_count,(SELECT operation_owners FROM durable_custody WHERE id=1) FROM durable_owner_counts WHERE id=1",
        [],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
    ).unwrap();
    assert_eq!(state, (0, 0, 0));
}

#[test]
fn orphan_and_operation_are_recorded_separately_and_remain_unresolved() {
    let fixture = Fixture::new();
    let db = fixture.create(true);
    let source = db.acquire_base_source(1).unwrap();
    let mut file = File::new(&db, source, 9, Vec::new());
    file.write(0, b"original named data");
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
    let _operation = db.acquire_operation(7).unwrap();
    assert!(db.check_file(handle, false).is_ok());
    drop(file);
    drop(db);

    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    let counts: (i64, i64, i64, i64) = connection.query_row(
        "SELECT orphan_count,operation_count,(SELECT orphan_owners FROM durable_custody),(SELECT operation_owners FROM durable_custody) FROM durable_owner_counts",
        [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
    ).unwrap();
    assert_eq!(counts, (1, 1, 1, 1));
    let orphan: (i64, Vec<u8>, i64, i64) = connection
        .query_row(
            "SELECT serial,root,top,floor FROM durable_orphan_owner",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(orphan.0, 9);
    assert_eq!(orphan.1, ROOT);
    assert!(orphan.2 >= orphan.3 && orphan.3 >= 0);
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM fs_inode WHERE inode=9 AND generation=-1",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    drop(connection);
    let before = fs::read(fixture.path()).unwrap();
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY),
        Err(OverlayError::Invalid(
            "durable overlay requires reconciliation"
        ))
    ));
    assert_eq!(fs::read(fixture.path()).unwrap(), before);
}

#[test]
fn typed_owner_requires_namespace_and_complete_captured_identity() {
    let fixture = Fixture::new();
    let db = fixture.create(false);
    let capture = db.capture().unwrap();
    let reader = db.acquire_captured_reader(capture, 9).unwrap();
    let request = u64::try_from(capture.generation.number()).unwrap();
    assert!(db.acquire_captured_operation(request, reader).is_err());
    assert!(db.retained_operation(request).unwrap().is_none());
    drop(db);
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    assert!(connection
        .execute(
            "INSERT INTO durable_operation_owner(owner,namespace,request,purpose) VALUES(9,1,9,2)",
            []
        )
        .is_err());
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM durable_operation_owner", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}
