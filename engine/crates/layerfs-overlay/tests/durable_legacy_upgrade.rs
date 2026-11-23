//! Genuine 277 durable data is not relabelled or discarded during upgrade.
#![cfg(target_os = "linux")]

use layerfs_overlay::{
    Changes, Overlay, OverlayCatalogRecovery, OverlayError, ProfileConfig, ReadyRecovery,
};
use rusqlite::Connection;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const IDENTITY: [u8; 32] = [42; 32];
const DIRTY: &[u8] = b"Domplete installed file 000001\n";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        Self::named("legacy_277")
    }
    fn named(name: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "layerfs-legacy-277-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name);
        for name in ["overlay.sqlite", "overlay.sqlite.owner"] {
            fs::copy(source.join(name), dir.join(name)).unwrap();
            fs::set_permissions(dir.join(name), fs::Permissions::from_mode(0o600)).unwrap();
        }
        Self(dir)
    }
    fn path(&self) -> PathBuf {
        self.0.join("overlay.sqlite")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_LEGACY_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn ready(path: &Path) -> ReadyRecovery {
    ready_for(path, IDENTITY)
}
fn ready_for(path: &Path, identity: [u8; 32]) -> ReadyRecovery {
    match OverlayCatalogRecovery::open(path, ProfileConfig::default(), identity).unwrap() {
        OverlayCatalogRecovery::Ready(value) => value,
        _ => panic!("expected a locally ready predecessor"),
    }
}

fn schema(path: &Path) -> Vec<(String, String, String, Option<String>)> {
    let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut statement = db
        .prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")
        .unwrap();
    let rows = statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap();
    rows.collect::<Result<_, _>>().unwrap()
}

fn schema_digest(rows: &[(String, String, String, Option<String>)]) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs:durable-schema:v1\0");
    for (kind, name, table, sql) in rows {
        for value in [kind, name, table] {
            hash.update(&(value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        match sql {
            Some(value) => {
                hash.update(&[1]);
                hash.update(&(value.len() as u64).to_be_bytes());
                hash.update(value.as_bytes());
            }
            None => {
                hash.update(&[0]);
            }
        }
    }
    hash.finalize().to_hex().to_string()
}

#[test]
fn genuine_d144_kept_row_survives_upgrade_and_first_writer_counts_it_once() {
    let fixture = Fixture::named("legacy_d144");
    let raw =
        Connection::open_with_flags(fixture.path(), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    assert_eq!(
        raw.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        42
    );
    assert_eq!(
        raw.query_row("SELECT serial,generation FROM durable_kept_live", [], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
        })
        .unwrap(),
        (60, 2)
    );
    assert_eq!(
        raw.query_row("SELECT kept_count FROM durable_custody", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    drop(raw);
    let predecessor = ready_for(&fixture.path(), [43; 32]);
    let observed = predecessor.observed().clone();
    let db = predecessor
        .upgrade_legacy(&observed)
        .unwrap()
        .complete(&observed)
        .unwrap();
    assert_eq!(db.profile().schema_version, 45);
    assert_eq!(db.durable_namespace().unwrap(), Some(1));
    assert_eq!(db.state().unwrap().dirty_inodes, 1);
    drop(db);
    let raw =
        Connection::open_with_flags(fixture.path(), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    assert_eq!(
        raw.query_row("SELECT serial,generation FROM durable_kept_live", [], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
        })
        .unwrap(),
        (60, 2)
    );
    assert_eq!(
        raw.query_row("SELECT kept_count FROM durable_custody", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    drop(raw);
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), [43; 32]).unwrap();
    let mut inode = db.inode(60).unwrap().unwrap();
    assert_eq!(inode.mtime_nanoseconds, 123_456_789);
    assert!(db.inode(61).unwrap().is_some());
    inode.mode = 0o600;
    for request in [7, 8] {
        let source = db.acquire_base_source(request).unwrap();
        let publication = db
            .apply(
                source,
                &Changes {
                    inodes: vec![inode.clone()],
                    ..Changes::default()
                },
            )
            .unwrap();
        db.reply_attempted(publication).unwrap();
        db.release_base_source(source).unwrap();
        assert_eq!(db.state().unwrap().dirty_inodes, 2);
    }
    drop(db);
    let raw =
        Connection::open_with_flags(fixture.path(), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    assert_eq!(
        raw.query_row("SELECT count(*) FROM durable_kept_live", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        raw.query_row("SELECT kept_count FROM durable_custody", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn genuine_277_ready_workspace_upgrades_without_losing_dirty_bytes_or_queue() {
    let fixture = Fixture::new();
    let raw = Connection::open(fixture.path()).unwrap();
    raw.execute("INSERT INTO cleanup_task(kind,resource,target,phase,cursor,aux,name,ready) VALUES(4,3,2,0,0,-1,X'',1)", []).unwrap();
    let queue: Vec<(i64, i64, i64, i64, i64, i64, Vec<u8>, i64)> = {
        let mut statement=raw.prepare("SELECT kind,resource,target,phase,cursor,aux,name,ready FROM cleanup_task ORDER BY kind,resource,target").unwrap();
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            })
            .unwrap();
        rows.collect::<Result<_, _>>().unwrap()
    };
    drop(raw);
    let predecessor = ready(&fixture.path());
    let observed = predecessor.observed().clone();
    assert_eq!(observed.previous_namespace, 1);
    let migrated = predecessor.upgrade_legacy(&observed).unwrap();
    let db = migrated.complete(&observed).unwrap();
    assert_eq!(db.profile().schema_version, 45);
    assert_eq!(db.durable_namespace().unwrap(), Some(1));
    assert_eq!(db.state().unwrap().dirty_inodes, 1);
    let source = db.acquire_base_source(7).unwrap();
    let read = db.source_read(source, 3, 0, 64).unwrap().unwrap();
    assert_eq!(read.data, DIRTY);
    db.release_base_source(source).unwrap();
    drop(db);
    let raw =
        Connection::open_with_flags(fixture.path(), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let actual: Vec<(i64, i64, i64, i64, i64, i64, Vec<u8>, i64)> = {
        let mut statement=raw.prepare("SELECT kind,resource,target,phase,cursor,aux,name,ready FROM cleanup_task ORDER BY kind,resource,target").unwrap();
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            })
            .unwrap();
        rows.collect::<Result<_, _>>().unwrap()
    };
    assert_eq!(actual, queue);
    drop(raw);
    let newdir = fixture.0.join("fresh");
    fs::create_dir(&newdir).unwrap();
    fs::set_permissions(&newdir, fs::Permissions::from_mode(0o700)).unwrap();
    let fresh = newdir.join("fresh.sqlite");
    drop(Overlay::create_durable(&fresh, ProfileConfig::default(), [43; 32]).unwrap());
    let canonical: Vec<_> = schema(&fresh)
        .into_iter()
        .filter(|(_, _, table, _)| table != "durable_external_capture")
        .collect();
    println!(
        "FRESH_SCHEMA45_COUNT={} BLAKE3={}",
        canonical.len(),
        schema_digest(&canonical)
    );
    assert_eq!(schema(&fixture.path()), canonical);
    let reopened = ready(&fixture.path());
    assert_eq!(reopened.observed(), &observed);
}

#[test]
fn altered_legacy_schema_and_unknown_custody_do_not_migrate() {
    let forged = Fixture::new();
    let raw = Connection::open(forged.path()).unwrap();
    raw.execute_batch(
        "DROP INDEX fs_inode_capture; CREATE INDEX fs_inode_capture ON fs_inode(generation)",
    )
    .unwrap();
    drop(raw);
    let predecessor = ready(&forged.path());
    let observed = predecessor.observed().clone();
    let (error, retained) = predecessor.upgrade_legacy(&observed).err().unwrap();
    assert!(matches!(
        error,
        OverlayError::Invalid("legacy overlay schema identity")
    ));
    drop(retained);
    let raw =
        Connection::open_with_flags(forged.path(), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    assert_eq!(
        raw.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        41
    );
    assert_eq!(
        raw.query_row("SELECT dirty_inodes FROM durable_state", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    drop(raw);

    let unresolved = Fixture::new();
    let raw = Connection::open(unresolved.path()).unwrap();
    raw.execute("UPDATE durable_state SET volatile_owners=1", [])
        .unwrap();
    drop(raw);
    let before = fs::read(unresolved.path()).unwrap();
    assert!(matches!(
        OverlayCatalogRecovery::open(&unresolved.path(), ProfileConfig::default(), IDENTITY),
        Err(OverlayError::Invalid(
            "legacy durable volatile custody unresolved"
        ))
    ));
    assert_eq!(fs::read(unresolved.path()).unwrap(), before);
}

mod failure_injection {
    use super::*;
    use rusqlite::{auto_extension, ffi};
    use std::{
        ffi::{c_char, c_int, c_void, CStr},
        sync::Mutex,
    };

    static WATCHED: Mutex<Option<PathBuf>> = Mutex::new(None);
    static INSTALLED: AtomicU64 = AtomicU64::new(0);
    static FIRED: AtomicU64 = AtomicU64::new(0);

    unsafe extern "C" fn authorize(
        _: *mut c_void,
        action: c_int,
        first: *const c_char,
        _: *const c_char,
        _: *const c_char,
        _: *const c_char,
    ) -> c_int {
        if action == ffi::SQLITE_CREATE_TABLE && !first.is_null() {
            // SAFETY: SQLite passes a NUL-terminated object name for this action.
            if unsafe { CStr::from_ptr(first) }.to_bytes() == b"durable_owner_counts" {
                FIRED.fetch_add(1, Ordering::Relaxed);
                return ffi::SQLITE_DENY;
            }
        }
        ffi::SQLITE_OK
    }

    fn install(connection: Connection) -> rusqlite::Result<()> {
        if connection.path().map(Path::new) != WATCHED.lock().unwrap().as_deref() {
            return Ok(());
        }
        // SAFETY: SQLite owns the live handle and the static callback outlives it.
        assert_eq!(
            unsafe {
                ffi::sqlite3_set_authorizer(
                    connection.handle(),
                    Some(authorize),
                    std::ptr::null_mut(),
                )
            },
            ffi::SQLITE_OK
        );
        INSTALLED.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    unsafe extern "C" fn opened(
        db: *mut ffi::sqlite3,
        error: *mut *mut c_char,
        _: *const ffi::sqlite3_api_routines,
    ) -> c_int {
        // SAFETY: SQLite invokes the registered callback on its own connection.
        unsafe { auto_extension::init_auto_extension(db, error, install) }
    }

    #[test]
    fn legacy_upgrade_rolls_back_all_ddl_on_injected_failure() {
        let fixture = Fixture::new();
        let before = fs::read(fixture.path()).unwrap();
        *WATCHED.lock().unwrap() = Some(fixture.path());
        // SAFETY: this callback neither opens nor closes SQLite connections.
        unsafe { auto_extension::register_auto_extension(opened).unwrap() };
        let predecessor = ready(&fixture.path());
        assert_eq!(INSTALLED.load(Ordering::Relaxed), 1);
        assert!(auto_extension::cancel_auto_extension(opened));
        let observed = predecessor.observed().clone();
        let (error, retained) = predecessor.upgrade_legacy(&observed).err().unwrap();
        assert!(
            matches!(error,OverlayError::Sql(rusqlite::Error::SqliteFailure(ref e,_)) if e.extended_code==ffi::SQLITE_AUTH)
        );
        assert!(
            FIRED.load(Ordering::Relaxed) > 0,
            "the injected post-DDL failure did not run"
        );
        assert_eq!(retained.observed(), &observed);
        drop(retained);
        assert_eq!(fs::read(fixture.path()).unwrap(), before);
        *WATCHED.lock().unwrap() = None;
    }
}
