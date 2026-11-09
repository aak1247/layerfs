//! Durable overlay transaction and owner-fence proofs through the public API.
#![cfg(target_os = "linux")]

mod payload_support;

use layerfs_overlay::{
    Changes, MaintenanceCursor, Overlay, OverlayError, PayloadWrite, ProfileConfig, StatementKind,
};
use payload_support::{claims, committed, noise, part, File as PayloadFile};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const IDENTITY: [u8; 32] = [43; 32];
const ROOT: [u8; 32] = [17; 32];
const BYTES: &[u8] = b"uncommitted durable workspace bytes";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "layerfs-durable-overlay-{}-{}",
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

fn create(path: &Path) -> Overlay {
    let db = Overlay::create_durable(path, ProfileConfig::default(), IDENTITY).unwrap();
    assert_eq!(db.profile().journal_mode, "delete");
    assert_eq!(db.profile().synchronous, 3);
    db.open_workspace(ROOT).unwrap();
    db
}

fn put(db: &Overlay) {
    let source = db.acquire_base_source(1).unwrap();
    let mut file = PayloadFile::new(db, source, 9, Vec::new());
    file.write(0, BYTES);
    // Deliberately leave this process-local base reader open. A different
    // engine may discard it only after the original owner has been fenced.
}

fn assert_recovered(db: &Overlay) {
    assert_recovered_at(db, ROOT);
}
fn assert_recovered_at(db: &Overlay, root: [u8; 32]) {
    let state = db.state().unwrap();
    assert_eq!(state.base_root, root);
    assert_eq!(state.dirty_inodes, 1);
    assert_eq!(state.base_readers, 0);
    let source = db.acquire_base_source(2).unwrap();
    let read = db.source_read(source, 9, 0, 1024).unwrap().unwrap();
    assert_eq!(read.data, BYTES);
    assert!(read.inherited.is_empty());
    db.release_base_source(source).unwrap();
}

#[test]
fn durable_overlay_fences_live_owner_and_checks_identity() {
    const CHILD: &str = "LAYERFS_DURABLE_LOCK_CHILD";
    if let Some(path) = std::env::var_os(CHILD) {
        assert!(
            Overlay::recover_durable(Path::new(&path), ProfileConfig::default(), IDENTITY).is_err()
        );
        return;
    }
    let fixture = Fixture::new();
    let db = create(&fixture.path());
    put(&db);
    assert!(Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).is_err());
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable_overlay_fences_live_owner_and_checks_identity",
        ])
        .env(CHILD, fixture.path())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "contending owner: {status}");
            break;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("contending owner did not exit");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let closed = db.close_database_file().unwrap();
    assert!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).is_err(),
        "the lease must outlive the old main-file descriptor"
    );
    drop(closed);
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), [44; 32]),
        Err(OverlayError::Invalid("durable overlay owner identity"))
    ));
    let reopened =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    assert_eq!(reopened.profile().synchronous, 3);
    assert_recovered(&reopened);
    let source = reopened.acquire_base_source(3).unwrap();
    let mut file = PayloadFile::new(&reopened, source, 9, Vec::new());
    file.expect = BYTES.to_vec();
    file.touched = true;
    file.write(BYTES.len() as u64, b" after recovery");
    reopened.release_base_source(source).unwrap();
    reopened.close_database().unwrap();
    let reopened =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    let source = reopened.acquire_base_source(4).unwrap();
    let read = reopened.source_read(source, 9, 0, 1024).unwrap().unwrap();
    assert_eq!(
        read.data,
        b"uncommitted durable workspace bytes after recovery"
    );
    reopened.release_base_source(source).unwrap();
    reopened.close_database().unwrap();
}

#[test]
fn durable_overlay_recovers_after_writer_process_is_killed() {
    const CHILD: &str = "LAYERFS_DURABLE_OVERLAY_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let db = create(&directory.join("overlay.sqlite"));
        db.initialize_durable_binding(ROOT, b"original snapshot")
            .unwrap();
        put(&db);
        fs::write(directory.join("ack"), b"completed").unwrap();
        std::thread::sleep(Duration::from_secs(15));
        panic!("the parent did not kill the writer");
    }
    let fixture = Fixture::new();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable_overlay_recovers_after_writer_process_is_killed",
        ])
        .env(CHILD, &fixture.0)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !fixture.0.join("ack").exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("writer exited before acknowledgement: {status}");
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("writer did not acknowledge in time");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    let reopened =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    assert_eq!(
        reopened.durable_binding().unwrap().unwrap(),
        b"original snapshot"
    );
    assert_recovered(&reopened);
    reopened.close_database().unwrap();
}

#[test]
fn legacy_durable_database_reopens_without_implicit_conversion() {
    let fixture = Fixture::new();
    let db = create(&fixture.path());
    put(&db);
    db.close_database().unwrap();
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    connection
        .execute_batch("DROP TABLE durable_binding; PRAGMA user_version=39;")
        .unwrap();
    drop(connection);
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    assert_eq!(db.profile().schema_version, 39);
    assert!(db.durable_binding().is_err());
    assert_recovered(&db);
    db.close_database().unwrap();
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        39
    );
}

#[test]
fn durable_binding_never_follows_an_unrecorded_install() {
    let fixture = Fixture::new();
    let db = create(&fixture.path());
    assert!(db.require_recorded_recovery_ready().is_err());
    db.initialize_durable_binding(ROOT, b"original snapshot")
        .unwrap();
    db.require_recorded_recovery_ready().unwrap();
    assert!(db.initialize_durable_binding(ROOT, b"replacement").is_err());
    let capture = db.capture().unwrap();
    assert!(db.require_recorded_recovery_ready().is_err());
    db.install(capture, [18; 32]).unwrap();
    assert!(db.require_recorded_recovery_ready().is_err());
    assert!(matches!(
        db.durable_binding(),
        Err(OverlayError::Invalid(
            "durable binding requires reconciliation"
        ))
    ));
    let resolved = db.capture().unwrap();
    db.install_recorded(resolved, [19; 32], b"resolved snapshot")
        .unwrap();
    assert_eq!(db.durable_binding().unwrap().unwrap(), b"resolved snapshot");
    db.require_recorded_recovery_ready().unwrap();
    db.close_database().unwrap();
}

#[test]
fn recovery_readiness_does_not_scan_a_large_safe_cleanup_queue() {
    let fixture = Fixture::new();
    let db = create(&fixture.path());
    db.initialize_durable_binding(ROOT, b"original snapshot")
        .unwrap();
    db.close_database().unwrap();
    let mut connection = rusqlite::Connection::open(fixture.path()).unwrap();
    let transaction = connection.transaction().unwrap();
    for resource in 1..=10_000 {
        transaction
            .execute(
                "INSERT INTO cleanup_task(kind,resource,target,ready) VALUES(4,?1,1,1)",
                [resource],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    drop(connection);
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    let before = db.diagnostics().statements[StatementKind::Frontier as usize];
    db.require_recorded_recovery_ready().unwrap();
    let after = db.diagnostics().statements[StatementKind::Frontier as usize];
    assert!(after.fullscan_steps - before.fullscan_steps < 64);
    assert!(after.vm_steps - before.vm_steps < 1000);
    db.close_database().unwrap();
}

#[test]
fn durable_overlay_resumes_receipted_retirement_without_losing_new_writes() {
    for partial in [false, true] {
        let fixture = Fixture::new();
        let db = create(&fixture.path());
        db.initialize_durable_binding(ROOT, b"original snapshot")
            .unwrap();
        let source = db.acquire_base_source(1).unwrap();
        let mut file = PayloadFile::new(&db, source, 9, Vec::new());
        file.write(0, b"the captured generation");
        db.release_base_source(source).unwrap();
        let capture = db.capture().unwrap();
        db.install_recorded(capture, [18; 32], b"installed snapshot")
            .unwrap();
        put(&db);
        assert!(!db.maintenance_idle().unwrap());
        if partial {
            assert!(db.maintain(MaintenanceCursor::default()).unwrap().is_some());
        }
        db.close_database().unwrap();

        let db =
            Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
        assert_eq!(
            db.durable_binding().unwrap().unwrap(),
            b"installed snapshot"
        );
        let mut cursor = MaintenanceCursor::default();
        for _ in 0..64 {
            match db.maintain(cursor).unwrap() {
                Some(step) => cursor = step.cursor,
                None => break,
            }
        }
        assert!(db.maintenance_idle().unwrap());
        assert_recovered_at(&db, [18; 32]);
        db.close_database().unwrap();
    }
}

#[test]
fn durable_overlay_keeps_unreceipted_retirement_for_reconciliation() {
    let fixture = Fixture::new();
    let db = create(&fixture.path());
    db.initialize_durable_binding(ROOT, b"original snapshot")
        .unwrap();
    let capture = db.capture().unwrap();
    db.install(capture, [18; 32]).unwrap();
    assert!(!db.maintenance_idle().unwrap());
    db.close_database().unwrap();
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY),
        Err(OverlayError::Invalid(
            "durable overlay requires reconciliation"
        ))
    ));
    assert!(fixture.path().is_file());
}

#[test]
fn durable_overlay_refuses_parked_or_wrong_retirement_targets() {
    for update in [
        "UPDATE cleanup_task SET ready=0 WHERE kind=3",
        "UPDATE cleanup_task SET target=2 WHERE kind=3",
        "UPDATE cleanup_task SET resource=1 WHERE kind=3",
    ] {
        let fixture = Fixture::new();
        let db = create(&fixture.path());
        db.initialize_durable_binding(ROOT, b"original snapshot")
            .unwrap();
        let capture = db.capture().unwrap();
        db.install_recorded(capture, [18; 32], b"installed snapshot")
            .unwrap();
        db.close_database().unwrap();
        let connection = rusqlite::Connection::open(fixture.path()).unwrap();
        assert_eq!(connection.execute(update, []).unwrap(), 1);
        drop(connection);
        assert!(matches!(
            Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY),
            Err(OverlayError::Invalid(
                "durable overlay requires reconciliation"
            ))
        ));
        assert!(fixture.path().is_file());
    }
}

#[test]
fn durable_overlay_retains_uncertain_capture_and_volatile_owners() {
    for capture in [false, true] {
        let fixture = Fixture::new();
        let db = create(&fixture.path());
        db.initialize_durable_binding(ROOT, b"original snapshot")
            .unwrap();
        if capture {
            db.capture().unwrap();
        } else {
            db.acquire_operation(7).unwrap();
        }
        assert!(db.require_recorded_recovery_ready().is_err());
        drop(db);
        assert!(matches!(
            Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY),
            Err(OverlayError::Invalid(
                "durable overlay requires reconciliation"
            ))
        ));
        assert!(
            fixture.path().is_file(),
            "refusal retains the original file"
        );
    }
}

#[test]
fn durable_overlay_recovers_pending_shrink_cleanup() {
    let fixture = Fixture::new();
    let db = create(&fixture.path());
    let source = db.acquire_base_source(1).unwrap();
    let mut file = PayloadFile::new(&db, source, 9, Vec::new());
    file.write(0, b"abcdefgh");
    file.resize(3);
    db.release_base_source(source).unwrap();
    assert!(!db.maintenance_idle().unwrap());
    db.close_database().unwrap();
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    assert!(db.maintenance_pending());
    let mut cursor = MaintenanceCursor::default();
    for _ in 0..32 {
        match db.maintain(cursor).unwrap() {
            Some(step) => cursor = step.cursor,
            None => break,
        }
    }
    assert!(db.maintenance_idle().unwrap());
    let source = db.acquire_base_source(2).unwrap();
    assert_eq!(
        db.source_read(source, 9, 0, 64).unwrap().unwrap().data,
        b"abc"
    );
    db.release_base_source(source).unwrap();
    db.close_database().unwrap();
}

#[test]
fn durable_overlay_recovers_pending_reference_piece_cleanup() {
    let fixture = Fixture::new();
    let db = create(&fixture.path());
    let source = db.acquire_base_source(1).unwrap();
    let content = noise(64 * 1024, 91);
    let mut file = committed(&db, source, &content);
    file.write_claimed(
        0,
        part(&content, 8192, 32768),
        claims(None, &[(0, 32768, 8192)]),
    );
    file.resize(4096);
    let expected = file.expect.clone();
    db.release_base_source(source).unwrap();
    assert!(!db.maintenance_idle().unwrap());
    db.close_database().unwrap();
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    let source = db.acquire_base_source(2).unwrap();
    let mut restored = committed(&db, source, &content);
    restored.expect = expected;
    restored.touched = true;
    restored.check();
    let mut cursor = MaintenanceCursor::default();
    for _ in 0..32 {
        match db.maintain(cursor).unwrap() {
            Some(step) => cursor = step.cursor,
            None => break,
        }
    }
    assert!(db.maintenance_idle().unwrap());
    restored.check();
    db.release_base_source(source).unwrap();
    db.close_database().unwrap();
}

#[test]
fn durable_overlay_refuses_nonprivate_directory_and_unaccounted_quota() {
    let fixture = Fixture::new();
    let limited = ProfileConfig {
        max_pages: Some(128),
        ..ProfileConfig::default()
    };
    assert!(Overlay::create_durable(&fixture.path(), limited, IDENTITY).is_err());
    assert!(!fixture.path().exists());
    fs::set_permissions(&fixture.0, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(Overlay::create_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).is_err());
    assert!(!fixture.path().exists());
}

#[test]
fn durable_overlay_quarantines_replaced_owner_or_database_path() {
    const CHILD: &str = "LAYERFS_DURABLE_QUARANTINE_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        for owner in [false, true] {
            let path = directory.join(if owner {
                "owner.sqlite"
            } else {
                "database.sqlite"
            });
            let db = create(&path);
            let target = if owner {
                directory.join("owner.sqlite.owner")
            } else {
                path.clone()
            };
            let saved = directory.join("displaced");
            fs::rename(&target, &saved).unwrap();
            assert!(matches!(db.state(), Err(OverlayError::Uncertain { .. })));
            fs::rename(saved, target).unwrap();
            assert!(matches!(db.state(), Err(OverlayError::Quarantined)));
            if owner {
                assert!(matches!(
                    db.close_database(),
                    Err(OverlayError::Quarantined)
                ));
            } else {
                drop(db);
            }
            assert!(Overlay::recover_durable(&path, ProfileConfig::default(), IDENTITY).is_err());
        }
        let path = directory.join("private.sqlite");
        let db = create(&path);
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(db.state(), Err(OverlayError::Uncertain { .. })));
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(matches!(db.state(), Err(OverlayError::Quarantined)));
        assert!(matches!(
            db.close_database(),
            Err(OverlayError::Quarantined)
        ));
        assert!(Overlay::recover_durable(&path, ProfileConfig::default(), IDENTITY).is_err());
        let path = directory.join("panic.sqlite");
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let unwound = std::panic::catch_unwind(|| {
            let _db = create(&path);
            panic!("test owner unwind");
        });
        std::panic::set_hook(hook);
        assert!(unwound.is_err());
        assert!(Overlay::recover_durable(&path, ProfileConfig::default(), IDENTITY).is_err());
        return;
    }
    let fixture = Fixture::new();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable_overlay_quarantines_replaced_owner_or_database_path",
        ])
        .env(CHILD, &fixture.0)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "quarantined owner: {status}");
            break;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("quarantined owner did not exit");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    for name in [
        "database.sqlite",
        "owner.sqlite",
        "private.sqlite",
        "panic.sqlite",
    ] {
        let db =
            Overlay::recover_durable(&fixture.0.join(name), ProfileConfig::default(), IDENTITY)
                .unwrap();
        db.close_database().unwrap();
    }
}

#[test]
fn durable_overlay_quarantines_failed_journal_sync_and_recovers_prior_state() {
    const CHILD: &str = "LAYERFS_DURABLE_SYNC_FAILURE_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let db = create(&directory.join("overlay.sqlite"));
        let source = db.acquire_base_source(1).unwrap();
        let mut file = PayloadFile::new(&db, source, 9, Vec::new());
        file.write(0, b"baseline");
        fs::write(directory.join("inject"), b"1").unwrap();
        let next: Arc<[u8]> = Arc::from(b"new value".as_slice());
        let result = db.apply(
            source,
            &Changes {
                inodes: vec![file.inode(next.len() as u64)],
                write: Some(PayloadWrite {
                    serial: 9,
                    offset: 0,
                    data: next,
                }),
                ..Changes::default()
            },
        );
        assert!(
            matches!(result, Err(OverlayError::Uncertain { .. })),
            "{result:?}"
        );
        assert!(matches!(db.state(), Err(OverlayError::Quarantined)));
        assert!(matches!(
            db.close_database(),
            Err(OverlayError::Quarantined)
        ));
        assert!(Overlay::recover_durable(
            &directory.join("overlay.sqlite"),
            ProfileConfig::default(),
            IDENTITY,
        )
        .is_err());
        // Neither explicit close nor dropping its error may release the
        // uncertain connection or its lease. Process exit releases both.
        std::process::exit(0);
    }
    let fixture = Fixture::new();
    let shim = fixture.0.join("sync-failure.so");
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/durable_sync_failure.c");
    let compiled = Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(source)
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap();
    assert!(compiled.success());
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable_overlay_quarantines_failed_journal_sync_and_recovers_prior_state",
        ])
        .env(CHILD, &fixture.0)
        .env("LD_PRELOAD", &shim)
        .env("LAYERFS_TEST_SYNC_TRIGGER", fixture.0.join("inject"))
        .env("LAYERFS_TEST_SYNC_RECEIPT", fixture.0.join("sync-failed"))
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("sync failure probe did not exit");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success(), "sync failure child: {status}");
    assert_eq!(fs::read(fixture.0.join("sync-failed")).unwrap(), b"EIO\n");
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    let state = db.state().unwrap();
    assert_eq!(state.revision, 1);
    let source = db.acquire_base_source(2).unwrap();
    assert_eq!(
        db.source_read(source, 9, 0, 64).unwrap().unwrap().data,
        b"baseline"
    );
    db.release_base_source(source).unwrap();
    db.close_database().unwrap();
}

#[test]
fn failed_install_sync_retains_matching_root_and_binding() {
    const CHILD: &str = "LAYERFS_DURABLE_INSTALL_SYNC_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let db = create(&directory.join("overlay.sqlite"));
        db.initialize_durable_binding(ROOT, b"original snapshot")
            .unwrap();
        let capture = db.capture().unwrap();
        fs::write(directory.join("inject"), b"1").unwrap();
        assert!(matches!(
            db.install_recorded(capture, [18; 32], b"new snapshot"),
            Err(OverlayError::Uncertain { .. })
        ));
        assert!(matches!(
            db.close_database(),
            Err(OverlayError::Quarantined)
        ));
        std::process::exit(0);
    }
    let fixture = Fixture::new();
    let shim = fixture.0.join("install-sync-failure.so");
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/durable_sync_failure.c");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(source)
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "failed_install_sync_retains_matching_root_and_binding",
        ])
        .env(CHILD, &fixture.0)
        .env("LD_PRELOAD", &shim)
        .env("LAYERFS_TEST_SYNC_TRIGGER", fixture.0.join("inject"))
        .env("LAYERFS_TEST_SYNC_RECEIPT", fixture.0.join("sync-failed"))
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success(), "install sync failure child: {status}");
    assert_eq!(fs::read(fixture.0.join("sync-failed")).unwrap(), b"EIO\n");
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    let (root, captured): (Vec<u8>, Option<i64>) = connection
        .query_row(
            "SELECT base_root,captured FROM durable_state WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let (binding_root, snapshot, recoverable): (Vec<u8>, Vec<u8>, bool) = connection
        .query_row(
            "SELECT root,snapshot,recoverable FROM durable_binding WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(root, ROOT);
    assert_eq!(binding_root, ROOT);
    assert_eq!(snapshot, b"original snapshot");
    assert!(
        recoverable && captured.is_some(),
        "the original in-flight capture remains unresolved"
    );
    drop(connection);
    assert!(Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).is_err());
}

#[test]
fn directory_sync_failure_never_acknowledges_the_write() {
    const CHILD: &str = "LAYERFS_DURABLE_DIR_SYNC_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let db = create(&directory.join("overlay.sqlite"));
        let source = db.acquire_base_source(1).unwrap();
        let mut file = PayloadFile::new(&db, source, 9, Vec::new());
        file.write(0, b"baseline");
        fs::write(directory.join("inject-directory"), b"1").unwrap();
        let next: Arc<[u8]> = Arc::from(b"new value".as_slice());
        let result = db.apply(
            source,
            &Changes {
                inodes: vec![file.inode(next.len() as u64)],
                write: Some(PayloadWrite {
                    serial: 9,
                    offset: 0,
                    data: next,
                }),
                ..Changes::default()
            },
        );
        assert!(
            matches!(result, Err(OverlayError::Uncertain { .. })),
            "{result:?}"
        );
        assert!(matches!(
            db.close_database(),
            Err(OverlayError::Quarantined)
        ));
        assert!(Overlay::recover_durable(
            &directory.join("overlay.sqlite"),
            ProfileConfig::default(),
            IDENTITY
        )
        .is_err());
        std::process::exit(0);
    }
    let fixture = Fixture::new();
    let shim = fixture.0.join("directory-sync-failure.so");
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/durable_dir_sync_failure.c");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(source)
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "directory_sync_failure_never_acknowledges_the_write",
        ])
        .env(CHILD, &fixture.0)
        .env("LD_PRELOAD", &shim)
        .env(
            "LAYERFS_TEST_DIR_SYNC_TRIGGER",
            fixture.0.join("inject-directory"),
        )
        .env(
            "LAYERFS_TEST_DIR_SYNC_TARGET",
            fs::canonicalize(&fixture.0).unwrap(),
        )
        .env(
            "LAYERFS_TEST_DIR_SYNC_RECEIPT",
            fixture.0.join("directory-sync-failed"),
        )
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success(), "directory-sync failure child: {status}");
    assert!(!fs::read(fixture.0.join("directory-sync-failed"))
        .unwrap()
        .is_empty());
    let db = Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), IDENTITY).unwrap();
    let source = db.acquire_base_source(2).unwrap();
    let bytes = db.source_read(source, 9, 0, 64).unwrap().unwrap().data;
    assert!(
        bytes == b"baseline" || bytes == b"new value",
        "unknown outcome stays one whole transaction"
    );
    db.release_base_source(source).unwrap();
    db.close_database().unwrap();
}

#[test]
#[ignore = "run only inside the owned VM power-loss harness"]
fn durable_save_survives_vm_power_loss() {
    let directory = PathBuf::from(std::env::var("LAYERFS_DURABLE_VM_ROOT").unwrap());
    let mode = std::env::var("LAYERFS_DURABLE_VM_MODE").unwrap();
    let path = directory.join("overlay.sqlite");
    match mode.as_str() {
        "write" => {
            fs::create_dir(&directory).unwrap();
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
            fs::File::open(directory.parent().unwrap())
                .unwrap()
                .sync_all()
                .unwrap();
            let db = create(&path);
            db.initialize_durable_binding(ROOT, b"vm original snapshot")
                .unwrap();
            let capture = db.capture().unwrap();
            db.install_recorded(capture, [18; 32], b"vm installed snapshot")
                .unwrap();
            put(&db);
            assert!(!db.maintenance_idle().unwrap());
            let size = fs::metadata(&path).unwrap().len();
            assert!(size > 0);
            eprintln!("LAYERFS_VM_ACK overlay_bytes={size}");
            std::thread::sleep(Duration::from_secs(60));
            panic!("host did not terminate the writer guest");
        }
        "read" => {
            let db = Overlay::recover_durable(&path, ProfileConfig::default(), IDENTITY).unwrap();
            assert_eq!(
                db.durable_binding().unwrap().unwrap(),
                b"vm installed snapshot"
            );
            assert_recovered_at(&db, [18; 32]);
            let mut cursor = MaintenanceCursor::default();
            for _ in 0..32 {
                match db.maintain(cursor).unwrap() {
                    Some(step) => cursor = step.cursor,
                    None => break,
                }
            }
            assert!(db.maintenance_idle().unwrap());
            assert_recovered_at(&db, [18; 32]);
            eprintln!("LAYERFS_VM_RECOVERED");
            std::thread::sleep(Duration::from_secs(60));
            panic!("host did not terminate the reader guest");
        }
        _ => panic!("unknown guest mode"),
    }
}
