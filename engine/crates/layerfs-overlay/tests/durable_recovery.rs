//! Durable overlay transaction and owner-fence proofs through the public API.
#![cfg(target_os = "linux")]

mod payload_support;

use layerfs_overlay::{
    Changes, MaintenanceCursor, Overlay, OverlayError, PayloadWrite, ProfileConfig,
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
    assert_eq!(db.profile().synchronous, 2);
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
    let state = db.state().unwrap();
    assert_eq!(state.base_root, ROOT);
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
    assert_recovered(&reopened);
    reopened.close_database().unwrap();
}

#[test]
fn durable_overlay_retains_uncertain_capture_and_volatile_owners() {
    for capture in [false, true] {
        let fixture = Fixture::new();
        let db = create(&fixture.path());
        if capture {
            db.capture().unwrap();
        } else {
            db.acquire_operation(7).unwrap();
        }
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
    for owner in [false, true] {
        let fixture = Fixture::new();
        let db = create(&fixture.path());
        let target = if owner {
            fixture.0.join("overlay.sqlite.owner")
        } else {
            fixture.path()
        };
        let saved = fixture.0.join("displaced");
        fs::rename(&target, &saved).unwrap();
        assert!(matches!(db.state(), Err(OverlayError::Uncertain { .. })));
        fs::rename(saved, target).unwrap();
        assert!(matches!(db.state(), Err(OverlayError::Quarantined)));
        db.close_database().unwrap();
    }
    let fixture = Fixture::new();
    let db = create(&fixture.path());
    fs::set_permissions(&fixture.0, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(matches!(db.state(), Err(OverlayError::Uncertain { .. })));
    fs::set_permissions(&fixture.0, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(matches!(db.state(), Err(OverlayError::Quarantined)));
    db.close_database().unwrap();
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
        // Leave the uncertain SQLite connection untouched; process termination
        // releases it without an application-level rollback or replay.
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
            put(&db);
            let size = fs::metadata(&path).unwrap().len();
            assert!(size > 0);
            eprintln!("LAYERFS_VM_ACK overlay_bytes={size}");
            std::thread::sleep(Duration::from_secs(60));
            panic!("host did not terminate the writer guest");
        }
        "read" => {
            let db = Overlay::recover_durable(&path, ProfileConfig::default(), IDENTITY).unwrap();
            assert_recovered(&db);
            eprintln!("LAYERFS_VM_RECOVERED");
            std::thread::sleep(Duration::from_secs(60));
            panic!("host did not terminate the reader guest");
        }
        _ => panic!("unknown guest mode"),
    }
}
