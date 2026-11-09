//! Terminal discovery requires an exact acknowledged close, including its namespace.
#![cfg(target_os = "linux")]

use layerfs_bridge::control::{Reply, WorkspaceToken};
use layerfs_daemon::durable_roster::{Authority, Roster, RosterError, WorkspaceRecord};
use layerfs_history::{BranchId, WorkspaceId};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

const AUTHORITY: Authority = Authority {
    sandbox: [21; 32],
    store: [22; 32],
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "layerfs-roster-lifecycle-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_DURABLE_ROSTER_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn record(id: u8) -> WorkspaceRecord {
    WorkspaceRecord {
        workspace: WorkspaceId::from_authority([id; 32]).unwrap(),
        branch: BranchId::from_authority([id; 16]),
    }
}
fn token(id: u8, namespace: i64) -> WorkspaceToken {
    WorkspaceToken {
        workspace: record(id).workspace,
        namespace,
    }
}
fn marker(path: &Path, record: WorkspaceRecord, suffix: &str) -> PathBuf {
    path.join(record.overlay_name().replace(".sqlite", suffix))
}

#[test]
fn pending_close_blocks_discovery_until_the_exact_receipt_is_recorded() {
    let fixture = Fixture::new();
    let mut roster = Roster::create(&fixture.0, AUTHORITY).unwrap();
    roster.record(record(1)).unwrap();
    roster.record(record(2)).unwrap();
    assert!(roster.begin_close(token(9, 3)).is_err());
    assert!(roster.begin_close(token(1, 0)).is_err());
    let closing = roster.begin_close(token(1, 42)).unwrap();
    assert_eq!(closing.record, record(1));
    assert_eq!(closing.token, token(1, 42));
    let mut visited = Vec::new();
    assert!(
        matches!(roster.visit(|r| visited.push(r)), Err(RosterError::PendingClose(value)) if value == closing)
    );
    assert!(visited.is_empty());
    assert!(
        matches!(roster.record(record(3)), Err(RosterError::PendingClose(value)) if value == closing)
    );
    assert!(
        matches!(roster.begin_close(token(2, 2)), Err(RosterError::PendingClose(value)) if value == closing)
    );
    assert!(roster
        .finish_close(&Reply::Unmounted(token(1, 41)))
        .is_err());
    assert!(roster
        .finish_close(&Reply::Unmounted(token(2, 42)))
        .is_err());
    assert!(roster.finish_close(&Reply::SessionEnded).is_err());
    assert_eq!(roster.pending_close().unwrap(), Some(closing));
    drop(roster);

    let mut roster = Roster::open(&fixture.0, AUTHORITY).unwrap();
    assert_eq!(roster.pending_close().unwrap(), Some(closing));
    assert!(
        matches!(roster.lookup(record(2).workspace), Err(RosterError::PendingClose(value)) if value == closing)
    );
    // In production the caller must independently verify this reply's provenance.
    roster
        .finish_close(&Reply::Unmounted(token(1, 42)))
        .unwrap();
    assert_eq!(roster.pending_close().unwrap(), None);
    let mut active = Vec::new();
    roster.visit(|value| active.push(value)).unwrap();
    assert_eq!(active, vec![record(2)]);
    assert_eq!(roster.lookup(record(1).workspace).unwrap(), None);
    assert!(matches!(
        roster.record(record(1)),
        Err(RosterError::AlreadyExists)
    ));
    assert!(matches!(
        roster.begin_close(token(1, 43)),
        Err(RosterError::AlreadyExists)
    ));
    roster.record(record(3)).unwrap();
    drop(roster);
    for suffix in [".record", ".closing", ".closed"] {
        assert!(marker(&fixture.0, record(1), suffix).is_file());
    }
    let mut roster = Roster::open(&fixture.0, AUTHORITY).unwrap();
    let mut active = Vec::new();
    roster.visit(|value| active.push(value)).unwrap();
    assert_eq!(active, vec![record(2), record(3)]);
}

#[test]
fn process_loss_preserves_intent_without_replaying_unmount() {
    const CHILD: &str = "LAYERFS_CLOSE_LOSS_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let mut roster = Roster::create(&root.join("catalog"), AUTHORITY).unwrap();
        roster.record(record(4)).unwrap();
        roster.begin_close(token(4, 88)).unwrap();
        fs::write(root.join("ack"), b"intent recorded").unwrap();
        std::thread::sleep(Duration::from_secs(30));
        panic!("parent did not terminate owner");
    }
    let fixture = Fixture::new();
    let catalog = fixture.0.join("catalog");
    fs::create_dir(&catalog).unwrap();
    fs::set_permissions(&catalog, fs::Permissions::from_mode(0o700)).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "process_loss_preserves_intent_without_replaying_unmount",
        ])
        .env(CHILD, &fixture.0)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !fixture.0.join("ack").exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("owner exited early: {status}");
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("owner did not acknowledge intent");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    let mut roster = Roster::open(&catalog, AUTHORITY).unwrap();
    let pending = roster.pending_close().unwrap().unwrap();
    assert_eq!(pending.record, record(4));
    assert_eq!(pending.token, token(4, 88));
    assert!(matches!(
        roster.visit(|_| panic!("must not visit")),
        Err(RosterError::PendingClose(_))
    ));
    assert!(!marker(&catalog, record(4), ".closed").exists());
}

#[test]
fn tampered_lifecycle_marker_retains_the_original_catalog() {
    const CHILD: &str = "LAYERFS_CLOSE_CORRUPT_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let mut roster = Roster::create(&root, AUTHORITY).unwrap();
        roster.record(record(5)).unwrap();
        roster.begin_close(token(5, 7)).unwrap();
        roster.finish_close(&Reply::Unmounted(token(5, 7))).unwrap();
        drop(roster);
        let path = marker(&root, record(5), ".closed");
        let mut bytes = fs::read(&path).unwrap();
        bytes[17] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            Roster::open(&root, AUTHORITY),
            Err(RosterError::Retained(_))
        ));
        assert!(Roster::open(&root, AUTHORITY).is_err());
        return;
    }
    let fixture = Fixture::new();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tampered_lifecycle_marker_retains_the_original_catalog",
        ])
        .env(CHILD, &fixture.0)
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    assert!(marker(&fixture.0, record(5), ".closed").is_file());
}

#[test]
fn failed_close_intent_directory_sync_retains_unknown_write_and_lock() {
    const CHILD: &str = "LAYERFS_CLOSE_SYNC_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let catalog = root.join("catalog");
        let mut roster = Roster::create(&catalog, AUTHORITY).unwrap();
        roster.record(record(6)).unwrap();
        fs::write(root.join("inject"), b"1").unwrap();
        assert!(matches!(
            roster.begin_close(token(6, 9)),
            Err(RosterError::Uncertain(_))
        ));
        assert!(matches!(
            roster.record(record(7)),
            Err(RosterError::Retained(_))
        ));
        drop(roster);
        assert!(Roster::open(&catalog, AUTHORITY).is_err());
        return;
    }
    let fixture = Fixture::new();
    let catalog = fixture.0.join("catalog");
    fs::create_dir(&catalog).unwrap();
    fs::set_permissions(&catalog, fs::Permissions::from_mode(0o700)).unwrap();
    let shim = fixture.0.join("failure.so");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../layerfs-overlay/tests/durable_dir_sync_failure.c")
        )
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "failed_close_intent_directory_sync_retains_unknown_write_and_lock",
        ])
        .env(CHILD, &fixture.0)
        .env("LD_PRELOAD", &shim)
        .env(
            "LAYERFS_TEST_DIR_SYNC_TARGET",
            fs::canonicalize(&catalog).unwrap(),
        )
        .env("LAYERFS_TEST_DIR_SYNC_TRIGGER", fixture.0.join("inject"))
        .env(
            "LAYERFS_TEST_DIR_SYNC_RECEIPT",
            fixture.0.join("sync-failed"),
        )
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    assert!(!fs::read(fixture.0.join("sync-failed")).unwrap().is_empty());
    assert!(marker(&catalog, record(6), ".closing").is_file());
    assert!(!marker(&catalog, record(6), ".closed").exists());
    let mut roster = Roster::open(&catalog, AUTHORITY).unwrap();
    assert_eq!(roster.pending_close().unwrap().unwrap().token, token(6, 9));
    assert!(matches!(
        roster.visit(|_| panic!("must not recover")),
        Err(RosterError::PendingClose(_))
    ));
}
