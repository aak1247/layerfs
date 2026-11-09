//! Durable discovery refuses incomplete identities and retains unknown writes.
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
    sandbox: [11; 32],
    store: [12; 32],
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let directory = std::env::temp_dir().join(format!(
            "layerfs-durable-roster-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        Self(directory)
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
fn entry(id: u8) -> WorkspaceRecord {
    WorkspaceRecord {
        workspace: WorkspaceId::from_authority([id; 32]).unwrap(),
        branch: BranchId::from_authority([id; 16]),
    }
}
fn record_path(path: &Path, record: WorkspaceRecord) -> PathBuf {
    path.join(record.overlay_name().replace(".sqlite", ".record"))
}

#[test]
fn records_remain_bound_to_the_original_authority_without_a_count_cap() {
    let fixture = Fixture::new();
    let mut roster = Roster::create(&fixture.0, AUTHORITY).unwrap();
    for id in 1..=64 {
        roster.record(entry(id)).unwrap();
    }
    assert!(matches!(
        roster.record(entry(1)),
        Err(RosterError::AlreadyExists)
    ));
    assert!(matches!(
        roster.record(WorkspaceRecord {
            workspace: entry(1).workspace,
            branch: entry(2).branch
        }),
        Err(RosterError::AlreadyExists)
    ));
    assert!(Roster::open(&fixture.0, AUTHORITY).is_err());
    let mut actual = Vec::new();
    roster
        .visit(|record| actual.push(record.workspace.to_bytes()[0]))
        .unwrap();
    actual.sort_unstable();
    assert_eq!(actual, (1..=64).collect::<Vec<_>>());
    assert_eq!(roster.lookup(entry(17).workspace).unwrap(), Some(entry(17)));
    assert_eq!(roster.lookup(entry(65).workspace).unwrap(), None);
    drop(roster);
    assert!(Roster::open(
        &fixture.0,
        Authority {
            sandbox: [13; 32],
            ..AUTHORITY
        }
    )
    .is_err());
    let mut reopened = Roster::open(&fixture.0, AUTHORITY).unwrap();
    let mut count = 0;
    reopened.visit(|_| count += 1).unwrap();
    assert_eq!(count, 64);
}

#[test]
fn private_empty_directory_is_required() {
    let fixture = Fixture::new();
    fs::set_permissions(&fixture.0, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(Roster::create(&fixture.0, AUTHORITY).is_err());
    fs::set_permissions(&fixture.0, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(fixture.0.join("unrelated"), b"owned evidence").unwrap();
    assert!(Roster::create(&fixture.0, AUTHORITY).is_err());
    assert_eq!(
        fs::read(fixture.0.join("unrelated")).unwrap(),
        b"owned evidence"
    );
}

#[test]
fn acknowledged_records_survive_process_loss() {
    const CHILD: &str = "LAYERFS_ROSTER_LOSS_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let catalog = directory.join("catalog");
        let mut roster = Roster::create(&catalog, AUTHORITY).unwrap();
        roster.record(entry(21)).unwrap();
        fs::write(directory.join("ack"), b"written").unwrap();
        std::thread::sleep(Duration::from_secs(30));
        panic!("parent did not terminate catalog owner");
    }
    let fixture = Fixture::new();
    let catalog = fixture.0.join("catalog");
    fs::create_dir(&catalog).unwrap();
    fs::set_permissions(&catalog, fs::Permissions::from_mode(0o700)).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "acknowledged_records_survive_process_loss"])
        .env(CHILD, &fixture.0)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !fixture.0.join("ack").exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("writer exited early: {status}");
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("writer did not acknowledge");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(Roster::open(&catalog, AUTHORITY).is_err());
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    let mut roster = Roster::open(&catalog, AUTHORITY).unwrap();
    let mut found = Vec::new();
    roster.visit(|record| found.push(record)).unwrap();
    assert_eq!(found, vec![entry(21)]);
}

#[test]
fn corrupted_record_quarantines_the_original_directory_lock() {
    const CHILD: &str = "LAYERFS_ROSTER_CORRUPT_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let mut roster = Roster::create(&directory, AUTHORITY).unwrap();
        roster.record(entry(22)).unwrap();
        drop(roster);
        let path = record_path(&directory, entry(22));
        let mut bytes = fs::read(&path).unwrap();
        bytes[110] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            Roster::open(&directory, AUTHORITY),
            Err(RosterError::Retained(_))
        ));
        assert!(Roster::open(&directory, AUTHORITY).is_err());
        return;
    }
    let fixture = Fixture::new();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "corrupted_record_quarantines_the_original_directory_lock",
        ])
        .env(CHILD, &fixture.0)
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    assert!(record_path(&fixture.0, entry(22)).is_file());
}

#[test]
fn directory_replacement_keeps_the_original_owner_fenced() {
    const CHILD: &str = "LAYERFS_ROSTER_REPLACEMENT_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let moved = directory.with_extension("original");
        let mut roster = Roster::create(&directory, AUTHORITY).unwrap();
        roster.record(entry(25)).unwrap();
        fs::rename(&directory, &moved).unwrap();
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(matches!(
            roster.record(entry(26)),
            Err(RosterError::Retained(_))
        ));
        drop(roster);
        assert!(Roster::open(&moved, AUTHORITY).is_err());
        assert!(!record_path(&directory, entry(26)).exists());
        return;
    }
    let fixture = Fixture::new();
    let directory = fixture.0.join("catalog");
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "directory_replacement_keeps_the_original_owner_fenced",
        ])
        .env(CHILD, &directory)
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    assert!(record_path(&directory.with_extension("original"), entry(25)).is_file());
}

#[test]
fn record_symlink_is_not_followed_or_deleted() {
    const CHILD: &str = "LAYERFS_ROSTER_SYMLINK_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let target = directory.parent().unwrap().join("target");
        let mut roster = Roster::create(&directory, AUTHORITY).unwrap();
        roster.record(entry(27)).unwrap();
        drop(roster);
        let path = record_path(&directory, entry(27));
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert!(matches!(
            Roster::open(&directory, AUTHORITY),
            Err(RosterError::Retained(_))
        ));
        assert_eq!(fs::read(target).unwrap(), b"owned target");
        assert!(fs::symlink_metadata(path).unwrap().file_type().is_symlink());
        return;
    }
    let fixture = Fixture::new();
    let directory = fixture.0.join("catalog");
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(fixture.0.join("target"), b"owned target").unwrap();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "record_symlink_is_not_followed_or_deleted"])
        .env(CHILD, &directory)
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(fs::read(fixture.0.join("target")).unwrap(), b"owned target");
}

#[test]
fn lookup_never_returns_an_early_match_from_an_incomplete_catalog() {
    const CHILD: &str = "LAYERFS_ROSTER_LOOKUP_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let mut roster = Roster::create(&directory, AUTHORITY).unwrap();
        roster.record(entry(33)).unwrap();
        roster.record(entry(34)).unwrap();
        let path = record_path(&directory, entry(34));
        let mut bytes = fs::read(&path).unwrap();
        bytes[110] ^= 1;
        fs::write(path, bytes).unwrap();
        assert!(matches!(
            roster.lookup(entry(33).workspace),
            Err(RosterError::Retained(_))
        ));
        drop(roster);
        assert!(Roster::open(&directory, AUTHORITY).is_err());
        return;
    }
    let fixture = Fixture::new();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "lookup_never_returns_an_early_match_from_an_incomplete_catalog",
        ])
        .env(CHILD, &fixture.0)
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn missing_or_incomplete_membership_never_opens_as_a_complete_roster() {
    const CHILD: &str = "LAYERFS_ROSTER_INCOMPLETE_CHILD";
    const MODE: &str = "LAYERFS_ROSTER_INCOMPLETE_MODE";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let mut roster = Roster::create(&directory, AUTHORITY).unwrap();
        roster.record(entry(28)).unwrap();
        let old_head = fs::read(directory.join("roster.head")).unwrap();
        roster.record(entry(29)).unwrap();
        drop(roster);
        match std::env::var(MODE).unwrap().as_str() {
            "missing" => fs::remove_file(record_path(&directory, entry(28))).unwrap(),
            "truncated" => {
                let file = fs::OpenOptions::new()
                    .write(true)
                    .open(directory.join("roster.ledger"))
                    .unwrap();
                file.set_len(file.metadata().unwrap().len() - 1).unwrap();
            }
            "orphan" => {
                fs::copy(
                    record_path(&directory, entry(28)),
                    record_path(&directory, entry(30)),
                )
                .unwrap();
            }
            "pending" => fs::write(directory.join("roster.head.pending"), b"interrupted").unwrap(),
            "old-head" => fs::write(directory.join("roster.head"), old_head).unwrap(),
            _ => panic!("unknown corruption mode"),
        }
        assert!(matches!(
            Roster::open(&directory, AUTHORITY),
            Err(RosterError::Retained(_))
        ));
        assert!(Roster::open(&directory, AUTHORITY).is_err());
        return;
    }
    let fixture = Fixture::new();
    for mode in ["missing", "truncated", "orphan", "pending", "old-head"] {
        let directory = fixture.0.join(mode);
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "missing_or_incomplete_membership_never_opens_as_a_complete_roster",
            ])
            .env(CHILD, &directory)
            .env(MODE, mode)
            .stdout(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success(), "failed mode {mode}");
    }
}

#[test]
fn failed_directory_sync_retains_unknown_record_and_original_lock() {
    const CHILD: &str = "LAYERFS_ROSTER_SYNC_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let mut roster = Roster::create(&directory, AUTHORITY).unwrap();
        fs::write(directory.join("inject"), b"1").unwrap();
        assert!(matches!(
            roster.record(entry(23)),
            Err(RosterError::Uncertain(_))
        ));
        assert!(matches!(
            roster.record(entry(24)),
            Err(RosterError::Retained(_))
        ));
        drop(roster);
        assert!(Roster::open(&directory, AUTHORITY).is_err());
        return;
    }
    let fixture = Fixture::new();
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
    let catalog = fixture.0.join("catalog");
    fs::create_dir(&catalog).unwrap();
    fs::set_permissions(&catalog, fs::Permissions::from_mode(0o700)).unwrap();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "failed_directory_sync_retains_unknown_record_and_original_lock",
        ])
        .env(CHILD, &catalog)
        .env("LD_PRELOAD", &shim)
        .env(
            "LAYERFS_TEST_DIR_SYNC_TARGET",
            fs::canonicalize(&catalog).unwrap(),
        )
        .env("LAYERFS_TEST_DIR_SYNC_TRIGGER", catalog.join("inject"))
        .env(
            "LAYERFS_TEST_DIR_SYNC_RECEIPT",
            fixture.0.join("sync-failed"),
        )
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    assert!(!fs::read(fixture.0.join("sync-failed")).unwrap().is_empty());
    assert!(record_path(&catalog, entry(23)).is_file());
    assert!(!record_path(&catalog, entry(24)).exists());
    fs::remove_file(catalog.join("inject")).unwrap();
    let mut roster = Roster::open(&catalog, AUTHORITY).unwrap();
    let mut found = Vec::new();
    roster.visit(|record| found.push(record)).unwrap();
    assert_eq!(found, vec![entry(23)]);
}

#[test]
#[ignore = "run only inside the owned VM power-loss harness"]
fn durable_save_survives_vm_power_loss() {
    let directory = PathBuf::from(std::env::var("LAYERFS_DURABLE_VM_ROOT").unwrap());
    match std::env::var("LAYERFS_DURABLE_VM_MODE").unwrap().as_str() {
        "write" => {
            fs::create_dir(&directory).unwrap();
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
            fs::File::open(directory.parent().unwrap())
                .unwrap()
                .sync_all()
                .unwrap();
            let mut roster = Roster::create(&directory, AUTHORITY).unwrap();
            roster.record(entry(31)).unwrap();
            roster.record(entry(32)).unwrap();
            roster.record(entry(33)).unwrap();
            let closed = WorkspaceToken {
                workspace: entry(31).workspace,
                namespace: 31,
            };
            roster.begin_close(closed).unwrap();
            // The guest probe tests the record format with synthetic receipts.
            roster.finish_close(&Reply::Unmounted(closed)).unwrap();
            let pending = WorkspaceToken {
                workspace: entry(33).workspace,
                namespace: 33,
            };
            roster.begin_close(pending).unwrap();
            let size = fs::metadata(record_path(&directory, entry(32)))
                .unwrap()
                .len();
            assert!(size > 0);
            eprintln!("LAYERFS_VM_ACK roster_bytes={size}");
            std::thread::sleep(Duration::from_secs(60));
            panic!("host did not terminate the writer guest");
        }
        "read" => {
            let mut roster = Roster::open(&directory, AUTHORITY).unwrap();
            let pending = roster.pending_close().unwrap().unwrap();
            assert_eq!(pending.record, entry(33));
            assert_eq!(pending.token.namespace, 33);
            assert!(matches!(
                roster.visit(|_| panic!("pending close must block discovery")),
                Err(RosterError::PendingClose(_))
            ));
            // A synthetic test receipt permits inspecting the remaining active set.
            roster
                .finish_close(&Reply::Unmounted(pending.token))
                .unwrap();
            let mut found = Vec::new();
            roster
                .visit(|record| found.push(record.workspace.to_bytes()[0]))
                .unwrap();
            found.sort_unstable();
            assert_eq!(found, vec![32]);
            assert!(matches!(
                roster.record(entry(31)),
                Err(RosterError::AlreadyExists)
            ));
            eprintln!("LAYERFS_VM_RECOVERED");
            std::thread::sleep(Duration::from_secs(60));
            panic!("host did not terminate the reader guest");
        }
        _ => panic!("unknown guest mode"),
    }
}
