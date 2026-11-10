#![cfg(target_os = "linux")]
//! Private record publication, path admission and pinned-directory identity.

use layerfs_sandbox::durable_records::{Directory, Lease};
use std::{
    ffi::OsStr,
    fs,
    os::unix::{ffi::OsStrExt, fs::PermissionsExt},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "layerfs-private-record-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
}

#[test]
fn lease_serializes_the_original_sidecar_across_processes() {
    let fixture = Fixture::new();
    let mut lease = Lease::try_acquire(Directory::open(&fixture.0).unwrap()).unwrap();
    let probe = |expect: &str| {
        let result = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "lease_child_probe", "--nocapture"])
            .env("LAYERFS_RECORD_LEASE_CHILD", &fixture.0)
            .env("LAYERFS_RECORD_LEASE_EXPECT", expect)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(String::from_utf8_lossy(&result.stdout).contains("1 passed"));
    };
    probe("busy");
    assert!(
        !lease
            .create(OsStr::new("ownership.lock"), b"replace")
            .unwrap_err()
            .attempted
    );
    assert!(
        !lease
            .save(
                OsStr::new("intent"),
                OsStr::new("ownership.lock"),
                b"replace"
            )
            .unwrap_err()
            .attempted
    );
    lease.create(OsStr::new("intent"), b"request").unwrap();
    lease
        .save(
            OsStr::new("intent"),
            OsStr::new("temporary"),
            b"acknowledged",
        )
        .unwrap();
    lease.remove(OsStr::new("intent")).unwrap();
    drop(lease);
    probe("available");
}

#[test]
fn lease_child_probe() {
    let Some(path) = std::env::var_os("LAYERFS_RECORD_LEASE_CHILD") else {
        return;
    };
    let expected = std::env::var("LAYERFS_RECORD_LEASE_EXPECT").unwrap();
    let result = Lease::try_acquire(Directory::open(&PathBuf::from(path)).unwrap());
    match (expected.as_str(), result) {
        ("busy", Err(error)) => assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock),
        ("available", Ok(lease)) => lease.verify().unwrap(),
        (expected, _) => panic!("unexpected lease outcome for {expected}"),
    }
}

#[test]
fn lease_refuses_a_replaced_sidecar_before_writing() {
    let fixture = Fixture::new();
    let mut lease = Lease::try_acquire(Directory::open(&fixture.0).unwrap()).unwrap();
    fs::rename(fixture.0.join("ownership.lock"), fixture.0.join("old-lock")).unwrap();
    fs::write(fixture.0.join("ownership.lock"), b"replacement").unwrap();
    fs::set_permissions(
        fixture.0.join("ownership.lock"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    assert!(lease.verify().is_err());
    assert!(
        !lease
            .create(OsStr::new("intent"), b"request")
            .unwrap_err()
            .attempted
    );
    assert!(!fixture.0.join("intent").exists());
}

#[test]
fn killed_lease_owner_leaves_original_intent_in_place() {
    let fixture = Fixture::new();
    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "lease_child_holds_until_killed", "--nocapture"])
            .env("LAYERFS_RECORD_LEASE_CRASH_CHILD", &fixture.0)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_for(fixture.0.join("child-ready"));
    match Lease::try_acquire(Directory::open(&fixture.0).unwrap()) {
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock),
        Ok(_) => panic!("another process acquired the active lease"),
    }
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let mut next = Lease::try_acquire(Directory::open(&fixture.0).unwrap()).unwrap();
    assert_eq!(
        fs::read(fixture.0.join("pending")).unwrap(),
        b"original intent"
    );
    assert!(next.create(OsStr::new("pending"), b"replayed").is_err());
    assert_eq!(
        fs::read(fixture.0.join("pending")).unwrap(),
        b"original intent"
    );
}

#[test]
fn lease_child_holds_until_killed() {
    let Some(path) = std::env::var_os("LAYERFS_RECORD_LEASE_CRASH_CHILD") else {
        return;
    };
    let path = PathBuf::from(path);
    let mut lease = Lease::try_acquire(Directory::open(&path).unwrap()).unwrap();
    lease
        .create(OsStr::new("pending"), b"original intent")
        .unwrap();
    fs::write(path.join("child-ready"), b"ready").unwrap();
    wait_for(path.join("release"));
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn publishes_and_removes_private_records() {
    let fixture = Fixture::new();
    let directory = Directory::open(&fixture.0).unwrap();
    directory.create(OsStr::new("intent"), b"original").unwrap();
    assert_eq!(fs::read(fixture.0.join("intent")).unwrap(), b"original");
    assert_eq!(
        fs::metadata(fixture.0.join("intent"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(directory
        .create(OsStr::new("intent"), b"duplicate")
        .is_err());
    directory
        .save(OsStr::new("intent"), OsStr::new("temporary"), b"ack")
        .unwrap();
    assert_eq!(fs::read(fixture.0.join("intent")).unwrap(), b"ack");
    assert!(!fixture.0.join("temporary").exists());
    directory.remove(OsStr::new("intent")).unwrap();
    directory.sync().unwrap();
    assert!(!fixture.0.join("intent").exists());
}

#[test]
fn refuses_nonlocal_record_names_before_any_effect() {
    let fixture = Fixture::new();
    let directory = Directory::open(&fixture.0).unwrap();
    for name in ["", ".", "..", "../other", "a/b", "/tmp/outside"] {
        let failure = directory.create(OsStr::new(name), b"value").unwrap_err();
        assert!(
            !failure.attempted,
            "name {name:?} must be refused before an effect"
        );
        assert!(directory.remove(OsStr::new(name)).is_err());
    }
    let nul = OsStr::from_bytes(b"record\0suffix");
    assert!(!directory.create(nul, b"value").unwrap_err().attempted);
    assert!(
        !directory
            .save(OsStr::new("same"), OsStr::new("same"), b"value")
            .unwrap_err()
            .attempted
    );
    assert!(
        !directory
            .save(OsStr::new("valid"), OsStr::new("../outside"), b"value")
            .unwrap_err()
            .attempted
    );
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
}

#[test]
fn refuses_replaced_directory_without_writing_into_it() {
    let fixture = Fixture::new();
    let original = fixture.0.join("records");
    let displaced = fixture.0.join("original");
    fs::create_dir(&original).unwrap();
    fs::set_permissions(&original, fs::Permissions::from_mode(0o700)).unwrap();
    let directory = Directory::open(&original).unwrap();
    fs::rename(&original, &displaced).unwrap();
    fs::create_dir(&original).unwrap();
    fs::set_permissions(&original, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        !directory
            .create(OsStr::new("intent"), b"value")
            .unwrap_err()
            .attempted
    );
    assert!(!original.join("intent").exists());
    assert!(!displaced.join("intent").exists());
}

#[test]
fn refuses_shared_or_symlinked_records_and_nonprivate_directories() {
    let fixture = Fixture::new();
    let directory = Directory::open(&fixture.0).unwrap();
    directory.create(OsStr::new("shared"), b"original").unwrap();
    fs::hard_link(fixture.0.join("shared"), fixture.0.join("alias")).unwrap();
    let error = directory
        .save(OsStr::new("shared"), OsStr::new("temporary"), b"changed")
        .unwrap_err();
    assert!(!error.attempted);
    assert!(directory.remove(OsStr::new("shared")).is_err());
    assert_eq!(fs::read(fixture.0.join("alias")).unwrap(), b"original");
    fs::write(fixture.0.join("outside"), b"outside").unwrap();
    std::os::unix::fs::symlink("outside", fixture.0.join("link")).unwrap();
    assert!(
        !directory
            .save(OsStr::new("link"), OsStr::new("temporary"), b"changed")
            .unwrap_err()
            .attempted
    );
    assert!(directory.remove(OsStr::new("link")).is_err());
    assert_eq!(fs::read(fixture.0.join("outside")).unwrap(), b"outside");
    fs::set_permissions(&fixture.0, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(directory.verify().is_err());
    assert!(Directory::open(&fixture.0).is_err());
}

#[test]
#[ignore = "requires an isolated caller-owned bind mount and external mount controller"]
fn refuses_same_inode_overmount_until_original_mount_returns() {
    let records = PathBuf::from(std::env::var_os("LAYERFS_RECORD_MOUNT_ROOT").unwrap());
    let control = PathBuf::from(std::env::var_os("LAYERFS_RECORD_MOUNT_CONTROL").unwrap());
    let directory = Directory::open(&records).unwrap();
    fs::write(control.join("ready"), b"ready").unwrap();
    wait_for(control.join("changed"));
    assert!(directory.verify().is_err());
    assert!(
        !directory
            .create(OsStr::new("intent"), b"value")
            .unwrap_err()
            .attempted
    );
    fs::write(control.join("refused"), b"refused").unwrap();
    wait_for(control.join("restored"));
    directory.verify().unwrap();
    directory
        .create(OsStr::new("intent"), b"original mount")
        .unwrap();
    directory.remove(OsStr::new("intent")).unwrap();
}

#[test]
#[ignore = "requires an isolated caller-owned bind mount and external mount controller"]
fn refuses_record_overmount_until_original_mount_returns() {
    let records = PathBuf::from(std::env::var_os("LAYERFS_RECORD_MOUNT_ROOT").unwrap());
    let control = PathBuf::from(std::env::var_os("LAYERFS_RECORD_MOUNT_CONTROL").unwrap());
    let directory = Directory::open(&records).unwrap();
    directory.create(OsStr::new("intent"), b"original").unwrap();
    fs::write(control.join("ready"), b"ready").unwrap();
    wait_for(control.join("changed"));
    assert!(
        !directory
            .save(
                OsStr::new("intent"),
                OsStr::new("temporary"),
                b"replacement"
            )
            .unwrap_err()
            .attempted
    );
    assert!(directory.remove(OsStr::new("intent")).is_err());
    fs::write(control.join("refused"), b"refused").unwrap();
    wait_for(control.join("restored"));
    assert_eq!(fs::read(records.join("intent")).unwrap(), b"original");
    directory
        .save(
            OsStr::new("intent"),
            OsStr::new("temporary"),
            b"replacement",
        )
        .unwrap();
    directory.remove(OsStr::new("intent")).unwrap();
}

fn wait_for(path: PathBuf) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "proof controller did not create {path:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
