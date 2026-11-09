//! Durable ownership through the service lock, independent of mount recovery.
#![cfg(target_os = "linux")]

use layerfs_overlay::{Changes, Inode, InodeKind, OverlayError, PayloadWrite, ProfileConfig};
use layerfs_store_service::{DurableWorkspace, Owner, OwnerClient, OwnerError, ServiceClass};
use layerfs_workspace::Route;
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
const BYTES: &[u8] = b"service-owned durable workspace";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let directory = std::env::temp_dir().join(format!(
            "layerfs-durable-owner-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        Self(directory)
    }
    fn path(&self) -> PathBuf {
        self.0.join("workspace.sqlite")
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
fn spec(path: &Path) -> DurableWorkspace<'_> {
    DurableWorkspace {
        path,
        profile: ProfileConfig::default(),
        identity: IDENTITY,
        namespace: 1,
        expected_base_root: ROOT,
    }
}
fn write(client: &OwnerClient, route: Route) {
    client
        .call(route, ServiceClass::Mutation, None, |db| {
            let source = db.acquire_base_source(1)?;
            let publication = db.apply(
                source,
                &Changes {
                    inodes: vec![Inode {
                        serial: 9,
                        kind: InodeKind::File,
                        mode: 0o644,
                        mtime_seconds: 1,
                        mtime_nanoseconds: 2,
                        nlink: 1,
                        size: BYTES.len() as u64,
                        inherited_cutoff: 0,
                        born: 0,
                        entries: 0,
                        subdirs: 0,
                    }],
                    write: Some(PayloadWrite {
                        serial: 9,
                        offset: 0,
                        data: Arc::from(BYTES),
                    }),
                    ..Changes::default()
                },
            )?;
            db.reply_attempted(publication)?;
            db.release_base_source(source)
        })
        .unwrap();
}
fn read(client: &OwnerClient, route: Route) -> Vec<u8> {
    client
        .call(route, ServiceClass::General, None, |db| {
            let source = db.acquire_base_source(2)?;
            let bytes = db.source_read(source, 9, 0, 1024)?.unwrap().data;
            db.release_base_source(source)?;
            Ok(bytes)
        })
        .unwrap()
}

#[test]
fn durable_owner_validates_identity_root_and_rejects_stale_routes() {
    let fixture = Fixture::new();
    let path = fixture.path();
    let mut invalid = spec(&path);
    invalid.namespace = 0;
    assert!(matches!(
        Owner::create_durable(invalid),
        Err(OwnerError::InvalidAdmission)
    ));
    assert!(!path.exists());

    let owner = Owner::create_durable(spec(&path)).unwrap();
    let client = owner.client();
    let old = client.bound_route().unwrap();
    write(&client, old);
    assert!(Owner::recover_durable(spec(&path)).is_err());
    owner.stop().unwrap();

    let mut wrong = spec(&path);
    wrong.identity = [44; 32];
    assert!(Owner::recover_durable(wrong).is_err());
    let mut wrong = spec(&path);
    wrong.expected_base_root = [18; 32];
    assert!(matches!(
        Owner::recover_durable(wrong),
        Err(OwnerError::Overlay(OverlayError::Invalid(
            "durable Workspace base root"
        )))
    ));
    let recovered = Owner::recover_durable(spec(&path)).unwrap();
    let client = recovered.client();
    let route = client.bound_route().unwrap();
    assert_ne!(old, route);
    assert!(client
        .call(old, ServiceClass::General, None, |_| Ok(()))
        .is_err());
    assert_eq!(read(&client, route), BYTES);
    recovered.stop().unwrap();
    assert!(path.is_file(), "checked stop retains the Workspace");
}

#[test]
fn durable_owner_recovers_service_writes_after_process_loss() {
    const CHILD: &str = "LAYERFS_DURABLE_SERVICE_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let owner = Owner::create_durable(spec(&directory.join("workspace.sqlite"))).unwrap();
        let client = owner.client();
        let route = client.bound_route().unwrap();
        write(&client, route);
        fs::write(directory.join("ack"), format!("{route:?}")).unwrap();
        std::thread::sleep(Duration::from_secs(15));
        panic!("the parent did not kill the writer");
    }
    let fixture = Fixture::new();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable_owner_recovers_service_writes_after_process_loss",
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
    let owner = Owner::recover_durable(spec(&fixture.path())).unwrap();
    let client = owner.client();
    let route = client.bound_route().unwrap();
    assert_ne!(
        fs::read_to_string(fixture.0.join("ack")).unwrap(),
        format!("{route:?}")
    );
    assert_eq!(read(&client, route), BYTES);
    owner.stop().unwrap();
}

#[test]
fn durable_owner_retains_panicked_connection_until_process_exit() {
    const CHILD: &str = "LAYERFS_DURABLE_SERVICE_PANIC_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let path = PathBuf::from(directory).join("workspace.sqlite");
        let owner = Owner::create_durable(spec(&path)).unwrap();
        let client = owner.client();
        let route = client.bound_route().unwrap();
        write(&client, route);
        assert!(client
            .call::<(), _>(route, ServiceClass::General, None, |_| panic!(
                "fixture panic"
            ))
            .is_err());
        assert!(owner.stop().is_err());
        drop(client);
        assert!(
            Owner::recover_durable(spec(&path)).is_err(),
            "the original connection remains owned"
        );
        return;
    }
    let fixture = Fixture::new();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable_owner_retains_panicked_connection_until_process_exit",
        ])
        .env(CHILD, &fixture.0)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    let owner = Owner::recover_durable(spec(&fixture.path())).unwrap();
    let client = owner.client();
    assert_eq!(read(&client, client.bound_route().unwrap()), BYTES);
    owner.stop().unwrap();
}
