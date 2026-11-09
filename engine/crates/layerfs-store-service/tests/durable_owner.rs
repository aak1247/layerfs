//! Durable ownership through the service lock, independent of mount recovery.
#![cfg(target_os = "linux")]

use layerfs_overlay::{Changes, Inode, InodeKind, OverlayError, PayloadWrite, ProfileConfig};
use layerfs_store_service::{
    DurableWorkspace, Owner, OwnerClient, OwnerConfig, OwnerError, ServiceClass,
};
use layerfs_workspace::Route;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc,
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
fn recovered_spec(path: &Path) -> DurableWorkspace<'_> {
    let mut value = spec(path);
    value.namespace = 2;
    value
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
fn bounded_roster_adopts_durable_owners_without_reusing_namespaces() {
    let fixture = Fixture::new();
    let roster = Owner::start(
        &fixture.0.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let make = |name: &str, identity: u8, namespace: i64| {
        let path = fixture.0.join(name);
        let mut value = spec(&path);
        value.identity = [identity; 32];
        value.namespace = namespace;
        Owner::create_durable(value).unwrap()
    };
    let first = roster.adopt_durable(make("first.sqlite", 41, 1)).unwrap();
    let first_route = first.bound_route().unwrap();
    write(&first, first_route);

    let rejected = match roster.adopt_durable(make("collision.sqlite", 42, 1)) {
        Ok(_) => panic!("reused namespace was admitted"),
        Err(failure) => failure,
    };
    assert!(matches!(rejected.cause, OwnerError::InvalidAdmission));
    assert!(
        !rejected
            .owner
            .client()
            .state(rejected.owner.client().bound_route().unwrap())
            .unwrap()
            .closed
    );
    rejected.owner.stop().unwrap();

    let second = roster.adopt_durable(make("second.sqlite", 43, 2)).unwrap();
    let second_route = second.bound_route().unwrap();
    write(&second, second_route);
    let rejected = match roster.adopt_durable(make("third.sqlite", 44, 3)) {
        Ok(_) => panic!("full roster admitted another owner"),
        Err(failure) => failure,
    };
    assert!(matches!(rejected.cause, OwnerError::AdmissionFull));
    assert_eq!(read(&second, second_route), BYTES);
    assert!(matches!(
        roster.client().retire_namespace(1),
        Err(OwnerError::InvalidAdmission)
    ));
    first.close(first_route).unwrap();
    roster.client().retire_namespace(1).unwrap();
    assert!(first.state(first_route).is_err());
    assert!(fixture.0.join("first.sqlite").is_file());

    let third = roster.adopt_durable(rejected.owner).unwrap();
    let third_route = third.bound_route().unwrap();
    write(&third, third_route);
    roster.stop().unwrap();
    assert!(second.state(second_route).is_err());
    for (name, identity, namespace) in [("second.sqlite", 43, 4), ("third.sqlite", 44, 5)] {
        let path = fixture.0.join(name);
        let mut value = spec(&path);
        value.identity = [identity; 32];
        value.namespace = namespace;
        let recovered = Owner::recover_durable(value).unwrap();
        assert_eq!(
            read(
                &recovered.client(),
                recovered.client().bound_route().unwrap()
            ),
            BYTES
        );
        recovered.stop().unwrap();
    }
}

#[test]
fn durable_and_disposable_owners_share_capacity_without_route_collision() {
    let fixture = Fixture::new();
    let roster = Owner::start(
        &fixture.0.join("template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let path = fixture.path();
    let mut value = spec(&path);
    value.namespace = 5;
    let durable = roster
        .adopt_durable(Owner::create_durable(value).unwrap())
        .unwrap();
    let durable_route = durable.bound_route().unwrap();
    write(&durable, durable_route);
    let disposable_route = roster.client().open(None, [44; 32], ROOT).unwrap();
    assert_eq!(disposable_route.namespace(), 6);
    assert_eq!(read(&durable, durable_route), BYTES);
    assert!(roster.client().open(None, [45; 32], ROOT).is_err());
    roster.client().close(disposable_route).unwrap();
    roster
        .client()
        .retire_namespace(disposable_route.namespace())
        .unwrap();
    roster.stop().unwrap();
    let mut recovered_spec = spec(&path);
    recovered_spec.namespace = 7;
    let recovered = Owner::recover_durable(recovered_spec).unwrap();
    assert_eq!(
        read(
            &recovered.client(),
            recovered.client().bound_route().unwrap()
        ),
        BYTES
    );
    recovered.stop().unwrap();
}

#[test]
fn stopped_owner_wait_returns_the_unattempted_action() {
    let fixture = Fixture::new();
    let owner = Owner::create_durable(spec(&fixture.path())).unwrap();
    let client = owner.client();
    let route = client.bound_route().unwrap();
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let holder = {
        let client = client.clone();
        std::thread::spawn(move || {
            client.call(route, ServiceClass::General, None, move |_| {
                held_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(())
            })
        })
    };
    held_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let stopped = Arc::new(AtomicBool::new(false));
    let attempted = Arc::new(AtomicBool::new(false));
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = {
        let client = client.clone();
        let stop = stopped.clone();
        let tried = attempted.clone();
        std::thread::spawn(move || {
            let check = Arc::new(move || stop.load(Ordering::Acquire));
            let result = client.call_cancellable(route, check, move |_| {
                tried.store(true, Ordering::Release);
                Ok(())
            });
            done_tx.send(result).unwrap();
        })
    };
    let premature = done_rx.recv_timeout(Duration::from_millis(100)).ok();
    stopped.store(true, Ordering::Release);
    let early = premature.is_some();
    let result = premature.unwrap_or_else(|| done_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    release_tx.send(()).unwrap();
    waiter.join().unwrap();
    holder.join().unwrap().unwrap();
    assert!(!early);
    assert!(
        matches!(result, Err(OwnerError::Unattempted { cause, .. }) if matches!(*cause, OwnerError::Cancelled))
    );
    assert!(!attempted.load(Ordering::Acquire));
    owner.stop().unwrap();
}

#[test]
fn stopping_after_owner_admission_keeps_the_original_result() {
    let fixture = Fixture::new();
    let owner = Owner::create_durable(spec(&fixture.path())).unwrap();
    let client = owner.client();
    let route = client.bound_route().unwrap();
    let stopped = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let worker = {
        let stop = stopped.clone();
        std::thread::spawn(move || {
            client.call_cancellable(
                route,
                Arc::new(move || stop.load(Ordering::Acquire)),
                move |_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(73)
                },
            )
        })
    };
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    stopped.store(true, Ordering::Release);
    release_tx.send(()).unwrap();
    assert_eq!(worker.join().unwrap().unwrap(), 73);
    owner.stop().unwrap();
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
    assert!(matches!(
        Owner::recover_durable(spec(&path)),
        Err(OwnerError::Overlay(OverlayError::Invalid(
            "stale durable namespace"
        )))
    ));
    let recovered = Owner::recover_durable(recovered_spec(&path)).unwrap();
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
    let owner = Owner::recover_durable(recovered_spec(&fixture.path())).unwrap();
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
fn recovered_namespace_is_not_reused_after_process_loss() {
    const CHILD: &str = "LAYERFS_DURABLE_NAMESPACE_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let path = PathBuf::from(&directory).join("workspace.sqlite");
        let first = Owner::create_durable(spec(&path)).unwrap();
        write(&first.client(), first.client().bound_route().unwrap());
        first.stop().unwrap();
        let second = Owner::recover_durable(recovered_spec(&path)).unwrap();
        assert_eq!(second.client().bound_route().unwrap().namespace(), 2);
        fs::write(PathBuf::from(directory).join("namespace-ack"), b"2").unwrap();
        std::thread::sleep(Duration::from_secs(15));
        panic!("the parent did not kill the namespace owner");
    }
    let fixture = Fixture::new();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "recovered_namespace_is_not_reused_after_process_loss",
        ])
        .env(CHILD, &fixture.0)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !fixture.0.join("namespace-ack").exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("owner exited before namespace acknowledgement: {status}");
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("namespace owner did not acknowledge");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert!(matches!(
        Owner::recover_durable(recovered_spec(&fixture.path())),
        Err(OwnerError::Overlay(OverlayError::Invalid(
            "stale durable namespace"
        )))
    ));
    let path = fixture.path();
    let mut next = recovered_spec(&path);
    next.namespace = 3;
    let recovered = Owner::recover_durable(next).unwrap();
    assert_eq!(
        read(
            &recovered.client(),
            recovered.client().bound_route().unwrap()
        ),
        BYTES
    );
    recovered.stop().unwrap();
}

#[test]
fn failed_namespace_sync_retains_ownership_and_does_not_reuse_the_value() {
    const CHILD: &str = "LAYERFS_DURABLE_NAMESPACE_FAILURE_CHILD";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let path = directory.join("workspace.sqlite");
        let owner = Owner::create_durable(spec(&path)).unwrap();
        write(&owner.client(), owner.client().bound_route().unwrap());
        owner.stop().unwrap();
        fs::write(directory.join("inject"), b"1").unwrap();
        assert!(matches!(
            Owner::recover_durable(recovered_spec(&path)),
            Err(OwnerError::Overlay(OverlayError::Uncertain { .. }))
        ));
        assert!(Owner::recover_durable(recovered_spec(&path)).is_err());
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
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "failed_namespace_sync_retains_ownership_and_does_not_reuse_the_value",
        ])
        .env(CHILD, &fixture.0)
        .env("LD_PRELOAD", &shim)
        .env(
            "LAYERFS_TEST_DIR_SYNC_TARGET",
            fs::canonicalize(&fixture.0).unwrap(),
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
    let path = fixture.path();
    let mut next = recovered_spec(&path);
    next.namespace = 3;
    let owner = Owner::recover_durable(next).unwrap();
    assert_eq!(
        read(&owner.client(), owner.client().bound_route().unwrap()),
        BYTES
    );
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
    let owner = Owner::recover_durable(recovered_spec(&fixture.path())).unwrap();
    let client = owner.client();
    assert_eq!(read(&client, client.bound_route().unwrap()), BYTES);
    owner.stop().unwrap();
}
