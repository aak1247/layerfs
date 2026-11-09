//! A manually assembled multi-owner daemon, fenced and reopened after process loss.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{
    Activity, ControlCode, Reply, Request, UnmountControl, WorkspaceToken,
};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{
        Failure, KernelConfig, KernelServing, RecordedCloseFailure, RecordedMountRequest, Service,
    },
    durable_roster::{Authority, Roster, RosterError, WorkspaceRecord},
    store::BindRequest,
    Owner, OwnerConfig,
};
use layerfs_history::{BranchId, WorkspaceId};
use layerfs_overlay::ProfileConfig;
use layerfs_persistence::{PersistenceConfig, SqliteAcquisitionSchema, SqlitePersistenceProfile};
use layerfs_storage::ReservationBlocks;
use layerfs_store_service::DurableWorkspace;
use std::{
    fs,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

const CHILD: &str = "LAYERFS_DURABLE_MULTI_CHILD";
const BUSY_CHILD: &str = "LAYERFS_DURABLE_CLOSE_BUSY_CHILD";
const CANCEL_FAIL: &str = "LAYERFS_DURABLE_CLOSE_CANCEL_FAIL";
const ITEMS: [(u8, i64, &str); 2] = [(51, 1, "first"), (52, 2, "second")];

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
struct Mounts(Vec<PathBuf>);
impl Drop for Mounts {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = nix::mount::umount2(path, nix::mount::MntFlags::MNT_DETACH);
        }
    }
}

fn kernel(path: &Path) -> Arc<KernelServing> {
    Arc::new(
        KernelServing::start(KernelConfig {
            mounts: path.to_owned(),
            owner_uid: 0,
            owner_gid: 0,
            command_uid: 65534,
            command_gid: 65534,
            ready_wait: Duration::from_secs(10),
            drain_wait: Duration::from_secs(10),
            control: layerfs_fuse::mount::CONTROL.into(),
        })
        .unwrap(),
    )
}

fn roster(path: &Path) -> Owner {
    Owner::start(
        path,
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap()
}

fn open(config: PersistenceConfig) -> Arc<layerfs_daemon::store::Store> {
    open_store(
        config,
        support::BINDING,
        support::CURSOR,
        2,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap()
}

fn attach(service: &Service, token: WorkspaceToken) -> PathBuf {
    let Reply::Ready(ready) = service
        .execute_control(&Request::Attach(token))
        .unwrap()
        .reply
    else {
        panic!("missing mount receipt")
    };
    PathBuf::from(ready.directory)
}

fn child(directory: &Path) {
    let config = PersistenceConfig::sqlite(directory.join("store-private/store.sqlite"))
        .with_sqlite_profile(SqlitePersistenceProfile::Durable)
        .with_sqlite_acquisition(SqliteAcquisitionSchema::Tables);
    let store = open(config);
    let branch = BranchId::from_authority([24; 16]);
    let roster = roster(&directory.join("writer-template.sqlite"));
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::create(&directory.join("catalog"), authority).unwrap();
    let service = Service::with_kernel(store.clone(), &roster, kernel(&directory.join("mounts")));
    for (identity, namespace, text) in ITEMS {
        let workspace_id = WorkspaceId::from_authority([identity; 32]).unwrap();
        let reply = service
            .mount_recorded(
                &mut discovery,
                &roster,
                &directory.join("durable"),
                RecordedMountRequest {
                    sandbox: [8; 32],
                    profile: ProfileConfig::default(),
                    workspace: workspace_id,
                    branch,
                },
            )
            .unwrap()
            .reply;
        let Reply::Bound { token, .. } = reply else {
            panic!("missing binding receipt")
        };
        assert_eq!(token.namespace, namespace);
        let mount = attach(&service, token);
        let file = mount.join("file-000000");
        let status = Command::new("/bin/sh")
            .args(["-c", "printf '%s' \"$2\" > \"$1\"", "sh"])
            .arg(&file)
            .arg(text)
            .uid(65534)
            .gid(65534)
            .status()
            .unwrap();
        assert!(status.success());
        fs::File::open(&file).unwrap().sync_all().unwrap();
        fs::File::open(mount).unwrap().sync_all().unwrap();
    }
    fs::write(directory.join("ack"), b"both mounted and synchronized").unwrap();
    std::thread::sleep(Duration::from_secs(90));
    panic!("parent did not terminate the old owner");
}

fn busy_child(directory: &Path) {
    let config = PersistenceConfig::sqlite(directory.join("store-private/store.sqlite"))
        .with_sqlite_profile(SqlitePersistenceProfile::Durable)
        .with_sqlite_acquisition(SqliteAcquisitionSchema::Tables);
    let store = open(config);
    let branch = BranchId::from_authority([24; 16]);
    let snapshot = store.history().branch_snapshot(branch).unwrap().unwrap();
    let roster = roster(&directory.join("busy-template.sqlite"));
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::create(&directory.join("catalog"), authority).unwrap();
    let service = Service::with_kernel(store.clone(), &roster, kernel(&directory.join("mounts")));
    let workspace = WorkspaceId::from_authority([53; 32]).unwrap();
    let record = WorkspaceRecord { workspace, branch };
    discovery.record(record).unwrap();
    let durable = Owner::create_durable(DurableWorkspace {
        path: &directory.join("durable").join(record.overlay_name()),
        profile: ProfileConfig::default(),
        identity: [53; 32],
        namespace: 1,
        expected_base_root: snapshot.effective_root.to_bytes(),
    })
    .unwrap();
    let client = roster.adopt_durable(durable).unwrap();
    let binding = store
        .bind_durable(client, BindRequest { workspace, branch }, snapshot)
        .unwrap()
        .workspace;
    let token = service.register_recorded(&mut discovery, binding).unwrap();
    let mount = attach(&service, token);
    let held = fs::File::open(mount.join("file-000000")).unwrap();
    if std::env::var_os(CANCEL_FAIL).is_some() {
        let failure = service
            .unmount_recorded(
                &mut discovery,
                UnmountControl {
                    token,
                    discard: true,
                },
            )
            .unwrap_err();
        assert!(
            matches!(failure, RecordedCloseFailure::Cancellation { cause: RosterError::Uncertain(_), original }
            if matches!(*original, Failure::Kernel(ref cause) if cause.code == ControlCode::Busy && cause.phase == "unmount:kernel"))
        );
        let Reply::Status(status) = service
            .execute_control(&Request::Status(token))
            .unwrap()
            .reply
        else {
            panic!("missing status")
        };
        assert_eq!(status.activity, Activity::Closing);
        assert!(service.operation(token).is_err());
        assert!(matches!(
            discovery.pending_close(),
            Err(RosterError::Retained(_))
        ));
        drop(held);
        fs::write(
            directory.join("busy-ack"),
            b"cancellation sync uncertain; original Busy retained",
        )
        .unwrap();
        std::thread::sleep(Duration::from_secs(90));
        panic!("parent did not terminate the old owner");
    }
    for _ in 0..2 {
        let failure = service
            .unmount_recorded(
                &mut discovery,
                UnmountControl {
                    token,
                    discard: true,
                },
            )
            .unwrap_err();
        assert!(
            matches!(failure, RecordedCloseFailure::Refused(Failure::Kernel(ref cause))
            if cause.code == ControlCode::Busy && cause.phase == "unmount:kernel")
        );
        assert_eq!(discovery.pending_close().unwrap(), None);
        assert_eq!(discovery.lookup(workspace).unwrap(), Some(record));
    }
    let Reply::Status(status) = service
        .execute_control(&Request::Status(token))
        .unwrap()
        .reply
    else {
        panic!("missing status")
    };
    assert_eq!(status.activity, Activity::Idle);
    drop(service.operation(token).unwrap());
    let cancellations = fs::read_dir(directory.join("catalog"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains(".cancelled-"))
        .count();
    assert_eq!(cancellations, 2);
    drop(held);
    let done = service
        .unmount_recorded(
            &mut discovery,
            UnmountControl {
                token,
                discard: true,
            },
        )
        .unwrap();
    assert_eq!(done.reply, Reply::Unmounted(token));
    fs::write(
        directory.join("busy-ack"),
        b"busy refusals cancelled and explicit close completed",
    )
    .unwrap();
    std::thread::sleep(Duration::from_secs(90));
    panic!("parent did not terminate the old owner");
}

#[test]
#[ignore = "requires a dedicated Linux container with CAP_SYS_ADMIN and /dev/fuse"]
fn recorded_busy_close_allows_a_later_explicit_close() {
    if let Some(directory) = std::env::var_os(BUSY_CHILD) {
        busy_child(Path::new(&directory));
        return;
    }
    let fixture = support::Fixture::new_durable(1, "durable-recorded-busy");
    let directory = &fixture.directory;
    for name in ["durable", "catalog", "mounts"] {
        fs::create_dir(directory.join(name)).unwrap();
        fs::set_permissions(
            directory.join(name),
            fs::Permissions::from_mode(if name == "mounts" { 0o755 } else { 0o700 }),
        )
        .unwrap();
    }
    let mount = directory.join("mounts/1");
    let mut cleanup = Mounts(vec![mount.clone()]);
    let mut old = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "recorded_busy_close_allows_a_later_explicit_close",
                "--ignored",
            ])
            .env(BUSY_CHILD, directory)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(40);
    while !directory.join("busy-ack").exists() {
        assert!(old.0.try_wait().unwrap().is_none(), "writer exited early");
        assert!(Instant::now() < deadline, "writer did not acknowledge");
        std::thread::sleep(Duration::from_millis(10));
    }
    old.0.kill().unwrap();
    assert!(!old.0.wait().unwrap().success());
    cleanup.0.clear();
    let store = open(fixture.config.clone());
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::open(&directory.join("catalog"), authority).unwrap();
    assert_eq!(discovery.pending_close().unwrap(), None);
    let workspace = WorkspaceId::from_authority([53; 32]).unwrap();
    assert_eq!(discovery.lookup(workspace).unwrap(), None);
    let roster = roster(&directory.join("busy-reader.sqlite"));
    let service = Service::new(store.clone(), &roster);
    let recovered = service
        .recover_catalog(
            &mut discovery,
            &roster,
            &directory.join("durable"),
            [8; 32],
            ProfileConfig::default(),
            2,
        )
        .unwrap();
    assert!(recovered.is_empty());
    drop(service);
    roster.stop().unwrap();
    drop(discovery);
    drop(store);
    fixture.cleanup();
}

#[test]
#[ignore = "requires a dedicated FUSE container and LAYERFS_TEST_SYNC_SHIM"]
fn recorded_busy_cancellation_sync_failure_retains_the_original_refusal() {
    if let Some(directory) = std::env::var_os(BUSY_CHILD) {
        busy_child(Path::new(&directory));
        return;
    }
    let shim = std::env::var_os("LAYERFS_TEST_SYNC_SHIM").expect("test-only sync shim");
    let fixture = support::Fixture::new_durable(1, "durable-cancel-sync-failure");
    let directory = &fixture.directory;
    for name in ["durable", "catalog", "mounts"] {
        fs::create_dir(directory.join(name)).unwrap();
        fs::set_permissions(
            directory.join(name),
            fs::Permissions::from_mode(if name == "mounts" { 0o755 } else { 0o700 }),
        )
        .unwrap();
    }
    let mount = directory.join("mounts/1");
    let mut cleanup = Mounts(vec![mount.clone()]);
    let workspace = WorkspaceId::from_authority([53; 32]).unwrap();
    let record = WorkspaceRecord {
        workspace,
        branch: fixture.branch,
    };
    let trigger = directory.join("catalog").join(
        record
            .overlay_name()
            .replace(".sqlite", ".cancelled-0000000000000002"),
    );
    let receipt = directory.join("sync-failed");
    let mut old = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "recorded_busy_cancellation_sync_failure_retains_the_original_refusal",
                "--ignored",
            ])
            .env(BUSY_CHILD, directory)
            .env(CANCEL_FAIL, "1")
            .env("LD_PRELOAD", shim)
            .env(
                "LAYERFS_TEST_DIR_SYNC_TARGET",
                fs::canonicalize(directory.join("catalog")).unwrap(),
            )
            .env("LAYERFS_TEST_DIR_SYNC_TRIGGER", trigger)
            .env("LAYERFS_TEST_DIR_SYNC_RECEIPT", &receipt)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(40);
    while !directory.join("busy-ack").exists() {
        assert!(old.0.try_wait().unwrap().is_none(), "writer exited early");
        assert!(Instant::now() < deadline, "writer did not acknowledge");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!fs::read(&receipt).unwrap().is_empty());
    old.0.kill().unwrap();
    assert!(!old.0.wait().unwrap().success());
    nix::mount::umount2(&mount, nix::mount::MntFlags::MNT_DETACH).unwrap();
    fs::remove_dir(&mount).unwrap();
    cleanup.0.clear();
    let store = open(fixture.config.clone());
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::open(&directory.join("catalog"), authority).unwrap();
    assert_eq!(discovery.pending_close().unwrap(), None);
    assert_eq!(discovery.lookup(workspace).unwrap(), Some(record));
    let roster = roster(&directory.join("cancel-reader.sqlite"));
    let service = Service::new(store.clone(), &roster);
    let recovered = service
        .recover_catalog(
            &mut discovery,
            &roster,
            &directory.join("durable"),
            [8; 32],
            ProfileConfig::default(),
            2,
        )
        .unwrap();
    assert_eq!(recovered.len(), 1);
    let done = service
        .unmount_recorded(
            &mut discovery,
            UnmountControl {
                token: recovered[0],
                discard: true,
            },
        )
        .unwrap();
    assert_eq!(done.reply, Reply::Unmounted(recovered[0]));
    drop(service);
    roster.stop().unwrap();
    drop(discovery);
    drop(store);
    fixture.cleanup();
}

#[test]
#[ignore = "requires a dedicated Linux container with CAP_SYS_ADMIN and /dev/fuse"]
fn two_durable_mounts_recover_independently_after_process_loss() {
    if let Some(directory) = std::env::var_os(CHILD) {
        child(Path::new(&directory));
        return;
    }
    let fixture = support::Fixture::new_durable(1, "durable-multi-fuse");
    let directory = &fixture.directory;
    fs::create_dir(directory.join("durable")).unwrap();
    fs::set_permissions(directory.join("durable"), fs::Permissions::from_mode(0o700)).unwrap();
    fs::create_dir(directory.join("catalog")).unwrap();
    fs::set_permissions(directory.join("catalog"), fs::Permissions::from_mode(0o700)).unwrap();
    fs::create_dir(directory.join("mounts")).unwrap();
    fs::set_permissions(directory.join("mounts"), fs::Permissions::from_mode(0o755)).unwrap();
    let mounts = directory.join("mounts");
    let mut cleanup = Mounts((1..=4).map(|n| mounts.join(n.to_string())).collect());
    let mut old = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "two_durable_mounts_recover_independently_after_process_loss",
                "--ignored",
            ])
            .env(CHILD, directory)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(40);
    while !directory.join("ack").exists() {
        assert!(old.0.try_wait().unwrap().is_none(), "writer exited early");
        assert!(Instant::now() < deadline, "writer did not acknowledge");
        std::thread::sleep(Duration::from_millis(10));
    }
    old.0.kill().unwrap();
    assert!(!old.0.wait().unwrap().success());
    for namespace in 1..=2 {
        let path = mounts.join(namespace.to_string());
        nix::mount::umount2(&path, nix::mount::MntFlags::MNT_DETACH).unwrap();
        fs::remove_dir(path).unwrap();
    }

    let store = open(fixture.config.clone());
    let roster = roster(&directory.join("reader-template.sqlite"));
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::open(&directory.join("catalog"), authority).unwrap();
    let service = Service::with_kernel(store.clone(), &roster, kernel(&mounts));
    let mut records = Vec::new();
    discovery.visit(|record| records.push(record)).unwrap();
    records.sort_by_key(|record| record.workspace.to_bytes());
    assert_eq!(records.len(), ITEMS.len());
    for record in &records {
        let workspace_id = record.workspace;
        let identity = workspace_id.to_bytes()[0];
        let (_, old_namespace, _) = ITEMS
            .iter()
            .copied()
            .find(|item| item.0 == identity)
            .unwrap();
        assert_eq!(record.branch, fixture.branch);
        let path = directory.join("durable").join(record.overlay_name());
        assert!(Owner::recover_recorded(
            &path,
            ProfileConfig::default(),
            [identity; 32],
            old_namespace,
        )
        .is_err());
    }
    let tokens = service
        .recover_catalog(
            &mut discovery,
            &roster,
            &directory.join("durable"),
            [8; 32],
            ProfileConfig::default(),
            3,
        )
        .unwrap();
    assert_eq!(tokens.len(), ITEMS.len());
    let mut minimum_namespace = 3;
    for token in &tokens {
        let workspace_id = token.workspace;
        let identity = workspace_id.to_bytes()[0];
        let (_, old_namespace, expected) = ITEMS
            .iter()
            .copied()
            .find(|item| item.0 == identity)
            .unwrap();
        assert!(token.namespace >= minimum_namespace && token.namespace > old_namespace);
        minimum_namespace = token.namespace.checked_add(1).unwrap();
        assert!(service
            .execute_control(&Request::Status(WorkspaceToken {
                workspace: workspace_id,
                namespace: old_namespace,
            }))
            .is_err());
        let mount = attach(&service, *token);
        let file = mount.join("file-000000");
        let output = Command::new("/bin/cat")
            .arg(&file)
            .uid(65534)
            .gid(65534)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, expected.as_bytes());
        fs::File::open(file).unwrap().sync_all().unwrap();
        fs::File::open(mount).unwrap().sync_all().unwrap();
    }
    assert_eq!(service.workspaces(), Some(2));
    for token in tokens {
        let refused = service
            .unmount_recorded(
                &mut discovery,
                UnmountControl {
                    token,
                    discard: false,
                },
            )
            .unwrap();
        assert!(matches!(refused.reply, Reply::Dirty(_)));
        assert_eq!(discovery.pending_close().unwrap(), None);
        let done = service
            .unmount_recorded(
                &mut discovery,
                UnmountControl {
                    token,
                    discard: true,
                },
            )
            .unwrap();
        assert_eq!(done.reply, Reply::Unmounted(token));
    }
    cleanup.0.clear();
    drop(service);
    roster.stop().unwrap();
    drop(discovery);
    let mut reopened = Roster::open(&directory.join("catalog"), authority).unwrap();
    let mut active = Vec::new();
    reopened.visit(|record| active.push(record)).unwrap();
    assert!(active.is_empty());
    drop(reopened);
    drop(store);
    fixture.cleanup();
}
