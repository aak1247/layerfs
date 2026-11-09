//! Durable owner registration uses ordinary daemon control without retargeting.
#![cfg(target_os = "linux")]

#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{Reply, Request, UnmountControl, WorkspaceToken};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{KernelConfig, KernelServing, Service},
    store::BindRequest,
    Owner,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
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

fn spec(path: &Path, identity: u8, namespace: i64, root: [u8; 32]) -> DurableWorkspace<'_> {
    DurableWorkspace {
        path,
        profile: ProfileConfig::default(),
        identity: [identity; 32],
        namespace,
        expected_base_root: root,
    }
}

#[test]
fn registration_checks_exact_ownership_and_retains_files_on_unmount() {
    let fixture = support::Fixture::new(1, "durable-registration");
    assert_eq!(fixture.count, 1);
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        16,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let snapshot = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let directory = fixture.directory.join("durable");
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    let first_path = directory.join("first.sqlite");
    let second_path = directory.join("second.sqlite");
    let first = Owner::create_durable(spec(&first_path, 42, 9, snapshot.effective_root.to_bytes()))
        .unwrap();
    let second = Owner::create_durable(spec(
        &second_path,
        43,
        10,
        snapshot.effective_root.to_bytes(),
    ))
    .unwrap();
    let bind = |owner: &Owner, id| {
        store
            .bind_durable(
                owner.client(),
                BindRequest {
                    workspace: WorkspaceId::from_authority([id; 32]).unwrap(),
                    branch: fixture.branch,
                },
                snapshot.clone(),
            )
            .unwrap()
            .workspace
    };
    let first_binding = bind(&first, 42);
    let second_binding = bind(&second, 43);
    let service = Service::new(store.clone(), &first);
    let rejected = service.register_durable(second_binding).unwrap_err();
    assert_eq!(rejected.workspace.snapshot().unwrap(), snapshot);
    assert_eq!(service.workspaces(), Some(0));
    rejected.workspace.close().unwrap();
    drop(rejected);
    second.stop().unwrap();

    let token = service.register_durable(first_binding).unwrap();
    assert_eq!(service.workspaces(), Some(1));
    assert!(matches!(
        service
            .execute_control(&Request::Status(token))
            .unwrap()
            .reply,
        Reply::Status(_)
    ));
    let stale = WorkspaceToken {
        namespace: token.namespace + 1,
        ..token
    };
    assert!(service.execute_control(&Request::Status(stale)).is_err());
    let unmounted = service
        .execute_control(&Request::Unmount(UnmountControl {
            token,
            discard: false,
        }))
        .unwrap();
    assert_eq!(unmounted.reply, Reply::Unmounted(token));
    assert_eq!(service.workspaces(), Some(0));
    drop(service);
    first.stop().unwrap();
    assert!(first_path.is_file() && second_path.is_file());
    drop(store);
    fixture.cleanup();
}

/// The container owns this test mount; always tear it down if an assertion fails.
struct MountGuard(Option<PathBuf>);
impl Drop for MountGuard {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = nix::mount::umount2(path, nix::mount::MntFlags::MNT_DETACH);
        }
    }
}
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
fn kernel(mounts: &Path) -> Arc<KernelServing> {
    Arc::new(
        KernelServing::start(KernelConfig {
            mounts: mounts.to_owned(),
            owner_uid: 0,
            owner_gid: 0,
            command_uid: 65534,
            command_gid: 65534,
            ready_wait: Duration::from_secs(5),
            drain_wait: Duration::from_secs(5),
            control: layerfs_fuse::mount::CONTROL.into(),
        })
        .unwrap(),
    )
}

#[test]
#[ignore = "requires a dedicated Linux container with CAP_SYS_ADMIN and /dev/fuse"]
fn real_fuse_write_survives_daemon_process_loss() {
    const CHILD: &str = "LAYERFS_DURABLE_MOUNT_CHILD";
    const BYTES: &str = "workspace bytes after process loss\n";
    let identity = WorkspaceId::from_authority([42; 32]).unwrap();
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = PathBuf::from(directory);
        let config = layerfs_persistence::PersistenceConfig::sqlite(directory.join("store.sqlite"))
            .with_sqlite_profile(layerfs_persistence::SqlitePersistenceProfile::Disposable)
            .with_sqlite_acquisition(layerfs_persistence::SqliteAcquisitionSchema::Tables);
        let store = open_store(
            config,
            support::BINDING,
            support::CURSOR,
            16,
            2 * 1024 * 1024,
            ReservationBlocks::default(),
        )
        .unwrap();
        let branch = layerfs_history::BranchId::from_authority([24; 16]);
        let snapshot = store.history().branch_snapshot(branch).unwrap().unwrap();
        let path = directory.join("durable/workspace.sqlite");
        let owner =
            Owner::create_durable(spec(&path, 42, 1, snapshot.effective_root.to_bytes())).unwrap();
        let binding = store
            .bind_durable(
                owner.client(),
                BindRequest {
                    workspace: identity,
                    branch,
                },
                snapshot,
            )
            .unwrap()
            .workspace;
        let serving = kernel(&directory.join("mounts"));
        let service = Service::with_kernel(store, &owner, serving);
        let token = service.register_durable(binding).unwrap();
        let Reply::Ready(ready) = service
            .execute_control(&Request::Attach(token))
            .unwrap()
            .reply
        else {
            panic!("no mount ready receipt")
        };
        let file = Path::new(&ready.directory).join("file-000000");
        let status = Command::new("/bin/sh")
            .args([
                "-c",
                "printf '%s\\n' 'workspace bytes after process loss' > \"$1\"",
                "sh",
            ])
            .arg(file)
            .uid(65534)
            .gid(65534)
            .status()
            .unwrap();
        assert!(status.success());
        fs::write(directory.join("ack"), &ready.directory).unwrap();
        std::thread::sleep(Duration::from_secs(60));
        panic!("parent did not fence the old daemon");
    }

    let fixture = support::Fixture::new(1, "durable-fuse-loss");
    assert_eq!(fixture.count, 1);
    let directory = &fixture.directory;
    fs::create_dir(directory.join("durable")).unwrap();
    fs::set_permissions(directory.join("durable"), fs::Permissions::from_mode(0o700)).unwrap();
    fs::create_dir(directory.join("mounts")).unwrap();
    fs::set_permissions(directory.join("mounts"), fs::Permissions::from_mode(0o755)).unwrap();
    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "real_fuse_write_survives_daemon_process_loss",
            "--ignored",
        ])
        .env(CHILD, directory)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);
    let deadline = Instant::now() + Duration::from_secs(15);
    while !directory.join("ack").exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "writer exited before acknowledgement"
        );
        assert!(
            Instant::now() < deadline,
            "writer did not acknowledge before deadline"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let old_path = PathBuf::from(fs::read_to_string(directory.join("ack")).unwrap());
    assert_eq!(old_path, directory.join("mounts/1"));
    let mut old_mount = MountGuard(Some(old_path.clone()));
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    nix::mount::umount2(&old_path, nix::mount::MntFlags::MNT_DETACH).unwrap();
    old_mount.0 = None;
    fs::remove_dir(&old_path).unwrap();

    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        16,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let (owner, snapshot) = Owner::recover_recorded(
        &directory.join("durable/workspace.sqlite"),
        ProfileConfig::default(),
        [42; 32],
        2,
    )
    .unwrap();
    let binding = store
        .bind_durable(
            owner.client(),
            BindRequest {
                workspace: identity,
                branch: fixture.branch,
            },
            snapshot,
        )
        .unwrap()
        .workspace;
    let service = Service::with_kernel(store.clone(), &owner, kernel(&directory.join("mounts")));
    let token = service.register_durable(binding).unwrap();
    let old_token = WorkspaceToken {
        workspace: identity,
        namespace: 1,
    };
    assert!(service
        .execute_control(&Request::Status(old_token))
        .is_err());
    let Reply::Ready(ready) = service
        .execute_control(&Request::Attach(token))
        .unwrap()
        .reply
    else {
        panic!("no recovered mount receipt")
    };
    assert_eq!(Path::new(&ready.directory), directory.join("mounts/2"));
    let mut recovered_mount = MountGuard(Some(PathBuf::from(&ready.directory)));
    let recovered_file = Path::new(&ready.directory).join("file-000000");
    assert_eq!(fs::read_to_string(&recovered_file).unwrap(), BYTES);
    let reader = Command::new("/bin/cat")
        .arg(&recovered_file)
        .uid(65534)
        .gid(65534)
        .output()
        .unwrap();
    assert!(reader.status.success());
    assert_eq!(reader.stdout, BYTES.as_bytes());
    let writer = Command::new("/bin/sh")
        .args(["-c", "printf '%s' 'after recovery' >> \"$1\"", "sh"])
        .arg(&recovered_file)
        .uid(65534)
        .gid(65534)
        .status()
        .unwrap();
    assert!(writer.success());
    assert_eq!(
        fs::read_to_string(recovered_file).unwrap(),
        format!("{BYTES}after recovery")
    );
    let done = service
        .execute_control(&Request::Unmount(UnmountControl {
            token,
            discard: true,
        }))
        .unwrap();
    assert_eq!(done.reply, Reply::Unmounted(token));
    recovered_mount.0 = None;
    drop(service);
    owner.stop().unwrap();
    drop(store);
    fixture.cleanup();
}
