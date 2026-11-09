//! A manually assembled multi-owner daemon, fenced and reopened after process loss.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod support;

use layerfs_bridge::control::{Reply, Request, UnmountControl, WorkspaceToken};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{KernelConfig, KernelServing, Service},
    durable_roster::{Authority, Roster, WorkspaceRecord},
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
    let snapshot = store.history().branch_snapshot(branch).unwrap().unwrap();
    let roster = roster(&directory.join("writer-template.sqlite"));
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let mut discovery = Roster::create(&directory.join("catalog"), authority).unwrap();
    let service = Service::with_kernel(store.clone(), &roster, kernel(&directory.join("mounts")));
    for (identity, namespace, text) in ITEMS {
        let workspace_id = WorkspaceId::from_authority([identity; 32]).unwrap();
        let record = WorkspaceRecord {
            workspace: workspace_id,
            branch,
        };
        discovery.record(record).unwrap();
        let path = directory.join("durable").join(record.overlay_name());
        let owner = Owner::create_durable(DurableWorkspace {
            path: &path,
            profile: ProfileConfig::default(),
            identity: [identity; 32],
            namespace,
            expected_base_root: snapshot.effective_root.to_bytes(),
        })
        .unwrap();
        let client = roster.adopt_durable(owner).unwrap();
        let workspace = store
            .bind_durable(
                client,
                BindRequest {
                    workspace: workspace_id,
                    branch,
                },
                snapshot.clone(),
            )
            .unwrap()
            .workspace;
        let token = service
            .register_recorded(&mut discovery, workspace)
            .unwrap();
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
    let mut tokens = Vec::new();
    let mut records = Vec::new();
    discovery.visit(|record| records.push(record)).unwrap();
    records.sort_by_key(|record| record.workspace.to_bytes());
    assert_eq!(records.len(), ITEMS.len());
    let mut minimum_namespace = 3;
    for record in records {
        let workspace_id = record.workspace;
        let identity = workspace_id.to_bytes()[0];
        let (_, old_namespace, expected) = ITEMS
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
        let (owner, snapshot) = Owner::recover_recorded_next(
            &path,
            ProfileConfig::default(),
            [identity; 32],
            minimum_namespace,
        )
        .unwrap();
        let recovered_namespace = owner.client().bound_route().unwrap().namespace();
        assert!(recovered_namespace >= minimum_namespace && recovered_namespace > old_namespace);
        minimum_namespace = recovered_namespace.checked_add(1).unwrap();
        let client = roster.adopt_durable(owner).unwrap();
        let workspace = store
            .bind_durable(
                client,
                BindRequest {
                    workspace: workspace_id,
                    branch: fixture.branch,
                },
                snapshot,
            )
            .unwrap()
            .workspace;
        let token = service
            .register_recorded(&mut discovery, workspace)
            .unwrap();
        assert!(service
            .execute_control(&Request::Status(WorkspaceToken {
                workspace: workspace_id,
                namespace: old_namespace,
            }))
            .is_err());
        let mount = attach(&service, token);
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
        tokens.push(token);
    }
    assert_eq!(service.workspaces(), Some(2));
    for token in tokens {
        let done = service
            .execute_control(&Request::Unmount(UnmountControl {
                token,
                discard: true,
            }))
            .unwrap();
        assert_eq!(done.reply, Reply::Unmounted(token));
    }
    cleanup.0.clear();
    drop(service);
    roster.stop().unwrap();
    drop(discovery);
    drop(store);
    fixture.cleanup();
}
