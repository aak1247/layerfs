//! Actual daemon startup uses a new durable catalog and never silently resumes it.
#![cfg(target_os = "linux")]

#[path = "support/daemon_binary.rs"]
mod daemon_binary;
#[allow(dead_code)]
#[path = "../../layerfs-daemon/tests/support/host_install.rs"]
mod support;

use layerfs_api::internal::Control;
use layerfs_bridge::{
    control::{DaemonPhase, HelloRequest, RefusalKind, Reply, Request, UnmountControl},
    daemon_setup::{DaemonLimits, DaemonSetup, FreshWorkspaceSetup},
    duplex,
};
use layerfs_daemon::{
    bootstrap::open_store,
    control::{DurableControlConfig, KernelConfig, KernelServing, Service},
    durable_roster::{Authority, Roster},
    Owner, OwnerConfig,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
use layerfs_persistence::{PersistenceConfig, SqlitePersistenceProfile};
use layerfs_project::store::InstallError;
use layerfs_storage::ReservationBlocks;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    net::{SocketAddr, TcpStream},
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant},
};

struct Process(Child);
impl Drop for Process {
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

fn setup(f: &support::Fixture, label: &str) -> (DaemonSetup, PathBuf) {
    fs::set_permissions(&f.directory, fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["catalog", "durable"] {
        let path = f.directory.join(name);
        fs::create_dir(&path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let mounts = std::env::temp_dir().join(format!(
        "layerfs-fresh-daemon-{label}-{}",
        std::process::id()
    ));
    let setup = DaemonSetup {
        listen: "127.0.0.1:0".into(),
        private_key: support::SERVER_PRIVATE,
        control_peer: duplex::public_key(&support::CLIENT_PRIVATE).unwrap(),
        store: f.project.manifest.locator.clone(),
        overlay: f.directory.join("template.sqlite").to_str().unwrap().into(),
        mounts: mounts.to_str().unwrap().into(),
        command_uid: 65534,
        command_gid: 65534,
        limits: DaemonLimits {
            connections: 4,
            handshake_ms: 3000,
            cache_bytes: 2 * 1024 * 1024,
            namespaces: 2,
            pager_kib: 1024,
            serial_low_water: 0,
            idle_exit_s: 0,
        },
        existing_store: None,
        fresh_workspace: Some(FreshWorkspaceSetup {
            sandbox: [8; 32],
            catalog_directory: f.directory.join("catalog").to_str().unwrap().into(),
            overlay_directory: f.directory.join("durable").to_str().unwrap().into(),
            create_directories: false,
        }),
        host_export: None,
    };
    (setup, mounts)
}

fn config(f: &support::Fixture, setup: &DaemonSetup, name: &str) -> PathBuf {
    let path = f.directory.join(name);
    fs::write(&path, setup.encode().unwrap()).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    path
}

fn start(f: &support::Fixture, setup: &DaemonSetup) -> (Process, SocketAddr) {
    let path = config(f, setup, "daemon.config");
    let mut process = Process(
        Command::new(daemon_binary::path())
            .arg("--config")
            .arg(path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(
                File::create(f.directory.join("daemon.stderr")).unwrap(),
            ))
            .spawn()
            .unwrap(),
    );
    let stdout = process.0.stdout.take().unwrap();
    let (send, receive) = mpsc::sync_channel(1);
    let reader = thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout.take(256))
            .read_line(&mut line)
            .map(|_| line);
        let _ = send.send(result);
    });
    let line = receive
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    reader.join().unwrap();
    let address = line
        .strip_prefix("LAYERFS_DAEMON_LISTEN ")
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    (process, address)
}

fn connect(address: SocketAddr) -> duplex::Connection {
    duplex::initiate(
        support::socket(TcpStream::connect_timeout(&address, Duration::from_secs(3)).unwrap()),
        &support::CLIENT_PRIVATE,
        duplex::public_key(&support::SERVER_PRIVATE).unwrap(),
    )
    .unwrap()
}

#[test]
fn disposable_install_is_refused_before_writing_store_or_catalog() {
    let fixture = support::Fixture::new("fresh-invalid-store", None);
    let (setup, mounts) = setup(&fixture, "invalid");
    let (process, address) = start(&fixture, &setup);
    let mut connection = connect(address);
    let failure = layerfs_api::internal::install(&fixture.project, &mut connection).unwrap_err();
    assert!(
        matches!(failure.error, InstallError::Remote(ref refusal) if refusal.kind == RefusalKind::Invalid && !refusal.claim_acknowledged && !refusal.published)
    );
    assert!(!Path::new(&setup.store).exists());
    assert_eq!(
        fs::read_dir(fixture.directory.join("catalog"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        fs::read_dir(fixture.directory.join("durable"))
            .unwrap()
            .count(),
        0
    );
    let mut control = Control::new(connect(address));
    assert!(
        matches!(control.call(Request::Hello(HelloRequest { expected_instance: None, wait_for_store: false })).unwrap(), Reply::Hello(status) if status.phase == DaemonPhase::InstallPending)
    );
    drop(control);
    drop(process);
    fs::remove_dir(mounts).unwrap();
    nix::mount::umount2(
        fixture.directory.join("fusectl").as_path(),
        nix::mount::MntFlags::MNT_DETACH,
    )
    .unwrap();
    fs::remove_dir(fixture.directory.join("fusectl")).unwrap();
    fixture.cleanup();
}

#[test]
fn failed_workspace_initialization_retains_store_and_never_uses_disposable_overlay() {
    let fixture = support::Fixture::new_durable("fresh-setup-retained", None);
    let (setup, mounts) = setup(&fixture, "retained");
    let old_file = fixture.directory.join("durable/original");
    fs::write(&old_file, b"original data").unwrap();
    let (process, address) = start(&fixture, &setup);
    let mut connection = connect(address);
    let installed = layerfs_api::internal::install(&fixture.project, &mut connection).unwrap();
    let mut control = Control::new(connection);
    assert!(
        matches!(control.call(Request::Hello(HelloRequest { expected_instance: None, wait_for_store: true })).unwrap(), Reply::Hello(status) if status.phase == DaemonPhase::Retained)
    );
    assert!(
        matches!(control.call(Request::Mount { workspace: WorkspaceId::from_authority([52; 32]).unwrap(), branch: fixture.project.branch.branch.id }).unwrap(), Reply::Refused(ref refusal) if refusal.code == layerfs_bridge::control::ControlCode::Unknown)
    );
    assert!(Path::new(&installed.manifest.locator).is_file());
    assert_eq!(fs::read(old_file).unwrap(), b"original data");
    assert_eq!(
        fs::read_dir(fixture.directory.join("catalog"))
            .unwrap()
            .count(),
        0
    );
    drop(control);
    drop(process);
    fs::remove_dir(mounts).unwrap();
    nix::mount::umount2(
        fixture.directory.join("fusectl").as_path(),
        nix::mount::MntFlags::MNT_DETACH,
    )
    .unwrap();
    fs::remove_dir(fixture.directory.join("fusectl")).unwrap();
    fixture.cleanup();
}

#[test]
fn real_daemon_creates_fresh_volume_directories_and_refuses_reuse() {
    let fixture = support::Fixture::new_durable("fresh-created-volume", None);
    let (mut setup, mounts) = setup(&fixture, "created");
    let base = fixture
        .directory
        .join("layerfs-durable")
        .join("08".repeat(32));
    let fresh = setup.fresh_workspace.as_mut().unwrap();
    fresh.catalog_directory = base.join("catalog").to_str().unwrap().into();
    fresh.overlay_directory = base.join("overlays").to_str().unwrap().into();
    fresh.create_directories = true;
    assert!(!base.exists());
    let (process, address) = start(&fixture, &setup);
    let mut connection = connect(address);
    let installed = layerfs_api::internal::install(&fixture.project, &mut connection).unwrap();
    let mut control = Control::new(connection);
    assert!(
        matches!(control.call(Request::Hello(HelloRequest { expected_instance: None, wait_for_store: true })).unwrap(), Reply::Hello(status) if status.phase == DaemonPhase::ControlReady)
    );
    assert_eq!(
        fs::metadata(&base).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let workspace = WorkspaceId::from_authority([53; 32]).unwrap();
    let bound = layerfs_api::internal::workspace(&mut control)
        .bind(workspace, fixture.project.branch.branch.id)
        .unwrap();
    assert!(base.join("catalog/roster.owner").is_file());
    assert_eq!(
        fs::read_dir(base.join("overlays"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".sqlite"))
            .count(),
        1
    );
    layerfs_api::internal::workspace(&mut control)
        .unmount(bound.token, false)
        .unwrap();
    drop(control);
    drop(process);

    setup.overlay = fixture
        .directory
        .join("created-retry-template.sqlite")
        .to_str()
        .unwrap()
        .into();
    setup.existing_store = Some(installed.manifest);
    let retry_config = config(&fixture, &setup, "created-retry.config");
    let error_file = fixture.directory.join("created-retry.stderr");
    let mut retry = Process(
        Command::new(daemon_binary::path())
            .arg("--config")
            .arg(retry_config)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(File::create(&error_file).unwrap()))
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = retry.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "duplicate Sandbox identity did not refuse"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert!(!status.success());
    assert!(fs::read_to_string(error_file)
        .unwrap()
        .contains("SandboxCreate"));
    assert!(base.join("catalog/roster.owner").is_file());
    fs::remove_dir(mounts).unwrap();
    nix::mount::umount2(
        fixture.directory.join("fusectl").as_path(),
        nix::mount::MntFlags::MNT_DETACH,
    )
    .unwrap();
    fs::remove_dir(fixture.directory.join("fusectl")).unwrap();
    fixture.cleanup();
}

#[test]
fn real_daemon_writes_durable_workspace_and_fenced_recovery_keeps_it() {
    let fixture = support::Fixture::new_durable("fresh-written", None);
    let (mut setup, mounts) = setup(&fixture, "written");
    let (mut process, address) = start(&fixture, &setup);
    let mut connection = connect(address);
    let installed = layerfs_api::internal::install(&fixture.project, &mut connection).unwrap();
    let mut control = Control::new(connection);
    assert!(
        matches!(control.call(Request::Hello(HelloRequest { expected_instance: None, wait_for_store: true })).unwrap(), Reply::Hello(status) if status.phase == DaemonPhase::ControlReady)
    );
    let workspace = WorkspaceId::from_authority([51; 32]).unwrap();
    let bound = layerfs_api::internal::workspace(&mut control)
        .bind(workspace, fixture.project.branch.branch.id)
        .unwrap();
    let ready = layerfs_api::internal::workspace(&mut control)
        .attach(bound.token)
        .unwrap();
    let mount = PathBuf::from(&ready.directory);
    let mut cleanup = Mounts(vec![mount.clone()]);
    let file = mount.join("file");
    assert!(Command::new("/bin/sh")
        .args(["-c", "printf 'durable daemon data' > \"$1\"", "sh"])
        .arg(&file)
        .uid(65534)
        .gid(65534)
        .status()
        .unwrap()
        .success());
    File::open(&file).unwrap().sync_all().unwrap();
    File::open(&mount).unwrap().sync_all().unwrap();
    drop(control);
    process.0.kill().unwrap();
    assert!(!process.0.wait().unwrap().success());
    nix::mount::umount2(&mount, nix::mount::MntFlags::MNT_DETACH).unwrap();
    fs::remove_dir(&mount).unwrap();
    cleanup.0.clear();

    // A fresh-start config must not adopt the old catalog, even after the test
    // has reaped the exact process and detached its one known old mount.
    setup.overlay = fixture
        .directory
        .join("retry-template.sqlite")
        .to_str()
        .unwrap()
        .into();
    setup.existing_store = Some(installed.manifest.clone());
    let retry_config = config(&fixture, &setup, "retry.config");
    let retry_error = fixture.directory.join("retry.stderr");
    let mut retry = Process(
        Command::new(daemon_binary::path())
            .arg("--config")
            .arg(retry_config)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(File::create(&retry_error).unwrap()))
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = retry.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "fresh restart did not refuse");
        thread::sleep(Duration::from_millis(10));
    };
    assert!(!status.success());
    assert!(fs::read_to_string(retry_error)
        .unwrap()
        .contains("overlay directory is not empty"));

    // The test owns the old process and mount; after fencing them it uses the
    // separate explicit recovery primitive. This is not automatic recovery.
    let manifest = installed.manifest;
    let store = open_store(
        PersistenceConfig::sqlite(&setup.store)
            .with_sqlite_profile(SqlitePersistenceProfile::Durable),
        &manifest.binding,
        manifest.cursor_key,
        2,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let owner = Owner::start(
        &fixture.directory.join("recovery-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let authority = Authority::for_catalog([8; 32], store.history()).unwrap();
    let roster = Roster::open(&fixture.directory.join("catalog"), authority).unwrap();
    let kernel = Arc::new(
        KernelServing::start(KernelConfig {
            mounts: mounts.clone(),
            owner_uid: 0,
            owner_gid: 0,
            command_uid: 65534,
            command_gid: 65534,
            ready_wait: Duration::from_secs(10),
            drain_wait: Duration::from_secs(10),
            control: fixture.directory.join("fusectl"),
        })
        .unwrap(),
    );
    let mut service = Service::with_kernel(store.clone(), &owner, kernel);
    let tokens = service
        .activate_durable_control(
            &owner,
            DurableControlConfig {
                catalog: roster,
                overlay_directory: fixture.directory.join("durable"),
                sandbox: [8; 32],
                profile: ProfileConfig::default(),
                minimum_namespace: 2,
            },
        )
        .unwrap();
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].workspace, workspace);
    assert!(tokens[0].namespace > bound.token.namespace);
    let Reply::Ready(ready) = service
        .execute_control(&Request::Attach(tokens[0]))
        .unwrap()
        .reply
    else {
        panic!("missing recovered mount")
    };
    cleanup.0.push(PathBuf::from(&ready.directory));
    let output = Command::new("/bin/cat")
        .arg(Path::new(&ready.directory).join("file"))
        .uid(65534)
        .gid(65534)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"durable daemon data");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = None;
    loop {
        let Reply::Status(status) = service
            .execute_control(&Request::Status(tokens[0]))
            .unwrap()
            .reply
        else {
            panic!("missing status")
        };
        let work = status.kernel.unwrap().work.unwrap();
        let now = (work.active, work.completed);
        if work.active == 0 && last == Some(now) {
            break;
        }
        assert!(Instant::now() < deadline);
        last = Some(now);
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        service
            .execute_control(&Request::Unmount(UnmountControl {
                token: tokens[0],
                discard: true
            }))
            .unwrap()
            .reply,
        Reply::Unmounted(tokens[0])
    );
    cleanup.0.clear();
    drop(service);
    owner.stop().unwrap();
    drop(store);
    fs::remove_dir(mounts).unwrap();
    nix::mount::umount2(
        fixture.directory.join("fusectl").as_path(),
        nix::mount::MntFlags::MNT_DETACH,
    )
    .unwrap();
    fs::remove_dir(fixture.directory.join("fusectl")).unwrap();
    fixture.cleanup();
}
