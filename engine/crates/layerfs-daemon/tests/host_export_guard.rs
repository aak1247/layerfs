//! Cross-namespace export verification; the fixture is supplied by the caller.
#![cfg(target_os = "linux")]
use layerfs_bridge::daemon_setup::{DaemonLimits, DaemonSetup, HostExportIdentity};
use layerfs_daemon::control::ContainerExportRoot;
use std::{
    env,
    fs::{self, DirBuilder, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

fn expected() -> HostExportIdentity {
    let number = |name| env::var(name).unwrap().parse().unwrap();
    HostExportIdentity {
        boot_id: env::var("LAYERFS_TEST_EXPORT_BOOT").unwrap(),
        device: number("LAYERFS_TEST_EXPORT_DEVICE"),
        inode: number("LAYERFS_TEST_EXPORT_INODE"),
        shared_peer: number("LAYERFS_TEST_EXPORT_PEER"),
    }
}

#[test]
#[ignore = "requires a caller-owned fresh shared export bound into this namespace"]
fn daemon_checks_the_recorded_host_root_and_refuses_other_identities() {
    let path_text = env::var("LAYERFS_TEST_EXPORT_PATH").unwrap();
    let path = Path::new(&path_text);
    let expected = expected();
    let guard = ContainerExportRoot::open_fresh(path, &expected).unwrap();
    guard.verify().unwrap();
    for index in 0..4 {
        let mut wrong = expected.clone();
        match index {
            0 => wrong.boot_id = "01234567-89ab-cdef-0123-456789abcdef".into(),
            1 => wrong.device ^= 1,
            2 => wrong.inode ^= 1,
            3 => wrong.shared_peer ^= 1,
            _ => unreachable!(),
        }
        assert!(ContainerExportRoot::open_fresh(path, &wrong).is_err());
    }
    let marker = path.join(format!("owned-test-marker-{}", std::process::id()));
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
        .unwrap()
        .write_all(b"owned test marker")
        .unwrap();
    assert!(ContainerExportRoot::open_fresh(path, &expected).is_err());
    guard.verify().unwrap();
    fs::remove_file(marker).unwrap();
}

#[test]
#[ignore = "requires an external controller to change an owned shared export"]
fn daemon_refuses_a_later_propagation_change() {
    let path = env::var("LAYERFS_TEST_EXPORT_PATH").unwrap();
    let guard = ContainerExportRoot::open_fresh(Path::new(&path), &expected()).unwrap();
    let ready = env::var("LAYERFS_TEST_READY").unwrap();
    let proceed = env::var("LAYERFS_TEST_PROCEED").unwrap();
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(ready)
        .unwrap()
        .write_all(b"guard is held")
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !Path::new(&proceed).exists() {
        assert!(
            Instant::now() < deadline,
            "controller did not change the root"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert!(guard.verify().is_err());
}

#[test]
#[ignore = "requires a caller-owned shared export and a FUSE-capable container"]
fn configured_daemon_starts_with_the_verified_export() {
    let directory = env::temp_dir().join(format!("layerfs-export-start-{}", std::process::id()));
    DirBuilder::new().mode(0o700).create(&directory).unwrap();
    let path = |name| directory.join(name).to_str().unwrap().to_owned();
    let setup = DaemonSetup {
        listen: "127.0.0.1:0".into(),
        private_key: [1; 32],
        control_peer: [2; 32],
        store: path("store.sqlite"),
        overlay: path("overlay.sqlite"),
        mounts: env::var("LAYERFS_TEST_EXPORT_PATH").unwrap(),
        command_uid: 1000,
        command_gid: 1000,
        limits: DaemonLimits {
            connections: 1,
            handshake_ms: 1000,
            cache_bytes: 65536,
            namespaces: 2,
            pager_kib: 1024,
            serial_low_water: 0,
            idle_exit_s: 0,
        },
        existing_store: None,
        fresh_workspace: None,
        host_export: Some(expected()),
    };
    let config = directory.join("config");
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&config)
        .unwrap()
        .write_all(&setup.encode().unwrap())
        .unwrap();
    let mut child = Command::new(env::var("LAYERFS_TEST_DAEMON_BIN").unwrap())
        .arg("--config")
        .arg(config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let _ = child.kill();
    let status = child.wait().unwrap();
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(
        line.starts_with("LAYERFS_DAEMON_LISTEN "),
        "daemon did not start: {status:?}; {stderr}"
    );
}
