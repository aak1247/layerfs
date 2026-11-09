//! Cross-namespace export verification; the fixture is supplied by the caller.
#![cfg(target_os = "linux")]
use layerfs_bridge::daemon_setup::{DaemonLimits, DaemonSetup, HostExportIdentity};
use layerfs_daemon::control::{ContainerExportRoot, KernelConfig, KernelServing};
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
#[ignore = "requires root and a caller-owned group-restricted export"]
fn daemon_refuses_permission_widening() {
    use std::os::unix::fs::PermissionsExt;
    let path = env::var("LAYERFS_TEST_EXPORT_PATH").unwrap();
    let guard = ContainerExportRoot::open_fresh(Path::new(&path), &expected()).unwrap();
    let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode & 0o007, 0);
    fs::set_permissions(&path, fs::Permissions::from_mode(mode | 0o005)).unwrap();
    let refused = guard.verify().is_err();
    fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
    assert!(refused, "daemon must refuse newly granted access");
}

#[test]
#[ignore = "requires root and a caller-owned empty shared export"]
fn claim_is_exclusive_and_detects_altered_or_replaced_records() {
    use std::os::unix::fs::FileExt;
    let path = env::var("LAYERFS_TEST_EXPORT_PATH").unwrap();
    let root = Path::new(&path);
    let mut guard = ContainerExportRoot::open_fresh(root, &expected()).unwrap();
    let unexpected = root.join(format!("owned-test-entry-{}", std::process::id()));
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&unexpected)
        .unwrap();
    assert!(guard.claim_fresh([8; 32]).is_err());
    assert!(!root.join(".layerfs-export-owner").exists());
    fs::remove_file(unexpected).unwrap();
    guard
        .claim_fresh(layerfs_bridge::duplex::public_key(&[7; 32]).unwrap())
        .unwrap();
    guard.verify().unwrap();
    assert!(guard.claim_fresh([8; 32]).is_err());
    assert!(ContainerExportRoot::open_fresh(root, &expected()).is_err());
    let marker = root.join(".layerfs-export-owner");
    let original = fs::read(&marker).unwrap();
    let file = OpenOptions::new().write(true).open(&marker).unwrap();
    file.write_all_at(&[original[0] ^ 1], 0).unwrap();
    assert!(guard.verify().is_err());
    file.write_all_at(&original[..1], 0).unwrap();
    guard.verify().unwrap();
    let saved = root.join(format!("owned-test-claim-{}", std::process::id()));
    fs::rename(&marker, &saved).unwrap();
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
        .unwrap()
        .write_all(&original)
        .unwrap();
    assert!(guard.verify().is_err());
    fs::remove_file(&marker).unwrap();
    fs::rename(saved, &marker).unwrap();
    guard.verify().unwrap();
    drop((guard, file));
    fs::remove_file(marker).unwrap();
}

#[test]
#[ignore = "requires root and a caller-owned empty shared export"]
fn exported_serving_requires_a_claimed_root() {
    let path = env::var("LAYERFS_TEST_EXPORT_PATH").unwrap();
    let root = Path::new(&path);
    let config = KernelConfig {
        mounts: root.to_owned(),
        owner_uid: 0,
        owner_gid: 0,
        command_uid: 1000,
        command_gid: 1000,
        ready_wait: Duration::from_secs(1),
        drain_wait: Duration::from_secs(1),
        control: "/nonexistent-control".into(),
    };
    let guard = ContainerExportRoot::open_fresh(root, &expected()).unwrap();
    assert!(KernelServing::start_with_export(config.clone(), guard).is_err());
    let mut guard = ContainerExportRoot::open_fresh(root, &expected()).unwrap();
    guard
        .claim_fresh(layerfs_bridge::duplex::public_key(&[7; 32]).unwrap())
        .unwrap();
    let serving = KernelServing::start_with_export(config, guard).unwrap();
    drop(serving);
    fs::remove_file(root.join(".layerfs-export-owner")).unwrap();
}

#[test]
#[ignore = "requires a caller-owned shared export and a FUSE-capable container"]
fn configured_daemon_starts_with_the_verified_export() {
    let directory = env::temp_dir().join(format!("layerfs-export-start-{}", std::process::id()));
    DirBuilder::new().mode(0o700).create(&directory).unwrap();
    let path = |name: &str| directory.join(name).to_str().unwrap().to_owned();
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
    let mut children = Vec::new();
    for index in 0..2 {
        let mut selected = setup.clone();
        selected.private_key = [index + 1; 32];
        selected.store = path(&format!("store-{index}.sqlite"));
        selected.overlay = path(&format!("overlay-{index}.sqlite"));
        let config = directory.join(format!("config-{index}"));
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&config)
            .unwrap()
            .write_all(&selected.encode().unwrap())
            .unwrap();
        children.push(
            Command::new(env::var("LAYERFS_TEST_DAEMON_BIN").unwrap())
                .arg("--config")
                .arg(config)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
    }
    let mut ready = 0;
    let mut refused = 0;
    for (index, mut child) in children.into_iter().enumerate() {
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let started = line.starts_with("LAYERFS_DAEMON_LISTEN ");
        let _ = child.kill();
        let status = child.wait().unwrap();
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        if started {
            ready += 1;
        } else {
            refused += 1;
            assert!(
                stderr.contains("host Workspace export"),
                "{status:?}; {stderr}"
            );
            assert!(!directory.join(format!("overlay-{index}.sqlite")).exists());
        }
    }
    assert_eq!((ready, refused), (1, 1));
    assert!(ContainerExportRoot::open_fresh(Path::new(&setup.mounts), &expected()).is_err());
    let claim = fs::read(Path::new(&setup.mounts).join(".layerfs-export-owner")).unwrap();
    assert!(claim.starts_with(b"LAYERFS-EXPORT-OWNER\x01"));
    assert!(!claim
        .windows(32)
        .any(|bytes| bytes == [1; 32] || bytes == [2; 32]));
}
