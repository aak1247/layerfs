//! Recovery startup rejects changed Store identities before creating an Overlay.
#![cfg(target_os = "linux")]

#[path = "support/daemon_binary.rs"]
mod daemon_binary;

use layerfs_bridge::{
    daemon_recovery_setup::{RecoveryDaemonSetup, RecoveryWorkspaceSetup},
    daemon_setup::{DaemonLimits, DaemonSetup},
    provision::{ProviderKind, StoreManifest, StoreProfile},
};
use std::{
    fs,
    io::Write,
    os::unix::fs::{symlink, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn private_directory(path: &Path) {
    fs::create_dir(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

fn rejected(setup: &RecoveryDaemonSetup, config: &Path, overlay: &Path) {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(config)
        .unwrap();
    file.write_all(&setup.encode().unwrap()).unwrap();
    drop(file);
    let mut child = Command::new(daemon_binary::path())
        .arg("--config")
        .arg(config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("invalid recovery config did not exit");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let result = child.wait_with_output().unwrap();
    assert!(!result.status.success());
    assert!(
        result.stdout.is_empty(),
        "invalid recovery reached the listener"
    );
    assert!(!overlay.exists(), "invalid recovery created an Overlay");
}

#[test]
fn recovery_requires_original_private_store_identity() {
    let root =
        std::env::temp_dir().join(format!("layerfs-recovery-startup-{}", std::process::id()));
    private_directory(&root);
    let config_dir = root.join("config");
    let store_dir = root.join("store");
    let volatile_dir = root.join("volatile");
    let sandbox = root.join("sandbox");
    for directory in [&config_dir, &store_dir, &volatile_dir, &sandbox] {
        private_directory(directory);
    }
    let catalog = sandbox.join("catalog");
    let overlays = sandbox.join("overlays");
    private_directory(&catalog);
    private_directory(&overlays);
    let store = store_dir.join("store.sqlite");
    fs::write(&store, b"owned non-SQLite test file").unwrap();
    fs::set_permissions(&store, fs::Permissions::from_mode(0o600)).unwrap();
    let metadata = fs::metadata(&store).unwrap();
    let uid = nix::unistd::Uid::effective().as_raw();
    let command_id = if uid == 1000 { 65534 } else { 1000 };
    let store_text = store.to_str().unwrap().to_owned();
    let overlay = volatile_dir.join("template.sqlite");
    let setup = RecoveryDaemonSetup {
        daemon: DaemonSetup {
            listen: "127.0.0.1:0".into(),
            private_key: [1; 32],
            control_peer: [2; 32],
            store: store_text.clone(),
            overlay: overlay.to_str().unwrap().into(),
            mounts: root.join("mounts").to_str().unwrap().into(),
            command_uid: command_id,
            command_gid: command_id,
            limits: DaemonLimits {
                connections: 1,
                handshake_ms: 1000,
                cache_bytes: 65536,
                namespaces: 1,
                pager_kib: 1024,
                serial_low_water: 0,
                idle_exit_s: 0,
            },
            existing_store: Some(StoreManifest {
                provider: ProviderKind::Sqlite,
                locator: store_text,
                profile: StoreProfile::Durable,
                binding: b"binding".to_vec(),
                cursor_key: [4; 32],
                stack: [5; 17],
                branch: [6; 17],
                root: [7; 32],
                bytes: 1,
                host_sqlite: "3.51.0".into(),
                daemon_sqlite: Some("3.51.0".into()),
            }),
            fresh_workspace: None,
            host_export: None,
        },
        workspace: RecoveryWorkspaceSetup {
            sandbox: [3; 32],
            catalog_directory: catalog.to_str().unwrap().into(),
            overlay_directory: overlays.to_str().unwrap().into(),
            minimum_namespace: 1,
            boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .unwrap()
                .trim()
                .into(),
            store_device: metadata.dev(),
            store_inode: metadata.ino(),
        },
    };
    let mut wrong = setup.clone();
    wrong.workspace.store_device ^= 1;
    rejected(&wrong, &config_dir.join("device"), &overlay);
    wrong = setup.clone();
    wrong.workspace.store_inode = if metadata.ino() == 1 {
        2
    } else {
        metadata.ino() - 1
    };
    rejected(&wrong, &config_dir.join("inode"), &overlay);
    wrong = setup.clone();
    let replacement = if wrong.workspace.boot_id.starts_with('0') {
        "1"
    } else {
        "0"
    };
    wrong.workspace.boot_id.replace_range(0..1, replacement);
    rejected(&wrong, &config_dir.join("boot"), &overlay);
    fs::set_permissions(&store, fs::Permissions::from_mode(0o644)).unwrap();
    rejected(&setup, &config_dir.join("mode"), &overlay);
    fs::set_permissions(&store, fs::Permissions::from_mode(0o600)).unwrap();
    let alias = store_dir.join("hardlink");
    fs::hard_link(&store, &alias).unwrap();
    rejected(&setup, &config_dir.join("hardlink"), &overlay);
    fs::remove_file(alias).unwrap();
    let original = store_dir.join("original");
    fs::rename(&store, &original).unwrap();
    symlink(&original, &store).unwrap();
    rejected(&setup, &config_dir.join("symlink"), &overlay);
    fs::remove_file(&store).unwrap();
    fs::rename(original, &store).unwrap();
    assert_eq!(fs::read(&store).unwrap(), b"owned non-SQLite test file");
    assert!(fs::read_dir(&catalog).unwrap().next().is_none());
    assert!(fs::read_dir(&overlays).unwrap().next().is_none());
    // All files under this root were created by this isolated test.
    fs::remove_dir_all(root).unwrap();
}
