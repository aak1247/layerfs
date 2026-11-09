//! A configured host export must not silently become a container-local mount.
#![cfg(target_os = "linux")]
use layerfs_bridge::daemon_setup::{DaemonLimits, DaemonSetup, HostExportIdentity};
use std::{
    fs::{self, DirBuilder, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    process::Command,
};

#[test]
fn unavailable_host_export_is_refused_before_overlay_or_store_creation() {
    let directory = std::env::temp_dir().join(format!(
        "layerfs-host-export-startup-{}",
        std::process::id()
    ));
    DirBuilder::new().mode(0o700).create(&directory).unwrap();
    let path = |name| directory.join(name).to_str().unwrap().to_owned();
    let setup = DaemonSetup {
        listen: "127.0.0.1:0".into(),
        private_key: [1; 32],
        control_peer: [2; 32],
        store: path("store.sqlite"),
        overlay: path("overlay.sqlite"),
        mounts: path("mounts"),
        command_uid: u32::MAX - 1,
        command_gid: u32::MAX - 1,
        limits: DaemonLimits {
            connections: 1,
            handshake_ms: 1000,
            cache_bytes: 65536,
            namespaces: 1,
            pager_kib: 1024,
            serial_low_water: 0,
            idle_exit_s: 0,
        },
        existing_store: None,
        fresh_workspace: None,
        host_export: Some(HostExportIdentity {
            boot_id: "01234567-89ab-cdef-0123-456789abcdef".into(),
            device: 1,
            inode: 1,
            shared_peer: 1,
        }),
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
    let result = Command::new(env!("CARGO_BIN_EXE_layerfs-daemon"))
        .arg("--config")
        .arg(&config)
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("host Workspace export"));
    assert!(!directory.join("overlay.sqlite").exists());
    assert!(!directory.join("store.sqlite").exists());
    assert!(!directory.join("mounts").exists());
    fs::remove_file(config).unwrap();
    fs::remove_dir(directory).unwrap();
}
