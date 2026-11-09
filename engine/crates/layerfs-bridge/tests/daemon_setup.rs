//! Private startup configuration keeps older disposable records readable.
use layerfs_bridge::{
    daemon_setup::{DaemonLimits, DaemonSetup, FreshWorkspaceSetup, HostExportIdentity},
    provision::{ProviderKind, StoreManifest, StoreProfile},
};

fn setup() -> DaemonSetup {
    DaemonSetup {
        listen: "127.0.0.1:8000".into(),
        private_key: [1; 32],
        control_peer: [2; 32],
        store: "/volume/global/store.sqlite".into(),
        overlay: "/local/overlay.sqlite".into(),
        mounts: "/workspaces".into(),
        command_uid: 1000,
        command_gid: 1000,
        limits: DaemonLimits {
            connections: 4,
            handshake_ms: 1000,
            cache_bytes: 65536,
            namespaces: 16,
            pager_kib: 1024,
            serial_low_water: 0,
            idle_exit_s: 0,
        },
        existing_store: None,
        fresh_workspace: None,
        host_export: None,
    }
}

#[test]
fn host_export_has_a_new_bounded_format_and_preserves_v7_without_it() {
    let mut value = setup();
    let ordinary = value.encode().unwrap();
    assert_eq!(&ordinary[..5], b"LFSD\x07");
    value.host_export = Some(HostExportIdentity {
        boot_id: "01234567-89ab-cdef-0123-456789abcdef".into(),
        device: 1031,
        inode: 42,
        shared_peer: 18,
    });
    let exported = value.encode().unwrap();
    assert_eq!(&exported[..5], b"LFSD\x08");
    assert_eq!(DaemonSetup::decode(&exported).unwrap(), value);
    for end in 0..exported.len() {
        assert!(DaemonSetup::decode(&exported[..end]).is_err());
    }
    let mut trailing = exported;
    trailing.push(0);
    assert!(DaemonSetup::decode(&trailing).is_err());
    for invalid in [
        "",
        "01234567-89ab-cdef-0123-456789abcdeX",
        "0123456789ab-cdef-0123-456789abcdef",
    ] {
        let mut changed = value.clone();
        changed.host_export.as_mut().unwrap().boot_id = invalid.into();
        assert!(changed.encode().is_err());
    }
    for invalid in [0, 1] {
        let mut changed = value.clone();
        if invalid == 0 {
            changed.host_export.as_mut().unwrap().inode = 0;
        } else {
            changed.host_export.as_mut().unwrap().shared_peer = 0;
        }
        assert!(changed.encode().is_err());
    }
    assert_eq!(DaemonSetup::decode(&ordinary).unwrap().host_export, None);
}

#[test]
fn fresh_workspace_roundtrips_and_old_records_remain_disposable() {
    let old = setup();
    let mut old_wire = old.encode().unwrap();
    assert_eq!(old_wire.pop(), Some(0));
    old_wire[4] = 5;
    assert_eq!(DaemonSetup::decode(&old_wire).unwrap(), old);
    let mut new = old;
    new.fresh_workspace = Some(FreshWorkspaceSetup {
        sandbox: [3; 32],
        catalog_directory: "/volume/sandboxes/one/catalog".into(),
        overlay_directory: "/volume/sandboxes/one/overlays".into(),
        create_directories: false,
    });
    let encoded = new.encode().unwrap();
    assert_eq!(DaemonSetup::decode(&encoded).unwrap(), new);
    let mut previous = encoded.clone();
    assert_eq!(previous.pop(), Some(0));
    previous[4] = 6;
    assert_eq!(DaemonSetup::decode(&previous).unwrap(), new);
    for end in 0..encoded.len() {
        assert!(DaemonSetup::decode(&encoded[..end]).is_err());
    }
    let mut trailing = encoded;
    trailing.push(0);
    assert!(DaemonSetup::decode(&trailing).is_err());
    assert!(!format!("{new:?}").contains(&format!("{:?}", new.private_key)));
}

#[test]
fn generated_directories_are_bound_to_the_volume_and_sandbox_identity() {
    let mut value = setup();
    let base = format!("/volume/layerfs-durable/{}", "03".repeat(32));
    value.fresh_workspace = Some(FreshWorkspaceSetup {
        sandbox: [3; 32],
        catalog_directory: format!("{base}/catalog"),
        overlay_directory: format!("{base}/overlays"),
        create_directories: true,
    });
    let encoded = value.encode().unwrap();
    assert_eq!(DaemonSetup::decode(&encoded).unwrap(), value);
    assert_eq!(
        value
            .fresh_workspace
            .as_ref()
            .unwrap()
            .creation_volume()
            .unwrap()
            .unwrap(),
        std::path::Path::new("/volume")
    );
    let mut wrong = value.clone();
    wrong.fresh_workspace.as_mut().unwrap().sandbox = [4; 32];
    assert!(wrong.encode().is_err());
    let mut wrong = value.clone();
    wrong.fresh_workspace.as_mut().unwrap().overlay_directory = format!("{base}/other");
    assert!(wrong.encode().is_err());
    value.store = "/other/store.sqlite".into();
    assert!(value.encode().is_err());
}

#[test]
fn fresh_workspace_rejects_overlap_and_disposable_existing_store() {
    let mut value = setup();
    value.fresh_workspace = Some(FreshWorkspaceSetup {
        sandbox: [3; 32],
        catalog_directory: "/volume/sandboxes/one/catalog".into(),
        overlay_directory: "/volume/sandboxes/one/overlays".into(),
        create_directories: false,
    });
    for invalid in [
        "/workspaces/hidden",
        "/volume/global",
        "/volume/sandboxes/one/catalog/child",
        "/volume/../escape",
    ] {
        let mut changed = value.clone();
        changed.fresh_workspace.as_mut().unwrap().overlay_directory = invalid.into();
        assert!(changed.encode().is_err(), "{invalid}");
    }
    value.existing_store = Some(StoreManifest {
        provider: ProviderKind::Sqlite,
        locator: value.store.clone(),
        profile: StoreProfile::Disposable,
        binding: b"binding".to_vec(),
        cursor_key: [4; 32],
        stack: [5; 17],
        branch: [6; 17],
        root: [7; 32],
        bytes: 1,
        host_sqlite: "3.51.0".into(),
        daemon_sqlite: Some("3.51.0".into()),
    });
    assert!(value.encode().is_err());
    value.existing_store.as_mut().unwrap().profile = StoreProfile::Durable;
    assert!(value.encode().is_ok());
}
