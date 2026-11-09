//! Private startup configuration keeps older disposable records readable.
use layerfs_bridge::{
    daemon_setup::{DaemonLimits, DaemonSetup, FreshWorkspaceSetup},
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
    }
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
    });
    let encoded = new.encode().unwrap();
    assert_eq!(DaemonSetup::decode(&encoded).unwrap(), new);
    for end in 0..encoded.len() {
        assert!(DaemonSetup::decode(&encoded[..end]).is_err());
    }
    let mut trailing = encoded;
    trailing.push(0);
    assert!(DaemonSetup::decode(&trailing).is_err());
    assert!(!format!("{new:?}").contains(&format!("{:?}", new.private_key)));
}

#[test]
fn fresh_workspace_rejects_overlap_and_disposable_existing_store() {
    let mut value = setup();
    value.fresh_workspace = Some(FreshWorkspaceSetup {
        sandbox: [3; 32],
        catalog_directory: "/volume/sandboxes/one/catalog".into(),
        overlay_directory: "/volume/sandboxes/one/overlays".into(),
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
