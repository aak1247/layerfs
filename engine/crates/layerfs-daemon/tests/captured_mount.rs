//! A real, nonpublishing captured mount beside its still-writable Workspace.
#![cfg(target_os = "linux")]
#[allow(dead_code)]
#[path = "support/installed_store.rs"]
mod installed;
#[allow(dead_code)]
#[path = "support/namespace_model.rs"]
mod model;
#[allow(dead_code)]
#[path = "support/mounted.rs"]
mod mounted;
#[allow(dead_code)]
#[path = "support/mounted_commit.rs"]
mod rig;

use layerfs_daemon::SnapshotPort;
use layerfs_fuse::{
    attributes::Identity,
    snapshot::{SnapshotConfig, SnapshotDetach, SnapshotSession},
};
use nix::errno::Errno;
use std::{
    fs,
    os::unix::fs::{symlink, MetadataExt, PermissionsExt},
    path::Path,
    sync::Arc,
    time::Duration,
};

fn base(root: &Path) {
    fs::create_dir(root.join("many")).unwrap();
    for index in 0..150 {
        fs::write(
            root.join("many").join(format!("file-{index:03}")),
            index.to_string(),
        )
        .unwrap();
    }
    fs::write(root.join("active"), b"base").unwrap();
    fs::set_permissions(root, fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(root.join("many"), fs::Permissions::from_mode(0o755)).unwrap();
    rig::stamp_tree(root);
}

#[test]
#[ignore = "requires a task-owned Linux mount namespace with CAP_SYS_ADMIN and /dev/fuse"]
fn captured_mount_stays_readonly_while_original_workspace_continues_writing() {
    let rig = rig::Rig::built("-captured-mount", base);
    fs::set_permissions(&rig.fixture.directory, fs::Permissions::from_mode(0o755)).unwrap();
    let ready = rig.mount(22);
    let live = Path::new(&ready.directory);
    fs::write(live.join("active"), b"before").unwrap();
    fs::set_permissions(live.join("active"), fs::Permissions::from_mode(0o640)).unwrap();
    fs::hard_link(live.join("active"), live.join("alias")).unwrap();
    symlink("active", live.join("snapshot-link")).unwrap();
    let original = fs::metadata(live.join("active")).unwrap();
    let branch = rig
        .store
        .history()
        .branch_snapshot(rig.harness.branch)
        .unwrap()
        .unwrap();

    let operation = rig.harness.service.operation(ready.token).unwrap();
    let route = operation.workspace().route();
    let pin = operation.overlay().pin_snapshot(route, 51_001).unwrap();
    let port = SnapshotPort::new(rig.harness.service.operation(ready.token).unwrap(), pin).unwrap();
    let root_serial = operation
        .workspace()
        .view_for_source(pin.source())
        .unwrap()
        .root_serial();
    let directory = rig.fixture.directory.join("captured-view");
    let mut mounted = SnapshotSession::attach(
        SnapshotConfig {
            directory: directory.clone(),
            owner_uid: nix::unistd::Uid::effective().as_raw(),
            owner_gid: nix::unistd::Gid::effective().as_raw(),
            identity: Identity {
                root: root_serial,
                uid: mounted::COMMAND,
                gid: mounted::COMMAND,
            },
            ready_wait: Duration::from_secs(5),
            drain_wait: Duration::from_secs(5),
        },
        Arc::new(port),
    )
    .unwrap();
    assert!(mounted
        .entry()
        .unwrap()
        .options
        .split(',')
        .any(|part| part == "ro"));
    assert_eq!(fs::read(directory.join("active")).unwrap(), b"before");
    let observed = fs::metadata(directory.join("active")).unwrap();
    assert_eq!(
        (
            observed.mode() & 0o7777,
            observed.mtime(),
            observed.mtime_nsec()
        ),
        (
            original.mode() & 0o7777,
            original.mtime(),
            original.mtime_nsec()
        )
    );
    assert_eq!(
        observed.ino(),
        fs::metadata(directory.join("alias")).unwrap().ino()
    );
    assert_eq!(
        fs::read_link(directory.join("snapshot-link")).unwrap(),
        Path::new("active")
    );
    assert_eq!(fs::read_dir(directory.join("many")).unwrap().count(), 150);

    fs::write(live.join("active"), b"after!").unwrap();
    fs::remove_file(live.join("alias")).unwrap();
    fs::rename(live.join("active"), live.join("renamed")).unwrap();
    fs::remove_file(live.join("snapshot-link")).unwrap();
    fs::write(live.join("later"), b"new").unwrap();
    fs::set_permissions(live.join("renamed"), fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(fs::read(directory.join("active")).unwrap(), b"before");
    assert_eq!(fs::read(directory.join("alias")).unwrap(), b"before");
    assert!(directory.join("renamed").symlink_metadata().is_err());
    assert!(directory.join("later").symlink_metadata().is_err());
    assert_eq!(
        fs::read_link(directory.join("snapshot-link")).unwrap(),
        Path::new("active")
    );
    assert_eq!(
        fs::metadata(directory.join("active")).unwrap().mode() & 0o7777,
        0o640
    );
    assert_eq!(fs::read(live.join("renamed")).unwrap(), b"after!");
    assert_eq!(
        rig.store
            .history()
            .branch_snapshot(rig.harness.branch)
            .unwrap()
            .unwrap(),
        branch
    );
    assert_eq!(
        fs::write(directory.join("active"), b"forbidden")
            .unwrap_err()
            .raw_os_error(),
        Some(Errno::EROFS as i32)
    );
    let command = rig::bash_in(
        mounted::COMMAND,
        &directory,
        "test \"$(cat active)\" = before",
    );
    rig::passed(&command, "command identity reads the retained snapshot");

    let held = fs::File::open(directory.join("active")).unwrap();
    assert_eq!(mounted.detach().unwrap(), SnapshotDetach::Busy);
    drop(held);
    assert_eq!(mounted.detach().unwrap(), SnapshotDetach::Detached);
    let drained = mounted.drain().unwrap();
    assert!(
        drained.is_clean(),
        "original snapshot session did not drain cleanly"
    );
    assert_eq!(drained.pin(), pin);
    operation
        .overlay()
        .release_snapshot_pin(route, drained.pin())
        .unwrap();
    mounted::until("nonpublication fold", || {
        operation
            .overlay()
            .state(route)
            .unwrap()
            .consolidating
            .is_none()
    });
    drop(operation);
    rig.unmount(&ready);
    rig.finish();
}
