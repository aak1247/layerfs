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
use nix::mount::{mount, umount2, MntFlags, MsFlags};
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

#[test]
#[ignore = "requires a task-owned Linux mount namespace with CAP_SYS_ADMIN and /dev/fuse"]
fn relative_snapshot_directory_is_rejected_before_effects() {
    let rig = rig::Rig::built("-snapshot-relative", base);
    let ready = rig.mount(23);
    let operation = rig.harness.service.operation(ready.token).unwrap();
    let route = operation.workspace().route();
    let pin = operation.overlay().pin_snapshot(route, 51_002).unwrap();
    let port = SnapshotPort::new(rig.harness.service.operation(ready.token).unwrap(), pin).unwrap();
    let root = operation
        .workspace()
        .view_for_source(pin.source())
        .unwrap()
        .root_serial();
    let relative = std::path::PathBuf::from(format!("relative-snapshot-{}", std::process::id()));
    assert!(fs::symlink_metadata(&relative).is_err());
    let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap();
    let failure = match SnapshotSession::attach(config(relative.clone(), root), Arc::new(port)) {
        Ok(_) => panic!("relative path was accepted"),
        Err(failure) => failure,
    };
    assert_eq!(failure.cause.kind(), std::io::ErrorKind::InvalidInput);
    assert!(failure.session.is_none());
    assert!(fs::symlink_metadata(&relative).is_err());
    assert_eq!(fs::read_to_string("/proc/self/mountinfo").unwrap(), mounts);
    let public_parent = rig.fixture.directory.join("public-parent");
    fs::create_dir(&public_parent).unwrap();
    fs::set_permissions(&public_parent, fs::Permissions::from_mode(0o777)).unwrap();
    let child = public_parent.join("snapshot");
    let port = SnapshotPort::new(rig.harness.service.operation(ready.token).unwrap(), pin).unwrap();
    let failure = match SnapshotSession::attach(config(child.clone(), root), Arc::new(port)) {
        Ok(_) => panic!("writable foreign parent was accepted"),
        Err(failure) => failure,
    };
    assert!(failure.session.is_none());
    assert!(fs::symlink_metadata(&child).is_err());
    assert_eq!(fs::read_to_string("/proc/self/mountinfo").unwrap(), mounts);
    fs::remove_dir(public_parent).unwrap();
    assert_eq!(
        operation
            .overlay()
            .retained_snapshot_pin(route, pin.request())
            .unwrap(),
        Some(pin)
    );
    operation
        .overlay()
        .release_snapshot_pin(route, pin)
        .unwrap();
    mounted::until("relative pin fold", || {
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

fn config(directory: std::path::PathBuf, root: u64) -> SnapshotConfig {
    SnapshotConfig {
        directory,
        owner_uid: nix::unistd::Uid::effective().as_raw(),
        owner_gid: nix::unistd::Gid::effective().as_raw(),
        identity: Identity {
            root,
            uid: mounted::COMMAND,
            gid: mounted::COMMAND,
        },
        ready_wait: Duration::from_secs(5),
        drain_wait: Duration::from_secs(5),
    }
}

/// The test-owned preload fails just the first mountinfo or mounted O_PATH
/// open, or the one outer spawn. All other calls reach the kernel.
fn injected_attach_failure(fault: &str) {
    assert_eq!(
        std::env::var("LFS_SNAPSHOT_TEST_FAULT").as_deref(),
        Ok(fault)
    );
    let rig = rig::Rig::built(&format!("-snapshot-{fault}"), base);
    let ready = rig.mount(24);
    let operation = rig.harness.service.operation(ready.token).unwrap();
    let route = operation.workspace().route();
    let pin = operation.overlay().pin_snapshot(route, 51_003).unwrap();
    let port = SnapshotPort::new(rig.harness.service.operation(ready.token).unwrap(), pin).unwrap();
    let root = operation
        .workspace()
        .view_for_source(pin.source())
        .unwrap()
        .root_serial();
    let directory = rig.fixture.directory.join("failed-snapshot");
    let failure = match SnapshotSession::attach(config(directory.clone(), root), Arc::new(port)) {
        Ok(_) => panic!("the requested failure was not injected"),
        Err(failure) => failure,
    };
    let expected = match fault {
        "mountinfo" => Errno::EACCES,
        "open" => Errno::EMFILE,
        "spawn" => Errno::EAGAIN,
        _ => panic!("unknown fault"),
    };
    assert_eq!(failure.cause.raw_os_error(), Some(expected as i32));
    let mut session = *failure
        .session
        .expect("retain the original mounted session");
    assert_eq!(
        operation
            .overlay()
            .retained_snapshot_pin(route, pin.request())
            .unwrap(),
        Some(pin)
    );
    if fault == "mountinfo" || fault == "open" {
        assert!(session.entry().is_none());
        let replacement = rig.fixture.directory.join("replacement");
        fs::create_dir(&replacement).unwrap();
        mount(
            Some(replacement.as_path()),
            directory.as_path(),
            None::<&str>,
            MsFlags::MS_BIND,
            None::<&str>,
        )
        .unwrap();
        assert!(
            session.observe_mount().is_err(),
            "a replacement mount must not be adopted"
        );
        umount2(directory.as_path(), MntFlags::empty()).unwrap();
        fs::remove_dir(replacement).unwrap();
        assert!(session
            .observe_mount()
            .unwrap()
            .source
            .starts_with("layerfs-snapshot-"));
    } else {
        assert!(session.entry().is_some());
    }
    assert_eq!(session.detach().unwrap(), SnapshotDetach::Detached);
    let drained = session.drain().unwrap();
    assert!(drained.outcome().is_none(), "no receiver was started");
    assert!(
        !drained.is_clean(),
        "a failed attach is not a successful archive"
    );
    assert_eq!(drained.pin(), pin);
    assert_eq!(
        operation
            .overlay()
            .retained_snapshot_pin(route, pin.request())
            .unwrap(),
        Some(pin)
    );
    operation
        .overlay()
        .release_snapshot_pin(route, drained.pin())
        .unwrap();
    mounted::until("failed snapshot pin fold", || {
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

#[test]
#[ignore = "requires a private FUSE mount and one-shot mountinfo fault injection"]
fn snapshot_original_mount_is_reobserved_after_mountinfo_error() {
    injected_attach_failure("mountinfo");
}

#[test]
#[ignore = "requires a private FUSE mount and one-shot outer-spawn fault injection"]
fn snapshot_unstarted_receiver_does_not_prevent_drain() {
    injected_attach_failure("spawn");
}

#[test]
#[ignore = "requires a private FUSE mount and one-shot mounted O_PATH fault injection"]
fn snapshot_original_mount_is_reobserved_after_mounted_open_error() {
    injected_attach_failure("open");
}
