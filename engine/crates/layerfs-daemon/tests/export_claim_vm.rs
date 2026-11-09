//! Guest memory-loss probe for an acknowledged ext4 host-export claim.
#![cfg(target_os = "linux")]
use layerfs_bridge::{daemon_setup::HostExportIdentity, duplex};
use layerfs_daemon::control::ContainerExportRoot;
use nix::mount::{mount, MsFlags};
use std::{
    env, fs,
    io::{self, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
    thread,
    time::Duration,
};

fn bind_shared(root: &Path) {
    mount(
        Some(root),
        root,
        None::<&str>,
        MsFlags::MS_BIND,
        None::<&str>,
    )
    .unwrap();
    mount(
        None::<&str>,
        root,
        None::<&str>,
        MsFlags::MS_PRIVATE,
        None::<&str>,
    )
    .unwrap();
    mount(
        None::<&str>,
        root,
        None::<&str>,
        MsFlags::MS_SHARED,
        None::<&str>,
    )
    .unwrap();
}

fn identity(root: &Path) -> HostExportIdentity {
    let table = fs::read_to_string("/proc/self/mountinfo").unwrap();
    let target = root.to_str().unwrap();
    let rows: Vec<_> = table
        .lines()
        .filter(|line| line.split(' ').nth(4) == Some(target))
        .collect();
    assert_eq!(rows.len(), 1);
    let peer = rows[0]
        .split(" - ")
        .next()
        .unwrap()
        .split(' ')
        .find_map(|field| field.strip_prefix("shared:"))
        .unwrap()
        .parse()
        .unwrap();
    let metadata = fs::metadata(root).unwrap();
    HostExportIdentity {
        boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .unwrap()
            .trim()
            .into(),
        device: metadata.dev(),
        inode: metadata.ino(),
        shared_peer: peer,
    }
}

#[test]
#[ignore = "requires the owned ext4 VM power-loss harness"]
fn durable_save_survives_vm_power_loss() {
    let root = env::var("LAYERFS_DURABLE_VM_ROOT").unwrap();
    let mode = env::var("LAYERFS_DURABLE_VM_MODE").unwrap();
    let root = Path::new(&root);
    if mode == "write" {
        fs::create_dir(root).unwrap();
        fs::set_permissions(root, fs::Permissions::from_mode(0o750)).unwrap();
        fs::File::open(root.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
    }
    bind_shared(root);
    let observed = identity(root);
    let public = duplex::public_key(&[7; 32]).unwrap();
    let marker = root.join(".layerfs-export-owner");
    match mode.as_str() {
        "write" => {
            let mut guard = ContainerExportRoot::open_fresh(root, &observed).unwrap();
            guard.claim_fresh(public).unwrap();
            guard.verify().unwrap();
            println!(
                "LAYERFS_VM_ACK export_bytes={}",
                fs::metadata(marker).unwrap().len()
            );
            io::stdout().flush().unwrap();
            loop {
                thread::sleep(Duration::from_secs(1));
            }
        }
        "read" => {
            let metadata = fs::symlink_metadata(&marker).unwrap();
            assert!(metadata.is_file() && metadata.uid() == 0 && metadata.nlink() == 1);
            assert_eq!(metadata.mode() & 0o7777, 0o600);
            let body = fs::read(&marker).unwrap();
            let magic = b"LAYERFS-EXPORT-OWNER\x01";
            assert_eq!(body.len(), magic.len() + 36 + public.len());
            assert_eq!(&body[..magic.len()], magic);
            let previous_boot = std::str::from_utf8(&body[magic.len()..magic.len() + 36]).unwrap();
            assert_ne!(previous_boot, observed.boot_id);
            assert_eq!(&body[magic.len() + 36..], public);
            assert!(ContainerExportRoot::open_fresh(root, &observed).is_err());
            println!("LAYERFS_VM_RECOVERED");
            io::stdout().flush().unwrap();
            loop {
                thread::sleep(Duration::from_secs(1));
            }
        }
        _ => panic!("unexpected VM mode"),
    }
}
