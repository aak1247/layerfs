//! Shared and exclusive admission on the same real volume directory inode.
#![cfg(target_os = "linux")]

use layerfs_daemon::durable_directories::SharedStoreUse;
use nix::fcntl::{Flock, FlockArg};
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{symlink, DirBuilderExt, MetadataExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn own_directory() -> io::Result<PathBuf> {
    let path = std::env::temp_dir().join(format!(
        "layerfs-shared-store-use-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    DirBuilder::new().mode(0o700).create(&path)?;
    Ok(path)
}

fn exclusive(path: &Path) -> Result<Flock<File>, nix::errno::Errno> {
    Flock::lock(File::open(path).unwrap(), FlockArg::LockExclusiveNonblock)
        .map_err(|(_, error)| error)
}

#[test]
fn shared_store_uses_exact_named_inode_and_excludes_maintenance() -> io::Result<()> {
    let base = own_directory()?;
    let volume = base.join("volume");
    DirBuilder::new().mode(0o700).create(&volume)?;
    let original = fs::metadata(&volume)?;
    let shared = SharedStoreUse::acquire(&volume)?;
    shared.verify()?;
    assert_eq!(
        exclusive(&volume).unwrap_err(),
        nix::errno::Errno::EWOULDBLOCK
    );
    let other = SharedStoreUse::acquire(&volume)?;
    drop(other);
    assert_eq!(
        exclusive(&volume).unwrap_err(),
        nix::errno::Errno::EWOULDBLOCK
    );
    fs::rename(&volume, base.join("original"))?;
    symlink(base.join("original"), &volume)?;
    assert!(shared.verify().is_err());
    assert!(SharedStoreUse::acquire_if_accessible(&volume)?.is_none());
    fs::remove_file(&volume)?;
    DirBuilder::new().mode(0o700).create(&volume)?;
    assert_ne!(fs::metadata(&volume)?.ino(), original.ino());
    assert!(shared.verify().is_err());
    drop(shared);
    let held = exclusive(&volume).unwrap();
    assert_eq!(
        SharedStoreUse::acquire(&volume).unwrap_err().raw_os_error(),
        Some(nix::errno::Errno::EWOULDBLOCK as i32)
    );
    drop(held);
    SharedStoreUse::acquire(&volume)?.verify()?;
    fs::remove_dir(&volume)?;
    fs::remove_dir(base.join("original"))?;
    fs::remove_dir(base)?;
    Ok(())
}

#[test]
#[ignore = "requires a caller for whom the real directory open is denied"]
fn inaccessible_directory_retains_ordinary_access_without_claiming_a_guard() -> io::Result<()> {
    let base = own_directory()?;
    let volume = base.join("search-only");
    DirBuilder::new().mode(0o111).create(&volume)?;
    // This exact environment must really deny an O_RDONLY directory open;
    // an elevated DAC capability is not evidence of the compatibility path.
    assert_eq!(
        File::open(&volume).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert!(SharedStoreUse::acquire_if_accessible(&volume)?.is_none());
    println!("LAYERFS_EACCES_COVERED");
    fs::remove_dir(&volume)?;
    fs::remove_dir(base)?;
    Ok(())
}

#[test]
fn shared_store_does_not_require_volume_owner_write_permission() -> io::Result<()> {
    let base = own_directory()?;
    let volume = base.join("read-only-volume");
    DirBuilder::new().mode(0o500).create(&volume)?;
    let guard = SharedStoreUse::acquire(&volume)?;
    guard.verify()?;
    assert_eq!(fs::metadata(&volume)?.mode() & 0o777, 0o500);
    drop(guard);
    fs::remove_dir(&volume)?;
    fs::remove_dir(base)?;
    Ok(())
}

// Selected only by the parent test. It deliberately retains the same guard
// after the setup function returns, as the actual daemon CLI does.
#[test]
fn shared_store_use_child() -> io::Result<()> {
    let Some(path) = std::env::var_os("LAYERFS_TEST_SHARED_VOLUME") else {
        return Ok(());
    };
    let ready = std::env::var_os("LAYERFS_TEST_SHARED_READY").unwrap();
    fn initialize(path: &Path) -> io::Result<()> {
        let guard = SharedStoreUse::acquire(path)?;
        guard.verify()?;
        std::mem::forget(guard);
        Ok(())
    }
    initialize(Path::new(&path))?;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(ready)?
        .write_all(b"held")?;
    let mut byte = [0];
    io::stdin().read_exact(&mut byte)?;
    Ok(())
}

#[test]
fn shared_store_guard_survives_setup_until_original_process_exit() -> io::Result<()> {
    let base = own_directory()?;
    let volume = base.join("volume");
    let ready = base.join("ready");
    DirBuilder::new().mode(0o700).create(&volume)?;
    let mut child = Command::new(std::env::current_exe()?)
        .args(["--exact", "shared_store_use_child", "--nocapture"])
        .env("LAYERFS_TEST_SHARED_VOLUME", &volume)
        .env("LAYERFS_TEST_SHARED_READY", &ready)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.exists() {
        if child.try_wait()?.is_some() || Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::other(
                "original guard child did not become ready",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(child.try_wait()?.is_none());
    assert_eq!(
        exclusive(&volume).unwrap_err(),
        nix::errno::Errno::EWOULDBLOCK
    );
    child.stdin.take().unwrap().write_all(b"x")?;
    assert!(child.wait()?.success());
    drop(exclusive(&volume).unwrap());
    fs::remove_dir_all(base)?;
    Ok(())
}
