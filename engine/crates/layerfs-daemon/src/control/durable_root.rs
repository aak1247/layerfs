//! Retain the exact private local Overlay directory throughout control service.
use nix::sys::statfs;
use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

pub(super) struct DurableRoot {
    pub path: PathBuf,
    file: Option<File>,
    quarantined: bool,
}
impl DurableRoot {
    pub fn open(path: &Path) -> io::Result<Self> {
        let path = path.canonicalize()?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open(&path)?;
        let root = Self {
            path,
            file: Some(file),
            quarantined: false,
        };
        root.verify()?;
        let kind = statfs::fstatfs(root.file.as_ref().expect("root retained"))
            .map_err(|error| io::Error::from_raw_os_error(error as i32))?
            .filesystem_type();
        if ![
            statfs::EXT4_SUPER_MAGIC,
            statfs::XFS_SUPER_MAGIC,
            statfs::BTRFS_SUPER_MAGIC,
        ]
        .contains(&kind)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "durable Overlay requires a local filesystem",
            ));
        }
        Ok(root)
    }
    pub fn verify(&self) -> io::Result<()> {
        let opened = self.file.as_ref().expect("root retained").metadata()?;
        let named = fs::symlink_metadata(&self.path)?;
        let uid = nix::unistd::geteuid().as_raw();
        if !opened.is_dir()
            || !named.is_dir()
            || opened.uid() != uid
            || named.uid() != uid
            || opened.mode() & 0o077 != 0
            || named.mode() & 0o077 != 0
            || opened.dev() != named.dev()
            || opened.ino() != named.ino()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "durable Overlay directory identity or privacy changed",
            ));
        }
        Ok(())
    }
    pub fn quarantine(&mut self) {
        self.quarantined = true;
    }
}
impl Drop for DurableRoot {
    fn drop(&mut self) {
        if self.quarantined {
            if let Some(file) = self.file.take() {
                std::mem::forget(file);
            }
        }
    }
}
