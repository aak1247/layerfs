//! Retain the exact private local Overlay directory throughout control service.
use nix::sys::statfs;
use std::{
    fs::{self, File, OpenOptions},
    io,
    os::fd::AsRawFd,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

pub(super) struct DurableRoot {
    pub path: PathBuf,
    file: Option<File>,
    mount_id: u64,
    quarantined: bool,
}
impl DurableRoot {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open(path)?;
        let mount_id = descriptor_mount_id(&file)?;
        let path = path.canonicalize()?;
        let root = Self {
            path,
            file: Some(file),
            mount_id,
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
        let held = self.file.as_ref().expect("root retained");
        let opened = held.metadata()?;
        let named_file = OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open(&self.path)?;
        let named = named_file.metadata()?;
        let uid = nix::unistd::geteuid().as_raw();
        if !opened.is_dir()
            || !named.is_dir()
            || opened.uid() != uid
            || named.uid() != uid
            || opened.mode() & 0o077 != 0
            || named.mode() & 0o077 != 0
            || opened.dev() != named.dev()
            || opened.ino() != named.ino()
            || descriptor_mount_id(held)? != self.mount_id
            || descriptor_mount_id(&named_file)? != self.mount_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "durable Overlay directory identity or privacy changed",
            ));
        }
        Ok(())
    }
    /// Resolve each Overlay through the retained inode, not a replaceable name.
    pub fn retained_path(&self) -> PathBuf {
        PathBuf::from(format!(
            "/proc/self/fd/{}",
            self.file.as_ref().expect("root retained").as_raw_fd()
        ))
    }
    pub fn duplicate(&self) -> io::Result<File> {
        self.file.as_ref().expect("root retained").try_clone()
    }
    pub fn device(&self) -> io::Result<u64> {
        Ok(self.file.as_ref().expect("root retained").metadata()?.dev())
    }
    pub fn inode(&self) -> io::Result<u64> {
        Ok(self.file.as_ref().expect("root retained").metadata()?.ino())
    }
    pub fn mount_id(&self) -> u64 {
        self.mount_id
    }
    /// Confirms creation of this directory and its entry in its private parent.
    pub fn sync_creation(&self) -> io::Result<()> {
        self.verify()?;
        let parent_path = self.path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "durable directory parent")
        })?;
        let parent = OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open(parent_path)?;
        let check_parent = || -> io::Result<()> {
            let opened = parent.metadata()?;
            let named = fs::symlink_metadata(parent_path)?;
            let uid = nix::unistd::geteuid().as_raw();
            if !opened.is_dir()
                || !named.is_dir()
                || opened.uid() != uid
                || named.uid() != uid
                || opened.mode() & 0o077 != 0
                || named.mode() & 0o077 != 0
                || opened.dev() != named.dev()
                || opened.ino() != named.ino()
                || opened.dev() != self.device()?
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "durable directory parent identity or privacy changed",
                ));
            }
            Ok(())
        };
        check_parent()?;
        self.file.as_ref().expect("root retained").sync_all()?;
        parent.sync_all()?;
        check_parent()?;
        self.verify()
    }
    pub fn quarantine(&mut self) {
        self.quarantined = true;
    }
}

fn descriptor_mount_id(file: &File) -> io::Result<u64> {
    let info = fs::read_to_string(format!("/proc/self/fdinfo/{}", file.as_raw_fd()))?;
    let mut matches = info.lines().filter_map(|line| line.strip_prefix("mnt_id:"));
    let id = matches
        .next()
        .and_then(|value| value.trim().parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "durable directory mount ID"))?;
    if matches.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "duplicate durable directory mount ID",
        ));
    }
    Ok(id)
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
