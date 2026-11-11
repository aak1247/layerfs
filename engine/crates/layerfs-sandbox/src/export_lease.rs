//! Live, cooperative exclusivity for one already-validated host export root.
use crate::host_export::HostExportRoot;
use nix::{
    fcntl::{open, Flock, FlockArg, OFlag},
    sys::stat::Mode,
};
use std::{
    fs::File,
    io,
    sync::atomic::{AtomicBool, Ordering},
};

/// Holds an advisory lock on the export directory's actual inode. This only
/// excludes other cooperating live owners; it is not a durable intent, an
/// owner-exit proof, or permission to recover after this guard is lost.
#[derive(Debug)]
pub struct HostExportLease {
    root: HostExportRoot,
    lock: Flock<File>,
    create_attempted: AtomicBool,
}

impl HostExportLease {
    /// Reacquires the exact saved export directory for an independently fenced
    /// controller. Existing Workspace mounts remain in place. The caller must
    /// validate the saved root identity before this call and prove the previous
    /// controller cannot issue further effects. The successful local lock does
    /// not establish durable freshness or settle an unknown operation.
    pub fn reattach(root: HostExportRoot) -> io::Result<Self> {
        root.verify()?;
        let flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
        let file = File::from(open(root.path(), flags, Mode::empty())?);
        root.matches_descriptor(&file)?;
        let lock = Flock::lock(file, FlockArg::LockExclusiveNonblock)
            .map_err(|(_, error)| io::Error::from(error))?;
        let lease = Self {
            root,
            lock,
            create_attempted: AtomicBool::new(true),
        };
        lease.verify()?;
        Ok(lease)
    }

    /// Attempts the directory lock without waiting and retains the root guard.
    /// No file is created or modified. An uncertain Sandbox result must keep
    /// this owner and its independent durable intent, not retry the operation.
    pub fn try_acquire(root: HostExportRoot) -> io::Result<Self> {
        root.verify()?;
        let flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
        let file = File::from(open(root.path(), flags, Mode::empty())?);
        root.matches_descriptor(&file)?;
        let lock = Flock::lock(file, FlockArg::LockExclusiveNonblock)
            .map_err(|(_, error)| io::Error::from(error))?;
        let lease = Self {
            root,
            lock,
            create_attempted: AtomicBool::new(false),
        };
        lease.verify()?;
        lease.root.verify_fresh()?;
        Ok(lease)
    }

    /// Original guarded path and identity for the exclusive owner.
    pub fn root(&self) -> &HostExportRoot {
        &self.root
    }

    /// Checks that the locked directory still names the guarded mount root.
    /// This does not prove that a previous process or consumer has exited.
    pub fn verify(&self) -> io::Result<()> {
        self.root.verify()?;
        self.root.matches_descriptor(&self.lock)
    }

    pub(crate) fn reserve_create(&self) -> io::Result<()> {
        self.verify()?;
        self.root.verify_fresh()?;
        self.create_attempted
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "host export create already attempted",
                )
            })?;
        self.root.verify_fresh()
    }
}

impl PartialEq for HostExportLease {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl Eq for HostExportLease {}
