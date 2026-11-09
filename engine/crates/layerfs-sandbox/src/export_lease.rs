//! Live, cooperative exclusivity for one already-validated host export root.
use crate::host_export::HostExportRoot;
use nix::{
    fcntl::{open, Flock, FlockArg, OFlag},
    sys::stat::Mode,
};
use std::{fs::File, io};

/// Holds an advisory lock on the export directory's actual inode. This only
/// excludes other cooperating live owners; it is not a durable intent, an
/// owner-exit proof, or permission to recover after this guard is lost.
#[derive(Debug)]
pub struct HostExportLease {
    root: HostExportRoot,
    lock: Flock<File>,
}

impl HostExportLease {
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
        let lease = Self { root, lock };
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
}
