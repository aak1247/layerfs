//! One independently owned, read-only captured FUSE connection.
use super::{SnapshotFilesystem, SnapshotSource};
use crate::{
    attributes::Identity,
    mount::{syscalls, MountEntry, Negotiation},
};
use fuser::{Config, Session, SessionACL, SessionMonitor, SessionOutcome, SessionPhase};
use layerfs_overlay::SnapshotPin;
use layerfs_workspace::WorkspaceError;
use nix::errno::Errno;
use std::{
    fmt, fs, io,
    os::{
        fd::{AsRawFd, OwnedFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
pub struct SnapshotConfig {
    /// New directory in the original daemon-owned mount parent.
    pub directory: PathBuf,
    pub owner_uid: u32,
    pub owner_gid: u32,
    pub identity: Identity,
    pub ready_wait: Duration,
    pub drain_wait: Duration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MountState {
    Mounted,
    Detached,
    Unknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotDetach {
    Detached,
    Busy,
}

/// The original mount, receiver owner and pin. Drop never detaches or releases.
#[must_use = "retain the original session until detach and receiver join are established"]
pub struct SnapshotSession {
    pin: SnapshotPin,
    directory: PathBuf,
    directory_identity: (u64, u64),
    directory_fd: fs::File,
    entry: Option<MountEntry>,
    state: MountState,
    device: Option<OwnedFd>,
    owner: Option<JoinHandle<SessionOutcome>>,
    monitor: Option<SessionMonitor>,
    negotiation: Arc<OnceLock<Negotiation>>,
    failure: Arc<Mutex<Option<WorkspaceError>>>,
    drain_wait: Duration,
}
pub struct SnapshotAttachFailure {
    pub cause: io::Error,
    /// Present after the mount succeeded; this original owner must be kept.
    pub session: Option<Box<SnapshotSession>>,
    /// A pre-mount directory removal failure, if cleanup was attempted.
    pub directory_cleanup: Option<io::Error>,
}
impl fmt::Debug for SnapshotAttachFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotAttachFailure")
            .field("cause", &self.cause)
            .field("session_retained", &self.session.is_some())
            .field("directory_cleanup", &self.directory_cleanup)
            .finish()
    }
}
impl fmt::Display for SnapshotAttachFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for SnapshotAttachFailure {}

/// Every created receiver and the exact mount have stopped; pin release is a
/// separate caller action after the original external consumer also ended.
pub struct SnapshotDrained {
    pin: SnapshotPin,
    entry: Option<MountEntry>,
    outcome: Option<thread::Result<SessionOutcome>>,
    source_failure: Option<WorkspaceError>,
    directory: PathBuf,
    removed: io::Result<()>,
}
impl SnapshotDrained {
    pub const fn pin(&self) -> SnapshotPin {
        self.pin
    }
    pub fn entry(&self) -> Option<&MountEntry> {
        self.entry.as_ref()
    }
    pub fn outcome(&self) -> Option<&thread::Result<SessionOutcome>> {
        self.outcome.as_ref()
    }
    pub fn source_failure(&self) -> Option<&WorkspaceError> {
        self.source_failure.as_ref()
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn removed(&self) -> &io::Result<()> {
        &self.removed
    }
    pub fn is_clean(&self) -> bool {
        self.outcome
            .as_ref()
            .is_some_and(|outcome| matches!(outcome, Ok(value) if value.is_clean()))
            && self.source_failure.is_none()
            && self.removed.is_ok()
    }
}
pub struct SnapshotDrainFailure {
    pub cause: io::Error,
    pub session: Box<SnapshotSession>,
}
impl fmt::Debug for SnapshotDrainFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotDrainFailure")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for SnapshotDrainFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for SnapshotDrainFailure {}

impl SnapshotSession {
    /// Creates one new mount and waits for its original receiver to serve.
    /// A post-mount failure returns that exact owner; it never retries or
    /// releases the captured pin.
    pub fn attach(
        config: SnapshotConfig,
        source: Arc<dyn SnapshotSource>,
    ) -> Result<Self, Box<SnapshotAttachFailure>> {
        let before = |cause, directory_cleanup| {
            Box::new(SnapshotAttachFailure {
                cause,
                session: None,
                directory_cleanup,
            })
        };
        let pin = source.pin();
        let filesystem = SnapshotFilesystem::new(source, config.identity)
            .map_err(|cause| before(cause, None))?;
        let negotiation = filesystem.negotiation();
        let failure = filesystem.failure();
        syscalls::create_directory(&config.directory).map_err(|cause| before(cause, None))?;
        let directory_fd = match fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open(&config.directory)
        {
            Ok(file) => file,
            Err(cause) => return Err(before(cause, None)),
        };
        let metadata = match directory_fd.metadata() {
            Ok(metadata) if metadata.is_dir() => metadata,
            Ok(_) => {
                return Err(before(
                    io::Error::other("snapshot directory identity"),
                    None,
                ))
            }
            Err(cause) => return Err(before(cause, None)),
        };
        let directory_identity = (metadata.dev(), metadata.ino());
        let device = match syscalls::open_device() {
            Ok(device) => device,
            Err(cause) => {
                return Err(before(
                    cause,
                    remove_owned(&config.directory, directory_identity).err(),
                ))
            }
        };
        let target = PathBuf::from(format!("/proc/self/fd/{}", directory_fd.as_raw_fd()));
        if let Err(errno) =
            syscalls::attach_readonly(&device, &target, config.owner_uid, config.owner_gid)
        {
            return Err(before(
                errno.into(),
                remove_owned(&config.directory, directory_identity).err(),
            ));
        }
        let mut session = Self {
            pin,
            directory: config.directory,
            directory_identity,
            directory_fd,
            entry: None,
            state: MountState::Mounted,
            device: Some(device),
            owner: None,
            monitor: None,
            negotiation,
            failure,
            drain_wait: config.drain_wait,
        };
        let entry = match syscalls::mount_entry(&session.directory) {
            Ok(entry)
                if entry.filesystem == "fuse"
                    && entry.source == "layerfs"
                    && entry.options.split(',').any(|option| option == "ro") =>
            {
                entry
            }
            Ok(_) => {
                return Err(session.retained(io::Error::other("snapshot read-only mount identity")))
            }
            Err(cause) => return Err(session.retained(cause)),
        };
        session.entry = Some(entry);
        let mut options = Config::default();
        options.acl = SessionACL::All;
        options.n_threads = Some(1);
        let device = session.device.take().expect("original snapshot device");
        let kernel = match Session::from_fd(filesystem, device, SessionACL::All, options) {
            Ok(kernel) => kernel,
            Err(cause) => return Err(session.retained(cause)),
        };
        let runner = kernel.into_runner();
        let monitor = runner.monitor();
        session.monitor = Some(monitor.clone());
        match thread::Builder::new()
            .name(format!("layerfs-snapshot-{}", pin.request()))
            .spawn(move || runner.run())
        {
            Ok(owner) => session.owner = Some(owner),
            Err(cause) => return Err(session.retained(cause)),
        }
        let ready = monitor.wait_ready(config.ready_wait);
        if ready.phase != SessionPhase::Serving
            || ready.entered != 1
            || session.negotiation.get().is_none()
        {
            return Err(session.retained(io::Error::other(format!(
                "snapshot receiver not serving: {ready:?}"
            ))));
        }
        Ok(session)
    }
    fn retained(self, cause: io::Error) -> Box<SnapshotAttachFailure> {
        Box::new(SnapshotAttachFailure {
            cause,
            session: Some(Box::new(self)),
            directory_cleanup: None,
        })
    }
    pub const fn pin(&self) -> SnapshotPin {
        self.pin
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn entry(&self) -> Option<&MountEntry> {
        self.entry.as_ref()
    }

    /// The one plain detach. A busy answer is reversible; any other error
    /// retains unknown disposition and cannot be resent by this session.
    pub fn detach(&mut self) -> Result<SnapshotDetach, Errno> {
        if self.state != MountState::Mounted {
            return Err(Errno::EINVAL);
        }
        let expected = self.entry.as_ref().ok_or(Errno::ESTALE)?;
        let held = self.directory_fd.metadata().map_err(|_| Errno::ESTALE)?;
        if (held.dev(), held.ino()) != self.directory_identity {
            return Err(Errno::ESTALE);
        }
        if syscalls::mount_entry(&self.directory).map_err(|_| Errno::ESTALE)? != *expected {
            return Err(Errno::ESTALE);
        }
        self.state = MountState::Unknown;
        match syscalls::detach(&self.directory) {
            Ok(()) => {
                self.state = MountState::Detached;
                Ok(SnapshotDetach::Detached)
            }
            Err(Errno::EBUSY) => {
                self.state = MountState::Mounted;
                Ok(SnapshotDetach::Busy)
            }
            Err(error) => Err(error),
        }
    }
    /// Only an acknowledged detach followed by all original joins yields a
    /// drained receipt. Timeout retains this exact session and its pin.
    pub fn drain(mut self) -> Result<Box<SnapshotDrained>, Box<SnapshotDrainFailure>> {
        if self.state != MountState::Detached {
            return Err(self.undrained(io::Error::other("snapshot mount not detached")));
        }
        let deadline = Instant::now() + self.drain_wait;
        if let Some(monitor) = &self.monitor {
            let mut loops = monitor.snapshot();
            while loops.phase != SessionPhase::Joined {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(self.undrained(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "snapshot receiver join pending",
                    )));
                }
                loops = monitor.wait_for_change(loops.revision, left);
            }
        }
        while self
            .owner
            .as_ref()
            .is_some_and(|owner| !owner.is_finished())
        {
            if Instant::now() >= deadline {
                return Err(self.undrained(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "snapshot owner join pending",
                )));
            }
            thread::sleep(Duration::from_millis(1));
        }
        let outcome = self.owner.take().map(JoinHandle::join);
        drop(self.device.take());
        let source_failure = self
            .failure
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        let removed = remove_owned(&self.directory, self.directory_identity);
        Ok(Box::new(SnapshotDrained {
            pin: self.pin,
            entry: self.entry,
            outcome,
            source_failure,
            directory: self.directory,
            removed,
        }))
    }
    fn undrained(self, cause: io::Error) -> Box<SnapshotDrainFailure> {
        Box::new(SnapshotDrainFailure {
            cause,
            session: Box::new(self),
        })
    }
}

fn remove_owned(path: &Path, expected: (u64, u64)) -> io::Result<()> {
    let current = fs::symlink_metadata(path)?;
    if !current.is_dir()
        || current.file_type().is_symlink()
        || (current.dev(), current.ino()) != expected
    {
        return Err(io::Error::other("snapshot directory identity changed"));
    }
    fs::remove_dir(path)
}
