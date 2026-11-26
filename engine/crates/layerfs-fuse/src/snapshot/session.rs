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
    io::Read,
    os::{
        fd::{AsRawFd, OwnedFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Component, Path, PathBuf},
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
    scope: MountScope,
    mounted_fd: Option<fs::File>,
    entry: Option<MountEntry>,
    state: MountState,
    device: Option<OwnedFd>,
    owner: Option<JoinHandle<SessionOutcome>>,
    monitor: Option<SessionMonitor>,
    negotiation: Arc<OnceLock<Negotiation>>,
    failure: Arc<Mutex<Option<WorkspaceError>>>,
    drain_wait: Duration,
}

/// A retained parent and namespace, plus a source name unique to this one
/// mount attempt. No caller-provided source string authorizes an observation.
struct MountScope {
    parent: PathBuf,
    parent_fd: fs::File,
    parent_identity: (u64, u64),
    namespace_fd: fs::File,
    namespace_identity: (u64, u64),
    leaf: std::ffi::OsString,
    source: String,
}
impl MountScope {
    fn prepare(directory: &Path) -> io::Result<Self> {
        if !directory.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot directory must be absolute",
            ));
        }
        let Some(Component::Normal(leaf)) = directory.components().next_back() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot directory leaf",
            ));
        };
        let parent = fs::canonicalize(directory.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "snapshot directory parent")
        })?)?;
        let parent_fd = fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open(&parent)?;
        let parent_metadata = parent_fd.metadata()?;
        let namespace_fd = fs::File::open("/proc/thread-self/ns/mnt")?;
        let namespace_metadata = namespace_fd.metadata()?;
        let mut random = [0u8; 16];
        fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
        let mut source = String::from("layerfs-snapshot-");
        for byte in random {
            use std::fmt::Write;
            write!(&mut source, "{byte:02x}").expect("writing a string cannot fail");
        }
        let scope = Self {
            parent,
            parent_fd,
            parent_identity: (parent_metadata.dev(), parent_metadata.ino()),
            namespace_fd,
            namespace_identity: (namespace_metadata.dev(), namespace_metadata.ino()),
            leaf: leaf.to_os_string(),
            source,
        };
        scope.verify()?;
        Ok(scope)
    }
    fn directory(&self) -> PathBuf {
        self.parent.join(&self.leaf)
    }
    fn held_directory(&self) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}", self.parent_fd.as_raw_fd())).join(&self.leaf)
    }
    fn verify(&self) -> io::Result<()> {
        let held_parent = self.parent_fd.metadata()?;
        let named_parent = fs::symlink_metadata(&self.parent)?;
        let held_namespace = self.namespace_fd.metadata()?;
        let current_namespace = fs::metadata("/proc/thread-self/ns/mnt")?;
        if !named_parent.is_dir()
            || (held_parent.dev(), held_parent.ino()) != self.parent_identity
            || (named_parent.dev(), named_parent.ino()) != self.parent_identity
            || (held_namespace.dev(), held_namespace.ino()) != self.namespace_identity
            || (current_namespace.dev(), current_namespace.ino()) != self.namespace_identity
        {
            return Err(io::Error::other(
                "snapshot mount parent or namespace changed",
            ));
        }
        Ok(())
    }
}

fn mount_id(file: &fs::File) -> io::Result<u64> {
    let info = fs::read_to_string(format!("/proc/self/fdinfo/{}", file.as_raw_fd()))?;
    let id = info
        .lines()
        .filter_map(|line| line.strip_prefix("mnt_id:").map(str::trim))
        .collect::<Vec<_>>();
    if id.len() != 1 {
        return Err(io::Error::other("snapshot mount descriptor identity"));
    }
    id[0]
        .parse()
        .map_err(|_| io::Error::other("snapshot mount descriptor identity"))
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
        let scope = MountScope::prepare(&config.directory).map_err(|cause| before(cause, None))?;
        let pin = source.pin();
        let filesystem = SnapshotFilesystem::new(source, config.identity)
            .map_err(|cause| before(cause, None))?;
        let negotiation = filesystem.negotiation();
        let failure = filesystem.failure();
        scope.verify().map_err(|cause| before(cause, None))?;
        syscalls::create_directory(&scope.held_directory()).map_err(|cause| before(cause, None))?;
        let directory_fd = match fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open(scope.held_directory())
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
        let named = match fs::symlink_metadata(scope.held_directory()) {
            Ok(named) => named,
            Err(cause) => return Err(before(cause, None)),
        };
        if !named.is_dir() || (named.dev(), named.ino()) != directory_identity {
            return Err(before(
                io::Error::other("snapshot directory identity changed"),
                None,
            ));
        }
        if let Err(cause) = scope.verify() {
            return Err(before(cause, None));
        }
        let device = match syscalls::open_device() {
            Ok(device) => device,
            Err(cause) => {
                return Err(before(
                    cause,
                    remove_owned(&scope.held_directory(), directory_identity).err(),
                ))
            }
        };
        let target = PathBuf::from(format!("/proc/self/fd/{}", directory_fd.as_raw_fd()));
        if let Err(errno) = syscalls::attach_readonly(
            &device,
            &target,
            config.owner_uid,
            config.owner_gid,
            &scope.source,
        ) {
            return Err(before(
                errno.into(),
                remove_owned(&scope.held_directory(), directory_identity).err(),
            ));
        }
        let mut session = Self {
            pin,
            directory: scope.directory(),
            directory_identity,
            directory_fd,
            scope,
            mounted_fd: None,
            entry: None,
            state: MountState::Mounted,
            device: Some(device),
            owner: None,
            monitor: None,
            negotiation,
            failure,
            drain_wait: config.drain_wait,
        };
        let mounted_fd = fs::OpenOptions::new()
            .read(true)
            .custom_flags(
                nix::libc::O_PATH
                    | nix::libc::O_DIRECTORY
                    | nix::libc::O_NOFOLLOW
                    | nix::libc::O_CLOEXEC,
            )
            .open(session.scope.held_directory());
        match mounted_fd {
            Ok(file) => session.mounted_fd = Some(file),
            Err(cause) => return Err(session.retained(cause)),
        }
        if let Err(cause) = session.observe_mount() {
            return Err(session.retained(cause));
        }
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
        match thread::Builder::new()
            .name(format!("layerfs-snapshot-{}", pin.request()))
            .spawn(move || runner.run())
        {
            Ok(owner) => {
                session.monitor = Some(monitor.clone());
                session.owner = Some(owner);
            }
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

    /// Read the original mount again after a failed observation. Only the
    /// held namespace, parent, unique source and mounted descriptor can bind
    /// the first receipt; a later observation must match that whole receipt.
    pub fn observe_mount(&mut self) -> io::Result<&MountEntry> {
        if self.state != MountState::Mounted {
            return Err(io::Error::other("snapshot mount is not mounted"));
        }
        self.scope.verify()?;
        let directory = self.directory_fd.metadata()?;
        if (directory.dev(), directory.ino()) != self.directory_identity {
            return Err(io::Error::other("snapshot directory identity changed"));
        }
        let expected_id = match (&self.mounted_fd, &self.entry) {
            (Some(file), _) => mount_id(file)?,
            (None, Some(entry)) => entry.id,
            (None, None) => {
                return Err(io::Error::other(
                    "original snapshot mount descriptor absent",
                ))
            }
        };
        let observed = syscalls::mount_entry(&self.directory)?;
        if observed.id != expected_id
            || observed.filesystem != "fuse"
            || observed.source != self.scope.source
            || !observed.options.split(',').any(|option| option == "ro")
            || self.entry.as_ref().is_some_and(|entry| entry != &observed)
        {
            return Err(io::Error::other(
                "snapshot read-only mount identity changed",
            ));
        }
        self.entry = Some(observed);
        Ok(self
            .entry
            .as_ref()
            .expect("original mount was just observed"))
    }

    /// The one plain detach. A busy answer is reversible; any other error
    /// retains unknown disposition and cannot be resent by this session.
    pub fn detach(&mut self) -> Result<SnapshotDetach, Errno> {
        if self.state != MountState::Mounted {
            return Err(Errno::EINVAL);
        }
        self.scope.verify().map_err(|_| Errno::ESTALE)?;
        let expected = self.entry.as_ref().ok_or(Errno::ESTALE)?;
        let held = self.directory_fd.metadata().map_err(|_| Errno::ESTALE)?;
        if (held.dev(), held.ino()) != self.directory_identity {
            return Err(Errno::ESTALE);
        }
        if syscalls::mount_entry(&self.directory).map_err(|_| Errno::ESTALE)? != *expected {
            return Err(Errno::ESTALE);
        }
        if let Some(mounted_fd) = &self.mounted_fd {
            if mount_id(mounted_fd).map_err(|_| Errno::ESTALE)? != expected.id {
                return Err(Errno::ESTALE);
            }
        }
        // An O_PATH reference itself keeps a mount busy. Its exact mount ID was
        // just checked and remains fixed in entry for any reversible EBUSY.
        drop(self.mounted_fd.take());
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
        let removed = self
            .scope
            .verify()
            .and_then(|()| remove_owned(&self.scope.held_directory(), self.directory_identity));
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
