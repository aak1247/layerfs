//! Checked private directory, cooperative owner lock and append-only records.
use super::{
    ledger::{decode_entry, decode_head, entry_bytes, head_bytes, Head, ENTRY_LEN, HEAD_LEN},
    record::{owner_bytes, Authority, WorkspaceRecord, OWNER_LEN},
};
use layerfs_history::WorkspaceId;
use nix::{
    fcntl::{openat, renameat, Flock, FlockArg, OFlag},
    sys::{stat::Mode, statfs},
};
use std::{
    ffi::OsStr,
    fmt, fs,
    fs::{File, OpenOptions},
    io,
    io::Write,
    os::{
        fd::AsRawFd,
        unix::fs::{FileExt, MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
};

const OWNER: &str = "roster.owner";
const JOURNAL: &str = "roster.ledger";
const HEAD: &str = "roster.head";
const PENDING: &str = "roster.head.pending";

/// Original admission or durability failure; uncertain writes are not replayed.
#[derive(Debug)]
pub enum RosterError {
    Io(io::Error),
    Invalid(&'static str),
    AlreadyExists,
    Uncertain(Arc<RosterError>),
    Retained(Arc<RosterError>),
}
impl fmt::Display for RosterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for RosterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(cause) => Some(cause),
            Self::Uncertain(cause) | Self::Retained(cause) => Some(cause.as_ref()),
            _ => None,
        }
    }
}
impl From<io::Error> for RosterError {
    fn from(cause: io::Error) -> Self {
        Self::Io(cause)
    }
}

/// Exclusive same-kernel custody of a private, dedicated discovery directory.
/// All owners must cooperate with this lock; old FUSE mounts need a separate fence.
pub struct Roster {
    path: PathBuf,
    directory: Option<File>,
    owner: Option<Flock<File>>,
    journal: Option<File>,
    head: Head,
    retained: Vec<File>,
    authority: Authority,
    failure: Option<Arc<RosterError>>,
}

impl Roster {
    /// The authority supplied when this catalog was created or opened.
    pub fn authority(&self) -> Authority {
        self.authority
    }

    /// Initializes an empty directory without replacing or deleting any entry.
    /// The caller creates and synchronizes the directory and its parent first.
    pub fn create(path: &Path, authority: Authority) -> Result<Self, RosterError> {
        let expected = owner_bytes(authority)?;
        let (path, directory) = open_directory(path)?;
        if fs::read_dir(fd_path(&directory))?
            .next()
            .transpose()?
            .is_some()
        {
            return Err(RosterError::Invalid("roster directory is not empty"));
        }
        let file = match open_file(&directory, OsStr::new(OWNER), true) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(RosterError::AlreadyExists)
            }
            Err(error) => return Err(RosterError::Uncertain(Arc::new(error.into()))),
        };
        let mut owner = Flock::lock(file, FlockArg::LockExclusiveNonblock)
            .map_err(|(_, error)| RosterError::Uncertain(Arc::new(errno(error).into())))?;
        let attempted = (|| {
            check_directory(&path, &directory)?;
            check_file(&directory, OsStr::new(OWNER), &owner)?;
            owner.write_all(&expected)?;
            owner.sync_all()?;
            check_file(&directory, OsStr::new(OWNER), &owner)?;
            let journal = open_file(&directory, OsStr::new(JOURNAL), true)?;
            journal.sync_all()?;
            check_file(&directory, OsStr::new(JOURNAL), &journal)?;
            let mut head = open_file(&directory, OsStr::new(HEAD), true)?;
            head.write_all(&head_bytes(authority, Head::empty()))?;
            head.sync_all()?;
            check_file(&directory, OsStr::new(HEAD), &head)?;
            directory.sync_all()?;
            check_directory(&path, &directory)?;
            check_file(&directory, OsStr::new(OWNER), &owner)?;
            check_file(&directory, OsStr::new(JOURNAL), &journal)?;
            Ok(journal)
        })();
        let journal = match attempted {
            Ok(journal) => journal,
            Err(cause) => {
                // An uncertain creation keeps the original lock until process exit.
                std::mem::forget(owner);
                std::mem::forget(directory);
                return Err(RosterError::Uncertain(Arc::new(cause)));
            }
        };
        let mut roster = Self {
            path,
            directory: Some(directory),
            owner: Some(owner),
            journal: Some(journal),
            head: Head::empty(),
            retained: Vec::new(),
            authority,
            failure: None,
        };
        if let Err(cause) = roster.verify() {
            return Err(roster.retain(cause, true));
        }
        let mut found = false;
        if let Err(cause) = roster.visit(|_| found = true) {
            return Err(roster.retain(cause, true));
        }
        if found {
            return Err(roster.retain(RosterError::Invalid("roster directory changed"), true));
        }
        Ok(roster)
    }

    /// Opens the exact original catalog. This is discovery, not old-mount fencing.
    pub fn open(path: &Path, authority: Authority) -> Result<Self, RosterError> {
        owner_bytes(authority)?;
        let (path, directory) = open_directory(path)?;
        let file = open_file(&directory, OsStr::new(OWNER), false)?;
        check_file(&directory, OsStr::new(OWNER), &file)?;
        let owner = Flock::lock(file, FlockArg::LockExclusiveNonblock)
            .map_err(|(_, error)| RosterError::Io(errno(error)))?;
        let mut roster = Self {
            path,
            directory: Some(directory),
            owner: Some(owner),
            journal: None,
            head: Head::empty(),
            retained: Vec::new(),
            authority,
            failure: None,
        };
        roster.verify_owner()?;
        let directory = roster
            .directory
            .as_ref()
            .expect("roster directory retained");
        let journal = match open_journal(directory) {
            Ok(journal) => journal,
            Err(cause) => return Err(roster.retain(cause.into(), false)),
        };
        roster.journal = Some(journal);
        let head = match read_head(directory, authority) {
            Ok(head) => head,
            Err(cause) => return Err(roster.retain(cause, false)),
        };
        roster.head = head;
        if let Err(cause) = roster.verify() {
            return Err(roster.retain(cause, false));
        }
        // Never admit another intent while an earlier record is incomplete.
        roster.visit(|_| {})?;
        Ok(roster)
    }

    /// Persist this identity before creating its Overlay or exposing a token.
    /// Existing identities are never overwritten, removed, or automatically reused.
    pub fn record(&mut self, record: WorkspaceRecord) -> Result<(), RosterError> {
        self.ensure()?;
        let name = record.file_name();
        let (next, bytes) = entry_bytes(self.authority, self.head, record)?;
        let offset = self.head.size()?;
        let directory = self.directory.as_ref().expect("roster directory retained");
        let journal = self.journal.as_ref().expect("roster journal retained");
        let mut file = match open_file(directory, OsStr::new(&name), true) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                self.visit(|_| {})?;
                return Err(RosterError::AlreadyExists);
            }
            Err(error) => return Err(self.retain(error.into(), true)),
        };
        let mut pending = None;
        let mut original_head = None;
        let attempted = (|| {
            check_file(directory, OsStr::new(&name), &file)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            check_file(directory, OsStr::new(&name), &file)?;
            if file.metadata()?.len() != ENTRY_LEN as u64 {
                return Err(RosterError::Invalid("roster record size"));
            }
            let mut verified = [0; ENTRY_LEN];
            file.read_exact_at(&mut verified, 0)?;
            if verified.as_slice() != bytes.as_slice() {
                return Err(RosterError::Invalid("roster record changed"));
            }
            journal.write_all_at(&bytes, offset)?;
            journal.sync_all()?;
            pending = Some(open_file(directory, OsStr::new(PENDING), true)?);
            let new_head = pending.as_mut().expect("pending head retained");
            new_head.write_all(&head_bytes(self.authority, next))?;
            new_head.sync_all()?;
            check_file(directory, OsStr::new(PENDING), new_head)?;
            original_head = Some(checked_head(directory, self.authority, self.head)?);
            self.verify_base()?;
            check_file(
                directory,
                OsStr::new(HEAD),
                original_head.as_ref().expect("original head retained"),
            )?;
            check_file(
                directory,
                OsStr::new(PENDING),
                pending.as_ref().expect("pending head retained"),
            )?;
            renameat(directory, PENDING, directory, HEAD).map_err(errno)?;
            Ok(())
        })();
        if let Err(cause) = attempted {
            self.retained.push(file);
            self.retained.extend(pending);
            self.retained.extend(original_head);
            return Err(self.retain(cause, true));
        }
        self.head = next;
        let synchronized = (|| {
            let directory = self.directory.as_ref().expect("roster directory retained");
            directory.sync_all()?;
            self.verify()?;
            check_file(directory, OsStr::new(&name), &file)?;
            let mut verified_file = [0; ENTRY_LEN];
            let mut verified_ledger = [0; ENTRY_LEN];
            file.read_exact_at(&mut verified_file, 0)?;
            self.journal
                .as_ref()
                .expect("roster journal retained")
                .read_exact_at(&mut verified_ledger, offset)?;
            if verified_file.as_slice() != bytes.as_slice()
                || verified_ledger.as_slice() != bytes.as_slice()
            {
                return Err(RosterError::Invalid("roster record or ledger changed"));
            }
            Ok(())
        })();
        if let Err(cause) = synchronized {
            self.retained.push(file);
            self.retained.extend(pending);
            self.retained.extend(original_head);
            return Err(self.retain(cause, true));
        }
        Ok(())
    }

    /// Visit each complete record without imposing a Workspace count limit.
    /// An error means the scan was incomplete; visited entries are tentative
    /// evidence. The callback must not start recovery or other side effects.
    /// A complete scan is not authorization or an old-mount fencing proof.
    pub fn visit(&mut self, mut receive: impl FnMut(WorkspaceRecord)) -> Result<(), RosterError> {
        self.ensure()?;
        let result = (|| {
            let directory = self.directory.as_ref().expect("roster directory retained");
            let journal = self.journal.as_ref().expect("roster journal retained");
            let mut cursor = Head::empty();
            while cursor.count < self.head.count {
                let mut bytes = [0; ENTRY_LEN];
                journal.read_exact_at(&mut bytes, cursor.size()?)?;
                let (next, record) = decode_entry(self.authority, cursor, &bytes)?;
                let name = record.file_name();
                let file = open_file(directory, OsStr::new(&name), false)?;
                check_file(directory, OsStr::new(&name), &file)?;
                if file.metadata()?.len() != ENTRY_LEN as u64 {
                    return Err(RosterError::Invalid("roster record size"));
                }
                let mut saved = [0; ENTRY_LEN];
                file.read_exact_at(&mut saved, 0)?;
                if saved != bytes {
                    return Err(RosterError::Invalid("roster record differs from ledger"));
                }
                check_file(directory, OsStr::new(&name), &file)?;
                self.verify()?;
                receive(record);
                cursor = next;
            }
            if cursor != self.head {
                return Err(RosterError::Invalid("roster ledger head"));
            }
            let mut count = 0u64;
            for entry in fs::read_dir(fd_path(directory))? {
                let name = entry?.file_name();
                if name == OWNER || name == JOURNAL || name == HEAD {
                    continue;
                }
                let text = name
                    .to_str()
                    .ok_or(RosterError::Invalid("roster record name"))?;
                if !text.starts_with("workspace-") || !text.ends_with(".record") {
                    return Err(RosterError::Invalid("roster record name"));
                }
                count = count
                    .checked_add(1)
                    .ok_or(RosterError::Invalid("roster record count"))?;
            }
            if count != self.head.count {
                return Err(RosterError::Invalid("roster record count"));
            }
            self.verify()
        })();
        result.map_err(|cause| self.retain(cause, false))
    }

    /// Returns one record only after the entire catalog was verified.
    pub fn lookup(
        &mut self,
        workspace: WorkspaceId,
    ) -> Result<Option<WorkspaceRecord>, RosterError> {
        let mut result = None;
        self.visit(|record| {
            if record.workspace == workspace {
                result = Some(record);
            }
        })?;
        Ok(result)
    }

    fn ensure(&mut self) -> Result<(), RosterError> {
        if let Some(cause) = &self.failure {
            return Err(RosterError::Retained(cause.clone()));
        }
        if let Err(cause) = self.verify() {
            return Err(self.retain(cause, false));
        }
        Ok(())
    }

    fn verify_owner(&self) -> Result<(), RosterError> {
        let directory = self.directory.as_ref().expect("roster directory retained");
        let owner = self.owner.as_ref().expect("roster owner retained");
        check_directory(&self.path, directory)?;
        check_file(directory, OsStr::new(OWNER), owner)?;
        if owner.metadata()?.len() != OWNER_LEN as u64 {
            return Err(RosterError::Invalid("roster owner size"));
        }
        let mut bytes = [0; OWNER_LEN];
        owner.read_exact_at(&mut bytes, 0)?;
        if bytes.as_slice() != owner_bytes(self.authority)? {
            return Err(RosterError::Invalid("roster authority mismatch"));
        }
        Ok(())
    }

    fn verify_base(&self) -> Result<(), RosterError> {
        self.verify_owner()?;
        let directory = self.directory.as_ref().expect("roster directory retained");
        let journal = self.journal.as_ref().expect("roster journal retained");
        check_file(directory, OsStr::new(JOURNAL), journal)?;
        if read_head(directory, self.authority)? != self.head {
            return Err(RosterError::Invalid("roster head changed"));
        }
        Ok(())
    }

    fn verify(&self) -> Result<(), RosterError> {
        self.verify_base()?;
        if self
            .journal
            .as_ref()
            .expect("roster journal retained")
            .metadata()?
            .len()
            != self.head.size()?
        {
            return Err(RosterError::Invalid("roster ledger length"));
        }
        Ok(())
    }

    fn retain(&mut self, cause: RosterError, uncertain: bool) -> RosterError {
        let cause = Arc::new(cause);
        self.failure = Some(cause.clone());
        if uncertain {
            RosterError::Uncertain(cause)
        } else {
            RosterError::Retained(cause)
        }
    }
}

impl Drop for Roster {
    fn drop(&mut self) {
        if self.failure.is_some() {
            for file in self.retained.drain(..) {
                std::mem::forget(file);
            }
            if let Some(journal) = self.journal.take() {
                std::mem::forget(journal);
            }
            if let Some(owner) = self.owner.take() {
                std::mem::forget(owner);
            }
            if let Some(directory) = self.directory.take() {
                std::mem::forget(directory);
            }
        }
    }
}

fn read_head(directory: &File, authority: Authority) -> Result<Head, RosterError> {
    let file = open_file(directory, OsStr::new(HEAD), false)?;
    decode_checked_head(directory, authority, &file)
}

fn checked_head(
    directory: &File,
    authority: Authority,
    expected: Head,
) -> Result<File, RosterError> {
    let file = open_file(directory, OsStr::new(HEAD), false)?;
    if decode_checked_head(directory, authority, &file)? != expected {
        return Err(RosterError::Invalid("roster head changed"));
    }
    Ok(file)
}

fn decode_checked_head(
    directory: &File,
    authority: Authority,
    file: &File,
) -> Result<Head, RosterError> {
    check_file(directory, OsStr::new(HEAD), file)?;
    if file.metadata()?.len() != HEAD_LEN as u64 {
        return Err(RosterError::Invalid("roster head size"));
    }
    let mut bytes = [0; HEAD_LEN];
    file.read_exact_at(&mut bytes, 0)?;
    let head = decode_head(authority, &bytes)?;
    check_file(directory, OsStr::new(HEAD), file)?;
    Ok(head)
}

fn open_directory(path: &Path) -> Result<(PathBuf, File), RosterError> {
    let path = path.canonicalize()?;
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
        .open(&path)?;
    check_directory(&path, &directory)?;
    let kind = statfs::fstatfs(&directory)
        .map_err(errno)?
        .filesystem_type();
    if ![
        statfs::EXT4_SUPER_MAGIC,
        statfs::XFS_SUPER_MAGIC,
        statfs::BTRFS_SUPER_MAGIC,
    ]
    .contains(&kind)
    {
        return Err(RosterError::Invalid("roster requires a local filesystem"));
    }
    Ok((path, directory))
}

fn check_directory(path: &Path, directory: &File) -> Result<(), RosterError> {
    let opened = directory.metadata()?;
    let named = fs::symlink_metadata(path)?;
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
        return Err(RosterError::Invalid("roster directory identity or privacy"));
    }
    Ok(())
}

fn check_file(directory: &File, name: &OsStr, file: &File) -> Result<(), RosterError> {
    let opened = file.metadata()?;
    let named = fs::symlink_metadata(fd_path(directory).join(name))?;
    let uid = nix::unistd::geteuid().as_raw();
    if !opened.is_file()
        || !named.is_file()
        || opened.nlink() != 1
        || named.nlink() != 1
        || opened.uid() != uid
        || named.uid() != uid
        || opened.mode() & 0o077 != 0
        || named.mode() & 0o077 != 0
        || opened.mode() & 0o600 != 0o600
        || named.mode() & 0o600 != 0o600
        || opened.dev() != named.dev()
        || opened.ino() != named.ino()
    {
        return Err(RosterError::Invalid("roster file identity or privacy"));
    }
    Ok(())
}

fn open_file(directory: &File, name: &OsStr, create: bool) -> io::Result<File> {
    let flags = if create {
        OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL
    } else {
        OFlag::O_RDONLY
    };
    openat(
        directory,
        name,
        flags | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::S_IRUSR | Mode::S_IWUSR,
    )
    .map(File::from)
    .map_err(errno)
}

fn open_journal(directory: &File) -> io::Result<File> {
    openat(
        directory,
        JOURNAL,
        OFlag::O_RDWR | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(errno)
}

fn fd_path(directory: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
}
fn errno(error: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(error as i32)
}
