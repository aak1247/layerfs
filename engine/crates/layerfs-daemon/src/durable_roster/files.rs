//! Checked private directory, cooperative owner lock and append-only records.
use super::{
    ledger::{decode_entry, decode_head, entry_bytes, head_bytes, Head, ENTRY_LEN, HEAD_LEN},
    record::{
        decode_record, identity_name, owner_bytes, Authority, Closing, Event, WorkspaceRecord,
        OWNER_LEN, RECORD_LEN,
    },
};
use layerfs_bridge::control::{Reply, WorkspaceToken};
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
    /// A close was durably started but has no recorded terminal receipt.
    PendingClose(Closing),
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
    pending_close: Option<Closing>,
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
        let (path, directory) = open_directory(path)?;
        Self::create_opened(path, directory, authority)
    }

    /// Creates through an already checked directory descriptor. This prevents
    /// a pathname swap from redirecting the initial writes to another inode.
    pub(crate) fn create_opened(
        path: PathBuf,
        directory: File,
        authority: Authority,
    ) -> Result<Self, RosterError> {
        let expected = owner_bytes(authority)?;
        check_directory(&path, &directory)?;
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
            pending_close: None,
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
            pending_close: None,
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
        // A complete but unresolved close remains inspectable under the lock.
        roster.pending_close = match roster.scan_structure() {
            Ok(pending) => pending,
            Err(cause) => return Err(roster.retain(cause, false)),
        };
        Ok(roster)
    }

    /// Persist this identity before creating its Overlay or exposing a token.
    /// Existing identities are never overwritten, removed, or automatically reused.
    pub fn record(&mut self, record: WorkspaceRecord) -> Result<(), RosterError> {
        self.ensure()?;
        self.ready()?;
        self.append(Event::Created(record))
    }

    /// Persist one exact close intent before attempting unmount or Close.
    /// The caller must first serialize admission and prove that the token is
    /// the current bound namespace. A pending intent blocks other mutations.
    pub fn begin_close(&mut self, token: WorkspaceToken) -> Result<Closing, RosterError> {
        self.ensure()?;
        self.ready()?;
        if token.namespace <= 0 {
            return Err(RosterError::Invalid("roster closing namespace"));
        }
        let name = format!("{}.record", identity_name(token.workspace));
        let record = match self.checked_marker(&name)? {
            Some((_, Event::Created(record))) if record.workspace == token.workspace => record,
            Some(_) => {
                return Err(self.retain(RosterError::Invalid("roster creation marker"), false))
            }
            None => return Err(RosterError::Invalid("unknown roster Workspace")),
        };
        let attempt = self
            .head
            .count
            .checked_add(1)
            .ok_or(RosterError::Invalid("roster sequence exhausted"))?;
        let closing = Closing {
            record,
            token,
            attempt,
        };
        if self
            .checked_marker(&Event::Closed(closing).file_name())?
            .is_some()
        {
            return Err(RosterError::AlreadyExists);
        }
        self.append(Event::Closing(closing))?;
        self.pending_close = Some(closing);
        Ok(closing)
    }

    /// Record the exact terminal reply only after its provenance is verified.
    /// A status read, missing process or old token is not an unmount receipt.
    pub fn finish_close(&mut self, reply: &Reply) -> Result<(), RosterError> {
        self.ensure()?;
        let closing = self
            .pending_close
            .ok_or(RosterError::Invalid("no pending close"))?;
        if !matches!(reply, Reply::Unmounted(token) if *token == closing.token) {
            return Err(RosterError::Invalid("close receipt does not match intent"));
        }
        match self.checked_marker(&Event::Closing(closing).file_name())? {
            Some((_, Event::Closing(saved))) if saved == closing => {}
            _ => return Err(self.retain(RosterError::Invalid("roster close intent"), false)),
        }
        self.append(Event::Closed(closing))?;
        self.pending_close = None;
        Ok(())
    }

    /// Cancel only after the Service has proven the original attempt had no
    /// terminal effect; this is not an authorization to retry an unknown one.
    pub(crate) fn cancel_close(&mut self, closing: Closing) -> Result<(), RosterError> {
        self.ensure()?;
        if self.pending_close != Some(closing) {
            return Err(RosterError::Invalid(
                "close cancellation does not match intent",
            ));
        }
        match self.checked_marker(&Event::Closing(closing).file_name())? {
            Some((_, Event::Closing(saved))) if saved == closing => {}
            _ => return Err(self.retain(RosterError::Invalid("roster close intent"), false)),
        }
        self.append(Event::Cancelled(closing))?;
        self.pending_close = None;
        Ok(())
    }

    /// Inspect an unresolved close after validating the entire catalog.
    /// Inspection does not establish the outcome of the unmount attempt.
    pub fn pending_close(&mut self) -> Result<Option<Closing>, RosterError> {
        self.checked_scan()
    }

    fn ready(&self) -> Result<(), RosterError> {
        match self.pending_close {
            Some(closing) => Err(RosterError::PendingClose(closing)),
            None => Ok(()),
        }
    }

    fn append(&mut self, event: Event) -> Result<(), RosterError> {
        let name = event.file_name();
        let (next, bytes) = entry_bytes(self.authority, self.head, event)?;
        let offset = self.head.size()?;
        let directory = self.directory.as_ref().expect("roster directory retained");
        let journal = self.journal.as_ref().expect("roster journal retained");
        let mut file = match open_file(directory, OsStr::new(&name), true) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                self.checked_scan()?;
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

    /// Visit each active record without imposing a Workspace count limit.
    /// An error means the scan was incomplete; visited entries are tentative
    /// evidence. The callback must not start recovery or other side effects.
    /// A complete scan is not authorization or an old-mount fencing proof.
    pub fn visit(&mut self, mut receive: impl FnMut(WorkspaceRecord)) -> Result<(), RosterError> {
        if let Some(closing) = self.checked_scan()? {
            return Err(RosterError::PendingClose(closing));
        }
        let result = (|| {
            let journal = self.journal.as_ref().expect("roster journal retained");
            let mut cursor = Head::empty();
            while cursor.count < self.head.count {
                let mut bytes = [0; ENTRY_LEN];
                journal.read_exact_at(&mut bytes, cursor.size()?)?;
                let (next, event) = decode_entry(self.authority, cursor, &bytes)?;
                if let Event::Created(record) = event {
                    let name = format!("{}.closed", identity_name(record.workspace));
                    if self.marker(&name)?.is_none() {
                        receive(record);
                    }
                }
                cursor = next;
            }
            self.verify()
        })();
        result.map_err(|cause| self.retain(cause, false))
    }

    fn checked_scan(&mut self) -> Result<Option<Closing>, RosterError> {
        self.ensure()?;
        let pending = match self.scan_structure() {
            Ok(value) => value,
            Err(cause) => return Err(self.retain(cause, false)),
        };
        if pending != self.pending_close {
            return Err(self.retain(RosterError::Invalid("roster close state changed"), false));
        }
        Ok(pending)
    }

    // Verify all immutable markers against their exact ledger positions before
    // interpreting an active set or exposing an unresolved close.
    fn scan_structure(&self) -> Result<Option<Closing>, RosterError> {
        let directory = self.directory.as_ref().expect("roster directory retained");
        let journal = self.journal.as_ref().expect("roster journal retained");
        let mut cursor = Head::empty();
        let mut pending = None;
        while cursor.count < self.head.count {
            let mut bytes = [0; ENTRY_LEN];
            journal.read_exact_at(&mut bytes, cursor.size()?)?;
            let (next, event) = decode_entry(self.authority, cursor, &bytes)?;
            if self.marker(&event.file_name())? != Some((next.count, event)) {
                return Err(RosterError::Invalid("roster record differs from ledger"));
            }
            match event {
                Event::Created(_) => {}
                Event::Closing(closing) => {
                    let created = self.marker(&Event::Created(closing.record).file_name())?;
                    let terminal = self.marker(&Event::Closed(closing).file_name())?;
                    if pending.is_some()
                        || (closing.attempt != 0 && closing.attempt != next.count)
                        || !matches!(created, Some((sequence, Event::Created(record))) if sequence < next.count && record == closing.record)
                        || matches!(terminal, Some((sequence, _)) if sequence < next.count)
                    {
                        return Err(RosterError::Invalid("roster close ordering"));
                    }
                    pending = Some(closing);
                }
                Event::Closed(closing) | Event::Cancelled(closing) => {
                    let intent = self.marker(&Event::Closing(closing).file_name())?;
                    if pending != Some(closing)
                        || !matches!(intent, Some((sequence, Event::Closing(saved))) if sequence < next.count && saved == closing)
                    {
                        return Err(RosterError::Invalid("roster close receipt ordering"));
                    }
                    pending = None;
                }
            }
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
            if !text.starts_with("workspace-")
                || !(text.ends_with(".record")
                    || text.ends_with(".closing")
                    || text.ends_with(".closed")
                    || text.contains(".closing-")
                    || text.contains(".cancelled-"))
            {
                return Err(RosterError::Invalid("roster record name"));
            }
            count = count
                .checked_add(1)
                .ok_or(RosterError::Invalid("roster record count"))?;
        }
        if count != self.head.count {
            return Err(RosterError::Invalid("roster record count"));
        }
        self.verify()?;
        Ok(pending)
    }

    fn marker(&self, name: &str) -> Result<Option<(u64, Event)>, RosterError> {
        let directory = self.directory.as_ref().expect("roster directory retained");
        let file = match open_file(directory, OsStr::new(name), false) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        check_file(directory, OsStr::new(name), &file)?;
        if file.metadata()?.len() != ENTRY_LEN as u64 {
            return Err(RosterError::Invalid("roster record size"));
        }
        let mut bytes = [0; ENTRY_LEN];
        file.read_exact_at(&mut bytes, 0)?;
        let sequence = u64::from_be_bytes(bytes[..8].try_into().expect("fixed sequence"));
        if sequence == 0 || sequence > self.head.count {
            return Err(RosterError::Invalid("roster marker sequence"));
        }
        let event = decode_record(self.authority, &bytes[8..8 + RECORD_LEN])?;
        if event.file_name() != name {
            return Err(RosterError::Invalid("roster marker identity"));
        }
        let mut ledger = [0; ENTRY_LEN];
        self.journal
            .as_ref()
            .expect("roster journal retained")
            .read_exact_at(&mut ledger, (sequence - 1) * ENTRY_LEN as u64)?;
        if ledger != bytes {
            return Err(RosterError::Invalid("roster marker ledger mismatch"));
        }
        check_file(directory, OsStr::new(name), &file)?;
        Ok(Some((sequence, event)))
    }

    fn checked_marker(&mut self, name: &str) -> Result<Option<(u64, Event)>, RosterError> {
        self.marker(name).map_err(|cause| self.retain(cause, false))
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

    /// Exact-marker lookup after this locked catalog's complete opening scan.
    /// Unlike `lookup`, this does not rescan unrelated Workspace markers.
    pub(crate) fn active_record(
        &mut self,
        workspace: WorkspaceId,
    ) -> Result<Option<WorkspaceRecord>, RosterError> {
        self.ensure()?;
        self.ready()?;
        let name = format!("{}.record", identity_name(workspace));
        let record = match self.checked_marker(&name)? {
            Some((_, Event::Created(record))) if record.workspace == workspace => record,
            Some(_) => {
                return Err(self.retain(RosterError::Invalid("roster creation marker"), false))
            }
            None => return Ok(None),
        };
        let name = format!("{}.closed", identity_name(workspace));
        match self.checked_marker(&name)? {
            None => Ok(Some(record)),
            Some((_, Event::Closed(closing))) if closing.record == record => Ok(None),
            Some(_) => Err(self.retain(RosterError::Invalid("roster terminal marker"), false)),
        }
    }

    /// Recheck the original directory, owner, ledger and head under its lock.
    pub(crate) fn check_live(&mut self) -> Result<(), RosterError> {
        self.ensure()
    }

    /// Retain this catalog lock when an adjacent durable owner loses identity.
    pub(crate) fn quarantine(&mut self, reason: &'static str) -> RosterError {
        if let Some(cause) = &self.failure {
            return RosterError::Retained(cause.clone());
        }
        self.retain(RosterError::Invalid(reason), false)
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
