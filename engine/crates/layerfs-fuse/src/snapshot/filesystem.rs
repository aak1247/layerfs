//! Kernel callbacks over one immutable, already-owned snapshot source.
use super::SnapshotSource;
use crate::attributes::Identity;
use fuser::{
    AccessFlags, Errno, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo,
    KernelConfig, LockOwner, OpenAccMode, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, Request,
};
use layerfs_content::{filesystem::PathName, object::inode_leaf::InodeKind, ContentError};
use layerfs_overlay::OverlayError;
use layerfs_workspace::{ViewStat, WorkspaceError};
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    io,
    os::unix::ffi::OsStrExt,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

const TTL: Duration = Duration::from_secs(60);

#[derive(Default)]
struct Directory {
    serial: u64,
    parent: u64,
    next: u64,
    offsets: BTreeMap<u64, Vec<u8>>,
    names: BTreeMap<Vec<u8>, u64>,
}
#[derive(Default)]
struct State {
    next: u64,
    parents: BTreeMap<u64, u64>,
    files: BTreeMap<u64, u64>,
    directories: BTreeMap<u64, Directory>,
}
impl State {
    fn handle(&mut self) -> Result<u64, Errno> {
        self.next = self.next.checked_add(1).ok_or(Errno::EOVERFLOW)?;
        Ok(self.next)
    }
    fn parent(&self, serial: u64) -> Result<u64, Errno> {
        self.parents.get(&serial).copied().ok_or(Errno::ESTALE)
    }
}

/// This connection's read-only callbacks. Its source is the exact retained
/// pin, never the active Workspace. No callback releases the pin.
pub struct SnapshotFilesystem {
    source: Arc<dyn SnapshotSource>,
    identity: Identity,
    generation: Generation,
    state: Mutex<State>,
    negotiation: Arc<OnceLock<crate::mount::Negotiation>>,
    failure: Arc<Mutex<Option<WorkspaceError>>>,
}
impl SnapshotFilesystem {
    pub fn new(source: Arc<dyn SnapshotSource>, identity: Identity) -> io::Result<Self> {
        let pin = source.pin();
        if identity.root == 0
            || identity.root != source.root_serial()
            || pin.source().engine() != pin.capture().engine()
            || pin.source().root() != pin.reader().root()
            || pin.source().installed_floor() != pin.reader().installed_floor()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot identity",
            ));
        }
        let mut state = State::default();
        state.parents.insert(identity.root, identity.root);
        Ok(Self {
            source,
            identity,
            generation: Generation(pin.reader().owner_id()),
            state: Mutex::new(state),
            negotiation: Arc::default(),
            failure: Arc::default(),
        })
    }
    pub fn negotiation(&self) -> Arc<OnceLock<crate::mount::Negotiation>> {
        self.negotiation.clone()
    }
    /// The first original source error other than a normal missing name or
    /// invalid name width remains owned through the session's disposition.
    pub fn failure(&self) -> Arc<Mutex<Option<WorkspaceError>>> {
        self.failure.clone()
    }
    fn serial(&self, inode: INodeNo) -> Result<u64, Errno> {
        self.identity.serial(inode)
    }
    fn stat(&self, serial: u64) -> Result<ViewStat, Errno> {
        self.source.stat(serial).map_err(|error| self.errno(error))
    }
    fn errno(&self, error: WorkspaceError) -> Errno {
        let code = match error {
            WorkspaceError::Content(ContentError::PathNotFound) => Errno::ENOENT,
            WorkspaceError::Content(ContentError::PathLimitExceeded) => Errno::ENAMETOOLONG,
            WorkspaceError::Overlay(OverlayError::Stale) => Errno::ESTALE,
            _ => Errno::EIO,
        };
        if code != Errno::ENOENT && code != Errno::ENAMETOOLONG {
            self.failure
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .get_or_insert(error);
        }
        code
    }
    fn remember_parent(&self, stat: &ViewStat, parent: u64) -> Result<(), Errno> {
        if stat.kind != InodeKind::Directory || stat.serial == self.identity.root {
            return Ok(());
        }
        let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
        match state.parents.get(&stat.serial) {
            Some(saved) if *saved != parent => Err(Errno::EIO),
            Some(_) => Ok(()),
            None => {
                state.parents.insert(stat.serial, parent);
                Ok(())
            }
        }
    }
    fn enumerate(
        &self,
        serial: u64,
        handle: u64,
        offset: u64,
        reply: &mut ReplyDirectory,
    ) -> Result<(), Errno> {
        let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
        let dir = state.directories.get_mut(&handle).ok_or(Errno::EBADF)?;
        if dir.serial != serial {
            return Err(Errno::EBADF);
        }
        let mut after = match offset {
            0..=2 => None,
            _ => Some(dir.offsets.get(&offset).cloned().ok_or(Errno::EINVAL)?),
        };
        for (name, inode, cookie) in [
            (".", self.identity.inode(serial)?, 1),
            ("..", self.identity.inode(dir.parent)?, 2),
        ] {
            if cookie > offset && reply.add(inode, cookie, FileType::Directory, name) {
                return Ok(());
            }
        }
        loop {
            let page = self
                .source
                .list(serial, after.as_deref())
                .map_err(|error| self.errno(error))?;
            for (name, child) in page.entries {
                let stat = self.stat(child)?;
                let kind = file_type(stat.kind);
                let cookie = if let Some(cookie) = dir.names.get(&name) {
                    *cookie
                } else {
                    dir.next.checked_add(1).ok_or(Errno::EOVERFLOW)?
                };
                if reply.add(
                    self.identity.inode(child)?,
                    cookie,
                    kind,
                    OsStr::from_bytes(&name),
                ) {
                    return Ok(());
                }
                if !dir.names.contains_key(&name) {
                    dir.next = cookie;
                    dir.offsets.insert(cookie, name.clone());
                    dir.names.insert(name, cookie);
                }
            }
            match page.continuation {
                Some(next) if after.as_ref().is_none_or(|old| *old < next) => after = Some(next),
                Some(_) => return Err(Errno::EIO),
                None => return Ok(()),
            }
        }
    }
}

fn file_type(kind: InodeKind) -> FileType {
    match kind {
        InodeKind::RegularFile => FileType::RegularFile,
        InodeKind::Directory => FileType::Directory,
        InodeKind::Symlink => FileType::Symlink,
    }
}
impl Filesystem for SnapshotFilesystem {
    fn init(&mut self, _: &Request, config: &mut KernelConfig) -> io::Result<()> {
        self.negotiation
            .set(crate::mount::negotiate(config)?)
            .map_err(|_| io::Error::other("duplicate snapshot initialization"))
    }
    fn lookup(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let result = (|| {
            let parent = self.serial(parent)?;
            let bytes = name.as_bytes();
            let (stat, named) = match bytes {
                b"." => (self.stat(parent)?, false),
                b".." => {
                    let value = self.state.lock().map_err(|_| Errno::EIO)?.parent(parent)?;
                    (self.stat(value)?, false)
                }
                _ => {
                    if bytes.len() > 255 {
                        return Err(Errno::ENAMETOOLONG);
                    }
                    let name = PathName::from_bytes(bytes).map_err(|_| Errno::EINVAL)?;
                    (
                        self.source
                            .lookup(parent, &name)
                            .map_err(|error| self.errno(error))?,
                        true,
                    )
                }
            };
            if named {
                self.remember_parent(&stat, parent)?;
            }
            self.identity.attributes(&stat)
        })();
        match result {
            Ok(attributes) => reply.entry(&TTL, &attributes, self.generation),
            Err(error) => reply.error(error),
        }
    }
    fn getattr(&self, _: &Request, inode: INodeNo, handle: Option<FileHandle>, reply: ReplyAttr) {
        let result = (|| {
            let serial = self.serial(inode)?;
            if let Some(handle) = handle {
                let state = self.state.lock().map_err(|_| Errno::EIO)?;
                if state.files.get(&handle.0) != Some(&serial)
                    && state.directories.get(&handle.0).map(|dir| dir.serial) != Some(serial)
                {
                    return Err(Errno::EBADF);
                }
            }
            self.identity.attributes(&self.stat(serial)?)
        })();
        match result {
            Ok(attributes) => reply.attr(&TTL, &attributes),
            Err(error) => reply.error(error),
        }
    }
    fn readlink(&self, _: &Request, inode: INodeNo, reply: ReplyData) {
        let result = self.serial(inode).and_then(|serial| {
            self.source
                .readlink(serial)
                .map_err(|error| self.errno(error))
        });
        match result {
            Ok(target) => reply.data(target.as_bytes()),
            Err(error) => reply.error(error),
        }
    }
    fn open(&self, _: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let result = (|| {
            if flags.acc_mode() != OpenAccMode::O_RDONLY {
                return Err(Errno::EROFS);
            }
            let serial = self.serial(inode)?;
            match self.stat(serial)?.kind {
                InodeKind::RegularFile => {}
                InodeKind::Directory => return Err(Errno::EISDIR),
                InodeKind::Symlink => return Err(Errno::ELOOP),
            }
            let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
            let handle = state.handle()?;
            state.files.insert(handle, serial);
            Ok(handle)
        })();
        match result {
            Ok(handle) => reply.opened(FileHandle(handle), FopenFlags::empty()),
            Err(error) => reply.error(error),
        }
    }
    fn read(
        &self,
        _: &Request,
        inode: INodeNo,
        handle: FileHandle,
        offset: u64,
        size: u32,
        _: OpenFlags,
        _: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let result = (|| {
            if size as usize > layerfs_overlay::READ_WINDOW {
                return Err(Errno::EINVAL);
            }
            let serial = self.serial(inode)?;
            if self
                .state
                .lock()
                .map_err(|_| Errno::EIO)?
                .files
                .get(&handle.0)
                != Some(&serial)
            {
                return Err(Errno::EBADF);
            }
            let mut bytes = Vec::with_capacity(size as usize);
            let count = self
                .source
                .read(serial, offset, size, &mut bytes)
                .map_err(|error| self.errno(error))?;
            if count != bytes.len() as u64 || bytes.len() > size as usize {
                return Err(Errno::EIO);
            }
            Ok(bytes)
        })();
        match result {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(error),
        }
    }
    fn release(
        &self,
        _: &Request,
        inode: INodeNo,
        handle: FileHandle,
        _: OpenFlags,
        _: Option<LockOwner>,
        _: bool,
        reply: ReplyEmpty,
    ) {
        let result = (|| {
            let serial = self.serial(inode)?;
            let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
            if state.files.get(&handle.0) != Some(&serial) {
                return Err(Errno::EBADF);
            }
            state.files.remove(&handle.0);
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error),
        }
    }
    fn opendir(&self, _: &Request, inode: INodeNo, _: OpenFlags, reply: ReplyOpen) {
        let result = (|| {
            let serial = self.serial(inode)?;
            if self.stat(serial)?.kind != InodeKind::Directory {
                return Err(Errno::ENOTDIR);
            }
            let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
            let parent = state.parent(serial)?;
            let handle = state.handle()?;
            state.directories.insert(
                handle,
                Directory {
                    serial,
                    parent,
                    next: 2,
                    ..Directory::default()
                },
            );
            Ok(handle)
        })();
        match result {
            Ok(handle) => reply.opened(FileHandle(handle), FopenFlags::empty()),
            Err(error) => reply.error(error),
        }
    }
    fn readdir(
        &self,
        _: &Request,
        inode: INodeNo,
        handle: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let result = self
            .serial(inode)
            .and_then(|serial| self.enumerate(serial, handle.0, offset, &mut reply));
        match result {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error),
        }
    }
    fn releasedir(
        &self,
        _: &Request,
        inode: INodeNo,
        handle: FileHandle,
        _: OpenFlags,
        reply: ReplyEmpty,
    ) {
        let result = (|| {
            let serial = self.serial(inode)?;
            let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
            if state.directories.get(&handle.0).map(|dir| dir.serial) != Some(serial) {
                return Err(Errno::EBADF);
            }
            state.directories.remove(&handle.0);
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error),
        }
    }
    fn statfs(&self, _: &Request, _: INodeNo, reply: ReplyStatfs) {
        reply.statfs(1 << 30, 1 << 30, 1 << 30, 1 << 30, 1 << 30, 4096, 255, 4096);
    }
    fn access(&self, _: &Request, _: INodeNo, _: AccessFlags, reply: ReplyEmpty) {
        // default_permissions is required at mount; the kernel performs it.
        reply.error(Errno::ENOSYS);
    }
}
