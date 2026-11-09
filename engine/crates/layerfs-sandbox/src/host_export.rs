//! Read-only admission of an explicitly provisioned host Workspace mount home.
//!
//! This does not create a mount, expose a Workspace, authorize a consumer or
//! prove that an old owner has exited. The caller retains this guard and must
//! separately fence lifecycle effects and each consumer.
use crate::export_lease::HostExportLease;
use nix::{
    fcntl::{open, openat, OFlag},
    sys::stat::{major, minor, Mode},
};
use std::{
    fs::{self, File, Metadata},
    io::{self, BufRead, BufReader},
    os::{
        fd::AsRawFd,
        unix::{ffi::OsStringExt, fs::MetadataExt},
    },
    path::{Component, Path, PathBuf},
};

/// Kernel and directory identity observed in one mount namespace. Persist it
/// with the original lifecycle intent. After the guard is lost, matching
/// numbers cannot prove continuous identity: kernel identifiers can be reused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportIdentity {
    pub boot_id: String,
    pub device: u64,
    pub inode: u64,
    pub mount_id: u64,
    pub shared_peer: u64,
}

/// Held directory descriptors keep the observed ancestor chain available for
/// revalidation. A path or mount change is refused, never repaired or adopted.
#[derive(Debug)]
pub struct HostExportRoot {
    path: PathBuf,
    directories: Vec<File>,
    permissions: Vec<(u32, u32, u32)>,
    identity: ExportIdentity,
}

/// A host-visible workspace mount pinned while its consumer uses it.
///
/// The mount descriptor may keep the mount busy. Drop this guard before
/// requesting an unmount. This is an observation, not consumer authorization,
/// durable lifecycle custody, or proof that an earlier owner has exited.
#[derive(Debug)]
pub struct HostWorkspaceMount<'a> {
    lease: &'a HostExportLease,
    path: PathBuf,
    descriptor: File,
    mount_id: u64,
    device_major: u32,
    device_minor: u32,
}

impl HostExportLease {
    /// Independently checks one host-visible FUSE mount against the device
    /// identity from the original authenticated daemon Ready receipt. The
    /// namespace must be that receipt's positive, never-reused namespace.
    pub fn observe_workspace(
        &self,
        namespace: i64,
        device_major: u32,
        device_minor: u32,
    ) -> io::Result<HostWorkspaceMount<'_>> {
        if namespace <= 0 {
            return Err(invalid("host workspace namespace"));
        }
        self.verify()?;
        let root = self.root();
        let name = namespace.to_string();
        let descriptor = open_workspace(root, std::ffi::OsStr::new(&name))?;
        let mount = HostWorkspaceMount {
            lease: self,
            path: root.path.join(name),
            mount_id: descriptor_mount_id(&descriptor)?,
            descriptor,
            device_major,
            device_minor,
        };
        mount.verify()?;
        Ok(mount)
    }
}

impl HostWorkspaceMount<'_> {
    /// Host path corresponding to the original daemon namespace.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Pinned directory descriptor for trusted, descriptor-relative consumers.
    pub fn descriptor(&self) -> &File {
        &self.descriptor
    }

    /// Host mount-table ID; it is distinct from the daemon namespace's ID.
    pub fn mount_id(&self) -> u64 {
        self.mount_id
    }

    /// Revalidates the host root, named mount, pinned descriptor and device.
    /// Callers must also fence their own effects and verify their consumers.
    pub fn verify(&self) -> io::Result<()> {
        self.lease.verify()?;
        let root = self.lease.root();
        let name = self
            .path
            .file_name()
            .ok_or_else(|| invalid("host workspace path"))?;
        let named = open_workspace(root, name)?;
        let held_meta = self.descriptor.metadata()?;
        let named_meta = named.metadata()?;
        if !held_meta.is_dir()
            || held_meta.dev() != named_meta.dev()
            || held_meta.ino() != named_meta.ino()
            || major(held_meta.dev()) != u64::from(self.device_major)
            || minor(held_meta.dev()) != u64::from(self.device_minor)
            || descriptor_mount_id(&self.descriptor)? != self.mount_id
            || descriptor_mount_id(&named)? != self.mount_id
        {
            return Err(invalid("host workspace descriptor identity changed"));
        }
        let entries = mount_entries()?;
        let mut matches = entries.iter().filter(|entry| entry.target == self.path);
        let selected = matches
            .next()
            .ok_or_else(|| invalid("host workspace mount missing"))?;
        if matches.next().is_some()
            || selected.id != self.mount_id
            || selected.parent != root.identity.mount_id
            || selected.device_major != u64::from(self.device_major)
            || selected.device_minor != u64::from(self.device_minor)
            || selected.root != Path::new("/")
            || selected.filesystem != b"fuse"
            || selected.source != b"layerfs"
            || entries
                .iter()
                .any(|entry| entry.target != self.path && entry.target.starts_with(&self.path))
        {
            return Err(invalid("host workspace mount identity changed"));
        }
        self.lease.verify()
    }
}

fn open_workspace(root: &HostExportRoot, name: &std::ffi::OsStr) -> io::Result<File> {
    let directory = root.directories.last().expect("root and leaf are held");
    let flags = OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
    Ok(File::from(openat(directory, name, flags, Mode::empty())?))
}

impl HostExportRoot {
    /// Checks an empty root-owned path with its own shared propagation group.
    /// All ancestors must be root-owned and not writable by group or others;
    /// the caller separately grants only the intended consumers access.
    pub fn open_fresh(path: &Path) -> io::Result<Self> {
        let guard = Self::open(path)?;
        guard.verify_fresh()?;
        Ok(guard)
    }

    /// Rechecks both identity and the absence of entries and child mounts.
    pub fn verify_fresh(&self) -> io::Result<()> {
        self.verify()?;
        if fs::read_dir(&self.path)?.next().transpose()?.is_some() {
            return Err(invalid("host export root is not empty"));
        }
        if mount_entries()?
            .iter()
            .any(|entry| entry.target != self.path && entry.target.starts_with(&self.path))
        {
            return Err(invalid("host export root already contains mounts"));
        }
        self.verify()
    }

    /// Compares a root against previously observed identity fields.
    /// A match is not proof of uninterrupted identity, owner exit, permission
    /// to recover a Workspace, or permission to replay an effect.
    pub fn reopen(path: &Path, expected: &ExportIdentity) -> io::Result<Self> {
        let guard = Self::open(path)?;
        if &guard.identity != expected {
            return Err(invalid("host export identity changed"));
        }
        guard.verify()?;
        Ok(guard)
    }

    /// Original source path for a trusted local runtime adapter.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Original observed identity for a caller's durable lifecycle record.
    pub fn identity(&self) -> &ExportIdentity {
        &self.identity
    }

    /// Rechecks the pinned chain, named path and exact propagation identity.
    /// It does not enumerate consumers or make an operation result certain.
    pub fn verify(&self) -> io::Result<()> {
        for (directory, expected) in self.directories.iter().zip(&self.permissions) {
            let metadata = directory.metadata()?;
            check_directory(&metadata)?;
            if permissions(&metadata) != *expected {
                return Err(invalid("host export directory permissions changed"));
            }
        }
        let current = open_chain(&self.path)?;
        if current.len() != self.directories.len() {
            return Err(invalid("host export ancestor chain changed"));
        }
        for ((held, named), expected) in
            self.directories.iter().zip(&current).zip(&self.permissions)
        {
            let held_meta = held.metadata()?;
            let named_meta = named.metadata()?;
            if permissions(&held_meta) != *expected
                || permissions(&named_meta) != *expected
                || held_meta.dev() != named_meta.dev()
                || held_meta.ino() != named_meta.ino()
                || descriptor_mount_id(held)? != descriptor_mount_id(named)?
            {
                return Err(invalid("host export directory identity changed"));
            }
        }
        if observe(&self.path, &current)? != self.identity {
            return Err(invalid("host export mount identity changed"));
        }
        Ok(())
    }

    pub(crate) fn matches_descriptor(&self, file: &File) -> io::Result<()> {
        let metadata = file.metadata()?;
        check_directory(&metadata)?;
        if metadata.dev() != self.identity.device
            || metadata.ino() != self.identity.inode
            || descriptor_mount_id(file)? != self.identity.mount_id
        {
            return Err(invalid("host export locked directory changed"));
        }
        Ok(())
    }

    fn open(path: &Path) -> io::Result<Self> {
        let directories = open_chain(path)?;
        let permissions = directories
            .iter()
            .map(|directory| directory.metadata().map(|value| permissions(&value)))
            .collect::<io::Result<_>>()?;
        let identity = observe(path, &directories)?;
        Ok(Self {
            path: path.to_owned(),
            directories,
            permissions,
            identity,
        })
    }
}

fn open_chain(path: &Path) -> io::Result<Vec<File>> {
    initial_user_namespace()?;
    if !path.is_absolute() || path == Path::new("/") || path.to_str().is_none() {
        return Err(invalid(
            "host export root must be a non-root absolute UTF-8 path",
        ));
    }
    let flags = OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
    let root = File::from(open(Path::new("/"), flags, Mode::empty())?);
    check_directory(&root.metadata()?)?;
    let mut directories = vec![root];
    for component in path.components() {
        match component {
            Component::RootDir => (),
            Component::Normal(name) => {
                let parent = directories.last().expect("root is held");
                let child = File::from(openat(parent, name, flags, Mode::empty())?);
                check_directory(&child.metadata()?)?;
                directories.push(child);
            }
            _ => return Err(invalid("host export path contains a relative component")),
        }
    }
    if directories.len() < 2 {
        return Err(invalid("host export root is missing"));
    }
    let leaf = directories
        .last()
        .expect("root and leaf are held")
        .metadata()?;
    if leaf.mode() & 0o700 != 0o700 {
        return Err(invalid("host export root owner permissions"));
    }
    Ok(directories)
}

fn check_directory(value: &Metadata) -> io::Result<()> {
    if !value.is_dir() || value.uid() != 0 || value.mode() & 0o022 != 0 {
        return Err(invalid("host export directory ownership or mode"));
    }
    Ok(())
}

fn permissions(value: &Metadata) -> (u32, u32, u32) {
    (value.uid(), value.gid(), value.mode() & 0o7777)
}

fn observe(path: &Path, chain: &[File]) -> io::Result<ExportIdentity> {
    let entries = mount_entries()?;
    let mut matches = entries.iter().filter(|entry| entry.target == path);
    let selected = matches
        .next()
        .ok_or_else(|| invalid("host export is not a mount root"))?;
    if matches.next().is_some() {
        return Err(invalid("host export has stacked mount roots"));
    }
    let peer = selected
        .shared
        .ok_or_else(|| invalid("host export is not shared"))?;
    let parent = entries
        .iter()
        .find(|entry| entry.id == selected.parent)
        .ok_or_else(|| invalid("host export parent mount is missing"))?;
    if parent.shared == Some(peer)
        || entries
            .iter()
            .filter(|entry| entry.shared == Some(peer))
            .count()
            != 1
    {
        return Err(invalid(
            "host export shares a propagation group with another host mount",
        ));
    }
    let leaf = chain.last().expect("root and leaf are held").metadata()?;
    if descriptor_mount_id(chain.last().expect("root and leaf are held"))? != selected.id {
        return Err(invalid("host export descriptor mount changed"));
    }
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned();
    if boot_id.is_empty() {
        return Err(invalid("host boot identity is empty"));
    }
    Ok(ExportIdentity {
        boot_id,
        device: leaf.dev(),
        inode: leaf.ino(),
        mount_id: selected.id,
        shared_peer: peer,
    })
}

fn descriptor_mount_id(file: &File) -> io::Result<u64> {
    let info = fs::read_to_string(format!("/proc/self/fdinfo/{}", file.as_raw_fd()))?;
    let mut matches = info.lines().filter_map(|line| line.strip_prefix("mnt_id:"));
    let id = matches
        .next()
        .and_then(|value| value.trim().parse().ok())
        .ok_or_else(|| invalid("host export descriptor mount identity"))?;
    if matches.next().is_some() {
        return Err(invalid("duplicate host export descriptor mount identity"));
    }
    Ok(id)
}

fn initial_user_namespace() -> io::Result<()> {
    for name in ["uid_map", "gid_map"] {
        let mapping = fs::read_to_string(format!("/proc/self/{name}"))?;
        let values: Vec<_> = mapping.split_whitespace().collect();
        if values != ["0", "0", "4294967295"] {
            return Err(invalid("host export requires the initial user namespace"));
        }
    }
    Ok(())
}

struct MountEntry {
    id: u64,
    parent: u64,
    target: PathBuf,
    shared: Option<u64>,
    device_major: u64,
    device_minor: u64,
    root: PathBuf,
    filesystem: Vec<u8>,
    source: Vec<u8>,
}

fn mount_entries() -> io::Result<Vec<MountEntry>> {
    let mut entries = Vec::new();
    for line in BufReader::new(File::open("/proc/self/mountinfo")?).split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let fields: Vec<_> = line.split(|byte| *byte == b' ').collect();
        let separator = fields
            .iter()
            .position(|part| *part == b"-")
            .ok_or_else(|| invalid("host mount table row"))?;
        if separator < 6 || fields.len() <= separator + 3 {
            return Err(invalid("host mount table row"));
        }
        let number = |bytes: &[u8]| -> io::Result<u64> {
            std::str::from_utf8(bytes)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| invalid("host mount table number"))
        };
        let mut shared = None;
        for field in &fields[6..separator] {
            if let Some(value) = field.strip_prefix(b"shared:") {
                if shared.replace(number(value)?).is_some() {
                    return Err(invalid("duplicate host mount propagation group"));
                }
            }
        }
        let mut device = fields[2].split(|byte| *byte == b':');
        let device_major = device.next().ok_or_else(|| invalid("host mount device"))?;
        let device_minor = device.next().ok_or_else(|| invalid("host mount device"))?;
        if device.next().is_some() {
            return Err(invalid("host mount device"));
        }
        entries.push(MountEntry {
            id: number(fields[0])?,
            parent: number(fields[1])?,
            target: PathBuf::from(std::ffi::OsString::from_vec(unescape(fields[4])?)),
            shared,
            device_major: number(device_major)?,
            device_minor: number(device_minor)?,
            root: PathBuf::from(std::ffi::OsString::from_vec(unescape(fields[3])?)),
            filesystem: fields[separator + 1].to_vec(),
            source: unescape(fields[separator + 2])?,
        });
    }
    Ok(entries)
}

fn unescape(input: &[u8]) -> io::Result<Vec<u8>> {
    let mut result = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if input[index] == b'\\' {
            let digits = input
                .get(index + 1..index + 4)
                .ok_or_else(|| invalid("host mount path escape"))?;
            if !digits.iter().all(|byte| (b'0'..=b'7').contains(byte)) {
                return Err(invalid("host mount path escape"));
            }
            let value = digits
                .iter()
                .fold(0u16, |n, byte| n * 8 + u16::from(byte - b'0'));
            result.push(u8::try_from(value).map_err(|_| invalid("host mount path escape"))?);
            index += 4;
        } else {
            result.push(input[index]);
            index += 1;
        }
    }
    Ok(result)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
