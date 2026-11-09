//! Read-only admission of an explicitly provisioned host Workspace mount home.
//!
//! This does not create a mount, expose a Workspace, authorize a consumer or
//! prove that an old owner has exited. The caller retains this guard and must
//! separately fence lifecycle effects and each consumer.
use nix::{
    fcntl::{open, openat, OFlag},
    sys::stat::Mode,
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
    identity: ExportIdentity,
}

impl HostExportRoot {
    /// Checks an empty root-owned path with its own shared propagation group.
    /// All ancestors must be root-owned and not writable by group or others.
    /// The caller creates and configures the mount separately.
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
        for directory in &self.directories {
            check_directory(&directory.metadata()?)?;
        }
        let current = open_chain(&self.path)?;
        if current.len() != self.directories.len() {
            return Err(invalid("host export ancestor chain changed"));
        }
        for (held, named) in self.directories.iter().zip(&current) {
            let held_meta = held.metadata()?;
            let named_meta = named.metadata()?;
            if held_meta.dev() != named_meta.dev()
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
        let identity = observe(path, &directories)?;
        Ok(Self {
            path: path.to_owned(),
            directories,
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
    if leaf.mode() & 0o705 != 0o705 {
        return Err(invalid("host export root is not traversable by consumers"));
    }
    Ok(directories)
}

fn check_directory(value: &Metadata) -> io::Result<()> {
    if !value.is_dir() || value.uid() != 0 || value.mode() & 0o022 != 0 {
        return Err(invalid("host export directory ownership or mode"));
    }
    Ok(())
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
        entries.push(MountEntry {
            id: number(fields[0])?,
            parent: number(fields[1])?,
            target: PathBuf::from(std::ffi::OsString::from_vec(unescape(fields[4])?)),
            shared,
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
