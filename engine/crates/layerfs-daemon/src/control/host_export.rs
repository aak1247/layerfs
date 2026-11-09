//! Daemon-side verification of one host export in this mount namespace.
use layerfs_bridge::{control::ReadyMount, daemon_setup::HostExportIdentity};
use nix::{
    fcntl::{open, openat, Flock, FlockArg, OFlag},
    sys::stat::Mode,
};
use std::{
    fs::{self, File, Metadata},
    io::{self, BufRead, BufReader, Write},
    os::{
        fd::AsRawFd,
        unix::{
            ffi::OsStringExt,
            fs::{FileExt, MetadataExt},
        },
    },
    path::{Component, Path, PathBuf},
};

/// Held daemon-side descriptors for a host export. This does not authorize a
/// consumer, prove owner exit, or make recovery/replay safe.
#[derive(Debug)]
pub struct ContainerExportRoot {
    path: PathBuf,
    directories: Vec<File>,
    permissions: Vec<(u32, u32, u32)>,
    expected: HostExportIdentity,
    mount_id: u64,
    claim: Option<Claim>,
}

#[derive(Debug)]
struct Claim {
    file: Flock<File>,
    body: Vec<u8>,
    gid: u32,
}

const CLAIM_FILE: &str = ".layerfs-export-owner";
const CLAIM_MAGIC: &[u8] = b"LAYERFS-EXPORT-OWNER\x01";

impl ContainerExportRoot {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
    /// Validates a fresh, empty root against the host's recorded identity.
    /// The caller retains this guard for the whole daemon lifetime.
    pub fn open_fresh(path: &Path, expected: &HostExportIdentity) -> io::Result<Self> {
        let directories = open_chain(path)?;
        let permissions = directories
            .iter()
            .map(|directory| directory.metadata().map(|value| permissions(&value)))
            .collect::<io::Result<_>>()?;
        let mount_id = observe(path, &directories, expected)?;
        fresh_root(path, false)?;
        let guard = Self {
            path: path.to_owned(),
            directories,
            permissions,
            expected: expected.clone(),
            mount_id,
            claim: None,
        };
        guard.verify()?;
        Ok(guard)
    }

    /// Revalidates the named path, pinned directory chain and local mount ID.
    /// An error never implies a prior mount was undone or may be retried.
    pub fn verify(&self) -> io::Result<()> {
        let current = open_chain(&self.path)?;
        if current.len() != self.directories.len() {
            return Err(invalid("host Workspace export directory chain changed"));
        }
        for ((held, named), expected) in
            self.directories.iter().zip(&current).zip(&self.permissions)
        {
            let old = held.metadata()?;
            let new = named.metadata()?;
            check_directory(&old)?;
            if permissions(&old) != *expected
                || permissions(&new) != *expected
                || old.dev() != new.dev()
                || old.ino() != new.ino()
                || descriptor_mount_id(held)? != descriptor_mount_id(named)?
            {
                return Err(invalid("host Workspace export directory changed"));
            }
        }
        if observe(&self.path, &current, &self.expected)? != self.mount_id {
            return Err(invalid("host Workspace export mount changed"));
        }
        if let Some(claim) = &self.claim {
            self.verify_claim(claim)?;
        }
        Ok(())
    }

    /// Claims this fresh directory once, with no replacement or cleanup on
    /// failure. Supply the daemon's public key, never its private key. A
    /// successful claim stays on disk until an independently fenced recovery
    /// or retirement handles it.
    pub fn claim_fresh(&mut self, public_key: [u8; 32]) -> io::Result<()> {
        if self.claim.is_some() || public_key == [0; 32] {
            return Err(invalid("host Workspace export claim admission"));
        }
        self.verify()?;
        fresh_root(&self.path, false)?;
        let directory = self.directories.last().expect("root and leaf are held");
        let flags =
            OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
        let file = File::from(openat(
            directory,
            Path::new(CLAIM_FILE),
            flags,
            Mode::from_bits_truncate(0o600),
        )?);
        let mut file = Flock::lock(file, FlockArg::LockExclusiveNonblock)
            .map_err(|(_, error)| io::Error::from(error))?;
        let mut body = CLAIM_MAGIC.to_vec();
        body.extend_from_slice(self.expected.boot_id.as_bytes());
        body.extend_from_slice(&public_key);
        file.write_all(&body)?;
        file.sync_all()?;
        let sync_flags =
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
        let parent = File::from(openat(
            directory,
            Path::new("."),
            sync_flags,
            Mode::empty(),
        )?);
        if descriptor_mount_id(&parent)? != self.mount_id {
            return Err(invalid("host Workspace export claim parent changed"));
        }
        parent.sync_all()?;
        let gid = file.metadata()?.gid();
        self.claim = Some(Claim { file, body, gid });
        self.verify()?;
        fresh_root(&self.path, true)?;
        self.verify()
    }

    pub(super) fn require_claimed(&self) -> io::Result<()> {
        if self.claim.is_none() {
            return Err(invalid("host Workspace export is not claimed"));
        }
        self.verify()
    }

    fn verify_claim(&self, claim: &Claim) -> io::Result<()> {
        let directory = self.directories.last().expect("root and leaf are held");
        let flags = OFlag::O_PATH | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
        let named = File::from(openat(
            directory,
            Path::new(CLAIM_FILE),
            flags,
            Mode::empty(),
        )?);
        let held = claim.file.metadata()?;
        let current = named.metadata()?;
        if !held.is_file()
            || held.uid() != 0
            || held.gid() != claim.gid
            || held.mode() & 0o7777 != 0o600
            || held.nlink() != 1
            || held.dev() != current.dev()
            || held.ino() != current.ino()
            || descriptor_mount_id(&claim.file)? != self.mount_id
            || descriptor_mount_id(&named)? != self.mount_id
            || held.len() != claim.body.len() as u64
        {
            return Err(invalid("host Workspace export claim identity"));
        }
        let mut body = vec![0; claim.body.len()];
        claim.file.read_exact_at(&mut body, 0)?;
        if body != claim.body {
            return Err(invalid("host Workspace export claim contents"));
        }
        Ok(())
    }

    /// A named child must still be the exact connection recorded at Ready.
    pub(super) fn verify_mount(&self, ready: &ReadyMount) -> io::Result<()> {
        self.verify()?;
        let path = self.path.join(ready.token.namespace.to_string());
        if Path::new(&ready.directory) != path {
            return Err(invalid("host Workspace export child path"));
        }
        let entries = mount_entries()?;
        let mut matching = entries.iter().filter(|entry| entry.target == path);
        let entry = matching
            .next()
            .ok_or_else(|| invalid("host Workspace export child mount is missing"))?;
        if matching.next().is_some()
            || entry.id != ready.receipt.mount_id
            || entry.device != (ready.receipt.device_major, ready.receipt.device_minor)
        {
            return Err(invalid("host Workspace export child mount changed"));
        }
        Ok(())
    }
}

fn open_chain(path: &Path) -> io::Result<Vec<File>> {
    if !nix::unistd::geteuid().is_root() {
        return Err(invalid("host Workspace export requires root"));
    }
    for name in ["uid_map", "gid_map"] {
        let mapping = fs::read_to_string(format!("/proc/self/{name}"))?;
        if mapping.split_whitespace().collect::<Vec<_>>() != ["0", "0", "4294967295"] {
            return Err(invalid(
                "host Workspace export requires initial user namespace",
            ));
        }
    }
    if !path.is_absolute() || path == Path::new("/") || path.to_str().is_none() {
        return Err(invalid("host Workspace export path"));
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
            _ => return Err(invalid("host Workspace export path components")),
        }
    }
    let leaf = directories.last().expect("root is held").metadata()?;
    if directories.len() < 2 || leaf.mode() & 0o700 != 0o700 {
        return Err(invalid("host Workspace export access"));
    }
    Ok(directories)
}

fn check_directory(metadata: &Metadata) -> io::Result<()> {
    if !metadata.is_dir() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
        return Err(invalid("host Workspace export ownership or mode"));
    }
    Ok(())
}

fn fresh_root(path: &Path, claimed: bool) -> io::Result<()> {
    let mut found = false;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if !claimed || found || entry.file_name() != CLAIM_FILE {
            return Err(invalid("host Workspace export is not fresh"));
        }
        found = true;
    }
    if found != claimed
        || mount_entries()?
            .iter()
            .any(|entry| entry.target != path && entry.target.starts_with(path))
    {
        return Err(invalid("host Workspace export is not fresh"));
    }
    Ok(())
}

fn permissions(metadata: &Metadata) -> (u32, u32, u32) {
    (metadata.uid(), metadata.gid(), metadata.mode() & 0o7777)
}

fn observe(path: &Path, directories: &[File], expected: &HostExportIdentity) -> io::Result<u64> {
    let entries = mount_entries()?;
    let mut matching = entries.iter().filter(|entry| entry.target == path);
    let entry = matching
        .next()
        .ok_or_else(|| invalid("host Workspace export is not a mount root"))?;
    if matching.next().is_some() || entry.shared != Some(expected.shared_peer) {
        return Err(invalid("host Workspace export mount propagation"));
    }
    let leaf = directories.last().expect("root and leaf are held");
    let metadata = leaf.metadata()?;
    let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    if boot.trim() != expected.boot_id
        || metadata.dev() != expected.device
        || metadata.ino() != expected.inode
        || descriptor_mount_id(leaf)? != entry.id
    {
        return Err(invalid("host Workspace export identity"));
    }
    Ok(entry.id)
}

fn descriptor_mount_id(file: &File) -> io::Result<u64> {
    let info = fs::read_to_string(format!("/proc/self/fdinfo/{}", file.as_raw_fd()))?;
    let mut matching = info.lines().filter_map(|line| line.strip_prefix("mnt_id:"));
    let id = matching
        .next()
        .and_then(|value| value.trim().parse().ok())
        .ok_or_else(|| invalid("host Workspace export descriptor mount ID"))?;
    if matching.next().is_some() {
        return Err(invalid(
            "duplicate host Workspace export descriptor mount ID",
        ));
    }
    Ok(id)
}

struct MountEntry {
    id: u64,
    target: PathBuf,
    shared: Option<u64>,
    device: (u32, u32),
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
            .ok_or_else(|| invalid("host Workspace export mount table row"))?;
        if separator < 6 || fields.len() <= separator + 3 {
            return Err(invalid("host Workspace export mount table row"));
        }
        let number = |bytes: &[u8]| -> io::Result<u64> {
            std::str::from_utf8(bytes)
                .ok()
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| invalid("host Workspace export mount table number"))
        };
        let mut shared = None;
        for field in &fields[6..separator] {
            if let Some(value) = field.strip_prefix(b"shared:") {
                if shared.replace(number(value)?).is_some() {
                    return Err(invalid("duplicate host Workspace export shared group"));
                }
            }
        }
        let mut device = fields[2].split(|byte| *byte == b':');
        let part = |value: Option<&[u8]>| -> io::Result<u32> {
            let value = value.ok_or_else(|| invalid("host Workspace export mount device"))?;
            u32::try_from(number(value)?).map_err(|_| invalid("host Workspace export mount device"))
        };
        let device = (part(device.next())?, part(device.next())?);
        if fields[2].iter().filter(|byte| **byte == b':').count() != 1 {
            return Err(invalid("host Workspace export mount device"));
        }
        entries.push(MountEntry {
            id: number(fields[0])?,
            target: PathBuf::from(std::ffi::OsString::from_vec(unescape(fields[4])?)),
            shared,
            device,
        });
    }
    Ok(entries)
}

fn unescape(input: &[u8]) -> io::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if input[index] == b'\\' {
            let digits = input
                .get(index + 1..index + 4)
                .ok_or_else(|| invalid("host Workspace export mount path escape"))?;
            if !digits.iter().all(|byte| (b'0'..=b'7').contains(byte)) {
                return Err(invalid("host Workspace export mount path escape"));
            }
            let value = digits
                .iter()
                .fold(0u16, |n, byte| n * 8 + u16::from(byte - b'0'));
            output.push(
                u8::try_from(value)
                    .map_err(|_| invalid("host Workspace export mount path escape"))?,
            );
            index += 4;
        } else {
            output.push(input[index]);
            index += 1;
        }
    }
    Ok(output)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
