//! Checked writes to caller-owned private lifecycle records on a pinned directory.
//!
//! This does not lock the directory, authenticate a previous owner, resolve an
//! uncertain operation, or authorize a retry. Callers hold their own exclusive
//! lease and retain any failure and outstanding intent across restarts.
use nix::fcntl::{Flock, FlockArg};
use std::{
    ffi::OsStr,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};

const LOCK_NAME: &str = "ownership.lock";

/// The exact original boundary; attempted writes must not be replayed.
#[derive(Debug)]
pub struct Failure {
    /// Whether this write reached an effect that may have changed storage.
    pub attempted: bool,
    phase: &'static str,
    cause: io::Error,
}
impl Failure {
    /// The original write boundary at which the operation failed.
    pub fn phase(&self) -> &'static str {
        self.phase
    }

    /// The original operating-system error.
    pub fn cause(&self) -> &io::Error {
        &self.cause
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "durable record {}: {}", self.phase, self.cause)
    }
}
impl std::error::Error for Failure {}

/// The original private directory, retained across a multi-step attempt.
pub struct Directory {
    file: File,
    canonical: PathBuf,
}

/// A cooperative exclusive lease on the original private record directory.
///
/// The fixed sidecar also coordinates with the Node SDK. The directory must
/// remain private from untrusted writers; a file lock alone cannot prevent an
/// unauthorized writer from replacing its inode. This lease does not prove an
/// earlier owner or consumer has exited and never settles an unknown effect.
pub struct Lease {
    directory: Directory,
    lock: Flock<File>,
}

impl Lease {
    /// Acquires the original sidecar once without waiting.
    pub fn try_acquire(directory: Directory) -> io::Result<Self> {
        directory.verify()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(0x20000)
            .open(directory.path().join(LOCK_NAME))?;
        check_file(&directory.file, OsStr::new(LOCK_NAME), &file)?;
        let lock = Flock::lock(file, FlockArg::LockExclusiveNonblock)
            .map_err(|(_, error)| io::Error::from(error))?;
        let lease = Self { directory, lock };
        lease.verify()?;
        lease.lock.sync_all()?;
        lease.directory.sync()?;
        lease.verify()?;
        Ok(lease)
    }

    /// Rechecks the original directory, mount and locked sidecar identity.
    pub fn verify(&self) -> io::Result<()> {
        self.directory.verify()?;
        check_file(&self.directory.file, OsStr::new(LOCK_NAME), &self.lock)
    }

    /// Descriptor-relative directory path, valid while this lease is held.
    pub fn path(&self) -> PathBuf {
        self.directory.path()
    }

    /// Creates one new record while retaining the lease across the write.
    pub fn create(&mut self, name: &OsStr, bytes: &[u8]) -> Result<(), Failure> {
        self.admit(name)?;
        self.directory.create(name, bytes)?;
        self.after_effect()
    }

    /// Replaces one record while retaining the lease across the write.
    pub fn save(&mut self, name: &OsStr, temporary: &OsStr, bytes: &[u8]) -> Result<(), Failure> {
        self.admit(name)?;
        self.admit(temporary)?;
        self.directory.save(name, temporary, bytes)?;
        self.after_effect()
    }

    /// Removes one record; an error after admission retains unknown custody.
    pub fn remove(&mut self, name: &OsStr) -> Result<(), Failure> {
        self.admit(name)?;
        self.directory.remove(name).map_err(|cause| Failure {
            attempted: true,
            phase: "remove",
            cause,
        })?;
        self.after_effect()
    }

    fn admit(&self, name: &OsStr) -> Result<(), Failure> {
        check_name(name).map_err(directory_failure)?;
        if name == OsStr::new(LOCK_NAME) {
            return Err(directory_failure(invalid("record lease name")));
        }
        self.verify().map_err(directory_failure)
    }

    fn after_effect(&self) -> Result<(), Failure> {
        self.verify().map_err(|cause| Failure {
            attempted: true,
            phase: "lease revalidation",
            cause,
        })
    }
}
impl Directory {
    /// Pins an existing private directory owned by the effective user.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(0x20000)
            .open(path)?;
        let canonical = path.canonicalize()?;
        check_directory(&canonical, &file)?;
        Ok(Self { file, canonical })
    }
    /// Rechecks the held directory against its original named identity.
    pub fn verify(&self) -> io::Result<()> {
        check_directory(&self.canonical, &self.file)
    }
    /// Synchronizes the original directory after verifying its identity.
    pub fn sync(&self) -> io::Result<()> {
        self.verify()?;
        self.file.sync_all()?;
        self.verify()
    }
    /// Returns a descriptor-relative path valid while this owner is held.
    pub fn path(&self) -> PathBuf {
        fd_path(&self.file).join(".")
    }
    /// Replaces one record with a separately named synchronized temporary file.
    pub fn save(&self, name: &OsStr, temp_name: &OsStr, bytes: &[u8]) -> Result<(), Failure> {
        check_name(name).map_err(directory_failure)?;
        check_name(temp_name).map_err(directory_failure)?;
        if name == temp_name {
            return Err(directory_failure(invalid("record temporary name")));
        }
        let mut phase = "directory";
        let mut attempted = false;
        let mut directory = Some(self.file.try_clone().map_err(directory_failure)?);
        let mut file = None;
        let result = (|| -> io::Result<()> {
            let held = directory.as_ref().expect("directory held");
            check_directory(&self.canonical, held)?;
            let named = fd_path(held).join(name);
            match fs::symlink_metadata(&named) {
                Ok(_) => {
                    let existing = OpenOptions::new()
                        .read(true)
                        .custom_flags(0x20000)
                        .open(&named)?;
                    check_file(held, name, &existing)?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                Err(error) => return Err(error),
            }
            let temp = fd_path(held).join(temp_name);
            phase = "create temporary";
            attempted = true;
            file = Some(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(0x20000)
                    .open(&temp)?,
            );
            let original = file.as_mut().expect("file held");
            check_file(held, temp_name, original)?;
            phase = "write temporary";
            original.write_all(bytes)?;
            phase = "sync temporary";
            original.sync_all()?;
            check_file(held, temp_name, original)?;
            check_directory(&self.canonical, held)?;
            phase = "publish";
            fs::rename(&temp, &named)?;
            check_file(held, name, original)?;
            phase = "sync directory";
            held.sync_all()?;
            check_directory(&self.canonical, held)?;
            check_file(held, name, original)
        })();
        if let Err(cause) = result {
            if attempted {
                if let Some(file) = file.take() {
                    std::mem::forget(file);
                }
                if let Some(directory) = directory.take() {
                    std::mem::forget(directory);
                }
            }
            return Err(Failure {
                attempted,
                phase,
                cause,
            });
        }
        Ok(())
    }

    /// A partial intent remains at its original name and blocks a later attempt.
    pub fn create(&self, name: &OsStr, bytes: &[u8]) -> Result<(), Failure> {
        check_name(name).map_err(directory_failure)?;
        let mut phase = "directory";
        let mut attempted = false;
        let mut directory = Some(self.file.try_clone().map_err(directory_failure)?);
        let mut file = None;
        let result = (|| -> io::Result<()> {
            let held = directory.as_ref().expect("directory held");
            check_directory(&self.canonical, held)?;
            phase = "create intent";
            attempted = true;
            file = Some(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(0x20000)
                    .open(fd_path(held).join(name))?,
            );
            let original = file.as_mut().expect("intent held");
            check_file(held, name, original)?;
            phase = "write intent";
            original.write_all(bytes)?;
            phase = "sync intent";
            original.sync_all()?;
            check_file(held, name, original)?;
            check_directory(&self.canonical, held)?;
            phase = "sync directory";
            held.sync_all()?;
            check_directory(&self.canonical, held)?;
            check_file(held, name, original)
        })();
        if let Err(cause) = result {
            if attempted {
                if let Some(file) = file.take() {
                    std::mem::forget(file);
                }
                if let Some(directory) = directory.take() {
                    std::mem::forget(directory);
                }
            }
            return Err(Failure {
                attempted,
                phase,
                cause,
            });
        }
        Ok(())
    }

    /// Removes one checked original record, then synchronizes its directory.
    /// The caller retains this Directory if any part of the effect is unknown.
    pub fn remove(&self, name: &OsStr) -> io::Result<()> {
        check_name(name)?;
        self.verify()?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(0x20000)
            .open(fd_path(&self.file).join(name))?;
        check_file(&self.file, name, &file)?;
        let result = fs::remove_file(fd_path(&self.file).join(name))
            .and_then(|()| self.file.sync_all())
            .and_then(|()| self.verify());
        if result.is_err() {
            std::mem::forget(file);
        }
        result
    }
}

/// Replaces one record only after syncing its original file and directory.
/// Unknown effects retain the original file and directory until process exit.
pub fn save(path: &Path, temporary: &Path, bytes: &[u8]) -> Result<(), Failure> {
    let parent = path
        .parent()
        .ok_or_else(|| directory_failure(invalid("record parent")))?;
    if temporary.parent() != Some(parent) {
        return Err(directory_failure(invalid("record temporary parent")));
    }
    let name = path
        .file_name()
        .ok_or_else(|| directory_failure(invalid("record name")))?;
    let temporary = temporary
        .file_name()
        .ok_or_else(|| directory_failure(invalid("record temporary name")))?;
    Directory::open(parent)
        .map_err(directory_failure)?
        .save(name, temporary, bytes)
}

fn check_name(name: &OsStr) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || bytes == b"."
        || bytes == b".."
        || bytes.contains(&b'/')
        || bytes.contains(&0)
    {
        return Err(invalid("record name"));
    }
    Ok(())
}

fn directory_failure(cause: io::Error) -> Failure {
    Failure {
        attempted: false,
        phase: "directory",
        cause,
    }
}

fn fd_path(directory: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
}
fn check_directory(path: &Path, directory: &File) -> io::Result<()> {
    let opened = directory.metadata()?;
    let named_directory = OpenOptions::new()
        .read(true)
        .custom_flags(0x20000 | 0x10000)
        .open(path)?;
    let named = named_directory.metadata()?;
    let uid = fs::metadata("/proc/self")?.uid();
    if !opened.is_dir()
        || !named.is_dir()
        || opened.uid() != uid
        || named.uid() != uid
        || opened.mode() & 0o077 != 0
        || named.mode() & 0o077 != 0
        || opened.dev() != named.dev()
        || opened.ino() != named.ino()
        || mount_id(directory)? != mount_id(&named_directory)?
    {
        return Err(invalid("record directory identity or privacy"));
    }
    Ok(())
}

fn mount_id(file: &File) -> io::Result<u64> {
    let info = fs::read_to_string(format!("/proc/self/fdinfo/{}", file.as_raw_fd()))?;
    let value = info
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:").map(str::trim))
        .ok_or_else(|| invalid("record directory mount identity"))?;
    value
        .parse()
        .map_err(|_| invalid("record directory mount identity"))
}
fn check_file(directory: &File, name: &OsStr, file: &File) -> io::Result<()> {
    let opened = file.metadata()?;
    let named_file = OpenOptions::new()
        .read(true)
        .custom_flags(0x20000)
        .open(fd_path(directory).join(name))?;
    let named = named_file.metadata()?;
    check_file_metadata(&opened)?;
    check_file_metadata(&named)?;
    if opened.dev() != named.dev()
        || opened.ino() != named.ino()
        || mount_id(file)? != mount_id(directory)?
        || mount_id(&named_file)? != mount_id(directory)?
    {
        return Err(invalid("record file identity"));
    }
    Ok(())
}
fn check_file_metadata(metadata: &fs::Metadata) -> io::Result<()> {
    let uid = fs::metadata("/proc/self")?.uid();
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
    {
        return Err(invalid("record file privacy"));
    }
    Ok(())
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
