//! One checked replacement of a private SDK record on its pinned directory.
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

/// The exact original boundary; attempted writes must not be replayed.
#[derive(Debug)]
pub(crate) struct Failure {
    pub attempted: bool,
    phase: &'static str,
    cause: io::Error,
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "durable record {}: {}", self.phase, self.cause)
    }
}

/// Replaces one record only after syncing its original file and directory.
/// Unknown effects retain the original file and directory until process exit.
pub(crate) fn save(path: &Path, temporary: &Path, bytes: &[u8]) -> Result<(), Failure> {
    let mut phase = "directory";
    let mut attempted = false;
    let mut directory = None;
    let mut file = None;
    let result = (|| -> io::Result<()> {
        let parent = path.parent().ok_or_else(|| invalid("record parent"))?;
        if temporary.parent() != Some(parent) {
            return Err(invalid("record temporary parent"));
        }
        let name = path.file_name().ok_or_else(|| invalid("record name"))?;
        let temp_name = temporary
            .file_name()
            .ok_or_else(|| invalid("record temporary name"))?;
        directory = Some(
            OpenOptions::new()
                .read(true)
                .custom_flags(0x20000)
                .open(parent)?,
        );
        let held = directory.as_ref().expect("directory held");
        let canonical = parent.canonicalize()?;
        check_directory(&canonical, held)?;
        let named = fd_path(held).join(name);
        match fs::symlink_metadata(&named) {
            Ok(metadata) => check_file_metadata(&metadata)?,
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
        check_directory(&canonical, held)?;
        phase = "publish";
        fs::rename(&temp, &named)?;
        check_file(held, name, original)?;
        phase = "sync directory";
        held.sync_all()?;
        check_directory(&canonical, held)?;
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

fn fd_path(directory: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
}
fn check_directory(path: &Path, directory: &File) -> io::Result<()> {
    let opened = directory.metadata()?;
    let named = fs::symlink_metadata(path)?;
    let uid = fs::metadata("/proc/self")?.uid();
    if !opened.is_dir()
        || !named.is_dir()
        || opened.uid() != uid
        || named.uid() != uid
        || opened.mode() & 0o077 != 0
        || named.mode() & 0o077 != 0
        || opened.dev() != named.dev()
        || opened.ino() != named.ino()
    {
        return Err(invalid("record directory identity or privacy"));
    }
    Ok(())
}
fn check_file(directory: &File, name: &OsStr, file: &File) -> io::Result<()> {
    let opened = file.metadata()?;
    let named = fs::symlink_metadata(fd_path(directory).join(name))?;
    check_file_metadata(&opened)?;
    check_file_metadata(&named)?;
    if opened.dev() != named.dev() || opened.ino() != named.ino() {
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
