//! Checked directory synchronization for a durable Store's WAL and main file.
use crate::backend::records::BackendError;
use std::{
    fs::{self, File},
    io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

pub(super) struct DurableDirectory {
    directory: File,
    parent: PathBuf,
    database: PathBuf,
    directory_id: (u64, u64),
    database_id: (u64, u64),
}
impl DurableDirectory {
    /// Duplicate the original directory description, even if its path moved.
    pub(super) fn writer_file(&self) -> io::Result<File> {
        self.directory.try_clone()
    }
    pub(super) fn open(database: &Path) -> Result<Self, BackendError> {
        let parent = database
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_owned();
        let directory = File::open(&parent).map_err(|_| BackendError::Unknown)?;
        let directory_meta = directory.metadata().map_err(|_| BackendError::Unknown)?;
        let database_meta = fs::symlink_metadata(database).map_err(|_| BackendError::Unknown)?;
        if !directory_meta.is_dir() || !database_meta.is_file() || database_meta.nlink() != 1 {
            return Err(BackendError::Integrity);
        }
        let result = Self {
            directory,
            parent,
            database: database.to_owned(),
            directory_id: (directory_meta.dev(), directory_meta.ino()),
            database_id: (database_meta.dev(), database_meta.ino()),
        };
        result.verify().map_err(|_| BackendError::Unknown)?;
        Ok(result)
    }

    pub(super) fn verify(&self) -> io::Result<()> {
        let opened = self.directory.metadata()?;
        let named = fs::metadata(&self.parent)?;
        let database = fs::symlink_metadata(&self.database)?;
        if !opened.is_dir()
            || !named.is_dir()
            || (opened.dev(), opened.ino()) != self.directory_id
            || (named.dev(), named.ino()) != self.directory_id
            || !database.is_file()
            || database.nlink() != 1
            || (database.dev(), database.ino()) != self.database_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "durable Store directory or database identity changed",
            ));
        }
        Ok(())
    }

    pub(super) fn sync(&self) -> io::Result<()> {
        self.verify()?;
        self.directory.sync_all()?;
        self.verify()
    }
}
