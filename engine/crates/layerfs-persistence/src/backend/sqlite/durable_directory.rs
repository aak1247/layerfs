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

    pub(super) fn verify_private(&self) -> io::Result<()> {
        self.verify()?;
        let owner = fs::metadata("/proc/self")?.uid();
        let directory = fs::symlink_metadata(&self.parent)?;
        let database = fs::symlink_metadata(&self.database)?;
        if !directory.is_dir()
            || directory.uid() != owner
            || directory.mode() & 0o077 != 0
            || database.uid() != owner
            || database.mode() & 0o077 != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "durable Store directory or database is not private",
            ));
        }
        Ok(())
    }

    /// Compare the caller's still-open original file with this session's
    /// captured Store identity and the current protected directory entry.
    pub(super) fn verify_original_file(&self, original: &File) -> io::Result<()> {
        self.verify_private()?;
        let metadata = original.metadata()?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || (metadata.dev(), metadata.ino()) != self.database_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "original Store file identity changed",
            ));
        }
        self.verify_private()
    }

    fn identity(&self) -> crate::StorePhysicalIdentity {
        crate::StorePhysicalIdentity {
            path: self.database.clone(),
            directory: self.directory_id,
            database: self.database_id,
        }
    }
}

impl super::connection::Session {
    /// The caller retains its authenticated original File and full Store fence
    /// across this opening window. Ordinary read-only opens have no such proof.
    pub(crate) fn connect_original_read_only(
        path: &Path,
        original: &File,
        layout: crate::SqlitePackLayout,
        acquisition: crate::SqliteAcquisitionSchema,
    ) -> Result<Self, BackendError> {
        let directory = DurableDirectory::open(path)?;
        directory
            .verify_original_file(original)
            .map_err(|_| BackendError::Integrity)?;
        let mut session = Self::connect(
            path,
            false,
            false,
            crate::SqlitePersistenceProfile::Durable,
            layout,
            acquisition,
        )?;
        directory
            .verify_original_file(original)
            .map_err(|_| BackendError::Integrity)?;
        session.durable_directory = Some(directory);
        Ok(session)
    }

    pub(crate) fn verify_original_store_file(&self, original: &File) -> Result<(), BackendError> {
        self.durable_directory
            .as_ref()
            .ok_or(BackendError::Integrity)?
            .verify_original_file(original)
            .map_err(|_| BackendError::Integrity)
    }

    pub(crate) fn observe_original_store(
        &self,
        original: &File,
    ) -> Result<crate::StorePhysicalIdentity, BackendError> {
        if self.writable || self.profile.persistence != crate::SqlitePersistenceProfile::Durable {
            return Err(BackendError::Integrity);
        }
        let directory = self
            .durable_directory
            .as_ref()
            .ok_or(BackendError::Integrity)?;
        directory
            .verify_original_file(original)
            .map_err(|_| BackendError::Integrity)?;
        Ok(directory.identity())
    }

    pub(crate) fn verify_observed_store(
        &self,
        original: &crate::StorePhysicalIdentity,
    ) -> Result<(), BackendError> {
        if !self.writable || self.profile.persistence != crate::SqlitePersistenceProfile::Durable {
            return Err(BackendError::Integrity);
        }
        let directory = self
            .durable_directory
            .as_ref()
            .ok_or(BackendError::Integrity)?;
        directory
            .verify_private()
            .map_err(|_| BackendError::Integrity)?;
        if &directory.identity() != original {
            return Err(BackendError::Integrity);
        }
        Ok(())
    }
}
