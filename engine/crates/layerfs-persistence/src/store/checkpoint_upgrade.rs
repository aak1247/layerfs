//! One locally authenticated Store receipt-schema transition.
use crate::{backend::Session, BackendSelection, PersistenceConfig, SqlitePersistenceProfile};
use layerfs_history::CatalogId;
use layerfs_storage::port::PersistenceError;
use std::{fmt, fs::File, os::unix::fs::MetadataExt, path::PathBuf, sync::Arc};

/// The local identity of a read-only Store session and its original open file.
/// It does not authenticate the Store's external owner or fence old consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorePhysicalIdentity {
    pub(crate) path: PathBuf,
    pub(crate) directory: (u64, u64),
    pub(crate) database: (u64, u64),
}

impl crate::StorageProvider {
    /// Compare a specially opened read-only Durable session with the caller's
    /// still-held original file and the captured private directory identity.
    /// Ordinary read-only sessions cannot establish this observation later.
    pub fn observe_original_store(
        &self,
        original: &File,
    ) -> Result<StorePhysicalIdentity, PersistenceError> {
        self.session
            .observe_original_store(original)
            .map_err(Into::into)
    }

    /// Verify that this newly opened Durable writer is the observed file.
    pub fn verify_observed_store(
        &self,
        original: &StorePhysicalIdentity,
    ) -> Result<(), PersistenceError> {
        self.session
            .verify_observed_store(original)
            .map_err(Into::into)
    }
}

/// An exclusive local migration session and the original open Store file.
///
/// Before preparation, a trusted application must authenticate the original
/// Store and roster and fence all prior readers, writers, mounts and consumers
/// across the Store. This handle checks local identity and owns one Session; it
/// does not establish that external fence. Keep the fence until the migrated
/// Store and all readers are reopened and verified. No provider from before
/// the transition may be reused after it.
pub struct PreparedCheckpointUpgrade {
    custody: Custody,
    catalog: CatalogId,
    binding: Vec<u8>,
    incarnation: u64,
}

/// A known Store transition result and the same original open file.
pub struct CheckpointUpgradeResult {
    /// True if the legacy schema was upgraded; false if it was already current.
    pub applied: bool,
    /// The original file remains open for the caller's continuing custody.
    /// Close it before opening the new SQLite Store, or retain it until all
    /// new connections to the same inode have closed.
    pub original: File,
}

/// The original cause and custody of a refused or uncertain transition.
pub struct CheckpointUpgradeFailure {
    cause: PersistenceError,
    custody: Custody,
}

// Armed during SQLite calls. The product's own transaction guard retains an
// uncertain Session on unwind; its paired original File must outlive it too.
struct Custody {
    retain: bool,
    session: Option<Arc<Session>>,
    original: Option<File>,
}

impl Custody {
    fn new(original: File) -> Self {
        Self {
            retain: true,
            session: None,
            original: Some(original),
        }
    }
}

impl Drop for Custody {
    fn drop(&mut self) {
        if self.retain {
            if let Some(session) = self.session.take() {
                std::mem::forget(session);
            }
            if let Some(original) = self.original.take() {
                std::mem::forget(original);
            }
        } else {
            drop(self.session.take());
            drop(self.original.take());
        }
    }
}

impl fmt::Debug for CheckpointUpgradeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckpointUpgradeFailure")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for CheckpointUpgradeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "checkpoint schema upgrade: {}", self.cause)
    }
}
impl std::error::Error for CheckpointUpgradeFailure {}

impl CheckpointUpgradeFailure {
    fn new(cause: PersistenceError, mut custody: Custody) -> Box<Self> {
        custody.retain = cause == PersistenceError::Uncertain;
        Box::new(Self { cause, custody })
    }

    /// The original failure classification; an uncertain result is observed
    /// only after the original writer has actually exited.
    pub fn cause(&self) -> &PersistenceError {
        &self.cause
    }

    /// Recover the original open file only after a definite failure and after
    /// the migration session has been closed. Unknown outcomes retain custody.
    pub fn release_original(mut self: Box<Self>) -> Result<File, Box<Self>> {
        if self.custody.retain {
            return Err(self);
        }
        drop(self.custody.session.take());
        Ok(self.custody.original.take().expect("original Store file"))
    }
}

impl PreparedCheckpointUpgrade {
    /// Prepare without exposing a writer, reader factory, Workspace or route.
    /// The original file must be the Store authenticated by the caller and
    /// must remain the same physical file as `config.path`.
    pub fn prepare(
        config: PersistenceConfig,
        original: File,
        binding: &[u8],
        incarnation: u64,
    ) -> Result<Self, Box<CheckpointUpgradeFailure>> {
        let mut custody = Custody::new(original);
        if config.backend != BackendSelection::Sqlite
            || config.sqlite_profile != SqlitePersistenceProfile::Durable
            || incarnation == 0
        {
            return Err(CheckpointUpgradeFailure::new(
                PersistenceError::Malformed,
                custody,
            ));
        }
        let named = std::fs::symlink_metadata(&config.path);
        let opened = custody
            .original
            .as_ref()
            .expect("original Store file")
            .metadata();
        if !matches!((named, opened), (Ok(named), Ok(opened))
            if named.is_file() && opened.is_file() && named.nlink()==1 && opened.nlink()==1
                && (named.dev(),named.ino())==(opened.dev(),opened.ino()))
        {
            return Err(CheckpointUpgradeFailure::new(
                PersistenceError::Malformed,
                custody,
            ));
        }
        let catalog = match CatalogId::derive(binding) {
            Ok(value) => value,
            Err(_) => {
                return Err(CheckpointUpgradeFailure::new(
                    PersistenceError::Malformed,
                    custody,
                ))
            }
        };
        custody.session = Some(
            match Session::connect(
                &config.path,
                true,
                false,
                config.sqlite_profile,
                config.sqlite_pack_layout,
                config.sqlite_acquisition,
            ) {
                Ok(value) => value.shared(),
                Err(error) => return Err(CheckpointUpgradeFailure::new(error.into(), custody)),
            },
        );
        let session = custody.session.as_ref().expect("migration Session");
        let checked = session
            .verify_original_store_file(custody.original.as_ref().expect("original Store file"))
            .and_then(|()| session.validate(catalog, binding))
            .and_then(|(actual, _)| {
                if actual == incarnation {
                    Ok(())
                } else {
                    Err(crate::backend::records::BackendError::Integrity)
                }
            });
        if let Err(error) = checked {
            return Err(CheckpointUpgradeFailure::new(error.into(), custody));
        }
        custody.retain = false;
        Ok(Self {
            custody,
            catalog,
            binding: binding.to_vec(),
            incarnation,
        })
    }

    /// Attempt the locally authenticated transition once. An unknown result
    /// retains the original writer and file until process exit; it is not retried.
    pub fn apply(self) -> Result<CheckpointUpgradeResult, Box<CheckpointUpgradeFailure>> {
        let Self {
            mut custody,
            catalog,
            binding,
            incarnation,
        } = self;
        custody.retain = true;
        let result = custody
            .session
            .as_ref()
            .expect("migration Session")
            .upgrade_checkpoint_receipts(
                catalog,
                &binding,
                incarnation,
                custody.original.as_ref().expect("original Store file"),
            );
        match result {
            Ok(applied) => {
                custody.retain = false;
                drop(custody.session.take());
                let original = custody.original.take().expect("original Store file");
                Ok(CheckpointUpgradeResult { applied, original })
            }
            Err(error) => Err(CheckpointUpgradeFailure::new(error.into(), custody)),
        }
    }
}
