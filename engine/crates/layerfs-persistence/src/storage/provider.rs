//! Shared physical provider; Objects and Metadata use one transaction boundary.
//! Object locates, pack reads and inode-value reads see only immutable rows
//! and run without one (`Session::read_immutable`); every other call keeps
//! its transaction.
use crate::{backend::Session, metadata, objects, publication};
use layerfs_content::ObjectId;
use layerfs_storage::{
    location::{InodeValueRow, LocatedObject, SignatureRow},
    port::*,
    StoragePolicy,
};
use std::sync::Arc;
/// A durable Store barrier stopped before its sync or failed with its exact
/// existing persistence outcome.
#[derive(Debug)]
pub enum DurableBarrierError {
    /// Waiting ended before the synchronization attempt began.
    Cancelled,
    /// The operation's original persistence outcome, possibly uncertain.
    Failed(PersistenceError),
}
impl std::fmt::Display for DurableBarrierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "durable Store barrier: {self:?}")
    }
}
impl std::error::Error for DurableBarrierError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cancelled => None,
            Self::Failed(cause) => Some(cause),
        }
    }
}
/// Application-owned bounded physical persistence over the combined Store.
pub struct StorageProvider {
    pub(crate) session: Arc<Session>,
}
impl StorageProvider {
    /// Whether the concrete Linux writer has a private Durable Store at the
    /// time of this check; the barrier rechecks it under writer ownership.
    pub fn supports_durable_barrier(&self) -> bool {
        self.session.supports_durable_barrier()
    }
    /// Check a Linux Durable Store under its original cooperative writer
    /// ownership. Its initial installed file must already be durable.
    pub fn durable_barrier(&self) -> Result<(), PersistenceError> {
        self.session.durable_barrier().map_err(Into::into)
    }
    /// Wait for the Store owner, unless `stopped` ends the still-unattempted
    /// wait. Once synchronization begins, its original result is returned.
    pub fn durable_barrier_cancellable(
        &self,
        stopped: &dyn Fn() -> bool,
    ) -> Result<(), DurableBarrierError> {
        self.session
            .durable_barrier_cancellable(stopped)
            .map_err(|cause| match cause {
                crate::backend::records::BackendError::Cancelled => DurableBarrierError::Cancelled,
                other => DurableBarrierError::Failed(other.into()),
            })
    }
    /// Merge build records over this session (#355); writable sessions only.
    pub fn merge_scratch(&self) -> crate::MergeScratch {
        crate::MergeScratch {
            session: self.session.clone(),
        }
    }
    /// Read-only counters from this already-open session, without a SQL query.
    /// Application observers need no history or acquisition capability.
    pub fn diagnostics(&self) -> Result<crate::SqlWork, PersistenceError> {
        self.session.diagnostics().map_err(Into::into)
    }
}
impl PackPersistence for StorageProvider {
    fn policy(&self) -> Result<StoragePolicy, PersistenceError> {
        self.session.run(false, metadata::policy::read)
    }
    fn locate(
        &self,
        ids: &[ObjectId],
        out: &mut Vec<LocatedObject>,
    ) -> Result<(), PersistenceError> {
        out.clear();
        self.session
            .read_immutable(|tx| metadata::locations::read(tx, ids, out))
    }
    fn read_packs(
        &self,
        ids: &[i64],
        out: &mut Vec<PersistedPack>,
    ) -> Result<(), PersistenceError> {
        out.clear();
        self.session
            .read_immutable(|tx| objects::read::read(tx, ids, out))
    }
    fn read_pack_selection(
        &self,
        id: i64,
        plan: &mut dyn PackReadPlan,
    ) -> Result<PersistedPackRead, PersistenceError> {
        self.session
            .read_immutable(|tx| crate::backend::sqlite::objects_selection::read(tx, id, plan))
    }
    fn read_scoped_pack(
        &self,
        id: i64,
        plan: &mut dyn PackReadPlan,
    ) -> Result<AcquiredPackRead, PersistenceError> {
        self.session.read_immutable(|tx| {
            crate::backend::sqlite::objects_selection::read_scoped(tx, id, plan)
        })
    }
    fn inode_values(
        &self,
        ordinals: &[u32],
        out: &mut Vec<InodeValueRow>,
    ) -> Result<(), PersistenceError> {
        out.clear();
        self.session
            .read_immutable(|tx| metadata::values::read(tx, ordinals, out))
    }
    fn signatures(&self, out: &mut Vec<SignatureRow>) -> Result<(), PersistenceError> {
        out.clear();
        self.session
            .run(false, |tx| metadata::signatures::read(tx, out))
    }
    fn reserve(&self, request: Reserve) -> Result<Reserved, PersistenceError> {
        self.session
            .run(true, |tx| metadata::allocation::reserve(tx, request))
    }
    fn publication_pack_cost(
        &self,
        pack: &PublishedPack,
    ) -> Result<(usize, u64), PersistenceError> {
        match self.session.layout {
            crate::SqlitePackLayout::Monolithic => Ok((1, pack.body.len() as u64)),
            crate::SqlitePackLayout::GroupRows | crate::SqlitePackLayout::GroupRowsIndexed => {
                crate::backend::sqlite::unit_layout::cost(pack)
            }
        }
    }
    fn publish(&self, batch: &Publication) -> Result<Published, PersistenceError> {
        publication::validate(batch)?;
        if self.session.layout.uses_units() {
            crate::backend::sqlite::unit_layout::validate_batch(batch)?;
        }
        // Only pack bytes are guarded; metadata-only batches may use the margin.
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        if !batch.packs.is_empty() {
            let bytes = batch.packs.iter().map(|pack| pack.body.len() as u64).sum();
            crate::backend::sqlite::headroom::admit(&self.session.path, bytes)?;
        }
        self.session.run(true, |tx| publication::publish(tx, batch))
    }
}
