//! Fresh operation custody over shared Workspace identity, base and serial ranges.
use super::{Store, StorePorts};
use crate::OwnerClient;
use layerfs_history::{BranchSnapshot, WorkspaceId};
use layerfs_workspace::{CanonicalClient, Workspace, WorkspaceResult};
use std::sync::{atomic::AtomicBool, Arc, Mutex};

pub struct BoundWorkspace {
    pub(super) store: Arc<Store>,
    pub(super) owner: OwnerClient,
    pub(super) snapshot: Mutex<BranchSnapshot>,
    pub(super) identity: WorkspaceId,
    pub(super) workspace: Arc<Workspace>,
    pub(super) committing: AtomicBool,
    pub(super) producer: super::producer::Producer,
    /// A conflicted merge installed locally and not yet committed.
    pub(super) pending: Mutex<Option<super::commit_merge::PendingMerge>>,
}
pub struct StoreOperation {
    workspace: Workspace,
    ports: Arc<StorePorts>,
    client: Arc<CanonicalClient>,
    owner: OwnerClient,
    durable_writer: Option<Arc<layerfs_persistence::StorageProvider>>,
}
impl BoundWorkspace {
    /// Terminal local close, then the Workspace's opened Store reader.
    /// A failed boundary retains its original cause and permits no replay.
    pub fn close(&self) -> Result<(), super::CloseError> {
        self.owner
            .close(self.route())
            .map_err(super::CloseError::Owner)?;
        self.producer.hand_to(&self.store.idle_producer);
        self.store
            .close_reader(self.identity)
            .map_err(super::CloseError::Reader)
    }
    pub fn operation(&self) -> WorkspaceResult<StoreOperation> {
        let ports = self.store.ports_for(self.snapshot()?.scope, self.identity);
        let client = ports.client();
        let workspace = self.workspace.scoped(client.clone())?;
        Ok(StoreOperation {
            workspace,
            ports,
            client,
            owner: self.owner.clone(),
            durable_writer: self.store.durable_writer.clone(),
        })
    }
    pub fn snapshot(&self) -> WorkspaceResult<BranchSnapshot> {
        self.snapshot
            .lock()
            .map(|snapshot| snapshot.clone())
            .map_err(|_| layerfs_workspace::WorkspaceError::BindingPoisoned)
    }
    pub const fn identity(&self) -> WorkspaceId {
        self.identity
    }
    pub fn route(&self) -> layerfs_workspace::Route {
        self.workspace.route()
    }
    /// True only for the exact in-process owner of this binding.
    pub fn owned_by(&self, owner: &OwnerClient) -> bool {
        self.owner.same_workspace(owner)
    }
    /// True only for the exact Store that authenticated this binding.
    pub fn belongs_to(&self, store: &Arc<Store>) -> bool {
        Arc::ptr_eq(&self.store, store)
    }
    /// A missing owner classification is treated as durable for sync refusal.
    pub fn has_durable_overlay(&self) -> bool {
        self.owner
            .durable_identity()
            .map_or(true, |identity| identity.is_some())
    }
    /// True only when the owner's durable identity matches this Workspace.
    pub fn has_verified_durable_overlay(&self) -> bool {
        self.owner
            .durable_identity()
            .is_ok_and(|identity| identity == Some(self.identity.to_bytes()))
    }
    /// True only for a concrete Linux Durable Store opened by this adapter.
    pub fn has_durable_store(&self) -> bool {
        self.store.durable_writer.is_some()
    }
}
impl StoreOperation {
    /// Verify the concrete Store under its original writer ownership.
    pub fn durable_barrier(&self) -> Result<(), layerfs_storage::port::PersistenceError> {
        self.durable_writer
            .as_ref()
            .ok_or(layerfs_storage::port::PersistenceError::Malformed)?
            .durable_barrier()
    }
    /// A cancellable readiness wait followed by the Store's original barrier.
    pub fn durable_barrier_cancellable(
        &self,
        stopped: &dyn Fn() -> bool,
    ) -> Result<(), layerfs_persistence::DurableBarrierError> {
        self.durable_writer
            .as_ref()
            .ok_or(layerfs_persistence::DurableBarrierError::Failed(
                layerfs_storage::port::PersistenceError::Malformed,
            ))?
            .durable_barrier_cancellable(stopped)
    }
    pub fn client_handle(&self) -> Arc<CanonicalClient> {
        self.client.clone()
    }
    pub const fn workspace(&self) -> &Workspace {
        &self.workspace
    }
    pub fn client(&self) -> &CanonicalClient {
        &self.client
    }
    /// The Store's memory-only client, for base facts read inside an owner job.
    pub fn resident(&self) -> Arc<CanonicalClient> {
        self.ports.resident()
    }
    pub fn ports(&self) -> &StorePorts {
        &self.ports
    }
    pub const fn overlay(&self) -> &OwnerClient {
        &self.owner
    }
}
