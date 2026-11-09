//! Rebind a durable owner to its original, independently recorded Branch view.
use super::{
    bind::checked_base, BindError, BindPhase, BindRefusal, BindRequest, BindSuccess,
    BoundWorkspace, Store,
};
use crate::OwnerClient;
use layerfs_history::{BranchSnapshot, HistoryCatalog, HistoryError};
use layerfs_workspace::Workspace;
use std::sync::{atomic::AtomicBool, Arc, Mutex};

impl Store {
    /// Binds an already opened durable owner to its exact recorded snapshot.
    ///
    /// The caller must authenticate the Workspace and snapshot from its durable
    /// recovery record, fence the old mount and consumers, and reconcile any
    /// unknown external publication before exposing this binding. Catalog
    /// validation proves the recorded roots exist in this Store; it does not
    /// prove that an untrusted caller was authorized to select them. A newer
    /// Branch head does not replace the original snapshot.
    #[allow(clippy::result_large_err)]
    pub fn bind_durable(
        self: &Arc<Self>,
        owner: OwnerClient,
        request: BindRequest,
        snapshot: BranchSnapshot,
    ) -> Result<BindSuccess, BindRefusal> {
        let mut phase = BindPhase::Snapshot;
        let refusal = |phase, error| BindRefusal {
            phase,
            request: request.clone(),
            snapshot: Some(snapshot.clone()),
            error,
        };
        let identity = owner
            .durable_identity()
            .map_err(|error| refusal(phase, BindError::Owner(error)))?;
        if identity != Some(request.workspace.to_bytes()) || snapshot.branch.id != request.branch {
            return Err(refusal(
                phase,
                BindError::History(HistoryError::Integrity("durable binding identity")),
            ));
        }
        self.readers
            .mount(request.workspace, false)
            .map_err(|error| refusal(phase, BindError::Read(error)))?;
        let result = (|| {
            let reader = self.reader(Some(request.workspace)).map_err(|error| {
                BindError::Read(Arc::new(super::PortError::ReadAdmission(error)))
            })?;
            reader
                .published(|history| validate_snapshot(history, &snapshot))
                .map_err(BindError::Read)?;
            phase = BindPhase::Root;
            let ports = self.ports_for(snapshot.scope, request.workspace);
            let base =
                checked_base(ports.client(), &snapshot).map_err(|error| BindError::Content {
                    error,
                    provider: ports
                        .failure()
                        .unwrap_or_else(|error| Some(Arc::new(error))),
                })?;
            phase = BindPhase::Open;
            let route = owner.bound_route().map_err(BindError::Owner)?;
            let state = owner.state(route).map_err(BindError::Owner)?;
            if state.base_root != snapshot.effective_root.to_bytes() {
                return Err(BindError::History(HistoryError::Integrity(
                    "durable binding root",
                )));
            }
            owner.claim_durable_binding().map_err(BindError::Owner)?;
            Ok(BindSuccess {
                workspace: BoundWorkspace {
                    store: self.clone(),
                    owner,
                    snapshot: Mutex::new(snapshot.clone()),
                    identity: request.workspace,
                    workspace: Arc::new(Workspace::bind(route, base)),
                    committing: AtomicBool::new(false),
                    producer: Default::default(),
                    pending: Mutex::new(None),
                },
            })
        })();
        result.map_err(|error| {
            let error = match self.close_reader(request.workspace) {
                Ok(()) => error,
                Err(cause) => BindError::Cleanup {
                    original: Box::new(error),
                    cause,
                },
            };
            refusal(phase, error)
        })
    }
}

fn validate_snapshot(
    history: &dyn HistoryCatalog,
    snapshot: &BranchSnapshot,
) -> Result<(), HistoryError> {
    let invalid = || HistoryError::Integrity("durable binding snapshot");
    let current = history
        .branch_snapshot(snapshot.branch.id)?
        .ok_or_else(invalid)?;
    if current.branch.stack != snapshot.branch.stack
        || current.scope != snapshot.scope
        || current.profile != snapshot.profile
    {
        return Err(invalid());
    }
    let layer = history
        .layer(snapshot.branch.base_layer)?
        .ok_or_else(invalid)?;
    if layer.stack != snapshot.branch.stack || layer.root != snapshot.base_root {
        return Err(invalid());
    }
    let head_root = match snapshot.branch.head_commit {
        Some(id) => {
            let commit = history.commit(id)?.ok_or_else(invalid)?;
            if commit.stack != snapshot.branch.stack
                || commit.base_layer != snapshot.branch.base_layer
            {
                return Err(invalid());
            }
            Some(commit.root)
        }
        None => None,
    };
    if head_root != snapshot.head_root || head_root.unwrap_or(layer.root) != snapshot.effective_root
    {
        return Err(invalid());
    }
    Ok(())
}
