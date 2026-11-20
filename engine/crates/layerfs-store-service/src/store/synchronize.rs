//! A bound Workspace's Store and Overlay barriers for one kernel descriptor.
use super::StoreOperation;
use crate::OwnerError;
use layerfs_content::ObjectId;
use layerfs_history::BranchSnapshot;
use layerfs_overlay::{ConflictSyncIdentity, KernelMount, OverlayError};
use layerfs_persistence::DurableBarrierError;
use layerfs_workspace::WorkspaceError;
use std::{fmt, sync::Arc};

/// Captured only from the actual bound Workspace's private projection.
pub(super) struct ConflictProjection {
    pub workspace: [u8; 32],
    pub catalog: [u8; 32],
    pub incarnation: u64,
    pub snapshot: BranchSnapshot,
    pub marker_root: ObjectId,
    pub descriptor: [u8; 32],
}

/// The outcome of this request's one local barrier attempt.
pub enum SynchronizeStep {
    Complete,
    Stopped,
    Unavailable,
}

/// Original failure before or during the Store or Workspace barrier.
#[derive(Debug)]
pub enum SynchronizeFailure {
    Store(DurableBarrierError),
    Workspace(WorkspaceError),
    Binding(layerfs_bridge::control::ControlError),
    Owner(OwnerError),
}
impl fmt::Display for SynchronizeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Workspace synchronize: {self:?}")
    }
}
impl std::error::Error for SynchronizeFailure {}

impl StoreOperation {
    /// Runs the actual Store barrier followed by the exact bound Overlay's
    /// cancellable barrier. No caller-supplied conflict identity is accepted.
    pub fn synchronize_cancellable(
        &self,
        mount: KernelMount,
        serial: u64,
        handle: u64,
        directory: bool,
        stopped: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<SynchronizeStep, SynchronizeFailure> {
        if stopped() {
            return Ok(SynchronizeStep::Stopped);
        }
        self.durable_barrier_cancellable(&|| stopped())
            .map_err(SynchronizeFailure::Store)?;
        if stopped() {
            return Ok(SynchronizeStep::Stopped);
        }
        let route = self.workspace.route();
        let expected = if let Some(conflict) = &self.sync_conflict {
            let base = self
                .workspace
                .base()
                .map_err(SynchronizeFailure::Workspace)?;
            if base.identity().0 != conflict.marker_root {
                return Ok(SynchronizeStep::Unavailable);
            }
            let snapshot = layerfs_bridge::snapshot::encode(&conflict.snapshot)
                .map_err(SynchronizeFailure::Binding)?;
            Some((
                conflict.workspace,
                conflict.catalog,
                conflict.incarnation,
                conflict.marker_root.to_bytes(),
                snapshot,
                conflict.descriptor,
            ))
        } else {
            None
        };
        let action_stopped = stopped.clone();
        self.owner
            .call_cancellable(route, stopped, move |db| {
                if action_stopped() {
                    return Ok(SynchronizeStep::Stopped);
                }
                let ready = match &expected {
                    Some((workspace, catalog, incarnation, marker_root, snapshot, descriptor)) => {
                        db.require_conflict_synchronize_ready(&ConflictSyncIdentity {
                            workspace: *workspace,
                            catalog: *catalog,
                            incarnation: *incarnation,
                            namespace: route.namespace(),
                            marker_root: *marker_root,
                            target_snapshot: snapshot,
                            descriptor: *descriptor,
                        })
                    }
                    None => db.require_recorded_recovery_ready(),
                };
                match ready {
                    Ok(()) => {}
                    Err(OverlayError::Invalid(
                        "durable binding profile"
                        | "durable binding is absent"
                        | "durable binding requires reconciliation"
                        | "durable overlay requires reconciliation"
                        | "durable conflict requires reconciliation"
                        | "durable conflict synchronization identity",
                    )) => return Ok(SynchronizeStep::Unavailable),
                    Err(error) => return Err(error),
                }
                db.synchronize_kernel(mount, serial, handle, directory)?;
                Ok(SynchronizeStep::Complete)
            })
            .map_err(SynchronizeFailure::Owner)
    }
}
