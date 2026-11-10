//! Bounded original Workspace observations, never a hidden Store refresh.
use super::WorkspaceApi;
use crate::OperationFailure;
use layerfs_bridge::control::{Reply, Request, WorkspaceStatus, WorkspaceToken};
use layerfs_history::WorkspaceId;

impl WorkspaceApi<'_> {
    /// Observes a Workspace recovered by the current daemon, without binding or
    /// mounting it. This observation never settles a previous unknown attempt;
    /// the controller must reconcile its original durable lifecycle journal.
    pub fn locate_recovered(
        &mut self,
        workspace: WorkspaceId,
    ) -> Result<WorkspaceStatus, Box<OperationFailure>> {
        self.locate(workspace)
    }
    /// Observes existing maintained state once. No unknown outcome is settled.
    pub fn status(
        &mut self,
        token: WorkspaceToken,
    ) -> Result<WorkspaceStatus, Box<OperationFailure>> {
        let request = Request::Status(token);
        match self.exchange(request.clone())? {
            Reply::Status(status) => Ok(*status),
            reply => Err(OperationFailure::unexpected(request, reply)),
        }
    }
}
