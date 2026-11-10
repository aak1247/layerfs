//! One mount with caller-owned persistence at each original boundary.
use super::{BoundWorkspace, MountedWorkspace, WorkspaceApi};
use crate::OperationFailure;
use layerfs_bridge::control::ReadyMount;
use layerfs_history::{BranchId, WorkspaceId};
use std::{fmt, io};

/// The original ordered boundaries of one bind and attachment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountMilestone {
    /// Before sending Bind.
    BeforeBind,
    /// The original Bind acknowledgement has been received.
    Bound,
    /// Before sending Attach for that binding.
    BeforeAttach,
    /// The original kernel Ready acknowledgement has been received.
    Attached,
}

/// The exact acknowledged owners available at a mount boundary.
pub struct MountObservation<'a> {
    /// The original boundary being recorded.
    pub milestone: MountMilestone,
    /// The caller's original Workspace identity.
    pub workspace: WorkspaceId,
    /// The caller's original Branch identity.
    pub branch: BranchId,
    /// The original binding, when acknowledged.
    pub bound: Option<&'a BoundWorkspace>,
    /// The original kernel readiness receipt, when acknowledged.
    pub ready: Option<&'a ReadyMount>,
}

/// The original failed operation or persistence observer.
#[derive(Debug)]
pub enum MountCause {
    /// An original daemon operation failed, with its exact request and response state.
    Operation(Box<OperationFailure>),
    /// The observer failed while recording this boundary.
    Observer(io::Error),
}

/// A stopped mount retains its original acknowledged binding and kernel receipt.
#[derive(Debug)]
pub struct ObservedMountFailure {
    /// The boundary at which the attempt stopped.
    pub milestone: MountMilestone,
    /// The original Workspace identity.
    pub workspace: WorkspaceId,
    /// The original Branch identity.
    pub branch: BranchId,
    /// The binding, if its acknowledgement arrived.
    pub bound: Option<BoundWorkspace>,
    /// The kernel receipt, if its acknowledgement arrived.
    pub ready: Option<ReadyMount>,
    /// The original failure; no operation is repeated or undone.
    pub cause: MountCause,
}
impl fmt::Display for ObservedMountFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Workspace mount {:?}: {:?}", self.milestone, self.cause)
    }
}
impl std::error::Error for ObservedMountFailure {}

impl WorkspaceApi<'_> {
    /// Attaches one previously recovered binding once, without rebinding it.
    /// The caller must authenticate the binding, durably record its recovery
    /// intent and establish that no prior Attach remains unresolved. In
    /// particular, a later status observation does not settle an unknown Attach.
    /// The observer must persist each original boundary before returning; on
    /// failure, the original binding and any Ready receipt are retained.
    pub fn attach_bound_observed(
        &mut self,
        bound: BoundWorkspace,
        mut observer: impl FnMut(MountObservation<'_>) -> io::Result<()>,
    ) -> Result<MountedWorkspace, Box<ObservedMountFailure>> {
        let workspace = bound.token.workspace;
        let branch = bound.binding.branch.id;
        if let Err(cause) = observer(MountObservation {
            milestone: MountMilestone::BeforeAttach,
            workspace,
            branch,
            bound: Some(&bound),
            ready: None,
        }) {
            return Err(Box::new(ObservedMountFailure {
                milestone: MountMilestone::BeforeAttach,
                workspace,
                branch,
                bound: Some(bound),
                ready: None,
                cause: MountCause::Observer(cause),
            }));
        }
        let ready = self.attach(bound.token).map_err(|cause| {
            Box::new(ObservedMountFailure {
                milestone: MountMilestone::BeforeAttach,
                workspace,
                branch,
                bound: Some(bound.clone()),
                ready: None,
                cause: MountCause::Operation(cause),
            })
        })?;
        if let Err(cause) = observer(MountObservation {
            milestone: MountMilestone::Attached,
            workspace,
            branch,
            bound: Some(&bound),
            ready: Some(&ready),
        }) {
            return Err(Box::new(ObservedMountFailure {
                milestone: MountMilestone::Attached,
                workspace,
                branch,
                bound: Some(bound),
                ready: Some(ready),
                cause: MountCause::Observer(cause),
            }));
        }
        Ok(MountedWorkspace { bound, ready })
    }

    /// Mounts once while a caller records each boundary before the next effect.
    /// Observer failures keep all acknowledgements and never send the next request.
    pub fn mount_observed(
        &mut self,
        workspace: WorkspaceId,
        branch: BranchId,
        mut observer: impl FnMut(MountObservation<'_>) -> io::Result<()>,
    ) -> Result<MountedWorkspace, Box<ObservedMountFailure>> {
        let mut bound = None;
        let mut ready = None;
        let mut record = |milestone, bound: &Option<BoundWorkspace>, ready: &Option<ReadyMount>| {
            observer(MountObservation {
                milestone,
                workspace,
                branch,
                bound: bound.as_ref(),
                ready: ready.as_ref(),
            })
        };
        macro_rules! observe {
            ($milestone:expr) => {
                if let Err(cause) = record($milestone, &bound, &ready) {
                    return Err(Box::new(ObservedMountFailure {
                        milestone: $milestone,
                        workspace,
                        branch,
                        bound,
                        ready,
                        cause: MountCause::Observer(cause),
                    }));
                }
            };
        }
        observe!(MountMilestone::BeforeBind);
        bound = Some(self.bind(workspace, branch).map_err(|cause| {
            Box::new(ObservedMountFailure {
                milestone: MountMilestone::BeforeBind,
                workspace,
                branch,
                bound: None,
                ready: None,
                cause: MountCause::Operation(cause),
            })
        })?);
        observe!(MountMilestone::Bound);
        observe!(MountMilestone::BeforeAttach);
        ready = Some(
            match self.attach(bound.as_ref().expect("bound acknowledged").token) {
                Ok(ready) => ready,
                Err(cause) => {
                    return Err(Box::new(ObservedMountFailure {
                        milestone: MountMilestone::BeforeAttach,
                        workspace,
                        branch,
                        bound,
                        ready: None,
                        cause: MountCause::Operation(cause),
                    }))
                }
            },
        );
        observe!(MountMilestone::Attached);
        Ok(MountedWorkspace {
            bound: bound.expect("bound acknowledged"),
            ready: ready.expect("ready acknowledged"),
        })
    }
}
