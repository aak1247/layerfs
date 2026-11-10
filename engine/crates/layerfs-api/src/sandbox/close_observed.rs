//! One explicit Sandbox stop and deletion with caller-owned durable observations.
use super::{ManagedSandbox, SandboxApi};
use layerfs_sandbox::WireFailure;
use std::{fmt, io};

/// Ordered boundaries for the original one-attempt Sandbox retirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxCloseMilestone {
    /// Before sending the one container Stop.
    BeforeStop,
    /// The original Stop returned, including any failure.
    StopFinished,
    /// Before sending Delete after an acknowledged Stop.
    BeforeDelete,
    /// The original Delete returned, including any failure.
    DeleteFinished,
}

/// The original Sandbox API result at a retirement boundary.
#[derive(Debug)]
pub enum SandboxCloseAttempt {
    /// The original operation was acknowledged.
    Acknowledged,
    /// The exact original failure, including its request and response custody.
    Failed(Box<WireFailure>),
}

/// Information available for one caller-owned journal write.
pub struct SandboxCloseObservation<'a> {
    /// Boundary of the original attempt.
    pub milestone: SandboxCloseMilestone,
    /// Original owner and its exact container identity.
    pub owner: &'a ManagedSandbox,
    /// The original result, present only at a finished boundary.
    pub attempt: Option<&'a SandboxCloseAttempt>,
}

/// A stopped retirement retains its owner and each original outcome.
pub struct ObservedSandboxCloseFailure {
    /// Boundary where this attempt stopped.
    pub milestone: SandboxCloseMilestone,
    /// Original runtime and authenticated control owner; no retry is implied.
    pub owner: ManagedSandbox,
    /// The original Stop result, if it returned.
    pub stop: Option<SandboxCloseAttempt>,
    /// The original Delete result, if it returned.
    pub delete: Option<SandboxCloseAttempt>,
    /// Original journal failure, when the observer could not commit.
    pub observer_error: Option<io::Error>,
}

impl fmt::Debug for ObservedSandboxCloseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservedSandboxCloseFailure")
            .field("milestone", &self.milestone)
            .field("container", &self.owner.runtime.identity())
            .field("stop", &self.stop)
            .field("delete", &self.delete)
            .field("observer_error", &self.observer_error)
            .finish()
    }
}
impl fmt::Display for ObservedSandboxCloseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sandbox close {:?}: ", self.milestone)?;
        if let Some(error) = &self.observer_error {
            write!(f, "observer: {error}")
        } else {
            write!(f, "stop={:?}, delete={:?}", self.stop, self.delete)
        }
    }
}
impl std::error::Error for ObservedSandboxCloseFailure {}

impl ObservedSandboxCloseFailure {
    /// Original deployment inputs, including private authority, kept by the owner.
    pub fn request(&self) -> &super::SandboxCreate {
        &self.owner.request
    }
}

impl SandboxApi {
    /// Stops and deletes the exact owner once while the caller records each boundary.
    ///
    /// The observer must durably record an intent before returning from each
    /// `Before` boundary and retain each original result before returning from
    /// its `Finished` boundary. A failed record stops before the next effect.
    /// This is not a Workspace drain, does not delete the borrowed Store, and
    /// never uses later observations to settle an unknown Engine response.
    pub fn close_observed(
        &self,
        mut owner: ManagedSandbox,
        mut observer: impl FnMut(SandboxCloseObservation<'_>) -> io::Result<()>,
    ) -> Result<(), Box<ObservedSandboxCloseFailure>> {
        let mut stop = None;
        let mut delete = None;
        macro_rules! observe {
            ($milestone:expr, $attempt:expr) => {
                if let Err(error) = observer(SandboxCloseObservation {
                    milestone: $milestone,
                    owner: &owner,
                    attempt: $attempt,
                }) {
                    return Err(Box::new(ObservedSandboxCloseFailure {
                        milestone: $milestone,
                        owner,
                        stop,
                        delete,
                        observer_error: Some(error),
                    }));
                }
            };
        }
        observe!(SandboxCloseMilestone::BeforeStop, None);
        stop = Some(match owner.runtime.stop(1) {
            Ok(()) => SandboxCloseAttempt::Acknowledged,
            Err(failure) => SandboxCloseAttempt::Failed(failure),
        });
        observe!(SandboxCloseMilestone::StopFinished, stop.as_ref());
        if matches!(stop, Some(SandboxCloseAttempt::Failed(_))) {
            return Err(Box::new(ObservedSandboxCloseFailure {
                milestone: SandboxCloseMilestone::StopFinished,
                owner,
                stop,
                delete,
                observer_error: None,
            }));
        }
        observe!(SandboxCloseMilestone::BeforeDelete, None);
        delete = Some(match owner.runtime.delete() {
            Ok(()) => SandboxCloseAttempt::Acknowledged,
            Err(failure) => SandboxCloseAttempt::Failed(failure),
        });
        observe!(SandboxCloseMilestone::DeleteFinished, delete.as_ref());
        if matches!(delete, Some(SandboxCloseAttempt::Failed(_))) {
            return Err(Box::new(ObservedSandboxCloseFailure {
                milestone: SandboxCloseMilestone::DeleteFinished,
                owner,
                stop,
                delete,
                observer_error: None,
            }));
        }
        Ok(())
    }
}
