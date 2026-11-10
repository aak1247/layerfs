//! One explicit normal Unmount, preserving its reply when journaling fails.
use super::{Unmounted, WorkspaceApi};
use crate::OperationFailure;
use layerfs_bridge::control::WorkspaceToken;
use std::{fmt, io};

/// Boundaries of one original normal Unmount.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnmountMilestone {
    /// Before sending the request to the daemon.
    BeforeUnmount,
    /// The original operation returned, including any refusal or failure.
    Finished,
}

/// The original one-attempt result, never a retry instruction.
#[derive(Debug)]
pub enum UnmountAttempt {
    /// The daemon acknowledged closure or a reversible Dirty refusal.
    Acknowledged(Unmounted),
    /// The original exchange, refusal or retained teardown custody.
    Failed(Box<OperationFailure>),
}

/// The exact information available to the caller's lifecycle journal.
pub struct UnmountObservation<'a> {
    /// Current boundary of this original attempt.
    pub milestone: UnmountMilestone,
    /// Original authenticated Workspace token.
    pub token: WorkspaceToken,
    /// Original caller choice to discard uncommitted data.
    pub discard: bool,
    /// Original result when the operation has returned.
    pub attempt: Option<&'a UnmountAttempt>,
}

/// A stopped Unmount preserves its original result and observer failure.
#[derive(Debug)]
pub struct ObservedUnmountFailure {
    /// Boundary at which the operation stopped.
    pub milestone: UnmountMilestone,
    /// Original Workspace token.
    pub token: WorkspaceToken,
    /// Original discard choice.
    pub discard: bool,
    /// Original result, absent when the request was not sent.
    pub attempt: Option<UnmountAttempt>,
    /// Caller journal error, when the observer failed.
    pub observer_error: Option<io::Error>,
}
impl fmt::Display for ObservedUnmountFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Workspace unmount {:?}: ", self.milestone)?;
        if let Some(error) = &self.observer_error {
            write!(f, "observer: {error}")
        } else {
            write!(f, "{:?}", self.attempt)
        }
    }
}
impl std::error::Error for ObservedUnmountFailure {}

impl WorkspaceApi<'_> {
    /// Sends one normal Unmount while the caller records both boundaries.
    /// The observer must durably retain the intent before returning from
    /// `BeforeUnmount` and the original result before returning from `Finished`.
    /// Unknown outcomes are not retried or resolved by a later status query.
    /// This does not send the separate forced teardown operation.
    pub fn unmount_observed(
        &mut self,
        token: WorkspaceToken,
        discard: bool,
        mut observer: impl FnMut(UnmountObservation<'_>) -> io::Result<()>,
    ) -> Result<Unmounted, Box<ObservedUnmountFailure>> {
        if let Err(error) = observer(UnmountObservation {
            milestone: UnmountMilestone::BeforeUnmount,
            token,
            discard,
            attempt: None,
        }) {
            return Err(Box::new(ObservedUnmountFailure {
                milestone: UnmountMilestone::BeforeUnmount,
                token,
                discard,
                attempt: None,
                observer_error: Some(error),
            }));
        }
        let attempt = match self.unmount_checked(token, discard) {
            Ok(acknowledged) => UnmountAttempt::Acknowledged(acknowledged),
            Err(failure) => UnmountAttempt::Failed(failure),
        };
        if let Err(error) = observer(UnmountObservation {
            milestone: UnmountMilestone::Finished,
            token,
            discard,
            attempt: Some(&attempt),
        }) {
            return Err(Box::new(ObservedUnmountFailure {
                milestone: UnmountMilestone::Finished,
                token,
                discard,
                attempt: Some(attempt),
                observer_error: Some(error),
            }));
        }
        match attempt {
            UnmountAttempt::Acknowledged(acknowledged) => Ok(acknowledged),
            failure => Err(Box::new(ObservedUnmountFailure {
                milestone: UnmountMilestone::Finished,
                token,
                discard,
                attempt: Some(failure),
                observer_error: None,
            })),
        }
    }
}
