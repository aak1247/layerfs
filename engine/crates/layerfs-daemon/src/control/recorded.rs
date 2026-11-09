//! Admission of an already-bound owner through exact discovery evidence.
use super::{registry::DurableRegistrationFailure, Failure, Service};
use crate::{
    durable_roster::{Authority, Roster, RosterError},
    store::BoundWorkspace,
};
use layerfs_bridge::control::WorkspaceToken;
use std::fmt;

/// Why a recorded binding was refused before its control entry was installed.
#[derive(Debug)]
pub enum RecordedRegistrationError {
    Discovery(RosterError),
    Missing,
    BranchMismatch,
    StoreMismatch,
    Snapshot(layerfs_workspace::WorkspaceError),
    Control(Failure),
}

/// Every refusal returns the exact original bound Workspace to its caller.
pub struct RecordedRegistrationFailure {
    pub cause: RecordedRegistrationError,
    pub workspace: Box<BoundWorkspace>,
}
impl fmt::Debug for RecordedRegistrationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordedRegistrationFailure")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RecordedRegistrationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "recorded Workspace registration: {:?}",
            self.cause
        )
    }
}
impl std::error::Error for RecordedRegistrationFailure {}

impl Service {
    /// Registers an already-authorized binding only if its Workspace and
    /// original Branch appear in a complete discovery catalog. The caller must
    /// authenticate the catalog's Sandbox and Store and fence old consumers.
    pub fn register_recorded(
        &self,
        roster: &mut Roster,
        workspace: BoundWorkspace,
    ) -> Result<WorkspaceToken, Box<RecordedRegistrationFailure>> {
        let reject = |cause, workspace| {
            Box::new(RecordedRegistrationFailure {
                cause,
                workspace: Box::new(workspace),
            })
        };
        let expected = Authority::for_catalog(roster.authority().sandbox, self.store.history());
        if !expected.is_ok_and(|expected| expected.store == roster.authority().store) {
            return Err(reject(RecordedRegistrationError::StoreMismatch, workspace));
        }
        let record = match roster.lookup(workspace.identity()) {
            Ok(Some(record)) => record,
            Ok(None) => return Err(reject(RecordedRegistrationError::Missing, workspace)),
            Err(cause) => {
                return Err(reject(
                    RecordedRegistrationError::Discovery(cause),
                    workspace,
                ))
            }
        };
        let snapshot = match workspace.snapshot() {
            Ok(snapshot) => snapshot,
            Err(cause) => {
                return Err(reject(
                    RecordedRegistrationError::Snapshot(cause),
                    workspace,
                ))
            }
        };
        if record.branch != snapshot.branch.id {
            return Err(reject(RecordedRegistrationError::BranchMismatch, workspace));
        }
        self.register_durable(workspace).map_err(|error| {
            let DurableRegistrationFailure { cause, workspace } = *error;
            Box::new(RecordedRegistrationFailure {
                cause: RecordedRegistrationError::Control(cause),
                workspace,
            })
        })
    }
}
