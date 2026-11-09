//! Serialized close of a catalog-backed binding, retaining uncertain outcomes.
use super::{
    kernel::Kernel,
    registry::{bound, bound_mut, idle},
    unmount::Departure,
    Failure, Service, Success,
};
use crate::durable_roster::{Authority, Roster, RosterError};
use layerfs_bridge::control::{
    Activity, ControlCode, MountPhase, Reply, UnmountControl, WorkspaceToken,
};
use layerfs_history::BranchId;
use std::fmt;

/// Original close failure and the point at which its effects became possible.
#[derive(Debug)]
pub enum RecordedCloseFailure {
    Invalid(&'static str),
    Discovery(RosterError),
    Before(Failure),
    /// The intent did not return success. An uncertain write keeps admission reserved.
    Intent {
        cause: RosterError,
        reservation_retained: bool,
    },
    /// The durable intent exists; the attempted unmount must not be replayed.
    Control {
        token: WorkspaceToken,
        cause: Failure,
    },
    /// Unmount succeeded, but recording its terminal receipt did not.
    Completion {
        cause: RosterError,
        original: Box<Success>,
    },
}
impl fmt::Display for RecordedCloseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "recorded Workspace close: {self:?}")
    }
}
impl std::error::Error for RecordedCloseFailure {}

impl Service {
    /// Close a catalog-backed binding once. The caller authenticates the
    /// Sandbox and physical Store, serializes access to the catalog, and keeps
    /// this Service and any original failure when the result is uncertain.
    /// A Dirty reply is a pre-effect refusal and does not record an intent.
    pub fn unmount_recorded(
        &self,
        roster: &mut Roster,
        control: UnmountControl,
    ) -> Result<Success, RecordedCloseFailure> {
        let authority = Authority::for_catalog(roster.authority().sandbox, self.store.history())
            .map_err(RecordedCloseFailure::Discovery)?;
        if authority != roster.authority() {
            return Err(RecordedCloseFailure::Invalid(
                "recorded close Store authority",
            ));
        }
        let record = roster
            .active_record(control.token.workspace)
            .map_err(RecordedCloseFailure::Discovery)?
            .ok_or(RecordedCloseFailure::Invalid(
                "recorded close Workspace missing",
            ))?;
        self.check_recorded_binding(control.token, record.branch)
            .map_err(RecordedCloseFailure::Before)?;
        if !control.discard {
            if let Some(report) = self
                .dirty(control.token)
                .map_err(RecordedCloseFailure::Before)?
            {
                return Ok(Success::reply(Reply::Dirty(Box::new(report))));
            }
        }
        self.reserve_recorded_unmount(control.token, record.branch)
            .map_err(RecordedCloseFailure::Before)?;
        if let Err(cause) = roster.begin_close(control.token) {
            let uncertain = matches!(&cause, RosterError::Uncertain(_) | RosterError::Retained(_));
            let released = !uncertain && self.release_recorded_unmount(control.token);
            return Err(RecordedCloseFailure::Intent {
                cause,
                reservation_retained: !released,
            });
        }
        let (workspace, departure) = match self.take_recorded_unmount(control.token) {
            Ok(value) => value,
            Err(cause) => {
                return Err(RecordedCloseFailure::Control {
                    token: control.token,
                    cause,
                })
            }
        };
        let outcome = match departure {
            Departure::Unattached => self.close(control.token, &workspace, None),
            Departure::Session(attached) => {
                self.unmount_kernel(control.token, workspace, *attached, true)
            }
        };
        let success = match outcome {
            Ok(success) => success,
            Err(cause) => {
                self.hold_recorded_unmount(control.token);
                return Err(RecordedCloseFailure::Control {
                    token: control.token,
                    cause,
                });
            }
        };
        if let Err(cause) = roster.finish_close(&success.reply) {
            return Err(RecordedCloseFailure::Completion {
                cause,
                original: Box::new(success),
            });
        }
        Ok(success)
    }

    fn check_recorded_binding(
        &self,
        token: WorkspaceToken,
        branch: BranchId,
    ) -> Result<(), Failure> {
        let entries = self.entries.lock().map_err(|_| Failure::Poisoned)?;
        let value = bound(&entries, token)?;
        if !value.recorded || value.snapshot.branch.id != branch {
            return Err(Failure::Rejected(
                ControlCode::Invalid,
                "recorded close binding",
            ));
        }
        Ok(())
    }

    fn reserve_recorded_unmount(
        &self,
        token: WorkspaceToken,
        branch: BranchId,
    ) -> Result<(), Failure> {
        let mut entries = self.entries.lock().map_err(|_| Failure::Poisoned)?;
        let value = bound_mut(&mut entries, token)?;
        if !value.recorded || value.snapshot.branch.id != branch {
            return Err(Failure::Rejected(
                ControlCode::Invalid,
                "recorded close binding",
            ));
        }
        if let Kernel::Retained(kept) = &value.kernel {
            return Err(Failure::Retained(Box::new(kept.custody(token))));
        }
        idle(value)?;
        match value.kernel {
            Kernel::Unattached | Kernel::Ready(_) => {}
            Kernel::Attaching => {
                return Err(Failure::kernel(
                    ControlCode::Unknown,
                    "unmount:admission",
                    "original Attach outcome retained",
                ))
            }
            Kernel::Leaving(_) => {
                return Err(Failure::kernel(
                    ControlCode::Busy,
                    "unmount:admission",
                    "terminal unmount already admitted",
                ))
            }
            Kernel::Retained(_) => unreachable!("retained custody checked"),
        }
        value.activity = Activity::Closing;
        value.epoch = value.epoch.saturating_add(1);
        Ok(())
    }

    fn release_recorded_unmount(&self, token: WorkspaceToken) -> bool {
        let Ok(mut entries) = self.entries.lock() else {
            return false;
        };
        let Ok(value) = bound_mut(&mut entries, token) else {
            return false;
        };
        if !value.recorded || value.activity != Activity::Closing {
            return false;
        }
        value.activity = Activity::Idle;
        value.epoch = value.epoch.saturating_add(1);
        true
    }

    fn hold_recorded_unmount(&self, token: WorkspaceToken) {
        if let Ok(mut entries) = self.entries.lock() {
            if let Ok(value) = bound_mut(&mut entries, token) {
                if value.recorded && value.activity == Activity::Idle {
                    value.activity = Activity::Closing;
                    value.epoch = value.epoch.saturating_add(1);
                }
            }
        }
    }

    fn take_recorded_unmount(
        &self,
        token: WorkspaceToken,
    ) -> Result<(std::sync::Arc<crate::store::BoundWorkspace>, Departure), Failure> {
        let mut entries = self.entries.lock().map_err(|_| Failure::Poisoned)?;
        let value = bound_mut(&mut entries, token)?;
        if !value.recorded || value.activity != Activity::Closing {
            return Err(Failure::Rejected(
                ControlCode::Unknown,
                "recorded close reservation lost",
            ));
        }
        let departure = match value.kernel {
            Kernel::Unattached => Departure::Unattached,
            Kernel::Ready(_) => {
                Departure::Session(super::detach::take(&mut value.kernel, MountPhase::Probing)?)
            }
            _ => {
                return Err(Failure::Rejected(
                    ControlCode::Unknown,
                    "recorded close kernel changed",
                ))
            }
        };
        Ok((value.workspace.clone(), departure))
    }
}
