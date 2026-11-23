//! Capture once and observe its original Store receipt under the current token.
use super::{registry::bound, Failure, Service, Success};
use layerfs_bridge::control::{
    Activity, CaptureControl, CaptureObservation, CaptureOutcome, CaptureProof, CapturePurpose,
    ControlCode, Reply,
};
use layerfs_history::CommitOutcome;
use layerfs_overlay::{CapturePurpose as OverlayPurpose, ExternalCapture};
use std::sync::Arc;

fn identity(control: CaptureControl) -> Result<ExternalCapture, Failure> {
    if control.identity.id == [0; 32] || control.identity.scope_digest == [0; 32] {
        return Err(Failure::Rejected(ControlCode::Invalid, "capture identity"));
    }
    Ok(ExternalCapture {
        id: control.identity.id,
        scope_digest: control.identity.scope_digest,
        purpose: match control.identity.purpose {
            CapturePurpose::ManagedWorkspace => OverlayPurpose::ManagedWorkspace,
        },
    })
}

fn matches_outcome(proof: &CaptureProof, outcome: &CommitOutcome) -> bool {
    match (&proof.outcome, outcome) {
        (
            CaptureOutcome::Committed {
                head,
                time_ms,
                displaced,
            },
            CommitOutcome::Committed {
                record,
                displaced: actual,
            },
        ) => {
            *head == record.id
                && *time_ms == record.meta.time_ms
                && *displaced == *actual
                && proof.root == record.root
        }
        (CaptureOutcome::UpToDate { head }, CommitOutcome::UpToDate { head: actual, root }) => {
            *head == *actual && proof.root == *root
        }
        _ => false,
    }
}

impl Service {
    pub(super) fn capture_external(&self, control: CaptureControl) -> Result<Success, Failure> {
        let capture = identity(control)?;
        let mut original = self.commit(control.token, |workspace| {
            workspace.capture_external(capture)
        })?;
        let observed = self.observe_capture(control);
        match observed {
            Ok(Success {
                reply: Reply::Capture(CaptureObservation::Published(proof)),
                ..
            }) if proof.identity == control.identity
                && proof.workspace == control.token.workspace
                && proof.original_namespace == control.token.namespace
                && original
                    .commit
                    .as_ref()
                    .and_then(|c| c.history.as_ref())
                    .is_some_and(|outcome| matches_outcome(&proof, outcome)) =>
            {
                original.reply = Reply::Capture(CaptureObservation::Published(proof));
                Ok(original)
            }
            Ok(_) => Err(Failure::After {
                cause: Box::new(Failure::Rejected(
                    ControlCode::Unknown,
                    "original capture receipt unavailable or changed",
                )),
                original: Box::new(original),
            }),
            Err(cause) => Err(Failure::After {
                cause: Box::new(cause),
                original: Box::new(original),
            }),
        }
    }

    pub(super) fn observe_capture(&self, control: CaptureControl) -> Result<Success, Failure> {
        let capture = identity(control)?;
        // Observation is allowed while the original Commit retains uncertain
        // custody. It neither starts another operation nor releases custody.
        let workspace = {
            let entries = self.entries.lock().map_err(|_| Failure::Poisoned)?;
            let value = bound(&entries, control.token)?;
            if value.activity == Activity::Closing {
                return Err(Failure::Rejected(ControlCode::Busy, "Workspace is closing"));
            }
            value.workspace.clone()
        };
        let result = workspace
            .observe_external_capture(capture)
            .map_err(Failure::CaptureObservation)?;
        let entries = self.entries.lock().map_err(|_| Failure::Poisoned)?;
        let current = bound(&entries, control.token)?;
        if current.activity == Activity::Closing || !Arc::ptr_eq(&workspace, &current.workspace) {
            return Err(Failure::Rejected(
                ControlCode::Busy,
                "Workspace changed during capture observation",
            ));
        }
        Ok(Success::reply(Reply::Capture(result)))
    }
}
