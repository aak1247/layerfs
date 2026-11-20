//! Prepared immutable binding and one known install attempt, preserving input.
use crate::{BaseView, Workspace, WorkspaceError, WorkspaceResult};
use layerfs_content::filesystem::FilesystemRootId;
use layerfs_overlay::{Capture, CheckpointOutcome, Overlay, OverlayError, RecordedCapture};

/// Scope-checked next immutable root retained before the SQL install attempt.
/// This is not proof of Save/history publication: that remains the caller's
/// exact prerequisite. Refusal/uncertainty returns this input without replay.
pub struct PreparedBase {
    capture: Capture,
    expected: FilesystemRootId,
    next: BaseView,
}

/// Inputs already checked against the original authenticated Store outcome.
/// These values do not themselves establish publication or recovery authority.
#[derive(Clone, Copy)]
pub struct CheckpointInstall<'a> {
    pub recorded: RecordedCapture,
    pub request_digest: [u8; 32],
    pub target_snapshot: &'a [u8],
    pub installed_snapshot: &'a [u8],
    pub outcome: CheckpointOutcome,
}

enum Install<'a> {
    Plain,
    Snapshot(&'a [u8]),
    Checkpoint(CheckpointInstall<'a>),
}
impl PreparedBase {
    pub const fn capture(&self) -> Capture {
        self.capture
    }
    pub fn root(&self) -> FilesystemRootId {
        self.next.identity()
    }
}
impl Workspace {
    /// Demand/validate one new canonical root before acquiring install state.
    /// No whole-tree validation/import/materialization is hidden in this step.
    pub fn prepare_base_install(
        &self,
        capture: Capture,
        root: FilesystemRootId,
    ) -> WorkspaceResult<PreparedBase> {
        if capture.engine() != self.route().engine() {
            return Err(OverlayError::Stale.into());
        }
        let base = self.base()?;
        let expected = base.identity();
        if capture.base_root != expected.0.to_bytes() {
            return Err(WorkspaceError::BaseChanged {
                expected: capture.base_root,
                actual: expected.0.to_bytes(),
            });
        }
        let next = base.rebind(root)?;
        Ok(PreparedBase {
            capture,
            expected,
            next,
        })
    }
    /// One local install after exact known upstream publication/UpToDate.
    /// Only the bounded SQL transition holds the binding lock; canonical IO was
    /// prepared outside it. On definite/uncertain failure both original error
    /// and prepared root/capture custody return. Never infer or resend history.
    /// Kernel actor composition must preserve this paired binding transition.
    pub fn install_prepared_base(
        &self,
        overlay: &Overlay,
        prepared: PreparedBase,
    ) -> Result<(), (WorkspaceError, PreparedBase)> {
        self.install_with_binding(overlay, prepared, Install::Plain)
    }
    /// Pairs a known durable history receipt with the local base transition.
    pub fn install_prepared_base_recorded(
        &self,
        overlay: &Overlay,
        prepared: PreparedBase,
        snapshot: &[u8],
    ) -> Result<(), (WorkspaceError, PreparedBase)> {
        self.install_with_binding(overlay, prepared, Install::Snapshot(snapshot))
    }
    /// Pairs an authenticated checkpoint outcome and its exact local capture
    /// with the same base-lock transition used by ordinary installs.
    pub fn install_prepared_checkpoint(
        &self,
        overlay: &Overlay,
        prepared: PreparedBase,
        input: CheckpointInstall<'_>,
    ) -> Result<(), (WorkspaceError, PreparedBase)> {
        self.install_with_binding(overlay, prepared, Install::Checkpoint(input))
    }
    fn install_with_binding(
        &self,
        overlay: &Overlay,
        prepared: PreparedBase,
        install: Install<'_>,
    ) -> Result<(), (WorkspaceError, PreparedBase)> {
        if prepared.capture.engine() != self.route().engine()
            || matches!(&install, Install::Checkpoint(input) if input.recorded.capture() != prepared.capture)
        {
            return Err((OverlayError::Stale.into(), prepared));
        }
        let mut base = match self.base.write() {
            Ok(base) => base,
            Err(_) => return Err((WorkspaceError::BindingPoisoned, prepared)),
        };
        if base.identity() != prepared.expected {
            return Err((
                WorkspaceError::BaseChanged {
                    expected: prepared.expected.0.to_bytes(),
                    actual: base.identity().0.to_bytes(),
                },
                prepared,
            ));
        }
        let root = prepared.next.identity().0.to_bytes();
        let installed = match install {
            Install::Plain => overlay.install(prepared.capture, root),
            Install::Snapshot(snapshot) => {
                overlay.install_recorded(prepared.capture, root, snapshot)
            }
            Install::Checkpoint(input) => overlay.install_recorded_checkpoint(
                input.recorded,
                input.request_digest,
                root,
                input.target_snapshot,
                input.installed_snapshot,
                input.outcome,
            ),
        };
        if let Err(error) = installed {
            return Err((error.into(), prepared));
        }
        // No fallible work follows known SQL success; old immutable plans own
        // their separate client/root and do not observe this pointer change.
        // A scoped preparer may carry an operation-local provider and its
        // retained failure state. Install shares the checked metadata/root,
        // while the original binding retains its original provider owner.
        let original_client = base.client();
        *base = prepared.next.with_client(original_client);
        Ok(())
    }
}

impl std::fmt::Debug for PreparedBase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedBase")
            .field("capture", &self.capture)
            .field("expected", &self.expected)
            .field("next", &self.next.identity())
            .finish()
    }
}
