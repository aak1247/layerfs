//! One nonpublishing capture and its read owners, admitted and released together.
use crate::{db::integer, Overlay, OverlayError, OverlayResult, SnapshotPin};

// Disjoint from ordinary source requests, diagnostics, visits and file reads.
const SNAPSHOT_SOURCE: i64 = 5;

impl Overlay {
    /// Seal the current published frontier and retain its base and captured
    /// reader in one owner transaction. `request` belongs to this original
    /// attempt and must not be reused after an unknown completion.
    pub fn pin_snapshot(&self, request: u64) -> OverlayResult<SnapshotPin> {
        if integer(request)? == 0 {
            return Err(OverlayError::Invalid("zero snapshot request"));
        }
        self.atomic_cleanup(|| {
            if self.open.borrow().snapshot.is_some() {
                return Err(OverlayError::CaptureInFlight);
            }
            let capture = self.capture_inner()?;
            let source = self.acquire_source_inner(request, SNAPSHOT_SOURCE)?;
            let reader = self.acquire_captured_reader_inner(capture, request)?;
            let pin = SnapshotPin {
                request,
                source,
                reader,
            };
            self.open.borrow_mut().snapshot = Some(pin);
            Ok(pin)
        })
    }

    /// Observe only this request's still-owned pin. Absence cannot establish
    /// that an unknown release succeeded or authorize another capture.
    pub fn retained_snapshot_pin(&self, request: u64) -> OverlayResult<Option<SnapshotPin>> {
        if integer(request)? == 0 {
            return Err(OverlayError::Invalid("zero snapshot request"));
        }
        self.state()?;
        let Some(pin) = self
            .open
            .borrow()
            .snapshot
            .filter(|pin| pin.request == request)
        else {
            return Ok(None);
        };
        self.checked_capture(pin.capture())?;
        self.source_state(pin.source)?;
        self.check_captured_reader(pin.reader)?;
        if self.retained_captured_reader(request)? != Some(pin.reader) {
            return Err(OverlayError::Stale);
        }
        Ok(Some(pin))
    }

    /// After all view and external consumers are proven finished, release
    /// this exact pin and schedule the original nonpublication fold. This
    /// never publishes a Branch or commits the Workspace.
    pub fn release_snapshot_pin(&self, pin: SnapshotPin) -> OverlayResult<()> {
        self.atomic_cleanup(|| {
            if self.retained_snapshot_pin(pin.request)? != Some(pin) {
                return Err(OverlayError::Stale);
            }
            self.release_captured_reader_inner(pin.reader)?;
            self.release_source_inner(pin.source)?;
            self.resolve_failed_capture_inner(pin.capture())?;
            self.open.borrow_mut().snapshot = None;
            Ok(())
        })
    }
}
