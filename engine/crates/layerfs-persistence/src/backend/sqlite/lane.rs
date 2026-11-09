//! The Store's publication lane: one connection turn and, on Linux, the
//! cooperative writer lock, held by one thread across a short read-decide-write
//! sequence. Its own SQL work runs as ordinary transactions inside the held
//! turn; no other publication of any process can interleave.
use super::{connection::Session, writer_admission::Writer};
use crate::backend::records::BackendError;
use std::{marker::PhantomData, sync::Arc};

/// A held lane; dropping it releases the writer lock, then the turn.
pub struct LaneHold {
    session: Arc<Session>,
    writer: Option<Writer>,
    // The hold belongs to the thread that took it.
    _thread: PhantomData<*const ()>,
}

impl Session {
    /// Waits its FIFO turn, then the cross-process writer lock, for the lane.
    /// A wait is readiness, not a retry: nothing is attempted before it is held.
    pub(crate) fn hold_lane(self: &Arc<Self>) -> Result<LaneHold, BackendError> {
        if !self.writable {
            return Err(BackendError::ReadOnly);
        }
        let turn = self
            .admission
            .enter(layerfs_telemetry::timer::waits::Wait::Writer)?;
        if turn.nested {
            return Err(BackendError::Integrity);
        }
        {
            let state = self.state.lock().map_err(|_| BackendError::Unknown)?;
            if state.quarantined {
                return Err(BackendError::Unknown);
            }
        }
        let writer = self.acquire_writer()?;
        turn.hold()?;
        Ok(LaneHold {
            session: self.clone(),
            writer: Some(writer),
            _thread: PhantomData,
        })
    }
}

impl Drop for LaneHold {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            // A nested transaction did not own `state.writer_hold`: the lane
            // still owns its lock. Transfer it to the retained session when a
            // durable outcome is unknown, including a poisoned unwind.
            let state = self.session.state.lock();
            let poisoned = state.is_err();
            let mut state = state.unwrap_or_else(|error| error.into_inner());
            if self.session.profile.persistence == crate::SqlitePersistenceProfile::Durable
                && (state.quarantined || poisoned || std::thread::panicking())
            {
                self.session.quarantine(&mut state);
                if state.writer_hold.is_none() {
                    state.writer_hold = Some(writer);
                } else {
                    std::mem::forget(writer);
                }
                drop(state);
                self.session.admission.release_hold();
                return;
            }
            drop(state);
            if let Err(retained) = writer.release() {
                // As a failed per-job release: keep the lock, refuse later work.
                if let Ok(mut state) = self.session.state.lock() {
                    self.session.quarantine(&mut state);
                    state.writer_hold = Some(retained);
                } else {
                    self.session.retain_uncertain();
                    std::mem::forget(retained);
                }
            }
        }
        self.session.admission.release_hold();
    }
}
