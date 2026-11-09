//! A checked barrier over the concrete durable Store writer.
use super::connection::Session;
use crate::backend::records::BackendError;

impl Session {
    /// Check the original durable Store under its cooperative writer owner.
    /// Durable provisioning must already have synchronized its initial file;
    /// later WAL/FULL writes synchronize each transaction before their reply.
    pub(crate) fn durable_barrier(&self) -> Result<(), BackendError> {
        self.durable_barrier_cancellable(&|| false)
    }

    pub(crate) fn durable_barrier_cancellable(
        &self,
        stopped: &dyn Fn() -> bool,
    ) -> Result<(), BackendError> {
        #[cfg(target_os = "linux")]
        {
            if !self.writable
                || self.profile.persistence != crate::SqlitePersistenceProfile::Durable
                || self.durable_directory.is_none()
            {
                return Err(BackendError::Integrity);
            }
            let turn = self
                .admission
                .enter_cancellable(layerfs_telemetry::timer::waits::Wait::Writer, stopped)?;
            if turn.nested {
                return Err(BackendError::Integrity);
            }
            let mut state = self.state.lock().map_err(|_| BackendError::Unknown)?;
            let _panic = super::transaction::RetainOnPanic(self);
            if state.quarantined {
                return Err(BackendError::Unknown);
            }
            if !state.connection.is_autocommit() {
                self.quarantine(&mut state);
                return Err(BackendError::Unknown);
            }
            let file = self
                .durable_directory
                .as_ref()
                .expect("durable writer directory")
                .writer_file()
                .map_err(|_| BackendError::Unknown)?;
            state.writer_hold = Some(super::writer_admission::Writer::acquire_file_cancellable(
                file, stopped,
            )?);
            if stopped() {
                self.finish_writer(&mut state)?;
                return Err(BackendError::Cancelled);
            }
            if let Err(cause) = self.sync_durable_directory() {
                state.sync_failure = Some(cause);
                self.quarantine(&mut state);
                self.finish_writer(&mut state)?;
                return Err(BackendError::Unknown);
            }
            self.finish_writer(&mut state)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = stopped;
            Err(BackendError::Integrity)
        }
    }
}
