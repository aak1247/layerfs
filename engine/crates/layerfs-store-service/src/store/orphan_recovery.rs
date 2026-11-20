//! Authenticate orphan recovery against the same durable Store before cleanup.
use super::{bind::checked_base, BindError, BindRequest, Store};
use crate::OwnerError;
use layerfs_content::ObjectId;
use layerfs_history::{BranchSnapshot, HistoryError};
use layerfs_overlay::{
    MaintenanceCursor, OrphanRecoveryRecord, OverlayError, PendingOrphanRecovery,
};

fn invalid(message: &'static str) -> BindError {
    BindError::History(HistoryError::Integrity(message))
}
fn local(error: OverlayError) -> BindError {
    BindError::Owner(OwnerError::Overlay(error))
}

impl Store {
    /// This only reads the leased original. The catalog caller fences the old
    /// writer and consumers and rechecks the original roster before activation.
    pub(super) fn prepare_orphan_recovery(
        self: &std::sync::Arc<Self>,
        pending: &PendingOrphanRecovery,
        request: &BindRequest,
    ) -> Result<(OrphanRecoveryRecord, BranchSnapshot, u64), BindError> {
        let record = pending.observed().clone();
        if !self.has_durable_writer() || record.workspace != request.workspace.to_bytes() {
            return Err(invalid("orphan recovery identity"));
        }
        let snapshot = layerfs_bridge::snapshot::decode(&record.snapshot)
            .map_err(|error| BindError::Owner(OwnerError::BindingCodec(error)))?;
        if snapshot.branch.id != request.branch || snapshot.effective_root.to_bytes() != record.root
        {
            return Err(invalid("orphan recovery binding"));
        }
        self.published(|history| super::durable_bind::validate_snapshot(history, &snapshot))
            .map_err(BindError::Read)?;
        let ports = self.ports(snapshot.scope);
        let original =
            checked_base(ports.client(), &snapshot).map_err(|error| BindError::Content {
                error,
                provider: ports
                    .failure()
                    .unwrap_or_else(|error| Some(std::sync::Arc::new(error))),
            })?;
        let root_serial = original.root().root_inode().serial();
        let check = |serial: i64, bytes: [u8; 32]| -> Result<(), BindError> {
            if u64::try_from(serial).ok() == Some(root_serial) {
                return Err(invalid("orphan recovery root inode"));
            }
            let mut historical = snapshot.clone();
            historical.effective_root =
                ObjectId::from_bytes(&bytes).map_err(|error| BindError::Content {
                    error,
                    provider: None,
                })?;
            let base =
                checked_base(ports.client(), &historical).map_err(|error| BindError::Content {
                    error,
                    provider: ports
                        .failure()
                        .unwrap_or_else(|error| Some(std::sync::Arc::new(error))),
                })?;
            if base.root().root_inode().serial() != root_serial {
                return Err(invalid("orphan recovery content root"));
            }
            Ok(())
        };
        let mut after = 0;
        loop {
            let rows = pending.owner_roots(after).map_err(local)?;
            if rows.is_empty() {
                break;
            }
            for (serial, root) in rows {
                check(serial, root)?;
                after = serial;
            }
        }
        let mut cursor = (0, 0);
        loop {
            let rows = pending.retirement_roots(cursor).map_err(local)?;
            if rows.is_empty() {
                break;
            }
            for (serial, generation, root) in rows {
                check(serial, root)?;
                cursor = (serial, generation);
            }
        }
        Ok((record, snapshot, root_serial))
    }

    pub(super) fn settle_orphan_recovery(
        &self,
        pending: &mut PendingOrphanRecovery,
        root_serial: u64,
    ) -> Result<(), BindError> {
        pending.schedule(root_serial).map_err(local)?;
        let mut cursor = MaintenanceCursor::default();
        while !pending.ready().map_err(local)? {
            cursor = pending
                .maintain(cursor)
                .map_err(local)?
                .ok_or_else(|| invalid("orphan recovery maintenance stalled"))?
                .cursor;
        }
        Ok(())
    }
}
