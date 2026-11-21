//! Authenticate orphan recovery against the same durable Store before cleanup.
use super::{
    bind::checked_base,
    catalog_recovery::LocalRecord,
    conflict_paths::{ConflictDecoder, ConflictDescription},
    BindError, BindRequest, Store,
};
use crate::OwnerError;
use layerfs_content::ObjectId;
use layerfs_history::{BranchSnapshot, HistoryError};
use layerfs_overlay::{
    MaintenanceCursor, OrphanRecoveryRecord, OverlayError, PendingCheckpointRecovery,
    PendingOrphanRecovery, SettledConflict,
};
use std::sync::Arc;

type PreparedOrphans = (
    LocalRecord,
    BranchSnapshot,
    Option<Arc<super::commit_merge::PendingProjection>>,
);

pub(super) struct PreparedConflictOrphans {
    pub record: OrphanRecoveryRecord,
    pub snapshot: BranchSnapshot,
    pub root_serial: u64,
    pub conflict: SettledConflict,
    pub projection: Arc<super::commit_merge::PendingProjection>,
}

fn invalid(message: &'static str) -> BindError {
    BindError::History(HistoryError::Integrity(message))
}
fn local(error: OverlayError) -> BindError {
    BindError::Owner(OwnerError::Overlay(error))
}

impl Store {
    pub(super) fn prepare_orphans(
        self: &Arc<Self>,
        pending: &PendingOrphanRecovery,
        request: &BindRequest,
    ) -> Result<PreparedOrphans, BindError> {
        if pending.conflict_record().is_some() {
            self.prepare_conflict_orphans(pending, request)
                .map(|prepared| {
                    (
                        LocalRecord::ConflictOrphans(
                            prepared.record,
                            prepared.root_serial,
                            prepared.conflict,
                        ),
                        prepared.snapshot,
                        Some(prepared.projection),
                    )
                })
        } else {
            self.prepare_orphan_recovery(pending, request)
                .map(|(record, snapshot, serial)| {
                    (LocalRecord::Orphans(record, serial), snapshot, None)
                })
        }
    }

    /// This only reads the leased original. The catalog caller fences the old
    /// writer and consumers and rechecks the original roster before activation.
    pub(super) fn prepare_orphan_recovery(
        self: &std::sync::Arc<Self>,
        pending: &PendingOrphanRecovery,
        request: &BindRequest,
    ) -> Result<(OrphanRecoveryRecord, BranchSnapshot, u64), BindError> {
        self.prepare_orphan_source(
            pending.observed().clone(),
            request,
            pending.conflict_record(),
            |after| pending.owner_roots(after),
            |after| pending.retirement_roots(after),
        )
    }

    pub(super) fn prepare_conflict_orphans(
        self: &Arc<Self>,
        pending: &PendingOrphanRecovery,
        request: &BindRequest,
    ) -> Result<PreparedConflictOrphans, BindError> {
        let conflict = pending
            .conflict_record()
            .cloned()
            .ok_or_else(|| invalid("mixed conflict recovery absent"))?;
        let (record, snapshot, root_serial) = self.prepare_orphan_recovery(pending, request)?;
        let mut decoder = ConflictDecoder::new(ConflictDescription {
            paths: conflict.paths_count,
            bytes: conflict.byte_count,
            digest: conflict.final_digest,
        })
        .map_err(|message| BindError::History(HistoryError::Integrity(message)))?;
        for number in 0..conflict.page_count {
            let page = pending.conflict_page(number).map_err(local)?;
            decoder
                .push(number, &page)
                .map_err(|message| BindError::History(HistoryError::Integrity(message)))?;
        }
        let conflicted = decoder
            .finish()
            .map_err(|message| BindError::History(HistoryError::Integrity(message)))?;
        let marker =
            ObjectId::from_bytes(&conflict.marker_root).map_err(|error| BindError::Content {
                error,
                provider: None,
            })?;
        Ok(PreparedConflictOrphans {
            record,
            snapshot,
            root_serial,
            conflict: conflict.clone(),
            projection: Arc::new(super::commit_merge::PendingProjection {
                merge: super::commit_merge::PendingMerge {
                    local_root: marker,
                    conflicted,
                },
                descriptor: Some(conflict.descriptor),
            }),
        })
    }

    pub(super) fn prepare_mixed_orphan_recovery(
        self: &std::sync::Arc<Self>,
        pending: &PendingCheckpointRecovery,
        request: &BindRequest,
    ) -> Result<(OrphanRecoveryRecord, BranchSnapshot, u64), BindError> {
        let record = pending
            .orphan_record()
            .cloned()
            .ok_or_else(|| invalid("mixed orphan recovery absent"))?;
        self.prepare_orphan_source(
            record,
            request,
            None,
            |after| pending.orphan_owner_roots(after),
            |after| pending.orphan_retirement_roots(after),
        )
    }

    fn prepare_orphan_source(
        self: &std::sync::Arc<Self>,
        record: OrphanRecoveryRecord,
        request: &BindRequest,
        conflict: Option<&SettledConflict>,
        owner_roots: impl Fn(i64) -> Result<Vec<(i64, [u8; 32])>, OverlayError>,
        retirement_roots: impl Fn((i64, i64)) -> Result<Vec<(i64, i64, [u8; 32])>, OverlayError>,
    ) -> Result<(OrphanRecoveryRecord, BranchSnapshot, u64), BindError> {
        if !self.has_durable_writer() || record.workspace != request.workspace.to_bytes() {
            return Err(invalid("orphan recovery identity"));
        }
        let snapshot = layerfs_bridge::snapshot::decode(&record.snapshot)
            .map_err(|error| BindError::Owner(OwnerError::BindingCodec(error)))?;
        if snapshot.branch.id != request.branch {
            return Err(invalid("orphan recovery binding"));
        }
        if let Some(conflict) = conflict {
            if conflict.workspace != record.workspace
                || conflict.previous_namespace != record.previous_namespace
                || conflict.catalog != self.history().catalog_id().to_bytes()
                || conflict.incarnation != self.history().incarnation()
                || conflict.marker_root != record.root
                || conflict.target_snapshot != record.snapshot
            {
                return Err(invalid("mixed conflict orphan binding"));
            }
            self.history()
                .require_checkpoint_receipts()
                .map_err(BindError::History)?;
        } else if snapshot.effective_root.to_bytes() != record.root {
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
        if conflict.is_some() {
            let mut marker = snapshot.clone();
            marker.effective_root =
                ObjectId::from_bytes(&record.root).map_err(|error| BindError::Content {
                    error,
                    provider: None,
                })?;
            let marker_base =
                checked_base(ports.client(), &marker).map_err(|error| BindError::Content {
                    error,
                    provider: ports
                        .failure()
                        .unwrap_or_else(|error| Some(Arc::new(error))),
                })?;
            if marker_base.root().root_inode() != original.root().root_inode() {
                return Err(invalid("mixed conflict marker root"));
            }
        }
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
            let rows = owner_roots(after).map_err(local)?;
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
            let rows = retirement_roots(cursor).map_err(local)?;
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
