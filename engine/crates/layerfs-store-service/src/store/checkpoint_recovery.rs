//! Store-authenticated reconciliation of one original checkpoint attempt.
use super::{bind::checked_base, BindError, BindRequest, OriginalCheckpoint, Store};
use crate::OwnerError;
use layerfs_content::ObjectId;
use layerfs_history::{
    BranchSnapshot, CatalogId, CheckpointAttemptId, CheckpointReceipt, HistoryError,
};
use layerfs_overlay::{
    CheckpointOutcome, CheckpointRecord, MaintenanceCursor, OrphanRecoveryRecord,
    PendingCheckpointRecovery,
};
use std::sync::Arc;

/// Private to this Store: the bytes and local facts came from one leased
/// Overlay and were validated against this Store before any mutation.
pub(super) struct PreparedCheckpoint {
    record: CheckpointRecord,
    pub snapshot: BranchSnapshot,
    target: Option<BranchSnapshot>,
    candidate: Option<ObjectId>,
    request: Option<Vec<u8>>,
    orphan: Option<(OrphanRecoveryRecord, u64)>,
}

pub(super) struct SettledCheckpoint {
    pub snapshot: BranchSnapshot,
    pub installed: Option<(Vec<u8>, CheckpointOutcome)>,
}

fn invalid(message: &'static str) -> BindError {
    BindError::History(HistoryError::Integrity(message))
}

fn local(error: layerfs_overlay::OverlayError) -> BindError {
    BindError::Owner(OwnerError::Overlay(error))
}

impl PreparedCheckpoint {
    pub fn previous_namespace(&self) -> i64 {
        self.record.namespace
    }

    pub fn request(&self) -> Option<&[u8]> {
        self.request.as_deref()
    }

    fn original(&self) -> Result<OriginalCheckpoint<'_>, BindError> {
        Ok(OriginalCheckpoint {
            catalog: CatalogId::from_bytes(self.record.catalog),
            incarnation: self.record.incarnation,
            attempt: CheckpointAttemptId::from_bytes(self.record.attempt),
            request: self
                .request
                .as_deref()
                .ok_or_else(|| invalid("checkpoint request absent"))?,
            request_digest: self
                .record
                .request_digest
                .ok_or_else(|| invalid("checkpoint request absent"))?,
            candidate_root: self
                .candidate
                .ok_or_else(|| invalid("checkpoint candidate absent"))?,
            target: self
                .target
                .as_ref()
                .ok_or_else(|| invalid("checkpoint target absent"))?,
        })
    }

    fn matches_receipt(&self, receipt: &CheckpointReceipt) -> bool {
        receipt.attempt == CheckpointAttemptId::from_bytes(self.record.attempt)
            && Some(receipt.request_digest) == self.record.request_digest
            && self
                .target
                .as_ref()
                .is_some_and(|target| receipt.branch == target.branch.id)
    }
}

impl Store {
    pub(super) fn prepare_checkpoint_recovery(
        self: &Arc<Self>,
        pending: &mut PendingCheckpointRecovery,
        binding: &BindRequest,
    ) -> Result<PreparedCheckpoint, BindError> {
        let record = pending.observed().clone();
        let history = self.history();
        if !self.has_durable_writer()
            || record.workspace != binding.workspace.to_bytes()
            || record.catalog != history.catalog_id().to_bytes()
            || record.incarnation != history.incarnation()
        {
            return Err(invalid("checkpoint recovery Store identity"));
        }
        history
            .require_checkpoint_receipts()
            .map_err(BindError::History)?;
        let snapshot = layerfs_bridge::snapshot::decode(&record.snapshot)
            .map_err(|error| BindError::Owner(OwnerError::BindingCodec(error)))?;
        if snapshot.branch.id != binding.branch
            || snapshot.effective_root.to_bytes() != record.base_root
        {
            return Err(invalid("checkpoint recovery binding"));
        }
        self.published(|history| super::durable_bind::validate_snapshot(history, &snapshot))
            .map_err(BindError::Read)?;
        let ports = self.ports(snapshot.scope);
        let original =
            checked_base(ports.client(), &snapshot).map_err(|error| BindError::Content {
                error,
                provider: ports
                    .failure()
                    .unwrap_or_else(|error| Some(Arc::new(error))),
            })?;
        let (request, target, candidate) = match record.phase {
            1 | 3 => (None, None, None),
            2 | 4 | 6 => {
                let length = record
                    .request_bytes
                    .ok_or_else(|| invalid("checkpoint request absent"))?;
                let capacity = usize::try_from(length)
                    .map_err(|_| BindError::History(HistoryError::Capacity("checkpoint input")))?;
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(capacity)
                    .map_err(|_| BindError::History(HistoryError::Capacity("checkpoint input")))?;
                let mut offset = 0_u64;
                while offset < length {
                    let page = pending.request_page(offset).map_err(local)?;
                    offset = offset
                        .checked_add(page.len() as u64)
                        .ok_or_else(|| invalid("checkpoint request length"))?;
                    bytes.extend_from_slice(&page);
                }
                if offset != length {
                    return Err(invalid("checkpoint request length"));
                }
                pending.verify_frozen_request(&bytes).map_err(local)?;
                let target = layerfs_bridge::snapshot::decode(
                    record
                        .target_snapshot
                        .as_deref()
                        .ok_or_else(|| invalid("checkpoint target absent"))?,
                )
                .map_err(|error| BindError::Owner(OwnerError::BindingCodec(error)))?;
                if target.branch.id != binding.branch {
                    return Err(invalid("checkpoint recovery Branch"));
                }
                let candidate = ObjectId::from_bytes(
                    &record
                        .candidate_root
                        .ok_or_else(|| invalid("checkpoint candidate absent"))?,
                )
                .map_err(|error| BindError::Content {
                    error,
                    provider: None,
                })?;
                let original_input = OriginalCheckpoint {
                    catalog: history.catalog_id(),
                    incarnation: history.incarnation(),
                    attempt: CheckpointAttemptId::from_bytes(record.attempt),
                    request: &bytes,
                    request_digest: record
                        .request_digest
                        .ok_or_else(|| invalid("checkpoint request absent"))?,
                    candidate_root: candidate,
                    target: &target,
                };
                self.validate_original_checkpoint(&original_input)
                    .map_err(BindError::History)?;
                let mut candidate_snapshot = target.clone();
                candidate_snapshot.effective_root = candidate;
                let candidate_ports = self.ports(target.scope);
                let candidate_base = checked_base(candidate_ports.client(), &candidate_snapshot)
                    .map_err(|error| BindError::Content {
                        error,
                        provider: candidate_ports
                            .failure()
                            .unwrap_or_else(|error| Some(Arc::new(error))),
                    })?;
                if candidate_base.root().root_inode() != original.root().root_inode() {
                    return Err(invalid("checkpoint candidate root"));
                }
                (Some(bytes), Some(target), Some(candidate))
            }
            _ => return Err(invalid("checkpoint recovery phase")),
        };
        let orphan = if pending.orphan_record().is_some() {
            let (orphan, current, root_serial) =
                self.prepare_mixed_orphan_recovery(pending, binding)?;
            let expected_snapshot = if record.phase == 4 {
                record.installed_snapshot.as_deref()
            } else {
                Some(record.snapshot.as_slice())
            };
            let expected_root = if record.phase == 4 {
                record.candidate_root
            } else {
                Some(record.base_root)
            };
            if orphan.workspace != record.workspace
                || orphan.previous_namespace != record.namespace
                || Some(orphan.snapshot.as_slice()) != expected_snapshot
                || Some(orphan.root) != expected_root
                || current.branch.id != binding.branch
                || root_serial != original.root().root_inode().serial()
            {
                return Err(invalid("mixed checkpoint orphan binding"));
            }
            Some((orphan, root_serial))
        } else {
            None
        };
        Ok(PreparedCheckpoint {
            record,
            snapshot,
            target,
            candidate,
            request,
            orphan,
        })
    }

    /// Uses the actual Store lane and its immutable receipt. The caller has
    /// rechecked the original roster and fenced all old publishers/consumers.
    pub(super) fn settle_checkpoint_recovery(
        &self,
        pending: &mut PendingCheckpointRecovery,
        plan: &PreparedCheckpoint,
    ) -> Result<SettledCheckpoint, BindError> {
        if pending.observed() != &plan.record {
            return Err(invalid("checkpoint recovery predecessor"));
        }
        let history = self.history();
        let attempt = CheckpointAttemptId::from_bytes(plan.record.attempt);
        let lane = history.publish_lane().map_err(BindError::History)?;
        let receipt = history
            .checkpoint_receipt(attempt)
            .map_err(BindError::History)?;
        match (pending.phase(), receipt.as_ref()) {
            (1, None) => pending.fold_before_publication().map_err(local)?,
            (3, None) => {}
            (2, None) => pending.fold_after_absent_receipt().map_err(local)?,
            (6, None) => {}
            (2 | 4, Some(value)) if plan.matches_receipt(value) => {}
            _ => return Err(invalid("checkpoint recovery receipt")),
        }
        drop(lane);

        let mut snapshot = plan.snapshot.clone();
        let mut installed = None;
        if let Some(receipt) = receipt.as_ref() {
            let observed = self
                .observe_original_checkpoint(plan.original()?)
                .map_err(BindError::History)?
                .ok_or_else(|| invalid("checkpoint receipt disappeared"))?;
            if observed.outcome != receipt.outcome {
                return Err(invalid("checkpoint receipt changed"));
            }
            let bytes = layerfs_bridge::snapshot::encode(&observed.installed)
                .map_err(|error| BindError::Owner(OwnerError::BindingCodec(error)))?;
            let outcome = super::commit::checkpoint_outcome(&observed.outcome);
            if pending.phase() == 2 {
                pending
                    .install_original_receipt(&bytes, outcome)
                    .map_err(local)?;
            } else if plan.record.installed_snapshot.as_deref() != Some(bytes.as_slice())
                || plan.record.outcome != Some(outcome)
            {
                return Err(invalid("checkpoint installed outcome"));
            }
            snapshot = observed.installed;
            installed = Some((bytes, outcome));
        }
        let current_orphan = if let Some((original, root_serial)) = &plan.orphan {
            let bytes = layerfs_bridge::snapshot::encode(&snapshot)
                .map_err(|error| BindError::Owner(OwnerError::BindingCodec(error)))?;
            let current = OrphanRecoveryRecord {
                workspace: original.workspace,
                previous_namespace: original.previous_namespace,
                root: snapshot.effective_root.to_bytes(),
                snapshot: bytes,
            };
            pending.verify_mixed_orphans(&current).map_err(local)?;
            Some((current, *root_serial))
        } else {
            None
        };
        pending.release_original_owners().map_err(local)?;
        if let Some((current, root_serial)) = &current_orphan {
            pending
                .schedule_mixed_orphans(current, *root_serial)
                .map_err(local)?;
        }
        let mut cursor = MaintenanceCursor::default();
        while !pending
            .cleanup_ready(plan.request.as_deref())
            .map_err(local)?
            || (current_orphan.is_some() && !pending.mixed_orphans_drained().map_err(local)?)
        {
            cursor = pending
                .maintain(cursor)
                .map_err(local)?
                .ok_or_else(|| invalid("checkpoint recovery maintenance stalled"))?
                .cursor;
        }
        // Re-observe the exact original receipt after maintenance; absence is
        // meaningful only after the caller's old-writer fence and this lane.
        let lane = history.publish_lane().map_err(BindError::History)?;
        let final_receipt = history
            .checkpoint_receipt(attempt)
            .map_err(BindError::History)?;
        if final_receipt != receipt {
            return Err(invalid("checkpoint receipt changed"));
        }
        drop(lane);
        if let Some(expected) = receipt {
            let current = self
                .observe_original_checkpoint(plan.original()?)
                .map_err(BindError::History)?
                .ok_or_else(|| invalid("checkpoint receipt disappeared"))?;
            if current.outcome != expected.outcome || current.installed != snapshot {
                return Err(invalid("checkpoint receipt changed"));
            }
        }
        Ok(SettledCheckpoint {
            snapshot,
            installed,
        })
    }
}
