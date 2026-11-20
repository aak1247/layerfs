//! Read one checkpoint's original Store transaction without republishing it.
use super::{durable_bind::validate_snapshot, Store};
use layerfs_content::ObjectId;
use layerfs_history::{
    checkpoint_request_digest, decode_checkpoint_request, BranchSnapshot, CatalogId,
    CheckpointAttemptId, CommitId, CommitOutcome, CommitRequest, HistoryError, HistoryResult,
};

/// Immutable values supplied by the already authenticated Workspace record.
/// These values alone neither establish Workspace authority nor fence a writer.
pub struct OriginalCheckpoint<'a> {
    pub catalog: CatalogId,
    pub incarnation: u64,
    pub attempt: CheckpointAttemptId,
    pub request: &'a [u8],
    pub request_digest: [u8; 32],
    pub candidate_root: ObjectId,
    pub target: &'a BranchSnapshot,
}

/// Exact History outcome and the original target with that outcome applied.
/// The caller must still validate the content root and its Workspace binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointObservation {
    pub outcome: CommitOutcome,
    pub installed: BranchSnapshot,
}

/// Checks outcome relationships implied by the frozen request. This is not a
/// Store receipt, a Workspace authorization or a nonpublication proof.
pub fn check_checkpoint_outcome(
    request: &CommitRequest,
    outcome: &CommitOutcome,
) -> HistoryResult<()> {
    match outcome {
        CommitOutcome::Committed { displaced, .. } => {
            if (!request.force && request.candidate_root == request.expected_root)
                || displaced.is_some_and(|id| !request.force || Some(id) == request.expected_head)
            {
                return Err(HistoryError::Integrity("checkpoint impossible Commit"));
            }
        }
        CommitOutcome::UpToDate { head, root } => {
            if !request.force && (*head != request.expected_head || *root != request.expected_root)
            {
                return Err(HistoryError::Integrity("checkpoint impossible UpToDate"));
            }
        }
    }
    Ok(())
}

impl Store {
    /// Validates the frozen input against this Store without reading an outcome.
    pub(super) fn validate_original_checkpoint(
        &self,
        original: &OriginalCheckpoint<'_>,
    ) -> HistoryResult<CommitRequest> {
        let history = self.history();
        if !self.has_durable_writer()
            || history.catalog_id() != original.catalog
            || history.incarnation() != original.incarnation
        {
            return Err(HistoryError::Integrity("checkpoint Store identity"));
        }
        history.require_checkpoint_receipts()?;
        let request = decode_checkpoint_request(original.request)?;
        if checkpoint_request_digest(original.request) != original.request_digest
            || request.candidate_root != original.candidate_root
        {
            return Err(HistoryError::Integrity("checkpoint frozen input"));
        }
        let target = original.target;
        if request.branch != target.branch.id
            || request.expected_head != target.branch.head_commit
            || request.expected_base != target.branch.base_layer
            || request.expected_root != target.effective_root
            || request.profile != target.profile
            || request.scope != target.scope
        {
            return Err(HistoryError::Integrity("checkpoint target"));
        }
        validate_snapshot(history, target)?;
        let current = history
            .branch_snapshot(target.branch.id)?
            .ok_or(HistoryError::Integrity("checkpoint Branch"))?;
        if current.branch.id != target.branch.id
            || current.branch.name != target.branch.name
            || current.branch.forked_from != target.branch.forked_from
        {
            return Err(HistoryError::Integrity("checkpoint Branch identity"));
        }
        Ok(request)
    }

    /// Reads the exact recorded outcome from this Store; it never publishes.
    /// None is only an observation and is not proof that a writer has stopped
    /// or permission to replay the request. The caller authenticates the Store
    /// and Workspace custody independently before using the result.
    pub fn observe_original_checkpoint(
        &self,
        original: OriginalCheckpoint<'_>,
    ) -> HistoryResult<Option<CheckpointObservation>> {
        let request = self.validate_original_checkpoint(&original)?;
        let history = self.history();
        let target = original.target;
        let Some(receipt) = history.checkpoint_receipt(original.attempt)? else {
            return Ok(None);
        };
        if receipt.attempt != original.attempt
            || receipt.branch != request.branch
            || receipt.request_digest != original.request_digest
        {
            return Err(HistoryError::Integrity("checkpoint receipt identity"));
        }
        check_checkpoint_outcome(&request, &receipt.outcome)?;
        let (head, root) = match &receipt.outcome {
            CommitOutcome::Committed { record, displaced } => {
                let mut meta = request.meta.clone();
                meta.branch = Some(request.branch);
                let via = meta.via;
                meta.message = Some(
                    meta.message
                        .unwrap_or_else(|| format!("{via:?} filesystem change")),
                );
                meta.displaced = *displaced;
                meta.time_ms = record.meta.time_ms;
                if record.root != request.candidate_root
                    || record.stack != target.branch.stack
                    || record.parent != request.expected_head
                    || record.base_layer != request.expected_base
                    || record.second_parent != request.second_parent
                    || record.meta != meta
                    || record.id
                        != CommitId::derive(
                            record.root,
                            record.parent,
                            record.base_layer,
                            record.second_parent,
                            &record.meta,
                        )
                {
                    return Err(HistoryError::Integrity("checkpoint committed outcome"));
                }
                (Some(record.id), record.root)
            }
            CommitOutcome::UpToDate { head, root } => {
                if *root != request.candidate_root {
                    return Err(HistoryError::Integrity("checkpoint current outcome"));
                }
                match head {
                    Some(id) => {
                        let commit = history
                            .commit(*id)?
                            .ok_or(HistoryError::Integrity("checkpoint outcome Commit"))?;
                        if commit.stack != target.branch.stack
                            || commit.base_layer != request.expected_base
                            || commit.root != *root
                        {
                            return Err(HistoryError::Integrity("checkpoint outcome Commit"));
                        }
                    }
                    None if *root != target.base_root => {
                        return Err(HistoryError::Integrity("checkpoint outcome Layer"));
                    }
                    None => {}
                }
                (*head, *root)
            }
        };
        let mut installed = target.clone();
        installed.branch.head_commit = head;
        installed.head_root = head.map(|_| root);
        installed.effective_root = root;
        Ok(Some(CheckpointObservation {
            outcome: receipt.outcome,
            installed,
        }))
    }
}
