//! Atomic checkpoint publication and observation of its immutable receipt.
use super::{
    branch, commit, layerstack, params, provider::HistoryProvider, rows, transaction::Transaction,
};
use crate::backend::sqlite::schema::CheckpointFormat;
use layerfs_history::{
    checkpoint_request_digest, encode_checkpoint_request, CheckpointAttemptId, CheckpointReceipt,
    CommitOutcome, CommitRequest, HistoryError, HistoryResult,
};

impl HistoryProvider {
    fn checkpoint_receipt_format(&self) -> HistoryResult<()> {
        match self.checkpoint_format {
            CheckpointFormat::Receipts => Ok(()),
            CheckpointFormat::Legacy => Err(HistoryError::Unsupported("checkpoint receipt")),
        }
    }

    pub(crate) fn checkpoint_receipts_supported(&self) -> HistoryResult<()> {
        self.checkpoint_receipt_format()?;
        if !self.session.writable {
            return Err(HistoryError::ContinuityUnavailable);
        }
        self.read(|_| Ok(()))
    }

    pub(crate) fn checkpoint_commit(
        &self,
        attempt: CheckpointAttemptId,
        request: &CommitRequest,
    ) -> HistoryResult<CommitOutcome> {
        self.checkpoint_receipt_format()?;
        let digest = checkpoint_request_digest(&encode_checkpoint_request(request)?);
        self.write(|tx| {
            if read(tx, attempt)?.is_some() {
                return Err(HistoryError::Integrity(
                    "checkpoint attempt already recorded",
                ));
            }
            let outcome = commit::commit_branch(tx, request)?;
            let (kind, result, root, displaced, truncated) = match &outcome {
                CommitOutcome::Committed { record, displaced } => (
                    1,
                    Some(record.id.to_bytes().to_vec()),
                    record.root,
                    displaced.map(|id| id.to_bytes().to_vec()),
                    Some(i64::from(record.meta.lww_truncated)),
                ),
                CommitOutcome::UpToDate { head, root } => {
                    (2, head.map(|id| id.to_bytes().to_vec()), *root, None, None)
                }
            };
            // NULL for the optional integer uses the same typed backend parameter
            // path as other optional SQL values.
            let mut values = params![
                attempt.as_slice(),
                digest.as_slice(),
                request.branch.as_slice(),
                request.expected_base.as_slice(),
                kind,
                result,
                root.as_bytes().as_slice(),
                displaced,
            ];
            values.push(match truncated {
                Some(value) => crate::backend::records::Param::I64(value),
                None => crate::backend::records::Param::OptionalBytes(None),
            });
            if tx.execute("checkpoint_receipt_insert", values)? != 1 {
                return Err(HistoryError::Integrity("checkpoint receipt insert"));
            }
            Ok(outcome)
        })
    }

    pub(crate) fn read_checkpoint_receipt(
        &self,
        attempt: CheckpointAttemptId,
    ) -> HistoryResult<Option<CheckpointReceipt>> {
        self.checkpoint_receipt_format()?;
        self.read(|tx| read(tx, attempt))
    }
}

fn read(
    tx: &Transaction<'_>,
    attempt: CheckpointAttemptId,
) -> HistoryResult<Option<CheckpointReceipt>> {
    let found = tx.query("checkpoint_receipt_select", [attempt.as_slice()])?;
    let Some(row) = found.first() else {
        return Ok(None);
    };
    if found.len() != 1 {
        return Err(HistoryError::Integrity("checkpoint receipt count"));
    }
    let digest: [u8; 32] = rows::cell::<Vec<u8>>(row, 0)?
        .try_into()
        .map_err(|_| HistoryError::Integrity("checkpoint digest"))?;
    let branch_id = rows::required(rows::branch_column(row, 1)?, "checkpoint branch")?;
    let base_id = rows::required(rows::layer_column(row, 2)?, "checkpoint base")?;
    let branch =
        branch::branch(tx, branch_id)?.ok_or(HistoryError::Integrity("checkpoint branch"))?;
    let base = layerstack::layer(tx, base_id)?.ok_or(HistoryError::Integrity("checkpoint base"))?;
    if base.stack != branch.stack {
        return Err(HistoryError::Integrity("checkpoint base ownership"));
    }
    let result = rows::commit_column(row, 4)?;
    let root = rows::object(row, 5)?;
    let displaced = rows::commit_column(row, 6)?;
    let truncated: Option<i64> = rows::cell(row, 7)?;
    let outcome = match rows::cell::<i64>(row, 3)? {
        1 => {
            let id = rows::required(result, "checkpoint commit")?;
            let mut record =
                commit::commit(tx, id)?.ok_or(HistoryError::Integrity("checkpoint commit"))?;
            if record.stack != branch.stack
                || record.base_layer != base_id
                || record.root != root
                || record.meta.branch != Some(branch_id)
                || record.meta.displaced != displaced
            {
                return Err(HistoryError::Integrity("checkpoint commit ownership"));
            }
            record.meta.lww_truncated = match truncated {
                Some(0) => false,
                Some(1) => true,
                _ => return Err(HistoryError::Integrity("checkpoint truncated flag")),
            };
            CommitOutcome::Committed {
                record: Box::new(record),
                displaced,
            }
        }
        2 => {
            if displaced.is_some() || truncated.is_some() {
                return Err(HistoryError::Integrity("checkpoint unchanged outcome"));
            }
            if let Some(id) = result {
                let record =
                    commit::commit(tx, id)?.ok_or(HistoryError::Integrity("checkpoint head"))?;
                if record.stack != branch.stack
                    || record.base_layer != base_id
                    || record.root != root
                {
                    return Err(HistoryError::Integrity("checkpoint head ownership"));
                }
            } else if base.root != root {
                return Err(HistoryError::Integrity("checkpoint base root"));
            }
            CommitOutcome::UpToDate { head: result, root }
        }
        _ => return Err(HistoryError::Integrity("checkpoint outcome")),
    };
    Ok(Some(CheckpointReceipt {
        attempt,
        request_digest: digest,
        branch: branch_id,
        outcome,
    }))
}
