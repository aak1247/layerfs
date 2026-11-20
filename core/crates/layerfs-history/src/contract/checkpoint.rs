//! Stable identities and complete request bytes for a single checkpoint publication.
use crate::{facts, BranchId, CommitOutcome, CommitRequest, HistoryError, HistoryResult};

/// A checkpoint attempt identity. It is not an authorization or a replay token.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CheckpointAttemptId([u8; 32]);
impl CheckpointAttemptId {
    /// Wraps the exact identity recorded by the authenticated Workspace owner.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    /// The complete identity bytes.
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
    /// The complete identity bytes as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

/// A Store's original atomic answer for exactly one checkpoint request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointReceipt {
    /// The original attempt, never authority to send the request again.
    pub attempt: CheckpointAttemptId,
    /// Digest of the complete canonical request bytes.
    pub request_digest: [u8; 32],
    /// Branch on which the attempt was made.
    pub branch: BranchId,
    /// The original transaction's result, independent of today's Branch head.
    pub outcome: CommitOutcome,
}

fn bytes(out: &mut Vec<u8>, value: &[u8]) -> HistoryResult<()> {
    let len = u64::try_from(value.len()).map_err(|_| HistoryError::Capacity("checkpoint input"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn optional(out: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(value);
        }
        None => out.push(0),
    }
}

/// Encodes every field of the original request in a versioned, unambiguous order.
/// This includes facts omitted from Commit identity and transport projections.
pub fn encode_checkpoint_request(request: &CommitRequest) -> HistoryResult<Vec<u8>> {
    request.meta.check()?;
    let mut out = b"LFCP\x01".to_vec();
    out.extend_from_slice(request.branch.as_slice());
    optional(
        &mut out,
        request.expected_head.as_ref().map(|id| id.as_slice()),
    );
    out.extend_from_slice(request.expected_base.as_slice());
    for id in [
        request.expected_root,
        request.candidate_root,
        request.profile,
        request.scope,
    ] {
        out.extend_from_slice(id.as_bytes());
    }
    optional(
        &mut out,
        request.second_parent.as_ref().map(|id| id.as_slice()),
    );
    optional(
        &mut out,
        request.meta.branch.as_ref().map(|id| id.as_slice()),
    );
    out.push(request.meta.via.code());
    match &request.meta.message {
        Some(message) => {
            out.push(1);
            bytes(&mut out, message.as_bytes())?;
        }
        None => out.push(0),
    }
    optional(
        &mut out,
        request.meta.merged_from.as_ref().map(|id| id.as_slice()),
    );
    out.push(u8::from(request.meta.lww_truncated));
    bytes(
        &mut out,
        &facts::encode(&request.meta.lww, &request.meta.units)?,
    )?;
    for number in [
        request.meta.statistics.logical,
        request.meta.statistics.new_canonical,
        request.meta.statistics.new_stored,
        request.meta.statistics.new_metadata,
    ] {
        out.extend_from_slice(&number.to_be_bytes());
    }
    optional(
        &mut out,
        request.meta.displaced.as_ref().map(|id| id.as_slice()),
    );
    out.extend_from_slice(&request.meta.time_ms.to_be_bytes());
    out.push(u8::from(request.force));
    Ok(out)
}

/// Domain-separated digest of complete, already encoded checkpoint request bytes.
pub fn checkpoint_request_digest(encoded: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs/checkpoint-request/v1\0");
    hash.update(encoded);
    *hash.finalize().as_bytes()
}
