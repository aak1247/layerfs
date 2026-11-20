//! Opaque summary of an independently authenticated Store publication outcome.
use crate::{CapturedReader, OperationOwner, RecordedCapture};

/// The original Store result, after the caller has verified its full receipt.
/// These IDs alone do not establish Store identity or authorize recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointOutcome {
    /// The original publication created or selected this Commit.
    Committed {
        /// The recorded Commit identity.
        head: [u8; 33],
        /// The original displaced head, if any.
        displaced: Option<[u8; 33]>,
    },
    /// The original publication found the candidate already current.
    UpToDate {
        /// The head at that original observation, possibly absent.
        head: Option<[u8; 33]>,
    },
}

/// Original values of one installed checkpoint, independently checked by the
/// caller against its Store receipt. These values do not establish authority.
#[derive(Clone, Copy, Debug)]
pub struct CheckpointCompletion<'a> {
    pub recorded: RecordedCapture,
    pub request: &'a [u8],
    pub request_digest: [u8; 32],
    pub candidate_root: [u8; 32],
    pub target_snapshot: &'a [u8],
    pub installed_snapshot: &'a [u8],
    pub outcome: CheckpointOutcome,
    pub reader: Option<CapturedReader>,
    pub operation: Option<OperationOwner>,
}
