//! Typed local-conflict staging, without publication or recovery authority.
use crate::{CapturedReader, OperationOwner, OverlayError, OverlayResult, RecordedCapture};

/// Fixed maximum bytes in one persisted conflict path page.
pub const CONFLICT_PAGE_BYTES: usize = 16_384;

/// Opaque conflict material selected by the caller for one captured view.
/// The caller must authenticate the Store and interpret the target snapshot.
pub struct ConflictStageInput<'a> {
    pub local_root: [u8; 32],
    pub target_snapshot: &'a [u8],
    pub paths_count: u64,
    pub byte_count: u64,
    pub final_digest: [u8; 32],
}

/// One engine-minted plan for the original captured Workspace. Its descriptor
/// alone does not authenticate a Store or authorize installing the result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConflictStage {
    pub(crate) recorded: RecordedCapture,
    pub(crate) descriptor: [u8; 32],
}
impl ConflictStage {
    pub const fn recorded(self) -> RecordedCapture {
        self.recorded
    }
    pub const fn descriptor(self) -> [u8; 32] {
        self.descriptor
    }
}

/// The exact locally installed conflict and original construction owners.
/// The caller must separately authenticate the Store and its historical target.
#[derive(Clone, Copy)]
pub struct ConflictCompletion<'a> {
    pub stage: ConflictStage,
    pub marker_root: [u8; 32],
    pub target_snapshot: &'a [u8],
    pub reader: Option<CapturedReader>,
    pub operation: Option<OperationOwner>,
}

/// One page-chain step. The initial predecessor is 32 zero bytes. Callers
/// encode their original ordered paths and compute the final digest before
/// staging; this hash is not a Store authorization or publication receipt.
pub fn conflict_page_digest(previous: [u8; 32], page: u64, data: &[u8]) -> OverlayResult<[u8; 32]> {
    if data.is_empty() || data.len() > CONFLICT_PAGE_BYTES {
        return Err(OverlayError::Invalid("conflict page size"));
    }
    let length =
        u64::try_from(data.len()).map_err(|_| OverlayError::Invalid("conflict page size"))?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs/conflict-page/v1\0");
    hash.update(&previous);
    hash.update(&page.to_be_bytes());
    hash.update(&length.to_be_bytes());
    hash.update(data);
    Ok(*hash.finalize().as_bytes())
}
