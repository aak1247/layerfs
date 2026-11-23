//! Durable association between an accepted external capture and one checkpoint.

/// An external use of a Workspace capture. The value is part of the original
/// identity; it is not itself an authorization to read the captured root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapturePurpose {
    ManagedWorkspace,
}

impl CapturePurpose {
    pub(crate) const fn code(self) -> i64 {
        match self {
            Self::ManagedWorkspace => 1,
        }
    }
}

/// The caller's original immutable identity, authenticated by its Owner.
/// Neither this identity nor its scope digest is a Store or filesystem token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExternalCapture {
    pub id: [u8; 32],
    pub scope_digest: [u8; 32],
    pub purpose: CapturePurpose,
}

impl ExternalCapture {
    pub(crate) fn valid(self) -> bool {
        self.id != [0; 32] && self.scope_digest != [0; 32]
    }
}

/// The original publication input, frozen before its one Store attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalCapturePublication {
    pub request: Vec<u8>,
    pub request_digest: [u8; 32],
    pub candidate_root: [u8; 32],
    pub target_snapshot: Vec<u8>,
}

/// Local disposition only. A Store receipt is checked independently.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalCaptureDisposition {
    /// The original attempt may still have published or be in progress.
    Unresolved,
    /// The original attempt was proved not published and cannot be sent.
    Unpublished,
}

/// One local immutable association. A missing publication is not proof that
/// a Store request was absent or that its original writer has stopped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalCaptureRecord {
    pub identity: ExternalCapture,
    pub attempt: [u8; 32],
    pub workspace: [u8; 32],
    pub catalog: [u8; 32],
    pub incarnation: u64,
    pub namespace: u64,
    pub generation: u64,
    pub revision: u64,
    pub base_root: [u8; 32],
    pub original_snapshot: Vec<u8>,
    pub publication: Option<ExternalCapturePublication>,
    pub disposition: ExternalCaptureDisposition,
}
