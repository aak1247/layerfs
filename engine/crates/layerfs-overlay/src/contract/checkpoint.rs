//! Opaque summary of an independently authenticated Store publication outcome.

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
