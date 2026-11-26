//! A read-only kernel view over one retained nonpublishing capture.
mod filesystem;
mod session;
mod source;

pub use filesystem::SnapshotFilesystem;
pub use session::{
    SnapshotAttachFailure, SnapshotConfig, SnapshotDetach, SnapshotDrainFailure, SnapshotDrained,
    SnapshotSession,
};
pub use source::SnapshotSource;
