//! Daemon service observations and completion ownership.
mod filesystem_port;
pub use filesystem_port::{request, Mounted};
#[cfg(target_os = "linux")]
mod snapshot_port;
#[cfg(target_os = "linux")]
pub use snapshot_port::SnapshotPort;
