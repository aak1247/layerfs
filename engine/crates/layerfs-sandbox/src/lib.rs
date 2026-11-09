//! Ordinary runtime operations, separate from filesystem admission and data ownership.
#![forbid(unsafe_code)]
#[cfg(unix)]
pub mod backend;
#[cfg(target_os = "linux")]
pub mod export_lease;
#[cfg(target_os = "linux")]
pub mod host_export;
mod types;
pub use types::{
    CommandIdentity, ContainerId, ErrorDiagnostic, ExecId, ExecInspection, PendingRequest,
    RuntimeError, WireFailure,
};
