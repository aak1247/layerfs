//! Actual ordinary Sandbox lifecycle and its Workspaces, with exact logical and kernel readiness.
#[cfg(unix)]
mod close;
#[cfg(unix)]
mod close_observed;
mod commit;
#[cfg(unix)]
pub(crate) mod create;
#[cfg(unix)]
pub(crate) mod exec;
#[cfg(target_os = "linux")]
mod host_workspace;
mod mount;
mod mount_observed;
#[cfg(target_os = "linux")]
mod reattach;
mod status;
mod unmount;
mod unmount_observed;
pub mod values;
#[cfg(unix)]
pub use close_observed::{
    ObservedSandboxCloseFailure, SandboxCloseAttempt, SandboxCloseMilestone,
    SandboxCloseObservation,
};
pub use commit::CommitReply;
#[cfg(unix)]
pub use create::{
    connect_address, SandboxCause, SandboxCreate, SandboxFailure, SandboxMilestone,
    SandboxObservation, SandboxPhase,
};
#[cfg(unix)]
pub use create::{ManagedSandbox, SandboxApi};
pub use mount::BoundWorkspace;
pub use mount::WorkspaceApi;
pub use mount::{MountFailure, MountedWorkspace};
pub use mount_observed::{MountCause, MountMilestone, MountObservation, ObservedMountFailure};
pub use unmount::Unmounted;
pub use unmount_observed::{
    ObservedUnmountFailure, UnmountAttempt, UnmountMilestone, UnmountObservation,
};
