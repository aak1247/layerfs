//! Bounded kernel control over a direct Store and one local overlay owner.
#[cfg(target_os = "linux")]
mod attach;
mod cleanup;
#[cfg(target_os = "linux")]
mod detach;
mod dirty;
#[cfg(target_os = "linux")]
mod durable_control;
#[cfg(target_os = "linux")]
mod durable_root;
mod failure;
mod force;
#[cfg(target_os = "linux")]
mod fresh_control;
#[cfg(target_os = "linux")]
mod fresh_volume;
#[cfg(target_os = "linux")]
mod host_export;
mod inspect;
mod invalidate;
mod kernel;
mod observation;
mod operations;
#[cfg(target_os = "linux")]
mod recorded;
#[cfg(target_os = "linux")]
mod recorded_close;
#[cfg(target_os = "linux")]
mod recorded_mount;
#[cfg(target_os = "linux")]
mod recover_control;
#[cfg(target_os = "linux")]
mod recovery;
mod registry;
mod serve;
#[cfg(target_os = "linux")]
mod serving;
mod status;
mod types;
mod unmount;
#[cfg(target_os = "linux")]
pub use durable_control::{DurableControlCause, DurableControlConfig, DurableControlFailure};
#[cfg(target_os = "linux")]
pub use fresh_control::{FreshControlCause, FreshControlConfig, FreshControlFailure};
#[cfg(target_os = "linux")]
pub use fresh_volume::{FreshVolumeCause, FreshVolumeConfig, FreshVolumeFailure};
#[cfg(target_os = "linux")]
pub use host_export::ContainerExportRoot;
pub use kernel::{KernelConfig, KernelServing};
pub(crate) use observation::{reply_token, request_token};
#[cfg(target_os = "linux")]
pub use recorded::{RecordedRegistrationError, RecordedRegistrationFailure};
#[cfg(target_os = "linux")]
pub use recorded_close::RecordedCloseFailure;
#[cfg(target_os = "linux")]
pub use recorded_mount::{RecordedMountFailure, RecordedMountRequest, RecordedMountReservation};
#[cfg(target_os = "linux")]
pub use recover_control::{RecoverControlCause, RecoverControlConfig, RecoverControlFailure};
#[cfg(target_os = "linux")]
pub use recovery::{CatalogRecoveryCause, CatalogRecoveryFailure};
pub use registry::{DurableRegistrationFailure, Service};
pub(crate) use serve::answer_call;
pub use serve::{ServeCause, ServeFailure, Served};
pub use types::{Failure, KernelEvidence, KernelFailure, Success};
