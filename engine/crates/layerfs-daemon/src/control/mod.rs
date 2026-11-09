//! Bounded kernel control over a direct Store and one local overlay owner.
#[cfg(target_os = "linux")]
mod attach;
mod cleanup;
#[cfg(target_os = "linux")]
mod detach;
mod dirty;
mod failure;
mod force;
mod inspect;
mod invalidate;
mod kernel;
mod observation;
mod operations;
#[cfg(target_os = "linux")]
mod recorded;
mod registry;
mod serve;
#[cfg(target_os = "linux")]
mod serving;
mod status;
mod types;
mod unmount;
pub use kernel::{KernelConfig, KernelServing};
pub(crate) use observation::{reply_token, request_token};
#[cfg(target_os = "linux")]
pub use recorded::{RecordedRegistrationError, RecordedRegistrationFailure};
pub use registry::{DurableRegistrationFailure, Service};
pub(crate) use serve::answer_call;
pub use serve::{ServeCause, ServeFailure, Served};
pub use types::{Failure, KernelEvidence, KernelFailure, Success};
