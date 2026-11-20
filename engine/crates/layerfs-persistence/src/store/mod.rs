//! Responsibility-scoped implementation modules and reexports.
#[cfg(target_os = "linux")]
pub(crate) mod checkpoint_upgrade;
pub(crate) mod config;
pub(crate) mod handles;
pub(crate) mod open;
pub(crate) mod seal;
