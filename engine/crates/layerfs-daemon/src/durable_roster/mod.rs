//! Durable discovery evidence for one independently authorized Sandbox.
//!
//! This is an append-only identity catalog, not authorization, mount fencing,
//! an automatic recovery mechanism, or proof of external operation outcomes.
mod files;
mod ledger;
mod record;

pub use files::{Roster, RosterError};
pub use record::{Authority, Closing, WorkspaceRecord};
