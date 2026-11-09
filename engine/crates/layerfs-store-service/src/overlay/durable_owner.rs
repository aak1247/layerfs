//! One durable Workspace behind the same service lock used by a mounted view.
//!
//! The caller owns durable discovery, authorization, mount fencing and unknown
//! publication reconciliation. This primitive does not provide those facts.
use super::{
    owner::{Context, Owner, OwnerClient, OwnerError},
    state::Shared,
};
use layerfs_overlay::{Overlay, OverlayError, ProfileConfig};
use layerfs_workspace::Route;
use std::{path::Path, sync::Arc};

/// Authenticated identity and expected root of one durable Workspace.
/// The namespace identifies its new daemon-local route, not a persisted token.
pub struct DurableWorkspace<'a> {
    pub path: &'a Path,
    pub profile: ProfileConfig,
    pub identity: [u8; 32],
    pub namespace: i64,
    pub expected_base_root: [u8; 32],
}

impl Owner {
    /// Creates a durable, initially bound Workspace. An existing file is never
    /// replaced. A failed attempt retains its files and is not auto-retried.
    /// The caller must authorize the identity and base root before calling.
    pub fn create_durable(spec: DurableWorkspace<'_>) -> Result<Self, OwnerError> {
        let incarnation = admission(&spec)?;
        let db = Overlay::create_durable(spec.path, spec.profile, spec.identity)
            .map_err(OwnerError::Overlay)?;
        db.open_workspace(spec.expected_base_root)
            .map_err(OwnerError::Overlay)?;
        bind(db, spec.namespace, spec.identity, incarnation)
    }

    /// Reopens an existing durable Workspace with a fresh route incarnation.
    /// Before exposing the result, the caller must fence every old mount and
    /// consumer, validate the root against the authorized Store, and reconcile
    /// unknown external publications. The file lease proves none of these.
    pub fn recover_durable(spec: DurableWorkspace<'_>) -> Result<Self, OwnerError> {
        let incarnation = admission(&spec)?;
        let db = Overlay::recover_durable(spec.path, spec.profile, spec.identity)
            .map_err(OwnerError::Overlay)?;
        if db.state().map_err(OwnerError::Overlay)?.base_root != spec.expected_base_root {
            return Err(OwnerError::Overlay(OverlayError::Invalid(
                "durable Workspace base root",
            )));
        }
        bind(db, spec.namespace, spec.identity, incarnation)
    }
}

fn admission(spec: &DurableWorkspace<'_>) -> Result<[u8; 32], OwnerError> {
    if spec.namespace <= 0 || spec.identity == [0; 32] {
        return Err(OwnerError::InvalidAdmission);
    }
    // A new process can reuse an engine number, so use a fresh unpredictable
    // incarnation instead of reusing the stable identity or a counter.
    let incarnation = layerfs_bridge::duplex::generate_keypair()
        .map_err(OwnerError::Randomness)?
        .public;
    if incarnation == [0; 32] {
        return Err(OwnerError::InvalidAdmission);
    }
    Ok(incarnation)
}

impl OwnerClient {
    /// Only owners created or recovered through the durable boundary have one.
    pub(crate) fn durable_identity(&self) -> Result<Option<[u8; 32]>, OwnerError> {
        Ok(self.connection()?.durable_identity)
    }
    /// A single logical binding owns this engine's base and Commit state.
    pub(crate) fn claim_durable_binding(&self) -> Result<(), OwnerError> {
        use std::sync::atomic::Ordering;
        let shared = self.connection()?;
        if shared.durable_identity.is_none() {
            return Err(OwnerError::InvalidAdmission);
        }
        shared
            .durable_binding_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| OwnerError::InvalidAdmission)
    }
}

fn bind(
    db: Overlay,
    namespace: i64,
    identity: [u8; 32],
    incarnation: [u8; 32],
) -> Result<Owner, OwnerError> {
    let route = Route::new(namespace, db.engine(), incarnation).map_err(OwnerError::Overlay)?;
    let profile = db.profile().clone();
    let shared = Arc::new(Shared::durable(namespace, identity));
    // This fresh Shared has no published readers or disposable Backing fd.
    shared.route.set(route).map_err(|_| OwnerError::Stopped)?;
    shared.state.lock().map_err(|_| OwnerError::Stopped)?.db = Some(Box::new(db));
    Ok(Owner {
        client: OwnerClient {
            context: Context::Workspace(shared),
        },
        profile,
    })
}
