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

/// One before-effect capacity slot and its never-reused local namespace.
pub struct DurableReservation {
    pub(super) roster: Arc<super::workspaces::Workspaces>,
    pub(super) namespace: i64,
    pub(super) identity: [u8; 32],
}
impl std::fmt::Debug for DurableReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableReservation")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

/// An attempted creation keeps its slot and any original live owner.
pub struct DurableCreationCustody {
    reservation: Option<DurableReservation>,
    owner: Option<Owner>,
}
impl DurableCreationCustody {
    pub fn namespace(&self) -> i64 {
        self.reservation
            .as_ref()
            .expect("reservation retained")
            .namespace
    }
    pub fn identity(&self) -> [u8; 32] {
        self.reservation
            .as_ref()
            .expect("reservation retained")
            .identity
    }
    pub fn owner(&self) -> Option<&Owner> {
        self.owner.as_ref()
    }
}
impl Drop for DurableCreationCustody {
    fn drop(&mut self) {
        if let Some(reservation) = self.reservation.take() {
            std::mem::forget(reservation);
        }
        if let Some(owner) = self.owner.take() {
            std::mem::forget(owner);
        }
    }
}

/// Original creation/admission failure and the exact retained owner, if any.
pub struct DurableCreationFailure {
    pub cause: OwnerError,
    pub custody: DurableCreationCustody,
}
impl std::fmt::Debug for DurableCreationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableCreationFailure")
            .field("cause", &self.cause)
            .field("namespace", &self.custody.namespace())
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for DurableCreationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "durable Workspace creation: {}", self.cause)
    }
}
impl std::error::Error for DurableCreationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

/// A failed before-effect release returns the unchanged reservation.
pub struct DurableReleaseFailure {
    pub cause: OwnerError,
    pub reservation: DurableReservation,
}
impl std::fmt::Debug for DurableReleaseFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableReleaseFailure")
            .field("cause", &self.cause)
            .field("namespace", &self.reservation.namespace)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for DurableReleaseFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "durable Workspace reservation release: {}",
            self.cause
        )
    }
}
impl std::error::Error for DurableReleaseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

impl DurableReservation {
    pub fn namespace(&self) -> i64 {
        self.namespace
    }
    pub fn identity(&self) -> [u8; 32] {
        self.identity
    }

    /// Release only before attempting durable Workspace creation. The local
    /// namespace remains consumed and is never reused by this owner.
    pub fn release(self) -> Result<(), Box<DurableReleaseFailure>> {
        match self
            .roster
            .release_durable_reservation(self.namespace, self.identity)
        {
            Ok(()) => Ok(()),
            Err(cause) => Err(Box::new(DurableReleaseFailure {
                cause,
                reservation: self,
            })),
        }
    }

    /// Attempt creation once and admit the resulting exact owner into this slot.
    /// On any failure the caller keeps its original error and custody; there is
    /// no retry, cleanup or release of the attempted reservation.
    pub fn create(
        self,
        path: &Path,
        profile: ProfileConfig,
        expected_base_root: [u8; 32],
    ) -> Result<OwnerClient, Box<DurableCreationFailure>> {
        let spec = DurableWorkspace {
            path,
            profile,
            identity: self.identity,
            namespace: self.namespace,
            expected_base_root,
        };
        let owner = match Owner::create_durable(spec) {
            Ok(owner) => owner,
            Err(cause) => {
                return Err(Box::new(DurableCreationFailure {
                    cause,
                    custody: DurableCreationCustody {
                        reservation: Some(self),
                        owner: None,
                    },
                }))
            }
        };
        match self
            .roster
            .admit_reserved_durable(self.namespace, self.identity, owner)
        {
            Ok(client) => Ok(client),
            Err(failure) => {
                let DurableAdmissionFailure { cause, owner } = *failure;
                Err(Box::new(DurableCreationFailure {
                    cause,
                    custody: DurableCreationCustody {
                        reservation: Some(self),
                        owner: Some(owner),
                    },
                }))
            }
        }
    }
}

/// Authenticated identity and expected root of one durable Workspace.
/// The namespace identifies its daemon-local route. Schema 41 records its
/// per-Workspace high-water mark before a token can be exposed.
pub struct DurableWorkspace<'a> {
    pub path: &'a Path,
    pub profile: ProfileConfig,
    pub identity: [u8; 32],
    pub namespace: i64,
    pub expected_base_root: [u8; 32],
}

/// A before-effect roster refusal returns the original owner to its caller.
pub struct DurableAdmissionFailure {
    pub cause: OwnerError,
    pub owner: Owner,
}
impl std::fmt::Debug for DurableAdmissionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableAdmissionFailure")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for DurableAdmissionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "durable Workspace admission: {}", self.cause)
    }
}
impl std::error::Error for DurableAdmissionFailure {}

impl Owner {
    /// Reserve capacity and a fresh namespace before writing a discovery intent.
    pub fn reserve_durable(&self, identity: [u8; 32]) -> Result<DurableReservation, OwnerError> {
        self.client
            .roster()
            .ok_or(OwnerError::InvalidAdmission)?
            .reserve_durable(identity)
    }

    /// Transfers an already authorized durable owner into this bounded roster.
    /// The caller retains responsibility for discovery, authorization and old
    /// mount/process fencing. A refusal returns the original owner unchanged.
    pub fn adopt_durable(&self, owner: Owner) -> Result<OwnerClient, Box<DurableAdmissionFailure>> {
        match self.client.roster() {
            Some(roster) => roster.adopt_durable(owner),
            None => Err(Box::new(DurableAdmissionFailure {
                cause: OwnerError::InvalidAdmission,
                owner,
            })),
        }
    }

    /// Creates a durable, initially bound Workspace. An existing file is never
    /// replaced. A failed attempt retains its files and is not auto-retried.
    /// The caller must authorize the identity and base root before calling.
    pub fn create_durable(spec: DurableWorkspace<'_>) -> Result<Self, OwnerError> {
        let incarnation = admission(&spec)?;
        let db = Overlay::create_durable(spec.path, spec.profile, spec.identity)
            .map_err(OwnerError::Overlay)?;
        db.open_workspace(spec.expected_base_root)
            .map_err(OwnerError::Overlay)?;
        db.initialize_durable_namespace(spec.namespace)
            .map_err(OwnerError::Overlay)?;
        bind(db, spec.namespace, spec.identity, incarnation, true)
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
        if db.profile().schema_version == 41 {
            db.advance_durable_namespace(spec.namespace)
                .map_err(OwnerError::Overlay)?;
        }
        bind(db, spec.namespace, spec.identity, incarnation, false)
    }

    /// Reopens from the persisted snapshot when no external root is available.
    /// The returned snapshot still needs Store authorization and validation,
    /// and the caller must fence old mounts and reconcile unknown operations.
    /// For schema 41, `namespace` must be greater than the recorded value;
    /// older files require the caller's independent freshness proof.
    pub fn recover_recorded(
        path: &Path,
        profile: ProfileConfig,
        identity: [u8; 32],
        namespace: i64,
    ) -> Result<(Self, layerfs_history::BranchSnapshot), OwnerError> {
        Self::recover_recorded_with(path, profile, identity, namespace, false)
    }

    /// Consumes a namespace greater than this Workspace's durable high-water
    /// and no smaller than `minimum_namespace`. The caller serializes this
    /// choice with other live owners and still fences old mounts and consumers.
    /// An uncertain persistence result retains its original owner and cannot
    /// be automatically retried. Legacy formats require an external proof.
    pub fn recover_recorded_next(
        path: &Path,
        profile: ProfileConfig,
        identity: [u8; 32],
        minimum_namespace: i64,
    ) -> Result<(Self, layerfs_history::BranchSnapshot), OwnerError> {
        Self::recover_recorded_with(path, profile, identity, minimum_namespace, true)
    }

    fn recover_recorded_with(
        path: &Path,
        profile: ProfileConfig,
        identity: [u8; 32],
        namespace: i64,
        choose_next: bool,
    ) -> Result<(Self, layerfs_history::BranchSnapshot), OwnerError> {
        let spec = DurableWorkspace {
            path,
            profile,
            identity,
            namespace,
            expected_base_root: [0; 32],
        };
        let incarnation = admission(&spec)?;
        let db = Overlay::recover_durable(path, profile, identity).map_err(OwnerError::Overlay)?;
        if choose_next && db.profile().schema_version != 41 {
            return Err(OwnerError::Overlay(OverlayError::Invalid(
                "durable namespace freshness is not recorded",
            )));
        }
        let bytes =
            db.durable_binding()
                .map_err(OwnerError::Overlay)?
                .ok_or(OwnerError::Overlay(OverlayError::Invalid(
                    "durable binding is absent",
                )))?;
        let snapshot =
            layerfs_bridge::snapshot::decode(&bytes).map_err(OwnerError::BindingCodec)?;
        if snapshot.effective_root.to_bytes() != db.state().map_err(OwnerError::Overlay)?.base_root
        {
            return Err(OwnerError::Overlay(OverlayError::Invalid(
                "durable binding root",
            )));
        }
        let namespace =
            if choose_next {
                let previous = db.durable_namespace().map_err(OwnerError::Overlay)?.ok_or(
                    OwnerError::Overlay(OverlayError::Invalid("durable namespace is absent")),
                )?;
                previous
                    .checked_add(1)
                    .ok_or(OwnerError::IdentityExhausted)?
                    .max(namespace)
            } else {
                namespace
            };
        if db.profile().schema_version == 41 {
            db.advance_durable_namespace(namespace)
                .map_err(OwnerError::Overlay)?;
        }
        let owner = bind(db, namespace, identity, incarnation, false)?;
        Ok((owner, snapshot))
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
    /// Reads the saved snapshot without selecting a current Branch head.
    pub fn durable_snapshot(&self) -> Result<layerfs_history::BranchSnapshot, OwnerError> {
        let route = self.bound_route()?;
        let (bytes, root) = self.call(route, crate::ServiceClass::General, None, |db| {
            Ok((db.durable_binding()?, db.state()?.base_root))
        })?;
        let bytes = bytes.ok_or(OwnerError::Overlay(OverlayError::Invalid(
            "durable binding is absent",
        )))?;
        let snapshot =
            layerfs_bridge::snapshot::decode(&bytes).map_err(OwnerError::BindingCodec)?;
        if snapshot.effective_root.to_bytes() != root {
            return Err(OwnerError::Overlay(OverlayError::Invalid(
                "durable binding root",
            )));
        }
        Ok(snapshot)
    }
    pub(crate) fn ensure_durable_snapshot(
        &self,
        snapshot: &layerfs_history::BranchSnapshot,
    ) -> Result<(), OwnerError> {
        let route = self.bound_route()?;
        let version = self.call(route, crate::ServiceClass::General, None, |db| {
            Ok(db.profile().schema_version)
        })?;
        // Existing v39 databases have no binding row and are never silently
        // converted. Their caller still supplies trusted external metadata.
        if version == 39 {
            return Ok(());
        }
        let expected =
            layerfs_bridge::snapshot::encode(snapshot).map_err(OwnerError::BindingCodec)?;
        let recorded = self.call(route, crate::ServiceClass::General, None, |db| {
            db.durable_binding()
        })?;
        match recorded {
            Some(recorded) if recorded == expected => Ok(()),
            Some(_) => Err(OwnerError::Overlay(OverlayError::Invalid(
                "durable binding snapshot",
            ))),
            None if self.connection()?.durable_fresh => {
                let root = snapshot.effective_root.to_bytes();
                self.call(route, crate::ServiceClass::General, None, move |db| {
                    db.initialize_durable_binding(root, &expected)
                })
            }
            None => Err(OwnerError::Overlay(OverlayError::Invalid(
                "durable binding is absent",
            ))),
        }
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
    fresh: bool,
) -> Result<Owner, OwnerError> {
    let route = Route::new(namespace, db.engine(), incarnation).map_err(OwnerError::Overlay)?;
    let profile = db.profile().clone();
    let shared = Arc::new(Shared::durable(namespace, identity, fresh));
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
