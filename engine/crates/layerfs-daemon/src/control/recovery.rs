//! One manually fenced, ordered recovery of a complete discovery catalog.
use super::{
    DurableRegistrationFailure, RecordedRegistrationError, RecordedRegistrationFailure, Service,
};
use crate::{
    durable_roster::{Authority, Roster, RosterError, WorkspaceRecord},
    store::{BindRefusal, BindRequest},
    Owner, OwnerError,
};
use layerfs_bridge::control::WorkspaceToken;
use layerfs_history::BranchSnapshot;
use layerfs_overlay::ProfileConfig;
use layerfs_store_service::DurableAdmissionFailure;
use std::{fmt, path::Path};

/// The exact operation that stopped this original recovery attempt.
#[derive(Debug)]
pub enum CatalogRecoveryCause {
    Invalid(&'static str),
    Capacity {
        requested: usize,
        capacity: usize,
    },
    Discovery(RosterError),
    Roster(OwnerError),
    DiscoveryAfterAdmission {
        record: WorkspaceRecord,
        cause: RosterError,
        route: layerfs_workspace::Route,
        snapshot: Box<BranchSnapshot>,
    },
    Open {
        record: WorkspaceRecord,
        cause: OwnerError,
    },
    Unadmitted {
        record: WorkspaceRecord,
        reason: &'static str,
        failure: Box<DurableAdmissionFailure>,
        snapshot: Box<BranchSnapshot>,
    },
    Bind {
        record: WorkspaceRecord,
        route: layerfs_workspace::Route,
        failure: Box<BindRefusal>,
    },
    Register {
        record: WorkspaceRecord,
        failure: Box<RecordedRegistrationFailure>,
    },
}

/// Completed control tokens and original failure are retained together. The
/// caller must also retain the original Service and Owner on any failure.
#[derive(Debug)]
pub struct CatalogRecoveryFailure {
    pub cause: CatalogRecoveryCause,
    pub completed: Vec<WorkspaceToken>,
}
impl fmt::Display for CatalogRecoveryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "catalog recovery: {:?}", self.cause)
    }
}
impl std::error::Error for CatalogRecoveryFailure {}

impl Service {
    /// Reopens every catalog entry without mounting it, in recorded order.
    /// Before calling, the trusted caller authenticates `sandbox`, proves the
    /// physical Store and Overlay directory, fences old processes, mounts and
    /// consumers, and reconciles unknown external operations. Keep the Service
    /// and roster private from concurrent admissions until this returns.
    /// A failure never retries or silently skips an entry; its original cause,
    /// already registered tokens and any unadmitted owner are returned.
    pub fn recover_catalog(
        &self,
        catalog: &mut Roster,
        owner: &Owner,
        overlay_directory: &Path,
        sandbox: [u8; 32],
        profile: ProfileConfig,
        minimum_namespace: i64,
    ) -> Result<Vec<WorkspaceToken>, Box<CatalogRecoveryFailure>> {
        let mut completed = Vec::new();
        if minimum_namespace <= 0 || minimum_namespace == i64::MAX {
            return Err(failed(
                CatalogRecoveryCause::Invalid("recovery namespace floor"),
                completed,
            ));
        }
        if profile.pager_kib == 0 || profile.max_pages.is_some() {
            return Err(failed(
                CatalogRecoveryCause::Invalid("durable recovery profile"),
                completed,
            ));
        }
        if !self.owner.same_roster(&owner.client()) || self.workspaces() != Some(0) {
            return Err(failed(
                CatalogRecoveryCause::Invalid("recovery owner or nonempty Service"),
                completed,
            ));
        }
        let floor = match owner.vacant_namespace_floor() {
            Ok(floor) => floor,
            Err(cause) => return Err(failed(CatalogRecoveryCause::Roster(cause), completed)),
        };
        let expected = Authority::for_catalog(sandbox, self.store.history());
        if !expected.is_ok_and(|expected| expected == catalog.authority()) {
            return Err(failed(
                CatalogRecoveryCause::Invalid("recovery catalog authority"),
                completed,
            ));
        }
        let mut records = Vec::new();
        if let Err(cause) = catalog.visit(|record| records.push(record)) {
            return Err(failed(CatalogRecoveryCause::Discovery(cause), completed));
        }
        if records.len() > self.capacity {
            return Err(failed(
                CatalogRecoveryCause::Capacity {
                    requested: records.len(),
                    capacity: self.capacity,
                },
                completed,
            ));
        }

        let mut next = minimum_namespace.max(floor);
        for record in records {
            let path = overlay_directory.join(record.overlay_name());
            let (recovered, snapshot) = match Owner::recover_recorded_next(
                &path,
                profile,
                record.workspace.to_bytes(),
                next,
            ) {
                Ok(recovered) => recovered,
                Err(cause) => {
                    return Err(failed(
                        CatalogRecoveryCause::Open { record, cause },
                        completed,
                    ))
                }
            };
            let route = match recovered.client().bound_route() {
                Ok(route) => route,
                Err(cause) => {
                    return Err(unadmitted(
                        record,
                        "recovered route",
                        cause,
                        recovered,
                        snapshot,
                        completed,
                    ))
                }
            };
            let namespace = route.namespace();
            let Some(after) = namespace.checked_add(1) else {
                return Err(unadmitted(
                    record,
                    "namespace exhausted",
                    OwnerError::IdentityExhausted,
                    recovered,
                    snapshot,
                    completed,
                ));
            };
            if snapshot.branch.id != record.branch {
                return Err(unadmitted(
                    record,
                    "recorded Branch mismatch",
                    OwnerError::InvalidAdmission,
                    recovered,
                    snapshot,
                    completed,
                ));
            }
            let client = match owner.adopt_durable(recovered) {
                Ok(client) => client,
                Err(failure) => {
                    return Err(failed(
                        CatalogRecoveryCause::Unadmitted {
                            record,
                            reason: "roster admission",
                            failure,
                            snapshot: Box::new(snapshot),
                        },
                        completed,
                    ))
                }
            };
            if let Err(cause) = catalog.check_live() {
                return Err(failed(
                    CatalogRecoveryCause::DiscoveryAfterAdmission {
                        record,
                        cause,
                        route,
                        snapshot: Box::new(snapshot),
                    },
                    completed,
                ));
            }
            let binding = match self.store.bind_durable(
                client,
                BindRequest {
                    workspace: record.workspace,
                    branch: record.branch,
                },
                snapshot,
            ) {
                Ok(bound) => bound.workspace,
                Err(failure) => {
                    return Err(failed(
                        CatalogRecoveryCause::Bind {
                            record,
                            route,
                            failure: Box::new(failure),
                        },
                        completed,
                    ))
                }
            };
            match self.register_durable(binding) {
                Ok(token) => completed.push(token),
                Err(failure) => {
                    let DurableRegistrationFailure { cause, workspace } = *failure;
                    let failure = Box::new(RecordedRegistrationFailure {
                        cause: RecordedRegistrationError::Control(cause),
                        workspace,
                    });
                    return Err(failed(
                        CatalogRecoveryCause::Register { record, failure },
                        completed,
                    ));
                }
            }
            next = after;
        }
        Ok(completed)
    }
}

fn failed(
    cause: CatalogRecoveryCause,
    completed: Vec<WorkspaceToken>,
) -> Box<CatalogRecoveryFailure> {
    Box::new(CatalogRecoveryFailure { cause, completed })
}

fn unadmitted(
    record: WorkspaceRecord,
    reason: &'static str,
    cause: OwnerError,
    owner: Owner,
    snapshot: BranchSnapshot,
    completed: Vec<WorkspaceToken>,
) -> Box<CatalogRecoveryFailure> {
    failed(
        CatalogRecoveryCause::Unadmitted {
            record,
            reason,
            failure: Box::new(DurableAdmissionFailure { cause, owner }),
            snapshot: Box::new(snapshot),
        },
        completed,
    )
}
