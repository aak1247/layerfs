//! Short control admission over bounded daemon-owned Workspace routes.
use super::{
    kernel::{Kernel, KernelServing},
    Failure,
};
use crate::{
    store::{BoundWorkspace, Store},
    Owner, OwnerClient,
};
use layerfs_bridge::control::{Activity, ControlCode, WorkspaceToken};
use layerfs_history::{BranchSnapshot, CommitOutcome, WorkspaceId};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
pub(super) struct Binding {
    pub kernel: Kernel,
    pub workspace: Arc<BoundWorkspace>,
    pub snapshot: BranchSnapshot,
    pub activity: Activity,
    /// Catalog-backed bindings must not bypass their recorded close protocol.
    pub recorded: bool,
    pub epoch: u64,
    pub published: Option<CommitOutcome>,
}
pub(super) enum Entry {
    Binding,
    Bound(Box<Binding>),
}
/// One daemon's kernel control owner. Whole operations run outside its registry mutex.
pub struct Service {
    pub(crate) store: Arc<Store>,
    pub(super) owner: OwnerClient,
    pub(super) capacity: usize,
    pub(super) entries: Mutex<BTreeMap<WorkspaceId, Entry>>,
    /// Absent for a control-only Service: Attach is then refused before effects.
    pub(super) kernel: Option<Arc<KernelServing>>,
    #[cfg(target_os = "linux")]
    pub(super) durable: Option<super::durable_control::DurableControl>,
}
/// A failed pre-effect registration retains the original bound Workspace.
pub struct DurableRegistrationFailure {
    pub cause: Failure,
    pub workspace: Box<BoundWorkspace>,
}
impl std::fmt::Debug for DurableRegistrationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableRegistrationFailure")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for DurableRegistrationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "durable Workspace registration: {:?}",
            self.cause
        )
    }
}
impl std::error::Error for DurableRegistrationFailure {}
impl Service {
    /// Binds control to the already initialized owner and its configured capacity.
    pub fn new(store: Arc<Store>, owner: &Owner) -> Self {
        Self {
            store,
            owner: owner.client(),
            capacity: owner.configuration().namespaces,
            entries: Mutex::new(BTreeMap::new()),
            kernel: None,
            #[cfg(target_os = "linux")]
            durable: None,
        }
    }
    /// Binds control and kernel serving to the same Store and overlay owner.
    pub fn with_kernel(store: Arc<Store>, owner: &Owner, kernel: Arc<KernelServing>) -> Self {
        Self {
            kernel: Some(kernel),
            ..Self::new(store, owner)
        }
    }
    /// Admits one already-authorized durable binding to ordinary control and
    /// Attach. The caller must fence the old mount and consumers, reconcile
    /// unknown operations, and use a namespace fresh across daemon restarts.
    /// A refusal retains the original binding and creates no registry entry.
    pub fn register_durable(
        &self,
        workspace: BoundWorkspace,
    ) -> Result<WorkspaceToken, Box<DurableRegistrationFailure>> {
        #[cfg(target_os = "linux")]
        if self.durable.is_some() {
            return Err(Box::new(DurableRegistrationFailure {
                cause: Failure::Rejected(ControlCode::Invalid, "durable control owns admission"),
                workspace: Box::new(workspace),
            }));
        }
        self.register_durable_with_recording(workspace, false)
    }

    pub(super) fn register_durable_with_recording(
        &self,
        workspace: BoundWorkspace,
        recorded: bool,
    ) -> Result<WorkspaceToken, Box<DurableRegistrationFailure>> {
        let refuse = |cause, workspace| {
            Box::new(DurableRegistrationFailure {
                cause,
                workspace: Box::new(workspace),
            })
        };
        let owner = self.owner.workspace_client(workspace.route());
        if !owner.as_ref().is_ok_and(|owner| workspace.owned_by(owner))
            || !workspace.belongs_to(&self.store)
            || !workspace.has_verified_durable_overlay()
        {
            return Err(refuse(
                Failure::Rejected(ControlCode::Invalid, "Workspace authority mismatch"),
                workspace,
            ));
        }
        let snapshot = match workspace.snapshot() {
            Ok(snapshot) => snapshot,
            Err(cause) => return Err(refuse(Failure::Workspace(cause), workspace)),
        };
        let token = WorkspaceToken {
            workspace: workspace.identity(),
            namespace: workspace.route().namespace(),
        };
        let mut entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(_) => return Err(refuse(Failure::Poisoned, workspace)),
        };
        if entries.contains_key(&token.workspace) {
            return Err(refuse(
                Failure::Rejected(ControlCode::Busy, "Workspace already admitted"),
                workspace,
            ));
        }
        if entries.len() >= self.capacity {
            return Err(refuse(
                Failure::Rejected(ControlCode::Capacity, "daemon Workspace capacity"),
                workspace,
            ));
        }
        entries.insert(
            token.workspace,
            Entry::Bound(Box::new(Binding {
                kernel: Kernel::Unattached,
                workspace: Arc::new(workspace),
                snapshot,
                activity: Activity::Idle,
                recorded,
                epoch: 1,
                published: None,
            })),
        );
        Ok(token)
    }
    /// Retains the selected binding for ordinary local projection/operations.
    /// Terminal admission prevents new transfers; stale tokens never redirect.
    pub fn operation(
        &self,
        token: WorkspaceToken,
    ) -> Result<crate::store::StoreOperation, Failure> {
        #[cfg(target_os = "linux")]
        self.check_durable_state()?;
        let workspace = {
            let entries = self.entries.lock().map_err(|_| Failure::Poisoned)?;
            let value = bound(&entries, token)?;
            if value.activity == Activity::Closing
                || (value.recorded && value.activity == Activity::Uncertain)
            {
                return Err(Failure::Rejected(ControlCode::Busy, "Workspace is closing"));
            }
            value.workspace.clone()
        };
        workspace.operation().map_err(Failure::Workspace)
    }
    /// Workspaces bound or binding; None when the registry is poisoned.
    pub fn workspaces(&self) -> Option<usize> {
        self.entries.lock().ok().map(|entries| entries.len())
    }
    pub(super) fn admit(
        &self,
        token: WorkspaceToken,
        activity: Activity,
    ) -> Result<Arc<BoundWorkspace>, Failure> {
        let mut entries = self.entries.lock().map_err(|_| Failure::Poisoned)?;
        let value = bound_mut(&mut entries, token)?;
        idle(value)?;
        value.activity = activity;
        value.epoch = value.epoch.saturating_add(1);
        Ok(value.workspace.clone())
    }
}
pub(super) fn bound(
    entries: &BTreeMap<WorkspaceId, Entry>,
    token: WorkspaceToken,
) -> Result<&Binding, Failure> {
    let value = match entries.get(&token.workspace) {
        Some(Entry::Bound(value)) => value,
        Some(Entry::Binding) => {
            return Err(Failure::Rejected(
                ControlCode::Busy,
                "Workspace binding retained",
            ))
        }
        None => return Err(Failure::Rejected(ControlCode::Missing, "Workspace absent")),
    };
    check(value, token)?;
    Ok(value)
}
pub(super) fn bound_mut(
    entries: &mut BTreeMap<WorkspaceId, Entry>,
    token: WorkspaceToken,
) -> Result<&mut Binding, Failure> {
    let value = match entries.get_mut(&token.workspace) {
        Some(Entry::Bound(value)) => value,
        Some(Entry::Binding) => {
            return Err(Failure::Rejected(
                ControlCode::Busy,
                "Workspace binding retained",
            ))
        }
        None => return Err(Failure::Rejected(ControlCode::Missing, "Workspace absent")),
    };
    check(value, token)?;
    Ok(value)
}
fn check(value: &Binding, token: WorkspaceToken) -> Result<(), Failure> {
    if value.workspace.route().namespace() != token.namespace {
        return Err(Failure::Rejected(
            ControlCode::Invalid,
            "stale Workspace namespace",
        ));
    }
    Ok(())
}

pub(super) fn idle(value: &Binding) -> Result<(), Failure> {
    match value.activity {
        Activity::Idle => Ok(()),
        Activity::Uncertain | Activity::LocalFailure => Err(Failure::Custody(Box::new(
            layerfs_bridge::control::ControlRefusal {
                code: ControlCode::Unknown,
                phase: "admission".into(),
                moved: None,
                published: value.published.clone(),
                detail: "original Commit custody retained".into(),
            },
        ))),
        _ => Err(Failure::Rejected(
            ControlCode::Busy,
            "Workspace control operation active",
        )),
    }
}
