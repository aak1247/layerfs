//! One fenced physical Store transition across all of its original rosters.
use super::{
    Authority, CatalogTransitionCause, CatalogTransitionConfig, CatalogTransitionFailure,
    CheckpointUpgradeFailure, File, OpenedStore, Owner, PreparationPurpose,
    PreparedCatalogTransition, PreparedCheckpointUpgrade, TransitionState, WorkspaceRecord,
};
use crate::{bootstrap::open_existing_store_observed, store::MaintainedCatalogRecovery};
use layerfs_storage::{port::PersistenceError, StorageError};
use std::{
    collections::{BTreeSet, VecDeque},
    fmt,
    fs::OpenOptions,
    io,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    sync::{Arc, Mutex},
};

/// The exact point at which a full-Store cohort transition stopped.
#[derive(Debug)]
pub enum CatalogCohortCause {
    Invalid(&'static str),
    Group {
        index: usize,
        failure: Box<CatalogTransitionFailure>,
    },
    Check {
        index: usize,
        cause: CatalogTransitionCause,
    },
    Directory(io::Error),
    Store(StorageError),
    StoreUpgrade(Box<CheckpointUpgradeFailure>),
    Persistence(PersistenceError),
    OverlayIdentity {
        index: usize,
        record: WorkspaceRecord,
        cause: layerfs_overlay::OverlayError,
    },
    Migration {
        index: usize,
        record: WorkspaceRecord,
        failure: Box<crate::store::CatalogMigrationFailure>,
    },
}

/// An unsuccessful attempt retains its original groups and all acquired
/// leases. The caller must also retain its original Owner and external fence.
pub struct CatalogCohortFailure {
    cause: Option<CatalogCohortCause>,
    _state: Mutex<CohortState>,
}
impl CatalogCohortFailure {
    pub fn cause(&self) -> &CatalogCohortCause {
        self.cause.as_ref().expect("original cohort cause retained")
    }

    /// Observed progress only; it does not authorize replay after an unknown
    /// Store or Overlay result.
    pub fn progress(&self) -> CatalogCohortProgress {
        let state = self
            ._state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let completed = state
            .completed
            .iter()
            .map(|entry| CatalogCohortProgressEntry {
                group: entry.group,
                record: entry.record,
                previous_namespace: entry.previous_namespace(),
                source_version: entry.source_version(),
            })
            .collect::<Vec<_>>();
        let pending = state
            .groups
            .iter()
            .enumerate()
            .flat_map(|(index, group)| {
                group
                    .records
                    .iter()
                    .copied()
                    .filter_map(|record| {
                        (!completed
                            .iter()
                            .any(|entry| entry.group == index && entry.record == record))
                        .then_some((index, record))
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        CatalogCohortProgress {
            store_upgraded: state.applied,
            completed,
            pending,
            current_group: match self.cause.as_ref() {
                Some(CatalogCohortCause::Group { index, .. })
                | Some(CatalogCohortCause::Check { index, .. })
                | Some(CatalogCohortCause::OverlayIdentity { index, .. })
                | Some(CatalogCohortCause::Migration { index, .. }) => Some(*index),
                _ => None,
            },
            unprepared_groups: state.remaining.len(),
        }
    }
}

/// A checked-closed migrated member's original namespace and source format.
#[derive(Clone, Debug)]
pub struct CatalogCohortProgressEntry {
    pub group: usize,
    pub record: WorkspaceRecord,
    pub previous_namespace: i64,
    pub source_version: i64,
}

/// Facts available while the failure still owns its original custody.
#[derive(Clone, Debug)]
pub struct CatalogCohortProgress {
    pub store_upgraded: Option<bool>,
    pub completed: Vec<CatalogCohortProgressEntry>,
    /// Members of groups whose preparation returned successfully.
    pub pending: Vec<(usize, WorkspaceRecord)>,
    /// The group named by the original failure, including a group whose
    /// preparation did not complete and is absent from `pending`.
    pub current_group: Option<usize>,
    /// Only the tail that had not yet been taken for preparation.
    pub unprepared_groups: usize,
}
impl fmt::Debug for CatalogCohortFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogCohortFailure")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for CatalogCohortFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "catalog cohort transition: {:?}", self.cause)
    }
}
impl std::error::Error for CatalogCohortFailure {}
impl Drop for CatalogCohortFailure {
    fn drop(&mut self) {
        let state = self
            ._state
            .get_mut()
            .unwrap_or_else(|poison| poison.into_inner());
        if state.preserve_on_drop {
            if let Some(cause) = self.cause.take() {
                std::mem::forget(cause);
            }
        }
    }
}

struct CohortState {
    remaining: VecDeque<CatalogTransitionConfig>,
    groups: Vec<TransitionState>,
    original: Option<File>,
    anchor: Option<File>,
    opened: Option<OpenedStore>,
    completed: Vec<CatalogCohortEntry>,
    applied: Option<bool>,
    preserve_on_drop: bool,
}
impl CohortState {
    fn failed(self, cause: CatalogCohortCause) -> Box<CatalogCohortFailure> {
        Box::new(CatalogCohortFailure {
            cause: Some(cause),
            _state: Mutex::new(self),
        })
    }

    fn check_group(&mut self, index: usize) -> Result<(), CatalogCohortCause> {
        let group = &mut self.groups[index];
        group
            .check_roots()
            .and_then(|()| group.check_named_store())
            .and_then(|()| group.check_records())
            .map_err(|cause| CatalogCohortCause::Check { index, cause })
    }

    fn check_groups(&mut self) -> Result<(), CatalogCohortCause> {
        for index in 0..self.groups.len() {
            self.check_group(index)?;
        }
        Ok(())
    }

    fn check_group_live(&mut self, index: usize) -> Result<(), CatalogCohortCause> {
        let group = &mut self.groups[index];
        group
            .check_roots()
            .and_then(|()| group.check_named_store())
            .and_then(|()| {
                group
                    .roster
                    .as_mut()
                    .unwrap()
                    .check_live()
                    .map_err(CatalogTransitionCause::Roster)
            })
            .map_err(|cause| CatalogCohortCause::Check { index, cause })
    }

    fn check_members(&self) -> Result<(), CatalogCohortCause> {
        for (index, group) in self.groups.iter().enumerate() {
            for (record, pending) in &group.legacy_entries {
                pending
                    .verify_original()
                    .map_err(|cause| CatalogCohortCause::OverlayIdentity {
                        index,
                        record: *record,
                        cause,
                    })?;
            }
        }
        for entry in &self.completed {
            entry.maintained.verify_original().map_err(|cause| {
                CatalogCohortCause::OverlayIdentity {
                    index: entry.group,
                    record: entry.record,
                    cause,
                }
            })?;
        }
        Ok(())
    }

    fn check_distinct(&self) -> Result<(), CatalogCohortCause> {
        let first = self
            .groups
            .first()
            .ok_or(CatalogCohortCause::Invalid("empty Store cohort"))?;
        let mut sandboxes = BTreeSet::new();
        let mut workspaces = BTreeSet::new();
        let mut roots = Vec::new();
        for group in &self.groups {
            if group.physical != first.physical
                || group.incarnation != first.incarnation
                || group.original_identity != first.original_identity
                || !sandboxes.insert(group.config.sandbox)
            {
                return Err(CatalogCohortCause::Invalid("Store cohort identity differs"));
            }
            for record in &group.records {
                if !workspaces.insert(record.workspace.to_bytes()) {
                    return Err(CatalogCohortCause::Invalid(
                        "Workspace appears in more than one roster",
                    ));
                }
            }
            for root in [&group.catalog_root, &group.overlay_root] {
                let root = root
                    .as_ref()
                    .ok_or(CatalogCohortCause::Invalid("cohort directory absent"))?;
                let identity = (
                    root.device().map_err(CatalogCohortCause::Directory)?,
                    root.inode().map_err(CatalogCohortCause::Directory)?,
                );
                if roots
                    .iter()
                    .any(|(path, old_identity): &(std::path::PathBuf, (u64, u64))| {
                        *old_identity == identity
                            || root.path.starts_with(path)
                            || path.starts_with(&root.path)
                    })
                {
                    return Err(CatalogCohortCause::Invalid("cohort directories overlap"));
                }
                roots.push((root.path.clone(), identity));
            }
        }
        Ok(())
    }
}
impl Drop for CohortState {
    fn drop(&mut self) {
        if !self.preserve_on_drop {
            return;
        }
        if let Some(original) = self.original.take() {
            std::mem::forget(original);
        }
        if let Some(anchor) = self.anchor.take() {
            std::mem::forget(anchor);
        }
        if let Some(opened) = self.opened.take() {
            std::mem::forget(opened);
        }
        std::mem::forget(std::mem::take(&mut self.groups));
        std::mem::forget(std::mem::take(&mut self.completed));
    }
}

/// All original rosters and leases authenticated without changing a Store or
/// Overlay. The caller retains the same external Store-wide fence throughout.
pub struct PreparedCatalogCohort {
    state: CohortState,
}

/// A completed Workspace remains under its original lease and old namespace.
pub struct CatalogCohortEntry {
    pub group: usize,
    pub record: WorkspaceRecord,
    maintained: MaintainedCatalogRecovery,
}
impl CatalogCohortEntry {
    pub fn previous_namespace(&self) -> i64 {
        self.maintained.observed().previous_namespace
    }
    pub fn source_version(&self) -> i64 {
        self.maintained.source_version()
    }
}

/// All SQLite maintenance handles have closed; original roster and Overlay
/// leases remain held until this value is dropped after the owner's receipt.
pub struct CatalogCohortSuccess {
    state: CohortState,
}
impl CatalogCohortSuccess {
    pub fn store_upgraded(&self) -> bool {
        self.state.applied.expect("Store outcome recorded")
    }
    pub fn completed(&self) -> &[CatalogCohortEntry] {
        &self.state.completed
    }
}

fn same_store(a: &CatalogTransitionConfig, b: &CatalogTransitionConfig) -> bool {
    a.store.backend == b.store.backend
        && a.store.path == b.store.path
        && a.store.sqlite_profile == b.store.sqlite_profile
        && a.store.sqlite_pack_layout == b.store.sqlite_pack_layout
        && a.store.sqlite_acquisition == b.store.sqlite_acquisition
        && a.binding == b.binding
        && a.cursor_key == b.cursor_key
}

impl PreparedCatalogCohort {
    /// Authenticate every supplied original roster and its ready Workspaces.
    /// Before calling, the trusted physical-Store owner must freeze all new
    /// users and fence every old writer, reader, mount and consumer across all
    /// groups. Local leases and this list do not themselves prove that fence.
    pub fn prepare(
        configs: Vec<CatalogTransitionConfig>,
        original: File,
        owner: &Owner,
    ) -> Result<Self, Box<CatalogCohortFailure>> {
        let mut state = CohortState {
            remaining: configs.into(),
            groups: Vec::new(),
            original: Some(original),
            anchor: None,
            opened: None,
            completed: Vec::new(),
            applied: None,
            preserve_on_drop: false,
        };
        let result = (|| -> Result<(), CatalogCohortCause> {
            let first = state
                .remaining
                .front()
                .ok_or(CatalogCohortCause::Invalid("empty Store cohort"))?;
            if state
                .remaining
                .iter()
                .any(|config| !same_store(first, config))
            {
                return Err(CatalogCohortCause::Invalid("Store cohort config differs"));
            }
            while let Some(config) = state.remaining.pop_front() {
                let index = state.groups.len();
                let original = state.original.take().expect("original Store retained");
                let prepared = PreparedCatalogTransition::prepare_for(
                    config,
                    original,
                    owner,
                    PreparationPurpose::Maintenance,
                )
                .map_err(|failure| CatalogCohortCause::Group { index, failure })?;
                let mut group = prepared.state;
                state.original = group.original.take();
                state.groups.push(group);
                if state.original.is_none() {
                    return Err(CatalogCohortCause::Invalid("original Store absent"));
                }
            }
            state.check_distinct()?;
            state.check_groups()?;
            state.check_members()?;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(Self { state }),
            Err(cause) => Err(state.failed(cause)),
        }
    }

    /// Perform one Store transition followed by every Overlay transition.
    /// No Workspace is admitted and no namespace is advanced. The same fence
    /// must remain held until the owner records the result and drops it.
    pub fn execute(mut self) -> Result<CatalogCohortSuccess, Box<CatalogCohortFailure>> {
        let result = (|| -> Result<(), CatalogCohortCause> {
            self.state.check_distinct()?;
            self.state.check_groups()?;
            self.state.check_members()?;
            let group = &self.state.groups[0];
            let config = group.config.store.clone();
            let binding = group.config.binding.clone();
            let cursor = group.config.cursor_key;
            let incarnation = group.incarnation;
            let physical = group.physical.clone().expect("original physical Store");
            let capacity = group.config.workspace_capacity;
            let cache_bytes = group.config.cache_bytes;
            let reservations = group.config.reservations;
            self.state.preserve_on_drop = true;
            let original = self.state.original.take().expect("original Store retained");
            let prepared =
                PreparedCheckpointUpgrade::prepare(config.clone(), original, &binding, incarnation)
                    .map_err(CatalogCohortCause::StoreUpgrade)?;
            let outcome = prepared.apply().map_err(CatalogCohortCause::StoreUpgrade)?;
            self.state.applied = Some(outcome.applied);
            self.state.original = Some(outcome.original);
            let original = self
                .state
                .original
                .as_ref()
                .unwrap()
                .metadata()
                .map_err(CatalogCohortCause::Directory)?;
            let anchor = OpenOptions::new()
                .read(true)
                .custom_flags(
                    (nix::fcntl::OFlag::O_PATH
                        | nix::fcntl::OFlag::O_NOFOLLOW
                        | nix::fcntl::OFlag::O_CLOEXEC)
                        .bits(),
                )
                .open(&config.path)
                .map_err(CatalogCohortCause::Directory)?;
            let anchored = anchor.metadata().map_err(CatalogCohortCause::Directory)?;
            if !anchored.is_file()
                || (
                    original.dev(),
                    original.ino(),
                    original.uid(),
                    original.mode(),
                    original.nlink(),
                ) != (
                    anchored.dev(),
                    anchored.ino(),
                    anchored.uid(),
                    anchored.mode(),
                    anchored.nlink(),
                )
            {
                return Err(CatalogCohortCause::Invalid("Store cohort anchor differs"));
            }
            self.state.anchor = Some(anchor);
            self.state.check_groups()?;
            drop(self.state.original.take());
            self.state.opened = Some(
                open_existing_store_observed(
                    config,
                    self.state.anchor.as_ref().unwrap(),
                    &binding,
                    cursor,
                    capacity,
                    cache_bytes,
                    reservations,
                )
                .map_err(CatalogCohortCause::Store)?,
            );
            let store = &self.state.opened.as_ref().unwrap().store;
            store
                .verify_original_physical(&physical)
                .map_err(CatalogCohortCause::Persistence)?;
            store
                .history()
                .require_checkpoint_receipts()
                .map_err(|cause| CatalogCohortCause::Check {
                    index: 0,
                    cause: CatalogTransitionCause::History(cause),
                })?;
            for (index, group) in self.state.groups.iter().enumerate() {
                let authority = Authority::for_catalog(group.config.sandbox, store.history())
                    .map_err(|cause| CatalogCohortCause::Check {
                        index,
                        cause: CatalogTransitionCause::Roster(cause),
                    })?;
                if authority != group.roster.as_ref().unwrap().authority()
                    || store.history().incarnation() != group.incarnation
                {
                    return Err(CatalogCohortCause::Invalid(
                        "reopened cohort authority changed",
                    ));
                }
            }
            for index in 0..self.state.groups.len() {
                while !self.state.groups[index].legacy_entries.is_empty() {
                    self.state.check_group_live(index)?;
                    let (record, plan) = self.state.groups[index].legacy_entries.front().unwrap();
                    plan.verify_original().map_err(|cause| {
                        CatalogCohortCause::OverlayIdentity {
                            index,
                            record: *record,
                            cause,
                        }
                    })?;
                    let (record, plan) =
                        self.state.groups[index].legacy_entries.pop_front().unwrap();
                    let maintained = self
                        .state
                        .opened
                        .as_ref()
                        .unwrap()
                        .store
                        .migrate_catalog_for_maintenance(plan)
                        .map_err(|failure| CatalogCohortCause::Migration {
                            index,
                            record,
                            failure,
                        })?;
                    self.state.completed.push(CatalogCohortEntry {
                        group: index,
                        record,
                        maintained,
                    });
                    let completed = self.state.completed.last().unwrap();
                    if completed.maintained.request().workspace != record.workspace
                        || completed.maintained.request().branch != record.branch
                    {
                        return Err(CatalogCohortCause::Invalid("cohort Workspace changed"));
                    }
                }
            }
            self.state.check_groups()?;
            self.state.check_members()?;
            if Arc::strong_count(&self.state.opened.as_ref().unwrap().store) != 1 {
                return Err(CatalogCohortCause::Invalid(
                    "maintenance Store handles remain live",
                ));
            }
            drop(self.state.opened.take());
            self.state.preserve_on_drop = false;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(CatalogCohortSuccess { state: self.state }),
            Err(cause) => Err(self.state.failed(cause)),
        }
    }
}
