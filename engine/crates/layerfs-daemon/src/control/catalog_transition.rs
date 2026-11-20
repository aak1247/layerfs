//! One fenced Store and roster transition, retaining every original lease.
mod cohort;

use super::{durable_root::DurableRoot, CatalogRecoveryFailure, KernelServing, Service};
use crate::{
    bootstrap::{open_store_observed, OpenedStore},
    durable_roster::{Authority, Roster, RosterError, WorkspaceRecord},
    store::{
        BindRequest, CatalogMigrationFailure, CatalogPreparationFailure, PreparedCatalogRecovery,
        PreparedLegacyRecovery, ReaderFactory, Store, StoreReader,
    },
    Owner, OwnerClient, OwnerError,
};
pub use cohort::{
    CatalogCohortCause, CatalogCohortEntry, CatalogCohortFailure, CatalogCohortProgress,
    CatalogCohortProgressEntry, CatalogCohortSuccess, PreparedCatalogCohort,
};
use layerfs_bridge::control::WorkspaceToken;
use layerfs_overlay::{OverlayCatalogRecovery, OverlayError, ProfileConfig};
use layerfs_persistence::{
    CheckpointUpgradeFailure, Handles, PersistenceConfig, PreparedCheckpointUpgrade,
    StorageProvider, StorePhysicalIdentity,
};
use layerfs_storage::{ReservationBlocks, Storage, StorageError};
use std::{
    collections::VecDeque,
    fmt,
    fs::{self, File},
    io,
    os::unix::fs::MetadataExt,
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// Original Store and catalog configuration supplied by the trusted owner.
pub struct CatalogTransitionConfig {
    pub store: PersistenceConfig,
    pub binding: Vec<u8>,
    pub cursor_key: [u8; 32],
    pub catalog_directory: PathBuf,
    pub overlay_directory: PathBuf,
    pub sandbox: [u8; 32],
    pub profile: ProfileConfig,
    pub minimum_namespace: i64,
    pub workspace_capacity: usize,
    pub cache_bytes: usize,
    pub reservations: ReservationBlocks,
}

/// The exact failure of one original transition attempt.
#[derive(Debug)]
pub enum CatalogTransitionCause {
    Invalid(&'static str),
    Directory(io::Error),
    Store(StorageError),
    History(layerfs_history::HistoryError),
    Persistence(layerfs_storage::port::PersistenceError),
    Roster(RosterError),
    Owner(OwnerError),
    Overlay {
        record: WorkspaceRecord,
        cause: OverlayError,
    },
    Preparation {
        record: WorkspaceRecord,
        failure: Box<CatalogPreparationFailure>,
    },
    Migration {
        record: WorkspaceRecord,
        failure: Box<CatalogMigrationFailure>,
    },
    StoreUpgrade(Box<CheckpointUpgradeFailure>),
    Recovery(Box<CatalogRecoveryFailure>),
}

/// Holds every remaining original lease and any committed prefix on failure.
pub struct CatalogTransitionFailure {
    cause: Option<CatalogTransitionCause>,
    _state: Mutex<TransitionState>,
}
impl CatalogTransitionFailure {
    pub fn cause(&self) -> &CatalogTransitionCause {
        self.cause.as_ref().expect("original cause retained")
    }
}
impl fmt::Debug for CatalogTransitionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogTransitionFailure")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for CatalogTransitionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "catalog transition: {:?}", self.cause)
    }
}
impl std::error::Error for CatalogTransitionFailure {}
impl Drop for CatalogTransitionFailure {
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

/// An authenticated preflight which has not changed the Store or namespace.
/// The caller must continue holding its original full-Store external fence.
pub struct PreparedCatalogTransition {
    state: TransitionState,
}

#[derive(Clone, Copy)]
enum PreparationPurpose {
    Recovery,
    Maintenance,
}

struct ReadOnlyView {
    store: Option<Arc<Store>>,
    source: Option<Arc<StorageProvider>>,
    original: Option<File>,
}
impl ReadOnlyView {
    fn open(
        config: &CatalogTransitionConfig,
        original: File,
    ) -> Result<Self, (StorageError, File)> {
        let opened = (|| {
            let handles = Handles::open_original_read_only(
                config.store.clone(),
                &original,
                &config.binding,
                config.cursor_key,
            )
            .map_err(StorageError::from)?;
            let source = handles.storage;
            let history = Arc::new(handles.history);
            drop(handles.acquisition);
            let control = StoreReader::new(Storage::new(source.clone())?, history.clone());
            let factory: ReaderFactory =
                Arc::new(|_| Err(StorageError::Integrity("catalog preflight cannot mount")));
            let store = Arc::new(Store::new(
                source.clone(),
                history,
                control,
                factory,
                config.cache_bytes,
                config.reservations,
                config.workspace_capacity,
            )?);
            Ok((store, source))
        })();
        match opened {
            Ok((store, source)) => Ok(Self {
                store: Some(store),
                source: Some(source),
                original: Some(original),
            }),
            Err(error) => Err((error, original)),
        }
    }
    fn release(mut self) -> Result<File, Self> {
        if self
            .store
            .as_ref()
            .is_some_and(|store| Arc::strong_count(store) != 1)
        {
            return Err(self);
        }
        drop(self.store.take());
        if self
            .source
            .as_ref()
            .is_some_and(|source| Arc::strong_count(source) != 1)
        {
            return Err(self);
        }
        drop(self.source.take());
        Ok(self.original.take().expect("original Store file retained"))
    }
}
impl Drop for ReadOnlyView {
    fn drop(&mut self) {
        if self
            .store
            .as_ref()
            .is_some_and(|store| Arc::strong_count(store) != 1)
        {
            if let Some(store) = self.store.take() {
                std::mem::forget(store);
            }
            if let Some(source) = self.source.take() {
                std::mem::forget(source);
            }
            if let Some(original) = self.original.take() {
                std::mem::forget(original);
            }
            return;
        }
        drop(self.store.take());
        if self
            .source
            .as_ref()
            .is_some_and(|source| Arc::strong_count(source) != 1)
        {
            if let Some(source) = self.source.take() {
                std::mem::forget(source);
            }
            if let Some(original) = self.original.take() {
                std::mem::forget(original);
            }
            return;
        }
        drop(self.source.take());
        drop(self.original.take());
    }
}

struct TransitionState {
    config: CatalogTransitionConfig,
    owner: OwnerClient,
    view: Option<ReadOnlyView>,
    original: Option<File>,
    opened: Option<OpenedStore>,
    service: Option<Service>,
    store_root: Option<DurableRoot>,
    catalog_root: Option<DurableRoot>,
    overlay_root: Option<DurableRoot>,
    roster: Option<Roster>,
    records: Vec<WorkspaceRecord>,
    original_entries: VecDeque<(WorkspaceRecord, OverlayCatalogRecovery)>,
    legacy_entries: VecDeque<(WorkspaceRecord, PreparedLegacyRecovery)>,
    prepared_entries: VecDeque<(WorkspaceRecord, PreparedCatalogRecovery)>,
    completed: Vec<WorkspaceToken>,
    physical: Option<StorePhysicalIdentity>,
    original_identity: Option<(u64, u64, u32, u32, u64)>,
    incarnation: u64,
    migrate: bool,
    preserve_on_drop: bool,
}
impl TransitionState {
    fn failed(self, cause: CatalogTransitionCause) -> Box<CatalogTransitionFailure> {
        Box::new(CatalogTransitionFailure {
            cause: Some(cause),
            _state: Mutex::new(self),
        })
    }
    fn quarantine(&mut self) {
        for root in [
            &mut self.store_root,
            &mut self.catalog_root,
            &mut self.overlay_root,
        ]
        .into_iter()
        .flatten()
        {
            root.quarantine();
        }
        if let Some(roster) = &mut self.roster {
            roster.quarantine("durable directory or Store identity changed");
        }
    }
    fn check_roots(&mut self) -> Result<(), CatalogTransitionCause> {
        let checked = (|| {
            for root in [&self.store_root, &self.catalog_root, &self.overlay_root] {
                root.as_ref()
                    .ok_or(CatalogTransitionCause::Invalid("transition root absent"))?
                    .verify()
                    .map_err(CatalogTransitionCause::Directory)?;
            }
            Ok(())
        })();
        if matches!(checked, Err(CatalogTransitionCause::Directory(_))) {
            self.quarantine();
        }
        checked
    }
    fn check_named_store(&mut self) -> Result<(), CatalogTransitionCause> {
        let expected = self
            .original_identity
            .ok_or(CatalogTransitionCause::Invalid(
                "original Store identity absent",
            ))?;
        let named = match fs::symlink_metadata(&self.config.store.path) {
            Ok(value) => value,
            Err(cause) => {
                self.quarantine();
                return Err(CatalogTransitionCause::Directory(cause));
            }
        };
        if !named.is_file()
            || named.nlink() != 1
            || (
                named.dev(),
                named.ino(),
                named.uid(),
                named.mode(),
                named.nlink(),
            ) != expected
        {
            self.quarantine();
            return Err(CatalogTransitionCause::Invalid(
                "original Store name changed",
            ));
        }
        Ok(())
    }
    fn check_records(&mut self) -> Result<(), CatalogTransitionCause> {
        let mut actual = Vec::new();
        self.roster
            .as_mut()
            .ok_or(CatalogTransitionCause::Invalid("transition roster absent"))?
            .visit(|record| actual.push(record))
            .map_err(CatalogTransitionCause::Roster)?;
        if actual != self.records {
            return Err(CatalogTransitionCause::Invalid("catalog records changed"));
        }
        Ok(())
    }
    fn check_legacy_namespaces(&self, floor: i64) -> Result<(), CatalogTransitionCause> {
        let mut next = floor.max(self.config.minimum_namespace);
        for (_, plan) in &self.legacy_entries {
            let namespace = plan
                .previous_namespace()
                .checked_add(1)
                .ok_or(CatalogTransitionCause::Invalid(
                    "transition namespace exhausted",
                ))?
                .max(next);
            if namespace == i64::MAX {
                return Err(CatalogTransitionCause::Invalid(
                    "transition namespace exhausted",
                ));
            }
            next = namespace + 1;
        }
        Ok(())
    }
}
impl Drop for TransitionState {
    fn drop(&mut self) {
        if !self.preserve_on_drop {
            return;
        }
        std::mem::forget(self.owner.clone());
        if let Some(view) = self.view.take() {
            std::mem::forget(view);
        }
        if let Some(original) = self.original.take() {
            std::mem::forget(original);
        }
        if let Some(opened) = self.opened.take() {
            std::mem::forget(opened);
        }
        if let Some(service) = self.service.take() {
            std::mem::forget(service);
        }
        if let Some(root) = self.store_root.take() {
            std::mem::forget(root);
        }
        if let Some(root) = self.catalog_root.take() {
            std::mem::forget(root);
        }
        if let Some(root) = self.overlay_root.take() {
            std::mem::forget(root);
        }
        if let Some(roster) = self.roster.take() {
            std::mem::forget(roster);
        }
        std::mem::forget(std::mem::take(&mut self.original_entries));
        std::mem::forget(std::mem::take(&mut self.legacy_entries));
        std::mem::forget(std::mem::take(&mut self.prepared_entries));
    }
}

impl PreparedCatalogTransition {
    /// Preflight under the actual original File, private directories and one
    /// Roster. Before calling, the trusted owner authenticates that File and
    /// fences every old Store writer, reader, mount and consumer. That fence
    /// must remain held through execute; it is not inferred from these checks.
    pub fn prepare(
        config: CatalogTransitionConfig,
        original: File,
        owner: &Owner,
    ) -> Result<Self, Box<CatalogTransitionFailure>> {
        Self::prepare_for(config, original, owner, PreparationPurpose::Recovery)
    }

    fn prepare_for(
        config: CatalogTransitionConfig,
        original: File,
        owner: &Owner,
        purpose: PreparationPurpose,
    ) -> Result<Self, Box<CatalogTransitionFailure>> {
        let mut state = TransitionState {
            config,
            owner: owner.client(),
            view: None,
            original: Some(original),
            opened: None,
            service: None,
            store_root: None,
            catalog_root: None,
            overlay_root: None,
            roster: None,
            records: Vec::new(),
            original_entries: VecDeque::new(),
            legacy_entries: VecDeque::new(),
            prepared_entries: VecDeque::new(),
            completed: Vec::new(),
            physical: None,
            original_identity: None,
            incarnation: 0,
            migrate: false,
            preserve_on_drop: false,
        };
        let result = (|| -> Result<(), CatalogTransitionCause> {
            if state.config.workspace_capacity == 0
                || state.config.workspace_capacity > owner.configuration().namespaces
                || state.config.minimum_namespace <= 0
                || state.config.minimum_namespace == i64::MAX
                || state.config.profile.pager_kib == 0
                || state.config.profile.max_pages.is_some()
            {
                return Err(CatalogTransitionCause::Invalid(
                    "catalog transition admission",
                ));
            }
            let floor = owner
                .vacant_namespace_floor()
                .map_err(CatalogTransitionCause::Owner)?;
            let original = state.original.as_ref().expect("original Store file");
            let held = original
                .metadata()
                .map_err(CatalogTransitionCause::Directory)?;
            let named = fs::symlink_metadata(&state.config.store.path)
                .map_err(CatalogTransitionCause::Directory)?;
            let uid = nix::unistd::geteuid().as_raw();
            if !held.is_file()
                || !named.is_file()
                || held.nlink() != 1
                || named.nlink() != 1
                || held.uid() != uid
                || named.uid() != uid
                || held.mode() & 0o077 != 0
                || (
                    held.dev(),
                    held.ino(),
                    held.uid(),
                    held.mode(),
                    held.nlink(),
                ) != (
                    named.dev(),
                    named.ino(),
                    named.uid(),
                    named.mode(),
                    named.nlink(),
                )
            {
                return Err(CatalogTransitionCause::Invalid(
                    "original Store file identity",
                ));
            }
            state.original_identity = Some((
                held.dev(),
                held.ino(),
                held.uid(),
                held.mode(),
                held.nlink(),
            ));
            let parent = state
                .config
                .store
                .path
                .parent()
                .ok_or(CatalogTransitionCause::Invalid("Store parent absent"))?;
            state.store_root =
                Some(DurableRoot::open(parent).map_err(CatalogTransitionCause::Directory)?);
            state.catalog_root = Some(
                DurableRoot::open(&state.config.catalog_directory)
                    .map_err(CatalogTransitionCause::Directory)?,
            );
            state.overlay_root = Some(
                DurableRoot::open(&state.config.overlay_directory)
                    .map_err(CatalogTransitionCause::Directory)?,
            );
            let store_root = state.store_root.as_ref().unwrap();
            let catalog_root = state.catalog_root.as_ref().unwrap();
            let overlay_root = state.overlay_root.as_ref().unwrap();
            if catalog_root.path.starts_with(&overlay_root.path)
                || overlay_root.path.starts_with(&catalog_root.path)
                || store_root
                    .device()
                    .map_err(CatalogTransitionCause::Directory)?
                    != held.dev()
                || catalog_root
                    .device()
                    .map_err(CatalogTransitionCause::Directory)?
                    != held.dev()
                || overlay_root
                    .device()
                    .map_err(CatalogTransitionCause::Directory)?
                    != held.dev()
                || store_root.mount_id() != catalog_root.mount_id()
                || store_root.mount_id() != overlay_root.mount_id()
                || catalog_root
                    .inode()
                    .map_err(CatalogTransitionCause::Directory)?
                    == overlay_root
                        .inode()
                        .map_err(CatalogTransitionCause::Directory)?
            {
                return Err(CatalogTransitionCause::Invalid(
                    "catalog transition directories",
                ));
            }
            state.check_roots()?;
            let original = state.original.take().expect("original Store file");
            state.view = Some(match ReadOnlyView::open(&state.config, original) {
                Ok(value) => value,
                Err((error, original)) => {
                    state.original = Some(original);
                    return Err(CatalogTransitionCause::Store(error));
                }
            });
            let view = state.view.as_ref().unwrap();
            let store = view.store.as_ref().unwrap();
            let source = view.source.as_ref().unwrap();
            state.physical = Some(
                source
                    .observe_original_store(view.original.as_ref().unwrap())
                    .map_err(CatalogTransitionCause::Persistence)?,
            );
            state.incarnation = store.history().incarnation();
            let receipts = match store
                .history()
                .checkpoint_receipt(layerfs_history::CheckpointAttemptId::from_bytes([0; 32]))
            {
                Ok(_) => true,
                Err(layerfs_history::HistoryError::Unsupported("checkpoint receipt")) => false,
                Err(cause) => return Err(CatalogTransitionCause::History(cause)),
            };
            let authority = Authority::for_catalog(state.config.sandbox, store.history())
                .map_err(CatalogTransitionCause::Roster)?;
            let catalog_root = state.catalog_root.as_ref().unwrap();
            state.roster = Some(
                Roster::open_opened(
                    catalog_root.path.clone(),
                    catalog_root
                        .duplicate()
                        .map_err(CatalogTransitionCause::Directory)?,
                    authority,
                )
                .map_err(CatalogTransitionCause::Roster)?,
            );
            let roster = state.roster.as_mut().unwrap();
            roster
                .visit(|record| state.records.push(record))
                .map_err(CatalogTransitionCause::Roster)?;
            if state.records.len() > state.config.workspace_capacity {
                return Err(CatalogTransitionCause::Invalid(
                    "catalog transition capacity",
                ));
            }
            let root = state.overlay_root.as_ref().unwrap().retained_path();
            for record in &state.records {
                let pending = OverlayCatalogRecovery::open(
                    &root.join(record.overlay_name()),
                    state.config.profile,
                    record.workspace.to_bytes(),
                )
                .map_err(|cause| CatalogTransitionCause::Overlay {
                    record: *record,
                    cause,
                })?;
                state.original_entries.push_back((*record, pending));
            }
            // Only an entire supported, ready group is migrated. Other healthy
            // formats keep the ordinary recovery path without a format rewrite.
            let mut legacy = false;
            let mut ordinary = false;
            let mut recorded = false;
            for (_, entry) in &state.original_entries {
                match entry {
                    OverlayCatalogRecovery::Ready(ready) => match ready.schema_version() {
                        41 | 42 => legacy = true,
                        45 => recorded = true,
                        43 | 44 => {
                            ordinary = true;
                            recorded = true;
                        }
                        _ => {
                            return Err(CatalogTransitionCause::Invalid(
                                "unsupported recorded schema",
                            ))
                        }
                    },
                    _ => {
                        ordinary = true;
                        recorded = true;
                    }
                }
            }
            state.migrate = match purpose {
                PreparationPurpose::Recovery => {
                    (legacy && !ordinary && (!recorded || receipts))
                        || (state.records.is_empty() && !receipts)
                }
                PreparationPurpose::Maintenance => {
                    if ordinary {
                        return Err(CatalogTransitionCause::Invalid(
                            "catalog maintenance requires ready supported formats",
                        ));
                    }
                    true
                }
            };
            if state.migrate {
                while let Some((record, pending)) = state.original_entries.pop_front() {
                    let request = BindRequest {
                        workspace: record.workspace,
                        branch: record.branch,
                    };
                    let prepared = store
                        .prepare_opened_catalog_recovery(pending, request)
                        .map_err(|failure| CatalogTransitionCause::Preparation {
                            record,
                            failure,
                        })?;
                    let plan = prepared
                        .into_legacy(source, view.original.as_ref().unwrap())
                        .map_err(|failure| CatalogTransitionCause::Migration { record, failure })?;
                    state.legacy_entries.push_back((record, plan));
                }
                state.check_legacy_namespaces(floor)?;
            }
            state.check_records()?;
            state.check_roots()?;
            let view = state.view.take().unwrap();
            state.original = Some(match view.release() {
                Ok(original) => original,
                Err(view) => {
                    state.view = Some(view);
                    return Err(CatalogTransitionCause::Invalid(
                        "old Store readers remain live",
                    ));
                }
            });
            state.check_named_store()?;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(Self { state }),
            Err(cause) => Err(state.failed(cause)),
        }
    }

    /// The exact roster records authenticated during preflight.
    pub fn records(&self) -> &[WorkspaceRecord] {
        &self.state.records
    }
}

/// Fully recovered control service and the same newly opened Store.
pub struct CatalogTransitionSuccess {
    pub opened: OpenedStore,
    pub service: Service,
    pub tokens: Vec<WorkspaceToken>,
}

impl PreparedCatalogTransition {
    /// Performs the selected original transition once, then admits its already
    /// leased Workspaces. The external fence must remain held until this returns.
    /// On failure the caller retains the original Owner alongside the failure.
    pub fn execute(
        mut self,
        owner: &Owner,
        kernel: Option<Arc<KernelServing>>,
        serial_low_water: u64,
    ) -> Result<CatalogTransitionSuccess, Box<CatalogTransitionFailure>> {
        let result = (|| -> Result<(), CatalogTransitionCause> {
            if !self.state.owner.same_roster(&owner.client()) {
                return Err(CatalogTransitionCause::Invalid("transition owner changed"));
            }
            let floor = owner
                .vacant_namespace_floor()
                .map_err(CatalogTransitionCause::Owner)?;
            if self.state.migrate {
                self.state.check_legacy_namespaces(floor)?;
            }
            self.state.check_roots()?;
            self.state.check_named_store()?;
            self.state.check_records()?;
            let original = self
                .state
                .original
                .take()
                .expect("original Store file retained");
            if self.state.migrate {
                self.state.preserve_on_drop = true;
                let prepared = match PreparedCheckpointUpgrade::prepare(
                    self.state.config.store.clone(),
                    original,
                    &self.state.config.binding,
                    self.state.incarnation,
                ) {
                    Ok(value) => value,
                    Err(failure) => {
                        if *failure.cause() != layerfs_storage::port::PersistenceError::Uncertain {
                            self.state.preserve_on_drop = false;
                        }
                        return Err(CatalogTransitionCause::StoreUpgrade(failure));
                    }
                };
                let outcome = match prepared.apply() {
                    Ok(value) => value,
                    Err(failure) => {
                        if *failure.cause() != layerfs_storage::port::PersistenceError::Uncertain {
                            self.state.preserve_on_drop = false;
                        }
                        return Err(CatalogTransitionCause::StoreUpgrade(failure));
                    }
                };
                drop(outcome.original);
            } else {
                drop(original);
            }
            // No ordinary File referring to the Store is closed after this point
            // while the new SQLite provider remains open.
            self.state.check_roots()?;
            self.state.check_named_store()?;
            self.state.preserve_on_drop = true;
            self.state.opened = Some(
                open_store_observed(
                    self.state.config.store.clone(),
                    &self.state.config.binding,
                    self.state.config.cursor_key,
                    self.state.config.workspace_capacity,
                    self.state.config.cache_bytes,
                    self.state.config.reservations,
                )
                .map_err(CatalogTransitionCause::Store)?,
            );
            let opened = self.state.opened.as_ref().unwrap();
            if let Err(cause) = opened
                .store
                .verify_original_physical(self.state.physical.as_ref().unwrap())
            {
                self.state.quarantine();
                return Err(CatalogTransitionCause::Persistence(cause));
            }
            let authority =
                Authority::for_catalog(self.state.config.sandbox, opened.store.history())
                    .map_err(CatalogTransitionCause::Roster)?;
            if authority != self.state.roster.as_ref().unwrap().authority()
                || opened.store.history().incarnation() != self.state.incarnation
            {
                return Err(CatalogTransitionCause::Invalid(
                    "reopened Store authority changed",
                ));
            }
            if self.state.migrate {
                while !self.state.legacy_entries.is_empty() {
                    self.state.check_roots()?;
                    self.state
                        .roster
                        .as_mut()
                        .unwrap()
                        .check_live()
                        .map_err(CatalogTransitionCause::Roster)?;
                    let (record, plan) = self.state.legacy_entries.pop_front().unwrap();
                    let prepared = self
                        .state
                        .opened
                        .as_ref()
                        .unwrap()
                        .store
                        .prepare_migrated_catalog_recovery(plan)
                        .map_err(|failure| CatalogTransitionCause::Migration { record, failure })?;
                    self.state.prepared_entries.push_back((record, prepared));
                }
            } else {
                while !self.state.original_entries.is_empty() {
                    self.state.check_roots()?;
                    self.state
                        .roster
                        .as_mut()
                        .unwrap()
                        .check_live()
                        .map_err(CatalogTransitionCause::Roster)?;
                    let (record, pending) = self.state.original_entries.pop_front().unwrap();
                    let request = BindRequest {
                        workspace: record.workspace,
                        branch: record.branch,
                    };
                    let prepared = self
                        .state
                        .opened
                        .as_ref()
                        .unwrap()
                        .store
                        .prepare_opened_catalog_recovery(pending, request)
                        .map_err(|failure| CatalogTransitionCause::Preparation {
                            record,
                            failure,
                        })?;
                    self.state.prepared_entries.push_back((record, prepared));
                }
            }
            self.state.check_records()?;
            self.state.check_roots()?;
            let store = self.state.opened.as_ref().unwrap().store.clone();
            store.set_serial_low_water(serial_low_water);
            self.state.service = Some(match kernel {
                Some(kernel) => Service::with_kernel(store, owner, kernel),
                None => Service::new(store, owner),
            });
            let mut next = self.state.config.minimum_namespace.max(floor);
            while !self.state.prepared_entries.is_empty() {
                self.state.check_roots()?;
                let (record, prepared) = self.state.prepared_entries.pop_front().unwrap();
                next = self
                    .state
                    .service
                    .as_ref()
                    .unwrap()
                    .admit_catalog_prepared(
                        self.state.roster.as_mut().unwrap(),
                        owner,
                        record,
                        prepared,
                        next,
                        &mut self.state.completed,
                    )
                    .map_err(CatalogTransitionCause::Recovery)?;
            }
            self.state.check_roots()?;
            self.state
                .roster
                .as_mut()
                .unwrap()
                .check_live()
                .map_err(CatalogTransitionCause::Roster)?;
            self.state.service.as_mut().unwrap().install_fresh_control(
                self.state.roster.take().unwrap(),
                self.state.overlay_root.take().unwrap(),
                self.state.catalog_root.take().unwrap(),
                self.state.config.sandbox,
                self.state.config.profile,
            );
            Ok(())
        })();
        if let Err(cause) = result {
            return Err(self.state.failed(cause));
        }
        self.state.preserve_on_drop = false;
        Ok(CatalogTransitionSuccess {
            opened: self.state.opened.take().unwrap(),
            service: self.state.service.take().unwrap(),
            tokens: std::mem::take(&mut self.state.completed),
        })
    }
}
