//! Host Init and original handoff/failure custody.
use crate as layerfs_project;
use layerfs_bridge::provision::{check_destination, ProviderKind, StoreManifest, StoreProfile};
use layerfs_history::{
    BranchId, BranchSnapshot, ForkRequest, ForkSource, HistoryCatalog, HistoryCatalogConfig,
    HistoryError, HistoryName, InitStatistics, LayerStackId,
};
use layerfs_persistence::{
    Handles, PersistenceConfig, SealedStore, SqliteAcquisitionSchema, SqlitePersistenceProfile,
};
use layerfs_project::{InitCounters, InitStep, Initialized, ProjectError};
use layerfs_storage::{port::PersistenceError, Storage, StorageError, StoragePolicy};
use layerfs_telemetry::timer::{Active, TimingScope};
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

/// One host Project Init. The Store is created once with acquisition tables.
#[derive(Debug)]
pub struct InitRequest {
    /// Complete source directory, with no Git-ignore filtering.
    pub source: PathBuf,
    /// New host output, explicit profile and pack layout; must not already exist.
    pub store: PersistenceConfig,
    /// Store placement inside the daemon's filesystem after installation.
    pub locator: String,
    /// Frozen content/storage policy for this Store.
    pub policy: StoragePolicy,
    /// Original catalog authority and cursor key.
    pub catalog: HistoryCatalogConfig,
    /// New LayerStack identity.
    pub stack: LayerStackId,
    /// Checked LayerStack name.
    pub stack_name: HistoryName,
    /// New first Branch identity.
    pub branch: BranchId,
    /// Checked first Branch name.
    pub branch_name: HistoryName,
    /// Filesystem inode allocation scope seed.
    pub scope_seed: [u8; 32],
    /// Explicit Init deadline; it does not apply to future Exec commands.
    pub deadline: Instant,
}
/// Closed host output. It owns no database session, Reader, Save or service.
#[derive(Debug)]
pub struct SealedProject {
    /// One checked closed file ready for bounded streaming.
    pub store: SealedStore,
    /// Installation metadata; the daemon version is absent until installation.
    pub manifest: StoreManifest,
    /// Original acknowledged import and its count diagnostics.
    pub initialized: Initialized,
    /// Original acknowledged first Branch.
    pub branch: BranchSnapshot,
}
/// Original failure, without cleanup, retry or an inferred history result.
#[derive(Debug)]
pub struct InitFailure {
    /// Exact inputs needed to identify retained output and authority.
    pub request: InitRequest,
    /// Deciding failure in its original typed carrier.
    pub error: InitError,
    /// Original acknowledged Init, if it completed before the failure.
    pub initialized: Option<Initialized>,
    /// Original acknowledged fork, if it completed before seal failed.
    pub branch: Option<BranchSnapshot>,
}
/// One Git-substituted Init failure, retaining both original inputs.
#[derive(Debug)]
pub struct GitInitFailure {
    /// The original Init failure, request and known outcomes.
    pub original: Box<InitFailure>,
    /// The separately authorized Git metadata directory used in the attempt.
    pub git_directory: PathBuf,
}
impl fmt::Display for GitInitFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Project Git Init: {}", self.original)
    }
}
impl std::error::Error for GitInitFailure {}
/// The original failing boundary of one Init-and-seal operation.
#[derive(Debug)]
pub enum InitError {
    /// Before-effect provisioning metadata refusal.
    Manifest(&'static str),
    /// Store creation or final seal.
    Persistence(PersistenceError),
    /// Storage construction.
    Storage(StorageError),
    /// Complete host import, including original acquisition custody.
    Project(ProjectError),
    /// First Branch publication.
    History(HistoryError),
}
impl fmt::Display for InitFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Project Init: {:?}", self.error)
    }
}
impl std::error::Error for InitFailure {}

/// Builds and seals one complete Store. It never opens that Store again and
/// returns no data-serving runtime. On failure, output and original known
/// outcomes remain owned by the caller; no file deletion or replay occurs.
pub(crate) fn initialize(
    request: InitRequest,
    timer: &TimingScope<'_, Active>,
    counters: Arc<InitCounters>,
    git_directory: Option<&Path>,
) -> Result<SealedProject, Box<InitFailure>> {
    let started = Instant::now();
    let mut initialized = None;
    let mut branch = None;
    let result = (|| {
        check_destination(
            &request.locator,
            &request.catalog.binding_key,
            request.catalog.cursor_key,
        )
        .map_err(InitError::Manifest)?;
        if let Some(git) = git_directory {
            validate_git_inputs(&request, git).map_err(InitError::Project)?;
        }
        let config = request
            .store
            .clone()
            .with_sqlite_acquisition(SqliteAcquisitionSchema::Tables);
        let handles = Handles::create(config, request.policy, &request.catalog)
            .map_err(InitError::Persistence)?;
        let storage = Storage::new(handles.storage.clone()).map_err(InitError::Storage)?;
        let imported = layerfs_project::init::init_observed_using(
            &storage,
            &handles.history,
            layerfs_project::InitRequest {
                source: &request.source,
                acquisition: &handles.acquisition,
                stack: request.stack,
                name: request.stack_name.clone(),
                scope_seed: request.scope_seed,
                deadline: request.deadline,
            },
            timer,
            Arc::clone(&counters),
            git_directory,
        )
        .map_err(InitError::Project)?;
        counters.step(InitStep::Seal);
        let source = ForkSource::Layer(imported.stack.head_layer);
        initialized = Some(imported);
        branch = Some(
            handles
                .history
                .fork(&ForkRequest {
                    stack: request.stack,
                    branch: request.branch,
                    name: request.branch_name.clone(),
                    source,
                })
                .map_err(InitError::History)?,
        );
        let totals = handles.history.statistics().map_err(InitError::History)?;
        let counts = counters.read();
        handles
            .history
            .record_init(&InitStatistics {
                entries: initialized.as_ref().expect("acknowledged Init").entries,
                files: counts.files,
                bytes: counts.bytes,
                canonical: totals.canonical,
                stored: totals.stored,
                metadata: totals.metadata,
                millis: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            })
            .map_err(InitError::History)?;
        drop(storage);
        let sealed = handles.seal().map_err(InitError::Persistence)?;
        Ok(sealed)
    })();
    match result {
        Ok(store) => {
            let manifest = StoreManifest {
                provider: ProviderKind::Sqlite,
                locator: request.locator.clone(),
                profile: match request.store.sqlite_profile {
                    SqlitePersistenceProfile::Durable => StoreProfile::Durable,
                    SqlitePersistenceProfile::Disposable => StoreProfile::Disposable,
                },
                binding: request.catalog.binding_key.clone(),
                cursor_key: request.catalog.cursor_key,
                stack: request.stack.to_bytes(),
                branch: request.branch.to_bytes(),
                root: initialized
                    .as_ref()
                    .expect("acknowledged Init")
                    .root
                    .to_bytes(),
                bytes: store.bytes,
                host_sqlite: store.sqlite_version.clone(),
                daemon_sqlite: None,
            };
            Ok(SealedProject {
                store,
                manifest,
                initialized: initialized.expect("acknowledged Init"),
                branch: branch.expect("acknowledged Branch"),
            })
        }
        Err(error) => Err(Box::new(InitFailure {
            request,
            error,
            initialized,
            branch,
        })),
    }
}

#[cfg(unix)]
fn validate_git_inputs(request: &InitRequest, git: &Path) -> Result<(), ProjectError> {
    let source = layerfs_project::scan::check_root(&request.source)?;
    let original = std::fs::symlink_metadata(request.source.join(".git"))?;
    if !original.is_file() && !original.is_dir() {
        return Err(ProjectError::refused(
            layerfs_project::SourceRefusal::Changed,
            request.source.join(".git"),
        ));
    }
    let replacement = layerfs_project::scan::check_root(git)?;
    layerfs_project::import::source::disjoint_directories(
        &source,
        &request.source,
        &replacement,
        git,
    )?;
    let parent = request
        .store
        .path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    layerfs_project::import::source::outside_source(&source, parent)?;
    layerfs_project::import::source::outside_source(&replacement, parent)?;
    Ok(())
}

#[cfg(not(unix))]
fn validate_git_inputs(_request: &InitRequest, _git: &Path) -> Result<(), ProjectError> {
    Err(ProjectError::Unsupported)
}

/// Host Project Init/seal/install and authenticated fork/history organization.
/// The facade retains no Store handle. Each method delegates one operation.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProjectApi;
impl ProjectApi {
    /// Creates the stateless public Project facade.
    pub fn new() -> Self {
        Self
    }
    /// Imports the complete supported host tree, publishes genesis/Branch and seals.
    /// The request selects the Store profile before creation.
    pub fn init(
        &self,
        request: InitRequest,
        timer: &TimingScope<'_, Active>,
    ) -> Result<SealedProject, Box<InitFailure>> {
        initialize(request, timer, Arc::new(InitCounters::new()), None)
    }
    /// As [`ProjectApi::init`], updating `counters` while it runs so another
    /// thread can show how far it has come; the last step, Seal, covers the
    /// first Branch and sealing.
    pub fn init_observed(
        &self,
        request: InitRequest,
        timer: &TimingScope<'_, Active>,
        counters: Arc<InitCounters>,
    ) -> Result<SealedProject, Box<InitFailure>> {
        initialize(request, timer, counters, None)
    }

    /// Imports the complete source tree while replacing its root `.git` entry
    /// with one separately authorized, complete Git metadata directory.
    ///
    /// The caller must authorize both sources, fence their writers, ensure
    /// they remain disjoint through all filesystem aliases, validate the Git
    /// metadata and retain the exact failure. This performs no Git validation,
    /// workspace recovery or permission grant. All other entries, including
    /// ignored files, are imported by the ordinary complete-tree scanner.
    pub fn init_with_git_directory(
        &self,
        request: InitRequest,
        git_directory: PathBuf,
        timer: &TimingScope<'_, Active>,
    ) -> Result<SealedProject, Box<GitInitFailure>> {
        initialize(
            request,
            timer,
            Arc::new(InitCounters::new()),
            Some(&git_directory),
        )
        .map_err(|original| {
            Box::new(GitInitFailure {
                original,
                git_directory,
            })
        })
    }
}
