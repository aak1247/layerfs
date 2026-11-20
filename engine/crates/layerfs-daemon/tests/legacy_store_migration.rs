//! Actual legacy Store/Overlay bytes remain usable across a coordinated upgrade.
#![cfg(target_os = "linux")]

use layerfs_bridge::control::{CommitControl, Reply, Request};
use layerfs_content::filesystem::PathName;
use layerfs_daemon::{
    bootstrap::open_store,
    control::{
        CatalogTransitionConfig, CatalogTransitionSuccess, PreparedCatalogTransition, Service,
    },
    durable_roster::{Authority, Roster, WorkspaceRecord},
    store::{
        BindRequest, BoundWorkspace, CommitOptions, ReaderFactory, Store, StoreOperation,
        StoreReader,
    },
    Owner, OwnerConfig,
};
use layerfs_history::CheckpointAttemptId;
use layerfs_overlay::ProfileConfig;
use layerfs_persistence::{
    Handles, PersistenceConfig, PreparedCheckpointUpgrade, SqliteAcquisitionSchema,
    SqlitePersistenceProfile,
};
use layerfs_storage::{ReservationBlocks, Storage, StorageError};
use layerfs_workspace::{Operation, Outcome, Position, Time};
use rusqlite::{Connection, OpenFlags};
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

const BINDING: &[u8] = b"daemon-installed-store";
const CURSOR: [u8; 32] = [43; 32];

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        Self::named("legacy_277")
    }
    fn named(name: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "layerfs-legacy-catalog-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        copy(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures")
                .join(name)
                .as_path(),
            &dir,
        );
        Self(dir)
    }
    fn config(&self) -> PersistenceConfig {
        PersistenceConfig::sqlite(&self.0.join("store-private/store.sqlite"))
            .with_sqlite_profile(SqlitePersistenceProfile::Durable)
            .with_sqlite_acquisition(SqliteAcquisitionSchema::Tables)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_LEGACY_CATALOG_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
fn copy(source: &Path, target: &Path) {
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let path = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            fs::create_dir(&path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            copy(&entry.path(), &path);
        } else {
            fs::copy(entry.path(), &path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
}
fn readonly_store(
    config: PersistenceConfig,
    original: &fs::File,
) -> (Arc<Store>, Arc<layerfs_persistence::StorageProvider>) {
    let handles = Handles::open_original_read_only(config, original, BINDING, CURSOR).unwrap();
    let storage = handles.storage;
    let history = Arc::new(handles.history);
    let control = StoreReader::new(Storage::new(storage.clone()).unwrap(), history.clone());
    let factory: ReaderFactory =
        Arc::new(|_| Err(StorageError::Integrity("migration verifier cannot mount")));
    let source = storage.clone();
    (
        Arc::new(
            Store::new(
                storage,
                history,
                control,
                factory,
                2 * 1024 * 1024,
                ReservationBlocks::default(),
                2,
            )
            .unwrap(),
        ),
        source,
    )
}
fn read(workspace: &BoundWorkspace, name: &str) -> Vec<u8> {
    let operation = workspace.operation().unwrap();
    read_operation(&operation, name)
}
fn read_operation(operation: &StoreOperation, name: &str) -> Vec<u8> {
    let route = operation.workspace().route();
    let source = operation.overlay().acquire_base_source(route, 7).unwrap();
    let view = operation.workspace().view_for_source(source).unwrap();
    let file = view
        .lookup(
            operation.overlay(),
            view.root_serial(),
            &PathName::new(name).unwrap(),
        )
        .unwrap();
    let mut bytes = Vec::new();
    view.read(operation.overlay(), file.serial, 0, 1024, &mut bytes)
        .unwrap();
    drop(view);
    operation
        .overlay()
        .release_base_source(route, source)
        .unwrap();
    bytes
}
fn write(workspace: &BoundWorkspace, name: &str, bytes: &[u8]) {
    let operation = workspace.operation().unwrap();
    write_operation(&operation, name, bytes);
}
fn write_operation(operation: &StoreOperation, name: &str, bytes: &[u8]) {
    let route = operation.workspace().route();
    let source = operation.overlay().acquire_base_source(route, 7).unwrap();
    let view = operation.workspace().view_for_source(source).unwrap();
    let file = view
        .lookup(
            operation.overlay(),
            view.root_serial(),
            &PathName::new(name).unwrap(),
        )
        .unwrap();
    let outcome = operation
        .workspace()
        .mutate(
            operation.overlay(),
            operation.ports(),
            &view,
            Operation::Write {
                serial: file.serial,
                position: Position::At(0),
                data: bytes.into(),
            },
            Time {
                seconds: file.metadata.mtime_seconds,
                nanoseconds: file.metadata.mtime_nanoseconds,
            },
        )
        .unwrap();
    let Outcome::Applied { publication, .. } = outcome else {
        panic!("write was not applied")
    };
    operation.overlay().reply_attempted(publication).unwrap();
    drop(view);
    operation
        .overlay()
        .release_base_source(route, source)
        .unwrap();
}
fn open_current(config: PersistenceConfig) -> Arc<Store> {
    open_store(
        config,
        BINDING,
        CURSOR,
        2,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap()
}

fn assert_roster(roster: &mut Roster, expected: WorkspaceRecord) {
    let mut records = Vec::new();
    roster.visit(|record| records.push(record)).unwrap();
    assert_eq!(records, [expected]);
}

#[test]
fn original_readonly_session_rejects_a_replaced_store_name() {
    let fixture = Fixture::new();
    let config = fixture.config();
    let bytes = fs::read(&config.path).unwrap();
    let original = fs::File::open(&config.path).unwrap();
    let ordinary = Handles::open_read_only(config.clone(), BINDING, CURSOR).unwrap();
    assert!(matches!(
        ordinary.storage.observe_original_store(&original),
        Err(layerfs_storage::port::PersistenceError::Malformed)
    ));
    drop(ordinary);
    let handles =
        Handles::open_original_read_only(config.clone(), &original, BINDING, CURSOR).unwrap();
    assert!(handles.storage.observe_original_store(&original).is_ok());
    let moved = config.path.with_extension("original");
    fs::rename(&config.path, &moved).unwrap();
    fs::write(&config.path, &bytes).unwrap();
    fs::set_permissions(&config.path, fs::Permissions::from_mode(0o600)).unwrap();
    let replacement = fs::File::open(&config.path).unwrap();
    assert!(matches!(
        handles.storage.observe_original_store(&replacement),
        Err(layerfs_storage::port::PersistenceError::Malformed)
    ));
    drop(replacement);
    drop(handles);
    drop(original);
    assert_eq!(fs::read(moved).unwrap(), bytes);
}

#[test]
fn equal_catalog_in_another_store_cannot_upgrade_the_original_overlay() {
    let fixture = Fixture::new();
    let config = fixture.config();
    let original = fs::File::open(&config.path).unwrap();
    let (old, source) = readonly_store(config.clone(), &original);
    let mut roster = Roster::open(
        &fixture.0.join("catalog"),
        Authority::for_catalog([8; 32], old.history()).unwrap(),
    )
    .unwrap();
    let mut records = Vec::new();
    roster.visit(|record| records.push(record)).unwrap();
    assert_eq!(records.len(), 1);
    let record = records[0];
    let request = BindRequest {
        workspace: record.workspace,
        branch: record.branch,
    };
    let path = fixture.0.join("overlays").join(record.overlay_name());
    let original_overlay = fs::read(&path).unwrap();
    let predecessor = old
        .prepare_catalog_recovery(&path, ProfileConfig::default(), request)
        .unwrap()
        .into_legacy(&source, &original)
        .unwrap();
    assert_roster(&mut roster, record);
    drop(old);
    drop(source);
    drop(original);

    let alternate_dir = fixture.0.join("alternate-store");
    fs::create_dir(&alternate_dir).unwrap();
    fs::set_permissions(&alternate_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let alternate_path = alternate_dir.join("store.sqlite");
    fs::copy(&config.path, &alternate_path).unwrap();
    fs::set_permissions(&alternate_path, fs::Permissions::from_mode(0o600)).unwrap();
    let alternate_config = PersistenceConfig {
        path: alternate_path.clone(),
        ..config
    };
    let alternate = fs::File::open(&alternate_path).unwrap();
    let result =
        PreparedCheckpointUpgrade::prepare(alternate_config.clone(), alternate, BINDING, 1)
            .unwrap()
            .apply()
            .unwrap();
    assert!(result.applied);
    drop(result.original);
    let store = open_current(alternate_config);
    let failure = store
        .prepare_migrated_catalog_recovery(predecessor)
        .err()
        .unwrap();
    assert!(matches!(
        failure.as_ref(),
        layerfs_daemon::store::CatalogMigrationFailure::Physical {
            cause: layerfs_storage::port::PersistenceError::Malformed,
            ..
        }
    ));
    drop(failure);
    drop(roster);
    drop(store);
    assert_eq!(fs::read(path).unwrap(), original_overlay);
}

#[test]
fn legacy_277_store_and_workspace_upgrade_then_reopen_and_commit() {
    let fixture = Fixture::new();
    let config = fixture.config();
    let original = fs::File::open(&config.path).unwrap();
    let (old, source) = readonly_store(config.clone(), &original);
    let authority = Authority::for_catalog([8; 32], old.history()).unwrap();
    let mut roster = Roster::open(&fixture.0.join("catalog"), authority).unwrap();
    let mut records = Vec::new();
    roster.visit(|record| records.push(record)).unwrap();
    assert_eq!(records.len(), 1);
    let record = records[0];
    let request = BindRequest {
        workspace: record.workspace,
        branch: record.branch,
    };
    let predecessor = old
        .prepare_catalog_recovery(
            &fixture.0.join("overlays").join(record.overlay_name()),
            ProfileConfig::default(),
            request.clone(),
        )
        .unwrap()
        .into_legacy(&source, &original)
        .unwrap();
    assert_roster(&mut roster, record);
    assert_eq!(Arc::strong_count(&old), 1);
    drop(old);
    drop(source);
    let upgrade = PreparedCheckpointUpgrade::prepare(config.clone(), original, BINDING, 1)
        .unwrap()
        .apply()
        .unwrap();
    assert!(upgrade.applied);
    drop(upgrade.original);
    let store = open_current(config.clone());
    assert_roster(&mut roster, record);
    let prepared = store
        .prepare_migrated_catalog_recovery(predecessor)
        .unwrap();
    assert_roster(&mut roster, record);
    let outer = Owner::start(
        &fixture.0.join("template-new-1.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let (owner, proof) = prepared.activate(2).unwrap();
    let client = outer.adopt_durable(owner).unwrap();
    let workspace = store
        .bind_catalog_recovery(client, request.clone(), proof)
        .unwrap()
        .workspace;
    assert_eq!(workspace.route().namespace(), 2);
    assert_eq!(
        read(&workspace, "file-000000"),
        b"Bomplete installed file 000000\n"
    );
    assert_eq!(
        read(&workspace, "file-000001"),
        b"Domplete installed file 000001\n"
    );
    assert!(workspace
        .commit_captured(&CommitOptions::default())
        .unwrap()
        .history
        .is_some());
    let first = workspace.snapshot().unwrap();
    let service = Service::new(store.clone(), &outer);
    let first_token = service.register_recorded(&mut roster, workspace).unwrap();
    assert_eq!(first_token.namespace, 2);
    drop(service);
    store.close_reader(record.workspace).unwrap();
    outer.stop().unwrap();
    drop(roster);
    drop(store);

    let raw = Connection::open_with_flags(&config.path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let (attempt, digest): (Vec<u8>, Vec<u8>) = raw
        .query_row(
            "SELECT attempt_id,request_digest FROM checkpoint_receipt",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        raw.query_row("SELECT count(*) FROM checkpoint_receipt", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    drop(raw);
    let attempt = CheckpointAttemptId::from_bytes(attempt.try_into().unwrap());
    let original = fs::File::open(&config.path).unwrap();
    let (readonly, source) = readonly_store(config.clone(), &original);
    let mut roster = Roster::open(
        &fixture.0.join("catalog"),
        Authority::for_catalog([8; 32], readonly.history()).unwrap(),
    )
    .unwrap();
    let already_migrated = readonly
        .prepare_catalog_recovery(
            &fixture.0.join("overlays").join(record.overlay_name()),
            ProfileConfig::default(),
            request.clone(),
        )
        .unwrap()
        .into_legacy(&source, &original)
        .unwrap();
    assert_roster(&mut roster, record);
    drop(readonly);
    drop(source);
    drop(original);
    let store = open_current(config.clone());
    let receipt = store
        .history()
        .checkpoint_receipt(attempt)
        .unwrap()
        .unwrap();
    assert_eq!(receipt.request_digest.as_slice(), digest);
    assert_eq!(receipt.branch, record.branch);
    assert_eq!(
        store
            .history()
            .branch_snapshot(record.branch)
            .unwrap()
            .unwrap(),
        first
    );
    let outer = Owner::start(
        &fixture.0.join("template-new-2.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let prepared = store
        .prepare_migrated_catalog_recovery(already_migrated)
        .unwrap();
    assert_roster(&mut roster, record);
    let (owner, proof) = prepared.activate(3).unwrap();
    let client = outer.adopt_durable(owner).unwrap();
    let workspace = store
        .bind_catalog_recovery(client, request, proof)
        .unwrap()
        .workspace;
    assert_eq!(workspace.route().namespace(), 3);
    assert_eq!(
        read(&workspace, "file-000000"),
        b"Bomplete installed file 000000\n"
    );
    assert_eq!(
        read(&workspace, "file-000001"),
        b"Domplete installed file 000001\n"
    );
    write(
        &workspace,
        "file-000001",
        b"Eomplete installed file 000001\n",
    );
    assert!(workspace
        .commit_captured(&CommitOptions::default())
        .unwrap()
        .history
        .is_some());
    assert_eq!(
        read(&workspace, "file-000001"),
        b"Eomplete installed file 000001\n"
    );
    let service = Service::new(store.clone(), &outer);
    let token = service.register_recorded(&mut roster, workspace).unwrap();
    assert_eq!(token.namespace, 3);
    drop(service);
    store.close_reader(record.workspace).unwrap();
    outer.stop().unwrap();
    drop(roster);
    drop(store);
    let raw = Connection::open_with_flags(&config.path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(
        raw.query_row("SELECT count(*) FROM checkpoint_receipt", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
}

fn transition_config(fixture: &Fixture) -> CatalogTransitionConfig {
    CatalogTransitionConfig {
        store: fixture.config(),
        binding: BINDING.to_vec(),
        cursor_key: CURSOR,
        catalog_directory: fixture.0.join("catalog"),
        overlay_directory: fixture.0.join("overlays"),
        sandbox: [8; 32],
        profile: ProfileConfig::default(),
        minimum_namespace: 1,
        workspace_capacity: 2,
        cache_bytes: 2 * 1024 * 1024,
        reservations: ReservationBlocks::default(),
    }
}

#[test]
fn catalog_transition_migrates_and_reopens_the_original_workspace() {
    let fixture = Fixture::new();
    exercise_original_catalog_transition(&fixture);
}

/// This explicit test is for a caller-owned, fenced legacy fixture. It never
/// deletes that fixture and must only be invoked against a dedicated sample.
#[test]
#[ignore = "requires a caller-owned legacy 277 fixture and exclusive fence"]
fn catalog_transition_migrates_supplied_original_277_fixture() {
    let root = std::env::var_os("LAYERFS_TEST_ORIGINAL_277_FIXTURE")
        .expect("supply a dedicated, exclusively fenced original 277 fixture");
    let fixture = std::mem::ManuallyDrop::new(Fixture(PathBuf::from(root)));
    exercise_original_catalog_transition(&fixture);
}

fn exercise_original_catalog_transition(fixture: &Fixture) {
    let config = fixture.config();
    let original_identity = fs::symlink_metadata(&config.path).unwrap();
    let owner = Owner::start(
        &fixture.0.join("transition-template-1.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let original = fs::File::open(&config.path).unwrap();
    let plan =
        PreparedCatalogTransition::prepare(transition_config(fixture), original, &owner).unwrap();
    assert_eq!(plan.records().len(), 1);
    let record = plan.records()[0];
    let recovered = plan.execute(&owner, None, 0).unwrap();
    assert_eq!(recovered.tokens.len(), 1);
    let token = recovered.tokens[0];
    assert_eq!(token.workspace, record.workspace);
    assert_eq!(token.namespace, 2);
    let operation = recovered.service.operation(token).unwrap();
    assert_eq!(
        read_operation(&operation, "file-000000"),
        b"Bomplete installed file 000000\n"
    );
    assert_eq!(
        read_operation(&operation, "file-000001"),
        b"Domplete installed file 000001\n"
    );
    drop(operation);
    let reply = recovered
        .service
        .execute_control(&Request::Commit(CommitControl {
            token,
            force: false,
            message: None,
        }))
        .unwrap()
        .reply;
    assert!(matches!(
        reply,
        Reply::Committed(_) | Reply::MergeCommitted(_)
    ));
    let first = recovered
        .opened
        .store
        .history()
        .branch_snapshot(record.branch)
        .unwrap()
        .unwrap();
    let CatalogTransitionSuccess {
        opened, service, ..
    } = recovered;
    drop(service);
    opened.store.close_reader(record.workspace).unwrap();
    owner.stop().unwrap();
    drop(opened);
    let named = fs::symlink_metadata(&config.path).unwrap();
    assert_eq!(
        (named.dev(), named.ino()),
        (original_identity.dev(), original_identity.ino())
    );
    let raw = Connection::open_with_flags(&config.path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let (attempt, digest): (Vec<u8>, Vec<u8>) = raw
        .query_row(
            "SELECT attempt_id,request_digest FROM checkpoint_receipt",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        raw.query_row("SELECT count(*) FROM checkpoint_receipt", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    drop(raw);

    let owner = Owner::start(
        &fixture.0.join("transition-template-2.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let original = fs::File::open(&config.path).unwrap();
    let plan =
        PreparedCatalogTransition::prepare(transition_config(fixture), original, &owner).unwrap();
    assert_eq!(plan.records(), &[record]);
    let recovered = plan.execute(&owner, None, 0).unwrap();
    let next = recovered.tokens[0];
    assert_eq!(next.namespace, 3);
    assert!(matches!(
        recovered.service.operation(token),
        Err(layerfs_daemon::control::Failure::Rejected(
            layerfs_bridge::control::ControlCode::Invalid,
            "stale Workspace namespace"
        ))
    ));
    let receipt = recovered
        .opened
        .store
        .history()
        .checkpoint_receipt(CheckpointAttemptId::from_bytes(attempt.try_into().unwrap()))
        .unwrap()
        .unwrap();
    assert_eq!(receipt.request_digest.as_slice(), digest);
    assert_eq!(
        recovered
            .opened
            .store
            .history()
            .branch_snapshot(record.branch)
            .unwrap()
            .unwrap(),
        first
    );
    println!(
        "REOPENED_ORIGINAL token={token:?} next={next:?} receipt={receipt:?} snapshot={first:?}"
    );
    let operation = recovered.service.operation(next).unwrap();
    assert_eq!(
        read_operation(&operation, "file-000001"),
        b"Domplete installed file 000001\n"
    );
    write_operation(
        &operation,
        "file-000001",
        b"Eomplete installed file 000001\n",
    );
    drop(operation);
    let reply = recovered
        .service
        .execute_control(&Request::Commit(CommitControl {
            token: next,
            force: false,
            message: None,
        }))
        .unwrap()
        .reply;
    assert!(matches!(
        reply,
        Reply::Committed(_) | Reply::MergeCommitted(_)
    ));
    let operation = recovered.service.operation(next).unwrap();
    assert_eq!(
        read_operation(&operation, "file-000001"),
        b"Eomplete installed file 000001\n"
    );
    drop(operation);
    let CatalogTransitionSuccess {
        opened, service, ..
    } = recovered;
    drop(service);
    opened.store.close_reader(record.workspace).unwrap();
    owner.stop().unwrap();
    drop(opened);
    let raw = Connection::open_with_flags(&config.path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(
        raw.query_row("SELECT count(*) FROM checkpoint_receipt", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[test]
fn empty_legacy_catalog_upgrades_the_store_before_new_workspaces() {
    let fixture = Fixture::new();
    let config = fixture.config();
    let original = fs::File::open(&config.path).unwrap();
    let (readonly, source) = readonly_store(config.clone(), &original);
    let authority = Authority::for_catalog([8; 32], readonly.history()).unwrap();
    drop(readonly);
    drop(source);
    drop(original);
    let catalog = fixture.0.join("empty-catalog");
    let overlay = fixture.0.join("empty-overlays");
    for directory in [&catalog, &overlay] {
        fs::create_dir(directory).unwrap();
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    }
    drop(Roster::create(&catalog, authority).unwrap());
    let owner = Owner::start(
        &fixture.0.join("empty-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let mut transition = transition_config(&fixture);
    transition.catalog_directory = catalog;
    transition.overlay_directory = overlay;
    let plan = PreparedCatalogTransition::prepare(
        transition,
        fs::File::open(&config.path).unwrap(),
        &owner,
    )
    .unwrap();
    assert!(plan.records().is_empty());
    let recovered = plan.execute(&owner, None, 0).unwrap();
    assert!(recovered.tokens.is_empty());
    recovered
        .opened
        .store
        .history()
        .require_checkpoint_receipts()
        .unwrap();
    let CatalogTransitionSuccess {
        opened, service, ..
    } = recovered;
    drop(service);
    owner.stop().unwrap();
    drop(opened);
}

#[test]
fn ready_current_overlay_keeps_legacy_store_readable_without_receipts() {
    let fixture = Fixture::new();
    let config = fixture.config();
    let original = fs::File::open(&config.path).unwrap();
    let (readonly, source) = readonly_store(config.clone(), &original);
    let mut roster = Roster::open(
        &fixture.0.join("catalog"),
        Authority::for_catalog([8; 32], readonly.history()).unwrap(),
    )
    .unwrap();
    let mut records = Vec::new();
    roster.visit(|record| records.push(record)).unwrap();
    assert_eq!(records.len(), 1);
    let record = records[0];
    drop(roster);
    drop(readonly);
    drop(source);
    drop(original);
    let path = fixture.0.join("overlays").join(record.overlay_name());
    let ready = match layerfs_overlay::OverlayCatalogRecovery::open(
        &path,
        ProfileConfig::default(),
        record.workspace.to_bytes(),
    )
    .unwrap()
    {
        layerfs_overlay::OverlayCatalogRecovery::Ready(value) => value,
        _ => panic!("expected ready legacy fixture"),
    };
    let observed = ready.observed().clone();
    drop(ready.upgrade_legacy(&observed).unwrap());
    let owner = Owner::start(
        &fixture.0.join("current-template.sqlite"),
        ProfileConfig::default(),
        OwnerConfig { namespaces: 2 },
    )
    .unwrap();
    let plan = PreparedCatalogTransition::prepare(
        transition_config(&fixture),
        fs::File::open(&config.path).unwrap(),
        &owner,
    )
    .unwrap();
    let recovered = plan.execute(&owner, None, 0).unwrap();
    let token = recovered.tokens[0];
    let operation = recovered.service.operation(token).unwrap();
    assert_eq!(
        read_operation(&operation, "file-000001"),
        b"Domplete installed file 000001\n"
    );
    drop(operation);
    assert!(matches!(
        recovered
            .opened
            .store
            .history()
            .require_checkpoint_receipts(),
        Err(layerfs_history::HistoryError::Unsupported(
            "checkpoint receipt"
        ))
    ));
    let CatalogTransitionSuccess {
        opened, service, ..
    } = recovered;
    drop(service);
    opened.store.close_reader(record.workspace).unwrap();
    owner.stop().unwrap();
    drop(opened);
}

mod interrupted_catalog {
    use super::*;
    use layerfs_history::{BranchId, WorkspaceId};
    use layerfs_overlay::{OverlayCatalogRecovery, OverlayError};
    use rusqlite::{auto_extension, ffi};
    use std::{
        ffi::{c_char, c_int, c_void, CStr},
        io::{ErrorKind, Write},
        process::{Command, Stdio},
        sync::{
            atomic::{AtomicU64, Ordering},
            Mutex,
        },
        thread,
        time::{Duration, Instant},
    };

    const CHILD: &str = "LAYERFS_TEST_INTERRUPTED_CATALOG_CHILD";
    static WATCHED: Mutex<Option<(u64, u64)>> = Mutex::new(None);
    static FIRED: AtomicU64 = AtomicU64::new(0);

    unsafe extern "C" fn authorize(
        _: *mut c_void,
        action: c_int,
        first: *const c_char,
        _: *const c_char,
        _: *const c_char,
        _: *const c_char,
    ) -> c_int {
        if action == ffi::SQLITE_CREATE_TABLE
            && !first.is_null()
            && unsafe { CStr::from_ptr(first) }.to_bytes() == b"durable_owner_counts"
        {
            FIRED.fetch_add(1, Ordering::Relaxed);
            return ffi::SQLITE_DENY;
        }
        ffi::SQLITE_OK
    }
    fn install(connection: Connection) -> rusqlite::Result<()> {
        let Some(path) = connection.path() else {
            return Ok(());
        };
        let Ok(metadata) = fs::metadata(path) else {
            return Ok(());
        };
        if Some((metadata.dev(), metadata.ino())) != *WATCHED.lock().unwrap() {
            return Ok(());
        }
        // SAFETY: SQLite owns this live connection; the callback is static and
        // does not open, close or retain a pointer to the connection.
        assert_eq!(
            unsafe {
                ffi::sqlite3_set_authorizer(
                    connection.handle(),
                    Some(authorize),
                    std::ptr::null_mut(),
                )
            },
            ffi::SQLITE_OK
        );
        Ok(())
    }
    unsafe extern "C" fn opened(
        db: *mut ffi::sqlite3,
        error: *mut *mut c_char,
        _: *const ffi::sqlite3_api_routines,
    ) -> c_int {
        // SAFETY: SQLite invokes this callback with its own live connection.
        unsafe { auto_extension::init_auto_extension(db, error, install) }
    }
    fn record(byte: u8) -> WorkspaceRecord {
        WorkspaceRecord {
            workspace: WorkspaceId::from_authority([byte; 32]).unwrap(),
            branch: BranchId::from_authority([24; 16]),
        }
    }
    fn overlay(root: &Path, byte: u8) -> PathBuf {
        root.join("overlays").join(record(byte).overlay_name())
    }
    type DirtyRow = (i64, i64, i64, i64, Vec<u8>, Option<Vec<u8>>);
    fn dirty_rows(path: &Path) -> Vec<DirtyRow> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let mut query = db
            .prepare(
                "SELECT inode,generation,cell_offset,epoch,data,validity FROM fs_data \
                 ORDER BY inode,generation,cell_offset",
            )
            .unwrap();
        let rows = query
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows
    }
    fn signal(path: &Path) {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        file.write_all(b"ready\n").unwrap();
        file.sync_all().unwrap();
    }
    fn await_file(path: &Path, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while !path.exists() {
            assert!(Instant::now() < deadline, "bounded test handoff expired");
            thread::sleep(Duration::from_millis(20));
        }
    }
    fn config(root: &Path) -> CatalogTransitionConfig {
        CatalogTransitionConfig {
            store: PersistenceConfig::sqlite(&root.join("store-private/store.sqlite"))
                .with_sqlite_profile(SqlitePersistenceProfile::Durable)
                .with_sqlite_acquisition(SqliteAcquisitionSchema::Tables),
            binding: BINDING.to_vec(),
            cursor_key: CURSOR,
            catalog_directory: root.join("catalog"),
            overlay_directory: root.join("overlays"),
            sandbox: [8; 32],
            profile: ProfileConfig::default(),
            minimum_namespace: 1,
            workspace_capacity: 3,
            cache_bytes: 2 * 1024 * 1024,
            reservations: ReservationBlocks::default(),
        }
    }
    fn child(root: &Path) {
        let target = fs::metadata(overlay(root, 43)).unwrap();
        *WATCHED.lock().unwrap() = Some((target.dev(), target.ino()));
        // SAFETY: this callback is local to this child process and does not
        // itself open or close any SQLite connection.
        unsafe { auto_extension::register_auto_extension(opened).unwrap() };
        let owner = Owner::start(
            &root.join("failed-template.sqlite"),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 3 },
        )
        .unwrap();
        let original = fs::File::open(root.join("store-private/store.sqlite")).unwrap();
        let plan = PreparedCatalogTransition::prepare(config(root), original, &owner).unwrap();
        assert_eq!(plan.records(), &[record(42), record(43), record(44)]);
        assert!(auto_extension::cancel_auto_extension(opened));
        let failure = plan.execute(&owner, None, 0).err().unwrap();
        assert!(matches!(failure.cause(),
            layerfs_daemon::control::CatalogTransitionCause::Migration { record: failed, failure }
            if *failed == record(43) && matches!(failure.as_ref(),
                layerfs_daemon::store::CatalogMigrationFailure::Upgrade {
                    cause: layerfs_daemon::store::BindError::Owner(layerfs_daemon::OwnerError::Overlay(OverlayError::Sql(rusqlite::Error::SqliteFailure(error,_)))), ..
                } if error.extended_code == ffi::SQLITE_AUTH)));
        assert!(FIRED.load(Ordering::Relaxed) > 0);
        drop(failure);
        signal(&root.join("failed-owner-held"));
        await_file(&root.join("release-failed-owner"), Duration::from_secs(30));
        drop(owner);
    }

    #[test]
    fn failed_middle_migration_retains_all_leases_then_resumes_original_prefix() {
        if let Some(root) = std::env::var_os(CHILD) {
            child(Path::new(&root));
            return;
        }
        let fixture = Fixture::named("legacy_277_three");
        let store_path = fixture.0.join("store-private/store.sqlite");
        let initial_store = fs::symlink_metadata(&store_path).unwrap();
        let before: Vec<_> = [42_u8, 43, 44]
            .iter()
            .map(|byte| fs::symlink_metadata(overlay(&fixture.0, *byte)).unwrap())
            .collect();
        let original_dirty: Vec<_> = [42_u8, 43, 44]
            .iter()
            .map(|byte| dirty_rows(&overlay(&fixture.0, *byte)))
            .collect();
        for (index, rows) in original_dirty.iter().enumerate() {
            let mut expected = b"complete installed file 000001\n".to_vec();
            expected[0] = b'X' + index as u8;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].4, expected);
        }
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("interrupted_catalog::failed_middle_migration_retains_all_leases_then_resumes_original_prefix")
            .arg("--exact").arg("--nocapture").env(CHILD, &fixture.0)
            .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !fixture.0.join("failed-owner-held").exists() {
            if child.try_wait().unwrap().is_some() {
                let output = child.wait_with_output().unwrap();
                panic!(
                    "child exited before the held-lease check: {}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            assert!(Instant::now() < deadline, "bounded test handoff expired");
            thread::sleep(Duration::from_millis(20));
        }
        for byte in [42_u8, 43, 44] {
            let result = OverlayCatalogRecovery::open(
                &overlay(&fixture.0, byte),
                ProfileConfig::default(),
                [byte; 32],
            );
            assert!(
                matches!(result, Err(OverlayError::Io(error)) if error.kind() == ErrorKind::WouldBlock)
            );
        }
        signal(&fixture.0.join("release-failed-owner"));
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let store_after = fs::symlink_metadata(&store_path).unwrap();
        assert_eq!(
            (store_after.dev(), store_after.ino()),
            (initial_store.dev(), initial_store.ino())
        );
        let raw =
            Connection::open_with_flags(&store_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            raw.query_row("SELECT count(*) FROM checkpoint_receipt", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(raw);
        for (index, byte) in [42_u8, 43, 44].into_iter().enumerate() {
            let path = overlay(&fixture.0, byte);
            let after = fs::symlink_metadata(&path).unwrap();
            assert_eq!(
                (after.dev(), after.ino()),
                (before[index].dev(), before[index].ino())
            );
            let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            assert_eq!(
                db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                if index == 0 { 45 } else { 41 }
            );
            assert_eq!(
                db.query_row("SELECT high_namespace FROM durable_route", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                (index + 1) as i64
            );
            assert_eq!(
                db.query_row("SELECT dirty_inodes FROM durable_state", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                1
            );
            drop(db);
            assert_eq!(
                dirty_rows(&overlay(&fixture.0, byte)),
                original_dirty[index]
            );
        }
        let owner = Owner::start(
            &fixture.0.join("resumed-template.sqlite"),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 3 },
        )
        .unwrap();
        let plan = PreparedCatalogTransition::prepare(
            config(&fixture.0),
            fs::File::open(&store_path).unwrap(),
            &owner,
        )
        .unwrap();
        assert_eq!(plan.records(), &[record(42), record(43), record(44)]);
        let recovered = plan.execute(&owner, None, 0).unwrap();
        assert_eq!(recovered.tokens.len(), 3);
        for (index, token) in recovered.tokens.iter().enumerate() {
            assert_eq!(token.workspace, record(42 + index as u8).workspace);
            assert_eq!(token.namespace, (index + 2) as i64);
            let operation = recovered.service.operation(*token).unwrap();
            let mut expected = b"complete installed file 000001\n".to_vec();
            expected[0] = b'X' + index as u8;
            assert_eq!(read_operation(&operation, "file-000001"), expected);
        }
        let CatalogTransitionSuccess {
            opened, service, ..
        } = recovered;
        drop(service);
        for byte in [42_u8, 43, 44] {
            opened.store.close_reader(record(byte).workspace).unwrap();
        }
        owner.stop().unwrap();
        drop(opened);
        let raw =
            Connection::open_with_flags(&store_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            raw.query_row("SELECT count(*) FROM checkpoint_receipt", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}
