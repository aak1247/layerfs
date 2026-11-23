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
        PersistenceConfig::sqlite(self.0.join("store-private/store.sqlite"))
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
    let overlay = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(
        overlay
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        45
    );
    assert_eq!(
        overlay
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name='durable_external_capture'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

mod interrupted_catalog {
    use super::*;
    use layerfs_daemon::control::{
        CatalogCohortCause, PreparedCatalogCohort, RecoverControlConfig,
    };
    use layerfs_history::{BranchId, HistoryCatalog, WorkspaceId};
    use layerfs_overlay::{OverlayCatalogRecovery, OverlayError};
    use rusqlite::{auto_extension, ffi};
    use std::{
        ffi::{c_char, c_int, c_void, CStr},
        io::{ErrorKind, Write},
        process::{Command, Stdio},
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Mutex,
        },
        thread,
        time::{Duration, Instant},
    };

    const CHILD: &str = "LAYERFS_TEST_INTERRUPTED_CATALOG_CHILD";
    static WATCHED: Mutex<Option<(u64, u64)>> = Mutex::new(None);
    static FIRED: AtomicU64 = AtomicU64::new(0);
    static FAIL_CAPTURE_EXTENSION: AtomicBool = AtomicBool::new(false);

    unsafe extern "C" fn authorize(
        _: *mut c_void,
        action: c_int,
        first: *const c_char,
        _: *const c_char,
        _: *const c_char,
        _: *const c_char,
    ) -> c_int {
        let table = if FAIL_CAPTURE_EXTENSION.load(Ordering::Relaxed) {
            &b"durable_external_capture"[..]
        } else {
            &b"durable_owner_counts"[..]
        };
        if action == ffi::SQLITE_CREATE_TABLE
            && !first.is_null()
            && unsafe { CStr::from_ptr(first) }.to_bytes() == table
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
            store: PersistenceConfig::sqlite(root.join("store-private/store.sqlite"))
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

    fn cohort_config(root: &Path, name: &str, sandbox: [u8; 32]) -> CatalogTransitionConfig {
        let mut value = config(root);
        value.catalog_directory = root.join(name).join("catalog");
        value.overlay_directory = root.join(name).join("overlays");
        value.sandbox = sandbox;
        value
    }

    fn cohort_configs(root: &Path) -> Vec<CatalogTransitionConfig> {
        vec![
            cohort_config(root, "sandbox-a", [8; 32]),
            cohort_config(root, "sandbox-b", [9; 32]),
        ]
    }

    fn cohort_overlay(root: &Path, byte: u8) -> PathBuf {
        let sandbox = if byte == 42 { "sandbox-a" } else { "sandbox-b" };
        root.join(sandbox)
            .join("overlays")
            .join(record(byte).overlay_name())
    }

    fn cohort_owner(root: &Path, name: &str) -> Owner {
        Owner::start(
            &root.join(name),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 3 },
        )
        .unwrap()
    }

    fn cohort_lease_is_held(root: &Path, byte: u8) {
        let result = OverlayCatalogRecovery::open(
            &cohort_overlay(root, byte),
            ProfileConfig::default(),
            [byte; 32],
        );
        assert!(
            matches!(result, Err(OverlayError::Io(error)) if error.kind() == ErrorKind::WouldBlock)
        );
    }

    fn cohort_receipts(root: &Path) -> i64 {
        let db = Connection::open_with_flags(
            root.join("store-private/store.sqlite"),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        db.query_row("SELECT count(*) FROM checkpoint_receipt", [], |row| {
            row.get(0)
        })
        .unwrap()
    }

    fn cohort_authorities(root: &Path) -> [Authority; 2] {
        let handles = Handles::open_read_only(config(root).store, BINDING, CURSOR).unwrap();
        [
            Authority::for_catalog([8; 32], &handles.history).unwrap(),
            Authority::for_catalog([9; 32], &handles.history).unwrap(),
        ]
    }

    fn cohort_rosters_are_held(root: &Path, authorities: [Authority; 2]) {
        for (name, authority) in ["sandbox-a", "sandbox-b"].into_iter().zip(authorities) {
            let result = Roster::open(&root.join(name).join("catalog"), authority);
            assert!(
                matches!(result, Err(layerfs_daemon::durable_roster::RosterError::Io(error))
                if error.kind() == ErrorKind::WouldBlock)
            );
        }
    }

    fn recover_cohort(root: &Path) {
        let store = open_current(config(root).store);
        for (name, sandbox, bytes) in [
            ("sandbox-a", [8; 32], &[42_u8][..]),
            ("sandbox-b", [9; 32], &[43_u8, 44][..]),
        ] {
            let owner = cohort_owner(root, &format!("recover-{name}.sqlite"));
            let mut service = Service::new(store.clone(), &owner);
            let tokens = service
                .recover_durable_control(
                    &owner,
                    RecoverControlConfig {
                        catalog_directory: root.join(name).join("catalog"),
                        overlay_directory: root.join(name).join("overlays"),
                        sandbox,
                        profile: ProfileConfig::default(),
                        minimum_namespace: 1,
                        store_device: fs::metadata(root.join("store-private/store.sqlite"))
                            .unwrap()
                            .dev(),
                    },
                )
                .unwrap();
            assert_eq!(tokens.len(), bytes.len());
            for (&byte, token) in bytes.iter().zip(&tokens) {
                assert_eq!(token.workspace, record(byte).workspace);
                assert_eq!(token.namespace, i64::from(byte - 40));
                let operation = service.operation(*token).unwrap();
                let mut expected = b"complete installed file 000001\n".to_vec();
                expected[0] = b'X' + byte - 42;
                assert_eq!(read_operation(&operation, "file-000001"), expected);
            }
            drop(service);
            for token in tokens {
                store.close_reader(token.workspace).unwrap();
            }
            owner.stop().unwrap();
        }
        drop(store);
    }

    #[test]
    fn cohort_migrates_both_original_rosters_before_any_admission() {
        let fixture = Fixture::named("legacy_277_two_rosters");
        let root = &fixture.0;
        let store_path = root.join("store-private/store.sqlite");
        let store_before = fs::metadata(&store_path).unwrap();
        let authorities = cohort_authorities(root);
        let before = [42_u8, 43, 44].map(|byte| {
            let path = cohort_overlay(root, byte);
            (fs::metadata(&path).unwrap().ino(), dirty_rows(&path))
        });
        let owner = cohort_owner(root, "cohort-maintenance.sqlite");
        let plan = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(&store_path).unwrap(),
            &owner,
        )
        .unwrap();
        for byte in [42_u8, 43, 44] {
            cohort_lease_is_held(root, byte);
        }
        let result = plan.execute().unwrap();
        assert!(result.store_upgraded());
        assert_eq!(result.completed().len(), 3);
        cohort_rosters_are_held(root, authorities);
        for (index, entry) in result.completed().iter().enumerate() {
            assert_eq!(entry.group, usize::from(index > 0));
            assert_eq!(entry.record, record(42 + index as u8));
            assert_eq!(entry.previous_namespace(), (index + 1) as i64);
            assert_eq!(entry.source_version(), 41);
            cohort_lease_is_held(root, 42 + index as u8);
        }
        assert_eq!(cohort_receipts(root), 0);
        assert_eq!(fs::metadata(&store_path).unwrap().ino(), store_before.ino());
        drop(result);
        owner.stop().unwrap();
        for (index, byte) in [42_u8, 43, 44].into_iter().enumerate() {
            let path = cohort_overlay(root, byte);
            assert_eq!(fs::metadata(&path).unwrap().ino(), before[index].0);
            assert_eq!(dirty_rows(&path), before[index].1);
            let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            assert_eq!(
                db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                46
            );
            assert_eq!(
                db.query_row("SELECT high_namespace FROM durable_route", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                (index + 1) as i64
            );
        }
        recover_cohort(root);
        assert_eq!(cohort_receipts(root), 0);
    }

    const COHORT_SWAP_CHILD: &str = "LAYERFS_TEST_COHORT_SWAP_CHILD";

    fn cohort_swap_child(root: &Path) {
        let target = cohort_overlay(root, 43);
        // Read before opening any SQLite connection to this Overlay.
        let original_bytes = fs::read(&target).unwrap();
        let owner = cohort_owner(root, "swap-maintenance.sqlite");
        let plan = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(root.join("store-private/store.sqlite")).unwrap(),
            &owner,
        )
        .unwrap();
        let displaced = target.with_extension("sqlite.displaced");
        fs::rename(&target, displaced).unwrap();
        fs::write(&target, original_bytes).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        let failure = plan.execute().err().unwrap();
        assert!(
            matches!(failure.cause(), CatalogCohortCause::OverlayIdentity {
            index: 1, record: failed, cause: OverlayError::Invalid("allocation path identity"),
        } if *failed == record(43))
        );
        let progress = failure.progress();
        assert_eq!(progress.store_upgraded, None);
        assert!(progress.completed.is_empty());
        assert_eq!(progress.pending.len(), 3);
        assert_eq!(progress.current_group, Some(1));
        signal(&root.join("cohort-swap-held"));
        await_file(&root.join("release-cohort-swap"), Duration::from_secs(30));
        drop(failure);
        drop(owner);
    }

    #[test]
    fn cohort_rejects_replaced_member_before_store_upgrade() {
        if let Some(root) = std::env::var_os(COHORT_SWAP_CHILD) {
            cohort_swap_child(Path::new(&root));
            return;
        }
        let fixture = Fixture::named("legacy_277_two_rosters");
        let root = &fixture.0;
        let store_path = root.join("store-private/store.sqlite");
        let authorities = cohort_authorities(root);
        let store_before = fs::metadata(&store_path).unwrap();
        let original: Vec<_> = [42_u8, 43, 44]
            .into_iter()
            .map(|byte| {
                let path = cohort_overlay(root, byte);
                (fs::metadata(&path).unwrap().ino(), dirty_rows(&path))
            })
            .collect();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("interrupted_catalog::cohort_rejects_replaced_member_before_store_upgrade")
            .arg("--exact")
            .arg("--nocapture")
            .env(COHORT_SWAP_CHILD, root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !root.join("cohort-swap-held").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "swap child exited early"
            );
            assert!(Instant::now() < deadline, "bounded swap handoff expired");
            thread::sleep(Duration::from_millis(20));
        }
        for byte in [42_u8, 43, 44] {
            cohort_lease_is_held(root, byte);
        }
        cohort_rosters_are_held(root, authorities);
        let store =
            Connection::open_with_flags(&store_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(store.query_row("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='checkpoint_receipt'", [], |row| row.get::<_, i64>(0)).unwrap(), 0);
        drop(store);
        signal(&root.join("release-cohort-swap"));
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "child failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::metadata(&store_path).unwrap().ino(), store_before.ino());
        for (index, byte) in [42_u8, 43, 44].into_iter().enumerate() {
            let path = cohort_overlay(root, byte);
            let observed = if byte == 43 {
                path.with_extension("sqlite.displaced")
            } else {
                path
            };
            assert_eq!(fs::metadata(&observed).unwrap().ino(), original[index].0);
            assert_eq!(dirty_rows(&observed), original[index].1);
            let db =
                Connection::open_with_flags(observed, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            assert_eq!(
                db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                41
            );
            assert_eq!(
                db.query_row("SELECT high_namespace FROM durable_route", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                (index + 1) as i64
            );
        }
    }

    const COHORT_FAILURE_CHILD: &str = "LAYERFS_TEST_COHORT_FAILURE_CHILD";

    fn cohort_failure_child(root: &Path) {
        let target = fs::metadata(cohort_overlay(root, 43)).unwrap();
        *WATCHED.lock().unwrap() = Some((target.dev(), target.ino()));
        // SAFETY: the callback is scoped to this child and only configures
        // the matching SQLite connection while SQLite owns it.
        unsafe { auto_extension::register_auto_extension(opened).unwrap() };
        let owner = cohort_owner(root, "failed-cohort-maintenance.sqlite");
        let plan = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(root.join("store-private/store.sqlite")).unwrap(),
            &owner,
        )
        .unwrap();
        assert!(auto_extension::cancel_auto_extension(opened));
        let failure = plan.execute().err().unwrap();
        assert!(
            matches!(failure.cause(), CatalogCohortCause::Migration { index: 1, record: failed, failure }
            if *failed == record(43) && matches!(failure.as_ref(),
                layerfs_daemon::store::CatalogMigrationFailure::Upgrade {
                    cause: layerfs_daemon::store::BindError::Owner(layerfs_daemon::OwnerError::Overlay(OverlayError::Sql(rusqlite::Error::SqliteFailure(error,_)))), ..
                } if error.extended_code == ffi::SQLITE_AUTH))
        );
        assert!(FIRED.load(Ordering::Relaxed) > 0);
        let progress = failure.progress();
        assert_eq!(progress.store_upgraded, Some(true));
        assert_eq!(progress.current_group, Some(1));
        assert_eq!(progress.unprepared_groups, 0);
        assert_eq!(progress.completed.len(), 1);
        assert_eq!(progress.completed[0].group, 0);
        assert_eq!(progress.completed[0].record, record(42));
        assert_eq!(progress.completed[0].previous_namespace, 1);
        assert_eq!(progress.completed[0].source_version, 41);
        assert_eq!(progress.pending, vec![(1, record(43)), (1, record(44))]);
        signal(&root.join("cohort-failure-held"));
        await_file(
            &root.join("release-cohort-failure"),
            Duration::from_secs(30),
        );
        drop(failure);
        drop(owner);
    }

    #[test]
    fn cohort_retains_original_groups_after_mid_batch_failure_and_resumes() {
        if let Some(root) = std::env::var_os(COHORT_FAILURE_CHILD) {
            cohort_failure_child(Path::new(&root));
            return;
        }
        let fixture = Fixture::named("legacy_277_two_rosters");
        let root = &fixture.0;
        let store_path = root.join("store-private/store.sqlite");
        let authorities = cohort_authorities(root);
        let store_before = fs::metadata(&store_path).unwrap();
        let original: Vec<_> = [42_u8, 43, 44]
            .into_iter()
            .map(|byte| {
                let path = cohort_overlay(root, byte);
                (fs::metadata(&path).unwrap().ino(), dirty_rows(&path))
            })
            .collect();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("interrupted_catalog::cohort_retains_original_groups_after_mid_batch_failure_and_resumes")
            .arg("--exact").arg("--nocapture").env(COHORT_FAILURE_CHILD, root)
            .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !root.join("cohort-failure-held").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "failure child exited early"
            );
            assert!(Instant::now() < deadline, "bounded failure handoff expired");
            thread::sleep(Duration::from_millis(20));
        }
        for byte in [42_u8, 43, 44] {
            cohort_lease_is_held(root, byte);
        }
        cohort_rosters_are_held(root, authorities);
        assert_eq!(cohort_receipts(root), 0);
        signal(&root.join("release-cohort-failure"));
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "child failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::metadata(&store_path).unwrap().ino(), store_before.ino());
        for (index, byte) in [42_u8, 43, 44].into_iter().enumerate() {
            let path = cohort_overlay(root, byte);
            assert_eq!(fs::metadata(&path).unwrap().ino(), original[index].0);
            assert_eq!(dirty_rows(&path), original[index].1);
            let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            assert_eq!(
                db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                if index == 0 { 46 } else { 41 }
            );
            assert_eq!(
                db.query_row("SELECT high_namespace FROM durable_route", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                (index + 1) as i64
            );
        }
        let owner = cohort_owner(root, "resumed-cohort-maintenance.sqlite");
        let result = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(&store_path).unwrap(),
            &owner,
        )
        .unwrap()
        .execute()
        .unwrap();
        assert!(!result.store_upgraded());
        assert_eq!(
            result
                .completed()
                .iter()
                .map(|entry| entry.source_version())
                .collect::<Vec<_>>(),
            vec![46, 41, 41]
        );
        for byte in [42_u8, 43, 44] {
            cohort_lease_is_held(root, byte);
        }
        drop(result);
        owner.stop().unwrap();
        recover_cohort(root);
        assert_eq!(cohort_receipts(root), 0);
    }

    const CAPTURE_FAILURE_CHILD: &str = "LAYERFS_TEST_COHORT_CAPTURE_FAILURE_CHILD";

    fn capture_failure_child(root: &Path) {
        let target = fs::metadata(cohort_overlay(root, 43)).unwrap();
        *WATCHED.lock().unwrap() = Some((target.dev(), target.ino()));
        FAIL_CAPTURE_EXTENSION.store(true, Ordering::Relaxed);
        // SAFETY: the SQLite callback is scoped to this child and the held
        // test-owned Overlay connection, as in the legacy fault fixture.
        unsafe { auto_extension::register_auto_extension(opened).unwrap() };
        let owner = cohort_owner(root, "failed-capture-cohort.sqlite");
        let plan = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(root.join("store-private/store.sqlite")).unwrap(),
            &owner,
        )
        .unwrap();
        assert!(auto_extension::cancel_auto_extension(opened));
        let failure = plan.execute().err().unwrap();
        assert!(matches!(failure.cause(),
            CatalogCohortCause::Migration { index: 1, record: failed, failure }
            if *failed == record(43) && matches!(failure.as_ref(),
                layerfs_daemon::store::CatalogMigrationFailure::Upgrade {
                    cause: layerfs_daemon::store::BindError::Owner(layerfs_daemon::OwnerError::Overlay(OverlayError::Sql(rusqlite::Error::SqliteFailure(error,_)))), ..
                } if error.extended_code == ffi::SQLITE_AUTH)));
        assert!(FIRED.load(Ordering::Relaxed) > 0);
        let progress = failure.progress();
        assert_eq!(progress.store_upgraded, Some(false));
        assert_eq!(progress.completed.len(), 1);
        assert_eq!(progress.completed[0].record, record(42));
        assert_eq!(progress.completed[0].source_version, 45);
        assert_eq!(progress.pending, vec![(1, record(43)), (1, record(44))]);
        signal(&root.join("capture-failure-held"));
        await_file(
            &root.join("release-capture-failure"),
            Duration::from_secs(30),
        );
        drop(failure);
        drop(owner);
    }

    fn predecessor_schema(path: &Path) -> Vec<(String, String, String, Option<String>)> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let mut statement = db.prepare(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE tbl_name<>'durable_external_capture' ORDER BY type,name",
        ).unwrap();
        statement
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn cohort_capture_extension_rolls_back_middle_then_resumes_original_prefix() {
        if let Some(root) = std::env::var_os(CAPTURE_FAILURE_CHILD) {
            capture_failure_child(Path::new(&root));
            return;
        }
        let fixture = Fixture::named("legacy_44_two_rosters");
        let root = &fixture.0;
        for byte in [42_u8, 43, 44] {
            let ready = match OverlayCatalogRecovery::open(
                &cohort_overlay(root, byte),
                ProfileConfig::default(),
                [byte; 32],
            )
            .unwrap()
            {
                OverlayCatalogRecovery::Ready(ready) => ready,
                _ => panic!("expected original ready Workspace"),
            };
            let observed = ready.observed().clone();
            let ready = ready.upgrade_legacy(&observed).unwrap();
            assert_eq!(ready.preflight_upgrade(&observed).unwrap(), 45);
            drop(ready);
        }
        let before = [42_u8, 43, 44].map(|byte| {
            let path = cohort_overlay(root, byte);
            let schema = predecessor_schema(&path);
            assert_eq!(schema.len(), 49);
            (
                fs::metadata(&path).unwrap().ino(),
                dirty_rows(&path),
                schema,
            )
        });
        let original_receipts = cohort_receipts(root);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("interrupted_catalog::cohort_capture_extension_rolls_back_middle_then_resumes_original_prefix")
            .arg("--exact").arg("--nocapture").env(CAPTURE_FAILURE_CHILD, root)
            .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !root.join("capture-failure-held").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "capture failure child exited early"
            );
            assert!(
                Instant::now() < deadline,
                "bounded capture failure handoff expired"
            );
            thread::sleep(Duration::from_millis(20));
        }
        for byte in [42_u8, 43, 44] {
            cohort_lease_is_held(root, byte);
        }
        signal(&root.join("release-capture-failure"));
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "child failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for (index, byte) in [42_u8, 43, 44].into_iter().enumerate() {
            let path = cohort_overlay(root, byte);
            assert_eq!(fs::metadata(&path).unwrap().ino(), before[index].0);
            assert_eq!(dirty_rows(&path), before[index].1);
            assert_eq!(predecessor_schema(&path), before[index].2);
            let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            assert_eq!(
                db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                if index == 0 { 46 } else { 45 }
            );
            assert_eq!(
                db.query_row(
                    "SELECT count(*) FROM sqlite_schema WHERE name='durable_external_capture'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
                if index == 0 { 1 } else { 0 }
            );
        }
        assert_eq!(cohort_receipts(root), original_receipts);
        let owner = cohort_owner(root, "resumed-capture-cohort.sqlite");
        let result = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(root.join("store-private/store.sqlite")).unwrap(),
            &owner,
        )
        .unwrap()
        .execute()
        .unwrap();
        assert_eq!(
            result
                .completed()
                .iter()
                .map(|entry| entry.source_version())
                .collect::<Vec<_>>(),
            vec![46, 45, 45]
        );
        drop(result);
        owner.stop().unwrap();
        for (index, byte) in [42_u8, 43, 44].into_iter().enumerate() {
            let path = cohort_overlay(root, byte);
            assert_eq!(predecessor_schema(&path), before[index].2);
            assert_eq!(dirty_rows(&path), before[index].1);
            let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            assert_eq!(
                db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                46
            );
        }
        assert_eq!(cohort_receipts(root), original_receipts);
        recover_cohort(root);
    }

    #[test]
    fn cohort_accepts_ready_schema45_on_an_original_legacy_store() {
        let fixture = Fixture::named("legacy_277_two_rosters");
        let root = &fixture.0;
        let first = match OverlayCatalogRecovery::open(
            &cohort_overlay(root, 42),
            ProfileConfig::default(),
            [42; 32],
        )
        .unwrap()
        {
            OverlayCatalogRecovery::Ready(ready) => ready,
            _ => panic!("expected locally ready original Workspace"),
        };
        let observed = first.observed().clone();
        drop(first.upgrade_legacy(&observed).unwrap());
        let store_path = root.join("store-private/store.sqlite");
        let db =
            Connection::open_with_flags(&store_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(db.query_row("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='checkpoint_receipt'", [], |row| row.get::<_, i64>(0)).unwrap(), 0);
        drop(db);
        let owner = cohort_owner(root, "current-cohort-maintenance.sqlite");
        let result = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(store_path).unwrap(),
            &owner,
        )
        .unwrap()
        .execute()
        .unwrap();
        assert!(result.store_upgraded());
        assert_eq!(
            result
                .completed()
                .iter()
                .map(|entry| entry.source_version())
                .collect::<Vec<_>>(),
            vec![45, 41, 41]
        );
        assert_eq!(result.completed()[0].previous_namespace(), 1);
        drop(result);
        owner.stop().unwrap();
        assert_eq!(cohort_receipts(root), 0);
    }

    #[test]
    fn cohort_preserves_an_existing_schema46_capture_and_original_receipt() {
        use layerfs_bridge::control::{
            CaptureControl, CaptureIdentity, CaptureObservation, CapturePurpose,
        };
        let fixture = Fixture::named("legacy_44_two_rosters");
        let root = &fixture.0;
        let store_path = root.join("store-private/store.sqlite");
        let maintenance = cohort_owner(root, "first-capture-maintenance.sqlite");
        let result = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(&store_path).unwrap(),
            &maintenance,
        )
        .unwrap()
        .execute()
        .unwrap();
        assert_eq!(result.completed().len(), 3);
        drop(result);
        maintenance.stop().unwrap();

        let store = open_current(config(root).store);
        let owner = cohort_owner(root, "capture-current-owner.sqlite");
        let mut service = Service::new(store.clone(), &owner);
        let tokens = service
            .recover_durable_control(
                &owner,
                RecoverControlConfig {
                    catalog_directory: root.join("sandbox-a/catalog"),
                    overlay_directory: root.join("sandbox-a/overlays"),
                    sandbox: [8; 32],
                    profile: ProfileConfig::default(),
                    minimum_namespace: 1,
                    store_device: fs::metadata(&store_path).unwrap().dev(),
                },
            )
            .unwrap();
        assert_eq!(tokens.len(), 1);
        let token = tokens[0];
        let result = service
            .execute_control(&Request::Capture(CaptureControl {
                token,
                identity: CaptureIdentity {
                    id: [91; 32],
                    scope_digest: [92; 32],
                    purpose: CapturePurpose::ManagedWorkspace,
                },
            }))
            .unwrap();
        assert!(matches!(
            result.reply,
            Reply::Capture(CaptureObservation::Published(_))
        ));
        drop(service);
        store.close_reader(token.workspace).unwrap();
        owner.stop().unwrap();
        drop(store);
        assert_eq!(cohort_receipts(root), 1);

        fn captured(path: &Path) -> Vec<rusqlite::types::Value> {
            let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            let mut statement = db.prepare("SELECT capture_id,scope_digest,purpose,attempt,workspace,catalog,incarnation,namespace,generation,revision,base_root,snapshot,request,request_digest,candidate_root,target_snapshot,disposition FROM durable_external_capture").unwrap();
            let count = statement.column_count();
            statement
                .query_row([], |row| (0..count).map(|index| row.get(index)).collect())
                .unwrap()
        }
        fn receipts(path: &Path) -> Vec<Vec<rusqlite::types::Value>> {
            let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            let mut statement = db
                .prepare("SELECT * FROM checkpoint_receipt ORDER BY attempt_id")
                .unwrap();
            let count = statement.column_count();
            statement
                .query_map([], |row| (0..count).map(|index| row.get(index)).collect())
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        }
        let path = cohort_overlay(root, 42);
        let original = captured(&path);
        let original_receipts = receipts(&store_path);
        assert_eq!(original_receipts.len(), 1);
        let maintenance = cohort_owner(root, "repeat-capture-maintenance.sqlite");
        let result = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(&store_path).unwrap(),
            &maintenance,
        )
        .unwrap()
        .execute()
        .unwrap();
        assert_eq!(
            result
                .completed()
                .iter()
                .map(|entry| entry.source_version())
                .collect::<Vec<_>>(),
            vec![46, 46, 46]
        );
        drop(result);
        maintenance.stop().unwrap();
        assert_eq!(captured(&path), original);
        assert_eq!(receipts(&store_path), original_receipts);
    }

    #[test]
    fn cohort_reports_the_group_whose_preparation_failed() {
        let fixture = Fixture::named("legacy_277_two_rosters");
        let root = &fixture.0;
        let mut configs = cohort_configs(root);
        configs[1].sandbox = [7; 32];
        let store_path = root.join("store-private/store.sqlite");
        let owner = cohort_owner(root, "refused-cohort-maintenance.sqlite");
        let failure =
            PreparedCatalogCohort::prepare(configs, fs::File::open(&store_path).unwrap(), &owner)
                .err()
                .unwrap();
        assert!(matches!(
            failure.cause(),
            CatalogCohortCause::Group { index: 1, .. }
        ));
        let progress = failure.progress();
        assert_eq!(progress.store_upgraded, None);
        assert_eq!(progress.current_group, Some(1));
        assert_eq!(progress.pending, vec![(0, record(42))]);
        assert!(progress.completed.is_empty());
        assert_eq!(progress.unprepared_groups, 0);
        cohort_lease_is_held(root, 42);
        drop(failure);
        owner.stop().unwrap();
        let db = Connection::open_with_flags(store_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(db.query_row("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='checkpoint_receipt'", [], |row| row.get::<_, i64>(0)).unwrap(), 0);
    }

    fn exercise_recorded_predecessor(version: i64) {
        let fixture = Fixture::named(&format!("legacy_{version}_two_rosters"));
        let root = &fixture.0;
        let store_path = root.join("store-private/store.sqlite");
        let store_before = fs::metadata(&store_path).unwrap();
        let before = [42_u8, 43, 44].map(|byte| {
            let path = cohort_overlay(root, byte);
            let identity = fs::metadata(&path).unwrap().ino();
            let rows = dirty_rows(&path);
            let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            assert_eq!(
                db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                version
            );
            (identity, rows)
        });
        assert_eq!(cohort_receipts(root), 0);
        let owner = cohort_owner(root, "recorded-cohort-maintenance.sqlite");
        let result = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(&store_path).unwrap(),
            &owner,
        )
        .unwrap()
        .execute()
        .unwrap();
        assert!(!result.store_upgraded());
        assert_eq!(result.completed().len(), 3);
        for (index, entry) in result.completed().iter().enumerate() {
            assert_eq!(entry.source_version(), version);
            assert_eq!(entry.previous_namespace(), (index + 1) as i64);
            cohort_lease_is_held(root, 42 + index as u8);
        }
        drop(result);
        owner.stop().unwrap();
        assert_eq!(fs::metadata(&store_path).unwrap().ino(), store_before.ino());
        for (index, byte) in [42_u8, 43, 44].into_iter().enumerate() {
            let path = cohort_overlay(root, byte);
            assert_eq!(fs::metadata(&path).unwrap().ino(), before[index].0);
            assert_eq!(dirty_rows(&path), before[index].1);
            let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            assert_eq!(
                db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                46
            );
            assert_eq!(
                db.query_row("SELECT high_namespace FROM durable_route", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                (index + 1) as i64
            );
        }
        recover_cohort(root);
        assert_eq!(cohort_receipts(root), 0);
    }

    #[test]
    fn cohort_migrates_original_schema43_dirty_workspaces() {
        exercise_recorded_predecessor(43);
    }

    #[test]
    fn cohort_migrates_original_schema44_dirty_workspaces() {
        exercise_recorded_predecessor(44);
    }

    #[test]
    fn cohort_rejects_same_version_with_different_schema() {
        let fixture = Fixture::named("legacy_43_two_rosters");
        let root = &fixture.0;
        let path = cohort_overlay(root, 42);
        let before = dirty_rows(&path);
        let db = Connection::open(&path).unwrap();
        db.execute_batch("DROP TRIGGER durable_checkpoint_attempt_owner_immutable; CREATE TRIGGER durable_checkpoint_attempt_owner_immutable BEFORE UPDATE ON durable_checkpoint_attempt BEGIN SELECT 1; END;").unwrap();
        drop(db);
        let owner = cohort_owner(root, "invalid-schema-maintenance.sqlite");
        let failure = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(root.join("store-private/store.sqlite")).unwrap(),
            &owner,
        )
        .err()
        .unwrap();
        assert!(
            matches!(failure.cause(), CatalogCohortCause::Group { index: 0, failure }
            if matches!(failure.cause(), layerfs_daemon::control::CatalogTransitionCause::Migration { record: failed, failure }
                if *failed == record(42) && matches!(failure.as_ref(),
                    layerfs_daemon::store::CatalogMigrationFailure::Refused {
                        cause: layerfs_daemon::store::BindError::Owner(layerfs_daemon::OwnerError::Overlay(OverlayError::Invalid("legacy overlay schema identity"))), ..
                    })))
        );
        assert_eq!(failure.progress().store_upgraded, None);
        drop(failure);
        owner.stop().unwrap();
        assert_eq!(dirty_rows(&path), before);
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            43
        );
        assert_eq!(
            db.query_row("SELECT high_namespace FROM durable_route", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(cohort_receipts(root), 0);
    }

    #[test]
    fn cohort_retains_unproven_legacy_serial_retirement() {
        let fixture = Fixture::named("legacy_43_two_rosters");
        let root = &fixture.0;
        let path = cohort_overlay(root, 42);
        let before = dirty_rows(&path);
        let db = Connection::open(&path).unwrap();
        db.execute(
            "INSERT INTO cleanup_task(kind,resource,target) VALUES(7,99,1)",
            [],
        )
        .unwrap();
        drop(db);
        let owner = cohort_owner(root, "unknown-retirement-maintenance.sqlite");
        let failure = PreparedCatalogCohort::prepare(
            cohort_configs(root),
            fs::File::open(root.join("store-private/store.sqlite")).unwrap(),
            &owner,
        )
        .err()
        .unwrap();
        assert!(
            matches!(failure.cause(), CatalogCohortCause::Group { index: 0, failure }
            if matches!(failure.cause(), layerfs_daemon::control::CatalogTransitionCause::Overlay { record: failed, .. }
                if *failed == record(42)))
        );
        assert_eq!(failure.progress().store_upgraded, None);
        drop(failure);
        owner.stop().unwrap();
        assert_eq!(dirty_rows(&path), before);
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            43
        );
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM cleanup_task WHERE kind=7 AND resource=99 AND target=1",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            db.query_row("SELECT high_namespace FROM durable_route", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(cohort_receipts(root), 0);
    }

    fn conflict_rows(path: &Path) -> Vec<Vec<rusqlite::types::Value>> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let mut result = Vec::new();
        for sql in [
            "SELECT * FROM durable_binding",
            "SELECT * FROM durable_conflict_plan ORDER BY generation",
            "SELECT * FROM durable_conflict_page ORDER BY generation,page_no",
            "SELECT * FROM durable_state",
            "SELECT * FROM durable_route",
            "SELECT * FROM cleanup_task ORDER BY kind,resource,target",
        ] {
            let mut statement = db.prepare(sql).unwrap();
            let columns = statement.column_count();
            let rows = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|column| row.get(column))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap();
            result.extend(rows.map(Result::unwrap));
        }
        result
    }

    fn receipt_rows(root: &Path) -> Vec<Vec<rusqlite::types::Value>> {
        let db = Connection::open_with_flags(
            root.join("store-private/store.sqlite"),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let mut statement = db
            .prepare("SELECT * FROM checkpoint_receipt ORDER BY attempt_id")
            .unwrap();
        let columns = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|column| row.get(column))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        rows
    }

    #[test]
    fn cohort_migrates_original_schema44_active_conflict_and_resolves_once() {
        use layerfs_daemon::store::MaintainedDisposition;

        let fixture = Fixture::named("legacy_44_active_conflict");
        let root = &fixture.0;
        let store_path = root.join("store-private/store.sqlite");
        let path = root.join("overlays").join(record(43).overlay_name());
        let marker = fs::read(root.join("expected-marker.bin")).unwrap();
        let store_before = fs::metadata(&store_path).unwrap();
        let overlay_before = fs::metadata(&path).unwrap();
        let before = conflict_rows(&path);
        let original_receipts = receipt_rows(root);
        assert_eq!(original_receipts.len(), 1);
        let original_head = {
            let handles = Handles::open_read_only(fixture.config(), BINDING, CURSOR).unwrap();
            handles
                .history
                .branch_snapshot(record(43).branch)
                .unwrap()
                .unwrap()
        };
        assert_eq!(cohort_receipts(root), 1);
        let owner = cohort_owner(root, "conflict-maintenance.sqlite");
        let result = PreparedCatalogCohort::prepare(
            vec![transition_config(&fixture)],
            fs::File::open(&store_path).unwrap(),
            &owner,
        )
        .unwrap()
        .execute()
        .unwrap();
        assert!(!result.store_upgraded());
        assert_eq!(result.completed().len(), 1);
        assert_eq!(result.completed()[0].record, record(43));
        assert_eq!(result.completed()[0].previous_namespace(), 2);
        assert_eq!(result.completed()[0].source_version(), 44);
        assert_eq!(
            result.completed()[0].disposition(),
            MaintainedDisposition::SettledConflict
        );
        assert!(
            matches!(OverlayCatalogRecovery::open(&path, ProfileConfig::default(), [43; 32]),
            Err(OverlayError::Io(error)) if error.kind() == ErrorKind::WouldBlock)
        );
        assert_eq!(receipt_rows(root), original_receipts);
        drop(result);
        owner.stop().unwrap();
        assert_eq!(fs::metadata(&store_path).unwrap().ino(), store_before.ino());
        assert_eq!(fs::metadata(&path).unwrap().ino(), overlay_before.ino());
        assert_eq!(conflict_rows(&path), before);
        let db = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            46
        );
        drop(db);

        let store = open_current(fixture.config());
        assert_eq!(
            store
                .history()
                .branch_snapshot(record(43).branch)
                .unwrap()
                .unwrap(),
            original_head
        );
        let owner = cohort_owner(root, "conflict-recovery.sqlite");
        let mut service = Service::new(store.clone(), &owner);
        let tokens = service
            .recover_durable_control(
                &owner,
                RecoverControlConfig {
                    catalog_directory: root.join("catalog"),
                    overlay_directory: root.join("overlays"),
                    sandbox: [8; 32],
                    profile: ProfileConfig::default(),
                    minimum_namespace: 1,
                    store_device: store_before.dev(),
                },
            )
            .unwrap();
        assert_eq!(tokens.len(), 1);
        let token = tokens[0];
        assert_eq!(token.workspace, record(43).workspace);
        assert_eq!(token.namespace, 3);
        let operation = service.operation(token).unwrap();
        assert_eq!(read_operation(&operation, "file-000000"), marker);
        drop(operation);
        let refused = service
            .execute_control(&Request::Commit(CommitControl {
                token,
                force: false,
                message: None,
            }))
            .unwrap()
            .reply;
        assert!(
            matches!(refused, Reply::Conflict(report) if report.markers && report.conflicts.iter().any(|item| item.path == "/file-000000"))
        );
        assert_eq!(
            store
                .history()
                .branch_snapshot(record(43).branch)
                .unwrap()
                .unwrap(),
            original_head
        );
        let operation = service.operation(token).unwrap();
        let resolution = vec![b'R'; marker.len()];
        write_operation(&operation, "file-000000", &resolution);
        drop(operation);
        let resolved = service
            .execute_control(&Request::Commit(CommitControl {
                token,
                force: false,
                message: None,
            }))
            .unwrap()
            .reply;
        assert!(matches!(
            resolved,
            Reply::Committed(_) | Reply::MergeCommitted(_)
        ));
        assert_ne!(
            store
                .history()
                .branch_snapshot(record(43).branch)
                .unwrap()
                .unwrap(),
            original_head
        );
        drop(service);
        store.close_reader(token.workspace).unwrap();
        owner.stop().unwrap();
        drop(store);
        let final_receipts = receipt_rows(root);
        assert_eq!(final_receipts.len(), 2);
        assert!(final_receipts.contains(&original_receipts[0]));
    }

    #[test]
    fn cohort_rejects_extra_conflict_page_before_any_migration() {
        let fixture = Fixture::named("legacy_44_active_conflict");
        let root = &fixture.0;
        let path = root.join("overlays").join(record(43).overlay_name());
        let db = Connection::open(&path).unwrap();
        let trigger: String = db.query_row("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='durable_conflict_page_insert'", [], |row| row.get(0)).unwrap();
        let (generation, page_count): (i64, i64) = db
            .query_row(
                "SELECT generation,page_count FROM durable_conflict_plan WHERE state=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        db.execute_batch("DROP TRIGGER durable_conflict_page_insert")
            .unwrap();
        db.execute(
            "INSERT INTO durable_conflict_page(generation,page_no,data) VALUES(?1,?2,X'00')",
            (generation, page_count),
        )
        .unwrap();
        db.execute_batch(&trigger).unwrap();
        drop(db);
        let before = conflict_rows(&path);
        let owner = cohort_owner(root, "extra-page-maintenance.sqlite");
        let failure = PreparedCatalogCohort::prepare(
            vec![transition_config(&fixture)],
            fs::File::open(root.join("store-private/store.sqlite")).unwrap(),
            &owner,
        )
        .err()
        .unwrap();
        assert!(
            matches!(failure.cause(), CatalogCohortCause::Group { index: 0, failure }
            if matches!(failure.cause(), layerfs_daemon::control::CatalogTransitionCause::Migration { record: failed, failure }
                if *failed == record(43) && matches!(failure.as_ref(),
                    layerfs_daemon::store::CatalogMigrationFailure::Refused {
                        cause: layerfs_daemon::store::BindError::Owner(layerfs_daemon::OwnerError::Overlay(OverlayError::Invalid("settled conflict contents"))), ..
                    })))
        );
        assert_eq!(failure.progress().store_upgraded, None);
        drop(failure);
        owner.stop().unwrap();
        assert_eq!(conflict_rows(&path), before);
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            44
        );
        assert_eq!(
            db.query_row("SELECT high_namespace FROM durable_route", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(cohort_receipts(root), 1);
    }

    #[test]
    fn ordinary_conflict_preparation_still_requires_the_durable_writer() {
        let fixture = Fixture::named("legacy_44_active_conflict");
        let root = &fixture.0;
        let path = root.join("overlays").join(record(43).overlay_name());
        let original = fs::File::open(root.join("store-private/store.sqlite")).unwrap();
        let (readonly, source) = readonly_store(fixture.config(), &original);
        let pending =
            OverlayCatalogRecovery::open(&path, ProfileConfig::default(), [43; 32]).unwrap();
        let failure = readonly
            .prepare_opened_catalog_recovery(
                pending,
                BindRequest {
                    workspace: record(43).workspace,
                    branch: record(43).branch,
                },
            )
            .err()
            .unwrap();
        assert!(matches!(
            failure.as_ref(),
            layerfs_daemon::store::CatalogPreparationFailure::Retained {
                cause: layerfs_daemon::store::BindError::History(
                    layerfs_history::HistoryError::Integrity("catalog recovery identity")
                ),
                ..
            }
        ));
        drop(failure);
        drop(readonly);
        drop(source);
        drop(original);
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            44
        );
        assert_eq!(
            db.query_row("SELECT high_namespace FROM durable_route", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
}

mod shared_store_recovery {
    use super::*;
    use layerfs_daemon::control::RecoverControlConfig;
    use layerfs_history::{
        BranchId, ForkRequest, ForkSource, HistoryError, HistoryName, WorkspaceId,
    };
    use nix::fcntl::OFlag;
    use std::{
        io::Write,
        os::unix::fs::OpenOptionsExt,
        process::{Child, Command, Stdio},
        thread,
        time::{Duration, Instant},
    };

    const CHILD: &str = "LAYERFS_TEST_SHARED_STORE_WRITER";
    type SchemaRow = (String, String, String, Option<String>);

    fn store_config(root: &Path) -> PersistenceConfig {
        PersistenceConfig::sqlite(root.join("store-private/store.sqlite"))
            .with_sqlite_profile(SqlitePersistenceProfile::Durable)
            .with_sqlite_acquisition(SqliteAcquisitionSchema::Tables)
    }
    fn marker(path: &Path) {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        file.write_all(b"done\n").unwrap();
        file.sync_all().unwrap();
    }
    fn wait_marker(path: &Path, mut child: Option<&mut Child>) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !path.exists() {
            if let Some(child) = child.as_deref_mut() {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "other Store writer exited early"
                );
            }
            assert!(
                Instant::now() < deadline,
                "bounded Store writer handoff expired"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
    fn schema(path: &Path) -> Vec<SchemaRow> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let mut query = db
            .prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")
            .unwrap();
        let rows = query
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows
    }
    fn other_writer(root: &Path) {
        let store = open_store(
            store_config(root),
            BINDING,
            CURSOR,
            2,
            2 * 1024 * 1024,
            ReservationBlocks::default(),
        )
        .unwrap();
        let original = BranchId::from_authority([24; 16]);
        let before = store.history().branch_snapshot(original).unwrap().unwrap();
        assert!(matches!(
            store.history().require_checkpoint_receipts(),
            Err(HistoryError::Unsupported("checkpoint receipt"))
        ));
        marker(&root.join("other-writer-ready"));
        wait_marker(&root.join("other-writer-continue"), None);
        let after = store.history().branch_snapshot(original).unwrap().unwrap();
        assert_ne!(after.branch.head_commit, before.branch.head_commit);
        let branch = BranchId::from_authority([77; 16]);
        let fork = store
            .history()
            .fork(&ForkRequest {
                stack: after.branch.stack,
                branch,
                name: HistoryName::new("other-pool").unwrap(),
                source: ForkSource::Commit {
                    branch: original,
                    commit: after.branch.head_commit.unwrap(),
                },
            })
            .unwrap();
        assert_eq!(fork.branch.id, branch);
        assert!(matches!(
            store.history().require_checkpoint_receipts(),
            Err(HistoryError::Unsupported("checkpoint receipt"))
        ));
        marker(&root.join("other-writer-continued"));
        wait_marker(&root.join("other-writer-stop"), None);
        drop(store);
    }

    #[test]
    fn ordinary_recovery_preserves_format_while_another_store_writer_continues() {
        if let Some(root) = std::env::var_os(CHILD) {
            other_writer(Path::new(&root));
            return;
        }
        let fixture = Fixture::new();
        let store_path = fixture.0.join("store-private/store.sqlite");
        let overlay_path = fixture
            .0
            .join("overlays")
            .join(format!("workspace-{}.sqlite", "2a".repeat(32)));
        let store_before = fs::metadata(&store_path).unwrap();
        let overlay_before = fs::metadata(&overlay_path).unwrap();
        let store_schema = schema(&store_path);
        let overlay_schema = schema(&overlay_path);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("shared_store_recovery::ordinary_recovery_preserves_format_while_another_store_writer_continues")
            .arg("--exact").arg("--nocapture").env(CHILD, &fixture.0)
            .stdout(Stdio::inherit()).stderr(Stdio::inherit()).spawn().unwrap();
        wait_marker(&fixture.0.join("other-writer-ready"), Some(&mut child));
        assert_eq!(schema(&store_path), store_schema);

        // The ordinary descriptor proves read access and closes before this
        // process opens SQLite. O_PATH remains safe to close while SQLite lives.
        let original = fs::OpenOptions::new()
            .read(true)
            .custom_flags((OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC).bits())
            .open(&store_path)
            .unwrap();
        let anchor = fs::OpenOptions::new()
            .read(true)
            .custom_flags((OFlag::O_PATH | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC).bits())
            .open(&store_path)
            .unwrap();
        assert_eq!(
            (
                original.metadata().unwrap().dev(),
                original.metadata().unwrap().ino()
            ),
            (
                anchor.metadata().unwrap().dev(),
                anchor.metadata().unwrap().ino()
            )
        );
        drop(original);
        let wrong = fs::OpenOptions::new()
            .read(true)
            .custom_flags((OFlag::O_PATH | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC).bits())
            .open(fixture.0.join("template.sqlite"))
            .unwrap();
        let refused = layerfs_daemon::bootstrap::open_existing_store_observed(
            store_config(&fixture.0),
            &wrong,
            BINDING,
            CURSOR,
            2,
            2 * 1024 * 1024,
            ReservationBlocks::default(),
        )
        .err()
        .unwrap();
        assert!(matches!(refused, StorageError::Io(ref error)
            if error.get_ref().and_then(|inner| inner.downcast_ref::<layerfs_storage::port::PersistenceError>())
                == Some(&layerfs_storage::port::PersistenceError::Malformed)));
        drop(wrong);
        let opened = layerfs_daemon::bootstrap::open_existing_store_observed(
            store_config(&fixture.0),
            &anchor,
            BINDING,
            CURSOR,
            2,
            2 * 1024 * 1024,
            ReservationBlocks::default(),
        )
        .unwrap();
        opened.store.verify_original_file(&anchor).unwrap();
        assert!(matches!(
            opened.store.history().require_checkpoint_receipts(),
            Err(HistoryError::Unsupported("checkpoint receipt"))
        ));
        let owner = Owner::start(
            &fixture.0.join("ordinary-template.sqlite"),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 2 },
        )
        .unwrap();
        let mut service = Service::new(opened.store.clone(), &owner);
        let tokens = service
            .recover_durable_control(
                &owner,
                RecoverControlConfig {
                    catalog_directory: fixture.0.join("catalog"),
                    overlay_directory: fixture.0.join("overlays"),
                    sandbox: [8; 32],
                    profile: ProfileConfig::default(),
                    minimum_namespace: 1,
                    store_device: store_before.dev(),
                },
            )
            .unwrap();
        assert_eq!(tokens.len(), 1);
        let token = tokens[0];
        assert_eq!(
            token.workspace,
            WorkspaceId::from_authority([42; 32]).unwrap()
        );
        assert_eq!(token.namespace, 2);
        opened.store.verify_original_file(&anchor).unwrap();
        drop(anchor);
        let operation = service.operation(token).unwrap();
        assert_eq!(
            read_operation(&operation, "file-000001"),
            b"Domplete installed file 000001\n"
        );
        drop(operation);
        let reply = service
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

        let empty_catalog = fixture.0.join("empty-catalog");
        let empty_overlays = fixture.0.join("empty-overlays");
        for path in [&empty_catalog, &empty_overlays] {
            fs::create_dir(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let authority = Authority::for_catalog([9; 32], opened.store.history()).unwrap();
        drop(Roster::create(&empty_catalog, authority).unwrap());
        let empty_owner = Owner::start(
            &fixture.0.join("empty-ordinary-template.sqlite"),
            ProfileConfig::default(),
            OwnerConfig { namespaces: 2 },
        )
        .unwrap();
        let mut empty = Service::new(opened.store.clone(), &empty_owner);
        let none = empty
            .recover_durable_control(
                &empty_owner,
                RecoverControlConfig {
                    catalog_directory: empty_catalog,
                    overlay_directory: empty_overlays,
                    sandbox: [9; 32],
                    profile: ProfileConfig::default(),
                    minimum_namespace: 1,
                    store_device: store_before.dev(),
                },
            )
            .unwrap();
        assert!(none.is_empty());
        marker(&fixture.0.join("other-writer-continue"));
        wait_marker(&fixture.0.join("other-writer-continued"), Some(&mut child));
        assert!(opened
            .store
            .history()
            .branch_snapshot(BranchId::from_authority([77; 16]))
            .unwrap()
            .is_some());
        drop(empty);
        empty_owner.stop().unwrap();
        drop(service);
        opened.store.close_reader(token.workspace).unwrap();
        owner.stop().unwrap();
        drop(opened);
        assert_eq!(schema(&store_path), store_schema);
        assert_eq!(schema(&overlay_path), overlay_schema);
        assert_eq!(fs::metadata(&store_path).unwrap().ino(), store_before.ino());
        assert_eq!(
            fs::metadata(&overlay_path).unwrap().ino(),
            overlay_before.ino()
        );
        let store =
            Connection::open_with_flags(&store_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(store.query_row("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='checkpoint_receipt'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
        drop(store);
        let overlay =
            Connection::open_with_flags(&overlay_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            overlay
                .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            41
        );
        assert_eq!(
            overlay
                .query_row("SELECT high_namespace FROM durable_route", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        drop(overlay);
        marker(&fixture.0.join("other-writer-stop"));
        assert!(child.wait().unwrap().success());
    }

    #[test]
    fn o_path_anchor_does_not_release_original_process_locks() {
        use nix::fcntl::{fcntl, FcntlArg};
        let lock = nix::libc::flock {
            l_type: nix::libc::F_WRLCK as _,
            l_whence: nix::libc::SEEK_SET as _,
            l_start: 0,
            l_len: 1,
            l_pid: 0,
        };
        if let Some(path) = std::env::var_os("LAYERFS_TEST_LOCK_PROBE") {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            let outcome = fcntl(&file, FcntlArg::F_SETLK(&lock));
            match std::env::var("LAYERFS_TEST_LOCK_EXPECT").unwrap().as_str() {
                "held" => assert!(matches!(
                    outcome,
                    Err(nix::errno::Errno::EAGAIN | nix::errno::Errno::EACCES)
                )),
                "released" => assert!(outcome.is_ok()),
                _ => panic!("unexpected lock probe"),
            }
            return;
        }
        let directory =
            std::env::temp_dir().join(format!("layerfs-o-path-lock-{}", std::process::id()));
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.join("store.sqlite");
        let holder = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        fcntl(&holder, FcntlArg::F_SETLK(&lock)).unwrap();
        let anchor = fs::OpenOptions::new()
            .read(true)
            .custom_flags((OFlag::O_PATH | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC).bits())
            .open(&path)
            .unwrap();
        drop(anchor);
        let probe =
            |expect: &str| {
                let status=Command::new(std::env::current_exe().unwrap())
                .arg("shared_store_recovery::o_path_anchor_does_not_release_original_process_locks")
                .arg("--exact").arg("--nocapture")
                .env("LAYERFS_TEST_LOCK_PROBE",&path).env("LAYERFS_TEST_LOCK_EXPECT",expect)
                .status().unwrap();
                assert!(status.success());
            };
        probe("held");
        let ordinary = fs::File::open(&path).unwrap();
        drop(ordinary);
        probe("released");
        drop(holder);
        fs::remove_file(path).unwrap();
        fs::remove_dir(directory).unwrap();
    }
}
