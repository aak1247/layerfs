//! Actual legacy Store/Overlay bytes remain usable across a coordinated upgrade.
#![cfg(target_os = "linux")]

use layerfs_content::filesystem::PathName;
use layerfs_daemon::{
    bootstrap::open_store,
    control::Service,
    durable_roster::{Authority, Roster, WorkspaceRecord},
    store::{BindRequest, BoundWorkspace, CommitOptions, ReaderFactory, Store, StoreReader},
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
    os::unix::fs::PermissionsExt,
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
                .join("tests/fixtures/legacy_277")
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
    let route = workspace.route();
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
    let route = workspace.route();
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
