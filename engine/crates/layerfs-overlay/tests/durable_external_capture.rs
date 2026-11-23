//! An externally identified capture keeps its original local checkpoint facts.
#![cfg(target_os = "linux")]

use layerfs_overlay::{
    CapturePurpose, Changes, ExternalCapture, ExternalCaptureDisposition, Inode, InodeKind,
    MaintenanceCursor, Overlay, OverlayCatalogRecovery, ProfileConfig,
};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const WORKSPACE: [u8; 32] = [43; 32];
const CATALOG: [u8; 32] = [27; 32];
const ROOT: [u8; 32] = [17; 32];
const SNAPSHOT: &[u8] = b"original branch snapshot";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "layerfs-external-capture-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
    fn path(&self) -> PathBuf {
        self.0.join("overlay.sqlite")
    }
    fn create(&self) -> Overlay {
        let db =
            Overlay::create_durable(&self.path(), ProfileConfig::default(), WORKSPACE).unwrap();
        db.open_workspace(ROOT).unwrap();
        db.initialize_durable_binding(ROOT, SNAPSHOT).unwrap();
        db.initialize_durable_namespace(7).unwrap();
        let source = db.acquire_base_source(1).unwrap();
        let reply = db
            .apply(
                source,
                &Changes {
                    inodes: vec![Inode {
                        serial: 60,
                        kind: InodeKind::File,
                        mode: 0o644,
                        mtime_seconds: 17,
                        mtime_nanoseconds: 123,
                        nlink: 1,
                        size: 0,
                        inherited_cutoff: 0,
                        born: 0,
                        entries: 0,
                        subdirs: 0,
                    }],
                    ..Changes::default()
                },
            )
            .unwrap();
        db.reply_attempted(reply).unwrap();
        db.release_base_source(source).unwrap();
        db
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_EXTERNAL_CAPTURE_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
fn identity() -> ExternalCapture {
    ExternalCapture {
        id: [77; 32],
        scope_digest: [88; 32],
        purpose: CapturePurpose::ManagedWorkspace,
    }
}
fn digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs/checkpoint-request/v1\0");
    hash.update(bytes);
    *hash.finalize().as_bytes()
}
fn drain(db: &Overlay) {
    let mut cursor = MaintenanceCursor::default();
    for _ in 0..256 {
        if db.maintenance_idle().unwrap() {
            return;
        }
        cursor = db
            .maintain(cursor)
            .unwrap()
            .expect("maintenance stalled")
            .cursor;
    }
    panic!("maintenance did not finish");
}

#[test]
fn original_identity_survives_local_fold_and_blocks_duplicate_ids() {
    let fixture = Fixture::new();
    let db = fixture.create();
    assert_eq!(db.profile().schema_version, 46);
    let before = db.state().unwrap();
    let capture = db
        .capture_external(identity(), WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let first = db.observe_external_capture(identity()).unwrap().unwrap();
    assert_eq!(first.attempt, capture.attempt());
    assert_eq!(
        (
            first.workspace,
            first.catalog,
            first.incarnation,
            first.namespace
        ),
        (WORKSPACE, CATALOG, 12, 7)
    );
    assert_eq!(
        (first.generation, first.revision, first.base_root),
        (before.active.number() as u64, before.revision as u64, ROOT)
    );
    assert_eq!(first.original_snapshot, SNAPSHOT);
    assert!(first.publication.is_none());
    assert_eq!(first.disposition, ExternalCaptureDisposition::Unresolved);
    let mut foreign = identity();
    foreign.scope_digest = [89; 32];
    assert_eq!(db.observe_external_capture(foreign).unwrap(), None);
    db.fold_unpublished_checkpoint(capture).unwrap();
    let mut folded = first;
    folded.disposition = ExternalCaptureDisposition::Unpublished;
    drain(&db);
    db.finish_folded_checkpoint_cleanup(capture).unwrap();
    let settled = db.state().unwrap();
    assert!(db
        .capture_external(identity(), WORKSPACE, CATALOG, 12, SNAPSHOT)
        .is_err());
    assert!(db
        .capture_external(foreign, WORKSPACE, CATALOG, 12, SNAPSHOT)
        .is_err());
    assert_eq!(db.state().unwrap(), settled);
    assert_eq!(
        db.observe_external_capture(identity()).unwrap(),
        Some(folded.clone())
    );
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    assert!(sql
        .execute("UPDATE durable_external_capture SET disposition=0", [])
        .is_err());
    assert!(sql
        .execute("UPDATE durable_external_capture SET request=x'01'", [])
        .is_err());
    assert!(sql
        .execute("DELETE FROM durable_external_capture", [])
        .is_err());
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    assert_eq!(
        db.observe_external_capture(identity()).unwrap(),
        Some(folded)
    );
    db.close_database().unwrap();
}

#[test]
fn publication_is_exact_and_observable_after_pending_reopen() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let capture = db
        .capture_external(identity(), WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let mut request = b"LFCP\x01".to_vec();
    request.extend((0..60000).map(|i| (i % 251) as u8));
    let target = b"original target snapshot";
    let root = [18; 32];
    assert!(db
        .prepare_checkpoint_publication(capture, &request, [0; 32], root, target)
        .is_err());
    assert!(db
        .observe_external_capture(identity())
        .unwrap()
        .unwrap()
        .publication
        .is_none());
    db.prepare_checkpoint_publication(capture, &request, digest(&request), root, target)
        .unwrap();
    let original = db.observe_external_capture(identity()).unwrap().unwrap();
    assert_eq!(original.disposition, ExternalCaptureDisposition::Unresolved);
    let publication = original.publication.as_ref().unwrap();
    assert_eq!(publication.request, request);
    assert_eq!(publication.request_digest, digest(&request));
    assert_eq!(publication.candidate_root, root);
    assert_eq!(publication.target_snapshot, target);
    assert!(db
        .prepare_checkpoint_publication(capture, &request, digest(&request), root, target)
        .is_err());
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    assert!(sql
        .execute("UPDATE durable_external_capture SET disposition=1", [])
        .is_err());
    drop(sql);
    let pending =
        match OverlayCatalogRecovery::open(&fixture.path(), ProfileConfig::default(), WORKSPACE)
            .unwrap()
        {
            OverlayCatalogRecovery::PendingCheckpoint(pending) => pending,
            _ => panic!("expected original pending checkpoint"),
        };
    assert_eq!(
        pending.observe_external_capture(identity()).unwrap(),
        Some(original)
    );
    let mut foreign = identity();
    foreign.scope_digest = [89; 32];
    assert_eq!(pending.observe_external_capture(foreign).unwrap(), None);
    drop(pending);
}

#[test]
fn phase_one_cannot_declare_unpublished_without_original_fold() {
    let fixture = Fixture::new();
    let db = fixture.create();
    db.capture_external(identity(), WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    assert!(sql
        .execute("UPDATE durable_external_capture SET disposition=1", [])
        .is_err());
    drop(sql);
    let mut pending =
        match OverlayCatalogRecovery::open(&fixture.path(), ProfileConfig::default(), WORKSPACE)
            .unwrap()
        {
            OverlayCatalogRecovery::PendingCheckpoint(pending) => pending,
            _ => panic!("expected original pending checkpoint"),
        };
    assert_eq!(
        pending
            .observe_external_capture(identity())
            .unwrap()
            .unwrap()
            .disposition,
        ExternalCaptureDisposition::Unresolved
    );
    pending.fold_before_publication().unwrap();
    assert_eq!(
        pending
            .observe_external_capture(identity())
            .unwrap()
            .unwrap()
            .disposition,
        ExternalCaptureDisposition::Unpublished
    );
}

#[test]
fn proven_absent_publication_keeps_original_request_after_fold() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let capture = db
        .capture_external(identity(), WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let request = b"LFCP\x01original request";
    db.prepare_checkpoint_publication(capture, request, digest(request), [18; 32], SNAPSHOT)
        .unwrap();
    let original = db.observe_external_capture(identity()).unwrap().unwrap();
    drop(db);
    let mut pending =
        match OverlayCatalogRecovery::open(&fixture.path(), ProfileConfig::default(), WORKSPACE)
            .unwrap()
        {
            OverlayCatalogRecovery::PendingCheckpoint(pending) => pending,
            _ => panic!("expected original pending checkpoint"),
        };
    // The real Store caller must hold its original lane and writer fence;
    // this local test exercises the transaction after that external proof.
    assert_eq!(
        pending.observe_external_capture(identity()).unwrap(),
        Some(original.clone())
    );
    pending.verify_frozen_request(request).unwrap();
    pending.fold_after_absent_receipt().unwrap();
    let mut expected = original;
    expected.disposition = ExternalCaptureDisposition::Unpublished;
    assert_eq!(
        pending.observe_external_capture(identity()).unwrap(),
        Some(expected.clone())
    );
    pending.release_original_owners().unwrap();
    let mut cursor = MaintenanceCursor::default();
    for _ in 0..256 {
        if pending.cleanup_ready(None).unwrap() {
            break;
        }
        cursor = pending
            .maintain(cursor)
            .unwrap()
            .expect("maintenance stalled")
            .cursor;
    }
    assert!(pending.cleanup_ready(None).unwrap());
    let db = pending.complete_settled(Some(request), None).unwrap();
    assert_eq!(
        db.observe_external_capture(identity()).unwrap(),
        Some(expected.clone())
    );
    db.close_database().unwrap();
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    assert_eq!(
        db.observe_external_capture(identity()).unwrap(),
        Some(expected)
    );
    db.close_database().unwrap();
}

#[test]
fn failed_local_record_rolls_back_capture_and_original_attempt() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let before = db.state().unwrap();
    assert!(db
        .capture_external(identity(), WORKSPACE, CATALOG, 12, b"different binding")
        .is_err());
    assert!(db
        .capture_external(identity(), WORKSPACE, CATALOG, 0, SNAPSHOT)
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    assert_eq!(db.observe_external_capture(identity()).unwrap(), None);
    db.close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let count = |table: &str| {
        sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap()
    };
    assert_eq!(
        (
            count("durable_checkpoint_attempt"),
            count("durable_external_capture")
        ),
        (0, 0)
    );
}

#[test]
fn extra_checkpoint_trigger_is_rejected_before_capture() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER inject_capture_conflict AFTER INSERT ON durable_checkpoint_attempt BEGIN \
      INSERT INTO durable_external_capture(capture_id,scope_digest,purpose,attempt,workspace,catalog,incarnation,namespace,generation,revision,base_root,snapshot) \
      VALUES(zeroblob(32),zeroblob(32),1,NEW.attempt,NEW.workspace,NEW.catalog,NEW.incarnation,NEW.namespace,NEW.generation,NEW.revision,NEW.base_root,NEW.snapshot); END;").unwrap();
    drop(sql);
    let before = fs::read(fixture.path()).unwrap();
    assert!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).is_err()
    );
    assert_eq!(fs::read(fixture.path()).unwrap(), before);
}

fn copy_legacy(path: &Path) {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy_d144");
    for name in ["overlay.sqlite", "overlay.sqlite.owner"] {
        fs::copy(source.join(name), path.join(name)).unwrap();
        fs::set_permissions(path.join(name), fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[test]
fn old_ready_format_requires_explicit_fenced_local_migration() {
    let fixture = Fixture::new();
    copy_legacy(&fixture.0);
    let ready =
        match OverlayCatalogRecovery::open(&fixture.path(), ProfileConfig::default(), WORKSPACE)
            .unwrap()
        {
            OverlayCatalogRecovery::Ready(ready) => ready,
            _ => panic!("expected ready"),
        };
    let original = ready.observed().clone();
    let ready = ready.upgrade_legacy(&original).unwrap();
    assert_eq!(ready.schema_version(), 45);
    let db = ready.complete(&original).unwrap();
    let before = db.state().unwrap();
    let original_inode = db.inode(60).unwrap();
    assert!(db
        .capture_external(identity(), WORKSPACE, CATALOG, 12, &original.snapshot)
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    db.close_database().unwrap();
    let ready =
        match OverlayCatalogRecovery::open(&fixture.path(), ProfileConfig::default(), WORKSPACE)
            .unwrap()
        {
            OverlayCatalogRecovery::Ready(ready) => ready,
            _ => panic!("expected ready"),
        };
    let mut wrong = original.clone();
    wrong.snapshot.push(0);
    let (_, ready) = ready.upgrade_external_capture(&wrong).err().unwrap();
    assert_eq!(ready.schema_version(), 45);
    let ready = ready.upgrade_external_capture(&original).unwrap();
    assert_eq!(ready.schema_version(), 46);
    let db = ready.complete(&original).unwrap();
    assert_eq!(db.state().unwrap(), before);
    assert_eq!(db.inode(60).unwrap(), original_inode);
    assert_eq!(
        db.durable_binding().unwrap().as_deref(),
        Some(original.snapshot.as_slice())
    );
    assert!(db.observe_external_capture(identity()).unwrap().is_none());
    db.close_database().unwrap();
}
