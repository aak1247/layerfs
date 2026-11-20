//! The original capture and its prepublication identity share one transaction.
#![cfg(target_os = "linux")]

use layerfs_overlay::{
    Changes, CleanupState, Inode, InodeKind, Overlay, OverlayError, ProfileConfig,
};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const WORKSPACE: [u8; 32] = [43; 32];
const CATALOG: [u8; 32] = [27; 32];
const ROOT: [u8; 32] = [17; 32];
const SNAPSHOT: &[u8] = b"authenticated original branch snapshot";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "layerfs-checkpoint-attempt-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        Self(root)
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
        let publication = db
            .apply(
                source,
                &Changes {
                    inodes: vec![Inode {
                        serial: 60,
                        kind: InodeKind::File,
                        mode: 0o644,
                        mtime_seconds: 17,
                        mtime_nanoseconds: 123_456_789,
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
        db.reply_attempted(publication).unwrap();
        db.release_base_source(source).unwrap();
        db
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("RETAINED_CHECKPOINT_FIXTURE {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

#[test]
fn capture_and_original_attempt_are_persisted_together_without_granting_recovery() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let original = db.state().unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let capture = recorded.capture();
    assert_eq!(capture.generation, original.active);
    assert_eq!(capture.revision, original.revision);
    assert_eq!(capture.base_root, ROOT);
    assert_ne!(recorded.attempt(), [0; 32]);
    assert!(matches!(
        db.resolve_failed_capture(capture),
        Err(OverlayError::Invalid(
            "recorded capture requires disposition"
        ))
    ));
    assert!(matches!(
        db.install_recorded(capture, ROOT, SNAPSHOT),
        Err(OverlayError::Invalid(
            "recorded capture requires disposition"
        ))
    ));
    db.close().unwrap();
    assert!(matches!(
        db.release_closed_capture(capture),
        Err(OverlayError::Invalid(
            "recorded capture requires disposition"
        ))
    ));
    assert_eq!(db.cleanup_state().unwrap(), CleanupState::Held);
    drop(db);

    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (Vec<u8>, Vec<u8>, Vec<u8>, i64, i64, i64, i64, Vec<u8>, Vec<u8>, i64) = connection.query_row(
        "SELECT attempt,workspace,catalog,incarnation,namespace,generation,revision,base_root,snapshot,phase FROM durable_checkpoint_attempt WHERE id=1",
        [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?)),
    ).unwrap();
    assert_eq!(row.0, recorded.attempt());
    assert_eq!(row.1, WORKSPACE);
    assert_eq!(row.2, CATALOG);
    assert_eq!((row.3, row.4), (12, 7));
    assert_eq!(
        (row.5, row.6),
        (capture.generation.number(), capture.revision)
    );
    assert_eq!(row.7, ROOT);
    assert_eq!(row.8, SNAPSHOT);
    assert_eq!(row.9, 1);
    drop(connection);
    let before = fs::read(fixture.path()).unwrap();
    assert!(matches!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE),
        Err(OverlayError::Invalid(
            "durable overlay requires reconciliation"
        ))
    ));
    assert_eq!(fs::read(fixture.path()).unwrap(), before);
}

#[test]
fn bad_identity_or_failed_attempt_insert_rolls_back_the_capture() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let before = db.state().unwrap();
    assert!(db
        .capture_recorded([42; 32], CATALOG, 12, SNAPSHOT)
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    assert!(db
        .capture_recorded(WORKSPACE, CATALOG, 12, b"wrong snapshot")
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    db.close_database().unwrap();

    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_attempt BEFORE INSERT ON durable_checkpoint_attempt BEGIN SELECT RAISE(ABORT, 'injected attempt failure'); END;").unwrap();
    drop(connection);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    assert!(db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    assert_eq!(db.active_inodes(0).unwrap()[0].serial, 60);
    db.close_database().unwrap();
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM durable_checkpoint_attempt", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    let persisted: (Option<i64>, i64) = connection
        .query_row(
            "SELECT captured,dirty_inodes FROM durable_state WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(persisted, (None, 1));
}

#[test]
fn catalog_identity_preserves_the_full_history_domain() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, [0; 32], 12, SNAPSHOT)
        .unwrap();
    assert_ne!(recorded.attempt(), [0; 32]);
    drop(db);
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    let catalog: Vec<u8> = connection
        .query_row(
            "SELECT catalog FROM durable_checkpoint_attempt WHERE id=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(catalog, [0; 32]);
}

fn request_digest(request: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs/checkpoint-request/v1\0");
    hash.update(request);
    *hash.finalize().as_bytes()
}

#[test]
fn publication_intent_is_frozen_once_for_the_original_capture() {
    let fixture = Fixture::new();
    let db = fixture.create();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let capture = recorded.capture();
    let request = b"LFCP\x01original complete request";
    let target = b"authenticated target branch snapshot";
    let candidate = [18; 32];
    assert!(db
        .prepare_checkpoint_publication(recorded, request, [0; 32], candidate, target)
        .is_err());
    assert!(db
        .prepare_checkpoint_publication(recorded, request, request_digest(request), candidate, b"")
        .is_err());
    assert_eq!(db.state().unwrap().captured, Some(capture.generation));
    db.prepare_checkpoint_publication(
        recorded,
        request,
        request_digest(request),
        candidate,
        target,
    )
    .unwrap();
    assert!(matches!(
        db.prepare_checkpoint_publication(
            recorded,
            request,
            request_digest(request),
            candidate,
            target
        ),
        Err(OverlayError::Invalid("checkpoint publication predecessor"))
    ));
    assert!(db.install_recorded(capture, candidate, target).is_err());
    assert!(db.resolve_failed_capture(capture).is_err());
    drop(db);

    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (Vec<u8>, i64, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) = sql.query_row(
        "SELECT attempt,phase,request,request_digest,candidate_root,target_snapshot FROM durable_checkpoint_attempt WHERE id=1",
        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
    ).unwrap();
    assert_eq!(
        row,
        (
            recorded.attempt().to_vec(),
            2,
            request.to_vec(),
            request_digest(request).to_vec(),
            candidate.to_vec(),
            target.to_vec()
        )
    );
    drop(sql);
    let before = fs::read(fixture.path()).unwrap();
    assert!(
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).is_err()
    );
    assert_eq!(fs::read(fixture.path()).unwrap(), before);
}

#[test]
fn failed_publication_intent_write_keeps_original_capture_and_phase() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_publication_intent BEFORE UPDATE ON durable_checkpoint_attempt BEGIN SELECT RAISE(ABORT, 'intent failure'); END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let before = db.state().unwrap();
    let request = b"LFCP\x01request";
    assert!(db
        .prepare_checkpoint_publication(
            recorded,
            request,
            request_digest(request),
            [18; 32],
            SNAPSHOT
        )
        .is_err());
    assert_eq!(db.state().unwrap(), before);
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let row: (i64, Option<Vec<u8>>, Option<Vec<u8>>) = sql
        .query_row(
            "SELECT phase,request,request_digest FROM durable_checkpoint_attempt WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(row, (1, None, None));
}

#[test]
fn changed_original_attempt_context_cannot_be_frozen_for_publication() {
    let fixture = Fixture::new();
    fixture.create().close_database().unwrap();
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    sql.execute_batch("CREATE TRIGGER alter_capture_context AFTER INSERT ON durable_checkpoint_attempt BEGIN UPDATE durable_checkpoint_attempt SET catalog=zeroblob(32) WHERE id=1; END;").unwrap();
    drop(sql);
    let db =
        Overlay::recover_durable(&fixture.path(), ProfileConfig::default(), WORKSPACE).unwrap();
    let recorded = db
        .capture_recorded(WORKSPACE, CATALOG, 12, SNAPSHOT)
        .unwrap();
    let before = db.state().unwrap();
    let request = b"LFCP\x01request";
    assert!(matches!(
        db.prepare_checkpoint_publication(
            recorded,
            request,
            request_digest(request),
            [18; 32],
            SNAPSHOT
        ),
        Err(OverlayError::Invalid("checkpoint original identity"))
    ));
    assert_eq!(db.state().unwrap(), before);
    drop(db);
    let sql = rusqlite::Connection::open(fixture.path()).unwrap();
    let (phase, request): (i64, Option<Vec<u8>>) = sql
        .query_row(
            "SELECT phase,request FROM durable_checkpoint_attempt WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((phase, request), (1, None));
}
