//! Durable checkpoint outcomes share the Branch publication transaction.
mod support;

use layerfs_history::*;
#[cfg(target_os = "linux")]
use layerfs_persistence::PreparedCheckpointUpgrade;
use layerfs_persistence::{Handles, PersistenceConfig, SqliteAcquisitionSchema, SqlitePackLayout};
use layerfs_storage::StoragePolicy;
use support::*;

const RECEIPTS: &str = include_str!("../sql/sqlite/checkpoint_receipts.sql");

fn config(path: &std::path::Path) -> PersistenceConfig {
    PersistenceConfig::sqlite(path)
}

fn create_durable(path: &std::path::Path) -> Handles {
    Handles::create(
        config(path),
        StoragePolicy::frozen_default(),
        &HistoryCatalogConfig {
            cursor_key: [71; 32],
            binding_key: BINDING.to_vec(),
            incarnation: 7,
        },
    )
    .unwrap()
}

fn make_branch(c: &dyn HistoryCatalog) -> BranchSnapshot {
    let stack = c
        .initialize_layerstack(&StackInitialization {
            logical: 0,
            stack: stack(1),
            name: name("main"),
            scope: root(200),
            profile: root(201),
            genesis_root: root(0),
        })
        .unwrap();
    c.fork(&ForkRequest {
        stack: stack.id,
        branch: branch_id(1),
        name: name("work"),
        source: ForkSource::Layer(stack.head_layer),
    })
    .unwrap()
}

fn request(snapshot: &BranchSnapshot, candidate: layerfs_content::ObjectId) -> CommitRequest {
    CommitRequest {
        branch: snapshot.branch.id,
        expected_head: snapshot.branch.head_commit,
        expected_base: snapshot.branch.base_layer,
        expected_root: snapshot.effective_root,
        candidate_root: candidate,
        profile: snapshot.profile,
        scope: snapshot.scope,
        second_parent: None,
        meta: CommitMeta::new(CommitVia::Workspace),
        force: false,
    }
}

fn attempt(tag: u8) -> CheckpointAttemptId {
    CheckpointAttemptId::from_bytes([tag; 32])
}

fn schema_definition(sql: &rusqlite::Connection) -> String {
    let mut statement = sql.prepare(
        "SELECT type,name,sql FROM sqlite_schema WHERE substr(name,1,7)!='sqlite_' ORDER BY type,name"
    ).unwrap();
    let rows = statement
        .query_map([], |r| Ok([r.get::<_, String>(0)?, r.get(1)?, r.get(2)?]))
        .unwrap();
    rows.map(|r| r.unwrap().map(|v| v + "\n").concat())
        .collect()
}

#[test]
fn recorded_results_remain_exact_after_the_branch_advances_and_reopen() {
    let temp = Temp::new("checkpoint-receipt");
    let path = temp.join("db");
    let handles = create_durable(&path);
    let c = &handles.history;
    c.require_checkpoint_receipts().unwrap();
    let initial = make_branch(c);
    let genesis_request = request(&initial, initial.effective_root);
    let genesis = c.commit_checkpoint(attempt(5), &genesis_request).unwrap();
    assert!(matches!(
        genesis,
        CommitOutcome::UpToDate { head: None, .. }
    ));
    let mut first = request(&initial, root(1));
    // The shared Commit table does not persist this transport flag; the
    // original result must nevertheless survive in its own receipt.
    first.meta.lww_truncated = true;
    let published = c.commit_checkpoint(attempt(1), &first).unwrap();
    let after_first = c.branch_snapshot(initial.branch.id).unwrap().unwrap();
    let unchanged_request = request(&after_first, after_first.effective_root);
    let unchanged = c.commit_checkpoint(attempt(2), &unchanged_request).unwrap();
    assert!(matches!(unchanged, CommitOutcome::UpToDate { .. }));
    c.commit_branch(&request(&after_first, root(2))).unwrap();
    let after_second = c.branch_snapshot(initial.branch.id).unwrap().unwrap();
    assert_eq!(
        c.commit_checkpoint(attempt(1), &first),
        Err(HistoryError::Integrity(
            "checkpoint attempt already recorded"
        ))
    );
    assert_eq!(
        c.branch_snapshot(initial.branch.id).unwrap().unwrap(),
        after_second
    );
    assert_eq!(c.checkpoint_receipt(attempt(3)).unwrap(), None);
    drop(handles);

    let reopened = Handles::open_read_only(config(&path), BINDING, [71; 32]).unwrap();
    assert_eq!(
        reopened.history.require_checkpoint_receipts(),
        Err(HistoryError::ContinuityUnavailable)
    );
    for (id, original, input) in [
        (attempt(1), published, first),
        (attempt(2), unchanged, unchanged_request),
        (attempt(5), genesis, genesis_request),
    ] {
        let observed = reopened.history.checkpoint_receipt(id).unwrap().unwrap();
        assert_eq!(observed.attempt, id);
        assert_eq!(observed.branch, initial.branch.id);
        assert_eq!(
            observed.request_digest,
            checkpoint_request_digest(&encode_checkpoint_request(&input).unwrap())
        );
        assert_eq!(observed.outcome, original);
    }
}

#[test]
fn failed_receipt_insert_rolls_back_the_branch_and_commit() {
    let temp = Temp::new("checkpoint-rollback");
    let path = temp.join("db");
    let handles = create_durable(&path);
    let c = &handles.history;
    let initial = make_branch(c);
    let input = request(&initial, root(1));
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute_batch("CREATE TRIGGER reject_test_receipt BEFORE INSERT ON checkpoint_receipt BEGIN SELECT RAISE(ABORT,'test receipt failure'); END;").unwrap();
    assert!(matches!(
        c.commit_checkpoint(attempt(4), &input),
        Err(HistoryError::Integrity(_))
    ));
    assert_eq!(
        c.branch_snapshot(initial.branch.id).unwrap().unwrap(),
        initial
    );
    assert_eq!(c.checkpoint_receipt(attempt(4)).unwrap(), None);
    let count: i64 = sql
        .query_row("SELECT count(*) FROM \"commit\"", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
    sql.execute_batch("DROP TRIGGER reject_test_receipt;")
        .unwrap();
}

#[test]
fn legacy_store_stays_legacy_and_unknown_definition_is_refused() {
    let temp = Temp::new("checkpoint-legacy");
    let path = temp.join("db");
    drop(create_durable(&path));
    let sql = rusqlite::Connection::open(&path).unwrap();
    let source: String = sql
        .query_row("SELECT schema_source FROM history_meta", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert!(source.ends_with(RECEIPTS));
    sql.execute_batch("DROP TABLE checkpoint_receipt;").unwrap();
    let legacy = source.strip_suffix(RECEIPTS).unwrap();
    sql.execute(
        "UPDATE history_meta SET schema_source=?1,schema_definition=?2",
        rusqlite::params![legacy, schema_definition(&sql)],
    )
    .unwrap();
    let original_definition = schema_definition(&sql);
    drop(sql);

    let handles = Handles::open_writable(config(&path), BINDING, [71; 32]).unwrap();
    let c = &handles.history;
    assert_eq!(
        c.require_checkpoint_receipts(),
        Err(HistoryError::Unsupported("checkpoint receipt"))
    );
    assert_eq!(
        c.checkpoint_receipt(attempt(1)),
        Err(HistoryError::Unsupported("checkpoint receipt"))
    );
    let initial = make_branch(c);
    assert_eq!(
        c.commit_checkpoint(attempt(1), &request(&initial, root(1))),
        Err(HistoryError::Unsupported("checkpoint receipt"))
    );
    assert_eq!(
        c.branch_snapshot(initial.branch.id).unwrap().unwrap(),
        initial
    );
    drop(handles);
    let sql = rusqlite::Connection::open(&path).unwrap();
    let after: String = sql
        .query_row("SELECT schema_source FROM history_meta", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(after, legacy);
    assert_eq!(schema_definition(&sql), original_definition);
    // Keep the same table and index names but forge both the actual index
    // definition and its stored projection. A mere table-list check or a
    // comparison against the self-reported definition would accept this.
    sql.execute_batch(
        "DROP INDEX layers_child; CREATE UNIQUE INDEX layers_child ON layer(parent_layer_id);",
    )
    .unwrap();
    let forged = schema_definition(&sql);
    sql.execute("UPDATE history_meta SET schema_definition=?1", [forged])
        .unwrap();
    drop(sql);
    assert!(Handles::open_read_only(config(&path), BINDING, [71; 32]).is_err());
}

#[cfg(target_os = "linux")]
fn reconstructed_legacy(path: &std::path::Path) -> BranchSnapshot {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(
        path.parent().unwrap(),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    let handles = create_durable(path);
    let initial = make_branch(&handles.history);
    handles
        .history
        .commit_branch(&request(&initial, root(1)))
        .unwrap();
    let before = handles
        .history
        .branch_snapshot(initial.branch.id)
        .unwrap()
        .unwrap();
    drop(handles);
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let sql = rusqlite::Connection::open(path).unwrap();
    let current: String = sql
        .query_row("SELECT schema_source FROM history_meta", [], |row| {
            row.get(0)
        })
        .unwrap();
    sql.execute_batch("DROP TABLE checkpoint_receipt").unwrap();
    sql.execute(
        "UPDATE history_meta SET schema_source=?1,schema_definition=?2 WHERE id=1",
        rusqlite::params![
            current.strip_suffix(RECEIPTS).unwrap(),
            schema_definition(&sql)
        ],
    )
    .unwrap();
    drop(sql);
    before
}

#[cfg(target_os = "linux")]
#[test]
fn receipt_upgrade_requires_original_file_and_preserves_history() {
    let temp = Temp::new("checkpoint-upgrade-identity");
    let path = temp.join("db");
    let before = reconstructed_legacy(&path);
    let other = temp.join("other");
    std::fs::write(&other, b"different file").unwrap();
    let error = PreparedCheckpointUpgrade::prepare(
        config(&path),
        std::fs::File::open(&other).unwrap(),
        BINDING,
        7,
    )
    .err()
    .unwrap();
    assert_eq!(
        error.cause(),
        &layerfs_storage::port::PersistenceError::Malformed
    );
    drop(error);
    let original = std::fs::File::open(&path).unwrap();
    let error = PreparedCheckpointUpgrade::prepare(config(&path), original, BINDING, 8)
        .err()
        .unwrap();
    assert_eq!(
        error.cause(),
        &layerfs_storage::port::PersistenceError::Malformed
    );
    let original = error.release_original().unwrap();
    let legacy = Handles::open_read_only(config(&path), BINDING, [71; 32]).unwrap();
    assert_eq!(
        legacy.history.require_checkpoint_receipts(),
        Err(HistoryError::Unsupported("checkpoint receipt"))
    );
    assert_eq!(
        legacy.history.checkpoint_receipt(attempt(1)),
        Err(HistoryError::Unsupported("checkpoint receipt"))
    );
    assert_eq!(
        legacy
            .history
            .branch_snapshot(before.branch.id)
            .unwrap()
            .unwrap(),
        before
    );
    drop(legacy);

    let prepared = PreparedCheckpointUpgrade::prepare(config(&path), original, BINDING, 7).unwrap();
    let upgraded = prepared.apply().unwrap();
    assert!(upgraded.applied);
    let handles = Handles::open_writable(config(&path), BINDING, [71; 32]).unwrap();
    handles.history.require_checkpoint_receipts().unwrap();
    assert_eq!(
        handles
            .history
            .branch_snapshot(before.branch.id)
            .unwrap()
            .unwrap(),
        before
    );
    let input = request(&before, root(2));
    let outcome = handles
        .history
        .commit_checkpoint(attempt(1), &input)
        .unwrap();
    let after = handles
        .history
        .branch_snapshot(before.branch.id)
        .unwrap()
        .unwrap();
    drop(handles);
    let reopened = Handles::open_read_only(config(&path), BINDING, [71; 32]).unwrap();
    let receipt = reopened
        .history
        .checkpoint_receipt(attempt(1))
        .unwrap()
        .unwrap();
    assert_eq!(receipt.outcome, outcome);
    assert_eq!(
        receipt.request_digest,
        checkpoint_request_digest(&encode_checkpoint_request(&input).unwrap())
    );
    assert_eq!(
        reopened
            .history
            .branch_snapshot(before.branch.id)
            .unwrap()
            .unwrap(),
        after
    );
    drop(reopened);
    let already = PreparedCheckpointUpgrade::prepare(config(&path), upgraded.original, BINDING, 7)
        .unwrap()
        .apply()
        .unwrap();
    assert!(!already.applied);
}

#[cfg(target_os = "linux")]
mod receipt_upgrade_fault {
    use super::*;
    use rusqlite::{auto_extension, ffi, Connection};
    use std::{
        ffi::{c_char, c_int, c_void, CStr},
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
    };

    static WATCHED: Mutex<Option<PathBuf>> = Mutex::new(None);
    static INSTALLED: AtomicUsize = AtomicUsize::new(0);
    static FIRED: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn trace(_: u32, _: *mut c_void, _: *mut c_void, sql: *mut c_void) -> c_int {
        if !sql.is_null() {
            // SAFETY: SQLite supplies a NUL-terminated statement string for this trace event.
            let value = unsafe { CStr::from_ptr(sql.cast::<c_char>()) }.to_bytes();
            if value
                .windows(b"injected receipt migration failure".len())
                .any(|w| w == b"injected receipt migration failure")
            {
                FIRED.fetch_add(1, Ordering::Relaxed);
            }
        }
        0
    }

    fn install(connection: Connection) -> rusqlite::Result<()> {
        let watched = WATCHED.lock().unwrap();
        if connection.path().map(std::path::Path::new) != watched.as_deref() {
            return Ok(());
        }
        // The product will set this same value during profile setup. Choosing
        // it now prevents that setup from clearing the temporary trigger.
        connection.execute_batch("PRAGMA temp_store=2; CREATE TEMP TRIGGER reject_receipt_metadata BEFORE UPDATE OF schema_source ON main.history_meta WHEN EXISTS(SELECT 1 FROM main.sqlite_schema WHERE name='checkpoint_receipt' AND type='table') BEGIN SELECT RAISE(ABORT,'injected receipt migration failure'); END;")?;
        // SAFETY: the static callback outlives the underlying connection.
        let code = unsafe {
            ffi::sqlite3_trace_v2(
                connection.handle(),
                ffi::SQLITE_TRACE_STMT,
                Some(trace),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(code, ffi::SQLITE_OK);
        INSTALLED.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    unsafe extern "C" fn opened(
        db: *mut ffi::sqlite3,
        error: *mut *mut c_char,
        _: *const ffi::sqlite3_api_routines,
    ) -> c_int {
        // SAFETY: SQLite invokes the registered callback with its own open connection.
        unsafe { auto_extension::init_auto_extension(db, error, install) }
    }

    #[test]
    fn receipt_ddl_rolls_back_if_metadata_update_fails() {
        let temp = Temp::new("checkpoint-upgrade-rollback");
        let path = temp.join("db");
        let before = reconstructed_legacy(&path);
        *WATCHED.lock().unwrap() = Some(path.clone());
        // SAFETY: the callback neither opens a connection nor closes SQLite's handle.
        unsafe { auto_extension::register_auto_extension(opened).unwrap() };
        let original = std::fs::File::open(&path).unwrap();
        let prepared =
            PreparedCheckpointUpgrade::prepare(config(&path), original, BINDING, 7).unwrap();
        assert_eq!(INSTALLED.load(Ordering::Relaxed), 1);
        assert!(auto_extension::cancel_auto_extension(opened));
        let error = prepared.apply().err().unwrap();
        assert_eq!(
            error.cause(),
            &layerfs_storage::port::PersistenceError::Malformed
        );
        assert!(
            FIRED.load(Ordering::Relaxed) > 0,
            "the injected post-DDL trigger did not run"
        );
        let original = error.release_original().unwrap();
        let sql =
            Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let present: i64 = sql
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name='checkpoint_receipt'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(present, 0);
        drop(sql);
        let legacy = Handles::open_read_only(config(&path), BINDING, [71; 32]).unwrap();
        assert_eq!(
            legacy.history.checkpoint_receipt(attempt(1)),
            Err(HistoryError::Unsupported("checkpoint receipt"))
        );
        assert_eq!(
            legacy
                .history
                .branch_snapshot(before.branch.id)
                .unwrap()
                .unwrap(),
            before
        );
        drop(legacy);
        let success = PreparedCheckpointUpgrade::prepare(config(&path), original, BINDING, 7)
            .unwrap()
            .apply()
            .unwrap();
        assert!(success.applied);
        drop(success);
        *WATCHED.lock().unwrap() = None;
    }
}

#[test]
fn durable_receipts_open_with_every_declared_layout_and_acquisition_shape() {
    for layout in [
        SqlitePackLayout::Monolithic,
        SqlitePackLayout::GroupRows,
        SqlitePackLayout::GroupRowsIndexed,
    ] {
        for acquisition in [
            SqliteAcquisitionSchema::Absent,
            SqliteAcquisitionSchema::Tables,
        ] {
            let temp = Temp::new("checkpoint-layout");
            let path = temp.join("db");
            let selected = config(&path)
                .with_sqlite_pack_layout(layout)
                .with_sqlite_acquisition(acquisition);
            let handles = Handles::create(
                selected.clone(),
                StoragePolicy::frozen_default(),
                &HistoryCatalogConfig {
                    cursor_key: [71; 32],
                    binding_key: BINDING.to_vec(),
                    incarnation: 7,
                },
            )
            .unwrap();
            handles.history.require_checkpoint_receipts().unwrap();
            drop(handles);
            let reopened = Handles::open_writable(selected, BINDING, [71; 32]).unwrap();
            reopened.history.require_checkpoint_receipts().unwrap();
            assert_eq!(
                reopened.history.checkpoint_receipt(attempt(1)).unwrap(),
                None
            );
        }
    }
}
