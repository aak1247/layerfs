//! Reconstruct the immediately previous empty schema for compatibility tests.
use std::path::Path;

pub fn convert(path: &Path) {
    let sql = rusqlite::Connection::open(path).unwrap();
    let schema: String = sql.query_row(
        "SELECT sql FROM sqlite_schema WHERE type='table' AND name='durable_checkpoint_attempt'",
        [], |row| row.get(0),
    ).unwrap();
    let mut old = schema
        .replace("original_pending_digest BLOB CHECK(original_pending_digest IS NULL OR length(original_pending_digest)=32), ", "")
        .replace("conflict_generation INTEGER, conflict_cleanup_completed INTEGER, ", "")
        .replace("phase IN(1,2,3,4,5,6)", "phase IN(1,2,3,4)")
        .replace("phase IN(3,6)", "phase=3")
        .replace("phase IN(2,6)", "phase=2")
        .replace("phase IN(4,5)", "phase=4")
        .replace("phase IN(1,3,5)", "phase IN(1,3)");
    let start = old.find("CHECK((conflict_generation IS NULL").unwrap();
    let end = start + old[start..].find("CHECK((phase IN(1,2)").unwrap();
    old.replace_range(start..end, "");
    assert_ne!(schema, old);
    assert!(!old.contains("original_pending_digest"));
    assert!(!old.contains("conflict_generation"));
    sql.execute_batch(
        "DROP TRIGGER durable_checkpoint_attempt_owner_immutable; DROP TABLE durable_checkpoint_attempt; \
         DROP TRIGGER durable_conflict_binding_identity; DROP TRIGGER durable_conflict_binding_insert; \
         DROP TABLE durable_conflict_page; DROP TABLE durable_conflict_plan; \
         ALTER TABLE durable_binding RENAME TO durable_binding_44; \
         CREATE TABLE durable_binding(id INTEGER PRIMARY KEY CHECK(id=1), root BLOB NOT NULL CHECK(length(root)=32), \
           snapshot BLOB NOT NULL CHECK(length(snapshot)>0 AND length(snapshot)<=4096), \
           recoverable INTEGER NOT NULL CHECK(recoverable IN(0,1))) STRICT; \
         INSERT INTO durable_binding SELECT id,root,snapshot,recoverable FROM durable_binding_44; \
         DROP TABLE durable_binding_44; DROP TABLE cleanup_task; \
         CREATE TABLE cleanup_task(kind INTEGER NOT NULL CHECK(kind BETWEEN 1 AND 12), \
           resource INTEGER NOT NULL CHECK(resource>=0), target INTEGER NOT NULL CHECK(target<>0), \
           phase INTEGER NOT NULL DEFAULT 0 CHECK(phase>=0), cursor INTEGER NOT NULL DEFAULT 0 CHECK(cursor>=-1), \
           aux INTEGER NOT NULL DEFAULT -1 CHECK(aux>=-1), name BLOB NOT NULL DEFAULT X'' CHECK(length(name)<=255), \
           ready INTEGER NOT NULL DEFAULT 1 CHECK(ready IN(0,1)), PRIMARY KEY(kind,resource,target)) STRICT, WITHOUT ROWID; \
         CREATE INDEX cleanup_task_ready ON cleanup_task(ready,kind,resource,target); \
         CREATE INDEX cleanup_task_generation ON cleanup_task(target,kind,resource);",
    ).unwrap();
    sql.execute_batch(&old).unwrap();
    sql.execute_batch(
        "CREATE TRIGGER durable_checkpoint_attempt_owner_immutable BEFORE UPDATE OF reader_owner,operation_owner ON durable_checkpoint_attempt \
         WHEN (OLD.reader_owner IS NOT NULL AND NEW.reader_owner IS NOT OLD.reader_owner) OR \
              (OLD.operation_owner IS NOT NULL AND NEW.operation_owner IS NOT OLD.operation_owner) \
         BEGIN SELECT RAISE(ABORT,'checkpoint owner identity is immutable'); END; PRAGMA user_version=43;",
    ).unwrap();
}
