//! Schema 44's local-conflict description and bounded maintenance backing.
//! No conflict admission or recovery is inferred from these records alone.
#[cfg(target_os = "linux")]
use crate::StatementKind;
use crate::{Overlay, OverlayError, OverlayResult};

#[cfg(target_os = "linux")]
pub(super) const BINDING_SCHEMA: &str = "CREATE TABLE durable_binding(id INTEGER PRIMARY KEY CHECK(id=1), \
 root BLOB NOT NULL CHECK(length(root)=32), \
 snapshot BLOB NOT NULL CHECK(length(snapshot)>0 AND length(snapshot)<=4096), \
 recoverable INTEGER NOT NULL CHECK(recoverable IN(0,1)), \
 conflict_generation INTEGER CHECK(conflict_generation>0), conflict_digest BLOB, \
 CHECK((conflict_generation IS NULL AND conflict_digest IS NULL) OR \
   (conflict_generation IS NOT NULL AND conflict_digest IS NOT NULL AND length(conflict_digest)=32))) STRICT";

#[cfg(target_os = "linux")]
const CLEANUP_SCHEMA: &str = "CREATE TABLE cleanup_task (\
 kind INTEGER NOT NULL CHECK(kind BETWEEN 1 AND 13), \
 resource INTEGER NOT NULL CHECK(resource>=0), target INTEGER NOT NULL CHECK(target<>0), \
 phase INTEGER NOT NULL DEFAULT 0 CHECK(phase>=0), cursor INTEGER NOT NULL DEFAULT 0 CHECK(cursor>=-1), \
 aux INTEGER NOT NULL DEFAULT -1 CHECK(aux>=-1), name BLOB NOT NULL DEFAULT X'' CHECK(length(name)<=255), \
 ready INTEGER NOT NULL DEFAULT 1 CHECK(ready IN(0,1)), \
 CHECK(kind<>13 OR (resource=0 AND target>0)), PRIMARY KEY(kind,resource,target)\
 ) STRICT, WITHOUT ROWID";
#[cfg(target_os = "linux")]
const CLEANUP_READY: &str =
    "CREATE INDEX cleanup_task_ready ON cleanup_task(ready,kind,resource,target)";
#[cfg(target_os = "linux")]
const CLEANUP_GENERATION: &str =
    "CREATE INDEX cleanup_task_generation ON cleanup_task(target,kind,resource)";
#[cfg(target_os = "linux")]
const PLAN_SCHEMA: &str = "CREATE TABLE durable_conflict_plan(\
 generation INTEGER PRIMARY KEY CHECK(generation>0), \
 attempt BLOB NOT NULL UNIQUE CHECK(length(attempt)=32), \
 workspace BLOB NOT NULL CHECK(length(workspace)=32), \
 catalog BLOB NOT NULL CHECK(length(catalog)=32), incarnation INTEGER NOT NULL CHECK(incarnation>0), \
 namespace INTEGER NOT NULL CHECK(namespace>0), local_root BLOB NOT NULL CHECK(length(local_root)=32), \
 target_snapshot BLOB NOT NULL CHECK(length(target_snapshot)>0 AND length(target_snapshot)<=4096), \
 paths_count INTEGER NOT NULL CHECK(paths_count>0), byte_count INTEGER NOT NULL CHECK(byte_count>0), \
 page_count INTEGER NOT NULL CHECK(page_count>0 AND page_count=1+(byte_count-1)/16384), final_digest BLOB NOT NULL CHECK(length(final_digest)=32), \
 descriptor_digest BLOB NOT NULL CHECK(length(descriptor_digest)=32), \
 next_page INTEGER NOT NULL CHECK(next_page>=0 AND next_page<=page_count), \
 written_bytes INTEGER NOT NULL CHECK(written_bytes>=0 AND written_bytes<=byte_count), \
 chain BLOB NOT NULL CHECK(length(chain)=32), state INTEGER NOT NULL CHECK(state IN(0,1,2)), \
 CHECK(state<>1 OR (next_page=page_count AND written_bytes=byte_count AND chain=final_digest))\
 ) STRICT";
#[cfg(target_os = "linux")]
const PLAN_ACTIVE: &str =
    "CREATE UNIQUE INDEX durable_conflict_active ON durable_conflict_plan(state) WHERE state=1";
#[cfg(target_os = "linux")]
const PAGE_SCHEMA: &str = "CREATE TABLE durable_conflict_page(\
 generation INTEGER NOT NULL REFERENCES durable_conflict_plan(generation), \
 page_no INTEGER NOT NULL CHECK(page_no>=0), \
 data BLOB NOT NULL CHECK(length(data)>0 AND length(data)<=16384), \
 PRIMARY KEY(generation,page_no)) STRICT, WITHOUT ROWID";

#[cfg(target_os = "linux")]
const PLAN_IDENTITY: &str = "CREATE TRIGGER durable_conflict_plan_identity \
 BEFORE UPDATE OF generation,attempt,workspace,catalog,incarnation,namespace,local_root,target_snapshot,paths_count,byte_count,page_count,final_digest,descriptor_digest ON durable_conflict_plan \
 BEGIN SELECT RAISE(ABORT,'conflict descriptor is immutable'); END";
#[cfg(target_os = "linux")]
const PLAN_INSERT: &str = "CREATE TRIGGER durable_conflict_plan_insert \
 BEFORE INSERT ON durable_conflict_plan \
 WHEN NEW.state<>0 OR NEW.next_page<>0 OR NEW.written_bytes<>0 OR NEW.chain<>zeroblob(32) \
 BEGIN SELECT RAISE(ABORT,'invalid conflict plan predecessor'); END";
#[cfg(target_os = "linux")]
const PLAN_STATE: &str = "CREATE TRIGGER durable_conflict_plan_state \
 BEFORE UPDATE OF state ON durable_conflict_plan \
 WHEN NOT ((OLD.state=0 AND NEW.state IN(0,1,2)) OR (OLD.state=1 AND NEW.state IN(1,2)) OR (OLD.state=2 AND NEW.state=2)) \
 OR (NEW.state=2 AND EXISTS(SELECT 1 FROM durable_binding WHERE conflict_generation=OLD.generation)) \
 BEGIN SELECT RAISE(ABORT,'invalid conflict state transition'); END";
#[cfg(target_os = "linux")]
const PLAN_PROGRESS: &str = "CREATE TRIGGER durable_conflict_plan_progress \
 BEFORE UPDATE OF next_page,written_bytes,chain ON durable_conflict_plan \
 WHEN OLD.state<>0 OR NEW.next_page<>OLD.next_page+1 OR NEW.written_bytes<=OLD.written_bytes \
 OR NOT EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation=OLD.generation \
 AND page_no=OLD.next_page AND length(data)=NEW.written_bytes-OLD.written_bytes) \
 BEGIN SELECT RAISE(ABORT,'invalid conflict page progress'); END";
#[cfg(target_os = "linux")]
const PLAN_DELETE: &str = "CREATE TRIGGER durable_conflict_plan_delete \
 BEFORE DELETE ON durable_conflict_plan \
 WHEN OLD.state<>2 OR EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation=OLD.generation) \
 OR EXISTS(SELECT 1 FROM durable_binding WHERE conflict_generation=OLD.generation) \
 BEGIN SELECT RAISE(ABORT,'conflict plan is still retained'); END";
#[cfg(target_os = "linux")]
const PAGE_INSERT: &str = "CREATE TRIGGER durable_conflict_page_insert \
 BEFORE INSERT ON durable_conflict_page \
 WHEN NOT EXISTS(SELECT 1 FROM durable_conflict_plan WHERE generation=NEW.generation AND state=0 \
 AND next_page=NEW.page_no AND next_page<page_count AND length(NEW.data)= \
 CASE WHEN next_page+1<page_count THEN 16384 ELSE byte_count-written_bytes END) \
 BEGIN SELECT RAISE(ABORT,'invalid conflict page predecessor'); END";
#[cfg(target_os = "linux")]
const PAGE_UPDATE: &str = "CREATE TRIGGER durable_conflict_page_update \
 BEFORE UPDATE ON durable_conflict_page \
 BEGIN SELECT RAISE(ABORT,'conflict page is immutable'); END";
#[cfg(target_os = "linux")]
const PAGE_DELETE: &str = "CREATE TRIGGER durable_conflict_page_delete \
 BEFORE DELETE ON durable_conflict_page \
 WHEN NOT EXISTS(SELECT 1 FROM durable_conflict_plan WHERE generation=OLD.generation AND state=2) \
 BEGIN SELECT RAISE(ABORT,'conflict page is still retained'); END";
#[cfg(target_os = "linux")]
const BINDING_TRIGGER: &str = "CREATE TRIGGER durable_conflict_binding_identity \
 BEFORE UPDATE OF root,snapshot,conflict_generation,conflict_digest ON durable_binding \
 WHEN NEW.conflict_generation IS NOT NULL AND NOT EXISTS(SELECT 1 FROM durable_conflict_plan \
 WHERE generation=NEW.conflict_generation AND state=1 AND descriptor_digest=NEW.conflict_digest \
 AND local_root=NEW.root AND target_snapshot=NEW.snapshot) \
 BEGIN SELECT RAISE(ABORT,'conflict binding identity'); END";
#[cfg(target_os = "linux")]
const BINDING_INSERT: &str = "CREATE TRIGGER durable_conflict_binding_insert \
 BEFORE INSERT ON durable_binding \
 WHEN NEW.conflict_generation IS NOT NULL AND NOT EXISTS(SELECT 1 FROM durable_conflict_plan \
 WHERE generation=NEW.conflict_generation AND state=1 AND descriptor_digest=NEW.conflict_digest \
 AND local_root=NEW.root AND target_snapshot=NEW.snapshot) \
 BEGIN SELECT RAISE(ABORT,'conflict binding identity'); END";
#[cfg(target_os = "linux")]
const ATTEMPT_CONFLICT: &str = "CREATE TRIGGER durable_checkpoint_conflict_identity \
 BEFORE UPDATE OF conflict_generation ON durable_checkpoint_attempt \
 WHEN OLD.conflict_generation IS NOT NULL AND NEW.conflict_generation IS NOT OLD.conflict_generation \
 BEGIN SELECT RAISE(ABORT,'checkpoint conflict identity is immutable'); END";
#[cfg(target_os = "linux")]
const ATTEMPT_PROGRESS: &str = "CREATE TRIGGER durable_checkpoint_progress \
 BEFORE UPDATE OF reader_released,operation_released,operation_cleanup_completed,conflict_cleanup_completed ON durable_checkpoint_attempt \
 WHEN (NEW.reader_owner IS OLD.reader_owner AND OLD.reader_released=1 AND NEW.reader_released IS NOT 1) OR \
 (NEW.operation_owner IS OLD.operation_owner AND OLD.operation_released=1 AND NEW.operation_released IS NOT 1) OR \
 (NEW.operation_owner IS OLD.operation_owner AND OLD.operation_cleanup_completed=1 AND NEW.operation_cleanup_completed IS NOT 1) OR \
 (NEW.conflict_generation IS OLD.conflict_generation AND OLD.conflict_cleanup_completed=1 AND NEW.conflict_cleanup_completed IS NOT 1) \
 BEGIN SELECT RAISE(ABORT,'checkpoint progress is immutable'); END";
#[cfg(target_os = "linux")]
const ATTEMPT_PHASE: &str = "CREATE TRIGGER durable_checkpoint_phase \
 BEFORE UPDATE OF phase ON durable_checkpoint_attempt \
 WHEN NOT ((OLD.phase=1 AND NEW.phase IN(1,2,3,5)) OR (OLD.phase=2 AND NEW.phase IN(2,4,6)) \
 OR (OLD.phase IN(3,4,5,6) AND NEW.phase=OLD.phase)) \
 BEGIN SELECT RAISE(ABORT,'invalid checkpoint phase transition'); END";
#[cfg(target_os = "linux")]
const ATTEMPT_FROZEN: &str = "CREATE TRIGGER durable_checkpoint_frozen_request \
 BEFORE UPDATE OF request,request_digest,candidate_root,target_snapshot ON durable_checkpoint_attempt \
 WHEN OLD.phase IN(2,4,6) AND (NEW.request IS NOT OLD.request OR NEW.request_digest IS NOT OLD.request_digest \
 OR NEW.candidate_root IS NOT OLD.candidate_root OR NEW.target_snapshot IS NOT OLD.target_snapshot) \
 BEGIN SELECT RAISE(ABORT,'checkpoint request is immutable'); END";

#[cfg(target_os = "linux")]
const OBJECTS: &[(&str, &str, &str, &str)] = &[
    (
        "table",
        "durable_binding",
        "durable_binding",
        BINDING_SCHEMA,
    ),
    (
        "table",
        "durable_checkpoint_attempt",
        "durable_checkpoint_attempt",
        super::durable_attempt::CHECKPOINT_SCHEMA,
    ),
    (
        "trigger",
        "durable_checkpoint_attempt_owner_immutable",
        "durable_checkpoint_attempt",
        super::durable_attempt::CHECKPOINT_TRIGGER,
    ),
    ("table", "cleanup_task", "cleanup_task", CLEANUP_SCHEMA),
    ("index", "cleanup_task_ready", "cleanup_task", CLEANUP_READY),
    (
        "index",
        "cleanup_task_generation",
        "cleanup_task",
        CLEANUP_GENERATION,
    ),
    (
        "table",
        "durable_conflict_plan",
        "durable_conflict_plan",
        PLAN_SCHEMA,
    ),
    (
        "index",
        "durable_conflict_active",
        "durable_conflict_plan",
        PLAN_ACTIVE,
    ),
    (
        "table",
        "durable_conflict_page",
        "durable_conflict_page",
        PAGE_SCHEMA,
    ),
    (
        "trigger",
        "durable_conflict_plan_identity",
        "durable_conflict_plan",
        PLAN_IDENTITY,
    ),
    (
        "trigger",
        "durable_conflict_plan_insert",
        "durable_conflict_plan",
        PLAN_INSERT,
    ),
    (
        "trigger",
        "durable_conflict_plan_state",
        "durable_conflict_plan",
        PLAN_STATE,
    ),
    (
        "trigger",
        "durable_conflict_plan_progress",
        "durable_conflict_plan",
        PLAN_PROGRESS,
    ),
    (
        "trigger",
        "durable_conflict_plan_delete",
        "durable_conflict_plan",
        PLAN_DELETE,
    ),
    (
        "trigger",
        "durable_conflict_page_insert",
        "durable_conflict_page",
        PAGE_INSERT,
    ),
    (
        "trigger",
        "durable_conflict_page_update",
        "durable_conflict_page",
        PAGE_UPDATE,
    ),
    (
        "trigger",
        "durable_conflict_page_delete",
        "durable_conflict_page",
        PAGE_DELETE,
    ),
    (
        "trigger",
        "durable_conflict_binding_identity",
        "durable_binding",
        BINDING_TRIGGER,
    ),
    (
        "trigger",
        "durable_conflict_binding_insert",
        "durable_binding",
        BINDING_INSERT,
    ),
    (
        "trigger",
        "durable_checkpoint_conflict_identity",
        "durable_checkpoint_attempt",
        ATTEMPT_CONFLICT,
    ),
    (
        "trigger",
        "durable_checkpoint_progress",
        "durable_checkpoint_attempt",
        ATTEMPT_PROGRESS,
    ),
    (
        "trigger",
        "durable_checkpoint_phase",
        "durable_checkpoint_attempt",
        ATTEMPT_PHASE,
    ),
    (
        "trigger",
        "durable_checkpoint_frozen_request",
        "durable_checkpoint_attempt",
        ATTEMPT_FROZEN,
    ),
];

#[cfg(target_os = "linux")]
pub(super) fn initialize(db: &Overlay) -> OverlayResult<()> {
    // A new durable database has no cleanup work yet. Old databases retain
    // their original table and are never changed by ordinary recovery.
    db.execute(StatementKind::Startup, "DROP TABLE cleanup_task", &[], 0)?;
    for statement in [
        CLEANUP_SCHEMA,
        CLEANUP_READY,
        CLEANUP_GENERATION,
        PLAN_SCHEMA,
        PLAN_ACTIVE,
        PAGE_SCHEMA,
        PLAN_IDENTITY,
        PLAN_INSERT,
        PLAN_STATE,
        PLAN_PROGRESS,
        PLAN_DELETE,
        PAGE_INSERT,
        PAGE_UPDATE,
        PAGE_DELETE,
        BINDING_TRIGGER,
        BINDING_INSERT,
        ATTEMPT_CONFLICT,
        ATTEMPT_PROGRESS,
        ATTEMPT_PHASE,
        ATTEMPT_FROZEN,
    ] {
        db.execute(StatementKind::Startup, statement, &[], 0)?;
    }
    Ok(())
}

/// The version number and quick_check do not authenticate these required
/// constraints. Extra unrelated test or application objects are unaffected.
#[cfg(target_os = "linux")]
pub(super) fn verify_schema(db: &Overlay) -> OverlayResult<()> {
    for (kind, name, table, expected) in OBJECTS {
        let rows: Vec<(String, String)> = db.query(
            StatementKind::Startup,
            "SELECT tbl_name,sql FROM sqlite_schema WHERE type=?1 AND name=?2",
            &[kind, name],
            64 + expected.len() as u64,
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if rows.len() != 1 || rows[0].0 != *table || rows[0].1 != *expected {
            return Err(OverlayError::Invalid("durable conflict schema identity"));
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(super) fn verify_schema(_: &Overlay) -> OverlayResult<()> {
    Err(OverlayError::Invalid("durable conflict schema unavailable"))
}
