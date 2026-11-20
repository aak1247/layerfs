//! Original, transaction-bound sources of durable serial retirement work.
//!
//! These records preserve facts for later authenticated recovery. They do not
//! themselves authorize recovering an old Workspace or retiring a layer.
use crate::{
    lifetime::orphan::{Orphan, ReleasedLayer},
    maintenance::SERIAL_RETIRE,
    Overlay, OverlayError, OverlayResult, StatementKind, WorkspaceState,
};

const MAGIC: &[u8; 5] = b"LFSR\x01";
const ORIGIN_BYTES: usize = 134;

#[cfg(target_os = "linux")]
const INSERT_TRIGGER: &str = "CREATE TRIGGER durable_serial_retirement_insert \
 BEFORE INSERT ON cleanup_task WHEN NEW.kind=7 AND length(NEW.name)<>134 \
 BEGIN SELECT RAISE(ABORT,'serial retirement origin is required'); END";
#[cfg(target_os = "linux")]
const IDENTITY_TRIGGER: &str = "CREATE TRIGGER durable_serial_retirement_identity \
 BEFORE UPDATE OF kind,resource,target,name ON cleanup_task \
 WHEN (OLD.kind=7 OR NEW.kind=7) AND (NEW.kind IS NOT OLD.kind OR \
 NEW.resource IS NOT OLD.resource OR NEW.target IS NOT OLD.target OR NEW.name IS NOT OLD.name) \
 BEGIN SELECT RAISE(ABORT,'serial retirement origin is immutable'); END";

#[cfg(target_os = "linux")]
pub(super) fn initialize(db: &Overlay) -> OverlayResult<()> {
    for sql in [INSERT_TRIGGER, IDENTITY_TRIGGER] {
        db.execute(StatementKind::Startup, sql, &[], 0)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(super) fn verify_schema(db: &Overlay) -> OverlayResult<()> {
    for (name, expected) in [
        ("durable_serial_retirement_insert", INSERT_TRIGGER),
        ("durable_serial_retirement_identity", IDENTITY_TRIGGER),
    ] {
        let rows: Vec<(String, String)> = db.query(
            StatementKind::Startup,
            "SELECT tbl_name,sql FROM sqlite_schema WHERE type='trigger' AND name=?1",
            &[&name],
            64 + expected.len() as u64,
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if rows.len() != 1 || rows[0].0 != "cleanup_task" || rows[0].1 != expected {
            return Err(OverlayError::Invalid(
                "durable serial retirement schema identity",
            ));
        }
    }
    Ok(())
}

/// Facts observed before either the last unlink or an orphan layer release.
#[derive(Clone, Copy)]
struct Origin {
    kind: u8,
    root: [u8; 32],
    floor: i64,
    top_before: i64,
    top_after: i64,
    active: i64,
    revision: i64,
}

fn enqueue(db: &Overlay, serial: i64, generation: i64, origin: Origin) -> OverlayResult<()> {
    if db.durable.is_none() || !db.profile.has_serial_retirement_origin() {
        return db.enqueue(SERIAL_RETIRE, serial, generation);
    }
    if serial <= 0
        || generation <= 0
        || origin.floor < 0
        || origin.active <= 0
        || origin.floor >= origin.active
        || origin.revision < 0
        || origin.top_before > origin.active
        || origin.top_after < origin.floor
        || match origin.kind {
            1 => {
                generation != origin.active
                    || origin.top_before != origin.active
                    || origin.top_after != origin.active
            }
            2 => {
                generation > origin.top_before
                    || generation <= origin.floor
                    || generation.checked_sub(1) != Some(origin.top_after)
            }
            _ => true,
        }
    {
        return Err(OverlayError::Invalid("serial retirement origin"));
    }
    let workspace: Vec<Vec<u8>> = db.query(
        StatementKind::Frontier,
        "SELECT identity FROM durable_identity WHERE id=1",
        &[],
        32,
        |row| row.get(0),
    )?;
    let [workspace] = workspace.as_slice() else {
        return Err(OverlayError::Invalid("serial retirement workspace"));
    };
    if workspace.len() != 32 {
        return Err(OverlayError::Invalid("serial retirement workspace"));
    }
    let namespace = db.durable_namespace()?.unwrap_or(0);
    let mut bytes = Vec::with_capacity(ORIGIN_BYTES);
    bytes.extend_from_slice(MAGIC);
    bytes.push(origin.kind);
    bytes.extend_from_slice(workspace);
    bytes.extend_from_slice(&origin.root);
    for value in [
        namespace,
        serial,
        generation,
        origin.floor,
        origin.top_before,
        origin.top_after,
        origin.active,
        origin.revision,
    ] {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    debug_assert_eq!(bytes.len(), ORIGIN_BYTES);
    let changed = db.execute(
        StatementKind::Reclaim,
        "INSERT INTO cleanup_task(kind,resource,target,name) VALUES(7,?1,?2,?3) \
         ON CONFLICT(kind,resource,target) DO UPDATE SET name=excluded.name \
         WHERE cleanup_task.name=excluded.name",
        &[&serial, &generation, &bytes],
        ORIGIN_BYTES as u64 + 16,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("serial retirement origin changed"));
    }
    db.maintenance_ready.set(true);
    Ok(())
}

/// Last unlink without any remaining open, lookup, or read owner.
pub(crate) fn last_unlink(db: &Overlay, serial: i64, state: &WorkspaceState) -> OverlayResult<()> {
    enqueue(
        db,
        serial,
        state.active.0,
        Origin {
            kind: 1,
            root: state.base_root,
            floor: state.installed,
            top_before: state.active.0,
            top_after: state.active.0,
            active: state.active.0,
            revision: state.revision,
        },
    )
}

/// A lower layer whose original orphan owner advanced in this transaction.
pub(crate) fn released(db: &Overlay, serial: i64, layer: ReleasedLayer) -> OverlayResult<()> {
    let Orphan { root, top, floor } = layer.before;
    enqueue(
        db,
        serial,
        layer.generation,
        Origin {
            kind: 2,
            root,
            floor,
            top_before: top,
            top_after: layer.after,
            active: layer.active,
            revision: layer.revision,
        },
    )
}
