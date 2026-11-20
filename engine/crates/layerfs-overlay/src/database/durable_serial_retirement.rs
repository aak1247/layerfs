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

impl Origin {
    fn validate(&self, serial: i64, generation: i64) -> OverlayResult<()> {
        if serial <= 0
            || generation <= 0
            || self.floor < 0
            || self.active <= 0
            || self.floor >= self.active
            || self.revision < 0
            || self.top_before > self.active
            || self.top_after < self.floor
            || match self.kind {
                1 => {
                    generation != self.active
                        || self.top_before != self.active
                        || self.top_after != self.active
                }
                2 => {
                    generation > self.top_before
                        || generation <= self.floor
                        || generation.checked_sub(1) != Some(self.top_after)
                }
                _ => true,
            }
        {
            return Err(OverlayError::Invalid("serial retirement origin"));
        }
        Ok(())
    }
}

/// Decoded original queue identity, not a Store authorization.
pub(crate) struct DecodedOrigin {
    pub kind: u8,
    pub workspace: [u8; 32],
    pub namespace: i64,
    pub serial: i64,
    pub generation: i64,
    pub root: [u8; 32],
    pub active: i64,
    pub revision: i64,
    pub floor: i64,
    pub top_after: i64,
}

pub(crate) fn decode(bytes: &[u8]) -> OverlayResult<DecodedOrigin> {
    if bytes.len() != ORIGIN_BYTES || &bytes[..5] != MAGIC {
        return Err(OverlayError::Invalid("serial retirement encoding"));
    }
    let array = |offset: usize| -> OverlayResult<[u8; 32]> {
        bytes[offset..offset + 32]
            .try_into()
            .map_err(|_| OverlayError::Invalid("serial retirement encoding"))
    };
    let integer = |offset: usize| -> OverlayResult<i64> {
        Ok(i64::from_be_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .map_err(|_| OverlayError::Invalid("serial retirement encoding"))?,
        ))
    };
    let serial = integer(78)?;
    let generation = integer(86)?;
    let origin = Origin {
        kind: bytes[5],
        root: array(38)?,
        floor: integer(94)?,
        top_before: integer(102)?,
        top_after: integer(110)?,
        active: integer(118)?,
        revision: integer(126)?,
    };
    origin.validate(serial, generation)?;
    let namespace = integer(70)?;
    let workspace = array(6)?;
    if namespace < 0 || workspace == [0; 32] {
        return Err(OverlayError::Invalid("serial retirement identity"));
    }
    Ok(DecodedOrigin {
        kind: origin.kind,
        workspace,
        namespace,
        serial,
        generation,
        root: origin.root,
        active: origin.active,
        revision: origin.revision,
        floor: origin.floor,
        top_after: origin.top_after,
    })
}

fn enqueue(db: &Overlay, serial: i64, generation: i64, origin: Origin) -> OverlayResult<()> {
    if db.durable.is_none() || !db.profile.has_serial_retirement_origin() {
        return db.enqueue(SERIAL_RETIRE, serial, generation);
    }
    origin.validate(serial, generation)?;
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
