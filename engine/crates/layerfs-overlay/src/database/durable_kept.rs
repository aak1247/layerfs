//! Transactional custody for live rows carried across a durable capture.
use crate::{Overlay, OverlayError, OverlayResult, StatementKind};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct UnsafeCustody {
    pub orphan: bool,
    pub operation: bool,
}

pub(super) fn custody(db: &Overlay) -> UnsafeCustody {
    UnsafeCustody {
        orphan: db.orphan_seen.get(),
        operation: db.open.borrow().operations.len() != 0,
    }
}

pub(super) fn save_custody(db: &Overlay, value: UnsafeCustody) -> OverlayResult<()> {
    let changed = db.execute(
        StatementKind::Frontier,
        "UPDATE durable_custody SET orphan_owners=?1,operation_owners=?2 WHERE id=1",
        &[&value.orphan, &value.operation],
        2,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("durable custody record is absent"));
    }
    Ok(())
}

/// Called in the same atomic job as the live inode and its journaled owner.
pub(crate) fn record(db: &Overlay, serial: i64, generation: i64) -> OverlayResult<()> {
    if db.durable.is_some() && db.profile.has_classified_custody() {
        db.execute(
            StatementKind::Frontier,
            "INSERT INTO durable_kept_live(serial,generation) VALUES(?1,?2)",
            &[&serial, &generation],
            16,
        )?;
        let changed = db.execute(
            StatementKind::Frontier,
            "UPDATE durable_custody SET kept_count=kept_count+1 WHERE id=1",
            &[],
            8,
        )?;
        if changed != 1 {
            return Err(OverlayError::Invalid("durable kept count is absent"));
        }
    }
    Ok(())
}

/// A writer takes over the carried row in the same atomic job.
pub(crate) fn forget(db: &Overlay, serial: i64, generation: i64) -> OverlayResult<()> {
    if db.durable.is_some() && db.profile.has_classified_custody() {
        let changed = db.execute(
            StatementKind::Frontier,
            "DELETE FROM durable_kept_live WHERE serial=?1 AND generation=?2",
            &[&serial, &generation],
            16,
        )?;
        if changed != 1 {
            return Err(OverlayError::Invalid("durable kept row changed"));
        }
        let changed = db.execute(
            StatementKind::Frontier,
            "UPDATE durable_custody SET kept_count=kept_count-1 WHERE id=1 AND kept_count>0",
            &[],
            8,
        )?;
        if changed != 1 {
            return Err(OverlayError::Invalid("durable kept count changed"));
        }
    }
    Ok(())
}

/// Capture transfers all carried rows from this active generation together.
pub(crate) fn clear(db: &Overlay, generation: i64, expected: usize) -> OverlayResult<()> {
    if db.durable.is_some() && db.profile.has_classified_custody() {
        let changed = db.execute(
            StatementKind::Frontier,
            "DELETE FROM durable_kept_live WHERE generation=?1",
            &[&generation],
            8,
        )?;
        if usize::try_from(changed).ok() != Some(expected) {
            return Err(OverlayError::Invalid("durable kept generation changed"));
        }
        let expected = i64::try_from(expected)
            .map_err(|_| OverlayError::Invalid("durable kept count overflow"))?;
        let changed = db.execute(
            StatementKind::Frontier,
            "UPDATE durable_custody SET kept_count=0 WHERE id=1 AND kept_count=?1",
            &[&expected],
            8,
        )?;
        if changed != 1 {
            return Err(OverlayError::Invalid("durable kept count changed"));
        }
    }
    Ok(())
}

/// Reconstruct only authenticated active rows, in bounded indexed pages.
#[cfg(target_os = "linux")]
pub(super) fn restore(
    db: &Overlay,
    generation: i64,
    bound: bool,
    expected: u64,
) -> OverlayResult<()> {
    let mut after = 0_i64;
    let mut count = 0_u64;
    loop {
        let rows: Vec<(i64, i64, bool)> = db.query(
            StatementKind::Frontier,
            "SELECT k.serial,k.generation,EXISTS(SELECT 1 FROM fs_inode i \
             WHERE i.inode=k.serial AND i.generation=k.generation) \
             FROM durable_kept_live k WHERE k.serial>?1 ORDER BY k.serial LIMIT 128",
            &[&after],
            16,
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if rows.is_empty() {
            if count != expected {
                return Err(OverlayError::Invalid(
                    "durable kept count requires reconciliation",
                ));
            }
            return Ok(());
        }
        for (serial, row_generation, inode_exists) in rows {
            if !bound || serial <= after || row_generation != generation || !inode_exists {
                return Err(OverlayError::Invalid(
                    "durable kept row requires reconciliation",
                ));
            }
            if db
                .open
                .borrow_mut()
                .kept_live
                .rows
                .insert(serial, ())
                .is_some()
            {
                return Err(OverlayError::Invalid("durable kept row is duplicated"));
            }
            after = serial;
            count = count
                .checked_add(1)
                .ok_or(OverlayError::Invalid("durable kept count overflow"))?;
        }
    }
}
