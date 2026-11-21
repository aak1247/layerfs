//! Exact queue classification for a checkpoint and orphan under one lease.
use crate::{Overlay, OverlayError, OverlayResult, RecordedCapture, StatementKind};

type CheckpointProgress = (
    i64,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);
type QueueRow = (i64, i64, i64, i64, i64, i64, Vec<u8>, bool);

fn checkpoint_work_shape(item: &crate::maintenance::Item) -> bool {
    let start = item.cursor == 0 && item.aux == -1 && item.name.is_empty();
    match (item.kind, item.phase) {
        (1, 0) => {
            item.aux == -1
                && (start || (item.cursor > 0 && !item.name.is_empty() && item.name.len() <= 255))
        }
        (1, 1) => {
            item.name.is_empty()
                && item.cursor >= 0
                && (item.aux == -1 || (item.cursor > 0 && item.aux >= 0 && item.aux % 4096 == 0))
        }
        (3, 0 | 4) => {
            start
                || (item.cursor > 0
                    && item.aux >= 0
                    && item.aux % 4096 == 0
                    && item.name.is_empty())
        }
        (3, 1) => start || (item.cursor > 0 && item.aux > 0 && item.name.is_empty()),
        (3, 2) => {
            start
                || (item.cursor > 0
                    && item.aux == -1
                    && !item.name.is_empty()
                    && item.name.len() <= 255)
        }
        (3, 3) => start || (item.cursor > 0 && item.aux == -1 && item.name.is_empty()),
        (3, 5) => start,
        _ => false,
    }
}

pub(super) fn check_mixed_item(
    db: &Overlay,
    orphans: &super::durable_orphan_recovery::MixedOrphans,
    recorded: RecordedCapture,
    phase: i64,
    operation: Option<i64>,
    item: &crate::maintenance::Item,
    ready: bool,
) -> OverlayResult<()> {
    let generation = recorded.capture.generation.0;
    super::durable_attempt::check_context(db, recorded, false)?;
    let rows: Vec<CheckpointProgress> = db.query(
        StatementKind::Frontier,
        "SELECT phase,fold_completed,retire_completed,reader_released,operation_released,operation_cleanup_completed FROM durable_checkpoint_attempt WHERE id=1 AND attempt=?1 AND generation=?2",
        &[&recorded.attempt.as_slice(), &generation], 48,
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
    )?;
    let [(stored_phase, fold, retire, reader_released, operation_released, operation_completed)] =
        rows.as_slice()
    else {
        return Err(OverlayError::Invalid("mixed recovery checkpoint"));
    };
    if *stored_phase != phase {
        return Err(OverlayError::Invalid("mixed recovery checkpoint"));
    }
    match (item.kind, item.resource, item.target) {
        (1, 0, target) if target == generation => {
            if !matches!(phase, 3 | 6) || *fold != Some(0) || !checkpoint_work_shape(item) {
                return Err(OverlayError::Invalid("mixed recovery fold"));
            }
        }
        (3, 0, target) if target == generation => {
            if !matches!(phase, 3 | 4 | 6)
                || *retire != Some(0)
                || (phase != 4 && *fold != Some(1))
                || !checkpoint_work_shape(item)
            {
                return Err(OverlayError::Invalid("mixed recovery retirement"));
            }
        }
        (3, 0, _) => {
            if !checkpoint_work_shape(item) {
                return Err(OverlayError::Invalid("mixed recovery retirement"));
            }
            return super::durable_orphan_recovery::check_item(db, &orphans.observed, item, ready);
        }
        (6, owner, 2) if Some(owner) == operation => {
            if *operation_released != Some(1)
                || *operation_completed != Some(0)
                || !ready
                || item.phase != 0
                || item.cursor != 0
                || item.aux != -1
                || !item.name.is_empty()
            {
                return Err(OverlayError::Invalid("mixed recovery operation cleanup"));
            }
            return Ok(());
        }
        (8, 0, target) if target == generation => {
            if !matches!(phase, 3 | 4 | 6) {
                return Err(OverlayError::Invalid("mixed recovery orphan wake"));
            }
            return super::durable_orphan_recovery::check_item(db, &orphans.observed, item, ready);
        }
        (9, 0, target) if target == generation => {
            if *reader_released != Some(1) || db.generation_held(generation)? {
                return Err(OverlayError::Invalid("mixed recovery reader wake"));
            }
            return super::durable_orphan_recovery::check_item(db, &orphans.observed, item, ready);
        }
        _ => return super::durable_orphan_recovery::check_item(db, &orphans.observed, item, ready),
    }
    if !ready && !db.generation_held(generation)? {
        let wake: Vec<(i64, i64)> = db.query(
            StatementKind::Frontier,
            "SELECT cursor,aux FROM cleanup_task WHERE kind=9 AND resource=0 AND target=?1 AND ready=1 AND phase=0 AND length(name)=0",
            &[&generation], 16, |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if *reader_released != Some(1)
            || !matches!(wake.as_slice(), [(cursor, aux)] if (*cursor, *aux) < (item.kind, item.resource))
        {
            return Err(OverlayError::Invalid("mixed recovery held generation"));
        }
    }
    Ok(())
}

pub(super) fn check_mixed_queue(
    db: &Overlay,
    orphans: &super::durable_orphan_recovery::MixedOrphans,
    recorded: RecordedCapture,
    phase: i64,
    operation: Option<i64>,
) -> OverlayResult<()> {
    let mut after = (0_i64, 0_i64, 0_i64);
    loop {
        let rows: Vec<QueueRow> = db.query(
            StatementKind::Startup,
            "SELECT kind,resource,target,phase,cursor,aux,name,ready FROM cleanup_task WHERE (kind,resource,target)>(?1,?2,?3) ORDER BY kind,resource,target LIMIT 64",
            &[&after.0, &after.1, &after.2], 512 * 64,
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?)),
        )?;
        if rows.is_empty() {
            return Ok(());
        }
        for (kind, resource, target, item_phase, cursor, aux, name, ready) in rows {
            if (kind, resource, target) <= after {
                return Err(OverlayError::Invalid("mixed recovery cleanup order"));
            }
            after = (kind, resource, target);
            let item = crate::maintenance::Item {
                kind,
                resource,
                target,
                phase: item_phase,
                cursor,
                aux,
                name,
            };
            check_mixed_item(db, orphans, recorded, phase, operation, &item, ready)?;
        }
    }
}
