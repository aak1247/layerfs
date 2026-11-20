//! Completion of the original, definitely unpublished checkpoint's local work.
use crate::{Overlay, OverlayError, OverlayResult, RecordedCapture, StatementKind};

struct Cleanup {
    reader: Option<i64>,
    reader_request: Option<i64>,
    reader_floor: Option<i64>,
    reader_released: Option<i64>,
    operation: Option<i64>,
    operation_released: Option<i64>,
    operation_completed: Option<i64>,
    fold_completed: Option<i64>,
    retire_completed: Option<i64>,
}

fn ready(db: &Overlay, recorded: RecordedCapture) -> OverlayResult<(bool, i64)> {
    let state = db.live()?;
    let namespace = super::durable_attempt::check_context(db, recorded, false)?;
    let generation = recorded.capture.generation.0;
    let rows: Vec<Cleanup> = db.query(
        StatementKind::Frontier,
        "SELECT reader_owner,reader_request,reader_floor,reader_released,operation_owner,\
         operation_released,operation_cleanup_completed,fold_completed,retire_completed \
         FROM durable_checkpoint_attempt WHERE id=1 AND attempt=?1 AND namespace=?2 \
         AND generation=?3 AND phase=3",
        &[&recorded.attempt.as_slice(), &namespace, &generation],
        72,
        |row| {
            Ok(Cleanup {
                reader: row.get(0)?,
                reader_request: row.get(1)?,
                reader_floor: row.get(2)?,
                reader_released: row.get(3)?,
                operation: row.get(4)?,
                operation_released: row.get(5)?,
                operation_completed: row.get(6)?,
                fold_completed: row.get(7)?,
                retire_completed: row.get(8)?,
            })
        },
    )?;
    let [cleanup] = rows.as_slice() else {
        return Err(OverlayError::Invalid("checkpoint fold cleanup predecessor"));
    };
    if state.captured_revision.is_some() {
        return Err(OverlayError::Invalid("checkpoint fold cleanup state"));
    }
    match (
        cleanup.reader,
        cleanup.reader_request,
        cleanup.reader_floor,
        cleanup.reader_released,
    ) {
        (None, None, None, None) => {}
        (Some(owner), Some(request), Some(floor), Some(released))
            if owner > 0 && request == generation && floor == state.installed =>
        {
            let owner = u64::try_from(owner)
                .map_err(|_| OverlayError::Invalid("checkpoint reader owner"))?;
            match (released, db.open.borrow().captured.get(owner).is_some()) {
                (0, true) => return Ok((false, namespace)),
                (1, false) => {}
                _ => return Err(OverlayError::Invalid("checkpoint reader cleanup identity")),
            }
        }
        _ => return Err(OverlayError::Invalid("checkpoint reader cleanup identity")),
    }
    let retained = db.open.borrow().operations.retained(generation);
    if let Some(owner) = retained {
        if owner.recorded.is_some()
            && (owner.recorded != Some(recorded)
                || i64::try_from(owner.owner).ok() != cleanup.operation
                || cleanup.operation_released != Some(0))
        {
            return Err(OverlayError::Invalid(
                "checkpoint operation cleanup identity",
            ));
        }
    }
    let durable: Vec<(i64, i64, i64, Vec<u8>, i64, i64)> = db.query(
        StatementKind::Frontier,
        "SELECT owner,capture_generation,capture_revision,capture_root,capture_floor,reader_owner \
         FROM durable_operation_owner WHERE namespace=?1 AND request=?2 AND purpose=2",
        &[&namespace, &generation],
        80,
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )?;
    match durable.as_slice() {
        [] if cleanup.operation_released != Some(0) => {}
        [(owner, captured, revision, root, floor, reader)]
            if Some(*owner) == cleanup.operation
                && cleanup.operation_released == Some(0)
                && *captured == generation
                && *revision == recorded.capture.revision
                && root.as_slice() == recorded.capture.base_root
                && Some(*floor) == cleanup.reader_floor
                && Some(*reader) == cleanup.reader => {}
        _ => {
            return Err(OverlayError::Invalid(
                "checkpoint durable operation identity",
            ))
        }
    }
    match (
        cleanup.operation,
        cleanup.operation_released,
        cleanup.operation_completed,
    ) {
        (None, None, None) => {}
        (Some(owner), Some(released), Some(completed)) if owner > 0 && cleanup.reader.is_some() => {
            let owner_u64 = u64::try_from(owner)
                .map_err(|_| OverlayError::Invalid("checkpoint operation owner"))?;
            match (
                released,
                completed,
                db.open.borrow().operations.get(owner_u64).is_some(),
            ) {
                (0, 0, true) | (1, 0, false) => return Ok((false, namespace)),
                (1, 1, false) => {}
                _ => {
                    return Err(OverlayError::Invalid(
                        "checkpoint operation cleanup identity",
                    ))
                }
            }
            let rows: Vec<(bool, bool, bool)> = db.query(
                StatementKind::Reclaim,
                "SELECT EXISTS(SELECT 1 FROM durable_operation_owner WHERE owner=?1),\
                 EXISTS(SELECT 1 FROM cleanup_task WHERE kind=6 AND resource=?1),\
                 EXISTS(SELECT 1 FROM commit_record WHERE operation=?1)",
                &[&owner],
                24,
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            if rows.as_slice() != [(false, false, false)] {
                return Err(OverlayError::Invalid("checkpoint operation cleanup state"));
            }
        }
        _ => {
            return Err(OverlayError::Invalid(
                "checkpoint operation cleanup identity",
            ))
        }
    }
    if cleanup.fold_completed != Some(1)
        || cleanup.retire_completed != Some(1)
        || state.consolidating.is_some()
        || db.generation_held(generation)?
    {
        return Ok((false, namespace));
    }
    let pending = db.query(
        StatementKind::Reclaim,
        "SELECT EXISTS(SELECT 1 FROM cleanup_task WHERE kind IN(1,3,8,9) AND resource=0 AND target=?1)",
        &[&generation],
        8,
        |row| row.get::<_, bool>(0),
    )?;
    match pending.as_slice() {
        [pending] => Ok((!*pending, namespace)),
        _ => Err(OverlayError::Invalid("checkpoint cleanup work state")),
    }
}

impl Overlay {
    /// Observes only the original folded checkpoint's local cleanup; it does
    /// not drive maintenance or authenticate any external Store publication.
    pub fn folded_checkpoint_cleanup_ready(
        &self,
        recorded: RecordedCapture,
    ) -> OverlayResult<bool> {
        ready(self, recorded).map(|(ready, _)| ready)
    }

    /// Rechecks and finishes the original definitely unpublished attempt once.
    /// Its caller must already have established that no publication was sent.
    pub fn finish_folded_checkpoint_cleanup(&self, recorded: RecordedCapture) -> OverlayResult<()> {
        self.atomic_cleanup(|| {
            let (complete, namespace) = ready(self, recorded)?;
            if !complete {
                return Err(OverlayError::Invalid("checkpoint fold cleanup pending"));
            }
            let changed = self.execute(
                StatementKind::Frontier,
                "DELETE FROM durable_checkpoint_attempt WHERE id=1 AND phase=3 AND attempt=?1 AND namespace=?2 AND generation=?3",
                &[&recorded.attempt.as_slice(), &namespace, &recorded.capture.generation.0],
                48,
            )?;
            if changed != 1 {
                return Err(OverlayError::Invalid("checkpoint fold cleanup predecessor"));
            }
            Ok(())
        })
    }
}
