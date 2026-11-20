//! Completion of the original, definitely unpublished checkpoint's local work.
use crate::{
    CheckpointCompletion, CheckpointOutcome, ConflictCompletion, Overlay, OverlayError,
    OverlayResult, RecordedCapture, StatementKind,
};

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

fn ready(
    db: &Overlay,
    recorded: RecordedCapture,
    installed: Option<CheckpointCompletion<'_>>,
    conflict: Option<ConflictCompletion<'_>>,
) -> OverlayResult<(bool, i64)> {
    let state = db.live()?;
    let namespace = super::durable_attempt::check_context(db, recorded, false)?;
    let generation = recorded.capture.generation.0;
    let phase = if installed.is_some() {
        4_i64
    } else if conflict.is_some() {
        5_i64
    } else {
        3_i64
    };
    let rows: Vec<Cleanup> = db.query(
        StatementKind::Frontier,
        "SELECT reader_owner,reader_request,reader_floor,reader_released,operation_owner,\
         operation_released,operation_cleanup_completed,fold_completed,retire_completed \
         FROM durable_checkpoint_attempt WHERE id=1 AND attempt=?1 AND namespace=?2 \
         AND generation=?3 AND phase=?4",
        &[
            &recorded.attempt.as_slice(),
            &namespace,
            &generation,
            &phase,
        ],
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
    if !super::durable_conflict_stage::cleanup_ready(db, recorded, phase)? {
        return Ok((false, namespace));
    }
    let expected_owners = if let Some(expected) = installed {
        check_installed(db, recorded, expected)?;
        if expected.recorded != recorded {
            return Err(OverlayError::Invalid("checkpoint cleanup original owners"));
        }
        Some((expected.reader, expected.operation))
    } else if let Some(expected) = conflict {
        let active = super::durable_conflict_stage::active_conflict(db)?;
        if expected.stage.recorded() != recorded
            || !active.as_ref().is_some_and(|c| {
                c.generation == generation
                    && c.attempt == recorded.attempt
                    && c.digest == expected.stage.descriptor()
                    && c.root.as_slice() == expected.marker_root
                    && c.target == expected.target_snapshot
            })
        {
            return Err(OverlayError::Invalid("conflict cleanup identity"));
        }
        Some((expected.reader, expected.operation))
    } else {
        None
    };
    if let Some((reader, operation)) = expected_owners {
        let expected_reader = reader.map(|reader| {
            (
                reader.owner,
                reader.capture,
                reader.installed,
                reader.recorded_attempt,
            )
        });
        let stored_reader = match (cleanup.reader, cleanup.reader_request, cleanup.reader_floor) {
            (None, None, None) => None,
            (Some(owner), Some(request), Some(floor)) if request == generation => Some((
                u64::try_from(owner)
                    .map_err(|_| OverlayError::Invalid("checkpoint reader owner"))?,
                recorded.capture,
                floor,
                Some(recorded.attempt),
            )),
            _ => return Err(OverlayError::Invalid("checkpoint reader cleanup identity")),
        };
        let expected_operation = operation.map(|owner| (owner.owner, owner.engine, owner.recorded));
        let stored_operation = cleanup
            .operation
            .map(|owner| {
                u64::try_from(owner)
                    .map(|owner| (owner, recorded.capture.engine, Some(recorded)))
                    .map_err(|_| OverlayError::Invalid("checkpoint operation owner"))
            })
            .transpose()?;
        if expected_reader != stored_reader || expected_operation != stored_operation {
            return Err(OverlayError::Invalid("checkpoint cleanup original owners"));
        }
    }
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
            if owner > 0
                && request == generation
                && if phase == 3 {
                    floor == state.installed
                } else {
                    floor >= 0 && floor < generation
                } =>
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
    if (phase == 3 && cleanup.fold_completed != Some(1))
        || (matches!(phase, 4 | 5) && cleanup.fold_completed.is_some())
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

fn check_installed(
    db: &Overlay,
    recorded: RecordedCapture,
    expected: CheckpointCompletion<'_>,
) -> OverlayResult<()> {
    type Installed = (
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
    );
    let rows: Vec<Installed> = db.query(
        StatementKind::Frontier,
        "SELECT request,request_digest,candidate_root,target_snapshot,installed_snapshot,\
         outcome_kind,outcome_head,outcome_displaced FROM durable_checkpoint_attempt \
         WHERE id=1 AND phase=4 AND attempt=?1",
        &[&recorded.attempt.as_slice()],
        144,
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
            ))
        },
    )?;
    let [(request, digest, candidate, target, snapshot, kind, head, displaced)] = rows.as_slice()
    else {
        return Err(OverlayError::Invalid(
            "checkpoint installed cleanup predecessor",
        ));
    };
    let (expected_kind, expected_head, expected_displaced) = match expected.outcome {
        CheckpointOutcome::Committed { head, displaced } => (
            1_i64,
            Some(head.to_vec()),
            displaced.map(|value| value.to_vec()),
        ),
        CheckpointOutcome::UpToDate { head } => (2_i64, head.map(|value| value.to_vec()), None),
    };
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs/checkpoint-request/v1\0");
    hash.update(request);
    if request.as_slice() != expected.request
        || digest.as_slice() != expected.request_digest
        || *hash.finalize().as_bytes() != expected.request_digest
        || candidate.as_slice() != expected.candidate_root
        || target.as_slice() != expected.target_snapshot
        || snapshot.as_slice() != expected.installed_snapshot
        || *kind != expected_kind
        || *head != expected_head
        || *displaced != expected_displaced
    {
        return Err(OverlayError::Invalid(
            "checkpoint installed cleanup identity",
        ));
    }
    Ok(())
}

impl Overlay {
    /// Observes only the original folded checkpoint's local cleanup; it does
    /// not drive maintenance or authenticate any external Store publication.
    pub fn folded_checkpoint_cleanup_ready(
        &self,
        recorded: RecordedCapture,
    ) -> OverlayResult<bool> {
        ready(self, recorded, None, None).map(|(ready, _)| ready)
    }

    /// Rechecks and finishes the original definitely unpublished attempt once.
    /// Its caller must already have established that no publication was sent.
    pub fn finish_folded_checkpoint_cleanup(&self, recorded: RecordedCapture) -> OverlayResult<()> {
        self.atomic_cleanup(|| {
            let (complete, namespace) = ready(self, recorded, None, None)?;
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

    /// Observes cleanup of a known installed checkpoint. The caller must
    /// independently authenticate its Store receipt and bound Workspace.
    pub fn installed_checkpoint_cleanup_ready(
        &self,
        expected: CheckpointCompletion<'_>,
    ) -> OverlayResult<bool> {
        ready(self, expected.recorded, Some(expected), None).map(|(ready, _)| ready)
    }

    /// Finishes the exact installed checkpoint after its authenticated
    /// receipt, original owners and local maintenance have been established.
    pub fn finish_installed_checkpoint_cleanup(
        &self,
        expected: CheckpointCompletion<'_>,
    ) -> OverlayResult<()> {
        self.atomic_cleanup(|| {
            let recorded = expected.recorded;
            let (complete, namespace) = ready(self, recorded, Some(expected), None)?;
            if !complete {
                return Err(OverlayError::Invalid("checkpoint installed cleanup pending"));
            }
            let changed = self.execute(
                StatementKind::Frontier,
                "DELETE FROM durable_checkpoint_attempt WHERE id=1 AND phase=4 AND attempt=?1 AND namespace=?2 AND generation=?3",
                &[&recorded.attempt.as_slice(), &namespace, &recorded.capture.generation.0],
                48,
            )?;
            if changed != 1 {
                return Err(OverlayError::Invalid("checkpoint installed cleanup predecessor"));
            }
            Ok(())
        })
    }

    /// Observes original local-conflict cleanup without claiming Store authority.
    pub fn conflict_checkpoint_cleanup_ready(
        &self,
        expected: ConflictCompletion<'_>,
    ) -> OverlayResult<bool> {
        ready(self, expected.stage.recorded(), None, Some(expected)).map(|(ready, _)| ready)
    }

    /// Finishes only the local attempt. Its active conflict plan remains bound
    /// to the Workspace until a later authenticated replacement is installed.
    pub fn finish_conflict_checkpoint_cleanup(
        &self,
        expected: ConflictCompletion<'_>,
    ) -> OverlayResult<()> {
        self.atomic_cleanup(|| {
            let recorded=expected.stage.recorded();
            let (complete,namespace)=ready(self,recorded,None,Some(expected))?;
            if !complete { return Err(OverlayError::Invalid("conflict cleanup pending")); }
            if self.execute(StatementKind::Frontier,
                "DELETE FROM durable_checkpoint_attempt WHERE id=1 AND phase=5 AND attempt=?1 AND namespace=?2 AND generation=?3",
                &[&recorded.attempt.as_slice(),&namespace,&recorded.capture.generation.0],48)?!=1 {
                return Err(OverlayError::Invalid("conflict cleanup predecessor"));
            }
            Ok(())
        })
    }
}
