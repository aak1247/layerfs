//! Local checkpoint identity. Publication and recovery authority live outside Overlay.
use crate::{
    Capture, CapturedReader, OperationOwner, Overlay, OverlayError, OverlayResult, RecordedCapture,
    StatementKind,
};

#[cfg(target_os = "linux")]
pub(super) fn initialize(db: &Overlay) -> OverlayResult<()> {
    db.execute(
        StatementKind::Startup,
        "CREATE TABLE durable_checkpoint_attempt(\
         id INTEGER PRIMARY KEY CHECK(id=1), attempt BLOB NOT NULL UNIQUE CHECK(length(attempt)=32), \
         workspace BLOB NOT NULL CHECK(length(workspace)=32), \
         catalog BLOB NOT NULL CHECK(length(catalog)=32), incarnation INTEGER NOT NULL CHECK(incarnation>0), \
         namespace INTEGER NOT NULL CHECK(namespace>0), generation INTEGER NOT NULL CHECK(generation>0), \
         revision INTEGER NOT NULL CHECK(revision>=0), base_root BLOB NOT NULL CHECK(length(base_root)=32), \
         snapshot BLOB NOT NULL CHECK(length(snapshot)>0 AND length(snapshot)<=4096), \
         phase INTEGER NOT NULL CHECK(phase IN(1,2)), request BLOB, request_digest BLOB, \
         candidate_root BLOB, target_snapshot BLOB, \
         reader_owner INTEGER CHECK(reader_owner>0), reader_request INTEGER, reader_floor INTEGER, reader_released INTEGER, \
         operation_owner INTEGER CHECK(operation_owner>0), operation_released INTEGER, \
         CHECK((reader_owner IS NULL AND reader_request IS NULL AND reader_floor IS NULL AND reader_released IS NULL) OR \
          (reader_owner IS NOT NULL AND reader_request IS NOT NULL AND reader_request=generation AND \
           reader_floor IS NOT NULL AND reader_floor>=0 AND reader_floor<generation AND \
           reader_released IS NOT NULL AND reader_released IN(0,1))), \
         CHECK((operation_owner IS NULL AND operation_released IS NULL) OR \
          (operation_owner IS NOT NULL AND operation_released IS NOT NULL AND operation_released IN(0,1) AND reader_owner IS NOT NULL)), \
         CHECK((phase=1 AND request IS NULL AND request_digest IS NULL AND candidate_root IS NULL AND target_snapshot IS NULL) OR \
         (phase=2 AND request IS NOT NULL AND request_digest IS NOT NULL AND length(request_digest)=32 AND \
          candidate_root IS NOT NULL AND length(candidate_root)=32 AND target_snapshot IS NOT NULL AND \
          length(target_snapshot)>0 AND length(target_snapshot)<=4096))) STRICT",
        &[],
        0,
    )?;
    Ok(())
}

/// Records only the local prepublication fact in the caller's capture job.
/// The caller must authenticate the selected catalog and its physical Store.
pub(crate) fn record(
    db: &Overlay,
    workspace: [u8; 32],
    catalog: [u8; 32],
    incarnation: u64,
    snapshot: &[u8],
    capture: Capture,
) -> OverlayResult<[u8; 32]> {
    if db.durable.is_none()
        || !db.profile.has_recorded_owners()
        || workspace == [0; 32]
        || snapshot.is_empty()
        || snapshot.len() > 4096
    {
        return Err(OverlayError::Invalid("recorded capture context"));
    }
    let incarnation = i64::try_from(incarnation)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(OverlayError::Invalid("recorded catalog incarnation"))?;
    let identities: Vec<Vec<u8>> = db.query(
        StatementKind::Frontier,
        "SELECT identity FROM durable_identity WHERE id=1",
        &[],
        0,
        |row| row.get(0),
    )?;
    if identities.len() != 1
        || identities[0] != workspace
        || db.durable_binding()?.as_deref() != Some(snapshot)
    {
        return Err(OverlayError::Invalid("recorded capture binding"));
    }
    let namespace = db
        .durable_namespace()?
        .filter(|value| *value > 0)
        .ok_or(OverlayError::Invalid("recorded capture namespace"))?;
    let attempt = identity(
        &workspace,
        &catalog,
        incarnation,
        namespace,
        capture,
        snapshot,
    );
    db.execute(
        StatementKind::Frontier,
        "INSERT INTO durable_checkpoint_attempt(id,attempt,workspace,catalog,incarnation,namespace,generation,revision,base_root,snapshot,phase) \
         VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8,?9,1)",
        &[&attempt.as_slice(), &workspace.as_slice(), &catalog.as_slice(), &incarnation, &namespace,
          &capture.generation.0, &capture.revision, &capture.base_root.as_slice(), &snapshot],
        snapshot.len() as u64 + 144,
    )?;
    Ok(attempt)
}

fn identity(
    workspace: &[u8],
    catalog: &[u8],
    incarnation: i64,
    namespace: i64,
    capture: Capture,
    snapshot: &[u8],
) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs-checkpoint-attempt-v1\0");
    hash.update(workspace);
    hash.update(catalog);
    hash.update(&incarnation.to_be_bytes());
    hash.update(&namespace.to_be_bytes());
    hash.update(&capture.generation.0.to_be_bytes());
    hash.update(&capture.revision.to_be_bytes());
    hash.update(&capture.base_root);
    hash.update(&(snapshot.len() as u64).to_be_bytes());
    hash.update(snapshot);
    *hash.finalize().as_bytes()
}

/// Checks the complete original context before a recorded owner or send stage.
pub(crate) fn check_original(db: &Overlay, recorded: RecordedCapture) -> OverlayResult<i64> {
    check_context(db, recorded, true)
}

fn check_context(db: &Overlay, recorded: RecordedCapture, starting: bool) -> OverlayResult<i64> {
    let capture = recorded.capture;
    db.checked_capture(capture)?;
    if starting && db.state()?.closed {
        return Err(OverlayError::Closed);
    }
    let namespace = db
        .durable_namespace()?
        .filter(|value| *value > 0)
        .ok_or(OverlayError::Invalid("recorded capture namespace"))?;
    let original: Vec<(Vec<u8>, Vec<u8>, i64, Vec<u8>, i64)> = db.query(
        StatementKind::Frontier,
        "SELECT workspace,catalog,incarnation,snapshot,phase FROM durable_checkpoint_attempt \
         WHERE id=1 AND attempt=?1 AND namespace=?2 AND generation=?3 \
         AND revision=?4 AND base_root=?5",
        &[
            &recorded.attempt.as_slice(),
            &namespace,
            &capture.generation.0,
            &capture.revision,
            &capture.base_root.as_slice(),
        ],
        112,
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    let [(workspace, catalog, incarnation, snapshot, phase)] = original.as_slice() else {
        return Err(OverlayError::Invalid("checkpoint publication predecessor"));
    };
    if (starting && *phase != 1) || (!starting && ![1, 2].contains(phase)) {
        return Err(OverlayError::Invalid("checkpoint publication predecessor"));
    }
    let identities: Vec<Vec<u8>> = db.query(
        StatementKind::Frontier,
        "SELECT identity FROM durable_identity WHERE id=1",
        &[],
        0,
        |row| row.get(0),
    )?;
    if workspace.len() != 32
        || catalog.len() != 32
        || *incarnation <= 0
        || identities.len() != 1
        || identities.first() != Some(workspace)
        || db.durable_binding()?.as_deref() != Some(snapshot.as_slice())
        || identity(
            workspace,
            catalog,
            *incarnation,
            namespace,
            capture,
            snapshot,
        ) != recorded.attempt
    {
        return Err(OverlayError::Invalid("checkpoint original identity"));
    }
    Ok(namespace)
}

pub(crate) fn acquire_reader(
    db: &Overlay,
    recorded: RecordedCapture,
    reader: CapturedReader,
    request: i64,
) -> OverlayResult<()> {
    let namespace = check_original(db, recorded)?;
    if reader.capture != recorded.capture || request != reader.capture.generation.0 {
        return Err(OverlayError::Invalid("checkpoint reader identity"));
    }
    let owner = crate::db::integer(reader.owner)?;
    let changed = db.execute(
        StatementKind::Frontier,
        "UPDATE durable_checkpoint_attempt SET reader_owner=?1,reader_request=?2,reader_floor=?3,reader_released=0 \
         WHERE id=1 AND attempt=?4 AND namespace=?5 AND phase=1 AND reader_owner IS NULL",
        &[&owner, &request, &reader.installed, &recorded.attempt.as_slice(), &namespace],
        64,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("checkpoint reader predecessor"));
    }
    Ok(())
}

pub(crate) fn acquire_operation(
    db: &Overlay,
    recorded: RecordedCapture,
    reader: CapturedReader,
    request: i64,
    owner: u64,
) -> OverlayResult<()> {
    let namespace = check_original(db, recorded)?;
    if reader.capture != recorded.capture || request != reader.capture.generation.0 {
        return Err(OverlayError::Invalid("checkpoint operation identity"));
    }
    if reader.recorded_attempt != Some(recorded.attempt) {
        return Err(OverlayError::Invalid("checkpoint operation reader"));
    }
    let owner = crate::db::integer(owner)?;
    let reader_owner = crate::db::integer(reader.owner)?;
    let changed = db.execute(
        StatementKind::Frontier,
        "UPDATE durable_checkpoint_attempt SET operation_owner=?1,operation_released=0 \
         WHERE id=1 AND attempt=?2 AND namespace=?3 AND phase=1 AND reader_owner=?4 \
         AND reader_request=?5 AND reader_floor=?6 AND reader_released=0 AND operation_owner IS NULL",
        &[&owner, &recorded.attempt.as_slice(), &namespace, &reader_owner, &request, &reader.installed],
        72,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("checkpoint operation predecessor"));
    }
    Ok(())
}

pub(crate) fn release_reader(
    db: &Overlay,
    reader: CapturedReader,
    request: i64,
) -> OverlayResult<()> {
    let Some(attempt) = reader.recorded_attempt else {
        if pending(db)? {
            let generations = db.query(
                StatementKind::Frontier,
                "SELECT generation FROM durable_checkpoint_attempt WHERE id=1",
                &[],
                0,
                |row| row.get::<_, i64>(0),
            )?;
            match generations.as_slice() {
                [generation] if *generation != reader.capture.generation.0 => return Ok(()),
                _ => return Err(OverlayError::Invalid("checkpoint reader release identity")),
            }
        }
        return Ok(());
    };
    check_context(
        db,
        RecordedCapture {
            capture: reader.capture,
            attempt,
        },
        false,
    )?;
    let owner = crate::db::integer(reader.owner)?;
    let changed = db.execute(
        StatementKind::Frontier,
        "UPDATE durable_checkpoint_attempt SET reader_released=1 WHERE id=1 AND reader_owner=?1 \
         AND reader_request=?2 AND reader_floor=?3 AND reader_released=0 AND generation=?4 \
         AND revision=?5 AND base_root=?6 AND attempt=?7",
        &[
            &owner,
            &request,
            &reader.installed,
            &reader.capture.generation.0,
            &reader.capture.revision,
            &reader.capture.base_root.as_slice(),
            &attempt.as_slice(),
        ],
        72,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("checkpoint reader release identity"));
    }
    Ok(())
}

pub(crate) fn release_operation(
    db: &Overlay,
    owner: OperationOwner,
    request: i64,
) -> OverlayResult<()> {
    let owner_id = crate::db::integer(owner.owner)?;
    let Some(recorded) = owner.recorded else {
        if pending(db)? {
            let matches = db.query(
                StatementKind::Frontier,
                "SELECT EXISTS(SELECT 1 FROM durable_checkpoint_attempt WHERE id=1 AND operation_owner=?1)",
                &[&owner_id],
                8,
                |row| row.get::<_, bool>(0),
            )?;
            if matches.as_slice() != [false] {
                return Err(OverlayError::Invalid(
                    "checkpoint operation release identity",
                ));
            }
        }
        return Ok(());
    };
    check_context(db, recorded, false)?;
    let changed = db.execute(
        StatementKind::Frontier,
        "UPDATE durable_checkpoint_attempt SET operation_released=1 WHERE id=1 AND attempt=?1 \
         AND operation_owner=?2 AND reader_request=?3 AND reader_released=1 AND operation_released=0 \
         AND EXISTS(SELECT 1 FROM durable_operation_owner o WHERE o.owner=?2 AND o.request=?3 \
         AND o.namespace=durable_checkpoint_attempt.namespace AND o.purpose=2 \
         AND o.capture_generation=durable_checkpoint_attempt.generation \
         AND o.capture_revision=durable_checkpoint_attempt.revision \
         AND o.capture_root=durable_checkpoint_attempt.base_root \
         AND o.capture_floor=durable_checkpoint_attempt.reader_floor \
         AND o.reader_owner=durable_checkpoint_attempt.reader_owner)",
        &[&recorded.attempt.as_slice(), &owner_id, &request],
        48,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid(
            "checkpoint operation release identity",
        ));
    }
    Ok(())
}

pub(crate) fn pending(db: &Overlay) -> OverlayResult<bool> {
    if db.durable.is_none() || !db.profile.has_recorded_owners() {
        return Ok(false);
    }
    let rows = db.query(
        StatementKind::Frontier,
        "SELECT EXISTS(SELECT 1 FROM durable_checkpoint_attempt)",
        &[],
        0,
        |row| row.get::<_, bool>(0),
    )?;
    match rows.as_slice() {
        [value] => Ok(*value),
        _ => Err(OverlayError::Invalid("recorded capture state")),
    }
}

impl Overlay {
    /// Freezes the complete request and candidate before its one Store send.
    /// The caller authenticates the Store and interprets both opaque snapshots;
    /// this local transition is neither publication nor recovery permission.
    pub fn prepare_checkpoint_publication(
        &self,
        recorded: RecordedCapture,
        request: &[u8],
        request_digest: [u8; 32],
        candidate_root: [u8; 32],
        target_snapshot: &[u8],
    ) -> OverlayResult<()> {
        if !request.starts_with(b"LFCP\x01")
            || target_snapshot.is_empty()
            || target_snapshot.len() > 4096
        {
            return Err(OverlayError::Invalid("checkpoint publication input"));
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"layerfs/checkpoint-request/v1\0");
        hash.update(request);
        if *hash.finalize().as_bytes() != request_digest {
            return Err(OverlayError::Invalid("checkpoint request digest"));
        }
        let bytes = u64::try_from(request.len())
            .ok()
            .and_then(|value| value.checked_add(target_snapshot.len() as u64))
            .and_then(|value| value.checked_add(144))
            .ok_or(OverlayError::Invalid("checkpoint publication size"))?;
        self.atomic_cleanup(|| {
            let namespace = check_original(self, recorded)?;
            let capture = recorded.capture;
            let changed = self.execute(
                StatementKind::Frontier,
                "UPDATE durable_checkpoint_attempt SET phase=2,request=?1,request_digest=?2,\
                 candidate_root=?3,target_snapshot=?4 WHERE id=1 AND phase=1 AND attempt=?5 \
                 AND namespace=?6 AND generation=?7 AND revision=?8 AND base_root=?9",
                &[
                    &request,
                    &request_digest.as_slice(),
                    &candidate_root.as_slice(),
                    &target_snapshot,
                    &recorded.attempt.as_slice(),
                    &namespace,
                    &capture.generation.0,
                    &capture.revision,
                    &capture.base_root.as_slice(),
                ],
                bytes,
            )?;
            if changed != 1 {
                return Err(OverlayError::Invalid("checkpoint publication predecessor"));
            }
            Ok(())
        })
    }
}
