//! Local checkpoint identity. Publication and recovery authority live outside Overlay.
use crate::{Capture, Overlay, OverlayError, OverlayResult, RecordedCapture, StatementKind};

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
            self.checked_capture(recorded.capture)?;
            if self.state()?.closed {
                return Err(OverlayError::Closed);
            }
            let namespace = self
                .durable_namespace()?
                .filter(|value| *value > 0)
                .ok_or(OverlayError::Invalid("recorded capture namespace"))?;
            let capture = recorded.capture;
            let original: Vec<(Vec<u8>, Vec<u8>, i64, Vec<u8>)> = self.query(
                StatementKind::Frontier,
                "SELECT workspace,catalog,incarnation,snapshot FROM durable_checkpoint_attempt \
                 WHERE id=1 AND phase=1 AND attempt=?1 AND namespace=?2 AND generation=?3 \
                 AND revision=?4 AND base_root=?5",
                &[
                    &recorded.attempt.as_slice(),
                    &namespace,
                    &capture.generation.0,
                    &capture.revision,
                    &capture.base_root.as_slice(),
                ],
                112,
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
            let [(workspace, catalog, incarnation, snapshot)] = original.as_slice() else {
                return Err(OverlayError::Invalid("checkpoint publication predecessor"));
            };
            let identities: Vec<Vec<u8>> = self.query(
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
                || self.durable_binding()?.as_deref() != Some(snapshot.as_slice())
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
