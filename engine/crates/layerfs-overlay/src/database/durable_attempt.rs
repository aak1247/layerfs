//! Local checkpoint identity. Publication and recovery authority live outside Overlay.
use crate::{Capture, Overlay, OverlayError, OverlayResult, StatementKind};

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
        || catalog == [0; 32]
        || snapshot.is_empty()
        || snapshot.len() > 4096
    {
        return Err(OverlayError::Invalid("recorded capture context"));
    }
    let incarnation = i64::try_from(incarnation)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(OverlayError::Invalid("recorded catalog incarnation"))?;
    let identity: Vec<Vec<u8>> = db.query(
        StatementKind::Frontier,
        "SELECT identity FROM durable_identity WHERE id=1",
        &[],
        0,
        |row| row.get(0),
    )?;
    if identity.len() != 1
        || identity[0] != workspace
        || db.durable_binding()?.as_deref() != Some(snapshot)
    {
        return Err(OverlayError::Invalid("recorded capture binding"));
    }
    let namespace = db
        .durable_namespace()?
        .filter(|value| *value > 0)
        .ok_or(OverlayError::Invalid("recorded capture namespace"))?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs-checkpoint-attempt-v1\0");
    hash.update(&workspace);
    hash.update(&catalog);
    hash.update(&incarnation.to_be_bytes());
    hash.update(&namespace.to_be_bytes());
    hash.update(&capture.generation.0.to_be_bytes());
    hash.update(&capture.revision.to_be_bytes());
    hash.update(&capture.base_root);
    hash.update(&(snapshot.len() as u64).to_be_bytes());
    hash.update(snapshot);
    let attempt = *hash.finalize().as_bytes();
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
