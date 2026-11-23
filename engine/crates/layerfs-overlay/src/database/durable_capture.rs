//! Append-only identities for explicitly requested external captures.
use crate::{
    ExternalCapture, ExternalCaptureDisposition, ExternalCapturePublication, ExternalCaptureRecord,
    Overlay, OverlayError, OverlayResult, StatementKind,
};

pub(super) const SCHEMA: &str = "CREATE TABLE durable_external_capture(\
    capture_id BLOB PRIMARY KEY CHECK(length(capture_id)=32), \
    scope_digest BLOB NOT NULL CHECK(length(scope_digest)=32), \
    purpose INTEGER NOT NULL CHECK(purpose=1), \
    attempt BLOB NOT NULL UNIQUE CHECK(length(attempt)=32), \
    workspace BLOB NOT NULL CHECK(length(workspace)=32), \
    catalog BLOB NOT NULL CHECK(length(catalog)=32), incarnation INTEGER NOT NULL CHECK(incarnation>0), \
    namespace INTEGER NOT NULL CHECK(namespace>0), generation INTEGER NOT NULL CHECK(generation>0), \
    revision INTEGER NOT NULL CHECK(revision>=0), base_root BLOB NOT NULL CHECK(length(base_root)=32), \
    snapshot BLOB NOT NULL CHECK(length(snapshot)>0 AND length(snapshot)<=4096), \
    request BLOB, request_digest BLOB, candidate_root BLOB, target_snapshot BLOB, \
    disposition INTEGER NOT NULL DEFAULT 0 CHECK(disposition IN(0,1)), \
    CHECK((request IS NULL AND request_digest IS NULL AND candidate_root IS NULL AND target_snapshot IS NULL) OR \
     (request IS NOT NULL AND length(request)>0 AND length(request)<=65536 AND \
      request_digest IS NOT NULL AND length(request_digest)=32 AND candidate_root IS NOT NULL AND \
      length(candidate_root)=32 AND target_snapshot IS NOT NULL AND \
      length(target_snapshot)>0 AND length(target_snapshot)<=4096))) STRICT";

pub(super) const IMMUTABLE: &str = "CREATE TRIGGER durable_external_capture_immutable \
    BEFORE UPDATE ON durable_external_capture WHEN \
    NEW.capture_id IS NOT OLD.capture_id OR NEW.scope_digest IS NOT OLD.scope_digest OR \
    NEW.purpose IS NOT OLD.purpose OR NEW.attempt IS NOT OLD.attempt OR \
    NEW.workspace IS NOT OLD.workspace OR NEW.catalog IS NOT OLD.catalog OR \
    NEW.incarnation IS NOT OLD.incarnation OR NEW.namespace IS NOT OLD.namespace OR \
    NEW.generation IS NOT OLD.generation OR NEW.revision IS NOT OLD.revision OR \
    NEW.base_root IS NOT OLD.base_root OR NEW.snapshot IS NOT OLD.snapshot OR \
    NEW.disposition<OLD.disposition OR \
    (OLD.disposition=0 AND NEW.disposition=1 AND NOT EXISTS(\
      SELECT 1 FROM durable_checkpoint_attempt a WHERE a.id=1 AND a.attempt=OLD.attempt AND \
      a.workspace=OLD.workspace AND a.catalog=OLD.catalog AND a.incarnation=OLD.incarnation AND \
      a.namespace=OLD.namespace AND a.generation=OLD.generation AND a.revision=OLD.revision AND \
      a.base_root=OLD.base_root AND a.snapshot=OLD.snapshot AND \
      ((a.phase=3 AND a.request IS NULL AND OLD.request IS NULL) OR \
       (a.phase=6 AND OLD.request IS NOT NULL AND a.request IS OLD.request AND \
        a.request_digest IS OLD.request_digest AND a.candidate_root IS OLD.candidate_root AND \
        a.target_snapshot IS OLD.target_snapshot)))) OR \
    ((OLD.request IS NOT NULL OR OLD.disposition=1 OR NEW.disposition IS NOT OLD.disposition) AND (NEW.request IS NOT OLD.request OR \
     NEW.request_digest IS NOT OLD.request_digest OR NEW.candidate_root IS NOT OLD.candidate_root OR \
     NEW.target_snapshot IS NOT OLD.target_snapshot)) \
    BEGIN SELECT RAISE(ABORT,'external capture identity is immutable'); END";

pub(super) const NO_DELETE: &str = "CREATE TRIGGER durable_external_capture_no_delete \
    BEFORE DELETE ON durable_external_capture \
    BEGIN SELECT RAISE(ABORT,'external capture history is retained'); END";

pub(super) fn initialize(db: &Overlay) -> OverlayResult<()> {
    for sql in [SCHEMA, IMMUTABLE, NO_DELETE] {
        db.execute(StatementKind::Startup, sql, &[], 0)?;
    }
    Ok(())
}

pub(crate) fn require_new(db: &Overlay, identity: ExternalCapture) -> OverlayResult<()> {
    if !db.profile.has_external_captures() || db.durable.is_none() || !identity.valid() {
        return Err(OverlayError::Invalid(
            "external capture capability or identity",
        ));
    }
    if super::durable_conflict_stage::active_conflict(db)?.is_some() {
        return Err(OverlayError::Invalid(
            "external capture has pending conflict",
        ));
    }
    let exists: Vec<bool> = db.query(
        StatementKind::Frontier,
        "SELECT EXISTS(SELECT 1 FROM durable_external_capture WHERE capture_id=?1)",
        &[&identity.id.as_slice()],
        32,
        |row| row.get(0),
    )?;
    if exists.as_slice() != [false] {
        return Err(OverlayError::Invalid(
            "external capture identity already recorded",
        ));
    }
    Ok(())
}

/// Called in the same transaction as the exact capture and checkpoint row.
pub(crate) fn record(
    db: &Overlay,
    identity: ExternalCapture,
    attempt: [u8; 32],
) -> OverlayResult<()> {
    let changed = db.execute(
        StatementKind::Frontier,
        "INSERT INTO durable_external_capture(capture_id,scope_digest,purpose,attempt,workspace,catalog,\
         incarnation,namespace,generation,revision,base_root,snapshot) \
         SELECT ?1,?2,?3,attempt,workspace,catalog,incarnation,namespace,generation,revision,base_root,snapshot \
         FROM durable_checkpoint_attempt WHERE id=1 AND phase=1 AND attempt=?4",
        &[&identity.id.as_slice(), &identity.scope_digest.as_slice(), &identity.purpose.code(), &attempt.as_slice()],
        96,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid(
            "external capture checkpoint predecessor",
        ));
    }
    Ok(())
}

/// Called in the original checkpoint publication transaction. Ordinary
/// checkpoints have no matching row and keep their existing behavior.
pub(super) fn freeze_publication(
    db: &Overlay,
    attempt: [u8; 32],
    request: &[u8],
    digest: [u8; 32],
    candidate: [u8; 32],
    target: &[u8],
) -> OverlayResult<()> {
    if !db.profile.has_external_captures() {
        return Ok(());
    }
    let exists: Vec<bool> = db.query(
        StatementKind::Frontier,
        "SELECT EXISTS(SELECT 1 FROM durable_external_capture WHERE attempt=?1)",
        &[&attempt.as_slice()],
        32,
        |row| row.get(0),
    )?;
    match exists.as_slice() {
        [false] => Ok(()),
        [true] => {
            if request.is_empty()
                || request.len() > 65536
                || target.is_empty()
                || target.len() > 4096
            {
                return Err(OverlayError::Invalid("external capture publication size"));
            }
            let changed = db.execute(
                StatementKind::Frontier,
                "UPDATE durable_external_capture SET request=?1,request_digest=?2,candidate_root=?3,\
                 target_snapshot=?4 WHERE attempt=?5 AND request IS NULL AND disposition=0",
                &[&request, &digest.as_slice(), &candidate.as_slice(), &target, &attempt.as_slice()],
                (request.len() + target.len() + 96) as u64,
            )?;
            if changed != 1 {
                return Err(OverlayError::Invalid(
                    "external capture publication predecessor",
                ));
            }
            Ok(())
        }
        _ => Err(OverlayError::Invalid(
            "external capture publication identity",
        )),
    }
}

/// Called only in the original fold transaction, after the caller proves the
/// original attempt was not sent or verifies its absence under the Store lane
/// and the original writer fence. No observation alone may call this.
pub(super) fn mark_unpublished(db: &Overlay, attempt: [u8; 32]) -> OverlayResult<()> {
    if !db.profile.has_external_captures() {
        return Ok(());
    }
    let exists: Vec<bool> = db.query(
        StatementKind::Frontier,
        "SELECT EXISTS(SELECT 1 FROM durable_external_capture WHERE attempt=?1)",
        &[&attempt.as_slice()],
        32,
        |row| row.get(0),
    )?;
    match exists.as_slice() {
        [false] => Ok(()),
        [true] => {
            let changed = db.execute(
                StatementKind::Frontier,
                "UPDATE durable_external_capture SET disposition=1 WHERE attempt=?1 AND disposition=0",
                &[&attempt.as_slice()], 32,
            )?;
            if changed != 1 {
                return Err(OverlayError::Invalid(
                    "external capture disposition predecessor",
                ));
            }
            Ok(())
        }
        _ => Err(OverlayError::Invalid(
            "external capture disposition identity",
        )),
    }
}

type Row = (
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    i64,
    i64,
    i64,
    i64,
    Vec<u8>,
    Vec<u8>,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    i64,
);

fn fixed(bytes: Vec<u8>) -> OverlayResult<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| OverlayError::Invalid("external capture record bytes"))
}

/// The caller still authenticates the Workspace, scope and Store receipt.
/// An absent result is an observation only, never permission to resend.
pub(crate) fn observe(
    db: &Overlay,
    identity: ExternalCapture,
) -> OverlayResult<Option<ExternalCaptureRecord>> {
    if !db.profile.has_external_captures() || db.durable.is_none() || !identity.valid() {
        return Err(OverlayError::Invalid(
            "external capture capability or identity",
        ));
    }
    let args: &[&dyn rusqlite::ToSql] = &[
        &identity.id.as_slice(),
        &identity.scope_digest.as_slice(),
        &identity.purpose.code(),
    ];
    let sizes: Vec<(i64, i64, i64, i64)> = db.query(
        StatementKind::Frontier,
        "SELECT count(*),coalesce(max(length(snapshot)),0),coalesce(max(length(request)),0),\
         coalesce(max(length(target_snapshot)),0) FROM durable_external_capture \
         WHERE capture_id=?1 AND scope_digest=?2 AND purpose=?3",
        args,
        0,
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    match sizes.as_slice() {
        [(0, 0, 0, 0)] => return Ok(None),
        [(1, snapshot, request, target)]
            if (1..=4096).contains(snapshot)
                && (0..=65536).contains(request)
                && (0..=4096).contains(target) =>
        {
            ()
        }
        _ => return Err(OverlayError::Invalid("external capture record size")),
    }
    let rows: Vec<Row> = db.query(
        StatementKind::Frontier,
        "SELECT attempt,workspace,catalog,incarnation,namespace,generation,revision,base_root,snapshot,\
         request,request_digest,candidate_root,target_snapshot,disposition FROM durable_external_capture \
         WHERE capture_id=?1 AND scope_digest=?2 AND purpose=?3",
        args, 0, |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,
            row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?,row.get(11)?,row.get(12)?,row.get(13)?)),
    )?;
    let [(
        attempt,
        workspace,
        catalog,
        incarnation,
        namespace,
        generation,
        revision,
        base_root,
        snapshot,
        request,
        digest,
        candidate,
        target,
        disposition,
    )] = rows
        .try_into()
        .map_err(|_| OverlayError::Invalid("external capture record"))?;
    let positive = |value: i64| {
        u64::try_from(value)
            .ok()
            .filter(|v| *v > 0)
            .ok_or(OverlayError::Invalid("external capture record number"))
    };
    let attempt = fixed(attempt)?;
    let workspace = fixed(workspace)?;
    let catalog = fixed(catalog)?;
    let base_root = fixed(base_root)?;
    positive(incarnation)?;
    positive(namespace)?;
    positive(generation)?;
    if revision < 0 {
        return Err(OverlayError::Invalid("external capture revision"));
    }
    let original = crate::Capture {
        engine: db.engine(),
        generation: crate::Generation(generation),
        revision,
        base_root,
    };
    if super::durable_attempt::identity(
        &workspace,
        &catalog,
        incarnation,
        namespace,
        original,
        &snapshot,
        None,
    ) != attempt
    {
        return Err(OverlayError::Invalid("external capture attempt identity"));
    }
    let publication = match (request, digest, candidate, target) {
        (None, None, None, None) => None,
        (Some(request), Some(digest), Some(candidate), Some(target_snapshot)) => {
            if !request.starts_with(b"LFCP\x01") || target_snapshot.is_empty() {
                return Err(OverlayError::Invalid("external capture publication"));
            }
            let request_digest = fixed(digest)?;
            let mut hash = blake3::Hasher::new();
            hash.update(b"layerfs/checkpoint-request/v1\0");
            hash.update(&request);
            if *hash.finalize().as_bytes() != request_digest {
                return Err(OverlayError::Invalid("external capture request digest"));
            }
            Some(ExternalCapturePublication {
                request,
                request_digest,
                candidate_root: fixed(candidate)?,
                target_snapshot,
            })
        }
        _ => return Err(OverlayError::Invalid("external capture publication shape")),
    };
    Ok(Some(ExternalCaptureRecord {
        identity,
        attempt,
        workspace,
        catalog,
        incarnation: positive(incarnation)?,
        namespace: positive(namespace)?,
        generation: positive(generation)?,
        revision: u64::try_from(revision)
            .map_err(|_| OverlayError::Invalid("external capture revision"))?,
        base_root,
        original_snapshot: snapshot,
        publication,
        disposition: match disposition {
            0 => ExternalCaptureDisposition::Unresolved,
            1 => ExternalCaptureDisposition::Unpublished,
            _ => return Err(OverlayError::Invalid("external capture disposition")),
        },
    }))
}
