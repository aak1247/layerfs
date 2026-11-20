//! An unrouteable observation of one original, captured checkpoint.
use crate::{
    Capture, CapturedReader, OperationOwner, Overlay, OverlayError, OverlayResult, RecordedCapture,
    StatementKind,
};
use std::fmt;

const REQUEST_PAGE: u64 = 64 * 1024;
type CustodyRows = (bool, bool, bool, bool, bool, bool, bool, bool);
type OperationRow = (i64, i64, i64, i64, i64, Vec<u8>, i64, i64);

/// Locally observed facts. The actual Store and the original external writer
/// fence must be authenticated independently before any reconciliation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRecord {
    pub workspace: [u8; 32],
    pub catalog: [u8; 32],
    pub incarnation: u64,
    pub namespace: i64,
    pub generation: i64,
    pub revision: i64,
    pub base_root: [u8; 32],
    pub snapshot: Vec<u8>,
    pub attempt: [u8; 32],
    pub phase: i64,
    pub request_bytes: Option<u64>,
    pub request_digest: Option<[u8; 32]>,
    pub candidate_root: Option<[u8; 32]>,
    pub target_snapshot: Option<Vec<u8>>,
    pub reader_held: bool,
    pub operation_held: bool,
}

/// Owns the original lease and internally reconstructed custody, but cannot
/// create a route, advance a namespace, or release the original checkpoint.
pub struct PendingCheckpointRecovery {
    db: Overlay,
    observed: CheckpointRecord,
    _recorded: RecordedCapture,
    _reader: Option<CapturedReader>,
    _operation: Option<OperationOwner>,
}

impl fmt::Debug for PendingCheckpointRecovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingCheckpointRecovery")
            .field("observed", &self.observed)
            .finish_non_exhaustive()
    }
}

struct Row {
    attempt: Vec<u8>,
    workspace: Vec<u8>,
    catalog: Vec<u8>,
    incarnation: i64,
    namespace: i64,
    generation: i64,
    revision: i64,
    base_root: Vec<u8>,
    snapshot: Vec<u8>,
    phase: i64,
    request_bytes: Option<i64>,
    request_digest: Option<Vec<u8>>,
    candidate_root: Option<Vec<u8>>,
    target_snapshot: Option<Vec<u8>>,
    reader: Option<i64>,
    reader_request: Option<i64>,
    reader_floor: Option<i64>,
    reader_released: Option<i64>,
    operation: Option<i64>,
    operation_released: Option<i64>,
    operation_completed: Option<i64>,
    conflict_generation: Option<i64>,
    conflict_completed: Option<i64>,
}

fn fixed(value: Vec<u8>) -> OverlayResult<[u8; 32]> {
    value
        .try_into()
        .map_err(|_| OverlayError::Invalid("checkpoint recovery identity"))
}

fn positive(value: i64) -> OverlayResult<u64> {
    u64::try_from(value)
        .ok()
        .filter(|n| *n > 0)
        .ok_or(OverlayError::Invalid("checkpoint recovery owner"))
}

impl PendingCheckpointRecovery {
    pub(super) fn open(db: Overlay) -> OverlayResult<Self> {
        let state = db.state()?;
        let rows: Vec<Row> = db.query(
            StatementKind::Frontier,
            "SELECT attempt,workspace,catalog,incarnation,namespace,generation,revision,base_root,snapshot,phase,\
             length(request),request_digest,candidate_root,target_snapshot,reader_owner,reader_request,reader_floor,\
             reader_released,operation_owner,operation_released,operation_cleanup_completed,conflict_generation,conflict_cleanup_completed \
             FROM durable_checkpoint_attempt WHERE id=1",
            &[], 256,
            |r| Ok(Row { attempt:r.get(0)?, workspace:r.get(1)?, catalog:r.get(2)?, incarnation:r.get(3)?,
                namespace:r.get(4)?, generation:r.get(5)?, revision:r.get(6)?, base_root:r.get(7)?,
                snapshot:r.get(8)?, phase:r.get(9)?, request_bytes:r.get(10)?, request_digest:r.get(11)?,
                candidate_root:r.get(12)?, target_snapshot:r.get(13)?, reader:r.get(14)?,
                reader_request:r.get(15)?, reader_floor:r.get(16)?, reader_released:r.get(17)?,
                operation:r.get(18)?, operation_released:r.get(19)?, operation_completed:r.get(20)?,
                conflict_generation:r.get(21)?, conflict_completed:r.get(22)? }),
        )?;
        let [row] = rows.as_slice() else {
            return Err(OverlayError::Invalid("checkpoint recovery predecessor"));
        };
        let workspace = fixed(row.workspace.clone())?;
        let catalog = fixed(row.catalog.clone())?;
        let attempt = fixed(row.attempt.clone())?;
        let base_root = fixed(row.base_root.clone())?;
        let incarnation = positive(row.incarnation)?;
        let generation = state
            .captured
            .ok_or(OverlayError::Invalid("checkpoint recovery capture"))?;
        if generation.number() != row.generation
            || state.captured_revision != Some(row.revision)
            || state.base_root != base_root
            || row.generation.checked_add(1) != Some(state.active.number())
            || state.installed >= row.generation
            || state.revision < row.revision
            || db.durable_namespace()? != Some(row.namespace)
        {
            return Err(OverlayError::Invalid("checkpoint recovery capture"));
        }
        let capture = Capture {
            engine: db.engine(),
            generation,
            revision: row.revision,
            base_root,
        };
        let recorded = RecordedCapture { capture, attempt };
        if !matches!(row.phase, 1 | 2)
            || row.conflict_generation.is_some()
            || row.conflict_completed.is_some()
        {
            return Err(OverlayError::Invalid("checkpoint recovery phase"));
        }
        super::durable_attempt::check_context(&db, recorded, false)?;
        let (request_bytes, request_digest, candidate_root, target_snapshot) = match row.phase {
            1 if row.request_bytes.is_none()
                && row.request_digest.is_none()
                && row.candidate_root.is_none()
                && row.target_snapshot.is_none() =>
            {
                (None, None, None, None)
            }
            2 => {
                let length = positive(
                    row.request_bytes
                        .ok_or(OverlayError::Invalid("checkpoint recovery request"))?,
                )?;
                let digest = fixed(
                    row.request_digest
                        .clone()
                        .ok_or(OverlayError::Invalid("checkpoint recovery request"))?,
                )?;
                let candidate = fixed(
                    row.candidate_root
                        .clone()
                        .ok_or(OverlayError::Invalid("checkpoint recovery request"))?,
                )?;
                let target = row
                    .target_snapshot
                    .clone()
                    .filter(|v| !v.is_empty() && v.len() <= 4096)
                    .ok_or(OverlayError::Invalid("checkpoint recovery request"))?;
                (Some(length), Some(digest), Some(candidate), Some(target))
            }
            _ => return Err(OverlayError::Invalid("checkpoint recovery phase")),
        };
        let (reader, reader_held) = match (
            row.reader,
            row.reader_request,
            row.reader_floor,
            row.reader_released,
        ) {
            (None, None, None, None) => (None, false),
            (Some(owner), Some(request), Some(floor), Some(released))
                if request == row.generation
                    && floor == state.installed
                    && matches!(released, 0 | 1) =>
            {
                (
                    Some(CapturedReader {
                        capture,
                        owner: positive(owner)?,
                        installed: floor,
                        recorded_attempt: Some(attempt),
                    }),
                    released == 0,
                )
            }
            _ => return Err(OverlayError::Invalid("checkpoint recovery reader")),
        };
        let (operation, operation_held) = match (
            row.operation,
            row.operation_released,
            row.operation_completed,
        ) {
            (None, None, None) => (None, false),
            (Some(owner), Some(released), Some(completed))
                if reader.is_some()
                    && matches!(released, 0 | 1)
                    && matches!(completed, 0 | 1)
                    && (released == 1 || completed == 0)
                    && (released == 0 || !reader_held)
                    && row.reader != Some(owner) =>
            {
                (
                    Some(OperationOwner {
                        engine: db.engine(),
                        owner: positive(owner)?,
                        recorded: Some(recorded),
                    }),
                    released == 0,
                )
            }
            _ => return Err(OverlayError::Invalid("checkpoint recovery operation")),
        };
        validate_custody(&db, row, reader_held, operation_held)?;
        {
            let mut open = db.open.borrow_mut();
            if reader_held {
                let reader = reader.ok_or(OverlayError::Invalid("checkpoint recovery reader"))?;
                open.captured.insert(
                    reader.owner,
                    row.generation,
                    reader,
                    "checkpoint recovery reader",
                )?;
                open.captured_generations.put(row.generation, 1);
            }
            if operation_held {
                let operation =
                    operation.ok_or(OverlayError::Invalid("checkpoint recovery operation"))?;
                open.operations.insert(
                    operation.owner,
                    row.generation,
                    operation,
                    "checkpoint recovery operation",
                )?;
            }
            open.commit();
        }
        Ok(Self {
            observed: CheckpointRecord {
                workspace,
                catalog,
                incarnation,
                namespace: row.namespace,
                generation: row.generation,
                revision: row.revision,
                base_root,
                snapshot: row.snapshot.clone(),
                attempt,
                phase: row.phase,
                request_bytes,
                request_digest,
                candidate_root,
                target_snapshot,
                reader_held,
                operation_held,
            },
            db,
            _recorded: recorded,
            _reader: reader,
            _operation: operation,
        })
    }

    pub fn observed(&self) -> &CheckpointRecord {
        &self.observed
    }

    /// Reads one bounded segment of the frozen request under the same lease.
    pub fn request_page(&self, offset: u64) -> OverlayResult<Vec<u8>> {
        let total = self
            .observed
            .request_bytes
            .ok_or(OverlayError::Invalid("checkpoint recovery request"))?;
        if offset >= total {
            return Err(OverlayError::Invalid("checkpoint recovery request offset"));
        }
        let wanted = (total - offset).min(REQUEST_PAGE);
        let start = i64::try_from(
            offset
                .checked_add(1)
                .ok_or(OverlayError::Invalid("checkpoint recovery request offset"))?,
        )
        .map_err(|_| OverlayError::Invalid("checkpoint recovery request offset"))?;
        let limit = i64::try_from(wanted)
            .map_err(|_| OverlayError::Invalid("checkpoint recovery request offset"))?;
        let rows:Vec<Vec<u8>> = self.db.query(StatementKind::Frontier,
            "SELECT substr(request,?1,?2) FROM durable_checkpoint_attempt WHERE id=1 AND phase=2 AND attempt=?3 AND namespace=?4",
            &[&start,&limit,&self.observed.attempt.as_slice(),&self.observed.namespace], wanted + 32, |r| r.get(0))?;
        let [page] = rows.as_slice() else {
            return Err(OverlayError::Invalid("checkpoint recovery request"));
        };
        if u64::try_from(page.len()).ok() != Some(wanted) {
            return Err(OverlayError::Invalid("checkpoint recovery request"));
        }
        Ok(page.clone())
    }
}

fn validate_custody(
    db: &Overlay,
    row: &Row,
    reader_held: bool,
    operation_held: bool,
) -> OverlayResult<()> {
    let count:Vec<(i64,i64,bool,bool)> = db.query(StatementKind::Frontier,
        "SELECT c.operation_count,c.orphan_count,d.operation_owners,d.orphan_owners FROM durable_owner_counts c,durable_custody d WHERE c.id=1 AND d.id=1",
        &[], 32, |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    if count.as_slice() != [(i64::from(operation_held), 0, operation_held, false)] {
        return Err(OverlayError::Invalid("checkpoint recovery custody count"));
    }
    let owner = row.operation.unwrap_or(0);
    let rows:Vec<CustodyRows> = db.query(StatementKind::Frontier,
        "SELECT EXISTS(SELECT 1 FROM durable_orphan_owner),\
         EXISTS(SELECT 1 FROM fs_inode WHERE generation=-1) OR EXISTS(SELECT 1 FROM fs_data WHERE generation=-1) OR EXISTS(SELECT 1 FROM fs_data_ref WHERE generation=-1) OR EXISTS(SELECT 1 FROM fs_truncate WHERE generation=-1),\
         EXISTS(SELECT 1 FROM durable_operation_owner WHERE owner<?1) OR EXISTS(SELECT 1 FROM durable_operation_owner WHERE owner>?1),\
         EXISTS(SELECT 1 FROM durable_operation_owner WHERE owner=?1),\
         EXISTS(SELECT 1 FROM commit_record WHERE operation<?1) OR EXISTS(SELECT 1 FROM commit_record WHERE operation>?1),\
         EXISTS(SELECT 1 FROM commit_record WHERE operation=?1),\
         EXISTS(SELECT 1 FROM cleanup_task WHERE kind=6 AND resource=?1 AND target=2),\
         EXISTS(SELECT 1 FROM cleanup_task WHERE ready=0) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind IN(1,2,7,8,10,11,13)) \
         OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=9 AND resource>0) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=9 AND resource=0 AND target<?2) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=9 AND resource=0 AND target>?2) \
         OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=6 AND resource<?1) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=6 AND resource>?1) \
         OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=6 AND resource=?1 AND target<2) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=6 AND resource=?1 AND target>2) \
         OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=3 AND resource>0) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=3 AND resource=0 AND target<=0) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=3 AND resource=0 AND target>?3)",
        &[&owner,&row.generation,&db.state()?.installed], 64,
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?)))?;
    let [(
        orphan,
        orphan_rows,
        other_owner,
        exact_owner,
        other_records,
        exact_records,
        cleanup,
        other_cleanup,
    )] = rows.as_slice()
    else {
        return Err(OverlayError::Invalid("checkpoint recovery custody"));
    };
    if *orphan
        || *orphan_rows
        || *other_owner
        || *other_records
        || *other_cleanup
        || *exact_owner != operation_held
    {
        return Err(OverlayError::Invalid("checkpoint recovery custody"));
    }
    if row.operation.is_none() && (*exact_records || *cleanup) {
        return Err(OverlayError::Invalid(
            "checkpoint recovery operation records",
        ));
    }
    if operation_held {
        if *cleanup {
            return Err(OverlayError::Invalid(
                "checkpoint recovery operation cleanup",
            ));
        }
        let original:Vec<OperationRow> = db.query(StatementKind::Frontier,
            "SELECT owner,namespace,request,purpose,capture_generation,capture_root,capture_revision,capture_floor FROM durable_operation_owner WHERE owner=?1 AND reader_owner=?2",
            &[&owner,&row.reader], 96, |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?)))?;
        if original.as_slice()
            != [(
                owner,
                row.namespace,
                row.generation,
                2,
                row.generation,
                row.base_root.clone(),
                row.revision,
                row.reader_floor.unwrap_or(-1),
            )]
        {
            return Err(OverlayError::Invalid(
                "checkpoint recovery operation identity",
            ));
        }
    } else if row.operation.is_some() {
        match (
            row.operation_released,
            row.operation_completed,
            *cleanup,
            *exact_records,
        ) {
            (Some(1), Some(0), true, _) | (Some(1), Some(1), false, false) => {}
            _ => {
                return Err(OverlayError::Invalid(
                    "checkpoint recovery operation cleanup",
                ))
            }
        }
    }
    let wake: Vec<bool> = db.query(
        StatementKind::Frontier,
        "SELECT EXISTS(SELECT 1 FROM cleanup_task WHERE kind=9 AND resource=0 AND target=?1)",
        &[&row.generation],
        8,
        |r| r.get(0),
    )?;
    if wake.as_slice() == [true] && (reader_held || row.reader_released != Some(1)) {
        return Err(OverlayError::Invalid("checkpoint recovery reader wake"));
    }
    Ok(())
}
