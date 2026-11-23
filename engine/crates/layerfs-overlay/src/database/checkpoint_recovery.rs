//! An unrouteable observation of one original, captured checkpoint.
use crate::{
    Capture, CapturedReader, CheckpointCompletion, CheckpointOutcome, Generation,
    MaintenanceCursor, MaintenanceStep, OperationOwner, Overlay, OverlayError, OverlayResult,
    RecordedCapture, StatementKind,
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
    pub installed_snapshot: Option<Vec<u8>>,
    pub outcome: Option<CheckpointOutcome>,
    pub reader_held: bool,
    pub operation_held: bool,
}

/// Owns the original lease and internally reconstructed custody. Its local
/// operations cannot authenticate a Store or advance a Workspace namespace.
pub struct PendingCheckpointRecovery {
    db: Overlay,
    observed: CheckpointRecord,
    recorded: RecordedCapture,
    reader: Option<CapturedReader>,
    operation: Option<OperationOwner>,
    reader_held: bool,
    operation_held: bool,
    phase: i64,
    installed: Option<(Vec<u8>, CheckpointOutcome)>,
    request_verified: bool,
    orphans: Option<super::durable_orphan_recovery::MixedOrphans>,
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
    installed_snapshot: Option<Vec<u8>>,
    outcome_kind: Option<i64>,
    outcome_head: Option<Vec<u8>>,
    outcome_displaced: Option<Vec<u8>>,
    fold_completed: Option<i64>,
    retire_completed: Option<i64>,
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

fn commit_id(value: Vec<u8>) -> OverlayResult<[u8; 33]> {
    let value: [u8; 33] = value
        .try_into()
        .map_err(|_| OverlayError::Invalid("checkpoint recovery outcome"))?;
    if value[0] != 0x12 {
        return Err(OverlayError::Invalid("checkpoint recovery outcome"));
    }
    Ok(value)
}

impl PendingCheckpointRecovery {
    /// Observe the original locally recorded identity while custody remains
    /// unresolved. Store receipt and caller authority are separate checks.
    pub fn observe_external_capture(
        &self,
        identity: crate::ExternalCapture,
    ) -> OverlayResult<Option<crate::ExternalCaptureRecord>> {
        self.db.observe_external_capture(identity)
    }

    pub(super) fn open(db: Overlay) -> OverlayResult<Self> {
        Self::open_with_orphans(db, false)
    }

    pub(super) fn open_mixed(db: Overlay) -> OverlayResult<Self> {
        Self::open_with_orphans(db, true)
    }

    fn open_with_orphans(db: Overlay, mixed: bool) -> OverlayResult<Self> {
        let state = db.state()?;
        let rows: Vec<Row> = db.query(
            StatementKind::Frontier,
            "SELECT attempt,workspace,catalog,incarnation,namespace,generation,revision,base_root,snapshot,phase,\
             length(request),request_digest,candidate_root,target_snapshot,reader_owner,reader_request,reader_floor,\
             reader_released,operation_owner,operation_released,operation_cleanup_completed,conflict_generation,conflict_cleanup_completed,\
             installed_snapshot,outcome_kind,outcome_head,outcome_displaced,fold_completed,retire_completed \
             FROM durable_checkpoint_attempt WHERE id=1",
            &[], 256,
            |r| Ok(Row { attempt:r.get(0)?, workspace:r.get(1)?, catalog:r.get(2)?, incarnation:r.get(3)?,
                namespace:r.get(4)?, generation:r.get(5)?, revision:r.get(6)?, base_root:r.get(7)?,
                snapshot:r.get(8)?, phase:r.get(9)?, request_bytes:r.get(10)?, request_digest:r.get(11)?,
                candidate_root:r.get(12)?, target_snapshot:r.get(13)?, reader:r.get(14)?,
                reader_request:r.get(15)?, reader_floor:r.get(16)?, reader_released:r.get(17)?,
                operation:r.get(18)?, operation_released:r.get(19)?, operation_completed:r.get(20)?,
                conflict_generation:r.get(21)?, conflict_completed:r.get(22)?,
                installed_snapshot:r.get(23)?, outcome_kind:r.get(24)?, outcome_head:r.get(25)?, outcome_displaced:r.get(26)?,
                fold_completed:r.get(27)?, retire_completed:r.get(28)? }),
        )?;
        let [row] = rows.as_slice() else {
            return Err(OverlayError::Invalid("checkpoint recovery predecessor"));
        };
        let workspace = fixed(row.workspace.clone())?;
        let catalog = fixed(row.catalog.clone())?;
        let attempt = fixed(row.attempt.clone())?;
        let base_root = fixed(row.base_root.clone())?;
        let incarnation = positive(row.incarnation)?;
        if row.generation <= 0 || db.durable_namespace()? != Some(row.namespace) {
            return Err(OverlayError::Invalid("checkpoint recovery capture"));
        }
        let capture = Capture {
            engine: db.engine(),
            generation: Generation(row.generation),
            revision: row.revision,
            base_root,
        };
        let recorded = RecordedCapture { capture, attempt };
        if !matches!(row.phase, 1 | 2 | 3 | 4 | 6)
            || row.conflict_generation.is_some()
            || row.conflict_completed.is_some()
        {
            return Err(OverlayError::Invalid("checkpoint recovery phase"));
        }
        if matches!(row.phase, 1 | 2)
            && (row.generation.checked_add(1) != Some(state.active.number())
                || state.installed >= row.generation
                || state.revision < row.revision
                || state.consolidating.is_some())
        {
            return Err(OverlayError::Invalid("checkpoint recovery capture"));
        }
        super::durable_attempt::check_context(&db, recorded, false)?;
        let (request_bytes, request_digest, candidate_root, target_snapshot) = match row.phase {
            1 | 3
                if row.request_bytes.is_none()
                    && row.request_digest.is_none()
                    && row.candidate_root.is_none()
                    && row.target_snapshot.is_none() =>
            {
                (None, None, None, None)
            }
            2 | 4 | 6 => {
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
        let (installed_snapshot, outcome) = match row.phase {
            4 => {
                let snapshot = row
                    .installed_snapshot
                    .clone()
                    .filter(|value| !value.is_empty() && value.len() <= 4096)
                    .ok_or(OverlayError::Invalid("checkpoint recovery outcome"))?;
                let head = row.outcome_head.clone().map(commit_id).transpose()?;
                let displaced = row.outcome_displaced.clone().map(commit_id).transpose()?;
                let outcome = match (row.outcome_kind, head, displaced) {
                    (Some(1), Some(head), displaced) => {
                        CheckpointOutcome::Committed { head, displaced }
                    }
                    (Some(2), head, None) => CheckpointOutcome::UpToDate { head },
                    _ => return Err(OverlayError::Invalid("checkpoint recovery outcome")),
                };
                (Some(snapshot), Some(outcome))
            }
            _ if row.installed_snapshot.is_none()
                && row.outcome_kind.is_none()
                && row.outcome_head.is_none()
                && row.outcome_displaced.is_none() =>
            {
                (None, None)
            }
            _ => return Err(OverlayError::Invalid("checkpoint recovery outcome")),
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
                    && if row.phase == 4 {
                        floor >= 0 && floor < row.generation
                    } else {
                        floor == state.installed
                    }
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
        let (orphans, orphan_rows) = if mixed {
            let (orphans, rows) = super::durable_orphan_recovery::MixedOrphans::inspect(
                &db,
                workspace,
                operation_held,
            )?;
            (Some(orphans), rows)
        } else {
            (None, Default::default())
        };
        validate_custody(&db, row, reader_held, operation_held, mixed)?;
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
            for (serial, orphan) in orphan_rows {
                open.orphans.put(serial, orphan);
            }
            db.orphan_seen.set(!open.orphans.rows.is_empty());
            open.commit();
        }
        if let Some(orphans) = &orphans {
            super::mixed_checkpoint_queue::check_mixed_queue(
                &db,
                orphans,
                recorded,
                row.phase,
                row.operation,
            )?;
        }
        let installed = installed_snapshot.clone().zip(outcome);
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
                installed_snapshot,
                outcome,
                reader_held,
                operation_held,
            },
            db,
            recorded,
            reader,
            operation,
            reader_held,
            operation_held,
            phase: row.phase,
            installed,
            request_verified: false,
            orphans,
        })
    }

    pub fn observed(&self) -> &CheckpointRecord {
        &self.observed
    }

    /// The original orphan facts, when this same leased capture has them.
    pub fn orphan_record(&self) -> Option<&crate::OrphanRecoveryRecord> {
        self.orphans.as_ref().map(|value| &value.observed)
    }

    pub fn orphan_owner_roots(&self, after: i64) -> OverlayResult<Vec<(i64, [u8; 32])>> {
        self.orphans
            .as_ref()
            .ok_or(OverlayError::Invalid("mixed orphan recovery absent"))?
            .owner_roots(&self.db, after)
    }

    pub fn orphan_retirement_roots(
        &self,
        after: (i64, i64),
    ) -> OverlayResult<Vec<(i64, i64, [u8; 32])>> {
        self.orphans
            .as_ref()
            .ok_or(OverlayError::Invalid("mixed orphan recovery absent"))?
            .retirement_roots(&self.db, after)
    }

    fn operation_id(&self) -> OverlayResult<Option<i64>> {
        self.operation
            .map(|owner| {
                i64::try_from(owner.owner)
                    .map_err(|_| OverlayError::Invalid("checkpoint recovery operation"))
            })
            .transpose()
    }

    /// Recheck both local domains against the current binding. The caller
    /// authenticates that binding and the original roots against the Store.
    pub fn verify_mixed_orphans(&self, current: &crate::OrphanRecoveryRecord) -> OverlayResult<()> {
        let orphans = self
            .orphans
            .as_ref()
            .ok_or(OverlayError::Invalid("mixed orphan recovery absent"))?;
        orphans.verify_current(&self.db, current, self.operation_held)?;
        super::mixed_checkpoint_queue::check_mixed_queue(
            &self.db,
            orphans,
            self.recorded,
            self.phase,
            self.operation_id()?,
        )
    }

    /// Schedule only the previously authenticated orphan records, after the
    /// original checkpoint owners have been released.
    pub fn schedule_mixed_orphans(
        &mut self,
        current: &crate::OrphanRecoveryRecord,
        root_serial: u64,
    ) -> OverlayResult<()> {
        if self.reader_held || self.operation_held || !matches!(self.phase, 3 | 4 | 6) {
            return Err(OverlayError::Invalid("mixed recovery owners held"));
        }
        self.verify_mixed_orphans(current)?;
        self.orphans
            .as_mut()
            .ok_or(OverlayError::Invalid("mixed orphan recovery absent"))?
            .schedule(&self.db, current, root_serial)
    }

    pub fn mixed_orphans_drained(&self) -> OverlayResult<bool> {
        self.orphans
            .as_ref()
            .ok_or(OverlayError::Invalid("mixed orphan recovery absent"))?
            .drained(&self.db)
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
            "SELECT substr(request,?1,?2) FROM durable_checkpoint_attempt WHERE id=1 AND phase IN(2,4,6) AND attempt=?3 AND namespace=?4",
            &[&start,&limit,&self.observed.attempt.as_slice(),&self.observed.namespace], wanted + 32, |r| r.get(0))?;
        let [page] = rows.as_slice() else {
            return Err(OverlayError::Invalid("checkpoint recovery request"));
        };
        if u64::try_from(page.len()).ok() != Some(wanted) {
            return Err(OverlayError::Invalid("checkpoint recovery request"));
        }
        Ok(page.clone())
    }

    /// The original local phase; it is not an external publication result.
    pub fn phase(&self) -> i64 {
        self.phase
    }

    fn verify_request(&self, request: &[u8]) -> OverlayResult<()> {
        let length = u64::try_from(request.len())
            .map_err(|_| OverlayError::Invalid("checkpoint recovery request"))?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"layerfs/checkpoint-request/v1\0");
        hash.update(request);
        if self.observed.request_bytes != Some(length)
            || self.observed.request_digest != Some(*hash.finalize().as_bytes())
        {
            return Err(OverlayError::Invalid("checkpoint recovery request"));
        }
        let mut offset = 0_u64;
        for expected in request.chunks(REQUEST_PAGE as usize) {
            if self.request_page(offset)? != expected {
                return Err(OverlayError::Invalid("checkpoint recovery request"));
            }
            offset += expected.len() as u64;
        }
        Ok(())
    }

    /// Validate the entire frozen request in bounded local pages. This is
    /// only an input identity check and does not authenticate a Store.
    pub fn verify_frozen_request(&mut self, request: &[u8]) -> OverlayResult<()> {
        self.verify_request(request)?;
        self.request_verified = true;
        Ok(())
    }

    /// Schedule the original phase-1 capture for local folding. The caller
    /// must independently fence the previous writer and authenticate Store.
    pub fn fold_before_publication(&mut self) -> OverlayResult<()> {
        if self.phase != 1 {
            return Err(OverlayError::Invalid("checkpoint recovery fold phase"));
        }
        self.db.fold_unpublished_checkpoint(self.recorded)?;
        self.phase = 3;
        Ok(())
    }

    /// Locally schedule phase 2 for folding while preserving its complete
    /// request. The caller must establish nonpublication from the same Store
    /// while holding its publication lane and fencing all original writers.
    pub fn fold_after_absent_receipt(&mut self) -> OverlayResult<()> {
        if self.phase != 2 || !self.request_verified {
            return Err(OverlayError::Invalid("checkpoint recovery fold phase"));
        }
        let record = &self.observed;
        let length = i64::try_from(
            record
                .request_bytes
                .ok_or(OverlayError::Invalid("checkpoint recovery request"))?,
        )
        .map_err(|_| OverlayError::Invalid("checkpoint recovery request"))?;
        let digest = record
            .request_digest
            .ok_or(OverlayError::Invalid("checkpoint recovery request"))?;
        let candidate = record
            .candidate_root
            .ok_or(OverlayError::Invalid("checkpoint recovery request"))?;
        let target = record
            .target_snapshot
            .as_deref()
            .ok_or(OverlayError::Invalid("checkpoint recovery request"))?;
        self.db.atomic_cleanup(|| {
            let namespace = super::durable_attempt::check_context(&self.db, self.recorded, false)?;
            super::durable_conflict_stage::require_no_stage(&self.db, self.recorded, 2)?;
            let changed = self.db.execute(
                StatementKind::Frontier,
                "UPDATE durable_checkpoint_attempt SET phase=6,fold_completed=0,retire_completed=0 \
                 WHERE id=1 AND phase=2 AND attempt=?1 AND namespace=?2 AND generation=?3 \
                 AND length(request)=?4 AND request_digest=?5 AND candidate_root=?6 AND target_snapshot=?7",
                &[&record.attempt.as_slice(), &namespace, &record.generation, &length,
                  &digest.as_slice(), &candidate.as_slice(), &target],
                target.len() as u64 + 120,
            )?;
            if changed != 1 {
                return Err(OverlayError::Invalid("checkpoint recovery fold predecessor"));
            }
            super::durable_capture::mark_unpublished(&self.db, self.recorded.attempt)?;
            self.db.schedule_failed_capture(self.recorded.capture)
        })?;
        self.phase = 6;
        Ok(())
    }

    /// Install locally after the caller authenticates the exact Store result
    /// and candidate. The outcome fields themselves are not Store authority.
    pub fn install_original_receipt(
        &mut self,
        installed_snapshot: &[u8],
        outcome: CheckpointOutcome,
    ) -> OverlayResult<()> {
        if self.phase != 2 || !self.request_verified {
            return Err(OverlayError::Invalid("checkpoint recovery install phase"));
        }
        let digest = self
            .observed
            .request_digest
            .ok_or(OverlayError::Invalid("checkpoint recovery request"))?;
        let candidate = self
            .observed
            .candidate_root
            .ok_or(OverlayError::Invalid("checkpoint recovery request"))?;
        let target = self
            .observed
            .target_snapshot
            .as_deref()
            .ok_or(OverlayError::Invalid("checkpoint recovery request"))?;
        let snapshot = installed_snapshot.to_vec();
        self.db.install_recorded_checkpoint(
            self.recorded,
            digest,
            candidate,
            target,
            &snapshot,
            outcome,
        )?;
        self.installed = Some((snapshot, outcome));
        self.phase = 4;
        Ok(())
    }

    /// Releases only the original, locally authenticated owners, in order.
    /// On error this object keeps the original lease and all remaining state.
    pub fn release_original_owners(&mut self) -> OverlayResult<()> {
        if !matches!(self.phase, 3 | 4 | 6) {
            return Err(OverlayError::Invalid("checkpoint recovery release phase"));
        }
        if self.reader_held {
            self.db.release_captured_reader(
                self.reader
                    .ok_or(OverlayError::Invalid("checkpoint recovery reader"))?,
            )?;
            self.reader_held = false;
        }
        if self.operation_held {
            self.db.release_operation(
                self.operation
                    .ok_or(OverlayError::Invalid("checkpoint recovery operation"))?,
            )?;
            self.operation_held = false;
        }
        Ok(())
    }

    fn completion<'a>(&'a self, request: &'a [u8]) -> OverlayResult<CheckpointCompletion<'a>> {
        let (snapshot, outcome) = self
            .installed
            .as_ref()
            .ok_or(OverlayError::Invalid("checkpoint recovery outcome"))?;
        Ok(CheckpointCompletion {
            recorded: self.recorded,
            request,
            request_digest: self
                .observed
                .request_digest
                .ok_or(OverlayError::Invalid("checkpoint recovery request"))?,
            candidate_root: self
                .observed
                .candidate_root
                .ok_or(OverlayError::Invalid("checkpoint recovery request"))?,
            target_snapshot: self
                .observed
                .target_snapshot
                .as_deref()
                .ok_or(OverlayError::Invalid("checkpoint recovery request"))?,
            installed_snapshot: snapshot,
            outcome: *outcome,
            reader: self.reader,
            operation: self.operation,
        })
    }

    /// A bounded local readiness observation; it does not drive maintenance.
    pub fn cleanup_ready(&self, request: Option<&[u8]>) -> OverlayResult<bool> {
        let installed =
            if self.phase == 4 {
                Some(self.completion(
                    request.ok_or(OverlayError::Invalid("checkpoint recovery request"))?,
                )?)
            } else {
                None
            };
        super::durable_cleanup::recovery_ready(&self.db, self.recorded, self.phase, installed)
    }

    /// One bounded local maintenance step; no old business call is sent.
    pub fn maintain(
        &mut self,
        cursor: MaintenanceCursor,
    ) -> OverlayResult<Option<MaintenanceStep>> {
        if !matches!(self.phase, 3 | 4 | 6) || self.reader_held || self.operation_held {
            return Err(OverlayError::Invalid(
                "checkpoint recovery maintenance phase",
            ));
        }
        if let Some(orphans) = &self.orphans {
            if !orphans.scheduled {
                return Err(OverlayError::Invalid(
                    "mixed orphan recovery is not scheduled",
                ));
            }
            let operation = self.operation_id()?;
            self.db.maintain_checked(cursor, |db, item| {
                super::mixed_checkpoint_queue::check_mixed_item(
                    db,
                    orphans,
                    self.recorded,
                    self.phase,
                    operation,
                    item,
                    true,
                )
            })
        } else {
            self.db.maintain(cursor)
        }
    }

    /// Completes local settlement without advancing the namespace. The caller
    /// must independently revalidate the same Store outcome before calling.
    #[allow(clippy::result_large_err)]
    pub fn complete_settled(
        self,
        request: Option<&[u8]>,
        installed: Option<(&[u8], CheckpointOutcome)>,
    ) -> Result<Overlay, (OverlayError, Self)> {
        let result = (|| {
            if self.reader_held || self.operation_held {
                return Err(OverlayError::Invalid("checkpoint recovery owners held"));
            }
            if self
                .orphans
                .as_ref()
                .is_some_and(|orphans| !orphans.scheduled)
            {
                return Err(OverlayError::Invalid(
                    "mixed orphan recovery is not scheduled",
                ));
            }
            if let Some(orphans) = &self.orphans {
                if !orphans.drained(&self.db)? {
                    return Err(OverlayError::Invalid("mixed orphan recovery pending"));
                }
            }
            let completion = match self.phase {
                3 if request.is_none() && installed.is_none() => None,
                6 if installed.is_none() => {
                    self.verify_request(
                        request.ok_or(OverlayError::Invalid("checkpoint recovery request"))?,
                    )?;
                    None
                }
                4 => {
                    let request =
                        request.ok_or(OverlayError::Invalid("checkpoint recovery request"))?;
                    self.verify_request(request)?;
                    let completion = self.completion(request)?;
                    if installed != Some((completion.installed_snapshot, completion.outcome)) {
                        return Err(OverlayError::Invalid(
                            "checkpoint recovery installed outcome",
                        ));
                    }
                    Some(completion)
                }
                _ => {
                    return Err(OverlayError::Invalid(
                        "checkpoint recovery completion phase",
                    ))
                }
            };
            let (root, snapshot) = match completion {
                Some(value) => (value.candidate_root, value.installed_snapshot),
                None => (self.observed.base_root, self.observed.snapshot.as_slice()),
            };
            super::durable_cleanup::finish_recovery(
                &self.db,
                self.recorded,
                self.phase,
                completion,
                root,
                snapshot,
            )
        })();
        match result {
            Ok(()) => Ok(self.db),
            Err(error) => Err((error, self)),
        }
    }
}

fn validate_custody(
    db: &Overlay,
    row: &Row,
    reader_held: bool,
    operation_held: bool,
    mixed: bool,
) -> OverlayResult<()> {
    let count:Vec<(i64,i64,bool,bool)> = db.query(StatementKind::Frontier,
        "SELECT c.operation_count,c.orphan_count,d.operation_owners,d.orphan_owners FROM durable_owner_counts c,durable_custody d WHERE c.id=1 AND d.id=1",
        &[], 32, |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    let counts_valid = match count.as_slice() {
        [(operation, orphan, operation_bit, orphan_bit)] => {
            *operation == i64::from(operation_held)
                && *operation_bit == operation_held
                && *orphan >= 0
                && *orphan_bit == (*orphan > 0)
                && (mixed || *orphan == 0)
        }
        _ => false,
    };
    if !counts_valid {
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
         EXISTS(SELECT 1 FROM cleanup_task WHERE ready=0 AND NOT (resource=0 AND target=?2 AND ((kind=1 AND ?4 IN(3,6)) OR (kind=3 AND ?4 IN(3,4,6))))) \
         OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind IN(2,7,10,11,13)) \
         OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=1 AND NOT (resource=0 AND target=?2 AND ?4 IN(3,6))) \
         OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=8 AND NOT (resource=0 AND target=?2 AND ?4 IN(3,4,6))) \
         OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=9 AND resource>0) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=9 AND resource=0 AND target<?2) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=9 AND resource=0 AND target>?2) \
         OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=6 AND resource<?1) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=6 AND resource>?1) \
         OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=6 AND resource=?1 AND target<2) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=6 AND resource=?1 AND target>2) \
         OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=3 AND resource>0) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=3 AND resource=0 AND target<=0) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=3 AND resource=0 AND target>?3 AND NOT (target=?2 AND ?4 IN(3,4,6)))",
        &[&owner,&row.generation,&db.state()?.installed,&row.phase], 64,
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
    if (!mixed && (*orphan || *orphan_rows || *other_cleanup))
        || *other_owner
        || *other_records
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
    let work: Vec<(bool, bool)> = db.query(
        StatementKind::Frontier,
        "SELECT EXISTS(SELECT 1 FROM cleanup_task WHERE kind=1 AND resource=0 AND target=?1), \
         EXISTS(SELECT 1 FROM cleanup_task WHERE kind=3 AND resource=0 AND target=?1)",
        &[&row.generation],
        16,
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let state = db.state()?;
    let consistent = match (
        row.phase,
        row.fold_completed,
        row.retire_completed,
        work.as_slice(),
    ) {
        (1 | 2, None, None, [(false, false)]) => true,
        (3 | 6, Some(0), Some(0), [(true, false)]) => {
            state.consolidating == Some(Generation(row.generation))
        }
        (3 | 6, Some(1), Some(0), [(false, true)]) => state.consolidating.is_none(),
        (3 | 6, Some(1), Some(1), [(false, false)]) => state.consolidating.is_none(),
        (4, None, Some(0), [(false, true)]) | (4, None, Some(1), [(false, false)]) => true,
        _ => false,
    };
    if !consistent {
        return Err(OverlayError::Invalid("checkpoint recovery maintenance"));
    }
    Ok(())
}
