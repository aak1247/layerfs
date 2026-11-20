//! Bounded prepublication staging. These records grant no Store authority.
use crate::{
    conflict_page_digest, ConflictStage, ConflictStageInput, Overlay, OverlayError, OverlayResult,
    RecordedCapture, StatementKind, CONFLICT_PAGE_BYTES,
};
type BindingRow = (Vec<u8>, Vec<u8>, bool, Option<i64>, Option<Vec<u8>>);
type SuccessorRow = (Vec<u8>, i64, Option<Vec<u8>>, Vec<u8>, Vec<u8>, i64);
type StagingRow = (Option<i64>, Option<i64>, bool, bool, bool);

struct Plan {
    attempt: Vec<u8>,
    workspace: Vec<u8>,
    catalog: Vec<u8>,
    incarnation: i64,
    namespace: i64,
    root: Vec<u8>,
    target: Vec<u8>,
    paths: i64,
    bytes: i64,
    pages: i64,
    final_digest: Vec<u8>,
    descriptor: Vec<u8>,
    next_page: i64,
    written: i64,
    chain: Vec<u8>,
    state: i64,
}

/// An active conflict's immutable local and history identities. This is not
/// authorization to open either root or to trust a caller-supplied Store.
pub(crate) struct ActiveConflict {
    pub generation: i64,
    pub attempt: Vec<u8>,
    pub workspace: Vec<u8>,
    pub catalog: Vec<u8>,
    pub incarnation: i64,
    pub root: Vec<u8>,
    pub target: Vec<u8>,
    pub digest: [u8; 32],
    pub namespace: i64,
    pub paths: i64,
    pub bytes: i64,
    pub pages: i64,
    pub final_digest: Vec<u8>,
}

fn descriptor(generation: i64, plan: &Plan) -> OverlayResult<[u8; 32]> {
    if plan.attempt.len() != 32
        || plan.workspace.len() != 32
        || plan.catalog.len() != 32
        || plan.root.len() != 32
        || plan.final_digest.len() != 32
        || plan.incarnation <= 0
        || plan.namespace <= 0
        || plan.paths <= 0
        || plan.bytes <= 0
        || plan.pages <= 0
        || plan.target.is_empty()
        || plan.target.len() > 4096
        || plan.pages != 1 + (plan.bytes - 1) / CONFLICT_PAGE_BYTES as i64
    {
        return Err(OverlayError::Invalid("conflict descriptor fields"));
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs/conflict-descriptor/v1\0");
    for bytes in [&plan.workspace, &plan.catalog] {
        hash.update(bytes);
    }
    for number in [plan.incarnation, plan.namespace, generation] {
        hash.update(&number.to_be_bytes());
    }
    hash.update(&plan.attempt);
    hash.update(&plan.root);
    hash.update(&(plan.target.len() as u64).to_be_bytes());
    hash.update(&plan.target);
    for number in [plan.paths, plan.bytes, plan.pages] {
        hash.update(&number.to_be_bytes());
    }
    hash.update(&plan.final_digest);
    Ok(*hash.finalize().as_bytes())
}

fn read_plan(db: &Overlay, generation: i64) -> OverlayResult<Plan> {
    let mut rows: Vec<Plan> = db.query(
        StatementKind::Frontier,
        "SELECT attempt,workspace,catalog,incarnation,namespace,local_root,target_snapshot,paths_count,byte_count,page_count,final_digest,descriptor_digest,next_page,written_bytes,chain,state FROM durable_conflict_plan WHERE generation=?1",
        &[&generation], 4320,
        |r| Ok(Plan { attempt:r.get(0)?,workspace:r.get(1)?,catalog:r.get(2)?,incarnation:r.get(3)?,namespace:r.get(4)?,root:r.get(5)?,target:r.get(6)?,paths:r.get(7)?,bytes:r.get(8)?,pages:r.get(9)?,final_digest:r.get(10)?,descriptor:r.get(11)?,next_page:r.get(12)?,written:r.get(13)?,chain:r.get(14)?,state:r.get(15)? }),
    )?;
    if rows.len() != 1 {
        return Err(OverlayError::Invalid("conflict plan predecessor"));
    }
    Ok(rows.remove(0))
}

/// Validates the fixed active descriptor without scanning its paged content.
/// Staging made each page immutable and proved the complete chain; cold bind
/// must independently read and validate that chain before exposing a view.
pub(crate) fn active_conflict(db: &Overlay) -> OverlayResult<Option<ActiveConflict>> {
    if db.profile.schema_version != 44 {
        return Ok(None);
    }
    let binding: Vec<BindingRow> = db.query(
        StatementKind::Frontier,
        "SELECT root,snapshot,recoverable,conflict_generation,conflict_digest FROM durable_binding WHERE id=1",
        &[], 128, |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
    )?;
    let [(root, snapshot, true, generation, digest)] = binding.as_slice() else {
        return Err(OverlayError::Invalid("conflict binding identity"));
    };
    let (generation, digest) = match (generation, digest) {
        (None, None) => {
            let rows: Vec<bool> = db.query(
                StatementKind::Frontier,
                "SELECT EXISTS(SELECT 1 FROM durable_conflict_plan WHERE state=1)",
                &[],
                8,
                |r| r.get(0),
            )?;
            if rows.as_slice() != [false] {
                return Err(OverlayError::Invalid("conflict active binding"));
            }
            return Ok(None);
        }
        (Some(g), Some(d)) => (*g, d),
        _ => return Err(OverlayError::Invalid("conflict binding identity")),
    };
    let plan = read_plan(db, generation)?;
    let actual = descriptor(generation, &plan)?;
    let state = db.state()?;
    let identity: Vec<Vec<u8>> = db.query(
        StatementKind::Frontier,
        "SELECT identity FROM durable_identity WHERE id=1",
        &[],
        32,
        |r| r.get(0),
    )?;
    let current_namespace = db
        .durable_namespace()?
        .ok_or(OverlayError::Invalid("conflict namespace"))?;
    if plan.state != 1
        || plan.next_page != plan.pages
        || plan.written != plan.bytes
        || plan.chain != plan.final_digest
        || plan.descriptor != actual
        || digest.as_slice() != actual
        || plan.root != *root
        || plan.target != *snapshot
        || plan.root.as_slice() != state.base_root
        || generation != state.installed
        || plan.namespace > current_namespace
        || identity.as_slice() != [plan.workspace.clone()]
    {
        return Err(OverlayError::Invalid("conflict active binding"));
    }
    Ok(Some(ActiveConflict {
        generation,
        attempt: plan.attempt,
        workspace: plan.workspace,
        catalog: plan.catalog,
        incarnation: plan.incarnation,
        root: plan.root,
        target: plan.target,
        digest: actual,
        namespace: plan.namespace,
        paths: plan.paths,
        bytes: plan.bytes,
        pages: plan.pages,
        final_digest: plan.final_digest,
    }))
}

/// A successor's atomic pointer replacement is retained in the original
/// maintenance item, so later attempts need not remain live during GC.
pub(crate) fn check_retired_active(
    db: &Overlay,
    generation: i64,
    next_generation: i64,
    evidence: &[u8],
) -> OverlayResult<()> {
    if evidence.len() != 64
        || next_generation <= generation
        || db.state()?.installed < next_generation
    {
        return Err(OverlayError::Invalid("conflict retired successor"));
    }
    let plan = read_plan(db, generation)?;
    let actual = descriptor(generation, &plan)?;
    let workspace: Vec<Vec<u8>> = db.query(
        StatementKind::Reclaim,
        "SELECT identity FROM durable_identity WHERE id=1",
        &[],
        32,
        |r| r.get(0),
    )?;
    let namespace = db
        .durable_namespace()?
        .ok_or(OverlayError::Invalid("conflict namespace"))?;
    let successor: Vec<SuccessorRow> = db.query(
        StatementKind::Reclaim,
        "SELECT attempt,phase,original_pending_digest,workspace,catalog,incarnation FROM durable_checkpoint_attempt WHERE id=1 AND generation=?1",
        &[&next_generation],
        112,
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)),
    )?;
    if plan.state != 2
        || plan.next_page != plan.pages
        || plan.written != plan.bytes
        || plan.chain != plan.final_digest
        || plan.descriptor != actual
        || evidence[..32] != actual
        || workspace.as_slice() != [plan.workspace.clone()]
        || plan.namespace > namespace
        || successor.len() > 1
        || successor.first().is_some_and(
            |(attempt, phase, pending, workspace, catalog, incarnation)| {
                attempt.as_slice() != &evidence[32..]
                    || !matches!(*phase, 4 | 5)
                    || pending.as_deref() != Some(actual.as_slice())
                    || workspace.as_slice() != plan.workspace
                    || catalog.as_slice() != plan.catalog
                    || *incarnation != plan.incarnation
            },
        )
    {
        return Err(OverlayError::Invalid("conflict retired identity"));
    }
    Ok(())
}

fn require_format(db: &Overlay) -> OverlayResult<()> {
    if db.durable.is_none() || db.profile.schema_version != 44 {
        return Err(OverlayError::Invalid("conflict staging format"));
    }
    Ok(())
}

/// Only one of a staged local conflict and a Store publication may follow the
/// original capture. Run inside the caller's checkpoint transaction.
pub(crate) fn require_no_stage(db: &Overlay, recorded: RecordedCapture) -> OverlayResult<()> {
    if db.profile.schema_version != 44 {
        return Ok(());
    }
    let rows: Vec<StagingRow> = db.query(
        StatementKind::Frontier,
        "SELECT conflict_generation,conflict_cleanup_completed,EXISTS(SELECT 1 FROM durable_conflict_plan WHERE generation=?1),EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation=?1),EXISTS(SELECT 1 FROM cleanup_task WHERE kind=13 AND resource=0 AND target=?1) FROM durable_checkpoint_attempt WHERE id=1 AND attempt=?2 AND phase=1",
        &[&recorded.capture.generation.0, &recorded.attempt.as_slice()], 24,
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
    )?;
    if rows.as_slice() != [(None, None, false, false, false)] {
        return Err(OverlayError::Invalid("checkpoint has conflict staging"));
    }
    Ok(())
}

/// A failed stage is settled only after its exact bounded cleanup completed.
pub(crate) fn cleanup_ready(
    db: &Overlay,
    recorded: RecordedCapture,
    phase: i64,
) -> OverlayResult<bool> {
    if db.profile.schema_version != 44 {
        return Ok(true);
    }
    let generation = recorded.capture.generation.0;
    let rows:Vec<StagingRow>=db.query(StatementKind::Reclaim,
        "SELECT conflict_generation,conflict_cleanup_completed,EXISTS(SELECT 1 FROM durable_conflict_plan WHERE generation=?1),EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation=?1),EXISTS(SELECT 1 FROM cleanup_task WHERE kind=13 AND resource=0 AND target=?1) FROM durable_checkpoint_attempt WHERE id=1 AND attempt=?2 AND phase=?3",
        &[&generation,&recorded.attempt.as_slice(),&phase],40,
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
    match rows.as_slice() {
        [(None, None, false, false, false)] => Ok(true),
        [(Some(g), Some(0), _, _, _)] if *g == generation && phase == 3 => Ok(false),
        [(Some(g), Some(1), false, false, false)] if *g == generation && phase == 3 => Ok(true),
        [(Some(g), Some(0), true, true, false)] if *g == generation && phase == 5 => Ok(true),
        _ => Err(OverlayError::Invalid("conflict cleanup state")),
    }
}

impl Overlay {
    /// Starts one local plan without publishing or installing the conflict.
    /// The caller authenticates the Store, marker root and historical target.
    pub fn begin_conflict_stage(
        &self,
        recorded: RecordedCapture,
        input: ConflictStageInput<'_>,
    ) -> OverlayResult<ConflictStage> {
        require_format(self)?;
        if input.target_snapshot.is_empty() || input.target_snapshot.len() > 4096 {
            return Err(OverlayError::Invalid("conflict target snapshot"));
        }
        let paths = i64::try_from(input.paths_count)
            .ok()
            .filter(|v| *v > 0)
            .ok_or(OverlayError::Invalid("conflict path count"))?;
        let bytes = i64::try_from(input.byte_count)
            .ok()
            .filter(|v| *v > 0)
            .ok_or(OverlayError::Invalid("conflict byte count"))?;
        self.atomic_cleanup(|| {
            let namespace = super::durable_attempt::check_original(self, recorded)?;
            require_no_stage(self, recorded)?;
            let rows: Vec<(Vec<u8>, Vec<u8>, i64)> = self.query(
                StatementKind::Frontier,
                "SELECT workspace,catalog,incarnation FROM durable_checkpoint_attempt WHERE id=1 AND attempt=?1 AND namespace=?2 AND phase=1 AND request IS NULL",
                &[&recorded.attempt.as_slice(),&namespace], 112,
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
            )?;
            let [(workspace,catalog,incarnation)] = rows.as_slice() else {
                return Err(OverlayError::Invalid("conflict staging predecessor"));
            };
            let generation=recorded.capture.generation.0;
            let plan=Plan { attempt:recorded.attempt.to_vec(),workspace:workspace.clone(),catalog:catalog.clone(),incarnation:*incarnation,namespace,root:input.local_root.to_vec(),target:input.target_snapshot.to_vec(),paths,bytes,pages:1+(bytes-1)/CONFLICT_PAGE_BYTES as i64,final_digest:input.final_digest.to_vec(),descriptor:Vec::new(),next_page:0,written:0,chain:vec![0;32],state:0 };
            let digest=descriptor(generation,&plan)?;
            self.execute(StatementKind::Frontier,
                "INSERT INTO durable_conflict_plan(generation,attempt,workspace,catalog,incarnation,namespace,local_root,target_snapshot,paths_count,byte_count,page_count,final_digest,descriptor_digest,next_page,written_bytes,chain,state) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,0,0,zeroblob(32),0)",
                &[&generation,&plan.attempt,&plan.workspace,&plan.catalog,&plan.incarnation,&namespace,&plan.root,&plan.target,&paths,&bytes,&plan.pages,&plan.final_digest,&digest.as_slice()], input.target_snapshot.len() as u64+256)?;
            let changed=self.execute(StatementKind::Frontier,
                "UPDATE durable_checkpoint_attempt SET conflict_generation=?1,conflict_cleanup_completed=0 WHERE id=1 AND attempt=?2 AND namespace=?3 AND phase=1 AND request IS NULL AND conflict_generation IS NULL AND conflict_cleanup_completed IS NULL",
                &[&generation,&recorded.attempt.as_slice(),&namespace], 48)?;
            if changed != 1 { return Err(OverlayError::Invalid("conflict staging predecessor")); }
            Ok(ConflictStage { recorded, descriptor:digest })
        })
    }

    /// Appends exactly one page. A failed final digest leaves all earlier
    /// pages and the original capture unchanged; callers must not replay an
    /// operation with an uncertain outcome.
    pub fn append_conflict_page(
        &self,
        stage: ConflictStage,
        page: u64,
        data: &[u8],
    ) -> OverlayResult<()> {
        require_format(self)?;
        let page =
            i64::try_from(page).map_err(|_| OverlayError::Invalid("conflict page number"))?;
        self.atomic_cleanup(|| {
            let namespace=super::durable_attempt::check_original(self,stage.recorded)?;
            let generation=stage.recorded.capture.generation.0;
            let marker: Vec<(Option<i64>,Option<i64>)>=self.query(StatementKind::Frontier,
                "SELECT conflict_generation,conflict_cleanup_completed FROM durable_checkpoint_attempt WHERE id=1 AND attempt=?1 AND namespace=?2 AND phase=1 AND request IS NULL",
                &[&stage.recorded.attempt.as_slice(),&namespace],16,|r|Ok((r.get(0)?,r.get(1)?)))?;
            let plan=read_plan(self,generation)?;
            if marker.as_slice()!=[(Some(generation),Some(0))] || plan.attempt!=stage.recorded.attempt || plan.namespace!=namespace
                || plan.state!=0 || plan.next_page!=page || page>=plan.pages || plan.descriptor!=stage.descriptor
                || descriptor(generation,&plan)?!=stage.descriptor {
                return Err(OverlayError::Invalid("conflict page predecessor"));
            }
            let expected_prefix=page.checked_mul(CONFLICT_PAGE_BYTES as i64).ok_or(OverlayError::Invalid("conflict page predecessor"))?;
            if plan.written!=expected_prefix || (page==0 && plan.chain.as_slice()!=[0;32]) {
                return Err(OverlayError::Invalid("conflict page predecessor"));
            }
            let expected=if page+1<plan.pages { CONFLICT_PAGE_BYTES as i64 } else { plan.bytes-plan.written };
            if expected<=0 || usize::try_from(expected).ok()!=Some(data.len()) {
                return Err(OverlayError::Invalid("conflict page length"));
            }
            let previous:[u8;32]=plan.chain.as_slice().try_into().map_err(|_|OverlayError::Invalid("conflict page chain"))?;
            let chain=conflict_page_digest(previous,page as u64,data)?;
            if page+1==plan.pages && chain.as_slice()!=plan.final_digest {
                return Err(OverlayError::Invalid("conflict final digest"));
            }
            let written=plan.written.checked_add(expected).ok_or(OverlayError::Invalid("conflict page length"))?;
            self.execute(StatementKind::Frontier,"INSERT INTO durable_conflict_page(generation,page_no,data) VALUES(?1,?2,?3)",&[&generation,&page,&data],data.len() as u64+16)?;
            let changed=self.execute(StatementKind::Frontier,
                "UPDATE durable_conflict_plan SET next_page=?1,written_bytes=?2,chain=?3 WHERE generation=?4 AND attempt=?5 AND descriptor_digest=?6 AND state=0 AND next_page=?7 AND written_bytes=?8 AND chain=?9",
                &[&(page+1),&written,&chain.as_slice(),&generation,&stage.recorded.attempt.as_slice(),&stage.descriptor.as_slice(),&page,&plan.written,&plan.chain],112)?;
            if changed!=1 { return Err(OverlayError::Invalid("conflict page predecessor")); }
            Ok(())
        })
    }

    /// Installs an already staged local conflict without sending History. The
    /// caller authenticates the Store, target snapshot and marker root; this
    /// atomic local transition alone does not authorize a cold recovery.
    pub fn install_recorded_conflict(
        &self,
        stage: ConflictStage,
        marker_root: [u8; 32],
        target_snapshot: &[u8],
    ) -> OverlayResult<()> {
        require_format(self)?;
        if target_snapshot.is_empty() || target_snapshot.len() > 4096 {
            return Err(OverlayError::Invalid("conflict target snapshot"));
        }
        self.atomic_cleanup(|| {
            let namespace=super::durable_attempt::check_original(self,stage.recorded)?;
            if !self.install_ready(stage.recorded.capture)? { return Err(OverlayError::BaseSourcesPending); }
            let generation=stage.recorded.capture.generation.0;
            let marker:Vec<(Option<i64>,Option<i64>,bool)>=self.query(StatementKind::Frontier,
                "SELECT conflict_generation,conflict_cleanup_completed,EXISTS(SELECT 1 FROM cleanup_task WHERE kind=13 AND resource=0 AND target=?1) FROM durable_checkpoint_attempt WHERE id=1 AND attempt=?2 AND namespace=?3 AND phase=1 AND request IS NULL",
                &[&generation,&stage.recorded.attempt.as_slice(),&namespace],24,
                |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
            let plan=read_plan(self,generation)?;
            if marker.as_slice()!=[(Some(generation),Some(0),false)]
                || plan.attempt!=stage.recorded.attempt || plan.namespace!=namespace || plan.state!=0
                || plan.descriptor!=stage.descriptor || descriptor(generation,&plan)?!=stage.descriptor
                || plan.root!=marker_root || plan.target!=target_snapshot || plan.next_page!=plan.pages
                || plan.written!=plan.bytes || plan.chain!=plan.final_digest {
                return Err(OverlayError::Invalid("conflict install predecessor"));
            }
            retire_active(self,stage.recorded)?;
            if self.execute(StatementKind::Frontier,
                "UPDATE durable_conflict_plan SET state=1 WHERE generation=?1 AND attempt=?2 AND descriptor_digest=?3 AND state=0",
                &[&generation,&stage.recorded.attempt.as_slice(),&stage.descriptor.as_slice()],72)?!=1 {
                return Err(OverlayError::Invalid("conflict install predecessor"));
            }
            self.install_captured_inner(stage.recorded.capture,marker_root,Some(target_snapshot))?;
            if self.execute(StatementKind::Frontier,
                "UPDATE durable_binding SET conflict_generation=?1,conflict_digest=?2 WHERE id=1 AND conflict_generation IS NULL AND conflict_digest IS NULL AND root=?3 AND snapshot=?4",
                &[&generation,&stage.descriptor.as_slice(),&marker_root.as_slice(),&target_snapshot],target_snapshot.len() as u64+72)?!=1 {
                return Err(OverlayError::Invalid("conflict install binding"));
            }
            if self.execute(StatementKind::Frontier,
                "UPDATE durable_checkpoint_attempt SET phase=5,retire_completed=0 WHERE id=1 AND attempt=?1 AND namespace=?2 AND generation=?3 AND phase=1 AND conflict_generation=?3 AND conflict_cleanup_completed=0",
                &[&stage.recorded.attempt.as_slice(),&namespace,&generation],48)?!=1 {
                return Err(OverlayError::Invalid("conflict install predecessor"));
            }
            Ok(())
        })
    }
}

/// Clear and retire the previous marker only with its exact active pointer.
/// The caller performs the replacement install in the same transaction.
pub(crate) fn retire_active(db: &Overlay, successor: RecordedCapture) -> OverlayResult<()> {
    let Some(active) = active_conflict(db)? else {
        return Ok(());
    };
    let next_generation = successor.capture.generation.0;
    if next_generation <= active.generation {
        return Err(OverlayError::Invalid("conflict retirement successor"));
    }
    let queued: Vec<bool> = db.query(
        StatementKind::Frontier,
        "SELECT EXISTS(SELECT 1 FROM cleanup_task WHERE kind=13 AND resource=0 AND target=?1)",
        &[&active.generation],
        8,
        |r| r.get(0),
    )?;
    if queued.as_slice() != [false] {
        return Err(OverlayError::Invalid("conflict retirement queue"));
    }
    if db.execute(StatementKind::Frontier,
        "UPDATE durable_binding SET conflict_generation=NULL,conflict_digest=NULL WHERE id=1 AND conflict_generation=?1 AND conflict_digest=?2",
        &[&active.generation,&active.digest.as_slice()],40)?!=1 {
        return Err(OverlayError::Invalid("conflict retirement binding"));
    }
    if db.execute(StatementKind::Frontier,
        "UPDATE durable_conflict_plan SET state=2 WHERE generation=?1 AND descriptor_digest=?2 AND state=1",
        &[&active.generation,&active.digest.as_slice()],40)?!=1 {
        return Err(OverlayError::Invalid("conflict retirement predecessor"));
    }
    let mut evidence = Vec::with_capacity(64);
    evidence.extend_from_slice(&active.digest);
    evidence.extend_from_slice(&successor.attempt);
    if db.execute(StatementKind::Frontier,
        "INSERT INTO cleanup_task(kind,resource,target,phase,cursor,aux,name) VALUES(13,0,?1,1,0,?2,?3)",
        &[&active.generation,&next_generation,&evidence],80)?!=1 {
        return Err(OverlayError::Invalid("conflict retirement queue"));
    }
    db.maintenance_ready.set(true);
    Ok(())
}

/// Retires only this attempt's staging; an older active binding is unaffected.
pub(crate) fn retire_staging(
    db: &Overlay,
    recorded: RecordedCapture,
    namespace: i64,
) -> OverlayResult<()> {
    if db.profile.schema_version != 44 {
        return Ok(());
    }
    let generation = recorded.capture.generation.0;
    let rows:Vec<StagingRow>=db.query(StatementKind::Frontier,
        "SELECT conflict_generation,conflict_cleanup_completed,EXISTS(SELECT 1 FROM durable_conflict_plan WHERE generation=?1),EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation=?1),EXISTS(SELECT 1 FROM cleanup_task WHERE kind=13 AND resource=0 AND target=?1) FROM durable_checkpoint_attempt WHERE id=1 AND attempt=?2 AND namespace=?3 AND phase=1",
        &[&generation,&recorded.attempt.as_slice(),&namespace],40,|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
    match rows.as_slice() {
        [(None, None, false, false, false)] => Ok(()),
        [(Some(g), Some(0), true, _, false)] if *g == generation => {
            let plan = read_plan(db, generation)?;
            if plan.attempt != recorded.attempt
                || plan.namespace != namespace
                || plan.state != 0
                || descriptor(generation, &plan)?.as_slice() != plan.descriptor
            {
                return Err(OverlayError::Invalid("conflict retirement identity"));
            }
            if db.execute(StatementKind::Frontier,"UPDATE durable_conflict_plan SET state=2 WHERE generation=?1 AND attempt=?2 AND state=0",&[&generation,&recorded.attempt.as_slice()],40)?!=1 {
                return Err(OverlayError::Invalid("conflict retirement predecessor"));
            }
            db.enqueue(13, 0, generation)
        }
        _ => Err(OverlayError::Invalid("conflict retirement predecessor")),
    }
}
