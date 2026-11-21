//! Non-routable, lease-owning inspection and bounded orphan settlement.
//!
//! The caller must first authenticate the original Store and every reported
//! content root, and fence the original writer, mount and all consumers. Local
//! records alone never prove those external facts.
use crate::{
    lifetime::orphan::Orphan,
    maintenance::{Item, ORPHAN},
    MaintenanceCursor, MaintenanceStep, Overlay, OverlayError, OverlayResult, StatementKind,
};
use std::{collections::BTreeMap, fmt};

const PAGE: usize = 64;

/// Original binding facts, before any namespace is advanced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrphanRecoveryRecord {
    pub workspace: [u8; 32],
    pub previous_namespace: i64,
    pub root: [u8; 32],
    pub snapshot: Vec<u8>,
}

/// The original database and exclusive lease, without a route or mount.
pub struct PendingOrphanRecovery {
    db: Overlay,
    observed: OrphanRecoveryRecord,
    conflict: Option<crate::SettledConflict>,
    scheduled: bool,
}
impl fmt::Debug for PendingOrphanRecovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingOrphanRecovery")
            .field("observed", &self.observed)
            .finish_non_exhaustive()
    }
}

pub(crate) fn pending(db: &Overlay) -> OverlayResult<bool> {
    if !db.profile().has_serial_retirement_origin() {
        return Ok(false);
    }
    let rows: Vec<bool> = db.query(
        StatementKind::Startup,
        "SELECT EXISTS(SELECT 1 FROM durable_orphan_owner) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind IN(7,8,9))",
        &[],
        8,
        |row| row.get(0),
    )?;
    match rows.as_slice() {
        [value] => Ok(*value),
        _ => Err(OverlayError::Invalid("orphan recovery state")),
    }
}

fn root(bytes: Vec<u8>) -> OverlayResult<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| OverlayError::Invalid("orphan recovery root"))
}

type OwnerRow = (i64, Vec<u8>, i64, i64, bool);
type QueueRow = (i64, i64, i64, i64, i64, i64, Vec<u8>, bool);

/// The orphan side of a mixed recovery. The Overlay and lease remain owned
/// by the checkpoint side; no file handles or routes are reconstructed.
pub(super) struct MixedOrphans {
    pub(super) observed: OrphanRecoveryRecord,
    pub(super) scheduled: bool,
}

impl MixedOrphans {
    pub(super) fn inspect(
        db: &Overlay,
        workspace: [u8; 32],
        operation_held: bool,
    ) -> OverlayResult<(Self, BTreeMap<i64, Orphan>)> {
        let state = db.state()?;
        if !db.profile().has_serial_retirement_origin() || state.closed {
            return Err(OverlayError::Invalid("orphan recovery predecessor"));
        }
        let observed = OrphanRecoveryRecord {
            workspace,
            previous_namespace: db
                .durable_namespace()?
                .filter(|namespace| *namespace > 0)
                .ok_or(OverlayError::Invalid("orphan recovery namespace"))?,
            root: state.base_root,
            snapshot: db
                .durable_binding()?
                .ok_or(OverlayError::Invalid("orphan recovery binding"))?,
        };
        let rows = owners(db, Some(operation_held))?;
        Ok((
            Self {
                observed,
                scheduled: false,
            },
            rows,
        ))
    }

    pub(super) fn owner_roots(
        &self,
        db: &Overlay,
        after: i64,
    ) -> OverlayResult<Vec<(i64, [u8; 32])>> {
        if after < 0 {
            return Err(OverlayError::Invalid("orphan recovery cursor"));
        }
        Ok(db
            .open
            .borrow()
            .orphans
            .rows
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .take(PAGE)
            .map(|(serial, orphan)| (*serial, orphan.root))
            .collect())
    }

    pub(super) fn verify_current(
        &self,
        db: &Overlay,
        current: &OrphanRecoveryRecord,
        operation_held: bool,
    ) -> OverlayResult<()> {
        if current.workspace != self.observed.workspace
            || current.previous_namespace != self.observed.previous_namespace
            || db.durable_namespace()? != Some(current.previous_namespace)
            || db.durable_binding()?.as_deref() != Some(current.snapshot.as_slice())
            || db.state()?.base_root != current.root
            || owners(db, Some(operation_held))? != db.open.borrow().orphans.rows
        {
            return Err(OverlayError::Invalid("orphan recovery predecessor"));
        }
        Ok(())
    }

    pub(super) fn retirement_roots(
        &self,
        db: &Overlay,
        after: (i64, i64),
    ) -> OverlayResult<Vec<(i64, i64, [u8; 32])>> {
        if after.0 < 0 || after.1 < 0 {
            return Err(OverlayError::Invalid("retirement recovery cursor"));
        }
        let rows: Vec<(i64, i64, Vec<u8>)> = db.query(StatementKind::Startup,
            "SELECT resource,target,name FROM cleanup_task WHERE kind=7 AND (resource,target)>(?1,?2) ORDER BY resource,target LIMIT 64",
            &[&after.0,&after.1], 192 * 64, |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
        rows.into_iter()
            .map(|(serial, generation, bytes)| {
                Ok((
                    serial,
                    generation,
                    check_retirement(db, &self.observed, serial, generation, &bytes)?,
                ))
            })
            .collect()
    }

    pub(super) fn schedule(
        &mut self,
        db: &Overlay,
        current: &OrphanRecoveryRecord,
        root_serial: u64,
    ) -> OverlayResult<()> {
        let root_serial = i64::try_from(root_serial)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(OverlayError::Invalid("orphan recovery root inode"))?;
        self.verify_current(db, current, false)?;
        let mut owner_cursor = 0;
        loop {
            let rows = self.owner_roots(db, owner_cursor)?;
            if rows.is_empty() {
                break;
            }
            for (serial, _) in rows {
                if serial == root_serial {
                    return Err(OverlayError::Invalid("orphan recovery root inode"));
                }
                owner_cursor = serial;
            }
        }
        let mut task_cursor = (0, 0);
        loop {
            let rows = self.retirement_roots(db, task_cursor)?;
            if rows.is_empty() {
                break;
            }
            for (serial, generation, _) in rows {
                if serial == root_serial {
                    return Err(OverlayError::Invalid("orphan recovery root inode"));
                }
                task_cursor = (serial, generation);
            }
        }
        let mut after = 0;
        loop {
            let rows = self.owner_roots(db, after)?;
            if rows.is_empty() {
                break;
            }
            for (serial, _) in rows {
                db.atomic_cleanup(|| {
                    if db.orphan(serial)?.is_none() {
                        return Err(OverlayError::Invalid("orphan recovery owner changed"));
                    }
                    db.enqueue(ORPHAN, serial, -1)?;
                    db.wake_orphan(serial)
                })?;
                after = serial;
            }
        }
        let mut cursor = (0, 0);
        loop {
            let rows = self.retirement_roots(db, cursor)?;
            if rows.is_empty() {
                break;
            }
            for (serial, generation, _) in rows {
                db.atomic_cleanup(|| {
                    let changed = db.execute(StatementKind::Reclaim,
                        "UPDATE cleanup_task SET ready=1 WHERE kind=7 AND resource=?1 AND target=?2",
                        &[&serial,&generation],16)?;
                    if changed != 1 { return Err(OverlayError::Invalid("serial retirement recovery changed")); }
                    db.maintenance_ready.set(true);
                    Ok(())
                })?;
                cursor = (serial, generation);
            }
        }
        self.scheduled = true;
        Ok(())
    }

    pub(super) fn drained(&self, db: &Overlay) -> OverlayResult<bool> {
        if !self.scheduled {
            return Ok(false);
        }
        let rows: Vec<bool> = db.query(StatementKind::Frontier,
            "SELECT EXISTS(SELECT 1 FROM durable_orphan_owner) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind IN(2,7,8,9))",
            &[],8,|row| row.get(0))?;
        match rows.as_slice() {
            [pending] => Ok(!pending),
            _ => Err(OverlayError::Invalid("orphan recovery state")),
        }
    }
}

// A mixed checkpoint supplies the independently checked operation count;
// ordinary orphan recovery still requires that no operation exists.
fn owners(
    db: &Overlay,
    checkpoint_operation: Option<bool>,
) -> OverlayResult<BTreeMap<i64, Orphan>> {
    let state = db.state()?;
    let counts: Vec<(i64, i64, bool, bool)> = db.query(
        StatementKind::Startup,
        "SELECT o.orphan_count,o.operation_count,c.orphan_owners,c.operation_owners \
         FROM durable_owner_counts o JOIN durable_custody c ON c.id=o.id WHERE o.id=1",
        &[],
        32,
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    let [(expected, operations, orphan_bit, operation_bit)] = counts.as_slice() else {
        return Err(OverlayError::Invalid("orphan recovery custody"));
    };
    let expected_operations = i64::from(checkpoint_operation.unwrap_or(false));
    if *expected < 0
        || *operations != expected_operations
        || *operation_bit != (expected_operations != 0)
        || *orphan_bit != (*expected > 0)
    {
        return Err(OverlayError::Invalid("orphan recovery custody"));
    }
    let other: Vec<bool> = db.query(
        StatementKind::Startup,
        "SELECT EXISTS(SELECT 1 FROM durable_operation_owner) OR EXISTS(SELECT 1 FROM commit_record) OR EXISTS(SELECT 1 FROM durable_checkpoint_attempt)",
        &[],
        8,
        |row| row.get(0),
    )?;
    if checkpoint_operation.is_none() && other.as_slice() != [false] {
        return Err(OverlayError::Invalid("orphan recovery other custody"));
    }
    let mut after = 0_i64;
    let mut result = BTreeMap::new();
    loop {
        let rows: Vec<OwnerRow> = db.query(
            StatementKind::Startup,
            "SELECT o.serial,o.root,o.top,o.floor,EXISTS(SELECT 1 FROM fs_inode i \
             WHERE i.inode=o.serial AND i.generation=-1 AND i.nlink=0) \
             FROM durable_orphan_owner o WHERE o.serial>?1 ORDER BY o.serial LIMIT 64",
            &[&after],
            64 * 64,
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
        if rows.is_empty() {
            break;
        }
        for (serial, bytes, top, floor, inode) in rows {
            if serial <= after
                || !inode
                || floor < 0
                || floor > state.installed
                || top < floor
                || top > state.active.0
            {
                return Err(OverlayError::Invalid("orphan recovery owner"));
            }
            if top > state.installed
                && db
                    .local_row(serial, crate::Generation(top), state.installed)?
                    .is_some()
                && db
                    .local_row(serial, state.active, state.installed)?
                    .is_none_or(|row| row.inode.nlink != 0)
            {
                return Err(OverlayError::Invalid("orphan recovery visible inode"));
            }
            after = serial;
            result.insert(
                serial,
                Orphan {
                    root: root(bytes)?,
                    top,
                    floor,
                },
            );
        }
    }
    if i64::try_from(result.len()).ok() != Some(*expected) {
        return Err(OverlayError::Invalid("orphan recovery owner count"));
    }
    // Every orphan-domain row must belong to one recorded owner, including
    // payload or truncate rows even if its inode record was lost.
    for sql in [
        "SELECT DISTINCT inode FROM fs_inode INDEXED BY fs_inode_capture WHERE generation=-1 AND inode>?1 ORDER BY inode LIMIT 64",
        "SELECT DISTINCT inode FROM fs_data INDEXED BY fs_data_generation WHERE generation=-1 AND inode>?1 ORDER BY inode LIMIT 64",
        "SELECT DISTINCT inode FROM fs_data_ref WHERE generation=-1 AND inode>?1 ORDER BY inode LIMIT 64",
        "SELECT DISTINCT inode FROM fs_truncate INDEXED BY fs_truncate_generation WHERE generation=-1 AND inode>?1 ORDER BY inode LIMIT 64",
    ] {
        let mut serial = 0_i64;
        loop {
            let rows: Vec<i64> = db.query(StatementKind::Startup, sql, &[&serial], 8 * 64, |row| row.get(0))?;
            if rows.is_empty() {
                break;
            }
            for next in rows {
                if next <= serial || !result.contains_key(&next) {
                    return Err(OverlayError::Invalid("orphan recovery unowned row"));
                }
                serial = next;
            }
        }
    }
    Ok(result)
}

fn check_retirement(
    db: &Overlay,
    expected: &OrphanRecoveryRecord,
    serial: i64,
    generation: i64,
    bytes: &[u8],
) -> OverlayResult<[u8; 32]> {
    let origin = super::durable_serial_retirement::decode(bytes)?;
    let state = db.state()?;
    if origin.workspace != expected.workspace
        || origin.namespace != expected.previous_namespace
        || origin.serial != serial
        || origin.generation != generation
        || origin.active > state.active.0
        || origin.floor > state.installed
        || origin.revision > state.revision
        || (origin.kind == 1 && origin.revision == state.revision)
    {
        return Err(OverlayError::Invalid("serial retirement recovery identity"));
    }
    if let Some(orphan) = db.orphan(serial)? {
        if origin.kind != 2
            || orphan.root != origin.root
            || orphan.floor != origin.floor
            || orphan.top > origin.top_after
            || orphan.holds(generation)
        {
            return Err(OverlayError::Invalid("serial retirement recovery owner"));
        }
    }
    if generation > state.installed {
        let row = db.local_row(serial, state.active, state.installed)?;
        if row.is_none_or(|row| row.inode.nlink != 0) {
            return Err(OverlayError::Invalid("serial retirement visible inode"));
        }
    }
    Ok(origin.root)
}

fn empty_orphan_domain(db: &Overlay, serial: i64) -> OverlayResult<bool> {
    let rows: Vec<bool> = db.query(
        StatementKind::Startup,
        "SELECT EXISTS(SELECT 1 FROM fs_inode WHERE inode=?1 AND generation=-1) \
         OR EXISTS(SELECT 1 FROM fs_data WHERE inode=?1 AND generation=-1) \
         OR EXISTS(SELECT 1 FROM fs_data_ref WHERE generation=-1 AND inode=?1) \
         OR EXISTS(SELECT 1 FROM fs_truncate WHERE inode=?1 AND generation=-1)",
        &[&serial],
        8,
        |row| row.get(0),
    )?;
    match rows.as_slice() {
        [exists] => Ok(!exists),
        _ => Err(OverlayError::Invalid("orphan recovery domain")),
    }
}

pub(super) fn check_item(
    db: &Overlay,
    expected: &OrphanRecoveryRecord,
    item: &Item,
    ready: bool,
) -> OverlayResult<()> {
    let state = db.state()?;
    match item.kind {
        2 if item.resource > 0
            && item.target == -1
            && db.orphan(item.resource)?.is_some()
            && item.name.is_empty()
            && item.phase <= state.active.0 =>
        {
            Ok(())
        }
        3 if item.resource == 0 && item.target > 0 && item.target <= state.installed && ready => {
            Ok(())
        }
        4 | 5 | 12
            if item.resource > 0
                && ready
                && (item.target > 0 && item.target <= state.active.0
                    || item.target == -1
                        && (db.orphan(item.resource)?.is_some()
                            || empty_orphan_domain(db, item.resource)?)) =>
        {
            Ok(())
        }
        7 if item.resource > 0
            && item.target > 0
            && item.phase == 0
            && item.cursor == 0
            && item.aux == -1 =>
        {
            check_retirement(db, expected, item.resource, item.target, &item.name)?;
            Ok(())
        }
        8 if item.resource == 0
            && item.target > 0
            && item.target <= state.active.0
            && ready
            && item.phase == 0
            && item.cursor == 0
            && item.aux == -1
            && item.name.is_empty() =>
        {
            Ok(())
        }
        9 if item.resource == 0
            && item.target > 0
            && item.target <= state.active.0
            && ready
            && item.phase == 0
            && item.name.is_empty()
            && ((item.cursor == 0 && item.aux == -1)
                || (matches!(item.cursor, 1 | 3 | 7) && item.aux >= 0)) =>
        {
            Ok(())
        }
        _ => Err(OverlayError::Invalid("orphan recovery cleanup")),
    }
}

fn check_queue(db: &Overlay, expected: &OrphanRecoveryRecord) -> OverlayResult<()> {
    let mut after = (0_i64, 0_i64, 0_i64);
    loop {
        let rows: Vec<QueueRow> = db.query(
            StatementKind::Startup,
            "SELECT kind,resource,target,phase,cursor,aux,name,ready FROM cleanup_task \
             WHERE (kind,resource,target)>(?1,?2,?3) ORDER BY kind,resource,target LIMIT 64",
            &[&after.0, &after.1, &after.2],
            512 * 64,
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
        if rows.is_empty() {
            return Ok(());
        }
        for (kind, resource, target, phase, cursor, aux, name, ready) in rows {
            let key = (kind, resource, target);
            if key <= after {
                return Err(OverlayError::Invalid("orphan recovery cleanup order"));
            }
            after = key;
            check_item(
                db,
                expected,
                &Item {
                    kind,
                    resource,
                    target,
                    phase,
                    cursor,
                    aux,
                    name,
                },
                ready,
            )?;
        }
    }
}

impl PendingOrphanRecovery {
    pub(crate) fn open(db: Overlay, workspace: [u8; 32]) -> OverlayResult<Self> {
        let state = db.state()?;
        if !db.profile().has_serial_retirement_origin()
            || state.closed
            || state.captured.is_some()
            || state.consolidating.is_some()
        {
            return Err(OverlayError::Invalid("orphan recovery predecessor"));
        }
        let observed = OrphanRecoveryRecord {
            workspace,
            previous_namespace: db
                .durable_namespace()?
                .filter(|namespace| *namespace > 0)
                .ok_or(OverlayError::Invalid("orphan recovery namespace"))?,
            root: state.base_root,
            snapshot: db
                .durable_binding()?
                .ok_or(OverlayError::Invalid("orphan recovery binding"))?,
        };
        let conflict = if super::durable_conflict_stage::active_conflict(&db)?.is_some() {
            let conflict = super::pending_recovery::observe(&db)?;
            if conflict.workspace != observed.workspace
                || conflict.previous_namespace != observed.previous_namespace
                || conflict.marker_root != observed.root
                || conflict.target_snapshot != observed.snapshot
            {
                return Err(OverlayError::Invalid("mixed conflict orphan binding"));
            }
            super::pending_recovery::verify_conflict(
                &db,
                &conflict,
                &conflict,
                observed.previous_namespace,
            )?;
            Some(conflict)
        } else {
            None
        };
        let rows = owners(&db, None)?;
        // Restore only the exact persisted orphan holders. This is an
        // in-memory baseline under the same lease, before any SQL write.
        {
            let mut open = db.open.borrow_mut();
            for (serial, orphan) in rows {
                open.orphans.put(serial, orphan);
            }
            open.commit();
            db.orphan_seen.set(!open.orphans.rows.is_empty());
        }
        check_queue(&db, &observed)?;
        Ok(Self {
            db,
            observed,
            conflict,
            scheduled: false,
        })
    }

    pub fn observed(&self) -> &OrphanRecoveryRecord {
        &self.observed
    }

    /// A local conflict projection requires separate Store authentication.
    pub fn conflict_record(&self) -> Option<&crate::SettledConflict> {
        self.conflict.as_ref()
    }

    pub fn conflict_page(&self, number: u64) -> OverlayResult<Vec<u8>> {
        let conflict = self
            .conflict
            .as_ref()
            .ok_or(OverlayError::Invalid("mixed conflict recovery absent"))?;
        super::pending_recovery::conflict_page(&self.db, conflict, number)
    }

    /// One bounded page of original orphan roots, for the actual Store check.
    pub fn owner_roots(&self, after: i64) -> OverlayResult<Vec<(i64, [u8; 32])>> {
        if after < 0 {
            return Err(OverlayError::Invalid("orphan recovery cursor"));
        }
        Ok(self
            .db
            .open
            .borrow()
            .orphans
            .rows
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .take(PAGE)
            .map(|(serial, orphan)| (*serial, orphan.root))
            .collect())
    }

    /// One bounded page of original queued retirement roots.
    pub fn retirement_roots(&self, after: (i64, i64)) -> OverlayResult<Vec<(i64, i64, [u8; 32])>> {
        if after.0 < 0 || after.1 < 0 {
            return Err(OverlayError::Invalid("retirement recovery cursor"));
        }
        let rows: Vec<(i64,i64,Vec<u8>)> = self.db.query(StatementKind::Startup,
            "SELECT resource,target,name FROM cleanup_task WHERE kind=7 AND (resource,target)>(?1,?2) ORDER BY resource,target LIMIT 64",
            &[&after.0,&after.1], 192 * 64, |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
        rows.into_iter()
            .map(|(serial, generation, bytes)| {
                Ok((
                    serial,
                    generation,
                    check_retirement(&self.db, &self.observed, serial, generation, &bytes)?,
                ))
            })
            .collect()
    }

    /// Once the caller has validated every original root and fenced all old
    /// consumers, schedule each authenticated orphan and wake its original
    /// retirement items. No owner or file handle is recreated for a caller.
    pub fn schedule(&mut self, root_serial: u64) -> OverlayResult<()> {
        let root_serial = i64::try_from(root_serial)
            .map_err(|_| OverlayError::Invalid("orphan recovery root inode"))?;
        if root_serial <= 0 {
            return Err(OverlayError::Invalid("orphan recovery root inode"));
        }
        if let Some(conflict) = &self.conflict {
            super::pending_recovery::verify_conflict(
                &self.db,
                conflict,
                conflict,
                self.observed.previous_namespace,
            )?;
        }
        if self.db.durable_namespace()? != Some(self.observed.previous_namespace)
            || self.db.durable_binding()?.as_deref() != Some(self.observed.snapshot.as_slice())
            || self.db.state()?.base_root != self.observed.root
            || owners(&self.db, None)? != self.db.open.borrow().orphans.rows
        {
            return Err(OverlayError::Invalid("orphan recovery predecessor"));
        }
        check_queue(&self.db, &self.observed)?;
        // Check every original serial before scheduling the first row. The
        // Store has separately authenticated the roots and this root serial.
        let mut owner_cursor = 0;
        loop {
            let rows = self.owner_roots(owner_cursor)?;
            if rows.is_empty() {
                break;
            }
            for (serial, _) in rows {
                if serial == root_serial {
                    return Err(OverlayError::Invalid("orphan recovery root inode"));
                }
                owner_cursor = serial;
            }
        }
        let mut task_cursor = (0, 0);
        loop {
            let rows = self.retirement_roots(task_cursor)?;
            if rows.is_empty() {
                break;
            }
            for (serial, generation, _) in rows {
                if serial == root_serial {
                    return Err(OverlayError::Invalid("orphan recovery root inode"));
                }
                task_cursor = (serial, generation);
            }
        }
        let mut after = 0;
        loop {
            let rows = self.owner_roots(after)?;
            if rows.is_empty() {
                break;
            }
            for (serial, _) in rows {
                self.db.atomic_cleanup(|| {
                    if self.db.orphan(serial)?.is_none() {
                        return Err(OverlayError::Invalid("orphan recovery owner changed"));
                    }
                    self.db.enqueue(ORPHAN, serial, -1)?;
                    self.db.wake_orphan(serial)
                })?;
                after = serial;
            }
        }
        let mut cursor = (0, 0);
        loop {
            let rows = self.retirement_roots(cursor)?;
            if rows.is_empty() {
                break;
            }
            for (serial, generation, _) in rows {
                self.db.atomic_cleanup(|| {
                    let changed = self.db.execute(StatementKind::Reclaim,
                        "UPDATE cleanup_task SET ready=1 WHERE kind=7 AND resource=?1 AND target=?2",
                        &[&serial,&generation],16)?;
                    if changed != 1 { return Err(OverlayError::Invalid("serial retirement recovery changed")); }
                    self.db.maintenance_ready.set(true);
                    Ok(())
                })?;
                cursor = (serial, generation);
            }
        }
        self.scheduled = true;
        Ok(())
    }

    pub fn maintain(&self, after: MaintenanceCursor) -> OverlayResult<Option<MaintenanceStep>> {
        if !self.scheduled {
            return Err(OverlayError::Invalid("orphan recovery is not scheduled"));
        }
        self.db
            .maintain_checked(after, |db, item| check_item(db, &self.observed, item, true))
    }

    pub fn ready(&self) -> OverlayResult<bool> {
        if !self.scheduled {
            return Ok(false);
        }
        let rows: Vec<bool> = self.db.query(StatementKind::Frontier,
            "SELECT EXISTS(SELECT 1 FROM durable_orphan_owner) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind IN(2,7,8,9))",
            &[],8,|row| row.get(0))?;
        if rows.as_slice() != [false] {
            return Ok(false);
        }
        if let Some(conflict) = &self.conflict {
            super::pending_recovery::verify_conflict(
                &self.db,
                conflict,
                conflict,
                self.observed.previous_namespace,
            )?;
            self.db
                .require_conflict_synchronize_ready(&crate::ConflictSyncIdentity {
                    workspace: conflict.workspace,
                    catalog: conflict.catalog,
                    incarnation: conflict.incarnation,
                    namespace: conflict.previous_namespace,
                    marker_root: conflict.marker_root,
                    target_snapshot: &conflict.target_snapshot,
                    descriptor: conflict.descriptor,
                })?;
        } else {
            self.db.require_recorded_recovery_ready()?;
        }
        Ok(true)
    }

    /// The same local lease remains held; Store authentication and old-writer
    /// fencing are the responsibility of the caller's recovery boundary.
    #[allow(clippy::result_large_err)]
    pub fn complete(
        self,
        expected: &OrphanRecoveryRecord,
    ) -> Result<Overlay, (OverlayError, Self)> {
        let checked = (|| {
            if &self.observed != expected
                || self.db.durable_namespace()? != Some(expected.previous_namespace)
                || self.db.durable_binding()?.as_deref() != Some(expected.snapshot.as_slice())
                || self.db.state()?.base_root != expected.root
                || !self.ready()?
            {
                return Err(OverlayError::Invalid("orphan recovery completion"));
            }
            Ok(())
        })();
        match checked {
            Ok(()) => Ok(self.db),
            Err(error) => Err((error, self)),
        }
    }
}
