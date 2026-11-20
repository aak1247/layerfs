//! Durable identity of live operation and unlinked-inode custody.
use crate::{
    contract::custody::OperationPurpose, db::integer, CapturedReader, Overlay, OverlayError,
    OverlayResult, StatementKind,
};

/// Part of durable schema creation or an explicitly authorized migration.
#[cfg(target_os = "linux")]
pub(super) fn initialize(db: &Overlay) -> OverlayResult<()> {
    for sql in [
        "CREATE TABLE durable_owner_counts(id INTEGER PRIMARY KEY CHECK(id=1), operation_count INTEGER NOT NULL CHECK(operation_count>=0), orphan_count INTEGER NOT NULL CHECK(orphan_count>=0)) STRICT",
        "CREATE TABLE durable_operation_owner(owner INTEGER PRIMARY KEY CHECK(owner>0), namespace INTEGER NOT NULL CHECK(namespace>=0), request INTEGER NOT NULL CHECK(request>0), purpose INTEGER NOT NULL CHECK(purpose IN(0,1,2)), capture_generation INTEGER, capture_revision INTEGER, capture_root BLOB, capture_floor INTEGER, reader_owner INTEGER, UNIQUE(namespace,request), CHECK(namespace>0 OR purpose=0), CHECK((purpose=2 AND capture_generation IS NOT NULL AND capture_generation>0 AND capture_revision IS NOT NULL AND capture_revision>=0 AND capture_root IS NOT NULL AND length(capture_root)=32 AND capture_floor IS NOT NULL AND capture_floor>=0 AND reader_owner IS NOT NULL AND reader_owner>0) OR (purpose<>2 AND capture_generation IS NULL AND capture_revision IS NULL AND capture_root IS NULL AND capture_floor IS NULL AND reader_owner IS NULL))) STRICT",
        "CREATE TABLE durable_orphan_owner(serial INTEGER PRIMARY KEY CHECK(serial>0), root BLOB NOT NULL CHECK(length(root)=32), top INTEGER NOT NULL CHECK(top>=0), floor INTEGER NOT NULL CHECK(floor>=0 AND top>=floor)) STRICT",
        "INSERT INTO durable_owner_counts(id,operation_count,orphan_count) VALUES(1,0,0)",
    ] {
        db.execute(StatementKind::Startup, sql, &[], 0)?;
    }
    Ok(())
}

fn recorded(db: &Overlay) -> bool {
    db.durable.is_some() && db.profile.has_recorded_owners()
}

/// The caller has already journaled this owner's in-memory request mapping.
pub(crate) fn acquire_operation(
    db: &Overlay,
    owner: u64,
    request: i64,
    purpose: OperationPurpose,
    reader: Option<CapturedReader>,
) -> OverlayResult<()> {
    if !recorded(db) {
        return Ok(());
    }
    let namespace = db.durable_namespace()?.unwrap_or(0);
    if namespace == 0 && purpose != OperationPurpose::Unknown {
        return Err(OverlayError::Invalid(
            "durable operation namespace is absent",
        ));
    }
    let owner = integer(owner)?;
    let occupied = db.query(
        StatementKind::Frontier,
        "SELECT EXISTS(SELECT 1 FROM commit_record WHERE operation=?1) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=6 AND resource=?1)",
        &[&owner],
        8,
        |row| row.get::<_, bool>(0),
    )?;
    if occupied.as_slice() != [false] {
        return Err(OverlayError::Invalid(
            "durable operation owner remains in cleanup",
        ));
    }
    let generation = reader.map(|value| value.capture.generation.0);
    let revision = reader.map(|value| value.capture.revision);
    let root = reader.map(|value| value.capture.base_root);
    let root_bytes = root.as_ref().map(|value| value.as_slice());
    let floor = reader.map(|value| value.installed);
    let reader_owner = reader.map(|value| integer(value.owner)).transpose()?;
    db.execute(
        StatementKind::Frontier,
        "INSERT INTO durable_operation_owner(owner,namespace,request,purpose,capture_generation,capture_revision,capture_root,capture_floor,reader_owner) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        &[&owner, &namespace, &request, &purpose.record(), &generation, &revision, &root_bytes, &floor, &reader_owner],
        96,
    )?;
    let changed = db.execute(
        StatementKind::Frontier,
        "UPDATE durable_owner_counts SET operation_count=operation_count+1 WHERE id=1",
        &[],
        8,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("durable operation count is absent"));
    }
    Ok(())
}

/// Transfer record cleanup only after the exact original owner releases.
pub(crate) fn release_operation(db: &Overlay, owner: u64, request: i64) -> OverlayResult<()> {
    if !recorded(db) {
        return Ok(());
    }
    let changed = db.execute(
        StatementKind::Frontier,
        "DELETE FROM durable_operation_owner WHERE owner=?1 AND request=?2",
        &[&integer(owner)?, &request],
        16,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("durable operation owner changed"));
    }
    let changed = db.execute(
        StatementKind::Frontier,
        "UPDATE durable_owner_counts SET operation_count=operation_count-1 WHERE id=1 AND operation_count>0",
        &[],
        8,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("durable operation count changed"));
    }
    Ok(())
}

pub(crate) fn insert_orphan(
    db: &Overlay,
    serial: i64,
    orphan: crate::lifetime::orphan::Orphan,
) -> OverlayResult<()> {
    if !recorded(db) {
        return Ok(());
    }
    db.execute(
        StatementKind::Frontier,
        "INSERT INTO durable_orphan_owner(serial,root,top,floor) VALUES(?1,?2,?3,?4)",
        &[&serial, &orphan.root.as_slice(), &orphan.top, &orphan.floor],
        56,
    )?;
    let changed = db.execute(
        StatementKind::Frontier,
        "UPDATE durable_owner_counts SET orphan_count=orphan_count+1 WHERE id=1",
        &[],
        8,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("durable orphan count is absent"));
    }
    Ok(())
}

pub(crate) fn advance_orphan(
    db: &Overlay,
    serial: i64,
    before: i64,
    after: i64,
) -> OverlayResult<()> {
    if !recorded(db) {
        return Ok(());
    }
    let changed = db.execute(
        StatementKind::Frontier,
        "UPDATE durable_orphan_owner SET top=?1 WHERE serial=?2 AND top=?3",
        &[&after, &serial, &before],
        24,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("durable orphan owner changed"));
    }
    Ok(())
}

pub(crate) fn remove_orphan(db: &Overlay, serial: i64) -> OverlayResult<()> {
    if !recorded(db) {
        return Ok(());
    }
    let changed = db.execute(
        StatementKind::Frontier,
        "DELETE FROM durable_orphan_owner WHERE serial=?1",
        &[&serial],
        8,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("durable orphan owner changed"));
    }
    let changed = db.execute(
        StatementKind::Frontier,
        "UPDATE durable_owner_counts SET orphan_count=orphan_count-1 WHERE id=1 AND orphan_count>0",
        &[],
        8,
    )?;
    if changed != 1 {
        return Err(OverlayError::Invalid("durable orphan count changed"));
    }
    Ok(())
}
