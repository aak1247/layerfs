//! Locally verified, same-lease transition of ready durable predecessors.
use crate::{
    ConflictSyncIdentity, Overlay, OverlayError, OverlayResult, ReadyRecord, SettledConflict,
    StatementKind,
};
use rusqlite::Connection;

const SCHEMA_41: &str = include_str!("../../sql/legacy_schema_41.sql");
const SCHEMA_43: &str = include_str!("../../sql/legacy_schema_43.sql");
const SCHEMA_44: &str = include_str!("../../sql/legacy_schema_44.sql");
const KEPT: &str = "CREATE TABLE durable_kept_live(serial INTEGER PRIMARY KEY CHECK(serial>0), generation INTEGER NOT NULL CHECK(generation>0)) STRICT";
const CUSTODY: &str = "CREATE TABLE durable_custody(id INTEGER PRIMARY KEY CHECK(id=1), orphan_owners INTEGER NOT NULL CHECK(orphan_owners IN(0,1)), operation_owners INTEGER NOT NULL CHECK(operation_owners IN(0,1)), kept_count INTEGER NOT NULL CHECK(kept_count>=0)) STRICT";
// Full four-field schema of a freshly created schema-45 database. The golden
// is checked against a separately created fresh database in the fixture test.
const SCHEMA_45_DIGEST: [u8; 32] = [
    0x55, 0x49, 0x26, 0x38, 0x37, 0x48, 0x1e, 0x14, 0xf2, 0x68, 0xf9, 0x54, 0x49, 0x84, 0xf3, 0x2d,
    0x11, 0x3f, 0x9e, 0xea, 0xaa, 0xc1, 0xd8, 0x05, 0x87, 0xc9, 0x31, 0xfc, 0xc2, 0xfd, 0xd5, 0x3e,
];
type SchemaObject = (String, String, String, Option<String>);
type StateRow = (
    i64,
    Option<i64>,
    Option<i64>,
    i64,
    i64,
    Vec<u8>,
    i64,
    i64,
    bool,
    Option<i64>,
    bool,
);

fn stored_state(db: &Overlay) -> OverlayResult<Vec<StateRow>> {
    db.query(StatementKind::Startup,
        "SELECT active,captured,captured_revision,installed,revision,base_root,dirty_inodes,dirty_entries,closed,consolidating,volatile_owners FROM durable_state WHERE id=1",
        &[],0,|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?,r.get(10)?)))
}

/// Compare complete original definitions, including implicit indexes and SQL
/// NULLs, against the independently constructed pinned predecessor schema.
fn schema_objects(
    db: &Overlay,
    count: usize,
    mismatch: &'static str,
) -> OverlayResult<Vec<SchemaObject>> {
    let bounded: Vec<(i64,bool)> = db.query(
        StatementKind::Startup,
        "SELECT count(*), EXISTS(SELECT 1 FROM sqlite_schema WHERE length(type)>16 OR length(name)>255 OR length(tbl_name)>255 OR length(sql)>32768) FROM sqlite_schema",
        &[], 0, |row| Ok((row.get(0)?,row.get(1)?)),
    )?;
    if bounded.as_slice() != [(count as i64, false)] {
        return Err(OverlayError::Invalid(mismatch));
    }
    db.query(
        StatementKind::Startup,
        "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name",
        &[],
        0,
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )
}

fn verify_predecessor(db: &Overlay, version: i64) -> OverlayResult<()> {
    let expected = Connection::open_in_memory()?;
    match version {
        41 => expected.execute_batch(SCHEMA_41)?,
        42 => {
            expected.execute_batch(SCHEMA_41)?;
            expected.execute_batch(&format!("{KEPT};{CUSTODY};"))?;
        }
        43 => expected.execute_batch(SCHEMA_43)?,
        44 => expected.execute_batch(SCHEMA_44)?,
        _ => return Err(OverlayError::Invalid("unsupported legacy migration schema")),
    }
    let mut statement =
        expected.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")?;
    let expected: Vec<SchemaObject> = statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<_, _>>()?;
    let actual = schema_objects(db, expected.len(), "legacy overlay schema identity")?;
    if actual != expected {
        return Err(OverlayError::Invalid("legacy overlay schema identity"));
    }
    Ok(())
}

fn verify_schema_45(objects: Vec<SchemaObject>) -> OverlayResult<()> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs:durable-schema:v1\0");
    for (kind, name, table, sql) in objects {
        for value in [&kind, &name, &table] {
            hash.update(&(value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        match sql {
            Some(value) => {
                hash.update(&[1]);
                hash.update(&(value.len() as u64).to_be_bytes());
                hash.update(value.as_bytes());
            }
            None => {
                hash.update(&[0]);
            }
        }
    }
    if hash.finalize().as_bytes() != &SCHEMA_45_DIGEST {
        return Err(OverlayError::Invalid("durable migration schema identity"));
    }
    Ok(())
}

fn verify_current(db: &Overlay) -> OverlayResult<()> {
    verify_schema_45(schema_objects(db, 49, "durable migration schema identity")?)
}

fn expected_capture_objects() -> OverlayResult<Vec<SchemaObject>> {
    let expected = Connection::open_in_memory()?;
    expected.execute_batch(&format!(
        "{};{};{};",
        super::durable_capture::SCHEMA,
        super::durable_capture::IMMUTABLE,
        super::durable_capture::NO_DELETE,
    ))?;
    let mut statement =
        expected.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")?;
    let objects = statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<_, _>>()?;
    Ok(objects)
}

/// Reject extra triggers and altered tables before capture can use the
/// checkpoint transaction. The complete old schema is part of the format.
pub(super) fn verify_capture_schema(db: &Overlay) -> OverlayResult<()> {
    let objects = schema_objects(db, 54, "external capture schema identity")?;
    let (capture, predecessor): (Vec<_>, Vec<_>) = objects
        .into_iter()
        .partition(|(_, _, table, _)| table == "durable_external_capture");
    if predecessor.len() != 49 {
        return Err(OverlayError::Invalid("external capture predecessor schema"));
    }
    verify_schema_45(predecessor)?;
    if capture != expected_capture_objects()? {
        return Err(OverlayError::Invalid("external capture schema identity"));
    }
    Ok(())
}

fn replace_binding(db: &Overlay) -> OverlayResult<()> {
    db.execute(
        StatementKind::Startup,
        "ALTER TABLE durable_binding RENAME TO legacy_durable_binding",
        &[],
        0,
    )?;
    db.execute(
        StatementKind::Startup,
        super::durable_conflict::BINDING_SCHEMA,
        &[],
        0,
    )?;
    db.execute(StatementKind::Startup,
        "INSERT INTO durable_binding(id,root,snapshot,recoverable,conflict_generation,conflict_digest) SELECT id,root,snapshot,recoverable,NULL,NULL FROM legacy_durable_binding", &[],0)?;
    let changed: Vec<bool> = db.query(StatementKind::Startup,
        "SELECT EXISTS(SELECT id,root,snapshot,recoverable FROM legacy_durable_binding EXCEPT SELECT id,root,snapshot,recoverable FROM durable_binding) OR EXISTS(SELECT id,root,snapshot,recoverable FROM durable_binding EXCEPT SELECT id,root,snapshot,recoverable FROM legacy_durable_binding)",
        &[],0,|row|row.get(0))?;
    if changed.as_slice() != [false] {
        return Err(OverlayError::Invalid("legacy durable binding changed"));
    }
    db.execute(
        StatementKind::Startup,
        "DROP TABLE legacy_durable_binding",
        &[],
        0,
    )?;
    Ok(())
}

/// Check the local predecessor before any related Store migration is started.
/// Store and old-consumer authority are established separately by the caller.
pub(super) fn preflight(db: &Overlay, record: &ReadyRecord) -> OverlayResult<i64> {
    let version = db.profile.schema_version;
    if !matches!(version, 41..=46) {
        return Err(OverlayError::Invalid("unsupported legacy migration schema"));
    }
    let original = db.state()?;
    if stored_state(db)?.len() != 1 {
        return Err(OverlayError::Invalid("legacy durable state absent"));
    }
    if db.durable_namespace()? != Some(record.previous_namespace)
        || db.durable_binding()?.as_deref() != Some(record.snapshot.as_slice())
        || original.base_root != record.root
    {
        return Err(OverlayError::Invalid("legacy migration predecessor"));
    }
    db.require_recorded_recovery_ready()?;
    match version {
        45 => verify_current(db)?,
        46 => verify_capture_schema(db)?,
        _ => verify_predecessor(db, version)?,
    }
    Ok(version)
}

/// No Store or caller authority is inferred here. The owner of the original
/// lease must authenticate the Store, roster and old consumer fence first.
pub(super) fn upgrade(db: &mut Overlay, record: &ReadyRecord) -> OverlayResult<()> {
    let version = preflight(db, record)?;
    if matches!(version, 45 | 46) {
        return Err(OverlayError::Invalid("unsupported legacy migration schema"));
    }
    let original = db.state()?;
    let stored = stored_state(db)?;
    db.atomic_cleanup(|| {
        db.begin()?;
        db.require_recorded_ready_in_cleanup()?;
        if db.state()? != original || stored_state(db)? != stored || db.durable_namespace()? != Some(record.previous_namespace)
            || db.durable_binding()?.as_deref() != Some(record.snapshot.as_slice())
        { return Err(OverlayError::Invalid("legacy migration predecessor")); }
        verify_predecessor(db, version)?;
        if version != 44 {
            replace_binding(db)?;
            if version == 41 {
                db.execute(StatementKind::Startup, KEPT, &[], 0)?;
                db.execute(StatementKind::Startup, CUSTODY, &[], 0)?;
                db.execute(StatementKind::Startup, "INSERT INTO durable_custody(id,orphan_owners,operation_owners,kept_count) VALUES(1,0,0,0)", &[], 0)?;
            }
            if version <= 42 {
                super::durable_owners::initialize(db)?;
            } else {
                db.execute(StatementKind::Startup, "DROP TABLE durable_checkpoint_attempt", &[], 0)?;
            }
            super::durable_attempt::initialize(db)?;
            super::durable_conflict::upgrade_legacy(db)?;
        }
        super::durable_serial_retirement::initialize(db)?;
        super::durable_conflict::verify_schema(db)?;
        super::durable_serial_retirement::verify_schema(db)?;
        verify_current(db)?;
        if db.state()? != original || stored_state(db)? != stored || db.durable_namespace()? != Some(record.previous_namespace)
            || db.durable_binding()?.as_deref() != Some(record.snapshot.as_slice())
        { return Err(OverlayError::Invalid("legacy migration result changed")); }
        db.execute(StatementKind::Startup, "PRAGMA user_version=45", &[], 0)?;
        let actual: Vec<i64> = db.query(StatementKind::Startup, "PRAGMA user_version", &[], 0, |row|row.get(0))?;
        if actual.as_slice() != [45] { return Err(OverlayError::Invalid("legacy migration version")); }
        Ok(())
    })?;
    db.profile.schema_version = 45;
    Ok(())
}

/// Local step of a separately fenced, whole-Store maintenance operation.
/// This does not establish the Store, roster or old-consumer authority.
pub(super) fn upgrade_capture_ready(db: &mut Overlay, record: &ReadyRecord) -> OverlayResult<()> {
    if preflight(db, record)? != 45 {
        return Err(OverlayError::Invalid(
            "external capture predecessor version",
        ));
    }
    let original = db.state()?;
    let stored = stored_state(db)?;
    db.atomic_cleanup(|| {
        db.begin()?;
        db.require_recorded_ready_in_cleanup()?;
        if db.state()? != original
            || stored_state(db)? != stored
            || db.durable_namespace()? != Some(record.previous_namespace)
            || db.durable_binding()?.as_deref() != Some(record.snapshot.as_slice())
        {
            return Err(OverlayError::Invalid(
                "external capture predecessor changed",
            ));
        }
        verify_current(db)?;
        super::durable_capture::initialize(db)?;
        verify_capture_schema(db)?;
        if db.state()? != original
            || stored_state(db)? != stored
            || db.durable_namespace()? != Some(record.previous_namespace)
            || db.durable_binding()?.as_deref() != Some(record.snapshot.as_slice())
        {
            return Err(OverlayError::Invalid(
                "external capture migration changed state",
            ));
        }
        db.execute(StatementKind::Startup, "PRAGMA user_version=46", &[], 0)?;
        let actual: Vec<i64> = db.query(
            StatementKind::Startup,
            "PRAGMA user_version",
            &[],
            0,
            |row| row.get(0),
        )?;
        if actual.as_slice() != [46] {
            return Err(OverlayError::Invalid("external capture migration version"));
        }
        Ok(())
    })?;
    db.profile.schema_version = 46;
    Ok(())
}

fn conflict_identity(record: &SettledConflict) -> ConflictSyncIdentity<'_> {
    ConflictSyncIdentity {
        workspace: record.workspace,
        catalog: record.catalog,
        incarnation: record.incarnation,
        namespace: record.previous_namespace,
        marker_root: record.marker_root,
        target_snapshot: &record.target_snapshot,
        descriptor: record.descriptor,
    }
}

/// Authenticate the local schema and exact settled disposition; the Store
/// and the old consumers remain the caller's separate responsibility.
pub(super) fn preflight_conflict(db: &Overlay, record: &SettledConflict) -> OverlayResult<i64> {
    let version = db.profile.schema_version;
    if !matches!(version, 44..=46)
        || super::pending_recovery::observe(db)? != *record
        || stored_state(db)?.len() != 1
    {
        return Err(OverlayError::Invalid("legacy conflict predecessor"));
    }
    db.require_conflict_synchronize_ready(&conflict_identity(record))?;
    match version {
        44 => verify_predecessor(db, version)?,
        45 => verify_current(db)?,
        46 => verify_capture_schema(db)?,
        _ => unreachable!("schema version checked"),
    }
    Ok(version)
}

pub(super) fn upgrade_conflict(db: &mut Overlay, record: &SettledConflict) -> OverlayResult<()> {
    if preflight_conflict(db, record)? != 44 {
        return Err(OverlayError::Invalid("unsupported legacy conflict schema"));
    }
    let original = db.state()?;
    let stored = stored_state(db)?;
    db.atomic_cleanup(|| {
        db.begin()?;
        db.require_conflict_ready_in_cleanup(&conflict_identity(record))?;
        if db.state()? != original
            || stored_state(db)? != stored
            || super::pending_recovery::observe(db)? != *record
        {
            return Err(OverlayError::Invalid("legacy conflict predecessor"));
        }
        verify_predecessor(db, 44)?;
        super::durable_serial_retirement::initialize(db)?;
        super::durable_serial_retirement::verify_schema(db)?;
        verify_current(db)?;
        if db.state()? != original
            || stored_state(db)? != stored
            || super::pending_recovery::observe(db)? != *record
        {
            return Err(OverlayError::Invalid("legacy conflict result changed"));
        }
        db.execute(StatementKind::Startup, "PRAGMA user_version=45", &[], 0)?;
        let actual: Vec<i64> = db.query(
            StatementKind::Startup,
            "PRAGMA user_version",
            &[],
            0,
            |row| row.get(0),
        )?;
        if actual.as_slice() != [45] {
            return Err(OverlayError::Invalid("legacy conflict migration version"));
        }
        Ok(())
    })?;
    db.profile.schema_version = 45;
    Ok(())
}

pub(super) fn upgrade_capture_conflict(
    db: &mut Overlay,
    record: &SettledConflict,
) -> OverlayResult<()> {
    if preflight_conflict(db, record)? != 45 {
        return Err(OverlayError::Invalid("external capture conflict version"));
    }
    let original = db.state()?;
    let stored = stored_state(db)?;
    db.atomic_cleanup(|| {
        db.begin()?;
        db.require_conflict_ready_in_cleanup(&conflict_identity(record))?;
        if db.state()? != original
            || stored_state(db)? != stored
            || super::pending_recovery::observe(db)? != *record
        {
            return Err(OverlayError::Invalid("external capture conflict changed"));
        }
        verify_current(db)?;
        super::durable_capture::initialize(db)?;
        verify_capture_schema(db)?;
        if db.state()? != original
            || stored_state(db)? != stored
            || super::pending_recovery::observe(db)? != *record
        {
            return Err(OverlayError::Invalid(
                "external capture conflict migration changed",
            ));
        }
        db.execute(StatementKind::Startup, "PRAGMA user_version=46", &[], 0)?;
        let actual: Vec<i64> = db.query(
            StatementKind::Startup,
            "PRAGMA user_version",
            &[],
            0,
            |row| row.get(0),
        )?;
        if actual.as_slice() != [46] {
            return Err(OverlayError::Invalid(
                "external capture conflict migration version",
            ));
        }
        Ok(())
    })?;
    db.profile.schema_version = 46;
    Ok(())
}
