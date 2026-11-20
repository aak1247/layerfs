//! A non-routable, lease-owning observation of one settled active conflict.
use crate::{
    conflict_page_digest, Overlay, OverlayError, OverlayResult, ProfileConfig, StatementKind,
    CONFLICT_PAGE_BYTES,
};
use std::{fmt, path::Path};

/// Local facts only. The caller must authenticate the real Store, historical
/// target and contents and must fence the original writers and consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettledConflict {
    pub workspace: [u8; 32],
    pub catalog: [u8; 32],
    pub incarnation: u64,
    pub original_namespace: i64,
    pub previous_namespace: i64,
    pub generation: i64,
    pub attempt: [u8; 32],
    pub marker_root: [u8; 32],
    pub target_snapshot: Vec<u8>,
    pub paths_count: u64,
    pub byte_count: u64,
    pub page_count: u64,
    pub final_digest: [u8; 32],
    pub descriptor: [u8; 32],
}

/// Holds the original database connection and exclusive lease. It cannot
/// advance a namespace, create an Owner, mount or dispatch a Workspace.
pub struct SettledConflictRecovery {
    db: Overlay,
    observed: SettledConflict,
}

/// A locally ready recorded binding. Store and writer fencing remain external.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadyRecord {
    pub workspace: [u8; 32],
    pub previous_namespace: i64,
    pub root: [u8; 32],
    pub snapshot: Vec<u8>,
}

/// One locally ready recorded database under its original lease.
pub struct ReadyRecovery {
    db: Overlay,
    observed: ReadyRecord,
}
impl fmt::Debug for ReadyRecovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadyRecovery")
            .field("observed", &self.observed)
            .finish_non_exhaustive()
    }
}

/// One open under the original lease. Neither variant is routable or mounted.
pub enum OverlayCatalogRecovery {
    Ready(ReadyRecovery),
    SettledConflict(SettledConflictRecovery),
    PendingCheckpoint(super::checkpoint_recovery::PendingCheckpointRecovery),
    PendingOrphans(super::durable_orphan_recovery::PendingOrphanRecovery),
}
impl fmt::Debug for OverlayCatalogRecovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready(value) => value.fmt(f),
            Self::SettledConflict(value) => value.fmt(f),
            Self::PendingCheckpoint(value) => value.fmt(f),
            Self::PendingOrphans(value) => value.fmt(f),
        }
    }
}

impl OverlayCatalogRecovery {
    /// Select locally ready or settled conflict in one leased open. This does
    /// not authenticate a Store or prove the previous consumer has stopped.
    pub fn open(path: &Path, config: ProfileConfig, workspace: [u8; 32]) -> OverlayResult<Self> {
        let db = super::durable::recover_for_catalog(path, config, workspace)?;
        if super::durable_attempt::pending(&db)? {
            return super::checkpoint_recovery::PendingCheckpointRecovery::open(db)
                .map(Self::PendingCheckpoint);
        }
        if super::durable_orphan_recovery::pending(&db)? {
            return super::durable_orphan_recovery::PendingOrphanRecovery::open(db, workspace)
                .map(Self::PendingOrphans);
        }
        if super::durable_conflict_stage::active_conflict(&db)?.is_some() {
            let observed = observe(&db)?;
            Ok(Self::SettledConflict(SettledConflictRecovery {
                db,
                observed,
            }))
        } else {
            if !db.profile().has_durable_namespace() {
                return Err(OverlayError::Invalid(
                    "durable namespace freshness is not recorded",
                ));
            }
            let previous_namespace = db
                .durable_namespace()?
                .ok_or(OverlayError::Invalid("durable namespace is absent"))?;
            let snapshot = db
                .durable_binding()?
                .ok_or(OverlayError::Invalid("durable binding is absent"))?;
            let root = db.state()?.base_root;
            Ok(Self::Ready(ReadyRecovery {
                db,
                observed: ReadyRecord {
                    workspace,
                    previous_namespace,
                    root,
                    snapshot,
                },
            }))
        }
    }
}

impl ReadyRecovery {
    pub fn observed(&self) -> &ReadyRecord {
        &self.observed
    }

    /// The locally opened schema, before any namespace or route is advanced.
    pub fn schema_version(&self) -> i64 {
        self.db.profile().schema_version
    }

    /// Check the original database and owner-file identities while retaining
    /// this connection and lease. This does not authenticate a Store or fence.
    pub fn verify_original(&self) -> OverlayResult<()> {
        let lease = self
            .db
            .durable
            .as_ref()
            .ok_or(OverlayError::Invalid("durable lease is absent"))?;
        let result = lease
            .verify()
            .and_then(|()| self.db.allocation.state().map(|_| ()))
            .and_then(|()| lease.verify());
        if result.is_err() {
            lease.quarantine();
        }
        result
    }

    /// Check the exact supported local format without changing its namespace
    /// or database. This does not authenticate the Store or consumer fence.
    #[cfg(target_os = "linux")]
    pub fn preflight_upgrade(&self, expected: &ReadyRecord) -> OverlayResult<i64> {
        if &self.observed != expected {
            return Err(OverlayError::Invalid("legacy migration predecessor"));
        }
        super::legacy_upgrade::preflight(&self.db, expected)
    }

    /// Upgrade an exact ready schema-41/42 predecessor while retaining this
    /// original lease and namespace. Store, roster and old-consumer authority
    /// must already have been authenticated by the trusted caller.
    #[cfg(target_os = "linux")]
    #[allow(clippy::result_large_err)]
    pub fn upgrade_legacy(mut self, expected: &ReadyRecord) -> Result<Self, (OverlayError, Self)> {
        if &self.observed != expected {
            return Err((OverlayError::Invalid("legacy migration predecessor"), self));
        }
        match super::legacy_upgrade::upgrade(&mut self.db, expected) {
            Ok(()) => Ok(self),
            Err(error) => Err((error, self)),
        }
    }
    /// Locally rechecks the original ready state, without advancing namespace.
    #[allow(clippy::result_large_err)]
    pub fn complete(self, expected: &ReadyRecord) -> Result<Overlay, (OverlayError, Self)> {
        let checked = (|| {
            if &self.observed != expected
                || self.db.durable_namespace()? != Some(expected.previous_namespace)
                || self.db.durable_binding()?.as_deref() != Some(expected.snapshot.as_slice())
                || self.db.state()?.base_root != expected.root
            {
                return Err(OverlayError::Invalid("ready recovery predecessor"));
            }
            self.db.require_recorded_recovery_ready()
        })();
        match checked {
            Ok(()) => Ok(self.db),
            Err(error) => Err((error, self)),
        }
    }
}
impl fmt::Debug for SettledConflictRecovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SettledConflictRecovery")
            .field("observed", &self.observed)
            .finish_non_exhaustive()
    }
}

fn fixed(bytes: Vec<u8>) -> OverlayResult<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| OverlayError::Invalid("settled conflict identity"))
}
fn observe(db: &Overlay) -> OverlayResult<SettledConflict> {
    let active = super::durable_conflict_stage::active_conflict(db)?
        .ok_or(OverlayError::Invalid("settled conflict binding is absent"))?;
    Ok(SettledConflict {
        workspace: fixed(active.workspace)?,
        catalog: fixed(active.catalog)?,
        incarnation: u64::try_from(active.incarnation)
            .map_err(|_| OverlayError::Invalid("settled conflict incarnation"))?,
        original_namespace: active.namespace,
        previous_namespace: db
            .durable_namespace()?
            .ok_or(OverlayError::Invalid("settled conflict namespace"))?,
        generation: active.generation,
        attempt: fixed(active.attempt)?,
        marker_root: fixed(active.root)?,
        target_snapshot: active.target,
        paths_count: u64::try_from(active.paths)
            .map_err(|_| OverlayError::Invalid("settled conflict paths"))?,
        byte_count: u64::try_from(active.bytes)
            .map_err(|_| OverlayError::Invalid("settled conflict bytes"))?,
        page_count: u64::try_from(active.pages)
            .map_err(|_| OverlayError::Invalid("settled conflict pages"))?,
        final_digest: fixed(active.final_digest)?,
        descriptor: active.digest,
    })
}

impl Overlay {
    /// Local active-conflict facts only; this does not authenticate a Store.
    pub fn settled_conflict(&self) -> OverlayResult<Option<SettledConflict>> {
        if super::durable_conflict_stage::active_conflict(self)?.is_none() {
            Ok(None)
        } else {
            observe(self).map(Some)
        }
    }
}

impl SettledConflictRecovery {
    pub fn observed(&self) -> &SettledConflict {
        &self.observed
    }

    /// Reads exactly one original bounded page; no row is modified.
    pub fn page(&self, number: u64) -> OverlayResult<Vec<u8>> {
        if number >= self.observed.page_count {
            return Err(OverlayError::Invalid("settled conflict page"));
        }
        let offset = number
            .checked_mul(CONFLICT_PAGE_BYTES as u64)
            .ok_or(OverlayError::Invalid("settled conflict page"))?;
        let size = self
            .observed
            .byte_count
            .checked_sub(offset)
            .ok_or(OverlayError::Invalid("settled conflict page"))?
            .min(CONFLICT_PAGE_BYTES as u64);
        let number =
            i64::try_from(number).map_err(|_| OverlayError::Invalid("settled conflict page"))?;
        let rows: Vec<Vec<u8>> = self.db.query(
            StatementKind::Frontier,
            "SELECT data FROM durable_conflict_page WHERE generation=?1 AND page_no=?2",
            &[&self.observed.generation, &number],
            CONFLICT_PAGE_BYTES as u64 + 16,
            |r| r.get(0),
        )?;
        let [data] = rows.as_slice() else {
            return Err(OverlayError::Invalid("settled conflict page"));
        };
        if size == 0 || u64::try_from(data.len()).ok() != Some(size) {
            return Err(OverlayError::Invalid("settled conflict page"));
        }
        Ok(data.clone())
    }

    fn verify(&self, expected: &SettledConflict, old_namespace: i64) -> OverlayResult<()> {
        if &self.observed != expected
            || observe(&self.db)? != *expected
            || old_namespace != expected.previous_namespace
        {
            return Err(OverlayError::Invalid("settled conflict predecessor"));
        }
        let mut digest = [0; 32];
        let mut bytes = 0_u64;
        for page in 0..expected.page_count {
            let data = self.page(page)?;
            digest = conflict_page_digest(digest, page, &data)?;
            bytes = bytes
                .checked_add(data.len() as u64)
                .ok_or(OverlayError::Invalid("settled conflict size"))?;
        }
        let pages = i64::try_from(expected.page_count)
            .map_err(|_| OverlayError::Invalid("settled conflict pages"))?;
        let extra:Vec<bool>=self.db.query(StatementKind::Frontier,
            "SELECT EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation<?1) OR EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation>?1) OR EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation=?1 AND page_no>=?2) OR EXISTS(SELECT 1 FROM durable_conflict_plan WHERE state<>1) OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=13) OR EXISTS(SELECT 1 FROM durable_checkpoint_attempt)",
            &[&expected.generation,&pages],16,|r|r.get(0))?;
        if digest != expected.final_digest
            || bytes != expected.byte_count
            || extra.as_slice() != [false]
        {
            return Err(OverlayError::Invalid("settled conflict contents"));
        }
        Ok(())
    }

    /// Completes local structural checks only, retaining the same lease and
    /// old namespace. The caller still authenticates the Store and fences all
    /// previous writers before constructing any routable Owner.
    #[allow(clippy::result_large_err)]
    pub fn complete_settled_active(
        self,
        expected: &SettledConflict,
        old_namespace: i64,
    ) -> Result<Overlay, (OverlayError, Self)> {
        match self.verify(expected, old_namespace) {
            Ok(()) => Ok(self.db),
            Err(error) => Err((error, self)),
        }
    }
}
