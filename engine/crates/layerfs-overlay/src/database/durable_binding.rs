//! Durable opaque Branch metadata, paired with an Overlay base root.
use crate::{Overlay, OverlayError, OverlayResult, StatementKind};

/// Locally checked conflict facts for a Store-authenticated synchronization.
/// This value does not itself authenticate a Store or fence another owner.
pub struct ConflictSyncIdentity<'a> {
    pub workspace: [u8; 32],
    pub catalog: [u8; 32],
    pub incarnation: u64,
    pub namespace: i64,
    pub marker_root: [u8; 32],
    pub target_snapshot: &'a [u8],
    pub descriptor: [u8; 32],
}

impl Overlay {
    /// Stores the first trusted Branch record for a newly created Workspace.
    /// The caller authenticates and encodes it; this engine validates its
    /// base identity and does not accept a record after local changes.
    pub fn initialize_durable_binding(&self, root: [u8; 32], snapshot: &[u8]) -> OverlayResult<()> {
        if self.durable.is_none()
            || !self.profile.has_durable_binding()
            || snapshot.is_empty()
            || snapshot.len() > 4096
        {
            return Err(OverlayError::Invalid("durable binding profile or size"));
        }
        self.atomic(|| {
            let state = self.state()?;
            if state.base_root != root
                || state.revision != 0
                || state.dirty_inodes != 0
                || state.dirty_directory_entries != 0
                || state.captured.is_some()
                || state.closed
            {
                return Err(OverlayError::Invalid("durable binding initial state"));
            }
            self.execute(
                StatementKind::Frontier,
                "INSERT INTO durable_binding(id,root,snapshot,recoverable) VALUES(1,?1,?2,1)",
                &[&root.as_slice(), &snapshot],
                snapshot.len() as u64 + 32,
            )?;
            Ok(())
        })
    }

    /// Returns a root-matched record. A later unresolved install is a refusal,
    /// not permission to infer or automatically replay a Store publication.
    pub fn durable_binding(&self) -> OverlayResult<Option<Vec<u8>>> {
        if self.durable.is_none() || !self.profile.has_durable_binding() {
            return Err(OverlayError::Invalid("durable binding profile"));
        }
        let rows: Vec<(Vec<u8>, Vec<u8>, bool)> = self.query(
            StatementKind::Frontier,
            "SELECT root,snapshot,recoverable FROM durable_binding WHERE id=1",
            &[],
            0,
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let Some((root, snapshot, recoverable)) = rows.into_iter().next() else {
            return Ok(None);
        };
        if !recoverable || root != self.state()?.base_root {
            return Err(OverlayError::Invalid(
                "durable binding requires reconciliation",
            ));
        }
        Ok(Some(snapshot))
    }

    /// Check the same local reconciliation conditions required by recovery.
    /// The caller still validates the recorded base against its durable Store
    /// and must fence the previous mount and external operations after loss.
    pub fn require_recorded_recovery_ready(&self) -> OverlayResult<()> {
        self.require_bound_ready()?;
        if self.profile.schema_version == 44 {
            super::durable_conflict::verify_schema(self)?;
            let pending = self.query(
                StatementKind::Frontier,
                "SELECT EXISTS(SELECT 1 FROM durable_conflict_plan) \
                 OR EXISTS(SELECT 1 FROM durable_conflict_page) \
                 OR EXISTS(SELECT 1 FROM durable_binding WHERE conflict_generation IS NOT NULL) \
                 OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=13)",
                &[],
                8,
                |row| row.get::<_, bool>(0),
            )?;
            if pending.as_slice() != [false] {
                return Err(OverlayError::Invalid(
                    "durable conflict requires reconciliation",
                ));
            }
        }
        self.require_ready_cleanup()
    }

    /// Checks one settled active conflict for a local barrier. The caller
    /// authenticates the actual Store and Workspace and verifies its base;
    /// these local comparisons alone do not authorize recovery or a mount.
    pub fn require_conflict_synchronize_ready(
        &self,
        expected: &ConflictSyncIdentity<'_>,
    ) -> OverlayResult<()> {
        self.require_bound_ready()?;
        if self.profile.schema_version != 44 {
            return Err(OverlayError::Invalid(
                "durable conflict synchronization identity",
            ));
        }
        super::durable_conflict::verify_schema(self)?;
        let active = super::durable_conflict_stage::active_conflict(self)?.ok_or(
            OverlayError::Invalid("durable conflict synchronization identity"),
        )?;
        if active.workspace.as_slice() != expected.workspace
            || active.catalog.as_slice() != expected.catalog
            || u64::try_from(active.incarnation).ok() != Some(expected.incarnation)
            || self.durable_namespace()? != Some(expected.namespace)
            || active.root.as_slice() != expected.marker_root
            || active.target.as_slice() != expected.target_snapshot
            || active.digest != expected.descriptor
        {
            return Err(OverlayError::Invalid(
                "durable conflict synchronization identity",
            ));
        }
        let other = self.query(
            StatementKind::Frontier,
            "SELECT EXISTS(SELECT 1 FROM durable_conflict_plan WHERE generation<?1) \
             OR EXISTS(SELECT 1 FROM durable_conflict_plan WHERE generation>?1) \
             OR EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation<?1) \
             OR EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation>?1) \
             OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=13)",
            &[&active.generation],
            8,
            |row| row.get::<_, bool>(0),
        )?;
        if other.as_slice() != [false] {
            return Err(OverlayError::Invalid(
                "durable conflict requires reconciliation",
            ));
        }
        self.require_ready_cleanup()
    }

    fn require_bound_ready(&self) -> OverlayResult<()> {
        self.available()?;
        if self.transaction.get() != super::connection::Transaction::None {
            return Err(OverlayError::Invalid("recovery check inside atomic job"));
        }
        if self.durable_binding()?.is_none() {
            return Err(OverlayError::Invalid("durable binding is absent"));
        }
        let state = self.state()?;
        if state.closed
            || state.captured.is_some()
            || state.consolidating.is_some()
            || super::durable::volatile(self)
            || super::durable_attempt::pending(self)?
        {
            return Err(OverlayError::Invalid(
                "durable overlay requires reconciliation",
            ));
        }
        Ok(())
    }

    fn require_ready_cleanup(&self) -> OverlayResult<()> {
        let pending = self.query(
            StatementKind::Frontier,
            super::durable::BOUND_PENDING_SQL,
            &[],
            0,
            |row| row.get::<_, bool>(0),
        )?;
        if pending.as_slice() != [false] {
            return Err(OverlayError::Invalid(
                "durable overlay requires reconciliation",
            ));
        }
        Ok(())
    }

    pub(crate) fn invalidate_durable_binding(&self, root: [u8; 32]) -> OverlayResult<()> {
        if self.durable.is_some() && self.profile.has_durable_binding() {
            self.execute(
                StatementKind::Frontier,
                "UPDATE durable_binding SET root=?1,recoverable=0 WHERE id=1",
                &[&root.as_slice()],
                32,
            )?;
        }
        Ok(())
    }

    /// Part of the already-running install transaction, never a separate
    /// acknowledgement of the Store's publication.
    pub(crate) fn install_durable_binding(
        &self,
        previous_root: [u8; 32],
        root: [u8; 32],
        snapshot: &[u8],
    ) -> OverlayResult<()> {
        if self.durable.is_none()
            || !self.profile.has_durable_binding()
            || snapshot.is_empty()
            || snapshot.len() > 4096
        {
            return Err(OverlayError::Invalid("durable binding profile or size"));
        }
        let changed = self.execute(
            StatementKind::Frontier,
            "UPDATE durable_binding SET root=?1,snapshot=?2,recoverable=1 WHERE id=1 AND root=?3",
            &[&root.as_slice(), &snapshot, &previous_root.as_slice()],
            snapshot.len() as u64 + 64,
        )?;
        if changed != 1 {
            return Err(OverlayError::Invalid("durable binding install predecessor"));
        }
        Ok(())
    }
}
