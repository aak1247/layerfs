//! Durable opaque Branch metadata, paired with an Overlay base root.
use crate::{Overlay, OverlayError, OverlayResult, StatementKind};

impl Overlay {
    /// Stores the first trusted Branch record for a newly created Workspace.
    /// The caller authenticates and encodes it; this engine validates its
    /// base identity and does not accept a record after local changes.
    pub fn initialize_durable_binding(&self, root: [u8; 32], snapshot: &[u8]) -> OverlayResult<()> {
        if self.durable.is_none()
            || self.profile.schema_version != 40
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
        if self.durable.is_none() || self.profile.schema_version != 40 {
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

    pub(crate) fn invalidate_durable_binding(&self, root: [u8; 32]) -> OverlayResult<()> {
        if self.durable.is_some() && self.profile.schema_version == 40 {
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
            || self.profile.schema_version != 40
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
