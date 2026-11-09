//! Persist one Workspace's monotonically increasing control namespace.
use crate::{Overlay, OverlayError, OverlayResult, StatementKind};

impl Overlay {
    /// The last namespace consumed for this Workspace, if initialized.
    pub fn durable_namespace(&self) -> OverlayResult<Option<i64>> {
        if self.durable.is_none() || self.profile.schema_version != 41 {
            return Err(OverlayError::Invalid("durable namespace profile"));
        }
        let rows = self.query(
            StatementKind::Frontier,
            "SELECT high_namespace FROM durable_route WHERE id=1",
            &[],
            0,
            |row| row.get::<_, i64>(0),
        )?;
        Ok(rows.into_iter().next())
    }

    /// Record a new owner's namespace before exposing its first control token.
    pub fn initialize_durable_namespace(&self, namespace: i64) -> OverlayResult<()> {
        if namespace <= 0 {
            return Err(OverlayError::Invalid("durable namespace"));
        }
        self.atomic(|| {
            if self.durable_namespace()?.is_some() {
                return Err(OverlayError::Invalid(
                    "durable namespace already initialized",
                ));
            }
            let state = self.state()?;
            if state.closed
                || state.revision != 0
                || state.dirty_inodes != 0
                || state.dirty_directory_entries != 0
                || state.captured.is_some()
                || state.consolidating.is_some()
            {
                return Err(OverlayError::Invalid("durable namespace initial state"));
            }
            self.execute(
                StatementKind::Frontier,
                "INSERT INTO durable_route(id,high_namespace) VALUES(1,?1)",
                &[&namespace],
                8,
            )?;
            Ok(())
        })
    }

    /// Consume a greater namespace before exposing a recovered control token.
    /// A missing or stale record is refused; it is never inferred from a file
    /// name or reset after restart. The caller separately prevents collisions
    /// among concurrently mounted Workspaces and fences the old owner.
    pub fn advance_durable_namespace(&self, namespace: i64) -> OverlayResult<()> {
        if namespace <= 0 {
            return Err(OverlayError::Invalid("durable namespace"));
        }
        self.atomic(|| {
            let previous = self
                .durable_namespace()?
                .ok_or(OverlayError::Invalid("durable namespace is absent"))?;
            if namespace <= previous {
                return Err(OverlayError::Invalid("stale durable namespace"));
            }
            if self.state()?.closed {
                return Err(OverlayError::Closed);
            }
            let changed = self.execute(
                StatementKind::Frontier,
                "UPDATE durable_route SET high_namespace=?1 WHERE id=1 AND high_namespace=?2",
                &[&namespace, &previous],
                16,
            )?;
            if changed != 1 {
                return Err(OverlayError::Invalid("durable namespace predecessor"));
            }
            Ok(())
        })
    }
}
