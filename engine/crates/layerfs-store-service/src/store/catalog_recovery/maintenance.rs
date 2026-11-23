//! Whole-Store maintenance of one already authenticated Workspace.
use super::*;
use layerfs_overlay::OverlayError;

fn refused(cause: OverlayError, pending: OverlayCatalogRecovery) -> Box<CatalogMigrationFailure> {
    Box::new(CatalogMigrationFailure::Upgrade {
        cause: BindError::Owner(OwnerError::Overlay(cause)),
        custody: RetainedCatalogRecovery {
            _pending: Box::new(pending),
        },
    })
}

fn current(version: i64) -> layerfs_overlay::OverlayResult<()> {
    if version == 46 {
        Ok(())
    } else {
        Err(OverlayError::Invalid("maintenance migration schema"))
    }
}

impl Store {
    /// Migrate and checked-close one already authenticated Workspace for a
    /// full-Store maintenance batch. The caller must retain its original
    /// Store-wide writer and consumer fence and the original roster throughout.
    /// This does not advance a namespace, register a Workspace or publish.
    pub fn migrate_catalog_for_maintenance(
        self: &Arc<Self>,
        original: PreparedLegacyRecovery,
    ) -> Result<MaintainedCatalogRecovery, Box<CatalogMigrationFailure>> {
        let source_version = original.version;
        let prepared = self.prepare_migrated_catalog_recovery(original)?;
        let PreparedCatalogRecovery {
            pending,
            record,
            request,
            ..
        } = prepared;
        let (db, previous_namespace, disposition) = match (pending, record) {
            (OverlayCatalogRecovery::Ready(ready), LocalRecord::Ready(observed)) => {
                let version = match ready.preflight_upgrade(&observed) {
                    Ok(version) => version,
                    Err(cause) => return Err(refused(cause, OverlayCatalogRecovery::Ready(ready))),
                };
                let ready =
                    match version {
                        45 => ready.upgrade_external_capture(&observed).map_err(
                            |(cause, ready)| refused(cause, OverlayCatalogRecovery::Ready(ready)),
                        )?,
                        46 => ready,
                        _ => {
                            return Err(refused(
                                OverlayError::Invalid("maintenance migration schema"),
                                OverlayCatalogRecovery::Ready(ready),
                            ))
                        }
                    };
                if let Err(cause) = ready.preflight_upgrade(&observed).and_then(current) {
                    return Err(refused(cause, OverlayCatalogRecovery::Ready(ready)));
                }
                let db = ready.complete(&observed).map_err(|(cause, ready)| {
                    refused(cause, OverlayCatalogRecovery::Ready(ready))
                })?;
                (
                    db,
                    observed.previous_namespace,
                    MaintainedDisposition::Ready,
                )
            }
            (OverlayCatalogRecovery::SettledConflict(value), LocalRecord::Conflict(observed)) => {
                let version = match value.preflight_upgrade(&observed) {
                    Ok(version) => version,
                    Err(cause) => {
                        return Err(refused(
                            cause,
                            OverlayCatalogRecovery::SettledConflict(value),
                        ))
                    }
                };
                let value = match version {
                    45 => value
                        .upgrade_external_capture(&observed)
                        .map_err(|(cause, value)| {
                            refused(cause, OverlayCatalogRecovery::SettledConflict(value))
                        })?,
                    46 => value,
                    _ => {
                        return Err(refused(
                            OverlayError::Invalid("maintenance migration schema"),
                            OverlayCatalogRecovery::SettledConflict(value),
                        ))
                    }
                };
                if let Err(cause) = value.preflight_upgrade(&observed).and_then(current) {
                    return Err(refused(
                        cause,
                        OverlayCatalogRecovery::SettledConflict(value),
                    ));
                }
                let namespace = observed.previous_namespace;
                let db = value
                    .complete_settled_active(&observed, namespace)
                    .map_err(|(cause, value)| {
                        refused(cause, OverlayCatalogRecovery::SettledConflict(value))
                    })?;
                (db, namespace, MaintainedDisposition::SettledConflict)
            }
            _ => unreachable!("legacy migration prepared a settled Workspace"),
        };
        let closed = db
            .close_database_file()
            .map_err(|cause| Box::new(CatalogMigrationFailure::Close { cause }))?;
        Ok(MaintainedCatalogRecovery {
            request,
            previous_namespace,
            source_version,
            disposition,
            _closed: closed,
        })
    }
}
