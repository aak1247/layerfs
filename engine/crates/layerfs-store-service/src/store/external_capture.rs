//! Observe one original externally identified checkpoint without publishing.
use super::{BoundWorkspace, CommitError, OriginalCheckpoint};
use crate::ServiceClass;
use layerfs_bridge::control::{
    CaptureIdentity, CaptureObservation, CaptureOutcome, CaptureProof, CapturePurpose,
};
use layerfs_content::ObjectId;
use layerfs_history::{CheckpointAttemptId, CommitOutcome, HistoryError};
use layerfs_overlay::{ExternalCapture, ExternalCaptureDisposition};

impl BoundWorkspace {
    /// Observes only this Workspace's original capture and exact Store receipt.
    /// The caller must separately authenticate the current token and authority.
    /// Missing records or receipts do not establish nonpublication.
    pub fn observe_external_capture(
        &self,
        identity: ExternalCapture,
    ) -> Result<CaptureObservation, CommitError> {
        let record = self
            .job(|client, route| {
                client.call(route, ServiceClass::General, None, move |db| {
                    db.observe_external_capture(identity)
                })
            })
            .map_err(CommitError::Owner)?;
        let Some(record) = record else {
            return Ok(CaptureObservation::Unknown);
        };
        let history = self.store.history();
        if !self.store.has_durable_writer()
            || record.workspace != self.identity.to_bytes()
            || record.catalog != history.catalog_id().to_bytes()
            || record.incarnation != history.incarnation()
            || self.owner.durable_identity().map_err(CommitError::Owner)? != Some(record.workspace)
        {
            return Err(CommitError::Context("external capture Store identity"));
        }
        history
            .require_checkpoint_receipts()
            .map_err(CommitError::History)?;
        let target = layerfs_bridge::snapshot::decode(&record.original_snapshot)
            .map_err(|_| CommitError::Context("external capture original snapshot"))?;
        let current = self.snapshot().map_err(CommitError::Workspace)?;
        if target.branch.id != current.branch.id
            || target.scope != current.scope
            || target.profile != current.profile
            || target.effective_root.to_bytes() != record.base_root
        {
            return Err(CommitError::Context("external capture Workspace binding"));
        }
        super::durable_bind::validate_snapshot(history, &target).map_err(CommitError::History)?;
        let attempt = CheckpointAttemptId::from_bytes(record.attempt);
        let Some(publication) = record.publication.as_ref() else {
            let receipt = history
                .checkpoint_receipt(attempt)
                .map_err(CommitError::History)?;
            if receipt.is_some() {
                return Err(CommitError::History(HistoryError::Integrity(
                    "external capture receipt without intent",
                )));
            }
            return Ok(match record.disposition {
                ExternalCaptureDisposition::Unresolved => CaptureObservation::Unknown,
                ExternalCaptureDisposition::Unpublished => CaptureObservation::Unpublished,
            });
        };
        if publication.target_snapshot != record.original_snapshot {
            return Err(CommitError::Context("external capture target changed"));
        }
        let root = ObjectId::from_bytes(&publication.candidate_root)
            .map_err(|_| CommitError::Context("external capture root"))?;
        let observed = self
            .store
            .observe_original_checkpoint(OriginalCheckpoint {
                catalog: history.catalog_id(),
                incarnation: record.incarnation,
                attempt,
                request: &publication.request,
                request_digest: publication.request_digest,
                candidate_root: root,
                target: &target,
            })
            .map_err(CommitError::History)?;
        match (record.disposition, observed) {
            (ExternalCaptureDisposition::Unpublished, None) => Ok(CaptureObservation::Unpublished),
            (ExternalCaptureDisposition::Unpublished, Some(_)) => Err(CommitError::History(
                HistoryError::Integrity("external capture unpublished receipt"),
            )),
            (ExternalCaptureDisposition::Unresolved, None) => Ok(CaptureObservation::Unknown),
            (ExternalCaptureDisposition::Unresolved, Some(observed)) => {
                let outcome = match observed.outcome {
                    CommitOutcome::Committed { record, displaced } => CaptureOutcome::Committed {
                        head: record.id,
                        time_ms: record.meta.time_ms,
                        displaced,
                    },
                    CommitOutcome::UpToDate { head, .. } => CaptureOutcome::UpToDate { head },
                };
                let positive = |n: u64| {
                    i64::try_from(n).map_err(|_| CommitError::Context("external capture number"))
                };
                Ok(CaptureObservation::Published(Box::new(CaptureProof {
                    identity: CaptureIdentity {
                        id: identity.id,
                        scope_digest: identity.scope_digest,
                        purpose: CapturePurpose::ManagedWorkspace,
                    },
                    workspace: self.identity,
                    original_namespace: positive(record.namespace)?,
                    generation: positive(record.generation)?,
                    revision: positive(record.revision)?,
                    base_root: record.base_root,
                    catalog: record.catalog,
                    incarnation: record.incarnation,
                    attempt: record.attempt,
                    request_digest: publication.request_digest,
                    root,
                    target_snapshot: publication.target_snapshot.clone(),
                    outcome,
                })))
            }
        }
    }
}
