//! The admitted Commit's original recorded capture and verified disposition.
use super::{
    bind::checked_base, commit::CommitRun, BoundWorkspace, CommitError, CommitFailure, CommitPhase,
    CommitSuccess, OriginalCheckpoint,
};
use crate::{OwnerError, ServiceClass};
use layerfs_content::ObjectId;
use layerfs_history::{
    checkpoint_request_digest, BranchSnapshot, CatalogId, CheckpointAttemptId, CommitOutcome,
    CommitRequest, HistoryError,
};
use layerfs_overlay::{
    Capture, CapturedReader, CheckpointCompletion, ConflictCompletion, ConflictStage,
    ExternalCapture, OperationOwner, RecordedCapture,
};

/// A background capture preserves its exact frontier and never merges a head
/// that moved concurrently. Ordinary Commit retains its original semantics.
#[derive(Clone, Copy)]
pub(super) enum CaptureIntent {
    Commit,
    External(ExternalCapture),
}
impl CaptureIntent {
    pub(super) fn ordinary(self) -> bool {
        matches!(self, Self::Commit)
    }

    pub(super) fn check_head(self, moved: bool) -> Result<(), CommitError> {
        if moved && !self.ordinary() {
            return Err(CommitError::Context("external capture Branch changed"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) enum CommitCapture {
    Legacy(Capture),
    Recorded(RecordedCapture),
}
impl CommitCapture {
    pub(super) fn capture(self) -> Capture {
        match self {
            Self::Legacy(capture) => capture,
            Self::Recorded(recorded) => recorded.capture(),
        }
    }
    pub(super) fn recorded(self) -> Option<RecordedCapture> {
        match self {
            Self::Legacy(_) => None,
            Self::Recorded(recorded) => Some(recorded),
        }
    }
}

/// Retained by the same admitted invocation until its original owners release.
pub(super) struct LiveCheckpoint {
    pub recorded: RecordedCapture,
    pub catalog: CatalogId,
    pub incarnation: u64,
    pub request: CommitRequest,
    pub request_bytes: Vec<u8>,
    pub digest: [u8; 32],
    pub original_binding: BranchSnapshot,
    pub target: BranchSnapshot,
    pub installed: BranchSnapshot,
    pub outcome: CommitOutcome,
}

/// The admitted no-publication conflict, held through its original releases.
pub(super) struct LiveConflict {
    pub stage: ConflictStage,
    pub catalog: CatalogId,
    pub incarnation: u64,
    pub original_binding: BranchSnapshot,
    pub target: BranchSnapshot,
    pub marker: ObjectId,
    pub paths: super::conflict_paths::ConflictDescription,
}

impl BoundWorkspace {
    /// Selects the actual durable format before capture. A schema 43 capture
    /// never falls back to an unrecorded Store if receipt support is absent.
    pub(super) fn capture_commit(
        &self,
        snapshot: &BranchSnapshot,
        intent: CaptureIntent,
    ) -> Result<CommitCapture, CommitError> {
        let identity = self.owner.durable_identity().map_err(CommitError::Owner)?;
        if let Some(identity) = identity {
            if identity != self.identity.to_bytes() {
                return Err(CommitError::Context("checkpoint Workspace identity"));
            }
            let (recorded, external, version) = self
                .job(|client, route| {
                    client.call(route, ServiceClass::General, None, |db| {
                        Ok((
                            db.profile().has_recorded_checkpoints(),
                            db.profile().has_external_captures(),
                            db.profile().schema_version,
                        ))
                    })
                })
                .map_err(CommitError::Owner)?;
            if matches!(intent, CaptureIntent::External(_)) && !external {
                return Err(CommitError::Context("external capture format unavailable"));
            }
            if recorded {
                if !self.store.has_durable_writer() {
                    return Err(CommitError::History(HistoryError::Unsupported(
                        "checkpoint receipt requires durable Store",
                    )));
                }
                let history = self.store.history();
                history
                    .require_checkpoint_receipts()
                    .map_err(CommitError::History)?;
                super::durable_bind::validate_snapshot(history, snapshot)
                    .map_err(CommitError::History)?;
                let bytes = layerfs_bridge::snapshot::encode(snapshot)
                    .map_err(|cause| CommitError::Owner(OwnerError::BindingCodec(cause)))?;
                let catalog = history.catalog_id().to_bytes();
                let incarnation = history.incarnation();
                return self
                    .job(|client, route| match intent {
                        CaptureIntent::Commit => {
                            client.capture_checkpoint(route, identity, catalog, incarnation, bytes)
                        }
                        CaptureIntent::External(capture) => client.capture_external_checkpoint(
                            route,
                            capture,
                            identity,
                            catalog,
                            incarnation,
                            bytes,
                        ),
                    })
                    .map(CommitCapture::Recorded)
                    .map_err(CommitError::Owner);
            }
            if !matches!(version, 39..=42) {
                return Err(CommitError::Context("unsupported durable overlay format"));
            }
        }
        if !intent.ordinary() {
            return Err(CommitError::Context(
                "external capture requires durable Workspace",
            ));
        }
        self.job(|client, route| client.capture(route))
            .map(CommitCapture::Legacy)
            .map_err(CommitError::Owner)
    }

    /// Rechecks the exact Store receipt, current Workspace projection and local
    /// owner facts before finishing this invocation's installed checkpoint.
    pub(super) fn finish_live_checkpoint(
        &self,
        checkpoint: &LiveCheckpoint,
        reader: Option<CapturedReader>,
        operation: Option<OperationOwner>,
    ) -> Result<(), CommitError> {
        if checkpoint_request_digest(&checkpoint.request_bytes) != checkpoint.digest {
            return Err(CommitError::Context("checkpoint request digest"));
        }
        let observed = self
            .store
            .observe_original_checkpoint(OriginalCheckpoint {
                catalog: checkpoint.catalog,
                incarnation: checkpoint.incarnation,
                attempt: CheckpointAttemptId::from_bytes(checkpoint.recorded.attempt()),
                request: &checkpoint.request_bytes,
                request_digest: checkpoint.digest,
                candidate_root: checkpoint.request.candidate_root,
                target: &checkpoint.target,
            })
            .map_err(CommitError::History)?
            .ok_or(CommitError::History(HistoryError::Integrity(
                "checkpoint receipt is absent",
            )))?;
        if observed.outcome != checkpoint.outcome
            || observed.installed != checkpoint.installed
            || self.snapshot().map_err(CommitError::Workspace)? != checkpoint.installed
            || self
                .pending_snapshot()
                .map_err(CommitError::Workspace)?
                .is_some()
        {
            return Err(CommitError::Context("checkpoint installed projection"));
        }
        let ports = self
            .store
            .ports_for(checkpoint.installed.scope, self.identity);
        let checked = checked_base(ports.client(), &checkpoint.installed).map_err(|error| {
            CommitError::Content {
                error,
                provider: ports
                    .failure()
                    .unwrap_or_else(|error| Some(std::sync::Arc::new(error))),
            }
        })?;
        let current = self.workspace.base().map_err(CommitError::Workspace)?;
        if current.identity() != checked.identity() || current.root() != checked.root() {
            return Err(CommitError::Context("checkpoint Workspace base"));
        }
        let target = layerfs_bridge::snapshot::encode(&checkpoint.target)
            .map_err(|cause| CommitError::Owner(OwnerError::BindingCodec(cause)))?;
        let installed = layerfs_bridge::snapshot::encode(&checkpoint.installed)
            .map_err(|cause| CommitError::Owner(OwnerError::BindingCodec(cause)))?;
        let request = checkpoint.request_bytes.clone();
        let recorded = checkpoint.recorded;
        let digest = checkpoint.digest;
        let candidate: ObjectId = checkpoint.request.candidate_root;
        let outcome = super::commit::checkpoint_outcome(&checkpoint.outcome);
        self.job(|client, route| {
            client.call(route, ServiceClass::General, None, move |db| {
                db.finish_installed_checkpoint_cleanup(CheckpointCompletion {
                    recorded,
                    request: &request,
                    request_digest: digest,
                    candidate_root: candidate.to_bytes(),
                    target_snapshot: &target,
                    installed_snapshot: &installed,
                    outcome,
                    reader,
                    operation,
                })
            })
        })
        .map_err(CommitError::Owner)
    }

    pub(super) fn finish_commit_run(
        &self,
        run: CommitRun,
        reader: Option<CapturedReader>,
        operation: Option<OperationOwner>,
    ) -> Result<CommitSuccess, Box<CommitFailure>> {
        if let Some(checkpoint) = run.checkpoint {
            if let Err(error) = self.finish_live_checkpoint(&checkpoint, reader, operation) {
                let mut failure = CommitFailure::new(error, self.identity, self.route());
                failure.binding = Some(checkpoint.original_binding);
                failure.phase = CommitPhase::Install;
                failure.intent = Some(checkpoint.request);
                failure.capture = Some(checkpoint.recorded.capture());
                failure.recorded = Some(checkpoint.recorded);
                failure.publication_prepared = true;
                failure.published = Some(checkpoint.outcome);
                failure.installed = Some(Box::new(run.success));
                return Err(Box::new(failure));
            }
        }
        if let Some(conflict) = run.conflict {
            if let Err(error) = self.finish_live_conflict(&conflict, reader, operation) {
                let recorded = conflict.stage.recorded();
                let mut failure = CommitFailure::new(error, self.identity, self.route());
                failure.binding = Some(conflict.original_binding);
                failure.phase = CommitPhase::Install;
                failure.capture = Some(recorded.capture());
                failure.recorded = Some(recorded);
                failure.installed = Some(Box::new(run.success));
                return Err(Box::new(failure));
            }
        }
        Ok(run.success)
    }

    /// Verify this local projection against the same authenticated Store and
    /// the exact staged path chain before settling its original owners.
    fn finish_live_conflict(
        &self,
        conflict: &LiveConflict,
        reader: Option<CapturedReader>,
        operation: Option<OperationOwner>,
    ) -> Result<(), CommitError> {
        let history = self.store.history();
        if history.catalog_id() != conflict.catalog || history.incarnation() != conflict.incarnation
        {
            return Err(CommitError::Context("conflict Store identity"));
        }
        super::durable_bind::validate_snapshot(history, &conflict.target)
            .map_err(CommitError::History)?;
        if self.snapshot().map_err(CommitError::Workspace)? != conflict.target {
            return Err(CommitError::Context("conflict history projection"));
        }
        let pending = self
            .pending_snapshot()
            .map_err(CommitError::Workspace)?
            .ok_or(CommitError::Context("conflict projection is absent"))?;
        if pending.descriptor != Some(conflict.stage.descriptor())
            || pending.local_root != conflict.marker
            || super::conflict_paths::pages(&pending.conflicted, |_, _| Ok(()))? != conflict.paths
        {
            return Err(CommitError::Context("conflict path projection"));
        }
        let ports = self.store.ports_for(conflict.target.scope, self.identity);
        let checked_history = checked_base(ports.client(), &conflict.target).map_err(|error| {
            CommitError::Content {
                error,
                provider: ports
                    .failure()
                    .unwrap_or_else(|error| Some(std::sync::Arc::new(error))),
            }
        })?;
        let mut marker = conflict.target.clone();
        marker.effective_root = conflict.marker;
        let checked_marker =
            checked_base(ports.client(), &marker).map_err(|error| CommitError::Content {
                error,
                provider: ports
                    .failure()
                    .unwrap_or_else(|error| Some(std::sync::Arc::new(error))),
            })?;
        let current = self.workspace.base().map_err(CommitError::Workspace)?;
        if checked_history.root().root_inode() != checked_marker.root().root_inode()
            || current.identity() != checked_marker.identity()
            || current.root() != checked_marker.root()
        {
            return Err(CommitError::Context("conflict Workspace base"));
        }
        let target = layerfs_bridge::snapshot::encode(&conflict.target)
            .map_err(|cause| CommitError::Owner(OwnerError::BindingCodec(cause)))?;
        let stage = conflict.stage;
        let root = conflict.marker.to_bytes();
        self.job(|client, route| {
            client.call(route, ServiceClass::General, None, move |db| {
                db.finish_conflict_checkpoint_cleanup(ConflictCompletion {
                    stage,
                    marker_root: root,
                    target_snapshot: &target,
                    reader,
                    operation,
                })
            })
        })
        .map_err(CommitError::Owner)
    }

    pub(super) fn finish_failed_checkpoint(&self, failure: &mut CommitFailure) {
        if !failure.locally_settled {
            return;
        }
        let Some(recorded) = failure.recorded else {
            return;
        };
        if let Err(error) = self.job(|client, route| {
            client.call(route, ServiceClass::General, None, move |db| {
                db.finish_folded_checkpoint_cleanup(recorded)
            })
        }) {
            failure.local_error = Some(error);
            failure.locally_settled = false;
        }
    }
}
