//! One synchronous Content/Save/history producer and exact paired local install.
use super::checkpoint::{CommitCapture, LiveCheckpoint, LiveConflict};
use super::{
    bind::checked_base, BoundWorkspace, CommitError, CommitFailure, CommitOptions, CommitPhase,
    CommitSuccess, MergeReport,
};
use crate::telemetry::phase;
use layerfs_content::filesystem::FilesystemRootId;
use layerfs_history::{
    checkpoint_request_digest, encode_checkpoint_request, BranchSnapshot, CheckpointAttemptId,
    CommitMeta, CommitOutcome, CommitRequest, CommitVia,
};
use layerfs_overlay::Capture;
use layerfs_storage::Save;
use layerfs_telemetry::timer::{Active, TimingScope};
use layerfs_workspace::WorkspaceError;
use std::sync::{atomic::Ordering, Arc};

pub(super) struct CommitRun {
    pub success: CommitSuccess,
    pub checkpoint: Option<LiveCheckpoint>,
    pub conflict: Option<LiveConflict>,
}

/// Admission belongs to exactly one invocation. It stays held unless that
/// invocation establishes a safe disposition, including its captured releases.
/// Unwinding or an unknown result conservatively retains admission.
pub(super) struct CommitAdmission<'a> {
    workspace: &'a BoundWorkspace,
    pub(super) released: bool,
}
impl Drop for CommitAdmission<'_> {
    fn drop(&mut self) {
        if self.released {
            self.workspace.committing.store(false, Ordering::Release);
        }
    }
}

impl BoundWorkspace {
    /// Captures once and drives one caller-owned Content producer over that
    /// exact context. The producer must represent the capture faithfully; live
    /// namespace normalization is a separate constructor, not an implicit Init.
    /// No owner or Workspace mutex spans construction or provider I/O. On definite
    /// nonpublication one local resolution preserves the accepted view; unknown
    /// and known-publication/install failure retain original custody.
    pub fn commit(
        &self,
        construct: impl FnOnce(
            &Save<'_>,
            Capture,
            &BranchSnapshot,
        ) -> Result<FilesystemRootId, CommitError>,
    ) -> Result<CommitSuccess, Box<CommitFailure>> {
        let mut admission = self.admit_commit()?;
        let result = crate::telemetry::timed("commit", |timing| {
            self.commit_admitted(
                |save, capture, snapshot| construct(save, capture.capture(), snapshot),
                || (),
                &CommitOptions::default(),
                CommitVia::Workspace,
                timing,
            )
        });
        let result = match result {
            Ok(run) => self.finish_commit_run(run, None, None),
            Err(mut failure) => {
                self.finish_failed_checkpoint(&mut failure);
                Err(failure)
            }
        };
        admission.released = match &result {
            Ok(_) => true,
            Err(failure) => failure.locally_settled,
        };
        result
    }
    pub(super) fn admit_commit(&self) -> Result<CommitAdmission<'_>, Box<CommitFailure>> {
        self.committing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                Box::new(CommitFailure::new(
                    CommitError::InFlight,
                    self.identity,
                    self.route(),
                ))
            })?;
        Ok(CommitAdmission {
            workspace: self,
            released: false,
        })
    }
    /// The admitted invocation owns the flag through any wrapper's releases.
    /// `published` runs once, only after History answered this publication
    /// with the saved root; a failed, unknown or conflicted publication, and
    /// every earlier failure, never runs it.
    ///
    /// When the Branch head moved since the Workspace bound, the candidate is
    /// merged into the head in the same Save unless `force`. A clean merge is
    /// published against that head and installed as the new base. A conflict
    /// publishes nothing: the merge, with marker blocks, is installed as the
    /// Workspace's local base and its history binding moves to the head.
    /// Each phase is a child of `timing`; untraced, it records nothing.
    pub(super) fn commit_admitted(
        &self,
        construct: impl FnOnce(
            &Save<'_>,
            CommitCapture,
            &BranchSnapshot,
        ) -> Result<FilesystemRootId, CommitError>,
        published: impl FnOnce(),
        options: &CommitOptions,
        via: CommitVia,
        timing: &TimingScope<'_, Active>,
    ) -> Result<CommitRun, Box<CommitFailure>> {
        let force = options.force;
        let mut retained = CommitFailure::new(
            CommitError::Context("incomplete Commit"),
            self.identity,
            self.route(),
        );
        let mut producer = None;
        let mut merge = None;
        let mut replaced = None;
        let mut checkpoint = None;
        let mut conflict = None;
        let mut installed = false;
        let result = (|| {
            let snapshot = self.snapshot().map_err(CommitError::Workspace)?;
            retained.binding = Some(snapshot.clone());
            let local = self.local_root(&snapshot)?;
            let original_base = self.workspace.base().map_err(CommitError::Workspace)?;
            // Nothing above the installed floor and the head where the
            // Workspace bound it: UpToDate with no capture, Save, lane or
            // install (#352 c04 records why the answer stays exact).
            let state = phase(timing, "uptodate.state", || {
                self.job(|job_client: &crate::OwnerClient, job_route| job_client.state(job_route))
                    .map_err(CommitError::Owner)
            })?;
            if state.dirty_inodes == 0
                && state.dirty_directory_entries == 0
                && state.captured.is_none()
                && state.consolidating.is_none()
                && state.base_root == local.to_bytes()
                && self
                    .pending_snapshot()
                    .map_err(CommitError::Workspace)?
                    .is_none()
            {
                let current = phase(timing, "uptodate.head", || {
                    self.store
                        .history
                        .branch_snapshot(snapshot.branch.id)
                        .map_err(CommitError::History)?
                        .ok_or(CommitError::Context("bound Branch is absent"))
                })?;
                if current.branch.head_commit == snapshot.branch.head_commit
                    && current.branch.base_layer == snapshot.branch.base_layer
                {
                    retained.published = Some(CommitOutcome::UpToDate {
                        head: current.branch.head_commit,
                        root: current.effective_root,
                    });
                    retained.saved = Some(Default::default());
                    retained.storage = Some(Default::default());
                    return Ok(());
                }
            }
            retained.phase = CommitPhase::Capture;
            let selected = phase(timing, "capture", || self.capture_commit(&snapshot))?;
            let capture = selected.capture();
            retained.capture = Some(capture);
            retained.recorded = selected.recorded();
            if capture.base_root != local.to_bytes() || original_base.identity().0 != local {
                return Err(CommitError::Context(
                    "capture differs from bound history root",
                ));
            }
            retained.phase = CommitPhase::Begin;
            let (storage, before) = phase(timing, "producer", || {
                self.producer
                    .take(&self.store)
                    .map_err(CommitError::Storage)
            })?;
            let built = (|| {
                let save = phase(timing, "begin", || {
                    storage.begin_save().map_err(CommitError::Storage)
                })?;
                retained.phase = CommitPhase::Construct;
                let candidate = match phase(timing, "construct", || {
                    construct(&save, selected, &snapshot)
                }) {
                    Ok(root) => root,
                    Err(original) => {
                        return Err(match save.take_failure() {
                            Some(storage) => CommitError::Construction {
                                original: Box::new(original),
                                storage,
                            },
                            None => original,
                        })
                    }
                };
                // Two-stage merge (#355). The head is read and merged with
                // before the publication lane, so the merge's records and its
                // whole build stay outside it. In the lane the head is read
                // again: if it moved meanwhile, only what landed in between
                // is merged into the result, in memory, then published.
                let current = phase(timing, "head", || {
                    self.refuse_markers(&save, candidate)?;
                    self.store
                        .history
                        .branch_snapshot(snapshot.branch.id)
                        .map_err(CommitError::History)?
                        .ok_or(CommitError::Context("bound Branch is absent"))
                })?;
                let moved = current.branch.head_commit != snapshot.branch.head_commit
                    || current.branch.base_layer != snapshot.branch.base_layer;
                let root = if moved && !force {
                    let (root, report) = phase(timing, "merge", || {
                        self.merge_candidate(&save, &snapshot, &current, candidate)
                    })?;
                    merge = Some(report);
                    root
                } else {
                    candidate
                };
                // Everything that grows with the Commit runs before the lane:
                // the Save's finish, the logical count and the binding hints.
                let node = current
                    .branch
                    .head_commit
                    .map(layerfs_history::NodeId::Commit)
                    .unwrap_or(layerfs_history::NodeId::Layer(current.branch.base_layer));
                let known = phase(timing, "logical.known", || {
                    self.store.logical_size(node).map_err(CommitError::History)
                })?;
                retained.phase = CommitPhase::Finish;
                let saved = phase(timing, "finish", || {
                    save.finish().map_err(CommitError::Storage)
                })?;
                self.store.note_save(&saved);
                retained.saved = Some(saved);
                // Count only after finish has proved the root's references
                // closed, so a missing dependency stays a Storage refusal.
                let logical = phase(timing, "logical.count", || {
                    self.logical_after(&storage, current.effective_root, root, known)
                })?;
                phase(timing, "bindings", || {
                    let reader = storage.reader().map_err(CommitError::Storage)?;
                    super::hints::record_new_bindings(
                        &reader,
                        &*self.store.history,
                        Some(FilesystemRootId(current.effective_root)),
                        root,
                    )
                })?;
                let lane = phase(timing, "lane", || {
                    self.store
                        .history
                        .publish_lane()
                        .map_err(CommitError::History)
                })?;
                let conflicted = merge
                    .as_ref()
                    .is_some_and(|report: &MergeReport| !report.conflicts.is_empty());
                let (current, root, moved, logical) = if force || conflicted {
                    (current, root, moved, logical)
                } else {
                    let now = phase(timing, "head.lane", || {
                        self.store
                            .history
                            .branch_snapshot(snapshot.branch.id)
                            .map_err(CommitError::History)?
                            .ok_or(CommitError::Context("bound Branch is absent"))
                    })?;
                    if now.branch.head_commit == current.branch.head_commit
                        && now.branch.base_layer == current.branch.base_layer
                    {
                        (current, root, moved, logical)
                    } else {
                        // The in-lane step has its own Save; its count and
                        // hints cover only what it changed in the result.
                        let step_save = storage.begin_save().map_err(CommitError::Storage)?;
                        let (stepped, step) = phase(timing, "merge.lane", || {
                            self.merge_in_lane(&step_save, &current, &now, root, candidate)
                        })?;
                        let step_saved = phase(timing, "finish.lane", || {
                            step_save.finish().map_err(CommitError::Storage)
                        })?;
                        self.store.note_save(&step_saved);
                        if let Some(saved) = retained.saved.as_mut() {
                            saved.canonical_bytes += step_saved.canonical_bytes;
                            saved.stored_bytes += step_saved.stored_bytes;
                            saved.metadata_bytes += step_saved.metadata_bytes;
                        }
                        let logical = phase(timing, "logical.lane", || {
                            self.logical_after(&storage, root.0, stepped, logical)
                        })?;
                        phase(timing, "bindings.lane", || {
                            let reader = storage.reader().map_err(CommitError::Storage)?;
                            super::hints::record_new_bindings(
                                &reader,
                                &*self.store.history,
                                Some(root),
                                stepped,
                            )
                        })?;
                        merge = Some(match merge.take() {
                            Some(first) => MergeReport {
                                from: first.from,
                                lww: first.lww.into_iter().chain(step.lww).collect(),
                                ..step
                            },
                            None => step,
                        });
                        (now, stepped, true, logical)
                    }
                };
                Ok((root, candidate, current, moved, logical, lane))
            })();
            retained.storage = Some(super::producer::since(storage.diagnostics(), before));
            let (candidate, ours, current, moved, logical, lane) = built?;
            if candidate != ours {
                replaced = Some((ours.0, candidate.0));
            }
            producer = Some(storage);
            let conflicted = merge
                .as_ref()
                .is_some_and(|report: &MergeReport| !report.conflicts.is_empty());
            let mut lane = Some(lane);
            if conflicted {
                drop(lane.take());
            }
            // A merge is published against the head it merged with; a forced
            // or unmoved Commit against the head the Workspace bound.
            let target = if merge.is_some() { &current } else { &snapshot };
            if !conflicted {
                retained.intent = Some(CommitRequest {
                    branch: snapshot.branch.id,
                    expected_head: target.branch.head_commit,
                    expected_base: target.branch.base_layer,
                    expected_root: target.effective_root,
                    candidate_root: candidate.0,
                    profile: snapshot.profile,
                    scope: snapshot.scope,
                    second_parent: None,
                    meta: CommitMeta {
                        message: Some(
                            options
                                .message
                                .clone()
                                .unwrap_or_else(|| "Commit Workspace changes".into()),
                        ),
                        statistics: {
                            let saved = retained
                                .saved
                                .as_ref()
                                .ok_or(CommitError::Context("finished Save"))?;
                            layerfs_history::CommitStatistics {
                                logical,
                                new_canonical: saved.canonical_bytes,
                                new_stored: saved.stored_bytes,
                                new_metadata: saved.metadata_bytes,
                            }
                        },
                        merged_from: merge.as_ref().map(|_| {
                            snapshot
                                .branch
                                .head_commit
                                .map(layerfs_history::NodeId::Commit)
                                .unwrap_or(layerfs_history::NodeId::Layer(
                                    snapshot.branch.base_layer,
                                ))
                        }),
                        lww: merge
                            .as_ref()
                            .map_or_else(Vec::new, |report| report.lww.clone()),
                        ..CommitMeta::new(via)
                    },
                    force: force && moved,
                });
            }
            retained.phase = CommitPhase::Validate;
            let mut next = target.clone();
            next.effective_root = candidate.0;
            retained.prepared = Some(phase(timing, "validate", || {
                let ports = self.store.ports(snapshot.scope);
                let client = ports.client();
                let checked =
                    checked_base(client.clone(), &next).map_err(|error| CommitError::Content {
                        error,
                        provider: ports
                            .failure()
                            .unwrap_or_else(|error| Some(Arc::new(error))),
                    })?;
                if checked.root().root_inode() != original_base.root().root_inode() {
                    return Err(CommitError::Context(
                        "candidate changes root inode identity",
                    ));
                }
                self.workspace
                    .scoped(client)
                    .map_err(CommitError::Workspace)?
                    .prepare_base_install(capture, candidate)
                    .map_err(CommitError::Workspace)
            })?);
            let pending = if conflicted {
                // Nothing is published: the history binding follows the head
                // merged with, and the merge stays the local base.
                next = current.clone();
                let pending = Arc::new(super::commit_merge::PendingMerge {
                    local_root: candidate.0,
                    conflicted: merge
                        .as_ref()
                        .map(|report| report.conflicts.iter().map(|c| c.path.clone()).collect())
                        .unwrap_or_default(),
                });
                if let Some(recorded) = retained.recorded {
                    let version = self
                        .job(|client, route| {
                            client.call(route, crate::ServiceClass::General, None, |db| {
                                Ok(db.profile().schema_version)
                            })
                        })
                        .map_err(CommitError::Owner)?;
                    if version != 44 {
                        return Err(CommitError::Context(
                            "recorded local conflict disposition is not available",
                        ));
                    }
                    let history = self.store.history();
                    super::durable_bind::validate_snapshot(history, &current)
                        .map_err(CommitError::History)?;
                    let description =
                        super::conflict_paths::pages(&pending.conflicted, |_, _| Ok(()))?;
                    let target_bytes =
                        layerfs_bridge::snapshot::encode(&current).map_err(|cause| {
                            CommitError::Owner(crate::OwnerError::BindingCodec(cause))
                        })?;
                    let root = candidate.0.to_bytes();
                    let stage = self
                        .job(|client, route| {
                            client.call(route, crate::ServiceClass::General, None, move |db| {
                                db.begin_conflict_stage(
                                    recorded,
                                    layerfs_overlay::ConflictStageInput {
                                        local_root: root,
                                        target_snapshot: &target_bytes,
                                        paths_count: description.paths,
                                        byte_count: description.bytes,
                                        final_digest: description.digest,
                                    },
                                )
                            })
                        })
                        .map_err(CommitError::Owner)?;
                    let checked =
                        super::conflict_paths::pages(&pending.conflicted, |page, data| {
                            self.job(|client, route| {
                                client.call(route, crate::ServiceClass::General, None, move |db| {
                                    db.append_conflict_page(stage, page, &data)
                                })
                            })
                            .map_err(CommitError::Owner)
                        })?;
                    if checked != description {
                        return Err(CommitError::Context("conflict path staging changed"));
                    }
                    conflict = Some(LiveConflict {
                        stage,
                        catalog: history.catalog_id(),
                        incarnation: history.incarnation(),
                        original_binding: snapshot.clone(),
                        target: current.clone(),
                        marker: candidate.0,
                        paths: description,
                    });
                }
                Some(pending)
            } else {
                retained.phase = CommitPhase::Publish;
                let intent = retained.intent.as_ref().expect("checked intent");
                let frozen = if let Some(recorded) = retained.recorded {
                    let request =
                        encode_checkpoint_request(intent).map_err(CommitError::History)?;
                    let digest = checkpoint_request_digest(&request);
                    let target_bytes =
                        layerfs_bridge::snapshot::encode(target).map_err(|cause| {
                            CommitError::Owner(crate::OwnerError::BindingCodec(cause))
                        })?;
                    let prepared = self.job(|client, route| {
                        client.prepare_checkpoint_publication(
                            route,
                            recorded,
                            request.clone(),
                            digest,
                            candidate.0.to_bytes(),
                            target_bytes,
                        )
                    });
                    match prepared {
                        Ok(()) => retained.publication_prepared = true,
                        Err(error) => {
                            retained.publication_prepared = super::owner_uncertain(&error);
                            return Err(CommitError::Owner(error));
                        }
                    }
                    Some((recorded, request, digest))
                } else {
                    None
                };
                let outcome = phase(timing, "publish", || match &frozen {
                    Some((recorded, _, _)) => self
                        .store
                        .history
                        .commit_checkpoint(
                            CheckpointAttemptId::from_bytes(recorded.attempt()),
                            intent,
                        )
                        .map_err(CommitError::History),
                    None => self
                        .store
                        .history
                        .commit_branch(intent)
                        .map_err(CommitError::History),
                })?;
                let (head, root) = match &outcome {
                    CommitOutcome::Committed { record, .. } => (Some(record.id), record.root),
                    CommitOutcome::UpToDate { head, root } => (*head, *root),
                };
                drop(lane.take());
                retained.published = Some(outcome.clone());
                if root != candidate.0 {
                    return Err(CommitError::Context(
                        "publication differs from saved candidate",
                    ));
                }
                next.branch.head_commit = head;
                next.head_root = head.map(|_| root);
                if let Some((recorded, request_bytes, digest)) = frozen {
                    let catalog = self.store.history.catalog_id();
                    let incarnation = self.store.history.incarnation();
                    let observed = self
                        .store
                        .observe_original_checkpoint(super::OriginalCheckpoint {
                            catalog,
                            incarnation,
                            attempt: CheckpointAttemptId::from_bytes(recorded.attempt()),
                            request: &request_bytes,
                            request_digest: digest,
                            candidate_root: candidate.0,
                            target,
                        })
                        .map_err(CommitError::History)?
                        .ok_or(CommitError::Context("checkpoint receipt is absent"))?;
                    if observed.outcome != outcome || observed.installed != next {
                        return Err(CommitError::Context(
                            "checkpoint receipt differs from result",
                        ));
                    }
                    checkpoint = Some(LiveCheckpoint {
                        recorded,
                        catalog,
                        incarnation,
                        request: intent.clone(),
                        request_bytes,
                        digest,
                        original_binding: snapshot.clone(),
                        target: target.clone(),
                        installed: observed.installed,
                        outcome,
                    });
                }
                published();
                None
            };
            retained.phase = CommitPhase::Install;
            let durable_snapshot =
                if !conflicted
                    && self
                        .owner
                        .durable_identity()
                        .map_err(CommitError::Owner)?
                        .is_some()
                {
                    Some(layerfs_bridge::snapshot::encode(&next).map_err(|cause| {
                        CommitError::Owner(crate::OwnerError::BindingCodec(cause))
                    })?)
                } else {
                    None
                };
            let prepared = retained.prepared.take().expect("prepared install");
            phase(timing, "install", || {
                if let Some(original) = &checkpoint {
                    let target_snapshot = layerfs_bridge::snapshot::encode(&original.target)
                        .map_err(|cause| {
                            CommitError::Owner(crate::OwnerError::BindingCodec(cause))
                        })?;
                    let input = crate::overlay::lifetime_operations::CheckpointInstallInput {
                        recorded: original.recorded,
                        request_digest: original.digest,
                        target_snapshot,
                        installed_snapshot: durable_snapshot.expect("recorded durable snapshot"),
                        outcome: checkpoint_outcome(&original.outcome),
                    };
                    self.job(|client, route| {
                        client.install_prepared_checkpoint(
                            route,
                            self.workspace.clone(),
                            prepared,
                            input,
                        )
                    })
                    .map_err(CommitError::Owner)
                } else if let Some(original) = &conflict {
                    let target_snapshot = layerfs_bridge::snapshot::encode(&original.target)
                        .map_err(|cause| {
                            CommitError::Owner(crate::OwnerError::BindingCodec(cause))
                        })?;
                    self.job(|client, route| {
                        client.install_prepared_conflict(
                            route,
                            self.workspace.clone(),
                            prepared,
                            original.stage,
                            target_snapshot,
                        )
                    })
                    .map_err(CommitError::Owner)
                } else {
                    self.job(|client, route| {
                        client.install_prepared_recorded(
                            route,
                            self.workspace.clone(),
                            prepared,
                            durable_snapshot,
                        )
                    })
                    .map_err(CommitError::Owner)
                }
            })?;
            installed = true;
            *self
                .snapshot
                .lock()
                .map_err(|_| CommitError::Workspace(WorkspaceError::BindingPoisoned))? = next;
            *self
                .pending
                .lock()
                .map_err(|_| CommitError::Workspace(WorkspaceError::BindingPoisoned))? = pending;
            Ok(())
        })();
        if let Err(error) = result {
            retained.error = error;
            if installed {
                retained.installed = Some(Box::new(CommitSuccess {
                    history: retained.published.clone(),
                    merge: merge.take(),
                    saved: retained.saved.expect("known Save"),
                    storage: retained.storage.expect("producer observations"),
                    capture: retained.capture,
                    namespace: None,
                    replaced,
                }));
            }
            return Err(self.settle_failure(retained));
        }
        if let Some(storage) = producer {
            self.producer.keep(storage);
        }
        Ok(CommitRun {
            success: CommitSuccess {
                history: retained.published,
                merge,
                saved: retained.saved.expect("known Save"),
                storage: retained.storage.expect("producer observations"),
                capture: retained.capture,
                namespace: None,
                replaced,
            },
            checkpoint,
            conflict,
        })
    }

    /// Logical size of `after`, from `before`'s known size and the change
    /// between them. File lengths come from the Store catalogue a window at
    /// a time (`Reader::file_lengths`), never from reading file payloads; a
    /// Storage refusal keeps its original error.
    fn logical_after(
        &self,
        storage: &layerfs_storage::Storage,
        before: layerfs_content::ObjectId,
        after: FilesystemRootId,
        known: u64,
    ) -> Result<u64, CommitError> {
        let reader = storage.reader().map_err(CommitError::Storage)?;
        let mut refused = None;
        let counted = layerfs_content::filesystem::statistics::logical_size_after_with(
            &reader,
            FilesystemRootId(before),
            after,
            known,
            &mut |roots| {
                reader.file_lengths(roots).map_err(|error| {
                    refused = Some(error);
                    layerfs_content::ContentError::OutputRejected
                })
            },
        );
        match (counted, refused) {
            (Ok(size), _) => Ok(size),
            (Err(_), Some(error)) => Err(CommitError::Storage(error)),
            (Err(error), None) => Err(CommitError::Content {
                error,
                provider: None,
            }),
        }
    }
}

pub(super) fn checkpoint_outcome(outcome: &CommitOutcome) -> layerfs_overlay::CheckpointOutcome {
    match outcome {
        CommitOutcome::Committed { record, displaced } => {
            layerfs_overlay::CheckpointOutcome::Committed {
                head: record.id.to_bytes(),
                displaced: displaced.map(|value| value.to_bytes()),
            }
        }
        CommitOutcome::UpToDate { head, .. } => layerfs_overlay::CheckpointOutcome::UpToDate {
            head: head.map(|value| value.to_bytes()),
        },
    }
}
