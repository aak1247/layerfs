//! Original Store receipts are observed without another publication.
#![cfg(target_os = "linux")]

use layerfs_content::ObjectId;
use layerfs_history::{
    checkpoint_request_digest, decode_checkpoint_request, encode_checkpoint_request, BranchId,
    BranchSnapshot, CatalogId, CheckpointAttemptId, CommitId, CommitMeta, CommitOutcome,
    CommitRecord, CommitRequest, CommitVia, ForkRequest, ForkSource, HistoryCatalog,
    HistoryCatalogConfig, HistoryError, HistoryName, LayerStackId, StackInitialization,
};
use layerfs_persistence::{Handles, PersistenceConfig, SqlitePersistenceProfile};
use layerfs_storage::{ReservationBlocks, StoragePolicy};
use layerfs_store_service::{
    bootstrap::open_store,
    store::{check_checkpoint_outcome, OriginalCheckpoint, Store},
};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

const BINDING: &[u8] = b"checkpoint-observation-store";
const CURSOR: [u8; 32] = [71; 32];

struct Fixture {
    directory: PathBuf,
    store: Option<Arc<Store>>,
    initial: BranchSnapshot,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let directory = std::env::temp_dir().join(format!(
            "layerfs-checkpoint-observation-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let config = PersistenceConfig::sqlite(&directory.join("store.sqlite"))
            .with_sqlite_profile(SqlitePersistenceProfile::Durable);
        let handles = Handles::create(
            config.clone(),
            StoragePolicy::frozen_default(),
            &HistoryCatalogConfig {
                binding_key: BINDING.to_vec(),
                incarnation: 7,
                cursor_key: CURSOR,
            },
        )
        .unwrap();
        let history = &handles.history;
        let stack = history
            .initialize_layerstack(&StackInitialization {
                logical: 0,
                stack: LayerStackId::from_authority([1; 16]),
                name: HistoryName::new("main").unwrap(),
                scope: object(4),
                profile: object(5),
                genesis_root: object(6),
            })
            .unwrap();
        let initial = history
            .fork(&ForkRequest {
                stack: stack.id,
                branch: BranchId::from_authority([2; 16]),
                name: HistoryName::new("work").unwrap(),
                source: ForkSource::Layer(stack.head_layer),
            })
            .unwrap();
        drop(handles);
        let store = open_store(
            config,
            BINDING,
            CURSOR,
            2,
            1024 * 1024,
            ReservationBlocks::default(),
        )
        .unwrap();
        assert!(store.has_durable_writer());
        Self {
            directory,
            store: Some(store),
            initial,
        }
    }
    fn store(&self) -> &Store {
        self.store.as_deref().unwrap()
    }
    fn observe(
        &self,
        attempt: u8,
        request: &CommitRequest,
        target: &BranchSnapshot,
    ) -> layerfs_history::HistoryResult<Option<layerfs_store_service::store::CheckpointObservation>>
    {
        let bytes = encode_checkpoint_request(request).unwrap();
        self.store()
            .observe_original_checkpoint(OriginalCheckpoint {
                catalog: CatalogId::derive(BINDING).unwrap(),
                incarnation: 7,
                attempt: CheckpointAttemptId::from_bytes([attempt; 32]),
                request: &bytes,
                request_digest: checkpoint_request_digest(&bytes),
                candidate_root: request.candidate_root,
                target,
            })
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "RETAINED_CHECKPOINT_OBSERVATION_FIXTURE {}",
                self.directory.display()
            );
        } else {
            drop(self.store.take());
            fs::remove_dir_all(&self.directory).unwrap();
        }
    }
}

fn object(value: u8) -> ObjectId {
    ObjectId::from_bytes(&[value; 32]).unwrap()
}
fn attempt(value: u8) -> CheckpointAttemptId {
    CheckpointAttemptId::from_bytes([value; 32])
}
fn context<'a>(
    catalog: CatalogId,
    incarnation: u64,
    raw: &'a [u8],
    target: &'a BranchSnapshot,
) -> OriginalCheckpoint<'a> {
    OriginalCheckpoint {
        catalog,
        incarnation,
        attempt: attempt(2),
        request: raw,
        request_digest: checkpoint_request_digest(raw),
        candidate_root: decode_checkpoint_request(raw).unwrap().candidate_root,
        target,
    }
}
fn request(snapshot: &BranchSnapshot, candidate: ObjectId) -> CommitRequest {
    CommitRequest {
        branch: snapshot.branch.id,
        expected_head: snapshot.branch.head_commit,
        expected_base: snapshot.branch.base_layer,
        expected_root: snapshot.effective_root,
        candidate_root: candidate,
        profile: snapshot.profile,
        scope: snapshot.scope,
        second_parent: None,
        meta: CommitMeta::new(CommitVia::Workspace),
        force: false,
    }
}

#[test]
fn original_receipts_survive_later_branch_movement_without_republication() {
    let fixture = Fixture::new();
    let history = fixture.store().history();
    let initial = fixture.initial.clone();
    let empty = request(&initial, initial.effective_root);
    let empty_result = history.commit_checkpoint(attempt(1), &empty).unwrap();
    assert!(matches!(
        empty_result,
        CommitOutcome::UpToDate { head: None, .. }
    ));
    let mut first = request(&initial, object(7));
    first.meta.message = Some("checkpoint".into());
    first.meta.lww_truncated = true;
    let published = history.commit_checkpoint(attempt(2), &first).unwrap();
    let second_target = history.branch_snapshot(initial.branch.id).unwrap().unwrap();
    let unchanged = request(&second_target, second_target.effective_root);
    let unchanged_result = history.commit_checkpoint(attempt(3), &unchanged).unwrap();
    assert!(matches!(
        unchanged_result,
        CommitOutcome::UpToDate { head: Some(_), .. }
    ));
    history
        .commit_branch(&request(&second_target, object(8)))
        .unwrap();
    let current = history.branch_snapshot(initial.branch.id).unwrap().unwrap();

    let empty_seen = fixture.observe(1, &empty, &initial).unwrap().unwrap();
    assert_eq!(empty_seen.outcome, empty_result);
    assert_eq!(empty_seen.installed, initial);
    let first_seen = fixture
        .observe(2, &first, &fixture.initial)
        .unwrap()
        .unwrap();
    assert_eq!(first_seen.outcome, published);
    assert_eq!(first_seen.installed, second_target);
    let unchanged_seen = fixture
        .observe(3, &unchanged, &second_target)
        .unwrap()
        .unwrap();
    assert_eq!(unchanged_seen.outcome, unchanged_result);
    assert_eq!(unchanged_seen.installed, second_target);
    assert!(fixture
        .observe(4, &unchanged, &second_target)
        .unwrap()
        .is_none());
    assert_eq!(
        history.branch_snapshot(current.branch.id).unwrap().unwrap(),
        current
    );
}

#[test]
fn wrong_store_or_original_context_is_refused_before_any_change() {
    let fixture = Fixture::new();
    let history = fixture.store().history();
    let input = request(&fixture.initial, object(7));
    history.commit_checkpoint(attempt(2), &input).unwrap();
    let branch = history
        .branch_snapshot(fixture.initial.branch.id)
        .unwrap()
        .unwrap();
    let raw = encode_checkpoint_request(&input).unwrap();
    let mut wrong_digest = context(
        CatalogId::derive(BINDING).unwrap(),
        7,
        &raw,
        &fixture.initial,
    );
    wrong_digest.request_digest = [0; 32];
    assert!(matches!(
        fixture.store().observe_original_checkpoint(wrong_digest),
        Err(HistoryError::Integrity("checkpoint frozen input"))
    ));
    let mut wrong_candidate = context(
        CatalogId::derive(BINDING).unwrap(),
        7,
        &raw,
        &fixture.initial,
    );
    wrong_candidate.candidate_root = object(9);
    assert!(matches!(
        fixture.store().observe_original_checkpoint(wrong_candidate),
        Err(HistoryError::Integrity("checkpoint frozen input"))
    ));
    assert!(matches!(
        fixture.store().observe_original_checkpoint(context(
            CatalogId::from_bytes([0; 32]),
            7,
            &raw,
            &fixture.initial
        )),
        Err(HistoryError::Integrity("checkpoint Store identity"))
    ));
    assert!(matches!(
        fixture.store().observe_original_checkpoint(context(
            CatalogId::derive(BINDING).unwrap(),
            8,
            &raw,
            &fixture.initial
        )),
        Err(HistoryError::Integrity("checkpoint Store identity"))
    ));
    let mut other_target = fixture.initial.clone();
    other_target.effective_root = object(9);
    assert!(matches!(
        fixture.store().observe_original_checkpoint(context(
            CatalogId::derive(BINDING).unwrap(),
            7,
            &raw,
            &other_target
        )),
        Err(HistoryError::Integrity("checkpoint target"))
    ));
    other_target = fixture.initial.clone();
    other_target.branch.name = HistoryName::new("other").unwrap();
    assert!(matches!(
        fixture.store().observe_original_checkpoint(context(
            CatalogId::derive(BINDING).unwrap(),
            7,
            &raw,
            &other_target
        )),
        Err(HistoryError::Integrity("checkpoint Branch identity"))
    ));
    let mut altered = input.clone();
    altered.meta.message = Some("different request".into());
    assert!(matches!(
        fixture.observe(2, &altered, &fixture.initial),
        Err(HistoryError::Integrity("checkpoint receipt identity"))
    ));
    assert_eq!(
        history.branch_snapshot(branch.branch.id).unwrap().unwrap(),
        branch
    );
}

#[test]
fn forced_results_keep_the_original_target_after_the_branch_moves() {
    let fixture = Fixture::new();
    let history = fixture.store().history();
    let target = &fixture.initial;
    history.commit_branch(&request(target, object(7))).unwrap();
    let mut force = request(target, object(8));
    force.force = true;
    let committed = history.commit_checkpoint(attempt(5), &force).unwrap();
    assert!(matches!(
        committed,
        CommitOutcome::Committed {
            displaced: Some(_),
            ..
        }
    ));
    let forced_snapshot = history.branch_snapshot(target.branch.id).unwrap().unwrap();
    let current = history.commit_checkpoint(attempt(6), &force).unwrap();
    assert!(matches!(
        current,
        CommitOutcome::UpToDate { head: Some(_), .. }
    ));
    history
        .commit_branch(&request(&forced_snapshot, object(9)))
        .unwrap();
    let newest = history.branch_snapshot(target.branch.id).unwrap().unwrap();
    let observed = fixture.observe(5, &force, target).unwrap().unwrap();
    assert_eq!(observed.outcome, committed);
    assert_eq!(observed.installed, forced_snapshot);
    let observed = fixture.observe(6, &force, target).unwrap().unwrap();
    assert_eq!(observed.outcome, current);
    assert_eq!(observed.installed, forced_snapshot);
    assert_eq!(
        history.branch_snapshot(target.branch.id).unwrap().unwrap(),
        newest
    );
}

#[test]
fn impossible_nonforced_and_displaced_outcomes_are_rejected() {
    let fixture = Fixture::new();
    let target = &fixture.initial;
    let mut request = request(target, object(7));
    let mut id_bytes = [8; 33];
    id_bytes[0] = 0x12;
    let other = CommitId::from_bytes(id_bytes).unwrap();
    let unchanged = CommitOutcome::UpToDate {
        head: Some(other),
        root: request.expected_root,
    };
    assert!(matches!(
        check_checkpoint_outcome(&request, &unchanged),
        Err(HistoryError::Integrity("checkpoint impossible UpToDate"))
    ));
    let record = CommitRecord {
        id: other,
        stack: target.branch.stack,
        root: request.candidate_root,
        parent: request.expected_head,
        base_layer: request.expected_base,
        second_parent: None,
        meta: request.meta.clone(),
    };
    let committed = CommitOutcome::Committed {
        record: Box::new(record),
        displaced: Some(other),
    };
    assert!(matches!(
        check_checkpoint_outcome(&request, &committed),
        Err(HistoryError::Integrity("checkpoint impossible Commit"))
    ));
    request.force = true;
    assert!(check_checkpoint_outcome(&request, &unchanged).is_ok());
    assert!(check_checkpoint_outcome(&request, &committed).is_ok());
    request.expected_head = Some(other);
    assert!(matches!(
        check_checkpoint_outcome(&request, &committed),
        Err(HistoryError::Integrity("checkpoint impossible Commit"))
    ));
}
