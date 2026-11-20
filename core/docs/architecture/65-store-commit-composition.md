# In-process Store Commit and local install

> **Status:** Implemented Store-half composition after `01e60021a`. S10 live
> namespace normalization and S8 FUSE/Exec are separate work.

> **Staging removed, 2026-10-15 ([#327](https://github.com/Ephemeral-AI-Lab/layerfs/issues/327)):**
> the `workspace_stage` table, stage tokens and the stage API (`stage_changes`,
> `commit_staged`, `stages`, `discard_stage`, `stage_and_commit`) are removed.
> `HistoryCatalog::commit_branch(CommitRequest)` publishes a saved candidate in one
> write transaction against an expected head: a moved head is `HeadMoved`, or with
> `force` an overwrite that keeps the captured parent and reports the displaced
> head. The text below keeps its original source pin.

> **Merge-on-commit, 2026-10-15 (#327 step 6):** when the Branch head moved
> since the Workspace bound, the Commit merges its candidate into the head in
> the same Save unless `force`. A clean merge is published against that head and
> installed as the new base; a conflict publishes nothing and installs the merge
> with marker blocks as the Workspace's local base. See
> [architecture 81](81-compare-and-merge.md#merge-on-commit).

> **Recorded checkpoint work:** the experimental durable checkpoint path
> freezes the complete `CommitRequest` in versioned LFCP bytes. Its decoder
> validates the entire canonical encoding, including merge facts and optional
> identities, before returning a request for comparison with an original Store
> receipt. Decoding does not publish, replay or authorize recovery; the
> authenticated Store/Workspace reconciliation path remains separate work.
> A Store-side observation can compare the request and original target against
> its own exact receipt and derive the installed snapshot without reading the
> current Branch head as the result. It checks durable Store identity and
> history structure; the caller still owns Workspace fencing and content-root
> validation. A missing receipt alone does not establish nonpublication.
> A live schema-43 or schema-44 durable Workspace selects this path only after the actual
> durable Store confirms receipt support. Its admitted Commit records capture,
> construction owners and the final request, publishes once, reads that Store's
> original receipt, then pairs the Overlay and Workspace base install and
> updates the outer binding. Original reader/operation release and exact local
> maintenance precede settlement. The clean no-capture answer and older durable
> formats retain their existing paths. Schema 43 still refuses a recorded
> merge conflict before publication. Schema 44 installs the checked local
> marker root and ordered conflict paths without publishing; a later Commit
> refuses unresolved markers and can publish after they are resolved. Cold
> pending-operation and orphan reconciliation remain separate work and are
> not implied by live settlement.
> New durable overlays use schema 44, which reserves an independently checked
> binding and paged conflict description; existing schema 43 is neither
> rewritten nor migrated on ordinary open. A local capture can stage ordered
> conflict bytes one bounded page at a time, excluding publication of that
> capture. A definite prepublication failure folds the original capture and
> reclaims only its written pages through bounded maintenance. After the caller
> fences the original writers and consumers, catalog recovery can reopen a
> settled active conflict under its original lease, authenticate its Store and
> historical target, and verify its ordered path pages before advancing the
> namespace. Ordinary recovery still refuses conflicts. Pending capture,
> publication and orphan reconciliation remain unimplemented; those original
> records remain retained, and an unknown call is never resent.

BoundWorkspace::commit admits one active lifecycle for that Workspace, captures
once through the existing overlay owner, creates independently owned Storage/Save
state and invokes one synchronous Content producer with the exact capture and
frozen BranchSnapshot. The producer returns a canonical root or an original
CommitError; it must construct the capture faithfully. There is no hidden native
Init, automatic Branch refresh, whole-Commit mutex or repeated attempt.

Save finishes before the adapter performs bounded candidate scope/profile/root
inode/directory/metadata checks and prepares the paired local base transition.
It then calls HistoryCatalog::stage_and_commit once. The exact known outcome
supplies the next head/root without a reread. One original InstallPrepared owner
job advances the shared Workspace base and preserves later active mutations.
The local Branch metadata is replaced only after known install. Ordinary reads
and other Workspaces retain independent sources and read capacity throughout.

CommitSuccess carries the exact history outcome, Save outcome and per-producer
Storage diagnostics and the original Capture. Capture and install return typed
values directly; daemon counters record their actual exclusive lock work. The counts are
Store calls/transactions, not a whole-operation timing/resource report; F14 still
owns complete accounting. A changed Commit or UpToDate with the selected small
input uses one initial reservation, one publication batch and one history write.
Required block refills remain separately counted for larger Saves.

CommitFailure retains phase, Workspace/route and original binding, capture and
StageRequest/candidate when known, Save diagnostics/outcome, prepared root,
known publication, and any exact attempted local error with its backing custody.
The first original Storage error from same-Save Content reads now survives both
take_failure and finish, matching output-sink custody. A public producer can
return typed Storage, Workspace, History or owner failures; opaque provider
uncertainty is retained conservatively rather than called definite.

After definite nonpublication, one ResolveFailed job preserves the composed live
view and starts existing bounded local consolidation. That is not Store cleanup
or a history retry. One invocation-owned admission guard covers either public
Commit entrypoint, including `commit_captured` reader and operation-owner releases.
It is acquired once before capture; the captured wrapper uses the admitted driver
without reacquiring it. Known install alone does not release the slot while those
owners remain. The Commit slot releases only on a known safe disposition.
Unknown outcome performs no resolution, install, resend or inferred discard.
Known publication with a failed/unattempted local install is explicitly still
published and blocks another Commit. A later observer read does not settle an
original unknown. These retained results remain the caller's ownership; restart
custody and an unknown resolver are not introduced. A refused `InFlight` call owns
no guard and cannot clear the admitted invocation's slot. Unwinding conservatively
keeps admission. Admission is per Workspace; disjoint Workspaces may construct,
publish and install concurrently through their independent attempts.

The [Store-half proof](../issues/307/PRE-S8-F8-COMPOSITION-20261007.md) first writes
real overlay bytes, then directly constructs the matching captured change with
public Content APIs. Its historical checks cover Committed/UpToDate, HeadMoved, missing
references, original read failures, real process-held writer Busy before Save and
at history publication, known publication/local install refusal, and later writes
through successive installs. The acknowledgement-loss case is an external public
port adapter over a real committed Store; it qualifies caller custody only,
not a native SQLite I/O failure or quarantine mechanism. A new Workspace reads
the exact published root. This does not implement or qualify S10's normalizer.

The shared Storage unknown-outcome classifier also examines both causes of a
CleanupFailed wrapper. Original unknowns supplied by a Content producer must
retain capture even when nested, while a definite wrapper still resolves once.
The real Commit regression and its prior failure are retained in the
[nested uncertainty checkpoint](../issues/307/PRE-S8-NESTED-UNKNOWN-20261007.md).

Owner supersession after `bd5ab61d6`: Commit overwrites the current Branch head
atomically and keeps the captured parent. Last database publication effect wins;
reply order does not. The same adapter installs each known candidate without
refresh/rebase. New Workspaces bind the last root; existing Workspaces keep their
own installed view. Current F8/F13 proofs and retained old-policy receipts are
linked from the [overwrite decision](../issues/307/BRANCH-OVERWRITE-DECISION-20261007.md).

## R4 captured namespace page boundary, 2026-10-08

Source addition after5cf3e1220: Workspace's
[OverlayCapturedNamespace](../../crates/layerfs-workspace/src/ports/captured_namespace.rs)
extends the existing captured-inode/run point boundary with ordered inode pages
and parent/full-name pages. Direct Overlay and the daemon
[OwnerClient adapter](../../crates/layerfs-daemon/src/overlay/captured_namespace_port.rs)
use the existing ReaderInodes/ReaderDirectoryEntries jobs and exact independently
retained CapturedReader. The constructor thread may use this synchronous port;
a native receiver/service worker must use the separate event-driven boundary.

These are local captured changes, including whiteouts, not the full base union.
Pages contain at most64 rows and resume after the last full key. Later active
inserts do not enlarge the captured EOF. Inode points and pages retain the same
reader root/floor through install and logical Close. The caller releases the
reader only after all consumers and original outcome custody have ended.

Each captured port call and Commit step waits for the Workspace lock and any
Capture/install readiness frontier before its one attempt. There are no queue
credits or reply enums. A stopped or invalid admission returns Unattempted with
the original input; an attempted error retains its original typed cause, work
and backing. Successful bounded pages transfer their owned values to the
constructor. The adapter never replays or releases reader ownership.
R4 added three reader-bound jobs (`ReaderParentDirectoryEntries`,
`ReaderDirectoryEntry`, `ReaderSymlink`) and one statement, with no schema or
index change ([captured namespace construction](78-captured-namespace-construction.md)).

[Component selection and receipts](../issues/307/checks/r4-captured-namespace-port-20261008/01-selection.json)
cover fixed capture membership, full-name ordering/whiteouts, points, retained
root/floor, release and original error custody. This boundary is implemented.
The sealed StreamedRowSource adapter, complete files/names/metadata construction
and Content backed validation/topology are implemented by R4
([captured namespace construction](78-captured-namespace-construction.md)) and reach this
driver through the product constructor `BoundWorkspace::commit_captured`
([product Commit](79-product-commit.md), R5). The driver itself is unchanged;
`CommitSuccess` and `CommitFailure` gained the `namespace` receipt.
