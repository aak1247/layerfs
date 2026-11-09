# Durable Workspace discovery evidence

`layerfs_daemon::durable_roster` provides a Linux, append-only catalog for one
Sandbox. It records an authorized Workspace identity and its original Branch
before the caller creates that Workspace's durable Overlay or exposes a token.
The catalog itself does not start a daemon or recover a mount. The daemon can
initialize one for a fresh Sandbox when explicitly configured; the Linux Node
SDK selects this fresh path for a Project initialized with a Durable Store.

The caller supplies a nonzero, stable Sandbox identity.
`Authority::for_catalog` derives the Store identity from the validated history
catalog identity and incarnation. The caller must independently authenticate
the Sandbox and prove the actual physical Store. The catalog's checksums detect
incomplete or corrupted records; they are not authentication.
All participants must use the same kernel and cooperate with its exclusive
owner-file lock. A lock released by an exited process does not prove that old
FUSE mounts, consumers or unknown external operations have been fenced.

The catalog has its own dedicated directory on ext4, XFS or Btrfs, private to
the calling user. The caller creates and synchronizes that directory and its
parent before initialization. The catalog never replaces an existing directory
or file and never removes records. Overlay files live separately; each record
supplies a deterministic filename component, and the caller must open it with
the Overlay's own identity and directory checks.

`Owner::reserve_durable` separately reserves a bounded owner slot and a fresh
local namespace before the caller writes a creation intent. Releasing an
unattempted reservation does not reuse that namespace. `create` consumes the
reservation, creates one durable Overlay and admits its exact owner into the
reserved slot. An attempted failure returns the original error and retained
slot/owner custody; it cannot be automatically released or retried. The caller
still serializes Workspace identity through this catalog and must retain both
owners when discovery or binding has an uncertain outcome.

`Service::mount_recorded` composes the capacity reservation, creation intent,
durable Overlay creation and exact Branch binding for a manually assembled
daemon. It returns the original failure and keeps the reserved control slot,
owner or bound Workspace when a later step is uncertain. Full capacity and a
duplicate identity refuse before creating an Overlay; a consumed namespace is
not reused. The caller authenticates and serializes the catalog and selects a
verified physical Store.

An explicitly activated, manually assembled Service can own the catalog and
route its existing Mount and Unmount control requests through those operations.
Activation requires a concrete Durable Store, verifies the private local Overlay
directory and performs one complete catalog recovery before admitting calls.
The directory inode is retained, each Overlay path is resolved through that
held directory, and its original name is checked before and after creation or
close; replacement or loss of privacy quarantines the original directory and
catalog lock. After that quarantine, Attach, Commit and other effectful
controls are refused even if a token was already exposed; read-only status and
Locate remain available. An uncertain operation is sent as Unknown while
retaining its original error or successful receipt. The caller must still independently
authenticate the physical Store, fence old processes/mounts and consumers,
and reconcile unknown operations before activation. AutoTask does not yet
activate this control path.

For a newly created Sandbox, `Service::activate_fresh_control` checks the
empty catalog and Overlay directories through retained descriptors, syncs each
directory and its private parent, and creates the catalog through the original
directory handle. Both directories must share the caller-observed Durable Store
device. An admitted failure retains the exact error and refuses subsequent
effectful controls, rather than falling back to disposable Workspaces.

The Linux daemon's version-7 private setup can select this fresh path with a
nonzero Sandbox identity and either two pre-created private directories or a
new identity-specific directory beneath the authenticated Store volume. The
caller supplies a fresh identity and authenticates the physical Store. A
Disposable installation is refused before Store creation; failure after an
installed Store retains the original service and reports the Retained phase.
Version-5 setup remains readable and keeps its disposable behavior; version 6
keeps its pre-created-directory meaning. The Docker SDK validates that both
requested directories are on the named Store volume, but does not prove an old
container and its consumers exited.
This fresh path refuses existing contents and is not recovery.

SDK discovery recognizes a durable Sandbox identity in its private record.
If the Engine reports that container stopped or missing, it retains the
container and records and returns `RecoveryRequired`; malformed identities
are refused rather than treated as disposable. Explicit Sandbox close also
refuses until durable catalog ownership can be resolved. Legacy records with
no durable identity keep their prior disposable behavior. New Sandboxes of a
Durable Project record their fresh identity; a Disposable Project continues to
use disposable Workspaces.

`SandboxApi::create_file_observed` also exposes one ordered creation attempt:
the caller can record intent before Docker Create and Start, then record the
acknowledged container, upload, Start, authenticated Hello and optional Store
installation. An observer failure stops before the next side effect and
returns the exact container, control owner, status and installation receipt
already obtained. This callback does not itself persist a journal or resolve
unknown Docker outcomes; its caller must do so. On Linux, the Node binding
synchronizes a private creation intent containing the original identities and
deployment inputs before Docker Create, then records each milestone before the
next effect. It writes a private Sandbox record and completion receipt before
retiring the intent. An uncertain outcome retains the original owner in the
process, leaves the durable evidence and refuses automatic retries. A stopped
or missing durable container still requires independently fenced recovery.
Native binding tests use a simulated Engine and daemon to check successful
creation, an unknown Start, and failure while recording the Create reply; they
do not establish a real Docker or FUSE recovery result for this SDK path.

An isolated Docker proof subsequently exercised the real Node binding, daemon
and FUSE mounts with a Durable Store: two distinct Workspaces wrote and synced
different bytes. After the original test Sandbox was killed, its two original
processes and FUSE connections were checked absent. A new Node process refused
to reclaim the stopped Sandbox and retained both records. A separate test-only
Service then opened the same Store and catalog, selected fresh namespaces,
remounted both exact Workspaces and read their original bytes. Its output,
source and binary hashes, old-owner fence and retention ownership are under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/sdk-durable-docker-20261110-sstqc67g/`.
This manually fenced same-host recovery does not implement automatic SDK
recovery, Kubernetes reconstruction or physical host power-loss handling.

A separate attempted 32-Workspace durable capacity proof did not reach its
first mount: Docker Create returned an unknown outcome while the Engine was
slow, and the exact test-labelled container was later observed in Created
state. The SDK retained the creation intent; a fresh Node process without a
Docker socket refused to replay it. The original test container, volume and
diagnostic evidence are retained under the same proof directory. This failed
attempt is not a 32-Workspace capacity result.

A separate manually assembled durable Service later mounted 32 real FUSE
Workspaces at once, refused a 33rd at its configured capacity and acknowledged
closure of all 32. Its successful output and hashes are under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-capacity-fuse-v2-20261110-8m775z31/`.
The prior test attempt had mounted 32 but failed an incorrect assertion about
the direct API's wrapped refusal; that failure is retained separately. This
functional capacity check does not measure performance or establish SDK or
Kubernetes capacity.

The same isolated Service test was then run with capacity 100: 100 durable
FUSE Workspaces were mounted together, the 101st was refused and all 100 were
closed. Output and hashes are under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-hundred-fuse-20261110-0p024vdd/`.
They share one Branch and were not used for a sustained workload; this is not
evidence of throughput, recovery at that scale, SDK capacity or Kubernetes.

A separate fresh SDK capacity fixture then used the real Docker/FUSE path with
capacity 32. It mounted 32 durable Workspaces from one Branch, received
`Capacity` on the 33rd, and acknowledged all 32 normal discarding closes. A
new Node process without a Docker socket validated 32 close records and found
no Workspace records before reporting the absent socket. The host's FUSE
connections returned to their pre-test baseline. Output, source/binary hashes,
the exact retained Sandbox and volume, and cleanup ownership are recorded in
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/sdk-durable-capacity-20261110-i71_ptdg/receipt.txt`.
This was an independent fixture; the earlier unknown Docker Create was not
retried. It does not prove sustained throughput, crash during close, automatic
recovery or Kubernetes behavior.

For a Sandbox record explicitly marked durable, the Node binding records a
private mount intent before Bind, the original Branch snapshot before Attach,
and the exact Ready receipt before reporting success. It synchronizes the
Workspace record and the completed intent relative to the original held
Sandbox directory. An incomplete or malformed intent blocks Sandbox reopening
and operations admitted through a cached Sandbox before Docker inspection or
daemon connection; an observer or publication failure retains its original
result and directory in the running process. Project inventory can still inspect
the container separately and is not evidence that the intent completed.
Checks wait for the Sandbox discovery guard, so another active mount can finish
before its intent is classified. Reopening never replays the old request. Completed records
are checked against the Sandbox, Workspace, Branch and receipt identities
before another mount is admitted. This does not recover an old daemon or prove
its physical consumers have exited. The native Node tests use an authenticated
simulated daemon and a real local Store: they cover distinct successful mounts,
lost Ready, failed intent sync, directory replacement, two concurrent mounts
and process reopen without access to the Docker socket. The
separate Docker/FUSE proof above exercises actual SDK-created mounts.

The Linux Node binding also synchronizes a close intent before sending a durable
Workspace's ordinary Unmount. It records the correlated terminal reply before
removing and synchronizing the original Workspace record, then records the
completed close. A lost reply, failed sync or inconsistent record blocks later
operations and is not replayed on reopen. A Dirty reply or the daemon's exact
reversible `unmount:kernel` Busy refusal is recorded as no effect and permits
a new, explicit request; other Busy phases do not establish that outcome.
Forced unmount is refused because it does not use the durable catalog's close
protocol. The native tests inject lost replies and sync errors against an
authenticated simulated daemon. In a separate real Docker/FUSE test, a
test-only shim stopped the SDK immediately after the daemon's Unmounted reply
and before syncing its replacement record; the SDK process was then killed.
An independent read-only verifier decoded the original intent and matching
unsynced reply, while another fresh Node process without Docker access refused
to replay the close. The retained fixture, ownership and limits are recorded in
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/sdk-durable-close-kill-20261110-khby5hm3/receipt.txt`.
This establishes that same-host process-kill boundary, not survival of
unsynced data after power loss or automatic Sandbox recovery. The retained
close records currently have no compaction protocol.

Automatic directory creation uses the fixed `layerfs-durable/<Sandbox-id>`
layout. It creates the identity directory exclusively under retained parent
descriptors and synchronizes the volume, parent and private children before
initializing the catalog. If the identity already exists, the daemon refuses
without adopting it. A failure retains the original phase and acquired owners;
activation rechecks the original directory chain and keeps both errors if the
chain changes during an already attempted activation. The caller must persist
its creation intent before starting the daemon.

The directory contains an authority-bound owner file, immutable Workspace and
lifecycle event files, a fixed-width hash-chained ledger and a durable ledger
head. Appending writes and synchronizes the event, appends and synchronizes the
ledger entry, writes and synchronizes a replacement head, renames it into place,
then synchronizes the checked directory before acknowledging success. The
original directory, owner lock, journal and attempted files remain in custody
when completion is uncertain. A caller must not automatically retry an unknown
write.

Reopening validates the original authority, file and directory identities,
the complete ledger chain and head, and a one-to-one match between ledger
entries and record files. Missing files, extra records, a partial ledger, an
unresolved replacement head, corruption or identity mismatch refuse discovery;
they are not silently skipped or deleted. An interrupted write may leave a
complete record or an incomplete catalog: either outcome needs reconciliation,
not an automatic replay. A consistent rollback or replacement of the entire
directory with an older copy cannot be detected without independent external
identity and monotonic-state evidence.

`Roster::visit` streams tentative records without imposing a Workspace count
cap. Its callback must only observe; a failed scan is not a complete catalog.
`Roster::lookup` returns a record only after a full successful scan, and a
manually assembled daemon may pass the catalog to `Service::register_recorded`
to require the bound Workspace and Branch to match it. This check does not
itself authenticate the Sandbox or physical Store. It rejects a different
history catalog identity or incarnation.
`Service::recover_catalog` provides one explicitly invoked recovery attempt:
it verifies the complete catalog, authority, available capacity and empty owner
roster before opening any Overlay. It then opens records in order with fresh
namespaces, validates their saved snapshots against the Store and registers the
bindings without mounting them. If an entry fails, it returns the original
failure, completed tokens and any unadmitted owner; the caller must retain the
same Service and Owner, reconcile the result and not automatically replay it.
An incomplete creation or missing file is an error, not permission to skip an
entry or create replacement data. The caller
must keep admissions serialized and independently prove the old mounts and
consumers stopped before invoking this operation.
Even after a successful scan the caller must separately authorize each
Workspace and Branch, prove the Store and original file identities, fence old
processes and mounts, and reconcile unknown operations before exposing data.
Before attempting a terminal close, the manually assembled daemon can use
`Service::unmount_recorded`: it checks the catalog and current bound token,
reserves the Workspace control entry, persists `Roster::begin_close(token)`,
performs one unmount and records its actual terminal reply. When durable control
is active, its ordinary control-wire Unmount uses this close path; ForceUnmount
is not yet implemented for these bindings. A Dirty refusal occurs before an intent is
recorded. Only one close may be pending in a catalog at a time. While one is pending, normal discovery,
recovery and new intents refuse without performing those operations.
`Roster::pending_close` exposes the exact retained Workspace, Branch and token;
it does not establish whether the original unmount succeeded. After verifying
the provenance of that exact unmount's `Reply::Unmounted`, the caller may pass
the reply to `Roster::finish_close`. Status, a disconnected process or a reply
for another namespace is not a receipt. An interrupted or unknown close must
be reconciled with independent evidence, never automatically retried or
declared complete. A kernel Busy refusal proves that this attempt made no
terminal change. The Service records that exact attempt's cancellation before
making the Workspace available again; a subsequent unmount requires a new
explicit request. If cancellation synchronization fails, the original Busy
receipt and binding remain in custody. No other failure is treated as proof of
no effect. An explicitly configured fresh daemon uses catalog-backed bindings
and this close path; it still does not reopen existing catalogs automatically.
The caller must authenticate the Sandbox and physical Store, serialize access
to the catalog, and retain the original Service and failure on an uncertain
attempt. A terminal reply that cannot be recorded is returned with the exact
original `Success`, including its kernel receipt.

A completed close removes the Workspace from the active discovery set while
keeping the original creation, intent and terminal event files. The same
Workspace identity cannot be reused. Cancelled attempts remain in the event
chain and every later attempt has its own sequence. The chain validates their
ordering and exact namespace. Create-only catalogs and v1 lifecycle events
remain readable; older binaries reject the new event tags and must not open a
catalog after lifecycle events they do not support have been added. The
catalog has no automatic cleanup or migration.

On 2026-11-09 an owned QEMU/KVM guest wrote two records to a raw ext4 disk
(`cache=none`) and acknowledged them. The harness killed that guest, booted
another against the same disk, and verified both records and refusal of a
duplicate identity. The receipt and serial logs are retained under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-roster-vm-20261109-ledger-verified/`;
the probe SHA-256 was
`5911e20792734aee40dcdc8f4930d6415fe26abca149117b2b74d1ce7e72a414`.
This simulates guest memory loss, not physical host power loss or mount recovery.

An isolated FUSE test recorded two Workspace/Branch identities before creating
their overlays, mounted both, wrote different bytes as an unprivileged user and
synchronized them. It killed and reaped the original daemon, detached its old
mounts, reopened the catalog against the same Store catalog identity and
incarnation, invoked one catalog recovery to discover the overlay filenames,
select fresh namespaces and register both bindings, then explicitly remounted
them. The old tokens were rejected and the separate bytes survived. The test-only
container used `/dev/fuse`, CAP_SYS_ADMIN and unconfined AppArmor and seccomp,
without network or a Docker socket. The runner output,
exit status, image and source hashes are under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-catalog-batch-final-20261109/`.
This is a manually fenced, single-host process-loss test, not automatic daemon
startup, container/Pod reconstruction, physical host power loss or scale qualification.

A separate control-only test recovered 64 distinct durable Overlay bindings
from one catalog, checked their identities and namespaces, then closed them.
It used a Disposable test Store and no FUSE mounts; it is a functional capacity
check, not a crash, throughput or latency qualification.

A later owned VM probe wrote one active record, one completed close and one
pending close, using synthetic close replies, then lost guest power after the
catalog acknowledgments. On the next boot it found the pending exact token,
refused discovery, and verified the active set after a synthetic reconciliation.
The result is under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-roster-lifecycle-vm-20261109/`;
the probe SHA-256 was
`2bbd1e7dbaf91a27c5f7d31360e3ce57284dbdcb0e92099b8239cdfa2f79df5a`.
This tests catalog persistence, not a real unmount or physical power loss.

The isolated two-Workspace FUSE test was extended to record intents around
the actual successful unmount replies, then reopen the catalog and verify that
neither closed Workspace is active. Its successful runner, hashes and container
exit are under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-roster-lifecycle-fuse-v2-20261109/`.
The first test setup failed because its bind-mounted proof directory did not
allow the test's unprivileged user to traverse it; that failed run is retained
separately in `durable-roster-lifecycle-fuse-20261109`.

The catalog-aware Service close was tested with directory-sync failures both
before unmount and after the terminal reply; it retained the original binding
or successful reply respectively. A separate isolated FUSE run retained a Busy
close intent across process loss, refused replay and recovery, and also closed
two recovered mounts through the new Service method. Its test output, hashes
and container exit are under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-recorded-close-fuse-20261109/`.
This is not automatic daemon startup or an end-to-end AutoTask integration.

The v2 attempt format was checked against a catalog created by the previous
implementation. A separate owned VM probe verified completed and pending v2
records after guest power loss; its evidence is under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-roster-attempt-v2-vm-20261109/`.
An isolated FUSE run verified two Busy refusals, distinct durable cancellation
records, an explicit later unmount, and injected cancellation-sync failure. Its
runner and test results are under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-close-cancellation-fuse-v2-20261109/`.
The first run retained its failed evidence separately: the test killed its FUSE
owner while it still held a file descriptor, leaving a blocked kernel flush;
the owned test connection was aborted and the fixture was corrected.

The two-Workspace FUSE process-loss test was then switched to create both
bindings through `Service::mount_recorded`. After independent writes and
synchronization it killed the original process, fenced the old mounts, reopened
the same Store and catalog, and recovered and closed both. Separate tests
covered both capacity limits, duplicate identities, an existing Overlay path
and a failed discovery directory sync. The FUSE test output and hashes are in
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-recorded-mount-fuse-20261109/`.
This remains a manually assembled and manually fenced test, not automatic
container/Pod reconstruction or physical host power-loss evidence.

An authenticated local TCP test exercised the unchanged control wire for
creation, capacity refusal and terminal close, and injected a directory-sync
failure to verify an Unknown reply with its original custody retained. A
separate isolated FUSE test used the same request dispatch for two Workspaces,
then killed and manually fenced the original owner before recovery; its output
is in
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-control-pinned-root-fuse-20261109/`.
Additional injected directory replacement during creation covered both a
completed binding and a creation refusal: each kept the original outcome and
the replaced-root failure, neither wrote into the replacement directory, and
neither allowed a later Attach.
