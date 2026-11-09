# Durable Workspace discovery evidence

`layerfs_daemon::durable_roster` provides a Linux, append-only catalog for one
Sandbox. It records an authorized Workspace identity and its original Branch
before the caller creates that Workspace's durable Overlay or exposes a token.
The catalog itself does not start a daemon or recover a mount. The daemon can
initialize one for a fresh Sandbox when explicitly configured; its SDK does not
yet supply that configuration.

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

The Linux daemon's version-6 private setup can select this fresh path with a
nonzero Sandbox identity and the two pre-created private directories. The
caller supplies a fresh identity and authenticates the physical Store. A
Disposable installation is refused before Store creation; failure after an
installed Store retains the original service and reports the Retained phase.
Version-5 setup remains readable and keeps its disposable behavior. The Docker
SDK checks that both requested directories are on the named Store volume; it
does not yet create them or prove an old container and its consumers exited.
This fresh path refuses existing contents and is not recovery.

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
