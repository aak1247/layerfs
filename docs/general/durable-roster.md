# Durable Workspace discovery evidence

`layerfs_daemon::durable_roster` provides a Linux, append-only catalog for one
Sandbox. It records an authorized Workspace identity and its original Branch
before the caller creates that Workspace's durable Overlay or exposes a token.
It does not create an Overlay, start a daemon or recover a mount. The normal
daemon application does not yet use this catalog.

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
Before attempting a terminal close the caller may persist
`Roster::begin_close(token)`. The caller must serialize control admission and
verify that this is the current bound Workspace and namespace. Only one close
may be pending in a catalog at a time. While one is pending, normal discovery,
recovery and new intents refuse without performing those operations.
`Roster::pending_close` exposes the exact retained Workspace, Branch and token;
it does not establish whether the original unmount succeeded. After verifying
the provenance of that exact unmount's `Reply::Unmounted`, the caller may pass
the reply to `Roster::finish_close`. Status, a disconnected process or a reply
for another namespace is not a receipt. An interrupted or unknown close must
be reconciled with independent evidence, never automatically retried or
declared complete. This is a low-level catalog protocol; the normal daemon's
unmount path does not yet perform it or provide the required admission fencing.

A completed close removes the Workspace from the active discovery set while
keeping the original creation, intent and terminal event files. The same
Workspace identity cannot be reused. The event chain validates both ordering
and the exact namespace. Create-only catalogs remain readable; older binaries
reject the new event tags and must not open a catalog after lifecycle events
have been added. The catalog has no automatic cleanup or migration.

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
