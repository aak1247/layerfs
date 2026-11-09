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

The directory contains an authority-bound owner file, a sequence of immutable
Workspace record files, a fixed-width hash-chained ledger and a durable ledger
head. Appending writes and synchronizes the record, appends and synchronizes the
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
Even after a successful scan the caller must separately authorize each
Workspace and Branch, prove the Store and original file identities, fence old
processes and mounts, and reconcile unknown operations before exposing data.
The catalog has no automatic cleanup, terminal-state transition or migration.

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
incarnation, discovered the two overlay filenames from its records and manually
rebound and remounted them. The old tokens were rejected and the separate bytes
survived. The test-only container used `/dev/fuse`, CAP_SYS_ADMIN and unconfined
AppArmor and seccomp, without network or a Docker socket. The runner output,
exit status, image and source hashes are under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-recorded-multi-fuse-final-20261109/`.
This is a manually fenced, single-host process-loss test, not automatic daemon
startup, container/Pod reconstruction, physical host power loss or scale qualification.
