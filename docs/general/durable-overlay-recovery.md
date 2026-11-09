# Durable overlay database boundary

The Overlay crate exposes `Overlay::create_durable(path, config, identity)`
and `Overlay::recover_durable(path, config, identity)` on Linux. This is a
single-Workspace database primitive. The current daemon, SDK and mounted FUSE
Workspaces still use the disposable overlay; these methods do not start a
Sandbox or recover a mount.

The caller supplies a stable, nonzero 32-byte Workspace identity and a private
directory owned by the calling user. The directory must be on ext4, XFS or
Btrfs, and all access to one database must be on the same kernel. A separate
owner file holds that identity and an exclusive nonblocking OS lock. Neither
file is automatically deleted or converted. Opening with another identity or
while the owner lease is held fails. The lease stays held until both SQLite
and the engine's main-file descriptor have closed; a failed checked close
retains it. File replacement or loss of directory privacy quarantines a live
engine instead of allowing it to resume.

The durable profile is SQLite DELETE/FULL/EXCLUSIVE. Each changed Workspace
generation, base root, revision, dirty count and volatile-custody flag is
written in the **same SQLite transaction** as the associated filesystem rows.
The parent directory and the initialized database are synchronized before
creation returns. A storage sync error on a transaction quarantines its owner
and reports an uncertain result; it is never automatically replayed. Explicit
close, ordinary drop and unwinding retain the uncertain SQLite connection,
open-state custody and owner lease until process exit, so SQLite is not asked
to roll it back implicitly.
An explicit
`max_pages` is refused for this profile because the rollback-journal bytes are
not yet included in the existing physical allocation observations.

Reopen checks the owner and database identities, SQLite profile, schema and
`quick_check`. Process-local base-reader windows are not recreated. Reopen
conservatively refuses a closed Workspace, an unresolved capture or fold, an
orphan, volatile operation ownership, operation records or cleanup other than
ready stale-cell/step/piece cleanup; all files remain in place. The safe
shrink-cleanup rows remain available to ordinary bounded maintenance. The caller
must separately prove the old mount and consumers have stopped, validate the
base root against its authorized Store, and reconcile any unknown external operation before exposing
the returned state. A released file lock alone does not establish those facts.

The scoped tests cover same- and cross-process ownership, identity mismatch,
process termination after acknowledged writes, further writes after reopening,
and preservation of unresolved states. A test-only sync fault against the
rollback journal verifies that the engine reports an uncertain result and
quarantines the connection, and that a fresh process sees the prior complete
transaction. The VM harness can exercise the overlay with `--probe-kind
overlay`; this proves only the database path on its guest filesystem, not FUSE
`fsync`, mount recovery, Docker/Kubernetes integration or physical host power
loss.

On 2026-11-09 the overlay VM probe passed on QEMU 6.2.0/KVM with a Linux
5.15.0-202 guest and an ext4 raw disk (`cache=none`): the first guest reported
an acknowledged 86,016-byte overlay and was killed, and the second guest
reopened that disk and checked the written file bytes. The receipt and serial
logs are retained under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-overlay-vm-20261109-retained-final/`.
The probe binary SHA-256 was
`35842eff1e9d7fe178a9864dcbea9ec8ce0ba78036e83f6f0f2bd6ab5aca0652`;
the overlay durable implementation SHA-256 was
`f6a5c6563d0837982c1a2930528d05ebdaae97c30635e3f3edcd57cb909b8318`,
and its connection-owner SHA-256 was
`43474bdfda377ffef4125c47e27097dfd729ff0047b57aeaff7b8a1818d06cd3`.
