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

`layerfs-store-service` also exposes `Owner::create_durable` and
`Owner::recover_durable` for one database behind its ordinary Workspace service
lock. The caller supplies the stable identity, the expected base root and a
daemon-local namespace. Recovery refuses a different stored root, and makes a
fresh random internal route incarnation so a route from the old owner cannot
select the recovered connection. Stopping the owner retains the files. These
APIs do not persist a roster, authenticate the caller, fence old FUSE mounts,
reconcile Commit outcomes or recover an SDK control token. They do not yet
replace the disposable owner used for a mounted Workspace.

`Store::bind_durable` attaches such an owner to its exact Branch snapshot. It
checks the Workspace identity, the original Layer and Commit roots against the
Store, and the overlay's recorded base against that snapshot. It rejects a
second logical binding of the same owner. A Branch that has advanced does not
replace the original view: a later explicit Commit uses the existing merge
path. The caller must keep the owner private during binding and authenticate
its Workspace, Store and requested Branch; the snapshot is not authorization.

New durable overlay files use schema 40 and persist their encoded snapshot.
For a known published Commit, its new snapshot and the overlay's installed
base advance in the same SQLite transaction. An install without a known
receipt marks the binding unresolved in that transaction; recovery refuses it
until reconciled. A subsequent known publication in the live owner can replace
that marker. A crash with a pending capture still refuses automatic recovery.
Existing schema 39 durable files are not converted and continue to require the
caller's independently retained snapshot. Old inode reservation windows are
not reused; future allocations require the same durable Store authority and
scope.
`Owner::recover_recorded` reads the snapshot and root from schema 40 without
requiring an external root, but still requires the independently known file
path and Workspace identity. The resulting snapshot must pass Store binding
and the caller's authorization and old-owner fencing checks.

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
ready stale-cell/step/piece cleanup and ready retirement of an installed
generation with a matching durable receipt; all files remain in place. These
qualified cleanup rows remain available to ordinary bounded maintenance. The caller
must separately prove the old mount and consumers have stopped, validate the
base root against its authorized Store, and reconcile any unknown external operation before exposing
the returned state. A released file lock alone does not establish those facts.

The scoped tests cover same- and cross-process ownership, identity mismatch,
process termination after acknowledged writes, further writes after reopening,
and preservation of unresolved states. Service-owner tests also cover stale
internal routes, base-root mismatch, process loss after a service write and a
panicked service call retaining its original connection. A Store binding test
checks a moved Branch, identity/root refusals, an explicit merge and recovery
of its installed snapshot against a live Store; it is not a mount crash proof.
Test-only sync faults against the
rollback journal verify that the engine reports an uncertain result and
quarantines the connection, and that a fresh process sees the prior complete
transaction or an unresolved capture with its matching prior snapshot. The VM
harness can exercise the overlay with `--probe-kind
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

The later schema-40 probe on the same guest and disk profile installed a new
base and snapshot together, acknowledged an additional file write, killed the
guest and recovered all three from the disk. Its receipt and serial logs are
retained under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-overlay-vm-20261109-binding-v40/`;
the probe SHA-256 was
`c2c7448ef266f438fc0328c1928232b19cb1cc2ffc8fcfc8aedf5954451c4b49`.
At that run, `durable.rs`, `durable_binding.rs`, `generation.rs` and the probe
source respectively had SHA-256
`0074fce8597b377385105d484c8432d39a21464bd1201cde77be91dd91349d74`,
`c29c7c9bf6ccae38ea5b8cbdfd9d4bfca017c102b9ae12bc74542d6505c14b40`,
`4886c9da03046e9a6dd590955f8c5509e0a60035e482b7055d55674b4f574bbd`, and
`cf9121d29e851672fc35416e97be97c0d76cf319d2cca95057063d926d2f859b`.

A further probe killed the writer with the receipted generation retirement
still pending, then reopened the same disk, verified the new bytes and
snapshot, completed bounded retirement and checked the bytes again. Its
receipt is retained under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-overlay-vm-20261109-pending-retire/`;
the probe SHA-256 was
`d58c9ae5740e5cbce3ae827f5ebcb3d9765110d3076c95388c73e25a9f02420f`
and `durable.rs` was
`ac626d9823857db21b4e1622b6a4c7ef1dbddd1d436573903796c2c3bdaadea7`.
