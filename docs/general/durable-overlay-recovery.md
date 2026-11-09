# Durable overlay database boundary

The Overlay crate exposes `Overlay::create_durable(path, config, identity)`
and `Overlay::recover_durable(path, config, identity)` on Linux. This is a
single-Workspace database primitive. The daemon can also initialize a fresh
durable catalog when explicitly configured with new private directories and a
Durable Store. Existing SDK Sandbox creation still selects disposable overlays;
neither path automatically recovers an old mount.

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
select the recovered connection. New schema 41 files also persist a
per-Workspace high-water mark before an owner or recovered token is returned.
Recovery requires a strictly greater namespace and records it durably before
exposing the new owner; gaps are allowed and a failed or unknown attempt must
not be retried with the same value. This protects the external Workspace token
as well as the internal route while the original database is retained. It does
not detect database rollback, replacement or reuse of a Workspace identity with
a different file. The caller must prevent those cases, avoid namespace collisions
among concurrent mounts and fence the old process, mount and consumers. Stopping
the owner retains the files. These APIs do not persist a roster, authenticate
the caller, fence old FUSE mounts, reconcile Commit outcomes or automatically
recover an SDK control token. They do not yet replace the normal disposable
roster used for mounted Workspaces.

`Store::bind_durable` attaches such an owner to its exact Branch snapshot. It
checks the Workspace identity, the original Layer and Commit roots against the
Store, and the overlay's recorded base against that snapshot. It rejects a
second logical binding of the same owner. A Branch that has advanced does not
replace the original view: a later explicit Commit uses the existing merge
path. The caller must keep the owner private during binding and authenticate
its Workspace, Store and requested Branch; the snapshot is not authorization.

New durable overlay files use schema 41 and persist their encoded snapshot.
For a known published Commit, its new snapshot and the overlay's installed
base advance in the same SQLite transaction. An install without a known
receipt marks the binding unresolved in that transaction; recovery refuses it
until reconciled. A subsequent known publication in the live owner can replace
that marker. A crash with a pending capture still refuses automatic recovery.
Existing schema 39 and 40 durable files are not converted; the caller must
independently prove a fresh namespace for either format. Schema 39 also
requires the caller's independently retained snapshot. Old inode reservation
windows are not reused; future allocations require the same durable Store
authority and scope.
`Owner::recover_recorded` reads the snapshot and root from schema 40 or 41 without
requiring an external root, but still requires the independently known file
path and Workspace identity. The resulting snapshot must pass Store binding
and the caller's authorization and old-owner fencing checks.
`Owner::recover_recorded_next` also selects a namespace greater than the
schema-41 high-water mark while holding the original file lease. Its caller
supplies a minimum and must serialize it with all other live Workspace routes;
legacy schemas lack this evidence and are refused by that method.

The separate [durable discovery catalog](durable-roster.md) can retain the
identities and original Branches of multiple Workspaces. It is not yet wired
by the daemon when a fresh durable Workspace setup is explicitly supplied.
It is not used to automatically reopen an existing Sandbox.

An explicitly assembled daemon `Service` can register already bound durable
owners and use its ordinary control and FUSE Attach path. The caller may first
transfer each owner into an in-memory bounded `Owner` roster with
`adopt_durable`; this shares the configured capacity with disposable owners.
Within one roster lifetime, adopted namespaces must increase and cannot collide
with earlier owners. A refused adoption returns the original owner; a refused
registration returns the original binding. Registration verifies the exact
roster owner, durable Workspace identity and Store. The caller selects the
namespaces and fences the prior process, mounts and consumers; schema 41 also
rejects reuse for the same Workspace. The roster itself is not persisted and
does not discover files or automatically restart a Sandbox.
Terminal unmount closes the database but retains its files. When the full
barrier is unavailable, a durable mount rejects FUSE FSYNC and FSYNCDIR with
EOPNOTSUPP; it does not use ENOSYS, which Linux would cache as successful
synchronization. A durable owner backed by a concrete Durable Store on a
supported local filesystem and in a private directory serves FSYNC and
FSYNCDIR through the Store and Overlay barriers. The Store's initial installed
file must already have been synchronized; durable installation does that
before acknowledging it. FLUSH and disposable mounts keep their prior behavior.

The Overlay provides a separate `synchronize_kernel` primitive: it validates
the live mount and the exact file or directory handle, then synchronizes the
durable overlay file and its original checked directory. A file or directory
sync failure quarantines the owner and retains its connection and lease; a
disposable database or a nested atomic job cannot claim this barrier. Tests
inject failures into each sync and exercise stale and mismatched handles. This
primitive alone does not establish that the Store or its base is durable. The
FUSE request checks the concrete Store separately, then applies the recorded
recovery check and this barrier under the exact Workspace owner. Unresolved
local custody returns EAGAIN without attempting the Overlay sync; failed
sync returns EIO and retains its original failure. A stop may cancel waiting
for the Store or Workspace owner before an attempt, while an attempted sync
retains its original result.
The service-owner variant also requires a recorded root-matched binding and
applies reopening's capture, volatile-custody, operation-record and cleanup
refusals. Its checks use the existing generation and cleanup indexes; they do
not repeat `quick_check` or authorize recovery of an old mount or unknown
external action.

The durable profile is SQLite DELETE/EXTRA/EXCLUSIVE. SQLite documents that
[FULL alone is not necessarily durable in DELETE mode](https://sqlite.org/pragma.html#pragma_synchronous).
In this pinned SQLite build, exclusive locking can reuse the journal rather
than delete it; its first journal-directory sync may ignore an error. The fork
therefore retains the checked directory handle and explicitly synchronizes it
after each successful SQLite commit, before acknowledging that transaction.
Failure or identity replacement quarantines the owner and reports an unknown
result. This adds a directory sync to each durable writing transaction; its
throughput and latency have not been qualified. Earlier fork revisions used FULL without this per-commit check;
reopening selects EXTRA for future transactions and cannot restore a write
already lost under the former profile. Each changed Workspace
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
Test-only sync faults against the rollback journal and its directory verify
that the engine reports an uncertain result and
quarantines the connection, and that a fresh process sees the prior complete
transaction or an unresolved capture with its matching prior snapshot. The VM
harness can exercise the overlay with `--probe-kind
overlay`; this proves only the database path on its guest filesystem, not FUSE
`fsync`, Sandbox recovery, Kubernetes integration or physical host power loss.

On 2026-11-09 the earlier FULL-profile overlay VM probe passed on QEMU 6.2.0/KVM with a Linux
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

The later FULL-profile schema-40 probe on the same guest and disk profile installed a new
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

A further FULL-profile probe killed the writer with the receipted generation retirement
still pending, then reopened the same disk, verified the new bytes and
snapshot, completed bounded retirement and checked the bytes again. Its
receipt is retained under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-overlay-vm-20261109-pending-retire/`;
the probe SHA-256 was
`d58c9ae5740e5cbce3ae827f5ebcb3d9765110d3076c95388c73e25a9f02420f`
and `durable.rs` was
`ac626d9823857db21b4e1622b6a4c7ef1dbddd1d436573903796c2c3bdaadea7`.

The EXTRA-profile and checked-directory implementation was exercised by the
same pending-retirement VM probe. Its receipt is retained under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-overlay-vm-20261109-extra-directory/`;
the probe SHA-256 was
`326cfef51a8298268ac887d60591082c7b33394b956630aa026ab8dbf0056ead`.
At that run, `profile.rs`, `durable.rs`, and `connection.rs` respectively had
SHA-256 `0e0775c1c06c4b1f1262c02d353e012a872f57096c1386c13ff3374f423f52b2`,
`d91d8f0c0cfbbf5bad2192227e678d54cc37eb7c62ca1a11ef625f7f33ac9538`, and
`120b9bb6b14cd28ab315595a7bbabf97a1248559157b485145eaadb5b4002cb0`.

The schema-41 probe advanced the namespace before acknowledging a write,
killed the guest, then reopened the same disk and rejected the previous value.
It also checked the recorded snapshot and file bytes. Its receipt and serial
logs are retained under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-overlay-vm-20261109-namespace-v41/`;
the probe SHA-256 was
`7b1e4cee612940aa39ae1e25a35466e389d6135bfcb88830ea3ae483ff9f25a2`.
This is guest memory loss on the tested ext4 disk, not physical host power loss
or recovery of a FUSE mount.

An isolated Docker test on 2026-11-09 used a manually assembled daemon
Service, one durable owner and a disposable test Store. An unprivileged
command wrote through a real FUSE mount; the test killed and reaped the daemon
process, detached its old mount, recovered the recorded binding under a new
namespace and remounted it. An unprivileged command read the original bytes and
appended new bytes. The old control token was rejected. This is a process-loss
and manual remount test, not container recreation, a durable global Store,
physical power loss, Kubernetes or an `fsync` guarantee. The test runner,
source and image hashes, kernel version, output and exit status are retained
under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-fuse-process-loss-extra-20261109/`.

The earlier isolated FUSE run checked both profiles before the request barrier
was connected: on a durable mount,
file `fsync` and directory `fsync` returned EOPNOTSUPP before and after the
manual recovery path; a disposable mount retained its prior successful
ENOSYS behavior. The test output, executable and source hashes, image and
kernel facts are retained under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-fuse-sync-refusal-20261109-final/`.

A later isolated FUSE run exercised the connected barriers with a synchronized
Durable Store and durable Workspace. An unprivileged process successfully
called file `fsync`, `fdatasync` and directory `fsync`; an unresolved test
capture returned EAGAIN until its known local resolution. Injected directory
sync errors in the Store and Overlay each returned EIO. In each case the
test killed and reaped the original daemon, detached its old mount, reopened
the original files under a fresh namespace and read the written bytes. A
Disposable Store still made the durable mount refuse synchronization. The
test-only container used `/dev/fuse`, CAP_SYS_ADMIN and unconfined AppArmor and
seccomp, with no network or Docker socket. This is a manual, single-Workspace
process-loss test, not container/Pod recreation, automatic mount recovery,
physical power loss or a performance qualification. The output, exit status,
executable/source hashes, test-only fault shim, image and kernel facts are under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-fuse-sync-verified-20261109/`.

The schema-41 FUSE rerun also explicitly attempted recovery with the old
namespace before remounting under a newer one; all five container test cases
passed. Docker reported the test container's exit code as 0; automatic removal
was still in progress after the client wait timed out, and a later inspection
found no such container. The runner
output, executable/source hashes and observed Docker state are retained under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-fuse-sync-namespace41-20261109/`.

A separate isolated FUSE test manually adopted two durable owners into one
daemon and mounted both against one Durable Store. An unprivileged process
wrote different bytes to each and synchronized them. After killing and reaping
the daemon, the test detached both old mounts, reopened each original file
under a newer namespace, rejected both old tokens and verified the distinct
bytes as an unprivileged user. The test then synchronized and explicitly
unmounted both recovered views. It used a test-only container with `/dev/fuse`,
CAP_SYS_ADMIN, unconfined AppArmor and seccomp, and no network or Docker socket.
This tests two manually assembled Workspaces after process loss, not automatic
discovery, Pod recovery, physical power loss or high-capacity performance. The
output, exit status and image/kernel/source hashes are retained under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-multi-fuse-20261109/`.

On 2026-11-09 a separate isolated FUSE test ran the real daemon executable
with a version-6 fresh durable setup. It verified a Disposable Store refusal
before installation, retained startup after an already acknowledged Durable
Store install with a pre-existing Overlay file, and an ordinary unprivileged
FUSE write followed by file and directory sync. It then killed and reaped the
exact daemon, detached its old mount, verified fresh startup refused the old
data, and explicitly recovered and remounted the original Workspace under a
newer namespace. The recovered bytes matched. All three tests passed in the
test-only container with `/dev/fuse`, CAP_SYS_ADMIN, unconfined AppArmor and
seccomp, no network and no Docker socket. The kernel was `5.15.0-198-generic`;
the image SHA-256 was
`24292ac486b1237366c849a4db2b0aaaa2ce7b09b64a834b0f3c902908185f5b`.
The test and daemon executable SHA-256 values were respectively
`7f4997f6e35d18040b22c57ed2c2431e50f4e20086cf302233359c58db29427d`
and `077b00b042603f96749dacca17054ef25f661e28063db48edcac054c3c33d459`;
the test source SHA-256 was
`b4aa1607830474343f481002f6a40e353882511109cf217513b354ea0b2334c1`.
The output and binaries remain under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/fresh-daemon-v2-20261109-v7sNIr7k/`.
This does not prove automatic daemon restart, container or Pod recreation,
node failure, physical power loss, or AutoTask integration.

On 2026-11-10 the real daemon was additionally tested with version-7 setup:
it created its private per-Sandbox directories on the Store volume, bound and
closed a Workspace, and refused a second creation with the same identity while
preserving the first catalog. The previous process-loss and refusal cases also
passed: four tests, exit 0, in an isolated test-only FUSE container with the
same image and kernel scope above. The test executable SHA-256 was
`e9fa7fd5a91943543c1896e8d8289addc64dc0e764eaf4e22ff407adb117799f`, the
daemon was `2ea9462249dc05aef1241f5afa0d057b7d502c685dbdf557cfe664d6bd7b06f3`,
and the test source was
`14ff4226f2277931a1cdc52ee7ca2a381da70bb15df86fbc443b04f09dfa5a06`.
The output and binaries are retained under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/fresh-volume-daemon-20261110-k32dPXv8/`.
This is still not automatic recovery or AutoTask integration.
