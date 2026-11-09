# Durable Store guest power-loss proof

`tools/durable_vm_power_loss.py` starts an owned Linux virtual machine with a
new ext4 disk. Its guest runs the actual `layerfs-persistence` Durable Store:
it creates a Store, completes a save and reports the still-open WAL size. The
host kills QEMU immediately after that acknowledgement, starts a fresh guest
on the same disk and checks the saved object's canonical bytes through the
Store API. Both guest logs, the disk and a receipt remain in the requested new
output directory.

The proof requires Linux with KVM access, a readable compatible Linux kernel,
a static BusyBox, QEMU, `mkfs.ext4`, `cpio` and the host libraries needed by
the built test executable. The guest kernel must support devtmpfs, ext4 and
the QEMU IDE disk. Build the probe first, obtaining its executable path from
Cargo's compiler-artifact output:

```sh
cslim-dev cargo +1.85.1 test --locked -p layerfs-persistence \
  --test durable_vm_recovery --no-run --message-format=json
timeout 120s python3 -B tools/durable_vm_power_loss.py \
  --output /path/to/new-owned-proof-directory \
  --kernel /path/to/readable/vmlinuz \
  --probe /path/to/durable_vm_recovery-test-executable
```

The harness refuses an existing output directory, runs without a guest network
and only kills its own QEMU processes. `cache=none` is used for the raw disk.
The acknowledgement precedes the kill, and the initial Store remains open;
the saved WAL is not closed or checkpointed by the guest before that kill.
This models guest RAM loss and tests SQLite's recovery on the same host-backed
disk. It does not simulate loss of the host's storage cache, a drive that lies
about flush completion, or a physical power failure.

This tests a completed global Store save. The Workspace overlay is a separate
disposable database, and this proof does **not** demonstrate its durability,
uncommitted file recovery, commit-outcome reconciliation, or mount recovery.

## Checked WAL directory synchronization

On Linux, the Durable writer retains an open handle to its original Store
directory and checks that directory and the database path still have their
original identities. Each successful SQLite write transaction is followed by a
checked directory `fsync` before its result is acknowledged. This also covers
the newly created WAL directory entry: the pinned SQLite Unix VFS can ignore a
directory-sync error. A checked seal synchronizes the directory again after
SQLite closes. Disposable writers keep their previous behavior.

If a write's directory sync or identity check fails, the result is unknown;
the original connection, error and cooperative writer lock remain held until
process exit, including through a publication lane and ordinary handle drops.
After a failed post-close seal, its writer lock is retained. Other cooperative
writers may wait for that owner to exit; no unknown operation is replayed.
This requires a trusted Store directory and cooperative access on the same
kernel; raw SQLite clients can bypass the cooperative lock. It does not make
an earlier write, acknowledged before this check existed, retroactively durable.
The per-transaction directory-sync cost has not been benchmarked.

A concrete Linux Durable writer also offers a checked Store barrier for a
separately synchronized Workspace. Its initial Store installation must already
have synchronized the file and directory. A caller may cancel while waiting
for the FIFO publication lane or another process's directory lock; after the
sync starts, the caller gets its original success or uncertainty. Tests cover
both waits and an injected failure whose stop signal arrives during the sync.
The single-owner FUSE service combines this with its recorded Overlay barrier;
automatic Workspace discovery and recovery remain separate prerequisites.

Injected `EIO` tests cover ordinary writes, writes inside a publication lane,
and the post-close seal. They verify no success acknowledgement, retained
writer ownership after handle drops, and reopening after the owner process exits.
Separate tests move the database and its parent directory and verify that the
original directory lock and file are retained rather than adopting the new
path. These are error-injection and process-loss tests, not proof against a
storage device that falsely reports synchronization.

## Recorded run

On 2026-11-09, the probe built over parent `5bfed282c7239143da3073e4d8ef861c7033a950`
passed on QEMU 6.2.0 with KVM, a Linux 5.15.0-202 guest kernel and an ext4 raw
disk. The first guest acknowledged a 197,792-byte WAL before SIGKILL; the
second guest reopened the original disk and verified the saved bytes.

- Guest test source SHA-256: `fb5d0e2008ad03579689b8c3fe970b0effe3dce802d7336b8aa0c42d9c9aa7d9`.
- Harness SHA-256: `4e0926937ed8e6cb2cd154286da87606686e76f66c6f55fcaae4afc65c0afa0f`.
- Probe executable SHA-256: `611f2fe3dbb07b015a89fd871911153a6364ff47dae1a159f034a2f80bbc9c8f`.
- Guest kernel SHA-256: `d820688f039cb28deea210c1c75eb377437ba832ddd415460b79b88d4bb02761`.
- The local receipt and logs are retained under
  `/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-vm-20261109/proof-final/`.

The checked-directory implementation was also run with the same VM and disk
profile. Its acknowledged live WAL and recovered object were verified after
guest termination. The receipt and logs are under
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/durable-store-vm-20261109-directory-sync/`;
the test executable SHA-256 was
`bcdab5b4a97b3e74d2c82d96ea20f7e96724a3ff348b7e92ddc9bfdb061c830c`.
