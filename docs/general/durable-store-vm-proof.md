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
