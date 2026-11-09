# Host and restricted-container Workspace visibility

Status: one Linux integration experiment, 2026-11-10. This is not an SDK
deployment mode or an automatic Workspace recovery implementation.

An isolated test assembled a durable Store, roster and two real FUSE
Workspaces with the public `layerfs-daemon` service. It ran the service in a
dedicated Docker container, with its mount home bound to a host directory.
The host directory was a separate mount made private before being made
shared. The Store and control data were kept outside that exported directory.
The host's test filesystem was ext4.

The host observed exactly two FUSE mount rows. Its ordinary uid/gid 1000 read
the first Workspace and wrote and synced a file there. A separate, networkless
container ran as uid/gid 1000, with all capabilities dropped, `no-new-privileges`,
and a read-only root. It bound only the first Workspace, read and wrote it, did
not see the other Workspace's marker, and had no LayerFS Store, private daemon
directory or `/dev/fuse`. After this consumer had exited, the original service
read both writes and normally unmounted both Workspaces. The exact observed host
FUSE mount rows disappeared; the producer and consumer exited; the test's
shared bind was normally unmounted.

One earlier experiment made a bind shared while it was still in the host
root's existing propagation group. Each of two new FUSE mounts then appeared
twice in the host mount table. That experiment was stopped and its original
four mount rows and test bind were explicitly unmounted. A separate earlier
attempt rejected Docker overlay as a durable roster filesystem; another
rejected the test catalog's initial directory mode. Neither reached a consumer.

The successful experiment used the ignored external test
`engine/crates/layerfs-daemon/tests/external_mount_visibility.rs`. Its owned
host harness, copied test executable, SHA-256, Docker container identities,
original process identity, mount IDs, output and cleanup receipt are retained
under `/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/host-visibility-20261110-3kkgt9yj/`.
The test executable was compiled with Rust 1.85.1 and run with a 90-second
timeout in the producer container; the test itself bounds its consumer wait.

This proves visibility and isolation for this explicitly assembled fixture
only. It does not prove a secure provisioning service, isolation between host
processes sharing a uid, project authorization, SDK support for host exports,
AutoTask integration, Kubernetes Pod behavior, crash recovery, or persistence
after node loss. The currently published SDK still creates Sandboxes whose
Workspace mounts are internal to their own container.
