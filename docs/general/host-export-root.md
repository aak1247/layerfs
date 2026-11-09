# Host export root admission

Status: Linux host- and daemon-side validation and an explicit Rust Sandbox
adapter. The JavaScript SDK, AutoTask, and a Kubernetes node service do not
yet use the host export.

`layerfs_sandbox::host_export::HostExportRoot::open_fresh` checks an explicitly
provisioned, empty mount root before a trusted local adapter considers exposing
Workspace mounts. It creates, mounts, changes and removes nothing. Every path
component is opened with no symlink following; the held descriptors must name
root-owned directories without group or other write permission, in the initial
user namespace. Provision the final directory with only the intended consumer
access, for example a dedicated group and mode `0750`; it need not be open to
all host users. The checking process must itself be able to read the directory.

The root must be an exact, unstacked shared mount in the caller's current mount
namespace, with a propagation peer distinct from its parent and every other
mount visible in that namespace. A fresh root must have no directory entries or
child mounts. This rejects the duplicate-propagation setup observed in the
[host visibility experiment](host-mount-visibility-proof.md).

The guard retains the ancestor descriptors and the observed boot, directory,
mount and peer identifiers. `verify` reopens and compares each named ancestor
against its held descriptor, including its kernel mount ID, and rechecks the
original ownership and mode of each component and the propagation topology.
`reopen` compares saved fields, but a match after the guard
has been lost does **not** prove uninterrupted identity: kernel identifiers can
be reused. A match is not an owner-exit proof or an authorization to replay a
mount, close, commit or other operation.

The optional version 8 daemon setup carries the boot ID, directory device and
inode, and shared propagation peer, but not the host mount ID: a container's
mount namespace gives the same bind a different ID. Ordinary configurations
continue to encode as version 7. Before opening an Overlay or Store, a daemon
that receives version 8 independently checks an empty export in its own mount
namespace, pins its own descriptor chain and local mount ID, and compares the
cross-namespace fields. The daemon requires the initial user namespace and
root-owned, non-writable path components. Unsupported or mismatched setups
are refused, rather than downgraded to a container-local Workspace.

Before opening storage, the daemon also creates `.layerfs-export-owner`
exclusively in the pinned root, writes the boot ID and its public key, and
syncs the file and directory. An existing or partially written marker is never
replaced. The daemon keeps an exclusive advisory lock on that exact file while
it owns the export. Each later guarded operation rechecks its original file
identity, permissions and contents. Two daemons racing for one root cannot both claim
it, even if the controller's advisory lock was lost. The marker remains after
normal exit; a caller must preserve it until an independently verified
retirement or recovery protocol says what can be done. Its physical-crash
durability depends on the export filesystem; a tmpfs test is not such proof.
Acquiring a released marker lock is only evidence about that cooperative lock;
it does not establish that old FUSE connections or external consumers have
gone away.

An owned KVM guest additionally wrote and synchronized a claim on a new ext4
disk (`cache=none`), then was killed immediately after the acknowledgement.
A second guest on the same disk read the original boot ID and public key,
checked mode and ownership, and refused to treat the claimed root as fresh.
The receipt and both serial logs are retained in
`~/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/export-claim-vm-rerun-20261110-2rAHnfWx/vm-secure-root/`.
This models guest memory loss on that disk, not physical host power loss,
automatic recovery, or proof that the old consumers have exited.

The kernel serving owner retains this daemon-side guard. It rechecks the root
before an Attach, and checks both the root and the exact child mount ID and
device before acknowledging Ready. Normal and forced teardown also check the
original root and child identity before acting; if the check fails after a
connection exists, the connection and its original close intent stay retained.
These checks are observations at operation boundaries, not atomic protection
against a privileged concurrent mount change.

The host must independently retain and recheck its own `HostExportRoot`: if a
host removes its bind from a propagation peer group, the container may still
see the old group unchanged. The daemon-side guard alone cannot establish that
the host is still a member, identify consumers, or authorize their use.

`HostExportLease::try_acquire` can additionally hold a nonblocking advisory
exclusive lock on the verified host directory inode. This prevents two
cooperating live owners from claiming that same inode, without creating a file
in the fresh root. The locked descriptor and the mount/path guard are checked
together. The lock disappears when its process loses the descriptor: it does
not fence noncooperating processes, record a durable intent, prove daemon or
consumer exit, or authorize adoption or recovery after a crash. A lifecycle
adapter must retain both its lease and its separate durable evidence, including
when a create or close result is unknown.

The Rust Docker adapter accepts one live lease as an explicit Sandbox creation
input, consumes it for at most one Create attempt, and retains it in the
original runtime owner. The deployment setup must name the same host identity;
the adapter checks the Engine's exact source, destination, read/write state and
`rshared` propagation, along with its existing topology checks. The startup
archive omits the export mountpoint itself: writing a directory header there
would let Docker change the host directory's ownership and permissions. The
adapter verifies the lease during startup and refuses an exported Sandbox
reopen until an independent fenced recovery protocol exists.

Given the original authenticated Ready receipt, the host lease can observe a
Workspace's positive namespace and device major/minor. It pins the host-visible
directory and checks its descriptor, host mount ID, parent, filesystem, source
and device against the held export root, refusing stacked or nested mounts and
changed identities. The host mount ID is local to the host namespace and must
not be compared to the daemon's mount ID. The descriptor can keep a mount busy,
so the consumer must drop the guard before requesting unmount. These checks
are boundary observations; they do not authorize a consumer, fence privileged
concurrent changes or make an uncertain effect safe to repeat.
An isolated Docker/FUSE test matched the authenticated device to the distinct
host mount ID, rejected a wrong device, temporarily stacked a test bind mount
over the Workspace and observed refusal, then removed that exact test bind and
verified the original mount before normal use and close. Its receipt is
`~/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/host-workspace-identity-20261110-jdx5FVHU/receipt.txt`.

`ManagedSandbox::observe_host_workspace` obtains this guard through the
Sandbox's own retained lease. It first checks that the acknowledged runtime
has not been stopped or deleted and that the original binding, Ready token
and container path agree. A recorded start does not prove the daemon remains
alive. The caller remains responsible for preserving the
original authenticated acknowledgement; the method does not reconstruct or
authenticate a receipt supplied from an unrelated source.
An isolated Docker/FUSE run exercised the Sandbox-owned method, including
refusal of a mismatched Ready path, token or device and of a stacked host
mount; it then restored the original mount and closed normally. Its receipt is
`~/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/sandbox-workspace-identity-20261110-IgKEvEeO/receipt.txt`.

This low-level adapter does not yet provide a complete durable lifecycle
journal and recovery for exported Sandboxes or automatic host guard checks
around every Workspace operation. A caller must supply and retain those
authorities itself. In particular, a released advisory lock or an existing
claim does not prove that an old daemon, mount or external consumer has stopped.

External tests used separate, newly provisioned mount roots on the local host.
They verified an isolated shared root, a path containing a space, refusal of a
root in its parent's peer group, refusal of a nonempty fresh root, and failure
after an original held root's propagation or ancestor permissions changed. The
exact test mounts were normally unmounted; the owned fixtures were removed.
Receipts are retained at `/tmp/layerfs-export-fence-smu1qg7u/` and
`/tmp/layerfs-export-ancestor-4xq566wo/`.

Further isolated tests compared the recorded identity inside a real Docker
namespace, rejected altered identity fields and a nonempty root, and refused
a later propagation change. The complete daemon also started and reported its
listener with the verified export in a FUSE-capable Docker container. A
separately assembled durable daemon Service
attached two FUSE Workspaces with this guard; a host process and a restricted
nonroot container accessed only their selected Workspace and both mounts were
normally closed. In a second run, changing the producer's propagation caused
the first recorded close to retain its original connection; the next close was
refused by that pending intent. After the test owner and consumer exited, only
the exact test mounts were normally unmounted. These are manually assembled
Service proofs. A separate real Rust `SandboxApi` Docker run used a `0750`
root owned by root and the authorized user's group: the matching host and
container identities accessed a Workspace, an unrelated uid without that
group could not traverse or read it, and the root's ownership and mode stayed
unchanged through startup and normal close. That run used a Disposable Store;
it is not JavaScript SDK or AutoTask integration or automatic recovery.

An adapter still needs its own exclusive lifecycle authority, a durable intent
and receipt, proof that its Docker or node runtime addresses this same root,
consumer-specific mount and uid permissions, project/device authorization,
and a confirmed fence for the original process, mounts and consumers before
recovery. This guard alone does not identify other mount namespaces, control
access between processes with the same uid, preserve Workspace content, or
provide a crash-recovery protocol.
