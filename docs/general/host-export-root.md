# Host export root admission

Status: Linux read-only validation primitive. It is not connected to the SDK,
Docker Sandbox creation, AutoTask, or a Kubernetes node service.

`layerfs_sandbox::host_export::HostExportRoot::open_fresh` checks an explicitly
provisioned, empty mount root before a trusted local adapter considers exposing
Workspace mounts. It creates, mounts, changes and removes nothing. Every path
component is opened with no symlink following; the held descriptors must name
root-owned directories without group or other write permission, in the initial
user namespace. The final directory must be traversable by ordinary consumers.

The root must be an exact, unstacked shared mount in the caller's current mount
namespace, with a propagation peer distinct from its parent and every other
mount visible in that namespace. A fresh root must have no directory entries or
child mounts. This rejects the duplicate-propagation setup observed in the
[host visibility experiment](host-mount-visibility-proof.md).

The guard retains the ancestor descriptors and the observed boot, directory,
mount and peer identifiers. `verify` reopens and compares each named ancestor
against its held descriptor, including its kernel mount ID, and rechecks the
propagation topology. `reopen` compares saved fields, but a match after the guard
has been lost does **not** prove uninterrupted identity: kernel identifiers can
be reused. A match is not an owner-exit proof or an authorization to replay a
mount, close, commit or other operation.

External tests used separate, newly provisioned mount roots on the local host.
They verified an isolated shared root, a path containing a space, refusal of a
root in its parent's peer group, refusal of a nonempty fresh root, and failure
after an original held root's propagation or ancestor permissions changed. The
exact test mounts were normally unmounted; the owned fixtures were removed.
Receipts are retained at `/tmp/layerfs-export-fence-smu1qg7u/` and
`/tmp/layerfs-export-ancestor-4xq566wo/`.

An adapter still needs its own exclusive lifecycle authority, a durable intent
and receipt, proof that its Docker or node runtime addresses this same root,
consumer-specific mount and uid permissions, project/device authorization,
and a confirmed fence for the original process, mounts and consumers before
recovery. This guard alone does not identify other mount namespaces, control
access between processes with the same uid, preserve Workspace content, or
provide a crash-recovery protocol.
