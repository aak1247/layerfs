# Explicit Rust SDK recovery of a host-exported Workspace

`SandboxApi::recover_observed` starts one new daemon against an existing
Durable Store and discovery catalog. The caller must first durably retain the
original authority and the results of fencing the old controller, daemon,
mounts and consumers. The caller must also authenticate the original physical
Store and detect copying or rollback independently. A new host-export lease is
required; the old one is not silently reused. The SDK does not perform those
checks, adopt an uncertain container, or retry any operation.

The observer records each original container lifecycle boundary. A started
daemon may have advanced durable Workspace state before authentication, so an
unknown Start or failed observer requires reconciliation, not replay. On
failure the SDK returns the original request, recovery authority, known runtime,
channel and receipts. On success it keeps the recovery authority with the
`ManagedSandbox`, including on later close failure.

`WorkspaceApi::locate_recovered` observes a recovered identity without
mounting it. Given a binding whose authority and lifecycle the controller has
independently checked, `attach_bound_observed` sends one Attach and retains the
original binding and Ready receipt if the caller's journal fails. A later
observation cannot settle an earlier unknown Bind, Attach or close. Consumer
authorization and preserving outstanding operations remain controller duties.

`WorkspaceApi::unmount_observed` records an explicit normal close intent and
the original result before returning. An observer failure after acknowledgement
retains that acknowledgement; a refusal or uncertain exchange retains its
original failure. The caller chooses whether to discard dirty data and must
keep unknown results without replay. Forced teardown is a separate operation.

## Isolated Linux proof, 2026-11-10

The `sandbox_recover` example and the original `sandbox_kernel_mount` example
were exercised against a dedicated Docker volume, daemon and two distinct
private shared host-export roots. The first SDK created a durable Workspace;
an ordinary host user wrote a file and synchronized the file and directory.
The controller then killed the exact original SDK process via pidfd, killed
its identified container, verified the original cgroup was empty, unmounted
the original FUSE mount and confirmed its connection disappeared. It observed
the Store file's device and inode and supplied those facts and the original
authority to the new SDK invocation.

The SDK recovered the original catalog and Overlay, observed the Workspace,
rejected the old namespace, attached the new namespace, verified the host
mount and read the previously synchronized bytes. The example then explicitly
discarded the synthetic Workspace and stopped and deleted its Sandbox. The
controller verified both FUSE connection and host mount were gone and removed
only the identified synthetic containers, volume and export roots. The local
receipt and individual observations are in
`~/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/sdk-api-recovery-20261110-Jy3jXxMJ/`.
The example's textual records illustrate lifecycle boundaries; they are not a
complete production journal or an implementation of automatic recovery.

The same fenced recovery was repeated with `unmount_observed` recording the
original intent and acknowledged close. The dedicated fixture was fully
retired after the original and recovered FUSE connections disappeared; its
receipt is in `~/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/sdk-observed-close-20261110-OaSBsLV1/`.

This is a single-host process-crash test with an ext4-backed Docker volume. It
does not prove physical power-loss durability, Kubernetes recovery, automatic
old-owner fencing, authorization of arbitrary project consumers, or survival
of state that was not acknowledged and synchronized.
