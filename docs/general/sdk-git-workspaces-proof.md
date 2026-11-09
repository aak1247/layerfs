# Git inside durable SDK Workspaces

Status: one synthetic local Linux Docker/FUSE experiment, 2026-11-10. This is
not an AutoTask integration or automatic recovery test.

A new source directory was initialized as a standalone Git repository with a
regular `.git` directory, one committed file and one Git-ignored dependency
file. The real Node binding initialized a Durable Store from it, forked two
LayerFS Branches and mounted one Workspace per Branch in the same Sandbox.
The Sandbox used the immutable AutoTask runtime image
`sha256:644c03ea57592a10b1ccc3d16c46a38f33ae275d265ad5ad3e50e58b4da37d02`,
whose Git was 2.39.5. The source and Store were private to this experiment.

Both Workspaces initially had the same Git HEAD, a clean status and the ignored
dependency. Each then changed the tracked file and made an independent Git
commit. Each change was captured by a separate LayerFS Workspace commit, and
both Workspaces were normally unmounted. New mounts of the same two LayerFS
Branches retained their respective Git HEADs and file contents, still had clean
Git status and could read the ignored dependency. The original source remained
at its original Git HEAD with its tracked file unchanged. Both reopened
Workspaces were explicitly closed; a read-only inspection of the original daemon
mount namespace then showed no Workspace mounts.

The original Node fixture is
[`durable_git_workspaces.cjs`](../../engine/crates/layerfs-node/tests/fixtures/durable_git_workspaces.cjs).
It was run in an owned test-only Docker harness with its own SDK home, volume,
synthetic source, local Docker socket and a 110-second command timeout. The
harness and original Sandbox container identities, original observed processes,
volume, source, executable binaries, logs and resulting Git HEADs are recorded
under `/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/sdk-git-workspaces-20261110-yt2olxzy/`.
The stopped or idle test resources are retained for evidence, under this task's
cleanup ownership.

This covers a small standalone repository and one successful run. It does not
cover importing a linked Git worktree whose `.git` file points into another
repository, sharing a Git object store between Workspaces, large repositories,
concurrent Git writers in one Workspace, crash recovery, Kubernetes, or Git
operations performed by AutoTask itself. LayerFS commits and Git commits are
distinct: the experiment explicitly made both before unmounting.
