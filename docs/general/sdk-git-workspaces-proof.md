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
cover sharing a Git object store between Workspaces, large repositories,
concurrent Git writers in one Workspace, crash recovery, Kubernetes, or Git
operations performed by AutoTask itself. LayerFS commits and Git commits are
distinct: the experiment explicitly made both before unmounting.

A second independent experiment imported a synthetic **linked Git worktree**,
whose `.git` is a file pointing to its main repository on the host. Its dirty
tracked file was visible in the Workspace, but both `git status` and
`git rev-parse HEAD` exited 128: the imported `.git` still named the host path,
which did not exist in the Sandbox. The source worktree and its dirty file
remained unchanged. The Workspace was explicitly unmounted. The fixture,
container and volume identities and original output are retained at
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/sdk-linked-git-20261110-ftfyc24r/`;
the Node input is also kept as
[`linked_git_worktree.cjs`](../../engine/crates/layerfs-node/tests/fixtures/linked_git_worktree.cjs).
This failure means linked worktrees need a self-contained, authorized Git
materialization before import; the successful standalone-repository result does
not establish that support.

A third experiment used the actual AutoTask `workspace_archive.rs` code in a
separate test harness to create a self-contained copy of a new synthetic linked
worktree. Before import the copy retained the original branch and Git HEAD,
staged index, unstaged and untracked drafts, ignored dependency, and valid Git
object connectivity; it did not include other worktrees' private metadata. The
SDK imported that copy, and the Sandbox observed the same state. After a Git
commit and a LayerFS commit, a normal unmount and new mount retained the new Git
HEAD and file contents. The original linked worktree remained unchanged. The
test harness, source hash, temporary archive, synthetic source and staged copy,
original runtime identities and output are retained at
`/home/yahui/.cache/layerfs-worktrees/layerfs-467ee0a0e226/runs/staged-linked-git-20261110-hh23b9as/`;
the SDK portion is
[`staged_linked_worktree.cjs`](../../engine/crates/layerfs-node/tests/fixtures/staged_linked_worktree.cjs).
The harness used the same direct `anyhow`, `tar` and `walkdir` versions as
AutoTask, not AutoTask's entire locked dependency graph.

This is a manually staged, small synthetic fixture, not a shipped integration.
It does not establish safe staging of an actively changing source, bounded
space usage for large projects, source authorization, credential handling,
automatic cleanup, crash recovery, or Kubernetes behavior. The archive was a
full temporary copy, and the existing upload routine can include Git config
and hooks; a production integration must preserve its own authorization and
sensitive-data boundaries rather than assume those details are safe.
