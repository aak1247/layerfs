# Complete-tree import with selected Git metadata

The Rust `ProjectApi::init_with_git_directory` imports one complete source
directory and substitutes its root `.git` entry with a separately supplied
directory. All other entries use the ordinary complete-tree importer: ignored
files are not filtered, symlinks are stored without following their targets,
and in-tree regular-file hard links retain their shared identity. The same
bounded acquisition and canonical construction serve ordinary `init`.

This interface lets a trusted integration supply independent metadata for a
linked Git worktree, whose original `.git` file may name a host-only path. The
caller must separately authorize the source and metadata directory, validate
that the latter is a complete usable Git repository, and fence every writer for
the entire import. LayerFS does not validate Git objects, interpret the source
`.git` pointer, establish a snapshot, authorize either source, or restore a
Workspace. Both directories must remain private and disjoint, including through
filesystem aliases. In particular, a bind-mount alias outside the named paths
needs a caller-provided check.

The source must contain a regular file or directory named `.git`; the supplied
replacement must be a real directory. Before creating the Store, the method
rejects source/metadata ancestor overlap and a Store parent inside either
input. The importer repeats these checks before starting acquisition, and
rejects a source whose `.git` entry is absent during the root scan. A failed
attempt retains the original `InitFailure`, including its request and known
outcomes, together with the supplied Git directory in `GitInitFailure`; it
does not delete output or replay an operation. The durable profile, deadlines,
and seal retain their existing contracts.

The public SDK tests exercise a durable Store, a source root wider than one
in-memory scan window, ignored files, a symlink and a hard-link pair. They also
check that overlapping inputs and a Store inside the metadata directory are
refused before Store creation. These tests use synthetic Git metadata and do
not establish Git integrity, source fencing, automatic recovery or platform
integration.
