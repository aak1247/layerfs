# Directory entries and operation records

> **Status:** Current source terminology, updated by issue #320 on 2026-10-14.
> The table names and the record shapes below were revised by issue #321
> Step C (2026-10-14); schemas 35–37 below supersede schema 34 and the
> relation names, the `ns` partition and the schema numbers in the older text.

## Schema 34: one Workspace per database (superseded by schemas 35–37)

Each Workspace has its own overlay database file, so no row carries a
Workspace key. Only the `workspace` row names its namespace (`ns`), and the
engine refuses to open a second Workspace in a database. Open state stays in
memory, keyed by that namespace. Rust type, method and counter names are
unchanged; only the SQL relations, their indexes and three column names changed.

| Relation (schema 34) | Was | Holds |
| --- | --- | --- |
| `fs_inode` | `inode` | inode rows per generation |
| `fs_entry` | `directory_entry` | directory entries, including recorded removals |
| `fs_data` | `payload` | payload cells and dense runs |
| `fs_data_ref` | `payload_reference` | reference pieces naming committed content |
| `fs_truncate` | `shrink` | truncation steps |
| `commit_run` | `operation_owner` | an engine-minted operation owner |
| `commit_reader` | `captured_reader` | a captured reader |
| `commit_base_pin` | `base_source` | a base-source owner |
| `commit_pin` | `lease` | a custody pin |
| `commit_record` | `indexed_operation_record` | indexed operation records |
| `cleanup_task` | `maintenance` | the bounded maintenance queue |

Columns `serial`, `gen` and `owner` are `inode`, `generation` and `holder`.
Indexes follow their tables (`fs_inode_capture`, `fs_entry_capture`,
`fs_data_generation`, `fs_truncate_generation`, `commit_pin_resource`,
`cleanup_task_ready`, `cleanup_task_generation`).

The integer-keyed `operation_record` and `owned_operation_record` relations
and their methods are removed, not merged. One shape cannot serve an integer
key and an (8-byte file scope, kind, 32-byte key) identity without an untyped
key, a caller-chosen and an engine-minted operation number could collide in
one relation, and neither integer-keyed relation had a production writer.

Schema 35 removes the `accounting` table and all 27 accounting triggers.
`Resources` computes identical row counts and byte sums on demand in one SQL
observation, projecting lengths rather than payload values. The explicit observer
pays for table visits; mutations, base reads and cleanup no longer maintain a
diagnostic row. The daemon aggregate remains the sum over Workspace files. The
maintenance cursor is (kind, resource, target). Configured page quotas remain
physical limits: creation fails if its schema exceeds the chosen quota. The
schema-34 30-page observation precedes accounting removal and is not a
schema-35 size claim.

`user_version` is 37. The file is created fresh and is never reopened: there
is no migration, conversion or retry.

<a id="rows-the-engine-remembers"></a>

## Runtime Workspace state and remembered SQL custody

Schema 36 removes `workspace` and its AUTOINCREMENT allocator. The engine owns
one fixed runtime value: base root, active/captured/installed
frontiers, captured revision, current revision, lifecycle, dirty counts,
base-reader count and consolidation frontier. It is authoritative state, not
an evictable SQL cache. `Overlay::state` reads it under the Workspace job lock.

Every atomic job retains one fixed pre-job value, alongside its mount slot.
Known whole-job failure restores those values after SQL rollback and withdraws
only that job's issued reply tickets; memory-only jobs have the same restoration
without a SQL BEGIN. Unknown SQL/rollback outcomes quarantine and retain the
attempted memory and original custody. No replay or guessed restoration occurs.
Counters retain the former nonnegative signed SQLite range. Capture freezes its
paired generation/revision; install preserves later active state and the same
visible view. Base-source acquisition/release no longer updates SQL counters.

Schema 37 also removes `commit_run`, `commit_reader`, `commit_pin` and
`commit_base_pin`. The existing journaled OwnerTable holds operation and captured
reader values indexed by minted holder and exact request. A generation-count
map indexes captured holds without scanning owners. Base sources live in one
journaled (holder, class) map; their retained root/floor remains exact. File
read/open/lookup custody is already resident. Last-owner release updates these
indexes and wakes held cleanup in the same atomic job.

No SQL custody cache or statement-text invalidation remains. Resources preserves
its previous count meanings from map lengths (read/operation/captured pins,
base sources, operation/reader details), alongside on-demand filesystem counts.
The closed template cannot report those live memory owners; no zero is inferred
from their absence in SQL. There are seven product tables: the five filesystem
relations, commit_record and cleanup_task. The final cleanup-list cut is pending.

The former LeaseKind is replaced by private Reader/File/Lookup reference classes;
no generic leases or SQL lease family remain. StatementKind also drops the now
unused Workspace family, leaving twelve SQL families. Telemetry section 36
records that cardinality so readers preserve historical fourteen-family receipts.

## Recorded checkpoint cleanup in durable schema 43

The experimental durable format records an original checkpoint attempt and its
reader and operation identities beside the Workspace's durable binding. A caller
may schedule its captured generation for folding only after establishing that its
Store publication was not sent. The attempt remains present while the existing
bounded maintenance processes that generation and its operation records. Fold,
retire and operation-cleanup completion are recorded in the same transactions as
the corresponding maintenance steps.

The local cleanup observation and finish operation check that exact attempt,
original binding and namespace, released owners, positive completion facts and
the relevant queue keys. They do not drain unrelated work. Finish rechecks and
removes the original folded attempt in one atomic job; an old token cannot finish
a later attempt. This local operation does not authenticate a Store outcome or
authorize recovery after an unknown publication. Installed attempts continue to
require a verified Store receipt and are not finished by this path.

## Earlier terminology (schema 16 to 33)

A `directory_entry` is a directory name-to-inode binding, including a recorded
removal. The Overlay public DirectoryEntry and its windows/changes describe the
same concept. The SQL table, capture index, accounting triggers, prepared
identifiers, source/captured read methods, counters and daemon responses use
that terminology. Bare component bytes remain `name`/PathName; kernel/VFS dentry
terminology remains external terminology.

An `operation_record` is an intermediate record owned by a Workspace operation.
The three existing access/ownership shapes retain their distinctions:

| SQL relation | Public shape | Existing ownership |
| --- | --- | --- |
| operation_record | OperationRecord, integer key | Explicit operation lease |
| owned_operation_record | OperationRecord, integer key | Engine-minted OperationOwner |
| indexed_operation_record | IndexedOperationRecordScope/Key/Change/Apply | OperationOwner plus full file scope/kind/binary key |

Workspace uses OverlayOperationRecords and typed OperationRecord replies,
refusals and copy observations. Daemon routes IndexedOperationRecord jobs through
the same fair service class/lane positions. Module filenames and function names
match those concepts. No keys, algorithms, limits or release prerequisites change.
Qualification and construction still use these rows in the one daemon-local
overlay. All Workspaces still share physical tables partitioned by `ns`; no
per-Workspace table or database design is introduced.

The physical SQL name change advances user_version from15 to16. The same
application id and MEMORY/OFF/EXCLUSIVE profile remain. Overlay creates a fresh
owned file with create_new and verifies schema16. Existing paths are refused;
there is no implicit migration, reopen, conversion or retry.

Generic Content sorting/decoder heap scratch, native crypto/frame buffers and
filesystem temporary scratch directories are different concepts and keep their
terminology. Historical source snapshots, raw receipts and E04 schema parsers
also keep the vocabulary of their recorded identity. The previous49 document
path remains as a historical link to the current operation-record description.

[The naming checkpoint](../issues/307/PRE-S8-TERMINOLOGY-20261007.md) records
file planning, exact SQL equivalence under names/version substitutions, runtime
checks, failures and source/LOC identities. This is a naming change and supplies
no new storage-layout, performance, FUSE or complete Commit qualification.


Resources is now observed on one bound Workspace handle. The daemon-wide
Resources job, disjoint snapshot summation and retiring-file aggregation are
removed. The closed template retains only its original logical size and
allocation identity; startup and template copying do not query logical row
counts. Allocation observations remain in exact roster work counters. Section
38 identifies the per-Workspace resource scope; missing live bindings produce
unavailable values, never successful zeros or a roster substitute.

## Kernel terminology (#320)

The overlay, Workspace, FUSE and daemon kernel-facing types and operations use
`Kernel` / `kernel_` names, including mount, visit, directory, mutation and
retained teardown custody. Source modules follow the same names. The SDK
example is `sandbox_kernel_mount`. This rename preserves the wire counters and
filesystem behavior. Bridge attach records and the authenticated channel retain
their old names until the separate mount/duplex rename. Existing numeric receipt
section 12 remains readable under its historical `native` decoder key; it
observes kernel serving, not host-file acquisition or a storage pack lane.
Tests are updated in the separate test pass. Historical receipts retain their
original names and verdicts.

Platform telemetry uses the `platform` feature and platform collector names.
Process start identity is a host observation. Sandbox capability/device prose
describes a kernel mount; SDK channel prose describes the duplex connection.
These terminology changes keep behavior and report encoding unchanged.
