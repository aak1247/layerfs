# Exact captured state and bounded custody observations

> **Status:** Current implemented architecture; S2 closure after `7019801f9`, not complete native/runtime/Commit qualification.

Schema 36 update (owner-directed #320 sweep, 2026-10-14): captured generation
and revision now belong to the engine's journaled Workspace state rather than a
SQLite Workspace row. Their exact pairing, retained capture observation,
install/release validation and original unknown custody are unchanged. SQL
capture rows remain sealed indexed input; no bulk copy is introduced. The
historical schema-5 mechanism below is superseded only in state placement.

Schema5 records `workspace.captured_revision` alongside the sealed generation.
Capture sets both atomically from existing published state, then advances active
and resets only active dirty counts. The installed base cannot change while that
capture is retained. Validation checks generation, frozen revision and base root;
modified or stale values cannot install, read or release another capture.
Known install and known closed release clear both paired fields in one transaction.

`Overlay::retained_capture` is a route-qualified point observation of the exact
existing capture. It distinguishes a live/closed Workspace without a capture from
an absent incarnation. `pending_publications` pages revision/generation tickets
through revision keys with 64-key windows in this Workspace's engine. Observation neither repeats the
operation nor releases a ticket or infers upstream history disposition. A caller
must retain exact uncertainty and settle only the actual reply-attempt/fenced
operation it owns. There is no automatic recovery, rollback or resend.

Routing now belongs to the daemon's bound Workspace handle. One job-entry
check selects the handle and rejects stale daemon routing metadata before any
effect, retaining its original input. Overlay APIs accept no Route. Tokens
carry only an exact nonrecycled engine identity where ownership needs it;
map/reader/capture/ticket checks retain full original identity. Reply tickets
are revision -> generation within that engine, with no namespace/incarnation
filter. This local identity is not a remote authenticator: control still
validates authenticated Workspace incarnations and authority.

Captured cell reads validate the exact retained capture, so construction with
existing custody can read bytes during logical close. Current live reads still
reject close. Install/release invalidates the capture token and its read surface;
independent reader/orphan ownership and live generation cleanup remain S6.
The daemon exposes typed bounded capture/name/cell/ticket observations with charged
reply windows, without holding SQL across content demand or whole operations.

## Captured Workspace read composition

`SourceView::captured` composes an already-retained `CapturedReader` with an
already-retained base source. It checks the engine, immutable root, installed
floor and both original owners before returning. Name and metadata reads use
the captured rows, never the later active generation; unchanged names and bytes
come from that same base. The caller keeps both owners for every read and its
consumers, and explicitly releases them after those consumers finish. A reader
also prevents its generation from being folded while it remains held.

`Overlay::pin_snapshot` can acquire the exact capture, source and reader for
one original request in the same owner transaction. The owner can observe
that request's retained pin without repeating it. While the pin is held,
ordinary Close and individual source, reader or capture releases are refused.
After every view and external consumer is known finished, the owner can release
the exact pin in one transaction; this schedules the existing nonpublication
fold and does not change the Branch. A copied token is not another owner, and
absence during observation does not prove an unknown release succeeded.

These are in-process owners and read composition, not a POSIX mount or a
remote snapshot API. They publish no Branch and perform no Commit. A durable
overlay with a captured or consolidating generation still
requires reconciliation on reopen; this reader does not authorize recovering,
replaying or releasing an earlier process's unknown work.

Actual EXPLAIN and correlated VM/row counters, source identities, covering host/
Linux checks and precise milestone/resource boundaries are in the
[S2 exit audit](../issues/307/S2-EXIT-AUDIT.md). Ticket/capture probes are scope
counts, not cache-cold latency measurements. Installed physical debt and aggregate
residency are not accepted from these tests.
