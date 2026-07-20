# Managed Jobs Event Repository Extraction

_Created: 2026-07-20_

## Problem

`sky/jobs/state.py` is 2,900 lines after its schema, Batch persistence, and
filtered query repositories were extracted. It still owns lifecycle and
recovery transitions, status-check reads, pool scheduling and resource
accounting, log-cleanup metadata, and the complete job-event repository.

The job-event block is a 235-line table-specific leaf. Transition helpers write
events, API and debug-dump callers read timelines, queue projection reads the
latest recovery and pending reasons, and API-server startup owns the retention
daemon. These callers do not use the module's scheduler locks, resource
accounting, task lifecycle SQL, or log-cleanup markers. Keeping event storage
interleaved with those responsibilities makes event ordering and retention
changes contend with unrelated lifecycle and scheduler work.

## Goals

Move the complete job-event persistence lifecycle behind the existing
`sky.jobs.state` entrypoint without changing public or historical private
signatures, callable identity through the facade, event schema identity,
database-manager identity, SQL statement shape, query or transaction counts,
row decoding, event ordering, retry behavior, retention timing, logging
category, or transition call ordering.

Keep the extraction bounded. Do not move managed-job transitions, status-check
reads, pool scheduling, resource accounting, log-cleanup metadata, schema
definitions, or server-side event and cluster-event presentation.

## Background and Responsibility Map

The current module has five independently changing responsibilities:

1. Managed-job lifecycle and recovery transitions. Controllers, recovery
   strategies, cancellation flows, and Skylet services call these functions.
   They depend on sync and async sessions, retry helpers, callbacks, and event
   writes. They own task status, schedule state, timestamps, and failure
   details. Their failures are lost transitions, stale overwrites, partial
   rollbacks, and sync or async divergence. They change with recovery and
   lifecycle correctness.
2. Status-check and point-read projection. Controller monitors, schedulers,
   cancellation cleanup, and API readers call these functions. They depend on
   slim snapshot queries, joined task rows, and compatibility fallbacks. They
   own no durable state. Their failures are inconsistent snapshots, stale
   cleanup decisions, and added round trips. They change with controller
   monitoring and recovery optimization.
3. Pool scheduling and resource accounting. The managed-jobs scheduler, pool
   controllers, Batch assignment, and Serve capacity readers call it. It
   depends on schedule states, assignments, resource JSON, and async waiting
   queries. It owns queue and occupancy state. Its failures are
   oversubscription, stale assignment, unfair ordering, and incorrect
   accelerator accounting. It changes with capacity and scheduling policy.
4. Log-cleanup metadata. The log garbage collector selects terminal task and
   controller logs, then records cleanup timestamps. It depends on task and job
   tables, terminal schedule state, local log paths, and retention cutoffs. It
   owns cleanup markers. Its failures are premature deletion, leaked logs, or
   duplicate cleanup. It changes with storage retention and garbage collection.
5. Job-event persistence and retention. Lifecycle transitions append events;
   API, debug-dump, and queue callers read timelines and latest reasons; server
   startup runs retention cleanup. It depends on one event table, sync and
   async sessions, status enums, timestamps, and retention constants. It owns
   append-only audit records and their retention lifecycle. Its failures are
   missing diagnostics, ordering drift, stale displayed reasons, premature
   deletion, or a failed daemon. It changes with diagnostics, event projection,
   and retention policy.

Responsibility 5 has a stable table-level seam and materially different
callers, durable state, failure modes, and reasons to change from
responsibilities 1 through 4. Its writes, reads, reason projection, cleanup,
and daemon must move together so one module owns the entire event-table
lifecycle.

## Solution

Create `sky/jobs/state_events.py` containing:

- synchronous and asynchronous event inserts;
- the asynchronous task-ID helper used by event workflows;
- event timeline reads and status decoding;
- latest recovery and pending reason projection;
- asynchronous retention deletion and the retention daemon;
- the existing retention constants.

Keep `sky.jobs.state` as the stable facade by importing these functions and
constants as direct aliases. Existing callers continue importing
`sky.jobs.state`; transition helpers continue resolving event writers through
the facade globals, so their call order and monkeypatch seam stay intact. There
is no wrapper, protocol, abstract base class, registry, dependency-injection
layer, or additional runtime call frame.

`state_events.py` uses the existing `state_storage.py` gateway so both modules
share one `DatabaseManager` object. It imports the existing schema tables and
status enum directly, and initializes its logger with the historical
`sky.jobs.state` name. SQLAlchemy statements, retry decoration, exception
handling, and sleep ordering move without behavioral edits.

## Alternatives Considered

Leaving the block in place avoids structural churn, but the event repository is
now the clearest low-state leaf left after three preceding extractions. It owns
one table end to end and has API, diagnostics, queue, and startup callers that
change independently from scheduling and lifecycle SQL.

Moving only retention cleanup would create a very small module while leaving
event writes and reads behind. Moving only reads would split ordering and
status-decoding behavior from the table owner. Moving the whole event lifecycle
keeps one repository boundary.

Moving log-cleanup metadata with job events would group two unrelated retention
mechanisms. Log cleanup owns filesystem deletion markers on job and task rows;
event retention owns database records in `job_events`. Log cleanup stays in
`state.py` for a later independent decision.

Changing callers to import `state_events` directly would enlarge the API change
and break historical patch locations. Direct aliases preserve the stable facade
without redundant forwarding functions.

## Test Plan

Before moving code, add and run characterization tests that pin every event
facade signature, retention constant, shared table and database-manager
identity, historical logger category, and the lifecycle transition's lookup of
the facade event writer. After extraction, extend the identity assertions to
prove every facade object is a direct alias of `state_events`.

Run the managed-jobs state and event suites, sync and async parity tests, server
event merge tests, queue projection tests, debug-dump and jobs utility tests,
and server startup tests that schedule the daemon. Existing tests cover event
ordering, task and job-level filtering, status decoding, latest-reason tie
breaking, async transition writes, retention cleanup, cancellation, and
server-side event presentation.

Run `format.sh --files` for every changed Python file, full mypy and Pylint as
invoked by that script, Python compilation, and `git diff --check`. Compare all
moved function ASTs after normalizing module placement. Measure alternating
cold imports of `sky.jobs.state`; direct aliases must add no wrapper calls,
queries, transactions, database round trips, row copies, or daemon iterations.

The unfiltered Python unit-test workflow collects every focused test file.
Static analysis, format, type checking, import-linter, and runtime-contract jobs
cover all changed Python paths. The PR must remain open unless the full visible
exact-head check rollup is green and review threads are clear.

## Rollout

This is a structural extraction with no migration or feature flag. Reverting
the commit restores the previous module layout without changing persisted data,
serialized values, process lifecycle, or caller imports.
