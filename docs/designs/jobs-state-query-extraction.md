# Managed Jobs Query Projection Extraction

_Created: 2026-07-20_

## Problem

`sky/jobs/state.py` is 3,482 lines after its schema and Batch repositories were
extracted. It still owns several persistence responsibilities that change for
different reasons. Managed-job lifecycle transitions, pool scheduling and
resource accounting, filtered queue and list projection, metrics aggregation,
and event or log retention all share one module.

The filtered projection family is a 577-line read-only block. API, CLI,
dashboard, metrics, and load-test callers use it to construct filters, count
jobs and statuses, paginate unique jobs, decode joined rows, and surface Batch
progress. It does not own lifecycle writes, scheduler locks, retry policy, or
retention state. Keeping it interleaved with those write paths makes both query
optimization and lifecycle review harder than necessary.

## Goals

Move the cohesive managed-job query and response-projection family behind the
existing `sky.jobs.state` entrypoint without changing public or historical
private signatures, callable identity through the facade, SQL statement shape,
query counts, result decoding, pagination, file fallback behavior, logging
category, schema identities, or database-manager identity.

Keep the extraction bounded. Do not move blob-GC reads, scheduler priority
reads, lifecycle or schedule transitions, pool accounting, event retention, or
unrelated point lookups.

## Background and Responsibility Map

The current module has four independently changing responsibilities:

1. Managed-job lifecycle and recovery transitions. Controllers, recovery
   strategies, cancellation flows, and Skylet services call these functions.
   They depend on sync and async sessions, retry helpers, callbacks, and event
   writes. They own status and schedule-state mutations. Their failures are
   lost transitions, stale overwrites, partial rollbacks, and sync or async
   divergence. They change with recovery and lifecycle correctness.
2. Filtered query and response projection. API endpoints, queue utilities,
   dashboard and CLI readers, Prometheus collectors, and scale tests call this
   family. It depends on SQL joins, filter expressions, pagination, joined-row
   decoding, and the shared storage gateway. It owns no durable state. Its
   failures are incorrect counts, duplicate or missing rows, filter or sort
   drift, response-shape drift, and added database round trips. It changes with
   API fields, dashboard filters, metrics, and query optimization.
3. Pool scheduling and resource accounting. The managed-jobs scheduler, pool
   controllers, Batch assignment, and Serve capacity readers call it. It
   depends on schedule states, assignments, resource JSON, and async waiting
   queries. It owns queue and occupancy state. Its failures are
   oversubscription, stale assignment, unfair ordering, and incorrect
   accelerator accounting. It changes with capacity and scheduling policy.
4. Job events and log-retention metadata. Transition helpers, APIs, debug
   dumps, startup, and garbage collection call it. It depends on event and
   task tables, timestamps, sessions, and retention constants. It owns
   append-only events and cleanup markers. Its failures are missing
   diagnostics, event ordering drift, and premature deletion. It changes with
   diagnostics and retention policy.

Responsibility 2 has distinct read-only callers, dependencies, failure modes,
and reasons to change. Its query builders, aggregate readers, joined-row
decoder, and Batch-progress subquery form one stable leaf. Responsibilities 1,
3, and 4 remain in `state.py`.

## Solution

Create `sky/jobs/state_queries.py` containing:

- the Batch-progress aggregation subquery;
- joined-row to managed-job response projection;
- response-field to SQLAlchemy-column mapping;
- filtered query construction and status aggregation;
- paginated managed-job list and total-count reads;
- metrics status-count reads.

Keep `sky.jobs.state` as the stable facade by importing these objects as direct
aliases. Existing callers continue importing `sky.jobs.state`; there is no
wrapper, protocol, abstract base class, registry, dependency-injection layer,
or additional runtime call frame. The earlier `state_storage.py` gateway gives
both modules the same `DatabaseManager` object.

`state.py` continues using the aliased row decoder and Batch-progress subquery
for its existing `get_managed_job_tasks()` read path. The extracted module
initializes its logger with the historical `sky.jobs.state` name so YAML
fallback diagnostics retain their category. The SQLAlchemy statements and row
decoding move without behavioral edits.

## Alternatives Considered

Leaving the code in place avoids structural churn, but the query family is now
the largest low-state leaf left in `state.py` and has a caller and performance
model distinct from transition, scheduling, and retention writes.

Moving only the public `get_managed_jobs_with_filters()` function would leave
its builders, field mapper, decoder, aggregation subquery, and metrics variants
behind or require a parameter-heavy forwarding layer. Moving the complete
query family keeps one SQL and projection boundary.

Moving blob-GC or scheduler-priority reads as well would group unrelated
reasons to change merely because all are reads. Those point queries stay with
their owning storage and scheduler responsibilities.

Changing callers to import `state_queries` directly would enlarge the API
change and break historical monkeypatch locations. Direct aliases preserve the
existing facade and avoid wrappers.

## Test Plan

Before moving code, add and run a characterization test that pins the facade
function parameters and Batch-progress subquery contract. After extraction,
extend it to prove direct callable identity, subquery identity, shared
database-manager identity, schema-table identity, and the historical logger
name.

Run the existing managed-jobs state query tests, metrics tests, queue tests,
jobs utility tests, and database scale-query characterization. Existing tests
cover filter combinations, pagination, sorting, field projection, refined
statuses, Batch progress, YAML fallback, and query-count expectations. Run the
sync and async parity suite to prove the structural move did not disturb the
remaining state facade.

Run `format.sh --files` for all changed Python files, full mypy and Pylint as
invoked by that script, Python compilation, and `git diff --check`. Compare the
moved function ASTs after normalizing module placement. Measure alternating
cold imports of `sky.jobs.state`; direct aliases must add no wrapper calls,
queries, transactions, database round trips, or row copies.

The unfiltered Python unit-test workflow collects every focused test file.
Static analysis, format, type checking, import-linter, and runtime-contract
jobs cover all changed Python paths. The PR must remain open unless the full
visible exact-head check rollup is green and review threads are clear.

## Rollout

This is a structural extraction with no migration or feature flag. Reverting
the commit restores the previous module layout without changing persisted data
or caller imports.
