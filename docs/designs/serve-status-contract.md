# SkyServe status contract

_Created: 2026-07-29_

## Problem

`sky/serve/serve_state.py` is 5,527 lines and owns both the service/replica
status contract used throughout SkyServe and the SQLAlchemy persistence gateway
that stores those statuses. The enums are consumed by controller policy,
replica orchestration, recovery, status presentation, server preconditions, and
tests. The rest of the module owns table metadata, migrations, lifecycle
fencing, load-balancer cutover transactions, storage cleanup, paid-capacity
coordination, replica and version persistence, and reserved-fill arbitration.

The status contract changes when lifecycle semantics or user-visible status
presentation changes. The persistence gateway changes when schemas,
transactions, locking, query plans, and database compatibility change. Keeping
the contract embedded in the database module makes broad non-persistence
callers depend on a module whose primary responsibility is transactional state.

## Goals

Move only `ReplicaStatus`, `ServiceStatus`, and their color mappings into a
focused module. Preserve `sky.serve.serve_state` as the public facade,
including direct object identity, historical `__module__` values, pickle
behavior, enum values and ordering, classification helpers, colored output,
and service-status derivation. Do not change database schemas, stored values,
queries, transactions, lock boundaries, controller behavior, or user-visible
output.

## Responsibility map

### Status contract

- Callers: Serve controller, replica manager, autoscalers, service lifecycle,
  API status projection, request preconditions, history reporting, and focused
  tests.
- Dependencies: `collections`, `enum`, and `colorama`.
- State owned: immutable enum members and static color mappings. Persisted
  representations are the existing string values, not module-owned mutable
  state.
- Failure modes: changed enum values or order, incorrect failed/terminal
  classification, changed scale-down order, changed aggregate service status,
  color-output drift, broken historical imports, or broken pickle globals.
- Performance sensitivity: comparisons and classification helpers run on
  controller ticks and fleet scans. The facade must remain a direct alias with
  no wrapper, allocation, query, lock, or copy.
- Change cadence: lifecycle semantics and status presentation.

### Schema, migration, and database initialization

- Callers: every Serve state read/write operation and database migration.
- Dependencies: SQLAlchemy metadata, SQLite/PostgreSQL dialects, Alembic
  migrations, Serve constants, and database utilities.
- State owned: table metadata, indexes, migration version, database engine, and
  initialization lifecycle.
- Failure modes: incompatible schema, migration loss, lock contention,
  initialization races, or database-format drift.
- Performance sensitivity: import/initialization time and connection setup.
- Change cadence: schema and database compatibility work.

### Service lifecycle, ownership, and HA cutover persistence

- Callers: service start/update/down paths, HA supervisor, load balancers, and
  recovery loops.
- Dependencies: service rows, lifecycle fences, PostgreSQL row/advisory locks,
  SQLite immediate transactions, and LB HA data contracts.
- State owned: lifecycle epochs, controller ownership, service status/version
  snapshots, active/pending LB slots, cutover generations, drain state, and
  demand handoffs.
- Failure modes: stale-owner commits, same-name teardown races, split-brain
  cutovers, lost demand, or incomplete rollback.
- Performance sensitivity: controller polling and recovery queries must remain
  bounded and transactional.
- Change cadence: lifecycle fencing, HA, and recovery protocols.

### Replica, placement, and paid-capacity persistence

- Callers: replica manager, autoscalers, reserved-capacity allocation, status
  summaries, and recovery.
- Dependencies: replica JSON/pickle codecs, service ownership, placement
  policy state, provider cost models, and paid-capacity tables.
- State owned: replica rows, placement and rebalance state, capacity claims,
  waiters, launch outcomes, and cleanup inventory.
- Failure modes: overlaunch, lost recovery state, stale claims, capacity
  leakage, serialization drift, or incorrect status counts.
- Performance sensitivity: fleet scans, batch writes, query counts, and row
  locking are controller hot-path constraints.
- Change cadence: fleet lifecycle, placement, and capacity coordination.

### Version and recovery artifact persistence

- Callers: service apply/update, controller recovery, storage cleanup, and
  status/version presentation.
- Dependencies: immutable version rows, YAML/spec serialization, quarantine
  records, placement catalogs, and HA recovery scripts.
- State owned: version allocation/commit results, submitted and effective YAML,
  specs, provenance, quarantine state, and recovery scripts.
- Failure modes: conflicting commits, wrong recovery version, provenance loss,
  or cleanup of live artifacts.
- Performance sensitivity: batch version reads and immutable commit
  transactions.
- Change cadence: rollout, provenance, and recovery semantics.

### Reserved-fill broker persistence

- Callers: reserved-capacity broker and atomic replica launch fencing.
- Dependencies: claim, round, lease, demand-observation, service, and replica
  tables plus SQLite/PostgreSQL transaction semantics.
- State owned: heartbeats, grants, feed state, utilization gates, lease tokens,
  per-pool epochs, and atomic launch fences.
- Failure modes: stale-writer publication, double allocation, unsafe release,
  fail-open launch, or mixed-version skew.
- Performance sensitivity: one bounded transaction per broker poll or launch
  fence; no extra provider or database calls.
- Change cadence: reserved-capacity arbitration and durability.

The status contract has materially different callers, dependencies, state,
failure modes, and reasons to change from the five persistence families. The
persistence families remain coupled by shared tables and transaction boundaries
and are not split here.

## Solution

Add `sky/serve/serve_statuses.py` containing the two existing enums and their
color mappings without behavioral edits. Set each moved enum's `__module__` to
`sky.serve.serve_state`, then bind direct aliases from the historical module.
The facade therefore adds no wrapper frame or allocation, and old pickle
globals continue to resolve.

Keep `VersionCommitResult` in `serve_state.py`: unlike the lifecycle statuses,
it is the direct result contract of the version commit transaction and changes
with that repository operation.

All production callers continue importing `sky.serve.serve_state`. The new
module is an implementation boundary, not a new required public API.

## Alternatives considered

Leaving the statuses in place avoids one module but preserves mixed domain and
persistence ownership across dozens of callers. Extracting SQL tables or one
repository family would remove more lines, but those regions share metadata,
database initialization, transaction helpers, and cross-table invariants; doing
so would require callback injection, a broad repository object, or circular
imports.

Moving `VersionCommitResult` with the lifecycle statuses would group enums by
syntax rather than responsibility. An abstract base class, registry, observer,
strategy, or dependency-injection layer is unnecessary because this is a
single stable value contract.

## Rollout and rollback

This is a source-only structural extraction with no data, schema, or
configuration migration. Rollback is the inverse move because the historical
facade remains the supported import path.

## Test plan

Before moving code, add characterization coverage for enum values and order,
failed and terminal classifications, replica scale-down order, aggregate
service-status derivation, colored strings, historical module names, and pickle
round-trips. After extraction, prove direct alias identity between the facade
and implementation module and compare protocol-5 serialized bytes.

Run the focused status contract, Serve state, status history/summary, replica
manager, controller, autoscaler, and service tests. Run repository formatting
and static checks for changed files, `git diff --check`, compile/import probes,
and an alternating cold-import timing comparison. Confirm the pull-request
workflow has no path filter excluding the changed production or test paths.
