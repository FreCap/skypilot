# Managed Jobs State Schema Extraction

## Problem

`sky/jobs/state.py` is 4,336 lines and owns both declarative SQL schema metadata
and the runtime persistence behavior built on that metadata. The schema block is
a low-state leaf, while the rest of the module contains transaction-sensitive
state transitions, filtered read projections, Batch coordination, pool resource
accounting, and event retention. Keeping the table declarations in the runtime
store makes schema ownership harder to find and leaves no cycle-free schema seam
for later bounded repository extractions.

## Responsibility map

| Responsibility | Callers | Dependencies and state | Failure modes | Sensitivity and cadence |
| --- | --- | --- | --- | --- |
| Declarative schema metadata | Managed-jobs state initialization and historical spot-jobs migrations through the `sky.jobs.state.Base` facade | SQLAlchemy metadata, dialect column types, `DagExecution`, and default priority; owns `Base` and seven table objects | Missing registration, changed object identity, migration drift, dialect mismatch | Cold-import construction; changes with schema migrations |
| Lifecycle transitions | Controllers, scheduler, recovery, cancellation, and Skylet services | Sync and async sessions, retries, callbacks, events, task and schedule status | Lost or illegal transitions, stale overwrites, duplicate events, partial rollback | Transaction-sensitive hot path; changes with lifecycle policy |
| Filtered query projection | API, CLI, dashboard, metrics, Serve capacity readers | SQL joins and expressions, pagination, response mapping, resources parsing | Wrong counts, duplicate rows, response drift, N+1 queries | High-fan-out read path; changes with API fields and query optimization |
| Batch persistence | Batch coordinator and recovery flows | Row locks, owner tokens, leases, dialect upserts, worker and attempt tables | Split brain, stale-owner writes, duplicate launch, lost terminal result | Round-trip-sensitive; changes with Batch recovery fencing |
| Pool scheduling and accounting | Scheduler, pool controllers, Batch assignment, Serve capacity readers | Schedule state, pool identifiers, resource JSON, accelerator parsing | Oversubscription, unfair ordering, stale assignments | Scheduler hot path; changes with capacity and fairness policy |
| Event and log retention metadata | Transition helpers, event API, debug dump, startup and garbage collection | Event and task tables, timestamps, retention policy, sync and async sessions | Missing diagnostics, ordering drift, unbounded retention, premature deletion | Insert and batch-query sensitive; changes with diagnostics and retention |

## Proposed seam

Move only `Base` and the seven `sqlalchemy.Table` declarations to
`sky/jobs/state_schema.py`. Keep `sky/jobs/state.py` as the stable facade by
importing and directly re-exporting the exact same objects. Keep
`_batch_progress_subquery` in `state.py` because it is a read projection rather
than schema metadata. Keep historical migration imports unchanged so old
migration code continues to use `sky.jobs.state.Base`.

This is a facade-first plain-module extraction. It introduces no wrapper calls,
base classes, registries, dependency injection, or new persistence abstraction.

## Behavior contract

- `sky.jobs.state.Base` remains importable and is the same object as
  `sky.jobs.state_schema.Base`.
- Existing table globals remain importable from `sky.jobs.state` and are the
  same objects registered in `Base.metadata`.
- Table names, columns, indexes, constraints, types, defaults, and registration
  order remain unchanged.
- Database-manager construction, migration ordering, transactions, queries,
  callbacks, event writes, serialized response shapes, and public status types
  remain unchanged.
- Importing the facade adds no forwarding function or runtime database call.

## Alternatives considered

- Leave the file unchanged. This avoids one module, but retains mixed schema and
  runtime ownership and leaves stateful repository extractions with no clean
  metadata dependency.
- Extract Batch persistence first. It would either import private tables and the
  database manager back from `state.py`, creating a cycle, or require moving the
  schema in the same change. That is too broad for one run.
- Move `_batch_progress_subquery` with the tables. It is query policy with a
  different reason to change, so moving it would blur the seam.
- Add an `AbstractStore` implementation or repository class. There is no second
  implementation, and the current plain-function store is the established seam.

## Implementation and verification

1. Add characterization coverage on the unsplit module for public schema names,
   table registration, object identity, and representative defaults.
2. Move the declarative block byte-for-byte into `state_schema.py`, then import
   and re-export its objects from `state.py`.
3. Extend the characterization to prove facade-to-owner identity.
4. Run focused state, sync/async parity, event, migration, and Batch recovery
   tests, then format, mypy, Pylint, static analysis, and `git diff --check`.
5. Compare repeated cold facade imports before and after. The extraction must not
   add database calls and must not materially regress import time.

## Rollout and rollback

This is an internal structural change with no data migration or feature flag.
Rollback is the inverse source move. Any object-identity, schema-shape, import,
test, performance, or CI mismatch blocks merge.
