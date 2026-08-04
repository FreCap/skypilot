# SkyServe state schema foundation

## Context

`sky/serve/serve_state.py` owns both the complete SQLAlchemy schema and every
repository operation over that schema. The file is 5,351 lines. Its first 581
lines define metadata, thirteen tables, indexes, database creation and migration
behavior, and the process-wide `DatabaseManager`; the remaining repository
functions implement service lifecycle, status projection, load-balancer
cutover, cleanup inventory, placement state, paid-capacity admission, replica
state, version state, and reserved-fill arbitration.

The schema and database bootstrap have a different reason to change from those
repositories. Schema ownership changes with columns, indexes, and migrations.
Repository functions change with controller protocols, transactional fencing,
query shapes, and policy. Keeping both in one module makes every later
repository extraction depend on private state in the historical facade.

## Responsibility map

### Schema and database bootstrap

- Callers: all Serve repository functions, database migration startup,
  placement and Serve history readers, and tests that construct isolated
  SQLite or PostgreSQL databases.
- Dependencies: SQLAlchemy metadata and dialect types, Serve constants and LB
  enum values, migration utilities, WSL detection, and `DatabaseManager`.
- State owned: one `Base.metadata` graph, thirteen table objects and their
  indexes, and one process-wide `DatabaseManager` with its cached engine.
- Failure modes: split metadata graphs, a second cached engine, migrations
  running against incomplete metadata, import-order-dependent tables, or
  historical tests patching a facade object that repository code no longer
  uses.
- Performance sensitivity: module import and first-engine initialization.
  The extraction must add no forwarding calls, metadata copies, database
  queries, or additional manager construction.
- Change cadence: database columns, indexes, migration version, and bootstrap
  behavior.

### Repository operations

- Callers: Serve controller, replica manager, service lifecycle, API routes,
  managed-job pools, history writers, placement and capacity brokers.
- Dependencies: the shared schema foundation plus lifecycle locks, serialized
  specs and replica records, LB HA contracts, placement and capacity policy.
- State owned: transactional service, version, replica, cleanup, placement,
  paid-capacity, reserved-fill, and LB cutover records.
- Failure modes: stale-owner commits, incompatible persisted records,
  incorrect transactional ordering, excess queries, and cross-controller
  races.
- Performance sensitivity: controller-loop query count, lock duration,
  batch size, serialization, and provider-call avoidance.
- Change cadence: lifecycle and recovery protocols, status projection,
  autoscaling and placement policy, and capacity arbitration.

### Load-balancer cutover repository

- Callers: Serve controller role reconciliation, service startup, and the
  Kubernetes LB implementation.
- Dependencies: the shared service table and database manager, PostgreSQL
  row locks and compare-and-set updates, and `lb_ha` state contracts.
- State owned: active and pending slots, cutover generation and phase, drain
  timestamp, and demand handoff snapshots.
- Failure modes: stale-owner promotion, lost demand floor, split active role,
  or an unfenced Kubernetes mutation.
- Performance sensitivity: bounded single-row reads and writes on controller
  role changes.
- Change cadence: external-LB HA and cutover protocol.

This run extracts only schema and bootstrap ownership. The LB repository
remains in the facade until it can move independently on the shared foundation.

## Design

Create `sky/serve/serve_state_schema.py` containing:

- the single declarative `Base`;
- all existing table and index objects;
- `create_table`;
- the single `_db_manager`;
- `ensure_tables_initialized`; and
- `get_database_engine`.

`sky/serve/serve_state.py` imports these objects at module scope and exposes
them as direct aliases at their historical names. Repository functions keep
using those aliases. There are no wrappers, abstract classes, protocols,
registries, factories, dependency-injection layers, or new package hierarchy.

The implementation module pins the moved functions' `__module__` to
`sky.serve.serve_state` so introspection retains the historical public
identity. SQLAlchemy tables, `Base`, and `_db_manager` are the exact same
objects through both modules. Only one `DatabaseManager` is constructed.

## Behavior contract

- `serve_state.Base`, every historical table symbol, and
  `serve_state._db_manager` remain available.
- Every table remains attached to the same `Base.metadata` object and the
  metadata table inventory is unchanged.
- `serve_state.create_table`, `ensure_tables_initialized`, and
  `get_database_engine` retain their historical module identity.
- Patching `serve_state._db_manager._engine` continues to affect all repository
  functions and the schema implementation.
- SQLite WAL setup, migration mode, database path, and first-engine
  initialization are unchanged.
- Import order between `serve_state` and `serve_state_schema` does not create a
  second metadata graph or database manager.
- There is no change to database formats, migrations, query count, transaction
  ordering, serialization, public imports, or user-visible behavior.

## Alternatives

### Keep the file cohesive

This avoids structural churn but leaves schema ownership entangled with every
repository and forces later extractions to import private facade state. The
schema block is a complete low-state foundation with an independently stable
identity, so retaining it no longer has the lower carrying cost.

### Extract the LB cutover repository directly

The cutover functions currently depend on `services_table` and `_db_manager`.
Importing those from `serve_state` would create a cycle, while passing them
into every operation would add dependency injection without a second
implementation. Moving the foundation first is smaller and removes that
constraint.

### Move schema and LB cutover together

This would remove more lines but combine two independently reviewable changes
and enlarge the PostgreSQL transaction blast radius. It is deferred.

### Add a repository class or database context object

There is no second implementation and no need for construction-time variation.
A plain module and direct aliases preserve the existing process-wide identity
with less surface.

## Milestones

1. Add and run characterization tests against the current monolith.
2. Move the schema and bootstrap block without semantic edits.
3. Add direct historical facade aliases and pin moved function identities.
4. Prove AST equivalence for moved functions and object identity for metadata,
   tables, indexes, and the database manager.
5. Run focused Serve state, migration, reserved-capacity, and import tests,
   followed by formatting, static checks, and the relevant component suite.

## Changed-path-to-test matrix

| Changed path | Responsibility | Verification |
|---|---|---|
| `sky/serve/serve_state_schema.py` | SQLAlchemy metadata, tables, migration bootstrap, database manager | schema contract tests; Serve state tests; migration tests; reserved-fill SQLite and PostgreSQL tests |
| `sky/serve/serve_state.py` | historical facade and repository access to shared objects | schema contract tests; Serve state tests; Serve utility tests; import-order subprocess tests |
| `tests/unit_tests/test_serve_state_schema_contract.py` | object and identity characterization | focused pytest on this file before and after extraction |

## Performance evidence

Measure alternating subprocess cold imports of `sky.serve.serve_state` before
and after extraction. Characterization tests assert exactly one
`DatabaseManager`, one metadata graph, direct function aliases, and no wrapper.
Repository SQL is not edited, so query and transaction counts remain identical.

## Rollout and rollback

This is an internal structural extraction with no migration or feature flag.
Rollout is the normal package deployment after exact-head CI. Rollback is a
normal revert because database schema and formats do not change. The change
must remain unmerged if any relevant CI job is absent, skipped, or non-green.
