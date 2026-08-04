# Global User State Operator Notification Boundary

_Created: 2026-08-01_

## Problem

`sky/global_user_state.py` is 4,126 lines after its declarative schema was
extracted. It still combines the process-wide database and migration lifecycle,
cluster persistence, cloud-check caches, storage and volume persistence,
authentication and configuration repositories, and the operator-notification
repository. Size is only a prioritization signal. The useful seam is the
complete operator-notification SQL and projection family, not an arbitrary
line-count split.

## Responsibility map

### Database engine, migration, and shared session lifecycle

Callers are server startup and every global-user-state repository. Dependencies
are `DatabaseManager`, migration utilities, retry policy, SQLAlchemy engine and
session construction, and runtime configuration. It owns the process-wide
engine, connection pool, schema initialization, caller-supplied session reuse,
and the historical `_db_manager` monkeypatch seam. Failures include pool
exhaustion, wrong database selection, migration-order drift, nested-connection
deadlocks, and broken tests or plugins that replace the facade manager. Query,
connection, and transaction counts are performance-sensitive. It changes with
deployment topology and database policy.

### Cluster, cloud-check, storage, token, and configuration repositories

Callers span cluster orchestration, managed jobs, SkyServe, storage and volume
commands, authentication, workspaces, the dashboard, and billing. Dependencies
include serialized backend handles, models, registries, status enums, YAML,
configuration codecs, and container-image validation. It owns domain mutations,
row projections, lifecycle events, legacy-row compatibility, and shared
transactions. Failures include corrupted state, invalid lifecycle transitions,
stale projections, serialization drift, and extra queries. Each repository
family changes with its product domain.

### Operator-notification persistence and projection

Callers are the low-cardinality notification recorder in
`sky.utils.operator_notifications` and two FastAPI dashboard endpoints.
Dependencies are three notification tables, SQLite and PostgreSQL conflict
inserts, SQLAlchemy expressions, and a caller-provided engine. It owns atomic
sequence allocation, incident coalescing, per-user monotonic cursors, lookback
filtering, and the returned dashboard projection. Failures include duplicate
sequence allocation, lost occurrence counts, stale messages replacing current
ones, cursor rollback, future cursor acknowledgement, and dialect drift. Its hot
path is database-bound; query and transaction counts must remain identical. It
changes with notification semantics and dashboard presentation rather than
cluster or database lifecycle policy.

## Solution

Add `sky/global_user_state_notifications.py` as a plain repository module. Move
the dialect selector, sequence allocator, SQL transactions, and row projection
into engine-accepting functions in that module. Keep the three historical
public functions in `sky.global_user_state` as the stable facade. They retain
their decorators, signatures, validation order, default-time behavior, module
identity, and `_db_manager` lookup, then delegate once with the resolved engine.

The facade directly aliases the two existing private helpers for compatibility,
but no new class, protocol, registry, factory, dependency-injection framework,
or package hierarchy is introduced. The engine parameter is the complete
repository boundary: the extracted module neither owns nor discovers the
process-wide database manager. Schema objects remain direct aliases from
`sky.global_user_state_schema`.

The extraction is structural only. SQL statements, transaction boundaries,
commit ordering, result shapes, retry and metrics decorators, table identities,
serialized formats, and API responses do not intentionally change.

## Alternatives considered

Moving the public functions as direct aliases would make their globals resolve
in the implementation module. That breaks the historical
`global_user_state._db_manager` replacement seam or requires hidden mutable
dependency registration.

Letting the implementation import `sky.global_user_state` lazily would create a
circular dependency and violate the module-scope import rule. Moving the shared
database manager would affect every repository and is not a bounded leaf.

Leaving the code in place avoids the small facade call but retains one complete
repository with distinct callers, dependencies, state, failure modes, and
change cadence in the broad persistence facade. The additional Python call is
negligible next to an engine lookup and database transaction, and must be
measured.

## Milestones

### v0: characterize the facade and repository behavior

Add a contract test for public signatures, module and qualified names,
decorator placement, helper availability, facade database-manager replacement,
result shape, and engine lookup counts. Run it against the unmodified
implementation before moving behavior.

### v1: extract the repository

Move only the notification SQL and projection implementation. Keep validation,
time defaults, decorators, database-manager lookup, and public entrypoints in
the facade.

### v2: validate and roll out

Run the contract and notification repository suites, notification utility and
server endpoint tests, failover and autoscaler callers, global-user-state schema
and database tests, formatting, static analysis, import contracts, compile, and
diff checks. Compare SQL statement and commit counts plus representative call
timing. Verify the pull-request workflows collect every mapped suite on the
exact pushed SHA before normal merge.

Rollback is a normal revert because schemas, data, and public contracts remain
unchanged.
