# Global User State Schema Boundary

_Created: 2026-08-01_

## Problem

`sky/global_user_state.py` constructs the global SQLAlchemy metadata and every
table used by cluster, user, storage, volume, authentication, configuration,
estimated-spend, and operator-notification persistence.  The same module also
implements all repositories, transaction lifecycles, row projections, event
retention, and database-capacity queries.  Schema identity and repository
behavior therefore change for different reasons and have materially different
callers, dependencies, state, and failure modes.

The module is 4,483 lines on `origin/improvements`.  Its size is only a
prioritization signal.  The useful seam is the complete, declarative schema
construction block, not an arbitrary line-count split and not one of the
stateful repositories.

## Responsibility map

### SQLAlchemy schema construction and identity

Callers are Alembic-style global-user-state migrations, auth-session storage,
estimated-spend queries, physical-capacity source queries, container-image
transactions, tests, and every repository in `global_user_state.py`.
Dependencies are SQLAlchemy declarative metadata, PostgreSQL and SQLite-neutral
column types, and the default-workspace constant.  It owns process-wide
`Base.metadata` and `Table` identities, table names, columns, constraints,
indexes, and defaults.  Failures include duplicate metadata registration,
changed table identity, migration drift, schema-order drift, and import cycles.
Import time and one-time metadata construction are performance-sensitive.  Its
change cadence follows database schema and migration evolution.

### Database engine, migration, and session lifecycle

Callers are all synchronous and asynchronous repositories plus server startup.
Dependencies are `DBManager`, migration utilities, retry policy, SQLAlchemy
sessions, and configuration.  It owns engine selection, pool lifecycle,
session ownership, commits, rollbacks, and migration ordering.  Failures include
pool exhaustion, partial transactions, wrong database selection, and broken
test monkeypatches.  Query and connection counts are performance-sensitive.  It
changes with deployment topology and transaction policy.

### Domain repositories and projections

Callers span cluster orchestration, managed jobs, SkyServe, storage, volumes,
authentication, workspaces, dashboard endpoints, and billing projections.
Dependencies include serialized backend handles, domain models, registries,
status enums, YAML, configuration, metrics, and container-image validation.  It
owns row mutation and projection contracts, lifecycle ordering, event history,
and compatibility with legacy rows.  Failures include corrupted handles,
incorrect lifecycle transitions, stale reads, row-shape drift, and extra
queries.  Hot paths are query-count and copy sensitive.  Each repository family
changes with its domain behavior.

## Solution

Move only `Base` and the complete SQLAlchemy `Table` declarations to
`sky/global_user_state_schema.py`.  `sky.global_user_state` imports and directly
re-exports those exact objects, so historical imports, migration imports,
metadata identity, table identity, serialized formats, and all repository call
sites remain unchanged.

Keep `ClusterEventType`, container-image validation, database manager and
initialization, `create_table`, sessions, repositories, row projection, and
retry decorators in `sky/global_user_state.py`.  No forwarding functions,
classes, protocols, registries, dependency-injection layers, or package
hierarchy are introduced.

The extraction is structural only.  Table declarations remain byte-for-byte
equivalent apart from their module location, and no database behavior or schema
is intentionally changed.

## Alternatives considered

Splitting a domain repository such as notifications, cloud-check persistence,
volumes, or cluster events would require moving or injecting the shared table,
database-manager, transaction, logger, retry, and monkeypatch seams.  That adds
indirection or changes runtime globals for a smaller ownership improvement.

Moving schema declarations one domain at a time would fragment one metadata
owner and make import order significant.  Moving all repositories into a new
package would be a broad redesign rather than one bounded extraction.

Leaving the file unchanged avoids churn, but retains a stable declarative leaf
inside the highest-change persistence facade even though external callers
already consume the schema objects independently from repository behavior.

## Milestones

### v0: characterize the public schema contract

Add tests for the exact metadata table set, direct object identity exposed from
`sky.global_user_state`, migration compatibility, table columns, constraints,
indexes, server defaults, and dialect-neutral compilation.

### v1: extract schema construction

Move the complete table family to the schema module and directly import the
objects into the historical facade.  Do not change declarations or repository
implementations.

### v2: validate and roll out

Run the global-user-state, migration, auth-session, estimated-spend,
physical-capacity, operator-notification, container-image PostgreSQL, Jobs and
Serve integration, and import-contract tests.  Run formatting, mypy, Pylint,
Ruff, BasedPyright, compile and diff checks.  Compare cold import and metadata
construction performance.  CI must execute the mapped suites on the exact
pushed SHA before normal merge.

Rollback is a normal revert because the database schema and persisted data are
unchanged.
