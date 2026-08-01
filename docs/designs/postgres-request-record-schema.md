# PostgreSQL request record schema

_Created: 2026-08-01_

## Problem

`sky/server/requests/postgres.py` owns both the declarative PostgreSQL record
schema and the stateful runtime that operates on it. The schema is a low-state
contract used by request persistence, queue delivery, controller leadership,
server liveness, migrations, and SQLite-to-PostgreSQL cutover. The runtime owns
thread, transaction, advisory-lock, lease, and recovery ordering. Keeping both
in one 2,408-line module mixes a stable record contract with several stateful
state machines and makes schema changes harder to review in isolation.

The module also contains valid but riskier seams: server-instance liveness,
controller leadership, `RequestBackend`, and `QueueBackend`. Those components
share engine initialization, transaction boundaries, lock predicates, and
recovery transitions, so they are not part of this extraction.

## Goals

Give the PostgreSQL record contract one owner while preserving every existing
import path, SQLAlchemy object identity, table shape, migration input, query,
transaction, and runtime call count. Keep `postgres.py` as the public facade
and leave all runtime behavior unchanged.

## Background

The current responsibilities are:

| Responsibility | Callers | Dependencies and state | Failure modes | Performance and change cadence |
|---|---|---|---|---|
| Declarative request record schema | PostgreSQL request and queue backends, cutover, migrations, event storage | SQLAlchemy metadata, PostgreSQL types, table and column identity; no mutable runtime state | Schema drift, incompatible migrations, changed table identity | Import-time construction only; changes with persisted record format |
| Server-instance liveness | API runtime, readiness middleware | Environment identity, heartbeat thread, database clock, readiness state | False readiness, stale leases, drain-order drift | Periodic heartbeat hot path; changes with deployment topology |
| Controller leadership and fencing | API runtime, jobs state, physical-capacity projector | Advisory locks, async sessions, generations, controller reservations | Split brain, stale ownership, leaked locks | Transaction and heartbeat sensitive; changes with HA ownership |
| Request persistence and recovery | `RequestBackend` callers | Request serialization, transactions, event emission, recovery policy | Lost or duplicated transitions, recovery drift | Query and transaction hot paths; changes with request lifecycle |
| Durable queue delivery | executor and queue factory callers | Claims, preconditions, leases, controller fences | Duplicate delivery, starvation, ambiguous mutation replay | Polling and claim hot paths; changes with scheduling and delivery policy |

Recent history reinforces the separation: durable request delivery, split API
roles, controller leadership, jobs ownership fencing, HA rollout guards, and
operational events landed as distinct commits. The schema declarations are the
only complete low-state leaf shared by all of them.

## Solution

Move the six SQLAlchemy table declarations, their shared metadata, and the
synthetic `pg_catalog.pg_locks` table into
`sky/server/requests/postgres_schema.py`. Import and directly alias those exact
objects from `postgres.py`. Direct aliases keep historical callers such as
`cutover.py` working and preserve object identity without wrappers, factories,
registries, or dependency injection.

Add a pure characterization test before the move. It pins the complete table
and column topology, shared metadata ownership, and the synthetic lock-table
shape without requiring Docker or a live database. Existing real-PostgreSQL
tests continue to cover migration shape and runtime behavior.

## Alternatives considered

Moving row codecs with the declarations would broaden a schema-only module
into request-model translation for little additional value. Moving server
liveness or controller leadership would require relocating engine and lock
ownership or adding callback plumbing. Moving either backend would split
transactional state machines across modules. Leaving the file unchanged avoids
one module, but retains a stable declarative contract inside a rapidly changing
runtime implementation despite an established sibling schema-module pattern.

## Rollout and rollback

This is an import-time structural change with no data migration or runtime
flag. Rollback is the inverse source move. A mismatch in table identity or
shape is a release blocker.

## Test plan

Run the schema characterization before and after the move. Then run the full
PostgreSQL request test module, request cutover tests, API runtime tests, and
request executor tests. Run `bash format.sh --files` on every changed Python
file, static checks selected by that command, import-order and import-time
checks, `git diff --check`, and inspect pull-request workflow path filters. The
manual check is importing both modules and proving each historical facade
object is the exact schema-module object.
