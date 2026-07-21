# Shared Auth Sessions

## Problem

The CLI browser-login flow currently stores its short-lived authorization
session in `~/.sky/api_server/requests.db` with direct SQLite statements. This
bypasses `SKYPILOT_DB_CONNECTION_URI`, even when the remote API server uses
PostgreSQL for centralized state.

The Helm chart deliberately gives every API pod an ephemeral `~/.sky` during a
rolling update so SQLite is not run on shared NFS storage. An authorization
request handled by one pod is therefore invisible to a polling request handled
by another pod. The CLI reports `Session not found` until its session expires,
despite the user having authorized it successfully. The same split can occur in
multi-replica deployments.

## Behavior Contract

- Remote API servers configured with PostgreSQL store auth sessions in the
  existing global-state database, making them visible to every API replica.
- Local API servers retain their supported local SQLite global-state database.
  This is local compatibility, not a second central storage implementation.
- Creating the same `code_challenge` again replaces its token and creation
  timestamp atomically.
- Polling atomically consumes at most one unexpired session. Concurrent polls
  cannot both obtain the token.
- Expired sessions are never returned. Creation opportunistically deletes
  expired sessions so abandoned rows remain bounded.
- Existing sessions in the legacy request SQLite file are not migrated. They
  live for only `AUTH_SESSION_TIMEOUT_SECONDS`; users authorizing during an
  upgrade may need to retry once.
- Tokens remain transient plaintext values, matching the existing behavior.
  They are deleted on successful polling and aged out by the existing timeout.

## Design

Define `auth_sessions` in the global user-state SQLAlchemy metadata with:

- `code_challenge TEXT PRIMARY KEY`
- `token TEXT NOT NULL`
- `created_at REAL NOT NULL`

Add the table through the next global user-state Alembic revision. The auth
store obtains the existing global user-state engine, so PostgreSQL deployments
use the configured central database and local API servers use the established
local engine.

Use dialect-native SQLAlchemy upserts for creation, because both PostgreSQL and
the supported local SQLite backend need atomic replacement. Use a single
`DELETE ... RETURNING token` statement for polling so one-time consumption is
atomic in both supported dialects. Keep these synchronous transactions behind
the endpoint's existing `asyncio.to_thread()` boundary.

The store accepts an engine provider for tests. Production uses
`global_user_state.initialize_and_get_db`, while tests can create independent
store objects over one database to model separate workers and replicas.

## Alternatives

### Keep the request SQLite file

This preserves the bug because rolling-update pods intentionally do not share
`~/.sky`. Sharing the file on NFS would contradict the chart's explicit SQLite
safety boundary.

### Add a dedicated auth database manager

PostgreSQL would still use the same physical database, but local mode would
gain another SQLite file and another initialization path. Auth sessions belong
to API-server identity state, so the existing global-state engine and migration
chain are the smaller ownership boundary.

### Store sessions in memory or use sticky routing

Memory remains replica-local. Sticky routing adds ingress coupling and does not
cover rolling updates or callers that change connections. Neither provides the
database-level atomic consume guarantee.

### Require PostgreSQL for every API server

That is a broader deployment change and would break the supported local API
server. This change removes the central SQLite bypass without expanding into a
full local-state migration.

## Milestones

1. Add the global-state table and Alembic revision.
2. Replace direct SQLite auth-session statements with SQLAlchemy transactions.
3. Add regression tests for shared visibility, atomic consumption, overwrite,
   expiry, migration creation, and PostgreSQL statement shape.
4. Run focused auth and migration tests, static analysis, formatting, and the
   complete pull-request check set.

## Rollout And Compatibility

The migration is additive and runs through the existing global-state database
initialization. New code stops reading the legacy request SQLite table. Mixed
old and new pods during a rolling update do not share sessions across the old
and new implementations, so a login started during that short window may need
one retry. No long-lived data is lost.

Rollback is safe for the rest of global state. Old code ignores the new table
and resumes using the legacy SQLite file. The additive table can remain.

## Test Plan

- Verify the migration creates `auth_sessions` with its primary key and
  non-null columns.
- Verify two store instances backed by the same database can create and poll
  the same session.
- Race concurrent polls and assert exactly one receives the token.
- Verify duplicate authorization replaces the token.
- Verify expired sessions are neither returned nor retained after a later
  creation.
- Compile the upsert and atomic consume statements for PostgreSQL to prevent a
  SQLite-only implementation from passing local tests.
- Run the auth endpoint and session unit tests, global-state migration tests,
  BasedPyright, Ruff async checks, `format.sh`, and all visible PR checks.

## Manual Verification

Deploy two API replicas with `SKYPILOT_DB_CONNECTION_URI` configured. Start CLI
login, authorize in the browser, and repeatedly send the polling request through
the load-balanced API service. The first valid poll must return the token and
all later polls must return 404, regardless of which pod serves each request.
