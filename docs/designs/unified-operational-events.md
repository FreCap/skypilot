# Unified Operational Events

Status: accepted for implementation

## Summary

SkyPilot will add a PostgreSQL-backed operational event plane for user-visible
resource lifecycle actions. The first end-to-end slice records terminal
cluster `launch`, `start`, `stop`, `down`, and `autostop` outcomes with a
durable actor snapshot, authoritative workspace, request correlation, stable
machine codes, safe messages, and resource targets.

The event plane is a product history and debugging surface. It does not replace
Datadog metrics, logs, traces, existing cluster status-transition events,
managed-job events, placement histories, or cost rollups.

The first public surfaces are:

- a synchronous, authorized `GET /events` API with keyset pagination;
- a version-gated Python SDK, including its async wrapper;
- `sky events` with filters, JSON output, pagination, and watch mode;
- a built-in event panel on active and historical cluster detail pages.

## Motivation and prior art

dstack has a coherent event product spanning a generic event model, emission
inside service transactions, authorization-aware listing, CLI filtering and
watching, a resource-scoped UI, and retention. SkyPilot has stronger request
correlation, PostgreSQL request durability, workspace RBAC, and actor types,
but its histories are separate implementation-specific tables. The existing
`cluster_events` table has no actor, workspace, stable event ID, general
target, cursor, or secure cross-workspace listing contract.

The useful concept to port is a unified resource event plane. SkyPilot should
not copy dstack's known authorization weakness where a multi-target event is
returned when the caller can see only one target. SkyPilot authorization is
based on the event's authoritative workspace and is applied in SQL before any
row is returned.

## Goals

1. Explain who performed a cluster lifecycle action, where, against which
   resource, with what terminal outcome, and under which API request.
2. Preserve history after requests, users, workspaces, or resources are
   deleted or renamed.
3. Emit exactly once for a live execution generation and commit the request
   terminal transition and event in one PostgreSQL transaction.
4. Remain correct across split API, executor, and controller replicas,
   executor lease loss, cancellation, recovery, and rolling upgrades.
5. Fail closed on authorization and sensitive data.
6. Provide one typed read contract shared by the SDK, CLI, and dashboard.
7. Bound storage with configurable retention and keyset pagination.

## Non-goals

- Copying Datadog metrics, logs, traces, monitors, or dashboards.
- Storing raw request payloads, task YAML, commands, environment variables,
  credentials, exception text, stack traces, provider identifiers, or return
  values.
- Replacing `cluster_events`, managed-job status events, Serve placement
  histories, request logs, or cost histories in this slice.
- Backfilling historical rows from existing event tables. Their actor and
  workspace cannot always be reconstructed authoritatively.
- Adding job, service, volume, image, workspace, or global-system event kinds
  in the first slice.
- Supporting the new store on the legacy SQLite/local request backend.
- Treating this operational history as a compliance audit log.

## Behavior contract

### Event kinds and outcomes

The first slice recognizes these request names and event kinds:

| Request name | Event kind | Primary target |
| --- | --- | --- |
| `sky.launch` | `cluster.launch` | cluster |
| `sky.start` | `cluster.start` | cluster |
| `sky.stop` | `cluster.stop` | cluster |
| `sky.down` | `cluster.down` | cluster |
| `sky.autostop` | `cluster.autostop` | cluster |

Only an active-to-terminal request transition emits:

- `SUCCEEDED` becomes outcome `succeeded`;
- `FAILED` becomes outcome `failed`;
- `CANCELLED` becomes outcome `canceled`.

Transitions to `PENDING`, `WAITING`, or `RUNNING` do not emit. In particular,
replayable controller handoff, executor lease recovery, broken worker-pool
retry, and `ExecutionRetryableError` do not emit until the live execution
generation reaches a terminal state.

Each event also carries a closed terminal cause. Initial causes are:

- `handler_succeeded`
- `handler_failed`
- `dispatcher_submit_failed`
- `explicit_cancel`
- `coroutine_disconnected`
- `graceful_shutdown_retry`
- `compatibility_restart`
- `controller_leadership_lost`
- `execution_lease_expired`
- `precondition_failed`
- `controller_reservation_conflict`

Messages are rendered from kind, outcome, and cause by server-owned code. They
never include arbitrary exceptions or request fields. Ambiguous interruption
causes use a fixed message that states the external outcome may be uncertain.

### Exactly-once scope

The uniqueness key is:

```text
(source_request_id, source_execution_generation, phase)
```

`phase` is `terminal` in this slice. The execution generation is included
because durable internal requests can be revived and because future resource
operations may safely reuse a logical request across reconciled attempts.

Retries that remain non-terminal produce no event. A stale or late executor
whose fenced request update affects zero rows produces no event. Event
insertion uses `ON CONFLICT DO NOTHING` as a final idempotency guard.

### Actor snapshot

The server snapshots:

- actor ID;
- display name;
- `UserType`: `system`, `basic`, `sa`, `sso`, or `legacy`;
- `unknown` only when an older or unauthenticated compatibility path did not
  provide a persisted type.

Actor fields never come from public event API inputs. Authenticated identity is
captured from `request.state.auth_user`; the service-account middleware must
preserve the persisted service-account type. The event read path never joins
the current users table, so deletion or renaming does not rewrite history.

### Workspace and target snapshot

An event requires an authoritative workspace. Request preparation creates a
closed, server-owned event context, but the worker persists the workspace only
after preferred/default workspace resolution and the normal RBAC check.
Failure to persist that context prevents an opted-in audited mutation from
starting.

A terminal request with no complete event context, such as an old request
created during a rolling upgrade, terminates normally and emits no fabricated
event. This is safer than trusting a client-supplied workspace.

The initial target has:

- type `cluster`;
- the durable cluster name;
- a cluster hash when it is authoritatively available;
- a snapshot display name.

Target IDs are nullable because a failed first launch may never create a
cluster hash. Target names and workspaces remain queryable after teardown.
There are no foreign keys from events to users, workspaces, requests, or
resources.

## Data model

Revision `004` in the PostgreSQL-only `api_requests` Alembic lineage adds one
nullable `api_requests.event_context JSONB` column and the following tables.
The context is validated by a closed Pydantic model before persistence and is
never returned directly.

All context and filter strings have explicit length bounds. Event kind, phase,
outcome, cause, actor type, and target type are closed enums. Actor IDs,
request IDs, workspace names, target IDs, and target names reuse their existing
SkyPilot limits or a stricter 256-character ceiling. Each repeated read filter
accepts at most 16 values. This prevents a client-controlled cluster name,
identity, or query list from becoming unbounded indexed event data.

### `resource_events`

| Column | Type | Contract |
| --- | --- | --- |
| `event_id` | UUID | application-generated primary key |
| `occurred_at` | TIMESTAMPTZ | database `clock_timestamp()` |
| `workspace` | TEXT | non-null authorization snapshot |
| `kind` | TEXT | stable machine code |
| `phase` | TEXT | `terminal` in v1 |
| `outcome` | TEXT | `succeeded`, `failed`, or `canceled` |
| `cause` | TEXT | closed machine code |
| `message` | TEXT | fixed safe rendering |
| `actor_id` | TEXT | immutable snapshot |
| `actor_name` | TEXT | immutable snapshot |
| `actor_type` | TEXT | closed actor type |
| `source_request_id` | TEXT | request correlation, no foreign key |
| `source_execution_generation` | BIGINT | idempotency and HA correlation |

The table has:

- a unique constraint on
  `(source_request_id, source_execution_generation, phase)`;
- an index on `(workspace, occurred_at DESC, event_id DESC)`;
- an index on `(workspace, actor_id, occurred_at DESC, event_id DESC)`;
- an index on `(source_request_id)`;
- a retention index on `occurred_at`.

### `resource_event_targets`

| Column | Type | Contract |
| --- | --- | --- |
| `event_id` | UUID | foreign key to the owning event, cascade on delete |
| `position` | SMALLINT | stable target order |
| `target_type` | TEXT | `cluster` in v1 |
| `target_id` | TEXT | nullable stable resource identity |
| `target_name` | TEXT | non-null display snapshot |

The primary key is `(event_id, position)`. Target lookup indexes cover
`(target_type, target_id, event_id)` and
`(target_type, target_name, event_id)`.

### Cursor authority

Migration `004` writes one random authority UUID under a namespaced key in
`api_request_store_metadata`. Every replica derives the same HMAC key from
that value and an event-specific salt. The authority is not exposed through
the API.

## Transactional emission

There is no outbox or database trigger. All live PostgreSQL terminal writers
route through a connection-aware helper in
`sky/server/requests/postgres.py`:

```python
_terminalize_locked_request(
    connection,
    request_row,
    *,
    status,
    cause,
    request_updates,
    extra_predicates=(),
    controller_action_state=None,
) -> bool
```

The caller owns `SELECT ... FOR UPDATE` inside `engine.begin()`. The helper:

1. verifies an active-to-terminal transition;
2. performs the guarded request update;
3. inserts the event and its targets when a complete opted-in context exists;
4. deletes durable queue delivery when appropriate;
5. updates controller action reservation state when appropriate;
6. returns true only if the guarded terminal transition occurred.

The request transition, event insert, queue deletion, and reservation update
commit or roll back together. Claim predicates remain caller-specific:

- normal executor completion retains generation, claim-token, instance, lease,
  and controller-generation fences;
- explicit cancellation, startup recovery, lease reaping, and controller
  fencing are intentionally unfenced but row-locked and active-status guarded.

The implementation routes these current writers through the helper:

1. normal success and failure;
2. stranded dispatcher failure;
3. explicit and cross-replica cancellation;
4. coroutine disconnect cancellation;
5. graceful-shutdown retry cancellation;
6. compatibility-process startup recovery;
7. stale controller generation for non-replayable work;
8. expired non-replayable execution claims;
9. precondition failure before claim;
10. controller action reservation conflict.

PostgreSQL `update_status_async()` rejects terminal statuses. Generic
`update_request()` contexts reject an active-to-terminal mutation, forcing new
terminal writers to choose a cause-aware path. Default implementations on the
abstract request backend preserve plugin and SQLite compatibility without
event emission.

`cutover.import_legacy_requests()` remains a data import, not a live terminal
transition. Existing terminal rows and imported `RUNNING` rows converted to
`CANCELLED` do not create events.

## Read API

### Endpoint

`GET /events` is synchronous and never enters the request queue. Query
parameters are:

- repeated `workspace`;
- repeated `kind`;
- repeated `outcome`;
- repeated `actor_id`;
- repeated `actor_type`;
- `target_type`;
- `target_id`, which requires `target_type`;
- `target_name`, which requires `target_type`;
- `request_id`;
- RFC3339 `since`;
- RFC3339 `until`;
- `direction`, either `older` (default) or `newer`;
- `limit`, default 50, minimum 1, maximum 100;
- opaque `cursor`.

`older` results are ordered by `(occurred_at DESC, event_id DESC)`. `newer`
results are ordered by `(occurred_at ASC, event_id ASC)`. Both directions fetch
`limit + 1` and return:

```json
{
  "items": [
    {
      "id": "uuid",
      "occurred_at": "2026-07-30T12:00:00Z",
      "kind": "cluster.launch",
      "phase": "terminal",
      "outcome": "succeeded",
      "cause": "handler_succeeded",
      "message": "Cluster launch succeeded.",
      "workspace": "research",
      "actor": {
        "id": "abc123",
        "name": "alice@example.com",
        "type": "sso"
      },
      "request_id": "request-uuid",
      "execution_generation": 1,
      "targets": [
        {
          "type": "cluster",
          "id": "cluster-hash",
          "name": "trainer"
        }
      ]
    }
  ],
  "next_cursor": "opaque-or-null",
  "poll_cursor": "opaque-newer-cursor",
  "has_more": false
}
```

The HMAC cursor binds:

- cursor version and scope;
- principal ID and admin state;
- sorted effective workspace set;
- normalized filters;
- traversal direction;
- the last `(occurred_at, event_id)` key.

The server recomputes authorization on every page. A changed principal,
permission set, or query returns HTTP 409 with
`STALE_OPERATIONAL_EVENT_CURSOR`. Malformed filters return 422. Cursors cannot
be shared across users or widened into another query.

`next_cursor` continues in the requested direction. `poll_cursor` is always a
`newer` cursor anchored to the newest row returned. If the initial result is
empty, it is anchored to the database's current timestamp, so a watcher can
start without a list-then-poll race. A `newer` caller drains pages while
`has_more` is true, then retains the last `poll_cursor` for its next poll.
This is the sole watch primitive; clients do not poll by rounded timestamps or
maintain an unbounded de-duplication set.

### Authorization

For authenticated non-admin callers, the server computes accessible
workspaces once and intersects them with requested workspace filters. The SQL
query always includes `workspace IN effective_workspaces`; filtering never
happens after retrieval. An unauthorized or nonexistent requested workspace
produces an empty intersection rather than revealing its existence.

Admins can read all event workspaces. Authentication-disabled single-user
servers retain their existing trusted-local behavior. The endpoint is added
to the viewer read allowlist.

On a server that does not use the PostgreSQL request backend, `/events`
returns HTTP 503 with closed code `OPERATIONAL_EVENTS_UNAVAILABLE`. It does not
silently return an empty history. The SDK and CLI render a direct explanation
that operational events require a PostgreSQL-backed API server. The dashboard
renders the same unavailable state instead of an error toast or a misleading
empty panel.

Multi-target events added later must be authorized only if every target's
workspace is within the effective set. The v1 schema has one event-level
workspace and therefore cannot accidentally use "any visible target"
semantics.

## SDK and CLI

API version increases from 63 to 64 and defines
`MIN_OPERATIONAL_EVENTS_API_VERSION = 64`.

The typed direct client is exposed as `sky.events.list(...)` and as
`sky.client.sdk.list_events(...)`. Both use
`@versions.minimal_api_version(64)` and return an `EventsPage`. The async SDK
uses `asyncio.to_thread`. A new client against an older server raises
`APINotSupportedError`; it never falls back to the under-specified
`/cluster_events` endpoint. Old clients against a new server are unchanged.
API version support and backend availability are separate: version 64 on a
legacy local backend produces the closed unavailable response described above.

`sky events` supports:

- `--cluster`;
- repeated `--workspace`, `--kind`, `--outcome`, `--actor`, and
  `--actor-type`;
- `--request-id`;
- `--since` and `--until`, accepting RFC3339 values and documented relative
  durations;
- `--limit`;
- `--cursor`;
- `--watch`;
- `--format table|json`.

The table columns are TIME, KIND, TARGET, OUTCOME, ACTOR, WORKSPACE, REQUEST,
and MESSAGE. Normal output is newest first. JSON preserves the complete typed
response. Watch mode prints the initial page in chronological order, then
uses `poll_cursor` with `direction=newer`, drains every `has_more` page before
sleeping, and prints each event once. JSON watch mode is newline-delimited JSON.
Ctrl-C exits cleanly. `--watch` rejects `--until` and an older-page `--cursor`
because either would make its live boundary ambiguous.

## Dashboard

The dashboard client API version increases to 64. A new direct connector uses
`apiClient.get`, not the request-ID polling client.

The existing `clusters.detail.events` plugin slot gains a core fallback panel
and receives cluster hash, name, workspace, and historical state. The panel:

- requests the latest 20 authorized cluster events;
- shows time, action, outcome, actor, request ID, and safe message;
- uses server cursors for "load more";
- cancels stale requests when navigation changes;
- renders a clear empty state;
- keeps plugin replacement behavior intact.

The dashboard does not replace its event response with `cluster_events` data
and does not fabricate operational events from cluster creation timestamps.
A cached version-63 dashboard does not know about the panel; a version-64
dashboard receiving a 404 renders an upgrade/reload state rather than silently
showing unscoped legacy history.

## Retention

`api_server.operational_event_retention_hours` defaults to 720 hours. A
negative value disables deletion. A controller-role distributed singleton
deletes events older than the cutoff in bounded batches once per hour; target
rows cascade. Deletion uses the retention index and commits between batches.

Retention failure logs a safe error and retries on the next interval. It never
affects request execution or event reads.

## Backward compatibility and migration

- Revision `004` is additive and PostgreSQL-only.
- `API_REQUESTS_VERSION` becomes `004`.
- Request context is nullable so revision-003 binaries can continue explicit
  writes against a revision-004 schema during binary rollback.
- Old binaries ignore the new tables and column.
- New binaries require migration `004` before enabling the PostgreSQL request
  backend.
- Rows with null or invalid event context terminalize without emission.
- The existing request, cluster-event, job-event, and dashboard contracts are
  not changed.
- Downgrade drops the event tables and request context only after verifying
  there are no event rows. Normal operational rollback leaves revision `004`
  in place and rolls back the binary.

## Rollout

1. Apply migration `004` through the Helm-owned migration job.
2. Deploy the new image to the isolated HA release with `--reuse-values`.
3. Verify all API, executor, and controller replicas report the exact image
   digest and commit.
4. Run lifecycle canaries for one success, failure, cancellation, and
   executor-loss/recovery case.
5. For each canary, prove the request terminal state and one matching event
   share request ID, execution generation, actor, workspace, and target.
6. Verify a user cannot read another workspace's events and an admin can.
7. Verify CLI pagination/watch and the cluster detail panel.
8. Monitor migration, API 4xx/5xx, request terminalization, event insert,
   cursor-staleness, retention, and replica health signals.
9. Roll back the binary, not the additive schema, if the canary fails.

No compatibility path is removed in this rollout. Removal is considered only
after a production observation window proves old binaries and request rows no
longer depend on it.

## Test plan

### Unit and model tests

- closed kind, outcome, cause, actor, target, and cursor validation;
- safe message rendering contains no arbitrary error or payload value;
- HMAC cursor tamper, principal, workspace, permission, and filter binding;
- API version gate and typed response parsing;
- CLI filters, table/JSON output, watch de-duplication, and Ctrl-C;
- dashboard loading, empty, error, pagination, stale-navigation, and plugin
  replacement states.

### Real PostgreSQL tests

- migration creates the context column, tables, constraints, indexes, and
  cursor authority;
- normal success and failure commit request, queue/reservation changes, event,
  and target exactly once;
- injected event-insert failure rolls the transaction back;
- stale fenced completion and non-terminal retry emit nothing;
- explicit, coroutine, and graceful-shutdown cancellation carry distinct
  causes;
- compatibility restart, expired lease, controller handoff, precondition
  failure, and reservation conflict emit exactly once;
- replayable lease/controller recovery returns to `WAITING` without an event;
- null-context rolling-upgrade rows and cutover imports emit nothing;
- user, service-account, system, and unknown actor snapshots survive deletion;
- SQL-side workspace isolation and admin behavior;
- request GC does not delete events;
- retention deletes only rows older than the cutoff and cascades targets.

### Compatibility and live tests

- revision-003 code can read and write requests against revision `004`;
- new client plus old server fails at the local version gate;
- old client plus new server remains unchanged;
- all six split-role pods remain ready during lifecycle canaries;
- API traffic remains successful during executor and controller pod eviction;
- event counts remain exactly one per terminal request generation.

## Alternatives considered

### Extend `cluster_events`

Rejected. Its key, retention, API, and consumers encode status-transition and
provision-debug semantics. Adding actor, workspace authorization, generic
targets, and cursors would overload it and still not cover other resources.

### Emit after request completion

Rejected. A second transaction can lose the event during executor or database
failure and cannot prove exact correlation with terminal state.

### PostgreSQL trigger

Rejected for this slice. A trigger hides cause-specific lifecycle semantics,
cannot validate the server-owned typed event context as clearly, and makes
administrative rewrites easier to misclassify as live actions. The explicit
helper provides the same transaction boundary while making every terminal
cause and fence testable in Python.

### Outbox and event consumer

Rejected. The event and request tables share one database. An outbox adds a
consumer, lag, health surface, and retention burden without improving the
atomic guarantee.

### Datadog as the product event store

Rejected. Datadog remains the operational telemetry backend, but product
history needs workspace authorization, stable API semantics, request
correlation, user-facing retention, and deterministic CLI/dashboard behavior.
