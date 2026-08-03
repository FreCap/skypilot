# Request wire projection ownership

## Context

`sky/server/requests/requests.py` is 2,684 lines and owns several request
subsystems: the in-memory request model, durable SQLite/PostgreSQL row
representations, client-wire serialization, query filters, backend lifecycle,
startup recovery, cancellation, and request-log garbage collection. The line
count is only a prioritization signal. This change is justified by the
client-wire family having a complete stateless boundary and different callers,
dependencies, failure modes, and reasons to change from request persistence.

## Responsibility map

### Request model and durable representations

- Callers: SQLite and PostgreSQL request backends, executor, recovery, cutover,
  and plugins implementing `RequestBackend`.
- Dependencies: request registry, durable payload formats, database columns,
  entrypoint and body identity, queue metadata, execution claims, and event
  context.
- State owned: the `Request` dataclass and its durable/legacy row fields.
- Failure modes: incompatible persisted rows, lost queue intent, changed pickle
  identity, corrupt payloads, or unsafe terminal transitions.
- Performance sensitivity: row serialization and deserialization are on request
  persistence hot paths.
- Change cadence: backend migration, execution, recovery, and schema evolution.

### Client-wire and display projection

- Callers: SDK result polling, `/api/status`, request-table display, smoke-test
  helpers, and old/new client compatibility paths.
- Dependencies: `RequestPayload`, client API version, return-value codecs,
  entrypoint/body codecs, user-name lookup, and the historical request logger.
- State owned: none. It projects a `Request` to and from client payloads.
- Failure modes: wire incompatibility, exposing large or private values in list
  views, failing to tolerate a newer entrypoint on an older client, changing
  the WAITING compatibility downgrade, or adding user queries.
- Performance sensitivity: list projection must keep one batched user lookup;
  full encode/decode must not add codec calls or copies.
- Change cadence: API compatibility, client display, and serializer evolution.

### Request persistence and query backends

- Callers: executor, preconditions, server endpoints, recovery, GC, and plugins.
- Dependencies: the existing `RequestBackend` seam, SQLite/file locks,
  PostgreSQL, transactions, filters, and request status transitions.
- State owned: database connections, rows, locks, leases, and transitions.
- Failure modes: duplicate execution, stale full-row writes, lock races, query
  regressions, or backend divergence.
- Performance sensitivity: polling, claims, and guarded terminal writes.
- Change cadence: HA execution, storage cutover, and backend correctness.

### Process lifecycle, recovery, cancellation, and log retention

- Callers: API startup/shutdown, daemon runner, executor, and disk-pressure GC.
- Dependencies: processes, signals, locks, cluster state, filesystem usage,
  retention policy, and background threads.
- State owned: worker processes, startup recovery decisions, log files, and GC
  cadence.
- Failure modes: dropped work, duplicate launch replay, unsafe cancellation,
  deleted active logs, or event-loop stalls.
- Performance sensitivity: startup time, filesystem scans, and daemon latency.
- Change cadence: reliability and operational policy.

## Chosen seam

Keep `Request`, `RequestStatus`, `ScheduleType`, `_status_value_for_client`, and
`encode_requests` in `sky.server.requests.requests`. Their signatures, module
and qualified names, public import paths, and class identity remain unchanged.
Move only the implementations of full wire encode/decode, entrypoint decode,
version-aware status projection, and batched display projection to a plain
`request_wire` module. The façade passes late-bound codec, version, user lookup,
logger, enum, and placeholder dependencies at call time so historical patch and
subclass seams remain intact.

This is a façade-first plain-function extraction. A class, protocol, registry,
factory, strategy, or dependency-injection layer would add an unproven second
implementation. Moving the `Request` dataclass itself would risk serialized and
pickled identity. Moving durable row conversion with client-wire conversion
would retain two different formats and reasons to change in the extracted file.

## Behavior contract

- Preserve all historical imports, signatures, modules, qualified names, and
  `Request` identity.
- Preserve `RequestPayload` fields and JSON/pickle codec ordering.
- Preserve the one batched `get_all_users()` call, including the historical
  call for an empty request list.
- Preserve WAITING-to-RUNNING downgrade behavior for older clients on full and
  display encodes while persisting the true status in database rows.
- Preserve `Request._decode_entrypoint()` fallback identity and subclass use of
  that method from `Request.decode()`.
- Preserve validation, error logging through the historical logger, exception
  types, and error propagation.
- Add no database query, transaction, lock, network, filesystem, or process
  operation.

## Milestones

1. Add characterization for façade identity, wire round-trip behavior,
   compatibility downgrade, late-bound user/version/decoder seams, and exact
   lookup counts. Run it on the unmodified implementation.
2. Add `request_wire.py` and replace method/function bodies with façade calls.
3. Run the focused wire tests, request component suites, SDK result tests,
   formatting, static analysis, import checks, and diff checks.
4. Compare cold import and representative encode/display loops against the
   exact base. Publish only if no material regression is observed.

## Changed-path-to-test matrix

| Changed path | Responsibility | Tests |
| --- | --- | --- |
| `sky/server/requests/request_wire.py` | Full wire encode/decode and display projection | new wire contract; request tests; SDK request-result tests |
| `sky/server/requests/requests.py` | Historical façade and model methods | new façade identity/patch contract; request DB hot-path tests; PostgreSQL request tests |
| `tests/unit_tests/test_sky/server/requests/test_request_wire_contract.py` | Characterization | run unchanged before and after extraction |
| `docs/designs/request-wire-projection.md` | Canonical contract | formatting and diff checks |

## CI mapping and rollout

The PR must verify that pull-request workflow filters include the two production
paths and the new test. Relevant unit, API, compatibility, and static-analysis
jobs must run on the exact final head. This is an internal structural change
with stable façades and no schema, protocol, CLI, remote-command, or lifecycle
change, so no staged runtime rollout is required. Leave the PR open if any
relevant check is absent, pending, skipped, neutral, or failing.
