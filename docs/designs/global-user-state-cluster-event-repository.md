# Global User State Cluster Event Repository

## Context

`sky/global_user_state.py` is a 3,637-line compatibility facade that owns the
shared database lifecycle plus cluster, event, history, usage, credential, and
SSH-key state.  Several table repositories already live behind this facade,
but cluster-event SQL still occupies about 487 lines and changes for event
observability, retention, Kubernetes lifecycle timing, Jobs history, and
dashboard launch progress.

Line count is only a prioritization signal.  The extraction is justified by a
complete table boundary with callers, dependencies, state, and failure modes
that differ from the surrounding cluster lifecycle orchestrator.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Shared database lifecycle and public facade | API startup, migrations, tests, every global-state consumer | `DatabaseManager`, sessions, schema metadata, retry and timing decorators | Engine, pool, table identity, migration and historical patch seams | Wrong database, schema drift, connection exhaustion, compatibility break | Cold import, engine lookups, connection and transaction counts | Database topology and compatibility |
| Cluster-event persistence and projection | Core, Cloud VM backend, Kubernetes provisioner, Jobs, API request recovery, debug dump, dashboard status reads | Cluster and event tables, SQLAlchemy dialects and windows, request IDs, regular expressions | Durable event rows, starting and ending status, reason, request ID, ordering | Generation-stale write, duplicate drift, missing cluster, ordering drift, retention loss, SQLite parameter overflow | Hot status reads, one-connection dedupe, batched query counts, bounded history materialization | Observability, lifecycle diagnostics, Jobs history and retention |
| Cluster lifecycle, identity, handle, usage and history orchestration | CLI and SDK cluster operations, refresh, Serve, Jobs and dashboard | Serialized handles, resources, locks, cluster and history tables, image ownership | Cluster identity, status, handles, usage intervals, metadata and history | Stale generation writes, invalid transitions, serialization drift, lock races | Refresh latency, lock duration, query and serialization counts | Cluster lifecycle and provider behavior |
| Event-retention scheduling policy | API runtime | Live config reload, logger, cancellation and sleep | Process task lifecycle and configured retention windows | Lost cancellation, busy loop, stale config, one event class retained incorrectly | One bounded sweep per configured interval | API runtime and operator policy |
| Remaining credential and SSH-key repositories | Authentication, launch and SSH consumers | Key material, encryption and serialization | Credential and SSH rows | Corruption, replacement or serialization drift | Login and launch latency | Security and credential policy |

The event repository has materially different readers, dependencies, durable
state, failure behavior, and reasons to change from cluster-row orchestration.
Its append path can still own a complete transaction: the cluster row and
status snapshot, generation fence, duplicate lookup, insert, and commit remain
inside one repository call and one session.

## Decision

Add `sky/global_user_state_cluster_events.py` as a plain function repository.
Keep every historical entrypoint, enum, decorator, signature, module identity,
and monkeypatch path in `sky.global_user_state`.

The facade passes late-bound engine, session, dialect, table, logger, clock,
request-ID, and helper dependencies explicitly.  It retains cross-table name
resolution before event listing and retains the retention daemon so config and
process scheduling do not move into a SQL repository.  The repository owns:

- append, generation fencing, duplicate detection, and unique-conflict policy;
- point, terminal-priority, batched latest, and status-age reads;
- ordered event listing by hash and bounded listing by persisted names; and
- retention deletion and its existing count-before-delete transaction.

This is facade-first decomposition with a module and functions.  A class,
protocol, registry, strategy, or dependency-injection container would invent a
second implementation.  Direct aliases are unsuitable because the historical
facade's globals are intentional patch seams and the retention daemon calls
the facade cleanup function.  Thin wrappers preserve those seams while moving
all event-table SQL and projection ownership.

## Behavior contract

- `ClusterEventType`, public imports, signatures, decorator depth, help and API
  behavior remain unchanged.
- Event append reads cluster hash and starting status together, applies the
  optional generation fence, and performs duplicate lookup in the same session
  before one insert and commit.  Unsupported dialects, missing clusters, unique
  conflicts, request IDs, timestamps, and rollback behavior remain unchanged.
- Caller-supplied sessions for `get_last_cluster_event` never acquire another
  engine or connection.  The duplicate path continues to call the historical
  facade helper so monkeypatching remains effective.
- Terminal events retain priority over newer status-change events.  All window
  ordering, empty-input short circuits, chunk sizes, result projections, and
  oldest/newest ordering remain byte-for-byte equivalent in meaning.
- Name-based history remains queryable after cluster-row teardown and bounds
  every chunk before the final global merge.
- Retention retains its count, delete, commit, logging, config reload,
  cancellation propagation, and sleep cadence.
- No table, migration, serialized object, database/config format, remote
  command, lifecycle ordering, public API, or CLI output changes.

## Alternatives

- Leave the family in the facade: safe, but retains a complete repository with
  independent callers and change cadence inside an already mixed module.
- Move only read projections: rejected because it splits one table's ownership
  and leaves append and retention SQL behind.
- Move cluster name resolution into the repository: rejected because that is a
  cross-table cluster-identity responsibility already owned by the facade.
- Move the retention daemon: rejected because config reload and process task
  lifecycle are runtime policy, not persistence.
- Move `ClusterEventType`: rejected because its public and serialized identity
  is a compatibility contract, while moving it adds no ownership clarity.

## Milestones and rollout

1. Add characterization against the unchanged facade for public shape,
   decorator depth, shared-session dedupe, generation fencing, late-bound
   helpers, query counts, ordering, chunking, and daemon patch seams.
2. Move only event-table SQL and projection code behind facade wrappers.
3. Run mapped suites, formatting and static analysis, import checks, and
   alternating cold-import and representative SQLite timing probes.
4. Rebase onto current `origin/improvements`, rerun affected gates, and merge
   only after every relevant exact-head CI check and review thread is green.

Rollback is a normal revert.  Persisted rows and public entrypoints do not
change, so no data or compatibility migration is required.

## Changed-path-to-test matrix

| Changed path or seam | Tests and checks |
| --- | --- |
| `sky/global_user_state.py` facade, signatures, decorators and patch seams | New cluster-event repository contract, retention daemon tests, import contracts |
| Append, generation fence, shared-session dedupe and dialect errors | Existing cluster-event and provision-fence suites, nested-session contract, new dependency and count checks |
| Latest, terminal-priority, status-age and batch reads | Existing cluster-event, Kubernetes autodown, Jobs event and cluster-status tests |
| Ordered and name-based history projection | Existing multi-type, post-teardown, global-limit and chunk-bound tests, Jobs server event tests |
| Retention SQL and runtime scheduling | Existing cleanup and daemon cancellation tests, new facade cleanup patch check |
| Structural and performance behavior | `format.sh --files`, mypy, Pylint, Ruff, BasedPyright, import-linter, compileall, import-order, `git diff --check`, cold import and SQLite read benchmarks |

No live-cloud smoke test is needed because the extraction changes no provider
call or remote command.  The relevant smoke behavior is fully represented by
the same durable rows, transaction boundaries, event ordering, and public
calls.
