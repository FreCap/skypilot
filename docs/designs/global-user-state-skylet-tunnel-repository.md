# Global User State Skylet Tunnel Repository

## Context

`sky/global_user_state.py` is a 3,491-line compatibility facade.  It owns the
shared database lifecycle plus cluster lifecycle, history, usage, SSH keys,
and several table repositories.  The Skylet tunnel snapshot and fenced
compare-and-set family now occupies one cohesive cluster-table column boundary
inside that facade.

Line count is only a prioritization signal.  This extraction is justified by
the tunnel family's complete state boundary and by callers, dependencies,
failure modes, and change cadence that differ from cluster lifecycle and
history persistence.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Shared database lifecycle and public facade | API startup, migrations, tests, every global-state consumer | `DatabaseManager`, schema metadata, SQLAlchemy sessions, timing decorators | Engine, pool, table identity, migration order, and historical patch paths | Wrong database, schema drift, connection exhaustion, or compatibility break | Cold import, engine lookup, connection, and transaction counts | Database topology and compatibility |
| Cluster lifecycle, identity, history, and usage | Core launch, refresh, stop, down, Jobs, Serve, dashboard, and recovery | Cluster and history tables, serialized handles and resources, action-aware locks, user and event projections | Cluster rows, record identity, status, handles, usage intervals, and history rows | Stale generation updates, split commits, invalid transitions, serialization drift, or reordered locks | Hot lifecycle locks, query counts, serialization, and dashboard history latency | Provisioning, lifecycle, recovery, and cost reporting |
| Skylet tunnel metadata gateway | Cloud VM backend tunnel recovery and direct state tests | One cluster-table field, cluster hash fence, SQLAlchemy update predicates, pickle, and `TunnelMutationResult` | Exact serialized tunnel blob and its same-row cluster incarnation observation | ABA overwrite, malformed blob reinterpretation, unfenced mutation, changed outcome enum, extra query, or premature engine checkout | Exactly one row read per snapshot, one update and commit per fenced mutation, and no database access for a null hash | Tunnel transport, process identity, and recovery fencing |
| Tunnel process and transport lifecycle | Cloud VM backend and Skylet channel clients | Processes, gRPC channels, readiness, UUID generations, and cleanup | Local tunnel process, channel generation, readiness, and retry state | Leaked process, stale channel reuse, readiness timeout, or cleanup race | Provisioning and command latency | Transport and remote-execution behavior |

The tunnel gateway has materially different callers and dependencies from the
surrounding cluster lifecycle orchestrator.  Its read and compare-and-set
operations own the complete field-level transaction contract without taking
ownership of cluster creation, removal, or history rows.

## Decision

Add `sky/global_user_state_skylet_tunnels.py` as a plain function repository.
Keep every historical entrypoint, signature, timing decorator, patch path, and
the snapshot class's `sky.global_user_state` pickle identity in the facade.

The repository owns:

- one-row snapshot reads of cluster hash and the exact serialized tunnel blob;
- fail-closed decoding that retains malformed bytes for explicit repair; and
- cluster-hash and exact-blob fenced compare-and-set updates and outcomes.

The facade supplies its late-bound engine getter, session factory, and table.
The compatibility metadata-only helper remains in the facade because it calls
the public snapshot function and therefore preserves the existing monkeypatch
seam.

This is facade-first decomposition with a module and functions.  A class,
protocol, registry, or dependency-injection container would invent an
unneeded second implementation.  Direct public moves would break import and
pickle identities.  Moving only SQL while retaining wrappers preserves those
contracts and gives the field-level gateway one owner.

## Behavior contract

- `SkyletSSHTunnelMetadata`, `ClusterSkyletSSHTunnelSnapshotV1`, all public
  functions, signatures, decorator depth, and patch paths remain available
  from `sky.global_user_state`.
- The snapshot class remains pickle-addressable as
  `sky.global_user_state.ClusterSkyletSSHTunnelSnapshotV1`.
- A snapshot performs one query that observes cluster hash and tunnel blob
  from the same row.  Missing rows, null blobs, byte normalization, malformed
  pickle handling, and equality remain unchanged.
- A compare-and-set with a null cluster hash returns
  `UNFENCED_CLUSTER_INCARNATION` before obtaining an engine or session.
- A fenced mutation uses the observed cluster hash and exact blob in one
  update predicate, performs one commit, maps row counts exactly, and keeps
  the same error message for invalid cardinality.
- The metadata-only compatibility helper continues to call the facade's
  late-bound snapshot function.
- No schema, migration, serialized blob, database/config format, lifecycle
  ordering, remote command, public API, CLI output, or user-visible behavior
  changes.

## Alternatives

- Leave the family in the facade: safe, but keeps transport-specific decoding
  and mutation fencing mixed into cluster history and lifecycle persistence.
- Move the snapshot class publicly: rejected because its module and pickle
  identity are compatibility contracts.
- Move tunnel process lifecycle with persistence: rejected because process,
  gRPC, and readiness policy belong to the existing backend transport seam.
- Extract cluster history instead: rejected because history writes, reads,
  usage intervals, and caller-supplied sessions still share cluster lifecycle
  transactions and table ownership.

## Milestones and rollout

1. Add characterization on the unchanged facade for public signatures, pickle
   identity, late-bound metadata projection, and snapshot value semantics.
2. Move the snapshot decoder and fenced update behind facade wrappers.
3. Run the tunnel, global-state, and backend regression suites, formatting and
   static analysis, import checks, query-count checks, and representative
   timing probes.
4. Rebase onto current `origin/improvements`, rerun affected gates, and merge
   only after every relevant exact-head CI check and review thread is green.

Rollback is a normal revert.  Persisted blobs and public entrypoints do not
change, so no database or compatibility migration is required.

## Changed-path-to-test matrix

| Changed path or seam | Tests and checks |
| --- | --- |
| `sky/global_user_state.py` facade, names, signatures, decorators, and patch seams | Tunnel facade characterization, import-order checks, existing backend patch-based tests |
| Snapshot query, exact blob preservation, malformed decoding, and value identity | Existing `test_global_user_state_skylet_tunnel.py`, new pickle and value contract |
| Fenced mutation, null-hash fast path, row-count outcomes, and commit count | Existing tunnel state tests and query/transaction instrumentation |
| Cloud VM tunnel caller behavior | Focused Cloud VM backend tunnel tests |
| Structural and performance behavior | `format.sh --files`, mypy, Pylint, Ruff, BasedPyright, import-linter, compileall, `git diff --check`, cold import and SQLite snapshot/CAS timing |

No live-cloud smoke test is needed because the extraction changes no provider
call or remote command.  The relevant behavior is represented by the same
durable row, exact update predicate, transport outcome, and public calls.
