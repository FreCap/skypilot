# Global user state cluster-record identity gateway

## Context

`sky/global_user_state.py` is a 3,554-line compatibility facade over the
global-state database.  Most table-specific repositories have already moved
behind that facade, but the action-aware cluster-record identity primitive
still owns PostgreSQL UUID validation, dual advisory locking, exact identity
commit/read behavior, and persisted-handle decoding in the facade itself.

This boundary was introduced for resource actions and has different callers,
dependencies, failure modes, and change cadence from cluster history,
presentation queries, usage accounting, and ordinary lifecycle orchestration.
The extraction must remain structural.  It must not alter cluster lifecycle
ordering, SQL, lock ordering, transactions, serialized handles, public import
paths, or exception and result identities.

## Before responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Cluster-record UUID contract | Resource-action launch, down, recovery, and tests | `uuid` | Canonical UUID text and UUID objects | Alternate spellings or invalid types accepted | Validation is on every action-aware operation | Resource-action identity protocol |
| Cluster-record lock and commit gateway | `add_or_update_cluster()`, direct transaction tests, resource-action writers | PostgreSQL advisory locks, `cluster_table`, SQLAlchemy PostgreSQL insert | Name-to-record-UUID uniqueness in the caller transaction | Same-name or same-UUID collision, deadlock, partial row, unsupported database | Exactly two advisory-lock acquisitions, two conflict reads, and at most one insert | PostgreSQL concurrency and identity fencing |
| Exact record snapshot gateway | Core down, VM backend, SkyServe replica management, and transaction tests | PostgreSQL advisory locks, `cluster_table`, pickle | Same-row UUID and exact serialized handle | Legacy/null identity, unreadable handle, stale or different record | Exactly two locks, one row read, and one handle decode | Resource-action consumers and recovery |
| Cluster lifecycle orchestration | `add_or_update_cluster()`, `remove_cluster()`, status and history callers | cluster and history tables, container-image lifecycle, events, usage intervals | Cluster rows, history rows, status, usage, image bindings | Lost transition, reordered cleanup, stale removal, history drift | Hot transactions, query counts, and lock order | Cluster lifecycle and product behavior |
| Cluster presentation and history projection | CLI, SDK, dashboard, jobs, Serve | user joins, pickle projections, RBAC/workspace filters | No independent durable state | N+1 queries, field drift, wrong filtering | Dashboard latency and batch cardinality | Presentation and reporting |

The first three rows form one stable PostgreSQL gateway.  They share the same
identity contract and lock order.  Their external consumers differ materially
from the lifecycle and projection callers, while the gateway can move end to
end without callbacks into orchestration code.

## Decision

Add `sky/global_user_state_cluster_record_identity.py` as a plain function
gateway.  Keep `sky.global_user_state` as the public facade:

- Preserve `ClusterRecordIdentityWriteOutcome`,
  `ClusterRecordIdentityConflictError`, `ClusterRecordHandleChangedError`,
  `ClusterRecordRemovalOutcome`, and `ClusterRecordIdentitySnapshot` at their
  historical import and pickle identities through facade aliases.
- Preserve `_canonical_cluster_record_uuid()`,
  `_lock_cluster_record_uuid_in_session()`,
  `_commit_cluster_record_identity_in_session()`,
  `_read_cluster_record_identity_in_session()`, and
  `get_cluster_record_identity_snapshot()` signatures and facade patch points.
- Pass the facade-owned lifecycle name lock into the extracted commit/read
  operations.  Pass the facade UUID-lock function as well so existing
  monkeypatch-based deadlock characterization continues to affect nested
  cluster upserts and direct transaction calls.
- Use direct facade aliases for the stateless UUID validator and UUID lock;
  retain wrappers only where late dependency binding is required.
- Keep retry ownership on the public facade snapshot wrapper.
- Keep caller-owned transactions caller-owned.  The extracted commit/read
  operations never commit or roll back.

The extracted gateway owns validation, PostgreSQL enforcement, UUID advisory
locking, exact identity commit/read SQL, conflict construction, handle decoding,
and the result types.  The facade owns dependency binding, retries, historical
names, and cluster lifecycle orchestration.

## Why a plain gateway fits

Plain functions and dataclasses match the single PostgreSQL implementation and
existing global-state repository style.  There is no second algorithm or
provider, so a strategy, adapter hierarchy, abstract repository, registry, or
dependency-injection container would add carrying cost without a concrete use.
A direct public move would break imports, pickled dataclass identity, test patch
points, and downstream code.  Leaving the code in place avoids a new file but
continues mixing concurrency protocol ownership with lifecycle and projection
code in the central facade.

## Behavior contract

1. Canonical UUID validation accepts `uuid.UUID` and lowercase canonical UUID
   text only, with unchanged exception types and messages.
2. Action-aware operations remain PostgreSQL-only.
3. Lock order remains cluster-name lifecycle lock, then record-UUID advisory
   lock, before row reads or writes.
4. Identity commit performs the same locked name read, locked UUID read, and
   `INSERT ... ON CONFLICT DO NOTHING`, returning the same enum members.
5. Identity reads perform one locked row query and decode the exact persisted
   handle once, preserving conflicts for null, mismatched, empty, or unreadable
   values.
6. Public names, signatures, decorators, exception inheritance, enum values,
   dataclass fields, facade monkeypatch paths, and pickle identities remain
   unchanged.
7. `add_or_update_cluster()` and `remove_cluster()` retain transaction and
   lifecycle ordering.  No behavioral optimization is included.

## After responsibility map

| Owner | Responsibilities |
| --- | --- |
| `sky/global_user_state_cluster_record_identity.py` | UUID contract, PostgreSQL enforcement, UUID lock, identity commit/read SQL, exact handle decode, identity result and error types |
| `sky/global_user_state.py` | Stable facade, dependency and patch-point binding, retry wrapper, cluster lifecycle transactions, history, usage, and projections |

## Milestones

1. Add facade-contract characterization tests and run them before movement.
2. Move the gateway implementation and result types behind facade aliases and
   wrappers without changing lifecycle call sites.
3. Run the characterization and real-PostgreSQL identity suites, lifecycle
   consumers, import/order checks, formatting, types, lint, and diff checks.
4. Measure cold `import sky.global_user_state`, UUID validation, and exact
   facade call counts against the base commit.
5. Publish one PR and require all relevant checks on its exact final head.

## Changed-path-to-test matrix

| Changed path / seam | Tests and checks |
| --- | --- |
| Gateway types, validation, signatures, identities | New facade contract test; existing PostgreSQL identity tests |
| Identity commit/read and lock order | `test_global_user_state_cluster_record_identity_pg.py`, including concurrency proof |
| `add_or_update_cluster()` and `remove_cluster()` integration | PostgreSQL identity suite and global-user-state unit tests |
| Core, VM backend, and Serve snapshot consumers | Core down-event, cloud VM backend, and resource-action legacy-down tests |
| Imports and static contracts | Both import orders, `compileall`, full mypy and Pylint, `format.sh --files`, `git diff --check` |

## CI mapping

The pull-request Python workflow has no changed-path exclusion for these files.
The Unit Tests job collects the new facade contract and global-state tests.  The
existing API and component jobs exercise cluster lifecycle consumers.  Format,
mypy, Pylint, Ruff, BasedPyright, compile, and import-linter jobs remain
applicable.  The PR must remain open if live checks show a path-filter or
collection gap.

## Rollout and rollback

This is an internal structural extraction with no schema, configuration, wire,
CLI, or deployment migration.  Rollout is the ordinary Python package release.
Rollback is a normal revert of the extraction commit.  Because facade names and
database behavior remain stable, no data rollback is required.
