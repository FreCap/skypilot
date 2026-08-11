# Global User State Raw Cluster Snapshot Gateway

## Context

`sky/global_user_state.py` is a stable public facade over the global state
database. It still owns cluster lifecycle mutation, caller-owned transactions,
serialized handle persistence, usage accounting, and several low-state cluster
read paths. The facade is 3,069 lines at the rebased start of this change.

The following functions form a distinct raw snapshot family:

- `get_cluster_status_fields()`
- `get_cluster_status_fields_by_prefix()`
- `get_managed_cluster_status_fields()`
- `get_managed_job_cluster_cleanup_candidates()`
- `get_cluster_refresh_fields()`

They select only plain cluster-table columns and return tuples, public
NamedTuples, or mappings used by refresh, request recovery, Serve
reconciliation, and managed-job cleanup. They do not deserialize handles,
mutate durable state, share caller transactions, or own lifecycle ordering.

Container-image binding reads are not part of this extraction. Their
caller-owned transaction, row-lock, binding-validity, and image-generation
semantics belong to the managed-image lifecycle seam.

## Responsibility Map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Exact or all-cluster status snapshot | backend refresh sweeps, request recovery, SkyServe replica management | SQLAlchemy session, cluster name/status/timestamp/managed columns, bounded `IN` chunks | Read snapshot only | Missing-name cardinality drift, managed-row leakage, deserialization of corrupt rows, unbounded queries | Empty input must issue zero queries; named input uses one query per 500 names; all-cluster input uses one query | Refresh and recovery behavior |
| Prefix status inventory | Serve placement normalization | SQL prefix predicate, name ordering, explicit row limit | Read snapshot only | Namespace escape, unbounded inventory, unstable ordering, overflow accepted | Exactly one bounded query selecting one extra row | Serve placement proofs |
| Managed workload status inventory | Serve cleanup and workload owners | managed flag, workload-type attribution, nonempty generation hash, and public `ManagedClusterStatusFields` constructor | Read snapshot only | Cross-workload cleanup, unmanaged or unfenced-row inclusion, generation loss, public type-identity drift | One plain-column query | Workload reconciliation and generation fencing |
| Managed-job cleanup candidate inventory | managed-jobs controller utilities | managed flag, workload type, legacy NULL attribution, workload ID | Read snapshot only | Legacy managed rows omitted, other workload rows included, ownership attribution drift | One two-column query | Managed-jobs cleanup |
| Single-cluster refresh fence | backend status refresh | status, status timestamp, autostop, autodown, generation, managed flag, workload type, and public `ClusterRefreshFields` constructor | Read snapshot only | Full-row deserialize failure, generation or workload fence omission, boolean-shape drift, public type-identity drift | One seven-column query | Cluster refresh concurrency and same-name replacement safety |
| Cluster lifecycle mutation and persistence | launch, refresh, stop, down, purge, recovery | transactions, locks, retries, history, events, serialized handles, usage intervals | Durable lifecycle rows and accounting | Lost transition, split transaction, lock inversion, ordering drift | Write, lock, and transaction sensitive | Lifecycle correctness |

## Decision

Extract the five raw snapshot functions into
`sky/global_user_state_cluster_raw_snapshots.py`. Keep the decorated public
functions and their exact signatures in `sky.global_user_state`, delegating to
plain module functions. Pass the live engine getter, session factory, cluster
table, chunk size, and the two public NamedTuple constructors on every relevant
call so the facade remains the stable import, type-identity, and monkeypatch
surface.

This is a facade-first plain-function gateway. There is one persistence
implementation, so a class, protocol, abstract repository, registry, strategy,
or dependency-injection layer would add carrying cost without a second
implementation. Extracting only a shared row formatter would leave query
ownership, bounds, filters, and session lifetime mixed into the lifecycle
facade. Defining the public NamedTuples in the gateway would move their public
module identity and create an import cycle if the facade imported them back;
constructor injection preserves both identities with no new abstraction.
Moving lifecycle writes would split transaction and ordering ownership.

## Behavior Contract

- Preserve public import paths, function names, signatures, type hints,
  decorators, return types, errors, and raw response shapes.
- Preserve zero database access for an empty exact-name list.
- Preserve one status query per `_CLUSTER_IN_QUERY_CHUNK_SIZE` named clusters,
  one query for an all-cluster snapshot, and one query for each other helper.
- Preserve omission of missing named clusters rather than filling them with
  `None`.
- Preserve plain-column reads without handle, owner, metadata, or status-enum
  deserialization.
- Preserve prefix escaping, name ordering, `row_limit + 1` fail-closed
  overflow detection, and validation errors.
- Preserve managed workload filters, omission of empty cluster hashes, exact
  `ManagedClusterStatusFields` construction, and legacy NULL managed-job
  attribution.
- Preserve `ClusterRefreshFields` construction, including `bool(to_down)` and
  `bool(is_managed)` normalization plus generation and workload fences.
- Preserve `ManagedClusterStatusFields` and `ClusterRefreshFields` public module
  identity in `sky.global_user_state`.
- Preserve the facade's module-level chunk-size monkeypatch point.
- Do not change database schemas, lifecycle writes, locks, transaction
  boundaries, remote behavior, or user-visible output.

## Implementation Milestones

1. Add characterization tests for facade identity and signatures, query and
   projection budgets, corrupt serialized columns, prefix validation and
   ordering, managed filters, legacy cleanup attribution, and refresh fences.
2. Run the characterization tests against the unmodified implementation.
3. Add the gateway module and replace facade bodies with dependency-explicit
   delegation.
4. Run focused callers, the broader global-state suite, static checks, import
   probes, and performance comparisons.
5. Open one PR and merge only after all relevant exact-head CI and review gates
   are green.

## Rollout and Rollback

This is an in-process structural extraction with no schema, API, configuration,
or serialized-data migration. Rollout is the normal Python package release.
Rollback is reverting the extraction commit. The facade keeps the public
contract stable in either direction.

## Test Plan

| Changed path or seam | Coverage |
| --- | --- |
| Public facade identity, signature, decorators, type hints, and import order | New cluster-status snapshot contract tests plus both module import orders |
| Named, all-cluster, and unmanaged status reads | New contract tests plus `test_global_user_state_batched_clusters.py` and refresh-sweep tests |
| Prefix filtering, ordering, limit, and validation | New contract tests plus existing placement-contract tests |
| Managed workload generation fence, public NamedTuple identity, and managed-job cleanup filters | New contract tests, global-state batched tests, Serve utility tests, and Jobs utility/controller restart tests |
| Refresh status/autostop/to-down/generation/workload fence and public NamedTuple identity | New contract tests plus `test_refresh_status_no_reread.py` and backend utility tests |
| Formatting, typing, lint, imports, and CI collection | `format.sh --files`, mypy, Pylint, Ruff, BasedPyright, import-linter, compile/import probes, `git diff --check`, and exact-head CI |

Performance evidence will compare cold imports and warm SQLite calls for named
status batches and single-cluster refresh snapshots. The extraction is accepted
only if query counts and per-row decoding budgets are unchanged and local timing
shows no material regression.
