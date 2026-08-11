# Global user state cluster-history report gateway

## Context

`sky/global_user_state.py` is a 3,299-line compatibility facade over the
global-state database.  It owns stateful cluster lifecycle writes, usage
interval mutation, exact and batched control-plane reads, status snapshots,
and read-only history reporting for `sky cost-report` and the dashboard.

This change considers only `get_clusters_from_history()`.  Its history query,
read-time filtering, pickle decoding, batched user and event enrichment, and
response projection form a complete read gateway.  Usage-interval mutation
and cluster lifecycle transitions remain in the facade because they share
caller-owned transactions, row and advisory locks, and lifecycle ordering.

## Before responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| History report query and filters | `sky.core.cost_report`, dashboard history list and detail | SQLAlchemy, history and active-cluster tables, current time | Read snapshot only | Wrong lookback, identifier OR semantics, managed-cluster leakage, unstable ordering | One history query; bulk paths must not fetch YAML and command blobs | Dashboard, reporting, RBAC |
| History decode and enrichment | Cost report and dashboard | Pickled intervals/resources, user repository, cluster-event repository | Read snapshot only | Corrupt legacy pickle, fallback-user drift, N+1 user/event reads | One batched user read and one batched event read | Reporting compatibility |
| History response projection | Cost report encoders and dashboard | Status enum, node-name display shaping, legacy response schema | None | Key drift, wrong duration, workspace fallback, changed sort | One decode and bounded projection per row | Presentation and accounting |
| Usage interval persistence | Launch, stop, down, recovery | History table, caller-owned sessions, commits, wall clock | Durable usage rows | Early commit, open-interval corruption, accounting drift | Transaction and lock sensitive | Lifecycle and accounting |
| Cluster lifecycle orchestration | Launch, refresh, stop, down, recovery | Cluster/history/event tables, locks, retries, backend handles | Durable cluster and history rows | Lost transitions, stale actions, split transactions | Write and contention sensitive | Lifecycle features |

The first three responsibilities have materially different callers,
dependencies, state, failure modes, and reasons to change from the last two.
They form one stable reporting seam.  Moving only a row formatter would leave
query shape and enrichment ownership mixed in the facade; moving usage writes
would cross the transaction boundary.

## Decision

Add `sky/global_user_state_cluster_history.py` as a plain-function read gateway
and keep `sky.global_user_state.get_clusters_from_history()` as the stable
facade.

The facade retains its public signature, decorators, import path, and late
monkeypatch points.  On every call it passes the live engine getter, session
factory, schema tables, clock, current-user lookup, batched user lookup,
batched event lookup, duration helper, and node-name display helper.  This
keeps tests and callers that patch facade-owned dependencies working and
avoids a circular import.

The gateway owns the complete history SELECT, lookback and identifier filters,
managed-cluster exclusion, heavy-column policy, interval/resource decoding,
batched enrichment, duration calculation call, response projection, and final
ordering.  It owns no write, transaction, schema, database lifecycle, or
cluster lifecycle behavior.

## Why a plain gateway fits

There is one history-report implementation and no policy variation, so a
class, repository hierarchy, protocol, strategy, registry, or dependency
injection container would add carrying cost without a second implementation.
A helper-only extraction would add forwarding without transferring ownership.
The one function behind the existing facade is the smallest complete seam.

## Behavior contract

1. `sky.global_user_state.get_clusters_from_history` keeps its name, module,
   signature, decorators, defaults, positional-call behavior, and return type.
2. The outer join, active-or-recent predicate, descending launch ordering,
   identifier OR semantics, and managed-cluster filter remain unchanged.
3. Bulk reports do not select `last_creation_yaml` or
   `last_creation_command`.  Non-abbreviated bulk responses retain those keys
   as `None`; abbreviated responses omit them; filtered detail queries fetch
   and return them.
4. Null historical user hashes continue to fall back to the current user.
   Users and last events remain batch-enriched once per report, never per row.
5. Corrupt usage/resource pickles remain non-fatal under the existing handled
   exception classes.  Duration, status, workspace, priority, node-name, and
   final ordering semantics remain unchanged.
6. Query count, decode count, event/user call count, memory/copy behavior,
   database formats, pickle identities, CLI output, API encoding, and lifecycle
   ordering do not change.

## After responsibility map

| Owner | Responsibilities |
| --- | --- |
| `sky/global_user_state_cluster_history.py` | History report query, filters, heavy-column policy, decoding, batched enrichment, projection, and ordering |
| `sky/global_user_state.py` | Stable facade and late dependency binding; usage persistence; exact/batched reads; status snapshots; lifecycle orchestration |

## Verification and CI mapping

Before movement, add and run a facade and read-gateway characterization test.
It freezes the public signature and module identity, one-query history scan,
one batched user call, one batched event call, current-user fallback, duration,
status, workspace, node-name, heavy-column, and result-shape behavior.  Run the
existing managed-history and global-state suites on the unchanged
implementation.

After movement, rerun those tests plus cost-report and estimated-spend
consumers, nested-session and remove-cluster suites, both import orders,
compileall, formatter, mypy, Pylint, dashboard checks, and diff checks.  Compare
cold import time and a representative SQLite history projection against the
base.  The pull-request Python workflows have no changed-path filter excluding
these paths: Unit Tests collect the new and focused suites, Config, Storage &
Compatibility Tests collect `tests/test_global_user_state.py`, and static jobs
cover both Python modules.

## Rollout and rollback

This is a structural extraction with no migration or behavior flag.  Rollout
is the ordinary package release.  Rollback is a normal commit revert; no data
rollback is required because schema, serialized data, and public contracts do
not change.
