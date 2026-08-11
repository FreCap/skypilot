# Global User State Cluster Control-Plane Read Gateway

## Context

`sky.global_user_state` remains the public persistence facade for cluster,
user, event, storage, volume, token, and configuration state. Its live-cluster
read section currently owns two related control-plane operations alongside
stateful lifecycle writes and presentation-oriented listing and history reads:

- `get_cluster_from_name()` reads one live cluster for Core, backend, Jobs,
  debug, and SkyServe control-plane callers. It optionally joins user display
  data and optionally loads verbose creation and terminal-event fields.
- `get_clusters_from_names()` reads summary records for many live clusters for
  SkyServe controllers, autoscalers, and replica managers. It bounds `IN`
  clauses and optionally performs one batched user read.

Both operations select and project the same durable cluster-record schema.
Their callers and latency contracts differ from lifecycle mutations, history
reporting, and dashboard listing, but the implementation is still embedded in
the facade.

## Responsibility Map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Exact live-cluster query | Core, cloud VM backend, Jobs, debug, SkyServe | SQLAlchemy session, cluster and user tables, current-user compatibility join | Read snapshot only | Missing-row drift, extra user query, verbose-column drift | One cluster SELECT; optional event SELECT outside the row session | Lifecycle and control-plane features |
| Batched live-cluster query | SkyServe controller, autoscaler, replica managers, Serve utilities | SQLAlchemy session, cluster and user tables, bounded `IN` chunks, batched user lookup | Read snapshot only | Missing-name cardinality drift, unbounded query, N+1 user reads | One cluster SELECT per 500 names plus at most one user SELECT | Serve scale and control-plane efficiency |
| Shared cluster-record projection | Exact and batched control-plane reads | Pickle compatibility, status enum, owner JSON compatibility, metadata JSON, boolean normalization | None | Public response-shape drift, decode drift, mutable-row leakage | One handle decode and one metadata/owner decode per returned row | Cluster schema and compatibility |
| Active-cluster listing presentation | Dashboard, metrics, status, resource checks | Listing filters, ordering, node display, priorities, batched event enrichment | Read snapshot only | Ordering, filter, or display drift | Bounded listing and event queries | Dashboard and RBAC |
| Cluster lifecycle and usage persistence | Launch, refresh, stop, down, purge, recovery | Caller-owned transactions, row locks, retries, history and event ordering | Durable lifecycle rows and usage intervals | Lost transition, early commit, lock inversion, accounting drift | Write, lock, and transaction sensitive | Lifecycle correctness |

The first three responsibilities form the proposed gateway. Listing remains in
`sky.global_user_state_cluster_listing`; lifecycle and usage persistence remain
in the facade because moving them would split transaction and ordering
ownership.

## Behavior Contract

Keep `sky.global_user_state.get_cluster_from_name` and
`sky.global_user_state.get_clusters_from_names` as the stable decorated public
facade functions. Their signatures, defaults, annotations, decorators, return
shapes, missing-row behavior, exception behavior, and monkeypatch points remain
unchanged.

Move the complete exact and batched read pipelines to a plain-function module:

- exact and batched query-field selection;
- the optional exact user join and legacy `NULL` user fallback;
- bounded batched `IN` queries and missing-name `None` filling;
- the optional batched user snapshot within the cluster session;
- shared summary projection and exact-only verbose fields;
- exact-only terminal-or-last status event lookup.

Pass live engine, session, table, chunk-size, user, event, owner-decoder, and
current-user dependencies from the facade on every invocation. This preserves
existing patches to `sky.global_user_state` and avoids a second persistence
object graph.

The extraction must preserve these budgets:

- exact summary with user data: one SELECT;
- exact verbose with user data: one cluster/user SELECT plus at most one event
  SELECT;
- batched summary without user data: one cluster SELECT per chunk;
- batched summary with user data: one cluster SELECT per chunk plus one batched
  user SELECT;
- no database access for an empty batched input;
- one handle, owner, and metadata decode per returned record.

No public import, serialized identity, database/config format, lifecycle order,
or user-visible behavior changes.

## Abstraction Choice

Use a facade-first plain-function read gateway. There is one persistence
implementation and no policy variation, staged construction, event fan-out, or
cross-cutting wrapper behavior, so a class, protocol, strategy, factory,
observer, decorator, registry, or dependency-injection layer would add carrying
cost without a second implementation. Extracting only the shared projection
would leave query ownership and user/event budgets split across modules, while
moving lifecycle writes would cross the safer read-only boundary.

## Alternatives

1. Leave the code in place. This avoids a new module but retains two hot
   control-plane gateways and shared projection ownership inside a facade that
   already spans unrelated persistence domains.
2. Extract only a row formatter. Rejected because it adds a forwarding layer
   while query fields, session lifetime, user enrichment, and event budgets stay
   mixed in the facade.
3. Move all live-cluster reads together. Rejected because raw status snapshots,
   image-consumer reads, filtered listing, and control-plane records have
   distinct corruption, projection, and query contracts.
4. Introduce a repository class or protocol. Rejected because no concrete
   second backend or instance lifecycle exists.

## Milestones

1. Add characterization coverage for public signatures, facade module identity,
   exact verbose/summary shapes, missing rows, legacy user fallback, batched
   cardinality, query counts, decode counts, and both import orders.
2. Run the characterization suite on the unmodified implementation.
3. Extract the complete gateway and keep thin decorated facade functions.
4. Re-run characterization and affected global-state, Jobs, backend, and
   SkyServe tests plus static checks.
5. Compare cold import time and representative exact/batched read timing and
   query/decode counts against the base revision.

## Changed-Path-to-Test Matrix

| Path or seam | Coverage |
| --- | --- |
| Stable facade signatures, defaults, annotations, module identity | New cluster-control-plane read contract tests and import-order probes |
| Exact query fields, user join, fallback, summary/verbose projection, event lookup | New contract tests plus existing batched-cluster and global-state tests |
| Batched chunks, missing-name cardinality, user snapshot, summary projection | Existing batched-cluster and nested-session tests plus new decode-count characterization |
| Downstream Core, backend, Jobs, and SkyServe callers | Focused backend-utils, Jobs utilities, Serve controller/autoscaler/replica suites selected from call sites |
| Formatting, imports, typing, lint, and CI collection | `format.sh --files`, mypy, Pylint, Ruff, BasedPyright, import-linter, compile/import probes, and exact-head CI |

## Rollout and Rollback

This is a structural extraction with no migration or feature flag. Roll back the
single extraction commit if facade behavior, query/decode counts, imports, or
latency regress. Do not merge until all relevant exact-head CI checks and review
threads are green and the current `origin/improvements` merge tree passes the
focused suite.

## Manual Test Plan

1. Create one explicit-user cluster and one legacy `NULL`-user cluster.
2. Read each in exact summary and verbose modes and confirm keys and user
   attribution match the pre-extraction results.
3. Read a batch containing both rows, a missing name, and duplicate input; verify
   stable key cardinality, summary-only fields, and user attribution.
4. Instrument SQLAlchemy `before_cursor_execute` and decode helpers to verify the
   query and per-row decode budgets above.
5. Import the facade before and after the extracted module in fresh processes to
   rule out circular imports.
