# Global user state active-cluster listing gateway

## Context

`sky/global_user_state.py` is a 3,426-line compatibility facade over the
global-state database.  It still owns lifecycle writes, exact and batched
control-plane reads, status snapshots, history and usage reporting, and the
dashboard-oriented active-cluster listing projection.

This change considers only `get_clusters()`.  It is a read-only listing gateway
with filters and presentation enrichment that change for dashboard, metrics,
and CLI reasons.  Exact `get_cluster_from_name()` and batched
`get_clusters_from_names()` reads serve orchestration and SkyServe callers with
different response shapes and query budgets, so they remain in the facade.

## Before responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Active-cluster listing query | Backend status, dashboard, server metrics, debug dump, volume and resource checks | SQLAlchemy, cluster and user tables, current user fallback | Read snapshot only | Wrong workspace/user/name filtering, duplicate rows, unstable order | One query for ordinary lists; one query per 500-name chunk | Dashboard, RBAC, metrics |
| Listing event enrichment | Dashboard list and detail responses | Cluster event repository, event types | Read snapshot only | N+1 queries, missing launch reason, wrong terminal event | Zero launch queries without INIT rows; at most two batched event reads | Dashboard presentation |
| Listing row projection | Dashboard, CLI/backend status, metrics | Handle pickle, status enum, default priority, node display shaping, legacy JSON | No durable state | Field/schema drift, unsafe decode, changed summary behavior | One handle decode and bounded field work per row | Presentation and reporting |
| Exact and batched control-plane reads | Core, backend, Jobs, SkyServe | Cluster table, optional user lookup, handle/status projection | Read snapshot only | Missing record, query amplification, control-plane latency | Single row or bounded 500-name chunks | Lifecycle and Serve control plane |
| Cluster lifecycle and history | Launch, stop, down, recovery, cost report | Transactions, locks, cluster/history/event tables, usage intervals | Durable cluster and history rows | Lost transitions, stale actions, usage drift | Lock, transaction, and query-count sensitive | Lifecycle and accounting |

The first three rows form one stable listing gateway.  They share the same
callers and presentation contract and can move end to end.  They differ
materially from exact/batched control-plane reads and from stateful lifecycle
orchestration.

## Decision

Add `sky/global_user_state_cluster_listing.py` as a plain-function gateway and
keep `sky.global_user_state.get_clusters()` as the stable facade.

The facade retains its public signature, decorators, import path, and
monkeypatch points.  On every call it passes the live engine getter, session
factory, schema tables, current chunk size, current event helpers and event
type, user-join policy, and owner decoder.  The gateway imports the stable
status, constants, and display utility modules directly.  This preserves tests
and callers that patch facade-owned dependencies while keeping the extracted
module free of circular imports.

The gateway owns the listing query, filters, sorting, event enrichment, handle
decoding, and result projection.  It does not own exact reads, status-only
snapshots, history reporting, writes, transactions, schema, or database
lifecycle.

## Why a plain gateway fits

There is one listing implementation and no algorithm variation, so a class,
strategy, protocol, repository hierarchy, registry, or dependency-injection
container would add carrying cost.  A pure row formatter would leave query and
enrichment ownership mixed in the facade.  Moving every cluster read would
combine control-plane and presentation contracts in a new oversized module.
The listing-only function is the smallest complete seam.

## Behavior contract

1. `sky.global_user_state.get_clusters` keeps its name, module, signature,
   decorators, defaults, and keyword-only parameters.
2. Query columns, joins, filters, 500-name chunking, de-duplication, ordering,
   and query counts remain unchanged.
3. Legacy null `user_hash` rows continue to resolve to the current user for
   joins and filters.
4. Summary responses preserve their exact key set.  Verbose responses preserve
   YAML, command, event, config, links, owner, metadata, last-use, and status
   update fields.
5. INIT rows receive one batched launch-progress lookup.  Non-INIT-only results
   issue no launch-progress query.  Verbose results retain one batched terminal
   or last-event lookup.
6. Handle decoding, priority defaults, status conversion, node-name display,
   JSON decoding, and result order remain unchanged.
7. No database, wire, CLI, pickle, configuration, or lifecycle behavior changes.

## After responsibility map

| Owner | Responsibilities |
| --- | --- |
| `sky/global_user_state_cluster_listing.py` | Active listing query, filters, bounded name chunks, event enrichment, and listing projection |
| `sky/global_user_state.py` | Stable facade and late dependency binding; exact/batched reads; status snapshots; lifecycle, history, and usage |

## Verification and CI mapping

Before movement, add a facade signature/type contract and run it with the
existing batched-cluster characterization suite.  After movement, rerun those
tests plus full global-state, backend listing consumers, metrics tests, both
import orders, compileall, formatter, mypy, Pylint, dashboard checks, and diff
checks.  Measure cold import and representative summary listing calls against
the base.  The pull-request workflows have no path filter excluding these
Python paths; Unit Tests and Config, Storage & Compatibility Tests collect the
mapped suites, while format, mypy, Pylint, Ruff, BasedPyright, and import-linter
remain applicable.

## Rollout and rollback

This is a structural extraction with no migration.  Rollout is the ordinary
package release and rollback is a normal commit revert.  Stable facade and data
contracts mean no data rollback is required.
