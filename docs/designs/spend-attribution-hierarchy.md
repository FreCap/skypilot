# Spend Attribution Hierarchy

_Created: 2026-07-30_

## Problems

The estimated-spend dashboard can show organization-scale cost but does not
provide a direct accounting path from an owner to a logical managed job and
then to the physical clusters that incurred the cost. The existing flat
workload view aggregates a correctly attributed managed job, but it hides the
owner and cannot be expanded into tasks or recovery attempts.

The chart also combines all groups below its top eight into `Other`, while the
flat table returns only the top 50 groups. A large legacy population has
`workload_type = managed` without a verifiable parent job. Presenting each of
those cluster-derived identifiers as a separate logical managed workload makes
the attribution appear more exact than the stored evidence supports.

Provider-billed cost is a separate concern. The current estimator is a
SkyPilot catalog-priced, pay-as-you-go-equivalent compute estimate and must not
be relabeled as an AWS invoice.

## Goals

The default accountability view must let an administrator:

1. See total estimated cost by owner.
2. Expand an owner into complete logical workloads.
3. Expand a multi-task managed job into tasks.
4. Expand a task or single-task workload into every physical cluster attempt.
5. Page through all children without loading the organization's full 90-day
   history into one response.
6. Recognize legacy managed cost whose parent cannot be proven.
7. Understand that chart `Other` is a presentation remainder rather than an
   unattributed billing category.

All parent totals must come directly from the same date-filtered
`estimated_spend_daily` rows as their children. The change must add no new
central database table and must remain compatible with old dashboard clients
and servers.

## Background

`estimated_spend_daily` already records the fields required for deterministic
drill-down:

- `user_hash` and `workspace`
- `workload_type`, `workload_id`, and `workload_task_id`
- `cluster_hash` and `cluster_name`
- estimated total, Spot, on-demand, priced-time, and excluded-time inputs

The existing `/estimated_spend` endpoint returns aggregate totals, the top 50
groups, and chart series for the top eight groups. It remains the correct
endpoint for the chart and root owner table. Returning every descendant
eagerly would make response size and query cost proportional to all physical
attempts in the selected range.

## Solution

### v0: Owner-first lazy hierarchy

The dashboard defaults its grouping selector to `User`. Its hierarchy table
uses a new admin-only `/estimated_spend/drilldown` endpoint, including the
paginated owner root, with the same inclusive UTC date range.

The hierarchy is:

```text
Owner
└── Logical workload
    ├── Task, only when more than one task ID exists
    │   └── Physical attempt
    └── Physical attempt, for zero-task and single-task workloads
```

The owner chart and metric totals continue to come from
`/estimated_spend?group_by=user`. The hierarchy table's owner rows come from
`/estimated_spend/drilldown?level=owner`, so the table is not constrained by
the chart endpoint's top-50 group limit. The drill-down endpoint returns one
page at a time and reports `total`, `offset`, `limit`, and `has_more`. The
dashboard appends subsequent pages with a `Load more` control scoped to the
expanded parent, including at the owner root.

Only the user view is hierarchical in v0. The existing flat workload and
purchase-option views remain available for chart analysis and backwards
compatibility.

### Drill-down contract

The endpoint accepts:

- `level`: `owner`, `workload`, `task`, or `cluster`
- `start_date` and `end_date`, with the same 90-day rules as
  `/estimated_spend`
- `owner_user_hash` for an attributed owner, or `owner_unknown = true` for
  rows whose `user_hash` is null; descendant levels require exactly one form
  of owner scope
- `workload_type` and `workload_id` for task or cluster descendants
- `workload_task_id` for cluster descendants of a task
- `offset` and `limit`, where `limit` is bounded to 100

Each row carries the same aggregate fields used by the existing table:
`estimated_cost`, `spot_estimated_cost`, `on_demand_estimated_cost`,
`priced_machine_seconds`, and `excluded_machine_seconds`.

Owner rows carry `user_hash`, `user_name`, `owner_unknown`, `workload_count`,
and `cluster_count`. Workload rows additionally carry `task_count`,
`unknown_task_cluster_count`, and `cluster_count`. Task rows carry
`workload_task_id` and `cluster_count`. Cluster rows carry `cluster_hash`,
`cluster_name`, and the recorded workspace.

For workload rows, source records with `workload_type = managed` are grouped as
one synthetic workload:

```text
workload_type = managed_unattributed
workload_id = null
label = Legacy managed, parent unknown
```

This is a read-time presentation grouping. The source rows are not rewritten.
Expanding the synthetic workload filters the original `managed` rows and shows
their physical clusters, so no cost becomes inaccessible.

The synthetic identity is exclusive to the additive drill-down endpoint. The
existing flat `/estimated_spend` response retains its established `managed`
type and stored workload IDs so an older dashboard never receives a grouping
value it cannot label correctly. The new dashboard labels those flat rows as
legacy physical attempts rather than presenting their IDs as proven parents.

The endpoint rejects incomplete or contradictory scope combinations with a
422 response. It applies the same admin authorization as `/estimated_spend`.

### Dashboard behavior

An expandable row uses a button with an accessible name and `aria-expanded`.
Loading and error states are local to that row, so a failed descendant query
does not replace the valid root estimate. When a later page fails, already
loaded descendants remain visible and Retry resumes from their current count.

A managed workload with more than one distinct non-null task ID expands to
task rows only when every physical attempt has an evidenced task ID. A
workload with any null-task attempt expands directly to all physical attempts,
so an incomplete historical task mapping cannot hide cost. All other
workloads also expand directly to physical attempts. This keeps the common
case concise while retaining the task boundary when it is both meaningful and
complete.

The hierarchy root refreshes when `/estimated_spend` reports a new `as_of`
snapshot, not on every one-minute browser poll. This keeps hierarchy totals
aligned with the metric cards without repeatedly collapsing expanded rows
between materialized rollup updates.

When the chart contains an `Other` series, its description states the number
of chart-leading groups and explains that all remaining groups can be reached
through the owner hierarchy. `Other` is never rendered as a workload row.

### Compatibility

The existing `/estimated_spend` response and grouping values remain unchanged,
including legacy `managed` rows and their stored identifiers. Synthetic
`managed_unattributed` rows appear only on the new drill-down route. Older
dashboard clients ignore the new route. During a
partially upgraded deployment, a server that supports user breakdowns but not
the new route falls back to the existing flat user table when a drill-down
request returns 404 or 405 and surfaces a small unavailable message. Older
servers without breakdowns retain the dashboard's existing fallback to their
flat workload table.

This is an internal admin dashboard addition and does not change the public
SkyPilot SDK contract.

## Alternatives Considered

### Return the whole tree from `/estimated_spend`

This is simpler for the browser but unbounded for 90-day ranges with thousands
of legacy rows and recovery attempts. It also repeats descendants whenever the
chart grouping changes.

### Backfill all legacy parent jobs

Historical identifiers cannot always be mapped to a parent job
deterministically. A speculative backfill would create false accounting
precision. Verified mappings can be added in a separate data-repair change.

### Join provider billing into this release

AWS CUR or Cost Explorer ingestion needs a billing-read role, freshness and
retention policy, reconciliation semantics, and a separate
`billed_spend_daily` data model. Combining it with the attribution hierarchy
would increase rollout risk and still would not make legacy parent mappings
knowable.

## Rollout and Rollback

Deploy the additive endpoint and dashboard together in the API-server image.
No schema migration or backfill is required. The release uses the existing
Helm values and PostgreSQL database.

Rollback is an ordinary Helm rollback to the prior image. The new route holds
no state, and the previous dashboard continues to use `/estimated_spend`.

## Test Plan

Backend unit tests must prove:

- Admin authorization and invalid-scope rejection.
- Owner pagination and filtering, including explicit null-owner scope.
- Managed-job aggregation across multiple physical attempts.
- Multi-task aggregation and task-scoped cluster results.
- Null-task attempts bypass an incomplete task hierarchy without lost cost.
- Legacy `managed` consolidation without lost cost.
- Flat legacy workload identities remain unchanged for older clients.
- Deterministic ordering, pagination metadata, and exact date filtering.
- Parent totals equal the sum of all unpaginated children in representative
  fixtures.

Dashboard tests must prove:

- User is the default grouping.
- Expanding an owner loads workloads for the selected date range.
- A multi-task managed job expands into tasks and then attempts.
- A single-task workload expands directly into attempts.
- `Load more` appends without replacing existing children.
- A failed later page preserves loaded children and retries at the same offset.
- Row-local loading, failure, and unsupported-server states.
- A new materialized snapshot refreshes the hierarchy root.
- The chart copy explains `Other` without presenting it as an attributable
  workload.

Run the focused backend and dashboard suites, formatter, dashboard production
build, and the full PR CI rollup on the exact pushed commit. After deployment,
verify the Helm revision, pod image and commit, root owner totals, an expanded
managed job, and an expanded standalone-cluster owner in production.
