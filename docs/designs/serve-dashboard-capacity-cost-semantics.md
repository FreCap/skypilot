# SkyServe dashboard capacity and cost semantics

## Problem

The service dashboard currently makes three different control-plane concepts
look interchangeable:

- `demand_target` is the traffic target after autoscaler hysteresis, not raw
  request volume.
- `capacity_target` is the greater of the traffic target and reserved-capacity
  fill target, not a second traffic estimate.
- `total_capacity` excludes failed statuses but includes stopping and preempted
  rows until their lifecycle records are reconciled.

The details card also calls failed capacity simply "failed" even though those
rows persist as history, and its cost aggregation prices every row carrying a
catalog price. That can make completed historical failures appear in the
estimated hourly cost long after their provider resources are gone.

## Behavior contract

- Label `demand_target` as the traffic target and explain that it includes
  autoscaler hysteresis.
- Label `capacity_target` as the traffic or reservation target because it is
  their maximum.
- Label `total_capacity` as non-failed tracked capacity, including stopping and
  preempted rows.
- Describe detail-card denominators as ready versus non-failed capacity. Treat
  the failed aggregate as a mixed bucket: it includes completed failure history
  as well as `FAILED_CLEANUP` and `UNKNOWN` rows whose provider cleanup may
  still be uncertain.
- Estimate cost only for rows with plausible current provider billability:
  provisioning, starting, ready, not ready, stopping, cleanup-failed, unknown,
  and any future status not explicitly known to be non-billable.
- Exclude pending intent, known completed failures, and provider-preempted rows
  from both priced and unpriced cost counts.
- Keep `SHUTTING_DOWN`, `FAILED_CLEANUP`, and `UNKNOWN` in the estimate because
  the provider may still bill them until cleanup is proven.
- State that the estimate uses the current catalog, covers compute only, and is
  not a provider bill.

## Design

The backend API and PostgreSQL history schema remain unchanged. The existing
fields already distinguish the relevant concepts; this change corrects their
presentation and frontend aggregation.

In the service connector, define the statuses known not to represent current
billability and filter them before aggregating catalog prices or exclusion
reasons. This is intentionally an exclusion list: an unknown future status is
safer to include than to silently understate possible spend.

In the history chart and details card, update labels and explanatory copy to
match the backend definitions. Failed rows remain available and are not deleted
by this change. Since the existing aggregate does not separate completed
failure history from cleanup-uncertain rows, its label must preserve both
possibilities rather than calling every failed row historical.

## Alternatives considered

- Add a separate `fill_target` database column. Rejected for this fix because
  it requires a migration and does not improve the accuracy of the existing
  combined capacity target.
- Use only `READY` rows for cost. Rejected because stopping and cleanup-failed
  infrastructure can still be provider-billed.
- Delete failed rows after a TTL. Rejected because retention is a separate
  product policy and those rows are useful operational evidence.

## Rollout and observability

This ships with the API-server dashboard bundle and requires no migration.
After deploy, verify labels on the production service, compare the displayed
estimate with the active provider inventory, and confirm historical failures
remain visible without contributing to the estimate.

## Test plan

- Connector tests cover active, stopping, cleanup-failed, preempted, pending,
  and historical failed rows with catalog prices.
- Component tests cover the chart labels and explanatory copy.
- Details-card tests cover ready/non-failed and historical-failure wording plus
  the provider-billing disclaimer.
- Run the focused dashboard Jest files, formatter, lint, and production build.
