# Dashboard tab-scoped service details

## Problem

The service detail route always starts both service reads:

1. A summary read for the header, replica history, and service-level fields.
2. A full read for the replica list.

The full read is materially more expensive at fleet scale, but the Versions and
Placement tabs render their own connector-backed content and do not consume the
replica list. Opening either tab therefore pays for a slow full read that cannot
affect the selected view.

## Behavior contract

- Versions and Placement initial loads request only the service summary.
- Overview initial loads retain the existing summary-plus-full behavior.
- Moving from a non-Overview tab to Overview for the same service starts exactly
  one deferred full read and reuses the already visible summary.
- Every service-route generation acquires a fresh summary, including an A-B-A
  navigation where a same-name snapshot from the first A is still visible.
- Moving away from Overview does not start another read when the current
  service snapshot is already visible.
- Manual refresh and polling request only the data required by the active tab.
- Request version fencing continues to reject late results from an older
  service, tab mode, or refresh owner.
- A summary failure cannot expose data owned by another service or make an
  initial route appear settled.
- Refresh ownership is reused only when the owner includes every read required
  by the new caller.

## Performance invariant

An initial Versions or Placement load makes one
`dashboardCache.get(getServices, ...)` call, not two. The change adds only
constant-time mode checks and constant-size ownership metadata. It adds no
timer, retry, query, connector call, or state proportional to service or
replica count.

## Alternatives

### Keep eager full reads

This is simpler locally but preserves the known unnecessary connector work and
the fleet-scale latency it causes.

### Split the hook into one summary hook and one replica hook

Separate hooks make tab demand explicit, but duplicate route fencing, polling,
manual refresh, invalidation, and ownership state. That increases the chance
that the two hooks publish mismatched snapshots.

### Cache the eager full read more aggressively

Caching reduces repeated work but does not remove the unnecessary first full
read. It also risks extending the lifetime of stale replica data.

The selected approach keeps one route owner and makes the required read set an
input to that owner.

## Changed-path-to-test matrix

| Changed path | Invariant | Test file | Command |
| --- | --- | --- | --- |
| `sky/dashboard/src/pages/services/[service].js` | Versions and Placement skip the full read | `sky/dashboard/src/tests/service-details.test.jsx` | `npm --prefix sky/dashboard test -- --runInBand src/tests/service-details.test.jsx` |
| `sky/dashboard/src/pages/services/[service].js` | Placement-to-Overview starts one deferred full read | `sky/dashboard/src/tests/service-details.test.jsx` | `npm --prefix sky/dashboard test -- --runInBand src/tests/service-details.test.jsx` |
| `sky/dashboard/src/pages/services/[service].js` | A-B-A navigation and late completions stay fenced | `sky/dashboard/src/tests/service-details.test.jsx` | `npm --prefix sky/dashboard test -- --runInBand src/tests/service-details.test.jsx` |
| `sky/dashboard/src/pages/services/[service].js` | Refresh failure releases ownership and later calls retry | `sky/dashboard/src/tests/service-details.test.jsx` | `npm --prefix sky/dashboard test -- --runInBand src/tests/service-details.test.jsx` |
| `sky/dashboard/src/pages/services/[service].js` | Polling and manual refresh do not overlap or invalidate unrelated keys | `sky/dashboard/src/tests/service-details.test.jsx` | `npm --prefix sky/dashboard test -- --runInBand src/tests/service-details.test.jsx` |
| `sky/dashboard/src/pages/services/[service].js` | Service connectors and tab-specific fetchers keep their contracts | `sky/dashboard/src/data/connectors/services.test.jsx`, `sky/dashboard/src/components/service-placement.test.jsx`, `sky/dashboard/src/components/service-version-history.test.jsx` | `npm --prefix sky/dashboard test -- --runInBand src/data/connectors/services.test.jsx src/components/service-placement.test.jsx src/components/service-version-history.test.jsx` |

The Dashboard workflow has no pull-request path filter and runs all four test
files above, followed by lint, formatting, and a production build.

## Milestones

1. Pin the non-Overview call count with a regression test that fails on the
   parent behavior.
2. Pass the active tab's read requirements into the existing route owner.
3. Reconcile the implementation with the current A-B-A route ownership guard.
4. Run the focused matrix, the exact Dashboard workflow test list, lint,
   formatting, build, and diff validation.
5. Push one exact head and require every relevant CI job to pass on that head.

## Rollout and rollback

This is a dashboard-only behavior change with no persisted state, API schema, or
server migration. Rollback is the single PR revert. A regression would be
visible as a missing service header, a stuck loading state, duplicate connector
calls, or stale service data after route or tab changes.
