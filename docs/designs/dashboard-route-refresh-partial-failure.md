# Dashboard route refresh partial-failure ownership

## Context

The service-detail page starts a fast summary read and a slower full-detail
read for each route. PR #995 correctly reuses both reads when a user requests a
manual refresh before either read has produced data for the new route. However,
the same name-based rule also reuses the aggregate load after the summary read
has failed. At that point the refresh button is available, old-route data is
still visible, and the only remaining work is the slow full read. Reusing that
load makes the manual retry ineffective.

## Behavior contract

- A manual refresh while the new route's summary is still pending reuses the
  route load and does not invalidate or duplicate either request.
- Once that summary settles, whether successfully or with an error, a manual
  refresh may supersede the remaining initial load.
- Concurrent manual refreshes continue to share one manual owner.
- Superseded summary and full responses remain fenced by request version and
  cannot overwrite the retry.
- Polling continues to reuse any load for the same service.

## Design

Add a `summaryPending` field to the existing refresh-owner record. Set it before
publishing the owner, clear it when that owner's summary settles, and require it
when a superseding manual refresh considers reusing an initial load whose
visible data belongs to another service.

This keeps ownership in the existing single-flight record. It adds no state,
render, timer, request, or cache operation to the normal path.

## Alternatives

- Keep reusing until the full read settles. Rejected because it makes the
  enabled refresh control unable to retry a failed summary.
- Clear old-route `serviceData` during navigation. Rejected because it removes
  the intentionally preserved snapshot and broadens the user-visible change.
- Track summary errors in React state. Rejected because ownership-local pending
  state is sufficient and avoids an extra render.

## Milestones and rollout

1. Add a deterministic test that is green on #995's parent and red on its merge.
2. Add ownership-local summary settlement tracking.
3. Run focused and full service-detail tests, dashboard lint, formatting, build,
   and exact-head CI.

The change is immediately reversible and needs no migration or feature flag.

## Test plan

- Route A completes, route B starts, B summary fails, and B full remains
  pending. A manual refresh must invalidate B's two cache keys, start exactly
  one replacement pair, and fence the stale first full response.
- The landed #995 route-transition test must retain one summary and one full
  request while B's summary is pending.
- Existing tests must continue to cover current-service supersession,
  concurrent manual coalescing, polling ownership, route-return cleanup, and
  stale-response fencing.
