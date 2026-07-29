# Dashboard route refresh partial-failure ownership

## Context

The service-detail page starts a fast summary read and a slower full-detail
read for each route. PR #995 correctly reuses both reads when a user requests a
manual refresh before either read has produced data for the new route. However,
the same name-based rule also reuses the aggregate load after the summary read
has failed. At that point the only remaining work is the slow full read.
Reusing that load makes a programmatic retry ineffective.

The initial page gate also historically waited for the summary request even
when the full-detail request had already produced a renderable current-route
snapshot. Conversely, after route ownership began hiding old-route data, a
failed summary must not settle the page while the new route's full read remains
pending. Otherwise service A's retained hook snapshot can make service B render
an empty state before either B request finds or rules out the service.

## Behavior contract

- A manual refresh while the new route's summary is still pending reuses the
  route load and does not invalidate or duplicate either request.
- Once that summary settles, whether successfully or with an error, a manual
  refresh may supersede the remaining initial load.
- Concurrent manual refreshes continue to share one manual owner.
- Superseded summary and full responses remain fenced by request version and
  cannot overwrite the retry.
- Polling continues to reuse any load for the same service.
- The initial page gate settles as soon as either request produces current-route
  service data.
- A retained snapshot from another route never settles the current route.
- If neither request finds the service, the page settles only after both
  requests finish.

## Design

Add a `summaryPending` field to the existing refresh-owner record. Set it before
publishing the owner, clear it when that owner's summary settles, and require it
when a superseding manual refresh considers reusing an initial load whose
visible data belongs to another service.

Track summary and full settlement inside the existing request closure. A single
load-gate helper checks request ownership, a newly landed current-route
snapshot, the visible snapshot's service name, and joint settlement. Both
request success and settlement paths call that helper.

This keeps ownership in the existing single-flight record. It adds no React
state, render, timer, request, or cache operation to the normal path.

## Alternatives

- Keep reusing until the full read settles. Rejected because it makes the
  enabled refresh control unable to retry a failed summary.
- Clear old-route `serviceData` during navigation. Rejected because it removes
  the intentionally preserved snapshot and broadens the user-visible change.
- Track summary errors in React state. Rejected because ownership-local pending
  state is sufficient and avoids an extra render.
- Let any retained snapshot settle the page. Rejected because route ownership
  deliberately hides data whose service name differs from the current route.

## Milestones and rollout

1. Add a deterministic test that is green on #995's parent and red on its merge.
2. Add ownership-local summary settlement tracking.
3. Let either current-route snapshot settle the initial load while requiring
   both requests to settle before publishing an empty state.
4. Run focused and full service-detail tests, dashboard lint, formatting,
   build, and exact-head CI.

The change is immediately reversible and needs no migration or feature flag.

## Test plan

- Route A completes, route B starts, B summary fails, and B full remains
  pending. A manual refresh must invalidate B's two cache keys, start exactly
  one replacement pair, and fence the stale first full response.
- The landed #995 route-transition test must retain one summary and one full
  request while B's summary is pending.
- A full current-route snapshot that lands before the summary must settle the
  initial load without starting another cache read.
- A failed summary with a pending full read must keep the initial gate closed,
  including when the hook retains a snapshot from the previous route.
- Existing tests must continue to cover current-service supersession,
  concurrent manual coalescing, polling ownership, route-return cleanup, and
  stale-response fencing.

## Changed-path-to-test matrix

| Changed production path or invariant | Test file and focused command |
| --- | --- |
| `useServiceDetails` initial load gate: full-first success, summary failure, joint empty settlement | `sky/dashboard/src/tests/service-details.test.jsx`; `npm --prefix sky/dashboard test -- --runInBand src/tests/service-details.test.jsx` |
| Route transition: an old-route snapshot cannot settle the new route | `sky/dashboard/src/tests/service-details.test.jsx::retries a failed new-route summary while the full read is pending`; same command |
| Request ownership, supersession, polling, and cache call counts | full `service-details.test.jsx`; same command |
| Rendered route ownership | `ServiceDetails route ownership rendering` in the same file; same command |
| Adjacent service views | `npm --prefix sky/dashboard test -- --runInBand src/tests/service-details.test.jsx src/components/services.test.jsx src/components/service-version-history.test.jsx src/components/service-placement.test.jsx` |
| Lint, formatting, and production build | `npm --prefix sky/dashboard run lint`; `npm --prefix sky/dashboard run format:check`; `npm --prefix sky/dashboard run build` |

`.github/workflows/dashboard.yml` runs the focused suite, lint, format check,
and build for every pull request targeting `improvements`.

## Performance

The full-first regression pins exactly two cache reads, one summary and one
full-detail request. The gate adds constant-time request-local boolean and
service-name checks only at promise success or settlement. It adds no fetch,
invalidation, retry, timer, poll, scan, or render state.
