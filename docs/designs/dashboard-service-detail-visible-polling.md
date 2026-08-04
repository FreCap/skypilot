# Pause Service-Detail Polling in Hidden Tabs

_Created: 2026-07-30_

## Problem

The service-detail hook refreshes the selected service every 60 seconds by
invalidating and fetching both its summary and full replica snapshot. The timer
runs while the browser tab is hidden, even though no user can observe the new
snapshot. A background service page therefore continues two potentially
expensive reads per interval and can contend with foreground dashboard work.

The hook already owns initial, manual, and periodic refreshes and coalesces
overlapping refreshes. The missing condition is visibility at the periodic
refresh boundary.

## Behavior Contract

- Initial loading still fetches the selected service regardless of browser
  visibility so the route can become ready.
- Explicit manual refresh still fetches regardless of visibility.
- A periodic tick while visible invalidates and fetches the selected summary
  and full replica keys exactly once.
- A periodic tick while hidden performs no invalidation and no fetch.
- Existing in-flight coalescing, route ownership, stale-response fencing, and
  unmount cleanup remain unchanged.

## Solution

Gate the existing periodic callback on `window.document.visibilityState`. Keep
the current timer owner and refresh function rather than adding a second
visibility-event lifecycle. The next visible timer boundary resumes polling
through the existing coalescing path.

This adds one constant-time browser-state read per interval. Hidden tabs remove
two cache invalidations and two scoped service reads per elapsed interval.

## Alternatives Considered

A `visibilitychange` listener could refresh immediately when a tab becomes
visible, but it would create a second event owner and can issue an extra refresh
after even a brief tab switch. Stopping and recreating the timer while hidden
adds more lifecycle state without improving the zero-request invariant. The
single callback guard is the smallest sufficient change.

## Changed-Path-to-Test Matrix

| Changed path | Invariant | Test and command |
| --- | --- | --- |
| `sky/dashboard/src/pages/services/[service].js` | Visible periodic ticks invalidate and fetch the summary and full replica snapshots once | `sky/dashboard/src/tests/service-details.test.jsx`; `npm --prefix sky/dashboard test -- --runInBand src/tests/service-details.test.jsx` |
| `sky/dashboard/src/pages/services/[service].js` | Hidden periodic ticks perform zero invalidations and zero fetches, then the next visible tick resumes exactly once | `sky/dashboard/src/tests/service-details.test.jsx`; same command |
| `sky/dashboard/src/pages/services/[service].js` | Initial and manual refresh ownership, slow-refresh coalescing, route transitions, and unmount cleanup remain intact | Existing lifecycle cases in `sky/dashboard/src/tests/service-details.test.jsx`; same command |

## Performance Evidence

The new boundary test asserts exact cache invalidation and fetch counts across
hidden and visible intervals. The hidden cost changes from two invalidations
and two service reads per 60-second interval to zero. The visible path adds one
constant-time visibility read and otherwise keeps the same call count and
complexity.

## Rollout and Verification

This is a dashboard-only lifecycle change with no API or schema migration. Run
the focused service-detail tests, then the complete CI-listed dashboard Jest
suite, lint, format-check, production build, and `git diff --check`.

`.github/workflows/dashboard.yml` runs
`src/tests/service-details.test.jsx` for pull requests targeting
`improvements`, followed by dashboard lint, formatting, and build checks.
