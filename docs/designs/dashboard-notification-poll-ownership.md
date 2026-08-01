# Own visible operator-notification polling by admin lifecycle

## Problem

The dashboard notification bell starts a request immediately and every 60
seconds, but does not own the in-flight request. Slow responses therefore allow
overlapping polls and out-of-order state publication. Its boolean `mountedRef`
also becomes true again after an admin-to-user-to-admin role transition, so a
request from the old admin lifecycle can overwrite the new lifecycle.

The lifecycle owner now coalesces and fences those requests, but its private
interval still polls while the document is hidden and does not refresh when the
document becomes visible. An inactive admin tab can therefore issue one
backend read per minute indefinitely, while the bell can remain stale for
almost another minute after the admin returns.

## Behavior contract

- One admin lifecycle has at most one notification poll in flight. Overdue
  interval ticks reuse that promise instead of starting more backend calls.
- Success and failure release only their exact owner, so the next interval can
  refresh again.
- Unmount or any role transition invalidates the active generation. A stale
  poll or acknowledgement cannot publish data or errors into a later admin
  lifecycle.
- Returning to the admin role starts a fresh request immediately.
- Hidden interval ticks issue no connector calls. Returning to a visible
  document starts exactly one immediate refresh, suppresses the adjacent timer
  boundary, and resumes one refresh per full visible interval.
- Non-admin users never poll or subscribe to visibility changes. Unmount and
  role exit remove interval and visibility ownership, and existing bell,
  acknowledgement, error, and unread rendering behavior stays unchanged.

## Design

Replace the boolean-only lifecycle check with a monotonically increasing
generation. Store the active poll as `{generation, promise}`. `refresh()`
returns the matching promise when a poll already owns that generation, and an
exact-promise `finally` releases it after either outcome. Effect cleanup
invalidates the generation and clears only its owner.

Acknowledgement captures the same generation before awaiting the connector,
so its success or failure cannot cross a role transition. This keeps all
asynchronous state publication behind one lifecycle proof without abort
controllers, extra render state, or connector changes.

Keep the immediate admin-lifecycle refresh in the lifecycle effect, then
delegate recurring timer, visibility, callback freshness, and cleanup ownership
to `useVisibleRefreshInterval`. Enable the shared hook only for admins and pass
the current lifecycle generation into the existing coalescing `refresh()`
owner. The hook already skips hidden ticks and suppresses a timer boundary for
one full interval after a visibility-triggered refresh.

## Alternatives considered

- Allow overlapping polls and only fence publication. Rejected because it
  preserves unnecessary backend work and request activity.
- Abort the old HTTP request. Rejected because the connector has no abort
  contract and generation fencing already provides deterministic ownership.
- Use only `mountedRef`. Rejected because the same component can become mounted
  for a new role lifecycle before an old promise settles.
- Add a visibility listener beside the private interval. Rejected because it
  duplicates timer suppression, callback freshness, and cleanup behavior
  already centralized in `useVisibleRefreshInterval`.
- Pause while hidden without refreshing on restoration. Rejected because it
  saves backend work but leaves notification liveness stale until the next
  interval.

## Changed-path-to-test matrix

| Changed path                                                           | Invariants                                                                                                                                                                                                                                                                                                                                                                                           | Test and command                                                                                                                                                                          | CI coverage                                                                                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `sky/dashboard/src/components/elements/operator-notification-bell.jsx` | initial admin render starts one refresh; hidden intervals start zero reads; visibility restoration starts exactly one immediate refresh; the adjacent scheduled tick starts zero extra reads; one full visible interval later starts one refresh; unmount or role exit removes timer and visibility ownership                                                                                          | `sky/dashboard/src/components/elements/operator-notification-bell.test.jsx`; `npm --prefix sky/dashboard test -- --runInBand src/components/elements/operator-notification-bell.test.jsx` | `.github/workflows/dashboard.yml`, `Dashboard Testing and Formatting / dashboard` explicitly runs this suite with no path filter |
| `sky/dashboard/src/components/elements/operator-notification-bell.jsx` | overlapping visible ticks coalesce; exact success/failure release; admin role exit and unmount fence stale poll/acknowledgement results; admin re-entry refreshes immediately; local acknowledgement remains monotonic across a stale poll                                                                                                  | Existing lifecycle cases in `sky/dashboard/src/components/elements/operator-notification-bell.test.jsx`; same command                                                                     | Same dashboard job                                                                                                               |
| `sky/dashboard/src/components/elements/operator-notification-bell.jsx` | no material performance regression: hidden request count stays zero, restoration performs one read without an adjacent duplicate, and steady visible cadence remains one read per interval; visibility restoration during an in-flight owner reuses that promise rather than adding a call                                                                                                               | Exact `getOperatorNotifications` call counts in `sky/dashboard/src/components/elements/operator-notification-bell.test.jsx`; same command                                                  | Same dashboard job                                                                                                               |
| `docs/designs/dashboard-notification-poll-ownership.md`                | behavior contract, alternatives, rollout, and the changed-path-to-test matrix remain synchronized                                                                                                                                                                                                                                                                                                     | design and diff review; `git diff --check origin/improvements...HEAD`                                                                                                                     | reviewer-visible PR diff and `format`                                                                                            |

## Performance evidence

Fake-timer call-count assertions pin three overdue ticks during one slow poll to
one connector call. After settlement, the next tick makes exactly one new call.
Visibility call-count assertions additionally pin zero reads while hidden,
exactly one immediate restoration read, zero adjacent-boundary duplicates, and
one read on the next full cadence. Coordination remains O(1), reuses the shared
single timer and listener owner, adds no render state or cache scan, and reduces
hidden backend traffic from one read per minute to zero.

## Rollout and verification

Add failing visibility-restoration, hidden-call-count, adjacent-boundary, and
cleanup assertions before the production change. Preserve the existing overlap,
failure-retry, stale-lifecycle, acknowledgement, and role-transition corpus.
Then implement the shared interval owner and run the focused suite, the exact
dashboard CI Jest inventory, ESLint, Prettier check, Next.js build, and
`git diff --check`. No migration or feature flag is needed; reverting this
component change restores the private polling interval.
