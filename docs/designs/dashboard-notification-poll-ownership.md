# Own operator-notification polling by admin lifecycle

## Problem

The dashboard notification bell starts a request immediately and every 60
seconds, but does not own the in-flight request. Slow responses therefore allow
overlapping polls and out-of-order state publication. Its boolean `mountedRef`
also becomes true again after an admin-to-user-to-admin role transition, so a
request from the old admin lifecycle can overwrite the new lifecycle.

## Behavior contract

- One admin lifecycle has at most one notification poll in flight. Overdue
  interval ticks reuse that promise instead of starting more backend calls.
- Success and failure release only their exact owner, so the next interval can
  refresh again.
- Unmount or any role transition invalidates the active generation. A stale
  poll or acknowledgement cannot publish data or errors into a later admin
  lifecycle.
- Returning to the admin role starts a fresh request immediately.
- Non-admin users never poll, timers are cleared on cleanup, and existing bell,
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

## Alternatives considered

- Allow overlapping polls and only fence publication. Rejected because it
  preserves unnecessary backend work and request activity.
- Abort the old HTTP request. Rejected because the connector has no abort
  contract and generation fencing already provides deterministic ownership.
- Use only `mountedRef`. Rejected because the same component can become mounted
  for a new role lifecycle before an old promise settles.

## Changed-path-to-test matrix

| Changed path                                                           | Invariants                                                                                                                                                                                                  | Test and command                                                                                                                                                                          | CI coverage                                                                                                                      |
| ---------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `sky/dashboard/src/components/elements/operator-notification-bell.jsx` | overlapping ticks coalesce; exact success/failure release; admin role exit and unmount fence stale poll/acknowledgement results; admin re-entry refreshes immediately; zero extra timers or connector calls | `sky/dashboard/src/components/elements/operator-notification-bell.test.jsx`; `npm --prefix sky/dashboard test -- --runInBand src/components/elements/operator-notification-bell.test.jsx` | `.github/workflows/dashboard.yml`, `Dashboard Testing and Formatting / dashboard` explicitly runs this suite with no path filter |
| `docs/designs/dashboard-notification-poll-ownership.md`                | contract, alternatives, rollout, and evidence remain synchronized                                                                                                                                           | design and diff review; `git diff --check origin/improvements...HEAD`                                                                                                                     | reviewer-visible PR diff and `format`                                                                                            |

## Performance evidence

Fake-timer call-count assertions pin three overdue ticks during one slow poll to
one connector call. After settlement, the next tick makes exactly one new call.
Coordination uses two refs and O(1) comparisons, with no additional timer,
render, cache scan, or backend request.

## Rollout and verification

Add failing overlap, failure-retry, and role-transition tests before production
changes. Then implement the owner, run the focused suite, the exact dashboard
CI Jest inventory, ESLint, Prettier check, Next.js build, and `git diff --check`.
No migration or feature flag is needed; reverting this component change restores
the former polling behavior.
