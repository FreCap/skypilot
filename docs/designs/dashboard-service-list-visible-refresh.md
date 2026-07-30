# Refresh the Service List When a Hidden Tab Becomes Visible

_Created: 2026-07-30_

## Problem

The service list skips its 30-second polling tick while the browser tab is
hidden. Returning to the tab does not request a new fleet summary, however.
The page can therefore continue showing the pre-hide snapshot for almost a full
polling interval.

The list already has one request owner for automatic work, explicit
supersession for manual refresh, and stale-response fencing. The missing
behavior is an immediate fresh read through that ownership path when the
document becomes visible again.

Managed Jobs has the same timer and visibility lifecycle in a private hook.
Keeping that lifecycle private would require the service list to duplicate its
listener cleanup, current-callback ref, and adjacent-tick suppression.

## Behavior Contract

- Initial service loading remains independent of document visibility.
- Manual refresh remains independent of document visibility.
- Periodic ticks while hidden perform zero service-summary reads.
- A transition to visible invalidates only the summary cache key and requests
  one immediate summary read.
- The visibility read supersedes an older automatic owner so a pre-hide result
  cannot overwrite the fresh snapshot, but it reuses a manual or visibility
  refresh that already owns a fresh read.
- A periodic tick immediately after a visibility refresh is suppressed. Later
  periodic ticks resume at the existing interval.
- Unmount removes both the timer and visibility listener and fences pending
  responses.
- Managed Jobs keeps its existing pool and job refresh behavior after the
  polling hook moves to a shared module.

## Solution

Move `useVisibleRefreshInterval` from `jobs.jsx` to a dashboard hook module and
reuse it from the service list. The hook owns one interval and one
`visibilitychange` listener, reads the current callback through a ref, and
forwards the trigger source to the existing consumer refresh owner.

The hook records a visibility-triggered refresh timestamp. If the periodic
timer fires within the same interval window, it skips that tick. This keeps the
existing steady-state cadence without issuing an adjacent duplicate read.

The service-list request owner records whether work is automatic, manual, or
visibility-triggered. A visibility transition first reuses an in-flight manual
or visibility owner. Otherwise it invalidates only
`getServices({summaryOnly: true})` and starts a superseding read through the
existing request-version fence. Manual refresh continues to invalidate every
`getServices` variant.

## Alternatives Considered

Adding another private helper to `services.jsx` duplicates lifecycle code that
Managed Jobs already relies on. Exporting the private helper from `jobs.jsx`
would make an unrelated page component the owner of shared polling behavior.

Calling the existing automatic `fetchData()` on visibility restore would reuse
a pre-hide request and could leave the page stale. Fetching without cache
invalidation could also return the two-minute cached summary while only
refreshing the cache in the background.

Resetting the interval on every visibility transition adds timer state and
delays the normal cadence. The timestamp guard is smaller and preserves the
existing interval owner.

## Changed-Path-to-Test Matrix

| Changed path                                           | Invariant                                                                                                                                                                                     | Test and command                                                                                                                                                                                                  |
| ------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sky/dashboard/src/hooks/useVisibleRefreshInterval.js` | Hidden ticks do no work; visibility restore refreshes once; an adjacent interval does not duplicate the refresh; unmount removes the listener and timer                                       | `sky/dashboard/src/components/services.test.jsx` and `sky/dashboard/src/components/jobs.test.jsx`; `npm --prefix sky/dashboard test -- --runInBand src/components/services.test.jsx src/components/jobs.test.jsx` |
| `sky/dashboard/src/components/services.jsx`            | Initial and manual loading are unchanged; visibility restore invalidates only the summary key, supersedes a pre-hide automatic request, reuses a manual owner, and keeps stale results fenced | `sky/dashboard/src/components/services.test.jsx`; same command                                                                                                                                                    |
| `sky/dashboard/src/components/jobs.jsx`                | Extracting the polling helper preserves pool and job ownership, hidden-tab gating, dynamic batch intervals, adjacent-tick suppression, and cleanup                                            | Existing lifecycle cases in `sky/dashboard/src/components/jobs.test.jsx`; same command                                                                                                                            |

## Performance Evidence

Focused tests assert exact service-summary read counts. Hidden intervals remain
at zero reads. Each visible transition adds exactly one read, and the adjacent
timer test proves it does not add a second periodic read. The steady visible
path keeps one interval and one constant-time visibility check per tick.

The hook extraction changes no Managed Jobs request counts. The full existing
jobs lifecycle suite remains the regression gate for that consumer.

## Rollout and Verification

This is a dashboard-only lifecycle change with no API, schema, or migration.
Run the focused service and jobs suites, then the exact CI-listed dashboard Jest
suite, dashboard lint, format check, production build, and `git diff --check`.

`.github/workflows/dashboard.yml` explicitly runs both focused test files for
pull requests targeting `improvements`, followed by the dashboard lint,
formatting, and build gates.
