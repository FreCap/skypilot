# Refresh Managed Jobs When a Hidden Tab Becomes Visible

_Created: 2026-07-30_

## Problem

The managed-jobs page already avoids its periodic pool and job reads while the
browser tab is hidden. Returning to the tab does not refresh either snapshot,
however. The page can therefore show stale pool state until the next 30-second
tick and stale job state until the next 5-second tick, or the next 1-second tick
when batch progress is active.

The pool and job surfaces have separate refresh owners, in-flight coalescing,
and stale-response fences. The missing behavior is an immediate refresh through
each existing owner when the document becomes visible again.

## Behavior Contract

- Initial pool and job loading remains independent of document visibility.
- Manual refresh remains independent of document visibility.
- Periodic ticks while hidden perform zero pool and job refresh work.
- A transition to visible requests one immediate refresh from each enabled
  surface through its existing refresh owner.
- A periodic tick immediately after a visibility refresh is suppressed. Later
  periodic ticks resume at the existing interval.
- In-flight automatic refreshes remain coalesced. A visibility refresh
  supersedes only an older automatic owner and reuses an in-flight manual or
  interactive owner.
- Filter, pagination, sort, manual, and post-controller-restart reads supersede
  any older snapshot. An older request may finish, but cannot publish state or
  clear the newer request owner.
- Dynamic batch intervals and cache invalidation behavior remain unchanged.
- Disabling or unmounting a surface removes both its timer and visibility
  listener, so later events perform zero work.

## Solution

Use the shared `useVisibleRefreshInterval` dashboard hook. It owns a single
interval and `visibilitychange` listener, reads the current callback through a
ref so callback updates do not recreate the timer, and forwards the trigger
source to the existing refresh owner. The service list now reuses the same
lifecycle instead of duplicating it.

The managed-jobs table classifies the one active request as automatic,
visibility, interactive, or manual. Periodic ticks reuse any active owner.
Visibility restoration reuses manual and interactive work, but replaces a
pre-hide automatic owner. Interactive callers supersede by default so deferred
page-reset options and the controller-restart path cannot accidentally reuse a
request made for older UI state. A single request sequence fences publication.

The helper records a visibility-triggered refresh timestamp. If the existing
periodic timer fires within the same interval window, it skips that one tick to
avoid a redundant request. It does not replace or bypass the consumers'
in-flight ownership. The timestamp uses the browser's monotonic performance
clock so wall-clock corrections cannot suppress later polling.

## Alternatives Considered

Keeping the current timers and adding a separate visibility effect to each
consumer duplicates listener lifecycle and timer-adjacency logic. Recreating
timers on every callback change makes the polling cadence depend on render
frequency. Resetting the interval whenever the page becomes visible adds more
timer state than the timestamp guard and delays the normal cadence.

Doing nothing leaves stale state visible at exactly the point where the user
returns to inspect it.

## Changed-Path-to-Test Matrix

| Changed path                                           | Invariant                                                                                                                                                    | Test and command                                                                                                            |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| `sky/dashboard/src/components/jobs.jsx`                | Hidden pool ticks do no work; visibility restore refreshes once; an adjacent interval does not duplicate the refresh; unmount removes the listener and timer | `sky/dashboard/src/components/jobs.test.jsx`; `npm --prefix sky/dashboard test -- --runInBand src/components/jobs.test.jsx` |
| `sky/dashboard/src/hooks/useVisibleRefreshInterval.js` | A backward wall-clock correction after visibility restore suppresses only the adjacent tick, and later polling resumes                                       | `sky/dashboard/src/components/jobs.test.jsx`; same command                                                                  |
| `sky/dashboard/src/components/jobs.jsx`                | Hidden job ticks do no work; visibility restore refreshes once through the existing automatic owner                                                          | `sky/dashboard/src/components/jobs.test.jsx`; same command                                                                  |
| `sky/dashboard/src/components/jobs.jsx`                | Initial/manual ownership, stale pool and job response fencing, dynamic batch intervals, cache reuse, and automatic refresh serialization remain intact       | Existing lifecycle cases in `sky/dashboard/src/components/jobs.test.jsx`; same command                                      |
| `sky/dashboard/src/components/jobs.jsx`                | A page-reset fetch supersedes an older request and publishes page 1; the older completion cannot overwrite it                                               | `sky/dashboard/src/components/jobs.test.jsx`; same command                                                                  |

## Performance Evidence

The focused tests assert exact pool and job read counts. Hidden intervals remain
at zero reads. Each visible transition adds at most one refresh per surface,
and the adjacent-timer test proves that the visibility refresh does not add a
second periodic read. The page-reset boundary performs exactly one necessary
replacement read. The steady visible path keeps the existing interval and
request complexity, adding only constant-time owner checks per trigger.

## Rollout and Verification

This is a dashboard-only lifecycle change with no API, schema, or migration.
Run the focused jobs suite, then the exact CI-listed dashboard Jest suite,
dashboard lint, format check, production build, and `git diff --check`.

`.github/workflows/dashboard.yml` explicitly runs
`src/components/jobs.test.jsx` for pull requests targeting `improvements`, then
runs the dashboard lint, formatting, and build gates.
