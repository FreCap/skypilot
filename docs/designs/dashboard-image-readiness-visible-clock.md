# Dashboard image-readiness visible clock

_Created: 2026-08-01_

## Problem

`ImageReadiness` owns a five-second interval that updates its client clock even
when the dashboard is hidden. Every tick re-renders the full readiness
projection and repeats profile, queue, worker, quarantine, and attestation
derivations whose cost grows with the bounded response size. Hidden pages do
not need those renders. Browser timer throttling can also leave the snapshot
and worker-heartbeat safety state stale until a later timer tick after the page
becomes visible.

The current focused test suite passes 14 tests, but it neither constrains
hidden render work nor exercises visibility restoration. The test file is also
absent from the dashboard CI workflow's explicit Jest inventory.

## Goals

The client clock must advance every five seconds while visible, perform no
clock-driven renders while hidden, update immediately when visibility returns,
avoid a duplicate update at an adjacent interval boundary, and stop all clock
updates after unmount. The implementation should reuse the existing shared
visibility lifecycle rather than own another timer and event listener.

## Solution

Replace the component's private `useEffect` interval with
`useVisibleRefreshInterval`. Its callback updates the existing `now` state from
`Date.now()`. The hook already owns callback freshness, hidden-interval
suppression, immediate visibility restoration, adjacent-boundary suppression,
and cleanup.

Add a focused fake-timer test that wraps the component in a React `Profiler`.
It will prove zero additional commits across twenty hidden seconds, one commit
on visibility restoration, no adjacent duplicate, one commit on the next full
interval, and no commits after unmount. The same test will assert the visible
heartbeat classification changes immediately on restore. Add the focused test
file to the dashboard workflow so pull requests changing this path execute the
regression in CI.

## Changed-path-to-test matrix

| Changed path or invariant                                                                                                                                | Test file                                               | Local command                                                                                                                                    | CI job                                         |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------- |
| `sky/dashboard/src/components/image-readiness.jsx`: visible-only clock ownership, visibility restoration, adjacent-boundary suppression, unmount cleanup | `sky/dashboard/src/components/image-readiness.test.jsx` | `npm --prefix sky/dashboard test -- --runInBand src/components/image-readiness.test.jsx`                                                         | `Dashboard Testing and Formatting / dashboard` |
| Hidden-render performance: zero full readiness renders during twenty hidden seconds; visible cadence remains one render per interval                     | `sky/dashboard/src/components/image-readiness.test.jsx` | Same focused command                                                                                                                             | `Dashboard Testing and Formatting / dashboard` |
| `.github/workflows/dashboard.yml`: the focused lifecycle regression is selected for dashboard changes                                                    | Workflow source plus full dashboard command             | Run the exact multiline Jest inventory from `.github/workflows/dashboard.yml`                                                                    | `Dashboard Testing and Formatting / dashboard` |
| Dashboard lint, formatting, type/build integration                                                                                                       | Existing dashboard sources                              | `npm --prefix sky/dashboard run lint`; `npm --prefix sky/dashboard run format:check`; `npm --prefix sky/dashboard run build`; `git diff --check` | `Dashboard Testing and Formatting / dashboard` |

## Alternatives considered

Keeping the private interval and adding a visibility check would remove hidden
state updates but retain duplicate timer lifecycle code and would not refresh
immediately on visibility restoration. Adding a private visibility listener
would reproduce behavior and cleanup already centralized in
`useVisibleRefreshInterval`. Moving all derived readiness state into memoized
selectors would be broader and would still leave an unnecessary hidden timer.

## Rollout

This is dashboard-only and requires no data migration or compatibility path.
The focused call-count/render-count contract and the production dashboard build
are the rollback gate. Reverting the single behavior commit restores the old
timer if unexpected UI behavior appears.
