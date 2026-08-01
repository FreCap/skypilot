# Dashboard Volume List Visible Refresh

_Created: 2026-08-01_

## Problem

The volume list owns a private `setInterval` loop that skips reads while the
document is hidden but does not refresh when visibility returns. If a hidden
timer boundary fires just before the tab becomes visible, the list can remain
stale for almost the full 30-second interval. The private loop also duplicates
the visibility lifecycle already owned by `useVisibleRefreshInterval`.

## Goal

Keep one refresh owner for the volume list: fetch immediately after preload,
perform no hidden reads, refresh exactly once when visibility returns, suppress
the adjacent interval boundary, then resume the normal visible cadence. Preserve
latest-request ownership and unmount cleanup without adding requests to the
steady-state visible cadence.

## Background

`VolumesTable` already fences async results with `requestVersionRef`. The shared
`useVisibleRefreshInterval` hook owns document visibility events, interval
cleanup, callback freshness, and suppression of a timer boundary that occurs
within one interval after a visibility refresh. Other dashboard lists use that
hook for the same lifecycle.

## Solution

Keep the initial volume read and request-version cleanup in the component
effect. Replace the component's private timer and visibility check with
`useVisibleRefreshInterval`, enabled only after preload completes. The hook will
call the existing fenced `fetchData`, so concurrent success, failure, and
unmount behavior remain under the same request owner.

### Changed-path-to-test matrix

| Changed production path                    | Invariant                                                                                                                                                                                       | Test path and command                                                                                                             |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `sky/dashboard/src/components/volumes.jsx` | Initial preload completion performs one read; visible intervals retain one read per interval                                                                                                    | `sky/dashboard/src/components/volumes.test.jsx`; `npm --prefix sky/dashboard test -- --runInBand src/components/volumes.test.jsx` |
| `sky/dashboard/src/components/volumes.jsx` | Hidden intervals perform zero reads; visibility restoration performs exactly one immediate read; an adjacent timer performs zero extra reads; one full visible interval later performs one read | `sky/dashboard/src/components/volumes.test.jsx`; same command                                                                     |
| `sky/dashboard/src/components/volumes.jsx` | Stale success/failure cannot publish; unmount revokes request ownership                                                                                                                         | Existing request-ownership cases in `sky/dashboard/src/components/volumes.test.jsx`; same command                                 |
| `sky/dashboard/src/components/volumes.jsx` | No material performance regression: hidden call count remains zero and visible steady-state call count remains one per interval                                                                 | Exact mock call-count assertions in `sky/dashboard/src/components/volumes.test.jsx`; same command                                 |

The pull request path is covered by `Dashboard Testing and Formatting / dashboard`
in `.github/workflows/dashboard.yml`, which explicitly executes the changed test
file, lint, formatting, and the production build for pull requests targeting
`improvements`.

## Alternatives considered

Keeping the private timer and adding a separate visibility listener would
duplicate suppression and cleanup state. Changing the shared hook is unnecessary
because its existing contract already matches the volume-list lifecycle.

## Rollout and validation

This is a client-only lifecycle change with no persisted state or migration.
Validate the focused file first, then the complete dashboard test suite, lint,
format check, production build, and `git diff --check`. The focused call-count
test is also the performance proof.
