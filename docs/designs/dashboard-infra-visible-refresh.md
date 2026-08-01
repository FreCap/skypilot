# Dashboard Infrastructure Visible Refresh

_Created: 2026-08-01_

## Problem

The infrastructure page owns a private interval that skips refreshes while the
document is hidden. It does not refresh when visibility returns, so cluster,
job, cloud, Kubernetes, SSH, and Slurm capacity can remain stale for almost the
full refresh interval. The private timer also duplicates visibility and cleanup
state already owned by `useVisibleRefreshInterval`, while a mutable callback ref
indirectly routes both timer and manual refreshes to `startRefresh`.

## Goal

Keep `startRefresh` as the single request owner. After the initial preload and
refresh complete, perform no hidden reads, refresh exactly once when visibility
returns, suppress the adjacent interval boundary, and resume one refresh per
normal visible interval. Preserve background coalescing, manual-refresh
supersession, unmount revocation, and the current steady-state request budget.

## Background

`startRefresh` already coalesces overlapping background polls onto its active
promise and assigns a newer generation to manual refreshes. `fetchData` and the
progressive context fanout check that generation before publishing. The shared
`useVisibleRefreshInterval` hook owns document visibility events, interval
cleanup, callback freshness, and suppression of a timer boundary immediately
after a visibility refresh.

## Solution

Enable `useVisibleRefreshInterval` only after `isInitialLoad` becomes false.
Route its callback directly to `startRefresh` with loading indicators disabled.
Route manual refreshes directly to `startRefresh` as well, then delete the
private timer, callback-ref assignment effect, and mutable callback ref.

### Changed-path-to-test matrix

| Changed production path | Invariant | Test path and command |
| --- | --- | --- |
| `sky/dashboard/src/components/infra.jsx` | Initial preload performs one refresh; hidden intervals perform zero reads; visibility restoration performs exactly one immediate refresh; the adjacent timer performs zero extra reads; one full visible interval later performs one refresh | `sky/dashboard/src/components/infra-page.test.jsx`; `npm --prefix sky/dashboard test -- --runInBand src/components/infra-page.test.jsx` |
| `sky/dashboard/src/components/infra.jsx` | Unmount removes visibility and interval ownership, so neither source can start another fanout | `sky/dashboard/src/components/infra-page.test.jsx`; same command |
| `sky/dashboard/src/components/infra.jsx` | Background polls still coalesce, a manual refresh still supersedes stale background success/failure, and unmount still revokes an active progressive fanout | Existing lifecycle cases in `sky/dashboard/src/components/infra-page.test.jsx` and `sky/dashboard/src/components/infra-refresh-lifecycle.test.jsx`; `npm --prefix sky/dashboard test -- --runInBand src/components/infra-page.test.jsx src/components/infra-refresh-lifecycle.test.jsx` |
| `sky/dashboard/src/components/infra.jsx` | No material performance regression: hidden call count stays zero, steady visible cadence stays one refresh per interval, and restoration does not double-fetch at an adjacent timer boundary | Exact `getWorkspaceContexts` cache-call counts in `sky/dashboard/src/components/infra-page.test.jsx`; focused command above |

The pull request paths are covered by `Dashboard Testing and Formatting /
dashboard` in `.github/workflows/dashboard.yml`, which explicitly runs both
infra lifecycle test files, lint, formatting, and the production build for pull
requests targeting `improvements`.

## Alternatives considered

Adding a second visibility listener beside the private interval would preserve
duplicated timer and cleanup ownership. Keeping the mutable callback ref only
for manual refreshes adds indirection without protecting a lifecycle boundary,
because `handleRefresh` can depend directly on the memoized `startRefresh`.

## Rollout and validation

This is a dashboard-only lifecycle change with no persisted state or migration.
Validate parent-red visibility behavior, the focused infra lifecycle files, the
dashboard CI test selection, lint, format check, production build, and
`git diff --check`. Exact fanout call counts provide the performance evidence.
