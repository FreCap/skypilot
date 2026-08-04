# Own visible cluster-list refresh lifecycle

## Problem

`useClusterData` owns a private interval that checks document visibility before
fetching. Hidden ticks avoid connector calls, but returning to a visible tab
does not trigger a refresh. Cluster lifecycle state can therefore remain stale
for an entire refresh interval after the user returns. The private effect also
duplicates timer, visibility, callback-freshness, boundary suppression, and
cleanup ownership already centralized in `useVisibleRefreshInterval`.

## Behavior contract

- The initial mount starts one automatic cluster-list read.
- Hidden interval ticks start no cluster-list reads.
- Returning to a visible document starts exactly one fresh read and suppresses
  the adjacent interval boundary for one full cadence.
- One full visible interval after restoration starts one automatic read.
- A visibility refresh supersedes an automatic request that began before the
  tab was hidden, and the older completion cannot publish stale data.
- A visibility refresh reuses a manual refresh already in flight.
- Context changes and unmount revoke timer, listener, request, and publication
  ownership.

## Design

Keep `fetchData` as the automatic owner used by mount and interval callbacks.
Keep `refreshData` as the freshness owner: it supersedes automatic work but
reuses an existing manual request. Delegate recurring timer, visibility,
callback-freshness, boundary suppression, and cleanup behavior to
`useVisibleRefreshInterval`. Route its `interval` source to `fetchData` and its
`visibilitychange` source to `refreshData`.

This preserves the existing O(1) request-owner checks. Visibility restoration
can temporarily overlap one stale automatic read with one fresh read, which is
the same bounded behavior as an explicit manual refresh and is required to
avoid publishing a pre-hide snapshot as current. Steady-state visible cadence
and hidden backend traffic do not increase.

## Alternatives considered

- Add a visibility listener beside the private interval. Rejected because it
  duplicates lifecycle behavior already owned by `useVisibleRefreshInterval`.
- Route every callback through `refreshData`. Rejected because interval ticks
  would supersede slow automatic requests and increase backend work.
- Route every callback through `fetchData`. Rejected because restoration could
  reuse a request started before hiding instead of acquiring a fresh snapshot.
- Pause while hidden without refreshing on restoration. Rejected because it
  preserves the stale-for-one-interval liveness gap.

## Changed-path-to-test matrix

| Changed path | Invariants | Test and command | CI coverage |
| --- | --- | --- | --- |
| `sky/dashboard/src/data/connectors/clusters.jsx` | one initial read; zero hidden reads; one restoration read; zero adjacent-boundary duplicates; one read after a full visible interval; unmount cleanup | `sky/dashboard/src/data/connectors/clusters.test.jsx`; `npm --prefix sky/dashboard test -- --runInBand src/data/connectors/clusters.test.jsx` | `.github/workflows/dashboard.yml`, job `dashboard`, explicitly runs this test file |
| `sky/dashboard/src/data/connectors/clusters.jsx` | restoration supersedes and fences a pre-hide automatic request; restoration reuses an in-flight manual owner | same focused file and command | same dashboard job |
| `sky/dashboard/src/data/connectors/clusters.jsx` | no material performance regression: zero hidden connector calls, at most one restoration call, no adjacent duplicate, one call per steady visible cadence | exact `dashboardCache.get` call-count assertions in the same focused file | same dashboard job |
| `docs/designs/dashboard-cluster-list-visible-refresh.md` | behavior, alternatives, rollout, matrix, and evidence remain synchronized | design review and `git diff --check origin/improvements...HEAD` | reviewer-visible PR diff and repository format checks |

## Rollout and verification

First add the restoration call-count regression test against unchanged
production code and record the parent-red result. Add adjacent owner and cleanup
boundaries, then replace the private effect. Run the focused suite, the exact
dashboard CI Jest inventory, ESLint, Prettier check, Next.js production build,
and `git diff --check`. No migration or feature flag is needed; reverting the
single production-path change restores the former interval owner.
