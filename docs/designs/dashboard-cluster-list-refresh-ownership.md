# Dashboard Cluster List Refresh Ownership

_Created: 2026-07-22_

## Problem

`useClusterData` starts a new hook-level request for every manual refresh and
visible auto-refresh tick. When the current cache read is slower than the
refresh interval, each tick supersedes the previous request version and creates
another async continuation waiting on the same cache-owned backend request.
The cache prevents duplicate network calls, but the hook still accumulates
pending work in proportion to the delay, repeatedly writes loading state, and
only lets the newest continuation publish the shared result.

The request-version fence must remain authoritative across page, limit, filter,
history-mode, plugin-mode, and unmount changes. Coalescing requests across any
of those boundaries would publish data for the wrong request context.

## Goal

For one stable cluster-list request context, at most one automatic refresh or
one explicit manual refresh is current. Automatic ticks reuse any current
owner. The first manual caller may supersede an automatic load to request a
newer snapshot, while duplicate manual callers reuse that manual promise.
Success and failure release ownership so a later refresh can make progress. A
context change or unmount revokes the old owner, and an old completion cannot
publish state or clear a newer owner's promise.

The change must preserve the existing single foreground cache read and optional
single next-page prefetch. Coordination must stay O(1), add no timer or backend
call, and bound pending hook continuations to one per request context.

## Background

The shared `DashboardCache` already deduplicates concurrent calls by connector
and arguments. `useClusterData` separately owns React publication through a
monotonic request version. The missing layer is ownership of the hook-level
refresh lifecycle itself.

## Solution

Split refresh acquisition from request execution. The existing server-side
fetch callback depends on every request input, including the client-side
subset, so its identity is the stable context token. The automatic `fetchData`
callback checks one ref for an owner with that exact token, returning its
promise when present. The public manual callback reuses an
exact-context manual owner but deliberately supersedes an automatic owner.
Acquisition increments the request version, starts the existing request body,
stores the promise and its intent, and clears only that exact owner in
`finally`.

Effect cleanup increments the request version and revokes only the owner for
the context being retired. Therefore a dependency change can immediately start
a distinct request while the old one settles harmlessly. The existing
publication checks continue to fence stale success, failure, prefetch, and
loading updates.

## Alternatives considered

Relying only on `DashboardCache` avoids duplicate backend calls but still
creates unbounded hook continuations and state churn during a slow request.
Skipping interval ticks based on `loading` would couple scheduling to rendered
state, would not coalesce manual callers, and can observe stale state inside the
timer closure. Cancelling the underlying request is not supported uniformly by
the plugin and client connectors and is unnecessary for this bounded fix.

## Changed-path-to-test matrix

| Changed path                                               | Invariant                                                                                                         | Test file                                             | Command                                                                                |
| ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `sky/dashboard/src/data/connectors/clusters.jsx`           | Overdue visible ticks and manual refreshes for one context reuse one hook promise and one foreground cache read   | `sky/dashboard/src/data/connectors/clusters.test.jsx` | `npm --prefix sky/dashboard test -- --runInBand src/data/connectors/clusters.test.jsx` |
| `sky/dashboard/src/data/connectors/clusters.jsx`           | Success and failure release the exact owner, so the next tick reacquires once                                     | `sky/dashboard/src/data/connectors/clusters.test.jsx` | same focused command                                                                   |
| `sky/dashboard/src/data/connectors/clusters.jsx`           | Page or option changes supersede rather than coalesce, and stale completion cannot publish or clear the new owner | `sky/dashboard/src/data/connectors/clusters.test.jsx` | same focused command                                                                   |
| `sky/dashboard/src/data/connectors/clusters.jsx`           | Unmount revokes publication and no later timer or prefetch is launched                                            | `sky/dashboard/src/data/connectors/clusters.test.jsx` | same focused command                                                                   |
| `sky/dashboard/src/data/connectors/clusters.jsx`           | Performance remains one foreground read plus at most one prefetch, with O(1) ownership state and no extra timers  | `sky/dashboard/src/data/connectors/clusters.test.jsx` | focused call-count tests plus exact dashboard workflow inventory                       |
| `docs/designs/dashboard-cluster-list-refresh-ownership.md` | Design and implementation remain synchronized and all changed paths are clean                                     | review and diff validation                            | `git diff --check origin/improvements...HEAD`                                          |

## CI coverage

`.github/workflows/dashboard.yml` has no pull-request path filter for these
paths. Its `Dashboard Testing and Formatting / dashboard` job explicitly runs
`src/data/connectors/clusters.test.jsx`, then ESLint, Prettier check, and the
Next.js production build. No workflow change is needed.

## Rollout and rollback

This is client-local coordination with no schema, API, or persistence change.
The normal dashboard build rollout is sufficient. Reverting the production and
test changes restores the prior superseding-per-tick behavior.
