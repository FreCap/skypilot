# Dashboard cluster detail route ownership

## Problem

`useClusterDetails` clears its prior cluster and job values in a route-change
effect. Effects run after render, so its first result for cluster B still
contains cluster A. The page can commit that stale result under B's URL,
including A's actions, jobs, persisted links, live-scanned links, and log
scanners.

## Behavior contract

One requested route owns one detail-data lifecycle:

- The first render after navigation exposes neither prior cluster data nor
  prior jobs.
- Both detail scopes report loading until the new route owns state.
- Removing old data synchronously unmounts its actions and log scanners, so
  their existing cleanup runs before new details appear.
- Same-route refreshes retain visible data and current coalescing.
- Navigation still performs one cluster read followed by one workspace-scoped
  jobs read.

## Design

The hook already stores the state owner's route in `activeClusterRef`. Return
cluster and job data only while that ref equals the requested route. When it
does not match, return null data and loading=true. The existing effect remains
the only writer that advances ownership, clears old values, and starts the
new request chain.

The gate adds no state, effect, timer, request, scan, or render loop. It only
prevents an old owner from reading state during the render before the existing
effect runs.

## Alternatives

Keying `ActiveTab` treats a consumer rather than the source and still hands
that consumer stale data for one render. Owner-tagged state objects are sound
but duplicate the owner on every write. The existing owner ref is already the
request-chain serialization point.

## Rollout and tests

No schema, API, or configuration changes are required. The focused connector
suite records render snapshots to prove the first new-route render is empty,
then exercises stale completion, unmount, failure, same-route refresh, and
call-count boundaries. The dashboard workflow already runs that suite,
followed by lint, formatting, and production build.
