# Dashboard managed-job detail route ownership

## Problem

`useSingleManagedJob()` keeps the last completed job snapshot while a new
managed-job route starts loading. Request-version fencing prevents an old
request from publishing after navigation, but it does not protect the first
render caused by the route change itself. That render can expose the previous
job's tasks and a settled loading flag under the new route.

The stale snapshot feeds job details, actions, pool links, and log controls.
Consumers should not need to repeat route-ownership checks around every use of
the hook.

The job and task detail pages also keep an `isInitialLoad` flag that becomes
false after the first request settles and never resets for later route changes.
After the connector correctly hides the old snapshot on navigation, those
pages can still interpret the new route's empty, loading snapshot as a settled
not-found result. The user briefly sees `Job not found` or `Task not found`
while the new route is still fetching.

## Behavior contract

The hook owns a snapshot only while the snapshot's route identifier matches its
current `jobId`.

- The first render after navigation from job A to job B returns `jobData: null`
  and `loading: true`.
- The route effect clears job A's snapshot before job B becomes the active
  owner.
- A late success or failure from job A cannot publish data or clear job B's
  loading state.
- Returning from job B to job A starts a new ownership epoch and cannot reuse
  an earlier in-flight refresh for job A.
- Same-route refreshes remain coalesced and explicit refreshes remain the only
  cache-invalidating path.
- A page with no matching job or task snapshot renders loading while the
  current route read is in progress, then renders not-found only after an empty
  result settles.
- A background refresh with a matching snapshot keeps the existing detail
  visible instead of replacing it with a full-page loading state.

## Solution

Keep one ref containing the route identifier that owns the hook state. During
render, compare that identifier with the current `jobId` and hide both stale
data and the settled loading flag when they differ. In the existing route
effect, advance ownership and clear the old snapshot before starting the new
read.

This keeps ownership in the connector, next to the request-version and refresh
ownership rules. It adds no state variable, effect, timer, request, cache
invalidation, or scan. The render path adds one constant-time ref comparison.
The snapshot reset shares the existing route effect and is batched with its
loading update.

The job and task detail pages derive their full-page loading state directly
from the connector lifecycle and their matching snapshot. Loading is shown
only when the connector is loading and the page cannot select its current job
or task. This removes both page-local `isInitialLoad` state variables and their
effects. It also preserves visible detail during background refreshes without
adding a refresh call or another lifecycle owner.

## Alternatives considered

Clearing state only in the effect is insufficient because effects run after
the route-change render. That first render would still publish job A under job
B.

Moving the ownership guard into both job-detail pages would duplicate a
connector invariant and leave future consumers exposed.

Storing `{jobId, data}` in a new state object would make ownership explicit,
but it expands the state transition surface without improving the render-time
gate or request fencing.

Resetting `isInitialLoad` from another route effect would add a second page
lifecycle owner and would still commit the reset after the first render of the
new route. Deriving loading from the connector snapshot covers that first
render and removes state instead.

## Rollout

The change is local to the dashboard connector and has no persisted-data, API,
or server compatibility impact. It can roll out with the normal dashboard
bundle. If reverted, the prior stale first-render behavior returns without a
migration.

## Test plan

`sky/dashboard/src/data/connectors/jobs.test.jsx` covers the route-change commit
boundary directly. The regression loads job A, rerenders with job B, records
the first job B render, and requires `jobData: null` plus `loading: true` before
job B's request resolves. It also requires exactly two connector reads across
the two routes.

The surrounding focused suite covers late completions, superseded failures,
leave-and-return ownership, same-route refresh coalescing, cache invalidation,
and loading cleanup. `.github/workflows/dashboard.yml` explicitly runs this
test file, lint, formatting checks, and a production dashboard build for pull
requests to `improvements`.

`sky/dashboard/src/components/job-detail-logs.test.jsx` covers both page
consumers. It loads route A, navigates the same mounted page to a loading route
B, and requires the page to avoid not-found until B settles empty. It also
requires passive navigation to call neither refresh callback. Adjacent cases
keep matching detail visible during background loading and verify that a
settled empty result and a settled invalid task index reach not-found.
