# Gate Cluster-Job Log Streams on the Current Snapshot

_Created: 2026-07-30_

## Problem

The cluster-job detail page derives `isPending` by looking up the route job in
the current cluster-job snapshot. When the snapshot is loaded but does not
contain that job, the lookup produces `undefined`. The page then treats
`!isPending` as true and enables `useLogStreamer`, issuing a log request for a
job that the route-owned snapshot did not confirm.

This can happen for a deleted or mistyped job ID and during lifecycle boundaries
where a job disappears between refreshes. The unnecessary request can occupy a
streaming connection and replace a stable missing state with a transport error.

## Behavior Contract

- Loading, pending, missing, superseded, and unmounted jobs do not start a log
  stream.
- A job confirmed by the current cluster snapshot starts a stream only when its
  status is not pending.
- The stream arguments continue to use the route cluster/job and the confirmed
  cluster workspace.
- Route changes rely on the existing `useClusterDetails` ownership fence. This
  change does not add another request owner or data fetch.
- Disabled `useLogStreamer` instances perform zero stream-function calls.

## Solution

Derive the selected job once from `clusterJobData` and `job`. Reuse that
snapshot for both pending-state calculation and the rendered job details.
Enable log streaming only when the selected job exists and is not pending.

The implementation remains render-derived. It adds no state, effect, timer,
request, or cleanup path.

## Alternatives Considered

Changing only the missing-job branch of `isPending` would stop the request, but
would keep two separate job lookups that could drift. Adding another route
ownership ref would duplicate the ownership fence already provided by
`useClusterDetails`. Returning a dedicated not-found page is useful UX work but
is outside this bounded liveness fix.

## Changed-Path-to-Test Matrix

| Changed path | Invariant | Test and command |
| --- | --- | --- |
| `sky/dashboard/src/pages/clusters/[cluster]/[job].js` | Missing and pending snapshots disable streaming; a confirmed runnable job enables one route-scoped stream | `sky/dashboard/src/components/job-detail-logs.test.jsx`; `npm --prefix sky/dashboard test -- --runInBand src/components/job-detail-logs.test.jsx` |
| `sky/dashboard/src/pages/clusters/[cluster]/[job].js` | Route transitions cannot enable a stream from an absent or mismatched snapshot | `sky/dashboard/src/components/job-detail-logs.test.jsx`; same command |
| Existing `useLogStreamer` disabled path | A disabled owner invokes the stream function zero times and creates no cleanup work | `sky/dashboard/src/hooks/useLogStreamer.test.js`; `npm --prefix sky/dashboard test -- --runInBand src/hooks/useLogStreamer.test.js` |

## Performance Evidence

The page test asserts the exact enabled-stream count and arguments. The hook
test asserts zero stream-function calls while disabled. The production change
performs one `Array.find()` per relevant render instead of two and adds no
network or cache read.

## Rollout and Verification

This is a dashboard-only change with no schema or API migration. Run the two
focused test files first, then the complete dashboard Jest suite, lint,
format-check, and build. The dashboard GitHub Actions job explicitly includes
both focused test files and runs on pull requests targeting `improvements`.
