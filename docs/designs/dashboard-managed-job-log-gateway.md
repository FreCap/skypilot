# Dashboard Managed Job Log Gateway

_Created: 2026-07-20_

## Problem

`sky/dashboard/src/data/connectors/jobs.jsx` owns four independently changing
areas: managed-job and pool query projection, React cache-backed single-job
state, controller action dispatch, and browser log transport. The log transport
is a cohesive leaf, but it currently brings streaming readers, activity timers,
long-poll retry policy, API-version headers, blob downloads, DOM anchors, and
download analytics into the broader jobs data connector.

The file is 1,096 lines on `origin/improvements` at
`0a446a2da8d3ee1b43bc8ad8913663dfd7c49c23`.
Its size is only a prioritization signal. The reason to change it is that log
transport has different callers, dependencies, state, failure modes, and change
cadence from job-list projection and React cache state.

## Goals

Move the complete managed-job log transport gateway into one plain module while
preserving the historical `@/data/connectors/jobs` entrypoint and all behavior.
The extraction must add no wrapper call, request, render, timer, retry, copy, or
browser allocation.

## Responsibility Map

### Managed-job query and projection

The jobs page, infra, users, workspaces, pool status, and cache manager call
`getManagedJobs` or its client-pagination variant. This responsibility depends
on the queued jobs API, response decoding, plugin data enhancements, and cache
keys. It owns no durable state, but its result shape and request count are
performance-sensitive. Its failures are request, compatibility, projection, and
cache errors. It changes with jobs API fields, filtering, pagination, and plugin
projection.

### Pool projection and active-job accounting

Jobs pages call `getPoolStatus`. It depends on the pool endpoint, the managed-job
query, terminal-status policy, and dashboard cache. It owns the derived active
job count per pool. Its failures are controller availability, partial pool/job
reads, and incorrect accounting. It changes with scheduling and pool semantics.

### Single-job React cache lifecycle

Job detail pages call `useSingleManagedJob`. It depends on React hooks,
`dashboardCache`, and `getManagedJobs`. It owns loading state and the refresh
generation fence. Its failures are stale publication, unnecessary invalidation,
and job-navigation cache misses. It changes with detail-page lifecycle and cache
behavior.

### Controller action dispatch

The jobs page calls `handleJobAction`. It depends on queued action requests,
error-type compatibility constants, toasts, and action-specific response
messages. It owns no persistent state. Its failures are dispatch, request-ID,
server-error decoding, and user-feedback errors. It changes with job actions and
controller capabilities.

### Managed-job log streaming and download transport

Job detail pages and the jobs table call `streamManagedJobLogs` and
`downloadManagedJobLogs`. This responsibility depends on the immediate streaming
client, `ReadableStream`, `TextDecoder`, activity timers, direct `fetch`,
API-version headers, queued-request long polling, blobs, object URLs, DOM anchors,
toasts, and download analytics. It owns only per-call timeout and reader state.
Its failures are aborts, inactivity, edge timeouts, missing request IDs, empty
log mappings, binary download failures, and browser save failures. Its hot paths
are chunk decoding, one timer schedule per activity interval, fixed retry counts,
and one blob allocation per download. It changes with log transport, proxy
behavior, browser download UX, and API compatibility.

## Solution

Create `sky/dashboard/src/data/connectors/managed-job-logs.js` and move the full
log transport responsibility into it:

- `DEFAULT_TAIL_LINES`
- `streamManagedJobLogs`
- `downloadLogsWithRetry`
- `downloadManagedJobLogs`

Keep `sky/dashboard/src/data/connectors/jobs.jsx` as the stable facade by directly
re-exporting `streamManagedJobLogs` and `downloadManagedJobLogs`. Existing callers
continue importing from the historical path. A direct re-export avoids a wrapper
frame and makes the new module the sole implementation owner.

The move is structural only. Function bodies, defaults, request order, headers,
timeouts, retry statuses, backoff, toast text, filenames, DOM lifecycle, and
analytics remain unchanged.

## Alternatives Considered

Do nothing has the lowest immediate cost, but leaves two browser transport
protocols and their failure policy embedded among unrelated query, projection,
cache, and React code. The log gateway already has multiple independent callers
and a focused transport contract, so the carrying cost of one implementation
module is lower than continued mixed ownership.

Extract only download handling would leave streaming and its timer/reader policy
behind even though both operations are the same caller-facing log transport
boundary. Extracting both produces one coherent leaf without a package hierarchy.

A class, strategy, adapter interface, registry, or dependency-injection layer is
not justified because there is one implementation. A plain module is sufficient.

Changing callers to import the new path would weaken the facade and expand the
compatibility surface. Direct re-exports preserve existing imports.

## Implementation and Verification

Run the existing characterization tests against the unsplit facade. They pin:

- streaming request body, chunk order, cancellation, and inactivity behavior;
- download dispatch headers and environment projection;
- retry against the same request ID for transient edge statuses;
- download request, filename, anchor lifecycle, analytics, and warning/error
  behavior.

Run those tests before moving code, then run them unchanged after extraction.
Compare the moved function ASTs before and after while ignoring export and source
location metadata. Run the full jobs connector, jobs component, job log action,
and cache-manager suites, plus dashboard lint, Prettier, production build, and
`git diff --check`.

The dashboard workflow explicitly collects both characterization suites and has
no pull-request path filter. Performance evidence is structural: direct
re-exports add no wrapper, and byte-identical bodies preserve request, timer,
reader, retry, copy, and allocation counts. Clean exact-base and extracted
production builds each contain 2,627,218 raw JavaScript bytes. Per-file gzip size
changes from 814,145 to 813,860 bytes, a 285-byte decrease.

## Rollout

This is an internal module extraction with no data migration or feature flag.
Rollback is a revert of the extraction commit. Public imports and browser-visible
behavior remain stable throughout.
