# Dashboard Volume Refresh Ownership

_Created: 2026-07-21_

## Problem

The volumes page can start duplicate forced refresh sweeps while the first manual refresh is still preloading. The refresh button is not yet covered by the table loading state during this window. The handler also invalidates `getVolumes` before the forced page preloader invalidates the same cache key again.

The concrete invariant is that one active manual refresh owns one forced volumes preload. Overlapping callers must share that owner, while a caller after settlement must be able to start a new refresh. The performance bottleneck is duplicate invalidation and backend reads from overlapping refresh triggers.

## Behavior contract

- A manual refresh starts one forced volumes preload and preserves one subsequent table refresh.
- Overlapping manual callers share the current preload without duplicating invalidation or preloader reads.
- A caller after success or handled preload failure can start a new refresh.
- A delete or plugin mutation completion supersedes a refresh that began before the mutation, so it cannot preserve stale data.
- A manual refresh can supersede the initial non-forced preload.
- Unmount revokes request publication and manual refresh ownership.
- The forced preloader solely owns the unfiltered `getVolumes` invalidation and fetch.
- Every caller retains the existing refresh analytics event.

## Solution

Keep the existing request-version fence for which preload may publish UI state. Add a separate promise ref for manual refresh ownership. The first manual caller stores the forced-preload promise, and overlapping manual callers return it. Delete and plugin mutation completions start a superseding forced preload because an older request may have observed pre-mutation state. Exact-promise cleanup releases ownership after settlement without letting an older request clear a newer owner. Unmount clears the owner and advances the request version.

Do not coalesce the initial preload with a forced refresh because the forced request must supersede and invalidate the initial request. Do not add React state solely for ownership because that would add renders and would not coordinate non-button callers any better than the shared callback.

## Changed-path-to-test matrix

| Changed production path | Invariant | Test path | Command |
| --- | --- | --- | --- |
| `sky/dashboard/src/components/volumes.jsx`, `handleRefresh` | Two overlapping manual callers make one forced preload while preserving one subsequent table read; analytics still records both callers | `sky/dashboard/src/components/volumes.test.jsx`, overlapping manual refresh regression | `npm --prefix sky/dashboard test -- --runInBand src/components/volumes.test.jsx` |
| `sky/dashboard/src/components/volumes.jsx`, refresh ownership cleanup | Success and handled preload failure release ownership so a later caller makes a fresh preload and read | `sky/dashboard/src/components/volumes.test.jsx`, success and failure reacquisition boundaries | `npm --prefix sky/dashboard test -- --runInBand src/components/volumes.test.jsx` |
| `sky/dashboard/src/components/volumes.jsx`, delete and plugin refresh callbacks | A post-mutation refresh supersedes a pending manual refresh; the older completion cannot publish or clear the newer owner | `sky/dashboard/src/components/volumes.test.jsx`, plugin-mutation supersession boundary | `npm --prefix sky/dashboard test -- --runInBand src/components/volumes.test.jsx` |
| `sky/dashboard/src/components/volumes.jsx`, request and unmount fences | Forced refresh supersedes initial preload; stale success/failure and unmount cannot publish or retain ownership | Existing and strengthened request-ownership cases in `sky/dashboard/src/components/volumes.test.jsx` | `npm --prefix sky/dashboard test -- --runInBand src/components/volumes.test.jsx` |
| `sky/dashboard/src/components/volumes.jsx`, invalidation delegation | One forced refresh makes exactly one `getVolumes` invalidation and one fetch, both owned by `CachePreloader` | Real-preloader call-count assertion in `sky/dashboard/src/components/volumes.test.jsx` | `npm --prefix sky/dashboard test -- --runInBand src/components/volumes.test.jsx` |

The full dashboard workflow has no pull-request path filter and explicitly runs `src/components/volumes.test.jsx`, ESLint, Prettier, and the Next.js production build in `Dashboard Testing and Formatting / dashboard`.

## Performance proof

The coordination path is O(1), using one promise ref and exact-promise comparison. The regression test is also the performance assertion: two overlapping refresh triggers must produce one forced preload, one preloader-owned invalidation, and one preloader fetch instead of two. The existing single subsequent table read is preserved. No timer, scan, render state, or backend call is added.

## Alternatives considered

Disabling only the visible button would add render state without coordinating programmatic manual callers. Coalescing every callback would be incorrect because a refresh that predates a successful mutation may contain stale data. Coalescing inside the global preloader would expand the blast radius across unrelated pages and would mix forced and background ownership semantics. Leaving the duplicate direct invalidation in place has no correctness benefit because the forced preloader invalidates the identical cache key immediately before fetching it.
