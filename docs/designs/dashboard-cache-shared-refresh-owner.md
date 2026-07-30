# Dashboard cache shared refresh ownership

## Problem

`DashboardCache` currently tracks foreground misses in `pendingRequests` and
stale-while-revalidate work in `backgroundJobs`. Each map deduplicates its own
kind of request, but the two maps do not share ownership.

If a background refresh remains in flight until the cached entry expires, the
next `get()` starts a second foreground connector call for the same function
and arguments. Slow or failing API reads can therefore amplify dashboard load
at the point when the backend is already degraded.

## Behavior contract

For one cache key and one invalidation generation:

1. At most one connector call may be in flight, regardless of whether it
   started as a foreground miss or a background refresh.
2. A fresh cache hit returns immediately and may start one background refresh.
3. A stale read that finds a background refresh in flight returns the stale
   value immediately without starting another connector call. It must not wait
   indefinitely for a slow or hung best-effort refresh.
4. If that background refresh succeeds, it updates the cache. If it fails or
   returns `__skipCache`, ownership is released and the next stale read retries,
   matching foreground stale-on-error behavior.
5. `invalidate()`, `invalidateFunction()`, and `clear()` revoke the current
   owner. A post-invalidation read starts a new connector call and neither the
   old result nor its cleanup may overwrite the new generation.
6. A background refresh that was started by a cache hit remains best effort:
   its rejection is handled even when no later caller waits for it.

The change adds no polling, retry, timer, network call, cache entry, or
ownership type. It reuses the existing `backgroundJobs` token as the single
in-flight owner.

## Changed-path-to-test matrix

| Changed production path or invariant                                                                                                 | Test file                                                                          | Command                                                                                                                                                            |
| ------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `DashboardCache.get`: stale foreground reads reuse a slow background owner, so one key has one connector call in flight              | `sky/dashboard/src/lib/cache.test.js`                                              | `npm --prefix sky/dashboard test -- --runInBand src/lib/cache.test.js`                                                                                             |
| `_refreshInBackground`: a slow owner does not block expired readers, and success updates the cache once                              | `sky/dashboard/src/lib/cache.test.js`                                              | Same focused command                                                                                                                                               |
| `_refreshInBackground`: failure and `__skipCache` release ownership so later reads retry                                             | `sky/dashboard/src/lib/cache.test.js`                                              | Same focused command                                                                                                                                               |
| `invalidate`: a revoked background owner cannot suppress or overwrite a post-invalidation request                                    | `sky/dashboard/src/lib/cache.test.js`                                              | Same focused command                                                                                                                                               |
| Background refresh rejections remain handled when no foreground reader waits                                                         | Existing background-refresh failure tests in `sky/dashboard/src/lib/cache.test.js` | Same focused command                                                                                                                                               |
| Shared cache integration across dashboard connectors and pages                                                                       | Dashboard workflow test list in `.github/workflows/dashboard.yml`                  | Exact workflow Jest command, then `npm --prefix sky/dashboard run lint`, `npm --prefix sky/dashboard run format:check`, and `npm --prefix sky/dashboard run build` |
| Performance: an expired read during a slow background refresh performs one refresh call, not one background plus one foreground call | Call-count assertion in `sky/dashboard/src/lib/cache.test.js`                      | Focused Jest command                                                                                                                                               |

## Alternatives

### Keep separate ownership

No code changes are needed, but slow refreshes can continue to duplicate
backend work. This does not meet the load-bounding invariant.

### Wait for the background result

This returns fresh data when the owner eventually succeeds, but a hung
best-effort refresh would make foreground dashboard reads hang too. Returning
the existing stale value keeps the stale-while-revalidate path live.

### Cancel background refreshes at TTL expiry

Most connector calls do not expose a cancellation signal, and cancellation
would discard useful work. It also adds lifecycle complexity without reducing
the already-started backend request reliably.

### Replace both maps with a new request scheduler

A unified scheduler could model every cache generation, but it would add a
larger abstraction and migration surface. Reusing the existing background
ownership token is sufficient for this bounded failure.

## Milestones

1. Add focused regression and boundary tests and prove the duplicate call on
   the exact parent.
2. Treat an existing background token as the owner for stale reads and return
   the stale value without another connector call.
3. Run the focused suite, the complete dashboard CI test list, lint, formatting
   check, build, and diff checks.

## Rollout

This is an internal dashboard cache behavior change with no persistence or API
schema change. Rollback is the single implementation commit. The cache remains
generation-fenced, so deploys do not require cache migration.
