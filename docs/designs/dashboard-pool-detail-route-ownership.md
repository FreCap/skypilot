# Dashboard Pool Detail Route Ownership

_Created: 2026-07-28_

## Problem

The managed-job pool detail page fences late request completions with a request
version, but its rendered state is not owned by a pool route. React effects run
after render, so navigating from pool A to pool B can render pool A's details,
worker actions, links, entrypoint, YAML, and settled loading state once under
pool B's URL before the new request begins.

This is a lifecycle correctness issue at the route commit boundary. The
existing request version correctly prevents an old request from overwriting a
new result later, but it cannot protect the render that occurs before effect
cleanup and setup.

## Goal

The first render for a new pool route must publish no state from the previous
pool. The new route must show its initial loading view until its own snapshot
or error settles. Late success and failure results, route changes during retry,
duplicate refresh coalescing, and refresh failure cleanup must keep their
existing behavior.

The fix must add no request, cache invalidation, timer, effect, scan, or render
loop. Per-render overhead must remain constant time.

## Background

`PoolDetailPage` owns the pool snapshot, loading flags, error, request version,
and refresh promise. Its route effect starts one pool-scoped cache read and
invalidates the prior request version during cleanup. The same dashboard
already uses route-owner refs in the cluster and managed-job detail connectors
to cover the render-before-effect boundary.

The pool status response contains a pool name, but using only
`poolData?.name === poolName` is insufficient. A failed or missing response has
no pool object, so an identity-only loading rule could remain loading forever
instead of showing the route's settled error.

## Solution

Add one ref that records which route owns the page state. Initialize it from
the current route. When the existing route effect observes a different pool,
advance ownership and clear the prior pool snapshot and error before starting
the existing fetch.

During render, compare the owner ref with the current route. A mismatch forces
the initial loading view and suppresses prior error state. The effect then
advances ownership, clears the stale snapshot, and the existing synchronous
loading updates keep the new route in its loading view until its request
settles.

No network or cache topology changes. Request-version fencing and the
pool-scoped refresh promise remain the only asynchronous owners.

## Alternatives considered

Keying the entire page component by route would reset all local UI state, but
that requires ownership outside this page and broadens the change beyond the
data lifecycle.

Deriving ownership only from `poolData.name` is smaller but cannot distinguish
a pending route from a settled missing or failed route.

Adding a route name to React state would work, but it adds a state transition
and render solely for bookkeeping. A ref plus the existing loading state is
the established constant-time pattern.

## Test and performance plan

`sky/dashboard/src/pages/jobs/pools/[pool].js` maps to
`sky/dashboard/src/tests/pool-details.test.jsx`.

The regression test must load pool A, rerender immediately for pool B, and
inspect that same render before effects are flushed. It must prove A's details
and refresh action are absent, B's loading view is present, and exactly one
additional pool read begins after effects run.

Adjacent tests cover late success, late failure, route changes during retry,
duplicate refresh coalescing, refresh success cleanup, and refresh failure
cleanup. The focused suite command is:

```text
npm --prefix sky/dashboard test -- --runInBand \
  src/tests/pool-details.test.jsx
```

The full dashboard workflow explicitly lists this test, then runs ESLint,
Prettier, and a production build for pull requests to `improvements`.

Performance evidence is structural and test-backed: the route change must
still cause exactly one cache read for pool B, while the implementation adds
one ref comparison per render and no request, invalidation, timer, effect,
state variable, collection scan, or additional render loop.

## Rollout

Ship as a dashboard-only change. No migration or compatibility path is needed.
If the focused ownership or exact call-count tests fail, revert the route gate
and retain the existing request-version behavior.
