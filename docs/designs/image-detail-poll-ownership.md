# Image detail poll ownership

_Created: 2026-07-27_

## Problem

The managed-image artifact detail page refreshes nonterminal artifacts with a
fixed five-second interval. Every refresh aborts the previous detail request.
If the capabilities and detail chain takes longer than five seconds, each timer
tick aborts the request before it can publish, so the page can remain stale
indefinitely while repeating backend work.

The same refresh entrypoint is also used for initial loads, manual refreshes,
route changes, and polling. That makes timer ownership implicit and lets a poll
race a manual refresh or continue across a pagination boundary.

## Goals

For one compound image/workspace route scope:

1. At most one artifact detail request chain is in flight.
2. Poll starts remain at least five seconds apart.
3. A request that outlives the interval is allowed to settle, then the overdue
   next poll starts without an additional five-second delay.
4. Manual refresh and route changes may deliberately supersede an older owner.
5. Poll failures retry, terminal results stop polling, and leaving the first
   collection pages revokes poll ownership.
6. Route changes and unmount abort old work, and stale completions cannot
   publish into the new scope.

No new backend calls, scans, render-state updates, worker threads, or background
tasks are introduced. Coordination stays O(1).

## Background

`ImageDetail` loads capabilities and then artifact detail under one
`AbortController`. A generation counter and compound request scope already
fence stale completions. Collection paging has separate controllers.

The missing concept is an explicit owner for the primary request promise. The
timer currently invokes `load()` blindly, while `load()` aborts whatever
controller is current.

## Solution

Keep one request-owner record containing the compound scope, source, start
time, promise, and controller. The existing generation and scope checks remain
the publication fence.

The initial, manual, and route-change paths call the load entrypoint with an
explicit source and retain their existing supersession authority. The polling
loop waits the existing five-second delay when first activated, then checks the
owner for the current scope:

- if a request is in flight, await that exact promise;
- if the last start is younger than five seconds, schedule only the remaining
  delay;
- otherwise start one poll;
- after settlement, schedule from the last start time, using zero delay when
  the interval is already overdue.

The loop uses one timeout instead of a fixed interval. Its cleanup revokes the
loop generation and clears the timeout. When polling is disabled because the
user pages away from the first collection pages, cleanup aborts only a
poll-owned primary request. Initial, manual, and route loads retain their
existing lifecycle.

## Alternatives considered

Skipping interval ticks while a request is active avoids overlap, but a
six-second request would wait until the ten-second tick. That adds avoidable
staleness and does not preserve the existing start cadence.

Serial `setTimeout(load, 5000)` calls avoid overlap but schedule from response
completion, degrading the effective cadence by the request latency.

Removing polling avoids the race but breaks progress visibility for
nonterminal artifacts.

## Rollout and compatibility

This is an internal dashboard lifecycle change with no API or persisted-state
contract. Backward compatibility is not required. The existing API-version
fallback, compound-scope fencing, manual refresh, collection paging, and cached
error behavior remain unchanged.

## Test plan

`sky/dashboard/src/components/image-detail.test.jsx` will use fake timers and
deferred promises to prove:

- slow requests are not aborted or overlapped and trigger one overdue poll
  after settlement;
- fast polls retain the five-second start cadence;
- failures release ownership and retry;
- terminal results stop polling;
- manual refresh supersedes a poll and the timer reuses the manual owner;
- leaving first collection pages aborts poll-owned work and prevents a late
  reset;
- route changes and unmount abort and revoke old owners.

The dashboard workflow must explicitly run this test file. Local gates are the
focused suite, the exact dashboard CI Jest inventory, ESLint, Prettier, the
Next.js production build, actionlint for the workflow edit, and
`git diff --check`.
