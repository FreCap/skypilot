# Image operation poll ownership

## Problem

The dashboard polls each nonterminal managed-image operation every two seconds.
The polling effect currently depends on the full operation object, so every
nonterminal response replaces that object, restarts the effect, and immediately
polls again. Fast responses can therefore create a hot request loop. The fixed
interval also starts overlapping requests when a response takes longer than two
seconds.

## Behavior contract

For one mounted operation identity:

- at most one status request is in flight;
- fast request starts remain at least two seconds apart;
- if a request itself exceeds two seconds, the next request starts immediately
  after settlement instead of overlapping it;
- nonterminal results update the visible operation without replacing the poll
  owner;
- terminal results stop polling and notify the parent exactly once;
- failures remain visible and retry on the same bounded cadence;
- detach, unmount, workspace change, and operation identity change abort and
  revoke the old owner.

## Design

One effect owns the complete polling lifecycle. It is keyed by operation ID,
workspace, and terminal state rather than by the full response object. The
effect starts one request, then schedules the next request with a recursive
timeout after the current one settles. The timeout subtracts request duration
from the two-second interval, preserving the existing start cadence for fast
responses while serializing slow responses.

The existing generation counter and `AbortController` fence stale completions
and clean up both the active request and pending timer.

## Alternatives

An in-flight boolean around the fixed interval would prevent overlap, but timer
ticks and request settlement would remain separate lifecycle owners. Depending
on memoized response object identity would move correctness into the connector.
A shared polling hook is unnecessary until another caller has the same contract.

## Rollout and rollback

This changes browser-side coordination only. There is no API, persistence, or
operation-state migration. Reverting the commit restores the previous poller.

## Test plan

Fake-timer tests cover fast nonterminal responses, slow requests, terminal
completion, retry after failure, and cleanup. The dashboard CI workflow must
explicitly execute the focused test file, followed by lint, format checking, and
the production build.
