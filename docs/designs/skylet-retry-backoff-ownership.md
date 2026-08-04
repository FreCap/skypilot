# Skylet Retry Backoff Ownership

_Created: 2026-08-01_

## Problem

`sky.backends.skylet_rpc._handle_grpc_error()` currently combines two
responsibilities: classifying a gRPC failure and sleeping before a possible
retry. This means cancellation during an `UNAVAILABLE` backoff is observed
only after the full sleep, and the last failed attempt sleeps even though no
retry remains.

The exact-parent baseline on `028440e7071f132e08f7f0921a2b1a7330cc1a15`
showed a request cancelled during the first retry backoff taking 0.409 seconds
to raise `CancelledError`. A single-attempt terminal failure also scheduled a
0.458-second backoff before raising `SkyletUnavailableError`.

## Goals

Transient error classification must not schedule lifecycle work. A retry loop
must wait only when another attempt remains, and that wait must wake immediately
when the current `SkyPilotContext` is cancelled. Active and context-free calls
must keep the same retry budgets, backoff values, attempt counts, exception
mapping, and streaming resume behavior.

The change must add no RPC, tunnel, database, cloud, thread, timer, or polling
operation. The cancellation-aware local wait overhead must be negligible
relative to the minimum 0.5-second production backoff.

## Background

Unary and streaming Skylet calls share `_handle_grpc_error()`. The helper
raises mapped exceptions for terminal gRPC statuses and returns only for a
retryable `UNAVAILABLE` error. Both retry loops already check cancellation
before each RPC attempt, but their shared helper uses `time.sleep()`, so a
cancel signal cannot wake the in-progress backoff.

`sky.utils.context_utils.sleep_with_cancellation()` is the established
synchronous bridge. With no request context it delegates directly to
`time.sleep()`. With a context it registers one callback, waits on a local
event, raises `asyncio.CancelledError` on cancellation, and always unregisters
the callback.

## Solution

Keep `_handle_grpc_error()` responsible only for error classification. In each
retry loop, enumerate attempts explicitly and call the cancellation-aware wait
only after a retryable failure and only when a subsequent attempt remains.

This gives one lifecycle owner to the loop:

1. invoke the unary call or consume the stream;
2. classify a gRPC failure;
3. if no attempt remains, raise the existing exhausted-retry exception;
4. otherwise wait once using the existing cancellation bridge;
5. begin the next attempt only if the request is still active.

The streaming loop retains its current whole-stream restart behavior after a
transient failure. Items already yielded remain unchanged.

## Changed-path-to-test matrix

| Changed production path or invariant | Test path | Exact command |
| --- | --- | --- |
| `sky/backends/skylet_rpc.py`: cancellation wakes unary retry backoff before another RPC | `tests/unit_tests/test_skylet_grpc_cancellable.py` | `pytest -o addopts='' tests/unit_tests/test_skylet_grpc_cancellable.py -q` |
| `sky/backends/skylet_rpc.py`: cancellation wakes streaming retry backoff before reopening the stream | `tests/unit_tests/test_skylet_grpc_cancellable.py` | same focused command |
| Unary and streaming terminal failures do not wait after the final attempt | `tests/unit_tests/test_skylet_grpc_cancellable.py` | same focused command |
| Active and context-free callers preserve exact attempt counts and backoff values | `tests/unit_tests/test_skylet_grpc_cancellable.py` | same focused command |
| Cancellation callback cleanup and no-context delegation remain intact | `tests/unit_tests/test_sky/utils/test_context_utils.py` | `pytest -o addopts='' tests/unit_tests/test_sky/utils/test_context_utils.py -q` |
| Skylet gateway facade and adjacent backend lifecycle behavior remain compatible | `tests/unit_tests/test_skylet_client_contract.py`, `tests/unit_tests/test_sky/backends/` | `pytest -o addopts='' tests/unit_tests/test_skylet_client_contract.py tests/unit_tests/test_sky/backends -q` |
| API request cancellation and backend integration guardrail | `tests/test_api.py`, `tests/test_jobs_and_serve.py` | `pytest -o addopts='' tests/test_api.py tests/test_jobs_and_serve.py -q` |
| Performance: no extra attempts or waits, no terminal wait, negligible local wait overhead | focused call-count tests plus an exact-head 100,000-iteration zero-timeout benchmark | run the focused tests and the benchmark recorded in the PR `Tested` section |

## CI coverage

`.github/workflows/pytest.yml` has no path filter for pull requests targeting
`improvements`. Its `Python Tests - Unit Tests` job executes all of
`tests/unit_tests`, including both focused files and the backend directory. Its
`Python Tests - Jobs & API Tests` job executes `tests/test_api.py` and
`tests/test_jobs_and_serve.py`. Repository format, mypy, pylint, Ruff,
async-lifecycle, resource-lifetime, and import checks provide the remaining
static and lifecycle gates for the changed Python path.

The live Skylet connectivity integration test requires a provisioned cluster
and is not selected by the pull-request workflow. This change does not alter
RPC wire behavior, tunnel creation, streaming framing, or provider interaction;
the required real-backend invariant is attempt and wait scheduling, which is
fully observable through exact call counts and cancellation wakeup tests.

## Alternatives considered

Adding another cancellation check after `time.sleep()` would prevent the next
RPC but would not improve cancellation latency and would retain the useless
terminal sleep. Passing a `final_attempt` flag into `_handle_grpc_error()` would
keep classification coupled to loop scheduling. A new retry abstraction would
increase surface area for two short loops and is not justified.

## Rollout and rollback

No schema, API, persisted state, or compatibility migration is involved. A
normal code rollback restores the previous retry timing. The behavioral risk is
bounded to transient Skylet gRPC retries and is covered for unary, streaming,
active, cancelled, terminal, and context-free states.
