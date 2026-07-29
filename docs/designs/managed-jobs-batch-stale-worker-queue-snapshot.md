# Managed Batch stale-worker queue snapshots

## Problem

A replacement Batch coordinator cleans durable worker records from every older
coordinator generation. When a record lacks an exact external job ID, cleanup
resolves it by reading the worker cluster queue and matching the generation's
immutable job name.

The stale cleanup pass currently reads that queue once per unresolved record.
Several stale generations on one worker therefore repeat the same remote calls.
Each later read can also observe a different queue after earlier exact
cancellations, so one logical cleanup pass does not use one discovery boundary.

## Behavior contract

- One stale cleanup pass uses at most one successful queue snapshot per worker
  cluster, shared across every stale coordinator generation.
- Different worker clusters use different snapshots.
- Durable job IDs and successful launch-request results still bypass the queue.
- Queue acquisition or parsing failure is not cached. Existing strict and
  best-effort failure handling can retry the cluster from a later stale
  generation or pass.
- Duplicate-name ambiguity still refuses cancellation. Every successful cleanup
  still targets one exact job ID and retires its durable record only after the
  cancellation completes.
- The pass adds no retry, poll, timer, database query, or provider operation.

## Solution

Create a pass-local dictionary in `_cleanup_stale_worker_services()` and thread
it through the token and record cleanup helpers. `_resolve_worker_job_id()`
stores a queue result only after the matching helper parses it successfully,
and reuses that list for later records on the same cluster. An invalid response
therefore retains the existing fail-closed strict behavior and remains
retryable during best-effort cleanup.

Extract the queue-record matching loop into a stateless helper so cached and
uncached resolution have one exact-name and exact-ID contract. Direct cleanup
callers omit the memo and retain their existing single-record behavior.

## Alternatives considered

A coordinator-wide cache risks using stale queue data across separate recovery
passes. Batching durable records by cluster would require a broader control-flow
rewrite. The pass-local memo supplies the needed consistency and call bound
without changing cleanup ordering or ownership.

## Changed-path-to-test matrix

| Changed path or invariant | Test file and command |
| --- | --- |
| `sky/batch/coordinator.py::_resolve_worker_job_id`, `_cancel_worker_record`: exact matching, durable-ID bypass, ambiguity refusal, queue failure containment | `tests/unit_tests/test_batch_recovery.py`; `uv run python -m pytest -o addopts='-s -n 0 -q --tb=short --disable-warnings' tests/unit_tests/test_batch_recovery.py` |
| `_cleanup_worker_services_for_token`, `_cleanup_stale_worker_services`: one snapshot per cluster across generations, distinct snapshots across clusters, exact cancellation, durable retirement | same file and full command; focused with `-k 'stale_cleanup_reuses_one_queue_snapshot_per_cluster'` |
| Performance: `sdk.queue` and its result `sdk.get` are called once per unresolved cluster, not once per stale record | focused regression above |
| Python formatting, typing, linting, and diff hygiene | `bash format.sh --files sky/batch/coordinator.py tests/unit_tests/test_batch_recovery.py`; `git diff --check` |

`.github/workflows/pytest.yml` has no pull-request path filter and runs the test
file in `Python Tests - Unit Tests`. The Jobs and API, format, mypy, Pylint, and
static-analysis jobs provide the adjacent component and repository checks.

## Baseline, rollout, and rollback

On the untouched parent, two stale generations sharing one worker cluster make
two queue requests. The regression requires one request for that cluster while
a second cluster receives its own snapshot. Exact cancel calls and record
retirement remain unchanged.

The memo is process-local and exists only for one cleanup call. There is no API,
schema, configuration, or migration change. Rollback restores per-record queue
reads while durable records continue to provide the recovery boundary.
