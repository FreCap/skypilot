# Managed Batch superseded cleanup fan-out

## Problem

When a newer managed Batch coordinator takes ownership, the old coordinator
must stop only the worker services from its own incarnation. The cleanup is
bounded by one global deadline because each synchronous SDK call may hang.

Before the active-worker fan-out, `BatchCoordinator.handle_superseded()` walked
the snapshot serially. A blocked shutdown request for the first worker consumed
the global deadline before any later worker received a shutdown request or an
exact job-ID cancellation. The old coordinator returned on time, but sibling
workers could remain live until a later recovery pass discovered their durable
records. Cleanup latency also grew as the sum of independent SDK latencies.

After active cleanup, the same method resolves durable worker records that may
not yet contain an exact job ID. Each coordinator owns at most one durable
record per worker cluster: the table primary key is `(job_id,
coordinator_token, worker_cluster)`, and superseded cleanup selects only its own
coordinator token. Each external call must remain a separate timed segment.
Otherwise a timed-out resolver can continue into later queue, persistence, or
cancellation actions after `handle_superseded()` returns.

## Behavior contract

- The active-worker snapshot remains the ownership boundary. Cleanup never
  targets a replacement coordinator's token or an inferred job ID.
- Each snapshotted worker begins cleanup independently. One blocked or failed
  worker must not prevent a sibling worker from starting cleanup.
- Within one worker, ordering remains shutdown request, shutdown completion
  when the request succeeded, exact job-ID cancellation, cancellation
  completion, and durable-record removal after confirmed cancellation.
- Every SDK and state call remains under the existing single global deadline.
  Once the deadline expires, no new external action starts.
- A per-worker ordinary failure remains contained. The worker still advances
  to exact cancellation when its shutdown request or completion fails.
- Durable unresolved-record recovery starts only after every active-worker
  pipeline finishes within the deadline. If any active pipeline times out,
  cleanup returns and leaves durable state for the replacement recovery path.
- Durable recovery performs at most one queue request and one queue-result read
  for each unresolved record. Because one coordinator token owns at most one
  record per worker cluster, this is also at most one queue attempt per owned
  cluster. Failed attempts leave durable state available to a later recovery
  owner.
- Launch-request recovery, queue request, queue result, exact-ID persistence,
  cancellation request, cancellation completion, and record removal remain
  separate deadline-guarded segments. A timed-out segment may finish in its
  worker thread, but it cannot start the next segment.
- Queue fallback continues to refuse ambiguous job names, and exact job IDs are
  persisted before cancellation. Missing, invalid, or failed queue results do
  not trigger guessed cancellation or durable-record removal.
- Cancellation of the old controller continues to be handled by
  `_finish_superseded_cleanup()`, which shields this bounded cleanup before
  re-raising the supersession signal.

## Solution

Keep `_run_call()` and `_cancel_exact()` as the shared deadline and exact-ID
primitives. Extract the existing active-worker sequence into one local async
pipeline, then run all snapshotted pipelines with `asyncio.gather()`.

Each pipeline catches ordinary SDK failures through `_run_call()` and returns a
boolean indicating whether it stayed within the global deadline. After all
pipelines settle, return immediately if any timed out; otherwise continue into
the existing durable-record recovery and final active-worker drain.

For durable recovery, retain the asynchronous sequence of individually guarded
external calls and reuse `_matching_worker_job_ids()` for the pure exact-name
decision. Do not add a pass-local queue snapshot map: after filtering by the
current coordinator token, the durable primary key makes a second record for
the same cluster unrepresentable. The synchronous replacement-owner cleanup
path is different because it intentionally walks several stale coordinator
tokens; its cross-token queue memo remains valid.

This does not add retries, polls, database reads, provider calls, or durable
state. Successful active cleanup keeps the same per-worker call count.
Coordination adds one coroutine/task per active worker and keeps O(worker count)
memory. Superseded durable recovery remains O(owned worker clusters) in queue
calls and uses O(1) additional collection memory.

## Alternatives considered

Leaving cleanup serial preserves low instantaneous SDK concurrency but lets one
hung endpoint strand every sibling, defeating the bounded cleanup's purpose.

Moving only exact cancellations ahead of graceful shutdown would shorten the
path but changes shutdown semantics and may discard in-flight worker result
publication.

Giving each worker an independent full timeout can make total cleanup exceed
the controller's 60-second lifecycle bound. The single shared deadline remains
the correct outer budget.

Running all durable-record recovery concurrently is broader and can duplicate
work against active records. The high-value liveness gap is limited to the
already-owned active snapshot, so unresolved-record recovery remains serial.

Calling the synchronous `_resolve_worker_job_id()` once through
`asyncio.to_thread()` is smaller in line count, but that resolver performs
launch recovery, queue request/result, and persistence internally. Timing out
the outer thread cannot stop it from starting later external actions after the
global deadline, so it is not a valid reuse boundary.

Caching queue snapshots by cluster was rejected after post-merge audit. The
only test demonstrating reuse supplied two records with the same job, token,
and cluster, which the durable primary key rejects. Keeping the memo would add
state and failure semantics without changing a representable production path.

## Changed-path-to-test matrix

| Changed path or invariant | Test file | Command |
| --- | --- | --- |
| `sky/batch/coordinator.py::handle_superseded`: sibling active cleanup starts while another shutdown call is blocked | `tests/unit_tests/test_batch_recovery.py` | `python -m pytest -q -o addopts='' tests/unit_tests/test_batch_recovery.py -k 'superseded_cleanup'` |
| Per-worker shutdown-before-cancel ordering and exact job-ID targeting | `tests/unit_tests/test_batch_recovery.py` | same focused command |
| Shutdown failure containment and sibling liveness | `tests/unit_tests/test_batch_recovery.py` | same focused command |
| One global deadline, no post-timeout calls after active-worker or durable launch-recovery timeouts, and cancellation shielding | `tests/unit_tests/test_batch_recovery.py` | same focused command |
| Durable exact-ID recovery processes one representable record per owned cluster, persists each exact ID, and cancels/removes only that ID | `tests/unit_tests/test_batch_recovery.py` | same focused command |
| The durable schema rejects duplicate `(job_id, coordinator_token, worker_cluster)` identities | `tests/unit_tests/test_batch_recovery.py` | same focused command |
| A failed queue attempt is contained without persistence, cancellation, or removal | `tests/unit_tests/test_batch_recovery.py` | same focused command |
| Ambiguous queue matches remain uncancelled and durable for later recovery | `tests/unit_tests/test_batch_recovery.py` | same focused command |
| Call-count and latency performance: unchanged calls per successful active worker, sibling starts before blocked worker release, and one durable queue call per representable owned cluster | `tests/unit_tests/test_batch_recovery.py` | same focused command |
| Adjacent Batch takeover, lease, retry, cleanup, and durable-state behavior | `tests/unit_tests/test_batch_recovery.py` | `python -m pytest -q -o addopts='' tests/unit_tests/test_batch_recovery.py` |
| Adjacent managed-jobs integration surface | `tests/test_jobs_and_serve.py`, `tests/test_jobs_state_async_vs_sync.py` | `python -m pytest -q -o addopts='' tests/test_jobs_and_serve.py tests/test_jobs_state_async_vs_sync.py` |
| Python formatting, typing, lint, async lifecycle, and import contracts | production and test paths | `bash format.sh --files sky/batch/coordinator.py tests/unit_tests/test_batch_recovery.py`; repository static-analysis commands; `git diff --check` |

`.github/workflows/pytest.yml` has no pull-request path filter and its
`Python Tests - Unit Tests` job includes the mapped unit file. The format,
mypy, Pylint, and static-analysis workflows also run for pull requests to
`improvements` without excluding these paths.

## Baseline and performance proof

The regression test installs two active workers. Worker A's shutdown SDK call
blocks. On the untouched base, worker B has zero shutdown and cancellation
calls until A is released or the deadline expires. With fan-out, worker B
completes its shutdown and exact cancellation while A is still blocked.

The test also asserts the successful worker's exact five-call sequence. An
eight-worker benchmark with 5 ms per existing external call measured a median
of 0.298404 seconds on the exact base and 0.037628 seconds on the exact head
across seven runs. This is deterministic evidence that the change reduces
independent cleanup latency from additive to concurrent without adding
external work.

For durable recovery, a post-merge audit compared 100 valid same-token records
on distinct clusters against 100 same-token records forced onto one cluster.
The exact parent and merge both made 100 queue requests for valid state. The
merge made one request only for the invalid state, which SQLite rejects with a
primary-key violation. The test matrix therefore covers the real uniqueness
constraint and valid multi-cluster call counts instead of claiming an
unreachable asymptotic improvement. A deadline regression blocks
launch-request recovery, waits for `handle_superseded()` to return, then
releases the blocking call and proves that no queue, persistence, cancellation,
or removal action starts afterward.

## Rollout and rollback

The change is process-local to superseded managed Batch cleanup and requires no
schema, API, configuration, or migration change. Rolling back the durable
recovery extension restores per-record queue calls while leaving the already
landed active-worker fan-out intact. Durable worker records remain the recovery
fallback in either version. No PostgreSQL-specific state transition changes are
involved, so real PostgreSQL coverage stays outside this change's required test
matrix.

## Manual verification

1. Launch a managed Batch job with at least two pool workers.
2. Replace or restart its jobs controller while both worker services are live.
3. Delay one worker cluster's shutdown SDK call.
4. Confirm the other worker receives graceful shutdown and exact cancellation
   before the delayed call returns.
5. Confirm the old controller exits the cleanup path within the configured
   global deadline and the replacement coordinator owns subsequent recovery.
6. With unresolved durable records on two worker clusters, confirm the old
   controller issues one queue request per cluster, persists each exact ID, and
   cancels only those IDs.
