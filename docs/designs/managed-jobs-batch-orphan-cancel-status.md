# Batch managed-job orphan cancel status reads

_Created: 2026-07-22_

## Problems

The consolidated managed-jobs controller scans a shared directory for cancel
signals. Signals for jobs owned by the current controller are delivered without
a database read, but every signal for a job owned elsewhere or already terminal
calls `get_status_async()` independently. Directory order determines when an
owned cancellation is delivered. A backlog of stale orphan signals can
therefore delay a live cancellation behind one database round trip per orphan,
and each controller repeats the same point-query pattern every 15 seconds.

The current behavior is correct for an isolated signal, but its liveness and
database cost degrade linearly in the number of unrelated orphan files.

## Goals

Each scan must deliver signals for jobs owned by this process before doing
orphan status I/O. All orphan terminality decisions in the scan must come from
one consistent batched status snapshot for ordinary scan sizes. Missing and
terminal jobs are reaped, non-terminal jobs remain for their owner, a signal
that vanishes under its file lock remains a no-op, and cancellation or a failed
scan must preserve the existing retry behavior.

The owned path must continue to issue zero status queries. For `n` orphan
signals, database work must be O(ceil(n / chunk_size)) queries and O(n) CPU and
memory, rather than O(n) queries. No backend or network call is added.

## Background

`ControllerManager._process_cancel_signals()` lists the consolidated signal
directory and checks `self.job_tasks` under `_job_tasks_lock`. Owned files are
consumed under an async file lock and cancel the matching task. Orphan files are
reaped only after `managed_job_state.get_status_async()` proves the job is
terminal or absent. Managed job status is the first non-terminal task in task
order, or the last task when all tasks are terminal.

The state layer already uses `_STATUS_CHECK_JOB_ID_CHUNK` for bounded `IN`
queries. The new batch read will preserve the same latest-task semantics while
using one async session and one ordered query per chunk.

## Solution

Add `get_statuses_async(job_ids)` to the managed-job state facade. It
deduplicates IDs in caller order, reads `(job_id, task_id, status)` rows ordered
by job and task, groups one bounded chunk in memory, and applies
`get_latest_task_id_from_statuses()` per job. Missing IDs map to `None`.

Change the cancel scan to parse numeric signal names once, snapshot owned tasks
under one lock acquisition, deliver those owned signals first, then fetch one
batched orphan status snapshot and reap only terminal or missing orphan files.
Classification is conservative under races: a newly owned job classified as an
orphan is non-terminal and keeps its signal for the next scan; a task that
finishes after the ownership snapshot can still safely receive cancellation,
matching the current behavior.

## Alternatives considered

Running point queries concurrently with `asyncio.gather()` reduces wall-clock
latency but retains O(n) database load and can exhaust the connection pool under
a large stale backlog. Reusing the synchronous cancellation snapshot API via
`asyncio.to_thread()` reads unrelated authorization and controller fields and
adds a worker-thread hop. Reaping without a status read risks deleting a signal
owned by another live controller.

## Changed-path-to-test matrix

| Changed production path or invariant | Test file and focused command |
| --- | --- |
| `sky/jobs/state.py::get_statuses_async`: latest non-terminal task, all-terminal fallback, missing IDs, duplicate IDs, chunked query bound | `tests/unit_tests/test_sky/jobs/test_state.py`; `pytest -n 0 tests/unit_tests/test_sky/jobs/test_state.py -k statuses_async` |
| `sky/jobs/controller.py::_process_cancel_signals`: owned cancellation precedes orphan status I/O and owned-only scans make zero status reads | `tests/unit_tests/test_sky/jobs/test_controller.py::TestCancelSignalScan`; `pytest -n 0 tests/unit_tests/test_sky/jobs/test_controller.py -k CancelSignalScan` |
| Terminal and missing orphan reaping, non-terminal preservation, vanished-file race, scan failure retry, cancellation propagation | `tests/unit_tests/test_sky/jobs/test_controller.py::TestCancelSignalScan`; same command |
| Query and scheduling performance: one state call for multiple orphans, no state call for owned-only scans, owned delivery occurs while the batch state read is blocked | `tests/unit_tests/test_sky/jobs/test_controller.py::TestCancelSignalScan`; same command |
| Adjacent managed-jobs controller and state behavior | `tests/unit_tests/test_sky/jobs/test_controller.py tests/unit_tests/test_sky/jobs/test_state.py`; `pytest -n 0 tests/unit_tests/test_sky/jobs/test_controller.py tests/unit_tests/test_sky/jobs/test_state.py` |
| Managed-jobs integration and async/sync state parity | `tests/test_jobs.py tests/test_jobs_state_async_vs_sync.py`; `pytest -n 0 tests/test_jobs.py tests/test_jobs_state_async_vs_sync.py` |
| Format, typing, lint, and diff hygiene | `bash format.sh --files sky/jobs/state.py sky/jobs/controller.py tests/unit_tests/test_sky/jobs/test_state.py tests/unit_tests/test_sky/jobs/test_controller.py`; `git diff --check origin/improvements...HEAD` |

`.github/workflows/pytest.yml` has no pull-request path exclusion. The `Python
Tests - Unit Tests` job executes both focused unit files, and `Python Tests -
Jobs & API Tests` executes `tests/test_jobs.py` and
`tests/test_jobs_state_async_vs_sync.py`. Formatting and static-analysis
workflows also have no relevant path exclusion.

## Baseline and performance proof

The two controller regressions fail on the untouched base: while the first
orphan point read is blocked, the owned task has zero cancellation calls, and a
multi-orphan scan reaches the test's forbidden point-read path. With the change,
the same scan delivers the owned cancellation first and awaits one batch state
call for all three orphans. The state tests pin one query for five IDs at the
default chunk size, three queries when the chunk size is two, and zero queries
for empty input. This deterministic call-count evidence is the performance
gate; the implementation adds only linear grouping over rows already required
to decide terminality.
