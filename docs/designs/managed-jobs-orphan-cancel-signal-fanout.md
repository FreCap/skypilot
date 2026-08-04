# Managed-jobs orphan cancel-signal fan-out

## Problem

Each jobs-controller process periodically scans the shared cancel-signal
directory. Signals for jobs owned by another controller stay in place while
the job is nonterminal. Signals for terminal or deleted jobs are stale and
must be removed so every controller does not list and status-check them
forever.

The scan already takes one batched status snapshot for all orphan signals, but
then reaps terminal signals serially. Each reaper acquires a distinct per-job
file lock. A contended or slow lock for one stale signal therefore prevents
every later terminal signal from being cleaned until it finishes, and total
cleanup latency is the sum of independent lock latencies. An ordinary
non-`OSError` failure also aborts the remainder of the scan.

## Behavior contract

- The one batched status snapshot remains the decision boundary. Only signals
  whose snapshotted status is terminal or absent may be reaped.
- Every eligible orphan signal starts reaping independently. A blocked or
  failed per-job lock must not prevent a sibling signal from progressing.
- Reaping remains idempotent and race-safe. A signal consumed concurrently by
  another controller is a successful no-op.
- Each per-job removal and its in-memory cancel-info cleanup are one
  cancellation-safe ownership unit. Cancelling the scan may return control to
  its caller, but already-started reapers finish their lock lifecycle and
  bookkeeping.
- An ordinary per-job cleanup failure is logged and contained. The signal
  remains eligible for a later scan.
- Owned cancellation delivery still finishes before orphan status I/O and
  orphan cleanup. The scan adds no database query, retry, timer, poll, or
  provider call.

## Solution

Derive the terminal-or-absent job IDs directly from the batched status
snapshot, then drain that finite snapshot through at most
`LAUNCHES_PER_WORKER` independent reaper workers. Each worker claims its next
job ID before awaiting, so one blocked worker does not prevent the others from
draining later entries. Remove the status predicate from the reaper so it owns
one responsibility: cancellation-safe removal and bookkeeping for an
already-eligible signal.

Shield the complete per-job reaper, not just its nested file removal, so scan
cancellation cannot remove a signal while skipping the matching cancel-info
cleanup. Contain ordinary exceptions within that per-job boundary so one
failure cannot cancel gathered siblings.

The fan-out reuses the controller's conservative eight concurrent external
launches per worker as its concurrency ceiling. This prevents many controller
processes scanning the shared directory from manufacturing a lock storm.
Coordination is `O(orphan signal count)` in total work,
but active worker state is
`O(min(eligible orphan signals, LAUNCHES_PER_WORKER))`. Steady-state work and
external call counts are unchanged.

## Alternatives considered

Keeping the serial loop minimizes instantaneous file-lock tasks but turns
independent per-job locks into a global convoy and makes the 15-second scan
cadence irrelevant while one reaper is blocked.

Moving cleanup into threads is unnecessary. `AsyncFileLock` and `anyio.Path`
already provide asynchronous acquisition and file operations.

Gathering one task per orphan signal would let a stale shared directory create
an unbounded number of tasks. A fixed set of local drain coroutines gives the
same sibling liveness without a task per signal. Reusing
`LAUNCHES_PER_WORKER` avoids adding a second external-operation concurrency
policy and is intentionally much smaller than the in-process 200-job capacity,
because every controller can race on the same shared orphan signals.

## Changed-path-to-test matrix

| Changed production path or invariant | Test file | Exact command |
| --- | --- | --- |
| `sky/jobs/controller.py::_process_cancel_signals`: one batched status snapshot, only terminal/absent orphan IDs are scheduled, and sibling reapers start independently | `tests/unit_tests/test_sky/jobs/test_controller.py` | `python -m pytest -q -o addopts='' tests/unit_tests/test_sky/jobs/test_controller.py -k 'CancelSignalScan'` |
| `sky/jobs/controller.py::_reap_orphan_cancel_signal`: per-job ordinary failure containment and sibling liveness | `tests/unit_tests/test_sky/jobs/test_controller.py` | same focused command |
| Cancellation safety: already-started reapers finish signal removal plus cancel-info cleanup after scan cancellation | `tests/unit_tests/test_sky/jobs/test_controller.py` | same focused command |
| Concurrency bound: a nine-signal snapshot starts at most `LAUNCHES_PER_WORKER` removals before any completes, then drains the remainder | `tests/unit_tests/test_sky/jobs/test_controller.py` | same focused command |
| Idempotent removal, owned-delivery ordering, repeated scan recovery, nonterminal preservation, and exact status-query counts | `tests/unit_tests/test_sky/jobs/test_controller.py` | same focused command |
| Performance: eight independent delayed reapers take approximately one delay rather than eight and add no status/file-operation calls | `tests/unit_tests/test_sky/jobs/test_controller.py` | same focused command |
| Adjacent managed-jobs controller lifecycle and cancellation behavior | `tests/unit_tests/test_sky/jobs/test_controller.py` | `python -m pytest -q -o addopts='' tests/unit_tests/test_sky/jobs/test_controller.py` |
| Python formatting, typing, lint, async lifecycle, and import contracts | production and test paths | `bash format.sh --files sky/jobs/controller.py tests/unit_tests/test_sky/jobs/test_controller.py`; repository static-analysis commands; `git diff --check` |

`.github/workflows/pytest.yml` has no pull-request path filter and runs the
entire mapped unit file under `Python Tests - Unit Tests`. The format, mypy,
Pylint, Ruff, basedpyright, async-lifecycle, worker-floor import, and import
contract workflows also run for pull requests targeting `improvements`.

## Baseline and performance proof

The regression installs two eligible orphan signals. The first reaper blocks
on its per-job removal. On the untouched base the second reaper has not started
before the first is released. With fan-out, the second signal is removed while
the first remains blocked.

The performance case installs eight eligible signals whose independent
removals each wait the same short delay. The untouched base takes roughly eight
delays; the changed implementation takes roughly one. The test also asserts
one batched status query, exactly eight removal attempts, and zero point-status
queries, proving that concurrency does not add external work.

With a 10 ms delay per removal, seven local runs measured a median of
0.087460 seconds on the untouched base and 0.011356 seconds on the changed
implementation, a 7.70x reduction. Both versions made exactly one batched
status read and eight reaper calls.

A separate boundary case blocks every started removal in a nine-signal
snapshot. Exactly `LAUNCHES_PER_WORKER` removals may start before release; once
released, all nine are attempted. This proves the liveness improvement does
not trade the serial convoy for unbounded task creation or a cross-controller
shared-lock storm.

## Rollout and rollback

This is process-local jobs-controller scheduling. It changes no schema, API,
signal format, status transition, retry cadence, or cross-controller ownership
rule. Rollback restores serial cleanup; idempotent signal files make mixed
versions safe.

## Manual verification

1. Place terminal orphan cancel signals for at least two managed jobs.
2. Hold the first signal's `.lock` from another process.
3. Run one controller cancel-signal scan.
4. Confirm the unlocked terminal signal is removed before releasing the first
   lock.
5. Release the lock and confirm the remaining signal and its local cancel-info
   entry are cleaned.
