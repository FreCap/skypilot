# Managed Jobs Controller Log Stream Extraction

_Created: 2026-07-21_

## Problems

`sky/jobs/utils.py` is 2,075 lines and remains the stable managed-jobs facade,
but it owns several independently changing responsibilities. In particular,
`stream_logs` contains both local controller-log file transport and remote task
log transport. The local controller path performs filesystem tailing and status
polling, while the task path resolves cluster handles and streams through the
runtime or backend. Keeping both implementations in the facade couples two
failure domains and makes either transport harder to characterize in isolation.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Queue facade and compatibility re-exports | CLI, SDK, controller codegen, plugins | `queue_utils`, protobuf decoding, cluster handles | Late-bound facade aliases | stale compatibility aliases or changed serialized identity | queue query count and formatting | queue fields and API compatibility |
| Controller lifecycle and status reconciliation | refresh daemon, scheduler, recovery strategy | process identity, scheduler, job state, backend status, workspace config | controller PIDs, schedule state, recovery gates | split brain, stale status, or incorrect terminal transitions | status query and process-probe count | HA and controller lifecycle |
| Cancellation and teardown | CLI/codegen, recovery strategy | cancellation snapshots, signal files, cluster teardown | cancellation files and job status | wrong-workspace cancellation, leaked clusters, race with completion | batched state reads and teardown retry pacing | cancellation semantics |
| Debug-dump compatibility facade | controller codegen | extracted `debug_dump` module and historical helper hooks | manifest assembly only | missing or over-broad diagnostics | bounded parallel collection | diagnostic schema |
| Remote task-log streaming | CLI/codegen and dashboard log requests | runtime/backend transport, job handles, status DB, disconnect watchdog | current task, retry/follow status, display lifecycle | dropped connections, retries, preemption, stale task selection | transport calls and polling cadence | runtime and backend transport |
| Local controller-log streaming | `stream_logs(..., controller=True)` | local files, `log_lib`, status DB, payload filtering | file position and last observed terminal status | absent/truncated files, hidden control payload leakage, follow termination | O(tail) backward reads and fixed polling | controller logging and CLI tail UX |
| Managed-job token cleanup | API daemon | token persistence, expiry clock, name contract | sweep count only | deleting non-job tokens or failing to retry | one bounded DB scan per sweep | service-account lifecycle |

## Goals

Move only the complete local controller-log streaming leaf to a focused module.
Keep `sky.jobs.utils.stream_logs` as the public entrypoint, preserve its
signature, messages, exit codes, output order, polling cadence, and name lookup
semantics, and leave remote task-log streaming unchanged.

## Solution

Add `sky/jobs/controller_log_stream.py` with one plain
`stream_controller_logs` function. It owns controller-job lookup, local file
waiting, historical tailing, follow polling, final-byte draining, and exit-code
projection. `sky.jobs.utils.stream_logs` delegates only its `controller=True`
branch to this function. The facade continues to own the public API and the
controller-log path and payload-filter helpers, which are passed as callables
so existing late-bound helper patches and log-path policy remain effective.

No new class, protocol, registry, strategy, or package hierarchy is needed.
The extra function call occurs once per log request, before filesystem or DB
work, and does not change file reads, status queries, allocations, sleeps, or
printed output.

## Alternatives considered

Moving the 48-line expired-token sweep would create a very small module without
materially clarifying the main component. Moving all log streaming would cross
the stateful runtime/backend transport boundary and disturb more constants and
monkeypatch seams. A private helper in `utils.py` would improve readability but
would not separate ownership. The bounded local-file transport leaf is the
smallest extraction that changes responsibility ownership.

## Behavior and compatibility contract

`sky.jobs.utils.stream_logs` remains importable and keeps the same callable
identity path for callers. Controller-log output still filters relayed rich
status payloads, preserves `tail` and `tail_offset`, waits only while following,
reports terminal status with the same message and exit code, and returns success
without output when a non-following request finds no file. Existing database,
config, CLI, controller-codegen, and remote-command formats do not change.

For modern managed jobs, a terminal task status does not end a following
controller-log request by itself. The controller may still be streaming task
logs or cleaning up resources. Following ends only after the scheduler's
durable `DONE` transition, then performs the existing final-byte drain. Legacy
jobs without a scheduler-state row retain the terminal-task fallback. Each
poll reads task status and scheduler state in one SQL statement so the stop
decision cannot combine values from different lifecycle snapshots or add a
second query to the polling path.

The lifecycle correction uses this changed-path-to-test matrix:

| Changed production path | Invariants | Focused tests and command |
| --- | --- | --- |
| `sky/jobs/controller_log_stream.py` | modern terminal plus `ALIVE` keeps following; `DONE` stops and drains; legacy `NULL` schedule uses terminal status; non-following paths are unchanged | `pytest -n 0 tests/unit_tests/test_sky/jobs/test_controller_log_stream.py tests/unit_tests/test_sky/jobs/test_log_follow_lifecycle.py tests/unit_tests/test_jobs_utils.py` |
| `sky/jobs/state.py`, `sky/jobs/status_types.py` | one coherent query returns active, finalized, missing, and legacy lifecycle snapshots; the facade type identity remains stable | `pytest -n 0 tests/unit_tests/test_sky/jobs/test_state.py` |
| Polling performance | one SQL statement per lifecycle snapshot and only the required `ALIVE` to `DONE` polls | the SQL-count and call-count assertions in the two focused lifecycle test files above |

## Milestones and test plan

1. Add facade-level characterization tests for controller-log tail filtering,
   offset handling, name resolution, missing files, and follow completion. Run
   them against the unsplit implementation.
2. Move the complete controller-local branch and rerun the same tests, the full
   managed-jobs utility suites, formatting, static checks, and `git diff --check`.
3. Compare the moved behavior structurally and benchmark non-following local
   requests to confirm the single dispatch frame is immaterial relative to the
   unchanged filesystem and status operations.
4. Correct following completion to use the scheduler's durable `DONE` state,
   while preserving the legacy terminal-status fallback. Prove modern,
   legacy, missing-job, final-drain, and one-query polling boundaries with the
   matrix above.

The changed Python paths are covered by the pull-request `Unit Tests` matrix in
`.github/workflows/pytest.yml`, which has no pull-request path filter and runs
`tests/unit_tests`.
