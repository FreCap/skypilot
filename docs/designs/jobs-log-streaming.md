# Managed Jobs Log Streaming Responsibility Split

_Created: 2026-08-02_

## Problem

`sky/jobs/utils.py` is 2,052 lines and owns several operationally distinct
lifecycles. It updates managed-job status after controller failures, performs
cancellation and cluster teardown, collects debug metadata, parses provisioning
progress, and streams controller or task logs. Line count alone does not justify
a split, but the 546-line `stream_logs_by_id` path and its supporting helpers
form a complete transport and presentation lifecycle with different dependencies,
failure modes, and reasons to change from status recovery and cancellation.

Recent history reinforces that boundary. The log-follow family changed repeatedly
for snapshot reuse, terminal detection, cancellation responsiveness, controller
fallback, and nonstreamable results. Those changes are cohesive with each other,
but independent of controller-death recovery and destructive cluster cleanup.

## Goals

Move the complete log-follow implementation into a focused plain module while
keeping `sky.jobs.utils` as the stable entrypoint used by generated remote code.
The extraction must preserve signatures, generated commands, historical patch
paths, polling order, status and query snapshots, stdout and stderr behavior,
return codes, cancellation, controller and task fallback, and runtime overrides.

This is a structural change only. It must not add a query, retry, sleep, thread,
filesystem read, remote backend call, runtime call, or copy on any characterized
path.

## Responsibility map before extraction

### Controller status and recovery policy

Callers include the API server, scheduler, recovery strategy, and controller
maintenance loops. Dependencies include managed-job state, process inspection,
global cluster state, scheduler ownership, and cluster teardown. It owns process
and schedule snapshots and can terminalize jobs or destroy clusters. Failure is
destructive, and status refresh latency and query counts are sensitive. It changes
when controller ownership and recovery policy change.

### Cancellation and debug collection

Callers include jobs server endpoints, controller tooling, and debug-dump flows.
Dependencies include task and pool state, command generation, cluster teardown,
event serialization, and filesystem manifests. It owns cancellation fanout and
debug artifact selection. Failures can leave clusters running or omit diagnostic
evidence. It changes with cancellation and support workflows.

### Log streaming and provisioning presentation

The generated managed-job command calls `sky.jobs.utils.stream_logs`, which
resolves a job and delegates to `stream_logs_by_id`. The lifecycle depends on log
files, `select`, threads, rich status presentation, controller-log streaming,
backend tailing, runtime overrides, task snapshots, and cancellation-aware sleeps.
It owns only transient stream state such as task selection, offsets, spinners, and
tail return codes. Failure modes are blocked follows, duplicate or missing output,
incorrect task selection, and leaked streaming work. It is latency-sensitive and
changes with log transport and presentation behavior.

These responsibilities share imported data models but not state ownership. The
log lifecycle has one narrow input and one integer return value, so it can move
without passing a broad orchestration object or splitting a transaction.

## Solution

Create `sky/jobs/log_streaming.py` and move the provisioning-log parser, follow
helpers, and `stream_logs_by_id` there. Keep `stream_logs` and a thin
`stream_logs_by_id` facade in `sky/jobs/utils.py` because generated remote commands
and tests use the historical module. The facade synchronizes only the few
replaceable function bindings and timing constants that callers patch directly;
module dependencies such as `managed_job_state`, `backends`, `threading`, and
`rich_utils` are shared module objects and require no forwarding layer.

The moved implementation remains a set of plain functions. There is no second
algorithm, construction protocol, event subscriber set, or external interface to
justify a strategy, builder, observer, adapter, registry, abstract base class, or
dependency injection layer. The existing `ManagedJobRuntime` and backend seams
remain authoritative.

## Compatibility contract

The following contracts must remain unchanged:

1. `sky.jobs.utils.stream_logs` and `stream_logs_by_id` signatures and return
   values.
2. Generated remote command text from `ManagedJobCodeGen.stream_logs`.
3. Historical patching of the facade sleep helper, name generator, provisioning
   parser, logger, and timing constants.
4. Controller versus task log selection, terminal and nonstreamable early exits,
   pool callbacks, runtime override selection, and backend fallback.
5. Polling, database snapshot, filesystem read, thread, `select`, remote tail,
   and sleep call counts for representative paths.
6. Output streams, spinner text, error wording, exception behavior, and return
   codes.
7. Import order and import time. No new eager heavy dependency is allowed.

One targeted follow-up is intentional after the extraction: when
`sky jobs logs --no-follow` sees a latest-task snapshot that is still
`RUNNING` but no longer has a stream target, it may perform one additional
local snapshot read before returning. That recheck is limited to the stale
RUNNING-without-stream-target case so a just-finished task can still surface
its cached logs; the common PENDING, STARTING, RECOVERING, and controller-tail
paths keep their previous operation counts.

## Alternatives considered

Leaving the module unchanged avoids a structural diff, but retains a fast-changing
transport lifecycle inside a module that also owns destructive recovery and
cancellation policy. Extracting only the provisioning parser is smaller, but it
does not remove log-stream ownership from the mixed module. Moving status or
cancellation first is riskier because those paths own database transitions and
cluster destruction. A class-based log streamer would add state and construction
surface without a second implementation.

## Test and performance plan

Add a characterization contract before movement that exercises the historical
facade patch seams and records representative operation counts. Run it on the exact
base and after extraction. The changed-path matrix is:

| Changed responsibility | Tests |
| --- | --- |
| Facade names, signatures, generated command | managed-job codegen tests and new facade contract |
| Snapshot, wait, task selection, terminal exits | `test_log_follow_lifecycle.py` |
| Provision progress parsing and controller fallback | `test_jobs_utils.py` and log-follow lifecycle tests |
| Runtime and backend tail dispatch | log-follow lifecycle and managed-job utility tests |
| Broader jobs behavior | focused jobs unit suites and smoke-test collection |

Run the focused characterization first, then the relevant jobs unit suites,
formatting, mypy, Pylint, Ruff, BasedPyright for changed files, compile and import
checks, `git diff --check`, and managed-job smoke collection. Inspect workflow path
filters and map the changed paths to the exact CI jobs before opening the pull
request.

Benchmark alternating base and head imports. For representative characterized
stream paths, compare operation counters rather than wall-clock timing because
network and polling latency are mocked; the required result is no additional
query, sleep, filesystem, thread, select, runtime, or backend-tail operation
except for the intentional one-time local snapshot refresh in the stale
RUNNING-without-stream-target `--no-follow` path.

## Rollout and rollback

The pull request is a pure source extraction with no deployment flag, database
migration, or serialized state change. Reverting the single extraction commit
restores the previous module. Merge only from the exact head that passes all
relevant CI and has no actionable review thread.
