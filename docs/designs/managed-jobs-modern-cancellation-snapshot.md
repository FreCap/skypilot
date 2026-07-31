# Modern Managed-Jobs Cancellation Snapshot

_Created: 2026-07-31_

## Problem

Managed-job cancellation still carries the controller-generation fields needed
to choose between the current consolidated signal directory and the retired
per-job legacy signal file. This keeps two delivery implementations alive and
adds controller PID columns and branching to every cancellation snapshot even
though modern lifecycle refresh already excludes legacy rows without a durable
workspace and schedule state.

## Goals

Cancellation should use one state snapshot shape and one signal transport.
Modern jobs must retain workspace authorization, status-refresh fencing,
graceful timeout payloads, retry behavior, and batched database reads. Legacy
single-controller rows may be excluded because this change does not preserve
backward compatibility.

## Background

`cancel_jobs_by_id()` takes an initial batched snapshot, refreshes all selected
live jobs once, and then takes a second batched snapshot before delivering
signals. `get_job_cancellation_states()` selects the cancellation-driving task
for each requested job. The old snapshot also returned controller PID metadata
solely so the caller could choose a signal-file location.

Modern controllers are represented by durable `job_info.workspace` and
`job_info.schedule_state` values and consume files from
`CONSOLIDATED_SIGNAL_PATH`. Legacy single-job controllers have incomplete
`job_info` lifecycle fields and consume a different file under `/tmp`.

## Solution

Restrict the cancellation snapshot query to rows with non-null workspace and
schedule state. Project only job ID, task ID, status, and workspace. Reduce
`JobCancellationState` to status and workspace, then always deliver eligible
live-job cancellation through `CONSOLIDATED_SIGNAL_PATH` under the existing
per-job file lock. Preserve the current graceful payload and OSError isolation
for each job.

Remove the legacy-controller classifier, signal-path constant, and signal enum
that existed only for the retired writer. Keep legacy rows excluded from both
status refresh and cancellation so a null schedule state cannot enter modern
lifecycle decoding.

## Alternatives Considered

Keeping the legacy branch while caching its classifier would reduce some point
queries but would retain two transports and the larger snapshot contract.
Migrating legacy rows in place would add startup migration and recovery risk
for data that this change is explicitly allowed to stop supporting.

## Changed-Path-to-Test Matrix

| Production path or invariant | Test file | Command |
| --- | --- | --- |
| `sky/jobs/utils.py`: running and graceful cancellation use the consolidated path; per-job write failures remain isolated | `tests/unit_tests/test_jobs_utils.py` | `pytest -q tests/unit_tests/test_jobs_utils.py -k "cancel_signal_file or graceful or cancel_batches_state_reads"` |
| `sky/jobs/state.py`: one latest modern task per job and legacy rows excluded | `tests/unit_tests/test_sky/jobs/test_state.py` | `pytest -q tests/unit_tests/test_sky/jobs/test_state.py -k "cancellation_state"` |
| `sky/jobs/state.py`: legacy cancellation does not enter refresh or signal delivery | `tests/unit_tests/test_sky/jobs/test_status_refresh_snapshot.py` | `pytest -q tests/unit_tests/test_sky/jobs/test_status_refresh_snapshot.py -k "legacy_job or issues_single_select"` |
| `sky/jobs/status_types.py`: reduced snapshot preserves facade identity and pickle behavior | `tests/unit_tests/test_sky/jobs/test_status_types.py` | `pytest -q tests/unit_tests/test_sky/jobs/test_status_types.py` |
| Performance: any number of selected jobs uses two batched cancellation snapshots and one batched refresh, with no per-job lifecycle reads | `tests/unit_tests/test_jobs_utils.py` | `pytest -q tests/unit_tests/test_jobs_utils.py -k cancel_batches_state_reads` |

The component suite is
`pytest -q tests/unit_tests/test_jobs_utils.py tests/unit_tests/test_sky/jobs`.
Formatting and static validation cover every changed Python path through
`bash format.sh --files`, `git diff --check`, mypy, pylint, and the repository
static-analysis workflow.

The full component suite also exercises job-event retention. Its fixture uses
an explicit UTC-aware timestamp so the existing retention contract is tested
identically on UTC CI workers and non-UTC developer machines; this is a test
determinism correction and does not change event persistence behavior.

## Rollout

This is a direct cutover with no compatibility mode. A cancellation request for
a legacy row returns the existing no-cancellable-job result and writes no
signal. Modern rows keep the existing database-call count and signal payloads.
Rollback is the parent commit because the schema is unchanged.
