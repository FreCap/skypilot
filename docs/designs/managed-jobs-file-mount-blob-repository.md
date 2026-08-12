# Managed Jobs File-Mount Blob Read Repository

_Created: 2026-08-11_

## Problems

`sky/jobs/state.py` is a 2,956-line stable facade that still implements two
file-mount blob reference reads alongside managed-job lifecycle transitions,
scheduler ownership, recovery, cancellation, and cleanup. The reads form one
field-specific protocol but have unrelated operational callers: the managed-job
controller resolves one immutable reference asynchronously, while the API
server's hourly garbage collector projects every live reference synchronously.

The seam must not grow into a generic submission-metadata module. Persisted
YAML, environment, config, task specs, workspace policy, and scheduler writes
have different transaction ownership and reasons to change. Sharing the
`job_info` table is not sufficient reason to move them together.

## Goals

Move the complete read protocol for `job_info.file_mounts_blob_id` to one
low-state repository while preserving all public behavior. Keep
`sky.jobs.state` as the public entrypoint with direct aliases, historical module
and pickle identity, exact signatures and decorators, SQL semantics, query
budgets, and caller behavior.

The extraction must leave lifecycle writes, job construction, scheduler
transactions, stored submission contents, task specs, and workspace policy in
`sky.jobs.state`.

## Background and Responsibility Map

The controller calls `get_file_mounts_blob_id_async(job_id)` once on demand and
caches the result for its lifetime. It depends on the async engine and
`retry_async`; missing jobs and null columns both return `None`. Its safety risk
is losing the artifact reference needed by a launch after controller failover.
Its hot-path budget is one query on the first access and no additional facade
frame.

The API server's blob collector calls `get_active_file_mounts_blob_ids()` once
per hourly pass. It depends on both `job_info` and task status rows: a blob is
live when any task row for its job is null or nonterminal. It returns distinct,
non-null IDs in one set-based query. Its safety risk is deleting live launch
contents; its performance risk is an N+1 scan or an extra query.

The retained facade owns materially different state: scheduler and lifecycle
transitions, controller generation fences, cancellation, recovery,
terminalization, cleanup, persisted YAML/env/config contents, task specs, and
default-workspace policy. Those responsibilities have transaction ordering,
mutability, and compatibility risks absent from the selected read protocol.

## Solution

### v0: Characterize the public protocol

Add focused tests against the unmodified facade for both readers. Prove public
signatures, `__module__`, pickle lookup, decorated retry identity, sync and async
results, null and missing-row behavior, active/terminal task filtering,
distinctness, and exact SQL statement counts.

### v1: Extract the field-specific repository

Create `sky/jobs/state_file_mount_blobs.py` containing the exact two
implementations. Use existing `state_storage` and `state_schema` dependencies
directly. Set the public function identities to `sky.jobs.state`, then bind
direct aliases in `sky/jobs/state.py`. Direct aliases preserve caller patch
points and add no runtime frame.

No caller, schema, database format, transaction boundary, lifecycle ordering,
CLI output, or serialized identity changes. Rollback is a source-only revert
because there is no migration or data rewrite.

## Alternatives Considered

Leaving both functions in place avoids a module, but retains a complete external
blob-liveness protocol inside the lifecycle facade after multiple larger state
families have moved out. The two-reader module has one durable field contract
and two independent consumers, so its ownership is more than line-count
tidying.

A broader submission-artifact gateway was rejected. `get_job_file_contents`,
`get_workspace`, `get_task_specs`, scheduler writes, and job construction share
some rows but not one lifecycle or transaction contract.

Wrappers were rejected because they add call frames and weaken exact public
identity. A class, protocol, strategy, registry, or dependency-injection layer
was rejected because there is no second database implementation or policy
variation.

## Test and Rollout Plan

The changed-path matrix is:

| Changed path | Responsibility | Local and CI evidence |
| --- | --- | --- |
| `sky/jobs/state_file_mount_blobs.py` | Sync liveness and async single-job reads | Focused characterization, Jobs unit suite, Python static checks |
| `sky/jobs/state.py` | Stable facade aliases and imports | Identity/pickle probes, state tests, import-order and compile probes |
| `tests/unit_tests/test_sky/jobs/test_file_mount_blob_repository.py` | Public contract and query budgets | Unit Tests CI and focused pytest |
| This design | Canonical boundary and evidence | Documentation build and lint |

Run characterization before movement and again afterward. Then run the focused
file-mount, controller, server-GC, state, scheduler, recovery, and async/sync
tests; collect relevant managed-job smoke tests; run `format.sh --files`, full
mypy and pylint through the formatter, Ruff, BasedPyright, import-linter,
compile/import probes, documentation checks, and `git diff --check`.

Measure alternating cold `import sky.jobs.state` samples against the exact base
and compare representative SQLite latency. Require unchanged statement counts
and no additional runtime call frame. Map the changed paths to CI workflow jobs,
push the exact tested head, and require the complete current-head check rollup
when GitHub has no configured required subset. Merge only with a clean merge
state and no actionable review thread.
