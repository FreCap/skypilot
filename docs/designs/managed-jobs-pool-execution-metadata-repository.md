# Managed Jobs Pool Execution Metadata Repository

## Problem

`sky.jobs.state` is the stable managed-job persistence facade, but it also
owns a contiguous pool execution metadata family.  That family reads the
submission-time pool and execution mode, records the selected worker and
infrastructure, preserves node lineage, stores the resolved resource choice,
records the worker-local job ID, and reads the resulting submit context.

These operations change with pool scheduling, recovery, and SkyServe worker
assignment.  The surrounding module changes for task and schedule transitions,
controller ownership, cancellation, recovery fencing, terminalization, and
cleanup.  Keeping both responsibilities in the same 3,111-line implementation
owner makes execution-assignment changes require navigating unrelated
safety-critical lifecycle code.

## Behavior contract

The extraction must preserve all existing behavior:

- `sky.jobs.state` remains the public import and monkeypatch facade.
- Function names, signatures, annotations, docstrings, wrapped retry identity,
  return shapes, and missing-row behavior remain unchanged.
- Function reflection and pickle lookup continue to resolve through
  `sky.jobs.state`.
- Sync and async readers keep identical projections and one-query budgets.
- Writers retain their current table, predicate, commit behavior, and retry
  behavior.
- Infrastructure updates still omit unspecified fields and perform no update
  or commit when every field is absent.
- Node lineage still uses a locked read followed by the existing merge helper.
- Selected resources remain stored on the task table while the other execution
  metadata remains stored on the job-info table.
- Callers in scheduler, recovery, controller, debug dump, managed-job utilities,
  and SkyServe require no source changes.
- Import ordering and cold import cost must not materially regress.

No serialized data, database schema, configuration format, CLI output, remote
command, lifecycle ordering, or user-visible behavior changes.

## Responsibility map

### Before

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Pool execution metadata | Scheduler, recovery strategy, controller, debug dump, managed-job utilities, SkyServe pool scheduling | `job_info` and `spot` tables, sync and async SQLAlchemy sessions, retry decorators, node-lineage merge helper | Submission pool and execution mode, worker assignment, selected infra/resources, worker-local job ID | Missing rows, partial assignment snapshots, lost node history, storing an unresolved resource choice, sync/async projection drift | One query per reader, bounded writes, locked node-lineage merge | Pool scheduling, recovery, and Serve worker assignment |
| Managed-job lifecycle persistence | Scheduler, controllers, recovery, APIs, Skylet, log consumers | Task and job tables, transition guards, controller ownership fences, cleanup protocols | Durable task/schedule state, controller authority, terminal outcome, cancellation and recovery state | Stale writes, invalid transitions, recovery races, incomplete terminalization | Hot refresh, scheduler, and controller paths | Controller reliability and lifecycle policy |

### After

| Owner | Responsibilities |
| --- | --- |
| `sky.jobs.state_pool_execution` | The complete pool execution metadata protocol: pool/execution reads, worker and infrastructure assignment, selected resources, worker-local job ID, and sync/async submit-context reads |
| `sky.jobs.state` | Stable facade aliases and all remaining managed-job task, schedule, controller, cancellation, recovery, and cleanup persistence |

## Chosen seam

Use a facade-first plain-module repository.  Move the nine functions from
`get_pool_from_job_id` through `get_pool_submit_info_async` without changing
their bodies.  The new module imports the existing schema tables, database
manager, retry decorators, and node-lineage helper directly.  `sky.jobs.state`
binds direct aliases to every historical function.

This is one complete persistence protocol rather than a general pool utility
module.  Submission-fixed identity, runtime worker assignment, resolved
resources, and submit context are the durable description of where a pool job
executes.  The extraction does not split a caller-owned transaction or move
task and schedule transitions.  Direct aliases avoid forwarding frames and
preserve existing caller monkeypatch paths.

## Alternatives considered

### Keep the family in `sky.jobs.state`

This has no immediate migration cost, but retains a 166-line protocol with a
separate caller set and reason to change inside the lifecycle owner.  The
family is large enough and complete enough that one flat repository reduces
mixed ownership without exposing a new public API.

### Add these functions to `state_pool_queries`

That module deliberately owns read-only aggregate projections.  Adding writes,
row locks, commits, and recovery metadata would recreate mixed responsibility
there and weaken its read-only contract.

### Extract only the sync and async submit-info readers

This would move a small projection while leaving its writers and the rest of
the execution-assignment protocol behind.  It would add another owner without
clarifying durable state ownership.

### Add a class, protocol, strategy, or dependency injection layer

There is one database implementation and no policy variant.  Construction and
abstraction machinery would add carrying cost without a concrete second
implementation.  Plain functions and direct aliases are sufficient.

## Implementation milestones

1. Add and run characterization for facade identity, signatures, pickle lookup,
   wrapped retry identity, return semantics, SQL counts, commit behavior, node
   lineage, and sync/async parity against the pre-extraction implementation.
2. Move the complete function family without behavioral edits.
3. Bind direct facade aliases and set historical module identity in the new
   owner.
4. Prove moved AST bodies are identical apart from module identity assignments.
5. Run the changed-path test matrix, formatting, typing, lint, import, and
   performance gates.

## Test plan

| Changed path | Coverage |
| --- | --- |
| `sky/jobs/state_pool_execution.py` | Pool execution metadata characterization, sync/async parity, missing rows, query and commit budgets, node lineage, selected resource persistence |
| `sky/jobs/state.py` | Facade identity, signatures, wrapped retry and pickle lookup, full managed-job state regression suite |
| `tests/unit_tests/test_sky/jobs/test_pool_execution_metadata_repository.py` | Direct characterization of the complete extracted family and public seam |
| This design | Documentation build and format checks |

Focused characterization must pass before and after movement.  The wider gate
includes managed-job state, async versus sync parity, scheduler, controller,
recovery strategy, job utilities, debug dump, SkyServe pool scheduling, and
pool resource accounting.  `format.sh --files` must pass for every changed
Python file, followed by `git diff --check`, compilation, both module import
orders, and the relevant static checks.

Performance evidence will compare cold imports and representative SQLite sync
and async reads against the exact base.  Structural equivalence and unchanged
SQL statement counts are the primary hot-path proof.

## CI and rollout

The PR targets `improvements`.  Before merge, inspect workflow path filters and
map the changed paths to Python Unit Tests, Jobs and API Tests, format, mypy,
Pylint, Ruff, BasedPyright, import-linter, and documentation checks.  All
relevant checks must pass on the exact final head.

This is an internal structural extraction with a stable facade and no data
migration.  Rollback is a normal revert that moves the functions back into
`sky.jobs.state`.
