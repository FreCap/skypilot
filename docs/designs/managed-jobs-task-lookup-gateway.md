# Managed Jobs Task Lookup Gateway

## Problem

`sky/jobs/state.py` is 3,787 lines and still combines transaction-sensitive
managed-job lifecycle writes, scheduler and recovery fencing, pool accounting,
log-retention metadata, and read-only task lookup queries. The task-filtered
wait and log-follow queries form a 207-line low-state leaf: both classify a
missing task against the exact task count from the same database snapshot, but
they serve different callers and deliberately select different columns.

Keeping these polling queries beside state transitions makes their one-query
and projection contracts harder to review. It also makes changes to log routing
or task-filter UX contend with unrelated scheduler and recovery work in the
largest remaining managed-jobs persistence module.

## Responsibility map

| Responsibility | Callers | Dependencies and state | Failure modes | Performance and change cadence |
| --- | --- | --- | --- | --- |
| Task-filtered wait snapshot | `sky.jobs.server.core.wait` and filtered log-follow status polling | `spot` task identity and status columns, exact task count, sync database session; owns no durable state | Missing job confused with missing task, duplicate names resolved inconsistently, stale status paired with a later count, or log-only columns added to polling | One slim query per poll; changes with wait and task-filter behavior |
| Task-filtered log-routing snapshot | `sky.jobs.log_streaming` initial resolution, terminal rendering, and follow handoff | `spot` status, task name and log metadata; `job_info` pool routing fields; exact task count; owns no durable state | Mixed recovery epochs, wrong pool target, invalid task classification, name-order drift, or extra round trips | One query per resolution; changes with log routing, recovery, and cached-log behavior |
| Latest-task and unfiltered log snapshots | Job-level log following and status readers | Shared `_latest_task_status_query`, terminal-status policy, `spot` and `job_info` | Latest-task drift, status-selection divergence, or duplicate query policy | Hot read path shared with status APIs; changes with latest-task semantics |
| Lifecycle, scheduler, recovery, cleanup, and accounting state | Controllers, scheduler, recovery strategy, Skylet, garbage collection, Batch, Serve, and API readers | Sync and async transactions, locks, callbacks, durable task and job rows, resource JSON, cleanup markers | Lost transitions, stale-owner writes, oversubscription, split cleanup, or accounting drift | Write, lock, and transaction sensitive; changes with lifecycle and scheduling policy |

The first two responsibilities share a stable task-filter lookup contract but
have materially different callers, dependencies, projections, and reasons to
change from the stateful responsibilities retained in `state.py`.

## Proposed seam

Create `sky/jobs/state_task_lookups.py` as a plain-function gateway that owns:

- `TaskWaitStatusLookup` and `TaskLogStreamLookup`;
- the exact task-count scope and wait-row projection helpers;
- task-filtered wait lookups by numeric ID and task name;
- task-filtered log lookups by numeric ID and task name.

Keep `sky.jobs.state` as the stable facade. Re-export both result types and all
four public lookup functions as direct aliases, restoring their historical
`sky.jobs.state` module identity. Keep `get_task_log_stream_snapshot()` as the
single small facade wrapper because existing callers and tests patch
`sky.jobs.state.get_task_log_stream_lookup`; the wrapper must continue to
resolve that patched facade global.

Leave `get_latest_log_stream_snapshot()` and `get_log_stream_context()` in
`state.py`. The latest lookup shares `_latest_task_status_query` with broader
status reads, while the older context query is not part of the filtered
missing-task contract. Moving either would broaden the extraction or create a
callback solely to cross the module boundary.

The owner module imports the existing `state_schema`, `state_storage`, status
types, retry helper, and SQLAlchemy primitives directly. It introduces no
class hierarchy, protocol, repository object, registry, dependency injection,
or new package level.

## Behavior contract

- All existing imports from `sky.jobs.state` remain valid with the same names,
  signatures, retry behavior, and historical module identities.
- `TaskWaitStatusLookup` and `TaskLogStreamLookup` keep their tuple shape,
  equality, constructor behavior, and pickle identity through
  `sky.jobs.state`.
- Numeric task filters match exactly. String filters continue selecting the
  first matching task in ascending `task_id` order.
- Because `(spot_job_id, task_id)` is not unique in the persisted schema, log
  lookups collapse duplicate rows before materialization. Any non-terminal
  incarnation wins over terminal duplicates; otherwise the newest terminal
  row wins. The selected row supplies status, task name, log metadata, and
  routing context together. Name filters resolve the first logical task ID,
  then apply the same duplicate policy.
- Missing jobs return `num_tasks == 0`; missing tasks in existing jobs return
  the exact positive task count. Neither case raises.
- Each lookup remains one database statement and one database snapshot.
- Wait lookups continue excluding `job_info`, log paths, cleanup timestamps,
  and pool-routing columns.
- Log lookups continue returning status, pool routing, task name, local log
  path, cleanup timestamp, and exact task count together.
- Existing database-manager, table, enum, and `JobLogStreamSnapshot` identities
  remain shared. No persistent format, transaction, or caller import changes.

## Alternatives considered

Leave the code in `state.py`. This avoids one module, but retains a complete
read-only polling gateway inside a large stateful store after the schema,
Batch, filtered-query, and event repositories have already established the
same facade-first structure.

Move every log-related read. This would capture a larger block, but latest-task
selection is shared status-query policy and log cleanup owns durable retention
markers. Combining them would replace one mixed module with another.

Add these functions to `state_queries.py`. That module owns dashboard and API
filtered projection over broad job sets. Task-filter polling has different
callers, exact-snapshot classification, and hot-path query budgets, so a small
dedicated gateway has clearer ownership.

Use a repository class or protocol. There is one database implementation and
no construction or policy variation. Plain functions preserve the established
managed-jobs state seam with less carrying cost.

## Characterization and test plan

Before moving behavior, add and run characterization that pins:

- historical type and function module identities;
- pickle round trips for both public result types;
- the facade-global patch seam used by
  `get_task_log_stream_snapshot()`;
- existing one-query, missing-task, missing-job, duplicate-name ordering, row
  projection, and recovery-snapshot behavior.
- both insertion orders for duplicate task identities, active-over-terminal
  precedence, newest-terminal selection, coherent selected-row metadata, and
  exactly one materialized row from one statement.

After extraction, extend the characterization to prove facade-to-owner object
identity. Run the focused state lookup tests, log-follow lifecycle and utility
tests, wait tests, async-versus-sync state parity, and managed-jobs server tests
that consume the facade. Run `format.sh --files` for every changed Python file,
its mypy and Pylint stages, Ruff, BasedPyright, import-linter, compilation,
import-order probes, and `git diff --check`.

Compare the moved implementations structurally against the characterized base.
Measure alternating cold imports of `sky.jobs.state` and representative SQLite
wait and log lookups. Query counts must remain exactly one, and import or lookup
timing must not materially regress.

The Python unit-test workflow and Jobs and API test workflow have no relevant
changed-path exclusions for these Python and test paths. Static analysis,
format, mypy, Pylint, and import-contract jobs must also run. The PR remains
open unless every relevant visible check is green on the exact final head and
review threads are clear.

## Rollout and rollback

This is a structural extraction with no migration, feature flag, or behavior
change. Reverting the commit restores the previous module layout without
changing stored data, serialized values, process ordering, or caller imports.
