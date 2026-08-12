# Managed Jobs Pool Query Repository

## Problem

`sky.jobs.state` is the stable managed-job persistence facade, but it also
owns a 317-line read-only family that projects pool queue demand, worker job
counts, worker job IDs, dashboard status counts, and resource usage from the
managed-job tables.  These projections are called by SkyServe pool status and
autoscaling paths.  They change for pool scheduling and presentation reasons,
while the surrounding module changes for scheduler transitions, controller
ownership, recovery fencing, terminalization, and destructive cleanup.

The mixed ownership makes changes to pool query shape require navigating the
full managed-job lifecycle store.  It also obscures that this family is a
read-only repository with no transaction shared with the adjacent scheduler
writes.

## Behavior contract

The extraction must preserve all existing behavior:

- `sky.jobs.state` remains the public import and monkeypatch facade.
- Function names, signatures, annotations, docstrings, return shapes, ordering,
  and exception behavior remain unchanged.
- Function reflection and pickle lookup continue to resolve through
  `sky.jobs.state`.
- SQLite and PostgreSQL SQL shapes remain unchanged, including distinct job
  counts, grouped status row counts, terminal filtering, ordered job IDs, and
  the window function that avoids PostgreSQL JSON equality.
- Empty inputs retain their zero-query behavior.
- Every non-empty public query retains its current one-query budget.
- Resource decoding and fail-closed behavior remain unchanged for missing,
  heterogeneous, and empty resource requests.
- Callers in SkyServe and managed-job utilities require no source changes.
- Import ordering and cold import cost must not materially regress.

No serialized data, database schema, configuration format, CLI output, remote
command, lifecycle ordering, or user-visible behavior changes.

## Responsibility map

### Before

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Pool queue and assignment projections | SkyServe autoscalers, pool status, managed-job pool cleanup | `job_info` and `spot` tables, terminal status set, SQLAlchemy | Read-only queue demand, worker assignment, and status projections | Counting task rows as jobs, including terminal jobs, losing deterministic ordering, N+1 queries | One aggregate or grouped query per call | Pool autoscaling and dashboard semantics |
| Pool resource accounting | SkyServe worker placement and status | `full_resources` JSON, `Resources` decoding, row-number window, SQLite and PostgreSQL | Read-only per-job and per-worker resource projections | PostgreSQL JSON equality, double counting task history, undercounting unknown or empty requests | One query, one resource decode per selected job | Resource-aware pool scheduling |
| Scheduler and controller lifecycle persistence | Scheduler, controllers, recovery, APIs, Skylet, log consumers | Async and sync sessions, controller ownership fences, schedule and task state | Durable lifecycle, ownership, terminal outcome, cleanup state | Stale writes, recovery races, invalid transitions, partial cleanup | Hot refresh and scheduler paths | Controller reliability and recovery policy |

### After

| Owner | Responsibilities |
| --- | --- |
| `sky.jobs.state_pool_queries` | All read-only pool queue, assignment, dashboard, and resource projections, plus their private SQL and resource-decoding helpers |
| `sky.jobs.state` | Stable facade aliases and all remaining managed-job scheduler, controller, recovery, and lifecycle persistence |

## Chosen seam

Use a facade-first plain-module repository.  Move the complete contiguous
family from `get_pending_jobs_count_by_pool` through
`get_pool_worker_used_resources_by_cluster`, including all private helpers.
The new module imports the existing schema tables, database manager, status
type, and resource value object directly.  `sky.jobs.state` binds direct aliases
to every historical function and private helper.

This is one complete low-state leaf.  It does not split a transaction, move a
write, introduce a second implementation, or require callers to know about the
new owner.  Direct aliases avoid forwarding frames and preserve facade
monkeypatches at call sites.

## Alternatives considered

### Keep the family in `sky.jobs.state`

This has zero immediate carrying cost, but retains a proven independent reason
to change and keeps two SQL projection families mixed with safety-critical
lifecycle writes.  The family is large enough and cohesive enough to justify
one flat owner.

### Extract resource accounting only

This is smaller, but leaves the SQL ranking helper and related pool assignment
queries split across owners.  Both projection families share the same tables,
terminal filtering, one-query budget, and SkyServe pool consumers.  Moving the
complete read-only family gives a clearer boundary.

### Add a class, protocol, strategy, or dependency injection layer

There is one database implementation and no policy variant.  An object graph
would add construction and test surface without improving ownership.  Plain
functions and direct aliases are sufficient.

### Change callers to import the new module

This would expose the structural move, expand the diff into active Serve code,
and break historical monkeypatch paths.  The existing facade is the safer and
simpler public contract.

## Implementation milestones

1. Add and run characterization for facade identity, signatures, pickle lookup,
   return semantics, SQL compilation, query counts, and fail-closed resource
   behavior against the pre-extraction implementation.
2. Move the complete function family without behavioral edits.
3. Bind direct facade aliases and set historical module identity in the new
   owner.
4. Prove moved AST bodies are identical apart from module identity assignments.
5. Run the changed-path test matrix, formatting, typing, lint, import, and
   performance gates.

## Test plan

| Changed path | Coverage |
| --- | --- |
| `sky/jobs/state_pool_queries.py` | Pool query characterization, grouped resource accounting, PostgreSQL SQL compilation and execution |
| `sky/jobs/state.py` | Facade identity, signatures, pickle lookup, full managed-job state tests, async versus sync parity |
| `tests/unit_tests/test_sky/jobs/test_pool_resource_accounting.py` | Direct characterization of the complete extracted family and query budgets |
| This design | Documentation build and format checks |

Focused tests must run before and after movement.  The wider gate includes
managed-job state, server queue, controller, scheduler, jobs utilities,
SkyServe autoscaler, and Serve utility tests.  `format.sh --files` must pass for
all changed Python files, followed by `git diff --check`, compilation, both
module import orders, and the relevant static checks.

Performance evidence will compare cold imports and representative SQLite query
and resource-accounting timings against the exact base.  Structural equivalence
and unchanged query budgets are the primary hot-path proof.

## CI and rollout

The PR targets `improvements`.  Before merge, inspect workflow path filters and
map the changed paths to the Python unit, Jobs and API, format, mypy, Pylint,
Ruff, BasedPyright, import-linter, and documentation jobs.  All relevant checks
must pass on the exact final head.

This is an internal structural extraction with a stable facade and no data
migration.  Rollback is a normal revert that moves the functions back into
`sky.jobs.state`.

## Validation evidence

Characterization ran successfully before and after movement.  The focused pool
suite proves stable facade identity, signatures, pickle lookup, direct alias
ownership, one SQL statement for every non-empty projection, zero SQL for an
empty worker set, grouped status and ID semantics, terminal-task filtering,
resource fail-closed behavior, and PostgreSQL window-query compilation.  The
real PostgreSQL execution case is skipped locally because Docker is unavailable
and remains covered by the unfiltered CI unit suite.

The changed-path matrix collected 714 tests across managed-job state, scheduler,
controller, server queue, job utilities, SkyServe autoscalers, and Serve status
callers, and completed successfully.  Seventeen Jobs and Serve smoke cases were
also collected for remote integration coverage.

All ten moved function ASTs are identical to `origin/improvements`.  Both import
orders, compilation, Ruff, full mypy, Pylint at 10.00, import-linter, isolated
CI-version BasedPyright, dashboard lint and format, and `git diff --check` pass.
The GitHub Python and static-analysis workflows target `improvements` without
path filters, so Unit Tests, Jobs and API Tests, and every relevant static job
run for these paths.

Nine alternating cold imports improved the median from 766.59 ms to 750.75 ms.
Five representative SQLite samples changed the grouped-count median from
105.97 us to 105.91 us and the grouped-resource median from 389.02 us to
396.84 us.  The 2.01 percent resource variation is within local timing noise;
the function AST and one-query budget are unchanged, and direct aliases add no
runtime frame.
