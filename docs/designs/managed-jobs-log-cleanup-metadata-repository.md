# Managed Jobs Log Cleanup Metadata Repository

## Problem

`sky/jobs/state.py` is 3,573 lines and still combines transaction-sensitive
managed-job lifecycle writes, scheduler and recovery fencing, accounting, and
the metadata protocol used by the log garbage collector. The log-cleanup
family is a 119-line leaf at the end of the module: it selects task and
controller artifacts whose retention period has elapsed, pages past failures,
and publishes cleanup timestamps after the filesystem work succeeds.

The garbage collector changes with retention and artifact-deletion policy,
while the rest of `state.py` changes with managed-job lifecycle and scheduling
semantics. Keeping the metadata protocol in the lifecycle facade makes its
selection and publication invariants harder to review and leaves an otherwise
bounded repository mixed with unrelated state transitions.

## Responsibility map

| Responsibility | Callers | Dependencies and state | Failure modes | Performance and change cadence |
| --- | --- | --- | --- | --- |
| Task-log cleanup candidate selection | `sky.jobs.log_gc._clean_task_logs_with_retention` | `spot` task identity, end time, local path, cleanup marker; `job_info.schedule_state`; current time; failed-task exclusion set | Active or incomplete logs selected, already-cleaned rows repeated, failed rows occupying every page, or task identity/path mismatched | One query per GC page; changes with task artifact retention and paging policy |
| Controller-log cleanup candidate selection | `sky.jobs.log_gc._clean_controller_logs_with_retention` | Job cleanup marker and schedule state; grouped `spot` rows; latest task end time; failed-job exclusion set | Controller log cleaned before the latest task finishes, null end times treated as old, duplicate jobs returned, or failed jobs starving later pages | One grouped query per GC page; changes with controller artifact retention and job aggregation policy |
| Task cleanup-marker publication | Task-log GC after successful filesystem deletion | Composite `(job_id, task_id)` keys, `spot.logs_cleaned_at`, one synchronous transaction | Failed deletion published as cleaned, duplicate keys expanding writes, empty batches opening a transaction, or partial task identity updates | One batched update and commit; changes with task cleanup acknowledgement semantics |
| Controller cleanup-marker publication | Controller-log GC after successful truncation | Job IDs, `job_info.controller_logs_cleaned_at`, one synchronous transaction | Failed truncation published as cleaned, duplicate IDs expanding writes, empty batches opening a transaction, or wrong job rows updated | One batched update and commit; changes with controller cleanup acknowledgement semantics |
| Managed-job lifecycle, scheduling, recovery, accounting, and other reads | Controllers, scheduler, recovery, Skylet, Serve, APIs, and status/log-follow consumers | Broad sync and async transactions, locks, callbacks, job/task rows, resource JSON, ownership fences | Lost transitions, stale-owner writes, cleanup-order races, oversubscription, or projection drift | Hot and transaction-sensitive; changes with lifecycle, scheduler, and API policy |

The four cleanup responsibilities form one artifact-retention metadata
protocol, but they are materially distinct from the stateful lifecycle
responsibilities retained in `state.py`. Candidate selection and cleanup-marker
publication also have separate caller phases, read versus write state,
failure modes, and transaction behavior. Task and controller flows use
different keys, tables, and aggregation rules.

## Proposed seam

Create `sky/jobs/state_log_cleanup.py` as a plain-function repository owning:

- `get_task_logs_to_clean()`;
- `get_controller_logs_to_clean()`;
- `set_task_logs_cleaned()`;
- `set_controller_logs_cleaned()`.

Keep `sky.jobs.state` as the stable facade and re-export all four functions as
direct aliases. Restore their historical `sky.jobs.state` module identity so
reflection and function pickle lookup continue to use the public entrypoint.
`sky.jobs.log_gc` continues importing and patching `sky.jobs.state`; it does
not gain a new dependency or bypass the facade.

The owner module imports the existing schema tables, database manager,
schedule-state enum, SQLAlchemy primitives, and time directly. It owns no
filesystem deletion, daemon scheduling, configuration reload, leader election,
retry policy, lifecycle transitions, or schema definitions. It introduces no
class hierarchy, protocol, registry, dependency injection, or package level.

## Behavior contract

- All four imports from `sky.jobs.state` retain their names, signatures,
  annotations, direct object identities, and historical module identity.
- Task candidates still require scheduler `DONE`, non-null and expired
  `end_at`, a non-null local log path, and no cleanup marker.
- Controller candidates still require scheduler `DONE`, a non-null local path,
  no controller cleanup marker, and a non-null maximum task end time older than
  the retention cutoff.
- Excluded task keys and job IDs remain outside the SQL result and do not
  consume batch slots. Empty and `None` exclusions retain existing behavior.
- Candidate result dictionaries retain exactly their current keys and value
  shapes. Batch limits and the current unspecified row order remain unchanged.
- Candidate selection remains one database statement and one snapshot per
  call. The cutoff continues to be calculated immediately after the session is
  opened from one `time.time()` call.
- Marker publication still deduplicates keys in first-seen order, performs one
  batched update and commit, and returns `None`.
- Empty marker batches return before obtaining the database engine or opening
  a transaction.
- No retry decorator, lock, ordering clause, chunking, validation, or row-count
  requirement is added as part of this structural extraction.
- Database schema, transaction boundaries, GC lifecycle ordering, filesystem
  behavior, remote commands, configuration, and serialized data do not change.

## Alternatives considered

Leave the functions in `state.py`. This avoids one module, but retains a
complete retention metadata protocol beside lifecycle transitions after the
schema, event, Batch, query, and task-lookup repositories have established the
same facade-first boundary.

Move the whole garbage collector. `sky/jobs/log_gc.py` owns filesystem
deletion, configuration, daemon loops, leader election, and logging. Those are
transport and process responsibilities, not database metadata, and moving them
would broaden the extraction rather than clarify ownership.

Move only the two selectors. Selection and marker publication are the two
halves of one at-least-once cleanup protocol. Separating them would make the
owner of retention eligibility different from the owner of durable
acknowledgement and leave mixed cleanup state in the facade.

Add methods to a repository class or protocol. There is one database
implementation and no construction or policy variation. Plain functions
preserve the established facade and add less carrying cost.

Change ordering, locking, retries, or chunking during the move. Those may be
valid behavioral changes, but none is required to establish the boundary.
They would obscure characterization of a pure extraction and need separate
evidence.

## Characterization and test plan

Before moving behavior, add and run characterization that pins:

- public function names, signatures, annotations, module identity, and
  function pickle lookup;
- exact task and controller eligibility, result projections, exclusion
  paging, batch limits, and one-query budgets;
- empty marker batches obtaining no engine and executing no SQL;
- duplicate marker keys updating the intended rows once as one statement;
- marker transaction counts and unchanged return values;
- existing `sky.jobs.log_gc` facade monkeypatch seams and success/failure
  publication ordering.

After extraction, extend characterization to prove facade-to-owner object
identity and both import orders. Run the focused state and log-GC suites plus
managed-jobs utility and log-follow tests that consume cleanup metadata. Run
`format.sh --files` for every changed Python file, mypy, Pylint, Ruff,
BasedPyright, import-linter, compilation, and `git diff --check`.

Compare the four moved function ASTs against the characterized base. Measure
alternating cold imports of `sky.jobs.state`, representative SQLite task and
controller candidate reads, and marker writes. SQL statement counts must be
identical, direct aliases must add no runtime frame, and timing must not
materially regress.

The Python unit-test workflow has no relevant changed-path exclusion for these
Python and test paths. The Jobs and API workflow covers the managed-jobs
consumer surface, while format, mypy, Pylint, Ruff, BasedPyright,
import-contract, and docs jobs cover the structural seam. The PR remains open
unless every relevant visible check is green on the exact final head and all
review threads are clear.

## Milestones

1. Land the canonical responsibility map and behavior contract.
2. Add characterization against the existing `sky.jobs.state` implementation
   and prove it passes before moving code.
3. Extract the complete four-function repository behind direct facade aliases.
4. Run the changed-path matrix, static checks, import and AST probes, and
   performance comparison.
5. Require relevant exact-head CI and review before a normal protected merge.

## Rollout and rollback

This is a structural extraction with no migration, feature flag, or behavior
change. Reverting the commit restores the previous module layout without
changing stored data, serialized values, process ordering, or caller imports.
