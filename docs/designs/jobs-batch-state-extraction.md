# Managed Jobs Batch State Extraction

_Created: 2026-07-20_

## Problem

`sky/jobs/state.py` is 4,113 lines and owns several persistence domains that
change for different reasons. General managed-job lifecycle transitions,
filtered query projection, pool scheduling and accounting, event retention,
and Sky Batch coordinator persistence all share one module even though Batch
state is called almost entirely by `sky.batch.coordinator` and has a separate
failure model built around coordinator ownership, worker launch journals,
attempt fencing, leases, and retry backoff.

The Batch functions are currently split into two regions of `state.py`: the
coordinator, worker, and attempt repository around lines 1951 through 2443,
and owner-fenced task lifecycle transitions around lines 3310 through 3438.
This makes Batch invariants harder to review as one unit and makes unrelated
managed-jobs changes contend with Batch persistence changes.

## Goals

Move the complete Batch-only persistence repository behind the existing
`sky.jobs.state` entrypoint without changing public signatures, callable
identity through the facade, database schemas, transaction boundaries, query
counts, lock ordering, retry decorators, logging behavior, or caller imports.

Keep the extraction bounded. Do not move general job lifecycle, pool
scheduling, query projection, schema definitions, or event-retention logic.

## Background and Responsibility Map

The current module has these independently changing responsibilities:

1. General managed-job lifecycle and query projection. Controllers,
   schedulers, API endpoints, CLI code, dashboard readers, and recovery code
   depend on it. It owns job and task status, schedule state, query filters,
   pagination, and response rows. Its failures are lost transitions, stale
   overwrites, response drift, and inefficient queries. It changes with the
   managed-jobs API, scheduler, and recovery behavior.
2. Batch coordinator ownership and fencing persistence. The Batch coordinator
   and recovery tests call it. It owns the durable coordinator token and the
   serialization point used by SQLite and PostgreSQL. Its failures are split
   brain and stale-owner mutation. It changes with Batch takeover behavior.
3. Batch worker launch-journal persistence. The Batch coordinator records
   launch intent, request ID, external job ID, and cleanup completion. Its
   failures are duplicate launches, leaked workers, and deleting the wrong
   generation. It changes with Batch worker launch and cleanup orchestration.
4. Batch attempt claim, lease, completion, retry, and reclaim persistence. The
   Batch dispatch loop calls it. It owns attempt IDs, owner tokens, lease
   expiry, retry counts, and backoff timestamps. Its failures are duplicate
   claims, stale completion, premature reclaim, and lost results. It changes
   with Batch dispatch and recovery policy.
5. Pool scheduling and resource accounting. The managed-jobs scheduler, pool
   controllers, Batch assignment, and Serve capacity readers call it. It owns
   waiting, launching, alive, and done state plus resource projections. Its
   failures are oversubscription, stale assignment, and incorrect accelerator
   accounting. It changes with scheduling and capacity policy.
6. Event and log-retention metadata. Transition helpers, APIs, debug dumps,
   and garbage collection call it. It owns append-only events and cleanup
   markers. Its failures are missing diagnostics, stale reasons, and premature
   deletion. It changes with diagnostics and retention policy.

Responsibilities 2 through 4 form one repository boundary. They share the
same owner-token lock and must move together. Their callers, state, failure
modes, and change cadence differ materially from responsibilities 1, 5, and 6.

## Solution

Create `sky/jobs/batch_state.py` containing the Batch-only repository:

- batch initialization and reads;
- coordinator ownership acquisition and validation;
- worker launch-journal insert, update, read, and removal;
- attempt claim, lease renewal, status transition, and expired-attempt reclaim;
- owner-fenced Batch task transitions to winding down, succeeded, and failed.

Keep `sky.jobs.state` as the stable facade by importing these functions as
direct aliases. Existing callers continue using `sky.jobs.state`; there is no
wrapper, registry, abstract base class, dependency-injection layer, or extra
runtime call frame.

Both modules require the same `DatabaseManager` object. Create the small
`sky/jobs/state_storage.py` gateway that owns `create_table` and the single
`DatabaseManager('spot_jobs', create_table)` instance. `state.py` directly
re-exports both historical names, and `batch_state.py` imports the same manager
object. This preserves the existing test and runtime behavior where changing
the manager's engine is immediately visible to every state operation.

The Batch module initializes its logger with the historical
`sky.jobs.state` name so extracted log records do not change category. The
SQLAlchemy statements and decorator placement move without behavioral edits.

## Alternatives Considered

Leaving the code in place avoids structural churn, but PR #690 demonstrated
that unrelated schedule-state changes still overlap the same 4,000-line file
and test surface. The Batch repository is now a stable, independently tested
leaf after schema ownership moved to `state_schema.py`.

Moving only worker journaling or only attempt leasing would split the
owner-token lock invariant across modules and leave Batch persistence harder to
reason about. Those operations must remain together.

Passing the database manager through seventeen facade wrappers would preserve
dependency direction but add redundant forwarding layers and call frames.
Module-level configuration or a registry would introduce mutable hidden state.
Creating a second database manager would break engine monkeypatching and risk
different SQLite connections. A tiny shared storage gateway is simpler and
preserves one manager identity.

## Test Plan

Before moving code, add and run a characterization test that pins every
historical Batch facade signature. After extraction, extend it to prove direct
callable identity, shared database-manager identity, logger name, and table
identity.

Run the SQLite Batch recovery suite and jobs-state suite, plus the sync/async
parity suite. Run the PostgreSQL Batch recovery suite when the local PostgreSQL
fixture is available. Existing tests pin owner takeover, lock ordering,
attempt-token compare-and-set predicates, leases, backoff, worker cleanup,
terminal transitions, and optimized query counts.

Run `format.sh --files` for all changed Python files, full mypy and Pylint as
invoked by that script, compile checks, and `git diff --check`. Compare the
moved function ASTs after normalizing module placement. Measure alternating
cold imports of `sky.jobs.state`; direct aliases must add no calls, queries,
transactions, or row copies.

The unfiltered Python unit-test workflow collects the Batch and jobs-state
tests. Static analysis, format, type checking, import-linter, and runtime
contract jobs cover all changed Python paths. The PR must remain open unless
the full visible exact-head check rollup is green and review threads are clear.

## Rollout

This is a structural extraction with no migration or feature flag. Reverting
the commit restores the previous module layout without changing persisted
data. No public caller is migrated in this change.
