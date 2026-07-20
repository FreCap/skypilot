# Managed jobs shared API token lifetime

Status: accepted after adversarial review

## Problem

`sky jobs launch --num-jobs` serializes one task definition for every job in
the submission, so jobs that request API-server access intentionally share one
service-account token. The jobs database records that token against every job
ID. Controller cleanup currently revokes the token as soon as any one job
finishes, including while cancellation is still in its non-terminal cleanup
phase. A fast or cancelled job can therefore invalidate API access for sibling
jobs that are still running.

The baseline cleanup path performs one token-association lookup and then
unconditionally deletes the returned token. It does not inspect the other jobs
that share the association.

## Goal and invariant

A managed-job API token is revocable only after every task of every job mapped
to that token is terminal. The finishing job must itself be terminal before the
decision is made. Missing associations are no-ops, and concurrent last-job
finalizers may attempt the idempotent token deletion without corrupting state.

The eligibility decision must use one bounded SQL statement, with no polling or
per-job query loop.

## Design

Add a state helper that starts from the finishing job's token association and
returns its token ID only when no task outer-joined through any sibling
association has a null or non-terminal status. The outer join makes a malformed
association with no task row fail closed. Implement the predicate as one
correlated `NOT EXISTS` query. The existing primary-key lookup on the finishing
`job_id` anchors the query, and a new `api_access_tokens.token_id` index bounds
the sibling lookup to jobs sharing that token. Ship the index as the next spot
jobs Alembic migration so existing databases receive the same query plan as new
installations. Build it concurrently on PostgreSQL so the upgrade does not
block managed-job token writes. Remove the superseded one-job token lookup so
there is only one cleanup decision path.

Move token cleanup out of `ControllerManager._cleanup()`, whose cancellation
path runs while the job is `CANCELLING`. After `run_job_loop()` has converted
cancellation to `CANCELLED` and repaired any other non-terminal exit to
`FAILED_CONTROLLER`, call a small best-effort token cleanup helper. Delete the
global token only when the state helper returns an eligible ID. Preserve the
existing behavior of logging and swallowing cleanup failures so token cleanup
cannot overwrite the job's terminal result; the expiration sweep remains the
eventual fallback.

No token format, launch behavior, or public API changes are required. The only
schema change is the non-unique reverse-lookup index.

## Alternatives considered

Creating one token per batch job would require rank-specific serialized task
definitions and materially expand the launch protocol. Leaving all tokens to
the three-day expiration sweep avoids early revocation but unnecessarily keeps
credentials live after a batch finishes. A read followed by one status query
per sibling is simpler to sketch but adds linear database round trips.

## Changed-path-to-test matrix

- `sky/jobs/state.py` and `sky/jobs/state_schema.py` map to
  `tests/unit_tests/test_sky/jobs/test_state.py`: missing association,
  single-job terminality, active sibling deferral, all-terminal eligibility,
  null-status fail-closed behavior, and exactly one SQL statement per decision.
- `sky/schemas/db/spot_jobs/024_add_api_access_token_index.py` and
  `sky/utils/db/migration_utils.py` map to
  `tests/unit_tests/test_batch_recovery.py`: upgrading an existing revision-23
  database creates the reverse-lookup index and the jobs database targets
  revision 24; PostgreSQL uses an online concurrent index build.
- `sky/jobs/controller.py` maps to
  `tests/unit_tests/test_sky/jobs/test_controller.py`: deletion only after the
  final terminal transition, shared-token deferral, idempotent deletion result,
  and best-effort failure isolation.

## Validation and performance evidence

Run both focused test files, the managed-jobs unit component, formatting and
static checks for the changed Python files, and `git diff --check`. The state
tests count SQL statements and require exactly one statement for missing,
deferred, and eligible decisions, and inspect the initialized schema for the
token index. The controller tests require zero global token deletes while a
sibling is active and one attempt after all jobs are terminal. CI must run
these files for the changed paths before merge.

## Rollout

Merge only after the exact PR head passes every visible relevant check and has
no actionable review thread. The existing token-expiration daemon is the
rollback-safe fallback if terminal cleanup encounters a database or token-store
failure.
