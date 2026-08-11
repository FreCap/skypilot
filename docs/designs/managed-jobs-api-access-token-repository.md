# Managed Jobs API Access Token Repository

## Status

Accepted for a bounded structural extraction from `sky.jobs.state`.

## Context

`sky/jobs/state.py` is a stable public persistence facade for managed jobs. At
3,458 lines it owns scheduler transitions, controller recovery, job and task
status, accounting, log metadata, and a small API access-token ownership
protocol. The token protocol is used by two materially different callers:

- `sky.jobs.server.core` associates one newly issued API token with every job
  in a submitted batch and revokes the token if persistence fails.
- `sky.jobs.controller` asks whether a completed job's token can be revoked,
  which is safe only when every job sharing that token has a terminal task.

The protocol has its own table, reverse-lookup index, dialect-specific upsert,
batching limit, transaction boundary, shared-token lifetime rule, and retry
behavior. It does not own token issuance or revocation, job lifecycle
transitions, scheduler policy, controller cleanup ordering, or schema
migrations.

## Responsibility map before extraction

| Responsibility | Callers | Dependencies and state | Failure modes | Sensitivity and cadence |
| --- | --- | --- | --- | --- |
| Batch token association | Managed-job submission | SQLite/PostgreSQL upsert, `api_access_tokens`, one transaction, 1,000-row chunks | Partial batch commit, lost duplicate ordering, unsupported dialect, replacement not persisted | Query/transaction-count sensitive; changes with submission and database compatibility |
| Shared-token release proof | Controller terminal cleanup | `api_access_tokens`, task status rows, terminal status set, retry decorator | Premature revocation, missing task treated as terminal, sibling job omitted, transient read not retried | One-query cleanup path; changes with token lifetime and terminal-state semantics |
| Token issuance and revocation | API server and global user state | Token service, credential store, failure compensation | Leaked or prematurely revoked credential | Security and API policy; remains outside this repository |
| Managed-job lifecycle and scheduling | Scheduler, controllers, APIs, Skylet | Job/task rows, ownership fences, async transactions, callbacks | Lost transitions, stale-owner writes, recovery races | High-state and latency sensitive; remains in `sky.jobs.state` |

The first two responsibilities form one repository seam because they jointly
define durable token ownership and its release condition. Their callers and
failure modes differ from the lifecycle facade, while their schema and reason
to change are shared with each other.

## Decision

Extract the two repository operations into
`sky.jobs.state_api_access_tokens`, using plain module-level functions. Keep
`sky.jobs.state.set_api_access_token_ids` and
`sky.jobs.state.get_releasable_api_access_token_id` as direct aliases so the
public import path, function identity, pickle lookup, signatures, retry
wrapper, and call overhead remain stable.

The extracted module owns:

- ordered de-duplication and bounded dialect-specific batch upserts;
- the single-transaction commit boundary for the whole association batch;
- the one-query, fail-closed proof that every token sibling is terminal; and
- the existing read retry decorator.

The facade retains all other managed-job persistence and exports the schema
table through its historical import path.

## Alternatives

- **Keep the functions in `state.py`:** simplest by file count, but leaves an
  independently evolving security credential protocol mixed into the
  scheduler and lifecycle facade. The repository has two distinct external
  callers and a complete schema-level contract, so the ownership gain is
  larger than a single helper extraction.
- **Move only the upsert or only the release query:** rejected because it would
  split one token-ownership protocol across modules.
- **Move token issuance and revocation too:** rejected because those operations
  belong to the API credential service and global user state, not managed-job
  persistence.
- **Introduce a repository class, protocol, adapter, or dependency injection:**
  rejected because there is one database implementation and no varying policy
  or construction lifecycle.
- **Wrap facade calls:** rejected because direct aliases preserve identity and
  avoid an extra hot-path frame.

## Behavior contract

- Preserve input order while de-duplicating job IDs.
- Empty input performs no engine lookup, query, or transaction.
- SQLite and PostgreSQL use native conflict-update syntax.
- Every chunk participates in one transaction and any failure rolls back the
  full batch.
- Re-association replaces the token for the selected job IDs only.
- Release returns a token only when the owner exists and no associated job has
  a missing or nonterminal task row.
- Release remains one SQL statement and retries transient database failures.
- Public names, signatures, module identity, pickle lookup, table export, and
  caller behavior remain unchanged.

## Implementation and rollout

1. Add and run characterization coverage against the current facade.
2. Move the complete repository implementation and batch constant.
3. Install direct aliases in `sky.jobs.state` and preserve historical module
   identity on both the retry wrapper and wrapped function.
4. Run focused state, submission, controller, schema, migration, and sync/async
   parity tests plus import, identity, SQL-budget, formatting, typing, and lint
   gates.
5. Compare import time and representative SQLite write/read timings against the
   exact base SHA. Roll back the extraction if it materially regresses them.

## Changed-path-to-test matrix

| Changed path | Contract | Verification |
| --- | --- | --- |
| `sky/jobs/state_api_access_tokens.py` | Association and release repository | Token characterization cases, SQL counts, rollback, dialect compilation, retry, timing |
| `sky/jobs/state.py` | Stable facade and historical identities | Signature/module/pickle tests, import-order probes, full state suite |
| `tests/unit_tests/test_sky/jobs/test_state.py` | Boundary characterization | The file itself and Unit Tests CI collection |
| This design | Canonical contract and rollout | Formatting, docs CI, and diff checks |

## CI mapping

The pull-request workflows on `improvements` have no exclusion for these
paths. `Python Tests - Unit Tests` collects the state, server-core, controller,
schema, and migration tests. `Python Tests - Jobs & API Tests` collects the
sync/async state parity suite. Limited-dependency imports, format, mypy,
Pylint, Ruff, BasedPyright, import-linter, docs, and dashboard checks provide
the remaining repository-wide gates.
