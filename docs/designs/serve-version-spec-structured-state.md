# Structured SkyServe Version Specification State

Status: Phase 0 design. No runtime or schema change has shipped.

Last updated: 2026-08-02.

## Problem

SkyServe stores every committed `SkyServiceSpec` in
`version_specs.spec` as a Python pickle. PostgreSQL is the production store,
but a current API server must import the historical Python object graph and
execute `pickle.loads()` before a controller, autoscaler, replica manager, or
version-history reader can use the row.

This format has three concrete risks:

- Rolling compatibility is manual. `SkyServiceSpec.__setstate__()` already
  supplies defaults for fields added after older rows were written, including
  logical-replica, downscale, high-availability, drain, and capacity policies.
  Every future field must preserve old semantics in both the constructor and
  the pickle compatibility shim.
- Schema evolution and operations are opaque. PostgreSQL cannot validate,
  inspect, compare, or safely migrate the resolved specification without
  importing application code and unpickling the row.
- Pickle deserialization can execute code. The database is access-controlled,
  so this is not treated as an untrusted request-input exploit, but a corrupted
  or incorrectly restored row crosses an executable deserialization boundary
  inside the API server.

The existing `yaml_content` column is not a safe replacement. Re-parsing an old
YAML document applies the current parser, schema, and defaults. That can change
the behavior of an immutable historical version. Examples include the explicit
legacy defaults in `__setstate__()` for physical replica semantics and
unbounded downscale.

Doing nothing keeps the smallest code surface, but it also makes every new
service-spec field another rolling-deploy compatibility obligation. The risk is
already active rather than hypothetical, so a bounded structured-state
migration is justified.

## Scope

This design covers only the resolved `SkyServiceSpec` value stored for one
`(service_name, version)` row.

It does not:

- make the entire Serve database PostgreSQL-only;
- change version allocation, immutable YAML, election, quarantine, provenance,
  placement-catalog, or service-lifecycle behavior;
- migrate replica pickle state, which already has a separate versioned JSON
  contract;
- change the public service YAML schema or SDK representation; or
- delete the existing `yaml_content` or `submitted_yaml_content` columns.

SQLite remains a supported local Serve-state backend during the migration.
The structured representation is dialect-neutral application data, while all
production validation and migration gates run against real PostgreSQL.

## Current Ownership And Callers

`sky/serve/serve_state.py` owns the `version_specs` table and all durable
version-spec reads and writes:

- `add_service()` atomically inserts the service and its committed initial
  version, including the pickled spec;
- `add_version()` inserts a placeholder containing `pickle.dumps(None)`;
- `add_or_update_version()` commits the immutable YAML and pickled spec in the
  same transaction;
- `get_spec()` and `get_specs()` serve controllers, autoscalers, replica
  managers, Serve utilities, and update paths;
- service-mode recovery, version history, latest committed, latest applicable,
  and quarantine-aware controller recovery readers deserialize the same blob;
  and
- service deletion removes the version rows with the rest of the service.

`sky/serve/service_spec.py` owns construction, YAML parsing and rendering,
copying, validation, and the historical pickle `__setstate__()` compatibility
contract.

The data owner is the API-server Serve database. Rows are immutable after the
placeholder-to-committed transition, except for separate additive metadata
such as quarantine and placement catalog.

## Behavior Contract

- A committed version reconstructs the same effective `SkyServiceSpec` before
  and after migration.
- Historical compatibility values remain explicit. A structured decode must
  not silently apply current constructor or YAML defaults.
- Placeholder versions remain distinguishable from committed versions.
- Version allocation, content-conflict, stale-version, logical-replica, HA,
  lifecycle-owner, and election fences remain in the existing transaction.
- A committed YAML row and its structured state are immutable.
- Reads remain one SQL statement for one version and one bounded statement for
  a batch of versions.
- Unknown state versions and malformed structured values fail closed. They may
  use the legacy fallback only during the explicitly observed compatibility
  phase.
- Rolling deployments and rollback work at every phase described below.
- No marker logs a service name, YAML, state payload, database URL, SQL
  parameter, credential, or user identity.

## Structured Format

Add a versioned application serialization contract to `SkyServiceSpec`:

```python
def to_storage_dict(self) -> dict[str, Any]:
    ...

@classmethod
def from_storage_dict(
    cls, state: dict[str, Any]
) -> 'SkyServiceSpec':
    ...
```

The format contains only JSON scalars, lists, and objects. Version 1 records
the resolved constructor inputs and compatibility-only state needed to
reconstruct exact behavior. It does not serialize `__dict__` generically.

Important rules:

- Enums and policy objects use stable string identifiers.
- TLS state stores the existing credential path fields, not file contents.
- Integer, float, boolean, nullable, mapping, and list fields validate their
  exact shapes before object construction.
- Compatibility-only fields such as whether logical-replica semantics were
  active are explicit even when they are not expressible in current YAML.
- Decode uses a storage-specific constructor path. It does not call
  `from_yaml_config()` and therefore does not reapply current YAML defaults.
- Unknown keys are rejected for a known state version. A new binary can add a
  new format version with an explicit decoder and upgrade path.
- The parity comparator uses normalized storage dictionaries, not object
  identity or pickled bytes.

This is intentionally similar to `ReplicaInfo.to_storage_dict()` and
`from_storage_dict()`, but it remains a separate contract because service
specifications and replica runtime state have different lifecycles.

## Schema

Add two nullable columns to `version_specs` through the next available Serve
migration revision:

- `spec_state_version INTEGER`
- `spec_state JSON` with PostgreSQL `JSONB`

Both columns are null for placeholders and historical rows until a committed
structured value is written. A check constraint requires both columns to be
null or both non-null. The existing `(service_name, version)` primary key
continues to own lookup and ordering, so no new index is needed.

The migration is additive and idempotent under the repository migration
helpers. It performs no pickle deserialization in Alembic. Data conversion is
owned by application code so it can use the exact decoder, parity checks, and
bounded transactions shipped in the same phase.

The legacy `spec` column remains untouched until the final contract phase.

## Migration Phases

### Phase 0: Baseline And Design

- Land this canonical design with the threat evidence, caller map, phase
  boundaries, rollback contract, and changed-path-to-test matrix.
- Characterize current and historical `SkyServiceSpec` pickle behavior before
  changing serialization.
- Inventory the exact state fields and types. Every constructor, copy, YAML,
  and `__setstate__()` field must map to the versioned contract or be
  deliberately derived.

No schema or runtime behavior changes in this phase.

### Phase 1: Additive Schema, Dual Write, And Backfill

- Add the two nullable columns and the version-pair constraint.
- Add version 1 structured encode/decode with exhaustive validation.
- Dual-write pickle and structured state in the existing immutable version
  commit transaction.
- Keep pickle authoritative for reads.
- After every new write, decode both representations and require normalized
  parity before commit.
- Backfill historical committed rows in primary-key order with bounded,
  restart-safe batches. Lock or condition each update on structured state
  still being null and never modify YAML or pickle bytes.
- Record backfill totals, invalid rows, parity failures, duration, batch size,
  and last processed key without logging row contents.

The backfill is idempotent. A retry skips rows with a supported structured
version after rechecking parity. Any decode, validation, or parity failure
stops advancement and leaves the pickle-authoritative deployment usable.

Bulk backfill necessarily deserializes dormant rows that ordinary traffic may
not currently read. It therefore runs as an explicit, short-lived
application-version-matched job, not during Alembic or ordinary API startup.
The job has database-only network access, no cloud or provider credentials, a
database role limited to reading `version_specs` and conditionally updating
the two structured columns, a fixed batch and wall-clock budget, and
operator-visible progress. These controls do not make pickle safe, but they
bound the one unavoidable conversion step and keep a malformed row from
silently entering the structured store. The implementation phase must either
prove this execution boundary exists in the deployment stack or stop before
backfill.

Rollback to the pre-Phase-1 binary is safe because every committed version
still contains the original pickle.

### Phase 2: Structured Authority And Legacy Marker

- Make structured state authoritative when its version is supported.
- Retain pickle fallback only for a missing, unsupported, malformed, or
  parity-failed structured value.
- Emit at most one structured legacy marker per process for each bounded
  `(operation, reason)` pair when a fallback is actually used.
- Continue dual-writing pickle for rolling deployment and rollback.
- Count all version-spec reads by structured or legacy backend and operation.
- Provide a parity audit that scans all committed rows without rewriting them.

The stable marker is:

```text
event_name=skypilot.persistence.legacy_format_used
component=serve_version_spec
operation=get|get_batch|get_latest|get_history|backfill
phase=read|migration
format=pickle
reason=missing|unsupported_version|malformed|parity_failed
server_version=<bounded version>
server_commit=<bounded commit>
```

The Datadog log query is:

```text
"event_name=skypilot.persistence.legacy_format_used" \
"component=serve_version_spec"
```

The traffic proxy is:

```text
sum:sky_persistence_operations_total{
  component:serve_version_spec
}.as_count()
```

The observation clock starts only after git ancestry proves the Phase 2 merge
is live, the backfill and full parity audit succeed, and the metrics and log
queries cover every relevant API-server replica.

### Phase 3: Observation

Observe at least ten full calendar days after proven deployment. Require:

- zero legacy markers over the complete interval and all replicas;
- representative nonzero structured reads, including controller recovery,
  autoscaler, replica-manager, batch, latest-applicable, and history paths;
- zero parity-audit failures and no null structured state on committed rows;
- restart, controller replacement, rolling upgrade, rollback, concurrency, and
  failure-recovery evidence; and
- no active configuration, caller, compatibility contract, or open PR that
  requires pickle.

Quiet or unexercised paths extend the observation window.

### Phase 4A: Detach Runtime From The Legacy Column

After every Phase 3 gate passes:

- remove pickle reads and fallback markers;
- stop dual-writing pickle;
- require structured state for committed versions;
- delete `SkyServiceSpec.__setstate__()` only after no remaining supported
  persistence or transport path needs historical pickles;
- remove pickle-specific tests and dependencies that have no other owner; and
- remove `version_specs.spec` from every runtime select, insert, update, table
  projection, schema fingerprint, and current-binary migration check while
  leaving the physical database column in place.

This rolling deployment is safe because both the Phase 2 and Phase 4A binaries
read structured state authoritatively. Rollback to Phase 2 is still possible
while the physical pickle column remains.

### Phase 4B: Drop The Legacy Column

After Phase 4A is proven live on every replica and has completed its own
stability observation:

- verify no live or rollback-target binary references `version_specs.spec`;
- drop the physical column in a separate contract migration; and
- deploy only binaries whose schema fingerprint and SQL projections omit it.

The drop migration can roll back only to the Phase 4A binary. It cannot safely
roll back to Phase 2 because Phase 2 still dual-writes the physical column.
Rollback to a pre-Phase-1 binary is unsupported after the drop.

## Failure And Concurrency Handling

- Initial service registration writes the service row, committed YAML, pickle,
  state version, and structured state in one transaction.
- The placeholder-to-committed update path writes YAML, pickle, state version,
  and structured state in one existing service-locked transaction.
- A partial structured backfill never changes the authoritative pickle.
- Concurrent backfill workers use deterministic bounded key ranges and
  conditional updates. At most one writer fills a row.
- An identical version retry preserves all immutable bytes and state. It may
  fill missing structured state only after matching the committed YAML and
  proving parity with the existing pickle.
- Unsupported versions fail closed rather than being treated as current.
- Corrupt structured state cannot silently fall through after Phase 4.
- A backfill or audit process must be bounded in memory and statements and must
  not hold locks while scanning the whole table.

## Performance Contract

- Single-version reads remain one indexed primary-key statement.
- Batch reads remain one bounded `IN` statement.
- Structured decode must not be materially slower than current pickle decode at
  representative specification sizes.
- Dual-write adds one JSON value to the existing insert or update rather than a
  second transaction.
- Backfill uses bounded batches, commits between batches, and records statement
  count, lock duration, rows per second, payload-size distribution, and WAL
  growth on a production-shaped PostgreSQL dataset.
- `EXPLAIN (ANALYZE, BUFFERS)` must continue to select the existing primary key
  for single and batch lookups.

## Changed-Path-To-Test Matrix

| Changed path | Required proof |
| --- | --- |
| `docs/designs/serve-version-spec-structured-state.md` | Exact-code adversarial review and design consistency |
| `sky/serve/service_spec.py` | Versioned encode/decode, type validation, all fields mapped, historical defaults, YAML/copy/pickle parity |
| `sky/serve/serve_state.py` | Atomic initial-registration and placeholder-commit dual-write, authoritative read, fallback marker, traffic labels, service-mode/batch/latest/applicable/recovery/history paths, immutable retry, quarantine, concurrency |
| `sky/schemas/db/serve_state/<revision>.py` | Fresh and repeated upgrade, pair constraint, rollback/forward compatibility, interrupted migration recovery |
| backfill entrypoint and deployment wrapper | Exact binary match, database-only access, scoped role, bounded batches/time, resume and failure evidence |
| focused Serve PostgreSQL test file | Real `postgres:16` backfill, parity, restart, rolling-version, concurrency, query plan, statement count, timing |
| existing Serve state/controller/autoscaler/replica-manager tests | No behavior change for callers and recovery |
| Python CI workflows | The changed paths select mandatory real-PostgreSQL, Jobs and API, unit, migration, format, type, and lint coverage |

## Test Plan

- Characterize representative current and historical pickles before edits.
- Prove every effective field survives pickle to state to object to state
  round trips, including legacy missing-field semantics.
- Reject unknown versions, unknown keys, wrong scalar types, non-finite
  numbers, malformed nested mappings, and invalid policy identifiers.
- Prove atomic initial registration, placeholder, committed, identical-retry,
  conflict, stale-version, lifecycle-owner, election, logical-replica, HA,
  quarantine-aware recovery, service-mode recovery, and deletion behavior.
- Run fresh, repeated, interrupted, and mixed-version migrations against a
  real local `postgres:16`.
- Prove the backfill refuses an unexpected schema or binary version, resumes
  from a bounded checkpoint, cannot mutate YAML or pickle bytes, and stops on
  the first malformed or non-parity row.
- Race version commit with backfill and two backfill workers.
- Restart independent engines and reconstruct all committed versions.
- Simulate old writer plus new reader and new dual writer plus old reader.
- Prove marker privacy and once-per-process-per-operation/reason emission only
  on actual fallback.
- Prove nonzero structured traffic metrics without a legacy marker.
- Compare primary-key plans, statement counts, decode timing, commit timing,
  lock duration, and batch throughput before and after.
- Run focused Serve state, service-spec, controller, autoscaler, replica
  manager, server, migration, and real-PostgreSQL suites.
- Run `bash format.sh --files` for changed Python files, `git diff --check`,
  relevant mypy and pylint, and the complete visible CI rollup on the exact
  pushed head.

## Rollout And Manual Verification

1. Deploy Phase 1 with pickle reads authoritative. Verify migration revision,
   dual-write parity, bounded backfill completion, row counts, and no API or
   controller behavior change.
2. Restart every API-server replica and at least one controller. Verify every
   committed version reconstructs with parity.
3. Deploy Phase 2. Prove the merge SHA is an ancestor of the live commit and
   that structured reads are nonzero across all replicas.
4. Query the stable fallback marker and traffic counter over the complete
   interval. Exercise version history, service update, controller recovery,
   autoscaling, and replica replacement.
5. After the full observation gates pass, deploy Phase 4A. Verify no runtime
   SQL, schema fingerprint, or migration check references the legacy column,
   while the untouched physical column still permits rollback to Phase 2.
6. After Phase 4A is stable on every replica, merge and deploy Phase 4B.
   Confirm rollback is limited to Phase 4A and no supported binary expects the
   dropped column.

## Alternatives

### Keep Pickle

This has no immediate migration cost, but preserves executable
deserialization, opaque rows, and the manual compatibility shim that already
grows with service-spec features.

### Reparse `yaml_content`

This is smaller but changes historical semantics when parser defaults or
schemas evolve. It cannot represent compatibility-only fields and is rejected.

### Store `to_yaml_config()` As JSON

This is queryable, but the public YAML representation intentionally omits or
normalizes internal compatibility state. Reconstructing through the current
YAML parser has the same default-drift problem as reparsing `yaml_content`.

### Generic `__dict__` JSON

This avoids a field map initially but leaks implementation names into the
durable schema, accepts accidental fields, and makes future validation and
migrations harder. An explicit versioned contract has higher initial cost and
lower long-term ambiguity.

### Immediate One-Release Conversion

Backfilling and removing pickle in one release breaks rolling rollback and
provides no production parity or fallback observation. It is rejected.
