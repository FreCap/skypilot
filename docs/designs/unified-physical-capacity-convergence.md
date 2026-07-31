# Unified Physical-Capacity Convergence

Status: C0 accepted; revision-001/C1 implemented, locally verified, and
deployed to the isolated test release in capacity mode `disabled`; C2 and all
later phases remain gated; no mutation authority is enabled

Last updated: 2026-07-31

Canonical owner: this file. External plans, pull requests, and rollout notes
must link here rather than restating a divergent contract.

## Summary

SkyPilot will add a PostgreSQL-only physical-capacity control plane beneath
SkyServe services, SkyServe pools, and dedicated managed jobs. Workload
controllers continue to decide desired size, placement, health, rollout,
drain, and recovery policy. The shared layer records:

- immutable physical-allocation identity;
- exact lifecycle ownership and fencing generations;
- desired and provider-observed state;
- provider-member identity and observation freshness;
- durable resource actions, typed outcomes, retries, and deletion proof; and
- pool-job occupancy separately from physical ownership.

The migration starts as a persistent read-only projection. Legacy controllers
remain authoritative while shadow reconciliation proves identity and state
parity. Mutation authority then moves one owner kind and action kind at a time:
explicit teardown retries, Serve and pool-worker launch/replacement, and
finally dedicated managed-job clusters. There is never more than one
authoritative actuator for a given allocation and action.

This design extends the PostgreSQL request, claim, lease, controller-generation,
and action-reservation foundations merged in PR #1070. It does not introduce a
second request journal or a second Serve/pool replica manager. PR #1071's
provider-confirmed ownerless-cluster refresh remains the rollback-compatible
fallback until the final removal gate.

## Motivation

The current physical resource is reconstructed from workload-specific rows:

- `clusters` owns the SkyPilot cluster generation and serialized handle;
- `services` and `replicas` own Serve and pool lifecycle state;
- `spot_job` and `job_info` own managed-job lifecycle and pool assignment;
- API request action reservations fence a subset of controller mutations; and
- cloud provisioners expose provider state through provider-specific handles
  and status calls.

This decomposition is intentional at the policy layer, but there is no durable
record that answers all of these questions together:

1. Which exact physical allocation generation should exist?
2. Which exact workload generation currently owns it?
3. Which provider members have been observed, and how fresh is that evidence?
4. Which external mutation is authorized or ambiguous?
5. Has deletion been proved by the provider, rather than inferred from a
   missing controller or cluster row?

That gap causes independent retry state, repeated provider observation,
inconsistent capacity accounting, weak orphan evidence, and difficult
whole-group replacement recovery. The convergence layer makes the physical
facts common without merging the workload schedulers.

## Value hypothesis and C2 go/no-go gate

The uploaded research establishes architectural breadth, not production ROI.
The read-only phase therefore measures the hypothesis before SkyPilot accepts
the mutation blast radius.

Before C2 starts, the baseline records for at least 30 days:

- orphaned and uncertain-delete accelerator-hours and estimated cost;
- physical-lifecycle incidents, manual repair hours, MTTD, and MTTR;
- ambiguous or duplicated provider mutations;
- provider observation calls and duplicate reads by owner kind;
- pool placement contention, assignment retries, and over-allocation; and
- physical retry/action implementations and defects duplicated across owners.

C4 is authorized only if C2 proves that at least two owner kinds share a useful
identity, observation, or recovery mechanism and at least one of these
materiality thresholds is met:

- avoidable physical leakage is at least 100 accelerator-hours or $5,000 per
  month, or at least one percent of managed compute spend;
- shared lifecycle failures cause at least two operator incidents or four
  engineer-hours of repair per month;
- a common observation pass removes at least 25 percent of provider reads
  without violating freshness SLOs; or
- PostgreSQL occupancy eliminates a reproduced pool assignment race or
  over-allocation that the legacy lock cannot prevent.

If those conditions are not met, the project stops after the read projection
and the narrower durable Serve-action work. Projection parity by itself is not
evidence of a large payoff. The thresholds may be changed only by updating and
re-reviewing this document before C2 data is examined.

## Goals

1. Give every managed physical allocation a stable identity created before its
   first external mutation and retained through provider-confirmed deletion.
2. Separate desired state from provider-observed state and record observation
   source, freshness, and certainty.
3. Fence every action to the stable owner incarnation, current workload-writer
   fence, desire transition, authority epoch, and action lease.
4. Make ambiguous actions restart-safe and reconcilable without blindly
   replaying provider mutations.
5. Preserve multi-node group topology and safe replacement ordering.
6. Represent pool jobs as resource occupancies, never as owners of shared
   workers.
7. Let Serve, pool, job, dashboard, status, and repair paths read one physical
   inventory after parity is proved.
8. Make each authority cutover independently deployable and revertible.
9. Leave an exact, evidence-gated ledger of legacy code and schema that must be
   deleted after migration.

## Non-goals

- A unified autoscaler, scheduler, or placement policy.
- Cross-workload reuse of dedicated Serve or job allocations.
- Moving HTTP readiness, load-balancer routing, logical replica slots,
  application drain proof, managed-job status, or recovery policy into the
  capacity reconciler.
- Treating ambient cloud availability or catalog capacity as provisioned
  physical inventory.
- Replacing `SkyPilotReplicaManager`, the managed-job scheduler, the API
  request queue, or existing provider implementations.
- Supporting the new central tables on SQLite. Local and non-consolidated
  controller databases retain their existing supported behavior during the
  migration.
- Automatically adopting or deleting a provider resource based only on a
  matching name or shape.
- Exactly-once provider side effects across an unknowable network partition.
  Ambiguous effects remain durable and require provider reconciliation.

## Terminology and public contract

This is initially an internal server contract. It adds no public SDK, CLI, or
YAML field. A future read API requires its own API-version review.

- **Capacity group:** one workload-owned desired topology and placement
  contract. A heterogeneous group can contain several exact physical specs.
- **Capacity allocation:** one SkyPilot cluster generation. It may contain
  multiple provider members.
- **Capacity member:** one provider VM, pod, TPU, or equivalent member.
- **Owner incarnation:** durable source identity that changes only when the
  workload is recreated: service/pool hash or managed-job task-row identity.
- **Writer fence:** mutable authority for the controller currently allowed to
  publish or release intent: Serve controller-owner fingerprint plus lifecycle
  epoch, or controller instance UUID and generation. It can advance without
  changing the owner incarnation.
- **Intent generation:** monotonically increasing desired physical
  configuration within one owner incarnation.
- **Desire transition:** stable per-allocation desired-state identity carried
  through benign later intent generations.
- **Action attempt:** one leased execution or reconciliation attempt for a
  stable logical resource action.
- **Occupancy:** a pool job's resource claim on a pool-owned allocation.
- **Deletion proof:** a typed provider observation that the exact allocation
  identity is absent. Absence of a SkyPilot database row is not deletion proof.

No field named simply `generation`, `epoch`, or `hash` is permitted in the
shared schema or API when more than one concept is possible. Owner incarnation,
writer-fence epoch, intent generation, authority epoch, action attempt, and
occupancy assignment generation remain explicit.

## Architecture

```text
Serve autoscaler        Managed-job recovery        Pool scaler/placer
health and rollout      task/retry policy           worker/occupancy policy
          \                    |                         /
                    desired allocation intents
                               |
               PostgreSQL physical-capacity core
         groups | allocations | members | occupancies
                  observations | durable actions
                               |
            existing SDK/core/provisioning actuators
                               |
          provider observations, outcomes, and proofs
```

### Responsibility boundary

Workload controllers exclusively author desired intent and owner-specific
release gates. Provider observers exclusively author observed physical facts.
The shared reconciler leases and executes an authorized intent, but it cannot
invent desired capacity, application readiness, or rollout safety.

`SkyPilotReplicaManager` remains the Serve and pool policy engine throughout
the migration. It first projects intent, then delegates physical action
execution. The managed-job controller retains task-state and recovery policy
while delegating dedicated cluster lifecycle. Pool placement retains resource
selection policy while committing occupancy through the shared ledger.

### Authority and writer matrix

| State | Sole writer | Required fence |
| --- | --- | --- |
| Group owner and immutable intent | Current workload controller adapter | Workspace, owner incarnation, current writer fence, next intent generation |
| Allocation desire/binding | Current workload controller adapter | Current immutable intent, owner incarnation, and writer fence |
| Per-verb actuation authority | Authority handoff transaction | Current authority epoch and stable/quiesced action set |
| Logical action | Capacity reconciler planner | Current desire transition, writer fence, and authority epoch |
| Action attempt | Capacity action worker | Exact action, permit, attempt, lease, request, controller instance/generation, writer fence, and authority epoch |
| Observation run and members | Provider observer | Exact provider scope and observer fence |
| Serve readiness/release gate | `SkyPilotReplicaManager` | Service incarnation, lifecycle epoch, version, and controller owner |
| Managed-job recovery/terminal evidence | Managed-job controller | Job/task, controller generation, and recovery generation |
| Pool occupancy | Pool placement/release transaction | Pool owner, job attempt, allocation, and occupancy lease |

Projection adapters may copy legacy state but cannot synthesize authority.
Dashboard, status, cost, and repair readers never write capacity state.

## PostgreSQL data model

The tables live in a new `capacity_state_db` Alembic lineage in the same
PostgreSQL database and schema as global, Serve, jobs, config, and request
state. Capacity shares the ordinary default SQLAlchemy engine namespace with
global, Serve, jobs, and config; API request state retains its existing
`api-requests-control` namespace. The lineage has its own version table and
migration ownership but does not create another database or connection pool.
C1 and C2 must not pass `engine_namespace`.

The runtime metadata and literal reviewed Alembic migration remain independent.
The first file map is:

- `sky/utils/db/migration_utils.py`:
  `CAPACITY_STATE_DB_NAME = 'capacity_state_db'`,
  `CAPACITY_STATE_VERSION = '001'`, and its lock path;
- `sky/setup_files/alembic.ini`: `[capacity_state_db]`,
  `version_locations = .../schemas/db/capacity_state`, and
  `version_table = alembic_version_capacity_state_db`;
- `sky/schemas/db/capacity_state/001_initial_schema.py`: literal migration;
- `sky/physical_capacity/schema.py`: independent runtime metadata; and
- `sky/physical_capacity/state.py`: lazy `DatabaseManager` and transactions,
  with no database work at import time.

`initialize_schema()` rejects a non-PostgreSQL dialect before invoking Alembic
and explicitly forwards `auto`, `upgrade`, `bootstrap`, or `verify`. `auto` is
permitted only in the existing single-process/Recreate compatibility topology;
multi-role HA uses the blocking migration Job plus verify-only roles, and no
capacity authority may activate in `auto`. The migration also rejects
non-PostgreSQL rather than stamping a revision without tables.
`database_migrations.initialize_central_databases()` initializes global state
first, retains that engine, initializes existing stores, and initializes
capacity last only when the actual global dialect is PostgreSQL. A local
SQLite server skips the disabled capacity store; requesting any non-disabled
capacity mode on SQLite fails with an unsupported-backend error.

`sky/physical_capacity/config.py` parses
`SKYPILOT_PHYSICAL_CAPACITY_MODE`, default `disabled`, before database
initialization. Closed values are `disabled`, `shadow`, `observe`, `teardown`,
`serve`, and `jobs`. An optional bounded
`SKYPILOT_PHYSICAL_CAPACITY_ALLOWLIST_JSON` contains provider, workspace,
owner-kind, group, and verb allowlists. Unknown fields or values fail startup.
This is server-admin deployment configuration, not a user workspace setting.
The parser recognizes future modes so mixed-version config fails clearly, but
the runtime also checks schema/binary capabilities: revision `001` permits only
`disabled` or `shadow`; `observe` requires accepted revision `002`;
`teardown`/`serve`/`jobs` require their accepted mutation revisions and
explicit authority support. A mode cannot activate a table or behavior absent
from the running binary.

The blocking Helm migration Job owns ordered HA upgrades. API, executor, and
controller roles run verify-only and all verify the capacity revision. The
separate per-lineage distributed locks do not make cross-lineage migrations
atomic, so capacity authority cannot activate in a multi-replica `auto`
migration topology. Capacity migrations have no cross-lineage foreign keys. A
future migration that needs a minimum Serve/jobs/global revision must preflight
it explicitly before DDL.

### Revision staging and implementation authority

The target model below is deliberately split so the read-only experiment does
not prematurely freeze mutation tables:

| Revision | Tables introduced or expanded | First phase that may use them |
| --- | --- | --- |
| `001` | Projection scans, groups, immutable intents/desires, and allocations | C1-C2 |
| `002` | Observation runs, provider members, allocation deletion-proof columns, and retained tombstone proof | C3 |
| `003` | Stable desire transitions, workload readiness/drain evidence, per-verb authority, actions, append-only attempts, workload-policy permits, and the atomic API-request submission/claim seam | C4 and C6 |
| `004` | Authoritative occupancy, lease, resource, and terminal-evidence tables/constraints | C5 |

Only revision `001` is implementation-authorized by C0. Before implementing
each later revision, this file must replace the corresponding target-model
description with literal columns, defaults, checks, indexes, foreign keys,
lock order, and migration compatibility; adversarial review must accept that
exact revision. Runtime metadata must then match the accepted literal
migration. No code may opportunistically create a later table from shared
metadata.

### Literal revision `001` contract

All timestamps are `TIMESTAMPTZ NOT NULL` and use
`server_default=clock_timestamp()` unless explicitly nullable. UUIDs are
application-generated PostgreSQL `UUID`; revision `001` does not install a
database extension. Durable ownership foreign keys use `ON DELETE RESTRICT`.
The intentional group/current-intent cycle alone uses
`DEFERRABLE INITIALLY DEFERRED ON DELETE NO ACTION`, so a future proof-aware GC
can delete the final group and its intents in one transaction after all
RESTRICT children are gone. Only non-authoritative last-scan provenance links
use `ON DELETE SET NULL`.
Every workspace copied into a child row is immutable and participates in a
same-workspace composite foreign key. Hash fields explicitly described as
canonical digests are lowercase 64-character hexadecimal text. JSONB size and
schema are also validated in the repository before insertion; database checks
below provide the fail-closed type boundary.

Revision `001` defines only a generic canonical-JSON envelope, not the
production Serve/job projection payloads. The codec accepts a root object with
string keys; maximum nesting depth 16; maximum 4,096 aggregate keys/list
elements; strings at most 4,096 UTF-8 bytes; signed 64-bit integers, booleans,
null, lists, and objects; and no floating-point values. Domain schemas later
represent non-integral quantities as canonical decimal strings. The compact
UTF-8 encoding uses sorted keys, `,`/`:` separators, no ASCII escaping, and no
NaN/infinity, and is at most 65,536 bytes. A digest is SHA-256 over the compact
encoding of:

```text
{"domain": <closed domain>, "schema_version": 1, "payload": <object>}
```

Revision `001` closes domains for `placement_contract`, `topology`,
`physical_spec`, `intent`, `source_incarnation`, `source_fingerprint`,
`source_partition`, and `projection_cursor`. It also limits workspace and
source identifiers to 256 UTF-8 bytes, other source keys to 512, error codes to
128, allowlist JSON to 65,536 bytes, allowlist entries to 1,000 per field, and
each entry to 512 bytes. The repository checks these limits before SQL.

No production caller may write placement/topology/spec/cursor/finding payloads
in C1. Before C2 implementation, this file must define and adversarially review
the v1 key/type model, closed finding categories, desire ordering, source
partition/incarnation/fingerprint inputs, and exact mappings for current Serve
services/replicas, pools/workers, consolidated managed-job tasks/clusters, and
legacy pool assignments. Until that review, C1 repository tests use only
generic codec fixtures and deployed tables remain empty. This prevents schema
foundation work from silently choosing physical identity semantics.

`capacity_projection_scans` contains:

- `scan_id UUID PRIMARY KEY`, `workspace TEXT NOT NULL`,
  `source_kind TEXT NOT NULL CHECK (source_kind IN
  ('serve_service', 'serve_pool', 'managed_job_task'))`, and
  `source_partition_hash CHAR(64) NOT NULL`;
- `cursor_schema_version INTEGER NOT NULL DEFAULT 1 CHECK
  (cursor_schema_version >= 1)` and `cursor JSONB NOT NULL CHECK
  (jsonb_typeof(cursor) = 'object')`;
- `state TEXT NOT NULL CHECK
  (state IN ('running', 'completed', 'failed'))`;
- nullable `controller_instance_id UUID` and
  `controller_generation BIGINT`, with an all-or-none pair check and positive
  generation check;
- `rows_seen BIGINT NOT NULL DEFAULT 0 CHECK (rows_seen >= 0)`;
- `finding_counts JSONB NOT NULL DEFAULT '{}'::jsonb CHECK
  (jsonb_typeof(finding_counts) = 'object')`;
- nullable bounded `error_code TEXT`, `started_at`, `updated_at`, and nullable
  `completed_at`;
- checks requiring `completed_at` for terminal state, forbidding it for
  running state, requiring `error_code` exactly for failed state, and requiring
  `completed_at >= started_at`;
- unique `(scan_id, workspace)` and
  `(scan_id, workspace, source_kind)`;
- a partial unique index on
  `(workspace, source_kind, source_partition_hash)` where `state='running'`;
  and
- indexes on `(workspace, source_kind, completed_at DESC)` and
  `(state, updated_at)`.

A source row can become `source_missing` only after a completed full scan whose
scope includes its prior source partition. Cursor JSON is a progress
optimization, never absence proof. Closed per-scan finding counters plus the
30-day metrics store provide the C2 value record; revision `001` deliberately
does not persist a second per-job occupancy ledger.

`capacity_groups` contains:

- `group_id UUID PRIMARY KEY`;
- `workspace TEXT NOT NULL`;
- `owner_kind TEXT NOT NULL CHECK (owner_kind IN
  ('service', 'pool', 'managed_job_task'))`;
- nullable `owner_id TEXT` and `owner_incarnation TEXT`;
- `writer_fence_kind TEXT NOT NULL CHECK (writer_fence_kind IN
  ('serve_lifecycle', 'controller_generation', 'legacy'))`;
- nullable `writer_controller_fingerprint CHAR(64)`,
  `writer_instance_id UUID`, and `writer_fence_epoch BIGINT`;
- `source_kind TEXT NOT NULL CHECK (source_kind IN
  ('serve_service', 'serve_pool', 'managed_job_task'))`,
  `source_key TEXT NOT NULL`, and
  `source_incarnation_hash CHAR(64) NOT NULL`;
- `projection_confidence TEXT NOT NULL CHECK
  (projection_confidence IN ('exact', 'legacy', 'unknown'))`;
- `current_intent_generation BIGINT NOT NULL CHECK
  (current_intent_generation >= 1)`;
- `lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle_state IN
  ('active', 'retiring', 'retired'))`;
- nullable `last_seen_scan_id UUID`, `source_missing_at`, and `retired_at`,
  with checks requiring `retired_at` exactly for retired state;
- `created_by_actor_id TEXT NOT NULL`, `updated_by_actor_id TEXT NOT NULL`,
  `created_by_actor_type TEXT NOT NULL`, and
  `updated_by_actor_type TEXT NOT NULL`, with each actor type checked against
  `('system', 'basic', 'sa', 'sso', 'legacy', 'unknown')`;
- `created_at` and `updated_at`; and
- unique `(group_id, workspace)` and
  `(workspace, source_kind, source_key, source_incarnation_hash)`.

Writer-fence checks require a lowercase SHA-256 controller-owner fingerprint,
null instance UUID, and positive epoch for `serve_lifecycle`; a null
fingerprint, instance UUID and positive generation for
`controller_generation`; and all three null for `legacy`. The Serve
fingerprint is the canonical current controller hash/PID/IP/port tuple, not the
service incarnation. Exact confidence requires non-null owner ID and
incarnation plus a complete non-legacy writer fence. Legacy/unknown groups are
projection-only. A partial unique index on
`(workspace, owner_kind, owner_id, owner_incarnation)` for exact,
non-retired rows prevents two active groups for one stable owner incarnation.
`last_seen_scan_id` alone references the globally unique scan UUID with
`ON DELETE SET NULL`; the repository additionally verifies matching workspace
and source kind. This non-authoritative provenance pointer is the sole
same-workspace-FK exception because a composite `SET NULL` would incorrectly
try to null the non-null tenant column.

For Serve/pools, the group writer fence is the workload-row lifecycle epoch
plus controller-owner fingerprint. The global outer-controller
instance/generation is a separate transaction fence because it changes for the
entire controller role, not the workload incarnation. Every exact intent
publication executes the current-controller leadership statement on the same
PostgreSQL connection before checking the controller fingerprint and advancing
the service lifecycle epoch. Projection scans store that outer controller
pair; revision `003` attempts and tombstones snapshot it. For managed jobs,
controller instance/generation is itself the workload writer fence and is
stored on the group. Compatibility `all` mode without a provable outer pair may
project but cannot mutate.

Revision `001` reserves `retired` for the later proof-backed lifecycle and its
repository/projector never writes it. A terminal or missing legacy source is
at most `retiring` plus source-missing provenance until revision `002` can
record exact provider absence. The same rule applies to allocations.

After `capacity_group_intents` exists, the migration adds
`(group_id, workspace, current_intent_generation)` referencing
`capacity_group_intents(group_id, workspace, intent_generation)` as
`DEFERRABLE INITIALLY DEFERRED ON DELETE NO ACTION`. Group and first intent are
therefore inserted in one transaction and both sides are present at commit; no
temporarily null current-intent pointer is allowed.

`capacity_group_intents` contains:

- `group_id UUID NOT NULL`, `workspace TEXT NOT NULL`, and
  `intent_generation BIGINT NOT NULL CHECK (intent_generation >= 1)`;
- `schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version >= 1)`;
- `placement_contract JSONB NOT NULL CHECK
  (jsonb_typeof(placement_contract) = 'object')`;
- `placement_contract_hash CHAR(64) NOT NULL`;
- `desired_count INTEGER NOT NULL CHECK (desired_count >= 0)`;
- `topology JSONB NOT NULL CHECK (jsonb_typeof(topology) = 'object')`;
- `intent_hash CHAR(64) NOT NULL`,
  `source_fingerprint CHAR(64) NOT NULL`, `created_by_actor_id TEXT NOT NULL`,
  `created_by_actor_type TEXT NOT NULL CHECK (created_by_actor_type IN
  ('system', 'basic', 'sa', 'sso', 'legacy', 'unknown'))`, and `created_at`;
- primary key `(group_id, intent_generation)`;
- unique `(group_id, workspace, intent_generation)`, a non-unique lookup index
  on `(group_id, intent_hash)`; and
- a `DEFERRABLE INITIALLY DEFERRED ON DELETE NO ACTION` composite foreign key
  `(group_id, workspace)` to `capacity_groups(group_id, workspace)`.

The runtime canonicalizer hashes a domain prefix, schema version, canonical
placement/topology JSON, desired count, and desire rows into `intent_hash`.
Equal JSON in different schema domains cannot collide semantically. Under the
locked group, the projector reuses only the **current** intent when its hash
matches. Otherwise it inserts `current + 1`, even if an older historical
intent has the same hash, so an A-to-B-to-A transition remains monotonic. A
new source fingerprint with unchanged current meaning does not mint a new
generation.

`capacity_allocations` contains:

- `allocation_id UUID PRIMARY KEY`, `group_id UUID NOT NULL`, and
  `workspace TEXT NOT NULL`;
- `created_by_intent_generation BIGINT NOT NULL CHECK
  (created_by_intent_generation >= 1)`;
- `source_kind TEXT NOT NULL CHECK (source_kind IN
  ('serve_replica', 'pool_worker', 'managed_job_cluster'))`,
  `source_key TEXT NOT NULL`, and
  `source_incarnation_hash CHAR(64) NOT NULL`;
- `identity_confidence TEXT NOT NULL CHECK
  (identity_confidence IN ('exact', 'legacy', 'unknown'))`;
- nullable `spec_schema_version INTEGER`, `physical_spec JSONB`, and
  `physical_spec_hash CHAR(64)`, with an all-or-none check, positive version,
  and object JSON check;
- nullable `cluster_name TEXT` and `cluster_hash TEXT`; `cluster_hash` is the
  existing SkyPilot cluster-generation identifier and is not assumed to be a
  SHA-256 digest;
- `lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle_state IN
  ('active', 'retiring', 'retired'))` and nullable `retired_at`, with
  `CHECK ((lifecycle_state = 'retired') = (retired_at IS NOT NULL))`;
- `projection_state TEXT NOT NULL DEFAULT 'current' CHECK
  (projection_state IN ('current', 'source_missing', 'quarantined'))`;
- `observed_state TEXT NOT NULL DEFAULT 'unknown' CHECK (observed_state IN
  ('unknown', 'provisioning', 'up', 'stopped', 'absent', 'failed',
  'partial'))`;
- `observation_certainty TEXT NOT NULL DEFAULT 'legacy' CHECK
  (observation_certainty IN ('legacy', 'registry', 'provider'))`;
- nullable `observed_at`, `last_seen_scan_id UUID`, and
  `source_missing_at`;
- `created_at` and `updated_at`;
- unique `(group_id, workspace, allocation_id)`;
- unique `(group_id, source_kind, source_key, source_incarnation_hash)`;
- composite foreign keys `(group_id, workspace)` to the group and
  `(group_id, workspace, created_by_intent_generation)` to its birth intent;
  `last_seen_scan_id` to the globally unique scan UUID with
  `ON DELETE SET NULL`; and
- a partial unique index on `(cluster_hash)` where
  `cluster_hash IS NOT NULL AND lifecycle_state != 'retired'`, plus indexes on
  `(workspace, group_id, lifecycle_state)` and
  `(workspace, projection_state, source_missing_at)`.

The physical spec is nullable only for legacy/unknown projection rows.
`identity_confidence='exact'` requires a complete spec triple; cluster hash may
remain null before the first provider mutation and is filled only from the
exact created cluster generation. Source disappearance records
`source_missing_at`; it never proves retirement or provider absence.

`capacity_allocation_desires` contains:

- `group_id UUID NOT NULL`, `workspace TEXT NOT NULL`,
  `intent_generation BIGINT NOT NULL CHECK (intent_generation >= 1)`,
  `allocation_id UUID NOT NULL`,
  `ordinal INTEGER NOT NULL CHECK (ordinal >= 0)`,
  `desired_state TEXT NOT NULL CHECK
  (desired_state IN ('present', 'stopped', 'absent'))`,
  `release_gate TEXT NOT NULL DEFAULT 'blocked' CHECK
  (release_gate IN ('blocked', 'open'))`,
  `reason_code TEXT NOT NULL CHECK (reason_code IN
  ('projection', 'carry_forward', 'scale_up', 'replacement', 'recovery',
  'scale_down', 'teardown'))`, and `created_at`;
- primary key `(group_id, intent_generation, allocation_id)`;
- unique `(group_id, workspace, intent_generation, ordinal)`;
- `CHECK (release_gate = 'blocked' OR desired_state = 'absent')`; and
- composite foreign keys to the same-workspace intent and allocation.

Revision `001` release state is a legacy projection only and never authorizes a
capacity mutation. Revision `003` adds stable desire transitions and typed
release proof, then deterministically backfills the current desire for exact
eligible rows and quarantines all others before action authority can exist.
Pool-job assignment is compared by joining legacy `job_info` cluster identity
to projected pool-worker allocations and emitting closed scan counters/metrics;
there is no shadow occupancy table to dual-write or later migrate.

Revision `001` downgrade is allowed only when every capacity table is empty.
Otherwise it fails before dropping anything. All constraints and indexes have
explicit stable names and migration/runtime metadata parity is tested against
a real PostgreSQL instance.

### `capacity_groups`

| Column | Contract |
| --- | --- |
| `group_id UUID` | Application-generated primary key |
| `workspace TEXT` | Immutable tenant/authorization boundary |
| `owner_kind TEXT` | `service`, `pool`, or `managed_job_task` |
| `owner_id TEXT` | Stable workload identity |
| `owner_incarnation TEXT` | Stable source incarnation: service/pool hash or managed-job task row identity |
| writer fence | Serve controller-owner fingerprint plus lifecycle epoch, or controller instance UUID/generation |
| `current_intent_generation BIGINT` | Current immutable intent revision |
| `state TEXT` | `active`, `retiring`, or `retired` |
| actor fields | Creator and last authority-changing actor snapshots |
| timestamps | Database-clock creation and update times |

There is one active row per
`(workspace, owner_kind, owner_id, owner_incarnation)`. The group is a durable
owner incarnation and never stores a mutable physical spec. Serve
`services.hash`/resource scope changes only on recreation and is the
incarnation; `services.lifecycle_epoch` advances on every lifecycle lock and is
only the mutable writer fence. For managed jobs, logical job/task plus the
durable task-row ID form the incarnation; the API controller instance and
generation are only the mutable writer fence. Failover or an ordinary Serve
update changes the writer fence without creating a new capacity group.

### `capacity_group_intents`

| Column | Contract |
| --- | --- |
| `group_id UUID`, `intent_generation BIGINT` | Composite primary key |
| `schema_version INTEGER` | Canonical-spec encoding version |
| `placement_contract JSONB` | Immutable canonical resource alternatives and policy-neutral constraints |
| `placement_contract_hash TEXT` | SHA-256 of versioned canonical JSON |
| `desired_count INTEGER` | Number of allocations, not logical slots |
| `topology JSONB` | Immutable versioned group/member shape |
| `created_by TEXT` | Workload-controller actor snapshot |
| `created_at TIMESTAMPTZ` | Database time |

The canonical placement contract may contain heterogeneous resource
alternatives and topology constraints but not a claim that all allocations use
one selected location. Workspace is a scalar on the group, not buried in JSON.
It excludes credentials, runtime commands, application health, logical Serve
slots, and load-balancer state. Canonical JSON serialization sorts keys,
rejects non-finite numbers and unknown schema versions, and hashes the schema
version with the payload. A schema-version upgrade creates a new intent; it
never re-hashes old rows in place.

### `capacity_allocations`

| Column | Contract |
| --- | --- |
| `allocation_id UUID` | Stable identity created before provider mutation |
| `group_id UUID`, `workspace TEXT` | Same-workspace owning group |
| `created_by_intent_generation BIGINT` | Immutable birth intent |
| source identity | Source kind, key/incarnation digest, confidence, last complete scan, and missing time |
| `spec_schema_version INTEGER` | Exact allocation-spec encoding |
| `physical_spec JSONB` | Immutable selected physical shape and provider scope |
| `physical_spec_hash TEXT` | SHA-256 of canonical selected spec |
| `cluster_name TEXT` | Compatibility display and actuator name |
| `cluster_hash TEXT` | Exact SkyPilot cluster generation when known |
| `lifecycle_state TEXT` | `active`, `retiring`, or `retired` |
| `retired_at TIMESTAMPTZ` | Nullable retirement time |
| `observed_state TEXT` | `unknown`, `provisioning`, `up`, `stopped`, `absent`, `failed`, or `partial` |
| `observation_certainty TEXT` | `legacy`, `registry`, or `provider` |
| `observed_at TIMESTAMPTZ` | Database-normalized observation time |
| deletion proof (revision `002`) | Complete exact-scope observation ID, canonical proof digest, and applied time |
| timestamps | Database-clock creation and update times |

Rows survive deletion of legacy cluster, replica, or job rows and remain until
the proof and retention gates pass. A non-null `cluster_hash` is unique among
rows whose own `lifecycle_state != 'retired'`; the partial unique index does not
depend on joined group state. The lifecycle check requires `retired_at` exactly
when state is retired. Source provenance contains identifiers and canonical
digests, not pickles or raw handles. Heterogeneous Serve and pool waves bind
each exact allocation to its own selected immutable spec before provider
mutation; legacy/unknown projection rows may have no spec and remain
read-only. Revision `001`
contains no deletion-proof column; revision `002` adds
`deletion_proven_by`, `deletion_proof_digest`, and
`deletion_proven_at` together with the same-allocation composite foreign key.

### `capacity_allocation_desire_transitions`

This table is introduced by revision `003`, not by the read-only foundation.
The migration derives one transition from each exact eligible current desire;
legacy/unknown rows stay quarantined.

| Column | Contract |
| --- | --- |
| `desire_transition_id UUID` | Primary key and stable action identity |
| `group_id UUID`, `allocation_id UUID` | Exact allocation binding |
| `created_by_intent_generation BIGINT` | Intent that changed desired state/spec |
| `desired_state TEXT` | `present`, `stopped`, or `absent` |
| `release_gate TEXT` | `blocked` or workload-confirmed `open` |
| `reason TEXT` | Closed scale, replacement, recovery, or teardown code |
| release proof fields (revision `003`) | Workload incarnation, current writer fence, typed evidence and digest, and open time |
| `created_at TIMESTAMPTZ` | Database time |

Changing desired state or replacing the allocation spec mints a transition.
Opening a release gate is the only in-place change and requires the exact
workload/controller fence and typed release proof described below. An absent
transition is created blocked. Generic capacity code cannot open it and cannot
infer release from desired count, route absence, a missing workload row, or a
deadline it selected itself.

### `capacity_allocation_desires`

| Column | Contract |
| --- | --- |
| `group_id UUID`, `intent_generation BIGINT`, `allocation_id UUID` | Composite primary key |
| `desire_transition_id UUID` | Stable transition referenced by this intent |
| `ordinal INTEGER` | Position in this intent |
| `created_at TIMESTAMPTZ` | Database time |

Every new intent references a desire transition for every live allocation in
the group after revision `003`; in revision `001` the column does not exist.
Scale-only intents carry an equivalent transition UUID forward.
Present/stopped/absent or physical-spec replacement mints a transition.
Replacements add new allocations and retain predecessors until the workload
opens their release gate; a current intent then references the predecessor's
absent transition. Ordinals may be reused across intent revisions but are
unique within one intent for allocations whose transition is present or
stopped. A down action requires the current intent to reference the same absent
transition with an open release gate. A benign later intent cannot strand a
submitted action because carry-forward preserves the transition identity.

### `capacity_actuation_authority`

| Column | Contract |
| --- | --- |
| `group_id UUID`, `verb TEXT` | Composite primary key |
| `authority TEXT` | `legacy` or `capacity` |
| `authority_epoch BIGINT` | Monotonic action fence |
| `handoff_state TEXT` | `stable`, `quiescing`, or `reconciling` |
| `requested_authority TEXT` | Nullable operator request |
| actor fields | Who requested and committed the change |
| timestamps | Database-clock request and commit times |

Config may request a handoff but cannot flip authority. A stable owner moves to
`quiescing`, which fences both actuators from creating or submitting a new
external effect. An attempt already recorded as submitted under the unchanged
authority epoch may still record provider correlation, observation, and a
terminal or ambiguous outcome. Reconciliation in `quiescing` may read provider
state and finalize that same attempt, but it may not replay, enqueue, or invoke
a new effect. After the current actuator proves that it has no unaccounted
request and the capacity core reconciles all actions and provider observations,
a CAS changes to the requested authority, advances `authority_epoch`, and
returns to `stable`. Reverse handoff follows the same protocol. A submitted
effect whose outcome cannot be disambiguated blocks the handoff and binary
rollback; an operator cannot acknowledge it away by changing config. Both
legacy and capacity actuators fence submission on stable authority and fence
completion on the same unchanged authority epoch and attempt identity.

### `capacity_observation_runs`

| Column | Contract |
| --- | --- |
| `observation_id UUID` | Primary key |
| `group_id UUID`, `workspace TEXT`, `allocation_id UUID` | Exact tenant-owned target |
| provider scope scalars | Provider, account/project/context, region, zone, namespace |
| `provider_scope_hash TEXT` | Hash of canonical authorized scope |
| observer fence | Controller instance UUID and generation, observer lease token and expiry |
| `completeness TEXT` | `complete`, `partial`, or `failed` |
| `result TEXT` | `present`, `absent`, `partial`, or `unknown` |
| completeness proof | Expected identity hash, member count, sorted-member digest, and bounded error code |
| timestamps | Start and completion database times |

A complete zero-member run is a first-class row. Only a complete,
exact-scope, current observer run can retire missing members or author deletion
proof. It has member count zero and the canonical empty-member digest, not a
missing result set. The completion transaction revalidates controller instance
and generation, observer lease token/expiry, allocation identity and lifecycle,
and the immutable scope snapshot before applying the run and members.
Provider evidence outranks registry and legacy evidence; an older or
lower-certainty observation cannot overwrite it. Scope mismatches, incomplete
enumeration, expired leases, and stale controller generations remain diagnostic
only.

### `capacity_observation_run_members`

Revision `002` stores the immutable member set behind each completed or partial
run, not only the mutable current-member projection. Its composite identity is
`(observation_id, provider_member_id)` and it repeats allocation,
same-workspace group, provider-scope hash, typed role/state, evidence hash, and
observation time. A composite foreign key binds every row to that exact
allocation/scope/run. The sorted immutable rows must reproduce the run's member
count and digest. Without this table, a later mutable-member update would erase
the evidence used to claim complete enumeration or deletion.

### `capacity_members`

| Column | Contract |
| --- | --- |
| `member_id UUID` | Primary key |
| `group_id UUID`, `workspace TEXT`, `allocation_id UUID` | Same-workspace parent allocation |
| `provider_scope_hash TEXT` | Exact observation scope |
| `provider_member_id TEXT` | VM/pod/resource identity within scope |
| `role TEXT` | `head`, `worker`, or provider-specific typed role |
| `observed_state TEXT` | Current normalized member state |
| `lifecycle_state TEXT` | `active` or `retired` |
| `retired_at TIMESTAMPTZ` | Nullable member-retirement time |
| `last_observation_id UUID` | Complete or partial run that wrote it |
| `observed_at TIMESTAMPTZ` | Database time |

The unique identity is
`(allocation_id, provider_scope_hash, provider_member_id)`. A completed
observation run explicitly retires previously observed members absent from the
same exact scope. A partial or failed run never does. The retirement check
requires `retired_at` exactly when lifecycle state is retired. Observation runs
referenced by an active member, an allocation deletion proof, a tombstone, or
an unresolved action are pinned by `ON DELETE RESTRICT` and cannot be removed
by retention GC.

The run table exposes a unique
`(observation_id, group_id, workspace, allocation_id, provider_scope_hash)`
key. Member `last_observation_id` and allocation deletion proof use composite
foreign keys through that key, so evidence from another allocation, workspace,
or scope cannot be attached accidentally.

### `capacity_allocation_tombstones`

Revision `002` introduces retained terminal proof independent of workload
source rows. A tombstone contains an application-generated ID; group,
workspace, owner kind/ID/incarnation, the final workload writer-fence kind/
instance/epoch snapshot, the final outer-controller instance/generation
snapshot when present, allocation ID, physical-spec hash, nullable cluster
hash, exact provider-scope hash, retirement time, proof observation ID,
canonical proof digest, creation actor/time, and `retain_until`. The proof
foreign key uses the observation run's same-allocation/workspace/scope
composite key. There is exactly one tombstone per allocation identity. Mutable
writer/controller snapshots are retained for audit but never replace the
stable owner incarnation.

The digest is canonical typed evidence copied before an allocation can be
removed: allocation identity and tags, provider scope, completeness/member
digest, absent result, observer controller/lease fence, and completed time.
It contains no credentials or raw provider response. An observation run remains
pinned while its tombstone exists; if long-term policy later permits removing
the run, the independently retained digest and typed fields first pass a
separate audited compaction migration. Unknown or partial evidence never
creates a tombstone.

### `capacity_actions`

This is the stable logical physical action. It does not replace
`api_controller_action_reservations`: reservation-bearing outer controller
requests retain that existing fence, while one logical capacity action may
have multiple append-only attempts and normal-class inner request IDs.

| Column | Contract |
| --- | --- |
| `action_id UUID` | Stable logical action |
| `workspace TEXT` | Tenant boundary copied from group |
| `group_id UUID`, `allocation_id UUID` | Same-workspace exact target |
| `desire_transition_id UUID` | Stable desired-state transition being enacted |
| `idempotency_key TEXT` | Unique workspace/allocation/transition/verb identity |
| `verb TEXT` | `launch`, `start`, `stop`, or `down` |
| owner/writer fields | Owner kind/ID/incarnation, writer fence, and intent generation snapshot |
| `authority_epoch BIGINT` | Persisted per-verb authority fence |
| `state TEXT` | Closed state machine below |
| `current_attempt BIGINT` | Nullable monotonic attempt counter |
| `next_retry_at TIMESTAMPTZ` | Durable retry deadline |
| timestamps | Database-clock creation and update times |

`observe` is represented by observation runs, not lifecycle actions. `adopt`
is a separately authorized and audited ownership transition, not an ordinary
action verb. The unique key is
`(workspace, allocation_id, desire_transition_id, verb)`. `group_id` and
`workspace` are protected by composite foreign keys to the allocation and
transition. Intent generation is an audit snapshot, not action identity. The
nullable `(action_id, current_attempt)` pointer uses a
`DEFERRABLE INITIALLY DEFERRED` foreign key to the attempt table, so creating
the first append-only attempt and advancing the pointer is one valid
transaction without a dangling placeholder.

### `capacity_action_attempts`

| Column | Contract |
| --- | --- |
| `action_id UUID`, `attempt BIGINT` | Composite primary key |
| `lease_token UUID`, `lease_expires_at TIMESTAMPTZ` | Exact attempt fence |
| `request_id TEXT` | Deterministic ID minted before enqueue |
| controller fence | Scheduler/controller instance UUID and generation |
| `submission_phase TEXT` | `pre_submit`, `submitted`, `effect_claimed`, or `reconciling` |
| `policy_permit_id UUID` | Workload-policy permit consumed for this submission |
| `provider_operation_id TEXT` | Nullable provider correlation |
| typed outcome columns | Kind, scope, retry-after, cleanup certainty, failover safety |
| `diagnostic JSONB` | Bounded, closed, redacted detail |
| timestamps | Database-clock start and finish times |

Claiming an attempt mints its deterministic request ID, but a pre-submit claim
does not assert that an API reservation exists. Revision `003` adds one
connection-taking request-store submission function. The capacity worker
begins one transaction on its ordinary/default PostgreSQL engine and passes
that same `Connection` to the capacity and request-table helpers. In that one
transaction it:

1. locks and revalidates the current controller instance/generation and
   live controller leadership;
2. locks the action/attempt, exact lease token, current carried-forward desire
   transition, stable authority epoch, and unexpired workload-policy permit;
3. inserts the deterministic normal-class API request and queue state using
   serialization factored from the existing request-store primitives; and
4. marks the attempt submitted and consumes its permit.

The leadership check uses the current request-store statement that verifies
the durable singleton owner, instance/generation, unreleased lease, and live
election/generation advisory locks, with its locking option enabled on the
same connection. Capacity mutation authority is unavailable in a compatibility
`all` process that has no exported controller instance/generation.

Capacity and request helpers used here accept that connection and may not open
their configured engine, a manager session, or a nested transaction. The
current HTTP middleware always generates a random request ID and the public
SDK learns it only after submission, so the capacity path does not call that
HTTP endpoint. It factors request construction into a pure builder plus the
caller-connection insert; request-log creation happens only after commit.
Because the stores are separate Alembic lineages but the same physical
PostgreSQL database/schema, this is one database transaction and needs no
cross-lineage foreign key. A crash commits neither side or both sides; there is
no committed `pre_submit` row whose request may have been enqueued. On a
deterministic-ID conflict, the helper locks the existing request and accepts it
only if handler, canonical payload hash, user/workspace, schedule, cluster,
and action-attempt binding all match. Any mismatch is a terminal security
conflict, not retry success.

`api_controller_action_reservations` is not created by enqueue. Existing
request claim logic creates and checks it only for the controller execution
classes/replay policies that require that outer request fence. The inner
`sky.execution:launch` or `sky.core:user_initiated_down` request is
normal-class and therefore has no controller-action reservation; it keeps its
normal request claim/lease semantics. A non-replayable normal request whose
execution lease expires is treated as ambiguous and requires provider
reconciliation before another attempt. The capacity action/attempt and
provider identity are the durable physical-effect fence. Revision `003` tests
both reservation-bearing outer controller requests and ordinary inner
physical requests and does not make completion depend on a reservation that
the selected path never creates.

The deterministic request carries a closed `capacity-action.v1` precondition
with action ID, attempt, request ID, allocation/spec hash, desire transition,
writer fence, authority epoch, submitting controller instance/generation,
attempt lease token, and expected normal-request claim generation. Immediately
before its first provider mutation, the physical handler opens a short
transaction and CASes the attempt from `submitted` to `effect_claimed` only if:

- the request leadership statement still proves the exact submitting
  controller instance/generation owns the live controller election;
- the capacity attempt has the exact unexpired attempt lease/token;
- the ordinary API request is `RUNNING` under the exact current execution
  generation, claim token, worker instance, and unexpired request lease;
- the current service/job writer fence, carried-forward transition, stable
  authority, request binding, and consumed permit all match.

The request executor threads its claim token/generation/worker identity into
the connection-taking precondition helper; looking up request ID alone is
insufficient. This claim locks authority before marking the effect boundary. A
concurrent controller or authority handoff either changes first and rejects
the claim, or observes `effect_claimed` and waits for its outcome. If any lease
or leadership fence expires before effect claim, the request performs no
provider call; a new controller may mint a new attempt only after the old
request/attempt is safely cancelled or reconciled. Provider work then runs
outside the transaction. A generic queue claim without this capacity CAS never
authorizes provider mutation.

Typed outcome kind is one of `success`, `capacity_exhausted`, `quota`,
`authentication`, `invalid_spec`, `provider_error`, `timeout`, `cancelled`, or
`unknown`. Scope is `member`, `allocation`, or `provider_scope`. Cleanup
certainty is `not_needed`, `complete`, `partial`, `uncertain`, or `unknown`.
Retry-after is a database timestamp and failover safety is explicit. Raw
exception text is never stored.

The logical action state machine is:

```text
pending -> leased_pre_submit -> submitted -> effect_claimed
   |              |                |               |
   |              +--lease expiry->pending         |
   |                               |               |
   +-> cancelled/superseded        +--safe cancel  |
                                                   +--lease expiry/unknown
                                                              |
                                                              v
                                                          ambiguous
submitted/effect_claimed --typed retry--> retry_wait -> pending
submitted/effect_claimed --terminal-----> succeeded/failed_terminal
ambiguous -> observe/reconcile -> succeeded/retry_wait/ambiguous
```

`cancelled` and `superseded` are normally legal only before submission. A
submitted request may be safely cancelled only when the request store and
capacity attempt jointly prove it never reached `effect_claimed`; otherwise it
is ambiguous. Launch, start, and stop may be terminally invalid or
unauthorized; a `down` action never
becomes `failed_terminal` because of a retry budget and becomes `succeeded`
only when `deletion_proven_by` references a complete exact-scope absence run.
An ambiguous down remains visible and retryable indefinitely. External
provider calls occur outside database transactions and row locks.

### Workload release proofs and policy permits

Revision `003` makes workload authorization a positive, durable input rather
than an inference made by the generic reconciler.

For Serve replacement or scale-down, opening an absent transition records:

- release-evidence schema version and canonical evidence hash;
- service hash, service lifecycle epoch, logical replica ID and version;
- controller instance UUID/generation and workload release sequence;
- load-balancer authority/session and monotonic report sequence proving the
  predecessor URL was withdrawn from the exact routing set;
- required ready capacity and observed replacement ready capacity at that
  workload observation; and
- one closed drain disposition: `never_routed`, `seen_then_clean`,
  `bounded_deadline`, `endpoint_deleted`, or
  `provider_already_absent_or_preempted`, with the corresponding seen/explicit
  zero, workload-selected deadline and retirement kind, endpoint-deletion
  fence, or exact provider witness.

Revision `003` persists workload-owned readiness/drain observations because
provider `up` is not readiness and the current latest HTTP/dummy-job probe and
ordinary load-balancer in-flight tuple are process memory. A witness binds
service hash/lifecycle/controller fence, replica/version/allocation, probe kind
and result, observed database time, readiness sequence, LB authority/session,
report sequence, routing/draining/unknown URL sets, explicit zero, and the
logical retirement snapshot/coverage. The component producing the current
readiness or LB report writes it; a capacity projector timestamp is not a
substitute.

Scale-down selection locks service, replica, then capacity rows and commits
the off-route logical state plus a blocked absent transition. Evidence is
collected outside that transaction. The release-open transaction takes the
same locks, revalidates the exact current witness, replacement ready coverage
and drain policy, stores typed evidence/hash, opens only that transition, and
issues the down action/permit. A current-version logical-retirement timeout
still aborts and re-advertises; only an outdated version with revalidated
replacement coverage may use its bounded completion rule. Abort before release
creates a new present transition and restores routing; immutable transition
history is not rewritten. After down submission, action state replaces
`logical_retirement_committed` and cannot be aborted back to ready.

A generic capacity worker cannot choose or extend a drain deadline. A
pool-worker release proof instead records pool lifecycle epoch, scheduler drain
epoch, worker removal from the eligible-placement set, fresh dummy-job
readiness as applicable, configured pool drain delay, and zero active,
dispatching, running, releasing, or uncertain occupancies under the locked
allocation. Pool services remain endpoint-free; no inference-routing or LB
field is fabricated for them.

`capacity_policy_permits` is introduced in revision `003`. A permit is bound
to workspace, group, allocation, desire transition, action verb, exact
physical-spec hash, owner incarnation, current writer fence, logical replica
and version where applicable, and one action attempt. Its policy kind is
`ordinary`, `paid_claim`, or `reserved_fill`; its state is `issued`,
`consumed`, `revoked`, or `expired`. Paid permits carry the exact paid-claim
identity `(service name, service hash, replica ID, pool key)`. A reserved-fill
decision carries pool key, round ID and per-pool round epoch—not the global
broker lease epoch. Every permit also snapshots launch origin (`demand`,
`reserved_fill`, `cost_rebalance`, `unknown_capacity_replacement`, or
`recovery`), canonical version/YAML payload hash, planned capacity, exact
location/resources/spec, zero-cost/reserved flags, predecessor, and policy
priority. It has database-clock issue/expiry times, creator actor, reason, and
a unique workload-policy idempotency key.

The existing reserved-fill broker lock, or shared zero-cost demand reservation
lock, is acquired before opening the database transaction and is never acquired
while row locks are held. Creating a logical replica/allocation/action and
issuing a permit then uses one passed PostgreSQL connection. The database lock
order is:

1. current outer-controller leadership row `FOR SHARE`, when present;
2. service or pool lifecycle-fence row;
3. paid-capacity pool/claim rows, or reserved-fill round and claim rows;
4. logical replica/version row;
5. capacity group, current intent, allocation, and desire transition;
6. per-verb actuation authority; then
7. permit and action/attempt rows.

The same commit writes the logical `PENDING` replica, its immutable selected
allocation spec/present transition/action, and the paid claim or accepted
reserved holding. Existing Serve helpers are refactored to connection-taking
internals; their wrappers may not open a nested session.

Paid recovery reuses the matching unresolved claim and never silently acquires
a second one. A reserved-fill permit has two phases: before first materialized
submission, the outer broker lock and transaction require a live claim,
matching per-pool round epoch, and no `fence_pending`; committing the
reserved-fill replica/allocation converts that grant into a conserved holding
counted by later rounds. A recovery or re-drive validates that exact holding
and owner, not whether its old round epoch remains current. It mints a fresh
attempt permit from the holding. Thus a wrong current round blocks only a
never-materialized decision; it cannot strand a legitimate persisted
`PENDING` fill replica.

The atomic request-submission transaction consumes the issued permit as
immutable proof for that request. Before the first provider mutation, the
capacity-aware request precondition CASes the exact
action/attempt/request/service/desire/authority binding and rechecks that the
permit was not revoked or expired before submission. A consumed permit is not
mistaken for absent authorization. If authority or ownership changes during a
long launch, the existing watchdog behavior cancels and classifies an
unresolved provider effect as ambiguous. General capacity authority never
bypasses paid claims, conserved reserved holdings, demand-reservation locks,
rollout envelopes, launch budgets, or request parallelism.

Placement outcome delivery is idempotent by
`(action_id, attempt, outcome_sequence)`. The workload adapter locks the
current service lifecycle/replica fence, applies placement feedback at most
once, and records the source key in the same transaction. A stale outcome is
retained for audit but cannot debit a new paid claim, alter a new broker epoch,
or drive current placement policy.

### `capacity_occupancies`

| Column | Contract |
| --- | --- |
| `occupancy_id UUID` | Stable claim |
| `group_id UUID`, `workspace TEXT`, `allocation_id UUID` | Same-workspace pool-owned worker |
| `authority_epoch BIGINT` | Persisted `occupy`-verb authority fence |
| `occupant_kind TEXT` | `managed_job_task` initially |
| occupant identity | Job ID, task ID, task recovery/attempt generation, and assignment generation |
| writer fence | Controller instance UUID and generation |
| execution binding | Deterministic exec request ID and nullable remote pool job ID |
| `resources JSONB` | Canonical requested resource vector |
| `state TEXT` | `claimed`, `dispatching`, `running`, `releasing`, `released`, or `uncertain` |
| lease fields | Separate assignment/release token and database-clock expiry |
| `terminal_evidence JSONB` | Positive fenced job completion/release proof |
| timestamps | Database-clock creation and update times |

An occupancy can release resources but cannot change an allocation's desired
state. There is one active occupancy per exact job/task/recovery attempt and
assignment generation; controller generation is a separate writer fence. The
C5 assignment transaction locks, in order: current outer-controller leadership
`FOR SHARE`; exact pool service incarnation/lifecycle row; `occupy` authority,
group and current intent; candidate allocation in deterministic ordinal/ID
order; active occupancies for that allocation in ID order; `job_info`; then the
exact managed-job task row. No job-to-capacity transaction may acquire those
locks in reverse order.

It revalidates pool mode/hash/lifecycle, current writer fence, replica
`READY`, a fresh pool dummy-job workload witness, allocation active and
present, provider `up` observation fresh, job pool hash and controller fence,
the exact task's expected nonterminal status and recovery generation, and no
conflicting assignment. Provider `up` alone is not pool readiness.

Resource fit subtracts all
`claimed|dispatching|running|releasing|uncertain` occupancies from the immutable
allocation spec. Unknown allocation or occupancy resources fail closed and
stay on the entirely legacy assignment path; capacity authority never silently
uses the legacy one-job fallback. An empty resource request is represented as
an exclusive occupancy.

The occupancy insert, selected heterogeneous `full_resources`, compatibility
`current_cluster_name`, and job cloud/region/zone/cluster resource identity
written by the current `set_job_infra` path use one passed SQLAlchemy
`Session`/connection in the shared PostgreSQL transaction. Only the exact task
row receives the selected `full_resources`; a job-wide update is forbidden.
The refactor makes those job-state helpers transaction-aware and forbids nested
manager sessions. No external call occurs inside the transaction. A crash
therefore commits the claim and all job assignment fields together or commits
none of them; a reconciler repairs only legacy pre-C5 partial assignments using
exact job and pool fences. If jobs state is not in the same central PostgreSQL
database, the operation stays entirely legacy and never half-writes an
occupancy.

Before `sdk.exec`, the same deterministic request seam binds the exec request
to the occupancy and moves `claimed` to `dispatching`. After the remote job ID
is returned, an exact-fence CAS records it and moves to `running`. A crash
between claim and request does not release by TTL: the reconciler proves no
submission or marks the occupancy `uncertain`. An idempotent retry of the same
job/task/recovery attempt returns the same allocation and occupancy token; a
stale controller cannot overwrite it.

A release reconciler requires positive terminal/cancellation evidence for the
same job/task/recovery generation, controller fence, exec request, and remote
job ID, or exact worker absence. Moving the task to `RECOVERING`, or a missing
or stale job row, is not release proof. Pool policy cannot request allocation
absence while claimed, dispatching, running, releasing, or uncertain
occupancies exist. It first drains them and commits release evidence.

## Ownership mapping

| Workload | Stable owner ID | Owner incarnation | Mutable writer fence | Intent/attempt generation |
| --- | --- | --- | --- | --- |
| Serve replica group | Service name | Service hash/resource scope | Service lifecycle epoch and controller owner | Physical spec/version transition |
| Pool worker group | Pool name | Pool hash/resource scope | Pool lifecycle epoch and controller owner | Worker spec/target transition |
| Pool job | No physical ownership; occupancy only | Job ID + task ID + recovery attempt identifies the occupancy | Controller instance/generation | Occupancy assignment generation |
| Dedicated job cluster | Job ID + task ID | Durable managed-job task-row ID | Controller instance/generation | Recovery generation |

Old source rows/pickles lacking a stable owner incarnation, current writer
fence, exact cluster generation, or selected physical spec are projected with
legacy/unknown confidence. They are visible but never mutation-authorized
until they complete one of three finite exits:

1. natural retirement under legacy authority followed by recreation with new
   identity tags;
2. explicit operator adoption with exact scoped member identity, current-owner
   consent, actor audit, and a successful authority handoff; or
3. indefinite quarantine, where the legacy fallback remains responsible and
   the capacity reconciler stays read-only.

Quarantine never ages into adoption or deletion. PR #1071 cannot be removed
while any live or quarantined legacy service allocation still needs its
fallback.

## Provider identity

New allocations propagate `allocation_id`, stable owner-incarnation digest,
and immutable birth intent generation to provider tags or labels in addition
to existing cluster-name and managed-resource tags.
Kubernetes, AWS, and GCP are independently gated capabilities, not one
all-or-nothing rollout. A provider may progress from tag-only to observe to
teardown authority only after its adapter passes the exact-scope and crash
matrix. Start and stop are enabled only for providers and owner types whose
existing actuator has defined semantics.

Provider adapters must support bounded exact-scope observation by allocation
identity. Scope uses scalar provider, account/project/context, region, zone,
and namespace fields plus a canonical hash. Provider member IDs are unique
only within that scope.

Legacy resources without these tags can be associated with a current
`cluster_hash` for read projection, but cannot be automatically adopted or
deleted by the new reconciler. Existing name-based actuator behavior remains
behind legacy authority until one of the finite exits above. A name-only
provider absence is not exact deletion proof for an untagged allocation.

## Security and tenancy

- `workspace` is an immutable indexed group column and participates in active
  owner uniqueness, action idempotency, authority lookup, and every read/write
  authorization check.
- Provider observation and adoption verify that the workspace is authorized
  for the exact provider account, project, context, region, and namespace.
- JSON fields never contain credentials, kubeconfigs, serialized handles, SSH
  material, environment values, raw commands, or raw provider exceptions.
- Diagnostics use closed codes and bounded redacted fields. Provider account
  and member identifiers are returned only to existing privileged
  infrastructure/operator surfaces, not ordinary dashboard/status readers.
- Adopt, release, manual retry, and authority changes record actor ID, type,
  workspace, reason code, and database timestamp.
- The first release uses SkyPilot's existing application-level PostgreSQL
  authorization boundary. Migration, projector, observer, actuator, and read
  responsibilities are separated in code and request handlers even though the
  deployment currently shares one database role. A future database-role split
  requires its own rollout.
- Tests include cross-workspace owner collisions, observation injection,
  unauthorized adoption, idempotency-key collision, and redaction failures.

## Invariants

1. A missing source row, owner row, cluster row, serialized handle, or request
   row is never deletion proof.
2. Only an explicit current `desired_state=absent` transition from the current
   owner incarnation and writer fence, with a workload-opened positive release
   proof, authorizes `down`.
3. Provider-confirmed absence is the only automatic deletion proof.
4. A stale owner incarnation, writer fence, desire transition, authority
   epoch, controller generation, lease token, permit, or attempt cannot submit
   an effect or apply its result as fulfillment of current intent. Exact
   provider outcome/observation is still appended to the originating attempt
   so it can be reconciled or cleaned up; it cannot mutate current workload
   policy. Benign intent carry-forward does not invalidate the same transition.
5. One allocation has exactly one lifecycle owner. Pool occupants never own
   their worker.
6. Provider identity is scoped; a matching unscoped name is not identity.
7. No provider call occurs while holding a database transaction or row lock.
8. Replacement allocation readiness is accepted by the workload controller
   before its predecessor becomes releasable.
9. Uncertain deletion remains owned, visible, and retryable indefinitely. It
   does not age into success.
10. Existing cluster resource-operation locks remain mandatory around legacy
    actuator calls.
11. Projection uncertainty fails closed: unknown resources consume capacity
    and block destructive repair.
12. Every mutable table uses PostgreSQL database time for leases and retry
    deadlines.
13. Every cross-lineage atomic operation receives one existing
    `Session`/connection; it cannot open a nested manager session.
14. Only stable persisted per-verb authority may create or submit a new
    effect. Quiescing may only reconcile and complete an already-submitted
    attempt under the unchanged authority epoch; ambiguity blocks transfer.
15. A lower-certainty or older observation cannot overwrite current provider
    evidence.
16. Paid, reserved-fill, readiness, routing, drain, and occupancy policy cannot
    be bypassed by generic lifecycle authority or a delayed retry.

## Reconciliation flows

### Serve and pool launch

The workload controller commits an immutable intent, allocation, desire,
logical launch action, and workload-policy permit before submission. The
action worker creates a leased attempt, then uses the one-connection submission
seam to atomically consume the permit, insert the deterministic normal-class
request and queue row, and mark the attempt submitted. It does not call the
public SDK/HTTP request-ID path. It later records provider-operation
correlation and observes the cluster generation and provider members. Serve
readiness and pool dummy-job readiness remain workload-owned. Only their
affirmative fenced proof can open replacement release; a later intent
explicitly carries the predecessor's absent transition.

### Explicit teardown

The workload controller publishes a new immutable intent whose current desire
marks the allocation absent. The reconciler leases the exact down attempt,
revalidates workspace, owner, carried-forward absent transition, typed release
proof, fresh policy permit, authority epoch, and occupancies, then atomically
submits the existing generation-fenced teardown handler under the
resource-operation lock. A successful API request is not deletion proof. Only
a complete exact-scope provider observation can mark the allocation absent and
complete the action.

### Pool placement

The pool placer selects a Ready pool-owned allocation from the projection and
uses one PostgreSQL session to lock in the documented order, insert an
occupancy, and write the compatibility managed-job assignment. It revalidates
the resource vector, owner incarnation, writer fence, current intent, provider
and workload-readiness observations, and exact job/task/recovery attempt.
Positive current-attempt and remote-job terminal evidence releases the
occupancy; job-row absence does not. Pool jobs never request worker teardown.

### Dedicated managed-job recovery

Each task recovery generation advances physical intent. A new recovery may
launch a replacement allocation. The preceding allocation moves to desired
absent only under the current managed-job controller writer fence. Controller
failover updates that fence but does not create a new owner group. Terminal
task cleanup reconciles every allocation generation before retiring the group.

## Failure and blast isolation

- Projection, observation, and action work is partitioned by workspace, owner
  group, and provider scope. One malformed row is quarantined with a closed
  reason and cannot stop other groups.
- Provider concurrency, QPS, batch size, and observation freshness are bounded
  separately per provider. Backpressure makes rows stale/unknown; it never
  drops them or interprets them as absent.
- The C2 projector runs as a child of the existing elected controller leader.
  It does not acquire another long-held singleton advisory-lock session.
- Provider outage, credential loss, schema mismatch, or observer failure
  disables destructive decisions only for the affected scope.
- Action workers use `FOR UPDATE SKIP LOCKED` and bounded leases; retry storms
  are limited per provider and owner.
- Metrics include projection lag, mismatch class/count, observation age,
  ambiguous-action age, deletion-proof backlog and estimated cost, lease
  contention, retry age, provider calls, and occupancy contention.

## Retention, garbage collection, and repair

Default terminal attempt and completed observation-run retention is 90 days.
An allocation/group tombstone retaining workspace, owner, allocation identity,
physical-spec and cluster hashes, provider-scope hash, terminal proof ID and
digest, and retirement time is retained for 365 days. Allocation and member
lifecycle markers are explicit; source disappearance does not mark either
retired. These server-admin durations may be increased but not reduced below
the active binary rollback window.

A group is GC-eligible only when:

- it is retired and every allocation is desired absent;
- every allocation has exact provider absence proof;
- no action or attempt is pending, leased, submitted, ambiguous, or retrying;
- every occupancy is released with positive evidence;
- no quarantined legacy resource remains;
- the compatibility and rollback windows have elapsed; and
- event/audit retention has captured the terminal identities.

The GC transaction first creates and verifies the same-allocation tombstone,
then removes dependents in explicit leaf-to-root order. A complete observation
run is not age-eligible while referenced by an active member, allocation
deletion proof, tombstone, or nonterminal/ambiguous action. Those references
are database foreign keys, not an application-only scan. A retained terminal
attempt pins any observation used to reconcile its outcome. Only after the
dependent retention window closes may the corresponding allocation and then
group be removed.

Unknown, partial, ambiguous, unproved, or quarantined rows never become
GC-eligible by age. Repair tooling is workspace-authorized, dry-run by default,
requires an actor and closed reason, and uses the same authority and lease
fences as automated work.

## Migration and stacked commits

Each stack commit is independently testable and deployable. A documentation-only
commit has no runtime artifact to deploy. Every code-bearing commit is deployed
to the isolated `skypilot-ha` test release before work begins on the next stack
commit, using Helm `--reuse-values`, exact image digest capture,
schema-revision verification, role readiness checks, and post-deploy residue
checks. If an exact-commit artifact or authenticated test control plane is
unavailable, the stack pauses at that commit; it is not substituted with the
shared production release or an unrelated fleet.

### C0: Canonical design

- Commit this design and complete adversarial review.
- Record the inspectable source/test baseline and the exact credential/tooling
  blocker; capture current test release, database revisions, and clean runtime
  baseline before the first C1 rollout.
- Runtime behavior and schema are unchanged.

### C1: PostgreSQL capacity foundation

- Add PostgreSQL-only revision `001` with only the literal read-projection
  tables authorized above.
- Add typed row enums, the bounded generic canonical-JSON/domain-hash codec,
  and a transaction repository with no production writer or projector.
- Add schema/runtime parity, projection-scan, immutable-intent/desire,
  tenant-FK, idempotency, concurrency, and non-empty downgrade-refusal tests.
- Initialize global first and capacity last in the central migration job; make
  every PostgreSQL server role verify revision `001`.
- Deploy with no projector and no runtime reads or actions.

### C2: Persistent read-only projection

- First update this design with literal v1 projection payload/source mappings
  listed in the C1 encoding boundary and pass another exact-file adversarial
  review.
- Add normalized adapters for Serve replicas, pool workers, and consolidated
  dedicated jobs. Compare pool-job assignments from legacy `job_info` to
  projected workers without persisting a shadow occupancy table. Represent
  remote/non-consolidated state as unknown and leave it entirely legacy.
- Add a controller-leader child projector with bounded batches and durable
  cursors, without a new engine namespace or singleton lock session.
- Retain allocation rows and mark `source_missing` only after a complete scan;
  do not call that disappearance retirement or deletion proof.
- Add drift categories and metrics; execute no provider mutation.
- Compare exact identity, owner, spec, state, and proposed actions with legacy
  sources and provider truth—not merely counts or two database derivations.
- Record parity through restarts and controller handoff, collect the 30-day
  value evidence, and apply the explicit C2 go/no-go gate.

### C3: Provider identity and observation

- Update this design with the literal revision `002` DDL and re-run
  adversarial review before implementation.
- Add allocation ID, owner-incarnation digest, and birth-intent tags/labels for
  Kubernetes, AWS, and GCP.
- Add exact-scope member observation and complete observation runs.
- Add explicit member retirement, same-allocation deletion proof, proof
  digests, tombstones, and observation-retention pins.
- Preserve legacy name-only behavior for pre-C3 allocations.
- Deploy with new launches tagged but legacy controllers still authoritative.
- Advance providers independently through tag-only and observe capability
  gates.

### C4: Durable explicit teardown

- Update this design with literal revision `003` authority/action/attempt/
  permit DDL and the reviewed connection-taking request-store API before
  implementation.
- Project explicit non-pool Serve owner teardown intent into capacity actions.
  Pool-worker teardown remains legacy until authoritative occupancies exist in
  C5 and pool lifecycle moves in C6; dedicated jobs remain legacy until C7.
- Perform the persisted per-verb authority handoff for an allowlisted provider,
  workspace, owner, and allocation only after legacy action quiescence.
- Execute only explicit, current-owner down retries under the authority epoch.
- Atomically fence controller leadership, consume the attempt permit, insert
  the normal API request/queue row, and mark submission on one connection.
- Keep launch/start/stop and inferred orphan repair in shadow mode.
- Require provider-confirmed absence before finalizing deletion.
- Fault-inject controller, executor, API, and database interruption at every
  action boundary.

### C5: Pool occupancy authority

- Update this design with literal revision `004` occupancy DDL and re-run
  adversarial review before implementation.
- Make PostgreSQL occupancies the pool assignment authority.
- Use one cross-lineage session for occupancy, selected `full_resources`,
  current cluster, and job infrastructure compatibility assignment, with the
  documented service/replica/job lock order and release reconciler.
- Use strict fail-closed resource accounting under occupancy authority. A
  group with unknown canonical resources remains entirely on the legacy
  conservative one-job-per-idle-worker path; it cannot half-enable C5.
- Keep pool autoscaling and worker lifecycle policy in
  `SkyPilotReplicaManager`.

### C6: Serve and pool-worker lifecycle authority

- Require C5 occupancy authority and exact zero-occupancy release proof before
  enabling any pool-worker lifecycle verb.
- Move launch, start, stop, replacement, and down execution for Serve and pool
  workers to capacity actions.
- Require fresh attempt-bound ordinary or paid-claim permits; reserved-fill
  re-drives derive a fresh permit from their conserved persisted holding
  rather than a now-obsolete broker round. Deliver placement outcomes
  idempotently.
- Retain legacy replica rows as compatibility projections.
- Cut over through persisted authority by service/pool, provider, and action
  verb; never run two actuators.
- Prove rolling readiness and drain gates remain workload-owned.

### C7: Dedicated managed-job lifecycle authority

- Migrate dedicated task allocations in consolidated PostgreSQL mode.
- Prove recovery-generation, controller-generation, cancellation, and terminal
  cleanup fencing.
- Keep non-consolidated remote controllers on legacy authority until a durable
  central intent feed is separately accepted and deployed.

### C8: Read cutover and compatibility retirement

- Move status, dashboard, spend, orphan diagnostics, and repair inventory to
  the shared projection.
- Complete a full rollback-window soak with no legacy-only allocation.
- Delete the legacy implementation listed below only when every row-level and
  fleet-level gate is satisfied.

## Deployment and rollback

The initial deployment target is the isolated HA test release documented by
`docs/designs/multi-replica-api-server.md`: Kubernetes context `boltz-test`,
namespace and Helm release `skypilot-ha`. The shared `gitops-hub-rainier`
release and the existing `test` namespace are out of scope.

The existing image/chart publisher accepts only the `improvements` branch.
Feature-branch deployment therefore requires either an explicitly approved
test-only exact-commit publisher or review/merge followed by verification that
the published image digest was built from that merge commit. The implementation
must not push or merge to `improvements` merely to manufacture a deployable
artifact. Before any rollout, preflight proves a live AWS STS identity in the
test account, the exact `boltz-test` kube context, available `kubectl` and
`helm`, release ownership, baseline request/service-version history, and
unresolved prior rollout state.

Before every deployment:

1. capture branch commit, image tag and digest, Helm revision, pod UIDs and
   images, database revisions, capacity row counts, and active test workloads;
2. run schema migration dry-run and Helm server-side dry-run;
3. use `helm upgrade ... --reuse-values`;
4. prove the migration Job completed before a target-image pod was created;
5. wait for all API, executor, and controller replicas and PDBs to be healthy;
6. run phase-specific conformance and compare exact image digests; and
7. remove one-shot Jobs, canaries, failed pods, and test workloads.

Schema changes are expand-first. Operational rollback never runs Alembic
downgrade. Old binaries ignore the additive capacity version table and rows;
new binaries accept a later additive numeric capacity revision but refuse a
missing or older required revision. A tested binary/schema compatibility
matrix is recorded for every commit.

Rollback requests a persisted reverse authority handoff. The current capacity
actuator enters quiescing, stops new claims, and reconciles all leased,
submitted, and ambiguous attempts. Only after no unaccounted effect remains
may the handoff CAS advance the authority epoch and restore stable legacy
authority. Config rollback alone cannot transfer ownership. Additive tables,
attempts, observations, and tombstones remain intact. A binary rollback cannot
cross a legacy-code deletion; C8 begins only after every supported binary and
live group is outside the rollback window.

Feature gates are server-owned, owner-kind and action-verb scoped:

- `shadow`: project and compare only;
- `observe`: provider observation, no lifecycle mutation;
- `teardown`: explicit non-pool Serve down retries only in C4;
- `serve`: Serve and pool-worker lifecycle authority;
- `jobs`: dedicated managed-job lifecycle authority.

Modes are server-admin intent, not authority. They include provider, workspace,
owner-kind, group, and verb allowlists and request a transition through
`capacity_actuation_authority`. An invalid or unknown value fails startup.
SQLite accepts only `disabled`; non-consolidated jobs remain legacy.
The `serve` mode cannot authorize a pool lifecycle verb until revision `004`
occupancy authority and its group-level handoff are complete.

The minimum compatibility sequence is:

1. old binary + pre-C1 schema;
2. old binary + additive C1 schema;
3. new shadow binary + C1-or-later schema;
4. mixed new binaries with no authority handoff;
5. all authority-capable binaries at or above the commit recorded for that
   handoff.

No authority transition is legal in steps 1-4.

## Legacy code and schema removal ledger

Removal is mandatory after its gate for the consolidated PostgreSQL control
plane; retaining two lifecycle authorities for the same eligible allocation is
not an acceptable completed migration. This project does **not** deprecate
supported local SQLite or non-consolidated controllers. Before C6, their
physical actuator is extracted behind an explicit `LegacyPhysicalActuator`
boundary that cannot be selected for a capacity-authorized group. Central
PostgreSQL branches and in-memory state are then deleted, while equivalent
implementation needed solely behind that compatibility boundary remains.
Deleting that final scoped actuator requires a separate accepted deprecation
and durable central-intent-feed migration.

Line numbers are not canonical and will move. Symbols and behavioral ownership
are canonical. “Delete” below means delete globally only where the disposition
explicitly says so; otherwise it means delete from the central/consolidated
path and retain the named compatibility implementation.

| Legacy surface | Final disposition | Earliest removal gate |
| --- | --- | --- |
| `SkyPilotReplicaManager._launch_thread_pool`, `_replica_to_request_id`, `_replica_to_launch_cancelled`, and `_down_thread_pool` in `sky/serve/replica_managers.py` | Delete these manager-owned symbols after append-only capacity attempts own central request identity, cancellation, and execution. Move only the still-supported SQLite/non-consolidated behavior behind `LegacyPhysicalActuator`; do not claim its code is gone. | C6 authority for every eligible active service/pool; no nonterminal central legacy request for one rollback window |
| `_failed_cleanup_retry_attempts`, `_failed_cleanup_retry_at`, `_failed_cleanup_retry_state()`, `_clear_failed_cleanup_retry()`, and `_schedule_failed_cleanup_retry()` | Delete from the central manager. Durable down actions own central retry attempts and database deadlines; a separately named local compatibility implementation may remain. | C6 authority; zero central process-local retry-only rows for one rollback window |
| Physical action/result portions of `_recover_replica_operations()`, `_refresh_thread_pool()`, `_thread_pool_refresher()`, and `_handle_sky_down_finish()` | Split and delete central launch/down scheduling, polling, and result ownership. Retain preemption detection, logical retirement, placement feedback, drain, other safety policy, and the scoped local actuator dispatch. | C6 plus exact behavior-port and crash tests |
| Physical action scheduling and request bookkeeping inside `SkyPilotReplicaManager._launch_replica()` and `_terminate_replica()` | Remove the central legacy-actuator branches and retain intent, release, readiness, policy, and explicit local-actuator adapters. Do not delete the manager. | C6 plus crash matrix and mixed-version rollback completion |
| Provider executor helpers `launch_cluster()` and `terminate_cluster()` in `sky/serve/replica_managers.py` | Move into reusable fenced physical handlers. Remove in-memory map/cancel-polling arguments from the central call path; preserve provider execution and cleanup ordering for both capacity and the scoped local adapter. | C6 central actions call fenced handlers exclusively |
| `ReplicaStatusProperty.sky_down_status` as physical action authority and the `replicas.sky_down_status` column | Stop central authority reads/writes and derive central display from capacity actions. Retain the field, column, JSON compatibility, launch/down status, and drain/retirement fields for supported local/non-consolidated controllers; global drop is outside this project. | Central read consumers use capacity actions; rollback window closed |
| `serve_state.get_replica_launch_budget_counts()` and physical launch-count wrappers consumed by `controller_utils.in_flight_launch_count()` | Replace central physical in-flight accounting with capacity action queries. Retain `can_provision`, `can_terminate`, request-parallelism policy, and the legacy weighted-count path for local controllers. | C6 launch authority and central count parity |
| PR #1071 shim: `global_user_state.get_managed_cluster_status_fields()`, `serve_state.get_replica_cluster_names()`, `serve_utils.get_orphaned_service_cluster_status_fields()`, and only its additive merge inside `backend_utils.refresh_cluster_records()` | Delete together. Retain ordinary user-cluster refresh and provider failure semantics. | Zero live/quarantined legacy service allocation needs the fallback; exact identity/proof soak complete |
| `serve_utils.get_existing_replica_cluster_names()`, `quiesce_service_replica_launch_requests()`, and physical cluster loops inside `_terminate_failed_services_locked()` and `_terminate_orphaned_service_children_impl()` | Replace central behavior with desired-absent intent and deletion-proof waits. Retain LB fencing/deletion, storage cleanup intents, lifecycle/hash checks, final metadata CAS, and scoped local cleanup. | C6 physical teardown authority and component tests |
| Pool assignment critical section in `serve_utils.get_next_cluster_name()` and the local `FileLock` used for worker selection | Remove from consolidated PostgreSQL placement and replace it with the one-session occupancy transaction. Retain the lock only inside the explicitly selected local/non-consolidated compatibility adapter, plus unrelated service filesystem locks and resource-fit/fallback policy. | C5 atomic contention, recovery, and fail-closed tests pass live |
| `jobs.state.set_current_cluster_name()` as the assignment authority | Remove central direct assignment callers; keep a compatibility projection writer/read field and local-controller caller. Dropping the field globally is outside this project unless all supported clients/controllers leave the rollback window. | C5 authority, then central fleet compatibility gate |
| Pool used-resource reconstruction in `jobs.state.get_pool_worker_used_resources*()` and `_ranked_nonterminal_job_resources()` | Delete central capacity-admission use. Status reporting moves to occupancies, but keep the explicitly routed local/non-consolidated scheduler implementation and any separately named job resource reporting. | C5 read parity and dashboard/status cutover |
| Direct dedicated-job physical launch in `jobs/recovery_strategy.StrategyExecutor`, `JobController._cleanup_cluster()`, and physical cleanup in `jobs/utils.terminate_cluster()` | Remove consolidated PostgreSQL legacy lifecycle branches; retain task recovery/failover decisions, capacity-intent adapter, and explicitly dispatched non-consolidated/local behavior. | C7 consolidated-job authority and cancellation/recovery crash matrix |
| Best-effort `clusters.workload_type`, `workload_id`, and `workload_task_id` as managed physical ownership authority | Stop lifecycle-authority reads only. Retain authoring, history, cost, and spend attribution unless a separate accepted spend design replaces every consumer. | C8 lifecycle query audit |
| Provider discovery that identifies a managed allocation solely by cluster-name tags | Remove from authoritative adoption/deletion. Name filters may remain for user-facing lookup and legacy read-only inventory. | C3-tagged fleet or explicit legacy quarantine complete |
| Capacity-specific use of `api_controller_action_reservations` as the only physical-operation record | Remove such reliance, not the table. Request reservations remain required for controller request fencing. | C4 actions authoritative |
| Projection adapters, dual-read mismatch writers, compatibility backfills, and migration-only feature gates | Delete for fully migrated central owner kinds after central reads use the capacity core. Keep explicit offline repair tooling and the minimal local/non-consolidated compatibility boundary; global deletion is a separate deprecation. | C8 central fleet soak and rollback-window closure |

The final C8 evidence must classify deletion at two levels:

| Fully removed by this project | Intentionally remains after this project |
| --- | --- |
| Central PostgreSQL call sites that schedule/poll launch or down through manager-owned thread pools and request-ID/cancel maps | The equivalent implementation, with different explicit names, inside `LegacyPhysicalActuator` for SQLite/non-consolidated mode |
| Central process-local failed-cleanup retry deadlines/counters | Local compatibility retry state |
| Central pool assignment through `get_next_cluster_name()`'s `FileLock` and job-resource reconstruction | The explicitly routed local/non-consolidated lock/reconstruction branch |
| Central direct managed-job lifecycle actuation and cleanup branches | Non-consolidated/local managed-job actuation |
| Capacity-mode use of replica launch/down status as physical authority | The fields/columns as local compatibility and workload display/history |
| PR #1071 ownerless fallback, but only after its exact no-legacy/no-quarantine proof gate | Generic cluster refresh and name-based read-only legacy inventory |
| C2 projector/dual-read/backfill runtime once every eligible central owner/read has cut over | Offline repair/export tools and compatibility-mode routing |

Consequently, this migration fully removes duplicate **central authority and
its call sites**, but it does not honestly claim to delete all actuator code or
schema from the product. That broader deletion cannot occur while supported
SQLite/non-consolidated modes can create new legacy resources.

Before each deletion commit, `rg` output for every symbol and column must be
attached to this document with a keep/remove classification. Code that still
owns application policy, user-facing history, non-physical cleanup, request
fencing, or local-controller SQLite compatibility must not be removed merely
because it is adjacent to migrated physical lifecycle code.

The following are explicitly retained by this project:

- the global `clusters` table and generic cluster CRUD/status/handle/history;
- `services`, `replicas`, `spot`, and `job_info` workload identity, status,
  readiness, version, history, and policy state;
- service lifecycle fences, logical capacity, rollout, LB routing/drain, and
  non-physical storage/LB cleanup;
- `SkyPilotReplicaManager`, Serve/pool autoscalers, reserved-fill and
  paid-capacity arbitration, pool readiness, and no-endpoint behavior;
- paid-claim recovery, reserved-fill broker and demand-reservation locks,
  conserved reserved holdings, and their SQLite atomic predicates;
- replica launch/down status plus drain/logical-retirement fields for
  compatibility modes and workload evidence;
- managed-job scheduling, status machines, recovery/failover policy, and
  non-consolidated local-controller support;
- generic `refresh_cluster_records()` for ordinary clusters; and
- the API request queue, controller leadership, and
  `api_controller_action_reservations`.

### Test retirement ledger

Behavior tests are ported before implementation-coupled assertions are deleted.
The ownerless shim tests in `test_refresh_sweep.py`, `test_serve_utils.py`,
`test_serve_state.py`, and global-state tests become exact projection and
deletion-proof tests before the shim disappears. Pool fit and fail-closed
fallback behavior in `test_serve_pool_scheduling.py` and
`test_pool_resource_accounting.py` is rerun unchanged against occupancy CAS.
Tests that call `_recover_replica_operations()` are split between capacity
action recovery and retained Serve policy. Graceful-drain, restart-bounded
drain, autoscaling, recovery, paid/reserved capacity, and application-readiness
tests are retained rather than deleted as collateral cleanup.

## Verification

### Automated

- Real-PostgreSQL migration upgrade, verify, bootstrap, additive compatibility,
  and non-empty downgrade refusal.
- Capacity manager forwards `verify`, `upgrade`, and `bootstrap`, rejects
  SQLite before Alembic, shares the default global engine without another
  `create_engine`, and performs no work at import time.
- Local SQLite common initialization skips disabled capacity and fails clearly
  if capacity is requested. A missing capacity revision fails every
  PostgreSQL role in verify mode.
- Fresh subprocess bootstrap creates
  `alembic_version_capacity_state_db=001` alongside all existing histories,
  with global first and capacity last.
- Runtime/migration metadata parity, same-lineage concurrent migration locking,
  immutable intent/desire, stable owner-incarnation versus mutable writer-fence
  behavior, authority handoff, idempotency, owner/intent/authority/lease
  fencing, database-clock expiry, and GC refusal tests.
- Projection tests for current and legacy Serve replicas, pools, consolidated
  dedicated jobs, pool-assignment parity counters, partial clusters, incomplete
  scans, missing source rows, and same-name successor generations. Shadow mode
  must make zero provider mutations.
- Provider tag/label and exact-scope discovery tests for Kubernetes, AWS, and
  GCP, gated independently.
- Observation-run tests cover complete zero-member, partial, failed, late,
  stale-fence, wrong-scope, duplicate callback, and credential-loss cases.
- Action fault injection before attempt commit, after deterministic request-ID
  commit but before atomic submit, inside request/queue+attempt submit, after
  queue claim but before capacity effect claim, after effect claim, after
  provider success but before outcome, after writer/authority transfer, during
  retry, and during deletion proof. Assertions prohibit double launch/delete,
  stale commit, resurrection, and premature retirement.
- Pool contention, capacity-vector serialization, assignment rollback,
  exact task-only resource update, deterministic exec binding, unknown
  occupancy, fresh dummy-job readiness, positive-evidence release, teardown
  race, controller restart, stale recovery attempt, remote-job ambiguity, and
  same-session/no-nested-session tests.
- Cross-workspace access, provider-scope authorization, adoption audit,
  diagnostic redaction, and secret-rejection tests.
- Serve rolling update, durable readiness/LB witness, seen-clean and bounded
  drain, health, scale-up/down, orphan, paid-claim recovery, reserved-holding
  re-drive after broker-epoch advance, demand-lock, and pool smoke tests.
- Managed-job launch, preemption, recovery, cancellation, terminal cleanup,
  job-group, pool, and controller-handoff tests.
- Old server/new client and new server/old client compatibility suites.

### Live test-cluster acceptance

- Exact migration revision and image digest on every role.
- Shadow projection reaches zero unexplained identity mismatch and no
  destructive proposal for legacy-confidence rows.
- Controller and executor deletion during each action boundary creates neither
  duplicate allocation nor lost retry.
- Explicit teardown retains the allocation until exact provider absence.
- Serve replacement maintains configured ready capacity and drain guarantees.
- Pool jobs never tear down shared workers and concurrent placements never
  over-allocate a worker.
- Dedicated jobs recover once under controller handoff and clean every
  allocation generation at terminal state.
- Rollback returns authority to the legacy path without creating a second
  actuator.
- Provider failure or one corrupt group leaves unrelated workspace/provider
  partitions converging.
- Final namespace has healthy declared release resources and no test workload,
  stale migration Job, failed Helm revision, terminating pod, provider
  resource, or load balancer residue.

### Operational backup and repair

Before the first authority handoff, export and SHA-256 digest the exact capacity
groups, intents, desires, authority, actions, attempts, observations, members,
occupancies, and corresponding legacy rows for the canary scope. The runbook
contains read-only drift, action, observation-age, deletion-proof, occupancy,
and authority queries plus fenced commands to request handoff, retry,
quarantine, and audited adoption. Direct database edits and name-only provider
deletion are not supported repair procedures.

## Verification evidence

Baseline source:

- branch base: `3464ffada`;
- implementation branch: `feat/unified-physical-capacity`, developed in an
  isolated worktree without modifying the original dirty worktree;
- PR #1070 request/action and controller-generation foundations are present;
- PR #1071 ownerless Serve provider-refresh fallback is present.

Revision-001/C1 implementation evidence on 2026-07-30:

- `capacity_state_db` revision `001`, independent runtime metadata, the lazy
  shared-engine repository, closed row enums, strict disabled-by-default
  configuration, and bounded generic canonical JSON were implemented. No
  production projector, writer, reader cutover, observation, action,
  authority, permit, or occupancy path exists.
- A disposable local PostgreSQL 14.23 instance executed the literal migration
  and independent runtime metadata. Their catalog tables, columns, defaults,
  constraints, indexes, deferrability, and delete actions matched exactly.
- Real-PostgreSQL tests passed for the deferred group/intent cycle, global
  active cluster-generation uniqueness, A-to-B-to-A intent history, immutable
  intent/desire collisions, cross-workspace foreign-key rejection, idempotent
  concurrent publication, scan provenance, empty-only downgrade, and
  `ACCESS EXCLUSIVE` locking of each of the five tables.
- The actual Alembic lineage passed `upgrade`, `bootstrap`, verify-only
  startup, missing-lineage refusal, concurrent first initialization, and
  later-additive numeric revision compatibility.
- A fresh Python subprocess bootstrapped the shared PostgreSQL schema with
  global state first and all central histories present:
  `state_db=027`, `sky_config_db=001`, `serve_db=031`,
  `spot_jobs_db=026`, `api_requests_db=004`, and
  `capacity_state_db=001`. Success proves capacity did not preempt the
  global-state empty-schema bootstrap check; mocked ordering separately proves
  capacity is invoked last.
- The focused command covering capacity models, repository, real PostgreSQL
  schema, and central migration integration exited zero. Running the schema
  and repository modules with `pytest -n 2 --dist loadgroup` also exited zero
  with 34 tests and proved both modules share one xdist group when an external
  test URI is used.
- The repository formatter completed YAPF and isort; mypy passed all 797
  checked source files; pylint reported 10.00/10; and dashboard lint/format
  passed. Final commands were rerun on the staged commit candidate.
- Every fixture and temporary schema was removed after validation; the
  disposable PostgreSQL `public` schema ended empty.

Revision-001/C1 isolated deployment evidence on 2026-07-31:

- The deployed code commit is
  `23a7c632ccf9811d29c62a098d11d56c8c2cc412`. The exact Linux/amd64 image
  reports version `1.1.927`, build `7968`, OCI revision equal to that full
  commit, and config ID
  `sha256:27fa2c35589474ba1785ad8a1b1cd1b4ea05636d8468acc18a09c52edabd2a88`.
- The commit-specific tag `test-capacity-c1-23a7c632c` was first published to
  `255203429798.dkr.ecr.us-east-1.amazonaws.com/skypilot-nightly-boltz`.
  That cross-account repository had neither a repository pull policy nor a
  replication rule, and the release had no image-pull Secret. Rather than
  widen IAM during the rollout, the same manifest was copied without rebuilding
  to the test account's existing immutable `skypilot-ha` repository. Both
  registries returned the identical manifest digest
  `sha256:4310ff0de03aa9e2d193733b463a62e96ef97cc0d59e8f2d5bf087e78987cbac`;
  the release uses the same-account digest-qualified reference.
- The packaged chart SHA-256 was
  `ad803ece8c15eed01eed86b51376dbecd192167f6f0a52c33eeeceb953cc604b`.
  The secret-safe database audit program SHA-256 was
  `6e830229237e341ac2d60844575258339cff84a7fd6c6683b0eac3dbd3ba2b3f`.
- STS resolved the Kubernetes operator to account `361913687221` and the
  first artifact registry operator to account `255203429798`. The exact target
  was active EKS cluster
  `arn:aws:eks:us-east-1:361913687221:cluster/boltz-platform-test-eks-cluster`,
  Kubernetes `v1.33.13-eks-8f14419`. Its endpoint is private-only, so a
  transient SSM port-forward through an existing managed node was used; no EKS
  endpoint, security-group, instance, or route configuration was changed.
- Baseline Helm revision `34` was deployed with PostgreSQL request storage,
  RWX state, blocking migrations, two API/executor/controller replicas,
  RollingUpdate, and three healthy PDBs. `global.imageRegistry` was unset,
  managed image workers were disabled, and capacity mode/allowlist variables
  were absent. The initial audit observed one unrelated launch in progress, so
  the rollout waited. The final preflight at `2026-07-31T03:22:50Z` had no
  active cluster, unresolved managed job, Serve entity, non-periodic request,
  or timeout-like request anomaly.
- The final preflight database revisions were `state_db=027`,
  `sky_config_db=001`, `serve_db=031`, `spot_jobs_db=026`,
  `api_requests_db=004`, `kv_cache_db=001`, and no capacity lineage or
  capacity tables. A Helm server-side dry run projected revision `35`, the
  revision-scoped pre-upgrade migration Job, and the exact digest. A parsed
  comparison of the nine retained Helm resources found no non-image manifest
  change.
- Helm's blocking `pre-upgrade` hook started at
  `2026-07-31T03:23:26.601017887Z` and completed successfully at
  `2026-07-31T03:23:37.125927219Z`. The Kubernetes Job was labeled for release
  `skypilot-ha`, component `database-migration`, revision `35`, used the exact
  digest, and exited zero. Helm hook ordering causally completed the migration
  before applying the Deployment updates; every target-image pod has a
  Kubernetes creation timestamp at or after `2026-07-31T03:23:37Z`.
- Surge pods initially waited for CPU and memory. One new API pod had a
  transient ConfigMap-cache `FailedMount`, and expected startup and draining
  probe warnings occurred. Karpenter supplied nodes, then evicted one executor
  at `03:27:23Z` and one API pod at `03:28:04Z` as `Underutilized`. During the
  executor replacement its PDB briefly had one healthy pod, one desired
  healthy pod, and zero disruptions allowed. Observed Deployment availability
  remained two for every role, so there was no observed role-availability
  loss, image-pull error, OOM, migration failure, or Helm failure.
- Helm revision `35` reached `deployed` at `03:27:17Z` with description
  `C1 unified capacity foundation 23a7c632c`. By `03:27:36Z`, six
  API/executor/controller pods were Running and Ready with zero restarts and
  exact image IDs at the target digest. Each Deployment was `2/2` updated,
  Ready, and available; each PDB had two healthy pods, one desired healthy pod,
  and one disruption allowed. Six stability samples from `03:29:37Z` through
  `03:31:33Z` were unchanged.
- Both API replicas returned HTTP 200, API version `64`, version
  `1.1.927`, build `7968`, and the exact C1 commit. API, executor, and
  controller processes each loaded capacity mode `disabled` with no allowlist.
  A live SDK/CLI request-status call through the API service succeeded.
- The post-rollout database revisions preserved every pre-existing lineage and
  added only `capacity_state_db=001`. All five capacity tables existed with
  zero rows. After removing timestamps, periodic lease timestamps/generations,
  the expected capacity lineage, and the five expected tables, the bounded
  pseudonymized pre/post non-capacity audit reports were byte-identical. The
  post-audit still had no active test workload or timeout-like anomaly.
- After its object and empty log were captured, only migration Job `35` was
  deleted. No revision-35 migration pod, failed or terminating pod, canary,
  test workload, or unexpected release resource remained. The pre-existing
  successful revision-34 migration Job was not modified and remains governed
  by its existing 86,400-second TTL.

For history, the 2026-07-30 pre-authentication read-only audit found:

- the configured shared API health endpoint returned HTTP 200;
- the server reported version `1.1.924`, commit `915d020a3`, and API version
  64, two commits behind this branch base;
- exact `skypilot-ha` Helm revision, image digest, database revisions, and pod
  state were not inspectable because every relevant AWS SSO session had
  expired and this host lacked Kubernetes/Helm tooling; and
- no shared or test-fleet schema, image, or deployment was changed by this
  project; only the disposable local PostgreSQL database was exercised;
- an isolated Python 3.14.3 virtual environment installed the editable
  all-cloud package and development requirements successfully; and
- the baseline command
  `pytest -n 0 -q tests/unit_tests/test_migration_utils.py
  tests/unit_tests/test_sky/utils/test_db_utils.py` passed with
  `SKYPILOT_DEBUG=1` and `SKYPILOT_DEV=1`.

## Open gates

1. C1/C2 share the ordinary engine namespace and existing controller
   leadership. Connection and lock-wait behavior must be measured before any
   capacity-specific namespace or lock session; if later justified, account
   for one additional strict pool per process/role.
2. C2 production writers require accepted literal v1 payload/source mappings;
   revision `001` alone authorizes only empty schema and generic codec tests.
3. Each revision `002`-`004` requires its literal DDL and another exact-file
   adversarial acceptance before implementation.
4. AWS, GCP, and Kubernetes tag propagation must preserve provider tag limits,
   existing selectors, billing markers, and old-resource discoverability.
5. Authoritative teardown cannot begin until the action journal, typed
   outcomes, and exact provider identity are live and shadow-tested.
6. Non-consolidated managed jobs remain a declared migration exception until a
   durable central intent feed has its own accepted design and deployment.
7. C4 cannot begin unless the quantified C2 value gate passes.
8. Before every remaining code-bearing stack deployment, an approved
   exact-feature-commit artifact or reviewed merge artifact must exist and its
   source commit/digest must be proved. The ordinary publisher still accepts
   only `improvements`; the C1 test-only exact-image procedure does not relax
   that branch guard.
9. The C1 rollout required Karpenter surge capacity and then experienced
   consolidation churn. Before the C2 rollout, preflight must prove eligible
   surge headroom or explicitly test and review the one-replica availability
   window and controller-leader handoff of a `maxSurge: 0`,
   `maxUnavailable: 1` strategy. It must also prove Karpenter consolidation
   cannot compound that strategy's zero-disruption-margin interval, and must
   not discover the choice after the migration hook runs.

## Adversarial review record

### Review 1: RESHAPE

The first review rejected the mutable five-table draft before implementation.
It found that mutable group specs erased old intent meaning, authority handoff
was asserted but not represented, action attempts were overwritten, complete
zero-member observations could not be proved, pool release and cross-lineage
transactions were underspecified, tenancy was buried in JSON, legacy exit and
GC were incomplete, and the removal ledger incorrectly proposed deleting cost
attribution.

This revision adds immutable intent and desire rows, per-verb persisted
authority handoff, append-only attempts with request IDs minted before submit,
complete exact-scope observation runs, scalar workspace fencing, one-session
occupancy transactions, positive release evidence, finite legacy exits,
retention/GC rules, failure isolation, a corrected keep/remove/test ledger, and
the quantified C2 stop/go gate. It also specifies the PostgreSQL-only lineage
without creating a new database engine or pool.

### Review 2: RESHAPE

The second review found four common-model blockers and four Serve/pool
blockers: benign intent carry-forward could strand actions; quiescing forbade
the reconciliation rollback required; request reservations were incorrectly
described as enqueue-time and left a crash gap; retirement/proof retention was
not representable; heterogeneous allocations needed per-allocation specs;
paid/reserved policy could be bypassed; release evidence lacked routing/drain
proof; and global actuator deletion contradicted supported SQLite and
non-consolidated modes.

Source audit then found an additional foundational error: Serve lifecycle epoch
and API controller generation are mutable writer fences, not stable owner
incarnations. This revision separates owner incarnation from writer fence,
limits literal revision `001` to five read-projection tables, defers stable
desire transitions/actions/occupancies to later reviewed revisions, adds
per-allocation specs, atomic request/queue+attempt submission and pre-effect
claim, exact observation retention, workload readiness/LB/drain evidence,
paid/reserved/demand permits including conserved reserved holdings, exact
task/recovery/exec occupancies, and an honest central-versus-global removal
ledger.

### Review 3: PURSUE

Three independent reviews accepted exact contract SHA-256
`b405be542d3e56e832731fce3121c8389a4e2b0eed833dfde248698990a8338a`
for C0 and literal revision-001/C1 implementation. The accepted C1 is five
empty additive projection-foundation tables, typed row enums, a bounded generic
canonical codec, and repository/migration tests. It has no production writer,
projector, read cutover, provider observation, action, authority, permit, or
occupancy behavior.

The reviewers confirmed stable owner versus mutable writer fences, global
cluster-generation uniqueness, A-to-B-to-A intent monotonicity, cyclic
group/intent deletion semantics, scan provenance, PostgreSQL/default-engine
initialization, SQLite/mode refusal, and later-phase gates. At review time,
live deployment was blocked on the documented test-fleet preflight and exact
artifact, not on C1 coding; the 2026-07-31 evidence above records satisfaction
of that deployment gate. This review-record/status amendment is
non-contractual; the hash above is the exact reviewed behavioral and schema
contract immediately before the verdict was recorded.

### Review 4: PURSUE

An independent implementation review accepted the revision-001/C1 commit
candidate after its first pass required five corrections: serialize both
real-PostgreSQL modules under the same xdist group; add explicit
cross-workspace foreign-key and immutable intent/desire tests; exercise a
fresh-process bootstrap of every central lineage; reject future row schema
versions from the v1 fixture repository; and replace the borrowed `intent`
domain for unhashed scan counters with domain-neutral bounded encoding.

The re-review found no remaining correctness or contract blocker. It confirmed
literal migration/runtime catalog parity, empty-only locked downgrade,
default-engine and PostgreSQL-only initialization, fail-closed modes, absence
of a production writer, and the final verification evidence above. Deployment
of the exact code-bearing commit was the remaining gate before C2 at review
time; the 2026-07-31 rollout satisfied it without waiving test-fleet preflight
or exact-artifact provenance for later commits.
