# Unified Physical-Capacity Evidence Scan

Status: C1 foundation implemented and verified; C2 evidence-scan contract
accepted; C2.1 implementation and local verification recorded with an exact
removal ledger; C2.2 disabled deployment verified on the test EKS cluster;
C2.3 shadow activation is blocked on a real Serve selector and independent
provider-call audit evidence, and every materialized or authoritative capacity
phase remains blocked

Last updated: 2026-07-31

Canonical owner: this file. Rejected materialized/active-control drafts remain
recoverable from branch history and the review record; they are not a second
implementation backlog.

## Decision

The research claim that unified physical-capacity convergence has the
“largest long-term payoff” is a hypothesis. It combines four different
products:

1. a shared read inventory;
2. a shared provider-observation cache;
3. a durable per-allocation action journal; and
4. a pool occupancy ledger.

The first C2 step is not any of those products. It is a bounded evidence scan:
pure adapters read explicitly selected Serve services, Serve pools, and
consolidated managed-job tasks; construct one ephemeral normalized inventory;
emit low-cardinality metrics; and persist only one scan summary and one
deterministic digest per selector partition in the existing C1
`capacity_projection_scans` table.

C2 does not write `capacity_groups`, `capacity_group_intents`,
`capacity_allocations`, or `capacity_allocation_desires`. It does not add
revision `002`, reconcile absence, persist selected specs, create intent
history, or expose a reader. There is no consumer that justifies those costs
before the value hypothesis is measured.

A `cluster_hash` is exact registry-generation evidence. Its attribution to a
Serve replica, pool worker, or managed task is best-effort and remains legacy
evidence because current source and registry writes were not atomic. C2 never
adds identity transport merely to improve its own coverage.

Products 1–4 require separate accepted designs after the value gate below.
The previously proposed admission controls, Helm proxy, quota reservations,
action tokens, compatibility floor, provider actions, and source-table
migrations are not authorized.

## Why convergence could pay off

If production evidence clears a gate, the likely restructuring is a narrow
shared kernel with separate responsibilities:

```text
Serve ─┐
Jobs  ─┼─ owner adapters ─ physical-generation identity
Pools ─┘                         │
                 ┌───────────────┼────────────────┐
          observation cache  action journal  occupancy ledger
```

- A shared observation cache could deduplicate provider reads and give all
  owners one freshness/certainty model.
- An action journal could make stop/down/retry idempotent across controller
  crashes and record which owner authorized each effect.
- An occupancy ledger could make pool-worker assignment transactional instead
  of inferred from mutable links.
- Exact pre-effect physical identity could prevent a stale owner from acting
  on a same-name successor.

Those benefits do not require one component to own Serve, jobs, and pools.
Each workload remains the source of logical desired state. Shared code would
own only physical-generation identity, observation, action durability, or
occupancy, with an explicit contract between those layers.

## Payoff hypothesis and go/no-go gates

Shadow begins with a canary, not the measurement window. The canary starts
with one service, exercises restart/handoff, then adds the intended pool and
managed-task selectors. It lasts at most three days and is excluded from gate
arithmetic. After canary acceptance, the DRI freezes one measurement manifest
containing the complete typed-selector array, every partition dependency/scope
hash, missing-signal declarations, `measurement_start_utc` at a UTC-day
boundary, and `measurement_end_utc = start + 30 days`. The fixed pilot end must
be at or after that measurement end.

The same frozen manifest cohort is then observed for the 30 complete UTC days
immediately preceding `measurement_start_utc`, where retained evidence exists,
and the 30 complete UTC days beginning at it:

- orphaned or uncertain-delete accelerator-hours and estimated cost;
- physical-lifecycle incidents, manual repair hours, MTTD, and MTTR;
- duplicated provider reads by caller path;
- duplicated or ambiguous provider mutations;
- pool assignment contention and reproduced over-allocation; and
- duplicated physical retry/action implementations and defects.

A selector or dependency/scope-hash change after freeze invalidates the
measurement window; rows from another scope are never combined. The DRI may
restart the baseline and 30-day clock only if the immutable pilot end still
contains a full replacement window. Otherwise the gate is unavailable and C2
expires. Canary observations cannot backfill baseline evidence.

The materiality thresholds remain:

- avoidable leakage is at least 100 accelerator-hours or $5,000 per month, or
  one percent of managed-compute spend in the same cohort;
- shared lifecycle failures cause at least two incidents or four
  engineer-hours of repair per month;
- one common observation pass can remove at least 25 percent of provider reads
  while preserving freshness SLOs; or
- a database occupancy ledger eliminates a reproduced assignment race or
  over-allocation.

They are product-specific permission to submit a new canonical design, not
blanket implementation authority:

| Follow-on product | Required causal gate |
| --- | --- |
| Shared read inventory or exact identity transport | The leakage or lifecycle threshold, evidence that missing cross-owner identity/inventory caused the measured harm, at least two owner kinds, and a named read consumer. |
| Provider-observation cache | The 25-percent provider-read threshold for at least two owner kinds, with the proposed common pass shown to preserve each freshness SLO. |
| Durable action journal | The lifecycle threshold specifically attributed to duplicated/ambiguous mutation or crash-retry behavior across at least two owner kinds. |
| Pool occupancy ledger | The deterministic competing-assignment reproducer and proof that the proposed transaction eliminates that race or over-allocation. |

Passing one row authorizes review only of that row's product. It does not
authorize another product, a schema migration, or mutation authority.
Projection parity and identity-gap counts alone do not prove payoff.

The design DRI owns one append-only decision record. Before measurement freeze
it records canary dates, cohort start/end UTC, the canonical selector and all
partition scope hashes, commit and image digest, database revisions, query
text and SHA-256, dashboard permalink, incident IDs, evidence owners,
denominators, and missing-data intervals.
Unavailable baseline signals cannot satisfy a gate and are not reconstructed
from C2 summaries.

Evidence ownership is fixed:

- the API-server operator exports provider calls by existing caller path and
  operation, with selector-days as denominator;
- cloud-finops joins documented incident cluster hashes only to retained
  billing/usage and `cluster_history.usage_intervals`;
- Serve and jobs maintainers jointly classify incident and repair records;
- the pool maintainer supplies a deterministic competing-assignment
  reproducer; and
- the design DRI records duplicated implementations by commit and symbol.

Two reviewers other than the DRI sign the arithmetic, cohort hash, missing
windows, numerators, and denominators. Because C2 changes no authority, the
record cannot claim that C2 caused a cost or incident reduction.

## Goals

1. Measure whether selected Serve, pool, and jobs state can be normalized into
   the same physical-generation vocabulary.
2. Quantify exact group identity, legacy physical association, missing
   identity, scalar placement coverage, status intent, and pool-link
   ambiguity.
3. Keep every record ephemeral except a bounded summary and digest.
4. Prove source reads are deterministic, indexed, HA-fenced, and free of
   provider calls and source writes.
5. Stop automatically at the end of the pilot.

## Non-goals

- No durable group, allocation, intent, desire, selected-spec, or occupancy
  row.
- No source-table mutation, write hook, token, or backfill.
- No provider API call or provider-backed absence/deletion assertion.
- No launch, start, stop, down, retry, cleanup, adoption, or placement.
- No reverse registry scan or orphan/leakage claim.
- No admission webhook, Helm proxy, quota reservation, compatibility floor, or
  API-version bump.
- No public SDK, CLI, dashboard, or capacity read path.
- No SQLite or non-consolidated/controller-local mode.
- No claim that a cluster name identifies a physical allocation.

## Operator contract

There is no public API. `disabled` remains the default. Shadow is enabled only
on an explicit split `controller` process with all three variables:

```text
SKYPILOT_PHYSICAL_CAPACITY_MODE=shadow
SKYPILOT_PHYSICAL_CAPACITY_SOURCES_JSON=<strict selector array>
SKYPILOT_PHYSICAL_CAPACITY_PILOT_END_UTC=<RFC3339 UTC timestamp>
```

Shadow requires the PostgreSQL request backend, consolidated Serve and jobs
state in the same normalized PostgreSQL database/default schema, C1 capacity
revision `001`, and a current API-controller instance/generation. The pilot
end uses the exact form `YYYY-MM-DDTHH:MM:SSZ` with no fraction/offset.
Parsing validates only that syntax. After every leadership acquisition and
before the daemon starts, one bounded activation query reconstructs all C2
mapping-version-1 rows and their durable union of `(workspace, source_kind)`
partitions:

- if no C2 scan row exists, the end must be later than the activation
  transaction time and no more than 35 days later;
- otherwise, every row must carry the same `pilot_end_utc`, and the configured
  end must equal it;
- the union of historical and currently configured partitions must contain at
  most 16 pairs; removing a selector or partition never frees a pilot slot;
  and
- an equal end at or before activation is valid restart state: the controller
  remains healthy, the projector emits its expiry metric, may finalize a stale
  running row, and starts no new partition scan.

The first inserted running scan is therefore the global durable activation
anchor. Changing selectors under that fixed end changes output scope, and new
partitions may be staged only while the durable union remains at most 16.
Changing or extending the end is rejected even though it would produce another
scope hash. A later pilot needs a new accepted mapping/revision rather than
deletion or reinterpretation of rows. At or after the fixed end the daemon
starts no new partition scan. Migration, API, executor, compatibility `all`,
and SQLite processes must use `disabled`; activation therefore sets these
variables only on the controller workload.

Modes `observe|teardown|serve|jobs` remain rejected. An old revision-`001`
binary ignores the two new C2 variables and, in `shadow`, still has no writer.

`SKYPILOT_PHYSICAL_CAPACITY_SOURCES_JSON` is exactly one top-level JSON array.
An object, scalar, nested selector array, unknown key, duplicate canonical
selector, non-standard constant, Boolean integer, or invalid UTF-8 is
rejected. Raw UTF-8 is capped at 64 KiB and the array at 1,000 selectors:

```text
{"workspace": string,
 "source_kind": "serve_service" | "serve_pool",
 "service_name": string}

{"workspace": string,
 "source_kind": "managed_job_task",
 "spot_job_id": positive integer,
 "task_id": non-negative integer}
```

Strings use existing workspace/source bounds. Selectors group into at most
16 `(workspace, source_kind)` partitions and sort by canonical bytes. Existing
non-empty workspace and owner-kind allowlists must admit every configured
selector or configuration is rejected; C2 does not silently filter
selectors. Provider and
verb allowlists are ignored by C2 but included in its scope hash. A non-empty
group allowlist is rejected in shadow because typed selectors already define
the cohort and a missing source has no derivable group ID. A missing selected
source is reported as `selector_missing`; it is never treated as capacity
absence and never matched to an old group.

The required source-schema minima are global-user-state `027`, Serve `031`,
managed jobs `026`, and API requests `004`. Startup verifies the referenced
tables, columns, types, primary/index leading columns, and C1 capacity shape.
A later source revision is accepted only if that exact catalog contract still
matches.

Validation is deliberately split around existing runtime initialization.
Strict JSON/timestamp syntax, process role, PostgreSQL request backend, and the
incremental connection budget are checked during common initialization.
Consolidation mode is checked only after
`managed_job_utils.setup_consolidation_mode_on_startup()` has run. URI/default-
schema co-location, revision/catalog shape, the global activation anchor, and
current leadership are checked after leadership acquisition and before the
isolated engine performs its first source scan. A failure keeps the controller
unready and releases leadership; migration initialization does not guess
consolidation state.

## C1 persistence contract

C2 uses capacity revision `001` unchanged. Only
`capacity_projection_scans` is written. The other four C1 tables remain empty.
There is no migration or new index.

`source_partition_hash` is the revision-`001` canonical
domain-`source_partition` hash of:

```text
{"mapping_version":1,
 "workspace":<workspace>,
 "source_kind":<source_kind>}
```

Raw arrays are not embedded in a 64-KiB canonical envelope. C2 adds the closed
canonical domain `scope_entry`. The selector dependency set for a Serve
partition is exactly the selectors in that `(workspace, source_kind)`
partition. For a managed-job partition it is those managed-job selectors plus
all configured `serve_pool` selectors in the same workspace, because only
those selectors can change its pool-assignment diagnostic. Selectors in other
workspaces and service selectors cannot affect that partition and are
excluded.

For each of `source_selectors`, `owner_kinds`, `providers`, `groups`, and
`verbs`, normalized records are encoded as:

```text
{"mapping_version":1,
 "component":<closed component name>,
 "value":<strict selector object or closed string>}
```

Within a component, records sort by their full unsigned canonical-envelope
bytes. The selector component contains exactly the dependency set above.
`owner_kinds`, `providers`, `groups`, and `verbs` contain their complete
normalized configured allowlist components; empty components still have a
count and digest. The current workspace is already a scalar in the scope
payload; unrelated workspace-allowlist entries are not semantic inputs after
startup has proved that the current workspace is admitted.

The component digest is SHA-256 over
`ASCII("skypilot-capacity-scope-component-v1")`, one NUL byte, the component
name length as unsigned 64-bit big-endian, its UTF-8 bytes, the record count
as unsigned 64-bit big-endian, and each canonical-envelope length as unsigned
64-bit big-endian followed by its bytes.

The compact domain-`source_partition` scope payload is:

```text
{"mapping_version":1,
 "workspace":string,
 "source_kind":"serve_service"|"serve_pool"|"managed_job_task",
 "pilot_end_utc":"YYYY-MM-DDTHH:MM:SSZ",
 "source_selectors":{"count":integer,"hash":lowercase SHA-256},
 "owner_kinds":{"count":integer,"hash":lowercase SHA-256},
 "providers":{"count":integer,"hash":lowercase SHA-256},
 "groups":{"count":integer,"hash":lowercase SHA-256},
 "verbs":{"count":integer,"hash":lowercase SHA-256}}
```

Its hash is `projection_scope_hash`. SHA-256 preimage collisions are outside
the threat model; C1 does not retain every preimage.

A fresh UUIDv4 scan starts with cursor:

```text
{"mapping_version":1,
 "phase":"full_snapshot",
 "projection_scope_hash":<hash>,
 "pilot_end_utc":"YYYY-MM-DDTHH:MM:SSZ",
 "scheduled_slot_utc":"YYYY-MM-DDTHH:MM:SSZ",
 "inventory_digest":null}
```

Completion replaces only `inventory_digest` with the lowercase SHA-256
defined below, stores exact `rows_seen` and finding counts, and changes the
same scan row to `completed`. A failed scan retains a null digest. Report
queries use the existing completed-scan index, exact workspace/source kind,
the 35-day time range, and a hard limit of 4,000 rows before filtering cursor
scope hashes in memory.

The activation query is two-phase and ordered by the scan primary key with a
hard limit of 53,777. Phase one streams batches of at most 256 containing only
`scan_id`, `octet_length(workspace)`, `octet_length(source_kind)`,
the fixed Boolean `source_kind IN (...)`, `cursor_schema_version`, and
`pg_column_size(cursor)`, plus a fixed Boolean proving a non-null controller
UUID/positive-generation pair; it never returns workspace or cursor contents.
It rejects more than 53,776 rows, a workspace over its 256-byte bound, a
non-closed source kind, an invalid controller pair, or a cursor over 4 KiB.
Phase two point-fetches only approved scan-ID batches and returns workspace,
source kind, `source_partition_hash`, the exact-cursor-validity Boolean, and
the text extractions of `projection_scope_hash`, `pilot_end_utc`, and
`scheduled_slot_utc`; it never returns the JSONB cursor.

A fixed SQL Boolean proves the cursor has exactly the six keys shown above.
It uses the supported JSONB all-keys-exist operator and proves that subtracting
that fixed text-array key set leaves the empty JSONB object; it does not rely
on a nonexistent object-length function. `jsonb_typeof` and typed JSONB
equality, rather than `->>` coercion, require numeric mapping version `1`,
string phase `full_snapshot`, lowercase 64-character projection hash, exact
20-byte timestamp strings matching the operator grammar, and an
`inventory_digest` that is JSON null for `running|failed` or a lowercase
64-character string for `completed`. Application decoding recomputes and
matches `source_partition_hash`, validates both timestamps completely, and
rejects a false Boolean or any unexpected scalar. A generic cursor that merely
spells `"mapping_version":"1"` therefore cannot become a C2 anchor.

While streaming, activation recomputes each partition's slot jitter, requires
every `scheduled_slot_utc` to be aligned to that partition's 900-second
lattice, and requires it to fall in the half-open interval
`[pilot_end_utc - 35 days - 900 seconds, pilot_end_utc)`. The tuple
`(workspace, source_kind, scheduled_slot_utc)` must be unique across all rows,
and each durable partition may have at most 3,361 rows. These checks, together
with the 16-partition union, prove both the 4,000-row report completeness bound
and the 53,776-row global bound; a conforming-looking generic row set cannot
bypass them.

Both phases use server-side streaming, a 4-MiB batch cap, a 64-MiB combined
serialized-input/retained-state cap, the isolated connection, fixed local
timeouts, and one 30-second deadline plus external watchdog. A timeout or
bound failure rejects activation and controller readiness. More than 53,776
rows, a non-C2 cursor, a second end, or a seventeenth durable partition rejects
shadow activation. Revision C1 has no production writer; any pre-existing
generic/test cursor therefore blocks shadow rather than being silently
reinterpreted. After the bounded read transaction commits, a short
`READ COMMITTED` transaction takes the controller leadership row `FOR SHARE`
and re-proves the live generation before the immutable activation snapshot is
handed to the daemon. Activation runs only after leadership acquisition and
before its daemon starts; a failed proof discards the snapshot. Rolling
controllers with different immutable environment snapshots therefore cannot
validate and write two disjoint unions concurrently. A configured different
end is rejected rather than treated as another cohort. No runtime reader uses
the summaries.

## Ephemeral evidence vocabulary

C2 adds closed canonical domain `evidence_record`. It constructs one group
record per live selected owner and zero or more physical-allocation candidate
records. Records contain only hashes and closed classifications:

```text
{"mapping_version":1,
 "record_type":"group",
 "source_incarnation_hash":lowercase SHA-256,
 "confidence":"exact"|"legacy"|"unknown",
 "lifecycle":"active"|"retiring"|"unknown",
 "status_class":"present"|"absent"|"unknown"}

{"mapping_version":1,
 "record_type":"allocation_candidate",
 "source_incarnation_hash":lowercase SHA-256,
 "group_source_incarnation_hash":lowercase SHA-256,
 "identity_confidence":"legacy"|"unknown",
 "association_status":"registry_hash"|"registry_missing"|"registry_unsafe"|
                      "source_malformed",
 "desired_state":"present"|"absent"|"unknown",
 "observed_state":"unknown"|"provisioning"|"up"|"stopped"|"partial",
 "scalar_placement_hash":lowercase SHA-256|null}
```

The group and allocation source-incarnation payloads are specified in the
source mappings below and hashed with domain `source_incarnation`. Scalar
placement uses domain `physical_spec`. No raw source identifier, cluster name,
cluster hash, provider, region, zone, task name, status, YAML, blob, or
exception text is persisted or logged.

Evidence records sort by `(record_type UTF-8 bytes,
source_incarnation_hash ASCII bytes, full canonical record bytes)`.
`inventory_digest` is SHA-256 over
`ASCII("skypilot-capacity-evidence-v1")`, one NUL byte, the record count as
unsigned 64-bit big-endian, and each canonical record length as unsigned
64-bit big-endian followed by its bytes. Equal input snapshots therefore
produce equal digests regardless of database row order.

Every completed scan stores exactly these non-negative integer keys:

```text
source_rows
selectors_present
selectors_missing
groups_exact
groups_legacy
groups_unknown
allocation_candidates
allocations_exact
allocations_legacy
allocations_unknown
identity_gap
no_cluster_yet
scalar_placement_known
selected_spec_gap
desired_present
desired_absent
desired_unknown
source_conflict
pool_assignment_unfenced
pool_assignment_ambiguous
```

`source_rows` counts decoded `services`, `replicas`, `job_info`, and `spot`
rows. Counts describe logical database rows, not SQL result packets or query
phases. Each partition has negative-result-aware caches keyed by full stable
key for `services`, replica prefixes, `job_info`, `spot`, `clusters`, and
`cluster_history`. A logical row returned by the two-phase length/value read
counts once. `source_rows` counts each distinct decoded source-table row once;
`rows_seen` counts those rows plus each distinct returned registry/history row
once. Repeated candidate enrichment, repeated missing point lookups, and
repeated pool diagnostics reuse the cache. The row and byte budgets charge a
row/value only when that logical row is first materialized. A Serve selector
is present when its service row exists and matches kind/workspace. A managed
selector is present only when its one job row and one logical task row exist.

The arithmetic is closed:

| Source disposition | Group count | Allocation-candidate count |
| --- | --- | --- |
| selected source missing | `selectors_missing += 1` | none |
| live owner | exactly one of `groups_exact|legacy|unknown` | source-kind rules below |
| Serve `PENDING` with null/empty cluster name | group only | `no_cluster_yet += 1`; none |
| managed `PENDING` with no dedicated current cluster | group only | `no_cluster_yet += 1`; none |
| pool-assigned managed task | group only | none; pool counters only |
| every other Serve replica or dedicated managed task | group already counted | exactly one candidate |

For every completed partition:

```text
selectors_present + selectors_missing = configured selectors
groups_exact + groups_legacy + groups_unknown = selectors_present
allocations_exact + allocations_legacy + allocations_unknown
    = allocation_candidates
desired_present + desired_absent + desired_unknown
    = allocation_candidates
identity_gap = selected_spec_gap = allocation_candidates
scalar_placement_known <= allocations_legacy
```

For every candidate, exactly one of `allocations_legacy|unknown`, exactly one
of `desired_present|absent|unknown`, `identity_gap += 1`, and
`selected_spec_gap += 1`. `allocations_exact` is always zero in mapping
version 1. A safe current registry-hash association is legacy; every other
association is unknown. `scalar_placement_known` increments only for a legacy
candidate with the complete safe scalar payload below. One source row creates
at most one candidate even if historical capacity may have had predecessors;
C2 has no durable predecessor knowledge.

Primary fields needed to create a stable evidence identity are strict.
Negative Serve replica IDs, non-positive managed `spot.job_id`, invalid
selector-key echoes, or an out-of-bound identifier fail the whole scan with
`source_decode_failed`; they are not converted into an unhashable unknown
record. Malformed controller fences, statuses, attribution, or association
fields whose record still has a valid primary identity use the closed unknown
classifications and fallback payloads below.

`source_conflict` increments once for each live owner or candidate classified
as conflicted because of an unrecognized status, malformed fence/identity,
duplicate hash claim inside that partition, or unsafe non-null attribution.
Ordinary missing registry evidence is unknown identity but not a conflict.
A group fence/status defect counts once for the owner. A replica/task
status or association defect counts once for that candidate; a candidate
whose desired state is unknown only because its parent service status was
unknown does not count the inherited defect again. A cross-workspace
registry/history row, duplicate logical managed task key,
selector kind/workspace contradiction, deterministic digest invariant
failure, schema mismatch, or bound failure fails the whole scan instead of
joining or summarizing cross-tenant data.

## Source mappings

### Serve service and pool

For each selector, C2 performs an indexed `services` primary-key read and a
`replicas` primary-key-prefix read. It never reads `version_specs`, a version
blob, or YAML.

The exact source columns are:

```text
services: name, workspace, status, pool, controller_pid,
          controller_port, controller_ip, hash, lifecycle_epoch,
          resource_scope
replicas: service_name, replica_id, status, version, cluster_name
```

Null Serve workspace is rejected. `pool=0` must match `serve_service`;
`pool=1` must match `serve_pool`. The group source-incarnation payload is:

```text
{"adapter":"serve",
 "mapping_version":1,
 "workload_kind":"serve_service"|"serve_pool",
 "workspace":string,
 "service_name":string,
 "service_hash":string|null,
 "resource_scope":string|null}
```

If `service_hash` or `resource_scope` is not null/string or exceeds the
canonical 4,096-byte string bound, the group instead uses:

```text
{"adapter":"serve",
 "mapping_version":1,
 "workload_kind":"serve_service"|"serve_pool",
 "workspace":string,
 "service_name":string,
 "source_identity_status":"source_malformed"}
```

That group is unknown and conflicted. Raw malformed values are neither hashed
nor retained.

Group confidence is exact only when service hash is non-empty,
`resource_scope == service_hash`, lifecycle epoch is positive, and the
existing controller-owner fingerprint recomputes from the stored
hash/PID/IP/port tuple. Missing exact-era fields whose present values are
well-typed and non-contradictory are compatible legacy evidence. A malformed
field, non-null scope unequal to service hash, or contradictory controller
tuple is unknown. No in-place confidence transition exists because no group
row is persisted.

Service `SHUTTING_DOWN|FAILED_CLEANUP` gives lifecycle `retiring`, status
class `absent`, and overrides every child desired state to `absent`. Every
other recognized ServiceStatus is lifecycle `active` and status class
`present`; an unrecognized service status has lifecycle/status class `unknown`
and makes each child desired state unknown.

Otherwise replica desire is:

- `PENDING|PROVISIONING|STARTING|READY|NOT_READY` -> `present`;
- `SHUTTING_DOWN|FAILED|FAILED_INITIAL_DELAY|FAILED_PROBING|
  FAILED_PROVISION|FAILED_CLEANUP|PREEMPTED` -> `absent`; and
- `UNKNOWN` or unrecognized -> `unknown`.

A candidate, including `PENDING` with a non-empty cluster name, is legacy only
when its non-empty cluster name point-looks up one current `clusters`
primary-key row with a non-empty `cluster_hash` that fits the canonical
4,096-byte string bound, `is_managed=1`, matching workspace, and no conflicting
non-null attribution against
`(service|pool, service_name, replicas.version)`. Null attribution remains
legacy evidence. Cross-workspace evidence fails the scan; other unsafe
evidence, including an oversized hash, creates a conflicted unknown
`registry_unsafe` candidate whose fallback payload never embeds that hash.

For every safe current row, registry status maps `INIT -> provisioning`,
`UP -> up`, `STOPPED -> stopped`, `AUTOSTOPPING -> partial`, and every other
value to `unknown`. Every unsafe/missing association has observed state
`unknown`. Registry observation never means provider absence.

The allocation source-incarnation payload is:

```text
{"adapter":"serve",
 "mapping_version":1,
 "group_source_incarnation_hash":lowercase SHA-256,
 "replica_id":non-negative integer,
 "cluster_hash":string}
```

For an unknown association, `cluster_hash` is replaced by the closed
`association_status` in this literal payload:

```text
{"adapter":"serve",
 "mapping_version":1,
 "group_source_incarnation_hash":lowercase SHA-256,
 "replica_id":non-negative integer,
 "association_status":"registry_missing"|"registry_unsafe"|
                      "source_malformed"}
```

Cluster name is never identity. Two selected replicas
claiming the same non-empty hash are unknown candidates and each increments
`source_conflict`; neither wins by row order.

### Consolidated managed jobs

Each selector performs an indexed `job_info` primary-key read and
`ix_spot_job_task(spot_job_id, task_id)` read. Exact columns are:

```text
job_info: spot_job_id, workspace, controller_instance_id,
          controller_generation, pool, current_cluster_name, is_batch
spot: job_id, spot_job_id, task_id, task_name, status
```

Missing either row is `selector_missing`. More than one task row for the
logical key fails the scan. Null workspace maps only to selector workspace
`default` and forces legacy group confidence. `job_info.is_batch` must be a
non-null Boolean; null is `source_decode_failed` because C2 cannot safely
decide whether a pool link is a Batch coordinator link.

The group source-incarnation payload is:

```text
{"adapter":"managed_jobs",
 "mapping_version":1,
 "workload_kind":"managed_job_task",
 "workspace":string,
 "spot_job_id":positive integer,
 "task_id":non-negative integer,
 "spot_row_id":positive integer}
```

The group is exact when IDs are valid and the complete controller
instance/generation pair is valid; for a nonterminal task the pair must equal
the projector's current controller generation. Both fields null is legacy.
A partial, malformed, or stale nonterminal pair is unknown.
Terminal or `CANCELLING` status gives lifecycle/status class
`retiring/absent`; processing status gives `active/present`; deprecated or
unrecognized status gives `unknown/unknown`.

For a non-pool task, the pure
`generate_managed_job_cluster_name(task_name, spot_job_id)` result is
point-looked up by the `clusters` primary key. A safe non-empty hash,
within the canonical 4,096-byte bound, managed flag, workspace, and
non-conflicting non-null attribution against
`("managed_job", str(spot_job_id), task_id)` create a legacy candidate.
Cross-workspace evidence fails the scan; other missing/unsafe evidence creates
an unknown candidate; an oversized hash is conflicted `registry_unsafe` and is
never embedded. The exception is `PENDING` with no current row, which is the
zero-candidate `no_cluster_yet` case.

The safe allocation source-incarnation payload is:

```text
{"adapter":"managed_jobs",
 "mapping_version":1,
 "group_source_incarnation_hash":lowercase SHA-256,
 "spot_row_id":positive integer,
 "cluster_hash":string}
```

An unknown association uses:

```text
{"adapter":"managed_jobs",
 "mapping_version":1,
 "group_source_incarnation_hash":lowercase SHA-256,
 "spot_row_id":positive integer,
 "association_status":"registry_missing"|"registry_unsafe"|
                      "source_malformed"}
```

Task name is lookup evidence only. Failure to derive the expected name is
`source_malformed`, including for `PENDING`; only a successfully derived name
whose primary-key lookup returns no row is `no_cluster_yet` for `PENDING`.

Managed desired state is:

- `PENDING` -> no candidate without a current association, otherwise present;
- `STARTING|RUNNING|WINDING_DOWN|RECOVERING` -> present;
- `CANCELLING` and every terminal status -> absent; and
- deprecated `SUBMITTED` or an unrecognized value -> unknown.

A task with `job_info.pool IS NOT NULL` never creates a managed allocation
candidate: its current cluster is a shared pool assignment, not job-owned
capacity.

### Scalar placement

For each legacy candidate, its exact cluster hash point-looks up
`cluster_history`. A scalar placement is safe only when `is_managed=1`,
workspace matches, non-null attribution does not conflict, and this payload is
valid:

```text
{"mapping_version":1,
 "evidence_kind":"registry_history_scalars",
 "provider":bounded non-empty string|null,
 "region":bounded non-empty string|null,
 "zone":bounded non-empty string|null,
 "node_count":positive integer|null,
 "shape_known":false}
```

At least provider or node count must be present. C2 selects only
`cluster_hash`, `workspace`, `cloud`, `region`, `zone`, `num_nodes`,
`is_managed`, `workload_type`, `workload_id`, and `workload_task_id`.
It never selects/decodes `launched_resources`, `requested_resources`,
`spot.full_resources`, a handle, YAML, or catalog data; never invokes a
`Resources` property, `__setstate__`, cloud adaptor, or Kubernetes context;
and never persists the scalar payload, only its canonical hash.

Every candidate still increments `selected_spec_gap`, because machine,
accelerator, CPU, memory, spot, disk, and network shape remain unknown.
Missing/malformed history scalars or non-managed history simply leave scalar
placement unknown. A different non-null history workspace fails the scan; a
conflicting non-null history attribution leaves placement unknown and
increments that candidate's `source_conflict`.

### Pool assignment diagnostic

For every non-Batch pool-assigned job with non-null
`current_cluster_name`, `pool_assignment_unfenced += 1`.

The link is unambiguous only if the current configuration contains the exact
same-workspace `serve_pool` selector named by `job_info.pool` and, within the
same managed-partition repeatable-read transaction:

1. that pool's service row is valid and its replica prefix is read once into a
   partition-local cache;
2. exactly one replica has the linked cluster name; and
3. that replica has the same safe current registry hash under the Serve pool
   association rules.

Otherwise `pool_assignment_ambiguous += 1`. Diagnostic pool rows count toward
the same row/byte/time bounds. Batch jobs and null links are excluded. No
capacity table, old scan, provider, or unindexed capacity lookup participates.

## Bounded read and publication algorithm

All tables must resolve to the same normalized PostgreSQL URI/default schema.
The dedicated connection sets
`application_name=skypilot-physical-capacity-evidence`.

Source normalization never locks the controller row. Its repeatable-read
transaction makes the non-locking current-leadership statement its first data
read, reads only source state, builds a bounded result, and commits. A separate
short `READ COMMITTED` publication transaction then executes
`current_controller_leadership_statement(..., lock=True)`, CAS-updates the
same running scan row, and commits. Generation advancement must update the
locked singleton row, so it occurs entirely before publication (and the proof
fails) or after the summary commit. The existing two-second
`ControllerLeaderLease.heartbeat()` can update during source normalization and
is blocked only for the bounded final proof/CAS statement. No source/database
snapshot is claimed after the read-only transaction commits; the summary
describes that completed coherent snapshot.

The executor exposes fixed query builders rather than arbitrary SQL. The
allowlist governs projector-submitted application statements. SQLAlchemy
dialect initialization and psycopg2 session setup may issue their fixed
connection-management probes (for example server-version, current-schema, and
standard-conforming-string discovery); they use the same DSN and cannot select
a workload relation or submit application DML. Pool pre-ping is disabled.
Projector-submitted statements allow only:

- the exact source-table column/index reads above;
- `clusters` primary-key reads of `name`, `cluster_hash`, `status`,
  `workspace`, `is_managed`, `workload_type`, `workload_id`, and
  `workload_task_id`;
- `cluster_history` primary-key reads of the scalar allowlist above;
- `SELECT|INSERT|UPDATE` on `capacity_projection_scans` only;
- the existing `current_controller_leadership_statement(..., lock=False)`,
  including both aliases of `pg_catalog.pg_locks` and its fixed advisory-lock
  predicates/operators;
- the same statement with `lock=True` only in short scan
  insert/finalize/failure and activation-proof transactions;
- fixed `SELECT` of the five Alembic version tables
  `alembic_version_state_db`, `alembic_version_serve_state_db`,
  `alembic_version_spot_jobs_db`, `alembic_version_api_requests_db`, and
  `alembic_version_capacity_state_db`;
- fixed startup reads of `pg_catalog.pg_class`, `pg_namespace`,
  `pg_attribute`, `pg_index`, and `pg_constraint` using only
  `pg_get_indexdef`, `pg_get_constraintdef`, and `format_type`;
- fixed `SELECT current_schema()` for normalized co-location proof;
- fixed `transaction_timestamp()`, `octet_length`, `pg_column_size`,
  `jsonb_typeof`, JSONB all-keys/subtract/extract operators, and closed
  regular-expression predicates used by the activation proof; and
- fixed transaction isolation, `SET LOCAL statement_timeout`,
  `lock_timeout`, `idle_in_transaction_session_timeout`, and connection
  application-name statements.

Every other projector-submitted relation/function, textual SQL, DDL, `CALL`,
provider import, source DML, or DML on the other four capacity tables is
rejected before execution.

One scan covers one `(workspace, source_kind, projection_scope_hash)`.
Source queries start only from configured selectors, order by full stable key,
request the remaining row budget plus one, and use only:

```text
services PK(name)
replicas PK(service_name, replica_id)
job_info PK(spot_job_id)
ix_spot_job_task(spot_job_id, task_id)
clusters PK(name)
cluster_history PK(cluster_hash)
capacity_projection_scans PK/completed/running-partition indexes
api_controller_leadership singleton PK plus pg_locks proof
```

There is no reverse registry/history scan, workspace scan, or best-effort
attribution filter.

Limits per partition are 10,000 source rows, 30,000 total rows including
registry/history/diagnostic enrichment, 1 MiB per variable-width value,
4 MiB per fetch batch, 64 MiB combined serialized input plus retained
canonical records, and 30 seconds. Before fetching variable text, phase one
selects stable keys/fixed scalars and logical `octet_length`, rejects a value
or aggregate over budget, then streams approved values in stable-key batches
of at most 256 rows/4 MiB. Canonical bytes are charged before retention.
Mapping version 1 selects no source JSON or bytea blob. Caches are
partition-local and are discarded after commit or rollback.

Timeout profiles are exact. One activation operation has a single 30-second
wall-clock deadline covering both bounded read phases and its final leadership
proof. One partition operation has a separate 30-second deadline beginning
before eligibility/insert and covering source attempts plus publication.
Long activation/source transactions set `statement_timeout` to the remaining
deadline capped at 30,000 ms, `lock_timeout=250ms`, and
`idle_in_transaction_session_timeout=35000ms`. Insert, publication, failure,
stale-finalization, and activation-proof transactions use
`statement_timeout=1000ms`, `lock_timeout=250ms`,
`idle_in_transaction_session_timeout=2000ms`, and a two-second client
watchdog; when nested in activation/partition work they must also fit its
remaining outer deadline. Thus a leadership-row lock is held for at most two
seconds and normally for one statement. Timeout during long work maps to
`scan_timeout`; timeout/lock failure in a short database operation maps as
specified below.

The controller-owned daemon runs partitions sequentially, with one live
transaction. It uses fixed 900-second UTC slots, not a process-local sleep
anchor. For a partition, jitter is the unsigned big-endian value of the first
eight bytes of
`SHA256(ASCII("skypilot-capacity-slot-v1") || NUL ||
source_partition_hash ASCII)` modulo 61. Slot `n` is eligible at Unix second
`n * 900 + jitter`. `scheduled_slot_utc` is the latest eligible slot timestamp
at the startup/loop transaction time.

Before inserting, the daemon uses the existing completed-scan index and
running-partition index to inspect the current partition. Any running or
terminal cursor with the same scheduled slot makes that slot ineligible,
regardless of projection-scope changes. A stale running row is first finalized
as failed, but its slot is not retried. Restarts, configuration changes, and
leadership handoffs therefore cannot create an extra cycle. There is no
out-of-slot retry; a failed partition waits for the next slot. With at most
16 partitions in the durable union and a first activation no more than
35 days before expiry, there are at most 3,361 rows per partition and 53,776
rows total, including the initial partial slot. A removed partition retains
its place in both bounds. Thus the 4,000-row, 35-day-per-partition terminal
lookup contains the complete pilot even after restart.

For each partition:

1. A bounded `READ COMMITTED` transaction checks the leadership-acquisition
   activation snapshot and scheduled-slot eligibility without a row lock,
   then takes the leadership row `FOR SHARE`, re-proves the generation, and
   inserts one UUIDv4 `running` scan. The C1 partial unique index prevents
   overlap; the row lock is held only across the final proof/insert.
2. One read-only `REPEATABLE READ` transaction sets the fixed timeouts and
   makes the existing non-locking live-leadership statement its first data
   read.
3. It performs bounded source reads, builds evidence records/counters, and
   computes the streaming digest, then commits. No provider/network call
   occurs.
4. A short `READ COMMITTED` transaction takes the leadership row `FOR SHARE`,
   re-proves the live generation, and CAS-updates only its running scan row to
   `completed`, using one `transaction_timestamp()` for completion, with exact
   cursor, row count, and findings, then commits.

A crash before step 4 leaves only a running scan. A current controller marks a
running scan older than ten minutes `failed/stale_scan` before the next cycle.
If the indexed running row is younger than ten minutes, that partition is
skipped without inserting another scan.
The scan uses a PostgreSQL engine namespace
`physical-capacity-evidence` with a strict one-connection `QueuePool`; it does
not check out the ordinary main-process connection. On shadow controller
startup, one usable PostgreSQL connection is subtracted before
`compute_server_config()` distributes the ordinary process/worker budget.
Activation requires at least two usable connections. The isolated connection
is created by a narrow `db_utils` isolated-engine helper whose cache key
includes the normalized credential-preserving URL, namespace, explicit
`pool_size=1`, `max_overflow=0`, `pool_pre_ping=false`, and application name.
It also fixes `pool_timeout=1s` and `connect_timeout=5s` and does not inherit
process-global `_max_connections`. The projector is the sole user of that
namespace. Shutdown cancels its live connection, disposes the namespace
engine, proves no checkout remains, and then joins the daemon. Disabled roles
create neither the namespace nor the reserved budget.

The scan transaction uses a 30-second database timeout and external watchdog.
SQLSTATE `40001` permits at most three total attempts for the same logical scan
row: the initial attempt, then retries after deterministic 50-ms and 100-ms
delays inside the same deadline. Each attempt starts a new `REPEATABLE READ`
snapshot and rebuilds all rows, counters, and digest; no result from the
aborted snapshot is carried forward. A
serialization failure in the short publication transaction permits a separate
maximum of three total publication attempts against the already completed
immutable in-memory result: initial, then 50-ms and 100-ms retries within the
same deadline. Exhausting either three-attempt budget yields
`serialization_exhausted`; a failed failure-CAS is left running for the stale
scan rule.

After any other failure, the source transaction rolls back. A separate short
`READ COMMITTED` transaction takes the leadership row `FOR SHARE` and CASes
only that scan from running to failed if the same controller generation is
still current; otherwise it leaves the row for the next leader's stale-scan
rule. Failed rows may store partial `rows_seen` but keep empty findings and a
null digest.

Cancellation, role shutdown, or leadership loss stops dequeueing, cancels and
closes the dedicated connection, proves the transaction absent, and joins the
projector before controller-leadership release. It never detaches a live
publication thread.

Failure CAS stores one closed code and no digest:

```text
row_limit_exceeded
byte_limit_exceeded
scan_timeout
source_decode_failed
source_conflict
selector_mismatch
source_index_missing
non_colocated_source_store
controller_fenced
serialization_exhausted
database_unavailable
database_statement_failed
stale_scan
```

Connection establishment/checkout failure, SQLSTATE class `08`, or an
invalidated/disconnected DBAPI connection maps to `database_unavailable`.
Deadlock, lock timeout, and every other SQL execution failure not already
classified as `scan_timeout`, `serialization_exhausted`, schema/index failure,
or fencing maps to `database_statement_failed`. Python decoding/mapping errors
map to `source_decode_failed`. If the best-effort failure CAS itself cannot
run, the row remains `running` for `stale_scan`; no new unclosed code or error
text is invented. Error text and source identifiers are not stored.

## Observability

Metrics are keyed only by source kind, workspace hash, group confidence, and
finding category:

`workspace_hash` is lowercase SHA-256 over
`ASCII("skypilot-capacity-workspace-metric-v1")`, one NUL byte, and the
workspace UTF-8 bytes. Raw workspace and selector identifiers are never metric
labels.

- scan duration, lag, rows, success/failure, and digest-change count;
- selector, group, candidate, identity, desired-state, and scalar-placement
  counts;
- pool assignment counters; and
- pilot expiry/projector health.

Metrics reproduce committed summary counters. Identity gaps are reports, not
pages, and are never labeled leakage without independent provider/billing or
incident evidence. Every report calls this the `typed-selector cohort`, lists
selector-days and missing intervals, and never claims fleet coverage.

## Deployment and rollback

1. Merge and deploy the implementation with mode `disabled` on every role.
   No database migration runs and the four materialized C1 tables remain
   empty.
2. Commit baseline queries, owners, pilot end, and unavailable-signal
   declarations, but do not start gate arithmetic.
3. Set the three C2 variables only through `controllerService.extraEnvs` on
   one controller deployment for one isolated service-selector canary.
4. Verify three scans across a controller restart and leadership handoff.
5. Add the intended pool and managed-task selectors only after source-write/
   provider-call audits remain zero; finish canary within three days.
6. Freeze the exact measurement manifest and partition scope hashes, set the
   next complete UTC boundary as `measurement_start_utc`, and prove 30 complete
   days fit before the immutable pilot end.
7. Compare the exact preceding and following 30-day manifest cohorts, sign the
   decision no later than 14 calendar days after expiry, and execute the
   removal deadline below.

Rollback is controller `shadow -> disabled`, unset both C2 variables, and roll
back the binary. The projector is joined before leadership release. Existing
scan summaries remain inert and old code ignores the new variables. No source
row, provider resource, Kubernetes admission object, Helm release history, or
public contract needs rollback.

## Temporary code and removal ledger

There is no C2 data migration whose temporary dual-write code must linger.
The decision record is due no later than 14 calendar days after pilot expiry.
Missing evidence or a missed decision deadline counts as no gate. All C2-only
code must be removed from the deployed controller no later than 45 calendar
days after expiry.

### Exact C2 removal ledger

Unless a separately accepted product design names and owns an exact pure
symbol before the 14-day decision deadline, remove the following no later than
45 calendar days after pilot expiry.

#### Delete these C2-only files in full

- `sky/physical_capacity/contracts.py`
- `sky/physical_capacity/hashing.py`
- `sky/physical_capacity/adapters.py`
- `sky/physical_capacity/source_queries.py`
- `sky/physical_capacity/metrics.py`
- `sky/physical_capacity/projector.py`
- `sky/physical_capacity/repository.py`
- `tests/unit_tests/test_physical_capacity_config.py`
- `tests/unit_tests/test_physical_capacity_hashing.py`
- `tests/unit_tests/test_physical_capacity_adapters.py`
- `tests/unit_tests/test_physical_capacity_projector.py`
- `tests/unit_tests/test_physical_capacity_scan_pg.py`

A later accepted product design may retain only the exact pure
contract/adapter/hash symbols it explicitly lists, tests, and gives a dated
replacement/removal milestone. It may not retain `EvidenceProjector`,
`ScanRepository`, source-query execution, metrics, scheduling, publication, or
runtime lifecycle hooks merely because one payoff gate passed.

#### Remove these C2 additions from shared files

- `sky/physical_capacity/canonical.py`
  - Remove `CanonicalDomain.SCOPE_ENTRY`.
  - Remove `CanonicalDomain.EVIDENCE_RECORD`.
  - Restore the C1-only module/class documentation.
- `sky/physical_capacity/config.py`
  - Remove imports `datetime` and `contracts`.
  - Remove `PHYSICAL_CAPACITY_SOURCES_ENV_VAR`,
    `PHYSICAL_CAPACITY_PILOT_END_ENV_VAR`, `SOURCES_ENV_VAR`, and
    `PILOT_END_ENV_VAR`.
  - Remove `API_SERVER_ROLE_ENV_VAR`, `API_REQUEST_BACKEND_ENV_VAR`,
    `CONTROLLER_SERVER_ROLE`, and `POSTGRES_REQUEST_BACKEND`.
  - Remove `MAX_SOURCES_JSON_BYTES`, `MAX_SOURCE_SELECTORS`,
    `MAX_SOURCE_PARTITIONS`, `_SERVE_SELECTOR_FIELDS`,
    `_MANAGED_SELECTOR_FIELDS`, and `_PILOT_END_PATTERN`.
  - Remove `CapacityConfig.sources`, `CapacityConfig.pilot_end_utc`, and
    `CapacityConfig.partitions`.
  - Remove `_selector_sort_key`, `_parse_selector`, `_parse_sources`,
    `parse_pilot_end_utc`, `pilot_end_datetime`, and
    `_validate_selector_allowlists`.
  - Remove the source-selector and pilot-end parsing/validation additions from
    `load_config`.
  - Remove `validate_common_runtime_environment`.
  - Retain the C1 mode/allowlist parser, `CapacityConfig.mode`,
    `CapacityConfig.allowlist`, `load_config`, and
    `validate_runtime_capability`.
- `sky/physical_capacity/models.py`
  - Remove `ProjectionScanPhase`.
  - Remove `ProjectionScanErrorCode`.
  - Restore the C1-only module documentation.
  - Retain `ProjectionSourceKind`, `ProjectionScanState`, and all other
    revision-001 row enums.
- `sky/server/runtime.py`
  - Remove imports `Callable`, `physical_capacity_config`,
    `capacity_projector_lib`, and the C2-only `serve_utils` import.
  - Remove `RuntimeState.physical_capacity_config`.
  - Remove `_ordinary_db_connections_after_capacity_reservation`.
  - From `initialize_common_runtime`, remove the C2 config load/common-runtime
    validation, Serve/pool/jobs consolidation gate, isolated-connection
    reservation/logging, reduced connection count passed to
    `compute_server_config`, and capacity config stored in `RuntimeState`.
  - From `_run_controller_role`, remove `capacity_projector`,
    `capacity_projector_failed`, the C2 `activating-controller` readiness hook,
    `start_controller_projector`, projector-health fencing and
    `capacity-projector-failed` readiness state, `stop_controller_projector`,
    the capacity failure guard on normal draining, and the final
    `Physical-capacity evidence projector failed` exception.
  - Remove the C2-originated generic drain-step/fail-stop rewrite and the
    corresponding `_kill_local_controller_children` behavior change unless
    another accepted design independently owns those exact controller-hardening
    changes.
- `sky/utils/db/db_utils.py`
  - Remove `_ISOLATED_POSTGRES_CONNECT_TIMEOUT_SECONDS`.
  - Remove `_ISOLATED_POSTGRES_POOL_TIMEOUT_SECONDS`.
  - Remove `_postgres_isolated_engine_cache`.
  - Remove `_isolated_postgres_engine_key`.
  - Remove `get_isolated_postgres_engine`.
  - Remove `isolated_postgres_engine_checked_out`.
  - Remove `dispose_isolated_postgres_engine`.
- `sky/physical_capacity/__init__.py`
  - Restore the C1 foundation documentation stating that revision 001 exposes
    no production projector or mutation path.

#### Retain the C1 foundation unchanged

Do not alter or delete these as ordinary C2 cleanup:

- `sky/schemas/db/capacity_state/001_initial_schema.py`, including all five
  tables and its `upgrade`/`downgrade` contract.
- `sky/physical_capacity/schema.py`, including `PROJECTION_SCANS`, `GROUPS`,
  `GROUP_INTENTS`, `ALLOCATIONS`, and `ALLOCATION_DESIRES`.
- `sky/physical_capacity/state.py`.
- The capacity initialization hook in `sky/server/database_migrations.py`.
- `[capacity_state_db]` in `sky/setup_files/alembic.ini`.
- `CAPACITY_STATE_DB_NAME` and `CAPACITY_STATE_VERSION` in
  `sky/utils/db/migration_utils.py`.
- The C1 portions of `canonical.py`, `config.py`, and `models.py`.
- `tests/unit_tests/test_physical_capacity_models.py`,
  `tests/unit_tests/test_physical_capacity_schema_pg.py`,
  `tests/unit_tests/test_physical_capacity_state.py`, and the C1 migration
  coverage in `test_migration_utils.py` and
  `container_images/test_postgres.py`.

Ordinary rollback or C2 removal does not delete scan summaries or run a schema
migration. Existing summaries become inert. Dropping any of the five C1 tables
requires a separate PostgreSQL migration, rollout, and rollback design. If
exact identity transport is later accepted, mapping version 1 remains
read-only historical evidence; a new version runs separately and never
rewrites legacy association. Observation-cache, action-journal, and
occupancy-ledger code cannot be added under the label of C2 cleanup.

## Verification

### C2.1 verification evidence

Implementation anchors:

- `67259d80f`: strict selectors, evidence contracts, and canonical hashing.
- `14c7c3a4b`: bounded read-only source queries and pure adapters.
- `2497b690d`: scan repository, metrics, projector, controller lifecycle
  integration, and PostgreSQL/runtime tests.

Observed against the exact implementation on 2026-07-31:

- all 165 non-PostgreSQL C2 tests passed: 29 selector/config, 20 hashing,
  93 source-adapter, and 23 projector/runtime cases;
- all 39 tests passed against disposable PostgreSQL 14: 20 C1 transaction
  repository, 14 C1 schema, and five C2 scan-repository cases;
- the existing 52-case C1 model suite passed;
- existing `sky.server.runtime` and `sky.utils.db.db_utils` unit-test files
  passed;
- mypy checked 804 source files clean, pylint reported 10.00/10, and formatting
  plus `git diff --check` were clean; and
- independent adversarial implementation review found no remaining C2.1
  contract blocker after the final fencing and committed-metric fixes.

### C2.2 disabled-deployment evidence

The exact implementation commit `0aae14884482642523ed96227a42523c5c0a1583`
was packaged as one `linux/amd64` image and pushed to the test account's
immutable ECR repository. The deployed digest is
`sha256:a349f24a81f1c37d85bc0fb896a05541b57cf4c142716d98948502603b73fa02`.
The packaged chart SHA-256 remained
`ad803ece8c15eed01eed86b51376dbecd192167f6f0a52c33eeeceb953cc604b`.

On 2026-07-31, account `361913687221`, EKS cluster
`boltz-platform-test-eks-cluster`, namespace and release `skypilot-ha`:

- release 35 was the verified rollback baseline, with API, controller, and
  executor at 2/2 on digest `sha256:4310ff0de03aa9e2d193733b463a62e96ef97cc0d59e8f2d5bf087e78987cbac`;
- the new digest was rolled out without capacity variables in three bounded
  stages because the static nodes could not safely absorb all three 4-CPU,
  8-GiB surges together: API at revision 36, executor at revision 37, and
  controller at revision 38;
- release history records the three stage descriptions and successful
  outcomes. Pre/post value sets are identical after excluding the three image
  fields, the staged deployment snapshots show the intended old/new role
  digests, and each normal PostgreSQL verification hook succeeded before its
  role rollout. Karpenter supplied at most one staged surge at a time; PDBs
  kept at least one healthy replica during rollout and ordinary
  underutilized-node consolidation;
- final release 38 was deployed with API, controller, and executor each 2/2,
  all six role pods on the exact new digest, zero restarts, and PDB disruption
  allowance restored to one per role;
- every role pod independently reported SkyPilot commit `0aae14884`, mode
  `disabled`, zero selectors, and no pilot end. The live Helm manifest
  contained none of the three C2 variables;
- the API health response reported `healthy`, build `7978`, and the exact
  commit. All four controller and executor readiness/liveness pairs returned
  `ok`, with no physical-capacity failure, traceback, or fatal signature in
  the post-rollout role logs;
- PostgreSQL reported zero connections with application name
  `skypilot-physical-capacity-evidence`. Before rollout, after stage 2, and
  after release 38, all five C1 capacity tables existed and each contained
  zero rows;
- central revisions remained state `027`, Serve `031`, jobs `026`, requests
  `004`, and capacity `001`. The final read-only audit found no active cluster,
  Serve service, Serve replica, or unresolved managed job; and
- the three successful staged migration-hook jobs had status and logs
  captured and were then removed to restore the baseline namespace shape.
  Helm release history and PostgreSQL state were retained.

Release 35 is the binary rollback anchor if disabled-mode regressions are
found. No rollback was required. The deployed implementation remains disabled.

### C2.3 activation gate result

C2.3 was not activated. The fresh pre- and post-deployment source audits found
zero Serve services and zero Serve replicas, so there is no real isolated
service selector. No independent provider-call audit exporter, query, owner,
or baseline was declared for this canary. A synthetic missing selector would
exercise only scheduling and cannot substitute for adapter or provider-call
evidence. Creating a purpose-built Serve service would mutate provider
resources and is outside this deployment's existing-state verification scope.

Consequently, there is no claim of zero provider calls, source-write safety
under an active scan, digest stability, three completed slots, or
restart/handoff behavior. All three C2 variables remain absent. C2.3 stays
blocked until a real selector and independent provider-call audit are supplied
and frozen in the canary manifest.

Automated tests cover:

- strict top-level selector grammar, UTF-8/count/partition/pilot-end bounds,
  role/backend/co-location gates, and old-binary variable ignorance;
- component/scope/evidence streaming hash goldens, maximal 64-KiB selector
  input, non-ASCII/order permutations, and digest stability;
- every Serve/service-parent and managed status mapping;
- exact/legacy/unknown group classification and the complete finding-arithmetic
  table, including mixed Serve children without group aggregation;
- safe/missing/unsafe registry association, duplicate in-partition hash,
  tenant conflict, null attribution, and `no_cluster_yet`;
- scalar placement hashing and proof that no resource/request/blob/catalog
  column or provider module is selected;
- direct current-scope pool diagnostics and Batch/null-link exclusions;
- 9,999/10,000/10,001 source rows, total-row, pre-materialization byte, batch,
  canonical-memory, and deadline limits;
- PostgreSQL running/completed/failed atomicity, existing index query plans,
  stale takeover, serialization retry, leadership handoff, watchdog
  cancellation, fixed global pilot end, and durable 16-partition union across
  rolling configuration changes;
- activation rejection for wrong JSONB scalar types/keys, malformed partition
  hashes, misaligned or duplicate slots, 3,362 rows in one partition, and a
  53,777th global row;
- finalization/handoff races and non-blocking two-second heartbeats during a
  30-second source scan;
- exact long/short statement, lock, idle, connect, checkout, and watchdog
  budgets plus every closed database-failure mapping;
- sequential 15-minute cadence, 35-day expiry, shutdown-before-lease-release,
  and no DML outside `capacity_projection_scans`; and
- metric/committed-counter parity with no high-cardinality labels.

Manual test:

```text
1. Deploy with mode disabled.
   Expected: no projector thread; all five C1 table counts unchanged.
2. Enable one isolated Serve selector on the controller with a <=35-day end.
   Expected: one completed summary per cycle, deterministic digest for an
   unchanged snapshot, and no rows in the other four capacity tables.
3. Add a pool and a managed task.
   Expected: arithmetic explains every live selector/candidate; pool jobs
   create diagnostics but no managed physical candidate.
4. Restart and then hand off controller leadership during a scan.
   Expected: the interrupted scan fails/stales, one successor completes, and
   no old-generation transaction survives release.
5. Diff provider audit logs and source-table update timestamps.
   Expected: zero projector provider calls and zero source writes.
6. Disable, unset C2 variables, and roll back the binary.
   Expected: workload behavior is unchanged and summaries are inert.
```

## Implementation phases and open gates

- C2.1 complete: strict configuration, pure adapters, digest/counters, scan
  repository, controller daemon, and unit/PostgreSQL tests.
- C2.2 complete: the exact binary is deployed on every role with mode
  `disabled`; zero projector connections and unchanged zero counts in all five
  C1 tables were verified before and after the rollout.
- C2.3 blocked: supply and freeze one real isolated Serve selector plus an
  independently owned provider-call audit. Then set all three variables only
  through `controllerService.extraEnvs`; run one real isolated service selector
  for three scans spanning a restart and leadership handoff, with independent
  proof of zero provider calls, zero source writes, and no rows in the four
  materialized C1 tables. Finish this canary within three days. Do not add pool
  or managed-task selectors until the audits pass.
- C2.4 gated: freeze the exact typed Serve/pool/jobs selector and scope-hash
  manifest, prove the preceding 30-day baseline exists and a complete following
  30-day window fits before the immutable pilot end, then collect the signed
  decision record.

The decision is due no later than 14 days after expiry. Unowned C2 code is due
for removal no later than 45 days after expiry. Missing live-selector or
external provider-call evidence blocks C2.3 rather than being replaced by a
synthetic selector or internal log inference.

No materialized inventory, provider observation cache, identity transport,
action journal, occupancy ledger, read cutover, or mutation authority is
implementation-authorized here.

## Intentional departure from rejected drafts

The first rejected draft made active identity transport, guarded Helm,
namespace admission, quota, and a permanent rollback floor prerequisites for
disabled shadow. The second still materialized groups, immutable intents,
allocation desires, source-missing lifecycles, quarantine, and retention
policy despite having no reader.

This contract restores the risk order: measure deterministic adapter output
with one bounded summary row, then build only the product whose production
value is independently demonstrated.
