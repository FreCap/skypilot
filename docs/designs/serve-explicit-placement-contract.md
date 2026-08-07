# SkyServe explicit placement contract

_Status: transition PR #1318 is merged, released as v1.1.1135, and deployed to
production as Helm revision 351.  Draft cleanup PR #1319's implementation
passed 30/30 checks but remains blocked on the measured removal gates and is
not approved to merge or deploy.  Platform pin PR #8090 was closed because
this SkyPilot control-plane release is deployed directly with Helm.  The first
production verification passed; the cleanup observation and inventory gates
remain open. Created 2026-08-07; last updated 2026-08-07._

## Decision summary

SkyServe will use one dynamic placement engine.  A service version resolves
its public `spot_placer` value exactly once into an explicit, immutable runtime
contract with five independent dimensions in addition to its engine:

- replica unit: physical backend or logical GPU slot;
- catalog expansion: configured shapes only or all whole-GPU shapes;
- cost unit: machine-hour or GPU-slot-hour;
- reserved-fill shape: not applicable, configured physical shape, or exactly
  one GPU per backend; and
- workload kind: service or pool.

`dynamic_fallback_per_gpu` is the primary policy for new GPU services that use
per-GPU concurrency.  It expands whole-GPU shapes, compares price per GPU
slot, and counts logical GPU slots.  `dynamic_fallback` remains a physical
compatibility policy for pools and existing services.  Both policies execute
through the same engine; there is no second per-GPU allocator subclass.

The transition does not silently change an existing version's replica unit.
Persisted `dynamic_fallback_per_gpu` versions created before logical semantics
remain physical, with historical whole-GPU catalog and per-GPU price ordering,
for the transition window only.  The stacked cleanup deletes that tuple and
rejects its artifacts after the durable-state and client-version gates prove
none remain.

This design is the placement-policy subdesign of
`docs/designs/skyserve-accelerator-compatibility.md`.  Its centralized catalog
continues to follow `docs/designs/serve-central-placement-catalog.md`.

## Context and problem

SkyServe currently encodes related decisions in several indirect interfaces:

- a string-to-subclass registry chooses between two placer classes;
- a class attribute decides whether the placement catalog expands GPU counts;
- an overridden method changes machine-hour ranking into GPU-slot ranking;
- the string `dynamic_fallback_per_gpu` normally implies logical replicas;
- a hidden persisted `_uses_logical_replicas` boolean overrides that inference
  for old versions;
- `SkyServiceSpec.copy()` temporarily constructs a legacy per-GPU spec as
  `dynamic_fallback`, then mutates its private fields back; and
- reserved-capacity policy discovers logical behavior with an `isinstance()`
  check against the per-GPU subclass.

Those interfaces let one policy name have two persisted replica-unit meanings
and make correctness depend on which subclass or reconstruction path happens
to be present.  Adding another allocator would multiply those happy paths.
Deleting the physical policy outright is also unsafe: pools promise physical
worker counts, generic physical services still use it, and old service-version
pickles must remain restartable.

## Goals

- Make the placement decisions required by runtime code explicit and typed.
- Run configured-shape and whole-GPU placement through one implementation.
- Make `dynamic_fallback_per_gpu` the documented primary policy for new GPU
  concurrency services without relying on an implicit client or server
  default.
- Preserve every committed service version's replica, catalog, and price
  semantics across copy, retry, controller restart, API rollback, and rolling
  update.
- Keep physical pool behavior explicit until a separately designed pool API
  can express logical GPU slots.
- Remove unused default-policy registration and subclass capability tests.
- Define exact gates for deleting legacy policy adapters and old persisted
  fields instead of leaving open-ended compatibility code.

## Non-goals

- Changing the public YAML spelling of either existing `spot_placer` value in
  the transition release.
- Silently defaulting an omitted `spot_placer` to per-GPU placement.  Explicit
  serialization is required so a new client cannot be misread by an old
  server.
- Reinterpreting pool `min_workers` or `max_workers` as logical GPU slots.
- Removing request-rate, instance-aware QPS, or on-demand fallback autoscalers.
  Those are separate public contracts and need independent deprecation data.
- Migrating a service during controller restart, exact-YAML retry, or an
  unrelated update.
- Changing provider pricing, catalog contents, reservation ownership, paid
  launch authority, or scale-to-zero policy.

## Public and durable contract

The accepted YAML values remain explicit.  Omission and explicit YAML `null`
are equivalent and select no placement engine:

| Public value | Workload | Engine | Replica unit | Catalog | Cost order | Reserved fill |
|---|---|---|---|---|---|---|
| omitted or `null` | service | none | physical backend | N/A | N/A | N/A |
| omitted or `null` | pool | none | physical backend | N/A | N/A | N/A |
| `dynamic_fallback` | service | dynamic fallback | physical backend | configured shapes | machine-hour | configured physical shape |
| `dynamic_fallback` | pool | dynamic fallback | physical backend | configured shapes | machine-hour | configured physical shape |
| `dynamic_fallback_per_gpu` | new service | dynamic fallback | logical GPU slot | whole-GPU shapes | GPU-slot-hour | exactly one GPU/backend |

One historical state is not constructible from new YAML.  The transition
reader preserves it until the removal gates pass:

| Persisted state | Workload | Engine | Replica unit | Catalog | Cost order | Reserved fill |
|---|---|---|---|---|---|---|
| per-GPU policy without the logical marker | service | dynamic fallback | physical backend | whole-GPU shapes | GPU-slot-hour | exactly one GPU/backend |

The cleanup reader rejects this historical tuple as well as every other tuple
outside the public table above.  In particular, a pool cannot use
`dynamic_fallback_per_gpu`, a logical contract cannot name a pool, and the
historical physical/per-GPU tuple is accepted only by compatibility decoding,
not by fresh YAML or an override, during the transition window.  Historical
fractional, non-exact, or otherwise odd resource shapes retain their old
physical behavior only in that window; cleanup deployment is forbidden until
the inventory proves no such recoverable state remains.

Policy names remain case-insensitive at the YAML boundary, matching the
existing service schema, but every newly constructed spec canonicalizes and
persists the lowercase spelling.  Explicit `null` is accepted in both service
and pool policy sections and canonical serialization omits it.

### Persisted schema and precedence

The transition persists the resolved contract as seven primitive
`SkyServiceSpec` fields:

```text
_placement_contract_version: int                 # 1 (transition write) |
                                                # 2 (cleanup write)
_placement_engine: str                           # none | dynamic_fallback
_placement_replica_unit: str                     # physical_backend | logical
_placement_catalog_mode: str                     # not_applicable |
                                                # configured_shapes |
                                                # whole_gpu_shapes
_placement_cost_unit: str                        # not_applicable |
                                                # machine_hour |
                                                # gpu_slot_hour
_placement_reserved_fill_mode: str               # not_applicable |
                                                # configured_shape |
                                                # single_gpu_backend
_placement_workload_kind: str                    # service | pool
```

Runtime enums and the frozen typed contract are never stored in a pickle.  A
transition-release object writes version 1, all other six fields, and the
existing primitive `_uses_logical_replicas: bool` rollback mirror together
before it can be published.  The version-spec pickle and the existing
service-level logical semantics fence are committed in the same
version-publication transaction.

`SkyServiceSpec.__setstate__()` is a compatibility decoder/materializer, not a
durable migration: it never rewrites or backfills committed pickle bytes.
Exact retries continue to read the original authoritative version artifact.
Its precedence rules are strict:

1. If all seven contract fields are absent, derive the contract from the
   persisted policy name, containing workload kind, and historical marker;
   an absent historical marker means physical semantics.  This reader uses
   the immediately preceding release's workload truthiness: a fieldless
   `_pool={}` is a service, while `True` or a non-empty mapping is a pool.
2. If `_placement_contract_version == 1`, all other six fields and the
   rollback mirror are required.  Validate their exact tuple, require the
   workload kind to match the containing service/pool object, and require the
   mirror to agree with the replica unit.  The transition reader also accepts
   the historical physical/per-GPU tuple; the cleanup reader accepts only the
   five public tuples and rejects that removed tuple.  A known public name
   cannot disagree with retained contract fields.
3. If `_placement_contract_version == 2`, all other six fields are required
   and the rollback mirror must be absent.  Validate the five public tuples and
   policy-name/workload match.  The cleanup exposes a zero-argument,
   v2-only `persisted_fields()` writer and `SkyServiceSpec.__getstate__()`
   projects every encodable v1 artifact to v2 without mutating the source.
   New service and version database writes use this serialization boundary;
   exact retries of an already committed row remain acknowledgement-only and
   byte preserving.  The transition reader accepts valid v2 state so the
   cleanup release is rollback-compatible, but the transition writer emits
   only v1.
4. A partial field set, boolean masquerading as a version, unknown version,
   unknown value, invalid tuple, workload mismatch, forbidden/missing mirror,
   or v1 mirror disagreement is malformed current state and fails loudly.  It
   never falls back to legacy derivation.

Fresh pool input treats any mapping, including `{}`, as an explicit pool.
Before a transition object is persisted, it canonicalizes that pool driver to
a deterministic non-empty mapping.  When the incoming driver has no
non-policy pool keys, a fixed pool with resolved size `N` persists exactly
`{'workers': N}`, while an autoscaling pool with resolved bounds `MIN` and
`MAX` persists exactly `{'min_workers': MIN, 'max_workers': MAX}`.  A non-empty
input mapping retains its non-policy pool settings.  In every case the policy
name is removed from the mapping and stored only in `_spot_placer`.  Thus fresh
`pool: {}` plus top-level `workers: N` becomes exactly `{'workers': N}`.  That
truthy mapping lets the immediately preceding reader preserve both pool kind
and size on rollback.  Versioned pool contracts therefore require a non-empty
mapping and reject `True`, `{}`, or another representation as malformed rather
than silently repairing it.  A fieldless legacy `{}` remains a service under
rule 1 and is materialized as `_pool=False` in memory.

The rollback mirror is written only for the transition compatibility window.
Runtime policy reads the resolved contract and the monotonic service-level
logical fence, not the mirror.

The public `spot_placer` string remains in serialized task YAML.  The seven
internal fields live only in the internal persisted `SkyServiceSpec` object;
`SkyServiceSpec.to_yaml_config()` and task serialization omit them.  Serve
status/API responses expose database status fields and stored or rendered task
YAML, not `SkyServiceSpec.__dict__`, so no new field is added to a public wire
payload and no API version bump is required.  Golden spec/task serialization
tests enforce the boundary at the only serializer that receives this object.

The stacked cleanup in this design does not remove a public YAML value:
`dynamic_fallback` remains required by physical pools.  Any later removal of
an accepted spelling is a separate breaking change with advance deprecation
warnings, documentation, usage telemetry, an API-version bump, a
release/CI-managed minimum-compatible-version advance, and an explicit
unsupported-policy error instead of semantic fallback.

## Runtime architecture

### Contract resolution

The dependency-neutral `sky.serve.placement_policy` module owns the resolver,
value domains, and frozen `PlacementContract`.  It imports neither
`SkyServiceSpec` nor the placement engine.  `SkyServiceSpec.placement_contract`
is the authoritative typed access point and reconstructs the frozen value from
the validated primitive fields.

The resolver owns the complete mapping from public or persisted state to:

```text
engine          = none | dynamic_fallback
replica_unit    = physical_backend | logical
catalog_mode    = not_applicable | configured_shapes | whole_gpu_shapes
cost_unit       = not_applicable | machine_hour | gpu_slot_hour
reserved_fill_mode = not_applicable | configured_shape | single_gpu_backend
workload_kind   = service | pool
```

Fresh specs resolve from the explicit policy name and workload kind.  Only
legacy persisted specs with all seven versioned fields absent resolve from the
policy name, workload kind, and historical logical marker.  Versioned specs
validate and reconstruct from their primitive fields under the precedence
rules above.  Impossible combinations fail at this boundary.  They never fall
through to a nearby policy.

The contract is immutable for a committed `(service_name, version)`.  In the
transition, a copy that overrides neither `spot_placer` nor `pool` carries the
exact contract forward.  In cleanup, every encodable v1 copy writes v2 and a
historical tuple is rejected during decode.  An explicit policy or
workload-kind override resolves a fresh contract and goes through ordinary
validation.  The public constructor cannot accept a resolved contract; only a
token-gated internal copy path may preserve a supported tuple.

Every consumer uses the typed access point: `SpotPlacer.validate_task()` and
`build_catalog()` receive it before an engine exists; placer construction and
ranking receive it directly; `uses_logical_replicas` and `replica_unit` derive
from it; the version-publication/CAS path checks it against the service-level
logical fence; controller, replica manager, and autoscaler cache declared
fields from it; and reserved fill reads its explicit fill mode.  No one of those
paths infers semantics from a policy string, concrete class, missing
attribute, or private compatibility mirror.

### One placement engine

`DynamicFallbackSpotPlacer` receives the resolved contract directly.  Catalog
construction consults `catalog_mode`; ranking consults `cost_unit`.
Zero-cost-first selection, bench/retry behavior, exact-location filtering,
workspace policy, and paid launch fencing remain shared and unchanged.

There is no class registry or default flag.  Before deleting
`CapacityAwareDynamicFallbackSpotPlacer`, the transition audits source imports,
plugins, retry artifacts, version pickles, controller/local databases,
snapshots, and rollback backups for its qualified class path.  If any durable
object references it, a deprecated deserialization-only import shim remains;
otherwise a real preceding-release pickle fixture must prove the deletion is
safe.  If a qualified-class reference does exist, a real fixture must prove
the deserialization-only shim loads it and resolves the typed contract without
restoring a second runtime engine.  Supported public names are a fixed mapping
to contract presets.
`spot_placer: null` constructs no placer.

Reserved-capacity validation reads `reserved_fill_mode` from the engine
contract.  It must not infer fill shape from replica unit, catalog expansion,
the public policy string, or the concrete Python class.  Both modern logical
per-GPU and historical physical/per-GPU versions retain the preceding
subclass's exactly-one-GPU-per-zero-cost-backend restriction.

### Primary GPU path

New heterogeneous GPU concurrency services explicitly use
`dynamic_fallback_per_gpu`.  They must retain the existing validation:

- one node per backend and exact GPU candidates;
- positive integer `target_concurrency_per_replica`;
- `graceful_drain_async_occupancy: true`;
- instance-aware exact-card routing where compatibility is enabled;
- logical `min_replicas` and `max_replicas`; and
- one-GPU reserved Kubernetes shapes when reserved fill is enabled.

For Boltz production services, the operational contract additionally requires
`min_replicas: 0`; every `min_replicas_by_accelerator` value absent or zero;
reserved-fill floor zero with utilization gating enabled; and no implicit
on-demand fallback.  That production YAML change is reviewed and deployed
separately from this behavior-preserving engine refactor.  Compliance is
claimed only after the YAML is applied, committed and applied versions match,
and prior warm replicas and claims have drained.

## Invariants

1. A committed version never changes replica units during restart or retry.
2. A public policy name is resolved once; runtime consumers do not infer
   capabilities from strings, subclasses, or missing attributes.
3. Catalog expansion, price normalization, and reserved-fill shape are
   independent from replica units so the historical physical/per-GPU state is
   exactly representable.
4. Physical-to-logical migration occurs only through the existing explicit,
   one-way rolling bridge.  Existing physical rows are never relabeled.
5. The existing service-level logical-semantics record remains a monotonic,
   authoritative fence.  Version publication, retry, recovery, and
   out-of-order commits compare the resolved contract to it; pickle fields can
   never clear or override it.
6. Logical-to-physical recovery/update and logical blue-green update remain
   rejected.
7. Pool counts remain physical and cannot select a logical contract.
8. A missing or malformed current contract fails loudly.  Compatibility
   defaults exist only in the persistence decoder.
9. Zero-cost supply, reconciliation targets, and paid launch authority remain
   separate typed signals.  A placement refactor cannot broaden paid authority.
10. A rollback to the immediately preceding server release can read specs
   written by the transition release with their old logical marker intact.
11. A fresh pool persists a non-empty rollback-readable `_pool` mapping;
    fieldless `_pool={}` retains the preceding reader's service meaning.

## Implementation and PR stack

### Transition PR [#1318](https://github.com/boltz-bio/skypilot/pull/1318)

1. Add the dependency-neutral contract resolver and seven primitive persisted
   fields.
2. Materialize old pickles in `SkyServiceSpec.__setstate__()` and dual-write
   the rollback marker for new specs without rewriting old artifacts.
3. Replace copy-time policy disguise with direct contract preservation.
4. Parameterize one dynamic engine by catalog and cost units.
5. Replace subclass checks with declared contract fields.
6. Delete the per-GPU subclass when the qualified-class audit proves it safe;
   otherwise retain only a deprecated deserialization shim.  Delete the class
   registry and unused default machinery.
7. Document per-GPU placement as the primary GPU concurrency policy.

The transition is behavior preserving for every row in the contract tables.
It may merge and deploy while legacy state exists.

PR #1318 merged as `95e0b41b15ad56598e06ef9cb08297815a65f662`
and was released as v1.1.1135.  Its control-plane deployment remains a
separate reviewed Platform change.

### Blocked cleanup PR [#1319](https://github.com/boltz-bio/skypilot/pull/1319)

The stacked cleanup removes the all-versioned-fields-absent decoder, the
historical physical/per-GPU tuple, and dead compatibility machinery only after
inventory proves that no fieldless physical or logical artifact and no
historical tuple remains.  Pre-transition pickles are immutable and block this
removal.  It writes contract v2 without a rollback mirror while continuing to
read the five supported v1 tuples; existing immutable v1 pickle bytes are never
rewritten in place.  Because the transition reader already accepts v2, rolling
back one release remains safe.  It deliberately retains the public
`dynamic_fallback` physical preset required by pools.  Public policy removal is
a separate breaking design/PR.  Keep this cleanup draft or otherwise blocked;
do not merge it merely because no old replicas are currently READY.

Transition PR #1318 and draft cleanup PR #1319 form gh-stack #1320.  Both link
this design; the cleanup PR states the exact merge gate below and remains
blocked until its evidence is attached.

## Compatibility, rollout, and rollback

1. Inventory all services and pools, including committed, applied, and
   quarantined versions; placement catalogs; replica and bridge rows; active
   client/controller/server versions; every authoritative supported
   controller/local database; retained snapshots/backups; and rollback
   artifacts.  PostgreSQL remains the only central/API-server database and the
   only target for central CAS tests.
2. Merge and release the transition with both old and new readers present.
3. Deploy the control plane through the reviewed Helm/image pin workflow.
   Freeze placement updates for the mixed-binary/Recreate interval.  Do not
   combine the controller rollout with a service policy migration.
4. Verify controller recovery of physical, logical, pool, and legacy
   per-GPU/physical fixtures.  In production require the exact release commit,
   healthy API/controller endpoints, committed/applied version convergence,
   no new quarantine, no paid-authority regression, and continuity of existing
   READY replicas.  A restart without `serve update` must not broaden any paid
   launch authority.  Roll back on contract decode/fence failure, controller
   health failure, service commit/apply divergence, new quarantine, endpoint or
   LB identity change, replica loss, or new unauthorized paid launch.
5. Migrate eligible physical GPU services only through an explicit rolling
   update.  Require every old replica to drain and the logical bridge to
   converge before counting a service as migrated.
6. Roll back the control plane by restoring the prior reviewed image/chart pin,
   without a database rollback because all persistence changes are additive.
   The dual-written logical marker preserves old-server interpretation.  A
   service update created by the new release remains explicit YAML and does not
   depend on a new implicit default.
7. Cross-release proof is bidirectional and uses the exact preceding and
   transition release artifacts/interpreters: previous writes -> transition
   reads; transition writes -> previous reads, copies or updates, and
   reserializes -> transition rereads; and previous writes during rollback ->
   transition roll-forward reads.  Replica unit, YAML, catalog, reserved-fill
   mode and shape validation, and fence must remain identical.  Same-binary
   missing-field tests are insufficient.

No production `serve update` may use a canonical spec that violates the Boltz
scale-to-zero contract.  Control-plane deployment and service-policy
deployment are separate approvals and rollback units.

## Removal gates

The blocked cleanup may merge only when all are true:

- Central PostgreSQL, every authoritative supported controller/local database,
  and retained snapshot/backup/rollback-artifact inventory report no live or
  recoverable all-versioned-fields-absent service spec (physical or logical),
  historical physical/per-GPU contract, or removed qualified class reference.
- No active bridge, cleanup record, retryable version, placement catalog, or
  replica row depends on that version.
- All eligible GPU services have committed/applied logical versions, no
  quarantined version, and zero remaining physical replicas from migration.
- Pools and intentionally physical services are either still supported by the
  physical preset or have completed a separately designed migration.
- The release/CI-managed minimum compatible API version and the minimum
  supported server, controller, and rollback image all include the transition
  contract reader.  Pre-transition rollback images are explicitly forbidden
  once v2 writes begin.
- At least two consecutive production releases and 30 continuous days after
  full transition deployment show zero legacy-decoder events and zero
  contract/fence mismatch or mixed-version recovery errors.  Structured
  controller/API logs for
  `event=skyserve_placement_contract_decode` with
  `outcome=legacy_materialized|rejected` are retained for at least 45 days;
  the observation clock does not start until the Platform log sink and exact
  zero-event query are attached to the transition PR.  The Serve
  maintainer and Platform on-call attach the zero-use query, all database and
  artifact inventories, release identities, and rollback drill evidence to
  the cleanup PR.
- The exact transition release has passed production validation for the
  generation-aware and typed paid-authority fixes tracked in the parent design.

## Verification plan

Automated coverage must prove:

- every fresh public policy resolves to the expected explicit contract;
- nullable and case-insensitive service/pool input canonicalizes to the
  lowercase public spelling, while disabled canonical output omits the key;
- every allowed and rejected tuple, including no-engine service/pool,
  per-GPU pool rejection, workload mismatch, and the sole historical tuple;
- in the transition, old pickles missing the logical marker remain physical,
  preserve whole-GPU expansion/GPU-slot pricing, and survive copy and
  exact-YAML retry; after the cleanup gates pass, the same fieldless and
  historical artifacts fail explicitly instead of selecting nearby semantics;
- transition pickles contain v1, all six dimension fields, and the rollback
  marker; cleanup fixtures contain v2 and no marker; partial fields, unknown
  versions/values, invalid tuples, public-policy mismatch, and
  forbidden/missing/mismatched mirrors fail loudly;
- an unmodified copy retains the exact contract, while an explicit policy or
  `pool` override resolves and validates a new contract;
- configured and whole-GPU catalogs contain the same candidates as before;
- machine-hour and GPU-slot-hour ordering match the previous implementations;
- the one engine preserves zero-cost preference, bench transitions, targeted
  placement, workspace filtering, and retry state;
- legacy physical services and pools recover across controller restart;
- reserved fill rejects wider-than-one-GPU modern logical and historical
  physical/per-GPU shapes using the explicit fill mode, without a
  concrete-class, replica-unit, or catalog-mode inference;
- golden spec/task YAML serialization contains no hidden contract fields, and
  the existing status/API path continues to carry only rendered task YAML;
- exact-release previous/new client-server and bidirectional pickle
  write/read/copy/reserialize behavior is unchanged, with no removed qualified
  class reference;
- fresh `pool: {}` plus `workers: N` persists and serializes exactly as
  `{'workers': N}`; the exact preceding release reads the same pool kind, size,
  and policy, its rollback reserialization round-trips through the transition
  reader unchanged, while a real fieldless legacy `_pool={}` remains a service;
- v1-to-v2 and v2-to-v1 read/copy/reserialize rollback paths preserve exact
  semantics for every v2-encodable v1 tuple; a frozen exact-transition
  historical physical/per-GPU v1 artifact is rejected by cleanup, and a class
  shim fixture (when required) never recreates a second engine;
- the public constructor and an invalid internal-copy token cannot inject the
  decode-only historical physical/per-GPU tuple;
- the monotonic service-level logical fence rejects mismatch under
  out-of-order version commits, exact retries, and recovery;
- physical-to-logical bridge/restart succeeds without relabeling old rows;
- logical-to-physical and logical blue-green operations remain rejected;
- multi-GPU logical capacity, occupancy-aware drain, cancellation, and full
  scale-to-zero leave no replica or capacity claim behind;
- restart between version commit, apply, and quarantine preserves the contract
  and never broadens paid authority; and
- reserved provenance and paid launch authority regressions, including issue
  #1301 and PRs #1303/#1304, remain covered.

Run focused unit suites, the real PostgreSQL persistence/CAS suite, formatting
and static checks, then a cold service smoke test for each eligible provider
class.  A paid-capacity smoke requires separate explicit cost authorization;
zero-cost reserved-capacity tests remain fail closed.

### Verification evidence as of 2026-08-07

- Adversarial review of this exact design and code-level implementation review
  both returned GO for the transition.  A separate final cleanup audit returned
  GO for committing and submitting the blocked cleanup, and NO-GO for merging
  or deploying it before every removal gate above is satisfied.
- Transition PR #1318 passed 30/30 pre-merge checks and merged as
  `95e0b41b15ad56598e06ef9cb08297815a65f662`.  The exact release workflows
  published v1.1.1135 image digest
  `sha256:cf76e4855167237c682ef43fc72554511adaa4aed374df40191a4ba0ea135706`
  and chart digest
  `sha256:3de28d2f3192b28e12d76430e369ea9cd1c0f14ebeb72edf2a3fec36e019609f`.
  The initial merge push exposed a pre-existing shared-context Kubernetes
  fence-test defect; test-only PR #1322 isolated those contexts and merged as
  `fdfc2eb972ebab97add9226fa3cbf41b5792ad49`.  All nine exact-SHA workflows
  then passed, including 13/13 Python and optimizer jobs.
- Draft cleanup PR #1319's exact implementation head
  `b5f7bd9638ba48964d0909bb5645c2f36c4131b3` passed 30/30 checks before the
  evidence-only design update.  Its mandatory Unit Tests job ran the
  repository's real-PostgreSQL suites and finished with 15,409 passed, one
  xfailed test, and no failed test.
- The exact unmodified v1.1.1132 source at
  `ab5ec55b89a8c576e20e6ea27cf240e88134bb64` read transition fixed-pool
  pickles, preserved pool size and policy through copy/protocol-4
  reserialization, and produced bytes the transition reread with the same
  contract.  The reverse fieldless `_pool={}` artifact retained the preceding
  release's service meaning.  This bidirectional proof passed on Python
  3.11.13 and production-family Python 3.14.3.
- Real v1.1.1132 and pre-marker v1.1.247 pickle fixtures pass restart, copy,
  and reserialization coverage in the transition without a removed
  placer-class reference.  Cleanup rejects both fieldless artifacts by
  contract.
- Frozen protocol-4 artifacts produced by the exact transition commit
  `aee3da9e0910d597dc33f31ee964497ced58b78c` cover both a supported logical
  per-GPU v1 contract and the removed historical physical/per-GPU v1 tuple.
  Cleanup accepts and upgrades the former and rejects the latter.  The raw
  SHA-256 digests are respectively
  `a4c549ae75412dcff8917d29f892284745040b9a503b38f70d7cea1686acd05a`
  and `3e912f262e1498d28891d27565f7c0720a429feb1b8131887adb40d13ce2ed28`.
- Every one of the five supported v1 public tuples passes cleanup read, copy,
  protocol-4 reserialization, YAML-equivalence, source-nonmutation, and
  v2-without-mirror checks.  Exact cross-worktree rollback testing passed all
  five tuples through cleanup v2 -> transition commit `aee3da9e0` read/copy to
  v1 -> cleanup reread/rewrite to v2.
- The affected Serve unit suite passes except for four AWS catalog tests whose
  only failure is the operator's expired SSO session; no login was initiated.
  Cleanup contract, persistence, controller retry/recovery, service daemon,
  replica-manager initialization/retry, dynamic placement, and
  reserved-capacity suites otherwise pass.  Focused Python 3.14.3 contract,
  factory, concurrency, and service-spec checks pass.
- Cleanup adds real-PostgreSQL coverage for initial-service writes,
  placeholder fills, direct version inserts, exact retry byte preservation,
  and historical-artifact rejection.  It collects locally but is skipped
  because this host has no Docker daemon; the mandatory real-PostgreSQL CI lane
  passed and no SQLite substitute was added.
- YAPF/isort, mypy over 887 source files, pylint, and `git diff --check` pass on
  both the transition and cleanup trees.
- Platform pin PR #8090 was closed without merge and its branch deleted at the
  operator's direction.  SkyPilot control-plane releases use a direct Helm
  upgrade and do not require a boltz-platform PR.
- At 2026-08-07 17:46 UTC, the production `skypilot/skypilot` release on the
  explicit `gitops-hub-rainier` context upgraded from rollback revision 350
  (`1.1.1116`, commit `cca1f1a8de83d284d884dc4d16e03ad66dadcb52`) to
  revision 351 with the exact v1.1.1135 chart and image digests above.  A
  server-side dry run preceded the apply.  The upgrade reused all live values
  and supplied the complete two-element init-container array with only its
  image fields changed; normalized non-image values before and after have the
  identical SHA-256
  `339b27d73779e49ddc741416a7f06e71f72abef8b5904df0f331dde9bafe3139`.
- The PostgreSQL migration hook succeeded.  The replacement API pod became
  Ready with zero restarts, both credential init containers exited zero, and
  the API, logrotate, migration, GCP-init, and Azure-init image references all
  resolved to v1.1.1135.  The external API reported healthy at exact commit
  `95e0b41b15ad56598e06ef9cb08297815a65f662`; bounded startup-log inspection
  found no traceback, fatal, panic, unhandled-exception, or startup/migration
  failure pattern.
- No `sky serve update` was run.  Existing service `boltz-l4-fleet` version 58
  remained `READY` and retained endpoint
  `http://k8s-skypilot-skypilot-d260f5c163-17791f5150791ad6.elb.us-east-1.amazonaws.com:30001`.
  Its elastic replica count changed from 59/89 immediately before the rollout
  to 73/78 after controller recovery; this count is observational and not a
  fixed-capacity assertion.

Credentialed provider catalog coverage and zero-cost production smoke evidence
remain open gates.  This is only the first transition production release, and
the 30-day observation clock has not started because the retained log sink and
exact zero-event query required by the removal gate are not yet attached.

## Manual test plan

1. Start the transition release against copied production metadata containing
   physical, logical, pool, and pre-marker per-GPU specs.  Confirm each reports
   the same replica unit and candidate/cost ordering as the previous release.
2. Restart the controller during an exact retry and during a
   physical-to-logical rolling bridge.  Confirm no version or existing replica
   changes units.
3. On a zero-cost test pool, exercise one-GPU logical reserved fill, complete
   scale-to-zero, and recovery.  Confirm no paid provider launch is attempted.
4. Use the exact preceding artifact to read, copy/update, and reserialize specs
   written by the transition release; roll forward and confirm exact semantics.
   Also write while rolled back and confirm the transition reader accepts it.
5. Restart once after commit but before apply/quarantine resolution and verify
   the service-level monotonic fence and exact version choice.
6. Restore the transition release and confirm version convergence, endpoint
   continuity, absence of quarantine, and unchanged paid launch authority.

## Open gates

- Draft cleanup PR #1319 is authored in stack #1320.  Its first production
  rollout check passed, but the remaining removal gates are unmet, so it is not
  approved to merge or deploy.
- Transition and cleanup GitHub CI, including the mandatory real-PostgreSQL
  lane, completed successfully.  Credentialed AWS catalog coverage remains
  open because it has not been rerun after the operator refreshed SSO for this
  deployment.
- Production control-plane v1.1.1135 is deployed directly through Helm; closed
  Platform PR #8090 is not part of the release path.  Rollback revision 350 and
  its exact v1.1.1116 image were captured but a rollback drill remains open.
- A second consecutive production release, the retained 45-day log sink, exact
  zero-event query, and 30 continuous qualifying days remain required.
- The Boltz fleet's canonical service YAML currently keeps warm capacity; its
  separate scale-to-zero policy must be reviewed, deployed/applied, converged,
  and drained before any service update can claim scale-to-zero compliance.
- Legacy durable-state inventory and the compatibility-window evidence needed
  to unblock removal are not yet complete.
