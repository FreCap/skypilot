# SkyServe explicit placement contract

_Status: transition PR #1318 is merged, released as v1.1.1135, and deployed.
Normalization PR #1328 is merged as
`ccdb295a4a6065fc72f67571e87a395d1e6ec2a1`, released as v1.1.1143, and
deployed directly with Helm.  Its supported transaction
converted 156 authoritative fieldless/v1 contracts to mirror-free v2; the
post-apply inventory is 156 explicit-v2 rows, one terminal historical
physical/per-GPU row, and seven placeholders, with zero pending supported
changes or ledger mismatches.  Protocol-2 retirement PR #1339 replaced both
incorrect raw cleanup readers with one typed
`_metadata` contract, all-column intent CAS, and a bounded predecessor-receipt
proof for the 49 version-referenced cleanup intents whose committed handoff
flag was missed.  Receipt-lock
hotfix PR #1330 is merged as
`ccae3e8ec2caae74a9baff8f0268078d35e03307`, released as v1.1.1146, and
deployed directly with Helm at revision 357.  All eight requested receipts,
controller ports, and controller health probes converged after that rollout;
all 16 external load-balancer deployments are Available.  Draft cleanup PR
#1319 remains blocked on every measured removal
gate and is not approved to merge or deploy.  Platform pin PR #8090 remains
closed because this SkyPilot control-plane release is deployed directly with
Helm.  Protocol-2 retirement PR #1339 is merged as
`3d98a371e4d320aa1b9f3067088caa94d620c4f9`, released as v1.1.1149, and was
deployed directly with Helm.  Its first held production retirement attempt
failed closed before mutation because six older-writer retained versions have
`created_at=NULL`; revision 360 cleared the hold and restored all eight
controllers and 16 load balancers.  The unrelated direct-shell OOM cleanup
PR #1183 subsequently deployed v1.1.1151 directly with Helm at revision 363,
with the controller hold disabled and the sole Recreate API pod healthy.  The
timestamp-proof hotfix merged as PR #1341 at
`5eb15b544e6fdb5bf43853b5e753d6e24cf4515e`, released as v1.1.1155, and
deployed reader-first and then held at direct Helm revision 365.  Its first
protocol-3 retirement attempt rolled back without a write because the target
service also has exact pickled-`None` placeholder versions 3 and 10.  Those
rows are not placement contracts, are neither current nor active, and have no
YAML, staged controller configuration, replica, catalog, action identity, or
typed resource-action root.  The protocol-4 follow-up below replaces the
blanket same-service-placeholder rejection with a complete locked proof of
that non-executable state; an unproved or concurrently owned reservation still
blocks the transaction.  Created 2026-08-07; last updated 2026-08-08._

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

The normalization release makes that durable-state gate achievable without a
new service version.  An operator-only PostgreSQL normalizer replaces each
valid fieldless public contract and each valid explicit-v1 contract with the
exact mirror-free explicit-v2 contract already understood by v1.1.1135.  New
writes from the normalization release are v2-only.  The normalizer preserves
the raw non-placement state and every service-version identity and runtime
semantic while removing the transition rollback mirror.  The transition-only
physical/per-GPU tuple is never relabeled as a public contract.  It is moved
to an explicit non-electable retired state only
when a strictly newer committed version exists and a dependency-complete
locked proof shows that the old version can never be selected, routed,
recovered, retried, or own a replica.  Retirement moves compiled YAML to a
history-only column, clears the committed-YAML marker, and replaces only the
legacy pickle with `pickle(None)`; the version row and recovery/cleanup
evidence remain.  Every normalization or retirement is digest-CASed and
recorded in a durable run ledger without retaining the legacy pickle.

A canonical protocol-4 `pickle.dumps(None, protocol=4)` version is an inventory reservation, not an executable
placement contract.  Its presence beside a terminal historical version does
not by itself block retirement.  Protocol 4 may retain such a row unchanged
only after proving, under the same freeze and transaction, that it is not the
service's current or active version, owns no replica, committed or submitted
YAML, placement catalog, staged controller configuration, controller receipt,
quarantine state, action-spec identity, image demand, or typed resource-action
root.  It must also be lower than a surviving committed current version; the
central version-commit CAS then permanently returns `STALE_VERSION` if any
caller tries to fill it.  A trailing placeholder with no higher committed
version remains fillable and blocks retirement.  The normalizer binds the
complete placeholder row inventory and external evidence to the retirement
ledger as the distinct durable `stale_placeholder/unchanged` classification.
It is not the fillable `placeholder/unchanged` outcome.  Active Serve requests
and service/controller processes must remain absent before, during, and after
the locked write.

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
- Remove valid fieldless and explicit-v1 placement contracts from
  authoritative PostgreSQL by normalizing them to explicit v2 without
  changing YAML, version identity, replica units, logical fences, catalogs,
  controller configuration, or runtime behavior.
- Make v2 the sole writable and controller-loadable post-normalization
  representation; a completed receipt can never bless fieldless or v1 bytes.
- Retire the transition-only historical tuple only with an auditable,
  fail-closed proof that the exact service version is terminal.
- Distinguish a non-executable placeholder from an owned in-flight reservation
  through explicit persisted and external evidence instead of treating every
  same-service placeholder as an implicit dependency.

## Non-goals

- Changing the public YAML spelling of either existing `spot_placer` value in
  the transition release.
- Silently defaulting an omitted `spot_placer` to per-GPU placement.  Explicit
  serialization is required so a new client cannot be misread by an old
  server.
- Reinterpreting pool `min_workers` or `max_workers` as logical GPU slots.
- Removing request-rate, instance-aware QPS, or on-demand fallback autoscalers.
  Those are separate public contracts and need independent deprecation data.
- Changing a service version, replica unit, task YAML, or placement policy as
  part of persistence normalization.  Ordinary controller restart,
  exact-YAML retry, and unrelated update paths remain non-migrating.
- Treating SQLite as a central/API-server normalization target.  The live
  authoritative migration and its correctness proof are PostgreSQL-only.
- Repairing, guessing, quarantining, or retiring a partial, malformed,
  undecodable, or unknown contract.
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
reader preserves it until normalization either proves and records terminal
retirement or reports it as a cleanup blocker:

| Persisted state | Workload | Engine | Replica unit | Catalog | Cost order | Reserved fill |
|---|---|---|---|---|---|---|
| per-GPU policy without the logical marker | service | dynamic fallback | physical backend | whole-GPU shapes | GPU-slot-hour | exactly one GPU/backend |

The cleanup reader rejects this historical tuple as well as every other tuple
outside the public table above.  The normalizer never converts it to the
nearby logical public tuple.  In particular, a pool cannot use
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
durable migration: ordinary reads never rewrite or backfill committed pickle
bytes.  The operator-only normalizer is the sole byte-rewrite boundary and
uses the same resolver through an explicit raw-state interface described
below.  New objects and intentional new-version serializations write v2;
exact retries continue to read and acknowledge the authoritative normalized
artifact without rewriting it.
The decoder's precedence rules are strict:

1. If all seven contract fields are absent, derive the contract from the
   persisted policy name, containing workload kind, and historical marker;
   an absent historical marker means physical semantics.  This reader uses
   the immediately preceding release's workload truthiness: a fieldless
   `_pool={}` is a service, while `True` or a non-empty mapping is a pool.
2. If `_placement_contract_version == 1`, all other six fields and the
   rollback mirror are required.  Validate their exact tuple, require the
   workload kind to match the containing service/pool object, and require the
   mirror to agree with the replica unit.  The transition and normalization
   compatibility readers accept the five public tuples plus the historical
   physical/per-GPU tuple; the cleanup reader rejects v1 entirely.  A known
   public name cannot disagree with retained contract fields.
3. If `_placement_contract_version == 2`, all other six fields are required
   and the rollback mirror must be absent.  Validate the five public tuples and
   policy-name/workload match.  The normalization release exposes a
   zero-argument, v2-only `persisted_fields()` writer.  During compatibility
   decode, `SkyServiceSpec.__setstate__()` projects supported fieldless/v1
   input to v2 in memory; `SkyServiceSpec.__getstate__()` accepts exact v2
   only and rejects a fieldless, v1, mirrored, or historical object.  New
   service and version database writes use this serialization boundary;
   exact retries of an already committed row remain acknowledgement-only and
   byte preserving.  The transition reader accepts valid v2 state so the
   normalization and cleanup releases are rollback-readable.  The transition
   release emitted v1; the normalization release and every later writer emit
   only v2.
4. A partial field set, boolean masquerading as a version, unknown version,
   unknown value, invalid tuple, workload mismatch, forbidden/missing mirror,
   or v1 mirror disagreement is malformed current state and fails loudly.  It
   never falls back to legacy derivation.

During the normalization release only, successful compatibility decoding of a
supported fieldless or v1 artifact projects the in-memory object to v2 and
removes the mirror; it does not rewrite the authoritative row.  The sole
historical tuple remains transition-only in memory so it can be identified,
copied by recovery code without applying newer validation, and retired.  That
token-gated copy retains explicit v1 plus the false rollback mirror, while
`__getstate__()` and every central write guard reject it.  Thus no
ordinary current runtime object or new write carries a legacy public contract,
while the raw operator path remains able to inventory old bytes exactly.

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

The rollback mirror was written only by the transition release.  Runtime
policy reads the resolved contract and the monotonic service-level logical
fence, not the mirror.  Normalization removes every persisted mirror as part
of v1-to-v2 conversion; no post-normalization write may recreate one.

### PostgreSQL normalization and retirement ledger

Schema revision 037 adds durable normalization-run and row-inventory ledgers;
the version retirement columns; and a per-service requested/loaded
normalization generation, but does not rewrite data at API startup.  A
separately invoked module defaults to dry-run and requires an explicit,
mutually exclusive `--apply-supported` or
`--retire-terminal-historical` mode.  It is available only in a server process
connected to PostgreSQL and never runs from a client or controller-local
SQLite database.  Supported-row normalization and historical retirement are
separate transactions and approvals: a known nonterminal historical tuple
blocks cleanup and retirement but does not prevent safe public-row
normalization.

The normalizer uses a restricted top-level pickle projection: a custom
`pickle.Unpickler.find_class()` maps only the exact
`sky.serve.service_spec.SkyServiceSpec` global to a state-capture proxy while
all other globals retain normal trusted-database decoding.  The proxy requires
one mapping state and never calls `SkyServiceSpec.__setstate__()`.  Protocols
4 and 5 are accepted; another top-level class, a qualified-class alias, a
duplicate/nested service spec, or a non-mapping state is a blocker.  A shared
`materialize_legacy_placement_contract_state()` compatibility interface
resolves the same tuple for the ordinary decoder and normalizer; only the
normalizer is allowed to persist its canonical v2 projection.
Before changing state, the projection pickler must reproduce the complete
source bytes exactly at the source protocol; any difference means a nested
compatibility hook or nondeterministic reducer changed unrelated state and the
row is a blocker.
The result is serialized as an exact protocol-4 `SkyServiceSpec` reduce/state
projection without running a class `__getstate__()`.  Key order and every
nested object/value outside the allowed top-level keys are retained.  The
normalizer classifies each row exactly once:

- a pickled `None` placeholder remains byte-for-byte unchanged because it is no
  contract;
- an explicit, valid v2 public contract remains byte-for-byte unchanged;
- a valid fieldless or explicit-v1 public tuple is rewritten to the same seven
  v2 fields, explicit policy/pool representation, and no rollback mirror;
- the exact fieldless or explicit-v1 historical physical/per-GPU tuple is
  reported unchanged by supported-row normalization and is eligible only in
  the separate terminal-retirement mode; and
- partial fields, an unknown tuple/version/value, a class-path failure, an
  undecodable payload, or any other object blocks the entire apply before the
  first mutation.

Raw classification is immutable.  Protocol-4 retirement adds a separate
manifest classification: after the complete proof below, an eligible
candidate-service raw `PLACEHOLDER` is recorded as
`stale_placeholder/unchanged`; an unproved or unrelated raw placeholder remains
`placeholder/unchanged`.

For every non-placeholder, non-retired version with a live `services` parent,
the raw contract workload kind must also match the parent's exact integer
`pool` discriminator (`0` for service, `1` for pool).  Invalid parent values or
cross-row disagreement are blockers rather than truthiness fallbacks.  Before a
normalization receipt is requested, both its current and quarantine-aware
recovery rows must also agree with the parent's monotonic
`logical_replica_semantics` fence.  This prevents a representation rewrite from
requesting a controller reload that the durable parent fence would reject.

Either apply mode is refused above an explicit dry-run row bound.  At
`SERIALIZABLE` isolation it sets bounded PostgreSQL lock/statement timeouts,
then acquires one transaction-scoped, process-global placement-normalizer
advisory lock followed by
`SHARE ROW EXCLUSIVE` table locks in this fixed order: `services`,
`version_specs`, `replicas`, and `ephemeral_storage_cleanup_intents`.  This
blocks all ordinary row writers while leaving status reads available.  The
operator separately freezes Serve up/update/down requests and proves there is
no pending retry before lock acquisition.  Apply also requires a Recreate API
deployment, the current non-empty pod UID and `all` server role, and exactly
one fresh API-server registry row in total.  That sole row must be ready,
non-draining, and carry the current pod UID and server instance ID; filtering
out a fresh draining or not-ready predecessor before counting is forbidden.  An
operator-supplied freeze digest is recorded separately and never substitutes
for these in-process proofs.  Apply rescans under the locks and commits all row
changes, service generation fences, one run manifest, and one inventory entry
for every scanned row in a single transaction.  Every update includes the
original bytes in its compare-and-swap predicate.

Historical retirement additionally uses a bounded `psutil` scan before lock
acquisition, again under the locks, and after the cross-database postflight.
It recognizes exact service-parent commands and the exact controller title
`sky.serve.controller --service-name NAME --service-incarnation HASH`, where
`setproctitle` exposes the title as the first command-line string rather than
tokenized argv.  The title is set before controller construction.  Target
service parents or controllers, malformed matching commands/titles, access
errors, an over-limit scan, or a changing canonical process digest block the
operation; disappeared processes and zombies are the only ignored races.  The
digest binds the schema version, pod UID, sorted target service identities,
and sorted PID/PPID/create-time/status/identity facts.  Preflight, locked, and
postflight scans must all be empty for target processes and byte-identical.
Retirement is available only in non-pool Serve consolidation mode, where every
service controller is local to that sole API pod, and while the explicit
non-pool controller hold is active.  A separate plain-column global-state scan
must find zero cluster records under the complete
`sky-serve-controller-` prefix, including names from older server identities.
The typed global-state boundary applies the prefix, ordering, and `limit + 1`
overflow check in SQL, so unrelated cluster history is never loaded and an
over-limit namespace fails closed; stale or terminal records still block.  Its
sorted record digest, the
consolidation premise, the exact non-pool parent discriminator, the hold, and
the local-process proof are retained in every retirement ledger entry.

The run manifest records a UUID, normalizer/schema/release versions, start and
completion time, pre/post canonical fleet-inventory digests, the bounded row
count, every classification count, and the operator-supplied freeze evidence
digest.  Each scanned-row entry records its classification, original/result
SHA-256, changed/retired outcome, exact contract projection, and exact service
hash/lifecycle and dependency facts.  No original pickle, YAML, controller
configuration, or task secret is copied into either ledger.  A concurrent
writer, changed preimage, missing row-inventory entry, incomplete manifest,
ledger collision, or timeout aborts the whole transaction.  Rerunning after
success must report zero pending fieldless/v1 changes and verify the completed
manifest, every surviving v2 result digest, every intentional retirement, and
all new rows.  Before the terminal protocol-4 retirement, the latest completed
full-run manifest must cover the exact current `(service_name, version)`
identity set; a later v2 or placeholder row is an explicit
`untracked_current_row` mismatch until a new zero-change apply records it.
After the terminal manifest, an untracked row is accepted without a newer
writer generation only when its version is above the terminal proved current
version and above the maximum version inventoried by that terminal manifest for
the same service incarnation.  It must be either an ordinary explicit-v2
commit with finite `created_at > completed_at` or an exact fillable canonical
placeholder with SQL-NULL `created_at`.  An older, stale-version, retired,
historical, or blocker row remains a mismatch.  The immutable admission
boundary is derived from manifested row identities, not today's mutable
current pointer or a copied dependency fact.  Spec-drift comparison uses the
terminal result for the exact
`(service_name, version, service_hash)` incarnation and does not compare
unrelated mutable status columns.  The ledger's lifecycle epoch is an exact
commit-time observation and must agree across the protocol-4 candidate,
current, and stale entries, but a later lifecycle claim on the same service
hash does not invalidate immutable version bytes or turn manifested rows into
untracked identities.

One same-identity mutation is explicit and asymmetric: a terminal-manifest
`placeholder/unchanged` entry is ordinary and may become a committed
explicit-v2 row for the same service hash when the central writer records a
finite `created_at > completed_at`.  This is the existing fillable reservation
contract.  A `stale_placeholder/unchanged` entry is never fillable and any
spec, YAML, side-state, row-hash, or identity change remains terminal proof
drift.

A post-terminal same-incarnation row follows the high-water rules above.  A
genuinely recreated service hash has no terminal version-number boundary: its
row is accepted only when it is committed explicit v2 with finite
`created_at > completed_at`.  A new-incarnation NULL-timestamp reservation
blocks terminal verification until the central writer either commits it with a
finite timestamp or removes it.  Deleting old-incarnation version rows during
normal service teardown does not erase their immutable terminal audit entries.

A successful protocol-4 retirement manifest containing a validated
`historical_physical_per_gpu/retired` outcome is the terminal writer
generation, including when its proved stale-placeholder inventory is empty.
Terminal identity does not depend on the continued presence of the very stale
entries it protects.  Every later
`--apply-supported` or `--retire-terminal-historical` invocation must fail
before external evidence collection or DML instead of emitting a newer
manifest that could downgrade the durable stale classification to an ordinary
fillable placeholder.  Read-only dry-runs remain available for verification;
the staged cleanup removes the transitional writer after its rollout gates.
The terminal fence is checked against the latest fully validated manifest once
before any external getter and again after acquiring the unchanged advisory and
table locks; two concurrent first-retirement attempts cannot both pass it.

The PostgreSQL ledger is an append-only authority, not a self-authenticating
JSON document.  Schema revision 040 installs `ENABLE ALWAYS` database triggers
that reject every `UPDATE`, `DELETE`, or `TRUNCATE` of a normalization run or
row, including under PostgreSQL's replica session role.  After an
immutable protocol-4 terminal `historical_physical_per_gpu/retired` row exists,
the database also rejects insertion of any later normalization run, including
one attempted by an older protocol-1/2/3 image.  The terminal transaction
inserts its run before its rows, so its own first generation remains atomic;
the insertion fence becomes active for subsequent transactions only after the
terminal row commits.  Revision 040 may be downgraded cleanly before terminal
retirement, but its downgrade must fail closed once a terminal row exists.
Later cleanup must explicitly remove these triggers as part of its reviewed
schema retirement, never weaken them in place or reuse the generic downgrade
as a post-terminal escape hatch.

An advisory-lock wait inside a trigger is not this fence: a repeatable-read or
serializable transaction may keep a snapshot taken before the wait and miss the
newly committed terminal row.  Revision 040 therefore owns one internal
singleton write-fence row containing a monotonic generation, the latest
admitted run UUID and transaction ID, and optional terminal run UUID and
transaction ID.  A run `BEFORE INSERT FOR EACH ROW` trigger takes the unchanged
cross-protocol advisory identity without waiting; failure to acquire it aborts
instead of continuing on the statement snapshot.  An `AFTER INSERT FOR EACH
ROW` trigger then atomically advances the singleton only while its terminal UUID
is NULL.  Its source run must have PostgreSQL `xmin` equal to
`pg_current_xact_id()`, and the singleton records that same transaction ID.
Concurrent read-committed statements re-evaluate the updated row after a wait,
while an older repeatable-read/serializable snapshot receives PostgreSQL's
concurrent-update serialization failure.

Terminal activation is an `AFTER INSERT FOR EACH ROW`, `DEFERRABLE INITIALLY
DEFERRED` constraint trigger on the row ledger.  At transaction end it performs
one conditional `UPDATE ... RETURNING` compare-and-set: the row must be the
exact protocol-4 `historical_physical_per_gpu/retired` tuple, both the run and
row `xmin` values must equal the current transaction ID, and the singleton's
latest run and admitted transaction ID must match them.  A run committed
without such a row can never be activated by a delayed insert.  The compare-and-
set accepts an already-terminal singleton only for the same run and same
transaction, so two or more valid retired candidates in one manifest are
idempotent while a competing activation fails.  Rollback rolls back admission
and terminal activation together; no nontransactional marker may poison the
fence.

The singleton is migration-private state, not an application interface.  It
has exactly one row and the exact columns `singleton boolean`, `generation
bigint`, `latest_run_id uuid`, `admitted_xid xid8`, `terminal_run_id uuid`, and
`terminal_xid xid8`.  `singleton` is the true primary key; generation is
nonnegative; generation zero is equivalent to both admission fields being
NULL; the two terminal fields are either both NULL or both present; and a
present terminal UUID equals the latest UUID.  Both UUIDs have nondeferrable,
restricting foreign keys to the run ledger.  The initial row is exactly
`(true, 0, NULL, NULL, NULL, NULL)`, and upgrade refuses any pre-existing
protocol-4 terminal tuple instead of seeding an incoherent open fence.

`ENABLE ALWAYS` singleton triggers reject every direct `INSERT`, `DELETE`, or
`TRUNCATE`.  Its `UPDATE` guard accepts only one of the two exact state
transitions above at trigger depth two.  This is not a depth-only bypass: an
admission transition must be a one-generation advance to a newly inserted run
whose unforgeable `xmin` is the recorded current transaction, and an activation
transition must retain every admission field and prove a newly inserted exact
terminal row with that same `xmin`.  Once the source trigger has made either
transition, a top-level replay cannot make another valid state change.  The
verified source-table trigger inventory admits no additional trigger that could
issue a transition first.

Revision 040 verifies the complete relation, constraint, index,
trigger/function, owner, ACL, and data envelope: exact columns, PostgreSQL
types, nullability, defaults, primary/check/foreign-key behavior, one coherent
singleton row, relation kind/persistence/RLS state, function owner and revoked
PUBLIC execution, fixed `pg_catalog` search path, security/volatility/parallel
attributes, exact relation/function/event and row/statement level,
always-enabled state, and no `WHEN` predicate, arguments, transition tables,
unexpected constraint metadata, duplicate name, overload, or extra source
trigger.  A partial, disabled, predicate-gated, shadowed, or owner/ACL/config-
drifted catalog is a migration failure, not an adoptable state.

Downgrade is serialized with the writer rather than relying on a point-in-time
terminal query.  It must first acquire the same advisory transaction lock
without waiting, then take fixed-order `ACCESS EXCLUSIVE` locks on the
singleton, run ledger, and row ledger, reverify the exact catalog and singleton,
and recheck terminal state before removing revision 040.  If a writer owns the
advisory authority, downgrade fails busy; if downgrade owns it first, a writer
cannot enter before the catalog removal commits.  A committed terminal UUID
always makes downgrade fail closed.

This database boundary is necessary because a maliciously coherent rewrite of
the run protocol, every row classification and dependency fact, all counts,
and all fleet digests cannot be distinguished from an originally written older
manifest by a pure function.  The shared validator still rejects every partial
or internally contradictory v3/v4 relabel.  PostgreSQL immutability rejects the
whole-manifest rewrite itself, and the post-terminal run-insert trigger keeps
old writers from replacing the latest terminal generation.  Receipt and
operator validation therefore rely on both layers explicitly instead of
pretending that recomputable digests authenticate their own source.

The cleanup-contract and receipt additions introduced normalizer protocol 2.
The NULL-timestamp proof below introduced protocol 3.  The explicit stale
placeholder proof introduces protocol 4.  Ledger verification
continues to accept and exactly validate completed protocol-1 manifests from
the supported v1.1.1143 transaction and completed protocol-2 manifests from
PR #1339; protocol-3 timestamp facts remain frozen exactly as released in
v1.1.1155.  Validation dispatches retirement facts by the recorded protocol
instead of reinterpreting old facts as a later schema.  Every new write uses
protocol 4.  The advisory-lock identity remains unchanged so v1, v2, v3, and
v4 operators cannot run concurrently.
`normalizer_version` has the exact grammar
`^(1|2|3|4):[0-9a-f]{40}$`; apply refuses a build without an exact release
commit.  Protocol 1 keeps its frozen mode/outcome matrix and original
retirement-fact validator byte-for-byte.  Protocol 2 keeps its frozen matrix
and cleanup/receipt validator, requires the exact cleanup-schema-v2 fact set,
and rejects every cleanup-schema-v3-only field.  Protocol 3 uses the same
mode/outcome matrix but requires the v3 timestamp-bound cleanup proof and the
same predecessor-receipt facts for every retired outcome; it rejects the new
stale-placeholder proof fields.  Protocol 4 has a separate retirement outcome
matrix: proved same-service rows use `stale_placeholder/unchanged`, while
unrelated ordinary reservations remain `placeholder/unchanged`; only the
latter is fillable.  It requires the complete v3 proof plus the run-level
cross-row placeholder inventory described below.  Protocol-specific v4
validation covers the retired entry and every `stale_placeholder` entry, not
only rows whose outcome is `retired`; every v1-v3 manifest rejects the v4
classification and both v4-only nested fact objects.  This dispatcher is
shared by complete-ledger verification, controller receipt-manifest
validation, and the global predecessor-receipt scan; an unknown protocol, a
malformed suffix, a partial relabeled v3/v4 proof, or any protocol/fact mismatch
is a blocker.  A coherent whole-manifest downgrade is rejected by the
append-only PostgreSQL boundary above.  Tests include a secret-free exact
manifest/row snapshot of production
run `3bacd32f-888e-4a1f-af87-8f17dd82f168`, not only a synthetic v1 ledger.
The dependency-light `placement_normalization_manifest` module owns this pure
persisted-manifest contract and imports no operator or database state module.
Both operator-side validation and the controller receipt reader call that same
validator.  The receipt reader loads the requested run's complete bounded row
inventory and validates it before returning a pending request, accepting a
completed receipt, or acknowledging and CASing a load; validating only the
selected current and recovery entries is insufficient for protocol 4.  For a
protocol-4 retirement run it also reads the current bounded `version_specs`
rows and parent service hash for every retired candidate service.  This live
query and its required parent-hash observation are scoped exactly to those
candidate names; unrelated services that merely appear in the full fleet
manifest are not live-proof inputs.  The query projects exactly the frozen
protocol-4 revision-038 columns listed below, so a later live-schema addition
cannot retroactively invalidate an old receipt.  While that
same service incarnation exists, every manifested stale identity must remain
the exact canonical, unretired pickled-`None` full row/column postimage, and
every additional canonical placeholder whose version is at or below the
proved current version is an omitted stale row and blocks.  The manifested
retired candidate must also remain present while that same-hash parent exists
and must retain the exact terminal `spec`, `yaml_content`,
`retired_yaml_content`, `retired_at`, `retirement_reason`, and
`retirement_run_id` postimage; changing or deleting the tombstone blocks even
though its `spec` is canonical pickled `None`.  If the old parent is absent,
missing stale and retired rows are permitted because the immutable audit
remains in the manifest.  If the name belongs to a different incarnation, an
unchanged old stale postimage at the same name/version would be implicitly
reassociated and blocks.  Reuse of an old version number is nevertheless
unambiguous and allowed when that row is instead a committed explicit-v2
postimage with a finite `created_at` strictly after manifest completion; the
different parent hash and later commit boundary prove that the old row was
removed and the new incarnation created a new one.  A new-incarnation
placeholder, including one at an old version number, remains ambiguous and
blocks.  A
later ordinary placeholder is permitted only when its version is above both
the proved current version and the terminal same-incarnation manifested
high-water mark, and its `created_at` remains SQL NULL, matching the central
reservation writer; it remains `placeholder/unchanged` and fillable.
This
live snapshot binding prevents a coherently rewritten ledger from omitting or
relabeling an unchanged stale placeholder without making later reservations
part of the terminal proof, and is repeated inside the acknowledgement
transaction.
The protocol-1 through protocol-3 validators retain the historical
`same_service_placeholder_dependency_absent=true` fact exactly.  Protocol 4
must neither emit nor accept that fact: its complete stale-placeholder
inventory replaces the blanket absence contract, so persisting both would be
contradictory rather than additional evidence.
The persisted `schema_revision="037"` is the immutable placement-normalization
ledger schema identifier for protocols 1-4, not the central database migration
number.  Protocol 4 nevertheless requires the current full column-hash maps to
contain exactly the frozen revision-038 `version_specs` columns
`service_name`, `version`, `spec`, `yaml_content`, `submitted_yaml_content`,
`created_at`, `created_by`, `quarantined_at`, `quarantine_reason`,
`retired_yaml_content`, `retired_at`, `retirement_reason`,
`retirement_run_id`, `placement_catalog`, `controller_config`,
`controller_config_digest`, `controller_config_snapshot_id`,
`controller_applied_at`, `resource_action_spec_identity`, and
`resource_action_spec_identity_sha256`.  The action-identity columns must hash
as SQL NULL for every stale placeholder.  Later live-schema additions do not
retroactively change this protocol-4 column contract.

For each historical candidate service, protocol 4 selects every exact
`PLACEHOLDER` row from the already bounded locked version inventory.  Each
must use bytes exactly equal to the central writer's canonical
`pickle.dumps(None, protocol=4)`, remain byte-for-byte and column-for-column
unchanged, and have SQL NULL in exactly `yaml_content`,
`submitted_yaml_content`, `placement_catalog`, `controller_config`,
`controller_config_digest`, `controller_config_snapshot_id`,
`controller_applied_at`, `quarantined_at`, `quarantine_reason`,
`retired_yaml_content`, `retired_at`, `retirement_reason`,
`retirement_run_id`, `resource_action_spec_identity`, and
`resource_action_spec_identity_sha256`.  Its version
must be strictly lower than the one surviving committed explicit-v2 current
row, it must be absent from `active_versions`, and its exact replica count must
be zero.  The service-wide unknown-version-replica proof remains zero.  After
this proof its separate manifest classification changes from raw `PLACEHOLDER`
to durable `stale_placeholder`; its raw classification and database row do not
change.  Its contextual dependency facts must also retain exact
`service_present=true`, `service_pool=0`, `service_active=false`,
`quarantined=false`, `controller_applied=false`, and `retired=false` values and
the same nonempty service hash and positive lifecycle epoch as the retired
candidate; contradictory fact booleans invalidate the complete manifest even
when the side-column hashes remain clean.

The preflight, locked, and postflight image-demand and typed resource-action
scans include both historical candidates and these stale placeholders.  Every
placeholder count must remain zero and every digest must remain stable across
the three checkpoints.  Each `stale_placeholder` ledger entry adds exactly one
top-level fact, `stale_placeholder_evidence`, whose value is an exact-key map:
`schema` (the literal `skyserve-stale-placeholder-retirement-v1`),
`service_name_sha256` (64 lowercase hex), `version` (the exact positive row
version), `original_row_sha256` (the ledger preimage row digest),
`strictly_newer_committed_version` (an exact integer greater than `version`),
`image_demand_count` and `resource_action_root_count` (exact integer zero), the
two 64-hex `image_demand_sha256` and `resource_action_root_sha256` digests, and
exact booleans
`state_clean=true` and `fill_stale_proved=true`.  No missing or extra nested key
is accepted.

Each retired candidate adds exactly one top-level fact,
`same_service_stale_placeholder_proof`, whose exact-key value contains the same
literal `schema`, the service-name digest, surviving `current_version`, exact
nonnegative `placeholder_count`, exact-zero aggregate `image_demand_count` and
`resource_action_root_count`, a 64-hex `inventory_sha256`, and
`fill_stale_proved=true`; no missing or extra nested key is accepted.  The
inventory digest is domain-separated canonical
compact JSON of
`{"schema":"skyserve-stale-placeholder-retirement-v1",
"service_name_sha256":...,"current_version":...,"placeholders":[...]}`;
`placeholders` is the version-sorted list of each complete exact-key
`stale_placeholder_evidence` map.  Full-manifest validation reconstructs this
envelope from all same-service `stale_placeholder` entries, proves each
entry's original and result row/column hashes are identical, proves every
listed side-column hash equals the canonical SQL-NULL hash, and proves the
placeholder's `contract_projection` is exactly NULL.  The surviving current
entry must be committed explicit v2 and its service hash/lifecycle epoch must
equal both the retired candidate and every stale entry.  A missing, extra,
duplicated, fillable, active, stateful, evidence-bearing, or substituted
placeholder invalidates the entire run.  The empty list uses the same envelope
and is valid.  Older-protocol all-row validation rejects either v4-only
top-level fact even if an attacker also relabels the manifest.

For a supported fieldless or v1 tuple, the result pickle is a real
`SkyServiceSpec`.  Its raw top-level mapping differs only by the explicit
policy/pool keys, seven placement keys, and removal of the rollback mirror. The
protocol-4 bytes of the ordered tuple of all unaffected `(key, value)` pairs
must match before normalization and after raw-state recapture; this preserves
key order, nested values, and aliases across unaffected keys.  The version
number, YAML and submitted YAML,
placement catalog, controller configuration, quarantine data, creation
provenance, service logical fence, election pointers, active versions, and
replica rows are not mutated.

For the historical tuple, retirement atomically copies `yaml_content` to
`retired_yaml_content`, sets `yaml_content` to SQL NULL, replaces `spec` with
the protocol-4 bytes for `pickle(None)`, and records `retired_at`, reason, and
run UUID.  It retains submitted YAML, provenance, placement catalog,
controller configuration, quarantine data, and the version row.  Current and
v1.1.1135 election/recovery/launch queries require non-NULL `yaml_content`, so
neither binary can treat the retired row as committed.  The current
exact-commit path also rejects `retired_at`; v1.1.1135 rejects a stale refill
because retirement requires a strictly newer committed version.  A
schema CHECK makes the retirement representation all-or-none: a non-NULL
`retired_at` requires NULL `yaml_content`, non-NULL `retired_yaml_content`,
reason, and run UUID, while a live row must have all retirement fields NULL.

Retirement is allowed only when the dependency-complete predicate proves all
of the following under the writer locks, regardless of service status or hash:
a live `services` row exists because the offline v1.1.1135 compatibility proof
and retained history both rely on the same-name version high-water mark; a
strictly newer committed version exists;
`services.current_version` is strictly greater; the version is not active; it
owns no exact-version replica row, and the service owns no replica whose
version is NULL or otherwise unknown; its own `quarantined_at` is NULL; it has
no non-quarantined
controller-applied receipt; it is not selected by the exact quarantine-aware
recovery expression; and no bridge,
catalog activation, in-progress retry, controller-config recovery, or cleanup
record depends on it.  The container-image catalog must report zero exact
service-version demands in `WARMING`, `READY`, or `FAILED` across every
incarnation hash and target-scoped derivative; that cross-database
snapshot is taken before and after the locked Serve transaction while requests
are frozen.  Missing, hash-mismatched, and terminal services do not bypass
those checks.  Historical service hashes are opaque nonempty strings and may
contain `:`.  Any-incarnation owner parsing therefore anchors the exact
`:v<version>` suffix from the right and conservatively accepts every nonempty
hash prefix; it must not use a `[^:]+` hash regex that can silently discard a
valid legacy owner.  An ambiguous or malformed possibly matching owner blocks
instead of producing zero-demand evidence.  The same bounded pre/locked/post
protocol requires zero exact
`ProviderLaunchContentSourceV1` roots for the candidate `(service_name,
version)` in either `api_resource_actions.immutable_spec` or
`serve_resource_action_shadow_samples.immutable_spec`.  Both complete bounded
stores are scanned.  Every Serve candidate is parsed through the typed
`ServeReplicaActionSpecV1` decoder and checked against its outer indexed
identity; a corrupted outer domain/type/name cannot hide a typed Serve root,
while valid non-Serve API actions remain out of scope.  Each checkpoint digest
binds the complete validated root-store snapshot, not only matching rows.  A
malformed possibly matching row, scan overflow, nonzero root count, or changed
   full-store digest blocks.  Shadow retention may delete a completed
   represented root without creating an API-action root, so retirement does not
   assume universal root retention.  Instead the candidate's live parent must
   have the exact inert `resource_action_mode='legacy'` default and NULL mode
   transition timestamp under the service-table writer lock.  Shadow admission
   requires `shadow`, authoritative admission requires its separate activated
   path, and the held consolidated controller plus zero legacy-controller
   inventory closes the old writer path.  Thus retention may remove a root (and
   a changed complete-store digest blocks), but no authority can create or
   recreate a target root during the three-checkpoint proof.
The proof deliberately treats retained terminal roots as dependencies rather
than trying to infer that no down action, attempt, request, coverage, or shadow
reference can still reach them.

Retirement cleanup proof is a typed protocol, not a service-wide count or a
truthiness test.  Task YAML serializes internal metadata under the exact
top-level `_metadata` field; `metadata` is not an alias.  A dependency-neutral
cleanup-contract parser owns that spelling and returns either no scope or an
exact `EphemeralStorageScope` containing nonempty `resource_scope`,
`scope_id`, and `storage_generation` strings plus a typed
`storage_mounts: list[str]`.  It rejects a partial scope, wrong type, empty
identity, unknown scope field, or a `scope_id` that does not equal the
canonical function of scope and generation.  The central version-commit
handoff reader and the retirement normalizer both use this one interface;
neither reads a second raw key, calls `getattr`, or treats a false-ish malformed
value as absence.

The retirement cleanup protocol initially accepts only a zero-deletion
target graph.  Under the existing cleanup-intent table writer lock it scans a
bounded, stable-PK-ordered inventory for every historical candidate service.
Each row must have exact nonempty string identity fields, an exact integer pool
bit matching the non-pool parent, a positive exact-integer lifecycle epoch no
greater than the parent epoch, a finite nonnegative creation time, and an exact
integer provisional bit.  Its YAML and the retained cleanup YAML of exactly
one version must be byte-identical and must independently parse to the same
canonical scope/generation/scope ID.  Each intent maps to exactly one retained
version and each historical candidate maps to exactly one intent; collisions,
missing matches, foreign scopes, or overflow block the whole transaction.
The parent `hash`, `resource_scope`, YAML scope, and intent scope must all be
the same nonempty identity.
Every matched version preimage must still be a genuinely committed live row:
`yaml_content` is a string and `retired_at` is NULL.  Protocol 3 derives its
timestamp proof from the already locked full version scan, whose total row
count is bounded by the manifest `row_bound`.  For each candidate service it
selects every such live row, orders by exact `(service_name, version)`, and
requires a positive exact-integer, unique version.  The service version is an
immutable primary-key component allocated monotonically by the version commit
path; a lower retained version therefore predates a higher retained version.

The canonical timestamp inventory is a JSON list in that order.  Each row is
the exact projection
`[service_name_sha256, version, "committed", created_at_mode,
normalized_version_created_at_or_null, legacy_boundary_version_or_null,
normalized_boundary_created_at_or_null, matched_intent_key_sha256_or_null,
normalized_intent_created_at_or_null]`.  Normalized timestamps are finite,
nonnegative JSON numbers; booleans, NaN, infinity, negative values, strings,
and every other type block.  The digest is SHA-256 of canonical compact JSON.
The ledger records exact candidate-service, live-row, matched-intent,
legacy-NULL-row, and boundary-service counts.  Live-row count equals the
projection length and is no greater than `row_bound`; matched-intent count
equals both cleanup match count and cleanup-intent inventory count.  Each
service has either zero NULL rows and zero boundary, or one nonempty strict
NULL prefix and exactly one first-finite boundary.  Consequently global
boundary count is exactly the number of candidate services using a legacy
prefix, never greater than either candidate-service count or NULL-row count.

A normal row uses mode `finite`, has no boundary fields, and requires
`intent.created_at <= version.created_at` when matched.  Older version writers
did not populate the nullable `version_specs.created_at` provenance column, so
a retained row may instead use exact mode `legacy_prefix_null`.  All live NULL
rows for that service must be lower versions forming the strict prefix; at
least one later live row must have a finite timestamp; and every matched NULL
row's exact one-to-one YAML/scope/generation intent must have a finite creation
time no later than the first-finite boundary.  The immutable monotonic version
order proves that the NULL candidate was committed before that boundary, while
the byte-identical YAML and unique scope/generation match prove that the
boundary-tested intent belongs to that exact version.  Neither fact alone is
sufficient.  A NULL after the boundary, an all-NULL service, a malformed
timestamp, a non-prefix version topology, or an intent newer than its matched
finite row or legacy boundary blocks.

Each retired candidate records its exact finite-or-legacy mode, matched intent
key hash, and boundary version or NULL.  A candidate-local proof-binding digest
is recomputed from the cleanup schema, all global timestamp digests/counts,
candidate identity/mode/boundary, and matched intent key hash.  This rejects
independent valid-hex digest, count, mode, boundary, or match substitutions.
Protocol 2 remains readable only through its frozen all-finite
cleanup-schema-v2 validator and rejects every v3-only field; any matched NULL
requires protocol 3.  One generation mapping to multiple versions, one version
mapping to multiple generations, or a reused-generation topology blocks rather
than selecting a winner.

The pure cleanup projection performs no filesystem, provider, storage
construction, or deletion call.  On both the candidate and its matched intent
it requires `file_mounts`, `storage_mounts`, and `volumes` to be absent/NULL or
exact empty mappings; `volume_mounts` to be absent/NULL or an exact empty list;
`workdir` to be absent/NULL; and scoped `storage_mounts` to be exactly `[]`.
The same zero-target requirement applies to every intent/version match repaired
in this first protocol.  Nonempty ownership remains unsupported even when an
intent appears to cover it.

The currently deployed writer missed committed handoff because it inspected
`metadata` while `Task.to_yaml_config()` emitted `_metadata`.  In the same
serializable retirement transaction, and before evaluating the final cleanup
predicate, the current protocol CAS-adopts each exactly matched stale intent from
integer `provisional=1` to `0`.  The CAS binds the primary key and every other
column preimage and changes no YAML, identity, pool, epoch, or timestamp.
Already committed integer-zero matches are unchanged; any other value blocks.
The operator validates and freezes the complete 49-row production repair plan
before issuing the first CAS; this first protocol deliberately repairs every
exact service-scoped match, not only the historical candidate's row.  It
requires the affected-row count to equal the planned one-to-zero count and
recomputes a post-inventory digest whose only permitted delta is precisely
those planned `provisional: 1 -> 0` fields.
All adoptions and the historical retirement commit atomically, so a later
retirement blocker rolls back the flag repair as well.  This is a durable-state
repair, not external cleanup, and cannot launch or delete provider resources.

The ledger stores the cleanup-proof protocol, complete intent inventory count
and secret-free pre/post digests, adopted count, candidate match count, hashed
intent key, candidate/intent YAML digests, zero-target projection digests and
counts, exact retired-YAML preservation, and the current-reader and
v1.1.1135 omission-lossless proofs.  It stores no raw YAML, path, secret, or
provider credential.  Postimage verification requires every intent column
except the explicitly adopted bit to remain byte-for-byte unchanged.  The
current cleanup reader continues to read `coalesce(yaml_content,
retired_yaml_content)`, so its inventory is unchanged; v1.1.1135 loses the
candidate's live YAML but retains the exact committed, zero-target intent.
The orphan-child-mode accessor uses the same history-only fallback instead of
interpreting `pickle(None)` plus NULL live YAML as an unknown child mode.

If the versioned controller-configuration protocol is active for any row of
the service, the strictly newer elected/recovery successor must carry and
validate a complete `controller_config`, `controller_config_digest`, and
`controller_config_snapshot_id` tuple before the historical row is eligible.
The ledger retains all pre/post column and payload digests and the terminal
proof, but none of the legacy pickle bytes.

Normalization is semantic persistence maintenance, not a service update, and
does not satisfy the separate production scale-to-zero YAML contract.  Apply
sets an affected live service's requested normalization run UUID and clears
its loaded receipt.  Before any run is requested, the deployed normalization
reader must still start a valid supported fieldless, v1, or historical service
so the no-rewrite Helm rollout is behavior preserving; malformed and
placeholder selected rows fail.  Once a run is requested, every startup
verifies from raw persisted bytes that the recovery and current live specs are
explicit v2 and mirror-free.  A pending receipt also verifies the exact
completed-ledger result digest, then owner-fenced CASes that same UUID into its
durable loaded receipt with its image commit, PID/IP, boot identifier, and
timestamp.  On later boots, an inventoried loadable row still binds its exact
result digest and service incarnation, but not the old lifecycle-operation
epoch: an ordinary later lifecycle lock may advance that epoch without
changing the immutable version.  The requested UUID must still resolve to a
well-formed completed-run manifest.  A version absent from that run is accepted
as a later ordinary version only when its immutable `created_at` is strictly
after the manifest's `completed_at`; an absent inventory row for a version that
already existed when the run completed is corruption and fails closed.  A
manifested `placeholder/unchanged` row may subsequently be filled through the
v2-only central writer; `stale_placeholder/unchanged` never may.  For a
fillable row, the completed check still binds that inventory
identity to the same service incarnation, but does not compare the obsolete
placeholder digest or lifecycle epoch.  Both cases remain subject to raw v2
validation; any other inventoried non-loadable outcome rejects.  Pending
acknowledgement remains exact-epoch fenced.
Before historical retirement may replace a service's requested receipt with a
new run UUID, a locked global receipt scan selects every service for which any
requested or loaded receipt column is non-NULL.  It is ordered by exact service
name, uses `limit + 1` against an explicit bound, and hashes the complete typed
tuple.  It therefore catches a NULL requested UUID with a stray loaded field as
well as ordinary pending/mismatched state.  Each requested UUID must equal its
loaded UUID; its protocol-1, protocol-2, protocol-3, or protocol-4 manifest,
per-version
ledger rows,
and raw current/recovery specs pass the shared full validator.  The loaded
image commit must be one of an explicit operator-supplied set of approved exact
40-hex v2-writer commits; PID is a positive exact integer; IP is NULL or a
nonempty string; boot ID is exactly 32 lowercase hex; and `loaded_at` is
finite and no earlier than the manifest completion.  A completed receipt is
historical one-time load evidence: it remains bound to the immutable service
incarnation hash and raw current/recovery specs through its ledger, but its
loaded PID/IP and ledger lifecycle epoch need not equal the current owner after
a later controller restart or lifecycle advance.  Exact current-owner and
epoch fencing applies only to the pending read/CAS path.  Unknown protocols,
overflow, duplicate names, partial state, orphan manifests, or unapproved
commits block.  The approved
commit set is itself hashed into the operator freeze evidence rather than
inferred from tags.  Protocols 2, 3, and 4 store the caller's exact 64-hex freeze digest
and the canonical digest of the sorted approved-commit set in each retirement
row, and the run manifest stores the canonical SHA-256 of
`{"approved_loaded_image_commit_sha256": <set digest>,
"operator_freeze_evidence_input_sha256": <caller digest>}`.  Ledger
verification recomputes that composition and requires it to equal the manifest
freeze digest, so neither input can be substituted independently.

A clean pending receipt is a hard blocker, not permission to overwrite
evidence.  The retirement ledger records the fully validated predecessor
receipt inventory count and digest.  The eight v1.1.1143 requested receipts
converged under exact v1.1.1146 commit
`ccae3e8ec2caae74a9baff8f0268078d35e03307`; that commit must be explicit in
the production retirement allowlist.  On PostgreSQL the pending
acknowledgement query locks exactly `services` with `FOR UPDATE OF services`,
not every joined relation: its requested/current/owner predicates are mutable
only under that row lock, while version and completed-ledger rows are
immutable.  An unqualified `FOR UPDATE` is forbidden because the manifest is
outer-joined and PostgreSQL cannot lock the nullable side.  Real-PostgreSQL
coverage must execute the pending path and prove the exact owner-fenced CAS;
SQLite coverage is not a substitute.
Supported-row normalization may run while controllers serve because it does
not change decoded semantics.  Historical retirement may run only after the
Helm value `serve.controllerHold=true` has restarted the sole Recreate API pod
with the exact `SKYPILOT_SERVER_SERVE_CONTROLLER_HOLD=true` recovery hold,
every controller for the affected service has been terminated, and the locked
freeze proof shows
no pending, applying, staged-config, retry, or locally cached mutation can
publish.  The hold is cleared only after retirement commits; fresh
controllers then load the successor and owner-fenced receipt.  This process
quiescence is part of the retirement predicate rather than operator prose.
The same explicit hold participates in the fresh database authorization at
both admission and the terminal provider boundary: an already-persisted
non-pool replica-launch request replayed after the Recreate restart cannot
provision while maintenance is active.  Pool launch authorization remains
available only when the current parent row's raw discriminator is exactly the
integer `pool == 1`; truthy malformed values fail closed.
All requested/loaded UUIDs and new-boot receipts must converge before the
zero-change dry-run and legacy-event observation clock start.

The cleanup release adds a pre-controller bootstrap guard: before any Serve
controller recovery is spawned, it verifies a completed manifest, scans the
authoritative rows with the cleanup decoder, and requires zero fieldless, v1,
historical, malformed, or ledger-mismatched state.  A restore/import that lacks
that proof leaves controller spawning disabled.  The restore chart/runbook must
first deploy the normalization release or a newer v2 writer with the explicit
controller-hold flag, run normalization, and clear the hold only after
requested/loaded receipts
converge.  Any later legacy decode, restore, or import invalidates the clean
generation and resets the observation clock.

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

The contract semantics are immutable for a committed
`(service_name, version)`.  The normalization boundary may replace only a
fieldless or explicit-v1 encoding with its exact v2 encoding; it cannot change
the resolved tuple.  A supported public copy that overrides neither
`spot_placer` nor `pool` carries the exact contract forward and serializes it
as v2.  The sole historical tuple may be copied only through the token-gated
internal compatibility path and remains non-serializable v1 state.  After the
cleanup gates, a historical, fieldless, or v1 tuple is rejected during decode.
An explicit
policy or workload-kind override resolves a fresh contract and goes through
ordinary validation.  The public constructor cannot accept a resolved
contract; only a token-gated internal copy path may preserve a supported
tuple.

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

1. A committed version never changes contract semantics or replica units.
   Digest-CASed fieldless/v1-to-v2 normalization is the sole permitted
   in-place encoding rewrite and preserves its version identity and complete
   raw non-placement state.
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
   Normalization validates the current and recovery contract projections
   against this fence before requesting a controller reload.
6. Logical-to-physical recovery/update and logical blue-green update remain
   rejected.
7. Pool counts remain physical and cannot select a logical contract.
   Every live parent/version pair must also agree on exact pool versus service
   identity; retirement is restricted to exact non-pool parents.
8. A missing or malformed current contract fails loudly.  Compatibility
   defaults exist only in the persistence decoder.
9. Zero-cost supply, reconciliation targets, and paid launch authority remain
   separate typed signals.  A placement refactor cannot broaden paid authority.
10. v1.1.1135 is an offline v2-read compatibility artifact, not a deployable
    post-normalization rollback: it writes v1 and has no controller-hold gate.
    The normalization release is the minimum post-apply server image.  Before
    apply, rolling back to v1.1.1135 remains safe; after apply it is forbidden.
11. A fresh pool persists a non-empty rollback-readable `_pool` mapping;
    fieldless `_pool={}` retains the preceding reader's service meaning.
12. Normalization and retirement never delete or reuse a version number.
    Historical retirement requires a strictly newer committed successor and
    retains the old history row with NULL committed YAML, so the service's
    `MAX(version)` and next-version identity do not change.
13. A historical tuple with a current/active pointer, replica row,
    non-quarantined applied receipt, or other recovery dependency is never
    retired; one such row aborts only the separate retirement phase.
14. Every scanned row has an inventory entry, every changed/retired row has
    matching preimage/result evidence and exact incarnation/lifecycle proof,
    and the run manifest covers the canonical fleet digest.  A partial ledger,
    postimage mismatch, or incomplete controller-load receipt fails closed.
15. The current serializer and both central version-write boundaries accept
    only the exact `SkyServiceSpec` base type with explicit v2, mirror-free
    state; subclass reducers cannot bypass the boundary.  The raw normalizer is
    the sole bypass.

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
and was released as v1.1.1135.  Its control plane was deployed directly with
the reviewed Helm upgrade; no boltz-platform PR is part of this release path.

### Normalization PR and release

The normalization change adds revision 037's run/inventory ledgers and durable
controller-load receipts, an explicit raw-state classifier/materializer, and
the dry-run/two-phase apply operator command.  It does not run a pickle data
migration during Alembic/API startup.  The release is deployed through Helm
with the transition reader still present.  The operator then freezes Serve
version mutations, proves an all-green bounded dry-run, applies supported
normalization, separately reproves and retires any terminal historical row,
restarts affected controllers, and proves receipt convergence and an
idempotent zero-change rerun.  An exact offline v1.1.1135 artifact must read
every normalized v2 public tuple and reject election of the retired historical
fixture before production apply.  Because that release can write v1 and lacks
the controller hold, it must never be deployed after apply; the normalization
release is the minimum server rollback floor.

The protocol-4 follow-up adds PostgreSQL-only schema revision 040 for the
append-only run/row triggers and the post-terminal run-insert fence described
above, including its single internal transaction-bound gate row.  It changes no
placement pickle during startup.  Reader-first rollout
must prove the trigger definitions are installed before the held operator
transaction; after the terminal row commits, rollback to any image that does
not understand protocol 4 and downgrade of revision 040 are forbidden
independently of the application-level writer fence.

Production baseline inventory found 155 valid fieldless public contracts,
seven pickled-`None` placeholders, and one fieldless transition-only
physical/per-GPU contract.  The historical row is eligible only if the
production apply independently reproves its terminal state under lock; these
counts are evidence, not hard-coded migration input.

### Blocked cleanup PR [#1319](https://github.com/boltz-bio/skypilot/pull/1319)

The stacked cleanup removes the all-versioned-fields-absent decoder, the v1
decoder and rollback mirror, the historical physical/per-GPU tuple, and dead
compatibility machinery only after inventory proves that no fieldless, v1, or
historical contract remains in any authoritative or recoverable artifact.  A
fieldless or v1 row not covered by a complete, digest-verified normalization
ledger blocks this removal.  It writes and reads only mirror-free v2.  A
restore of older state must run the normalization release with controller hold
before cleanup code may recover controllers.  It deliberately retains the public
`dynamic_fallback` physical preset required by pools.  Public policy removal is
a separate breaking design/PR.  Keep this cleanup draft or otherwise blocked;
do not merge it merely because no old replicas are currently READY.

Transition PR #1318 and draft cleanup PR #1319 originated as gh-stack #1320.
The cleanup branch must be rebased on the merged normalization release and
retain its draft/blocked status.  All three changes link this design; the
cleanup PR states the exact merge gate below and remains blocked until its
evidence is attached.

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
5. Deploy the normalization release with no automatic data rewrite.  Freeze
   Serve up/update/down mutations, run the PostgreSQL dry-run, and require
   exact expected classifications, zero malformed/unknown/blocking rows, an
   inventory row count below the configured bound, exact freeze-evidence
   digest, and exact v1.1.1135 rollback-read/stale-retry proof.  Apply
   supported normalization only at `SERIALIZABLE` isolation with the
   fixed-order bounded writer locks.  Verify its postimages, then enable the
   Serve-controller recovery hold with `serve.controllerHold=true` and
   terminate every affected controller.
   Do not enable that hold until every requested supported-normalization load
   receipt is complete and matching.  If a receipt remains cleanly pending,
   diagnose controller startup/ownership and roll forward with a v2 writer;
   never overwrite it with a retirement request.
   Reprove that no controller process, pending/applying update, staged-config
   publication, retry, recovery launch, or replayed persisted provider request
   remains before invoking the separate historical retirement mode.  Verify
   directly that the provider-boundary fence rejects a known-valid non-pool
   launch context under the hold.  That mode must reprove its complete
   dependency matrix under a new transaction and explicit approval.  It must
   also bind the exact `_metadata` cleanup graph, atomically adopt only exact
   committed intent matches, and prove zero deletion targets and unchanged
   cleanup YAML/intent postimages.  Any
   digest CAS miss, unknown state, missing inventory entry, or timeout rolls
   back that complete phase; a known nonterminal historical tuple leaves
   normalization committed but retirement blocked.  Keep the hold active
   throughout this transaction.
   Every retirement-protocol rollout is explicitly reader-first and
   two-phase.  Protocol 2 completed that sequence with v1.1.1149, but its
   retirement transaction failed closed before mutation on the legacy NULL
   timestamps.  Protocol 3 completed its reader-first v1.1.1155 rollout and
   entered the separate hold, but its transaction rolled back before DML on
   the blanket placeholder guard.  Revision 366 then restored the same exact
   image with `serve.controllerHold=false`; all eight controller health probes
   and all 16 load-balancer deployments recovered.  For protocol 4, first
   deploy the exact corrected image directly with Helm `--reuse-values` while
   `serve.controllerHold=false`.  Let the Recreate pod recover controllers and
   require every existing protocol-1/protocol-2/protocol-3 receipt and manifest
   to pass its frozen validator, all eight requested receipts to remain converged, all
   controller ports and health probes to recover, all endpoint/LB identities
   and 16 LB deployments to remain healthy, and
   committed/elected/applied versions, replica inventory, and paid-capacity
   authority to remain unchanged.  Only after attaching that reader-first
   evidence may a separate Helm revision set the hold to true.  Its final
   preflight must reproduce all 164 rows and the exact preimage digest, classify
   exactly 156 explicit-v2, one historical, and seven placeholders, prove
   `{3, 10}` is the complete same-service placeholder set below committed/current
   version 51, and prove every required side column, replica count, image demand,
   and action root is zero/NULL before applying protocol-4 retirement.  The hold
   revision must retain the exact protocol-4 image and chart pins.  Post-apply,
   require both placeholder rows to retain identical full row/column hashes;
   among preexisting version and cleanup rows, only historical version 2 and
   the 49 exact `provisional: 1 -> 0` bits may change.  Recompute the complete
   protocol-4 ledger and postimage before clearing the hold.
6. Clear the recovery hold and start each affected live controller under the
   normalization release's transition-compatible decoder.  Verify
   committed/elected/applied convergence, active versions, endpoint and LB
   identity, replica inventory, paid launch authority, and zero decode/fence
   errors.  Rerun the normalizer in dry-run mode and require zero changes plus
   complete run/inventory/postimage verification and requested/loaded
   generation receipts for every affected controller, including the new
   retirement run separately from the predecessor receipts.  The test service's
   committed/current version must remain 51 and its requested/loaded protocol-4
   receipt must converge after unhold.  Start the zero-event
   clock only after this final reload and query.
7. Migrate eligible physical GPU services only through an explicit rolling
   update.  Require every old replica to drain and the logical bridge to
   converge before counting a service as migrated.
8. The normalization release is the minimum post-supported-apply server-image
   rollback floor.  Its placement-contract-v2 serializer and central write
   guards prevent reintroduction.
   v1.1.1135 remains an offline read fixture only and must not be deployed
   against the post-apply database because it can write v1 and has no
   controller hold.  Before apply, the ordinary Helm rollback to v1.1.1135 is
   still available.  After apply, roll back chart/config changes while pinning
   the normalization-or-newer image, or restore the pre-apply database backup
   before deploying v1.1.1135.  Normalized rows are mirror-free v2.
   Once any protocol-4 run manifest or requested receipt exists, the exact
   protocol-4-capable release becomes the stricter live-database rollback
   floor: v1.1.1155 and every older image reject the `4:<commit>` identity and
   must not be started against that database.  Rolling back chart or config
   after that point must retain the protocol-4-capable image.  Deploying an
   older image requires first restoring a verified pre-protocol-4 database
   backup under the controller hold.  Historical retirement is intentionally
   irreversible in the live database:
   the retained history row and strictly newer committed row preserve the
   version high-water mark, while NULL committed YAML makes the retired row
   invisible to old and new election/recovery readers and stale commit retries
   fail against that successor.  Restore from a pre-apply backup is permitted
   only through a runbook that deploys the normalization reader/writer with the
   controller hold active and normalizes before controllers resume.  No older
   binary may perform partial-restore orphan cleanup against a retired row.
9. Cross-release proof uses the exact v1.1.1135 and normalization artifacts:
   v1 writes -> normalization reads and normalizes; normalization v2 writes ->
   offline v1.1.1135 read only; and v2 writer copy/reserialize -> v2 reread.
   Tests do not bless a v2-to-v1 round trip.  Replica unit, YAML, catalog,
   reserved-fill mode and shape validation, and fence remain identical.

No production `serve update` may use a canonical spec that violates the Boltz
scale-to-zero contract.  Control-plane deployment and service-policy
deployment are separate approvals and rollback units.

## Removal gates

The blocked cleanup may merge only when all are true:

- Central PostgreSQL and every authoritative supported controller/local
  database report no live all-versioned-fields-absent or v1 service spec
  (physical or logical), historical physical/per-GPU contract, or removed
  qualified class reference.  A retained snapshot/backup containing such state
  is classified as non-directly-recoverable and must be covered by the tested
  hold-and-normalize-before-resume restore runbook below.
- The terminal PostgreSQL run manifest and row inventory cover its exact
  point-in-time row identity set and canonical fleet digest; every manifested
  current spec matches the terminal result-spec digest for its exact owner
  generation.  Every later unmanifested row satisfies the strict
  higher-version and explicit-v2-with-post-completion-timestamp or
  fillable-placeholder-with-NULL-timestamp contract above; it cannot replace
  or mutate a manifested row.
  A later lifecycle epoch on the same service hash is accepted without
  rewriting immutable manifest rows; a recreated hash follows the stricter
  post-completion committed-v2 rule above.
  Every
  terminal retirement includes the locked dependency proof and retired-row
  digests; requested/loaded controller receipts converge; and a post-reload
  dry-run reports zero pending changes, zero blockers, and zero ledger
  mismatches.  Later mutable status/catalog columns do not invalidate an
  immutable spec proof.
- No bridge, retryable version, placement catalog, replica row,
  resource-action root, or shadow root depends on that version.  A retained
  cleanup intent is permitted only when its frozen protocol-2 all-finite proof,
  protocol-3 timestamp-bound proof, or protocol-4 timestamp-plus-placeholder
  proof records its exact committed match, zero
  deletion targets, byte-exact retained YAML, and omission-lossless old-reader
  coverage; every other cleanup dependency blocks.  A NULL version timestamp
  requires protocol 3 or 4.
- All eligible GPU services have committed/applied logical versions, no
  quarantined version, and zero remaining physical replicas from migration.
- Pools and intentionally physical services are either still supported by the
  physical preset or have completed a separately designed migration.
- The release/CI-managed minimum compatible API version and the minimum
  supported server, controller, and rollback image all write placement-contract
  v2 and include the clean-generation guard.  After a protocol-4 manifest or
  request exists, every live-database rollback image also parses protocol 4.
  v1-writer images are explicitly forbidden once supported normalization apply
  commits.
- At least two consecutive production releases and 30 continuous days after
  the final normalization/reload show zero legacy-decoder events and zero
  contract/fence mismatch or mixed-version recovery errors.  Structured
  controller/API logs for
  `event=skyserve_placement_contract_decode` with
  `outcome=legacy_materialized|rejected` are retained for at least 45 days;
  the observation clock does not start until the Platform log sink and exact
  zero-event query are attached to the transition PR.  The Serve
  maintainer and Platform on-call attach the zero-use query, all database and
  artifact inventories, release identities, and rollback drill evidence to
  the cleanup PR.
- Every retained backup/snapshot has expired or is covered by a tested
  transition-reader-first, normalize-before-controller-resume restore runbook.
- The cleanup pre-controller bootstrap guard fails closed without a current
  clean manifest/generation; the transition restore chart's controller-hold
  mechanism is tested, and any restore/import or legacy decode durably resets
  that generation and the zero-event observation clock.
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
- the raw-state classifier distinguishes pickled-`None`, every supported
  fieldless public tuple, explicit v1/v2, the sole historical tuple, partial
  state, unknown class references, undecodable bytes, and non-spec objects
  without invoking the ordinary compatibility materializer;
- an exact source-protocol raw projection reproduces every eligible source
  pickle byte-for-byte before mutation; nested compatibility materialization,
  nondeterministic reduction, or another unrelated round-trip difference
  blocks normalization;
- fieldless normalization changes only explicit policy/pool representation and
  the seven placement fields; v1 normalization changes only the contract
  version and removes the rollback mirror.  All unrelated raw state keys and
  values, YAML columns, catalogs, controller configuration, service
  fences/pointers, and replica rows remain identical;
- real-PostgreSQL apply is all-or-nothing at `SERIALIZABLE` isolation under
  fixed-order table locks and CAS races, enforces its row bound/timeouts, is
  restart-safe, verifies its run/fleet/row/postimage digests, preserves the
  version high-water mark through a newer successor, and reports zero pending
  fieldless/v1 changes on an idempotent rerun;
- the terminal-retirement matrix covers current, active, replica-owning,
  applied non-quarantined, quarantined, superseded, missing-service,
  terminal-service, bridge/catalog/retry/cleanup-dependent, and concurrent
  update/down/recreate cases; only conclusively terminal historical tuples
  with a strictly newer committed successor are retired;
- an unmodified supported copy retains the exact contract as v2, while the
  historical copy retains its exact contract only in memory and cannot
  serialize; an explicit policy or `pool` override resolves and validates a
  new contract;
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
- exact-release transition/normalization client-server behavior preserves
  semantics, with no removed qualified class reference;
- an exact offline v1.1.1135 binary reads every normalized v2 public tuple with
  unchanged replica, catalog, cost, fill, and workload semantics and does not
  elect a retired historical fixture; no post-apply v2-to-v1 write path is
  supported or tested as valid;
- an exact unmodified v1.1.1135 post-retirement cleanup harness reads the
  PostgreSQL fixture through its own accessors: its live-version scan omits the
  candidate's NULL `yaml_content`, its intent scan still enumerates the exact
  now-committed matched intent, and the resulting deletion-target set is
  byte-for-byte/canonically identical to the pre-retirement zero-target set;
  the harness must not substitute the current reader's
  `retired_yaml_content` fallback;
- fresh `pool: {}` plus `workers: N` persists and serializes exactly as
  `{'workers': N}`; the exact preceding release reads the same pool kind, size,
  and policy, its rollback reserialization round-trips through the transition
  reader unchanged, while a real fieldless legacy `_pool={}` remains a service;
- every supported v1 and fieldless tuple normalizes to v2, and every supported
  current copy/reserialization remains v2; a frozen exact-transition
  historical physical/per-GPU v1 artifact survives only an in-memory copy,
  fails serialization, and is retired or rejected, while a class shim fixture
  (when required) never recreates a second engine;
- every fresh serializer and central version-write boundary rejects fieldless,
  v1, mirrored, or historical state, while exact retries preserve existing v2
  bytes and the raw normalizer remains the only in-place rewrite bypass;
- a non-pool replica request persisted before the controller-hold rollout is
  rejected by the fresh execution/provider-boundary authority check after the
  API pod restarts, while an exact current `pool == 1` parent remains
  launchable and a malformed truthy discriminator is rejected;
- every controller boot validates raw persisted v2/mirror-free bytes; pending
  receipts also bind the exact completed-ledger result digest, and later byte
  substitution fails even after a previously completed receipt;
- historical retirement proves a sole fresh Recreate API pod and stable empty
  target-process evidence before, under, and after locks, including the
  single-string `setproctitle` representation and fail-closed malformed/access/
  overflow cases;
- `_metadata` is the only internal Task-YAML metadata spelling; initial and
  later version commits adopt an exact matching cleanup intent, while
  `metadata`, missing/partial scope objects, wrong types, scope-ID mismatch,
  parent-scope mismatch, or a future lifecycle epoch fail closed;
- the production-shaped 49-intent corpus is bounded and maps one-to-one to
  retained version YAMLs; the retirement transaction CASes only each stale
  provisional bit, retains every other intent column and every cleanup YAML
  byte, and rolls all repairs back if retirement or postimage verification
  fails;
- the exact production timestamp shape is covered: six NULL timestamps form a
  strict older-writer prefix anchored by the first finite version, while a
  NULL after the boundary, no finite boundary, or an intent newer than its
  matched finite row or legacy boundary fails closed; exact protocol-v2 ledger
  fixtures remain readable, while protocol-v3 field deletion, valid-looking
  digest/count/mode/boundary substitution, and relabeling as v2 all fail;
- the production-shaped same-service placeholders at versions 3 and 10 are
  accepted in real PostgreSQL only with historical version 2,
  committed/current explicit-v2 version 51, canonical protocol-4 `None` bytes,
  clean NULL side columns, zero replicas, and stable zero image/action evidence;
  protocol-5 or other merely decodable-`None` bytes, a trailing fillable
  placeholder, current/active placeholder, side-state
  field, replica, demand/action root, or placeholder add/fill/side-state/replica
  race between preflight and locked scan fails and rolls back the complete
  transaction;
- independently inject locked and postflight image/action nonzero counts,
  zero-count digest drift, and evidence-target-map drift for candidates and
  stale placeholders; delete, substitute, or swap each protocol-4 count,
  successor, per-row evidence digest, row/column hash, candidate inventory
  digest, nested key, and `stale_placeholder` classification; partially
  relabel v3 as v4 and v4 as v3 and require full-manifest rejection; attempt a
  coherent whole-manifest v4-to-v3 rewrite in real PostgreSQL and require the
  first ledger mutation to fail at the append-only trigger; prove the trigger
  also fires for `TRUNCATE` and with `session_replication_role=replica`; after a
  successful terminal transaction, attempt a protocol-3 run insert and a
  revision-040 downgrade and require both post-terminal database fences to
  reject them; prove a pre-terminal downgrade removes exactly the revision-040
  catalog; race terminal insertion against read-committed, repeatable-read, and
  serializable old-writer transactions whose snapshots precede or overlap the
  terminal commit and require every physically later run either to fail at the
  busy advisory gate, observe the terminal singleton, or abort with a
  serialization failure; commit a terminal-mode run without a retired row and
  require a later row insertion to fail its transaction binding; accept two
  terminal candidates inserted by the same admitted run and reject a competing
  activation; race downgrade and terminal admission in both lock orders and
  require exactly one authority to proceed; tamper with every singleton
  column/default/type/nullability/key/check/FK, singleton row value/count,
  relation owner/ACL/kind/persistence/RLS state, and function/trigger owner,
  ACL, search-path, body, event, enablement, predicate, argument, transition,
  constraint, overload, or duplicate/extra-source-trigger field and require
  fail-closed rejection; attempt every top-level singleton DML operation and a
  depth-two update without the exact current-transaction source tuples and
  require rejection;
- either candidate or intent ownership, duplicate/missing/colliding matches,
  malformed false-ish cleanup fields, noncanonical scope metadata, inventory
  overflow/drift, or ledger-fact tampering blocks retirement;
- protocol-4 verification accepts the exact completed protocol-1,
  protocol-2, and protocol-3 manifests through their frozen validators without weakening
  their predicates, and a clean pending, partial,
  mismatched, or orphan-manifest predecessor receipt blocks retirement; a
  real-PostgreSQL pending acknowledgement emits `FOR UPDATE OF services`, not
  an unqualified outer-join lock, and commits the exact owner-fenced receipt;
  after completion the receipt remains valid across controller PID/IP
  replacement and lifecycle advance, while an uncompleted receipt stays
  exact-owner/epoch fenced;
- bounded typed resource-action and shadow scans block every exact candidate
  source root, including retained terminal roots, malformed possible matches,
  overflow, and pre/locked/post evidence drift;
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
  The normalization release accepts and upgrades the former; it leaves the
  latter unchanged for the separate terminal-retirement protocol.  Rebased
  cleanup accepts neither v1 artifact and rejects both.  The raw SHA-256
  digests are respectively
  `a4c549ae75412dcff8917d29f892284745040b9a503b38f70d7cea1686acd05a`
  and `3e912f262e1498d28891d27565f7c0720a429feb1b8131887adb40d13ce2ed28`.
- An earlier cleanup prototype proved that all five supported v1 tuples retain
  semantics when projected to mirror-free v2.  Its cross-worktree
  v2 -> transition-read/copy -> v1 result also proves why v1.1.1135 cannot be
  deployed after normalization.  The revised normalizer covers v1-to-v2; the
  revised cleanup must reject v1 and rerun its full CI evidence.
- The normalization implementation's exact mirror-free v2 serializers were
  read successfully by an unmodified, clean v1.1.1135 worktree under Python
  3.14.3.  Five fixtures covered service/pool without a placement engine,
  physical dynamic service/pool, and logical per-GPU service; their SHA-256
  digests were respectively
  `8ec4bf9413b83151543a38044538f7b95f619cfe0ffe954b3b88e5b3ffeb7834`,
  `16a6f9c75527e820f4a0fbbecd72907b608345f2305395fd3554ea6df55503d7`,
  `4d09f45fb61066aed7283351a55b30743fdf5e87ab5811ca999473a0ca411da3`,
  `8a12777f3e704bfe032a57c6ba9edc05512bd70b89aea9268c8b34538fed6a39`,
  and
  `5cc7c1dbd5ebe9ce135f472e0a3bdd934c47e2adec614ebf6df3fbe528c54726`.
  Contract fields and rendered YAML were identical and none of the artifacts
  contained the rollback mirror.
- The same v1.1.1135 artifact was bound read-only to a real PostgreSQL
  schema-037 fixture containing a retired v1 row and committed v2 successor.
  Every committed/applicable/recovery/liveness/HA/launch selection chose the
  v2 successor, the retired row materialized as terminal with `spec=None` and
  no YAML, an actual stale `add_or_update_version()` returned
  `STALE_VERSION` without changing any retired bytes or metadata, and the next
  allocation preserved the high-water mark by allocating version 3.
- Adversarial review of the exact schema-037 migration and retirement code
  returned GO after the global controller-cluster inventory was changed to a
  bounded SQL prefix query.  The review covered additive PostgreSQL schema,
  foreign keys and checks, serializable locks and CAS, exact manifests,
  NULL/orphan replica blocking, parent pool/logical/resource-mode fences, and
  tamper-resistant retirement ledgers.
- After the final fence hardening, the placement normalization, schema-037,
  resource-action schema/store, bounded cluster-prefix, and broad Serve state,
  controller, respawn, service, implementation, utility, glob, schema-contract,
  and daemon unit suites all exited zero against local PostgreSQL.  Targeted
  mypy over the new normalization/schema/placement modules, pylint over the
  changed source, Helm lint, the 120-case modified deployment chart suite, and
  `git diff --check` also passed.  Docker-backed PostgreSQL suites could not
  start on this host because it has no Docker socket and remain mandatory in
  CI; no SQLite substitute was used.
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
- Controller recovery tied its new L4 launches to a version-58 logical target,
  explicit per-accelerator cold-launch authority, measured demand, and the
  bounded global paid-capacity admission window.  The resulting public-cloud
  replicas reported Spot resources; no ordinary on-demand candidate was
  observed.
- A bounded sample of the last 5,000 controller-log lines contained 128
  `event=skyserve_placement_contract_decode outcome=legacy_materialized`
  events for the logical service contract and no rejected decode event.  This
  is expected transition-reader behavior for the pre-transition version-58
  artifact, but it directly proves that cleanup cannot remove the legacy
  reader or begin its zero-event observation window.
- The pre-normalization PostgreSQL inventory classified 163 version payloads:
  seven are pickled-`None` placeholders, 156 are valid fieldless contracts,
  and none is partial, malformed, undecodable, or a removed-class reference.
  Of the valid fieldless rows, 155 resolve to supported public tuples.  The
  remaining `boltz-l4-fleet-test` version 2 resolves to the exact
  transition-only physical/per-GPU tuple and was observed as non-current,
  non-active, non-applied, replica-free, catalog-free, and without a matching
  container-image demand or file/storage cleanup payload.  A versioned
  controller configuration snapshot is retained, but the exact recovery
  expression does not select it because version 51 is the strictly newer
  committed successor.  This is baseline evidence only; the apply must
  reprove terminality under its writer lock and frozen cross-database checks.
- At 2026-08-07 18:35:52--18:36:02 UTC, production remained healthy at
  v1.1.1135 with `boltz-l4-fleet` current/elected/committed/applied version 58,
  active versions `[58]`, stable HA LB slot `a`, and the unchanged endpoint.
  All 55 READY replicas belonged to version 58; the preceding 15 minutes had
  zero requests, in-flight work, queued work, or rejections.  This read-only
  snapshot authorizes no capacity launch and is not normalization evidence.
- Normalization PR #1328 passed 32/32 checks and merged as
  `ccdb295a4a6065fc72f67571e87a395d1e6ec2a1`.  The deterministic v1.1.1143
  release image is
  `sha256:cefcfc0f4a620707770f0a69e51317b60aab365d331e9dc77877c577c7f6cbc4`
  and the chart digest is
  `sha256:0cf390a9ce6eb3d36a3cba229ffdcb7c9124ece55c0ae6e5d94de71af6bd7056`.
  Direct Helm revision 353 deployed that exact image with reused values and
  schema 037; revisions 354 and 355 exercised and then cleared the explicit
  controller hold without changing the image or chart.
- The production supported freeze evidence digest was
  `7da998baa11b4e0defab15ae9b72987cc7b1862c07f39b3797ec56e40baadba7`.
  Run `3bacd32f-888e-4a1f-af87-8f17dd82f168` changed 156 rows and committed
  post-inventory digest
  `288cc2d84d8e884806797640d3457436f864a6bc5e6872674e1c225403b37716`.
  A post-apply dry-run reports 156 explicit-v2, one historical, and seven
  placeholder rows, with zero changes, blockers, or prior-ledger mismatches.
  The minimum live-database rollback image is therefore v1.1.1143 or newer;
  v1.1.1135 and all earlier v1 writers are offline fixtures only.
- The held retirement preflight proved consolidation, a sole fresh Recreate
  API pod, zero active Serve mutation requests, zero local target processes,
  zero legacy controller clusters, zero candidate replicas or unknown-version
  replicas, zero image demand, and zero typed resource-action roots.  It did
  not apply retirement because `boltz-l4-fleet-test` has 49 cleanup intents.
  The hold was cleared immediately and all 16 external load-balancer
  deployments returned Available.
- A bounded read-only audit found that each of those 49 intents maps one-to-one
  to a retained live version YAML by exact bytes and by the exact
  `_metadata.sky_serve_ephemeral_storage_scope` scope/generation.  Every scope
  equals the parent hash/resource scope, every scope ID recomputes, and every
  owned-mount list and top-level file/storage/volume/workdir target is empty.
  All 49 intents nevertheless retain integer `provisional=1`.  The root cause
  is explicit: both `_ephemeral_storage_generation_from_yaml()` and the first
  retirement ownership reader looked for `config['metadata']`, while the Task
  serializer writes `config['_metadata']`.  The candidate historical version
  2 maps exactly to the intent created at lifecycle epoch 2.  No production
  row was changed by this audit.
- Eight services requested by supported normalization initially had clean
  pending receipts: requested UUID and manifest were present, loaded UUID and
  all loaded fields were NULL, and there were no partial or mismatched
  receipts.  Bounded
  controller-log inspection found the same exact cause for the active main and
  historical test services:
  `psycopg2.errors.FeatureNotSupported: FOR UPDATE cannot be applied to the
  nullable side of an outer join`.  The query joins the optional run manifest
  and then calls unqualified `with_for_update()`.  The follow-up must use
  `with_for_update(of=services_table)`.  PR #1330 merged that fix as
  `ccae3e8ec2caae74a9baff8f0268078d35e03307` and published v1.1.1146 image
  digest
  `sha256:4b497fc70e5cee9f58772b66149c837c940ef37e6823719ae1473192096fca1c`.
  Direct Helm revision 357 deployed that exact digest with hold false.  The API
  pod and init/migration containers completed with zero restarts; all eight
  receipts converged, all eight controller ports were present, all eight direct
  controller health checks returned 200, and all 16 LB deployments became
  Ready and Available.  PR #1330's compile assertion alone did not execute the
  PostgreSQL path; the protocol-2 follow-up closes that gap with the exact
  pending-receipt lock/CAS test below.
- The protocol-2 follow-up validates the exact secret-free production
  protocol-1 run `3bacd32f-888e-4a1f-af87-8f17dd82f168` and all 164 ledger
  rows from a compressed fixture whose canonical JSON SHA-256 is
  `5067bd30eb5c2b2604ba1302d020e8e609cec81d25afd74702d2917e1af27ef6`.
  The test asserts the exact schema projection, excludes every raw spec, YAML,
  and controller-configuration field, and accepts the snapshot through the
  shared protocol/mode verifier without synthetic identities.  Typed cleanup,
  shared protocol dispatch, receipt-incarnation anchoring, and retirement
  ledger tests pass locally; targeted mypy reports no issues in the four
  changed source modules.  All 11 receipt/retirement cases pass against an
  isolated native PostgreSQL 14 server: the exact pending-receipt
  `FOR UPDATE OF services` and one-time all-NULL CAS; pending, partial,
  mismatched, orphan-manifest, unapproved-commit, and bounded-overflow
  blockers; the production-shaped 49-intent retirement; both
  cleanup-CAS/postimage atomic rollbacks; and the exact production snapshot.
  Final adversarial review returned GO for code review and merge after finding
  no remaining correctness blocker.  Production execution remains gated on
  the documented Helm hold, fresh freeze/preflight, and postflight checks;
  cleanup PR #1319 remains blocked on its separate removal gates.
- The protocol-3 timestamp hotfix reproduces the exact retained production
  version ordering `1,2,4,5,6,7,8,9,11..51`: the first six rows have NULL
  timestamps, historical candidate version 2 binds to the first finite
  boundary at version 8, and all 49 intent matches are included in an
  independently recomputed canonical inventory and candidate proof digest.
  The complete placement-contract and identity unit modules pass.  All 14
  retirement/receipt cases pass against shared native PostgreSQL 14, including
  the exact positive topology, NULL-after-boundary, all-NULL, intent-after-
  boundary/row, CAS rollback, and postimage rollback cases.  Targeted mypy
  reports no issues and targeted pylint rates both changed source modules
  10.00/10.  Protocol-1 and protocol-2 readers remain covered through their
  frozen validators; protocol relabeling, v3-field deletion, source-timestamp
  mismatch, and valid-looking proof-fact substitution fail closed.  The exact
  updated design passed adversarial review before implementation.  Final code
  review, merge CI, immutable artifact publication, and production execution
  completed as PR #1341 and v1.1.1155.  Direct Helm revision 364 completed the
  reader-first rollout and revision 365 enabled the hold on the same immutable
  chart and image.  The exact held preimage remained 164 rows at
  `9867b1f55c74b49dd0b1f52e0eb8384a7dda8336bbe0d94865283ccce19b7dee`.
- The first protocol-3 operator invocation exited with
  `Historical service still has a non-retired version placeholder or
  reservation` before its first DML.  A post-failure dry-run reproduced the
  same pre/post digest, 156 explicit-v2 rows, one historical row, seven
  placeholders, zero changes, and zero ledger mismatches.  The target service
  has exact clean placeholders at versions 3 and 10 below committed/current
  version 51; neither is current, active, applied, quarantined, configured,
  cataloged, replica-owning, demanded, or action-rooted.  Revision 366 disabled
  the hold without changing the v1.1.1155 pins; the replacement pod and both
  init containers have zero restarts, all eight controller health probes
  return 200, and all 16 load-balancer deployments are Ready and Available.
  Protocol 4 implementation, review, immutable release, reader-first rollout,
  and a fresh held apply remain open.

Credentialed provider catalog coverage and zero-cost production smoke evidence
remain open gates.  This is only the first transition production release, and
the 30-day observation clock has not started because legacy materialization is
nonzero and the retained log sink and exact zero-event query required by the
removal gate are not yet attached.

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
7. With Serve mutations frozen, run the normalization dry-run against a copied
   production PostgreSQL database.  Require exact classifications and no
   blockers, then inject malformed, CAS-racing, active, applied, replica-owning,
   and recovery-dependent historical rows and prove the complete apply aborts.
8. Prove normalized v2 fixtures with the exact v1.1.1135 artifact in an offline
   read-only harness, and prove the normalization release is v2-only across
   fresh writes, copies, retries, controller boots, and central write paths.
   Deploy the normalization release through Helm, repeat the dry-run, apply the
   bounded transactions, restart affected controllers, and require a
   zero-change ledger-verifying dry-run plus unchanged versions, endpoint/LB,
   replica inventory, and paid-authority state.  Verify Helm rollback remains
   available before apply and that post-apply runbooks forbid a v1-writer image.

## Open gates

- Draft cleanup PR #1319 is authored in stack #1320.  Its first production
  rollout check passed, but the remaining removal gates are unmet, so it is not
  approved to merge or deploy.  After protocol 4 merges, rebase and refresh it
  to retain or extract the shared protocol-1/2/3/4 identity and receipt reader,
  a read-only full-ledger/bootstrap guard, the production-shaped protocol-4
  fixture, the stale-placeholder zero-materialization gate, and the new rollback
  floor.  The transition decoder/writer may be deleted only after those final
  readers and the documented 30-day gate pass.
- Transition and cleanup GitHub CI, including the mandatory real-PostgreSQL
  lane, completed successfully.  Credentialed AWS catalog coverage remains
  open because it has not been rerun after the operator refreshed SSO for this
  deployment.
- Production control-plane v1.1.1155 is deployed directly through Helm at
  revision 366 with the controller hold disabled; closed Platform PR #8090 is
  not part of the release path.  The sole Recreate API pod is healthy on the
  exact PR #1341 image with both init containers and both app containers Ready
  and zero restarts.  Protocol-2 and protocol-3 retirement attempts made no
  mutation.  Protocol 4 must pass design/code review, merge, release, and
  complete a reader-first Helm rollout before a separate hold revision and
  retirement attempt.  Normalization apply has committed, so the current
  rollback floor is v1.1.1143; after any protocol-4 manifest or request exists,
  the protocol-4-capable release becomes the floor and older rollback requires
  a pre-protocol-4 database restore.  That restore path still needs a drill.
- A second consecutive production release, the retained 45-day log sink, exact
  zero-event query, and 30 continuous qualifying days remain required.
- The Boltz fleet's canonical service YAML currently keeps warm capacity; its
  separate scale-to-zero policy must be reviewed, deployed/applied, converged,
  and drained before any service update can claim scale-to-zero compliance.
- Service version 58 is authoritative mirror-free placement-contract v2.  Its
  requested load receipt and the seven other requested receipts converged
  under v1.1.1146 and remain healthy through v1.1.1155.  The one historical
  test-service tuple requires the protocol-4 stale-placeholder proof to pass
  review, merge, deploy reader-first, and run its locked terminal-retirement
  proof before the zero-event window can start.
