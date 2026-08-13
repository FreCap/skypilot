# Multi-pool SkyServe reserved-capacity fill

Status: implementation and the exact automated freeze are complete after the
identity-free worker-partition correction; three consecutive adversarial
reviews are pending; not merged, deployed, or activated

Last updated: 2026-08-13

Canonical owner: this file

Rollout policy: generation-fenced fix forward, no capacity-consuming canary,
and no supported demotion after fleet activation

## Decision summary

The steady-state reserved-fill path is:

```text
concurrent physical-pool observation
  -> immutable PostgreSQL observation generations
  -> immutable per-version worker/Kueue projection and canonical digest
  -> deployment-policy authorization of the exact projected claim set
  -> broker round with exact observation provenance
  -> one authenticated service-wide allocation map
  -> the ordinary autoscaler reconciliation coordinator
  -> pure typed fill plan
  -> locked durable capacity admission and replica-row acceptance
  -> exact accepted/deferred commit receipt
  -> exact projection revalidation and deployment-policy authorization at the
     terminal provider boundary
  -> one-way Pod materialization boundary and fresh authority around every
     bounded post-Pod runtime or workload effect
  -> existing asynchronous launch path
```

The target correction removes the two root causes of the 2026-08-11 underfill:

1. Physical-capacity reads no longer wait behind slow replica actuation while
   their conservative freshness timestamp expires.
2. Planned fill capacity is no longer counted as spent unless the replica
   manager returns a receipt for the exact rows that became durable.

The terminal PostgreSQL insert is also an admission ledger, not a passive
recording step. Under the service-owner and replica-row locks it re-parses the
authenticated map, rejects an intent replay, recomputes the durable service
ceiling, and spends one physical pool/card slot. A delayed or concurrent
caller therefore cannot oversubscribe capacity that an in-memory planner once
observed.

One PostgreSQL selector changes the whole writer fleet from the compatibility
path to the sequenced path. The transition is one way. After activation, an
unavailable or invalid allocation map withholds new fill; it never falls back
to the old speculative launch path. Ordinary demand reconciliation continues.

This is a fix-forward deployment. We do not run a GPU, service, or BCL canary
and do not retain a second operator-selectable happy path. A problem after
activation is repaired by deploying a successor image and authorizing one new
gate generation through the same command and transaction used for first
activation. The prior generation then fails closed.

Code rollout and gate activation are separate operations. The image may be
rolled out while the gate remains `LEGACY_ACTIVE`. Activation is withheld if
the deployment cannot prove that inference and BCL/research workloads share a
Kueue preemption domain; Kubernetes Pod priority by itself is not that proof.

A final integration audit found that the first implementation bound the
server-owned Pod PriorityClass but still resolved the Kueue LocalQueue from
mutable launch-time configuration and copied the task-owned
`resources.priority_class` into Kueue's WorkloadPriorityClass label. Claim and
launch scopes also omitted the committed service version and worker-projection
digest. That split ownership could attest Pod priority `-1000/Never` while
submitting a differently prioritized Kueue Workload, and a service update could
reuse the old claim generation. The steady-state correction is worker
placement projection protocol v2: one immutable version record owns namespace,
service account, explicit Pod Identity role or explicit identity-free state,
Pod priority, accelerator scheduling, LocalQueue, and WorkloadPriorityClass.
Its canonical digest and service version are fenced through every durable
stage. No reserved-fill-specific parallel projection is introduced.

The production inference partition intentionally has no AWS Pod Identity
association. Protocol v2 therefore treats `pod_identity_role_arn: null` as a
closed, hash-bound negative identity contract, not as a missing projection.
Protocol v1 retains its historical non-null role requirement. The deployment
policy receives the nullable value in every typed projected admission and must
attest either the exact role association or its absence for the projected
namespace/service-account pair. This keeps identity-bearing and identity-free
partitions on the same canonical projection path without inventing a sentinel
role or a deployment-specific compatibility branch.

## Historical context and incident evidence

Multi-pool protocol v2 and its physical-cluster identity fences predate this
correction. Historical protocol work includes PR
[#1261](https://github.com/boltz-bio/skypilot/pull/1261), its draft protocol-v1
cleanup PR [#1263](https://github.com/boltz-bio/skypilot/pull/1263), and the
later detached-authority correction in PR #1440 (`c964b5480`). Those changes
made multiple physical Kubernetes pools representable and fail closed, but did
not solve the lock convoy or receipt-less partial admission described here.
The ordinary-launch prerequisites for this correction merged as
[#1434](https://github.com/boltz-bio/skypilot/pull/1434) and
[#1435](https://github.com/boltz-bio/skypilot/pull/1435).

Production inspection on 2026-08-11 found server `1.1.1243` at `2d2c67efb`.
The A100 pool remained underfilled while the protocol-v2 broker repeatedly
published 34 free slots. Those snapshots reached the autoscaler 181--250
seconds after their conservative source timestamps, beyond the existing
180-second authority horizon. Physical polling and slow manager/provider work
were serialized by `_actuation_epoch_lock`.

The same investigation found that
`Autoscaler._apply_reserved_capacity_fill_v2()` reduced its in-memory feed for
every decision it emitted, while `ReplicaManager.scale_up_batch()` could accept
only a prefix and returned no accepted-prefix receipt. A deferred tail was
therefore neither durable nor eligible for immediate replanning.

The broker's successful, UID-fenced publication of free A100 capacity is
positive evidence that the live identity-read permission and reserved-cluster
module deployment were not the underfill cause. RBAC drift should still be
reconciled declaratively, but neither a permission change nor a larger timeout
fixes these two defects.

## Prior-plan reconciliation and intentional departures

This file remains the one canonical design. Its detailed pre-implementation
state is preserved immutably at commit
`dbaad6213b582ae1b2e3bb364d6cc5e55bd7d311` and can be inspected with:

```bash
git show dbaad6213b582ae1b2e3bb364d6cc5e55bd7d311:\
docs/designs/serve-multi-pool-reserved-capacity-fill.md
```

That revision is historical evidence, not an alternate contract or operator
runbook. The implementation audit deliberately replaced these planned
mechanisms before activation:

| Superseded plan | Canonical implemented contract | Reason |
|---|---|---|
| One composite `(physical UID, accelerator set)` pool per context | One atomic `(physical UID, exact accelerator card)` edge; authenticated aliases are bounded query routes | Heterogeneous A100/H200 supply, width, and failure must remain independent without multiplying physical capacity. |
| Slot-valued provider observations | Raw exact-card GPU counts plus an exact presence set; the broker converts once using `broker_slot_width` | Claimant width is service policy, not physical evidence; converting at observation time caused ambiguous or double conversion. |
| Advance a capacity counter when observation begins | Snapshot three non-advancing commit counters: total admission, ordinary admission, and first-success materialization | Observation start is not a capacity-consuming event. Commit order, rather than wall time, closes admission and provider-visibility races. |
| Direct single-context broker polling under the actuation path | Bounded concurrent observation cohorts, per-edge alias failover, immutable PostgreSQL results, and post-commit notification | Provider latency must not consume observation freshness or serialize unrelated physical pools. |
| Ordered-prefix receipt | A bijective sparse receipt: pool-local failures skip only their intents; global authority loss defers the remaining ordered tail | One unavailable cluster must not starve an independent cluster, while service-wide fences remain atomic. |
| A new durable intent state machine, provider scheduler, mutation arbiter, and debt path | Durable replica-row acceptance is the receipt boundary; accepted rows use the existing asynchronous launch/request path | A second scheduler and actuator would create another happy path and duplicate lifecycle ownership. |
| Pod `PriorityClass`, task-owned Kueue priority, mutable launch-time queue resolution, or activation-time attestation as sufficient reclaim proof | The immutable worker projection is the sole admission owner; one entry-point-loaded deployment policy must prove and durably identify the shared Kueue domain, then authorize every sequenced claim set and terminal provider launch against the exact service version and projection digest | Kueue can withhold higher-priority BCL Pods before kube-scheduler priority can act, and a one-time census or Pod-only identity cannot govern later claims, service updates, or restarted executors. |
| External release supervisor, phase-0 authority reset, bootstrap/maintenance modes, capacity canary, rollback, and fixed 24-hour soak | Full immutable split-role rollout at `LEGACY_ACTIVE`, exact activation prerequisites, then generation-fenced fix forward; no capacity canary or supported demotion | The Serve045 gate/reclaim receipt plus Serve046 version/projection binding form one smaller fail-closed transition and match the current lightly used service. |
| Provider-progress status as launch authority | Provider-free `reserved_fill_reconciliation` diagnostics derived from authenticated allocation and durable rows | Observability must not perform provider I/O or become a second authorization source. |

Rejected alternatives remain rejected: increasing the 180-second TTL hides the
lock convoy; a finite `provision_timeout` cannot distinguish initialization
from capacity exhaustion; a fallback planner or actuator duplicates authority;
an external scheduler/supervisor duplicates lifecycle ownership; Pod priority
alone does not prove Kueue reclaim; and a canary or rollback protocol is not
required for this fix-forward rollout.

## Goals

- Fill idle, zero-cost Kubernetes GPU capacity from every eligible physical
  pool of one service without multiplying its service-wide policy.
- Preserve the conservative 180-second capacity-authority horizon.
- Query independent Kubernetes contexts concurrently and outside slow
  actuation locks.
- Publish only a complete service-wide planner input authenticated against the
  current service owner, claim generation, pool rounds, physical identities,
  and exact observations.
- Use the same autoscaler decision tick for demand, scale-down shelter, and
  reserved fill, with one reconciliation coordinator and no lost wakeup.
- Debit ordinary demand and already accepted fill before producing new fill
  intents.
- Count capacity as spent and advance pool rotation only from a validated
  durable commit receipt.
- Revalidate the exact intent, current service ceiling, and remaining
  aggregate and accelerator-card feed in the same transaction that inserts
  each sequenced fill row.
- Keep every fill launch pinned to a zero-cost location in the exact physical
  pool and accelerator class that authorized it.
- Preserve and mechanically respect the deployment-owned Kueue admission and
  preemption contract under which BCL work may reclaim preemptible inference
  slots.
- Make worker placement projection v2 the only owner of projected Kubernetes
  admission: task inputs cannot select Pod priority, LocalQueue, or
  WorkloadPriorityClass, and launch rendering cannot reread those values from
  mutable configuration.
- Bind the exact deployment reclaim-policy identity into PostgreSQL, allocation
  authentication, replica provenance, and the terminal provider launch fence,
  together with the committed service version and complete worker-projection
  digest; a missing, legacy, ambiguous, stale, or differently identified
  policy or projection fails closed.
- End with the sequenced path as the only launch path and a concrete stacked
  removal change for the compatibility code.

## Non-goals

- No new YAML, SDK, or CLI service policy field.
- No increase to the observation freshness horizon and no postdating of a slow
  provider read.
- No inference from `provision_timeout` that a Kubernetes request is either
  initializing or out of capacity.
- No paid fallback. A reserved-fill intent is zero-cost-only and is skipped if
  its exact zero-cost location cannot be proved.
- No fixed readiness SLA for a 200-replica wave. Durable admission is bounded;
  provider scheduling, image pull, setup, and readiness retain their real
  latencies.
- No capacity-consuming canary, shadow planner, dual actuator, per-service
  rollout flag, or supported post-activation demotion.
- No caller-selectable Kubernetes namespace, service account, PriorityClass,
  LocalQueue, WorkloadPriorityClass, toleration, or BCL priority policy. The
  existing deployment-owned values move into one immutable projection and are
  reasserted; this feature does not choose new priority semantics.
- No status-side provider calls, mutation authority, or new provider-progress
  scheduler. Diagnostics are a read-only projection of already authenticated
  PostgreSQL authority and exact durable replica attribution.

## Public contract

The existing policy forms remain unchanged:

```yaml
service:
  replica_policy:
    reserved_capacity_fill: true
```

and:

```yaml
service:
  replica_policy:
    reserved_capacity_fill:
      floor_replicas: 10
      weight: 100
      utilization_gate: true
```

Zero-cost Kubernetes candidates are grouped by physical pool. The steady-state
atomic identity is one physical Kubernetes cluster UID plus one exact
accelerator card; the access context and replica width are carried separately.
A single context offering `A100: 1` and `H200: N` therefore produces two
independent, deterministically ordered edges. The repeated context is valid:
only an overlap on the same physical UID/card is ambiguous. Context aliases
that prove the same UID/card cannot multiply one physical pool, and aliases
that disagree on width for that exact card fail closed.

The existing service-wide meanings remain authoritative:

- `floor_replicas` is one total fill floor, not one floor per context.
- `weight` is relative between services sharing a pool. If a second service
  with the same weight joins, the broker shares eligible capacity according to
  both services' floors, weights, holdings, and caps; setting both weights to
  `1000` is equivalent to setting both to `1`.
- `utilization_gate` measures service demand once and bounds the same global
  fill budget.
- `max_replicas` is a hard service-wide ceiling across versions and pools.
- Ordinary demand has priority in the service headroom calculation and in the
  allocation-local pool/card debits.

`kubernetes.provision_timeout` is not changed by this feature. In particular,
`-1` may remain the correct choice for preemptible reserved capacity that
should wait rather than fail the service. A finite timeout such as 30 seconds
is not used as a capacity classifier. The observer records explicit success or
typed blackout evidence, while ordinary replica/provider states continue to
describe initialization and launch progress. The indefinite-wait liveness
guarantee applies to the instrumented built-in Kubernetes reserved-fill path,
whose passive scheduling/readiness waits hold no authority guard. Opaque
provisioners are not eligible for protocol-v2 reserved fill: v2 requires the
in-tree Kubernetes create/adopt/attest boundary so success has an unambiguous
one-way Pod-materialization transition.

## Architecture and invariants

### 1. Provider-free observation ledger

`sky/serve/pool_capacity_observer.py` owns physical-pool reads. For one
observation tick it:

- builds an immutable target from pool key, physical UID, every authenticated
  access-context route, and the exact accelerator card;
- rejects service configurations with more than eight resolved exact-card
  `(physical UID, card)` edges, then starts the bounded independent pool
  queries concurrently; each edge independently accepts at most eight
  authenticated access-context routes;
- passes each query an absolute deadline (45 seconds by default);
- rotates the first alias attempted, gives each remaining alias a fair share of
  the remaining root deadline, and emits a typed pool-local blackout only after
  every authenticated route fails; and
- commits either raw exact-card GPU success with the winning route or a typed
  pool-local blackout; and
- notifies reconciliation only after the completed row is durable.

The observer does not allocate, plan, mutate replicas, or hold the controller's
actuation lock during provider I/O. A slow or failed pool cannot serialize a
healthy sibling. The provider adapter must consume the same absolute batch
deadline, bound its blocking Kubernetes calls to the remaining time, and free
executor capacity after timeout; a wrapper-only timeout is insufficient.
Exact-card edges in the same context join one in-progress physical-UID capture
under that deadline rather than interpreting their shared initializer as
capacity failure. Accelerator names returned by the provider are case-folded
only after rejecting collisions, and the catalog's negative forbidden-read
sentinel becomes a typed permission blackout.

`PoolCapacityObservationRepository` is PostgreSQL-only. The canonical
`begin_observations()` boundary locks the event sequencer once, locks the
requested pool rows in sorted order, and atomically acquires every independently
due, unleased pool in a cohort. Busy or not-due members are skipped without
starving healthy siblings; their prior completed evidence remains independently
bounded by its own freshness deadline. Every returned lease shares one capture
of:

- a per-pool observation generation and lease token;
- the global all-zero-cost admission high-water;
- the global ordinary-zero-cost admission high-water;
- the global first-successful-launch materialization high-water;
- a conservative `observed_at` at query start; and
- `valid_until = observed_at + 180 seconds` under current defaults.

Starting an observation advances none of the event counters. Observation
generation identifies a provider read; admission and materialization counters
identify replica-row commits that can invalidate or contextualize that read.
Only the ordinary-admission counter must match across the old and new pool
evidence combined into a service-wide allocation. The total admission and
materialization counters remain per-pool debit boundaries, so skipped older
evidence is safe when its exact provenance is retained. Repository and target
validation both enforce the eight-edge bound; route validation separately
enforces the eight-alias-per-edge bound. There is no silent chunking that could
reintroduce staggered ordinary-admission prefixes.

Completion verifies the latest lease and identity and writes a SHA-256 over
the complete identity, sequence, payload, legacy projection, and timestamps.
A success stores physical GPU counts, never service-specific replica slots.
The exact-card count remains present even at zero; the separate canonical
presence set distinguishes a present-but-full card from a card that is absent
from the physical cluster. The winning access context must belong to the
immutable route set acquired in the lease and is persisted as observation
provenance.

A transient physical-UID discovery failure retains the last proven
context-to-UID edge instead of deleting fleet-wide capacity topology. This is
not stale launch authority: every provider observation and every admitted
launch still re-proves that UID through the captured Kubernetes client. A
retargeted context therefore fails its identity fence, while another alias for
the same physical pool may continue the observation.

A timed-out, superseded, malformed, permission-denied, identity-mismatched, or
otherwise failed query grants no capacity. A newer completed blackout prevents
fallback to an older success. A newer in-progress generation does not erase
the last completed result until it completes.

The fixed-rate poll remains 60 seconds. The correction is event-driven after a
publication: it does not wait for the former autoscaler polling interval or
for unrelated actuation, but it cannot discover capacity before the next
physical observation tick.

### 2. Commit-order sequencing for zero-cost admission and materialization

The protocol singleton has three deliberately separate counters:

- `zero_cost_admission_sequence` is the total commit order for every accepted
  zero-cost replica row. It provides row attribution and replay diagnostics.
- `ordinary_zero_cost_admission_sequence` is the cross-service invalidation
  generation for ordinary demand. Ordinary capacity is not broker-partitioned,
  so a commit by any service invalidates every allocation map based on the old
  generation. Reserved-fill commits do not advance this counter because their
  capacity is already partitioned by authenticated broker grants.
- `zero_cost_materialization_sequence` is the total commit order for the first
  persisted successful `sky.launch` of every zero-cost row. It closes the
  interval between row admission and provider-visible occupancy without using
  `created_at`, readiness, or an inferred provisioning timeout.

Each observation snapshots all three counters without advancing any of them.
An allocation publication is valid only when every included observation
captured the same ordinary high-water and that value still equals the locked
protocol singleton. A reader repeats the exact-equality check; a newer ordinary
commit makes the map stale instead of trying to repair it with application
clocks.

In `SEQUENCED_ACTIVE`, an ordinary zero-cost insert atomically advances both
counters and stores its database-assigned total sequence on `ReplicaInfo`. A
typed fill insert carries the allocation's ordinary high-water, requires it to
still equal the locked singleton, and only then advances the total counter.
The manager performs that final revalidation and row insert while participating
in the same global demand-admission lock as ordinary placement. Provider
preflight stays outside that lock. This closes both directions of the race:
ordinary demand cannot commit between fill revalidation and persistence, and a
fill cannot consume stale evidence while an ordinary placement transaction is
in flight.

The pure planner still debits ordinary decisions from the same reconciliation
tick before they have committed. A target-less or otherwise ambiguous decision
is conservatively debited against every compatible map-local pool, with each
debit capped by that pool's authenticated feed. The broker also performs one
complete row snapshot across claimant and nonclaimant services. For a sequenced
observation, a compatible nonterminal zero-cost row debits observed free when
its admission is newer than the observation admission high-water, its first
successful launch is newer than the observation materialization high-water, or
either marker is missing or malformed. A row whose valid admission and
materialization markers are both no newer than the observation is left to the
provider measurement and is not double-debited. This prevents two services
from spending one observed slot without making broker-disjoint fill commits
invalidate each other.

Physical placement, not economic classification, determines whether a replica
can race a pool observation. Sequenced scans therefore include every
Kubernetes row whose current access context and accelerator match the pool,
plus rows with complete immutable pool provenance and conservative same-card
fallbacks for unattributed zero-cost/fill rows on retired aliases. This rule is
durable across replica rewrites: serialization upgrades a pre-v11 row to the
latest record version but cannot reconstruct its historical `is_zero_cost`
truth. A false cost flag can therefore never make a physically matching row
disappear from the debit. Until every Kubernetes launch row carries immutable
physical-UID attribution, an unattributed same-card row on another context may
be using a retired alias and is conservatively debited from every compatible
v2 pool. A row on a non-Kubernetes cloud is excluded unless its persisted pool
provenance still makes this pool plausible. This conservative occupancy
accounting is not a second launch path and is intentionally retained by cleanup
PR #1452. A separate future stack may persist the physical UID on ordinary
placement, migrate live ordinary rows as they are authoritatively refreshed,
and remove same-card duplication only after no nonterminal zero-cost row lacks
that identity for one complete observation horizon. Neither this feature nor
#1452 claims that removal.

The complete replica snapshot is part of spendable sequenced authority. A
grouped enumeration, query, or decode failure rejects the new observation and
does not publish a successor round; the previous round remains bounded by its
original freshness deadline. The legacy callback path retains its historical
per-service fallback only while the durable selector remains in
`LEGACY_ACTIVE`.

The materialization marker is assigned only after the provider operation has
reported success, in the same PostgreSQL transaction that projects the locked
terminal request evidence and ordinarily first persists
`sky_launch_status == SUCCEEDED`. If teardown already made `INTERRUPTED`
absorbing, the reducer passes the exact provider-success bit separately so it
can stamp materialization without reviving the replica. A pre-effect
cancellation passes false and cannot stamp it. The marker is never written
before provider visibility. A provider query that overlaps the bind-to-marker interval may
already exclude the pod while the sequence rule also debits it. That is a
deliberate conservative underfill for at most the current observation round:
the next observation snapshots the committed marker and leaves the row to the
provider measurement. The opposite, oversubscribing because a launch became
visible after the query started, is not allowed.

Occupancy is reconciled in the same slot and accelerator units carried by the
provider observation. Complete pool-key/physical-UID provenance dominates a
retired access alias or a later claim-width change: a queued old row can still
bind to that physical pool, so its historical GPU count is converted to the
current width's slot equivalent. Partial, legacy, or shapeless zero-cost rows
debit every plausible same-card pool/card until their physical identity is
known. Exact-card debits are subtracted from that exact card before feed
partitioning; an A100 row can never make the broker withhold H200 while leaving
A100 launch authority. Rows proven physically off-pool are excluded; a paid
classification alone is not physical absence and cannot override an exact
placement match.

A `SHUTTING_DOWN` or `FAILED_CLEANUP` zero-cost row remains cleanup-unproven
occupancy. A successful launch status or durable materialization marker is
enough to conserve fill entitlement. Independently, every such row whose event
markers do not prove it preceded the observation debits the provider snapshot,
including an interrupted launch with a missing or malformed materialization
marker: the pod may have bound immediately before cancellation while its
success reducer lost the race. Fill rows also participate in the same-map
planner replay debit and service `max_replicas` headroom until physical cleanup
deletes the row or transitions it to a cleanup-proven terminal state. This
prevents one allocation map from re-spending a slot during graceful drain or an
ambiguous cancellation.

All PostgreSQL replica writers that may insert a zero-cost row or persist its
first successful launch use one SQL lock order:

```text
zero-cost event sequencer
  -> protocol/lifecycle/service authority
  -> sorted pool/claim authority where applicable
  -> replica row
```

The bound-request reducer takes the sequencer mutex at transaction entry,
before it can inspect the service and replica to learn whether the launch is
zero-cost. Stale whole-row updates merge the immutable database-assigned event
markers from the locked row, so a retry cannot erase or replace either one.

Replica state version 17 persists the complete typed fill attribution:

- allocation generation, input hash, and claim generation;
- observation generation and sequence;
- intent idempotency key; and
- reconciliation-gate generation and the exact three-part reclaim-policy
  identity for that generation;
- the SHA-256 digest of the exact protocol-v2 worker placement projection that
  owns Kubernetes and Kueue admission for this replica;
- database-assigned zero-cost admission sequence; and
- database-assigned first-success materialization sequence, once launched.

The six historical allocation/observation/intent fields are all present or all
absent. The five gate/policy/projection fields are likewise all present or all
absent and require the historical tuple. A v15 row with only the historical
tuple remains readable, but it cannot authorize a sequenced launch; that
terminal fence requires the complete successor tuple to match current durable
authority. Worktree-only v16 was never merged or deployed and is not a durable
compatibility format. The
materialization marker is null before launch success and a positive integer
afterward. Missing event attribution is a conservative debit in sequenced
rounds, and only fully attributed current rows count as same-allocation replay
debits.

### 2a. Durable reclaim authorization receipt

Serve045 is a forward-only successor to Serve044. It adds a nullable reclaim
receipt to the PostgreSQL protocol-authority singleton. The receipt contains:

- `reclaim_fleet_bundle_sha256`, `reclaim_policy_revision`, and
  `reclaim_provider_inventory_sha256`, which form the typed
  `ReclaimPolicyIdentity`;
- `reclaim_claim_scope_count` and `reclaim_claim_scope_sha256`;
- `reclaim_evidence_sha256` and `reclaim_authorized_at`; and
- the existing protocol-v2 writer proof: image digest, Deployment generation
  and UID, and Pod inventory count and digest.

The reconciliation-gate constraint is closed: reclaim fields are null in
`LEGACY_ACTIVE`; a `SEQUENCED_ACTIVE` row has one complete well-formed receipt
and protocol version 2. First activation is `LEGACY_ACTIVE ->
SEQUENCED_ACTIVE` at exactly `generation + 1`. Fix-forward reauthorization is
`SEQUENCED_ACTIVE -> SEQUENCED_ACTIVE`, also at exactly `generation + 1`, with
a different complete evidence digest. Exact receipt replay is an
application-level no-op: it changes neither generation nor timestamp.
Demotion, generation jumps, partial edits, and same-generation authority edits
are rejected by an `ENABLE ALWAYS` trigger. Serve044 remains historical
migration authority and is not rewritten.

Policy or writer rotation therefore uses no alternate protocol, feature flag,
or in-place identity edit. The operator converges the full fleet, reruns the
same `activate` authorization command, and atomically replaces the receipt in
a successor generation. The transaction clears every authenticated allocation
map. Already durable rows remain conservative occupancy, while queued requests
carrying the old generation fail the terminal provider fence. This is the one
canonical fix-forward path.

`reclaim_provider_inventory_sha256` fingerprints the immutable allowed fleet
and enforcement inventory owned by the bundle. It must not hash live Pods, the
current claim census, or another naturally changing observation; those values
are activation/authorization evidence and would make an immutable identity
drift without an explicit protocol transition.

Serve046 is the forward-only admission-binding successor to Serve045. It adds
the committed `service_version` to each authoritative claim set and the exact
closed `worker_projection_sha256_by_accelerator` mapping to every normalized
claim edge. The application
requires both for a sequenced set, locks the immutable version row, selects the
exact `(context, accelerator, count)` worker projection for every card,
validates protocol v2 with non-null Kueue admission, and recomputes every digest
before claim persistence. The mapping has exactly one case-folded accelerator
key for every edge accelerator and no extras. This remains correct for the
canonical one-card edge while safely authenticating an older composite edge;
one scalar edge digest cannot identify multiple candidates. Legacy-active
compatibility rows may keep the version and mapping null; they cannot publish a
sequenced allocation or authorize a sequenced launch.

Allocation-map schema 5 hash-binds the gate generation, all three policy
identity fields, the committed service version, and each edge's exact
accelerator-to-worker-projection-digest mapping. A `FillIntent` narrows that map
to the one selected accelerator digest, and a sequenced replica row persists
that scalar as part of its immutable fill attribution.
The protocol-v2 API launch fence carries it into the durable request row. The
fill-persistence transaction revalidates the allocation, current gate
generation, exact identity, idempotency key, service ceiling, and remaining
aggregate and per-card feed before accepting the row. This makes the
activation proof, broker claim, allocation, row, and provider effect one
traceable authority chain rather than independent checks.

### 3. Broker provenance and authenticated allocation map

In compatibility mode, the protocol-v2 broker may still query a provider
inside its old round path. In `SEQUENCED_ACTIVE`, it consumes only a fresh
completed observation and calls
`run_round_from_committed_observation()`. The committed round stores the exact
observation generation, admission sequence, materialization sequence, and
payload digest as one nullable-all-or-present tuple. Publication revalidates
that tuple against the digest-valid observation while holding the event
sequencer, and allocation reads repeat the exact match; an in-memory
provenance value that was not durably persisted cannot authorize fill.

The broker is the only raw-GPU-to-replica-slot conversion boundary. It chooses
one deterministic authenticated claim width for a physical UID/card, divides
the committed exact-card GPU count once, and persists that `broker_slot_width`
beside both the converted observation and per-service feed. Claims with a
different width remain in the authoritative claim set but receive explicit
zero launch and shelter authority for that round; they cannot reinterpret or
double-convert another claimant's slot count. Allocation publication
recomputes the conversion from the exact committed raw observation and rejects
any mismatch. Old observation payload schemas are not silently inferred as raw
GPU authority and fail closed.

During the pre-activation image rollout, legacy broker rounds retain their
existing exact-card envelope bytes and do not emit the new slot-width metadata.
The width key first appears in committed-observation rounds after gate
activation has proved exact writer convergence. This avoids mixed-binary epoch churn
without adding a second sequenced representation.

`ReservedFillAllocationRepository` is the sole durable adapter from broker
rounds to the pure planner. It publishes a map only if every claimed pool has a
complete `PoolFillSnapshot` and all of these still agree in one PostgreSQL
transaction:

- protocol v2 and the current reconciliation-gate generation;
- service hash, resource scope, and controller owner;
- service claim-set generation and ordered edge topology;
- sorted pool round identities, epochs, grants, feeds, and exact-card feeds;
- physical cluster UIDs and access contexts; and
- latest fresh completed observation provenance.

The canonical lock order is protocol, service, sorted pool rounds, claim set
and edges, then exact observations. The map hash covers the complete ordered
map. A no-op republication returns the existing generation. A semantic claim
replacement clears the old map before a successor can be published. Readers
revalidate the current rounds, claim set, owner, gate, and freshness rather
than trusting the stored JSON alone.

The first shipped allocation-map wire schema is explicitly versioned as 5;
the version is both persisted and covered by its authentication hash. Earlier
worktree-only schemas 3 and 4 and earlier shapes were never merged, deployed,
or activation-capable,
so they are rejected as unknown durable state instead of creating a permanent
compatibility decoder. This preserves one canonical map path from the first
production release.

The closed top-level schema contains exactly `schema_version = 5`,
`allocation_generation`, `allocation_input_sha256`,
`allocation_claim_generation`,
`service_version`,
`ordinary_zero_cost_admission_sequence_high_water`,
`reconciliation_gate_generation`, `reclaim_fleet_bundle_sha256`,
`reclaim_policy_revision`, `reclaim_provider_inventory_sha256`, and
`pool_snapshots`.
Each ordered snapshot contains exactly `protocol_version`, `pool_key`,
`physical_cluster_uid`, `service_generation`,
`worker_projection_sha256_by_accelerator`, `edge_cap`,
`broker_slot_width`, `free_slots`, `free_slots_by_accelerator`, `grant`,
`grant_epoch`, `observation_generation`, `observation_sequence`,
`ordinary_zero_cost_admission_sequence`, `valid_until`, and
`zero_cost_location_keys`. The hash covers the schema version, ordering, and
every authoritative input field; `allocation_input_sha256` stores that hash.
Missing or unknown fields and any schema other than 5 fail closed.
Materialization provenance is authenticated by the allocation's exact
observation/round join and is not duplicated into this map.

Allocation-map schema 5, worker placement projection protocol 2, internal
observation-authority payload schema 3, and PostgreSQL Serve schema 046 are
independent version domains. Equality of their
numbers carries no compatibility meaning.

Observation access context is query-route provenance, not physical-pool
identity. Several contexts may alias one physical UID and accelerator set. A
current physical-pool observation may authorize service edges reached through
those aliases once every edge independently proves that same physical UID; map
publication must not require the observer's chosen context string to equal
every claim's service-edge context. Each accepted intent remains pinned to its
own authenticated service-edge context.

Ordinary zero-cost rows currently persist their Kubernetes context and exact
accelerator shape, but not the context's physical-cluster UID. The sequenced
occupancy scan therefore conservatively debits such a row against every v2
pool with the same accelerator card and per-replica width, regardless of
context alias. Physically disjoint same-card pools may underfill; they cannot
oversubscribe. This compatibility duplication is not a second placement path.
The steady-state follow-up is to persist the physical UID on ordinary
placement, migrate live ordinary rows as they are authoritatively refreshed,
then remove the same-card duplication after no nonterminal zero-cost row lacks
that identity for one complete observation horizon.

Publication is all-or-nothing across a service's pools. If one edge is missing,
stale, blacked out, malformed, or concurrently replaced, no new complete map is
published. A previously published map is also rejected by `read_current()` as
soon as any of its authority moves or expires.

### 4. One reconciliation coordinator and pure planning

`ScaleReconcileCoordinator` is the single consumer for autoscaler work. It
coalesces notifications with a monotonic in-process generation, compares the
generation before waiting, and performs a bounded five-second recovery reread
even if an in-process notification is lost. The controller rereads durable
state on every pass. Provider calls and slow manager actions run without the
coordinator condition lock or the actuation-generation lock.

The controller takes a short optimistic actuation generation before planning
and revalidates it before each mutation. An update moves that generation to an
odd transition value, so stale work cannot publish into the successor runtime.

Once the durable gate is `SEQUENCED_ACTIVE`, the controller enters
`Autoscaler.sequenced_reserved_fill_planning()`. The existing autoscaler still
computes ordinary demand, scale-down shelter, and the legacy status projection,
but it emits no legacy fill launch and does not spend feed or advance rotation.
A missing or unreadable authenticated map means zero new fill for that pass;
there is no fallback.

`ReservedFillPlanner` is database- and provider-free. From one immutable map it
computes deterministic, exact pool/card intents after applying:

- service-global `max_replicas` headroom in the configured physical or logical
  capacity unit;
- ordinary demand debits;
- durable nonterminal fill rows from the same allocation map; and
- the last receipt-proven rotation anchor.

Planning mutates no feed, fairness cursor, or replica state. Its deterministic
idempotency key is correlation and replay-debit evidence; the database-assigned
replica row and returned receipt remain the commit boundary.

### 5. Concurrent multi-cluster preflight and commit receipt

`SkyPilotReplicaManager.accept_reserved_fill()` validates the typed plan and
manager/service owner before provider admission. It then acquires one fenced
provider phase and starts one physical-UID capture thread per distinct
`(Kubernetes context, physical UID)` pair. Independent contexts initialize in
parallel. A same-context initializer already in progress returns typed
backpressure instead of blocking the whole wave. The preflight deadline is 45
seconds for the whole batch, measured from one shared absolute deadline, and is
unrelated to `kubernetes.provision_timeout`. Per-context waits and thread joins
must consume only the remaining batch budget.

After all distinct-pool preflights report, one manager critical section
acquires the global demand-capacity reservation lock and revalidates service
ownership, current version, service-global headroom, pool epochs, observation
expiry, and physical identities. Intents are admitted in plan order while both
locks remain held through the existing protocol-v2 replica-row transaction.
That transaction independently revalidates the ordinary admission generation,
gate, allocation identity, round provenance, fresh observation, claim topology,
and owner before assigning the total zero-cost admission sequence. The
in-process lock order is manager then demand reservation; provider preflight
holds neither. Inside PostgreSQL, the zero-cost event sequencer is acquired
before lifecycle/service, round, claim, and replica rows, matching ordinary
admission and launch-result writers.

`FillCommitResult` is a bijective receipt that accounts for every planned
intent exactly once as accepted or deferred. A pool-local identity or preflight
failure produces a sparse receipt and does not starve healthy independent
contexts; a service-global owner, version, sequence, headroom, or provider-phase
failure defers the remaining ordered tail. Each accepted entry names its intent
hash and durable replica ID. The controller advances pool rotation only from
durably accepted rows. If authority remains current while any intent is
deferred, the controller immediately coalesces another reconciliation pass.

This avoids `N * provision_timeout` cluster initialization. For a 200-intent
wave across two Kubernetes clusters, the two physical-cluster captures begin in
parallel, then the manager persists every independently admissible intent and
returns an exact sparse receipt. Receipt acceptance proves durable replica-row
admission, not provider completion or readiness. Launch workers and the existing
request machinery proceed asynchronously. Readiness may still be
gradual because Kubernetes scheduling, image pulls, setup, model loading, the
provider phase, and executor capacity are real limits; the feature does not
claim all 200 become ready at once.

### 6. BCL reclaim invariant

Reserved fill remains zero-cost-only and uses the server/workspace-owned
preemptible inference placement. Worker placement projection protocol v2 is
the single admission owner. Each candidate adds
`projection_version: 2` and either `kueue_admission: null` or the exact closed
mapping `{local_queue_name, workload_priority_class_name}`. Namespace, service
account, Pod PriorityClass name/value/preemption policy, accelerator scheduling,
LocalQueue, and WorkloadPriorityClass are frozen together when the service
version is committed. `require_managed` is derived from non-null Kueue
admission; it is not separately caller-selectable.

Protocol v2 also owns the scheduler and actual binding seam. The immutable
candidate freezes `scheduler_name` from only the server-owned context/workspace
Pod configuration, defaults it to `default-scheduler`, and binds it through the
candidate digest and typed reclaim-policy view. Final rendering removes any
caller/restored `spec.nodeName` and installs exactly the projected scheduler.
The create response and a still-gated Pod must remain unbound; an admitted or
post-wait bound Pod is freshly joined to its exact Node, whose projected
accelerator label key/value must match the immutable candidate. Frozen affinity
without this bound-Node proof is not sufficient reclaim or capacity evidence
because direct `nodeName` binding bypasses the scheduler.

The LocalQueue is resolved from the service workspace's server-owned
`kubernetes.kueue.local_queue_name`/`kubernetes.quota.queue`. The
WorkloadPriorityClass is resolved only from the new server-owned
`serve_worker_kueue_workload_priority_class_name`. A managed queue without that
class, a class without a queue, or request-owned `resources.priority_class`,
`kubernetes.kueue`, or `kubernetes.quota.queue` makes a projected version or
launch fail closed. The full validated candidate has one deterministic
canonical JSON SHA-256 digest. Mutable launch-time configuration is never
reread for a projected worker.

The reclaim boundary is Kueue, not Pod priority alone. A Kubernetes
PriorityClass at `-1000` with `preemptionPolicy: Never` keeps inference below
ordinary scheduler work, but it cannot make a Kueue-gated BCL gang reclaim an
unmanaged inference Pod: Kueue may refuse to admit and therefore never create
the higher-priority Pods while unmanaged inference occupies the topology. This
is the root cause recorded in
`docs/designs/kubernetes-kueue-fail-closed-pods.md`.

Activation therefore requires deployment evidence for every reserved
inference context that:

- the inference namespace has an active LocalQueue selected by server-owned
  SkyPilot configuration;
- its ClusterQueue admits that namespace and shares a reviewed preemption
  domain with BCL/research workloads;
- lower inference workload priority and higher BCL/research priority are
  enforced in that domain;
- SkyPilot's strict plain-Pod path can read the queue objects and fails closed
  unless the admission response attests `managed=true`, the exact queue, and
  the Kueue scheduling gate; and
- the deployment's fail-closed admission policy prevents an unmanaged direct
  inference Pod from bypassing that path.

The evidence and enforcement boundary is one
`ReservedFillReclaimPolicy`. It is a code-owned, typed deployment extension,
not an environment variable, operator boolean, or JSON assertion. Python
entry-point group `skypilot.reserved_fill_reclaim_policy` must resolve exactly
one implementation; zero or multiple implementations fail closed. The generic
distribution intentionally installs none.

The required interface contract is
`GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2`. It supersedes the
worktree-only V1 contract, whose scope did not prove immutable Kueue admission.
V1 may remain parseable solely for an explicit historical diagnostic, but it
is rejected by activation, reauthorization, claim, and launch validation. The
activation evidence hash uses schema 2 and includes this exact V2 enum. An old
policy cannot become activation-ready merely by echoing a newly extended Python
scope: it must return V2 evidence under a correspondingly reviewed policy
identity and full-fleet bundle. Any already active experimental generation
would require normal fix-forward reauthorization, which advances the gate and
invalidates its allocation maps.

The one interface owns three operations:

1. activation attestation enumerates the exact current durable claim edges and
   returns the immutable fleet-bundle, policy-revision, and provider-inventory
   identity under the V2 claim/admission/launch contract;
2. every sequenced complete claim-set replacement authorizes its exact
   normalized requested edges, committed service version, and typed projected
   admissions against the stored identity; and
3. every sequenced launch authorizes its exact service, claim generation,
   physical UID, service version, complete worker-projection digest, namespace,
   service account, projected scheduler, Pod-priority contract, LocalQueue,
   WorkloadPriorityClass, accelerator, and width against the identity carried
   by the durable launch fence.

The typed policy view is derived from the worker projection; it is not another
persisted projection schema. It includes the exact nullable Pod Identity role,
so a policy must verify positive identity and identity-free admission with the
same interface. A claim edge stores only its exact closed
accelerator-to-digest map beside its existing normalized identity. The full
source remains the immutable version row. Claim replacement locks that row and
recomputes every edge map before commit. A version or admission-only change
therefore changes the semantic hash, advances the claim generation, and
invalidates the prior allocation even when pool topology and capacity policy
are unchanged.

Provider and Kubernetes reads for activation and claims complete before the
broker and PostgreSQL row locks are acquired. Launch authorization completes
before the fleet-wide reclaim guard and before any PostgreSQL transaction or
row lock. Reserved fill cannot also carry the ordinary bound-launch context.
For built-in Kubernetes reserved fill, every provider-mutation factory call
acquires the service-owner guard, obtains a fresh deployment-policy ticket,
then acquires the fleet guard, revalidates exact durable authority, performs
one bounded mutation, and releases all three before any passive wait. Ordinary
bound launches and opaque provisioners retain their existing whole-call
service guard. The deployment policy call receives one absolute five-second
monotonic deadline and must be cancellation-aware; a result returned after
that deadline is rejected before any authority lock or mutation. No path
acquires the per-service guard after the fleet-wide reclaim guard, so the lock
order remains acyclic. The returned typed authorization is short-lived and
exact-scope. The
claim transaction locks and revalidates the current gate generation and
identity plus the exact normalized edges, version row, projections, and
digests before persistence. Allocation publication and replica insertion
repeat the locked version/digest comparison. At launch, the executor reloads
the exact committed version projection, verifies the protocol-v2 digest carried
by the durable fence, and obtains a fresh exact authorization immediately
before the provider guard. Inside that guard it revalidates the typed
authorization, durable launch fence, current claim edge, current version row,
and generation-bound gate identity before yielding to provider mutation. A
restarted executor with a missing plugin, a differently identified bundle, a
stale version or digest, or a partial fence cannot launch.

Kubernetes deploy-variable generation receives the selected persisted
projection. It takes the LocalQueue and WorkloadPriorityClass only from
`kueue_admission`, sets the provider's required-managed contract, and never
uses `resources.priority_class` for a projected worker. The post-merge and
legacy-YAML-restore enforcement step reasserts both the provider fields and the
exact Pod labels/priority fields. The provisioner continues to preflight the
projected LocalQueue and attest the admitted Pod; any mutation of queue,
WorkloadPriorityClass, namespace, service account, Pod priority, or accelerator
shape fails closed before workload execution.

The terminal reclaim guard covers each bounded Kubernetes compute-mutation
window, including provider-internal create retries that can submit the
Kueue-managed workload and immediate rejection cleanup for an admitted Pod
whose projected identity changed.  The built-in Kubernetes provisioner
receives the canonical provider-effect guard from the backend and enters it at
the per-Pod create/retry boundary for reserved fill; it does not
rely on an outer guard around the opaque bulk-provision call. Every normal,
AppArmor-retry, and 409 replacement create attempt reacquires
separately. Force-remove and rejected-identity delete/read attempts also
reacquire separately, with all retry sleeps outside the guard. Every successful
create response is checked
for the exact queue, WorkloadPriorityClass, admission scheduling gate,
namespace, service account, Pod priority, and accelerator shape before that
guarded call returns. Existing Pods are reattested against their current Kueue
lifecycle state: an admitted Pod may have had the gate removed, but must retain
the exact managed/queue/WorkloadPriorityClass labels, Kueue's managed finalizer,
`podset=role-hash`, and the exact LocalQueue/ClusterQueue outputs bound at
preflight. `AssignQueueLabelsForPods` is therefore a deployment prerequisite.
A label-only Pod without either the create-response gate or the complete
post-admission binding is rejected and deleted under fresh authority.

Passive scheduling and readiness waits on this reserved-fill built-in
Kubernetes path never hold the per-service or fleet-wide advisory guard. After
the wait, every Pod is fresh-read and its complete admitted identity reattested
in one new guard epoch before provisioning can return. The fresh object must
remain `Running` and retain the exact UID captured by the all-containers-
running observation; same-name replacement and still-gated objects fail
closed. In
particular, a correctly Kueue-pending Pod
with `provision_timeout: -1` may wait indefinitely without blocking a service
version mutation, controller takeover, or reclaim-policy reauthorization.
Any later provider mutation or retry must enter a fresh guard, obtain a fresh
policy authorization, and revalidate the durable fence. If authority changed
while a Pod was pending, the stale request cannot perform another create or
destructive retry; durable controller reconciliation owns eventual cleanup of
the old replica.  A failure after entering this instrumented path does not run
opaque request-owned teardown; it returns a terminal reserved-fill fence and
the durable replica owner performs exact cleanup. This mutation/wait split is
the one canonical reserved-fill built-in Kubernetes path, including
protocol-v2-fenced requests emitted while `LEGACY_ACTIVE`. Fence-less
historical requests remain on the opaque whole-call path. An opaque
protocol-v2 provisioner is rejected before provider mutation; only the in-tree
Kubernetes provisioner can produce the exact create/adopt attestation required
to enter the materialized tail.

Pre-Pod auxiliary bootstrap and object-storage construction cannot occupy a
reserved accelerator slot and remain outside the reclaim guard. The successful
in-tree bulk/adoption return is the single one-way materialization boundary.
It is recorded before deploy-variable generation or any other local tail can
fail. From that point, every error is normalized to a terminal reserved-fill
fence: no capacity classification, cross-placement failover, or broad
request-owned teardown is permitted. Config-hash reuse is disabled for v2, so
an existing Pod re-enters the same current Kueue/projection adoption
attestation rather than inferring identity from cached configuration.

Post-Pod runtime preparation, internal file mounting, Ray/skylet startup,
workdir and file-mount synchronization, task setup, autostop/hook mutation,
port reconciliation, and job submission each run under a fresh bounded
service/policy/fleet guard. Passive Kueue scheduling and readiness waits remain
outside every guard. A missing guard fails closed; terminal cursor restoration
and other best-effort reporting cannot replace the typed materialized result.
Ordinary bound requests retain their existing authority path.

The asynchronous request boundary has one exact execution-quiescence protocol.
Every claimed invocation retains generation, claim token, worker instance,
outer-guardian Linux PID, and `/proc/<pid>/stat` process-start ticks until the
exact process-family boundary finishes effect-bearing handler code and cleanup
and publishes its receipt. API-request schema 010 has not been deployed, so
this identity has one universal meaning from its first rollout; no historical
schema-010 handler-PID rows exist. Lease expiry, controller handoff, signal
delivery, guardian absence, and process-pool failure revoke authority but are
not quiescence proof. They cannot make the request replayable. PID signalling
uses a pidfd and repeats the guardian's process-birth check, so PID reuse cannot
target another invocation. A schema-010 guardian PID disappearing without its
exact receipt remains fail closed; local PID absence never synthesizes family
quiescence.

The parent Future monitor is the durable receipt-delivery owner. The child
wrapper never writes an execution receipt: its return only reaches an inner
warden, and cancellation, retry, or failure can still require descendant
drain after that return. Durable PostgreSQL claims use one disposable
per-invocation execution path; reusable `ProcessPoolExecutor` workers and
forgotten broken-pool shutdown threads are removed. The retained
`BurstableExecutor` capacity interface owns one finite set of these
invocations, so a full lane leaves work durably queued instead of claiming an
unbounded hidden backlog.

Each invocation has a dedicated two-level process boundary: a minimal outer
guardian and an inner warden. Both become Linux child subreapers before the
inner warden spawns the handler as leader of a new session/process group. The
outer guardian PID and process-start ticks are published before handler
admission and remain the durable claim identity. Both owners are outside the
handler group, and bidirectional lifetime pipes make
each owner drain if the owner on the other side dies. If the inner warden is
hard-killed, its complete orphaned family, including children that called
`setsid()`, reparents to the per-invocation outer guardian instead of joining a
process-global orphan set. If the outer guardian is hard-killed, the inner
warden observes EOF and drains its family. API-parent death makes the outer
guardian drain. This per-invocation kernel ancestry boundary is canonical;
the API process is not used as a shared fallback subreaper because concurrent
families would become indistinguishable after reparenting.

The handler remains alive, or remains an unreaped zombie, as the exact family
root until descendant drain is complete. The direct-child guardian is not
reaped until its authenticated completion and durable receipt are accepted.
Every outcome, including normal success, terminates and reaps every descendant
before the handler root is released. A finite API invocation may not hand a
long-lived child to durable state: runtime daemons and managed-job controller
slots are the explicit runtime-owned abstractions for long-lived work. The
inner warden repeatedly terminates and reaps adopted descendants; the outer
guardian independently requires a stable empty family before it reports
completion and permits the exact handler to be reaped. Cancellation targets
the exact direct-child guardian, which treats `SIGTERM` as a drain request
rather than exiting. An unreadable
identity, a surviving child, or a termination timeout keeps the guardian,
warden, and claim unquiesced. Best-effort psutil enumeration is never receipt
proof. Graceful shutdown requests the same guardian-owned convergence protocol
instead of killing an untracked process. The parent Future becomes complete
only after the outer boundary reports typed outcome plus exact family absence;
only then may its monitor publish the first execution receipt.

The execution result is a closed typed outcome: `SUCCEEDED`, `PRE_EFFECT`,
`CANCELLED`, `RETRYABLE`, or `FAILED`. Every outcome requires the warden's and
guardian's complete descendant-absence proof. This turns arbitrary handler
threads, double forks, and `setsid()` descendants into one ownership boundary
instead of relying on a racy process-tree snapshot or a success-only child
handoff exception.

Every outer-boundary Future outcome proves both that the submitted callable
returned and that its required family drain completed. Transported wrapper or
result-serialization exceptions are closed typed outcomes and enter one
idempotent receipt loop with bounded backoff and no terminal give-up. The loop
ends only when the exact receipt is accepted or a database read proves that the
exact generation/token/worker identity no longer requires it. Abrupt guardian
or warden loss without the surviving peer's stable-empty proof remains
ambiguous and uses local family convergence; a cancelled-before-admission
invocation uses the claimed pre-effect proof below. Result monitors are
registered before their thread starts, removed only after durable convergence,
and joined after executor shutdown. Role ownership cannot be released while
one is still delivering a receipt.

Monitor setup is transactional: registration precedes `Thread.start()`, a
start failure runs the monitor synchronously, and an outermost `finally`
removes the exact registration. Executor startup likewise returns ownership
only after the queue server and every worker have started; any partial failure
stops and joins all earlier components before propagating. No effect owner can
exist outside the runtime's returned ownership aggregate.

Receipt delivery and outcome reconciliation are one parent-owned convergence
protocol, not independent fire-and-forget callbacks. A normal return preserves
the wrapper's already-fenced terminal result. `ExecutionRetryableError`
atomically consumes the exact parent-proven family result into the request's
`RUNNING -> WAITING` transition, clears the claim, and publishes one queue
delivery with a database-clock `available_at`. The transaction deliberately
ignores lease age: the exact generation, token, worker, live origin,
uncancelled row, and claimed queue delivery are its authority. The Future
monitor acknowledges the boundary and releases finite executor capacity
immediately after that handoff; it never sleeps while retaining a process
boundary. Any other transported callable exception is terminalized as the
exact claim's failure without overwriting a terminal child result, then
receives the same durable receipt.
Each database mutation is generation/token/worker fenced and idempotent, and a
transient failure retries the incomplete convergence rather than abandoning a
RUNNING row.

A locked claimed request in `PENDING` or `WAITING` with no PID has not crossed
the guarded `RUNNING` transition; revoking that exact generation is canonical
pre-effect proof and must atomically publish its exact generation-bound
quiescence receipt before cancellation, dispatcher failure, worker loss, or
shutdown can remove or terminalize its delivery. This applies uniformly to
ordinary and provider-reserved dispatch. A `RUNNING` request with a
nullable legacy API009 process identity remains ambiguous and fails closed.
For a terminal request whose family lost all receipt publishers after result
persistence, the result remains terminal but retention stays pinned; process
absence does not close it and no terminal request is reopened. Exact boundary
receipt closure accepts every replay policy, including `NEVER` and
provider-mutating handlers, because it cannot schedule an invocation; it is
also association-agnostic, so a bound ordinary launch can close retention
after persisting its immutable result. `READ_ONLY` and `RECONCILE` policy may
record retry intent after owner loss, but any execution-quiescence-required
claim becomes replayable only after the exact boundary receipt. Rows that
never entered this protocol may retain their existing policy recovery because
they have no admitted effect family.

There is intentionally no local PID-death observer or `/proc`-absence reducer.
It would have no authorizing fact once the recorded PID names the outer
guardian: abrupt guardian absence can precede the inner warden's drain. The
outer guardian, inner warden, and parent result monitor are the only receipt
publishers, and each may publish only after authenticated stable-empty proof.

Graceful role shutdown first stops all request dispatchers from claiming and
then converges every owned disposable boundary and receipt monitor. Executor
shutdown must return explicit per-guardian reaped or absent proof after its
receipt; a kill-helper timeout or a still-live boundary is a fail-stop result
and cannot be treated as drain completion. Controller shutdown first stops
runtime-daemon supervisors and proves their process groups absent; a generic
child kill must not race or replace the subsystem-specific supervisors. The
controller may release its leadership session, and an executor/all role may
release its instance lease, only after this sequence completes. Any timed-out
or failed supervisor, guardian, monitor, or queue join enters a not-Ready
convergence loop while retaining the leadership and instance sessions. It
retries authoritative drain and never exits merely to drop the PostgreSQL
session; only complete effect-owner absence permits ownership release.

API-request schema 010 adds the process-birth identity required by this
contract. Whole-Pod hard death of a finite API request intentionally remains
fail-closed: Kubernetes 404, force deletion, or same-name replacement does not
prove that containers on a partitioned node stopped, and an invocation is not
replayed across an unattested executable-image or handler-contract change.
Cross-Pod request replay is not implemented as a parallel happy path. A future
change may recover it only with a durable claim-bound executable contract and
authoritative effect-stop fence.

Perpetual controller maintenance loops are not finite API requests and must not
depend on an invocation receipt that can disappear with their owner Pod. The
steady state therefore removes every registered internal-daemon handler and
daemon queue submission path. The daemon specifications remain in
`sky.server.daemons`, but the elected controller runtime owns them directly:

1. after controller leadership is established, it retires every request and
   queue row whose ID is in the versioned historical-daemon allowlist before
   stale-claim fencing or generic request re-enqueue can decode or deliver one;
2. it evaluates each specification's `should_skip` predicate once for that
   leadership term and starts every selected daemon as a dedicated subprocess;
3. the subprocess starts a new Linux session/process group and enters through a
   `-S`, minimal standard-library launcher. Before importing any SkyPilot or
   handler module, the launcher receives the expected parent PID and
   process-start ticks, arms `PR_SET_PDEATHSIG`, re-reads both parent
   identities, becomes non-dumpable, and forks a minimal fail-stop guardian
   that kills the complete owned process group. The guardian immediately
   closes the capability transport and every inherited descriptor except its
   private control channel, reasserts non-dumpability, independently
   revalidates both process identities, and never installs controller
   authority. Only after that race-free
   effect-admission check does it restore the startup-captured clean server
   environment, load the executor plugin context, establish the system
   execution context, write the existing per-daemon log, and run the existing
   blocking event loop;
4. one runtime supervisor restarts an unexpectedly exited subprocess with
   bounded backoff, while cancellation sends `SIGTERM` to the owned process
   group, escalates to `SIGKILL` after a bound, reaps the exact child, and does
   not return until the process group no longer exists;
5. the minimal launcher makes parent death terminate the complete owned
   process group, including daemon grandchildren. A minimal guardian outside
   the daemon group and a launcher-side guardian-liveness monitor form a
   two-way fail-stop contract: supervisor/launcher death makes the guardian
   drain the group, while guardian death makes the launcher kill its own group.
   Neither an unmonitored guardian nor parent-death signalling of only the
   launcher is sufficient; and
6. controller shutdown cancels and joins those supervisors before releasing
   the outer leadership session, so a graceful leadership handoff cannot leave
   a locally owned daemon behind. A bounded join may escalate process-group
   termination, but failure to prove the group absent keeps leadership release
   fail closed.

The split controller's `ControllerLeaderLease` remains the outer fleet fence.
It sets the generation environment and completes legacy-row retirement before
stale-claim fencing. PostgreSQL `all` mode always uses the same lease,
regardless of whether managed-job consolidation is enabled, because its mixed
request queue can claim controller-class Serve and Jobs handlers even when no
fixed managed-job slot is active. It establishes that generation, opaque
origin capability, and loss monitor before it starts either execution class;
when managed-job consolidation is enabled, it also starts the fixed slots
before either class. It is a packaging compatibility mode, not a second
authority protocol. It attaches that exact generation to every
controller-class request claim; no PostgreSQL role admits a controller claim
whose generation is null. Normal-class requests remain outside this controller
generation fence. The combined process passes its owner explicitly through
the startup-maintenance and controller-claim boundaries rather than publishing
the generic controller identity process-wide to normal request work. Under one
legacy-daemon transition session it performs generation-fenced allowlist
retirement, fences nullable and prior-generation controller claims, and then
performs generation-fenced request recovery before starting any background
daemon, managed-job slot, or request worker. SQLite `all` mode uses the same
runtime interfaces
with one private owner-only authority file bound to an exact local PID and
process-start tick. Before any request recovery, decoding, re-enqueue, or
daemon startup, each mode retires the explicit historical IDs. Failure keeps
the role from serving. Each runtime daemon then uses its existing singleton
lock; local SQLite remains single-process and retires the same allowlist before
local recovery.
SkyServe and pool refresh retain
their existing, independently probed consolidation locks because those locks
fence controller recovery effects, not merely process scheduling. The other
maintenance events are overlap-tolerant by their existing resource locks or
idempotent database/telemetry operations. A PostgreSQL session can be released
server-side before its former owner observes the failure, so a singleton lock
alone is never described as proof that arbitrary old effects stopped.

The managed-job refresh loop and controller capacity are also explicit
controller-runtime ownership. A controller generation starts one fixed set of
runtime-owned `ManagedJobControllerSlotSupervisor`s. Each numbered slot owns
one local guardian handle and one disposable `ControllerManager` process
family; it starts at runtime admission, polls for work even when the queue is
empty, and is never created by an API request, `submit_jobs()`, a PID-file
scan, or another controller process. The slot count is computed once from the
existing controller parallelism policy at generation startup. Each manager
still multiplexes the existing bounded jobs-per-worker capacity, so the
topology remains bounded at the current 2,000-job fleet ceiling rather than
creating one process or request per job.

Managed-job schema 028 adds nullable `controller_slot_id` and
`controller_slot_attempt` columns plus a non-null
`controller_slot_quiescing` column with a database default of false. The
migration rejects an already adopted nullable quiescence shape instead of
silently retaining a schema weaker than the runtime invariant. A slot attempt
is a fresh UUID for each disposable manager birth. `WAITING -> LAUNCHING`
atomically stores the exact
`(controller_instance_id, controller_generation, controller_slot_id,
controller_slot_attempt)` tuple under the existing shared leadership-row lock.
Every controller-owned state transition, cleanup decision, and reservation of
a new provider effect compares that whole tuple. Controller-originated nested
API actions carry it as internal admission metadata. Normal service-account or
loopback authentication remains mandatory; a separate 256-bit opaque
controller capability authenticates the claimed outer origin, and only its
SHA-256 digest is durable. The elected controller runtime installs the raw
value in a PID-bound process-local registry, removes every raw/path environment
representation, and becomes non-dumpable before it spawns owned work. It
captures one canonical RequestWorker environment with the generic controller
pair and every managed-job owner/job/slot field removed. Controller-class
request handlers receive their durable outer pair only for local database
fencing; without an authenticated managed origin and process-local capability,
that pair emits no controller-origin SDK headers. Trusted runtime daemons
receive the nonsecret outer pair as explicit launcher arguments and the raw
capability through a fresh one-shot inherited pipe on every restart, install
both before plugin/daemon effects, and never recover them from the neutral
RequestWorker snapshot. The managed-runtime owner published by PostgreSQL
`all` mode alone does not authorize controller-origin SDK headers, even while
the process-local capability exists; a normal combined-process coroutine
therefore remains ordinary work. A complete managed attempt context or the
generic pair installed at a trusted daemon/controller boundary is required.
The runtime also passes the value explicitly to the slot
supervisor, which transports it to
each disposable manager through one-shot inherited pipes across both guardian
owners. Transfer handles are redeemed immediately by a non-dumpable boundary
owner; raw authority is then relayed only through close-on-exec descriptors, so
pre-admission cancellation cannot strand it in a parent resource sharer. The
manager starts through a `-S`, standard-library-only bootstrap, becomes
non-dumpable, consumes and closes its descriptor, and installs the same
PID-bound registry before enabling site packages or importing SkyPilot,
plugins, or lifecycle code. It removes all transport state before it can
execute a user event callback. Every
runtime-daemon birth or restart similarly runs through a `-S`,
standard-library-only bootstrap, receives a fresh one-shot descriptor and
explicit nonsecret outer owner pair, then becomes non-dumpable, consumes and
closes the descriptor, and installs process-local authority before enabling
site packages or importing `setproctitle`, SkyPilot, or plugins. It then
installs the outer pair before the first SkyPilot import.

A disposable request handler receives a fresh descriptor only when its queue
claim carries the complete, transactionally verified five-field managed-job
origin. The handler rechecks that tuple against the durable request, installs
it as bounded request context, and resets that context after execution. A
controller-class request without that origin receives no raw authority. An
exec child has no process-local registry, and neither the runtime,
guardian/warden, daemon, manager, handler, nor callback environment or argv
contains the raw capability. Registry clearing is PID-bound and exact-owner
scoped: a fork child fails closed, and cleanup never clears unrelated process
authority. This boundary isolates the new internal controller authority; it
does not claim that historical user or service credentials are absent from
callback environments. Caller-supplied
origin headers are stripped case-insensitively before server-owned values are
installed. The API persists
the complete five-field job/outer/slot tuple and accepts creation, queue claim,
and guarded `RUNNING` admission only while the live outer generation and exact
non-quiescing slot attempt own the job. PostgreSQL always locks outer
leadership, job, request, then queue in that order. A stale process can
therefore neither mutate durable job state nor ask the API tier to begin a new
provider effect after its slot is replaced. User cancellation remains its
separate durable intent path.

Each slot uses the same two-owner shape as a finite request: a local outer
guardian and inner subreaper warden surround the manager session. The runtime
does not publish, interpret, or compact a shared PID inventory: Linux PIDs,
process-start ticks, and guardian handles are Pod-local supervision evidence,
not cross-Pod authority. If a slot manager exits unexpectedly, its local
supervisor first converges and reaps that exact process family. It then closes
nested admission by setting `controller_slot_quiescing`, terminalizes
unadmitted deliveries with pre-effect receipts, cancels admitted deliveries,
and waits for every exact API guardian receipt. Only after both proofs may it
reset every non-`INACTIVE`, non-`DONE` job carrying the dead slot attempt,
rotate the attempt UUID, and start the replacement. A row whose task family is
already terminal returns to `WAITING` as cleanup-only lifecycle work; a row
with a nonterminal task returns to the ordinary execution path. Any uncertain
local or nested family drain keeps
the controller generation not Ready and retains leadership rather than
resetting or replacing the slot. A whole-Pod or outer-leadership loss is
recovered differently: the successor generation first closes every stale
exact nested origin and waits for its receipts, then resets every non-
`INACTIVE`, non-`DONE` prior-generation row and only then admits its fixed
slots. A terminal task family is again cleanup-only, never workload recovery.
Schema 010 and slot schema 028 are first-rollout additions. A fully nullable
pre-slot row may be adopted exactly once only after the successor owns fresh
outer leadership and a locked request-store query proves that no request row
for that job carries any managed-job origin. A partial job slot identity, a
partial nested-request origin, or any nested origin associated with that
nullable job is ambiguous and fails closed. No successor invents an origin,
interprets a foreign PID, or needs a shared PID inventory to do so.

Managed-job provider mutations have one path. Launch, recovery, cancel,
cluster teardown, pool cancellation, status confirmation, and ephemeral
storage deletion enter through SDK-created API requests while the exact
per-job context is active. The fixed-slot manager is the sole cleanup owner:
after exact stale-attempt quiescence, terminal task families are claimed as
cleanup-only work and run the ordinary complete manager cleanup without
constructing `JobController`, relaunching a workload, or changing its terminal
task outcome. Cleanup failure retains the exact claim and retries by phase;
`DONE` is an exact-attempt, non-quiescing, all-tasks-terminal commit after
cleanup and token revocation succeed. The outer refresh reconciler is
observation-only for every complete fixed-slot row and performs no provider or
storage effect. It retains only the narrow pre-slot PID terminalization needed
during the first image rollout and defers its resulting terminal row to the
same cleanup-only manager path. There is no refresh-owned or operator-owned
second cleanup authority.

Startup creates the refresh owner and every fixed slot transactionally before
the controller role becomes Ready or starts controller-class request workers.
Shutdown first prevents new refresh, slot, and request claims. The exact
refresh thread reaches effect quiescence while retaining its thread-local
consolidation lock; request boundaries and slot guardians then drain and join.
The main thread finally asks the refresh owner to release its own lock, joins
it, and only then releases outer controller leadership. Any bounded join may
report current failure, but subsequent convergence iterations re-run the
authoritative liveness and cleanup checks while retaining ownership. A cached
timeout, logged best-effort kill, PID-file deletion, or one-shot process-tree
snapshot is never a handoff boundary.

This is the sole managed-job controller happy path. The historical
`JOB_CONTROLLER_PID_PATH` inventory, `get_alive_controllers()`,
`start_controller()`, request-triggered `maybe_start_controllers()`, and their
polling/cutover machinery are deleted in the feature change rather than kept as
a compatibility branch. Rows written before slot schema 028 are nullable only
for migration. A new runtime always advances the outer generation before
recovery, applies the locked no-associated-origin proof above, handles each
eligible row once as stale lifecycle ownership, and never decodes it as a
current slot claim. The stacked cleanup removes this nullable adoption rule
after one complete fleet rollout and a database proof that no non-`INACTIVE`,
non-`DONE` pre-slot row remains.

Shutdown convergence is live and retryable. A bounded background/task join can
record its current failure, but the next convergence iteration must re-run
authoritative liveness and cleanup checks; it cannot replay a cached timeout as
permanent failure after the effect has disappeared. Every retry keeps the role
not Ready and retains the outer PostgreSQL ownership sessions.

The historical allowlist comprises the six daemon IDs in the current source
plus the retired `managed-job-status-refresh-daemon`; arbitrary user request
IDs ending in `-daemon` are never deleted. This is one execution path, not a
daemon-specific replay exception. Daemon
rows are excluded from API status, cancellation, shutdown, request retention,
and execution-quiescence logic because the new runtime never creates them. A
narrow legacy-row retirement helper is the only transition artifact. The
stacked cleanup removes that helper, the historical ID allowlist, recognizer
for historical supported-handler names with the `daemon:` prefix, and pickle
compatibility stubs after one full controller-fleet rollout and a PostgreSQL
query proves zero rows for every allowlisted ID for at least one
controller-instance stale window. Request IDs are never classified by a
`-daemon` suffix. Rolling back to a binary that recreates daemon requests is
unsupported;
operational recovery is fix-forward with a successor image.

Activation and active reauthorization similarly perform external attestation
first, then predicate-lock and revalidate the exact PostgreSQL claim scope
under the same-session broker and fleet locks before atomically binding the
complete evidence and writer receipt in the gate CAS. On first activation,
every PENDING or PROVISIONING row is locked and decoded. Non-fill rows are
ignored only after a current decode; every queued fill must be an exact current
ReplicaInfo v17 record whose scalar columns agree, service version equals the
locked committed version, projection digest matches the locked claim
admission, and policy tuple names the successor generation. Worktree-only v16,
a stale version or digest, a partial policy tuple, or any decode/proof failure
blocks activation. READY legacy rows remain readable. On active rotation,
old allocation maps are invalidated and the successor generation terminally
fences queued old requests. An activation-time census without the ongoing
claim and launch methods is insufficient. The deployment must preserve the
currently authorized policy identity and its external enforcement while
requests bearing that generation can execute.

First activation may attest a legacy-null claim version/digest pair by deriving
it from the locked current protocol-v2 version. Activation does not mutate the
claim. The canonical sequenced claim heartbeat must refresh that row with
Serve046 version and digest fields before schema-5 allocation publication can
resume.

Therefore the preparatory feature image may run at `LEGACY_ACTIVE`, but it
cannot activate `SEQUENCED_ACTIVE`: `activate` fails before broker-lock
acquisition or gate mutation when no unique plugin exists. Read-only `status`
continues to work. The Boltz deployment must ship the canonical policy and its
Kueue bundle before activation. This is an explicit open gate, not an operator
bypass or a claim that the current east1 or Phoenix topology is reclaimable.

Historical worker projection v1 rows remain readable only for ordinary launch
during the pre-activation transition. They cannot participate in a sequenced
claim, allocation, fill admission, or terminal launch. After all active service
versions are recommitted with protocol v2 and production has remained
`SEQUENCED_ACTIVE` through the documented cleanup gate, stacked cleanup PR
#1452 removes the v1 ordinary-launch decoder and its transition tests. New
writes always use v2; no compatibility setting can create a v1 projection.

When this external contract holds, a fill intent cannot spill to a paid
candidate, Kueue can evict lower-priority inference Workloads before admitting
BCL/research work, and SkyServe's existing preemption handling reconciles the
reclaimed replica away. A subsequent physical observation reduces free supply,
so no new map can spend a slot while BCL owns it.

`provision_timeout: -1` neither creates nor weakens the reclaim contract. It
only permits a correctly Kueue-managed inference Workload to remain pending.
This implementation launches no GPU or BCL canary. Existing deployment
evidence or a separately authorized Kueue rollout must satisfy the contract
before activation. If it does not, the new image may deploy but the gate stays
`LEGACY_ACTIVE`.

## What is implemented and what is not

### Present in the current worktree

- PostgreSQL Serve044 observation, sequencing, provenance, allocation, and
  fail-closed gate columns; Serve044 follows the upstream Serve043 placement-
  projection migration without rewriting historical migration authority.
  Forward-only Serve045 adds the complete generation-bound reclaim receipt and
  exact activation/reauthorization guard. Forward-only Serve046 adds committed
  service version and closed accelerator-to-projection digest maps.
- `reserved_fill_projection_authority.py` is the canonical adapter from one
  immutable worker projection to typed reclaim admission. New writes emit
  homogeneous explicit projection protocol v2; sequenced paths require
  non-null typed Kueue admission. Protocol v2 supports both an exact AWS role
  ARN and an explicit null identity contract, and the value is hash-bound and
  exposed to the deployment policy. Protocol v1 remains only as the historical
  ordinary-launch decoder pending cleanup PR #1452.
- API capability 77 advertises projection protocol v2; allocation-map schema 5
  binds service version and the closed digest map; ReplicaInfo v17 persists the
  selected scalar digest. Historical v15 records remain readable but cannot
  pass a sequenced launch fence, and worktree-only v16 is not a durable format.
- Concurrent provider-free pool observer and typed blackouts.
- Committed-observation broker rounds and complete authenticated maps.
- Ordinary zero-cost commit sequencing and complete v17 attribution.
- Lost-wakeup-free controller coordinator and optimistic actuation generation.
- Pure planner, autoscaler adapter, manager sparse receipt, and receipt-driven
  rotation.
- Fleet transition CLI requiring protocol v2, Serve046, API010, an exact stable
  split-role `api`/`controller`/`executor` writer cohort on one immutable image
  digest, and one entry-point-loaded deployment reclaim policy. The same
  command reauthorizes active fix-forward generations. The generic build has
  no policy plugin and deliberately blocks authorization before broker lock or
  CAS.
- Provider-free reconciliation diagnostics derived from current authenticated
  schema-5 allocation, its exact durable observations, and exact-version/
  projection v17 replica rows.
- An exact v17 queued-effect proof at first activation and per-mutation
  Kubernetes guards with immediate create-response attestation, guard-free
  passive waits, and durable-owner cleanup after a terminal fence.

None of the above is merged, deployed, or activated as of this update. The
implementation and owner-death/request-liveness integration are complete in
this worktree. The final serial PostgreSQL validation passes on the exact code
revision recorded below; three consecutive adversarial reviews remain before
merge.

### Runtime audit corrections implemented and frozen for review

The runtime audits found additional correctness and bounded-progress defects.
The current worktree now:

1. conservatively debits a target-less ordinary `SCALE_UP` against every
   compatible authenticated pool/card feed, clipped independently per feed;
2. uses one absolute deadline for the complete multi-context physical preflight
   batch, including cancellation, release, and thread joins; bounds provider
   root child-drain on exit; and keeps an incompatible provider phase closed
   out until any non-cooperative child actually releases;
3. propagates one cancellation-aware absolute deadline through Kubernetes
   client admission, fence capture, RPC timeouts, retries, and parsing
   checkpoints so timed-out work releases observer capacity; and
4. treats observation access context as authenticated acquisition provenance,
   while allocation consumption joins aliases by physical UID, pool key, and
   accelerator identity and independently revalidates each service-edge launch
   context; and
5. separates all-zero-cost row attribution from the global ordinary-demand
   invalidation generation, requires exact generation equality at allocation
   read and fill commit, and joins final fill persistence to the shared demand
   lock. This prevents a service-B ordinary commit from racing a service-A fill
   while allowing broker-disjoint fill commits to proceed independently;
6. snapshots an independent global first-success materialization sequence in
   every observation, stamps each zero-cost row transactionally on its first
   successful launch persistence, and uses admission plus materialization event
   order instead of wall-clock or readiness guesses for sequenced occupancy;
7. treats the complete grouped replica snapshot as part of sequenced spendable
   authority, rejecting the new round on enumeration, query, or decode failure
   rather than optimistically skipping an unread service; and
8. includes ordinary zero-cost rows owned by nonclaimant services in sequenced
   occupancy, with conservative same-card/same-width duplication until
   ordinary placement persists physical-cluster UID attribution;
9. observes raw exact-card GPU counts independent of claimant width, converts
   them exactly once under the broker's authenticated deterministic width, and
   publishes the width as allocation and diagnostic provenance;
10. treats same-context heterogeneous cards as disjoint physical UID/card
    edges with deterministic unique positions, while still rejecting a
    duplicate exact-card edge or conflicting exact-card alias width;
11. keeps last-proven UID topology across transient discovery failures and
    tries authenticated context aliases under one fair bounded query deadline,
    persisting only the winning route; and
12. initializes distinct physical captures in parallel outside the manager
    lock, retains each successful capture through every join-only persistence
    seam, rechecks pending service versions before and at the row transaction,
    and returns sparse receipts for pool-local failures; and
13. makes same-context exact-card observers join one UID initializer, rejects
    case-folded provider-card collisions, and preserves permission-denied
    evidence instead of intermittently blacking out a sibling card as generic
    provider failure; and
14. replaces the activation-only reclaim boundary with one uniquely loaded
    policy whose immutable identity is bound by Serve045, whose current
    service version/projection is bound by Serve046, and which is enforced at
    each sequenced claim transaction and terminal provider launch, while a
    complete successor receipt supports active fix-forward reauthorization;
    and
15. makes the final sequenced replica insert the durable capacity-spend
    boundary: under the locked current service specification and sorted
    replica rows it rejects service-wide intent replay, enforces physical
    aggregate/per-card feed, and enforces the physical-or-logical
    `max_replicas` unit before advancing the admission sequence; and
16. binds Serve046 service version and projection-v2 Kueue identity through
    claims, schema-5 maps, v17 rows, activation, status, and terminal launch,
    while splitting built-in Kubernetes mutation guards from passive `-1`
    scheduling/readiness waits; and
17. retains exact finite-request generation/token/worker/guardian-PID/process-
    birth identity until real process-family quiescence, routes pre-effect
    revocation through the sole replay reducer, fences every signal against PID
    reuse, preserves terminal results, and leaves guardian or whole-Pod death
    without a receipt fail-closed instead of treating deletion or lease age as
    process-stop evidence; and
18. removes perpetual controller daemons from the API request/replay protocol,
    retires legacy daemon rows before request recovery, and supervises one
    parent-death-fenced subprocess per selected daemon under controller/runtime
    singleton ownership with bounded termination and exact child reaping; and
19. closes Kubernetes direct-binding as a parallel placement path by removing
    projected `nodeName`, freezing and attesting the exact server-owned
    scheduler, rejecting webhook or gated-Pod binding, and requiring a fresh
    exact bound-Node accelerator-label join before admitted adoption or
    post-wait success; and
20. closes finite-request liveness under graceful shutdown: every claimed
    PID-less pre-effect terminalization publishes an exact receipt, terminal
    results remain pinned until a boundary-authored receipt, and leadership or
    instance ownership remains held until guardians and durable Future-receipt
    monitors are quiescent; and
21. replaces reusable/untracked request-process ownership with one disposable
    per-invocation outer-guardian/inner-warden subreaper boundary, publishes no
    receipt until its exact family is absent, makes startup and monitor
    registration transactional, makes daemon guardians mutually fail-stop,
    and replaces managed-job PID-file spawning with fixed runtime-owned slots
    whose generation/slot-attempt fence covers every state and provider-effect
    boundary and whose exact families participate in the same
    retry-until-proven-absent ownership handoff.

Regression tests exist for corrections 1--21, including owner-death,
request-liveness, legacy-row retirement, runtime-daemon supervision,
scheduler/bound-Node admission, capability bootstrap, and fixed managed-job
slot ownership. The complete non-PostgreSQL matrix, changed-source lint, and
Terraform module tests pass. Final serial PostgreSQL evidence and three
consecutive adversarial passes remain required before merge.

The audit also proved that BCL reclaim is a deployment prerequisite rather
than a Pod-priority property. The current east1 evidence recorded by the Kueue
design found no inference LocalQueue and a research ClusterQueue that excludes
the inference namespace. Phoenix has standalone Kueue with topology-aware
scheduling disabled, but its checked-in queue policy likewise covers only the
research namespace. The `mt_hybrid` workspace selects both contexts while the
control-plane configuration supplies no inference `kueue.local_queue_name` for
either. Those topologies are not activation-capable. Open platform PR #8211 is
Phoenix-only, currently conflicting, and explicitly leaves east unchanged.
This SkyPilot change intentionally does not duplicate cluster queue policy in
core.

### Status actually exposed

The public server API version is 77. Reserved-fill reconciliation status keeps
its independent minimum capability at 76; immutable worker placement
projection requires API 77 plus projection protocol 2. These are distinct from
the PostgreSQL API-request schema revision 010 required for activation. The
controller's `/autoscaler/info` response always contains a nested
`reserved_fill_reconciliation` object. The user-facing service status copies
the same object when `with_target_num_replicas` is requested.

The stable top-level fields are:

| Field | Meaning |
|---|---|
| `enabled` | Whether reserved-capacity fill is enabled for this service. |
| `authority_mode` | `disabled`, `legacy`, `sequenced`, or `unavailable`. Once selected, `sequenced` remains visible even if its map is missing or stale; diagnostics never imply a legacy fallback. |
| `allocation_current` | Whether a complete authenticated allocation map is current. |
| `allocation_generation` | Current map generation, or `null`. |
| `allocation_input_sha256` | Current map content hash, or `null`. |
| `allocation_claim_generation` | Claim-set generation authenticated by the current map, or `null`. |
| `reconciliation_gate_generation` | Current sequenced gate generation authenticated by the map, or `null`. |
| `reclaim_policy_identity` | The three durable reclaim-policy identity fields when sequenced, or `null`; this is metadata, not permission to launch. |
| `pools` | Mapping keyed by canonical physical pool key. |

Each pool in a current allocation exposes:

| Field | Meaning |
|---|---|
| `physical_cluster_uid` | Physical Kubernetes identity fenced by the allocation. |
| `kubernetes_context` | Access context carried separately from physical identity. |
| `service_generation` | Broker service generation in the authenticated edge. |
| `observation_generation` | Exact durable observation generation used by the map. |
| `observation_sequence` | Exact global zero-cost sequence at observation start. |
| `observation_valid_until` | Conservative authority expiry timestamp. |
| `observation_available` | Whether the exact durable observation payload was available to the diagnostic read. |
| `broker_slot_width` | Authenticated GPU width used by the broker's one and only raw-GPU-to-slot conversion. |
| `observed_free_gpus` and `observed_free_gpus_by_accelerator` | Raw physical supply from that exact observation; `null` if the optional diagnostic read fails. |
| `observed_free_slots` and `observed_free_slots_by_accelerator` | Diagnostic conversion of that raw observation using `broker_slot_width`; this is not additional authority. |
| `spendable_slots` and `spendable_slots_by_accelerator` | Broker-published feed in the current allocation, bounded and split before planner replay debits; it is neither a live provider total nor a guaranteed remaining tail. |
| `grant` and `edge_cap` | Authenticated service allocation bounds. |
| `current_allocation_admitted_replicas` | Nonterminal reserved-fill rows carrying the exact schema-5 allocation identity, service version, selected-card worker-projection digest, observation provenance, typed intent, and positive database admission sequence; `null` if the optional progress read fails. This is not total pool or service holdings. |
| `current_allocation_ready_replicas` | Ready subset of those exact current-allocation rows; `null` if the optional progress read fails. This is not total pool or service readiness. |

The projection never queries a provider and never authorizes a launch. Failure
to inspect the reconciliation selector yields `authority_mode: unavailable`.
Failure of an optional exact-observation or replica-progress read preserves
`authority_mode: sequenced` and the current allocation metadata while returning
`false`/`null` for the unavailable detail. The endpoint remains healthy in
either case.

## Implementation phases and intentional departures

| Phase | Scope | State |
|---|---|---|
| 0 | Historical multi-pool protocol v2, UID fences, claims, grants, and zero-cost-only launch seam | Already present before this correction. |
| 1 | Observation ledger, admission sequence, authenticated map, coordinator, pure planner, manager receipt, diagnostics, and Serve045 reclaim-policy identity | Implemented and behavior-frozen; exact automated freeze passes; review pending. |
| 1b | Worker projection protocol v2, Serve046 version/digest claim binding, allocation schema 5, replica state v17, exact projected Kueue rendering, and terminal revalidation | Implemented and behavior-frozen; exact automated freeze passes; review pending. |
| 2a | Full-fleet feature-image rollout while the gate remains `LEGACY_ACTIVE` | Not deployed. |
| 2b | Deployment-owned Kueue bundle and unique entry-point policy, including ongoing future-claim and launch fences | Core interface/enforcement is implemented; no Boltz plugin/bundle exists, and current east/Phoenix evidence does not pass. |
| 2c | Generation-fenced reconciliation authorization after exact fleet, schema, claim, and reclaim attestation | Not activated; intentionally impossible in the generic feature build. |
| 3 | Compatibility-path deletion in draft cleanup PR [#1452](https://github.com/boltz-bio/skypilot/pull/1452), including forward-only Serve047 steady-state bootstrap | The stale draft is not mergeable as written; it must be reauthored from the exact frozen feature revision and remains merge-gated below. |

Durable acceptance hands rows to the existing asynchronous launch path, and
status projects the same allocation/observation evidence used by
reconciliation; neither is a second source of launch authority.

## Deployment, activation, and fix-forward reauthorization

### Preconditions

Before changing the gate:

1. Run the complete required test suite and three passing adversarial reviews
   against this exact file and code revision.
2. Merge the feature only after its stacked cleanup PR has been authored and
   linked as described below; the cleanup remains draft and merge-gated.
3. Build and push one immutable image from the merged feature commit.
4. Record the live Helm release name, namespace, chart revision, complete
   release values, rendered role topology, and immutable image identities for
   diagnosis, but use fix forward rather than Helm rollback.
5. Review one direct Helm upgrade that preserves the live release values and
   changes only the full-fleet runtime image for this rollout. Do not change
   the PostgreSQL target, credentials, persistent storage, role topology,
   namespace, reserved-pool infrastructure, or Kueue policy in the same
   upgrade.
6. API-request schema 010 and the managed-job slot columns have not shipped.
   Apply their additive migrations before target-image Pods become Ready. The
   ordinary controller leader handoff is the only cutover: the old leader stops
   claims and drains its request workers and controller processes before
   releasing leadership; the new image advances the outer generation, recovers
   nullable or prior-generation managed-job rows as stale ownership, then
   transactionally starts its fixed slot supervisors. Verify that no old-image
   controller lease or controller process remains before calling the
   controller cohort converged. There is no shared PID-file migration, Recreate
   flag, or alternate scheduler path to activate or later remove.
7. Wait for the full split-role `api`, `controller`, and `executor` cohort to
   be Ready on the same immutable image. This is a fleet rollout, not a
   capacity canary. The gate remains `LEGACY_ACTIVE` throughout image
   convergence.
8. Verify public server API capability 77, API-request schema 010, Serve schema
   046, worker placement projection protocol 2, allocation-map schema 5,
   replica state 17, and reserved-fill protocol v2. Confirm every active
   reserved-fill service version has exact protocol-v2 worker projections;
   historical v1 projections cannot pass activation-ready claim checks.
   Confirm every queued PENDING/PROVISIONING fill row is an exact current v17
   record bound to the locked current service version, claim projection digest,
   and successor policy tuple; drain any legacy, stale, or undecodable row.
9. Prove the deployment-owned Kueue LocalQueue, ClusterQueue namespace
   selection, shared preemption domain, workload priorities, strict SkyPilot
   configuration, RBAC, and fail-closed admission contract for every reserved
   inference context. Pod priority alone does not pass this gate. Launch no GPU
   or BCL verification workload for this rollout.
10. Build and deploy a second immutable full-fleet image containing the unique
   Boltz policy plugin after that deployment-owned Kueue contract passes. Wait
   for every split-role writer to converge on its digest and re-run the exact
   fleet/schema/policy status proof before activation. The preparatory generic
   feature image remains safe at `LEGACY_ACTIVE` but is never activation-ready.

The live Helm release is the deployment authority. A platform repository pin,
Terraform/Terragrunt state, or open platform PR is neither a prerequisite nor
evidence of the deployed SkyPilot version. Before applying, capture `helm get
values` and `helm get manifest`, resolve the exact `api`, `controller`, and
`executor` Deployments, and review the rendered diff. Upgrade the existing
release with the repository chart and `--reuse-values`, setting the immutable
image for every role that has an explicit override. Any unintended namespace,
ingress, PVC, database, authentication, Secret, service-account, role-topology,
or other persistent-resource change blocks the apply. Post-deploy evidence is
the live Helm revision, immutable image digest, rollout state, schema status,
and non-compute health checks—not a repository tag or PR alone.

The separately owned Kueue contract is a different deployment change. It may
proceed through its own reviewed authority only after the east and Phoenix
inference partitions, exact RBAC, server queue configuration, fail-closed
admission, and the code-owned policy plugin are implemented and reviewed. It
is not smuggled into the generic runtime-image Helm upgrade and does not block
deploying that image safely at `LEGACY_ACTIVE`.

### Mechanical activation

Run the transition command from the deployed control-plane environment that
has the central PostgreSQL URI and the chart's Kubernetes inventory access:

```bash
python -m sky.serve.reserved_fill_reconciliation_transition status --json
python -m sky.serve.reserved_fill_reconciliation_transition activate
python -m sky.serve.reserved_fill_reconciliation_transition status --json
```

Activation fails unless all of the following are true:

- the database is central PostgreSQL;
- reserved-fill protocol version is exactly 2;
- Serve and API-request schema revisions are exactly 046 and 010;
- Kubernetes and PostgreSQL inventory attest exactly the split roles
  `api`, `controller`, and `executor`, with no compatibility `all` writer;
- all attested writer pods are Ready and all recent process leases match that
  exact pod cohort; and
- every writer Deployment uses one immutable image digest; and
- exactly one deployment-installed `ReservedFillReclaimPolicy` returns fresh
  typed evidence for the exact current claims and the global future-claim and
  terminal-launch enforcement contract. The activation CAS binds its exact
  fleet-bundle, policy-revision, and provider-inventory identity. The generic
  feature image always fails this check.

On one PostgreSQL session, authorization acquires the broker and fleet advisory
locks, opens the SQL transaction, predicate-locks claim scope, and performs one
generation-fenced CAS. First activation changes `LEGACY_ACTIVE` to
`SEQUENCED_ACTIVE`; subsequent fix-forward runs retain `SEQUENCED_ACTIVE` and
advance one generation. A retry with the exact receipt is idempotent. There is
no demotion command, and the PostgreSQL guard rejects a transition back to
legacy.

### Fix-forward behavior

After `SEQUENCED_ACTIVE`:

- old or mixed writers are not a supported target;
- a controller that cannot read the gate or current map suppresses new fill;
- ordinary demand and existing serving replicas continue through their
  existing paths;
- existing legacy fill rows remain readable and may be sheltered or cleaned up,
  but cannot authorize new sequenced capacity; and
- a defect is repaired by deploying a newer full-fleet image and invoking the
  same authorization command. A changed writer/policy receipt advances the
  gate exactly once and invalidates old allocation maps; an exact retry does
  nothing. Durable observation and occupancy history remain in place.

No rollback or canary protocol is promised. The safe failure mode is temporary
reserved-capacity underfill, not duplicate fill or paid spill.

## Manual verification after activation

No step below creates compute.

1. Confirm the transition status reports protocol 2,
   `SEQUENCED_ACTIVE`, Serve046, API010, and the exact expected durable reclaim
   identity.
   Confirm the full fleet reports public server API capability 77 and worker
   placement projection protocol 2.
2. Confirm every claimed physical pool is producing completed `SUCCESS` or
   explicit `BLACKOUT` observation generations, with no success used past its
   `valid_until`.
3. Confirm a fill-enabled service publishes a nonzero allocation generation
   only when every current edge has matching round and observation provenance;
   confirm schema 5 carries the current gate generation, committed service
   version, exact accelerator-to-worker-projection-digest map per edge, and
   durable reclaim identity.
4. Confirm newly accepted fill replica rows carry the full allocation,
   observation, intent, reclaim identity, service version, exact worker
   projection digest, and positive
   `zero_cost_admission_sequence` tuple.
   After its first successful launch persistence, confirm the row also carries
   one immutable positive `zero_cost_materialization_sequence`.
5. Confirm an ordinary zero-cost row created after allocation publication
   advances both admission counters, makes that allocation unreadable, and
   causes any stale fill persistence attempt to write no row. Confirm a peer
   fill advances only the total counter and does not invalidate a map at the
   same ordinary-demand high-water. Confirm first launch success advances only
   the materialization counter and that retrying the same success preserves
   the row's original marker.
6. Confirm `/autoscaler/info` reports `authority_mode: sequenced`, a current
   allocation generation/hash/claim generation, and one exact-provenance pool
   record per authenticated edge. Confirm the same nested object is propagated
   through service status when target replica counts are requested.
7. For an existing service already spanning two reserved contexts, confirm
   observations for both contexts overlap in wall time and new rows remain
   pinned to their authorizing context/physical UID. Do not deploy a synthetic
   service to manufacture this evidence.
8. Confirm every new fill replica remains on a configured zero-cost location
   and no ordinary paid cloud request was created by the fill path.
9. Passively inspect the inference LocalQueue, its active ClusterQueue and
   namespace selector, shared BCL/research preemption policy, effective
   workload priorities, managed inference Workload evidence, and fail-closed
   admission policy; confirm this rollout did not mutate them. Verify from
   logs/metrics that the unique policy authorized each new sequenced claim and
   launch under the same identity, without launching a synthetic workload.
10. Confirm service version convergence and ordinary request handling remain
    healthy. A missing map should stop only new fill, not fail the service.

## Verification plan and evidence

### Required automated commands

```bash
uv run --no-sync pytest -q \
  tests/unit_tests/test_pool_capacity_observation.py \
  tests/unit_tests/test_pool_capacity_observer.py \
  tests/unit_tests/test_reserved_fill_planner.py \
  tests/unit_tests/test_reserved_fill_manager_receipt.py \
  tests/unit_tests/test_reserved_fill_autoscaler_adapter.py \
  tests/unit_tests/test_reserved_fill_status.py \
  tests/unit_tests/test_reserved_fill_reclaim_attestation.py \
  tests/unit_tests/test_reserved_fill_reclaim_policy_unit.py \
  tests/unit_tests/test_reserved_fill_execution_fence.py \
  tests/unit_tests/test_reserved_fill_reconciliation_transition.py \
  tests/unit_tests/test_serve_platform_projection.py \
  tests/unit_tests/kubernetes/test_provision.py \
  tests/unit_tests/test_sky/provision/test_provision_cluster_incarnation.py \
  tests/unit_tests/test_sky/provision/test_provisioner_pause.py \
  tests/unit_tests/test_sky/test_failover_classification.py \
  tests/unit_tests/test_backend_utils.py \
  tests/unit_tests/test_sky/clouds/test_kubernetes.py \
  tests/unit_tests/test_serve_scale_reconciliation.py \
  tests/unit_tests/test_serve_controller.py \
  tests/unit_tests/test_serve_controller_event_loop.py \
  tests/unit_tests/test_reserved_capacity_fill.py \
  tests/unit_tests/test_reserved_fill_broker.py \
  tests/unit_tests/test_serve_cleanup_recovery_script_order.py \
  tests/unit_tests/test_serve_ordinary_launch_binding.py \
  tests/unit_tests/test_serve_replica_api.py \
  tests/unit_tests/test_serve_replica_managers.py \
  tests/unit_tests/test_serve_replica_record_contract.py \
  tests/unit_tests/test_serve_utils.py \
  tests/unit_tests/test_interrupt_request_for_retry.py \
  tests/unit_tests/test_sky/server/requests/test_executor.py \
  tests/unit_tests/test_sky/server/requests/test_process.py \
  tests/unit_tests/test_api_requests_postgres_schema.py \
  tests/unit_tests/test_orphaned_inflight_requests.py \
  tests/unit_tests/test_server_request_recovery.py \
  tests/unit_tests/test_sky/server/requests/test_internal_daemon_submission.py \
  tests/unit_tests/test_sky/server/test_daemons.py \
  tests/unit_tests/test_sky/server/test_runtime.py \
  tests/unit_tests/test_sky/server/test_runtime_daemons.py \
  tests/unit_tests/test_batch_recovery.py \
  tests/unit_tests/test_jobs_utils.py \
  tests/unit_tests/test_managed_job_controller_restart_race.py \
  tests/unit_tests/test_sky/jobs/test_scheduler.py \
  tests/unit_tests/test_sky/jobs/test_controller.py \
  tests/unit_tests/test_sky/jobs/test_controller_attempt_fencing.py \
  tests/unit_tests/test_sky/jobs/test_controller_ownership.py \
  tests/unit_tests/test_sky/jobs/test_controller_slots.py \
  tests/unit_tests/test_sky/jobs/test_jobs_state.py \
  tests/unit_tests/test_sky/jobs/test_managed_job_refresh_thread.py \
  tests/unit_tests/test_sky/client/test_service_account_auth.py \
  tests/unit_tests/test_sky/utils/test_controller_capability.py

SKYPILOT_TEST_POSTGRES_URL=postgresql:///postgres \
  uv run --no-sync pytest -q -n 0 \
  tests/unit_tests/test_api_requests_pg.py \
  tests/unit_tests/test_batch_recovery_pg.py \
  tests/unit_tests/test_pool_capacity_observation_pg.py \
  tests/unit_tests/test_reserved_fill_allocation_pg.py \
  tests/unit_tests/test_reserved_fill_broker_pg.py \
  tests/unit_tests/test_reserved_fill_terminal_fence_pg.py \
  tests/unit_tests/test_reserved_fill_multi_pool_state.py \
  tests/unit_tests/test_serve_ordinary_launch_handoff_schema_041_pg.py \
  tests/unit_tests/test_serve_placement_normalization_schema_040_pg.py \
  tests/unit_tests/test_serve_resource_action_schema_033_pg.py \
  tests/unit_tests/test_serve_resource_action_schema_038_pg.py \
  tests/unit_tests/test_serve_resource_action_schema_039_pg.py \
  tests/unit_tests/test_serve_resource_action_state_pg.py \
  tests/unit_tests/test_serve_resource_actions_pg.py \
  tests/unit_tests/test_serve_system_recovery_persistence_pg.py
```

PostgreSQL tests must run against real PostgreSQL with repository-default xdist
explicitly disabled (`-n 0`); parallel schema migration fixtures can otherwise
exhaust a small server's shared lock table without exercising feature
correctness. Formatting, typing, lint, and diff integrity must also pass for
every changed file.

The automated policy tests cover zero and multiple entry points, malformed or
stale typed evidence, identity mismatch, a claim change between external proof
and locked persistence, an activation change between attestation and CAS,
missing policy after executor restart, partial/forged launch fences, and a
policy mismatch immediately before provider mutation. They also prove that
all policy/provider reads occur outside broker, service-authority, and database
locks and that `LEGACY_ACTIVE` retains its bounded compatibility behavior.
Kubernetes tests cover fresh authorization for normal/AppArmor/409 create
attempts and rejection cleanup, immediate create-response attestation,
guard-free passive `provision_timeout: -1` waits, terminal cancellation, and
durable-owner cleanup instead of opaque request teardown. PostgreSQL activation
tests prove exact v17 queued authority and reject v16, stale service versions,
and stale projection digests. Request tests cover exact outer-guardian
receipts, pre-effect pidless claims, ambiguous RUNNING legacy claims, PID reuse
and pidfd signalling, abrupt-boundary deferral, terminal result preservation,
and the absence of any PID-death receipt shortcut. They cover ordinary and
provider-reserved claimed `PENDING`/`WAITING` cancellation and
dispatcher-no-Future races; and prove terminal boundary-receipt closure for
`NEVER`, `READ_ONLY`, and `RECONCILE`, including a bound ordinary launch,
without reopening terminal work. They also inject PostgreSQL loss before the
parent receipt write, prove the parent monitor retries to durable convergence,
cover transported callable and result-serialization exceptions as typed
outcomes, and prove shutdown joins all receipt monitors. Disposable-boundary
tests kill the inner warden while a `setsid()` grandchild is live, kill the
outer guardian while the inner family is live, and race cancellation against
handler return; no Future or receipt becomes visible until the
surviving owner drains the exact family and the handler root is safely reaped.
Capability qualification proves fresh-FD delivery for every manager, daemon
restart, and verified managed-origin handler; no grant for an origin-less
controller-class request; non-dumpability before raw authority or plugin
access; absence of the bearer from environment and argv; exec/fork fail-closed
behavior; exact scoped cleanup; and pre-admission cancellation without a
stranded transfer-handle duplicate.
They also prove that Pod deletion,
replacement, lease age, and signal delivery do not synthesize quiescence.
Runtime-daemon tests additionally cover retirement before generic re-enqueue,
no daemon handler registration or queue delivery, exact selected-daemon
inventory, isolated child restart, parent-death setup, clean environment and
system context initialization, bounded `SIGTERM`/`SIGKILL` shutdown, and child
and grandchild group reaping before graceful controller leadership release.
Shutdown tests prove every request guardian is explicitly receipt-complete and
reaped/absent (including a simulated kill/join timeout), and make an incomplete
background/supervisor join fail closed before leadership or instance-lease
release. Real-PostgreSQL tests
seed queued, claimed, and terminal legacy daemon rows and prove that only those
rows are retired under the current controller generation while ordinary
requests survive.
Managed-job slot tests cover fixed eager slot birth, empty-queue polling,
transactional four-field claim publication, exact-slot state and nested-action
fencing, local slot crash and complete family drain before exact-attempt reset,
replacement-attempt rotation, stale outer-generation recovery after whole-Pod
loss, graceful refresh/slot/request drain ordering, and the absence of any
PID-file, request-triggered controller spawn, or shared-PID decoder.

### Evidence recorded so far

- Historical live diagnosis: repeated UID-fenced 34-slot A100 publications,
  with 181--250-second age at autoscaler consumption.
- Implementation tests exist for concurrent observation, pool-local blackout,
  generation-fenced activation/reauthorization CAS, ordinary zero-cost
  sequencing, map authentication and
  current-round revalidation, deterministic planning, same-map replay debit,
  parallel multi-context preflight, exact sparse receipt, lost wakeups, and
  sequenced-controller selection. Status tests cover exact durable-provenance
  joins, legacy/unavailable shapes, optional diagnostic failure,
  endpoint fail-closed fallback, and service-status propagation.
- Added tests prove same-tick `target=None` ordinary-demand debit, one shared
  multi-context preflight deadline, release of observer capacity after a
  timed-out Kubernetes query, access-context alias consumption with preserved
  UID/context fences, and alias de-duplication without physical-pool double
  counting.
- Real-PostgreSQL regressions prove a service-B ordinary row invalidates
  service A's published map and rejects its stale fill transaction with no row,
  while a broker-partitioned peer fill advances only total row attribution and
  remains valid input to a later observation at the unchanged ordinary
  high-water.
- Additional real-PostgreSQL regressions prove protocol-first zero-cost writes
  do not form a sequencer/service crossed-lock deadlock; a pre-observation
  unbound ordinary nonclaimant is debited; a launch materializing during an
  observation remains debited; a pre-observation materialization is not
  double-debited; and post-observation admission is ordered correctly even
  when its application timestamp is older. A grouped replica decode failure
  rejects sequenced occupancy instead of publishing optimistic capacity.
- Historical pre-projection-v2 validation on 2026-08-12 passed its focused
  policy, Serve045, broker, non-PostgreSQL, format, mypy, pylint, dashboard, and
  Prettier checks. Those counts do not certify the current Serve046 worktree.
- The final 2026-08-13 non-PostgreSQL matrix passed all 3,298 tests across the
  50 documented files in 83 seconds after formatting. Its first run exposed
  and the implementation corrected one backend integration regression: the
  historical optional planner/DAG input must remain optional for a successful
  reserved-fill Kubernetes adoption while the post-materialization authority
  guard is carried forward. The focused regression and complete rerun pass.
- Changed-source pylint passes at 10.00/10 and `git diff --check` is clean.
  Changed-source mypy has only the same pre-existing `backend_utils.py`
  overload diagnostic present on `origin/improvements`; the repository mypy
  target reaches six unrelated baseline/environment diagnostics in unchanged
  files. No feature-owned typing diagnostic remains.
- On the exact corrected behavior tree
  `688521ffd6cce0838b55c98fbb1196584116fc70`, Terraform 1.15.8 validates both
  changed spoke modules. The EKS module passes all 43 tests and the RBAC module
  passes all 20 tests from their explicit `terraform-tests` directories.
- The final serial real-PostgreSQL matrix passed all 618 tests across the 15
  documented files with zero failures, errors, or skips. Repository-default
  xdist was disabled; four ordered chunks each exited zero, with an aggregate
  wall time of 1,252 seconds (20m52s) and aggregate JUnit test time of
  1,221.896 seconds. The exact code revision was
  `688521ffd6cce0838b55c98fbb1196584116fc70`; the four retained JUnit artifacts
  are `/tmp/feature-pg-chunk1-688521ffd.R57qNi.xml` (206 tests),
  `/tmp/feature-pg-chunk2-688521ffd.HiL1jE.xml` (211 tests),
  `/tmp/feature-pg-chunk3-688521ffd.MoLBSj.xml` (80 tests), and
  `/tmp/feature-pg-chunk4-688521ffd.Ax9kzJ.xml` (121 tests). Process audits
  proved one pytest owner throughout each chunk and no PostgreSQL pytest
  remained after the freeze.
- No merge commit, deployment revision, activation result, live GPU fill, or
  BCL preemption result is claimed in this document yet.

### Adversarial review record

The implementation is not merge-ready until three consecutive reviews pass
against the exact current file and code. A finding that changes behavior must
update this file before the next round.

| Round | Revision reviewed | Result | Material findings/fixes |
|---|---|---|---|
| 1 | pending | pending | pending |
| 2 | pending | pending | pending |
| 3 | pending | pending | pending |

Reviews should be pragmatic and fix-forward oriented. They must reject an
oversubscription, stale-authority, duplicate-happy-path, paid-spill, or BCL
priority regression, but should not require a canary or a general rollback
system.

## Transitional code and stacked removal path

The feature PR is stacked with a cleanup PR before the feature is marked
ready. The stack is:

1. Feature PR [#1451](https://github.com/boltz-bio/skypilot/pull/1451),
   `feat/serve-event-driven-reserved-fill`: additive observation ledger,
   authenticated allocation, sequenced planner/receipt, activation and active
   reauthorization, compatibility path, and transition tests.
2. Draft cleanup PR [#1452](https://github.com/boltz-bio/skypilot/pull/1452),
   `cleanup/serve-sequenced-reserved-fill`: final steady state with the
   standalone protocol activation/demotion and legacy launch paths deleted.
   It retains `reserved_fill_reconciliation_transition status/activate` as the
   sole first-authorization and reauthorization surface and remains blocked
   until the merge gate below is met.

Exact feature and cleanup commit IDs are recorded after the final feature
rebase and cleanup restack. PR #1452 must be restacked onto the frozen/squashed
feature, reauthored for Serve046/projection v2 and the controller-runtime
transition removals, and pass its final-state tests before either OID is
recorded. The stale cleanup commits are design input only and must not be
blindly rebased or cherry-picked. Historical cleanup PR #1263 is not this
correction's removal PR.

The cleanup uses a new forward-only Serve047 migration; it never edits or
renumbers historical Serve044, Serve045, or Serve046. Serve047 preserves the
Serve045 reclaim receipt/generation and every Serve046 service-version and
projection-digest column and constraint. It replaces the Serve045 gate check,
default, and `ENABLE ALWAYS` trigger with the protocol-v2-only final two-state
domain: `UNAUTHORIZED` with a completely null authorization receipt, or
`SEQUENCED_ACTIVE` with a complete Serve045 receipt. Under the migration lock,
a well-formed null-receipt
`LEGACY_ACTIVE` bootstrap row becomes `UNAUTHORIZED` without changing its
generation; a valid active row and receipt are preserved byte-for-byte; a
partial or malformed shape aborts migration. Thus a fresh database is inert,
and a migrated but not-yet-authorized database has no legacy actuator.
`UNAUTHORIZED` permits ordinary reconciliation but suppresses every
reserved-fill provider observation, allocation, and launch effect. It still
maintains provider-free protocol-v2 service claims and immutable worker
projections, so the canonical command has a complete current claim scope to
attest on first authorization. Before the protocol-v1 decoder is removed,
migration mechanically rejects any active protocol-v1 worker projection. The
same canonical command authorizes `UNAUTHORIZED` to
`SEQUENCED_ACTIVE` at exactly `generation + 1` and reauthorizes
`SEQUENCED_ACTIVE` after a fix-forward rollout; cleanup does not introduce a
second bootstrap actuator.

The cleanup change removes, rather than perpetuates:

- the `LEGACY_ACTIVE` provider-query branch in protocol-v2 broker cycles;
- legacy fill launch emission and emission-time feed/rotation spending from
  `_apply_reserved_capacity_fill_v2()`;
- direct-call poller compatibility that lacks an actuation-generation fence
  and therefore holds the old broad lock;
- new-admission tolerance for an unattributed protocol-v2 fill tuple after the
  legacy fleet has drained;
- the worker-projection protocol-v1 ordinary-launch decoder and its transition
  tests after every active version and launch has remained on protocol v2 for
  the removal horizon;
- obsolete autoscaler/manager fill signaling methods superseded by
  `ScaleReconcileCoordinator`; and
- the nullable pre-slot managed-job adoption decoder, queries, and transition
  tests after its fleet horizon; and
- `LEGACY_REQUEST_DAEMON_IDS`, the legacy `daemon:` supported-handler census
  and transition lock, row-retirement methods and startup calls, retired
  daemon pickle symbols, and their transition tests after their stale-writer
  horizon. The fixed-slot managed-job supervisors and runtime-daemon
  subprocess supervisors remain. Historical controller PID inventory and
  cutover helpers are already deleted by the feature and are not claimed again
  by cleanup.

Cleanup explicitly retains the canonical `status` plus
authorization/reauthorization command, its `_read_stable_writer_rollout`
attestation, and `get` access to ReplicaSets. Pod -> ReplicaSet -> Deployment
identity is part of every first authorization and fix-forward reauthorization,
not transition-only code. Read-only historical row decoding may remain only
where durable terminal data still requires it; the protocol-v1 ordinary worker
projection decoder does not remain after its gate passes.

The cleanup PR's exact merge gate is:

1. the feature PR is merged and the full split-role fleet is running its image;
2. production reports `SEQUENCED_ACTIVE` and cannot demote;
3. at least three consecutive observation periods complete without a stale
   writer overwriting a successor, a map-authentication failure caused by
   current code, or a receipt-accounting error;
4. at least one controller restart and one ordinary service update complete on
   the sequenced path;
5. every nonterminal protocol-v2 fill row is fully attributed, or every
   remaining unattributed legacy row has become terminal and stayed absent for
   one complete 180-second authority horizon;
6. no new fill row appears without a positive database-assigned admission
   sequence after activation;
7. ordinary traffic, no-paid-spill, `max_replicas`, two-context concurrency,
   and the deployment-owned Kueue reclaim contract pass; and
8. every active service version uses worker projection protocol v2 and no
   ordinary launch has consumed the v1 decoder for one complete 180-second
   authority horizon; and
9. after one complete controller-fleet rollout, no non-`INACTIVE`, non-`DONE`
   managed-job row has a null or partial slot identity; separately, for one
   complete controller-instance stale window, no allowlisted daemon ID exists
   in request, queue, or retention state and no live/recent writer advertises
   a legacy `daemon:` handler; and
10. the cleanup branch's final-state tests pass with the compatibility code
    physically absent, while fresh-install `UNAUTHORIZED` authorization and
    active fix-forward reauthorization both pass through the same command and
    require the stable Pod -> ReplicaSet -> Deployment owner chain. Tests also
    prove chart RBAC retains `get` on ReplicaSets, the protocol-v1 worker
    projection decoder is physically absent, and schema-5 allocations plus
    v17 replica attribution still round-trip and fence correctly.

This gate is intentionally short and fix-forward compatible. It proves the
old path is no longer needed without imposing a 24-hour soak or a GPU/BCL
canary. If it fails, fix the feature or cleanup branch forward; do not reopen
legacy activation.

## Open gates

- Freeze the reviewed documentation-only evidence commit over exact behavior
  revision `688521ffd6cce0838b55c98fbb1196584116fc70`.
- Complete and record three consecutive adversarial review passes.
- Restack cleanup PR #1452 as the Serve047 successor over the frozen Serve046
  feature, including protocol-v1 projection-decoder removal and the enumerated
  managed-job/daemon transition removals, then record both OIDs.
- Merge the feature stack.
- Build and deploy one immutable image to the complete split-role fleet with a
  reviewed direct Helm upgrade using `--reuse-values`; verify the rendered
  change and live rollout preserve all existing release values and persistent
  resources.
- Record the pre-activation writer/image/schema proof.
- Deploy and attest the separately owned Kueue inference queue/preemption
  contract for every reserved context; current east1 evidence does not pass.
- Ship its code-owned `ReservedFillReclaimPolicy` and ongoing claim-admission
  and launch fence in a second immutable Boltz deployment bundle. The policy
  must consume and authorize the projected `scheduler_name` and nullable Pod
  Identity role together with the existing namespace, service-account,
  priority, Kueue, and accelerator fields. For the identity-free inference
  partition it must positively prove that no Pod Identity association exists;
  null is not permission to skip the check.
  Converge the full split-role fleet on that digest and repeat the
  pre-activation proof; no generic assertion bypass exists.
- Perform initial activation and non-compute manual verification; later policy
  or writer changes use the same command for one successor generation.
- After the documented removal gate passes, merge the cleanup PR.

Until those gates are recorded, this document must not describe the correction
as merged, deployed, activated, or proven by live capacity.
