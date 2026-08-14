# Multi-pool SkyServe reserved-capacity fill

Status: Serve046 merged in source PR #1451. ReplicaInfo v18 and its one-shot
normalizer then merged in PR #1483 at
`df71f6cff011a74ddce2c23245629a6b83d306cf` (tree
`e47880cf96d1df0611b8ae03be8148b8ed9f8e67`) and were published successfully
as `v1.1.1277`. That published precursor is historical evidence only and is
activation-ineligible: it predates the capacity, generated-Service annotation,
and audit-target contracts in this design and the corresponding CI-isolation
fix. Activation successor A is the single current source successor. It
subsumes that v18-only precursor and adds all three pre-activation contracts;
it is not yet merged, published, or deployed. Exact-only publisher B and final
Serve047 cleanup C are pending. The prior review record is historical. A
requires independent security/contract review and CI before merge; three
consecutive pragmatic adversarial rounds are required later against the frozen
final A/B/C and platform heads before final deployment/completion. Platform PRs
14, 17, 18, and 19 remain unapplied, reserved-fill activation has not occurred,
and cleanup remains blocked on the documented live receipt gates.

Last updated: 2026-08-14 (activation-successor pre-activation contracts)

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

The cross-repository rollout has one ordered fix-forward stack:

1. Activation successor A is the final release produced by the old publisher.
   It carries ReplicaInfo v18 and its one-shot normalizer together with the
   authenticated queue-capacity, generic generated-Service annotation, and
   audit-target contracts below. Platform PR 14 pins A's one immutable source,
   image, chart, and module tuple, migrates the live one-pod `all` topology to
   an exact 2/2/2 `api`/`controller`/`executor` cohort, physically deletes
   `all`, and proves all six Ready writers run that tuple.
2. Platform PR 17 invokes A's one-shot normalization against the unchanged PR
   14 tuple and archives its receipt. It performs no image, chart, module,
   values, topology, Pod, or Helm-revision change.
3. Only after that receipt, independently releasable publisher B establishes
   the sole exact source image/chart publication contract and physically
   deletes superseded moving-tag, overlay, and parallel chart publication
   paths. Its first run must fail closed without publishing because the
   canonical role does not exist yet. Platform PR 18 then adopts and hardens
   the Rainier runtime and Como chart registries and publisher roles in
   separate account applies/readbacks; it creates no Helm revision.
4. Only after both PR 18 readbacks, final cleanup C drops the pickle column and
   physically removes the temporary v17 runtime reader, normalizer, one-pod/
   `all` source controls, and every other transition-only executable path. The
   canonical roles publish C's immutable Serve047 image/chart tuple. Platform
   PR 19 alone pins that tuple, upgrades the already-split release in place,
   enables typed storage, and physically removes the remaining platform and
   publisher transition paths.

The last successfully published precursor is source/tag commit
`df71f6cff011a74ddce2c23245629a6b83d306cf`, Git tag `v1.1.1277`, source tree
`e47880cf96d1df0611b8ae03be8148b8ed9f8e67`, runtime image
`255203429798.dkr.ecr.us-east-1.amazonaws.com/skypilot-nightly-boltz:1.1.1277@sha256:a84ec11d8838b5367c4669bd5c4792ef96b735a8503ba2f2de6bf054459e3470`,
and chart
`oci://699626303757.dkr.ecr.us-east-1.amazonaws.com/helm-charts/skypilot:1.1.1277@sha256:4efdeb85a46dbcfcea1269d9a75f15891a83161741381e12fcb8b70a74329561`.
Its chart tree is `e8828c247` and module tree is `39e3108c65`. Publication
succeeded, but this tuple is not a PR 14 candidate and must not be activated:
it lacks A's three pre-activation contracts and did not include the CI-isolation
fix. PR 14 must bind fresh live Helm values and an exact render to A's later
immutable publication receipt. Revision-389 release 1.1.1273 at source commit
`c24ae4fe08a03101180c8401a34a1b241444116b` is historical audit evidence only,
never an operational checkpoint, split target, intermediate apply, or fallback.

## Activation successor A pre-activation contracts

A is one artifact, not a second v18 precursor. It inherits the already-merged
v18 writer and normalizer and adds the contracts that PR 14 needs before it can
both split and roll the fleet. These contracts are deliberately independent of
reserved-fill activation: they can be rendered, started, and read back while
the generation gate remains closed.

### Authenticated request-queue capacity

The authenticated `/_lb/capacity` response is the canonical admission contract
for the platform client. When `load_balancer.request_queue` is configured it
always includes:

| Field | Contract |
|---|---|
| `request_queue_capacity` | Dynamic waiting capacity derived from the current ready/logical fleet, bounded by configured minimum and maximum. |
| `request_queue_dispatch_limit` | Dynamic backend dispatch concurrency; zero while no usable backend capacity exists. |
| `request_queue_submission_limit` | Capacity-insensitive controller HTTP concurrency, exactly `max_size + max_concurrency`. This lets a cold service accept its configured backlog before its first worker is Ready. |
| `request_queue_min_size` | Immutable configured minimum waiting capacity. |
| `request_queue_size_per_replica` | Immutable configured waiting capacity per ready/logical replica unit. |
| `request_queue_max_size` | Immutable configured waiting-capacity ceiling. |
| `request_queue_max_concurrency` | Immutable configured active-dispatch ceiling. |
| `request_queue_max_request_body_bytes` | Immutable configured per-request body ceiling. |
| `request_queue_timeout_seconds` | Immutable configured queue-wait timeout. |
| `request_queue_uses_async_occupancy` | Immutable configured occupancy mode. |

The three admission fields are zero on a non-armed/non-active LB slot; the
immutable echoes remain present so a reader can diagnose role mismatch without
mistaking it for a different service contract. All queue fields are `null` only
when the queue is disabled. Presence and exact JSON types form the compatibility
boundary; this localized response extension does not add another schema/version
switch or alternate endpoint.

The PR 14 cold-start contract is exactly `min_size: 200`,
`size_per_replica: 10`, `max_size: 2000`, `max_concurrency: 128`,
`max_request_body_bytes: 1048576`, `timeout_seconds: 3600`, and
`use_async_occupancy: true`. Before any worker is Ready, the response therefore
reports queue capacity `200`, dispatch limit `0`, and submission limit `2128`.

Timeout ownership is intentionally layered rather than duplicated:

| Owner | Setting | Seconds | Invariant |
|---|---|---:|---|
| Platform node-local model/router render | `--request-timeout-seconds` | 315 | Returns before SkyServe's upstream stream deadline. |
| SkyPilot source/service spec | SkyServe LB `stream_timeout_seconds` | 330 | Covers the 315-second model/router request with margin. |
| Platform outbound SkyServe client config | request timeout | 3960 | Covers the 3600-second queue wait plus 330-second stream window and margin. |
| Platform generated-Service annotation | NLB TCP listener idle timeout | 4000 | Strictly exceeds the 3960-second client deadline. |

Source owns and supports the 330-second LB setting and the queue contract.
Platform owns the 315-, 3960-, and 4000-second rendered values. Neither side
silently derives or rewrites the other's values.

### Generic generated-Service annotations

The only operator input is the exact string map
`serve.externalLoadBalancer.serviceAnnotations`. Helm validates only that the
value is an object with string keys and values, serializes it deterministically
as JSON, and projects the same reserved environment variable into every
`api`, `controller`, and `executor` Pod. Python is the sole semantic authority:
startup and every reconciliation reject malformed Kubernetes annotation keys,
duplicate JSON keys, non-string values, and conflicts with SkyPilot-owned
`skypilot.co/` keys or the exact third-party-domain TLS, DNS, and backend-
protocol keys managed by SkyPilot.

Every generated inference `Service` receives the map. SkyPilot records only
those operator-owned keys in the canonical durable annotation
`skypilot.co/serve-lb-operator-annotation-keys`. On update it sets current owned
keys and emits strategic-merge `null` only for retired keys in that ledger;
unrelated annotations injected by AWS Load Balancer Controller, ExternalDNS,
or another provider/controller remain untouched. A malformed ledger fails
closed. During A's bounded PR 14-to-C transition, a missing ledger is accepted
as a bootstrap and is interpreted as owning zero existing keys. A cannot prove
from a markerless Kubernetes object whether it predates A or lost its ledger
after A, so it deliberately preserves every unmarked key in either case. A
post-A missing ledger is therefore observable drift that fails the PR 14/C
receipt gates even though A can still repair the marker without deleting an
unknown provider-owned key.

PR 14 must reconcile and read back every live generated inference Service with
a canonical ledger. That receipt is cleanup C's exact removal gate: Serve047
must physically remove markerless acceptance and reject a missing ledger while
retaining the ledger and narrow merge behavior as the permanent single path.
The cleanup absence tests must prove no `require_marker=False` call or
missing-marker bootstrap remains and that a missing live ledger fails closed;
this is not a TODO or optional soak gate.

The interface is provider-neutral. The platform's current AWS contract uses:

```yaml
serve:
  externalLoadBalancer:
    serviceAnnotations:
      service.beta.kubernetes.io/aws-load-balancer-listener-attributes.TCP-30001: tcp.idle_timeout.seconds=4000
```

The key shape and TCP idle-timeout range follow the official
[AWS Load Balancer Controller Service annotation contract](https://kubernetes-sigs.github.io/aws-load-balancer-controller/latest/guide/service/annotations/)
and [AWS Network Load Balancer listener behavior](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-listeners.html).

### Audit targets independent of Kueue object creation

`reserved_fill_reclaim_audit` is an audit identity and exact read target, not
the owner of partition Kueue objects. Its required
`local_queue_name` and `inference_cluster_queue_name` fields are therefore
available even when the selected `partition.kueue` is `null`. Terraform can
stage the audit role, EKS access entry, and exact Kubernetes RBAC before a
separately owned Kueue rollout. Once that partition enables Kueue, a lifecycle
precondition requires exact equality with its `local_queue_name` and
`cluster_queue_name`; there is no alias, fallback, inferred default, or second
source of queue identity.

This decoupling changes neither ordinary workspace placement nor its
credentials. The existing ordinary `wa` and `skypilot-wa` paths remain
unchanged. A's module tests must prove both the Kueue-null staging state and the
later exact-match rejection boundary.

Only frozen historical Alembic replay code remains after C; runtime code does
not import it. There is no rollback branch, legacy publisher, topology
re-creation, or second Helm/runtime happy path to preserve.

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
The deployment policy enforces the same atom invariant through one shared
claim-set validator used by both activation and every later claim replacement:
it accepts multiple edges sharing an access context, attests that context once,
and rejects only a second claim on the same
`(physical_cluster_uid, exact_card)` atom before provider calls.
The typed admission for that atom also carries the normalized accelerator
scheduling tuple: label key, sorted label values, and extended-resource key.
Activation, every claim-set replacement, and every terminal launch must match
that tuple exactly against a disjoint code-owned card contract. A logical card
cannot own a flavor or scheduling tuple already owned by another logical card.

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

Replica state version 18 persists the complete typed fill attribution. The
precursor's narrowly bounded v17 reader exists only to normalize the two exact
observed v17 shapes described in the deployment gate below:

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

### Boltz deployment policy bundle

Boltz implements this interface in the separate
`boltz-skypilot-reserved-fill-reclaim-policy` distribution under
`boltz/reserved_fill_reclaim_policy/`. The generic SkyPilot wheel remains
entry-point-free. The Boltz overlay builds and installs the generic wheel and
the deployment-policy wheel independently, then verifies that the combined
image exposes exactly one `skypilot.reserved_fill_reclaim_policy` entry point.
The overlay release version stamps the policy revision; any policy-code change
therefore rotates durable policy identity even if the fleet JSON is unchanged.

The package embeds one strict JSON fleet bundle. Unknown or duplicate keys are
rejected. Its normalized semantic sections are hashed independently with
domain-separated SHA-256 prefixes: the admission/reclaim contract produces
`fleet_bundle_sha256`, while physical/provider inventory produces
`provider_inventory_sha256`. Reordering contexts, flavors, or quota rows does
not rotate either identity. The package caches only temporary AssumeRole
credentials; it never caches Kubernetes or AWS attestation results.

The initial public inference contract is service-name-agnostic. A second
service, including one with traffic weight 1000, uses the same claim and launch
path if its immutable projection matches this bundle. No service allowlist or
second scheduling path exists. The exact shared object contract is:

| Contract | East | PHX |
|---|---|---|
| Context | `prod_research_cluster_eks` | `phx_research_cluster_eks` |
| Physical cluster UID | `14de98b4-cb7b-4f82-beb7-6f754a96f1dd` | `ba2dcdca-2a0d-447f-ad8a-31849a63c1d5` |
| Namespace / service account | `rescluster-k8s-prod-east1-preemptible-inference` / `skypilot-pool-sa` | same |
| Pod Identity | absent | absent |
| LocalQueue (spoke-module-owned) | `default` | `default` |
| ClusterQueue (Kueue-chart-owned) | `skyserve-inference-borrowed` | same |
| WorkloadPriorityClass (Kueue-chart-owned) | `skyserve-inference-low` | same |
| Pod PriorityClass (spoke-module-owned) | `rescluster-k8s-prod-east1-preemptible-inference-low`, value -1000, `Never` | same |
| Scheduler | `gpu-binpack-scheduler` | same |
| GPU resource | `nvidia.com/gpu` | `nvidia.com/gpu` |
| Exact worker accelerator scheduling | `A100`: `nvidia.com/gpu.product=NVIDIA-A100-SXM4-40GB`, `nvidia.com/gpu`; `A100-80GB`: `nvidia.com/gpu.product=NVIDIA-A100-SXM4-80GB`, `nvidia.com/gpu` | `H200`: `nvidia.com/gpu.product=NVIDIA-H200`, `nvidia.com/gpu` |

The inference ClusterQueue has zero nominal GPU quota in the same cohort as
the research queue. Its per-flavor borrowing limits are bounded by research's
nominal GPU quota, and it has no preemption permission of its own. The research
queue must retain `reclaimWithinCohort: Any`. The policy proves the exact
LocalQueue target, both current Active ClusterQueues, cohort, namespace
selectors, GPU flavor quotas, preemption policies, provider-owned
ResourceFlavor instance selectors, WorkloadPriorityClass, Pod PriorityClass,
immutable custom scheduler deployment, current Kueue controller, Pod
integration,
`AssignQueueLabelsForPods: true`, and the Deny queue-name admission-policy
binding. It does not infer Pod admission from a webhook configuration name:
for both the mutating and validating configurations it requires exactly one
named Pod webhook with the reviewed core/v1 Pod operations, Kueue service
name/namespace/path/port, nonempty CA bundle, admission review version,
selectors, side effects, timeout, match and failure policies, plus the
mutating reinvocation policy. Any missing rule or endpoint drift fails
activation and every later policy check. HyperPod remains the sole owner of its
ResourceFlavors: the attestor proves each flavor's exact provider-owned
instance selector, then cross-binds that selector to the GPU product label and
`nvidia.com/gpu` capacity on the current physical cluster's Nodes. It requires
at least one non-deleting Node for every reviewed flavor and rejects every
non-deleting Node of that shape whose product or GPU capacity differs. Node
readiness and allocatable occupancy remain physical-observation inputs, so a
temporarily initializing Node is not misclassified as policy drift. The
inference namespace UID and physical cluster UID are immutable inventory;
replace either only by shipping a new bundle and normal fix-forward
reauthorization.

AWS absence is a positive proof, not an omitted check. For each context the
plugin uses the hub writer's Pod Identity session to assume the exact spoke
roles `skypilot-rf-b6ca6363ec70-audit` in east and
`skypilot-rf-fe7c6c421c88-audit` in PHX. The spoke module derives these
collision-resistant identities from the exact cluster and partition. Each
role is read-only and limited
to `eks:DescribeCluster`, `eks:ListPodIdentityAssociations`, and
`eks:DescribePodIdentityAssociation` on the exact cluster and its association
resources. The spoke trust names the single current hub Pod Identity writer
role, permits only `sts:AssumeRole` plus the required `sts:TagSession`, and
requires its transitive `eks-cluster-arn`, `kubernetes-namespace`, and
`kubernetes-service-account` session tags. The chart renders API, controller,
and executor writers with the same `skypilot-api-sa`; a chart test must keep
that invariant true. Every proof describes the exact active EKS cluster,
paginates the filtered association index with cycle detection, and requires
zero associations for the public inference service account. A non-null future
bundle instead requires exactly one summary plus one exact described
association, including role, null target role, ARN, and owner agreement. The
Kubernetes proof independently rejects an IRSA annotation on the service
account.

Activation runs both provider domains for both contexts concurrently under the
caller's single absolute five-second deadline. Claim authorization does the
same for every distinct requested context, and launch authorization runs AWS
and Kubernetes concurrently for its one context. Static identity, pool-key,
projection, accelerator, and admission mismatches fail before provider I/O.
All network calls use the remaining deadline, one-attempt client retries, and a
shared cancellation event. Raw provider payloads and credential material never
enter errors or proof output.

The deployment preflight is machine-readable:

```bash
python -m boltz_reserved_fill_reclaim_policy
```

It prints exactly one JSON object. Successful preflight and the structured
activation, claim, and launch log payloads use schema 1 with `operation`,
`success`, the V2 `contract`, all three identity fields, completion time, and
one record per attested context. Each AWS record includes
`association_count`, `expected_role_arn`, and the explicit boolean
`identity_absence_proven`; each Kubernetes record includes the physical and
namespace UIDs, exact queue names, IRSA-absence result,
`assign_queue_labels_for_pods`, and per-flavor non-deleting Node counts plus
reviewed product/capacity. Failed CLI preflight returns exit 1 and only
`{"schema_version":1,"operation":"preflight","success":false,
"error_code":"ATTESTATION_FAILED"}`. Runtime activation and authorization
fail closed with `ReclaimAttestationError`.

Rollout is fix-forward. Apply and attest the IAM, namespace/service-account,
queue, priority, admission-policy, Kueue configuration, and server projection
first; remove the east unmanaged inference Pods and its drifted Pod Identity
association before activation. Then build one immutable two-wheel Boltz image,
deploy it to the complete writer fleet, run the JSON preflight, and invoke the
normal activation command. A correction ships a successor bundle/image and
uses the same reauthorization command to advance the generation; it does not
reopen legacy activation or introduce a rollback-only happy path.

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
ReplicaInfo v18 record whose scalar columns agree, service version equals the
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
  binds service version and the closed digest map; ReplicaInfo v18 persists the
  selected scalar digest. Activation successor A reads only the two exact
  historical
  v17 shapes long enough to produce the required normalization receipt;
  historical v15 and worktree-only v16 are not live formats.
- Concurrent provider-free pool observer and typed blackouts.
- Committed-observation broker rounds and complete authenticated maps.
- Ordinary zero-cost commit sequencing and complete v18 attribution.
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
  projection v18 replica rows.
- An exact v18 queued-effect proof at first activation and per-mutation
  Kubernetes guards with immediate create-response attestation, guard-free
  passive waits, and durable-owner cleanup after a terminal fence.

The Serve046 base above merged in source PR #1451, and the v18 live contract
plus normalizer merged in PR #1483. Activation successor A is this local
successor: it retains that v18 code and adds the queue-capacity,
generated-Service annotation, and audit-target prerequisites. A is not merged,
published, or deployed, and reserved fill is not activated. The validation and
three-round review records below describe earlier exact Serve046 revisions and
remain historical evidence only. They do not satisfy A's independent merge
review or the three final reviews of the frozen integrated A/B/C/platform
stack. Remote CI on each exact successor must pass before that successor
merges; its immutable publication receipt is captured only after merge.

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
    claims, schema-5 maps, v18 rows, activation, status, and terminal launch,
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
    retry-until-proven-absent ownership handoff; and
22. makes deployment attestation use the same physical-UID/card atom as the
    broker so east's A100-40 and A100-80 edges may share one context without
    duplicating capacity, binds activation and later claim replacement to one
    shared atom validator, and binds both Kueue Pod webhooks to their exact
    reviewed rules and service endpoints rather than trusting object names;
    and
23. carries the normalized accelerator label key, sorted label values, and
    resource key in every typed reclaim admission, requires their exact
    code-owned match at activation, claim, and terminal launch, and makes the
    east A100-40 and A100-80 bundle contracts disjoint.

Regression tests exist for corrections 1--23, including owner-death,
request-liveness, legacy-row retirement, runtime-daemon supervision,
scheduler/bound-Node admission, capability bootstrap, and fixed managed-job
slot ownership. The complete non-PostgreSQL matrix, changed-source lint, and
Terraform module tests pass. The exact serial PostgreSQL freeze and corrected
deployment-policy matrix pass, and all three restarted consecutive adversarial
reviews passed without findings.

The audit also proved that BCL reclaim is a deployment prerequisite rather
than a Pod-priority property. A read-only production audit on 2026-08-13 found
that both east and PHX lack the inference `default` LocalQueue,
`skyserve-inference-borrowed` ClusterQueue, and
`skyserve-inference-low` WorkloadPriorityClass. Both already have the exact
Pod PriorityClass at value -1000 with `preemptionPolicy: Never`, and both
research ClusterQueues are Active with `reclaimWithinCohort: Any`. East's
research GPU quotas are nominal 64/264 with borrowing limits 0/0; PHX is
nominal 512 with borrowing limit 512. The inference service accounts have no
IRSA annotation. East has one Pod Identity association,
`a-rsvzwdtaesxvxorkh` to `research-dropzone-irsa`, but it belongs to the
separate research namespace/service account; PHX has no association. The
plugin filters and paginates by the exact projected inference namespace and
service account, so the unrelated east research identity is preserved while
the inference identity-free absence proof still requires zero exact matches.

HyperPod's live ResourceFlavors are provider-owned and intentionally omit GPU
product labels. East exposes 8 non-deleting `ml.p4d.24xlarge` Nodes with 8
A100-40GB GPUs each and 33 `ml.p4de.24xlarge` Nodes with 8 A100-80GB GPUs each;
PHX exposes 64 `ml.p5e.48xlarge` Nodes with 8 H200 GPUs each. The east
ResourceFlavor selectors are the live beta/stable/HyperPod labels for p4d and
beta/HyperPod labels for p4de; PHX has beta/stable/HyperPod labels for p5e.
The queue-name Deny policy and binding are absent in east and present in PHX;
Kueue webhooks fail closed in both. A direct 2026-08-13 east read additionally
confirmed that the new exact mutating and validating Pod-webhook contracts
match the live v0.18 objects; PHX must pass the same code-owned v0.19 contract
through deployment preflight before activation. The current hub writer is forbidden from
reading the required Namespace, ServiceAccount, queue, priority, flavor, Node,
scheduler, controller, and admission objects in both spokes. These are exact
platform IAM/RBAC and object gates; SkyPilot core does not duplicate their
ownership.

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
| 1 | Observation ledger, admission sequence, authenticated map, coordinator, pure planner, manager receipt, diagnostics, and Serve045/046 reclaim-policy identity | Merged in source PR #1451. Its prior freeze/reviews are historical evidence, not a pass for the current stack. |
| 1b | PR #1483 precursor: replica state v18 plus its one-shot normalizer | Merged and published as 1.1.1277, but activation-ineligible because it lacks A's pre-activation contracts. |
| 2a | Activation successor A and platform PR 14: publish/pin A, migrate one-pod `all` directly to exact split 2/2/2 on A, and delete `all` | A is local and PR 14 is not applied; both are blocked on A review, CI, publication, and exact tuple binding. |
| 2b | Platform PR 17: invoke normalization and accept its receipt on the unchanged PR 14 A tuple | Blocked until PR 14's exact split-A rollout/readback passes; PR 17 creates no rollout or Helm revision. |
| 2c | Publisher B and platform PR 18: exact-only source publication, expected pre-adoption no-publish, then separate registry/role adoption and readbacks with no Helm revision | Not merged or applied. |
| 2d | Deployment-owned Kueue bundle, unique entry-point policy, and generation-fenced authorization | Not activated; the live IAM/RBAC/Kueue attestation gates remain open. |
| 3 | Source cleanup C and platform PR 19: publish/pin Serve047, upgrade the already-split release, enable typed storage, and physically delete transition paths | C is being restacked and is unmerged/unpublished; PR 19 is blocked on C's immutable publication receipt and final absence gates. |

Durable acceptance hands rows to the existing asynchronous launch path, and
status projects the same allocation/observation evidence used by
reconciliation; neither is a second source of launch authority.

## Deployment, activation, and fix-forward reauthorization

### Preconditions

Before changing the gate:

1. Activation successor A must pass its independent security/contract review,
   CI, exact Python/Helm/Terraform suites, and deterministic generated-file
   checks before merge. After PR 17's unchanged-A normalization receipt, B/PR
   18, and C's publication, freeze every final source and platform head and run
   three consecutive pragmatic adversarial rounds before PR 19's final
   deployment/completion. Any material change resets that final sequence.
2. Merge and publish A through the old publisher as that publisher's final
   release. Amend platform PR 14 with one reviewed source, version, runtime
   digest, chart digest, module pin, API version, and structural proof. The
   historical 1.1.1277 tuple is forbidden here.
3. During PR 14, apply API-request schema 010 and the managed-job slot columns
   before A Pods become Ready. In the same Helm revision, convert one-pod `all`
   directly to exact 2/2/2 `api`/`controller`/`executor` on A and physically
   delete `all`. Let ordinary controller leader handoff drain old
   claims/processes; capture live values, render, Pods, and writer leases.
4. PR 14 must prove exactly six Ready same-digest A writers and no old writer;
   API capability 77, API-request 010, Serve 046, projection protocol 2,
   allocation schema 5, replica state 18, and reserved-fill protocol v2; the
   exact authenticated cold queue-capacity response; the identical annotation
   projection on all three roles; canonical ownership ledgers on every live
   generated inference Service; and the audit module's explicit queue targets.
   Authority remains `LEGACY_ACTIVE` and PR 14 does not run normalization.
5. Platform PR 17 runs A's normalization command and archives its accepted
   exact receipt while the PR 14 source, image, chart, module, values, Pods,
   topology, and Helm revision remain unchanged. It is an operation against A,
   not an A deployment or a second release.
6. Only after that receipt, merge publisher B. Its first automatic run must
   fail closed without publishing while the canonical role is absent. Platform
   PR 18 then adopts and hardens the Rainier runtime and Como chart
   registries/roles in separate account applies and readbacks. PR 18 creates no
   Helm revision and does not deploy A or change topology.
7. Deploy and attest the separately owned Kueue inference contract and the
   unique policy-bearing A image, converge the full split-role fleet on that
   image, and repeat the pre-activation proof. The generic A image cannot
   authorize activation.
8. Perform A's initial generation-fenced activation and non-compute manual
   verification. Keep A deployed until every cleanup-C runtime/removal horizon
   below passes, including `SEQUENCED_ACTIVE`, one controller restart, and one
   ordinary service update on the sequenced path.
9. Only after both PR 18 account readbacks and all cleanup-C runtime gates pass,
   merge cleanup C. C physically removes the normalizer, v17 live decoder,
   pickle column, and source topology/transition controls, while requiring B's
   superseded publisher paths to remain absent. The canonical roles publish one
   immutable C image/chart tuple and receipt.
10. Amend platform PR 19 with that exact C tuple and publication receipt. PR 19
    upgrades the already-split release in place, enables the typed worker-cache
    contract, and physically removes platform publisher/storage/topology
    transition paths. It must not create or replace the split topology. Invoke
    the same generation-fenced command to reauthorize the C fleet; no rollback
    or second activation path exists.
11. Prove every active reserved-fill service version has exact protocol-v2
   worker projections and every queued PENDING/PROVISIONING fill row is exact
   v18 bound to its locked service version, projection digest, and successor
   policy tuple. Drain any stale or undecodable row; no legacy fallback is
   permitted.
12. Prove the deployment-owned Kueue LocalQueue, ClusterQueue namespace
   selection, shared preemption domain, workload priorities, strict SkyPilot
   configuration, RBAC, and fail-closed admission contract for every reserved
   inference context. Pod priority alone does not pass this gate. Launch no GPU
   or BCL verification workload for this rollout.
13. Prove every split-role Pod runs the one immutable C image and the unique
    Boltz policy entry point resolves from its separately packaged wheel before
    C reauthorization. Later defects replace the complete fleet with a new
    immutable tuple and use that same generation-fenced command; no rollback or
    second policy/image path exists.

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
is not smuggled into the runtime-image Helm upgrade and does not block
deploying the combined image safely at `LEGACY_ACTIVE`; it does block
activation.

### ReplicaInfo v18 expand-and-normalize gate

Production exposed a v17 label collision: retained rows were labelled current
while some omitted the 13 sequenced-attribution keys introduced during the
Serve046 development sequence. Generic legacy materialization would conceal
that collision and could invent authority. PR #1483 therefore moved the writer
to v18, and activation successor A carries that writer and normalizer together
with its other pre-activation contracts. Runtime accepts exact v18, while
the transitional v17 reader accepts exactly two closed v17 shapes: complete
v17, or the one observed v17 collision shape with all 13 known collision
fields absent. The collision shape becomes 13 explicit `null` values; a
complete v17 value is preserved exactly. A partially missing attribution
bundle, unknown field, a
missing non-collision field, an incomplete status subdocument, and every other
record version fail closed. A also stops all live pickle
dual-writes; the nullable column remains only until Serve047 drops it.

Platform PR 14 establishes the split topology directly on A and deletes `all`.
Only after exactly two `api`, two `controller`, and two `executor` Pods and
their six Pod-bound writer leases are Ready on the same immutable A digest,
with no old writer remaining, PR 17 runs the source-owned internal one-shot
operation from that exact unchanged API Deployment (replace `<namespace>` and
`<api-deployment>` with the reviewed live objects):

```bash
kubectl -n <namespace> exec deploy/<api-deployment> -c skypilot-api -- \
  python -m sky.serve.replica_record_normalization --json
```

There is deliberately no public API, SDK, CLI, feature flag, or alternate
normalization path. The operation proves the token-bound exact 2/2/2
API/controller/executor writer rollout before and during the cutover, requires
PostgreSQL at exact Serve schema revision 046, and takes the shared broker lock
plus an `ACCESS EXCLUSIVE` replicas-table lock. It validates every retained row
before the first update, including exact parity between JSON and every
JSON-derived query scalar (`status`, `sky_down_status`, `version`,
`cluster_name`, `created_at`, `is_spot`, and `paid_capacity_pool_key`). It does
not repair denormalized scalar authority. It rewrites all rows atomically
through the canonical serializer, preserves present
attribution with exact JSON types and values, materializes only absent
collision fields as null, and clears every legacy pickle. In the same atomic
boundary it installs an enforced check requiring non-null outer version 1,
non-null v18 JSON, and a null pickle column, so neither a v17 writer, a
SQL-NULL outer version, nor stale pickle repopulation can return. Constraint
validation is a separate, safely resumable transaction because replica updates
can leave deferred foreign-key trigger events; the already-enforced check
protects the gap. Controlled failures identify only an opaque row ordinal and a
controlled reason or exception class. One outer public boundary lets the lock
and transaction contexts finish cleanup, then converts every unexpected
operation, database, or transaction exception to one generic error with raw
exception chaining suppressed. No failure can therefore expose persisted
identifiers, payloads, credentials, driver SQL, or bound parameters.
`ReplicaInfo.from_storage_dict()` is a pure decoder with no logging side
effects, and `ReplicaInfo.status` is the single pure status projection. The
ordinary Serve row-read wrapper owns operational quarantine reporting and emits
an identifier-free warning when that canonical projection is `UNKNOWN`; the
normalizer calls the same pure decoder and projection directly. Quarantined
recovery fields or an `UNKNOWN` stored status therefore cannot leak a persisted
row identity while A validates retained state.

The single stdout line is the durable deployment receipt. It contains counts
and the immutable writer digest, never row payloads, credentials, or service
names. Platform evidence must retain the exact JSON and require all of these
invariants:

```json
{
  "already_current_records": 0,
  "constraint": "ck_replicas_replica_info_version_18",
  "contract": "skyserve.replica-info-v18-normalization/v1",
  "invalid_records": 0,
  "remaining_legacy_pickle_records": 0,
  "remaining_noncurrent_records": 0,
  "rewritten_records": 1,
  "scanned_records": 1,
  "scanned_services": 1,
  "schema_version": 18,
  "serve_database_revision": "046",
  "writer_deployment_roles": ["api", "controller", "executor"],
  "writer_image_digest": "sha256:<exact-A-digest>",
  "writer_pod_inventory_count": 6,
  "writer_pod_inventory_sha256": "<exact-inventory-sha256>",
  "writer_process_count": 6
}
```

The counts are live values, not expected constants, but
`scanned_records == rewritten_records + already_current_records`, both
`remaining_*` fields and `invalid_records` are zero,
`serve_database_revision` is `046`, the role list, Pod count, process count,
and inventory hash prove the exact 2/2/2 cohort, and `writer_image_digest`
equals the digest proven on every split writer role. A
failed or absent receipt blocks Serve047. Rerunning after an interrupted
validation is safe and produces an idempotent receipt. Serve047 then asserts
the exact v18/key shape, drops the pickle column, and physically deletes the
v17 runtime decoder and this normalization module; normalization is not a
permanent happy path. Historical Alembic replay retains only the
migration-owned, executable-global-allowlisted frozen pickle converter used by
revisions 010 (maximum v7) and 026 (maximum v11). No runtime module imports it,
and it cannot be called as a live compatibility path.

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
# Activation successor A pre-activation contracts.
uv run --no-sync pytest -q -n 0 \
  tests/unit_tests/test_serve_lb_k8s.py \
  tests/unit_tests/test_serve_request_queue.py \
  tests/unit_tests/test_reserved_fill_reclaim_policy_unit.py \
  tests/unit_tests/test_sky/utils/test_context.py

helm lint charts/skypilot
helm unittest charts/skypilot
helm schema -f charts/skypilot/values.yaml \
  -o /tmp/skypilot-values.schema.json
cmp /tmp/skypilot-values.schema.json charts/skypilot/values.schema.json

terraform -chdir=infra/terraform/modules/skypilot-spoke-workspace-pool-eks \
  fmt -check -recursive
terraform -chdir=infra/terraform/modules/skypilot-spoke-workspace-pool-eks \
  validate
terraform -chdir=infra/terraform/modules/skypilot-spoke-workspace-pool-eks \
  test -test-directory=terraform-tests

# Existing reserved-fill protocol regression set.
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
  tests/unit_tests/test_serve_state.py \
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
  tests/unit_tests/test_replica_record_normalization_pg.py \
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

The generated values schema is a required enforcement layer, not decorative
documentation: `serviceAnnotations.additionalProperties` must remain
`type: string`. The Helm helper independently rejects a non-map or non-string
entry before serializing deterministic JSON, and Python tests feed raw JSON
directly to the semantic boundary to reject numeric values, duplicate keys,
malformed keys, reserved-key conflicts, and malformed ownership ledgers. This
combination is the numeric-value evidence; a `helm-unittest --set` fixture is
not authoritative because that harness may coerce scalar input before the
template observes its original YAML type.

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
tests prove exact v18 queued authority and reject pre-normalization v17/v16,
stale service versions, and stale projection digests. Request tests cover exact
outer-guardian
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

- Activation successor A's pre-activation contract suite passes locally on the
  exact current worktree: 350/350 Python tests across external-LB Kubernetes
  lifecycle, request queue, reclaim-policy, and request-context isolation;
  Helm lint plus 21/21 suites and 305/305 tests; byte-identical regenerated
  values schema; an expected-negative schema lint rejecting an integer Service
  annotation; Terraform 1.14.8 init/validate/fmt plus 51/51 module tests; and
  `format.sh` mypy/pylint/dashboard checks. These are source/render tests, not
  PR 14 deployment, generated-Service readback, route materialization, live
  capacity, or BCL-preemption evidence.
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
- After the exact accelerator-scheduling correction, the final complete
  non-PostgreSQL rerun on code/design revision
  `123c16762aea510d34db74f52c0c27e733fbb07d` passed all 3,384 tests across the
  same documented 50-file matrix with zero failures or skips in 93.733
  seconds. The retained JUnit artifact is
  `/tmp/feature-nonpg-123c16762-rerun.xml`, SHA-256
  `c0afd4330fa7d5ef500f49e52d54abd3a8033c97c1b1c656360bf07e294d6092`.
- Changed-source pylint passes at 10.00/10 and `git diff --check` is clean.
  Changed-source mypy has only the same pre-existing `backend_utils.py`
  overload diagnostic present on `origin/improvements`; the repository mypy
  target reaches six unrelated baseline/environment diagnostics in unchanged
  files. No feature-owned typing diagnostic remains.
- On the exact corrected behavior tree
  `688521ffd6cce0838b55c98fbb1196584116fc70`, Terraform 1.15.8 validates both
  changed spoke modules. The final EKS module at audit-boundary revision
  `3af32dfdcd1ca9b27985e53e990d9f9efd256d58`, including its separate
  collision-resistant role, exact transitive-tag trust, derived queue grant,
  and clean invalid-partition failures, passes all 48 tests. The RBAC module
  passes all 20 tests from its explicit `terraform-tests` directory.
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
- The exact post-accelerator-correction serial real-PostgreSQL rerun on
  revision `123c16762aea510d34db74f52c0c27e733fbb07d` passed the same 618 tests
  with zero failures, errors, or skips in 1,345.917 seconds. It used one pytest
  process and the required real PostgreSQL server. The retained JUnit artifact
  is `/tmp/feature-pg-123c16762.xml`, SHA-256
  `19101b79676824822e55f4fdf9c9c2299ae8792944aa59e8c673bc934a0229ac`.
- The publication CI correction changes only static contracts and preserves
  the three-times-reviewed behavior: it replaces deprecated typing/import
  forms, freezes the already-narrowed observation repository in its callback,
  records existing dynamic factory and cookie contracts for the type checker,
  moves blocking runtime-daemon path setup to one worker-thread helper, and
  narrows the existing lifecycle-removal locator without changing its
  obligation. Exact Ruff, basedpyright, mypy, lifecycle-removal, formatting,
  import-order, changed-source pylint, and focused regression checks pass on
  the corrected tree. The callback freeze also removes a latent loop
  late-binding ambiguity while retaining the reviewed repository identity.
- On integrated implementation revision
  `244cc34fbfb61ba719691b33c92f93d039ef610f`, the corrected separate Boltz
  plugin and generic policy interface pass all 113 tests in the focused
  superset across policy, packaging,
  overlay-manifest, release-version, attestation, and generic-interface
  suites. Ruff, targeted mypy, changed-source pylint at 10.00/10, JSON parsing,
  Python compilation, formatting, and `git diff --check` also pass. The
  repository-wide mypy step reaches one unrelated baseline diagnostic in
  unchanged `sky/server/common.py`; targeted feature mypy is clean. New tests
  reject a
  missing or mismatched Node inventory, selector/product/capacity drift, and a
  deleting-only flavor while accepting a non-Ready initializing Node.
- On correction revision `70cb55a2fb003a4cd9665c5f3118c2b923a1f6ea`, the
  integrated policy/interface superset passes 121/121. New tests accept the two
  east exact-card edges in one context, reject a duplicate physical UID/card
  atom before provider calls, and reject missing Pod rules or drift in webhook
  operations, endpoint, CA bundle, and namespace selection. Ruff and targeted
  mypy pass, pylint is 10.00/10, JSON and Python compilation pass, and the exact
  live east mutating and validating objects pass the same validator.
- On behavior revision `f4a8aa8d003f256e5c2b621ca29461d75f84fdcd`,
  activation and later claim replacement call the same exact-card atom
  validator. The integrated policy/interface superset passes 123/123; new
  activation tests accept east's distinct A100-40/A100-80 claims in one
  context and reject a duplicated physical UID/card atom before provider
  calls. Ruff and targeted mypy pass, pylint is 10.00/10, formatting and
  `git diff --check` pass.
- On correction revision `bfbd6cbe0a9f22487d035f9149ac673ca4dacd95`,
  the fix after failed review round 1 carries the exact Kubernetes
  accelerator scheduling atom through the typed admission and rejects drift in
  its label key, label values, or resource key before provider calls at claim,
  activation, and launch. The strict bundle rejects cross-card flavor or
  scheduling overlap, and its east A100 contract now owns only the 40GB
  product. The integrated policy/interface superset passes 132/132; Ruff and
  targeted mypy pass, pylint is 10.00/10, JSON parsing, compilation,
  formatting, and `git diff --check` pass.
- A historical post-correction local Serve047 implementation restack at
  `1efa6b284` passed 58/58
  focused final-state, cleanup-presence, manager-receipt,
  reconciliation-transition, and status tests; the combined policy superset
  passes 134/134; and its required real-PostgreSQL Serve047 schema suite passes
  12/12 with zero skips against the isolated local PostgreSQL server.
  These results describe a superseded cleanup tree. They do not validate or
  freeze current cleanup C, whose exact replacement revision and tests remain
  open.
- No A, B, or C merge/publication, platform PR 14/17/18/19 apply, activation
  result, live GPU fill, or BCL preemption result is claimed in this document.

### Historical adversarial review record

The final integrated A/B/C/platform stack has zero passing final review rounds.
Three new consecutive pragmatic reviews run only after every final head is
frozen and before final deployment/completion; any material change resets the
sequence. A's earlier merge gate is the independent security/contract review
and CI described above, not a separate three-round sequence. The table below is
retained only as historical Serve046 evidence and no row counts toward the new
sequence.

One non-counting review attempt on feature `b93db03fb` and cleanup
`1094b9ded` failed. It found that the deployment policy rejected valid
same-context exact-card edges and that webhook attestation trusted names
without proving Pod admission rules or the Kueue endpoint. Revision
`70cb55a2f` fixes both findings and adds the fail-closed coverage above. The
consecutive sequence therefore restarts from round 1.

The first restarted round on feature `9baca9dca` and cleanup `cae4abd87`
also failed and does not count. It found that the policy ticket bound only the
logical accelerator name and count, while terminal launch used an accelerator
label/resource tuple absent from that ticket. East's bundle also let logical
`a100` name both 40GB and 80GB products. The correction above binds the exact
scheduling atom end to end and makes logical card contracts disjoint. The
consecutive sequence restarts from round 1 again.

| Round | Revision reviewed | Result | Material findings/fixes |
|---|---|---|---|
| 1 | feature/design `123c16762aea510d34db74f52c0c27e733fbb07d`; stacked Serve047 cleanup `bc2725c54149d14bc4e90edb2df24af5efccd789` | pass | No material or non-material findings. The review traced the exact normalized accelerator label key, sorted values, and resource through projection, activation, claim replacement, durable receipt/scope hashing, terminal authorization, rendering, Pod admission/adoption, and bound-Node proof. It also found no oversubscription, stale-authority, paid-spill, duplicate-happy-path, or BCL reclaim regression, and confirmed Serve047 leaves one forward-only two-state authorization path. |
| 2 | feature/design `a0fe24207854cdc3f98a4d2a879cc9dce4bfa0f7`; stacked Serve047 cleanup `175e04e8376d8507c9d08428f2f2a34516df8b2e`; design SHA-256 `b6037bab7e8de936aa5d447b7547f7ea2faf012395bfb36ccc1eb8006cecf486` | pass | No material or non-material findings. Independent review reverified the terminal PostgreSQL admission ledger, exact projection-to-Node accelerator scheduling atom, disjoint physical-card contracts, fail-closed zero-cost launch, live-attested bounded Kueue borrowing and BCL/research reclaim, and Serve047's sole forward authorization path. All non-design blobs remained byte-identical to round 1. |
| 3 | feature/design `cea111a5ddcf7f84e7426d75920e23cae7d33b65`; stacked Serve047 cleanup `c4a46c2debe54a832916cd64408c8306a50dc266`; feature design SHA-256 `fb9a88be168ac3a951a51c053f054a86d97ad5f363504c005a4c1be10bd2d398`; cleanup design SHA-256 `793fc1b5000f3d2ec46da066798645c9e36fc703f4faade9ef4fbc35bc4a89e5` | pass | No material or non-material findings. Final independent review reconfirmed bounded generation-fenced observation, complete terminal admission revalidation, exact-card zero-cost-only launch, projection and deployment-policy identity, zero-nominal inference borrowing with research reclaim, and Serve047's two-state forward-only authority. Every non-design blob remained byte-identical to round 2. |

Reviews should be pragmatic and fix-forward oriented. They must reject an
oversubscription, stale-authority, duplicate-happy-path, paid-spill, or BCL
priority regression, but should not require a canary or a general rollback
system.

## Transitional code and stacked removal path

Source PR #1451 is the already-merged Serve046 base, not an open transition
PR. The current source stack is:

1. activation successor A, targeting `improvements`: the already-merged v18
   live reader/writer and one-shot normalizer plus the queue-capacity,
   generated-Service annotation, and audit-target contracts required for
   platform PR 14's direct split-and-roll;
2. independently releasable publisher B: the exact-only publication contract
   and physical deletion of superseded publisher paths, with no runtime or
   schema behavior change; and
3. final cleanup C: forward-only Serve047 plus physical deletion of every live
   compatibility and transition path. C retains
   `reserved_fill_reconciliation_transition status/activate` as the sole
   first-authorization and reauthorization surface.

Platform PR 14 alone pins and deploys A while creating the split topology; PR
17 normalizes retained rows on that unchanged tuple and creates no Helm
revision; PR 18 follows B and also creates no Helm revision; PR 19 alone pins
and deploys C. The remote draft cleanup PR
[#1452](https://github.com/boltz-bio/skypilot/pull/1452) is a stale predecessor,
not current C or merge/deployment evidence. It must be replaced or updated to
the exact reviewed C revision. Historical cleanup PR #1263 is unrelated.

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

- the `replica_info` pickle column, live v17 reader, one-shot v18 normalizer,
  its command/tests, and every transition-only receipt consumer after the exact
  archived receipt passes; frozen revisions 010/026 migration replay is the
  only historical pickle code retained;
- the one-pod/all runtime role and corresponding Helm values, templates,
  conditionals, tests, and operator knobs after the exact split-role receipt;
- transition-only acceptance of a generated inference Service without
  `skypilot.co/serve-lb-operator-annotation-keys`, after PR 14 proves every
  live Service has been reconciled to a canonical ledger; the permanent narrow
  ownership ledger and strategic-merge behavior remain;
- no superseded image/chart publisher workflow, overlay builder, moving tag,
  or release fallback; B must physically delete those source paths before C is
  eligible, C's final absence tests prevent their reintroduction, and PR 19
  later deletes the retired external publisher identity;
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

Cleanup C's merge/publish gate and platform PR 19's later apply gate are
distinct. Before evaluating the runtime gates below, the archived PR 17
normalization receipt for the unchanged A tuple must prove Serve revision 046,
v18, zero
pickle/noncurrent rows, exact 2/2/2 Pods and writer leases, and one immutable
image digest. Publisher B must be merged, its expected pre-adoption run must
have failed closed without publishing, and both platform PR 18 account applies
and readbacks must have passed without creating a Helm revision. Missing
evidence blocks C; it never permits a legacy path.

After the runtime gates pass, C may merge and the canonical roles publish its
immutable Serve047 image/chart tuple. PR 19 must not merge, plan, or apply until
that exact publication receipt and all final platform absence gates pass. PR 19
then pins C and upgrades the existing split topology in place; it does not
create or replace that topology.

1. source PR #1451 and the v18 precursor PR #1483 are merged, PR 14's exact
   split-A receipt is accepted, and PR 17's normalization receipt names the
   unchanged full split-role A fleet;
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
    exact v18 replica attribution still round-trip and fence correctly.

This gate is intentionally short and fix-forward compatible. It proves the
old path is no longer needed without imposing a 24-hour soak or a GPU/BCL
canary. If it fails, fix the feature or cleanup branch forward; do not reopen
legacy activation.

## Open gates

- Complete A's independent security/contract review, exact validation, and CI;
  merge and publish A through the old publisher, then bind that exact tuple to
  platform PR 14. The published 1.1.1277 precursor is not eligible.
- Apply platform PR 14 once to convert one-pod `all` directly to exact split
  2/2/2 on A and delete `all`; archive its live Helm/render/lease proof, exact
  cold queue-capacity response, all-role annotation projection, canonical
  generated-Service ownership ledgers, and exact audit-target proof.
- Apply PR 17 only as the one-shot normalization operation on PR 14's unchanged
  A tuple; archive the normalization receipt and prove no Helm revision, Pod
  rollout, values change, or second release occurred.
- Merge B only after that receipt. Prove B's expected no-publish run, then
  apply and read back PR 18's separate canonical registry/role adoptions; PR 18
  must create no Helm revision.
- Record the pre-activation writer/image/schema proof.
- Deploy and attest the separately owned Kueue inference queue/preemption
  contract for every reserved context; current east1 evidence does not pass.
- Deploy the combined immutable image containing the code-owned
  `ReservedFillReclaimPolicy` from the separately packaged
  `boltz-skypilot-reserved-fill-reclaim-policy` wheel and its ongoing
  claim-admission and launch fence. The policy
  must consume and authorize the projected `scheduler_name` and nullable Pod
  Identity role together with the existing namespace, service-account,
  priority, Kueue, and accelerator fields. For the identity-free inference
  partition it must positively prove that no Pod Identity association exists;
  null is not permission to skip the check.
  Converge the full split-role fleet on that one digest and repeat the
  pre-activation proof; no generic assertion bypass or generic-only image path
  exists.
- Perform initial activation of A and non-compute manual verification through
  the one generation-fenced command. Keep A live until every cleanup-C runtime
  gate and removal horizon passes; later fixes use the same command.
- Only then complete C, merge it, and archive its canonical immutable
  image/chart publication receipt. Amend PR 19 with that exact tuple, freeze
  every final A/B/C/platform head, then pass three consecutive pragmatic
  adversarial rounds; any material change resets the sequence.
- Pass every final absence gate, then apply PR 19 to upgrade the already-split
  release in place, delete all remaining transition paths, and reauthorize C
  through the same generation-fenced command.

Until those gates are recorded, this document must not describe A, B, C, or
platform PR 14/17/18/19 as merged, published, deployed, activated, or proven by
live capacity.
