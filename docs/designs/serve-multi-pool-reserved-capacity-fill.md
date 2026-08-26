# SkyServe multi-pool reserved-capacity fill

Last updated: 2026-08-26

Status: **in progress**. The single PostgreSQL-authoritative reserved-capacity
path has already proved full East occupancy and reclaim. The immediate active
gate is now the independent paid-provider lifecycle proof: at least 100
physical one-L4 GCP Spot VMs concurrently `RUNNING`, followed by exact VM,
managed-boot-disk, and PostgreSQL teardown. The source contract for PHX uses
its existing externally owned Kueue lane without changing scheduler policy,
but that candidate is not yet present in the live service definition. The
later 10,000-terminal-request qualification remains a separate end-to-end
gate.

This file is the canonical living design. It describes the current contract,
the latest production evidence, and only the gates that remain. Historical
incident chronology is intentionally left to Git.

## Decision

Run one heterogeneous SkyServe service with one capacity authority:

1. Observe compatible zero-cost Kubernetes capacity without mutating the
   scheduler.
2. Commit and debit scheduler-permitted reserved capacity in PostgreSQL.
3. Compute paid residual only after that reserved commitment is visible.
4. Admit only bounded L4 Spot for the residual.
5. Make every provider effect conditional on the exact committed PostgreSQL
   graph that authorized it.

The live production service currently uses East A100 and A100-80GB as
zero-cost reserved capacity and AWS/GCP L4 Spot as demand-only residual. The
target service additionally uses PHX H200 only through the existing
`boltz-research/be -> research-be` Kueue lane. SkyPilot submits at workload
priority `be-lt` and Pod priority -1000, treats Kueue admission as the final
spendable-capacity authority, and does not create or modify scheduler policy.

The control plane is PostgreSQL-only. EFS, a PVC, KubeRay, Terraform,
Terragrunt, and `boltz-platform` application state are not part of this design.

## Current state

The distinction between source, deployment, activation, and proof is
intentional. A source-complete behavior is not production-proven merely because
it has merged or been deployed.

| Layer | Current state |
|---|---|
| Source base | `origin/improvements` at `201fbe3c0684dd22ef27f306a3d17013e42d0fdd` (PR #1729). A focused fix-forward candidate adds exact retained-request GCP project/zone/name recovery plus combined VM-and-managed-boot-disk reconciliation for ordinary paid launches; merge, immutable-image publication, and production proof remain pending. |
| Deployed control plane | SkyPilot `1.1.1498`, Helm revision 620. API, controller, and executor roles all use `255203429798.dkr.ecr.us-east-1.amazonaws.com/skypilot-nightly-boltz:1.1.1498@sha256:ccd49b4481ec5c99b0c3b390317c43c2812c6f6e635afbd7e4f1cefca861d4b7`. Helm storage remains disabled and the namespace has no PVC. |
| Writer protocol | Public API 93, worker projection 10, non-pool capability cohort 12, and async request-ledger protocol 1. |
| Storage | PostgreSQL is the sole central correctness store; Helm `storage.enabled=false`; no SkyPilot EFS or PVC. |
| Active service | Lifecycle 105, service incarnation `ca8f8349-3bdb-49c6-9557-416337b7feb1`, `SHUTTING_DOWN`. The first GCP-only qualification was stopped after exposing the missing GCP provider-evidence path; service recreation waits for evidence-backed row convergence. |
| Reserved occupancy | The 2026-08-25 23:30 UTC East census found all 328 healthy compatible physical GPUs allocated: research requested 156 and `boltz-l4-fleet` requested the remaining 172. PostgreSQL independently had 172/172 reserved replicas `READY`; compatible free capacity was therefore zero. At 23:38 UTC Kubernetes then recorded exactly 128 distinct SkyPilot Pod preemptions by 16 higher-priority research Pods of eight GPUs each. SkyPilot correctly yielded from 172 to 44 rather than blocking research. As ten short-lived preemptors exited, authenticated observations and allocations advanced in bounded waves from 44 to the exact compatible remainder of 132 (77 A100-80GB and 55 A100), reaching 132/132 `READY` at 2026-08-26 00:01:55 UTC with zero paid capacity. Together with the 188 research GPU requests, that occupied all 320 GPUs on the 40 then-healthy GPU nodes. A later exact 00:29 UTC census found 41 healthy GPU nodes and 328 allocatable GPUs, with 188 requested by `hyperpod-ns-research`, 140 running SkyPilot workers, and no pending SkyPilot worker; PostgreSQL independently reported the same 140 reserved replicas `READY`. Thus newly exposed capacity refilled automatically and all 328 GPUs were again occupied with zero paid capacity. This proves reclaim correctness and 100% eventual refill, while exposing a roughly 24-minute worst observed refill horizon that the provider-phase and bounded-admission work must reduce. |
| PHX | Excluded from the live service definition, so it currently contributes zero. The 2026-08-25 23:40 UTC physical census found 512 healthy schedulable H200 GPUs, 355 requested by research, and 157 physically free. The bounded SkyPilot identity deliberately cannot list other tenants' Kueue Workloads, so physical freedom is not treated as scheduler admission. Live server-mode readback of PostgreSQL config identity revision 4 resolved the exact `boltz-research/be` projection, priority and worker identity. A read-only cluster read at 2026-08-26 00:18 UTC confirmed the unchanged `research-be` ClusterQueue is active, has zero nominal H200 quota, may borrow at most 512 H200 GPUs from `shared-pool`, uses `BestEffortFIFO`, and permits only lower-priority reclaim/preemption; its current admission and reservation counts were zero. The existing `boltz-research/be -> research-be` lane is ready for service-only activation; each exact Kueue admission remains the final authority. Its unchanged reclaim policy is conditional, not a guarantee that every research class immediately reclaims every fill admission. |
| PHX access | The controller identity can exact-read the required namespace/queue and manage only worker Pod/Service lifecycle; it cannot list or patch ClusterQueues. The worker ServiceAccount is tokenless and cannot read Pods, queues, or secrets. A historical audit-only group still has an unused broad Kueue LIST grant from platform PR #8800; it is read-only, has no scheduling effect, and is not used or expanded by this rollout. |
| Paid state at idle | Provider-native GCP inventory is empty after safe exact cleanup: zero matching VMs and zero tracked boot disks, so no current provider billing remains. Thirty-seven lifecycle-105 replica rows are retained while `1.1.1498` fails closed on ten post-effect-ambiguous GCP launch failures; they must converge through the fix-forward observer rather than manual SQL deletion. |
| Routing and queue | Both load-balancer slots advertise ledger protocol 1 and the exact current incarnation. Direct PostgreSQL-backed `/serve/boltz-l4-fleet/demand` readback after refill reported telemetry `fresh/complete`, confirmed and aggregate in-flight 0, queue depth 0, recent requests 0, and an available protocol-1 async ledger with no active or terminal rows. The 2026-08-26 01:34 UTC read-only cross-source proof reported two fresh reporters and 140 logical slots. A fresh nonzero telemetry proof remains part of the final load run. |
| Partial mixed proof | Provider/DB censuses at 2026-08-25 19:45:47.538 and 19:45:56.281 UTC bracketed a 72-request completion wave and both had 44 reserved plus 28 paid replicas all `READY`, the same 28 AWS Spot instances—27 `g6.2xlarge` and one `g6.4xlarge`—and zero on-demand. The wave completed from 19:45:48.956 through 19:45:51.187; every request performed 9.533–12.451 seconds of concurrency-one GPU work, so at least 28 necessarily executed on Spot beside the 44 reserved workers. The Spot instances later fully drained at the provider. |
| GCP Spot lifecycle proof | Not complete. The first traffic-coupled attempt reached 27 concurrently `RUNNING` one-L4 `g2-standard-4` Spot VMs across exact GCP zones and zero on-demand. It was intentionally stopped: demand scaling created only a partial target and ten terminal launch failures lacked a GCP provider-absence contract. The replacement test uses a trivial task with a 100-replica floor, so application traffic and model startup cannot distort provider ramp. |
| Final load proof | Not complete. The prior run accepted 4,640 requests and observed 4,568 completion markers, but ended with 802 ambiguous submissions. Those identities must not be replayed as a substitute for a fresh exact-ledger run. |
| Scale-convoy correction | Source-qualified candidate only in `fix/serve-probe-scale-convoy-v3`; not merged, deployed, activated, or production-proven. It moves probe/provider I/O outside the manager lock, batches provider/status work, and commits bounded P/D admission before starting workers. Adversarial review caught and the candidate now fixes three release blockers: transient unresolved endpoints no longer masquerade as authoritative route-zero; a canonically cancelled pointerless `INTERRUPTED` launch can consume D only when every exact retained association proves settled execution quiescence; and bounded teardown bookkeeping is keyed by immutable `(replica_id, replica_record_id)` rather than the mutable record object. PostgreSQL reducer coverage includes queue and retention-pin removal, legacy pointerless rows without evidence still fail closed, and reserve/restore receipts cannot cross replica identity. |

The latest independent provider census was green at 2026-08-26 01:34 UTC: 140
reserved rows were `READY`; paid rows and GPU units, AWS/GCP instances, open
Spot requests, and tracked provider disks were all zero. Dashboard
rows such as `SHUTTING_DOWN` are controller lifecycle records, not provider
billing proof. Cost closure always requires the provider-native census plus
the PostgreSQL paid-authority census.

## Goals and acceptance criteria

### Reserved capacity

- Assign 100% of healthy, exact-card-compatible physical capacity exposed as
  free in East and 100% of the compatible capacity actually admitted by
  Simone's unchanged Kueue policy in PHX.
- Count every GPU on a multi-GPU machine once. One logical asynchronous worker
  owns one GPU; an eight-GPU node can therefore host eight workers.
- Treat the denominator as dynamic. When research releases a compatible slot,
  the next fresh observation makes it eligible for fill; when research
  or Kueue reclaims a slot, SkyPilot yields it and stops counting it as
  spendable. SkyPilot does not claim that Kueue will choose a victim outside
  the policy it actually implements.
- Never infer free capacity from nominal hardware totals, stale rows, or a
  pending workload that the scheduler has not admitted.

### Paid residual

- Commit reserved holdings, pending reserved admission, and allocation tail
  before computing the residual for paid capacity.
- Allow paid capacity only for authenticated demand not covered by compatible
  reserved capacity.
- Use L4 Spot only. Ordinary on-demand is forbidden.
- Enforce `max_live_paid_gpu_units` in logical GPU units across ready,
  provisioning, shutting-down, and cleanup-unproven paid rows.
- Select the cheapest currently eligible normalized Spot pool first, then use
  exact provider feedback to move to another eligible Spot pool. A static
  region order or stale UI price is not cost authority.

### Request execution and observability

- Execute a fresh campaign of 10,000 stable logical requests through the
  service-owned asynchronous queue.
- Recover an ambiguous HTTP outcome by looking up the same PostgreSQL ledger
  identity before replaying; never create a second live execution for the same
  intent.
- Count HTTP admission separately from processing, terminal completion, and
  scientific artifact verification.
- Show fresh queued, processing, in-flight, rejected, completed, and unknown
  counts in the service UI, with explicit completeness and freshness.

### Convergence and cleanup

- Admit large launch waves without serial provider, probe, drain, or teardown
  I/O holding the fleet manager lock.
- Reach the bounded 100-paid-GPU target from sustained excess demand without a
  fixed ten-replica launch prefix. Controller admission time and provider boot
  time are measured separately.
- Let the positive paid cap remain in place while demand falls to zero. Natural
  retirement, rather than lowering the cap, must remove paid capacity.
- Prove zero paid PostgreSQL authority and zero provider instances, Spot
  requests, and residual disks immediately, at +10 minutes, at +30 minutes,
  and through one complete stale/quiescence interval.

## Non-goals and ownership boundaries

- Do not create or modify Kueue queues, cohorts, flavors, quotas, fairness,
  priorities, or preemption. PHX uses the existing externally owned
  `boltz-research/be -> research-be` lane; SkyPilot creates no workaround lane
  and does not enlarge the capacity Kueue admits.
- Do not add Terraform, Terragrunt, KubeRay, HPTO, EFS/PV/PVC, or a
  `boltz-platform` runtime pin.
- Do not change `boltz-platform` application code. The request ledger,
  dispatch, and capacity transactions are internal SkyPilot control-plane
  behavior. The service definition may be operated from the allowed
  `ml_models/providers/skypilot/` boundary.
- Do not grant SkyPilot application-admin or cluster-admin permissions. The
  controller uses bounded server-owned namespace lifecycle permissions; an
  audit identity is read-only; workers cannot mutate Kubernetes policy.
- Do not add ordinary on-demand fallback or split services by accelerator,
  cloud, region, campaign, or revision. `use_spot: false` is valid here only
  for an exact fail-closed zero-cost Kubernetes pool.
- Do not make status or the dashboard call providers or become launch authority.
- Do not hide a lock convoy with a longer TTL, a larger timeout, an unbounded
  retry, a feature flag, or a second scheduler.
- Do not repair ambiguous state by manually deleting rows, Pods, Workloads, or
  provider resources without exact ownership and quiescence evidence.

## Production service contract

The existing public service-policy fields are sufficient. The live
qualification shape is equivalent to:

```yaml
resources:
  accelerators: {L4: 1}
  use_spot: true
  any_of:
    - infra: k8s/prod_research_cluster_eks
      accelerators: {A100-80GB: 1}
      use_spot: false
    - infra: k8s/prod_research_cluster_eks
      accelerators: {A100: 1}
      use_spot: false
    # Reviewed AWS and GCP L4 locations follow; all inherit use_spot: true.

service:
  replica_policy:
    min_replicas: 0
    max_live_paid_gpu_units: 100
    max_replicas: 1000
    reserved_capacity_fill:
      floor_replicas: 0
      weight: 100
      utilization_gate: false
```

`min_replicas: 0` and the paid cap make paid capacity demand-only and
scale-to-zero. They do not make reserved occupancy scale to zero.
`utilization_gate: false` deliberately fills free reserved GPUs even at zero
traffic because the accepted objective is 100% use of free reserved capacity.
This is an intentional departure from the general utilization-gated
scale-to-zero default. It never creates paid demand.

`floor_replicas: 0` is not a target of zero. It means there is no unconditional
minimum. The fresh scheduler-authorized grant remains the target when the
utilization gate is false.

Every reserved intent is pinned to the exact zero-cost Kubernetes location and
accelerator that authorized it. It cannot fall through to Spot. Paid Spot is a
separate residual path, so “no reserved spill” must not be misread as “the
service can never use Spot.”

The service advertises exact accelerator compatibility. A request keeps one
scientifically valid compatibility set independent of current availability;
the load balancer routes it only to a compatible ready worker.

## State and storage boundaries

### PostgreSQL control-plane authority

PostgreSQL owns current SkyServe correctness state:

- lifecycle, service version, owner incarnation, and capability epochs;
- physical-pool observations and authenticated reserved allocations;
- reserved intents, replicas, associations, API requests, queue rows, and
  retention pins;
- durable demand, route projections, paid plans, claims, and global paid
  debits;
- request-ledger admission, attempt, processing, and terminal status; and
- cleanup, quiescence, provider evidence, and retirement authority.

No local controller file, EFS object, cache, in-memory map, dashboard row, or
provider name inferred from a log can override this state.

### Object and worker storage

Object storage remains appropriate for immutable model bundles, request
payloads, result artifacts, and scientific completion markers. Those objects
do not replace the PostgreSQL capacity or request ledger. Signed request
capabilities are ephemeral and must not be written into durable control-plane
state or logs.

Worker caches and scratch are disposable, bounded, server-owned local storage.
They are not shared control-plane state. A worker restart may lose cache, but
must not lose a committed request or create a second dispatch.

## Capacity model

### Physical identity and units

The atomic reserved-pool identity is:

```text
(physical Kubernetes cluster UID, exact accelerator card)
```

The Kubernetes access context and worker width are carried separately. Context
aliases that prove the same physical UID/card cannot multiply capacity. A
context or card collision, inconsistent width, missing physical UID, or
unrecognized card fails closed.

Provider observations store raw exact-card GPU counts. The broker converts raw
GPUs to worker slots exactly once using the authenticated worker width. The
service ceiling and paid cap use logical GPU units for this fleet.

### Dynamic denominator

The reserved denominator is not the number of installed GPUs. It is the fresh
intersection of:

```text
healthy physical GPUs
∩ exact-card compatibility
∩ server-owned worker projection
∩ capacity currently available under the existing scheduler
```

East has no Kueue dependency. Its server-owned projection uses the existing
`gpu-binpack-scheduler` and
`rescluster-k8s-prod-east1-preemptible-inference-low` PriorityClass at numeric
priority -1000 with `preemptionPolicy: Never`. `Never` prevents a fill Pod from
preempting another Pod; higher-priority research can still evict the fill Pod.

PHX is outside the current live denominator only because the service YAML omits
it. Its target projection is server-owned and exact: Kubernetes context
`phx_research_cluster_eks`, namespace `boltz-research`, LocalQueue `be`,
ClusterQueue `research-be`, WorkloadPriorityClass `be-lt` at value 11,
ServiceAccount `skypilot-pool-sa`, Pod priority -1000 with
`preemptionPolicy: Never`, and one H200 per worker. The LocalQueue and
ClusterQueue are existing externally owned objects. SkyPilot validates them
read-only and never creates or mutates them.

A PHX physical-free observation can authorize an intent, but a waiting Kueue
Pod is not spendable capacity and cannot suppress paid residual. Only the exact
Pod UID whose Workload is admitted to `research-be` debits PHX capacity. Kueue
may queue or preempt that workload under its unchanged policy; SkyPilot then
refills or yields without interpreting nominal quota as an entitlement. The
2026-08-25 snapshot had 158 physically and quota-free H200s, but 158 is an
observation, not a fixed target or a new quota.

That unchanged policy has deliberate limits. MA and WA can nominate a
lower-priority `be-lt` fill victim while reclaiming nominal quota; HA has no
nominal GPU guarantee and relies on the configured fair-sharing decision; a
research BE workload at the same `be-lt` WorkloadPriorityClass cannot
Kueue-preempt another `be-lt` workload. Pod priority -1000 lets an already
admitted higher-priority research Pod win at kube-scheduler, but cannot itself
unblock Kueue admission. SkyPilot therefore consumes only exact admission,
revokes capacity on eviction, preemption, or lost admission, and launches
nothing on observer uncertainty. If the unchanged policy fails to reclaim in
a real research incident, the safe operation is to disable PHX fill, not add a
SkyPilot scheduler workaround.

### Observation freshness

Read-only pool observations run independently with bounded deadlines. A slow
or unavailable pool cannot serialize a healthy sibling. A successful result is
timestamped at observation start and expires after the conservative authority
horizon, currently 180 seconds. A newer blackout prevents fallback to an older
success.

Starting an observation consumes no capacity. Admission and first successful
materialization advance separate PostgreSQL sequences. These commit-order
sequences, not wall clocks or readiness guesses, prevent an observation from
double-spending a Pod created while the read was in flight.

## Reconciliation architecture

The one-way flow is:

```text
read-only scheduler/provider observation
    -> authenticated PostgreSQL pool snapshot
    -> broker allocation and reserved debit
    -> reserved replica/intent/request admission
    -> exact zero-cost provider effect

durable LB demand + immutable route projection
    -> target - committed/pending reserved - existing paid
    -> bounded paid residual plan
    -> paid claim/debit and launch binding
    -> exact Spot provider effect
```

There is no provider side effect on an arrow before the corresponding durable
commit.

### Reserved admission

Reserved fill uses one PostgreSQL transaction to:

1. lock the current protocol, service, allocation, and intent authority;
2. revalidate service ceiling, exact-card feed, projection digest, and owner;
3. allocate the global zero-cost admission sequence;
4. insert the replica and consume the durable intent/capacity debit; and
5. bind the generic non-pool association, API request, queue row, and retention
   pin.

The transaction returns exact replica, record, association, request, and launch
generation identities. Only then may the request executor adopt the launch.
A lost acknowledgement is reconciled from that receipt; it does not create a
second row or provider action.

Reserved launch resources contain one exact Kubernetes candidate. Retry
optimization and cloud failover are disabled for that execution capsule. If
the candidate or physical-UID fence is unavailable, the intent waits or fails
closed; it never becomes a paid launch.

### Paid residual and admission

The paid plan is computed from one locked, fresh graph:

```text
paid residual = max(
    0,
    authenticated demand target
    - committed reserved capacity
    - pending/allocation-reserved capacity
    - existing paid capacity)
```

The result is clipped by the elected service cap and all cleanup-unproven paid
rows. The plan carries exact demand, route, service, capacity-graph, reserved
allocation, accelerator, and pool identity.

The deployed paid path currently has two durable commits:

1. atomically insert the replica, paid claim, and global/pool debit; then
2. bind its association, API request, queue row, retention pin, and execution
   claim through the common non-pool request path.

The provider guard locks and exact-matches the complete graph from both commits
before the first provider effect. An interruption between them leaves inert,
recoverable committed state; it cannot launch. Later demand heartbeats may
change after the complete admission, but they cannot revoke or duplicate that
one already-authorized first provider effect.

This is not a literal one-transaction paid graph. The current correctness
contract is “no provider effect until the complete graph is durable.” If a
future requirement insists that replica, claim, association, request, queue,
pin, and execution claim share one database transaction, the paid path must be
generalized onto the reserved atomic-admission module. That refactor is not
implemented and is not required to describe the deployed safety boundary.

Every AWS paid association uses a stable provider idempotency token bound to
the immutable association and exact instance parameters. A lost provider
response is replayed with the same token. Only a typed provider rejection with
complete negative evidence may authorize absence and release the debit; a
timeout, unknown response, partial create, or lost acknowledgement remains
ambiguous and conserves the claim.

Every GCP paid association derives cleanup identity only from the retained
PostgreSQL launch graph: the paid-pool key freezes workspace, region, zone,
shape, Spot market, and node count; the immutable request snapshot freezes the
workspace's GCP project; and the association freezes the generated provider
cluster name. The observer never consults today's ambient workspace. After
exact executor quiescence, it reads that project and zone outside database row
locks and treats the allocation as absent only when the exact generated-name VM
set and SkyPilot-managed boot-disk set are empty, no attributable GCE insert
operation is non-`DONE`, and a second uncached census agrees. Cohort 12 stops
deleting timed-out Compute Operation metadata, because that API call never
cancels the underlying create and destroys reconciliation evidence. A complete
set of exact `DONE` child insert operations can settle immediately; otherwise a
300-second post-quiescence propagation horizon is also required. The new
cohort-12 controller may use this conservative contract to settle retained
cohort-11 rows, while old binaries cannot authorize it. The finite horizon is a
conservative mitigation for a missing terminal child-operation record, not the
formal request-ID/operation receipt proof required by the final steady state.
Presence grants only immediate fenced cleanup; it does not settle or release
the paid debit. Cleanup
idempotently deletes the exact resources, waits for VM disappearance and disk
detach/delete, and then requires fresh VM, disk, and create-operation absence
before PostgreSQL releases the claim and retention pin. A missing retained
request, mismatched project/workspace, unsupported disk identity, provider read
failure, stale controller authority, in-flight create, or incomplete
quiescence remains `UNKNOWN` and fails closed.

### Price and pool feedback

Placement compares current eligible Spot offerings in normalized cost units.
It first chooses the cheapest exact compatible pool that passes account,
region, zone, subnet, image, disk, and provider fences. A typed capacity or
quota rejection closes only that exact pool for a bounded period and permits
the next eligible Spot pool. It does not authorize on-demand capacity.

The dashboard price is diagnostic. The committed paid-pool key, decision-time
catalog evidence, and provider market establish what was selected and billed.

## Probe, launch, and cleanup concurrency

The fleet manager lock protects short in-memory reductions. It must never be a
lease for slow or blocking work.

The required probe/reconcile shape is:

1. capture an immutable opening lifecycle and replica snapshot;
2. perform provider-fenced reads outside the manager lock;
3. perform HTTP, URL resolution, Kubernetes, provider, SSH, join, cancel, and
   readiness waits outside the manager lock;
4. exact-reread current records and apply one short reducer/CAS under the lock;
5. enqueue durable cleanup claims under the lock but execute cleanup outside;
6. carry exact replica record, owner, and worker identity in every completion;
7. discard a stale completion with zero side effects, even if its numeric
   replica ID has been reused; and
8. admit queued launches before or independently of slow drain URL resolution.

Launch and teardown admission use separate bounded counters behind one stable
PostgreSQL transaction-scoped advisory key shared by every API/controller pod.
One short transaction acquires the key on its own connection, reads the global
durable counters once, exact-locks an ordered candidate batch, persists every
admitted row as running, and commits before any worker or provider effect can
start. Connection loss therefore rolls the reservations back and releases the
lock atomically; there is no second-session liveness window and a node-local
file lock is not production authority. Protocol-v2 reserved admission joins
this same transaction so its replica, association, executable request, queue,
retention pin, capacity debit, and counted launch reservation become visible
together. Launches retain the full configured launch parallelism. External
teardowns retain the historical maximum of two lightweight `core.down` workers
per launch slot, subject to the per-service teardown cap. Saturating either
direction therefore cannot starve the other; normal scale-down, whole-service
cleanup, failed-service purge, and orphan purge all use this gate rather than
independent per-row loops.

An active launch worker may call `core.down` inline to clean a partial provider
attempt before retrying. That cleanup remains charged to its launch reservation
for the entire call and does not recursively acquire the teardown budget. Thus
external teardown is bounded by `D = 2P`, inline launch cleanup by `P`, and the
absolute simultaneous `core.down` bound is `D + P = 3P`; the launch reservation
cannot be released until the inline cleanup returns.

The current deployed release does not yet satisfy this completely. The source
candidate in `fix/serve-probe-scale-convoy-v3` moves the remaining probe,
preemption, thread refresh, URL resolution, and completion paths across this
boundary. It remains unmerged and unproven in production.

Launch and down workers use per-worker immutable identity and cancellation
tokens, not a shared numeric-ID flag. Completion must exact-match the current
record and owner before changing a row, paid feedback, placement state, route,
or cleanup authority.

## Recovery and retirement

- Controller restart reconstructs demand, route, allocation, intent, claim,
  request, and retention ownership from PostgreSQL.
- A replacement controller uses a new controller incarnation and owner epoch;
  the service incarnation remains unchanged unless the service itself is
  recreated. Stale predecessor reports and worker completions cannot mutate
  the successor.
- A reserved Pod reclaimed by research is removed from spendable capacity on
  the next fresh observation and follows normal exact-identity cleanup.
- A paid replica becomes retirement-eligible from fresh durable idle demand,
  not from a controller-local empty queue.
- Graceful drain first removes routing, then waits for exact asynchronous
  occupancy within its bounded cap, then tears down the provider resource.
- Once that bounded functional drain settles, provider termination runs in a
  fresh logless context. Opening, writing, copying, or synchronizing a local or
  remote diagnostic log is never a prerequisite for the teardown effect.
- A controller `SHUTTING_DOWN` row remains cleanup-conservative until durable
  provider/quiescence evidence settles it. It is not proof that a provider
  instance still exists or is billable.
- No replica row lacking quiescence or provider evidence is manually deleted.
  Provider-free pre-effect cancellation may retire only with its exact
  `NOT_STARTED`/`NOT_QUERIED` proof.
- Provider-present cleanup uses the immutable association and provider identity;
  absence is never inferred from a missing SkyPilot cluster row.

PR #1726 added exact cancelled pre-effect retirement. PR #1727 added exact
completed guardian-family reaping. Both are merged and deployed in `1.1.1496`;
the former lifecycle-102 PHX cleanup rows are no longer live blockers.

## Routing, request ledger, and UI

### Route projection

The controller publishes a complete immutable PostgreSQL route generation
after a successful probe round. It binds service lifecycle/version,
incarnation, replica record identity, URL, exact accelerator/count, occupancy
mode, and economic provenance. A partial or ambiguous round publishes nothing.

Load balancers apply one coherent route generation. A new controller starts
fail-closed until it publishes current owner-bound routes; a warm load balancer
may retain only its last already-coherent snapshot during that bounded gap.
Route and request reads make no provider call.

### Exact asynchronous request ledger

A protocol-covered request supplies the advertised ledger protocol, exact
service incarnation, stable execution request ID and semantic intent digest,
and stable job/demand identity.

PostgreSQL allocates the server-owned attempt and revision. Retrying after a
transport ambiguity uses the same execution identity and intent. A different
intent under the same identity fails closed. A read-only receipt lookup cannot
create a row.

HTTP 202/200 admission is not completion. Terminal success requires the exact
ledger state plus the request's durable completion/artifact evidence. The UI
must identify whether its counts are exact or partial.

### UI fields

With a source timestamp and freshness, the service UI exposes accepted/recent,
queued, processing, in-flight asynchronous, rejected, terminal completed and
failed, and unknown/protocol-uncovered requests.

Replica presentation has independent economic (reserved fill, other zero-cost,
paid Spot, paid non-Spot, unknown) and lifecycle (ready, provisioning, shutting
down, cleanup-uncertain, historical) axes.

This prevents a historical `SHUTTING_DOWN` row from being presented as a live
billable Spot instance and prevents an idle zero count from being mistaken for
missing telemetry.

## Core invariants

1. **Conservation:** one physical UID/card GPU can be spent at most once.
2. **Reserved before paid:** committed and pending reserved supply is debited
   before paid residual is published.
3. **Commit before effect:** no Kubernetes or cloud create may precede its
   durable admission graph and exact provider guard.
4. **No reserved spill:** a reserved intent can effect only its exact zero-cost
   Kubernetes candidate.
5. **No on-demand:** paid residual can effect only Spot candidates.
6. **Bounded cost:** every cleanup-unproven paid GPU unit counts against the
   elected global cap.
7. **Scheduler ownership:** SkyPilot consumes, queues, or yields under external
   scheduler policy and never changes that policy.
8. **Exact identity:** lifecycle, incarnation, replica record, association,
   request generation, worker, and provider token fence every side effect.
9. **Fresh authority:** stale, incomplete, malformed, unknown-version, or
   owner-mismatched evidence authorizes zero new capacity.
10. **No slow lock:** blocking provider, HTTP, URL-resolution, Kubernetes, SSH,
    join, or cancel I/O never runs while holding the fleet manager mutex or a
    PostgreSQL admission lock. Short PostgreSQL rereads and CAS operations are
    intentionally allowed under reducer/admission locks.
11. **Single happy path:** reserved and paid launches share the generic non-pool
    request/executor/recovery path; reserved admission adds no second executor.
12. **PostgreSQL central truth:** EFS, local files, caches, and process memory
    are not control-plane recovery authority.
13. **Multi-GPU completeness:** every visible compatible GPU has its own worker
    slot and must complete fresh work in qualification.
14. **Telemetry is not authority:** UI and status are read-only projections and
    cannot launch, retire, or prove provider billing state.
15. **Teardown is log-independent:** after bounded functional drain, provider
    termination uses a fresh context without an inherited file-log sink and is
    never gated by diagnostic log I/O.
16. **Mutation directions cannot starve:** launch and teardown have independent
    bounded admission budgets, so a full provisioning wave cannot retain a
    billable `SHUTTING_DOWN` resource and a cleanup wave cannot convoy fill.

## Compatibility contract

Compatibility applies to durable state, not permission for mixed binaries to
start fresh provider effects.

| Runtime/cohort | Read and status | Recovery/settlement | Fresh admission/provider effect |
|---|---|---|---|
| N (current) | Yes | Yes | Yes, after homogeneous-writer and current-authority checks. |
| N-1 | Yes for declared additive shapes | Broad retained cleanup/recovery for the exact adjacent cohort | No while the writer cohort is mixed or current authority does not exact-match. |
| N-2 | Exact bounded decode/status | Only already-proven terminal, quiesced, pin-released, provider-`ABSENT`/`PROJECTED` row retirement | Never. |
| Older, newer, malformed, or partial | Fail closed | No guessed repair | Never. |

Current code writes worker projection protocol 10 and non-pool capability
cohort 12. Older readers are retained only for the bounded contracts above.
Decodability alone never grants provider authority.

New durable shapes are additive. A removal waits for a current-row census, no
old writers, and one complete stale/quiescence horizon. New effects require a
homogeneous current writer digest even when N-1 can safely read current state.

SkyPilot is currently test-only. For `boltz-l4-fleet`, old service-state
migration is not an operational requirement: normal evidence-backed `serve
down` followed by fresh `serve up` on the current schema is preferred when it
is simpler. This does not weaken N/N-1/N-2 cleanup rules for any retained row,
and it does not permit manual SQL deletion.

## Deployment and fix-forward operation

### Preconditions

- PostgreSQL is healthy and the central schema is at the current image head.
- Provider and cost guards report zero unexpected paid/on-demand resources.
- A control-plane release may be deployed while the current service still
  excludes PHX. Before PHX activation, the reviewed service definition adds
  only one non-Spot `k8s/phx_research_cluster_eks` H200 candidate to the East
  A100/A100-80GB and reviewed AWS/GCP L4 Spot candidates.
- Read back the exact server-owned PHX projection and existing external lane.
  Do not copy those settings into task-owned Kubernetes overrides and do not
  apply a Kueue or Terraform change.
- Do not rerun the historical Terraform config-seed Job. Its retained
  ConfigMap omits the PHX namespace and replaces `workspaces` wholesale; the
  PostgreSQL server-config row is the current authority. Helm does not run that
  Job.
- `min_replicas: 0`, fill floor 0, `utilization_gate: false`, and the explicit
  paid cap read back from the submitted service version.
- No task-owned namespace, service account, scheduler, priority, Kueue, raw
  Pod config, hostPath, or PVC override is present.
- Helm storage remains disabled.

### Control-plane deployment

Build and publish one immutable image. Upgrade the existing release with
`helm upgrade --reuse-values`, explicitly selecting that same image for API,
controller, and executor roles. Do not update a `boltz-platform` runtime pin.

Before provider effects resume, prove all two API, two controller, and three
executor Pods are Ready on the exact same image digest. If the reclaim-policy
or writer identity changes, invoke the existing PostgreSQL transition command
to authorize one fix-forward generation; an exact retry is idempotent.

The Helm migration inserts PostgreSQL server configuration only when the row
is absent; it cannot overwrite the retained revision. Read back the exact
server-config identity and PHX projection before and after Helm. Service
recreation does not recreate or reseed that central configuration.

### Service deployment

The local reviewed service YAML is deployment authority for this test service.
A clean `serve down`/`serve up` is allowed and preferred over compatibility
migration when a current lifecycle is easier to reason about. Teardown still
must finish through supported evidence-backed cleanup, and callers must refresh
the endpoint after recreation.

### Failure and rollback

The capacity-authority transition is one-way. After activation or an additive
schema commit, repair by deploying a newer homogeneous image and reauthorizing
forward. Do not demote authority, restore old database rows, or roll back to a
binary that cannot decode the current head.

The safe failure mode is underfill: stale or unavailable authority suppresses
new reserved and paid provider effects while already healthy coherent routes
continue serving. It is never duplicate capacity or on-demand spill.

## Verification plan

### Source qualification

- Run formatter/type/lint checks on every changed source file.
- Run focused probe batching, replica-manager, paid-capacity, reserved
  admission, route, request-ledger, recovery, and teardown tests.
- Run real-PostgreSQL tests for transaction atomicity, lost acknowledgements,
  stale identity, owner takeover, provider rejection/ambiguity, and concurrent
  admission.
- Prove no blocking provider/HTTP/URL/Kubernetes/SSH/join/cancel call occurs
  under the manager lock with deterministic race tests; allow only the short
  PostgreSQL reread/CAS critical sections the reducer requires.
- Prove a stale same-numeric-ID completion causes zero row, placement, paid,
  route, or cleanup side effects.
- Do not add SQLite migration coverage for this central PostgreSQL path.

### Post-Helm qualification

- Read back the exact image and chart digest and the 2/2/3 homogeneous role
  inventory.
- Verify PostgreSQL health, storage disabled, no PVC/EFS, bounded controller
  memory/thread count, and continuously fresh proof renewal.
- Verify the active service lifecycle, version, incarnation, authority modes,
  route generation, and both load-balancer slots.
- Verify no Kueue, Terraform, Terragrunt, KubeRay, IAM, or application object
  changed as part of the rollout.

### Reserved-capacity proof

- Reconcile physical East and PHX capacity, research occupancy, SkyPilot Pods,
  Kueue admission in PHX, PostgreSQL intents/replicas, routes, and ready workers
  at one synchronized observation.
- Require every compatible free East GPU and every PHX GPU admitted by the
  unchanged Kueue policy to be committed/provisioning or ready, with spendable
  capacity zero and a typed reason for any residual. Waiting PHX Pods remain
  visible as waiting and do not count as spendable or admitted capacity.
- Continuously inspect pending GPU research Workloads during qualification. If
  one remains quota-blocked while PHX fill holds admissions that unchanged
  Kueue does not promptly reclaim, disable PHX fill and record the scheduler
  limitation; do not mutate Kueue or synthesize victims.
- Prove all eight logical workers can pack on one healthy eight-GPU node and
  each device completes fresh accelerator-attested work.
- Delete no Pod or row merely to make totals align.

### Paid Spot provider-lifecycle proof

Before coupling provider throughput to application demand, recreate a
disposable GCP-only service whose floor, ceiling, and live paid cap are all 100.
Every replica is exactly one L4 on one `g2-standard-4` Spot VM and runs a
trivial HTTP task, so this gate measures provider/controller lifecycle rather
than model download or request routing. Start the provider/DB guard before
raising the floor, require at least 100 distinct provider VMs concurrently
`RUNNING` with zero on-demand resources, then request normal service teardown.
Completion requires zero exact test VMs, zero attributable managed boot disks,
zero live paid claims/waiters, and no nonterminal replica rows. Record the
creation, target-100, row-100, first/second launch-wave, provider-100, teardown,
provider-zero, and database-zero timestamps. Leave the disposable service
absent afterward. Preserve the canonical heterogeneous scale-to-zero YAML for
an explicit later activation; this gate must not activate or modify Kueue.

This provider-only gate deliberately sends no application traffic and does not
claim that reserved replicas served a request. It is a prerequisite for, not a
substitute for, the end-to-end campaign below.

### Paid Spot and 10,000-request proof

1. Start from the synchronized East-plus-Kueue-admitted-PHX reserved census,
   zero paid claims/waiters, and empty AWS/GCP provider inventories.
2. Arm a fresh bounded run for exactly 100 paid L4 GPU units and 10,000 new
   stable logical request IDs.
3. Verify both load balancers advertise the same ledger protocol and service
   incarnation before submission.
4. Fill and refill the authenticated reported queue capacity. Honor
   `Retry-After`; retry definitive pre-admission rejections with bounded
   backoff, and reconcile ambiguous outcomes by exact receipt lookup.
5. Require paid claims and provider Spot units to rise toward exactly 100 while
   every current compatible reserved worker remains committed first.
6. Require zero ordinary on-demand instance, zero paid non-Spot row, and no
   provider action outside the armed service/run envelope.
7. Record selection-time pool, instance shape, normalized price, provider
   request, and actual accelerator evidence. Verify cheaper eligible pools are
   attempted before more expensive ones unless typed availability feedback
   closed them.
8. Finish only when all 10,000 logical IDs have exact terminal ledger receipts
   and durable completion/artifact evidence, with no ambiguous or unsettled
   tail.

### Natural-drain proof

- Stop submitting demand without lowering the positive paid cap.
- Require queue, processing, in-flight, claims, and waiters to converge to zero.
- Require every paid route to drain and every provider resource to terminate.
- Check PostgreSQL, AWS, and GCP immediately, at +10, at +30, and through one
  complete stale/quiescence horizon.
- For AWS, inspect instances, open Spot requests, and every tracked EBS volume;
  for GCP, inspect instances and disks.
- Require the UI to separate historical/shutting-down rows from current
  billable provider resources.

### Restart and takeover proof

- Restart one service-controller child and then replace one controller Pod.
- Require a new controller incarnation/owner epoch but the same service
  incarnation, lifecycle, version, and routes; also require no duplicate
  provider effect, renewed proof receipts, and continued request telemetry.
  Only explicit service recreation may change the service incarnation.
- A stale predecessor worker completion must be ignored.

## Evidence to retain

Each final run retains one compact immutable evidence bundle: source,
image/chart and service-YAML digests; Helm revision and exact role inventory;
lifecycle/version/incarnation/authority generation; synchronized reserved and
worker/device proof; request manifest and ledger/artifact census; paid
claims/pool decisions and provider census; and all drain-horizon receipts.

Do not paste raw logs, credentials, signed URLs, request payloads, or repeated
minute-by-minute chronology into this design. Link the durable evidence bundle
and keep only the latest result in the current-state table.

## Remaining gates

1. Merge the source-qualified manager probe/cleanup concurrency correction and
   deploy one homogeneous immutable release.
2. Add the PHX H200 service candidate only, then prove the clean current
   lifecycle reaches 100% of the fresh East denominator and 100% of the PHX
   capacity actually admitted by the unchanged external Kueue policy. Make no
   Kueue, Terraform, IAM, or application change.
3. Run a fresh protocol-1 exact-ledger campaign that reaches exactly 100 live
   L4 Spot GPU units beside reserved capacity with zero on-demand.
4. Reconcile exactly 10,000 terminal logical requests with no ambiguous,
   missing, failed, or unsettled tail.
5. Prove natural paid drain and provider/database absence at every required
   horizon while the paid cap remains positive.
6. Capture a nonzero service-UI sample for queued, processing, in-flight, and
   completed requests, then a fresh idle-zero sample after drain.
7. Re-audit transitional cleanup PRs against current source. PRs #1619 and
   #1633 are still stacked on historical feature branches; PRs #1660 and #1556
   are currently dirty against `improvements`. Rebase or replace only the
   still-required deletion changes after the exact retained-row and stale-writer
   census; do not merge the old branches as-is.

The production objective is complete only when gates 1–6 pass. Gate 7 is
separately required before declaring transition-code and architecture cleanup
complete; it does not authorize expanding the Kueue or infrastructure scope.

## Rejected alternatives

- Mutate Simone's Kueue topology, add a SkyPilot-owned lane, infer entitlement
  from nominal quota, or treat Pod priority alone as a reclaim contract.
- Restore EFS/PVC state, platform dispatch logic, infrastructure expansion, or
  per-provider/accelerator services.
- Let reserved spill to paid, Spot spill to on-demand, or retries mask a lock
  convoy.
- Treat UI lifecycle as billing truth or delete state without exact evidence.

## Historical record

The pre-compaction 9,311-line implementation and incident record is available
with `git show a700ef02:docs/designs/serve-multi-pool-reserved-capacity-fill.md`.

That revision is historical evidence, not a second design, rollout runbook, or
source of current production state.
