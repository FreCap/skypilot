# Multi-pool SkyServe reserved-capacity fill

Status: protocol v2, the durable executor/provider fence, and the sequential
detached-authority correction are merged.  The latter merged in PR #1440 as
`c964b5480`; it supersedes the incomplete correction in PR #1433.  Production
inspection on 2026-08-11 found server `1.1.1243` at `2d2c67efb` and confirmed
that the reserved A100 pool was not full even though the broker repeatedly
published 34 free slots.  Those snapshots reached the autoscaler 181--250
seconds after their conservative source timestamps, beyond the unchanged
180-second freshness horizon, because capacity polling and slow actuation
shared `_actuation_epoch_lock`.  The same incident exposed a second loss of
progress: `_apply_reserved_capacity_fill_v2()` debited every planned decision,
while `ReplicaManager.scale_up_batch()` could durably accept only a prefix and
returned no receipt for the deferred tail. The commit-ordered observation
ledger, event-driven reconciliation, durable-intent state/receipt, bounded fair
provider submission, fleet-wide activation fence, and status correction
specified below are the planned root-cause fix; they are not implemented or
deployed.
The broker's successful UID-fenced 34-slot publication proves that the live
identity-read permission and reserved-cluster module state were not the cause
of this underfill. The separately documented temporary RBAC bridge remains
declarative-drift cleanup, but changing permissions cannot repair either
stale-after-lock capacity or the missing accepted-prefix receipt.
Strict epoch fencing remains authoritative. No timeout increase, alternate
planner, user/service-selectable behavior flag, or capacity-consuming canary is part of
the correction.  Historical protocol-v2 cleanup and live-verification gates
remain explicitly separate.
The 2026-08-11 operator decision is fix-forward only. The first irreversible
boundary for a nonempty source is the Historical Authority Reset's anchored-A
cutoff-marker/legacy-`NOLOGIN` transaction after `CUTOFF_PREPARED`; for an
empty source it is the first guard/enforcement-receipt mutation. Every later
recovery resumes that monotonic reset
or uses a newer phase-0 revision. After anchor
enforcement, the compatibility image is the minimum supported binary, and after
the first `PREPARING_SEQUENCED` commit neither an old image nor an old data state
is a supported rollback target. Planned and unplanned control-plane iteration
use one sequenced maintenance kernel, which
quiesces provider effects, replaces/re-attests the full cohort, and resumes the
same reconciliation path.

Last updated: 2026-08-12

Canonical owner: this file. The implementation, rollout evidence, and its
not-yet-authored stacked current-correction removal change must stay
synchronized with this contract.

Merged feature PR [#1261](https://github.com/boltz-bio/skypilot/pull/1261) is
the historical revision-035 rollout change. Draft cleanup PR
[#1263](https://github.com/boltz-bio/skypilot/pull/1263) is its historical
protocol-v1 cleanup in GitHub stack #1264; it is not the removal PR for the
current reconciliation correction. That current removal PR is authored with
the implementation stack and receives its own link and exact merge gate before
implementation merge.

## Summary

Before protocol v2, `reserved_capacity_fill` accepted only one Kubernetes
context per service. Its durable claim, controller cache, autoscaler snapshot,
demand gate, and launch override also each held only one pool. Removing the
validator alone would have overwritten one context with another, multiplied
service-wide policy, and allowed a grant measured in one cluster to launch or
shelter a replica in a different cluster.

This change partitions a service's zero-cost Kubernetes locations into one
broker pool per context. Each `(service, pool)` edge has an independent claim,
round, feed, grant, snapshot, damping state, and launch fence. The service's
existing floor, utilization policy, demand target, `max_replicas`, and fill
headroom remain global. A deterministic allocator divides that one global
budget among its pool edges before the existing cross-service broker allocates
capacity within each pool.

The public YAML is unchanged. Adding a second Kubernetes context to a service
that already enables `reserved_capacity_fill` opts that service into the new
behavior.

## Goals

- Let one SkyServe service borrow idle reserved GPUs from multiple Kubernetes
  clusters at the same time.
- Keep `floor_replicas`, utilization gating, and `max_replicas` service-wide;
  adding a context must not multiply any of them.
- Preserve the existing one-pool behavior and YAML contract.
- Isolate pool observation, staleness, grant damping, demand admission,
  scale-down shelter, and epoch fencing.
- Pin every fill launch to the exact pool whose feed authorized it, including
  when two contexts expose the same accelerator name.
- Roll out with additive PostgreSQL state and a one-way minimum image from the
  first durable activation-state change.
- Reconcile a fresh capacity publication immediately through the same planner
  used for demand and periodic work, without waiting for an unrelated slow
  provider or readiness operation.
- Turn a fill plan into durable accepted intents before counting its capacity
  as spent, so a busy provider phase cannot silently discard the unaccepted
  tail of a wave.
- Initialize independent Kubernetes pools concurrently and fairly while
  preserving the existing global provider-concurrency bound and BCL's right to
  reclaim preemptible inference slots.
- Expose raw observed capacity, freshness, spendable capacity, durable intent,
  provider progress, and ready capacity as distinct states.
- End with one capacity-to-replica path and an explicit removal gate for every
  superseded lock, wakeup, debit, and provider-dispatch path.
- Keep the sequenced control-plane membership contract to one supported
  topology: split-role PostgreSQL HA (`api`, `controller`, and `executor`).
  Compatibility-mode `all` remains a legacy-only deployment and is not given a
  second Recreate handoff protocol.

## Non-goals

- User-configurable per-context floors, weights, or preferences. Stable task
  resource order is the initial pool order; an optional policy can be designed
  later without changing this contract.
- Combining disjoint accelerator groups inside one Kubernetes context. All
  zero-cost accelerator names in a context remain one physical pool group.
- Parallel Kubernetes observations were a non-goal of the first protocol-v2
  release. The correction now queries physical pools concurrently through the
  provider-free Serve observer and durable admission-order seam. It retains the
  short global broker scan-to-publish safety boundary and also parallelizes
  independent provider initialization. Sharding that remaining broker critical
  section requires a separate durable per-pool publish-lease proof and is
  triggered only if measured broker-lock time consumes the freshness budget.
- Treating two kubeconfig aliases as independent capacity. The new pool claim
  records a Kubernetes cluster UID and rejects overlapping aliases when that
  identity is available. A context whose physical identity cannot be verified
  does not participate in multi-pool fill; ordinary demand placement remains
  available.
- Weakening the 180-second freshness horizon, postdating a slow observation,
  or treating `provision_timeout` as evidence that a provider is either out of
  capacity or still initializing.
- A second fill-only planner, actuator, queue, or wakeup loop.  All triggers
  enter one reconciliation coordinator and all accepted replicas enter one
  durable provider-dispatch path.
- A promise that 200 admitted replicas become ready in a fixed time.  Admission
  is bounded; readiness still depends on actual capacity and provider latency.
- A canary, shadow actuator, service selector, or user-visible old/new behavior
  flag for the current correction. The mandatory fleet-wide mixed-image gate
  is a safety protocol, not an experiment. A small canary cannot reproduce the large-fleet lock convoy and
  two live planners would weaken the capacity and `max_replicas` invariants.
- Live changes to an authority Deployment's desired replica count, selector,
  rolling strategy, service account, role identity, or other authority
  topology while `SEQUENCED_ACTIVE`. Planned changes first enter
  `SEQUENCED_MAINTENANCE`, then use the one fix-forward cohort-replacement
  kernel. An out-of-band mutation enters the same fail-closed maintenance
  recovery; it never opens a second live-effect handoff path.
- Changing the central PostgreSQL connection target, database, schema, or
  installation identity in this rollout. The only *central-database*
  credential change is the pre-provisioned, same-A legacy-role to sequenced-
  role fence carried together in the external anchor during phase 0. The cold
  Historical Authority Reset separately rotates/revokes the old AWS, GCP,
  Kubernetes, static-certificate, and vendor provider principals so no old
  runtime can retain effect authority. After that reset revokes the legacy
  epoch, the successor central-database
  Secret identities and values are immutable. Any later credential rotation,
  failover to a separately writable clone, or database transfer requires a
  separately designed protocol that fences both endpoints; the control-plane
  image fix-forward path rejects it.
- Adding image-copy, lifecycle, or canary workers to the first sequenced
  authority cohort. Existing transitions reject these optional database/
  provider clients before guard mutation; later support must give them the same
  process lease, guard token, effect fence, and maintenance contract. The
  phase-0 transition itself requires Kubernetes 1.27 or newer even though
  unrelated SkyPilot Kubernetes workload support retains its broader floor.

## Public contract

The existing forms remain valid:

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

For fill-enabled services, zero-cost Kubernetes candidates are grouped by
context. Within each context:

- all configured accelerator names form one broker pool;
- every physical-backend candidate must use the same positive whole GPU count;
- logical-replica candidates must continue to use exactly one GPU.

Independent physical-backend contexts may use different per-replica GPU
counts. Non-Kubernetes and paid candidates are unaffected.

The policy fields retain these meanings:

- `floor_replicas` is one total floor across all of the service's pools;
- `weight` is the service's relative weight on every pool edge;
- `utilization_gate` observes the service once and governs the same global
  fill budget;
- fill headroom is `max(0, max_replicas - demand_target)` across all pools;
- the final number of nonterminal replicas across every version and location
  never exceeds `max_replicas` because of fill.

No new public SDK/CLI mutation or YAML field is introduced. The additive raw,
spendable, intent, provider, and ready status fields are a client-server
response change, so the transition implementation bumps `API_VERSION` and
gates new-client presentation on the remote version. Old clients ignore the
additive fields; a new client talking to an old server may display only the
legacy aggregate with an explicit unavailable detail state. That display-only
compatibility never supplies control-plane authority or a fallback planner.

## Architecture and invariants

### Pool discovery and identity

The poller builds an ordered `FillPoolSpec` for each Kubernetes context. It
contains the context, canonical accelerator shapes, matching zero-cost
locations, broker key, and the Kubernetes `kube-system` namespace UID used as
a non-secret physical-cluster identity.

Protocol-v2 pool keys are versioned and use the physical UID plus canonical
accelerator set. They never share a round with protocol-v1 context keys. The
access context remains a separate claim field used only to query and launch.
An overlapping live claim reached through a second context alias therefore
joins the same physical pool instead of being counted twice. If one service
configures two aliases of the same physical pool, the first task-resource
position is the deterministic survivor and the duplicate edge is rejected.

Physical identity is cached for at most one poll interval. Lookup uses a
bounded read of the `kube-system` namespace UID. A failed lookup withdraws only
that edge and feeds it zero; it never substitutes the context string as
identity. The final Serve pool observer runs its realtime availability listing
inside a capture pinned to that edge's expected UID and durably publishes the
result for the provider-free broker. A context retarget during
measurement is therefore a blackout, never capacity evidence that can grant
or drain holdings belonging to the prior physical cluster. Fill-intent
physical preflight takes `V2_FENCED` admission outside the manager,
reservation, and broker locks. Phase or initializer contention returns a typed
deferral before a durable row exists. The short final acceptance seam then
takes the shared reservation/manager/broker order and atomically persists
`ACCEPTED`; it starts no thread or provider request.

At `SUBMITTING`, the final durable API launch context carries protocol, pool
key/epoch, service generation, policy revision, observation generation/sequence/
validity, physical UID, Kubernetes context, exact accelerator name/count, and
the immutable intent idempotency key. The bounded legacy adapter carries only
the historical seven-field subset while `LEGACY_ACTIVE`. Presence of any final
field requires the complete final tuple, a
complete normal Serve owner fence, and a controller-originated request.
Pool protocol must be exact integer `2`; generation and count must be positive
exact integers (not booleans); strings must be nonempty; and the parsed v2 pool
key must encode the same UID and contain the canonical accelerator. API ingress
rejects every partial, malformed, contradictory, or non-Serve tuple before
scheduling a request. Absence of the complete active-protocol tuple is ordinary
demand or protocol v1 and performs no physical-identity read; only the
`LEGACY_ACTIVE` adapter recognizes the exact historical seven-field tuple. The
tuple is copied into the immutable
PostgreSQL request body and must survive request/executor restart without
consulting controller memory. API ingress independently validates and
atomically binds the carried tuple, intent state, request ID, and execution
generation without relying on earlier controller authority. The one bounded API
submission attempt acquires a fresh phase and physical fence, revalidates the
carried context/shape/epoch/observation validity with PostgreSQL time, creates
or discovers the deterministic Kubernetes object, and durably commits its UID
and `WAITING_CAPACITY` or `INITIALIZING` state before releasing the mutation
permit. Capacity waiting and later startup/readiness are asynchronous
observation checkpoints, not one phase-holding long worker. After the request
is bound, recovery may only discover and adopt the same idempotency key,
request, and object; it never issues another create. The
tuple is bound to the same service incarnation as the normal owner fence. Once
the exact object was validly created, observation expiry alone does not delete
it; a newer broker/headroom decision uses the fenced cancellation state
machine.

After request recovery, admin policy, and optimization, the executor requires
the final selected resources to retain the exact Kubernetes context and shape.
That early check is not the last placement authority: immediately before the
bounded provider mutation, the submission checkpoint revalidates its selected
resources against the durable tuple. The attempt must still use Kubernetes,
the exact context, and the canonical accelerator name/count. A mismatch is a
terminal request cancellation before provider-side actuation; it is never
eligible for retry or failover. Thus an internal retry, optimizer alternative,
or admin-policy alternative cannot leave the fenced context or shape after the
executor's final-plan check. The executor activates a process-local provider
fence for the bounded object lookup/create checkpoint. Later Kubernetes
observation re-proves the same object UID/context without retaining a global
provider permit. Fence activation captures
one immutable, least-readable kubeconfig target, reads its `kube-system`
Namespace UID, and admits the scope only when it matches the durable UID.
External exec-auth contexts retain their captured exec credential contract but
cannot change endpoint; the synthetic in-cluster region and the provider's
normalized `None` context resolve to the same fenced target.

Every Kubernetes API wrapper used by the operation, including calls from its
Pod-creation thread pool, borrows the capture-pinned raw client. Transparent
refresh cannot reread the ambient kubeconfig while a fence is active. Client
replacement waits for outstanding call leases, while calls sharing one proved
client remain concurrent; the fence must not serialize multi-Pod creation by
holding a refresh mutex across each network call. Every external-context
`kubectl` command path, including exec, port-forward, and rsync, receives the
same capture-pinned kubeconfig rather than the ambient context. For an
authority-role in-cluster command, the capture creates one private tmpfs
directory and mode-0600 kubeconfig containing the pinned API server/CA plus a
`tokenFile` reference to the rotating provider-token projection; token bytes
never enter argv, environment, rendered config, or logs. Every local `kubectl`,
exec, port-forward, and rsync child receives only that path. The capture scrubs
it after all children join, and no command may fall back to the ambient/default
kubeconfig or directly read a fixed service-account directory. `None` resolves
to the active in-cluster fence for API-client verification. Every Kubernetes
caller in this change, authority or otherwise, receives an explicit
`KubernetesCredentialSource`; non-authority composition roots may pass the
standard projected-file paths, but helpers never discover/read those literals
themselves. Only an authority source can create this UID fence. Conflicting simultaneous
expected UIDs for one context fail closed. Retargeting between observation, enqueue,
restart, policy, optimization, client refresh, provider actuation, runtime
bootstrap, setup, or job submission therefore cannot mutate or deliver data to
a different physical cluster. Concurrent UID reads never publish out of
generation order. A superseded successful read returns the newer generation's
live cache value when one is already published; otherwise both forced and non-
forced callers may return their own independently successful read without
publishing it. Generations order lookup starts, not read completion, so
discarding that value is not a stronger identity proof: it spuriously
withdraws the pool edge on observation and refuses every matching launch
guard. A failed or empty read still returns no identity, and the exact launch
capture independently proves the carried UID before provider mutation.
Ordinary demand placement remains available.

The process registry is deliberately conservative while a capture is active:
an unleased same-context provider call, or a leased call that attempts a
second context, fails closed instead of borrowing the capture or falling back
to ambient configuration. Explicitly supported fan-out propagates the lease.
An unrelated ordinary request that overlaps this short scope may therefore
retry after the scope retires; outside an active scope its historical ambient
behavior is unchanged.

Fleet work that can consult mutable Kubernetes authority must make that retry
boundary explicit. A mixed legacy/protocol-v2 status or probe batch is
partitioned: all v2 rows run with exact propagated leases while their owners
are live; only after those owners retire may Kubernetes or unknown ordinary
work enter `AMBIENT_LEGACY`. The two gated modes may remain internally parallel
but never overlap. Exact non-Kubernetes operations explicitly enumerated below
may run outside either mode only when they consume the same prefetched durable
handle, or an exact UUID-and-handle-predicated record, and cannot re-resolve
provider identity by name. The v2 phase includes result classification,
preemption refresh, and teardown that can consult Kubernetes authority;
joining only the initial futures is insufficient. Shared provider-free
reduction may run afterward, but no deferred v2 Kubernetes call may escape its
lease. Malformed v2 rows remain identity-uncertain and are never downgraded.
One round performs one UID proof per physical v2 pool.

Each OS process that can issue Kubernetes-authority-bearing work (API,
controller, or executor) has a provider-phase gate with exact modes
`V2_FENCED` and `AMBIENT_LEGACY`.
Same-mode callers may overlap. Fresh callers
receive FIFO tickets: after an opposite-mode ticket queues, later same-mode
callers cannot barge, and the next maximal same-mode prefix becomes one cohort
when the active cohort drains. A root admission may explicitly authorize
already-planned child workers to join its exact process/PID/boot/epoch-bound
cohort; an ordinary thread or copied/stale admission cannot. The root closes
child admission before exit and already-admitted children drain first.
Same-mode nesting on one thread is reentrant, cross-mode nesting is rejected,
and cancellation or timeout removes the waiter and wakes the next turn. An
expired opposite-mode FIFO barrier is pruned even while a compatible cohort is
still active; a now-compatible queue prefix joins that same epoch, while the
first live opposite waiter remains an absolute barrier. Every blocking
acquisition has one 30-second absolute monotonic deadline and fails closed with
a typed phase-timeout error. An `after_in_child` fork hook replaces
the condition, queue, active phase, admissions, and thread-local state without
touching a possibly inherited locked mutex. The composed physical-fence
registry performs the same child reset for its lock, condition, active and
initializer maps, failure generations, and `ContextVar`; it never unlinks a
parent-owned captured kubeconfig from the child.

The final lock order is the non-reversing provider-preflight, shared zero-cost
reservation, short manager mutation, then broker/persist order defined in
"Generation-triggered reconciliation and durable intent admission" below.
Probe and refresher work no longer form continuously locked read-modify-write
cycles. They snapshot row/worker identity briefly, take provider phases and run
I/O outside `self.lock`, then conditionally merge only if the row version,
request ID, and worker generation still match. A denied or stale result
publishes no readiness, absence, preemption, identity-mismatch, cleanup, or
capacity-reservation evidence and wakes the common coordinator. Fill admission
returns a typed accepted/deferred receipt; a busy item cannot silently consume
or discard the remainder of its plan.

The autoscaler interleaves protocol-v2 fill decisions in stable configured
pool order.  It remembers the stable identity of the pool that actually
emitted the prior wave's first decision; the next wave starts after that pool
in the complete ordered pool map and only then filters pools with no spendable
feed.  A tick that emits no fill decision does not consume a fairness turn.
The identity is carried across an in-process autoscaler state transfer only
when it is a string naming a pool in the validated restored map.  Anchored-pool
removal, protocol demotion, fill disablement, a malformed or unknown anchor,
and a full controller restart reset it.  Thus a continuously running controller
gives every continuously actionable pool a first-admission opportunity within
at most one emitted wave per pool in the validated map, without adding provider
or database calls; ordering remains linear in the number of pools.  A detached
decision wave also captures the ordered-map revision.  If pool membership or
ordering changes, or a lifecycle reset removes and then re-adds the same pool
identity, the stale wave is discarded before the replica manager and may
neither suppress ordinary work, mutate the replacement feed, nor recreate the
cleared rotation anchor; the caller's ordinary decisions remain unchanged.  If
any live pool's generation, physical UID, or broker epoch differs without a
map-order change, the complete detached overlay is likewise discarded and the
caller's exact ordinary decisions are restored.  Target/headroom partitioning
and demand/shelter coverage couple every pool, so no unchanged peer's detached
subset is independently committable.  Rollback aligns the aggregate target
projection with the replacement live per-pool targets but does not debit feed
or advance the rotation anchor; a fresh tick retries the complete map within
one decision interval.  A same-authority timestamp or damping refresh remains
committable. Only after every authority tuple matches and at least one intent
is durably accepted does that accepted intent's pool advance the rotation
anchor. The pure `ReservedFillPlanner` mutates no feed or anchor when it emits
a plan; `FillCommitResult` is the commit seam for accepted occupancy and
fairness. The short policy-revision CAS plus the manager's durable broker
fences remain authoritative across planning and acceptance. Dynamic-state
loading builds an unpublished replacement planner and swaps it under the short
policy lock; provider, database, broker, and manager work occur outside that
lock.

There is deliberately no exclusive manager-wide reconciliation round: one
unreachable job-status SSH call must not block readiness or the refresher that
admits already-enqueued launches and downs. Job status takes every blocking
phase outside `self.lock`, partitions strict v2 rows before gated ordinary work,
and passes admission to every phased worker. An ordinary SSH worker may run
unphased only when the same prefetched durable record supplies an exact
`CloudVmRayResourceHandle` with a real non-Kubernetes `Cloud`, and the backend
consumes that object without a name lookup. Success reduction is provider-free.
If an unphased fetch raises `CommandError`, fresh provider-backed preemption
classification acquires `AMBIENT_LEGACY` before the manager lock, then re-reads
the replica under the lock; a row that changed to v2 is deferred to the next
strict round.
Missing, malformed, fake-cloud, or Kubernetes handles stay ambient. Probe and
refresher use the snapshot/probe/conditional-merge contract above. Their short
snapshot and merge sections retain atomicity against scale, update, and other
pickled-row writers without holding `self.lock` across provider admission or
I/O.

One probe snapshots its provider-free fleet/cluster and durable identities,
tick spec, process guards, and route registry exactly once. Outside the manager
lock it runs the complete v2 subset under `V2_FENCED`, then the ordinary subset
under `AMBIENT_LEGACY`, and joins their readiness/status/liveness work. The
final short merge re-reads affected row identities and persists only matching
results. A denied or superseded subset leaves rows unchanged. One-time state is
not reset, pruned, persisted, or finalized once per subset.

The refresher uses the same v2-before-ordinary phase partition outside the
manager lock. Wait-for-idle URL resolution leaves its tracker untouched when
admission is busy. Inline log sync and drain-URL lookup remain best-effort, and
the exact worker generation is revalidated before registry removal or cleanup
merge. Boot recovery consumes phase busy as a typed deferral and wakes the
coordinator; it never enters the generic 30-second retry sleep while retaining
`self.lock`.

An asynchronous launch HTTP request never holds a phase. Before persisting a v2
fill launch, the carried override is classified provider-free and the exact
`(context, UID)` physical fence proves the pin under the standalone-blocking or
batch-try admission described above; ambient force-refresh is not used. Failure
returns before row persistence or request submission. A deferred down worker
waits for drain with neither lock. Each v2 retry independently enters
`V2_FENCED`, selects the workspace, creates a fresh physical proof, and performs
the provider mutation. An ordinary action-aware retry may bypass the Kubernetes
phase only when its exact UUID-predicated cluster snapshot proves a real non-
Kubernetes cloud and `core.down` retains both that UUID and the classified raw
serialized-handle fingerprint. The backend re-proves the same handle under its
two action locks before provider effects; a same-UUID handle rotation retries
from fresh classification. Legacy/name-only, missing, malformed, fake-cloud,
and Kubernetes cleanup remains
`AMBIENT_LEGACY`. Every retry reclassifies; all paths release phase/fence before
backoff and never reuse the originating round's proof.

Paths without manager state use the process phase directly. These include cold
and warm load-balancer route synchronization, standalone active-URL reads,
full API service-status serialization (including multi-service fanout), and
reserved-capacity observations. They run complete v2 groups before ordinary
rows and never turn a phase timeout into negative evidence. A load-balancer
route-sync phase timeout aborts that synchronization with 503 and publishes no
new mapping or warm-cache state; it cannot produce a successful mapping with
the timed-out rows omitted. Standalone/API status may report identity unknown
but never physical absence. The final Serve pool observer enters `V2_FENCED`
for the exact physical pool, queries outside broker and manager locks, durably
publishes by observation lease/UID, and releases its phase.
`run_round_if_stale` then consumes that immutable row without entering a
provider phase. The shared admission order and reservation boundary, rather
than a provider callback under the broker lock, conserve rows that race the
query. A phase or broker timeout publishes no new authority and cannot become
physical absence. One driven observation creates one physical proof for its
pool.

Interactive log follow is the bounded-operation exception. It uses a dedicated
streaming fence which does not acquire or hold a process phase. A v2 row still
validates its durable handle and holds the immutable physical fence for the
whole stream; it may join an existing same-UID capture, while conflicting UID
or initializer admission fails closed. An ordinary follow remains ungated and
the central adaptor rejects any unleased collision. Streaming bytes cannot
publish lifecycle evidence. Consequently a long v2 follow can make ordinary
same-context reconciliation retry until the operator stops it, but it cannot
monopolize the fair phase gate. Bounded/non-follow tails use their normal phase
for the complete read.

An independent broker UID discovery that encounters the typed
owner-or-initializer collision waits at most 30 seconds for that context to
become ambient again, using one absolute monotonic deadline across capture
replacement races, and then re-reads the UID from fresh ambient credentials.
The retry retains its original lookup generation and follows the same
publication rule: a forced or non-forced caller can report its successful
post-wait read without stealing cache ownership when the newer generation has
not yet published, while an available newer live cache value wins.
It performs this wait without a broker/cache lock and only when the caller has
no fence token, so it cannot deadlock its own scope. Owner and initializer
retirement wake waiters. Other identity errors, a timeout, a context mismatch,
or an owner failure still withdraw that pool and never borrow the expected UID,
capture target, client, or token. This prevents a valid pool from flapping
merely because legacy reconciliation overlapped a v2 batch without weakening
the process-global fail-closed rule.

The durable physical fence continues after launch for every alias-sensitive
replica lifecycle read. Endpoint discovery, readiness routing, job-status SSH,
candidate/recovery status, interruption detection, drain registration, status
serialization, active-route enumeration, and interactive replica log tailing
must reconstruct the protocol-v2 context/UID authority before contacting
Kubernetes. A durable cluster handle used by those reads must also remain a
Kubernetes handle for the same cluster name and context. An identity or handle
mismatch produces unknown/off-route evidence; it is never reclassified as
preemption, application failure, an absent job, or a valid replacement
endpoint. The load-balancer controller
re-gates both cold and warm route-cache entries on every synchronization. If
no protocol-v2 replica can prove its physical identity, its verified-ready
count is zero and the empty mapping explicitly retires prior routes rather
than triggering the generic spurious-empty safeguard.

Lifecycle reconciliation batches this verification by durable
`(Kubernetes context, physical cluster UID)`. One synchronization or probe
round captures and verifies each physical pool once; nested per-replica reads
reuse that active capture. Parallel worker calls enter the fence inside the
worker (`ContextVar` state is not implicitly inherited by legacy thread pools)
and concurrent scopes for the same pool coalesce on one initializer.
Consequently a 1,000-replica pool does not issue 1,000 namespace UID reads per
probe interval.

Cleanup is bound to the same physical authority. Immediate failed-launch
cleanup and every later log-sync/down retry for a protocol-v2 fill reconstruct
the exact context/UID fence from durable launch and replica state before any
provider or command-runner call. This applies equally to ordinary replica
retirement, interrupted-fill recovery, failed-controller cleanup,
failed-service purge, and orphan purge. A missing, partial, unreadable, or
mismatched identity performs no cleanup through that alias; the replica and
cluster rows stay visible as cleanup-uncertain and retry only under a matching
capture. Provider identity is independent from the SkyPilot cluster-row
generation: cleanup also snapshots the nonempty durable `cluster_hash`, unless
an action-owned `cluster_record_uuid` is available, in which case that UUID is
authoritative. The chosen UUID-or-hash fence is revalidated after acquiring
both cluster locks and again after status refresh, before request cancellation,
credential checks, status/provider calls, or row removal. The service-owner
continuation guard is likewise rechecked under those locks. Exact cleanup
skips name-only launch-request cancellation, pre-lock and post-lock
`force_unlock`, and pre-lock teardown hooks because each can otherwise affect a
queued or already-created same-name successor. Final legacy row removal uses
the same `cluster_hash`; action cleanup uses its exact UUID and handle. A
missing SkyPilot cluster record is not independent proof that a
protocol-v2 provider object is absent, so bulk scale-down and service-purge
absence fast paths retain that row and prevent parent deletion. Partial
resources on the original cluster can therefore remain until its identity is
restored or an operator independently proves absence and explicitly purges
them, but the replacement cluster is never used as cleanup evidence or mutated
by name.

Cleanup also carries an exact durable SkyPilot cluster-record generation. A
resource-action record UUID is authoritative when present; a nonempty legacy
cluster hash is the fail-closed fallback for older/null-action rows. The
backend acquires both the cluster-status and resource-operation locks without
force-unlocking either one, re-proves that exact UUID or hash before the first
tunnel, credential, status, or provider interaction, and proves it again after
status refresh. Exact cleanup suppresses pre-lock teardown hooks and both
pre-lock and under-lock name-wide request cancellation, since any of those
could affect a queued same-name successor. Final database removal uses the
same UUID or hash predicate. A missing, rotated, or changed generation and an
owner-continuation failure therefore perform no teardown effect and leave the
replica cleanup-uncertain for later reconciliation.

The control-plane identity on every configured pool must therefore have the
exact cluster-scoped permission `get` on the core `namespaces` resource with
`resourceNames: ["kube-system"]`. It does not need namespace `list`, mutation,
or access to any other Namespace object. The chart renders this rule directly
in the applicable role-specific provider ClusterRole for an in-cluster
candidate; relying only on an inherited/default `rbac.clusterRules` value would
let an imported predecessor preserve the old incomplete list. Complete signed
target values and RBAC projections must contain the rule. The reusable spoke workspace-pool
RBAC module owns the same grant for remote pool identities alongside its
existing cluster-wide read contract. Externally managed kubeconfig identities
must receive an equivalent grant from their operator.

Workspace eligibility is evaluated on the effective merged configuration. If
a workspace inherits global `kubernetes.allowed_contexts`, materializing an
explicit workspace list that equals the inherited set, or retains every
inherited context and adds another context, is an equivalent or additive
change. It is safe in the presence of active resources for the same reason as
an already-explicit list growth: no running context is removed. A finite list
may also broaden to `all`. Conversely, inherited `all` or legacy unrestricted
behavior may not materialize a finite list around active resources, and a
finite effective set may not lose a context. Other Kubernetes fields and all
other workspace fields remain part of the ordinary active-resource guard; an
empty `kubernetes: {}` produced only by extracting `allowed_contexts` is
normalized away for comparison and never persisted as a semantic change.

Validation resolves the effective current and proposed values from one
immutable request snapshot: a workspace-only update uses the same snapshotted
top-level default on both sides, while a whole-config update uses the current
snapshot for the current side and the submitted configuration for the proposed
side. The write must reject or revalidate if the snapshotted workspace or
top-level default changed before the file-lock-protected commit. User-access
validation still runs when an equivalent/additive context change is combined
with a private/allowed-user change. The comparison must not reject safe
materialization merely because the current workspace-local field is absent.

An operator must verify both
`kubectl auth can-i get namespace/kube-system` and a nonempty `.metadata.uid`
through every configured context before expecting a protocol-v2 edge to become
authoritative. This feature-specific preflight must not become a general
Kubernetes credential-check requirement: ordinary demand placement does not
need it. Missing permission is a safe pool-local blackout, not a reason to
weaken identity to a context name or another alias-sensitive value.

There is at most one pool edge for a service in a context. Overlapping but
non-identical accelerator groups remain invalid across services and are also
invalid for two edges of one service.

Claimants sharing one physical pool must also use one replica-slot width. The
width used by the most claimants wins deterministically, with the smaller
width winning a tie. Protocol v1 retains its historical behavior of deleting
losing claims. Under protocol v2, a losing edge remains in its authoritative
complete service set at the same generation but receives round-local zero
grant/feed authority. Its differently-scaled poller cannot drive the shared
capacity query; a matching-width poller publishes the durable round with the
complete generation map and explicit zero entries for every loser. Thus a
width conflict blackouts only that pool edge and cannot generation-fence a
healthy sibling pool or create a delete/re-add heartbeat loop.

### Durable claims and compatibility

Serve schema revision 035 adds three PostgreSQL tables:

- `reserved_fill_protocol_state`, a singleton durable activation gate whose
  initial protocol is v1 and whose audit evidence records the common writer
  image digest, canonical API/controller Deployment generation/UID
  inventories, and the combined pod/process inventory count/SHA-256;
- `reserved_fill_service_claim_sets`, one authoritative set marker and global
  budget/governor row per service; and
- `reserved_fill_pool_claims`, primary-keyed by `(service_name, pool_key)`,
  carrying the current edge fields, access context, physical UID, and service
  generation.

Revision 035 also adds `claim_generations`, `protocol_version`, and nullable
`feed_by_accelerator` to each pool round. The latter is a JSON map from service
to its exact-card portion of the already-arbitrated feed. `NULL` means an old
writer/round had no exact-card metadata; a present empty service map is
authoritative zero shaped feed. Migration copies legacy claims into
generation-zero normalized rows and
marks their service sets `migration_shadow`. Shadows are never authoritative:
while the protocol is v1, every new binary runs the behaviorally identical
legacy one-pool path and reads only `reserved_fill_claims`. Its only additive
decision field is the internal carried protocol used by the activation fence.

Protocol v2 cannot be activated merely by submitting a multi-context spec or
setting an environment variable. A separate zero-argument operator action runs
inside an API pod and accepts no rollout identity or proof. Under the global
broker lock it requires exact Serve schema head 035 and protocol v1. It reads
`SKYPILOT_OWNER_CHAIN_TOKEN_PATH`,
`SKYPILOT_OWNER_CHAIN_CA_PATH`, and
`SKYPILOT_OWNER_CHAIN_NAMESPACE_PATH` only, using a symlink-safe confined open
contract over the projected-volume root, bounded regular-file `fstat`, and no
`subPath`; kubelet AtomicWriter link swaps remain valid but traversal outside
that root does not. It rejects malformed, legacy, wrong-audience, expired, or
otherwise unbound tokens without complete nested namespace, Pod name, and Pod
UID claims. A one-shot client uses exactly those token/CA bytes plus the pinned
API endpoint, disables refresh, and shares that client across all Core and Apps
reads. It never reads provider variables, the default service-account directory,
kubeconfig, or ambient `load_incluster_config()`. A token rotation between
identity parsing and client binding therefore fails closed instead of
decoupling identity from Kubernetes authentication.

The action reads the token-bound Pod by claimed namespace/name, requires its
live UID to match, and follows the immutable controller-owner chain
Pod -> ReplicaSet -> Deployment, checking every name and UID. From that
authenticated API Deployment's chart-owned literal `SKYPILOT_RELEASE_NAME`,
literal `SKYPILOT_API_SERVER_ROLE`, fixed name, and Helm instance label it
mechanically discovers the complete writer topology. Compatibility mode
requires exactly the API Deployment with role `all` and rejects separate
controller or executor Deployments. HA mode requires the same API Deployment
with role `api` plus the exact sibling controller and executor Deployments with
roles `controller` and `executor`. Their images may be independently
configured, but every live immutable digest must equal the API digest before
activation.

Every discovered Deployment and Pod container must also carry the literal
`SKYPILOT_API_REQUEST_BACKEND=postgres` and literal
`SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS=true`. The latter is an
explicit protocol-v2 preparation mode; its chart value defaults false so
existing PostgreSQL plugins retain their compatibility contract unless an
operator prepares this feature. Schema 008 existing in the shared
PostgreSQL catalog is not by itself proof that request execution uses it: a
release inheriting the chart's SQLite request-store default must fail
activation and demotion attestation. Protocol v2 therefore cannot activate
until the one-way request-store cutover has completed for the entire writer
fleet. The literal environment value is necessary but not sufficient because
a server plugin can replace the request storage or queue factory after process
startup. Every server-instance lease therefore also records the fully qualified
runtime types of the resolved storage backend and queue factory, plus an
`execution_quiescence_capable` bit that is true only for the exact built-in
PostgreSQL storage and queue implementations. Activation and demotion require
the expected type identities and true capability from every API, controller,
and executor lease. A plugin override remains available to protocol-v1
deployments while preparation mode is false. Preparation mode and an already
active protocol-v2 gate both require the built-ins. Plugins load independently in MAIN, UVICORN,
and EXECUTOR process contexts, so every one of those contexts also validates
the exact built-in PostgreSQL storage and queue immediately after plugin
installation and exits before accepting or executing work on any mismatch.
The MAIN lease is rollout evidence; it is not treated as evidence about a
different process context. An executor child adopts the main process's clean
server-environment snapshot into both its process snapshot and its fresh-child
`os.environ` before loading or validating plugins, so a request-scoped
environment mutation at lazy worker spawn cannot suppress the PostgreSQL check
and then leave a custom backend installed.

The action reads every discovered Deployment, every Pod in the release
namespace, and every recent `all`/`api`/`controller`/`executor`
server-instance lease in the shared PostgreSQL database twice. Each Deployment
must retain the same
generation/resourceVersion/UID and exact pod UID/resourceVersion cohort. Every
Deployment controller must have observed its generation; desired replicas
must be positive; current, updated, ready, and available counts must all equal
desired; and unavailable must be zero. Every selected Pod must be
non-terminating, Running, Ready, carry the same chart/release/role identity,
and have its fixed `skypilot-api`, `skypilot-controller`, or
`skypilot-executor` container ready at one common immutable imageID digest. A
nonterminal same-release database migration Pod or an unattested same-release
request-serving or writer Pod blocks activation.

The database inventory closes the independently deployed and cross-namespace
process hole: every recent `all`, `api`, `controller`, or `executor` lease,
including unready or draining leases until the request backend's full stale
horizon expires, must map one-to-one by role, Pod name, and Pod UID to the
attested Pods. The server instance ID must itself equal that Downward-API Pod
UID, and the runtime backend capability fields must retain their expected
values. Thus an old process in another release or namespace, or a process whose
plugin replaced a request backend, blocks activation even though the
token-bound Kubernetes Role cannot enumerate or inspect it. Both complete
reads include the Deployment, Pod, database-process, and resolved-backend
identities and must be identical.
All Kubernetes reads use the bounded timeout and the one no-refresh client.
The exact token-bound Pod name and UID must be present in both verified API
cohorts; caller arguments, Downward API environment, and generic `HOSTNAME`
never supply activation identity.

The singleton has dialect-portable database checks requiring proof fields to
be all-null or all-present and requiring every v2 row to carry a structurally
valid complete proof. The action records the common digest, canonical
API/controller/executor Deployment generation/UID inventories, and deterministic
combined pod/process inventory count/SHA-256 while atomically advancing
`reserved_fill_protocol_state`. Until that durable gate is v2, a multi-context
poller withdraws normalized claims, feeds zero fill, and reports the exact
activation error. This mechanically separates the complete writer rollout
from feature activation.

On its first protocol-v2 heartbeat, a new controller atomically adopts its
shadow. Under the exact existing global broker lock, and only after acquiring
that lock (never while holding a database transaction), one owner-fenced
transaction:

1. locks the service owner, including `resource_scope`, and the service
   claim-set row; protocol v2 requires the scope to be nonempty and equal the
   expected service hash before publishing any edge. An owner-valid unscoped
   or mismatched service atomically withdraws its prior normalized edges,
   claim-set row, and legacy projection so it cannot absorb pool grants it is
   unable to launch, and its in-process grant cache is cleared;
2. retains the existing generation for an identical semantic heartbeat, or
   allocates a new globally monotonic generation from the protocol singleton
   for every semantic change (including edge removal and disable/re-enable);
3. writes the authoritative complete edge set for that generation;
4. deletes normalized edges absent from the new set;
5. writes the service-global budget and utilization state; and
6. writes the stable first edge as the legacy projection.

Readers select by the durable protocol gate, never by opportunistic fallback:
v1 reads only legacy rows; v2 reads only complete, unexpired
`authoritative_v2` sets whose normalized rows match the declared generation
and edge count. A missing, shadow, corrupt, or expired set under v2 contributes
no claim and feeds zero. The two representations are never unioned. Legacy
heartbeat, move, or delete activity therefore cannot invent a second edge
after v2 adoption, and a migration shadow cannot mask a fresher legacy
heartbeat. The compatibility projection always retains the protocol-v1
context-plus-accelerator pool key; it never copies the normalized
physical-UID key.

Disable and teardown delete the set, all normalized edges, and the legacy row
in the same owner-fenced transaction. Reconciliation of a removed pool deletes
that normalized pair and advances the complete set generation. Prune, overlap
rejection, and deletion take the same global broker lock before their
transaction and preserve the representation invariant. A confirmed
protocol-v2 phantom retains its edge and generation while publishing zero
authority, as described below. The lock order is always broker lock, then
database transaction/row locks; no path reverses it or holds the lock during a
caller-owned transaction.

For historical revision 035, the compatibility projection was transitional.
Its stacked cleanup gate required every production process to use revision 035,
every live fill service to have normalized claims, the then-required multi-pool
canaries to pass, and that historical old-image rollback window to close. This
paragraph is audit history, not a rollback or canary requirement for the
current Serve041 fix-forward correction.

### Service-global budget partition

Before publishing claims, the poller reads replica rows once and attributes
fill holdings to pools by their complete persisted origin tuple (pool key,
immutable launch generation, and physical UID). An older positive launch
generation remains valid while it is no newer than the service's current
generation. If any origin field is present, a partial tuple, unknown pool,
future generation, UID mismatch, or placement outside the claimed pool fails
closed and receives no holding or scale-down shelter. Exact location matching
is used only when all three fields are absent on a genuinely legacy row.

Let `H = max(0, max_replicas - demand_target)`. Utilization gating is advanced
exactly once per service heartbeat and persisted on the service claim-set row,
using the existing dwell, bounded-step, boot-hold, blindness, and actuation
rules. It produces one global utilization ceiling `U`; with the gate disabled,
`U = H`. The allocatable budget is `G = min(H, U)`. Edge broker rows carry no
independent utilization governor. A deterministic pure allocator produces
edge caps and floors with these invariants:

```text
sum(edge_cap) <= G <= H
sum(edge_floor) <= min(service_floor, G)
edge_floor <= edge_cap
sum(edge utilization entitlement) <= U
```

The exact allocation algorithm is:

1. Iterate pools in stable task-resource order. Give each pool
   `min(holdings, remaining G)` first. If holdings exceed `G`, later pools lose
   cap first; the global max/headroom reduction always wins over blackout
   stickiness.
2. Define a pool's capacity hint as follows. A successful round no older than
   the configured staleness limit contributes `holdings + last_observed_free`.
   A never-observed, launchable pool contributes `holdings + 1`, permitting a
   one-slot discovery probe. A stale/blackout pool contributes
   `max(holdings, previous_edge_cap)`, clipped by remaining `G`. A benched pool
   contributes holdings only.
3. Distribute residual `G` by equal-weight capped water filling up to
   `max(0, hint - assigned)`. Integer remainder goes to stable pool order.
   Capacity beyond all hints remains unallocated; it is not guessed. After a
   successful probe, the next heartbeat may expand that pool from its measured
   hint.
4. Set `F = min(service_floor, G)`. In stable order assign
   `edge_floor = min(edge_cap, remaining F)` until `F` is exhausted. Thus the
   existing/first context remains the primary floor holder and overflow moves
   only when that edge cannot carry it.

The utilization sample and release clock exist only on the service-set row.
Measurement blackout never creates feed, and it cannot preserve authority
above a reduced `H` or `U`: the new service generation immediately invalidates
old grants. A global release step may be computed while a pool is blind, but
the actuation gate prevents another step until the prior cap is reflected in
aggregate holdings; there is no banked multi-step drop on recovery.

The semantic input hash covers protocol, ordered pool identities and shapes,
physical UIDs, edge cap/floor, global `H`/`U`, and policy. An identical
heartbeat retains its generation; any semantic change draws the next value
from the singleton's global counter. Disable does not reset that counter, and
same-name service recreation therefore cannot reuse an old generation.
Heartbeat-only or round-local holdings/feed changes do not advance it.

### Broker isolation

Claims, allocation lookups, grant cache entries, reconciliation removal, and
prune reports use `(service_name, pool_key)` identities. Pool rounds remain
keyed by `pool_key`, and their JSON grants/feeds remain service-keyed because a
service has at most one edge in a pool. Each published service entry also
carries the exact authoritative service generation used to compute it.

For every pool `p`:

```text
sum(feed[p, service]) <= observed_free[p]
sum(feed_by_accelerator[p, service].values()) <= feed[p, service]
grant[p, service] <= edge_cap[p, service]
```

When the provider reports an exact-card split, the broker validates that it
sums to the aggregate observation and contains only cards in the physical pool
identity. It deterministically partitions the service feeds over that split
and fences an exact-card-only redistribution by advancing the pool epoch. A
malformed present split is a measurement blackout; malformed persisted shaped
feed authorizes zero launches. Rounds written before the nullable field exists
retain their aggregate compatibility behavior.

Measured capacity may override a zero-cost placement bench only after the
measurement belongs to the canonical observation repository and its generation
and admission sequence are referenced by a successfully published broker round.
Provider-query results never update a controller-local placer. A writer that
loses its observation lease, or whose result is malformed, phantom, or
blackout, supplies no placement evidence. Every peer rereads the same immutable
observation and applies it only to the exact `FillPoolSpec.locations` whose
physical UID/card identity matches the round. Capacity from one physical pool
or accelerator card therefore cannot release a bench in another.

While `LEGACY_ACTIVE`, the compatibility adapter retains the historical
`$skypilot-observed-free-v1` entry inside `feed_by_accelerator` for old readers.
After promotion, the round has explicit observation generation/sequence
provenance and readers use the canonical repository; the reserved JSON key is
not a second cache or authority and is deleted by the stacked cleanup PR. The
aggregate legacy `last_observed_free` and `last_observed_free_ts` fields remain
pre-activation compatibility projections only. New freshness uses the repository's conservative
database `observed_at`/`valid_until`, never a post-query timestamp.

Service shaped feed and observation provenance are validated independently. A
malformed service entry retains protocol-v2 fail-closed zero-launch behavior.
Missing, malformed, stale, or UID/card-mismatched observation provenance
suppresses measured bench release and launchable feed without invalidating
non-launching shelter. Exact-card observations accept only canonical cards,
nonnegative integer counts (never booleans), no case-folded duplicates, and a
sum equal to the persisted aggregate. A raw measurement change above an
unchanged service cap does not churn the pool epoch; any service feed/card,
claim generation, lease, or positive-authority blackout change retains the
existing epoch fence.

A failed, stale, benched, or phantom pool feeds zero only for that pool. It
does not erase another healthy edge of the same service. A pool epoch change
fences only launches stamped for that pool.  Every decision stamped with a
superseded epoch fails closed before location selection and row persistence,
even when a newer round currently has positive feed for the same service,
generation, physical UID, or accelerator card.  The final row persist retains
its atomic epoch/generation/UID/owner checks for a round that changes after the
early check.

Provider `snapshot_time` and replica `created_at` are wall-clock observations,
not a durable admission order.  They cannot prove whether the broker's later
row scan included a replica, and clocks on different controllers may disagree.
Consequently no launch may adopt a newer epoch by subtracting rows ordered by
those timestamps. The final correction supplies the PostgreSQL-backed
`zero_cost_admission_sequence` specified below. Every fill and ordinary
zero-cost acceptance increments that singleton counter in the same transaction
as its row under the shared reservation boundary, and every observation carries
its sequence into the broker round. The committed sequence defines debit; wall time only expires
authority. Strict pool epoch
fencing remains unchanged, so the ledger does not authorize a queued decision
to adopt a newer epoch.

After the existing consecutive-observation threshold confirms a protocol-v2
phantom, the broker publishes explicit zero grant/feed authority for that pool
but retains the complete normalized claim set and its service generation. It
must not remove the edge mid service poll: doing so would advance the global
service fence, invalidate sibling rounds already driven in that poll, and let
the next configured heartbeat re-add the edge in an endless generation-churn
loop. Protocol v1 retains its legacy claim-removal behavior.

If driving one protocol-v2 round raises or times out, that edge publishes feed
zero, launch grant zero, and no epoch. It may retain only its prior grant from
the same pool key and physical UID, at the same or an older service generation,
clipped to the current edge cap, as non-launching `shelter_grant`; a peer
pool's successful round remains usable. A future prior generation, changed UID,
or absent pool carries nothing. The complete-map swap removes deleted edges,
while the UID prevents a replacement physical cluster from inheriting shelter.
Thus a headroom or policy change may invalidate all launch authority without
turning a transient first-round provider timeout into destructive teardown of
healthy holdings.

Consecutive failed forward generations may relay that same shelter, but each
relay is non-increasing and re-clipped to the current edge cap; shrinking to
zero cannot regrow when a later cap expands. This is an intentional
availability-first failure mode for already-materialized zero-cost holdings:
it can delay their retirement while provider proof remains unavailable, but it
cannot authorize a launch. Disabling fill or losing the live placer withdraws
the claim and clears the process-local pool map even when the durable removal
must be retried. A later same-key re-add therefore starts with zero shelter.
Malformed live or restored protocol, generation, physical UID, composite pool
identity, cap, feed, grant, shelter, or epoch supplies no edge authority.

Round freshness is conditional on generation equality. If the caller's claim
generation is absent or differs from `claim_generations` in an otherwise fresh
round, the broker must drive a new round; it may not return the old grant. A
protocol-v2 pool always publishes an integer grant capped by its edge cap,
including the one-claimant case. The historical `grant=None` fast path exists
only in protocol v1.

### Autoscaler state and actuation

The final `ReservedFillPlanner` stores immutable per-pool snapshots in a map
keyed by `pool_key`; `Autoscaler` supplies only the ordinary traffic-demand
plan. Each `PoolFillState` owns its locations, physical UID, authoritative
service generation, partitioned edge cap, raw and damped feed, optional
service-specific exact-card feed, observation generation/sequence/timestamp/validity,
grant, and epoch. The allocation assembler publishes a complete same-generation
map atomically. Free-slot increase damping, staleness, and occupancy debit run
independently per pool; planning never spends feed. Only the durable accepted
rows returned in `FillCommitResult` become occupancy. A service-generation
change atomically invalidates every old pool feed and launch grant. A pool
remains at feed zero until a round carrying that exact generation arrives, and
its local grant is always clamped to its edge cap. Non-launching shelter is
independent: on a failed first round, an unchanged pool key and physical UID
may carry the older shelter forward as described above. A complete-map edge
removal and an operator disable are lifecycle boundaries: both delete its
process-local shelter, so re-creation cannot inherit the removed claim's
entitlement.
Live ingestion and dynamic restore accept authority only when the composite v2
key is canonical and its physical UID, finite timestamp no more than one stale
window in the future, and nonempty Kubernetes location set form one exact
identity. Every edge uses one context;
every location has one positive whole-GPU shape; the location cards exactly
cover the key's canonical cards at one width; and any shaped feed is a subset
of those cards. A complete map uses each context once and keeps accelerator
sets pairwise disjoint for aliases of one physical UID. Any live feed or grant
also carries a positive epoch. Invalid live snapshots are rejected; invalid
restored edges are omitted, while a cross-edge or mixed-generation restore
conflict drops the complete map instead of choosing authority by serialized
order.

Existing aggregate `fill_free_slots`, `fill_snapshot_age`, and `fill_target`
status fields retain their exact compatibility meanings:
`fill_free_slots` is the aggregate damped broker feed, not raw provider
capacity and not the final spendable count. Additive raw-observed and spendable
fields plus per-pool status are reported separately. A legacy dynamic-state
dump is admitted as one anonymous pool only when it represents a single
context; ambiguous state restores locations for scale-down protection but
grants no launchable feed.

Protocol-v2 dynamic state persists only each pool's last real grant as a
`shelter_grant`. Loading it restores conservative scale-down shelter but always
sets feed to zero and epoch to absent, so a controller restart cannot replay a
launch entitlement. A fresh generation-matching broker round is required
before scale-up resumes.

Scale-down shelter is pool-local. A pool may shelter only zero-cost rows
attributed to that pool, up to its live target or restored `shelter_grant` and
after the global demand coverage attribution described below. Demand-placed
rows remain ordinary demand first, but may become opportunistic shelter when
demand falls, matching protocol-v1 behavior. Aggregate `max_replicas` remains
the last launch guard.

Every typed `FillIntent` carries:

- its protocol version;
- its pool key;
- its pool round epoch; and
- the exact pickleable location set belonging to that pool.

Protocol-v2 decisions additionally carry the authoritative service generation,
physical-cluster UID, and an exact accelerator shape when exact-card telemetry
is available. Protocol v1 retains its legacy context-key authority and does not
claim normalized generation or physical-UID provenance.

The replica manager consumes those typed fields before constructing
`Resources`, intersects them with current active zero-cost locations, and
skips with no row or thread if the intersection is empty or does not match the
pool. During the bounded protocol-v2 transition, the compatibility adapter
maps each typed intent to the old override shape; when exact-card metadata is
present, that adapter carries the
measured card and its exact per-replica GPU count; the manager independently
requires both the selected location and final persisted resource override to
match that shape. It then re-reads the physical UID through that context. It
never falls through to a different zero-cost context or paid capacity. The
durable request-submission state machine also carries an early protocol-v2
guard. Immediately before every API request commit, after any admission delay,
that guard again
requires the exact pinned Kubernetes context/card/count and force-refreshes
the context's physical UID. It prevents a known-stale request from being
enqueued; it is not provider-actuation proof. The durable tuple and
provider-fenced executor path above remain authoritative across queueing,
request recovery, policy/optimizer changes, and Kubernetes client refresh.
Protocol-v2 fill is admitted only when the service's nonempty durable
`resource_scope` equals its service-incarnation hash; its replica cluster name
must be the deterministic name for that exact service scope and replica ID.
This makes the cluster name an incarnation identity rather than the reusable
legacy `{service}-{replica}` name. Because protocol selection is global, an
existing pre-scope service has reserved fill inactive after protocol-v2
activation until it is recreated with an incarnation scope; it cannot keep
emitting protocol-v1 fill independently.
The final correction replaces the ambiguous interrupted-row contract with the
durable intent states `ACCEPTED`, `REQUEST_BOUND`, `SUBMITTING`,
`WAITING_CAPACITY`, `INITIALIZING`, `OBSERVATION_UNKNOWN`, `READY`,
`CANCEL_REQUESTED`, `CLEANING`, `TERMINAL`, and
`EFFECT_RECOVERY_REQUIRED`. `ACCEPTED` means the fenced replica row exists,
owns a durable FIFO ordering ticket, and counts against
feed/headroom/`max_replicas`, but no provider effect is possible. There is no
replaceable dispatch-lease state: a nonlocking candidate read either loses its
single atomic `ACCEPTED -> REQUEST_BOUND` transaction or commits the complete
request binding. `REQUEST_BOUND` binds one immutable `PENDING` API
request with a five-second database-time claim deadline; it holds no provider
mutation permit and cannot be rebound. Only the atomic executor-claim
transition acquires a non-pool-scope mutation permit, assigns the exact execution
generation, changes the request to `RUNNING`, and enters `SUBMITTING`.
`WAITING_CAPACITY` means the exact Kubernetes object UID is durable but the Pod
is unbound; `INITIALIZING` means it is bound and startup/readiness is in
progress. The last two states hold neither a non-pool-scope provider-mutation permit
nor an API long worker.

A dead `ACCEPTED` row remains eligible for the same atomic binding only while a
transaction proves that no API request is associated with the intent. After
`SUBMITTING`, process-lease expiry is never absence proof and the row can never
return to `ACCEPTED`, bind another launch request, or retry a create. Recovery
uses the exact request ID plus deterministic intent label/object name:
it adopts that same live request or exact object UID and writes
`EFFECT_RECOVERY_REQUIRED` on effect ambiguity. If the exact worker is
quiesced, the request is terminal, and provider absence is proved, the row
becomes `TERMINAL`; only fresh capacity authority may create a new intent row.
A crash after object creation but before UID persistence discovers and adopts
that exact object by
the immutable intent label. `WAITING_CAPACITY` and `INITIALIZING` always
observe the same request/object. This proves at most one provider start, not an
unqualified exactly-once execution claim.

Cancellation or replanning may terminalize `ACCEPTED` and still-`PENDING`
`REQUEST_BOUND` rows directly under the scheduler/request CAS. `SUBMITTING`,
`WAITING_CAPACITY`, and `INITIALIZING` require exact request cancellation,
exact-UID cleanup, observed provider absence, and the execution-quiescence
barrier below before becoming `TERMINAL`. Any subsequent launch attempt is a
fresh intent under fresh authority. `READY` uses the same fenced drain/cleanup
contract.

The legal transition table is exhaustive:

| State | Required durable fields/effect status | Legal next state |
| --- | --- | --- |
| `ACCEPTED` | Immutable authority/allocation tuple, intent id, and FIFO ordering ticket; no request, permit, or provider effect | `REQUEST_BOUND` only by one atomic request-binding transaction, or `TERMINAL` when superseded/expired |
| `REQUEST_BOUND` | One immutable `PENDING` API request and claim deadline plus a prospective-lane reservation; no mutation permit/effect | `SUBMITTING` only by atomic executor claim; or `TERMINAL` if claim deadline, authority, or validity loses before claim; never another request binding |
| `SUBMITTING` | Immutable idempotency/object identity, exact API request ID/execution generation/process boot nonce, and active scheduler mutation ticket; effect possible | `WAITING_CAPACITY` or `INITIALIZING` on definitive create; `CANCEL_REQUESTED` only after definitive create identity plus a carried cancel-after-submit flag; `TERMINAL` on quiesced definitive no-effect; or `EFFECT_RECOVERY_REQUIRED` on ambiguity; never `ACCEPTED` or a new launch binding |
| `EFFECT_RECOVERY_REQUIRED` | The same immutable request/object identity, provider-neutral mutation receipt, and mandatory concurrency-debt ticket; worker or provider effect unknown | Adopt a definitive create into `WAITING_CAPACITY`, `INITIALIZING`, or `CANCEL_REQUESTED` when cancel-after-submit is set; adopt a definitive delete into `CLEANING`; or `TERMINAL` only after exact worker quiescence, terminal request, and provider absence |
| `WAITING_CAPACITY` | Exact request and Kubernetes object UID; unbound; no mutation permit after proved submit completion | `INITIALIZING`, `CANCEL_REQUESTED`, or `OBSERVATION_UNKNOWN` if exact object observation is temporarily unavailable |
| `INITIALIZING` | Exact bound object UID/request; startup/readiness in progress; no mutation permit | `READY`, `CANCEL_REQUESTED`, or `OBSERVATION_UNKNOWN` |
| `OBSERVATION_UNKNOWN` | Exact durable object UID/request plus immutable `resume_state` in `WAITING_CAPACITY`, `INITIALIZING`, or `READY`; no mutation permit or concurrency debt | The exact `resume_state` after a successful UID-fenced read; `CANCEL_REQUESTED`; or `EFFECT_RECOVERY_REQUIRED` only if a later cleanup/cancellation effect becomes ambiguous |
| `READY` | Existing live replica lifecycle plus immutable origin tuple | `OBSERVATION_UNKNOWN` on UID-fenced observation loss, or `CANCEL_REQUESTED` under existing scale-down/preemption policy |
| `CANCEL_REQUESTED` | Exact object target and cancellation generation, with either no request or one immutable `PENDING` cancel/cleanup request and prospective deadline; no effect without guarded claim | `CLEANING` only by atomic guarded claim, or `TERMINAL` on proved pre-effect absence |
| `CLEANING` | Exact UID-addressed cancellation request/receipt; active ticket only during its bounded call, then no ticket while a definitive accepted deletion is observed to absence | `TERMINAL` on UID-fenced absence; `CANCEL_REQUESTED` at the next generation after a quiesced definitive no-effect retryable result; or `EFFECT_RECOVERY_REQUIRED` on ambiguity |
| `TERMINAL` | No possible provider effect; ticket released | none; fresh authority creates a new intent/row |

All transitions CAS an immutable intent ID plus monotonically increasing
`intent_state_version`. Authority, allocation, launch idempotency, launch
request ID/execution generation, and object identity fields can only move from
null to their one proved value; they never change in place. Cancellation and
cleanup API rows are immutable per monotonically increasing
`cancellation_generation`. A later generation may be created only after the
prior request is terminal, its exact worker is quiesced, and its receipt proves
that no provider effect occurred; an ambiguous generation is recovered in
place and never skipped. Ownership lease fields are the only replaceable
fields within one mutation generation. `EFFECT_RECOVERY_REQUIRED` has no
timeout-based escape. `OBSERVATION_UNKNOWN` retains occupancy but never mints
concurrency debt merely because a known object cannot be read.

The `ACCEPTED -> REQUEST_BOUND` PostgreSQL transaction locks the gate/
sequencer, exact calling authority-process boot lease/ack, lifecycle/service
owner, scheduler cursor/ticket, replica intent row, then API request row. It
creates the
deterministic request in durable `PENDING`, binds its request ID and five-second
claim deadline to the intent, and only then makes it visible to the existing
request queue. A crash before commit leaves the unchanged `ACCEPTED` row and no
request/effect. A crash after commit leaves one discoverable `PENDING`
request that can only be claimed through the guarded transition below; it never
creates a second request.

The guarded `REQUEST_BOUND -> SUBMITTING` claim transaction follows the same
row order with the executor's exact process boot lease. It rechecks gate/ack,
owner, ticket turn, authority, PostgreSQL freshness, claim deadline, and
its live prospective reservation under
`active_mutations + live_prospective + debt <= C`; atomically changes the API request to
`RUNNING`, assigns its one execution generation, converts the prospective lane
to an active mutation ticket, and changes the intent state. The provider call
cannot begin before that commit. A nonlocking API-queue candidate read is only
a hint; the queue holds no request-row lock before entering this canonical
transaction. API009 database enforcement rejects a direct
`PENDING -> RUNNING` claim for a sequenced reserved-fill request unless the
same guarded operation converts its exact intent/prospective lane. If the claim
deadline or authority loses first, the request becomes ineligible for claim by
the database-time predicate even if cleanup has not acquired its locks. Every
claim samples `clock_timestamp()` after all lock waits and immediately before
commit; API009 enforcement rejects a late direct claim. `live_prospective`
therefore means exactly `REQUEST_BOUND`, request still `PENDING`, and
`clock_timestamp() < claim_deadline`. At the deadline the lane ceases counting
against `C` without relying on a process wakeup. The existing PostgreSQL queue
expiry sweep polls once per second and CAS-marks the request cancelled plus the
intent `TERMINAL`; its healthy-control-plane terminalization SLO is 15 seconds,
but it is cleanup rather than claim authority. A paused or lock-delayed sweeper
can leave an observable expired tombstone and temporarily retain replica
headroom, but cannot permit a late provider effect or occupy a mutation lane.

Guarded claim, higher-priority reclaim, and expiry cleanup use nonblocking
canonical row acquisition (`NOWAIT` for an all-or-rollback claim/reclaim and
`SKIP LOCKED` for cleanup). They never wait for a later intent/API row while
holding the scheduler singleton. A conflicted claim rolls back all earlier
locks and retries only while its database deadline remains; the final commit
predicate resamples `clock_timestamp()`. Thus lock contention across the
deadline cannot extend a live prospective lane or produce a late claim.

At most `max(0, C - debt - active_mutations)` live prospective
`REQUEST_BOUND` lanes exist at once under the scheduler singleton, preserving
`active_mutations + live_prospective + debt <= C`. Queue
activation, executor claim, and provider-object discovery all retain the same
immutable binding.
`active_mutations` and `debt` are a disjoint database-time partition of
effect-capable tickets: a ticket with no definitive receipt at or after its
result deadline counts only as debt, regardless of whether the materialization
sweep has rewritten its state.

The controller classifies every such row from its durable pool key, service generation, and
physical-cluster UID. A complete, internally consistent protocol-v2 tuple uses
the strong PostgreSQL history barrier; a genuine protocol-v1 tuple with no
physical UID and its historical null/zero service generation uses the
compatibility active-request barrier; any partial or contradictory tuple fails
recovery closed. Mixed waves run both barriers before any row is torn down. The
controller batches each partition, snapshots every relevant API launch request
for those exact replica clusters, and retains the exact request IDs through
cancellation. A terminal request status alone is not
quiescence: PostgreSQL cancellation publishes `CANCELLED` before the remote
executor necessarily observes its heartbeat and stops the handler. Revision
008 therefore adds `execution_quiescence_required` plus nullable
`execution_quiesced_generation` and `execution_quiesced_at` request columns.
Old rows and inserts from API007 writers default to not required at the
PostgreSQL server; the canonical SQLAlchemy schema retains that server default
so schema-created compatibility tables and raw inserts have the same contract.
Every version-70 queue claim sets required and clears the prior receipt. This
avoids treating legacy signal delivery as proof without retaining all
pre-version-70 request history forever. The
generation field is the durable proof that one exact request execution has
stopped running effect-bearing handler code; the existing legacy
`cancel_acknowledged_at` signal-delivery timestamp is explicitly insufficient.
Cancellation may publish quiescence
atomically only while its locked row is still `PENDING` and has the matching
durable delivery, because the terminal transition then prevents
`try_mark_running` from winning. `WAITING` is not sufficient: a broken shared
process pool can publish and requeue `WAITING`, clearing its claim/worker,
before every surviving handler is known stopped. `WAITING` therefore requires
an existing exact wrapper receipt. A `RUNNING` row is never inferred quiescent
from a null PID. Otherwise the owning executor publishes the proof, fenced by
request ID, execution generation, claim token, and worker instance. The
executing worker publishes it
only at the end of its own effect-bearing cleanup, for any terminal outcome.
The monitor may retry that write after `Future.result()` returns normally or
after it receives the wrapper's explicitly serialized `ExecutionRetryableError`;
the latter fallback runs before the monitor changes the row to `WAITING` or
requeues it. Both outcomes prove that exact wrapper invocation returned. The
monitor must not acknowledge `BrokenProcessPool` or another ambiguous Future
exception: CPython can break unrelated pending futures before every process in
the shared pool has exited, and this pool deliberately has no safe task-to-PID
map. A claimed version-70 request is never automatically requeued after
`BrokenProcessPool`, regardless of its ordinary retry flag. Requeueing would
create a second execution generation whose later receipt could mask the still-
running first generation. The request instead becomes terminal-but-unproven,
and recovery retains the replica until the exact old invocation is reconciled.
Ordinary capacity retries raised by a live wrapper remain unchanged.

The receipt proves completion of the exact wrapper/handler invocation, not
exit of its reusable ProcessPool PID. Durable handlers must not detach
effect-bearing Python threads; cancellation runs the wrapper's child-process
cleanup before it publishes the receipt. The worker's SIGTERM interruption
gate is one-shot and exact-claim-addressed. Before signalling, the owning
executor creates a mode-0600 cancellation marker whose path includes a digest
of the PID, request ID, execution generation, and claim token. The active
wrapper independently derives and holds that path. Its signal handler raises
`KeyboardInterrupt` only when the marker for its current invocation exists,
atomically consumes that marker, and disarms the gate first. The sender first
verifies through the local process table that the PID is a titled ProcessPool
executor direct child of the current server process and refuses a PID already
observed as unrelated. This matches production, where the short and long
RequestWorker dispatchers are threads rather than OS processes. It then holds
the exact request row lock across
marker creation, `os.kill`, and the signal-delivery write, and does not signal
an invocation that already has its exact receipt. A successful wrapper receipt
takes that same lock before its
Future can return and its ProcessPool PID can be reused. Thus a wrapper-first
race normally observes the receipt and sends no signal, while a sender-first
race keeps generation A in its wrapper until signal delivery commits. If the
receipt write fails or the worker crashes, the exact marker remains the fence:
a generation-A marker is ignored by a replacement worker running generation B.
A non-SkyPilot process could theoretically acquire the PID between the process-
table check and `os.kill`; closing that operating-system TOCTOU completely
would require persisting a process birth identity or using a Linux pidfd. That
pre-existing, same-container race is outside this rollout's exact SkyPilot-
worker reuse guarantee.
A duplicate signal during A's cleanup is ignored. The next invocation installs
its distinct path immediately before handler execution, and wrapper exit clears
only its own marker. Marker removal is best effort: an unlink error cannot turn
an otherwise completed wrapper Future into an ambiguous failure and suppress
its receipt; claim-addressed paths make a residual marker inert and bounded-age
pruning removes it later. Receipt publication is additionally guarded by an
explicit wrapper-local completion flag. Cancellation sets that flag only after
child-process cleanup returns successfully; cleanup failure leaves it false
and the exceptional Future cannot publish the monitor fallback. Tests cover delayed
PID reuse, locked sender/wrapper ordering, a duplicate while cleanup is blocked,
and cleanup failure. Literal retirement of every reusable worker process is
neither required nor claimed.

Before cancellation the controller captures each retained exact request ID and
`execution_generation`. It polls those IDs including terminal rows, and
requires every terminal target to report
`execution_quiesced_generation == captured_generation`. This also covers a
handler that wins the terminal race with `SUCCEEDED` or `FAILED`; status alone
never proves quiescence. The generation proof is a mixed-rollout capability
fence: an old executor may still write the legacy cancellation timestamp, but
cannot populate the new column. A missing row, null/mismatched generation, an
old server response without the fields, an unexpected identity/status,
timeout, transport error, or ownership change is uncertainty: the durable
replica rows remain and the recovery pass retries.

An active-only discovery is insufficient because another controller or caller
can already have published `CANCELLED` while the remote handler still runs.
Interrupted-fill recovery therefore uses an additive API-version-70,
POST/body-backed status filter that reads all request statuses for the whole
set of validated incarnation-scoped replica cluster names in one PostgreSQL
query. The server itself applies the exact `sky.launch` request-name allowlist;
callers cannot supply arbitrary internal request names. This internal mode is
accepted only from the current PostgreSQL controller generation proven by the
server's existing origin middleware (or a loopback compatibility caller), is
absent from the legacy GET endpoint, and uses a scalar PostgreSQL projection.
Unrelated request history and large launch payloads are therefore neither
transferred nor decoded.
There is deliberately no request-age cutoff: wall-clock age is not execution
or identity proof, and every retained required/unproved request for the exact
incarnation-scoped cluster name remains relevant. The body transport also
carries the retained exact request IDs during polling, so
large waves cannot overflow ingress URL/header limits or degrade into one
database query per ID; exact polling uses one primary-key `IN` query. Revision
008 adds a partial `(cluster_name, status)` index only for receipt-required
rows whose exact generation is still unproved; the query combines those rows
with the existing active-status index instead of scanning either unrelated
terminal history or the growing set of successfully proved requests. It
captures every matching launch generation,
cancels the active subset, and requires the generation proof from both that
subset and matching requests that were already terminal.
The API request retention predicate keeps any required terminal generation
whose quiescence proof is null or mismatched, so request GC cannot erase the
only evidence while its handler may still mutate; pre-version-70 rows remain
subject to the existing retention policy. Comprehensive discovery ignores
non-required terminal legacy history but requires the bit and exact receipt for
every active protocol-v2 target. This history mode is required for
protocol-v2 interrupted fill; compatibility teardown retains active-only
discovery for local/SQLite and pre-version-70 deployments.
Only after this barrier completes and a fresh active-request scan remains empty
does recovery recheck provider inventory, schedule immediate teardown for the
whole batch, and let the broker refill opportunistic slots under fresh
authority. Ordinary demand recovery is unchanged.

The selected replica persists `reserved_fill_pool_key`,
`reserved_fill_service_generation`, and
`reserved_fill_physical_cluster_uid`; the final transaction verifies that
the carried and round protocol equal the durable gate, plus the authoritative
service generation, matching live composite claim, round generation, pool
epoch, and selected physical identity. A protocol-v1 decision queued
immediately before v2 activation (or a v2 decision queued before demotion)
therefore cannot persist afterward.

The advisory lock is not itself treated as a durable fence: PostgreSQL may
drop its dedicated session while the process still believes the lock is held.
Before a fill-row persist uses its ordinary ORM connection, it advances the
existing global lease epoch on the exact advisory-lock session and carries that
token into the replica transaction. The transaction locks and validates that
epoch before inserting. Every replacement round advances the same epoch before
its replica scan. If the persist transaction locks the token first, the
replacement blocks and subsequently scans the committed row; if the replacement
advances first, the stale persist writes nothing. The persist token does not
refresh lease expiry, which remains evidence of a completed broker round.
The local SQLite/FileLock path retains its historical mutex-only behavior and
does not touch the PostgreSQL lease token. The replica transaction determines
this exception from its database dialect: every PostgreSQL persist rejects a
missing or non-positive token even if distributed-lock auto-detection
transiently returned a FileLock.

This service-generation predicate is the cross-pool fence. Repartitioning
budget from pool A to B invalidates every queued A and B decision from the old
generation before either can persist, even when one pool's previous round is
otherwise fresh.

Demand placement reads a `(service, pool)` grant cache. Saturation excludes
only that pool's zero-cost locations. One saturated context cannot close an
unrelated context that still has entitlement.

Scale-down shelter preserves protocol-v1 behavior unchanged. Under protocol
v2, let `T_i` be each pool target after ordinary decisions reserve their free
slots, and `D` the demand target. Attribute `min(D, sum(T_i))` units of demand
coverage across `T_i` in stable pool order. Pool `i` receives shelter quota
`Q_i = T_i - coverage_i`, proving
`sum(Q_i) = max(0, sum(T_i) - D)`, the existing aggregate v1 surplus. Victim
suppression is performed independently per pool, from the tail of that pool's
ordered zero-cost victims, while preserving the original global output order.

When the existing exact-card demand map is complete, the same equation is
applied independently per exact card before stable pool allocation. When it is
incomplete, the aggregate equation above is used, exactly as v1 falls back to
aggregate shelter. With one pool both paths produce the identical v1 quota and
victim suffix for every origin and victim order; paid rows, fill-origin rows
currently serving demand, and demand-origin zero-cost rows neither add nor
subtract a second copy of demand. With several pools, every `Q_i` can shelter
only victims physically attributed to `i`, so another pool's grant cannot
shelter them.

### Generation-triggered reconciliation and durable intent admission

This section is the canonical contract for the planned large-fleet underfill
correction. It replaces the current lock-coupled observation and speculative
debit behavior; it does not weaken any protocol-v2 identity, conservation,
epoch, or zero-cost placement fence described above.

#### Incident and root cause

The 2026-08-11 production sample separated capacity from launch authority:

- broker generations 4594--4596 published 34 free east A100-80GB slots;
- the controller consumed those publications 181--250 seconds after their
  pre-query source timestamps, so the 180-second freshness predicate made all
  34 slots unspendable;
- no Kubernetes pod was pending and no provider operation was running during
  the empty interval, which excludes slow node initialization, pool RBAC, and
  scheduler capacity as causes; and
- a later 49-decision wave persisted only the prefix admitted before the
  provider-phase busy boundary, approximately the configured global provider
  parallelism. The autoscaler nevertheless debited all 49 decisions from both
  raw and damped feed, making the deferred tail invisible until two subsequent
  increasing polls.

The first defect is a lock convoy. `_run_autoscaler()` holds
`_actuation_epoch_lock` across database reads, planning, readiness work, and
`scale_up_batch()`. `reserved_capacity.poller_loop()` takes the same lock
across owner reads, pool discovery, Kubernetes observation, broker publication,
and map ingestion. Its next deadline is also calculated by sleeping a full 60
seconds after all that work. A slow actuation wave therefore delays and ages
the evidence required to start the next wave.

The second defect is an ownership error at the planner/manager seam.
`_apply_reserved_capacity_fill_v2()` treats emitted dictionaries as completed
admissions, while `scale_up_batch()` returns no accepted-prefix receipt. A
plan is not durable state and must not consume authority. Only a replica row
that passed the final transaction is an accepted fill intent.

#### Final single-path topology

```text
fixed-rate Serve pool observer
    -> immutable durable PoolCapacityObservation
    -> observation-generation notification

demand tick / service update / replica or provider transition
    -> the same ScaleReconcileCoordinator generation notification

ScaleReconcileCoordinator.reconcile_once()
    -> ordinary demand plan + one grouped durable replica snapshot
    -> CAS-publish complete policy/version-fenced claim set generation G
    -> ReservedCapacityAllocationAssembler drives every pool for G
       against committed observations
    -> atomically publish one complete same-G allocation map
    -> ReservedFillPlanner.plan(demand_plan, allocation_map, replicas)
    -> FillPlan
    -> ReplicaManager.accept_reserved_fill(FillPlan)
    -> FillCommitResult
    -> one durable, fair provider dispatcher
```

There is one controller-owned reconciliation coordinator per service. It is
the only loop that combines demand, `max_replicas`, and reserved fill, and it
uses the existing manager mutation boundary for scale-up and scale-down.
`ReservedFillPlanner` is a pure policy module called by that loop, not a second
thread or actuator. `Autoscaler` continues to calculate traffic demand; fill
state, freshness, shelter, and fill-plan construction move behind the planner
interface so their policy can be tested without manager or provider I/O.

Demand-derived policy precedes allocation. For one immutable reconciliation
input revision, the coordinator calculates demand, utilization ceiling,
service-global headroom, edge caps/floors, and current holdings from one grouped
durable-row snapshot. It then CAS-publishes the complete ordered claim set and
one service generation `G`; no old allocation is actuatable after that commit.
The service-local `ReservedCapacityAllocationAssembler` is the sole owner that
drives or reads each cross-service pool round for exactly `G` and assembles the
result. It publishes the map only when every edge is represented at `G`.
A failed or stale edge contributes zero feed/grant plus only the already
permitted same-identity clipped shelter; it cannot omit the edge or invalidate
a healthy sibling. The complete map publication wakes the same coordinator.

`reserved_fill_service_claim_sets` stores the authoritative
`allocation_generation`, `allocation_input_sha256`,
`allocation_claim_generation`, complete `allocation_map`, and
`allocation_published_at`. The input hash covers `G` plus the ordered vector of
each exact pool key, round epoch, observation generation/sequence, and normalized
authority payload. A claim-generation change atomically clears the map. A
capacity-only change may publish a new allocation generation while `G` remains
stable, but at most one map is published for one exact input hash. Every fill
plan carries and revalidates all three allocation identity fields; a map for a
different round vector is not interchangeable merely because its claim
generation matches.

Map publication takes the shared reservation lock, then the broker lock, then
one PostgreSQL transaction in the canonical row order below. It locks the
protocol singleton, calling authority-process boot lease/ack, lifecycle/service-owner
row, service claim-set row, complete ordered edge set, and every expected
pool-round row; rereads the
service incarnation, active version, controller owner, policy revision, claim
generation, reconciliation-gate generation, and exact round/observation
vector; recomputes `allocation_input_sha256`; and CAS-increments
`allocation_generation` only if every value still matches the assembler's
input. Owner takeover is therefore serialized inside the publication
transaction rather than fenced only by an earlier in-process token check.
Missing, added, failed, owner-changed, or advanced rows make the CAS affect zero
rows and wake a fresh assembly. Thus a delayed assembler at the same `G` cannot
overwrite a newer capacity map.

Publication is idempotent under those locks. If the current claim-set row
already contains the same gate generation, claim generation, input hash, and
byte-canonical allocation payload, the assembler returns the stored
`allocation_generation` without an update, generation increment, timestamp
change, or notification. The same input identity with a different payload is
durable corruption and fails closed. Only an absent map or a different exact
current input may publish a new generation. This implements the invariant of
at most one map publication per exact input hash even after lost replies or
controller restart.

A demand, policy, or holdings change therefore takes a bounded two-pass path.
Pass one publishes `G` and returns no fill actuation while the exact map is
absent. Allocation assembly then publishes the complete map, and pass two
plans and accepts fill only if its demand input revision and `G` still match.
A newer change CAS-publishes `G+1`, makes the incomplete or complete `G` map
unspendable, and coalesces into the next pass. There is at most one claim-set
publication per stable demand input and one complete-map publication per exact
allocation input hash; capacity observations may legitimately advance maps
without changing `G`. The coordinator never recursively spins waiting for a
broker round.

There is also one Serve-scoped physical-pool observation repository. It is
not the retired cross-domain capacity control plane from
`unified-physical-capacity-convergence.md`: its only consumers are the
reserved-fill broker, Kubernetes-only Serve placement, and Serve status, and
it owns no allocation or provider action. The current
`demand_capacity_observations` PostgreSQL table is extended instead of adding a
second observation cache. A canonical observation carries:

- physical-cluster UID, canonical accelerator set, and access context;
- a monotonic observation generation and durable per-pool lease token;
- exact-card and aggregate free slots or an explicit blackout/error;
- `observed_at`, captured before the provider query as the conservative row
  debit anchor;
- `completed_at`, used only for polling and diagnostics;
- `valid_until`, derived from `observed_at` and the unchanged 180-second
  authority horizon; and
- `published_at`, used only for end-to-end lag diagnostics.

The observer acquires a durable per-physical-pool query lease, queries
independent pools concurrently through a bounded worker set, and conditionally
publishes each result by lease token and physical UID. It holds no controller,
broker, autoscaler, or manager mutation lock during provider I/O. An expired
writer cannot overwrite a successor. The schedule uses monotonic fixed-rate
deadlines at 60-second intervals; missed deadlines coalesce into one immediate
round and never create a catch-up storm. One slow or blacked-out pool publishes
its own failure and cannot delay a healthy peer's observation. Each Kubernetes
observation call has a 45-second absolute monotonic deadline, including retries
and physical-identity proof. Timeout publishes an explicit blackout, releases
the worker/lease, and authorizes no capacity; `provision_timeout: -1` does not
apply to observation RPCs.

Moving provider I/O outside the broker lock requires a durable commit-order
seam; wall clocks and a bare PostgreSQL sequence are not that seam. The
`reserved_fill_protocol_state` singleton owns an additive BIGINT
`zero_cost_admission_sequence`. At observation begin, one PostgreSQL transaction
takes that singleton `FOR UPDATE`, increments the counter, inserts the
in-progress observation row with that `observation_sequence`, samples
`clock_timestamp()` as `observed_at`, and commits. The observer also holds
`DEMAND_CAPACITY_RESERVATION_LOCK_ID` for this short transaction, then releases
it before provider I/O.

Every ordinary and fill zero-cost replica acceptance takes the same distributed
lock, locks and increments the singleton, and inserts its
`zero_cost_admission_sequence` with the replica row in that same transaction. The
singleton row lock is retained through commit, so counter values serialize commit
boundaries; `nextval()` allocated before a later rollback or out-of-order commit
is explicitly insufficient. Gaps from a rolled-back transaction are harmless.
Paid and non-Kubernetes rows carry no order and are never authorized by this
ledger. While the new reconciliation gate is active, PostgreSQL enforcement
rejects any zero-cost insert lacking the atomic singleton increment/order,
including an unexpected old writer.

After the query conditionally completes the exact in-progress observation by
lease token and physical UID, the broker opens one PostgreSQL transaction,
locks the protocol/sequencer singleton `FOR UPDATE` before its grouped replica
snapshot, and retains that row lock through debit, allocation, and round
publication. Rows ordered
after `observation_sequence` are unambiguously post-observation and are debited
even under host clock skew. Rows ordered before it committed before observation
began; the existing not-ready feed debit and fill-holdings accounting remain
conservative for a pod not yet visible to Kubernetes. No zero-cost acceptance
can commit in the scan-to-publish gap because its transaction must acquire the
same singleton row. The observation lease token still fences two provider
readers; the sequencer transaction fences provider evidence against replica
writers. They have separate responsibilities.

The non-reversing lock order is:

1. provider-phase/physical-UID preflight, when required, with no manager,
   reservation, or broker lock held;
2. `DEMAND_CAPACITY_RESERVATION_LOCK_ID`;
3. the service manager lock for the short replica-ID/row mutation only;
4. `RESERVED_FILL_BROKER_LOCK_ID`, when required; and
5. one PostgreSQL transaction in this canonical row-lock order:
   protocol/sequencer singleton; permanent retired-Pod revocations by
   Pod UID; current authority-process instance projections by
   `(role, Pod UID)`; immutable boot-lease rows by
   `(role, Pod UID, process boot nonce)`; fleet-transition coordinator lease;
   maintenance journal and detailed retirement evidence in generation/Pod
   order; the
`api_controller_leadership` singleton when controller leadership is touched;
   exact observation row; reserved-fill lease;
   lifecycle/service owner; claim set, edges, and pool rounds in canonical key
   order; scheduler singleton, pool/service cursors, and tickets in canonical
   pool/service/ticket order; replica intent rows/inserts by
   `(service_name, replica_id)`; then API request rows by request ID.

The observer releases step 2 before entering step 1 and reacquires no provider
phase during publication. A broker round uses steps 2, 4, and 5 and never the
manager lock. Ordinary and fill zero-cost admission both use steps 2, 3, and
5; fill additionally uses step 4 for the existing round/lease predicate. The
atomic request-binding transition uses steps 2 and 5 and follows the scheduler,
replica, then API-request suffix of the same row-lock order. No code may
acquire a provider phase or reservation lock while holding the manager or
broker lock. The advisory reservation lock coalesces planning and reduces
contention, but the singleton `FOR UPDATE` transaction is the correctness
boundary even if a writer ignores or loses the advisory lock. One grouped row snapshot is shared by demand budgeting, claim
construction, and fill planning for the input revision; final persistence
still revalidates authority transactionally. The current
`scale_up_batch()`/`scale_up_to_logical_capacity()` manager-then-reservation
order is removed; a lock-order assertion and regression test reject its
reintroduction.

The existing API queue may claim and commit a non-provider API request row by
itself. It must release that row lock before entering a Serve provider-effect
checkpoint; it may not acquire the protocol, process, scheduler, or replica
rows while holding a request row. For any Serve provider-mutation request,
including ordinary demand and reserved-fill `REQUEST_BOUND`, a nonlocking
candidate read is followed by the full guarded arbiter transaction above; no
standalone request-row claim is legal. Any later transition that
atomically touches both the intent and request reacquires the full order and
validates the immutable claim token/execution generation/process boot nonce.
This keeps the API queue from introducing an API-request-to-replica reverse
edge.

API009's statement-level instance trigger makes even an old registration or
heartbeat enter the step-5 singleton before PostgreSQL locks its target
instance row. The only deliberate table-lock exception is the final promotion
flush described below: after all effect writers are quiesced and new claims are
database-disabled, it briefly locks `api_requests` before the singleton to
drain legacy row-first DML. No routine writer may use that order.

The controller's dedicated-session election and generation advisory locks are
a nonblocking outer reservation, not an additional row-lock order. A candidate
first takes the election advisory lock with `pg_try_advisory_lock` while no SQL
transaction or other lock is held. Under that exclusive election lock it uses
an autocommit, non-row-locking read of the current durable generation `g`,
derives the existing collision-checked generation-lock key for `g + 1`, and
takes that advisory lock nonblockingly on the same session. It then begins the
step-5 singleton -> candidate boot lease -> current authority inventory ->
`api_controller_leadership` transaction, revalidates that the row is still
exactly `g`, the gate is active, and the candidate belongs to the exact current
effect-eligible inventory, and atomically publishes `g + 1`. Failure at either
advisory acquisition or any guarded CAS
rolls back and releases every acquired session lock; success retains both
locks for that leadership generation. Generation-aware release commits its
canonical singleton-to-leadership-row update before unlocking generation and
then election. No path waits for either advisory lock while holding the
singleton.

Every controller-owned transaction that also touches Serve sequencing takes
the singleton before a shared leadership-row fence; transactions that take
only the leadership row finish without entering Serve locks. This preserves
the existing generation fence without introducing a leadership-row ->
singleton reverse edge.

`reserved_capacity_broker.run_round_if_stale()` consumes a committed immutable
observation rather than accepting a Kubernetes callback. Under the singleton-
locked scan-to-publish transaction it revalidates UID, observation
generation and admission sequence, PostgreSQL freshness, claim set, and service
generation; scans the grouped replica rows; calculates the allocation; and
publishes the round. Provider I/O is therefore absent from the broker critical
section. The initial correction retains the global broker lock and lease in
addition to the singleton transaction; the shared reservation lock is a
liveness optimization only. Once protocol v1 is removed, a
separately reviewed cleanup may shard that short critical section by physical
pool using durable per-pool publish leases. It must first prove mixed-writer
activation and replica-persist conservation; merely moving the query outside
the lock is not permission to weaken those invariants.

The broker and ordinary Kubernetes-only placement both read the canonical
observation repository. Status reads the same rows. The direct broker query
and the separate status/demand cache are removed, so a displayed observation
cannot disagree with the evidence the broker is allowed to spend.

#### Lost-wakeup-free coordination

`ScaleReconcileCoordinator` owns a monotonic in-process generation and a
condition. Every publisher increments the generation before notifying. The
consumer records the generation used for a reconciliation, performs slow work
without the condition lock, and compares the current generation before it
waits. If it changed, the consumer loops immediately. Duplicate publications
coalesce; generations are not a work queue and no event carries launch
authority. A five-second maximum-idle recovery deadline rereads the durable
observation, allocation, lifecycle, and request publication generations for
the controller's owned services; it advances this same coordinator generation
when any durable input is newer. It is a lost-notification safety net, not a
second planner or capacity poll. The slower periodic demand deadline remains an
additional recovery path.

Observation and broker-round commits also advance durable publication
generations and issue a PostgreSQL notification containing only the physical
pool key and generation. Every controller with a live claim on that pool wakes
its allocation assembler, rereads authoritative rows, and attempts its complete
same-service-generation map. The notification is never authority and may be
lost, duplicated, or reordered; the durable generation plus the five-second
maximum-idle reread recovers it within the 15-second publication-to-accept SLO.
This is how a round driven by service A wakes service B
within the publication SLO without creating a central fill actuator.

The following events publish to that same coordinator:

- a committed complete broker allocation map;
- a periodic traffic-demand deadline;
- service version, policy, owner, or ordered pool-map revision change;
- a replica status, launch completion, teardown completion, readiness, or
  provider-slot transition; and
- recovery of accepted but not yet dispatched replica intents after controller
  restart.

The manager-owned `_scale_reconciliation_event`, its clear-before-wait
protocol, and any capacity-specific sleep are removed after all publishers use
the coordinator. A wake that races with a reconcile or wait can therefore
coalesce, but cannot be lost.

#### Narrow locks and stale-work rejection

`_actuation_epoch_lock` is replaced by a short policy-revision lock. It may
only capture or swap an immutable token containing the autoscaler identity,
service incarnation and active version, controller owner, policy revision, and
ordered pool-map revision. No database scan, provider observation, broker
round, readiness probe, planner call, manager actuation, or wait occurs while
it is held.

Observation, planning, and preflight work capture that token and execute
outside the lock. Complete-map publication and fill-intent acceptance compare
it with the current token. A mismatch discards the result and notifies the
coordinator for an immediate retry. Broker claim heartbeat and withdrawal
also carry the captured service version, owner, and claim-set generation; they
CAS the durable authority they read rather than blindly upserting it. An old
poller can therefore neither re-heartbeat a disabled policy nor publish a map
after an update.

The manager lock protects only local replica-map mutation and launch-worker
bookkeeping. Kubernetes UID reads, provider calls, request polling, readiness
checks, and network I/O take an immutable snapshot under that lock, run
outside it, and merge only if replica ID, row version, request ID, and worker
generation still match. A stale merge is discarded and wakes reconciliation.
This snapshot/probe/conditional-merge rule replaces continuous lock ownership
in `_refresh_thread_pool()`.

The broker's short durable lock remains a capacity-conservation boundary, not
a controller update lock. Its hold time, pool count, and age consumed before
allocation publication are measured. The broker/scheduler singleton critical
section, including the production-sized process-heartbeat cohort and API009
legacy statement-trigger traffic, must remain below 250 ms p99 and two seconds
maximum in the 200-intent, multi-service PostgreSQL stress gate, leaving the
five-second prospective claim window meaningful. A violation blocks activation
and requires query/index or separately reviewed sharding work; the response is
never to increase either the claim deadline or 180-second freshness horizon to
hide lock contention.

#### Plan, commit receipt, and provider dispatch

The planner emits typed immutable values:

```python
FillPlan(
    policy_revision,
    reconcile_generation,
    allocation_generation,
    allocation_input_sha256,
    allocation_claim_generation,
    intents: tuple[FillIntent, ...],
)

FillCommitResult(
    accepted_replica_ids: tuple[int, ...],
    deferred: tuple[DeferredFillIntent, ...],
    authority_current: bool,
)
```

Each `FillIntent` carries the plan's allocation generation/input hash/claim
generation plus the existing service incarnation/version/owner, service
generation, pool key and epoch, physical UID, observation generation/sequence
and `valid_until`, exact accelerator shape, and exact allowed location set.
`DeferredFillIntent` carries a typed reason such as provider-queue backpressure,
stale observation, superseded policy, lost owner, changed epoch, physical UID
mismatch, or `max_replicas` exhaustion. Sentinel keys in generic resource
dictionaries are not an authority interface.

`ReplicaManager.accept_reserved_fill()` performs slow physical-identity
preflight outside the manager lock, conditionally revalidates the snapshot,
and persists accepted replica rows through the existing protocol-v2 fenced
transaction. PostgreSQL transaction time must be no later than the carried
observation's `valid_until`; specifically, the predicate samples
`clock_timestamp()` after every reservation, advisory, and row-lock wait, not
transaction-start `now()` and not a controller wall clock. Checking only epoch
is insufficient because an unchanged round can age past 180 seconds. The
transaction also requires the carried observation generation/admission sequence and
retains the
current calling process boot membership/ack, owner, reconciliation generation,
allocation generation/input
hash/claim generation, service generation, pool epoch, physical UID, exact shape,
`max_replicas`, global lease-token, and live-claim predicates. It never
performs a provider launch while holding the manager lock. Under the scheduler
singleton, the same transaction allocates the immutable FIFO ordering ticket
stored on every accepted row; an accepted intent is therefore never invisible
to dispatch-horizon accounting.

The replica rows are the durable intent journal; no second intent table or
in-memory-only queue is introduced. A commit receipt reports exactly the rows
that exist. The planner debits only `accepted_replica_ids` during that
reconciliation. The next reconciliation derives occupancy from durable rows,
so a lost reply after a successful insert is conservative and idempotent.
Deferred or transaction-rejected tails remain immediately eligible under the
same fresh allocation and trigger another reconciliation; they do not modify
raw or damped broker feed and do not wait for two higher polls.

Accepted rows enter the durable intent state machine defined above. Its queue
is the replica table. A thin PostgreSQL-backed
`ServeProviderAdmissionScheduler` owns only a bounded mutation semaphore,
durable ordering ticket, prospective-lane reservation, and fairness cursors;
it is not an action queue, executor, or second provider
authority. The existing API request queue/executor remains the sole execution
lease and effect record. Service-local workers may activate only the exact API
request bound in `REQUEST_BOUND` and claimed by the guarded `SUBMITTING`
transition.

Before the atomic `ACCEPTED -> REQUEST_BOUND` transition, the scheduler repeats
the version, owner, epoch, physical UID, observation generation/admission
sequence, and final `clock_timestamp() <= valid_until` checks. An expired
no-effect intent is retired and replanned. Request binding persists the
immutable idempotency key, deterministic provider object identity, and exact
request ID but no execution generation or mutation permit. There is no
heartbeat or renewable delay between ticket selection and this transaction;
lock contention loses the nonblocking attempt and leaves the row `ACCEPTED`.
Once `REQUEST_BOUND`, only the five-second claim deadline applies. The guarded
API claim rechecks every fence and requires the full 15-second
claim-to-durable-receipt checkpoint to remain before `valid_until`; it
then assigns the execution generation and mutation ticket atomically. Claim-
deadline expiry after request binding never permits a second request or effect.
Recovery follows the discovery/quiescence rules above.

The scheduler singleton is the one durable authority for positive integer
`mutation_limit C` and monotonic `mutation_limit_generation` for non-pool
SkyServe provider work; processes never independently enforce a locally
derived value after activation. During initial attestation, every capable
API/controller/executor boot reports its local safe maximum from
`controller_utils._get_request_parallelism(pool=False)` plus a stable hash of
the environment, resource, and worker-pool inputs that produced it. Promotion
selects the minimum positive maximum across the complete effect-capable fleet,
persists it as `C`, and requires every boot ACK to name that stored value and
generation while retaining its own maximum/input hash. Different valid local
maxima are allowed; a missing/nonpositive value, a payload/hash mismatch, or a
boot whose maximum is below the stored `C` blocks that boot from active
membership. This fail-closed default cannot exceed any executor's attested
capacity. The affected production inventory currently resolves the minimum to
eight, but eight is not a protocol constant. SkyServe Pools (`pool=True`) do
not share this singleton or its worker lane; they use the same mutation-kernel
implementation with their separately attested pool budget and cannot mutate a
non-pool service ticket.

Changing `C` uses a scheduler-local gated drain, not live per-process config:
stop new prospective lanes and claims, terminalize expired/no-effect lanes,
wait for active mutations, live prospective lanes, and debt to reach zero, collect
complete-fleet acknowledgement of the target, then atomically advance
`mutation_limit_generation` and publish the new positive limit. Timeout leaves
the old limit active and resumes it; debt blocks the change. Every ticket and
effect start carries/rechecks the limit generation, while a completed change
terminalizes stale no-effect tickets for fresh planning. This operation changes
only concurrency for the same provider path and creates no service selector or
canary.

`ServeProviderMutationArbiter` is this scheduler's shared mutation kernel. Its
count includes ordinary non-pool SkyServe launch/cancel/cleanup checkpoints
and their ambiguous debt, not only reserved-fill work. Ordinary traffic demand and
cleanup retain their existing higher operational priority; reserved fill may
create prospective lanes only from the remaining global capacity and cannot
delay such work beyond an already-started bounded checkpoint. Fill admission is
weight-blind hierarchical round-robin: physical pools first, services within a
pool second, and FIFO within
a service lane. The FIFO key is the durable ordering ticket assigned at
acceptance, not an in-memory timestamp. Both cursors are durable. A pool with
many services gains no extra global turns. Let `E = C - debt`. While `E > 0`, every eligible pool
receives a first permit before one pool receives its second, and with more pools
than effective permits each pool advances within `ceil(pool_count / E)` finite
submission waves when executors meet the five-second claim deadline and the
provider meets its bounded submit contract and continuously queued higher-
priority ordinary work is absent. Queue backpressure or demand preemption
terminalizes or defers a
no-effect request with a typed reason instead of violating freshness; it is an
acceptance-SLO failure, not a stale provider start. At `E = 0`, safety
deliberately suspends liveness until exact recovery resolves debt.

Broker weight is the sole owner of allocation quantity. A second service with
weight 1000 may receive more accepted intents from the broker than a lower-
weight peer, but weight is not an input to provider admission and cannot skip a
peer's temporal first turn. Provider capacity completion or failure never
mints, moves, or resizes a grant.

Every non-pool SkyServe API request capable of a provider mutation carries a closed
`provider_mutation_kind`: `CLEANUP`, `CANCEL`, `ORDINARY_DEMAND`, or
`RESERVED_FILL`. Cleanup/cancel has first admission priority, ordinary traffic
demand second, and opportunistic fill last. The existing API request row is the
durable ordinary-work ticket/effect/debt record; reserved fill additionally
locks and advances its replica intent. The transaction that first makes any
provider-mutation request `PENDING` assigns its immutable scope ordering ticket
under the scheduler singleton, so no higher-priority backlog is invisible to
fill horizon accounting. After a nonlocking candidate read, the
guarded ordinary claim transaction takes the shared reservation boundary and
canonical PostgreSQL order--protocol/process lease, lifecycle/service,
scheduler, optional replica, then API request--and atomically acquires a
mutation ticket, assigns one execution generation, and changes the request to
`RUNNING`. If no slot exists it remains PENDING and consumes no API worker.
When the arbiter selects it, the ordinary request receives the same five-second
database-time prospective deadline; only that live prospective selection
counts against `C`, and expiry has the same immediate no-late-claim predicate
and bounded-cleanup behavior as fill.
Success releases the ticket only after the same UID/result proof as fill;
ambiguous create/cancel/cleanup converts that exact request ticket into debt
until execution quiescence plus provider resolution. The database effect-start
predicate and sole typed executor reject any non-pool Serve provider call whose
request did not pass this arbiter.

The guarded claim returns one immutable `ProviderMutationPermit` carrying the
scheduler scope/ticket, request ID and execution generation, mutation kind,
provider, provider-specific idempotency/effect identity, exact object
precondition when one exists, reconciliation/inventory/limit generations,
process boot nonce, and absolute 15-second result deadline. The sole
`ServeProviderMutationExecutor` consumes that typed permit, atomically records
`effect_started_at` and a provider-neutral mutation receipt identity, and only
then calls an adapter. Every provider adapter used by this scope implements one
bounded issue seam with a closed durable result: definitive create plus stable
effect/object identity, definitive delete accepted for the exact identity,
proved absence, definitive no-effect failure, or ambiguous outcome. A provider
may use a Kubernetes UID, cloud operation ID, or deterministic cluster handle,
but it must support bounded lookup/adoption/cancellation of that exact identity.
The adapter receives an internal RPC deadline at least three seconds earlier than
the permit's absolute result deadline, reserving those final three seconds for the
PostgreSQL receipt/state commit. The 15-second checkpoint is therefore measured
from guarded claim commit through durable definitive receipt, not merely until
the SDK returns. If no definitive receipt commits by that database deadline,
the ticket is effective debt immediately by database-time predicate even if a
crashed worker never materializes the debt flag; the recovery sweep persists
that classification but does not create it.
The normal result transaction rejects a commit at or after the deadline. A
late provider response may be appended only as monotonic recovery evidence for
the same execution generation; it remains debt until exact worker quiescence
and the lookup/adoption rules below validate that receipt. It cannot revive the
expired permit or start a follow-up effect.
Timeout, worker loss, or a result outside the closed set is ambiguous debt;
locally cancelling a client future is not a receipt. A definitive delete-
accepted receipt releases the active mutation ticket and leaves the replica in
ticket-free `CLEANING` while UID/effect-identity observation proves absence.
The implementation cannot activate until ordinary non-Kubernetes create,
cancel, and cleanup adapters satisfy this same receipt contract. Direct
provider call sites are rejected without a current permit, and the removal
stack deletes their untyped entry points.

Provider-mutating requests do not use the generic claim-before-submit path.
After its nonlocking candidate read, `RequestWorker` must first obtain a
process-local, one-use idle-worker reservation from
`BurstableExecutor.try_reserve_idle_worker()`; provider requests are never
placed in `submit_until_success()`'s guaranteed-pool backlog. The guarded
PostgreSQL claim is attempted with `NOWAIT` only after that reservation. Losing
the ticket/request CAS releases the reservation without changing durable
state. Winning commits `RUNNING` and hands the exact request to
`submit_reserved()`; handoff failure or worker death preserves the active
ticket as debt until the reservation generation, process boot, request, and
provider receipt prove whether an effect started. If no worker is idle, the
request remains `PENDING`, consumes no API worker or active mutation ticket,
and its database claim deadline expires normally. Generic API work retains its
existing queue, but queue selection filters make a provider mutation
unclaimable by that path.

The current `_LegacyReplicaMutationRuntime.down_thread_pool` is an explicit
transition item, not an exception. Its `SafeThread` path through
`terminate_cluster()` to direct `core.down()` remains available only inside the
`LEGACY_ACTIVE` compatibility adapter. The transition image writes an API008
execution/quiescence record with exact service, replica, cluster, process boot,
and worker generation before starting each such compatibility thread. An old
binary without that record remains an unattested process and blocks promotion.
Promotion inventories and quiesces every such thread; a live or ambiguous call
blocks the flip. Under
`SEQUENCED_ACTIVE`, termination first persists the exact `CLEANUP`/`CANCEL` API
request and object precondition, and only the permit-consuming executor above
may invoke the provider-neutral down adapter. Direct `core.down()` from the
replica manager fails closed without the carried permit. The stacked removal
PR deletes the legacy runtime, `SafeThread` dispatch, and
`down_thread_pool` ownership after the full-fleet gate.

An ordinary or cleanup arrival may reclaim only a no-effect fill prospective
lane. Under the scheduler singleton it locks the fill intent first and all
affected API request rows by request ID, CAS-cancels the still-PENDING fill
request with typed `ordinary_preemption`, releases its prospective lane, and
then admits the higher-priority request in the same transaction. It cannot
preempt `SUBMITTING`, erase debt, or exceed `C`; in that case it waits only for
the already-started bounded checkpoint. Every ordinary/fill bind, claim,
reclaim, result, cancellation, and recovery transition uses the same singleton
and limit generation. Non-provider API requests retain the generic request
queue path and do not consume this provider-mutation budget.

Provider request execution is split into bounded issue, observe, and cancel
checkpoints in the existing API request/provider path. Kubernetes supplies the
fill implementation; ordinary cloud work uses the same receipt interface. The
scope-global mutation permit covers only the issue/RPC seam and has a hard
15-second result deadline. It is released after a definitive create identity
plus `WAITING_CAPACITY` or `INITIALIZING` state is durably committed, after a
definitive exact-identity delete-accepted result is durably committed into
ticket-free `CLEANING`, or after proved pre-effect absence. Terminal intent
still requires exact request quiescence and observed provider absence. A crash
or deadline with ambiguous effect enters `EFFECT_RECOVERY_REQUIRED` and retains conservative occupancy;
it never retries the create. Its mutation ticket becomes durable
**concurrency debt** and continues to count against `C` after a process crash or
the 15-second deadline. The debt is released only after the exact API execution
generation is quiesced and provider-neutral recovery yields a discovered
create identity durably adopted into `WAITING_CAPACITY`/`INITIALIZING`, a
definitive exact-identity delete-accepted receipt durably adopted into ticket-
free `CLEANING`, or terminal request plus provider-absence proof that makes the
intent `TERMINAL`. Lease expiry, a lost heartbeat, or a locally cancelled
future is not release evidence. If ambiguous debt reaches
`C`, new provider mutations stop safely until recovery resolves a ticket; this
is an explicit liveness incident and rollout stop, not a reason to exceed the
limit or promise fairness. After exact prior-worker quiescence, the recovery
executor may re-lease that same debt ticket for bounded lookup/adoption or
exact-identity cancellation; this is a continuation of the one carried effect,
not a new slot or create. Further ambiguity retains the same debt, while
create/delete adoption or terminal/absence proof releases it. Thus debt resolution remains
possible even when debt equals `C`. Lightweight asynchronous observation of a proved
`WAITING_CAPACITY`/`INITIALIZING` row holds neither a mutation permit nor an API
long worker. Loss of that later UID-fenced observation enters
`OBSERVATION_UNKNOWN`, not effect recovery, and never consumes concurrency
debt. If the Kubernetes provider path cannot expose this post-create
seam, that refactor is a pre-activation blocker; fairness with
`provision_timeout: -1` must not be claimed around a blocking `sdk.launch`.

Provider completions and state transitions notify the owning service's common
coordinator. A stable capacity decrease may directly retire only no-effect
`ACCEPTED` work. Leased and still-PENDING request-bound work can be terminalized
only by their proved no-effect CAS. Submitting, waiting, initializing,
observation-unknown, and ready work uses the corresponding fenced
cancellation/cleanup transition; observation expiry alone cannot delete an
object whose first effect was valid.

`kubernetes.provision_timeout: -1` remains the appropriate queueing policy for
a fixed reserved pool whose occupied slots are expected to return. It means a
durable `WAITING_CAPACITY` pod may wait indefinitely; it does not extend RPC,
mutation, lease, or observation deadlines, serialize planning/durable
admission, consume a provider permit, or block another pool's lane, and it does not
cause the whole service to fail while another exact location is usable. A
finite timeout remains an operator escape from ambiguous scheduling, not the
classifier for `WAITING_CAPACITY` versus `INITIALIZING`. Those states come
from explicit provider and Kubernetes pod transitions. The public timeout
default remains owned by
`serve-reserved-gpu-fallback-and-worker-reconciliation.md` and is not changed
here.

Acceptance is additionally bounded by the observation authority horizon. In
the final persist transaction, after locking the scheduler singleton and
reading the ordered ticket set, the scheduler computes `outstanding_work` as
every active non-debt mutation ticket, every ordinary or fill prospective lane
that is still live by database time, every existing fill `ACCEPTED` ordering
ticket, and every currently eligible higher-priority `CLEANUP`, `CANCEL`, or
`ORDINARY_DEMAND` PENDING ticket not already represented by a prospective lane
that the arbiter must select first. The
candidate is one additional unit. The authoritative prospective count is an
indexed database-time predicate over ticket/request rows under the singleton,
never a cached counter that can remain occupied after its deadline. Counting
the whole set is
deliberately more conservative than trying to predict a dynamic hierarchical
position. Every scheduler transition takes that singleton, so the count cannot
change until the candidate ticket is inserted. It admits the candidate only
when

`clock_timestamp() + ceil((outstanding_work + 1) / max(1, C - debt)) * 20 seconds + 30 seconds <= valid_until`.

Here `debt` is the database-time effective unresolved concurrency debt defined
below, and the 30 seconds is a fixed scheduling/commit safety margin. The
20-second wave charges the full
five-second API claim window plus the 15-second claim-to-durable-receipt
checkpoint even to an active ticket; a concurrent claim or completion can only
make the estimate more conservative. The guarded executor claim separately
requires that full 15-second receipt checkpoint to remain. `debt` includes both
materialized debt rows and an active ticket whose result deadline has expired
without a definitive receipt. Later tickets may receive an
intentional cross-pool fairness turn, but the aggregate outstanding bound and
durable cursors prevent them from starving an already eligible lane. If
`debt >= C`, no new intent is accepted. An unbounded or continuously arriving
higher-priority backlog may therefore defer fill acceptance entirely; a finite
backlog is charged explicitly rather than hidden behind the fill queue. Only
the prefix with a conservative
chance to reach the bounded submit seam while its evidence is valid enters
`ACCEPTED`; the remainder stays eligible broker feed rather than becoming stale
durable occupancy. An `ACCEPTED` or still-`PENDING`
`REQUEST_BOUND` intent that reaches its applicable deadline with no provider
effect loses all admission/claim authority by PostgreSQL time; bounded cleanup
then writes `TERMINAL`, after which only a fresh observation may create a new
intent. The old row is never reauthorized in place or attached to a newer
epoch.

A 200-replica target therefore does not imply 200 simultaneous accepted
no-effect intents. With debt zero, `C = 8`, the 20-second claim-plus-receipt wave,
the 30-second margin, and a fresh 180-second authority horizon, at most the
first 56 queue positions are accepted initially; completions and newer observations can admit
more. Actual fast submissions may advance the target sooner, but the protocol
does not claim the invalid `ceil(200 / C) * 15 seconds <= 180 seconds` bound.
Making all 200 replicas `READY` has no finite bound when capacity is
unavailable under `provision_timeout: -1`. Independent pools still receive
fair bounded submit opportunities whenever `C - debt` is positive; ambiguous
debt exhausting `C` is surfaced as the safe liveness failure above.

#### BCL reclaim and scale-down

Reserved fill remains low-priority, zero-cost, and preemptible. It never
evicts BCL, changes a pod's configured priority, or spills to a paid location.
When higher-priority BCL work consumes a slot, the canonical observation falls,
the broker reduces feed, and the common reconciler removes unstarted excess and
applies the existing pool-local shelter/downscale rules. A provider request or
pod already admitted under `provision_timeout: -1` may wait or be preempted;
neither state converts it into demand authority. BCL reclaim does not wait for
a SkyServe poll in order to occur because Kubernetes priority/preemption is the
physical authority. Reconciliation makes SkyServe converge afterward.

#### Status and operational history

Per pool and exact card, raw status exposes:

- observed free, `observed_at`, age, `valid_until`, blackout/error, and
  observation generation;
- broker grant/feed, pool epoch, published generation, and allocation age;
- spendable free after freshness, damping, and durable occupancy debit;
- planned, durably accepted, request-bound/PENDING, active provider mutation,
  observation-unknown, provider-running, scheduled, ready, and draining replica
  counts;
- last reconciled generation, publication-to-accept latency, fixed-rate poll
  lag, oldest queued-intent and oldest PENDING-claim age, prospective/active
  provider slots split by ordinary/fill priority, expired no-effect tombstones,
  durable non-pool-scope limit/generation and attested fleet minimum, unresolved
  concurrency-debt count and age, idle-worker reservation/backpressure, exact
  provider receipt outcome/age, and the latest typed deferral or rejection reason.

Fleet status separately exposes reconciliation gate/inventory/limit
generations, transition owner/reason/hook phase/lease generation/expiry,
installed schema heads, immutable database-authority-anchor identity and latest
authority-guard/credential state and attempt generation, anchor-enforcement
receipt/cohort/ConfigMap final-resourceVersion acknowledgement,
`release_instance_id`, phase-0 recorder health/cursor/evidence gaps,
Historical Authority Reset phase/generation/receipt/egress/credential/runtime/
effect-frontier evidence, retirements/debt, source-versus-target ownership
phase, guard-namespace identity, live role/ACL-closure hash, guarded DDL/catalog generation and
state, source-session drain/challenge coverage,
source and target inventory hashes, rendered-target
payload hash, Deployment/ReplicaSet cursors, desired/local ACK and
quiescence state per boot, excluded/retiring UIDs, permanent retired-Pod
revocation count, protected-topology hash/guard outcome, and controller
leadership-release generation. This is the evidence used by the zero-argument transition
module, not an operator-editable control surface.

`fill_free_slots` remains the deprecated aggregate damped-broker-feed
projection for API compatibility; it is neither raw provider capacity nor the
final launchable count. New control and UI code reads the additive
`fill_spendable_free_slots` field, while `fill_observed_free_slots` reports raw
provider evidence explicitly. Status must never display 34 raw slots as
launchable when their age makes spendable capacity zero. The existing
minute-level Serve status snapshot in PostgreSQL is
extended with these already-collected fields for post-deploy verification.
Correct operation does not depend on unavailable controller-log scraping or on
adding a new metrics backend.

#### Fleet-wide one-way activation and fix-forward maintenance

The correction adds an orthogonal reconciliation-protocol gate to the existing
`reserved_fill_protocol_state` singleton. The physical-pool broker remains
protocol v2; this gate selects which reconciliation and provider-effect kernel
owns it. Exactly one kernel is authoritative at a time, globally. There is no
service selector, shadow actuator, canary, or user-visible old/new flag.

Serve041 first requires an existing physical singleton to be exactly protocol
v2; an exactly empty bootstrap creates it as v2 in the migration transaction.
Here and below, `exactly empty` means no application, workload, request,
service, provider-effect, or prior migration-lineage state; the externally
anchored guard, its permanent receipt/acknowledgement, and empty-bootstrap
bookkeeping created by `-25` are the only permitted pre-baseline objects.
The migration adds a permanent PostgreSQL `CHECK` floor that rejects every stored protocol
value below 2, including an update made by the historical demotion binary or
ordinary direct SQL through the application role. The floor is installed while
the reconciliation gate is still `LEGACY_ACTIVE`, is retained by cleanup, and
has no application operation that can lower or remove it. Singleton locking
serializes an old demotion attempt with activation, but both possible orders
reject the v1 write before it commits. A database superuser dropping safety DDL
is outside the supported operational model and must not be used as a recovery
procedure. This mechanical floor, rather than the historical prose below,
makes the physical broker protocol one-way.

The durable states are `LEGACY_ACTIVE`, `PREPARING_SEQUENCED`,
`SEQUENCED_BOOTSTRAP`, `SEQUENCED_ACTIVE`, and
`SEQUENCED_MAINTENANCE`. An existing database with legacy state enters
`LEGACY_ACTIVE`; a truly empty new database enters effect-closed
`SEQUENCED_BOOTSTRAP`. The transition graph is closed:

| Current | Next | Required proof |
| --- | --- | --- |
| `LEGACY_ACTIVE` | `PREPARING_SEQUENCED` | Complete split-role capable inventory and new activation journal |
| `PREPARING_SEQUENCED` | `SEQUENCED_ACTIVE` | Forward projection, fresh observations, conservation, and one final inventory/ack transaction |
| `SEQUENCED_BOOTSTRAP` | `SEQUENCED_ACTIVE` | Empty-database proof, complete split-role capable inventory, protected topology, and one final inventory transaction |
| `SEQUENCED_ACTIVE` | `SEQUENCED_MAINTENANCE` | Source inventory and reason captured; the same commit closes every new claim/effect start, while live boots must later ACK and missing boots remain recovery debt |
| `SEQUENCED_MAINTENANCE` | `SEQUENCED_ACTIVE` | Exact desired capable cohort, excluded-boot revocation, zero ambiguity/debt, conservation, and one final inventory transaction |

Every other edge is rejected in PostgreSQL. Bootstrap cannot adopt or import a
nonempty legacy database and has no edge to legacy or preparation. In
particular, after the first `PREPARING_SEQUENCED` commit there is no edge back
to `LEGACY_ACTIVE`, and there is never an active-to-legacy or schema-downgrade
operation. A crash resumes the same activation or maintenance journal. A bad
image, incomplete rollout,
ambiguous provider receipt, or migration defect remains fenced and is repaired
with a newer capable image or forward data repair. This is the explicit
fix-forward contract requested for the lightly used service, and it removes the
reverse projection, legacy image rollback, pairwise live-effect handoff, and
their compatibility branches from the design.

Sequenced activation and bootstrap are supported only for the chart's split-role PostgreSQL HA
topology: `api`, `controller`, and `executor` Deployments, each with at least two
replicas. A recent compatibility `all` lease, an `all` Deployment, SQLite or a
custom request backend/queue, or disabled split-role HA blocks activation.
Compatibility `all` remains usable only before the one-way activation. In
bootstrap, preparing, active, or maintenance, synchronous registration rejects
an `all` process.

Phase 0 also rejects enabled image-copy, lifecycle, or canary worker
Deployments. Those optional components currently share the central database
credential and may perform provider effects but do not participate in
`ServerInstanceLease`; silently omitting them would make the fleet proof false.
The source release must disable them and prove their Pods/effects terminated
before `ANCHOR_SCAFFOLD`. If release history or provider-effect evidence cannot
prove they were never enabled or cleanly terminated, the mandatory Historical
Authority Reset must inventory, egress-fence, credential-revoke, terminalize,
and debt-classify them before its baseline can commit; there is no separate
future escape protocol.
Adding them later requires extending the same lease/guard/maintenance cohort,
not a worker-specific bypass. Rendered-chart and live-topology checks enforce
this restriction before the first guard mutation.

##### Additive state and writer fencing

Serve schema 041 and API-request schema 009 are additive PostgreSQL migrations.
If either revision is already allocated, implementation advances both numbers
and updates this file without changing the contract. They add the observation
and admission sequence, allocation provenance, typed intent state and
idempotency identity, request binding/claim deadlines, provider-neutral
receipt/debt state, persisted mutation limit/tickets, exact effect-start
process identity, runtime capabilities, immutable process boot leases,
one-way activation journal, maintenance journal, validation runs, the singleton
`serve_reconciliation_removal_gate`, and
permanent retired-Pod revocations described here. No central SQLite migration,
fallback, or test target is added.

The first migration that creates the transition gate classifies the database.
Exact absence of every legacy service, request, replica, claim, observation,
controller, workload/effect receipt, and provider-effect row, with only the
permitted guard/anchor evidence above, initializes `SEQUENCED_BOOTSTRAP`. Any
nonempty pre-gate database initializes `LEGACY_ACTIVE` and must use forward
activation; an uncertain or partially initialized database fails migration.
Once the gate exists, every later migration preserves its exact state and
journal generation unless the ordered transition transaction above changes
it. Classification is durable and cannot be changed by a Helm value or later
image.

The protocol singleton stores the gate/activation generation, exact authority
inventory count/hash/revision, implementation and capability contract,
persisted mutation limit/generation, transition phase/cursor/projection hash,
protected-topology envelope/hash, and latest Kubernetes topology attestation.
It also stores one immutable `central_database_identity`: a generated
installation UUID and the ID/hash of an installation-scoped Kubernetes
`DatabaseAuthorityAnchor`.

The anchor is an immutable, operator-provisioned ConfigMap in the dedicated
guard namespace and outside PostgreSQL. The mutable-to-immutable enforcement
receipt and every revision proof ConfigMap are in that namespace too; the API
ServiceAccount's broad release-namespace ConfigMap patch permission cannot
reach them. Namespaced Roles in each target namespace, with RoleBindings whose
subjects are the explicitly named guard- or release-namespace ServiceAccounts,
give the specifically enumerated hook/recorder identities GET only, the bounded phase-0 writer its one named
enforcement-receipt update, and the transition runner create-only revision
evidence as described below.
It contains a random 256-bit anchor ID; the namespace/name/UID/resourceVersion
and salted content digest of three pre-provisioned database Secrets (legacy
application and sequenced application in the release namespace, plus the
migration/guard administrator in a dedicated operator-owned guard namespace);
the guard namespace name/UID; and a
non-secret direct source endpoint tuple: resolved `hostaddr` and port, TLS
server-public-key fingerprint/name, PostgreSQL system identifier, database
OID/name, and schema; and an externally proved `source_kind` of exactly
`EMPTY_BOOTSTRAP` or `EXISTING_CURRENT_HEAD`. It also contains a random installation-scoped
`release_instance_id` bound to the Kubernetes cluster identity (the immutable
`kube-system` Namespace UID), release namespace name/UID, guard namespace
name/UID, and Helm release name. Every later reference to `release UID` means
this anchor field--Helm has no `.Release.UID`. The identity is stable across
Helm revisions and is never inferred from a Deployment; exact-name proof
objects use a short hash of it plus the Helm revision.

The only production database-authority kind for this initiative is
`AWS_AURORA_POSTGRESQL_RDS_SUPERUSER_V1`; there is no literal-PostgreSQL-
superuser or self-hosted fallback. The anchor's versioned `admin_authority`
object binds the AWS partition/account/region, DB cluster identifier/ARN/
resource ID, `aurora-postgresql` engine/version/mode, master username, IAM-
authentication setting, and canonical `DescribeDBClusters` hash. For the
reviewed Rainier source this is account `255203429798`, region `us-east-1`,
cluster ARN `arn:aws:rds:us-east-1:255203429798:cluster:skypilot-aurora`,
resource ID `cluster-YFI5BUUYKYZJ7RQM3KPW57IZSU`, Aurora PostgreSQL `16.13`,
`EngineMode=provisioned` with Serverless v2 scaling, master role `skypilot`, and
IAM database authentication disabled. A fresh infrastructure read must equal
those reviewed fields before anchor creation; the literal values are rollout
evidence, never chart defaults.

The object also binds `server_version_num`, system/database identities, writer
endpoint/hostaddr/TLS identity; the complete administrator-role tuple and
`rolconfig`; the AWS-managed `rds_superuser` name/OID/attribute tuple and full
forward/reverse membership closure; every membership row's role/member/grantor
OIDs and admin/inherit/set options; `pg_has_role` `MEMBER`/`USAGE`/`SET`
results; one exact administrator-to-guard-owner membership edge; the
`createrole_self_grant` setting; hashes of the complete provider-predefined-role
catalog and customer-login high-authority set; and the checked-in capability-
probe version, program SHA-256, and receipt SHA-256. Capability facts come only
from that live probe, never from an operator-supplied boolean.

The two application Secrets name distinct PostgreSQL roles on the same
database. The anchor pins the OID, `LOGIN`, `SUPERUSER`, `BYPASSRLS`,
`INHERIT`, `CREATEROLE`, `CREATEDB`, and `REPLICATION` attributes and complete
effective membership graph of the legacy, sequenced, and migration/guard
administrator roles. It also pins one dedicated `NOLOGIN NOSUPERUSER
NOBYPASSRLS NOINHERIT` guard-owner role and its OID. The anchor records two
phase-specific complete legacy-role profiles: before credential fencing it is
`LOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT`; from the monotonic fencing commit it
is `NOLOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT` permanently. The guard state
selects the exact immutable profile, so no anchor update is required. Because
today's migration Job uses the application Secret, the anchor separately pins
the exact source ownership/effective-privilege/default-privilege graph and the
normalized target graph. Before the guard transaction the legacy role may own
only the exact source objects recorded there. That transaction atomically
materializes its required effective privileges as explicit grants, transfers
every database/schema/relation/sequence/view/type/function owner to the pinned
nonlogin guard owner (or another catalog-pinned nonapplication owner), rewrites
default privileges, and installs the permanent guard directly. The reset makes
legacy login impossible before this transaction, so no transitional tokenless
policy is installed. After commit both application roles own nothing and the
source graph can never be accepted again.

In the normalized graph, neither application role has a direct or transitive
membership or grant that permits `SET ROLE` to the guard owner, administrator,
`rds_superuser`, another customer authority login, or any owner, `BYPASSRLS`,
`CREATEROLE`, `CREATEDB`, or replication authority. The legacy role's phase-
varying `LOGIN` bit is its only closure exception. The sequenced role remains
`LOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT`. The administrator is the exact
Aurora master role, pinned as `LOGIN NOSUPERUSER INHERIT CREATEDB CREATEROLE
NOREPLICATION NOBYPASSRLS`; `rolsuper=true` is forbidden. It has one exact
direct effective `MEMBER`/`USAGE`/`SET` membership in AWS-managed
`rds_superuser` and is the only non-provider login with any membership, use,
set, or admin-option path to that role. It also has one direct `SET TRUE,
INHERIT FALSE, ADMIN FALSE` edge to the guard owner. The guard owner remains
`NOLOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT NOCREATEROLE NOCREATEDB
NOREPLICATION`, owns every guarded relation/function, and has no other inbound
or outbound membership. AWS internal roles are accepted only as the exact
pinned provider-role set; a new role or edge blocks pending recertification.
The trusted anchored-Aurora-administrator exception is confined by the isolated
credential/Job boundary and proved managed capabilities rather than the
PostgreSQL `SUPERUSER` bit. The administrator owns each event trigger; its exact OID,
definition, tags, and `ENABLE ALWAYS` mode are pinned by the guard-migration
receipt and protection catalog. Guarded relations use
`ENABLE` and `FORCE ROW LEVEL SECURITY`; guard `SECURITY DEFINER` functions pin
the guard owner, an explicit `pg_catalog`-first search path, definitions, and an
execute ACL limited to the exact application/admin roles. Per-owner/per-schema
default privileges revoke `PUBLIC`, forbid future application access by
default, and are part of the anchored hash. The administrator Secret is mounted
only by the bounded guard-migrator, anchor-enforcement,
anchor-evidence-finalizer, and guarded-migration Jobs running in the
dedicated guard namespace--never by a target transition runner, prerequisite,
recorder, or authority Pod. The release API ServiceAccount has no RoleBinding or Pod-creation
authority there, so its broad release-namespace Secret/Pod permissions cannot
read or mount the administrator credential. The namespace and Secret are
operator-owned and survive supervised release decommission; Jobs, ServiceAccounts, and their
cross-namespace least-privilege bindings are chart-owned.

In the normalized target graph, `PUBLIC` and both application roles have neither
`CREATE` nor `TEMP`; at the application schema they have `USAGE` but no
`CREATE`. Database/schema owners and ACLs, application-role `rolconfig`, and
database/role settings are pinned. Every authoritative SQL reference is schema
qualified. Application sessions and guard functions use the exact
`pg_catalog,<anchored-schema>` search path and cannot select a shadow object.
The transaction-local guard predicate re-derives the live role OIDs,
attributes, recursive membership/assumption closure, owners, database/schema/
column/default ACLs, and role/database settings once per transaction before
the first authoritative read or write; provider-effect admission repeats it in
its commit transaction. A mismatch fails closed immediately rather than
waiting for a later hook. Database infrastructure policy separately reserves
the administrator credential for these Jobs and audits all shared-object role
DDL; event triggers alone are not claimed to intercept `ALTER ROLE` or
membership changes.

Before anchor creation, and again immediately before the first guard mutation,
the checked-in Aurora capability probe connects to the exact writer with the
administrator Secret under a fixed advisory lock. It canonically records
`pg_roles`, `pg_auth_members` including all options/grantors, every relevant
`pg_has_role` result, provider/customer authority sets, server identity/version,
database/schema/default ACLs, and `createrole_self_grant`. In one read-write
transaction it creates uniquely named probe schema/table/functions; creates
`ddl_command_start`, `ddl_command_end`, and `sql_drop` event triggers and sets
them `ENABLE ALWAYS`; observes the exact create/alter/drop event sequence;
creates and ownership-transfers a pinned-search-path `SECURITY DEFINER`
function through the explicit guard-owner `SET ROLE` edge; enables and forces
RLS; creates policies, statement triggers, and a deferred constraint trigger;
exercises required revoke/grant/default-privilege operations; and tests the
legacy-role `NOLOGIN` alteration. It canonicalizes observations and rolls the
whole transaction back. A fresh connection must prove zero probe schema,
function, event-trigger, membership, or default-ACL residue. Separate sessions
using both application Secrets must fail to assume administrator,
`rds_superuser`, or guard owner; create database/schema objects or event
triggers; and bypass forced RLS. The pre-rollback observations plus post-
rollback absence are the capability receipt bound into the anchor. A vanilla
PostgreSQL fixture tests canonicalization only; the release gate requires this
live Aurora 16.13 probe.

Every authority startup/checkout, transition hook, and provider-effect
admission recomputes the server/database/engine identity; administrator,
application, guard-owner, `rds_superuser`, and provider-role tuples; complete
membership/use/set/admin-option closures; the sole customer authority-login
set; ownership and database/schema/object/default ACL hashes; permanent event-
trigger OIDs/owners/definitions/tags/functions/modes; forced-RLS, policy,
security-definer owner/ACL/search-path, and protection-catalog hashes; and the
receipt binding to cluster resource ID, engine version, role OIDs, and probe
program hash. Before guarded migration, an undeclared DDL statement inside a
savepoint must receive the exact guard rejection from `ddl_command_start`; a
success or different error blocks before declared DDL. Engine-version change,
master reset, role recreation, membership/grantor/option drift, another
`rds_superuser` member, provider-role drift, or receipt mismatch closes DDL and
provider effects. Requalification requires a reviewed guard-kernel maintenance
revision; there is no automatic re-anchor or generic PostgreSQL fallback, and
an Aurora major upgrade is outside ordinary image maintenance.

`dbAuthorityAnchor.existingConfigMap` and
`dbAuthorityAnchor.enforcementReceiptConfigMap` are mandatory for the
transition and are
not defaulted or generated by Helm, a runner, DNS, the database Secret, or an
application heartbeat. For `EXISTING_CURRENT_HEAD`,
`dbAuthorityAnchor.historicalResetReceiptConfigMap` is also mandatory and must
already be immutable; `EMPTY_BOOTSTRAP` rejects that value. In the guard
namespace, the operator bootstrap creates
the empty mutable enforcement-receipt ConfigMap first with no ownerReferences,
finalizers, deletion timestamp, Helm ownership/resource-policy annotation, or
hook annotation. For an existing source it then executes the complete cold
reset above and creates its immutable retained receipt. Only afterward does it
create the immutable anchor, binding the enforcement receipt's namespace/name/
UID/initial resourceVersion and retention invariant plus the reset receipt's
namespace/name/UID/content/retention hash. Anchor contents come from the
database infrastructure control plane and an out-of-band direct connection to
known source A. The source evidence, both application-role grants, reset
receipt when required, empty enforcement receipt, and anchor are reviewed and
created before any transition chart renders. The database infrastructure must
guarantee that this direct identity names exactly one writable primary and that
every extant legacy application session and every resolution of the current
Secret reaches that same server. If this cannot be proved, activation blocks
and requires the separate two-endpoint transfer protocol excluded above; the
application may not choose a source by trust on first use. Database failover,
anchor replacement, or another credential rotation likewise requires that
separate protocol.

Every hook/runner revalidates the anchor's cluster, release-namespace, and
guard-namespace UIDs, release name, `release_instance_id`, every pinned database
role OID and attribute, Aurora administrator/`rds_superuser` capability receipt,
provider-role catalog, customer authority set, and membership/`SET ROLE`
closure. Before the first
guard transaction it requires the exact pinned source ownership/ACL/default-
privilege graph; that one transaction is the sole source-to-target edge. Every
later call requires the normalized target ownership, enabled/forced-RLS,
policy/trigger/function, default-privilege, and effective ACL hashes. A role
recreation, wrong phase graph, attribute or membership change, new assumption
path, owner change, RLS disablement, mutable function `search_path`, unexpected
grant, or default-privilege drift blocks before any later database or
Kubernetes mutation.

Every nonempty pre-recorder installation first performs one mandatory cold
`HistoricalAuthorityReset`; it is the sole existing-install phase-0 path, not
an optional stale-row exception. Before release cutover, platform IaC
provisions one KMS-encrypted DynamoDB `AuthorityEpochJournal`; the reset
operator has conditional-transaction access only to this release/installation
partition, and the later supervisor is read-only until adoption. Each entry
hashes its predecessor and stores generation, admission revision, closed plan,
request IDs, receipts, blocker, and exact anchored-A marker hash. Strongly
consistent reads and one conditional transaction per transition make this the
durable external reset journal; Kubernetes objects and process memory are not
reset state.

Before the irreversible edge, the administrator also installs one minimal
pre-schema marker kernel on A under a tentative run of the same checked
capability lock/probe (not the later anchor receipt): a dedicated
schema and append-only `database_authority_reset_markers` table owned by the
pinned nonlogin guard owner with zero `PUBLIC`/application privileges. This is
the only pre-guard database object added by reset and is included in source-head
and later anchor hashes. `ANCHOR_PREPARE/-40` adopts it into the permanent guard
catalog and never deletes its rows.

The journal drives one generation-CASed state machine:

`PLANNED -> INGRESS_CLOSED -> OLD_EGRESS_CLOSED ->`
`CUTOFF_PREPARED -> CREDENTIAL_REVOCATION_STARTED ->`
`CREDENTIAL_ISSUERS_REVOKED ->`
`OLD_SUBSTRATE_TERMINAL ->`
`EFFECT_FRONTIER_CLOSED -> RESET_RECEIPT_FINALIZED`.

`PLANNED` is tentative, not an authority snapshot. Before any irreversible
mutation, the operator installs generation-bound infrastructure admission that
denies creation or mutation of release writers, effect actors, hosts, and every
credential/issuer class, closes external ingress, applies the complete old-
substrate egress deny, and then stable-double-inventories the world below. Any
delta restarts planning under a new generation. A conditional DynamoDB
transaction first advances `OLD_EGRESS_CLOSED -> CUTOFF_PREPARED` and durably
records the plan hash, random cutoff nonce, and every deterministic provider
revocation request ID. The first irreversible anchored-A administrator
transaction then takes the complete relation/advisory lock order, inserts the
same nonce/plan/request-ID hash into `database_authority_reset_markers`, makes
the legacy role `NOLOGIN`, terminates and barriers all legacy sessions, and
records the database cutoff time. A lost database response is reconciled by
exact marker GET: the one matching row proves commit, no row permits replay of
the identical transaction, and any conflict blocks. Only after that proof does
a conditional DynamoDB transaction advance to
`CREDENTIAL_REVOCATION_STARTED`; there is deliberately no cross-system atomic-
commit claim. External issuer revocations use only those already committed
request IDs. The state advances to
`CREDENTIAL_ISSUERS_REVOKED` only after every provider receipt, policy/version
postcondition, and cached-credential denial probe is durable. Runtime teardown
cannot start from the in-progress state. No old database writer
can appear after the cutoff and no infrastructure writer can create a missing
actor or credential. Every later state transition revalidates the generation,
admission revision, full inventory hash, and previous request IDs; a concurrent
or stale operator can only fail its CAS.

That closed plan includes every `api_server_instances` row
without an age cutoff; all Deployments/ReplicaSets/Pods and infrastructure
runtimes; every nonterminal request, resource action, Serve/Jobs/controller/
optional-worker effect actor; and every reachable credential source or issuer.
The credential inventory includes rendered/current Secret references and
digests, projected and legacy Kubernetes tokens, kubeconfig certificate/exec
identities, AWS roles/sessions/access keys, GCP service-account unique IDs/
keys/tokens, vendor/API keys, credential-manager and user credential sources,
plugins, and copied credentials on remote managed-job controllers. An unknown
dynamic source, shared principal that cannot be revoked, unmanaged effect
actor, BYO runtime without a terminal infrastructure proof, or incomplete host
closure rejects the plan before the irreversible edge. Process rows are only
diagnostic (`LIVE_TRACKED`, exact external-runtime termination, or
`PRE_RECORDER_UNPROVED`); Pod absence, lease age, database staleness, and
request status never retire one.

The reset's ingress and out-of-band egress closures are reversible preparation
until the anchored-A cutoff-marker/legacy-`NOLOGIN` transaction above; they cover the complete old
execution substrate so arbitrary credentials already in memory cannot call a
provider during revocation. The fence is controlled by infrastructure authority
outside release RBAC and is tested against every egress path. After the locked
database cutoff it requires two zero-session samples, removes old AWS assume-
role trust and long-term keys, applies
`AWSRevokeOlderSessions`, and uses a distinct successor role; disables each old
GCP service account and deletes its keys, using a new unique ID; removes all old
Kubernetes subject bindings and legacy-token Secrets/ServiceAccounts, invalidates
bound tokens, and uses a distinct successor principal. Any Helm-owned legacy
ServiceAccount or binding is removed only by the pre-registered reset
principal's signed deletion set while release writes are frozen; adoption
imports those exact absences and permanently revokes that principal. The reset
also removes mutation RBAC from every static certificate subject and requires provider-side revocation
receipts for every vendor key. Secret deletion or byte rotation alone is never
evidence because a process may retain bytes. Successor authority Pods do not
mount the sequenced database Secret or provider credentials until
`ANCHOR_COMPATIBLE`. The administrator credential is a separate operator
transition class available only to the bounded guard Jobs. The recorder is the
only earlier sequenced-Secret consumer: it remains unmounted in
`ANCHOR_SCAFFOLD` and is projected only by the ordinary `ANCHOR_PREPARE`
recorder update after `-40` has committed `ENFORCED`; its runtime can obtain
only the recorder-scoped guard token and therefore cannot execute an authority
or provider effect. No old identity is ever re-enabled.

The operator then terminates every runtime that could contain an old effect
actor and proves each exact instance, task, VM, node/container, and remote
controller terminal through the infrastructure API, including objects absent
from Kubernetes. The reviewed Rainier plan uses an epoch-isolated replacement
authority substrate: old authority runtimes are drained/terminated under the
external egress fence, and new nodes carry a taint/toleration and principal that
no old ReplicaSet has. If safe isolation would require terminating an unknown or
shared host, the reset blocks before revocation; it never destroys an
unreviewed shared node. An old Pod or image therefore cannot restart while the
forward compatible release is prepared.

Runtime death and credential revocation close future calls but do not erase an
asynchronous call accepted just before the fence. The reset enumerates every
historical effect-capable request and provider mutation through provider audit/
operation watermarks and classifies it only as `DEFINITIVE_NO_EFFECT`,
`DEFINITIVE_EFFECT` with exact object/effect identity, or `AMBIGUOUS_DEBT`.
Kubernetes effects resolve by exact object UID/name; AWS, GCP, and other async
operations resolve through their provider operation/audit identities. Unknown
calls/resources are imported as durable legacy effect debt, permanently
tombstone their scope, count conservatively against occupancy/concurrency, and
block `SEQUENCED_ACTIVE` until the ordinary typed recovery proves adoption,
delete-accepted-plus-absence, or no effect. Every external step has a fixed
deadline that persists a typed blocked cursor; a deadline is operational
progress control, never safety evidence. Retry resumes the same generation and
provider request IDs.

After the frontier closes, `RESET_RECEIPT_FINALIZED` creates immutable retained
`HistoricalAuthorityResetReceipt` evidence in the guard namespace. It binds
release/cluster/source-A identity; cutoff database time; complete process,
actor, host, credential/issuer, request, and effect inventory hashes/counts;
old/new principal IDs and Secret UID/resourceVersion/digests; revocation request
IDs/timestamps/policy etags; exact terminal runtime IDs; egress-fence revision;
effect classifications/debt hash; final AuthorityEpochJournal partition/head;
and exact A-side cutoff-marker nonce/row hash/database time. The immutable
`DatabaseAuthorityAnchor`, created afterward, binds that receipt's namespace,
name, UID, content hash, and empty ownership/garbage-collection metadata. Its
source kind `EXISTING_CURRENT_HEAD` requires the receipt; `EMPTY_BOOTSTRAP`
forbids one. The A-side cutoff transaction is the irreversible boundary. A crash
afterward resumes forward from external request IDs and immutable evidence;
legacy login, old credentials, old runtime identities, and old images are never
restored. `BASELINE_COMMITTED` is deliberately not an external-reset state: it
is the later anchored-A database import committed only by
`ANCHOR_PREPARE/-40` and recorded in the enforcement journal. The immutable
reset receipt proves the closed external frontier, while the separate database
receipt proves that exact evidence was imported once.

An existing installation then takes three fixed chart revisions before
Serve041/API009 migration. `ANCHOR_SCAFFOLD` changes no database or authority
Deployment: it installs the ordinary managed Job/recorder identities and RBAC,
the immutable release-namespace `DatabaseAuthorityBundle`, and a dormant
termination recorder. `ANCHOR_PREPARE` leaves every authority Deployment
template byte-identical while `-40` verifies the reset receipt, normalizes
ownership/ACLs, permanently retires every pre-reset process row as
`AUTHORITY_RESET_RETIRED`, imports reset debt/tombstones, installs the stable
guard directly in `ENFORCED`, and commits the reset baseline. It never deletes
historical rows and never admits tokenless legacy SQL. It then arms the recorder
from that new baseline. `ANCHOR_COMPATIBLE` rolls the full compatible cohort on
the new authority substrate and completes the immutable enforcement receipt.
No old authority Pod is expected to remain Running; a pending old ReplicaSet is
ineligible for the new taint and principal. None of these revisions adds a
planner or changes broker protocol v2.

Every compatible API/controller/executor checkout connects through the anchored
direct `hostaddr`, validates the complete server/database tuple, and registers
an exact boot/session generation. The stable guard records anchor/reset
generations, boot/session identities, provider-effect/debt references, and
permanent process/Pod retirements; installs statement triggers and forced row
policies on the complete manifest; and requires a transaction-local random
guard token on every authoritative read/write and a guarded intent before every
external effect. Only the token hash is durable. `SET LOCAL` binds the token to
anchor, Pod UID, boot nonce, role, capability hash, and session generation;
retirement or generation advance revokes it forever. There is one database
guard state, `ENFORCED`; incomplete reset/ownership/cohort work is represented
by monotonic external/reset/enforcement journal cursors while all provider
effects remain closed. There is no `UNENFORCED`, tokenless bridge,
credential-refencing, legacy-login reset, or second steady-state policy branch.

`ANCHOR_SCAFFOLD` is a separate chart revision with no hook and no database
mutation. Its artifact-pinned scaffold render omits every pre/post-install and
pre/post-upgrade hook, including the otherwise existing `-10` database
migration Job; this is not a user-selectable chart value. Its rendered
authority Deployments, images, Secrets, and grace periods must be byte-identical
to the live release. It creates ordinary Helm-managed ServiceAccounts,
Roles/ClusterRoles, and bindings for three sets: successor role-specific API,
controller, and executor authority identities/RBAC that no current Deployment
references; transitional
`guard-prerequisite-check`, guard-migrator, recorder, recorder-ready, and
enforcement identities; and permanent
empty-bootstrap finalizer, transition-runner, and guarded-migration identities.
The old authority ServiceAccount and bindings are absent exactly as recorded by
the reset/adoption receipts and are never recreated. Successor projected tokens
do not exist until `ANCHOR_COMPATIBLE` references the new ServiceAccounts.
It also creates the ordinary immutable release-namespace
`DatabaseAuthorityBundle`, rendered from the exact immutable-anchor bytes in
the signed external read-set, and
the ordinary recorder Deployment in
an inert `WAITING_FOR_GUARD` mode with no database Secret mount or database
environment. Because these are ordinary Helm-managed
resources spanning the release and guard namespaces rather than hook resources,
later rendering and supervised decommission have normal Helm ownership;
the stacked cleanup PR, not the feature chart, removes only the transitional
set after its soak gate. The permanent set stays for future guarded upgrades
and fresh installation.
The operator-owned guard Namespace/Secret/anchor/receipt are never Helm-owned.

Only after scaffold ownership is verified does `ANCHOR_PREPARE` run fixed
hooks from the exact compatibility image. `pre-upgrade/-50`
`guard-prerequisite-check` proves every pre-existing managed identity/RBAC,
guard namespace, both application Secrets, anchor, receipt, exact current migration head, and
byte-identical source envelope. Its release-namespace Role includes exact-name
GET of the immutable `DatabaseAuthorityBundle`; it compares that mirror to a
fresh direct guard-anchor GET before succeeding. `pre-upgrade/-40`
`anchor-guard-migrate` uses the already-managed migrator ServiceAccount. Its
release-namespace Roles allow read-only GET plus namespace-scoped list of
Deployments/ReplicaSets/Pods, with code-level exact topology/owner filtering,
and GET of the two application Secrets. Guard-namespace Roles allow it to list Pods only in that
namespace and code-filter its exact Pod -> Job owner chain, use the stable Job
GET to verify its deterministic revision-bound Job, GET the anchor and receipt,
and exact-name GET plus mount of the administrator Secret. Cluster-scoped RBAC grants exact-name GET of only the
release, guard, and `kube-system` Namespace objects. Cross-namespace
RoleBindings grant no create/update/delete verb. It has no provider
credential, service endpoint, request worker, or workload mutation permission.
The migrator trusts its direct guard-anchor read rather than a mirror and gains
no bundle read; the prerequisite has already proved the live bundle immutable
and byte-identical to that anchor, including source-object UID/resourceVersion/
hash and retention fields.

Using direct anchored A, `anchor-guard-migrate` validates the pinned source
ownership/ACL/default-privilege graph. With a 30-second `lock_timeout`, no
pre-held SQL/advisory lock, and the complete relation lock order below, one
transaction materializes equivalent explicit legacy grants, transfers all
owners to their pinned nonlogin target, removes database/schema `CREATE` and
`TEMP`, rewrites default privileges, installs/revalidates the stable guard
schema/functions/`ENABLE ALWAYS` event triggers/forced policies/two catalogs,
verifies/imports the immutable Historical Authority Reset receipt, marks every
pre-cutoff process permanently `AUTHORITY_RESET_RETIRED`, imports effect debt,
sets the immutable recorder baseline generation, and commits the guard directly
as `ENFORCED`. The reset has already made the legacy role `NOLOGIN`, terminated
its sessions, and closed old provider egress, so no tokenless policy or bridge
deadline exists. Any lock timeout, reset-receipt/ownership/grant/head/definition
mismatch rolls back the entire source-to-target transaction while the external
reset remains irreversibly fenced for forward retry. A nonempty source must
already equal all pinned pre-feature heads; phase 0 never runs a historical
migration.

The preparation revision then wakes the already-managed
`phase0-termination-recorder`; `post-upgrade/0` `phase0-recorder-ready` requires
its gap-free cursor while re-proving the unchanged authority envelope. The
next `ANCHOR_COMPATIBLE` revision reruns `-50/-40` as read-only exact target
revalidation, requires a fresh recorder lease before any authority Deployment
template changes, rolls all three roles, and only then runs `post-upgrade/0`
`anchor-enforcement-runner`. A failure before `ANCHOR_PREPARE`'s mutating `-40`
transaction leaves the source ownership graph unchanged, but the legacy cohort
is already terminal and its credentials/egress irreversibly fenced by the
reset; the similarly named `ANCHOR_COMPATIBLE/-40` performs no mutation. After the preparation commit the
supported operation is forward repair. An old chart can remove the ordinary
recorder only by changing managed topology, but cannot restore the reset's old
egress, credentials, substrate, legacy login, or permanently retired process
identities. Recorder loss blocks new-baseline retirement evidence; any
unclassified effect remains debt and blocks enforcement.

| Revision/lifecycle | Component | Contract |
| --- | --- | --- |
| `ANCHOR_SCAFFOLD` ordinary resources | transitional phase-0 and permanent finalizer/runner/migration identities/RBAC, immutable authority bundle, plus dormant recorder | Establish Helm ownership and isolated guard-namespace access without a database or authority-Deployment change |
| `ANCHOR_PREPARE pre-upgrade/-50` | `guard-prerequisite-check` | Validate scaffold, external identities, exact source envelope/heads, and the source ownership graph |
| `ANCHOR_PREPARE pre-upgrade/-40` | `anchor-guard-migrate` | Verify/import the Historical Authority Reset, atomically normalize ownership/ACLs, install the permanent `ENFORCED` guard, and capture the post-reset process/Pod baseline |
| `ANCHOR_PREPARE` ordinary resource | `phase0-termination-recorder` | Watch from the post-reset Pod-list resourceVersion and append process termination evidence |
| `ANCHOR_PREPARE post-upgrade/0` | `phase0-recorder-ready` | Prove a gap-free watch and byte-identical application envelope |
| `ANCHOR_COMPATIBLE pre-upgrade/-50,-40` | prerequisite/guard revalidation | Require exact target ownership, managed RBAC, live recorder, and unchanged source identity before any rollout |
| `ANCHOR_COMPATIBLE post-upgrade/0` | `anchor-enforcement-runner` | Prove the complete compatible cohort/effect/debt inventory under the reset generation and finalize the receipt chain |

Every phase-0 Job runs in the isolated guard namespace with a distinct fixed
ServiceAccount. `guard-prerequisite-check` and `phase0-recorder-ready` may list
Pods only in that namespace and then code-filter the exact Pod -> Job owner
chain. Because the Role is installed before future pre-upgrade hooks, every
fixed Job identity has namespace-scoped `get` on `batch/jobs` with no
list/watch/write; code accepts only the deterministic revision name, returned
UID, Pod owner chain, and image digest. The prerequisite check has
release-namespace read-only topology list/GET and exact-name GET of the two
application Secrets and `DatabaseAuthorityBundle`, guard-namespace GET of
anchor/receipt and GET without
list/write for the deterministic revision-evidence name,
and exact-name GET of the scaffold-managed ServiceAccounts, Roles, and
RoleBindings in each namespace. Its cluster-scoped RBAC grants exact-name
GET of the three Namespace objects and every scaffold-managed ClusterRole and
ClusterRoleBinding, with no list/watch. It
reads the exact legacy application Secret through the Kubernetes API, without a
cross-namespace volume mount, to verify current heads and source ownership.
Recorder-ready has release-namespace read-only GET/list/watch for the complete
authority envelope and recorder Deployment/ReplicaSet/Pods plus exact-name GET
of the sequenced application Secret and `DatabaseAuthorityBundle`,
guard-namespace anchor/receipt GET, and the same exact-name Namespace GETs. It
reads that Secret and bundle through the Kubernetes API,
without a cross-namespace volume mount, to verify the guard receipt and durable
watch cursor. Neither identity has a Kubernetes write, namespace list/watch,
application/admin-Secret volume mount, provider credential, workload verb, or
authority membership.

The prerequisite has no guard-namespace Secret verb: Kubernetes Secret GET
would reveal `data`, not metadata alone. `ANCHOR_PREPARE/-40` is already in the
trusted administrator set and, before its first database mutation, validates
the mounted admin Secret's exact name/UID/resourceVersion/content digest against
the anchor. Every later admin-authorized Job repeats that validation itself.

`phase0-termination-recorder` is an ordinary one-replica Deployment in the
release namespace and outside the authority cohort. Its scaffold form has no
database credential and can only wait. After `ANCHOR_PREPARE/-40` commits
`ENFORCED`, that revision's ordinary-resource update mounts the same-namespace
sequenced application Secret and immutable
`DatabaseAuthorityBundle` read-only. At startup and on every database checkout
it validates the bundle's complete direct source identity/hash against the
stable guard on anchored A; a missing, mutable, stale, or mismatched mount
closes its heartbeat and append path. It has no guard-namespace RBAC, guard
administrator Secret, provider credential, or projected cross-namespace token.
Its nonconfigurable ServiceAccount can only get/list/watch
release-namespace Pods, ReplicaSets, and Deployments, read its own exact
Deployment/Pod owner chain, and GET the exact release Namespace through
cluster-scoped RBAC; mounted same-namespace Secret/ConfigMap delivery requires
no Kubernetes Secret or ConfigMap GET verb. It has no Kubernetes write,
service endpoint, controller role, or request worker. It connects to anchored A
with the sequenced Secret and a recorder-only guard token whose
`SECURITY DEFINER` function can append, but not update/delete, termination
evidence or renew any other generation. The guard transaction imports every
pre-cutoff `api_server_instances` row as a permanent
`AUTHORITY_RESET_RETIRED` record from the immutable reset receipt, without
deleting history, and stores a complete post-reset Pod list plus collection
resourceVersion. The recorder is responsible only for boots at or after that
baseline. An absent post-reset Pod or stale row still blocks; absence never
proves death. A process that never registered never reached provider-capable
dispatch. The recorder watches from the captured resourceVersion and records Pod UID, node name, image digest,
container name/ID, restart count, kubelet-reported terminated reason/exit code/
finished time, and source resourceVersion. A restart adds a generation; it
never overwrites one. Kubernetes `>=1.27` is a phase-0 render/runtime gate.
Force deletion, static-Pod exceptions, API compaction, a watch gap, recorder
identity change, a post-baseline pre-list ghost, or deletion without a preceding
exact terminated state sets durable `RECORDER_REBASE_REQUIRED` and closes every
effect-start predicate; there is no age/absence inference. The supervisor then
admits only the two typed fix-forward rebase intents. The same external
`AuthorityEpochJournal` starts a higher generation in `POST_RESET_REBASE` mode:
it freezes ingress/release writes, applies the current-substrate egress deny,
inventories every post-baseline boot/effect/credential, proves all exact
runtimes terminal, rotates the provider/Kubernetes principals, and classifies
the complete effect frontier into effect/no-effect/debt. The sequenced database
Secret need not rotate: every old boot/session/Pod guard identity is permanently
revoked and the transaction-local guard token cannot be recreated by a terminal
runtime.

The immutable `AuthorityEpochRebaseReceipt` chains the prior baseline and the
new journal/marker/effect hashes. A supervised `ANCHOR_REBASE_PREPARE` admin
hook imports it, records all covered permanent revocations/debt, and creates a
fresh recorder list/resourceVersion baseline while the guard stays
`ENFORCED`/maintenance. `ANCHOR_REBASE_COMPATIBLE` then starts the complete
cohort on a fresh epoch-isolated substrate and completes the ordinary
enforcement receipt before `resume`. A crash resumes those receipts and never
restores an earlier principal or baseline. Thus evidence loss is a disruptive
fix-forward rebaseline, not an indefinite stop or a second steady-state path.
Tests never infer death from Pod/API absence. The recorder remains live through compatibility,
feature rollout, and the 24-hour removal soak; only the stacked cleanup PR
removes its managed runtime/RBAC while retaining append-only evidence and
retirement tombstones. Cleanup rejects any open rebase generation and can merge
only after the final uninterrupted soak.

The compatibility chart uses the scaffold-managed dedicated
`anchor-enforcement-runner` ServiceAccount/RBAC for its post-upgrade Job; it only validates and
mounts the operator-pre-created exact-name
`DatabaseAnchorEnforcementReceipt` ConfigMap. The
post-upgrade Job from the exact compatibility image is the only caller of the
zero-argument `enforce-anchor` operation. It runs in the isolated guard
namespace, may list Pods only there and code-filter its exact Pod -> Job owner
chain, then use the stable Job GET to verify its deterministic revision-bound
`batch/jobs` object.
Release-namespace RoleBindings grant read-only GET plus namespace-scoped list of
Deployments/ReplicaSets/Pods, with exact topology/owner filtering in code, and
GET of the two application Secrets plus exact-name GET of the immutable
`DatabaseAuthorityBundle`;
guard-namespace RBAC grants anchor GET and `get/update` on only that one receipt
ConfigMap via `resourceNames`, plus exact-name GET of the administrator Secret;
it cannot create/delete ConfigMaps or mutate a workload. Cluster-scoped RBAC grants exact-name GET of only the release, guard,
and `kube-system` Namespace objects. It mounts that administrator database Secret but no provider
credential, request worker, service endpoint, or controller authority. It
proves its Job/Pod owner chain and exact image digest before a guard mutation.
The permanent transition runner has read-only access to the enforcement receipt;
ordinary API/controller/executor identities have no guard-namespace RBAC and
validate the release-namespace bundle against PostgreSQL instead. The Job uses
`before-hook-creation,hook-succeeded` deletion; a failed Job remains only for
evidence and a newer revision gets a distinct Job identity. The feature chart
retains the inert transitional identities/recorder while validating the
immutable receipt; only the soak-gated stacked cleanup chart stops rendering
that transitional set. The permanent finalizer, transition-runner, and
guarded-migration identities remain. The
operator-owned guard namespace, Secret, anchor, and receipt remain.

The guard migration imports the reset's complete prior-process and effect
frontier. Every old process is retired because the exact old execution
substrate is terminal under infrastructure proof, old credentials and issuers
are revoked, old egress remains denied, old database sessions are gone, and
any already-accepted provider call is represented by exact completion/effect
identity or durable debt. None of those facts is inferred from Kubernetes or
database absence. A debt row blocks activation until the same typed provider
recovery resolves it; it is never silently terminalized. The exact compatible
cohort then registers on anchored A using only successor credential/principal
epochs. If a post-baseline guard-aware compatibility boot dies before ACK, the
runner proves zero A sessions/advisory locks/live workers/leadership, resolves
every captured effect to completion or debt, verifies its exact recorder
termination evidence, and permanently records `PHASE0_BOOT_RETIRED`. An
overlap, unclassified effect, or source ambiguity blocks. A retired old or new
boot can never register, read authoritative rows, write, or start an effect.

After the exact compatibility cohort is stable, the enforcement Job requires
every non-retired boot to stop/join workers, release leadership, close pooled
sessions, reconnect only to A, and ACK two advancing anchor-bound heartbeat
generations. It revalidates the immutable reset receipt, permanently revoked
legacy login/credentials/issuers, infrastructure terminal set, egress-fence
generation, out-of-band single-writable-server proof, complete role/ownership/
ACL/RLS closure, exact Kubernetes cohort, and all reset plus post-baseline
effect/debt coverage. With new effects still closed, it proceeds directly to
the final relation-lock barrier and immutable enforcement receipt. A crash
resumes the same cursor; it cannot restore a legacy principal or old runtime.

The migration pins two complete but type-specific catalogs. The lockable
guarded-relation manifest is exactly every central-database base,
partitioned, or partition table on which either application role has an
effective table- or column-level `SELECT`, `INSERT`, `UPDATE`, `DELETE`, or
`TRUNCATE` path. It includes all API request/queue/resource-action/server/
leadership tables and every Serve service/version/replica/lifecycle/claim/
round/lease/capacity/resource-action/history table. Migration bookkeeping and
the guard singleton/append-only evidence tables are excluded only because both
application roles have zero direct table/column privilege on them; access is
through exact pinned guard-owner functions.

The complete protection catalog additionally covers database and schema
owners/ACLs; `pg_class` tables, partitions, views, materialized views,
sequences, and indexes; `pg_attribute.attacl` column grants; constraints,
triggers, RLS policies, and partition bounds; functions/procedures and their
owner, language, volatility, security mode, configuration, definition, and
execute ACL; application-usable types/domains and their ACLs; plus all
per-owner/per-schema default privileges. Tables require enabled/forced RLS and
the exact guard trigger/policy set. An application-readable view must be
security-invoker/barrier-safe with a pinned definition and protected underlying
relations. Sequences, types, indexes, and constraints have their own exact
owner/ACL/definition invariants and are never subjected to nonsensical RLS
requirements. Every catalog entry stores type, OID, schema/name, dependencies,
effective privileges, and normalized definition hash; both the stored set and
a fresh privilege-derived enumeration must match exactly.

The final enforcement transaction sets a 30-second PostgreSQL `lock_timeout`
and, while holding no other SQL/advisory/application lock, takes `SHARE ROW
EXCLUSIVE` on every entry of the lockable relation manifest in bytewise
schema/name order, then re-derives the complete protection catalog. An added,
missing, renamed, repartitioned, unguarded, newly executable, or newly usable
object blocks. This table-first order accounts for PostgreSQL acquiring a DML
table lock before a statement trigger can lock the guard.

Only after all relation locks are held does the transaction lock the guard
singleton `FOR UPDATE`, then recheck the anchor, committed Historical Authority
Reset generation/receipt, permanently revoked legacy identities, exact
cohort/retirements, session/leadership/effect/debt inventory, complete role/RLS/
ACL/default-privilege closure, both protection catalogs, and zero old writer. It atomically commits `ENFORCED` plus the old-writer-flush
and cohort receipt. A timeout or deadlock rolls back the whole transaction,
changes no receipt, leaves effects/legacy login closed, and retries from a new
database-time attempt after releasing every lock. From the commit, forced row
policies deny an unregistered or stale boot even `SELECT` access to recoverable
state, and triggers reject every wrong-anchor/session write. Thus an actual
prior binary using its revoked Secret cannot connect; even if deliberately
given the sequenced Secret, it lacks a valid guard token and cannot read a
persisted launch/cleanup row before provider I/O. No external effect may start
without a guarded read/claim and intent transaction.

Finally, the Job fills the pre-created enforcement ConfigMap by one
resourceVersion CAS that requires the anchor's initial UID/resourceVersion and
empty garbage-collection metadata, changes only `data` plus `immutable`, and
sets it immutable. The canonical payload binds only values known before that
request: object UID and initial resourceVersion, committed guard
generation/receipt hash, `release_instance_id`, anchor hash, full
old/new/retired boot inventory, drained session generations, and database
timestamp. It contains no digest of itself and does **not** contain the
API-assigned final resourceVersion. `payload_sha256` is SHA-256 over the exact
RFC-8785 canonical UTF-8 payload bytes. The ConfigMap data is exactly
`payload.json=<those bytes>` plus `payload.sha256=<lowercase hex digest>`;
the forbidden ownership metadata is any ownerReference/finalizer/deletion
timestamp, `helm.sh/hook`, `helm.sh/hook-delete-policy`,
`helm.sh/resource-policy`, `meta.helm.sh/release-name`, or
`meta.helm.sh/release-namespace` annotation, or a Helm-managed-by label.
`object_content_sha256` is SHA-256 over the canonical tuple
`(apiVersion,kind,namespace,name,uid,immutable=true,ownerReferences=[],`
`finalizers=[],deletionTimestamp=null,forbiddenOwnershipMetadataAbsent=true,`
`data)` and excludes all other Kubernetes metadata, including resourceVersion.
Before CAS, PostgreSQL
stores both expected digests. After the update, the Job GETs the immutable
object, recomputes them, and writes a monotonic PostgreSQL Kubernetes-receipt
acknowledgement containing its returned final resourceVersion, UID, immutable
bit, empty owner/finalizer/deletion/forbidden-annotation proof, payload digest,
and object-content digest. A lost update response is
recovered by that GET; matching immutable content is success, while mutable or
conflicting content fails. A crash after the database receipt, ConfigMap CAS,
or acknowledgement resumes the same bytes/cursor. If `-25` exhausts after the
immutable CAS but before acknowledgement, the next chart correctly omits `-25`.
Its `-30` accepts only the exact database-registered expected bytes; at the very
start of `-20`, the stable pre-schema `SECURITY DEFINER` ABI permits the
sequenced role to GET that exact object and monotonically store only the missing
UID/final-resourceVersion/content/retention acknowledgement. It cannot change
the guard, gate, effect fence, or any payload during this recovery. Every `-20`
then requires the PostgreSQL enforcement receipt, immutable ConfigMap, and
final-resourceVersion acknowledgement to agree before any other action or
migration. Later hooks validate that three-part chain and retention metadata
rather than expecting the initial resourceVersion to remain current. Hook Job
deletion and supervised release decommission must leave the same receipt UID/content GETtable.

Every schema change after guard enforcement, including Serve041, API009, later
additive migrations, and final cleanup, uses one `GuardedSchemaMigrator` ABI.
The preceding `-20` transaction sets an orthogonal monotonic DDL generation to
`DDL_CLOSED`, which makes guard policy reject new claims/effects while carried
completion/debt may finish. That transaction records the exact
`release_instance_id`, Helm revision, target hash, migration Job name, and image
digest allowed to claim the new generation. After proving its own exact
Job/Pod owner chain, `-10` atomically exchanges that claim for a database-minted
256-bit, single-generation migration token; only its hash is stored and the
plaintext exists only in that Job's session. The claim also stores an ordered,
token-bound expected-change manifest: command tag, normalized target identity,
allowed old hash, required new type/owner/ACL/dependency/protection hash, and
whether the object is created, altered, or dropped. An `ENABLE ALWAYS`
`ddl_command_start` event trigger authenticates the token/administrator and
requires the next declared operation before execution. An `ENABLE ALWAYS`
`ddl_command_end` trigger inspects `pg_event_trigger_ddl_commands()` and requires
the actual created/altered objects to match that declaration; `sql_drop` does
the same for dropped identities. Each event marks exactly one operation
consumed. A guard-owned `DEFERRABLE INITIALLY DEFERRED` constraint trigger on
the migration-claim row runs at transaction commit and rejects unless every
declared operation was consumed, every resulting object has its required
protection/catalog entry, both complete catalog hashes match, and the generation
was explicitly sealed. Thus even an administrator holding a valid token cannot
commit undeclared or unmanifested DDL. Exact pinned PostgreSQL-internal/
extension exclusions are declarations too, never an open wildcard. Shared-object role DDL is
separately caught by the live role-closure predicate because PostgreSQL does
not expose it reliably to event triggers. Application roles have neither
database/schema `CREATE` nor event-trigger disable authority. The migration Job then locks the current guarded-relation manifest
in the same bytewise `SHARE ROW EXCLUSIVE` order, locks the guard singleton,
and revalidates the anchor, role closure, effect fence, source receipt, and
both protection-catalog hashes. Nontransactional DDL, including concurrent-index creation, is
outside this ABI and blocks until a separately reviewed protocol is added.

PostgreSQL does not fire event triggers for commands that create/alter/drop the
event triggers themselves. The exact start/end/drop trigger set is therefore
created only by the initial trusted guard transaction, pinned in its receipt,
and is not an allowed `GuardedSchemaMigrator` target. Any later OID, owner,
definition, tag, or enabled-mode mismatch closes DDL/effects and requires a
separate reviewed guard-kernel migration; ordinary feature or cleanup DDL
cannot suppress its own enforcement.

Within one PostgreSQL transaction, every new object is first created with the
guard owner (or another catalog-pinned non-application owner) and no `PUBLIC`
or application-role privilege. A table/partition receives enabled/forced RLS
and guard policies/triggers; a view, sequence, function/procedure, type/domain,
index, or constraint receives its type-specific protection and definition
hash. Only after the object and all dependencies satisfy the complete
protection catalog does the transaction add its lockable entry, if any, and
grant the exact sequenced-role privilege. An altered or dropped object has
every application path revoked before its protection or catalog entry changes.
The transaction atomically advances both catalogs and the DDL generation; any
DDL, policy, trigger, owner, grant, dependency, or hash failure rolls back all
schema/catalog changes and leaves the prior generation current. The
one-use claim is consumed only by a committed matching catalog generation;
retry of a rolled-back transaction reacquires a new token for the same closed
generation, while a stale token, Job UID, target, or revision cannot commit.
`-5` and final cohort attestation require the committed generation, and one
short guard transaction returns DDL state to `OPEN` only after every capable
boot echoes it. A crash stays effect-closed and a newer fix-forward Job resumes
the same generation. No migration--including cleanup--may create, alter, grant,
drop, or make an application-readable object outside this protocol.

Historical central-database migrations are not retrofitted into this ABI.
Phase 0 accepts a nonempty database only at the exact pinned pre-feature heads
for every central lineage; a lagging head must first be upgraded by the old
release before anchor preparation. For an exactly empty PostgreSQL database,
`-10` uses one checked-in, versioned `CurrentHeadPostgresBaseline` instead of
replaying the historical Alembic chain. The baseline covers every lineage
initialized by the central migration entrypoint--global user state, SkyPilot
configuration, Serve, managed jobs, PostgreSQL API requests, lifecycle
actions, physical capacity, key-value cache, and recipes. The implementation
first moves the currently lazy `kv_cache_db` and `recipes_db` lineages into the
canonical central migration entrypoint and makes their runtime initialization
verify-only in server PostgreSQL mode. This nine-lineage enumeration is closed;
adding another central lineage requires changing the baseline/catalog contract
in the same reviewed migration. The baseline creates
ordinary indexes non-concurrently, applies type-specific guard protection
before any application grant, and stamps all exact current heads in the same
transaction as the two catalogs/DDL generation. The baseline artifact and
lineage enumeration are one generated-and-reviewed source; omission of a
lineage or object fails the privilege-derived catalog comparison. CI builds an
independent disposable database through the complete historical migrations,
normalizes definitions and ACLs without comparing allocated OIDs, and requires
it to equal the empty-database baseline at the same heads. Production empty-
database bootstrap executes only the transactional baseline.

A truly fresh install still requires the externally provisioned anchor and
empty enforcement ConfigMap, but has no compatibility rollout. Its first Helm
revision is `BOOTSTRAP_SCAFFOLD`. Its artifact-pinned render is hookless and
cannot be selected by a user value. It creates only the permanent managed
empty-bootstrap-finalizer, transition-runner, and guarded-migration
identities/RBAC in the empty release namespace and already-provisioned guard
namespace plus the ordinary immutable release-namespace
`DatabaseAuthorityBundle` rendered from the signed exact anchor read-set--no transitional
recorder resources, authority Deployment, or database Job. The required next
upgrade's `-30` verifies that bundle against the anchor before running the
complete fixed
`-30/-25/-20/-10/-5` chain and selects the `-25`
`anchor-evidence-finalize` writer only when the immutable anchor says
`EMPTY_BOOTSTRAP`. That Job runs in the guard namespace, mounts the admin
Secret, and has exact-name update permission on the same-namespace receipt.
It proves there is no source Deployment/Pod/session and the database is exactly
empty, requires the anchored legacy role already be `NOLOGIN`, transactionally
creates the stable guard directly in `ENFORCED` state with the pinned role/RLS/
default-privilege closure, then writes the PostgreSQL receipt, immutable
ConfigMap, and final-resourceVersion acknowledgement described above.

For `EXISTING_CURRENT_HEAD`, phase 0 must already have made the enforcement
receipt immutable and complete, so no `-25` Job or nonempty-validator identity
exists. `-30` verifies the external chain read-only and `-20` revalidates and
records it in the revision receipt through the stable pre-schema ABI. A missing,
mutable, or mismatched receipt blocks rather than selecting another writer. For
an `EMPTY_BOOTSTRAP` origin after its first successful finalization, the same
complete-receipt rule omits `-25` on every later upgrade. The final cleanup chart
retains only the conditional empty-bootstrap finalizer ABI for future
installations. The anchor is permanent and is
never reconstructed from a target chart. Thus an A-effect/B-heartbeat split
can neither create nor satisfy the root of trust, and a fresh installation does
not strand an empty mutable receipt for its next upgrade.

After anchor enforcement, every API/controller/executor PostgreSQL client uses the
anchor's direct `hostaddr` plus the pinned sequenced-application Secret, validates the complete
server/database tuple on every connection checkout, and tags
`application_name` with role, Pod UID, boot nonce, and DB-session generation.
The five-second topology attestation rereads the same-namespace immutable
bundle and Secret metadata and compares the bundle's source anchor tuple/hash
with the PostgreSQL guard/singleton.
Registration, heartbeat, leadership, request claim, provider-effect start,
transition-lease renewal, journal write, and final resume all require the boot
tuple and checked-out connection to equal the singleton/anchor. A DNS change
therefore cannot redirect a new connection to clone B; if pinned source A is
unreachable, the fleet remains fenced rather than adopting B.

Every planned transition first connects to and closes the gate on pinned source
A, never on a rendered candidate database. Each live boot stops/joins effect
workers, releases leadership, closes every pooled/session connection except
one boot-fenced transition channel to A, advances its DB-session generation,
and echoes a fresh source challenge. An ACKless boot is excluded only by
inserting its permanent Pod-UID revocation on A after all carried effects are
identified. An absent, partitioned, or `Terminating` Pod receives the same
source-A revocation and exact effect/debt proof; candidate-side absence is
never evidence. Already-open transactions serialize through the singleton
before gate closure, and later A-side transaction/checkout predicates reject
the revoked UID. Thus neither a ghost A session nor a B heartbeat can authorize
the transition.

The same source-anchor challenge/session-generation proof runs before any new
Pod, replacement boot, or coordinator takeover joins active inventory. Every
boot lease and protected target envelope carries the anchor tuple. Secret,
anchor, endpoint, database, or schema changes fail before effect closure and
require the separate database protocol.

A `fleet_maintenance_runs` row stores its generation, reason
(`PLANNED_UPDATE` or `MEMBERSHIP_RECOVERY`), database deadlines, source
inventory, pinned source database anchor and challenge/session-drain coverage,
target rendered envelope revisions, current Deployment/ReplicaSet
resource versions, complete desired Pod set, acknowledgements, quiescence and
leadership-release receipts, excluded boots, permanent revocations, and final
inventory hash. Target-envelope updates are monotonic journal entries under the
same maintenance generation, allowing a broken target image to be replaced by a
newer fix-forward image without resetting authority or opening another path.

API revision 009 adds complete runtime capability and process-boot identity to
`api_server_instances` and immutable
`api_server_process_boot_leases`. A fresh UUID boot nonce is generated on every
container/process start and is distinct from the reusable Downward-API Pod UID.
A guarded registration/heartbeat atomically publishes role, Pod UID, boot
nonce, image digest, implementation SHA, schema capabilities, resolved request
storage and queue types, local mutation maximum/input hash, gate
acknowledgement, complete central-database identity/challenge echo, and one
database timestamp/payload hash. The current instance
row is only a projection; immutable boot history remains while any request or
effect refers to it.

Same-Pod process restart uses nonce-scoped supersession, not Pod revocation. A
new nonce under an unrevoked Pod UID registers only as `STARTING`. On pinned
source A, every connection is tagged with the old nonce/session generation, so
recovery can prove the old process has no PostgreSQL session, advisory lock,
leadership generation, live worker, or unclassified effect. It also requires
the Kubernetes container ID/restart count to identify the replacement and
every carried execution to be completed or explicit debt. One singleton-first
transaction then terminalizes the old immutable boot lease as
`PROCESS_RETIRED`, advances the Pod's current-boot projection, and admits the
new nonce to maintenance membership. Every later transaction under the old
nonce is permanently rejected. If old/new processes or sessions overlap, or
absence cannot be proved, the new boot remains effect-ineligible and the
Deployment is fix-forward rolled to a new Pod UID; only that Pod retirement
uses permanent UID revocation.

Serve041 also adds globally keyed `retired_pod_uid_revocations`. Each row stores
the Pod UID plus original release, namespace, role, retirement generation,
reason, and immutable evidence hash. A Pod UID is never reused by Kubernetes,
so this compact safety row is permanent. Detailed per-boot retirement evidence
may be collected only after the Pod object is absent and no execution, request,
leadership, or maintenance row refers to it; the global revocation remains.
Registration and legacy conflict-update triggers check the revocation before
trusting a claimed role or boot nonce. Every heartbeat, readiness publication,
leadership acquire, request claim, provider effect, maintenance-lease
acquire/renew, and journal commit rejects every boot under a revoked Pod UID.
Only an exact already-started completion/debt record may finish while its
immutable reference remains. A force-deleted ghost or a fresh boot nonce under
the same UID therefore cannot return after API absence, detail GC, or gate
changes.

The lock order extends the canonical order above with: protocol singleton;
retired Pod UIDs in byte order; instance projections by role/Pod UID; boot
leases by role/Pod UID/boot nonce; fleet-transition coordinator lease; maintenance
journal; leadership row; and then the existing observation, scheduler, replica,
and API-request suffix. Registration, maintenance, activation, and provider
effect transactions use this order. The singleton serializes an absent-row
revocation insert against registration, so a unique-index race cannot reopen a
retired identity.

Serve041 is installed before API009. API009 statement-level triggers take the
protocol singleton before every insert, update, conflict-update, or delete on
server instances and authority-bearing request fields. An old writer that
started first commits wholly before the preparing generation; a writer that
starts second sees the gate and is rejected. Under `LEGACY_ACTIVE`, a legacy
heartbeat first checks permanent Pod revocation, then clears all capability and
boot acknowledgement projections rather than preserving a capable row. Under
any later state, legacy registration and every unsequenced zero-cost/provider
transition fail. Old-statement-first/gate-first tests cover insert,
`ON CONFLICT DO UPDATE`, heartbeat, request transition, and delete.

##### Runtime and chart contract

`FleetMaintenanceCoordinator` runs in every capable API MAIN role supervisor,
independent of controller leadership. One shared database-time
`fleet_transition_coordinator` lease (15 seconds,
renewed every five) selects the writer. It starts after synchronous
`ServerInstanceLease` registration and before role dispatch. Kubernetes reads
happen outside SQL/advisory locks; a short transaction revalidates resource
versions, coordinator lease generation/expiry, gate generation, and complete
payload. A stale owner can finish a read but cannot publish. The coordinator
has no planner, request claim, mutation permit, or provider adapter and cannot
start an effect.

Every lease or journal mutation follows the global row order: protocol
singleton, any revocations/process boots, the one transition-coordinator lease,
then the journal and leadership row. It validates the lease token, generation,
database expiry, and gate generation in the same short transaction. Owners do
Kubernetes reads and all waits without SQL/advisory locks. A target runner may
take over from an API coordinator in one transaction that first closes the
effect-start gate, advances the lease generation, and records the handoff; the
old token can no longer renew or publish. A crashed owner therefore delays a
successor by at most the 15-second database lease, and concurrent API, hook,
and coordinator callers converge on one journal cursor.

The chart supplies one permanent `transition-runner` identity from the exact
target API image, invoked in three bounded phases (`-30`, `-20`, and `-5`)
around the existing migration Job, plus the conditional prerequisite
anchor-evidence finalizer. All call the same stable
library; they are ordered phases of one transition, not alternative state
machines. Fixed hook weights are:

| Weight | Hook | Contract |
| --- | --- | --- |
| `-30` | `transition-runner prerequisites` | Validate the scaffold-managed fixed identities/RBAC, immutable rendered-target payload, and pre-existing authority anchor/receipt identities. Before Serve041 exists, also validate the deterministic pre-schema revision-receipt name and require it absent or already immutable and byte-identical to its database registry on retry; once Serve041 exists, validate the PostgreSQL journal identity and require no Kubernetes revision receipt. Accept a registered exact enforcement receipt whose final Kubernetes acknowledgement is pending |
| `-25` | anchor evidence, conditional | Render `anchor-evidence-finalize` only when the anchor proves `EMPTY_BOOTSTRAP` and the pre-created enforcement receipt is both empty and mutable; omit it for every complete receipt and every nonempty source; mutable-nonempty or immutable-empty is corrupt and blocks |
| `-20` | `transition-runner fence` | First recover any missing monotonic PostgreSQL acknowledgement from the exact immutable enforcement receipt, then require the complete anchor-enforcement chain for every source before closing effects or proving the applicable legacy/bootstrap precondition |
| `-10` | identity-verified database migration Job | Revalidate the fence receipt/database tuple, then apply the guarded additive migrations on a pinned nonempty source or the transactional all-lineage current-head baseline on an exact empty install |
| `-5` | `transition-runner attest-target` | Verify schema head and journal the complete target envelope before any Deployment mutation |

The executable hook matrix is closed:

| Release/database state | Rendered chain | Required result |
| --- | --- | --- |
| Existing current-head source after `ANCHOR_COMPATIBLE`, before Serve041 | `-30,-20,-10,-5` | `-30/-20` validate the immutable phase-0 receipt; `-10` installs Serve041/API009 |
| Any database-bearing attempt after `BOOTSTRAP_SCAFFOLD`, anchor `EMPTY_BOOTSTRAP`, receipt still both empty and mutable (initial attempt or newer-revision pre-CAS retry) | `-30,-25,-20,-10,-5` | `-25` resumes/creates the enforced guard and completes the receipt; `-10` installs the transactional current-head baseline |
| Retry after `-25` made the exact receipt immutable but crashed before PostgreSQL final-resourceVersion acknowledgement | `-30,-20,-10,-5` | `-30` accepts only the database-registered expected bytes; `-20` GETs the exact object and completes the monotonic acknowledgement before any fence action |
| Second or later revision from an `EMPTY_BOOTSTRAP` origin, receipt immutable/acknowledged | `-30,-20,-10,-5` | Omit the finalizer even though immutable `source_kind` remains `EMPTY_BOOTSTRAP`; validate through the stable receipt/guard |
| Cleanup and every post-cleanup active/maintenance upgrade | `-30,-20,-10,-5` | Use PostgreSQL transition receipts and the permanent external chain; no phase-0 action or nonempty validator exists |

For split-role PostgreSQL HA, chart rendering rejects a missing matching
scaffold revision, a disabled migration Job,
a missing pre-existing database Secret, user overrides of these hook
annotations/weights, omission of any of the three transition-runner phases, omission of
`-25` when the anchored empty-bootstrap receipt requires it, or presence of
`-25` otherwise. The intent builder selects that closed render from its signed
anchor/receipt read-set; the chart performs no live `lookup`.
`-30/-25/-20` independently revalidate the live anchor/receipt/database state,
and `-20` contains the only acknowledgement-recovery seam described above, so
the signed read-set is never runtime authority. The signed intent contains complete canonical
values; runtime `--reuse-values` is forbidden and no imported predecessor value
can bypass the convoy. Every Job has a fixed active
deadline and `before-hook-creation` cleanup keyed by Helm revision. Managed
identity/RBAC/recorder resources and immutable anchor/enforcement/revision
receipt evidence persist for the next fix-forward attempt and removal soak.
Every phase independently rereads the Secret/anchor/enforcement-receipt
metadata, validates the pinned direct source connection, and requires the
immediately preceding PostgreSQL journal receipt or the pre-schema receipt
below.
The migration wrapper verifies the identity and challenge/fence generation in
the same database transaction/connection target used to begin DDL; a mismatch
fails before any migration statement. Secret mutation between `-20`, `-10`,
and `-5` therefore cannot redirect a later phase.

The nonconfigurable `anchor-evidence-finalize` identity is distinct from the
transition runner. Its Job runs in the isolated guard namespace only under the
both-empty-and-mutable receipt predicate above, lists Pods only there, code-filters its
exact Pod -> Job owner chain, and uses the stable Job GET to verify its
deterministic revision-bound Job. Its release-namespace Role reads the exact
release topology, two application Secrets, and exact immutable
`DatabaseAuthorityBundle`; guard-namespace Roles grant
exact-name GET plus mount of the admin Secret and exact anchor/enforcement
receipt access;
cluster-scoped RBAC grants exact-name GET of only the `kube-system`, release,
and guard Namespace objects. It alone may update the exact enforcement-receipt
name. It has no provider credential, request worker, service endpoint,
controller authority, or workload/create/delete permission. A successful chain
makes the receipt immutable, after which chart rendering must omit this Job.
The finalizer contract and managed identity remain permanently for new
anchor-proved empty installs, each of which begins with a bootstrap scaffold;
there is no nonempty-validator Job, role, or code path.

The identity-verified `-10` migration Job likewise runs in the guard namespace
with a distinct fixed ServiceAccount. It may list Pods only in that dedicated
namespace to resolve its generated Pod name/owner reference and use the stable
Job GET to verify its deterministic release-instance/revision-bound
`batch/jobs` object, then read the mounted
immutable target payload plus authority anchor/enforcement receipt and admin
Secret metadata there by exact-name GET, and mounts that Secret. It GETs the deterministic runner-created immutable
revision receipt through the Kubernetes API after `-20` only on the pre-
Serve041 path; its ConfigMap read has no list/watch/write verb. On Serve041+ it
validates only the PostgreSQL journal claim. Release-namespace Roles grant
exact-name GET of the two application Secret metadata objects and immutable
`DatabaseAuthorityBundle`; cluster-scoped RBAC grants
exact-name GET of only the `kube-system`, release, and guard Namespace objects.
It mounts only that administrator database Secret, has no ConfigMap
write, provider credential, workload verb, or authority membership, and can
claim the DDL generation only after proving that Pod -> Job owner chain and
exact target image digest.

The dedicated runner identity is independent of the API/controller/executor
service account and is never configurable or part of authority inventory. Its
Jobs run in the guard namespace. There it may list Pods for owner resolution
and use the stable Job GET to verify the three deterministic revision-named runner
Jobs by name/UID/owner/image. Its ConfigMap Role grants
`get/create` but no list/watch/update/patch/delete: Kubernetes cannot restrict a
create verb by future revision name, so the stable database guard admits only
the deterministic release-instance/revision name and exact registered payload.
An extra object can cause denial of service but cannot overwrite the
pre-existing anchor/enforcement receipt or become authority. Revision-evidence
creation is its only permitted Kubernetes write and is exercised only before
Serve041 exists; Serve041+ phases issue no ConfigMap create. A release-namespace
Role and RoleBinding for this exact guard-namespace ServiceAccount grant
`get/list/watch` on
Deployments, ReplicaSets, and Pods plus exact-name GET of both source and target
application database Secrets for metadata/identity comparison, exact-name GET
of `DatabaseAuthorityBundle`, and exact-name GET of release-namespace scaffold
ServiceAccounts, Roles, and RoleBindings. A separate guard-namespace Role and
RoleBinding grant exact-name GET of scaffold ServiceAccounts, Roles, and
RoleBindings there; namespaced RBAC never purports to read objects in another
namespace. Cluster-scoped RBAC grants exact-name GET of only the `kube-system`,
release, and guard Namespace objects plus every scaffold-managed ClusterRole and
ClusterRoleBinding, with no list/watch. The hookless scaffold creates no
"scaffold receipt." Instead, the immutable target payload carries an artifact-
pinned canonical ABI manifest and hash for the complete fixed ServiceAccount/
Role/RoleBinding/ClusterRole/ClusterRoleBinding set, excluding only API-assigned
UID/resourceVersion. `-30` stable-double-reads every exact name, compares rules,
subjects, and canonical specs to that manifest, and atomically records
`(namespace, kind, name, UID, resourceVersion, spec_hash)` in the database
target claim. Retry of that revision requires the recorded tuples; a newer
revision may register newly stable tuples only after its reviewed hookless
scaffold predecessor installed the RBAC change. It reads the exact target application PostgreSQL Secret
through the Kubernetes API into process memory and never mounts a
cross-namespace Secret; it cannot read the guard-admin Secret and receives no
cloud/provider credentials, request-worker configuration, service endpoint,
controller leadership, mutation permit, or workload-mutation verb. The fixed-
name runner prerequisites remain chart-owned
across upgrades. The permanent anchor and enforcement receipt are
operator-owned; immutable revision evidence is runner-created retained
evidence, deliberately not Helm release-managed. All three are read-only after
their bounded writer phase. The
rendered-target payload is generated directly by the target chart and contains
the normalized complete authority Deployment envelope plus its SHA-256. It is
not an ordinary or hook ConfigMap that would need to exist before pre-upgrade
hooks. Instead, the chart embeds the same RFC-8785 bytes and hash in a fixed,
non-overridable annotation on every hook Job's Pod template and exposes them
through a read-only downward-API volume. Rendering rejects a canonical payload
larger than 128 KiB. Each phase proves its Pod -> Job owner chain, rereads the
Job template, and requires the mounted bytes/hash and every preceding database
receipt to agree. The runner never guesses an unapplied target by reading
current Deployments. The payload includes the rendered target Secret reference. The runner computes its
target identity from that exact API-read Secret, joins it to the external source
anchor, immutable enforcement receipt, and source-A challenge proof, and
rejects every live-source/rendered-target Secret, anchor, credential epoch,
endpoint, database, schema, or cohort difference before `-20` may mutate the
authority guard, reconciliation gate, transition lease, or revision receipt.

On upgrade, the prerequisite Role's Secret `resourceNames` are limited to the
source reference captured from the stable-double-read predecessor Deployment
in the signed attempt-envelope read-set and the rendered target reference; on install
only the target exists. Immediately before render, the supervisor repeats the
exact predecessor read, and the runner repeats that live owner-chain read
before mutation. A source/target reference mismatch is rejected without
opening either database for a transition.

Before Serve041/API009 exists, `fence` first commits or resumes one immutable
pre-schema claim in the stable `database_authority_guard` revision-evidence
registry. The claim stores the deterministic release-instance/revision
ConfigMap name, target hash, runner Job UID/image digest, authority-anchor and
enforcement-receipt hashes/origin, source-proof digest, database-generated
random nonce and completion timestamp, and RFC-8785 payload bytes/hash. The
runner then creates that complete ConfigMap once with `immutable: true`; it
never patches an empty object. A lost create response is recovered by exact-
name GET. The runner accepts only the registered canonical bytes, computes the
same UID-bearing `object_content_sha256` domain used by the enforcement receipt,
and monotonically acknowledges the object UID, resourceVersion, and content
hash in the stable registry. A pre-existing unregistered, mutable, or byte-
different object and any concurrent or tampered content fail closed.

The pre-Serve041 `-10` Job GETs that exact receipt through the Kubernetes API,
validates the registry acknowledgement before DDL, and, in the transaction that
creates Serve041, imports its hash/nonce as the first transition-journal record.
A crash before commit safely reuses the same object; a crash after commit
observes the identical imported record. The corresponding `-5` phase requires
the imported record to match that immutable object.

Once Serve041 exists, its PostgreSQL transition journal is the sole inter-hook
handoff for every active, maintenance, cleanup, and post-cleanup revision.
`-30` validates the deterministic database target identity, `-20` commits or
resumes the immutable target/journal claim, `-10` validates that claim in the
same transaction/connection that begins DDL, and `-5` validates the resulting
journal record. Those revisions neither create nor require a Kubernetes
revision-receipt ConfigMap. The permanent runner retains ConfigMap `get/create`
only because an exactly empty installation still has no Serve041 journal: its
first database-bearing revision uses the one pre-schema Kubernetes handoff
above, imports it while installing the transactional current-head baseline,
and every later revision uses PostgreSQL only.

No transition, cleanup, or post-cleanup chart deletes or rewrites an existing
revision receipt. Every pre-schema immutable object remains audit/retry evidence
until explicit database-authority decommission. The final cleanup chart retains
the empty-bootstrap pre-schema ABI and its create-only RBAC, but has no
Kubernetes revision-evidence write on a nonempty Serve041+ database. The
authority anchor and enforcement receipt remain permanent. These retained
ConfigMaps are evidence handoffs, never a second runtime authority.

Each per-revision ConfigMap is runner-created evidence in the isolated guard
namespace, labeled with the exact `release_instance_id`/revision, and is neither
a Helm hook nor release-managed and has no ownerReference to a Job, release
object, or other garbage-collected resource. The stable runner Role has
create-only write authority; no principal can update, patch, or delete
revision-evidence ConfigMaps. Only the bounded phase-0 writers may update the
separately pre-created enforcement receipt by exact resourceName. Hook Job
deletion and supervised release decommission leave revision evidence untouched. After the release and its database have been deliberately
decommissioned, an operator may delete the entire operator-owned guard namespace
out of band; object-by-object deletion is not an application recovery path. This
avoids a broad in-cluster cleanup principal that could erase the anchor or
enforcement receipt. A normal upgrade, fix-forward attempt, cleanup, or release
supervised decommission never invokes an evidence-deletion path.

The `fence` entrypoint is a deliberately small compatibility ABI retained in
every supported image. It can read the installed migration version and the
stable protocol singleton without importing target-schema models. It completes
the source/target database-identity proof above before any gate, lease, or
migration mutation. Before
Serve041/API009 exists, it only proves an empty/fresh or legacy database and
requires the authority guard and permanent enforcement-receipt chain to be
`ENFORCED`/complete for both a nonempty upgrade and the exact empty bootstrap,
and records the immutable
pre-schema receipt above, not a nonexistent transition-journal row; the migration and `attest-target` phase then
initialize `SEQUENCED_BOOTSTRAP` or `LEGACY_ACTIVE`. Once the transition
schema exists, `fence` atomically takes the shared lease and: leaves bootstrap
effect-closed; resumes any `PREPARING_SEQUENCED` cursor; changes active to
maintenance; or advances existing maintenance. At any preparation cursor,
target-runner takeover appends the prospective target revision and continues
the existing one-way activation journal; it never chooses legacy.
Thus an initial rollout orders anchor-evidence -> fence-proof -> migration -> target attestation,
while every later active upgrade orders effect closure -> migration -> target
attestation. A final cleanup image retains the ABI but rejects legacy or
preparing states with the remediation contract below instead of pretending it
still contains activation code.

Maintenance entry prevents every new effect in PostgreSQL immediately. The
runner waits a fixed 30-second database-time acknowledgement period, with no
row lock held. A normal ACK includes the process-wide worker/leadership drain,
closure of old DB sessions, and new session-generation/source-A challenge
receipt. A still-heartbeating boot whose ACK loop is broken cannot block
fix-forward forever: because the gate was closed on pinned source A, after the
deadline the runner proves on A that every
already-started effect for that boot has an immutable completion or debt
identity, generation-fences any leadership, and in canonical order inserts the
Pod UID's permanent revocation and marks the boot excluded. This is permitted
even while the Pod object remains Running. Triggers then reject its heartbeat,
readiness, leadership, claim, and effect transactions; the runner has no
Kubernetes delete permission. An unrecorded provider call is impossible
because effect identity is committed before provider I/O. If exact carried
effect/debt proof is unavailable, or source A cannot be reached, the hook
remains blocked. The same source-A proof is required for absent, partitioned,
and `Terminating` Pods; their apparent absence in any candidate database is
ignored.

The `attest-target` phase validates the exact new schema, source topology, and
rendered target. In `LEGACY_ACTIVE` it journals only the prospective capable
target. In any preparation cursor it appends a successor and leaves the gate
preparing for the target cohort. In maintenance it appends or advances that
same run. For a fresh
`SEQUENCED_BOOTSTRAP` install, where no source Deployments exist yet, it
repeats the empty-database proof and journals only the target envelope; the
post-install coordinator attests the created cohort before bootstrap resume.
Only after
every started effect has immutable completion/debt identity and controller
leadership is released or generation-fenced does it permit Deployment
mutation. A broken fence Job changes neither schema nor Deployment; migration
or attestation failure after active-state fencing leaves the gate in
maintenance. A newer target image resumes the same journal. Neither phase can
authorize legacy return, an old-image restore, or provider work.

The Kubernetes adaptor exposes one `KubernetesCredentialSource` abstraction
for every cloud/catalog/provision/LB/subprocess caller. Its implementations are
explicit projected files and explicit kubeconfig/exec credentials; there is no
ambient detection. Non-authority composition roots pass their chart/config-
owned projected paths through the same bounded reader, so direct fixed-path
reads are removed repository-wide rather than retained as a second happy path.

The chart sets `automountServiceAccountToken: false` on every authority Pod and
uses two separately scoped projected identities. API MAIN always receives one short-
lived, Pod-bound owner-chain projection at a fixed owner-chain-only path; it
grants namespace-scoped `get/list/watch` on only the release's Deployments,
ReplicaSets, and Pods. `_read_token_bound_pod_identity()` and the activation
reader consume the three `SKYPILOT_OWNER_CHAIN_*_PATH` files rather than the
Kubernetes adaptor's provider path. The owner and provider projections use
the pinned Kubernetes API audience, a bounded one-hour maximum token TTL,
`serviceAccountToken` plus `kube-root-ca.crt` ConfigMap and downward-API
namespace sources, read-only volume mode, and no `subPath`. Confined readers
accept kubelet's AtomicWriter symlink layout only when the resolved open file
remains under that projection root and `fstat` proves a bounded regular file.
Controller and executor receive no owner-chain projection.

`kubernetesCredentials.projectedTokenAudience` is a mandatory, nondefaulted
chart/platform value for every split-role release because API always needs the
owner projection. The platform module derives the accepted audience from the
target API-server authentication configuration and proves it with a bounded
TokenReview/authenticated self-read before creating `ReleaseSupervisorRoot`;
each east/PHX cluster records its own value rather than assuming they match.
The root, signed release intent, attempt envelope, Pod projection, and runtime
capability hash all bind that exact value. Missing input, an audience accepted
on only the other cluster, or a projected JWT whose `aud` differs blocks render/
startup; remote kubeconfig credentials retain their own audience contract.

Local Kubernetes provider access is separate and conditional. One canonical
`ProjectedCredentialFiles` implementation owns `available()`, `namespace()`,
`new_client()`, subprocess capture, and token/CA rotation. For the authority
provider identity it accepts only
`SKYPILOT_PROVIDER_KUBE_TOKEN_PATH`,
`SKYPILOT_PROVIDER_KUBE_CA_PATH`, and
`SKYPILOT_PROVIDER_KUBE_NAMESPACE_PATH` for an authority-role in-cluster
provider identity.
It bounded-reads regular files whenever it constructs or refreshes a client,
never copies the token into a durable kubeconfig, and observes kubelet rotation
without caching token bytes past their lifetime. Every cloud, catalog,
provisioner, context-availability/namespace helper, external-LB controller/
cleanup path, and external-LB RBAC preflight uses the common source; fixed
`/var/run/secrets/kubernetes.io/serviceaccount/*`, ambient
`load_incluster_config()`, and fallback-to-default-namespace reads are forbidden
outside the separate owner-chain reader. When
`kubernetesCredentials.useApiServerCluster=true`, the chart projects a short-
lived, Pod-bound provider token, CA, and namespace at those paths into
controller and executor MAIN. When `serve.externalLoadBalancer.enabled=true`,
the projection exists only on controller MAIN; the existing API-startup LB
RBAC preflight moves to controller startup so API remains owner-chain-only.
This checked-in role-to-capability table is closed:

| Capability | API | Controller | Executor |
| --- | --- | --- | --- |
| Authority owner-chain read | required | none | none |
| Local Kubernetes provider (`useApiServerCluster`) | none | provision/reconcile | request-side provider execution |
| Local external-LB lifecycle | none | preflight/create/reconcile/delete | none |

When both provider features are enabled, controller shares one provider
projection and client interface; it never receives duplicate tokens. Both
conditions false means no provider projection unless the cleanup-only state
below is active. A remote kubeconfig remains an independent credential and
never makes the local projection implicit.

Disabling the external LB is a two-revision forward transition, not immediate
credential removal. Revision one sets durable `EXTERNAL_LB_CLEANUP`, stops new
non-pool service admission, and retains the controller projection plus a
cleanup-only subset of LB verbs. The controller deletes every exactly owned LB
Deployment, Service, PDB, and Pod and commits a UID-complete absence receipt
only after all non-pool services are down. Crash/restart resumes those exact
objects. Revision two requires that receipt and removes the projection/RBAC,
then records `EXTERNAL_LB_DISABLED`. A direct true-to-false render, absence by
name without UID/delete evidence, or a both-false render while cleanup is open
fails. Re-enabling uses a newer forward revision; it never cancels or rewinds
an open cleanup.

API, controller, and executor use distinct role ServiceAccounts. Each gets
only the union of the static table's provider verbs for the enabled modes; the API
ServiceAccount additionally gets the owner-chain reads above. Rendering fails
if a role advertises local-provider or local-external-LB capability outside the
table or without its
projection, paths, audience, and exact Role/ClusterRole binding, or if it
renders a projection for a role with neither capability. Provider identities
cannot read or mutate Helm history, supervisor or guard namespaces, release-
writer identities/tokens, authority receipts, RoleBindings, or database-
transition objects; they cannot mint or impersonate any ServiceAccount. The
release admission policy independently denies Helm-owned-object writes from
every Pod identity. No SkyPilot Pod ever receives the external supervisor's
credential.

Authority Pods cannot mount the guard-namespace anchor. Instead, the hookless
scaffold chart creates one ordinary,
immutable, release-namespace `DatabaseAuthorityBundle`. Its canonical data is
the complete anchor payload supplied by the signed external read-set plus
source namespace/name/UID/resourceVersion,
immutable/retention proof, content hash, and `release_instance_id`. The bundle
is non-authoritative: the phase-0 prerequisite compares it byte-for-byte with a
fresh guard-namespace anchor GET, and MAIN compares its payload/hash with the
stable guard/singleton on startup and every database checkout. A bundle read-
set or live revalidation mismatch blocks; signed render input never establishes
runtime authority.

The bundle has one fixed release-instance-derived name. Because a guard-
namespace Pod cannot mount a release-namespace volume, the chart creates one
release-namespace Role whose only ConfigMap rule is core `configmaps`, verb
`get`, and that exact `resourceName`. Individual RoleBindings name only the
guard-namespace ServiceAccounts for `guard-prerequisite-check`,
`phase0-recorder-ready`, `anchor-enforcement-runner`,
`anchor-evidence-finalize`, `transition-runner`, and `guarded-migration`; there
is no wildcard subject, cluster-scoped ConfigMap grant, or list/watch/write
verb. The runner's guard-namespace `get/create` revision-evidence rule is a
separate Role. Every named phase performs a fresh exact-name GET immediately
before its phase success or first mutation and requires `immutable: true`, no
deletion timestamp, exact canonical data/hash, and matching inner anchor
namespace/name/UID/resourceVersion/content hash and `release_instance_id`.
Where the stable guard exists it also compares the inner identity to
PostgreSQL; prerequisite and finalizer additionally compare it to a fresh
direct guard-anchor GET. The outer bundle UID/resourceVersion is retry evidence,
not authority: byte-identical recreation is accepted only while the inner bytes
still match the permanent anchor and PostgreSQL. A missing, mutable, deleting,
or mismatched bundle blocks before database or Kubernetes mutation.

All three authority containers mount the same-release-namespace sequenced
database Secret and `DatabaseAuthorityBundle` read-only; their only other
credentials are the explicit owner/provider projections admitted above. They
have no guard-namespace RBAC. The API ServiceAccount's existing broad release-namespace
ConfigMap patch path can at most delete or metadata-corrupt this immutable
mirror and cause fail-closed unavailability: it cannot change immutable data,
and a recreated/different bundle is accepted only when its inner source-anchor
identity and bytes match PostgreSQL. No control-plane role gains patch/delete
authority in the guard namespace. A missing/mutable/mismatched bundle prevents
MAIN from starting. Activation performs the real owner-token-bound
Pod -> ReplicaSet -> Deployment owner-chain read. Missing token/audience/RBAC,
an alias, caller-supplied identity, or an unobserved Deployment generation
fails closed.

MAIN owns the process-local readiness bit. API MAIN starts the lightweight
role-health server before Uvicorn; the chart points the Kubernetes readiness
probe to that parent port. The Uvicorn diagnostic endpoint proxies the
loopback parent instead of interpreting a database row as process-local
readiness. A boot-fenced local IPC reports listener/worker startup and loss;
children can report their health but cannot set desired traffic or gate state.
Controller and executor probes already terminate in MAIN-owned health servers.
API keeps its real `minReadySeconds: 10`; controller/executor retain the
Kubernetes zero default.

Local Kubernetes readiness and effect eligibility are separate predicates.
During maintenance, a fully initialized boot that has ACKed the maintenance
generation may report Ready so the real Deployment can roll, while every claim,
leadership, background-effect, and provider-start predicate remains false.
Returning to active never relies on that Ready bit alone: only the final
inventory/gate commit enables effect eligibility, and controller execution
still requires the subsequent dedicated leadership generation.

API, controller, and executor share one process-owner-only local retirement
socket and mode-0600 boot token under `/var/run/skypilot`. The common preStop
hook asks MAIN to clear local readiness and record the exact boot's maintenance
or shutdown receipt. All three termination grace values are at least 120
seconds and chart rendering fails below that. Shutdown ordering is normative:
clear readiness; stop/join the maintenance coordinator if local; stop claim
loops and effect workers; release controller leadership; stop Uvicorn and
background children; write the final quiescence/draining receipt; and call
`ServerInstanceLease.stop()` last. Failure leaves the boot stale/debt-fenced,
never eligible.

##### Initial one-way activation

The new image and additive migrations deploy while `LEGACY_ACTIVE`, with the
sequenced planner/effect path inert. Promotion mechanically discovers the exact
three Deployments and every recent database process lease. It requires stable
double reads, observed generations, desired/current/updated/ready/available
counts equal and at least two, zero unavailable Pods, reviewed rolling
strategies, parent readiness/retirement wiring, at least 120 seconds grace,
mounted API token/RBAC, exact built-in PostgreSQL request storage and queue, and
the complete capability contract. Every Pod must map one-to-one to a fresh
boot lease. Old, unready, draining, unowned, cross-release, or ambiguous
processes block. Compatible digests may coexist for safety, but binary
acceptance and the validation run require the intended single immutable digest.

`activate` writes `PREPARING_SEQUENCED` and makes every capable process stop
legacy claim/admission/provider starts. This includes every effect-bearing API
request worker and controller background handler, not only reserved-fill
Serve work. Existing effect-bearing work is quiesced and checkpointed. The
action then drains the last possible pre-gate request writer. With new claims
database-disabled and all known effect processes acknowledged, one bounded
transaction sets a 30-second PostgreSQL `lock_timeout`, takes
`SHARE ROW EXCLUSIVE` on `api_requests`, then locks the protocol singleton and
revalidates the exact gate, inventory, acknowledgements, and zero ambiguous
effects. This table lock conflicts with every row-writing transaction, so any
legacy row-first DML commits wholly before the barrier or finishes before the
lock is granted. The transaction persists an immutable old-writer-flush
receipt and commits; API009's singleton-first triggers reject every later
unsequenced writer. This is the sole table-before-singleton exception and it
holds no manager, broker, provider, or request-row lock while waiting. Timeout
or deadlock aborts the whole barrier transaction and leaves the one-way
preparation cursor safely retryable; it does not reopen legacy effects.

Activation then performs restart-safe canonical
batches over every nonterminal ordinary and fill zero-cost row, assigning
commit-ordered admission identity, binding exact request/object state, and
projecting every active/prospective/debt item into the shared scheduler. A
legacy PENDING request is proved no-effect and terminalized; running work must
publish an exact provider identity/state or remain an explicit blocker. No
state is guessed from age.

After projection, activation invalidates every legacy observation/allocation
map. The provider-free observer may publish during preparation, but allocation,
intent acceptance, request claim, and provider effects remain closed.
Activation derives the nonempty exact union of configured and claimed reserved
pool descriptors and requires a fresh successful authoritative observation for
each. An explicit zero-capacity result is successful coverage; blackout,
provider error, missing physical identity, or empty/disagreeing sets block.
The final transaction repeats complete process/topology/conservation checks,
publishes the global mutation limit as the minimum positive complete-fleet
limit, stores the protected topology/inventory hashes, creates the immutable
validation run, and commits `SEQUENCED_ACTIVE`. Only that commit wakes the
sequenced observer, planner, admission, and provider dispatcher.

From the `PREPARING_SEQUENCED` commit onward, a missed deadline or caller loss
keeps effects closed and can only resume the same forward journal. Target-runner
takeover at any cursor appends its prospective target atomically and continues
activation. A failed process remains quiesced; an unresolved provider identity
remains debt; a schema or code defect is repaired in a newer capable image. No
rollback image or reverse-data procedure exists.

The activation capability test starts the previous digest against a restored
database copy with both a new Pod UID and reused instance row and proves it
refuses every post-mutation gate state. This is a compatibility fence test, not
a supported production rollback. Additive schema is retained permanently.

##### One maintenance kernel for every fix-forward iteration

Every planned image, configuration, desired replica count, selector, strategy,
service-account, role-wiring, or other authority-topology change after
activation uses the same sequenced maintenance kernel. The chart's pre-upgrade
guard rejects any authority Deployment mutation while `SEQUENCED_ACTIVE`.
Operators normally run zero-argument `begin-maintenance` before Helm so they
can observe quiescence. It commits
`SEQUENCED_MAINTENANCE`, closes new plan/accept/bind/claim/provider effects for
all effect-bearing API request classes and controller loops, and waits for
each process to acknowledge or reach the fixed runner exclusion proof.
Already-started immutable completion,
adoption, cancellation, and debt-resolution records may advance, but cannot
bind fresh work, retry a create, change identity, or mint capacity.
The mandatory target-image runner invokes that exact idempotent transition when
the current API image cannot do so. Pre-entry and hook-entry therefore converge
on one journal generation and proof; they are two transports for one state
transition, not two operational behaviors.

While maintenance is active, the target-image runner verifies the mounted
chart-generated target payload/hash and appends that target revision to the
same PostgreSQL maintenance journal before Helm mutates a Deployment. A later
newer fix-forward Helm revision appends a successor target; it never edits the
prior evidence in place. The coordinator accepts only a live Kubernetes cohort
matching the journal's latest target hash. Thus the hook is an intent fence as
well as a state check, while direct `--no-hooks` drift remains detectable and
cannot be adopted silently.

The controller leader stops claims, joins effect children/background loops,
writes exact quiescence, releases its generation and both advisory locks, and
does not elect a replacement during maintenance. API endpoints may remain
available for health, status, read-only operations, and durable no-effect
enqueue, but no controller or executor starts a provider effect. This brief
fleet-wide actuation pause is the deliberate simplification enabled by the
service's current light use: Kubernetes can replace any number of authority
Pods without a second pairwise live-effect handoff state machine.

After the gate is in maintenance, Helm may apply a valid split-role target.
Normal `RollingUpdate(maxSurge=1,maxUnavailable=0)` preserves API availability,
but correctness does not depend on victim order, one-successor lookahead, or
terminating-Pod count. New boots register `STARTING`, advertise capability, and
may become locally traffic-ready while remaining effect-ineligible. Old boots
stay effect-quiesced. The maintenance journal records each observed target
envelope revision. If an image never becomes capable/Ready or the rollout
stalls, the gate remains maintenance and the operator builds and applies a
newer reviewed fix-forward digest. The next stable target revision supersedes
the broken target in the same journal; there is no previous-digest restore
exception and no legacy path.

`resume` is zero-argument and discovers evidence rather than accepting caller
inventory. Stable double reads must show the rendered split-role envelope, at
least two desired/current/updated/Ready/Available Pods per role, no unavailable
or unowned nonterminating Pod, every desired boot fresh/capable, and exact
request backend/queue/limit attestations. Every source-inventory boot excluded
from the desired cohort must be locally stopped or session-lost with
effect/debt recovery complete. The final transaction permanently revokes all
excluded Pod UIDs except a same-Pod old boot that completed the exact
nonce-scoped `PROCESS_RETIRED` handoff. It accepts lingering Kubernetes
`Terminating` objects only when their processes are fenced on pinned source
A, rejects any still-live excluded coordinator lease,
recomputes the full inventory and mutation limit, invalidates pre-maintenance
observations/allocation maps, proves zero active/prospective/debt tickets and
legacy writers, and advances the inventory/gate generation. Maintenance resume
creates a new validation run for the new implementation identity; fresh
bootstrap creates a non-qualifying bootstrap-health record with an explicitly
empty expected-pool set. The same transaction commits `SEQUENCED_ACTIVE`.
Controller election and sequenced reconciliation start only after that commit.

A coordinator whose API Pod is excluded cannot revoke itself. In the normal
path it stops/joins, relinquishes its lease, and another API boot takes a later
lease generation before finalization. If its ACK path is broken, the
non-authority target runner first takes the next shared lease generation and
then performs the canonical permanent revocation; split-role HA guarantees a
peer for normal API coordination but is not required to recover the broken
owner. Lease acquire, renew, and every journal CAS check permanent revocation.
Force-deleted old
processes that later reconnect or restart under the old Pod UID are rejected
forever.

Unplanned Pod/container/node loss and same-Pod process restart use the same
maintenance kernel, not a smaller handoff path. Detection of an inventory boot
loss, fresh nonce, protected-topology mismatch, or stale topology attestation
closes new effects and creates a `MEMBERSHIP_RECOVERY` maintenance run. The
Deployment may create several replacements concurrently; all remain
effect-ineligible until the exact complete cohort is attested. For an unchanged
rendered envelope the coordinator may automatically resume after the same
proofs; a same-Pod fresh nonce additionally needs the
`PROCESS_RETIRED` session/lock/effect proof above and never revokes its own
still-desired Pod UID. If capability/topology cannot converge, it remains safely fenced for an
operator's newer fix-forward deployment. Thus loss during rollout, several
missing members, queued successors, and lingering terminating objects all
collapse into one full-fleet quiescence/re-attestation path.

Topology attestations are database-time stamped every five seconds. Every new
claim/provider-effect start requires the current gate/inventory generation and
an attestation no older than 15 seconds. An unsupported direct
`kubectl scale/patch` or `helm --no-hooks` therefore cannot be adopted: new
effects close, extra Pods stay `STARTING`, deleted boots become recovery debt,
and the coordinator enters maintenance. The operator reasserts the intended
envelope or supersedes it using a newer reviewed fix-forward Helm revision and
resumes through the same journal; no previous revision is restored.

The transition module is the only gate implementation for both operators and
the chart runner:

```bash
python -m sky.serve.reserved_fill_reconciliation_transition status --json
python -m sky.serve.reserved_fill_reconciliation_transition enforce-anchor
python -m sky.serve.reserved_fill_reconciliation_transition activate
python -m sky.serve.reserved_fill_reconciliation_transition begin-maintenance
python -m sky.serve.reserved_fill_reconciliation_transition resume
python -m sky.serve.reserved_fill_reconciliation_transition verify
python -m sky.serve.reserved_fill_reconciliation_transition prepare-removal
```

The mutating commands take no service subset, inventory, digest, timeout, or
proof arguments. `enforce-anchor` rejects every caller except the exact
owner-verified phase-0 Job/ServiceAccount, uses the pre-created ConfigMap CAS,
and operates on the full boot inventory; every later command uses PostgreSQL ownership so caller
loss is harmless and concurrent API-pod or target-runner callers converge on
one generation. The chart invokes
the same module's noninteractive target-envelope operation; it cannot select a
different transition or weaken a proof. `verify` evaluates the immutable
validation run created by activation/resume; it cannot move the gate.

##### Permanent sequenced bootstrap

`SEQUENCED_BOOTSTRAP` is the one fresh-install path before and after legacy
cleanup. It is available only when the migration transaction proves the
central database contains no service, request, replica, claim, observation,
leadership, execution, workload/effect receipt, or provider-effect state;
only the already-validated permanent guard/anchor evidence is present. All effect predicates
are false. Split-role Pods may register and become locally Ready exactly as in
maintenance, but cannot claim, lead, plan, or call a provider.

The coordinator or zero-argument `resume` discovers and attests the complete
split-role cohort, dedicated runner identity, request backend/queue, protected
topology, schemas, and mutation limit. The final transaction repeats the empty
state proof, publishes the inventory and limit generation, and commits active.
An empty expected-pool set is permitted only for this fresh bootstrap and does
not count as transition-feature rollout evidence; adding the first service
creates its normal observation set. Bootstrap never imports legacy rows and
cannot enter `LEGACY_ACTIVE` or `PREPARING_SEQUENCED`.

Serve041 also creates the one-row PostgreSQL
`serve_reconciliation_removal_gate`; its writer is the same transition
coordinator on anchored A. Removal uses one fix-forward state machine:

`OPEN -> PLATFORM_FROZEN -> SEALED -> CLEANUP_APPLYING -> CLEANUP_COMPLETE`.

After the qualifying soak, `prepare-removal` first asks the external supervisor
ledger to conditionally freeze adoption, database transfer/credential change,
release creation, and topology mutation for the exact installation UUID,
`release_instance_id`, cluster/namespace UIDs, and platform inventory
generation. That reversible `PLATFORM_FROZEN` receipt is strongly consistent
external authority; a new endpoint, clone, release, or writer cannot be
admitted under the frozen identities. The zero-argument transition command
then enters maintenance and, in one A transaction under the ordinary global
row/lease order, revalidates the durable `PASSED` run, complete database-
process/effect/debt inventory, source/anchor/secret identities, supervisor
freeze receipt, and exact release/cohort hashes. It commits immutable `SEALED`
content/hash and closes new process/effect registration for the sealed
generation.

The cleanup release intent and attempt envelope bind both receipt hashes and
the frozen platform generation. Immediately before its first mutation, the
supervisor strongly-consistently revalidates the external freeze, while `-30`
locks the A row, repeats the complete closed inventory, and atomically advances
`SEALED -> CLEANUP_APPLYING`. A changed endpoint/inventory/credential, a new
process, an open epoch rebase, or an unregistered clone cannot pass either
fence. `-10` applies cleanup DDL only under that generation; `-5` commits
`CLEANUP_COMPLETE`, after which the supervisor records the matching external
completion and may unfreeze ordinary steady-state upgrades. The retained row/
external receipt are the audit evidence. A crash before either CAS resumes the
same state; a conflict requires a newer capable fix-forward intent, never a
stale receipt or old interpreter.

The cleanup migration accepts active, maintenance, or an exactly empty
database. The removal PR is scoped to the one anchored installation and cannot
merge or deploy until the immutable `SEALED` removal receipt proves that database
is `SEQUENCED_ACTIVE` after its complete 24-hour run, no registered endpoint/
clone is `LEGACY_ACTIVE` or `PREPARING_SEQUENCED`, and every release inventory
matches the receipt. It nevertheless rejects either legacy/preparing label
before cleanup DDL as unsupported precondition drift and instructs the operator
to deploy a *newer capable fix-forward revision*; it never names or restores the
old transition image. A separately built migration artifact may activate a
`LEGACY_ACTIVE` installation that never crossed the irreversible preparing
edge, but that artifact is not a cleanup/runtime rollback. Any
`PREPARING_SEQUENCED` database requires a newer image containing the same
forward-resume interpreter. If PostgreSQL enum storage makes physical label
removal unsafe, the two labels remain inert tombstones solely for rejection; no
final-image handler accepts them after `CLEANUP_COMPLETE`.

##### Validation and removal boundary

Each activation or maintenance resume starts with five consecutive healthy
60-second full-fleet intervals inside a fixed ten-minute deadline. Every
expected pool must publish a strictly newer successful observation per interval
and be at most 75 seconds old; zero capacity is valid coverage. A stable
positive spendable allocation, when one occurs, must reach durable intent
acceptance within 15 seconds. When none occurs the event-dependent latency is
`N/A`, never fabricated by a GPU canary. All unconditional scheduler,
provider-receipt, no-paid-spill, `max_replicas`, status-coherence, and BCL
priority invariants still apply.

The initial removal gate then requires a fixed 24-hour full-fleet
`SEQUENCED_ACTIVE` soak on one immutable implementation/image/capability/limit
identity. Any maintenance entry, inventory/digest change, sample gap, stale
observation, debt, duplicate effect, paid spill, or safety violation fails or
resets that run. A fix-forward image therefore earns its own binary acceptance
and, until legacy cleanup merges, a fresh removal soak. The deterministic
kube-scheduler BCL test supplies positive reclaim evidence; production creates
no BCL or GPU canary workload.

After one run passes, the stacked cleanup PR removes `LEGACY_ACTIVE`,
`PREPARING_SEQUENCED`, their legacy projections, the one-time writer-drain
barrier, the lock-coupled observer/planner/debit/manager-event paths, and the
now-unreachable activation code. It does **not** remove the database
triggers that reject unsequenced writers, the admission-order seam, the stable
pre-migration fence ABI, enforced database-authority guard/triggers/forced row
policies, guarded-DDL event triggers/manifest/generation ABI, complete pinned
role/owner/default-privilege closure, sequenced role and phase-0 retirement
tombstones, immutable database authority anchor and permanent enforcement
receipt/final-resourceVersion acknowledgement, source-session drain checks,
permanent physical-protocol floor, pre-schema receipt ABI, exact empty-bootstrap
anchor-evidence finalizer, or permanent Pod-UID revocations. The final steady
state retains `SEQUENCED_BOOTSTRAP`, `SEQUENCED_ACTIVE`,
`SEQUENCED_MAINTENANCE`, the single transition coordinator, typed
intent/provider recovery, and one observer/reconciler. This is the explicit
path that prevents two happy paths from surviving rollout while keeping fresh
install and fix-forward recovery possible.

##### Alternatives rejected

- Increasing the 180-second freshness horizon or changing
  `provision_timeout` hides the lock convoy and conflates capacity wait with
  initialization; it does not recover an unaccepted fill-plan tail.
- A second fill-only planner, shadow actuator, service flag, or canary creates
  competing debit/`max_replicas` ownership and cannot reproduce the large-fleet
  convoy.
- Pairwise live-effect handoff plus a reverse projection can preserve provider
  mutation availability during every control-plane Pod replacement, but it
  requires victim selection, controller-leader transfer, multi-loss overlap,
  terminating-Pod tombstones, and two rollback-compatible data paths. Given the
  service's current light use, one short global actuation pause is the simpler
  and safer root solution.
- Application/schema downgrade after forward projection would expose typed
  intent/provider states to code that cannot interpret them. The supported
  response to a bad release is a newer capable image in the same maintenance
  journal.
- Treating a changed PostgreSQL URI or restored clone as part of an image
  rollout can fence one database while the old cohort writes another.
  Database/credential movement is rejected here and needs its own two-endpoint
  quiescence and transfer design.
- Sharding the remaining short broker scan/publish section is deferred until
  measurement shows it consumes the freshness budget; doing it now needs a
  separate durable per-pool publication-order proof.

#### Implementation phases

These are implementation and review phases inside one required stacked
initiative, not service-selectable runtime modes. The global protocol gate
above ensures that only the lock-coupled or sequenced path is authoritative;
there is no service selector or behavior flag.

Release-authority prerequisite (before phase 0). Implement the external
supervisor in this repository under `sky/release_supervisor/`: `cli.py` owns
the apply/status/decommission API; `orchestrator.py`, `intent.py`, `gateway.py`,
and `ledger.py` own Python orchestration, deterministic intent/envelope
verification, semantic Kubernetes I/O, and DynamoDB transactions. `rpc.py` and the checked-in protobuf under
`sky/schemas/proto/release_supervisor.proto` own the closed worker/gateway ABI,
but Python never implements Helm storage.

The credentialless Helm worker is a checked-in Go module at
`sky/release_supervisor/helmworker/` with pinned `go.mod`/`go.sum`,
`cmd/sky-helm-worker`, generated Go protobuf bindings, and internal packages for
the pinned `helm.sh/helm/v3` SDK, render/wait engine, and custom CAS Secret
driver. The protobuf regeneration script emits both Python and Go bindings; CI
rejects generated drift and vulnerable/unpinned module changes. The supervisor
image compiles that binary in a pinned Go builder, copies only the binary plus
the Python parent into the runtime image, and records both hashes in
`ReleaseSupervisorRoot`. Python spawns the binary as the sandboxed worker and
owns every credential, RPC decision, ledger transition, and semantic gateway
operation. The image build and signed OCI-intent publisher are versioned with
those sources. Platform ownership lives only in
`boltz-platform/modules/sky-release-supervisor`, which provisions the external
host/unit, ledger/KMS, trust/issuer, registry, namespaces/CRDs, writer RBAC and
admission; it never pins a SkyPilot application revision. Complete the persisted
operational prerequisite before phase 0: provision the guard namespace/admin
Secret/empty receipt and reset journal; freeze writes; run the cold Historical
Authority Reset for a nonempty source (or prove empty); complete
`READ_ONLY_ADOPTED -> ... -> SUPERVISOR_ACTIVE`; rerun the live Aurora probe;
and create the anchor. The anchor binds the resulting root/reset/adoption/probe
receipts, so no provisional/raw writer can substitute.

0. Consume the already immutable supervisor-root, reset/adoption, Aurora-probe,
   anchor, and empty-enforcement receipts from the operational prerequisite;
   phase 0 never reruns reset or adopts a release. Deploy `ANCHOR_SCAFFOLD` first;
   it adds only ordinary Helm-managed identities/RBAC, the immutable release-
   namespace `DatabaseAuthorityBundle`, and the inert recorder, with byte-
   identical authority Deployments and no database mutation. Deploy
   `ANCHOR_PREPARE` second; its bounded Job atomically normalizes the real
   legacy-owner source graph, imports reset retirements/debt, installs the
   permanent `ENFORCED` guard, and arms the rollout-spanning recorder from the
   post-reset lease/Pod baseline. Deploy the
   bounded `ANCHOR_COMPATIBLE` image third to the entire
   split-role fleet while the existing broker remains protocol v2 and produce
   the immutable `DatabaseAnchorEnforcementReceipt`. These prerequisite stack
   entries add anchor-bound
   checkout validation, session tagging, cold-start full-fleet registration on
   the epoch-isolated successor substrate,
   the fixed `pre-upgrade/-50` prerequisite proof and
   `pre-upgrade/-40` isolated-admin guard migrator that install the stable additive authority
   guard/event triggers/forced row policies before any compatible Pod starts,
   phase-0 reset/boot retirement/debt state and permanent legacy-to-sequenced
   database-role/principal fence,
   source-to-target ownership/ACL/default-privilege transfer, complete pinned
   role closure, and the exact-name
   `post-upgrade/0` `anchor-enforcement-runner` Job and exact identity/RBAC, but no
   new planner, schema 041/009 migration, or provider behavior.
   Rendering requires Kubernetes >=1.27 and rejects all optional image-worker
   Deployments before mutation. They accept no age/absence shortcut. Every
   pre-reset authority lease and process row requires the immutable reset's
   host/credential/effect-frontier proof; every post-reset captured container
   requires the recorder's exact termination evidence. Only a dead guard-aware
   compatibility boot may advance through the A-rooted
   `PHASE0_BOOT_RETIRED` proof and replacement-attempt protocol above. This
   full-fleet release is not a
   canary. The feature PR is mechanically blocked unless the anchor and receipt
   match the exact live release/database/cohort. The stacked cleanup change is
   authored at the same time and later removes the one-time enforcement action,
   recorder, and prerequisite/migrator/recorder-ready/enforcement Job identities
   and their write RBAC only after the soak. It retains the
   empty-bootstrap finalizer, transition-runner, guarded-migration identities,
   immutable receipt, sequenced role, guard policies/tombstones, and read-only
   anchor validation.

1. Extend `demand_capacity_observations` and the Serve schema with physical
   identity, generation, lease, error, validity, publication, and payload-hash
   fields. Retain its context-keyed legacy projection only for pre-activation
   `LEGACY_ACTIVE` operation and the bounded forward cutover.
   A new reader accepts a row only when the hash covers the complete legacy
   payload and all new authority fields. An old writer that updates only
   `snapshot_time`, `completed_at`, and `availability` therefore invalidates
   the hash and causes a fail-closed refresh; it cannot accidentally relabel
   old data with a new lease or generation. Add the protocol-singleton
   `zero_cost_admission_sequence`, nullable replica admission sequence,
   observation sequence, allocation generation/input hash/claim generation/map,
   durable per-pool query lease, intent-state/API request association and claim
   deadline, provider-mutation limit/generation/ticket/debt/cursor state for
   ordinary and fill requests, process boot nonce and complete
   heartbeat-attestation fields, runtime capabilities, boot-local readiness and
   retirement receipts, immutable central-database authority-anchor identity,
   source-session generation/challenge echoes, controller-leadership generations, permanent retired-
   Pod-UID revocations, protected-topology envelope/attestation, the boot-
   nonce-fenced shared fleet-transition coordinator lease/journal,
   empty-database `SEQUENCED_BOOTSTRAP`, one-way fleet
   reconciliation gate, and
   activation-pinned expected-pool validation evidence in PostgreSQL. No new
   central SQLite schema or fallback is added. The migration Job applies
   Serve041 before API009. Serve041 requires exact existing physical broker
   protocol v2 (or creates v2 for an exactly empty bootstrap) and installs the
   permanent protocol-2 database floor before creating the
   reconciliation gate; API009 checks
   the exact minimum Serve head and refuses to install its cross-schema
   statement triggers if the singleton is absent. Route Serve041, API009, every
   later additive revision, and cleanup through the one
   `GuardedSchemaMigrator`: `-20` closes effects and binds a DDL generation,
   `-10` atomically extends the protection manifest with the schema, and `-5`
   opens only after the target cohort echoes that generation. For a fresh empty
   install, a bootstrap scaffold precedes the fixed writer-capable `-25`
   finalizer, which creates the already-enforced stable guard and completes the
   permanent receipt/acknowledgement chain. Nonempty sources and later upgrades
   omit `-25`; `-30/-20` validate the completed receipt chain read-only. The
   empty `-10` path creates and stamps all
   central current heads through the one transactional
   `CurrentHeadPostgresBaseline`; it never replays historical autocommit or
   concurrent-index revisions. Move `kv_cache_db` and `recipes_db` into this
   canonical nine-lineage entrypoint and make their server runtime verify-only.
   A nonempty phase-0 source must already be at all pinned pre-feature heads.
2. Extract the Serve pool observer and repository, switch the sequenced broker,
   ordinary Kubernetes-only placement, and status to that repository, and remove
   provider callbacks from the new broker API. Add observation-generation
   provenance to a round. While `LEGACY_ACTIVE`, only the lock-coupled v2
   adapter reads or writes authority. After fleet-wide promotion, a sequenced
   reader rejects a round lacking
   provenance and drives a new provider-free round; it never falls back to a
   direct query.
3. Add `ScaleReconcileCoordinator` and pure `ReservedFillPlanner`; route every
   demand, capacity, policy, replica, provider, and restart publisher to the
   one generation protocol. Replace broad update-lock ownership and fixed-delay
   polling with token validation and fixed-rate observation.
4. Add typed plan/receipt admission and the global fair provider-admission
   scheduler/shared ordinary-and-fill mutation arbiter, make accepted replica
   rows restart-safe before provider start,
   bind at most one bounded PENDING request per prospective lane, acquire the
   mutation permit only in guarded executor claim, add the idle-worker
   reservation-before-claim seam and provider-neutral receipt adapters, and
   move readiness/provider I/O outside the manager lock. Route
   `_LegacyReplicaMutationRuntime`, `SafeThread`, `terminate_cluster()`, and
   direct replica-manager `core.down()` through the durable cleanup request;
   leave their old behavior reachable only through `LEGACY_ACTIVE`. Add
   coherent raw status and PostgreSQL history fields. Add the one
   `FleetMaintenanceCoordinator` outside controller leadership so planned
   updates and involuntary membership recovery use global quiescence, complete
   cohort re-attestation, and one atomic resume rather than pairwise effect
   handoff. In `sky/server/runtime.py` it starts beside the
   `ServerInstanceLease` before role dispatch, not through the controller
   `_BackgroundLoop`; maintenance drains/releases controller workers and its
   leader session before Kubernetes replacement. Make MAIN own API readiness
   on the role-health port and the mode-0600 retirement socket/token, point the
   API Kubernetes probe and Uvicorn diagnostic proxy to that parent authority,
   and stop/join the supervisor and every effect worker before the final
   `ServerInstanceLease.stop()`. Update the split-role API/controller/executor
   chart Deployments with the same boot-local retirement hook and at least
   120-second grace; the hook writes exact boot evidence and waits. Preserve
   API `minReadySeconds: 10` and controller/executor zero. Set
   `automountServiceAccountToken: false` for all three roles, give each role a
   distinct ServiceAccount, and mount the explicit owner-chain projection only
   on API MAIN with its release-namespace read-only
   Deployment/ReplicaSet/Pod binding. Add the explicit provider-token/CA/
   namespace adaptor interface and implement the closed table above: API gets
   owner-chain only; local Kubernetes provider projects into controller and
   executor; local external-LB projects into controller and moves preflight
   there. Render no provider token when both modes are off except during the
   first revision of the typed `EXTERNAL_LB_CLEANUP` transition; the second
   removes it only after the UID-complete absence receipt. Route adaptor client construction, context
   availability/namespace detection, provisioning, external-LB preflight, and
   external-LB create/reconcile/delete through that single interface and remove
   every ambient fixed-service-account-path fallback. Mount the scaffold-created immutable same-namespace
   `DatabaseAuthorityBundle` in all three MAIN containers. Provider RBAC is
   role-specific and excludes Helm history, supervisor/guard objects, writer
   identities, impersonation/token minting, RBAC mutation, and transition
   state; no authority ServiceAccount gets guard-namespace RBAC. Consume the
   scaffold-managed permanent transition-runner identity for the three
   target-image phases at `-30`, `-20`, and `-5`, the conditional `-25`
   anchor-evidence hook, and the existing migration hook at `-10`. The runner phases use the shared transition lease and
   library, API-read exact target PostgreSQL Secret, immutable chart-rendered
   target payload, and read-only Kubernetes access except for create-only
   ConfigMap evidence, with no provider credentials or authority membership;
   their only Kubernetes write creates the complete immutable revision receipt,
   while the authority anchor and enforcement receipt are read-only. They reject a source/target database-identity
   mismatch and prove the reset's zero-old-session barrier plus a fresh
   successor-cohort source-A challenge before mutation. The pre-migration ABI and immutable revision receipt
   reach maintenance or forward-resume any preparation cursor before any
   post-activation migration; the post-migration phase journals the target.
   Broken live ACK loops are permanently revoked after the fixed bounded proof.
   Add the bounded
   `api_requests` old-writer flush before the first forward activation mutation;
   require maintenance for every post-activation authority Deployment change;
   and reject activation outside split-role PostgreSQL HA.
5. In the same stack, author the draft removal PR that deletes the legacy
   observer/direct-query APIs, broad lock, manager event, speculative debit,
   void fill batch, sentinel transport, direct cleanup thread runtime/untyped
   provider calls, activation implementation, transitional
   `anchor-enforcement-runner` command/write RBAC, phase-0 termination-recorder
   runtime/RBAC, Historical Authority Reset implementation and provider/
   infrastructure adapters (while retaining its receipt/debt/tombstones), and
   legacy projections. Retain the sequenced bootstrap/maintenance kernel, stable
   pre-migration fence/receipt ABI, enforced database-authority
   guard/triggers/forced row policies, guarded-DDL event triggers and manifest
   extension ABI, transactional current-head baseline/empty finalizer, permanent
   receipt/final-resourceVersion acknowledgement, complete pinned
   role/owner/default-privilege closure and per-transaction live validation,
   sequenced role and phase-0 retirement
   tombstones, immutable database authority anchor, source-session validation,
   unsequenced-writer triggers, admission sequence,
   transition lease, nonce-scoped process retirement, and permanent Pod-UID
   revocations.
   The cleanup migration initializes only an exactly empty database in
   `SEQUENCED_BOOTSTRAP`; its `SEALED` removal receipt precludes an existing
   legacy/preparing source, and runtime drift to either state is rejected with
   instructions for a newer capable fix-forward revision, never an old image.
   Keep it blocked only on the direct full-rollout evidence below, then merge
   it without another feature rollout path.

All three fixed phase-0 stack entries merge and deploy in order, and the compatible
fleet reaches its immutable full-fleet receipt before the feature PR can merge.
The feature PR may be split into reviewable commits, but phases
1--4 do not merge or activate independently. Deployment under `LEGACY_ACTIVE` exists only
to attest the complete sequenced capability before the atomic fleet promotion.
This avoids a partially authoritative observer, an accepted-intent queue
without restart recovery, or two planners becoming active together.

#### Single-path deprecation ledger

| Superseded path | Final owner | Removal gate |
| --- | --- | --- |
| Broker-owned direct Kubernetes query and separate demand/status observation cache | Serve pool observer and one PostgreSQL observation repository | Observation equivalence, UID/lease race, blackout, and broker-conservation tests pass; all new readers use the repository. |
| Wall-clock `snapshot_time`/`created_at` race ordering | Protocol-singleton `zero_cost_admission_sequence` committed atomically by observations and every zero-cost insert | PostgreSQL commit-order, rollback, clock-skew, ordinary/fill race, and scan-to-publish-gap tests pass. |
| Poller-wide `_actuation_epoch_lock` and autoscaler-wide ownership of that lock | Short policy-revision capture/CAS | Update/disable/owner-race tests and 250-second convoy test pass. |
| Sleep-after-work poll cadence | Monotonic fixed-rate observer | Fake-clock missed-deadline and no-catch-up-storm tests pass. |
| Manager `_scale_reconciliation_event`, `clear_scale_reconciliation_signal()`, and `wait_for_scale_reconciliation()` | Controller `ScaleReconcileCoordinator` | Every capacity, demand, replica, provider, and restart publisher is ported and lost-wakeup tests pass. |
| `_apply_reserved_capacity_fill_v2()` speculative emission-time debit and void fill `scale_up_batch()` seam | `ReservedFillPlanner`, `FillPlan`, and `FillCommitResult` | Prefix/partial-failure/restart/idempotency tests pass and no unaccepted tail changes feed. |
| Provider launch and readiness/network work while the manager lock is held, plus process-local/independently derived concurrency checks | Existing API request executor with bounded submit/observe/cancel checkpoints, one persisted-limit ordinary-and-fill mutation arbiter, and snapshot/probe/conditional merge | Manager/crash/quiescence, ordinary-versus-fill CAS, limit-change, and indefinite-wait two-pool fairness tests pass. |
| `_LegacyReplicaMutationRuntime.down_thread_pool`, replica-manager `SafeThread`, and untyped `terminate_cluster()`/`core.down()` cleanup | One durable `CLEANUP`/`CANCEL` API request, typed provider permit, and provider-neutral receipt executor | Direct-call rejection, restart before/after request claim/effect receipt, exact-object CAS, ordinary-versus-fill priority, and maintenance-quiescence tests pass. |
| Dictionary sentinel metadata for fill authority | Typed `FillIntent` fields | Every producer and consumer is typed and old-pickle compatibility is confined to deserialization. |
| Ambient/default kubeconfig and direct fixed service-account-path reads across authority and non-authority Kubernetes callers | One explicit `KubernetesCredentialSource` abstraction with projected-files and kubeconfig/exec implementations | Repository search finds no direct client/helper path read outside composition roots; provider, LB, UID-fence, kubectl/exec/port-forward/rsync, token-rotation, and non-authority regression tests pass on east/PHX audience fixtures. |
| One-time Historical Authority Reset, anchor-preparation recorder, and `ANCHOR_COMPATIBLE` `anchor-enforcement-runner` action/write RBAC | Permanent read-only `DatabaseAuthorityAnchor`, immutable reset/enforcement receipts and debt/tombstones, sequenced role/live role-closure predicate, and enforced guard/protection catalogs/event triggers | Closed actor/credential/runtime inventory, old-egress and issuer revocation, infrastructure-terminal proof, effect-frontier classification, live Aurora capability probe, post-reset gap-free recording, exact relation-lock barrier, actual-prior-binary zero-provider-call, writer-RBAC/receipt-CAS/final-RV acknowledgement, and current-head fresh-bootstrap tests pass; cleanup retains guard, receipts, baseline/finalizer ABI, catalogs/policies, retirements, and debt but no reset executor, recorder, writer, or enforcement command. |
| `LEGACY_ACTIVE` adapter, one-way activation implementation, writer-drain barrier, and legacy observation/request projections | Permanent sequenced bootstrap/active/maintenance path with unsequenced-writer and Pod-revocation fences retained | Ten-minute binary acceptance, a fixed 24-hour full-fleet sequenced soak, executable fresh-bootstrap/rejection, Kubernetes BCL-preemption, and fix-forward maintenance tests pass; the stacked removal PR becomes the minimum supported image. |

The transition PR must deprecate these symbols in code and documentation and
must be stacked with a draft removal PR that deletes them. The removal PR's
exact merge gate is completion of the full-rollout acceptance window below on
one durable `PASSED` validation run for the intended immutable digest and
activation/limit generation, including exact inventory revisions for every
sample, no stale acceptance, no lost tail, and no paid spill, followed by its
fixed 24-hour full-fleet soak with zero safety violations. Every maintenance
entry or inventory identity change resets it; verifier continuity alone cannot
hide a replacement.
Positive BCL reclaim is supplied by the real kube-scheduler integration test;
the gate never waits indefinitely for naturally occurring production traffic
or capacity. The removal PR tests only the final
bootstrap/active/maintenance path, including fresh empty installation and
machine-readable rejection of legacy/preparing databases. Existing draft
protocol-v1 cleanup PR #1263 remains a separate older deprecation stack; this
correction expands or rebases it where symbols overlap and does not add another
protocol-v1 behavior.

## Deployment and fix-forward operation

### Current reconciliation correction: direct full rollout, no canary

This correction has no capacity-consuming canary, shadow planner, service
selector, or user behavior flag. The fleet-wide safety gate above is mandatory,
not an experiment. The convoy depends on a large live fleet, and a
small service would produce false confidence while a second active path would
make debit and `max_replicas` ownership ambiguous. The pre-deployment gates
are therefore deterministic concurrency tests, the PostgreSQL Serve suite,
the exact-head formatter/type/lint gates, three passing adversarial reviews of
this exact canonical design, and successful fix-forward maintenance rehearsal.

One internal transition module is the only gate implementation for operators
and the chart runner:

```bash
python -m sky.serve.reserved_fill_reconciliation_transition status --json
python -m sky.serve.reserved_fill_reconciliation_transition enforce-anchor
python -m sky.serve.reserved_fill_reconciliation_transition activate
python -m sky.serve.reserved_fill_reconciliation_transition begin-maintenance
python -m sky.serve.reserved_fill_reconciliation_transition resume
python -m sky.serve.reserved_fill_reconciliation_transition verify
```

The mutating commands have no inventory, service, digest, timeout, or subset
arguments; they discover and attest the complete fleet. The transitional
`enforce-anchor` command exists only in the compatibility/transition image,
rejects API-pod invocation, and runs only in the exact chart-created phase-0
Job identity. It reads the operator-provisioned anchor, verifies the already
completed reset and `ENFORCED` import, attests every compatible successor boot/
session/effect-debt identity, permanently retires any failed post-reset boot
through the typed proof, and completes the immutable enforcement receipt. It
never revokes a legacy credential, drains/rebinds an old process, or reopens old
egress; cleanup removes the command while retaining checkout validation. The other mutating commands
resume the durable forward activation, bootstrap, or maintenance journal. The final `activate` or
maintenance `resume` transaction creates the one qualifying validation run
for that generation with `started_at` equal to the database timestamp of its
atomic `SEQUENCED_ACTIVE` commit. Bootstrap `resume` creates only the
non-qualifying bootstrap-health record. `verify` only resumes and
evaluates that run, never substitutes its invocation time. It
reconstructs any earlier interval from durable snapshots, applies the fixed ten-minute binary gate and
24-hour soak without operator-adjustable durations, and returns nonzero on a
failed/reset run. `status` is read-only. Operators normally run every command
except `enforce-anchor` in one current API pod. The phase-0 Jobs,
anchor-evidence Job, and three transition-runner phases use the same library
from the target image when that pod is unavailable; PostgreSQL ownership makes
caller loss harmless and prevents API and Job callers from creating different
runs.

#### External supervised release authority

Every SkyPilot release write uses one permanent external
`sky-release-supervisor`. Platform IaC, not the SkyPilot chart, provisions and
upgrades it; it is the sole principal authorized to mutate Helm history Secrets
or Helm-owned objects. Raw Helm install/upgrade/rollback/uninstall, Terraform
`helm_release`, an in-cluster writer Job, and a second supervisor are unsupported
after cutover. The deleted in-cluster Job/ServiceAccount/Lease idea is not a
fallback.

A caller submits only an immutable signed OCI release-intent digest and the
expected current predecessor identity. The operation key is
`(cluster_uid, release_instance_id, release_namespace_uid, release_name,`
`expected_prior_revision_and_history_hash, desired_intent_digest)`. Before
acknowledging it, one conditional DynamoDB transaction writes the hash-chained
`ACCEPTED` entry and advances the operation-chain head in the platform-IaC-
owned, KMS-encrypted ledger. The service acknowledges only after DynamoDB's
successful response; a lost client response is recovered by a strongly
consistent read of the same idempotency key and chain head. Kubernetes and the SkyPilot database are not
release-operation authority. Client disconnect, timeout, or retry never cancels
work; an identical request returns the existing operation and the same
predecessor with a different intent conflicts. Cancellation only drains at the
next proved-safe boundary.

`apply --output json` returns only durable acceptance plus `operation_id` and
`attempt_envelope_id`; success is never inferred from that response. The sole
read-only query is:

```bash
sky-release-supervisor status \
  --operation-id "$OPERATION_ID" \
  --output json
```

It reports the ledger generation, intent/envelope/predecessor hashes, current
state (`ACCEPTED`, `RENDERED`, `APPLYING`, `RECOVERING`, `BLOCKED`,
`SUCCEEDED`, or `FAILED_FORWARD`), typed blocker/result, and latest attempt.
`SUCCEEDED` and `FAILED_FORWARD` are terminal for that operation; `BLOCKED`
requires the documented operator fence/evidence and is not terminal. Status has
no cancel, retry, mutation, or timeout side effect. A caller polls it to a
terminal result (or reports `BLOCKED`) before deriving the next predecessor.

The signed OCI release intent is the complete revision-neutral desired input:
chart archive, vendored dependencies/lock, schema, full canonical target
values, cluster/release/namespace identities, pinned Helm SDK/Secret-driver/
gateway protocol versions, Secret references by stable logical identity and
content digest, and signed expected application-manifest, hook-template, and
inventory projections. It contains no Helm revision, live predecessor hash,
resourceVersion, operation ID, attempt ID, or live-read result.

For each attempt, the supervisor stable-double-reads the accepted predecessor
and constructs a KMS-signed, ledger-retained `ReleaseAttemptEnvelope` that
binds the intent digest, exact prior release config/history hash, revision,
operation/attempt IDs, Secret name/UID/resourceVersion/content digest, and one
closed versioned external read-set: exact anchor/receipt objects and
predecessor Deployment, release-history, namespace, and RBAC identities with
their UID/resourceVersion/canonical hashes. The envelope resolves all
conditional hooks, `resourceNames`, revision-bound names, and
`DatabaseAuthorityBundle` bytes as ordinary deterministic render input. The
supervisor verifies the intent manifest digest
and platform signature against an IaC-owned root, safely extracts with path,
link, count, and size limits, and caches bytes read-only by digest before
execution. Before render and again before the first mutation, its parent fresh-
reads the same exact objects through the semantic gateway and requires the
envelope identities/hashes; the sandboxed worker and chart have no Kubernetes
read or Helm `lookup` path. Mutable tags, local chart paths, runtime `--reuse-values`, dependency
downloads, worker registry/DNS access, inline Secret values, plugins, post-
renderers, `lookup`, random/time functions, `generateName`, and unenumerated
revision-dependent fields are rejected. The intent may inspect a predecessor
as an authoring aid, but no base identity or merge result is signed authority:
its target values are complete, and the supervisor never performs reuse or
live merging. Each attempt envelope independently binds and validates the
actual accepted predecessor. Retrying the same intent as `N+1` creates a new
attempt envelope against the now-latest recovered/failed Helm record while
preserving every ordinary-object target projection; only enumerated revision-
bound hook/history identities change, and irreversible hooks resume exclusively
from durable database receipts.

The supervisor starts one sandboxed Helm-SDK worker at a time on its attested
host. The worker has no Kubernetes/registry credential, network route, host
mount, exec credential, plugin, privilege, ptrace, or delegated cgroup; it uses
peer-credentialed Unix RPC to the parent. The service uses
`KillMode=control-group`; `pidfd`/`waitid` plus an empty nondelegated cgroup prove
the worker and every descendant dead. Worker exit status is never release
authority.

The parent exposes a closed semantic read gateway as well as the mutation
gateway; it is never a general HTTP proxy. The read ABI has a discovery/schema
snapshot pinned by the attempt envelope, exact GET of only enumerated
GVR/namespace/name identities, typed Helm-history Get/List constrained to the
one release, and deterministic polling of only the attempt's exact hook Job and
owned Pods. It provides no arbitrary list/watch, logs, exec, proxy, Secret-data
read, or unenumerated discovery. Every response and canonical projection hash
is appended to the attempt chain; a resource outside the signed inventory or a
changed discovery/owned-field schema blocks. The credentialless Helm worker
uses only this peer-credentialed RPC for three-way reads, custom-driver state,
and waits.

Before
every send it conditionally appends hash-chained durable `PREPARED` and `SENT`
records with sequence, attempt, verb, GVR/namespace/name, exact UID/
resourceVersion preimage, canonical request, signed owned-field postcondition,
and hashes; afterward it records `ACK` or `UNKNOWN`. No later mutation or
recovery proceeds while a call is unresolved. The closed write ABI supports
only deterministic-name creates carrying operation/attempt identity; full UID/
resourceVersion-conditional updates; deletes with UID/resourceVersion
preconditions plus exact descendant/finalizer convergence; and typed Helm
release-Secret creates/CAS updates through a custom driver. Lost create/update/
delete responses reconcile through an exact GET/absence proof or replay of the
identical CAS. An object matching neither the exact preimage nor authorized
postimage blocks. Admission/defaulted/status fields are excluded only by the
versioned signed owned-field projection. Unsupported verbs/subresources block.
This per-object journal/CAS and the single proved-dead worker replace the
invalid cross-object-Lease claim.

The supervisor sets `SkipCRDs=true` and `CreateNamespace=false`; platform IaC
owns all namespaces and required CRDs because Helm may install CRDs before
writing pending history. `Atomic`, `CleanupOnFail`, `Force`, `Recreate`,
`TakeOwnership`, rollback, generic Helm uninstall, and referenced-history pruning are
forbidden. The separately typed supervised decommission below is not Helm's
uninstall action. Helm Secret storage is the only driver. Its custom adapter requires
the exact Secret name/UID/resourceVersion on every update; stock unconditional
`Storage.Update` is not used.

Generic Helm `Wait` is disabled. Each signed intent carries one closed phase-
specific postcondition program evaluated by the parent read gateway. Scaffold
requires only its deterministic identities/RBAC/bundle and inert recorder Pod
created or explicitly Pending for the signed successor-schedulability reason;
it never waits for the intentionally terminal authority Deployments. Prepare
requires terminal `-50/-40`, the armed recorder Pod on the epoch-isolated
successor substrate, and the gap-free `WATCH_ARMED` receipt. Compatible requires
all successor authority Pods Ready/effect-ineligible, every hook receipt, and
the immutable enforcement receipt. Feature and ordinary maintenance intents
require their journal target plus exact cohort convergence. Each phase-0 Job
and recorder template carries the reviewed successor toleration/node selector,
transition ServiceAccount, and no old credential; the supervisor proves at
least one admissible successor node from signed identity/capacity facts before
the first mutation. Timeout records an incomplete pending attempt for typed
recovery; it never changes a safety fact or causes rollback.

Release completion is a custom finalization protocol. After all manifest and
hook postconditions and durable receipts are proved, the gateway CAS-updates
the exact prior `deployed -> superseded` record and then exact new
`pending-upgrade -> deployed` record. Both transitions are journaled and
idempotent; recovery accepts only the precise intermediate
`old=superseded,new=pending` and completes the second CAS. An install has only
`pending-install -> deployed`. No revision is deployed while another remains
authoritative or before its full inventory is proved.

Pending recovery starts only after the worker cgroup is empty, every
`SENT/UNKNOWN` call is reconciled, every exact hook Job and owned Pod is
terminal with no retry/finalizer path, and each deleted successful hook has its
exact database receipt. PID death, Job/Pod absence, time, or host unreachability
alone is insufficient. The supervisor classifies the exact latest history plus
journal: retry before any sent mutation; finish complete pending finalization;
CAS only pending status/fixed description/driver label to failed when effects
are quiescent but finalization is incomplete; recognize that exact recovery
marker idempotently; accept an exact deployed target only with matching
inventory; or prepare `N+1` after an exact quiescent child-produced failure.
Higher/nonlatest/multiple pending records, two deployed records, or any
identity/hash/effect ambiguity block. A lost repair response succeeds only on
the exact marker and raw/decoded preimage hash. Repair never runs hooks, mutates
a manifest/database object, deletes/supersedes history, or accepts a caller-
selected revision/hash. `N+1` uses the same signed desired intent with a new
attempt envelope after an
infrastructure interruption or a newer reviewed intent after a target defect;
both traverse this one supervisor state machine and resume database cursors.

A supervisor process crash restarts on the same attested host and recovers from
the external ledger. There is no automatic host failover. Replacing a lost host
requires platform power/network fencing, revocation of its issuer/auth mapping,
waiting the maximum prior credential TTL, reconciliation of every ledger call,
replacement-host attestation, and only then a new short-lived credential. A
cloned or revived host stays denied. Unprovable death or lost ledger blocks
release writes; availability never weakens fencing.

Before the first supervised release operation, platform IaC creates the
supervisor host/unit/image pin, ledger, host identity/trust roots, short-lived
credential issuer, exact writer RBAC/admission, registry access, namespaces,
and CRDs. It creates an immutable signed `ReleaseSupervisorRoot` binding their
identities/spec hashes; the later `DatabaseAuthorityAnchor` binds that root and
the exact adoption receipt. For a nonempty pre-recorder source, cutover occurs
only after the cold Historical Authority Reset has made the old substrate
terminal. During that reset, platform admission freezes every release write and
allows one pre-registered reset principal only the exact signed old-identity/
RBAC deletion set; that principal cannot create or mutate a Deployment, Helm
history, successor identity, or guard object and is permanently revoked at
cutover. This is the only pre-supervisor exception and is removed by the
transition cleanup.

Existing-release cutover is one persisted state machine:
`FREEZE_WRITES -> RESET_COMPLETE_OR_EMPTY -> READ_ONLY_ADOPTED ->`
`TERRAFORM_OWNER_REMOVED -> OLD_WRITERS_REVOKED -> SUPERVISOR_ACTIVE`.
Read-only adoption requires nonpending history, imports/hashes complete history/
config plus the reset-fenced live inventory, binds the immutable reset receipt
for a nonempty source, and writes an adoption receipt. For each Helm-history
manifest object removed by the reset principal, the receipt records one
`EXTERNALLY_DELETED_BEFORE_ADOPTION` entry with the prior manifest bytes/hash,
last live UID/resourceVersion, signed reset-deletion request/ACK and exact GET-
absence proof. That entry is the only legal missing-preimage transition: the
first supervisor diff treats the enumerated delete as already satisfied and
journals a no-op; a different/missing object, recreate, or absence without the
entry blocks. No later attempt may create such an entry. The receipt also binds
the Terraform backend/workspace, state lineage/serial, exact resource address,
configuration commit, and pre-change plan hash. Platform IaC removes or moves
the `helm_release` resource from desired configuration and state without
touching live resources; a fresh `terraform plan` must prove zero create,
update, or delete for the release and its former address before the state can
advance. A bare `terraform state rm` while the resource remains configured is
forbidden. The cutover then revokes every raw Helm/CI/application/reset writer
and atomically activates the supervisor binding conditional on the adoption,
Terraform-plan, and revocation receipts. It proves a supervisor semantic dry-
run succeeds and every competing write is denied.

An empty source has no reset receipt and requires an exact empty live/history
match. Unknown legacy pending state or drift outside the signed reset deletion
set simply blocks adoption. If legacy tooling first normalizes a pending record,
that is a separately designed and approved pre-cutover operation under old
authority; it must produce an exact nonpending release and then enter this same
ordinary adoption path. The supervisor has no manual-patch exception.
Supervisor upgrades drain intake and prove no worker,
unresolved call, active hook, or pending/finalizing release; replacements must
read all ledger versions and follow the manual credential handoff above.

Release retirement is a two-step supervised fix-forward sequence. First a
signed `RETIRE_RELEASE` intent enters permanent maintenance, stops admission,
downscales/deletes every managed service and workload through the ordinary
typed effect path, resolves all provider debt, commits the immutable database/
release decommission receipt, then scales the three authority Deployments to
zero with exact process/Pod termination convergence. It retains the transition
Jobs and supervisor authority; crash recovery resumes the same retirement
journal. Only then may `sky-release-supervisor decommission-release` remove the
release. That operation requires the receipt, permanently closed effects, zero
authority/workload processes and provider debt, an empty worker cgroup, no
unresolved gateway call or hook action, and an exact nonpending deployed
inventory. A signed decommission intent enumerates
every Helm-owned ordinary object and its UID/resourceVersion; the gateway
deletes only that set with preconditions, never a guard/evidence object, then
CAS-marks the latest Helm Secret `uninstalled` while retaining all history.
Crashes resume the same object journal; absence without a prior exact delete
ACK/GET proof blocks. The operation revokes release credentials only after the
last owned object is gone and retains the supervisor ledger/history through the
database audit period. It is irreversible and does not authorize reinstall or
rollback under the same `release_instance_id`; a later installation needs a new
anchor and instance identity. Raw Helm and the discarded writer path are
deprecated at cutover and deleted by the stacked cleanup PR.

A validation run is keyed by activation generation, exact implementation-SHA/
image-digest inventory hash, capability-contract hash, and mutation-limit generation;
every sample additionally names its exact inventory hash/revision. Verifier
restart resumes the same database `started_at` and fixed deadline--it never
restarts or extends the clock. Any process replacement enters the common
maintenance kernel and creates a new inventory revision/run on resume. A
digest, implementation, capability contract, mutation-limit generation,
scale/topology inventory change, maintenance entry, safety violation, or
missing durable health coverage resets/fails the run and requires a new full
24 hours after the next successful active identity. The cleanup PR requires one durable
`PASSED` run, so verifier or pod restarts cannot manufacture soak evidence.
Capability-compatible mixed digests may satisfy the safety flip, but `verify`
requires exactly one implementation SHA and immutable image digest across the
active inventory; a mixed set cannot pass binary acceptance or start the soak.

The run also pins the non-empty expected pool-observation set created by the
activation transaction. The verifier evaluates database-clock candidate
intervals at fixed 60-second boundaries. A candidate is accepted only if every
expected descriptor has published a strictly newer successful authoritative
observation generation than that descriptor's durable last-consumed cursor,
initialized to the generation captured at activation, with no observation
older than 75 seconds at close. Sampling atomically records
the exact closed outcomes and advances a descriptor's cursor to any newer
present success/blackout/error generation even for an invalid candidate; a
missing descriptor leaves its cursor unchanged, and the strict-newer predicate
still prevents reuse of its earlier success. Thus one observation cannot be
reused across interval boundaries. A reported free count of zero is successful
evidence. Any blackout,
provider error, unresolved or changed physical identity, missing descriptor,
payload-hash failure, or configured/claimed-set change invalidates that
interval; a set or identity change resets the run under the topology rule
above. An invalid interval resets the consecutive-interval counter and the
original ten-minute deadline is not extended. Exceeding 75 seconds without a
new successful observation is a typed run failure. Thus `N/A` applies only to
the publication-to-accept latency event when all observation coverage is
successful but no positive spendable allocation occurs; it can never turn
missing telemetry into a pass.

Before deployment, platform IaC provisions and attests the external release
supervisor, ledger, credential issuer/RBAC/admission, OCI trust root,
namespaces, and CRDs, then creates the immutable `ReleaseSupervisorRoot`; it
does not yet grant the supervisor release-write authority. Record the current
immutable image digest, complete Helm values,
release revision/history hash, service versions and controller owners, broker protocol and
rounds, claims, replica counts by state/pool/card, canonical capacity
observations, and Kubernetes pod counts/priorities. Capture tentative source
evidence for reset planning, but do not create the anchor or accept a capability
receipt yet. Provision the
dedicated guard namespace and admin Secret, record the Secret UID/
resourceVersion/content digest, and prove the release API ServiceAccount has no
RoleBinding or Pod-create path there. In that namespace, pre-create the named
empty enforcement receipt with proved-empty ownerReferences/finalizers/
ownership-hook annotations. For the nonempty source, execute the complete cold
Historical Authority Reset, create its immutable retained receipt, and keep all
old effects fenced; its pre-registered reset principal may remove only the
signed old-identity/RBAC set while every release write is frozen. Then adopt
the exact nonpending, reset-fenced release through the persisted
`READ_ONLY_ADOPTED -> TERRAFORM_OWNER_REMOVED -> OLD_WRITERS_REVOKED ->
SUPERVISOR_ACTIVE` states: import the known reset deletion set and complete
history/config/live inventory; remove/move the `helm_release` from desired IaC
and state; prove the fresh no-op Terraform plan; revoke the reset principal and
every other writer; conditionally activate the supervisor; and prove its dry-
run plus competing denial. An empty source skips the reset and requires exact live/history
identity at adoption. This one-time infrastructure prerequisite is not a
boltz-platform SkyPilot application pin or canary.

After reset/adoption and immediately before anchor creation, rerun the complete
live Aurora capability probe and zero-residue check against direct source A,
then stable-revalidate the reset's database cutoff, role graph, Secret
identities, ownership/ACL source graph, and reset receipt. The final receipt's
database time, server identity, role/catalog hashes, and reset generation are
bound together; any drift restarts probe/anchor preparation while the reset
remains closed. Only then construct and create the immutable
`DatabaseAuthorityAnchor` outside Helm, binding `ReleaseSupervisorRoot`, the
adoption receipt, guard namespace/admin Secret, reset and enforcement receipts,
Aurora authority, and direct source-A identity; record every object identity/
hash. Verify
Kubernetes >=1.27 and disable/block
every optional image worker. Build and publish the reviewed compatibility
image and signed revision-neutral OCI release intents with complete canonical
values. Each submission supplies the exact expected predecessor; the
supervisor binds it into the retained attempt envelope. First apply the reviewed
`ANCHOR_SCAFFOLD` intent with the exact
current API/controller/executor image digests and otherwise byte-identical
authority Deployment envelope. Its guard/recorder image is the reviewed
compatibility digest, but it performs no database operation and the application
cohort does not roll:

```bash
sky-release-supervisor apply \
  --intent "oci://$RELEASE_INTENT_REPOSITORY@$ANCHOR_SCAFFOLD_INTENT_DIGEST" \
  --expected-prior-revision "$ANCHOR_SCAFFOLD_PRIOR_REVISION" \
  --expected-prior-history-sha256 "$ANCHOR_SCAFFOLD_PRIOR_HISTORY_SHA256" \
  --output json
```

The scaffold chart rejects unless those images and the whole authority envelope
equal live state; verify the managed cross-namespace identities/RBAC and inert
`WAITING_FOR_GUARD` recorder, with an unchanged database hash. Record the
returned operation ID and attempt-envelope ID, poll the former to its terminal
success, and only then refresh the exact predecessor revision/history hash.
Then apply the separately signed `ANCHOR_PREPARE` intent:

```bash
sky-release-supervisor apply \
  --intent "oci://$RELEASE_INTENT_REPOSITORY@$ANCHOR_PREPARE_INTENT_DIGEST" \
  --expected-prior-revision "$ANCHOR_PREPARE_PRIOR_REVISION" \
  --expected-prior-history-sha256 "$ANCHOR_PREPARE_PRIOR_HISTORY_SHA256" \
  --output json
```

Its
`-50/-40` chain must atomically normalize ownership/install the guard, the
recorder must become healthy, and `post-upgrade/0` must persist a gap-free
`WATCH_ARMED` cursor. Do not begin the next revision without that receipt.
Record and wait for its operation/envelope exactly as above; refresh the
predecessor only after terminal success or completed typed recovery. Then
upgrade all three roles to the compatible image through its complete signed
intent; its canonical values explicitly contain all anchor/credential
identities and all three image digests:

```bash
sky-release-supervisor apply \
  --intent "oci://$RELEASE_INTENT_REPOSITORY@$ANCHOR_COMPATIBLE_INTENT_DIGEST" \
  --expected-prior-revision "$ANCHOR_COMPATIBLE_PRIOR_REVISION" \
  --expected-prior-history-sha256 "$ANCHOR_COMPATIBLE_PRIOR_HISTORY_SHA256" \
  --output json
```

The compatibility chart's fixed `-50/-40` identity/guard revalidation hooks and
fresh recorder barrier must pass before Helm changes any Deployment.
For `ANCHOR_PREPARE`, `ANCHOR_COMPATIBLE`, and every later hook-bearing
revision, `--no-hooks`, `--atomic`, `--rollback-on-failure`, `helm rollback`,
and an unanchored generic migration Job are forbidden and fail closed when they
reach the database guard. `ANCHOR_SCAFFOLD` intentionally renders no hooks. A failure before the first
guard transaction leaves the database unchanged, but an existing source is
already cold and irreversibly fenced by its reset (the inert scaffold resources
remain); only a newer phase-0 image/revision is a recovery action. An old chart
cannot restore old egress, revoked credential issuers, terminal substrate,
legacy database login, or reset-retired identities. Removing the recorder only
blocks new-baseline retirement evidence. There is no mixed old/new authority
rollout: old Pods cannot schedule on the epoch-isolated successor substrate and
only the compatible cohort receives successor credentials. The dedicated post-upgrade Job automatically invokes
the zero-argument `enforce-anchor` action after the exact full fleet is ready.
Do not use Pod deletion as evidence: every pre-reset runtime/credential/effect
actor must be covered by the immutable reset receipt and every later captured
container by the recorder. A later guard-aware compatible boot that dies before
ACK must pass the A-rooted `PHASE0_BOOT_RETIRED` effect/debt proof and be
replaced in a newer attempt. Any evidence ambiguity remains fenced. Continue
fix forward until every live
boot ACKs or is safely retired and the immutable enforcement receipt exists.
After that receipt, the compatibility image is the minimum supported image and
existing protocol-v2 effects may resume only through token-bearing,
anchor-validated sequenced-role sessions on A; the legacy role remains
`NOLOGIN`.

Build and publish the feature image and complete signed release intent from the
reviewed implementation head, then submit that digest directly to the external
supervisor; do not use or wait for a boltz-platform SkyPilot application PR.
`ANCHOR_COMPATIBLE` and the
feature upgrade are the two direct full-fleet releases, not canaries. The
preceding `ANCHOR_PREPARE` revision is
safety instrumentation with a byte-identical application cohort, and
`ANCHOR_SCAFFOLD` is inert chart ownership; neither is a service
or capacity canary. The production release must already be
split-role PostgreSQL HA while the gate is `LEGACY_ACTIVE`. Do not run the
migration out of order or use `--no-hooks`: the chart's fixed
`-30/-20/-10/-5` prerequisite, fence, migration, and target-attestation hooks
must all pass before Deployments change. Do not use `--atomic`,
`--rollback-on-failure`, or `helm rollback`; they can submit an old
declarative target that the one-way database fence must reject. A supervised
writer failure leaves the current gate and journal unchanged. If it also leaves
the latest Helm record pending, the supervisor runs the exact recovery protocol
above before it accepts `N+1`. Because implicit reuse could retain the old
60-second grace or independent controller/executor overrides, intent
construction supplies complete values with all three images and all three
120-second grace values explicitly:

```bash
sky-release-supervisor apply \
  --intent "oci://$RELEASE_INTENT_REPOSITORY@$FEATURE_INTENT_DIGEST" \
  --expected-prior-revision "$FEATURE_PRIOR_REVISION" \
  --expected-prior-history-sha256 "$FEATURE_PRIOR_HISTORY_SHA256" \
  --output json
```

Use the actual immutable intent repository/digest and refreshed exact
predecessor for the environment. Record the returned operation/envelope IDs,
poll terminal status, and record its release/name/image values, rendered target
hash, and all four
ordinary hook results. Any fresh-bootstrap
attempt whose receipt is still both empty and mutable runs the conditional fifth `-25`
hook; it is omitted once the receipt is immutable. Setting all three images is mandatory because
the signed predecessor can contain independent controller/executor overrides;
the complete target intent must explicitly replace them, and relying on their
fallback to `apiService.image` is not an attestation. Ordinary rolling pod replacement is allowed,
but one service must never
have two controller owners. The command must not change
`apiService.dbConnectionSecretName` away from the anchored sequenced Secret or
mutate that Secret; the complete intent's target reference must equal the
immutable anchor.
Record every pinned Secret's name/UID/resourceVersion and normalized connection
identity before and after every hook, plus the immutable enforcement-receipt
UID/content hash/final resourceVersion and matching PostgreSQL acknowledgement.
The intended rollout uses the one built digest,
but promotion accepts a mixed digest inventory only when every exact Pod-bound
authority lease advertises the same required capability/schema contract under
`LEGACY_ACTIVE`; it persists the full inventory hash. Before promotion, verify every new
status/SLO field is queryable, all capabilities/process leases attest, the
previous digest refuses every post-mutation sequenced gate in the restored-copy
compatibility test, and the maintenance/fix-forward rehearsal completes. Then
run the zero-argument full-fleet promotion. Acceptance time
starts only when its atomic transaction commits `SEQUENCED_ACTIVE`.

For every later image/configuration iteration, normally run
`begin-maintenance`, wait for quiescence in `status`, build and sign a new
complete release intent with a *newer* reviewed image in all three image fields,
submit it with the exact current predecessor, wait for its supervised terminal
status, and run
`resume`. The mandatory pre-migration fence performs or resumes that same
entry if the current image cannot, and the post-migration phase journals the
target. If the target does not converge, leave the gate in
maintenance and submit only a new complete intent with the next fix-forward image; do
not deploy the previous image or call `resume` against an incomplete cohort.

Acceptance uses the existing production fleet rather than a synthetic GPU
workload. It requires five consecutive 60-second health intervals and has a
ten-minute outer deadline from `SEQUENCED_ACTIVE`. The health gate is valid even
when every pool reports zero spendable capacity; it does not manufacture GPU
demand to turn an event-dependent latency SLO into a prerequisite. Require:

- every descriptor in the activation-pinned expected pool set has a new,
  successful, authoritative observation generation in each accepted interval
  and is no more than 75 seconds old at interval close; explicit zero capacity
  passes observation coverage, while blackout, error, missing/unknown identity,
  or a changed set invalidates the interval and cannot be classified as
  latency `N/A`;
- if a stable positive spendable allocation and eligible headroom occur during
  the window, it reaches durable intent acceptance within 15 seconds of
  allocation publication; otherwise this event-dependent SLO is explicitly
  recorded as `N/A`, not passed from fabricated work and not treated as a
  rollout failure;
- published-to-applied reconciliation generation lag is at most 15 seconds,
  with a 5-second p99 objective once the controller is warm;
- every request-bound fill is either atomically claimed within five seconds or
  terminalized with typed executor backpressure, every claimed provider call
  records its bounded result within 15 seconds or explicit concurrency debt,
  no claim commits after its PENDING deadline, every expired tombstone is
  terminalized within 15 seconds, and rollout debt remains zero;
- every process attests the scheduler singleton's exact mutation limit and
  generation, combined ordinary/fill active plus live-prospective plus debt
  never exceeds it, and no Serve provider request bypasses the shared arbiter;
- raw, spendable, accepted, provider-running, and ready status agree with
  PostgreSQL rows and Kubernetes pods; if the 34-free-slot incident shape or
  any positive capacity occurs, it cannot remain idle solely because an
  observation crossed 180 seconds;
- when eligible work exists, no pool or service is starved, independent pools
  hold provider work concurrently, and a blocked lane does not consume every
  provider slot; deterministic tests remain the proof when production has no
  eligible work;
- stale acceptance, oversubscription, paid spill, duplicate provider requests,
  and total replicas above `max_replicas` remain exactly zero; and
- live pod PriorityClass, numeric priority, and preemption-policy inspection
  still proves inference cannot evict BCL and BCL may evict inference. The
  deterministic scheduler/integration suite is the positive reclaim proof. A
  naturally occurring BCL reclaim during the window is corroborating evidence,
  not a binary-acceptance prerequisite, and no BCL job is launched solely for
  this gate.

Any missing observation beyond 75 seconds, eligible stable-positive
publication-to-accept gap beyond 15 seconds, late PENDING claim, expired
tombstone older than 15 seconds, unresolved concurrency debt,
provider receipt-checkpoint deadline violation, stale persist, paid spill,
oversubscription, duplicate launch, lost tail, service failure, BCL priority
inversion, or failure to complete five consecutive healthy intervals within
ten minutes rejects the rollout. Absence of stable positive spendable capacity
does not. Do not
increase the freshness horizon, set `provision_timeout` to 30, enable a second
planner, bypass an epoch check, or deploy the old image. A zero-tolerance
safety violation enters or remains in `SEQUENCED_MAINTENANCE`, preserving all
effect fences. Repair the exact state and deploy a newer reviewed digest with
the explicit three image overrides; `resume` only after full conservation and
cohort attestation. Reconciliation notifications are acceleration, not authority.

After binary acceptance, run the same sequenced path over the entire fleet for
a fixed 24-hour soak. The cleanup PR unblocks when that clock completes with no
safety violation, all required health intervals recorded, the executable
kube-scheduler BCL-preemption test green on the implementation head, and the
fix-forward maintenance rehearsal green. No naturally positive capacity or BCL
event is required, so the deprecated path has a finite removal gate. This is a
full-fleet validation period after atomic activation, not a canary, shadow, or
second happy path.

### Historical protocol-v2 rollout

The steps and rollback language in this historical subsection describe the
already-completed revision-035 protocol-v2 rollout. They are retained as audit
evidence and do not define a rollback edge for the current one-way
reconciliation correction. Serve041's permanent physical-protocol floor makes
the demotion command and every equivalent application-role SQL write fail even
while the new reconciliation gate is still `LEGACY_ACTIVE`; this is executable
history, not merely a documentation convention.

1. Merge revision 035, normalized claims, pool-aware runtime, the durable
   launch tuple/provider fence, and tests while
   the durable protocol gate remains v1. Protocol-v1 one-pool behavior is
   unchanged and multi-context fill remains mechanically inactive.
2. Upgrade every API/controller/executor role to the fenced image, then apply
   the reusable spoke workspace-pool RBAC module (or equivalent
   operator-managed RBAC) to every remote candidate. An already-active v2
   deployment must keep remote UID reads denied until all roles run the fenced
   image. If it already has an authorized in-cluster fill candidate, disable
   fill before the Helm upgrade because the chart RBAC and image change are
   applied together; re-enable it only after rollout verification. This avoids
   either mixed direction: a new controller with an old executor that ignores
   the tuple, or an old controller that omits it for a new executor.

   Verify the control-plane subject can get exactly the `kube-system`
   Namespace and read its nonempty UID through each configured context. Keep
   this permission in the same declarative ownership path as the rest of the
   pool RBAC. A separately named live ClusterRole and binding may serve as a
   fix-forward bridge, but must not take ownership of or patch the declarative
   system's existing roles. Remove the binding first and prove UID reads remain
   healthy through the declarative grants before removing the bridge role.
3. Complete the supported one-way API request-store cutover to PostgreSQL, run
   the migration Job, wait for its Pod to become terminal, then replace every
   API/controller/executor process. Resource
   action authority remains explicitly disabled during the 034-to-035 mixed
   rollout: old code recognizes only 034 evidence, new code recognizes only
   freshly produced 035 evidence, and neither may promote from evidence
   produced for the other schema head.
   The `blocked` cutover phase admits only the configured and resolved legacy
   SQLite runtime so it can drain; it rejects a normal PostgreSQL runtime until
   the importer atomically commits `cutover-complete`. The import Job invokes
   the importer directly and therefore does not need to start a server runtime.
   Once the shared cutover gate records `cutover-complete`, every server role
   resolves its actual request storage backend before database recovery and
   refuses startup unless it is the exact built-in PostgreSQL backend. This
   turns a later declarative or plugin-driven regression to SQLite into an
   explicit rollout outage instead of allowing stale-source reads or recovery
   against the frozen one-way source. This runtime fence does not make an
   out-of-band Helm value declarative; operators must still prevent a later
   configuration apply from reverting `requestStore.backend`.
   Enable the chart's built-in execution-quiescence backend guard as part of
   the PostgreSQL runtime rollout. The chart value is deliberately optional
   in the generated schema and renders as `false` when absent so an existing
   historical revision-035 releases upgraded with Helm `--reuse-values`
   remained backward compatible;
   the PostgreSQL cutover must nevertheless set it explicitly to `true`.
   Once the durable broker gate is v2, MAIN
   startup for the `all`, `api`, `controller`, and `executor` roles
   independently requires that preparation flag and exact built-ins, so a
   later new-code rollout cannot silently disable the guard. The proposed
   resource-action authority-worker role was retired before activation and is
   absent; reserved-fill has no separate authority-role exception to this
   protocol gate.
4. Verify healthy legacy rounds, then run the zero-argument explicit activation
   action inside an API pod. The action takes the global broker lock,
   mechanically verifies exact Serve schema head 035, exact API-request schema
   head 008, plus stable all-ready API and, in HA mode, controller and executor
   Deployment/pod cohorts at one immutable digest with a literal PostgreSQL
   request backend and literal execution-quiescence preparation flag. It also
   requires every database-wide recent process lease
   to resolve to the exact built-in PostgreSQL request storage and queue
   implementations, advertise execution-quiescence capability, and equal
   those Pods;
   wait at least the server-instance stale horizon after retiring an old or
   draining release before retrying. The action derives its proof and performs
   the one-row gate transaction. Verify the durable protocol gate reads v2 and
   contains the common digest, canonical Deployment generation/UID
   inventories, and combined pod/process inventory count/hash.
5. Let every live fill controller atomically adopt an authoritative v2 claim
   set. Verify generation/edge-count integrity, integer grants, and fresh 035
   reserved-fill evidence. The retired dedicated authority mode cannot be
   re-enabled.
6. Before materializing PHX eligibility, deploy the phase-scope hotfix to every
   API/controller/executor process and prove one immutable digest. While normal
   load-balancer sync, job-status, readiness, and broker pollers remain active,
   exercise both production failure shapes: enqueue a multi-item protocol-v2
   fill batch (at least ten decisions) and complete at least one exact-UUID
   ordinary GCP teardown attempt. Observe the GCP operation without an
   `AMBIENT_LEGACY` owner and observe each v2 admission retire after at most one
   physical proof/persist seam; a queued ambient waiter must run before a later
   batch item can barge. Then observe at least three complete 60-second broker
   intervals with advancing broker generations/refill rows, successful route
   sync, and no provider-phase timeout, route-sync 503, controller-failed
   transition, healthy-edge withdrawal, or physical-identity uncertainty.
   Every process must retain the same hotfix digest throughout.
7. Update the inference workspace from its inherited east context to an
   explicit east-plus-PHX list through the validated workspace API. Deploy the
   config-snapshot update protocol described in
   `docs/designs/serve-controller-config-refresh.md` to every API/controller
   process, then submit
   a fresh immutable `boltz-l4-fleet` version. The API must stage the
   policy-admitted controller projection under the durable service workspace;
   the controller must build that version's catalog under the staged snapshot,
   atomically commit the version/catalog and sanitized HA recovery generation,
   and only then install and reload it. An old controller fails capability
   preflight before version allocation. A restart is a verification step, not
   the mechanism that refreshes config. Confirm two claims, independent rounds,
   exact-context/UID east and PHX canaries, restart persistence, and the global
   cap.
8. Historically, the stacked cleanup PR remained blocked until the observation
   window and then-applicable rollback gate below passed.

The following is non-runnable audit history for schema 035. Serve041 and later
operators must not execute it: the permanent protocol floor mechanically rejects
it. In the historical window, rollback occurred while the v2 image still ran:
fill was disabled (or every secondary context removed), pending v2 launches
were fenced, secondary normalized claims disappeared, and the zero-argument
`python -m sky.serve.reserved_capacity_demotion` action ran inside an API Pod.
That action took the same global broker lock, required exact schema head 035 and
API-request schema head 008, and used its mounted Pod-bound token to double-read
and attest the complete stable API/controller/executor writer rollout. It
accepted no operator-supplied rollout identity.

Under that historical lock, demotion took PostgreSQL table locks that also
excluded an old v1 writer unaware of the protocol singleton. In one transaction
it inventoried every authoritative set, refused any multi-edge, malformed,
stale-generation, incomplete, or legacy-only set, rebuilt the complete legacy
row for each valid single edge, reread the projection inventory, and only then
flipped the durable gate to v1. A projection-write or final-validation failure
rolled back both the rebuild and gate. The command had to report protocol v1
before the image was rolled back. The atomic carried-protocol predicate fenced
every queued v2 persist when the gate changed; additive tables remained.

An emergency old-image rollback against an active multi-context spec historically
promised only that the old controller emitted no new multi-context fill. It
could not delete normalized claims, and zero/stale feed could continue sheltering
existing rows; it was not a supported drain procedure. At schema 035, operators
had to restore the v2 image and perform the then-applicable disable/demote
sequence. This is not a Serve041+ recovery option.

Rolling back the phase-scope hotfix while #1275's mixed legacy/v2 behavior was
active was unsupported because it restored the multi-item and non-Kubernetes
cleanup phase convoys that caused 30-second timeouts and transient controller
failure. The historical choices were to fix forward or to disable fill and
drain pending work under the hotfix before reverting. Rolling below #1275 while
legacy and v2 rows coexisted was also unsupported because it restored the
provider-call collision that could withdraw a healthy edge. The explicit
east-plus-PHX workspace superset could remain because it removed no eligible
context. Serve041+ permits only the current fix-forward path.

Revision 035 advanced resource-action activation evidence to exact revision
035. Evidence from revision 034 was invalid for new code and was not silently
re-labeled. Authority stayed disabled for the entire mixed-image rollout and
was re-enabled only from fresh 035 evidence. Rolling back the image while the
database remained at 035 likewise required authority to remain disabled,
because the old validator could not recognize current evidence.

API request revision 008 was retained-additive and intentionally had no schema
downgrade. Removing its required/proven quiescence fields could erase the fact
that a terminal request still had executing code, while removing its runtime
capability fields could invalidate the proof used to activate protocol v2.
Historical application rollback therefore left the API request schema at 008,
as the retained-additive migration policy did for earlier durable-request
kernel revisions.

The historical cleanup PR's recorded merge gates were:

- all production API, controller, and executor processes had run the new image
  for the historically documented rollback window;
- every live legacy claim had an equivalent normalized edge;
- east-only and east-plus-PHX services had completed update and restart
  canaries;
- same-accelerator cross-context launch fencing had been observed; and
- recorded operator acceptance that rollback to a pre-035 image required
  removing multi-context fill first.

These bullets are audit evidence only. They do not authorize a rollback or
demotion after Serve041.

## Verification

### Current reconciliation-correction verification

The implementation change is not mergeable until all of the following are
deterministic and pass on its exact final head:

- fake-clock tests at 179, 180, 181, and 250 seconds prove that source
  timestamps are never postdated, stale capacity is never accepted, and a
  fresh publication wakes reconciliation without waiting for the next demand
  tick;
- a 250-second autoscaler/manager operation cannot delay fixed-rate pool
  observation or publication, while a missed observer deadline coalesces and
  does not create either fixed-delay drift or a catch-up storm; the
  production-sized 200-intent/multi-service PostgreSQL stress fixture records
  broker/scheduler singleton hold time below 250 ms p99 and two seconds max
  while the full expected process cohort performs guarded and legacy-triggered
  heartbeats at production cadence;
- east and PHX observations complete out of order, a hung or blacked-out pool
  cannot block its peer, a stale lease holder cannot publish, and broker,
  Kubernetes-only demand placement, and status consume the same committed
  generation and exact-card values;
- real-PostgreSQL races cover a provider observation concurrent with a fill
  persist, a persist inside the former scan/publish gap, lease replacement,
  claim-set replacement, and broker publication failure, preserving both
  `sum(feed) <= observed_free` and exact-card conservation;
- commit-order tests allocate competing observation, ordinary-demand, and fill
  order values, force out-of-order client execution, transaction rollback, and
  host clock skew, and prove that singleton `FOR UPDATE` order plus the shared
  reservation boundary admits no row into the debit-scan-to-publish gap; a bare
  `nextval()` implementation must fail this regression;
- a demand/headroom change first publishes one complete claim generation and
  emits no fill, then a complete same-generation map wakes the bounded second
  pass; failed pools contribute explicit zero authority without suppressing a
  healthy sibling, and a superseding demand revision fences the old map. A
  same-claim-generation pool-round race changes the canonical round vector and
  input hash, so the old assembler CAS cannot publish; a persisted `FillPlan`
  with any mismatched allocation generation, input hash, claim generation, or
  round vector is rejected. An owner takeover between assembly and commit is
  rejected under the lifecycle row lock, while replay of the exact same input
  and canonical payload returns the existing generation without an update or
  second notification; a same-hash/different-payload row fails closed;
- update, disable, owner rotation, service recreation, pool-map reorder, UID
  retarget, and epoch change at every capture/publication/commit boundary reject
  stale work; specifically, an old poller cannot re-heartbeat a removed claim;
- the final persist accepts at the transaction-time freshness boundary and
  rejects immediately after it even when service generation and pool epoch are
  unchanged;
- notification-before-wait, notification-during-reconcile, duplicate,
  out-of-order, and coalesced generations have no lost wakeup. Dropping the
  PostgreSQL LISTEN connection and every process-local notify still discovers
  the durable generation on the five-second maximum-idle reread and remains
  within the 15-second publication-to-accept SLO;
- a 49-intent plan with only seven accepted rows debits exactly seven, returns
  42 typed deferrals, and can accept the tail immediately without two new
  capacity polls; partial transaction failure after `K` commits, and a lost
  commit reply, produce neither lost feed nor duplicate rows;
- the full legal/illegal intent-state transition table and stale state-version/
  lease fences are tested, with crash injection before and after acceptance,
  ticket selection, request binding/activation, Kubernetes object creation, UID
  persistence, capacity wait, bind, and readiness. Ambiguous `SUBMITTING`
  enters `EFFECT_RECOVERY_REQUIRED`; discovery/adoption proves at most one provider
  start and never resurrects a stale intent. After any `SUBMITTING` transition,
  even terminal-request/provider-absence proof permits only `TERMINAL` plus a
  fresh intent, never re-leasing or rebinding the old row. Loss of reads for an
  exact durable object enters `OBSERVATION_UNKNOWN` with no debt and returns
  only to its immutable resume state after a UID-fenced read;
- every accepted row receives its durable ordering ticket in the acceptance
  transaction. Request binding locks and validates the controller's exact boot
  lease/ack; executor claim locks and validates the executor boot lease/ack.
  Queue saturation and executor crash before claim leave a still-PENDING
  no-effect request that loses live prospective authority exactly at its
  five-second database deadline even with the sweeper paused. Claim after a
  lock wait resamples database time and loses; cleanup terminalizes within the
  separate 15-second SLO. Claim/sweep/lock-contention races have exactly one
  effect-capable winner, and no API worker or mutation permit waits behind the
  queue;
- activation persists `C` as the minimum positive complete-fleet
  `_get_request_parallelism(pool=False)` safe-maximum attestation. It permits
  different valid local maxima, persists their input hashes, and rejects
  missing/nonpositive, payload-hash-inconsistent, wrong-`pool`, below-stored-C,
  or mixed-generation values. Processes with different environment/resource
  inputs still enforce the one stored minimum. A `pool=True` request cannot
  consume or mutate the non-pool singleton. Limit-change
  crash tests prove no generation changes before active/live-prospective/debt
  drain and complete acknowledgement; timeout resumes the old limit, while a
  successful change fences stale tickets. A lower-maximum replacement remains
  STARTING, uses only the recovery lease to resolve an inherited ambiguous
  ticket after old-worker quiescence, and joins solely in the combined drained
  inventory/limit transaction; it cannot mint work at either limit;
- ordinary Serve create/cancel/cleanup and fill binding/claim race before and
  after every scheduler, intent, and API-request CAS boundary. Cleanup then
  ordinary demand then fill priority is deterministic; an ordinary arrival may
  atomically reclaim only a still-PENDING fill prospective lane. Direct generic
  queue claim, more than `C` combined effects, debt erasure, and preemption of
  `SUBMITTING` are all rejected. Kubernetes and at least one ordinary
  non-Kubernetes provider run definitive-create, delete-accepted, definitive-
  no-effect, timeout, worker-loss, lookup/adoption, and exact cancellation
  receipt cases through the same typed executor. A direct provider call without
  its permit fails. Legacy `SafeThread`/`down_thread_pool` cleanup is quiesced at
  promotion, rejected while sequenced, and survives crash injection before and
  after durable cleanup binding/claim/receipt only through the API request
  recovery path. With every generic long worker busy, a provider request stays
  PENDING and never enters `queue.get()` or the guaranteed ProcessPool backlog;
  losing an idle-worker reservation releases it, while winning claim plus
  handoff crash preserves the exact active/debt ticket. When one worker becomes
  idle, a fresh eligible request claims it without a generic-queue bypass;
- deadline and process-crash injection converts ambiguous mutation tickets into
  concurrency debt. Active mutations plus live prospective lanes plus debt
  never exceed the scheduler singleton's persisted `C`; if debt reaches `C`, no
  new request is bound.
  At the 15-second database boundary an unreceipted active ticket becomes debt
  without waiting for its sweep and is never double-counted; a late normal
  result is rejected and may release debt only as exact quiesced recovery
  evidence.
  Exact worker quiescence plus object adoption or terminal-request/provider-
  absence proof releases the corresponding debt, while lease expiry and
  heartbeat loss do not. A blackout affecting any number of already-known
  waiting/initializing objects creates observation-unknown rows but zero debt,
  so it cannot consume healthy pools' mutation capacity;
- with a continuously fresh 120/80 east/PHX budget and sufficient capacity, the
  first eligible provider wave includes both pools and eventual successful
  submissions are exactly 120/80. For a 200-replica target, the accepted
  no-effect prefix never exceeds the dispatch-horizon formula; at `C = 8`, zero
  debt, a 20-second claim-plus-receipt wave, 30-second margin, and a fresh
  180-second horizon, the initial prefix is at most 56 rather than 200. Expired no-effect rows become
  `TERMINAL`, fresh observations replan with new intent identities, and no
  newer epoch reauthorizes an old row. A horizon test begins with all `C`
  mutation tickets active and proves they are included in `outstanding_work`;
  all accepted and live request-bound tickets in every lane are counted, as is
  every eligible higher-priority pending mutation request ahead of fill;
  their
  concurrent completion may reduce later admission but can never make the
  committed prefix exceed the deadline bound. A finite higher-priority backlog
  reduces the fill prefix by its charged waves and a continuously arriving
  backlog defers new fill acceptance. A stalled dispatcher cannot renew or
  create a pre-request lease because no dispatch-lease state exists. With no
  higher-priority ordinary work, the first eight submissions are 4/4;
  every east request may remain in `WAITING_CAPACITY` indefinitely while PHX
  keeps receiving later permits, and no waiting row holds an API long worker;
- two services sharing one pool, including a new traffic-bearing service with
  weight 1000, receive only broker-authorized weighted capacity, retain the
  global `max_replicas` constraints, and cannot starve an eligible peer. The
  dispatcher has no weight input, hierarchical pool/service cursors prevent
  service-count multiplication, and more than `C` pools meet the finite
  submission-wave bound whenever `C - debt` is positive;
- readiness, request-status, UID, and provider probes racing with replica
  replacement or deletion merge only into the exact snapshotted row and worker
  generation, without holding the manager lock during I/O;
- BCL consumption between observation, acceptance, and provider dispatch
  cannot produce paid spill or stale acceptance; higher-priority BCL preempts
  or occupies the slot, unstarted excess is withdrawn, and subsequent free
  capacity re-enters fill normally. In
  `tests/kubernetes/test_serve_reserved_fill_preemption.py`, a dedicated
  Kind/test cluster runs the real kube-scheduler with the actual rendered
  SkyPilot PriorityClass, numeric priority, and `PreemptLowerPriority` pod
  fields. A constrained test extended resource may replace a physical GPU:
  first a low-priority fill pod runs; then a priority-0 BCL pod preempts that
  exact fill UID; while BCL runs another `provision_timeout: -1` fill pod stays
  Pending, cannot preempt BCL, and creates no paid/cloud request; after BCL is
  deleted, the pending or freshly reconciled fill pod schedules. The test
  asserts exact pod UIDs plus scheduler nomination/preemption events and is not
  a production canary; and
- status distinguishes observed, spendable, accepted, queued, provider-running,
  scheduled, and ready values, including a regression fixture that displays 34
  raw slots and zero spendable slots for a stale observation rather than
  reporting 34 launchable slots. Old-client/new-server and new-client/old-server
  API-version tests preserve legacy display compatibility without letting a
  missing new field authorize control behavior;
- the named `status`/`activate`/`begin-maintenance`/`resume`/`verify`
  module is the sole gate entrypoint and rejects service subsets,
  caller-supplied inventory/digest, and adjustable acceptance/soak durations.
  Verifier crash/restart retains the database start/deadline. Exact sample/run
  tests pin the nonempty configured/claimed pool set, require a strictly newer
  successful observation for every descriptor in every accepted interval,
  accept explicit all-zero coverage plus latency `N/A`, and reject missing,
  blackout/error, changed UID, reused generation, stale payload, set change,
  maintenance entry, inventory change, or sample gap. Only one run can become
  `PASSED` for one active inventory generation;
- initial promotion rejects any old process lease, missing capability/schema,
  custom request backend, unsequenced writer, ambiguous provider effect, `all`
  process, or acknowledgement timeout. No sequenced effect occurs before the
  atomic flip. Old-statement-first and gate-first races cover registration,
  `ON CONFLICT` heartbeat, request transition, and delete; each legacy commit is
  wholly before preparation or rejected. The bounded request-table barrier
  drains a pre-preparing legacy provider statement or times out without
  flipping. Forward backfill covers every nonterminal ordinary/fill zero-cost
  row, rejects duplicate/unresolved identity, invalidates legacy observations,
  and resumes its exact cursor after every injected crash. From the first
  PREPARING commit, every attempted legacy edge and previous-image startup is
  rejected and only forward resume succeeds. A crash immediately after that
  commit but before the
  writer barrier/first projection, with the current command and ACK handler
  broken, is recovered by the target runner: it journals the target, rolls the
  cohort, and continues the same activation cursor;
- pre-schema authority-reset tests provision A and an exact external
  infrastructure/provider fixture, then run
  `PLANNED -> INGRESS_CLOSED -> OLD_EGRESS_CLOSED ->`
  `CUTOFF_PREPARED -> CREDENTIAL_REVOCATION_STARTED ->`
  `CREDENTIAL_ISSUERS_REVOKED ->`
  `OLD_SUBSTRATE_TERMINAL ->`
  `EFFECT_FRONTIER_CLOSED -> RESET_RECEIPT_FINALIZED`; only later does
  `ANCHOR_PREPARE/-40` import it and commit the database baseline. A historical process row
  whose Pod was deleted but whose exact EC2/container runtime still runs is
  blocked; provider-native terminal proof plus revoked issuer/egress evidence
  retires it without claiming a Pod death time. Secret-only rotation fails.
  Cached AWS STS, GCP OAuth, Kubernetes bound and legacy tokens, kubeconfig
  cert/exec identities, and vendor keys all fail after their exact provider-
  native revocation, while a process paused after its last durable read cannot
  make a provider call. An actor, host, credential, issuer, or Helm-owned
  identity created between either inventory read restarts planning under a new
  generation; two concurrent operators race every DynamoDB state transaction
  and the stale one performs no external action. Tests lose the response before/
  after `CUTOFF_PREPARED`, before/after the A marker/`NOLOGIN` transaction, and
  before/after the external state advance; exact marker GET distinguishes
  replay from commit without claiming cross-system atomicity. They then kill
  after each provider action; the state remains
  `CREDENTIAL_REVOCATION_STARTED`, runtime teardown is rejected, and retry
  reuses the same IDs until all receipts and cached-credential denial probes
  permit `CREDENTIAL_ISSUERS_REVOKED`. Unknown plugins, credential sources, shared/unproved
  hosts, optional workers, or remote controllers reject preflight before the
  irreversible edge. The old ReplicaSet cannot schedule on epoch-tainted new
  nodes. Crashes before/after every egress, credential, runtime, frontier,
  immutable-receipt, anchor-CAS, post-reset capability-probe, and database-
  import boundary resume the same generation/request IDs. Post-reset/pre-anchor
  role/catalog/Secret drift invalidates probe/anchor preparation; after the
  A-side cutoff no test restores an old identity.

  A provider call accepted immediately before the cutoff is imported by exact
  audit/operation identity as effect, no-effect, or ambiguous debt. Ambiguous
  debt blocks activation and conservatively consumes capacity/concurrency until
  the ordinary typed recovery resolves it, without duplicate calls. Pod/lease/
  database absence, age, request terminal state, and fixed deadlines never
  classify safety. Receipt tamper, missing actor/host/credential inventory,
  post-cutoff old-principal use, a nonterminal exact runtime, or missing audit
  coverage leaves a finite typed blocked cursor. Tests build the immutable
  reset receipt, bind its exact identity/content/retention hash into the later
  anchor, import every pre-cutoff row as `AUTHORITY_RESET_RETIRED`, and prove
  that recorder responsibility starts only at the post-reset baseline. No GPU,
  BCL, provider-capacity workload, service canary, or shadow planner is used.

  The source fixture matches current ownership reality: the legacy application
  role owns historical Alembic objects. `ANCHOR_PREPARE/-40` verifies the reset,
  materializes identical explicit privileges, transfers every owner to the
  pinned nonlogin owner, rewrites default privileges, and installs the guard
  directly as `ENFORCED` atomically. Every lockable relation and complete
  dependent-object/ACL catalog entry is covered; partial transfer, unexpected
  owner/grant, lock timeout, or reset mismatch leaves the source-to-target
  transaction unchanged while the external reset stays closed for retry. The
  actual prior binaries fail under revoked legacy credentials and, even if
  deliberately given the sequenced Secret, lack a guard token; provider-adapter
  network calls remain zero. A process retaining an A effect while reporting
  through clone B cannot satisfy the reset/enforcement receipt or `-20`.

  The live Rainier Aurora 16.13 gate runs the checked-in transactional
  capability probe and post-rollback residue check. For either application
  role, role recreation,
  OID/owner change, superuser or `BYPASSRLS`, inheritance or recursive
  membership/`SET ROLE` to any owner/bypass role, database/schema/column/default
  grant, database `CREATE`/`TEMP`, schema `CREATE`, role/database search-path
  drift, forced-RLS removal, function-owner/ACL/search-path mutation, and an
  authentication completing during the first zero-session sample all block.
  The administrator must match the anchored Aurora master/`rds_superuser`
  capability graph and live capability receipt; `rolsuper=true`, loss or change
  of managed membership, another customer-login authority path, or capability
  drift blocks. Every event trigger must retain its anchored owner and `ENABLE
  ALWAYS` mode, and both application roles remain nonowners. Mutation after a successful hook but before an
  ordinary read/effect is caught by that transaction's live recursive closure
  check and produces zero provider calls;
- phase-0 bootstrap/chart tests prove the operator pre-creates the isolated
  guard namespace/admin Secret and one empty enforcement receipt; for a
  nonempty source it then executes the complete reset, creates the immutable
  reset receipt, and only afterward creates the anchor binding both objects.
  `EMPTY_BOOTSTRAP` forbids a reset receipt. Helm cannot generate or reconcile
  any of those operator-owned objects.
  `ANCHOR_SCAFFOLD` creates ordinary Helm-managed identities/RBAC and a dormant
  recorder with no hook, database Secret/provider projection, or authority-
  Deployment change. The test begins with the real old manifest and reset-
  deleted ServiceAccount/bindings, proves adoption emits exact
  `EXTERNALLY_DELETED_BEFORE_ADOPTION` entries, and proves the first real Helm
  diff accepts only those missing objects as journaled no-ops. Supervised decommission
  owns deletion of all ordinary managed resources; the cleanup chart deletes
  only the transitional set/recorder and demonstrably retains the permanent
  finalizer/transition-runner/guarded-migration set. `ANCHOR_PREPARE` uses
  those existing identities for exact `pre-upgrade/-50` validation before the
  `pre-upgrade/-40` guard-migration Job and cannot update a Deployment until
  that Job's database receipt commits. It wakes the fixed read-only recorder and does not complete until the
  distinct `post-upgrade/0` recorder-ready Job proves a gap-free `WATCH_ARMED`
  cursor from the captured post-reset list resourceVersion. Scaffold/prepare
  use generic Helm Wait=false and their signed postcondition programs ignore
  the intentionally zero-Ready old authority Deployments. Phase-0 Jobs and the
  recorder carry the successor toleration/selector/transition ServiceAccount
  and demonstrably schedule on the epoch-isolated substrate; PREPARE projects
  the sequenced Secret only after `-40` commits `ENFORCED`. Every pre-cutoff
  process row is imported as `AUTHORITY_RESET_RETIRED` from exact immutable
  runtime/credential/effect evidence; the recorder inventory begins with only
  post-reset boots plus the current Pod list. A post-baseline stale row whose
  Pod disappears still blocks; a process cannot dispatch provider work before
  registering that row. Recorder restart resumes its
  durable cursor; watch compaction, a missed event, or deletion without an exact
  preceding kubelet-reported container termination first blocks promotion and
  then runs the complete `POST_RESET_REBASE -> ANCHOR_REBASE_PREPARE ->
  ANCHOR_REBASE_COMPATIBLE` recovery. Crash injection at every journal,
  runtime-termination, receipt-import, new-baseline, and compatible-cohort
  boundary converges on one higher epoch with no old effect; unresolved debt
  stays blocked. Rendering/runtime
  below Kubernetes 1.27 blocks. A fixture deletes a post-reset compatible Pod
  before the later enforcement hook and proves the retained container-ID/
  restart-count/termination row--not Pod absence--is the evidence.
  The compatible revision revalidates that barrier before changing a
  Deployment and leaves the recorder live through final receipt acknowledgement.
  The migrator and `post-upgrade/0` `enforce-anchor` Job run in the dedicated
  guard namespace with distinct owner-verified identities, Pod list permission
  only there, stable namespace-scoped Job GET with no list/watch/write and
  code-level exact release-instance/revision-bound name/UID/owner/image checks,
  release-namespace read-only cohort/two-application-Secret
  access, same-namespace anchor/receipt access and admin Secret mount, and
  cluster-scoped exact-name Namespace GET. Only the
  enforcement Job has `get/update` on the exact receipt ConfigMap; neither has
  create/delete/workload/provider permission. API, controller, executor, and
  later transition-runner identities cannot write it. A successful Job follows
  its fixed hook-deletion policy; the feature chart retains the inert
  transitional phase-0 identities/recorder until the soak-gated cleanup chart
  removes that set. Permanent finalizer/transition/migration identities and the
  operator-owned receipt remain, and all runner-created retained per-revision
  receipts remain unchanged through retry and cleanup; none is assumed to be
  Helm-deleted.
  The recorder and recorder-ready Job have separate fixed identities: the
  former has only release-topology get/list/watch and append-only guarded
  database evidence, and the latter has exact own-Job/topology/anchor reads but
  no provider, receipt-write, or workload verb. A failure before
  `ANCHOR_PREPARE`'s mutating `-40` transaction leaves the source ownership
  graph unchanged but the reset irreversibly cold and resumable; compatible
  `-40` is read-only. After preparation, recorder loss blocks post-baseline
  retirement evidence but cannot restore old egress, credentials, substrate,
  login, or process identities. A pre-fence provider call is exact effect or
  durable debt and prevents enforcement until resolved.
  The canonical payload contains no digest of itself. Tests independently
  compute the RFC-8785 payload digest and canonical immutable object-content
  digest (excluding resourceVersion/unlisted metadata but including every empty
  garbage-collection/ownership field), inject loss of the CAS
  response, GET the same UID/bytes, and accept only a monotonic PostgreSQL
  acknowledgement of both digests and the returned final resourceVersion. Crashes
  before/after CAS, GET, and acknowledgement converge on the same three-part
  chain. Any ownerReference, finalizer, deletion timestamp, Helm ownership/
  resource-policy/hook annotation, mutable object, self-referential/incorrect
  digest, conflicting content, UID recreation, or forged/stale acknowledgement
  blocks. Deleting the successful hook Job and supervised decommission of the release leave
  the same enforcement-receipt UID/content GETtable;
- fresh-anchor tests start with `BOOTSTRAP_SCAFFOLD`, an exactly empty
  PostgreSQL database, external anchor, and empty receipt with no source
  Deployment, Pod, session, or effect. The scaffold creates the permanent
  managed identities/RBAC and immutable authority bundle, whose source tuple/
  hash `-30` compares with a fresh anchor GET, but no authority Deployment,
  hook, or database mutation. The anchored
  `EMPTY_BOOTSTRAP` source with an empty mutable receipt renders the
  writer-capable fixed `-25` `anchor-evidence-finalize` Job; an
  `EXISTING_CURRENT_HEAD` source or already-immutable receipt renders no `-25`.
  The finalizer proves its exact Job/namespace/release identity and requires the
  legacy role already `NOLOGIN`. It creates the stable guard directly
  as `ENFORCED` and completes the database-receipt -> immutable ConfigMap ->
  final-resourceVersion acknowledgement chain before `-20`; a lost update
  response is recovered by GET exactly as in phase 0. For a nonempty source,
  `-30/-20` only verify an already-enforced, complete chain; missing, mutable,
  empty, recreated, or mismatched receipt evidence blocks. A second upgrade
  after fresh bootstrap sees the same `EMPTY_BOOTSTRAP` anchor but immutable
  acknowledged receipt, omits `-25`, and succeeds through `-30/-20/-10/-5`.
  Cleanup retains this exact fresh-install path and never needs the removed
  nonempty `enforce-anchor` implementation;
- current-head baseline tests run the actual empty PostgreSQL bootstrap across
  all nine central PostgreSQL lineages, including formerly lazy `kv_cache_db`
  and `recipes_db`, through `CurrentHeadPostgresBaseline` in one
  transaction, with no historical Alembic revision, autocommit block, or
  `CREATE INDEX CONCURRENTLY`. All heads, catalogs, protections, and grants
  appear atomically. A separately built database that replays the complete
  historical migration chain normalizes to the same current definitions/ACLs.
  Removing one lineage/object from the baseline, failing a statement, or
  exposing an intermediate grant rolls back the baseline and leaves effects
  closed. A nonempty database one revision behind any pinned pre-feature head
  is rejected by anchor preparation rather than upgraded under the guard. In
  server PostgreSQL mode, first runtime use of cache/recipes is verify-only and
  executes zero DDL;
- guarded-schema-migration tests prove Serve041, API009, a later additive
  revision, and cleanup all use the same `GuardedSchemaMigrator`. Direct DDL by
  either application role, by the administrator without the exact one-use
  database token, with a stale Job UID/revision/target, or while DDL is `OPEN`
  fails through the event triggers. A valid token with an undeclared command,
  wrong `ddl_command_end` result, unconsumed expected operation, missing
  catalog/protection step, or omitted generation seal fails the deferred
  commit-time trigger and rolls back. Event-trigger mutation itself is not an
  ordinary allowed command and any trigger owner/OID/definition/mode drift
  blocks. The approved `-20/-10/-5` path closes
  effects, binds the exact migration identity, locks the complete prior
  lockable manifest and complete protection catalog, creates every object with
  no application/PUBLIC grant, installs its type-specific table/view/sequence/
  function/type/index/constraint protection, and only then atomically extends
  the catalogs and grants access. Tests enumerate effective column grants,
  database/schema ACLs, view definitions, sequences, functions/procedures,
  types/domains, indexes/constraints, default privileges, and prove excluded
  guard evidence tables have zero direct application privilege. Crash/rollback
  injection before every DDL, protection, manifest, grant, generation, and
  reopen boundary leaves either the complete old generation or complete new
  generation; no unmanifested application-readable object is observable.
  Alter/drop paths revoke access first, nontransactional DDL is rejected, and
  `-5` cannot reopen for a cohort that does not echo the exact committed DDL
  generation;
- Serve041 migration requires stored physical broker protocol v2 and installs
  the permanent protocol-2 `CHECK` floor before the reconciliation gate. The
  historical demotion command, its old binary, ordinary application-role SQL,
  and concurrent activate-versus-demote transactions all fail to store v1 in
  both lock orders and leave the singleton/gate unchanged. Cleanup retains the
  constraint and its old-binary regression;
- rendered-chart tests run split-role PostgreSQL HA with at least two API,
  controller, and executor replicas, `maxSurge=1,maxUnavailable=0`, API
  `minReadySeconds: 10`, controller/executor zero, and 120-second grace.
  It rejects Kubernetes <1.27, every enabled image-copy/lifecycle/canary worker,
  an extra central-DB writer/provider-effect Deployment, or incomplete prior
  worker termination evidence before phase-0 mutation. It asserts the
  artifact-pinned inert scaffold ordinary-resource lifecycle renders no hook or
  database Job, then `pre-upgrade/-50`,
  `pre-upgrade/-40`, the managed recorder, and the applicable `post-upgrade/0`
  recorder-ready/enforcement Jobs; distinct fixed ServiceAccounts; stable
  guard-namespace `batch/jobs` GET without list/watch/write plus exact
  revision-name/UID/owner/image checks; bounded guard-namespace Pod list for Job
  owner proof; release-topology list/read where required; exact-name
  `resourceNames` GET and no list/watch for Namespace objects; and no Kubernetes
  Node read because evidence records node name, not UID. It also asserts the
  permanent runner has exact-name GET and no list/write on every scaffold
  ServiceAccount/Role/RoleBinding/ClusterRole/ClusterRoleBinding and that `-30`
  rejects any UID/rule/subject/hash drift against the canonical target-payload
  ABI manifest. Adding/removing a verb or subject, recreating an object between
  its stable double reads, or changing UID/spec after the database target claim
  blocks; an RBAC ABI change requires a preceding hookless scaffold revision.
  No scaffold receipt exists. The admin Secret exists
  only in the operator-owned guard namespace. Prerequisite, recorder,
  recorder-ready, target-runner, and authority identities have zero Secret verb
  there; guard-migrator, enforcement, finalizer, and migration identities each
  have exact-name GET plus same-namespace mount and validate UID/resourceVersion/
  content before mutation. Tests give the API its real broad
  release-namespace Secret/Pod Role and prove it still cannot read the Secret,
  create a Pod, or obtain a RoleBinding in the guard namespace. Every authority
  Deployment sets automatic token mounting false. The
  hookless scaffold renders an immutable same-namespace
  `DatabaseAuthorityBundle` from the signed exact anchor read-set; there is no anchor-reader
  init container, token, `emptyDir`, or authority guard RoleBinding. All MAIN
  containers mount that bundle read-only and startup/every checkout reject a
  missing, mutable, tampered, recreated-with-different-inner-anchor, or
  PostgreSQL-mismatched bundle. A fresh phase-0 prerequisite compares it to a
  direct anchor GET. Authorization tests impersonate prerequisite,
  recorder-ready, enforcement, finalizer, runner, and migration ServiceAccounts:
  their separate release-namespace RoleBindings permit only exact-name bundle
  GET and deny list/watch, a sibling ConfigMap, and every write. They prove no
  ConfigMap ClusterRole/wildcard binding exists and that scaffold object reads
  are granted by separate exact-name Roles in each object's namespace. Every
  phase fresh-GETs the bundle; missing RBAC, wrong token audience,
  missing/mutable/deleting/tampered data, changed inner anchor identity, or
  PostgreSQL mismatch blocks before mutation. Mutation between every adjacent
  hook pair is caught by the next phase. Byte-identical outer recreation is
  accepted only with matching direct anchor and database guard. The ordinary
  recorder Deployment is in the release namespace, mounts only the sequenced
  Secret and bundle read-only, and uses no guard/admin/provider credential or
  Kubernetes Secret/ConfigMap API verb. Tests prove startup/every checkout use
  anchored A and that bundle/Secret/database drift stops its heartbeat and
  append activity, preventing any later retirement proof or promotion from
  using that interval. The
  migrator has no provider/workload verb or bundle read, and a
  failed guard/barrier leaves authority
  Deployment templates unapplied. Every authority Deployment disables automatic
  token mounting and uses a distinct role ServiceAccount. API MAIN always has
  only the explicit owner-chain token at its dedicated path when both local-
  provider flags are false; controller/executor then have no Kubernetes token.
  A four-case render/runtime matrix for
  `kubernetesCredentials.useApiServerCluster` and
  `serve.externalLoadBalancer.enabled` proves that each enabled mode projects
  the rotating provider token/CA/namespace only into the roles named by the
  static role-capability table, exports exactly the three provider-path variables,
  and that the adaptor rereads rotated bounded regular files. Tests use the
  real kubelet AtomicWriter symlink layout and rotation with the default service-
  account directory absent. Actual local `kubectl`, exec, port-forward, and
  rsync helpers receive only the private tmpfs mode-0600 kubeconfig whose
  `tokenFile` points at the projected token; token bytes appear in no argv,
  environment, rendered kubeconfig, or log, and the capture disappears only
  after every child joins. Audience fixtures give east and PHX their independently attested accepted
  values, bind each through root/intent/envelope/projection/runtime, and reject
  a missing value, a cross-cluster swap, or mismatched JWT `aud`. Repository-
  wide tests route non-authority in-cluster callers through explicit standard-
  path `ProjectedCredentialFiles` and fail if a helper directly reads those
  paths or ambient kubeconfig. The external-LB-
  only case (`useApiServerCluster=false`, external LB enabled) runs the real
  availability/namespace helpers, RBAC preflight, and create/reconcile/delete
  lifecycle before and after token rotation; the both-enabled case proves one
  shared provider projection, and the both-false case proves no provider
  identity is observable. Separate disable tests crash before/after every exact
  `EXTERNAL_LB_CLEANUP` object delete and absence-receipt commit; revision one
  retains only controller cleanup projection/RBAC, and revision two removes
  both only after the complete receipt. Direct true-to-false removal and
  name-only absence fail. A missing
  projection, wrong audience/path, stale cached token, capability/projection
  mismatch, or remote-kubeconfig-to-local fallback blocks. Impersonation tests
  prove every role-specific provider identity is denied Helm-history and Helm-
  owned-object writes, supervisor/guard objects, release-writer identities and
  token minting, RoleBinding mutation, transition state, and every verb outside
  its enabled capability projection. No rendered Pod can obtain the external
  supervisor credential. Feature rendering also asserts the closed hook
  matrix above: `-30/-20/-10/-5` normally and conditional `-25` on every
  empty-bootstrap attempt while its receipt remains both empty and mutable (including a
  newer-revision pre-CAS retry), with distinct
  nonconfigurable finalizer, runner, and migration
  ServiceAccounts. It proves every hook Job carries identical canonical target
  bytes/hash in the fixed Pod-template annotation/downward-API volume, no target
  payload ConfigMap is rendered, an override or payload above 128 KiB is
  rejected, and a mounted/template/hash mismatch blocks. Each has only stable guard-namespace Job GET and code accepts
  only its own exact Job identity; guard-namespace Jobs may list only
  guard-namespace Pods to prove generated owner chains. Only the conditional
  empty-source finalizer has exact-name update on the enforcement receipt; no
  nonempty validator is rendered. Mutable-nonempty and immutable-empty receipt
  shapes fail rendering or `-30`; neither may omit `-25` and continue. The
  transition runner has ConfigMap create but no
  list/watch/update/patch/delete, and the migration Job has no Kubernetes write.
  Before Serve041, runner and migration GETs are exercised against only the
  deterministic database-registered evidence name; Serve041+ phases use only
  the PostgreSQL journal and perform no revision-evidence ConfigMap create or
  GET. All have only the exact
  topology/namespace/anchor/Secret reads described above, an immutable
  complete target payload/hash, exact target-image use, and no provider
  secret/env/volume inheritance. Initial legacy and empty-database
  installs prove the pre-migration ABI without new-schema imports; active,
  maintenance, and both pre-/post-mutation preparation upgrades prove effect
  fencing precedes migration. Crash/failure at every hook boundary changes no later
  resource and is resumable by a newer image. Pre-041 tests prove the immutable
  Kubernetes receipt is created immutable in one request and imports exactly
  once across crashes. They inject a lost create response, GET and acknowledge
  the same UID/bytes/resourceVersion, reject an unregistered name-squatting
  object, conflicting retry/tampering/concurrent revision, and prove an extra
  create-only ConfigMap can cause at most fail-closed denial and never becomes
  authority. The object has no ownerReferences or Helm hook annotations;
  deleting every hook Job and supervised decommission leaves the same UID and
  bytes GETtable. The exact registry acknowledgement binds `-20` to `-10`.
  A missing, generated, or target-derived authority anchor and a missing or
  mismatched upgrade enforcement receipt block before `-20`. Target Secret
  name, UID, resourceVersion, URI endpoint, database, schema,
  direct host address, TLS/server identity, and installation-UUID changes fail
  before `-20` can change the gate or lease. Empty/populated/restored clone B
  cannot substitute for live A: all hooks connect through the externally
  provisioned A anchor, while an upgrade fixture must carry the exact
  full-fleet enforcement receipt. Mutation of the Secret between any two hooks invalidates the
  target revision. Unchanged DNS redirected to B fails the pinned
  hostaddr/server check; an unreachable A blocks rather than adopting B.
  With both local-provider flags false, API still mounts its owner-chain
  projection and performs exact owner-chain reads; missing token/audience/Role/
  binding blocks. Retained 60-second values fail. A fixture
  with independent old controller/executor image overrides proves that changing
  only `apiService.image` leaves an incapable cohort, while explicitly setting
  all three image values produces the intended immutable digest. An `all`
  Deployment/recent lease and disabled split-role HA always block activation;
- release-instance tests prove `release_instance_id` remains stable across Helm
  revisions, exact proof-object names hash that identity plus the revision, and
  every hook rejects a changed cluster UID, release namespace name/UID, Helm
  release name, guard namespace name/UID, or anchor field. Supervised
  decommission followed by a new install requires a newly generated
  anchor/release instance and cannot adopt prior proof objects. Rendering and
  runtime contain no inferred or referenced `.Release.UID`;
- runtime tests prove API Kubernetes readiness terminates in MAIN, Uvicorn
  workers only report listener/worker health through boot-fenced local IPC,
  controller/executor probes use their MAIN lease, and a wrong boot token cannot
  use the mode-0600 retirement socket. For every role, missing/mutable/tampered
  bundle, wrong source anchor UID/resourceVersion/hash, wrong
  `release_instance_id`, or bundle/database mismatch prevents MAIN startup.
  Recreating a byte-identical outer bundle is non-authoritative and succeeds
  only when its inner source identity/hash still matches PostgreSQL; every
  checkout revalidates it, and no authority Pod has a guard token/RoleBinding.
  Every shutdown joins the maintenance
  coordinator and effect workers, releases controller leadership, then stops
  the instance lease last. Registration, heartbeat, leadership, provider
  effect, coordinator lease acquire/renew, and journal commits all reject a
  permanently retired Pod UID regardless of claimed role or fresh boot nonce.
  They also reject a boot or target whose complete central-database identity
  differs from the singleton/anchor. Every pool checkout validates the direct
  endpoint and tags its boot/session generation. A maintenance ACK proves
  process-wide connection drain/rebind; injected A-effect/B-heartbeat mixed
  sessions cannot pass.
  Concurrent API command, API coordinator, and runner calls use one transition
  lease in canonical order; forced runner takeover closes effects and advances
  the lease generation atomically, stale renew/publish fails, expiry recovers,
  and no caller holds a SQL row lock while waiting;
- `tests/kubernetes/test_server_role_maintenance_handoff.py` uses real
  `N >= 3` API/controller/executor Deployments. `begin-maintenance` first stops
  every new effect, drains carried work, and releases the exact controller
  generation. Kubernetes then rolls all roles concurrently; new boots may
  become locally Ready but remain effect-ineligible, and the provider-effect
  counter cannot advance. Real ReplicaSet victim order, more than one
  terminating Pod, a queued successor, simultaneous node loss, same-Pod fresh
  nonce, and several replacement Pods require no pair classification and all
  converge through the same complete-cohort journal. The same-Pod case proves
  old/new nonce overlap blocks, then source-A session/advisory/effect absence
  atomically writes `PROCESS_RETIRED` without revoking the desired Pod UID;
- that test injects coordinator, controller leader, old Pod, and new Pod crashes
  before/after every maintenance acknowledgement, Helm target revision,
  quiescence/debt receipt, Pod-UID revocation, inventory hash, and final commit.
  A coordinator whose API Pod leaves the target normally relinquishes its
  lease; it cannot revoke itself, while another API takes the next generation.
  A separate case keeps that Pod heartbeating but breaks both its CLI and
  maintenance-ACK loop. Its normal heartbeat must echo the fresh database
  source-A identity; after the fixed 30-second ACK period the target runner proves
  every carried effect/debt identity, generation-fences leadership, permanently
  revokes the live Pod UID on A, and allows rollout. An injected
  unrecorded/ambiguous effect instead keeps the hook blocked.
  Force-deleted ghosts reconnect before and after detailed evidence GC and a
  fresh boot nonce under the same UID is rejected permanently. Lingering
  `Terminating`/API-absent ghosts with an A session or pre-gate transaction
  block; they are allowed only after source-A singleton serialization,
  effect/debt resolution, and permanent UID revocation;
- a deliberately incapable image stalls in maintenance without a provider
  effect or service canary. The target-image runner has no provider credentials
  or authority membership and cannot increment the provider-effect counter.
  Tests make the current API command unavailable, then prove the runner enters
  or resumes the same journal and Helm applies a newer reviewed image to all
  three roles. A broken runner mutates no Deployment; a subsequent newer
  runner resumes without changing generation. A failed migration or
  post-migration attestation likewise stays fenced; fixed hook ordering is
  preserved on retry. Attempts using `--atomic`, `helm rollback`, an old
  chart without the hooks, or the prior image cannot regain effect authority
  and must be superseded by a newer target. The stable capable cohort resumes.
  Separate
  cases change desired counts, strategy/service account, and recreate a
  Deployment for a selector/role-wiring change while quiesced. `resume` accepts
  only the exact rendered cohort. Direct active-state Helm, `kubectl` drift, or
  `--no-hooks` cannot be adopted and enters fail-closed membership maintenance.
  There is no deactivation command, `PREPARING_LEGACY` state, reverse
  projection, previous-digest restore exception, or old-image success case;
- `tests/kubernetes/test_serve_helm_fix_forward.py` runs the real pinned Helm
  SDK worker, semantic mutation gateway, custom CAS Secret driver, and an
  isolated strongly-consistent test ledger through the external supervisor.
  Its build gate runs `go test ./...` in `sky/release_supervisor/helmworker`,
  regenerates both protobuf languages and rejects drift, verifies the pinned
  module graph, and compares the worker binary and Python-parent hashes with
  `ReleaseSupervisorRoot` before any release attempt.
  It disconnects/retries the client and kills the worker or supervisor after
  `ACCEPTED`, `PREPARED`, `SENT` before API-server receipt, API-server commit
  before `ACK`, `ACK`, pending-history creation, every hook submission and
  durable phase receipt, every ordinary-object mutation/wait, both release-
  history finalization CAS operations, and final response. Lost create/update/
  delete responses reconcile only from the exact UID/resourceVersion preimage
  or signed owned-field postimage; any third state blocks. A worker that forks
  grandchildren, ignores termination, or exits while a descendant lives is
  fenced until `KillMode=control-group`, `pidfd`/`waitid`, and the empty cgroup
  prove the whole attempt dead.
  Recovery remains blocked while any `SENT/UNKNOWN` call is unresolved, a hook
  Job/Pod retains an action or finalizer path, a successful deleted hook lacks
  its database receipt, or a database effect/session/lease/token/transaction/
  advisory lock is live. The status matrix covers hookless `pending-install`,
  hook-bearing `pending-upgrade`, the exact
  `old=superseded,new=pending` finalization intermediate, an already deployed
  matching target, and the exact quiescent failed marker. Wrong release/
  revision/history/intent/envelope/hash/UID/resourceVersion, nonlatest or multiple
  pending records, two deployed records, `pending-rollback`, unregistered
  operations, missing hook evidence, CAS/RBAC denial, and gateway/SDK/driver
  version drift all fail closed. Repair mutates only the exact pending Secret's
  permitted status/description/driver-label projection and is idempotent across
  a lost repair response; byte comparison proves chart/config/manifest/hook,
  database, and application objects unchanged. An infrastructure interruption
  produces `N+1` from the same intent under a new envelope and a target defect
  requires a newer reviewed intent; neither restores `N` or the old image, and both resume the
  same durable database cursor.
  OCI cases reject an invalid signature/digest, mutable tag, unsafe archive
  path/link/count/size, missing vendored dependency, live value lookup,
  nondeterministic template function, inline secret, unenumerated revision
  field, projection mismatch, a changed external read-set between either stable
  read or the pre-mutation revalidation, or worker network/credential access. Bootstrap
  tests create the IaC namespaces/CRDs/ledger/trust/issuer/admission root and
  exercise every `FREEZE_WRITES -> ... -> SUPERVISOR_ACTIVE` boundary. They
  adopt one exact nonpending release read-only, bind its Terraform lineage/
  serial/address/configuration/plan, remove or move the resource from both
  desired configuration and state, require a fresh no-op plan, revoke old/reset
  writers, atomically activate the supervisor, and prove its semantic dry-run
  succeeds while Helm, Terraform, application, and second-supervisor writes are
  denied. A bare state removal with configured ownership fails. Hard-host tests clone or revive the
  old machine, corrupt or truncate the ledger, and lose power acknowledgement;
  writes remain blocked until the documented manual power/network fence,
  issuer revocation, maximum-TTL wait, full call reconciliation, replacement
  attestation, and fresh credential complete. `RETIRE_RELEASE` tests crash
  before/after admission closure, each typed workload/service teardown, every
  effect/debt convergence receipt, immutable database decommission commit,
  each authority Deployment scale-to-zero, and final process/Pod convergence.
  They resume one retirement journal, prove no effect reopens, and reject
  `decommission-release` before the final zero-process receipt. Supervised-decommission cases
  kill the supervisor before/after every exact object deletion and the retained-
  history CAS, reject a missing decommission receipt/live effect/debt/unknown
  object/generic Helm uninstall, and prove every operator-owned guard/evidence
  object plus the full `uninstalled` history remains. No SkyPilot Pod can mint,
  impersonate, mount, or read the supervisor credential, and deliberate release-
  API tampering with an application's real broad release-namespace permissions
  can cause only a CAS/hash denial;
- delayed completion receipts in active or maintenance state advance only their
  exact immutable carried execution and cannot start another effect. The final
  resume rejects active/prospective/debt tickets, live excluded boots,
  unrevoked old UIDs, surplus/unowned Pods, changed target envelope, mixed
  incapable digests, or live out-of-cohort controller generation. It
  atomically invalidates pre-maintenance observations, installs the new
  inventory/limit generation, and only then lets controller election and
  reconciliation restart;
- final-cleanup tests install an exactly empty PostgreSQL database into
  `SEQUENCED_BOOTSTRAP`, start locally Ready but effect-ineligible split-role
  Pods, and atomically resume active after full attestation. On the existing-
  installation path they exercise `OPEN -> PLATFORM_FROZEN -> SEALED ->
  CLEANUP_APPLYING -> CLEANUP_COMPLETE` with crashes before/after the external
  freeze, A sealing transaction, attempt-envelope binding, `-30` CAS, DDL, and
  completion receipt. Concurrent endpoint/clone/release adoption, credential/
  topology change, process registration, open epoch rebase, or stale platform
  inventory fails before DDL; an unauthorized clone has no anchor credential.
  The cleanup image
  rejects nonempty `LEGACY_ACTIVE` and `PREPARING_SEQUENCED` databases
  before cleanup DDL with the exact newer-capable-revision remediation and no
  old-image target. It cannot deploy until the anchored `SEALED` removal receipt
  proves neither state exists across the closed inventory. Old binaries
  remain rejected by the revoked legacy role, forced guard policies, and
  retained writer triggers. The guarded-DDL event triggers, both protection
  catalogs/generation, live role/ACL closure, permanent receipt plus final-RV
  acknowledgement, transactional current-head baseline, exact empty-bootstrap
  finalizer, phase-0 retirements, and permanent Pod revocations survive cleanup.
  The phase-0 termination-recorder Deployment/RBAC, executable Historical
  Authority Reset adapters/commands, and `enforce-anchor` command are absent;
  immutable reset receipts, debt, and retirements remain. No nonempty validator
  exists in either transition or final charts. A
  normal cleanup and supervised release decommission leave immutable revision evidence and
  all operator-owned guard objects untouched. Rendered RBAC has no ConfigMap
  delete verb; a separate explicit database-authority decommission test deletes
  the whole guard namespace out of band only after both release and database
  retirement are proved. From both `SEQUENCED_ACTIVE` and
  `SEQUENCED_MAINTENANCE`, the cleanup revision renders and completes exactly
  `-30/-20/-10/-5`: it renders no `-25`, phase-0 action, or nonempty-validator
  identity, creates no Kubernetes revision receipt, and hands
  `-20 -> -10 -> -5` through one immutable PostgreSQL journal target. After
  transitional resources are removed, a subsequent final-image active upgrade
  and a maintenance-enter/upgrade/resume cycle repeat that exact four-hook
  database-only path. They preserve the authority-anchor, enforcement-receipt,
  pre-schema revision-evidence, and `DatabaseAuthorityBundle` identities and
  content; the runner/migration Jobs issue no revision-evidence ConfigMap
  create/update/delete call. Separately, a fresh final-chart installation runs
  `BOOTSTRAP_SCAFFOLD`, conditionally renders `-25` while the receipt is both
  empty and mutable, creates exactly one pre-schema immutable revision receipt
  for the current-head baseline, and proves its second upgrade omits `-25`,
  creates no further Kubernetes receipt, and uses the PostgreSQL journal; and
- the BCL scheduler test remains independent of this CPU-only maintenance test
  and proves a priority-0 BCL Pod reclaims the exact low-priority fill Pod while
  no paid/cloud fallback is created.
The focused local gate is expected to include at least:

```bash
SKYPILOT_REQUIRE_SERVE_POSTGRES=1 pytest \
  tests/unit_tests/test_reserved_capacity_fill.py \
  tests/unit_tests/test_reserved_fill_broker_pg.py \
  tests/unit_tests/test_reserved_fill_activation.py \
  tests/unit_tests/test_reserved_fill_transition_hooks.py \
  tests/unit_tests/test_reserved_fill_execution_fence.py \
  tests/unit_tests/test_api_requests_pg.py \
  tests/unit_tests/test_server_request_recovery.py \
  tests/unit_tests/test_serve_autoscaler.py \
  tests/unit_tests/test_serve_autoscaler_decision_contract.py \
  tests/unit_tests/test_serve_replica_managers.py
bash format.sh --files sky/serve tests/unit_tests
helm unittest charts/skypilot
git diff --check
```

The pre-deployment Kubernetes gate additionally provisions a disposable
Kind/test cluster and runs:

```bash
pytest -q \
  tests/kubernetes/test_serve_anchor_phase0.py \
  tests/kubernetes/test_serve_helm_fix_forward.py \
  tests/kubernetes/test_server_role_maintenance_handoff.py \
  tests/kubernetes/test_serve_reserved_fill_preemption.py
```

`test_serve_anchor_phase0.py` pins a supported Kubernetes version and disposable
PostgreSQL source, then applies `ANCHOR_SCAFFOLD`, `ANCHOR_PREPARE`, and
`ANCHOR_COMPATIBLE` in order. It proves the scaffold runs zero hooks and changes
neither database nor authority cohort; exercises real release-/guard-namespace
Roles, cross-namespace RoleBindings, exact Namespace GETs, deny cases, and every
Job owner-chain check; and forces recorder list/watch restart, compaction/gap,
durable-cursor recovery, and the full disruptive rebase when the cursor cannot
be recovered. It requires `WATCH_ARMED` before the compatible
rollout and the immutable enforcement receipt afterward. Old-chart,
`--atomic`, and rollback attempts fail closed, while a newer phase-0 revision
resumes the same evidence. Finally, the cleanup chart removes only transitional
recorder/RBAC resources and retains the permanent identities plus every
operator/retained evidence object. The test asserts Namespace list/watch is
never granted and every Pod list is confined to the isolated guard namespace or
the explicitly required release-topology reader and filtered by owner/topology
in code.

`test_serve_helm_fix_forward.py` is the executable gate for the external
supervisor, signed release intent/attempt envelope, semantic read/mutation gateway, and stale-pending
protocol above. It uses no GPU or workload-provider credential; all hook/
application Pods are CPU-only fixtures, but release history is real Secret-
backed Helm state and all worker/host-death, journal, CAS, receipt, and storage-
field assertions run against the actual supervisor/SDK/driver adapters.

The rollout test uses ordinary CPU-only authority-role Pods. The BCL test uses
a scheduler-managed test extended resource. They therefore consume no reserved
GPU, launch no production BCL workload, and create no cloud spend.

Run the broad PostgreSQL Serve suite and every required GitHub check after the
focused gate. Record the exact commands, counts, failures or skips, commit SHA,
and formatter/type/lint output in this section and in the PR before merge. The
transition PR tests the one-way activation and planned/unplanned fix-forward
maintenance transitions. Its stacked removal PR tests the sequenced
protocol-v2 bootstrap/active/maintenance steady state with no deprecated
observer, lock, event, void batch, sentinel, or debit path importable.

The no-canary production verifier polls raw service status, broker/claim and
replica PostgreSQL rows, API/controller/executor image digests, and Kubernetes
pods once per reconciliation interval for five consecutive intervals within a
ten-minute outer deadline. Each poll appends an idempotent minute snapshot
under the durable validation-run identity and exact inventory revision; a
restarted verifier reconstructs continuity from those rows rather than from
local time. It records
the SLOs and zero-tolerance counters in the current rollout section. Synthetic
GPU or BCL work is not created. Deterministic integration supplies positive BCL
reclaim proof; passive live priority/preemption configuration is rechecked.
When no stable positive spendable capacity occurs, its event-dependent latency
result is `N/A`; five healthy intervals and every unconditional invariant are
still required. After binary acceptance, the verifier continues over the full
fleet for the fixed 24-hour cleanup soak. No natural capacity or BCL transition
is a gate. A verifier sample gap or identity change follows the reset/failure
rules above; it cannot silently pause or extend the soak. A missed deadline or safety invariant triggers sequenced
maintenance and a newer fix-forward image/state repair rather than
manufacturing a canary or reviving the legacy path.

### Historical protocol-v2 verification

Automated coverage must include:

- bool/object/old-pickle configuration compatibility;
- multi-context validation with per-context physical widths and logical
  one-GPU enforcement;
- populated PostgreSQL 034-to-035 upgrade, retained legacy rows, composite
  edge upserts, old-style legacy upserts after migration, and re-upgrade;
- one-to-two-to-one edge replacement and disable cleanup;
- same-service claims in two pools without overwrite;
- migration-shadow versus legacy-heartbeat races, legacy move/delete versus
  v2 adoption, owner rotation, and atomic set-plus-projection failure;
- protocol-v2 activation rejection for either schema mismatch, any non-
  PostgreSQL writer request backend, a plugin-overridden storage backend or
  queue factory, an incomplete or mixed-image
  API/controller/executor rollout, a missing or independently
  overridden controller or executor, a changing double-read cohort, an active migration Pod, an
  unattested same-release writer Pod, an extra/unready/draining database
  writer lease, an invalid Pod -> ReplicaSet -> Deployment UID chain,
  malformed/unbound or swapped in-cluster tokens, spoofed caller/environment
  identity, or an already-active gate, plus successful compatibility and HA
  persisted derived evidence;
- a completed one-way cutover marker rejects process startup when the resolved
  request storage is SQLite or plugin-overridden, while the pre-cutover
  `blocked` phase permits only the configured/resolved legacy process to drain
  and rejects an early PostgreSQL server start;
- MAIN-, UVICORN-, and EXECUTOR-only plugin backend overrides each fail their
  process context before work begins when preparation mode is enabled; v1 with
  preparation disabled retains plugin compatibility; active v2 rejects a new
  MAIN process with preparation disabled; and direct API008-to-007 downgrade
  is rejected without dropping columns or changing the schema head;
- historical schema-035 zero-argument v2-to-v1 demotion with token-bound
  stable-writer attestation, exact projection rebuild,
  multi-edge/malformed/legacy-only rejection, and atomic projection-failure
  rollback; the current Serve041 suite separately proves every such invocation
  and direct v1 write fails at the permanent protocol floor;
- overlap rejection, including kube-context aliases sharing one physical UID;
- mixed-width protocol-v1 claim deletion plus protocol-v2 pool-local zero
  authority without edge deletion, generation churn, or sibling invalidation;
- per-pool grant, feed, damping, staleness, phantom, bench, epoch, cache, and
  removal isolation;
- a service-generation advance followed by one pool's provider timeout: the
  unchanged pool/physical UID retains clipped shelter while grant, feed, and
  epoch stay zero, the healthy sibling advances independently, and the live
  production shape suppresses every prior-generation H200 fill victim without
  suppressing paid victims or emitting a failed-pool launch;
- repeated failed generations monotonically clip shelter, complete-map removal
  and disable/re-enable reset it to zero, and malformed live or dynamic-state
  authority is rejected;
- downscale-held backed reassignment uses only materialized supply and only the
  fresh no-supply source subset, preserving unrelated held exact-card retries
  and restoring same-card cold authority if the alternative supply vanishes;
- committed measured-capacity dissemination to two independent pollers sharing
  one fresh round, with one provider query and identical exact-pool/card bench
  release for both controllers;
- no measured bench release from a lease-CAS-lost writer, invalid exact-card
  split, phantom, or blackout, and conservative pre-query observation-time
  ordering against a concurrent or newer bench;
- protocol-v1 one-context and protocol-v2 multi-pool/card observation
  isolation through the actual broker-cycle paths, including a fresh-round
  reader that never invokes its query callback;
- old rounds without the reserved observation entry, old readers presented
  with the additive reserved key, independently malformed service and
  observation entries, and raw-observation-only changes that do not advance
  the pool epoch while real service feed/card changes still do;
- service-global floor/cap conservation and deterministic over-cap drain;
- global utilization-cap conservation, including need=1 across two pools;
- headroom shrink during a pool blackout and a stale-round cap-repartition
  race with concurrent replica persistence;
- real-PostgreSQL protocol-v2 advisory-session termination after a persist
  token is minted, with the replacement paused after its replica scan and
  before publish while the stale persist proves it cannot land inside that
  window;
- one replica-table read and one utilization sample per service poll cycle;
- identical H200 names in two contexts with exact-context launch selection;
- complete launch-origin attribution in both autoscaler shelter and broker
  occupancy scans: older/current tuples remain valid, while partial, future,
  UID/context/exact-shape-mismatched tuples fail closed and only genuinely legacy
  rows retain the legacy placement fallback;
- all-or-none durable launch-tuple validation at ingress, including each
  missing/mistyped field, non-Serve origin, pool/UID/card contradictions, and
  immutable PostgreSQL request-body round-trip across executor restart;
- replica JSON persistence preserves an exact boolean fill marker: integer,
  string, partial, and contradictory values fail closed after durable-row
  deserialization rather than being truthiness-coerced into legacy or v2;
- context retargeting before persistence and after enqueue, plus final-plan
  context/card/count changes from admin policy or optimization. Executor
  rejection invokes neither backend provisioning nor a provider call;
- retry/failover after a matching initial plan, where a later provisioning
  attempt selects another context or accelerator shape. The mismatched attempt
  terminates before any provider call and cannot fail over again;
- provider-fence races after the executor's initial read, including a positive
  kubeconfig refresh interval, a second Kubernetes API wrapper, concurrent
  Pod-creation thread-pool calls without serialization, the normalized
  in-cluster context, capture-pinned `kubectl` exec/rsync, and simultaneous
  conflicting UIDs for one context;
- mixed same-context legacy/protocol-v2 job-status and complete probe rounds
  run all v2 status, endpoint, candidate/route-status, liveness, preemption,
  and teardown provider work before every gated Kubernetes or unknown tokenless
  legacy provider call; the explicitly audited exact non-Kubernetes operations
  above may overlap without consulting Kubernetes authority;
  futures and exceptional/early-return paths fully join and retire owners,
  malformed v2 rows never enter the legacy phase, shared reduction preserves
  fleet state, and one UID proof is performed per v2 pool per round;
- provider-phase tests cover same-mode overlap, FIFO cohort/no-barging order,
  bounded timeout removal, an expired opposite barrier admitting queued
  active-mode followers into the same epoch without bypassing a live barrier,
  same-mode reentrancy, cross-mode rejection, explicit child admission and
  drain, stale/copied admission rejection, cancellation cleanup, phase-gate
  fork-while-held reset, and physical-registry fork-while-owner/initializer-held
  reset without deleting the parent's capture;
- blocking Kubernetes/unknown job status acquires phase before `self.lock` yet
  leaves probe and mutation refresh able to run while an SSH worker hangs. An
  exact prefetched GCP handle performs its slow SSH call without excluding a v2
  Kubernetes phase; a GCP `CommandError` regression proves fresh preemption
  classification takes ambient admission before `self.lock`. Arbitrary cloud
  objects remain ambient. Probe, refresher, and locked recovery use only
  immediate try admission. An active opposite
  phase, or a same-mode/same-context physical initializer after successful
  phase try, makes those locked paths return promptly with unchanged rows, no
  negative/identity-uncertain evidence, a released manager lock, and a
  successful next-round retry;
- one mixed probe resets its tick memo and prunes process/route registries once,
  runs complete admitted v2 work before ordinary work, joins every future and
  inline teardown before phase retirement, merges denied partitions unchanged,
  and never duplicates persistence or finalization;
- refresher tests partition wait-for-idle URL and optional log/drain reads,
  leave trackers and completed-worker ownership recoverable on phase busy, and
  still schedule the independently fenced down without treating phase busy as
  cleanup or physical-identity uncertainty;
- boot recovery under an active opposite phase completes its acquired-lock
  handshake, leaves exact rows retryable, and does not enter the generic
  30-second exception backoff while holding `self.lock`;
- a standalone v2 scale-up takes blocking phase admission before the manager
  lock. A v2 batch retains one manager-lock acquisition and each item takes a
  zero-wait phase plus zero-wait physical-initializer admission only at its
  capture/persist seam. In one real sequence item 1 owns v2, an ambient waiter
  queues, item 1 retires, ambient enters, and item 2 stops the remaining wave
  without a row/thread/list/ID leak; it is retried later. Busy/identity failure
  persists no row and registers no thread.
  Deferred down waits for drain first, takes a fresh phase/workspace/UID proof
  on every Kubernetes retry, and exact action-aware GCP cleanup bypasses the
  Kubernetes phase while retaining its UUID plus raw-handle fingerprint;
  same-UUID handle change retries provider classification, while name-only,
  fake, unknown, and Kubernetes cleanup remains ambient. Every path releases
  phase/fence during backoff;
- cold/warm LB route sync, standalone active URLs, API status serialization and
  fanout partition complete v2 groups before ordinary rows; a phase timeout
  aborts route sync with 503 and publishes no mapping/cache update, while status
  yields unknown rather than stale or negative evidence;
- interactive v2 follow holds its immutable physical fence but no process
  phase, ordinary colliding follow fails closed, and bounded/non-follow tails
  hold the normal phase for their complete read;
- v2 broker observations enter the process phase before the broker callback,
  never while its round lock is already held, use zero-wait broker-lock
  admission while the phase is live, and perform one UID proof for each driven
  physical pool; legacy/shared-demand observations use the ambient phase under
  the same rules;
- broker UID discovery waits only for the typed active owner/initializer
  collision, wakes on successful or failed retirement, survives repeated
  capture replacement with one 30-second absolute deadline, rejects a caller
  carrying a fence token, rereads fresh ambient credentials, and fails closed
  without cache fallback on timeout or non-collision identity errors;
- retargeting during restart drain, ordinary drain registration, endpoint
  discovery, status serialization, warm and cold load-balancer route sync,
  job-status SSH, candidate status, and interruption detection yields no
  provider call, READY evidence, stale retained route, preemption, or cleanup
  through the replacement cluster; a multi-replica round performs one UID
  verification per physical pool rather than one per replica;
- a physical-identity failure after partial launch never invokes an unfenced
  immediate terminate or later down/log-sync worker. The durable row remains
  cleanup-uncertain while the alias is mismatched, and a restored matching
  identity permits the exact fenced cleanup retry;
- failed-controller, failed-service, orphan-purge, interrupted-fill, and bulk
  scale-down paths retain protocol-v2 rows when cluster-record or physical
  absence is unproven, and do not finalize parent deletion around them;
- a stale cleanup blocked behind cluster locks cannot run hooks, cancel
  requests, force-unlock, refresh status, contact the provider, or remove state
  after either its resource-action UUID or legacy cluster hash has been
  replaced; both identities are re-proved after status refresh and final
  removal remains generation-predicated;
- cleanup blocked on cluster locks revalidates the action UUID, or the
  nonempty legacy `cluster_hash` fallback, plus service ownership before any
  hook, request cancellation, force-unlock, credential/status read, provider
  mutation, or row removal. Same-name row recreation, action-UUID rotation,
  and service-owner takeover therefore leave the successor untouched and the
  stale row cleanup-uncertain;
- recovery of interrupted fill rows schedules batched teardown without a
  second launch, including an already-accepted launch whose cluster record is
  initially absent: terminal cancellation without executor acknowledgement is
  insufficient, the exact request-generation barrier waits for durable handler
  quiescence, arbitrarily old required history is still included, mixed v1/v2
  waves run separate compatibility/strong barriers before any teardown, and
  malformed scope/protocol, missing, old-server, timeout, or ownership
  uncertainty retains every row while ordinary demand recovery remains
  unchanged, plus delayed same-PID delivery cannot cancel the next invocation
  and a broken pool cannot create a masking execution generation;
- no row/thread on malformed, removed, benched, or superseded pool launches;
- a stale protocol-v2 epoch fails closed before placement and persistence even
  when the current same-generation/same-UID aggregate or exact-card feed is
  positive; ordinary and cross-service post-snapshot rows, broker debit
  overlap, malformed/NaN/future timestamps, and aggregate/exact authority
  transitions cannot reauthorize it; the unchanged final persist fence still
  rejects a round or ownership change after the early check;
- workspace allowed-context validation covers inherited finite-list equality
  materialization and supersets, explicit finite-list supersets, list-to-`all`,
  empty-block normalization, and combined user-access checks; removals,
  inherited `all`/unrestricted-to-list, unrelated field changes, and a changed
  current snapshot before commit remain blocked around active resources;
- pool-local demand saturation and scale-down shelter;
- legacy dynamic-state load and new per-pool dump/load; and
- unchanged aggregate status plus additive per-pool status; and
- the chart API ClusterRole and reusable spoke RBAC module each grant only
  `get` on the named `kube-system` Namespace for physical-cluster identity,
  without namespace `list` or mutation, including a chart upgrade that reuses
  values from a pre-feature release.

Focused validation commands:

```bash
pytest -q tests/unit_tests/test_reserved_fill_broker.py
pytest -q tests/unit_tests/test_reserved_capacity_fill.py
pytest -q tests/unit_tests/test_serve_replica_managers.py
pytest -q tests/unit_tests/test_sky/workspaces/test_workspace_management.py
pytest -q tests/unit_tests/test_spot_placer_hybrid.py
pytest -q tests/unit_tests/test_concurrency_autoscaler.py
pytest -q tests/unit_tests/test_reserved_fill_broker_pg.py
bash format.sh --files <changed-python-files>
git diff --check
```

Local pre-PR evidence on 2026-08-04: the feature-focused suite completed with
1,547 passed, 54 environment-dependent skips (including PostgreSQL/Docker),
and 28 passing subtests using the supported Kubernetes client 35.0.0; all 133
affected Helm unit tests passed;
repository mypy completed with no issues across 883 files; pylint scored
10.00/10; dashboard lint completed with no warnings; and `git diff --check`
passed.

Final pre-push safety validation on 2026-08-04 reran the complete affected
activation, demotion, broker, multi-pool state, replica-manager, request
executor, request wire/storage, server route, and SDK transport suite
successfully. Focused real-process regressions prove the production thread-
dispatcher ProcessPool ownership topology and prove marker-cleanup I/O errors
cannot suppress a quiescence receipt. The changed production Python passes
mypy across 883 files and pylint at 10.00/10. Follow-up adversarial regressions
prove exact backend enforcement independently in MAIN, UVICORN, and EXECUTOR
plugin contexts, active-v2 startup enforcement, clean child-environment
restoration, exact SQLite/blocked and PostgreSQL/completed cutover ordering,
and retained API008 downgrade rejection. The generated chart schema matches
the values contract and the full local Helm run passes all 313 tests across 20
suites.
Docker is unavailable locally, so real-PostgreSQL execution remains a required
CI gate rather than being reported as local evidence.

Hotfix pre-PR validation on 2026-08-04 reran the complete affected executor,
provider-fence, Kubernetes adaptor/command, controller, replica lifecycle,
strict-drain, status, and teardown suite successfully. The one excluded
unchanged fixture asserts that mode `000` makes a file unreadable and cannot
hold when pytest itself runs as root; every other test in that backend file,
including the new under-lock UUID/hash races, passed. The full chart suite
passed all 315 tests, the reusable spoke RBAC module passed all 12 Terraform
tests, and the checked-in formatter gate completed with mypy clean across 883
files and production pylint at 10.00/10. Shell syntax, Python compilation,
Terraform formatting, and `git diff --check` also passed. Independent
adversarial review found no remaining release blocker in the lifecycle-wide
physical-cluster or cluster-generation fence.

#1275 pre-PR validation on 2026-08-04 passed 555 focused broker,
reserved-capacity, workspace, physical-fence, executor, and replica-contract
tests plus 62 subtests. After updating three stale provider-phase test doubles,
the broad affected Serve suite passed all 908 tests plus 23 subtests. Repository
mypy completed with no issues across 884 source files, YAPF and isort completed,
Python compilation and `git diff --check` passed. Its exact-head review and
required CI reported no release blocker. Subsequent production evidence below
invalidated that concurrency acceptance; these results are historical and do
not close the phase-scope hotfix gate.

Production diagnosis on 2026-08-04 separated three independent refill
failures. Merged PR #1269 lets a successfully observed zero-cost location
override an older placement bench, and merged PR #1271 keeps concurrent
non-forced UID observations from discarding their own successful reads. Helm
revision 333 deployed both as release `1.1.1095`. A post-deploy broker sample
then proved the residual shared-reader gap: the east pool persisted 102 free
slots and a 100-replica `boltz-l4-fleet` feed while the service reported one
fill holding and continued paid placement. The controller that read the fresh
shared round without driving its query callback did not receive #1269's local
placer observation. The committed-round observation metadata and reader-side
application in this corrective change close that owner/peer asymmetry without
cross-pool or cross-card leakage.

PR #1272 subsequently merged an aggregate reader-side replay of that shared
round. This branch retains #1272's owner/peer repair but carries the committed
observation through the ordinary `Allocation` result with a validated exact-
card map. That prevents aggregate A100-family capacity from releasing a full
peer card in the same context, rejects malformed or CAS-lost evidence, and
keeps v1 one-context and v2 exact-pool behavior aligned.

PR #1274 subsequently repaired the other half of the UID race: a forced
launch-guard read that completed successfully but lost cache publication had
also returned `None`, causing every otherwise matching fill launch to be
reported as a physical-UID mismatch. This branch inherits #1274 and extends its
same successful-read rule through the bounded physical-fence-retirement retry;
failures and genuine mismatches remain closed.

Helm revision 335 deployed release `1.1.1097`, which includes both #1272 and
#1274. This supersedes the revision-333 deployment state above without closing
the remaining exact-card, provider-phase, or inherited-workspace gates in this
corrective change.

Corrective PR #1275 merged as `57a0283ffb4a817d0bebed6f934cdededc726b57`
after all 30 required checks passed. Release `1.1.1099` was published and
deployed as Helm revision 336 on 2026-08-05. Runtime inspection confirmed the
exact commit/version, exact-card A100 versus A100-80GB broker observations, and
refill progress; `boltz-l4-fleet` returned to READY without an API/controller
pod restart.

That live run also found a release blocker in #1275's phase scope. Four
ordinary GCP `core.down` retries held `AMBIENT_LEGACY` across multi-minute cloud
operations, and one ten-item v2 scale batch held `V2_FENCED` across every UID
proof and row persist. Controller broker, job-status, and load-balancer callers
then exceeded the 30-second phase deadline; the service transiently reported
controller failure before self-healing. Process stack samples confirmed bounded
head-of-line blocking rather than a leaked mutex. They also exposed a gate
liveness edge: after an opposite FIFO waiter timed out, a compatible follower
could remain stranded behind its removed barrier until all original roots
retired. The phase-scope hotfix in this design addresses those exact findings;
revision 336 is not final acceptance evidence.

Phase-scope hotfix pre-PR validation on 2026-08-05 passed the complete 803-test
affected selection covering the provider gate, refill/broker, replica manager,
action-aware teardown, core fingerprint, and backend handle-change contracts.
The exact concurrency/fingerprint regression subset also passed after rebasing
onto merged PR #1277 / release `1.1.1100`. The checked-in formatter completed
with mypy clean across 884 source files, production pylint at 10.00/10, and
dashboard lint/format clean. Three independent implementation, test, and
canonical-design reviews found no remaining release blocker. Required GitHub
checks on the final PR head remain the merge gate.

Required feature CI on the preceding code-bearing head `1357dec79` completed
all 32 checks successfully. The mandatory unit job ran with
`SKYPILOT_REQUIRE_SERVE_POSTGRES=1` and completed with 14,467 passed, 1
xfailed, 197 warnings, and 103 subtests passed. Phase-scope hotfix #1280 is now
merged and deployed; those counts are historical evidence, not a current
release identity. Follow-up #1433 merged at the weakened head above after its
31 reported relevant checks passed, but before the audit's final adversarial
gate; those checks did not cover epoch-blackout, remove/re-add, or globally
coupled shelter races. The sequential correction containing this design must
pass every relevant GitHub check on its exact final head, including the
PostgreSQL Serve suite. Its local focused/broad test counts,
formatter/type/lint results, and exact-head adversarial review must be recorded
in the PR before merge and in audit state after the gate completes.

Sequential-correction pre-PR validation on 2026-08-11 passed 227 reserved-fill
tests (80 subtests), 49 refresh-row contract tests, 113 autoscaler contract
tests (7 subtests), 459 replica-manager tests, 261 broker/persist/activation/
spec tests, and 488 concurrency/controller tests (15 subtests).  The checked-in
formatter completed with mypy clean across 895 source files, production pylint
at 10.00/10, and dashboard lint/format clean; the production dashboard build
also completed.  Three alternating 21-sample end-to-end runs at 100 and 1,000
pools showed unchanged call counts and median-of-medians deltas of +0.007 ms
and -0.057 ms versus merged #1433, respectively.  No provider, broker,
database, catalog, or network call was added.

Historical phase-scope acceptance requires both active convoy exercises from
historical deployment step 6 and the sustained no-timeout/progress window.
Historical protocol-v2 feature acceptance additionally requires two live pool
claims for `boltz-l4-fleet`, a successful PHX H200 replica canary whose
persisted location is PHX, unchanged east serving health, no paid spill from a
fill decision, and an observed total fleet no larger than the configured
`max_replicas`. Those historical canary requirements do not apply to the
current direct reconciliation-correction rollout above.

## Open gates

- The externally rooted database anchor, full-fleet anchor-compatible
  drain/guard, event-driven observation/reconciliation, typed intent receipt,
  durable provider dispatcher, coherent status, and deprecation stack in the
  current correction are design-complete but not implemented. First, this exact
  canonical design must receive three passing adversarial reviews. The phase-0
  implementation stack must then pass its deterministic/PostgreSQL/Kubernetes
  gates and exact-head CI, merge, and publish immutable artifacts; operators
  deploy the inert `ANCHOR_SCAFFOLD`, byte-identical
  `ANCHOR_PREPARE`/recorder revision, and full-fleet `ANCHOR_COMPATIBLE` release
  in order and require the permanent receipt chain. Only then may the feature
  implementation PR pass its own exact-head gates, merge, publish, and deploy
  directly to the full fleet for the no-canary production acceptance window.
  Until then the 180-second lock-convoy and unaccepted-tail underfill
  remain known production failure modes; increasing the TTL or setting a
  30-second provisioning timeout is not an accepted mitigation.
- Required feature and durable provider-fence CI passed. Helm revision 336 and
  release `1.1.1099` from `57a0283ff` are historical rollout evidence; they do
  not identify the current live release. A later 2026-08-11 production check
  reported server `1.1.1243` at `2d2c67efb`; PR #1440's sequential
  detached-authority correction had already merged as `c964b5480` and is no
  longer a pending merge gate. Before any rollout or fix-forward update, still fetch and
  record the live Helm revision, chart, image tag, and immutable digest rather
  than inferring them from the server version.
- Historical live version 51 acceptance retained serving health and
  demonstrated exact-card committed observation/refill behavior, but revision
  336 transiently entered controller failure under the phase convoy described
  above. Hotfix #1280 is merged and deployed. Sustained acceptance evidence for
  the current release remains open: actively pass both convoy exercises in
  deployment step 6, then complete at least three broker intervals with
  progressing broker/LB/job-status work and no provider-phase timeout, 503
  route sync, or controller-failed transition.
- The no-platform-PR production bridge is a separately named ClusterRole and
  ClusterRoleBinding, `skypilot-physical-cluster-identity-reader`, on east and
  PHX, bound to EKS group
  `rescluster-k8s-prod-east1-preemptible-inference`. It remains an explicit
  drift item until both platform pool roots consume the fixed module. At that
  point remove the bridge binding, prove both exact UID reads still pass, and
  then remove the bridge role.
- The one-time external `sky-release-supervisor` platform-IaC prerequisite is
  required before this rollout: provision and attest its ledger/trust/issuer/
  RBAC/admission root and complete the persisted `FREEZE_WRITES -> ... ->
  SUPERVISOR_ACTIVE` cutover: reset or prove empty, read-only adopt the exact
  nonpending release, remove/move its Terraform desired resource and state,
  prove a fresh no-op plan, revoke every old/reset writer, atomically activate
  the supervisor, and prove sole-writer admission. This is not a boltz-platform SkyPilot application image/chart
  pin. After cutover, application revisions are complete signed OCI release intents
  submitted directly to the supervisor and do not wait for a separate
  boltz-platform application PR. This audit did not verify the live Helm
  revision, chart version, image tag, or image digest; import and sign those
  exact predecessor values during adoption and record them before and after
  every supervised rollout.
- The PHX H200 candidate has not yet been restored to `boltz-l4-fleet`.
  Isolated exact-context and two-pool canaries passed, including an H200 replica
  and HTTP 200 from Rainier, but versions 51--53 retained east-only placement
  catalogs because the controller config was frozen. The atomic config refresh,
  fresh production version, restart-persistence check, exact two-edge claim,
  H200 model endpoint check, and east regression check remain open.
- Draft cleanup PR #1263 is the historical protocol-v1 cleanup in stack #1264
  and remains governed by its historical gates. It is not the current
  correction's removal PR, which is not yet authored and must be linked here
  with its exact 24-hour-soak merge gate when the implementation stack is
  created.
- PR #1422's stale-epoch liveness premise remains unresolved after restoring
  strict safety fencing.  Any replacement must first add and roll out the
  durable admission-order contract described above, prove mixed-version
  fail-closed behavior, and include cross-service race, restart, large-fleet
  call-count, and timing evidence.
