# Multi-pool SkyServe reserved-capacity fill

Status: feature implementation and final adversarial review are complete;
required CI, production rollout, and the compatibility-cleanup merge gates
remain open

Last updated: 2026-08-04

Canonical owner: this file. The implementation, rollout evidence, and the
stacked compatibility-removal change must stay synchronized with this
contract.

Feature PR [#1261](https://github.com/boltz-bio/skypilot/pull/1261) is the
revision-035 rollout change. Draft cleanup PR
[#1263](https://github.com/boltz-bio/skypilot/pull/1263) is stacked directly
above it in GitHub stack #1264 and must remain blocked until the cleanup gates
in this design pass.

## Summary

`reserved_capacity_fill` currently accepts only one Kubernetes context per
service. Its durable claim, controller cache, autoscaler snapshot, demand
gate, and launch override also each hold only one pool. Removing the validator
alone would overwrite one context with another, multiply service-wide policy,
and allow a grant measured in one cluster to launch or shelter a replica in a
different cluster.

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
- Roll out with an additive PostgreSQL schema and a fail-closed path back to a
  one-pool binary.

## Non-goals

- User-configurable per-context floors, weights, or preferences. Stable task
  resource order is the initial pool order; an optional policy can be designed
  later without changing this contract.
- Combining disjoint accelerator groups inside one Kubernetes context. All
  zero-cost accelerator names in a context remain one physical pool group.
- Parallel Kubernetes observations in the first release. Protocol v2 retains
  the existing global broker lock and lease. A later cleanup may shard them
  after protocol v1 is removed.
- Treating two kubeconfig aliases as independent capacity. The new pool claim
  records a Kubernetes cluster UID and rejects overlapping aliases when that
  identity is available. A context whose physical identity cannot be verified
  does not participate in multi-pool fill; ordinary demand placement remains
  available.

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
identity. Every fill launch first selects an exact carried location and then
performs a forced UID refresh through that location's context before
persistence or provider actuation. It compares the result with the carried
identity, so retargeting a kubeconfig context between observation and
actuation fails closed with no row or launch thread. A stale concurrent UID
lookup may return only a newer live cache generation or failure, never its
older observation. Ordinary demand placement remains available.

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
broker lock it requires exact Serve schema head 035 and protocol v1, reads the
fixed mounted in-cluster service-account token, and rejects malformed, legacy,
or otherwise unbound tokens without complete nested namespace, Pod name, and
Pod UID claims. It loads an explicit in-cluster API client, requires that
client's installed bearer credential to equal those exact token bytes, disables
credential refresh, and shares that client across all Core and Apps reads. A
token rotation between identity parsing and client binding therefore fails
closed instead of decoupling identity from Kubernetes authentication.

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

The compatibility projection is transitional. A stacked cleanup PR removes
the legacy read/write path only after every production process uses revision
035, every live fill service has normalized claims, multi-pool canaries pass,
and the old-image rollback window is explicitly closed.

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

A failed, stale, benched, or phantom pool feeds zero only for that pool. It
does not erase another healthy edge of the same service. A pool epoch change
fences only launches stamped for that pool.

After the existing consecutive-observation threshold confirms a protocol-v2
phantom, the broker publishes explicit zero grant/feed authority for that pool
but retains the complete normalized claim set and its service generation. It
must not remove the edge mid service poll: doing so would advance the global
service fence, invalidate sibling rounds already driven in that poll, and let
the next configured heartbeat re-add the edge in an endless generation-churn
loop. Protocol v1 retains its legacy claim-removal behavior.

If driving one protocol-v2 round raises or times out, that edge publishes
feed zero, launch grant zero, and no epoch. It may retain only its prior
same-generation, same-physical-UID grant, clipped to the current edge cap, as
non-launching `shelter_grant`; a peer pool's successful round remains usable.

Round freshness is conditional on generation equality. If the caller's claim
generation is absent or differs from `claim_generations` in an otherwise fresh
round, the broker must drive a new round; it may not return the old grant. A
protocol-v2 pool always publishes an integer grant capped by its edge cap,
including the one-claimant case. The historical `grant=None` fast path exists
only in protocol v1.

### Autoscaler state and actuation

The autoscaler stores immutable per-pool snapshots in a map keyed by
`pool_key`. Each `PoolFillState` owns its locations, physical UID,
authoritative service generation, partitioned edge cap, raw and damped feed,
optional service-specific exact-card feed, timestamp, grant, and epoch. The
poller publishes a complete map atomically;
free-slot increase damping, staleness, occupancy debit, and emission-time
spending run independently per pool. A service-generation change atomically
invalidates every old pool feed. A pool remains at feed zero until a round
carrying that exact generation arrives, and its local grant is always clamped
to its edge cap.

Existing aggregate `fill_free_slots`, `fill_snapshot_age`, and `fill_target`
status fields remain compatibility projections. Additive per-pool status is
reported separately. A legacy dynamic-state dump is admitted as one anonymous
pool only when it represents a single context; ambiguous state restores
locations for scale-down protection but grants no launchable feed.

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

Every brokered fill scale-up carries:

- its protocol version;
- its pool key;
- its pool round epoch; and
- the exact pickleable location set belonging to that pool.

Protocol-v2 decisions additionally carry the authoritative service generation,
physical-cluster UID, and an exact accelerator shape when exact-card telemetry
is available. Protocol v1 retains its legacy context-key authority and does not
claim normalized generation or physical-UID provenance.

The replica manager consumes those internal fields before constructing
`Resources`, intersects them with current active zero-cost locations, and
skips with no row or thread if the intersection is empty or does not match the
pool. When exact-card metadata is present, every emitted override carries the
measured card and its exact per-replica GPU count; the manager independently
requires both the selected location and final persisted resource override to
match that shape. It then re-reads the physical UID through that context. It
never falls through to a different zero-cost context or paid capacity. The
launch thread also carries a protocol-v2 pre-cloud guard. Immediately before
every `sdk.launch` attempt, after any launch-pool queue delay, that guard again
requires the exact pinned Kubernetes context/card/count and force-refreshes
the context's physical UID; a mismatch fails closed before the cloud mutation.
This closes context retargeting between row persistence and provider launch.
Protocol-v2 fill is admitted only when the service's nonempty durable
`resource_scope` equals its service-incarnation hash; its replica cluster name
must be the deterministic name for that exact service scope and replica ID.
This makes the cluster name an incarnation identity rather than the reusable
legacy `{service}-{replica}` name. Because protocol selection is global, an
existing pre-scope service has reserved fill inactive after protocol-v2
activation until it is recreated with an incarnation scope; it cannot keep
emitting protocol-v1 fill independently.
An interrupted `PENDING` or `PROVISIONING` fill row is never recovery-re-driven.
The new controller classifies every interrupted row from its durable pool key,
service generation, and physical-cluster UID. A complete, internally
consistent protocol-v2 tuple uses the strong PostgreSQL history barrier; a
genuine protocol-v1 tuple with no physical UID and its historical null/zero
service generation uses the compatibility active-request barrier; any partial
or contradictory tuple fails recovery
closed. Mixed waves run both barriers before any row is torn down. The
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

## Deployment and rollback

1. Merge revision 035, normalized claims, pool-aware runtime, and tests while
   the durable protocol gate remains v1. Protocol-v1 one-pool behavior is
   unchanged and multi-context fill remains mechanically inactive.
2. Complete the supported one-way API request-store cutover to PostgreSQL, run
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
   release upgraded with Helm `--reuse-values` remains backward compatible;
   the PostgreSQL cutover must nevertheless set it explicitly to `true`.
   Once the durable broker gate is v2, MAIN
   startup for the `all`, `api`, `controller`, and `executor` roles
   independently requires that preparation flag and exact built-ins, so a
   later new-code rollout cannot silently disable the guard. The separately
   attested resource-action authority-worker role is outside reserved-fill
   launch execution and is not coupled to this protocol gate.
3. Verify healthy legacy rounds, then run the zero-argument explicit activation
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
4. Let every live fill controller atomically adopt an authoritative v2 claim
   set. Verify generation/edge-count integrity, integer grants, and fresh 035
   resource-action evidence before separately re-enabling any authority mode.
5. Update `boltz-l4-fleet` to append the PHX H200 context. Confirm two claims,
   independent rounds, an exact-context/UID H200 canary, and the global cap.
6. Keep the stacked cleanup PR blocked until the observation window and
   rollback gate below pass.

Normal rollback must happen while the v2 image still runs: disable fill (or
remove every secondary context), wait for pending v2 launches to be fenced and
for secondary normalized claims to disappear, then run the zero-argument
`python -m sky.serve.reserved_capacity_demotion` action inside an API pod. The
action takes the same global broker lock, requires exact schema head 035 and
API-request schema head 008, and uses its mounted pod-bound token to double-read
and attest the complete stable API/controller/executor writer rollout. It
accepts no operator-supplied rollout identity.

Under that lock, demotion takes PostgreSQL table locks that also exclude an old
v1 writer unaware of the protocol singleton. In one transaction it inventories
every authoritative set, refuses any multi-edge, malformed, stale-generation,
or incomplete set, and refuses every legacy-only row. It rebuilds the complete
legacy row for each valid single edge (including missing or divergent
projections), rereads the exact projection inventory, and only then flips the
durable gate to v1. A projection write or final validation failure rolls back
both the rebuild and gate. Verify the command reports protocol v1 before rolling
back the image. Demotion need not discover in-memory pending launches: the
atomic carried-protocol predicate fences every queued v2 persist as soon as the
gate changes. The additive tables remain.

An emergency old-image rollback against an active multi-context spec promises
only that the old controller emits no new multi-context fill. It cannot delete
normalized claims and zero/stale feed may continue sheltering existing rows;
it is not a supported drain procedure. Operators must restore the v2 image and
perform the explicit disable/demote sequence above.

Revision 035 advances resource-action activation evidence to exact revision
035. Evidence from revision 034 is invalid for new code and is not silently
re-labeled. Authority stays disabled for the entire mixed-image rollout and
is re-enabled only from fresh 035 evidence. Rolling back the image while the
database remains at 035 likewise requires authority to remain disabled,
because the old validator cannot recognize current evidence.

API request revision 008 is retained-additive and intentionally has no schema
downgrade. Removing its required/proven quiescence fields could erase the fact
that a terminal request still has executing code, while removing its runtime
capability fields could invalidate the proof used to activate protocol v2.
Application rollback therefore leaves the API request schema at 008, as the
existing retained-additive migration policy does for earlier durable request
kernel revisions.

The cleanup PR may merge only after:

- all production API, controller, and executor processes have run the new image for the
  documented rollback window;
- every live legacy claim has an equivalent normalized edge;
- east-only and east-plus-PHX services have completed update and restart
  canaries;
- same-accelerator cross-context launch fencing has been observed; and
- operators explicitly accept that rollback to a pre-035 image requires
  removing multi-context fill first.

## Verification

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
- zero-argument v2-to-v1 demotion with token-bound stable-writer attestation,
  exact projection rebuild, multi-edge/malformed/legacy-only rejection, and
  atomic projection-failure rollback;
- overlap rejection, including kube-context aliases sharing one physical UID;
- mixed-width protocol-v1 claim deletion plus protocol-v2 pool-local zero
  authority without edge deletion, generation churn, or sibling invalidation;
- per-pool grant, feed, damping, staleness, phantom, bench, epoch, cache, and
  removal isolation;
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
- context retargeting both before persistence and after launch-pool queuing,
  with the latter rejected by the per-attempt pre-cloud UID/shape guard;
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
- pool-local demand saturation and scale-down shelter;
- legacy dynamic-state load and new per-pool dump/load; and
- unchanged aggregate status plus additive per-pool status.

Focused validation commands:

```bash
pytest -q tests/unit_tests/test_reserved_fill_broker.py
pytest -q tests/unit_tests/test_reserved_capacity_fill.py
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

Required feature CI on the preceding code-bearing head `1357dec79` completed
all 32 checks successfully. The mandatory unit job ran with
`SKYPILOT_REQUIRE_SERVE_POSTGRES=1` and completed with 14,467 passed, 1
xfailed, 197 warnings, and 103 subtests passed. The final pre-cloud launch
guard and execution-quiescence head must retain the same green required checks
before merge.

Production acceptance requires two live pool claims for `boltz-l4-fleet`, a
successful PHX H200 replica canary whose persisted location is PHX, unchanged
east serving health, no paid spill from a fill decision, and an observed total
fleet no larger than the configured `max_replicas`.

## Open gates

- Revision 035 migration, concurrency, demotion, and killed-session suites have
  passed against real PostgreSQL in required CI. The production database
  migration, request-store PostgreSQL cutover, and token-bound activation
  remain to be executed during rollout.
- The SkyPilot image has not yet been built or deployed.
- The PHX H200 candidate has not yet been restored to `boltz-l4-fleet`.
- Draft compatibility cleanup PR #1263 is authored in stack #1264 and must
  remain blocked until the rollout gates above pass.
