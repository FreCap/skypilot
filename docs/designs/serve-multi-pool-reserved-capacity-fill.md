# Multi-pool SkyServe reserved-capacity fill

Status: feature and durable executor/provider-fence PRs are merged and deployed;
the production PostgreSQL cutover, protocol-v2 activation, and pool-identity
RBAC are complete; measured-capacity PR #1269 and UID-race PR #1271 are merged
and deployed, and shared-round replay PR #1272 is merged but not yet deployed;
live acceptance also exposed exact-card replay, mixed legacy/v2 provider-phase,
and inherited workspace-context eligibility gaps, whose corrective hotfix,
redeployment, PHX canary, and compatibility-cleanup merge gates remain open

Last updated: 2026-08-04

Canonical owner: this file. The implementation, rollout evidence, and the
stacked compatibility-removal change must stay synchronized with this
contract.

Merged feature PR [#1261](https://github.com/boltz-bio/skypilot/pull/1261) is
the revision-035 rollout change. Draft cleanup PR
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
identity. The protocol-v2 broker also runs its realtime availability listing
inside a capture pinned to that edge's expected UID. A context retarget during
measurement is therefore a blackout, never capacity evidence that can grant
or drain holdings belonging to the prior physical cluster. Every fill launch
first selects an exact carried location, takes blocking `V2_FENCED` admission
before the manager lock, and activates the exact `(context, UID)` physical
capture. It retains both authorities through the atomic
`persist_fill_replica` transaction and construction/freeze of the queued
launch tuple and thread arguments, then releases them before starting or
submitting the asynchronous launch thread; it never performs an ambient forced
UID refresh. Its durable API launch context then carries seven fields:
protocol, pool key, service generation, physical UID, Kubernetes context, and
exact accelerator name/count. Presence of any one requires all seven, a
complete normal Serve owner fence, and a controller-originated request.
Protocol must be exact integer `2`; generation and count must be positive
exact integers (not booleans); strings must be nonempty; and the parsed v2 pool
key must encode the same UID and contain the canonical accelerator. API ingress
rejects every partial, malformed, contradictory, or non-Serve tuple before
scheduling a request. Absence of all seven is ordinary demand or protocol v1
and performs no physical-identity read. The tuple is copied into the immutable
PostgreSQL request body and must survive request/executor restart without
consulting controller memory. API ingress independently validates and
atomically commits the carried tuple to that immutable request body without
relying on the earlier controller authority. Each API execution attempt
acquires a fresh phase and physical fence around registration, provisioning,
runtime bootstrap, file sync, setup, and job submission. Exact-equivalent
lower-level Kubernetes provisioning retries remain inside that one immutable
capture; the provisioner still revalidates the carried context and shape
immediately before every provider mutation. A scheduler-level retry unwinds
the complete execution scope before waiting and acquires a fresh phase and
capture on its next invocation. The tuple is bound to the same service
incarnation as the normal owner fence. Round epoch is intentionally absent:
the epoch is consumed by the atomic `persist_fill_replica` transaction, after
which that durable pending row is the reservation carried into launch.

After request recovery, admin policy, and optimization, the executor requires
the final selected resources to retain the exact Kubernetes context and shape.
That early check is not the last placement authority: immediately before every
provider attempt, the retrying provisioner revalidates the attempt's selected
resources against the durable tuple. The attempt must still use Kubernetes,
the exact context, and the canonical accelerator name/count. A mismatch is a
terminal request cancellation before provider-side actuation; it is never
eligible for retry or failover. Thus an internal retry, optimizer alternative,
or admin-policy alternative cannot leave the fenced context or shape after the
executor's final-plan check. The executor also activates a process-local
provider fence for that context/expected UID across provisioning, runtime
bootstrap, workdir sync, setup, and job submission. Fence activation captures
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
same capture-pinned kubeconfig rather than the ambient context. In-cluster
commands retain explicit `--kubeconfig /dev/null` isolation and the fixed
service-account endpoint/token mount, while `None` resolves to the active
in-cluster fence for API-client verification. Conflicting simultaneous
expected UIDs for one context fail closed. Retargeting between observation, enqueue,
restart, policy, optimization, client refresh, provider actuation, runtime
bootstrap, setup, or job submission therefore cannot mutate or deliver data to
a different physical cluster. Concurrent UID reads never publish out of
generation order. A superseded forced launch-time read may return only the
newer generation's live cache value or failure, never its own result. A
non-forced observation whose successful read loses publication may return its
own UID when no newer live entry exists: generations order lookup starts, not
the reads themselves, and discarding that independently successful observation
would spuriously withdraw the pool edge. Ordinary demand placement remains
available.

The process registry is deliberately conservative while a capture is active:
an unleased same-context provider call, or a leased call that attempts a
second context, fails closed instead of borrowing the capture or falling back
to ambient configuration. Explicitly supported fan-out propagates the lease.
An unrelated ordinary request that overlaps this short scope may therefore
retry after the scope retires; outside an active scope its historical ambient
behavior is unchanged.

Provider-bearing fleet work must make that retry boundary explicit. A mixed
legacy/protocol-v2 status or probe batch is partitioned into phases: all v2
rows run with their exact propagated leases while the batch owners are live,
the owners retire, and only then may legacy or ordinary tokenless provider
calls run. The phases may remain internally parallel, but must never overlap.
The v2 phase includes provider-bearing result classification, preemption
refresh, and teardown: merely joining the initial status or readiness futures
is insufficient. Shared provider-free reduction and persistence may run after
the phase results are materialized, but no deferred v2 provider call may escape
its lease and no tokenless call may begin before every owner has retired.
Malformed protocol-v2 rows remain identity-uncertain and are never downgraded
into the legacy phase. One round continues to perform one UID proof per
physical v2 pool.

Each OS process that can issue provider work (API, controller, or executor)
has a provider-phase gate with exact modes `V2_FENCED` and `AMBIENT_LEGACY`.
Same-mode callers may overlap. Fresh callers
receive FIFO tickets: after an opposite-mode ticket queues, later same-mode
callers cannot barge, and the next maximal same-mode prefix becomes one cohort
when the active cohort drains. A root admission may explicitly authorize
already-planned child workers to join its exact process/PID/boot/epoch-bound
cohort; an ordinary thread or copied/stale admission cannot. The root closes
child admission before exit and already-admitted children drain first.
Same-mode nesting on one thread is reentrant, cross-mode nesting is rejected,
and cancellation or timeout removes the waiter and wakes the next turn. Every
blocking acquisition has one 30-second absolute monotonic deadline and fails
closed with a typed phase-timeout error. An `after_in_child` fork hook replaces
the condition, queue, active phase, admissions, and thread-local state without
touching a possibly inherited locked mutex. The composed physical-fence
registry performs the same child reset for its lock, condition, active and
initializer maps, failure generations, and `ContextVar`; it never unlinks a
parent-owned captured kubeconfig from the child.

Blocking acquisition order is provider phase, `self.lock`, then lower-level
resources, physical-UID-cache, and broker locks.
There is one deliberate exception for the existing probe/refresher atomic
read-modify-write cycles: while continuously holding `self.lock`, they may use
only a zero-time `try_enter`. A try never queues, sleeps, joins an initializing
physical fence, or barges past a queued opposite phase. Failure skips that
provider sub-operation or partition immediately. It cannot publish readiness,
absence, preemption, identity-mismatch, or cleanup evidence and is not recorded
as physical-identity uncertainty; the unchanged row is retried next round.

There is deliberately no exclusive manager-wide reconciliation round: one
unreachable job-status SSH call must not block readiness or the refresher that
admits already-enqueued launches and downs. Job status takes its blocking phase
outside `self.lock`, partitions strict v2 rows before genuine ordinary rows,
passes the admission explicitly to every worker, and re-reads each row under
`self.lock` before reducing the materialized result. Probe and refresher retain
one continuous `self.lock` acquisition for their whole round, preserving their
existing atomicity against scale, update, and other pickled-row writers. They
therefore use only try admission while that lock is held.

One probe performs its provider-free fleet/cluster snapshot, durable handle
shape checks, tick-spec reset, process-guard prune, and route-registry prune
exactly once. It then runs the complete v2 subset under try-`V2_FENCED` and one
physical owner per `(context, UID)`, joins every readiness/status/liveness
future, finishes preemption classification, reduction, persistence, and inline
teardown, and retires all owners. Only then does the complete ordinary subset
run under try-`AMBIENT_LEGACY`. A denied subset contributes its original,
unchanged rows to the provider-free ordered final merge. One-time state is not
reset, pruned, persisted, or finalized once per subset.

The refresher similarly tries v2 work before ordinary work while retaining its
one lock acquisition. Wait-for-idle URL resolution leaves its tracker
untouched when admission is busy. Inline log sync and drain-URL lookup are
best-effort: phase busy skips log sync or uses the existing bounded drain wait,
then still schedules the separately fenced down. A phase-busy result must not
be handled after a completed launch/down worker has been removed from its
runtime registry, so it cannot strand cleanup. Recovery paths that already
hold `self.lock` follow the same try-only/no-evidence rule. Boot recovery
consumes phase busy as an ordinary deferral inside its reconciliation pass; it
must not raise into the generic 30-second retry sleep while retaining
`self.lock`.

An asynchronous launch HTTP request never holds a phase. Before persisting a v2 fill
launch, the carried override is classified provider-free, blocking
`V2_FENCED` admission is acquired before `self.lock`, and the exact
`(context, UID)` physical fence proves the pin; ambient force-refresh is not
used. Failure returns before row persistence or request submission. A deferred
down worker waits for drain with neither lock, then each retry independently
enters the row's blocking phase, selects the workspace, creates a fresh v2
physical proof where required, and performs the provider mutation. It releases
phase and fence before retry backoff and never reuses the originating round's
proof.

Paths without manager state use the process phase directly. These include cold
and warm load-balancer route synchronization, standalone active-URL reads,
full API service-status serialization (including multi-service fanout), and
reserved-capacity observations. They run complete v2 groups before ordinary
rows and never turn a phase timeout into negative evidence. A load-balancer
route-sync phase timeout aborts that synchronization with 503 and publishes no
new mapping or warm-cache state; it cannot produce a successful mapping with
the timed-out rows omitted. Standalone/API status may report identity unknown
but never physical absence. The v2 poller
enters `V2_FENCED` before `run_round_if_stale`, so it never waits for the phase
from inside the broker callback/lock; legacy/shared-demand observations enter
`AMBIENT_LEGACY` under the same rule. One driven v2 observation creates one
physical proof for its pool.

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
publication rule: a non-forced observation can report its successful post-wait
read without stealing cache ownership, while a superseded forced read remains
failed closed unless the newer generation has published a live value.
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
in its API ClusterRole for an in-cluster candidate; putting it only in the
default `rbac.clusterRules` value would let `helm upgrade --reuse-values`
silently retain the old incomplete list. The reusable spoke workspace-pool
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

Measured capacity may override a zero-cost placement bench only after the
measurement belongs to a successfully published broker round. Query callbacks
never update a controller-local placer: a writer that loses the lease-token
publish race, or whose observation is rejected as malformed, phantom, or
blackout, supplies no placement evidence. Both the writer and every peer that
reads the fresh shared round reconstruct the same optional observation through
the ordinary `Allocation` reader and apply it only to that poller's locations
for the exact pool. Protocol v1 retains its one-context invariant; protocol v2
uses the exact `FillPoolSpec.locations` for the returned `pool_key`. Capacity
from one physical pool or accelerator card therefore cannot release a bench in
another.

This committed-round dissemination uses no schema change. The existing
`feed_by_accelerator` outer JSON object carries an additional reserved key,
`$skypilot-observed-free-v1`, whose value is the validated, normalized raw
per-card observation. The `$` prefix cannot collide with a valid service name.
Normal per-service shaped-feed entries remain present, including for protocol
v1 rounds, so an older reader ignores the extra key while continuing to find
its own entry. The aggregate value and observation time remain in
`last_observed_free` and `last_observed_free_ts`; the latter is the conservative
pre-query snapshot time, not a post-query timestamp. A bench at or after that
time therefore still wins.

The service shaped-feed entry and reserved observation entry are parsed and
validated independently. A malformed service entry retains the existing
protocol-v2 fail-closed zero-launch behavior. A missing or malformed reserved
entry suppresses only measured bench release; it cannot invalidate a valid
service allocation. The reserved map accepts only canonical cards from the
pool identity, nonnegative integer counts (never booleans), no case-folded
duplicates, and a sum equal to the persisted aggregate. Old rounds without the
reserved entry provide no measured bench override and recover on the next
successfully published round.

The reserved observation is metadata, not additional launch allocation. It is
removed before comparing shaped service feeds for epoch advancement. A raw
measurement change above an unchanged service cap therefore does not churn the
pool epoch or fence queued launches. Any change to a service feed, its exact
card allocation, protocol/generation metadata, lease state, or blackout of
positive authority retains the existing epoch fence.

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
launch thread also carries an early protocol-v2 guard. Immediately before
every `sdk.launch` attempt, after any launch-pool queue delay, that guard again
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
   release upgraded with Helm `--reuse-values` remains backward compatible;
   the PostgreSQL cutover must nevertheless set it explicitly to `true`.
   Once the durable broker gate is v2, MAIN
   startup for the `all`, `api`, `controller`, and `executor` roles
   independently requires that preparation flag and exact built-ins, so a
   later new-code rollout cannot silently disable the guard. The separately
   attested resource-action authority-worker role is outside reserved-fill
   launch execution and is not coupled to this protocol gate.
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
   resource-action evidence before separately re-enabling any authority mode.
6. Before materializing PHX eligibility, deploy the mixed-phase corrective
   image to every API/controller/executor process. Observe at least three
   complete mixed legacy/v2 job-status, readiness-probe, and 60-second broker
   intervals with no unpropagated-fence error, broker edge withdrawal, or
   physical-identity uncertainty for a healthy pool. Every process must report
   the same immutable corrective digest throughout that observation.
7. Update the inference workspace from its inherited east context to an
   explicit east-plus-PHX list through the validated workspace API, restart the
   long-lived API/controller processes so they load that committed snapshot,
   and submit a fresh immutable `boltz-l4-fleet` version. Confirm two claims,
   independent rounds, exact-context/UID east and PHX canaries, and the global
   cap.
8. Keep the stacked cleanup PR blocked until the observation window and
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

Rolling back only the mixed-phase corrective image while legacy and v2 rows
coexist is also unsupported: it restores the provider-call collision that can
withdraw a healthy edge. Fix forward, or first disable fill and drain every
legacy row under the corrective image before reverting. The explicit
east-plus-PHX workspace superset itself may remain on a code rollback because
it removes no previously eligible context.

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
  and teardown provider work before every tokenless legacy provider call;
  futures and exceptional/early-return paths fully join and retire owners,
  malformed v2 rows never enter the legacy phase, shared reduction preserves
  fleet state, and one UID proof is performed per v2 pool per round;
- provider-phase tests cover same-mode overlap, FIFO cohort/no-barging order,
  bounded timeout removal, same-mode reentrancy, cross-mode rejection,
  explicit child admission and drain, stale/copied admission rejection,
  cancellation cleanup, phase-gate fork-while-held reset, and physical-registry
  fork-while-owner/initializer-held reset without deleting the parent's capture;
- blocking job status acquires phase before `self.lock` yet leaves probe and
  mutation refresh able to run while an SSH worker hangs. Probe, refresher, and
  locked recovery use only immediate try admission. An active opposite phase,
  or a same-mode/same-context physical initializer after successful phase try,
  makes those locked paths return promptly with unchanged rows, no
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
- a v2 scale-up takes blocking phase admission before the manager lock, proves
  its exact carried context/UID without ambient lookup, and releases both
  before asynchronous submission; failure persists no row. Deferred down waits
  for drain first, takes a fresh phase/workspace/UID proof on every retry, and
  releases phase/fence during backoff;
- cold/warm LB route sync, standalone active URLs, API status serialization and
  fanout partition complete v2 groups before ordinary rows; a phase timeout
  aborts route sync with 503 and publishes no mapping/cache update, while status
  yields unknown rather than stale or negative evidence;
- interactive v2 follow holds its immutable physical fence but no process
  phase, ordinary colliding follow fails closed, and bounded/non-follow tails
  hold the normal phase for their complete read;
- v2 broker observations enter the process phase before the broker callback,
  never while its round lock is already held, and one observation performs one
  UID proof for each physical pool; legacy/shared-demand observations use the
  ambient phase under the same no-lock-at-admission rule;
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

Corrective pre-PR validation on 2026-08-04 passed 555 focused broker,
reserved-capacity, workspace, physical-fence, executor, and replica-contract
tests plus 62 subtests. After updating three stale provider-phase test doubles,
the broad affected Serve suite passed all 908 tests plus 23 subtests. Repository
mypy completed with no issues across 884 source files, YAPF and isort completed,
Python compilation and `git diff --check` passed, and an independent exact-head
adversarial review found no release-blocking concurrency or identity issue.
Required GitHub CI on the final rebased head remains a merge gate.

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

- Required feature and durable provider-fence CI passed. Helm revision 333
  currently runs release `1.1.1095` (merge
  `912555e6ee160aff404aca0db89337d2981493a1`) with merged PRs #1269 and #1271.
  PR #1272 is merged upstream but is not present in that deployed image.
  The production request store completed its one-way PostgreSQL cutover, Serve
  is at schema head 035, token-bound protocol v2 is active, and the exact
  `kube-system` Namespace read is present on east and PHX. The corrective
  shared-round/provider-phase/workspace release, its required CI, and its
  mixed-round observation are still open.
- Live version 51 acceptance retained east serving health and brought up its
  paid fallback, but correctly omitted PHX from the immutable placement
  catalog because the inference workspace still inherited the global east-only
  context. The same run exposed tokenless legacy version-50 provider calls
  overlapping a protocol-v2 batch owner, plus transient broker UID discovery
  withdrawal during that overlap. The corrective mixed-phase/wait and
  effective-workspace-validation release must merge, deploy, and pass the
  observation in deployment step 6 before PHX eligibility is materialized.
- The no-platform-PR production bridge is a separately named ClusterRole and
  ClusterRoleBinding, `skypilot-physical-cluster-identity-reader`, on east and
  PHX, bound to EKS group
  `rescluster-k8s-prod-east1-preemptible-inference`. It remains an explicit
  drift item until both platform pool roots consume the fixed module. At that
  point remove the bridge binding, prove both exact UID reads still pass, and
  then remove the bridge role.
- The Helm release is declaratively owned by boltz-platform Terragrunt, whose
  `skypilot-pin.json` remains at `1.1.1084`. Revisions 331 through 333 and this
  requested corrective Helm rollout are intentional direct-deploy drift; a
  later Terragrunt apply will revert them unless the pin is reconciled. This
  rollout deliberately creates no boltz-platform PR, per operator direction.
- The PHX H200 candidate has not yet been restored to `boltz-l4-fleet`. After
  the corrective rollout, the workspace update, fresh service version, exact
  two-edge zero-cost claim/replica canary, H200 model endpoint check, and east
  regression check remain open.
- Draft compatibility cleanup PR #1263 is authored in stack #1264 and must
  remain blocked until the rollout gates above pass.
