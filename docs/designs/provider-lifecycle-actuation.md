# Provider and Lifecycle Actuation Architecture

Status: M1, M2 S1, M2 S2a.1, the S2a.2 deterministic-gzip prerequisite and
source composer, M3-S0, M3-S1, and M3-S2 are merged. M3-S0 passed exact-head
CI, exact-parent merge verification, staged revisions 49 through 51, and
bounded monitoring. The test deployment had no managed volume, so positive
live per-volume parity remains explicitly unproven and the shadow remains
diagnostic-only. M3-S2 has an exact locally verified candidate image, but its
test-cluster deployment remains pending. The M3 action graph, action runtime,
authoritative volume writer, and M4 implementation still require their
dedicated exact-design adversarial reviews and activation gates.

Canonical owner: this file. The implementation, stacked commits, removal
ledger, rollout evidence, and any contract corrections must stay synchronized
here.

## Summary

SkyPilot will move to a three-owner architecture:

1. Domain planners decide what should happen and why.
2. A durable action runtime decides when work is due and owns execution
   mechanics.
3. Typed provisioner and provider facets implement and observe external
   operations.

This removes repeated ownership of provider selection, lifecycle capabilities,
retry timing, leases, and status projection without creating one universal
resource state machine. Clusters, volumes, managed jobs, Serve, pools, and
managed container images retain their domain-specific desired state, legal
transitions, incarnation identity, compensation policy, and deletion proof.

The useful dstack concepts are explicit provider capabilities, first-class
placement offers carried from planning into actuation, nonblocking provisioning
observations, and one reusable leased-work scheduler. SkyPilot strengthens the
offer handoff with recursive immutability, stable and observation identities,
freshness, typed revalidation, bounded redaction, and serialized-handle
compatibility. SkyPilot will not copy dstack's mutable offer updates,
provider-private unbounded payloads, nondeterministic Kubernetes offer ordering,
or destructive-operation semantics. A lost lease or elapsed deadline must never
convert an ambiguous provider mutation into success.

Every milestone is independently deployable to the isolated `skypilot-ha`
release in Kubernetes context `boltz-test`. The final migration removes the
legacy paths listed in the removal ledger after objective usage and rollback
gates close.

## Current State

Provider and lifecycle responsibilities are split across several layers:

- `sky/clouds/cloud.py` owns catalog queries, feasibility, feature policy,
  deployment variables, and three lifecycle version switches.
- `sky/provision/__init__.py` exposes module-shaped provider dispatch. A
  registered plugin may implement only part of a lifecycle and silently fall
  through to the in-tree provider module for the rest.
- `sky/backends/cloud_vm_ray_backend.py` and
  `sky/backends/backend_utils.py` branch on provider versions, reconstruct
  placement, classify failures, retry, clean up, and project status.
- provider modules mix mutation, waiting, observation, and error
  classification.
- volume, cluster, managed-job, Serve, and image controllers each implement
  some combination of due-work selection, retry timing, leases, persistence,
  and cleanup.

SkyPilot already has stronger reusable foundations:

- PostgreSQL request claims use generation, token, worker, controller, and
  lease fences.
- controller action reservations persist logical action identity, resource
  identity, generation, provider operation ID, and reconciliation state.
- operational events can commit in the same transaction as a guarded request
  terminal transition.
- managed container images persist intent before external I/O, idempotency
  keys, owner epochs, database-time leases, provider-call fences, readback,
  quarantine, and exact absence proof.
- Serve cleanup retains incarnation-scoped inventory until provider absence is
  proved.

The migration generalizes these mechanics while preserving their stronger
contracts.

## Responsibility Deduplication Review

This review compares the current SkyPilot tree with
`dstackai/dstack@c9ebdaad6bbaa3105061d79f6ab52af9d609e99d`. It extends the
original three concepts with the ownership changes required to remove
duplication rather than only type it.

At this snapshot SkyPilot has 23 `sky/provision/*/instance.py` modules and all
23 implement `run_instances()`. Twenty of those modules contain blocking
`time.sleep()` polling and twenty contain provider-local status maps. The
Lambda, DigitalOcean, and Fluidstack implementations each own variants of the
same algorithm:

- drain or wait for pending instances;
- discover the current cluster and select or repair the head;
- compare observed and desired node counts;
- resume or create the missing nodes;
- poll until the desired count is ready;
- construct the same `ProvisionRecord` projection.

M1 gives this existing method group one typed owner. It deliberately does not
claim that 23 copies of the algorithm are the desired final architecture.

dstack demonstrates three useful responsibility boundaries:

1. `ComputeWithCreateInstanceSupport.run_job()` is a template method that
   converts domain configuration once and delegates only `create_instance()` to
   a provider.
2. Provider mutations return quickly, while later
   `update_provisioning_data()` calls observe readiness.
3. `Pipeline`, `Fetcher`, `Heartbeater`, `Worker`, and
   `PipelineModelMixin` reuse claim, lease, heartbeat, queue, wakeup, and
   guarded-update mechanics across resource types.

SkyPilot will adopt those boundaries, not those exact implementations.
dstack's mutable offer handoff, import-time mixin capability lists, coarse
provider exceptions, application-clock leases, and database-only stale-worker
fence are weaker than SkyPilot's required contracts. A stale worker that loses
its database lease may already have caused an external effect. Therefore a
guarded state update alone is not sufficient provider-call fencing.

The next architecture has three additional workstreams.

### Shared cluster reconciler over provider primitives

`InstanceLifecycleV1` remains the compatibility facet for the existing bulk
entry points. M4 introduces a separately versioned, opt-in node-actuation facet
for promoted providers. The exact M4 interface requires its own design review,
but its responsibility boundary is fixed here:

- the provider facet owns raw discovery, one bounded mutation submission,
  provider-native identity, raw operation observation, status translation,
  and exact absence evidence;
- a core cluster planner owns desired roles, target count, resume policy, and
  the ordered actions to request;
- a core cluster reconciler owns head selection, adoption rules, sequencing,
  waiting, deadlines, retry timing, `ProvisionRecord` construction, and
  cleanup ordering;
- domain policy owns whether typed capacity, quota, authentication,
  throttling, or ambiguous-effect evidence permits retry, failover,
  compensation, or quarantine.

The provider contract is command/query separated. A mutation method returns
without polling for terminal readiness and yields bounded typed evidence with a
serializable provider operation handle when one exists. Observation is a
separate method. A logical cluster action may require several provider effects,
such as an Azure NIC followed by a VM, a Kubernetes PVC followed by a
Deployment, or an AWS instance batch followed by role tags. The runtime, not an
in-memory provider call, must journal progress between those effects. The
minimum semantic operations are:

```text
discover(identity, scope) -> NodeInventory
prepare(exact_action, inventory) -> ProviderEffectPlan
submit_effect(exact_effect, idempotency_key) -> EffectEvidence
observe_effect(effect_handle, identity) -> EffectObservation
prove_absent(identity) -> AbsenceEvidence
```

`exact_action` has a closed operation kind and the exact deterministic names,
roles, identities, or requested create count chosen by the core planner.
`prepare()` is pure and emits a bounded deterministic DAG of exact provider
effects, identities, and dependencies. The runtime persists that whole plan
before external I/O, then persists each effect intent before calling
`submit_effect()`. A returned effect may use a provider-supported batch only
when every target has deterministic identity and per-target evidence. The
facet cannot own cluster target-count policy, select a head, choose a new
placement offer, select a compensation, sleep until ready, or project a domain
terminal status. A compensation is another exact action selected by domain
policy. Provider code only prepares and submits its effects.

`EffectEvidence` records per-target and aggregate provider request or operation
identity, effect certainty, affected provider locus, and bounded redacted
diagnostics. A crash after an effect and before evidence persistence therefore
leaves a durable effect intent that must enter readback. A provider with no
safe operation identity must return deterministic resource identity sufficient
for readback or an ambiguous-effect result that enters quarantine. If one SDK
request unavoidably creates several resource types with provider-assigned IDs,
that request is one effect only when an exact readback locator was persisted
before submission. The locator contains the provider idempotency token when
available, an unforgeable attempt and incarnation ownership label or tag,
provider scope, expected resource kinds, and bounded cardinality. Returned
child IDs are persisted as soon as they exist. Literal preknown child IDs are
required only where the provider supports them. If post-loss readback cannot
enumerate a complete bounded owned child set from the locator, the effect is
quarantined rather than treated as absent or replayed.

The shared reconciler first runs in shadow mode. It consumes the same frozen
provider inventory as the legacy implementation and compares:

- selected head identity and role assignment;
- existing, resumed, created, stopped, and terminated identity sets;
- next requested action;
- pending and terminal status projection;
- final `ProvisionRecord`;
- cleanup obligations and absence evidence.

Shadow mode never executes both mutation paths. A live shadow comparison is
valid only after the provider exposes one raw immutable inventory consumed by
both a side-effect-free legacy projection adapter and the new planner. Where
the legacy method performs hidden reads that cannot yet consume the snapshot,
only an offline frozen-inventory corpus may compare plans; it is not live
parity evidence.

The shadow wrapper captures the caller-visible `ProvisionRecord` returned by
the actual legacy `run_instances()` call, or its raised error and bounded
mutation evidence, before any projection adapter runs. After that sole mutation
owner returns, both final projection adapters may consume one new immutable
post-mutation inventory. The new projection is compared directly with the
captured legacy return, including head identity, resumed and created counts,
provider region and zone, and every caller-visible field, as well as with the
actual post-mutation identity set. A reconstructed legacy post projection is
diagnostic only and can never substitute for the actual return in a promotion
or removal gate.

Promotion is per provider and per dependency-closed operation subset. Read-only
discovery may promote independently. A mutating subset cannot promote unless
its observation, retry/readback, cleanup, compensation, and absence-proof
dependencies are promoted with it, or a characterized compatibility adapter
proves that every legacy stop, down, and recovery path can consume the new
resource identities. New create behind new evidence with legacy-only teardown
is not a valid promotion unit. A provider stays on `InstanceLifecycleV1` until
the characterized partial-create, lost-response, restart, scale, stop,
termination, and same-name-recreation corpus passes. Lack of live credentials
permits an additive contract or offline shadow adapter to land, but never
permits authoritative promotion or legacy removal.

### Durable action runtime as a declarative kernel

M3 adopts dstack's reusable leased-work shape but must not require a new
Fetcher, Heartbeater, and Worker subclass for every SkyPilot domain. One
PostgreSQL action store and worker kernel own the mechanics. The action table
itself owns one generic runnable-state and `next_attempt_at` due query for
already-admitted actions. Domain-specific admission queries and reconcilers
remain in the domain. Domain code records priority and requested timing when it
admits or reduces an action; it does not supply custom due-work SQL or a
Fetcher subclass to the worker kernel. A domain adapter declaratively supplies:

- action kind, resource identity, incarnation, and desired-generation fence;
- planner and reducer callbacks;
- the provider facet operation to invoke;
- retry, compensation, quarantine, and retention policy;
- connection-borrowing transactional state and event mutations.

The generic kernel does not own domain admission. Before an action becomes
runnable, the domain command or reconciler locks and validates its own
generation, controller or owner fence, capacity or scheduling reservation, and
legal transition. That domain-owned admission transaction writes the
reservation and generic action row together. The action retains the exact
reservation and fence identities. Retries reuse them; they never reserve the
same capacity again. The kernel claims only already-admitted rows and
revalidates those durable identities immediately before external I/O.

The kernel uses a fresh PostgreSQL statement clock after blocking locks, one
unique claim token per action attempt, `FOR UPDATE SKIP LOCKED`, bounded
queueing, wake hints, heartbeat, and a synchronous live-fence assertion
immediately before provider mutation. It
persists intent and cleanup identity before external I/O. Lost leases,
timeouts, or broad exceptions never imply that an external effect did not
happen. They enter readback or quarantine according to typed evidence.

This kernel reuses mechanics without defining a universal resource status enum
or a universal reducer. Volumes, clusters, Serve, pools, managed jobs, and
managed images retain their legal domain transitions and generation fences.

### Provider registration projection, descriptor, and generated conformance

M1 still leaves provider identity represented in parallel places:
`CLOUD_REGISTRY`, Cloud subclasses, lifecycle version switches, the
provisioner registry, the built-in provisioner inventory, configuration
schemas, service-catalog ownership, and plugin hooks.

The current registries cannot directly produce an authoritative
`ProviderDescriptorV1`. They have no common transaction or replacement
generation, and valid registrations are intentionally asymmetric: strict or
legacy provisioner-only plugins may have no Cloud entry, while IBM has a Cloud
entry but remains outside the new provisioner path.

M4 therefore starts with an immutable read-only
`ProviderRegistryAuditSnapshotV1`, captured only after a quiescent plugin
registration barrier. It takes the union rather than the intersection of the
current registries and records a typed present or absent state for every
legacy facet. It has no dispatch authority and claims no historical replacement
generation. The audit snapshot joins:

- canonical name and compatibility aliases;
- Cloud planning facet;
- strict provisioner bundle;
- optional offer source;
- optional node-actuation, volume, port, diagnostics, and configuration
  facets;
- positive provider-wide capabilities;
- typed resource-dependent support predicates;
- current plugin implementation identity and registration source.

The service catalog remains an external planning facet in V1. Moving catalog
registration into the descriptor would combine two migrations and is deferred
until lifecycle ownership is stable.

The audit explicitly classifies expected partial providers and proves
one-to-one identity only where two old registries are both expected to own a
facet. An unexpected partial entry is a conformance failure, not an import
failure.

After the inventory is characterized, a transaction-like
`ProviderRegistrationV1` coordinator becomes the only path for newly migrated
registrations. One coordinator call validates an immutable
`ProviderDescriptorV1`, publishes one immutable registry snapshot, and updates
all compatibility views under the same coordinator lock. A monotonic
process-local snapshot generation exists only for cache invalidation.

Each descriptor instead carries a stable `implementation_digest` over its
canonical facet contract versions and executable artifact fingerprints. The
digest is identical across replicas of the same implementation and changes
when a facet implementation or semantic contract changes. Built-ins derive
artifact fingerprints from the release build manifest. Plugins must supply an
installed-artifact or entry-point fingerprint that the registrar can verify.
A dynamic plugin without a stable verifiable fingerprint remains audit-only
and cannot receive authoritative promotion.

A direct legacy registration for a migrated provider immediately forces that
provider back to legacy routing and invalidates its promotions. An audit may
diagnose the mutation but cannot re-enable authority. Only an explicit
`ProviderRegistrationV1` adoption that republishes every migrated facet under a
new stable implementation digest can restore eligibility. Bootstrap state
never fabricates history that the legacy registries did not retain.

After one compatibility release with zero unexplained audit mismatches and no
direct legacy registration for migrated providers, the descriptor snapshot may
become dispatch-authoritative and legacy registry views may be derived from it.
Only that later commit can remove parallel inventories or lifecycle version
switches.

Promotion and durable actuation bind to stable implementation identity, not the
process-local snapshot generation. A promotion record is keyed by canonical
provider, facet contract version, dependency-closed operation subset, realized
actuation mode, control-plane and durable-store mode, and
`implementation_digest`. V1 authority is limited to
`central_postgresql`; `local_sqlite` and `controller_sqlite` remain separate
legacy bindings. Every durable effect plan and attempt stores that binding.
Immediately before each external effect, dispatch must resolve the current
descriptor and prove exact digest, realized mode, and control-plane/store-mode
compatibility. Replacement invalidates promotion and cannot inherit prior live
qualification. An implementation may read back or continue an older binding
only through an explicitly declared cross-digest compatibility adapter that
passes the same conformance and live gates; otherwise the action enters legacy
readback or quarantine.

Resource-dependent support is evaluated into an immutable
`ActuationBindingV1` before the first create effect. It records the realized
provider mode, such as GCP MIG versus unmanaged instances, and the exact
control-plane and durable-store mode. It does not re-evaluate mutable config or
requested `Resources` during later stop, down, or recovery. Existing resources
with no proven binding remain on the legacy path unless complete provider
discovery establishes and durably records every binding axis. A config, plugin,
or store-mode change therefore cannot silently route an old cluster into a
newly claimed capability.

Each declared descriptor facet and operation-dependency edge selects an
executable conformance matrix. The matrix is not satisfied by descriptor
self-assertion: every promoted provider supplies a deterministic fake, recorded
provider fixture, or contract adapter that can drive every declared scenario,
plus live qualification for the authoritative operation subset. The matrix
covers:

- exact signatures and no silent fallback;
- import without optional provider dependencies;
- positive and resource-dependent capability behavior;
- durable realized-mode binding independent of later configuration changes;
- pre-effect implementation-digest equality and replacement invalidation;
- idempotent adoption and stable external identity;
- pending observation without blocking;
- partial create and lost response;
- typed capacity, quota, authentication, authorization, throttling, and
  invalid-request evidence;
- stop and restart behavior;
- idempotent termination and complete absence proof;
- dependency-closed activation and legacy readback or teardown compatibility;
- redaction, plugin replacement, aliasing, and old/new client compatibility.

Datadog remains the observation plane for shadow mismatch, fallback, and
legacy-usage counters. No new statistics store is introduced.

### Further dstack review: next three bounded improvements

An additional review compared current SkyPilot at `af20f62b3` with dstack at
`c9ebdaad6`. It deliberately excluded Datadog, typed provisioner bundles,
placement-offer propagation, the durable leased-action mechanics above,
provider descriptors, typed provider failures, and lifecycle events because
those are already implemented or owned by this design. The following three
items are the remaining high-value architectural concepts. They are separately
reviewed workstreams within this migration and become M7 completion
prerequisites through their removal rows, but they do not expand M3-S3.

#### Negotiated Skylet capabilities and one fallback router

dstack's runner client negotiates a runner or shim version once, then routes
feature selection through centralized semantic-version thresholds. SkyPilot
will keep that single selection owner but improve the contract by advertising
method versions directly instead of copying the version thresholds. SkyPilot
currently repeats
`SKYLET_GRPC_FALLBACK_ERRORS` handling in `cloud_vm_ray_backend.py`, `core.py`,
and several methods of `CloudVmRayBackend`. Managed-jobs utilities and
`serve_rpc_utils.py` make additional, different flag and gRPC-error routing
decisions. These owners can therefore select different transports for the
same cluster incarnation.

SkyPilot will add an immutable `SkyletCapabilitiesV1` response containing the
Skylet identity and explicit service and method contract versions. One router,
cached by cluster incarnation and Skylet identity, selects gRPC or the
characterized legacy transport. The provisional cache key is the cluster
incarnation plus endpoint and channel generation; a successful response adds
the returned Skylet boot identity. Concurrent misses and refreshes use one
single-flight handshake. Incarnation change, channel replacement, or changed
boot identity invalidates the entry, and every entry has a maximum 60-second
revalidation interval so an in-place old-Skylet upgrade is discovered. A valid
capability response or an explicit unsupported-method result may select legacy
transport only for that interval. Timeout, unavailable, internal, malformed,
or authentication failure is transient or fatal typed evidence and never
caches a legacy choice. Capability advertisement, not a scattered semantic
version comparison or a caught transport exception, becomes the positive
selection signal.

The first slice is read-only: add backward-compatible `GetCapabilities`, then
route only `GetJobStatus` through the central selector. Old Skylets return
`UNIMPLEMENTED` and use the existing transport. No launch, cancellation,
teardown, or job-state behavior changes in that slice.

The removal gate requires single-flight handshakes with the bounded refresh
contract above, mixed old and new Skylet qualification, zero unexplained
fallback for one compatibility release, and repository proof that migrated
methods have no fallback catch or direct SSH transport outside the router.
Old-node fallback remains until that gate closes.

#### Shared raw-offer normalization, filtering, and explicit cache policy

dstack separates raw provider inventory from shared requirement filtering,
and cache construction while preserving provider input order. SkyPilot's
placement offer carries a selected result safely into actuation, but provider
Cloud classes still repeat earlier feasibility work across
`_get_feasible_launchable_resources()`, `regions_with_offering()`,
`zones_provision_loop()`, price lookup, and accelerator matching.

SkyPilot will introduce an opt-in immutable `RawOfferSnapshotV1`. A provider
source owns credentialed inventory acquisition, freshness, provider-specific
fields, input order, and declared modifiers. Shared pure code owns
normalization, generic resource filtering, rejection reasons, and an explicit
stable-order policy where ordering is semantically required. A separate exact
inventory must identify each cache owner, key, freshness source, and
invalidation rule before shared cache construction or any cache deletion is in
scope. The optimizer and existing `PlacementOfferV1` remain the owners of
cross-provider ranking and selected-offer propagation.

The design does not copy dstack's fixed cache TTL, JSON-string cache keys,
mutable offer modifiers, or assumption that one catalog implementation fits
every provider. Freshness and invalidation are typed provider inputs. The first
slice is a DigitalOcean shadow adapter over one frozen catalog snapshot. It
compares candidate resources, region order, price, and fuzzy accelerator
matches without changing the optimizer input. DigitalOcean's legacy empty
hint and missing-rejection-reason behavior is characterized as absence; the
shadow cannot claim reason parity that the legacy path does not expose.

The removal gate is per provider. A promoted provider exposes only raw
inventory and declared modifiers; its exact duplicated generic filter and
ordering code is deleted. Cache deletion is a separate locator-specific gate
after the cache inventory exists. Frozen optimizer corpora and bounded
read-only catalog qualification must show identical or explicitly reviewed
safer candidates and ordering before authority changes.

#### Shared child-workload actuation for managed jobs and Serve

dstack compiles tasks, services, and development environments into one child
job contract and reuses submission, observation, cancellation, and termination
mechanics. SkyPilot currently repeats those mechanics in
`execution._execute()`, `JobController._run_one_task()`, managed-job recovery,
and Serve replica launch, status polling, probing, and termination. Managed
jobs and Serve correctly own different domain state machines, but they should
not each own raw child-cluster transport and result normalization.

SkyPilot will add `ChildWorkloadSpecV1`, `ChildWorkloadObservationV1`, and a
versioned `ChildWorkloadActuatorV1` with launch, observe, cancel, and terminate
operations. Managed jobs and Serve remain the sole owners of admission,
recovery, autoscaling, rollout, replica health, retry policy, and terminal
domain state. Their adapters compile domain intent into the shared child
contract and reduce typed observations back into their own state machines.

The design does not copy dstack's universal `RunModel`, one shared resource
status enum, or a single giant submitted-job worker. The first slice is
read-only: route managed-job and Serve child status polling through
`ChildWorkloadObservationV1` and prove caller-visible parity. Launch and
teardown migrate only after the M3 action kernel is stable and can durably own
their external effects.

The removal gate requires repository proof that migrated Jobs and Serve
controllers make no direct `backend.get_job_status`, `sdk.launch`,
`backend.cancel_jobs`, cancellation facade, or `core.down` call outside the
adapter, plus passing recovery, cancellation, same-name recreation, rollout,
mixed-version, and cleanup qualification.

The implementation order is Skylet capability negotiation first, raw-offer
normalization as an independent provider pilot, and child-workload actuation
after the durable action kernel is qualified. This ordering avoids using a new
shared facade to conceal the same duplicated lifecycle ownership underneath.

### Patterns explicitly not ported

- Mutable offers or provider-private unbounded payloads.
- Import-time capability lists derived only from class inheritance.
- Capability presence as a replacement for resource-dependent support.
- Broad provider exceptions that authorize blind replay or immediate failover.
- A lease heartbeat that fences only the database commit and not the external
  effect.
- Per-domain copies of fetcher, worker-construction, heartbeat, and backoff
  boilerplate.
- One universal state machine for unrelated resource domains.

## Goals

- Give every provider capability one explicit, typed owner.
- Prohibit accidental mixing of plugin and built-in lifecycle methods.
- Carry one immutable placement decision from discovery through provisioning.
- Separate provider mutation from waiting and status projection.
- Reuse admitted-action due-work, lease, heartbeat, retry, and fenced-commit
  mechanics.
- Persist mutation intent and stable identity before provider I/O.
- Make ambiguous outcomes recoverable through readback or quarantine.
- Commit state transitions and lifecycle events atomically when they share a
  PostgreSQL transaction.
- Preserve public SDK, CLI, serialized handle, and plugin compatibility during
  the migration window.
- Remove every superseded compatibility path after its objective gate closes.
- End every test deployment with a healthy Helm release, healthy API,
  executor, and controller Deployments, and no orphaned test workload.

## Non-Goals

- Replacing Datadog metrics, traces, logs, monitors, or dashboards.
- Defining one status enum or one state machine for every resource type.
- Normalizing provider SDK clients, credentials, raw payloads, eventual
  consistency, endpoint discovery, or diagnostics.
- Claiming exactly-once provider effects after an unknowable network
  partition.
- Moving local or controller databases that still officially support SQLite
  into the central action store.
- Removing resource-dependent feature policy that cannot be represented as a
  provider-wide static capability.
- Rewriting every legacy Ray autoscaler provider before a stable adapter can
  contain it.

## Responsibility Contract

| Responsibility | Owner |
| --- | --- |
| Desired state, autoscaling, recovery, rollout, and cleanup policy | Domain planner |
| Offer production and provider-private placement evidence | Provider offer facet |
| Cross-provider offer filtering and ranking | Optimizer |
| Due selection for admitted actions, claim, lease, heartbeat, attempt, backoff, deadline | Durable action runtime |
| Provider API mutation and raw observation | Provider lifecycle facet |
| Failure kind, affected provider locus, request or operation ID, and effect certainty evidence | Provider lifecycle facet |
| Retry, failover, or quarantine decision | Domain policy using typed evidence |
| Domain status projection | Domain reducer |
| Child ownership and exact deletion proof | Domain cleanup policy plus provider observation |
| Durable lifecycle event and transition commit | Domain transaction through shared helper |

The action runtime may carry and validate a domain fence. It must not invent
or replace that fence. Serve keeps its lifecycle epoch, managed jobs keep
controller generation, clusters keep their incarnation or cluster hash, and
managed images keep owner epoch and desired generation.

## Provisioner Bundle and Provider Descriptor

### Positive capabilities

M1 introduces an immutable, versioned `ProvisionerBundleV1` inside
`sky.provision`. It owns only the current provision-module seam. It does not
replace the Cloud registry, catalog ownership, optimizer ownership, or
`CloudImplementationFeatures`.

The required `InstanceLifecycleV1` facet is exactly the synchronous method
group implemented by all 24 built-in new-provisioner packages:

- `query_instances`
- `bootstrap_instances`
- `run_instances`
- `stop_instances`
- `terminate_instances`
- `wait_instances`
- `get_cluster_info`

The facet signatures omit the public facade's `provider_name` because the
facade consumes that argument before dispatch. The facet remains synchronous
in M1. The future nonblocking `prepare()`, `submit_effect()`, and
`observe_effect()` contract is a separately versioned actuator facet introduced
only when cluster actuation migrates.

Granular optional provisioner facets are added only when an immediate consumer
migrates:

- `ClusterInventory`
- `PortLifecycle`
- `VolumeLifecycle`
- `RuntimeConfigurator`
- `Diagnostics`

Each facet is a structural protocol. Registration validates a complete method
group once, when it is registered. Runtime dispatch does not repeat signature
inspection beyond the existing public facade argument binding. A bundle either
owns a complete facet or does not own it.

M2 introduces `OfferSource` beside the existing Cloud and optimizer seams. It
is deliberately not part of `ProvisionerBundleV1`. A future
`ProviderDescriptor` may join Cloud planning metadata and provisioner facets
only after canonical-name, alias, plugin, and registry ownership have one
explicit design. M1 must not create a second universal provider registry.

The first compatibility release supports two registration modes:

1. Strict bundles declare complete facets and never fall through to built-in
   methods inside a declared facet.
2. Legacy module registration retains method-by-method fallback and emits a
   deprecation diagnostic once per provider, facet, and process when it
   produces a mixed owner. The diagnostic uses existing logs and Datadog
   collection and does not add a statistics store.

Strict registration has one resolved owner per facet. Repeating the identical
registration is idempotent. Replacement preserves the existing
last-registration-wins plugin contract and emits one replacement diagnostic;
it never composes two strict owners. The registry rejects:

- an incomplete declared facet;
- an unknown capability;
- a strict plugin facet that implicitly calls a built-in implementation;
- a provider name that normalizes differently between lookup and
  registration.

One canonicalization function owns registration and lookup aliases. Both
`lambda` and the historical `lambda_cloud` spelling resolve to canonical
`lambda`. Because this corrects the existing mismatch where a Lambda template
override can work while its lifecycle override is ignored, it lands as an
explicitly tested behavior fix in the M1 resolver commit rather than an
undocumented side effect.

Built-in modules are exposed through an explicit, late-bound 24-entry bundle
getter map before call sites move. Late binding preserves both attribute and
whole-module monkeypatch seams without returning to dynamic `globals()`
discovery. The map includes DigitalOcean and Paperspace rather than depending
on incidental imports from their Cloud modules. IBM remains on the legacy Ray
path. A typed legacy adapter temporarily quarantines the known built-in
signature drift while forwarding the facade's original arguments unchanged.
This establishes the typed seam without silently promoting a legacy provider.

### Resource-dependent support

Provider-wide capability presence does not replace resource-dependent support.
For example, stop support, placement groups, spot, or ports can depend on
resource kind and provider mode. A facet may expose a typed
`supports(operation, resources)` predicate. Existing policy remains until the
new predicate has characterization and provider-conformance coverage.

### Plugin compatibility

The following surfaces remain compatible during the declared window:

- `register_provisioner(cloud_name, module, template_override=...)`;
- direct imports from `sky.provision`;
- registered template overrides;
- provider function signatures;
- built-in module monkeypatch seams used by tests and downstream plugins.

The strict API is additive. Legacy removal requires repository search,
downstream plugin inventory, one release of deprecation evidence, and an
explicit removal commit.

## Placement Offer

### V1 Ownership and Source Contract

`PlacementOfferV1` is a recursively immutable internal decision object produced
by an optional provider `OfferSourceV1`, ranked by the optimizer, and carried
unchanged into actuation unless an explicit revalidation produces a new
observation of the same stable offer. It is not part of
`ProvisionerBundleV1`. `Cloud.get_offer_source()` is an additive positive
capability whose default implementation returns `None`; existing Cloud
subclasses and plugins therefore retain their current behavior without a second
universal provider registry.

The V1 source boundary is:

```python
class OfferOperationV1(enum.Enum):
    PLAN_CREATE = 'plan_create'
    FRESH_CREATE = 'fresh_create'
    REUSE = 'reuse'
    RESTART = 'restart'


class OfferActuationKindV1(enum.Enum):
    DIRECT_POD = 'direct_pod'
    CONTROLLER = 'controller'
    HA_DEPLOYMENT = 'ha_deployment'
    UNKNOWN = 'unknown'


class ObservationFreshnessV1(enum.Enum):
    ALLOW_REQUEST_CACHE = 'allow_request_cache'
    REQUIRE_FRESH = 'require_fresh'


@dataclasses.dataclass(frozen=True)
class OfferRequestV1:
    resources: 'resources_lib.Resources'
    num_nodes: int
    workspace: str | None
    has_volume_mounts: bool
    has_storage_mounts: bool
    operation: OfferOperationV1
    actuation_kind: OfferActuationKindV1


@typing.runtime_checkable
class ProviderObservationSnapshotV1(typing.Protocol):

    @property
    def provider(self) -> str:
        ...

    @property
    def observed_at(self) -> datetime.datetime:
        ...

    @property
    def capture_id(self) -> str:
        ...


@typing.runtime_checkable
class ProviderActuationContextV1(typing.Protocol):

    @property
    def provider(self) -> str:
        ...

    @property
    def capture_id(self) -> str:
        ...

    def close(self) -> None:
        ...


@dataclasses.dataclass(frozen=True)
class ObservationCaptureV1:
    observation: ProviderObservationSnapshotV1
    actuation_context: ProviderActuationContextV1 | None

    def __post_init__(self) -> None:
        # Protocol conformance and the provider, capture-ID, and UTC timestamp
        # grammars are checked before these equality checks.
        if self.actuation_context is not None:
            if (self.actuation_context.provider !=
                    self.observation.provider):
                raise ValueError('Observation and context providers differ.')
            if (self.actuation_context.capture_id !=
                    self.observation.capture_id):
                raise ValueError('Observation and context captures differ.')


@typing.runtime_checkable
class OfferSourceV1(typing.Protocol):

    def capture_observation(
        self,
        request: OfferRequestV1,
        *,
        observed_at: datetime.datetime,
        freshness: ObservationFreshnessV1,
    ) -> ObservationCaptureV1:
        ...

    def list_offers(
        self,
        request: OfferRequestV1,
        *,
        observation: ProviderObservationSnapshotV1,
    ) -> OfferSetResultV1:
        ...

    def revalidate(
        self,
        offer: PlacementOfferV1,
        request: OfferRequestV1,
        *,
        observation: ProviderObservationSnapshotV1,
    ) -> OfferRevalidationResultV1:
        ...
```

The caller supplies a timezone-aware UTC `observed_at`, so identity, expiry, and
tests do not depend on a hidden provider clock. All three methods are read-only
with respect to provider state. A provider snapshot is a provider-specific
frozen value containing only the bounded raw fields needed by both projections.
It is process-local, is never serialized, persisted, hashed into an offer,
logged, or sent to Datadog, and rejects access by a source for a different
provider or observation time. Its random `capture_id` distinguishes provider
reads without entering offer identity.

Every observation, context, and selection capture ID is exactly the canonical
lowercase string form of an RFC 4122 UUIDv4 generated with `uuid.uuid4()`:
`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`.
No braces, uppercase letters, alternate UUID versions, nil UUID, compact form,
or provider-native ID is accepted.

`ObservationCaptureV1.__post_init__()` requires `observation` to implement
`ProviderObservationSnapshotV1`, validates its provider, capture ID, and aware
whole-second UTC timestamp, and, when a context is present, requires it to
implement `ProviderActuationContextV1` and validates the same provider and
capture-ID grammars before enforcing the equalities shown above.
`ObservationCaptureV1` keeps any provider client outside that frozen snapshot.
Optimizer-only captures close and discard their actuation context before
ranking. Kubernetes `REQUIRE_FRESH` returns a
`KubernetesPinnedActuationContextV1` that owns one newly constructed
`ApiClient`; the source derives the endpoint fingerprint from that exact
client's attached `Configuration` and performs namespace, node, identity, and
resource-readiness calls through API objects built from the same client. It
never independently reloads kubeconfig to describe a different target. The
authoritative V1 subset rejects every configured autoscaler before querying it;
in particular, it never combines this pinned Kubernetes identity with the
ambient GCP client used by `GKEAutoscaler`. The context is request-local,
contains credentials, is never hashed, serialized, logged, or placed in an
offer, and is closed by its current owner on every terminal success or
exception path. The only credential copy is the private exact-target transport
described below.

The generic offer and evidence modules are leaf modules. S1 is the generic
offer contract foundation in `sky/placement/offer.py`; the separate
`ActualPlacementEvidenceV1` leaf and `ProvisionRecord` field are deferred to
S3. Both leaves import only the standard library and existing leaf JSON typing
helpers. `Resources` and `Cloud` annotations are quoted and imported only under
`TYPE_CHECKING`; the generic modules do not import clouds, optimizer, backend,
provisioner, server, or Kubernetes modules. `Cloud.get_offer_source()` likewise
uses a quoted return annotation and performs no provider SDK, kubeconfig, or
plugin work at import time. Placement types are not re-exported from
`sky.__init__` or `sky.clouds.__init__`.

The two S1 leaf files remain importable on the repository's supported Python
3.10 through 3.14 range. They use `from __future__ import annotations` and the
PEP 585 and PEP 604 forms required by the repository's Ruff `py310` target.
`FrozenJSONDict` is declared before the recursive `FrozenJSONValue` runtime
alias. Its runtime base is
`collections.abc.Mapping[str, 'FrozenJSONValue']`; after that class exists,
`FrozenJSONValue` may include the class object in a PEP 604 union. The leaf
never evaluates `UnionType | 'FrozenJSONDict'`, which raises `TypeError` on
Python 3.10 through 3.13. The existing `worker-floor-import` CI job explicitly
imports both leaves on Python 3.10 and compile-checks the whole package; normal
CI imports and tests them on the deployed Python 3.14 ceiling.

#### Exact V1 leaf types

`Cloud.get_offer_source()` is an instance method. The generic leaf defines
these additional closed enums and exact wire values:

| Enum | V1 members and wire values |
|---|---|
| `OfferSetStatusV1` | `OK='ok'`, `NO_OFFERS='no_offers'`, `NOT_REPRESENTABLE='not_representable'` |
| `OfferRevalidationStatusV1` | `VALID='valid'`, `UNAVAILABLE='unavailable'`, `NOT_REPRESENTABLE='not_representable'` |
| `OfferPriceBasisV1` | `NODE_HOUR='node_hour'` |
| `OfferCurrencyV1` | `USD='USD'` |
| `OfferPurchaseModeV1` | `ON_DEMAND='on_demand'` |
| `OfferAvailabilityV1` | `UNKNOWN='unknown'`, `UNAVAILABLE='unavailable'` |
| `OfferRevalidationPolicyV1` | `BEFORE_MUTATION='before_mutation'` |
| `OfferReservationEvidenceV1` | `NOT_APPLICABLE='not_applicable'` |
| `OfferQuotaEvidenceV1` | `UNKNOWN='unknown'`, `UNAVAILABLE='unavailable'` |
| `OfferCapacityEvidenceV1` | `SHAPE_FITS_EXISTING_NODE='shape_fits_existing_node'`, `CONTEXT_UNREACHABLE='context_unreachable'`, `SHAPE_NO_LONGER_SUPPORTED='shape_no_longer_supported'`, `CAPACITY_UNAVAILABLE='capacity_unavailable'`, `PROVIDER_OBJECT_CONFLICT='provider_object_conflict'` |

These generic V1 sets intentionally equal the values needed by the Kubernetes
pilot. A later provider does not silently extend them; a new wire value requires
an explicit schema-version design update.

The in-memory offer has exactly these frozen nested dataclasses and fields:

```python
@dataclasses.dataclass(frozen=True)
class OfferScopeV1:
    kind: str
    id: str


@dataclasses.dataclass(frozen=True)
class OfferAcceleratorV1:
    name: str
    count: int


@dataclasses.dataclass(frozen=True)
class OfferResourcesV1:
    instance_type: str
    cpus: str
    memory_gib: str
    accelerators: tuple[OfferAcceleratorV1, ...]
    disk_tier: str | None
    network_tier: str | None
    placement_constraints_digest: str | None


@dataclasses.dataclass(frozen=True)
class OfferPriceV1:
    amount: str
    basis: OfferPriceBasisV1
    currency: OfferCurrencyV1


@dataclasses.dataclass(frozen=True)
class OfferEvidenceV1:
    reservation: OfferReservationEvidenceV1
    quota: OfferQuotaEvidenceV1
    capacity: OfferCapacityEvidenceV1
    requested_nodes: int


@dataclasses.dataclass(frozen=True, init=False)
class OfferProviderPayloadV1:
    version: int
    identity: FrozenJSONDict
    observation: FrozenJSONDict


@dataclasses.dataclass(frozen=True, init=False)
class PlacementOfferV1:
    schema_version: int
    operation: OfferOperationV1
    actuation_kind: OfferActuationKindV1
    offer_id: str
    observation_id: str
    provider: str
    scope: OfferScopeV1
    resources: OfferResourcesV1
    region: str
    candidate_zones: tuple[str, ...]
    batching_scope: str
    price: OfferPriceV1
    purchase_mode: OfferPurchaseModeV1
    availability: OfferAvailabilityV1
    observed_at: datetime.datetime
    ttl_seconds: int
    revalidation_policy: OfferRevalidationPolicyV1
    evidence: OfferEvidenceV1
    provider_payload: OfferProviderPayloadV1
```

`FrozenJSONDict` is a detached, recursively immutable mapping. Objects are
stored with keys sorted by Unicode code point; arrays become tuples. Equal
frozen objects therefore have equal hashes regardless of input insertion
order. Thawing always produces a fresh tree of JSON built-ins.

`OfferProviderPayloadV1` has only one supported construction path:

```python
@classmethod
def create(
    cls,
    *,
    identity: dict[str, JSONValue],
    observation: dict[str, JSONValue],
    payload_schema: ProviderPayloadSchemaV1,
) -> OfferProviderPayloadV1:
    ...
```

`PlacementOfferV1` has only these three supported construction paths:

```python
@classmethod
def create(
    cls,
    *,
    operation: OfferOperationV1,
    actuation_kind: OfferActuationKindV1,
    provider: str,
    scope: OfferScopeV1,
    resources: OfferResourcesV1,
    region: str,
    candidate_zones: tuple[str, ...],
    batching_scope: str,
    price: OfferPriceV1,
    purchase_mode: OfferPurchaseModeV1,
    availability: OfferAvailabilityV1,
    observed_at: datetime.datetime,
    ttl_seconds: int,
    revalidation_policy: OfferRevalidationPolicyV1,
    evidence: OfferEvidenceV1,
    provider_payload: OfferProviderPayloadV1,
    payload_schema: ProviderPayloadSchemaV1,
) -> PlacementOfferV1:
    ...


@classmethod
def from_envelope(
    cls,
    envelope: dict[str, JSONValue],
    *,
    payload_schema: ProviderPayloadSchemaV1,
) -> PlacementOfferV1:
    ...


@classmethod
def from_json(
    cls,
    serialized: str | bytes,
    *,
    payload_schema: ProviderPayloadSchemaV1,
) -> PlacementOfferV1:
    ...
```

Both dataclasses use `init=False`. Callers never supply `version`,
`schema_version`, `offer_id`, or `observation_id`; the factory fixes the
versions and computes both IDs. All three placement-offer paths apply the
generic envelope validators and injected payload schema. `observed_at` is an
aware UTC `datetime` with whole-second precision. Candidate-zone order is
preserved because it enters stable identity. Accelerator names must be unique
and arrive in normalized sorted order; the parser rejects an unsorted or
duplicate list rather than silently changing a provider decision.

Only `PLAN_CREATE` and `FRESH_CREATE` offers may be constructed.
`PLAN_CREATE` is process-local and cannot be enveloped or handed off.
`REUSE` and `RESTART` are request classifications only and return
`NOT_REPRESENTABLE(UNSUPPORTED_OPERATION)` in V1.

The leaf also defines an injected, recursively immutable payload allowlist:

```python
class ProviderPayloadNodeKindV1(enum.Enum):
    STRING = 'string'
    DIGEST = 'digest'
    INTEGER = 'integer'
    BOOLEAN = 'boolean'
    NULL = 'null'
    OBJECT = 'object'
    ARRAY = 'array'


@dataclasses.dataclass(frozen=True)
class ProviderPayloadSchemaNodeV1:
    kind: ProviderPayloadNodeKindV1
    fields: tuple[tuple[str, 'ProviderPayloadSchemaNodeV1'], ...] = ()
    item: 'ProviderPayloadSchemaNodeV1 | None' = None
    allowed_strings: tuple[str, ...] = ()
    allow_empty: bool = False

    def __post_init__(self) -> None:
        # Enforce the closed shape rules below before an instance is usable.
        ...


@dataclasses.dataclass(frozen=True)
class ProviderPayloadSchemaV1:
    provider: str
    identity: ProviderPayloadSchemaNodeV1
    observation: ProviderPayloadSchemaNodeV1

    def __post_init__(self) -> None:
        # Enforce the provider grammar and object roots described below.
        ...
```

`ProviderPayloadSchemaNodeV1.__post_init__()` requires `fields` to be an exact
tuple of exact two-tuples, every key to satisfy the provider-payload key bound,
every child to be a schema node, and keys to be unique and already sorted by
Unicode code point. An `OBJECT` node has no `item`, `allowed_strings`, or
`allow_empty`. An `ARRAY` node has exactly one schema-node `item` and has no
fields, `allowed_strings`, or `allow_empty`. Every scalar node has no fields or
item. Only `STRING` may have `allowed_strings` or `allow_empty`;
`allowed_strings` is an exact tuple of valid fixed-`NFC_V1` strings that is
unique and already sorted by Unicode code point. An empty allowed string is
valid only when `allow_empty` is true.

`ProviderPayloadSchemaV1.__post_init__()` requires `provider` to match
`^[a-z0-9](?:[a-z0-9._-]{0,61}[a-z0-9])?$`, requires both roots to be exact
`ProviderPayloadSchemaNodeV1` instances, and requires both root kinds to be
`OBJECT`.
The schema is passed explicitly to payload creation and envelope parsing, and
its provider must equal the offer provider. This is not a global registry and
does not import provider code into the generic leaf. The Kubernetes source owns
and passes its exact schema. Unknown keys are rejected by this schema at every
payload level.

For each eligible optimizer comparison in shadow mode the orchestrator calls
`capture_observation()` exactly once. The provider's legacy projection adapter
and `list_offers()` independently consume that same object. The legacy adapter
is allowed only at this comparison seam and cannot accept offers as its input.
The closed configuration and resource classifiers run before provider reads.
When they return `NOT_REPRESENTABLE`, shadow records that result and invokes the
untouched legacy feasibility path; it does not query excluded systems merely to
manufacture a comparison snapshot. The later under-lock binding and
pre-mutation revalidation are distinct captures with distinct IDs. Outside
shadow mode, the legacy path retains its existing observation behavior until
the M4 descriptor-owned authoritative promotion commit.

Optimizer shadow capture uses `ALLOW_REQUEST_CACHE` so the two projections can
share the request's existing provider read. Pre-mutation revalidation must call
`capture_observation()` with `REQUIRE_FRESH` and pass that new snapshot to
`revalidate()`. A provider implementation of `REQUIRE_FRESH` bypasses every
request-scoped node, context, and configuration cache and performs new
underlying reads. Revalidation rejects the selection snapshot's `capture_id`.
Tests assert both a distinct capture ID and increased underlying Kubernetes
client call counts.

`PLAN_CREATE` is a read-only optimizer intent, not proof that a fresh create is
still safe. It may produce comparison offers but no `PLAN_CREATE` offer can
cross into provisioning. `CloudVmRayBackend._check_existing_cluster()` owns the
authoritative operation classification because it runs while both the cluster
status lock and cluster resource-operation lock are held, after
`refresh_cluster_record()` has resolved the live handle and status:

- no record, no live handle, and no restart or resume path is `FRESH_CREATE`;
- an existing UP cluster is `REUSE`;
- an existing STOPPED or recoverable INIT cluster is `RESTART`.

For `FRESH_CREATE`, `_check_existing_cluster()` captures a new observation and
binds an exact offer to the concrete `to_provision` winner before constructing
`ToProvisionConfig`. `REUSE` and `RESTART` return
`NOT_REPRESENTABLE(UNSUPPORTED_OPERATION)` and clear any optimizer shadow
decision. When M4 enables authoritative binding, it is limited to the first
provider mutation attempt of a locked provisioning entry.
`RetryingVmProvisioner` carries a
monotonic process-local `provider_attempt_count`, incremented immediately before
calling `bulk_provision()`. Once that call starts, any return or exception
permanently makes later candidates in that entry
`NOT_REPRESENTABLE(RETRY_AFTER_PROVIDER_ATTEMPT)`. The retry loop clears the
offer and first runs only the exact handle-backed fenced reconciler described
below. If and only if that reconciler proves every planned Kubernetes name
absent and clears the fence, the loop may continue through the existing legacy
placement path; it emits the typed fallback event and passes the explicit
trusted `LEGACY_RETRY_AFTER_PROVIDER_ATTEMPT` handoff described below. An
unresolved or quarantined fence stops failover. M2 does not generalize that
Kubernetes name-and-UID proof into provider-wide cleanup evidence and never
binds another authoritative offer.
Failures before `bulk_provision()` do not increment the count and may replan
only after any precommitted attempt fence is proved mutation-free and cleared.
Recoverable INIT remains `RESTART` for the whole entry. Shadow mode may continue
comparing offers on every retry but never
hands them to mutation. A later durable cleanup milestone may broaden this only
after every provider has typed absence evidence and an atomic cluster-record
reset. A control path that releases either lock must reacquire both and return
to `_check_existing_cluster()`. Dry run always remains `PLAN_CREATE`.

`OfferReasonCodeV1` is a closed enum with exactly these member names and wire
values:

```python
class OfferReasonCodeV1(enum.Enum):
    NONE = 'none'
    NO_FEASIBLE_SHAPE = 'no_feasible_shape'
    UNSUPPORTED_OPERATION = 'unsupported_operation'
    UNSUPPORTED_ACTUATION_KIND = 'unsupported_actuation_kind'
    UNSUPPORTED_NODE_COUNT = 'unsupported_node_count'
    UNSUPPORTED_ACCELERATOR = 'unsupported_accelerator'
    UNSUPPORTED_RESOURCE_MODE = 'unsupported_resource_mode'
    UNSUPPORTED_NETWORK_TIER = 'unsupported_network_tier'
    VOLUME_OR_STORAGE_MOUNT = 'volume_or_storage_mount'
    KUEUE_ENABLED = 'kueue_enabled'
    RESERVATION_REQUESTED = 'reservation_requested'
    CUSTOM_PLACEMENT_CONFIG = 'custom_placement_config'
    UNRESOLVED_SCOPE = 'unresolved_scope'
    CONTEXT_UNREACHABLE = 'context_unreachable'
    SCOPE_CHANGED = 'scope_changed'
    CONFIGURATION_CHANGED = 'configuration_changed'
    SHAPE_NO_LONGER_SUPPORTED = 'shape_no_longer_supported'
    CAPACITY_UNAVAILABLE = 'capacity_unavailable'
    QUOTA_UNAVAILABLE = 'quota_unavailable'
    OFFER_IDENTITY_CHANGED = 'offer_identity_changed'
    OBSERVATION_LIMIT_EXCEEDED = 'observation_limit_exceeded'
    PROVIDER_OBJECT_CONFLICT = 'provider_object_conflict'
    SOURCE_ERROR = 'source_error'
    RETRY_AFTER_PROVIDER_ATTEMPT = 'retry_after_provider_attempt'
```

`OfferSetResultV1` is a validated direct-construction dataclass with this exact
field order:

```python
@dataclasses.dataclass(frozen=True)
class OfferSetResultV1:
    status: OfferSetStatusV1
    offers: tuple[PlacementOfferV1, ...]
    reason_code: OfferReasonCodeV1
```

Its `__post_init__()` enforces the disposition matrix below.
`OfferSetResultV1.status` is exactly one of:

- `OK`, with a nonempty ordered tuple of offers;
- `NO_OFFERS`, when the provider observation proves no feasible offer;
- `NOT_REPRESENTABLE`, when the V1 source cannot faithfully encode a supported
  legacy placement constraint.

`OK` requires `reason_code=NONE`. `NO_OFFERS` requires an empty offer tuple and
`NO_FEASIBLE_SHAPE`. `NOT_REPRESENTABLE` requires an empty offer tuple and one
of exactly `UNSUPPORTED_OPERATION`, `UNSUPPORTED_ACTUATION_KIND`,
`UNSUPPORTED_NODE_COUNT`, `UNSUPPORTED_ACCELERATOR`,
`UNSUPPORTED_RESOURCE_MODE`, `UNSUPPORTED_NETWORK_TIER`,
`VOLUME_OR_STORAGE_MOUNT`, `KUEUE_ENABLED`, `RESERVATION_REQUESTED`,
`CUSTOM_PLACEMENT_CONFIG`, `UNRESOLVED_SCOPE`, or
`OBSERVATION_LIMIT_EXCEEDED`. `SOURCE_ERROR` and
`RETRY_AFTER_PROVIDER_ATTEMPT` are orchestration outcomes, not source result
dispositions.

`OfferRevalidationResultV1` has this exact field order and blocks direct
construction:

```python
@dataclasses.dataclass(frozen=True, init=False)
class OfferRevalidationResultV1:
    status: OfferRevalidationStatusV1
    offer: PlacementOfferV1 | None
    reason_code: OfferReasonCodeV1
```

Its `status` is exactly one of:

- `VALID`, with a new observation of the same stable offer;
- `UNAVAILABLE`, with typed quota, capacity, reachability, or reservation
  evidence;
- `NOT_REPRESENTABLE`, when current inputs can no longer be represented safely.

`VALID` requires a non-null offer, the original `offer_id`,
`reason_code=NONE`, and a nondecreasing `observed_at`. `UNAVAILABLE` requires a
non-null observation of the same offer with unavailable evidence and one of
`CONTEXT_UNREACHABLE`, `SHAPE_NO_LONGER_SUPPORTED`, `CAPACITY_UNAVAILABLE`, or
`QUOTA_UNAVAILABLE`; the rendered-name preflight may additionally use
`PROVIDER_OBJECT_CONFLICT`. That preflight converts a `VALID`
replacement into an unavailable observation of the same stable offer with
`PROVIDER_OBJECT_CONFLICT`; that is a non-failover terminal safety result, not
permission to enter the legacy mutation path. `NOT_REPRESENTABLE` has no offer
and requires
`SCOPE_CHANGED`, `CONFIGURATION_CHANGED`, or `OFFER_IDENTITY_CHANGED`. A
revalidation that computes a different stable identity returns
`NOT_REPRESENTABLE` with `OFFER_IDENTITY_CHANGED`; only orchestration may
explicitly replan.

`OfferRevalidationResultV1` is `init=False` and has only these supported
factories:

- `valid(original, replacement)`;
- `unavailable(original, replacement, reason_code)`;
- `not_representable(reason_code)`.

The first two factories enforce the same `offer_id`, requested-node count, and
nondecreasing `observed_at` against `original`. The full evidence state is
closed, not just the reason-associated field:

| Factory and reason | Availability | Reservation | Quota | Capacity |
|---|---|---|---|---|
| `valid()` | `UNKNOWN` | `NOT_APPLICABLE` | `UNKNOWN` | `SHAPE_FITS_EXISTING_NODE` |
| `unavailable(CONTEXT_UNREACHABLE)` | `UNAVAILABLE` | `NOT_APPLICABLE` | `UNKNOWN` | `CONTEXT_UNREACHABLE` |
| `unavailable(SHAPE_NO_LONGER_SUPPORTED)` | `UNAVAILABLE` | `NOT_APPLICABLE` | `UNKNOWN` | `SHAPE_NO_LONGER_SUPPORTED` |
| `unavailable(CAPACITY_UNAVAILABLE)` | `UNAVAILABLE` | `NOT_APPLICABLE` | `UNKNOWN` | `CAPACITY_UNAVAILABLE` |
| `unavailable(QUOTA_UNAVAILABLE)` | `UNAVAILABLE` | `NOT_APPLICABLE` | `UNAVAILABLE` | `SHAPE_FITS_EXISTING_NODE` |
| `unavailable(PROVIDER_OBJECT_CONFLICT)` | `UNAVAILABLE` | `NOT_APPLICABLE` | `UNKNOWN` | `PROVIDER_OBJECT_CONFLICT` |

`not_representable()` accepts exactly `SCOPE_CHANGED`,
`CONFIGURATION_CHANGED`, or `OFFER_IDENTITY_CHANGED` and stores no offer.

Expected absence and unsupported-shape results use these typed outcomes rather
than exceptions. Unexpected provider, credential, or programming failures retain
their existing typed exception behavior. Shadow mode records such a source
failure and leaves mutation on the legacy path. Authoritative mode never turns a
source failure into an implicit legacy placement or a different offer.

The offer source may consume the same bounded raw provider observation snapshot
as the legacy projection during shadow comparison. It must not call
`make_launchables_for_valid_region_zones()`, `_yield_zones()`, or construct its
offers from the already generated legacy candidates. Sharing raw read-only
observations avoids time-skew noise; independently projecting those observations
keeps the comparison non-tautological.

### V1 Offer and Envelope Schema

The in-memory offer uses frozen dataclasses, tuples, enums, canonical decimal
strings, and immutable provider payload values. It is never stored directly in a
cluster handle. `to_envelope()` produces a fresh dictionary containing only JSON
built-ins.

The V1 persisted envelope has this exact shape:

```json
{
  "schema_version": 1,
  "operation": "fresh_create",
  "actuation_kind": "direct_pod",
  "offer_id": "kubernetes:sha256:<64-lowercase-hex>",
  "observation_id": "sha256:<64-lowercase-hex>",
  "provider": "kubernetes",
  "scope": {
    "kind": "kubernetes_context_endpoint_identity_namespace_v1",
    "id": "sha256:<64-lowercase-hex>"
  },
  "resources": {
    "instance_type": "4CPU--16GB",
    "cpus": "4",
    "memory_gib": "16",
    "accelerators": [],
    "disk_tier": null,
    "network_tier": null,
    "placement_constraints_digest": null
  },
  "region": "context-name",
  "candidate_zones": [],
  "batching_scope": "context",
  "price": {
    "amount": "0.42",
    "basis": "node_hour",
    "currency": "USD"
  },
  "purchase_mode": "on_demand",
  "availability": "unknown",
  "observed_at": "2026-07-30T12:34:56Z",
  "ttl_seconds": 15,
  "revalidation_policy": "before_mutation",
  "evidence": {
    "reservation": "not_applicable",
    "quota": "unknown",
    "capacity": "shape_fits_existing_node",
    "requested_nodes": 1
  },
  "provider_payload": {
    "version": 1,
    "identity": {
      "rendered_pod_placement_fingerprint": "sha256:<64-lowercase-hex>",
      "service_account_identity_digest": "sha256:<64-lowercase-hex>"
    },
    "observation": {
      "capacity_evidence": "shape_fits_existing_node",
      "configuration_fingerprint": "sha256:<64-lowercase-hex>"
    }
  }
}
```

Accelerators are encoded as a list of
`{"name": <normalized-name>, "count": <positive-integer>}` objects sorted by
normalized name. Decimal quantities and prices are canonical non-exponent
strings, never JSON floats. Timestamps are UTC RFC 3339 values normalized to
`Z`. Object keys are sorted for canonical JSON; array order is significant
except where the schema explicitly requires sorting.

M2 V1 accepts only the derived offer-ID grammar. No pilot provider declares a
native offer-ID grammar. Adding one requires a later explicit policy and
schema-version design update rather than an inferred provider-name special
case. `offer_id` is `<provider>:sha256:<digest>` over a canonical object whose
keys and nesting are exactly:

```text
schema_version
provider
operation
actuation_kind
scope {kind, id}
region
candidate_zones
batching_scope
resources {
  instance_type, cpus, memory_gib, accelerators,
  disk_tier, network_tier, placement_constraints_digest
}
purchase_mode
provider_payload {version, identity}
```

Price, availability, observation time, TTL, revalidation policy, evidence, and
the provider payload `observation` object are excluded from the stable ID.
`observation_id` is `sha256:<digest>` over a canonical object whose keys and
nesting are exactly:

```text
offer_id
price {amount, basis, currency}
availability
observed_at
ttl_seconds
revalidation_policy
evidence {reservation, quota, capacity, requested_nodes}
provider_payload {observation}
```

Changing requested node count changes the observation ID through evidence but
does not change the per-placement stable ID.

`canonical_json_bytes_v1(value)` is exactly:

```python
json.dumps(
    value,
    ensure_ascii=False,
    sort_keys=True,
    separators=(',', ':'),
    allow_nan=False,
).encode('utf-8')
```

There is no trailing newline. Every string is already NFC-normalized before
this call. `NFC_V1` is fixed to
`unicodedata.ucd_3_2_0.normalize('NFC', value)`, never the interpreter's
default `unicodedata.normalize()`. Ingestion accepts a string only when it
equals that fixed normalization. It rejects every surrogate code point
`U+D800` through `U+DFFF` and every C0 or C1 control code point in the explicit
ranges `U+0000` through `U+001F` and `U+007F` through `U+009F`; control
validation does not consult a runtime Unicode category database. The parser
recomputes both digest preimages with this function and rejects a mismatch.
Cross-version golden fixtures lock both
`NFC_V1('\u0301\U00016ff0') == '\U00016ff0\u0301'` and
`NFC_V1('\u0301\U00016ff1') == '\U00016ff1\u0301'`; the left-hand input forms
are rejected as non-normalized and the right-hand forms are accepted.

The persistence-ingestion entry point is exactly the `from_json()` signature
above, where `serialized` is `str` or strict UTF-8 `bytes`; `bytearray` and
every other type are rejected. It uses `object_pairs_hook` to reject a duplicate
key before dictionary materialization and rejects JSON constants and floats.
`from_envelope()` accepts an already-materialized in-process dictionary only;
it cannot prove that upstream text had no duplicate keys and is not a
persistence-ingestion API. The `bulk_provision()` validation path calls
`to_envelope()`, canonical-serializes that fresh dictionary, and then calls
`from_json()` with the provider schema.

A V1 provider payload is constructed only through the injected provider
allowlist. Raw SDK responses, kubeconfigs, credentials, tokens, environment
variables, pod configuration, labels, annotations, or admission payloads are
never accepted as generic payload data. Suspicious secret-like keys are
rejected as defense-in-depth, but key-name filtering is not considered
redaction.

Secret-key matching is exact and recursive. Lowercase the printable ASCII key,
split it on one or more `_` or `-` characters, and reject if any segment is
`secret`, `password`, `passwd`, `token`, `credential`, `credentials`,
`kubeconfig`, `authorization`, or `cookie`; if any adjacent pair is `api_key`,
`access_key`, `private_key`, or `client_secret`; or if the unsplit lowercase
key is `apikey`, `accesskey`, `privatekey`, or `clientsecret`. Substrings do
not match, so `tokenizer`, `credentialed`, `monkey`, and `key_count` are
allowed by this defense-in-depth filter, subject to the provider allowlist.

The canonical provider payload is limited to 4 KiB and the full canonical
envelope to 16 KiB. Exactly 4,096 UTF-8 bytes is accepted for the complete
canonical provider payload and 4,097 is rejected. Exactly 16,384 UTF-8 bytes is
accepted for the full canonical envelope and 16,385 is rejected. V1 has these
closed per-field bounds:

| Field | V1 bound |
|---|---|
| `schema_version` | integer exactly `1` |
| `operation` | exactly `fresh_create` for a provisionable or persisted envelope; `plan_create` is process-local and cannot be enveloped |
| `actuation_kind` | exact closed enum; Kubernetes V1 envelopes require `direct_pod` |
| `provider` | 1 to 63 lowercase ASCII letters, digits, `.`, `_`, or `-`, starting and ending with a letter or digit |
| `offer_id` | `<provider>:sha256:` plus exactly 64 lowercase hexadecimal characters, with a total maximum of 256 ASCII characters |
| `observation_id`, scope ID, optional constraint digest, and provider identity digests | `sha256:` plus exactly 64 lowercase hexadecimal characters |
| scope kind and batching scope | 1 to 128 lowercase ASCII letters, digits, or `_`; Kubernetes V1 values are exact enums |
| instance type | 1 to 256 UTF-8 bytes after fixed `NFC_V1` validation, with the explicit control ranges forbidden |
| CPU and memory decimal strings | exact regex `(?:0|[1-9][0-9]{0,37})(?:\.[0-9]{0,17}[1-9])?`; canonical, nonnegative, and non-exponent |
| accelerators | at most 8 entries; normalized name 1 to 128 UTF-8 bytes; count integer 1 through 2,147,483,647 |
| disk and network tier | null or 1 to 64 lowercase ASCII letters, digits, `_`, or `-` |
| region and each zone | 1 to 1,024 UTF-8 bytes after fixed `NFC_V1` validation, with the explicit control ranges forbidden |
| candidate zones | at most 32 unique entries |
| price amount | the same exact decimal regex as CPU and memory |
| price basis, currency, purchase mode, availability, revalidation policy, and evidence values | exact closed enums; V1 currency is `OfferCurrencyV1.USD` |
| observed time | exactly 20 ASCII bytes in `YYYY-MM-DDTHH:MM:SSZ` form and a valid UTC datetime |
| TTL | integer 1 through 300 seconds; Kubernetes V1 emits exactly 15 |
| requested nodes | integer 1 through 10,000; the Kubernetes authoritative V1 subset requires exactly 1 |
| provider payload version | integer exactly `1` |

Each provider-payload object has at most 32 keys, each key is 1 to 64 printable
ASCII characters, and the combined `identity` and `observation` trees have at
most 64 keys and 128 array elements. Maximum nesting depth is four below either
payload root and each array has at most 32 elements. Payload strings are at most
1,024 UTF-8 bytes after fixed `NFC_V1` validation; integers are in the signed 64-bit
range; only strings, integers, booleans, nulls, bounded arrays, and bounded
objects are allowed. JSON floats are forbidden. Empty strings are allowed only
for a provider field whose allowlist explicitly declares them meaningful;
Kubernetes V1 declares none.

For these counts, each `identity` and `observation` root object is depth zero;
a directly nested container is depth one and a container at depth four is the
deepest accepted container. Root keys count toward the combined 64-key limit,
and every member of every array counts toward the combined 128-element limit.
The 4 KiB limit covers the complete canonical `provider_payload` object,
including `version`, `identity`, and `observation`.

V1 ingestion rejects unknown keys at every level, duplicate object keys before
dictionary materialization, non-JSON values, invalid normalization, nonfinite
or negative monetary values, invalid UTC timestamps, invalid enum values, and
values outside these bounds. A later shape change increments
`schema_version`; it does not silently extend V1.

The Kubernetes V1 parser narrows the generic enums to these exact values:

| Field | Accepted Kubernetes V1 values |
|---|---|
| scope kind | `kubernetes_context_endpoint_identity_namespace_v1` |
| batching scope | `context` |
| price basis | `node_hour` |
| currency | `USD` |
| purchase mode | `on_demand` |
| availability | `unknown`, `unavailable` |
| revalidation policy | `before_mutation` |
| reservation evidence | `not_applicable` |
| quota evidence | `unknown`, `unavailable` |
| capacity evidence | `shape_fits_existing_node`, `context_unreachable`, `shape_no_longer_supported`, `capacity_unavailable`, `provider_object_conflict` |

The injected Kubernetes payload schema has exactly these leaf paths:

| Payload root and field | Schema node |
|---|---|
| `identity.rendered_pod_placement_fingerprint` | `DIGEST` |
| `identity.service_account_identity_digest` | `DIGEST` |
| `observation.capacity_evidence` | `STRING`, allowed strings exactly `shape_fits_existing_node`, `context_unreachable`, `shape_no_longer_supported`, `capacity_unavailable`, `provider_object_conflict` |
| `observation.configuration_fingerprint` | `DIGEST` |

Both roots reject every other key and no string field permits an empty value.

Kubernetes V1 additionally requires an empty accelerator list, null disk and
network tiers, and an empty candidate-zone list. No sample value implicitly
extends these enums.

Payload-schema validation narrows only `provider_payload`; it is not allowed to
stand in for provider-wide envelope policy. The Kubernetes-owned leaf therefore
defines this exact post-validator:

```python
def validate_kubernetes_offer_v1(
    offer: PlacementOfferV1,
) -> PlacementOfferV1:
    ...
```

The validator returns the identical object after requiring provider
`kubernetes`; operation `PLAN_CREATE` or `FRESH_CREATE`; actuation kind
`DIRECT_POD`; scope kind
`kubernetes_context_endpoint_identity_namespace_v1`; batching scope `context`;
price basis `NODE_HOUR`; currency `USD`; purchase mode `ON_DEMAND`;
revalidation policy `BEFORE_MUTATION`; TTL 15; one requested node; empty
accelerators and candidate zones; null disk, network, and placement-constraint
tiers or digests; and one of exactly the six complete availability,
reservation, quota, and capacity tuples in the revalidation matrix above.
There is no cross-product between rows: in particular, quota `UNAVAILABLE`
requires capacity `SHAPE_FITS_EXISTING_NODE`, and every capacity-derived
unavailable value requires quota `UNKNOWN`. It also requires the allowlisted
`provider_payload.observation.capacity_evidence` to equal
`evidence.capacity.value`.

The Kubernetes source calls this validator after every generic `create()`,
`from_envelope()`, and `from_json()` result. Orchestration and
`bulk_provision()` call it again before placing any Kubernetes offer in an
actuation handoff. Generic parsing alone never establishes Kubernetes
eligibility.

An offer expires when
`now >= observed_at + ttl_seconds`. Provisioning revalidates an expired offer or
any offer whose policy is `before_mutation`. An unavailable revalidation returns
a typed outcome to orchestration. The source never silently substitutes another
region, zone batch, context, purchase mode, or resource shape.

### Kubernetes Pilot

Kubernetes is the first M2 source because it uses the new provisioner, represents
a Sky region as a Kubernetes context, and has no zone retry dimension. The first
authoritative subset is deliberately narrower than all Kubernetes support:

- a fresh cluster create, not reuse or restart;
- a direct Pod launch for an ordinary cluster, not any managed-jobs, Serve, or
  pool controller and not a high-availability Deployment/PVC launch;
- one node;
- CPU-only on-demand resources;
- no explicit disk tier, network tier, ephemeral-storage request, local disk,
  accelerator arguments, resource labels, resource volumes, FUSE requirement,
  or job-recovery mode;
- no nonempty `Resources.ports`;
- no volume mounts or storage mounts;
- no Kueue queue;
- no reservation;
- no configured Kubernetes autoscaler;
- an explicitly resolved, existing, non-default Kubernetes service account, so
  authoritative bootstrap does not create or patch shared RBAC resources;
- no resource priority or priority class;
- a context, cloud identity, and effective namespace that can be resolved before
  rendering, where that namespace already exists and its live Kubernetes UID is
  readable.

Inputs outside this subset return `NOT_REPRESENTABLE` and remain on the legacy
path until a later characterization and promotion commit explicitly adds them.
`PLAN_CREATE` may list offers only for read-only shadow comparison.
`FRESH_CREATE` is the only operation that may be revalidated or handed to
actuation. `REUSE` and `RESTART` return `UNSUPPORTED_OPERATION`.

The backend derives `OfferActuationKindV1` from the exact cluster name,
workload type, `Controllers.from_name()`, and
`controller_utils.high_availability_specified()` result that
`backend_utils.write_cluster_config()` uses. Only `DIRECT_POD` is eligible;
`CONTROLLER`, `HA_DEPLOYMENT`, and `UNKNOWN` return
`UNSUPPORTED_ACTUATION_KIND`. Before handoff, `_retry_zones()` also parses the
independently rendered cluster YAML and requires a Pod node config with no
`deployment_spec` or `pvc_spec`. `bulk_provision()` repeats that check, and
Kubernetes `run_instances()` rejects an authoritative envelope if its config
would take the Deployment branch. A disagreement between the request
classification and rendered YAML fails before provider mutation.

The resource classifier is also closed. It enumerates every field in the
current `Resources` version and accepts only the Kubernetes cloud, one concrete
context, no zone, the CPU/memory instance shape, ordinary image/container
selection, default OS-disk size, maximum-price filtering, autostop, and hooks.
Spot, accelerators, accelerator arguments, nonempty ports, explicit disk or
network tier including `NetworkTier.BEST`, nondefault ephemeral storage, local
disk, labels, resource volumes, FUSE, priority, priority class, and job recovery
return `UNSUPPORTED_RESOURCE_MODE` or `UNSUPPORTED_NETWORK_TIER` as applicable.
A new
`Resources` field is ineligible until this table and its frozen corpus are
updated. This prevents `_detect_network_type()` and other label-dependent
render paths from running inside the V1 subset while the snapshot deliberately
stores no raw node labels. Rejecting nonempty ports also makes the backend
`_open_ports()` branch unreachable before READY; port Services and Ingresses
remain entirely on the legacy path until their full UID inventory is modeled.

Eligibility is closed over the effective configuration, not inferred from a
small set of presence flags. A new
`classify_kubernetes_offer_config_v1()` helper freezes
`skypilot_config.to_dict()`, the explicit `OfferRequestV1.workspace`,
`Resources.cluster_config_overrides`, the sorted registered Kubernetes-property
names, every registered queue-key path, and current Kubernetes provisioner and
template-override ownership once per observation. From that frozen input it
reads every applicable raw scope: global `kubernetes`, workspace `kubernetes`,
each selected `context_configs.<context>` block, and resource overrides. It
preserves the existing precedence, then accepts only these semantic V1
properties:

| Effective property | Allowed V1 value and representation |
|---|---|
| `allowed_contexts` | `all` or a bounded list containing the candidate context; used to construct the candidate set |
| `namespace` | a nonempty resolved namespace whose live UID is readable; encoded only through the opaque scope digest |
| `autoscaler` | absent; any explicitly configured value, including GKE and optimistic autoscalers, is outside V1 |
| `remote_identity` | required and resolves to one existing non-default Kubernetes service-account name; its nonsecret digest enters provider placement identity |
| `pricing` | absent or the built-in `_PRICING_SCHEMA`; the CPU and memory components are encoded in offer price |
| `provision_timeout` | absent or an integer accepted by the existing schema; execution-only and excluded from placement identity |
| workspace `disabled` | absent or exactly `false` |
| `context_configs` | only as the container for the selected context's properties above |

Every other built-in Kubernetes property is outside the authoritative V1
subset when explicitly present at an applicable scope, even when its value is
null, false, empty, or equal to a current default. This includes
`allowed_nodes`, `networking`, `ports`, `pod_config`, `custom_metadata`,
`high_availability`, `kueue`, `quota`, `dws`,
`post_provision_runcmd`, `apt_mirrors`, `set_pod_resource_limits`,
`auto_mounts`, and `enable_docker`. Any property registered through
`register_kubernetes_property()`, any queue spelling registered by a plugin,
and any unknown client-pass-through property is also outside V1 whenever
present. The classifier returns
`NOT_REPRESENTABLE(CUSTOM_PLACEMENT_CONFIG)` before reading or logging its
value. Adding an allowed property requires a schema-versioned design and frozen
characterization cases; plugin registration alone can never expand
authoritative eligibility.

A registered property name that collides with any built-in or structural
Kubernetes key fails closed even when that property is absent, because the
current last-registration-wins schema registry can replace the built-in schema.
Initial V1 also requires both
`provision.get_registered_provisioner('kubernetes') is None` and
`provision.get_provisioner_template_override('kubernetes') is None`; a plugin
provisioner or template can change placement without any Kubernetes config
property. M2 adds a read-only
`provision.get_builtin_implementation_fingerprint_v1('kubernetes')` helper that
compares the resolved built-in module object and all seven
`InstanceLifecycleV1` callable objects with their import-time captured
identities. Whole-module or attribute monkeypatches, including supported M1
test/downstream seams, produce `CUSTOM_PLACEMENT_CONFIG` rather than silently
entering authoritative mode. The implementation fingerprint contains only
module, qualname, code-digest, and match booleans, never `id()` values, and is
recaptured during revalidation.

The classifier hashes only the secret-free normalized allowed values, explicit
request-workspace digest, sorted registry names and queue paths, registration
and template ownership, and the built-in implementation fingerprint into
`configuration_fingerprint`, then discards the raw configuration copy.
Revalidation captures them again and returns
`NOT_REPRESENTABLE(CONFIGURATION_CHANGED)` if the fingerprint differs.

`KubernetesPlacementObservationV1` is the frozen Kubernetes snapshot. It stores
the exact candidate-context order returned by the current legacy enumeration
and a tuple of per-context observations. Each per-context observation contains
the context name; a nonsecret endpoint fingerprint; a nonsecret cloud-identity
digest; the effective namespace and live `Namespace.metadata.uid`; configured
price; the resolved non-default service-account name, its live
`ServiceAccount.metadata.uid`, and a digest over namespace, name, and UID; a
provider-order tuple of normalized node records containing `status.capacity`,
`status.allocatable`, and the exact existing `is_ready()` boolean; a bounded
tuple of CPU avoid-accelerator label keys derived by consuming exact provider
node and label-map order, with no raw labels retained; the configuration
fingerprint; and the closed eligibility result above. A separately sorted
projection of the node records is used only for offer ordering and identity.
The endpoint fingerprint hashes only the
normalized API-server scheme, host, port, path, and CA bundle digest from the
loaded client configuration. Userinfo, query strings, client certificates,
keys, tokens, exec-plugin arguments, environment variables, and raw kubeconfig
data are forbidden inputs.

Candidate enumeration and selected-context clients come from one private,
capture-scoped kubeconfig load session. The session captures the kubeconfig
path, merged config tree, current context, context names, in-cluster
availability, and in-cluster name once. A single pure
`resolve_kubernetes_allowed_contexts()` policy owner accepts those captured
facts, the effective `allowed_contexts` value, and captured environment-option
booleans. Both `Kubernetes.existing_allowed_contexts()` and the offer source
call that owner. The live wrapper retains the historical warning behavior;
the pure function returns both selected and skipped names without logging. It
deliberately preserves the legacy set-derived order until a separately
reviewed behavior change replaces it.

Effective configuration precedence also has one owner. A pure
`get_effective_workspace_region_config_from_snapshot()` helper applies the
same resource-override, explicit-workspace, context, and global precedence as
`get_effective_workspace_region_config()`. The live getter delegates to it;
the offer classifier calls it against the copied config. The classifier may
still inspect raw applicable scopes to reject unknown or forbidden keys, but
it must not independently decide an allowed property's value.

Selected clients are constructed from the retained config tree, so a changed
kubeconfig file, environment variable, active workspace, or same-name context
cannot retarget observation after enumeration. Kubeconfig users containing
`exec` or `auth-provider` fail closed before upstream `KubeConfigLoader` can
execute or log them; a bounded, scrubbed credential implementation is required
before either mode becomes eligible. Endpoint and CA identity are frozen while
ordinary external `tokenFile`, client-certificate, and client-key rotation
remains live. An external token file's final path component is opened without
following symbolic links, must be a regular file, and is read through a 1 MiB
cap on every authorization lookup; upstream unbounded token-file loading is
bypassed. Its UTF-8 text, including leading and trailing whitespace, is used
exactly. A failed refresh clears the prior authorization value and permanently
disables that target's token-file refresh rather than reusing a stale token.
Inline token data remains frozen with the captured config tree. The returned
target owns its client and temporary CA and has an idempotent `close()` that
removes both immediately even if the closed target remains referenced,
including Kubernetes-client-version-specific refresh callables that capture a
loader or token path. Concurrent `close()` calls are linearizable: every
caller returns only after that one cleanup has completed. Kubeconfig capture
also contains and detaches every `BaseException` raised after credential data
has been loaded. Ordinary invalid-config exceptions yield the fixed empty
inventory, while control exceptions retain their type and are re-raised only
from a credential-free outer boundary. A finalizer is fallback only. The
credential-bearing session and target never enter the snapshot, an offer, a
digest, logs, or Datadog.

The snapshot contains no kubeconfig, credential, token, full node object, pod
configuration, raw node label map, label value, annotation, or admission
payload. Both the legacy shadow
adapter and offer source project from this exact value. The capture preserves
provider node order because the legacy no-fit reason is order-sensitive when
nodes tie on maximum CPU. The comparison-only legacy adapter consumes that
order exactly. The offer source sorts a separate normalized node projection,
so provider response or future-completion order cannot affect offer ordering.
The comparison-only legacy adapter also preserves the captured legacy context
order; the offer source sorts by normalized context identity. The comparator
records order and winner differences separately. Neither projection replaces
or reorders the real legacy candidate list in shadow mode, which is important
because the current `allowed_contexts: all` path passes through a set.

One observation accepts at most 256 candidate contexts, 10,000 node records per
context, 64 MiB of decompressed node-list input across all contexts, 64 nested
JSON containers, 256 registered Kubernetes-property names, 256 registered
queue paths, and 256 resulting offers. Context, node-name, resource-value,
registry-name, and queue-path strings have explicit UTF-8 byte bounds.
Transient node-label keys are at most 1,024 UTF-8 bytes and values at most 256
KiB; neither is retained. The derived CPU avoid-accelerator tuple has at most
16 keys of at most 317 UTF-8 bytes each. An `ijson` counting stream enforces
the aggregate byte budget while parsing and projects each node directly into
`KubernetesNodeResources`; it never constructs or retains a raw `V1Node`, raw
label map, label value, annotation, or unrelated provider field. The source
requires exactly one root collection-metadata map, checks its pagination
fields, per-field bounds, and projected lengths, never truncates, and returns
`NOT_REPRESENTABLE(OBSERVATION_LIMIT_EXCEEDED)` on any overflow. Encoded HTTP
content length is never used as decoded EOF when a content decoder can retain
buffered output; the byte cap and EOF probe apply to decompressed bytes.
Provider completion order cannot decide which entries survive because overflow
rejects the whole observation. If an otherwise valid response has unknown
length and consumes its exact accepted-byte limit, the reader performs at most
one discard-only one-byte EOF probe for that context. Therefore the hard
physical read ceiling is 64 MiB plus 256 bytes, while accepted and retained
response content remains capped at 64 MiB. A nonempty probe rejects the whole
observation.

Readiness and accelerator-family selection remain single-owner policies.
`_transition_kubernetes_node_readiness()` owns the first-`Ready` condition and
exact `status == 'True'` decision used by both `V1Node.is_ready()` and the
streaming projection. `_GPULabelFormatterSelector` owns formatter priority,
first-nonempty matching, and value validation used by both
`detect_gpu_label_formatter()` and the streaming projection. The observation
path retains only one tri-state decision per formatter and registers no
invalid-value callback, so the shared policy does not retain label values.

Capture has an aggregate monotonic deadline and a fixed worker bound. Each
context writes to its original candidate index, so completion order cannot
change offers. Deadline or budget exhaustion cancels pending work, closes every
opened target, discards every partial result, and returns one typed whole-capture
failure. A partial context set is never published.

The legacy projection applies the recorded readiness filter and then uses
`status.capacity`, not allocatable resources, because the existing
`check_instance_fits()` contract intentionally tests total Ready-node capacity
and leaves changing free capacity to the scheduler. The rendered-request
projection applies the same recorded readiness filter before the existing
allocatable clamp. The frozen corpus proves the snapshot-backed legacy adapter
and clamp return the same results and reasons as the existing helpers.

For an eligible request, `region` is the selected context and
`candidate_zones` is empty. The opaque scope ID hashes canonical JSON containing
the context name, endpoint fingerprint, cloud-identity digest, effective
namespace, and live namespace UID. A shared Kubernetes scope helper derives the
identity digest from the same normalized kubeconfig cluster/user or in-cluster
identity used by `Kubernetes.get_identity_from_context_name()`, without storing
the raw identity. Pure adaptor-layer normalizers own the historical
cluster/user/namespace and in-cluster identity wire shapes. Exact-target
construction, `Kubernetes.get_identity_from_context()`, and
`in_cluster_identity()` delegate to those normalizers, including the existing
default-namespace and underscore behavior. `REQUIRE_FRESH` reloads that
identity instead of reusing a credential or context cache. Only the scope
digest is stored; the raw endpoint, context identity, namespace, and UID are
not stored in the provider payload.

Kubernetes V1's allowlisted provider-payload `identity` object contains exactly
`rendered_pod_placement_fingerprint` and
`service_account_identity_digest`. The latter hashes canonical JSON containing
the effective namespace, resolved service-account name, and live UID.
`rendered_pod_placement_fingerprint` covers normalized resource
requests for every regular and init container, Pod overhead and Pod-level
resources, after the existing allocatable clamp, plus every hard scheduling
input: node selector, required node affinity and anti-affinity, scheduler,
runtime class, priority class, `DoNotSchedule` topology spread, tolerations,
resource claims, volume-binding constraints, and the resolved service-account
name.

S2 does not maintain a second Pod renderer. Production first resolves external
facts, including runtime-class existence, allowed-node policy, and Docker-cache
PVC identity. One pure `pod_spec.finalize_pod_spec()` owner then copies the
base Pod and applies every pre-admission scheduling mutation: role metadata,
runtime class, Docker cache volume, multi-node affinity, TPU and GPU
tolerations, and allowed-node affinity. `_create_pods()` and offer projection
both call this owner. Provider reads, waits, adoption, PVC checks, and the final
API create remain outside it. Inputs outside the V1 subset still use the same
function in production, while V1 supplies the characterized single-node CPU
facts.

#### S2a.2 shared built-in Kubernetes base-Pod materialization

S2a.2 replaces the duplicated built-in Kubernetes base-Pod materialization and
merge boundary. It does not make an arbitrary full cluster template pure. That
broader boundary
would pull credential paths, plugin callbacks, and unrelated provider objects
into placement capture and would still be superseded by reuse. Instead, one
authoritative built-in fragment, one initial recomposed render and
pre-combination parse owner, and one pure post-parse owner remove production
merge duplication without claiming a safe offer-time renderer. A temporary
digest-locked monolith remains only as a downstream compatibility mirror until
its removal gate closes.

The source boundary is exact. At the S2a.1 merge baseline
`06ce2213526652621d7d7ae37137221f16b798c4`, the semantic `node_config`
fragment is the UTF-8 byte range `[10652, 78464)` of
`sky/templates/kubernetes-ray.yml.j2`, current lines 277 through 1715. It is
67,812 bytes and has SHA-256
`09ea5d743a09286649c56f26c5b737764b81730b90fefae2bd561e0707a72e04`.
Implementation copies those exact bytes into the new authoritative
`sky/templates/kubernetes-ray-node-config.yml.j2`; runtime never slices a
template by a line or byte range. During the compatibility window the new
authoritative outer is
`sky/templates/kubernetes-ray-outer.yml.j2`. It retains
`head_node_type`, `available_node_types`, and `ray_head_default`, and replaces
the old inline body with exactly one reserved source-marker line:

```jinja
{{ skypilot_kubernetes_node_config_fragment_v1 }}
```

That literal marker line is 50 UTF-8 bytes including LF. The expected physical
outer source is 16,028 bytes with SHA-256
`3f9343f8ff289711d931af2915391338ac628d30d96fb10e66b4808578eadcd1`.
Recomposing it with the raw fragment must recover the current 83,790-byte
monolith at SHA-256
`988b6d5e2afd7e96b3a6d7e0091c661a3d05d5a61d23fd7efa138ab75d55a6f8`.
The existing `sky/templates/kubernetes-ray.yml.j2` remains byte-for-byte at
that monolith digest only as a temporary compatibility mirror for a replacement
renderer that opens its received `template_ref` directly. It is never edited
independently, and build and test gates verify that it equals the recomposed
source. Exact built-in runtime rendering never reads the mirror: it validates
the authoritative outer, fragment, and recomposed digests only. After the
downstream gate closes, a separate removal commit replaces that path atomically
with the validated outer, removes the temporary `-outer` path, and makes the
facade compose directly from the final outer plus fragment.

One `compose_builtin_kubernetes_template_source(outer_text, fragment_text)`
owner
requires that exact marker once, validates the outer and fragment digests, and
replaces the complete raw marker line with the raw fragment bytes. The resulting
UTF-8 source must equal the 83,790-byte monolith and exact digest above before
Jinja compilation. The ordinary built-in path renders that one recomposed
source once and safe-parses the complete rendered YAML once at the existing
pre-combination parse point. It never renders or parses the outer and fragment
independently. Existing later auth, restore, name-read, hashing, and file-mount
optimization parses remain outside this invariant. Consequently anchors,
aliases, directives, plain-scalar injection, Jinja evaluation order, YAML
source coordinates, and initial render/parse error precedence remain those of
the current monolith.

An initial behavior-preserving source-composer staging commit adds the outer
and fragment while retaining the compatibility monolith. The established
`common_utils.fill_template(template_ref, variables, output_path)` callable and
its exact three arguments remain the rendering facade. When the logical
reference is the built-in Kubernetes template, the built-in facade selects the
authoritative temporary `-outer` path, composes it with the fragment, and passes
the validated result to the current single Jinja render instead of opening the
mirror. A wrapper that delegates to the facade receives the same three
arguments and the facade composes internally; a replacement that opens
`template_ref` sees the unchanged compatibility monolith. Other templates
retain their current read and render path. This makes the composer the only
authoritative built-in source owner while leaving mutable Pod and metadata
combination authoritative, and is deployed before strict-merge shadowing
begins. The validated mirror is temporary compatibility data, not a second
independently editable source owner.

The initial pre-combination parse wrapper preserves
`yaml_utils.safe_load()` fallback behavior
and returns the loader class actually used together with the parsed object. An
initial `CSafeLoader` `AttributeError` flips the same process flag and retries
once with `SafeLoader`; an already-fallen-back process uses `SafeLoader`
directly. The complete renderer identity records that returned loader and the
current Jinja2 and PyYAML versions without triggering a second initial parse.

"One initial parse" never means one parse for the complete writer request.
S2a.2 retains every later YAML parse and serialization at its current call
site. For dry run, and for non-dry paths whose first file-mount optimization
attempt succeeds, the exact Kubernetes/SSH parse stages and top-level
serialization stages are:

| Path | Parse stages in order | Top-level serialization stages in order |
| --- | --- | --- |
| dry run | initial rendered `safe_load`; deterministic-hash `read_yaml` | combined-object `dump_yaml`; deterministic-hash `dump_yaml_str` |
| non-dry, no restore | initial rendered `safe_load`; auth `read_yaml`; restored-name `read_yaml`; deterministic-hash `read_yaml`; file-mount optimization `read_yaml`; post-optimization usage-redaction `read_yaml_all` | combined-object `dump_yaml`; auth `dump_yaml`; deterministic-hash `dump_yaml_str`; file-mount optimization `dump_yaml` |
| non-dry restore | the no-restore sequence plus `_replace_yaml_dicts` `safe_load` of new YAML and old YAML between auth and restored-name read | the no-restore sequence plus `_replace_yaml_dicts` `dump_yaml_str` between auth and deterministic hash |
| non-dry managed-image restore | the restore sequence plus `_restore_managed_container_image_fields` `safe_load` of fresh and restored YAML before restored-name read | the restore sequence plus its `dump_yaml_str` before deterministic hash |

Thus the ordinary successful totals are two parse operations for dry run, six
for non-dry without restore, eight for restore, and ten for managed-image
restore. The final non-dry operation is `read_yaml_all()` through
`read_yaml_all_str()` and `safe_load_all()` for usage redaction. Because every
`yaml_utils.dump_yaml()` internally calls the public `dump_yaml_str()`, a
wrapper on the latter also observes nested calls. Before file-mount retries,
the exact successful serialization wrapper totals are one `dump_yaml()` and
two total `dump_yaml_str()` calls for dry run; three and four for non-dry
without restore; three and five for restore; and three and six for managed-image
restore, respectively. The serialization column lists top-level stages, not
all nested wrapper invocations.
The existing hash exception handling remains, and each current
`_optimize_file_mounts()` retry retains its own `read_yaml` and successful
`dump_yaml`. Each parse stage identifies the public entry point reached by the
writer; nested helpers retain their current calls, and the one-time C-extension
fallback can make two loader attempts inside one parse operation. No later
parse is reused, memoized, or routed through the base-Pod owner.

The complete rendered cluster object remains an impure process-local assembly
result. Credential paths, plugin objects, and unrelated provider fields never
enter the immutable base-Pod input, offer payload, or renderer identity. After
the initial full pre-combination parse, the assembler freezes only the built-in `node_config`
subtree and already-resolved effective Pod config, and passes those detached
values to the pure owner below. S2a.2 is production-only. S2b may neither invoke
the full assembly nor independently render or parse the fragment. Authoritative
offer projection remains blocked until a separate exact design proves a bounded
source-safe projection that reads no credential, private-key, logging-agent,
plugin, or unrelated outer-template input and introduces no second policy
owner.

The current fragment has exactly these 71 context-visible Jinja names, sorted
here as the closed V1 contract:

```text
accelerator_count
avoid_label_keys
cluster_name_on_cloud
conda_installation_commands
cpus
disk_size
ha_recovery_log_path
high_availability
image_id
k8s_acc_label_key
k8s_acc_label_values
k8s_apt_mirrors
k8s_automount_sa_token
k8s_cpu_limit
k8s_docker_buildkit_image
k8s_docker_dind_image
k8s_efa_count
k8s_enable_docker_all
k8s_enable_docker_build
k8s_enable_flex_start
k8s_enable_gpudirect_rdma
k8s_enable_gpudirect_rdma_a4
k8s_enable_gpudirect_tcpx
k8s_enable_gpudirect_tcpxo
k8s_enable_oci_roce
k8s_env_vars
k8s_ephemeral_storage
k8s_ephemeral_storage_limit
k8s_fuse_device_required
k8s_fusermount_setup_command
k8s_fusermount_shared_dir
k8s_high_availability_deployment_run_script_dir
k8s_high_availability_deployment_setup_script_path
k8s_high_availability_deployment_volume_mount_name
k8s_high_availability_deployment_volume_mount_path
k8s_high_availability_restarting_signal_file
k8s_high_availability_storage_class_name
k8s_host_network
k8s_ipc_lock_capability
k8s_kueue_local_queue_name
k8s_max_run_duration_seconds
k8s_memory_limit
k8s_namespace
k8s_network_type
k8s_resource_key
k8s_service_account_name
k8s_spot_label_key
k8s_spot_label_value
k8s_topology_label_key
k8s_topology_label_value
labels
memory
node_id
num_nodes
original_user
preemption_hook_timeout
priority_class
ray_dashboard_port
ray_head_start_command
ray_installation_commands
ray_port
ray_worker_start_command
runcmd
sky_python_cmd
sky_unset_pythonpath_and_set_cwd
skypilot_ray_port
user
uv_installation_commands
volume_mount_rw_paths
volume_mounts
workspace
```

Their canonical newline-delimited name digest is
`458bb234308e1ac0afc20945c24c10e524d29dc813253347943a9af8184819fe`.
The test that derives this set removes Jinja default globals before parsing, so
a future use of a shadowable global such as `range` cannot escape the contract.
A source, name-set, Jinja, PyYAML-loader, merge-contract, or validation-contract
change produces a new production renderer identity and invalidates the
source-safe projector's parity evidence. S2b remains ineligible until that
separate projector and differential corpus are reviewed against the new
identity. The initial characterized runtime identity is Jinja2 3.1.6, PyYAML
6.0.3, and `CSafeLoader`; production can continue to support the repository
dependency range. The base-render identity never enters an offer payload,
provider identity, or stable offer ID.

The pure owner has this conceptual interface:

```python
@dataclasses.dataclass(frozen=True)
class FrozenRenderSequenceV1:
    kind: typing.Literal['list', 'tuple']
    values: tuple['FrozenRenderValueV1', ...]


@dataclasses.dataclass(frozen=True)
class FrozenRenderMapV1:
    # Unique string keys in original insertion order.
    items: tuple[tuple[str, 'FrozenRenderValueV1'], ...]


@dataclasses.dataclass(frozen=True)
class FrozenVolumeInfoV1:
    name: str
    path: str
    volume_name_on_cloud: str | None
    volume_id_on_cloud: str | None
    sub_path: str | None
    volume_type: str | None
    host_path: str | None


@dataclasses.dataclass(frozen=True)
class FrozenDateV1:
    year: int
    month: int
    day: int


@dataclasses.dataclass(frozen=True)
class FrozenDateTimeV1:
    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int
    microsecond: int
    utc_offset_microseconds: int | None
    fold: int


FrozenRenderValueV1 = typing.Union[
    None,
    bool,
    int,
    float,
    str,
    bytes,
    FrozenDateV1,
    FrozenDateTimeV1,
    FrozenRenderSequenceV1,
    FrozenRenderMapV1,
    FrozenVolumeInfoV1,
]


@dataclasses.dataclass(frozen=True)
class KubernetesBaseRendererIdentityV1:
    schema_version: typing.Literal[1]
    fragment_sha256: str
    outer_sha256: str
    monolith_sha256: str
    binding_names_sha256: str
    jinja2_version: str
    pyyaml_version: str
    yaml_loader: typing.Literal['CSafeLoader', 'SafeLoader']
    merge_contract_version: typing.Literal[1]
    source_composition_contract_version: typing.Literal[1]
    custom_metadata_contract_version: typing.Literal[1]
    pod_validation_contract_version: typing.Literal[1]


class KubernetesRenderErrorCodeV1(enum.Enum):
    INVALID_INPUT = 'invalid_input'
    THAW_FAILED = 'thaw_failed'


@dataclasses.dataclass(frozen=True)
class KubernetesBasePodRenderInputV1:
    schema_version: typing.Literal[1]
    renderer_identity: KubernetesBaseRendererIdentityV1
    base_node_config: FrozenRenderMapV1
    effective_pod_config: FrozenRenderMapV1


@dataclasses.dataclass(frozen=True)
class FrozenOptionalNodeConfigFieldV1:
    present: bool
    value: FrozenRenderValueV1 | None


@dataclasses.dataclass(frozen=True)
class KubernetesBasePodRenderResultV1:
    schema_version: typing.Literal[1]
    renderer_identity: KubernetesBaseRendererIdentityV1
    node_config: FrozenRenderMapV1
    base_pod: FrozenRenderMapV1
    pvc_spec: FrozenOptionalNodeConfigFieldV1
    deployment_spec: FrozenOptionalNodeConfigFieldV1


def build_kubernetes_base_pod_spec(
    render_input: KubernetesBasePodRenderInputV1,
) -> KubernetesBasePodRenderResultV1:
    ...
```

An invalid frozen envelope or impossible thaw raises
`InvalidCloudConfigs` with exact value-free message
`Invalid Kubernetes render: <code>.` and no cause, where `<code>` is one of the
two closed enum values above. Production-value merge exceptions are not
translated into this contract category.

`FrozenRenderMapV1` is deliberately not `FrozenJSONDict`: sorting keys would
change YAML serialization and the cluster config hash. Freeze and thaw perform
recursive detached copies, retain list-versus-tuple and insertion order, reject
duplicate or non-string map keys, reject non-finite floats and arbitrary
objects, and snapshot every mutable `VolumeInfo`. They preserve all scalar
types currently produced by the safe-YAML config path, including `bytes`,
`datetime.date`, naive `datetime.datetime`, and fixed-offset aware
`datetime.datetime`. Every accepted container, scalar, and `VolumeInfo` has the
exact closed runtime type, rather than a subclass with user methods. An aware
datetime is accepted only when
`type(value.tzinfo) is datetime.timezone` and its built-in `tzname(None)` equals
the canonical name produced by `datetime.timezone(value.utcoffset())`; a custom
timezone name or any other `tzinfo` takes compatibility. The closed exact type
check makes both built-in calls non-user code. The frozen value stores the full
signed offset in microseconds, so fractional-second offsets round-trip. Date and
datetime thawing recreates the exact safe-YAML value, including microseconds,
offset, and `fold`; it never calls a user `tzinfo`. The impure assembler
validates the strict UTF-8 outer, fragment, and recomposed-source sizes and
digests in the complete renderer identity before rendering. No repeated
alias-emitting identity found by one traversal of the parsed full object or by
the subsequent detached render-config traversal crosses the strict boundary.
The full object is traversed exactly once with its active `node_config` subtree
in place; that subtree is never registered as a second root. The classifier
derives the frozen base-node value from that in-place path, then traverses the
detached effective Pod config with the same seen-identity set so a genuine
cross-root alias still fails closed.

An exact `VolumeInfo` type is not sufficient by itself because the current
dataclass is not slotted. Strict admission calls `vars(value)` and requires its
keys to be exactly `name`, `path`, `volume_name_on_cloud`,
`volume_id_on_cloud`, `sub_path`, `volume_type`, and `host_path`. `name` and
`path` must have exact runtime type `str`; each other field must have exact
runtime type `str` or be `None`. An extra instance attribute or any other field
value selects mutable compatibility. The freezer neither silently drops extra
programmatic state nor traverses an arbitrary object stored in a declared
field.

Freeze validation is iterative, identity-based, cycle-safe, and capped at
100,000 graph edges and 64 nested containers. The edge cap matches the existing
safe-YAML graph check. It does not call user equality, hashing, iteration
protocols outside the closed built-in types, `repr`, or serialization.
Overflow, a cycle, or a repeated identity for any alias-emitting supported
value selects the fixed compatibility reason without retaining a partial
snapshot. Identity tracking includes maps, lists, tuples, `VolumeInfo`, dates,
and datetimes; it excludes only these PyYAML alias-ignored primitives: `None`,
strings, bytes, booleans, integers, and floats. This preserves anchors for
repeated nonempty tuple, date, and datetime identities by keeping those
identity-admitted graphs on the private typed-mutable path. PyYAML also ignores aliases for exact empty tuples;
the V1 classifier conservatively identity-tracks `()` anyway, so repeated empty
tuple identity also takes compatibility rather than depending on that
representer special case.

Safe-YAML graphs outside that closed grammar, including sets, cycles, shared
aliases whose identity would change dump output, non-string mapping keys, and
custom tagged or programmatic objects, take an explicit
`UNFREEZABLE_KUBERNETES_RENDER_INPUT` private typed-mutable path after identity
admission. That path is always authoritative-offer-ineligible. It emits only the fixed reason code
through existing Datadog observability, never a type name, key, value, or
rendered cluster. Shadow inventory must prove which officially accepted
shapes remain before the strict owner cuts over; the typed-mutable path has its
own removal gate below. Public mutable compatibility does not run this
classifier.

The same diagnostic applies when the initial full pre-combination parse yields a
graph outside the frozen grammar, a cycle, or any actual repeated
alias-emitting identity during that single full-object traversal or the
subsequent detached effective-Pod traversal. Merely reaching the embedded
`node_config` at its ordinary path is not a repeated identity. Input
classification selects mutable combination before the pure owner; a
parsed-graph classification reuses the already rendered and parsed full object.
Neither case performs a second Jinja render or YAML parse. The compatibility
result cannot feed offer projection.

`UNFREEZABLE_KUBERNETES_RENDER_INPUT` is an internal render-dispatch diagnostic,
not a new `OfferReasonCodeV1`. If offer classification can reach such an input,
it returns `NOT_REPRESENTABLE(CUSTOM_PLACEMENT_CONFIG)` before rendering.

The private typed-mutable Pod-combination adapter consumes the same resolved mutable Pod
config that the strict freezer inspected plus the initially parsed full object. If
selected, it performs the first full-object deep copy before merging. After
either Pod path succeeds, the assembler performs the second full-object deep
copy before invoking the metadata projector. The one ownership-transfer
metadata applicator then consumes that later resolved metadata and the solely
owned post-Pod copy. Together these paths preserve the current two deep copies,
shared merge-algorithm semantics, recursive call topology, alias graph,
arbitrary supported objects, and declared error ordering. Every
identity-admitted private base, config, typed-mutable, and metadata path uses
the factored closed implementation; only public mutable compatibility invokes
the late-bound `merge_k8s_configs()` facade. The adapter and applicator perform no ambient config,
workspace, context, resource-override, template, or YAML read. Freeze
classification changes only the current stage's combination dispatch or
detached metadata representation, never selected values or source rendering.

The ordinary full variable mapping retains current presence semantics for each
of the 71 fragment-visible names. An absent key remains absent from Jinja's
context and is distinct from a present key whose value is `None`; no owner
materializes defaults. In the current built-in producer, `k8s_cpu_limit`,
`k8s_docker_buildkit_image`,
`k8s_docker_dind_image`, `k8s_ephemeral_storage`,
`k8s_ephemeral_storage_limit`, `k8s_memory_limit`, and
`preemption_hook_timeout` can be absent. The first, ephemeral-storage, its
limit, and preemption timeout are explicitly guarded by `is defined`.
`node_id` is intentionally absent and appears only in a source comment.
Docker image bindings may be absent only with both enable flags false, and
CPU and memory limit presence remains correlated. `k8s_apt_mirrors: None`
retains built-in mirrors while `[]` disables them; freeze and thaw must not
collapse those states. The derived 71-name contract is static evidence and a
renderer-identity gate, not a second offer-owned variable mapping.

The owner performs the following operations in order:

1. Validate the schema, complete renderer identity, frozen grammar, and
   detached-value invariants.
2. Thaw fresh order-preserving base-node and effective-Pod-config values.
3. Merge the already-resolved effective Pod config into `node_config` with the
   existing `merge_k8s_configs()` semantics.
4. Freeze the complete `node_config` with `pvc_spec` and `deployment_spec`
   retained. Independently copy the base Pod and remove those two keys only from
   that copy. Project each removed field with an explicit presence bit and its
   exact frozen safe-YAML value, so absent, present-null, mapping, scalar, and
   sequence remain distinct. Return no aliases. Validation is deliberately not
   part of this pure owner.

An explicit Kubernetes cloud-variable core executes the existing cloud-specific
steps in their current order. At the current host-network decision point, and
not earlier, it captures one raw config, override, cloud, and context snapshot.
It immediately projects effective Pod config exactly once, derives configured
`hostNetwork` and related probe environment values, and returns the snapshot,
cloud variables, and detached projection. This preserves any config reference
replacement completed by a generic-prefix or earlier cloud step before the
first current Pod read. A shared
deploy-variable orchestration owner retains the exact
`Resources.make_deploy_variables()` sequence: generic prefix, cloud callback at
the current call slot, then generic suffix. Production supplies the
explicit Kubernetes core as that callback; all other providers use their
existing callback in the same slot. Generic-prefix and earlier Kubernetes
failures therefore retain precedence over Pod resolution.

The orchestration returns a private identity-carrying
`DeployVariableAssemblyV1` with the final template-variable mapping and an
optional `KubernetesDeployVariableProjectionV1`. Their conceptual interface is:

```python
@dataclasses.dataclass(frozen=True)
class KubernetesDeployVariableProjectionV1:
    schema_version: typing.Literal[1]
    render_config_snapshot: 'KubernetesRenderConfigSnapshotV1'
    effective_pod_config: dict[str, typing.Any]


@dataclasses.dataclass(frozen=True)
class KubernetesPrivateDeployContextV1:
    schema_version: typing.Literal[1]
    writer_attempt_token: object
    resolved_owners: 'ResolvedKubernetesRenderOwnersV1'


@dataclasses.dataclass(frozen=True)
class KubernetesCloudDeployVariableResultV1:
    schema_version: typing.Literal[1]
    cloud_specific_variables: dict[str, typing.Any]
    kubernetes_projection: KubernetesDeployVariableProjectionV1 | None


@dataclasses.dataclass(frozen=True)
class DeployVariableAssemblyV1:
    schema_version: typing.Literal[1]
    template_variables: dict[str, typing.Any]
    kubernetes_projection: KubernetesDeployVariableProjectionV1 | None
```

The private call protocol is local to one production config write. The exact
built-in `_retry_zones()` caller resolves
`backend_utils.write_cluster_config`, requires its import-time built-in
identity, creates an exact `object()` token, and invokes that same checked
reference with a private keyword-only writer token whose default is `None`.
Replacements receive only the current public arguments. At built-in writer
entry, a token must have `type(token) is object`; token-`None`, direct, and
delegating-writer calls stay on public mutable compatibility. After the
physical built-in template and writer-entry `fill_template` gate pass, the
writer resolves the bound `Resources.make_deploy_variables()` method at its
current call slot. If that gate passes, it creates one
`KubernetesPrivateDeployContextV1` containing the token and process-local
resolved-owner record and invokes that same checked bound method with the new keyword-only
`_kubernetes_deploy_context`; its default is `None`. In private mode, the exact
Resources method preserves generic-prefix, cloud-slot, and generic-suffix order
and returns `DeployVariableAssemblyV1`. All generic-prefix and generic-suffix
`skypilot_config.get_nested` calls remain the current dynamically resolved
public calls; S2a.2 neither closes nor gates them. Immediately before the cloud
slot the Resources method resolves the bound cloud callback. If that gate
passes, it calls the same
checked callback with the same private keyword. At the exact current first
Pod-read stage, the Kubernetes callback resolves and gates the historical Pod
resolver, combined Pod/metadata facade, effective-region getter, Kubernetes
merge primitive, `config_utils.get_cloud_config_value_from_dict`, and
the composite `Config` projection owner. That composite gate requires both
`config_utils.Config` and its `get_nested` method to retain their captured
built-in identities. Only if all six projection and combination owner gates
pass does it enter the closed projector. A class or class-method replacement or
any seam failure invokes the currently resolved
historical public Pod path with legacy arguments and returns no projection.
When all six gates pass, the exact Kubernetes callback returns
`KubernetesCloudDeployVariableResultV1`. If the cloud gate fails, the
exact Resources method calls the replacement with the exact legacy arguments
and no new keyword, accepts its ordinary mapping, completes generic suffix
assembly, and returns a typed assembly whose `kubernetes_projection` is `None`.

The writer-created context contains the token, resolved writer, checked
`fill_template`, and Resources bound method; its later slots are absent. The exact
Resources method does not mutate that frozen record. At each later gate it
creates a frozen enriched owner record and context containing the same token,
cloud bound reference, and seven callable identities across six Pod-stage
projection/combination owner gates
as they become available. It passes only the applicable
enriched context to the next exact built-in owner. All contexts are discarded
before the writer returns. A
missing, extra, mismatched-token, or wrong-type private field raises
`InvalidCloudConfigs` with fixed message
`Invalid Kubernetes deploy-variable context.` and no cause before any provider
mutation or Resources/cloud replacement callback.

When the private context is absent, both exact built-in methods retain their
current signatures at invocation, ambient resolution behavior, and ordinary
dict return. If the Resources bound-method gate fails, the writer likewise
calls that checked replacement with the exact legacy arguments and no new
keyword and accepts its ordinary mapping. No replacement, wrapper, subclass,
inherited SSH override, or inventoried direct caller receives
`_kubernetes_deploy_context` or a typed return. A delegating replacement enters
the built-in default-`None` path. The private context is an exact-type,
identity-checked, process-local envelope; it and the resolved-owner record are
never durable, logged, hashed, or exposed through the public SDK.

The envelopes prevent field rebinding, but the mappings deliberately retain
the exact mutable objects and nested identities produced by the current
callback; only the Pod projection is guaranteed detached by its existing
config-copy boundaries. Only the physical built-in Kubernetes or SSH-node-pool
template path creates a projection. Other providers and arbitrary full-template
plugins retain their current callback contract in this slice. The writer
consumes the snapshot and Pod projection completely during render, combination,
and metadata application. Nothing crosses the config-write return boundary.
Final-variable construction retains the current shallow
`dict(cloud_specific_variables, **generic_suffix)` behavior and does not mutate
or copy the separately carried Kubernetes mapping. The existing public
`Resources.make_deploy_variables()` surface is a compatibility projection of
only the final mapping. The built-in production writer uses the private result,
keeps the Pod projection unchanged through the one exactly recomposed
full-source render and parse, and returns the same ordinary config mapping as
today. `write_cluster_config()` gains only the private default-`None` token and
no alternate return type. Direct and delegated calls retain their current
arguments, public deploy-variable dispatch, mutable combination, and ordinary
mapping return. A writer wrapper or replacement is still invoked by its caller
with the exact current arguments and no private keyword.

S2a.2 deliberately does not bind provider mutation ownership and does not
carry the first cloud-variable result across provisioning. The existing
post-`bulk_provision()` `make_deploy_resources_variables()` callback remains at
its current stage for Kubernetes, SSH node pools, every other provider, and all
plugin paths. It is re-resolved there and receives the current arguments,
including restored `handle.cluster_name_on_cloud` when reuse changes the name.
Its config reloads, provider reads, values, errors, and side effects remain
authoritative for post-provision runtime setup. Removing that second lifecycle
callback requires the M4 immutable provider descriptor and a provider-wide
freshness inventory; S2a.2 introduces no Kubernetes-only lifecycle facet,
private bulk keyword, or process-local handoff.

After the initial full pre-combination parse, the identity-admitted private path
has two combination branches. The strict branch classifies and freezes the
parsed `node_config` and same private Pod projection, calls the pure owner,
performs the first legacy-equivalent deep copy of the full object, thaws the
returned `node_config`, and replaces only that existing value; assignment
retains its insertion position. If the parsed or projected value is explicitly
unfreezable, the private typed-mutable adapter instead passes the same parsed
object and already-projected mutable Pod config to the mutable combination
algorithm, which performs the current first full-object deep copy before
merging. This typed-mutable branch retains the private snapshot and one
writer-time Pod projection; it differs from public compatibility only in the
declared per-attempt coherence behavior.

After either identity-admitted branch succeeds, the assembler performs the
current second deep copy of the entire post-Pod object. Only after that copy
succeeds may it project effective custom metadata from the same snapshot. It
classifies the metadata while retaining the already-detached second copy.
Freezeable metadata is thawed to a fresh mutable value; unfreezeable metadata
retains the detached typed-mutable value. The assembler then transfers
exclusive ownership of the second copy and selected metadata value to the
single deterministic full-object metadata applicator. That applicator mutates
and returns the same solely owned full object and performs no further
full-object copy.

Public mutable compatibility is separate. Any owner or template gate failure,
token-`None` direct or delegating-writer call, Resources or cloud override,
registered template owner, custom template variables, custom failover override,
or arbitrary full-template plugin creates no private config snapshot, Pod
projection, or typed cloud result. A cloud-method gate can fail only after the
transient writer context has reached the exact Resources method, but the
replacement cloud method receives no context and no context survives the
writer. The writer invokes the resolved
public methods with legacy arguments. After the initial parse it invokes the
currently resolved public combined Pod/metadata facade once at its current call
site. The exact historical facade performs the first full-object copy and
second ambient Pod resolution, then the second full-object copy and ambient
metadata resolution; a replacement's output, exception, calls, and side effects
remain authoritative. The later post-provision cloud callback is outside this
dispatch and remains unchanged on both private and public config-write paths.

The resulting full object from any branch then passes to the existing late
managed-image owner when needed and the existing full YAML dump. Only after
that call returns, the assembler resolves `head_node_type` from the same full
object with the current default, takes the exact active post-managed-image
`node_config` mapping by reference, pops `deployment_spec` and then `pvc_spec`
from that same mapping, and passes that same mapping to `check_pod_config()`.
There is no validation copy. A wrapped `dump_yaml` that retains its argument
therefore observes those later pops just as it does today. This preserves the
current second-copy-before-metadata ordering, merge-error,
dump-before-validation, object identity, post-dump mutation, exact pre-dump
value type, and managed-image precedence.
`check_pod_config()` stays outside the pure owner because its Kubernetes model
loading and caches are ambient runtime behavior. The assembler preserves the
top-level and nested insertion positions used by the monolith; no sorting or
canonicalization is introduced in S2a.2.

The pure owner performs no Jinja rendering, YAML parsing or serialization,
filesystem, config, workspace, environment, provider, catalog, registry,
plugin, database, clock, randomness, cache, logging, warning, or externally
visible mutation operation. Contract-validation and impossible-after-validation
thaw errors identify only the contract field and renderer identity. They never
include values, metadata, commands, environment values, or Pod configuration,
and have no cause chain. The factored strict merge core does not catch or
translate merge failures; their production exception class, message, cause,
and stage remain exact.
The impure full-source render and single YAML parse deliberately retain the
current renderer, loader fallback, exception class, message, source coordinate,
and evaluation order because their source bytes are identical.

S2a.2 has exactly two intentional compatibility deltas. First, its prerequisite
replaces the time-bearing host-network probe gzip member from the S2a.1 merge
baseline `06ce2213526652621d7d7ae37137221f16b798c4` with the specified portable
deterministic member. Every built-in Kubernetes and SSH-node-pool render embeds
the runtime-gated probe in its Ray start commands, even when effective
`hostNetwork` is false. Their rendered bytes and cluster hashes are therefore
compared against the deterministic-gzip prerequisite head, not that older
merge. Second, each identity-admitted exact built-in Kubernetes or SSH-node-pool
private config-write attempt reuses its first effective-Pod projection for both
host-network variables and the later Pod merge. A config reference replacement,
stateful `__deepcopy__`, changing external fact, or Pod-projector side effect
between those two writer stages can no longer produce a different Pod value or
a second writer-only failure. The first current Pod projection's value or error
is intentionally authoritative for that config write. Gate-failed, token-`None`,
direct, delegating-writer, method-override, custom-template/failover, and
arbitrary full-template public mutable
paths retain the historical ambient host-network Pod read, later combined-facade
Pod and metadata reads. Deferred custom
metadata is also selected from the raw loaded-config and override references
captured at that first Pod-read point. Replacing or reloading either reference
after capture is intentionally not observed, whereas an in-place mutation of a
captured object remains visible when the deferred metadata projector runs
because capture is shallow and reference-only.

The existing post-`bulk_provision()` cloud callback is explicitly outside this
coherence delta. It is re-resolved at its current stage, performs its current
fresh Pod and provider reads, and controls runtime `custom_resources`. Its value
may differ from the render-time mapping, and its error or side effect remains
observable after provider mutation. Successful built-in Kubernetes and SSH
new-provisioner paths therefore retain two cloud callbacks: one during config
writing and one after bulk provisioning. Pod resolution changes only within the
writer, so that end-to-end path changes from three Pod reads to two.

Callable check-and-call coherence is part of the same bounded render-attempt
delta. The eleven callable identities across ten declared render/config owner
gates are observed at their defined slots; every undeclared config read remains
dynamic. On an
identity-admitted private config write, the four
execution owners use the same checked reference for invocation, while the six
Pod projection, metadata, region, and merge seams are admission-only identities
that the closed projection/combination unit does not invoke or reread. A
replacement installed after one of those six gates cannot enter that closed
unit and is observed there on the next config write. It remains visible at any
unrelated historical generic or cloud config call later in the same writer.
Replacements already installed when the Pod-boundary slots are reached select
public mutable compatibility for combination. All pinning ends when
`write_cluster_config()` returns; the retained post-provision callback performs
its ordinary late lookup.

Public compatibility deliberately has no blanket once-resolved guarantee. The
writer reference already selected by `_retry_zones()` and the first bound-method
call at its gate slot use those checked references, but historical Pod,
metadata, and config stages perform their current late lookups. Public
`merge_k8s_configs()` also rereads its facade at every recursive edge. Thus a
separate gate failure or nonidentity public cause allows a later module/class
replacement to remain visible at the same current stage exactly as
characterized below. The writer is captured at `_retry_zones()` admission and
invoked through that same reference. `fill_template` is captured without
invocation at built-in writer entry. The bound `Resources` and cloud methods
are captured immediately before their respective current call sites. The six
projection/combination seams are captured together immediately before the
first current Pod read. Capturing the combined facade there is intentionally
earlier than its legacy post-parse lookup; the corpus below locks that bounded
timing. Unrelated config calls before and after this unit remain dynamically
resolved. This removes check-versus-call races only from the shared closed unit
while preserving ordinary wrapper, replacement, and subclass behavior at
config-write boundaries.

On the post-prerequisite baseline, stable officially accepted config and
resource inputs plus stable provider facts retain exact successful tree, dump,
hash, and validation behavior. Every other production render, parse,
projection, merge, dump, validation, and compatibility failure retains its
stage, exception class, message, cause, source coordinate, and ordering. The
stateful and mid-request-mutation corpus proves the bounded coherence delta
rather than claiming impossible byte or error equivalence for the removed
second reads. Fixed contract errors are reachable only for an invalid internal
frozen envelope or implementation defect and fail before provider effects.
Old-client, new-client, CLI, server error-wire, and rollback corpora prove the
remaining boundary.

Effective `pod_config` and `custom_metadata` come from one already-authorized
request snapshot. A new `KubernetesRenderConfigSnapshotV1` captures request-owned
raw loaded-config and resource-override references, cloud kind, and context once
without recursively traversing or projecting either value. Capture occurs at
the existing first Pod-resolution point inside the Kubernetes cloud callback,
after every generic-prefix and earlier cloud step. Its conceptual shape is:

```python
@dataclasses.dataclass(frozen=True)
class KubernetesRenderConfigSnapshotV1:
    schema_version: typing.Literal[1]
    loaded_config: config_utils.Config
    cluster_config_overrides: dict[str, typing.Any]
    cloud_kind: typing.Literal['kubernetes', 'ssh']
    context: str | None
```

The envelope is frozen but both config fields are deliberately captured by
reference. Two snapshot-fed functions,
`resolve_effective_kubernetes_pod_config_from_snapshot()` and
`resolve_effective_kubernetes_custom_metadata_from_snapshot()`, reproduce the
two current helpers exactly. The Pod projector runs once at the current
host-network point inside Kubernetes deploy-variable construction and its
result is reused after the full parse;
the metadata projector runs only after Pod combination and the second
legacy-equivalent full-object deep copy both succeed. For Pod
config, the cloud object selects `ssh` only when it is an
`SSH` cloud; a non-null SSH context retains the current required `ssh-` prefix
and assertion, then strips that prefix. Otherwise Pod config selects
`kubernetes` and leaves the context unchanged. For custom metadata, the raw
context alone selects `ssh` and loses its prefix when it starts with `ssh-`;
otherwise it selects `kubernetes`. The projection owners compute these values
separately, so even their historical behavior for a null or
mismatched cloud/context pair is unchanged. For each projection and for each of
the captured server config and `Resources.cluster_config_overrides`, the
Kubernetes algorithm merges a
non-null `context_configs.<context>` mapping over the cloud-level mapping with
`merge_k8s_configs()`. The SSH algorithm instead uses the entire non-null
context value in place of the cloud-level value; even an empty context mapping
replaces it. Either algorithm falls back to the cloud-level value only when the
context value is absent. Finally, it merges the resulting resource projection
over the resulting server-config projection with `merge_k8s_configs()`, as the
current Pod-config and metadata helpers do. Thus Kubernetes retains recursive
global-plus-context merge semantics, SSH retains whole-projection context
replacement, and resource overrides retain their final merge semantics.
Patch-keyed and ordinary lists keep their exact existing behavior rather than
being summarized as scalar precedence.

Each projector reproduces the exact current `Config` and `get_nested()` call
graph at the moment that stage is reached. Server config and resource overrides
have separate `Config` views; within each view, context and cloud-level
`get_nested()` calls return independent deep copies before precedence merge.
No projector replaces those calls with two reads from one copied subtree or
memoizes across them. The Pod and metadata projectors likewise share no detached
view. Thus an alias between global and context values or between raw
`pod_config` and `custom_metadata` is broken at the same boundaries as today,
while aliases within one returned value retain that call's current copy
behavior.

The projection owners perform no workspace lookup or workspace precedence.
Workspace `pod_config` or `custom_metadata` is semantically ignored by the
production render as it is today and remains explicitly offer-ineligible when
present. Exact whole-`Config` deep-copy behavior can still traverse a workspace
subtree; this is not a physical no-read claim. The live
helpers delegate to the corresponding snapshot projection instead of retaining
ambient reads. The deploy-variable owner accepts the already-projected Pod
config and performs no config read. Pod projection does not semantically
select, merge, classify, or apply custom metadata before Pod combination. Its
exact `Config.get_nested()` implementation still deep-copies the complete
`Config`, so a programmatic sibling metadata object can be traversed or fail at
the same Pod-projection boundary as today; S2a.2 does not claim path-local copy
isolation. The base owner consumes only that same frozen effective Pod config.
After the initial full-object pre-combination parse, successful Pod combination, and second
legacy full-object deep copy, one snapshot-fed deterministic ownership-transfer
`apply_kubernetes_custom_metadata()` owner consumes the selected detached
custom metadata and that already-detached full cluster object. The freezeable
path thaws metadata exactly once before transfer; compatibility transfers its
already-detached mutable value. The applicator receives exclusive ownership of
the full-object reference from the assembler and applies that same mutable
metadata object through the single factored closed merge implementation with
the existing semantics. It intentionally mutates and returns
the same full-object reference without another copy; no other component may
retain or observe the transferred input. The exact
destination order is autoscaler service-account metadata, role metadata,
role-binding metadata, the same service-account metadata a second time, Pod
metadata, then Service metadata in provider order. It returns the resulting
detached full object, including the legacy cross-destination aliases and
source-mutation effects, without an ambient config read.

That order is observable behavior, not an accident that S2a.2 may normalize.
For example, schema-accepted `custom_metadata: {finalizers: ['x']}` causes the
second service-account merge to extend the shared list, so the Pod and all outer
destinations receive `['x', 'x']`; shared mappings likewise produce PyYAML
anchors and aliases. The single applicator preserves the parsed tree, alias
graph, dump bytes, and cluster hash. The base owner never applies custom
metadata, and no disjoint Pod-versus-outer metadata applicator exists. Neither
production nor S2b may independently resolve or merge either effective value.

The process-local full render and detached base result are sensitive. `runcmd`,
literal Pod environment values, annotations, and Pod config may contain secrets
or personal data. S2a.2 adds no rendered cluster, full `node_config`, or base
Pod to an offer payload, Datadog field, new log, internal contract error,
database column, durable event, config hash input beyond the existing cluster
hash, or persistent fingerprint. Legacy Jinja, YAML, merge, dump, and validation
exceptions remain byte-for-byte compatible and may retain their current source
excerpt or value behavior; S2a.2 does not broaden or sanitize them. S2b may not
consume this full materialization, synthesize a second
71-name context, or copy scheduling-field extraction. Its separate source-safe
projection design must prove parity against this production owner for the
closed eligible subset while reading none of the forbidden outer inputs. Until
that review passes, S2b remains non-authoritative and no base-render identity
enters an offer.

Production dispatch is explicit. The source composer is selected only when the
logical template reference resolves to the built-in
`sky/templates/kubernetes-ray.yml.j2` path and the authoritative outer and
fragment have their exact identities, for either Kubernetes or its
SSH-node-pool use. During the compatibility window that logical path is the
validated monolith mirror; after its removal gate it is the final outer. The
private deploy-variable
projection requires ten owner gates: the
resolved `backend_utils.write_cluster_config` and
`common_utils.fill_template` callables, the resolved bound
`Resources.make_deploy_variables()` on an exact `Resources` instance, and the
resolved bound cloud callback on an exact built-in `Kubernetes` or `SSH`
instance. At the Pod boundary, the historical
`kubernetes_utils.resolve_effective_pod_config` and
`kubernetes_utils.combine_pod_config_fields_and_metadata` facade aliases must
be their import-time captured built-in implementations,
`skypilot_config.get_effective_region_config` must likewise retain its captured
built-in identity, `config_utils.merge_k8s_configs` must retain its import-time
captured built-in identity, and
`config_utils.get_cloud_config_value_from_dict` and
the composite Config projection owner must retain their captured public
built-in identities. That composite checks both the `config_utils.Config` class
object and its `get_nested` method. The private writer token
must also be present. A class replacement,
instance attribute replacement, subclass override, or inherited SSH override at
any seam fails that gate. In that case production calls the existing resolved
public rendering and deploy-variable callables, retains mutable Pod/metadata
combination, emits only fixed internal diagnostic
`CUSTOM_RENDER_OR_DEPLOY_OWNER`, and remains offer-ineligible. The source
composer may still reconstruct a physical built-in template on that
compatibility path, but the private projection and strict merge owner
are unreachable.

`ResolvedKubernetesRenderOwnersV1` is the conceptual, process-local record of
those resolutions. It is not an eagerly constructed public dataclass: each slot
is populated once at its defined gate point as the built-in writer path
advances, then is immutable for that config write. `_retry_zones()` invokes the
same resolved writer reference it checked. On an identity-admitted private
write, the writer invokes the same resolved `fill_template` reference it
checked. Exact `Resources` and cloud instances
resolve their bound methods once; the gate checks the receiver and the bound
method's `__func__` by identity, and the checked bound reference is the one
invoked. At writer entry only `fill_template` is checked directly with `is`.
At the first current Pod read, the six projection/combination seams are checked
together. They are admission identities only: exact identities admit the
closed Pod, deferred-metadata, base-merge, and
metadata-merge unit, while any failure selects the public mutable path that
invokes the currently resolved facades at their historical stages. The closed
unit never invokes or rereads those six public seams. Their unrelated call sites
elsewhere in Resources or the cloud callback remain dynamically resolved and
may observe a later replacement. The conceptual record,
callable references, and receiver references are never persisted, hashed,
logged, sent to Datadog, placed in an offer, or retained after the writer
returns. On public compatibility, the admission observations do not pin later
facade calls: the historical renderer, Pod, metadata, config, and recursive
merge stages perform their current dynamic lookups.

Closed Pod/metadata config resolution does not call the captured public
facades. One shared
`_merge_k8s_configs_impl()` contains the existing merge algorithm and accepts
an explicit recursion function. The public `merge_k8s_configs()` facade calls
that implementation with a late-binding resolver that looks up the public
facade again on every recursive edge, preserving current wrapper replacement,
exception, and nested-call behavior on mutable compatibility. The strict
`merge_k8s_configs_closed_v1()` entry passes a closure over itself instead. Its
nested mapping, `imagePullSecrets`, named-list, unnamed-container, and all other
recursive edges therefore use only the closed implementation even if the
public module attribute changes during the operation. This factors one merge
algorithm rather than copying it.

Likewise, one factored Pod/metadata config-projection implementation accepts
explicit loaded-config, nested-get, recursive-update, and Kubernetes-merge
dependencies. Public `Config.get_nested()`,
`get_cloud_config_value_from_dict()`, and
`skypilot_config.get_effective_region_config()` retain their current dynamic
facades and call ordering for compatibility. The strict
`resolve_effective_region_config_v1()` path supplies only the closed recursive
update and closed merge dependencies through resource-override application and
Kubernetes general-plus-context combination. Generic Resources variable reads
continue to invoke the dynamically resolved public
`skypilot_config.get_nested()` at every current call site. All Kubernetes cloud
callback config reads outside the Pod/deferred-metadata projection likewise
retain their current public call graph before and after the Pod boundary. At the first current
Pod read, the private projector captures the raw loaded-config reference into
`KubernetesRenderConfigSnapshotV1` before performing the same closed projection;
deferred metadata projects from that captured reference as specified below.
The current Pod and metadata projection passes each raw mapping to
`get_cloud_config_value_from_dict()`, which constructs a fresh
`config_utils.Config(dict_config)` before any `get_nested()` call. It therefore
does not dispatch through an instance attribute or subclass method on the
active loaded-config object. The composite gate locks the actual module-level
constructor and its class method; replacing `config_utils.Config` with a
subclass fails even when that subclass inherits the original method object.
Instance-level behavior of the active loaded config remains observable only at
the unrelated public `skypilot_config.get_nested()` call sites that S2a.2 leaves
dynamic.
The private loaded-config accessor is a source-closed factoring of the current
ContextVar/global-context selection and invokes no replaceable config facade.
No strict nested config or merge edge performs a module attribute lookup.
Underscore-prefixed projection implementation helpers such as `_recursive_update()` and
`_get_loaded_config()` remain factored implementation details, not supported
replacement seams; the public gates above own compatibility. The strict and
public entries share their exact algorithm bodies, but a direct monkeypatch of
an underscore helper is outside the declared extension contract.

The dispatcher emits one such diagnostic for each failed gate that it actually
resolves, bounded by the ten owner gates per config write. The fixed
`owner_seam` enum
can name the built-in writer,
`fill_template`, Pod resolver, combined Pod/metadata facade, effective-region
getter, Kubernetes merge primitive, cloud-config projector, Config projection
owner, resource deploy-variable method, or cloud
deploy-variable callback. It never contains a callable name, module, type, key,
value, or rendered input. This bounded discriminator makes each
compatibility-removal gate observable in existing Datadog collection without
adding a statistics store. On an identity-admitted private write, the built-in
writer resolves and checks `fill_template` at its writer-entry admission slot,
then invokes that same reference at the actual rendering call. A token-`None`
direct or delegating write, or a write that selects public compatibility at a
later gate, instead re-resolves `fill_template` at the current render stage, so
a post-entry replacement remains authoritative. A
replacement writer that never delegates cannot reach that facade;
out-of-facade physical template reads remain governed by repository and
downstream-package inventory.

Public compatibility can also be selected with every callable identity intact.
The dispatcher emits one additional value-free event for each present fixed
cause: `NO_PRIVATE_WRITER_CONTEXT`, `REGISTERED_TEMPLATE_OWNER`,
`CUSTOM_TEMPLATE_VARIABLES`, or `CUSTOM_FAILOVER_OVERRIDE`. These causes do not
encode the plugin, variable, key, or value and bring the total bound to fourteen
events per config write. Per-cause events prevent one compatibility reason from
masking another during removal observation.

The pure owner is selected after the initial pre-combination parse only when all
ten owner gates pass, no
registered template override owns the attempt, and the
parsed object plus resolved configs satisfy the frozen grammar. The final
variable mapping preserves existing precedence:
Kubernetes deploy variables, generic resource variables, common variables,
then cloud-specific failover overrides. Registered `TemplateSpec.variables`
never enter the strict path. A nonempty or unfreezable custom failover override
remains on public mutable compatibility. Any registered Kubernetes provisioner or
template ownership is
`CUSTOM_PLACEMENT_CONFIG` for offers, even when its callback returns `None`.
Provisioner registration without template ownership does not by itself select
mutable render compatibility; lifecycle dispatch remains outside S2a.2.
A `TemplateSpec` that supplies custom full-template content or variables keeps
the current full-template renderer and mutable combination. It does not call,
wrap, or partly reuse the strict base-Pod owner. If it selects the logical
built-in template reference whose authoritative source is the private
outer, the same source composer reconstructs the monolith
before the plugin's final-precedence variables render. A plugin-shipped full
template renders its own captured source as it does today.

That plugin path is an explicit compatibility adapter, not a second
authoritative built-in owner. Removing it requires a structured template
extension that can express additions without replacing the base Pod owner, an
inventory of downstream plugins, exact conformance for every inventoried
extension, one compatibility release with zero legacy-only use, and a separate
removal commit. Plugin registration can never expand offer eligibility by
itself.

Managed-image enforcement, authentication, reuse restoration, and S2a.1 final
Pod mutation remain after the base owner. `_enforce_managed_kubernetes_image()`
stays the single managed-image mutation body before and after reuse; the restore
helper delegates to it. Any non-null `resolved_container_image` is
authoritative-offer-ineligible in V1 until a later shared post-reuse finalizer
is designed and characterized. `REUSE` and `RESTART` remain unsupported offer
operations because restoration can replace `node_config`. Authentication may
still globally replace the historical `skypilot:ssh_user` and
`skypilot:ssh_public_key_content` sentinels, so S2a.2 does not claim that it is
placement-inert for every legacy input. Before offer projection, V1 scans every
allowlisted scheduling string and rejects either reserved sentinel with
`NOT_REPRESENTABLE(CUSTOM_PLACEMENT_CONFIG)` and the internal fixed diagnostic
`AUTH_RESERVED_SENTINEL`. This is defense in depth beyond V1's rejection of
explicit Pod config. The final pre-create fingerprint is taken after
authentication, so an escaped replacement cannot match an offer.
The S2a.1 `finalize_pod_spec()` owner consumes a detached copy of the post-reuse,
post-authentication Pod and remains the only owner of final pre-admission
scheduling mutations.

The split cannot cut over on object similarity alone. A test-only legacy
monolith oracle and the new split path run against a frozen corpus covering CPU,
accelerators, host networking, every optional binding as absent, present-null,
and present-value, volumes, Docker sidecars, HA, Pod config, custom metadata,
duplicate-list custom metadata, cross-destination aliases, safe-YAML date,
naive datetime, canonical fixed-offset datetime including a fractional-second
offset, custom-named-timezone compatibility, and binary scalars, repeated tuple,
including empty and nonempty cases, date, and datetime aliases,
unfreezable compatibility cases, SSH node pools, plugin bypass, managed images,
authentication sentinels in every scheduling string class, absent, null,
mapping, scalar, and sequence PVC and Deployment siblings, fresh create, and
reuse. Tests prove all of the following:

- the physical fragment source and derived 71-name set match their exact
  digests;
- raw source composition recovers the exact monolith before Jinja, invokes one
  current full-template render and one current initial full-document
  pre-combination safe parse, and preserves spontaneous-environment cache and
  YAML fallback effects exactly;
- the established `common_utils.fill_template` facade is called once with the
  exact current template-reference string, identical variables object, and
  output path. A wrapper receives those same arguments and, when it delegates,
  the built-in facade internally consumes the recomposed source. A replacement
  that opens the template reference sees the unchanged compatibility monolith,
  and its output, exception, and side effects remain authoritative on the
  public mutable compatibility path;
- the reserved physical marker occurs exactly once; exact built-in and
  delegating-facade paths never read or render the compatibility monolith; only
  an identity-gated replacement renderer that opens its received
  `template_ref` directly may do so during the compatibility window; and no
  built-in or supported facade path renders or parses the fragment
  independently;
- cross-boundary anchors and aliases synthesized by accepted plain strings,
  duplicate anchors across the physical split, directives, and quoted path
  parse failures have exact legacy success, tree, exception, source-coordinate,
  and stage precedence behavior;
- parsed cluster trees, post-merge object order, final dumped YAML bytes,
  deterministic cluster hashes, and post-managed-image validation outcomes are
  identical for successful strict renders;
- global, selected-context, and resource-override config precedence is exact,
  including Kubernetes recursive context merge, SSH whole-context replacement,
  the distinct Pod-versus-metadata cloud selection and SSH prefix rules, null
  and mismatched cloud/context pairs, and both clouds' final resource-override
  merge;
  workspace-only Pod config or metadata remains ignored in production and
  offer-ineligible;
- the identity-admitted exact built-in private path invokes the Pod projector
  exactly once at the current
  host-network point in deploy-variable construction; the same detached result
  controls configured `hostNetwork`, probe environment variables, and the
  post-parse Pod merge, while OCI RoCE retains its independent
  forced-host-network branch. The public direct-call compatibility path retains
  its current ambient host-network Pod read and ordinary dict return;
- each identity-admitted exact built-in Kubernetes or SSH-node-pool private
  config-write attempt invokes its cloud-variable callback once and uses that
  mapping only for rendering. A successful built-in Kubernetes or SSH
  new-provisioner attempt invokes the unchanged cloud callback again after
  `bulk_provision()`; that second result controls runtime `custom_resources`.
  Stateful value, error, side-effect, config-reload, provider-read, reuse,
  skip, dry-run, legacy, failover, failure, and other-provider fixtures lock the
  current callback stage and count. The ordinary successful path retains two
  cloud callbacks and changes only from three Pod reads to two;
- paired stateful sentinels for every gate-failed, token-`None`, direct,
  delegating-writer, method-override, custom-template/failover, and full-template
  public path prove the first
  ambient Pod read controls host networking, the later historical combined
  facade performs its own Pod and metadata reads, the post-provision callback
  is reached at its current stage when applicable, and the current value,
  error, and side-effect precedence is unchanged. No private snapshot or typed
  projection is constructed; no private context reaches a replacement or
  survives the writer;
- freeze-rejected config and parsed-graph fixtures reached after exact private
  admission prove the typed-mutable adapter reuses the one Pod projection and
  raw snapshot, invokes the closed mutable merge adapter without rerendering, and
  retains private writer-local projection behavior without invoking the public
  combined facade;
- direct exact calls to `Resources.make_deploy_variables()` and
  `Kubernetes.make_deploy_resources_variables()` receive no private keyword and
  return the same ordinary dict type and object graph as today. Exact private
  calls return the typed assembly and cloud result with the same underlying
  mappings. Exact-signature Resources and cloud replacements assert they
  receive only legacy arguments, return ordinary dicts, and retain current call
  count, exception, and side-effect behavior. Provisioner registration without
  template ownership and lifecycle-function replacement do not change
  render-owner admission or lifecycle dispatch;
- class and instance monkeypatches plus subclass overrides at both
  `Resources.make_deploy_variables()` and the Kubernetes cloud callback,
  including an SSH override, preserve their current output, exception,
  side-effect, and characterized call count, including the current second call
  when the post-provision stage is reached, through the public mutable
  compatibility path; module-level writer, `fill_template`, historical Pod resolver, and
  combined Pod/metadata facade wrappers and replacements, plus replacements of
  `config_utils.merge_k8s_configs` and
  `skypilot_config.get_effective_region_config`,
  `config_utils.get_cloud_config_value_from_dict`, and
  the composite `config_utils.Config` class plus `Config.get_nested` owner, are characterized at the same
  boundary. An exact-signature replacement writer asserts that it receives no
  private token or new keyword and returns the ordinary mapping; a delegating
  writer reaches the built-in default-`None` mutable path. A merge replacement
  present at the Pod boundary selects and remains authoritative on mutable
  compatibility. Exact built-in callable identity checks use `is` and
  invoke no user equality or descriptor beyond the already-resolved bound
  call;
- a stateful race corpus replaces each gated module or class attribute after
  its resolution and proves that no unchecked custom callable enters the closed
  projection/combination unit. The checked writer, fill, Resources, and cloud
  execution references remain authoritative only at their specified stages.
  The six Pod-boundary owner gates remain authoritative only inside the closed
  unit; a later replacement is visible to unrelated dynamic config calls in the
  same writer and selects mutable compatibility at the next Pod boundary.
  Wrappers prove that writer-entry fill capture performs no early invocation,
  later calls retain their current order and count, every strict
  effective-region projection uses the source-closed config core, all six
  boundary owners are gated immediately before the first Pod read, exact and
  inherited-method replacement Config classes fail the composite gate, and bound
  methods are not resolved before their current call slots. Nested-map, named-list, unnamed
  container, `imagePullSecrets`, Kubernetes override, and context-merge
  fixtures replace the public merge attribute after outer entry and prove that
  strict recursion reaches only the closed merge core, while the next attempt
  selects mutable compatibility. Public-facade fixtures prove the same
  replacement remains visible at the exact current nested edge on the mutable
  path. Stateful `skypilot_config.get_nested` replacements and nonprojection
  uses of the six boundary facades remain dynamically visible at their current
  generic and cloud call sites on both private and public writer paths. A
  replacement installed after writer return remains visible to the
  retained post-provision callback in the same physical attempt;
- a reuse fixture whose restored YAML `cluster_name_on_cloud` differs from the
  fresh writer-generated name proves the retained post-provision callback still
  receives the restored handle name;
- on the identity-admitted private path, a Pod projector that would return a
  different value or fail on a second writer-time invocation proves that only
  the first current projection is observed and authoritative within the writer.
  A stateful cloud callback then returns a different value or fails at the
  retained post-provision invocation and proves that later value, side effect,
  or error remains observable at its current stage;
- an earlier cloud-step sentinel that replaces the loaded-config reference is
  observed by snapshot capture at the current first Pod-read point. After that
  capture, replacing or reloading the global config or override reference is
  not observed; an in-place mutation of custom metadata through the captured
  reference remains visible to the deferred metadata projector, while the
  already-detached Pod projection remains unchanged;
- a paired generic-prefix failure and Pod-projection failure preserves the
  generic-prefix winner, and paired earlier-Kubernetes-step and Pod failures
  preserve the current cloud-specific winner;
- a malformed Pod projection or merge fails before custom metadata is projected
  or semantically selected, merged, classified, or applied, including a paired
  malformed-metadata case. A separate sentinel fails during the second
  legacy-equivalent full-object deep copy and proves the metadata projector is
  not invoked first. Whole-`Config` deep-copy traversal of sibling metadata at
  the Pod boundary remains characterized rather than forbidden;
- YAML aliases shared between global and context values and between raw Pod
  config and custom metadata are independently copied at the exact current
  `Config.get_nested()` boundaries within each staged projection, with
  identical merged identities, dump anchors, and bytes for a stable snapshot;
- the custom-metadata applicator preserves the exact historical destination
  order, shared-object graph, duplicate service-account self-extension, Pod
  values, PyYAML anchors and aliases, and provider-order Service effects;
- every exact built-in or delegating-`fill_template` facade path invokes exactly
  one composed full render and one initial pre-combination full parse. A
  replacement renderer that successfully writes output retains its own render,
  exception, and side-effect behavior and then reaches the one current initial
  parse without any composer-call requirement. On the identity-admitted private
  path, a freezeable parsed graph invokes only the strict base-Pod owner and an
  unfreezable input or parsed graph invokes only typed-mutable combination
  without a rerender; a custom full-template plugin invokes only its declared
  public compatibility path;
- a normal rendered monolith proves that visiting its embedded active
  `node_config` once at the ordinary path is not classified as an alias;
  separate fixtures with a real identity shared inside the full object or
  across the full object and detached Pod config select mutable compatibility;
- dry-run, non-dry fresh, restore, and managed-image restore fixtures lock the
  exact later parse and serialization order and successful totals above,
  including hash exception behavior and file-mount optimization retries;
- `build_kubernetes_base_pod_spec()` input-mutation, returned-input-alias,
  ambient-read, sensitive-error, and logging sentinels stay silent; separate
  applicator tests prove sole-reference ownership transfer, same-object return,
  and no ambient read or retained input reference;
- exact `VolumeInfo` values with an extra instance attribute or a declared
  field whose runtime value is not exact `str` or `None` select mutable
  compatibility without dropping or traversing that extra state;
- internal contract-validation and impossible thaw errors are fixed and
  value-free. Against the deterministic-gzip prerequisite head and a stable
  officially accepted snapshot, production config-projection, merge,
  full-render, full-parse, final-dump, validation, and compatibility failures
  retain exact legacy class, message, cause, coordinate, stage, and
  old/new-client wire behavior; the stateful corpora separately lock the
  declared projection and per-attempt callable coherence deltas;
- managed-image and reuse cases prove their late owners still supersede the
  fresh base result exactly as today, including a tuple-valued effective Pod
  `containers` sequence that must retain the legacy managed-image error, and a
  Pod `VolumeInfo` plus selector conflict that must retain conflict-before-dump
  error precedence;
- wrapped `dump_yaml` and `check_pod_config` callables prove the full object is
  dumped first, the exact active `node_config` reference is then mutated by
  popping `deployment_spec` followed by `pvc_spec`, the retained dump argument
  observes those pops, and validation receives that same mapping object;
- each reserved auth sentinel in a scheduling field returns the fixed typed
  fallback before offer creation, while every eligible post-authentication
  scheduling projection remains unchanged.

The first prerequisite makes the current host-network binding deterministic.
`ray_commands.host_network_probe_b64()` replaces time-bearing
`gzip.compress()` with `gzip.GzipFile(filename='', mode='wb', fileobj=...,
compresslevel=9, mtime=0)`. Bare `gzip.compress(..., mtime=0)` is forbidden
because Python 3.11 and 3.12 may expose a platform-specific gzip OS byte. The
test asserts header `1f8b08000000000002ff`, byte identity in separate
processes on Python 3.10 through 3.14, round-trip, actual probe compilation,
cache-clear stability, and Base64 ASCII output. Built-in Kubernetes and SSH
render goldens cover effective `hostNetwork` false and true plus OCI RoCE and
prove that the only prerequisite-relative byte/hash delta is the embedded gzip
member before any later render golden is accepted.

Rollout has seven gates. First, the deterministic-gzip prerequisite passes and
becomes the byte/hash comparison baseline. Second, a source-composer staging
commit adds the authoritative fragment and outer, retains the digest-locked
monolith mirror and exact three-argument `fill_template` seam, installs the
built-in facade's temporary-outer selection, and makes the exact composer authoritative for the
built-in path while mutable combination remains unchanged. Relative to the
prerequisite head, its exact differential corpus, mirror-equality and dispatch
searches, exact-head CI, and rollback-image qualification pass before that image
is deployed and monitored.
Third, a separate coherence commit introduces the identity-admitted private
writer-local deploy-variable assembly, raw config snapshot, reused Pod
projection, staged metadata projection, and exact render/config owner gates
while its typed-mutable combination remains authoritative. Public mutable
compatibility stays on the historical writer-time ambient reads. The existing
post-provision callback remains unchanged on every path. Built-in
Kubernetes/SSH private-path stable-input parity and regression coverage lock
two cloud callbacks and two Pod reads on the successful new-provisioner path,
while retaining current read/callback counts for every gate-failed, token-`None`, direct,
delegating-writer, method-override, custom-template/failover,
full-template-plugin, and other-provider path,
first-projection mutation sentinels, replacement-after-capture sentinels for all
ten owner gates, post-writer replacement and post-provision freshness
sentinels, exact call counts, no-retention checks, exact-head CI,
rollback qualification, and a bounded deployment pass before strict shadowing.
Fourth, on that image, a
temporary comparator computes strict and private typed-mutable combination from detached
copies of the same single rendered and parsed object and staged config
projections. Existing Datadog observability, without a statistics store,
records zero unexplained tree, byte, hash, validation, error-stage, or dispatch
mismatch; it never emits config or rendered data. Fifth, after exact-head CI,
minimum-client qualification, plugin inventory, and dedicated implementation
review, a separate promotion commit makes strict combination authoritative for
freezeable built-in inputs while the closed mutable and custom-plugin adapters
remain. That image is deployed and monitored independently. Sixth, after the
required downstream window, a separate source-layout commit replaces
the compatibility monolith atomically with the validated outer, deletes the
temporary outer path and selection branch, and passes its direct-reader removal
gate before deployment. Seventh, each remaining adapter is removed only through
its own ledger gate and separate deployment. Every stage rolls back to its
immediately preceding image rather than an untracked dual-render fallback or an
unvalidated source copy.

Immediately before the Kubernetes create call, production fingerprints the
exact output of the shared finalizer and compares it with the selected offer.
Since the complete identity object enters the stable offer ID, same-name
service-account replacement changes stable identity and cannot pass
revalidation.
Admission-added sidecars, requests, or hard constraints therefore produce an
actual-result mismatch instead of escaping the offer contract. Soft preferences
and runtime-only image, environment, command, and metadata fields are excluded.
The `observation` object contains exactly `capacity_evidence` and
`configuration_fingerprint`; it contains no raw node or configuration data.

The source preserves context-specific configured Kubernetes pricing using
`Decimal(str(existing_price))`. It does not assume Kubernetes cost is zero.
Current Kubernetes fit probes establish shape support, not free capacity or a
reservation, so V1 reports `availability: unknown`, `quota: unknown`, and
`capacity: shape_fits_existing_node`. A no-fit observation produces
`NO_OFFERS(NO_FEASIBLE_SHAPE)` rather than consulting an autoscaler.

Kubernetes V1 uses `revalidation_policy: before_mutation`. Revalidation repeats
the read-only scope, reachability, service-account name-and-UID, shape, and
configuration checks immediately before provider mutation. A changed
service-account UID produces a different stable identity and
`OFFER_IDENTITY_CHANGED`; it is never accepted as a fresh observation of the
old offer. A positive result remains advisory and does not claim reserved
capacity. The Kubernetes utilities expose uncached internal
`get_kubernetes_nodes_uncached()` path; existing request-cached helpers may
delegate to it, but `REQUIRE_FRESH` calls the uncached path directly through the
pinned client. Tests replace the underlying Kubernetes client and prove
revalidation performs new calls after optimizer capture. A configured
autoscaler returns `NOT_REPRESENTABLE(CUSTOM_PLACEMENT_CONFIG)` before the
offer source or revalidation constructs or queries any autoscaler client. The
independent legacy projection may retain its current autoscaler query before
typed fallback; that evidence never enters an offer.

### Shadow, Propagation, and Persistence

The server-side gate is:

`SKYPILOT_KUBERNETES_PLACEMENT_OFFER_MODE=off|shadow|authoritative`

The default is `off`. Invalid values fail closed to `off` and emit one warning.
This is an operator-controlled server setting, not a client request field.
Until the M4 descriptor/actuation readiness bit is built and true,
`authoritative` is a recognized but unavailable value: startup/readiness fails
with a fixed value-free configuration error instead of silently weakening it to
shadow or allowing mutation.

In `shadow` mode, an eligible source projects candidates from the same raw
observation snapshot used by the legacy adapter. The current optimizer objective
is applied to both projections, but only legacy `Resources` controls mutation.
An ineligible classifier result skips dual projection and leaves the untouched
legacy path authoritative. The comparator records:

- safety class: `eligible`, `no_offer`, `not_representable`, or `source_error`;
- normalized placement-set equality;
- optimizer-winner equality;
- price: `match`, `drift`, or `not_comparable`;
- availability: `match`, `drift`, or `not_comparable`;
- freshness: `fresh` or `expired`;
- actual provider result: `match`, `drift`, or `not_comparable`;
- a bounded typed reason code.

The normalized placement class is provider, opaque scope, region, ordered zone
batch, batching scope, normalized resources, purchase mode, and requested node
count. Comparator logs contain only the mode, provider, offer and observation
IDs, counts, comparison axes, and typed reason. They do not contain raw scope,
context identity, namespace, provider payload, or user configuration. Existing
logs and Datadog collection are the observation plane; M2 adds no statistics
store.

Shadow mode deliberately leaves the current refreshable Kubernetes wrappers in
control of mutation. It therefore does not claim that every legacy call used
one client or endpoint. It compares the legacy logical region, resource shape,
and node count, but marks endpoint-, scope-, UID-, and admission-sensitive
actual-result axes `not_comparable`. It never fabricates
`ActualPlacementEvidenceV1` from a wrapper that may transparently refresh.
Exact scope and provider evidence are authoritative-mode gates proven by
focused tests and isolated `boltz-test` authoritative canaries before wider
promotion, not claims inferred from shadow mutation.

Offer matching returns a provisioner-owned sidecar rather than mutating a
`Task`, `Resources`, or DAG:

```python
@dataclasses.dataclass(frozen=True)
class TaskPlacementDecisionV1:
    task_index: int
    resources_fingerprint: str
    operation: OfferOperationV1
    offer: PlacementOfferV1 | None
    selection_capture_id: str | None


@dataclasses.dataclass(frozen=True)
class OptimizationOfferPlanV1:
    decisions: tuple[TaskPlacementDecisionV1, ...]
```

`TaskPlacementDecisionV1` is a validated direct-construction dataclass. Its
`task_index` is nonnegative, its fingerprint has the exact SHA-256 grammar, and
its operation is exactly `PLAN_CREATE`. A non-null offer must also have
operation `PLAN_CREATE`, must equal the decision operation, and requires a
non-null canonical UUIDv4 `selection_capture_id`. A null offer may carry either
a null selection capture ID when classification ended before a provider
capture, or a non-null canonical UUIDv4 ID when the captured observation
produced no selected offer. Thus offer presence implies capture-ID presence,
but capture-ID presence does not imply offer presence. `FRESH_CREATE`, `REUSE`,
and `RESTART` decisions are rejected because their authoritative state is
carried separately after the locks.

`OptimizationOfferPlanV1` accepts only an exact tuple of decisions, preserves
their supplied order, and rejects any negative or duplicate `task_index`.

Immediately after every successful pre-lock `Optimizer.optimize()` call, the
internal placement runtime builds a fresh `OptimizationOfferPlanV1` for the
returned DAG and current optimize target using `operation=PLAN_CREATE`.
`task_index` is the task's position in that exact DAG.
`resources_fingerprint` is the canonical normalized placement class and
requested node count, excluding runtime-only image resolution, serialized with
`canonical_json_bytes_v1()` and encoded as `sha256:<64-lowercase-hex>`.
`task_index` is a nonnegative integer and decision indices are unique within a
plan. The runtime
independently ranks the offer projection and compares it with
`task.best_resources`. This plan is comparison evidence only: no
`PLAN_CREATE` decision is copied into `ToProvisionConfig`, `_retry_zones()`, or
`bulk_provision()`.

After live-state refresh under both locks,
`CloudVmRayBackend._check_existing_cluster()` discards the `PLAN_CREATE`
decision, classifies the operation, and, only for `FRESH_CREATE`, captures a new
observation and resolves exactly one offer matching the concrete
`to_provision` winner and objective. Optional
`ToProvisionConfig.placement_offer`, `selection_capture_id`, and `operation`
carry only that newly bound decision into `provision_with_retries()`. The
runtime never writes either plan onto a `Task`, `Resources`, or DAG.

After a cross-region or cross-cloud failure, the retry loop atomically clears
its offer, capture ID, and operation before changing `task.best_resources` or
`to_provision`. It may record a new `PLAN_CREATE` comparison, but if
`provider_attempt_count > 0` it records
`RETRY_AFTER_PROVIDER_ATTEMPT` and leaves the next mutation on the existing
legacy retry path only after the exact attempt fence has been reconciled and
cleared. An unresolved fence stops the retry. A missing, duplicate, stale, or
fingerprint-mismatched first-attempt binding is recorded and leaves mutation on
the legacy path in
shadow mode. It is a fail-closed error for an otherwise eligible authoritative
first attempt. An offer is never carried across a changed placement,
operation, or provider-attempt boundary.

`_retry_zones()` revalidates immediately before `bulk_provision()`. Shadow mode
records the result and leaves mutation unchanged. Authoritative mode either
mutates the exact valid offer or returns the typed unavailable result for an
explicit replan. A `VALID` result replaces the stale in-memory offer before
mutation. The replacement must retain the exact `offer_id`, have a
nondecreasing `observed_at`, and come from a `REQUIRE_FRESH` snapshot with a
different capture ID.

The handoff is an explicit same-process API, not cluster YAML or a subprocess
side channel. The trusted actuation mode is separate from the offer envelope:

```python
class PlacementOfferActuationModeV1(enum.Enum):
    SHADOW = 'shadow'
    SHADOW_LEGACY_FALLBACK = 'shadow_legacy_fallback'
    AUTHORITATIVE = 'authoritative'
    LEGACY_FIRST_ATTEMPT = 'legacy_first_attempt'
    LEGACY_RETRY_AFTER_PROVIDER_ATTEMPT = (
        'legacy_retry_after_provider_attempt')


@dataclasses.dataclass(frozen=True)
class PlacementOfferHandoffV1:
    mode: PlacementOfferActuationModeV1
    offer: PlacementOfferV1 | None
    actuation_context: ProviderActuationContextV1 | None
    provider_attempt_count: int
    reason_code: OfferReasonCodeV1
```

S2b shadow instrumentation gives `provisioner.bulk_provision()` an optional
keyword-only `placement_offer_handoff: PlacementOfferHandoffV1 | None` on the
exact built-in path. This is independent of S2a.2, which creates no lifecycle
bridge or bulk argument. A replacement bulk owner receives no new keyword until
it adopts a reviewed structured extension. `off` passes null. Before M4's
immutable provider descriptor and pinned actuation owner exist, bulk accepts
only `off`, `SHADOW`, or `SHADOW_LEGACY_FALLBACK`; the authoritative
dispositions below are a dormant contract and fail before mutation if selected.
Shadow passes mode `SHADOW`, a valid recursively immutable offer, a null context,
`reason_code=NONE`, and the current positive attempt ordinal. If any shadow
classification, listing, binding, or revalidation outcome has no valid offer,
including `NO_FEASIBLE_SHAPE`, source error, stale identity, or a retry-only
disagreement, shadow passes `SHADOW_LEGACY_FALLBACK`, null offer and context,
the current positive attempt ordinal, and that exact non-`NONE` reason.
`PROVIDER_OBJECT_CONFLICT` is the sole reason forbidden in this disposition.
This mode is accepted only while the re-read server gate is `shadow`, only when
the legacy optimizer selected the concrete mutation candidate, and only when
the cluster handle has no unresolved attempt fence. It is valid for first and
later attempts and never creates or clears a fence. Thus an offer-side
disagreement cannot alter legacy mutation.

An authoritative first attempt passes mode `AUTHORITATIVE`, a valid offer, a
non-null context, `provider_attempt_count=1`, and `reason_code=NONE`.
`PlacementOfferHandoffV1` deliberately has no capture-ID field and the offer
identity deliberately excludes request-local capture IDs, so this dataclass
cannot prove capture provenance by itself. `_retry_zones()` must call
this exact leaf helper immediately before constructing the handoff:

```python
def validate_authoritative_capture_v1(
    offer: PlacementOfferV1,
    capture: ObservationCaptureV1,
    *,
    freshness: ObservationFreshnessV1,
    selection_capture_id: str,
) -> ProviderActuationContextV1:
    ...
```

It requires `freshness is REQUIRE_FRESH`, validates
`selection_capture_id`, requires a non-null context, requires observation and
context provider and capture ID equality, rejects a capture ID equal to
`selection_capture_id`, requires `offer.provider` to equal that provider, and
requires `offer.observed_at` to equal the observation's supplied
`observed_at`. It returns that exact validated context object. Only the
returned object may be put in the handoff.

A deterministic first-attempt legacy fallback passes mode
`LEGACY_FIRST_ATTEMPT`, null offer and context, attempt ordinal one, and the
exact non-`NONE` reason returned by classification. Under the authoritative
gate, accepted reasons are only `UNSUPPORTED_OPERATION`,
`UNSUPPORTED_ACTUATION_KIND`, `UNSUPPORTED_NODE_COUNT`,
`UNSUPPORTED_ACCELERATOR`, `UNSUPPORTED_RESOURCE_MODE`,
`UNSUPPORTED_NETWORK_TIER`, `VOLUME_OR_STORAGE_MOUNT`, `KUEUE_ENABLED`,
`RESERVATION_REQUESTED`, `CUSTOM_PLACEMENT_CONFIG`, `UNRESOLVED_SCOPE`, and
`OBSERVATION_LIMIT_EXCEEDED`.
`NO_FEASIBLE_SHAPE` never reaches mutation because the optimizer has no
candidate in authoritative mode. Reachability, changed configuration or
identity, stale or missing eligible bindings, and
`PROVIDER_OBJECT_CONFLICT` fail or replan; they cannot use this authoritative
fallback.

After any provider mutation was attempted,
an authoritative retry passes mode
`LEGACY_RETRY_AFTER_PROVIDER_ATTEMPT`, null offer and context, an attempt
ordinal of at least two, and reason `RETRY_AFTER_PROVIDER_ATTEMPT`. No other field
combination beyond the dispositions above is valid. Before an authoritative
call to `bulk_provision()`, `_retry_zones()` requires the helper above to accept
the capture; it rejects a missing, non-fresh, reused selection, or
cross-provider context before mutation.
`_retry_zones()` passes the revalidated immutable offer and the pinned context
from that capture in the frozen handoff.
`bulk_provision()` re-reads the server gate. Under an authoritative gate, and
only after the M4 descriptor/actuation readiness gate passes, it
accepts only `AUTHORITATIVE` with attempt ordinal one, the exact allowlisted
first-attempt fallback, or the exact typed legacy-retry combination above.
Under a shadow gate it accepts only `SHADOW` or
`SHADOW_LEGACY_FALLBACK`; every changed or mismatched combination fails before
mutation. For a handoff with an offer, it materializes a fresh built-in
envelope, reparses it,
recomputes its digests, and verifies provider, scope, region, ordered zones,
resource fingerprint, node count, operation, freshness, and revalidation policy
before any bootstrap or provider mutation. It then puts the validated handoff
in an optional in-memory field on the `ProvisionConfig` passed through the
existing provider facet. Kubernetes
bootstrap must retain the identical context object and build every Core, Apps,
Auth, and other API facade from its pinned client; `bulk_provision()` verifies
object identity again before `run_instances()`. `bulk_provision()` returns that
same opaque context beside the record in its process-local result. The backend,
not `bulk_provision()`, owns its lifetime through final cluster-info reads,
runtime setup, the READY transaction, or failure cleanup. Every Kubernetes API
call in that interval, including cluster-info and cleanup calls currently
reconstructed from `provider_config`, must instead use a facade from the pinned
client. A credential error may enter readback or quarantine but may not reload
the context and silently change endpoints. The backend closes the context only
after the READY transaction commits or the attempt is durably quarantined and
all safe same-process cleanup reads finish. No handoff value is written into
`cluster_yaml` or generic provider configuration, and mode, context, attempt
count, and reason are never persisted in a handle.

`KubernetesPinnedActuationContextV1` also owns the exact-target command
transport used after provisioning. It renders a minimal request-local
kubeconfig from the already attached `ApiClient.Configuration`, never from the
ambient kubeconfig path. The directory is mode 0700 and the file is mode 0600;
any required CA, certificate, or key bytes are copied into that directory
rather than referring back to mutable ambient files. Tokens, certificates, and
keys never appear in command arguments or logs, and every file is deleted by
`close()`. `KubernetesCommandRunner` receives that exact path and generated
context name for every `kubectl` invocation through READY.
If the pinned configuration cannot be represented safely for `kubectl`, the
request is not authoritative. `make_deploy_resources_variables()` consumes the
already frozen placement/configuration inputs and performs no later context
lookup only after M4 removes the retained post-bulk callback in favor of the
descriptor-owned deploy-variable snapshot. Before that migration, the current
second callback and its ambient lookup remain authoritative and therefore keep
the placement gate in shadow. `get_cluster_info()`, port-forward or exec setup, runtime setup, final
UID/scope fencing, and failure cleanup all use either facades from the pinned
client or this exact-target transport. Immediately before READY, the backend
re-reads scope and all attempt-owned UIDs through the pinned client.

Authoritative mutation also uses a durable, Kubernetes-specific attempt fence
inside the already persisted early INIT handle:

```python
@dataclasses.dataclass(frozen=True)
class KubernetesOwnedObjectV1:
    api_version: str
    kind: str
    namespace: str
    name: str
    uid: str | None
    state: str


@dataclasses.dataclass(frozen=True)
class PlacementAttemptFenceV1:
    schema_version: int
    attempt_id: str
    state: str
    provider: str
    scope_kind: str
    scope_id: str
    context_name: str
    namespace: str
    namespace_uid: str
    offer_id: str
    created_at: str
    reason_code: str
    owned_objects: tuple[KubernetesOwnedObjectV1, ...]
```

The V1 fence accepts exactly provider `kubernetes`, state `in_flight` or
`quarantined`, operation `fresh_create` by implication, the same bounded scope
fields as the envelope, an RFC 3339 creation time, a UUIDv4 attempt ID, and at
most three objects for the one-node pilot: the two rendered Services and the
head Pod. Object kinds are exactly `Service` or `Pod`, API version is exactly
`v1`, names and namespaces are valid bounded Kubernetes names, UIDs are null or
valid bounded Kubernetes UIDs, and state is exactly `planned`, `created`,
`absent`, or `foreign_replacement`. Raw context, namespace, object names, and
UIDs are persisted only in this internal fence so reconciliation can act; they
are omitted from logs, events, Datadog, client display, and exception strings.
The serialized representation is a validated JSON-built-in dictionary, not a
dataclass instance. `in_flight` requires `reason_code=none`;
`quarantined` requires exactly `provider_error`,
`placement_evidence_mismatch`, `uid_conflict`, `target_scope_changed`, or
`process_recovery`. Unknown fields or values fail closed.

Before calling `bulk_provision()`, `_retry_zones()` derives all three names from
the independently rendered YAML and uses the pinned client to list all
cluster-labelled Pods without a phase filter and read each exact Pod and Service
name. Any Pending, Running, Terminating, Failed, Succeeded, Unknown, deleted-but
still-readable, or same-name object returns the non-failover terminal
`PROVIDER_OBJECT_CONFLICT` result. Authoritative mode never enters the legacy
adoption, stale-Pod deletion, terminating-Pod force-delete, or 409 retry
branches. A create-time 409 after the preflight is a race and enters fenced
quarantine without deleting the conflicting object.

After that all-phase preflight, `_retry_zones()` stamps the attempt ID and full
cluster name on each object template, stores the `in_flight` fence with null
UIDs on the early INIT handle, and commits that handle under the existing
cluster lock. Provider I/O is forbidden until this commit succeeds.
Authoritative bootstrap is a closed path: the required non-default service
account and namespace must already exist, no RBAC, namespace, volume, PVC, or
other shared resource may be created or patched, and the only allowed mutations
are fresh creation of the two rendered Services followed by the head Pod
through the pinned client. Immediately before Pod creation, `run_instances()`
re-reads the ServiceAccount and requires the same UID digest as the revalidated
offer. Every successful create response records its UID in the process-local
result, and every object retains the attempt-ID annotation.

The READY transaction clears `placement_attempt_fence` only after exact
placement evidence succeeds, final pinned-client reads prove the two Services
and Pod still have their create-response UIDs and attempt-ID annotations, and a
final ServiceAccount read has the same identity digest. The transaction then
atomically persists the offer envelope and clears the fence. Any
exception after the fence commit carries the bounded owned-object inventory
back to `_retry_zones()`, which durably advances the fence to `quarantined`
before returning the error. A process crash may leave `in_flight`; recovery
must create a fresh pinned client, recompute the identical scope, read every
planned name, adopt a UID only when the attempt-ID annotation matches, and then
use UID-preconditioned cleanup or quarantine. It never replays a create or
falls through to generic cleanup while the outcome is unknown.

One central backend guard rejects launch, restart, stop, down, autostop cleanup,
retry cleanup, and direct calls into Kubernetes generic terminate or
`cleanup_cluster_resources()` whenever the handle has a non-null attempt fence.
The only allowed mutator is the fenced reconciler for that exact attempt. It
deletes only a stored matching UID and waits for absence. A same-name object
with a different UID is marked `foreign_replacement` and left untouched. The
cluster record and fence remain until every planned name is absent; only then
may reconciliation remove the record without invoking generic cleanup.

The deployment and rollback tooling scans every cluster handle using the
current image and refuses an image rollback while any attempt fence is non-null.
The exact pre-M2 rollback-image qualification therefore runs only after the
fenced reconciler proves zero live fences. Bypassing this preflight is an
unsupported unsafe operator action. Unit and live crash-at-every-create tests
prove the fence is durable before the first provider call, all later lifecycle
entries refuse broad cleanup, foreign replacements survive, and the disposable
test scope ends with no fence or owned object.

`ProvisionConfig.get_redacted_config()` must reconstruct its log dictionary
without traversing the handoff or opaque context, then emit only mode, reason
code, attempt count, schema version, provider, offer ID, and observation ID. It
never logs scope, region, provider payload, evidence, context, or the complete
envelope. The
`ProvisionRecord` logging path may emit `ActualPlacementEvidenceV1` because that
object contains only bounded enums, counts, and digests.

Shadow mode carries the envelope only for logical result comparison; the
provider continues to mutate from the legacy configuration and returns no
`ActualPlacementEvidenceV1`. Authoritative Kubernetes
requires its `run_instances()` path to consume and verify the exact
`ProvisionConfig` envelope. On success, `_retry_zones()` returns that same
validated built-in envelope in its output `config_dict` beside the
`ProvisionRecord`. Only this revalidated envelope can later be persisted on the
READY handle. A changed stable ID, expired replacement, invalid envelope, or
failure to propagate it forces explicit replanning and never mutates with the
stale offer.

The existing `ProvisionRecord.region` and `zone` fields are input echoes for
Kubernetes and are not accepted as actual-placement evidence. This contract is
implemented in M2 S3, not in the S1 generic offer foundation. S3 adds the
separate leaf type and an optional final `ProvisionRecord.placement_evidence`
field defaulting to null:

```python
@dataclasses.dataclass(frozen=True)
class ActualPlacementEvidenceV1:
    schema_version: int
    operation: str
    actuation_kind: str
    provider: str
    scope_kind: str
    scope_id: str
    candidate_zones: tuple[str, ...]
    batching_scope: str
    provider_placement_fingerprint: str
    provider_workload_identity_digest: str
    purchase_mode: str
    requested_nodes: int
    observed_nodes: int
    created_nodes: int
    provider_evidence_kind: str
    provider_evidence_id: str
    provider_attempt_inventory_digest: str
```

Every string uses the corresponding envelope bound. All fingerprint fields are
`sha256:` plus 64 lowercase hexadecimal characters, candidate zones use the
envelope bound, and all node counts are integers 0 through 10,000.
`schema_version` is exactly 1, `operation` is exactly `fresh_create`,
`actuation_kind` is exactly `direct_pod`, and Kubernetes V1 requires
`requested_nodes == observed_nodes == created_nodes == 1`,
`scope_kind == kubernetes_context_endpoint_identity_namespace_v1`,
`batching_scope == context`,
`provider_evidence_kind == kubernetes_pod_binding_v1`, an empty zone tuple, and
`purchase_mode == on_demand`.

The evidence is produced only after `run_instances()` performs a final labeled
API read of the live Namespace and all cluster Pods. Authoritative
`run_instances()` uses the exact pinned client carried through
`ProvisionConfig` for creation and all final reads. Shadow mode does not produce
this evidence.
`scope_id` is recomputed
from the identical canonical five-field tuple used by the offer: context name,
fresh endpoint fingerprint, fresh cloud-identity digest, server-returned
Namespace name, and server-returned Namespace UID.
`provider_placement_fingerprint` is computed from all final server-returned
scheduling inputs above and must match
`rendered_pod_placement_fingerprint` in the offer's provider identity payload.
`provider_workload_identity_digest` is recomputed from the final pinned-client
ServiceAccount namespace, name, and UID and must match
`service_account_identity_digest` in the offer's provider identity payload.
`provider_attempt_inventory_digest` covers the attempt ID and final kind,
namespace, name, UID, and attempt annotation of both Services and the Pod.
`provider_evidence_id` additionally covers both digests and the
context name, freshly loaded endpoint fingerprint and cloud-identity digest; the
server-returned Namespace name and UID; the final Pod namespace, name, UID,
Running phase, and bound `spec.nodeName`; normalized accepted `ray-node` CPU and
memory requests, all other container and Pod scheduling resources, and hard
scheduling constraints; the cluster label and full-name annotation; and the UID
from the original create response. The final Pod UID must equal the
create-response UID. Only bounded digests, enums, and counts leave the provider.
Raw context identity, namespace, Pod UID, Pod name, node name, labels, and
annotations, plus the ServiceAccount name and UID, are neither logged nor stored
in the record. Thus neither scope, workload identity, nor resource evidence can
be satisfied by copying `region`, `provider_config`, or the submitted Pod spec.

Shadow mode returns no exact evidence and leaves the legacy mutation
authoritative. In authoritative mode, the all-phase preflight has already
proved there were no cluster-labelled or same-name Pods or Services, the
dedicated create path has bypassed every adoption and name-only cleanup branch,
and exactly two Services plus one Pod were freshly created. It validates the
evidence against the revalidated envelope inside `run_instances()`. On mismatch
it uses the raw internally held names and UIDs to delete only exact
attempt-created objects with Kubernetes UID preconditions and waits for
absence. It then raises a typed
`PlacementEvidenceMismatchError` whose cleanup disposition is
`QUARANTINE_FENCED`. That exception carries raw created identities only in a
non-stringified internal field; its message and logs contain digests.

`bulk_provision()` catches this error before its broad cleanup handlers, marks
the INIT attempt non-failover, and never invokes existing generic
`terminate_instances()` or `cleanup_cluster_resources()` for it. Those paths
select by label or name and could delete a foreign Pod recreated under the same
name. If the UID precondition reports a replacement, the provider leaves that
replacement untouched and returns its exact owned-object inventory. The backend
durably changes the precommitted attempt fence to `quarantined` before exposing
the error. M2 cleans only a stored exact attempt-created UID whose precondition
still matches; all two Services and the Pod have planned entries before
mutation and captured create-response UIDs when known. No cross-cloud retry
follows this error. Unit and live fault tests inspect the durable fence, prove
the replacement survives, then use the fenced reconciler to clean the
disposable test scope explicitly. The provider never returns a successful
`ProvisionRecord` for an adopted object, UID change, unbound or non-Running Pod,
or evidence mismatch.

The early INIT handle does not contain an offer envelope, but authoritative
mutation requires its precommitted attempt fence. After provider success, the
backend defensively reparses `ProvisionRecord.placement_evidence` and compares
it with the selected envelope rather than trusting `ProvisionRecord.region`.
Only an exact match may copy the envelope to
`CloudVmRayResourceHandle.placement_offer` and clear the fence atomically in the
READY transaction. A failed provider call, failed runtime setup, absent
evidence, or actual-placement mismatch leaves the offer attribute `None` and
the fence non-null; in authoritative mode an absent or mismatched evidence
object raises the same non-failover quarantine error and skips generic cleanup
rather than allowing READY. Other provider or runtime failures retain their
existing typed cleanup behavior only when no attempt fence exists.

Adding the two handle attributes bumps `CloudVmRayResourceHandle._VERSION` from
13 to 14. `placement_offer` and `placement_attempt_fence` each have type exactly
`dict[str, JSONValue] | None`; a placement class, enum, dataclass, `Decimal`,
datetime, mapping proxy, or provider SDK object is forbidden in serialized
state. V13 and older state defaults both attributes to `None`. Pickle and
`to_dict()`/`from_dict()` validate and preserve both built-in envelopes.

Neither envelope is a new public `Resources` field or an independently typed
REST response field. They are opaque optional metadata inside the already
serialized handle. No API version bump is required because supported old
clients can load the built-in state, may retain the unknown attributes, and do
not inspect them. An old server image may not become mutation owner while a
fence exists, which is enforced by the rollback preflight rather than reader
compatibility. A later durable action migration stores the offer and attempt
inventory in its action row.

### Shadow and Deferred Promotion Gates

The M2 shadow implementation, M4 authoritative promotion, and later legacy
removal are separate commits and deployments. M2 cannot enable the
authoritative server mode.

The frozen Kubernetes characterization corpus must cover:

- allowed, filtered, missing, and unreachable contexts;
- effective namespace precedence;
- Ready and non-Ready nodes with identical capacity/allocatable shapes;
- configured zero and nonzero pricing;
- existing-node fit and no-fit;
- every configured autoscaler, including queryable GKE and optimistic
  implementations, falls back before the offer source or revalidation
  constructs or queries an autoscaler client;
- every accepted effective-config property, every excluded built-in Kubernetes
  property, a plugin-registered property, an unknown client-pass-through
  property, built-in module and attribute monkeypatches, every current
  `Resources` mode, direct Pod versus controller/HA actuation, and every
  `NOT_REPRESENTABLE` reason;
- offer expiry and provider-required revalidation;
- every envelope scalar and collection boundary, payload depth and aggregate
  limit, observation context/node/offer overflow, every handoff disposition
  invariant, and `PLAN_CREATE` handoff rejection;
- shadow no-offer, source-error, binding-drift, revalidation-drift, and later
  retry cases all preserve the concrete legacy mutation candidate through
  `SHADOW_LEGACY_FALLBACK`;
- cross-context and cross-cloud reoptimization;
- under-lock create/reuse/restart classification, first-provider-attempt
  authority, and typed legacy retry after an attempted mutation;
- cached-client endpoint A versus freshly configured endpoint B, proving every
  authoritative observation, mutation, post-provision read, runtime setup call,
  and cleanup call uses the client whose configuration was hashed;
- provider-observed namespace, endpoint, Pod UID, rendered-request match and
  mismatch, plus authoritative UID-conflict quarantine that preserves a
  same-name replacement;
- service-account absence, default-account rejection, name and UID binding,
  same-name UID replacement before revalidation, before Pod creation, and
  before READY;
- nonempty ports fall back and never call `_open_ports()` in authoritative V1;
- all Pod phases and both Service names at preflight, plus a create-time 409,
  prove every legacy adoption and name-only cleanup branch is unreachable;
- crash before and after each Service and Pod create, durable attempt-fence
  recovery, every later lifecycle guard, and rollback refusal until
  reconciliation;
- envelope size, redaction, and identity invariants.

Promotion is dormant until M4 has made immutable descriptor dispatch,
provider-wide deploy-variable freshness, and pinned actuation authoritative.
After that prerequisite, promotion requires all of the following on the exact
pushed SHA:

1. Full focused and compatibility CI passes.
2. The frozen corpus has zero unexplained safety, placement-set, optimizer-winner,
   or comparable actual-result mismatches; shadow endpoint-sensitive axes are
   explicitly `not_comparable`, never silently counted as matches.
3. `skypilot-ha` runs in shadow mode for at least 30 minutes and records at least
   20 eligible decisions and three successful one-node create/down cycles in
   `boltz-test`.
4. The live window includes at least one expected `NOT_REPRESENTABLE` decision
   and proves that it remains on the legacy path.
5. Every eligible mutation records revalidation immediately before the provider
   call; an injected-clock test proves the same ordering for an expired offer.
6. Datadog contains every expected bounded shadow event and zero unexplained
   safety, placement-set, winner, or comparable actual-result mismatches.
7. Classified price or availability drift is retained and explained. It is
   nonblocking only when it cannot change safety, the candidate set, the
   optimizer winner, or actual placement.
8. The minimum-compatible old client, the current client, and the captured
   pre-M2 rollback image pass the handle qualification below.
9. With the authoritative-capable image still rollback-safe, an isolated
   `boltz-test` authoritative canary completes three create, runtime setup,
   status, exec, and down cycles with exact offer, scope, UID, Service, and
   pinned-transport evidence. After each cycle the attempt-fence inventory is
   empty.
10. The final Helm revision, immutable image digest, pod readiness, restarts,
   error logs, request execution, cluster cleanup, and absence of orphaned pods
   are re-read and recorded.

The M4 authoritative commit changes only the eligible Kubernetes subset to use the
offer projection and exact selected offer. All other Kubernetes requests retain
the typed legacy fallback, except provider-object conflicts and unresolved
attempt fences, which always fail closed. Rollback changes the server mode to
`shadow` or `off`; an image rollback additionally requires the current-image
preflight to prove every attempt fence is null. M2 introduces no database schema
and leaves both handle fields null in production shadow mode.

Kubernetes-specific shadow and fallback code remains for one full compatibility
release after authoritative promotion. Generic placement reconstruction is not
removed based on a Kubernetes-only gate.

## Provisioning Attempt and Provider Outcome

Provider mutation becomes nonblocking from the orchestration perspective.
The runtime durably creates a `ProvisioningAttempt`, persists the provider
facet's pure `ProviderEffectPlan`, and journals each effect intent before
calling `submit_effect(exact_effect, idempotency_key)`. `observe_effect()`
returns a typed snapshot. Start, create, stop, terminate, and compensation are
closed exact-action kinds rather than provider-selected orchestration.

An attempt contains:

- stable attempt ID and idempotency key;
- resource type, resource ID, incarnation, and desired generation;
- selected offer ID and provider payload version;
- immutable provider-effect plan version and dependency graph;
- requested, existing, resumed, and created counts and IDs;
- per-effect intent, idempotency key, exact readback locator, expected resource
  kinds and cardinality, all known returned child IDs, and provider request or
  operation IDs;
- phase and observation completeness;
- retry disposition and scope;
- external-effect certainty;
- cleanup obligation;
- raw provider code, safe message, and redacted evidence.

Operation state is closed and separate from failure classification:

- `succeeded`
- `pending`
- `failed`

Failure kinds are:

- `capacity`
- `quota`
- `authentication`
- `authorization`
- `invalid_request`
- `throttled`
- `not_found`
- `conflict`
- `transport`
- `provider_unavailable`
- `provider_internal`
- `unknown`

External-effect certainty is independently typed as no effect, confirmed
effect, possible effect, or unknown. Provider adapters emit normalized failure
kind, affected provider locus, raw safe code, request or operation identity,
and effect-certainty evidence. They do not decide request, zone, region,
provider, account, or terminal retry scope. Capacity and failover domain policy
derives retry scope from demand, resource policy, prior attempts, and provider
evidence, then decides whether to retry, fail over, compensate, or quarantine.

## Durable Action Runtime

### Shared mechanics

The shared runtime owns:

- selecting due admitted actions with a fresh database statement clock
  acquired after blocking locks;
- claiming with `FOR UPDATE SKIP LOCKED`;
- lease token, owner, expiry, heartbeat, and attempt count;
- synchronous ownership assertion immediately before provider mutation;
- intent, idempotency key, phase, operation ID, and readback evidence;
- next-attempt time and bounded jittered backoff;
- token-guarded completion;
- deterministic transition ID;
- state transition and lifecycle-event or outbox commit.

The runtime does not own:

- domain admission, controller leadership, or capacity reservation;
- legal domain transitions;
- desired-state policy;
- resource incarnation construction;
- provider observation mapping to domain status;
- compensation selection;
- exact deletion proof.

### Admission and transaction boundary

A domain action is eligible for the generic due query only after a
domain-owned admission transaction has:

1. locked and checked the current resource incarnation and desired generation;
2. acquired or referenced any domain capacity, scheduling, controller, owner,
   or handoff reservation;
3. run the pure planner and validated the requested legal transition;
4. inserted the action with the exact fence and reservation identities.

For central PostgreSQL migrations, these writes use one physical connection
and transaction. At admission, the domain command or reconciler owns that
transaction and calls an action-store insert that must not commit, close, or
open a nested session. During worker execution, the kernel owns each
transaction and calls domain mutations that borrow the supplied connection and
likewise cannot commit, close, or open a nested session. Completion or
supersession updates the domain state, releases or transfers the reservation,
writes the deterministic event, and closes the action in one transaction.

The generic kernel never reconstructs domain eligibility in its due-work SQL.
If a later domain change invalidates admitted work, the desired-generation or
reservation fence fails and the action is superseded, replanned, or retained
for cleanup according to domain policy. Existing image, job, Serve, and
controller reservations are semantic inputs to admission, not second
reservations to be reacquired by the kernel.

Every transaction that touches more than one ownership class uses this global
lock order:

1. database ownership-epoch row for the exact domain, dependency-closed
   operation subset, and store mode;
2. controller, leadership, or domain-owner fence rows;
3. domain parent and resource-incarnation rows;
4. capacity, scheduling, handoff, and reservation rows;
5. action rows;
6. child-request and action-to-child binding rows;
7. the global operational-event sequence row.

Multiple rows within one class are locked in canonical resource-key order.
Action claim may lock only an action row and commit; any later transaction that
also needs domain state reacquires locks from the beginning in the global
order and validates the claim token. No provider I/O occurs while any of these
locks are held. A PostgreSQL deadlock abort retries the whole effect-free
transaction with bounded jitter; it never retries across external I/O.

### Nested request binding

An action that delegates to another SkyPilot API request must bind that child
before dispatch. The outer effect intent stores a deterministic child request
ID derived in the action namespace. If parent and child use the same
PostgreSQL store, child creation and the action-to-child binding commit in one
transaction. Across an HTTP or process boundary, the internal API accepts an
authenticated caller-supplied idempotency and child request ID and atomically
upserts that binding before acknowledging dispatch.

The binding stores the parent action and effect IDs, resource incarnation,
desired generation, canonical request-payload digest, workspace, and
authenticated actor digest. Reusing the child ID is idempotent only when every
bound field matches exactly. The same ID with a different parent, payload,
generation, workspace, or actor fails closed as a conflict and never overwrites
or joins the existing child.

Middleware must not replace the bound ID with an unrelated UUID. A successor
observes, joins, or cancels the exact child request and consumes its terminal
result; it never submits a second anonymous SDK call because the first HTTP
response was lost. A domain whose existing nested API path cannot expose this
binding is not eligible to migrate to the action kernel. Controller action
reservations remain until this parent-child hierarchy and recovery path are
qualified.

### Database clock and connection budget

Lease issue, renewal, expiry, and due-time comparisons use a fresh
`clock_timestamp()` statement executed after any blocking row lock. PostgreSQL
transaction-start time, application wall clocks, and a timestamp read before a
lock wait are forbidden for lease safety.

No database transaction or connection is held across provider or nested API
I/O. The `IN_FLIGHT` intent transaction commits before bytes may be sent.
Heartbeats and synchronous fences use a reserved connection budget or
dedicated bounded pool that worker concurrency cannot consume. If that reserve
cannot obtain a connection before the lease safety margin, the worker stops
starting new effects. An effect whose call may already have occurred follows
the same `READBACK` or quarantine rule; pool starvation never licenses replay.
Provider SDK deadlines and hidden retries are bounded and included in the
lease and ambiguity-horizon calculation. The live fence is rechecked after any
provider-side rate-limit wait and before each underlying SDK attempt.

### External-effect phases

Every mutating action uses these phases:

1. `PRE_INTENT`: identity, idempotency key, desired generation, and cleanup
   obligation are durable before provider I/O.
2. `IN_FLIGHT`: the live owner passed its synchronous fence and may have
   called the provider.
3. `READBACK`: the provider result was lost, pending, or ambiguous and must be
   reconciled.
4. `COMPLETED`: the domain success proof is durable.
5. `QUARANTINED`: automatic progress is unsafe and operator or stronger
   evidence is required.

A lease protects database ownership. It is not proof that a provider side
effect ran once. A stale owner may discard its computation, but the system
must never discard the only known external resource identity.

Reclaim is phase-sensitive. `PRE_INTENT` with no effect intent may be claimed
and planned normally. Once any effect has an `IN_FLIGHT` intent, a new owner
must enter `READBACK` for that exact effect before it can consider another
submission. Lease expiry, a negative point-in-time query, or a worker heartbeat
failure alone can never authorize replay. Replay requires provider-native
idempotency or deterministic conflict adoption plus complete no-effect evidence
after the provider-specific ambiguity horizon. Without that proof, the action
remains in readback or quarantine. This also covers an old process that passed
its final database fence and resumed its network call after losing the lease.

### Store boundary

The runtime is a mechanics library with a PostgreSQL implementation and a
caller-supplied domain adapter. It does not add lease columns to every domain
row.

M3 is not approved by this revision. Before any M3 schema or worker code is
written, this exact file must define and receive a second adversarial approval
for:

- action versus attempt table identity, keys, generations, phases, and
  retention;
- the exact admission-owned and kernel-owned transaction APIs and enforcement
  that borrowed SQLAlchemy connections cannot commit, close, or nest;
- schema-specific realization of the global lock order and bounded
  effect-free deadlock retry;
- migration lineage and downgrade compatibility;
- the exact versioned event source union, M3 volume kind and target enums,
  endpoint negotiation, API version bump, and old-client filtering contract;
- deterministic action-to-child-request keys, authenticated idempotent API
  admission, and child retention;
- lease duration, fresh-statement clock queries, reserved heartbeat connection
  budget, and bounded provider SDK attempt timeouts;
- domain, dependency-closed operation-subset, and store-mode scoped ownership
  epoch and minimum-compatible-reader fields;
- a compatibility reconciler that is shipped and tested before the new writer
  can be enabled;
- backfill and coexistence with `api_controller_action_reservations`;
- schema and transaction tests for the lifecycle-event contract below;
- proving zero unresolved, quarantined, or cleanup-bearing action before any
  pre-N rollback, or retaining a compatible pinned reconciler while pre-N
  mutation ownership stays excluded.

The deployed server resolves global state and API request engines to the same
PostgreSQL URI but intentionally uses distinct process-local pools. Atomicity
is therefore permitted only when the transaction owner passes one physical
connection to every action, domain, reservation, and generalized-event write.
Opening nested sessions or performing cross-connection best-effort writes is
forbidden. If one connection cannot own all required writes, the updated design
must specify a durable outbox and idempotent reducer before M3 can be approved.

The volume pilot is central-PostgreSQL-only, disabled by default behind a
server-side gate. The synchronous public `volumes apply` and `volumes delete`
contract waits for the durable action to reach its terminal result. The old
FileLock and synchronous path remain for officially supported local or
controller SQLite operation until a separate product deprecation gate closes.

Managed container images remain the semantic reference. They move onto the
shared interface only after equivalence tests prove no loss of provider-call
fencing, quarantine, exact cleanup, or scheduler semantics. Their domain
admission reconciler continues to own the two-level due order of shard
`(copy_next_at, id)` then location `(copy_claimable_at, id)`, recovery before
fresh work, and due-shard rotation through `last_dispatch_at`. It atomically
reserves a shard slot and enqueues an action; the generic kernel must not
replace those queries with global action-table policy.

Equivalence also preserves shard `max_in_flight`, exact increment only for a
fresh claim, no increment for lease recovery, exactly-once decrement or
transfer on every terminal path, and publication or capacity reservations.
Crash injection covers `COPY`, `VERIFY`, `EVICT`, and `READBACK` before and
after provider I/O and state commit. Promotion requires the scoped ownership
epoch to prove zero legacy claim owner and zero dual due, lease, heartbeat, or
retry owner. Only the duplicate execution mechanics move to the kernel; image
fairness and reservation policy remain domain admission.

## Domain Planner and Reducer

Each migrated domain exposes two pure seams:

```text
plan(desired, observed, durable_evidence) -> actions
reduce(previous_status, observations, action_results) -> new_status
```

The planner may request an action but cannot execute provider I/O. The reducer
may project status but cannot schedule hidden retries. Product-specific state
remains in the domain.

Pure seams run in shadow mode before taking ownership. Shadow output is keyed
by resource incarnation and desired generation so same-name recreation cannot
compare unrelated resources.

### Pre-M3 volume projection characterization slice

Before M3 adds any schema, claim owner, worker, or mutation path, one additive
slice establishes a pure characterization seam for the current volume refresh
reducer. This slice is named M3-S0. It is not approval for the M3 action store
or volume writer and it cannot be used as evidence that mutation ownership
moved.

One consumer does not yet earn a generic reconciliation package. M3-S0 adds
only `sky/volumes/refresh_projection.py`. A shared comparison helper may be
extracted later only when a second production domain needs the identical,
characterized exception-containment and comparison contract. Until then the
volume module owns its pure projector, tagged snapshots, tagged projections,
and shadow comparison outcome.

There are two frozen snapshot variants. `UsedByFetchFailed` represents the
existing early exit and requires no fabricated current row. `ObservedRefresh`
contains the current status, current error, current ordered usage tuples,
observed error, and observed ordered usage tuples captured while the existing
volume lock is held. There are three tagged projection variants: `SKIP`,
`NO_WRITE`, and `WRITE(payload)`. Only the diagnostic `WRITE` payload uses
tuples. It never supplies arguments to the authoritative writer, which keeps
receiving the original mapped lists from `volume_refresh()`.

The projector preserves the current contract exactly:

- a failed used-by observation produces `SKIP`;
- a truthy provider error has precedence and produces `NOT_READY`;
- otherwise nonempty pod use produces `IN_USE`;
- no pod use produces `READY`, including cluster-only use;
- current and observed usage are compared as sets, so ordering alone produces
  `NO_WRITE`;
- a diagnostic `WRITE` retains observed order, while the legacy writer
  independently receives the original mapped lists.

`sky/volumes/server/core.py::volume_refresh()` remains the sole orchestration
owner. It performs the existing batched error and used-by reads exactly once,
retains the current file lock and latest-row read, computes the current inline
legacy decision, and leaves `global_user_state.update_volume_status()` as the
only status writer. The failed-used-by branch captures its tagged snapshot and
`SKIP` decision without adding a lock, latest-row read, mapping call, or config
refresh. The ordinary branch captures its immutable snapshot and legacy
decision under the current lock but still passes the original lists directly
to the writer.

All candidate projection, equality comparison, and diagnostic reporting run
only after the complete authoritative loop has finished every status write and
config refresh and released every volume lock. A candidate cannot therefore
delay, skip, reorder, or alter authoritative work for the current or a later
volume. Candidate exceptions and comparison exceptions are distinct closed
outcomes, neither retains an exception object or message, and neither aborts
comparison of later snapshots. `BaseException` is never contained as an
ordinary shadow failure.

Deferred capture is explicitly bounded. One sweep may retain at most 128
complete snapshots, 4,096 total current-plus-observed usage references, and
256 KiB of UTF-8 usage-identity bytes. Admission checks all three remaining
budgets before converting any usage list to a tuple. Capture admits only exact
built-in `list` containers and exact built-in `str` identities. Other runtime
types are classified `NOT_SAMPLED_BUDGET` without invoking their overridable
length, iteration, encoding, equality, or hashing behavior. Each admitted list
is traversed once into a temporary list while enforcing the remaining reference
and UTF-8 byte budgets; the immutable snapshot is then built only from those
bounded copies. A snapshot is retained in full or classified
`NOT_SAMPLED_BUDGET`; it is never truncated. Budget accounting itself is
exception-contained after the authoritative work for that volume. These
constants are characterization limits, not volume product limits, and they do
not reject or alter a legacy write or config refresh.

The caller emits no success metric, database row, per-volume warning, or
per-match log. If the complete sweep contains any mismatch, projector error,
comparison error, or `NOT_SAMPLED_BUDGET`, it attempts one bounded warning
suitable for the existing Datadog log pipeline. The warning contains total
compared and closed-outcome counts plus at most three volume names. Those names
are admitted directly into a fixed-size three-element sample; the caller never
collects every anomaly name and slices later. The warning must not contain
provider payloads, exception messages, volume error text, credentials,
projection payloads, or full usage collections. Logging failure is contained
after all authoritative work. A warning-free deployment proves only absence of
reported anomalies; it does not prove a positive match denominator or live
parity.

Config refresh, daemon cadence, public volume commands, apply, delete, and
purge are unchanged.

Volumes do not have the M3 incarnation and desired-generation fields yet.
Therefore M3-S0 parity is diagnostic only and is not same-name-recreation,
ownership-cutover, or authoritative-promotion evidence. M3 still has to add
those fences and pass its complete schema, transaction, compatibility,
activation, and rollback review before any action or writer implementation.

M3-S0 explicitly adds no PostgreSQL or SQLite schema, request-queue reuse,
claim, lease, heartbeat, retry timing, provider mutation, second observation,
shared resource-status enum, generic lifecycle state machine, generic shadow
package, Datadog metric, dashboard, or statistics store. No cluster, Serve,
jobs, pool, image, or API request production path adopts the volume seam in
this slice.

### M3-S1 volume mutation transcript and exact durable action envelope

M3-S1 is the next stacked slice after the deployed M3-S0 shadow. It updates
this canonical design and adds behavior-characterization tests only. It adds no
action table, domain column, migration, producer, worker, queue, due query,
claim, lease, heartbeat, retry owner, provider mutation, request endpoint,
event kind, activation flag, compatibility reconciler, or statistics store.
The legacy volume request handler, provider calls, status writer, database
sessions, and public synchronous behavior remain authoritative and unchanged.

The characterization fixes three logical keys separately from their mutable
record state and chooses the minimum storage ownership needed by the next DDL
slice:

- An **action** is one stable logical domain transition. Its unique business
  key is the canonical tuple `(control-plane store identity, workspace,
  volume resource key, resource incarnation, desired generation, tombstone
  generation or zero, operation kind)`. Workspace and volume name are
  nonempty fixed-`NFC_V1` UTF-8 strings of at most 256 bytes. Store identity
  and incarnation are canonical lowercase UUID strings. Desired and tombstone
  generations are nonnegative signed 64-bit integers, with zero reserved for
  no tombstone. Operation kind is a closed ASCII enum. Its `action_id` is
  UUIDv5 over the exact encoding below. The record also retains the canonical
  request-payload digest, authenticated
  admission subject, admission ownership epoch and fence, cleanup requirement,
  request correlations, and mutable terminal domain result. A retried HTTP
  request joins the existing action only when its business key, operation,
  payload digest, and authenticated subject match exactly; otherwise admission
  returns a conflict. An initiating request ID is correlation, never part of
  action identity. An action survives worker and process changes and cannot be
  replaced merely because another attempt is due.
- An **attempt** is one bounded ownership interval for an action. Its key is
  `(action_id, monotonically increasing attempt number)`. A unique claim token
  fences the mutable ownership record but is not attempt identity. The record
  retains worker identity, database-clock lease and heartbeat times, start and
  finish times, and the normalized attempt outcome. Attempt identity never
  doubles as provider-effect identity and lease expiry never proves that an
  effect did not occur.
- An **effect** is one deterministic provider-side step within an action. Its
  unique key is `(action_id, dependency ordinal)`, where ordinal is zero-based
  and limited to a nonnegative signed 32-bit integer, with a stable UUIDv5
  `effect_id` derived from the action and ordinal. Immutable fields are the
  versioned effect kind, canonical effect payload and digest, actuation
  binding, idempotency key, and initial readback locator. Mutable fields are a
  returned provider operation identity, exact observed resource UID, cleanup
  obligation, phase, certainty, and normalized outcome. A retry observes or
  continues the same effect. It cannot create a fresh effect identity to hide
  an ambiguous prior submission.

The PostgreSQL expand layout uses a sidecar
`volume_lifecycle_resources` row keyed by the currently global logical volume
name. It can exist without a legacy `volumes` row and therefore owns absent-name
reservations, incarnation, desired and tombstone generations, workspace,
writer mode, ownership epoch, current binding digest, public visibility, and
domain state. For an action-owned live or tombstoned provider resource it also
owns the versioned canonical base locator and digest, exact observed provider
UID, comparison-rules version, canonical requested projection and digest, and
ownership-annotation version. Those fields cannot be cleared or replaced by
effect compaction; they change only through an incarnation-fenced domain
transition and remain until exact cleanup proof is durable. It is not deleted
on ordinary volume deletion. The existing `volumes` row remains the old-reader
public projection and may be absent while the sidecar retains a tombstone. A
volume row with no sidecar is unambiguously legacy. The dark identity slice
creates no sidecar table or row; later action admission creates or locks the
sidecar and writes its action and effects in one transaction. The sidecar has
no foreign key to `volumes`, because the name reservation and tombstone must
outlive that row.

A new incarnation is random UUIDv4 persisted before any provider I/O, with
desired generation 1 and tombstone generation 0. Each admitted desired-state
change increments desired generation. Delete increments desired generation and
sets tombstone generation to that same value. Same-name recreation is legal
only after the prior cleanup gate, allocates a fresh UUIDv4, and restarts its
per-incarnation desired generation at 1 with tombstone 0. UUID identity, not a
reused generation number, separates old and new effects.

The control-plane store identity is a UUID in one singleton PostgreSQL row. It
is created once before any sidecar or action row, is never derived from a pod,
hostname, context, or image, and is preserved by backup and authoritative
restore. Database state alone cannot distinguish a source from its clone. The
same row therefore carries a nullable SHA-256 digest of an external 256-bit
writer-authority seal. The seal itself is not stored in this database or its
backup. Seal possession and its database digest are proof material, not
exclusive authority. A dark store has a null digest and cannot write lifecycle
work.

Before any scope may leave `DARK`, the deployment must acquire an externally
enforced, short-lived, renewable single-writer grant by compare-and-set. The
grant record is keyed only by store UUID and exact scope key. Its value carries
the authority generation. Acquisition or transfer compares the expected prior
generation and cannot replace any unexpired grant except a same-holder renewal;
using generation as part of the record key is prohibited because two
generations could then remain live. The grant is issued only through
deployment-specific workload identity and is excluded from the database, Helm
values, deployment backups, and ordinary secret copying. Its signed or
attested payload binds the store UUID, scope key, authority generation, target
ownership epoch, deployment identity, writer and reconciler implementation
digests, serial, issue time, and expiry. Every later legacy-intent producer,
action producer, reconciler, worker, and underlying provider attempt must
freshly validate that live grant plus the seal digest and scoped epoch. Cached
seal or grant possession is insufficient. Datadog observes expiry or
split-brain signals but never grants authority.

An authoritative restore or transfer first enters `DRAINING`, fences the
source workload identity and its provider-call credentials or provider-call
authority proxy, and revokes the old grant or waits for its expiry. It then
waits the reviewed maximum check-to-call, provider-call, and hidden-SDK-retry
ambiguity horizon and reconciles any call that may have escaped to readback or
quarantine. Only after that proof may it increment the authority generation
and ownership epoch and issue a target-bound grant. Grant expiry alone is
insufficient because a paused old holder could have validated immediately
before expiry and call afterward. A database and secret clone cannot acquire
the source's unexpired external grant. A fork may receive a new store UUID,
seal, and authority lineage only after proving that it has no action-owned
volume and no nonterminal, quarantined, or cleanup-required action; terminal
evidence keeps its original store UUID. The exact external grant service,
credential fence, ambiguity horizon, and transfer protocol require the later
activation review and are not implemented by the inert M3-S2 slice.

Action UUIDv5 namespace is
`e4abeb94-a9e7-4722-a9e7-2776d6d9e18b`. Effect UUIDv5 namespace is
`1bdc5439-e342-4933-b48d-18f295d8023d`. For either identity, `LP(x)` is the
ASCII decimal UTF-8 byte length of `x`, one colon, then the exact UTF-8 bytes of
`x`. The action UUID name bytes are
`skypilot-volume-action-v1` followed in order by `LP()` encodings of canonical
store UUID, workspace, logical volume name, incarnation UUID, desired
generation decimal, tombstone generation decimal, and operation enum. There
are no nullable fields or implicit separators. The effect UUID name bytes are
`skypilot-volume-effect-v1` followed by `LP()` of canonical action UUID and
zero-based ordinal decimal. These byte strings are valid UTF-8 and are passed
unchanged as the UUIDv5 name. The generic idempotency key is
`skypilot-volume-v1:<lowercase effect UUID>`; a facet may use a
provider-constrained lossless encoding only when its collision proof is part of
the binding. The Kubernetes pilot persists the effect UUID directly in its
reserved annotation.

Effect phase is closed to `PRE_INTENT`, `IN_FLIGHT`, `READBACK`, `COMPLETED`,
or `QUARANTINED`. Effect certainty is independently closed to
`NOT_SUBMITTED`, `SUBMISSION_UNKNOWN`, `PROVIDER_ACCEPTED`, `EXACT_PRESENT`,
`EXACT_ABSENT`, `REPLACED`, or `FOREIGN_CONFLICT`. An action terminal result is
closed to `SUCCEEDED`, `SUCCEEDED_REPLACED`, `CONFLICT`, or `FAILED_SAFE`.
`QUARANTINED` is a nonterminal action state with no terminal result.
`REPLACED` means the exact old provider UID is absent while the same name now
resolves to another UID. It completes cleanup for the old incarnation but
never authorizes deletion of the replacement.
`DETACHED_CLEANUP_PENDING` is a nonterminal domain-visibility state, not a
success result: force purge may hide the volume only after the same admission
transaction persists the exact cleanup effect and locator. The action remains
owned until absence is proved or it reaches visible quarantine.

Runnable ownership is not stored on an effect. An action owns lifecycle state
closed to `WAITING`, `OWNED`, `QUARANTINED`, or `TERMINAL`, its
database-clock `next_attempt_at`, monotonically increasing `attempt_count`,
nullable `current_attempt_number`, retry counters, and terminal result. An
attempt owns its unique claim token, worker identity, database-clock lease and
heartbeat, start and finish times, and an outcome that is null while live and
then closed to `SUCCEEDED`, `READBACK_REQUIRED`, `RETRYABLE_NO_EFFECT`,
`FENCED`, `LEASE_EXPIRED`, `QUARANTINED`, or `FAILED_SAFE`.

Claiming locks one due `WAITING` action, requires no current attempt, increments
`attempt_count`, inserts exactly that attempt, and changes the action to
`OWNED` with its current-attempt pointer in the same transaction. A unique
partial constraint permits only one unfinished attempt per action, and the
action pointer has a composite foreign key to its attempt. Completion or retry
locks both records, validates the claim token and fresh database-clock lease,
finishes the attempt, clears the pointer, and changes the action to `TERMINAL`,
`QUARANTINED`, or `WAITING` with a newly computed
`next_attempt_at`. An expired owner is finished as `LEASE_EXPIRED`; any effect
that reached `IN_FLIGHT` or has `SUBMISSION_UNKNOWN` returns through
`READBACK`, while exact `PRE_INTENT/NOT_SUBMITTED` work may be retried without
readback. Backoff and due time live only on the action; attempt rows are
immutable after finish.

The legal effect pairs are exact. Initial state is
`PRE_INTENT/NOT_SUBMITTED`. The final pre-call transaction writes
`IN_FLIGHT/SUBMISSION_UNKNOWN` before releasing its lock for the provider call.
Acknowledgement writes `READBACK/PROVIDER_ACCEPTED`; timeout, lost response, or
lease loss writes or retains `READBACK/SUBMISSION_UNKNOWN`. Create completes
only as `COMPLETED/EXACT_PRESENT`; delete completes only as
`COMPLETED/EXACT_ABSENT` or `COMPLETED/REPLACED`; an observed foreign object
completes as `COMPLETED/FOREIGN_CONFLICT`; and a proved no-call failure may
complete as `COMPLETED/NOT_SUBMITTED` with action result `FAILED_SAFE`.
`QUARANTINED` preserves the prior certainty and cannot conceal it. No other
phase and certainty pair is valid. Only nonterminal certainty
`NOT_SUBMITTED`, `SUBMISSION_UNKNOWN`, or `PROVIDER_ACCEPTED` may enter
`QUARANTINED`, and the effect records its exact pre-quarantine phase.

Cleanup state is separately closed to `NONE`, `REQUIRED`, or `PROVED`. Delete
starts `REQUIRED` and becomes `PROVED` only with `EXACT_ABSENT` or `REPLACED`.
A still-desired create remains `NONE`; superseding an ambiguous or present
create atomically changes cleanup to `REQUIRED` and admits its exact delete
effect before the create action can close. Force purge retains `REQUIRED`.
`TERMINAL` requires every effect completed, no cleanup state `REQUIRED`, and an
exact result-compatible certainty. `QUARANTINED` and
`DETACHED_CLEANUP_PENDING` are never selected as terminal success.

Quarantine is reactivated only by an audited reconciliation transaction. It
requires no live attempt, proves that the original binding is again resolvable
or that an approved compatibility adapter exists, increments the action
ownership epoch, appends the operator or deploy-reconciler identity, changes
each proven `NOT_SUBMITTED` effect back to
`PRE_INTENT/NOT_SUBMITTED`, changes each `SUBMISSION_UNKNOWN` or
`PROVIDER_ACCEPTED` effect to `READBACK` with certainty unchanged, and sets the
action to `WAITING` at database time. The safe branch requires recorded
pre-quarantine phase `PRE_INTENT` and zero submissions. An unknown effect never
returns to `PRE_INTENT` or directly to submission. Merely starting a different
image does not requeue quarantine.

Nonterminal, quarantined, detached-cleanup, and cleanup-required rows have no
time-based expiry. Full terminal action, effect, attempt, and evidence rows are
retained for at least 90 days, configurable only upward. Compaction then writes
an immutable identity tombstone in the same transaction, retaining action ID,
complete business key and payload digests, authenticated-subject digest,
terminal result, an ordinal-keyed final-certainty and cleanup-proof entry for
every effect, original binding digest, and completion time before removing
bulky attempt or evidence data. A scalar effect summary is not sufficient.
Compaction cannot remove a locator, UID, requested projection, binding, or
cleanup fact still needed by a live or tombstoned domain resource; the sidecar
retains that provider identity independently of effect retention. Identity
tombstones and `volume_lifecycle_resources` rows persist for the lifetime of
the control-plane store and continue to reject reuse of an old business key.
Executable compatibility implementations may be removed only when no
nonterminal effect references the binding; terminal tombstones retain the
digest but do not require executable code.

The API request row is not this ledger. Existing volume handlers use the
ordinary request system and its request-lifetime policy, which can cancel a
request after lease loss without resolving an ambiguous provider mutation. A
future action may retain and correlate the initiating request ID, but effect
recovery, readback, cleanup, and retention remain independently durable. The
public request handler must not remain a second provider-call owner. Normal
apply and delete wait for the action terminal result. Force purge instead waits
only for the atomic visibility transaction, which stores request-facing result
`DETACHED_CLEANUP_PENDING`, action ID, and exact cleanup locator before hiding
the legacy volume row; it then completes the current request with the same
successful detach semantics while cleanup continues durably. Request-facing
result and action terminal result are separate columns. New clients may expose
the action correlation after a separately versioned API change; old clients
retain their current empty successful response.

Admission is one exact transaction boundary. Typed provider preflight reads
run before the transaction and carry target identity, capture time, and a
bounded freshness deadline. The admission transaction locks the existing
`volume_lifecycle_resources` name reservation or conflict-safely inserts and
locks an exact sidecar when the name is new. It revalidates the authenticated
subject, expected incarnation, desired and tombstone generations, domain
fence, payload digest, and preflight freshness, then writes the desired volume
transition, action, and complete deterministic effect plan through one borrowed
PostgreSQL connection. A unique
business-key conflict either joins the exact matching action or fails; it never
silently discards a different payload. No action is claimable before this
transaction commits, and no provider I/O or provider polling occurs while a
database lock is held. Existing `global_user_state` helpers that open and
commit their own sessions cannot be used by this transaction.

The locked volume reservation or tombstone serializes apply, attachment
admission, and delete for the exact incarnation. The deterministic provider
name is derived from that incarnation and stored in the same transaction, not
randomly generated inside a later handler.

Immediately before a provider call, the action kernel obtains a typed live
precondition observation without holding a database lock. It then opens a
short transaction, locks the action and volume rows, validates the claim token,
database-clock lease, action and domain fences, target binding, observation
freshness, and effect phase, records the observation, and commits before one
bounded provider call. Delete `usedby` policy belongs to the volume domain, but
Kubernetes usage is a typed provider observation and is refreshed at this
pre-effect fence. A nonempty or incomplete usage observation prevents delete.
StorageClass existence/default checks and their current 401/403-unverified
outcome are typed create-preflight observations. They do not become generic
action-kernel policy.

Responsibility is fixed as follows for DDL design:

- the volume domain admission owner controls incarnation and desired
  generation construction, tombstones, legal transitions, duplicate-name and
  `usedby` checks, purge policy, and the user-visible terminal result;
- the generic action kernel controls already-admitted due selection, claim,
  fresh-database-clock lease, heartbeat, bounded backoff, effect phase,
  pre-call live fencing, and connection-borrowing transaction orchestration;
- a provider volume facet controls bounded raw submission and typed
  observation or exact-absence evidence, but no request scheduling, domain
  status transition, blind retry, or database commit;
- the volume reducer maps typed observation and effect evidence into volume
  domain state; and
- Datadog remains the observation plane. M3-S1 adds no statistics database or
  parallel metrics store.

The candidate provider contract is `VolumeActuationV1`:

```text
observe_preflight(exact_intent, target) -> PreflightObservation
prepare(exact_action, preflight, effect_observation?) -> EffectPlan
submit_effect(exact_effect, idempotency_key) -> EffectEvidence
observe_effect(readback_locator) -> EffectObservation
prove_absent(readback_locator) -> AbsenceEvidence
```

Each method accepts and returns exact versioned value types. Submission is one
bounded provider SDK attempt and does not poll to readiness; observation is a
separate operation. `prepare()` is pure and cannot access credentials, the
database, or the provider. The facet cannot translate a transport timeout into
no-effect evidence. Observation and absence proof must distinguish the exact
resource incarnation from a later same-name resource. The current generic
Kubernetes delete retry helper and RunPod's method-wide HTTP retry layer are
prohibited inside `submit_effect()` because they can hide several mutations
behind one live fence.

M3 does not make the full `ProviderDescriptorV1` authoritative. The volume
pilot instead requires one immutable `VolumeActuationBindingV1` containing the
canonical provider, facet contract version, dependency-closed operation
subset, realized provider mode, `central_postgresql` store mode, and stable
implementation digest. For Kubernetes it also contains a credential-free
transport identity digest over the effective API-server origin, TLS server
name, CA identity or explicit insecure mode, plus the target namespace name
and observed namespace UID. The immutable base readback locator contains that
target plus PVC name. A first exact observation appends PVC UID as
claim-fenced, versioned effect evidence; it never mutates the base locator. A
context name alone is not an identity because kubeconfig can retarget it. Every
effect retains its original binding, and each migrated volume retains an
explicit action-writer and current binding marker.

The runtime registry must resolve every nonterminal binding to its exact
compatible implementation. A deploy preflight rejects removal of a binding
while a nonterminal effect references it; released images retain the current
and prior compatible implementations for the reviewed retention window. If a
binding is nevertheless unavailable, the worker performs no provider call and
sets the effect phase to `QUARANTINED` and the action state to
`QUARANTINED`, with a bounded Datadog alert. Recovery requires
either rollback to an image that resolves the binding or a separately audited
compatibility-adapter row that retains the original binding, pins the adapter
implementation digest, and proves the old locator and payload map exactly. The
effect binding is never rewritten. A plugin or implementation replacement
never inherits an old effect merely because the provider name is unchanged.

The first future mutation pilot is limited to new SkyPilot-owned Kubernetes PVC
create with `use_existing` false, and delete only for PVCs created or explicitly
adopted by that action writer, all on central PostgreSQL. It is not activated by
M3-S1. Legacy PVC rows have no trustworthy ownership marker or stored UID;
their delete path remains legacy until a separately reviewed observation and
backfill proves exact ownership. Mixed-version activation uses the per-volume
writer marker, so a legacy-created PVC cannot switch delete owners merely
because the server was upgraded.

That marker is not the mixed-version fence by itself. Ownership lives in
`lifecycle_ownership_scopes`, keyed by `(domain, dependency-closed operation
subset, store mode)`. The volume pilot key is exactly
`(VOLUME, KUBERNETES_PVC_OWNED_LIFECYCLE_V1, CENTRAL_POSTGRESQL)`. Routing mode
is closed to `DARK`, `LEGACY_OPEN`, `DRAINING`, or `ACTION_OPEN`. `DARK` is an
inert expanded schema that current legacy handlers do not consult and from
which no lifecycle producer or worker may claim authority. The scoped row also
stores `minimum_lifecycle_version`, a monotonically increasing ownership
epoch, a monotonically increasing authority generation, and nullable selected
writer and reconciler implementation digests.

Before a later compatibility release changes `DARK` to `LEGACY_OPEN`, every
`all`, `api`, `controller`, or `executor` supervisor eligible to host or
deliver a volume handler must advertise its supported lifecycle version,
exact image digest, role, and binding and reconciler support through a
database-clock heartbeat in a separately reviewed process-capability table.
An executor supervisor's advertisement covers only child handlers from the
same exact image, and active attempts record both identities. `LEGACY_OPEN`
requires every
legacy mutation admission to register an exact legacy intent and validate the
scope epoch and writer-authority seal. Activation later enters `DRAINING`,
rejects new apply and delete admission, waits for every earlier volume request
and known provider call to finish, and terminates every older eligible process.
Lease-lost or otherwise unresolved legacy requests are imported as explicit
`legacy_volume_intents` keyed by every affected logical name, request payload
digest, and request execution identity. Those names reject action admission
until a separately pinned reconciler proves provider and legacy-row outcome;
request cancellation alone is not resolution. Activation requires no old
process heartbeat and every remaining eligible process plus the pinned
reconciler to advertise `volume_action_v1`. One transaction then increments
the ownership epoch and sets mixed routing: no-sidecar rows remain legacy,
while an action-writer sidecar routes exclusively to the action handler. Every
later admission, attempt claim, pre-call fence, domain write, and cleanup
validates that epoch.

Once any action-owned sidecar exists, rollback to a pre-action binary is
prohibited because that binary ignores the sidecar and epoch. Supported
rollback is only to an image that still contains the mixed router, original
binding implementation or approved adapter, and pinned action reconciler. The
deployment preflight refuses an older image while the ownership scope is past
`DARK` or any action-owned sidecar remains. Returning to a pre-action binary
requires a separately reviewed reverse migration that first proves no
action-owned volume, nonterminal effect, quarantine, or cleanup obligation
remains. A forced old binary outside that gate is explicitly unsupported and
cannot be treated as a rollback path.

Admission persists a deterministic `name_on_cloud`, exact target binding,
canonical requested PVC projection, comparison-rules version, and readback
locator before create. The provider body reserves these annotations:
`skypilot.co/volume-incarnation`, `skypilot.co/volume-effect-id`, and
`skypilot.co/requested-spec-sha256`. User metadata under the
`skypilot.co/` prefix is rejected rather than overwritten. The canonical
requested projection contains namespace name and UID, PVC name, the exact
single requested access mode, storage request as canonical bytes, the
three-way storage-class intent of omitted, empty, or explicit name, and
volume-mode intent with missing and `Filesystem` defined as equivalent. The
readback projection ignores server-owned UID, resource version, status, bound
volume name, finalizers, and other admission fields; it canonicalizes quantity
values; requires exact ownership annotations, access mode, requested storage,
and every explicit user-controlled immutable field; and applies only the
persisted storage-class and volume-mode defaulting equivalences. A preflight
default-class set or 401/403-unverified result is persisted so a later
readback uses the same reviewed equivalence rule rather than current mutable
cluster policy.

Create begins with an exact GET. A 404 permits one create submission. An
existing nonterminating PVC is adopted only when the reserved ownership
annotations and normalized projection match exactly; its UID is then persisted
with a claim-fenced compare-and-set. Any foreign marker or immutable-spec
mismatch is terminal `CONFLICT`. An exactly matching PVC with a
`deletionTimestamp` remains in `READBACK`: the worker neither adopts it as
complete nor submits create until exact absence is observed, after which the
same effect identity may submit the deterministic create. A lost create
response or 409 moves to GET readback and never directly replays POST.

The effect retains `submission_count`, database-clock call start and deadline,
and every readback observation. Before the first submission, an exact GET 404
permits POST immediately. After an ambiguous submission, GET 404 is evidence
but not immediate replay permission. The same effect and byte-identical body
may submit again only after the prior call deadline plus a 30-second ambiguity
horizon, three complete 404 GETs from fresh calls spanning that horizon with at
least five seconds between observations, unchanged target and ownership epoch,
a fresh claim and final fence, and `submission_count < 3`. Any incomplete read,
matching object, foreign object, or target change resets or terminates that
decision as specified above. If the third submission remains ambiguous and
the same absence horizon completes, the effect enters `QUARANTINED` with
certainty `SUBMISSION_UNKNOWN`; it never creates a fourth POST. Counts and
horizon evidence survive attempts and process restarts.

Create action completion means an exact, nonterminating, owned PVC object has
been observed with the persisted comparison projection. It means Kubernetes
API acceptance and object existence, preserving current synchronous semantics;
it does not wait for `Bound`. `Pending` is retained as typed observation for
the existing refresh and Datadog diagnostics and does not make the create call
block.

Delete carries target transport identity, namespace UID, PVC name, and the
stored PVC UID. Its one submission uses a Kubernetes UID precondition. A lost
response enters readback. GET 404 proves `EXACT_ABSENT`; GET of the same UID
with a deletion timestamp remains in readback; GET of the same UID without a
deletion timestamp may re-submit the same effect after a fresh claim and usage
fence; and GET of a different UID proves `REPLACED`, completing the old cleanup
as `SUCCEEDED_REPLACED` without ever deleting the replacement.

HostPath cleanup, `use_existing`, RunPod network volumes, local SQLite,
controller SQLite, clusters, Serve, pools, jobs, images, and generic provider
descriptor dispatch are excluded from the pilot. HostPath deletion is a
multi-effect cleanup, and RunPod create currently depends on a
provider-assigned identifier; both need separate effect and readback contracts
after the one-effect PVC pilot is qualified.

M3-S1 freezes the current legacy transcript with passing tests before any fix
is implemented:

1. Non-`use_existing` volume apply records random `name_on_cloud` generation
   before the file lock, then the current-row read, provider create, and
   database insert order under the lock. `use_existing=true` instead preserves
   the logical name without UUID or name normalization and runs the
   post-provider duplicate check; the new-PVC path does not run that check.
2. Provider create success followed by database insert failure records the
   current orphan-resource window without deleting or compensating in the
   test. The global insert's current `ON CONFLICT DO NOTHING` and absent
   affected-row check are also frozen.
3. Provider delete success followed by database deletion failure records the
   current stale-row window without fabricating a failed provider call. Delete
   reads the row and performs the provider `usedby` observation before taking
   the file lock, does not re-read under that lock, and fires its best-effort
   hook only after the independent database delete commits.
4. Purge records the current behavior that removes the database row after a
   provider deletion failure. This is characterization, not approval; the
   future action writer must retain a cleanup obligation instead. Two
   concurrent same-name deletes may both call the provider and hook, and a
   multi-name delete can commit a prefix then fail on a later name such that a
   retry of the original list stops at its now-missing first item; both gaps are
   frozen.
5. Global volume create, update, and delete helpers record their current
   independently opened and committed sessions, proving that future atomic
   action/domain writes require explicit borrowed-connection variants.
6. The current request registry records that volume apply and delete are normal
   workers with `ReplayPolicy.NEVER`. On the PostgreSQL durable request path,
   lease loss cancels the request as an ambiguous mutating outcome, but request
   execution generation and claim token do not fence a stale handler's
   provider call or independent volume database write. The local SQLite path
   has no durable claim lease and ignores those fence arguments. Neither
   request row can serve as the action/effect ledger.
7. Current Kubernetes tests record initial GET 404 then POST; unconditional
   adoption of any same-name PVC without ownership or spec comparison; POST
   409 and transport failure propagation with no second GET; and delete by
   namespace and name through hidden retries, treating 404 as success without
   UID precondition or absence wait. Existing explicit/default StorageClass
   reads and their 401/403-tolerated path are retained. These passing tests
   prove the future readback and UID cases are missing; M3-S1 adds no skips,
   expected failures, or tests that imply the future behavior already exists.
8. The exact diff proves M3-S1 changes only this design and the four named test
   files. Pilot exclusions remain a design gate; runtime assertions arrive
   with the first disabled selector and binding registry.

The smallest characterization patch is limited to
`tests/unit_tests/test_sky/volumes/test_core.py`,
`tests/unit_tests/test_sky/volumes/test_global_user_state_volumes.py`,
`tests/unit_tests/test_sky/volumes/test_k8s_volume.py`, and
`tests/unit_tests/test_api_requests_pg.py`. RunPod remains exclusion evidence,
not a pilot or a new test owner. The future exact-match, terminating-object,
Pending, UID-delete, lost-response, replacement, and binding-unavailable cases
remain acceptance requirements for the later implementation; they are not
executed against unchanged production code in M3-S1.

The M3-S1 exit gate is an adversarial `PURSUE` verdict over the exact section
digest plus a passing characterization corpus with no production behavior
change. The review must find the action, attempt, effect, transaction,
provider-binding, request-correlation, activation, compatibility, and rollback
boundaries complete enough to design the inert identity foundation without
guessing. It does not approve the full action graph DDL.

Only the next separately reviewed slice, M3-S2, may add a new PostgreSQL-only
Alembic lineage sharing the ordinary central engine. That slice is limited to
exactly two tables: `lifecycle_store_identity` and
`lifecycle_ownership_scopes`. It seeds exactly one `global` store row with a
random UUIDv4, schema version 1, null writer-authority digest, and database
creation time, plus exactly one volume-pilot scope row with the key above,
`DARK`, minimum version 0, ownership epoch 1, null writer and reconciler
digests, authority generation 0, and database update time. Startup fails closed
on a missing, non-singleton, malformed, or schema-version-mismatched store
identity, and the runtime repository exposes no operation that can replace its
UUID. Downgrade may remove only those exact inert seed rows after locking both
tables and proving there are no other rows; otherwise it fails closed. The
lineage's Alembic version table is migration metadata, not a third lifecycle
data table.

M3-S2 adds no sidecar, process-capability, binding, legacy-intent, action,
request-correlation, attempt, effect, evidence, compatibility-adapter,
identity-anchor, or identity-tombstone table or row. It changes no volume,
request, provider, routing, reconciliation, or event code and ships no
producer, queue, fetcher, heartbeater, worker, or deployment seal. Runtime
roles verify the lineage only; the Helm migration job remains the sole DDL
owner. Local and controller SQLite never initialize or emulate this lineage.

#### M3-S2 exact inert PostgreSQL foundation

This subsection is the canonical M3-S2 relational, runtime, rollback, and test
contract. M3-S2 uses Alembic section `lifecycle_actions_db`, numeric revision
`001`, version table `alembic_version_lifecycle_actions_db`, migration directory
`sky/schemas/db/lifecycle_actions`, and runtime package
`sky/lifecycle_actions`. It is a separate PostgreSQL-only lineage on the
ordinary central engine. It does not use an engine namespace or create another
connection pool. The only lifecycle data tables are the following two tables;
the Alembic version table is metadata.

Revision `001` owns literal migration DDL equivalent to this exact shape. The
runtime SQLAlchemy metadata independently declares the same columns,
PostgreSQL types, order, defaults, nullability, primary keys, and named checks.
The migration must not import runtime metadata.

```sql
CREATE TABLE lifecycle_store_identity (
    store_key TEXT NOT NULL,
    store_uuid UUID NOT NULL,
    schema_version INTEGER NOT NULL,
    writer_authority_digest TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT pk_lifecycle_store_identity
        PRIMARY KEY (store_key),
    CONSTRAINT ck_lifecycle_store_identity_singleton
        CHECK (store_key = 'global'),
    CONSTRAINT ck_lifecycle_store_identity_uuid_v4
        CHECK (
            store_uuid::text ~
            '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        ),
    CONSTRAINT ck_lifecycle_store_identity_schema_version
        CHECK (schema_version = 1),
    CONSTRAINT ck_lifecycle_store_identity_writer_authority_format
        CHECK (
            writer_authority_digest IS NULL OR
            writer_authority_digest ~ '^[0-9a-f]{64}$'
        ),
    CONSTRAINT ck_lifecycle_store_identity_m3s2_unsealed
        CHECK (writer_authority_digest IS NULL),
    CONSTRAINT ck_lifecycle_store_identity_created_at_finite
        CHECK (isfinite(created_at))
);

CREATE TABLE lifecycle_ownership_scopes (
    domain TEXT NOT NULL,
    operation_subset TEXT NOT NULL,
    store_mode TEXT NOT NULL,
    routing_mode TEXT NOT NULL,
    minimum_lifecycle_version INTEGER NOT NULL,
    ownership_epoch BIGINT NOT NULL,
    authority_generation BIGINT NOT NULL,
    writer_implementation_digest TEXT NULL,
    reconciler_implementation_digest TEXT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT pk_lifecycle_ownership_scopes
        PRIMARY KEY (domain, operation_subset, store_mode),
    CONSTRAINT ck_lifecycle_ownership_scopes_domain
        CHECK (domain = 'VOLUME'),
    CONSTRAINT ck_lifecycle_ownership_scopes_operation_subset
        CHECK (operation_subset = 'KUBERNETES_PVC_OWNED_LIFECYCLE_V1'),
    CONSTRAINT ck_lifecycle_ownership_scopes_store_mode
        CHECK (store_mode = 'CENTRAL_POSTGRESQL'),
    CONSTRAINT ck_lifecycle_ownership_scopes_routing_mode
        CHECK (
            routing_mode IN
                ('DARK', 'LEGACY_OPEN', 'DRAINING', 'ACTION_OPEN')
        ),
    CONSTRAINT ck_lifecycle_ownership_scopes_minimum_version
        CHECK (minimum_lifecycle_version >= 0),
    CONSTRAINT ck_lifecycle_ownership_scopes_ownership_epoch
        CHECK (ownership_epoch >= 1),
    CONSTRAINT ck_lifecycle_ownership_scopes_authority_generation
        CHECK (authority_generation >= 0),
    CONSTRAINT ck_lifecycle_ownership_scopes_writer_digest
        CHECK (
            writer_implementation_digest IS NULL OR
            writer_implementation_digest ~ '^[0-9a-f]{64}$'
        ),
    CONSTRAINT ck_lifecycle_ownership_scopes_reconciler_digest
        CHECK (
            reconciler_implementation_digest IS NULL OR
            reconciler_implementation_digest ~ '^[0-9a-f]{64}$'
        ),
    CONSTRAINT ck_lifecycle_ownership_scopes_m3s2_inert
        CHECK (
            routing_mode = 'DARK' AND
            minimum_lifecycle_version = 0 AND
            ownership_epoch = 1 AND
            authority_generation = 0 AND
            writer_implementation_digest IS NULL AND
            reconciler_implementation_digest IS NULL
        ),
    CONSTRAINT ck_lifecycle_ownership_scopes_updated_at_finite
        CHECK (isfinite(updated_at))
);
```

There is no unique constraint on `store_uuid`: the singleton key check and
primary key already limit the table to one row, so a UUID uniqueness index
would add no revision-001 invariant. There is no foreign key between the two
tables and no non-constraint index. Semantic columns have no server defaults;
every seed value is explicit. Only the two database timestamps use
`clock_timestamp()` defaults.

The broad routing enum documents the eventual vocabulary, but the named
M3-S2 inert check dominates it and makes every non-`DARK` or otherwise changed
shape illegal in revision `001`. The unsealed check likewise makes even a
well-formed writer-authority digest illegal. M3-S3 must leave both inert checks
unchanged. A separately reviewed activation-schema revision must explicitly
drop them and replace them with the complete legal cross-column routing, seal,
epoch, generation, and implementation-digest matrix before Release N can move
the pilot from `DARK`. That revision widens the domain, operation-subset, or
store-mode checks only if it also adds a reviewed scope. Its downgrade first
proves the exact revision-001 seed shape, restores these inert checks, and
fails otherwise.

Revision `001` rejects every non-PostgreSQL dialect before issuing DDL. In one
ordinary Alembic transaction it creates identity, creates scopes, generates
one UUID with `uuid.uuid4()`, and performs two plain inserts without
`ON CONFLICT`, adoption, replacement, or repair:

- identity is exactly `('global', <uuid4>, 1, NULL, <database time>)`;
- scope is exactly
  `('VOLUME', 'KUBERNETES_PVC_OWNED_LIFECYCLE_V1',
  'CENTRAL_POSTGRESQL', 'DARK', 0, 1, 0, NULL, NULL, <database time>)`.

PostgreSQL transactional DDL makes a failed create or seed atomic. Concurrent
first initialization is owned by `safe_alembic_upgrade()` and its
section-specific PostgreSQL advisory lock plus post-lock revision recheck. The
migration itself performs no conflict-ignore insert. Independent contenders
therefore converge on the UUID generated by the one winning migration rather
than replacing or adopting it.

The central initialization order is global state, config, Serve, jobs,
optional PostgreSQL requests, lifecycle actions, then physical capacity.
Global state remains first so bootstrap can prove the shared effective schema
is empty, and capacity remains last. The lifecycle branch is entered only when
the global engine dialect is PostgreSQL. The Helm migration job runs the
lineage in `upgrade` or explicit fresh-install `bootstrap` mode. API,
controller, executor, and any enabled image-worker roles use `verify` mode.
SQLite never imports or initializes this lineage through the central entry
point, and a direct private initializer rejects SQLite before Alembic can
create a lifecycle table or version stamp.

The public runtime surface is limited to `initialize_and_verify()`,
`read_foundation()`, and frozen store, scope, and foundation snapshot types.
It exposes no raw engine, connection, session, transaction, insert, update,
delete, transition, seal, grant, producer, reconciler, or worker API. Its
private `DatabaseManager` has no engine namespace. Every public read executes
explicit known-column `SELECT` statements in a PostgreSQL read-only
transaction, so a later additive revision can add columns without breaking an
M3-S2 reader and an accidental DML statement fails at the database boundary.

After `safe_alembic_upgrade()` succeeds in the configured mode, startup reads
all identity rows and requires exactly one frozen snapshot with:

- key `global`;
- a native RFC 4122 variant UUID at version 4;
- schema version 1;
- null writer-authority digest; and
- a finite, timezone-aware database creation time.

It then reads the exact pilot primary key and requires one frozen snapshot with
`DARK`, minimum version 0, ownership epoch 1, authority generation 0, both
implementation digests null, and a finite, timezone-aware database update
time. A missing, extra, malformed, incompatible, or changed required row fails
startup. Runtime verification never calls `uuid4()`, inserts, repairs, or
replaces a seed. It deliberately ignores additional scope rows introduced by
a later reviewed migration while still requiring the exact pilot row. A later
pilot activation consequently makes an old M3-S2 process fail startup, which
is part of the rollback gate.

Downgrade is one PostgreSQL transaction. Before any read it executes
`LOCK TABLE lifecycle_ownership_scopes, lifecycle_store_identity IN ACCESS
EXCLUSIVE MODE`. While both locks are held, it reads every row and proves there
is exactly one valid UUIDv4 `global` identity with schema version 1, null
authority digest, and finite creation time, plus exactly one scope with the
exact inert pilot key and values above. Missing, extra, activated, sealed,
malformed, or otherwise changed data raises before deletion or table drop. On
the exact inert dataset it deletes the scope seed, deletes the identity seed,
drops scopes, and drops identity, without `CASCADE`. Alembic owns removal of
the revision row; its empty version table may remain. Any rejected downgrade
leaves both tables, both rows, and revision `001` unchanged.

The exact M3-S2 test contract includes:

1. Literal migration versus runtime metadata parity for column order,
   PostgreSQL types, defaults, nullability, named constraints, primary keys,
   and indexes, plus an exact two-data-table assertion.
2. Exact seed, UUIDv4 and RFC variant, finite database timestamps, null
   digests, epoch, generation, and version checks.
3. Constraint tests that reject wrong keys, non-v4 UUIDs, schema versions,
   malformed digests and ranges, plus well-formed but forbidden seals,
   implementation digests, non-`DARK` modes, and positive changed epochs or
   generations.
4. Upgrade, bootstrap, verify, uninitialized-lineage rejection, and acceptance
   of a simulated later numeric revision only while the pilot remains
   compatible.
5. Two independent migration processes contending on one isolated PostgreSQL
   schema, proving one revision, one UUID, one pilot, and no replacement.
6. Frozen public readers executed with a connection that rejects DML, no raw
   engine or mutation surface, configured-mode forwarding, and SQLite
   rejection with no lifecycle table or version stamp.
7. Runtime rejection of missing, extra, malformed, sealed, or changed required
   seeds, no UUID generation or repair, and compatibility with a separately
   widened future additional scope.
8. Post-revision backup and clone preservation of the same UUID and successful
   verification, plus failure of a stamped clone missing either seed. A fork
   created before revision `001` may receive a different UUID; M3-S2 claims no
   historical-clone exclusivity because `DARK` owns no external effect.
9. Guarded downgrade success and independent failures for every missing,
   extra, sealed, activated, or changed row. Each failure proves both tables,
   both seeds, and revision `001` remain atomically unchanged.
10. Conflicting-lock tests for both tables, central initialization ordering,
    fresh-process revision maps and seeds, and an import/diff boundary proving
    no lifecycle consumer, volume handler, provider callback, router, worker,
    or SQLite path entered the slice.

M3-S2 deployment changes the candidate image and runs the existing blocking
Helm migration hook with `--reuse-values`; it changes no chart resource or
feature value. Before upgrade, the rendered diff must prove the migration job
uses the exact candidate digest in upgrade mode, every central runtime role
uses verify mode, no role remains pinned to an incompatible older image, and
no enabled optional worker is unintentionally rolled through an inherited API
image. Qualification proves hook completion precedes candidate pod creation,
reads the exact revision, constraints, tables, and seeds directly, runs the
public verifier, checks health and readiness without volume mutation traffic,
and monitors Kubernetes and Datadog for migration, PostgreSQL, readiness,
restart, and 5xx regressions. Normal rollback is image-only and leaves the
inert additive tables in place. Database downgrade is a separate maintenance
operation subject to the exact lock-and-proof contract above.

M3-S3 cannot add the remaining tables until a dedicated exact-DDL review fixes
their table and column types, closed values and legal row shapes, generic
versus volume-specific identity model, immutable binding persistence,
process-capability ownership, request-correlation cardinality, action-level
versus effect-level evidence ownership, canonical payload byte storage,
cross-live-and-tombstone business-key exclusion, tombstone-before-compaction
enforcement, ordinal effect tombstones, retention indexes, and restricted
borrowed-transaction facade. No raw provider callback may receive a connection
on which it can commit, close, or begin a nested transaction.

#### M3-S3 architecture review decision

The review selects permanent narrow action and effect rows. It rejects separate
identity-anchor, live-row, and tombstone tables. `lifecycle_actions` permanently
owns one complete canonical business key. `lifecycle_action_effects`
permanently owns every `(action_id, effect_ordinal)`. Neither row is ever
deleted. A later compactor may clear bulky payload and verbose evidence only
after terminal and cleanup gates, but identity, binding digest, locator,
provider operation identity, observed UID, final certainty, cleanup proof, and
completion time remain.

This gives one ordinary unique domain for a business key and one permanent
primary-key domain for an effect ordinal. Separate live and tombstone tables
would require deferred cross-table triggers to prevent a gap, duplicate, or
partial ordinal summary while retaining the same number of permanent identity
records. The roadmap terms `identity anchor` and `identity tombstone` therefore
refer to the permanent row before and after compaction, not extra tables.

Revision `002` remains an empty, expand-only schema and does not authorize
compaction. Its action and effect storage shape is closed to `FULL`. A later
reviewed revision may add `COMPACTED`, its irreversible row-local transition,
retention indexes, and the compactor only after terminal actions and exact
cleanup proof exist. Revision `002` adds no retention worker, tombstone table,
compaction trigger, producer, claim API, heartbeat writer, provider call,
router, reconciler, or public mutation facade.

The selected revision-`002` graph contains these twelve empty tables:

1. `lifecycle_actuation_bindings`;
2. `lifecycle_binding_compatibility_adapters`;
3. `lifecycle_process_capabilities`;
4. `lifecycle_process_binding_capabilities`;
5. `volume_lifecycle_resources`;
6. `legacy_volume_intents`;
7. `lifecycle_actions`;
8. `lifecycle_action_request_correlations`;
9. `lifecycle_action_attempts`;
10. `lifecycle_action_effects`;
11. `lifecycle_action_evidence`; and
12. `lifecycle_effect_evidence`.

Action evidence and effect evidence are separate, never polymorphic. Action
evidence is closed to `ADMISSION_PREFLIGHT_V1`, `ADMISSION_FENCE_V1`,
`QUARANTINE_REACTIVATION_V1`, and `TERMINAL_REDUCTION_V1`.
`ADMISSION_PREFLIGHT_V1` is the one provider-produced observation retained at
action scope because admission persists it before an effect row exists. Every
post-admission provider observation belongs only to an exact effect ordinal
and is closed to `PRE_CALL_FENCE_V1`, `SUBMISSION_V1`, `READBACK_V1`,
`ABSENCE_HORIZON_V1`, `CLEANUP_DECISION_V1`, and
`COMPATIBILITY_RESOLUTION_V1`. Both evidence tables are append-only while the
action is nonterminal. Attempts own claim interval and outcome, not provider
evidence.

Canonical bytes use one extracted leaf implementation of the existing strict
canonical JSON V1 rules: UTF-8, sorted object keys, compact separators, signed
64-bit integers, no floats, and no non-NFC string, surrogate, control
character, duplicate decoded key, unknown schema field, cycle, or unbounded
container. Action and effect payloads and adapter proof manifests are limited
to 65,536 bytes. Binding, locator, preflight, capability, and individual
evidence envelopes are limited to 16,384 bytes. Each stored envelope has an
explicit encoding token and schema version plus a lowercase SHA-256 digest.
PostgreSQL checks format and byte bounds without adding `pgcrypto`; every
trusted writer and reader recomputes the digest. Activation scans and rejects
any row whose bytes and digest disagree.

The process-role vocabulary reuses the deployed central-server contract
exactly: `all`, `api`, `executor`, and `controller`. The capability manifest
uses the closed tokens `LEGACY_ADMISSION_V1`, `ACTION_ADMISSION_V1`,
`ACTION_RECONCILE_V1`, `ACTION_EXECUTE_V1`, and `PROVIDER_CALL_V1`. A separate
many-to-many process-binding row is `NATIVE` with no adapter digest or
`COMPATIBILITY_ADAPTER` with an exact adapter implementation digest. Child
execution identity belongs to an attempt; revision `002` does not invent a
second generic worker role. For the volume pilot, an executor supervisor
advertises the capability and exact image for its child handlers. Each attempt
stores both that supervisor process identity and its child execution identity.
Activation fences new executor delivery, drains every earlier child attempt,
and requires no live attempt owned by an incompatible or missing supervisor
before changing the scope. Future independently deployed workers require a
separate schema review and an explicit new role rather than masquerading as an
executor.

The volume pilot is closed to action kinds `VOLUME_CREATE_V1` and
`VOLUME_DELETE_V1` and effect kinds `KUBERNETES_PVC_CREATE_V1` and
`KUBERNETES_PVC_DELETE_V1`. A sidecar exists only for the action writer, so its
writer mode is `ACTION_V1`; absence of a sidecar remains the legacy marker.
Visibility is `VISIBLE` or `DETACHED`. Domain state is closed to `RESERVED`,
`CREATE_PENDING`, `LIVE`, `DELETE_PENDING`,
`DETACHED_CLEANUP_PENDING`, `TOMBSTONED`, `QUARANTINED`, or `FAILED_SAFE`.
`attempt_count` counts allocated claims and `retry_count` counts committed
transitions from a finished attempt back to `WAITING`; both are monotonic and
`retry_count <= attempt_count`.

One compatibility-adapter approval is identified by source binding digest,
adapter implementation digest, proof-manifest digest, and monotonically
increasing approval revision. Its proof payload is immutable. It stores the
exact ownership scope, source contract version, canonical proof manifest and
digest, approving-subject digest, approval epoch, and database approval time.
A one-way, epoch-fenced revocation records revocation time, subject digest, and
reason digest on that approval; a stronger proof creates a new approval
revision rather than changing the old payload. Process capability and every
use record pin the exact approval identity, and a revoked approval cannot
authorize a new use. Use of an active row appends
`COMPATIBILITY_RESOLUTION_V1` to the original effect with the adapter identity
and original-versus-mapped payload and locator digests. It never rewrites the
effect binding.

The credential-free base locator, provider operation identity, exact target or
owned UID, terminal-observed replacement or foreign UID, and permanent typed
cleanup-proof digest remain on the permanent effect. Target and
terminal-observed identity are distinct nullable columns with an exact
certainty-dependent shape; `REPLACED` and `FOREIGN_CONFLICT` cannot overwrite
the old owned identity. The volume sidecar also retains the authoritative
current-incarnation copy while live or cleanup-relevant. That duplication is
deliberate: same-name recreation may advance the sidecar, while the old effect
remains the historical identity and cleanup proof.

This architecture decision is not the literal DDL approval. Before revision
`002` is implemented, one follow-up subsection must enumerate exact column
order, PostgreSQL types, nullability, named checks, foreign keys, partial
indexes, immutable-row guards, lock order, empty-only downgrade, runtime
metadata parity, and every negative row-shape test for all twelve tables. The
implementation may begin only after that exact subsection receives a
`PURSUE` verdict.

## Cleanup Contract

- Intent and cleanup obligation are durable before provider mutation.
- Provider-native idempotency is used when available.
- Otherwise SkyPilot uses deterministic names or tags that permit readback.
- A lost response moves to `READBACK`, not a blind replay.
- Parent deletion waits for every child of the exact incarnation to be absent.
- Provider `not_found` is success only when the query was complete and scoped
  to the intended identity.
- Deadline expiry may escalate or quarantine. It may not fabricate deletion.
- Force purge may detach user-visible ownership only after recording a durable
  cleanup incident that retains provider identity and retry responsibility.

## Transactional Lifecycle Events

The existing operational-event transaction is extended rather than replaced.
Datadog remains the telemetry plane.

For domains whose state and action share a PostgreSQL transaction, one helper
validates expected state, generation, domain fence, and action lease token,
then writes the new state and deterministic lifecycle event together.

The expand migration generalizes `resource_events` without overloading request
identity:

- `source_type` is closed to `api_request` or `lifecycle_action`;
- request events require `source_request_id`,
  `source_execution_generation`, and phase while action-source columns are
  null;
- action events require `source_action_id`, desired generation,
  transition ordinal, and `source_transition_id` while request-source columns
  are null;
- `correlation_root_request_id` optionally links an action and its nested child
  requests to the initiating API request;
- database check constraints enforce those mutually exclusive source shapes.

Storage expansion alone does not change the current event wire. Release N
backfills `source_type=api_request`, adds nullable action-source columns and the
new checks and partial indexes, and updates every event reader before any
action event can be inserted. Only after all event readers are compatible may
N+1 relax the current request-column `NOT NULL` constraints and enable the
scoped action writer.

The existing events endpoint and `OperationalEvent` model remain V1 and return
only `api_request` rows with the exact required `request_id`,
`execution_generation`, cluster-only kinds, and cluster target type. M3 adds a
separate V2 endpoint and bumps the server API version. `OperationalEventV2`
uses a discriminated `source` union:

```text
ApiRequestEventSourceV2(request_id, execution_generation)
LifecycleActionEventSourceV2(action_id, desired_generation,
                             transition_id, correlation_root_request_id)
```

V2 has a closed M3 vocabulary containing the existing cluster kinds plus the
volume create, register, refresh, and delete kinds, and cluster and volume
target types. Its cursor query fingerprint includes `event_schema_version=2`.
Future domain kind or target additions require a new negotiated event schema
version rather than sending an unknown closed enum to an older V2 client.

Old clients against a new server remain on V1 and never see action rows or new
enum values. A new client uses V2 only when the remote API version advertises
it and otherwise falls back to V1 with action events explicitly unavailable.
Mixed-reader, pagination, filtered-cursor, and old-client tests are required
before action event insertion is enabled.

The action transaction locks the action row, allocates its next transition
ordinal, and derives `source_transition_id` as UUIDv5 over canonical action ID,
resource incarnation, desired generation, ordinal, phase, and outcome. A
unique action-source index covers action ID, desired generation, transition
ordinal, and phase. The existing request-source uniqueness remains a separate
partial index. Retrying the same transition is therefore a no-op; a different
legal transition cannot collide.

Request and action events share the existing globally serialized
`event_sequence` allocator so current cursor ordering remains valid. Action
emission is mandatory: invalid context or an event insert failure raises and
rolls back the action state, domain state, reservation release, and event
together through the same borrowed physical connection. Request emission may
retain its existing optional-context behavior. High-frequency heartbeats and
provider observations never allocate operational-event sequence numbers.

The durable event answers:

- which owner and action changed the resource;
- which incarnation and desired generation were affected;
- which provider request or operation was involved;
- whether cleanup was proved or remains ambiguous.

High-volume observations are not operational events. They remain metrics,
logs, traces, or domain history.

## Milestones and Stacked Commits

### M0: Canonical design and baseline

- accept this design through adversarial review;
- pin exact source and deployed image;
- capture provider registry, placement, volume, cluster, Serve, jobs, and image
  characterization tests;
- capture the clean `skypilot-ha` rollback revision.

### M1: Typed provider bundles

- add `InstanceLifecycleV1`, immutable `ProvisionerBundleV1`, strict
  registration, and registration-time validation;
- adapt the explicit 24-provider built-in map without changing routed
  behavior;
- make lifecycle and template resolution share one resolver;
- retain the legacy plugin path, arbitrary module-shaped objects, direct
  imports, monkeypatch seams, last-registration-wins, and meaningful facade
  defaults;
- emit the mixed-owner diagnostic once per provider, facet, and process;
- land `lambda` and `lambda_cloud` normalization as an explicitly tested
  behavior fix;
- add all-built-in and plugin compatibility characterization tests.

Static qualification covers every built-in bundle even though the live canary
uses Kubernetes. Deployment proves API, executor, controller, request
execution, cluster status, and one Kubernetes test launch still behave
identically.

### M2: Placement offer

- lock the exact `OfferSourceV1`, result, identity, envelope, redaction, and
  revalidation contracts in this file and pass a new adversarial review;
- add the optional Cloud offer-source capability without adding it to
  `ProvisionerBundleV1` or creating a second universal provider registry;
- implement recursively immutable offers and built-in-only handle envelopes;
- adapt the initial single-node CPU Kubernetes subset;
- independently shadow-project the old and new placement sets from one raw
  observation snapshot while legacy `Resources` remains mutation owner;
- carry an exactly matched selected offer through the first provider mutation
  attempt in shadow only, while the legacy path remains sole mutation owner;
- exercise pre-mutation revalidation and compare the selected offer with the
  actual provider result without enabling offer-owned mutation;
- define the optional successful-READY envelope and authoritative retry fence,
  but gate their activation on M4 descriptor-owned actuation;
- pass the frozen corpus, bounded Datadog observation, stale-offer,
  minimum-compatible-client, and rollback-image gates;
- leave every Kubernetes request on legacy mutation in M2; M4 promotes the
  eligible subset only after descriptor and deploy-variable freshness gates
  pass.

M2 implementation is split into reviewed slices:

- S1, named `generic offer contract foundation`, owns
  `sky/utils/json_types.py`, `sky/placement/offer.py`, the docstring-only
  `sky/placement/__init__.py`, the side-effect-free default
  `Cloud.get_offer_source()`, `PlacementOfferHandoffV1`,
  `validate_authoritative_capture_v1()`, their focused leaf tests, and the
  narrow `.github/workflows/static-analysis.yml` edit that imports both leaves
  and executes the fixed-Unicode goldens in the existing Python 3.10
  `worker-floor-import` job. It does not add Kubernetes policy, orchestration
  wiring, `ActualPlacementEvidenceV1`, or a `ProvisionRecord` field.
- S2a.1 owns shared context/config policy, bounded one-load observation
  primitives, and the production-shared final pre-admission Pod-spec owner.
  It removes the corresponding policy and mutation bodies from legacy callers
  before adding an offer source.
- S2a.2 owns the physically extracted built-in Kubernetes `node_config`
  fragment, exact source composer and one initial production render and
  pre-combination parse, frozen
  post-parse base-Pod merge owner, one identity-admitted private pre-render
  snapshot-fed Pod projection reused by host-network deploy variables and
  merge, deferred metadata projection, full-object exact-order metadata
  applicator, exact render/config owner gates,
  deterministic host-network probe encoding, and a characterization corpus
  that locks stable-input parity and all declared compatibility deltas. The
  rejected offer-time full renderer, independent fragment renderer, and
  arbitrary full-template purity boundary are not carried forward. Custom
  full-template plugins remain on public mutable compatibility; explicitly
  unfreezable inputs reached after private admission use the typed-mutable
  adapter. Both are offer-ineligible. Implementation
  begins only after the exact contract above passes its dedicated design review
  and the live physical-capacity branch is reconciled with the deployment base.
- S2a.2 prerequisite A removes `gzip.compress()` from
  `host_network_probe_b64()` before any render golden is accepted. The local
  2026-07-31 qualification observed baseline-red timestamp and separate-process
  drift, then proved the portable header `1f8b08000000000002ff`, compressed
  SHA-256 `75aa9fefe49fe72bd89bcf70158467754871fa19c9541fd5fcbe5181ae285895`,
  3,373-byte identity, round-trip, and compilation under Python 3.10.18,
  3.11.15, 3.12.11, 3.13.14, and 3.14.6. A dedicated Python 3.10 through
  3.14 CI matrix executes the production packager in separate interpreters
  and enforces the same identity, header, digest, round-trip, and compilation
  contract. The Python 3.10 worker-floor job and Python 3.14 unit lane retain
  endpoint-specific coverage. Full writer-path render goldens cover built-in
  Kubernetes and SSH with effective `hostNetwork` both false and true plus
  Kubernetes OCI RoCE. Each golden locks the fixed legacy and replacement
  hashes and proves that replacing exactly the two embedded Base64 gzip
  members accounts for the entire full-render delta. PR #1099 passed all 29
  visible checks on exact head
  `fbc616a7d7128af4456d3eb58e265f3406250da9`, then merged normally as
  `1bf168a800ebbee77d76172f5c2d4d6ea46e4eee`, with first parent
  `24310104235e54de446d7fb4a2a9de9bdcfd2510` and that tested head as second
  parent. Image digest
  `sha256:36fe70700a797101dbec0fd31c5b324e41e5ab72d1848d4f72d4d2f19c4a6324`
  reports the exact merge commit and build 8057. Helm revisions 43 through 45
  deployed API, executor, and controller separately with reused values and the
  merged PR #1100 RBAC chart retained. Six consecutive 30-second revision 45
  snapshots kept every role at two ready, updated, and available replicas,
  all six pods at zero restarts on the exact digest, all PDBs healthy, and
  every direct health, ready, and live probe at HTTP 200. Capacity remained
  disabled, all five capacity tables remained empty, and PostgreSQL reported
  zero `skypilot-physical-capacity-evidence` connections. The retained PR
  #1100 Helm bindings still target the external `skypilot-ha-api` service
  account.
- S2a.2 source-composer staging extracts the exact 67,812-byte `node_config`
  fragment and 16,028-byte outer while leaving the 83,790-byte compatibility
  monolith unchanged. The standard-library-only source owner validates UTF-8,
  marker count, size, and SHA-256 identities before one Jinja render. The
  established three-argument `fill_template()` facade dispatches only the
  canonically resolved built-in path; wrappers still delegate, replacement
  renderers still own their output, same-basename plugin paths and other bare
  templates remain unchanged, and exact built-in runtime reads the outer and
  fragment once each without reading the mirror. Local qualification passes
  all 72 cases in the complete source and backend utility test files, including
  the 26 focused source, facade, and full-writer render cases, plus an installed
  wheel smoke test that proves all three physical sources and the composer are
  packaged and operational. Repository formatting, mypy, pylint, dashboard
  lint, and dashboard formatting pass. Exact-head CI, merge, staged deployment,
  and monitoring remain gates for this slice.
- S2b owns the Kubernetes observation source, payload schema, closed
  resource/config classifiers, aggregate deadline, and a separately reviewed
  bounded source-safe scheduling projection. S2b may not invoke S2a.2 full
  materialization or become authoritative before that projection proves parity
  without credential, private-key, logging-agent, plugin, or unrelated outer
  reads. It then owns
  `validate_kubernetes_offer_v1()`, with no mutation ownership change. It must
  call the applicable S2a.1 policy owners and the reviewed source-safe
  projector, and contains no candidate, precedence, or full Pod-rendering policy
  of its own.
- S3 owns orchestration propagation and shadow use of the already-defined
  placement-offer handoff, shadow comparison, actual-placement evidence, and
  dormant persistence/fence qualification. Authoritative promotion is gated on
  the M4 descriptor and actuation contract.
- A later independent raw-offer slice starts with a DigitalOcean shadow over
  one frozen catalog snapshot. It does not change M2's selected-offer handoff
  and cannot remove provider feasibility code until its per-provider gate
  closes.

Deployment proves candidate safety, optimizer winner, selected placement,
pre-mutation revalidation, actual provisioning result, handle compatibility,
rollback, and cleanup on the exact image digest.

### M3: Durable action runtime and volume pilot

- land M3-S0 first: the domain-local pure volume refresh projection and
  shadow-only wiring, with the current reducer and writer still authoritative;
- deploy M3-S0 without a schema migration, prove mismatch, projector,
  comparison, and diagnostic-reporting errors are contained, and keep the
  shadow result diagnostic-only until volume incarnation and
  desired-generation fencing exists;
- land M3-S1 as the exact legacy mutation transcript and four-file
  characterization corpus, with no production behavior change;
- in M3-S2, add only the reviewed dark PostgreSQL-only store-identity and
  scoped-ownership lineage, with its two inert seed rows and no producer,
  worker, process-capability row, domain table, or routing change;
- in M3-S3, after its own exact-DDL adversarial review, add the sidecar,
  process-capability, binding, legacy-intent, action, request-correlation,
  attempt, permanent effect, separate action and effect evidence, and
  compatibility-adapter graph with no domain rows and every producer and
  worker disabled; permanent action and effect rows are the logical identity
  anchors and later compacted tombstones rather than separate tables;
- in separately reviewed later slices, add the disabled action kernel and
  volume reducer, then the Release N compatibility reader, reconciler, and
  legacy-intent producer, then shadow admission, then implement and qualify
  create plus UID-scoped delete, readback, replacement, and cleanup while both
  remain disabled, then enable that dependency-closed owned-PVC lifecycle as
  one scope, and only then replace force purge with the durable detach and
  cleanup acknowledgement;
- add volume desired generation, incarnation, tombstone, observation, and
  deletion proof through the sidecar before either mutation is promoted;
- add the negotiated V2 action-event wire and emit the volume lifecycle
  transition atomically while preserving the request-only V1 endpoint.

The new writer is disabled by default. The compatibility reconciler must be
deployed and exercised before the writer can be enabled. The pilot covers only
central PostgreSQL; the supported local or controller SQLite path remains
legacy until its separate deprecation gate.

Deployment exercises create, refresh, delete, lost worker, stale lease,
ambiguous provider response, readback, and cleanup.

### M4: Cluster provisioning and teardown

- introduce `SkyletCapabilitiesV1` and centralize read-only `GetJobStatus`
  transport selection before migrating any mutating Skylet method;
- update this file with the exact node-actuation facet, operation evidence,
  provider descriptor, shared cluster planner, reconciler transaction,
  activation, and rollback contracts and pass a dedicated adversarial review
  before implementation;
- add the read-only `ProviderRegistryAuditSnapshotV1`, classify expected
  partial registrations, and prove one-to-one identity where legacy registries
  overlap without adding a second registration owner;
- add `ProviderRegistrationV1` and an immutable `ProviderDescriptorV1` snapshot
  only after the audit is characterized, then migrate registrations through
  the coordinator before making descriptor dispatch authoritative;
- add the opt-in provider node-actuation facet and a shared cluster planner and
  reconciler;
- inventory each provider's deploy-variable inputs and freshness semantics,
  introduce a descriptor-owned typed snapshot, and remove the post-bulk
  `make_deploy_resources_variables()` callback one promoted provider at a time;
- activate the qualified placement-offer handoff only after descriptor dispatch
  pins the selected provider facet and all post-provision reads to that same
  actuation context;
- compare pre-mutation launch, start, stop, down, port, head-selection,
  target-count, and cleanup plans from one frozen raw inventory only where a
  side-effect-free legacy projection exists; otherwise use the offline frozen
  corpus and do not call it live parity;
- after only the authoritative legacy mutation runs, capture its actual
  caller-visible return and compare the new status, identities,
  `ProvisionRecord`, and cleanup projection from one new frozen post-mutation
  inventory against that return and the observed provider state;
- promote one characterized provider and dependency-closed operation subset at
  a time;
- preserve cluster hash or successor incarnation fencing;
- shadow-compare status projection;
- route failover through typed provider outcomes;
- generate conformance from descriptor capabilities and keep providers without
  live promotion evidence on the legacy bulk facet.

#### M4 deploy-variable snapshot foundation and DigitalOcean pilot

This callback audit is pinned to SkyPilot
`289482c9327a8011f6ba0f503bcf978dcc24fb57` and dstack
`c9ebdaad6bbaa3105061d79f6ab52af9d609e99d`.

The two deploy-variable callbacks currently have different lifecycle positions.
`write_cluster_config()` calls `Resources.make_deploy_variables()` and the
provider callback before rendering. A successful new-provisioner mutation then
calls `Cloud.make_deploy_resources_variables()` again. Only
`custom_resources` from that second result reaches post-provision runtime setup;
all other fields are discarded, and the whole result is unused when the
provisioner reports that runtime setup is already complete. This callback is
therefore not a general runtime-bootstrap plan. M4 replaces it with a typed
provider deploy snapshot and one explicitly declared runtime projection.

This subsection refines M4 ordering. Read-only registry audit, the dormant
registration and descriptor foundation, and shadow-only deploy snapshot
comparison may land before `SkyletCapabilitiesV1` because they select no
transport and authorize no external effect. `SkyletCapabilitiesV1` remains a
prerequisite for the first node-actuation promotion. A provider's single
deploy-variable callback may become authoritative independently only after the
descriptor and snapshot gates below close and a credentialed provider canary
passes. The existing post-bulk callback remains authoritative before that
point.

The implementation stack is fixed:

1. Commit this exact design, the complete producer coverage gap, and
   characterization tests.
2. Add `ProviderRegistryAuditSnapshotV1` as read-only evidence with no dispatch
   authority.
3. Add dormant `ProviderRegistrationV1` and `ProviderDescriptorV1`; legacy
   registries remain the dispatch owners.
4. Add `ProviderDeploySnapshotV1` and DigitalOcean shadow comparison while the
   second callback remains authoritative.
5. Make the exact built-in DigitalOcean route single-callback only after an
   exact-image create, runtime-setup, status, down, and provider-absence canary.

Each runtime commit that introduces a temporary selector, comparator,
diagnostic branch, or compatibility router must add its exact locator to the
executable removal manifest in a later commit whose `introduced_by` is the
actual runtime commit SHA. The global post-bulk callback remains owned by
`PLA-M2-009`; DigitalOcean promotion cannot complete that row.

##### Descriptor and route contract

`ProviderRegistrationV1` is the validated input to the existing coordinator.
It contains the canonical provider name, immutable aliases, the complete set
of migrated facets, expected legacy projections, and a stable verifiable
implementation digest. It is rejected if an alias is ambiguous, a declared
facet is incomplete, or its executable artifact fingerprint cannot be
verified. It does not itself mutate a compatibility registry.

`ProviderDescriptorV1` is the coordinator's recursively immutable output. It
contains schema version 1, canonical provider name, sorted aliases, stable
implementation digest, process-local descriptor generation, registration
source, expected-partial classification, and the exact optional facet objects.
The process-local generation detects replacement within one process. It is not
durable identity and never substitutes for the stable implementation digest.

Shadow capture may use the dormant descriptor only as read-only evidence; it
does not select lifecycle dispatch and the dynamically resolved second callback
remains authoritative. Authoritative snapshot routing is selected and pinned
before provider mutation. Eligibility for DigitalOcean promotion requires all
of the following:

- the authoritative exact built-in DigitalOcean descriptor and exact deploy
  snapshot facet;
- the exact built-in `DO` Cloud class, not a subclass;
- the checked and subsequently invoked built-in
  `DO.make_deploy_resources_variables` producer identity;
- the exact built-in `Resources.make_deploy_variables`, config writer,
  DigitalOcean physical template, and renderer owners;
- no registered plugin template, arbitrary template, nonempty or replaced
  failover override, instance method replacement, wrapper, or delegating
  override.

A strict plugin may opt in only through its own complete reviewed descriptor
and separate live qualification. A legacy registration always blocks built-in
snapshot eligibility, even when the built-in bundle is still discoverable for
compatibility fallback. Any failed promotion gate before provider mutation runs
the complete legacy two-callback lifecycle. In authoritative mode the selected
route and checked producer reference are retained for the attempt. Once
provider mutation begins, that attempt never changes route or falls back to a
newly resolved legacy callback. Shadow mode deliberately retains the current
dynamic second resolution and records replacement as comparison evidence.

##### `ProviderDeploySnapshotV1`

`ProviderDeploySnapshotV1` is a request-local, process-memory-only value with
these exact fields:

- `schema_version`, exactly `1`;
- `canonical_provider`;
- `descriptor_generation`;
- `descriptor_implementation_digest`;
- `producer_contract`, initially
  `make_deploy_resources_variables.digitalocean.v1`;
- `producer_identity_digest`, derived from the checked release artifact;
- `values`, a detached `FrozenJSONDict` containing the provider projection;
- `runtime_projection_contract`, initially
  `ray_custom_resources_from_deploy_variables.v1`;
- `process_comparison_token`, an HMAC-SHA-256 over canonical compact JSON of
  the preceding public metadata and `values`, keyed by one random process-local
  256-bit key.

Construction accepts only null, exact booleans, finite integers or floats,
bounded UTF-8 strings, string-keyed objects, and arrays. It rejects cycles,
duplicate logical keys, non-string keys, non-finite numbers, and values beyond
depth 8, 128 aggregate entries, 4 KiB per string, or 32 KiB canonical bytes.
The DigitalOcean pilot narrows this further to its scalar output grammar. The
snapshot cannot contain clients, credentials, credential paths, raw user
configuration, provider responses, telemetry payload values, or an arbitrary
plugin object. Each facet supplies a closed field allowlist and value grammar;
an undeclared field fails capture instead of being generically frozen. Snapshot
repr and errors expose only provider, contract, and schema. The comparison key
is generated after process start, never exposed, logged, serialized, or
persisted, and rotates on process restart. The token is used only to avoid
repeated canonicalization during same-process equality checks. It is not a
redaction boundary and never reaches telemetry. Telemetry emits only bounded
reason and count tags, never `values` or any value-derived digest.

`to_legacy_runtime_variables()` is the only public projection. It validates the
declared runtime projection contract and returns a new mutable mapping. The
DigitalOcean V1 projector reads only `custom_resources`; no duplicated mutable
or frozen field can disagree with `values`.

The process comparison token is separate from the cluster config hash.
DigitalOcean's `custom_resources` is not rendered by `do-ray.yml.j2`, and the
cluster hash
also has restoration and file-mount semantics unrelated to this provider
projection. The snapshot may freeze a rendered provider delta, but it never
owns raw catalog acquisition or competes with `RawOfferSnapshotV1`.

The snapshot provides within-attempt coherence only. It is not persisted before
I/O, cannot recover a process crash, and is not an idempotency or effect fence.
Any future durable form requires a separate schema, redaction, retention, and
mixed-version design.

##### Attempt ownership and consumption

The existing public `write_cluster_config()` signature and dictionary return
remain unchanged and snapshot-free. Direct calls, wrappers, replacements, and
plugins can reach only that contract. When the backend resolves the exact
built-in public writer and every promotion or shadow owner gate passes, it
instead invokes one shared private implementation through an exact checked
reference. That implementation returns `DeployConfigWriteResultV1`, a private
linear carrier containing the ordinary `config_dict` plus at most one
`ProviderDeploySnapshotV1`. The public writer delegates to the same
implementation with snapshot capture disabled and unwraps only `config_dict`,
so rendering and restoration have one owner.

`DeployConfigWriteResultV1` is an exact-type context manager and never appears
in a public annotation or return. Its `take_snapshot()` succeeds at most once,
replaces the held reference with null, and returns the snapshot. Its
`discard_snapshot()` is idempotent. `__exit__` always discards any untaken
snapshot, including on `BaseException`. The backend's context scope encloses
config-hash and dry-run decisions, provider mutation, the shadow callback or
authoritative runtime projection, and assignment of the existing public
`resources_vars` mapping. Only `config_dict` can escape that scope.

The private writer captures one fresh snapshot after the first checked provider
callback for each physical config, zone, and cloud-specific failover attempt.
An error from that first callback is authoritative for that attempt and returns
no carrier. A failed attempt's snapshot is discarded by the carrier and is
never reused by another retry, zone, provider, template, or outer optimizer
attempt. A replacement of either the private implementation or result type
fails the pre-I/O gate and uses the complete public legacy path.

In shadow mode the post-bulk callback remains authoritative. The attempt
compares a separately validated and frozen second result with the snapshot by
exact value equality, using the process token only as a fast inequality check.
It then takes and releases the private snapshot and returns the existing mutable
`resources_vars` projection unchanged. Stable equality, intentional drift,
callback error, and comparison failure are distinct events; comparison failure
never changes legacy control flow.

In authoritative mode the attempt takes the snapshot, thaws only the declared
runtime projection to a fresh legacy-shaped mapping, releases the snapshot, and
never calls the provider producer again. Dry run and config-hash skip leave the
snapshot untaken so context exit discards it before returning. Bulk failure,
cleanup, cancellation, `Exception`, and `BaseException` likewise exit through
the carrier cleanup. Both the normal runtime-setup path and
`runtime_setup_done=True` path consume it. No carrier or snapshot may remain in
a public return, config YAML, template variables, config hash, cluster handle,
global state, exception, or log record.
Existing-cluster YAML restoration remains authoritative and completes before
the config hash and cluster name are returned. It cannot be overwritten by the
snapshot.

##### DigitalOcean freshness classification

The exact built-in DigitalOcean producer is the first candidate because its
return contains only scalar values and it performs no provider mutation or
credentialed API call.

| Input or observation | Current behavior | Pilot contract |
| --- | --- | --- |
| `resources.assert_launchable()` | validates and returns the launchable resource | consumed once by the checked producer |
| instance type | returned as `instance_type` | frozen for the attempt |
| accelerator catalog projection | looked up from the instance type and compact-JSON encoded | frozen for the attempt; later catalog drift is an intentional delta |
| cloud image mapping | selects the global image or the current region entry | frozen for the attempt |
| region | returned and used for regional image selection | frozen for the attempt |
| cluster name, zones, node count, dry-run flag, volume mounts | ignored by the built-in producer | invariance is characterized, including a restored existing-cluster name |
| ambient config, credentials, provider API | not read | any future read changes the producer contract and implementation digest, invalidating promotion |
| return mutability | a fresh mutable dict containing immutable scalar leaves | detached into `FrozenJSONDict`; every legacy projection is a new dict |

The provider-wide audit uses four freshness classes. `F0` consumes explicit
attempt inputs plus the service catalog. `F1` also resolves local ambient
configuration, environment, or a credential profile. `F2` performs live
provider or cluster discovery and may mutate a cache. `F3` returns secret-bearing
or opaque execution context that must be split from the public deploy snapshot.
Every class is recaptured for each physical retry or failover attempt. The class
controls the producer boundary; it never licenses reuse across attempts.

| Provider producer | Class | Freshness, side effect, and mutability finding |
| --- | --- | --- |
| AWS | F2 | regional and workspace config plus conditional EC2, EFA, and AMI reads; LRU and persistent AMI cache writes; ingress and EFA option aliases require recursive detachment |
| Azure | F1 | resource-group config and Azure CLI subscription profile; subscription cache mutation; fresh cloud-init list |
| Cudo | F0 | instance type and region plus accelerator catalog; scalar result |
| DigitalOcean | F0 | instance type, image, and region plus accelerator catalog; no provider I/O; scalar result |
| Fluidstack | F0 | instance type and region plus accelerator catalog; scalar result |
| GCP | F2 | workspace, task, ADC, and project state plus conditional Compute zone and disk reads; auth caches and nested volume and option values |
| Hyperbolic | F0 | instance type plus accelerator catalog; fixed one-node projection |
| IBM | F3 plus F2 | credential YAML and conditional VPC image listing; nested result currently includes plaintext IAM API key and cannot enter a generic snapshot |
| Kubernetes | F2 | kubeconfig, layered config, image catalog, and extensive live node, label, accelerator, and network reads; deeply mutable result |
| Lambda | F0 | instance type and region plus accelerator catalog; fresh GPU option list |
| Mithril | F1 | environment and repeatedly read profile, project, and API-key YAML; public scalar result only after one coherent local-config resolution |
| Nebius | F2 | filesystem, static-IP, security-group, project, credential, and conditional IAM project-list reads; mutable filesystem and environment aliases |
| OCI | F2 | profile and credentials plus conditional availability-domain and OS-image reads; module-global tenancy-prefix mutation |
| Paperspace | F0 | instance type and region plus accelerator catalog; scalar result |
| Prime Intellect | F0 | instance type, region, and first zone plus accelerator catalog; scalar result |
| RunPod | F0 | instance, image, spot, Docker username, region, zone, volume validation, and pricing catalogs; public scalar result |
| SCP | F0 | instance, image, and region plus accelerator and image catalogs; scalar result with typed failover error |
| Seeweb | F0 after normalization | instance, accelerator, Docker image, region, and mutable `ClusterName`; nested GPU data and cluster-name reference require canonical detachment |
| Shadeform | F0 | resource and region plus conditional feasibility lookup; scalar result |
| Slurm | F2 | local Slurm and SkyPilot config plus live SSH partition, node, and GRES reads; TTL cache writes and nested `sbatch_options` aliases |
| Vast | F3 after F1 | config and opaque `create_instance_kwargs`; returned nested values can contain registry, command, environment, or other secrets |
| Verda | F1 | explicit inputs plus `SKYPILOT_VERDA_IMAGE_ID`; scalar result after validation |
| vSphere | F0 | instance, region, and zones plus accelerator catalog; scalar result |
| Yotta | F3 | explicit inputs and catalogs, but returns the mutable password-bearing `DockerLoginConfig` object by reference |

No concrete producer consumes a provisioning result or newly created node
identity. The eventual second-callback removal is therefore valid for every
built-in, but the migration boundary differs by class. F0 providers can use a
bounded attempt snapshot after characterization. F1 providers first need one
coherent ambient resolution. F2 providers need one explicitly placed live-read
stage and must name cache effects. F3 providers must split public render values
from secret or request-scoped execution context before any snapshot promotion.
IBM, Yotta, and Vast are prohibited from whole-result snapshotting.

The first callback result becomes authoritative only after promotion. A
stateful second result, later catalog mutation, callback replacement after the
route is pinned, or a legacy second-callback exception after successful bulk
mutation is then an intentional compatibility delta. Those deltas must be
named in tests and release evidence. They must not be hidden as parity.

DigitalOcean shadow and promotion qualification covers absent, global, and
regional image IDs; no accelerator and accelerator JSON; output bytes and the
real deterministic config hash; exact callback order and counts; restored
cluster names; dry-run, hash-skip, bulk-failure, retry, and runtime-ready paths;
stateful drift and second-callback failure after mutation; recursive detachment,
redaction, and complete snapshot consumption; every owner-gate failure; and
stable `ProvisionRecord`, runtime custom resources, cleanup, and absence.

Kubernetes remains entirely legacy in this pilot. At the pinned source it has
two effective-Pod reads while writing YAML and one post-bulk read. The corpus
must retain those three reads, including fresh ambient state and a third-read
failure after mutation, until its separate mutable-input and Pod-projection
migration closes. A Kubernetes deployment validates import, API-server,
executor, controller, and general regression safety, but does not qualify the
DigitalOcean route. Without credentialed DigitalOcean access, the rollout must
stop at shadow mode.

### M5: Serve and pools

- shadow `ChildWorkloadObservationV1` against current replica job-status
  polling before shared child launch or teardown is reachable;
- extract pure planners and reducers for the central PostgreSQL deployment;
- persist central replica launch and down attempts;
- keep lifecycle epoch, immutable versions, and incarnation inventory;
- make the jobs and Serve pool handoff an explicit fenced contract;
- retain the officially supported SQLite Serve path until a separate
  dialect-capable runtime or product deprecation closes its ledger row.

### M6: Managed jobs

- shadow the same `ChildWorkloadObservationV1` against managed-job child
  status polling while managed jobs retain recovery and terminal-state policy;
- migrate central PostgreSQL recovery and cleanup actions last;
- retain controller generation and admission fencing;
- remove central process-local retry ownership only after recovery equivalence
  tests;
- retain controller-local SQLite retry ownership until a separate
  dialect-capable runtime or product deprecation closes its ledger row;
- after the kernel is proven across the migrated domains, move only duplicate
  managed-image execution mechanics onto it while retaining image shard
  fairness, rotation, and reservation accounting in domain admission.

### M7: Compatibility removal

- verify every removal gate below;
- delete legacy dispatch, version switches, reconstruction, and duplicated
  lifecycle mechanics;
- run full provider conformance, API compatibility, rollback, and live
  cleanup qualification.

## Removal Ledger

Removal is part of completion, not optional follow-up.

The canonical executable ledger is
`docs/designs/provider-lifecycle-actuation-removals.yaml`. It is validated by
`tools/check_lifecycle_removals.py`, focused checker tests, and static-analysis
CI. The manifest is authoritative; the Markdown table below is its manually
verified human summary. Every manifest row has a stable
`PLA-(BASE|M[0-7])-NNN` ID and records:

- milestone, introducing commit, obligation, disposition, and exact domain,
  store, provider, and operation scope;
- one or more semantic locators consisting of a path plus a Python symbol,
  attribute, call-within-symbol, enum member, physical file, SQL object, or
  exact test node;
- the replacement owner and dependencies;
- exact source, test, telemetry, release-window, and schema gates;
- the only retained-reference allowlist, recorded evidence, any external
  blocker, and the final removal commit and deployment proof.

Known inventory that cannot yet be assigned safely to an executable removal
row is never omitted. It is recorded under the required top-level
`coverage_gaps` list with a stable `PLA-GAP-NNN` ID, exact candidate symbols,
an owning milestone and responsibility, a reason it cannot yet be split, and
an explicit closure gate. Current-phase validation resolves every candidate
symbol. Final-phase validation rejects every remaining coverage gap. Artifact
references to a manifest artifact or coverage gap must resolve to a declared
ID.

Line numbers are evidence, never identity. Broad scopes such as `migrated` or
`all promoted providers` are invalid until expanded into exact rows. Semantic
AST checks are authoritative for Python identity. Source-structural checks
identify SQL objects, while live PostgreSQL catalog proof remains an explicit
schema gate; `rg` is supplementary. Released migration files are
`retain_history` artifacts and remain byte-identical. Their live tables,
columns, checks, indexes, runtime metadata, and imports are separate
`must_contract` artifacts removed only by a new forward migration.
A planned `retain_history` row reserves the exact future migration path and
linked contraction IDs with a null checksum. The checksum and introducing SHA
become mandatory as soon as that migration exists.

Statuses are `planned`, `present`, `gating`, `ready_to_remove`,
`removal_in_progress`, `removed`, `blocked`, and `retained_verified`.
`removed` is the only completion status for `must_remove` and `must_contract`.
`retained_verified` is legal only for `retain_history` and
`retain_characterization`. A blocker can stop progress, but it never completes
an obligation and never permits this migration to be reported complete.

The manifest enums are closed. Obligations are `must_remove`,
`must_contract`, `retain_history`, or `retain_characterization`. Dispositions
are `delete_file`, `delete_symbol`, `delete_branch`, `delete_enum_member`,
`replace_content`, `contract_live_schema`, `retain_history`, or
`retain_characterization`. Locator kinds are `python_symbol`,
`python_attribute`, `python_call_within`, `python_enum_member`,
`python_ast_pattern`, `file_digest`, `path`, `packaged_path`, `sql_object`,
`runtime_metadata`, `runtime_import`, or `test_node`.

The checker rejects a line-only locator, a wildcard provider or store, an
unresolved `present` locator, an invalid status transition, and `blocked` as a
terminal state. The normal progression is `planned` to `present` to `gating`
to `ready_to_remove` to `removal_in_progress` to `removed`. An incomplete row
may enter `blocked` only with `blocked_from_status`, owner, issue, and evidence,
and may resume only to that recorded status. `removed` requires removal SHA,
exact-head CI, every source and schema gate, and deployment evidence for a
runtime-affecting artifact. `retain_history` requires a SHA-256 checksum and
linked live-schema contraction rows. Every `introduced_by` SHA must resolve to
a commit on the checked HEAD ancestry. CI therefore fetches full history for
this job. A dependency must already be `removed`, or `retained_verified` for a
retention obligation, before a dependent artifact can become
`ready_to_remove`, `removal_in_progress`, or `removed`.

The checker rejects YAML aliases that share mutable gate or evidence objects
between artifacts. It validates exact qualified calls without accepting a
same-suffix call on a different owner. A `file_digest` locator is available
for an exact byte-level compatibility body. Every live SQL contraction must
have one reverse-linked immutable `retain_history` owner for the migration
that introduced it, and a migration path cannot have multiple history owners.
A planned future migration may reserve its exact path without a checksum, but
must add its checksum and introduction provenance as soon as it exists.

Terminal evidence keeps four facts separate: the removal commit, full CI on
that exact head, the normal merge commit with both parents, and, for runtime
artifacts, deployment of the merge commit. Passing local tests, deploying an
unmerged head, or recording a merge without its exact tested second parent
cannot complete a row. In a Git checkout, every proof SHA must exist on the
checked HEAD ancestry and the recorded first and second parents must equal the
actual parents of a normal two-parent merge commit.

Every retained local or controller SQLite compatibility row is
`must_remove`. Product deprecation may satisfy its gate, but the artifact is
still incomplete until the code is deleted and the row reaches `removed`.

| Legacy code | Remove after | Objective gate |
| --- | --- | --- |
| `Provisioner.module: Any` and legacy module registration in `sky/provision/__init__.py` | all built-ins and inventoried plugins use strict bundles | repository and downstream inventory show zero callers for one compatibility release |
| `LegacyInstanceLifecycleAdapter` and its variadic forwarding boundary | all 24 built-in lifecycle entry points conform to the exact synchronous V1 signatures | signature-conformance corpus reports zero drift and facade behavior remains green |
| method-by-method plugin fallback in `_route_to_cloud_impl()` | strict facet ownership is deployed | mixed-owner diagnostic count is zero and plugin conformance passes |
| dynamic provider-module lookup through `globals()` in `sky/provision/__init__.py` | explicit late-bound built-in getter map owns all providers | every built-in provider registers and passes import-without-extra-dependencies tests |
| legacy `lambda_cloud` registry key | canonical `lambda` normalization is deployed | both spellings pass lifecycle and template-hook tests and the alias diagnostic is zero for one compatibility release |
| `ProvisionerVersion` and `StatusVersion` in `sky/clouds/cloud.py` plus backend branches | legacy Ray providers are adapted or frozen behind one explicit legacy facet | no call site reads either version and old/new client-server compatibility passes |
| provider-wide `OpenPortsVersion` branches | `PortLifecycle` expresses launch-only, updatable, or reconcilable behavior | all port tests derive behavior from the facet |
| provider-specific type checks and string parsing in `capacity_policy.py` and `failover_error_policy.py` | providers emit typed outcomes | characterization corpus maps to identical or safer retry decisions |
| Kubernetes use of `make_launchables_for_valid_region_zones()` and backend `_yield_zones()` for the declared eligible subset | the eligible Kubernetes subset is authoritative through `PlacementOfferV1` and its rollback window is closed | frozen corpus and bounded live window have zero unexplained safety, placement-set, optimizer-winner, or actual-result mismatches; minimum-client and rollback-image qualification pass |
| Kubernetes placement-offer shadow dual projection | Kubernetes authoritative mode has remained healthy for one full compatibility release | Datadog records no unexplained mismatch or rollback, and repository tests retain a frozen legacy-versus-offer characterization corpus |
| Kubernetes `NOT_REPRESENTABLE` legacy fallback | every officially supported Kubernetes placement-affecting input has a typed, characterized offer representation | one compatibility release records zero fallback for supported inputs, the full Kubernetes corpus passes, and repository search finds no eligible legacy call |
| candidate selection body in `Kubernetes.existing_allowed_contexts()` and provisional `resolve_kubernetes_candidate_contexts_v1()` | shared pure context policy is authoritative | legacy characterization is exact, offer tests call the same owner, and repository search finds no second selector |
| duplicated workspace, context, and global precedence in the provisional Kubernetes offer source | snapshot getter is authoritative | live and frozen-input corpora agree and no offer-owned effective-value helper remains |
| pre-admission Pod mutation body inside Kubernetes `_create_pods()` | shared final Pod-spec owner is called immediately before create | head, worker, CPU, GPU, TPU, allowed-node, Docker-cache, single-node, and multi-node corpus is exact and repository search finds no second mutation body |
| time-bearing `gzip.compress()` in `host_network_probe_b64()` | removed and qualified by merged S2a.2 prerequisite A at `1bf168a800ebbee77d76172f5c2d4d6ea46e4eee` | `GzipFile` output has the exact portable header, is byte-identical across Python 3.10 through 3.14 and separate processes, round-trips, and compiles the actual probe; exact-head CI passed 29 checks and revision 45 passed the staged six-snapshot deployment monitor |
| authoritative inline built-in Kubernetes `node_config` body and raw one-file built-in render | the physical fragment, single source composer behind the established facade, one initial render/pre-combination parse, and `build_kubernetes_base_pod_spec()` are authoritative | against the deterministic-gzip prerequisite head, outer, fragment, monolith, and name digests, exact facade arguments, wrapper behavior, anchor and error-coordinate corpus, initial parsed tree, exact later parse/serialization sequence, dumped YAML, validation, deterministic hash, SSH use, managed-image, reuse, and rollback behavior are exact; exact built-in and delegating-facade dispatch has no composer bypass or independent fragment render, and the old monolith is only the validated compatibility mirror tracked by its own row |
| digest-locked `kubernetes-ray.yml.j2` compatibility monolith and temporary-outer selection branch | direct physical-template readers have completed their inventory and deprecation window | delegating wrappers remain green; the identity-gated custom-`fill_template` compatibility diagnostic records zero dispatches for one compatibility release; repository and downstream-package inventory find no reader that opens the physical path outside the facade; all inventoried readers migrate to the facade or a structured extension; and a separate commit replaces the mirror path atomically with the validated outer, removes the temporary `-outer` path and selection branch, and leaves repository search with no inline monolith or second physical source copy |
| post-`bulk_provision()` `make_deploy_resources_variables()` callback for Kubernetes, SSH, and other providers | the M4 immutable provider descriptor supplies an inventoried typed deploy-variable snapshot with an explicit freshness contract to every promoted provider | per-provider config, credential, catalog, API-read, side-effect, mutable-value, plugin, failover, reuse-name, and post-mutation semantics are characterized; identity-admitted Kubernetes/SSH writer paths retain two cloud callbacks and two Pod reads, while public mutable paths retain their three Pod reads until their separate gate closes; each promoted provider passes stable-input and intentional-delta conformance; repository search finds no promoted-provider second callback, while unpromoted providers remain behind an explicit compatibility branch |
| module, facade, class, instance, and subclass render/config/deploy-variable compatibility dispatch | downstream replacements use reviewed structured template, config-projection, and deploy-variable extensions over the shared owners | exact writer, `fill_template`, effective-region getter, cloud-config projector, composite exact `Config` class and `get_nested` owner, Kubernetes merge primitive, historical Pod resolver, and combined Pod/metadata facade replacements plus `Resources`, Kubernetes, and SSH class, instance, inherited, and subclass override corpora pass; dynamically resolved generic `skypilot_config.get_nested` calls remain unchanged; replacement-after-resolution tests prove check-and-call identity for execution gates, admission-only facade isolation, closed strict recursion, and exact public late-bound nested merge behavior; downstream inventory and one compatibility release record zero per-gate or nonidentity public-compatibility event and zero unstructured use; a separate removal commit deletes the identity-gated public/mutable path without changing direct-call public behavior until its own deprecation closes |
| ambient Pod-config resolution inside `Kubernetes.make_deploy_resources_variables()` | shared generic-prefix/cloud-slot/generic-suffix orchestration and the snapshot-producing Kubernetes core are authoritative for the identity-admitted private writer path, and every public mutable caller has migrated | within the identity-admitted writer the Pod projector runs exactly once and the same detached projection controls configured host networking and post-parse merge; the intentionally retained post-bulk callback performs its separate current Pod read until the provider-freshness ledger row closes; generic-prefix, earlier-cloud-step, OCI RoCE, SSH, first-projection mutation-sentinel, and error-order corpora pass; public gate-failed, token-`None`, direct, delegating-writer, method-override, custom-template/failover, and full-template paths retain the characterized ambient host-network and later combined-facade Pod reads until every inventoried caller supplies explicit input and a separate removal commit deletes the ambient compatibility body |
| ambient `pod_config` and `custom_metadata` reads inside built-in Kubernetes combine helpers | one raw render-config snapshot feeds sequential Pod and metadata projectors, the base-Pod owner, and one full-object metadata applicator on the identity-admitted private path, and public mutable compatibility has closed | on the private path, Pod failure precedes metadata semantic selection, merge, classification, and application; whole-`Config` deep-copy traversal remains exact, and a second-full-copy failure precedes the metadata projector; Kubernetes recursive context merge, SSH whole-context replacement, distinct Pod-versus-metadata cloud selection, SSH prefix and mismatched-context behavior, per-`get_nested()` and per-projection deep-copy boundaries, final resource-override merge, and semantically ignored workspace behavior are exact; capture occurs at the first current Pod read, later reference replacement is ignored, and later in-place mutation remains visible to deferred metadata; the base owner and ownership-transfer applicator perform no ambient reads; the exact SA, role, role-binding, SA, Pod, Services order and shared alias graph are preserved; public compatibility retains historical ambient reads until its gate closes, after which repository search finds no second resolver or metadata merge owner |
| `UNFREEZABLE_KUBERNETES_RENDER_INPUT` private typed-mutable adapter | the frozen grammar covers every officially accepted and observed safe-YAML parsed object and render-config input | one compatibility release records zero typed-mutable dispatch after private admission; date, datetime, binary, intra-object and cross-object alias, cycle, set, non-string-key, and downstream config conformance passes; a separate removal commit deletes the private mutable combination owner without changing the single source composer or public compatibility gates |
| independent Kubernetes offer template renderer and hand-maintained Jinja variable set | a separately reviewed bounded source-safe projector is authoritative and parity-gated against the production base-Pod owner | the closed eligible corpus has identical scheduling projections, changed template input fails closed, no credential, private-key, logging-agent, plugin, or unrelated outer read is reachable, and no offer-only full-template or fragment renderer remains |
| arbitrary full-template Kubernetes plugin compatibility renderer | every inventoried plugin uses a reviewed structured extension over the shared base-Pod owner | downstream inventory and conformance pass, one compatibility release records zero legacy-only plugin render, and a separate removal commit deletes the adapter |
| V1 managed-image, reuse, and restart offer exclusions | a reviewed shared post-reuse finalizer owns managed-image and restoration placement semantics | fresh, reuse, restart, image, selector-conflict, old/new client, hash, and actual-placement corpora pass and the schema-versioned eligibility change has a closed rollback window |
| V1 `AUTH_RESERVED_SENTINEL` placement exclusion | authentication replaces placeholders only at reviewed template-owned paths | every scheduling-string sentinel corpus passes, post-authentication projection is exact, the schema-versioned eligibility change and old/new client qualification pass, and repository search finds no global replacement that can reach placement fields |
| first-provider-attempt-only placement-offer fence and `RETRY_AFTER_PROVIDER_ATTEMPT` fallback activated with M4 | M4 carries typed complete cleanup and provider-absence evidence across every failover provider and resets the cluster record atomically | cross-provider lost-response, partial-create, teardown, absence, and stale-record corpus passes with no blind replay |
| M4 handle-backed `placement_attempt_fence`, reconciler, and `QUARANTINE_FENCED` path | the durable action runtime stores every cluster attempt and UID inventory and the pre-authoritative rollback window is closed | crash and UID-replacement tests prove foreign objects survive, every owned child reaches proved absence, no generic label/name delete is reachable, and repository search finds no handle-backed fence writer |
| provider-agnostic region and zone reconstruction in `resources_utils.py` and backend launch loops | every supported provider is authoritative through a placement-offer source or is explicitly frozen behind a declared legacy adapter | provider-wide corpus and bounded observation gates pass, repository and plugin inventory find zero migrated callers, and old/new client-server compatibility passes |
| blocking provider wait ownership inside `run_instances()` implementations | `ProvisioningAttempt` effect observation owns progress | provider conformance proves pending, success, timeout, and partial-create behavior |
| provider-local target-count, head-selection, resume, readiness, and `ProvisionRecord` algorithms inside `run_instances()` | the shared cluster planner and reconciler are authoritative for that provider and dependency-closed operation subset | frozen-inventory plans and post-mutation projections match the captured caller-visible legacy return or are explicitly safer, live create/restart/scale/down qualification passes, and repository search finds no promoted-provider orchestration in its primitive facet |
| parallel canonical-name and alias inventories across Cloud and provisioner registration | all migrated registrations flow through `ProviderRegistrationV1`, descriptor dispatch is authoritative, and both legacy views are derived | one compatibility release records zero unexplained audit mismatch, expected partial providers remain explicit, plugin replacement and alias conformance pass, and no independent mutable inventory remains |
| import-time or hand-maintained provider-wide capability matrices for migrated facets | the immutable provider descriptor generates positive capability views and resource-dependent predicates | every declared capability has executable conformance and repository search finds no migrated parallel list |
| generic retry, cache update, and failure classification in `RetryingVmProvisioner` | typed attempts and domain retry policy are authoritative | old and new failover traces agree on the characterization corpus |
| generic feasibility and ordering bodies in `DigitalOcean._get_feasible_launchable_resources()`, `DigitalOcean.regions_with_offering()`, and `DigitalOcean.zones_provision_loop()` | the DigitalOcean `RawOfferSnapshotV1` source plus shared pure offer policy is authoritative | the frozen and bounded live DigitalOcean corpus preserves or explicitly improves candidates, price, region order, fuzzy matching, and the characterized absence of rejection hints; semantic search finds no second DigitalOcean generic filter or ordering owner |
| gRPC exception fallback and direct SSH status transport in `CloudVmRayBackend.get_job_status()` | `SkyletCapabilitiesV1` and the bounded, incarnation-and-channel-keyed `GetJobStatus` router are authoritative | mixed old and new Skylet tests pass, handshakes are single-flight and boundedly refreshed, qualified deployments record zero unexplained fallback for one compatibility release, and this method contains no fallback catch or direct SSH transport outside the router |
| central-PostgreSQL `FileLock` and synchronous provider-call ownership in `sky.volumes.server.core.volume_apply()` and `volume_delete()` | M3 action worker owns central volume create and delete | HA stale-worker, readback, UID replacement, and cleanup tests pass, the server gate is promoted, and both exact functions have no central-PostgreSQL provider-call branch |
| `sky.server.daemons.refresh_volume_status_event`, its daemon registration, and its direct call into `sky.volumes.server.core.volume_refresh()` | the action reconciler and domain reducer own central-PostgreSQL volume observation | refresh parity and missed-wakeup recovery pass, no central-PostgreSQL daemon registration or direct call remains, and the separately supported SQLite refresh path retains an exact ledger row |
| local or controller SQLite volume mutation path and `FileLock` | the product separately deprecates that officially supported path | deprecation window and local compatibility inventory are complete |
| volume `--purge` row deletion after provider error | durable cleanup incident is deployed | ambiguous-delete test retains provider identity and eventually proves absence |
| future `legacy_volume_intents` live table, runtime metadata, repository, and indexes | every central-PostgreSQL volume scope is permanently `ACTION_OPEN`, the pre-action rollback window is closed, and no unresolved legacy intent remains | a new forward migration drops the live objects, runtime metadata and imports are absent, upgrade and fresh-chain PostgreSQL catalogs prove absence, and the historical creation migration remains checksum-identical |
| future Release N volume compatibility reader, legacy-intent producer, reconciler, and mixed router | the selected scope and every later volume scope have completed action-authority promotion and their rollback windows are closed | exact locator rows are added before implementation; one compatibility release records zero legacy admission or reconciliation, mixed-version rollback qualification passes, and semantic search finds no compatibility dispatch owner |
| future executable lifecycle binding compatibility adapters | no nonterminal or quarantined effect references the exact adapter approval and every supported rollback image resolves all remaining bindings natively | exact executable locators and adapter approval IDs have zero eligible use for one compatibility release; executable code is deleted while permanent approval, revocation, and use evidence remains audit history |
| future `LEGACY_OPEN` and `DRAINING` routing branches plus `LEGACY_ADMISSION_V1` capability handling | every lifecycle ownership scope is permanently `ACTION_OPEN` and pre-action rollback is prohibited or retired | transition and mixed-version corpora pass, no scope or process advertises a legacy capability for one compatibility release, and a forward schema contraction removes only the legacy states and token while retaining action capability heartbeats |
| central-PostgreSQL cluster process-local provisioning and teardown retry loops | M4 action runtime owns them | crash-at-every-phase tests and test-cluster cleanup pass |
| local or controller SQLite cluster provisioning and teardown retry loops | a dialect-capable durable runtime is deployed or the product deprecates that path | the separate compatibility or deprecation window closes and repository inventory finds no supported SQLite caller |
| central-PostgreSQL Serve in-memory replica request retry ownership and duplicate scheduling loops | M5 action runtime owns mechanics | lifecycle-epoch, same-name recreation, rollout, scale, and failed-cleanup tests pass |
| local or controller SQLite Serve retry and scheduling ownership | a dialect-capable durable runtime is deployed or the product deprecates that path | the separate compatibility or deprecation window closes and SQLite Serve qualification is retired |
| central-PostgreSQL managed-job process-local recovery and cleanup retry ownership | M6 action runtime owns mechanics | controller handoff, preemption, cancellation, and cleanup conformance passes |
| controller-local SQLite managed-job recovery and cleanup ownership | a dialect-capable durable runtime is deployed or the product deprecates that path | the separate compatibility or deprecation window closes and SQLite jobs qualification is retired |
| direct child status transport in `sky.jobs.utils.get_job_status()` | `ChildWorkloadObservationV1` owns the read-only child-status call while managed jobs retain recovery and terminal-state policy | the exact managed-job status and transient-error corpus passes, and the function contains no direct `backend.get_job_status` call outside the adapter |
| direct child status transport in `SkyPilotReplicaManager._fetch_job_status()` | `ChildWorkloadObservationV1` owns the read-only child-status call while Serve retains replica health and rollout policy | the exact replica status, preemption, pool, timeout, and lock-free polling corpus passes, and the method contains no direct `backend.get_job_status` call outside the adapter |
| duplicate managed-image worker lease, heartbeat, retry, and provider-call mechanics | the shared kernel executes already-admitted image actions while image domain admission retains shard fairness and reservations | shard `max_in_flight`, two-level due rotation, fresh-versus-recovery accounting, publication reservations, `COPY`/`VERIFY`/`EVICT`/`READBACK` crash recovery, fencing, quarantine, exact absence, and canary qualification pass with zero dual due or lease owner |
| `api_controller_action_reservations` and `_reserve_controller_action()` / `_mark_controller_action_state()` | generalized action ledger preserves controller-generation fencing and active reservations are backfilled | compatibility reconciler observes zero unmigrated active reservations and rollback qualification passes |

The fleet-gated M5 compatibility paths in
`docs/designs/multi-replica-api-server.md` are outside this migration's deletion
authority. A successful `skypilot-ha` test-cluster rollout does not remove
their fleet rollback gate.

`CloudImplementationFeatures` is not deleted wholesale. Entries move only when
the replacement can express resource-dependent support without losing policy.

## Test Strategy

### Provider contract

Every declared facet generates conformance tests for:

- complete registration and no silent fallback;
- import without provider extras installed;
- audit-snapshot canonical-name, alias, current plugin-identity, expected
  partial-state, and overlapping legacy-view agreement;
- coordinator-era process-local snapshot generation, atomic replacement,
  direct-legacy invalidation, and derived compatibility-view agreement;
- stable implementation digest agreement across replicas, digest change on
  facet replacement, pre-effect equality fencing, and rejection of an
  unverified dynamic plugin;
- provider-wide positive capabilities and resource-dependent predicates;
- persisted realized actuation mode survives configuration changes and keeps
  unbound legacy resources on legacy routing;
- central PostgreSQL promotion never activates the same provider and operation
  binding in local or controller SQLite modes;
- stable create adoption or idempotency;
- typed capacity, quota, authentication, authorization, throttling, and
  invalid-request evidence;
- pending operations remain nonterminal;
- `prepare()` is deterministic and side-effect-free, and the mutation
  submission path contains no readiness polling or domain-status projection;
- compound effects are dependency ordered, individually journaled, and
  readback-safe across a crash after every effect;
- activated mutation subsets include observation, cleanup, compensation, and
  absence-proof dependencies or a characterized legacy compatibility adapter;
- frozen inventories produce deterministic head, role, target-count, and next
  action plans;
- shared and legacy cluster traces agree on existing, resumed, created,
  stopped, and terminated identity sets, and the new projection matches the
  actual caller-visible legacy `ProvisionRecord`, not only a reconstructed
  adapter result;
- absent-resource termination is idempotent;
- ambiguous outcomes enter readback;
- provider request and operation IDs are preserved;
- terminal deletion requires complete absence proof.

### Placement offer

- exact enum member names and wire values, protocol method signatures and
  order, dataclass field order, and factory-only constructors;
- equal recursively frozen payload objects have equal hashes regardless of
  input insertion order, and mutation of the source or thawed output cannot
  mutate the offer;
- a golden canonical-byte fixture locks both digest preimages and IDs;
- the injected payload schema rejects unknown, missing, unsorted, wrongly
  typed, disallowed-string, and empty fields at every depth;
- JSON text rejects duplicates before materialization, floats, constants,
  lone surrogates, explicit C0/C1 controls, invalid fixed `NFC_V1`, and unknown
  envelope keys;
- the locked secret-key deny and allow corpus matches exactly;
- scalar, per-container, combined-tree, 4 KiB payload, and 16 KiB envelope
  boundaries test the accepted value and the first rejected value;
- offer-set, revalidation-factory, capture/context, plan, and handoff
  disposition matrices cover every closed enum member;
- plan tests reject duplicate `task_index` values and accept unique
  nonnegative indices without reordering decisions;
- stable-field changes change both IDs, observation-only changes preserve the
  offer ID and change the observation ID, and requested node count remains
  observation-only;
- import tests prove the generic leaf has no runtime cloud, optimizer,
  backend, provisioner, server, or Kubernetes import and no public root
  re-export;
- the default `Cloud.get_offer_source()` is side-effect-free and returns null.

The S1 suite has these 15 named acceptance tests, with the noted assertions
owned by the named test rather than left implicit:

1. `test_v1_enum_value_sets_are_exact` also inspects exact protocol method
   declaration order and signatures, dataclass field order, factory signatures,
   and disabled direct constructors.
2. `test_offer_payload_is_recursively_immutable_and_detached` also constructs
   every invalid schema-node shape, unsorted or duplicate field and string
   allowlist, invalid provider grammar, and non-object root.
3. `test_observation_capture_requires_matching_provider_and_capture_id` covers
   canonical UUIDv4 acceptance and rejects every provider, context, freshness,
   selection-reuse, UUID case, version, variant, and shape mismatch.
4. `test_offer_set_result_disposition_matrix`.
5. `test_offer_revalidation_result_disposition_matrix` covers every exact
   availability, reservation, quota, and capacity tuple and rejects the full
   invalid cross-product.
6. `test_stable_and_observation_identity_field_partition` includes golden
   canonical bytes and both IDs.
7. `test_envelope_round_trip_returns_fresh_json_builtins`.
8. `test_envelope_recomputes_and_rejects_mismatched_digests`.
9. `test_plan_create_cannot_be_enveloped_or_handed_off` also covers every
   `TaskPlacementDecisionV1` combination, rejects negative and duplicate plan
   indices, and proves unique decision order is preserved.
10. `test_envelope_rejects_unknown_duplicate_float_and_secret_like_values`.
11. `test_envelope_scalar_collection_depth_and_byte_boundaries` also runs
    fixed-`NFC_V1` golden cases containing `U+16FF0` and `U+16FF1` beside the
    older combining mark `U+0301`, and proves C0/C1 rejection is independent of
    the runtime Unicode database. The same goldens execute without pytest in
    the Python 3.10 worker-floor import job.
12. `test_handoff_disposition_matrix`.
13. `test_offer_module_has_only_allowed_leaf_imports` also runs the leaf import
    and runtime-alias compatibility check on Python 3.10 in the existing
    worker-floor job and on Python 3.14 in normal CI; package metadata and
    classifiers cover the supported 3.10 through 3.14 range.
14. `test_placement_types_are_not_publicly_reexported`.
15. `test_cloud_offer_source_default_is_none_and_side_effect_free`.

### Action runtime

M3-S0 precedes the action runtime and has its own focused contract tests:

- tagged failed-fetch and observed snapshots require no fake fields or added
  I/O, and tagged `SKIP`, `NO_WRITE`, and `WRITE` projections admit no invalid
  payload combination;
- the volume projector table covers failed observation, truthy and falsy
  errors, error precedence, pod use, no use, cluster-only use, missing current
  status, identical state, order-only changes, duplicate-only usage changes,
  and each independently changed field;
- projection never mutates snapshot tuples and diagnostic `WRITE` preserves
  observed order while change detection retains legacy set equality;
- candidate and equality errors are separately classified without retaining
  an exception or message, later projections still run, and cancellation or
  another `BaseException` is not contained as an ordinary shadow failure;
- import and source inspection prove the volume projection leaf has no
  database, provider, clock, sleep, lock, logging, or mutation dependency;
- volume-refresh facade tests prove exactly one batched provider read per
  cloud, exact original list arguments reach the legacy writer, unchanged
  missing-handle and missing-row skips, and unchanged config refresh;
- the failed-used-by branch still performs no lock, latest-row read, mapping,
  status write, or config refresh;
- all candidate work begins only after every authoritative write, config
  refresh, and lock release, and candidate or diagnostic-logging failure cannot
  affect current or later legacy work;
- tests independently exceed the snapshot-count, total-usage-reference, and
  UTF-8 identity-byte budgets and prove snapshots are never truncated,
  candidate calls stay capped, `NOT_SAMPLED_BUDGET` is counted, and every
  authoritative write and config refresh still runs;
- lying and stateful list and string subclasses cannot bypass accounting or
  cause a second input traversal, and exact built-in lists are copied once
  through a reference-capped traversal before tuple conversion;
- one bounded anomaly summary covers many volumes, samples at most three
  names through fixed-size accumulation, and contains no raw error, provider
  payload, exception message, projection payload, or full usage collection;
- the PR diff proves M3-S0 adds no schema, worker, claim, lease, heartbeat,
  retry, provider mutation, statistics store, generic shadow package, or second
  production consumer.

The M3-S0 deployment uses the normal exact-SHA gate but has no migration or
ownership activation. It verifies all API roles on one immutable digest,
exercises at least one volume refresh cycle when a safe test volume is
available, and confirms the current writer and public volume behavior remain
authoritative. Absence of a safe live volume is recorded as missing live parity
evidence and never converted into promotion evidence.

M3-S1 adds no production deployment surface. Its characterization gate freezes
non-`use_existing` naming and apply order, conditional existing-resource
deduplication, provider-success/database-failure orphaning, delete
provider-success/database-failure staleness, purge forgetting, concurrent and
partial multi-name delete behavior, independently committed volume sessions,
normal-worker `NEVER` replay policy, PostgreSQL lease ambiguity, local SQLite
no-lease behavior, and current Kubernetes GET, POST, adoption, conflict,
transport-failure, hidden-delete-retry, 404, and no-UID behavior. The exact diff
must contain only this design and the four named characterization files.

### Action runtime after M3-S0

- admission atomically writes the existing domain reservation, exact fence
  identities, and generic action without acquiring capacity twice;
- a domain change invalidates admitted work through its generation or
  reservation fence without domain-specific due-work SQL;
- borrowed transaction callbacks cannot commit, close, or open a nested
  session;
- admission, supersession, completion, child binding, and event emission obey
  the global lock order; injected inverse concurrency either completes without
  deadlock or retries only the aborted effect-free transaction;
- two workers cannot own one live lease;
- lease expiry permits a new owner but fences the stale owner;
- delayed row locks prove lease issue and renewal use a fresh post-lock
  `clock_timestamp()`, independent of application clock skew;
- pool-starvation tests prove the reserved heartbeat budget stops new effects
  before it loses the fence;
- the pre-provider-call fence rejects stale work;
- pause after the final fence, request-bytes-sent with response lost, lease
  expiry during an SDK call, and a hidden SDK retry all enter readback or
  recheck the fence without duplicate submission;
- retry timing is bounded and jittered;
- provider result loss enters readback;
- a lost nested API response recovers through the prebound child request ID,
  observes or cancels that child, and never creates a second request;
- reuse of a child request ID with a different parent, payload, generation,
  workspace, or actor digest fails closed;
- every `IN_FLIGHT` reclaim enters readback and a negative point query alone
  cannot authorize replay;
- crash injection after every compound provider effect retains its intent,
  exact readback locator, expected resource kinds and cardinality, all known
  returned child IDs, and exact next dependency;
- transition and event commit or roll back together;
- action and request source constraints, deterministic transition IDs, partial
  uniqueness, correlation, and the shared event sequence prevent malformed or
  duplicate events;
- V1 event readers see only request-sourced cluster events with their existing
  required fields, while negotiated V2 readers round-trip both source variants,
  volume kinds and targets, and schema-versioned cursors;
- an event insertion failure rolls back action state, domain state,
  reservation release, and sequence allocation on the same connection;
- desired generation changes supersede old work without losing cleanup.

### Domain qualification

- volumes: create, register, refresh, attach conflict, delete, purge, provider
  timeout, lost response, and eventual consistency;
- clusters: create, resume, partial create, stop, terminate, ports, failover,
  same-name recreation, and provider absence;
- Serve and pools: lifecycle epoch, replica inventory, rolling update,
  autoscaling, pool handoff, failed cleanup, and service recreation;
- managed jobs: admission generation, preemption recovery, cancellation,
  controller handoff, and cleanup;
- managed images: shard `max_in_flight`, shard and location due-order rotation,
  recovery-before-fresh fairness, exact in-flight and publication-reservation
  accounting, `COPY`, `VERIFY`, `EVICT`, and `READBACK` crash recovery, and
  zero dual scheduler, lease, heartbeat, or retry owner.

### Compatibility

- mixed N and N+1 roles cannot enable one scoped writer before every owner of
  that domain, operation subset, and store mode is compatible, `DRAINING`
  closes only that admission scope, a paused legacy mutation remains a counted
  intent, and the scoped database epoch fences stale writers in the same
  transaction without disturbing other domains;
- rollback is exercised from every action phase, both with a separately pinned
  reconciler and with full pre-N rollback refusal while any quarantined action,
  unresolved effect, cleanup obligation, legacy intent, or new-reader resource
  remains;
- old client with new server;
- new client with old server where the API version permits;
- serialized handles without new metadata;
- legacy plugin registration during the compatibility window;
- rollback to the previous image before irreversible schema activation.
- supported SQLite cluster, Serve, and managed-job paths remain on their
  characterized legacy owners until their separate ledger gates close.

#### M2 placement-offer handle qualification

M2 extends the existing two-environment harness in
`tests/smoke_tests/backward_compat/test_backward_compat.py` for pickle and client
compatibility only. The base environment defaults to
`v{MIN_COMPATIBLE_VERSION}` and must be isolated from the current checkout. The
local harness does not claim that its sequential API servers share PostgreSQL
state.

The CI qualification performs both directions:

1. The current environment creates and pickles a V14 handle fixture whose
   `placement_offer` and `placement_attempt_fence` recursively contain only JSON
   built-ins.
2. The isolated base environment unpickles that fixture without importing a new
   offer class, exercises its existing handle read methods, and semantically
   ignores the opaque metadata.
3. A base client runs `sky status`, queue, logs, and down-compatible read
   operations against a current server containing an offer-bearing handle.
4. In the reverse direction, a base server emits a pre-M2 handle and the current
   client verifies both new attributes are `None`.

The CI job asserts the active interpreter's `sky.__file__`, SkyPilot version, and
API version before each direction, uses no current-checkout path in the base
process, and fails if the envelope contains any non-built-in value. Loading two
copies of the current class in one interpreter is not compatibility evidence.

Server rollback is a separate `skypilot-ha` PostgreSQL qualification before
promotion:

1. Capture the exact pre-M2 image digest, Helm revision, PostgreSQL Secret, and
   values. Both images must report the same redacted PostgreSQL URI fingerprint
   at runtime.
2. The current image creates an eligible Kubernetes cluster and commits a READY
   V14 handle to that PostgreSQL database.
3. Set placement-offer mode to `off`, drain all active API requests and provider
   mutations, run the current-image rollback preflight, prove every cluster
   handle has `placement_attempt_fence is None`, and only then deploy the exact
   pre-M2 image with `--reuse-values` against the same database.
4. The pre-M2 server must read `sky status`, queue, logs, and other
   down-compatible operations for that row without an unpickle, import, or
   schema error. It is not required to delete the unknown attribute.
5. Restore the current image against the same database, read the same handle,
   tear down the canary cluster, and prove no row, pod, Service, PVC, request, or
   test credential remains.

The test records image digests, database fingerprints, handle version, exact
read results, pod restarts, and logs at every transition. The
minimum-compatible CI test and exact pre-M2 rollback-image test are independent
gates.

## Per-Commit Deployment Gate

Each stacked commit follows this gate:

1. Run focused unit and characterization tests.
2. Run `format.sh` for changed files and the required type and lint checks.
3. Build and push an image tagged with the exact commit SHA.
4. Resolve and record the pushed digest.
5. Capture the current Helm revision and values.
6. Run a server-side Helm diff and prove only intended image or schema changes.
7. Upgrade `skypilot-ha` with `--reuse-values` and the exact digest.
8. Wait for database migration, API, executor, and controller rollout.
9. Verify readiness, request execution, controller leadership, and current
   milestone behavior.
10. Run the applicable in-cluster conformance or canary.
11. Check pod restarts, failed jobs, stuck terminating pods, orphaned
    workloads, and new error logs.
12. Roll back on any failed gate and verify the prior revision is healthy.

No deployment result is inferred from a successful image push or Helm command.
The final live revision is re-read after monitoring. Until the health endpoint
contains a stamped commit, source identity is proven by the immutable image
digest, the ECR exact-SHA tag that resolves to that digest, and the Helm
revision description. A template-valued health commit is not accepted as
identity evidence.

## Rollback

- M1 is code-only additive and rolls back by image. M2 adds no database schema,
  but image rollback is permitted only after the current-image preflight proves
  every `placement_attempt_fence` is null.
- New schema is expand-first. Old readers ignore new tables and columns.
- Mutation ownership switches behind a server-side gate and can return to the
  old writer only before the new writer performs an irreversible action.
- Ownership epochs are independent rows keyed by domain, dependency-closed
  operation subset, and store mode, with routing closed to `DARK`,
  `LEGACY_OPEN`, `DRAINING`, or `ACTION_OPEN`. M3 can therefore cut over only
  `(VOLUME, KUBERNETES_PVC_OWNED_LIFECYCLE_V1, CENTRAL_POSTGRESQL)` without
  changing cluster, Serve, jobs, image, or SQLite ownership. A later milestone
  repeats the same protocol for its own scope.
- M3-S2 is an earlier pure dark expansion, not Release N. It leaves the pilot
  scope at `DARK`, has no process-capability or legacy-intent table, and makes
  no current handler consult the scope or writer-authority seal.
- Release N later ships a compatibility reader, reconciler, and legacy-intent
  producer against the already-expanded M3-S3 schema while the selected scope
  is still `DARK`. Every `all`, `api`, `controller`, or `executor` supervisor
  capable of hosting or delivering that scope must run N or later, advertise
  the exact child-handler image when applicable, and pass mixed-version
  qualification. One guarded transition must first acquire the external grant
  for the next authority generation, then bind the writer-authority seal,
  increment the authority generation and epoch, and move only that scope to
  `LEGACY_OPEN`. Release N+1 cannot enable the action writer before that
  transition and its qualification window complete.
- Release N also makes every legacy mutation admission in the selected scope
  lock and check that scope's database ownership epoch and persist an active
  legacy intent before external I/O. The intent contains its resource
  incarnation, request identity, deterministic readback locator, and effect
  certainty. It becomes terminal only after the provider call and required
  readback finish. A wait or hidden SDK retry rechecks the epoch and intent
  token before its next underlying attempt.
- N+1 cutover first moves only the selected scope from `LEGACY_OPEN` to
  `DRAINING` in one transaction. That closes new legacy and action admission
  for the scope. The compatibility reconciler then proves its counted active
  and ambiguous legacy-intent set is empty. `DRAINING` permits only the exact
  already-registered intent token to finish or read back; it cannot create
  another logical mutation. Only a second epoch transaction may verify that
  empty set, the minimum compatible reader, and the selected scope's deployed
  mutation-role set, then record the exact writer implementation and move the
  scope to `ACTION_OPEN`.
- Every action admission, claim, effect-intent, and terminal mutation checks
  its scope's `ACTION_OPEN` epoch in its own state transaction. A process-local
  flag cannot grant ownership. A release N process paused after its legacy
  guard remains in the scoped counted intent set, so cutover cannot pass it
  silently.
- A pre-N image cannot enforce a field it does not understand. After N+1 has
  admitted any action, a full image rollback to pre-N or replacement of the
  only compatible reconciler is forbidden until the current image proves zero
  nonterminal or quarantined actions, zero unresolved effects or cleanup
  obligations, zero active legacy intents, and zero live resource incarnation
  that requires the new reader. Only then may it disable every activated scope
  in final database transactions. Quarantine is unresolved evidence, not a
  safe drain result.
- A role-specific rollback of stateless readers is allowed only while a
  separately deployed compatible reconciler and worker image remains pinned
  and every activated scoped ownership epoch excludes the rolled-back role.
  The Helm
  preflight records that retained deployment. Otherwise the system rolls
  forward rather than replacing the reconciler.
- Schema contraction occurs only in M7 after the rollback window closes.
- A rollback never deletes action, event, provider identity, or cleanup
  evidence.

## Completion Criteria

This migration is complete only when:

- M1 through M7 are merged with full CI on each exact pushed SHA;
- every stacked commit has a recorded successful `skypilot-ha` deployment and
  milestone-specific live proof;
- provider conformance covers every strict built-in facet;
- every migrated central-PostgreSQL domain path uses the shared mechanics
  without losing its domain fences, while retained SQLite paths remain named
  legacy ledger rows rather than being counted as migrated;
- every `must_remove` and `must_contract` removal-manifest row is `removed`,
  and every retained-history or characterization row is `retained_verified`;
  a row with an external blocker remains incomplete;
- the removal checker passes in `final` mode on the exact merged SHA, with no
  `must_remove` or `must_contract` row left in any incomplete status;
- every live-schema contraction passes PostgreSQL catalog assertions after an
  upgrade and a fresh full-chain installation, and every retained migration is
  checksum-identical and unreachable from runtime imports;
- repository search finds no superseded version branches, silent plugin
  fallback, placement reconstruction, or duplicated retry owner in migrated
  paths;
- the final Helm release is deployed and healthy;
- no failed migration Job, stale canary, terminating pod, or orphaned test
  workload remains;
- this design matches the final code and deployed behavior.

## Adversarial Review Record

### Review 1

Verdict: `RESHAPE`.

The review rejected a universal M1 provider registry, unversioned facet
contracts, provider-owned retry scope, a literal zero-mismatch placement gate,
an unspecified action-store transaction, premature SQLite volume removal, and
a rollback reconciler introduced only after an in-flight action existed.

This revision responds by:

- limiting M1 to `ProvisionerBundleV1` and the exact synchronous
  `InstanceLifecycleV1`;
- separating `OfferSource` and deferring a universal `ProviderDescriptor`;
- preserving and precisely characterizing the legacy plugin contract;
- separating operation state, failure kind, effect certainty, and domain retry
  policy;
- defining M2 offer identity and storage boundaries;
- making M3 explicitly unapproved until its transaction, schema, activation,
  compatibility, and rollback contracts are written here and re-challenged;
- retaining officially supported SQLite volume operation;
- adding controller-action reservation and fleet-gated HA removal boundaries.

### Review 2

Verdict: `PURSUE` for M1 and M2.

The second review verified coherent ownership, the versioned V1 compatibility
contract, concrete offer identity and persistence, and objective rollout and
removal gates. M3 remains explicitly unapproved and always requires its later
dedicated review.

### Review 3

Verdict: `PURSUE` for M2.

Three independent reviews reloaded the complete M2 contract at exact SHA-256
`038a4d9ea459c8e1117cac56b440a3972e7dc8f2924fc08960d5c17e08ea226a`.
They verified:

- complete shadow-only legacy fallback without weakening authoritative gates;
- live ServiceAccount UID binding in stable identity, revalidation, Pod
  creation, final evidence, and READY;
- exclusion of ports, autoscalers, controllers, HA, reuse, restart, mounts,
  plugins, and unmodeled resource or configuration modes;
- one pinned Kubernetes client and exact-target command transport through
  bootstrap, mutation, runtime setup, READY, and safe failure cleanup;
- all-phase Pod and Service conflict checks plus a dedicated fresh-create path;
- a durable attempt fence committed before provider I/O, exact Service and Pod
  UID inventory, fenced reconciliation, lifecycle guards, and rollback refusal;
- first-provider-attempt authority and typed initial or later legacy fallback.

No confirmed M2 safety, compatibility, or implementation blocker remained.
M3 was out of scope and remains explicitly unapproved.

### Review 4

Verdict: `PURSUE` for M2 S1.

Three independent reviews reloaded the complete generic offer contract at exact
SHA-256
`3877665d095e330ff55ad96c72b4241b3d838d9f91506d5fb0c64d23f6c6938e`.
The final review verified:

- exact wire enums, dataclass and protocol order, factory-only construction,
  canonical bytes, digest preimages, and bounds;
- an immutable injected payload-schema DSL with constructor-time invariants;
- fixed Unicode 3.2 normalization, explicit control ranges, and deterministic
  behavior on the Python 3.10 through 3.14 support range;
- canonical UUIDv4 capture provenance and a distinct fresh authoritative
  capture;
- closed offer-set, revalidation evidence, plan-decision, and handoff
  disposition matrices;
- provider-owned Kubernetes envelope narrowing without importing provider code
  into the generic leaf;
- explicit S1 ownership of the generic handoff contract and existing Python
  3.10 worker-floor qualification, while S2 and S3 retain provider and
  orchestration ownership.

No confirmed S1 wire, compatibility, ownership, or implementation blocker
remained. M3 was out of scope and remains explicitly unapproved.

### Review 5

Verdict: `PURSUE` and `MERGE` for the M2 S1 implementation.

Three independent read-only reviews verified the final implementation against
this contract. The reviewed leaf hashes were:

- `sky/placement/offer.py`:
  `793548c249ecff76c659c45909bf464206ef0fb0d26fcf29041671e34cb158b8`;
- `sky/utils/json_types.py`:
  `1e1b7eb856b3e0f537d02d139fcb580f99be8b407cadad7b84f25d3f70a6d988`;
- `tests/unit_tests/test_placement_offer.py`:
  `e4ed1aa7e666092711380a323e70fffd29ab200f7d5c46e194c17d16a57a8b11`;
- `.github/workflows/static-analysis.yml`:
  `2d46ea4666376818843122469659f2b8fade175b411fe526bcb74c891252fdc8`;
- `sky/clouds/cloud.py`:
  `296c22a9c8c23ce37ee30a2eeba9a8d80cee3547723363ba5673e27115fd7a53`;
- `sky/placement/__init__.py`:
  `0f6cd7b22225e11c37c395297672e8a68cac603304dd133ecd30c46afd0b58ec`.

The security review found that runtime protocol membership alone accepts a
non-callable `close` attribute. The implementation now explicitly requires a
callable cleanup method, and the capture, authoritative validation, and
handoff tests lock that rejection across Python 3.10 through 3.14.

The final 15-test acceptance suite also calls every blocked constructor and
covers request validation, `REUSE` and `RESTART` construction rejection, TTL
boundaries, empty-string schema behavior, nested missing keys, malformed
persisted timestamps, and the complete contract matrices. Local qualification
passed the focused suite, YAPF, isort, Ruff, mypy, Pylint, full-package Python
3.10 compilation, and installed-package imports at the Python 3.10 and 3.14
support endpoints. S1 remains additive: it activates no provider source,
changes no mutation owner, and closes no removal-ledger row. Exact-head CI,
image qualification, deployment, canary, and monitoring remain required before
S1 is recorded as deployed.

### Review 6

Verdict: `MERGE` completed for M2 S1.

Exact head `8721dd9f968cd1cdf5d5eb228c3f8d92ead54d6a` passed all 24
GitHub checks and resolved to linux/amd64 image digest
`sha256:62a781c80cf9fac78e8fcdf7c6fca79484efc3c470e1d992da0da8c39fac0f30`.
Helm revision 34 ran that exact runtime commit, version `1.1.0`, and build
`7972` in all six API, controller, and executor pods. Its migration Job
succeeded 1/1 with zero failures.

Canary `m2-s1-8721dd-canary` passed launch, exec, status, down, and complete
temporary resource cleanup. Six explicit monitor samples from `03:20:01Z`
through `03:23:16Z` were clean, and the independent audit found no later
candidate warning before unrelated revision 35 superseded it. The revision 34
migration Job was removed after audit. Revision 35 was assessed separately and
is not represented as S1 live-state evidence.

PR #1080 merged at
`53973b18a6e214b37b0ac3985d148886eb422a01`. Its first parent is
`9b909c44205dbd584a54f9d3ece751e69a43f480`; its second parent is the exact
tested and deployed S1 head. The remote feature branch was deleted after the
merge.

### Review 7

Verdict: `PURSUE` for the responsibility-deduplication architecture.

The review compared SkyPilot with
`dstackai/dstack@ccef71f46b8e61ce3c139d3c147911b6dd19f8a2` and selected three
additional boundaries: a shared cluster reconciler over journaled provider
effects, a generic kernel for already-admitted durable actions, and a staged
provider registry audit and authoritative descriptor coordinator.

Provider-specific challenge returned `PURSUE` after the design added
provider-assigned-ID readback locators, stable implementation digests,
realized actuation and store-mode bindings, dependency-closed promotion, and
provider-scoped legacy fallback. Two independent final reviews reloaded exact
contract SHA-256
`04a5daade6267c316a5c547ce423fe502f2e593f0a699b217587326e9422fe65`
and returned `PURSUE` after the design closed:

- domain-owned admission and global transaction lock ordering;
- nested child-request identity and fail-closed idempotency;
- fresh post-lock database time and reserved heartbeat capacity;
- scoped legacy-intent drain and N then N+1 ownership cutover;
- versioned request and action event wires;
- PostgreSQL versus retained SQLite scope;
- actual legacy `ProvisionRecord` shadow comparison;
- managed-image fairness, accounting, phase recovery, and zero-dual-owner
  gates.

This verdict accepts the architecture and removal ledger. It does not approve
M3 schema or M4 actuation implementation. Each still requires the dedicated
exact-design review named in its milestone before code is written.

### Review 8

Verdict: `MERGE` completed for the responsibility-deduplication design.

Exact head `d601e17e339b231e036766f37b8d465f042abc92` passed all 24
GitHub checks with no review or comment state. PR #1081 merged at
`7aaa99041065a57c6f733ceed04f025520bac871`; its first parent is the M2 S1
merge `53973b18a6e214b37b0ac3985d148886eb422a01` and its second parent is the
exact reviewed design head. The PR changed only this canonical design, so it
had no runtime image or live deployment delta. The remote feature branch was
deleted after the merge.

### Review 9

Verdict: `RESHAPE`; the first M2 S2 prototype must not merge.

The exact working-tree review found that the provisional 2,249-line
Kubernetes source repeated candidate selection, configuration precedence, and
template rendering instead of removing those owners. Its implementation
fingerprint covered seven provision lifecycle callables but not the production
render and pre-create mutation path; changing
`Kubernetes.make_deploy_resources_variables()` could therefore change a hard
selector while classification remained eligible. Node reads were count-bounded
but could retain an arbitrarily large label object, context reads had no
aggregate deadline, upstream kubeconfig exec authentication could run without
the bounded scrubbed path, and explicit close left credentials and temporary CA
files alive until garbage collection.

S2 is approved to resume only through S2a.1 and S2a.2 above. A new exact-diff
review must prove that production and offer capture call the same context,
precedence, base-render, and final-Pod owners; that node input and wall time are
aggregate-bounded; and that credential execution and cleanup fail closed. The
rejected source is research evidence, not an implementation to merge.

### Review 10

Verdict: `PURSUE` for S2a.1 only.

The challenge review found that sharing context selection, effective-config
precedence, final Pod mutation, and bounded observation inputs removes proven
owners without committing the system to an offer-specific framework. Each
change has a direct legacy caller and a reversible characterization boundary.
S2a.2 did not pass the same completeness bar because the immutable resolved
inputs to base rendering are not yet enumerated; its explicit design and review
gate above prevents implementation from starting on an assumed interface.

### Review 11

Verdict: `PURSUE` for the completed S2a.1 shared observation primitives.

The final independent architecture review inspected the settled working tree
on base head `c014c1dba8` and returned `PASS` with no confirmed blocker. The
reviewed leaf hashes were:

- `sky/adaptors/kubernetes.py`:
  `d82b885189476f3c6e91eaa1c3675741a3da241536d35209eeb6e99b8ffddd3e`;
- `sky/clouds/kubernetes.py`:
  `1a0f3c4f9815a94e1ee3b58b064835fed1662d1fb42dc79a4fd13c6428069dc4`;
- `sky/provision/kubernetes/utils.py`:
  `d50327ec09b2e3b65aeb5409cf3dc4b33d20e40b768053e76c815f5b4b0f42b9`;
- `tests/unit_tests/test_sky/adaptors/test_kubernetes_observation_primitives.py`:
  `ec43408de6bd7d7a981d6052a61b3c2de94481604acfef0a92272788d9d2d178`;
- `tests/unit_tests/kubernetes/test_kubernetes_node_observation_primitives.py`:
  `5dced105a72a17e6bd0c4ca002dad6edc7f849c52b96084d22e8dd84bd7078b9`;
- `tests/unit_tests/test_sky/clouds/test_kubernetes.py`:
  `5938f0528aec59bbe7acbdde8f5acf085f5585cbee2c95ec96983be49115f865`.

The iterative review closed cleanup-exception isolation, exact root metadata,
decoded gzip EOF, and three responsibility-duplication findings. Shared owners
now decide GPU formatter selection, first-Ready readiness, kubeconfig identity,
in-cluster identity, and API-client cleanup. The bounded path adds exactly one
provider `list_node()` operation and retains no raw label map or value. It adds
no S2b offer, classifier, aggregate-deadline, or orchestration wiring, no S2a.2
base renderer, and no provider mutation.

Local acceptance imported the intended worktree and passed 720 serial tests,
exact YAPF 0.32, isort 5.12, Python compilation, changed-file mypy, exact
Pylint 2.14.5 at 10.00/10 for production and new tests, and
`git diff --check`. The three broader mypy findings in
`check_instance_fits()` predate and are outside every changed hunk. Exact-head
CI, review state, and merge proof remain required before this slice advances to
S2a.2.

### Review 12

Verdict: `PURSUE` for the final rebased S2a.1 implementation and its CI
corrections.

Review 11 records the pre-rebase implementation review. This review supersedes
it as the merge evidence after rebasing onto exact base
`7ad9483f62a5da58d18cb8d09e564411ce8b1757` and applying the current CI
contracts. The final independent read-only architecture review returned
`PASS`. It confirmed that YAPF 0.43 changes are AST-identical formatting, the
explicit `ASYNC103` and `ASYNC104` annotations preserve the designed
cleanup and credential exception-containment boundaries, optional deployment
state is only statically narrowed, and tests now mock the canonical snapshot
configuration owner instead of relying on the removed ambient-read seam.

The reviewed leaf hashes are:

- `.basedpyright-baseline.json`:
  `a403cf78e495487b59687e1f0cb3a2f557348b064a598392f0fe9a9421bbdd7c`;
- `sky/adaptors/kubernetes.py`:
  `3e58acfe82f3dc2deddfc2747a122ceabda11b212ba347c0a6505883dfca2b6a`;
- `sky/clouds/kubernetes.py`:
  `33022b2315ec85921b932c56c51f1239f2f95be95dc9f42ceb55282f87a004ea`;
- `sky/provision/kubernetes/instance.py`:
  `dbe15d4de0cef17d0eff1d4a1101fd42e221ef4005e40ca71eccfabf9a53044b`;
- `sky/provision/kubernetes/utils.py`:
  `69df95a4f6581f7184faacfef2dccd773537bb4d431a32c4f91fb92b456dab2e`;
- `tests/unit_tests/test_sky/adaptors/test_kubernetes_observation_primitives.py`:
  `ec43408de6bd7d7a981d6052a61b3c2de94481604acfef0a92272788d9d2d178`;
- `tests/unit_tests/kubernetes/test_kubernetes_node_observation_primitives.py`:
  `5dced105a72a17e6bd0c4ca002dad6edc7f849c52b96084d22e8dd84bd7078b9`;
- `tests/unit_tests/test_sky/clouds/test_kubernetes.py`:
  `c5ad057fbf2e856f53b1062c1c79b62e66df0b045a9f666d1671976bf37282a4`;
- `tests/unit_tests/test_sky/test_check.py`:
  `44f7c5374b712b308714a7c1b1f475c82252a47556dc57a3727002249d706c32`.

Local acceptance imported the intended worktree and passed 741 affected serial
tests, 3 subtests, YAPF 0.43, isort 5.12, Python compilation, and exact Pylint
2.14.5 at 10.00/10 for production and new tests. Exact basedpyright 1.39.9 on
Python 3.14 reported zero errors, warnings, or notes and removed exactly five
resolved baseline entries. The flake8 7.3.0 and flake8-async 27.7.1 output
remained byte-identical to the 17-line repository baseline at SHA-256
`145746a05f5781e3d6654ca963172382f87b9a10abb5cd08d92fea8c987a8d11`.
The corrective diff adds no placement offer, classifier, aggregate-deadline or
orchestration activation, provider mutation, retry owner, or statistics
storage. Exact-head CI, review state, merge proof, and live rollout evidence
remain required.

### Review 13

Verdict: `RESHAPE` for the initial S2a.2 full-template proposal.

The adversarial review rejected treating the entire Kubernetes cluster template
as the pure placement boundary. `TemplateSpec.variables` accepts arbitrary
plugin objects, the full template includes credential and path bindings that an
offer must not acquire, managed-image policy runs after render and again after
reuse, and reuse can replace the newly rendered `node_config`. A full-template
owner would therefore either break plugin compatibility or claim placement
authority over state it does not own.

This finding supersedes Review 9's provisional requirement that production and
offer capture call the same base renderer. They must share characterized policy
and scheduling semantics, but S2b must prove those semantics through the bounded
source-safe projector gate above rather than invoking production full
materialization.

The review accepted the built-in-only fragment direction subject to an exact
canonical contract. The S2a.2 section above now records the physical outer,
fragment, monolith, and 71-name digests; one exact source composer and current
initial full render/pre-combination parse plus preserved later parse sequence; a
frozen post-parse merge input; missing-versus-null
semantics; one identity-admitted private pre-render snapshot-fed Pod projection
shared by host-network variables and merge, followed by deferred metadata
resolution; public mutable compatibility for owner/template gates; full-object
exact-order metadata ownership; safe-YAML scalar and alias preservation through
typed mutable-combination fallback; the sensitive-data boundary; plugin compatibility
adapter; authentication-sentinel, managed-image, reuse, and S2b full-render
exclusions; post-managed-image validation order; deterministic-gzip
prerequisite; rollout; and removal gates. This edit still requires adversarial
review against the exact file digest before implementation is approved.

### Review 14

Verdict: `PURSUE` for the bounded S2a.2 semantic contract at SHA-256
`cdd29b4ea099b1635725cee2dafccbbbe63f9ea4aeff05e529a8193a0115e830`.

The exact source review and compatibility review returned `PASS`, and the
adversarial architecture review returned `PURSUE`. They independently verified
the file digest and clean diff. The source review rechecked the physical
template offsets and digests, 71-name presence contract, parse and serialization
counts, Pod and metadata merge order, private and public callback counts, and
the retained post-bulk boundary.

The review rejected the intermediate Kubernetes-only pinned lifecycle facet,
private bulk keyword, and post-provision deploy-variable handoff. That design
duplicated the provider authority reserved for M4 and failed to cover existing
dynamic bulk helper and teardown seams without further expansion. The accepted
contract ends at `write_cluster_config()`: one writer-time Pod projection feeds
host-network variables and post-parse merge, deferred metadata uses the same
shallow raw-reference snapshot, and the existing post-bulk callback remains
authoritative for fresh runtime setup.

The accepted dispatch has eleven callable identities across ten owner gates.
Its composite Config projection owner checks both the exact module-level
`config_utils.Config` class and exact `Config.get_nested` method, so a replacement
class cannot inherit the method and evade compatibility dispatch. Unrelated
generic and Kubernetes config reads remain dynamically resolved. Authoritative
placement and provider-wide deploy-variable freshness remain disabled until M4
supplies immutable descriptor-owned actuation. Implementation may begin only
after the live physical-capacity branch is reconciled with the deployment base.

### Review 15

Verdict: `MERGE` and staged deployment completed for the S2a.2 built-in
Kubernetes template source composer.

PR #1103 passed all 29 visible checks on unchanged head
`389c8e861e7695b482fba94505e1765427776b9d`. It merged normally at
`24d2eb250274b9ed3052a5891cddc3edbf322eae`; the first parent is
`c96bd97d4c6a0b3573b00ac25a3a3a7f90cb91ed` and the second parent is the exact
tested head. The remote feature branch was deleted only after merge and parent
verification.

The clean-clone merge image resolved to immutable ECR digest
`sha256:d3beddddc62a75662fe1c3ff8e36f9d3e1d8477631e9bdae95103750ef6f2c9d`.
The pulled image reported commit `24d2eb250274b9ed3052a5891cddc3edbf322eae`,
build 8067, and exact monolith, outer, and fragment SHA-256 values
`988b6d5e2afd7e96b3a6d7e0091c661a3d05d5a61d23fd7efa138ab75d55a6f8`,
`3f9343f8ff289711d931af2915391338ac628d30d96fb10e66b4808578eadcd1`, and
`09ea5d743a09286649c56f26c5b737764b81730b90fefae2bd561e0707a72e04`.
Runtime composition reproduced the monolith byte for byte.

Helm revisions 46, 47, and 48 rolled API, executor, and controller separately
with `--reuse-values`. Final revision 48 is deployed with all three roles on
the immutable digest. Migration Job 48 used that digest, completed with one
success and zero failures, and the external-service-account RBAC values and
bindings remained intact.

Six post-deploy samples ran at `00:27:22Z`, `00:28:06Z`, `00:28:50Z`,
`00:29:31Z`, `00:30:10Z`, and `00:31:02Z` on 2026-08-01. The first observed a
PDB-safe Karpenter executor replacement at one of two Ready replicas. Samples
two through six were stable at two Ready, updated, and available replicas for
every role, all on the exact digest with zero restarts. API, executor, and
controller health endpoints returned HTTP 200 in every sample. No Warning
event occurred after the second sample, and current pod logs contained no
source-composer or attributable error. Capacity mode remained disabled,
capacity schema version remained 001, and all five capacity tables remained
empty.

### Review 16

Verdict: `PURSUE` for the pre-M3 volume projection characterization contract
at SHA-256
`8cde65a846b790e9d1d306fd1651e273e279c8cda2b10a8067e3f30cfb4e2c52`.

The first adversarial pass returned `RESHAPE` because one consumer did not earn
a generic reconciliation package, failed observation could not honestly fill
one ordinary snapshot, candidate work could precede later authoritative
writes, per-volume warnings could flood the existing Datadog pipeline, and the
initial deferred snapshot collection was unbounded. The reviewed contract now
keeps the seam volume-local; uses tagged failed-fetch and observed snapshots;
uses tagged `SKIP`, `NO_WRITE`, and diagnostic `WRITE` projections; runs all
candidate work after the authoritative sweep; distinguishes projection and
comparison failures; caps complete snapshots, usage references, and UTF-8
identity bytes; and accumulates at most three anomaly names without collecting
an unbounded list.

The accepted slice adds no generic worker, action store, schema, lease, retry
owner, provider mutation, second observation, status enum, Datadog metric, or
statistics store. Original lists continue directly to the legacy writer, and
the pure diagnostic projection can never supply writer arguments. A generic
comparison helper remains prohibited until a second production domain proves
the same contract.

### Review 17

Verdict: `PURSUE` for the corrected M3-S0 implementation contract at exact
heading-through-before-Cleanup SHA-256
`e98439a57dd81c7956b6f0eb1aaeb8affe2a8753c546c3bec3915e55308847d8`.

Two independent implementation reviews initially returned `DO NOT MERGE`
because the first capture implementation accounted overridable list and string
operations, then traversed each input again during tuple conversion. A lying or
stateful subtype could therefore retain more than 4,096 references or 256 KiB
while debiting a smaller value.

The corrected contract and implementation reject non-exact list and string
inputs without invoking subtype behavior, traverse each admitted exact list
once into reference-capped built-in copies, account UTF-8 bytes from the exact
identities retained in those copies, and construct snapshots only from the
bounded copies. Both original reproductions now fail closed with no budget
debit. Both reviewers returned `LGTM`, and the exact-design adversarial
re-review independently verified the section digest and returned `PURSUE`.

Local qualification currently includes 45 pure-projection and facade tests,
the existing 55 volume-core tests, clean mypy across 817 source files, and a
10.00/10 pylint result. Exact-head CI, merge, immutable-image deployment, and
live-volume evidence remain open gates.

### Review 18

Verdict: `MERGE` and staged deployment completed for the M3-S0 volume refresh
shadow projection.

PR #1110 passed all 29 visible checks on exact head
`2732284e329cd45bac313e5fa301c9bf6d86ac53` with no legacy status context,
review, or comment left unresolved. It merged normally at
`6070160a1379dc0e76fc2a614651cb3ae83b391b`; the first parent is
`8c64e4c50c681db42741142a7c9792d40a14a2be` and the second parent is the exact
tested head. Changed paths are byte-identical between the tested head and merge
result. The remote topic branch was deleted only after merge and parent proof.

The merge image tag `pr1110-6070160a13` resolves to immutable digest
`sha256:a5afbd26e62ebe2f6990b2f311a59caaf3ef2901f2eab5d6dddd46527320f00a`.
The Linux amd64 image reports exact commit
`6070160a1379dc0e76fc2a614651cb3ae83b391b`, build 8080, and imports
`sky.volumes.refresh_projection` from
`/skypilot/sky/volumes/refresh_projection.py`.

Helm revisions 49, 50, and 51 rolled API, executor, and controller separately
with `--reuse-values`. Migration Jobs 49 through 51 each completed once with no
failure, and all used the exact immutable digest. A PDB retained at least one
ready replica during Karpenter churn. After the last churn reset, six clean
samples at `02:19:29Z`, `02:20:21Z`, `02:21:10Z`, `02:22:00Z`, `02:22:51Z`,
and `02:23:46Z` on 2026-08-01 showed two ready, updated, and available replicas
for every role, all six pods on the exact digest with zero restarts. API health
and readiness, executor readiness and liveness, and controller readiness and
liveness passed in every sample. Logs after reset contained no volume-shadow
anomaly, traceback, exception, fatal, panic, or error match, and no Warning
event occurred after reset.

Capacity mode remained disabled on every role and pod. Schema revision stayed
at 001, all five capacity tables stayed empty, and PostgreSQL reported zero
capacity-projector connections. An authenticated cached `sky volumes ls` from
an API pod returned no existing volumes. The warning-free rollout is therefore
negative safety evidence only; it does not prove a positive per-volume parity
denominator, and the shadow remains diagnostic.

The apparent ECR scan summary change was a pagination artifact. A complete
paginated comparison found the same 209 suppressed findings on both digests,
with zero added or removed Critical, High, or Medium findings and identical
package versions. No vulnerability-based rollback was required.

### Review 19

Verdict: `PURSUE` for the M3-S1 volume mutation transcript and durable action
envelope at exact heading-through-before-Cleanup SHA-256
`873e0c45e8b028059a7f1d6dce09c3c6170acb1576341e45cfa86c35475b8aa1`.

The first adversarial pass returned `RESHAPE` because the draft treated mutable
record state as identity, overstated the new-PVC duplicate check, left action
deduplication and atomic admission implicit, could strand old bindings, lacked
exact Kubernetes ownership and defaulting rules, and confused replacement UID
evidence with foreign deletion authority. The second pass required an exact
sidecar choice, reproducible UUID encoding, claim and transition ownership,
quarantine recovery, permanent identity evidence, mixed-version routing,
lost-create-response replay bounds, and a separate purge detach acknowledgement.

The accepted contract now fixes the sidecar and store identity, action,
attempt, and effect keys, legal phase and certainty pairs, claim allocation,
cleanup ownership, compatibility adapters, activation epoch, retention,
Kubernetes transport and namespace identity, reserved ownership annotations,
defaulting-aware comparison, terminating and Pending behavior, bounded
same-effect create resubmission, UID-scoped delete, replacement completion, and
legacy exclusion. The final correction records pre-quarantine phase so only a
proved zero-submission effect returns to `PRE_INTENT`; ambiguous work resumes
through `READBACK`. Independent source review returned `PASS` against exact
base `3f4f73b3abeb3943abb0af601df05ff570010122`, and an independent adversarial
review also returned `PURSUE`.

### Review 20

Verdict: `PURSUE` for the corrected M3-S1 contract at exact
heading-through-before-Cleanup SHA-256
`60fba2aa5323a7bbd224582991177ebeada66f1e1d6af743ba75f8ee6f1cca99`.
This verdict supersedes Review 19 for implementation scope while preserving it
as the history of the earlier review.

A pre-merge M3-S2 DDL audit returned `RESHAPE`. It found that the earlier text
mixed a pure dark expansion with Release N legacy interception, described
activation as both global and scope-keyed, allowed compactable effect evidence
to own the only live PVC UID, did not fence an ordinary database clone with
authority outside the backup, and attempted to assign the full action graph to
M3-S2 before its exact relational model was reviewed. A separate milestone
review also rejected enabling create authority before its dependency-closed
UID-scoped delete and cleanup path was qualified.

The corrected contract retains live provider identity on the durable sidecar,
uses an external writer-authority seal, renewable exclusive grant, and a
scope-keyed ownership epoch and authority generation, plus a source workload,
provider-call-authority, and ambiguity-horizon transfer fence. It defines
`DARK`, separates M3-S2 from Release N, and narrows M3-S2 to exactly two
PostgreSQL-only lifecycle data tables and two inert seed rows. The full graph,
process capabilities, immutable bindings, payload bytes, correlations,
evidence cardinality, identity anchors, per-effect tombstones, and restricted
transaction facade move behind M3-S3's dedicated exact-DDL review. Create and
UID-scoped delete, readback, replacement, and cleanup must be implemented and
qualified while disabled, then enabled together as one dependency-closed
owned-PVC lifecycle scope.

Independent review against base
`cb51285f90b54314ff76d1fbb59a779bbd059a0e` returned `PASS`. The dstack-boundary
review and the PostgreSQL schema adversarial review independently returned
`PURSUE` on the exact corrected digest. The characterization corpus remains
unchanged and passing.

### Review 21

Verdict: `PURSUE` for the M3-S2 exact inert PostgreSQL foundation subsection at
SHA-256
`1e3c78b35ee24b2d8c1d74536ab5f02621977cc8c3b72bbdb6faed001168fdc4`.

The first exact-DDL pass returned `RESHAPE` because the scratch proposal
admitted active routing states and well-formed authority digests in revision
`001`, created a redundant UUID uniqueness index, exposed an engine-shaped
runtime surface, and tested same-process migration serialization rather than
independent contenders. It also needed explicit clone preservation,
database-enforced read-only snapshots, atomic failed-downgrade assertions, and
direct SQLite rejection.

The corrected contract pins the only legal revision-001 scope to the exact
unsealed `DARK` seed in named database checks. Any later activation revision
must explicitly replace those checks with the reviewed legal transition
matrix and prove the exact inert shape before restoring them on downgrade. The
runtime exposes only frozen snapshots, the guarded downgrade locks both tables
before reading, and the test contract now covers independent processes,
post-revision clones, no repair, DML rejection, and complete rollback
atomicity.

Independent source-convention review returned `PASS`, PostgreSQL schema review
returned `PURSUE`, and deployment and rollback review returned `PASS` on the
exact digest. No implementation began before those verdicts.

### Review 22

Verdict: `PURSUE` for the additional dstack architecture review at SHA-256
`5ec3503d55c4c5cc59346ce76f3448f84016636edcbf5f27e52d168d27cda0e4`
and the M3-S3 architecture decision at SHA-256
`9d2cd2fe758222927012e7a25e2000f442a508d8482c47a55b72438ae11f952f`.
This verdict approves a design-only boundary, not revision-`002` literal DDL
or implementation.

The first M3-S3 challenge returned `RESHAPE` because separate anchor, live,
and tombstone tables required fragile cross-table uniqueness and completeness
triggers; preflight evidence ownership was ambiguous; one UID could not prove
replacement or conflict; adapter approval had no revision or revocation;
process roles did not explain supervised children; and compaction machinery
was premature. The corrected contract uses permanent narrow action and effect
rows, separates admission from post-admission provider evidence, retains
target and terminal-observed identities, pins epoch-fenced adapter approvals,
fences supervisor and child execution, and leaves revision `002` full-only and
empty. Independent adversarial re-review returned `PURSUE` with every listed
blocker resolved.

The independent dstack source review initially rejected claims that dstack
advertises method capabilities or imposes stable offer order. It also required
single-flight and bounded capability revalidation, explicit transient-failure
semantics, characterization of DigitalOcean's missing rejection hints, and a
complete child cancellation removal gate. The corrected text distinguishes
the dstack lesson from SkyPilot's stronger proposed contract and narrows the
first removal rows to exact read-only pilots. Source re-review returned
`LGTM`.

The removal-ledger review at SHA-256
`33368a3615074107c6a0380bb868e50f34bdc5d37613bc4e8655c60c12dbc336`
keeps Markdown authoritative for this design-only slice and makes the
manifest, semantic checker, tests, and CI invocation the mandatory next slice.
It closes manifest enums and transitions, makes blockers nonterminal, requires
schema and historical-migration evidence, classifies SQLite compatibility as
incomplete `must_remove` work, adds transitional M3 obligations, and splits
volume core mutation from the exact daemon owner. The manifest and checker do
not yet exist, so no claim of executable enforcement is made by this review.

### Review 23

Verdict: `PURSUE` for the M4 deploy-variable snapshot foundation and
DigitalOcean pilot subsection at SHA-256
`15815eeb688472d3a02a0e3c2c97d23528619ba3b026ebbedeb704c124a076bd`
and the executable removal manifest at SHA-256
`a8ff84b514d73bc81d2c063821ad189e6c4f130bbb975f17040320408f1dedd1`.

The first challenge returned `RESHAPE` because a DigitalOcean-only backend
branch would have bypassed the documented registry audit, registration, and
descriptor prerequisites. The corrected design adds the reusable stack, exact
snapshot and attempt contracts, the complete 24-producer freshness inventory,
`PLA-GAP-004`, provider-terminal `PLA-M2-009` gates, and a credentialed
DigitalOcean canary before authority.

The second challenge returned `RESHAPE` because the snapshot had no exact
private carrier across the writer boundary and its unsalted value digest was
incorrectly described as redacted telemetry. The final contract keeps the
public writer dictionary snapshot-free, carries the snapshot only in an
exact-type linear context that discards it on every exit path, and limits the
value-derived comparison token to keyed process-local equality. Telemetry gets
only bounded reason and count tags. Re-review returned `PURSUE` with no
remaining blocker. This verdict approves design and characterization work. It
does not authorize single-callback promotion before the descriptor and live
provider gates close.
