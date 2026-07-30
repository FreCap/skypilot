# Provider and Lifecycle Actuation Architecture

Status: approved for M1 and M2 implementation; M3 requires a dedicated review

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

The useful dstack concepts are explicit provider capabilities, immutable
placement offers, nonblocking provisioning observations, and one reusable
leased-work scheduler. SkyPilot will not copy dstack's destructive-operation
semantics. A lost lease or elapsed deadline must never convert an ambiguous
provider mutation into success.

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

## Goals

- Give every provider capability one explicit, typed owner.
- Prohibit accidental mixing of plugin and built-in lifecycle methods.
- Carry one immutable placement decision from discovery through provisioning.
- Separate provider mutation from waiting and status projection.
- Reuse due-work, lease, heartbeat, retry, and fenced-commit mechanics.
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
| Due selection, claim, lease, heartbeat, attempt, backoff, deadline | Durable action runtime |
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
in M1. The future nonblocking `start_or_reconcile()` and `observe()` contract
is a separately versioned actuator facet introduced only when cluster
actuation migrates.

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

`PlacementOffer` is an immutable internal object produced by `OfferSource`,
ranked by the optimizer, and consumed first by the existing launch path and
later by the versioned nonblocking actuator facet.

It contains:

- stable offer ID, observation ID, and schema version;
- normalized resources;
- provider and account or cluster scope;
- region, candidate zones, and batching scope;
- price, price basis, and currency;
- spot or on-demand mode;
- availability classification;
- observation time and TTL;
- reservation, quota, and capacity evidence;
- a versioned, size-bounded, redacted provider payload.

Availability is advisory. Provisioning revalidates expired or provider-required
evidence immediately before mutation. An unavailable revalidation produces a
typed outcome rather than silently selecting a different placement.

The stable offer ID is a provider-namespaced native offer ID when one exists.
Otherwise it is a SHA-256 digest of canonical JSON containing only placement
identity: schema version, provider, account or cluster scope, region, zone
batch, instance type or equivalent resource identity, purchase mode, and the
versioned redacted provider identity payload. Price, availability,
`observed_at`, and TTL are excluded from the stable ID. The observation ID
hashes the stable ID plus those time-varying observation fields.

During M2, existing `Resources` remains the public and serialized
representation. The selected offer is held in an optional internal
`RetryingVmProvisioner.ToProvisionConfig.placement_offer` field through
failover. After a successful launch, its redacted versioned envelope is copied
to an optional `CloudVmRayResourceHandle.placement_offer` attribute beside
`launched_resources`. It is not added to the public `Resources` schema or wire
API. Older readers continue to ignore the unknown pickled-handle attribute;
old/new handle compatibility tests prove this before promotion. A later
durable action migration stores the same envelope in the action row rather
than relying on the handle.

A shadow comparator records whether the old region and zone reconstruction
selects the same safety and placement class. Mutation stays on the old path
until a frozen characterization corpus and a bounded test-cluster observation
window have zero unexplained safety or placement-class mismatches. Transient
availability or price disagreement is classified and retained, not treated as
an impossible literal-zero requirement.

## Provisioning Attempt and Provider Outcome

Provider mutation becomes nonblocking from the orchestration perspective.
`start_or_reconcile()` returns a durable `ProvisioningAttempt`; `observe()`
returns a typed snapshot; `stop_or_terminate()` returns complete or in
progress.

An attempt contains:

- stable attempt ID and idempotency key;
- resource type, resource ID, incarnation, and desired generation;
- selected offer ID and provider payload version;
- requested, existing, resumed, and created counts and IDs;
- provider request or operation IDs;
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

- selecting due work with database time;
- claiming with `FOR UPDATE SKIP LOCKED`;
- lease token, owner, expiry, heartbeat, and attempt count;
- synchronous ownership assertion immediately before provider mutation;
- intent, idempotency key, phase, operation ID, and readback evidence;
- next-attempt time and bounded jittered backoff;
- token-guarded completion;
- deterministic transition ID;
- state transition and lifecycle-event or outbox commit.

The runtime does not own:

- legal domain transitions;
- desired-state policy;
- resource incarnation construction;
- provider observation mapping to domain status;
- compensation selection;
- exact deletion proof.

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

### Store boundary

The runtime is a mechanics library with a PostgreSQL implementation and a
caller-supplied domain adapter. It does not add lease columns to every domain
row.

M3 is not approved by this revision. Before any M3 schema or worker code is
written, this exact file must define and receive a second adversarial approval
for:

- action versus attempt table identity, keys, generations, phases, and
  retention;
- the one component that owns the caller-supplied SQLAlchemy
  `Session` or `Connection`;
- migration lineage and downgrade compatibility;
- the generic deterministic event key and uniqueness constraint;
- activation epoch and minimum-compatible-reader fields;
- a compatibility reconciler that is shipped and tested before the new writer
  can be enabled;
- backfill and coexistence with `api_controller_action_reservations`;
- rollback-on-event-failure proof;
- draining, reconciling, or quarantining every live action before image
  rollback.

The deployed server resolves global state and API request engines to the same
PostgreSQL URI but intentionally uses distinct process-local pools. Atomicity
is therefore permitted only when one caller-owned connection writes the action
row, volume row, and generalized event tables. Opening nested sessions or
performing cross-connection best-effort writes is forbidden. If one connection
cannot own all three writes, the updated design must specify a durable outbox
and idempotent reducer before M3 can be approved.

The volume pilot is central-PostgreSQL-only, disabled by default behind a
server-side gate. The synchronous public `volumes apply` and `volumes delete`
contract waits for the durable action to reach its terminal result. The old
FileLock and synchronous path remain for officially supported local or
controller SQLite operation until a separate product deprecation gate closes.

Managed container images remain the semantic reference. They move onto the
shared interface only after equivalence tests prove no loss of provider-call
fencing, quarantine, or exact cleanup.

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

- add immutable offer and serialization contract;
- adapt one new-provisioner cloud;
- shadow-compare old and new placement selection;
- persist the optional redacted envelope through the internal launch config and
  successful cluster handle;
- promote only after the bounded explained-mismatch and stale-offer gates pass.

Deployment proves the selected placement and provisioning result match the old
path.

### M3: Durable action runtime and volume pilot

- update this file with the complete M3 transaction, schema, activation, and
  rollback contract and pass a second adversarial review before implementation;
- add the PostgreSQL action store and worker;
- add volume desired generation, incarnation, tombstone, observation, and
  deletion proof;
- remove force-purge success on ambiguous provider errors;
- emit the volume lifecycle transition atomically.

The new writer is disabled by default. The compatibility reconciler must be
deployed and exercised before the writer can be enabled. The pilot covers only
central PostgreSQL; the supported local or controller SQLite path remains
legacy until its separate deprecation gate.

Deployment exercises create, refresh, delete, lost worker, stale lease,
ambiguous provider response, readback, and cleanup.

### M4: Cluster provisioning and teardown

- adapt launch, start, stop, down, port reconciliation, and status observation;
- preserve cluster hash or successor incarnation fencing;
- shadow-compare status projection;
- route failover through typed provider outcomes.

### M5: Serve and pools

- extract pure planners and reducers;
- persist replica launch and down attempts;
- keep lifecycle epoch, immutable versions, and incarnation inventory;
- make the jobs and Serve pool handoff an explicit fenced contract.

### M6: Managed jobs

- migrate recovery and cleanup actions last;
- retain controller generation and admission fencing;
- remove process-local retry ownership only after recovery equivalence tests.

### M7: Compatibility removal

- verify every removal gate below;
- delete legacy dispatch, version switches, reconstruction, and duplicated
  lifecycle mechanics;
- run full provider conformance, API compatibility, rollback, and live
  cleanup qualification.

## Removal Ledger

Removal is part of completion, not optional follow-up.

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
| region and zone reconstruction in `resources_utils.py` and backend launch loops | `PlacementOffer` is authoritative | the frozen corpus and bounded observation window have zero unexplained safety or placement-class mismatches; classified transient availability or price differences may remain |
| blocking provider wait ownership inside `run_instances()` implementations | `ProvisioningAttempt.observe()` owns progress | provider conformance proves pending, success, timeout, and partial-create behavior |
| generic retry, cache update, and failure classification in `RetryingVmProvisioner` | typed attempts and domain retry policy are authoritative | old and new failover traces agree on the characterization corpus |
| central-PostgreSQL volume mutation `FileLock`, synchronous provider calls, and refresh daemon ownership in `sky/volumes/server/core.py` | M3 action worker owns central volume lifecycle | HA stale-worker, readback, and cleanup tests pass and the server gate is promoted |
| local or controller SQLite volume mutation path and `FileLock` | the product separately deprecates that officially supported path | deprecation window and local compatibility inventory are complete |
| volume `--purge` row deletion after provider error | durable cleanup incident is deployed | ambiguous-delete test retains provider identity and eventually proves absence |
| cluster process-local provisioning and teardown retry loops | M4 action runtime owns them | crash-at-every-phase tests and test-cluster cleanup pass |
| Serve in-memory replica request retry ownership and duplicate scheduling loops | M5 action runtime owns mechanics | lifecycle-epoch, same-name recreation, rollout, scale, and failed-cleanup tests pass |
| managed-job process-local recovery and cleanup retry ownership | M6 action runtime owns mechanics | controller handoff, preemption, cancellation, and cleanup conformance passes |
| duplicate managed-image lease and scheduling mechanics | shared interface proves semantic equivalence | image fencing, quarantine, exact absence, and canary qualification remain green |
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
- stable create adoption or idempotency;
- typed capacity, quota, authentication, authorization, throttling, and
  invalid-request evidence;
- pending operations remain nonterminal;
- absent-resource termination is idempotent;
- ambiguous outcomes enter readback;
- provider request and operation IDs are preserved;
- terminal deletion requires complete absence proof.

### Action runtime

- two workers cannot own one live lease;
- lease expiry permits a new owner but fences the stale owner;
- heartbeat uses database time and matching token;
- the pre-provider-call fence rejects stale work;
- retry timing is bounded and jittered;
- provider result loss enters readback;
- transition and event commit or roll back together;
- deterministic transition IDs prevent duplicate events;
- desired generation changes supersede old work without losing cleanup.

### Domain qualification

- volumes: create, register, refresh, attach conflict, delete, purge, provider
  timeout, lost response, and eventual consistency;
- clusters: create, resume, partial create, stop, terminate, ports, failover,
  same-name recreation, and provider absence;
- Serve and pools: lifecycle epoch, replica inventory, rolling update,
  autoscaling, pool handoff, failed cleanup, and service recreation;
- managed jobs: admission generation, preemption recovery, cancellation,
  controller handoff, and cleanup.

### Compatibility

- old client with new server;
- new client with old server where the API version permits;
- serialized handles without new metadata;
- legacy plugin registration during the compatibility window;
- rollback to the previous image before irreversible schema activation.

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

- M1 and M2 are code-only additive changes and roll back by image.
- New schema is expand-first. Old readers ignore new tables and columns.
- Mutation ownership switches behind a server-side gate and can return to the
  old writer only before the new writer performs an irreversible action.
- The compatibility reconciler is deployed and proven before the writer gate
  can create an action. Once an action enters `IN_FLIGHT`, rollback preserves
  the action store and keeps that reconciler active until every action is
  completed or quarantined.
- The activation epoch and minimum-compatible-reader fence prevent an older
  image from becoming mutation owner while newer live actions exist.
- Schema contraction occurs only in M7 after the rollback window closes.
- A rollback never deletes action, event, provider identity, or cleanup
  evidence.

## Completion Criteria

This migration is complete only when:

- M1 through M7 are merged with full CI on each exact pushed SHA;
- every stacked commit has a recorded successful `skypilot-ha` deployment and
  milestone-specific live proof;
- provider conformance covers every strict built-in facet;
- all domains use the shared mechanics without losing their domain fences;
- every removal-ledger row is deleted or has an explicit externally owned
  blocker and is not falsely reported complete;
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
