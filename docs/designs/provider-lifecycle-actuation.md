# Provider and Lifecycle Actuation Architecture

Status: M1 merged; M2 approved for shadow implementation; M3 requires a dedicated review

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

The generic offer and evidence modules are leaf modules. They import only the
standard library and existing leaf JSON typing helpers. `Resources` and `Cloud`
annotations are quoted and imported only under `TYPE_CHECKING`; the generic
modules do not import clouds, optimizer, backend, provisioner, server, or
Kubernetes modules. `Cloud.get_offer_source()` likewise uses a quoted return
annotation and performs no provider SDK, kubeconfig, or plugin work at import
time. Placement types are not re-exported from `sky.__init__` or
`sky.clouds.__init__`.

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
its authoritative promotion commit.

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
decision. M2 authoritative binding is limited to the first provider mutation
attempt of a locked provisioning entry. `RetryingVmProvisioner` carries a
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

`OfferReasonCodeV1` is a closed enum with exactly these V1 values:

- `NONE`;
- `NO_FEASIBLE_SHAPE`;
- `UNSUPPORTED_OPERATION`;
- `UNSUPPORTED_ACTUATION_KIND`;
- `UNSUPPORTED_NODE_COUNT`;
- `UNSUPPORTED_ACCELERATOR`;
- `UNSUPPORTED_RESOURCE_MODE`;
- `UNSUPPORTED_NETWORK_TIER`;
- `VOLUME_OR_STORAGE_MOUNT`;
- `KUEUE_ENABLED`;
- `RESERVATION_REQUESTED`;
- `CUSTOM_PLACEMENT_CONFIG`;
- `UNRESOLVED_SCOPE`;
- `CONTEXT_UNREACHABLE`;
- `SCOPE_CHANGED`;
- `CONFIGURATION_CHANGED`;
- `SHAPE_NO_LONGER_SUPPORTED`;
- `CAPACITY_UNAVAILABLE`;
- `QUOTA_UNAVAILABLE`;
- `OFFER_IDENTITY_CHANGED`;
- `OBSERVATION_LIMIT_EXCEEDED`;
- `PROVIDER_OBJECT_CONFLICT`;
- `SOURCE_ERROR`;
- `RETRY_AFTER_PROVIDER_ATTEMPT`.

`OfferSetResultV1` contains exactly `status`, `offers`, and `reason_code`.
`OfferSetResultV1.status` is exactly one of:

- `OK`, with a nonempty ordered tuple of offers;
- `NO_OFFERS`, when the provider observation proves no feasible offer;
- `NOT_REPRESENTABLE`, when the V1 source cannot faithfully encode a supported
  legacy placement constraint.

`OK` requires `reason_code=NONE`. `NO_OFFERS` requires an empty offer tuple and
`NO_FEASIBLE_SHAPE`. `NOT_REPRESENTABLE` requires an empty offer tuple and one
of the bounded unsupported-input or unresolved-scope codes above.

`OfferRevalidationResultV1` contains exactly `status`, `offer`, and
`reason_code`. Its `status` is exactly one of:

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

A provider-namespaced native offer ID may be used only when the provider
contract declares it stable, nonsecret, and bounded. Otherwise `offer_id` is
`<provider>:sha256:<digest>` over canonical JSON containing only:

- schema version and canonical provider name;
- operation;
- actuation kind;
- opaque scope kind and ID;
- region and ordered candidate-zone batch;
- batching scope;
- normalized per-node resource identity;
- purchase mode;
- provider payload version and its allowlisted `identity` object.

Price, availability, observation time, TTL, revalidation policy, evidence, and
the provider payload `observation` object are excluded from the stable ID.
`observation_id` is the SHA-256 digest of the stable offer ID plus all of those
observation fields. Changing requested node count changes the observation ID
through evidence but does not change the per-placement stable ID.

The parser recomputes every digest it can verify and rejects a mismatch. A V1
provider payload is constructed only from a provider-specific allowlist. Raw
SDK responses, kubeconfigs, credentials, tokens, environment variables, pod
configuration, labels, annotations, or admission payloads are never accepted as
generic payload data. Suspicious secret-like keys are rejected as
defense-in-depth, but key-name filtering is not considered redaction.

The canonical provider payload is limited to 4 KiB and the full canonical
envelope to 16 KiB. V1 has these closed per-field bounds:

| Field | V1 bound |
|---|---|
| `schema_version` | integer exactly `1` |
| `operation` | exactly `fresh_create` for a provisionable or persisted envelope; `plan_create` is process-local and cannot be enveloped |
| `actuation_kind` | exact closed enum; Kubernetes V1 envelopes require `direct_pod` |
| `provider` | 1 to 63 lowercase ASCII letters, digits, `.`, `_`, or `-`, starting and ending with a letter or digit |
| `offer_id` | 1 to 256 ASCII characters and either the declared provider-native grammar or `<provider>:sha256:` plus exactly 64 lowercase hexadecimal characters |
| `observation_id`, scope ID, optional constraint digest, and provider identity digests | `sha256:` plus exactly 64 lowercase hexadecimal characters |
| scope kind and batching scope | 1 to 128 lowercase ASCII letters, digits, or `_`; Kubernetes V1 values are exact enums |
| instance type | 1 to 256 UTF-8 bytes after NFC normalization, with control characters forbidden |
| CPU and memory decimal strings | canonical, nonnegative, non-exponent decimal; at most 38 integral and 18 fractional digits; no sign, leading zero, or trailing fractional zero |
| accelerators | at most 8 entries; normalized name 1 to 128 UTF-8 bytes; count integer 1 through 2,147,483,647 |
| disk and network tier | null or 1 to 64 lowercase ASCII letters, digits, `_`, or `-` |
| region and each zone | 1 to 1,024 UTF-8 bytes after NFC normalization, with control characters forbidden |
| candidate zones | at most 32 unique entries |
| price amount | the same decimal grammar and digit bounds as CPU and memory; nonnegative |
| price basis, currency, purchase mode, availability, revalidation policy, and evidence values | exact closed enums; currency is exactly three uppercase ASCII letters |
| observed time | exactly 20 ASCII bytes in `YYYY-MM-DDTHH:MM:SSZ` form and a valid UTC datetime |
| TTL | integer 1 through 300 seconds; Kubernetes V1 emits exactly 15 |
| requested nodes | integer 1 through 10,000; the Kubernetes authoritative V1 subset requires exactly 1 |
| provider payload version | integer exactly `1` |

Each provider-payload object has at most 32 keys, each key is 1 to 64 printable
ASCII characters, and the combined `identity` and `observation` trees have at
most 64 keys and 128 array elements. Maximum nesting depth is four below either
payload root and each array has at most 32 elements. Payload strings are at most
1,024 UTF-8 bytes after NFC normalization; integers are in the signed 64-bit
range; only strings, integers, booleans, nulls, bounded arrays, and bounded
objects are allowed. JSON floats are forbidden. Empty strings are allowed only
for a provider field whose allowlist explicitly declares them meaningful;
Kubernetes V1 declares none.

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

Kubernetes V1 additionally requires an empty accelerator list, null disk and
network tiers, and an empty candidate-zone list. No sample value implicitly
extends these enums.

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
`skypilot_config.to_dict()`, the active workspace,
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

The classifier hashes only the secret-free normalized allowed values,
active-workspace digest, sorted registry names and queue paths, registration and
template ownership, and the built-in implementation fingerprint into
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
deterministically sorted tuple of normalized node records containing
`status.capacity`, `status.allocatable`, and the exact existing `is_ready()`
boolean; the configuration fingerprint; and the closed eligibility result
above. The endpoint fingerprint hashes only the
normalized API-server scheme, host, port, path, and CA bundle digest from the
loaded client configuration. Userinfo, query strings, client certificates,
keys, tokens, exec-plugin arguments, environment variables, and raw kubeconfig
data are forbidden inputs.

The snapshot contains no kubeconfig, credential, token, full node object, pod
configuration, label, annotation, or admission payload. Both the legacy shadow
adapter and offer source project from this exact value. Node input order is
normalized before either projection, so provider response or future-completion
order cannot affect offer ordering. The comparison-only
legacy adapter preserves the captured legacy context order; the offer source
sorts by normalized context identity. The comparator records order and winner
differences separately. Neither projection replaces or reorders the real legacy
candidate list in shadow mode, which is important because the current
`allowed_contexts: all` path passes through a set.

One observation accepts at most 256 candidate contexts, 10,000 node records per
context, 256 registered Kubernetes-property names, 256 registered queue paths,
and 256 resulting offers. Context, registry-name, and queue-path strings use the
envelope's 1,024-byte region or 128-byte key bound as applicable. The source
checks API collection metadata and the materialized lengths, never truncates,
and returns `NOT_REPRESENTABLE(OBSERVATION_LIMIT_EXCEEDED)` on any overflow.
Provider completion order cannot decide which entries survive because overflow
rejects the whole observation.

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
the raw identity. `REQUIRE_FRESH` reloads that identity instead of reusing a
credential or context cache. Only the scope digest is stored; the raw endpoint,
context identity, namespace, and UID are not stored in the provider payload.

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
name. The offer projection computes the expected value from the snapshot and
pure template inputs. The independently rendered cluster YAML must match it
before `bulk_provision()` is called. Since the complete identity object enters
the stable offer ID, same-name service-account replacement changes stable
identity and cannot pass revalidation.
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

Immediately after every successful pre-lock `Optimizer.optimize()` call, the
internal placement runtime builds a fresh `OptimizationOfferPlanV1` for the
returned DAG and current optimize target using `operation=PLAN_CREATE`.
`task_index` is the task's position in that exact DAG.
`resources_fingerprint` is the canonical normalized placement class and
requested node count, excluding runtime-only image resolution. The runtime
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

`provisioner.bulk_provision()` gains one optional keyword-only
`placement_offer_handoff: PlacementOfferHandoffV1 | None`. `off` passes null.
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
non-null context, `provider_attempt_count=1`, and `reason_code=NONE`; its
provider and revalidation capture IDs must match.

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
call to `bulk_provision()`, `_retry_zones()` requires
`capture.observation.capture_id == capture.actuation_context.capture_id`; it
rejects a missing, reused selection, or cross-provider context before mutation.
`_retry_zones()` passes the revalidated immutable offer and the pinned context
from that capture in the frozen handoff.
`bulk_provision()` re-reads the server gate. Under an authoritative gate it
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
lookup. `get_cluster_info()`, port-forward or exec setup, runtime setup, final
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
Kubernetes and are not accepted as actual-placement evidence. M2 adds this
leaf-type contract and an optional final `ProvisionRecord.placement_evidence`
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

### Shadow and Promotion Gates

The shadow implementation, authoritative promotion, and legacy removal are
separate commits and deployments.

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

Promotion requires all of the following on the exact pushed SHA:

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

The authoritative commit changes only the eligible Kubernetes subset to use the
offer projection and exact selected offer. All other Kubernetes requests retain
the typed legacy fallback, except provider-object conflicts and unresolved
attempt fences, which always fail closed. Rollback changes the server mode to
`shadow` or `off`; an image rollback additionally requires the current-image
preflight to prove every attempt fence is null. No database schema is introduced
by M2.

Kubernetes-specific shadow and fallback code remains for one full compatibility
release after authoritative promotion. Generic placement reconstruction is not
removed based on a Kubernetes-only gate.

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

- lock the exact `OfferSourceV1`, result, identity, envelope, redaction, and
  revalidation contracts in this file and pass a new adversarial review;
- add the optional Cloud offer-source capability without adding it to
  `ProvisionerBundleV1` or creating a second universal provider registry;
- implement recursively immutable offers and built-in-only handle envelopes;
- adapt the initial single-node CPU Kubernetes subset;
- independently shadow-project the old and new placement sets from one raw
  observation snapshot while legacy `Resources` remains mutation owner;
- carry an exactly matched selected offer only through the first provider
  mutation attempt, then use a typed legacy retry until M4 can prove complete
  cleanup and atomically reset the cluster record;
- revalidate immediately before mutation and compare the selected offer with the
  actual provider result;
- persist the optional envelope only on a successful READY handle;
- pass the frozen corpus, bounded Datadog observation, stale-offer,
  minimum-compatible-client, and rollback-image gates;
- promote the eligible Kubernetes subset in a separate commit and retain all
  other Kubernetes requests on the typed legacy fallback.

Deployment proves candidate safety, optimizer winner, selected placement,
pre-mutation revalidation, actual provisioning result, handle compatibility,
rollback, and cleanup on the exact image digest.

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
| Kubernetes use of `make_launchables_for_valid_region_zones()` and backend `_yield_zones()` for the declared eligible subset | the eligible Kubernetes subset is authoritative through `PlacementOfferV1` and its rollback window is closed | frozen corpus and bounded live window have zero unexplained safety, placement-set, optimizer-winner, or actual-result mismatches; minimum-client and rollback-image qualification pass |
| Kubernetes placement-offer shadow dual projection | Kubernetes authoritative mode has remained healthy for one full compatibility release | Datadog records no unexplained mismatch or rollback, and repository tests retain a frozen legacy-versus-offer characterization corpus |
| Kubernetes `NOT_REPRESENTABLE` legacy fallback | every officially supported Kubernetes placement-affecting input has a typed, characterized offer representation | one compatibility release records zero fallback for supported inputs, the full Kubernetes corpus passes, and repository search finds no eligible legacy call |
| M2 first-provider-attempt-only authoritative fence and `RETRY_AFTER_PROVIDER_ATTEMPT` fallback | M4 carries typed complete cleanup and provider-absence evidence across every failover provider and resets the cluster record atomically | cross-provider lost-response, partial-create, teardown, absence, and stale-record corpus passes with no blind replay |
| M2 handle-backed `placement_attempt_fence`, reconciler, and `QUARANTINE_FENCED` path | M4 stores every cluster attempt and UID inventory in the durable action runtime and the pre-M2 rollback window is closed | crash and UID-replacement tests prove foreign objects survive, every owned child reaches proved absence, no generic label/name delete is reachable, and repository search finds no handle-backed fence writer |
| provider-agnostic region and zone reconstruction in `resources_utils.py` and backend launch loops | every supported provider is authoritative through a placement-offer source or is explicitly frozen behind a declared legacy adapter | provider-wide corpus and bounded observation gates pass, repository and plugin inventory find zero migrated callers, and old/new client-server compatibility passes |
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
