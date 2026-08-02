# Provider and Lifecycle Actuation Architecture

Status: M1, M2 S1, M2 S2a.1, the S2a.2 deterministic-gzip prerequisite and
source composer, M3-S0, M3-S1, and M3-S2 are merged. The M4 typed resolved
provider operation foundation and DigitalOcean authoritative query projection
extraction are also merged and deployed. M3-S0 passed exact-head CI,
exact-parent merge verification, staged revisions 49 through 51, and bounded
monitoring. The test deployment had no managed volume, so positive live
per-volume parity remains explicitly unproven and the shadow remains
diagnostic-only. M3-S2 has an exact locally verified candidate image, but its
test-cluster deployment remains pending. The M3 action graph, action runtime,
authoritative volume writer, and remaining M4 implementation still require
their dedicated exact-design adversarial reviews and activation gates.

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
audited legacy axis. It has no dispatch authority and claims no historical
replacement generation.

The first V1 implementation is deliberately narrower than the final
descriptor. It joins canonical names and aliases, the Cloud planning object,
the strict provisioner bundle, the legacy provisioner registration, the
built-in provisioner inventory, lifecycle version switches, the seven-method
`InstanceLifecycleV1` contract, and template-override ownership. It records
only the static implementation identities of exact Cloud methods
`get_offer_source()` and `_unsupported_features_for_resources()`; it never
invokes them. Volume, port, command-runner, cleanup, diagnostics,
runtime-configuration, and positive capability facets remain outside this
snapshot schema until their exact method sets and facade-default semantics
receive dedicated design review. Method-presence inference is not allowed to
invent those contracts.

The current registries do not retain configured plugin provenance. V1 can
truthfully classify an exact match to the pre-plugin built-in audit baseline,
an entry observed on the strict registry axis, an entry observed on the legacy
registry axis, or an external or replaced implementation. It cannot prove
which API or plugin inserted a raw dictionary entry. Module and qualname
observations are not causal provenance. Configured plugin class path and stable
artifact provenance begin only with `ProviderRegistrationV1`.

The service catalog remains an external planning facet in V1. Moving catalog
registration into the descriptor would combine two migrations and is deferred
until lifecycle ownership is stable.

The audit explicitly classifies expected partial providers and proves
one-to-one identity only where two old registries are both expected to own a
facet. An unexpected partial entry is a conformance failure, not an import
failure.

#### Exact `ProviderRegistryAuditSnapshotV1` contract

This contract is pinned to SkyPilot
`b65d4e0cd233e22750f344eef2e6ae5750250482` and dstack
`ccef71f46b8e61ce3c139d3c147911b6dd19f8a2`.

dstack's useful boundary is the typed chain from `Configurator.TYPE` through
`Configurator.BACKEND_CLASS` to `Backend.COMPUTE_CLASS`. Capability mixins can
be inspected without initializing a credentialed backend. SkyPilot adopts the
same separation between declaration and request-scoped execution. It does not
copy dstack's repeated import list, `BackendType` inventory, derived map and
list, or import-time feature caches. In the pinned dstack revision, dynamic
configurator registration updates the lookup map but not the separately cached
available-type and feature lists. The SkyPilot end state is therefore one
validated descriptor registration with derived compatibility projections, not
another independently mutable list.

The current SkyPilot state has five raw axes that the audit must preserve
independently:

1. `CLOUD_REGISTRY` canonical keys and its separate alias map;
2. strict `ProvisionerBundleV1` registrations;
3. legacy `Provisioner` registrations;
4. the late-bound built-in provisioner getter inventory;
5. the provisioner compatibility normalization
   `lambda_cloud -> lambda`.

The clean built-in baseline is exactly 25 Cloud entries and 24 built-in
provisioner entries. `ibm` is the only Cloud-only built-in and is expected
because it declares `RAY_AUTOSCALER` provisioning and `CLOUD_CLI` status. The
other 24 overlap and declare SkyPilot provisioning and status. Cloud aliases
are exactly `digitalocean -> do` and `k8s -> kubernetes`; the provisioner-only
compatibility alias is exactly `lambda_cloud -> lambda`. These alias surfaces
remain evidence and do not expand either current dispatcher.

The audit-only built-in expectation table is captured once from exact object
references immediately after the built-in Cloud import set and built-in
provisioner getter inventory are complete, before server plugin loading. It
retains the original Cloud singleton and exact type, the original provisioner
module held by each getter's direct-global binding, the binding name, the
getter's exact Python function type, code object, defaults, keyword defaults,
closure, and globals mapping, and the built-in aliases. Every current built-in
getter has no parameters, defaults, keyword defaults, or closure. It is a
read-only conformance oracle, not a dispatch registry, and it is removed with
the audit migration machinery. Capturing this baseline later from already
plugin-mutated registries is prohibited.

The published snapshot and later token-free report define only recursively
frozen values. Every collection is a tuple, and neither value retains a live
callable, module, Cloud object, bundle, descriptor, or mutable mapping. The
audit-only pre-plugin expectation table is the sole long-lived internal holder
of the exact live baseline references named above. One in-progress capture may
also retain a private strong-reference anchor tuple for identity-bearing
objects observed in its first phase. The projector never inspects that tuple,
and it is discarded after final revalidation rather than published. The closed
supporting enums are:

- `AuditPresenceV1`: `ABSENT`, `PRESENT`;
- `AuditRuntimeIdentityKindV1`: `MODULE`, `CLASS`, `INSTANCE`,
  `PYTHON_FUNCTION`, `BUILTIN_FUNCTION`, `BOUND_METHOD`, `CALLABLE_OBJECT`,
  `DESCRIPTOR`, `VALUE`;
- `AuditRawNameKindV1`: `VALID_STRING`, `INVALID_STRING`, `NON_STRING`;
- `RegistrationKindV1`: `CLOUD`, `BUILTIN_PROVISIONER`,
  `STRICT_PROVISIONER`, `LEGACY_PROVISIONER`;
- `RegistrationSourceObservationV1`: `BUILTIN_BASELINE_MATCH`,
  `STRICT_REGISTRY_OBSERVED`, `LEGACY_REGISTRY_OBSERVED`,
  `EXTERNAL_OR_REPLACED`;
- `AliasSourceV1`: `CLOUD_REGISTRY`, `PROVISIONER_COMPATIBILITY`;
- `LifecycleSwitchStateV1`: `ABSENT`, `VALID`, `MALFORMED`;
- `LifecycleMemberStateV1`: `ABSENT`, `CALLABLE`, `NON_CALLABLE`,
  `UNSAFE_DESCRIPTOR`;
- `LifecycleOwnerV1`: `STRICT`, `LEGACY`, `BUILTIN`, `FACADE_DEFAULT`,
  `ABSENT`, `INDETERMINATE`;
- `LifecycleCompletenessV1`: `EMPTY`, `PARTIAL`, `COMPLETE`,
  `INDETERMINATE`;
- `TemplateOwnerV1`: `STRICT`, `LEGACY`, `BUILTIN`, `ABSENT`,
  `INDETERMINATE`;
- `ProviderAuditContextV1`: `MAIN`, `UVICORN`, `EXECUTOR`, `CONTROLLER`;
- `ProviderRegistryIssueSeverityV1`: `WARNING`, `ERROR`;
- `ProviderRegistryFacetV1`: `REGISTRY_KEY`, `ALIAS`, `CLOUD`,
  `LIFECYCLE_SWITCH`, `INSTANCE_LIFECYCLE`, `TEMPLATE_OVERRIDE`,
  `OFFER_DECLARATION`, `RESOURCE_SUPPORT_PREDICATE`.

`AuditRuntimeIdentityV1` contains one identity kind, module and qualname strings
only when each is an exact `str` of at most 256 code points, and a keyed
process-local comparison token. Oversized or non-string metadata is omitted.
Bound methods are normalized over their function and bound owner so repeated
attribute access compares equal. The token is valid only inside the producing
process and is exactly 32 lowercase hexadecimal characters produced by a
keyed process-local hash. It is never serialized, logged, persisted, or
emitted to telemetry and is not an implementation digest.
The key is created lazily and replaced before use whenever the current process
ID differs from the process that created it, so a forked child cannot reproduce
its parent's tokens for inherited objects.
First-phase identity-bearing objects remain strongly anchored until final
signature comparison. This prevents object-address reuse from giving a
different second-phase object the same token after direct removal and
reinsertion.

`AuditRawNameV1` contains a raw-name kind, exact text only when the value is an
exact `str` of at most 128 code points, normalized lowercase text only for that
case, and an `AuditRuntimeIdentityV1` for malformed evidence. A valid canonical
name is nonempty, already lowercase, has no leading or trailing whitespace,
and is at most 128 code points. `INVALID_STRING` and `NON_STRING` values never
become provider-entry keys. Invalid raw registration keys remain in the
snapshot's sorted `UnkeyedRegistrationAuditV1` tuple and emit
`MALFORMED_PROVIDER_KEY`. Invalid alias names or targets remain as bounded
`AuditRawNameV1` values in `AliasAuditV1` and emit `MALFORMED_ALIAS`; an invalid
target contributes no entry key. No `repr()` is retained.

An exact bounded nonempty string that differs from its lowercase trimmed form
uses `INVALID_STRING` and `UNREACHABLE_PROVIDER_KEY`. An empty, oversized, or
non-string registration key uses `MALFORMED_PROVIDER_KEY`. Any invalid alias
name or target uses `MALFORMED_ALIAS`; the more specific collision or dangling
code is added only when both participating names are valid strings.

`RegistrationAuditV1` contains presence, registration kind, observable source,
runtime identity, and optional template-hook identity. `AliasAuditV1` contains
two `AuditRawNameV1` values and an alias source. It never coalesces the Cloud
and provisioner alias surfaces. `UnkeyedRegistrationAuditV1` contains one raw
source axis, malformed raw name, and registration runtime identity, with no
live value. `LifecycleSwitchAuditV1` contains a switch state and one exact enum
value only when it is a member of the expected `ProvisionerVersion`,
`StatusVersion`, or `OpenPortsVersion` type.

`LifecycleMethodAuditV1` contains the method name, strict, legacy, and built-in
member state and identity, whether the facade body is a meaningful default,
and one effective-owner value. `InstanceLifecycleAuditV1` contains exactly the
seven methods in the declared `INSTANCE_LIFECYCLE_V1_METHODS` order, the sorted
candidate-owner tuple, one completeness value for each candidate, and whether
legacy fallback mixes owners. `TemplateOwnershipAuditV1` contains strict,
legacy, and built-in hook member state and identity plus the effective owner.
A non-`None`, non-callable hook remains the projected current owner and emits
`NONCALLABLE_TEMPLATE_OVERRIDE`.

`ProviderRegistryAuditEntryV1` contains one valid canonical name, its aliases,
Cloud, strict, legacy, and built-in registration observations, the three
lifecycle switches, instance lifecycle, template ownership, and static
implementation identities for exactly `get_offer_source` and
`_unsupported_features_for_resources`. It also contains one partial
classification and its sorted issues. `ProviderRegistryAuditSnapshotV1`
contains schema version 1, one capture context, entries, aliases, unkeyed
registrations, aggregate issues, and `is_conformant`, which is true only when
no `ERROR` exists. It contains no wall-clock time, process ID, historical
generation, dispatch method, or barrier receipt.

Both audited Cloud implementation members must resolve statically as callable.
An absent, non-callable, unsafe descriptor, or custom-resolution result emits
`UNSAFE_OFFER_DECLARATION` or `UNSAFE_RESOURCE_SUPPORT_PREDICATE` for its exact
facet; the optional identity remains bounded evidence rather than proof of
usability.

`BUILTIN_BASELINE_MATCH` requires exact current object identity and exact type
identity for a Cloud singleton, or exact current module identity for a
provisioner, against the pre-plugin built-in expectation table. It never uses
`isinstance()`, `Cloud.is_same_cloud()`, module text, or qualname text alone.
An expected built-in Cloud that is absent emits
`CLOUD_BUILTIN_IDENTITY_MISMATCH`; expectation keys therefore remain in the
entry union even when the corresponding live Cloud entry was deleted.
Strict and legacy source values describe only the raw registry axis. They do
not claim which API or plugin inserted the entry.

Each expected built-in Cloud alias whose current exact raw alias key is absent
or whose target differs emits exactly one `CLOUD_BUILTIN_ALIAS_MISMATCH`.
Its `canonical_name` is the expected target. Its subject identity is the
observed raw target identity when the exact alias key is present, and `None`
when the alias key is absent. The raw alias observation and any other alias
issues remain independently represented.

The effective lifecycle projection mirrors current routing without returning
a callable: strict owns all seven lifecycle methods; otherwise a statically
observed non-`None` legacy member wins; otherwise the raw built-in member wins;
otherwise the facade default applies. A non-callable winning legacy member
remains the projected owner and is an error because current dispatch would
fail only when invoked. A dynamic member that cannot be inspected without
executing plugin code is `INDETERMINATE` and an error.
If a winning strict bundle, legacy wrapper, or required owner field is not the
exact expected container shape under static inspection, the affected owner is
`INDETERMINATE`; precedence never fabricates a usable member from malformed
state.
Projection-to-resolver characterization is mandatory, but the resolver remains
the sole dispatch owner. Template projection is separately exact: strict wins
even when its hook is absent, then a non-`None` legacy hook, then the built-in
hook, then absence.

Completeness in V1 means static presence and callability only. The audit never
asks an arbitrary object for `__signature__`. Because the strict registry does
not retain proof that its public validator was the insertion path, every strict
raw entry is `STRICT_SIGNATURE_UNVERIFIED` and ineligible for descriptor
promotion even when its seven members are statically complete. The later
coordinator supplies verifiable contract evidence. All seven current facade
bodies raise `NotImplementedError`, so their meaningful-default value is false
in the pinned baseline.

Static member lookup invokes only the built-in `type.__dict__`, `type.__mro__`,
and exact instance-`__dict__` descriptors. It walks their detached items and
accepts only exact `str` keys before string comparison. It never performs a
hashed lookup or equality comparison against a plugin-controlled dictionary or
mapping-proxy key; any non-exact-string namespace key makes that lookup unsafe.
Type and MRO membership comparisons use object identity
only, and class `__module__` and `__qualname__` metadata come from the same
static namespace scan. This avoids invoking custom metaclass equality,
descriptors, or attribute lookup while retaining the actual runtime resolution
order. Exact `staticmethod` and `classmethod` wrappers are unwrapped through
`__func__`. Python functions, built-in functions, bound methods, and objects
whose exact type supplies a callable `__call__` slot are `CALLABLE` without
invoking them. `property` and every other object with a custom `__get__`
descriptor are `UNSAFE_DESCRIPTOR`; the audit does not execute them. Module or
class `__getattr__` fallback is never executed. An owner whose exact type, or
whose metaclass when the owner is a class, replaces the corresponding default
`__getattribute__` is also unsafe even when a same-named static member exists,
because current dispatch can replace that member dynamically. Custom
resolution on a strict or legacy registration container makes its required
fields malformed and the owner indeterminate. A data or custom descriptor that
wins one of those container fields has the same result and is never invoked. A
missing Cloud lifecycle switch with dynamic `__getattr__` fallback is
`MALFORMED`. Lifecycle and hook members
become `UNSAFE_DESCRIPTOR`; any other custom Cloud lifecycle-switch resolution
also becomes `MALFORMED`. The static `get_offer_source` and
`_unsupported_features_for_resources` observations likewise become unsafe and
emit their facet-specific issue when Cloud attribute resolution is custom. The
only Cloud members inspected are the three version switches,
`get_offer_source`, and `_unsupported_features_for_resources`.

The entry-key union is the valid canonical strings from the Cloud, strict,
legacy, and built-in maps, the built-in Cloud expectation keys, plus valid
Cloud-alias targets. The fixed `lambda_cloud -> lambda` compatibility alias
contributes its target but never rewrites a raw key. Provider entries sort by
canonical name. Raw-name evidence sorts by `(kind.value, normalized_text or
'', text or '', process_token or '')`. Aliases sort by `(source.value,
alias_raw_name_sort_key, target_raw_name_sort_key)`. Candidate owners use fixed
order `STRICT`, `LEGACY`, `BUILTIN`, `FACADE_DEFAULT`, filtered to present
candidates. Methods retain the declared seven-method order. Issues sort by
`(severity.value, code.value, canonical_name or '', facet.value,
subject_token or '')`.

Alias anomaly checks are source-specific. A Cloud alias target is resolved
only against the Cloud canonical axis, and alias-to-alias checks use only Cloud
alias names. The provisioner compatibility target is resolved only against the
strict, legacy, and built-in provisioner canonical axes. Neither source can
make the other source's missing target valid. A Cloud alias name colliding with
a Cloud canonical key emits `ALIAS_CANONICAL_COLLISION`; an alias name from
either source colliding with a distinct provisioner canonical key emits
`ALIAS_PROVISIONER_CANONICAL_CONFLICT`. `EXCLUDED_ALIAS` applies only to a raw
Cloud alias named `local`.

The closed partial classifications are `NONE`,
`IBM_LEGACY_RAY_CLOUD_ONLY`, `UNDECLARED_STRICT_PROVISIONER_ONLY`,
`UNDECLARED_LEGACY_PROVISIONER_ONLY`, `UNEXPECTED_CLOUD_ONLY`, and
`UNEXPECTED_BUILTIN_PROVISIONER_ONLY`. Only `NONE` and the exact IBM case are
conformant. A provisioner-only entry remains structurally supported by legacy
dispatch, but V1 cannot distinguish an intentional plugin from a typo because
the old registries retain no declaration. It is therefore an error until the
later `ProviderRegistrationV1` explicitly declares the partial contract.

The issue-code set and severities are exact:

| Code | Severity | Facet |
|---|---|---|
| `MALFORMED_PROVIDER_KEY`, `UNREACHABLE_PROVIDER_KEY` | `ERROR` | `REGISTRY_KEY` |
| `MALFORMED_ALIAS`, `CLOUD_BUILTIN_ALIAS_MISMATCH`, `ALIAS_CANONICAL_COLLISION`, `DANGLING_ALIAS`, `ALIAS_TO_ALIAS`, `EXCLUDED_ALIAS`, `ALIAS_PROVISIONER_CANONICAL_CONFLICT` | `ERROR` | `ALIAS` |
| `WRONG_CLOUD_FACET_TYPE`, `CLOUD_BUILTIN_IDENTITY_MISMATCH`, `UNEXPECTED_CLOUD_ONLY` | `ERROR` | `CLOUD` |
| `MALFORMED_LIFECYCLE_SWITCH` | `ERROR` | `LIFECYCLE_SWITCH` |
| `PROVISIONER_BUILTIN_IDENTITY_MISMATCH`, `UNDECLARED_STRICT_PROVISIONER_ONLY`, `UNDECLARED_LEGACY_PROVISIONER_ONLY`, `UNEXPECTED_BUILTIN_PROVISIONER_ONLY`, `SKYPILOT_CLOUD_WITHOUT_LIFECYCLE` | `ERROR` | `INSTANCE_LIFECYCLE` |
| `STRICT_AND_LEGACY_PRESENT`, `MALFORMED_STRICT_REGISTRATION`, `MALFORMED_LEGACY_REGISTRATION`, `INCOMPLETE_STRICT_LIFECYCLE`, `INCOMPLETE_BUILTIN_LIFECYCLE`, `STRICT_SIGNATURE_UNVERIFIED`, `NONCALLABLE_LEGACY_MEMBER`, `UNSAFE_DYNAMIC_MEMBER`, `MIXED_INSTANCE_LIFECYCLE_OWNER` | `ERROR` | `INSTANCE_LIFECYCLE` |
| `REPLACED_BUILTIN_GETTER` | `ERROR` | `INSTANCE_LIFECYCLE` |
| `NONCALLABLE_TEMPLATE_OVERRIDE`, `TEMPLATE_OWNER_INDETERMINATE` | `ERROR` | `TEMPLATE_OVERRIDE` |
| `UNSAFE_OFFER_DECLARATION` | `ERROR` | `OFFER_DECLARATION` |
| `UNSAFE_RESOURCE_SUPPORT_PREDICATE` | `ERROR` | `RESOURCE_SUPPORT_PREDICATE` |
| `PARALLEL_LIFECYCLE_OWNER` | `WARNING` | `INSTANCE_LIFECYCLE` |

`ProviderRegistryAuditIssueV1` contains only one listed code, its fixed
severity, canonical name when valid, facet, and an optional subject runtime
identity. It has no free-form detail or provider exception text. Malformed or
unexpected entries do not abort unrelated construction. Whole-capture failure
instead raises `ProviderRegistryAuditCaptureErrorV1` with exactly one of
`MISSING_RECEIPT`, `INVALID_RECEIPT`, `WRONG_PROCESS`, `STALE_EPOCH`,
`ACTIVE_SESSION`, `REGISTRY_CHANGED`, or `OBSERVED_MEMBER_CHANGED`. No partial
snapshot accompanies that exception.

##### Registration barrier and capture algorithm

The existing `plugins_loaded()` boolean is not the barrier. It is a schema
leniency flag, can remain true across a later failed context load, and does not
cover import-time decorators. V1 adds a process-local registration-session
coordinator in a lightweight utility module shared by the plugin loader, Cloud
registry, and provisioner facade. It does not change the boolean's current
schema semantics: a successful load still sets it true, and a later failed load
does not temporarily weaken concurrent schema validation. The independent
receipt is invalidated even when that legacy boolean remains true.

`load_plugins()` takes a process-local reentrant load mutex and begins a
registration session before reading or importing configured plugin classes.
Beginning a session invalidates every older receipt in that process. The
active-session marker spans module import, context filtering, construction,
and `install()`, but the registry mutation lock is held only while beginning or
completing the session and around each individual registration. This lets a
plugin complete synchronous registration on a child thread without deadlocking
the loader. Recursive `load_plugins()` in the same process fails explicitly on
the active-session check. Plugin registration that outlives `install()` is
unsupported; a later supported mutation invalidates the receipt.

Only successful completion of the entire load pass returns an exact
`ProviderRegistrationBarrierV1` receipt containing the plugin context,
process-local load epoch, producing process ID, and opaque process-local nonce.
An empty plugin configuration still completes a receipt. Any exception aborts
the session and leaves no current receipt. The nonce is an accidental-staleness
capability, not a security boundary against code already executing in the same
Python process. MAIN, UVICORN, EXECUTOR, and CONTROLLER receipts are
process-local; starting a later context in the same process invalidates the
earlier receipt.

The supported Cloud decorator path and both provisioner registration APIs use
the same reentrant mutation lock. A supported registration outside an active
plugin-load session invalidates the current receipt before mutation. The
strict-to-legacy and legacy-to-strict `pop` plus assignment become one locked
mutation for audit consistency while preserving last-registration-wins
dispatch. This lock has no dispatch role and does not make registry entries
authoritative.

On platforms with `register_at_fork`, the child hook replaces both inherited
locks and resets the active session, receipt, epoch, and coordinator process ID
before child code can acquire either lock. The lazy process-ID check remains a
secondary reset for process starts that do not inherit the module state. A
child therefore cannot deadlock on a lock held by a vanished parent thread or
reuse an inherited receipt.

Inherited direct `dict` mutation, direct writes to the private alias map, and
in-place module or class member replacement are legacy escape hatches with no
provenance. They are not reclassified as supported registration. Capture uses
this two-phase linearization:

1. Under the coordinator mutation lock, validate the exact latest receipt and
   take detached primitive observations of all five raw axes, aliases,
   lifecycle switches, exact audited Cloud members, lifecycle members, and
   template hooks, and construct the recursively frozen candidate snapshot.
   The captured signature includes raw key/value identities and every static
   member or enum identity represented by the snapshot, including the Cloud
   type MRO used for Cloud-facet classification. A private
   strong-reference anchor tuple keeps every first-phase identity-bearing
   object alive through step 3. The candidate is not yet published.
2. Release the lock while retaining only the frozen candidate, its signature,
   and private anchors. No live provider object is inspected and no duplicate
   mirror projection schema is introduced in this phase.
3. Reacquire the lock immediately before return, revalidate receipt type,
   process, epoch, and inactive-session state, and recompute the complete raw
   and observed-member signature through the same projector. Compare both
   signatures and the two recursively frozen projections before releasing the
   lock. Projection equality is the completeness backstop for bounded metadata
   or classification changed in place without changing object identity. Any
   difference raises the exact typed capture error and discards the entire
   candidate snapshot.

A stable direct replacement is retained as `EXTERNAL_OR_REPLACED`.
Replace-then-restore cannot produce a torn published result:
if capture observed the replacement, its captured signature differs from the
final signature; if both observations equal the restored state, the frozen
projection contains only that restored state. Callable internals and mutable
provider instance attributes are outside V1 and receive no stronger claim.
The later coordinator removes these escape hatches rather than granting them
provenance.

The pre-plugin expectation table retains the exact original getter function,
sealed executable shape, direct-global binding name, and module for each
built-in provisioner. Every pinned getter is an exact zero-argument Python
function whose significant bytecode is exactly one `LOAD_GLOBAL` followed by
`RETURN_VALUE`, with no defaults, keyword defaults, or closure. Capture checks
the function identity, exact function type, code object, defaults, keyword
defaults, closure, and globals mapping against that seal. Exact function fields
are read through the built-in function descriptors without invoking plugin
attribute access. Capture then scans the sealed globals dictionary with exact
`dict.items()` and accepts only an exact `str` key equal to the sealed binding
name. It does not use hashed lookup because a plugin can insert a colliding
non-string key whose equality method executes code. It never invokes a getter.
The observed binding must be the exact baseline module before any member is
inspected. A replaced getter or an in-place executable-shape change is recorded
by getter identity with `REPLACED_BUILTIN_GETTER`; a missing or changed global
binding emits `PROVISIONER_BUILTIN_IDENTITY_MISMATCH`. Thus neither function
replacement nor a check-to-call race can run arbitrary plugin, credential,
catalog, or network code through the audit.

Capture must not call `CLOUD_REGISTRY.from_str()`, `_resolve_provisioner()`,
`repr()`, `Cloud.canonical_name()`, `Cloud.is_same_cloud()`, credential or
catalog APIs, capability methods, lifecycle methods, descriptors, arbitrary
plugin properties, or provider callbacks. It does not mutate any registry,
alias map, compatibility diagnostic set, or plugin state. All inventory
observation is static; no getter or provider callback is executed.

##### Characterization, rollout, and removal gates

The first implementation must prove:

- a clean subprocess imports the exact worktree and yields the sorted 25 Cloud,
  24 built-in provisioner, two Cloud alias, one provisioner alias, and IBM
  expected-partial baseline;
- all 24 raw built-in modules have callable static members for the seven exact
  lifecycle methods, while IBM has the exact legacy switches;
- canonical and Cloud-alias lookup retain the same current singleton identity,
  without using lookup during snapshot construction;
- the snapshot is recursively immutable, detached from later mutation,
  deterministic for an unchanged registry, never calls any getter, and
  statically observes the exact sealed global binding once per signature phase;
- strict-only and legacy-only provisioner registrations receive their exact
  undeclared-partial classifications and conformance errors;
- strict overlays, complete legacy overlays, partial legacy mixed ownership,
  template-only legacy registration, non-callable lifecycle and template
  members, incomplete built-in lifecycle, simultaneous raw strict and legacy
  state, and all four replacement directions are represented without changing
  effective dispatch;
- canonical-alias collisions, alias-canonical collisions, dangling aliases,
  alias-to-alias targets, excluded aliases, noncanonical, oversized, and
  non-string names, wrong Cloud values, and alias-as-provisioner-canonical
  entries produce exact bounded issues and deterministic unkeyed evidence;
- deleting an expected built-in Cloud or deleting or retargeting an expected
  built-in Cloud alias remains visible through its exact baseline mismatch;
- a replaced Cloud singleton, strict lifecycle, legacy module, template hook,
  built-in module getter, and individual method produce bounded identity
  evidence without retaining the object;
- pre-barrier capture, a failed plugin load, a stale receipt after a second
  context starts, a wrong-process receipt, and a supported post-barrier
  registration fail with their exact capture-error reason;
- import-time Cloud decoration and install-time provisioner registration are in
  the same successful receipt, and MAIN, UVICORN, EXECUTOR, and CONTROLLER
  receipts never cross process boundaries;
- concurrent supported replacement and capture produce a complete old or new
  observation, never the strict/legacy intermediate state, while concurrent
  direct member replacement fails whole-capture revalidation;
- registry identities and representative resolver results before and after
  capture are identical, and no provider, credential, catalog, or network code
  runs;
- hostile metaclass equality, MRO and metadata descriptors, colliding
  non-string instance, module, class, and getter-global keys, container-level
  custom resolution, and dynamic lifecycle-switch fallback never execute;
- in-place Cloud-base mutation changes the signed observation and projection,
  and a fork child can begin registration while a non-surviving parent thread
  held either coordinator lock.

This first slice adds no automatic audit logging, database row, API response,
or dispatch branch. An explicit caller receives only the in-memory snapshot.
It therefore does not start the compatibility-release clock. Before descriptor
authority or removal can use release evidence, a later reviewed integration
must capture the latest receipt in every active process context, publish only
bounded schema version, release identity, context, issue code, severity, and
count through the existing Datadog path, and attach a token-free
`ProviderRegistryAuditReportV1` to the exact-image release qualification
artifacts. No new statistics store is introduced. Runtime identity tokens,
module names, qualnames, raw malformed names, and provider exception text never
enter Datadog or the report.

No descriptor promotion can consume the audit until every `ERROR` is
eliminated or accepted by a later exact descriptor declaration. The required
one-compatibility-release gate is measured only after that Datadog and release
artifact integration is deployed in MAIN, UVICORN, EXECUTOR, and CONTROLLER.
The release report and bounded Datadog counts must agree on zero unexplained
errors for the exact image.

All code introduced by this audit slice is temporary migration machinery:
the built-in expectation tables and getter-shape seals, raw-name and identity
types, snapshot and capture API, issue and capture-error enums, effective-owner
projection, process-local token key, load mutex, mutation lock, epoch and receipt state,
registration-session coordinator, plugin-loader begin, complete, and abort
hooks, Cloud and provisioner mutation hooks, raw/member signature machinery,
and compatibility source classifications. None receives dispatch authority.
After the runtime commit SHA exists, a later commit must add an exact executable
removal-manifest locator for every one of those symbols or hooks. The later
`ProviderRegistrationV1` coordinator may replace the synchronization primitive
in place, but it cannot silently make an audit-only symbol permanent.

Removal requires one measured compatibility release with no unexplained audit
error, descriptor-derived legacy views, alias conformance, plugin replacement
tests, and repository proof that no independent mutable provider inventory or
lifecycle switch remains.

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

An additional review compared SkyPilot at `af20f62b3` with dstack at
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
| Slurm | F3 plus F2 | local Slurm and SkyPilot config plus live SSH partition, node, and GRES reads; TTL cache writes and nested `sbatch_options` aliases; returned `slurm_private_key` is an SSH `IdentityFile` credential path and must be split before snapshot promotion |
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
Slurm's `slurm_private_key` is credential-path execution context, not a public
render value; recursive detachment cannot make it snapshot-safe. IBM, Slurm,
Yotta, and Vast are prohibited from whole-result snapshotting.

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

#### Rejected M4 query-capability planner shadow

Status: rejected before any runtime commit by Review 28. This section is
retained as an auditable rejected alternative and is not authorized for
implementation. The active prerequisite is the typed resolved provider
operation foundation below.

This lifecycle audit is pinned to SkyPilot
`e61407e93acc8e4566476feceb1da0166f48470d` and dstack
`c9ebdaad6bbaa3105061d79f6ab52af9d609e99d`.

dstack has a useful responsibility boundary that SkyPilot does not yet have.
Its VM backends expose short create, terminate, and provisioning-observation
operations. The server pipeline owns repeated observation, deadlines, and
state transitions. The DigitalOcean implementation, for example, returns an
incomplete `JobProvisioningData` after create and later fills its address via
`update_provisioning_data()`. This does not make dstack's whole pipeline a
drop-in design for SkyPilot, and several dstack providers still contain local
waits. The transferable rule is narrower: provider code should translate and
submit provider-native operations, while one shared owner should decide the
next cluster step from an immutable observation.

SkyPilot currently repeats that second responsibility in provider modules.
Every concrete new-provisioner `run_instances()` owns some combination of
target-count policy, head selection, stopped-node reuse, create ordering,
polling, timeouts, result projection, and cleanup. DigitalOcean makes the
boundary especially visible. Its `run_instances()` performs seven different
time-varying reads around waits and effects, starts every stopped node even
when `resume_stopped_nodes` is false, selects heads by provider page order,
uses random names, and proves readiness by counts rather than by intended
provider identities. Its stop and terminate paths likewise prove global
counts rather than the state or absence of every submitted target. The generic
post-run `wait_instances()` adds no safety because the DigitalOcean
implementation is a no-op. A single pre-mutation observation cannot reconstruct
those histories or truthfully predict the final `ProvisionRecord`.

The first pilot therefore introduces two foundations with deliberately
different evidence levels:

1. a provider-neutral, pure, one-step cluster planner exercised against a
   frozen DigitalOcean corpus; and
2. a duplicate-preserving DigitalOcean inventory sidecar and exact query
   projector shadowed from the same provider response as the authoritative
   legacy `query_instances()` result.

The first pilot has no actuation path. It does not add a required method to
`InstanceLifecycleV1`, select dispatch, call a provider from the planner, or
claim end-to-end `run_instances()` or `ProvisionRecord` parity.

##### Raw capture, semantic inventory, and legacy evidence

`DigitalOceanInventoryCaptureResultV1` is a closed union produced during one
existing paginated droplet-list traversal. Success is exact type
`DigitalOceanInventoryCaptureSuccessV1` and contains only recursively
immutable DigitalOcean node rows, a bounded legacy response order, a canonical
semantic inventory, and successful capture evidence. Failure is exact type
`DigitalOceanInventoryCaptureFailureV1` and contains only one closed reason
plus saturated page and row counts. Failure contains no node, response order,
partial inventory, or digest. Constructors reject every other combination.
The legacy instance mapping remains a plain new `dict` with the same
last-value-wins and first-key-position behavior as today. It is not stored in
the semantic inventory. Raw provider mappings, SDK objects, clients,
credentials, provider configuration, tags, networks, arbitrary nested values,
request identifiers, cursor URLs, and exception text never enter a frozen
value.

`DigitalOceanNodeRefV1` has `provider_instance_id` and
`legacy_instance_name`. The former is the canonical decimal DigitalOcean
droplet ID and is the only candidate future effect identity. The latter is the
current droplet name and exists for legacy projection only.
`DigitalOceanNodeObservationV1` adds the closed
`DigitalOceanNativeStateV1`, closed `NodeRoleV1`, and provider region. The
native state grammar is exactly `new`, `archive`, `active`, and `off`.
Operation-specific projectors retain that grammar: query maps `new` and
`archive` to `INIT`, `active` to `UP`, and `off` to `STOPPED`, while launch
projection rejects `archive` instead of normalizing it to `new`. Role is
`HEAD` only when the exact name ends in `-head`, `WORKER` only when it ends in
`-worker`, and `UNCLASSIFIED` otherwise.

A normal row must be an exact string-keyed `dict`. `id`, `name`, and `status`
are read from exact scalar values. Provider region is read only from exact
nested shape `row['region']['slug']`, where both dictionaries have exact
string keys and `slug` is an exact bounded string. Missing, subclassed,
aliased, malformed, or over-bound region shape makes only the sidecar fail.
Unexpected exact-string keys and their values are ignored without access.

The canonical semantic inventory has these exact fields:

- `schema_version`, exactly `1`;
- `provider_name`, exactly `do` for this adapter;
- `cluster_name_on_cloud`;
- `ownership_kind`, exactly `LEGACY_CLUSTER_TAG`;
- `incarnation_kind`, exactly `UNKNOWN`;
- `actuation_eligible`, exactly false;
- canonical nodes sorted by the UTF-8 bytes of
  `(provider_instance_id, legacy_instance_name)`; and
- `semantic_sha256`, computed from canonical compact JSON of every preceding
  field and canonical node, excluding the digest field itself.

Capture also retains `legacy_response_order` as a bounded tuple of provider
IDs and `capture_sha256`, which includes that order. Response order is excluded
from `semantic_sha256`. Page count and closed capture codes are evidence and
are not planner input. Equal semantic inventories produced by different page
order therefore have the same semantic digest and pure plan, while their
capture digests and legacy projections may differ. Neither digest is a
provider version, redaction boundary, ownership proof, effect fence, or
telemetry value.

`project_digitalocean_launch_inventory_v1()` is the only adapter into the
provider-neutral `NodeInventoryV1`. It receives the explicit launch request
because read-only `query_instances()` has no requested-region argument. It
translates native `new`, `active`, and `off` into `PENDING`, `READY`, and
`STOPPED`; preserves provider and legacy identities, role, observed region,
ownership, incarnation, and the source semantic digest; verifies every
observed region against the request; and returns a closed not-representable
result for `archive` or any DigitalOcean-specific ambiguity. The query
projector reads the native capture directly, so no universal state conversion
can erase DigitalOcean semantics. Requested region is never guessed from
provider config or one observed node and is excluded from the raw semantic
digest.

Capture reads only an exact allowlist of exact built-in scalar and container
types. It accepts exact `dict`, `list`, `str`, and non-boolean `int` values,
then manually copies required scalars into tuples. Subclasses, hostile
descriptors, noncanonical or negative IDs, invalid UTF-8, missing fields,
source alias mutation during capture, and unknown state tokens produce one
closed failure code. Capture never calls `str()`, `repr()`, generic deep copy,
generic serialization, or hashing on an unvalidated provider value. Frozen
constructors repeat exact-type, tuple, enum, and bound validation so callers
cannot create nominally frozen objects containing mutable leaves. Identifiers
are excluded from repr, generic serialization, and pickling.

V1 sidecar limits are 32 observed pages, 1,024 observed nodes, 50 rows per
requested page, 1 to 1,024 UTF-8 bytes for `cluster_name_on_cloud`, 64 ASCII
bytes for an ID, 255 UTF-8 bytes for a node name, 1 to 64 UTF-8 bytes for an
observed region, 32 UTF-8 bytes for a native state token, and 2,048 UTF-8 bytes
for an inspected next cursor. Every string must have exact type `str` and
encode as strict UTF-8 with no surrogate. These are diagnostic eligibility
limits only. They never truncate or terminate the authoritative legacy
traversal. After any limit is crossed, legacy collection continues unchanged
and the sidecar retains only a closed overflow code, not a partial inventory.
Repeated or backward cursors, malformed response shape, duplicate provider
IDs, and more than 50 returned rows on one requested page likewise make the
sidecar ineligible without changing legacy behavior.

`DigitalOceanCaptureFailureReasonV1` is exactly
`INVALID_RESPONSE_CONTAINER`, `INVALID_DROPLETS_CONTAINER`,
`INVALID_LINKS_CONTAINER`, `INVALID_PAGE_SIZE`, `PAGE_LIMIT`, `NODE_LIMIT`,
`INVALID_NODE_CONTAINER`, `INVALID_NODE_ID`, `INVALID_NODE_NAME`,
`INVALID_NATIVE_STATE`, `INVALID_NODE_REGION`, `INVALID_CLUSTER_NAME`,
`DUPLICATE_PROVIDER_ID`, `SOURCE_MUTATED`, `INVALID_CURSOR`, and
`CURSOR_LIMIT`. The fixed enum order is the diagnostic sort order. Provider
exceptions remain legacy exceptions and never become capture-failure reasons.

Duplicate droplet names are preserved in response evidence and canonical
nodes so the audit exposes the current dictionary collapse. A duplicate
provider ID rejects capture. A duplicate legacy name, multiple heads,
unclassified role, cross-region node, or archived node makes launch planning
`NOT_REPRESENTABLE`; it is never resolved by arbitrary ordering. Unknown
incarnation is instead a mandatory promotion blocker on every otherwise valid
diagnostic plan. Current resources carry only name-scoped cluster tags, so
`actuation_eligible` remains false and no plan action is executable. Promotion
requires a reserved durable incarnation marker and a reviewed compatibility
policy for older unmarked resources.

The historical DigitalOcean cluster-name limit is not an authority gate.
`DO._max_cluster_name_length()` returns 247, while the normal writer resolves
the public `max_cluster_name_length()` and currently receives null. The random
worker grammar adds 12 bytes, making values above 243 unsafe for a 255-byte
provider name. Shadow accepts a bounded compatibility string but classifies
create requirements above 243 bytes as
`NOT_REPRESENTABLE(NAME_LENGTH_UNSAFE)`. Fixing the public/private method drift
and replacing random names with a reviewed deterministic grammar are separate
prerequisites for create promotion.

##### Pure one-step planner

`ClusterPlanRequestV1` contains only `cluster_name_on_cloud`, `region`,
`desired_count`, and exact boolean `resume_stopped_nodes`. Desired count must
have exact type `int`, excluding `bool`, and be between 1 and 1,024 for V1.
Cluster name must be exact `str`, strict UTF-8, 1 to 1,024 bytes, and equal the
inventory cluster name. Requested region must be exact `str`, strict UTF-8,
and 1 to 64 bytes. Values outside that grammar are
`NOT_REPRESENTABLE(INVALID_REQUEST_SHAPE)` shadow inputs; they do not change
current legacy validation or exceptions.

`NodeRefV1` contains exact bounded strings `provider_instance_id` and
`legacy_instance_name`. `NodeObservationV1` contains one ref, exact
`NodeStateV1`, exact `NodeRoleV1`, and exact bounded `provider_region`.
`NodeInventoryV1` contains schema version, exact provider and cluster strings,
`NodeOwnershipKindV1`, `NodeIncarnationKindV1`, exact false
`actuation_eligible`, canonical node tuple, and source semantic digest. Direct
constructors validate the same closed grammar as adapter construction.

`plan_cluster_next_action_v1(request, inventory)` is mechanically pure. It
cannot import or call provider adaptors, clients, credentials, environment or
configuration readers, clocks, UUID or random generators, sleeps, logging,
telemetry, mutable module state, or callbacks. It returns one recursively
immutable `ClusterNextActionPlanV1` with the schema version, source semantic
digest, request, `execution_authority`, canonical state partitions, unique
selected head if present, one closed next action, disposition, and sorted
blockers. `execution_authority` is exactly `PlanExecutionAuthorityV1.NONE` in
V1. Construction rejects any other authority, callable, method that could
submit, or executable handle.

The dispositions are `COMPLETE`, `WAIT`, `WOULD_SUBMIT`, `REJECT`, and
`NOT_REPRESENTABLE`; there is no `SUBMIT` value. The next-action union is
closed and has these exact payloads:

- `WaitForReobservationV1(nodes)` contains a canonical nonempty tuple of refs;
- `ResumeNodesV1(nodes)` contains canonical nonempty exact provider refs;
- `AssignHeadV1(node)` contains the canonical-minimum ready ref and is valid
  only when ready count equals desired and no head exists;
- `CreateSlotsV1(slots)` contains a nonempty tuple of
  `CreateSlotV1(ordinal, role)`, ordinals exactly `0..n-1`, and a head slot at
  ordinal zero only when required;
- `CompleteV1()` has no payload;
- `RejectV1(reason, nodes)` has one `RejectReasonV1` and canonical affected
  refs; and
- `NotRepresentableV1(reason, nodes)` has one
  `NotRepresentableReasonV1` and canonical affected refs.

Plan blockers are a duplicate-free tuple in fixed `PlanBlockerV1` enum order.
No action contains a create or rename name. The DigitalOcean projection adds
`RANDOM_LEGACY_HEAD_NAME` and `LEGACY_STALE_HEAD_RETURN` for head assignment,
and `RANDOM_LEGACY_CREATE_IDENTITY` for every create requirement. These are
promotion blockers even when the diagnostic action shape otherwise matches.
The complete V1 blocker order is `LEGACY_CLUSTER_TAG_ONLY`,
`INCARNATION_UNKNOWN`, `LEGACY_IGNORES_RESUME_POLICY`,
`RANDOM_LEGACY_HEAD_NAME`, `LEGACY_STALE_HEAD_RETURN`, and
`RANDOM_LEGACY_CREATE_IDENTITY`.

`RejectReasonV1` is exactly `TOO_MANY_READY` and `TOO_MANY_OWNED`.
`NotRepresentableReasonV1` is exactly `INVALID_REQUEST_SHAPE`,
`V1_COUNT_LIMIT`, `DUPLICATE_LEGACY_NAME`, `MULTIPLE_HEADS`,
`UNCLASSIFIED_ROLE`, `CROSS_REGION_NODE`, `ARCHIVED_NODE_PRESENT`, and
`NAME_LENGTH_UNSAFE`. Capture failure is not a not-representable plan and
cannot reach the planner.

Planning order is exact:

1. validate request scope, inventory identity, bounds, roles, region, native
   projection, duplicate names, head uniqueness, and ownership truth;
2. return `WAIT` when any pending node requires a fresh observation;
3. when resume is true, reject if ready plus stopped exceeds desired;
   otherwise reject when ready alone exceeds desired;
4. when resume is true and stopped nodes fill a positive deficit, return one
   canonical `ResumeNodesV1`; when resume is false, stopped nodes do not count
   toward desired and DigitalOcean's current always-resume behavior is an
   explicit `LEGACY_IGNORES_RESUME_POLICY` delta;
5. if ready count is below desired, return `CreateSlotsV1`, including a head
   slot first only when no unique ready head exists;
6. if ready count equals desired but no head exists, return one deterministic
   `AssignHeadV1`; and
7. return `COMPLETE` only when ready count equals desired and exactly one head
   exists.

Every nonterminal decision requires a new inventory before another decision.
The planner does not emit a loop, deadline, retry policy, provider mutation,
cleanup, final identity set, or `ProvisionRecord`. Same request plus same
semantic inventory produces byte-identical canonical plan JSON independent of
provider response order. `to_canonical_json_bytes()` is the only supported
serialization for a plan and is closed to its public scalar, enum, tuple, and
null fields. Generic dataclass, pickle, and object serializers are prohibited.

##### Query shadow integration and compatibility containment

The first live hook is read-only `query_instances()`, not `run_instances()`.
Public `sky.provision.do.instance.query_instances()` and public
`filter_instances()` remain capture-disabled legacy delegates. The shared
provision facade is the only owner that knows the selected route. When it has
pinned the exact built-in DigitalOcean resolution with no strict or legacy
registration, it enters `ProviderDiagnosticRouteV1` carrying one private
process-local capability token and then invokes that already-pinned legacy
callable exactly once. The built-in query can request the private carrier only
while that exact token is active. A direct call, strict or legacy plugin,
partial plugin fallback, delegating wrapper, whole-module or method
replacement, `filter_instances` monkeypatch, custom module attribute
resolution, wrong process, nested invocation, reused token, or failed
executable seal cannot activate capture. It invokes only the already-selected
legacy owner. The token is single-use, exact-context, PID-bound,
unserializable, and reset after fork. It is a compatibility-routing capability,
not provider authority, authentication, or a security boundary.

Within the tokenized built-in route, the current traversal is factored through
a private exact owner with an exact legacy-only mode and an exact capture mode.
The unchanged public helper always uses legacy-only mode, which constructs no
sidecar and runs no capture, projector, comparator, or diagnostic. Capture mode
returns the ordinary plain dictionary plus the closed capture result from the
same rows and is reachable only through the consumed private token. Admission
also requires the facade, module, public helper, private traversal owner, query
function, function objects, executable code, defaults, and static attribute
resolution to match sealed built-in values. The token, admission branch, and
capture mode are temporary compatibility artifacts and receive exact removal
rows in the same pull request after their runtime commit SHA exists.

The exact route performs one provider traversal. It constructs the legacy
mapping with current filtering, overwrite, and insertion-order behavior,
builds the authoritative query result first, and immediately returns or raises
if legacy behavior fails. Only after a successful legacy result does it run
the frozen legacy projector and comparator. The pure projector iterates the
ordered legacy evidence and must match both result values and item order.
Ordinary shadow exceptions are contained as closed outcomes. Cancellation,
`KeyboardInterrupt`, `SystemExit`, and other `BaseException` control signals
retain normal propagation. There is no second provider call, no second legacy
call, no fallback after shadow work, and no change to the returned plain
dictionary. Diagnostic formatting and logging run inside the same containment
boundary, so an ordinary logging failure cannot change the legacy result.

Existing Datadog collection remains the only telemetry plane. The hook emits
no per-query match log. For `MISMATCH`, `NOT_REPRESENTABLE`, `CAPTURE_FAILURE`,
or `SHADOW_ERROR`, it may emit one warning per exact closed
`(contract, outcome, reason)` tuple per process generation. A PID-sensitive,
fork-reset, bounded set no larger than the finite enum cross-product suppresses
every repeat. Counts saturate at their V1 limits. Diagnostic formatting,
suppression, and logging are inside containment. Logs never include cluster or
node identities, inventory or value-derived digests, provider values,
exception text, config, credentials, or raw responses. This slice creates no
metrics store, database table, persisted report, timer, or identity-keyed
cardinality.

The query hook proves only same-read identity capture and status projection.
The pure next-action planner remains offline corpus evidence in this slice.
Live launch comparison becomes eligible only after each relevant legacy read
is factored through the same frozen observation boundary. Even then, each
comparison is one next decision, never an end-to-end prediction.

##### Characterization, rollout, and removal gates

The frozen corpus covers empty and multi-page responses; 1,024-node and
overflow boundaries; finite repeated or backward cursor traces; malformed
response shape; duplicate IDs and names; all four native states and unknown
state; hostile
container and scalar subclasses; source alias mutation; field byte limits;
page permutations; and secret-like values in every ignored response and
configuration location. It proves that ignored and malformed values never
reach snapshot repr, plans, exceptions, logs, pickles, or diagnostics.

An actually repeated provider cursor can keep today's unchanged legacy loop
alive forever, in which case no sidecar result exists and no diagnostic can be
emitted. Repeated-cursor capture assertions are therefore finite offline
evidence only until a separately reviewed pagination timeout changes legacy
behavior.

Planner cases cover empty create with a head slot; complete unique-head
clusters; head assignment; create workers with and without a head; pending
wait; stopped-node resume and no-resume divergence; ready or owned excess;
multiple heads; unclassified names; archives; cross-region nodes; duplicate
names; desired counts 0, `True`, 1,024, and 1,025; name lengths 243, 244, and
247; input permutation; byte-identical repeated plans; and tripwires for every
forbidden I/O or ambient dependency.

Compatibility tests prove exact cold and warm provider call counts, page
parameters and order, ordinary result type, content and insertion order,
legacy exception object and cause, plugin and monkeypatch routing, no shadow on
failed identity seals, source detachment, comparator failure containment, and
zero UUID, sleep, client, credential, config, clock, logging, or telemetry
access from the pure planner. Direct public query plus every launch, stop,
terminate, cluster-info, and status-filtered helper path proves zero capture,
projector, comparator, token, and diagnostic work. Legacy lifecycle
characterization separately records the seven launch reads, unbounded pending
wait, 96-poll bounded waits,
always-resume behavior, branch-dependent resumed IDs, stale head ID after
rename, random create identities, count-only readiness, multiple-head
inconsistency, and count-only stop and terminate completion. Those tests are
evidence, not authorization to preserve the defects.

The runtime change is deployed as an exact image with the shadow unable to
mutate. Without a credentialed DigitalOcean account, Kubernetes rollout proves
only import and general API, controller, and executor safety. It does not count
as a DigitalOcean canary. Promotion remains prohibited until credentialed
create, observe, stop, terminate, cleanup, and provider-absence tests pass with
durable incarnation and action identity.

`PLA-GAP-001` remains the dependency-closed owner for every DigitalOcean and
cross-provider lifecycle responsibility not migrated by this pilot. After the
runtime commit exists, a later bookkeeping commit in the same pull request
adds exact removal rows whose `introduced_by` is that runtime SHA for the route
token, query shadow selector, private carrier mode, comparator, diagnostic
limiter, and emission. That commit lands and passes the manifest checker before
any image build or deployment. The immutable inventory and pure planner are
intended permanent owners; only temporary comparison and compatibility routing
are `must_remove`. Later promotion rows replace the
legacy `_get_head_instance`, target-count, start, create, readiness, stop,
terminate, and `ProvisionRecord` owners only after their separate evidence
gates close.

#### M4 typed resolved provider operation foundation

This foundation applies the narrowest lesson needed before a shared cluster
reconciler can safely call provider primitives. dstack keeps many provider
backends comparatively short because its shared pipeline owns the surrounding
workflow and calls typed backend operations. SkyPilot cannot move that workflow
until a routed operation has one explicit owner and the exact callable selected
for that invocation. Selecting a provider and then resolving a module attribute
again during execution leaves a race between selection and invocation. The
foundation therefore changes operation resolution, not provider behavior.

`sky.provision.provider_facets` adds the exact callable protocol
`QueryInstancesFnV1` and the frozen
`BuiltinQueryInstancesDiagnosticV1`. The diagnostic facet contains exactly:

- `authoritative_implementation: QueryInstancesFnV1`; and
- `diagnostic_implementation: QueryInstancesFnV1`.

`ProvisionerBundleV1` appends the defaulted field
`builtin_query_instances_diagnostic` after all existing fields so existing
positional construction remains compatible. A caller of the public strict
registration API must not populate this field. Strict registration rejects a
non-`None` diagnostic facet because this is a private opt-in seam reserved for
in-tree built-in modules. Rejection occurs before validation or mutation of
either registration map. The field participates in exact idempotent bundle
comparison even though public registration cannot populate it.

`sky.provision` adds private operation-resolution types. The unique enum
`_ProvisionerOperationOwnerV1` has exactly `STRICT`, `LEGACY`, and `BUILTIN`.
The frozen `_ResolvedProvisionerOperation` contains `owner`,
`authoritative_implementation`, and an optional `diagnostic_implementation`.
Its `implementation` property returns the diagnostic callable when present and
otherwise the authoritative callable. `BUILTIN` describes ownership of one
operation, not exclusive ownership of the whole provider.

`_ProvisionerResolution.resolve_operation(method_name)` preserves the existing
precedence and returns one typed operation:

1. a strict lifecycle implementation returns owner `STRICT`;
2. a non-`None` legacy module attribute returns owner `LEGACY`;
3. a built-in fallback returns owner `BUILTIN`; and
4. an unavailable operation returns `None`.

The mixed-owner warning remains immediately before a built-in lifecycle
fallback. A partial legacy registration can therefore resolve its missing
operation to `BUILTIN`, with the existing warning-once behavior. The typed
built-in bundle keeps `LegacyInstanceLifecycleAdapter` as its compatibility
surface, but operation resolution reads the raw built-in method exactly once
and constructs the authoritative invocation from that exact object. The
value-returning lifecycle methods use the selected raw callable directly.
Pinned wrappers for `stop_instances`, `terminate_instances`, and
`wait_instances` invoke the selected raw callable but discard its return value,
preserving the adapter's current `None` result even if a monkeypatch returns a
sentinel. A monkeypatch applied before an operation resolution is observed; a
change after resolution cannot redirect that invocation. Resolution never
caches a bundle, module, or operation, so the next facade call observes the
next current binding.

`_make_builtin_bundle()` may discover one private static module binding named
`_QUERY_INSTANCES_DIAGNOSTIC_V1` using `inspect.getattr_static`. It accepts the
binding only when its exact type is
`BuiltinQueryInstancesDiagnosticV1` and both stored callables pass runtime
validation. Validation uses the same parameter names, kinds, and defaults as
`InstanceLifecycleV1.query_instances` after dropping protocol `self`; both
fields must be exact undecorated Python functions with synchronous declarations
and no `__wrapped__` binding. Validation derives the actual declaration from a
clean function built only from the candidate's exact `__code__`, `__globals__`,
`__name__`, `__defaults__`, `__closure__`, and `__kwdefaults__`; it never asks
`inspect.signature()` to interpret the candidate or its writable signature
metadata. This makes `__signature__`, `__text_signature__`, `_partialmethod`,
and equivalent inspection overrides irrelevant. This is the closed shape of
the intended in-tree module functions. Bound methods, callable classes or
instances, partials, cache wrappers, decorated or cyclic functions, custom
call descriptors, signature-spoofed functions, and any other callable shape
are invalid even when they expose a compatible apparent signature. A missing
binding, subclass, descriptor result mismatch, noncallable, coroutine or
async-generator function, invalid callable shape, variadic or otherwise
incompatible code-derived signature, or ordinary static-discovery failure is
treated as no facet. Invalid diagnostic metadata must not make authoritative
provider resolution fail. No provider defines the binding in this foundation
slice, so all production calls keep their current implementation and result.

The resolver attaches the optional diagnostic implementation only when every
condition below holds:

- the requested method is exactly `query_instances`;
- the selected operation owner is `BUILTIN`;
- there is no legacy registration for the provider, including a partial one;
- the built-in bundle has an exact runtime-valid diagnostic facet; and
- the facet's authoritative implementation is, by identity, the raw built-in
  module `query_instances` observed for this resolution.

The resolver uses the same single raw attribute object for authoritative
invocation and diagnostic identity admission; it performs no second module
lookup. The identity condition invalidates a stale facet after an attribute
monkeypatch. Rebuilding the built-in bundle through the existing late-bound
module getter on every resolution preserves both attribute monkeypatches and
whole-module replacement between facade calls. A replacement module can opt in
only by supplying its own exact valid facet. Selection never branches on a
provider name.

The generic facade performs one resolution and one invocation:

```python
operation = resolution.resolve_operation(func.__name__)
if operation is not None:
    return operation.implementation(*args, **kwargs)
return func(provider_name, *args, **kwargs)
```

It does not invoke an authoritative query and then a diagnostic query. A future
provider diagnostic must own its single provider traversal, contain any shadow
failure internally, and return the authoritative result. The generic facade
must not catch a diagnostic exception and retry the authoritative callable,
because retrying may issue a second provider operation. Exceptions from the
selected authoritative or diagnostic callable propagate unchanged. Public
signatures, decorator order, metadata, `__wrapped__`, argument binding, and
default-body fallback remain unchanged.

This foundation explicitly excludes provider diagnostics, facade authorities,
route or executable seals, admission tokens, context variables, locks, process
or fork state, and provider-specific facade branches. It does not introduce a
live planner, immutable inventory, or DigitalOcean capture path. Those require
their own design and evidence after this resolution seam is established.

The focused contract suite must prove:

1. exact `STRICT`, `LEGACY`, and `BUILTIN` ownership;
2. strict precedence over both other sources and legacy precedence over a
   built-in;
3. partial-legacy built-in fallback with the warning emitted once;
4. built-in attribute and whole-module replacements made before resolution are
   invoked, while a descriptor returning a different callable on each lookup
   is read once and its first callable is invoked;
5. a missing built-in operation resolves to `None` and reaches the facade
   default;
6. old positional and keyword bundle construction defaults the diagnostic
   field to `None`;
7. strict registration rejects a populated diagnostic facet before either
   registration map changes;
8. an exact valid fake built-in facet selects its diagnostic implementation
   exactly once with the facade arguments and does not separately invoke the
   authoritative implementation;
9. replacing the raw authoritative query invalidates the facet;
10. any legacy registration suppresses the facet, including on a built-in
    fallback;
11. the query facet never attaches to any other operation;
12. noncallable, coroutine, variadic, wrong-default, and wrong-parameter
    diagnostics are absent while the authoritative operation still runs;
    both fields reject async generators, decorated or cyclic functions, bound
    methods, callable classes or instances, partials, cache wrappers, and
    unknown call descriptors or signature overrides;
13. a built-in stop callable returning a sentinel still yields `None`;
14. a subprocess can import and reload `sky.provision`;
15. one direct `_ProvisionerResolution` fixture containing strict, legacy, and
    built-in sources proves exact precedence, owner, and callable identity,
    independently of public last-registration-wins behavior; and
16. the existing public signature and metadata characterization remains
    unchanged.

The repository test gate includes the focused provider-facet suite, the full
provision unit suite, formatting, type checking, linting, import checks, and
the existing migration-removal checker. Deployment is required because this
changes the generic provision facade. The exact built image must be deployed to
the test control plane and prove database migration completion, API-server,
controller, and executor readiness, direct health, stable restarts, and clean
logs. Because no provider enables the diagnostic facet in this slice, the
rollout is control-plane regression evidence only. It cannot claim a provider
route or data-plane operation was exercised.

This foundation adds no temporary compatibility machinery and closes no
removal row. Existing `PLA-M4-102` and `PLA-M4-103` continue to track legacy
DigitalOcean responsibilities, but this slice neither introduces nor removes
those owners. The next slice may add a provider-local diagnostic facet only
after its exact one-traversal contract and failure containment are accepted.

#### M4 DigitalOcean authoritative query projection extraction

This slice applies the smallest durable part of dstack's provider boundary:
one provider read followed by one deterministic provider-local translation.
It does not introduce a generic inventory or shared reconciler yet. It removes
the inline DigitalOcean status translation from the effectful query entry
point so later observation work has one explicit, independently testable
translation owner.

`sky.provision.do.query_projection` adds one internal
`project_query_instances(instances, cluster_status)` function. It receives the
exact object returned by `utils.filter_instances()` and the exact current
`status_lib.ClusterStatus` binding read by `do.instance` for that call, then
returns the existing ordered
`dict[str, tuple[ClusterStatus | None, str | None]]`. The function owns the
current native-state map:

```text
new -> INIT
archive -> INIT
active -> UP
off -> STOPPED
```

It performs no provider call, retry, sleep, logging, telemetry, configuration
lookup, clock read, random generation, or mutation of its input. It constructs
one new result dictionary and otherwise reproduces the current loop exactly.
For each value from `instances.values()`, evaluation order remains:

1. read `instance_meta['status']`;
2. map that value through the current native-state map;
3. read `instance_meta['name']`; and
4. assign `(mapped_status, None)` to that name in the result.

The map remains function-local and is built from the explicitly passed status
binding on every invocation, preserving replacement of the existing
`do.instance.status_lib` binding without adding mutable module state. Duplicate
projected names retain Python dictionary first-insertion position and
last-value-wins value semantics.

`sky.provision.do.instance.query_instances()` retains all argument deletion,
provider-config assertion, and the single exact `utils.filter_instances()`
call with positional `cluster_name_on_cloud` and keyword
`status_filters=None`. It then calls `project_query_instances()` exactly once
with the helper result and the current `status_lib.ClusterStatus` object, and
directly returns that dictionary. Direct provider calls, facade calls, helper
and status-module replacements, exported aliases, strict and legacy plugin
precedence, and every non-DigitalOcean route keep their existing resolver path.
No V2 diagnostic metadata, context state, hook, shadow event, database state,
feature flag, or removal-manifest row is added.

Characterization tests prove the empty result, every mapped native state,
ordered values, duplicate projected names, and exact returned value shape.
Recording mappings prove the four-step per-row access order. Failure tests
prove an unknown state raises before reading the name, missing keys and
unhashable names retain their ordinary exception type and message, and neither
the projector nor query retries. Query integration tests prove one helper
lookup and call with the exact arguments, one projector invocation with the
helper's exact return object and current status binding, replacement of the
existing `do.instance.status_lib` binding, and direct return of the projector's
exact result object. Existing provider-facet, full provision, backend-status,
import, formatting, type, and lint gates also run.

This extraction is authoritative immediately because its finite mapping and
ordering semantics are fully characterized without a second live execution
path. Rollback is the ordinary exact-image rollback; there is no compatibility
artifact to age out. It grants no raw pagination, completeness, provider ID,
region, ownership, incarnation, absence, head selection, create, resume, stop,
terminate, wait, cleanup, retry, `ProvisionRecord`, planner, or reconciliation
authority. The next shared-reconciler slice still requires a typed immutable
node observation with deterministic identity and effect/incarnation fencing.

#### M4 DigitalOcean incarnation locator foundation

Status: accepted implementation boundary after exact-design adversarial and
simplicity review. This slice is pinned to SkyPilot
`22d64ffe7a344db282904dfe7061847c89e79b8e` and dstack
`c9ebdaad6bbaa3105061d79f6ab52af9d609e99d`.

dstack demonstrates the useful command/query separation that M4 will adopt:
its provider create returns bounded provisioning data promptly and a later
`update_provisioning_data()` observes readiness. Its current DigitalOcean
implementation nevertheless creates a randomly named droplet with an empty
tag list. Its pipeline lock token guards the pre-call database refetch and the
post-call result update, but it does not identify or fence the external create
itself. A worker that loses its lease after the request is accepted can still
leave an unowned droplet. SkyPilot must therefore port the separation of
responsibilities without copying that ownership gap.

SkyPilot already persists a stable `cluster_hash` before new-provisioner
provider I/O and rejects completion writes for a stale same-name generation.
The hash currently stops at `CloudVmRayBackend`; DigitalOcean droplets and
their paired volumes carry only the cluster-name tag. This slice defines the
smallest bridge, logically named `DigitalOceanIncarnationLocatorV1`. It owns
only propagation of the existing cluster-generation identity and creation-time
resource stamping. It adds no new identity store and no second telemetry
plane.

`ProvisionConfig` appends the keyword-only field
`cluster_incarnation: str | None = dataclasses.field(default=None,
kw_only=True, repr=False)` after every existing field. All eight existing
constructor parameters retain their positional and keyword mapping, and an
out-of-tree dataclass subclass may still append a required positional field.
The new field is last in `dataclasses.fields()`, participates in equality, and
is omitted from repr. Its class-level default also makes an object restored
from an older pickle with no instance-state entry read as `None`; no custom
pickle state or migration is added. The DigitalOcean consumer still uses
`getattr(config, 'cluster_incarnation', None)` for older config-like objects.
`None` is the exact legacy compatibility mode.

`get_redacted_config()` removes `cluster_incarnation` from the copied
`dataclasses.asdict()` result before returning it. The existing provision-log
JSON therefore remains byte-for-byte shape-compatible and never emits the raw
cluster identity. Direct `dataclasses.asdict()` and field enumeration see the
intentional additive internal field. There is no in-tree persistence or
reconstruction of `ProvisionConfig`; the compatibility suite nevertheless
pins a real missing-state pickle round trip, constructor signature, required
subclass, equality, repr, and field order.

`provisioner.bulk_provision()` adds a keyword-only
`cluster_incarnation: str | None = None` after its existing parameters and
copies it into the `ProvisionConfig` constructed for bootstrap. The production
new-provisioner call occurs only after `add_or_update_cluster()` has returned.
It asserts `_active_cluster_hash is not None` and passes that exact returned
value. It never re-reads identity by cluster name, derives identity from the
rendered YAML, or creates a provider-local replacement. New launch, retry,
resume, and scale therefore reuse the cluster generation already owned by the
database. Rendered cluster YAML, configuration hashing, public provider
facets, and `InstanceLifecycleV1` remain unchanged.

The existing dynamic `bulk_provision` replacement seam retains its exact old
call shape. `sky.provision.provisioner` records the exact built-in function in
one private import-generation alias after definition. The backend reads the
current callable and that alias exactly once for an invocation. It supplies
the new keyword only when the two objects are identical. A rebound function,
replacement module, wrapper, or old-signature monkeypatch receives precisely
the previous arguments and owns the whole substituted workflow; it receives
no implicit incarnation authority. There is no `TypeError` fallback and no
second call after an exception. Reload reconstructs the function and alias
together, and the next invocation observes that new import generation.

DigitalOcean reserves the tag key
`skypilot-cluster-incarnation`. When the optional value is absent,
`utils.create_instance()` preserves the exact current tag ordering and request
shape. Exact type `str` opts into the system marker. `None` and every value
whose exact type is not `str` preserve the byte-for-byte legacy unmarked tag
request; this compatibility downgrade grants no attribution or future
actuation authority. Every exact string, including empty, non-ASCII,
surrogate-containing, and arbitrarily long legacy values, has one total
deterministic encoding. The encoded value is exactly `v1-` followed by
lowercase hexadecimal SHA-256 of the byte domain separator
`skypilot-do-cluster-incarnation-v1\0` followed by the raw value encoded with
UTF-8 `surrogatepass`. No Unicode normalization, truncation, raw identity
substring, clock, salt, random value, or provider lookup participates. The
resulting full tag uses only ASCII letters, digits, colon, and dash and has a
fixed length below DigitalOcean's 255-character limit. The provider documents
that grammar at
<https://docs.digitalocean.com/reference/api/reference/tags/>.

User-supplied tags retain their current sorting and legacy override behavior
for `Name`, `ray-cluster-name`, and `skypilot-cluster-name`. Legacy mode also
preserves every marker-like user tag exactly. In marked mode, the helper first
formats the existing tags in their current deterministic order, then removes
every formatted tag whose case-folded value occupies the reserved
`skypilot-cluster-incarnation:` namespace. This includes the exact key and a
key that already contains a suffix after the reserved key. It preserves the
relative order and exact value of every remaining tag, then appends exactly one
authoritative marker as the final tag. This is managed-namespace overwrite,
not validation or rejection. The same immutable-by-convention tag list is
supplied to both the droplet request and its paired volume request. The input
`config.tags` mapping is not mutated. DigitalOcean bootstrap continues to
return the exact `ProvisionConfig` object, so the field survives without
another copy owner.

One pure DigitalOcean helper owns the existing tag projection, exact-string V1
encoding, and marked-mode reserved-namespace replacement. `create_instance()`
calls it exactly once before SSH-key lookup or any droplet, volume, or
attachment request. For all in-contract values the helper is total and adds no
new exception class, retry, cleanup bypass, or provider call. Existing
exceptions from copying, sorting, or string-formatting malformed legacy tags
retain their current type and ordinary cleanup behavior. `run_instances()`
does not duplicate marker preparation before discovery, resume, or rename
because preparation cannot reject a request and this slice stamps only newly
created resources.

The marker is durable generation-correlation evidence emitted by an exact
built-in marked create, not standalone proof of system authorship, ownership,
or an external-effect fence. Legacy mode can preserve a marker-like user tag.
`cluster_hash` is stable across multiple creates, retries, resume, and scale
within one cluster generation, so it is not a unique create-attempt identity.
The current DigitalOcean create remains three unjournaled effects: droplet,
volume, and attachment. A response lost after any of them remains ambiguous.
Current stop, termination, and failure cleanup still discover by the
cluster-name tag and ignore the marker, so stale cleanup can still affect a
same-name replacement. A new same-name cluster generation receives a distinct
marker, but that fact grants no cleanup isolation or mutation authority.

Existing unmarked droplets and volumes are never backfilled and are not
silently adopted into future typed actuation. A later immutable observer may
classify a resource marker as `MATCH`, `MISSING`, or `MISMATCH` against the
expected cluster incarnation and use the canonical provider droplet ID as node
identity. This slice does not add that observer or change `query_instances()`,
`filter_instances()`, `run_instances()`, `ProvisionRecord`, head selection,
resume, rename, stop, termination, wait, or query projection behavior. Only a
future `MATCH` observation can become an actuation candidate, and only after a
separate authority review. `MATCH` is necessary but never sufficient: one
`MISSING` or `MISMATCH` sibling blocks typed mutation for the entire cluster
generation, as does incomplete inventory. A marker from a stale attempt is
attribution evidence only and never proves current ownership.

Before any DigitalOcean mutation subset can promote to the shared reconciler,
the runtime must also persist a unique effect attempt and exact readback
locator before provider I/O. That locator must bind provider account scope,
region, resource kinds, expected bounded cardinality, and the cluster
incarnation; a synchronous live fence must pass immediately before submission;
and lost-response handling must quarantine until exact readback and absence
proof close the ambiguity. The generation marker alone satisfies none of
those gates.

The focused compatibility suite must prove:

1. all eight old positional and keyword `ProvisionConfig` parameters retain
   their signature and default the new field to `None`; a required-field
   dataclass subclass remains legal; field order, equality, repr, a real
   missing-state pickle round trip, and the unchanged redacted-log dictionary
   are exact;
2. an old config-like object with no attribute retains the exact legacy
   DigitalOcean request, and direct `bulk_provision()` calls default to legacy
   mode while an explicit incarnation reaches the exact bootstrapped config;
3. the backend passes the exact new or existing hash returned by
   `add_or_update_cluster()` only to the exact built-in call and never puts it
   into rendered YAML; an exact-old-signature replacement and a replacement
   module receive the old argument shape once, including after import reload;
4. absent incarnation preserves the exact old droplet and volume tag lists,
   while a marked request appends the exact same V1 marker to both;
5. encoding is byte-repeatable and domain-separated for ASCII, empty,
   non-ASCII, lone-surrogate, very long, 255-character, and 256-character raw
   hashes, always producing the closed fixed-length tag grammar;
6. `None` and every non-exact-string value preserve the exact legacy tag
   request, including marker-like user tags, without adding a marker;
7. marked requests filter exact-key, suffixed-key, and mixed-case occupants of
   the reserved namespace, preserve all remaining relative order and values,
   append exactly one marker last, and leave the input tags unchanged;
8. a legacy unmarked create followed by a marked same-name create, and two
   distinct same-name generations, produce the expected mixed attribution
   without asserting cleanup isolation, current ownership, or mutation
   eligibility;
9. the merged DigitalOcean query-projector, provider-facet, and stale database
   generation-fence characterization suites remain unchanged and green; and
10. no new exception class or cleanup bypass exists, while existing ordinary
    provisioning-failure cleanup characterization remains unchanged and green.

This runtime change requires an exact-image test-cluster deployment. The
rollout proves migration completion, import safety, API-server, controller,
and executor health, stable restarts, Datadog node coverage, and clean logs.
Without credentialed DigitalOcean access it is control-plane regression
evidence only and cannot claim a DigitalOcean create was exercised. Rollback
is the ordinary prior-image rollback, but its persistent provider consequence
is explicit: already marked resources retain their tag, the old image ignores
it, and the old image may create unmarked siblings. A later upgrade must treat
that mixed generation as ineligible for typed mutation until legacy resources
are gone or a separately reviewed migration proves ownership and completeness.
The tag is not removed during rollback. The slice adds no database schema,
feature flag, metric, event, persisted report, dual execution path, or
temporary removal-ledger owner.

#### M4 DigitalOcean dual-family inventory traversal, deferred

Status: design rejected before runtime implementation. The rejected proposal
was pinned to SkyPilot `612197aa45add7242187bf8338fbfd256255b9d4` and dstack
`c9ebdaad6bbaa3105061d79f6ab52af9d609e99d`. No dependency, provider-read,
lifecycle, database, or deployment change is authorized by this subsection.

DigitalOcean documents two disjoint list families. A request filtered by
`tag_name` returns tagged non-GPU droplets because the default list excludes
GPU droplets. A request with `type=gpus` returns GPU droplets, but `type` cannot
be combined with `tag_name`. See the official
[Droplet API](https://docs.digitalocean.com/products/droplets/reference/api/droplets/)
and
[PyDo list reference](https://docs.digitalocean.com/reference/pydo/reference/droplets/list/).
SkyPilot currently calls only the first family. The resulting GPU discovery
gap is real, but correcting a read path is not authority-neutral when every
result is immediately consumed by legacy mutation paths.

The current `filter_instances()` result is a mutable name-keyed dictionary
used by readiness, status, start, stop, rename, termination, and cleanup. It
does not preserve canonical identity, credential scope, duplicate rows, or an
atomic provider snapshot. Feeding account-wide GPU rows into that dictionary
would therefore expand destructive reach before typed ownership and effect
fences exist. A cross-family duplicate name could redirect a lifecycle action
to a different droplet, and rollback could not restore a stopped or deleted
resource. The proposed `authority=NONE` raw carrier did not constrain the
legacy dictionary that actually reached those effects.

The rejected design also changed compatibility and availability semantics. It
read `status` when `status_filters is None`, although the current path reads
only `name`; retained dead raw-row data; treated changing account-wide
`meta.total` as a hard cluster failure without obtaining a snapshot; changed
client-resolution timing; mixed native and translated pagination failures;
and doubled each polling traversal before token-scoped pacing or single-flight
coordination existed. Stable totals could still accept duplicate IDs and omit
another row, so the additional failures did not prove completeness.

The known GPU omission remains explicit until a future promotion satisfies all
of these prerequisites in one reviewed behavior contract:

1. one immutable observation preserves a validated canonical provider ID,
   exact cluster tag, incarnation marker classification, native state, region,
   and a request-scoped credential or account identity;
2. a read-only consumer receives dual-family rows without changing the legacy
   dictionary, lifecycle decisions, or provider effects, and fails closed on
   duplicate IDs, cross-family name collisions, malformed membership, and
   incomplete traversal;
3. token-scoped pacing and single-flight ownership bound repeated account-wide
   scans before any polling loop can use them;
4. an actuation attempt is durably recorded before a provider effect, and the
   effect owner revalidates the exact provider ID, credential scope, cluster
   tag, and incarnation through an exact-ID read immediately before mutation;
5. absence, partial pagination, scope mismatch, collision, stale incarnation,
   and lost responses all produce closed non-mutating outcomes; and
6. a read-only credentialed canary proves both list families and their rate
   behavior before a separate promotion enables any mutation consumer.

Legacy compatibility must remain exact while these prerequisites are absent.
In particular, `status_filters=None` must not read `status`, existing
field-access and exception timing must remain characterized, and no GPU row may
enter a name-keyed destructive path. The PyDo minimum-version increase belongs
with the first production consumer of the explicit `type='gpus'` call, not as
an isolated dependency change.

dstack's useful lesson remains narrower than the rejected implementation: it
keeps the provider-generated droplet ID from creation and later performs an
exact-ID read, while its shared server pipeline owns repeated readiness and
state transitions. SkyPilot should adopt that command/query separation only
after its observation carries identity and scope and its shared effect owner is
fenced. Until then, preserving a visible limitation is safer than silently
widening legacy authority.

### M5: Serve and pools

- shadow `ChildWorkloadObservationV1` against current replica job-status
  polling before shared child launch or teardown is reachable;
- extract pure planners and reducers for the central PostgreSQL deployment;
- persist central replica launch and down attempts;
- keep lifecycle epoch, immutable versions, and incarnation inventory;
- make the jobs and Serve pool handoff an explicit fenced contract;
- retain the officially supported SQLite Serve path until a separate
  dialect-capable runtime or product deprecation closes its ledger row.

#### M5 fenced pooled-worker coordination

This subsection is the canonical design for the first M5 runtime seam. It is
not permission to activate the seam until every staged capability and rollout
gate below is satisfied. Early releases may shadow central-PostgreSQL worker
assignment and ordinary queue-driven retirement, but no pool may enter version
`1` until every destructive admission source closes the exact worker first.
SQLite retains its characterized behavior until a later reviewed promotion.

The current race crosses two nominal owners. Managed-job placement takes a
per-service filesystem lock, reads a replica snapshot, selects a worker, and
persists the selected heterogeneous resources and worker name through separate
transactions. Queue autoscaling independently reads nonterminal job counts,
selects an idle replica, and later schedules teardown. An assignment can
therefore commit after the autoscaler's idle snapshot while teardown commits
against the same worker. The filesystem lock does not serialize distinct HA
processes and it does not cover retirement admission.

dstack's useful pattern is the split between slow candidate discovery and a
short authoritative transaction: select from a snapshot, lock the durable
rows, recompute suitability, and atomically reserve. Its durable worker token
and heartbeat are not copied literally. A token check after an external effect
does not prevent a stale worker from producing that effect. SkyPilot instead
uses one random durable operation key through the central assignment, API
request, and remote Skylet submission so a stale controller can only re-drive
the same effect and cannot publish a different binding.

##### Responsibility and public contract

`PoolWorkerCoordinator` is a provider-neutral lifecycle owner, not a new
provider interface or a generic workflow framework. Its pure DTOs and ranking
live in a low-state module that imports no `serve_utils`, `replica_managers`,
`jobs.state`, SDK, server implementation, or provider object. A storage adapter
exposes three closed, typed decisions:

- `try_assign_job()` returns `ASSIGNED`, `NO_CAPACITY`, `RETRY`, `FENCED`, or
  `INCOMPATIBLE` plus an immutable assignment intent only for `ASSIGNED`;
- `try_close_worker_admission()` accepts `GRACEFUL` or `FORCED` and returns
  `ADMITTED`, `BUSY`, `RETRY`, `FENCED`, or `INCOMPATIBLE` plus an immutable
  retirement intent only for `ADMITTED`; and
- `try_release_assignment()` returns `RELEASED`, `RETRY`, or `FENCED` after an
  exact remote attempt reaches a terminal or proved-absent state.

None of these methods raises normal scheduling outcomes as exceptions. None
calls a provider, the API server, Skylet, SSH, `sdk.exec`, `sky.down`, or a
thread join while its database transaction is open. Provider and remote
runtime objects remain observations passed into pure candidate planning. The
coordinator owns only admission, durable intent, exact identity checks, and
state transitions.

Assignment and every destructive admission source must use the same
coordinator before the first behavior promotion. Migrating only assignment or
only ordinary queue retirement does not close the race and is forbidden.
Queue scale-down and retirement of a live cost-rebalance incumbent use
`GRACEFUL`, which refuses any live binding. Cleanup of a failed cost-rebalance
replacement, purge, update replacement, service down, preemption, launch
failure, user-job failure, readiness failure, and failed-cleanup re-drive use
`FORCED` only after their existing domain transition proves that the worker
must leave service. `FORCED` closes admission even when a prior assignment
still needs managed-job recovery; it does not silently declare that remote
attempt absent.

The outer capable-code authority check lives in
`execute_coordinated_pool_termination()`, below `_terminate_replica()` and
immediately above the permanent coordinated teardown effect. Failed-service cleanup,
orphan cleanup, controller cleanup, and failed-launch compensation must create
the same typed request before reaching it. A version-1 call carries the exact
service hash, worker incarnation, replica ID, cluster name, close token,
retirement operation ID, identity digest, the existing global `cluster_hash`,
provider-target digest, and provider-object digest. Immediately before every
`core.down()` retry, the
adapter resolves both the exact closed replica row and its permanent
physical-target row and rejects absent, stale, reopened, retired, or mismatched
authority. It then passes an internal expected-target value through `core.down()`
to the backend, where the final check occurs after handle refresh and
immediately before the provider teardown effect. A coordinated replica row
with no parent service is quarantined and cannot be terminated from cluster
name alone.

The common adapter selecting handle-backed `core.down()` or the S4-qualified
target-native close path is the retained permanent effect boundary and is not
a removal locator for this seam. M5-S3 moves non-pool and SQLite direct
behavior into explicitly named compatibility helpers. The legacy direct calls
recorded by PLA-M5-016 therefore disappear from their current owning symbols,
while `_terminate_replica()` may remain as a router whose pool branch can only
carry a closed intent. Its caller list is coverage inventory and test input,
not an impossible call-presence removal gate.

The storage-level `remove_replica()` and `remove_replicas()` boundaries
similarly require the exact close token, operation ID, identity digest, zero
capacity-consuming assignments, matching global `cluster_hash`, matching
provider-target and provider-object digests when an object was observed, and
same-call exact absence plus
no-later-create proof for coordinated workers. Service deletion requires zero
coordinated child rows.
This covers bulk deletion of absent logical launches and never-started or
placement-benched rows as well as normal teardown completion. Version-0
callers retain their characterized signatures. Coordinated callers use
distinctly named removal adapters, so the exact direct legacy calls in
PLA-M5-016 can reach `removed` without deleting the retained general-purpose
storage primitives.

##### Durable identity and additive schema

The coordinator reuses the authoritative `services`, `replicas`, `job_info`,
and `spot` rows. It does not add mirror inventory or assignment tables. Serve
revision `033`, spot-jobs revision `027`, and API-requests revision `005` add
only additive dormant schema. The permanent target and launch-attempt tables
remain empty until a version-1 worker birth, and no version-0 decision reads
them.

`services` adds:

- `pool_coordination_version INTEGER NOT NULL DEFAULT 0`, where `0` is legacy,
  `-1` is a fenced transition, and `1` is this exact contract;
- `pool_coordination_transition_direction TEXT NULL`, whose only values are
  `ACTIVATING` and `DEACTIVATING`; and
- `pool_coordination_transition_operation_id TEXT NULL`, a canonical random
  UUID that makes one transition and its crash recovery unambiguous; and
- `pool_coordination_transition_parent_operation_id TEXT NULL`, used only when
  an incomplete activation is durably handed to deactivation;
- `pool_coordination_origin TEXT NOT NULL DEFAULT 'LEGACY'`, whose other closed
  value is `COORDINATED_V1`; and
- `pool_coordination_birth_operation_id TEXT NULL`, the immutable canonical
  random UUID of a coordinated service birth; and
- `pool_coordination_scope_uuid TEXT NULL`, the immutable server-minted
  physical namespace for that coordinated service incarnation; and
- `pool_coordination_birth_publication_state TEXT NULL`, whose coordinated
  values are `PENDING`, `COMPLETE`, `FAILED`, or `ABORTED` and which is null
  for legacy services.

Version `-1` requires transition direction and operation, while versions `0`
and `1` require direction and operation null; the parent is normally null.
Version `0` additionally requires origin `LEGACY` and null birth operation,
scope, and birth-publication state. Origin `COORDINATED_V1` permits only
versions `-1` and `1`; a
coordinated row is deleted by its marked terminal deactivation transaction and
is never persisted as a legacy-compatible version-0 row.
Direction and operation ID are immutable until the same marked transition
owner reaches its terminal version, except for one guarded
`ACTIVATING -> DEACTIVATING` abort handoff. That handoff mints a fresh operation
ID, stores the activation operation as parent, proves no version-1 request or
effect was admitted, and is irreversible. A recovery process must resume the
stored direction and operation; it may not infer activation versus deactivation
from partially installed local fences. Activation clears the transition fields
on entry to version `1`; terminal deactivation deletes the coordinated service
row while its permanent scope and target tombstones retain the identity.
The birth-publication state starts `PENDING`, remains `PENDING` across the
activation commit to version `1`, and is then advanced exactly once to
`COMPLETE`, `FAILED`, or `ABORTED` by the birth owner. `ABORTED` is used only by
the guarded activating-to-deactivating handoff. A service at version `1` with
`PENDING` publication retains the immutable birth request as its sole recovery
owner even though its transition fields are null.

Migration assigns every existing service `LEGACY` with null birth operation,
scope UUID, and birth-publication state.
Only the capable coordinated-create owner may insert `COORDINATED_V1`; that
same insert must set version `-1`, direction `ACTIVATING`, transition and birth
operation to the same UUID, a non-null scope UUID, and birth-publication state
`PENDING` before any
controller, worker, or provider admission exists. Origin, birth operation, and
scope are immutable. No update may move a `LEGACY` row to `-1` or `1`, no
update may move a coordinated row to version `0`, and no ordinary delete may
remove a coordinated row.
Existing pools therefore remain version `0`; the first promotion applies only
to a fresh coordinated service incarnation.

Serve `033` also adds a permanent `pool_coordinated_scopes` allocation registry
keyed by `pool_coordination_scope_uuid`, with birth operation, initial service
name and hash, creation time, closing operation, and state `ALLOCATED` or
`CLOSED`. The initial service name is indexed but is not unique because a
later capable birth may reuse the logical name with a new scope. The API server
generates the scope UUID with cryptographic randomness before request creation;
the birth transaction inserts the allocation and service together, and no row
is ever deleted, reopened, or rebound. The exact final deactivation transaction
makes the only `ALLOCATED -> CLOSED` transition and stores its operation ID.
A `CLOSED` row is a permanent physical-scope and stale-authority tombstone.
Service, request, cluster-record, and child-writer triggers consult it even
after the service row is deleted and reject any recreation or mutation for
that physical scope. For its logical name they reject every legacy or untyped
service birth and every untyped mutating request, including inserts from an old
binary, because those rows cannot distinguish the closed incarnation from a
later one. They permit read-only requests, the typed batch-v1 parent, and one
fresh typed coordinated birth that reserves a new scope while the logical name
has no current service or nonterminal transition reservation. Every physical target
name, provider idempotency
key, target identity, worker bootstrap marker, and provider tag or equivalent
includes a length-safe encoding of this scope. Freshness therefore does not
pretend that deleted service history is still queryable. The public logical
name must be currently unused, but closed scopes with that historical name do
not block a fresh typed birth. Safety comes from the durable scope tombstones,
the permanent ban on untyped mutation of a name after its first coordinated
incarnation, the never-reused server-minted physical namespace, exact
birth-request binding, and the pilot provider's
authoritative proof that no object exists in that new namespace.
An old provider call admitted before the scope was minted cannot address it.

`replicas` adds:

- `pool_worker_incarnation TEXT NULL`, a canonical random UUID assigned once
  at a fresh version-1 worker row birth;
- `pool_worker_fence_operation_id TEXT NULL`, the stable local lifecycle-fence
  operation UUID;
- `pool_worker_fence_identity_digest TEXT NULL`, the complete immutable worker
  fence identity digest;
- `pool_worker_fence_receipt_digest TEXT NULL`, written only after exact local
  `FENCED` readback and retained until the replica row is deleted after exact
  worker absence;
- `pool_launch_operation_id TEXT NULL`, the stable private launch request UUID;
- `pool_launch_payload_digest TEXT NULL`;
- `pool_launch_identity_digest TEXT NULL`; and
- `pool_launch_state TEXT NULL`, whose closed set is `INTENT`,
  `EFFECT_STARTED`, `READY`, `QUARANTINED`, and `ABSENT`;
- `pool_admission_closed BOOLEAN NOT NULL DEFAULT FALSE`;
- `pool_retirement_token TEXT NULL`; and
- `pool_retirement_operation_id TEXT NULL`, a separate canonical random UUID
  used as the stable cleanup operation and, when an object exists, internal
  down-request ID. It is never the close token;
- `pool_retirement_transition_operation_id TEXT NULL`, immutable typed
  provenance for the deactivation transition that admitted this close. It is
  null when the close committed while the service was active version `1` and
  equals the service transition operation when the close committed at
  `-1/DEACTIVATING`;
- `pool_retirement_identity_digest TEXT NULL`, the complete immutable
  `PoolWorkerRetirementIdentityV1` digest prepared by every close transaction;
  and
- `pool_down_identity_digest TEXT NULL`, prepared only after a matching
  provider object and global cluster record make a coordinated down request
  meaningful.

The worker identity is `(services.hash, pool_worker_incarnation)`. Replica ID
remains a checked logical attribute and may be reused after deletion. Physical
cluster name is a permanently non-reusable alias, not the identity itself.
Before a version-1 replica row is born, slow optimizer and provider-offer
discovery resolves one exact candidate provider, account scope, region, zone,
realized provider mode, and resource shape without a provider mutation. The
row-birth transaction revalidates that prepared candidate and capability, then
binds it into the target identity. If no candidate exists, no replica or target
row is inserted. A different provider, placement, mode, or resource fallback is
a new worker birth after the old target is formally retired; version 1 never
changes candidate identity under one launch operation. This is the same
snapshot-then-authoritative-reservation split used by the assignment planner,
not a second optimizer inside the transaction.

Serve `033` adds a permanent `pool_worker_physical_targets` registry keyed by
cluster name, with unique worker incarnation, service hash, replica ID, launch
operation and identity digest, nullable retirement operation and identity
digest, nullable retirement-transition operation, non-null provider-target
digest, nullable global cluster hash and
provider-object digest, nullable provider/gateway no-later-create receipt ID
and digest, and state `RESERVED`, `LIVE`, `QUARANTINED`, or `RETIRED`. Row birth
reserves a canonical length-safe name derived from the coordination-scope UUID
and random worker UUID and prepares the complete provider-target digest in the
same transaction as the replica. Before provider entry, the executor allocates the global
`cluster_hash`, publishes it to the permanent target, and rechecks both
identities. Exact provider acceptance or authoritative discovery writes the
immutable provider-object digest once; exact cluster-record and handle
publication then advances the target to `LIVE`. A lost or ambiguous create
advances it to `QUARANTINED`, never to absence, and may have a null object
digest until authoritative discovery resolves one. Only exact external absence
plus the provider/gateway no-later-create receipt advances it to `RETIRED`.
Registry rows and their request locators are never deleted or rebound. A
recovery sweep owns every non-retired target and closes any matching late
create while the service, replica, and typed down authority remain retained.
After `RETIRED`, the receipt is the formal proof that no old caller can create;
a violated receipt is an external provider/gateway invariant alarm, not
authority to synthesize an unowned teardown after service deletion. PostgreSQL
request and cluster-record writer triggers
reject any public, legacy, or mismatched launch of a reserved, quarantined, or
retired name even after the service and replica rows are gone. A partial unique
index also rejects duplicate
non-null worker incarnations on live replicas. Ordinary replica upserts must
never overwrite an existing worker incarnation, worker-fence operation,
identity or receipt digest, launch operation, payload, identity or state,
admission fence, retirement token, operation ID, retirement identity, or down
identity digest from an `excluded` row. The replica and permanent-target copies
of retirement-transition provenance are written together and must match.

`pool_down_provider_target_digest` and the permanent target's corresponding
field are the SHA-256 digest of canonical `PoolProviderTargetIdentityV1` bytes,
not a digest of an arbitrary provider config dictionary. The common envelope
contains its schema version, the qualified actuation implementation digest,
canonical provider name, a non-secret credential and resource-scope digest,
normalized region and zone or equivalent placement scope, cloud-local cluster
name, coordination-scope UUID, launch operation ID, and one closed
provider-native target locator. The
locator identifies the stable teardown namespace or selector, not a mutable
snapshot of current node IDs or status. Each promoted provider supplies only a
pure projector and canonical locator schema; the common lifecycle code owns
storage, comparison, fencing, and failure policy. M5-S4 must specify the pilot
provider's exact locator fields and authoritative reconstruction rules before
that provider can advertise the capability. This target-scope and idempotency
digest is prepared before any provider entry, is non-null from `RESERVED`, and
binds `NEVER_ACCEPTED` and absent closure receipts even when no provider object
ever exists.

`PoolObservedProviderObjectIdentityV1` is separate canonical evidence produced
by the qualified authoritative query. It binds the provider-target digest to
the provider-native immutable object or group locator that an actual stop or
terminate effect will address. Its SHA-256 `provider-object digest` is null
until an object is accepted or discovered and then immutable. The global
`cluster_hash` remains a separate required SkyPilot generation identity and is
allocated and persisted before provider entry. A matching object requires both
hashes and both digests for coordinated down; a never-accepted or proved-absent
attempt can close directly with its target digest, attempt evidence, exact
absence, and no-later-create receipt without inventing a handle or down request.

Serve `033` also adds append-only `pool_worker_launch_attempts`, keyed by
`(pool_launch_operation_id, api_request_execution_generation)`. Each row binds
the permanent target, request claim and capable owner. Its identity columns are
immutable, while guarded closure columns move monotonically through `ADMITTED`,
`PROVIDER_ENTERED`, `ACKNOWLEDGED` or `AMBIGUOUS`, and `FENCED`; its nullable
receipt ID and digest may move once from null to one exact verified value. The
current `api_requests.execution_generation` may advance, but attempt identity
and closed evidence are never reset or deleted. `ACKNOWLEDGED` and `AMBIGUOUS`
are observations, not entry permission. Every generation that reached
`PROVIDER_ENTERED` must advance to `FENCED` with a verified generation-closing
receipt before a later generation may enter the provider. If an acknowledged
matching object is adopted, the same generation continues and no new create
generation enters. A generation that never reached provider entry may advance
from `ADMITTED` to `FENCED` only with issuer evidence for `NEVER_ACCEPTED`.
Final target retirement requires a fenced receipt for every generation plus an
operation-closing receipt. This is an effect-attempt ledger, not a second worker
inventory: the replica and permanent target remain the only lifecycle owners.

`pool_worker_launch_receipts` is an append-only verification store, not a bare
digest assertion. `NoLaterCreateV1` canonical bytes contain schema version,
issuer implementation digest and key ID, canonical provider name, non-secret
credential and resource-scope digest, launch operation ID, provider-target
digest, highest closed execution generation, close scope `GENERATION` or
`OPERATION`, resolution `NEVER_ACCEPTED`, `TERMINAL_MATCH`, or
`TERMINAL_ABSENT`, accepted-call-set digest, and issuer timestamp. The store
retains those bytes, signature, verification result, and database verification
time. Only an allowlisted actuation issuer key may sign; rotation adds a key and
retains old verification keys until every dependent target is retired. Receipt
IDs, bytes, signatures, attempt closure, and target closure are immutable or
monotonic under database triggers. They are redacted from ordinary request and
event output.

Version-1 worker identity is allocated at durable row birth, before external
launch begins, in the same transaction that inserts the replica row. Recovery
of that same row and exact external cluster preserves the UUID. A retry that
has not created an external cluster also preserves it. A replacement may mint
a new UUID only after exact absence of the prior external cluster is proved and
the old row is deleted; reinserting a reused replica ID is a new birth. A
nonnull incarnation is immutable in place. Fresh version-1 rows without one
are rejected. Every version-1 row is a new coordinated birth and requires a
canonical worker-fence operation UUID and identity digest plus a canonical
launch operation UUID, payload digest, identity digest, and non-null launch
state from row birth. READY additionally requires the exact FENCED receipt
digest. Legacy version-0 rows remain nullable and can never become version-1
rows. A fresh coordinated service starts with no replica inventory, and every
later replica is born with the complete version-1 identity instead of being
backfilled. Worker-fence identity fields are immutable for the row lifetime,
while the receipt may move from null to its one exact digest before READY. For a
coordinated row, `pool_admission_closed = FALSE` requires null retirement
token, operation ID, retirement identity, and down identity fields. `TRUE`
requires a canonical non-null token, operation ID, and SHA-256 retirement
identity digest. Its retirement-transition operation is null exactly when the
close transaction observed active version `1`; when that transaction observed
`-1/DEACTIVATING`, it is non-null and equals the locked service transition
operation. A later service transition never rewrites the null provenance of an
already closed worker. The down digest remains null until an object is observed
and then moves once to the exact `InternalPoolDownIdentityV1` digest. All close
fields are immutable or one-way after close.

Version-1 provisioning does not reuse a worker row across ambiguous failed
provider attempts. Failed-launch compensation force-closes the never-ready row
and uses its exact coordinated-down request only when authoritative discovery
has produced a provider-object digest. A never-accepted attempt, or one proved
absent before object publication, closes without a synthetic down request only
after every attempt generation is fenced and the operation-closing receipt is
verified. It may delete the replica and permit a new durable birth only after
the permanent target reaches `RETIRED`. An absence observation alone is
insufficient. A target that cannot prove no later create remains `QUARANTINED`
with its replica, launch request, and cleanup owner retained. A retry after
proved retirement is a new durable birth with a new worker incarnation and new
permanently reserved name. This
replaces the current in-function `launch_cluster()` cleanup-and-retry loop for
coordinated pools and prevents a stale cleanup for an earlier attempt from
destroying a later READY worker under the same identity. Version-0 launch
retry remains characterized until M5-S3.

The row-birth transaction prepares the complete private launch identity before
any request or provider effect and sets `pool_launch_state = INTENT`.
Immediately before entering the retained launch boundary, the capable executor
CASes that exact row to `EFFECT_STARTED`. It may publish `READY` only after the
matching cluster handle and provider identity are durable, the exact Skylet
runtime is live, and the local worker lifecycle fence has been installed and
read back. `QUARANTINED` consumes the row and permits no second logical launch.
`ABSENT` requires both the provider-specific exact-absence proof and the
permanent target's no-later-create proof. No state or retry may replace the
operation or any launch digest in place.

Every replica inserted while its service is version `1` must reserve its
permanent physical target and carry all launch fields in `INTENT`; it cannot be
READY or use a nullable legacy exception. READY publication requires the
matching launch operation, payload and identity, `LIVE` physical-target row,
durable cluster handle and provider identity, and FENCED receipt in the same
marked CAS. Assignment admission independently requires those same fields.

`job_info` adds:

- `pool_worker_incarnation TEXT NULL`;
- `pool_assignment_operation_id TEXT NULL`;
- `pool_assignment_payload_digest TEXT NULL`;
- `pool_assignment_identity_digest TEXT NULL`;
- `pool_assignment_precondition_deadline FLOAT NULL`;
- `pool_assignment_containment_profile TEXT NULL`;
- `pool_assignment_containment_plan_digest TEXT NULL`;
- `pool_cancel_operation_id TEXT NULL`;
- `pool_cancel_identity_digest TEXT NULL`;
- `pool_assignment_state TEXT NULL`.

The closed non-null state set is `INTENT`, `BOUND`, `QUARANTINED`, and
`RELEASED`. `INTENT`, `BOUND`, and `QUARANTINED` consume worker capacity and
block retirement. `RELEASED` retains routing history but grants no authority.
`job_info.pool_hash` remains the service-incarnation binding and
`job_id_on_pool_cluster` remains the exact remote job ID after binding. A
partial lookup index covers `(pool, pool_hash, pool_worker_incarnation)` for
the three capacity-consuming states.

Additive checks require every non-null assignment state to have a service hash,
worker incarnation, operation ID, payload digest, complete internal identity
digest, finite absolute precondition deadline, qualified containment profile,
and complete containment plan digest. `INTENT` requires a null
remote job ID and null cancel fields. `BOUND` requires a non-null remote job ID,
canonical random cancel operation UUID, and complete cancel identity digest.
For `QUARANTINED` and `RELEASED`, remote job ID and both cancel fields are
either all null or all non-null, preserving whether a remote binding ever
existed. Reassignment from `RELEASED` clears the prior remote ID and cancel
fields in the same transaction that writes the new `INTENT`. Legacy rows with a
null assignment state continue to satisfy the checks only on version-0
services. Fresh coordinated birth rejects every currently retained managed-job
binding for the public name. A deleted historical binding is harmless because
the new service hash, coordination scope, worker incarnation, and physical
namespace are all distinct, so a version-1 service never inherits its authority.

API-requests revision `005` adds nullable
`internal_identity_version INTEGER` and `internal_identity_digest TEXT` fields
to `api_requests`, with a check that both are null or both are non-null. It also
adds nullable `pool_down_authority_version`, `pool_down_service_name`,
`pool_down_service_hash`, `pool_down_replica_id`,
`pool_down_worker_incarnation`, `pool_down_cluster_hash`,
`pool_down_provider_target_digest`, `pool_down_provider_object_digest`,
`pool_down_retirement_token`, `pool_down_operation_id`, and
`pool_down_transition_operation_id` projections. The last field is
provenance-based, not derived from the service's current version: it is null
iff the immutable retirement row says the close committed while the service
was active version `1`, and it equals that row's
`pool_retirement_transition_operation_id` iff the close committed during
deactivation. A later `1 -> -1/DEACTIVATING` transition does not rewrite an
already created request or make its null projection invalid. It further adds nullable
`pool_launch_service_name`, `pool_launch_service_hash`,
`pool_launch_replica_id`, `pool_launch_worker_incarnation`,
`pool_launch_worker_fence_operation_id`, and `pool_launch_operation_id`
projections; plus nullable
`pool_cancel_assignment_operation_id`, `pool_cancel_service_hash`,
`pool_cancel_worker_incarnation`, `pool_cancel_remote_job_id`, and
`pool_cancel_operation_id` projections; plus nullable
`pool_transition_service_name`, `pool_transition_scope_uuid`,
`pool_transition_direction`, `pool_transition_operation_id`, nullable
`pool_transition_parent_operation_id`, and nullable
`pool_transition_parent_batch_request_id` parent-request projections; plus
nullable `pool_down_batch_version` and `pool_down_batch_payload_digest`
projections on the caller-visible `JOBS_POOL_DOWN` parent. The launch and
cancel groups are each either all null or all non-null. The down group follows
the same rule except for its provenance-conditional transition field. The
transition service, scope, direction, and operation fields are all null or all
non-null. An activating birth has both transition-parent fields null. A
deactivation has exactly one origin: activation abort stores the immutable
birth operation in `pool_transition_parent_operation_id`, while public down
stores the first immutable creator batch in
`pool_transition_parent_batch_request_id`. The two batch projection fields are
both null or both non-null. Every effect or transition operation ID must equal
`request_id`.
Keeping typed projections outside JSON gives PostgreSQL triggers a stable
comparison surface. The signed internal worker launch, exec, exact cancel, and
coordinated-down create paths store version `1` and a SHA-256 digest of their
complete canonical identity tuple in the same transaction that creates the
request and queue row. The coordinated birth and deactivation requests likewise
store their exact service, direction, scope, operation, typed origin, and
identity digest.
Ordinary and legacy rows keep all internal fields null. Identity and authority
projections are immutable. The digest and typed
projections remain on `api_requests` after terminal execution removes its queue
row, so a late lost-response retry never depends on deleted priority or
precondition columns.

Revision `005` also adds durable `pool_down_batches` and
`pool_down_batch_targets` tables. The batch row is keyed by the caller-visible
`JOBS_POOL_DOWN` request ID and retains the authenticated normalized selector,
immutable payload digest, state `UNRESOLVED`, `RESOLVED`, `RUNNING`, or
`TERMINAL`, one controller-execution selection linearization time, next ordinal,
terminal result, and nullable first hard error.
Target rows are keyed by `(batch_request_id, ordinal)` and uniquely bind one
resolved public name, target kind `SERVICE` or `LEGACY_ORPHAN`, nullable service
hash, coordination scope and observed coordination version, nullable immutable
legacy-orphan inventory digest, nullable route `LEGACY_V0_DOWN`,
`COORDINATED_V1_DEACTIVATE`, or `POLICY_SKIP`, nullable child request ID, state
`PREPARED`, `CHILD_ADMITTED`, or `TERMINAL`, structured outcome, message
fragment, and nullable serialized hard error. `PREPARED` requires null route,
child, and outcome. A `SERVICE` target requires its selected hash and observed
version and a null orphan digest. `LEGACY_ORPHAN` is legal only for purge,
requires null service identity and one SHA-256 digest over the sorted raw child
lifecycle epochs, replica and cluster identities, and resource scopes selected
by the existing orphan query. At its ordinal, one transaction revalidates the immutable
selected identity and current sequential policy. A soft skip writes
`POLICY_SKIP`, no child, and `TERMINAL`; a destructive route either mints and
stores one canonical random child UUID or joins the already admitted exact
coordinated child. A legacy route writes `CHILD_ADMITTED` while its short
one-name compatibility child executes. A coordinated route atomically admits
or joins the child, commits or observes the exact service
`1 -> -1/DEACTIVATING` transition, stores the characterized scheduled outcome,
retains the child reference, and writes `TERMINAL` immediately; public completion never
waits for provider absence or final service deletion. A terminal destructive
route retains its child. The coordinated child ID, not the batch ID, is the service
transition operation. Target identity and ordering are immutable after
preparation; route is write-once and target and aggregate states advance
monotonically. A coordinated child
may be referenced by more than one authenticated purge batch when concurrent
callers select the same already-owned transition; legacy children are
parent-specific and never deduplicated across public calls. A batch contains a
service at most once. This is request
progress and result aggregation, not a second pool inventory.

No second role-capability column is added. The exact releases advertise
separately versioned internal-worker-launch, internal-exec, exact-cancel,
coordinated-down, and coordinator capability tokens, including their
implementation digest, through the existing
`api_server_instances.supported_handlers` and
`supported_payload_versions` fields. Old processes publish neither token and
therefore block activation.

All UUID text is parsed and re-emitted in canonical RFC 4122 form before use.
Malformed, nil, predictable, or mismatched values fail closed. The retirement
token is a durable database coordination nonce, not an authentication
credential. The typed down payload necessarily persists that exact nonce so
the request can be re-driven, but request serialization, exceptions, events,
and logs always redact it. Credentials, HMACs, user emails, provider account
names, and auth-file paths are not stored in these fields.

##### Stable request and remote effect key

The caller generates the random assignment operation UUID and one absolute
precondition deadline before planning so the stable run timestamp and
candidate-specific payload can include them. The deadline is validated against
the database clock when committed and then stored on `job_info`; recovery never
recomputes it from `time.time()`. The UUID and deadline gain authority only
when committed with the `INTENT`, before any HTTP or remote job effect. The
UUID is simultaneously:

1. the pool assignment operation ID;
2. the private internal `/exec` request ID; and
3. the Skylet submission key.

File mounts are uploaded once before candidate ranking. After a pure planner
proposes one exact worker, the SDK derives an immutable candidate-specific
`ExecBody` outside the coordinator transaction by adding the selected cluster
name, then computes its canonical effect-bearing payload digest and complete
`InternalPoolExecIdentityV1` digest. The transaction stores both digests. If
locked revalidation rejects the proposal, the caller returns to ranking and
derives a new body from the already uploaded mount identity. Retrying an
accepted intent must rebuild both exact digests or return `QUARANTINED`; a
retry may not silently submit changed work, producer metadata, scheduling
semantics, file-mount identity, or deadline under the same operation ID.

Private `/exec` accepts the stored absolute deadline directly. It does not call
the public timeout-to-deadline serializer, which derives a fresh wall-clock
value on every request. The request identity digest therefore remains exact
across response loss, controller restart, queue deletion, and later duplicate
lookup.

Controller generation headers remain fencing metadata and never authorize this
private mode. Release M5-S1 provisions a dedicated internal-controller HMAC
key to API and controller roles only. Before signing, the controller builds the
complete `InternalPoolExecIdentityV1` tuple below and computes its canonical
identity digest. It signs method, path, authenticated user ID, operation UUID,
that full identity digest, current leader UUID and generation, and a bounded
timestamp. The payload digest and absolute precondition deadline are therefore
covered through the signed identity, not merely accepted and recorded on first
insertion. After ordinary authentication, the `/exec` handler reconstructs the
tuple, verifies the identity digest, HMAC, signed user, current PostgreSQL
leadership, and timestamp, then validates a canonical UUID and replaces the
middleware's random ID. The normal public SDK and user-creatable service
accounts cannot obtain the key or override request IDs. Signatures are never
logged or persisted and support a two-key rotation window. The response must
echo the exact accepted ID. A mismatch is an incompatible-server failure, not
permission to retry with a fresh ID.

PostgreSQL request creation becomes create-or-return only for this signed
internal mode. `InternalPoolExecIdentityV1` is the canonical equivalence tuple:
authenticated user; request and handler names; payload type, format, version,
producer version, and canonical JSON; exact service name and hash, replica ID,
worker incarnation, worker-fence operation ID and identity digest; assignment
operation ID; execution class; qualified containment profile and complete
ordered owner-plan digest; target cluster; schedule type; queue priority and
retry flags; ignore-return-value flag; file-mount blob identity; and
precondition type, payload, and deadline. On a duplicate ID, every tuple field
must match. An exact match returns the existing request only when the
recomputed identity digest equals the immutable digest on `api_requests`; it
never relies on the possibly deleted queue row and never overwrites or enqueues
again. Any mismatch returns `409`, preserves the original row, and quarantines
the pool intent. This HTTP create-or-return path is lookup, not request
recovery: an exact duplicate returns the existing active or terminal result and
never resurrects delivery. SQLite and ordinary API requests keep create-once
behavior.

A fourth non-interchangeable identity owns version-1 worker creation. The row-
birth transaction mints `pool_launch_operation_id`; that UUID is the private
request ID. `InternalPoolWorkerLaunchIdentityV1` contains authenticated
internal user; request and handler names; the closed launch payload type,
format, version, producer version, and complete canonical JSON; exact service
name and hash; replica ID; cluster name; worker incarnation; worker-fence
operation ID and digest; launch operation ID; exact task, resources, workspace,
mount identity, backend, optimizer and setup options; fixed `dryrun=false`,
`down=false`, `idle_minutes_to_autostop=null`, and
`is_launched_by_sky_serve_controller=true`; long execution class; schedule,
priority and retry policy; ignore-return-value flag; and the exact durable
service-replica launch precondition. The closed private constructor rejects
generic `LaunchBody` extras and stores every effect-bearing field in its
canonical JSON. The controller signs and the handler verifies this tuple under
the same HMAC protocol, with a distinct method, path, handler, and capability
token.

The private launch route is create-or-return and `ReplayPolicy.RECONCILE`; it
never falls back to public `/launch`. Insert and claim join the typed
projections to the one open replica row. New request insertion requires
`INTENT`; recovery claims for that same immutable request may also join
`EFFECT_STARTED`, but no other state. Immediately before provider entry the executor revalidates the row, capability and
leadership, appends its exact execution-generation attempt, and CASes the row to
`EFFECT_STARTED`. Recovery then invokes the shared provider launch reconciler
with the exact operation identity. A matching durable cluster handle plus
provider identity is adopted. A negative resource query after effect start is
observation only and never by itself permits another create or `ABSENT`.
Re-drive requires exact absence plus valid no-later-create receipts for every
earlier provider-entered generation; any receipt gap, foreign resource, changed
generation, or ambiguous effect quarantines the row. A provider-specific
actuation owner must propagate and query the operation identity and issue the
formal receipt; an ordinary provider adapter may not self-assert it. After launch, the worker remains
nonassignable until Skylet v41 is live and the exact local lifecycle fence is
installed and read back; only that sequence permits `READY`. Response loss,
executor death, and a stale request after row replacement therefore adopt one
matching worker, retry only after prior generations are formally fenced, or
quarantine without a second effect.

Here, provider support is stronger than operation tags plus a point-in-time
list. The qualified actuation owner is the sole holder of pooled-worker create
authority for its provider scope. It records every internal SDK retry or
accepted outbound call in the attempt's accepted-call-set before allowing it
through a generation-fenced egress boundary. Its create primitive must accept a
provider-native idempotency key or be mediated by that boundary, and the
authoritative query must resolve the same key. A canonical provider or gateway
`NoLaterCreateV1` receipt is bound to launch operation, provider-target digest,
and the highest closed execution generation. It formally proves that every
accepted or in-flight call through that generation is terminal and that every
old caller generation is dead or revoked. Process or pod death, elapsed time,
and a negative list result are not substitutes unless the promoted provider
contract itself makes them the signed equivalent of that receipt. The next
execution generation uses the same immutable create key only after all earlier
generations are covered. `RETIRED` additionally requires an operation-closing
receipt proving that no launch delivery, claim, provider call, or callable
credential can still create. A provider with tag search but no such formal
fence cannot satisfy this contract and keeps the target `QUARANTINED`; it is not
enabled for version `1`.

This document defines the consumer and verification contract but names no
current issuer or eligible provider. M5-S4 must update the canonical design with
one concrete provider protocol and its sole-credential/egress enforcement
before any activation. A normal in-process adapter, provider tag, timeout, or
operator assertion cannot mint a receipt.

The permanent target row anchors the retained late-create cleanup owner while
the service and replica remain present, retains the launch request locator and
exact provider identity, and is scanned independently of autoscaler demand. A
lease-lost owner that resumes after another owner sees absence therefore either
hits the same provider idempotency key, is rejected by the actuation fence, or
creates a resource that the retained target owner immediately closes. The
replica cannot be deleted and the target cannot become `RETIRED` until the
operation-closing receipt rules out that last case. The target tombstone
survives later logical cleanup only to prevent name and identity reuse.

Retirement and provider down are separate identities. Every close transaction
mints `pool_retirement_operation_id` and stores
`PoolWorkerRetirementIdentityV1`: exact birth scope, service and worker
identity, replica and physical target, provider-target digest, close mode,
retirement token and operation, and any admitting deactivation operation. That
identity exists even when no provider object was ever accepted. If a matching
provider object exists, the same operation UUID becomes the private down
request ID and is safe to expose in normal request diagnostics.
`InternalPoolDownIdentityV1` then contains
the authenticated internal user; request and handler names; the closed private
payload type, format, version, producer version, and canonical JSON; exact
service name and hash; worker incarnation; replica ID; cluster name;
global `cluster_hash`; provider-target digest; provider-object digest;
retirement operation ID; retirement token; nullable service-transition
operation ID matching the immutable retirement-transition provenance; fixed `terminate=true`,
`purge=false`, `graceful=false`, `graceful_timeout=null`, and
`user_initiated=false` semantics; normal execution class; short schedule;
queue priority and retry flags; ignore-return-value flag; and null
precondition. The private payload constructor does not expose setters for
those five fixed teardown options. Its executor passes them explicitly to the
retained `core.down()` boundary and rejects any generic `StopOrDownBody` or
deserialized field outside the closed schema. The object-discovery transaction
stores the down digest before any request is sent. The controller must
reconstruct the same digest, then signs method, path, user, operation ID, that
digest, current leader UUID and generation, and bounded timestamp. The full
signed message therefore covers leadership and freshness, while those changing
values are not part of immutable request equivalence. Leadership may change
without changing request identity. The handler reconstructs and verifies the
full tuple and stored digest before create-or-return. The raw token is redacted
from every diagnostic surface. A public `/down`, a signed exec identity, a
generic down payload, or a down identity with one changed field cannot create
the request. A retirement with no observed object creates no down request and
can finish only through the fenced attempt and operation-closing receipt path.

Cancellation uses a third non-interchangeable private identity. The `BOUND`
transaction that first stores the exact remote job ID also mints a random
`pool_cancel_operation_id` and stores the digest of
`InternalPoolCancelIdentityV1`: internal user; request, handler, payload, and
producer versions; service hash; worker incarnation; cluster name; assignment
operation ID; one exact remote job ID; cancel operation ID; canonical payload
JSON with `job_ids=[exact_remote_id]`, `all=false`,
`all_users=false`, and `try_cancel_if_cluster_is_init=true`; normal execution
class; short schedule; queue and retry flags; fixed keyed-runtime cancellation
policy `TERM_10S_THEN_HARD_KILL_V1`; and null precondition. The
controller signs the full digest plus current leader and timestamp exactly as
above. PostgreSQL create-or-return and claim triggers join the typed projections
to that one current `BOUND` row. A changed `all_users` or
`try_cancel_if_cluster_is_init` value, multi-ID, cancel-all, public, released,
mismatched-worker, or changed-digest cancellation is rejected before queueing.
Repeating cancellation of the same exact remote ID is idempotent. The executor
revalidates its claim and the same exact `BOUND` row immediately before the
remote `CancelJobKeyed` call and never falls back to legacy `CancelJobs` or
SSH; recovery observes exact terminal status before release.

Stable IDs require an explicit middleware contract. The private handler
replaces `request.state.request_id` only after authentication and identity
verification and before preparing the request. `RequestIDMiddleware` reads the
post-handler state for its response header rather than echoing its original
random local variable. No ordinary handler may replace the ID, and a response
whose header differs from the operation ID is rejected by the controller.

Internal worker launch, pool exec, exact cancel, and coordinated down use
distinct registered handler names and `ReplayPolicy.RECONCILE`. Executor lease
loss therefore requeues the exact immutable request instead of terminalizing
an ambiguous mutation or selecting a fresh ID. Reconciliation still passes the
launch attempt fence, Skylet key, exact `BOUND` cancellation identity, or
closed retirement authority checks before any repeated remote effect.

The caller-visible request name remains `JOBS_POOL_DOWN`, but replay ownership
does not vary by row columns. Pre-M5 rows retain stable handler
`sky.jobs.server.core:pool_down` and its existing `ReplayPolicy.NEVER`. A
capable endpoint selects distinct stable handler
`sky.jobs.server.core:pool_down_batch_v1` before request creation; the closed
registry explicitly registers that handler with `ReplayPolicy.RECONCILE` rather
than relying on the module scanner's default. Recovery therefore obtains the
policy from the persisted handler identity exactly as it does today. The v1
handler is safe to reconcile because its frozen next ordinal and every
effectful child are persisted before admission, and it never replays the
monolithic legacy loop. Each version-0 child keeps `ReplayPolicy.NEVER`; an
ambiguous legacy child records a terminal result for that target and no later
child is admitted while the parent remains live. The coordinated child follows
the typed transition contract above.
Concretely, the closed request registry adds
`sky.jobs.server.core:pool_down_batch_v1` to an explicit
`_RECONCILE_HANDLER_NAMES` set checked by `_register_module_handlers()` before
its `READ_ONLY` or default-`NEVER` choice. The existing `pool_down` identity is
not aliased to the new function, so registering builtins cannot overwrite old
metadata or assign two policies to one handler.

These private handlers also have an explicit post-effect failure contract.
Before the first effect-start CAS, a proved deterministic validation failure may
finish `FAILED`. All four private request registrations fix `retryable=true`
and retain their queue row for the entire nonterminal lifetime. After the CAS,
a timeout, response loss, provider exception, or other outcome that does not
prove the domain effect terminal first persists reconcile-required state on the
domain owner, then raises `ExecutionRetryableError`. The existing request
monitor CASes the current `RUNNING` row to `WAITING`; its subsequent
`PostgresQueueBackend.put()` uses the old claim token and execution generation
to clear that claim and return the existing delivery to `queued`. Requeue does
not increment generation. The next capable `get()` increments
`execution_generation` and creates the new claim. A crash between the two
transactions leaves `WAITING` plus the old claimed delivery, which expired-
claim recovery requeues because the handler is `RECONCILE`. A missing queue row
is an invariant violation and is not recreated from lossy defaults.

Each requeue and claim revalidates the domain row, handler capability, current
specialized controller leadership, every preserved launch-attempt generation,
and permanent target where applicable. Generic exception terminalization is
forbidden after effect start. A request may become terminal only after exact
success, a proved pre-effect failure, or a durable domain quarantine that
retains a separate cleanup owner. Terminal rows are never requeued, including
by a duplicate HTTP request.

The coordinated birth and deactivation parent registrations are also
`RECONCILE`, fix `retryable=true`, and retain their one queue delivery until the
owning transaction terminalizes them. They use the same claim-expiry and
old-claim requeue mechanics. Deactivation requires the exact stored transition
operation. Birth recovery accepts exactly either the matching
`-1/ACTIVATING` transition or version `1` with matching immutable birth
operation and birth-publication state `PENDING`; no other version-1 request may
borrow that exception.

Skylet version `41` adds new `GetKeyedSubmissionCapability`, `AddJobKeyed`,
`QueueJobKeyed`, `CancelJobKeyed`, `GetJobBySubmissionKey`, and
`SealSubmissionKeyAbsent` RPC methods. The v41 autostop service separately adds
`InstallPoolWorkerLifecycleFence` and `GetPoolWorkerLifecycleFence`. It does
not add optional authority fields
to existing mutation methods: an old Skylet must return `UNIMPLEMENTED` before
mutation rather than ignore an unknown proto3 field. At process start, the
keyed service generates a random runtime-incarnation UUID. A capability request
contains a fresh caller nonce; the response echoes it and returns protocol
version, implementation digest, local schema version, runtime UUID, and a
closed list of qualified submission-containment profiles and implementation
digests. Before
each keyed job or lifecycle-fence operation, the caller obtains that response
and sends its exact runtime UUID and echoed nonce. A mismatch fails before
mutation. A retry re-probes, performs exact keyed lookup in persistent local
state, and adopts a present matching effect before accepting a new runtime
UUID. Job lookup is an observation only and can never prove absence.

Version 41 supports two immutable runtime modes. The default is `LEGACY`; it
preserves the current `StopEvent`, `SetAutostop`, hook, indicator, exception,
and provider-retry behavior byte-for-byte and rejects lifecycle-fence install
and keyed mutation as `FAILED_PRECONDITION`. `COORDINATED_V1` is selected only
from a root-owned bootstrap marker installed before the first v41 Skylet start by
the qualified private worker-launch path. `PoolWorkerBootstrapIdentityV1`
contains the fresh coordination-scope UUID, service hash, worker incarnation,
cluster name, launch operation, lifecycle-fence operation and digest, expected
Skylet implementation digest, and fixed runtime mode. On its first v41 start,
Skylet creates one singleton `pool_worker_bootstrap_state` row in
`skylet_config.db` regardless of marker presence: an absent marker commits
immutable `LEGACY` with a null digest, while an exact marker commits immutable
`COORDINATED_V1` with its canonical digest. Later marker creation on a `LEGACY`
row, marker absence or replacement on a coordinated row, mode change, or
identity mismatch cannot rewrite that row and fails coordinated capability and
mutation. Capability readback reports the persisted mode and marker digest.
M5-S0 includes no marker producer, so every pre-existing and ordinarily
launched worker remains on the exact legacy path. M5-S3 adds the dormant
private-launch marker producer, but it is unreachable until S4 permits a fresh
coordinated birth.

In `COORDINATED_V1` mode, version 41 closes the worker's independent idle-
teardown owner with one local file lock shared by `StopEvent`, `SetAutostop`,
and both lifecycle-fence RPCs. This protocol is active from the first Skylet
event-loop iteration, before fence installation, so a legacy idle decision can never be
in flight when install succeeds. The fence is one-way for the physical worker
lifetime. There is no release RPC: a coordinated worker must be permanently
terminated, not downgraded in place, before service deactivation or worker-
image rollback. Keyed job RPCs still revalidate the exact active fence before
mutation, but no cross-database release-versus-add protocol exists to race with
them.

In coordinated mode, `StopEvent` takes the lifecycle lock before reading
autostop configuration and holds it through its idle decision and a durable
teardown claim, but releases it before hook or provider I/O. A teardown winner
records a random attempt ID, current boot identity, cluster/provider identity,
immutable canonical autostop payload, exact cluster-YAML digest, complete hook
snapshot and digest, and state `CLAIMED` in `skylet_config.db`; the immutable
payload is separate from the mutable current autostop config. The same
transaction writes the autostopping indicator. `SetAutostop` may update the
future config but cannot erase or rewrite that attempt. A pre-effect
cancellation can terminalize only a still-`CLAIMED` attempt. Once any hook or
provider phase starts, cancel is a future-config change and the attempt remains
a blocking effect record.

The durable phase machine is `CLAIMED -> HOOK_STARTED -> HOOK_COMPLETED ->
PROVIDER_ENTERED`, followed only by proved `TERMINAL` or `AMBIGUOUS`.
`HOOK_STARTED` is committed before the hook process and `HOOK_COMPLETED` after
its result. `PROVIDER_ENTERED` is committed before the first provider call.
A crash or exception after either entered phase never clears the attempt and
never blindly repeats that effect. Recovery may continue from
`HOOK_COMPLETED` into provider entry. S0 does not implement or assume a general
provider teardown reconciler. If the provider call returns normally, the same
owner commits `TERMINAL`; a caught timeout or exception commits `AMBIGUOUS`,
and a restart that finds `PROVIDER_ENTERED` after a crash also commits
`AMBIGUOUS` without performing a provider call.
A later milestone may resolve that row only through an explicitly promoted
provider capability that binds an idempotent teardown operation to an
authoritative query. Until then, operator proof or permanent worker teardown is
required, and the row blocks lifecycle-fence install and pool activation. An
inconclusive hook phase likewise remains `AMBIGUOUS`. All payload bytes remain
local and are redacted from logs and RPC diagnostics. Thus a fence installer
either wins before the event reads configuration or observes a durable claim
before it can report success.

The existing hook claim file becomes a compatibility mirror, not the recovery
authority for an idle teardown. Before a snapshotted hook runs, v41 commits
`HOOK_STARTED` and the same attempt ID, then claims or verifies the file under
the lifecycle lock. If another teardown event already owns that file, the idle
attempt becomes `AMBIGUOUS` and performs no second hook or provider effect.
Skylet startup on the same boot reconstructs the mirror from a durable
nonterminal attempt and does not unconditionally clear it. A changed boot may
retire the mirror only after runtime and provider identity re-probe. A crash in
`HOOK_STARTED` never reruns the hook; `HOOK_COMPLETED` is the durable receipt
that permits the provider phase. Other hook event ingress keeps its
characterized file CAS, but any unresolved foreign claim also blocks lifecycle-
fence install. This removes restart-driven duplicate hook execution from the S0
claim without pretending an arbitrary user hook is idempotent.

An additive `pool_worker_lifecycle_fences` table in that same database retains
the one-way install row keyed by operation UUID. The canonical
`PoolWorkerLifecycleFenceIdentityV1` is service hash, cluster name, worker
incarnation, operation UUID, protocol and digest versions, and the fixed
requirements `autostop_disabled=true` and `autodown_disabled=true`. Install is
create-or-return: under the shared file lock, one SQLite transaction rejects a
current-boot teardown claim, autostopping indicator, nonterminal keyed
submission, keyed pending row, or nonterminal local or external containment
scope; compares every
identity field; disables autostop in the existing config row; clears only an
unclaimed stale indicator; and commits the exact `FENCED` row. It then re-reads
the transaction result and returns the full identity and config digest. A
matching retry returns that row. A changed or previously conflicting operation
identity, incompatible active fence, or ambiguous teardown returns a closed
failure and performs no write.

While an active fence exists, `StopEvent` returns before its idle decision and
v41 `SetAutostop` rejects every request that would arm stop or down. The fence
has no released state and is never cleared on the live worker. Fence state and
identity survive a v41 process restart, and capability readback returns them
with the fresh runtime UUID. Deactivation first closes and permanently downs
every version-1 worker, proves provider absence, retires its permanent physical
target, and deletes the replica row. A v40 process does not honor this new
lock, so a live fenced worker also categorically blocks worker-image rollback;
the control plane can downgrade only after the worker inventory is proved
empty.

The migration does not alter `jobs` or `pending_jobs`. Current Skylets use
positional inserts into both tables, so even nullable appended columns would
make a rolled-back v40 binary unwritable. Instead, a new `keyed_submissions`
side table has primary key `(username, submission_key)`, a unique `job_id`, add
digest, nullable queue digest, exact service hash, worker incarnation, active
lifecycle-fence operation ID and digest, one authoritative lifecycle state,
driver token, submission-containment plan and digest, nullable provisional root
PID and process-start identity, local supervisor operation and cgroup-scope
UUID, nullable supervisor launch-receipt digest, and nullable cancel operation ID,
digest, origin state, fixed grace-policy version, accepted host boot ID,
absolute `CLOCK_BOOTTIME` grace deadline in nanoseconds, monotonic cancel phase,
nullable completion operation ID and identity digest, pending logical legacy
status and supervisor outcome-receipt digest, monotonic completion phase, and
exact terminal legacy status. The cancel phase is null before cancellation
and then `INTENT`, `GRACE_ENTERED`, `HARD_KILL_ENTERED`, or
`OWNERS_RETIRED`. Cancel operation, digest, origin, policy, boot ID, deadline,
and phase are all null before cancellation and all non-null once `CANCELLING`
commits; they are immutable or monotonic thereafter. The parent writes the
provisional process and containment tuple while the row remains `LAUNCHING`;
the trusted control reducer confirms the supervisor launch receipt and moves it
to `STARTED`. Completion operation, identity, pending status, outcome receipt,
and phase are all null before `COMPLETING`; on entry they become immutable, and
phase advances only through `OUTCOME_RECORDED`, `OWNERS_SEALED`, and
`OWNERS_RETIRED`.
PID, process-group, session, argv, and token evidence remain diagnostics and
reaping identity, never whole-job absence proof. The legacy job row remains the
source of ordinary job fields and status. Legacy jobs and duplicate legacy
pending rows are neither rewritten nor deduplicated.

A separate `keyed_submission_containments` table is keyed by `(username,
submission_key, ordinal)`. It stores a closed containment kind and
implementation digest, never-reused owner UUID, complete canonical identity
digest, provider-native owner locator where applicable, and monotonic state
`PLANNED`, `PREPARED`, `LAUNCHED`, `SEALED`, `EMPTY`, `RETIRED`, or
`QUARANTINED`, plus an
immutable effect or terminal receipt. Every plan has one
`LOCAL_CGROUP_V2_DIRECT_V1` row and zero or more
qualified external-execution owner rows. The full ordered plan is immutable
before queue admission. `PLANNED` is the durable side-table row before the
runtime owner acknowledges its ledger entry and cannot queue or launch;
`PREPARED` requires the exact adapter-side prepare receipt and still proves no
effect. `RETIRED` requires a kind-specific verified absence and
no-later-effect receipt;
missing state or an inconclusive owner query becomes `QUARANTINED`. These rows
are execution-effect ownership, not duplicate job status.

The closed durable state machine is `UNQUEUED -> QUEUED -> LAUNCHING ->
STARTED`, with cancellation edges from `QUEUED`, `LAUNCHING`, or `STARTED` to
`CANCELLING`, the natural-completion edge `STARTED -> COMPLETING`, the restricted
pre-effect failure edge `LAUNCHING -> COMPLETING`, and exact terminal states
`SUCCEEDED`, `FAILED`, and `CANCELLED`. `QUARANTINED` is reachable from every
ambiguous state. A winning `QUEUE` absence seal is the only direct
`UNQUEUED -> CANCELLED` edge. A `CANCELLING` row retains its immutable origin
state so retry can distinguish no-spawn, pre-receipt, and receipt-backed
cancellation. Keyed `FAILED` retains an exact terminal legacy-status member:
`FAILED`, `FAILED_SETUP`, or `FAILED_DRIVER`. The reducer preserves that value
in the unchanged legacy row and lookup response rather than flattening setup or
driver failure; keyed `SUCCEEDED` and `CANCELLED` map one-to-one.
The `LAUNCHING -> COMPLETING` edge requires a matching manager ledger with gate
closed, no `EFFECT_ADMITTED`, and one closed failed-launch outcome receipt; its
pending status is exactly `FAILED_DRIVER` and it can use only the
`SealFailedLaunchV1` protocol below.
`INSERTING`, `UPDATING`, `RECEIPTING`, `STATUS_UPDATING`, and `DELETING` are
transaction-local guard states: one owner writes the transient value, performs
the associated legacy-table mutation, and advances to the next durable state
in one `BEGIN IMMEDIATE` transaction, so no other connection can observe or
borrow it.

Persistent SQLite triggers fence the unchanged legacy tables for mapped keyed
jobs. A `pending_jobs` insert requires state `INSERTING` and no existing pending
row for `NEW.job_id`; update requires `UPDATING`; delete requires `DELETING`.
Updates to the legacy job status, PID, start, or end fields require the matching
keyed transaction guard state. `QueueJobKeyed` changes `UNQUEUED` to
`INSERTING`, inserts one legacy-shaped pending row, changes the job from `INIT`
to `PENDING`, and advances to `QUEUED` in one commit. Before creating a new side
mapping, `AddJobKeyed` also rejects an orphan pending row for its newly
allocated job ID. The SQLite write lock prevents an old connection from
borrowing any uncommitted marker, and the row-existence predicate rejects a
same-transaction second insert. Duplicate legacy or keyed mutation of a mapped
row is therefore rejected, while ordinary legacy behavior is unchanged.

###### M5-S0a persistence foundation pre-slice

M5-S0 first lands one separately reviewed, merged, deployed, and monitored
v40-compatible persistence pre-slice before any v41 process is advertised.
M5-S0a creates no keyed row, registers no RPC, changes no scheduler or status
path, and keeps `SKYLET_VERSION = 40`. This distinction is required because a
Skylet version bump force-kills and restarts the existing daemon: using `41`
for schema-only code would both cause an unnecessary restart and prevent the
complete v41 implementation from forcing its required later restart. M5-S0a
does not satisfy any v41 capability, activation, or worker-compatibility gate.

`KEYED_SUBMISSION_SCHEMA_VERSION = 1` is a code-owned constant, not
`PRAGMA user_version`; the existing database is shared with unversioned legacy
migrations. The exact v1 side-table layout is:

```sql
CREATE TABLE keyed_submissions (
  username TEXT NOT NULL,
  submission_key TEXT NOT NULL,
  job_id INTEGER NOT NULL CHECK (job_id > 0),
  add_digest TEXT NOT NULL,
  queue_digest TEXT,
  service_hash TEXT NOT NULL,
  worker_incarnation TEXT NOT NULL,
  lifecycle_fence_operation_id TEXT NOT NULL,
  lifecycle_fence_identity_digest TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN (
    'UNQUEUED', 'QUEUED', 'LAUNCHING', 'STARTED', 'CANCELLING',
    'COMPLETING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'QUARANTINED',
    'INSERTING', 'UPDATING', 'RECEIPTING', 'STATUS_UPDATING', 'DELETING')),
  driver_token TEXT,
  containment_plan BLOB NOT NULL,
  containment_plan_digest TEXT NOT NULL,
  provisional_root_pid INTEGER CHECK (
    provisional_root_pid IS NULL OR provisional_root_pid > 0),
  provisional_process_start_identity BLOB,
  local_supervisor_operation_id TEXT,
  local_cgroup_scope_uuid TEXT NOT NULL,
  supervisor_launch_receipt_digest TEXT,
  cancel_operation_id TEXT,
  cancel_digest TEXT,
  cancel_origin_state TEXT CHECK (
    cancel_origin_state IS NULL OR
    cancel_origin_state IN ('QUEUED', 'LAUNCHING', 'STARTED')),
  cancel_grace_policy_version TEXT CHECK (
    cancel_grace_policy_version IS NULL OR
    cancel_grace_policy_version = 'TERM_10S_THEN_HARD_KILL_V1'),
  cancel_host_boot_id TEXT,
  cancel_deadline_boottime_ns INTEGER CHECK (
    cancel_deadline_boottime_ns IS NULL OR
    cancel_deadline_boottime_ns >= 0),
  cancel_phase TEXT CHECK (
    cancel_phase IS NULL OR cancel_phase IN (
      'INTENT', 'GRACE_ENTERED', 'HARD_KILL_ENTERED', 'OWNERS_RETIRED')),
  completion_operation_id TEXT,
  completion_identity_digest TEXT,
  pending_legacy_status TEXT CHECK (
    pending_legacy_status IS NULL OR pending_legacy_status IN (
      'SUCCEEDED', 'FAILED', 'FAILED_SETUP', 'FAILED_DRIVER')),
  supervisor_outcome_receipt_digest TEXT,
  completion_phase TEXT CHECK (
    completion_phase IS NULL OR completion_phase IN (
      'OUTCOME_RECORDED', 'OWNERS_SEALED', 'OWNERS_RETIRED')),
  terminal_legacy_status TEXT CHECK (
    terminal_legacy_status IS NULL OR terminal_legacy_status IN (
      'SUCCEEDED', 'FAILED', 'FAILED_SETUP', 'FAILED_DRIVER', 'CANCELLED')),
  PRIMARY KEY (username, submission_key),
  CHECK ((provisional_root_pid IS NULL) =
         (provisional_process_start_identity IS NULL)),
  CHECK (state NOT IN (
    'UPDATING', 'LAUNCHING', 'STARTED', 'RECEIPTING', 'COMPLETING',
    'SUCCEEDED', 'FAILED') OR local_supervisor_operation_id IS NOT NULL),
  CHECK (state NOT IN (
    'UPDATING', 'LAUNCHING', 'STARTED', 'RECEIPTING', 'COMPLETING',
    'SUCCEEDED', 'FAILED') OR driver_token IS NOT NULL),
  CHECK (cancel_origin_state IS NULL OR cancel_origin_state = 'QUEUED' OR
         (local_supervisor_operation_id IS NOT NULL AND
          driver_token IS NOT NULL)),
  CHECK (
    (cancel_operation_id IS NULL AND cancel_digest IS NULL AND
     cancel_origin_state IS NULL AND cancel_grace_policy_version IS NULL AND
     cancel_host_boot_id IS NULL AND
     cancel_deadline_boottime_ns IS NULL AND cancel_phase IS NULL) OR
    (cancel_operation_id IS NOT NULL AND cancel_digest IS NOT NULL AND
     cancel_origin_state IS NOT NULL AND
     cancel_grace_policy_version IS NOT NULL AND
     cancel_host_boot_id IS NOT NULL AND
     cancel_deadline_boottime_ns IS NOT NULL AND cancel_phase IS NOT NULL)),
  CHECK (state != 'CANCELLING' OR cancel_operation_id IS NOT NULL),
  CHECK (
    (completion_operation_id IS NULL AND
     completion_identity_digest IS NULL AND pending_legacy_status IS NULL AND
     supervisor_outcome_receipt_digest IS NULL AND completion_phase IS NULL) OR
    (completion_operation_id IS NOT NULL AND
     completion_identity_digest IS NOT NULL AND
     pending_legacy_status IS NOT NULL AND
     supervisor_outcome_receipt_digest IS NOT NULL AND
     completion_phase IS NOT NULL)),
  CHECK (state NOT IN ('COMPLETING', 'SUCCEEDED', 'FAILED') OR
         completion_operation_id IS NOT NULL),
  CHECK (state NOT IN ('COMPLETING', 'SUCCEEDED', 'FAILED') OR
         cancel_operation_id IS NULL),
  CHECK (state != 'CANCELLED' OR completion_operation_id IS NULL),
  CHECK (
    (state = 'SUCCEEDED' AND terminal_legacy_status IS 'SUCCEEDED') OR
    (state = 'FAILED' AND terminal_legacy_status IS NOT NULL AND
     terminal_legacy_status IN ('FAILED', 'FAILED_SETUP', 'FAILED_DRIVER')) OR
    (state = 'CANCELLED' AND terminal_legacy_status IS 'CANCELLED') OR
    (state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED') AND
     terminal_legacy_status IS NULL))
);

CREATE UNIQUE INDEX keyed_submissions_job_id_uq
  ON keyed_submissions(job_id);
CREATE INDEX keyed_submissions_state_idx
  ON keyed_submissions(state, job_id);

CREATE TABLE keyed_submission_containments (
  username TEXT NOT NULL,
  submission_key TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  kind TEXT NOT NULL CHECK (kind = 'LOCAL_CGROUP_V2_DIRECT_V1'),
  implementation_digest TEXT NOT NULL,
  owner_uuid TEXT NOT NULL,
  identity_digest TEXT NOT NULL,
  provider_native_owner_locator BLOB,
  state TEXT NOT NULL CHECK (state IN (
    'PLANNED', 'PREPARED', 'LAUNCHED', 'SEALED', 'EMPTY', 'RETIRED',
    'QUARANTINED')),
  prepare_receipt BLOB,
  prepare_receipt_digest TEXT,
  launch_receipt BLOB,
  launch_receipt_digest TEXT,
  effect_receipt BLOB,
  effect_receipt_digest TEXT,
  seal_receipt BLOB,
  seal_receipt_digest TEXT,
  empty_receipt BLOB,
  empty_receipt_digest TEXT,
  retirement_receipt BLOB,
  retirement_receipt_digest TEXT,
  PRIMARY KEY (username, submission_key, ordinal),
  CHECK ((prepare_receipt IS NULL) = (prepare_receipt_digest IS NULL)),
  CHECK ((launch_receipt IS NULL) = (launch_receipt_digest IS NULL)),
  CHECK ((effect_receipt IS NULL) = (effect_receipt_digest IS NULL)),
  CHECK ((seal_receipt IS NULL) = (seal_receipt_digest IS NULL)),
  CHECK ((empty_receipt IS NULL) = (empty_receipt_digest IS NULL)),
  CHECK ((retirement_receipt IS NULL) =
         (retirement_receipt_digest IS NULL)),
  CHECK (state != 'PREPARED' OR prepare_receipt IS NOT NULL),
  CHECK (state != 'LAUNCHED' OR
         (prepare_receipt IS NOT NULL AND launch_receipt IS NOT NULL)),
  CHECK (state != 'SEALED' OR seal_receipt IS NOT NULL),
  CHECK (state != 'EMPTY' OR
         (seal_receipt IS NOT NULL AND empty_receipt IS NOT NULL)),
  CHECK (state != 'RETIRED' OR retirement_receipt IS NOT NULL)
);

CREATE UNIQUE INDEX keyed_submission_containments_owner_uuid_uq
  ON keyed_submission_containments(owner_uuid);
CREATE UNIQUE INDEX keyed_submission_containments_local_owner_uq
  ON keyed_submission_containments(username, submission_key)
  WHERE kind = 'LOCAL_CGROUP_V2_DIRECT_V1';
CREATE INDEX keyed_submission_containments_state_idx
  ON keyed_submission_containments(state, username, submission_key);

CREATE TABLE keyed_submission_seals (
  username TEXT NOT NULL,
  submission_key TEXT NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN ('ADD', 'QUEUE')),
  expected_add_digest TEXT NOT NULL,
  job_id INTEGER,
  queue_digest TEXT,
  state TEXT NOT NULL CHECK (state = 'SEALED_ABSENT'),
  created_at REAL NOT NULL CHECK (created_at >= 0),
  PRIMARY KEY (username, submission_key, phase),
  CHECK ((phase = 'ADD' AND job_id IS NULL AND queue_digest IS NULL) OR
         (phase = 'QUEUE' AND job_id IS NOT NULL AND job_id > 0 AND
          queue_digest IS NOT NULL))
);
```

All identifier, digest, token, and canonical-byte fields are additionally
validated by the v41 reducer before insertion. SQLite owns only the closed
state, nullability, numeric, primary-key, and uniqueness invariants above; it
does not guess future digest or UUID versions. Foreign keys are deliberately
absent because existing v40 connections do not enable SQLite foreign-key
enforcement. The reducer must validate the parent submission in its same
`BEGIN IMMEDIATE` transaction. The partial unique local-owner index enforces at
most one `LOCAL_CGROUP_V2_DIRECT_V1` row per submission; the v41 reducer and
full-plan digest enforce the required at-least-one row before admission.

The installer runs only after the legacy additive-column commits, requires no
active transaction, and creates all three tables and five named indexes in one
`BEGIN IMMEDIATE`. It validates the code-owned canonical SQL for every object
before commit. Canonicalization removes the exact `IF NOT EXISTS` clause that
SQLite omits from `sqlite_master.sql`, removes ASCII whitespace outside
single-quoted literals, strips one trailing semicolon, and otherwise preserves
every UTF-8 byte. Repeated and concurrent installation is create-or-validate.
A reserved-name or owned-object shape mismatch rolls back every object created
by that attempt, reports keyed schema unavailable, and leaves ordinary v40 job
initialization available; an I/O error, corrupt database, or failure involving
either legacy table still propagates. The later v41 capability revalidates this
exact object set and advertises no keyed profile when it is unavailable.

M5-S0a deliberately installs no trigger and no mapped-row producer. The
persistent negative-authority triggers land in M5-S0b together with the only
v41 reducer able to satisfy their transient guard protocol. Their literal SQL
and catalog-validation bytes must be added to this canonical design before S0b
implementation. The S0b triggers must use `RAISE(ROLLBACK, ...)`, reject
pending inserts after either applicable seal, preserve immutable pending-row
identity while advancing `submit` from zero, bind PID publication to the exact
stored provisional process, enforce exact status transitions and retired-owner
terminalization, reject mapped job replace, rekey, or delete, and prevent
replacement or rewriting of side-table identities and already stored receipt
columns. Landing them with the reducer avoids expanding every v40 write path
before any keyed owner exists while retaining the final stale-writer barrier
before the first keyed row can be produced.

A v40 binary can still add and queue ordinary jobs after the schema appears.
If it later sees an existing keyed pending row, its autonomous `_run_job()`
fails on the guarded pending update before process spawn, its status heuristic
fails before a keyed job update, and its cleanup fails before pending deletion.
Once keyed rows are used, central activation independently forbids a v40
Skylet, but the persistent local triggers remain the durable last barrier.
Those triggers fence v40 scheduler and database writers before spawn or a
legacy-table mutation. They cannot prevent the raw v40 `CancelJobs` handler
from signalling a process before its later database write. The supported
takeover boundary therefore also requires exactly one v41 listener, proves no
predecessor service process remains, and never sends a legacy cancel RPC for a
keyed row. A capable v41 generic handler rejects keyed IDs before signalling.
An arbitrary independently launched v40 RPC server is outside the S0 safety
claim and blocks activation or worker-image rollback.

Keyed execution is admitted only through a qualified
`SubmissionContainmentAdapterV1`. The common protocol prepares an immutable
ordered owner plan before queue admission, launches or binds each owner before
its first effect, observes it by exact owner identity, requests cancellation,
and verifies a terminal absence receipt. Capacity release requires `RETIRED`
receipts from every local and external owner in the plan; managed-job status,
driver exit, a Ray status string, or one empty process query cannot substitute.
A backend adds only its canonical owner locator, actuation, query, and receipt
verifier. Common Skylet code owns storage, ordering, replay, quarantine, and
release policy.

Preparation is an explicit adapter operation, not an inferred lack of a
process. `AddJobKeyed` first commits every immutable owner as `PLANNED`. Recovery
then calls idempotent `PrepareOwnerV1` for each exact identity before queue
admission. For the local adapter, the supervisor create-or-returns a durable
`PREPARED` ledger entry and permanent UUID reservation without creating a
cgroup, process, gate, or other workload effect; changed identity conflicts.
Skylet verifies that receipt and advances the side row `PLANNED -> PREPARED`.
`QueueJobKeyed` rejects a plan unless every owner is `PREPARED`. Thus a crash
after `LAUNCHING` but before `CreateAndLaunchV1` still has an authoritative
supervisor ledger and cannot be mistaken for an absent unowned launch.

Every adapter also implements idempotent `SealNeverLaunchedV1`. It accepts the
complete planned identity in either `PLANNED` or `PREPARED`, proves that no
effect was admitted, writes the adapter's permanent no-later-launch tombstone,
and returns the receipt needed for `RETIRED`. For the local manager it can
atomically create a sealed tombstone from an exact never-observed planned
identity or transition its matching `PREPARED` ledger; it refuses a launched or
changed owner. Response loss is resolved by querying that same ledger and
tombstone, never by cgroup or process absence.

A launched but gate-closed owner uses a third explicit operation,
`SealFailedLaunchV1`; it is neither never-launched nor cancelled. The trusted
reducer first locks the job, loses to an already committed `CANCELLING` owner if
present, otherwise binds one immutable failed-launch completion identity to the
add and queue digests, owner and manager generations, exact `LAUNCHED` receipt,
closed failure receipt, gate-closed proof, and pending `FAILED_DRIVER`, and
commits `LAUNCHING -> COMPLETING/OUTCOME_RECORDED` before calling the manager.
The supervisor create-or-returns only when its matching ledger is `LAUNCHED`,
the payload gate has never opened, and `EFFECT_ADMITTED` is absent. It persists
`SEALED` before permanently rejecting gate release, kills the exact shim and
any setup descendants through the retained cgroup root, proves recursive empty
and reap, removes the root, persists `RETIRED`, and returns a permanent
no-later-effect receipt. Changed identity, an open gate, or ambiguous ledger
quarantines and performs no cleanup.

Skylet mirrors `OWNERS_SEALED` and `OWNERS_RETIRED` from those receipts and only
then uses transient `DELETING` to remove pending state, write legacy
`FAILED_DRIVER`, and commit keyed `FAILED`. Exact response-loss and restart
retries query and resume the same failed-launch operation at `SEALED`, `EMPTY`,
or `RETIRED`; they never call `CreateAndLaunchV1` again. Initial M5 has no
external owner, while a future composite profile must retire every still-
prepared external owner through its exact never-launched seal before this
terminal commit.

Every adapter also implements an authoritative `ListOwnedV1` inventory scoped
to the exact worker-bootstrap identity. Reconciliation compares both sets: each
`PREPARED` or later non-retired durable plan row must resolve to exactly one
matching runtime ledger or owner,
and each observed owned runtime object must resolve to one plan row or permanent
tombstone. A `PLANNED` row is reconciled only by exact `PrepareOwnerV1` or
`SealNeverLaunchedV1`; an already prepared ledger with a still-`PLANNED` side
row is joined by identity and its receipt is mirrored. A missing, duplicate,
changed, or unbound object is quarantined. An
exact orphan may be sealed and retired only when the retained central assignment
or submission tombstone supplies its full immutable authority; a merely
unrecognized runtime object is never deleted by garbage collection. This ports
dstack's useful server-versus-shim inventory reconciliation while replacing its
best-effort orphan removal with identity-bound adoption or quarantine.

M5 initially qualifies only `LOCAL_CGROUP_V2_DIRECT_V1`: one single-node
worker, one direct driver, and the mandatory local cgroup owner, with no Ray job,
Slurm step, SSH fan-out, Kubernetes job, or other external execution owner.
The assignment planner rejects any coordinated task that needs another profile
before it writes `INTENT`. Current `RayCodeGen` is not a qualified owner: its
tasks run in pre-existing Ray worker processes, and a submission status or
driver PID does not prove detached actors or remote subprocesses absent.
Current Slurm execution is likewise ineligible: `srun` may move work into
Slurm-owned cgroups on other nodes, and a local driver cgroup owns only the
client. A future `RAY_JOB_V1` adapter needs an exact non-detachable execution
owner plus cancel and terminal-absence receipt. A future
`SLURM_ALLOCATION_V1` adapter must reserve one exact allocation per submission,
persist its JobID before user effect, use exact `scancel`, and verify terminal
accounting plus proctrack/cgroup absence under Slurm's
[job termination owner](https://slurm.schedmd.com/job_launch.html); a step name
or JobID.StepID alone is
insufficient because sibling steps can outlive it. Multi-node, Ray-backed, and
Slurm-backed tasks remain on version-0 pools until those contracts are
qualified.

`LOCAL_CGROUP_V2_DIRECT_V1` is enforced by a persistent root-owned
`skypilot-containmentd.service` with an exclusively delegated cgroup-v2 root in
the qualified worker image, following systemd's
[single-writer delegation contract](https://systemd.io/CGROUP_DELEGATION/).
The manager moves itself into a `supervisor`
subtree. Each never-reused submission UUID names a process-free manager-owned
kill root with a `payload` child into which the shim and all descendants are
born. The kill root is never delegated to payload code. If nested cgroups are
needed, only the payload subtree is visible inside a private cgroup and mount
namespace, while the inaccessible manager root remains the recursive kill
target.

Skylet runs as a dedicated control identity, the native outcome shim runs as a
second non-root trusted identity, job commands run as a third unprivileged
payload identity, and peer credentials admit only the control identity to the
supervisor command socket. The payload identity cannot ptrace, signal, inspect,
or inherit FDs from the shim identity. Only the control identity opens
`skylet_config.db`, takes the per-job lock, or calls the keyed reducer. The
trusted shim receives one inherited, submission-scoped, bounded bidirectional
`SOCK_SEQPACKET` request/ack FD created and retained by the supervisor; user
commands never inherit it. It is not the command socket and supports no launch,
signal, cgroup, query, or database operation. Shim-to-manager frames are closed
to `PAYLOAD_READY` and `LOGICAL_OUTCOME_V1` with the manager-supplied sequence
and sealed submission identity. Manager-to-shim frames are closed to the exact
durable acknowledgment for the immediately preceding sequence; unsolicited,
out-of-order, oversized, duplicate-changed, or command-shaped frames close and
quarantine the channel. The supervisor binds the FD to its owner ledger,
durably records each accepted event before sending its acknowledgment, and
retains a separate one-way payload-gate writer.
Skylet's trusted control reconciler queries that ledger and alone mirrors launch
receipts, start state, logical outcome, cleanup phases, and terminal legacy
status into SQLite.

The payload has no host root, `CAP_SYS_ADMIN`, manager command FD or database
access, writable ancestor or sibling `cgroup.procs`, host cgroup namespace or
systemd bus, privileged container socket, or equivalent out-of-tree spawner. A
same-UID Skylet and workload is ineligible unless a separately qualified
isolation profile proves the payload cannot ptrace the manager, steal its FDs,
call the helper, or reach writable host cgroupfs. The supervisor owns a durable
registry and permanent tombstone for every containment UUID. A worker that
lacks this principal separation, a
single unified cgroup-v2 mount, real systemd delegation, recursive
`cgroup.kill`, `clone3(CLONE_INTO_CGROUP | CLONE_PIDFD)`, exact
`cgroup.events` readback, or supervisor-registry recovery advertises no local
containment capability. These are behavioral runtime probes, not version
checks or best-effort warnings.

`CgroupContainmentCapabilityV1` binds the systemd unit and control-group
identity, actual delegation ownership, manager and native-launcher digests and
runtime UUID, cgroup2 mount and delegated-root identities, host boot ID,
`clone3` and pidfd behavior, non-threaded process-free kill-root topology,
`cgroup.kill`, recursive `cgroup.events` semantics, qualified reaper,
control/payload isolation profile, the submission-scoped event-channel protocol
and ledger digest, private namespace profile, and the closed
external-adapter set. Its probe creates a throwaway containment, directly
clones a helper that double-forks, calls `setsid`, clears its environment and
argv marker, closes inherited FDs, and leaves a daemon after the root exits.
It proves the payload cannot write or migrate to ancestor or sibling cgroups,
see or mount the host hierarchy, join the host cgroup namespace, call the
manager, ptrace or steal manager FDs, or reach a configured external spawner.
It then seals, recursively kills, observes `populated 0`, reaps, retires, and
proves the old operation cannot recreate its path. A seccomp denial, writable
escape path, missing delegation, failed readback, or surviving helper removes
the capability.

The add digest is the canonical hash of username, job name, stable run
timestamp, resources, metadata, service hash, worker incarnation, active
lifecycle-fence operation and digest, and the full containment plan digest.
`AddJobKeyed` holds one
`BEGIN IMMEDIATE` transaction through seal check, explicit-column legacy job
insert, side-row insert, exact-conflict comparison, log-directory derivation,
and log-directory update. It does not call the existing commit-owning
`add_job()` or `set_log_dir_no_lock()`. A repeated add returns the existing job
ID and log directory only when every field and digest match. Add, queue,
cancel, lookup, and phase seal first require the same currently active local
lifecycle fence; a missing or changed fence performs no job mutation.

A new `keyed_submission_seals` table has primary key `(username,
submission_key, phase)`, where phase is exactly `ADD` or `QUEUE`, and stores
the expected add digest, optional exact job ID and queue digest, state
`SEALED_ABSENT`, and database creation time. S0 retains these tombstones
indefinitely and has no garbage collector or retention owner. An exact repeated
seal returns the existing result; any identity or digest mismatch is a hard
conflict and cannot overwrite the row.

`SealSubmissionKeyAbsent(ADD)` takes `BEGIN IMMEDIATE`, compares the exact
keyed job and digest, and returns `PRESENT` when one exists. Otherwise it
inserts the `ADD` tombstone and returns `SEALED_ABSENT` in the same commit.
`AddJobKeyed` and `QueueJobKeyed` both reject that tombstone before mutation.
SQLite write serialization means an already-started add either commits first
and is observed as present, or loses to the seal and is rejected later.

`SealSubmissionKeyAbsent(QUEUE)` requires the exact existing keyed job and add
digest, then takes the same per-job file lock as `QueueJobKeyed`. Under
`BEGIN IMMEDIATE` it returns `PRESENT` for a stored matching queue digest or
inserts a `QUEUE` tombstone for that job and expected queue digest. When it wins
against an exact `UNQUEUED` row, that first commit leaves the job `UNQUEUED` but
permanently prevents `QueueJobKeyed`. While retaining the file lock, or after a
crash by reacquiring it and joining the tombstone, the seal owner drives every
immutable `PLANNED` or `PREPARED` containment through idempotent
`SealNeverLaunchedV1`. Only after every row is `RETIRED` with its permanent
no-later-launch receipt does a final `BEGIN IMMEDIATE` transaction revalidate
the tombstone, use transient `STATUS_UPDATING` to change legacy `INIT` to
`CANCELLED`, and commit the direct keyed `UNQUEUED -> CANCELLED` edge; no
process or external owner was admitted.
It therefore leaves neither a nonterminal keyed row nor an unretired owner. A keyed queue
checks the tombstone while holding that file lock and before any file or
database mutation. Any pending row, non-`INIT` job status, PID, supervisor receipt,
or queue state without the expected digest returns `AMBIGUOUS` and writes no
tombstone. Thus a delayed queue either commits first and is observed, or is
permanently rejected and its add-only shell is terminalized. If no keyed job
exists, an already committed matching `ADD` seal is the complete absence proof
because every later keyed add or queue checks it. A negative
`GetJobBySubmissionKey` response, process absence, timeout, or transport error
is never such proof.

Both digests use SHA-256 over a versioned, length-prefixed canonical encoding,
not Python `repr`, pickle, map iteration order, or protobuf wire defaults.
JSON objects use sorted keys and normalized UTF-8, resource fields use the
canonical task YAML projection, and protobuf inputs use deterministic
serialization. A future field or normalization change requires a new digest
version and RPC version.

The queue digest is the canonical hash of job ID, codegen bytes or the exact
uploaded script bytes, script path, remote log directory, and deterministic
managed-job proto bytes, execution profile, and containment plan digest.
`QueueJobKeyed` takes the per-job file lock before any
file or database mutation and first rejects either applicable tombstone. It
atomically writes code through temporary-file rename, then uses one
`BEGIN IMMEDIATE` transaction to recheck the seals, compare or initialize the
queue digest, perform the trigger-authorized one pending-row insert, change the
job status from `INIT` to `PENDING`, and durably reach launch state `QUEUED`.
It does not call the commit-owning `JobScheduler.queue()` or
`_set_status_no_lock()`. The shared file lock keeps a queue-absence seal from
linearizing between the pre-file check and that commit.
A matching retry verifies or repairs a missing pre-launch artifact and invokes
the scheduler; it is not a no-op merely because a digest exists. A crash before
rename leaves no database intent. A crash after rename but before the
transaction leaves an unauthoritative exact artifact that a retry verifies and
reuses unless a later queue seal rejects it. A crash after the transaction
leaves a durable `QUEUED` row that recovery schedules.

`JobScheduler.schedule_step()` identifies keyed rows before calling any legacy
status, reboot, pending, or spawn helper. `update_status()` splits its input:
legacy IDs retain the exact current PID and boot-time heuristic, while keyed
`QUEUED` and `LAUNCHING` rows are never marked `FAILED_DRIVER` before a
supervisor launch or exit receipt and `STARTED` and `COMPLETING` rows use the
keyed receipt-aware reconciler. The keyed
branch never calls `_run_job()`, `remove_job_no_lock()`,
`_set_status_no_lock()`, `set_status()`, or `JobScheduler.queue()`. Pending
submit updates, status changes, PID writes, and terminal cleanup use new
non-committing SQL helpers inside the state-owner transaction. Bulk failure and
cancellation writers likewise exclude keyed rows and enter the keyed reducer
explicitly.

Before spawn, one transaction changes `QUEUED` to transient `UPDATING`, updates
`pending_jobs.submit`, stores a random driver token and the immutable local
containment operation, revalidates every adapter `PREPARED` receipt, and commits
durable `LAUNCHING`. The scheduler
retains the same per-job file lock across that commit and
`launch_keyed_contained()` until the exact supervisor and cgroup receipt is
durable or launcher failure is recorded; a live scheduler cannot start a
process after cancellation acquires the lock. The launcher never uses the
legacy background `nohup bash -c` shape.

The root supervisor's `CreateAndLaunchV1` create-or-return call requires and
transitions the exact `PREPARED` ledger. It binds the worker
bootstrap digest, submission and job identity, add and queue digests, owner
UUID, command and launcher digests, control and workload identities, host boot
ID, manager runtime and implementation digests, delegated-root digest,
isolation profile, and the one never-reused cgroup scope UUID already minted in
the immutable containment plan, plus the closed payload-event protocol. It
opens the unique kill
root and payload child FD-relative from its retained delegated-root FD, then
launches the small native shim directly into the payload cgroup with
`clone3(CLONE_INTO_CGROUP | CLONE_PIDFD)`. The child blocks on a
supervisor-owned gate before setup, shell, direct driver, or user effect; the
[Linux clone contract](https://man7.org/linux/man-pages/man2/clone.2.html)
places the child directly in the cgroup named by the supplied v2 cgroup FD. The
supervisor durably records `LAUNCHED`, the root PID and process-start identity,
cgroup2 mount ID, inode and never-reused path, retained cgroup FDs, pidfd
identity, reaper generation, and its retained end of the submission event
channel before responding. Exact response-loss retries
return that record; any changed tuple conflicts. There is one launch for this
containment operation. A pre-receipt exit is failed-driver evidence, not
permission to spawn a second shim under the same submission key; the manager
records its closed pre-effect failure receipt and the reducer enters
`SealFailedLaunchV1` before publishing failure.

Skylet still holds the per-job lock. It opens or adopts the returned pidfd
before inspecting process identity, verifies the supervisor registry and exact
cgroup FDs, and stores the provisional tuple and `LAUNCHED` containment row on
the exact `LAUNCHING` submission. The native shim creates the qualified private
cgroup and mount namespace, hides the host hierarchy, starts as the dedicated
shim UID with only effective `CAP_SETUID`, `CAP_SETGID`, and `CAP_SETPCAP`,
closes every manager
command FD, and verifies its cgroup and immutable launch tuple. Before any user
bytes execute, the fixed native shim creates a private status pipe and forks
exactly once. In the child it clears supplementary groups, sets the configured
payload GID and UID with `setresgid()` and `setresuid()`, clears effective,
permitted, inheritable, ambient, and bounding capabilities, sets
`no_new_privs`, closes the shim event endpoint, reports readiness over the
private pipe, and blocks on the separate supervisor-owned payload gate. In the
parent, the shim immediately drops its three capabilities, sets `no_new_privs`,
retains only the bounded request/ack endpoint and private status pipe, and emits
`PAYLOAD_READY` after verifying the child's credential and gate state. Neither
process opens the job lock or SQLite database. The capability probe executes
this exact handoff and rejects any residual capability, supplementary group,
wrong UID/GID, inherited event or command FD, or pre-gate user instruction.

The trusted Skylet reducer reads the manager's durable `PAYLOAD_READY` receipt,
uses transient `RECEIPTING` to store its digest and mirror only the PID into
legacy `jobs.pid`, and then calls idempotent `ReleasePayloadV1`. The manager
rejects release after any seal, durably records `EFFECT_ADMITTED` before opening
the gate, and returns that receipt. Skylet commits `STARTED` only after exact
`EFFECT_ADMITTED` readback. Response loss is closed: recovery queries the
manager ledger, commits `STARTED` if effect was admitted, retries release if the
gate is still closed, or enters the stored cancel or failure cleanup path if it
was sealed. The unchanged legacy schema has no OS or containment receipt
columns; `jobs.start_at` remains a lifecycle timestamp and is never process
identity. A Skylet crash before provisional publication leaves the gate closed;
recovery stores the exact tuple and continues only if the keyed submission is
still `LAUNCHING`, otherwise it seals and retires the no-effect containment.

The root supervisor is the shim's parent and durable reaping owner. It calls
`wait` exactly once for every generation, including immediate exit, records
exit and reaped evidence in its registry, and wakes Skylet recovery; it never
infers job success from exit code. A supervisor crash reparents the shim to
the qualified host service manager or init, whose subreaping behavior and
registry reconciliation are capability gates. Skylet never calls `waitpid` on
a non-child. Terminal containment requires both the supervisor's reaped
evidence or qualified init adoption and recursive cgroup absence, so repeated
keyed jobs cannot accumulate zombie roots.
If supervisor restart loses a live shim event channel before an outcome was
durably recorded, recovery never reconstructs success from exit status and does
not seal first. After identifying the same ledger and cgroup, the manager writes
the closed `CHANNEL_LOST_V1` outcome receipt, which maps only to
`FAILED_DRIVER`, without performing cleanup. Under the per-job lock, the Skylet
reducer then linearizes that receipt against cancellation. An already committed
`CANCELLING` row keeps cancellation ownership and ignores the synthetic outcome;
otherwise an `EFFECT_ADMITTED` row is recovered to `STARTED` if necessary and
then commits `COMPLETING/OUTCOME_RECORDED` before calling
`SealAfterCompletionV1`. For a gate-closed ledger the manager instead writes
closed receipt `CHANNEL_LOST_PRE_EFFECT_V1`; the reducer commits the restricted
`LAUNCHING -> COMPLETING/OUTCOME_RECORDED` edge and invokes only
`SealFailedLaunchV1`. An identity or adoption gap quarantines without an outcome
or cleanup effect.

After gate release the already demoted payload child execs the qualified direct
driver, while the unprivileged trusted shim remains its parent and outcome
reporter. The driver returns
one closed logical status over a private inherited pipe; the shim forwards an
exactly-once `LOGICAL_OUTCOME_V1` message on the manager request/ack endpoint and waits for
its durable acknowledgment before exiting. An exit without that closed message
becomes `FAILED_DRIVER`; the supervisor itself never infers user success from
an arbitrary exit code. A new session, daemon fork, argv or environment-token removal,
and root-process exit do not move descendants out of the immutable cgroup.
PIDFD, PID, PGID, SID, token, and `/proc` checks may support diagnostics and a
graceful first signal but never authorize terminal release. Generated direct-
driver status functions report only through the private payload pipe; they do
not open SQLite or enter the keyed reducer. The trusted Skylet control
reconciler alone applies the manager's durable outcome receipt under the keyed
state machine. The parent performs no post-spawn PID write and the direct
profile never invokes Ray or Slurm codegen.

`CancelJobKeyed` accepts the exact username, submission key, job ID, add and
queue digests, containment plan digest, cancel operation ID and digest, runtime
UUID, nonce proof, and fixed `TERM_10S_THEN_HARD_KILL_V1` grace policy. The
policy and ten-second duration are covered by the central cancel identity; the
worker mints the one absolute deadline only on first acceptance.
Under the per-job lock it validates the full immutable submission identity and
durably records one matching `CANCELLING` intent, its origin state, current host
boot ID, one `CLOCK_BOOTTIME + 10 seconds` absolute deadline, and cancel phase
`INTENT` before any process action. Exact retries return that stored deadline
and never recompute it. A different boot ID skips remaining grace and proceeds
to hard kill after the qualified reboot reconciliation; it never grants a new
window. A retry with the same identity resumes only that branch:

- from `QUEUED`, no supervisor launch exists. The cancel owner seals the
  planned or prepared local and external containment identities through exact
  `SealNeverLaunchedV1` calls,
  advances every owner to `RETIRED` with its permanent no-later-launch
  tombstone, uses transient `DELETING` to remove the pending row, writes legacy
  `CANCELLED`, writes cancel phase `OWNERS_RETIRED`, and commits keyed
  `CANCELLED`; the scheduler cannot spawn after losing the same lock; and
- from `LAUNCHING` or `STARTED`, the owner resolves only the exact supervisor
  operation and every stored external owner. A missing provisional tuple is
  reconciled through the supervisor ledger and retained cgroup identity, never
  by token or process enumeration. It invokes idempotent `SealAndKillV1`.

`SealAndKillV1` durably writes `SEALED` before any signal and rejects every
later launch, gate release, clone, attach, or reopen for that containment UUID.
It binds the same grace policy, boot ID, and absolute deadline into the
supervisor ledger. The manager persists `GRACE_ENTERED` before a
descriptor-first `SIGTERM` to the verified root. An exact retry may repeat that
safe identity-bound hint but waits only the remaining portion of the original
deadline. The signal and phase are not safety evidence. At expiry, on boot
change, or when grace is inapplicable, it
persists `HARD_KILL_ENTERED` before the authoritative hard stage writes `1` to
`cgroup.kill` through the retained manager-owned kill-root FD. It does not rely
on freezing, process-group membership, or `cgroup.procs`. The supervisor polls
that exact root's `cgroup.events` until recursive `populated 0`, using the
[kernel cgroup-v2 kill and recursive-population semantics](https://docs.kernel.org/admin-guide/cgroup-v2.html), separately
requires its root-child reap receipt, persists `EMPTY`, removes empty descendants bottom-up,
removes the never-reused kill root, persists `RETIRED`, and retains a permanent
no-later-launch tombstone. Skylet mirrors `OWNERS_RETIRED` only after every
planned owner has its receipt. Concurrent forks remain inside the kill root;
session changes, double forks, root exit, and token removal do not weaken the
proof. A missing or replacement cgroup, nonterminal submission with no matching
manager ledger, threaded or writable kill root, failed external-owner proof,
or any escape invariant becomes `QUARANTINED`.

Natural completion has a separate durable protocol and never borrows the cancel
operation or its grace deadline. The supervisor first persists the exact
`LOGICAL_OUTCOME_V1` event and receipt before acknowledging the payload shim.
If the shim or direct driver exits without such an event, the supervisor
persists the closed `ROOT_EXIT_WITHOUT_OUTCOME` receipt, whose only logical
mapping is `FAILED_DRIVER`; manager channel-loss recovery may instead persist
the equally closed `CHANNEL_LOST_V1` receipt before any cleanup. Under the
per-job lock, the trusted reducer accepts one of those receipts only for the
exact `STARTED` owner, mints a completion
operation, binds `CompletionIdentityV1` to the submission, containment plan,
supervisor ledger generation, outcome receipt, and pending legacy status, and
commits `COMPLETING/OUTCOME_RECORDED` before cleanup. A concurrent cancel wins
only if it committed `CANCELLING` first; once `COMPLETING` commits, exact cancel
is a no-op that re-drives completion and cannot replace the user's stored
outcome.

`SealAfterCompletionV1` create-or-returns by that completion identity. It
persists `SEALED` before rejecting every later release, clone, attach, or reopen,
waits for or reaps the outcome-reporting root, then uses the same authoritative
`cgroup.kill`, recursive `populated 0`, bottom-up removal, and permanent
no-later-launch tombstone as cancellation to retire any leaked descendants. The
logical outcome is already durable, so killing a reporter after its receipt
cannot erase or change it. External adapters receive the same completion
identity and must seal and retire their exact owners. Skylet advances completion
phase to `OWNERS_SEALED` and then `OWNERS_RETIRED` only from adapter receipts.
One final `BEGIN IMMEDIATE` transaction uses transient `DELETING` to write the
pending legacy status, remove the pending row, and commit keyed `SUCCEEDED` or
the exact keyed `FAILED` subtype. Crashes after outcome receipt, completion
intent, any seal, empty proof, or retirement resume only the stored completion
operation. No terminal job status is visible before every planned owner is
`RETIRED`.

Every external containment adapter follows the same stored plan and must seal,
cancel, prove exact absence, and retire its owner. Only after every row is
`RETIRED` does terminalization use transient `DELETING` to update legacy status
and remove the pending row before committing keyed `CANCELLED`. Natural driver
completion uses the separate completion operation above; root exit while
recursive `populated=1` is not terminal.
A matching cancel after logical completion is a no-op only after all owners are
retired. A changed identity, cancel-all, capable generic `CancelJobs` call for a
keyed ID, or ambiguous containment performs no unrelated effect. Generic v41
`CancelJobs` and `FailAllInProgressJobs` exclude keyed rows.

Supervisor recovery scans only its exclusive delegated `submissions` subtree
and joins it to the root-owned ledger. It reopens and verifies `LAUNCHED`
owners, repeats kill and empty proof for `SEALED`, and quarantines an unknown
populated cgroup or any ledger/path identity conflict. An unknown empty
directory is never rebound. For a crash after kill-root removal but before the
`RETIRED` write, durable `SEALED`, the permanent UUID, exclusive manager
ownership, and the kernel rule that a populated cgroup cannot be removed permit
that same operation to finish retirement. A host boot-ID change needs its own
qualified reboot receipt and cannot stand in for an external Ray, Slurm, or
other runtime proof.

Skylet recovery re-probes the supervisor and compares its runtime UUID and
capability digest. A mismatch closes new admission while exact existing ledger
entries reconcile or quarantine; there is no process-group or legacy fallback.
`CANCELLING` recovery never launches, and no `STARTED` or pre-receipt keyed
submission spawns a second shim under the same key. Repeated queue and
cancel calls return success only for their matching digest and recoverable
state. Exact lookup returns job ID, all stored digests, authoritative state,
driver and containment receipts, cancel identity and origin, current Skylet
runtime, and supervisor runtime incarnation.

Internal keyed execution requires these versioned gRPC methods and a fresh
runtime-incarnation proof plus the exact active worker lifecycle-fence identity
on every effect. It must not fall back to legacy RPC methods or the SSH
add/queue path.

This remote idempotency is required even though the API request row is durable.
The unary Skylet client retries ambiguous transport failures, and current
`AddJob` and `QueueJob` mutations are not idempotent. Stable HTTP correlation
alone would merely move the lost-response window to the worker.

##### Assignment transaction

Slow observation happens before the transaction. A planner input may contain
readiness, immutable launched resources, display metadata, and task resource
alternatives, but these observations never become authority. The pure planner
returns only the exact service hash, replica ID, worker incarnation, replica
state version, cluster name, worker-fence operation, identity and receipt
digests, launch operation and state, selected task resource alternative,
freshly generated assignment operation UUID, absolute precondition deadline,
prepared containment profile and owner-plan digest, and prepared exec payload
and identity digests. Its module has the low-state
import boundary specified above.
Missing or ambiguous observations yield `RETRY` or `INCOMPATIBLE`, never a
guessed reservation. Display metadata is projected after commit and is not an
input to fencing, capacity, recovery, or teardown.

The PostgreSQL transaction locks in this order:

1. the current `api_controller_leadership` row in shared mode;
2. the `services` row by name;
3. the proposed `replicas` row, with any multi-worker transaction ordered by
   `(service_name, replica_id)`;
4. relevant `job_info` rows ordered by `spot_job_id`; and
5. relevant `spot` rows ordered by `(spot_job_id, task_id)`.

It then re-reads and validates current controller generation, exact job owner,
pool status, service hash, coordination version, lifecycle state, worker
incarnation, replica state version, cluster name, readiness, active worker-
fence operation, identity and FENCED receipt, launch identity and READY state
for a coordinated post-activation birth, and open admission. It recomputes the
task's containment requirements and rejects any profile not present in the
worker's currently read-back capability digest. It then recomputes
capacity from the three capacity-consuming assignment states while holding the
worker rows. Resource-aware pools may pack multiple jobs. Unknown, empty, or
unprovable capacity keeps the characterized fail-closed one-job-per-worker
behavior.

For one winning worker, the same commit:

- stores worker incarnation, operation ID, payload digest, complete identity
  digest, absolute deadline, and `INTENT`;
- stores the selected cluster name; and
- replaces heterogeneous `spot.full_resources` with the exact selected
  alternative.

No partial binding is externally visible. A terminal or cancelling job, an
already capacity-consuming assignment, stale controller ownership, or changed
service or worker incarnation cannot win.

After commit, any controller incarnation may submit or observe the same
operation ID, but only the current owner may publish progress. `sdk.exec`
submits the prepared body with that ID, and `sdk.get(operation_id)` supplies
the exact remote job ID. The caller prepares a random cancel operation UUID and
the complete candidate cancel identity for that returned ID. The `BOUND`
compare-and-set locks the current leadership and job row, checks operation ID,
service hash, worker incarnation, payload digest, identity digest, and returned
request ID, then atomically stores the remote ID and cancel fields. Concurrent
losers discard their candidate and adopt only the stored exact identity. A
stale controller can create or observe the same idempotent effect but cannot
bind a different result.

Recovery of `INTENT` is deterministic:

- an absent API request permits create-or-return with the same prepared body;
- a pending or running request is observed, not duplicated;
- a successful request binds its exact remote ID;
- a failed, cancelled, or missing request triggers exact Skylet lookup by the
  same submission key;
- exactly one digest-matching remote job is adopted;
- an observation reporting no keyed job is followed by an atomic `ADD` seal;
  only `SEALED_ABSENT` permits the old operation to be marked `RELEASED` and a
  later assignment to use a new operation ID;
- when a matching add exists but cancellation must prevent an uncommitted
  queue, an exact `QUEUE` seal and cleanup or exact terminal evidence are both
  required before release; and
- multiple, mismatched, or unprovable effects become `QUARANTINED` and keep the
  worker unavailable for retirement or new capacity.

Every `BOUND` row represents one exact remote task attempt. The normal terminal
reducer locks current leadership, the exact `job_info` row, and the relevant
`spot` task rows in the common order. It verifies operation ID, remote job ID,
service hash, worker incarnation, and an exact terminal, cancelled, or
proved-absent remote observation plus `RETIRED` receipts for every stored
submission containment before changing `BOUND` to `RELEASED`. Pools
accept one task at a time and do not accept parallel job groups. A serial
multi-task job cannot bind its next task until the prior task's one exact
remote ID is released. Crashing after the `spot` terminal write but before
release leaves capacity consumed; recovery queries the exact remote ID and
re-drives the reducer. A terminal `spot` state alone never releases capacity.

Cancellation and reassignment use the same reducer. The capable controller
submits or observes only the stored private cancel operation for the exact
`BOUND` remote ID; cancel-all and public child cancellation never enter the
path. Capacity releases only after that exact API request and remote job reach
cancelled or terminal state, or exact absence is proved, and every local and
external containment owner is retired. Routing fields may
remain for diagnostics, but the mutable job row contains only its current or
latest assignment. This design does not claim a durable history of prior
operation identities. A replacement may overwrite the released assignment and
cancel fields only with a new random operation ID after the old state is
`RELEASED`.

##### Retirement transaction and re-drive

Autoscaling and every other destructive source may continue to rank candidates
from cheap snapshots, but that ranking grants no teardown authority. Before a
source schedules, records, or invokes a destructive effect, it calls
`try_close_worker_admission()` for the exact worker. The coordinator locks the
service and worker in the common order, verifies current controller ownership,
service hash, lifecycle epoch, replica ID, cluster name, replica state version,
worker incarnation, and source-specific domain transition, then locks all
capacity-consuming job bindings in job-ID order.

`GRACEFUL` is used only by queue scale-down and cost rebalance. Any
capacity-consuming binding returns `BUSY` without mutation. `FORCED` is used by
purge, update replacement, service down, preemption, launch failure, user-job
failure, readiness failure, and failed-cleanup re-drive after their existing
domain transition proves that the worker must leave service. It closes
admission even when a binding remains, but it neither releases capacity nor
claims that the remote attempt is absent. For either admitted mode, one commit
sets `pool_admission_closed`, a random `pool_retirement_token`, a distinct
random retirement operation ID, the typed retirement-transition provenance on
both replica and permanent target, and the complete worker-retirement identity
digest. The provenance is null under locked active version `1` and equals the
locked service operation under `-1/DEACTIVATING`; no other version or direction
is admitted. It writes the down identity digest only if a provider object and global
cluster identity are already bound; later authoritative discovery may populate
that digest once before creating the same-ID down request. Assignment winning
first makes graceful retirement busy; closing
admission first makes the worker unavailable to every later assignment. A
forced close may win over a live assignment only because the domain has
already required replacement or shutdown, and managed-job recovery retains
ownership of that exact attempt.

Only after commit may the existing teardown worker run. It carries the exact
service hash, worker incarnation, replica ID, cluster name, global
`cluster_hash`, provider-target digest, provider-object digest, retirement
token, operation ID, identity digest, and any deactivation transition operation
that admitted the close. In central PostgreSQL mode it does not call
`core.down()` directly. It
creates or observes the signed private coordinated-down request whose stable ID
was minted by the close transaction. A controller restart scans closed admitted
replicas and re-drives that same idempotent request regardless of the obsolete
autoscaler selection epoch. SQLite retains its explicitly named direct
compatibility path.

The new executor enters `execute_coordinated_pool_termination()` immediately
before the permanent coordinated teardown effect boundary. In one authorization
transaction it verifies its current execution claim and capable leader,
recomputes the immutable request identity digest, and locks the exact service
and replica to require either active version `1`, or exact version
`-1/DEACTIVATING`. The request projection must equal the replica and permanent
target's immutable retirement-transition provenance. A non-null value must
also equal the current deactivation operation; an already closed version-1
worker retains null provenance and may continue after the service enters
deactivation only under the same immutable birth scope.
Both cases require closed admission, matching service hash, worker incarnation,
replica ID, cluster name, operation ID, close token, and stored digest.
`ACTIVATING` is never accepted. The transaction also locks the permanent target
and requires the same global `cluster_hash`, provider-target digest,
provider-object digest, launch operation, and non-retired state. It then
releases the transaction and selects one of two S4-qualified common actuation
paths. A matching durable cluster handle, including one authoritatively
reconstructed and published from the exact observed object, uses `core.down()`
for that exact permanently reserved name with an internal-only
`ExpectedPoolTargetV1(cluster_hash, provider_target_digest,
provider_object_digest)` value. This value cannot be
supplied by the public API and grants no authority by itself; it is the
immutable precondition prepared by the already authorized request. If the
provider accepted an object but response loss prevents exact handle
reconstruction, the common coordinator does not invent a generic handle and
does not call `core.down()`. It invokes the S4 provider-actuation owner's
`close_observed_target()` primitive with the same cluster hash, target digest,
object digest, attempt ledger, and retirement identity. That owner repeats the
authoritative target query immediately before mutation, records the accepted
close call in the fenced actuation ledger, and returns only provider absence or
ambiguous evidence. Retirement still requires authoritative absence and the
same operation-closing no-later-create receipt. A provider is ineligible unless
S4 specifies and tests either exact handle reconstruction or this target-native
close path for every response-lost accepted object. Admission
is permanent and the name cannot be reused, so an ambiguous or duplicate
provider-down call remains safe after lease loss: the executor may re-drive
only the same closed worker and can never retarget a successor. Log sync, drain
waits, API calls, provider calls, and thread waits hold no database transaction.

For the handle-backed path, the expected target propagates without
reinterpretation through `core.down()`
and `CloudVmRayBackend.teardown()`. Under the existing cluster resource lock,
the backend performs its normal hard status and handle refresh. The coordinated
branch suppresses `core.down()`'s pre-refresh `_maybe_run_down_hooks()` call.
Immediately after refresh and before any SSH, hook, Ray-stop, provider-stop, or
provider-terminate effect, the backend re-reads the live cluster row by name,
requires the same non-null global `cluster_hash`, reconstructs
`PoolProviderTargetIdentityV1` from the refreshed handle and pure projector,
obtains `PoolObservedProviderObjectIdentityV1` from the qualified authoritative
query, and compares both digests in constant time. Only after that match may it
run the retained teardown-hook CAS and hook body against the refreshed handle.
It repeats the same target check after the hook and Ray-stop phases and
immediately before `provisioner.teardown_cluster()` or any legacy-provider stop
or terminate mutation. Missing state, an unsupported projector, a changed
cloud-local name or scope, a hash mismatch, or an ambiguous reconstruction
performs no later remote mutation and leaves the coordinated request retryable
or quarantined. A matching absent observation is returned to the retained
reducer but is not terminal proof without the operation-closing no-later-create
receipt. These are common backend preconditions, not copies in every provider
implementation; a provider is eligible only when all of its hook, Ray, and
teardown effects are downstream of them.

Current `core.down()` and both common actuation preconditions reject a cluster that
resolves to a version-1 coordinated replica unless the active request context
contains that exact verified identity and expected target. This is defense in
depth for capable code, not the stale-binary fence. Every central-PostgreSQL
Serve controller down source, including launch compensation, cleanup, purge,
orphan handling, and replica-manager teardown, is migrated to the typed request
before any pool activates. No generic public `/down` request is allowed to
target an active coordinated worker.

After forced teardown proves exact external absence and the launch side proves
no later create, the owner-fenced recovery reducer marks the permanent target
`RETIRED` and releases each matching assignment using that proof before the
replica row is deleted. If teardown fails or either proof is ambiguous, the closed
worker and every capacity-consuming assignment remain durable. No source may
clear the close flag, replace its token, operation ID, or identity digest, or
delete the row to make progress. Both `_terminate_replica()` and every direct
caller must pass the exact service hash, worker incarnation, replica ID,
cluster name, global `cluster_hash`, provider-target and provider-object
digests, close token, operation ID, identity digest, and any admitting
deactivation transition operation to the coordinated request adapter.
Controller `_cleanup()`, failed-service purge, and orphan cleanup must close
each exact row before creating a teardown thread or request. A version-1 orphan
is an invariant violation and remains quarantined; legacy orphan cleanup is
allowed only for rows without a worker incarnation.

Fresh coordinated birth rejects an existing service row and every currently
active legacy child or nonterminal request binding for the public name. It
allows terminal historical coordinated requests and retired targets only when
their scopes are `CLOSED`; those rows remain immutable evidence, not current
inventory. It does not claim to reconstruct garbage-collected history. Instead,
every version-1 birth uses its new permanent coordination-scope allocation and
target registry, and name-history triggers reject untyped mutation, so a stale
legacy request or retirement tuple cannot target the new physical namespace.
Service deletion and purge may not remove an admitted replica row until cleanup
proves exact absence, no-later-create, and matching assignment release; they
never remove the scope allocation or permanent target tombstone.

##### Fresh activation and rollback

The four releases below are additive and promote no coordinator decision or
external effect until the provider-actuation dependency below is qualified.
They apply Skylet `41`, API-requests
`005`, API version `69`, Serve `033`, and spot-jobs `027` in separate merge,
deploy, and monitoring gates. PostgreSQL version-0 paths preserve their
existing decisions, writes, and locking exactly, and SQLite retains its
existing filesystem-lock path. M5 does not attempt an in-place cutover of
either topology.

Fresh coordinated creation takes a transaction-scoped advisory lock for the
new service name and inserts the permanent scope allocation and service directly
at version `-1` before controller publication, worker row birth, child-request
admission, or provider effect. The insert trigger requires the exact typed birth
marker and permits one pre-existing request only: the authenticated
`JOBS_POOL_APPLY_COORDINATED_V1` row whose request ID equals the birth operation
and whose service name, scope UUID, direction, payload digest, and capable
handler projections match exactly. It rejects every other current service,
replica, version, job binding, nonterminal parent or child request, live cluster
record, or non-retired provider target for the public name, and every row at all
for the new physical scope. Terminal requests and retired targets attached to a
prior `CLOSED` scope do not conflict. Since the server
minted scope did not exist before that request, no version-0 SQL or provider
call can already address it. Migrating an existing live pool in place, or
reusing its physical provider namespace, remains explicitly deferred.

Point-in-time fleet and capability checks are necessary but not sufficient: a
previous binary can start late and ignore a Python fence. Serve `033` and
spot-jobs `027` therefore install PostgreSQL writer-fence triggers. Every
coordinator transaction sets the transaction-local marker
`skypilot.pool_coordination_writer` to the exact value `m5-v1`. For a service
at transition version `-1`, triggers reject every `services` or child
`replicas` insert, update, or delete unless the exact transition marker and
operation match. This fences fresh activation and deactivation. The one marked
terminal deactivation transaction may delete its exact `-1/DEACTIVATING`
service only after it locks the scope row, proves zero child inventory, changes
that scope from `ALLOCATED` to `CLOSED` with the same operation, terminalizes
the matching child request, and removes its queue delivery in the same commit.
It never writes a coordinated version-0 service row. At active version `1`, the service
trigger guards insertion, deletion, and every column change except the
explicitly display-only `uptime` projection. This includes coordination and
transition fields, hash, lifecycle epoch, resource scope, workspace,
controller job/PID/IP/ports, status, pool mode, current and active versions,
requested resources, policy and auto-restart inputs, logical-replica semantics,
placement and cost-rebalance state, and load-balancer authority or demand
state. Those values can select an owner, resource identity, capacity, launch,
or teardown even when they look like controller metadata. Legitimate version-1
setters are marker-enabled; API-role lease and heartbeat fields live in their
separate tables and remain writable through their existing owners.

The permanent-scope registry has its own trigger: only the exact birth
transaction may insert `ALLOCATED`, only the exact final deactivation operation
may move it once to `CLOSED`, and no caller may update its identity, reopen it,
or delete it. Independent service, request, cluster-record, replica, and target
triggers reject any row whose physical scope matches a `CLOSED` registry row,
even when no `services` row exists. For a public name with any coordinated
scope history, they also reject version-0 service inserts and untyped mutating
requests, so an old binary cannot alias a stale request onto a later
incarnation. The only mutating name-level exception after closure is an exact
fresh typed birth with a new scope and its typed batch/deactivation descendants;
read-only requests remain allowed. The fleet-wide process-role fences may
become dormant after the last active coordinated row disappears; these scope
and stale-authority fences do not.

At version `1`, the replica trigger guards insertion, deletion, and changes to
worker incarnation, worker-fence operation, identity or receipt digest, launch
operation, payload, identity or state, admission close, retirement token,
retirement operation ID, retirement identity or down identity digest, service
or replica key, cluster name, status, down status, version, state version,
authoritative JSON state, or its rollback blob. These are authority-bearing because readiness,
launch, drain, and destructive state currently share the JSON projection.
M5-S3 marker-enables the common `add_or_update_replica()`,
`add_or_update_replicas()`, paid-capacity outcome upsert, and exact removal
owners plus every service-status setter that changes a guarded field. It does
not duplicate their domain decisions. Activation inventory and compatibility
tests must prove every legitimate version-1 writer uses one of these marked
storage owners; an unknown writer blocks promotion.

The `job_info` trigger guards changes or deletion involving `pool`,
`pool_hash`, `current_cluster_name`, `job_id_on_pool_cluster`, worker
incarnation, operation ID, payload digest, identity digest, absolute
precondition deadline, containment profile or plan digest, cancel operation ID,
cancel identity digest, or assignment state when either the old or new pool is
protected. The `spot`
trigger guards selected `full_resources` changes and
deletion of rows backing a capacity-consuming protected assignment. Future
coordination contracts use a different marker. This is a compatibility epoch,
not an authentication secret.

Serve `033` also installs four dormant database admission fences for the
destructive effect path. First, an `api_requests` insert or identity-bearing
update whose `cluster_name` resolves to a coordinated replica is classified by
closed request name, handler, and payload semantics. The initial child-mutation
inventory is exactly `CLUSTER_LAUNCH`, `CLUSTER_EXEC`, `CLUSTER_STOP`,
`CLUSTER_DOWN`, `CLUSTER_START`, `CLUSTER_AUTOSTOP`, and
`CLUSTER_JOB_CANCEL`; payload flags such as post-job down are covered inside
their complete canonical payload. Unknown new mutating handlers fail closed.
Read-only status, queue, logs, endpoints, cost, and event requests are
unaffected. At service version `-1` with direction `ACTIVATING`, every child
mutation is rejected. At
direction `DEACTIVATING`, only exact private cancel
for an existing `BOUND` row and coordinated down for an exactly closed replica
may insert or run; worker launch, exec, start, stop, autostop, and every public
mutation remain rejected. At version `1`, launch is accepted only for the exact private coordinated-worker-
launch handler, system user, internal identity version and digest, typed launch
projections, operation ID, service hash, worker incarnation, replica ID,
cluster name, open admission, and immutable launch fields on one `INTENT`
replica. A requeue or claim for the already-created same launch request may
instead join its exact `EFFECT_STARTED` row, subject to the append-only attempt
and no-later-create gates; it cannot create a new request there. Down is accepted only for the exact private coordinated-down
handler, system user, internal identity version and digest, typed authority
projections, operation ID, service hash, worker incarnation, replica ID,
cluster name, close token, and immutable request options already stored on that
closed replica. Ordinary exec is rejected, while private internal pool exec is
validated against the exact `INTENT` job row. Cancel-all, multi-ID, and public
cancel are rejected, while private exact cancel is validated against one
`BOUND` row and its stored cancel identity. Internal identity or authority
projections with no exact durable owner row are rejected. Public `sky.down`,
pre-activation queued child mutation, and changed payload JSON therefore fail
in the database even if application validation is bypassed.

Second, `api_request_queue` insert, claim, requeue, and delete plus request-row
claim and `RUNNING` transitions resolve every classified child mutation, not
only new private requests. At version `-1`, claim, requeue, and transition to
`RUNNING` follow the same direction rule: none at `ACTIVATING`, and only exact
private cancel or down at `DEACTIVATING`. A distinct capable-transition marker
may terminally cancel only a never-running, unclaimed legacy row and delete its
queued delivery; it cannot alter payload or identity. At version `1`, every non-private
child mutation is rejected, including a row inserted while the service was
version `0`. Private worker launch, pool exec, exact cancel, or down
additionally require the transaction-local M5 process marker, an executor
instance advertising the exact handler and implementation digest, and the same
row-to-owner identity match. The request-row update that installs a claim, the
queue transition to claimed, and `PENDING` or `WAITING` to `RUNNING` all
revalidate independently.
An old executor therefore cannot claim or re-drive a coordinated effect. The
capable request-storage owners marker-enable all legitimate lifecycle
transitions for these request types; unrelated requests retain their existing
behavior.

The initial parent-mutation inventory is `JOBS_LAUNCH` when `pool` is non-null,
`JOBS_CANCEL` when its name, job IDs, pool, or all-selector resolves to a pool,
`JOBS_POOL_DOWN`, `JOBS_POOL_DOWN_LEGACY_V0`, `JOBS_POOL_APPLY`,
`JOBS_POOL_APPLY_COORDINATED_V1`,
`JOBS_POOL_DEACTIVATE_COORDINATED_V1`, `SERVE_UP`, `SERVE_UPDATE`,
`SERVE_DOWN`, and `SERVE_TERMINATE_REPLICA` when the payload resolves to an
existing coordinated service or pool. These requests target a controller in
`api_requests`, not a child cluster. Their immutable payload is therefore
resolved against named services, pools, or exact managed job rows. For the
batch handler, names, globs, lists, and `all=true` resolve to the one durably
stored controller-execution target set described below. At version `-1` their insert, claim,
requeue, and transition to `RUNNING` are rejected, except that the exact
`JOBS_POOL_APPLY_COORDINATED_V1` request whose request ID equals the fresh
service birth operation may resume `ACTIVATING`, and the exact
`JOBS_POOL_DEACTIVATE_COORDINATED_V1` request whose request ID equals the stored
transition operation may resume `DEACTIVATING`. A capable
`JOBS_POOL_DOWN` batch parent whose immutable target row references that
exact transition request may also claim or requeue only to commit the terminal
scheduled outcome for that target and continue its other ordinals; it never
waits for child cleanup and receives no service or provider mutation marker.
The same exception admits recovery when the batch's exact next target is still
`PREPARED`, its frozen service hash and scope match the now-committed
deactivation, and it has no route or child yet. Under the service lock the
handler may only record the read-only purge join to that child or the plain-down
already-shutting-down `POLICY_SKIP`, terminalize that target, and advance the
ordinal. It receives no child-operation writer marker in this recovery case.
All exceptions require their typed projections and
the capable process marker; the transition
owner may also terminally cancel a never-running unrelated delivery. At version `1` parent
mutations may run only on a
live executor instance that advertises the exact M5 parent-handler and
coordinator implementation. A service whose birth-publication state is
`PENDING` admits only its exact birth request, matched by immutable birth
operation, until that request publishes or fails and terminalizes. Other
capable handlers then use the same fenced
assignment, cancel, or close path for every child. A queued or claimed old
parent-level request cannot hide behind a controller cluster name. A new
parent mutation request name must be added to this closed inventory before its
handler can target a coordinated pool.

There is one narrow version-1 batch exception. While holding the service lock,
the capable batch handler may mint the child operation and set transaction-local
writer marker `m5-v1` to that child ID only for the single transaction that
inserts the typed deactivation child and delivery, changes the matching service
from `1` to `-1/DEACTIVATING`, and terminalizes the creator target as scheduled.
The trigger joins the marker to that newly inserted child, its creator-batch
projection, and the exact locked target; it rejects any other service column
change or missing transaction member. The parent receives no worker, request-
execution, or provider-effect authority. A later batch joining the already
committed transition receives no mutation marker and only records its own
scheduled target after exact readback.

Third, while any service is at version `-1` or `1`, registration, ready
transition, or lease renewal in
`api_server_instances` is accepted only for the specialized `api`,
`controller`, and `executor` roles with their exact role-specific M5 handlers
and implementation digests. The compatibility `all` role is rejected because
it runs both worker classes and controller daemons without the outer
`api_controller_leadership` generation. A late incapable follower cannot
become a live queue worker even when it does not seek controller leadership.
Existing incapable or `all` leases are activation blockers and must expire for
one full database-clock lease interval.

M5 version `1` is intentionally unsupported for non-consolidated pool or jobs
controllers. Their authoritative Serve database and owner tuple can live
outside the central request/leadership triggers, including through a forwarded
PostgreSQL URI. Activation therefore verifies consolidated pool, Serve, and
managed-jobs modes in durable configuration, proves no remote controller
process or lease remains, and refuses promotion otherwise. Supporting that
topology later requires its controller to register the same capabilities and
participate in the same outer leadership generation; merely sharing a database
URI is insufficient.

Fourth, while any service is at version `-1` or `1`, acquisition, takeover, or renewal of
`api_controller_leadership` requires a separate transaction-local
M5 process marker whose instance ID resolves to a live
`api_server_instances` row advertising the exact coordinator capability. A
fresh old API or controller process cannot acquire or retain legitimate
controller authority after activation. An already admitted old controller
child, executor claim, down request, or teardown thread is instead an
activation blocker and must finish, be cancelled before claim, or prove exact
absence. These database admission fences close the current old-controller
direct-`core.down()` route: new code uses the typed request, a late old leader
cannot become authoritative, and an old executor cannot claim the request.
They fence the repository's legitimate process paths, not arbitrary external
use of leaked provider credentials. Stronger protection against an arbitrary
manually launched old binary would require isolating provider-down credentials
and egress to the capable executor role; that broader credential split is not
claimed by M5 and is recorded as a later M4 action-runtime gate.

Every assignment path performs one of those guarded durable writes before
`sdk.exec` or Skylet mutation. Every destructive source performs the guarded
close CAS before scheduling a thread or creating a down request. A transaction
without the marker fails before the external effect. Activation is forbidden
until tests prove that ordering for every source. The capable-code
`core.down()` and replica-deletion checks remain mandatory defense in depth;
the leadership and request admission triggers cover an old process that cannot
execute those new checks. The migration revision alone is not a writer barrier
because existing schema helpers intentionally tolerate newer additive
revisions. These writer, process, request, and claim triggers remain the durable
enforcement boundary if a stale process appears after image and lease
inspection.

Activation is an explicit operator action, never an automatic consequence of
schema presence. It requires:

1. PostgreSQL consolidated pool, Serve, and managed-jobs state under the
   specialized split-role topology, with no remote/non-consolidated controller
   and no live or recently heartbeating `all` role;
2. every live, ready, non-draining API, controller, and executor role lease to
   advertise the exact M5 capabilities and implementation digest;
3. Kubernetes evidence that every role pod uses that same immutable image and
   no predecessor pod or controller child remains;
4. a launch template pinned to the qualified immutable worker image with
   Skylet `41` keyed add, queue, cancel, lookup, absence-seal, and worker-
   lifecycle-fence support, a first-boot `COORDINATED_V1` bootstrap marker, and
   no SSH fallback; plus the exact `skypilot-containmentd` and native-launcher
   digests, dedicated control and payload identities, and a passing
   `LOCAL_CGROUP_V2_DIRECT_V1` behavioral probe. The initial service permits
   only single-node direct execution and rejects Ray, Slurm, and every
   unqualified external execution owner;
5. a currently unused public service name and a newly server-minted,
   permanently allocated coordination-scope UUID. No service, replica, version,
   managed-job binding, cluster record, permanent target, or provider object may
   exist for that scope, the public name must have no current service or active
   transition reservation, and no nonterminal parent or child request may exist
   for that name except the exact typed birth request itself. Terminal request,
   retired target, and closed child history under a prior closed scope is
   allowed, but no historical row may reference the new scope. Such history
   permanently forces this and every later mutating birth onto the typed
   coordinated path;
6. at least one explicitly named provider whose generation-fenced actuation
   implementation, receipt issuer, authoritative query, and conformance suite
   satisfy the launch closure contract below; and
7. repository and executable-manifest evidence that every assignment and
   destructive source performs its guarded write before an external effect,
   and repository inventory proves every legitimate guarded service or replica
   writer enters a marked storage owner.

Activation request preparation is itself serialized before request insertion.
One transaction takes the global pool-catalog advisory lock and then the
service-name advisory lock, checks permanent scope history and current
inventory, mints the request ID and scope, and inserts the request plus queue
delivery. A partial unique index and trigger permit at most one nonterminal
typed transition request for a public service name. That typed activation row
is also a pending-name reservation: request and service-insert triggers reject
any legacy or mismatched request or version-0 service insert for the same name
until it terminalizes. An exact retry of the same caller operation and payload
returns the existing row. A different concurrent operation loses under the
lock, returns conflict, and persists no second request or scope allocation.
If a pre-birth request terminalizes without inserting a service or scope row, a
later operation may retry with a new scope; once the birth transaction inserts
the permanent allocation, normal deactivation must close that scope and the
scope can never be reused. The logical name may be selected again only by a
fresh typed birth after closure. Crashes before or after request commit therefore cannot leave
two birth owners or make each owner reject the other.

Cancellation of the typed birth is closed by the same request-row
linearization. If cancellation locks the request before any service or scope
row exists, it may terminalize the pre-birth request and remove its delivery;
the later birth transaction observes terminal state and creates nothing. Once a
matching service and allocation exist at `-1/ACTIVATING` or version
`1/PENDING`, the PostgreSQL cancellation path rejects cancellation, does not
signal the activator PID, and leaves the request, claim, and delivery unchanged.
Request-status triggers also reject a direct generic `CANCELLED` update in that
state. Only the capable birth owner may leave it, by successful publication,
proved publication failure, or the atomic activation-abort handoff. Thus user
cancellation cannot erase the sole recovery owner or strand an allocated scope.

A deferred constraint trigger requires every committed version `-1` service to
have exactly one nonterminal typed transition request whose name, scope,
direction, operation, and origin projections match. The partial unique index is
checked in normal statement order and permits at most one such row throughout a
transaction. The same deferred trigger requires a version-1 service with
birth-publication state `PENDING` to retain exactly one nonterminal typed birth
request matching its immutable birth operation and scope; `COMPLETE` or
`FAILED` permits none. The activation-abort transaction uses a dedicated marker and,
after all proofs and locks, first terminalizes the birth request and removes its
delivery, then changes the service to the new deactivation operation, then
inserts that exact deactivation request and delivery. Only the marked service
update may temporarily lack its request inside this transaction; the deferred
trigger rejects commit unless the new owner is complete. A crash rolls back all
three steps. The same service update sets birth-publication state `ABORTED`, so
the terminal birth request cannot later be mistaken for a pending publisher.

`JOBS_POOL_APPLY_COORDINATED_V1` is a distinct `RECONCILE` parent handler and
the only activation entry. Its already durable API request ID becomes the birth
operation, and its typed projection carries the server-minted scope UUID. In
one transaction it locks current specialized controller leadership, its exact
request and claim, the global pool-catalog advisory key, and the fresh
service-name advisory key in canonical order; proves item 5 with
the birth request as the sole exact exception; inserts the permanent scope
allocation; and inserts the service directly with origin `COORDINATED_V1`,
version `-1`, direction `ACTIVATING`, matching birth and transition operation,
and that scope. No external effect or controller child exists before that
commit.

At `-1/ACTIVATING` the database rejects every child mutation and every parent
mutation except recovery of that exact birth request. The activator waits one full
database-clock role-lease interval, then rechecks the immutable image,
specialized leadership, consolidated topology, provider actuation capability,
fresh coordination scope, the unchanged exact birth request, and zero replica,
job, other-request, cluster, and target inventory. One final marked transaction
writes version `1`, clears only the transition fields, and deliberately leaves
birth-publication state `PENDING` and the birth request nonterminal. At version
`1/PENDING`, request and claim triggers admit only that exact request by matching
`pool_coordination_birth_operation_id`; the active-transition unique index
continues to block deactivation admission until birth terminalizes. The birth
owner publishes the consolidated controller and normal parent state through
guarded, identity-checked, idempotent version-1 writers. A crash after the
version-1 commit therefore requeues the same request and reconstructs
publication from the durable service fields rather than looking for cleared
transition columns.

After exact publication readback, one transaction changes publication state to
`COMPLETE`, terminalizes the birth request, clears its claim, and removes its
delivery. A proved deterministic publication failure instead writes `FAILED`
and terminalizes the request in the same order; the service remains a typed
version-1 failed incarnation that can be removed by the ordinary public
deactivation path after the birth owner is gone. The activating abort handoff
writes `ABORTED` while terminalizing the birth request. No public down request
can race a `PENDING` birth, and no success crash leaves a nonterminal request
without a valid domain precondition. Every worker is subsequently a fresh
private-launch birth with a permanent physical target and a coordinated-mode
v41 lifecycle fence before READY.

No existing version-0 service row may take this path. Such pools stay on the
characterized legacy implementation until decommissioned. A formerly used
public name may be selected only after its current state is gone, and it always
receives a new coordination scope and physical namespace. After the first
coordinated incarnation, legacy or untyped recreation of that name is
permanently rejected, but a fresh typed coordinated birth may reuse the logical
name after the prior scope is `CLOSED`. In-place upgrade and physical-scope
reuse are not claimed by M5; current same-name down-then-apply behavior is
preserved through a new typed incarnation.

An `ACTIVATING` transition that cannot finish does not jump directly to version
`0`. Under the same exclusive advisory lock, a marked row-locking abort
transaction first proves version `1` was never committed, no private worker
launch, exec, cancel, or down request was admitted, and no keyed assignment or
retirement effect or replica row exists. It then performs the one legal
direction handoff using the ordered abort transaction above. The exact internal
`JOBS_POOL_DEACTIVATE_COORDINATED_V1` `RECONCILE` request ID becomes the new
transition operation, the service stores the activation operation as parent,
and the terminal birth result records the new deactivation operation. The
service remains at `-1` throughout. Because activation began as a
fresh service birth and performs no effects at `-1`, the ordinary final-zero
inventory verification below can finish without a worker RPC. A crash before the atomic
handoff resumes activation; a crash after it requeues or resumes only the
durable deactivation request.

An old-image rollback is behaviorally safe only while every service is version
`0`, no keyed central or pool-down batch request is nonterminal, and the
coordinated worker inventory is empty. A crash after committing `-1` requires the capable image
to resume its stored direction or perform the guarded activation-abort handoff;
the fact that no service reached `1` does not make an old binary safe. After
activation, an old binary is likewise unsafe even though its guarded writes
fail.

Public deactivation is owned by the durable outer
`JOBS_POOL_DOWN` request under the caller-visible
operation ID. `JobsPoolDownBody` and the SDK and CLI shapes remain unchanged:
one name, multiple names, glob patterns, `--all`, and mixed legacy and
coordinated results are supported. Name expansion remains at controller
execution, as it is today. New capable rows carry the batch identity version
and stable handler `sky.jobs.server.core:pool_down_batch_v1`, explicitly
registered `RECONCILE`; pre-M5 untyped rows retain handler
`sky.jobs.server.core:pool_down`, its `NEVER` policy, and the version-0
compatibility loop and can never claim against a coordinated target. The jobs
endpoint chooses the handler before `prepare_request()` and persists it on the
row; it also passes `retryable=True` and the existing short schedule explicitly,
so executor loss enters the persisted `RECONCILE` continuation instead of the
default non-retryable failure path. No row-level replay-policy switch is
introduced.
On the first claim, the capable handler performs the existing controller-
accessibility and names-versus-`all` validation before any lifecycle mutation.
It then locks its request and claim, takes the global pool-catalog advisory
lock, resolves the selector with the existing pool/glob and purge-orphan rules,
and takes every resolved service advisory lock in sorted order. One transaction
revalidates and persists only the frozen batch and ordered `PREPARED` target
identity rows, with null route and child. It does not admit every child at once.
Retries use that stored target set and never add a pool that appeared after the
selection linearization point. A committed empty
batch distinguishes no matches from a pre-resolution crash and retains the
current `No pool to terminate` result.

The parent processes exactly one stored ordinal at a time. Under that target's
service lock it first revalidates the frozen hash and scope, then performs the
same current status, purge, and nonterminal-job policy checks that the existing
sequential loop performs at that ordinal. A deleted or replaced incarnation is
a structured soft result and is never retargeted. A `LEGACY_ORPHAN` target
additionally requires that no service row now exists and that the exact orphan
inventory digest is unchanged; otherwise it stores the same changed-target soft
result and requires a later public command. A qualifying legacy target
receives a parent-specific `JOBS_POOL_DOWN_LEGACY_V0` child with
`ReplayPolicy.NEVER` that calls the explicitly named one-name version-0
compatibility implementation with the original `purge` semantics and frozen
service or orphan precondition. A qualifying
active coordinated target receives one
`JOBS_POOL_DEACTIVATE_COORDINATED_V1` `RECONCILE` child. A policy-skipped target,
including a non-purge failed pool or a pool with a current nonterminal managed
job, atomically stores the characterized warning, `POLICY_SKIP`, and no
destructive child. For a coordinated target, child admission, the structured
`scheduled to be terminated` outcome, exact service transition to
`-1/DEACTIVATING`, target `TERMINAL`, and next-ordinal advance occur in one
transaction. The child then reconciles provider absence,
target retirement, and service deletion independently; the caller-visible
`SHORT` request never waits for those potentially unbounded proofs. For a
legacy target, the parent alone yields `WAITING` while the one-name child runs,
then stores its characterized synchronous result before advancing. A soft skip
advances; the first hard legacy-child error terminalizes the live parent with
the same exception and no later child is admitted. This preserves the current
sequential prefix effects, fail-fast boundary for the synchronous legacy path,
message ordering, scheduled-name summary, and public null result without
parsing logs or turning `pool down` into a cleanup wait. A mixed batch can
therefore resume a completed prefix without repeating it.

Batch-v1 cancellation uses a handler-aware PostgreSQL transaction rather than
the generic terminalizer alone. It locks the request, claim, batch, and targets;
marks the batch `TERMINAL`; converts every still-`PREPARED` target to a
structured cancelled `POLICY_SKIP`; leaves already admitted children and their
immutable target references intact; sets the request `CANCELLED`; clears
`claim_token`, `worker_instance_id`, `lease_expires_at`, and `pid`; and removes
the queue delivery in the same commit. A legacy child completion may still
write its target result for audit after parent cancellation, but cannot admit a
later target. A coordinated child never depends on parent status or claim and
continues from its own typed transition. Cancellation therefore prevents later
admission without revoking an admitted effect or leaving a claimed observer
that can block final service deletion.

Child preparation takes the service-name advisory lock and uses the active-
transition unique index before insertion. For an active version-1 service it
revalidates the frozen target's hash, scope, status, purge policy, and
nonterminal managed-job set before any child exists. A policy change writes the
structured soft target result with no child. Otherwise one transaction creates
one random child ID, uses it as the service transition operation, inserts the
typed child and delivery, writes service version `-1`, direction
`DEACTIVATING`, and that operation, records the target's terminal scheduled
outcome, and advances the batch ordinal. No committed state contains an
admitted child while the service remains active. If
another batch already created the child, or the service is already
`-1/DEACTIVATING`, the new batch references the exact stored child after
service hash, scope, direction, and authenticated authorization checks and does
not insert another request only for a purge re-drive. That joining transaction
locks the service, child, and target, verifies the already committed transition,
then stores its own terminal scheduled outcome and advances its ordinal without
a service writer marker. A plain down that observes
an already admitted deactivation stores a `POLICY_SKIP` target with the
structured already-shutting-down no-op result and exposes no shared child ID.
A conflicting activation remains the sole owner and makes the batch retry
without changing direction.
Thus two named, glob, or `--all` requests in either lock order create at most
one transition child, while each caller retains its own durable aggregate
operation and result and legacy children are never deduplicated. The
activation-abort handoff creates the same specialized child directly, without
an outer public batch, in its existing atomic transaction.

The per-service `JOBS_POOL_DEACTIVATE_COORDINATED_V1` request ID is the
transition operation.
Under the exclusive service advisory lock, the deactivation handler locks its
own request and claim. A child admitted from active version `1` has a non-null
creator-batch projection and must find the immutable creator target already
terminal with its scheduled outcome plus the exact matching service at
`-1/DEACTIVATING`; mutable pre-transition policy is never rerun after that
admission transaction. An activation-abort child instead has a null creator-batch
projection, carries the birth operation in its typed parent-operation
projection, must match the service's immutable activation parent operation, and
is admitted only by the atomic abort handoff above. Once either origin has
committed the exact `-1/DEACTIVATING` transition, recovery never re-runs mutable
pre-transition policy and parent cancellation cannot revoke the admitted child;
it only revalidates the stored service, scope, direction, and operation and
continues reconciliation. This direction rejects
new worker launch, exec, assignment,
start, stop, autostop, and public cancellation, but permits the exact
deactivation parent request and only the private cancel and coordinated-down
children needed to drain existing bindings and workers. Claim expiry and the
existing `RECONCILE` queue protocol requeue that same parent after a crash; no
process-local scanner or inferred owner is authoritative. The handler
reconciles or cancels every launch delivery, fences every prior launch execution
owner, terminalizes assignments, force-closes every worker with the transition
operation bound into new down identities, re-drives each worker's one private
down, and
proves provider absence plus no later create before marking the permanent
physical target `RETIRED` and deleting the replica row. A worker already closed
before deactivation retains its exact earlier down identity and may finish
under the same immutable birth scope. An `ABSENT` launch row may be deleted only
after the target is `RETIRED`; an ambiguous or quarantined launch keeps its
replica and cleanup owner and holds the service at `-1`. The local lifecycle
fence is never released: it disappears only with the permanently terminated
worker.

A final transaction takes the global pool-catalog advisory lock and then the
service advisory lock in canonical order and requires zero replica rows, capacity-consuming
assignments, other effect-bearing keyed requests, launch or down effects, other
claims, threads, live provider targets, and stale-role leases. It permits the
exact typed deactivation request and its current matching claim plus zero or
more immutable `JOBS_POOL_DOWN` batch target rows that reference that child.
Those targets are already terminal scheduled outcomes and grant no service-
mutation authority; their parent requests may be succeeded, failed, or
cancelled and may retain only historical execution fields. The finalizer locks
the child, referenced parent requests, and target rows in request-ID order but
does not require or rewrite parent status,
then atomically stores that request's terminal success result, marks the
permanent coordination-scope allocation `CLOSED` with the same operation,
deletes the exact coordinated service row through the marked terminal-delete
exception, and deletes the request's queue delivery. It never exposes a
committed coordinated version-0 row. A crash
before this commit leaves the same request resumable at `-1`; after commit
there is no service row or nonterminal transition owner, while the closed scope
and stale untyped authority remain permanently fenced. Only after every remaining service is
legacy version `0`, the coordinated worker inventory is empty, and the capable
batch and typed-request inventory is terminal may the capable fleet drain and
an old image roll out. Its database writes still cannot
recreate a closed physical scope or issue an untyped mutation against a name
with coordinated history. A capable image may later create that logical name
with a new typed birth and new scope. Otherwise rollback is a forward fix.

##### Milestones and removal gates

M5-S0 is split into two ordered merge, deploy, and monitoring gates. M5-S0a
installs only the exact v1 side tables and named indexes specified above, keeps
Skylet `40`, and has no trigger, side-row producer, or capability claim. Its
upgrade and rollback tests must pass before merge. Deployment proves the API
image and newly provisioned or naturally restarted v40 workers retain ordinary
legacy behavior; it does not force-restart the existing worker fleet and cannot
count as the v41 compatibility gate.

M5-S0b adds only Skylet `41` keyed add, queue, exact cancel, observational
lookup, atomic absence seal, validation and consumption of the additive side
tables, and the local mutation and side-identity triggers,
supervisor prepare, launch, payload-event, completion, and retirement receipts,
receipt-aware scheduling, the dormant
`SubmissionContainmentAdapterV1` reducer, root-owned
`skypilot-containmentd` protocol and native-launcher client, local
containment-ledger mirror, local idle-teardown claim and worker-lifecycle-fence
RPCs, immutable runtime-mode bootstrap reader, and compatibility tests. It does
not alter the legacy job or pending table, contains no bootstrap marker
producer, and no central caller sends a key, launches a containment, or installs
a fence. Legacy
mode preserves the existing `StopEvent` and `SetAutostop` implementation and
provider retry behavior exactly; the new durable teardown protocol is exercised
only by isolated coordinated-mode tests. S0b force-restarts the capable worker
canary, merges, deploys, and completes the actual v41 worker compatibility
monitoring gate before M5-S1 begins.

M5-S1 adds only API-requests `005`, API version `69`, HMAC-authorized stable
internal worker launch, exec, exact cancel, and coordinated-down
create-or-return routes, typed launch, cancel and down authority projections,
stable-ID middleware behavior, existing-field role capability advertisement,
prepared identity hashing, redaction, and response-ID verification. No pool
caller sends a key, launch or cancel identity, or down authority and no pool
service activates. It has its own merge, deploy, and monitoring gate.

M5-S2 adds Serve `033`, spot-jobs `027`, the dormant coordinator schema, pure
planner, transactions, writer and request/process admission triggers, fresh-
birth and deactivation state machines, and shadow comparison. Every
service remains version `0`; no coordinator decision or external effect is
promoted and no activation request is registered. It has its own merge, deploy,
and monitoring gate before any source migration.

M5-S3 migrates worker row birth and private launch, assignment, exact
cancellation, normal terminal release, serial-task release, and every graceful
and forced destructive source one bounded change at a time, including parent
pool requests and the typed coordinated-down adapter. It also adds the dormant
single-node direct execution producer and rejects Ray, Slurm, multi-node, and
other unqualified containment profiles before coordinated assignment.
Each source change merges, deploys, and proves its version-0 shadow or
compatibility behavior before the next one. It stops with every service at
version `0` and the coordinated branches unreachable.

M5-S4 is a hard external dependency, not an assumed receipt. Before registering
`JOBS_POOL_APPLY_COORDINATED_V1`, the canonical design must be updated in place
to name at least one pilot provider and the exact generation-fenced actuation
owner, canonical signed receipt payload and issuer key rotation, internal retry
coverage, authoritative query, close protocol, and conformance fixtures. That
implementation must merge, deploy, and pass its own adversarial review. No
current provider is declared eligible by this document. Only after that update,
repository search, the executable removal manifest, writer-order tests,
request/process admission tests, and executable
`LOCAL_CGROUP_V2_DIRECT_V1` supervisor and escape conformance may one newly
scoped PostgreSQL test pool be
born at version `-1` and promoted to `1`. Existing pools remain version `0`.

The PostgreSQL `get_next_cluster_name()` filesystem-lock scheduling body and
direct pool teardown admissions can be removed only after every production pool
has been recreated under coordinated authority and the rollback window closes.
Until then they live solely in an explicitly named PostgreSQL version-0
compatibility implementation; SQLite retains its separately named
compatibility implementation rather than an implicit fallthrough.

##### Required verification

The design is not promotable without all of the following:

- assignment versus retirement in both lock interleavings proves exactly one
  winner;
- two jobs at one capacity boundary never overbook, while resource-aware
  multi-job packing and heterogeneous resource choice remain exact;
- stale controller generation, stale service controller owner, untyped or
  old-scope service recreation, worker UUID mismatch, replica-ID reuse, and
  worker-name reuse perform zero writes and zero effects; a fresh typed
  same-logical-name birth succeeds only with a new scope after prior closure;
- terminal and cancelling jobs cannot acquire an assignment;
- normal terminal completion, cancellation, forced exact absence, and
  serial-task advance release exactly one matching `BOUND` attempt, while a
  terminal `spot` write alone retains capacity and pool job groups remain
  rejected;
- concurrent `BOUND` publishers persist one cancel operation and identity;
  exact repeated cancellation is idempotent, while cancel-all, multiple remote
  IDs, released assignments, and wrong service or worker identity perform zero
  mutation;
- queue scale-down, cost rebalance, purge, update replacement, service down,
  preemption, launch failure, user-job failure, readiness failure, and failed
  cleanup all close the exact worker before scheduling or producing a
  destructive effect;
- private worker launch response loss before request acceptance, after
  target-scope and global-cluster-hash publication, before provider entry, after
  provider creation, after object and handle publication, after Skylet
  readiness, after local fence install, and before READY either adopts the one
  matching operation, re-drives only after provider-specific proved absence and
  no-later-create receipts for every earlier execution generation, or
  quarantines. Never-accepted and pre-publication absent attempts retire without
  constructing a down request, while an observed object requires its immutable
  object digest. In the exact race where owner A passes its effect fence,
  owner B observes absence, and A resumes late, B cannot re-drive, retire, or
  delete until A's generation is formally fenced; any late matching create is
  adopted or closed by the retained owner. A stale request after replica
  deletion or reuse performs zero effect, and a new version-1 insert cannot use
  a null-launch exception;
- fault injection after assignment commit, after HTTP acceptance, after
  Skylet add, after Skylet queue-file rename, before driver spawn, after spawn
  before the supervisor receipt, after `LAUNCHED` before Skylet publication,
  after `PAYLOAD_READY`, after `EFFECT_ADMITTED` before SQLite `STARTED`, after
  SQLite `STARTED` before job effect, before remote-ID binding, after retirement commit, and before
  teardown all converge without duplicate effects or a second shim. Every
  `LAUNCHED` but gate-closed failure first commits the restricted failed-launch
  completion identity, then re-drives only `SealFailedLaunchV1` through seal,
  empty proof, retirement, and terminal `FAILED_DRIVER`;
- duplicate internal request IDs and submission keys return the one matching
  object, while payload, user, handler, cluster, add-digest, queue-digest,
  containment profile, plan, manager, or owner conflicts fail closed. Duplicate
  HTTP create never enqueues, including for a
  terminal row; post-effect `ExecutionRetryableError` passes
  `RUNNING -> WAITING`, old-claim requeue, crash between those transactions, expired-claim
  recovery, and next-claim generation increment without losing queue priority
  or preconditions. Batch-v1 endpoint tests assert the persisted distinct
  handler, `retryable=true`, and `RECONCILE` recovery, while legacy pool-down
  rows retain `retryable=false` and `NEVER`;
- add versus `ADD` seal and queue versus `QUEUE` seal pass in both lock orders;
  only one side wins, matching repeated seals are idempotent, mismatched seals
  fail closed, a committed seal permanently rejects the delayed mutation, and
  a winning queue seal terminalizes the exact add-only `UNQUEUED` shell with no
  pending row or process and advances every planned or prepared containment owner to
  `RETIRED` with an immutable never-launched receipt;
- `PrepareOwnerV1` response loss before and after manager-ledger commit, a crash
  with side state `PLANNED` and manager state `PREPARED`, and a crash after
  `LAUNCHING` before `CreateAndLaunchV1` all join the exact owner without a
  cgroup or second launch. Queue rejects any owner without a verified prepare
  receipt, and `SealNeverLaunchedV1` retires both planned and prepared cases
  with one permanent tombstone;
- the Skylet migration preserves the legacy `jobs` and `pending_jobs` schemas,
  duplicate legacy pending rows, and ordinary v40 writes; persistent triggers
  reject a same-transaction second keyed pending insert, orphan future-job
  pending row, stale-v40 pending update before spawn, status or PID update,
  and pending delete, while capable generic `CancelJobs` and
  `FailAllInProgressJobs` reject or exclude keyed rows and ordinary legacy
  scheduling remains unchanged;
- periodic status refresh during keyed `QUEUED` and `LAUNCHING` never applies
  the legacy PID or reboot heuristic, every keyed status writer enters the
  identity-bound reducer, payload code has neither DB nor command-socket access,
  and direct keyed shim-to-driver execution preserves its
  exact supervisor operation, root PID/start diagnostics, and cgroup identity.
  The executable handoff proves the shim alone starts with only `CAP_SETUID`,
  `CAP_SETGID`, and `CAP_SETPCAP`, the child reaches the exact payload UID/GID with zero residual
  capabilities before gate release, the shim drops its own capabilities, user
  code inherits neither event nor command FD, and each bounded event receives
  only its matching durable acknowledgment;
- exact `CancelJobKeyed` versus scheduling passes both lock orders from
  `QUEUED` and `LAUNCHING`, including response loss before and after cancel
  intent, immutable grace-deadline commit, `GRACE_ENTERED`, TERM hint, deadline
  expiry, `HARD_KILL_ENTERED`, kill-root creation, clone, supervisor receipt,
  gate release, payload-ready and effect-admitted receipts, durable seal,
  recursive kill, empty proof,
  reap, rmdir, retirement, and terminal commit. Retries and process restarts on
  the same boot use only the remaining original grace, and boot change grants
  zero new grace after qualified reboot reconciliation. The executable payload double-forks, calls `setsid`,
  reparents, clears environment and argv markers, closes inherited FDs, exits
  its root first, and runs a fork storm during kill; cancellation still reaches
  exact `SEALED`, recursive `populated 0`, root reaped, and permanent
  `RETIRED`. Allowed nested migration remains below the manager root, while
  ancestor, sibling, host-namespace, same-UID manager, FD, socket, ptrace,
  systemd, container-socket, and external-spawner escape attempts fail the
  capability probe. No PID, process-group, token, `cgroup.procs`, or negative
  scan result can substitute for that proof;
- cancellation from `QUEUED` retires every planned or prepared local and external owner
  without launch, process, or provider effect before publishing keyed
  `CANCELLED`; a missing never-launched receipt leaves the job nonterminal;
- keyed containment recovery covers exit before waiter registration, natural
  driver terminal with a surviving daemon, exact cancellation, Skylet restart,
  manager restart with populated and sealed roots, host reboot identity change,
  unknown populated cgroups, path or inode replacement, and crashes after
  mkdir, clone, `LAUNCHED`, `SEALED`, kill, `EMPTY`, rmdir, and before
  `RETIRED`. Terminal publication observes zero zombie roots, every owner is
  retired, and repeated keyed jobs do not accumulate cgroups or zombies;
- failed-launch cleanup covers response loss and process or manager crash before
  and after its outcome receipt, `COMPLETING`, `SEALED`, `EMPTY`, and `RETIRED`;
  cancel-versus-failed-launch both lock orders select exactly one owner, an open
  gate rejects `SealFailedLaunchV1`, and no failure becomes terminal before all
  owner receipts are retired;
- natural success, setup failure, user failure, and root exit without an
  outcome each persist one immutable manager outcome receipt, commit
  `COMPLETING`, seal and retire every owner, and only then publish the exact
  terminal legacy status. Faults after outcome receipt, completion intent,
  `OWNERS_SEALED`, hard cleanup, and `OWNERS_RETIRED`, plus both cancel-versus-
  completion lock orders, re-drive one completion identity without losing the
  outcome or exposing terminal state early. Manager restart with a lost event
  channel writes `CHANNEL_LOST_V1` before cleanup and both lock orders prove
  that either cancellation or completion, never an emergency seal outside the
  reducer, owns retirement;
- capable-v41 generic `CancelJobs`, cancel-all, wrong-runtime, wrong-digest,
  wrong-containment, delayed retired-owner, and PID-reuse cases perform no
  process, cgroup, supervisor, or database mutation for a keyed row;
  a v40 listener or independently launched v40 service process blocks
  activation and rollback rather than satisfying a zero-signal claim;
- direct single-node containment passes qualification, while single-node Ray,
  multi-node Ray, Slurm, rootful Docker, host `systemd-run`, unmanaged MPI or
  SSH fan-out, and every missing external owner fail before assignment. Future
  adapters pass exact-owner, response-loss adoption, durable seal,
  authoritative absence, no-later-effect, and composite all-owners-retired
  conformance before advertisement;
- coordinated-mode `StopEvent` decision versus lifecycle-fence install passes both local-lock
  orders: the fence wins and no teardown claim or provider effect occurs, or
  the event's durable claim wins and install fails. Delays after config read,
  idle decision, claim commit, indicator write, hook claim, provider entry, and
  provider failure never produce fence-success followed by teardown. Crash or
  exception after `PROVIDER_ENTERED` in an isolated coordinated-mode S0 test
  becomes `AMBIGUOUS`, performs no provider re-drive, and blocks install until
  explicit proof or worker down;
- matching worker-fence install, readback, process restart, and
  response-loss retries are idempotent; changed service, worker, cluster,
  operation, config, or runtime identity fails closed; active keyed work and
  ambiguous idle teardown block install. A missing, changed, or late bootstrap
  marker cannot enter coordinated mode; legacy-mode v41 uses the exact old
  autostop, hook, indicator, exception, and provider-retry path with no new
  lifecycle lock or attempt state;
- response-loss retries reuse the exact stored absolute precondition deadline
  and identity digest before and after terminal queue-row deletion;
- an absent or garbage-collected API request treats exact Skylet lookup as
  observation only; release requires a matching committed `SEALED_ABSENT`, and
  ambiguous pending, status, PID, or receipt evidence quarantines;
- forged HMACs, replayed timestamps, caller-supplied leader headers, public
  service accounts, middleware-local IDs, response-ID mismatches, and tampering
  with any exec, cancel, or down identity member cannot select a private
  request ID; down tampering includes payload or producer version,
  `terminate`, `purge`, `graceful`, `graceful_timeout`, and `user_initiated`;
- legacy Skylets return `UNIMPLEMENTED` for the new methods, v41 legacy mode
  returns `FAILED_PRECONDITION` for coordinated mutations, and a keyed request
  bound to a prior Skylet or containment-manager runtime performs no new
  mutation after restart while its exact durable owner is reconciled or
  quarantined;
- raw old destructive request inserts, claims, requeues, and transitions to
  `RUNNING` are rejected for version `-1` or `1`; the exact private worker
  launch, exec, cancel, and down requests plus the one direction-matching birth
  or deactivation child request alone mutate on a capable executor. A typed
  pool-down batch may use the exact child-operation writer marker only in its
  atomic version-1 child-admission and service-transition transaction. At
  deactivation it may claim only to commit a later joining target and process
  unrelated ordinals, never to mutate the service or provider.
  Launch and down revalidate immediately before effect, and down returns the
  retirement operation ID. Transition triggers require both parent projections
  null for birth and exactly one for deactivation: the activation birth
  operation for abort, or the first creator batch for public down;
- public launch, start, stop, down, autostop mutation, ordinary exec, and exec
  or launch with down enabled cannot target a coordinated worker; private
  worker launch must match row-birth authority and private exec and cancel must
  match the exact assignment identity; and queued, claimed, or running parent-
  level pooled `JOBS_LAUNCH`, `JOBS_CANCEL`,
  `JOBS_POOL_DOWN` name, list, overlapping glob, `--all`, and mixed v0/v1
  cases freeze one execution-time snapshot, admit children strictly in order,
  wait only for characterized short legacy children, terminalize coordinated
  targets either in the same transaction that newly commits the exact
  `1 -> -1/DEACTIVATING` transition or, for a concurrent purge re-drive, after
  locking and verifying the already committed exact transition child. A crash
  with the later batch's next target still `PREPARED` can reclaim only to store
  that read-only join or plain-down skip, then continue. These paths preserve soft
  warnings and the legacy first-hard-error prefix behavior, retain the caller's
  parent ID, and converge concurrent coordinated purges on one exact
  service-transition child. Old rows persist handler `pool_down` and recover as
  `NEVER`; new typed rows persist handler `pool_down_batch_v1` and recover as
  `RECONCILE`. Cancellation atomically clears the parent claim, terminalizes
  unadmitted targets, and leaves admitted children independently resumable.
  Existing pooled `JOBS_POOL_APPLY`, Serve up, Serve down,
  Serve update, and terminate-replica cases are fenced in activation and
  execution, while the typed coordinated birth and deactivation parents are
  accepted only for their exact stored transition operations;
- writer and leadership triggers reject representative old assignment SQL,
  destructive transitions, late controller acquisition, and stale executor
  claims before an external effect, including when a stale process starts after
  the point-in-time fleet check;
- the coordinated request adapter, common backend teardown precondition, and
  replica deletion reject missing or stale service hash, worker incarnation,
  replica ID, global `cluster_hash`, provider-target or provider-object digest,
  close token, retirement operation ID, transition operation, retirement
  identity, or down identity for `_terminate_replica()`, controller cleanup,
  both purge paths, failed-launch compensation, and direct bulk deletion. Fault
  injection after outer authorization, after handle refresh, before and after
  the refreshed teardown hook, after Ray stop, after the cluster-row re-read,
  and immediately before provider teardown proves that a changed cluster hash,
  cloud-local name, provider scope, canonical target locator, or observed object
  produces no mismatched SSH, hook, Ray, or provider call. Exact absence remains
  nonterminal without the no-later-create receipt, and coordinated orphan rows
  remain quarantined. A response-lost accepted object with no published handle
  either reconstructs and publishes the exact matching handle before
  `core.down()` or uses the S4 target-native `close_observed_target()` ledger;
  inability to do either quarantines and never synthesizes a handle;
- coordinated birth rejects an existing service, current legacy managed-job or
  remote-worker binding, live cluster or provider inventory, every nonterminal
  child or parent request bound by exact name, and every row bound to the new
  scope except the typed birth request, replica, and version metadata. Terminal
  rows for a prior closed scope remain accepted history. An unresolved glob or
  `--all` batch is ordered by the same catalog lock and, if it resolves later,
  is subject to the transition claim fences rather than treated as hidden birth
  authority.
  It atomically reserves one server-minted scope UUID that is never reused and
  proves the provider namespace empty; deleted logical-name history is neither
  required nor misrepresented as queryable. The service insert commits version
  `-1` before any controller or child effect, and only the exact birth request
  may resume activation. Faults immediately before and after the version-1
  commit, each idempotent controller-publication write, publication readback,
  and atomic birth terminalization resume from immutable birth operation and
  `PENDING` publication state; deactivation cannot race that owner. A crash at
  version `-1` or version `1/PENDING` requires capable recovery and never
  permits old-image rollback. Birth-request cancellation in both lock orders
  either terminalizes before scope allocation and prevents birth, or is
  rejected after allocation without changing or signalling the sole recovery
  owner. Live or recent `all` roles,
  non-consolidated controllers, forwarded-database remote owners, and any
  provider without the separately qualified actuation runtime block birth. Two
  concurrent birth preparations in either lock order persist exactly one typed
  request; the loser leaves no request or scope. After final deactivation the
  closed scope permanently rejects its service, request, cluster, and child
  identities; old-image or untyped mutation of that logical name is also
  rejected. A same-name capable reapply succeeds only as a fresh typed birth
  with a distinct scope and empty provider namespace. Activation abort fault injection before
  and after birth terminalization, service handoff, and deactivation-child
  insertion either rolls back to the exact birth owner or commits one matching
  deactivation owner, never zero or two;
- deactivation refuses active bindings, unresolved effects, and incomplete
  retirement, every nonterminal or quarantined launch row, and every admitted
  private launch request. Its exact per-service `RECONCILE` child survives claim expiry and
  resumes after crashes before and after transition commit, launch-owner
  fencing, exact cancel, worker close, provider absence, no-later-create
  receipt, target retirement, replica deletion, and final service-row deletion.
  New child downs bind the deactivation operation, preclosed workers retain
  null retirement-transition provenance across the later `1 -> -1` transition,
  close requests admitted during deactivation carry its exact non-null
  operation, and `ACTIVATING` never authorizes down. The final transaction
  admits its own typed request and live claim plus immutable terminal batch
  target references regardless of their parent request's terminal status,
  while rejecting every other effect-bearing request or claim, closes the
  permanent scope, terminalizes the child,
  and deletes the coordinated service row and delivery atomically without
  exposing version `0`. Old-image rollback refuses an active keyed worker row,
  pending row, non-retired containment, active local lifecycle fence,
  coordinated bootstrap marker, armed autostop configuration, or nonterminal
  typed batch;
- current and final manifest validation retains PLA-GAP-005 until one named
  pilot provider closes the S4 actuation and receipt contract, and the
  PostgreSQL version-0 assignment and teardown compatibility owners cannot be
  removed before zero legacy production-pool inventory and rollback closure;
- deterministic multi-worker and multi-job lock ordering passes deadlock and
  retry tests;
- planner import tests enforce the low-state dependency boundary, and proposal
  serialization contains no provider objects, SDK objects, readiness snapshot,
  or display metadata;
- no provider, SDK, SSH, Skylet, file upload, log sync, or thread wait occurs
  inside a coordinator transaction; and
- feature-off PostgreSQL and every SQLite test preserve legacy return values,
  exceptions, field timing, and scheduling behavior.

Rejected alternatives are a second idle check under the filesystem lock,
holding a database transaction through `sdk.exec` or teardown, migrating only
assignment or only retirement, identifying workers by replica ID or cluster
name, recovering by non-unique job name, allowing keyed SSH fallback, mirroring
authoritative rows in a generic action table, treating PID, process group,
token scans, or a head-node cgroup as whole-job ownership for Ray or Slurm, and
copying dstack's lease token or bare-process kill without end-to-end
idempotency and authoritative runtime ownership. Each leaves either a cross-process race, an
ambiguous external effect, a stale-incarnation mutation, or a second source of
truth.

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
| central-PostgreSQL pool assignment inside `get_next_cluster_name()` under a service `FileLock` | `PoolWorkerCoordinator` owns exact worker reservation and selected-resource binding while SQLite has an explicitly named compatibility implementation | assignment, retirement, capacity, identity, crash recovery, writer-fence, and exact-effect tests pass, one release records no legacy PostgreSQL entry, and PLA-M5-015 reaches `removed` |
| direct central-PostgreSQL pool teardown admission through `_terminate_replica()` callers and failed-service purge | every graceful and forced source closes the exact worker through `PoolWorkerCoordinator` before scheduling or producing a destructive effect | every inventoried source passes pre-effect close-token and stale-incarnation tests, one release records no unfenced admission, and PLA-M5-016 reaches `removed` |
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

### Review 24

Verdict: `PURSUE` for the corrected M4 deploy-variable snapshot foundation
and DigitalOcean pilot subsection at SHA-256
`7188af83f30eac72dffb5ac9d85d461a2511d8b6e7b3529401787b19b03f012f`.
The executable removal manifest remains at SHA-256
`a8ff84b514d73bc81d2c063821ad189e6c4f130bbb975f17040320408f1dedd1`.

Exact-diff review first returned `RESHAPE` because Slurm's producer exposes an
SSH identity-file path, the DigitalOcean writer test proved only repeatability,
the producer tests used a nonexistent instance type and impossible
multi-accelerator projection, and the lifecycle tests simulated rather than
observed the real writer boundary. The corrected design classifies Slurm as
`F3 plus F2` and prohibits whole-result snapshotting until credential context
is split. The characterization corpus now uses real `Resources`, catalog-valid
DigitalOcean CPU and GPU types, the real built-in writer path, fixed normalized
render and config-hash oracles, and an observed
`Resources.make_deploy_variables()` boundary. A second challenge found that
the full writer oracle still used a synthetic instance type; after replacing
it with catalog-backed `g-2vcpu-8gb` in valid `nyc1`, re-review returned
`PURSUE` with no remaining blocker.

### Review 25

Verdict: `PURSUE` for the exact read-only
`ProviderRegistryAuditSnapshotV1` contract at SHA-256
`72ea561d0f8a64d5f6292229ff16a8f6699380c99184b4353237c73a4e7bb2c8`.

The first adversarial review returned `RESHAPE` because an arbitrary replaced
built-in getter could execute provider code, top-level identity signatures did
not fence in-place member replacement, provisioner-only registrations were
accepted without a declaration, the barrier overstated same-process security,
and an opt-in memory-only audit could not measure a compatibility release. The
corrected contract never invokes inventory getters, signs and revalidates
every observed static member, treats undeclared provisioner-only entries as
errors, defines the receipt only as a stale-state correctness token, preserves
the legacy schema flag semantics, and makes existing Datadog counts plus an
exact-image token-free release report prerequisites for the release gate.

Independent implementation review then required bounded evidence for arbitrary
raw keys, exact two-phase publication, closed capture-error reasons, and an
allowlisted static descriptor grammar. Independent contract review required
snapshot-only live-reference exclusion, executable sort keys, and exact
non-callable template and incomplete built-in lifecycle issues. After those
corrections, implementation review returned `PASS`, contract review returned
`PASS`, and two exact-diff adversarial re-reviews returned `PURSUE`. This
verdict approves design and the read-only audit implementation only. It grants
no descriptor or lifecycle dispatch authority.

A subsequent deletion audit found that the entry union could omit a deleted
Cloud-only baseline such as IBM and that a deleted or retargeted built-in Cloud
alias had no correctly faceted issue. The contract now retains built-in Cloud
expectation keys, treats an absent expected Cloud as
`CLOUD_BUILTIN_IDENTITY_MISMATCH`, and adds
`CLOUD_BUILTIN_ALIAS_MISMATCH`. The re-review then required deterministic
alias-mismatch attribution, which is now exact.
A runtime-safety review also proved that Python function identity alone does
not prevent in-place `__code__` replacement and that custom
`__getattribute__` can diverge from static members. The contract therefore
seals each getter's executable shape, statically reads its direct-global
binding instead of calling it, and treats custom attribute resolution as
unsafe. The same review also required PID-sensitive token re-keying after fork,
strong-reference anchors against address reuse, and source-specific Cloud
versus provisioner alias validation. These corrections require a final
exact-diff adversarial re-review before implementation is committed. The
capture now constructs its single frozen candidate under the registration lock
and revalidates its complete signature under that lock before publication,
avoiding a second mirror projection schema. The final clarification makes any
non-callable or otherwise unsafe audited Cloud implementation member an exact
facet error rather than identity-only evidence.

The final implementation-safety pass then proved that generic static lookup,
hashed lookup into live namespaces, equality-based type membership, and
inherited post-fork locks could still execute plugin callbacks or deadlock a
child. It also found container-resolution and dynamic-switch projection gaps,
plus an in-place Cloud-base change not covered by identity-only signatures.
The corrected contract uses only built-in namespace and MRO descriptors,
exact-string item scans, identity comparisons, child lock reinitialization,
signed Cloud MROs, and frozen-projection equality as the final completeness
backstop. These corrections require one final exact-diff adversarial re-review
before the runtime commit.

### Review 26

Verdict: `PURSUE` for the implemented read-only provider-registry audit and
exact-image rollout. The final tested source head was
`1ac0bbe433f75bd9f3ef9737e2b695393fd85419`, with contract SHA-256
`72ea561d0f8a64d5f6292229ff16a8f6699380c99184b4353237c73a4e7bb2c8`.
All 31 visible checks passed on that exact head, no review or unresolved thread
remained, and PR 1141 merged normally without administrator bypass as
`8c2b5c01d34893e046852220cdda100bcae62427`. Its exact parents are
`b1c5f73339ee458e030cec642a8ab45cb43943c5` and
`1ac0bbe433f75bd9f3ef9737e2b695393fd85419`.

The exact merge was built as linux/amd64 and pushed by digest
`sha256:8a96571aaa0bfd4f0cabea4236f7d948cfb0b923ae6c29994422b83409ac1ee5`.
Four changed runtime file hashes matched the detached merge checkout, and a
read-only-root container smoke captured 25 entries, three aliases, zero issues,
and a conformant main-process snapshot. Helm revision 52 deployed that digest
to the `skypilot-ha` API, controller, executor, and migration hook while
retaining the PostgreSQL request backend, database secret, existing release
values, and disabled physical-capacity default. The migration completed once
with exit zero.

Three stabilization samples through `2026-08-01T14:41:20Z` found two desired,
updated, ready, and available replicas for each runtime role. All six runtime
pods used the exact digest with zero restarts. Direct readiness passed for both
API pods, both controllers, both executors, and the API service. The final
20-minute application-log scan found no error, critical, traceback, exception,
fatal, or panic match. Initial scheduling and startup-probe warnings ended by
`2026-08-01T14:26:53Z`; Karpenter had consolidated one newly added
underutilized node, the disruption budget preserved availability, and the
replacement became ready. Existing Datadog agents were ready on every node
hosting a runtime pod. No workload pod or service existed in
`skypilot-ha-workloads`, so this rollout provides no data-plane traffic claim.

This closes implementation and release qualification for the read-only audit.
It does not close its removal rows, authorize descriptor dispatch, or qualify
any provider mutation path.

### Review 27

Verdict: `PURSUE` for the exact M4 shared cluster next-action planner and
DigitalOcean inventory pilot contract at SHA-256
`bf5a549bc51f36ae1e7f41a79b137b033fdec232b0064de269fb69640ba39333`
and removal manifest at SHA-256
`52c3ca898c9ac056769c48b2147ae6b46a2992997140524a2b3a474bb9b4eb87`.

The first adversarial pass returned `RESHAPE` because a delegating plugin could
activate a public built-in shadow without route context, non-actuatable
inventories still exposed a `SUBMIT` disposition, capture failure could coexist
with partial inventory, plan payloads and deterministic head selection were
underspecified, random create identity was not a blocker, provider-region
shape was implicit, warnings were unbounded, and a truly repeated cursor could
prevent any diagnostic result. The corrected contract uses one facade-pinned,
single-use, PID-bound private route capability; keeps public delegates
capture-disabled; makes capture a disjoint success/failure union; fixes
execution authority to `NONE` and disposition to `WOULD_SUBMIT`; closes every
action, reason, and blocker payload; reads only exact
`row['region']['slug']`; rate-limits warnings over a finite closed key set; and
classifies repeated-cursor evidence as finite and offline until legacy
pagination changes.

A second implementation-sequencing pass returned `RESHAPE` because the private
traversal did not distinguish legacy-only from capture mode, the removal-row
dependency direction was reversed, and new rows used a legacy provider alias.
The corrected design gives every public and non-query path zero capture work,
makes `PLA-M4-102` depend on callsite removal in `PLA-M4-103`, uses canonical
provider `do`, and requires the exact runtime-SHA bookkeeping commit in the
same pull request before image build or deployment. A final string-safety pass
required exact strict UTF-8 bounds for cluster and requested-region input and a
closed cluster-scope capture failure; those are now part of constructor and
adapter validation.

Independent contract and implementation re-reviews then returned `PURSUE` with
no concrete blocker. The current-phase removal checker, checker tests, and
`git diff --check` pass. This verdict authorizes implementation of the
read-only same-response query shadow and offline pure planner only. It grants
no node-actuation, retry, cleanup, provider dispatch, or `ProvisionRecord`
authority.

### Review 28

Verdict: `PURSUE` for the M4 typed resolved provider operation foundation at
SHA-256
`bbbdd42ec8c804ea3b832eee0040fe2efb97f8c73834662dd8bca9b4bcc92cb2`.
The locator-only removal manifest is at SHA-256
`712420900df178e7f166b21a43d23304e016a4e80a8453a04c480ae2ac1a6ce5`.

Implementation of the Review 27 query-capability planner shadow was rejected
before any runtime or test commit. Independent runtime review reproduced a
race in which the facade verified one built-in query and the legacy adapter
resolved and invoked a replacement. It also reproduced a second race in which
DigitalOcean admission verified one capture helper and later dynamic lookup
invoked a replacement whose exception displaced the legacy result. The same
review found that closure state was outside the claimed executable seal.
Independent simplicity review found an import-and-reload regression from
issuing a singleton diagnostic authority twice, generic-facade work on every
provider call for a DigitalOcean-only feature, duplicate seals without an
authoritative security boundary, and a live hook that compared only query
projection while the proposed shared planner remained test-only. The rejected
machinery was deleted from the worktree and never committed.

The replacement contract first returned `RESHAPE` because a built-in
`LegacyInstanceLifecycleAdapter` still performed a second attribute lookup,
the diagnostic protocol did not runtime-validate either callable, and public
registration could not construct the simultaneous ownership state required to
prove precedence. The corrected contract resolves one raw built-in callable,
pins it for that invocation while preserving legacy void-return behavior,
treats invalid static diagnostic metadata as absent, rejects strict diagnostic
registration before mutation, and requires a direct all-sources resolution
fixture. Re-review of the exact section returned `PURSUE` with no concrete
blocker.

Renaming the resolver method made the two source locators for the still-open
`PLA-M1-003` legacy method-level fallback stale. The manifest update changes
only those symbols and their exact return pattern. It does not change the
obligation, status, dependencies, gates, or removal authority.

This verdict authorizes only the typed resolution foundation and its optional
but unused built-in query diagnostic field. No provider may define that field
in this slice. It grants no query shadow, inventory, planner, provider
actuation, retry, cleanup, or `ProvisionRecord` authority.

### Review 29

Verdict: `PURSUE` for the implemented M4 typed resolved provider operation
foundation at contract SHA-256
`a82453de8a074a076f3dc96e61cb547b3263e366e076bab7c3ddc91ccd9ef3a8`.
The locator-only removal manifest remains at SHA-256
`712420900df178e7f166b21a43d23304e016a4e80a8453a04c480ae2ac1a6ce5`.

The first implementation review reproduced that callable objects with async
`__call__` and equality-colliding defaults could pass validation. Subsequent
passes found async generators, class and static method descriptors, partial and
cache wrappers, intermediate async `__wrapped__` layers, callable classes,
custom call descriptors, and writable `__signature__`, `__text_signature__`,
and `_partialmethod` metadata. The contract was corrected before each
implementation change. Its final closed admission shape is an exact bare
Python function with no `__wrapped__`, synchronous declaration, exact-type
defaults, and a signature derived from a clean function built from code and
real defaults rather than candidate inspection metadata. Every other shape is
absent metadata and leaves the authoritative provider operation selected.

The final independent runtime review returned `LGTM` and the final simplicity
review returned `PASS`. The focused provider-facet suite passes 120 tests, the
complete provision suite passes 405 tests, the removal-checker suite passes 25
tests, and the current-phase manifest checker passes. YAPF, isort, mypy,
pylint, dashboard lint, and dashboard formatting pass on the scoped files.

This review still grants no provider diagnostic binding or provider behavior
claim. It authorizes only the generic typed resolver and the unused private
built-in facet seam for exact-image control-plane regression rollout.

### Review 30

Verdict: `PURSUE` for the M4 DigitalOcean authoritative query projection
extraction at contract SHA-256
`2aecc86b674b232ddf80cd508753f8ca7819b8cbd652dbf4ecae774a91d00e47`.
The unchanged locator-only removal manifest remains at SHA-256
`712420900df178e7f166b21a43d23304e016a4e80a8453a04c480ae2ac1a6ce5`.

Two diagnostic proposals were rejected before implementation. The first
mistook a non-atomic, name-collapsed DigitalOcean traversal for an inventory
and used a writable closure rather than a pinned authoritative query. The
second corrected those faults but required a new V2 resolver branch, immutable
context capture chain, cross-thread lease, shadow comparison, telemetry event,
and removal lifecycle to validate a finite four-state translation. Its
`DEBUG` event would not reach the default production log stream, and its sole
V2 consumer would disappear at promotion while the generic contract remained.

The accepted replacement has one effectful provider read followed by one
authoritative provider-local pure translation. It adds no parallel runtime
path, resolver capability, context state, telemetry, feature flag, or removal
debt. Independent runtime and simplicity re-reviews of the exact section both
returned `PURSUE` with no confirmed blocker. Implementation must additionally
prove that `values()` is evaluated once and the input mapping remains
unmodified. This verdict authorizes only the DigitalOcean query projection
extraction and its characterization tests. It grants no provider inventory,
node actuation, retry, cleanup, planning, or reconciliation authority.

The first implementation review then reproduced one compatibility regression:
rebinding the existing `sky.provision.do.instance.status_lib` module binding no
longer affected the extracted translation. The corrected contract passes the
entry point's exact current `status_lib.ClusterStatus` object into the pure
projector on every call and requires a replacement regression test. This
correction passed the focused 12-test suite, full provision and backend-status
lanes, current removal-manifest gate, combined formatter, type, lint, and
import/reload checks. Final implementation re-review returned `LGTM`, and final
simplicity review returned `PASS` with no remaining blocker.

### Review 31

Verdict: `RESHAPE` for the initial M4 DigitalOcean incarnation locator
foundation committed at `c298cc53d02a13ce601808cff19ddffd2fd239c5`, with
initial contract SHA-256
`f638822e151fcf8d1aa90ff8c332f2e7998bfa924cd785b1955981f81ce274e3`.

The marker was judged worth implementing, but the first contract left five
compatibility and authority gaps. A default base-dataclass field could prevent
subclasses from adding required fields; the new backend keyword could break an
exact-old-signature `bulk_provision` replacement and then enter destructive
cleanup; arbitrary historical cluster hashes were not proved to fit the raw
DigitalOcean tag grammar; validation inside `create_instance()` occurred after
earlier resume or rename mutations and its error could invoke cleanup; and a
per-resource marker match did not address mixed marked and unmarked siblings.

The corrected contract makes the field keyword-only, omits it from repr and
the existing redacted provision-log shape, and pins equality, field order,
required subclasses, and a real missing-state pickle round trip. The class
default is sufficient for missing old instance state, so no custom pickle hook
is introduced. Exact built-in callable identity gates the new keyword without
a retry. A domain-separated V1 digest encodes every Python string into one
fixed tag-safe marker. One pure validator runs before any DigitalOcean
lifecycle mutation and its closed admission error bypasses destructive
cleanup. Finally, any missing or mismatched sibling, incomplete inventory, or
stale attempt blocks future typed mutation for the whole generation, and
rollback explicitly preserves that mixed-resource consequence.

The reshaped contract has SHA-256
`16a26ee885670f3e279f51a82461a90fb9e654741a275c88d886b1906980f98f`.
It requires a fresh exact-contract adversarial and simplicity review before
implementation.

### Review 32

Verdict: `PURSUE` for the M4 DigitalOcean incarnation locator foundation at
commit `45af22dd26cfc1ee10c5581c688ae71940b8c9c2`, with exact reviewed
subsection SHA-256
`2cd8ff1b1edd9f38634ed6da719c2e1b881411b5b180be6068f2b3f68c9d87ad`.
Independent simplicity review returned `PASS`.

Re-review of the Review 31 correction found that a new admission exception
could bypass cleanup inside `bulk_provision()` yet still reach the backend's
outer broad cleanup handler. Adding a privileged exception through both
layers was rejected. The accepted contract instead makes marker preparation
total for in-contract inputs: exact strings receive the fixed V1 digest,
marked requests replace every occupant of the newly reserved namespace, and
missing or non-exact-string values preserve exact legacy tag behavior. The
helper is consumed only by `create_instance()`, so discovery, resume, rename,
and zero-create paths do no marker work. No new exception class or cleanup
bypass exists.

The exact adversarial re-review returned `PURSUE` with no concrete blocker.
This verdict authorizes only propagation of the existing cluster generation
and creation-time DigitalOcean droplet and volume stamping. It grants no
system-authorship proof from a tag, provider-effect fence, query-inventory
authority, cleanup isolation, shared-reconciler mutation, retry, or
`ProvisionRecord` change.

### Review 33

Verdict: `RESHAPE` for the initial M4 DigitalOcean complete inventory capture
proposal committed at `846f23dfb902479564cd80781fca46917a766044`, with exact
reviewed subsection SHA-256
`b724023e27f037198f2c91ef5d747aa391e944a451dd136dec5657631f9c8f51`.
Independent simplicity review returned `FAIL`.

The proposal correctly identified DigitalOcean's disjoint non-GPU and GPU
list families, but it was not authority-neutral. It routed newly discovered
GPU rows into the same name-keyed dictionary used by stop, termination,
rename, and cleanup. A duplicate name could redirect an irreversible effect,
and neither the proposed raw carrier nor its `authority=NONE` label restricted
that legacy consumer. The design also changed the `status_filters=None`
short-circuit, retained raw rows without a production consumer, used unstable
account totals as hard availability gates without obtaining a snapshot, mixed
pagination exception contracts, and increased account-wide polling before
token-scoped pacing existed.

No runtime implementation was started. The canonical subsection was replaced
in place by the explicit deferred boundary at SHA-256
`0e9c738e6e899d1435e8b10d96906164d263428202c90c834c51b151d87ad692`.
It keeps the discovery gap visible and requires immutable canonical identity,
credential scope, collision and completeness closure, paced read-only canary
evidence, a persisted effect attempt, and exact-ID pre-effect revalidation
before any dual-family row can reach mutation. This review grants no
dependency, provider-read, lifecycle, database, deployment, or removal-ledger
authority.
