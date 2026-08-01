# SkyServe Resource-Action Provider Facet

Status: bounded M0 accepted after independent adversarial review; parent M1
kernel complete; M2 cluster identity, immutable provider contracts, and typed
shadow-store foundations implemented and locally verified; runtime provider
propagation, observation, and shadow instrumentation pending

Last updated: 2026-08-01

Canonical owner: this file governs only the provider-mutation and observation
seam for `docs/designs/durable-serve-replica-actions.md`. It does not supersede
the broader provider ownership and placement program in
`docs/designs/provider-lifecycle-actuation.md`; it consumes that program's
shared read-only owners where their contracts are already qualified. For
central PostgreSQL SkyServe launch/down, this narrower contract is authoritative
and reuses the existing API-request execution lease instead of adding the
broader program's proposed generic action lease.

## Decision and boundary

This design defines the provider seam needed by the first durable SkyServe
launch/down actions. It is not a new provisioner, provider queue, workflow
engine, credential plane, or cross-provider reconciliation service.

The bounded responsibility split is:

```text
Serve adapter
  chooses desired launch/down intent, placement, retry, and state reduction
        |
resource-action kernel
  persists identity/attempt/due/result and materializes one PR #1070 request
        |
existing API request handler
  owns the sole execution claim/lease and invokes existing provider code
        |
provider lifecycle facet
  validates a frozen plan, invokes one existing high-level actuation per
  attempt, and observes its target
```

The provider facet never scans due actions, creates queue rows, leases work,
chooses placement, computes Serve backoff, or writes Serve state. The action
kernel never stores provider credentials or reimplements cloud SDKs.

The first program supports broad shadow evaluation but narrow authoritative
eligibility. A provider path remains legacy/shadow until it can supply the
stable locator and exact observation/absence evidence below.

## Goals

- Freeze the provider target selected by existing SkyPilot planning before an
  action is admitted.
- Journal the mutation boundary before external I/O.
- Persist a provider operation/request ID immediately when one exists, without
  making it mandatory for providers that expose none.
- Normalize provider results into a closed outcome used by the Serve reducer.
- Resolve lost acknowledgements by exact readback of the frozen target.
- Make launch adoption and teardown absence explicit and testable.
- Ensure shadow executes exactly one legacy mutation and compares the proposed
  durable interpretation with its real result.

## Non-goals

- Replacing `sky.provision`, `sky.launch`, `sky.down`, or current cloud
  adaptors.
- Re-running the optimizer inside a retry or observation.
- Adding a generic provider operation queue or long-lived provider lease.
- Persisting arbitrary SDK responses, credentials, kubeconfigs, or tracebacks.
- Claiming all existing clouds are authoritative in the first rollout.
- Central PostgreSQL principal convergence, schema downgrade, maintenance
  fleets, rollout pins, or full-uninstall protocols.

## Responsibility interfaces

The v1 lifecycle facet is deliberately small:

```text
ProviderLifecycleFacetV1
  normalize_plan(existing_launch_or_down_input) -> ProviderLifecyclePlanV1
  submit(plan, request_execution_fence) -> ProviderSubmissionV1
  observe(plan, optional_operation_id, optional_resolved_target)
      -> ProviderLifecycleObservationV1
```

`normalize_plan()` is pure and bounded. `submit()` invokes the existing
high-level launch/down path at most once for one request attempt. That existing
path may perform multiple internal SDK calls; v1 does not pretend those are a
generic effect graph. A helper is authoritative only if its partial effects are
fully observable/reconcilable as one logical target. `observe()` is read-only.
Provider-specific code cannot create another SkyPilot API request or own retry
scheduling.

The action request uses the normal executor with `retryable=false` and no queue
precondition. Because the current generic executor otherwise requeues
`ExecutionRetryableError` and `ExecutionPausedError` regardless of that flag,
the action handler/facet catches both families and normalizes them into a closed
typed retry/uncertain outcome before they escape. The same request ID never
re-enters provider submission; only the Serve reducer may admit attempt `n+1`.

The existing API request worker remains the only long-lived executor. Its
claim token/execution generation fences handler writes exactly as in PR #1070.
Action IDs do not authorize provider calls by themselves.

## Immutable plan

```text
ProviderLifecyclePlanV1 = {
  version: 1,
  profile: "pod_cluster_v1",
  action_kind: "launch" | "down",
  resource_identity: {
    service_hash: Text,
    service_incarnation: UUID,
    replica_id: NonnegativeInteger,
    replica_incarnation: UUID,
    desired_generation: PositiveInteger
  },
  placement_decision_sha256: Sha256,
  resources_snapshot_sha256: Sha256,
  workspace_identity_sha256: Sha256,
  requested_target: ProviderLocatorV1,
  prior_resolved_target: null | ResolvedProviderTargetV1,
  request_payload_sha256: Sha256,
  redaction_profile: "provider_lifecycle_redaction_v1"
}
```

The plan embeds or references the complete bounded normalized preimage for each
hash in the action descriptor; a hash-only provider-private interpretation is
not authoritative. It contains no secret bytes.

For action-aware SkyServe rows, `service_hash` is the canonical UUID text
already stored in `services.hash`, and `service_incarnation` is that same value
parsed as a UUID. A null/noncanonical legacy hash is ineligible. No second
service incarnation is minted or backfilled.

Placement is chosen once by existing Serve/SkyPilot policy. A retry uses the
same plan. A policy decision to choose a different cloud/zone/shape is a new
Serve desired generation and action, not a mutation of an attempt.

## Stable provider locator

```text
ProviderLocatorV1 = {
  version: 1,
  profile: "pod_cluster_v1",
  cloud: Text,
  region: null | Text,
  zone: null | Text,
  sky_cluster_name: Text,
  sky_cluster_record_uuid: UUID,
  kubernetes: null | {
    cluster_fingerprint_sha256: Sha256,
    namespace: Text,
    workload_kind: Text,
    workload_name: Text,
    cluster_record_uuid_label: Text,
    replica_incarnation_label: Text
  }
}
```

For the initial authoritative `pod_cluster_v1` cohort, `cloud` is
`kubernetes`, the `kubernetes` block is nonnull, and `workload_kind` is exactly
`Pod`. `cluster_record_uuid_label` must equal
`str(sky_cluster_record_uuid)` and `replica_incarnation_label` must equal
`str(replica_incarnation)`, both in lowercase canonical UUID form. A null block,
another workload kind, missing label, or noncanonical/mismatching value
normalizes to unsupported before submission and to conflict if encountered
during observation; it is never eligible.

The SkyPilot cluster-record UUID and replica-incarnation label are generated
and persisted before launch submission. Display name alone is never enough for
authoritative adoption or deletion. The requested locator and plan hash never
change. Provider-native IDs and Kubernetes UIDs discovered later are persisted
as write-once resolved-target evidence on the attempt/observation; they do not
rewrite the plan.

The current cluster-state helper creates `clusters.cluster_hash` only inside
`add_or_update_cluster()`, after request construction, and its update path may
replace the hash for a same-name row. `existing_cluster_hash` is an update-only
compatibility parameter and must not be repurposed. The action-aware launch
path instead carries an internal `cluster_record_uuid` from the persisted
replica/plan through the launch request and backend. Its fail-closed cluster
row primitive has exactly three outcomes before provider I/O: insert a missing
name with the requested UUID, adopt/update a row with the same UUID, or reject
a same-name different-UUID row. It validates canonical UUID text and never
silently overwrites identity.

Global-user-state revision 028 adds nullable
`clusters.cluster_record_uuid` using SQLAlchemy's portable UUID type (native
UUID on PostgreSQL) and enforces uniqueness for every nonnull value. It is an
independent identity commitment, not an alias for `cluster_hash`, and migration
028 does not backfill historical rows. The inert nullable column and unique
index are present on the still-supported local SQLite catalog so current
metadata remains readable; initialization/adoption remains PostgreSQL-only.
Only the internal action-aware cluster-row primitive may initialize it: insert
a missing name with the requested UUID, exactly adopt/update a row with the
same UUID, and reject a same-name null or different-UUID row. Ordinary cluster
updates omit this column and therefore cannot initialize, clear, or replace a
nonnull commitment. An old name-only row stays ineligible until removed;
launch never mints identity onto an already-live resource.

Migration 028 downgrade may remove the unique index and column only when no
row has a nonnull `cluster_record_uuid`; otherwise it raises. Application
rollback keeps revision 028 and never invokes schema down.

For Kubernetes, the cluster-record UUID and Serve replica-incarnation UUID use
the reserved immutable labels `skypilot.co/cluster-record-uuid` and
`skypilot.co/serve-replica-incarnation` on the workload and pod template. Both
UUID values fit Kubernetes label syntax. Create/adopt/read/delete paths select
and validate the display-name label plus both identity labels; every existing
pod must agree before adoption. Every mutable resource in the selected topology—including
Services, Deployments, and persistent claims when present—must receive and
validate the identity; validating Pods alone is insufficient. A same-name
workload with missing or different labels is a conflict and is never deleted.
Until that coverage is proven, eligibility is restricted to a reviewed
direct-Pod topology. The workload UID is resolved-resource evidence, not a
provider operation ID, so a null operation ID remains valid.

The bounded write-once evidence is:

```text
ResolvedProviderTargetV1 = {
  version: 1,
  requested_target_sha256: Sha256,
  provider_resource_id: null | Text,
  workload_uid: null | Text,
  provider_operation_id: null | Text,
  resolved_at: UtcTimestamp
}
```

The profile defines which resolved fields are required for authoritative
present/absence proof. A caller loads this object from prior attempt evidence
and passes it to later `observe()` calls. A launch plan starts with
`prior_resolved_target=null`; discovery is written to the attempt, leaving that
plan immutable. Down admission copies the matching launch's resolved target
into the new down plan's immutable `prior_resolved_target`. A conflicting
second value is corruption and cannot replace the first.

`pod_cluster_v1` is authoritative only when the existing launch/down path can
propagate and later query these identities without guessing. A name-only
pre-existing replica remains shadow until replaced by a natively identified
generation.

## Submission journal

The action-attempt row has a closed mutation boundary:

```text
NOT_STARTED -> INTENT_COMMITTED -> SUBMITTED_OR_AMBIGUOUS -> SETTLED
```

Before provider bytes may be sent, the request handler locks action, attempt,
and correlated request in that order. After any wait it uses a fresh database
clock to revalidate the exact correlation, `RUNNING` state, execution
generation, claim token, worker, current owner fence, unexpired lease, exact
plan hash, and attempt identity, then commits `INTENT_COMMITTED`. A request is
never locked before its action. The handler then performs one existing
launch/down call.

`ProviderSubmissionV1` is:

```text
{
  disposition: "not_submitted" | "accepted" | "ambiguous",
  provider_operation_id: null | Text,
  normalized_response_sha256: null | Sha256,
  normalized_error: null | ProviderErrorV1
}
```

An observed provider correlation token is normalized into
`provider_operation_id` and persisted as soon as the handler can
fenced-write it. This may be a provider operation ID or, where that is the
provider's only stable correlation primitive, its request ID. A crash before
that write does not authorize a blind replay; the attempt is ambiguous and
must run `observe()` first.

Once `INTENT_COMMITTED` exists, recovery by a different request execution
treats submission as ambiguous even if no operation ID was recorded. A
point-in-time absence read does not prove the earlier call cannot arrive later.
Automatic resubmission is legal only when a stable provider-side idempotency
key makes overlapping calls converge, or authoritative operation evidence
proves no earlier submission remains capable of taking effect. Otherwise Serve
continues observation or quarantines the conflict.

The facet must not infer `not_submitted` from a client exception unless the
underlying SDK guarantees no request bytes crossed its mutation boundary.
Otherwise the result is `ambiguous`.

## Typed error and outcome

```text
ProviderErrorV1 = {
  category: "transient" | "capacity" | "quota" | "rate_limited" |
            "invalid_request" | "permission" | "conflict" |
            "unknown",
  provider_code: null | Text,
  retry_after_seconds: null | NonnegativeInteger,
  normalized_message: null | Text
}
```

Raw exceptions and provider payloads are not state-machine inputs. The facet
maps only reviewed codes/statuses into the closed category; unknown values stay
`unknown`. The Serve reducer decides retry, block, or terminal failure.

## Observation contract

```text
ProviderLifecycleObservationV1 = {
  version: 1,
  target_sha256: Sha256,
  state: "present" | "absent" | "conflict" | "uncertain",
  certainty: "authoritative" | "eventually_consistent" | "unknown",
  observed_provider_operation_id: null | Text,
  observed_provider_resource_id: null | Text,
  observed_cluster_record_uuid: null | UUID,
  observed_workload_uid: null | Text,
  observed_replica_incarnation_label: null | Text,
  resolved_target: null | ResolvedProviderTargetV1,
  ready: null | Boolean,
  evidence_sha256: Sha256,
  observed_at: UtcTimestamp
}
```

The persisted evidence is a bounded normalized preimage plus recomputed hash,
not an arbitrary SDK object. It records the authority/source/query used and
enough identity fields to distinguish the target from a same-name replacement.

`present` is authoritative only when the frozen requested cluster
UUID/incarnation and every available write-once native UID/resource ID match.
`absent` is authoritative only
when the reviewed profile's strongest read proves the exact target absent.
A same-name different incarnation is `conflict` unless the evidence separately
proves the frozen target UID absent. Partial listings, transport errors, stale
caches, and missing identity are `uncertain`.

Eventually consistent absence cannot terminalize down by itself. The facet may
return it as diagnostic evidence and the Serve reducer schedules another
observation.

## Launch behavior

For a fresh authoritative launch attempt:

1. validate the immutable plan and current request claim fence;
2. commit `INTENT_COMMITTED` before provider I/O;
3. invoke the existing launch path once with the frozen target identity;
4. persist an operation/resource ID when returned;
5. observe the exact target; and
6. return a typed outcome to the action reducer.

On recovery or an ambiguous submission, observation always precedes another
mutation. Exact `present` adopts the resource only when the profile also proves
the launch-readiness contract. Exact `absent` permits a retry only when the
Serve classifier marks the error retryable and the profile proves stable
idempotency or that no prior operation can still take effect. Recoverable
`uncertain` schedules another observation; identity `conflict` quarantines the
action.

The facet never creates a second resource merely because the original API
request lease expired.

## Down behavior

Down always targets the frozen locator. It may invoke the existing teardown
path once per materialized attempt, but success requires a subsequent
authoritative absence observation. Delete acknowledgement, a missing local
handle, or a name-only inventory miss is insufficient.

The expected cluster-record UUID is carried through the internal down request,
`core.down()`, and backend teardown. The backend reloads the cluster row and
checks the UUID after acquiring its resource lock, immediately before any
provider delete or state-row removal. An earlier name lookup is not a fence:
if a same-name successor appears while teardown waits for the lock, the
post-lock mismatch returns conflict and no mutation occurs. Handle refresh and
cluster-row deletion use the same expected UUID.

If a different incarnation now uses the display name, the facet must avoid
deleting it. It terminalizes the old target only when native UID/resource-ID
evidence proves that old target absent. Otherwise it returns conflict or
recoverable uncertainty; Serve quarantines the former and schedules another
observation for the latter.

There is no provider-facet cleanup deadline. Serve may schedule bounded
database-clock retries indefinitely.

## Shadow protocol

Shadow has one mutation owner: the existing legacy launch/down thread. The
durable path receives the same frozen decision inputs but does not submit.

For every eligible decision in a service's shadow window, one logical parent
is committed with replica/capacity or teardown intent before the legacy enqueue.
It contains the would-be identity/plan and final legacy/proposed projections.
Retries never create another parent.

Each legacy SDK/direct mutation boundary creates one request-sequenced child
immediately before the call. Each `logical_attempt` has exactly one
`PRIMARY_LAUNCH` or `PRIMARY_DOWN` child and zero or more
`LAUNCH_CLEANUP_DOWN` children. The latter records an internal cleanup
`sdk.down()` performed between legacy launch retries under the launch parent;
it is not a separately admitted logical down action. A child stores
`planned_execution_kind` (`api_request` or `legacy_direct_down`), the real
request ID when returned, actual/proposed outcomes, retry decision, pre/post
observation, provider correlation, and bounded divergence.

Only a proven pre-call path may be abandoned. Once SDK request creation is
entered, failure to receive or bind its ID is
`REQUEST_ASSOCIATION_UNKNOWN` and promotion-blocking. Owner handoff adopts the
existing parent and contiguous next request sequence; it never invents an ID
or a second parent.

Closed divergence classes include identity mismatch, placement mismatch,
submission-certainty mismatch, operation-ID mismatch, retry mismatch,
observation mismatch, terminal mismatch, and unsupported provider profile.
Shadow never reports parity from an offline fixture when the live legacy path
produced a different result. Promotion requires every parent and expected
child in the candidate window to be complete and nondivergent; shadow is not a
statistical sample of that service's mutations. A `legacy_direct_down` child is
always promotion-blocking: M2 must route teardown through the existing
`sdk.down()` request path and record its real ID before the promotion window
begins. No synthetic request ID may be created for a direct call.

The replica's launch/down shadow links retain the corresponding parent and all
children while the replica or a cleanup intent exists. An authoritative down
of a shadow-launched replica must revalidate the completed launch child's
`ResolvedProviderTargetV1` and copy it into the frozen down plan; missing or
name-only evidence is not eligible.

## Eligibility and activation

A lifecycle profile is authoritative only when checked-in contract tests prove:

- stable locator creation before mutation;
- one-call submission with a defined mutation boundary;
- operation ID persistence where supported;
- exact present/adoption readback after lost acknowledgement;
- stable provider idempotency or authoritative proof that a prior ambiguous
  launch can no longer take effect before any resubmission;
- exact absence/readback for down;
- same-name replacement discrimination;
- bounded redaction; and
- no provider-specific queue, lease, due scanner, or retry loop.

The first candidate profile is `pod_cluster_v1`; it is not accepted for
authority merely because this name appears in a stored plan. During M2 it stays
`unsupported_provider_profile` and promotion-blocking until checked-in tests
prove the exact cluster-UUID row primitive, end-to-end UUID/incarnation
propagation, Kubernetes label-qualified read/delete, and the invocation
fingerprint below. Other clouds/providers may participate in shadow and
accumulate evidence without being silently treated as eligible. Accepting an
authoritative profile updates this canonical file and its contract fixtures.

## Invocation fingerprint and redaction

`request_payload_sha256` is not a hash of `LaunchBody.model_dump()` or another
generic request object. Those objects include ambient configuration, uploaded
mount state, environment values, and secrets and are constructed after client
side effects. A pure closed `ProviderLifecycleInvocationV1` builder produces
the exact bounded, redacted provider-affecting input that the action-aware
launch/down call consumes. That normalized object is embedded in the immutable
action spec and its canonical bytes produce `request_payload_sha256`.

The closed invocation union is:

```text
ProviderLifecycleInvocationV1 = {
  version: 1,
  profile: "pod_cluster_v1",
  redaction_profile: "provider_lifecycle_redaction_v1",
  action_kind: "launch" | "down",
  resource_identity: ProviderLifecyclePlanV1.resource_identity,
  requested_target: ProviderLocatorV1,
  launch: null | ProviderLaunchInvocationV1,
  down: null | ProviderDownInvocationV1
}

ProviderLaunchInvocationV1 = {
  source: {
    store: "serve_version_specs",
    service_name: Text,
    service_incarnation: UUID,
    service_version: PositiveInteger,
    yaml_content_sha256: Sha256,
    workspace: Text
  },
  resources: ProviderPodResourceSnapshotV1,
  replica_env: {"SKYPILOT_SERVE_REPLICA_ID": DecimalIntegerText},
  security_group_scope: Text,
  admin_policy_input_sha256: Sha256,
  admin_policy_output_sha256: Sha256,
  retry_until_up: Boolean,
  exact_resources_override: Boolean,
  backend: "cloud_vm_ray",
  optimize_target: "cost",
  dryrun: false,
  no_setup: false,
  clone_disk_from: null,
  fast: false,
  file_mounts_blob_id: null | Text,
  tls_material_ref: null | Text
}

ProviderDownInvocationV1 = {
  cluster_name: Text,
  expected_cluster_record_uuid: UUID,
  workspace: Text,
  purge: false,
  graceful: false,
  graceful_timeout: null
}

ProviderPodResourceSnapshotV1 = {
  version: 1,
  cloud: "kubernetes",
  cluster_fingerprint_sha256: Sha256,
  namespace: Text,
  instance_type: null | Text,
  accelerator: null | {name: Text, count: PositiveInteger},
  cpus: null | Text,
  memory: null | Text,
  image_id: null | Text,
  disk_size_gb: PositiveInteger,
  disk_tier: null | Text,
  ports: [Text],
  labels: [{key: Text, value: Text}],
  use_spot: false
}
```

The Serve adapter's immutable action spec is the closed object:

```text
ServeReplicaActionSpecV1 = {
  version: 1,
  provider_plan: ProviderLifecyclePlanV1,
  invocation: ProviderLifecycleInvocationV1
}
```

Unknown keys and floats are rejected, and the canonical object is bounded to
65,536 UTF-8 bytes. `provider_plan.validate_invocation(invocation)` must pass;
the plan and invocation derive the enclosing action ID, and
`provider_plan.request_payload_sha256` equals `invocation.sha256`. The shadow
parent's separately indexed `provider_plan` and hash are an exact byte-equal
copy of the wrapper member, and a primary child's invocation is an exact
byte-equal copy of the wrapper invocation. A `LAUNCH_CLEANUP_DOWN` child is the
sole exception: it uses the closed down invocation derived from the same
launch identity and frozen target. Typed reads reconstruct this exact wrapper;
arbitrary mappings are not accepted. Golden canonical-byte/hash fixtures plus
unknown-key, float, identity-mismatch, and mutated-plan/invocation rejection
tests freeze this wrapper contract.

Exactly one of `launch` and `down` is nonnull and it must match
`action_kind`. Objects reject unknown keys. Text is NFC, lists are sorted and
duplicate-free by their canonical element/key, UUIDs are lowercase hyphenated,
integers are JSON integers, and floats are forbidden. Each text is 1..1,024
UTF-8 bytes except `cluster_name`, namespace, label keys/values, and ports,
which are 1..253 bytes; lists contain at most 256 items; the whole canonical
object is at most 65,536 bytes. SHA fields are 64 lowercase hexadecimal
characters.

The `source` tuple is a content-addressed reference to an immutable retained
`version_specs` row; the builder verifies its YAML bytes before use. The first
eligible cohort requires `file_mounts_blob_id=null`, `tls_material_ref=null`,
no task secrets/storage/local mounts, and an admin-policy output whose only
changes from the referenced source are exactly the represented resource,
replica-env, and security-group transforms. Any other resource field,
nondefault launch flag, unrepresented policy mutation, secret/material source,
or compound topology normalizes to unsupported. This intentionally narrow gate
lets the source reference plus the closed transformations reconstruct the same
prepared request without copying YAML, commands, environment values, or secret
bytes into action JSON.

The builder includes the action/target identity, normalized topology and
resource selection, workspace/config identities, and content identities for
provider-affecting artifacts, but no credential, kubeconfig, secret value,
private key, arbitrary environment value, uploaded body, or traceback. Secret
names or opaque references may be represented only when they cannot disclose a
secret value. A hash-only field is insufficient unless its bounded normalized
preimage or a reviewed nonsecret content-addressed reference is also stored.
The profile stays ineligible until the same builder output demonstrably drives
the real invocation; a parallel observer-only serialization is not authority.

One preparation function returns both the in-memory request consumed by the
legacy submission and its `ProviderLifecycleInvocationV1` projection after
placement/resource overrides, Serve replica/TLS/security-group mutation,
admin-policy application, mount/blob resolution, and final launch/down options
have been applied exactly once. The adapter recomputes and compares the
projection immediately before submission; retries reuse the frozen prepared
input. A changed provider-effective input is a new desired generation. TLS and
other secret values may exist only in the ephemeral in-memory request and are
represented in the projection solely by reviewed nonsecret identities.

Golden fixtures store the exact canonical UTF-8 bytes and lowercase SHA-256 for
one launch and one down invocation. Tests compare literal bytes and hashes, not
round-tripped JSON equality, and mutate every key/type/ordering/redaction field.
The same prepared in-memory object that yielded the accepted projection is
consumed by submission; no policy, placement, resource, mount, identity, or
option mutation is permitted afterward.

## Implementation phases

### P1: pure normalization

- Extract bounded plan/locator/error/observation normalization around existing
  launch/down helpers.
- Add the closed redacted invocation builder; do not serialize raw generic
  request bodies.
- Add no provider mutation call sites.
- Build golden fixtures from current provider results with secrets removed.

### P2: live shadow observation

- Capture actual legacy request/result.
- Add global-user-state revision 028 and propagate the precommitted
  cluster-record UUID through the prepared launch request, backend, cluster
  row, and provider labels without repurposing `cluster_hash`.
- Characterize the current direct `core.down()` compatibility path, then route
  legacy teardown through `sdk.down()` and require real request IDs for the
  promotion window.
- Perform reviewed read-only pre/post observations.
- Store parity/divergence with no second mutation.

### P3: request-handler integration

- Add journal-before-I/O mutation boundary to action-correlated requests.
- Persist operation IDs and typed outcomes under existing request claim fences.
- Invoke the in-server execution/core seam directly; the handler must not call
  `sdk.launch()` or `sdk.down()` and create a nested API request.
- Keep authoritative dispatch limited to synthetic/canary actions.

### P4: selected Serve authority

- Enable one eligible service/profile after the parent design's gates.
- Preserve per-service fallback only for services that never promoted.
- Delete duplicate provider retry/observation ownership from the eligible
  Serve path after soak.

## Tests

Contract tests must cover:

- canonical plan/locator bytes and identity mismatch rejection;
- mutation intent committed before the provider mock observes a call;
- crash before intent, after intent, after bytes, after operation ID, and after
  response;
- lost launch acknowledgement followed by exact adoption;
- lost launch acknowledgement with exact absence but no idempotency/final
  operation proof continuing observation without retry;
- lost launch acknowledgement followed by one legal retry only after stable
  idempotency or proof the prior operation cannot take effect;
- ambiguous/name-only evidence blocking a retry;
- down acknowledgement followed by present, uncertain, then exact absent;
- same-name/new-incarnation protection;
- stale request claim/execution generation write rejection;
- retry/pause exceptions terminalize once without same-request requeue or a
  second provider submission;
- both intent/evidence-versus-terminalization race directions, proving that a
  writer which loses the request lock rejects and a writer which wins commits
  before terminalization;
- providers with and without operation IDs;
- redaction and byte/depth/node bounds;
- shadow records every eligible candidate, executes one legacy high-level
  mutation, and compares its actual result; and
- absence of any new provider queue, worker lease, or domain retry scheduler.

The isolated HA smoke test kills API/controller/executor pods at every mutation
boundary and asserts one logical action, one request per attempt, no duplicate
resource, and no false teardown completion.

## Deployment and rollback

Provider changes ship dark, then shadow, then per-service authoritative. The
blocking migration job must converge all three independent additive
heads—global-user-state 028, Serve032, and API005—before any action-aware
process image is activated. Activation is gated on all three verified heads;
there is no cross-lineage Alembic dependency. No provider profile is enabled
globally by schema migration. Application rollback retains all three heads and
uses only a compatible image that preserves nonnull cluster-record UUIDs as
write-once commitments and preserves nonterminal shadow/action state. It does
not run provider compensation or schema down. After first authority, rollback
to a pre-action-aware image is unsupported.

Unknown or drifted provider evidence fails closed to `BLOCKED`. Operators may
inspect and repair it, but cannot replace the frozen locator or fabricate an
absence result through a public API.

## Open gates

- Exact inventory of existing providers that can propagate a stable
  cluster-record UUID/incarnation before launch; multi-node/compound launch is
  ineligible until all effects have one exact observable target contract.
- Checked-in `pod_cluster_v1` observation fixtures against real Kubernetes and
  the selected Boltz canary path.
- Implementation and contract verification of the exact cluster-row UUID
  primitive, Kubernetes label-qualified adoption/deletion, redacted invocation
  builder, and request-handler pre-I/O/operation-ID callbacks without
  duplicating provider code.
- A measured complete-shadow window and minimum volume for launch, retry,
  ambiguity, and down.
