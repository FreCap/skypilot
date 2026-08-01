# SkyServe Resource-Action Provider Facet

Status: bounded M0 accepted after independent adversarial review; parent M1
kernel complete; M2 cluster identity, initial immutable provider contracts, and
typed shadow-store and Serve033 coverage/promotion foundations plus the generic
API006 progress substrate are implemented and locally verified; the
committed-effect/origin, closed handler-return, API006 representability, and
partial-launch quiescence literal contract is frozen in this design while its
Serve-owned cursor validator/reducer and both
prior-launch-basis parser/admission paths remain pending; the
candidate-only normalization boundary and atomic durable coverage handshake are
in progress; the closed Kubernetes transport/scope leaf is implemented and
independently verified; topology/image/config-policy/principal/authorization
foundation leaves are implemented and locally verified; full execution-config
composition remains pending; bounded resource values and the generic
prerequisite/object-plan/renderer foundations, exact prerequisite role/kind map
and role-dispatched prerequisite leaf, and launch-only endpoint
projection/workload leaves are implemented and independently verified, while
the enclosing exact 12-record inventory validator is implemented and
independently verified and named capsule composition and request-body
renderer/normalizer artifacts remain design-frozen and code-pending;
foundational runtime/job/endpoint leaves now include the closed Skylet job-spec,
submit-request, retained-evidence, and policy-boundary-proof leaves and are
independently verified; the policy-free request-identity, scheduling, storage,
metadata, security, object-mutation-effect, and exact launch/down mutation
contract leaves are implemented and independently verified, while contextual
action-bound submit comparison, launch-only server-effective identity
canonicalization/projector and down-current-identity absence, enclosing capsule
and execution-config composition, and live preflight remain pending; shared
leaf-validator, named raw-field, and bounded canonical-JSON exact-type
hardening is implemented and independently verified; runtime provider
propagation, observation, and shadow instrumentation remain pending

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
  owns the sole execution claim/lease and invokes the reviewed provider seam
        |
provider lifecycle facet
  validates a frozen plan, invokes one bounded fixed-topology actuation per
  attempt through extracted existing primitives, and observes its target
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
  normalize_plan(existing_launch_or_down_input,
                 frozen_scope_and_execution_config)
      -> ProviderLaunchNormalizationResultV1 | ProviderDownNormalizationResultV1
  submit(spec: ServeReplicaActionSpecV1,
         durable_progress: null | ProviderLifecycleProgressV1,
         request_execution_fence)
      -> ProviderSubmissionV1
  observe(spec: ServeReplicaActionSpecV1,
          optional_operation_id,
          durable_progress: null | ProviderLifecycleProgressV1,
          request_execution_fence)
      -> ProviderLifecycleObservationV1
```

`normalize_plan()` is pure and bounded over explicitly supplied read-only
preflight values; it never opens a client or reads ambient config. In shadow,
the existing high-level launch/down path remains the only mutation.
`submit()` and `observe()` typed-read the complete immutable action spec; a
normalized plan alone is never execution authority. The action kind selects
the corresponding launch or down invocation and execution configuration from
that spec. Before constructing a cloud or Kubernetes client, the facet verifies
the plan/invocation hashes, frozen executor cohort, current request-execution
fence, and every kind-specific scope and principal field. A caller cannot
supply replacement execution configuration beside the spec.
Authoritative `submit()` invokes the fixed `pod_cluster_v1` object graph and
one idempotent Skylet job submission for one request attempt through pure
renderer/session primitives extracted from that path. Before each next effect,
it consumes the claim-fenced `ProviderLifecycleProgressV1` snapshot from
API006; after each effect it commits the newly observed UID/handle/job evidence.
It may send only the three journaled object creates/deletes and the one
journaled job RPC, not an unbounded or conditional effect graph. A helper is
authoritative only if each partial effect is fully observable/reconcilable as
the one logical target.
`observe()` is read-only.
Provider-specific code cannot create another SkyPilot API request or own retry
scheduling.

The action request uses the normal executor with `retryable=false` and no queue
precondition. Because the current generic executor otherwise requeues
`ExecutionRetryableError` and `ExecutionPausedError` regardless of that flag,
the action handler/facet catches both families and normalizes them into a closed
typed retry/uncertain provider result inside the parent design's
`ServeReplicaActionRequestReturnV1` before they escape. The same request ID never
gets executor-driven retry/requeue. A later claim generation for that request
may recover the handler, but it observes and consumes API006 progress and never
repeats a committed effect; only the Serve reducer may admit attempt `n+1`.

The existing API request worker implementation remains the only long-lived
executor. The canary deploys that same implementation in a dedicated
Kubernetes/RBAC cohort and restricts its existing claim query by private
handler allowlist; it adds no provider worker loop. Its claim token/execution
generation fences handler writes exactly as in PR #1070. Action IDs do not
authorize provider calls by themselves.

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
  prior_launch_basis: null | PriorLaunchBasisV1,
  prior_cleanup_target: null | ProviderKubernetesCleanupTargetV1,
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

For `pod_cluster_v1`, every plan digest has an exact typed preimage. Define:

```text
ProviderWorkspaceIdentityV1 = {
  version: 1,
  workspace: Text,
  kubernetes_scope: ProviderKubernetesScopeV1
}

PriorLaunchBasisV1 = CompletedLaunchBasisV1 | PartialLaunchCleanupBasisV1

CompletedLaunchBasisV1 = {
  version: 1,
  basis_kind: "completed_launch",
  source_store: "api_resource_actions" |
                "serve_resource_action_shadow_samples",
  launch_action_id: UUID,
  launch_resource_identity: ProviderLifecyclePlanV1.resource_identity,
  launch_requested_target: ProviderLocatorV1,
  launch_resources: ProviderPodResourceSnapshotV1,
  launch_workspace_identity: ProviderWorkspaceIdentityV1,
  launch_resolved_target: ResolvedProviderTargetV1,
  launch_resolved_target_sha256: Sha256,
  launch_handle: ProviderKubernetesHandleV1,
  launch_handle_sha256: Sha256,
  launch_cleanup_target: ProviderKubernetesCleanupTargetV1,
  launch_cleanup_target_sha256: Sha256,
  launch_immutable_spec_sha256: Sha256,
  exact_resources_override: true
}

ProviderLaunchEffectClaimV1 = {
  version: 1,
  launch_attempt: PositiveInteger,
  request_id: UUID,
  request_execution_generation: PositiveInteger,
  worker_attestation: ProviderAuthorityWorkerAttemptAttestationV1,
  worker_attestation_sha256: Sha256
}

ProviderLaunchCommittedEffectEvidenceV1 =
    ProviderCoreV1CreateCommitEvidenceV1 |
    ProviderClusterRecordCommitEvidenceV1 |
    ProviderSkyletJobCommitEvidenceV1

ProviderCoreV1CreateCommitEvidenceV1 = {
  version: 1,
  evidence_kind: "core_v1_create_committed",
  effect_sequence: 0 | 1 | 2,
  effect_kind: "core_v1_create",
  role: ProviderObjectRoleV1,
  intent_phase: "CREATE_INTENT",
  intent_origin: ProviderLaunchEffectClaimV1,
  evidence_commit_origin: ProviderLaunchEffectClaimV1,
  commit_disposition: "created" | "adopted_exact",
  request_body_sha256: Sha256,
  requested_semantic_sha256: Sha256,
  object_at_commit: ProviderKubernetesResolvedObjectV1
}

ProviderClusterRecordCommitEvidenceV1 = {
  version: 1,
  evidence_kind: "cluster_record_insert_committed",
  effect_sequence: 3,
  effect_kind: "cluster_record_insert",
  role: null,
  intent_phase: "HANDLE_INTENT",
  intent_origin: ProviderLaunchEffectClaimV1,
  evidence_commit_origin: ProviderLaunchEffectClaimV1,
  write_disposition: "inserted" | "adopted_exact",
  intended_handle: ProviderKubernetesHandleV1,
  intended_handle_sha256: Sha256
}

ProviderSkyletJobCommitEvidenceV1 = {
  version: 1,
  evidence_kind: "skylet_job_submit_committed",
  effect_sequence: 4,
  effect_kind: "skylet_job_submit",
  role: null,
  intent_phase: "JOB_INTENT",
  intent_origin: ProviderLaunchEffectClaimV1,
  evidence_commit_origin: ProviderLaunchEffectClaimV1,
  commit_disposition: "submitted" | "adopted_exact",
  submit_request_sha256: Sha256,
  job_at_commit: ProviderSkyletJobEvidenceV1
}

ProviderLaunchEffectDefinitiveNoEffectV1 = one of:
  {version: 1,
   proof_kind: "core_v1_422_no_create",
   request_body_sha256: Sha256,
   response_status: 422,
   post_observation: ProviderLifecycleObservationV1}
  {version: 1,
   proof_kind: "cluster_record_no_commit",
   intended_handle_sha256: Sha256,
   transaction_result: "rolled_back",
   cluster_name: Text,
   expected_cluster_record_uuid: UUID,
   post_read_disposition: "not_found",
   observed_cluster_record_uuid: null,
   observed_handle: null,
   observed_at: UtcTimestamp}
  {version: 1,
   proof_kind: "cluster_record_no_commit",
   intended_handle_sha256: Sha256,
   transaction_result: "conflict_no_write",
   cluster_name: Text,
   expected_cluster_record_uuid: UUID,
   post_read_disposition: "different_identity_conflict",
   observed_cluster_record_uuid: UUID,
   observed_handle: ProviderKubernetesHandleV1,
   observed_at: UtcTimestamp}
  {version: 1,
   proof_kind: "skylet_rejected_before_job_commit",
   submit_request_sha256: Sha256,
   rejection: "same_key_different_spec" | "schema_rejected",
   post_job: ProviderSkyletJobEvidenceV1,
   pending_start_outbox: false,
   active_run_token: false}

ProviderLaunchNoEffectResolutionV1 = one of:
  {version: 1,
   effect_sequence: 0 | 1 | 2 | 3 | 4,
   effect_kind: "core_v1_create" | "cluster_record_insert" |
                "skylet_job_submit",
   role: null | ProviderObjectRoleV1,
   intent_phase: "CREATE_INTENT" | "HANDLE_INTENT" | "JOB_INTENT",
   intent_cursor_sha256: Sha256,
   intent_origin: ProviderLaunchEffectClaimV1,
   resolution_origin: ProviderLaunchEffectClaimV1,
   resolution: "definitive_no_effect",
   evidence_sha256: Sha256,
   definitive_no_effect: ProviderLaunchEffectDefinitiveNoEffectV1}
  {version: 1,
   effect_sequence: 0 | 1 | 2 | 3 | 4,
   effect_kind: "core_v1_create" | "cluster_record_insert" |
                "skylet_job_submit",
   role: null | ProviderObjectRoleV1,
   intent_phase: "CREATE_INTENT" | "HANDLE_INTENT" | "JOB_INTENT",
   intent_cursor_sha256: Sha256,
   intent_origin: ProviderLaunchEffectClaimV1,
   resolution_origin: ProviderLaunchEffectClaimV1,
   resolution: "call_not_entered",
   evidence_sha256: null,
   definitive_no_effect: null}

ProviderLaunchEffectQuiescenceV1 = one of:
  {version: 1,
   effect_sequence: 0 | 1 | 2 | 3 | 4,
   effect_kind: "core_v1_create" | "cluster_record_insert" |
                "skylet_job_submit",
   role: null | ProviderObjectRoleV1,
   intent_phase: "CREATE_INTENT" | "HANDLE_INTENT" | "JOB_INTENT",
   resolution: "evidence_committed",
   evidence_sha256: Sha256,
   committed_evidence: ProviderLaunchCommittedEffectEvidenceV1,
   definitive_no_effect: null}
  {version: 1,
   effect_sequence: 0 | 1 | 2 | 3 | 4,
   effect_kind: "core_v1_create" | "cluster_record_insert" |
                "skylet_job_submit",
   role: null | ProviderObjectRoleV1,
   intent_phase: "CREATE_INTENT" | "HANDLE_INTENT" | "JOB_INTENT",
   resolution: "definitive_no_effect",
   evidence_sha256: Sha256,
   committed_evidence: null,
   intent_origin: ProviderLaunchEffectClaimV1,
   resolution_origin: ProviderLaunchEffectClaimV1,
   definitive_no_effect: ProviderLaunchEffectDefinitiveNoEffectV1}
  {version: 1,
   effect_sequence: 0 | 1 | 2 | 3 | 4,
   effect_kind: "core_v1_create" | "cluster_record_insert" |
                "skylet_job_submit",
   role: null | ProviderObjectRoleV1,
   intent_phase: "CREATE_INTENT" | "HANDLE_INTENT" | "JOB_INTENT",
   resolution: "call_not_entered",
   evidence_sha256: null,
   committed_evidence: null,
   intent_origin: ProviderLaunchEffectClaimV1,
   resolution_origin: ProviderLaunchEffectClaimV1,
   definitive_no_effect: null}

ProviderLaunchSupersessionQuiescenceV1 = {
  version: 1,
  launch_action_id: UUID,
  launch_attempt: PositiveInteger,
  request_id: UUID,
  request_terminal_state: "SUCCEEDED",
  active_claim: false,
  handler_terminal_result_sha256: Sha256,
  launch_provider_cursor_sha256: Sha256,
  effects: [ProviderLaunchEffectQuiescenceV1],
  settled_at: UtcTimestamp
}

PartialLaunchCleanupBasisV1 = {
  version: 1,
  basis_kind: "partial_launch_cleanup",
  source_store: "api_resource_actions",
  launch_action_id: UUID,
  launch_attempt: PositiveInteger,
  launch_resource_identity: ProviderLifecyclePlanV1.resource_identity,
  launch_requested_target: ProviderLocatorV1,
  launch_resources: ProviderPodResourceSnapshotV1,
  launch_workspace_identity: ProviderWorkspaceIdentityV1,
  launch_provider_cursor: ProviderLaunchProgressV1,
  launch_provider_cursor_sha256: Sha256,
  launch_provider_progress_revision: PositiveInteger,
  launch_quiescence: ProviderLaunchSupersessionQuiescenceV1,
  launch_quiescence_sha256: Sha256,
  launch_cleanup_target: ProviderKubernetesCleanupTargetV1,
  launch_cleanup_target_sha256: Sha256,
  launch_immutable_spec_sha256: Sha256,
  exact_resources_override: true
}
```

`PriorLaunchBasisV1` intentionally has no `NOT_STARTED` variant. The parent's
reducer maintains a closed monotonic `ServeLaunchNoIoPrefixV1` in each settled
revision-zero no-I/O launch outcome. Each link embeds the complete current
attempt projection and the predecessor's reducer-owned prefix hash; the
preimages remain in immutable retained attempt rows, while direct proof/replay
locks only predecessor then current attempt and is O(1). The direct path accepts
exactly an unmaterialized empty prefix, a terminal-request-unsettled attempt it
settles atomically, or an already-settled current attempt whose request may
have been garbage-collected. The retained-settled path preserves that attempt's
historical outcome and writes only the later action result. A retry attempt with
an inherited nonnull cursor has null prefix and is not eligible even if its own
provider-I/O watermark remains `NOT_STARTED`.

The path persists the closed
`ServeReplicaActionDirectNoEffectCancellationV1` proof and exact reducer-owned
cancellation outcome. The Serve transaction creates no down action, down link,
cleanup target, prior-launch basis, or provider no-effect resolution; it
terminalizes the launch with `terminal_disposition='CANCELLED_NO_EFFECT'`,
releases its counted slot/capacity exactly once, and applies the owner-fenced
replica/generation cancellation projection. A real down action is required only
after the launch has action-wide provider-I/O-started evidence or a completed
launch exists. `launch_io_started` is an O(1) typed predicate over the locked
current attempt, not a scan or serialized list of historical attempts. It is
true exactly when that attempt has either (a) a valid non-`NOT_STARTED`
provider-I/O boundary and its required nonnull cursor, or (b) a valid inherited
nonnull cursor even though the new retry's own boundary remains `NOT_STARTED`.
Case (b) is sufficient because attempt materialization already byte-validated
the cursor against the immutable predecessor outcome before committing it. The
request-lifecycle `mutation_boundary='SETTLED'` never satisfies the predicate
by itself. An exact revision-zero current attempt can establish no-I/O only
through the parent's rolling prefix; a missing/invalid prefix or cursor/
boundary mismatch is corruption, not either proof. Thus an inherited retry
must be reconciled and quiesced, while direct proof construction locks at most
predecessor and current attempt.

Supersession quiescence uses action-internal mutation order: create roles 0-2,
cluster-record insert 3, and Skylet submit 4. This is deliberately distinct
from the legacy wire-effect trace, which excludes the local cluster-row
transaction and therefore numbers Skylet submit as 3.

The action-internal effect table is a literal v1 protocol constant:

| Sequence | Effect kind | Role | Intent phase | Frozen full preimage |
|---:|---|---|---|---|
| 0 | `core_v1_create` | `head_ssh_service` | `CREATE_INTENT` | object-plan 0 `request_body` |
| 1 | `core_v1_create` | `head_service` | `CREATE_INTENT` | object-plan 1 `request_body` |
| 2 | `core_v1_create` | `head_pod` | `CREATE_INTENT` | object-plan 2 `request_body` |
| 3 | `cluster_record_insert` | null | `HANDLE_INTENT` | complete `intended_handle` |
| 4 | `skylet_job_submit` | null | `JOB_INTENT` | complete `submit_request` |

The CoreV1 request-body and semantic hashes are checked against the full
applicable `ProviderKubernetesObjectPlanV1` preimages in the immutable spec.
For Skylet, the one pure session-owned submit comparator specified below
reconstructs the complete `ProviderSkyletSubmitRequestV1` from the enclosing
launch action ID, invocation, and execution capsule and computes its canonical
bytes and SHA-256. Every sequence-4 intent, commit, or no-effect proof must equal
that one reconstruction and hash. Hash or authority metadata without those full
preimages is insufficient. The cluster-row record retains its full intended
handle as well as its hash.

`ProviderLaunchEffectClaimV1` is the immutable origin of an intent or evidence
commit. Its request ID is exactly
`uuid5(enclosing_launch_action_id, f"attempt:{launch_attempt}")`; its embedded
attestation has the same request ID and execution generation, and
`worker_attestation_sha256` is the canonical hash of that complete embedded
attestation, including its worker and authority-worker identity preimages. The
claim stores the attestation snapshot that existed at the origin commit; a
later legal `after: null -> exact identity` completion in the attempt envelope
does not rewrite that snapshot or its hash.

Each intent cursor retains an `intent_origin` written in the same API006
transaction as the intent. Each `C<i>` copies that origin byte-for-byte and
adds the immutable claim-origin snapshot used by the evidence checkpoint as
`evidence_commit_origin`. Both are validated against their own attempt's
deterministic request and live execution fence; a live attestation may be the
same snapshot or its one legal same-execution `after` completion without
rewriting the origin. Exact readback under a later claim may create committed
evidence with a later evidence-commit origin, but it cannot replace the
original intent origin. Origin order is lexicographic by
`(launch_attempt, request_execution_generation)`, never by bare generation:
the evidence-commit origin cannot precede its intent origin, and origins for a
later effect cannot precede the preceding committed effect's evidence origin.
Execution generation may restart at one for a new attempt.

A launch has `prior_launch_basis=null` and `prior_cleanup_target=null`. Its
resource hash is the canonical
SHA-256 of `resources`; its placement hash is the canonical SHA-256 of
`{version: 1, launch_resource_identity: resource_identity,
launch_requested_target: requested_target, launch_resources: resources,
exact_resources_override: true}`; and its workspace hash is the canonical
SHA-256 of `ProviderWorkspaceIdentityV1`.

A primary down requires a `PriorLaunchBasisV1` and a byte-equal
`prior_cleanup_target`. The basis action ID must be the UUID derived from its
launch identity/spec. Admission loads and locks that exact retained launch row
and applicable attempt evidence from `source_store`, plus the exact
global-user-state cluster row disposition named by the cleanup target. It
recomputes every added hash from the full typed preimages and rejects
caller-supplied bytes that are not equal to retained state.

For `completed_launch`, the retained launch is terminal-successful. An API
launch's final API006 cursor supplies the resolved target and handle; a shadow
launch's completed child supplies the exact resolved-target observation and
the same-UUID cluster row supplies the handle. Both must agree on cluster UUID,
all three object UIDs, Pod UID, provider scope, and the complete cleanup target.
The cluster row's provider block is byte-equal to `launch_handle`.

`partial_launch_cleanup` is allowed only for a settled API-action launch that
did not succeed, satisfies the action-wide `launch_io_started` predicate above,
and whose exact final outcome contains
`ProviderLaunchSupersessionQuiescenceV1`. Admission first
holds the exact service/replica-incarnation fence and constructs the complete
candidate down identity/spec from a read-only source snapshot. It then visits
the sorted union of source-launch and deterministic down action IDs in canonical
action-ID order. At each key it locks and validates the existing row, or
inserts/exactly adopts the allowed new down row at that key. An insert or
conflict adoption is an action-row-class acquisition at its sorted position; the
transaction never locks a higher action ID and later inserts a lower one. After
all action keys are acquired it locks the source attempt and revalidates every
source cursor, quiescence, cleanup-target, and immutable-spec byte used to
construct the candidate. Any mismatch rolls back the entire transaction,
including a newly inserted down row. It derives the three-slot cleanup target
from the retained launch object plans, every committed UID/allocation, and an
exact same-UUID cluster-row read.

The progress and quiescence prefixes are also literal protocol constants. In
the following table, `C<i>` is the complete immutable committed-effect record
for sequence `i`; `E<i>` is an `evidence_committed` quiescence entry that embeds
and hashes that exact record; and `N<i>` is exactly one `call_not_entered` or the
effect-specific `definitive_no_effect` entry allowed below. Brackets denote the
complete list, not a subset or a pattern:

| Exact `ProviderLaunchProgressV1` phase | Exact `committed_effects` | Exact quiescence `effects` |
|---|---|---|
| `CREATE_INTENT(head_ssh_service)` | `[]` | `[N0]` |
| `OBJECTS_PARTIAL` with 1 committed slot | `[C0]` | `[E0]` |
| `CREATE_INTENT(head_service)` | `[C0]` | `[E0, N1]` |
| `OBJECTS_PARTIAL` with 2 committed slots | `[C0, C1]` | `[E0, E1]` |
| `CREATE_INTENT(head_pod)` | `[C0, C1]` | `[E0, E1, N2]` |
| `OBJECTS_PARTIAL` with 3 committed slots and no Pod `nodeName` | `[C0, C1, C2]` | `[E0, E1, E2]` |
| `OBJECTS_EXACT` | `[C0, C1, C2]` | `[E0, E1, E2]` |
| `HANDLE_INTENT` | `[C0, C1, C2]` | `[E0, E1, E2, N3]` |
| `HANDLE_COMMITTED` | `[C0, C1, C2, C3]` | `[E0, E1, E2, E3]` |
| `RUNTIME_READY` | `[C0, C1, C2, C3]` | `[E0, E1, E2, E3]` |
| `JOB_INTENT` | `[C0, C1, C2, C3]` | `[E0, E1, E2, E3, N4]` |
| `JOB_COMMITTED` | `[C0, C1, C2, C3, C4]` | `[E0, E1, E2, E3, E4]` |
| `JOB_RUNNING` | `[C0, C1, C2, C3, C4]` | `[E0, E1, E2, E3, E4]` |
| `ENDPOINT_RESOLVED` | `[C0, C1, C2, C3, C4]` | `[E0, E1, E2, E3, E4]` |
| `SUCCEEDED` | `[C0, C1, C2, C3, C4]` | `[E0, E1, E2, E3, E4]`; partial basis forbidden |

`OBJECTS_PARTIAL` with three slots is valid only while the head Pod's sole
scheduler allocation is absent; once `nodeName` is known, the cursor is
`OBJECTS_EXACT`. Runtime checks and endpoint resolution add no mutation effect.
A current intent cannot resolve as `evidence_committed` while retaining its
intent phase: the same claim-fenced API006 commit first appends `C<i>` and
advances to the corresponding post-effect phase. An inherited current intent
with neither exact committed readback nor an origin-bound `N<i>` result from
the original execution claim cannot be handed off.

The allowed `N<i>` resolution is exact by sequence. `N0` through `N2` are
either `call_not_entered` or `core_v1_422_no_create`; `N3` is either
`call_not_entered` or `cluster_record_no_commit`; and `N4` is either
`call_not_entered` or `skylet_rejected_before_job_commit`. A timeout, reset,
5xx response, lost acknowledgement, expired claim, or point-in-time NotFound
is no resolution at any sequence. Each `evidence_committed` entry has
`definitive_no_effect=null`, embeds the canonical complete `C<i>` record, and
hashes that record including both origins and its disposition.
Each `definitive_no_effect` entry embeds the complete proof and hashes that
canonical proof. Both no-effect resolutions retain the cursor's byte-equal
`intent_origin` and the handler result's `resolution_origin`. Their origin
claims, including the complete attestation preimage and hash, must be
byte-equal, so a reclaimed or later-attempt handler cannot resolve an earlier
entrant. `call_not_entered` has null evidence/proof fields and is claim-fenced-
written before that original claim enters the call. For a definitive proof,
the terminal result's live worker attestation may be the origin snapshot's one
documented same-execution `after` completion, but `resolution_origin` remains
the original immutable claim. A different request ID, attempt, execution
generation, worker, or authority identity is never a no-effect resolver;
cross-claim recovery must exact-adopt committed evidence or remain observation-
first.

The CoreV1 422 proof is valid only for the exact current create intent, exact
request-body hash and original intent-origin execution claim, and the
synchronous 422 response to that claim's create. Its post-observation uses the
same live client and has
complete, byte-equal frozen-scope reads before and after all three exact-name
GETs. Its object list and overall state are fixed by this table:

| Failing sequence | Exact object dispositions in role order | Overall state and certainty |
|---:|---|---|
| 0 | `not_found, not_found, not_found` | `absent, authoritative` |
| 1 | `present, not_found, not_found` | `uncertain, authoritative` |
| 2 | `present, present, not_found` | `uncertain, authoritative` |

Every earlier `present` entry is byte-equal in identity, UID, admitted
semantic hash, and allocations to its `C<i>` record. The failing and every
later role are exact `not_found`; their response-derived UID, identity-label,
normalized-observed-semantic, observed-semantic-hash, spec-match, allocation,
deletion, phase, and readiness fields are respectively null or empty. Their
role/kind/name/scope and requested-semantic hash still equal the frozen plan.
At the observation top level, provider operation ID, provider resource ID,
workload UID, resolved target, and readiness are null. The authoritative mixed
state for sequences 1 and 2 is deliberately `uncertain`; no additional partial
state exists. If exact readback instead finds the failing object, the handler
must adopt it and append the commit evidence before quiescence. A conflict or
uncertain read blocks; it cannot be rewritten as 422 no-effect.

The cluster-row no-commit proof has this exhaustive field matrix:

| `transaction_result` | `post_read_disposition` | `observed_cluster_record_uuid` | `observed_handle` |
|---|---|---|---|
| `rolled_back` | `not_found` | null | null |
| `conflict_no_write` | `different_identity_conflict` | nonnull and different from expected | exact typed handle for that UUID and the same cluster name |

Every row also requires the exact sequence-3 intended-handle hash, cluster
name, expected cluster-record UUID, and transaction/lock result. `rolled_back`
means an observed transaction rollback; a later NotFound alone cannot prove
it. A legacy same-name row with a null UUID, any null or unparseable provider
block, a same expected UUID with a different handle, the expected UUID under
another name, or an out-of-contract conflict race blocks. A structurally valid
different-UUID conflict can settle
the source launch, but partial-down admission still requires a fresh cleanup
read yielding either the exact own-UUID handle or exact NotFound; the conflicting
row itself cannot become the cleanup target.

The Skylet rejection proof requires the exact current `JOB_INTENT`. The pure
session-owned submit comparator must reproduce that cursor's complete request,
launch-action submission key, capsule job-contract hash, job-spec bytes/hash,
and `submit_request_sha256`; the proof hash and every expected-value copy in
`post_job` equal that result. The proof also carries the runtime evidence's
exact state-store UUID. The job read, retained-request reconstruction, and the
`pending_start_outbox=false` and `active_run_token=false` facts come from one
SQLite transaction, or from two reads pinned to the same unchanged record
revision. For `schema_rejected`,
`post_job.read_disposition="not_found"`; `retained_submit_request`, durable
state, job ID, run epoch, and record revision are null, while both top-level job
hashes equal the comparator's attempted request. For
`same_key_different_spec`, the disposition is `conflict`, the reconstructed
`retained_submit_request` is nonnull and canonical-byte-unequal to the expected
request, durable state is exactly `SUCCEEDED`, `FAILED`, or `BLOCKED`, and job
ID, run epoch, and record revision are nonnull. No retained-hash difference is
required: equal hashes with unequal full bytes remain conflict. A runnable
state, `uncertain` read, null required revision, changing revision, pending
outbox, or active run token blocks handoff. Conversely, `C4` requires a
`present` job whose nonnull reconstructed retained request is byte-equal to the
same comparator result and whose durable state, job ID, run epoch, and revision
are nonnull. Later job evidence may only follow the documented monotonic
state/revision/run-epoch transitions with stable expected key/hashes, byte-equal
retained request, job ID, and state-store UUID; `job_at_commit` itself remains
immutable.

`ProviderLaunchSupersessionQuiescenceV1` is constructed by the Serve reducer,
never accepted from a caller. It names the exact action, attempt, request,
terminal request state, final cursor hash, and retained request settlement row:
`request_terminal_state` and `settled_at` equal that row's terminal state and
terminal-transition timestamp. `active_claim=false` is proved by the terminal
request fence, not trusted from the serialized boolean. Its effect list is
exactly the applicable phase table row, with no omitted, duplicated, reordered,
or caller-added entry. Each `E<i>` embeds and hashes the byte-equal `C<i>`;
each `N<i>` copies the intent and resolution origins from the validated handler
terminal result. Origin tuples obey the lexicographic attempt/generation order
above; no standalone generation monotonicity exists. Request terminalization
atomically closes the
request envelope without locking the action attempt. The handler may
claim-fenced-persist only the current `N<i>` resolution input in its typed
terminal result. That input is exactly one
`ProviderLaunchNoEffectResolutionV1` nested in the companion's closed
`ServeReplicaActionHandlerTerminalResultV1`; neither it nor request
terminalization constructs the final quiescence object. The later
owner-fenced attempt-outcome transaction byte-compares API006 and the terminal
request row, requires the exact handler-terminal-result hash named here,
constructs this object, and persists it before admitting any partial basis.
The request row must be terminal `SUCCEEDED`, retain the companion's exact
nonnull return envelope and hash-valid terminal DTO with
`reduction_kind="supersede_to_down"`, and have no active claim. For a current-
intent phase the DTO contains exactly the matching original-claim `N<i>` and
the reducer constructs `E* + N<i>`. For every nonintent, non-`SUCCEEDED` phase
the DTO has null resolution and the reducer constructs the exact E-only row.
Every legal row in that phase table has a nonnull cursor and satisfies the
current-attempt `launch_io_started` predicate. Revision-zero/null-cursor input
is never an E-only row and must use the parent's direct no-effect route after
the request fence. An E-only phase with a resolution, an intent phase without
its exact resolution, any supersession result at `SUCCEEDED`, or a
noncancelled provider tuple is corruption and admits no down.

External `FAILED`/`CANCELLED` terminalization, a null/dropped return, or an
invalid/mismatched terminal-`SUCCEEDED` DTO is categorically ineligible for
quiescence and partial cleanup. It uses the parent's closed request-fallback
table: an exact non-`SUCCEEDED` cursor remains observation-first, an exact
`SUCCEEDED` cursor commits normal provider success, a revision-zero empty
journal retries when the action remains desired, and malformed progress
blocks. External terminalization never supplies `N<i>` or partial handoff. The
only exception is not a provider quiescence path: when owner-fenced teardown is
requested and the parent's exact no-I/O prefix proves the empty, newly settled,
or retained-settled shape, the reducer constructs
`ServeReplicaActionDirectNoEffectCancellationV1` and
`CANCELLED_NO_EFFECT`; the retained request may already be garbage-collected.
That proof is not
`ProviderLaunchNoEffectResolutionV1`; no provider intent exists, so inventing
an intent origin, resolution origin, or `call_not_entered` entry is invalid.
For partial cleanup, the request must be terminal with no active claim before
admission, so no old handler can emit another effect. A `SUCCEEDED` cursor or
successful launch action rejects `partial_launch_cleanup` regardless of request
terminal state.

An ambiguous or in-flight intent, a lost acknowledgement without exact
evidence, or a merely expired claim keeps the old launch observation-first and
prevents down admission. Unknown UID slots are allowed only for create roles
whose effect is outside the cursor prefix or whose current intent has
`call_not_entered` or a matching `core_v1_422_no_create` proof. They are never
guessed from NotFound.

Both variants share service, service incarnation, replica ID, replica
incarnation, target, resources, and workspace with the down; the down
generation is exactly launch generation plus one. Down recomputes resource,
placement, workspace, cursor/target, resolved-target when present, and handle
hashes with the canonical serializer. It never copies unverifiable digest-only
values. Retention protects the referenced launch action/sample, attempt/child,
and cluster row disposition while down is live. `LAUNCH_CLEANUP_DOWN` remains
only a non-authoritative legacy-shadow child; it is not the partial authoritative
basis described here.

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
    scope: ProviderKubernetesScopeV1,
    cluster_fingerprint_sha256: Sha256,
    namespace: Text,
    name_basis: ProviderWorkloadNameBasisV1,
    provider_cluster_name: Text,
    workload_kind: Text,
    workload_name: Text,
    cluster_record_uuid_label: Text,
    replica_incarnation_label: Text,
    topology: ProviderPodTopologyV1
  }
}

ProviderKubernetesTransportIdentityV1 = {
  version: 1,
  server_origin: {scheme: "https", host: Text,
                  port: PositiveInteger, path: Text},
  tls_server_name: null | Text,
  ca_cert_der_base64: [CanonicalRfc4648Base64Text4To16384]
}

ProviderRepoArtifactRefV1 = {
  repo_path: Text,
  byte_size: PositiveInteger,
  sha256: Sha256
}

ProviderSkyletJobContractV1 = {
  schema_id: "skypilot.serve.prebooted-canary-job.v1",
  schema_artifact: ProviderRepoArtifactRefV1,
  renderer_artifact: ProviderRepoArtifactRefV1,
  state_store_schema_artifact: ProviderRepoArtifactRefV1,
  protocol_artifact_role: "skylet_job_protocol"
}

ProviderSkyletJobSpecV1 = {
  version: 1,
  schema_id: "skypilot.serve.prebooted-canary-job.v1",
  source: ProviderLaunchContentSourceV1,
  command_profile: "image_serve_canary_entrypoint_v1",
  entrypoint_artifact_role: "serve_canary_entrypoint",
  replica_id: DecimalIntegerText,
  environment: {"SKYPILOT_SERVE_REPLICA_ID": DecimalIntegerText},
  working_directory: null,
  setup: null,
  mounts: [],
  secrets: [],
  lifecycle: "long_running_until_pod_delete",
  restart_policy: "same_pod_same_logical_job"
}

ProviderSkyletSubmitRequestV1 = {
  protocol: "skylet_idempotent_submit_v1",
  submission_key: UUID,
  job_contract_sha256: Sha256,
  job_spec: ProviderSkyletJobSpecV1,
  job_spec_sha256: Sha256
}

ProviderWorkloadArtifactBindingV1 = {
  role: "ray_runtime" | "skylet_runtime" | "skylet_job_protocol" |
        "skylet_state_schema" | "startup_probe" |
        "serve_canary_entrypoint",
  workload_image_digest: "sha256:" + 64LowerHex,
  installed_root: Text,
  source_manifest: ProviderRepoArtifactRefV1,
  image_build_attestation: ProviderRepoArtifactRefV1,
  measurement_contract: "canonical_regular_file_tree_v1"
}

ProviderRuntimeArtifactMeasurementV1 = {
  role: ProviderWorkloadArtifactBindingV1.role,
  binding_sha256: Sha256,
  observed_tree_sha256: Sha256,
  matches_expected_manifest: true
}

ProviderSkyletDurabilityContractV1 = {
  volume_name: "skylet-state",
  volume_kind: "emptyDir",
  store: "sqlite_wal_synchronous_full_v1",
  schema_artifact: ProviderRepoArtifactRefV1,
  transaction_contract: "job_and_start_outbox_same_transaction_v1",
  drain_order: "job_id_ascending",
  launcher_contract: "durable_run_token_and_post_exec_handshake_v1"
}

ProviderKubernetesScopeV1 = {
  version: 1,
  context_name: Text,
  context_identity: [Text],
  in_cluster: Boolean,
  namespace: Text,
  transport: ProviderKubernetesTransportIdentityV1,
  kube_system_namespace_uid: Text,
  target_namespace_uid: Text,
  api_server_git_version: Text,
  caller_service_account_namespace: Text,
  caller_service_account_name: Text,
  caller_service_account_uid: Text,
  workload_service_account_namespace: Text,
  workload_service_account_name: Text,
  workload_service_account_uid: Text
}

ProviderKubernetesScopeReadV1 = {
  disposition: "complete" | "not_found" | "forbidden" | "timeout" |
               "transport_error" | "malformed",
  scope: null | ProviderKubernetesScopeV1,
  observed_at: UtcTimestamp
}

ProviderWorkloadNameBasisV1 = {
  version: 1,
  display_name: Text,
  frozen_user_hash: Text,
  max_length: 42,
  cluster_name_hash_length: 8
}

ProviderPodTopologyMutableObjectV1 = {
  kind: "Service" | "Pod",
  role: "head_ssh_service" | "head_service" | "head_pod",
  name: Text,
  labels: [{key: Text, value: Text}]
}

ProviderPodTopologyV1 = {
  version: 1,
  kind: "single_direct_pod_two_services",
  node_count: 1,
  application_port: DecimalPortText,
  resources_ports: [DecimalPortText],
  mutable_objects: [  # each entry is ProviderPodTopologyMutableObjectV1
    {kind: "Service", role: "head_ssh_service", name: Text,
     labels: [{key: Text, value: Text}]},
    {kind: "Service", role: "head_service", name: Text,
     labels: [{key: Text, value: Text}]},
    {kind: "Pod", role: "head_pod", name: Text,
     labels: [{key: Text, value: Text}]}
  ],
  shared_prerequisites: "preexisting_read_only"
}

ProviderKubernetesObjectRoleMapV1 = [
  {plan_sequence: 0, role: "head_ssh_service", kind: "Service",
   name_rule: "workload_name_plus_-ssh", create_sequence: 0,
   delete_sequence: 1},
  {plan_sequence: 1, role: "head_service", kind: "Service",
   name_rule: "workload_name", create_sequence: 1, delete_sequence: 0},
  {plan_sequence: 2, role: "head_pod", kind: "Pod",
   name_rule: "workload_name", create_sequence: 2, delete_sequence: 2}
]

ProviderObjectRoleV1 =
  "head_ssh_service" | "head_service" | "head_pod"

ProviderKubernetesObjectPlanV1 = {
  sequence: 0 | 1 | 2,  # 0=head_ssh_service, 1=head_service, 2=head_pod
  role: "head_ssh_service" | "head_service" | "head_pod",
  api_version: "v1",
  kind: "Service" | "Pod",
  namespace: Text,
  name: Text,
  required_identity_labels: [{key: Text, value: Text}],
  request_body: CanonicalJsonObject,
  request_body_sha256: Sha256,
  requested_semantic: CanonicalJsonObject,
  requested_semantic_sha256: Sha256,
  comparison_contract: "kubernetes_admitted_object_v1",
  normalization_profile: ProviderRepoArtifactRefV1
}

ProviderKubernetesServerAllocationV1 = {
  json_pointer: Text,
  allocator: "api_server" | "scheduler",
  value: CanonicalJsonValue
}
```

The job-spec leaf requires its two decimal replica-ID copies to be byte-equal
and recomputes its complete canonical bytes. The submit-request leaf owns only
closed shape, the fixed protocol, scalar UUID/SHA validation, typed job-spec
parsing, and
`job_spec_sha256 == canonical_sha256(job_spec.canonical_value())`.
`submission_key` and `job_contract_sha256` are deliberately context-free
scalars at that leaf; it cannot know the enclosing action ID or the capsule's
complete job contract. It also does not accept or compute a hash of itself.

`KubernetesResourceActionSession.validate_skylet_submit_binding_v1()` is the
single pure contextual comparator. It performs no Kubernetes, Skylet, database,
or filesystem I/O. Given the complete immutable launch spec, it reconstructs
the job spec through the fixed profile from the byte-equal invocation/capsule
source and the one replica environment value; requires the candidate job spec
to be byte-equal; requires `submission_key` to equal the enclosing launch action
ID; and requires `job_contract_sha256` to equal the canonical SHA-256 of the
complete capsule `post_provision.job_submission.contract`. It then returns the
canonical complete submit-request bytes and SHA-256. The execution session,
typed API006 cursor validator, committed-effect validator, and no-effect-proof
validator all call this exact comparator rather than reimplementing a subset.
For Skylet readback, the same comparator reconstructs a retained submit request
from the transactional `SkyletJobRecordV1` key, contract hash, complete job
spec, and spec hash. It compares canonical request bytes, not a claimed equality
boolean or hash equality; a bounded byte-different retained spec remains a
conflict even if adversarial hash scalars collide.

`ProviderKubernetesObjectRoleMapV1` is a literal protocol constant, not
configuration. Topology objects, execution-config object plans, partial/full
targets, and observation objects all serialize in plan-sequence order from
that table. The legacy provider-wire trace uses create sequences 0-2 followed
by Skylet submit at wire sequence 3. The authoritative action-internal journal
instead inserts cluster-record commit at sequence 3 and numbers Skylet submit
as sequence 4, exactly as its separate table specifies. Down effects use delete
sequence. A role, kind, name, or sequence mismatch is invalid even when every
object is otherwise canonical.

The only v1 server allocations, in exact pointer order within each role, are:

```text
head_ssh_service: /spec/clusterIP, /spec/clusterIPs, /spec/ipFamilies,
                  /spec/ipFamilyPolicy                    (api_server)
head_service:     /spec/clusterIP, /spec/clusterIPs, /spec/ipFamilies,
                  /spec/ipFamilyPolicy                    (api_server)
head_pod:         /spec/nodeName                           (scheduler)
```

The pure allocation leaf dispatches only on its pointer and validates no
cross-allocation state. The four Service pointers require the `api_server`
allocator: `clusterIP` is either canonical IPv4/IPv6 text or the literal
`None`; `clusterIPs` is a one-element array containing one canonical IP or
`None`; `ipFamilies` is exactly `IPv4` or `IPv6` in a one-element array; and
`ipFamilyPolicy` is exactly `SingleStack`. Canonical IP text is ASCII without a
zone identifier and must satisfy
`str(ipaddress.ip_address(value)) == value` under the checked-in helper; the
parsed `version` determines `IPv4` versus `IPv6`. `/spec/nodeName` requires the
`scheduler` allocator and a 1..253-byte canonical Kubernetes DNS-subdomain
name: split on `.`, require each 1..63-byte segment to match
`[a-z0-9](?:[a-z0-9-]*[a-z0-9])?`, and require the complete ASCII value to be
at most 253 bytes. No other pointer, allocator, scalar, empty value, or list
length is valid.

`ProviderKubernetesResolvedObjectV1`,
`ProviderKubernetesCleanupObjectV1`, and
`ProviderKubernetesObjectEvidenceV1`, rather than the allocation leaf, enforce
the role tuple. Each Service commits all four values atomically in the
displayed pointer order. `head_ssh_service` requires a non-`None` IP whose
address family equals its sole `ipFamilies` member and whose singleton
`clusterIPs` member is byte-equal. `head_service` requires
`clusterIP="None"`, `clusterIPs=["None"]`, and one server-selected family.
The first renderer never requests dual stack, so a default other than
`SingleStack` is not representable. The Pod has either no allocation while it
is not yet scheduled or exactly the one `nodeName` allocation; a committed
name never changes. A cleanup object with `committed_uid=null` has no
allocations. A non-`present` object-evidence read has no allocations; a
`present` read uses the role-valid form above. A completed-launch cleanup target
and an exact resolved target require every role's complete allocation form,
including Pod `nodeName`; partial progress/cleanup may retain an unscheduled
Pod's empty form. These enclosing validators also enforce immutability and
prevent a partial Service quartet.

The requested Pod body must omit `spec.nodeName`; an input that sets it is not
representable. Allocation arrays follow the table exactly. UID and semantic
hash commitments are write-once, while the complete allowed allocation tuple
may be appended once after UID commitment; it can never be removed, partially
committed, or changed.

`request_body` is the exact nonsecret CoreV1 body sent by the session and is
bounded together with the complete action spec to 65,536 canonical UTF-8
bytes. The fixed renderer may contain only its reviewed nonsecret runtime env
entries plus one caller-derived replica-ID entry; arbitrary task/caller env is
not accepted. Any Secret/config-map reference, projected token, credential,
private key, raw user YAML, or unbounded field rejects. Both body and requested
semantic preimages are embedded next to their hashes; the implementation does
not rely on a hash-only private interpretation.

An object-plan leaf independently enforces sequence/role/kind against
`ProviderKubernetesObjectRoleMapV1`, `api_version="v1"`, Kubernetes DNS-label
syntax for its generated name, and exactly the three sorted
`required_identity_labels` keys `skypilot-cluster-name`,
`skypilot.co/cluster-record-uuid`, and
`skypilot.co/serve-replica-incarnation`. The two identity values are canonical
UUIDs. The display-label value is derived from `plan.name` under the exact role
mapping: `head_ssh_service` requires and removes terminal `-head-ssh`, while
`head_service` and `head_pod` require and remove terminal `-head`. It recomputes
both body/preimage hashes and requires `request_body.apiVersion`, `kind`, and
`metadata.{namespace,name}` to equal the plan. The body `metadata.labels` is a
canonical object containing at least the three required key/value pairs.

The leaf validates `requested_semantic` as bounded canonical JSON and its hash,
but cannot claim to execute an artifact by hash. Preflight construction and the
execution session each resolve and hash/size-check `normalization_profile`, run
that exact normalizer over `request_body`, and require the resulting canonical
bytes to equal `requested_semantic.canonical_bytes`; neither operand is
reconstructed from a hash. The enclosing capsule requires exactly three plans
in role order, names equal to its workload-name basis/topology, every role's
complete body label map equal to that topology entry, and every plan's
`normalization_profile` byte-equal to
`renderer.admitted_object_normalization`. The Pod body omits `spec.nodeName`;
the exact body contract is:

| Role | Exact request-body bindings |
|---|---|
| `head_pod` | `/spec/nodeName` key absent; `/spec/containers` has exactly one entry and `/spec/containers/0/name="ray-node"`; its `image` equals the digest-qualified workload image and `imagePullPolicy="Always"`; its resource requests/limits contain exactly the frozen CPU and memory translations with no accelerator or ephemeral-storage entry; `/spec/serviceAccountName` equals the frozen workload ServiceAccount and `/spec/automountServiceAccountToken=false` |
| `head_ssh_service` | `/spec/ports` is exactly the singleton `[{"protocol":"TCP","port":22,"targetPort":22}]`; requested `/spec/clusterIP` is absent so the API server allocates it |
| `head_service` | `/spec/clusterIP="None"`; `/spec/ports` key is absent, not null or an empty list |

The `ray-node` env list is inspected by name, never by a numeric index. It has
exactly one `SKYPILOT_SERVE_REPLICA_ID` entry whose scalar `value` is the
canonical `DecimalIntegerText` from the launch invocation and job spec;
`valueFrom` is absent. Missing, duplicate, wrong-value, or `valueFrom` forms
reject. Other entries are only the byte-exact fixed nonsecret renderer-owned
runtime env preimage.

The management-port position is intentionally fixed:
`/spec/containers/0/ports/4/containerPort` is JSON integer `46590`. The
application-port integer appears in no Pod `containerPort`, including that
entry, and in neither Service. The normalizer explicitly rejects an application
port equal to 22, 46590, or any member of the complete fixed renderer-owned
container-port set. The SSH Service has no application or management port, and
the head Service has no port list at all. In `podip` mode `open_ports()` and
`cleanup_ports()` take
their literal no-op branches and emit no Service, Ingress, LoadBalancer, patch,
or other provider mutation.

`kubernetes_admitted_object_v1` must be implemented by the checked-in, pinned
`normalization_profile`. It removes only `status`, the enumerated server-owned
metadata fields (`uid`, `resourceVersion`, `generation`, creation/deletion
timestamps, and `managedFields`), and scheduler-assigned Pod `/spec/nodeName`.
It retains every other label, annotation, owner reference, finalizer,
container, init container, volume, service-account, security, scheduling,
image, port, selector, and spec field. The renderer sets every controllable
Pod/Service default explicitly. The profile permits only its literal reviewed
Kubernetes Pod defaults and the enumerated server allocations: Service
`clusterIP`, `clusterIPs`, `ipFamilies`, and `ipFamilyPolicy`, plus Pod
`nodeName`. Service allocations are recorded on first read. Pod `nodeName` may
be absent while scheduling and then append exactly one nonempty value; once
recorded it is write-once. A complete `ResolvedProviderTargetV1` and
`OBJECTS_EXACT` require that value, while partial evidence may retain the Pod
UID before assignment. Every later read requires every recorded allocation to
match. An injected sidecar/init container/volume/image pull secret,
label, annotation, owner reference, finalizer, or any unreviewed path/value is a
conflict. Both a 201 response readback and a 409 readback must normalize to the
stored requested semantic bytes, with only those typed allocations separated.
Raw request/readback JSON equality is never used.

This paragraph is normative, not evidence that those artifacts currently
exist. The present branch has only structural artifact-reference and object-
metadata leaves; it has no checked-in candidate renderer, binding schema, or
admitted-object normalizer that executes this body contract. Until all three
resolve by exact repository path/size/hash and realistic plus maximal goldens
pass, launch normalization returns `unrepresented_execution_config`, remains
shadow-only, and sends no action-owned provider bytes. The generic Jinja/config
path is not a substitute and is ineligible for authority.

For the initial authoritative `pod_cluster_v1` cohort, `cloud` is
`kubernetes`, the `kubernetes` block is nonnull, and `workload_kind` is exactly
`Pod`. `cluster_record_uuid_label` must equal
`str(sky_cluster_record_uuid)` and `replica_incarnation_label` must equal
`str(replica_incarnation)`, both in lowercase canonical UUID form. A null block,
another workload kind, missing label, or noncanonical/mismatching value
normalizes to unsupported before submission and to conflict if encountered
during observation; it is never eligible.

`ProviderKubernetesScopeV1` is the bounded nonsecret identity returned by the
same isolated Kubernetes context load that constructs the API client. For a
kubeconfig context, `context_identity` is the exact tuple returned by
`KubernetesContextIdentity.identity`; the authoritative cohort requires
in-cluster execution and uses the exact normalized in-cluster tuple. Tuple
element order is semantic and is preserved; the general sorted-list
canonicalization rule applies only to fields declared as sets. Namespace,
server version, and both service-account identities must agree with the frozen
execution capsule. `kube_system_namespace_uid`, `target_namespace_uid`, and
both service-account UIDs are exact live Kubernetes `metadata.uid` values. An
unreadable/missing identity or an authenticated caller other than
`system:serviceaccount:<namespace>:<name>` is not representable.

The target namespace and namespace UIDs obey
`(namespace == "kube-system") ==
(target_namespace_uid == kube_system_namespace_uid)`. The workload
service-account namespace equals the target namespace. The caller and workload
service-account `(namespace, name)` pairs are equal if and only if their UIDs
are equal. These are internal-consistency checks only; live reads and the
reviewed normalizer establish that the values describe the selected target.

The transport preimage contains only the normalized HTTPS scheme, host, port,
path, optional TLS server name, and the exact public CA certificate set encoded
as DER. The list contains 1..256 values in sorted, duplicate-free order. Each
canonical RFC 4648 base64 scalar is 4..16,384 ASCII bytes and decodes to a
nonempty payload of at most 12,288 bytes; the enclosing transport and scope
objects retain the 65,536-byte canonical-object limit. The pure DTO validates
only the closed shape, encoding, internal consistency, and bounds: constructing
one directly or through `from_value` is not representability or authority
proof. The reviewed live normalizer parses source CA material as X.509 and
emits the certificate DER; live admission consumes that typed normalization and
preflight result, never an arbitrary standalone parsed scope. Userinfo, query
strings, proxies, insecure verification, ambient system trust, client
certificates, private keys, tokens, exec/auth-plugin inputs, credential paths,
and raw kubeconfig are forbidden. Unparseable or over-budget origins/CA bundles
are not representable. `cluster_fingerprint_sha256` is exactly the lowercase
SHA-256 of the complete scope object's canonical UTF-8 bytes, so its bounded
nonsecret preimage is embedded beside the digest. A logical control plane is
defined by this transport, context identity, and both namespace UIDs;
API-server replicas behind that origin are equivalent, and a clone preserving
the full tuple is intentionally the same logical target for v1.

Preparation may use one isolated nonmutating `KubernetesApiClientTarget` and
then close it. Each execution or observation attempt opens exactly one fresh
target, derives and compares its full scope under the request execution fence,
and keeps that target alive for SelfSubjectReview/AccessReview, all exact
prerequisite reads, exact object reads, creates, and deletes in that attempt. It
revalidates all 12 semantic prerequisite roles with that same client after the
final provider call before accepting evidence. Launch additionally revalidates
both exact LB Deployment projections; down has no endpoint projection and makes
no LB Deployment read. Both may coalesce only the required release/LB Namespace
alias read while reproducing all three role records, then close the target. Scope
evidence always uses
`ProviderKubernetesScopeReadV1`: a failed
before/after read is encoded with a null scope and closed disposition, never a
fabricated complete object. The action-aware path passes its raw `ApiClient`
explicitly through the provisioner seam; it may not call cached
`core_api(context)`, reload kubeconfig, or construct a second mutation client.
A pre-mutation mismatch is an identity conflict and sends no mutating provider
bytes; a
post-call read failure makes the effect ambiguous.

The concrete mutation seam owns this exact facade set:

```text
KubernetesResourceActionSession
  target: KubernetesApiClientTarget
  core: CoreV1ResourceActionFacade
  apps_read: AppsV1WorkerAttestationFacade
  networking_read: NetworkingV1PrerequisiteFacade
  admission_read: AdmissionregistrationV1PrerequisiteFacade
  version_read: VersionReadFacade
  authentication_read: AuthenticationV1SelfReviewFacade
  authorization_read: AuthorizationV1SelfReviewFacade
```

Every wrapper receives the object-identical raw `target.api_client`; contract
tests reject a second or cached client. CoreV1 permits exact Namespace,
ServiceAccount, Pod, and Service GET, namespaced Pod/Service create, and
UID-preconditioned Pod/Service delete. AppsV1 permits the exact current
authority-worker ReplicaSet/frozen Deployment GETs and the two exact frozen
load-balancer Deployment GETs only. NetworkingV1 permits exact named
NetworkPolicy GET. AdmissionregistrationV1 permits exact named policy and
binding GET.
The kind-matched session call inventory permits the two load-balancer Deployment
GETs only for launch. A down session has no locator or method path for either GET;
an attempted down call is rejected before invoking the shared raw client.
Version permits only `GET /version`; AuthenticationV1 only creates
SelfSubjectReview; AuthorizationV1 only creates one namespaced
SelfSubjectRulesReview and the fixed SelfSubjectAccessReview matrix. The request handler
commits the applicable progress intent and revalidates its database claim and
provider scope immediately before each emitted CoreV1 or Skylet call. It never
uses the legacy create retry helper: HTTP 409 requires normalized
spec/identity/UID-qualified exact readback or conflict, 422 is terminal without
alternate bytes, and a timeout is ambiguous and forces observation. There is
no discovery client, list/watch, patch/update, collection delete,
Secret/ConfigMap read, persistent RBAC/admission/Apps mutation, PVC, Ingress,
`exec`, `cp`, or bootstrap call. Deployment access is only the exact attestation
GET above.

For the direct-Pod topology, `sky_cluster_name` is the SkyPilot display name
and `ProviderWorkloadNameBasisV1.frozen_user_hash` is the bounded nonsecret
server-effective value frozen by the launch identity canonicalization proof
below; the raw request environment map is never persisted in the action. The
controller first freezes `PreparedLaunchRequest`, then sends only its exact
`SKYPILOT_USER`/`SKYPILOT_USER_ID` slice and the already-known resource identity
to the private no-enqueue canonicalizer. The normalizer requires a byte-equal
proof response before constructing the name basis or identity leaf. A missing,
empty, non-ASCII, or over-bound effective username or a missing/invalid
effective user hash is `unfrozen_identity`. Neither path invokes an empty-
argument username helper or an ambient fallback. A new pure
`make_cluster_name_on_cloud_for_user(..., user_hash=...)` owns the historical
normalization/truncation algorithm. The existing ambient helper becomes a
compatibility wrapper that supplies `get_user_hash()`. The pure result must
equal `provider_cluster_name`; `workload_name` must equal
`provider_cluster_name + "-head"`. Invalid or overlong user hashes are not
representable. Execution recomputes these names from the basis and never reads
a later ambient user identity.

The provider-final identity slice is established before represented admission
by a private API-side no-enqueue canonicalizer. Authentication middleware has
already produced `auth_user`; the canonicalizer and `prepare_request_async()`
must call one extracted resolver with identical semantics: a nonnull
`auth_user` replaces both submitted values with its exact ID/name, while the
legacy no-auth case uses the submitted pair. The canonicalizer creates no API
request row, queue entry, action, coverage row, or provider effect and returns
only the closed bounded proof below. It is authorized only for the fenced Serve
controller preparation path. The proof is bound to the complete logical
resource identity, so it cannot be reused for another replica incarnation or
generation. A malformed, mismatched, missing, or unavailable proof is
`unfrozen_identity` and cannot enter `ACTION_ACTIVE` or represented shadow
admission.

In shadow, the controller later submits the object-identical pre-auth
`PreparedLaunchRequest` to legacy `/launch`. When the real request ID is bound,
the typed binder locks the already-created child and that exact API request row,
decodes its effective persisted `LaunchBody`, and in the same transaction
compares `SKYPILOT_USER`/`SKYPILOT_USER_ID` with the proof's effective pair.
Equality writes `REQUEST_BOUND` with no identity divergence. Mismatch writes
`REQUEST_BOUND` plus write-once `IDENTITY_MISMATCH`; completion may preserve but
never clear or replace that divergence, and the parent finalizes as promotion-
blocking. It does not rewrite the immutable `REPRESENTABLE` coverage row or
stop the already-authoritative legacy owner. The authoritative private
resource-action request does not carry a legacy `LaunchBody`; its handler
reprojects identity solely from the immutable action spec and never reads its
current request environment or authenticated actor as provider input.

Once a correct-kind, unowned real request row is found, missing, malformed, or
invalid identity fields are the mismatch branch and the ID is still atomically
bound; validation failure cannot roll the child back to an ambiguous
`PRE_SUBMIT` after SDK admission. A missing row, wrong request kind/action
correlation, or request ID already owned elsewhere remains a request-
association conflict and never fabricates equality or an alternate ID.

The first topology contains exactly the SSH Service named
`workload_name + "-ssh"`, the Service named `workload_name`, and that head Pod,
in the one canonical serialized/create order
`head_ssh_service, head_service, head_pod`. Every object plan's sequence is
fixed to that mapping; partial and resolved targets, observations, and create
effect traces use the same role-keyed order. Delete emission alone uses the
separate frozen order `head_service, head_ssh_service, head_pod` without
reordering stored evidence. `resources_ports` contains exactly the one
SkyServe application port, byte-equal to `application_port`, and the frozen
Kubernetes port mode is `podip`; `open_ports()` therefore creates no per-port
Service, LoadBalancer, or Ingress. A zero-port profile is not usable for a
normal non-pool SkyServe endpoint. Namespace, authorization/admission/network
policy, and service-account prerequisites must already exist and are read-only
during the action. Worker Pods, workload Deployments, persistent claims,
generic bootstrap mutation, and every extra Kubernetes object are not
representable. The one workload Pod uses the digest-pinned prebooted
Ray/Skylet runtime and accepts exactly one idempotent job RPC after startup.
Each of the three mutable objects carries its role-specific exact final label
map, including the display-cluster label `skypilot-cluster-name`,
`skypilot.co/cluster-record-uuid`, and
`skypilot.co/serve-replica-incarnation`. Source, policy, resource, and
`custom_metadata` inputs that contain either reserved identity label are
rejected before the system labels are injected after all allowed merges.
The three complete maps are not byte-equal: the Pod has its fixed head and
component labels while the two Services have their fixed Service labels. Each
map is independently sorted by unique key. The display-label value is the
provider cluster name (the workload name without its terminal `-head`); it and
the values of the two reserved identity labels are byte-equal across all three
maps, and the two reserved values are canonical UUID text. For each role, the topology label
bytes equal that role's request-body `metadata.labels`; every
`required_identity_labels` map is the exact shared three-label subset, not a
replacement for the role-specific final map.
Create, adopt, observe, and delete validate every recorded object, label, and
normalized semantic spec; the Pod remains the primary resolved target.

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
`skypilot.co/serve-replica-incarnation` on every mutable object. Both
UUID values fit Kubernetes label syntax. Create/adopt/read/delete paths select
and validate the display-name label plus both identity labels; every existing
object must agree before adoption. The selected v1 topology has only the head
Pod and its two Services, and all three receive and validate the identity;
validating the Pod alone is insufficient. A same-name object with missing or
different labels is a conflict and is never deleted. Deletes use the observed
UID as a Kubernetes precondition after label validation. The workload UID is
resolved-resource evidence, not a provider operation ID, so a null operation
ID remains valid.

The bounded write-once evidence is:

```text
ProviderKubernetesResolvedObjectV1 = {
  role: "head_pod" | "head_service" | "head_ssh_service",
  kind: "Pod" | "Service",
  namespace: Text,
  name: Text,
  uid: Text,
  observed_semantic_sha256: Sha256,
  server_allocations: [ProviderKubernetesServerAllocationV1]
}

ProviderKubernetesResolvedObjectSlotV1 = {
  sequence: 0 | 1 | 2,
  role: "head_ssh_service" | "head_service" | "head_pod",
  disposition: "unknown" | "committed",
  object: null | ProviderKubernetesResolvedObjectV1
}

PartialResolvedProviderTargetV1 = {
  version: 1,
  requested_target_sha256: Sha256,
  kubernetes_objects: [ProviderKubernetesResolvedObjectSlotV1]  # exactly 3
}

ResolvedProviderTargetV1 = {
  version: 1,
  requested_target_sha256: Sha256,
  provider_resource_id: null | Text,
  workload_uid: null | Text,
  kubernetes_objects: [ProviderKubernetesResolvedObjectV1],
  provider_operation_id: null | Text,
  resolved_at: UtcTimestamp
}

ProviderKubernetesCleanupObjectV1 = {
  sequence: 0 | 1 | 2,
  role: "head_ssh_service" | "head_service" | "head_pod",
  plan: ProviderKubernetesObjectPlanV1,
  committed_uid: null | Text,
  committed_server_allocations: [ProviderKubernetesServerAllocationV1]
}

ProviderKubernetesCleanupTargetV1 = {
  version: 1,
  basis_kind: "completed_launch" | "partial_launch_cleanup",
  requested_target_sha256: Sha256,
  cluster_name: Text,
  cluster_record_uuid: UUID,
  objects: [ProviderKubernetesCleanupObjectV1],  # exactly 3
  cluster_row_disposition: "exact_handle" | "not_found",
  handle: null | ProviderKubernetesHandleV1,
  observed_at: UtcTimestamp
}
```

The cleanup target is the complete immutable down-addressing preimage. Its
three entries follow `ProviderKubernetesObjectRoleMapV1` and embed every exact
nonsecret object plan, including requested semantic bytes. A completed basis
has all three committed UIDs/allocations and an exact handle. A partial basis
retains every commitment from the launch cursor and uses explicit nulls for
unknown UIDs; it may carry either a byte-equal same-UUID handle or an exact
cluster-row NotFound. The handle/disposition pair is exact. A same-name row
with a null/different UUID or different provider block is conflict, never
`not_found`. Typed admission recomputes the target hash and byte-compares it to
the retained launch evidence and current same-UUID cluster-row read.

The profile defines which resolved fields are required for authoritative
present/absence proof. A caller loads this object from prior attempt evidence
and passes it to later `observe()` calls. A launch plan starts with
`prior_cleanup_target=null`; discovery is written to the attempt, leaving that
plan immutable. Down admission derives and stores exactly one matching launch
cleanup target, complete or partial, in the new down plan. A conflicting
second value is corruption and cannot replace the first.

For `pod_cluster_v1`, every resolved or partial object collection contains
exactly three role-keyed entries in canonical order
`head_ssh_service, head_service, head_pod`; sequence and role must match that
mapping. A partial target uses an explicit `unknown`/null slot rather than a
sparse or differently ordered list, and committed slots form a prefix of the
create order. A full target has all three committed objects, and `workload_uid`
equals the head-Pod entry. Every later present/adopt/delete check requires all
known write-once UIDs as well as the identity labels and semantic hashes; a
replacement Service cannot be adopted merely by copying labels. After each
role is created or exact-read, its slot is changed once from `unknown` to
`committed` in the claim-fenced API006 progress snapshot before the next
mutation. `submit()` and `observe()`
must consume that partial value and reject a different UID/spec/allocation. On
the first lost-ack 409 where no UID was yet committed, exact labels and semantic
spec permit the observed UID to become the one write-once commitment; a later
409 must match it. `resolved_target` remains null until all three are present
and then is written once as a complete object. `pod_cluster_v1` is authoritative only when the extracted launch/down
seam can propagate and later query these identities without guessing. A name-only
pre-existing replica remains shadow until replaced by a natively identified
generation.

The API006 attempt snapshot carries the bounded recovery cursor:

```text
ProviderKubernetesHandleV1 = {
  version: 1,
  cluster_record_uuid: UUID,
  cluster_name: Text,
  cluster_name_on_cloud: Text,
  requested_target_sha256: Sha256,
  launched_resources_sha256: Sha256,
  provider_config: {
    context_mode: "in_cluster",
    scope_sha256: Sha256,
    namespace: Text,
    port_mode: "podip",
    use_internal_ips: true,
    application_port: DecimalPortText,
    pod_name: Text,
    pod_uid: Text,
    node_name: Text,
    pod_ip: Text,
    head_service_uid: Text,
    head_ssh_service_uid: Text,
    ambient_fallback: false
  },
  provider_config_sha256: Sha256
}

ProviderSkyletJobEvidenceV1 = {
  protocol: "skylet_idempotent_submit_v1",
  submission_key: UUID,  # enclosing launch action ID
  job_contract_sha256: Sha256,
  job_spec_sha256: Sha256,
  retained_submit_request: null | ProviderSkyletSubmitRequestV1,
  state_store_uuid: UUID,
  read_disposition: "present" | "not_found" | "conflict" | "uncertain",
  durable_state: null | "COMMITTED_PENDING_START" | "START_INTENT" |
                 "START_COMMITTED" | "RUNNING" | "RECOVERY_PENDING" | "SUCCEEDED" |
                 "FAILED" | "BLOCKED",
  job_id: null | PositiveInteger,
  run_epoch: null | NonnegativeInteger,
  record_revision: null | PositiveInteger,
  observed_at: UtcTimestamp
}

ProviderLifecycleProgressV1 = {
  version: 1,
  cursor: ProviderLifecycleCursorV1,
  worker_attestation: null | ProviderAuthorityWorkerAttemptAttestationV1
}

ProviderLifecycleCursorV1 = ProviderLaunchProgressV1 | ProviderDownProgressV1

ProviderLaunchProgressV1 = one of:
  {version: 1, action_kind: "launch", phase: "CREATE_INTENT",
   role: ProviderObjectRoleV1,
   intent_origin: ProviderLaunchEffectClaimV1,
   committed_effects: [ProviderLaunchCommittedEffectEvidenceV1],
   known_objects: PartialResolvedProviderTargetV1,
   pre_observation: ProviderLifecycleObservationV1}
  {version: 1, action_kind: "launch", phase: "OBJECTS_PARTIAL",
   committed_effects: [ProviderLaunchCommittedEffectEvidenceV1],
   known_objects: PartialResolvedProviderTargetV1,
   post_observation: ProviderLifecycleObservationV1}
  {version: 1, action_kind: "launch", phase: "OBJECTS_EXACT",
   committed_effects: [ProviderLaunchCommittedEffectEvidenceV1],
   resolved_target: ResolvedProviderTargetV1,
   post_observation: ProviderLifecycleObservationV1}
  {version: 1, action_kind: "launch", phase: "HANDLE_INTENT",
   intent_origin: ProviderLaunchEffectClaimV1,
   committed_effects: [ProviderLaunchCommittedEffectEvidenceV1],
   resolved_target: ResolvedProviderTargetV1,
   intended_handle: ProviderKubernetesHandleV1}
  {version: 1, action_kind: "launch", phase: "HANDLE_COMMITTED",
   committed_effects: [ProviderLaunchCommittedEffectEvidenceV1],
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1}
  {version: 1, action_kind: "launch", phase: "RUNTIME_READY",
   committed_effects: [ProviderLaunchCommittedEffectEvidenceV1],
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1,
   runtime_evidence: ProviderKubernetesRuntimeEvidenceV1}
  {version: 1, action_kind: "launch", phase: "JOB_INTENT",
   intent_origin: ProviderLaunchEffectClaimV1,
   committed_effects: [ProviderLaunchCommittedEffectEvidenceV1],
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1,
   runtime_evidence: ProviderKubernetesRuntimeEvidenceV1,
   submit_request: ProviderSkyletSubmitRequestV1}
  {version: 1, action_kind: "launch", phase: "JOB_COMMITTED",
   committed_effects: [ProviderLaunchCommittedEffectEvidenceV1],
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1,
   runtime_evidence: ProviderKubernetesRuntimeEvidenceV1,
   job: ProviderSkyletJobEvidenceV1}
  {version: 1, action_kind: "launch", phase: "JOB_RUNNING",
   committed_effects: [ProviderLaunchCommittedEffectEvidenceV1],
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1,
   runtime_evidence: ProviderKubernetesRuntimeEvidenceV1,
   job: ProviderSkyletJobEvidenceV1}
  {version: 1, action_kind: "launch", phase: "ENDPOINT_RESOLVED",
   committed_effects: [ProviderLaunchCommittedEffectEvidenceV1],
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1,
   runtime_evidence: ProviderKubernetesRuntimeEvidenceV1,
   job: ProviderSkyletJobEvidenceV1,
   endpoint: ProviderKubernetesEndpointEvidenceV1}
  {version: 1, action_kind: "launch", phase: "SUCCEEDED",
   committed_effects: [ProviderLaunchCommittedEffectEvidenceV1],
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1,
   runtime_evidence: ProviderKubernetesRuntimeEvidenceV1,
   job: ProviderSkyletJobEvidenceV1,
   endpoint: ProviderKubernetesEndpointEvidenceV1,
   success_observation: ProviderLifecycleObservationV1}

ProviderKubernetesDeleteObjectV1 = {
  plan_sequence: 0 | 1 | 2,
  role: ProviderObjectRoleV1,
  expected_uid: null | Text,
  state: "present_exact" | "absent_exact",
  requested_semantic_sha256: Sha256
}

ProviderKubernetesDeleteTargetV1 = {
  version: 1,
  requested_target_sha256: Sha256,
  prior_launch_basis_sha256: Sha256,
  objects: [ProviderKubernetesDeleteObjectV1],  # exactly 3 canonical roles
  observation: ProviderLifecycleObservationV1
}

ProviderClusterRecordRemovalEvidenceV1 = {
  version: 1,
  cluster_name: Text,
  expected_cluster_record_uuid: UUID,
  disposition: "removed_exact" | "already_absent",
  removed_handle: null | ProviderKubernetesHandleV1,
  removed_handle_sha256: null | Sha256,
  observed_at: UtcTimestamp
}

ProviderDownProgressV1 = one of:
  {version: 1, action_kind: "down", phase: "TARGET_RESOLVED",
   delete_target: ProviderKubernetesDeleteTargetV1}
  {version: 1, action_kind: "down", phase: "DELETE_INTENT",
   role: ProviderObjectRoleV1,
   delete_target: ProviderKubernetesDeleteTargetV1}
  {version: 1, action_kind: "down", phase: "DELETE_PARTIAL",
   delete_target: ProviderKubernetesDeleteTargetV1}
  {version: 1, action_kind: "down", phase: "ABSENCE_EXACT",
   delete_target: ProviderKubernetesDeleteTargetV1,
   absence_observation: ProviderLifecycleObservationV1}
  {version: 1, action_kind: "down", phase: "HANDLE_REMOVE_INTENT",
   delete_target: ProviderKubernetesDeleteTargetV1,
   absence_observation: ProviderLifecycleObservationV1,
   expected_handle: null | ProviderKubernetesHandleV1}
  {version: 1, action_kind: "down", phase: "HANDLE_REMOVED",
   delete_target: ProviderKubernetesDeleteTargetV1,
   absence_observation: ProviderLifecycleObservationV1,
   handle_removal: ProviderClusterRecordRemovalEvidenceV1}
  {version: 1, action_kind: "down", phase: "SUCCEEDED",
   delete_target: ProviderKubernetesDeleteTargetV1,
   absence_observation: ProviderLifecycleObservationV1,
   handle_removal: ProviderClusterRecordRemovalEvidenceV1}
```

`ProviderKubernetesHandleV1.cluster_name_on_cloud` is byte-equal to the
enclosing requested target's nonnull
`kubernetes.provider_cluster_name`; handle construction and refresh do not
derive it from ambient user state. Its `provider_config.pod_ip` is canonical,
zone-free IPv4 or IPv6 text under the same checked-in
`str(ipaddress.ip_address(value)) == value` rule used by server allocations.
Arbitrary text, alternate IP spelling, and a zone identifier are not
representable.

In `ProviderSkyletJobEvidenceV1`, the top-level submission key and two job
hashes are always the immutable expected-query basis supplied by the session
comparator. `retained_submit_request` is nonnull exactly for `present` and
`conflict`, is reconstructed from one typed Skylet record, and has the same
submission key. It is canonical-byte-equal to the expected request only for
`present` and canonical-byte-unequal only for `conflict`; the contextual session
comparator owns that comparison. For both `present` and `conflict`, durable
state, job ID, run epoch, and record revision are nonnull values read from that
same record. For `not_found` and `uncertain`, the retained request and every
response-derived durable state, job ID, run epoch, and record revision are null.
The state-store UUID and observed timestamp remain nonnull in every disposition
because they describe the exact queried store/read. A leaf validates this
disposition/nullability matrix and the retained request's internal hashes but
cannot grant present/conflict authority without the expected full request and
enclosing action. A no-effect proof separately narrows a conflicting record to
the terminal durable states displayed in its matrix; the generic evidence leaf
does not silently apply that proof-only restriction.

Unknown keys are forbidden in every variant. Launch normally alternates
`CREATE_INTENT(role) -> OBJECTS_PARTIAL` in canonical create order. The head-Pod
edge may instead be `CREATE_INTENT(head_pod) -> OBJECTS_EXACT` when its first
exact readback already contains the write-once scheduler `nodeName`; otherwise
it reaches `OBJECTS_PARTIAL` with three slots and later `OBJECTS_EXACT`. Launch
then follows `OBJECTS_EXACT -> HANDLE_INTENT -> HANDLE_COMMITTED -> RUNTIME_READY ->
JOB_INTENT -> JOB_COMMITTED -> JOB_RUNNING -> ENDPOINT_RESOLVED -> SUCCEEDED`.
`CREATE_INTENT.role` is exactly the first unknown role. `OBJECTS_PARTIAL` has
one to three committed slots; three is permitted while scheduler `nodeName` is
still absent. `OBJECTS_EXACT` requires three UIDs, all required server
allocations, and authoritative exact semantic readback. `HANDLE_INTENT` freezes
the complete intended handle before cluster-row I/O. `JOB_COMMITTED` requires
the session-owned submit comparator to validate the reconstructed request and a
present, fsync-committed job whose key, contract hash, and spec hash equal that
result, with a nonnull job ID;
`JOB_RUNNING` additionally requires its exact durable state to be `RUNNING`.
Launch `SUCCEEDED` retains every proof and an authoritative `present`
observation.

Every launch variant carries the exact `committed_effects` list selected by the
literal phase table above. Records are ordered by strictly contiguous ascending
`effect_sequence`; no sparse, duplicate, out-of-order, or extra record is
valid. A record is appended only by the same claim-fenced API006 transaction
that advances the current intent to its post-effect phase. Every prior record
is byte-equal forever. The record's `intent_origin` equals the current intent
cursor's immutable origin, and its `evidence_commit_origin` equals the live
claim's immutable origin snapshot and complete content-addressed attestation
provenance. Their lexicographic
attempt/generation ordering follows the rule above. A newer attempt or request
generation can exact-adopt and append later evidence but cannot relabel an
earlier intent or committed record.

For `C0` through `C2`, sequence, role, request-body hash, and requested-semantic
hash equal the corresponding full immutable object plan, and
`object_at_commit` equals that role's committed slot in UID, admitted semantic
hash, and then-known allocations. Both Service records have their complete
immutable allocation quartet. The Pod record may initially omit only the
scheduler-owned `/spec/nodeName`; later progress may append exactly that
allocation to the same-UID current object without changing `C2`; when the first
readback already contains it, `object_at_commit` includes it and the cursor
takes the direct `OBJECTS_EXACT` edge. `C3` retains the byte-equal complete
intended handle and its hash, and its disposition is the exact insert/adopt
transaction result. For `C4`, the session-owned comparator reconstructs the
full submit request from the immutable spec; `submit_request_sha256` equals its
result, and `job_at_commit` is the exact fsync-committed job read whose key and
hashes equal that reconstruction. None of these records is reconstructed later
from a resource name, authority identity, or digest alone.

The evidence disposition is also provenance, not an advisory diagnostic.
CoreV1 `created`, cluster-row `inserted`, and Skylet `submitted` require the
evidence-commit origin to be byte-equal to the intent origin and require that
same claim's corresponding synchronous success result. `adopted_exact` means the
evidence checkpoint came from qualified exact readback of already-committed
state, including 409/lost-ack recovery; it is mandatory whenever the evidence-
commit origin is a later attempt or generation and is also legal after 409 or
readback in the same byte-equal claim. A disposition/origin/response/readback
mismatch is invalid.

Progress is never validated as a free-standing union. The typed API006 store
receives the exact immutable action ID, kind, plan, spec hash, and (for down)
prior-launch basis. It requires `cursor.action_kind` to equal the action kind;
every requested-target, cleanup-target, prior-basis, resource, cluster-record,
service/replica-incarnation, and nested object hash/identity to equal the
applicable frozen preimage. For every `JOB_INTENT` and every later job-bearing
cursor/effect/proof, it calls
`KubernetesResourceActionSession.validate_skylet_submit_binding_v1()` against
that same immutable spec and rejects any unequal request, action key, contract
hash, spec, spec hash, or submit-request hash. All repeated resolved targets,
handles, runtime records, and endpoint or observation targets are mutually
byte-equal. Repeated job evidence key, hashes, job ID, and state-store UUID are
byte-equal; every `present` retained submit request is byte-equal to
`job_at_commit`, while a typed conflict retains its byte-unequal full request.
Only the closed monotonic job state/revision/run-epoch transitions may differ
from `job_at_commit`. For
down, `delete_target.observation`, each later `absence_observation`, and the final
handle-removal proof describe that same three-role target without a present/
absent contradiction. A structurally valid cursor for another action, plan, or
incarnation is corruption and cannot be inherited, reduced, or used for I/O.

Down follows `TARGET_RESOLVED -> (DELETE_INTENT(role) -> DELETE_PARTIAL)* ->
ABSENCE_EXACT -> HANDLE_REMOVE_INTENT -> HANDLE_REMOVED -> SUCCEEDED`.
Target resolution exact-reads all three frozen names and may extend only an
unknown UID from a partial-launch basis. `DELETE_INTENT.role` is the first
`present_exact` role in the literal delete order. Each delete-target object's
`plan_sequence` remains its canonical role-map index; it is never overloaded
with emission order, which is obtained only from the role map's
`delete_sequence`. UID commitments remain in the
role entry after it becomes `absent_exact`; all three must be absent and the
observation authoritative before `ABSENCE_EXACT`. `already_absent` means an
exact no-row result under the cluster resource lock after
`HANDLE_REMOVE_INTENT`, never a differently identified same-name row.

Each next-effect intent and each resulting evidence checkpoint is a
claim-fenced API006 commit. The cursor may only remain at a read-only
observation point or take one listed edge. UIDs, semantic hashes, allocations,
resolved target, intended/committed handle, job identity, and absence evidence
cannot be erased or changed; only the permitted unknown-to-committed and
allocation-append transitions exist. Provider failure does not invent a
`BLOCKED` cursor phase: the typed attempt/action outcome blocks while retaining
the last safe cursor. The envelope's worker attestation is attempt-scoped. A
newer fenced request execution generation may replace it; within one generation
only the write-once `after: null -> exact identity` completion is legal. A
carried cursor can have null attestation before the new request is claimed, but
every pre-I/O intent and post-I/O evidence commit requires the current exact
attestation and its per-effect revalidation. This snapshot is a recovery
cursor, not a queue, lease, or generic child workflow.

On every attempt, `submit()` and `observe()` receive the complete immutable
spec plus only the current attempt's persisted progress. It is null only for
the exact `provider_io_boundary='NOT_STARTED'`, revision-zero fresh-cursor
shape, including an `n+1` materialized from a proved pre-I/O predecessor.
Otherwise a retry receives its already-carried
`ProviderLifecycleProgressV1`. They never scan arbitrary prior attempts,
reconstruct a cursor from object names, or accept a caller-supplied cursor.
Attempt materialization byte-copies a settled predecessor's nonnull cursor,
sets `worker_attestation=null`, recomputes the envelope hash, and starts local
progress revision one; its typed validator separately proves the copied cursor
preserves every immutable predecessor commitment before any new attestation or
effect. In particular, it copies `committed_effects` exactly; retry
materialization cannot truncate, normalize, regenerate, or rehash that list,
and an inherited intent retains its byte-equal `intent_origin`. It clears only
the envelope's attempt-scoped worker attestation. A successor claim may
exact-adopt evidence under its own evidence-commit origin, but cannot replace
the inherited intent origin or produce `call_not_entered`/definitive-no-effect
under its newer claim.

Materialization also requires the predecessor attempt to be strictly below the
parent's `RESOURCE_ACTION_MAX_ATTEMPT_V1`. At the maximum, the parent's exact
attempt-domain exhaustion reduction persists the otherwise-retrying `R`/`U`/
`P0`/`O` outcome, blocks without a deadline, and creates no request or attempt
max-plus-one. Provider code never widens, wraps, or resets that counter.

### API006 representability gate

Provider-authoritative admission runs a pure size enumerator before creating a
request or permitting provider I/O. The immediate pre-I/O check reruns that
same versioned enumerator before the first intent/watermark commit. It uses the
exact frozen launch/cohort preimages plus every exact live registered worker
identity and claim/attempt-attestation preimage eligible at that check; only
still-unknown response-derived leaves and the reachable five-effect origin
schedule are maximized. It then canonical-renders every v1 launch progress
shape, every handler no-effect
resolution/return shape, and every reducer-built quiescence shape. Every
rendered object, not merely its PostgreSQL JSONB stored rendering, must be at
most 65,536 canonical UTF-8 bytes. Any leaf still lacking a finite protocol
bound makes the candidate unrepresentable. Oversize or unbounded candidates
remain legacy/shadow (or block an already materialized dark action) before any
provider-I/O watermark or intent is committed; runtime truncation, origin
elision, hash-only substitution, and late terminal-result dropping are
forbidden.

The checked-in golden fixture manifest has both `realistic` and
`candidate_maximal` members for each of these exact cases:

- all phase-table rows: three `CREATE_INTENT` roles, one/two/three-slot
  `OBJECTS_PARTIAL`, `OBJECTS_EXACT` through both Pod edges, `HANDLE_INTENT`,
  `HANDLE_COMMITTED`, `RUNTIME_READY`, `JOB_INTENT`, `JOB_COMMITTED`,
  `JOB_RUNNING`, `ENDPOINT_RESOLVED`, and `SUCCEEDED`;
- `call_not_entered` for sequences 0-4; CoreV1 422 for failing sequences 0-2;
  cluster-row `rolled_back/not_found` and the one typed different-UUID conflict;
  Skylet `schema_rejected` plus same-key conflict in each allowed terminal job
  state;
- reducer quiescence for every nonsuccessful phase-table row, including every
  E-only post-effect/read-only row and every legal `E* + N<i>` intent row, plus
  explicit rejection of `SUCCEEDED`, E-only-with-N, intent-without-N, and
  wrong-claim N; same-claim created/submitted/inserted commits, same-claim
  adoption, later-generation adoption, generation reset across attempts, and
  rejection of a retry-local no-effect resolution for an inherited intent;
- the parent's complete handler/outcome cross-field cases: domain success `S`;
  every revision-zero, nonintent, and current-intent error-category mapping to
  `R`/`U`/`B`, including maximal bounded code/message/retry leaves and every
  invalid cross-combination; supersession `Q` for E-only and E+N; all three
  unmaterialized, terminal-request-unsettled, and retained-settled direct
  `CANCELLED_NO_EFFECT` bases, including retained request present/GC; request-
  terminal fallback `P0`, `O`, external-cursor `S`, and corruption `X`, for
  each compatible terminal request state and missing/invalid-return reason.
  Direct fixtures cover empty, one-link, and maximum-integer-count rolling no-
  I/O prefixes, immutable historical attempt outcomes, immediate-link tamper,
  inherited cursor, and crossed-predecessor rejection. Maximum-attempt fixtures
  also cover the parent's exact exhaustion reduction for handler `R`/`U` and
  fallback `P0`/`O`, including no max-plus-one request and direct-teardown
  precedence.

Each fixture records case ID, payload kind, canonical byte count, and SHA-256;
the `candidate_maximal` member is not a synthetic protocol-wide fill of every
`Text` leaf. It keeps the selected frozen spec/cohort and complete live
registered worker/attempt-attestation preimages byte-exact, substitutes declared
maxima only for runtime response values not known at admission, and uses the
maximum number of distinct full intent/evidence claims reachable in five
effects. CI requires both members
to remain at or below 65,536 bytes and checks the committed golden hashes. A
failing realistic or candidate-maximal fixture keeps provider authority
disabled and requires this design to deduplicate provenance or tighten an
explicit leaf bound before implementation; passing is not assumed. The
companion's request-return envelope and final action outcome are
included in the terminal/quiescence measurements, so fitting the cursor alone
does not pass this gate.

## Submission journal

The action-attempt row has a closed request lifecycle plus a retained provider-
I/O watermark:

```text
mutation_boundary:
  NOT_STARTED -> INTENT_COMMITTED -> SUBMITTED_OR_AMBIGUOUS -> SETTLED
provider_io_boundary:
  NOT_STARTED -> INTENT_COMMITTED -> SUBMITTED_OR_AMBIGUOUS
```

Before settlement the fields are equal. Settlement changes only
`mutation_boundary`; it never erases `provider_io_boundary`.

Before mutating provider bytes may be sent, the request handler locks action, predecessor
attempt when present, current attempt, and correlated request in that order.
Attempts are locked in increasing number. After any wait it uses a fresh database
clock to revalidate the exact correlation, `RUNNING` state, execution
generation, claim token, worker, current owner fence, unexpired lease, exact
plan hash, and attempt identity. An exact `NOT_STARTED`/null/revision-zero
attempt, whether initial or a pre-I/O retry successor, commits
`INTENT_COMMITTED` to both boundary fields and the first legal nonnull progress
cursor in that same transaction: launch uses
`CREATE_INTENT(first_missing_role)` with the current immutable
`ProviderLaunchEffectClaimV1` origin, and down uses `TARGET_RESOLVED`. An exact
`NOT_STARTED` inherited revision-one seed instead atomically commits
`INTENT_COMMITTED` to both fields and binds its current worker attestation to
the already-carried, predecessor-validated cursor. An authoritative attempt
therefore cannot have `provider_io_boundary != NOT_STARTED` with null API006
progress. A request is
never locked before its action. The handler then performs one bounded
fixed-topology session mutation group. The first effect uses the combined
boundary/intent write above; before each later CoreV1 or Skylet effect it
commits the corresponding monotonic
`ProviderLifecycleProgressV1` intent and its current claim origin; after an
exact readback it commits the resulting UID/spec/allocation/handle/job record,
its exact disposition, and the current evidence-commit origin before the next
effect. A
worker replacement consumes that snapshot rather than reconstructing partial
commitments from names or process memory.

`ProviderSubmissionV1` is:

```text
{
  disposition: "not_submitted" | "accepted" | "ambiguous",
  provider_operation_id: null | Text,
  normalized_response_sha256: null | Sha256,
  normalized_error: null | ProviderErrorV1
}
```

For `pod_cluster_v1`, `normalized_response_sha256` is always null. A successful
Kubernetes call establishes only `accepted`; provider success, exact object
UIDs, and readiness come exclusively from the typed observation below. The
profile exposes no required provider operation ID, so
`provider_operation_id` may also be null. A nonnull response hash is rejected
rather than accepted as an opaque provider preimage.

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
`unknown`. The complete normalized error is retained in the parent handler DTO
and byte-binds its provider tuple. For revision-zero or nonintent journals,
`transient`, `capacity`, `quota`, and `rate_limited` map to parent tuple `R`
with the same retry class and
`D=min(retry_after_seconds if nonnull else 60, 3600)`; `unknown` maps to `U`;
and `invalid_request`, `permission`, and `conflict` map to `B`. At a current
launch or down intent, the four retry categories and `unknown` map
conservatively to `U`, because an exception cannot resolve the entrant, while
the three invalid/permission/conflict categories map to `B`. Code/message
leaves are copied exactly after bounded redaction. Below the parent's attempt
maximum, `R` retries at `D`, `U` observes at 60 seconds, and `B` blocks while
retaining the Serve projection; none is a handler-domain terminal action. At
the maximum, the parent preserves `R`/`U` but applies its one typed exhaustion
override instead of constructing an unrepresentable next attempt. Success `S`
and supersession `Q` have null normalized error. Any other disposition/
certainty/retry/observation/error combination is invalid. The provider facet
does not choose a different retry, block, terminal, or replan policy.

## Observation contract

```text
ProviderKubernetesObjectEvidenceV1 = {
  role: "head_pod" | "head_service" | "head_ssh_service",
  api_version: "v1",
  kind: "Pod" | "Service",
  namespace: Text,
  name: Text,
  query_mode: "exact_name_get_then_validate_labels",
  read_disposition: "present" | "not_found" | "uncertain",
  uid: null | Text,
  cluster_name_label: null | Text,
  cluster_record_uuid_label: null | UUID,
  replica_incarnation_label: null | UUID,
  requested_semantic_sha256: Sha256,
  normalized_observed_semantic: null | CanonicalJsonObject,
  observed_semantic_sha256: null | Sha256,
  spec_match: null | Boolean,
  server_allocations: [ProviderKubernetesServerAllocationV1],
  deletion_timestamp: null | UtcTimestamp,
  pod_phase: null | "Pending" | "Running" | "Succeeded" | "Failed" |
             "Unknown",
  ready: null | Boolean
}

ProviderPodObservationEvidenceV1 = {
  version: 1,
  source: "core_v1_exact_get_same_live_client",
  frozen_scope: ProviderKubernetesScopeV1,
  observed_scope_before: ProviderKubernetesScopeReadV1,
  observed_scope_after: ProviderKubernetesScopeReadV1,
  objects: [ProviderKubernetesObjectEvidenceV1]
}

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
  evidence: ProviderPodObservationEvidenceV1,
  evidence_sha256: Sha256,
  observed_at: UtcTimestamp
}

ProviderKubernetesRuntimeEvidenceV1 = {
  version: 1,
  pod_uid: Text,
  container_name: "ray-node",
  requested_image: ProviderOCIImageQualificationV1,
  observed_runtime_image: ProviderRuntimeImageIdentityV1,
  container_started: true,
  startup_probe_succeeded: true,
  runtime_contract_sha256: Sha256,
  artifact_measurements: [ProviderRuntimeArtifactMeasurementV1],
  ray_health: "ready",
  skylet_health: "ready",
  skylet_state_store_uuid: UUID,
  observed_at: UtcTimestamp
}

ProviderKubernetesEndpointEvidenceV1 = {
  version: 1,
  pod_uid: Text,
  pod_ip: Text,
  application_port: DecimalPortText,
  provider_config_sha256: Sha256,
  resolution: "exact_handle_podip",
  observed_at: UtcTimestamp
}
```

`objects` has exactly the three topology entries in fixed role order; each
namespace/name/kind and requested hash must equal its object plan.
`normalized_observed_semantic` is the complete bounded nonsecret projection
produced by `kubernetes_admitted_object_v1`; its hash, `spec_match`, and typed
server allocations are recomputed on read. `evidence_sha256` is recomputed from
the embedded evidence's canonical bytes. The top-level identity, state,
readiness, resolved-target, and observed fields are derived from evidence and
cannot contradict it. Raw Kubernetes response bodies and arbitrary metadata
are never persisted.

`present` is authoritative only when the frozen requested cluster
UUID/incarnation and all three exact-name objects are present with matching
final labels, exact normalized semantic specs, and admitted server allocations;
every known partial/resolved object UID must also match. `absent` is authoritative only
when all three exact reads return Kubernetes NotFound and the before/after
scope reads are `complete` and byte-equal to the frozen scope. A same-name
different incarnation/cluster UUID, different known UID, or semantic-spec
mismatch is `conflict`. Forbidden responses, timeouts, partial
reads, stale caches, missing identity, and scope read failures are `uncertain`
or `conflict` according to the typed evidence; they cannot manufacture
absence.

Eventually consistent absence cannot terminalize down by itself. The facet may
return it as diagnostic evidence and the Serve reducer schedules another
observation.

Runtime evidence is bound to retrievable preimages. `runtime_artifacts` has
exactly the six `ProviderWorkloadArtifactBindingV1` roles in the declared enum
order. Each source manifest's canonical bytes enumerate sorted relative paths,
byte sizes, executable bits, and file SHA-256 values; the expected tree hash is
recomputed from that manifest. The requested qualification's manifest digest
must equal `ProviderPodImageV1.qualification.oci_manifest_digest`, and every
artifact binding's `workload_image_digest` equals that same manifest digest.
The requested reference/config/qualification artifact are byte-equal to the
frozen Pod image qualification. The raw CRI image ID is
parsed under its declared runtime scheme, its digest must equal the qualified
OCI config digest, and the retrievable qualification artifact must map that
config digest back to the requested manifest digest. The raw CRI digest is
never compared directly to the manifest digest. Every measurement must equal
its complete binding, and
`runtime_contract_sha256` is the hash of the ordered bindings. A health response
with merely well-formed arbitrary hashes is invalid. The startup probe and
session independently verify the same measurements, Ray/Skylet health, and
Skylet state-store UUID.

The job contract is similarly closed. Its checked-in JSON Schema and renderer
are the only accepted path from the retained, allowlisted canary source to
`ProviderSkyletJobSpecV1`; the retained source hash must be the schema
artifact's one allowlisted canary hash. Unknown fields or any arbitrary command,
argv, setup, workdir, environment, mount, or secret are not representable. The
launch action ID is absent from the immutable job spec to avoid an identity
cycle and appears only as the runtime submit request's `submission_key`.

## Launch behavior

For a fresh authoritative launch attempt:

1. validate the immutable plan, current request claim, and exact authority
   worker attestation;
2. exact-observe the first missing role, then atomically commit
   `INTENT_COMMITTED` plus `CREATE_INTENT(first_missing_role)`; send only its
   frozen create body, exact-read it, and commit the extended
   `OBJECTS_PARTIAL` target; repeat the intent/evidence pair for later roles.
   For the head Pod, commit `OBJECTS_EXACT` directly instead when that first
   exact readback already has `nodeName`;
3. only when the Pod readback lacked `nodeName`, wait for that write-once
   scheduler allocation, exact-read all three admitted specs/UIDs/allocations,
   and advance from three-slot `OBJECTS_PARTIAL` to `OBJECTS_EXACT`;
4. construct and commit the full intended handle, including that node name and
   same-UID Pod IP, at `HANDLE_INTENT`; exact-insert/adopt the same-UUID cluster
   row and commit `HANDLE_COMMITTED`;
5. verify the digest-pinned prebooted container's exact artifact measurements,
   startup probe, Ray/Skylet health, and state-store UUID, then commit
   `RUNTIME_READY`;
6. use `KubernetesResourceActionSession.validate_skylet_submit_binding_v1()` to
   reconstruct the closed `ProviderSkyletSubmitRequestV1` and its canonical
   hash from the immutable spec, commit that byte-equal request at `JOB_INTENT`,
   and send those exact bytes in the one keyed Skylet RPC;
7. exact-read the fsync-committed same-key job/outbox row and commit
   `JOB_COMMITTED`, then wait for exact `RUNNING` evidence and commit
   `JOB_RUNNING`;
8. resolve the one `podip` endpoint solely from the frozen handle and commit
   `ENDPOINT_RESOLVED`;
9. revalidate the complete launch-success proof and current request claim,
   commit the final no-effect `SUCCEEDED` progress snapshot, and only then
   return a typed success outcome to the action reducer.

Launch success means all three admitted objects are exact, the exact handle is
durable, the prebooted Ray/Skylet runtime is healthy, the same-spec job is
durably `RUNNING` under the action UUID, and the frozen endpoint resolves. Pod Ready by
itself is not success. SkyServe application `READY` remains a later Serve
readiness-probe decision; it must target that exact Pod UID/IP/port and is not
fabricated by the provider facet.

On recovery or an ambiguous submission, observation always precedes another
mutation. Exact `present` adopts the resource only when the profile also proves
the launch-readiness contract. Exact `absent` permits a retry only when the
Serve classifier marks the error retryable and the profile proves stable
idempotency or that no prior operation can still take effect. Recoverable
`uncertain` schedules another observation; identity `conflict` quarantines the
action.

For the fixed Kubernetes create group, exact object names plus the immutable
identity labels/spec are provider-side idempotency keys. A recovery exact-reads
each role through the same live client: a present object is adopted only when
its `kubernetes_admitted_object_v1` semantic bytes, identity labels, and any
recorded partial UID/allocations agree; an absent role may receive the exact
same create bytes, and overlapping creates converge to one name with qualified
409 readback. A partially present group therefore resumes only its absent roles in
fixed order;
it never creates a second logical object. Delete uses the recorded UID
precondition, so a repeated delete converges to exact NotFound and cannot remove
a same-name replacement.

The facet never creates a second resource merely because the original API
request lease expired.

The job submission has the same lost-ack property. Skylet's internal
`skylet_idempotent_submit_v1` API accepts the closed submit request, returns the
existing job ID only when the contract-hash scalar is equal and the reconstructed
complete submit request is byte-equal, rejects any difference, and supports
readback by key. The complete job-contract preimage remains in the execution
capsule; the submit protocol deliberately carries only its hash. Therefore the
session comparator proves that preimage, reconstructs the full request, and
compares its canonical bytes; hash equality alone is insufficient.
`JOB_INTENT` commits before the RPC. After a timeout or worker death, recovery
queries by key before any send. In one SQLite read transaction Skylet returns
the complete stored key, contract hash, job spec, and spec hash needed to
reconstruct `retained_submit_request`; the session comparator exact-compares it
to the immutable expected request. Byte-equal presence adopts, exact absence
permits the same keyed send, and any byte difference blocks—even if every hash
scalar is equal. The public/generic `backend.execute()` path has no authority
fallback.

The exact local durable records are:

```text
SkyletJobRecordV1 = {
  submission_key: UUID,
  job_contract_sha256: Sha256,
  job_spec: ProviderSkyletJobSpecV1,
  job_spec_sha256: Sha256,
  job_id: PositiveInteger,
  pod_uid: Text,
  state_store_uuid: UUID,
  state: "COMMITTED_PENDING_START" | "START_INTENT" | "START_COMMITTED" | "RUNNING" |
         "RECOVERY_PENDING" | "SUCCEEDED" | "FAILED" | "BLOCKED",
  run_epoch: NonnegativeInteger,
  run_token: null | UUID,
  skylet_process_epoch: null | UUID,
  process_pid: null | PositiveInteger,
  process_start_ticks: null | NonnegativeInteger,
  entrypoint_measurement_sha256: null | Sha256,
  revision: PositiveInteger
}

SkyletStartOutboxV1 = {
  job_id: PositiveInteger,
  run_epoch: NonnegativeInteger,
  run_token: null | UUID,
  state: "PENDING" | "DELIVERED"
}
```

Those four stored request fields are the complete bounded preimage of
`ProviderSkyletSubmitRequestV1`; readback reconstructs that typed request
rather than returning only hashes. The record does not need a second opaque
request blob, and a caller cannot replace the reconstructed preimage with a
boolean equality assertion.

The exact `skylet-state` `emptyDir` uses SQLite WAL with synchronous FULL. The
first submit transaction allocates one job ID and fsync-commits the byte-equal
job plus a pending start-outbox row before returning or waking the drainer. A
startup and continuous drainer processes job IDs in order. It transactionally
advances `COMMITTED_PENDING_START|RECOVERY_PENDING -> START_INTENT`, increments
the run epoch, creates a run token, and rewrites the same outbox to that
positive epoch/token in `PENDING`; it then
spawns only the pinned launcher shim. Before invoking the entrypoint, that shim
transactionally validates the token, records its stable PID/start ticks,
advances the row to `START_COMMITTED`, and marks the outbox delivered. Only
after that commit does it `exec` the pinned long-running canary entrypoint,
preserving the PID. Successful `exec` is not inferred from that transaction.
The pinned entrypoint first validates the same job/epoch/token/PID, measures its
own allowlisted artifact, and fsync-commits `START_COMMITTED -> RUNNING` plus
`entrypoint_measurement_sha256` before starting any service-command byte. Only
that post-exec handshake is `JOB_RUNNING` evidence. A watcher records terminal
process state.

On Skylet/container restart, the drainer validates the same Pod UID, store UUID,
schema, and artifacts. It turns stale `START_INTENT`, `START_COMMITTED`, or
`RUNNING` from an older Skylet process epoch into `RECOVERY_PENDING` for the
same logical job ID and starts a new run epoch. That restart is legal only for this reviewed
long-running restart-safe canary schema; one-shot or arbitrary jobs are not
representable. Missing/corrupt state, a changed Pod/store UUID, schema drift,
or a live mismatched PID/token is `BLOCKED`, never `not_found` and never
authority to create another job. A Pod replacement has a different UID and is
an identity conflict.

The initial outbox row is exactly `{run_epoch: 0, run_token: null,
state: "PENDING"}`. A positive epoch always has a nonnull token; `DELIVERED`
requires both to be nonzero/nonnull and byte-equal to the associated job row.
Recovery may change a delivered row back to `PENDING` only in the same
transaction that advances that same job to a new `START_INTENT` epoch. No
second outbox row exists for a job.
`START_COMMITTED` requires a delivered same-epoch/token outbox and null
`entrypoint_measurement_sha256`; `RUNNING` requires the same delivered outbox
plus the exact nonnull allowlisted entrypoint measurement written by the
post-exec handshake.

Crash tests cover both sides of job/outbox commit, lost response, commit before
wake, both sides of `START_INTENT`, spawn before the shim transaction, both
sides of `START_COMMITTED`, failed/crashed `exec`, both sides of the entrypoint
handshake, service start before central observation, watcher loss, and
Skylet/container restart in every local state. They assert that
`START_COMMITTED` can never satisfy `JOB_RUNNING`, plus one submission key, one
job ID, monotonic run epochs, mandatory startup drain, and no second job row.

## Down behavior

Down always targets the frozen locator. It may invoke the fixed three-object
delete group once per materialized attempt, but success requires a subsequent
authoritative absence observation and then expected-UUID removal of the exact
cluster handle/row. Each delete intent and exact absence is checkpointed in the
down phase cursor. Delete acknowledgement, a missing local handle before a
durable `HANDLE_REMOVE_INTENT`, or a name-only inventory miss is insufficient.
The handler first exact-reads all three plans from the immutable cleanup target.
Matching objects validate or extend only previously unknown UID commitments;
NotFound becomes `absent_exact`. A different known UID, identity label, or
semantic spec blocks. It atomically commits `INTENT_COMMITTED` plus
`TARGET_RESOLVED`, then deletes only
`present_exact` roles in the frozen delete order with UID preconditions,
checkpointing every intent and result. This same path handles a completed
launch and a superseded partial launch; there is no hidden provider cleanup in
the old launch action.

After `ABSENCE_EXACT`, the handler commits `HANDLE_REMOVE_INTENT`, removes or
adopts an exact no-row result for the same-UUID row, commits `HANDLE_REMOVED`, then
revalidates the complete down-success proof and current claim and commits the
final no-effect `SUCCEEDED` snapshot before returning a typed success outcome.
A crash before either final `SUCCEEDED` commit is recovered by observation and
monotonic progress adoption; the reducer cannot infer success from an earlier
phase.

In shadow, the expected cluster-record UUID is carried through the real
`sdk.down()` request, `core.down()`, and legacy backend teardown. In authority,
the action handler supplies it directly to the extracted fail-closed cluster
row/session seam. Both reload the cluster row and check the UUID after acquiring
its resource lock, immediately before any provider delete or state-row removal.
An earlier name lookup is not a fence: if a same-name successor appears while
teardown waits for the lock, the post-lock mismatch returns conflict and no
mutation occurs. Handle refresh and cluster-row deletion use the same expected
UUID. The handle is removed only after all three objects are authoritatively
absent and `HANDLE_REMOVE_INTENT` commits. A crash after exact row removal but
before `HANDLE_REMOVED` adopts an exact no-row result only from that durable
intent; a differently identified same-name row is conflict and it replays no
provider delete.

If a different incarnation now uses the display name, the facet must avoid
deleting it. The exact-name mismatch is `conflict` and never terminalizes the
old target as absent, even if a previously recorded UID is no longer visible.
Serve quarantines the action for operator repair; only three exact NotFound
reads under the frozen scope can establish authoritative absence in v1.

There is no time- or failure-count-based provider-facet cleanup deadline. Serve
schedules bounded database-clock retries while the parent's finite attempt
domain remains; reaching its exact maximum takes the typed exhaustion block and
never wraps or manufactures a max-plus-one attempt.

## Shadow protocol

Shadow has one mutation owner: the existing legacy launch/down thread. The
durable path receives the same frozen decision inputs but does not submit.

For every eligible decision in a service's shadow window, one logical parent
is committed with replica/capacity or teardown intent before the legacy enqueue.
It contains the would-be identity/plan and final legacy/proposed projections.
Retries never create another parent.

M2 parity is captured at the actual provider boundary, not inferred from the
earlier server-policy hook. A request-scoped context binds a read-only tap in
the Kubernetes client after path/query/body serialization and immediately
before `RESTClientObject.request`; authentication headers and credential
material are excluded. The same context binds a tap immediately before the
Skylet job-submit RPC. The effect trace includes only CoreV1 POST/DELETE
mutations and the job-submit RPC; exact GET and review/preflight reads remain in
typed observation/prerequisite evidence. Each represented child persists this closed trace and
its canonical hash:

```text
ProviderLegacyCoreV1CreateBodyV1 = {
  body_kind: "core_v1_create",
  role: ProviderObjectRoleV1,
  serialized_object: CanonicalJsonObject
}

ProviderLegacyCoreV1DeleteBodyV1 = {
  body_kind: "core_v1_delete",
  role: ProviderObjectRoleV1,
  serialized_delete_options: {preconditions: {uid: Text}}
}

ProviderLegacySkyletSubmitBodyV1 = {
  body_kind: "skylet_job_submit",
  submit_request: ProviderSkyletSubmitRequestV1
}

ProviderLegacyEffectBodyV1 =
  ProviderLegacyCoreV1CreateBodyV1 |
  ProviderLegacyCoreV1DeleteBodyV1 |
  ProviderLegacySkyletSubmitBodyV1

LegacyProviderEffectV1 = one of:
  {effect_kind: "core_v1_create",
   sequence: 0 | 1 | 2,
   boundary: "core_v1_http",
   method: "POST",
   canonical_path: Text,
   canonical_query: {},
   request_body: ProviderLegacyCoreV1CreateBodyV1,
   body_semantic_sha256: Sha256,
   response_status: null | Integer,
   returned_uid_or_job_id: null | Text}
  {effect_kind: "core_v1_delete",
   sequence: 0 | 1 | 2,
   boundary: "core_v1_http",
   method: "DELETE",
   canonical_path: Text,
   canonical_query: {},
   request_body: ProviderLegacyCoreV1DeleteBodyV1,
   body_semantic_sha256: Sha256,
   response_status: null | Integer,
   returned_uid_or_job_id: null}
  {effect_kind: "skylet_job_submit",
   sequence: 3,
   boundary: "skylet_job_submit",
   rpc: "skylet_idempotent_submit_v1",
   request_body: ProviderLegacySkyletSubmitBodyV1,
   body_semantic_sha256: Sha256,
   response_status: null | Integer,
   returned_uid_or_job_id: null | DecimalIntegerText}

LegacyProviderEffectTraceV1 = {
  version: 1,
  effects: [LegacyProviderEffectV1]
}
```

For a create, `serialized_object` is byte-equal to the applicable frozen
`ProviderKubernetesObjectPlanV1.request_body`. For a delete,
`serialized_delete_options` contains exactly the committed UID precondition
and no unlisted option. For a `LAUNCH_CLEANUP_DOWN` child, that UID is
byte-equal to the current logical attempt's write-once same-role UID
commitment. That commitment is established only by the same parent's
primary-launch create/adoption evidence or by the earliest request-sequenced
cleanup pre-observation containing an exact-present object entry whose `role`
equals the delete role, and it is carried unchanged across later cleanup
retries. Every later exact-present same-role observation must agree. A delete
does not clear the commitment; a different create/adoption/replacement, a
missing commitment, or a mismatch makes the trace incomplete or divergent and
promotion-blocking. The parent launch capsule freezes names/specs but cannot
invent a runtime-assigned UID, and there is no name-only delete fallback. Each
CoreV1 path is the exact scope/kind/name path derived from the frozen plan;
create and delete query objects are empty. Create and delete sequences equal
the role map's respective sequence. For Skylet, the body is
exactly the closed `ProviderSkyletSubmitRequestV1`; arbitrary job JSON is not
representable. A secret-bearing, unbounded, or cross-kind body makes the
candidate not representable and persists no body/hash. For an eligible launch,
the complete trace is exactly create head SSH Service, create head Service,
create head Pod, then one action-UUID-keyed Skylet submission. Down is exactly
the three UID-preconditioned deletes in frozen order. Any missing or
uncorrelated tap, extra provider call, body/path/query mismatch, bootstrap/
`exec`/`cp`/patch call, or job-spec mismatch makes the child incomplete or
divergent. The post-policy hook remains useful policy evidence but cannot by
itself establish provider-byte parity.

Before the candidate window starts, the narrow legacy-owned path is routed
through the same pure object renderer and prebooted-runtime/Skylet seam while
the existing SafeThread and one real PR #1070 request remain the only mutation
owner. It uses a private `serve_shadow_candidate_launch/down` handler that only
the attested authority-worker cohort may claim; public clients cannot select
that handler, and general normal executors explicitly exclude it. If the
current generic launch still emits bootstrap, sync, setup, or a non-idempotent
job call, the trace diverges and authority remains closed. M4 consumes the same
renderer objects and job spec inside `KubernetesResourceActionSession`; the tap
then acts as an assertion rather than a parallel serializer.

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
`ResolvedProviderTargetV1`, exact-read the same-UUID global-user-state cluster
row and its full `ProviderKubernetesHandleV1`, and copy both typed preimages and
their hashes into `PriorLaunchBasisV1`; missing, hash-only, or name-only
evidence is not eligible.

## Eligibility and activation

A lifecycle profile is authoritative only when checked-in contract tests prove:

- stable locator creation before mutation;
- one fixed bounded mutation group, with intent committed before its first
  object and exact observation of every possible partial effect;
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
fingerprint below. Inputs outside the closed profile remain on legacy and add a
bounded coverage blocker; they do not get coerced into a fake provider plan.
Accepting an authoritative profile updates this canonical file and its contract
fixtures.

## Invocation fingerprint and redaction

`request_payload_sha256` is not a hash of `LaunchBody.model_dump()` or another
generic request object. Those objects include ambient configuration, uploaded
mount state, environment values, and secrets and are constructed after client
side effects. A pure closed `ProviderLifecycleInvocationV1` builder produces
the exact bounded, redacted provider-affecting input that the action-aware
launch/down call consumes. That normalized object is embedded in the immutable
action spec and its canonical bytes produce `request_payload_sha256`.

The closed invocation union is:

The previously implemented flattened object also used `version: 1`, but it was
a local pre-authority scaffold introduced only on the unmerged feature branch:
its introducing commits are contained by no tag or remote branch other than
`origin/feat/serve-resource-actions-m1a`, and it has never been released or
deployed. The contract below is the first deployable v1 wire shape and replaces
that scaffold in place. There is no dual reader, backfill, or
optional-execution-config form.

The one-time first-deployment gate keeps every API, worker, and controller on
the proven baseline image while the additive migrations reach API006,
Serve033, and global-user-state 028. In one consistent read-only PostgreSQL
snapshot it then requires zero Serve replica rows in `api_resource_actions`,
their attempts and correlated `api_requests`; zero rows in all six Serve033
sample, represented-attempt, coverage, coverage-attempt, worker-cohort, and
worker-cohort-reference tables; zero replica action/sample/coverage links; and
no service mode other than `legacy`. The pinned baseline cannot race this
snapshot because it has no resource-action writer. The exact query output,
schema heads, and baseline digest are retained as rollout evidence before API
rollout begins.

An unexpected/missing revision or table, nonzero row/link, nonlegacy service,
or mixed writer image aborts rollout before a new v1 reader or writer runs.
Application code never rewrites, reinterprets, or purges an old row. If one is
found, v1 remains frozen and the feature requires either v2 or a separately
reviewed offline canonical migration; new-shape v1 cannot coexist with old-
shape v1. An encountered flattened row fails the closed parser and blocks
promotion/recovery. Canonical invocation, plan, and wrapper hashes and their
golden fixtures change; deterministic launch/down action UUIDs do not, because
they derive only from logical resource identity and action kind.

```text
ProviderLaunchLifecycleInvocationV1 = {
  version: 1,
  profile: "pod_cluster_v1",
  redaction_profile: "provider_lifecycle_redaction_v1",
  action_kind: "launch",
  resource_identity: ProviderLifecyclePlanV1.resource_identity,
  requested_target: ProviderLocatorV1,
  launch: ProviderLaunchInvocationV1,
  down: null
}

ProviderDownLifecycleInvocationV1 = {
  version: 1,
  profile: "pod_cluster_v1",
  redaction_profile: "provider_lifecycle_redaction_v1",
  action_kind: "down",
  resource_identity: ProviderLifecyclePlanV1.resource_identity,
  requested_target: ProviderLocatorV1,
  launch: null,
  down: ProviderDownInvocationV1
}

ProviderLifecycleInvocationV1 =
  ProviderLaunchLifecycleInvocationV1 |
  ProviderDownLifecycleInvocationV1

The `ParentSpec` symbol in the child-only schema below is exactly `self` in the
zero-argument method
`ServeReplicaActionSpecV1.launch_cleanup_down_invocation(self)`. The method
first requires `ParentSpec.invocation` to be a primary launch and receives no
replacement target, workspace, basis, config, or other argument.

ServeLegacyLaunchCleanupDownInvocationV1 = {
  version: 1,
  contract: "serve_legacy_launch_cleanup_down_v1",
  request_role: "LAUNCH_CLEANUP_DOWN",
  effect_kind: "down",
  profile: "pod_cluster_v1",
  redaction_profile: "provider_lifecycle_redaction_v1",
  parent_launch_action_id: UUID,
  parent_launch_request_payload_sha256: Sha256,
  resource_identity: ParentSpec.invocation.resource_identity,
  requested_target: ParentSpec.invocation.requested_target,
  legacy_down_request: {
    cluster_name: ParentSpec.invocation.requested_target.sky_cluster_name,
    expected_cluster_record_uuid:
        ParentSpec.invocation.requested_target.sky_cluster_record_uuid,
    workspace: ParentSpec.invocation.require_launch().source.content.workspace,
    purge: false,
    graceful: false,
    graceful_timeout: null
  }
}

ServeShadowAttemptInvocationV1 =
  ProviderLifecycleInvocationV1 |
  ServeLegacyLaunchCleanupDownInvocationV1

ProviderLaunchIdentityCanonicalizationInputV1 = {
  version: 1,
  contract: "api_server_effective_launch_identity_v1",
  service_name: Text,
  resource_identity: ProviderLifecyclePlanV1.resource_identity,
  prepared_original_user: Text,
  prepared_user_hash: Text
}

ProviderLaunchIdentityCanonicalizationContextV1 = {
  version: 1,
  decision_id: UUID,
  cohort_id: Text,
  action_type: "launch",
  controller_owner_fence: Text,
  lifecycle_epoch: PositiveInteger,
  preparation_reference_revision: 1,
  reference_state: "PREPARING",
  preparation_capability_sha256: Sha256,
  input: ProviderLaunchIdentityCanonicalizationInputV1,
  input_sha256: Sha256
}

ProviderLaunchIdentityCanonicalizationRequestV1 = {
  version: 1,
  context: ProviderLaunchIdentityCanonicalizationContextV1,
  context_sha256: Sha256,
  preparation_capability: 64LowerHex
}

ProviderLaunchIdentityCanonicalizationProofV1 = {
  version: 1,
  boundary: "api_server_post_auth_no_enqueue",
  context: ProviderLaunchIdentityCanonicalizationContextV1,
  context_sha256: Sha256,
  effective_original_user: Text,
  effective_user_hash: Text
}

ProviderLaunchIdentityCanonicalizationResponseV1 = {
  version: 1,
  decision_id: UUID,
  context_sha256: Sha256,
  proof: ProviderLaunchIdentityCanonicalizationProofV1,
  proof_sha256: Sha256
}

ProviderLaunchContentSourceV1 = {
  store: "serve_version_specs",
  service_name: Text,
  service_incarnation: UUID,
  service_version: PositiveInteger,
  yaml_content_sha256: Sha256,
  workspace: Text
}

ProviderLaunchSourceV1 = {
  content: ProviderLaunchContentSourceV1,
  identity_canonicalization:
      ProviderLaunchIdentityCanonicalizationProofV1,
  identity_canonicalization_sha256: Sha256
}

ProviderLaunchInvocationV1 = {
  source: ProviderLaunchSourceV1,
  resources: ProviderPodResourceSnapshotV1,
  topology: ProviderPodTopologyV1,
  execution_config: ProviderKubernetesExecutionConfigV1,
  replica_env: {"SKYPILOT_SERVE_REPLICA_ID": DecimalIntegerText},
  security_group_scope: Text,
  admin_policy_mode: "absent_controller_and_executor",
  managed_secrets_mode: "absent",
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
  prior_launch_basis: PriorLaunchBasisV1,
  execution_config: ProviderKubernetesDownExecutionConfigV1,
  purge: false,
  graceful: false,
  graceful_timeout: null
}

The exact endpoint is `POST /internal/resource-actions/v1/launch-identity/canonicalize`
on the normal API origin, after its ordinary authentication middleware and
outside the request executor. The manager generates 32 random bytes from the
OS CSPRNG for each new reference, transports them as exactly 64 lowercase hex
characters, stores only SHA-256 of the decoded 32 bytes in the `PREPARING`
reference, and gives the raw value only to that live preparation cell. The
request context repeats the stored commitment and every nonsecret reference/
owner field. The proof retains that complete context and its hash, never the
raw capability. Response validation requires byte-equal decision/context/hash
echoes before the proof can enter `ProviderLaunchSourceV1`; recovery recomputes
the retained context/proof hashes and validates all immutable reference fields
and the capability hash against the still-retained same-ID reference, but never
needs the raw capability. The proof's `PREPARING`/revision-one values are the
historical canonicalization boundary. Admission stores the proof while
atomically advancing that row to `SHADOW_ACTIVE` or `ACTION_ACTIVE`; recovery
therefore requires the exact kind-matched legal successor revision/state and
bind timestamp, not a current row still equal to `PREPARING`.

The endpoint reads the raw body with a hard 65,536-byte pre-parse limit,
requires `Content-Type: application/json` and no `Content-Encoding`, decodes
canonical UTF-8 JSON, and calls the closed
`ProviderLaunchIdentityCanonicalizationRequestV1.from_value` parser directly.
The FastAPI signature accepts only `Request`; it has no auto-decoded body model.
The controller sends the typed request's exact `canonical_bytes` as raw content
through `make_authenticated_request`, not its `json=` convenience argument. It
must not use `BasePayload` or any `extra='ignore'` model; every extra key,
subclass-spoofed in-process value, noncanonical byte sequence, or mismatched
hash rejects before lookup. It accepts no `LaunchBody`, task YAML, generic
environment map, credential, or provider configuration. In one read-only
database session the direct handler recomputes the input/context hashes and
deterministic decision ID, exact-compares the raw capability hash in constant
time with the context and stored row, and read-validates the exact
`PREPARING` revision-one cohort reference, service incarnation, controller-
owner fence, lifecycle epoch, and replica/generation identity. It then applies
the same extracted effective-identity resolver used by
`prepare_request_async()`, validates the bounded result, and returns the closed
response. The later admission transaction locks and revalidates every
optimistic ownership/reference/context field; this endpoint grants no mutation
authority.

The direct endpoint itself inserts or mutates no API request, queue, action,
coverage, reference, service, replica, or provider row. Ordinary authentication
middleware may independently update its existing token-last-used or proxy-user
bookkeeping before the route. Exact statuses are: `400` for malformed,
noncanonical, self-hash-invalid, or deterministically inconsistent bytes;
`401` for ordinary authentication failure; `403` for a capability mismatch;
`409` for an unknown/non-`PREPARING` reference or stale/
unequal owner/cohort/epoch/resource context, `413` for an oversized body, `415`
for content-type/encoding failure, and `503` for transient database
unavailability. Only a connection reset, timeout, or `503` permits one retry
after 100 ms with identical bytes; every other non-`200`, malformed response,
or unequal decision/context/input/proof/hash is terminal
`unfrozen_identity`. Redirects are disabled.

For an authenticated request the effective pair is exactly
`(auth_user.name, auth_user.id)`; with no authenticated user it is exactly the
submitted prepared pair, matching legacy `/launch`. The proof context and
output are immutable members of `ProviderLaunchSourceV1`; their hashes are
recomputed on every parse, and the context input `service_name` equals
`source.content.service_name` and the locked service. Cross-process recovery
resolves only the referenced exact YAML and uses the retained effective pair to
reconstruct the full invocation. It never re-runs
authentication, consults a current actor, infers a hash from
`version_specs.created_by`, or treats `services.hash` as a user hash.
Within a live shadow preparation, canonicalization and the later `/launch`
submission use the same `server_common.make_authenticated_request` credential/
endpoint resolution; no alternate HTTP client or identity header is allowed.
Credential or proxy-auth change between the two calls is detected only by the
locked persisted-request comparison and becomes `IDENTITY_MISMATCH`, never a
silent change to the admitted action.

DecimalPortText = canonical ASCII decimal text matching
  `[1-9][0-9]{0,4}` whose integer value is at most 65535. Leading zeroes,
  signs, whitespace, ranges, and alternate numeric spellings are invalid.
  This type applies to workload application and Skylet management ports; the
  integer port inside `ProviderKubernetesServerOriginV1` is a separate
  transport-decomposition field and is not silently coerced through this type.

DecimalIntegerText = canonical ASCII decimal text matching
  `(?:0|[1-9][0-9]{0,18})` whose integer value is at most
  9223372036854775807. Leading zeroes, signs, whitespace, decimal points,
  exponents, unit suffixes, more than 19 digits, and larger 19-digit values are
  invalid. This type is used for the retained replica-ID environment value and
  legacy decimal job-ID text; callers do not silently coerce an integer into
  it.

CanonicalPositiveDecimalText = canonical ASCII text matching
  `(?:[1-9][0-9]*|(?:0|[1-9][0-9]*)\.[0-9]{0,2}[1-9])`, whose exact decimal
  value is greater than zero and at most 9223372036854775807. The integer part
  is always present; leading integer zeroes, a decimal point without a
  fraction, trailing fractional zeroes, more than three fractional digits,
  signs, whitespace, exponent, unit suffix, and every zero spelling are
  invalid. Thus `1`, `0.5`, `0.001`, and `1.23` are canonical, while `01`,
  `.5`, `1.0`, `1.230`, and `0.000` are not.

CanonicalJsonValue and CanonicalJsonObject use the existing NFC canonical JSON
domain but add mandatory pre-serialization bounds and an exact parsed-value
type boundary. The only accepted runtime types are the exact built-ins
`NoneType`, `bool`, `int`, `str`, `list`, and `dict`; subclasses and arbitrary
`Mapping` implementations are invalid at the root and every nested position.
An object variant has an exact built-in `dict` root, and every object key is an
exact built-in `str`. Each string/key is 1..1,024 UTF-8 bytes, every integer
fits signed 64-bit, floats and reference cycles are forbidden, and each
object/list has at most 256 members. Container depth is at most 16 with a root
container counted as depth one, and the aggregate sum of object members plus
list elements is at most 4,096. Each standalone canonical value and the
enclosing typed object are at most 65,536 canonical UTF-8 bytes. Validation is
iterative and checks a container's exact type and cardinality before copying or
iterating its members. It constructs a detached validated graph, rechecks the
copied cardinality, and calls the generic recursive serializer only on that
detached graph. Mutation of the caller's original graph after a node is copied,
including during or after serialization, cannot change that detached node or
introduce an unvalidated value into the committed bytes. Concurrent mutation
while different nodes are being copied may cause rejection or determine which
fully validated values enter the snapshot; a caller requiring an atomic
multi-node point-in-time view must synchronize its own graph. Arrays preserve
order and objects use the canonical key ordering. Empty strings,
nontext/duplicate-after-NFC keys, noncanonical text, tuples masquerading as
parsed JSON arrays, scalar or container subclasses, and any value outside this
domain are invalid. Rejected container/`Mapping` subclasses are rejected on
their runtime type before invoking their iterator, `items()`, `len()`, or other
overridable method.

The shared scalar leaf validators, when a value is presented to them directly,
likewise accept exact built-in strings, integers, and Booleans rather than
subclasses or coercible values. A shared enum leaf accepts either an exact
member of its declared enum class or an exact built-in string with the
canonical member value. A UUID leaf accepts an exact `uuid.UUID` or exact
canonical built-in string. An action-kind leaf accepts an exact `ActionKind` or
exact canonical built-in string. Typed enum/UUID/action-kind positives are
process-local helper/direct-constructor inputs; a JSON-wire field still uses
its displayed canonical string spelling.

The literal `context_mode="in_cluster"` and `port_mode="podip"` fields in
`ProviderKubernetesHandleProviderConfigV1` use exact raw-string gates because
that parser deliberately preserves its shallow input. The raw JSON-wire
`action_kind` fields in `ProviderLifecycleInvocationV1`,
`ProviderLifecyclePlanV1`, and `ServeShadowProjectionV1` are exact built-in
strings and are checked before their existing canonical encode/reparse step;
their direct constructors then use the shared exact action-kind gate. The same
gate also protects `ProviderResourceIdentityV1.action_identity()` before it
delegates deterministic action-ID construction to the generic kernel. Exact
`CanonicalJsonValue` is required at the server-allocation embedding, and exact
`CanonicalJsonObject` is required for both object-plan body embeddings; wrapper
subclasses cannot override bytes or hashes at those persisted seams.
Equality-, hash-, length-, or bound-spoofing subclasses cannot satisfy any of
these named gates.

Existing invalid-value categories remain stable. Shared text, enum, UUID, and
action-kind wrong-type failures retain their current `TypeError` text; unknown
exact enum/action-kind strings retain their current `ValueError` unsupported
text. Hash, integer, timestamp, and their format/range failures retain their
current `ValueError` messages, while Boolean wrong types retain their current
`TypeError` message. The three action-kind DTO constructors still translate
either shared-gate failure to their existing class-specific `ValueError`
message, and `action_identity()` retains the generic kernel's existing
`ValueError("action_kind must be launch or down.")`. Newly rejected subclasses
use the same category as a non-exact value at that gate. This hardening adds no
coercion or alternate wire spelling.

This is a bounded shared-helper and named-call-site contract, not a claim that
every older direct DTO constructor in this module already rejects every scalar,
container, or typed-child subclass. It also does not change legacy
`from_value()` paths that intentionally pass through `_closed_object`, whose
canonical encode/reparse may turn otherwise valid JSON-like subclasses into
built-ins before a leaf validator receives them. Only the three raw
action-kind fields named above add a pre-normalization gate. A constructor,
parser field, or wrapper embedding not routed through one of these helpers or
named gates remains governed by its separately reviewed contract until it is
explicitly migrated and tested.

DigestPinnedOCIReference = canonical secret-free OCI reference accepted
  byte-for-byte by `sky.container_images.models.validate_oci_reference`, with
  one terminal `@sha256:` plus 64 lowercase hexadecimal characters. Its parsed
  digest must equal the enclosing `oci_manifest_digest`. The reusable leaf may
  retain a validator-accepted tag before that digest; the first
  `pod_cluster_v1` workload normalizer independently rejects every tag,
  including tag-plus-digest. A scheme, whitespace, userinfo, query, fragment,
  percent encoding, backslash, absent digest, uppercase/noncanonical spelling,
  or normalized output unequal to the input is invalid. The approved cohort
  inventory pins the validator implementation; changing these v1 semantics
  requires a new profile/version.

ProviderPodResourceSnapshotV1 = {
  version: 1,
  cloud: "kubernetes",
  cluster_fingerprint_sha256: Sha256,
  namespace: Text,
  instance_type: Text,
  accelerator: null,
  cpus: CanonicalPositiveDecimalText,
  memory_gb: CanonicalPositiveDecimalText,
  image: ProviderPodImageV1,
  disk_size_gb: PositiveInteger,
  disk_tier: null | Text,
  ports: [DecimalPortText],  # exactly one
  labels: [],
  use_spot: false
}

ProviderOCIImageQualificationV1 = {
  requested_reference: DigestPinnedOCIReference,
  oci_manifest_digest: "sha256:" + 64LowerHex,
  oci_config_digest: "sha256:" + 64LowerHex,
  qualification_artifact: ProviderRepoArtifactRefV1
}

ProviderRuntimeImageIdentityV1 = {
  raw_image_id: Text,
  runtime_image_id_scheme: "containerd" | "cri-o" | "docker-pullable",
  runtime_image_id_digest: "sha256:" + 64LowerHex,
  qualified_oci_manifest_digest: "sha256:" + 64LowerHex,
  qualified_oci_config_digest: "sha256:" + 64LowerHex,
  qualification_artifact_sha256: Sha256,
  runtime_id_contract: "qualified_oci_config_digest_v1"
}

ProviderPodImageV1 = {
  source: "explicit",
  qualification: ProviderOCIImageQualificationV1,
  auth_strategy: "anonymous",
  implementation_contract: "kubernetes_serve_prebooted_runtime_v1"
}

`ProviderPodImageV1` is a fixed shape: `source`, `auth_strategy`, and
`implementation_contract` accept only the displayed literals. Its
qualification applies the complete `DigestPinnedOCIReference` contract above,
including parsed requested-reference digest equality with
`oci_manifest_digest`; merely passing the generic OCI parser is insufficient.
Every resource-contract, object-body, runtime, and artifact-binding copy of the
workload image must agree under the enclosing capsule checks.

ProviderKubernetesConfigProjectionV1 = {
  version: 1,
  workspace: Text,
  context_mode: "in_cluster",
  target_namespace: Text,
  port_mode: "podip",
  built_in_provider: true,
  custom_provider_implementation: null,
  custom_provisioner: null,
  custom_template: null,
  custom_pod_config: null,
  custom_metadata: [],
  global_labels: [],
  runtime_class_name: null,
  priority_class_name: null,
  queue: null,
  kueue: false,
  dws: false,
  autoscaler: null,
  detected_network_type: "default",
  persistent_volumes: [],
  object_stores: [],
  file_mounts: [],
  workdir: null,
  fuse: false,
  docker_cache: false,
  auto_mounts: false,
  tls_material: null,
  managed_secrets: [],
  task_secrets: [],
  service_account_bootstrap: false,
  rbac_bootstrap: false,
  config_access_inventory: ProviderRepoArtifactRefV1
}

ProviderPolicyModeEvidenceV1 = {
  admin_policy_entrypoint: null,
  admin_policy_applied: false,
  managed_secrets_provider: null,
  managed_secret_reference_count: 0
}

ProviderPolicyBoundaryProofV1 = {
  version: 1,
  boundary: "serve_controller_prepare" | "api_executor_pre_io",
  config_projection_sha256: Sha256,
  modes: ProviderPolicyModeEvidenceV1,
  policy_subject_sha256: Sha256,
  projection_before_sha256: Sha256,
  projection_after_sha256: Sha256,
  projections_equal: true
}

ProviderAnnotationV1 = {
  key: Text,    # 1..1,024 UTF-8 bytes
  value: Text  # 1..1,024 UTF-8 bytes
}

ProviderKubernetesServiceAccountProjectionV1 = {
  namespace: Text,
  name: Text,
  uid: Text,
  resource_version: Text,
  labels: [{key: Text, value: Text}],
  annotations: [ProviderAnnotationV1],
  automount_service_account_token: Boolean,
  image_pull_secrets: [Text],
  legacy_secret_refs: [Text]
}

ProviderKubernetesSelfIdentityV1 = {
  username: Text,
  uid: Text,
  groups: ["system:authenticated", "system:serviceaccounts",
           "system:serviceaccounts:" + CallerNamespace],
  extra_keys: []
}

ProviderKubernetesApiGroupV1 =
  "" | "apps" | "networking.k8s.io" |
  "admissionregistration.k8s.io" |
  "authentication.k8s.io" | "authorization.k8s.io"

ProviderKubernetesResourceV1 =
  "namespaces" | "serviceaccounts" | "pods" | "services" |
  "replicasets" | "deployments" | "networkpolicies" |
  "validatingadmissionpolicies" |
  "validatingadmissionpolicybindings" | "selfsubjectreviews" |
  "selfsubjectrulesreviews" | "selfsubjectaccessreviews"

ProviderKubernetesVerbV1 =
  "get" | "create" | "delete" | "list" | "watch" | "patch" |
  "update" | "deletecollection"

ProviderKubernetesApiGroupResourceMapV1 = {
  "": ["namespaces", "pods", "serviceaccounts", "services"],
  "admissionregistration.k8s.io":
      ["validatingadmissionpolicies",
       "validatingadmissionpolicybindings"],
  "apps": ["deployments", "replicasets"],
  "authentication.k8s.io": ["selfsubjectreviews"],
  "authorization.k8s.io":
      ["selfsubjectaccessreviews", "selfsubjectrulesreviews"],
  "networking.k8s.io": ["networkpolicies"]
}

ProviderKubernetesResourceRuleV1 = {
  api_groups: [ProviderKubernetesApiGroupV1],  # exactly one
  resources: [ProviderKubernetesResourceV1],  # 1..256
  resource_names: [Text],                     # 0..256
  verbs: [ProviderKubernetesVerbV1]           # 1..256
}

ProviderKubernetesNonResourceRuleV1 = {
  urls: ["/version"],
  verbs: ["get"]
}

ProviderKubernetesRulesReviewV1 = {
  namespace: Text,
  incomplete: false,
  evaluation_error: false,
  resource_rules: [ProviderKubernetesResourceRuleV1],       # 1..256
  non_resource_rules: [ProviderKubernetesNonResourceRuleV1] # exact singleton
}

ProviderKubernetesResourceAccessV1 = {
  api_group: ProviderKubernetesApiGroupV1,
  resource: ProviderKubernetesResourceV1,
  subresource: null,
  verb: ProviderKubernetesVerbV1,
  namespace: null | Text,
  name: null | Text
}

ProviderKubernetesNonResourceAccessV1 = {verb: "get", path: "/version"}

ProviderKubernetesAccessDecisionV1 = {
  check_sequence: NonnegativeInteger,
  resource: null | ProviderKubernetesResourceAccessV1,
  non_resource: null | ProviderKubernetesNonResourceAccessV1,
  expected_allowed: Boolean,
  observed_allowed: Boolean,
  observed_denied: Boolean,
  evaluation_error: false
}

ProviderKubernetesAuthorizationEvidenceV1 = {
  identity: ProviderKubernetesSelfIdentityV1,
  rules: ProviderKubernetesRulesReviewV1,
  rules_sha256: Sha256,
  access_matrix_contract: ProviderRepoArtifactRefV1,
  access_decisions: [ProviderKubernetesAccessDecisionV1],  # 1..256
  access_decisions_sha256: Sha256
}

ProviderKubernetesPrincipalsV1 = {
  caller: ProviderKubernetesServiceAccountProjectionV1,
  workload: ProviderKubernetesServiceAccountProjectionV1,
  caller_authorization: ProviderKubernetesAuthorizationEvidenceV1
}

Each resource rule has exactly one API group; every resource in that rule, and
every resource access decision, must belong to that group under
`ProviderKubernetesApiGroupResourceMapV1`. The required rule arrays are
nonempty as annotated, while `resource_names` may be empty because create
permissions and rollout-generated Pod/ReplicaSet names cannot use a fixed
`resourceNames` fence. All inner sets and compound-rule collections retain the
canonical sorting rules below. The nonresource-rule collection is exactly one
rule with `urls=["/version"]` and `verbs=["get"]`.

An access decision has exactly one nonnull member of `resource` and
`non_resource`. Decisions are a nonempty contiguous zero-based sequence and
`observed_allowed == expected_allowed`; `evaluation_error` is exactly false.
`observed_denied` preserves the Kubernetes authorization response rather than
being synthesized as the complement of `observed_allowed`: the two observed
booleans cannot both be true, an allowed result requires `observed_denied=false`,
and a not-allowed result permits either value of `observed_denied`. Exact check
count, attributes, expectation, and order are supplied by and byte-compared to
the content-addressed access-matrix artifact, not invented by the leaf parser.
The enclosing action kind binds this reference to one unique approved inventory
role: `launch_access_matrix` includes both exact LB Deployment GET decisions,
while `down_access_matrix` omits those two decisions. Both include the exact
12-role prerequisite, worker-chain, version, self-review, and action-applicable
CoreV1 decisions. Crossed, missing, duplicate, or extra action-kind decisions
reject; a down matrix cannot silently inherit the launch-only GETs.

Live ServiceAccount normalization rejects a null/omitted
`automountServiceAccountToken`; the first cohort requires an explicit `true`
on the caller authority-worker ServiceAccount and an explicit `false` on the
no-permission workload ServiceAccount. Absent labels, annotations,
`imagePullSecrets`, or legacy `secrets` normalize to their typed empty
collections. The workload image-pull-secret and legacy-secret collections are
exactly empty. The principals envelope requires caller and workload
namespace/name/UID to equal the corresponding scope fields, workload namespace
to equal the target namespace, and rules-review namespace to equal that same
target namespace. Its self identity is exactly
`system:serviceaccount:<caller.namespace>:<caller.name>`, has UID byte-equal to
`caller.uid`, and has the displayed three groups with the caller namespace in
the final group. The scope's existing rule that caller/workload
`(namespace,name)` pairs are equal if and only if their UIDs are equal remains
in force.

ProviderKubernetesPrerequisiteRoleV1 =
  "authority_release_namespace" | "target_namespace" |
  "kube_system_namespace" | "serve_lb_slot_0_namespace" |
  "serve_lb_slot_1_namespace" | "caller_service_account" |
  "workload_service_account" | "serve_lb_slot_0_service_account" |
  "serve_lb_slot_1_service_account" | "endpoint_network_policy" |
  "validating_admission_policy" |
  "validating_admission_policy_binding"

ProviderKubernetesPrerequisiteV1 = {
  role: ProviderKubernetesPrerequisiteRoleV1,
  api_version: Text,
  kind: "Namespace" | "ServiceAccount" | "NetworkPolicy" |
        "ValidatingAdmissionPolicy" | "ValidatingAdmissionPolicyBinding",
  namespace: null | Text,
  name: Text,
  uid: Text,
  resource_version: Text,
  deletion_timestamp: null,
  spec: ProviderKubernetesPrerequisiteSpecV1,
  spec_sha256: Sha256
}

ProviderKubernetesPrerequisiteSpecV1 = one of:
  {kind: "Namespace", labels: [{key: Text, value: Text}],
   annotations: [ProviderAnnotationV1]}
  {kind: "ServiceAccount",
   projection: ProviderKubernetesServiceAccountProjectionV1}
  {kind: "NetworkPolicy", contract: "serve_action_network_policy_v1",
   manifest: ProviderRepoArtifactRefV1}
  {kind: "ValidatingAdmissionPolicy",
   contract: "serve_action_validating_policy_v1",
   manifest: ProviderRepoArtifactRefV1}
  {kind: "ValidatingAdmissionPolicyBinding",
   contract: "serve_action_validating_binding_v1",
   manifest: ProviderRepoArtifactRefV1}

ProviderKubernetesPrerequisiteKindMapV1 = {
  "Namespace": {api_version: "v1", scope: "cluster"},
  "ServiceAccount": {api_version: "v1", scope: "namespaced"},
  "NetworkPolicy":
      {api_version: "networking.k8s.io/v1", scope: "namespaced"},
  "ValidatingAdmissionPolicy":
      {api_version: "admissionregistration.k8s.io/v1", scope: "cluster"},
  "ValidatingAdmissionPolicyBinding":
      {api_version: "admissionregistration.k8s.io/v1", scope: "cluster"}
}

ProviderKubernetesPrerequisiteRoleMapV1 = [
  {sequence: 0, role: "authority_release_namespace", kind: "Namespace"},
  {sequence: 1, role: "target_namespace", kind: "Namespace"},
  {sequence: 2, role: "kube_system_namespace", kind: "Namespace"},
  {sequence: 3, role: "serve_lb_slot_0_namespace", kind: "Namespace"},
  {sequence: 4, role: "serve_lb_slot_1_namespace", kind: "Namespace"},
  {sequence: 5, role: "caller_service_account", kind: "ServiceAccount"},
  {sequence: 6, role: "workload_service_account", kind: "ServiceAccount"},
  {sequence: 7, role: "serve_lb_slot_0_service_account",
   kind: "ServiceAccount"},
  {sequence: 8, role: "serve_lb_slot_1_service_account",
   kind: "ServiceAccount"},
  {sequence: 9, role: "endpoint_network_policy", kind: "NetworkPolicy"},
  {sequence: 10, role: "validating_admission_policy",
   kind: "ValidatingAdmissionPolicy"},
  {sequence: 11, role: "validating_admission_policy_binding",
   kind: "ValidatingAdmissionPolicyBinding"}
]

ProviderKubernetesPrerequisiteInventoryV1 =
  [ProviderKubernetesPrerequisiteV1]  # exact 12 role-map entries/order

ProviderKubernetesResourceContractV1 = {
  source_cpus: CanonicalPositiveDecimalText,
  source_memory_gb: CanonicalPositiveDecimalText,
  pod_cpu_request: CanonicalK8sQuantity,
  pod_cpu_limit: CanonicalK8sQuantity,
  pod_memory_request: CanonicalK8sQuantity,
  pod_memory_limit: CanonicalK8sQuantity,
  translation_contract: "sky_to_k8s_exact_resources_v1",
  set_pod_resource_limits: true,
  resource_limit_multiplier: 1,
  live_allocatable_clamp: false,
  accelerator: null,
  ephemeral_storage: null,
  image: ProviderPodImageV1,
  image_pull_policy: "Always",
  application_port: DecimalPortText,
  resources_ports: [DecimalPortText],
  port_mode: "podip"
}

ProviderKubernetesRendererV1 = {
  contract: "serve_prebooted_direct_pod_v1",
  outer_template: ProviderRepoArtifactRefV1,
  node_fragment: ProviderRepoArtifactRefV1,
  binding_schema: ProviderRepoArtifactRefV1,
  config_access_inventory: ProviderRepoArtifactRefV1,
  admitted_object_normalization: ProviderRepoArtifactRefV1,
  source: ProviderLaunchContentSourceV1
}

ProviderKubernetesProvisionRuntimeMetadataV1 = {
  runtime_setup_done: true,
  has_ray: true,
  has_skylet: true,
  has_job_queue: true,
  workdir_synced: false,
  file_mounts_synced: false,
  setup_done: true,
  run_started: false
}

ProviderKubernetesJobSubmissionV1 = {
  protocol: "skylet_idempotent_submit_v1",
  submission_key_source: "launch_action_id",
  run_source: ProviderLaunchContentSourceV1,
  contract: ProviderSkyletJobContractV1,
  durability: ProviderSkyletDurabilityContractV1,
  job_spec_profile: "ProviderSkyletJobSpecV1"
}

ProviderKubernetesPostProvisionV1 = {
  runtime_mode: "prebooted_ray_skylet_v1",
  runtime_artifacts: [ProviderWorkloadArtifactBindingV1],  # exact role order
  provision_runtime_metadata: ProviderKubernetesProvisionRuntimeMetadataV1,
  sync_workdir: "assert_absent_skip",
  sync_file_mounts: "assert_absent_skip",
  user_setup: "assert_null_skip",
  pre_exec_hooks_autostop: "assert_absent_skip",
  management_transport: "skylet_grpc_only",
  management_port: "46590",  # DecimalPortText
  ssh_fallback: false,
  job_submission: ProviderKubernetesJobSubmissionV1
}

ProviderKubernetesEndpointCallerWorkloadV1 = {
  api_version: "apps/v1",
  kind: "Deployment",
  namespace: Text,
  name: Text,
  uid: Text,
  resource_version: Text,
  generation: PositiveInteger,
  observed_generation: PositiveInteger,
  deletion_timestamp: null,
  selector: [{key: Text, value: Text}],
  pod_template_labels: [{key: Text, value: Text}],
  service_account_name: Text,
  automount_service_account_token: false
}

ProviderKubernetesEndpointCallerV1 = {
  role: "serve_lb_slot_0" | "serve_lb_slot_1",
  namespace: Text,
  namespace_uid: Text,
  pod_selector: [{key: Text, value: Text}],
  service_account_name: Text,
  service_account_uid: Text,
  workload: ProviderKubernetesEndpointCallerWorkloadV1
}

ProviderKubernetesEndpointContractV1 = {
  mode: "podip",
  application_port: DecimalPortText,
  ambient_fallback: false,
  prerequisite_projection: [ProviderKubernetesPrerequisiteV1],
  required_callers: [ProviderKubernetesEndpointCallerV1]
}

ProviderAuthorityWorkerImageV1 = {
  qualification: ProviderOCIImageQualificationV1,
  runtime: ProviderRuntimeImageIdentityV1
}

ProviderAuthorityWorkerCohortManifestV1 = {
  version: 1,
  cohort_id: Text,
  namespace: Text,
  deployment_name: Text,
  service_account_name: Text,
  container_name: "skypilot-authority-worker",
  image: ProviderOCIImageQualificationV1,
  pod_template_contract: ProviderRepoArtifactRefV1,
  artifact_inventory: ProviderRepoArtifactRefV1,
  callable_inventory: ProviderRepoArtifactRefV1,
  claim_contract: "frozen_action_cohort_join_v1",
  handler_allowlist: ["serve_shadow_candidate_launch",
                      "serve_shadow_candidate_down",
                      "serve_resource_action_launch",
                      "serve_resource_action_down"]
}

ProviderAuthorityWorkerCohortV1 = {
  version: 1,
  manifest: ProviderAuthorityWorkerCohortManifestV1,
  manifest_sha256: Sha256,
  deployment_uid: Text,
  service_account_uid: Text
}

ProviderKubernetesControllerOwnerV1 = {
  api_version: "apps/v1",
  kind: "ReplicaSet" | "Deployment",
  name: Text,
  uid: Text
}

ProviderAuthorityWorkerIdentityV1 = {
  namespace: Text,
  pod_name: Text,
  pod_uid: Text,
  pod_resource_version: Text,
  pod_service_account_name: Text,
  pod_controller_owner: ProviderKubernetesControllerOwnerV1,
  replica_set_name: Text,
  replica_set_uid: Text,
  replica_set_resource_version: Text,
  replica_set_controller_owner: ProviderKubernetesControllerOwnerV1,
  deployment_name: Text,
  deployment_uid: Text,
  deployment_resource_version: Text,
  deployment_generation: PositiveInteger,
  deployment_observed_generation: PositiveInteger,
  pod_template_contract_sha256: Sha256,
  image: ProviderAuthorityWorkerImageV1,
  service_account_uid: Text,
  artifact_inventory_sha256: Sha256,
  callable_inventory_sha256: Sha256,
  handler_allowlist_sha256: Sha256,
  observed_at: UtcTimestamp
}

ProviderAuthorityWorkerRegistrationV1 = {
  worker: ProviderAuthorityWorkerIdentityV1,
  pod_ready: true,
  deployment_spec_replicas: 2,
  deployment_status_observed_generation: PositiveInteger,
  deployment_ready_replicas: 2,
  deployment_available_replicas: 2,
  registered_at: UtcTimestamp
}

ProviderAuthorityWorkerRegistrationSetV1 = {
  version: 1,
  cohort_identity_sha256: Sha256,
  workers: [ProviderAuthorityWorkerRegistrationV1]
}

ProviderAuthorityWorkerAttemptAttestationV1 = {
  request_id: UUID,
  request_execution_generation: PositiveInteger,
  request_worker_id: Text,
  claimed_cursor_sha256: null | Sha256,
  before: ProviderAuthorityWorkerIdentityV1,
  after: null | ProviderAuthorityWorkerIdentityV1
}

ProviderKubernetesRequestIdentityV1 = {
  cleaned_user: Text,
  original_user: Text,
  frozen_user_hash: Text
}

clean_username_for_explicit_user_v1(original_user: Text) -> Text

project_provider_kubernetes_request_identity_v1(
  original_user: Text,
  name_basis: ProviderWorkloadNameBasisV1
) -> ProviderKubernetesRequestIdentityV1

ProviderKubernetesSchedulingContractV1 = {
  node_count: 1,
  use_spot: false,
  accelerator: null,
  node_selector: [],
  allowed_nodes: [],
  avoid_accelerator_label_keys: [Text],
  runtime_class_name: null,
  priority_class_name: null,
  queue: null,
  kueue: false,
  dws: false,
  autoscaler: null,
  detected_network_type: "default"
}

ProviderKubernetesStorageContractV1 = {
  persistent_volumes: [],
  object_stores: [],
  file_mounts: [],
  workdir: null,
  fuse: false,
  docker_cache: false,
  auto_mounts: false
}

ProviderKubernetesMetadataContractV1 = {
  global_labels: [],
  custom_pod_config: null,
  custom_metadata: [],
  reserved_labels_injected_last: true
}

ProviderKubernetesSecurityContractV1 = {
  tls_material: null,
  managed_secrets: [],
  task_secrets: [],
  service_account_bootstrap: false,
  rbac_bootstrap: false
}

ProviderKubernetesObjectMutationEffectV1 = {
  sequence: 0 | 1 | 2,
  role: "head_ssh_service" | "head_service" | "head_pod",
  kind: "Service" | "Pod"
}

ProviderKubernetesLaunchMutationContractV1 = {
  role_map_contract: "ProviderKubernetesObjectRoleMapV1",
  create_effects: [ProviderKubernetesObjectMutationEffectV1],
  delete_effects: [ProviderKubernetesObjectMutationEffectV1],
  job_effect: "one_action_keyed_skylet_submit",
  allowed_patches: [],
  allowed_updates: [],
  allowed_collection_deletes: [],
  delete_requires_identity_labels_and_uid_precondition: true,
  create_409: "exact_admitted_readback_or_conflict",
  create_422: "terminal_no_rewrite"
}

ProviderKubernetesDownMutationContractV1 = {
  role_map_contract: "ProviderKubernetesObjectRoleMapV1",
  delete_effects: [ProviderKubernetesObjectMutationEffectV1],
  delete_requires_identity_labels_and_uid_precondition: true,
  cluster_record_removal: "same_uuid_exact_handle_after_absence_v1",
  allowed_creates: [],
  allowed_patches: [],
  allowed_updates: [],
  allowed_collection_deletes: []
}

ProviderKubernetesExecutionCapsuleV1 = {
  version: 1,
  implementation_contract: "kubernetes_serve_prebooted_runtime_v1",
  executor_cohort: ProviderAuthorityWorkerCohortV1,
  config_projection: ProviderKubernetesConfigProjectionV1,
  config_projection_sha256: Sha256,
  scope: ProviderKubernetesScopeV1,
  principals: ProviderKubernetesPrincipalsV1,
  prerequisites: ProviderKubernetesPrerequisiteInventoryV1,
  request_identity: ProviderKubernetesRequestIdentityV1,
  resources: ProviderKubernetesResourceContractV1,
  renderer: ProviderKubernetesRendererV1,
  objects: [ProviderKubernetesObjectPlanV1],
  post_provision: ProviderKubernetesPostProvisionV1,
  endpoint: ProviderKubernetesEndpointContractV1,
  scheduling: ProviderKubernetesSchedulingContractV1,
  storage: ProviderKubernetesStorageContractV1,
  metadata: ProviderKubernetesMetadataContractV1,
  security: ProviderKubernetesSecurityContractV1,
  topology: ProviderPodTopologyV1,
  mutation_contract: ProviderKubernetesLaunchMutationContractV1
}

ProviderLaunchPolicySubjectV1 = {
  version: 1,
  source: ProviderLaunchSourceV1,
  requested_target: ProviderLocatorV1,
  resources: ProviderPodResourceSnapshotV1,
  topology: ProviderPodTopologyV1,
  execution_capsule_sha256: Sha256,
  replica_env: {"SKYPILOT_SERVE_REPLICA_ID": DecimalIntegerText},
  security_group_scope: "not_applicable:kubernetes",
  admin_policy_mode: "absent_controller_and_executor",
  managed_secrets_mode: "absent",
  retry_until_up: Boolean,
  exact_resources_override: true,
  backend: "cloud_vm_ray",
  optimize_target: "cost",
  dryrun: false,
  no_setup: false,
  clone_disk_from: null,
  fast: false,
  file_mounts_blob_id: null,
  tls_material_ref: null
}

ProviderKubernetesExecutionConfigV1 = {
  version: 1,
  capsule: ProviderKubernetesExecutionCapsuleV1,
  execution_capsule_sha256: Sha256,
  policy_subject: ProviderLaunchPolicySubjectV1,
  policy_subject_sha256: Sha256,
  policy: {
    controller: ProviderPolicyBoundaryProofV1,
    executor: ProviderPolicyBoundaryProofV1
  }
}

ProviderKubernetesDownExecutionCapsuleV1 = {
  version: 1,
  implementation_contract: "kubernetes_serve_exact_cleanup_v1",
  executor_cohort: ProviderAuthorityWorkerCohortV1,
  config_projection: ProviderKubernetesConfigProjectionV1,
  config_projection_sha256: Sha256,
  scope: ProviderKubernetesScopeV1,
  principals: ProviderKubernetesPrincipalsV1,
  prerequisites: ProviderKubernetesPrerequisiteInventoryV1,
  cleanup_target: ProviderKubernetesCleanupTargetV1,
  cleanup_target_sha256: Sha256,
  mutation_contract: ProviderKubernetesDownMutationContractV1
}

ProviderDownPolicySubjectV1 = {
  version: 1,
  requested_target: ProviderLocatorV1,
  workspace: Text,
  prior_launch_basis_sha256: Sha256,
  cleanup_target_sha256: Sha256,
  execution_capsule_sha256: Sha256,
  admin_policy_mode: "absent_controller_and_executor",
  managed_secrets_mode: "absent",
  purge: false,
  graceful: false,
  graceful_timeout: null
}

ProviderKubernetesDownExecutionConfigV1 = {
  version: 1,
  capsule: ProviderKubernetesDownExecutionCapsuleV1,
  execution_capsule_sha256: Sha256,
  policy_subject: ProviderDownPolicySubjectV1,
  policy_subject_sha256: Sha256,
  policy: {
    controller: ProviderPolicyBoundaryProofV1,
    executor: ProviderPolicyBoundaryProofV1
  }
}

ProviderLifecycleExecutionCapsuleV1 =
  ProviderKubernetesExecutionCapsuleV1 |
  ProviderKubernetesDownExecutionCapsuleV1

ProviderLifecyclePolicyBoundaryProofV1 =
  ProviderPolicyBoundaryProofV1
```

The identity leaf remains context-free; only the pure projector grants these
relations. `original_user` must be exact nonempty ASCII text within the generic
text bound. The cleaner applies the historical algorithm in this literal order:
ASCII `A-Z` become `a-z`; bytes outside `[a-z0-9-_]` are removed; the maximal
leading run matching `[0-9-]+` is removed; exactly one final `-`, if present, is
removed; and the result is truncated to 63 ASCII bytes. An empty result or a
result that is not a canonical Kubernetes label value is not representable.
The projector accepts no `cleaned_user` or independent hash argument. It sets
`cleaned_user` only from that cleaner, copies `original_user` byte-for-byte, and
copies `frozen_user_hash` only from `name_basis.frozen_user_hash`. It performs no
environment, user-database, request-context, filesystem, network, clock, or
randomness read. The existing ambient `get_cleaned_username()` path remains
legacy-only and must delegate to the same explicit-input cleaner after choosing
its compatibility fallback.

The launch execution capsule carries the projector result. A down capsule has
no standalone `request_identity` field and invokes no identity projector: exact
cleanup uses the prior requested target, frozen cleanup object identity,
current Kubernetes principals/prerequisites, and current worker cohort, and
performs no rendering or name derivation from a current user. Its locator and
cleanup target necessarily retain immutable launch-derived name-basis hash,
labels, and Pod annotation bytes so they can identify and precondition the
exact old objects; those are prior-launch deletion evidence, not down-request
identity. A current down request's authenticated identity remains process-local
scheduling and audit context and is not provider-affecting input. The down
parser rejects a standalone `request_identity` key even when null.

The controller-owner leaf accepts either closed kind because it is shared. The
enclosing worker-identity validator requires
`pod_controller_owner.kind="ReplicaSet"` and byte-equal name/UID to the
embedded ReplicaSet fields, and requires
`replica_set_controller_owner.kind="Deployment"` and byte-equal name/UID to
the embedded Deployment fields. Swapped kinds, a direct Pod-to-Deployment
owner, multiple controller owners, or any crossed name/UID is invalid.

The object-mutation-effect leaf validates only its closed scalar union. The
enclosing launch mutation contract requires `create_effects` to be exactly
`[(0, head_ssh_service, Service), (1, head_service, Service),
(2, head_pod, Pod)]` and `delete_effects` to be exactly
`[(0, head_service, Service), (1, head_ssh_service, Service),
(2, head_pod, Pod)]`. The down mutation contract requires that same exact
delete list. No list may be empty, reordered, duplicated, extended, or contain a
role/kind mismatch. The launch/down mutation-contract validators are the sole
owners of these exact list comparisons; a capsule only requires the correctly
typed, already-validated kind-matched mutation contract.

Every prerequisite leaf dispatches through
`ProviderKubernetesPrerequisiteKindMapV1`: its outer and spec `kind` are equal,
its API version is the displayed literal, cluster-scoped kinds require
`namespace=null`, and namespaced kinds require a nonnull namespace. The stored
`spec_sha256` is recomputed from `canonical_sha256(spec.canonical_value())`.
All prerequisite kinds carry top-level `deletion_timestamp=null`; a deleting
live object is not representable before or after effects.

The leaf also dispatches through
`ProviderKubernetesPrerequisiteRoleMapV1` and requires its role/kind pair to be
one literal map entry. The enclosing inventory validator, which can see the
container, owns list position: a launch or down capsule contains exactly all 12
records in map order; no key sort, missing/extra/duplicate/swapped role, or
wrong-kind substitution is accepted. V1 always serializes every semantic role,
even when two roles name the same live object.

The Boltz-v1 alias outcome is exact. The three Namespace roles
`authority_release_namespace`, `serve_lb_slot_0_namespace`, and
`serve_lb_slot_1_namespace` are required aliases for the one Helm release
Namespace. Their canonical projections after omitting only the serialized
`role` field are byte-equal, while each retains its exact distinct role and
list position. No other
roles may alias: the target and `kube-system` Namespace records have distinct
keys/UIDs, and all four ServiceAccount roles have distinct keys/UIDs. A required
alias with unequal API version, key, UID, resourceVersion, deletion state, spec,
or spec hash rejects; the same UID under two nonaliased keys also rejects. This
alias contract is a protocol constant, not inferred from coincident names.

The launch-only endpoint `prerequisite_projection` is exactly five records in this order:
`endpoint_network_policy`, `serve_lb_slot_0_namespace`,
`serve_lb_slot_0_service_account`, `serve_lb_slot_1_namespace`, and
`serve_lb_slot_1_service_account`. Each is byte-equal, including `role`, to its
record in the enclosing 12-role inventory. The two `required_callers` remain in
slot-0/slot-1 order. Each caller's namespace/name/UID fields equal its Namespace
and ServiceAccount projection records, and its nonempty selector is the one
parsed by the NetworkPolicy contract; aliasing the two Namespace roles never
removes a projection position.

Each caller also embeds one exact live Deployment projection. Its namespace is
the caller Namespace; its name/UID is distinct from the other slot; its
`observed_generation` equals `generation`; its exact selector is byte-equal to
`caller.pod_selector`; and its complete sorted Pod-template labels contain
every selector pair. The Deployment template's
`service_account_name` equals the caller and that slot's prerequisite
ServiceAccount name, and its explicit token automount is false. Thus the
relation is closed as selector -> frozen LB Deployment -> Pod-template
ServiceAccount -> exact live ServiceAccount UID. A structurally valid caller,
ServiceAccount, or Deployment leaf does not prove this relation outside the
endpoint/enclosing-capsule validator.

The down capsule deliberately contains no current/down-request identity DTO,
endpoint contract, prerequisite projection, caller, or caller-workload
evidence. Its closed parser rejects any such key. Down retains the complete
12-role prerequisite inventory and prior-launch locator/cleanup evidence for
current scope/principal/admission and exact deletion, but it neither projects
the five endpoint roles into a second field, reads an LB Deployment, nor reads
or derives a current request user/hash for provider cleanup.

The remaining cross-field bindings are exhaustive. The authority/release
record's name equals the executor-cohort, caller-ServiceAccount, and live
authority-worker Namespace. Both LB Namespace role records are its required
aliases. For launch, both endpoint callers' `namespace`/`namespace_uid` equal
that record's name/UID; this prerequisite UID is the sole typed caller-Namespace
UID source in v1. Down has no endpoint-caller copy to bind.
For launch, `requested_target.sky_cluster_name`,
`requested_target.kubernetes.name_basis.display_name`, and the frozen
`PreparedLaunchRequest.body.cluster_name` are byte-equal. The capsule
`request_identity.frozen_user_hash`, name basis `frozen_user_hash`, and identity
proof `effective_user_hash` are byte-equal. The proof's
`effective_original_user` is byte-equal to `request_identity.original_user`,
and the complete request identity is byte-equal to the pure projector result.
The proof context input resource identity is byte-equal to the enclosing plan/invocation
resource identity; its service name equals the content source and locked
service, and `source.content.service_incarnation` equals that resource
identity's service incarnation. The context `decision_id`, `cohort_id`,
controller-owner fence, lifecycle epoch, reference revision, and capability
hash are byte-equal to the deterministic launch ID and exact retained
preparation reference; `action_type="launch"`,
`reference_state="PREPARING"`, and `preparation_reference_revision=1` at the
canonicalization boundary. Its prepared pair is byte-equal to the exact two
values in the process-local frozen body. For a legacy-shadow submission only,
request association additionally requires the proof's effective pair to be
byte-equal to the effective post-auth persisted body's `SKYPILOT_USER` and
`SKYPILOT_USER_ID`. All three object request bodies have
`metadata.labels["skypilot-user"]` byte-equal to
`request_identity.cleaned_user`; the head Pod alone has
`metadata.annotations["skypilot-user"]` byte-equal to
`request_identity.original_user`. A missing, extra-role, unequal, or
post-proof rewritten identity field in the frozen candidate is not
representable. Post-canonicalization auth drift on the already-submitted
legacy-shadow request is instead write-once `IDENTITY_MISMATCH` divergence.
Down has no standalone request-identity field, projector invocation, or
current-user read; it does retain the prior frozen object metadata and name
basis that exact deletion must match.
The target record equals `scope.namespace`/`target_namespace_uid`, config and
resource namespaces, every object-plan namespace, workload-ServiceAccount and
rules-review Namespace, and the NetworkPolicy Namespace. The kube-system record
has name exactly `kube-system` and the scope's kube-system UID. Caller and
workload ServiceAccount records are byte-equal to `principals.caller` and
`principals.workload`, match their scope fields, and respectively match the
cohort ServiceAccount and Pod `/spec/serviceAccountName`; caller automount is
true and workload automount is false. Both LB ServiceAccounts have explicit
automount false and empty image-pull/legacy-secret references.

For launch, the NetworkPolicy manifest selects the exact target Pod identity,
allows only the two LB role namespace/Pod selectors to the application port and
the authority-worker namespace/selector to management port 46590, and contains
no extra ingress path. NetworkPolicy does not select by ServiceAccount; each
launch caller's exact live Deployment projection supplies the selector-to-
ServiceAccount binding, and the Namespace/ServiceAccount records supply its live
UID attestation. Down still exact-compares the content-addressed prerequisite to
the same live NetworkPolicy but does not derive a caller projection from it. The
ValidatingAdmissionPolicy binds the exact authenticated caller and frozen
workload/object contract. Its binding names that exact policy and target
Namespace. The access-matrix artifact contains the literal GET decisions for
all 12 semantic prerequisite roles; only the launch artifact additionally
contains the two exact LB Deployment GETs. Preflight and each execution session
use the same live client to exact-read, normalize, and revalidate all 12 records
before and after effects. Launch also reads both Deployment projections in that
window; down performs zero LB Deployment GETs. An implementation may coalesce
the one required Namespace alias GET but must reproduce three records equal after
omitting only their distinct exact `role` fields.

For a ServiceAccount, outer namespace/name/UID/resourceVersion are byte-equal
to its embedded projection. For Namespace, its live metadata name/UID/
resourceVersion populate the outer record and its sorted labels/annotations
populate the spec.

`ProviderKubernetesEndpointCallerWorkloadV1` is produced only from one exact
AppsV1 Deployment GET. Its leaf validates the displayed literals, nonempty
bounded names/UID/resourceVersion, positive generations, null deletion
timestamp, sorted duplicate-free nonempty selector, sorted duplicate-free
complete Pod-template labels, nonempty ServiceAccount name, and explicit false
token automount. A usable projection additionally requires
`observed_generation == generation`; raw Deployment JSON, containers, status
conditions, and unrelated metadata are not persisted. Preflight and execution
reconstruct this same projection from the live Deployment and apply the
endpoint cross-field bindings above.
This leaf and live projection path are launch-only; down cannot carry a
structurally valid workload leaf as unused evidence.

The manifest-backed contract-to-normalizer map is literal:
`serve_action_network_policy_v1` uses
`serve_action_network_policy_live_projection_v1`,
`serve_action_validating_policy_v1` uses
`serve_action_validating_policy_live_projection_v1`, and
`serve_action_validating_binding_v1` uses
`serve_action_validating_binding_live_projection_v1`. These checked-in
normalizers are separate callable-inventory roles even though they share the
following v1 transform. Each requires an object root, removes exactly top-level
`status` and metadata `uid`, `resourceVersion`, `generation`,
`creationTimestamp`, `deletionTimestamp`, and `managedFields`, and preserves
every other key and array order. It inserts only a canonical
`metadata.namespace`: absent or JSON null becomes null for a cluster-scoped
kind, while a namespaced kind requires and preserves the exact nonnull outer
namespace. An empty-string namespace is invalid. The expected artifact must
omit the removed server-owned fields and `status`; every retained API default
must therefore be explicit in that artifact or comparison fails.

Preflight and the execution session each load and hash/size-check the referenced
artifact, then exact-read the named live object. Before normalization, both
require the artifact and live object API version, kind, normalized namespace,
and name to equal the outer record; the live UID and resourceVersion must be
byte-equal to the outer values, and live `metadata.deletionTimestamp` must be
absent or null. They then require the canonical bytes of
`contract_normalize(artifact)` to equal the canonical bytes of
`contract_normalize(live_object)`. The pure prerequisite
leaf validates the spec hash, kind/version/scope dispatch, internal identity,
null deletion timestamp, and content-addressed reference only; it does not read
an artifact or claim the live proof.

`ProviderKubernetesRendererV1` validates only its fixed `contract`, five typed
artifact references, and the shape of its typed launch source; it performs no
cross-field or artifact-content comparison. The inventory-role map is exact:
`outer_template -> renderer.outer_template`,
`node_fragment -> renderer.node_fragment`,
`binding_schema -> renderer.binding_schema`,
`config_access_inventory -> renderer.config_access_inventory`, and
`admitted_object_normalization -> renderer.admitted_object_normalization`.
The expected cohort manifest and preflight bind each complete reference to the
corresponding distinct role/path/size/hash in the approved artifact inventory;
the references are not interchangeable merely because each is structurally
valid. The execution session resolves and revalidates those same bindings and
applies the template/schema/config-access/normalization artifact according to
its named role.

The launch capsule owns only comparisons available inside it:
`renderer.source == post_provision.job_submission.run_source`, complete
byte-equality of renderer/config-projection `config_access_inventory`, and
complete byte-equality of every object-plan `normalization_profile` with
`renderer.admitted_object_normalization`. Preflight and the execution session
own the inventory binding and artifact execution checks above.

A down capsule deliberately has no renderer. Its preflight and execution session
resolve `capsule.config_projection.config_access_inventory` directly against the
unique `config_access_inventory` role in the content-addressed approved artifact
inventory named by `capsule.executor_cohort.manifest.artifact_inventory` and
require the complete `ProviderRepoArtifactRefV1` values to be byte-equal. A
missing, duplicate, crossed-role, or unequal approved entry is not representable.
This is the sole down config-access-inventory binding; no absent renderer field
is inferred or reconstructed.

Policy subjects are never accepted as self-consistent caller-selected
preimages. Two pure nonrecursive projectors are the only constructors:

- `project_provider_launch_policy_subject_v1(resource_identity, source,
  requested_target, resources, topology, replica_id, retry_until_up, capsule)`
  takes exactly those variable fields, converts the replica ID to its canonical
  one-entry environment, sets `security_group_scope`, both policy/secret modes,
  `exact_resources_override`, backend, optimize target, every fixed launch
  flag, mount-blob field, and TLS-material field to the displayed protocol
  literals, and computes `execution_capsule_sha256` from the full capsule. The
  replica value also equals `resource_identity.replica_id`, and both capsule
  source copies equal `source`. A resource-identity replica ID that cannot be
  represented by `DecimalIntegerText` rejects rather than truncating or changing
  spelling. Neither the controller nor worker call includes or dereferences
  `launch.execution_config`.
- `project_provider_down_policy_subject_v1(requested_target, workspace,
  prior_launch_basis, prior_cleanup_target, capsule)` takes exactly those
  variable fields; sets the policy/secret modes and purge/graceful fields only
  to the displayed literals; computes `prior_launch_basis_sha256` from the
  complete typed basis; computes the input cleanup-target hash from the complete
  `prior_cleanup_target`; and computes `execution_capsule_sha256` from the full
  capsule. The outer nonnull `prior_cleanup_target`, the basis's complete
  `launch_cleanup_target`, the preflight cleanup target, and the capsule cleanup
  target are byte-equal. The basis's `launch_cleanup_target_sha256`, preflight
  `cleanup_target_sha256`, capsule `cleanup_target_sha256`, and projected-subject
  `cleanup_target_sha256` all equal that recomputed input hash. No input includes
  or dereferences `down.execution_config`.

Execution-config/spec admission calls the kind-matched projector and requires
its complete canonical result to be byte-equal to the embedded policy subject.
It also requires every projector input to equal the corresponding plan,
invocation, retained basis, and preflight field. For launch, it additionally
compares the outer invocation field-for-field with the projected subject:
source, requested target, resources, topology, replica environment,
security-group scope, policy and managed-secret modes, retry flag, exact-resource
override, backend/optimizer, every boolean launch option, clone-disk value,
mount-blob value, and TLS-material value must equal the projector input or its
displayed fixed result. The invocation's execution-config field is compared to
the complete enclosing config but is not a projector input. For down, requested
target, workspace, cluster name, expected cluster-record UUID, complete prior
basis, purge/graceful flags, and timeout are bound to the plan, cleanup target,
projected subject, and enclosing config; both basis and cleanup hashes are
recomputed from their complete typed preimages. A self-consistent subject/hash
graph with one changed target, retry flag, replica value, option, cleanup target,
cluster identity, or prior-basis hash is therefore invalid.

Every null, empty collection, and literal in
`ProviderKubernetesExecutionConfigV1` is semantic; execution may not replace it
with an ambient default. Every renderer, binding, normalization, inventory, and
runtime artifact has a retrievable repository path, byte size, and hash within
the exact approved executor-cohort image; no bare private hash is a preimage.
The action-aware candidate renderer is pure over its policy-free capsule plus the retained
source. It emits exactly the three stored `objects` bodies; it does not call the
generic config writer/bootstrap path or rediscover user/workspace, image,
CPU/GPU labels, RuntimeClass, storage, queue, service account, SSH identity,
port mode, pod config, mounts, or credentials.
If the current template cannot be reconstructed under those constraints, the
decision is not representable and remains shadow-only.

The split is intentionally nonrecursive and content-addressed within one closed
envelope. The `ProviderPolicyBoundaryProofV1` leaf validates only its closed
shape, fixed boundary/mode literals, scalar hashes,
`projection_before_sha256 == projection_after_sha256`, and
`projections_equal=true`. It deliberately cannot compare its hash scalars to a
capsule, config projection, or policy-subject preimage.

The enclosing launch/down execution-config validator owns the complete graph.
It recomputes `capsule.config_projection_sha256` from the co-located config
projection; recomputes `execution_capsule_sha256` from the capsule and requires
the policy subject's capsule hash to equal it; recomputes
`policy_subject_sha256` from the one co-located subject; and requires every
proof config-projection hash, subject hash, and before/after hash to equal those
respective results. The controller proof occupies only the
`serve_controller_prepare` slot and the executor proof only the
`api_executor_pre_io` slot; crossed or duplicate boundary literals reject.
Thus every nonsecret preimage is co-located exactly once rather than repeating
the capsule and object bodies four times. A bare hash or valid proof leaf
outside this typed enclosing config remains non-authoritative. Raw effective
config, raw tasks, admin-policy output, and managed-secret responses are
forbidden. The checked-in config-access inventory is the finite list of reads;
an unlisted read or a value outside the closed projection makes the candidate
not representable.
For launch only, `capsule.config_projection.config_access_inventory` and
`capsule.renderer.config_access_inventory` are two bindings to that one artifact
and must be byte-equal as complete `ProviderRepoArtifactRefV1` objects. Each leaf
validates only its closed artifact reference; the enclosing launch capsule owns
this co-located cross-field equality. Down uses the direct approved-inventory
binding above instead.

Down never inherits execution authority from the prior launch. At down
admission, the same private preflight selects the then-active versioned worker
cohort and constructs a current `ProviderKubernetesDownExecutionConfigV1` from
the frozen cleanup target plus current typed scope, principals, access matrix,
prerequisites, and policy-absence evidence. That complete config is immutable
for the down action and every retry. The request handler revalidates it through
the same one-client facade before and after I/O. A retained launch basis
supplies target evidence only; it cannot supply an obsolete worker identity,
ambient config, or security authority.
The down capsule's scope/namespace, cleanup target/hash, three object plans,
cluster UUID, workspace/config projection, principals, prerequisites, cohort,
policy subject, and two boundary proofs are exhaustively byte-equal to the down
invocation, plan, preflight result, and retained target fields; any
contradiction is not representable.

SelfSubjectReview, RulesReview, and AccessReview responses are immediately
normalized into the named nonsecret types above. The action-kind-selected
access-matrix artifact contains the exact ordered required and forbidden checks;
check sequences must match it. Wildcard groups/resources/verbs, unknown
nonresource URLs, extra
identity groups/keys, incomplete rules, evaluation errors, and any result that
differs from `expected_allowed` are rejected. Kubernetes reason/error strings
and raw review bodies are never persisted. Prerequisite manifests are loaded
from their content-addressed artifacts and byte-compared to the typed live
projection; arbitrary Kubernetes response JSON is not stored.

Launch cross-field validation is exhaustive. Both resource/image copies and the
Pod container image are byte-equal. `source_cpus` and `source_memory_gb` are
nonnull canonical positive decimals with at most three fractional digits and
no `+`, relative `x`, exponent, or unit suffix. The candidate requires
`instance_type == source_cpus + "CPU--" + source_memory_gb + "GB"`,
`pod_cpu_request == pod_cpu_limit == source_cpus`, and
`pod_memory_request == pod_memory_limit == source_memory_gb + "G"`; those four
strings are byte-equal to the normalized Pod spec. The live allocatable clamp
is disabled and cannot rewrite them. Scope/namespace/fingerprint, every
prerequisite Namespace and ServiceAccount role, name basis and all object names,
each role's final
labels against that role's topology and request body,
topology order, source/workspace, image/digest/pull policy, and the one port in
the invocation/resource snapshot, capsule resource contract, topology,
endpoint contract, and parsed workload NetworkPolicy must also be byte-equal.
Those are the only execution-capsule application-port copies: the value is
absent from the Pod container-port set and both Service port lists. Every
duplicated cluster UUID and replica incarnation is equal to the enclosing
identity. The separate fixed Skylet management port must equal
`post_provision.management_port="46590"`, Pod
`/spec/containers/0/ports/4/containerPort=46590`, and the parsed workload
NetworkPolicy rule; it is never exposed as a user resource port. Preflight and
the execution session must resolve and parse the NetworkPolicy artifact because
a structurally valid artifact reference cannot prove either port. A
contradictory but individually canonical object is rejected.

The workload-image digest is allowlisted only after a checked-in build proves
that its entrypoint contains the exact SkyPilot runtime bundle, Ray, Skylet,
startup probe, and job-queue protocol named by `post_provision`. Generic
`post_provision_runtime_setup()`, wheel upload, SSH, kubectl `exec`/`cp`, workdir
or file sync, task setup, hooks, and autostop are hard failures; the normal
SYNC/SETUP/PRE_EXEC stages are recorded as asserted no-ops rather than silently
skipped. The startup probe succeeds only when every expected workload artifact
measurement and Ray/Skylet health check is exact. The action then reaches the service command solely
through the action-keyed Skylet protocol. There is no private-key locator,
ambient SSH credential, or fallback to ordinary backend execution in v1.

After `HANDLE_INTENT`, the action-aware cluster-row UUID primitive persists
the exact `ProviderKubernetesHandleV1`. Its provider block contains the frozen
in-cluster scope, target namespace, `podip`, `use_internal_ips=true`, one port,
all three object UIDs, the write-once scheduler node name, and the current IP
read from the same-UID Pod. The
compatibility `CloudVmRayResourceHandle` is a deterministic projection of this
object; no cluster YAML, ambient context/namespace, or later config lookup may
fill a missing field. `get_endpoints()` for this profile consumes only that
provider block, revalidates Pod UID before refreshing its IP, and never falls
back to display name or ambient provider config. A crash before the progress
advance exact-adopts the same-UUID/byte-equal handle; a null/different UUID or
different handle blocks. No cross-database atomicity is assumed.

The frozen endpoint prerequisites include the exact NetworkPolicy allowing the
two warm-standby load-balancer namespace/Pod selectors to the one application
port and allowing the authority-worker selector to the Skylet management port
only. The associated namespace and ServiceAccount UIDs are additional caller
attestation; NetworkPolicy itself is not claimed to select by ServiceAccount.
All policy, selector, and caller evidence is revalidated. Provider action
success requires endpoint resolution, not application health. The normal Serve
readiness probe later uses that exact endpoint; promotion additionally requires
a live smoke from both active/standby load-balancer slots after `JOB_RUNNING`.

A checked-in JSON inventory records the exact template path/size/hash, renderer
binding schema, reviewed qualified-function call graph, every config key,
environment/file/global-user-state/Kubernetes-discovery input with disposition
`embedded`, `content_addressed`, `fixed`, or `forbidden`, and the exact CoreV1
read/create/delete allowlist. An AST guard fails on an unlisted
`skypilot_config` access, environment/file/global-state read, provider
discovery, renderer binding, mutation call, or custom provisioner/template.
The eligible runtime consumes the explicit object rather than consulting that
inventory as a source of defaults. Controller preparation and the executor
immediately before mutation both recalculate the inventory/template/callable
fingerprints and require the approved cohort deployment UID,
service-account UID, and image digest. Each execution additionally records its
typed `ProviderAuthorityWorkerAttemptAttestationV1`. Using downward-API
name/namespace/UID, it exact-reads its Pod, requires its sole controller owner
to be the byte-equal typed ReplicaSet, then requires that ReplicaSet's sole
controller owner to be the byte-equal frozen Deployment. Pod,
ReplicaSet template, and Deployment template must agree on the service account,
digest image, handler allowlist, and pod-template contract. The named container
status must expose the qualified runtime image ID, and checked-in image
qualification must map the OCI manifest to the expected OCI config digest; a
CRI `imageID` is never blindly equated with a manifest digest. The handler
requires `worker_identity.image.qualification` to be byte-equal to the frozen
cohort manifest's image, and requires its runtime identity to map back through
that same artifact. The handler claim-fenced-writes `claimed_cursor_sha256` and `before`
after claim and before the first mutating external effect. Immediately before
every CoreV1 or Skylet effect it
re-reads the full Pod -> ReplicaSet -> Deployment chain and requires every
identity field except the fresh `observed_at` to be byte-equal to `before`.
Immediately after every effect it re-reads the same chain: the first such read
fills `after` write-once, and later reads must equal that stored identity except
for a later `observed_at`. The before/after identity fields must equal each
other, and final success requires nonnull `after`. Within one execution
generation the attestation can change only from `after=null` to that one exact
post-effect identity. A new execution generation may replace the attempt-scoped
attestation, bind it to the carried cursor, and reconcile that cursor, but it
cannot repeat a committed effect. An unattested worker rejects; a replacement
Pod in the same frozen cohort may recover.

`manifest.cohort_id` names an immutable versioned Deployment, not a mutable
release-wide slot. Multiple cohort Deployments may run concurrently. The
action's frozen resolved cohort alone may claim it; a newer active cohort cannot
claim or reinterpret an older action. An old cohort remains deployed while any
nonterminal authoritative action or private shadow request/coverage attempt
references its resolved cohort identity. Template/image changes therefore
create a new cohort instead of invalidating in-flight launch or down recovery.

`claimed_cursor_sha256` is null exactly when the request claimed a fresh
attempt whose provider progress was null; otherwise it equals the canonical
hash of the cursor read under that claim. The first intent may create the
envelope and attestation together, but no mutating provider byte can precede that
claim-fenced commit.

An execution generation that reaches success by read-only adoption and emits
no mutation writes the complete byte-equal `before`/`after` pair with its
claim-fenced terminal observation checkpoint; the two worker-chain reads bracket
that target readback. Thus `after` is still nonnull at success without
inventing an effect. Any generation that does emit an effect must persist
`before` first and complete `after` immediately after the first effect as above.

Before action-specific preparation, the controller selects only the rendered
active manifest whose complete resolved identity is registered as `ACCEPTING`
in the Serve033 worker-cohort registry. A suffix-only PostgreSQL transaction
locks that cohort and inserts or exactly adopts the decision's nonexecuting
`PREPARING` retention reference, then releases every lock before the network
preflight. The authority-worker cohort created its registry identity during
rollout by self-attesting the same projected static manifest plus its live
Deployment and ServiceAccount UIDs. The first worker inserts `REGISTERING`; each
distinct ready Pod exactly adopts the immutable identity and appends its closed
registration evidence. Only the typed two-worker gate changes it to
`ACCEPTING`. The action-specific preflight response's `resolved_cohort` must be
byte-equal to that registry identity. The reference authorizes no claim or I/O;
it only prevents retirement while prepared work is unadmitted.

Because a remote Serve controller cannot attest executor-local config or an
in-cluster target, preparation uses this closed synchronous protocol:

```text
ProviderLaunchPreflightSeedV1 = {
  version: 1,
  resource_identity: ProviderLifecyclePlanV1.resource_identity,
  workspace: Text,
  source: ProviderLaunchSourceV1,
  requested_target: ProviderLocatorV1,
  requested_cloud: "kubernetes",
  context_mode: "in_cluster",
  target_namespace: Text,
  resources: ProviderPodResourceSnapshotV1,
  topology: ProviderPodTopologyV1,
  replica_id: NonnegativeInteger,
  retry_until_up: Boolean,
  request_identity: ProviderKubernetesRequestIdentityV1,
  config_projection: ProviderKubernetesConfigProjectionV1
}

ProviderDownPreflightSeedV1 = {
  version: 1,
  resource_identity: ProviderLifecyclePlanV1.resource_identity,
  workspace: Text,
  requested_target: ProviderLocatorV1,
  prior_launch_basis: PriorLaunchBasisV1,
  prior_launch_basis_sha256: Sha256,
  cleanup_target: ProviderKubernetesCleanupTargetV1,
  cleanup_target_sha256: Sha256,
  context_mode: "in_cluster",
  config_projection: ProviderKubernetesConfigProjectionV1
}

ProviderLifecyclePreflightSeedV1 =
  ProviderLaunchPreflightSeedV1 | ProviderDownPreflightSeedV1

ProviderAuthorityPreflightRequestV1 = {
  version: 1,
  contract: "provider_kubernetes_preflight_v1",
  action_kind: "launch" | "down",
  nonce: UUID,
  seed: ProviderLifecyclePreflightSeedV1,
  expected_cohort_manifest: ProviderAuthorityWorkerCohortManifestV1,
  request_sha256: Sha256
}

ProviderLaunchAuthorityPreflightResponseV1 = {
  version: 1,
  contract: "provider_kubernetes_preflight_v1",
  action_kind: "launch",
  nonce: UUID,
  request_sha256: Sha256,
  disposition: "complete" | "not_representable",
  reason: null | ProviderLaunchNotRepresentableReasonV1,
  resolved_cohort: null | ProviderAuthorityWorkerCohortV1,
  execution_capsule: null | ProviderKubernetesExecutionCapsuleV1,
  executor_policy_proof: null | ProviderPolicyBoundaryProofV1,
  worker_identity: null | ProviderAuthorityWorkerIdentityV1
}

ProviderDownAuthorityPreflightResponseV1 = {
  version: 1,
  contract: "provider_kubernetes_preflight_v1",
  action_kind: "down",
  nonce: UUID,
  request_sha256: Sha256,
  disposition: "complete" | "not_representable",
  reason: null | ProviderDownNotRepresentableReasonV1,
  resolved_cohort: null | ProviderAuthorityWorkerCohortV1,
  execution_capsule: null | ProviderKubernetesDownExecutionCapsuleV1,
  executor_policy_proof: null | ProviderPolicyBoundaryProofV1,
  worker_identity: null | ProviderAuthorityWorkerIdentityV1
}

ProviderAuthorityPreflightResponseV1 =
  ProviderLaunchAuthorityPreflightResponseV1 |
  ProviderDownAuthorityPreflightResponseV1
```

Registration workers are canonically sorted by distinct Pod UID. A
`REGISTERING` row permits one or two current entries; `ACCEPTING` requires
exactly two. Every worker identity must match the row's complete cohort,
manifest/artifact/callable/handler hashes, Deployment observed generation, and
ServiceAccount UID. Registration evidence must be fresh against PostgreSQL time
at the transition: both each registration's `registered_at` and its embedded
worker identity's `observed_at` must be at or before the transaction's fresh
`clock_timestamp()` and no more than five minutes old. This server-owned bound
is not configurable in M2/M3. Unknown, stale, duplicate, unready, or mixed
evidence cannot activate the cohort.

Only `complete` has all four evidence fields; only `not_representable` has a
reason and all four evidence fields null. The `action_kind` discriminator
selects the kind-specific reason constructor, seed, capsule, subject projector,
and enclosing proof bindings; a spelling shared by both reason enums is still
decoded through that selected constructor. The proof leaf type itself is common
to launch and down. Launch produces the launch execution config; down produces
the current down execution config and never copies the launch capsule. Any
wrong-kind reason, seed, or capsule, or any common proof whose hashes do not bind
the selected capsule/subject, is invalid rather than coerced into the other
variant. The hash covers the request without its hash field. Nonce and transport
envelope are process-local and absent from the action spec.

The two boundary proofs have one closed construction sequence:

1. After the controller's final policy/managed-secret boundary, it freezes the
   complete kind-specific seed. Launch includes the full requested target and
   `retry_until_up` plus the pure projected request identity; down includes the
   full prior-launch basis and cleanup target beside their recomputed hashes,
   but no current/down-request identity DTO. Prior launch-derived target and
   deletion bytes remain. Every seed field is byte-equal to the outer
   plan/invocation projector input.
2. The authority preflight validates that seed, constructs the kind-matched
   capsule, runs the same pure subject projector, and returns only an
   `executor_policy_proof` whose boundary is exactly `api_executor_pre_io` and
   whose hashes bind that capsule projection and projected subject. A controller
   proof in this response, or an executor field with the controller boundary,
   is invalid. For launch it also validates the retained identity proof and
   recomputes request identity solely from
   `seed.source.identity_canonicalization.effective_original_user` and
   `seed.requested_target.kubernetes.name_basis`; the proof's effective hash
   must equal the name basis hash, and the full result must be byte-equal to the
   seed and capsule copies before rendering. Down has no projector invocation
   or standalone identity copy.
3. In the same bounded preparation cell, the controller recomputes the capsule
   and subject hashes, reruns the projector against its retained seed, and
   constructs the only `serve_controller_prepare` proof from its byte-equal
   before/after subject projection and absent modes. It then assembles the
   execution config with that local controller proof and the response's exact
   executor proof; neither side supplies the other's slot.
4. Immediately before the first provider-I/O intent, the frozen-cohort handler
   reconstructs the current capsule/config projection and kind-matched subject
   from the immutable spec and one live client, recomputes an
   `api_executor_pre_io` proof, and requires it to be byte-equal to the stored
   preflight proof. Drift blocks before I/O. The controller proof is hash-
   revalidated but never regenerated by the handler. A launch handler also
   validates the immutable source proof, then reprojects the immutable
   capsule's identity from the proof's effective original user and the
   immutable name basis and rejects drift before committing an effect intent;
   it does not inspect handler request identity. A down handler has no current-
   identity projection, comparison, or user lookup.

The launch/down seed parsers recompute every content hash, including full prior
basis and cleanup target, before any projector runs. This sequence is the sole
path to the dual-proof config; ambient reconstruction or an undifferentiated
proof is not allowed.

The expected cohort manifest contains only values knowable from the rendered
release: names, qualified image identity, inventories, template contract,
claim contract, and handler allowlist. Generated Deployment and ServiceAccount
UIDs are deliberately absent. Helm renders the canonical manifest into an
immutable, read-only projected file mounted by controllers and the matching
worker cohort; neither side performs a runtime ConfigMap GET. A complete
response's `resolved_cohort.manifest` must be byte-equal to the request's
manifest, and its `manifest_sha256` must recompute from those bytes. The
authenticated worker identity must prove that the serving Pod is owned by the
returned Deployment UID and runs under the returned ServiceAccount UID. The
execution capsule's `executor_cohort` must equal the returned resolved cohort.
Any mismatch is not representable and is never normalized away.

The controller may use generated UIDs as preparation/execution evidence only
from this authenticated live response. The registry copy is a retention and
equality fence; it cannot populate a response or execution capsule. The
controller uses the returned UIDs only in the same bounded preparation cell and
freezes the complete resolved cohort into an admitted action or private shadow
record. The response is bound to its nonce, request hash, action kind, and
expected manifest and cannot be replayed for another preparation.

The exact endpoint is
`POST https://<release>-resource-action-authority.<release-namespace>.svc:46583/internal/resource-actions/v1/kubernetes/preflight`.
It is served by a read-only extension of the authority executor's role-health
supervisor, not public FastAPI and not a request handler. The endpoint imports
only preflight construction and no action submit/session mutation callable.
Request and response are canonical JSON capped at 65,536 bytes; redirects are
disabled; connect timeout is one second and total timeout five seconds. The
controller may retry once after 100 ms with the identical nonce/body only for a
connection reset, timeout, or 502/503/504. It never retries a 2xx/4xx response.
Exhaustion, malformed bytes, nonce/hash mismatch, mixed cohort identity, or an
unequal capsule is not representable and never enqueues fallback work.

Transport authentication uses the existing file-backed token-ring parser and
constant-time comparison with a distinct
`SKYPILOT_RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE`; API-user, LB-sync,
controller-admin, and data-plane tokens are forbidden. A purpose-specific TLS
Secret supplies `tls.crt`, `tls.key`, and `ca.crt`, with a SAN for the exact
Service DNS. Authority workers mount cert/key/CA and controllers mount only CA;
both reread token files for rotation. Helm refuses authority enablement without
the two named existing Secrets. A ClusterIP Service selects only ready
authority-worker Pods, has no Ingress/LoadBalancer, and exposes only 46583. A
NetworkPolicy admits that port only from controller-role Pods in the release
namespace; an existing controller egress policy receives the reciprocal rule
without replacing its other egress.

The preflight endpoint itself creates no API request, queue row, lease, or
durable state; the caller's already-committed `PREPARING` retention reference is
outside that endpoint and carries no execution authority. The endpoint returns
only the action-kind-matched capsule, executor policy proof, and current worker
identity, never credentials or a live client. The later request handler
independently reconstructs and byte-compares the result with its one mutation client
immediately before I/O. Preflight is admission evidence, not mutation authority
or a TOCTOU substitute.

The same live execution client revalidates the exact 12-role prerequisite
inventory, `/version`, and the worker Pod/ReplicaSet/Deployment chain immediately
before the first effect and after the last. Launch additionally revalidates both
frozen LB Deployment projections; down makes no such GET and rejects endpoint
evidence in its spec. The applicable reads require exact UID, resourceVersion,
typed spec/artifact
preimage, image qualification, and canonical hash. SelfSubjectReview must
produce the closed frozen identity; SelfSubjectRulesReview must be complete and
byte-equal to its typed preimage; and every required/forbidden access decision
must equal the pinned matrix. Workload token automount, image pull secrets, and
legacy secret refs remain exactly false/empty. Drift is conflict, and post-admission Pod normalization
independently rejects any injected token volume, imagePullSecret, sidecar, or
other field.

For the first cohort, both the controller/client and executor/server admin
policy are proven absent, not merely projection-preserving. Managed-secret
resolution is also proven absent. The one bounded nonsecret policy-subject
preimage is co-located in the execution config; each boundary records only its
hashes and absence modes. The boundary leaf proves local before/after hash
equality, while the enclosing execution-config validator recomputes the subject
and projection hashes and binds both boundary slots to those preimages.
The preparation trace proves the DAG is byte-exactly reconstructible from the
retained source after only the frozen exact-resource and replica-environment
transforms; setup, additional environment, hook, secret, mount, storage,
service, resource, or option differences are not representable.

In M2, the ordinary legacy API handler still traverses
`execution.launch()`'s final server policy boundary. A read-only shadow hook
immediately after that boundary and managed-secret resolution recomputes the
closed projection/config. A mismatch marks the same parent unsupported and
promotion-blocking through its terminal
`parity_class=UNSUPPORTED_PROVIDER_PROFILE` (it does not rewrite immutable
coverage or parent inputs), but does not submit a second call or suppress the
one legacy mutation. Provider-byte parity comes only from the actual CoreV1/
Skylet effect trace defined above. In M4, the internal action handler calls a dedicated prepared
execution seam that consumes the frozen object and deliberately bypasses
policy reapplication, managed-secret resolution, generic bootstrap, and
ambient config. That seam is inaccessible to ordinary public requests and
revalidates every fingerprint immediately before provider bytes.

`security_group_scope` is exactly `not_applicable:kubernetes` for this profile.
For an already Kubernetes-pinned candidate,
`_scope_security_group_to_service()` must take a literal no-op branch and a
byte comparison proves no task change. The existing AWS-only rewrite remains
for other legacy inputs. Any source/policy-provided security-group, custom
network, or cluster-config override is not representable. TLS modes that inject
material are not representable. The non-material `off` and `unverified` modes
both normalize to `tls_material_ref=null`; the load-balancer's TLS choice is
outside the Kubernetes resource mutation fingerprint.

The first normalizer is deliberately candidate-only. It accepts exactly one
task, one node, one location-pinned resource (`exact_resources_override=true`),
one in-cluster non-spot Kubernetes context, explicit nonnull CPU and memory,
one CPU-only resource with no accelerator, an explicit anonymous digest-pinned
prebooted OCI image, exactly one application port with
`kubernetes.ports=podip`, and the exact reviewed direct-Pod/two-Service
topology. A tag, omitted/default image, credentialed
image, unresolved catalog value, or changed runtime-image contract is not
representable. Deployment, worker, PVC/volume/storage, ephemeral-storage,
FUSE, host-network, custom pod/network metadata, custom provisioner/template or
plugin, compound topology, bootstrap mutation, secrets, managed-secret refs,
local mounts/workdirs, a nonnull mount blob, credentials, and every nondefault
or unrepresented launch/resource field are not representable. Setup, hooks,
autostop, a run source outside the approved nonsecret content-addressed canary
contract, SSH/private-key dependency, and workload service-account token
automount are also rejected. All 12 semantic prerequisite roles, including the
required release/LB Namespace aliases, four distinct ServiceAccounts, admission
objects, NetworkPolicy, dual load-balancer caller identities, and executor
cohort must already exist with frozen UIDs/specs, and the caller must satisfy
the complete authorization matrix. An unpinned request is
not provider-final because the server optimizer may still choose a different
resource; it cannot produce a v1 invocation.

Normalization returns one closed internal result rather than manufacturing a
partial invocation:

```text
ProviderLaunchNormalizationResultV1 =
  PreparedProviderLaunchV1 {
    sdk_request: process-local immutable PreparedLaunchRequest,
    invocation: ProviderLaunchLifecycleInvocationV1
  }
| ProviderLaunchNotRepresentableV1 {
    reason: ProviderLaunchNotRepresentableReasonV1
  }

ProviderLaunchNotRepresentableReasonV1 =
  "request_contract" | "secret_or_tls_material" |
  "source_mismatch" | "policy_configured_or_mutated" |
  "managed_secrets" | "multi_task" | "multi_node" |
  "multi_resource" | "mount_or_storage" | "non_kubernetes" |
  "spot" | "non_direct_pod_topology" | "port_contract" |
  "reserved_label_collision" | "mutable_image" |
  "custom_provider_implementation" |
  "preflight_unavailable_or_invalid" |
  "authority_worker_attestation" |
  "authorization_or_principal_drift" |
  "prerequisite_or_network_drift" |
  "admitted_object_contract" | "runtime_or_job_contract" |
  "unrepresented_execution_config" |
  "unrepresented_resource" | "unfrozen_placement" |
  "unfrozen_identity" | "unfrozen_kubernetes_scope" |
  "target_mismatch"

ProviderDownNotRepresentableReasonV1 =
  "request_contract" | "prior_launch_basis" | "target_mismatch" |
  "preflight_unavailable_or_invalid" | "authority_worker_attestation" |
  "authorization_or_principal_drift" |
  "prerequisite_or_network_drift" | "policy_configured_or_mutated" |
  "unrepresented_execution_config" | "unfrozen_kubernetes_scope"

ProviderLifecyclePreflightNotRepresentableReasonV1 =
  ProviderLaunchNotRepresentableReasonV1 |
  ProviderDownNotRepresentableReasonV1

ProviderDownNormalizationResultV1 =
  PreparedProviderDownV1 {
    sdk_request: process-local immutable PreparedDownRequest,
    invocation: ProviderDownLifecycleInvocationV1
  }
| ProviderDownNotRepresentableV1 {
    reason: ProviderDownNotRepresentableReasonV1
  }
```

Launch normalization uses one immutable `PreparedLaunchRequest`, one exact
server-effective identity proof for that resource identity, and no ambient
identity source. It requires exact nonempty built-in strings at
`body.env_vars[SKYPILOT_USER]` and `body.user_hash`, requires them to equal the
proof context input, and requires the proof/context/reference/resource-identity
bindings above. It
constructs `ProviderWorkloadNameBasisV1` from `body.cluster_name` and the
proof's `effective_user_hash`, then constructs
`ProviderKubernetesRequestIdentityV1` only through the pure projector from the
proof's `effective_original_user` and that name basis. The name-basis,
requested-target, rendered metadata, source-proof, and capsule equalities all
run before a represented candidate is returned. The complete environment map
remains process-local and is never copied into the invocation. Before
`ACTION_ACTIVE` admission, the live worker reprojects the same prepared pair
plus proof back to the admitted invocation. Recovery instead resolves the
retained source, uses its immutable effective pair, and must reproduce the
complete stored spec/invocation byte-for-byte without an authenticated-user
lookup.

For a legacy-shadow submission, the API-request binder locks and decodes the
persisted effective `LaunchBody` and exact-compares its two identity values with
the retained proof in the same request-binding transaction. A mismatch writes
`IDENTITY_MISMATCH` once, leaves the immutable represented coverage row
unchanged, and grants no provider authority; terminal completion cannot clear
or replace it.

Down normalization never looks up `SKYPILOT_USER`, `SKYPILOT_USER_ID`, or an
ambient/current user and never constructs a standalone request identity.
`PreparedDownRequest` may retain authenticated actor data for the existing
request scheduler/audit path, but it is not copied as provider input. The down
invocation, seed, capsule, policy subject, and cleanup cursor may transitively
retain the prior launch's frozen name-basis hash, object labels, and Pod
annotation as exact target/deletion evidence; those bytes are immutable and do
not change when the down actor changes. Tests replace the current actor between
launch and down and require identical down canonical bytes and cleanup
behavior.

Each not-representable result has no free-form detail and is not an action
specification. The normalizer uses bounded accessors and returns the first
failure in the enum order shown above; tests cover inputs with multiple
failures. A malformed/unbounded/unknown typed input is `request_contract`; an
unexpected exception is not converted to free-form coverage and the mutation
gate remains closed. In M2 the adapter writes the exact identity, contract
version, outcome, and bounded reason to
`serve_resource_action_shadow_coverage` in the same transaction as approved
replica/capacity intent. It leaves that launch on the legacy path and creates
no synthetic `pod_cluster_v1` parent. Counters or logs cannot satisfy
promotion coverage. Additional clouds accumulate action samples only after
this file defines their own closed invocation profile; a shadow-mode service
flag alone does not make arbitrary input representable.

This is an intentional correction to the earlier contract. A generic frozen
`LaunchBody` is request-final but is not provider-final for unpinned placement,
discards the pre-policy boundary, and may contain secret or ambient state. The
earlier fixture also used the display cluster name where Kubernetes actually
mutates the suffixed head Pod. Narrowing v1 and embedding the Kubernetes scope
preimage prevents shadow evidence from overstating authority. Before the
measured candidate window, the narrow legacy-owned path deliberately adopts
the same renderer/prebooted runtime/job seam; services outside that closed
cohort keep the original legacy path.

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
byte-equal copy of the wrapper invocation. For primary down, the plan and down
invocation carry byte-equal `PriorLaunchBasisV1` values and admission validates
the referenced retained row before accepting either. A
`LAUNCH_CLEANUP_DOWN` child is the sole exception: it uses
`ServeLegacyLaunchCleanupDownInvocationV1`, derived byte-for-byte from the
parent launch spec by `launch_cleanup_down_invocation()`. Its parent action ID,
parent invocation hash, resource identity, target, workspace, and fixed down
flags must all match that derivation. In particular,
`parent_launch_request_payload_sha256 == ParentSpec.invocation.sha256 ==
ParentSpec.provider_plan.request_payload_sha256`. This child-only value
deliberately has no `PriorLaunchBasisV1` and no current down execution config:
it observes the
existing legacy cleanup between launch retries and grants no replay or provider
authority. It cannot appear in `ServeReplicaActionSpecV1`, a primary child,
coverage admission, or either authoritative handler. Static role, path, name,
and request-body parity comes only from the parent launch capsule; runtime UID
parity is proven by the causally scoped evidence rule above. Any extra ambient
cleanup behavior is divergent and promotion-blocking. Typed reads reconstruct
the exact applicable union member; arbitrary mappings are not accepted. Golden
canonical-byte/hash fixtures plus unknown-key, float, identity-mismatch, and
mutated-plan/invocation rejection tests freeze this wrapper contract.

The cleanup derivation performs no database read, preflight, policy evaluation,
cohort selection, clock read, or prior-attempt lookup. The special closed shape
forbids prior-launch basis, execution config, cleanup target, cohort, policy,
and progress keys even when null. Attempt recovery decodes `request_role`
before its invocation: primary roles accept only the wrapper's byte-equal
`ProviderLifecycleInvocationV1`, while cleanup accepts only the exact special
derivation under a launch parent. Cleanup retries reuse those same bytes;
logical-attempt and request sequence remain in the attempt envelope. Its
outcome is classified as a down effect, so success requires authoritative
absence against the frozen target. Outcome/request validation receives and
binds `(request_role, parent_spec, invocation)`, parsing the cleanup role and
parent before selecting this special union member. Cleanup observation comes
from the shadow pre/post observer; the child never passes through
`ProviderLifecycleFacet.submit()` or `ProviderLifecycleFacet.observe()`, and
its special invocation hash is never required to equal the generic API-request
body hash. Immediately before SDK entry, the worker projects the actual cleanup
call arguments into the exact `legacy_down_request` shape and requires byte
equality to the special invocation's `legacy_down_request` member. The bound
request ID is the real ID returned by that call; generic transport-body hash
equality is neither required nor authority. Recovery preserves legacy request
association and retry fencing and never upgrades this evidence fingerprint to
an independently admitted down action.

Exactly one of `launch` and `down` is nonnull and it must match
`action_kind`. Objects reject unknown keys. Text is NFC, lists are sorted and
duplicate-free by their canonical element/key when declared set-valued;
ordered tuples such as context identity, topology roles, and mutation order
preserve their specified semantic order. UUIDs are lowercase hyphenated,
integers are JSON integers, and floats are forbidden. Each text is 1..1,024
UTF-8 bytes except `cluster_name`, namespace, label keys/values, and ports,
which are 1..253 bytes, and each canonical DER-certificate base64 scalar, which
is 4..16,384 ASCII bytes and decodes to 1..12,288 bytes; lists contain at most
256 items; the whole canonical object is at most 65,536 bytes. SHA fields are
64 lowercase hexadecimal characters.

Collection order is part of the wire contract; parsers reject rather than
silently reorder a noncanonical input:

- `context_identity` and arrays inside an opaque canonical Kubernetes JSON
  preimage preserve source semantic order. `access_decisions` are contiguous
  and ordered by `check_sequence` from zero.
- topology mutable objects, execution object plans, partial/full/cleanup object
  slots, server allocations, create/delete effects, and endpoint callers use
  their exact protocol order: role-map plan order, declared JSON-pointer order,
  mutation sequence, or `serve_lb_slot_0, serve_lb_slot_1`, respectively.
  `resources_ports` contains exactly the one application port.
- runtime artifacts use the exact role order `ray_runtime, skylet_runtime,
  skylet_job_protocol, skylet_state_schema, startup_probe,
  serve_canary_entrypoint`; the worker handler allowlist and authenticated
  identity groups use their literal displayed order. Authority-worker
  registrations are sorted by unique `worker.pod_uid` and reject duplicate Pod
  UIDs.
- CA certificates are a sorted duplicate-free set by encoded scalar. Label,
  annotation, and selector pairs are sorted by key and reject duplicate keys.
  Image-pull-secret names, legacy-secret references, API groups, resources,
  resource names, verbs, rule objects, nonresource-rule objects, and
  avoid-accelerator label keys are sorted duplicate-free sets; compound rules
  sort by canonical bytes after their inner sets are canonicalized.
- prerequisites use the exact 12-entry
  `ProviderKubernetesPrerequisiteRoleMapV1` order and the launch-only endpoint
  projection uses its exact five-role order. They are never key-sorted. Only the three
  release/LB Namespace roles may share one logical key/UID, as the required
  alias group; every other duplicate logical key, duplicate nonaliased UID, or
  alias mismatch rejects. All fields displayed as `[]` in a v1 config are
  empty-only, not unordered extension points.

Before linked represented admission is enabled, checked-in realistic launch and
down golden fixtures must include the observed 1,036-byte `boltz-test` CA scalar,
three complete requested/semantic object bodies, the full kind-specific
principal/authorization inventory, and all 12 prerequisite role records. The
launch golden additionally includes the exact five-role endpoint projection,
six runtime artifacts, and both endpoint callers with their complete live
Deployment projections. The down golden contains none of those launch-only
endpoint/runtime/job fields, and insertion of any one rejects. Tests
record each full `ServeReplicaActionSpecV1` byte length, require
it to be at most 60,000 bytes (preserving at least 5,536 bytes of rollout
headroom), and still enforce the absolute 65,536-byte parser bound. Failure is
`NOT_REPRESENTABLE`; no truncation, compression, omitted preimage, or external
hash-only lookup is allowed.

The `source` object contains a `content` reference to an immutable retained
`version_specs` row plus the closed server-effective identity proof;
the builder verifies the row's exact UTF-8 YAML bytes against
`yaml_content_sha256` and every proof hash/binding before use. The first
eligible cohort requires
`file_mounts_blob_id=null`, `tls_material_ref=null`, no task
secrets/storage/local mounts, and byte-equal pre/post-policy projections as
defined above. The v1 run source is the reviewed nonsecret canary content
addressed by that row; arbitrary user commands remain not representable until a
separate secret-safe job-input commitment exists. Any other resource field, nondefault launch flag, policy
mutation, secret/material source, or compound topology normalizes to not
representable. This intentionally narrow gate lets the source reference, the
bounded nonsecret prepared/effective identity pairs, and the closed
transformations reconstruct the same provider-effective prepared projection
without copying YAML, commands, an arbitrary environment map, or secret bytes
into action JSON.

Only `ProviderLaunchContentSourceV1` enters renderer, runtime/job-submission,
and Skylet wire contracts. The identity proof is action/recovery authority and
never appears in a Kubernetes object body, Skylet submit request, workload
environment, or legacy-effect comparison. The launch policy subject and
preflight seed retain the complete `ProviderLaunchSourceV1` so those boundaries
still bind both content and identity provenance.

The builder includes the action/target identity, normalized topology and
resource selection, workspace/config identities, and content identities for
provider-affecting artifacts, but no credential, kubeconfig, secret value,
private key, arbitrary environment value, uploaded body, or traceback. Secret
names or opaque references may be represented only when they cannot disclose a
secret value. A hash-only field is insufficient unless its bounded normalized
preimage or a reviewed nonsecret content-addressed reference is also stored.
The profile stays ineligible until the same builder output demonstrably drives
the real invocation; a parallel observer-only serialization is not authority.

Launch `SafeThread` becomes a bounded two-phase prepare/wait/submit worker with
state `NEW -> PREPARING -> PREPARED -> APPROVED | APPROVED_LEGACY |
DENIED/CANCELLED -> PRE_SUBMIT -> SDK_ENTERED -> DONE`. Preparation applies the
allowed transforms once, proves both policy/secret modes, and privately retains
the immutable request. The exact launch order is: freeze that request and mint
resource/decision identity; create the `PREPARING` reference with a fresh
capability hash; make identity canonicalization the first post-reference
network call using the raw capability; validate and freeze the complete source
proof/name basis/request identity; then perform authority preflight and obtain
the read-only scope/config inputs. No launch source, requested target, rendered
object, preflight seed, or represented result may be constructed before the
identity proof. The worker publishes
only the redacted typed result and then waits on a one-shot
condition. It has no path to `sdk.launch()` before approval. Preparation and
the wait hold no SQL transaction, database row lock, resources-file lock, or
logical-state lock and use a separate bounded pool from provider submission.

When an existing provider slot is available, the manager performs the parent
design's short service -> replica -> capacity -> cohort -> reference ->
coverage -> optional-parent transaction. Before signaling it requires the
same-ID `PREPARING` reference and atomically changes it to `SHADOW_ACTIVE` while
writing `worker_cohort_ref_id=decision_id`; mismatch or rollback leaves no
approval. It signals only after commit or exact lost-commit readback. A
representable approval carries the same-ID coverage/parent, stored spec and
invocation hashes, and an unguessable process-local preparation nonce; a
not-representable approval carries coverage and that nonce but grants no
cross-process replay authority. The same-cell worker recomputes the projection,
rechecks owner/cancel/scope fences, and commits either the represented child or
coverage-only attempt `PRE_SUBMIT` before entering SDK request creation.
Construction/start of a PREPARING worker is
not mutation enqueue; release of this gate is the enforceable provider
boundary.

The live worker uses the same in-memory request object and retained identity
proof until SDK request admission, but object identity is not a distributed
authority. Across HTTP the
durable contract is the full stored `ServeReplicaActionSpecV1`, including the
frozen execution config, scope, invocation, and retrievable references. Generic
request/HTTP bytes and credentials are not persisted or hashed. After process
loss, represented recovery reconstructs from the retained source and must
reproduce the entire stored spec/invocation byte-for-byte. A
not-representable reason never authorizes reconstruction or replay. A proved
pre-SDK mismatch abandons the represented parent and replans under a new
generation; once either kind of `PRE_SUBMIT` exists, recovery is ambiguous and
observes or blocks before any retry. A changed provider-effective input is
never silently submitted under the old generation.

Golden fixtures store the exact canonical UTF-8 bytes and lowercase SHA-256 for
one launch and one down invocation. Tests compare literal bytes and hashes, not
round-tripped JSON equality, and mutate every key/type/ordering/redaction field.
Within a live preparation epoch the same object that yielded the accepted
projection is consumed by request admission; cross-process tests require full
stored spec/invocation equality, not generic request-byte equality. No policy,
placement, resource, mount, identity, or option
mutation is permitted afterward.

## Implementation phases

### P1: pure normalization

- Extract bounded plan/locator/error/observation normalization around existing
  launch/down helpers.
- Add the closed redacted invocation builder and typed not-representable result;
  do not serialize raw generic request bodies.
- Add the explicit frozen Kubernetes transport/scope, execution config,
  principal/prerequisite evidence, exact resource translation, dual-boundary
  policy trace, shared API effective-identity resolver and private no-enqueue
  canonicalization proof, pure naming helper, exact three-object renderer/admission
  normalizer, prebooted runtime/job/endpoint contract, and checked-in access/
  call inventory. Restrict v1 normalization to the location-pinned canary
  candidate above.
- Add the policy-free execution capsule, typed policy subject/proofs, closed
  Kubernetes review/prerequisite projections, exact same-client facade
  inventory, versioned worker cohort/image identity, launch/down execution
  capsules, and closed discriminated preflight wire contract.
- Harden the shared scalar helpers, detached bounded canonical-JSON wrappers,
  three persisted wrapper embeddings, Handle provider-config literals, three
  named raw/direct action-kind conversion sites, and action-ID delegation to
  the exact-type contract above without broadening the claim to unrelated
  constructors or canonicalizing parsers.
- Add no provider mutation call sites.
- Build golden fixtures from current provider results with secrets removed.

P1 leaf verification evidence on 2026-08-01: the pure closed Kubernetes
transport, scope, and scope-read DTOs pass literal canonical-byte/hash,
round-trip, closed-shape, scalar/list/object-bound, semantic tuple-order,
namespace/service-account consistency, and failed-read discriminator tests.
The 16,384-byte CA scalar ceiling covers the 1,036-byte canonical DER base64
observed on `boltz-test`; independent adversarial re-review accepted the leaf.
This does not claim live URL or X.509 normalization, preflight, execution-config
closure, or representability authority.

Additional P1 leaf verification evidence on 2026-08-01: the exact 12-role
prerequisite role/kind map, launch-only five-role endpoint projection, two
Deployment caller projections, Namespace/ServiceAccount identity graph, closed
Skylet job-spec/submit-request/retained-evidence values, and context-free policy
boundary proof pass their focused canonical-wire, closed-shape, type/bound,
alias, distinct-key/UID, cross-namespace, retained-byte, and negative mutation
tests. The focused suites plus the adjacent provider-value, execution-foundation,
legacy invocation, Serve033, completed-down, Kubernetes-scope, and naming suites
pass together. Independent adversarial review accepted these context-free
leaves. Endpoint direct and wire probes additionally reject scalar, tuple,
list, and typed-child subclasses, nested cycles, and 10,000-item raw
collections before recursive parsing. This evidence does not claim
execution-capsule composition, contextual policy or retained-request
comparison, renderer/normalizer artifacts, provider I/O, or authority.

Further P1 leaf verification evidence on 2026-08-01: the policy-free request
identity, scheduling, storage, metadata, security, object-mutation-effect, and
exact launch/down mutation contracts pass fixed canonical byte-size/hash,
round-trip, closed-shape, semantic-order, exact-list-cardinality, scalar and
whole-object-bound, direct/wire container, and every-literal mutation tests.
Hostile-input probes also require bounded rejection before recursive
serialization or child parsing and reject cycles, 1,100-level nesting, falsey
collection subclasses, equality/length-spoofing scalars, and typed-child
subclasses with hidden wire fields. The focused suite and the adjacent 11-file
provider DTO suite pass together; independent adversarial review accepted the
leaves. This evidence does not claim prepared-request identity provenance, the
capsule/config composition, policy subject projection, provider effect
emission, I/O, or authority.

Further P1 inventory verification evidence on 2026-08-01: the private bare-list
validator accepts exactly the 12 prerequisite records in literal role-map order,
requires the release/LB Namespace alias group to be byte-equal after omitting
only `role`, and rejects every other duplicate logical key or UID. Focused tests
cover missing/extra/swapped roles, each side of the required alias comparison,
same-key and same-UID collisions, cross-kind UID reuse, exact direct types, and
positive same-name cases whose kind or Namespace keeps the logical keys
distinct. The prerequisite wire parser now validates closed outer shapes and
all raw collection cardinalities before child parsing; exact-length cyclic
records, cyclic leaf scalars, list subclasses, and 10,000-item nested lists
therefore reject with bounded contract errors rather than recursive
serialization. The focused suite passes, and independent adversarial review
accepted the exact inventory implementation and ownership boundary. This
evidence does not claim the launch-only endpoint-to-inventory equality, any
other capsule cross-field binding, provider I/O, or authority.

Further P1 bounded-wire verification evidence on 2026-08-01: the shared scalar
helpers, three raw/direct action-kind sites, action-ID delegation, two Handle
literals, three canonical-wrapper embedding seams, and detached bounded JSON
graph implement the exact-type contract above. Hostile probes reject root and
nested scalar/container/key subclasses and arbitrary `Mapping` values without
invoking overridden `__class__`, `len`, iteration, lookup, or `items`; the
serializer receives only a deeply detached graph even when the caller mutates
its source at serialization time. Existing cycle, shared-reference, depth,
member, aggregate, integer, text, and byte-size boundaries remain green. The
focused files and adjacent 12-file non-PostgreSQL provider DTO suite pass
together. Repository YAPF/isort, mypy over 825 source files, pylint at
10.00/10, and dashboard lint/format checks pass. Independent adversarial review
accepted exact source hash
`3329427c9506aed6a9f40152d4e8c63e4977659e37be92972237736ca0d419d1`
and the four focused test hashes. This evidence remains scoped to the helpers,
raw fields, direct sites, wrapper embeddings, and canonical wrappers named
above; it does not claim exact raw-subclass rejection in unrelated legacy
canonicalizing parsers or every direct DTO constructor.

First-deployment cutover evidence on 2026-08-01: read-only `boltz-test`
inspection found API schema 004, Serve 031, global-user-state 027, and no API or
Serve resource-action tables. Helm revision 51 has every role on baseline
digest
`sha256:a5afbd26e62ebe2f6990b2f311a59caaf3ef2901f2eab5d6dddd46527320f00a`,
whose recorded source baseline predates the resource-action DTO module. This
proves the flattened local scaffold is not present there; the post-migration
zero-row/link/mode snapshot remains a mandatory rollout artifact.

### P2: live shadow observation

- Capture the actual serialized CoreV1 and Skylet effect trace through its
  closed create/delete/typed-submit union, not merely the high-level legacy
  request/result.
- Add global-user-state revision 028 and propagate the precommitted
  cluster-record UUID through the prepared launch request, backend, cluster
  row, and provider labels without repurposing `cluster_hash`.
- Characterize the current direct `core.down()` compatibility path, then route
  legacy teardown through `sdk.down()` and require real request IDs for the
  promotion window.
- Perform reviewed read-only pre/post observations.
- Add durable coverage for every approved normalization decision and exercise
  the prepare/admit/authorize gate without changing the sole legacy mutation.
- Before opening the measured window, route the narrow legacy-owned candidate
  through the common renderer, prebooted runtime, exact handle, and
  action-keyed Skylet seam; leave every other service on its existing path.
- Store parity/divergence with no second mutation.

### P3: request-handler integration

- Add journal-before-I/O mutation boundary to action-correlated requests.
- Add API006 monotonic provider progress and persist partial object UIDs/specs,
  exact handle, runtime, job intent/ID, endpoint, operation IDs, and typed
  outcomes under existing request claim fences.
- Carry the exact provider cursor from attempt `n` to `n+1`, clear/recompute
  only the attempt-scoped attestation envelope, and reject any crossed-boundary
  gap or regression.
- Invoke the in-server execution/core seam directly; the handler must not call
  `sdk.launch()` or `sdk.down()` and create a nested API request.
- Drive exact CoreV1 operations through one `KubernetesResourceActionSession`;
  bypass policy reapplication, generic bootstrap/config discovery, cached
  clients, and mutation helpers that rewrite bytes.
- Implement per-effect worker-chain revalidation, the purpose-authenticated TLS
  preflight endpoint, dark Helm versioned authority-worker Deployments/Service/
  RBAC/NetworkPolicy resources, active/frozen cohort claim join, and the closed
  handler claim filter.
- Implement Skylet submit/readback idempotency by action UUID plus its fsynced
  job/start-outbox and launcher run-token state machine; remove every SSH/
  generic-execute fallback from the candidate.
- Implement superseded partial-launch handoff to one normal down action with no
  hidden launch cleanup or alternate scheduler.
- Keep authoritative dispatch limited to synthetic/canary actions.

### P4: selected Serve authority

- Enable one eligible service/profile after the parent design's gates.
- Require the exact provider block in endpoint lookup, same-UID Pod IP refresh,
  and both warm-standby load-balancer slots' connectivity smoke.
- Preserve per-service fallback only for services that never promoted.
- Delete duplicate provider retry/observation ownership from the eligible
  Serve path after soak.

## Tests

Contract tests must cover:

- pre-release flattened-v1 bytes are rejected, the first-deployment preflight
  passes only with legacy service modes and absent/empty operational tables,
  and the cutover changes hashes/goldens without changing deterministic action
  UUIDs;
- canonical plan/locator bytes and identity mismatch rejection;
- exact built-in acceptance and subclass rejection at the shared text, hash,
  integer, timestamp, enum, UUID, and action-kind helpers; exact-Boolean
  acceptance plus integer/`IntEnum` rejection; exact typed enum/UUID/
  action-kind direct-input positive controls; equality/hash/length/
  bound-spoofing rejection at both Handle literals, all three named raw/direct
  action-kind conversion sites, and action-ID delegation; and preservation of
  the explicit invalid-value exception/message categories above;
- exact built-in root, key, scalar, list, and dict acceptance for bounded
  canonical JSON; rejection of root and nested scalar/container/key subclasses
  and arbitrary `Mapping` implementations without invoking their overridden
  methods; serialization of only a detached validated graph under a
  deterministic mutation-at-serialization probe; exact-wrapper enforcement at
  the three persisted embedding fields; and retention of normal integer, text,
  member-count, aggregate, byte-size, depth, and cycle boundaries;
- literal frozen transport/scope bytes/hash, pure user-hash naming, and the
  actual suffixed head-Pod/two-Service names;
- prior-launch basis lookup, mutated-basis/down-digest rejection, embedded
  observation evidence/hash derivation, and nonnull v1 response-hash rejection;
- all exhaustive cross-field equalities, nonnull CPU/memory and literal
  request/limit translations, `imagePullPolicy: Always`, and byte equality to
  the normalized Pod/Service specs;
- the exact 12-role prerequisite inventory and role/kind/order map, the one
  required release/LB Namespace alias group, rejection of every other key/UID
  alias, launch-only exact five-role endpoint projection and two-caller order, all
  role-to-scope/principal/policy bindings, alias equality after omitting only
  the exact role field, and same-client pre/post drift for each individual role;
- both launch endpoint callers bind their NetworkPolicy selector through an exact live
  LB Deployment projection to its distinct Pod-template ServiceAccount and
  prerequisite UID; missing/default/shared/wrong ServiceAccounts, selector or
  template-label drift, stale observed generation, deletion, cross-slot
  Deployment identity, and pre/post replacement all reject; a down capsule
  rejects every endpoint/caller/workload key and emits zero LB Deployment GETs;
- the exact three request bodies: absent Pod `spec.nodeName`, one `ray-node`
  container, digest image, exact CPU/memory-only requests/limits, workload
  ServiceAccount and false token automount, replica environment lookup by name
  with scalar value and absent `valueFrom`, management port as JSON integer
  `46590` at `/spec/containers/0/ports/4/containerPort`, singleton SSH-Service
  port 22 with absent `clusterIP`, and headless Service `clusterIP="None"`
  with an absent—not null or empty—`ports` key;
- application-port rejection against every fixed runtime/management/SSH port,
  absence from all Pod/Service port lists, and literal `podip`
  `open_ports()`/`cleanup_ports()` no-op assertions with zero Service,
  Ingress, LoadBalancer, patch, or other provider calls;
- missing, unreadable, hash-drifted, or behavior-drifted candidate renderer,
  binding-schema, or admitted-object-normalizer artifacts return exactly
  `unrepresented_execution_config`, preserve shadow-only routing, and emit no
  action-owned provider bytes; realistic and candidate-maximal artifact goldens
  are required before that gate can open;
- exact retained-source verification, both policy boundaries absent,
  byte-equal pre/post projections, and deterministic precedence for every
  closed not-representable reason;
- launch requires byte-equal renderer/config-projection config-access references;
  down has no renderer and instead binds its config-projection reference directly
  to the cohort's unique approved `config_access_inventory` role; missing,
  duplicate, crossed-role, or unequal entries reject;
- the two pure nonrecursive policy-subject projectors copy every displayed
  launch/down field from outer plan/invocation/basis/capsule preimages; mutation
  of each target, retry, replica, option, basis, cleanup target, or recomputed
  hash rejects even when the submitted subject graph is internally consistent;
- launch preflight seeds include full requested target and retry value; down
  seeds include the full prior basis and cleanup target beside their hashes;
  responses accept only an `api_executor_pre_io` proof, controllers construct only
  `serve_controller_prepare`, and the handler's immediate pre-I/O executor
  recomputation must be byte-equal to the stored proof;
- down preflight freezes a current down-only execution capsule/cohort/security
  proof, rejects an obsolete launch authority or ambient reconstruction, and
  revalidates the exact same-client evidence on every retry;
- the preflight response discriminator rejects a launch response carrying the
  down-only `prior_launch_basis` reason and a down response carrying the
  launch-only `multi_task` reason, even when all other fields are well formed;
- prepared launch/down results accept only their corresponding refined
  lifecycle invocation and reject a wrong-kind invocation before hashing,
  admission, or provider execution;
- `submit()` and `observe()` accept the complete launch/down spec, select its
  kind-matched invocation and execution config, and reject a plan-only call or
  any replacement scope, principal, cohort, execution config, or stale/missing
  request-execution fence;
- one exact application port in `podip` mode and rejection of zero/multiple
  ports, Ingress/LoadBalancer generation, mutable images, reserved-label
  collisions, bootstrap writes, and custom implementations;
- literal execution-config/template/inventory fixtures and an AST guard for
  every config/environment/file/global-state/discovery/mutation access;
- exact caller/workload ServiceAccount UID/resourceVersion/spec, authenticated
  SelfSubjectReview, complete rules preimage, kind-specific required/forbidden
  access-review matrix, crossed launch/down matrix rejection, and drift
  before/after effects;
- one live isolated Kubernetes client for typed scope-before/after, exact
  prerequisite and CoreV1 reads/creates/deletes, with failed scope reads
  encoded, exact worker Pod/ReplicaSet/Deployment, launch's two exact LB
  Deployment GETs, down's zero LB Deployment GETs, and zero second-client,
  patch/update/collection-delete/Secret/RBAC mutation/PVC/Deployment mutation/
  Ingress/exec/cp calls;
- every facade wraps the object-identical raw `ApiClient`, and the exact
  CoreV1/AppsV1/NetworkingV1/AdmissionregistrationV1/Version/
  AuthenticationV1/AuthorizationV1 method inventory rejects all unlisted
  calls;
- every requested/admitted object semantic preimage/hash, allowed literal
  default/server allocation, 201/409 exact adoption, injected field rejection,
  and partial UID commitment supplied to each later effect;
- the literal three-role plan order and exact launch/down mutation-contract
  create/delete sequence-role-kind tuples, including rejection of an individually
  valid mutation-effect leaf in a wrong list position; requested Pod omission of
  `spec.nodeName`, scheduler-only append of that allocation, incomplete
  pre-scheduling readback, immutable allocation replay, and exact node name in
  the committed handle;
- the same `PreparedLaunchRequest` object through one live admission epoch, and
  full stored spec/invocation equality rather than generic request-byte or
  object-identity authority after recovery; missing/empty/non-ASCII original
  user, independently mutated prepared/effective/cleaned/original/hash values,
  proof context/input/resource/capability-hash drift, wrong raw capability,
  capability reuse after `PREPARING`, every extra raw key, wrong content type/
  encoding, each body-size edge and exact HTTP status, no-enqueue resolver drift
  from `prepare_request_async()`, name-basis drift, authenticated
  canonicalization/persisted-request drift, rendered user-label/Pod-annotation
  drift, and any ambient helper access reject; request binding atomically preserves either
  equality or write-once `IDENTITY_MISMATCH`, while changing the current actor
  before down leaves the prior-launch-derived down bytes unchanged and a
  standalone down `request_identity` key rejects; inserting any identity proof
  field into the content source, Kubernetes bodies, or Skylet request rejects;
- the legacy launch-cleanup child is the exact parent-derived special wire
  member, is accepted only for `LAUNCH_CLEANUP_DOWN`, and is rejected as an
  action spec, primary child, coverage input, or authoritative-handler input;
- every content-addressed policy edge recomputes to its one co-located capsule,
  config projection, and policy-subject preimage; crossed controller/executor,
  before/after, capsule, or subject hashes are rejected;
- a standalone policy-proof leaf accepts only its local closed shape, absence
  modes, and equal before/after hashes but grants no authority; only the
  enclosing launch/down config binds its controller/executor slot and all
  projection/capsule/subject hashes to co-located preimages;
- registration sets require ascending unique worker Pod UIDs and reject both a
  permuted list and duplicate UID;
- realistic launch/down golden specs exercise their complete kind-specific
  inventories and observed CA size, record their canonical byte counts, retain
  5,536 bytes of headroom, and reject a one-byte-over-budget variant without
  dropping a preimage;
- secret sentinels, raw YAML, arbitrary environment, kubeconfig/config, and
  launch-fence values never appearing in canonical invocation bytes;
- the first launch/down progress cursor and `INTENT_COMMITTED` commit atomically
  to both boundary fields before the provider mock observes a call; a crossed
  authoritative provider-I/O watermark with null progress is impossible,
  including a crash immediately after that combined commit;
- an attempt `n+1` with the exact inherited revision-one cursor consumes only
  that current cursor, clears and recomputes only the attempt-scoped
  attestation envelope, and rejects regressed or predecessor-mismatched
  crossed-boundary progress;
- an attempt `n+1` materialized with the exact `NOT_STARTED`/null/revision-zero
  pre-I/O shape takes the fresh-cursor branch and atomically initializes its
  first cursor with both `INTENT_COMMITTED` boundary fields before the provider
  mock observes any call;
- a crash after inherited-cursor materialization but before boundary/
  attestation binding remains retryable only for the exact revision-one,
  predecessor-equal, null-attestation seed; it is not corruption and never
  proves `CANCELLED_NO_EFFECT`;
- every external effect exact-revalidates the worker Pod -> ReplicaSet ->
  Deployment chain, with exact owner kinds and byte-equal embedded name/UID
  fields plus rejection of swapped/direct/crossed owners; the first effect fills
  one write-once post-attestation, later effects match it, and a replacement
  execution generation binds a new attestation to the carried cursor without
  replaying a committed effect;
- crash before intent and before/after every object, progress, handle, runtime,
  job-intent/job-ID, endpoint, operation-ID, and response boundary;
- lost launch acknowledgement followed by exact adoption;
- lost launch acknowledgement with exact absence but no idempotency/final
  operation proof continuing observation without retry;
- lost launch acknowledgement followed by one legal retry only after stable
  idempotency or proof the prior operation cannot take effect;
- ambiguous/name-only evidence blocking a retry;
- down acknowledgement followed by present, uncertain, then exact absent;
- same-name/new-incarnation protection;
- authoritative partial-launch supersession fences the old request, commits one
  real queued down action, carries every known/null UID slot and intended/
  committed handle, rejects hidden cleanup and replacements, proves three exact
  NotFound results, and only then removes/adopts the exact cluster row;
- a never-started launch cancellation creates no down action, removes its
  provisional replica/count once, and is idempotent; effectful supersession
  inserts/adopts source/down action IDs in both possible UUID sort orders;
- every old launch effect intent has exact cursor evidence, a typed definitive-
  no-effect proof bound to its original intent claim, or an origin-byte-equal
  `call_not_entered` proof before
  supersession; after earlier committed effects, a later CoreV1 422, rolled-back
  cluster-record insert, or pre-commit Skylet conflict hands off only with its
  exact proof, while timeout, reset, 5xx, lost acknowledgement, expired lease,
  and post-call NotFound remain ineligible;
- prebooted runtime imageID/startup/Ray/Skylet evidence, asserted no-op generic
  stages, action-keyed same-spec job adoption, different-spec conflict, and no
  SSH/private-key fallback;
- the one pure session-owned Skylet comparator reconstructs a byte-exact job
  spec/request, binds action ID and capsule job-contract preimage, and supplies
  every intent/commit/rejection hash; leaf-valid but crossed action keys,
  contract hashes, specs, request hashes, or duplicated partial validators
  reject; lost-ack readback reconstructs the full retained request from one
  SQLite revision, equal bytes adopt, unequal bytes conflict even under equal
  adversarial hash scalars, and not-found/uncertain evidence retains no request;
- six ordered workload artifact bindings with retrievable manifest/build
  preimages, qualified OCI manifest/config/runtime-image identity for both
  workload and authority containers, explicit CRI-scheme/config mapping instead
  of raw-CRI-to-manifest equality, independent startup/session measurement, and
  rejection of anonymous hashes;
- Skylet crashes on both sides of job/outbox fsync, before wake, around
  `START_INTENT`, spawn, `START_COMMITTED`, failed/successful `exec`, post-exec
  handshake, watcher observation, and restart in every state preserve one
  job/outbox row and ID, write-once run tokens, and monotonic run epochs;
  `START_COMMITTED` can never satisfy `JOB_RUNNING` or launch success;
- exact provider-handle persistence, no ambient endpoint fallback, same-UID Pod
  IP refresh, application readiness separate from provider success, frozen
  NetworkPolicy, and reachability from both load-balancer slots;
- stale request claim/execution generation write rejection;
- launch `ENDPOINT_RESOLVED` and down `ABSENCE_EXACT`/`HANDLE_REMOVED` reject
  terminal success; only their final claim-fenced `SUCCEEDED` variants reduce;
- retry/pause exceptions terminalize once without same-request requeue or a
  second provider submission;
- both intent/evidence-versus-terminalization race directions, proving that a
  writer which loses the request lock rejects and a writer which wins commits
  before terminalization;
- providers with and without operation IDs;
- redaction and byte/depth/node bounds;
- private preflight request/response bounds, nonce/hash equality, exact HTTPS
  endpoint, token-ring/TLS purpose separation and rotation, retry matrix,
  timeout/redirect behavior, mutation-import prohibition, and zero durable
  preflight state;
- static-manifest mismatch, Deployment-owner UID mismatch, ServiceAccount UID
  mismatch, registry identity mismatch, and execution-capsule cohort mismatch;
- Helm render/install/upgrade tests for dark-by-default immutable versioned
  two-replica authority cohorts, active-cohort preflight selection, frozen-
  cohort claim joins, `REGISTERING` plus two distinct ready adopters before
  `ACCEPTING`, digest pins, namespace-local Secrets/static-manifest mounts,
  release-namespace worker/Service selectors, two frozen LB Deployment
  selectors with distinct explicit ServiceAccounts and exact GET-only evidence,
  separate canary workload namespace, ClusterIP Service, NetworkPolicy, exact
  namespaced/cluster RBAC
  grants and forbidden verbs, plus API -> new worker cohort -> controller
  rollout and current-chart rollback while both cohorts remain claimable;
- atomic `PREPARING -> SHADOW_ACTIVE|ACTION_ACTIVE` binding with admission;
  active-cohort switches between preflight/admission; retirement between zero-
  reference discovery/admission; stale preparation owners; a nonterminal
  private shadow request; missing/unreadable/malformed references; rollback
  from `DRAINING` with the exact Deployment; and removal only after
  `REMOVAL_AUTHORIZED`, with the surviving API verifier retaining only the
  derived tombstone GETs until exact NotFound commits `RETIRED`;
- concurrent ordinary, paid-capacity, and reserved-fill admission at the cap,
  commit-before-signal recovery, release/admission and owner-handoff races, and
  exact loser/no-artifact and no-double-release assertions from the parent;
- shadow records every eligible candidate, executes one legacy high-level
  mutation, captures the closed effect-body union with exactly three creates
  plus one typed job submission (or three UID-preconditioned deletes), and
  rejects arbitrary job JSON or a missing/extra/mismatched effect; and
- absence of any new provider queue, worker lease, or domain retry scheduler.

The isolated HA smoke test kills API/controller/authority-worker pods at every
mutation/progress boundary and asserts one logical action, one request per
attempt, no duplicate object/job, exact partial-state adoption, both LB-slot
paths, and no false teardown completion.

## Deployment and rollback

Provider changes ship dark, then shadow, then per-service authoritative. The
blocking migration job must converge all three independent additive
heads—global-user-state 028, Serve033, and API005 for shadow; API006 replaces
API005 as the required API head before provider dispatch or authority. There is
no cross-lineage Alembic dependency. No provider profile is enabled globally by
schema migration. Application rollback retains all three heads and
uses only a compatible image that preserves nonnull cluster-record UUIDs as
write-once commitments and preserves nonterminal shadow/action state. It does
not run provider compensation or schema down. After first authority, rollback
to a pre-action-aware image is unsupported.

The companion inherits the parent's exact role rollout: build one immutable
digest, upgrade the API role first until all required heads converge, upgrade
the new authority-worker cohort second and attest it, then upgrade controllers
last. Every `helm upgrade` uses `--reuse-values` and explicitly pins every
untouched API/controller image and every extant worker cohort. The first
additive migration stage omits `--atomic`; repair or
rollback uses the current chart with the prior compatible immutable digests
against the retained heads, never native `helm rollback`.

On `boltz-test`, every authority-worker Deployment, versioned ServiceAccount,
selector Service, purpose Secret projection, static-manifest projection, and
worker NetworkPolicy lives in the Helm release namespace `skypilot-ha`; the
Service never attempts a cross-namespace Pod selector. The first provider
workload target instead uses the dedicated `skypilot-actions-canary` namespace,
never `default` or the control-plane namespace. The capability-filtered normal
executor still claims the existing PR #1070 queue/lease; it is not a second
queue or domain lease. Every ready worker must attest the frozen Deployment
UID, ServiceAccount UID, imageID, and artifact/callable inventory or it cannot
claim the handler.

The dark-by-default Helm contract is:

```yaml
resourceActions:
  authorityWorker:
    enabled: false
    activeCohort: ""
    cohorts: []                 # [{id, replicas, image, imagePullPolicy}]
    retirementTombstones: []    # [cohort-id]; exact names derive from the ID
    healthPort: 46581
    preflightPort: 46583
    auth:
      existingSecret: ""
      key: tokens
    tls:
      existingSecret: ""
      certKey: tls.crt
      privateKeyKey: tls.key
      caKey: ca.crt
```

Every v1 cohort entry requires a DNS-label `id`, `replicas: 2`, a digest-pinned
`image`, and `imagePullPolicy: Always`. When enabled, `activeCohort` names
exactly one entry. Helm renders one immutable, version-named normal-executor
Deployment and one immutable version-named ServiceAccount per entry, one
ClusterIP preflight Service selecting only the active cohort, purpose-token/TLS
projections, the canonical static cohort manifest as an immutable read-only
projected file, and NetworkPolicy. It renders no additional queue, Ingress, LoadBalancer, or
mutation service, and the runtime needs no ConfigMap GET permission.
Each retirement tombstone is a previously rendered cohort ID; the chart derives
its two fixed names and rejects arbitrary name overrides. Operational removal
requires the registry row to be `REMOVAL_AUTHORIZED` with those exact names and
UIDs before the tombstone-bearing upgrade.

A cohort ID permanently binds its static manifest plus Deployment and
ServiceAccount UIDs. An upgrade never changes those fields in place: it adds a
new cohort. The first worker self-attests its projected manifest/live owner
chain and inserts a `REGISTERING` Serve033 identity; each of the two distinct
ready Pods exactly adopts that identity and appends its bounded registration.
Only the typed two-worker/Deployment-readiness transaction changes it to
`ACCEPTING`, after which the cohort may be selected. New launch/down
preparations first create a `PREPARING` reference under that `ACCEPTING`
identity and then freeze the same resolved cohort returned by preflight.
Kubernetes readiness during `REGISTERING` proves only the self-attestation/
health contract; the existing queue claim predicate remains disabled until
both `ACCEPTING` and active selection hold, so activation has no readiness
cycle.

Moving active selection away does not remove the old cohort. The typed
retirement helper first locks it and commits `DRAINING`, which rejects new
preparation references while existing preparation, private-shadow, and action
references remain claimable. After all active references release, one
transaction locks the cohort and its references, performs fail-closed
nonlocking defensive reads over authoritative action specs/attempts/requests,
Serve parent/child evidence, and private shadow requests/coverage attempts, and
commits `REMOVAL_AUTHORIZED`. Unknown, malformed, inaccessible, cross-identity,
or ambiguously decoded state counts as a reference. Only then may the current
chart remove the Deployment and ServiceAccount; exact NotFound commits
`RETIRED`. Removal moves their exact names into
`authorityWorker.retirementTombstones`; the surviving API-role retirement
verifier keeps tombstone-scoped release-namespace GET permission, performs the
NotFound checks, and commits `RETIRED` before a later chart upgrade prunes that
permission. The removed worker/ServiceAccount is never the verifier. A
prepared-but-unadmitted decision therefore pins its old cohort, as do private
shadow requests and nonterminal actions. Rollback may change
`DRAINING -> ACCEPTING` only in the transaction that replaces registration
evidence with two current Pod attestations while the exact Deployment,
ServiceAccount, qualified image, and manifest still exist; a retired ID is
never recreated.

Claim filtering is by a closed server-owned handler allowlist plus frozen
cohort predicate in the existing queue query. For action requests the query
joins the existing action/attempt correlation and matches the immutable spec's
cohort ID/Deployment UID; for private shadow-candidate requests it matches the
same closed cohort fields in their internal payload. A cohort admits only its
matching private shadow/resource-action handlers; ordinary normal executors
exclude them, and every cohort excludes unrelated public handlers. A
missing/mixed allowlist, unknown cohort, mutable cohort manifest, or handler
inventory blocks shadow-window collection and authority. This adds no request
class, queue row type, claim token, heartbeat, or lease.

Action claims additionally require the same decision's `ACTION_ACTIVE`
reference; private shadow claims require its `SHADOW_ACTIVE` reference. A
`DRAINING` cohort does not invalidate either. A released, missing,
cross-decision, or cross-Deployment reference rejects the claim.

Deployment precreates and freezes the namespace, a no-permission workload
ServiceAccount with token automount disabled and no image pull secrets, the
authorization bindings, a validating admission policy/binding, and the
NetworkPolicy for the two load-balancer slots and Skylet management path. The
two warm-standby LB Deployments use distinct named ServiceAccounts with explicit
false token automount; their immutable selectors, complete Pod-template labels,
Deployment names/UIDs/generations, and ServiceAccount names are the endpoint
caller workload projections. A rollout that reuses the default ServiceAccount,
shares one ServiceAccount across slots, or leaves either Deployment projection
unavailable cannot enter shadow or authority. The
RBAC grants are exact and split by scope:

- a ClusterRole/Binding permits GET of only the named release, canary,
  `kube-system`, and two LB Namespaces; the named ValidatingAdmissionPolicy and
  Binding; and nonresource `/version`. It permits CREATE of SelfSubjectReview,
  SelfSubjectRulesReview, and SelfSubjectAccessReview only. Kubernetes cannot
  apply `resourceNames` to create; these self-only review resources are the
  sole cluster create verbs. There are no persistent cluster writes;
- a release-namespace authority-self-attestation Role/Binding permits GET of
  Pods and ReplicaSets because
  rollout-generated names cannot be predeclared, plus GET of the exact rendered
  versioned authority Deployments and authority/controller ServiceAccounts by
  `resourceNames`; it is bound only to authority-worker ServiceAccounts. A
  distinct release-namespace retirement-verifier Role/Binding grants the
  surviving API role GET only for Deployment and ServiceAccount names derived
  from `retirementTombstones` through the exact NotFound/`RETIRED` transition;
  it is pruned afterward. Neither Role grants Apps/Core mutation;
- a canary-namespace Role/Binding permits GET/CREATE/DELETE Pods and Services
  and GET of the exact workload ServiceAccount and NetworkPolicy. Dynamic
  create names cannot be RBAC-restricted; the typed session and admission
  policy are the name/shape fences; and
- each LB role Namespace has a Role/Binding, bound only to authority-worker
  ServiceAccounts, granting GET by `resourceNames` of its one named LB
  ServiceAccount and one named LB Deployment. Required Namespace aliases may
  co-locate these rules in the release Namespace but do not broaden either name.

No rule grants list/watch/patch/update/deletecollection, Secret/ConfigMap read,
RBAC/admission mutation, PVC, Ingress, or Deployment/ReplicaSet mutation. The
precreated validating policy is bound only to the canary namespace and exact
authority username. On CREATE it enforces the allowed prefix, UUID labels,
workload ServiceAccount, digest image, no secret/token references, and closed
Pod/Service shapes; on DELETE it requires the old object's identity labels.
UID delete preconditions remain an independent application/session fence.

Shadow activation remains blocked until a launch-capability in-cluster preflight
on the deployed cohort proves all 12 semantic prerequisite roles and their exact
alias/projection contract, both LB selector-to-Deployment-to-ServiceAccount
projections, complete SelfSubjectRulesReview, every required/forbidden launch
access review, admission and network policy fingerprints, policy/managed-secret
absence, and the renderer/runtime inventory. This is a cohort rollout gate, not
an instruction to copy endpoint evidence into a down action. A down action runs
its smaller kind-specific preflight and access matrix with the same 12-role
inventory and zero LB Deployment GETs. Historical or out-of-cluster evidence is
insufficient for either.
Authority additionally requires the complete zero-divergence effect window,
201/409/defaulting/admission fixtures, every phase-cursor crash test, action-keyed
job recovery, and live application-port reachability from both load-balancer
slots.

Unknown or drifted provider evidence fails closed to `BLOCKED`. Operators may
inspect and repair it, but cannot replace the frozen locator or fabricate an
absence result through a public API.

## Open gates

- Exact inventory of existing providers that can propagate a stable
  cluster-record UUID/incarnation before launch; multi-node/compound launch is
  ineligible until all effects have one exact observable target contract.
- The current branch has no checked-in candidate `pod_cluster_v1` renderer,
  binding schema, or `kubernetes_admitted_object_v1` normalizer, so
  `unrepresented_execution_config` and shadow-only routing remain mandatory.
  Add those three pinned artifacts plus realistic/candidate-maximal body
  goldens, prebooted runtime, action-keyed Skylet, mutation-trace,
  handle/endpoint, and observation fixtures against real Kubernetes, then
  exercise the exact 12-role in-cluster principal/authorization/admission/
  network preflight plus both launch LB Deployment-to-ServiceAccount projections
  on the selected Boltz canary path, and verify the down preflight performs zero
  LB Deployment GETs.
- Implementation and contract verification of execution config and access
  inventory, the two nonrecursive policy-subject projectors and their contextual
  consumption, complete preflight seeds and controller/executor dual
  policy-absence proof sequence,
  preparation/counted-slot gate,
  normalized spec/partial UID-qualified adoption/deletion, redacted invocation
  builder, private handler claim filter, and request-handler pre-I/O/
  operation-ID callbacks without duplicating provider policy. The exact
  cluster-row UUID primitive and generic API006 progress journal are complete;
  the Serve-owned cursor validator/reducer remains open.
- Before the live Kubernetes normalizer is implemented, freeze exact server-URL
  decomposition and rejection rules, including host, default-port, path, IPv6,
  IDNA, and percent-encoding handling. The pure transport DTO intentionally
  does not invent those source-normalization rules.
- Implementation and verification of cross-attempt cursor carry, effect-fenced
  worker attestation, quiesced superseded partial-launch cleanup, current
  down-only execution authority, the closed effect-body trace, qualified
  manifest/config/CRI runtime identity, and the Skylet fsynced outbox/run-token/
  post-exec-handshake recovery state machine, including full retained-request
  reconstruction and equal-hash/unequal-byte conflict handling.
- Design-complete/code-pending: this canonical file now freezes the literal
  five-effect and every-phase prefix tables, immutable full effect claims,
  committed-effect hash preimages and created/adopted dispositions, CoreV1 422
  observation matrix, cluster-row disposition matrix, Skylet rejection matrix,
  lexicographic attempt/generation origin ordering, closed handler terminal
  result, candidate-maximal size representability fixtures/preflight, and
  quiescence ownership.
  Implement and adversarially verify the Serve-owned API006 progress
  validator/reducer and owner-fenced quiescence builder before partial cleanup
  admission; provider authority remains disabled until both realistic and
  candidate-maximal size goldens are measured below the bound. No deployable
  `PriorLaunchBasisV1` parser or admission path may be
  exposed until it accepts and fully validates both `completed_launch` and
  `partial_launch_cleanup`; the generic opaque API006 JSONB substrate requires
  no second queue or database migration for this provider-specific validator.
- Rendered and live verification of the dedicated authority-worker Helm
  versioned-cohort contract, `REGISTERING`/two-ready-Pod activation,
  release-namespace worker/Service/RBAC/projections, two distinct frozen LB
  Deployments and explicit ServiceAccounts, separate canary workload namespace,
  purpose-specific TLS/token transport, exact same-client facade and
  RBAC/access-review matrices, controller-only preflight network path,
  frozen-cohort claim routing/retention, surviving-API tombstone verification,
  and the pinned API -> new worker cohort -> controller rollout/current-chart
  rollback sequence.
- A measured complete-shadow window and minimum volume for launch, retry,
  ambiguity, and down.
