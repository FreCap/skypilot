# SkyServe Resource-Action Provider Facet

Status: bounded M0 accepted after independent adversarial review; parent M1
kernel complete; M2 cluster identity, initial immutable provider contracts, and
typed shadow-store and Serve033 coverage/promotion foundations plus the generic
API006 progress substrate are implemented and locally verified; the
candidate-only normalization boundary and atomic durable coverage handshake are
in progress; the closed Kubernetes transport/scope leaf is implemented and
independently verified, while execution-config closure, runtime provider
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
typed retry/uncertain outcome before they escape. The same request ID never
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

ProviderLaunchEffectDefinitiveNoEffectV1 = one of:
  {version: 1,
   proof_kind: "core_v1_422_no_create",
   request_body_sha256: Sha256,
   response_status: 422,
   post_observation: ProviderLifecycleObservationV1}
  {version: 1,
   proof_kind: "cluster_record_no_commit",
   intended_handle_sha256: Sha256,
   transaction_result: "rolled_back" | "conflict_no_write",
   cluster_name: Text,
   expected_cluster_record_uuid: UUID,
   post_read_disposition: "not_found" | "different_identity_conflict",
   observed_cluster_record_uuid: null | UUID,
   observed_handle: null | ProviderKubernetesHandleV1,
   observed_at: UtcTimestamp}
  {version: 1,
   proof_kind: "skylet_rejected_before_job_commit",
   submit_request_sha256: Sha256,
   rejection: "same_key_different_spec" | "schema_rejected",
   post_job: ProviderSkyletJobEvidenceV1,
   pending_start_outbox: false,
   active_run_token: false}

ProviderLaunchEffectQuiescenceV1 = {
  effect_sequence: 0 | 1 | 2 | 3 | 4,
  effect_kind: "core_v1_create" | "cluster_record_insert" |
               "skylet_job_submit",
  role: null | ProviderObjectRoleV1,
  intent_phase: "CREATE_INTENT" | "HANDLE_INTENT" | "JOB_INTENT",
  resolution: "evidence_committed" | "definitive_no_effect" |
              "call_not_entered",
  evidence_sha256: null | Sha256,
  definitive_no_effect: null | ProviderLaunchEffectDefinitiveNoEffectV1,
  request_execution_generation: PositiveInteger
}

ProviderLaunchSupersessionQuiescenceV1 = {
  version: 1,
  launch_action_id: UUID,
  launch_attempt: PositiveInteger,
  request_id: UUID,
  request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
  active_claim: false,
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

`PriorLaunchBasisV1` intentionally has no `NOT_STARTED` variant. After fencing
and terminalizing any materialized request, a launch whose current attempt
still has `provider_io_boundary='NOT_STARTED'` and null API006 progress is
cancelled as a proved no-effect launch. A retry attempt with an inherited
nonnull cursor is not eligible for this cancellation even if its own provider-
I/O watermark remains `NOT_STARTED`. The Serve transaction creates no down
action, down link, cleanup target, or prior-launch basis; it terminalizes the
launch with `terminal_disposition='CANCELLED_NO_EFFECT'`, releases its counted
slot/capacity exactly once, and applies the owner-fenced replica/generation
cancellation projection. A real down action is required only after the launch
has action-wide provider-I/O-started evidence or a completed launch exists.
`launch_io_started` is a typed predicate over the locked retained attempt
chain: at least one attempt has
`provider_io_boundary != 'NOT_STARTED'`. The request-lifecycle
`mutation_boundary='SETTLED'` never satisfies it by itself. An exact inherited
nonnull cursor keeps the predicate true even when the current retry's own
watermark remains `NOT_STARTED`, because admission revalidates its byte-equal
predecessor chain back to the attempt that crossed the watermark. Such a retry
must be reconciled and quiesced; it cannot fall back to direct no-effect
cancellation.

Supersession quiescence uses action-internal mutation order: create roles 0-2,
cluster-record insert 3, and Skylet submit 4. This is deliberately distinct
from the legacy wire-effect trace, which excludes the local cluster-row
transaction and therefore numbers Skylet submit as 3.

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
exact same-UUID cluster-row read. The quiescence list is the exact canonical
prefix of mutation intents present in the cursor. Each entry has exactly one
resolution. `evidence_committed` has a nonnull `evidence_sha256` naming
immutable effect evidence retained by the cursor and has
`definitive_no_effect=null`. `definitive_no_effect` embeds the complete closed
proof above, sets `evidence_sha256` to its canonical hash, and must match the
entry's sequence, kind, role, intent phase, request generation, frozen request
bytes, and current cursor. `call_not_entered` has both evidence fields null and
is written by the same-generation handler before entering that effect call.

A definitive-no-effect proof is valid only for a synchronous Kubernetes 422
that cannot persist the object, a cluster-record transaction proved not to
have committed, or a Skylet protocol rejection proved to occur before job-row
commit. A same-key/different-spec Skylet conflict additionally requires a
terminal-or-`BLOCKED` conflicting job with no pending start outbox or active run
token; a runnable conflicting job blocks handoff. Timeout, reset, 5xx, lost
acknowledgement, expired lease, or a point-in-time NotFound never qualifies.
The handler claim-fenced-commits the
proof in the typed terminal result; request terminalization atomically closes
the envelope with its own terminal state and `active_claim=false` without
reading or locking the attempt;
the reducer later byte-compares the supplied cursor/effect commitments to
API006 before copying the final `ProviderLaunchSupersessionQuiescenceV1` into
the attempt outcome. Callers cannot supply it.
An ambiguous or in-flight `CREATE_INTENT`/`JOB_INTENT`, a lost acknowledgement
with no exact evidence, or a merely expired lease is not eligible: the old
launch remains observation-first and the down action is not admitted. The
request must be terminal with no active claim before admission, so no old
handler can emit another effect. Unknown UID slots are allowed only for roles
whose create intent never existed, has `call_not_entered`, or has a matching
`core_v1_422_no_create` definitive-no-effect proof; they are never guessed from
NotFound.

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
  source: ProviderLaunchInvocationV1.source,
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

ProviderPodTopologyV1 = {
  version: 1,
  kind: "single_direct_pod_two_services",
  node_count: 1,
  application_port: DecimalPortText,
  resources_ports: [DecimalPortText],
  mutable_objects: [
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

`ProviderKubernetesObjectRoleMapV1` is a literal protocol constant, not
configuration. Topology objects, execution-config object plans, partial/full
targets, and observation objects all serialize in plan-sequence order from
that table. Launch mutation effects use create sequence followed by the Skylet
submit at effect sequence three; down effects use delete sequence. A role,
kind, name, or sequence mismatch is invalid even when every object is otherwise
canonical.

The only v1 server allocations, in exact pointer order within each role, are:

```text
head_ssh_service: /spec/clusterIP, /spec/clusterIPs, /spec/ipFamilies,
                  /spec/ipFamilyPolicy                    (api_server)
head_service:     /spec/clusterIP, /spec/clusterIPs, /spec/ipFamilies,
                  /spec/ipFamilyPolicy                    (api_server)
head_pod:         /spec/nodeName                           (scheduler)
```

The requested Pod body must omit `spec.nodeName`; an input that sets it is not
representable. Allocation arrays follow the table exactly. UID and semantic
hash commitments are write-once, while an allowed allocation may be appended
once after UID commitment; it can never be removed or changed.

`request_body` is the exact nonsecret CoreV1 body sent by the session and is
bounded together with the complete action spec to 65,536 canonical UTF-8
bytes. The candidate rejects any env value other than its fixed nonsecret
replica ID, any Secret/config-map reference, projected token, credential,
private key, raw user YAML, or unbounded field. Both body and requested semantic
preimages are embedded next to their hashes; the implementation does not rely
on a hash-only private interpretation.

`kubernetes_admitted_object_v1` is implemented by the checked-in, pinned
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
and keeps that target alive for SelfSubjectReview/AccessReview, namespace and
service-account reads, exact object reads, creates, and deletes in that attempt.
It re-reads both namespaces and both service accounts with that same client
after the final provider call before accepting evidence, then closes the
target. Scope evidence always uses `ProviderKubernetesScopeReadV1`: a failed
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
UID-preconditioned Pod/Service delete. AppsV1 permits exact current ReplicaSet
and frozen Deployment GET only. NetworkingV1 permits exact named NetworkPolicy
GET. AdmissionregistrationV1 permits exact named policy and binding GET.
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
value already frozen in `PreparedLaunchRequest.body.user_hash`; the raw request
environment map is never persisted. A new pure
`make_cluster_name_on_cloud_for_user(..., user_hash=...)` owns the historical
normalization/truncation algorithm. The existing ambient helper becomes a
compatibility wrapper that supplies `get_user_hash()`. The pure result must
equal `provider_cluster_name`; `workload_name` must equal
`provider_cluster_name + "-head"`. Invalid or overlong user hashes are not
representable. Execution recomputes these names from the basis and never reads
a later ambient user identity.

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
Each of the three mutable objects carries
the exact final label map, including display-cluster name,
`skypilot.co/cluster-record-uuid`, and
`skypilot.co/serve-replica-incarnation`. Source, policy, resource, and
`custom_metadata` inputs that contain either reserved identity label are
rejected before the system labels are injected after all allowed merges.
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
   known_objects: PartialResolvedProviderTargetV1,
   pre_observation: ProviderLifecycleObservationV1}
  {version: 1, action_kind: "launch", phase: "OBJECTS_PARTIAL",
   known_objects: PartialResolvedProviderTargetV1,
   post_observation: ProviderLifecycleObservationV1}
  {version: 1, action_kind: "launch", phase: "OBJECTS_EXACT",
   resolved_target: ResolvedProviderTargetV1,
   post_observation: ProviderLifecycleObservationV1}
  {version: 1, action_kind: "launch", phase: "HANDLE_INTENT",
   resolved_target: ResolvedProviderTargetV1,
   intended_handle: ProviderKubernetesHandleV1}
  {version: 1, action_kind: "launch", phase: "HANDLE_COMMITTED",
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1}
  {version: 1, action_kind: "launch", phase: "RUNTIME_READY",
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1,
   runtime_evidence: ProviderKubernetesRuntimeEvidenceV1}
  {version: 1, action_kind: "launch", phase: "JOB_INTENT",
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1,
   runtime_evidence: ProviderKubernetesRuntimeEvidenceV1,
   submit_request: ProviderSkyletSubmitRequestV1}
  {version: 1, action_kind: "launch", phase: "JOB_COMMITTED",
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1,
   runtime_evidence: ProviderKubernetesRuntimeEvidenceV1,
   job: ProviderSkyletJobEvidenceV1}
  {version: 1, action_kind: "launch", phase: "JOB_RUNNING",
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1,
   runtime_evidence: ProviderKubernetesRuntimeEvidenceV1,
   job: ProviderSkyletJobEvidenceV1}
  {version: 1, action_kind: "launch", phase: "ENDPOINT_RESOLVED",
   resolved_target: ResolvedProviderTargetV1,
   handle: ProviderKubernetesHandleV1,
   runtime_evidence: ProviderKubernetesRuntimeEvidenceV1,
   job: ProviderSkyletJobEvidenceV1,
   endpoint: ProviderKubernetesEndpointEvidenceV1}
  {version: 1, action_kind: "launch", phase: "SUCCEEDED",
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

Unknown keys are forbidden in every variant. Launch alternates
`CREATE_INTENT(role) -> OBJECTS_PARTIAL` in canonical create order, then follows
`OBJECTS_EXACT -> HANDLE_INTENT -> HANDLE_COMMITTED -> RUNTIME_READY ->
JOB_INTENT -> JOB_COMMITTED -> JOB_RUNNING -> ENDPOINT_RESOLVED -> SUCCEEDED`.
`CREATE_INTENT.role` is exactly the first unknown role. `OBJECTS_PARTIAL` has
one to three committed slots; three is permitted while scheduler `nodeName` is
still absent. `OBJECTS_EXACT` requires three UIDs, all required server
allocations, and authoritative exact semantic readback. `HANDLE_INTENT` freezes
the complete intended handle before cluster-row I/O. `JOB_COMMITTED` requires a
present, fsync-committed same-key/byte-equal job and nonnull job ID;
`JOB_RUNNING` additionally requires its exact durable state to be `RUNNING`.
Launch `SUCCEEDED` retains every proof and an authoritative `present`
observation.

Progress is never validated as a free-standing union. The typed API006 store
receives the exact immutable action ID, kind, plan, spec hash, and (for down)
prior-launch basis. It requires `cursor.action_kind` to equal the action kind;
every requested-target, cleanup-target, prior-basis, resource, cluster-record,
service/replica-incarnation, and nested object hash/identity to equal the
applicable frozen preimage; and every launch submission key to equal the launch
action ID. All repeated resolved targets, handles, runtime/job records, and
endpoint or observation targets are mutually byte-equal. For down,
`delete_target.observation`, each later `absence_observation`, and the final
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
effect.

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
`CREATE_INTENT(first_missing_role)` and down uses `TARGET_RESOLVED`. An exact
`NOT_STARTED` inherited revision-one seed instead atomically commits
`INTENT_COMMITTED` to both fields and binds its current worker attestation to
the already-carried, predecessor-validated cursor. An authoritative attempt
therefore cannot have `provider_io_boundary != NOT_STARTED` with null API006
progress. A request is
never locked before its action. The handler then performs one bounded
fixed-topology session mutation group. The first effect uses the combined
boundary/intent write above; before each later CoreV1 or Skylet effect it
commits the corresponding monotonic
`ProviderLifecycleProgressV1` intent; after an exact readback it commits the
resulting UID/spec/allocation/handle/job evidence before the next effect. A
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
`unknown`. The Serve reducer decides retry, block, or terminal failure.

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
   `OBJECTS_PARTIAL` target; repeat the intent/evidence pair for later roles;
3. wait for the scheduler's write-once Pod `nodeName`, exact-read all three
   admitted specs/UIDs/allocations, and commit `OBJECTS_EXACT`;
4. construct and commit the full intended handle, including that node name and
   same-UID Pod IP, at `HANDLE_INTENT`; exact-insert/adopt the same-UUID cluster
   row and commit `HANDLE_COMMITTED`;
5. verify the digest-pinned prebooted container's exact artifact measurements,
   startup probe, Ray/Skylet health, and state-store UUID, then commit
   `RUNTIME_READY`;
6. construct the closed `ProviderSkyletSubmitRequestV1`, commit `JOB_INTENT`,
   and send the one keyed Skylet RPC;
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
existing job ID for the same key and byte-equal contract/spec, rejects any
difference, and supports readback by key. Hash equality alone is insufficient.
`JOB_INTENT` commits before the RPC. After a timeout or worker death, recovery
queries by key before any send; same-spec presence adopts, absence permits the
same keyed send, and conflict blocks. The public/generic `backend.execute()`
path has no authority fallback.

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

There is no provider-facet cleanup deadline. Serve may schedule bounded
database-clock retries indefinitely.

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
and no unlisted option. Each CoreV1 path is the exact scope/kind/name path
derived from the frozen plan; create and delete query objects are empty. Create
and delete sequences equal the role map's respective sequence. For Skylet, the body is
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
    workspace: ParentSpec.invocation.require_launch().source.workspace,
    purge: false,
    graceful: false,
    graceful_timeout: null
  }
}

ServeShadowAttemptInvocationV1 =
  ProviderLifecycleInvocationV1 |
  ServeLegacyLaunchCleanupDownInvocationV1

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

ProviderKubernetesServiceAccountProjectionV1 = {
  namespace: Text,
  name: Text,
  uid: Text,
  resource_version: Text,
  labels: [{key: Text, value: Text}],
  annotations: [{key: Text, value: Text}],
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

ProviderKubernetesResourceRuleV1 = {
  api_groups: [ProviderKubernetesApiGroupV1],
  resources: [ProviderKubernetesResourceV1],
  resource_names: [Text],
  verbs: [ProviderKubernetesVerbV1]
}

ProviderKubernetesNonResourceRuleV1 = {
  urls: ["/version"],
  verbs: ["get"]
}

ProviderKubernetesRulesReviewV1 = {
  namespace: Text,
  incomplete: false,
  evaluation_error: false,
  resource_rules: [ProviderKubernetesResourceRuleV1],
  non_resource_rules: [ProviderKubernetesNonResourceRuleV1]
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
  access_decisions: [ProviderKubernetesAccessDecisionV1],
  access_decisions_sha256: Sha256
}

ProviderKubernetesPrincipalsV1 = {
  caller: ProviderKubernetesServiceAccountProjectionV1,
  workload: ProviderKubernetesServiceAccountProjectionV1,
  caller_authorization: ProviderKubernetesAuthorizationEvidenceV1
}

ProviderKubernetesPrerequisiteV1 = {
  api_version: Text,
  kind: "Namespace" | "ServiceAccount" | "NetworkPolicy" |
        "ValidatingAdmissionPolicy" | "ValidatingAdmissionPolicyBinding",
  namespace: null | Text,
  name: Text,
  uid: Text,
  resource_version: Text,
  spec: ProviderKubernetesPrerequisiteSpecV1,
  spec_sha256: Sha256
}

ProviderKubernetesPrerequisiteSpecV1 = one of:
  {kind: "Namespace", labels: [{key: Text, value: Text}],
   annotations: [{key: Text, value: Text}], deletion_timestamp: null}
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
  source: ProviderLaunchInvocationV1.source
}

ProviderKubernetesPostProvisionV1 = {
  runtime_mode: "prebooted_ray_skylet_v1",
  runtime_artifacts: [ProviderWorkloadArtifactBindingV1],  # exact role order
  provision_runtime_metadata: {
    runtime_setup_done: true,
    has_ray: true,
    has_skylet: true,
    has_job_queue: true,
    workdir_synced: false,
    file_mounts_synced: false,
    setup_done: true,
    run_started: false
  },
  sync_workdir: "assert_absent_skip",
  sync_file_mounts: "assert_absent_skip",
  user_setup: "assert_null_skip",
  pre_exec_hooks_autostop: "assert_absent_skip",
  management_transport: "skylet_grpc_only",
  management_port: DecimalPortText,
  ssh_fallback: false,
  job_submission: {
    protocol: "skylet_idempotent_submit_v1",
    submission_key_source: "launch_action_id",
    run_source: ProviderLaunchInvocationV1.source,
    contract: ProviderSkyletJobContractV1,
    durability: ProviderSkyletDurabilityContractV1,
    job_spec_profile: "ProviderSkyletJobSpecV1"
  }
}

ProviderKubernetesEndpointContractV1 = {
  mode: "podip",
  application_port: DecimalPortText,
  ambient_fallback: false,
  network_prerequisites: [ProviderKubernetesPrerequisiteV1],
  required_callers: [
    {role: "serve_lb_slot_0", namespace: Text, namespace_uid: Text,
     pod_selector: [{key: Text, value: Text}], service_account_name: Text,
     service_account_uid: Text},
    {role: "serve_lb_slot_1", namespace: Text, namespace_uid: Text,
     pod_selector: [{key: Text, value: Text}], service_account_name: Text,
     service_account_uid: Text}
  ]
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

ProviderAuthorityWorkerIdentityV1 = {
  namespace: Text,
  pod_name: Text,
  pod_uid: Text,
  pod_resource_version: Text,
  pod_service_account_name: Text,
  pod_controller_owner: {api_version: "apps/v1", kind: "ReplicaSet",
                         name: Text, uid: Text},
  replica_set_name: Text,
  replica_set_uid: Text,
  replica_set_resource_version: Text,
  replica_set_controller_owner: {api_version: "apps/v1", kind: "Deployment",
                                 name: Text, uid: Text},
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

ProviderKubernetesExecutionCapsuleV1 = {
  version: 1,
  implementation_contract: "kubernetes_serve_prebooted_runtime_v1",
  executor_cohort: ProviderAuthorityWorkerCohortV1,
  config_projection: ProviderKubernetesConfigProjectionV1,
  config_projection_sha256: Sha256,
  scope: ProviderKubernetesScopeV1,
  principals: ProviderKubernetesPrincipalsV1,
  prerequisites: [ProviderKubernetesPrerequisiteV1],
  request_identity: {
    cleaned_user: Text,
    original_user: Text,
    frozen_user_hash: Text
  },
  resources: ProviderKubernetesResourceContractV1,
  renderer: ProviderKubernetesRendererV1,
  objects: [ProviderKubernetesObjectPlanV1],
  post_provision: ProviderKubernetesPostProvisionV1,
  endpoint: ProviderKubernetesEndpointContractV1,
  scheduling: {
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
  },
  storage: {
    persistent_volumes: [], object_stores: [], file_mounts: [],
    workdir: null, fuse: false, docker_cache: false, auto_mounts: false
  },
  metadata: {
    global_labels: [], custom_pod_config: null, custom_metadata: [],
    reserved_labels_injected_last: true
  },
  security: {
    tls_material: null, managed_secrets: [], task_secrets: [],
    service_account_bootstrap: false, rbac_bootstrap: false
  },
  topology: ProviderPodTopologyV1,
  mutation_contract: {
    role_map_contract: "ProviderKubernetesObjectRoleMapV1",
    create_effects: [{sequence: 0, role: "head_ssh_service", kind: "Service"},
                     {sequence: 1, role: "head_service", kind: "Service"},
                     {sequence: 2, role: "head_pod", kind: "Pod"}],
    delete_effects: [{sequence: 0, role: "head_service", kind: "Service"},
                     {sequence: 1, role: "head_ssh_service", kind: "Service"},
                     {sequence: 2, role: "head_pod", kind: "Pod"}],
    job_effect: "one_action_keyed_skylet_submit",
    allowed_patches: [], allowed_updates: [], allowed_collection_deletes: [],
    delete_requires_identity_labels_and_uid_precondition: true,
    create_409: "exact_admitted_readback_or_conflict",
    create_422: "terminal_no_rewrite"
  }
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

ProviderLaunchPolicySubjectV1 = {
  version: 1,
  source: ProviderLaunchInvocationV1.source,
  requested_target: ProviderLifecycleInvocationV1.requested_target,
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

ProviderKubernetesDownExecutionCapsuleV1 = {
  version: 1,
  implementation_contract: "kubernetes_serve_exact_cleanup_v1",
  executor_cohort: ProviderAuthorityWorkerCohortV1,
  config_projection: ProviderKubernetesConfigProjectionV1,
  config_projection_sha256: Sha256,
  scope: ProviderKubernetesScopeV1,
  principals: ProviderKubernetesPrincipalsV1,
  prerequisites: [ProviderKubernetesPrerequisiteV1],
  request_identity: {
    cleaned_user: Text,
    original_user: Text,
    frozen_user_hash: Text
  },
  cleanup_target: ProviderKubernetesCleanupTargetV1,
  cleanup_target_sha256: Sha256,
  mutation_contract: {
    role_map_contract: "ProviderKubernetesObjectRoleMapV1",
    delete_effects: [{sequence: 0, role: "head_service", kind: "Service"},
                     {sequence: 1, role: "head_ssh_service", kind: "Service"},
                     {sequence: 2, role: "head_pod", kind: "Pod"}],
    delete_requires_identity_labels_and_uid_precondition: true,
    cluster_record_removal: "same_uuid_exact_handle_after_absence_v1",
    allowed_creates: [], allowed_patches: [], allowed_updates: [],
    allowed_collection_deletes: []
  }
}

ProviderDownPolicySubjectV1 = {
  version: 1,
  requested_target: ProviderLifecycleInvocationV1.requested_target,
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

ProviderDownPolicyBoundaryProofV1 = {
  version: 1,
  boundary: "serve_controller_prepare" | "api_executor_pre_io",
  config_projection_sha256: Sha256,
  modes: ProviderPolicyModeEvidenceV1,
  policy_subject_sha256: Sha256,
  projection_before_sha256: Sha256,
  projection_after_sha256: Sha256,
  projections_equal: true
}

ProviderKubernetesDownExecutionConfigV1 = {
  version: 1,
  capsule: ProviderKubernetesDownExecutionCapsuleV1,
  execution_capsule_sha256: Sha256,
  policy_subject: ProviderDownPolicySubjectV1,
  policy_subject_sha256: Sha256,
  policy: {
    controller: ProviderDownPolicyBoundaryProofV1,
    executor: ProviderDownPolicyBoundaryProofV1
  }
}

ProviderLifecycleExecutionCapsuleV1 =
  ProviderKubernetesExecutionCapsuleV1 |
  ProviderKubernetesDownExecutionCapsuleV1

ProviderLifecyclePolicyBoundaryProofV1 =
  ProviderPolicyBoundaryProofV1 | ProviderDownPolicyBoundaryProofV1
```

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
envelope. Each execution config embeds its capsule and policy subject exactly
once and stores both recomputed hashes. The capsule likewise stores its
config-projection hash. The subject contains `execution_capsule_sha256`, never
the capsule or full execution config. Each boundary proof stores only the
recomputed config-projection hash, the same policy-subject hash, and the
before/after hashes. Admission requires both proof
config-projection hashes to equal `capsule.config_projection.sha256`; the
capsule's stored projection hash to equal that same value; the config's stored
capsule hash and subject capsule hash to equal `capsule.sha256`; both proof
subject hashes to equal `policy_subject.sha256`; and every before/after hash to
equal that same subject hash when `projections_equal=true`. Thus every
nonsecret preimage is co-located exactly once rather than repeating the capsule
and object bodies four times. A bare hash outside this typed enclosing config
remains non-authoritative. Raw effective config, raw tasks, admin-policy output, and
managed-secret responses are forbidden. The checked-in config-access inventory
is the finite list of reads; an unlisted read or a value outside the closed
projection makes the candidate not representable.

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
and both policy subjects are exhaustively byte-equal to the down invocation,
plan, preflight result, and retained target fields; any contradiction is not
representable.

SelfSubjectReview, RulesReview, and AccessReview responses are immediately
normalized into the named nonsecret types above. The access-matrix artifact
contains the exact ordered required and forbidden checks; check sequences must
match it. Wildcard groups/resources/verbs, unknown nonresource URLs, extra
identity groups/keys, incomplete rules, evaluation errors, and any result that
differs from `expected_allowed` are rejected. Kubernetes reason/error strings
and raw review bodies are never persisted. Prerequisite manifests are loaded
from their content-addressed artifacts and byte-compared to the typed live
projection; arbitrary Kubernetes response JSON is not stored.

Cross-field validation is exhaustive. Both resource/image copies and the Pod
container image are byte-equal. `source_cpus` and `source_memory_gb` are
nonnull canonical positive decimals with at most three fractional digits and
no `+`, relative `x`, exponent, or unit suffix. The candidate requires
`instance_type == source_cpus + "CPU--" + source_memory_gb + "GB"`,
`pod_cpu_request == pod_cpu_limit == source_cpus`, and
`pod_memory_request == pod_memory_limit == source_memory_gb + "G"`; those four
strings are byte-equal to the normalized Pod spec. The live allocatable clamp
is disabled and cannot rewrite them. Scope/namespace/fingerprint, both
service-account identities, name basis and all object names, final labels,
topology order, source/workspace, image/digest/pull policy, and the one port in
resources/topology/Services/endpoint must also be byte-equal. Every duplicated
cluster UUID, replica incarnation, and down basis is equal to the enclosing
identity. The separate fixed Skylet management port must equal the prebooted
runtime, Pod, and NetworkPolicy projections and is never exposed as a user
resource port. A contradictory but individually canonical object is rejected.

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
name/namespace/UID, it exact-reads its Pod, follows the sole controller owner to
an exact ReplicaSet, then follows that owner to the frozen Deployment. Pod,
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
  source: ProviderLaunchInvocationV1.source,
  requested_cloud: "kubernetes",
  context_mode: "in_cluster",
  target_namespace: Text,
  resources: ProviderPodResourceSnapshotV1,
  topology: ProviderPodTopologyV1,
  replica_id: NonnegativeInteger,
  config_projection: ProviderKubernetesConfigProjectionV1
}

ProviderDownPreflightSeedV1 = {
  version: 1,
  resource_identity: ProviderLifecyclePlanV1.resource_identity,
  workspace: Text,
  requested_target: ProviderLocatorV1,
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
  policy_proof: null | ProviderPolicyBoundaryProofV1,
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
  policy_proof: null | ProviderDownPolicyBoundaryProofV1,
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
selects the kind-specific reason constructor as well as the seed, capsule, and
policy-proof variants; a spelling shared by both reason enums is still decoded
through that selected constructor. Launch produces the launch execution config;
down produces the current down execution config and never copies the launch
capsule. Any wrong-kind reason, seed, capsule, or proof is invalid rather than
coerced into the other variant. The hash covers the request without its hash
field. Nonce and transport envelope are process-local and absent from the
action spec.

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
outside that endpoint and carries no execution authority. The endpoint
returns only the action-kind-matched capsule, policy proof, and current worker identity,
never credentials or a live client. The later request handler independently
reconstructs and byte-compares the result with its one mutation client
immediately before I/O. Preflight is admission evidence, not mutation authority
or a TOCTOU substitute.

The same live execution client re-reads both Namespaces, caller/workload/LB
ServiceAccounts, NetworkPolicy, admission policy/binding, `/version`, and the
worker Pod/ReplicaSet/Deployment chain immediately before the first effect and
after the last. It requires exact UID, resourceVersion, typed spec/artifact
preimage, image qualification, and canonical hash. SelfSubjectReview must
produce the closed frozen identity; SelfSubjectRulesReview must be complete and
byte-equal to its typed preimage; and every required/forbidden access decision
must equal the pinned matrix. Workload token automount, image pull secrets, and
legacy secret refs remain exactly false/empty. Drift is conflict, and post-admission Pod normalization
independently rejects any injected token volume, imagePullSecret, sidecar, or
other field.

For the first cohort, both the controller/client and executor/server admin
policy are proven absent, not merely projection-preserving. Managed-secret
resolution is also proven absent. Each boundary embeds its bounded nonsecret
before/after projection preimages beside their hashes; typed validation
recomputes both and requires byte equality.
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
automount are also rejected. Namespace, both service accounts, admission and
network policies, dual load-balancer caller identities, and executor cohort
must already exist with frozen UIDs/specs, and the caller must satisfy the
complete authorization matrix. An unpinned request is
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
coverage admission, or either authoritative handler. Expected object/trace
parity comes only from the parent launch capsule; any extra ambient cleanup
behavior is divergent and promotion-blocking. Typed reads reconstruct the
exact applicable union member; arbitrary mappings are not accepted. Golden
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
absence against the frozen target. Recovery preserves legacy request
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
- prerequisites and endpoint network prerequisites are sorted by
  `(api_version, kind, namespace-or-empty, name)` and reject duplicate logical
  keys, including two UIDs for one key. All fields displayed as `[]` in a v1
  config are empty-only, not unordered extension points.

Before linked represented admission is enabled, checked-in realistic launch and
down golden fixtures must include the observed 1,036-byte `boltz-test` CA
scalar, three complete requested/semantic object bodies, the full principal/
authorization/prerequisite inventory, six runtime artifacts, and both endpoint
callers. Tests record each full `ServeReplicaActionSpecV1` byte length, require
it to be at most 60,000 bytes (preserving at least 5,536 bytes of rollout
headroom), and still enforce the absolute 65,536-byte parser bound. Failure is
`NOT_REPRESENTABLE`; no truncation, compression, omitted preimage, or external
hash-only lookup is allowed.

The `source` tuple is a content-addressed reference to an immutable retained
`version_specs` row; the builder verifies the row's exact UTF-8 YAML bytes
against `yaml_content_sha256` before use. The first eligible cohort requires
`file_mounts_blob_id=null`, `tls_material_ref=null`, no task
secrets/storage/local mounts, and byte-equal pre/post-policy projections as
defined above. The v1 run source is the reviewed nonsecret canary content
addressed by that row; arbitrary user commands remain not representable until a
separate secret-safe job-input commitment exists. Any other resource field, nondefault launch flag, policy
mutation, secret/material source, or compound topology normalizes to not
representable. This intentionally narrow gate lets the source reference plus
the closed transformations reconstruct the same prepared request without
copying YAML, commands, environment values, or secret bytes into action JSON.

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
allowed transforms once, proves both policy/secret modes, obtains the read-only
scope/config inputs, and privately retains the immutable request. It publishes
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

The live worker uses the same in-memory request object until SDK request
admission, but object identity is not a distributed authority. Across HTTP the
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
  policy trace, pure naming helper, exact three-object renderer/admission
  normalizer, prebooted runtime/job/endpoint contract, and checked-in access/
  call inventory. Restrict v1 normalization to the location-pinned canary
  candidate above.
- Add the policy-free execution capsule, typed policy subject/proofs, closed
  Kubernetes review/prerequisite projections, exact same-client facade
  inventory, versioned worker cohort/image identity, launch/down execution
  capsules, and closed discriminated preflight wire contract.
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
- literal frozen transport/scope bytes/hash, pure user-hash naming, and the
  actual suffixed head-Pod/two-Service names;
- prior-launch basis lookup, mutated-basis/down-digest rejection, embedded
  observation evidence/hash derivation, and nonnull v1 response-hash rejection;
- all exhaustive cross-field equalities, nonnull CPU/memory and literal
  request/limit translations, `imagePullPolicy: Always`, and byte equality to
  the normalized Pod/Service specs;
- exact retained-source verification, both policy boundaries absent,
  byte-equal pre/post projections, and deterministic precedence for every
  closed not-representable reason;
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
  SelfSubjectReview, complete rules preimage, required/forbidden access-review
  matrix, and drift before/after effects;
- one live isolated Kubernetes client for typed scope-before/after, exact
  prerequisite and CoreV1 reads/creates/deletes, with failed scope reads
  encoded, exact worker Pod/ReplicaSet/Deployment GETs, and zero second-client,
  patch/update/collection-delete/Secret/RBAC mutation/PVC/Deployment mutation/
  Ingress/exec/cp calls;
- every facade wraps the object-identical raw `ApiClient`, and the exact
  CoreV1/AppsV1/NetworkingV1/AdmissionregistrationV1/Version/
  AuthenticationV1/AuthorizationV1 method inventory rejects all unlisted
  calls;
- every requested/admitted object semantic preimage/hash, allowed literal
  default/server allocation, 201/409 exact adoption, injected field rejection,
  and partial UID commitment supplied to each later effect;
- the literal three-role plan/create/delete order, requested Pod omission of
  `spec.nodeName`, scheduler-only append of that allocation, incomplete
  pre-scheduling readback, immutable allocation replay, and exact node name in
  the committed handle;
- the same `PreparedLaunchRequest` object through one live admission epoch, and
  full stored spec/invocation equality rather than generic request-byte or
  object-identity authority after recovery;
- the legacy launch-cleanup child is the exact parent-derived special wire
  member, is accepted only for `LAUNCH_CLEANUP_DOWN`, and is rejected as an
  action spec, primary child, coverage input, or authoritative-handler input;
- every content-addressed policy edge recomputes to its one co-located capsule,
  config projection, and policy-subject preimage; crossed controller/executor,
  before/after, capsule, or subject hashes are rejected;
- registration sets require ascending unique worker Pod UIDs and reject both a
  permuted list and duplicate UID;
- realistic launch/down golden specs exercise the full inventories and observed
  CA size, record their canonical byte counts, retain 5,536 bytes of headroom,
  and reject a one-byte-over-budget variant without dropping a preimage;
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
  Deployment chain, the first effect fills one write-once post-attestation,
  later effects match it, and a replacement execution generation binds a new
  attestation to the carried cursor without replaying a committed effect;
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
  no-effect proof, or a same-generation `call_not_entered` proof before
  supersession; after earlier committed effects, a later CoreV1 422, rolled-back
  cluster-record insert, or pre-commit Skylet conflict hands off only with its
  exact proof, while timeout, reset, 5xx, lost acknowledgement, expired lease,
  and post-call NotFound remain ineligible;
- prebooted runtime imageID/startup/Ray/Skylet evidence, asserted no-op generic
  stages, action-keyed same-spec job adoption, different-spec conflict, and no
  SSH/private-key fallback;
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
  release-namespace worker/Service selectors, separate canary workload
  namespace, ClusterIP Service, NetworkPolicy, exact namespaced/cluster RBAC
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
- each LB namespace has a Role/Binding granting only GET of its named LB
  ServiceAccount.

No rule grants list/watch/patch/update/deletecollection, Secret/ConfigMap read,
RBAC/admission mutation, PVC, Ingress, or Deployment/ReplicaSet mutation. The
precreated validating policy is bound only to the canary namespace and exact
authority username. On CREATE it enforces the allowed prefix, UUID labels,
workload ServiceAccount, digest image, no secret/token references, and closed
Pod/Service shapes; on DELETE it requires the old object's identity labels.
UID delete preconditions remain an independent application/session fence.

Shadow activation remains blocked until an in-cluster preflight on the deployed
cohort proves both service-account and namespace UIDs/specs, complete
SelfSubjectRulesReview, every required/forbidden access review, admission and
network policy fingerprints, policy/managed-secret absence, and the renderer/
runtime inventory. Historical or out-of-cluster evidence is insufficient.
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
- Checked-in `pod_cluster_v1` renderer/admission-normalization, prebooted runtime,
  action-keyed Skylet, mutation-trace, handle/endpoint, and observation fixtures
  against real Kubernetes plus the in-cluster namespace/principal/
  authorization/admission/network preflight on the selected Boltz canary path.
- Implementation and contract verification of execution config and access
  inventory, dual policy-absence proof, preparation/counted-slot gate,
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
  post-exec-handshake recovery state machine.
- Rendered and live verification of the dedicated authority-worker Helm
  versioned-cohort contract, `REGISTERING`/two-ready-Pod activation,
  release-namespace worker/Service/RBAC/projections, separate canary workload
  namespace, purpose-specific TLS/token transport, exact same-client facade and
  RBAC/access-review matrices, controller-only preflight network path,
  frozen-cohort claim routing/retention, surviving-API tombstone verification,
  and the pinned API -> new worker cohort -> controller rollout/current-chart
  rollback sequence.
- A measured complete-shadow window and minimum volume for launch, retry,
  ambiguity, and down.
