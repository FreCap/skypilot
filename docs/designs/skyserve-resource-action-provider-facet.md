# SkyServe Resource-Action Provider Facet

Last updated: 2026-08-07

Status: bounded M0 accepted after independent adversarial review; the parent M1
kernel and the M2 schema, cluster-identity, shadow-store, and immutable provider
contract foundations are implemented. As of 2026-08-02, the exact three-object
`ResolvedProviderTargetV1` is the first deployable v1 resolved-target wire; the
unreleased flattened v1 scaffold is rejected. Immutable launch and down
invocations/execution configurations, completed and partial down bases, the
closed API006 progress/lineage/reduction contract, and strict authoritative-
handler return codecs are implemented and locally verified. The shadow-handler
strict codecs required before P3 dispatch are not implemented. The five pinned
renderer artifacts, closed renderer input/seed, exact three-object body and atomic
capsule-validator cutover, effect-free staged renderer, and pure request and
admitted-object normalizers are also implemented and locally verified. Live
target observation, manager/runtime admission and session, request-handler
dispatch, provider observation/effect capture, and live provider authority are
not implemented. The P2a preflight-only transport, exact release/static
manifest contracts, two-Pod self-attestation/bootstrap, fail-closed stale-row
retirement, API tombstone verifier, and disabled-by-default Helm topology are
implemented and locally unit/PostgreSQL/Helm verified. The merged-image dark
rollout and cohort qualification are still pending. The exact frozen V1
representative launch spec is 60,851 bytes, above the separate 60,000-byte
activation budget but below the 65,536-byte parser ceiling; it cannot qualify
the required V2 envelope. The additive compact V2 capsule/config/invocation/
plan graph, V2-only full-spec parser, typed locked-row cohort resolver, and
exact structural full-spec goldens are now implemented and locally verified without
changing V1. The additive V2 preflight wire/parser, disjoint `/v2/` transport,
exact realistic and candidate-maximal structural envelope goldens, and frozen-V1
isolation are also implemented and locally verified. The production
authority-worker runtime now supplies the mutation-free V2 trust evaluator
over an isolated size-one PostgreSQL pool. Its revision-one `PREPARING` store,
complete locked trust join, initial-root/rotated-active policy distinction,
zero-queue single flight, hard five-second transport/publication deadline, and
typed dark response are locally unit and real-PostgreSQL verified. Missing,
crossed, expired, saturated, corrupt, or late evidence remains fixed typed 503.
Manager admission has no production preparation caller yet, so the valid
branch remains unreachable outside tests and authority remains disabled.
The native V2 launch/down seed and input codecs, launch constructor, sole
cleanup-target rederiver/shared binding validator, down constructor, and exact
V2 binding-schema artifact are implemented and locally focused-tested. The
down construction root consumes a typed cleanup-rederivation input and invokes
the sole rederiver itself; a caller cannot inject a cleanup target. The
authoritative direct-no-effect builder and expanded authoritative handler /
direct/fallback outcome parser, raw-invalid journal profile/classifier
integration, shared post-materialization projection, and V2 config-access/six-
role artifact/callable inventories are implemented and locally focused-tested.
The finite representability inventory is only partially expanded: its accepted
index-plus-two-shard form exists, but only three of seven boundary families are
implemented and the final fixture/result/golden evidence does not yet exist.
The rejected monolithic draft measured 75,247 bytes and exceeded its
65,536-byte file contract. The accepted replacement keeps the top six-role
inventory reference stable while using a small index, two content-addressed
case shards, and one result file per fixture. The current 366 cases are
provisional and only three of seven boundary families are implemented. The
rebased repository baseline is exact API008 and Serve037: API008 owns
generation-bound request execution-quiescence and API-instance backend /
capability attestations, Serve035 owns multi-pool reserved fill, Serve036 owns
versioned controller-configuration snapshots, and Serve037 owns the placement-
normalization/retirement ledger tables, service receipts, and version-retirement
columns. None is the M4 authority schema. The additive Serve038
membership/authority migration and Serve039 lineage/terminal-selector migration
are implemented and locally schema-tested. The exact `008/039/028` V2
policy/candidate/proof codecs are implemented and locally focused-tested; the
claim-start barrier, connection-borrowing seams, and historical settled-replay
validator remain design-only. The frozen
V1 renderer and existing full-spec/preflight-envelope goldens cannot be used as
substitute V2 qualification evidence.

The rebased forward-only migration lineage is exact and nonoverlapping:
Serve038 membership/authority has `down_revision='037'`; Serve039 execution
history/terminal selectors has `down_revision='038'`; and the stacked M5a
closure Serve040 has `down_revision='039'`. The existing Serve035, Serve036,
and Serve037 revisions retain their current reserved-fill, controller-
configuration, and placement-normalization/retirement meanings; no M4 migration
reuses any of those numbers.
The frozen Serve034 ledger retirement bridge has only historical
API007/Serve034 verification; its rebased cleanup-only chart ceiling and test
matrix at exact API008/Serve037/state028 remain open before Serve038 may run.

The current dark intermediate installs a V2-only `PREPARING` reference transaction
and stateful evaluator fence. It returns only the action-kind-matched typed
`not_representable: preflight_unavailable_or_invalid` response, and only after
locking and revalidating the exact `ACCEPTING` cohort, absence of a nonterminal
handoff, both accepted fresh registration leases, the same revision-one
`PREPARING` reference, and the current unready `authority-worker` API-instance
lease. Every absent, expired, crossed, or corrupt input remains fixed 503. The
launch branch requires the exact typed
`ProviderLaunchIdentityCanonicalizationContextV1` carried at
`seed.source.identity_canonicalization.context`. While holding the service and
reference locks, it compares the context's service name, complete resource
identity, decision/cohort/action identity, controller-owner fence, lifecycle
epoch, preparation revision/state, and capability commitment with the locked
rows. The down branch requires explicit absence of that launch context and
keeps its capability commitment reference-owned; a context on down or no
context on launch rejects. No fallback derives launch context from ambient or
unlocked state. The intermediate creates no capsule/spec, changes no reference,
advertises no readiness, starts no claimant/executor, and performs no provider
I/O; it is a trust-fence milestone, not P2b completion or action authority.
The hard deadline covers transport and result publication. One isolated,
mutation-free trust read may finish late if DBAPI/network cancellation is
impossible; it has one nonblocking slot, no queue, and a request-local result
that is permanently discarded after timeout. The server object is one-shot:
`stop()` is terminal and recovery creates a fresh object/process, so a late
daemon slot cannot cross transport generations. Short pool/connect/statement/
lock/idle-transaction/TCP budgets contain responsive failures. The cumulative
four-second monotonic guard forbids a new statement after its boundary; it is
not a hard transaction kill, and a just-admitted statement retains its own
3.5-second server timeout. The currently implemented pre-pool dark P2a process
explicitly budgets a persistent ceiling of three synchronous physical
connections: shared central state for
authority bootstrap, `api-requests-control` for the API-instance heartbeat,
and isolated preflight. Transient startup/migration advisory-lock `NullPool`
sessions close when their lock ends and sit outside that persistent ceiling.
The operational formula is `3 * authority Pod count`, or `6 * concurrently
rendered two-member cohort count`; one live-plus-candidate rotation renders
two pre-pool cohorts and therefore reserves 12 persistent synchronous
connections. Once M4 enables its fixed `N`-child execution pool, the manifest-
bound ceiling supersedes this pre-pool figure and is `3 + 2*N` per Pod.
There is no Helm control for PostgreSQL `max_connections`, so authority cannot
activate until the external database proves that persistent capacity and
separate headroom for transient advisory-lock sessions.
Activation evidence rejects API006 and API007 as current authority heads:
API005 is limited to legacy-controller shadow, while exact API008 gates
private-handler dispatch readiness and `shadow -> authoritative`. Its named
backend/capability columns must prove PostgreSQL request storage and queueing
plus execution-quiescence support; default `unknown`/false evidence fails
closed. This does not yet implement any of the parent design's three
server-owned API008 proof builders or their transition/dispatch writes and does not claim M4 or
provider-authoritative rollout. The candidate provider-plan/renderer V1
artifacts are packaged and present in a dark ordinary-role deployment, but
have not been accepted into or exercised by an executor cohort; the eventual
canary-namespace, persisted 201/409, scheduler, and runtime gates remain
incomplete. The parent design's Serve038 candidate-epoch/policy binding codecs,
immutable authority-policy epoch codecs, and Serve039 historical authority/V2
head contracts and migrations now exist with focused local evidence. Their
production stores and complete runtime writers, the durable pre-injection
crash-canary intent/result store, and exact attempt-exhaustion event projection
remain M4 gates and are not yet wired into the current action foundation.
The already-shipped closed `ServeReplicaActionSpecV1` wrapper remains frozen
for pre-Serve038 history and exact-034 cleanup-only tooling. The corrected M4
contract below uses `ServeReplicaActionSpecV2`, including the parent design's
complete service-version identity/hash and closed shadow-candidate or
authoritative-policy binding. That V2 envelope and its V2-only live parser are
implemented structural contracts with local evidence, not live provider-
authority evidence; they cannot activate until the V2 preflight and every
locked-row/runtime gate below are complete.
The M4 Serve-side capacity prerequisite is
`ordinary_ondemand_physical_width1_v1`: paid-capacity, reserved-fill,
cost-rebalance, spot/fallback, and logical-replica modes remain legacy and do
not enter private shadow or authority under this profile.

P2a shipped normally in PR #1232 at merge commit
`4c91d3345ccb5f19538c9f8376c5e7403f5644cc`, with runtime source
`3e6d2c92c7995bf41d25cfcc31e58107860aabfe` and synchronized contract source
`4232b9aac166d1202dd036eba8e752ab6f234640`. P2a is the forward-only
preflight foundation, not a rollback target. Its closed wire envelopes,
private HTTPS transport, two-Pod
self-attestation, complete static-manifest projection, and retirement fences
deliberately start no request executor, claim no queue row, accept no manager
admission, construct no workload/action-provider client, and perform no
Kubernetes mutation or provider effect. Its dedicated observer only GETs its
own Pod, owner ReplicaSet, exact Deployment, and ServiceAccount. Its initial
evaluator may return only the typed
`not_representable: preflight_unavailable_or_invalid` result after the cohort
is accepted. The next bounded implementation tranche is one complete M4
feature PR over this foundation: qualify P2a, then complete P2b live target
observation, P3 private dispatch, and P4 selected Serve authority. It does not
recreate or bypass preflight and does not downgrade Serve034.

Source commit `a836825ef9c219563bb2abc740707c825c26edc5` and immutable
image digest
`sha256:c5f1306f91c7fe2db151c34131ca4cd39be9beba3d21d170f5757996338f375e`
completed a dark `boltz-test` API -> ordinary executor -> controller rollout,
compatible-image rollback with retained additive heads, and staged re-upgrade.
This establishes additive-schema and mixed-image deployment compatibility only.
Authority-worker resources remained disabled and absent; no provider session,
private-handler dispatch, shadow sample, action row, provider I/O, or M4
authority was exercised.

The later API006-rejection/API007-readiness correction and frozen
renderer-contract merge
commit `4f024b60f2fc71852fa8fb9747390f4d3917b03f` was deployed dark as immutable
digest
`sha256:06c9e71c5744ea970c41402fb9c4934e6722a7b53271f6715231b4b275525d25`.
Helm revisions 71--73 converged all six ordinary-role Pods to that digest with
zero current restarts while authority remained disabled and every action and
Serve graph table had zero rows at the final checkpoint. This is deployability
evidence for the contract correction, not renderer or provider-runtime
evidence.

The pure-renderer merge commit
`0e894c2a5d7186d15b10d62bbfdb8283201e4e63` was built from a clean detached
checkout, published as immutable tag `resource-actions-0e894c2a5` and digest
`sha256:b21f0e7cc39f62a21bc5887406f941d0b298d8fc277f0b5abb8b1f170c88b198`,
and deployed dark in Helm revisions 74--76. At the 20:30:53 UTC stable
checkpoint, all six ordinary-role Pods were ready at that exact image ID with
zero restarts and reported the exact merge commit. Both API Pods also reported
the packaged 23,710-byte config-access inventory at SHA256
`19901e8e0491a4e9f957f7ff2a1244fc1baff132c37015c9e8e726af2d538f13`.
Authority remained disabled and absent, every action and Serve graph count was
zero, and the four schema heads were unchanged. This proves dark packaging and
ordinary-role deployability only; no renderer entrypoint, executor cohort,
provider I/O, shadow comparison, or authority path was exercised.

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
- Ensure each legacy-controller shadow candidate executes exactly one legacy
  mutation, while each selected Serve039 represented-private candidate executes
  exactly one attested private-handler mutation; a candidate never runs both.

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
  submit(spec: ServeReplicaActionSpecV2,
         durable_progress: null | ProviderLifecycleProgressV1,
         request_execution_fence)
      -> ProviderSubmissionV1
  observe(spec: ServeReplicaActionSpecV2,
          optional_operation_id,
          durable_progress: null | ProviderLifecycleProgressV1,
          request_execution_fence)
      -> ProviderLifecycleObservationV1
```

`normalize_plan()` is pure and bounded over explicitly supplied read-only
preflight values; it never opens a client or reads ambient config. In the
legacy-controller shadow phase, the existing high-level launch/down path
remains the only mutation. In Serve039 private shadow, a represented V2 private
handler instead invokes this facet as the sole provider-effect owner and never
also enters the high-level legacy path.
`submit()` and `observe()` call the parent contract's sole live parser,
`serve_replica_action_spec_from_value_v2()`, and typed-read the complete
immutable action spec; V1 or a normalized plan alone is never execution
authority. The action kind selects
the corresponding launch or down invocation and execution configuration from
that spec. Before constructing a cloud or Kubernetes client, the facet verifies
the embedded service-version identity hash; the plan/invocation hashes; the
shadow-candidate or UUID authority-policy binding against the exact consumed
dispatch context; the frozen executor cohort; the current request-execution
fence; and every kind-specific scope and principal field. A caller cannot
supply replacement identity, binding, or execution configuration beside the
spec.
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
gets executor-driven retry/requeue. Handler completion, same-owner quiescent
acknowledgement, and typed process/Pod-stale or cold-recovery fences all obey
`ReplayPolicy.NEVER`: they terminalize the old request once. Generic lease
expiry alone never terminalizes a claimed private request. Only the Serve reducer may
then admit attempt `n+1`, whose new deterministic request has one
generation-one claim and carries the settled API006 cursor without repeating a
committed effect.

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
  prior_launch_basis_sha256: null | Sha256,
  prior_cleanup_target_sha256: null | Sha256,
  request_payload_sha256: Sha256,
  redaction_profile: "provider_lifecycle_redaction_v1"
}
```

The plan is the indexed identity projection, not a second copy of the provider
execution document. For launch, both prior hashes are null. For down, the full
typed prior basis appears exactly once in `invocation.down` and the full cleanup
target appears exactly once in that invocation's execution capsule; the basis
retains that cleanup target's hash and the two plan hashes equal the complete
in-spec basis and capsule preimages. A hash-only provider-private interpretation
is not authoritative. Admission externally validates each basis cleanup-target
hash against its retained source and the sole capsule copy. The partial basis's
cursor and quiescence preimages likewise already exist in locked, typed API006
action/attempt rows and are not provider execution inputs. No retained-source
hash authorizes a handler lookup or provider call. The graph contains no secret
bytes.

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
  launch_provider_cursor_sha256: Sha256,
  launch_provider_progress_revision: PositiveInteger,
  launch_quiescence_sha256: Sha256,
  launch_cleanup_target_sha256: Sha256,
  launch_immutable_spec_sha256: Sha256,
  exact_resources_override: true
}

ServeShadowPartialLaunchCleanupBasisV1 = {
  version: 1,
  basis_kind: "shadow_partial_launch_cleanup",
  source_store: "serve_resource_action_shadow_execution_history",
  launch_decision_id: UUID,
  launch_request_sequence: PositiveInteger,
  launch_logical_attempt: PositiveInteger,
  launch_resource_identity: ProviderLifecyclePlanV2.resource_identity,
  launch_requested_target: ProviderLocatorV1,
  launch_resources: ProviderPodResourceSnapshotV1,
  launch_workspace_identity: ProviderWorkspaceIdentityV1,
  launch_provider_cursor_sha256: Sha256,
  launch_provider_progress_revision: PositiveInteger,
  launch_quiescence_sha256: Sha256,
  launch_outcome_sha256: Sha256,
  launch_terminal_history_sha256: Sha256,
  launch_execution_authority_lineage_sha256: Sha256,
  launch_cleanup_target_sha256: Sha256,
  launch_immutable_spec_sha256: Sha256,
  exact_resources_override: true
}

PriorLaunchBasisV2 = CompletedLaunchBasisV1 |
                     PartialLaunchCleanupBasisV1 |
                     ServeShadowPartialLaunchCleanupBasisV1
```

`PartialLaunchCleanupBasisV1` is a retained-source reference, not a truncated
proof. Its full `launch_provider_cursor` and `launch_quiescence` are deliberately
not copied into the down spec. They remain in, respectively, the exact settled
source attempt's API006 progress envelope and the exact reducer-owned source
outcome named by `(source_store, launch_action_id, launch_attempt)`. The basis
retains their canonical hashes and the exact progress revision, immutable-spec
hash, resource/target/workspace preimages, and derived cleanup-target hash. The
complete cleanup target appears only in the down execution capsule. V1 retains
all API resource actions and attempts indefinitely because it has no generic
action/attempt garbage collection. A future GC must first add a typed persisted
reverse-reference relation and migration before either source row may be
removed; scanning hashes or JSON during deletion is insufficient. Request GC
remains safe only after the final progress and outcome preimages have been
snapshotted into the settled attempt.

Partial-down admission performs the external validation that the standalone
value parser cannot. It first constructs an optimistic candidate, then acquires
the sorted union of source-launch and deterministic-down action IDs; only after
all action keys are acquired does it lock the named source attempt. In the
first-settlement branch the unsettled locked attempt protects its request from
GC; admission nonlocking-reads and validates the terminal request, complete
typed API006 cursor, and terminal handler DTO, then constructs and persists
reducer-owned quiescence, terminalizes the source as exact
`SUPERSEDED_TO_DOWN`, revalidates the down row already inserted or exact-adopted
during sorted-union acquisition, and links it in one commit. Its lost-ack branch
requires the already-settled attempt's retained request snapshot and source
outcome/quiescence byte-for-byte
and revalidates the already-acquired down before exact-adopting the same link;
the original request may be GCed, while a
surviving row is only compared nonlocking. Both
branches re-derive the cleanup target, require it byte-equal to the sole capsule
copy, and verify the retained cursor hash, progress revision, quiescence hash,
cleanup-target hash, immutable-spec hash, and every local basis projection.
Concretely they require `source_action.current_attempt ==
basis.launch_attempt`; deterministic source action and request IDs; a `SETTLED`
source attempt; valid API006 progress JSON/hash/revision and nested cursor hash;
source attempt outcome/hash byte-equal to source action `last_result`/hash; exact
Q disposition and quiescence action/attempt/request/cursor bindings; and exact
source spec identity/resource/target/workspace projections. The cleanup target
is derived only from that locked spec/cursor and exact same-UUID cluster-row
disposition. Any absent preimage, hash/revision mismatch, wrong attempt, changed
cleanup projection, or retention violation rolls back the whole transaction.
The source may never be terminalized in one transaction and handed to a later
down-admission transaction.
The down handler needs none of the omitted provenance to mutate: it executes
only the full cleanup target and current authority material frozen in its own
capsule.

The additive shadow partial basis is equally a retained-source reference, not a
hash-only proof and not an edit to byte-frozen `PriorLaunchBasisV1`. Its source
preimages are the settled primary-launch child, its
`serve_resource_action_shadow_execution_history` row, exact class-17 terminal
history, strict handler return, `ProviderShadowLifecycleProgressV1`, and
reducer-owned `ProviderShadowLaunchSupersessionQuiescenceV1`. The source remains
retained until the linked primary down settles. The source history stores the
normalized successor decision/sequence and basis hash under an indexed exact-Q
shape; the target's partial-down basis is the reciprocal typed preimage. Before
any GC lock, indexed outgoing and reverse-incoming discovery closes exactly one
source/target pair and constructs its per-class sorted key union; a missing
reciprocal edge, branch, chain, cycle, or crossed basis blocks. GC may release
and delete the pair only when both sides are independently eligible and neither
side has a surviving request, replica/cleanup link, protected window,
reference, or other root. It releases both references and deletes both complete
graphs atomically or neither, never leaves one pointer/basis dangling, and never
scans JSON or expands from one locked parent to another. A strict shadow launch result
may use `supersede_to_down` only when its current intent has the exact origin-
bound `ProviderShadowLaunchNoEffectResolutionV1`, or its phase is the exact
nonintent E-only row; fallback never creates this basis. The reducer first
constructs the deterministic target and full cleanup preflight without
authority. Its one transaction then walks the canonical sorted source+target
union at every applicable class--service/replica, cohorts/handoffs/leases/
references, coverage, parents, and children/histories--before revalidating the
source and target. Only then does it settle the source as `Q` and create/exact-adopt one normal represented
`PRIMARY_DOWN` decision, parent/child/history `BOUND` row, deterministic private
request/queue, reference, and replica link; request/queue classes follow the
complete class-10 union. A crash before the commit leaves the source unsettled and
retries; a lost acknowledgement adopts the entire byte-equal graph. No
`LAUNCH_CLEANUP_DOWN`, API006 action/attempt, hidden provider call, or unlinked
settlement is legal. Missing preflight, uncertain current intent, or any hash/
UID/lineage mismatch leaves the source blocked and retained rather than
stranding unowned cleanup.

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

A launch has `prior_launch_basis_sha256=null` and
`prior_cleanup_target_sha256=null`. Its
resource hash is the canonical
SHA-256 of `resources`; its placement hash is the canonical SHA-256 of
`{version: 1, launch_resource_identity: resource_identity,
launch_requested_target: requested_target, launch_resources: resources,
exact_resources_override: true}`; and its workspace hash is the canonical
SHA-256 of `ProviderWorkspaceIdentityV1`.

A frozen V1 primary down requires `PriorLaunchBasisV1`; a live V2 primary down
requires the additive `PriorLaunchBasisV2`. Both carry the full typed basis in
their invocation and a full cleanup target in the execution capsule. The plan retains only their
canonical hashes, and the basis retains only the cleanup-target hash. Admission
re-derives the target from the locked retained launch evidence, requires that
target byte-equal to the capsule copy, and requires both retained hashes and
both plan hashes to equal their complete validated preimages. The kind-specific
source ID is deterministic from its launch identity/spec: action/completed
bases use `launch_action_id`, while the shadow-partial basis uses
`launch_decision_id` plus its exact sequence/logical attempt. Admission loads
and locks that exact retained launch row and applicable attempt/history evidence
from `source_store`, plus the exact
global-user-state cluster row disposition named by the cleanup target. It
recomputes every added hash from the full typed preimages and rejects
caller-supplied bytes that are not equal to retained state.

For `completed_launch`, the retained launch is terminal-successful. An API
launch's final API006 cursor supplies the resolved target and handle; a shadow
launch's completed child supplies the exact resolved-target observation and
the same-UUID cluster row supplies the handle. For a shadow source, admission
locks that global-user-state cluster row and complete handle in the parent
design's lock class 2 with service/version/replica identity, before cohort,
reference, coverage, parent, action, or attempt locks. Both must agree on
cluster UUID, all three object UIDs, Pod UID, provider scope, and the complete
re-derived cleanup target. The cluster row's provider block is byte-equal to
`launch_handle`; the re-derived cleanup target hash equals the basis commitment
and its bytes equal the sole capsule copy.

For every API-source down, candidate construction nonlockingly discovers the
bounded pre-existing action conflicts and rejects a natural key already bound
to a divergent UUID. The parent design's class 11 then walks the canonical
UUID-sorted union of all source action IDs and the deterministic new down ID.
At each position it locks and validates an existing source or inserts/exact-
adopts the allowed new down; it never locks a higher UUID and later inserts a
lower one. Only after the whole action union is acquired may it lock
predecessor/current attempt rows in class 12. Completed, partial-launch, and
lost-ack source paths share this acquisition primitive; none may insert the
down before or after a separately ordered source lock.

Nonlocking discovery is only an ordering input, not a conflict decision. If a
natural-key conflict commits after discovery and wins the unique-index race,
the transaction inspects that conflict at the deterministic new action's sorted
position. It may adopt only the same deterministic UUID with byte-exact action
content; a different UUID or any content drift aborts the transaction before
attempt locks or links.

`partial_launch_cleanup` is allowed only for an API-action launch that did not
succeed and satisfies the action-wide `launch_io_started` predicate above. The
first-settlement branch starts from a terminal request and unsettled source;
the replay branch starts from the exact settled source outcome containing
`ProviderLaunchSupersessionQuiescenceV1`. Admission first holds the exact
service/replica-incarnation fence and constructs the complete candidate down
identity/spec from a read-only source snapshot. It then visits
the sorted union of source-launch and deterministic down action IDs in canonical
action-ID order. At each key it locks and validates the existing row, or
inserts/exactly adopts the allowed new down row at that key. An insert or
conflict adoption is an action-row-class acquisition at its sorted position; the
transaction never locks a higher action ID and later inserts a lower one. After
all action keys are acquired it locks the source attempt. The first-settlement
branch nonlocking-reads the still-retained terminal request, then constructs/
persists quiescence and source terminal state in the same transaction that
revalidates the already-inserted/exact-adopted down and links it. The lost-ack
branch revalidates the already-
settled byte-equal request snapshot/outcome/quiescence without requiring the
original request row, revalidates the already-acquired down, and exact-adopts
that same link. Both revalidate every retained source
cursor/quiescence preimage and every cleanup-target and immutable-spec byte used
to construct the candidate. Any mismatch rolls back the entire transaction,
including a newly inserted down row. They derive the three-slot cleanup target
from the retained launch object plans, every committed UID/allocation, and an
exact same-UUID cluster-row read. No transaction may settle the source without
also committing the matching down/link.

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

External `FAILED`/`CANCELLED` terminalization or a null/dropped return is
categorically ineligible for quiescence and partial cleanup. It uses the parent's closed request-fallback
table: an exact non-`SUCCEEDED` cursor remains observation-first, an exact
`SUCCEEDED` cursor commits normal provider success, a revision-zero empty
journal retries when the action remains desired, and malformed progress
blocks. External terminalization never supplies `N<i>` or partial handoff. The
strict private codec converts a malformed returned DTO to fixed `FAILED` before
persistence; a persisted invalid/mismatched nonnull terminal-`SUCCEEDED` value
is quarantined corruption, not fallback. The
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

`ProviderKubernetesServerOriginV1` is constructed only from the exact selected
kubeconfig cluster's `server` scalar by the following closed normalizer. The
input is nonempty ASCII UTF-8 text and must begin with literal lowercase
`https://`. It is parsed as one hierarchical absolute URI; userinfo, an empty
host, query, fragment, backslash, control/space, non-ASCII text, and every `%`
byte are rejected. Thus no percent decoding, Unicode/IDNA conversion, IPv6 zone
identifier, environment expansion, or proxy/base-URL substitution occurs.

The raw authority is either one bracketed IPv6 literal or an unbracketed DNS/
IPv4 host, followed by at most one port. Brackets are mandatory and stripped
only for IPv6; brackets on DNS/IPv4 and an unbracketed colon reject. IPv4 must
be four canonical decimal octets with no leading zero. IPv6 is parsed as 128
bits and rendered as lowercase RFC 5952 compressed text; IPv4-embedded IPv6 is
rendered by the same rule. A DNS host is lowercased and must then be 1..253 ASCII
bytes of RFC 1123 labels (1..63 each), with no empty label, underscore, leading/
trailing hyphen, trailing dot, or `xn--` A-label. Uppercase ASCII DNS is the sole
host spelling normalized rather than rejected. An absent port becomes integer
443; an explicit port is canonical `[1-9][0-9]{0,4}` in 1..65535, so explicit
443 and absence have one identity while leading-zero or signed forms reject.

The source path is either empty, `/`, or an absolute sequence of nonempty ASCII
RFC 3986 `pchar` segments. Empty and `/` both canonicalize to `path=""`.
Otherwise repeated slashes, trailing slash, dot/dot-dot segments, percent
escapes, and bytes outside unreserved or `!$&'()*+,;=:@` reject; a legal path is
retained byte-for-byte. `tls_server_name` is null only when absent. When present
it is nonempty ASCII, has no trailing dot or `xn--` label, and is normalized by
the same lowercase RFC 1123 DNS rule; IP literals and ports are forbidden.
Every loader (preflight, runtime, qualification, and drift recheck) calls this
one pure function and byte-compares the resulting `{scheme,host,port,path}` and
TLS name. Tests freeze absent/explicit 443, nondefault ports, root/prefixed paths,
DNS case, canonical and alternate IPv6 spellings, and every rejection above on
the minimum supported Python/Kubernetes client versions.

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
bounded together with the complete live `ServeReplicaActionSpecV2` to 65,536
canonical UTF-8 bytes. The fixed renderer may contain only its reviewed
nonsecret runtime env
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
canonical object whose complete role-specific key set and values are closed
below; the three required identity pairs are a subset, not permission for extra
labels.

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
the exact body contract is the literal
`KubernetesServeThreeObjectBodySchemaV1` below. Its schema ID is
`skypilot.kubernetes.serve-three-object-body.v1`, version is integer 1,
API version is `v1`, and its exact role order is `head_ssh_service`,
`head_service`, `head_pod`. All listed keys are required, no unlisted key is
accepted, and every value has the displayed JSON type and literal. A field
described as absent is absent, not JSON null, an empty collection, or an empty
string. `request_body` is the role-valid request form in the table;
`requested_semantic` is the same body after the allocation projection defined
below. An admitted body is valid only when that same projection returns
byte-equal semantic bytes plus one role-valid allocation tuple.

| Role | Exact request-body bindings |
|---|---|
| `head_pod` | `/spec/nodeName` is absent. The exact spec keys are `automountServiceAccountToken`, `containers`, `dnsPolicy`, `enableServiceLinks`, `preemptionPolicy`, `priority`, `restartPolicy`, `schedulerName`, `securityContext`, `serviceAccount`, `serviceAccountName`, `terminationGracePeriodSeconds`, and `tolerations`. Both service-account fields equal the frozen workload ServiceAccount; automount is false. The literal defaults are `dnsPolicy="ClusterFirst"`, `enableServiceLinks=true`, `preemptionPolicy="PreemptLowerPriority"`, `priority=0`, `restartPolicy="Always"`, `schedulerName="default-scheduler"`, `securityContext={}`, and `terminationGracePeriodSeconds=30`. Tolerations are exactly, in order, the `node.kubernetes.io/not-ready` and `node.kubernetes.io/unreachable` `Exists`/`NoExecute` entries with integer `tolerationSeconds=300`. |
| `head_pod` container | `/spec/containers` has exactly one entry. Its exact keys are `env`, `image`, `imagePullPolicy`, `name`, `ports`, `resources`, `terminationMessagePath`, and `terminationMessagePolicy`; name is `ray-node`, image is the digest-qualified workload image, pull policy is `Always`, termination path is `/dev/termination-log`, and termination policy is `File`. Resources contain exactly the frozen CPU and memory requests/limits with no accelerator or ephemeral-storage entry. The five ports are exactly `10001`, `10002`, `10003`, `10004`, and `46590` in that order, and each entry explicitly contains `protocol="TCP"`. |
| both Services | The shared exact spec keys are `type="ClusterIP"`, `sessionAffinity="None"`, `internalTrafficPolicy="Cluster"`, and `selector`. The nonempty selector is byte-derived from the head-Pod topology labels and contains exactly `component`, `skypilot-cluster-name`, `skypilot.co/cluster-record-uuid`, and `skypilot.co/serve-replica-incarnation`; its complete map is byte-equal on both Services, and every entry equals the head Pod's corresponding label. |
| `head_ssh_service` | In addition to the shared Service keys, `/spec/ports` is exactly the singleton `[{"protocol":"TCP","port":22,"targetPort":22}]`. Requested `clusterIP`, `clusterIPs`, `ipFamilies`, and `ipFamilyPolicy` are all absent so the API server allocates the quartet. |
| `head_service` | In addition to the shared Service keys, `/spec/clusterIP="None"`. Requested `clusterIPs`, `ipFamilies`, and `ipFamilyPolicy` are absent. `/spec/ports` is absent, not null or an empty list. |

Every role has top-level keys exactly `apiVersion`, `kind`, `metadata`, and
`spec`. Service metadata has exactly `labels`, `name`, and `namespace`; its
`annotations` key is absent. Pod metadata has exactly `annotations`, `labels`,
`name`, and `namespace`. Let `B` be the renderer input's exact
`ProviderWorkloadNameBasisV1`, copied byte-for-byte from
`requested_target.kubernetes.name_basis`. The staged-input validator reconstructs
an expected basis with `display_name=sky_cluster_name`,
`frozen_user_hash=seed.request_identity.frozen_user_hash`, `max_length=42`, and
`cluster_name_hash_length=8`, and requires its canonical bytes to equal `B`.
The independently copied renderer-input `sky_cluster_name` must also be
byte-equal to `B.display_name`. Let `C = B.provider_cluster_name` and
`W = B.workload_name`, using the existing
`ProviderWorkloadNameBasisV1` properties rather than a second cleaner or naming
algorithm. Thus `W = C + "-head"`. Let `U` be the frozen cleaned user, `O` the
frozen original user, `R` the lowercase canonical text of the renderer input's
`sky_cluster_record_uuid`, `I` the lowercase canonical text of
`resource_identity.replica_incarnation`, and `P = W`. The SSH Service name is
exactly `W + "-ssh"`; the head Service and Pod names are exactly `W`. All three
topology name fields, all three `skypilot-cluster-name` labels, both selector
copies, all three cluster-record labels, and all three replica-incarnation
labels must be byte-equal to these derived values before rendering. The
complete metadata maps are:

```text
head_ssh_service labels = {
  "service-role": "head_ssh_service",
  "skypilot-cluster-name": C,
  "skypilot-user": U,
  "skypilot.co/cluster-record-uuid": R,
  "skypilot.co/serve-replica-incarnation": I
}
head_service labels = {
  "service-role": "head_service",
  "skypilot-cluster-name": C,
  "skypilot-user": U,
  "skypilot.co/cluster-record-uuid": R,
  "skypilot.co/serve-replica-incarnation": I
}
head_pod labels = {
  "component": P,
  "skypilot-cluster-name": C,
  "skypilot-user": U,
  "skypilot.co/cluster-record-uuid": R,
  "skypilot.co/serve-replica-incarnation": I
}
head_pod annotations = {"skypilot-user": O}
```

Those are exact five-key label maps. No `app`, legacy Ray, Helm, admission,
queue, scheduling, or provider label is implicit. The two Service specs have no
container or environment field. The sole Pod container's `env` array has
exactly one entry, in that position, with exact key set `name`, `value` and
value `{"name":"SKYPILOT_SERVE_REPLICA_ID","value":DecimalIntegerText}`.
`valueFrom` is absent. Each container-port entry has exactly `containerPort`
and `protocol`; the SSH Service port entry has exactly `port`, `protocol`, and
`targetPort`. `resources` has exactly `limits` and `requests`, and each of those
has exactly `cpu` and `memory`. These closed nested key sets apply before any
hash is computed.

The selector is a semantic requirement, not cosmetic metadata. The Pod's
role-specific `component` label names the exact head Pod; the three identity
labels prevent a Service from selecting an older same-display-name
incarnation. `skypilot-user` is intentionally not a selector key. A missing,
null, or empty selector rejects even though CoreV1 accepts those shapes.

The only empty collection in the request specs is the required Pod
`securityContext={}` literal. In particular, head-Service `ports` and the
unrequested allocation fields are absent. The SSH Service's allocation fields
are absent rather than null or empty. The Pod's scheduler `nodeName` is absent.
Canonical construction happens directly as JSON; a YAML decoder's
null/empty/omitted equivalences are not part of the contract.

The `ray-node` env list is inspected by name, never by a numeric index. It has
exactly one `SKYPILOT_SERVE_REPLICA_ID` entry whose scalar `value` is the
canonical `DecimalIntegerText` from the launch invocation and job spec;
`valueFrom` is absent. Missing, duplicate, wrong-value, or `valueFrom` forms
reject; any other env entry also rejects.

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

`kubernetes_admitted_object_v1` has two distinct entrypoints; a common function
that first demands the admitted allocation shape is invalid for request-side
construction.

The `admitted_object_normalization` member of
`ResolvedProviderKubernetesRendererArtifactSetV1` has the closed type
`ResolvedProviderKubernetesNormalizationArtifactV1 =
{artifact_ref:ProviderRepoArtifactRefV1,
contract:KubernetesAdmittedObjectNormalizationV1}`. `contract` is the parsed,
schema-validated canonical document defined below; `artifact_ref` is the exact
pinned reference whose bytes produced it. Neither field may be reconstructed
or obtained from a global, closure, path argument, or ambient read.

The exact request-normalizer signature is
`normalize_kubernetes_request_object_v1(role, validated_request_body,
normalization_artifact)`. `normalization_artifact` must be that exact typed
`ResolvedProviderKubernetesNormalizationArtifactV1` member, and the entrypoint
uses its `contract`; it accepts no omitted, raw-dict, reference-only, or
wrong-role artifact argument. `validated_request_body` must be an exact
`ValidatedKubernetesServeThreeObjectBodyV1` transient produced by
`validate_kubernetes_serve_three_object_body_v1`; that preceding validator, not
the request normalizer, enforces `KubernetesServeThreeObjectBodySchemaV1`. The
request normalizer returns `{requested_semantic,
requested_allocation_intent}` and accepts only these role-exact request
allocation shapes:

| Role | Required present allocation fields | Required absent allocation fields | Intent | Semantic removal |
|---|---|---|---|---|
| `head_ssh_service` | none | all four Service allocation pointers | `allocate_single_stack_cluster_ip` | none |
| `head_service` | `clusterIP="None"` only | `clusterIPs`, `ipFamilies`, `ipFamilyPolicy` | `headless_single_stack` | `clusterIP` |
| `head_pod` | none | `nodeName` | `schedule_one_node` | none |

It does not fabricate an absent field, require a complete Service quartet, or
return server allocations. `requested_allocation_intent` is a literal scalar
from the table, not an allocation value. Request normalization removes only the
one displayed present head-Service intent field; every other request byte is
retained.

The exact admitted-normalizer signature is
`normalize_kubernetes_admitted_object_v1(role, admitted_object,
normalization_artifact, *, require_pod_node_name)`. Its
`normalization_artifact` requirement is identical to the request normalizer's
and it uses that typed member's `contract`. It returns
`{admitted_semantic, server_allocations}`. Before invoking it, the session
requires `metadata.deletionTimestamp` to be absent or JSON null; a nonnull value
is a conflict and is never hidden by stripping. The request schema requires the
key absent. After that gate, admitted normalization removes only top-level
`status` and metadata `uid`, `resourceVersion`, `generation`,
`creationTimestamp`, `deletionTimestamp`, and `managedFields`. Each Service
must contain all four allocation pointers atomically before any is removed; a
missing or partial quartet conflicts. The Pod may omit `nodeName` only in the
documented unscheduled partial-evidence phase, otherwise it returns exactly one
validated scheduler allocation. The transform removes the complete role-valid
allocation set from the semantic output and returns the serializable ordered
allocation entries defined below. `require_pod_node_name` is required,
keyword-only, and accepted only when `type(value) is bool`; it has no default,
and integers, truthy objects, and every non-built-in Boolean representation
reject before object normalization. `False` permits the Pod allocation array to
contain zero or exactly one `nodeName`; `True` requires exactly one. Both values
leave each Service's exactly-four allocation requirement unchanged. Partial
progress/cleanup reads pass `False`; construction of `ResolvedProviderTargetV1`
and every `OBJECTS_EXACT` read pass `True`. No other default is inserted, removed,
sorted, coerced, or rewritten; array order is retained.

The comparator requires request `requested_semantic` and admitted
`admitted_semantic` canonical bytes to be equal and independently validates the
request intent against the admitted allocation tuple. In particular, the
head-Service request's one-field `None` intent is not parsed as a malformed
partial admitted quartet, and an admitted `None` quartet cannot authorize an
SSH Service request.

The contract identifier `kubernetes_admitted_object_v1` names only this
request/admitted split and projected-semantic behavior. A removed provisional
profile/interpreter reinjected the requested head-Service `clusterIP="None"`
into its semantic result while claiming that same identifier. That transform
was nonconforming, could not bind an approved inventory or produce object-plan
hashes, and must not be restored. For the retained evidence body below, its
nonconforming no-LF hash was
`6f56a60c19a22958840c5caffb8a613246107d085d8c5a7dad13f08034fa6ecb`;
the canonical projected no-LF hash is
`b9f6e3e86df0c26dfe4da1576fe58ba9fd07af0c75c06be920bc5ac65520dd15`.
No authority or shadow-parity evidence may mix those domains.

The renderer therefore sets every retained admission default explicitly. The
profile retains every other label, annotation, owner reference, finalizer,
container, init container, volume, service-account, security, scheduling,
image, port, selector, and spec field. Service allocations are recorded on
first read. Pod `nodeName` may be absent while scheduling and then append
exactly one nonempty value; once recorded it is write-once. A complete
`ResolvedProviderTargetV1` and `OBJECTS_EXACT` require that value, while partial
evidence may retain the Pod UID before assignment. Every later read requires
every recorded allocation to match. An injected sidecar/init container/volume,
image pull secret, label, annotation, owner reference, finalizer, or any
unreviewed path/value is a conflict. Both a 201 response readback and a 409
readback must normalize to the stored requested semantic bytes, with only those
typed allocations separated. Raw request/readback JSON equality is never used.

### Pre-object renderer input and staged capsule construction

Rendering cannot consume the completed capsule because that capsule already
contains the three expected object bodies. The pre-object constructor therefore
uses these two closed DTOs:

```text
ProviderKubernetesExecutionCapsuleSeedV1 = {
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
  post_provision: ProviderKubernetesPostProvisionV1,
  endpoint: ProviderKubernetesEndpointContractV1,
  scheduling: ProviderKubernetesSchedulingContractV1,
  storage: ProviderKubernetesStorageContractV1,
  metadata: ProviderKubernetesMetadataContractV1,
  security: ProviderKubernetesSecurityContractV1,
  topology: ProviderPodTopologyV1,
  mutation_contract: ProviderKubernetesLaunchMutationContractV1
}

ProviderKubernetesRendererInputV1 = {
  version: 1,
  contract: "validated_launch_spec_v1",
  resource_identity: ProviderResourceIdentityV1,
  sky_cluster_name: Text,
  sky_cluster_record_uuid: UUID,
  name_basis: ProviderWorkloadNameBasisV1,
  seed: ProviderKubernetesExecutionCapsuleSeedV1,
  retained_source: ProviderLaunchContentSourceV1
}
```

The seed key set is exactly the completed
`ProviderKubernetesExecutionCapsuleV1` key set minus `objects`; it has no null
placeholder, object hash, object-plan reference, or backpointer. The renderer
input has exactly the eight displayed keys. `sky_cluster_name`, `name_basis`,
and `sky_cluster_record_uuid` are copied byte-for-byte from
`requested_target.sky_cluster_name`,
`requested_target.kubernetes.name_basis`, and
`requested_target.sky_cluster_record_uuid` before the target is omitted from the
policy-free input. Its canonical JSON document is the
sole RFC 6901 pointer root: every binding pointer below starts at this object,
so `/seed/scope/namespace` and `/resource_identity/replica_id` are
unambiguous. `validated_launch_spec_v1` means the resource identity and every
seed child have passed their existing pure leaf validation and all seed-only
cross-field comparisons; it is a literal contract value, not a claimed boolean
or caller-selected label. It additionally requires
`retained_source == seed.renderer.source ==
seed.post_provision.job_submission.run_source` by canonical bytes. It
reconstructs `name_basis` from `sky_cluster_name` and
`seed.request_identity.frozen_user_hash` exactly as specified above, requires
`sky_cluster_name == name_basis.display_name`,
`retained_source.service_incarnation == resource_identity.service_incarnation`,
and requires the resource identity's replica incarnation and the independently
copied cluster-record UUID to equal every corresponding seed topology label.
It also requires the exact `C`/`W` topology names and labels above. A
self-consistent topology cannot supply its own naming or cluster identity.

Construction is nonrecursive and has exactly four stages:

1. Build and fully validate `ProviderKubernetesRendererInputV1` without any
   object plan.
2. Resolve the five pinned artifacts, resolve only the allowed bindings from
   the renderer-input root into the closed 17-entry typed binding set, and emit
   exactly three request bodies. Neither the renderer nor a transform can read
   an `objects` field because none exists.
3. Validate each body with `KubernetesServeThreeObjectBodySchemaV1`, run the
   request-side normalization entrypoint with the resolved typed normalization
   artifact, and construct the three object plans, their complete requested
   semantic preimages, both hashes, and the byte-equal normalization artifact
   references.
4. Copy every seed field unchanged, append those typed object plans, construct
   `ProviderKubernetesExecutionCapsuleV1`, and rerun its complete contextual
   validation. Any mismatch fails construction; it never triggers a rerender
   from the completed capsule.

The stored full capsule is thus an output and later comparison operand, never a
renderer input. The public staged constructor and the ten public leaves named
in the config-access inventory below are the only project entrypoints in this
four-stage sequence. In stage 3 the staged constructor invokes
`body_validate` before `request_normalize`; the latter consumes the typed
validated-body transient and does not invoke the validator a second time. This
staged constructor is required before a renderer artifact can be accepted.

### Native V2 construction and inventory ownership

The V1 seed, input, constructor, and config-access inventory above are frozen
Serve034 history. A live M4 launch does not construct a V1 capsule and replace
its cohort, serialize through a V1 value, or call a V1 staged constructor. Its
closed pre-object values are instead:

```text
ProviderKubernetesExecutionCapsuleSeedV2 = {
  version: 2,
  implementation_contract: "kubernetes_serve_prebooted_runtime_v1",
  executor_cohort: ProviderAuthorityWorkerCohortReferenceV1,
  config_projection: ProviderKubernetesConfigProjectionV1,
  config_projection_sha256: Sha256,
  scope: ProviderKubernetesScopeV1,
  principals: ProviderKubernetesPrincipalsV1,
  prerequisites: ProviderKubernetesPrerequisiteInventoryV1,
  request_identity: ProviderKubernetesRequestIdentityV1,
  resources: ProviderKubernetesResourceContractV1,
  renderer: ProviderKubernetesRendererV1,
  post_provision: ProviderKubernetesPostProvisionV1,
  endpoint: ProviderKubernetesEndpointContractV1,
  scheduling: ProviderKubernetesSchedulingContractV1,
  storage: ProviderKubernetesStorageContractV1,
  metadata: ProviderKubernetesMetadataContractV1,
  security: ProviderKubernetesSecurityContractV1,
  topology: ProviderPodTopologyV1,
  mutation_contract: ProviderKubernetesLaunchMutationContractV1
}

ProviderKubernetesRendererInputV2 = {
  version: 2,
  contract: "validated_launch_spec_v2",
  resource_identity: ProviderResourceIdentityV1,
  sky_cluster_name: Text,
  sky_cluster_record_uuid: UUID,
  name_basis: ProviderWorkloadNameBasisV1,
  seed: ProviderKubernetesExecutionCapsuleSeedV2,
  retained_source: ProviderLaunchContentSourceV1
}

ProviderKubernetesDownExecutionCapsuleInputV2 = {
  version: 2,
  implementation_contract: "kubernetes_serve_exact_cleanup_v1",
  executor_cohort: ProviderAuthorityWorkerCohortReferenceV1,
  config_projection: ProviderKubernetesConfigProjectionV1,
  config_projection_sha256: Sha256,
  scope: ProviderKubernetesScopeV1,
  principals: ProviderKubernetesPrincipalsV1,
  prerequisites: ProviderKubernetesPrerequisiteInventoryV1,
  mutation_contract: ProviderKubernetesDownMutationContractV1
}
```

The V2 seed key set is exactly the displayed completed
`ProviderKubernetesExecutionCapsuleV2` key set minus `objects`; the renderer
input has exactly the eight displayed keys. The down input has exactly the nine
displayed `ProviderKubernetesDownExecutionCapsuleV2` keys minus
`cleanup_target` and `cleanup_target_sha256`; those two fields may come only
from the rederiver below. The compact cohort reference is the
only cohort value in either persisted value. The complete parsed
`ProviderAuthorityWorkerCohortV2` is a separate transient argument to context
validation and construction.
`validate_provider_kubernetes_renderer_input_v2(renderer_input,
resolved_cohort)` and
`validate_provider_kubernetes_execution_capsule_context_v2(capsule,
resolved_cohort)` call
`validate_locked_action_spec_cohort_v2()`, then perform every seed-internal,
cohort/Namespace/principal, request-identity, source, topology, resource,
scheduling, storage, metadata, security, runtime-artifact, normalization, and
object-plan comparison required by the V1 seed and completed-capsule validators,
with the full cohort supplied externally rather than embedded. A structurally
valid V2 seed or capsule that has not passed that contextual validator remains
non-authoritative.
The down peer is named
`validate_provider_kubernetes_down_execution_capsule_context_v2(capsule,
resolved_cohort, rederived_cleanup_target)` and requires its third argument to
be byte-equal to the capsule target.

Only the complete V2 preflight evaluator constructs either input. It derives the
compact reference from the canonical identity of the resolved cohort rather
than accepting a reference from the request. It copies `resource_identity`,
the target's three naming fields, and retained launch source from the validated
`ProviderLaunchPreflightSeedV2`; fills the capsule seed only from the
kind-specific live preflight results; and requires the request's config
projection, request identity, source, resources, topology, target Namespace,
and replica identity to equal their seed/input projections before artifact
resolution. For down it fills the nine-key input from kind-specific live
preflight results, constructs the exact typed completed/partial cleanup-
rederivation input from locked retained preimages, and passes that input to the
down construction root.
Recovery repeats either projection and must reproduce the same
canonical input. No manager, transport decoder, fixture loader, or stored
capsule can provide replacement renderer-input fields.

`sky/serve/resource_action_renderer.py` and its exact nine public V1 functions
remain sealed. Native construction lives in the separate
`sky/serve/resource_action_renderer_v2.py` module. Its root
`construct_provider_kubernetes_execution_capsule_v2(renderer_input,
resolved_cohort)` performs the same nonrecursive validate -> resolve -> render
-> body-validate -> request-normalize -> object-plan -> append -> contextual-
revalidate order directly over V2 types. It returns
`ProviderKubernetesExecutionCapsuleV2` and may not instantiate, parse, or call a
constructor for `ProviderKubernetesExecutionCapsuleV1`,
`ProviderKubernetesExecutionCapsuleSeedV1`, or
`ProviderKubernetesRendererInputV1`. The V2 down root
`construct_provider_kubernetes_down_execution_capsule_v2(down_input,
resolved_cohort, cleanup_rederivation_input)` invokes
`rederive_provider_kubernetes_cleanup_target_v2()` internally, constructs
`ProviderKubernetesDownExecutionCapsuleV2` directly from only that output, has
no renderer input, and performs its full external cohort/scope/principal /
prerequisite/cleanup context validation without constructing a V1 down
capsule. A cleanup target is never a direct construction-root argument.

The V2 cohort owns two new top-level package artifacts and does not widen a V1
artifact in place:

- `sky/serve/resource_action_artifacts/provider_authority_v2/artifact_inventory.json`
  has contract `provider_authority_artifact_inventory_v2`. Its ordered roles
  are exactly `outer_template`, `node_fragment`, `binding_schema`,
  `config_access_inventory`, `admitted_object_normalization`, and
  `representability_case_inventory`. The first, second, and fifth roles may point to
  the byte-identical shipped V1 leaf artifacts because their schema contracts
  and rendered object leaves remain V1. The third points only to
  `sky/serve/resource_action_artifacts/kubernetes_renderer_v2/binding_schema.json`:
  it has schema ID
  `skypilot.serve.prebooted-direct-pod.bindings.v2`, the same closed 17 binding
  names/pointers/transforms/targets, and exact
  `input_contract="validated_launch_spec_v2"`; the V1 binding schema hardcodes
  `validated_launch_spec_v1` and is invalid V2 evidence. The fourth points only
  to
  `sky/serve/resource_action_artifacts/kubernetes_renderer_v2/config_access_inventory.json`;
  its schema ID is
  `skypilot.serve.prebooted-direct-pod.config-access-inventory.v2`, and it
  inventories all four pure roots named below, their complete internal call
  graph, every explicit typed input root, and every allowed pointer (including
  exact zero-access declarations). The sixth points only to the closed
  representability case-index specified below; that index then
  content-addresses exactly two bounded explicit shards. Neither index nor
  shard contains fixture references, rendered values, or result hashes. All
  six top-level paths are distinct and every role has an exact size/hash
  binding; startup/preflight compare every capsule renderer reference with the
  first five roles byte-for-byte and validate the sixth role's complete
  index/shard closure.
- `sky/serve/resource_action_artifacts/provider_authority_v2/callable_inventory.json`
  has contract `provider_authority_callable_inventory_v2`. `handlers` remains
  the exact four-name handler/result-codec registry used by the claim filter;
  it does not silently become a renderer inventory. A separate ordered
  `pure_entrypoints` array has exactly four roles:
  `launch_capsule_constructor`, `down_capsule_constructor`,
  `cleanup_target_rederiver`, and `representability_enumerator`, each with one
  importable module/qualname pair. Their exact qualified callables are,
  respectively,
  `sky.serve.resource_action_renderer_v2.construct_provider_kubernetes_execution_capsule_v2`,
  `sky.serve.resource_action_renderer_v2.construct_provider_kubernetes_down_execution_capsule_v2`,
  `sky.serve.resource_action_cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2`,
  and
  `sky.serve.resource_action_representability.enumerate_provider_resource_action_representability_v2`.
  Runtime projects both arrays from the actual
  registry/imports and requires canonical byte equality with the installed
  artifact. The V2 config-access inventory, not this top-level callable list,
  owns the complete internal call/access graph reachable from all four pure
  roots, including renderer leaves, cleanup binding/rederivation helpers, and
  representability case dispatch.

The V2 static manifest binds these two V2 inventories. A V2 cohort may not
point at `provider_authority_v1/renderer_artifact_inventory.json`, use the V1
config-access inventory, or claim V2 construction from the four-handler-only V1
callable inventory. The immutable image/source qualification, installed bytes,
V2 artifact inventory, V2 callable inventory, capsule renderer references, and
native V2 config-access inventory must all agree before a complete response is
representable. This is an additive V2 boundary; it does not rename or mutate
the five shipped leaf formats.

### Packaged candidate renderer artifact formats

All five renderer artifacts are RFC 8259 JSON encoded as canonical UTF-8 by the
repository canonicalizer: object keys are sorted, insignificant whitespace is
absent, duplicate keys/nonfinite numbers are invalid, and the raw artifact is
the compact canonical JSON followed by exactly one LF byte. They are data, not
Jinja, Python, YAML, or executable source. The exact checked-in candidate bytes
are:

| Role | Bytes | Raw SHA-256, including one final LF |
|---|---:|---|
| `outer_template` | 972 | `769039b9c25956833032fb670148797c3ba74cd5a12253faf1e99443a27444b8` |
| `node_fragment` | 1,632 | `2000b68c74ccb6710e43b03963cf31f40c35ec879743977a3e3ba6ff3baa43db` |
| `binding_schema` | 4,520 | `2c64a3ed8ee6ac3108fbf13d509ef348c73937d60473b5f697b24ee077611aef` |
| `config_access_inventory` | 23,710 | `19901e8e0491a4e9f957f7ff2a1244fc1baff132c37015c9e8e726af2d538f13` |
| `admitted_object_normalization` | 3,033 | `3ab35d775ff1324587c1c10854d5de8572ce127a8541dc08d85349be06e8f850` |

Packaging and local resolution of these bytes do not claim that they have been
accepted into an executor-cohort inventory.

`outer_template` has schema ID
`skypilot.serve.prebooted-direct-pod.outer-template.v1` and exactly these
top-level keys:

```text
{
  schema: "skypilot.serve.prebooted-direct-pod.outer-template.v1",
  contract: "serve_prebooted_direct_pod_v1",
  object_order: ["head_ssh_service", "head_service", "head_pod"],
  service_templates: [
    {role: "head_ssh_service", body: RendererTemplateValueV1},
    {role: "head_service", body: RendererTemplateValueV1}
  ],
  pod_fragment_role: "node_fragment"
}
```

`node_fragment` has schema ID
`skypilot.serve.prebooted-direct-pod.node-fragment.v1` and exact shape
`{schema, role:"head_pod", body:RendererTemplateValueV1}`. The two Service
bodies and Pod body encode exactly
`KubernetesServeThreeObjectBodySchemaV1`. A `RendererTemplateValueV1` is an
ordinary canonical JSON value or the exact whole-value marker
`{"$binding":BindingName}`. `$binding` is reserved anywhere else; string
interpolation, conditionals, loops, includes, merge keys, and partially bound
strings are invalid. The renderer substitutes each marker with the complete
typed JSON value, appends the one node-fragment body after the two Service
bodies, and emits exactly the displayed role order.

`binding_schema` has schema ID
`skypilot.serve.prebooted-direct-pod.bindings.v1` and exact top-level keys
`schema`, `input_contract`, `marker_key`, `bindings`,
`forbidden_source_pointers`, and `output_contract`.
`input_contract="validated_launch_spec_v1"`, `marker_key="$binding"`, and
`output_contract="KubernetesServeThreeObjectBodySchemaV1"`.
`forbidden_source_pointers` is exactly
`["/retained_source","/seed/post_provision/job_submission/run_source",
"/seed/renderer"]` for binding resolution. The staged-input validator may
compare those source copies and resolve renderer artifact references, but none
may supply a manifest value.

Each binding is the exact JSON object
`{name,source_pointer,transform,json_type,targets}`. `name` matches
`[a-z][a-z0-9_]{0,63}`. `source_pointer` is one absolute RFC 6901 pointer into
the canonical `ProviderKubernetesRendererInputV1` root. `json_type` is one of
`string`, `object`, or `array`. `targets` is a nonempty array of exact objects
`{artifact_role,pointer}`; the pointer is rooted at the parsed artifact named by
`artifact_role`. The binding array is strictly increasing by UTF-8 binding
name. Each target array is sorted first by the five-role artifact sequence and
then by pointer bytes. JSON duplicate object keys, duplicate binding names,
duplicate target pairs within or across bindings, an unlisted marker, a listed
target without the matching marker, or an unused binding all reject before
rendering. Repeated use of one binding is permitted only through its explicitly
listed distinct targets.

The exact binding array is:

In the table, `outer_template:/x` is only a compact rendering of the literal
JSON target `{"artifact_role":"outer_template","pointer":"/x"}`; artifact
files contain the object form, never the colon shorthand.

| Binding | Source pointer | Transform / type | Exact targets |
|---|---|---|---|
| `head_labels` | `/seed/topology/mutable_objects/1/labels` | `label_pairs_to_exact_object_v1` / object | `outer_template:/service_templates/1/body/metadata/labels` |
| `head_name` | `/seed/topology/mutable_objects/1/name` | `copy_v1` / string | `outer_template:/service_templates/1/body/metadata/name` |
| `head_pod_labels` | `/seed/topology/mutable_objects/2/labels` | `label_pairs_to_exact_object_v1` / object | `node_fragment:/body/metadata/labels` |
| `head_pod_name` | `/seed/topology/mutable_objects/2/name` | `copy_v1` / string | `node_fragment:/body/metadata/name` |
| `head_service_selector` | `/seed/topology/mutable_objects/2/labels` | `head_service_selector_v1` / object | `outer_template:/service_templates/0/body/spec/selector`, `outer_template:/service_templates/1/body/spec/selector` |
| `head_ssh_labels` | `/seed/topology/mutable_objects/0/labels` | `label_pairs_to_exact_object_v1` / object | `outer_template:/service_templates/0/body/metadata/labels` |
| `head_ssh_name` | `/seed/topology/mutable_objects/0/name` | `copy_v1` / string | `outer_template:/service_templates/0/body/metadata/name` |
| `image_pull_policy` | `/seed/resources/image_pull_policy` | `copy_v1` / string | `node_fragment:/body/spec/containers/0/imagePullPolicy` |
| `original_user` | `/seed/request_identity/original_user` | `copy_v1` / string | `node_fragment:/body/metadata/annotations/skypilot-user` |
| `pod_cpu_limit` | `/seed/resources/pod_cpu_limit` | `copy_v1` / string | `node_fragment:/body/spec/containers/0/resources/limits/cpu` |
| `pod_cpu_request` | `/seed/resources/pod_cpu_request` | `copy_v1` / string | `node_fragment:/body/spec/containers/0/resources/requests/cpu` |
| `pod_memory_limit` | `/seed/resources/pod_memory_limit` | `copy_v1` / string | `node_fragment:/body/spec/containers/0/resources/limits/memory` |
| `pod_memory_request` | `/seed/resources/pod_memory_request` | `copy_v1` / string | `node_fragment:/body/spec/containers/0/resources/requests/memory` |
| `replica_id_text` | `/resource_identity/replica_id` | `decimal_integer_text_v1` / string | `node_fragment:/body/spec/containers/0/env/0/value` |
| `target_namespace` | `/seed/scope/namespace` | `copy_v1` / string | `outer_template:/service_templates/0/body/metadata/namespace`, `outer_template:/service_templates/1/body/metadata/namespace`, `node_fragment:/body/metadata/namespace` |
| `workload_image` | `/seed/resources/image/qualification/requested_reference` | `copy_v1` / string | `node_fragment:/body/spec/containers/0/image` |
| `workload_service_account` | `/seed/principals/workload/name` | `copy_v1` / string | `node_fragment:/body/spec/serviceAccount`, `node_fragment:/body/spec/serviceAccountName` |

`copy_v1` copies the already-canonical value without coercion.
`decimal_integer_text_v1` accepts only the validated nonnegative integer and
emits its base-10 form with no sign or leading zero except `0`.
`label_pairs_to_exact_object_v1` requires the role-exact ordered label-pair
array, rejects duplicate/missing/extra/mis-role entries, and emits the complete
five-key object above. `head_service_selector_v1` accepts only the exact
head-Pod label-pair array and emits exactly `component`,
`skypilot-cluster-name`, `skypilot.co/cluster-record-uuid`, and
`skypilot.co/serve-replica-incarnation`. Source pointers may repeat only where
the table explicitly uses different transforms. The retained source is
byte-compared during staged-input validation but intentionally has no binding.

Binding resolution returns the closed typed value
`ResolvedProviderKubernetesBindingSetV1 =
{version:1,contract:"skypilot.serve.prebooted-direct-pod.resolved-bindings.v1",
bindings:[ResolvedProviderKubernetesBindingV1]}`. Each binding entry has exactly
`{sequence,name,json_type,value}`: `sequence` is its zero-based position in the
17-row name-sorted binding table, `name` and `json_type` are byte-equal to that
row, and `value` is canonical JSON of exactly that declared type produced by
applying the row's transform to its validated input pointer. The array contains
exactly all 17 rows in table order, with no missing, extra, duplicate, or
untyped value. `render_provider_kubernetes_objects_v1` invokes
`resolve_provider_kubernetes_bindings_v1`, receives this value as the sole
`resolved_bindings` transient, and substitutes only those typed values at the
targets in the already-resolved binding schema. Rendering cannot re-read a
binding source or invoke a transform itself.

`config_access_inventory` has schema ID
`skypilot.serve.prebooted-direct-pod.config-access-inventory.v1` and exact keys
`schema`, `artifact_roles`, `entrypoints`, `call_graph`, `input_access`,
`transient_flow`, `provider_operations`, and `forbidden_sources`. Every
collection below is an array, never a JSON object used as an unordered map.
Every entry has a required integer `sequence` equal to its zero-based array
position.

The resolved config-inventory member uses the implementation contract
`ResolvedProviderKubernetesConfigAccessInventoryArtifactV1 =
{artifact_ref:ProviderRepoArtifactRefV1,
raw_artifact:RawCanonicalRendererArtifactBytesV1,
inventory:ProviderKubernetesConfigAccessInventoryV1}`. The raw wrapper proves
the pinned file is compact canonical RFC 8259 JSON followed by exactly one LF,
and the typed inventory parser enforces the closed schema below. It must not be
parsed through `CanonicalJsonObject`: that generic value contract rejects empty
text, while every exact CoreV1 object-session entry intentionally has
`api_group=""`. The typed parser permits that empty literal only at those exact
CoreV1 `api_group` fields; an empty string anywhere else remains invalid.

An artifact-role entry has exact shape
`{sequence,role,schema_id,consumers}`. `consumers` is a nonempty sorted,
duplicate-free array of qualified callable names. The five literal entries are:

| Sequence / role | Schema ID | Consumers |
|---|---|---|
| 0 / `outer_template` | `skypilot.serve.prebooted-direct-pod.outer-template.v1` | `sky.serve.resource_action_renderer.render_provider_kubernetes_objects_v1`, `sky.serve.resource_action_renderer.resolve_provider_kubernetes_renderer_artifacts_v1` |
| 1 / `node_fragment` | `skypilot.serve.prebooted-direct-pod.node-fragment.v1` | `sky.serve.resource_action_renderer.render_provider_kubernetes_objects_v1`, `sky.serve.resource_action_renderer.resolve_provider_kubernetes_renderer_artifacts_v1` |
| 2 / `binding_schema` | `skypilot.serve.prebooted-direct-pod.bindings.v1` | `sky.serve.resource_action_renderer.resolve_provider_kubernetes_bindings_v1`, `sky.serve.resource_action_renderer.resolve_provider_kubernetes_renderer_artifacts_v1` |
| 3 / `config_access_inventory` | `skypilot.serve.prebooted-direct-pod.config-access-inventory.v1` | `sky.serve.resource_action_renderer.resolve_provider_kubernetes_renderer_artifacts_v1`, `sky.serve.resource_action_renderer.validate_provider_kubernetes_config_access_inventory_v1` |
| 4 / `admitted_object_normalization` | `skypilot.kubernetes.admitted-object-normalization.v1` | `sky.serve.resource_action_provider_artifacts.normalize_kubernetes_admitted_object_v1`, `sky.serve.resource_action_provider_artifacts.normalize_kubernetes_request_object_v1`, `sky.serve.resource_action_renderer.build_provider_kubernetes_object_plans_v1`, `sky.serve.resource_action_renderer.resolve_provider_kubernetes_renderer_artifacts_v1` |

Those qualified renderer/normalizer callables are implemented. Acceptance
tests import-resolve and AST-check exactly those names and reject another
public renderer entrypoint, call edge, project helper, or input access.

An entrypoint has exact shape `{sequence,phase,qualified_name}`. The exact phase
order and qualified names are:

| Sequence / phase | Qualified name |
|---|---|
| 0 / `staged_construct` | `sky.serve.resource_action_renderer.construct_provider_kubernetes_execution_capsule_v1` |
| 1 / `input_validate` | `sky.serve.resource_action_renderer.validate_provider_kubernetes_renderer_input_v1` |
| 2 / `artifact_resolve` | `sky.serve.resource_action_renderer.resolve_provider_kubernetes_renderer_artifacts_v1` |
| 3 / `inventory_validate` | `sky.serve.resource_action_renderer.validate_provider_kubernetes_config_access_inventory_v1` |
| 4 / `binding_resolve` | `sky.serve.resource_action_renderer.resolve_provider_kubernetes_bindings_v1` |
| 5 / `render` | `sky.serve.resource_action_renderer.render_provider_kubernetes_objects_v1` |
| 6 / `body_validate` | `sky.serve.resource_action_renderer.validate_kubernetes_serve_three_object_body_v1` |
| 7 / `request_normalize` | `sky.serve.resource_action_provider_artifacts.normalize_kubernetes_request_object_v1` |
| 8 / `object_plan_build` | `sky.serve.resource_action_renderer.build_provider_kubernetes_object_plans_v1` |
| 9 / `capsule_assemble` | `sky.serve.resource_action_renderer.assemble_and_revalidate_provider_kubernetes_execution_capsule_v1` |
| 10 / `admitted_normalize` | `sky.serve.resource_action_provider_artifacts.normalize_kubernetes_admitted_object_v1` |

A call-graph entry has exact shape `{sequence,caller,callees}`. There is exactly
one entry per entrypoint in that same sequence. `caller` and every callee are
the exact qualified names above. A callee array is duplicate-free and ordered
by required invocation order, not lexical order. The complete graph is:

| Sequence / caller phase | Exact callee phase array |
|---|---|
| 0 / `staged_construct` | [`input_validate`, `artifact_resolve`, `inventory_validate`, `render`, `body_validate`, `request_normalize`, `object_plan_build`, `capsule_assemble`] |
| 1 / `input_validate` | [] |
| 2 / `artifact_resolve` | [] |
| 3 / `inventory_validate` | [] |
| 4 / `binding_resolve` | [] |
| 5 / `render` | [`binding_resolve`] |
| 6 / `body_validate` | [] |
| 7 / `request_normalize` | [] |
| 8 / `object_plan_build` | [] |
| 9 / `capsule_assemble` | [] |
| 10 / `admitted_normalize` | [] |

The phase tokens in this graph serialize as their table-mapped qualified names;
they are not a second alias accepted by the artifact. The first candidate may
use no additional project-qualified helper. Standard-library operations and
the already-closed canonical DTO constructors/serializers are language and
contract primitives, not ambient project helpers; they may not read config,
environment, filesystem paths other than the artifact resolver's five pinned
references, provider clients, or mutable global state. Any additional
project-qualified callable requires a design/inventory update and new
fingerprint before acceptance.
The staged constructor passes the resolver's typed
`admitted_object_normalization` member directly to both normalizers and to
`object_plan_build`; the normalizers consume its `contract`, while
`object_plan_build` copies its `artifact_ref` byte-for-byte into every plan's
`normalization_profile`. No one may resolve that reference again, capture the
contract in a closure, consult a module global, or insert an unlisted adapter
or helper between these entrypoints. `render` obtains `resolved_bindings` only
as the direct typed result of its listed `binding_resolve` callee.

An input-access entry has exact shape
`{sequence,consumer,source_pointer,disposition,use,binding_names}`.
`source_pointer` is rooted at `ProviderKubernetesRendererInputV1` and
`input_access` inventories only semantic reads from that root. Passing the
already-typed root or a transient value along a listed call edge is not an
unlisted semantic read. Transient artifacts, bindings, bodies, normalizations,
plans, and the completed capsule are closed separately by `transient_flow`
below.
`binding_names` is sorted and nonempty only for `use="manifest_binding"`; it is
empty otherwise. The array is sorted by consumer entrypoint sequence then
pointer bytes. The complete 51-entry sequence is:

| Seq. | Consumer phase | Pointer | Disposition / use | Binding names |
|---:|---|---|---|---|
| 0 | `staged_construct` | `/contract` | `fixed` / `root_contract` | `[]` |
| 1 | `staged_construct` | `/version` | `fixed` / `root_contract` | `[]` |
| 2 | `input_validate` | `/name_basis` | `embedded` / `name_basis_recompute` | `[]` |
| 3 | `input_validate` | `/resource_identity` | `embedded` / `resource_identity_fence` | `[]` |
| 4 | `input_validate` | `/retained_source` | `embedded` / `byte_equal_source` | `[]` |
| 5 | `input_validate` | `/seed/post_provision/job_submission/run_source` | `embedded` / `byte_equal_source` | `[]` |
| 6 | `input_validate` | `/seed/renderer/source` | `embedded` / `byte_equal_source` | `[]` |
| 7 | `input_validate` | `/seed/request_identity/cleaned_user` | `embedded` / `request_identity_projection` | `[]` |
| 8 | `input_validate` | `/seed/request_identity/frozen_user_hash` | `embedded` / `request_identity_projection` | `[]` |
| 9 | `input_validate` | `/seed/request_identity/original_user` | `embedded` / `request_identity_projection` | `[]` |
| 10 | `input_validate` | `/seed/topology/mutable_objects` | `embedded` / `topology_identity` | `[]` |
| 11 | `input_validate` | `/sky_cluster_name` | `embedded` / `display_name_projection` | `[]` |
| 12 | `input_validate` | `/sky_cluster_record_uuid` | `embedded` / `cluster_record_identity` | `[]` |
| 13 | `artifact_resolve` | `/seed/renderer/admitted_object_normalization` | `content_addressed` / `artifact_ref` | `[]` |
| 14 | `artifact_resolve` | `/seed/renderer/binding_schema` | `content_addressed` / `artifact_ref` | `[]` |
| 15 | `artifact_resolve` | `/seed/renderer/config_access_inventory` | `content_addressed` / `artifact_ref` | `[]` |
| 16 | `artifact_resolve` | `/seed/renderer/node_fragment` | `content_addressed` / `artifact_ref` | `[]` |
| 17 | `artifact_resolve` | `/seed/renderer/outer_template` | `content_addressed` / `artifact_ref` | `[]` |
| 18 | `binding_resolve` | `/resource_identity/replica_id` | `embedded` / `manifest_binding` | `["replica_id_text"]` |
| 19 | `binding_resolve` | `/seed/principals/workload/name` | `embedded` / `manifest_binding` | `["workload_service_account"]` |
| 20 | `binding_resolve` | `/seed/request_identity/original_user` | `embedded` / `manifest_binding` | `["original_user"]` |
| 21 | `binding_resolve` | `/seed/resources/image/qualification/requested_reference` | `embedded` / `manifest_binding` | `["workload_image"]` |
| 22 | `binding_resolve` | `/seed/resources/image_pull_policy` | `embedded` / `manifest_binding` | `["image_pull_policy"]` |
| 23 | `binding_resolve` | `/seed/resources/pod_cpu_limit` | `embedded` / `manifest_binding` | `["pod_cpu_limit"]` |
| 24 | `binding_resolve` | `/seed/resources/pod_cpu_request` | `embedded` / `manifest_binding` | `["pod_cpu_request"]` |
| 25 | `binding_resolve` | `/seed/resources/pod_memory_limit` | `embedded` / `manifest_binding` | `["pod_memory_limit"]` |
| 26 | `binding_resolve` | `/seed/resources/pod_memory_request` | `embedded` / `manifest_binding` | `["pod_memory_request"]` |
| 27 | `binding_resolve` | `/seed/scope/namespace` | `embedded` / `manifest_binding` | `["target_namespace"]` |
| 28 | `binding_resolve` | `/seed/topology/mutable_objects/0/labels` | `embedded` / `manifest_binding` | `["head_ssh_labels"]` |
| 29 | `binding_resolve` | `/seed/topology/mutable_objects/0/name` | `embedded` / `manifest_binding` | `["head_ssh_name"]` |
| 30 | `binding_resolve` | `/seed/topology/mutable_objects/1/labels` | `embedded` / `manifest_binding` | `["head_labels"]` |
| 31 | `binding_resolve` | `/seed/topology/mutable_objects/1/name` | `embedded` / `manifest_binding` | `["head_name"]` |
| 32 | `binding_resolve` | `/seed/topology/mutable_objects/2/labels` | `embedded` / `manifest_binding` | `["head_pod_labels","head_service_selector"]` |
| 33 | `binding_resolve` | `/seed/topology/mutable_objects/2/name` | `embedded` / `manifest_binding` | `["head_pod_name"]` |
| 34 | `body_validate` | `/resource_identity/replica_id` | `embedded` / `body_expected_value` | `[]` |
| 35 | `body_validate` | `/seed/principals/workload/name` | `embedded` / `body_expected_value` | `[]` |
| 36 | `body_validate` | `/seed/request_identity/original_user` | `embedded` / `body_expected_value` | `[]` |
| 37 | `body_validate` | `/seed/resources/image/qualification/requested_reference` | `embedded` / `body_expected_value` | `[]` |
| 38 | `body_validate` | `/seed/resources/image_pull_policy` | `embedded` / `body_expected_value` | `[]` |
| 39 | `body_validate` | `/seed/resources/pod_cpu_limit` | `embedded` / `body_expected_value` | `[]` |
| 40 | `body_validate` | `/seed/resources/pod_cpu_request` | `embedded` / `body_expected_value` | `[]` |
| 41 | `body_validate` | `/seed/resources/pod_memory_limit` | `embedded` / `body_expected_value` | `[]` |
| 42 | `body_validate` | `/seed/resources/pod_memory_request` | `embedded` / `body_expected_value` | `[]` |
| 43 | `body_validate` | `/seed/scope/namespace` | `embedded` / `body_expected_value` | `[]` |
| 44 | `body_validate` | `/seed/topology/mutable_objects/0/labels` | `embedded` / `body_expected_value` | `[]` |
| 45 | `body_validate` | `/seed/topology/mutable_objects/0/name` | `embedded` / `body_expected_value` | `[]` |
| 46 | `body_validate` | `/seed/topology/mutable_objects/1/labels` | `embedded` / `body_expected_value` | `[]` |
| 47 | `body_validate` | `/seed/topology/mutable_objects/1/name` | `embedded` / `body_expected_value` | `[]` |
| 48 | `body_validate` | `/seed/topology/mutable_objects/2/labels` | `embedded` / `body_expected_value` | `[]` |
| 49 | `body_validate` | `/seed/topology/mutable_objects/2/name` | `embedded` / `body_expected_value` | `[]` |
| 50 | `capsule_assemble` | `/seed` | `embedded` / `seed_copy` | `[]` |

The `consumer` field contains the qualified name for the displayed phase, not
the phase shorthand. Uniqueness is by `(consumer,source_pointer)`; the displayed
cross-consumer repeats are required independent checks, not implicit shared
access. A binding name not byte-equal to the binding artifact, or an access to
another renderer-input pointer, rejects.

`transient_flow` covers every non-`RendererInput` value crossing an entrypoint
edge. An entry has exact shape
`{sequence,name,producer,consumers,value_contract,cardinality}`; producer and
consumers are exact qualified entrypoint names, consumers are ordered by their
entrypoint sequence, and the exact array is:

| Sequence / name | Producer | Consumers | Value contract / cardinality |
|---|---|---|---|
| 0 / `resolved_artifacts` | `artifact_resolve` | [`inventory_validate`, `binding_resolve`, `render`, `request_normalize`, `object_plan_build`, `admitted_normalize`] | `ResolvedProviderKubernetesRendererArtifactSetV1` / `exactly_5` |
| 1 / `resolved_bindings` | `binding_resolve` | [`render`] | `ResolvedProviderKubernetesBindingSetV1` / `exactly_17_name_sorted_unique` |
| 2 / `rendered_bodies` | `render` | [`body_validate`] | `CanonicalJsonObject` / `exactly_3_role_ordered` |
| 3 / `validated_bodies` | `body_validate` | [`request_normalize`, `object_plan_build`] | `ValidatedKubernetesServeThreeObjectBodyV1` / `exactly_3_role_ordered` |
| 4 / `request_normalizations` | `request_normalize` | [`object_plan_build`] | `ProviderKubernetesRequestNormalizationV1` / `exactly_3_role_ordered` |
| 5 / `object_plans` | `object_plan_build` | [`capsule_assemble`] | `ProviderKubernetesObjectPlanV1` / `exactly_3_role_ordered` |
| 6 / `completed_capsule` | `capsule_assemble` | [`staged_construct`] | `ProviderKubernetesExecutionCapsuleV1` / `exactly_1` |

For `resolved_artifacts`, a consumer receives either the complete typed set or
the exact named member required by its signature. Direct typed member
projection is part of this one producer flow, not an eighth transient:
normalizers receive `admitted_object_normalization`, and `object_plan_build`
receives that same member to copy its reference. No consumer may project an
unlisted member or convert a member to an untyped mapping.

No transient entry may carry a client, callback, credential, ambient or
unpinned filesystem path, environment view, completed-capsule backpointer, or
untyped dict in place of its displayed canonical contract. The only path
fields are the five already-validated pinned `ProviderRepoArtifactRefV1`
members inside `resolved_artifacts`; consumers may copy the exact
`admitted_object_normalization.artifact_ref` but cannot resolve or replace it.
Missing, extra, reordered, or multiply produced transient entries reject.

`provider_operations` is the exact object
`{renderer:[],normalizer:[],object_session:[...],preflight_contracts:[...]}`.
The empty arrays are literal proof that rendering and normalization perform no
Kubernetes/provider I/O. An object-session entry has exact shape
`{sequence,phase,role,api_group,api_version,resource,verb,scope,result_use}`.
Entries are serialized phase-major using phase order below, then role order
`head_ssh_service`, `head_service`, `head_pod`; sequence is
`phase_index * 3 + role_index`:

| Phase index / phase | Verb | Result use |
|---|---|---|
| 0 / `create` | `create` | `admitted_readback` |
| 1 / `readback_201` | `get` | `exact_compare` |
| 2 / `readback_409` | `get` | `exact_adopt_or_conflict` |
| 3 / `observe` | `get` | `reconcile` |
| 4 / `delete` | `delete` | `uid_precondition` |

For both Service roles each expanded entry has `api_group=""`,
`api_version="v1"`, `resource="services"`, and `scope="Namespaced"`; for the
Pod role it instead has `resource="pods"` with the other three literals
unchanged. Thus the array has exactly 15 entries. No list, watch, patch, update,
deletecollection, proxy, exec, port-forward, or other resource entry exists.

Each preflight-contract entry has exact shape
`{sequence,action_kind,access_matrix_artifact_role,comparison}` and the array is
exactly `launch/launch_access_matrix` then `down/down_access_matrix`, both with
`comparison="complete_byte_equal"`. Their separately content-addressed
matrices own prerequisite and authorization-review operations; this inventory
cannot broaden them.

`forbidden_sources` is the exact sorted array
`["ambient_filesystem","ambient_kubernetes_context","capsule_objects",
"credentials","custom_provisioner","environment","generic_jinja",
"global_user_state","provider_discovery","proxy","raw_kubeconfig","secret",
"skypilot_config","unpinned_filesystem_path"]`. The artifact resolver's sole
filesystem authority is descriptor-safe resolution of the five exact
`repo_path`/`byte_size`/`sha256` references read at input-access sequences
13–17 beneath the installed distribution root that contains the imported
top-level `sky/` package; it rejects symlinks, path escape,
nonregular files, size/hash drift, and every caller-supplied or discovered path.
That bounded content-addressed read is not `ambient_filesystem` or
`unpinned_filesystem_path`. A missing or extra artifact role, input access,
callable, graph edge, provider operation, or forbidden-source literal rejects.

For these five v1 renderer members, `ProviderRepoArtifactRefV1.repo_path` is a
repository-root-relative canonical POSIX path, not a path relative to the
`sky/` package directory. The exact role-to-path map is
`sky/serve/resource_action_artifacts/kubernetes_renderer_v1/outer_template.json`,
`node_fragment.json`, `binding_schema.json`, `config_access_inventory.json`,
and `admitted_object_normalization.json` under that same directory. At runtime
the resolver derives that distribution root only from the imported regular
`sky` package location, opens from the root descriptor, and requires the
complete role-exact path; it does not scan or discover another `sys.path`
entry. A path with another prefix or basename rejects even if its bytes, size,
and hash match an approved member. Packaging adds the exact
`recursive-include sky/serve/resource_action_artifacts/kubernetes_renderer_v1 *.json`
rule and a built-wheel content test for all five paths. This renderer-specific
rule does not narrow the general-purpose `ProviderRepoArtifactRefV1` value
type.

`admitted_object_normalization` has schema ID
`skypilot.kubernetes.admitted-object-normalization.v1` and exact keys `schema`,
`comparison_contract`, `request_schema`, `readback_preconditions`,
`strip_top_level`, `strip_metadata`, `request_allocation_rules`,
`admitted_parameters`, `admitted_allocation_rules`, `array_order`,
`unknown_path`, and `retained_defaults`. The sole readback
precondition is the closed object
`{pointer:"/metadata/deletionTimestamp",allowed:["absent",null]}` and is
evaluated before any strip. `comparison_contract` is
`kubernetes_admitted_object_v1`, `request_schema` is
`KubernetesServeThreeObjectBodySchemaV1`, `strip_top_level` is exactly
`["status"]`, `strip_metadata` is exactly `["uid","resourceVersion",
"generation","creationTimestamp","deletionTimestamp","managedFields"]`,
`array_order="preserve"`, `unknown_path="conflict"`, and
`retained_defaults="all_explicit_in_request"`.

`admitted_parameters` is exactly
`[{sequence:0,name:"require_pod_node_name",kind:"keyword_only",
type:"builtin_bool",required:true,default:"absent"}]`. The public admitted
normalizer signature is exactly
`normalize_kubernetes_admitted_object_v1(role, admitted_object,
normalization_artifact, *, require_pod_node_name)`, where
`normalization_artifact` is the required typed
`ResolvedProviderKubernetesNormalizationArtifactV1` member defined above.
`admitted_parameters` inventories caller-selected normalization behavior, not
that fixed artifact dependency. Omission of `require_pod_node_name` raises the
language's missing-required-keyword
error, and the entrypoint then requires `type(require_pod_node_name) is bool`
before inspecting either object. The artifact cannot supply, override, or
default that invocation value.

A request-allocation rule is the exact object
`{sequence,role,kind,present,absent,intent,semantic_removals}`. `present` entries
have exact shape `{sequence,json_pointer,value}`; the other two pointer arrays
are ordered, duplicate-free string arrays. The complete serializable array is:

```text
[
  {sequence: 0, role: "head_ssh_service", kind: "Service",
   present: [],
   absent: ["/spec/clusterIP", "/spec/clusterIPs", "/spec/ipFamilies",
            "/spec/ipFamilyPolicy"],
   intent: "allocate_single_stack_cluster_ip", semantic_removals: []},
  {sequence: 1, role: "head_service", kind: "Service",
   present: [{sequence: 0, json_pointer: "/spec/clusterIP", value: "None"}],
   absent: ["/spec/clusterIPs", "/spec/ipFamilies",
            "/spec/ipFamilyPolicy"],
   intent: "headless_single_stack",
   semantic_removals: ["/spec/clusterIP"]},
  {sequence: 2, role: "head_pod", kind: "Pod",
   present: [], absent: ["/spec/nodeName"],
   intent: "schedule_one_node", semantic_removals: []}
]
```

An admitted-allocation rule is the exact object
`{sequence,role,kind,cardinality,parameter_cardinality,entries,constraints}`.
`parameter_cardinality` is null for Services. For the Pod it is the exact
object `{parameter:"require_pod_node_name",false_value:"zero_or_1",
true_value:"exactly_1"}`. Each entry is
`{sequence,json_pointer,allocator,value_schema}`. The complete array is:

```text
[
  {sequence: 0, role: "head_ssh_service", kind: "Service",
   cardinality: "exactly_4", parameter_cardinality: null,
   entries: [
     {sequence: 0, json_pointer: "/spec/clusterIP",
      allocator: "api_server", value_schema: "canonical_ip_text"},
     {sequence: 1, json_pointer: "/spec/clusterIPs",
      allocator: "api_server", value_schema: "singleton_cluster_ip"},
     {sequence: 2, json_pointer: "/spec/ipFamilies",
      allocator: "api_server", value_schema: "singleton_matching_ip_family"},
     {sequence: 3, json_pointer: "/spec/ipFamilyPolicy",
      allocator: "api_server", value_schema: "literal_SingleStack"}],
   constraints: ["clusterIPs_0_equals_clusterIP",
                 "ipFamilies_0_matches_clusterIP"]},
  {sequence: 1, role: "head_service", kind: "Service",
   cardinality: "exactly_4", parameter_cardinality: null,
   entries: [
     {sequence: 0, json_pointer: "/spec/clusterIP",
      allocator: "api_server", value_schema: "literal_None"},
     {sequence: 1, json_pointer: "/spec/clusterIPs",
      allocator: "api_server", value_schema: "singleton_literal_None"},
     {sequence: 2, json_pointer: "/spec/ipFamilies",
      allocator: "api_server", value_schema: "singleton_IPv4_or_IPv6"},
     {sequence: 3, json_pointer: "/spec/ipFamilyPolicy",
      allocator: "api_server", value_schema: "literal_SingleStack"}],
   constraints: ["clusterIP_and_clusterIPs_0_are_None"]},
  {sequence: 2, role: "head_pod", kind: "Pod",
   cardinality: null,
   parameter_cardinality: {parameter: "require_pod_node_name",
                           false_value: "zero_or_1",
                           true_value: "exactly_1"},
   entries: [{sequence: 0, json_pointer: "/spec/nodeName",
              allocator: "scheduler",
              value_schema: "kubernetes_dns_subdomain"}],
   constraints: ["absent_only_in_unscheduled_partial_evidence",
                 "write_once_when_present"]}
]
```

The admitted result serializes `server_allocations` as the ordered array of
`{json_pointer,allocator,value}` entries after applying the selected rule; it
never serializes pointers as JSON object keys. Services return exactly four
entries. Pod returns zero or one when `require_pod_node_name` is exactly false
and exactly one when it is exactly true. The checked
interpreter implements only these closed arrays; the artifact cannot name a
callable, predicate, regex, patch, or arbitrary JSON Pointer outside them.

### `boltz-test` server-side dry-run evidence (2026-08-02)

At 13:47-14:50 UTC, Kubernetes context `boltz-test` reported server
`v1.33.13-eks-8f14419`. The retained Service readbacks have API-server capture
timestamp `2026-08-02T14:18:42Z`; the corrected `1G` Pod readback has timestamp
`2026-08-02T14:50:08Z`. Only `dryRun=All` creates were submitted; follow-up GETs
returned NotFound for all three probe names. The future
`skypilot-actions-canary` namespace did not exist, so the representative
candidate used the release's deployed `skypilot-ha-workloads` namespace and
its `skypilot-service-account`. This freezes the candidate body/default schema
but is not the eventual canary-namespace activation proof.

The exact retained requests, raw dry-run readbacks, hashes, normalized hash
domains, and reproduction commands are under
[`docs/designs/evidence/skyserve-resource-action-renderer-v1/`](evidence/skyserve-resource-action-renderer-v1/README.md).
The six raw JSON files and their byte commitments are:

| Evidence file | Bytes | Raw SHA-256, including one final LF |
|---|---:|---|
| `head_ssh_service.request.json` | 764 | `97c3e83ff160245ef3d8c7a66d7cb99ef9e765768395105f626cccc8abb91e98` |
| `head_ssh_service.dryrun.json` | 987 | `a91e2b9f3c0dd2ddc69f0fd838997ecc611caf16ffd77c43e2aa2e78b6ff2560` |
| `head_service.request.json` | 720 | `441f855dc0baddd66bbe0d45c8cb709e17ad34df6bbb8fc70b0a0eb7b4f086e5` |
| `head_service.dryrun.json` | 912 | `13bff0588a85daa28a3cba9ad49e80f345a119a4d8152ea3946e040e8a193b52` |
| `head_pod.request.json` | 1,655 | `c5aa3dfe8232a364151da16c650252b4da3b7962a7a636626b168393f93ed937` |
| `head_pod.dryrun.json` | 1,811 | `5254c4578f335f4c35015091dd0f512a9c27b7646120e30638d9347d07915316` |

Repository/evidence artifact hashes cover the raw file including LF. Persisted
`CanonicalJsonObject.sha256` covers compact canonical JSON without LF. After
the role-distinct allocation projections, the no-LF semantic hashes are
`01f85e19668f5ce16850181367f80ad4bb83d2ba2b3db1e314cbf023f583f2c3`
for SSH Service,
`b9f6e3e86df0c26dfe4da1576fe58ba9fd07af0c75c06be920bc5ac65520dd15`
for head Service, and
`eb037b6c53d4900a22532126b08a20eff9144f755a2bbb9e3c24da57d51ddb38`
for Pod. The evidence README reproduces both domains and exact-compares request
and admitted semantic files; a test that hashes the LF-terminated jq output as
the persisted semantic is invalid.

The minimal Pod admission added exactly container
`terminationMessagePath=/dev/termination-log`,
`terminationMessagePolicy=File`, and `protocol=TCP` on all five ports, plus Pod
`dnsPolicy=ClusterFirst`, `enableServiceLinks=true`,
`preemptionPolicy=PreemptLowerPriority`, `priority=0`, `restartPolicy=Always`,
`schedulerName=default-scheduler`, `securityContext={}`, the duplicate
`serviceAccount` scalar, `terminationGracePeriodSeconds=30`, and the two
300-second NoExecute tolerations displayed above. Replaying the request with
every retained default explicit produced the same retained admitted body after
removing status and server metadata. No sidecar, init container, volume,
secret/token projection, extra label/annotation, resource, or scheduling field
was injected.

The SSH Service returned the complete allocation quartet with an IPv4 address
and `SingleStack`, plus retained defaults `type=ClusterIP`,
`sessionAffinity=None`, and `internalTrafficPolicy=Cluster`. The selected
headless Service returned `clusterIP=None`, `clusterIPs=[None]`, IPv4, and
`SingleStack` plus those same retained defaults. Dry-run allocation used a
synthetic address and is evidence of quartet shape, never a reusable allocation
value.

Selector and null/empty probes were decisive. A headless Service with the
nonempty exact selector returned IPv4/`SingleStack`; with selector absent,
`{}`, or null it instead returned `[IPv4,IPv6]` and `RequireDualStack`, which is
outside the frozen v1 allocation contract. Absent, empty, and null head-Service
ports all serialized as absent. Null SSH `clusterIP`, empty `clusterIPs`, and an
absent allocation quartet all triggered allocation. Therefore the request
schema requires the nonempty exact selector and one canonical absence form; it
does not rely on API coercion of null or empty values.

### Required renderer-contract tests

No renderer test currently authorizes provider I/O. Before any candidate
artifact inventory is accepted, one focused suite covers every one of the five
artifact roles and a separate object matrix covers every one of the three
object roles:

| Artifact role | Required positive coverage | Required rejection coverage |
|---|---|---|
| `outer_template` | exact top-level keys, role order, both complete Service bodies, every literal default and marker target | extra/missing/reordered role, YAML/Jinja/string interpolation, stray marker, selector omission |
| `node_fragment` | exact Pod metadata/spec/container/env/ports/resources/defaults and marker targets | sidecar/init/volume/secret, extra label/annotation/env, reordered port/toleration, omitted retained default |
| `binding_schema` | all 17 sorted bindings, 16 distinct sources, every exact target, transforms and output types | duplicate name/key/target, unsorted array, absent/extra/unused marker, forbidden/invalid pointer, coercion, capsule-object read |
| `config_access_inventory` | five artifact entries, 11 entrypoints, exact staged call graph, all 51 RendererInput-root accesses, all seven transient-flow entries including the closed 17-binding set, explicit typed normalization-artifact consumers, empty renderer/normalizer I/O, 15 object-session entries and both preflight contracts | missing/extra access, transient, callable, artifact argument, or edge; wildcard, provider discovery, unlisted helper, closure/global contract capture, ambient/unpinned filesystem, forbidden source, list/watch/patch/update/proxy/exec |
| `admitted_object_normalization` | both exact normalizer signatures with the resolved typed artifact argument, all three request rules, all three admitted rules, full Service quartets, required keyword-only exact built-in Boolean parameter, Pod false zero/one and true exactly-one scheduler allocation, deletion precondition, persisted no-LF hashes | omitted/raw/reference-only/wrong-role artifact, omitted/defaulted/non-Boolean parameter, truthy integer/object, request/admitted shape crossover, partial quartet, reinsertion of head `clusterIP`, allocation reorder/duplicate, nonnull deletion timestamp, injected retained field |

The object matrix contains realistic and maximal goldens for
`head_ssh_service`, `head_service`, and `head_pod`; it exact-checks closed
top-level/metadata/nested key sets, role label maps, Service annotation/env
absence, the Pod's sole annotation/env entry, both selector copies, request
intent, admitted allocations, and request/readback semantic byte equality. A
five-artifact-role test is not a substitute for these three object-role
goldens, or vice versa. The selector absent/empty/null/exact dry-run probe and
both LF/no-LF hash domains are regression fixtures. Staged-constructor tests
also mutate each of the eight renderer-input keys, independently drift the
copied Sky cluster name, reconstructed name basis, and cluster-record UUID, and
prove exact `sky_cluster_name == B.display_name`,
`C = B.provider_cluster_name`, `W = B.workload_name`, topology-name, label, and
selector equality before any artifact or provider operation. They also prove
that `binding_resolve` is the sole producer of the complete typed 17-binding
set, `render` its sole consumer, both normalizers receive the exact resolved
normalization member explicitly, and every object plan copies that member's
artifact reference byte-for-byte.

The table and matrix above remain normative acceptance requirements. The five
artifacts, staged renderer, exact body validator, typed request/admitted
normalizers, and completed-capsule revalidation now exist. The four focused
files `test_serve_resource_action_renderer.py`,
`test_serve_resource_action_renderer_artifacts.py`,
`test_serve_resource_action_renderer_values.py`, and
`test_serve_resource_action_provider_artifacts.py` collect and pass 60 local
cases. They pin the five raw byte preimages, resolve artifacts from the
descriptor-bound imported package, reject non-regular initializer and artifact
leaves without blocking (including FIFO regressions), close the 11-entrypoint
call graph and 51 RendererInput accesses with runtime code-object and CI AST
checks, exercise
the exact three object roles, and cover the pure allocation normalizers. This
is local pure-code evidence, not executor-cohort, candidate-maximal, or live
Kubernetes acceptance.

The code and fixture side of the same-v1 cutover completed atomically: every
minimal/fake `admissionDefaults` fixture and validator acceptance was removed
in the same change that added the exact three bodies, and no reader accepts
both shapes. Before any image containing that cutover runs, a consistent
read-only PostgreSQL preflight must still prove zero persisted represented
launch specs, actions, attempts, and representation links that could contain
the old body shape. A nonzero or indeterminate result aborts and requires a new
comparison version or separately reviewed offline migration; code does not
rewrite or delete such rows. The previously observed empty `boltz-test`
resource-action graph makes the cutover eligible but does not replace this
deployment-time preflight, staged deployment of every reader/writer, or a new
final checkpoint. Until the implemented artifacts are cohort-bound, realistic
plus candidate-maximal goldens pass, and the same dry-run comparison repeats
byte-exactly in the eventual canary namespace, runtime launch normalization
returns `unrepresented_execution_config`, remains shadow-only, and sends no
action-owned provider bytes. The generic Jinja/config path is not a substitute
and is ineligible for authority. Persisted 201/409, scheduler, runtime, P2, and
P3 evidence remain separate later gates and are not claimed by these tests.

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

The exact three-object `ResolvedProviderTargetV1` above is the first deployable
v1 resolved-target wire. An earlier unreleased pre-authority scaffold carried
the scalar target fields but omitted `kubernetes_objects`; it was never
deployed and is not a compatibility format. The closed parser rejects that
flattened shape and any incomplete replacement rather than adding a dual reader
or inferring object identity from provider-resource/workload scalars.

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
`prior_cleanup_target_sha256=null`; discovery is written to the attempt,
leaving that plan immutable. Down admission derives and stores exactly one
matching launch cleanup target, complete or partial, in the new down execution
capsule and stores its hash in the indexed plan. A conflicting second value is
corruption and cannot replace the first.

Live V2 code has one construction boundary for that value. The public pure root
`sky.serve.resource_action_cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2()`
accepts no caller-supplied cleanup target. Its sole input union is:

```text
ProviderKubernetesCleanupClusterRowObservationV2 = {
  version: 2,
  cluster_name: Text,
  cluster_record_uuid: UUID,
  disposition: "exact_handle" | "not_found",
  handle: ProviderKubernetesHandleV1 | null,
  observed_at: UtcTimestamp
}

ProviderKubernetesCompletedCleanupRederivationInputV2 = {
  version: 2,
  source: "completed_launch",
  basis: CompletedLaunchBasisV1,
  source_object_plans: [ProviderKubernetesObjectPlanV1],  # exact three
  cluster_row: ProviderKubernetesCleanupClusterRowObservationV2
}

ProviderKubernetesPartialCleanupRederivationInputV2 = {
  version: 2,
  source: "partial_launch_cleanup",
  basis: PartialLaunchCleanupBasisV1,
  source_object_plans: [ProviderKubernetesObjectPlanV1],  # exact three
  source_progress: ProviderLifecycleProgressV1,
  source_progress_revision: PositiveInteger,
  source_quiescence: ProviderLaunchSupersessionQuiescenceV1,
  cluster_row: ProviderKubernetesCleanupClusterRowObservationV2
}

ProviderKubernetesShadowPartialCleanupRederivationInputV2 = {
  version: 2,
  source: "shadow_partial_launch_cleanup",
  basis: ServeShadowPartialLaunchCleanupBasisV1,
  source_object_plans: [ProviderKubernetesObjectPlanV1],  # exact three
  source_progress: ProviderShadowLifecycleProgressV1,
  source_progress_revision: PositiveInteger,
  source_quiescence: ProviderShadowLaunchSupersessionQuiescenceV1,
  source_terminal_history: ProviderShadowRequestTerminalHistoryV2,
  cluster_row: ProviderKubernetesCleanupClusterRowObservationV2
}
```

The cluster observation requires a nonnull byte-equal same-UUID handle exactly
for `exact_handle` and null exactly for locked NotFound; its timestamp is the
preparation-frozen target `observed_at`. The completed basis supplies the
complete resolved target and launch handle. The action partial input supplies
the exact API006 cursor/revision and reducer-owned quiescence. The shadow
partial input instead supplies the exact class-10 history cursor/revision,
shadow quiescence, and class-17 receipt; no parser accepts one source shape as
the other.

The cleanup-rederivation input is a transient typed join of separately bounded
retained preimages. It is never one wire value, database value, rendered
capsule, or provider request, so its aggregate canonical encoding does not use
the generic 65,536-byte stored/wire-object ceiling; a candidate-maximal partial
input may exceed that ceiling. Every child retains its existing closed parser
and byte bound, the union has an exact key set and exactly three plans, and the
rederived cleanup target and every persisted/wire result remain subject to
their unchanged bounds. This narrow aggregate exemption cannot relax the
60,000-byte qualification budget or any 65,536-byte capsule/envelope limit.

It reconstructs all three `ProviderKubernetesCleanupObjectV1` entries from the
source plans plus committed UID/allocation evidence, then constructs the sole
`ProviderKubernetesCleanupTargetV1`. It reads no database, clock, Kubernetes
client, ambient config, or supplied target; transaction adapters own those
reads and pass only the typed preimages. Rederivation never refreshes
`observed_at`.

`validate_provider_kubernetes_cleanup_target_binding_v2()` is the one shared
pure basis/target binding leaf used by the V2 preflight-seed decoder, down-
capsule constructor, preflight response validator, and rederiver. No duplicate
V2 binding implementation remains in the action or V2 wire module. The frozen
V1 graph may retain its existing V1-local validator for Serve034 history and
cleanup; it is not imported or called as live V2 construction authority. The optimistic
manager preparation, complete preflight evaluation, locked down admission, and
immediate pre-I/O reauthorization all call the same rederiver; the last three
byte-compare its output with the sole seed/capsule copy and recompute the basis,
cleanup-target, plan, and capsule hashes. Only the transaction adapters may
assert that a source row or NotFound observation was locked. Static tests may
parse fixture targets, but no other production function may construct a live
V2 cleanup target. This is the cleanup rederivation boundary covered by the
V2 callable and config-access inventories above.

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

ProviderShadowLaunchEffectClaimV1 = {
  version: 1,
  decision_id: UUID,
  request_sequence: PositiveInteger,
  logical_attempt: PositiveInteger,
  request_role: "PRIMARY_LAUNCH",
  request_id: UUID,
  request_execution_generation: 1,
  worker_attestation: ProviderAuthorityWorkerAttemptAttestationV1,
  worker_attestation_sha256: Sha256
}

ProviderShadowLaunchCommittedEffectEvidenceV1 =
  the exact closed ProviderLaunchCommittedEffectEvidenceV1 union with every
  recursive ProviderLaunchEffectClaimV1 leaf replaced by
  ProviderShadowLaunchEffectClaimV1

ProviderShadowLaunchProgressV1 =
  the exact closed ProviderLaunchProgressV1 phase union with every
  ProviderLaunchEffectClaimV1 leaf and every
  ProviderLaunchCommittedEffectEvidenceV1 child recursively replaced by the
  two shadow types above

ProviderShadowLifecycleProgressV1 = {
  version: 1,
  cursor: ProviderShadowLaunchProgressV1 | ProviderDownProgressV1,
  worker_attestation: null | ProviderAuthorityWorkerAttemptAttestationV1
}

ProviderShadowLaunchNoEffectResolutionV1 =
  the exact closed ProviderLaunchNoEffectResolutionV1 union with both
  intent_origin and resolution_origin replaced by
  ProviderShadowLaunchEffectClaimV1

ProviderShadowLaunchEffectQuiescenceV1 =
  the exact closed ProviderLaunchEffectQuiescenceV1 union with every recursive
  action claim/committed-evidence/no-effect leaf replaced by its shadow sibling

ProviderShadowLaunchSupersessionQuiescenceV1 = {
  version: 1,
  launch_decision_id: UUID,
  launch_request_sequence: PositiveInteger,
  launch_logical_attempt: PositiveInteger,
  request_id: UUID,
  request_terminal_state: "SUCCEEDED",
  active_claim: false,
  shadow_handler_terminal_result_sha256: Sha256,
  launch_provider_cursor_sha256: Sha256,
  effects: [ProviderShadowLaunchEffectQuiescenceV1],
  settled_at: UtcTimestamp
}
```

The `the exact closed ... replaced` notation above is a schema definition, not
a permissive runtime generic. It preserves every key, discriminator, phase,
effect sequence, evidence body, list bound, and hash rule of the named action
type and substitutes only the recursively named identity leaf. Generated strict
parsers have separate concrete shadow classes and cross-reject action-shaped
origins. Shadow origin order is lexicographic
`(logical_attempt, request_sequence, request_execution_generation)`. A carried
origin point-loads the retained predecessor child plus execution history; a
later claim may exact-adopt committed evidence but can never resolve an older
claim's intent as no-effect. The Skylet submission key remains the real
`decision_id`/`would_be_action_id`, never an invented action attempt. Down has
no action-origin leaf and may reuse its structural cursor only under the
shadow-context validator, which binds the parent, child, invocation, history,
and current worker authority.

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
the last safe cursor. The envelope's worker attestation is attempt-scoped. Its
request has exactly one generation-one claim, within which only the write-once
`after: null -> exact identity` completion is legal. A carried cursor in a new
attempt can have null attestation before that request is claimed, but
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
exact frozen V2 action/preflight/cohort preimages plus every exact live
registered worker
identity and claim/attempt-attestation preimage eligible at that check; only
still-unknown response-derived leaves and the reachable five-effect origin
schedule are maximized. It canonical-renders every launch and down progress
shape, every complete/not-representable preflight envelope, every handler no-
effect resolution/return shape, every reducer-built quiescence/outcome, and
every enclosing V2 capsule/config/invocation/plan/spec shape. Every
rendered object, not merely its PostgreSQL JSONB stored rendering, must be at
most 65,536 canonical UTF-8 bytes. Any leaf still lacking a finite protocol
bound makes the candidate unrepresentable. Oversize or unbounded candidates
remain legacy/shadow (or block an already materialized dark action) before any
provider-I/O watermark or intent is committed; runtime truncation, origin
elision, hash-only substitution, and late terminal-result dropping are
forbidden.

The byte ceiling is not a case count. In particular, `65,536` always means the
maximum canonical UTF-8 bytes for one rendered value or one packaged contract
file including its required final LF; it never means 65,536 fixtures. Runtime
and CI use a deliberately one-way bounded artifact DAG. The sixth role of the
top V2 artifact inventory still points at the existing cohort-bound path
`provider_authority_v2/representability_case_inventory.json`, but that file is
now the small closed index below rather than a monolithic `cases` array:

```text
ProviderResourceActionRepresentabilityCaseInventoryIndexV2 = {
  version: 2,
  contract: "provider_resource_action_representability_case_inventory_index_v2",
  profile: "pod_cluster_v1",
  shards: [{
    ordinal: 0 | 1,
    first_case_sequence: NonnegativeInteger,
    last_case_sequence: NonnegativeInteger,
    case_count: PositiveInteger,
    artifact: ProviderRepoArtifactRefV1
  }]  # exactly ordinal 0 then 1
}

ProviderResourceActionRepresentabilityCaseInventoryShardV2 = {
  version: 2,
  contract: "provider_resource_action_representability_case_inventory_shard_v2",
  profile: "pod_cluster_v1",
  ordinal: 0 | 1,
  cases: [{
    sequence: ContiguousNonnegativeInteger,
    case_id: Text,
    dispatch_kind: "authoritative_action" | "shadow_candidate",
    action_kind: "launch" | "down",
    boundary: "complete_preflight" | "linked_admission" |
              "claimed_execution" | "pre_io" |
              "terminalization" | "settlement" |
              "owner_fenced_transition",
    payload_kind: "preflight_request" | "preflight_response" |
                  "cohort" | "worker_identity" |
                  "attempt_attestation" | "renderer_input" |
                  "rendered_body" | "cleanup_target" |
                  "execution_capsule" | "execution_config" |
                  "invocation" | "plan" | "action_spec" |
                  "request_input" | "dispatch_membership" |
                  "execution_authority" |
                  "terminal_authority_selector" |
                  "authority_fence_operation" | "progress" |
                  "no_effect_resolution" | "request_return" |
                  "quiescence" | "action_outcome" |
                  "shadow_progress" |
                  "shadow_request_return" | "shadow_terminal_history" |
                  "shadow_terminal_commitment" |
                  "shadow_settlement_commitment" | "shadow_projection" |
                  "shadow_fallback_evidence" | "shadow_outcome" |
                  "shadow_retry_decision" | "shadow_observation" |
                  "shadow_effect_trace" | "shadow_partial_down_basis"
  }]
}

ProviderResourceActionRepresentabilityGoldenResultsV2 = {
  version: 2,
  contract: "provider_resource_action_representability_golden_results_v2",
  fixture_name: "realistic" | "candidate_maximal",
  mode: "current" | "candidate_maximal",
  case_inventory: ProviderRepoArtifactRefV1,
  results: [{case_sequence, canonical_byte_count, sha256}]
}

ProviderResourceActionRepresentabilityGoldenManifestV2 = {
  version: 2,
  contract: "provider_resource_action_representability_goldens_v2",
  artifact_inventory: ProviderRepoArtifactRefV1,
  case_inventory: ProviderRepoArtifactRefV1,
  fixture_sets: [
    {name: "realistic",
     input: ProviderRepoArtifactRefV1,
     results: ProviderRepoArtifactRefV1},
    {name: "candidate_maximal",
     input: ProviderRepoArtifactRefV1,
     results: ProviderRepoArtifactRefV1}
  ]
}
```

The index has exactly two descriptors. Their artifact paths are exactly
`provider_authority_v2/representability_case_inventory/000.json` and
`provider_authority_v2/representability_case_inventory/001.json`; no alternate
path or third shard is legal. For the current provisional 366-row set, shard 0
is exactly sequences `0..182` and shard 1 exactly `183..365`, with 183 rows in
each. The count is not qualification evidence: only three of seven boundary
families are implemented, and a final generated-byte audit may change the
semantic set only after this design, both ranges, and every affected hash are
updated and re-reviewed.

Each shard's `cases` is nonempty and fully expanded: ranges, regular
expressions, implicit Cartesian products, and "all enum values" instructions
are invalid artifact bytes. Concatenation in descriptor order has global
sequences exactly `0..len(concatenated_cases)-1`, globally unique case IDs, exact descriptor
ranges/counts, and exact canonical equality to the production enumerator's
ordered `(case_id, dispatch_kind, action_kind, boundary, payload_kind)` code
tuple. The index contains no fixture references, payload bytes, result counts,
or result hashes.

The index, both shards, both fixture inputs, both result files, and the golden
manifest are packaged canonical JSON artifacts, each independently at most
65,536 bytes including exactly one final LF. Their loader opens the fixed
package root once and descriptor-reads each literal descendant without a
name-based reopen; it rejects absolute paths, `..`, symlinks, non-regular
files, duplicate descriptors, path substitution, byte/hash mismatch,
noncanonical JSON/LF bytes, changed descriptor identity, and unreferenced shard
files. Validation never trusts a path supplied by an artifact outside the two
closed literal sets.

The separate CI-only golden manifest is exactly
`sky/serve/resource_action_artifacts/provider_authority_v2/representability_goldens.json`.
It is generated only after the final V2 artifact-inventory hash exists and is
not referenced by the cohort static manifest, artifact inventory, callable
inventory, case index/shards, capsule, or preflight request. Its
`artifact_inventory` reference must equal the final V2 inventory and its
`case_inventory` reference must equal that inventory's sixth role.
The `realistic` input path is exactly
`sky/serve/resource_action_artifacts/provider_authority_v2/representability/realistic.json`;
the `candidate_maximal` input path is the same directory's
`candidate_maximal.json`. Their result paths are exactly the same directory's
`realistic.results.json` and `candidate_maximal.results.json`. Each result file
binds its matching fixture name/mode and final case-index reference and contains
exactly one result for every globally concatenated case in sequence order and
no other result. Those inputs and results are CI-only, not runtime authority.
Consequently the graph is acyclic: V2 artifact inventory -> case index -> two
shards; later golden manifest -> final artifact inventory/case index plus the
two fixture/result pairs. No cohort-bound artifact points back to the goldens.

The projector input is not an open mapping. The production module defines this
closed boundary-discriminated union:

```text
ProviderResourceActionRepresentabilityConstructionV2 =
  {action_kind: "launch",
   renderer_input: ProviderKubernetesRendererInputV2,
   execution_capsule: ProviderKubernetesExecutionCapsuleV2} |
  {action_kind: "down",
   capsule_input: ProviderKubernetesDownExecutionCapsuleInputV2,
   cleanup_rederivation_input:
       ProviderKubernetesCompletedCleanupRederivationInputV2 |
       ProviderKubernetesPartialCleanupRederivationInputV2 |
       ProviderKubernetesShadowPartialCleanupRederivationInputV2,
   rederived_cleanup_target: ProviderKubernetesCleanupTargetV1,
   execution_capsule: ProviderKubernetesDownExecutionCapsuleV2}

ProviderResourceActionPreflightRepresentabilityInputV2 = {
  version: 2,
  boundary: "complete_preflight",
  dispatch_kind: "authoritative_action" | "shadow_candidate",
  action_kind: "launch" | "down",
  request: ProviderAuthorityPreflightRequestV2,
  candidate_complete_response: ProviderAuthorityPreflightResponseV2,
  construction: ProviderResourceActionRepresentabilityConstructionV2,
  accepted_memberships: [ProviderAuthorityWorkerAcceptedExecutionMembershipV2]
}

ProviderResourceActionRequestTerminalSnapshotV2 = {
  request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
  request_finished_at: UtcTimestamp,
  active_claim: false,
  request_execution_generation: 0 | 1,
  request_worker_id: null,
  handler_name: "serve_resource_action_launch" |
                "serve_resource_action_down",
  request_return: null | ServeReplicaActionRequestReturnV1,
  request_return_sha256: null | Sha256
}

# Cross-field rules: terminalization clears the complete API007-defined claim triple under API008,
# so request_worker_id is null even when generation is positive. The immutable
# selector instead captures the exact pre-update claim worker. A successful
# authoritative handler return requires that nonnull selector worker and its
# request/generation/worker attestation to equal the return.

ProviderResourceActionAttemptTerminalAuthoritySelectorV2 = {
  version: 2,
  action_id: UUID,
  attempt: PositiveInteger,
  request_id: UUID,
  request_input_sha256: Sha256,
  request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
  request_execution_generation: 0 | 1,
  authority_worker_instance_id: null | UUID,
  worker_instance_id: null | UUID,  # process-unique API claim owner
  handler_name: "serve_resource_action_launch" |
                "serve_resource_action_down",
  authority_disposition: "NO_SUCCESSFUL_CLAIM_START" | "LINEAGE",
  lineage_generation: null | PositiveInteger,
  terminal_cause: "HANDLER_RETURN" | "REQUEST_FAILED" |
                  "REQUEST_CANCELLED" |
                  "CLAIM_START_NOT_REPRESENTABLE" |
                  "CLAIM_REAUTHORIZATION_FAILED" |
                  "TERMINAL_BEFORE_CLAIM_START",
  request_finished_at: UtcTimestamp
}

# The parent table is exhaustive: HANDLER_RETURN/LINEAGE/SUCCEEDED;
# REQUEST_FAILED/LINEAGE/FAILED; REQUEST_CANCELLED/LINEAGE/CANCELLED;
# CLAIM_START_NOT_REPRESENTABLE/NO_SUCCESSFUL_CLAIM_START/FAILED; or
# CLAIM_REAUTHORIZATION_FAILED/LINEAGE/FAILED; or
# TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START/(FAILED|CANCELLED).
# No other state/disposition/cause tuple parses.

ProviderResourceActionReducerAttemptSnapshotV2 = {
  action_id: UUID,
  attempt: PositiveInteger,
  request_id: UUID,
  request_terminal_snapshot:
      null | ProviderResourceActionRequestTerminalSnapshotV2,
  terminal_authority_selector:
      null | ProviderResourceActionAttemptTerminalAuthoritySelectorV2,
  request_input_sha256: Sha256,
  mutation_boundary: "NOT_STARTED" | "INTENT_COMMITTED" |
                     "SUBMITTED_OR_AMBIGUOUS" | "SETTLED",
  provider_io_boundary: "NOT_STARTED" | "INTENT_COMMITTED" |
                        "SUBMITTED_OR_AMBIGUOUS",
  provider_progress_revision: NonnegativeInteger,
  provider_progress: null | ProviderLifecycleProgressV1,
  provider_progress_sha256: null | Sha256,
  provider_operation_id: null | Text,
  typed_outcome: null | ServeReplicaActionOutcomeV1,
  typed_outcome_sha256: null | Sha256,
  settled_at: null | UtcTimestamp,
  historical_authority: [  # 0..RESOURCE_ACTION_ATTEMPT_AUTHORITY_KEYS_MAX_V2
    ProviderResourceActionExecutionAuthorityLineageV2
  ]
}

ProviderResourceActionReducerHistoryProjectionV2 = {
  version: 2,
  action_id: UUID,
  action_kind: "launch" | "down",
  action_revision: NonnegativeInteger,
  action_current_attempt: NonnegativeInteger,
  action_last_result: null | ServeReplicaActionOutcomeV1,
  action_last_result_sha256: null | Sha256,
  locked_predecessor: null | ProviderResourceActionReducerAttemptSnapshotV2,
  locked_current: null | ProviderResourceActionReducerAttemptSnapshotV2,
  launch_no_io_prefix: null | ServeLaunchNoIoPrefixV1,
  supersession_quiescence:
      null | ProviderLaunchSupersessionQuiescenceV1
}

ProviderResourceActionPreMaterializationActionSnapshotV2 = {
  action_id: UUID,
  domain: "serve",
  resource_type: "replica",
  resource_identity: ResourceActionIdentityV1,
  desired_generation: PositiveInteger,
  action_kind: "launch" | "down",
  immutable_spec: ServeReplicaActionSpecV2,
  immutable_spec_sha256: Sha256,
  kernel_state: "READY",
  current_attempt: NonnegativeInteger,
  next_attempt_at: UtcTimestamp,
  last_result: null | ServeReplicaActionOutcomeV1,
  last_result_sha256: null | Sha256,
  terminal_disposition: null,
  terminal_at: null,
  revision: PositiveInteger
}

ProviderResourceActionClaimHandoffFenceV2 = {
  version: 2,
  cohort_id: Text,
  cohort_revision: PositiveInteger,
  registration_set_revision: PositiveInteger,
  nonterminal_handoff_id: null,
  completed_cold_recovery_id: null | UUID,
  checked_at: UtcTimestamp
}

ProviderResourceActionReferenceSnapshotV2 = {
  version: 2,
  decision_id: UUID,
  cohort_id: Text,
  service_hash: UUID,
  replica_incarnation: UUID,
  desired_generation: PositiveInteger,
  action_kind: "launch" | "down",
  controller_owner_fence: Text,
  lifecycle_epoch: PositiveInteger,
  preparation_capability_sha256: Sha256,
  reference_state: "ACTION_ACTIVE",
  revision: PositiveInteger
}

ProviderShadowCandidateReferenceSnapshotV2 = {
  version: 2,
  decision_id: UUID,
  cohort_id: Text,
  service_hash: UUID,
  replica_incarnation: UUID,
  desired_generation: PositiveInteger,
  action_kind: "launch" | "down",
  controller_owner_fence: Text,
  lifecycle_epoch: PositiveInteger,
  preparation_capability_sha256: Sha256,
  reference_state: "SHADOW_ACTIVE",
  revision: PositiveInteger,
  created_at: UtcTimestamp,
  bound_at: UtcTimestamp,
  released_at: null
}

ProviderResourceActionApiInstanceSnapshotV2 = {
  version: 2,
  instance_id: UUID,  # process-unique claim owner; not the Pod UID
  authority_worker_instance_id: UUID,
  role: "authority-worker",
  pod_name: Text,
  pod_uid: UUID,
  pod_ip: Text,
  server_version: Text,
  started_at: UtcTimestamp,
  heartbeat_at: UtcTimestamp,
  draining_at: null,
  ready: true,
  health_detail: {
    phase: "authority-ready-v2",
    boot_nonce: UUID,
    authority_worker_instance_id: UUID,
    execution_owner_sha256: Sha256,
    pool_generation: PositiveInteger
  },
  supported_handlers: [
    "serve_resource_action_down", "serve_resource_action_launch",
    "serve_shadow_candidate_down", "serve_shadow_candidate_launch"
  ],
  supported_payload_versions:
      ProviderResourceActionPrivatePayloadVersionInventoryV1
}

ProviderResourceActionApiInstanceBootstrapSnapshotV2 = {
  version: 2,
  instance_id: UUID,
  authority_worker_instance_id: UUID,
  role: "authority-worker",
  pod_name: Text,
  pod_uid: UUID,
  pod_ip: Text,
  server_version: Text,
  started_at: UtcTimestamp,
  heartbeat_at: UtcTimestamp,
  draining_at: null,
  ready: false,
  health_detail: {
    phase: "authority-bootstrap-v2",
    boot_nonce: UUID,
    authority_worker_instance_id: UUID,
    execution_owner_sha256: null,
    pool_generation: 0
  },
  supported_handlers: [
    "serve_resource_action_down", "serve_resource_action_launch",
    "serve_shadow_candidate_down", "serve_shadow_candidate_launch"
  ],
  supported_payload_versions:
      ProviderResourceActionPrivatePayloadVersionInventoryV1
}

The V2 authority API health machine is closed. The only `(phase, ready,
draining_at, execution_owner_sha256)` shapes are bootstrap
`("authority-bootstrap-v2", false, null, null)`, bound-but-unwarmed
`("authority-bound-v2", false, null, Sha256)`, ready
`("authority-ready-v2", true, null, Sha256)`, transient rewarming
`("authority-rewarming-v2", false, null, Sha256)`, and draining
`("authority-draining-v2", false, UtcTimestamp, Sha256)`. The instance/stable
IDs, Pod name/UID/IP, role, version, stored `started_at`, boot nonce, handlers,
and payload inventory are immutable. Insert/exact-adopt creates bootstrap;
BIND/SUPERSEDE atomically changes bootstrap to bound with the installed owner
hash and `pool_generation=1`; completed initial eager warming changes bound to
ready without changing that generation. A typed current-owner pool-failure CAS
changes ready to rewarming while incrementing `pool_generation` exactly one; a
successful full-pool quiesce/rebuild/warm CAS changes that same generation back
to ready. Permanent withdrawal changes bound, ready, or rewarming to draining
without changing the generation. Heartbeat may only advance `heartbeat_at` while repeating
the current shape, and every row requires
`started_at <= heartbeat_at < clock_timestamp() + interval '1 second'` plus
freshness `clock_timestamp() < heartbeat_at + interval '20 seconds'` at an
authority commit gate. No missing-row recreation, ready/draining recovery edge,
or other phase/generation edge is legal. Every current-owner API validation
requires the exact phase matrix below and, in bound/ready/rewarming/draining,
`health_detail.execution_owner_sha256 == lease.execution_owner_sha256`, plus
the scalar/JSON/API process and stable-Pod/start equalities.

The phase matrix is exact. Initial activation reads bound rows. Normal same-owner
lease RENEW accepts bound, ready, rewarming, or draining. A new claim, claim
renewal, claim-start, progress/pre-I/O CAS, provider effect, or handler return
requires ready. Handoff OPEN requires a ready survivor and a bootstrap candidate;
survivor acknowledgement requires ready; completion requires that same ready
survivor plus the bound candidate. Cold recovery reads either historical owner-
bound phase for old rows and changes both candidates bootstrap to bound. Initial
or replacement warming is bound to ready; pool recovery alone is ready to
rewarming to ready. Same-owner or typed UID/process terminal closure may validate
the exact historical bound/ready/rewarming/draining owner phase, while revocation
or removal accepts those same owner-bound phases. GC accepts any unready phase
only after its independent stale-and-rootless program. Bootstrap never renews,
claims, acknowledges, terminalizes a generation-one request as its owner, or
revokes an owned lease. A typed supersession/cold-recovery transaction may lock
a bootstrap candidate while closing only the prior owners' requests before the
candidate becomes bound.

Pool recovery is a fenced current-process protocol, not a health toggle. Initial
bound warm failures remain bound and claimless. A failure after ready locks
cohort -> handoff -> current lease -> API row and commits ready-to-rewarming with
generation + 1 before killing/joining the full pool. Claims and effects use that
same prefix and require ready. The supervisor closes every already-committed
current-owner request through its exact post-claim-failure or pending-intent
owner-ack path, rebuilds and proves the fixed distinct-child set, then a second
prefix transaction requires zero active rows owned by the process and commits
rewarming-to-ready at the same generation. Lost acknowledgement adopts only the
exact phase/generation/owner bytes. Failure stays unready or exits the container;
draining never returns.

ProviderResourceActionPrivatePayloadVersionInventoryV1 = {
  "pydantic-json": {minimum: 1, maximum: 1}
}

ProviderResourceActionRequestClaimSnapshotV2 = {
  version: 2,
  request_id: UUID,
  status: "RUNNING",
  request_execution_generation: 1,
  authority_worker_instance_id: UUID,
  worker_instance_id: UUID,  # equals api_instance.instance_id
  claim_token_sha256: Sha256,
  controller_generation: null,
  lease_expires_at: UtcTimestamp,
  heartbeat_at: UtcTimestamp,
  cancel_requested_at: null,
  cancel_acknowledged_at: null,
  delivery_state: "claimed",
  claim_generation: 1,
  queue_priority: 0
}

ProviderResourceActionDispatchMembershipV2 = {
  version: 2,
  registration_set: ProviderAuthorityWorkerRegistrationSetV2,
  accepted_membership: ProviderAuthorityWorkerAcceptedExecutionMembershipV2,
  handoff_fence: ProviderResourceActionClaimHandoffFenceV2,
  reference: ProviderResourceActionReferenceSnapshotV2,
  api_instance: ProviderResourceActionApiInstanceSnapshotV2,
  request_claim: ProviderResourceActionRequestClaimSnapshotV2
}

ProviderShadowCandidateDispatchMembershipV2 = {
  version: 2,
  registration_set: ProviderAuthorityWorkerRegistrationSetV2,
  accepted_membership: ProviderAuthorityWorkerAcceptedExecutionMembershipV2,
  handoff_fence: ProviderResourceActionClaimHandoffFenceV2,
  reference: ProviderShadowCandidateReferenceSnapshotV2,
  api_instance: ProviderResourceActionApiInstanceSnapshotV2,
  request_claim: ProviderResourceActionRequestClaimSnapshotV2
}

ProviderExecutionAuthorityProofV2 = {
  version: 2,
  schema_heads: AuthoritySchemaHeadsV2,
  service_hash: UUID,
  policy_epoch: UUID,
  policy_sha256: Sha256,
  authority_binding_sha256: Sha256,
  policy_admission_state: "OPEN" | "DRAINING",
  policy_admission_revision: PositiveInteger,
  action_id: UUID,
  action_kind: "launch" | "down",
  immutable_spec_sha256: Sha256,
  resolved_cohort: ProviderAuthorityWorkerCohortV2,
  registration_set_sha256: Sha256,
  cohort_id: Text,
  deployment_uid: Text,
  reference_revision: PositiveInteger,
  api_instance_started_at: UtcTimestamp,
  api_instance_heartbeat_at: UtcTimestamp,
  preflight_request_sha256: Sha256,
  preflight_response_sha256: Sha256,
  representability_case_inventory_sha256: Sha256
}

ProviderShadowExecutionAuthorityProofV2 = {
  version: 2,
  schema_heads: AuthoritySchemaHeadsV2,
  service_hash: UUID,
  candidate_since: UtcTimestamp,
  decision_id: UUID,
  request_sequence: PositiveInteger,
  logical_attempt: PositiveInteger,
  request_role: "PRIMARY_LAUNCH" | "PRIMARY_DOWN",
  action_kind: "launch" | "down",
  immutable_spec_sha256: Sha256,
  invocation_sha256: Sha256,
  resolved_cohort: ProviderAuthorityWorkerCohortV2,
  registration_set_sha256: Sha256,
  cohort_id: Text,
  deployment_uid: Text,
  reference_revision: PositiveInteger,
  api_instance_started_at: UtcTimestamp,
  api_instance_heartbeat_at: UtcTimestamp,
  preflight_request_sha256: Sha256,
  preflight_response_sha256: Sha256,
  representability_case_inventory_sha256: Sha256
}

ProviderResourceActionExecutionAuthorityLineageV2 = {
  version: 2,
  action_id: UUID,
  attempt: PositiveInteger,
  request_id: UUID,
  request_input_sha256: Sha256,
  request_execution_generation: 1,
  authority_worker_instance_id: UUID,
  worker_instance_id: UUID,  # process-unique API claim owner
  claim_token_sha256: Sha256,
  controller_generation: null,
  service_hash: UUID,
  policy_epoch: UUID,
  policy_sha256: Sha256,
  authority_binding_sha256: Sha256,
  policy_admission_state: "OPEN" | "DRAINING",
  policy_admission_revision: PositiveInteger,
  cohort_id: Text,
  cohort_revision: PositiveInteger,
  registration_set_revision: PositiveInteger,
  worker_lease_revision: PositiveInteger,
  reference_revision: PositiveInteger,
  api_instance_started_at: UtcTimestamp,
  api_instance_heartbeat_at: UtcTimestamp,
  dispatch_membership: ProviderResourceActionDispatchMembershipV2,
  dispatch_membership_sha256: Sha256,
  execution_authority: ProviderExecutionAuthorityProofV2,
  execution_authority_sha256: Sha256,
  authorized_at: UtcTimestamp
}

The shadow membership/proof pair is a closed action-free sibling, not a
nullable action proof. Its reference is exactly `SHADOW_ACTIVE`; its decision,
sequence, logical attempt, primary role, kind, spec/invocation, cohort,
Deployment, API process/stable owner, request claim, and fresh preflight bytes
cross-equal the locked parent/child/history/request graph. It contains no policy
epoch, action attempt, API006 progress, or cleanup-child union member. The
canonical shadow execution-lineage hash is computed over exactly
`{version:1, decision_id, request_sequence, request_id,
request_input_sha256, request_execution_generation,
authority_worker_instance_id, worker_instance_id, claim_token_sha256,
dispatch_membership_sha256, execution_authority_sha256, authorized_at}` in that
field order. It is historical identity, not a row or bearer capability, and is
stored only after both independently bounded proof/hash pairs validate.

`OPEN` is the sole admission state for creating a new authoritative reference/
action root. A private-shadow decision carries no authority-policy tuple and is
instead admitted only while the locked service remains `shadow` under the exact
current candidate epoch/binding and accepted cohort activation proof. `OPEN |
DRAINING` is the closed current-execution union only for an action and
`ACTION_ACTIVE` reference already byte-bound to that exact policy before its
admission-state CAS. Such bound work may create its deterministic current-
attempt request as continuation history--including attempt one after an action
was admitted but before any request existed--then claim, claim-start, checkpoint, perform
provider I/O, and return so rotation can drain. That retry materialization must
preserve the existing action/reference/policy binding and may not create a new
authoritative reference/action or independent request root. The proof
records the current state/revision. `CLOSED | SUPERSEDED` authorizes no current
execution, and no `DRAINING` proof can admit a new authority root.

Every V2 claim-token hash in the request snapshot, lineage, selector context,
handoff fence, or cold-recovery fence is exactly lowercase
`SHA256(canonical-lowercase-UUID-text encoded as UTF-8)`. Hashing UUID binary,
canonical JSON, uppercase text, or any alternate token spelling is invalid.
The authority-worker Helm template injects nonempty `POD_IP` from the
`status.podIP` downward-API field alongside Pod name/namespace/UID; the API
instance writer persists that exact canonical IP. Before publishing request
readiness it uses a fresh random `SKYPILOT_API_SERVER_INSTANCE_ID` for this
Python/container start, binds it to the stable Pod-UID authority worker through
the exact lease V2 execution owner, and writes exactly `ready=true`, null
`draining_at`, and the closed health detail above. Reusing the Pod UID as the
API instance ID is invalid. The claimed-execution reader
requires those exact values; the historical `preflight-only` shape remains
bootstrap-only and cannot satisfy V2 dispatch.

ProviderResourceActionAdmissionRepresentabilityInputV2 = {
  version: 2,
  boundary: "linked_admission",
  dispatch_kind: "authoritative_action",
  action_kind: "launch" | "down",
  request: ProviderAuthorityPreflightRequestV2,
  complete_response: ProviderAuthorityPreflightResponseV2,
  locked_action: ProviderResourceActionPreMaterializationActionSnapshotV2,
  registration_set: ProviderAuthorityWorkerRegistrationSetV2,
  handoff_fence: ProviderResourceActionClaimHandoffFenceV2,
  accepted_memberships: [ProviderAuthorityWorkerAcceptedExecutionMembershipV2],
  accepted_api_instances: [ProviderResourceActionApiInstanceSnapshotV2],
      # exactly two, index-aligned with accepted_memberships
  database_now: UtcTimestamp,
  next_attempt: PositiveInteger,
  deterministic_request_id: UUID,
  request_input: ResourceActionRequestInputV1,
  request_input_sha256: Sha256,
  reducer_history: ProviderResourceActionReducerHistoryProjectionV2
}

ProviderResourceActionPostMaterializationActionSnapshotV2 = {
  action_id: UUID,
  domain: "serve",
  resource_type: "replica",
  resource_identity: ResourceActionIdentityV1,
  desired_generation: PositiveInteger,
  action_kind: "launch" | "down",
  immutable_spec: ServeReplicaActionSpecV2,
  immutable_spec_sha256: Sha256,
  kernel_state: "QUEUED",
  current_attempt: PositiveInteger,
  next_attempt_at: null,
  last_result: null | ServeReplicaActionOutcomeV1,
  last_result_sha256: null | Sha256,
  terminal_disposition: null,
  terminal_at: null,
  revision: PositiveInteger
}

ProviderResourceActionPostMaterializationProjectionV2 = {
  version: 2,
  action: ProviderResourceActionPostMaterializationActionSnapshotV2,
  attempt: ProviderResourceActionReducerAttemptSnapshotV2,
  request_input: ResourceActionRequestInputV1,
  request_input_sha256: Sha256,
  reducer_history: ProviderResourceActionReducerHistoryProjectionV2
}

ProviderResourceActionClaimedExecutionRepresentabilityInputV2 = {
  version: 2,
  boundary: "claimed_execution",
  dispatch_kind: "authoritative_action",
  action_kind: "launch" | "down",
  action_id: UUID,
  stored_spec: ServeReplicaActionSpecV2,
  resolved_cohort: ProviderAuthorityWorkerCohortV2,
  accepted_membership: ProviderAuthorityWorkerAcceptedExecutionMembershipV2,
  attempt: PositiveInteger,
  request_id: UUID,
  request_execution_generation: 1,
  current_progress: null | ProviderLifecycleProgressV1,
  worker_attestation: ProviderAuthorityWorkerAttemptAttestationV1,
  database_now: UtcTimestamp,
  lineage_disposition: "candidate_insert" | "stored_adoption",
  execution_authority_lineage:
      ProviderResourceActionExecutionAuthorityLineageV2,
  reducer_history: ProviderResourceActionReducerHistoryProjectionV2
}

ProviderResourceActionPreIoRepresentabilityInputV2 = {
  version: 2,
  boundary: "pre_io",
  dispatch_kind: "authoritative_action",
  action_kind: "launch" | "down",
  action_id: UUID,
  stored_spec: ServeReplicaActionSpecV2,
  resolved_cohort: ProviderAuthorityWorkerCohortV2,
  accepted_membership: ProviderAuthorityWorkerAcceptedExecutionMembershipV2,
  attempt: PositiveInteger,
  request_id: UUID,
  request_execution_generation: 1,
  current_progress: null | ProviderLifecycleProgressV1,
  worker_attestation: ProviderAuthorityWorkerAttemptAttestationV1,
  execution_authority_lineage:
      ProviderResourceActionExecutionAuthorityLineageV2,
  reducer_history: ProviderResourceActionReducerHistoryProjectionV2
}

ProviderShadowLinkedAdmissionPermanentFailureV2 = {
  failure_kind: "unbounded" | "oversized" | "unsupported",
  case_sequence: NonnegativeInteger,
  case_id: Text,
  payload_kind: Text,
  mode: "current" | "candidate_maximal",
  canonical_byte_count: null | PositiveInteger
}

ProviderShadowLinkedAdmissionFallbackCommitmentV1 = {
  version: 1,
  operation_id: UUID,
  decision_id: UUID,
  deterministic_request_id: UUID,
  initial_admission_source_sha256: Sha256,
  production_failure: ProviderShadowLinkedAdmissionPermanentFailureV2,
  production_failure_sha256: Sha256,
  committed_at: UtcTimestamp
}

ProviderShadowLinkedAdmissionFallbackReceiptV1 = {
  commitment: ProviderShadowLinkedAdmissionFallbackCommitmentV1,
  commitment_sha256: Sha256
}

ProviderShadowLinkedAdmissionFallbackProgressCommitmentV1 = one of:
  {version: 1,
   progress_kind: "LEGACY_PRE_SUBMIT",
   decision_id: UUID,
   fallback_operation_id: UUID,
   progress_operation_id: UUID,
   fallback_commitment_sha256: Sha256,
   first_request_sequence: 1,
   first_child_invocation_sha256: Sha256,
   first_child_admitted_at: UtcTimestamp,
   parent_revision_at_progress: PositiveInteger,
   reference_revision_at_progress: PositiveInteger,
   progressed_at: UtcTimestamp}
  {version: 1,
   progress_kind: "TERMINAL_NO_CALL_RELEASE",
   decision_id: UUID,
   fallback_operation_id: UUID,
   progress_operation_id: UUID,
   fallback_commitment_sha256: Sha256,
   terminal_parent_phase: "ABANDONED_PRE_SUBMIT",
   capacity_release_operation_id: UUID,
   parent_revision_at_progress: PositiveInteger,
   released_reference_revision: PositiveInteger,
   progressed_at: UtcTimestamp}

ProviderShadowLinkedAdmissionFallbackProgressReceiptV1 = {
  commitment: ProviderShadowLinkedAdmissionFallbackProgressCommitmentV1,
  commitment_sha256: Sha256
}

# `initial_admission_source_sha256` names the complete canonical bytes of the
# exact `initial_candidate_insert` source, including its original database
# time, preflight, accepted memberships/API instances, and request input. The
# failure hash names the adjacent complete typed failure. This deliberately
# permanent, bounded commitment is the only information discarded by the
# PENDING_SELECTION/PREPARING -> LEGACY_CONTROLLER/SHADOW_ACTIVE transition;
# it permits an unknown-commit caller retaining the original root to distinguish
# exact adoption from a different legal winner after mutable authority rows
# advance. A hash without this typed commitment row is never adoption evidence.

ProviderShadowRepresentedParentSnapshotV2 = {
  version: 2,
  decision_id: UUID,
  service_name: Text,
  service_hash: UUID,
  service_incarnation: UUID,
  replica_id: NonnegativeInteger,
  replica_incarnation: UUID,
  desired_generation: PositiveInteger,
  action_kind: "launch" | "down",
  resource_identity: ResourceActionIdentityV1,
  immutable_spec: ServeReplicaActionSpecV2,
  immutable_spec_sha256: Sha256,
  provider_plan: ProviderLifecyclePlanV2,
  provider_plan_sha256: Sha256,
  profile_eligibility: "ELIGIBLE",
  execution_route: "PENDING_SELECTION" | "LEGACY_CONTROLLER" |
                   "PRIVATE_API_REQUEST",
  private_fallback_reason: null | "linked_admission_not_representable",
  private_fallback_evidence:
      null | ProviderShadowLinkedAdmissionFallbackCommitmentV1,
  private_fallback_evidence_sha256: null | Sha256,
  phase: "PENDING" | "RUNNING",
  legacy_projection: null,
  legacy_projection_sha256: null,
  proposed_projection: null,
  proposed_projection_sha256: null,
  parity_class: "PENDING",
  revision: PositiveInteger,
  created_at: UtcTimestamp,
  updated_at: UtcTimestamp,
  completed_at: null
}

ProviderShadowCompletedParentProjectionV2 = {
  version: 2,
  decision_id: UUID,
  service_name: Text,
  service_hash: UUID,
  service_incarnation: UUID,
  replica_id: NonnegativeInteger,
  replica_incarnation: UUID,
  desired_generation: PositiveInteger,
  action_kind: "launch" | "down",
  resource_identity: ResourceActionIdentityV1,
  immutable_spec: ServeReplicaActionSpecV2,
  immutable_spec_sha256: Sha256,
  provider_plan: ProviderLifecyclePlanV2,
  provider_plan_sha256: Sha256,
  profile_eligibility: "ELIGIBLE",
  execution_route: "LEGACY_CONTROLLER" | "PRIVATE_API_REQUEST",
  private_fallback_reason: null | "linked_admission_not_representable",
  private_fallback_evidence:
      null | ProviderShadowLinkedAdmissionFallbackCommitmentV1,
  private_fallback_evidence_sha256: null | Sha256,
  phase: "COMPLETE",
  legacy_projection: ServeShadowProjectionV1,
  legacy_projection_sha256: Sha256,
  proposed_projection: ServeShadowProjectionV1,
  proposed_projection_sha256: Sha256,
  parity_class: "MATCH" | "IDENTITY_MISMATCH" |
                "PLACEMENT_MISMATCH" | "SUBMISSION_CERTAINTY_MISMATCH" |
                "OPERATION_ID_MISMATCH" | "RETRY_MISMATCH" |
                "OBSERVATION_MISMATCH" | "TERMINAL_MISMATCH" |
                "UNSUPPORTED_PROVIDER_PROFILE",
  revision: PositiveInteger,
  created_at: UtcTimestamp,
  updated_at: UtcTimestamp,
  completed_at: UtcTimestamp
}

ProviderShadowParentSnapshotV2 =
    ProviderShadowRepresentedParentSnapshotV2 |
    ProviderShadowCompletedParentProjectionV2

`PENDING_SELECTION` is legal only on a `PENDING` parent with all three fallback
members null and no descendant. Successful initial linked admission atomically changes
it to `PRIVATE_API_REQUEST/RUNNING`; permanent pre-write representability
fallback atomically changes it to
`LEGACY_CONTROLLER/RUNNING/linked_admission_not_representable`. Ordinary legacy
parents use `LEGACY_CONTROLLER` with all three fallback members null. A fallback
legacy parent requires the nonnull reason plus one hash-valid typed commitment;
`PRIVATE_API_REQUEST` requires all three null. `PRIVATE_API_REQUEST` and
`LEGACY_CONTROLLER` cross-require private-history and legacy child shapes,
respectively; no transition between them is legal after the first child exists.

ProviderShadowRawExecutionHistorySnapshotV2 =
    the exact flat storage projection of ProviderShadowExecutionHistoryV1 with
    `provider_progress` replaced by
    `provider_progress_raw: null | CanonicalJsonObject`. The outer row must
    still have valid identity, handler, request/input, the immutable nonnull
    `preflight_request`/`preflight_request_sha256` and
    `preflight_response`/`preflight_response_sha256` pairs, enum, counter,
    authority-bundle, strict provider-effect-trace/hash pair,
    terminal-receipt, return, settlement, timestamp, and
    per-column 65,536-byte/lowercase-hash shapes. It deliberately does not
    require the raw progress object to decode as
    ProviderShadowLifecycleProgressV1 or to agree with its declared hash,
    revision, watermark, or operation ID.

ProviderShadowJournalClassificationV2 = one of:
  {journal_class: "not_started_empty",
   typed_progress: null}
  {journal_class: "valid_nonterminal" | "valid_succeeded",
   typed_progress: ProviderShadowLifecycleProgressV1}
  {journal_class: "invalid",
   typed_progress: null}

The sole raw-shadow-journal classifier consumes the raw snapshot and produces
exactly this union. Execution admission, claim authorization, progress, pre-I/O,
and handler return accept only a strict ProviderShadowExecutionHistoryV1 and
therefore cannot run on `invalid`. Terminalization, settlement, settled replay,
retention, and GC accept the raw storage snapshot; `X` preserves the raw bytes
and declared hash/revision in place while storing only bounded fallback/outcome
children. An invalid outer identity/authority/settlement shape remains
quarantined corruption and is not the typed `invalid` journal case.

For an `X` settlement, `candidate_settled_history` byte-copies the raw progress
object, declared hash/revision, watermark, and operation ID and changes only the
independently derived terminal-receipt/settlement fields. That output is parsed
as `ProviderShadowRawExecutionHistorySnapshotV2`, never forced through the
strict progress codec and never normalized. Every other executable or settled
history must strict-decode. Once the classifier returns `invalid`, the exact
historical-origin source list is empty: raw fragments that resemble an origin
are never parsed or accepted as authority, and `X` blocks with no successor.

ProviderShadowBoundChildProjectionV2 = {
  version: 2,
  decision_id: UUID,
  request_sequence: PositiveInteger,
  logical_attempt: PositiveInteger,
  request_role: "PRIMARY_LAUNCH" | "PRIMARY_DOWN",
  action_kind: "launch" | "down",
  planned_execution_kind: "private_api_request",
  phase: "REQUEST_BOUND",
  request_id: UUID,
  invocation: ProviderLifecycleInvocationV2,
  invocation_sha256: Sha256,
  immutable_payload_sha256: Sha256,
  provider_operation_id: null,
  actual_outcome: null,
  actual_outcome_sha256: null,
  proposed_outcome: null,
  proposed_outcome_sha256: null,
  retry_decision: null,
  retry_decision_sha256: null,
  pre_observation: null,
  pre_observation_sha256: null,
  post_observation: null,
  post_observation_sha256: null,
  legacy_effect_trace: null,
  legacy_effect_trace_sha256: null,
  divergence_class: null,
  admitted_at: UtcTimestamp,
  request_bound_at: UtcTimestamp,
  completed_at: null,
  updated_at: UtcTimestamp
}

ProviderShadowCompletedChildProjectionV2 = {
  version: 2,
  decision_id: UUID,
  request_sequence: PositiveInteger,
  logical_attempt: PositiveInteger,
  request_role: "PRIMARY_LAUNCH" | "PRIMARY_DOWN",
  action_kind: "launch" | "down",
  planned_execution_kind: "private_api_request",
  phase: "COMPLETE",
  request_id: UUID,
  invocation: ProviderLifecycleInvocationV2,
  invocation_sha256: Sha256,
  immutable_payload_sha256: Sha256,
  provider_operation_id: null | Text,
  actual_outcome: ServeShadowCandidateOutcomeV1,
  actual_outcome_sha256: Sha256,
  proposed_outcome: ServeShadowCandidateOutcomeV1,
  proposed_outcome_sha256: Sha256,
  retry_decision: ServeShadowRetryDecisionV1,
  retry_decision_sha256: Sha256,
  pre_observation: null | ProviderLifecycleObservationV1,
  pre_observation_sha256: null | Sha256,
  post_observation: null | ProviderLifecycleObservationV1,
  post_observation_sha256: null | Sha256,
  legacy_effect_trace: LegacyProviderEffectTraceV1,
  legacy_effect_trace_sha256: Sha256,
  divergence_class: "MATCH" | "IDENTITY_MISMATCH" |
                    "PLACEMENT_MISMATCH" |
                    "SUBMISSION_CERTAINTY_MISMATCH" |
                    "OPERATION_ID_MISMATCH" | "RETRY_MISMATCH" |
                    "OBSERVATION_MISMATCH" | "TERMINAL_MISMATCH" |
                    "UNSUPPORTED_PROVIDER_PROFILE",
  admitted_at: UtcTimestamp,
  request_bound_at: UtcTimestamp,
  completed_at: UtcTimestamp,
  updated_at: UtcTimestamp
}

ProviderShadowChildSnapshotV2 = ProviderShadowBoundChildProjectionV2 |
                                ProviderShadowCompletedChildProjectionV2

PROVIDER_SHADOW_HISTORICAL_ORIGINS_MAX_V2 = 13  # 2 * 5 effects + 3

ProviderShadowSettlementReceiptV1 = {
  commitment: ProviderShadowSettlementCommitmentV1,
  commitment_sha256: Sha256
}

ProviderShadowHistoricalOriginSourceV2 = {
  origin: ProviderShadowLaunchEffectClaimV1,
  completed_child: ProviderShadowCompletedChildProjectionV2,
  settled_history: ProviderShadowExecutionHistoryV1,
      # strict SETTLED, never X/raw-invalid
  terminal_history: ProviderShadowRequestTerminalHistoryV2,
  settlement_receipt: ProviderShadowSettlementReceiptV1,
  retained_terminal_request: null | ProviderShadowRequestTerminalSnapshotV2
}

# Each boundary extracts every distinct *prior-request* shadow origin recursively
# reachable from its current cursor, handler return, no-effect/quiescence
# evidence, and immediate predecessor. An origin whose decision/sequence/
# generation equals the enclosing current request is validated directly against
# the current child/history/attestation and excluded from this list. The prior
# origins are sorted by the declared order and require this exact 0..13 list: no
# duplicate, missing, or extra source is legal. Every source
# point-loads and cross-validates the retained child/history/terminal receipt/
# settlement receipt. The settlement commitment must bind the same identity,
# terminal-history hash, settle time, and every locally present successor key.
# The store-owned same-transaction relational integrity scan validates the
# remaining original settlement component without embedding an unbounded or
# recursively expanding graph in this DTO. Request GC
# changes only the nullable request snapshot.

ProviderShadowCandidateServiceSnapshotV2 = {
  version: 2,
  service_name: Text,
  service_hash: UUID,
  service_incarnation: UUID,
  resource_action_mode: "shadow",
  candidate_since: UtcTimestamp,
  candidate_epoch: UUID,
  qualification_policy_sha256: Sha256,
  qualification_binding_sha256: Sha256,
  controller_owner_fence: Text,
  lifecycle_epoch: PositiveInteger
}

ProviderShadowPreparingReferenceSnapshotV2 =
    ProviderShadowCandidateReferenceSnapshotV2 with `reference_state`,
    `bound_at`, and `released_at` replaced by exactly {
  reference_state: "PREPARING",
  bound_at: null,
  released_at: null
}

ProviderShadowReleasedReferenceSnapshotV2 =
    ProviderShadowCandidateReferenceSnapshotV2 with `reference_state` and
    `released_at` replaced by exactly {
  reference_state: "RELEASED",
  released_at: UtcTimestamp
}

ProviderShadowCompletePreflightSourceV2 = {
  request: ProviderAuthorityPreflightRequestV2,
  complete_response: ProviderAuthorityPreflightResponseV2
}

ProviderShadowGenerationZeroRequestQueueSnapshotV2 = {
  request_id: UUID,
  request_input: ResourceActionRequestInputV1,
  request_input_sha256: Sha256,
  handler_name: "serve_shadow_candidate_launch" |
                "serve_shadow_candidate_down",
  private_route: ResourceActionPrivateRouteV1,
  request_status: "PENDING",
  request_execution_generation: 0,
  request_worker_id: null,
  claim_token_sha256: null,
  controller_generation: null,
  claim_lease_expires_at: null,
  heartbeat_at: null,
  cancel_requested_at: null,
  cancel_acknowledged_at: null,
  delivery_state: "queued",
  claim_generation: 0,
  queue_priority: 0,
  schedule_type: "long",
  replay_policy: "NEVER",
  retryable: false,
  ignore_return_value: false,
  created_at: UtcTimestamp,
  updated_at: UtcTimestamp
}

ProviderShadowAdmissionInsertionProjectionV2 = {
  version: 2,
  post_insert_parent: ProviderShadowRepresentedParentSnapshotV2,
      # exact PRIVATE_API_REQUEST/RUNNING descendant at insertion revision
  inserted_child: ProviderShadowBoundChildProjectionV2,
  inserted_history: ProviderShadowExecutionHistoryV1,
      # exact BOUND insertion bytes
  inserted_request: ProviderShadowGenerationZeroRequestQueueSnapshotV2,
  post_insert_reference: ProviderShadowCandidateReferenceSnapshotV2
}

ProviderShadowRetryPredecessorSourceV2 = {
  completed_child: ProviderShadowCompletedChildProjectionV2,
  settled_history: ProviderShadowRawExecutionHistorySnapshotV2,
  terminal_history: ProviderShadowRequestTerminalHistoryV2,
  settlement_receipt: ProviderShadowSettlementReceiptV1,
  retained_terminal_request: null | ProviderShadowRequestTerminalSnapshotV2,
  historical_shadow_origins: [ProviderShadowHistoricalOriginSourceV2]
}

ProviderShadowAdmissionRequestDescendantV2 = one of:
  {descendant_kind: "bound_queued",
   generation_zero: ProviderShadowGenerationZeroRequestQueueSnapshotV2,
   claim: null,
   terminal_request: null,
   terminal_history: null}
  {descendant_kind: "authorized_running",
   generation_zero: null,
   claim: ProviderResourceActionRequestClaimSnapshotV2,
   terminal_request: null,
   terminal_history: null}
  {descendant_kind: "terminal_request_present",
   generation_zero: null,
   claim: null,
   terminal_request: ProviderShadowRequestTerminalSnapshotV2,
   terminal_history: ProviderShadowRequestTerminalHistoryV2}
  {descendant_kind: "terminal_request_gced",
   generation_zero: null,
   claim: null,
   terminal_request: null,
   terminal_history: ProviderShadowRequestTerminalHistoryV2}

ProviderShadowAdmissionStoredDescendantV2 = {
  parent: ProviderShadowParentSnapshotV2,
  child: ProviderShadowChildSnapshotV2,
  history: ProviderShadowRawExecutionHistorySnapshotV2,
  settlement_receipt: null | ProviderShadowSettlementReceiptV1,
  reference: ProviderShadowCandidateReferenceSnapshotV2 |
             ProviderShadowReleasedReferenceSnapshotV2,
  request_descendant: ProviderShadowAdmissionRequestDescendantV2,
  historical_shadow_origins: [ProviderShadowHistoricalOriginSourceV2]
}

# `settlement_receipt` is nonnull exactly when `history.phase=SETTLED` and is
# null for BOUND/AUTHORIZED descendants. When nonnull it cross-binds the same
# decision/sequence, terminal-history hash, stored settle time, and every
# locally available successor key. A same-transaction store-owned relational
# scan validates the remaining settlement component; this bounded descendant
# does not attempt to embed or reconstruct an unbounded recursive graph.
# Missing or crossed permanent evidence is corruption; a later insertion/
# adoption cannot treat it as an earlier phase. The same rule applies to every
# settled retry-predecessor source above.

ProviderShadowAdmissionSourceV2 = one of:
  {admission_disposition: "initial_candidate_insert",
   candidate_service: ProviderShadowCandidateServiceSnapshotV2,
   reference_before: ProviderShadowPreparingReferenceSnapshotV2,
   locked_parent: ProviderShadowRepresentedParentSnapshotV2,
       # exact PENDING_SELECTION/PENDING parent
   predecessor: null,
   preflight: ProviderShadowCompletePreflightSourceV2,
   registration_set: ProviderAuthorityWorkerRegistrationSetV2,
   handoff_fence: ProviderResourceActionClaimHandoffFenceV2,
   accepted_memberships:
       [ProviderAuthorityWorkerAcceptedExecutionMembershipV2],
   accepted_api_instances: [ProviderResourceActionApiInstanceSnapshotV2],
       # exactly two and index-aligned
   database_now: UtcTimestamp,
   deterministic_request_id: UUID,
   request_input: ResourceActionRequestInputV1,
   request_input_sha256: Sha256}
  {admission_disposition: "retry_candidate_insert",
   candidate_service: ProviderShadowCandidateServiceSnapshotV2,
   reference_before: ProviderShadowCandidateReferenceSnapshotV2,
   locked_parent: ProviderShadowRepresentedParentSnapshotV2,
       # exact PRIVATE_API_REQUEST/RUNNING parent
   predecessor: ProviderShadowRetryPredecessorSourceV2,
   preflight: ProviderShadowCompletePreflightSourceV2,
   registration_set: ProviderAuthorityWorkerRegistrationSetV2,
   handoff_fence: ProviderResourceActionClaimHandoffFenceV2,
   accepted_memberships:
       [ProviderAuthorityWorkerAcceptedExecutionMembershipV2],
   accepted_api_instances: [ProviderResourceActionApiInstanceSnapshotV2],
       # exactly two and index-aligned
   database_now: UtcTimestamp,
   deterministic_request_id: UUID,
   request_input: ResourceActionRequestInputV1,
   request_input_sha256: Sha256}
  {admission_disposition: "stored_adoption",
   stored_descendant: ProviderShadowAdmissionStoredDescendantV2}

ProviderShadowAdmissionRepresentabilityInputV2 = {
  version: 2,
  boundary: "linked_admission",
  dispatch_kind: "shadow_candidate",
  action_kind: "launch" | "down",
  source: ProviderShadowAdmissionSourceV2,
  candidate_projection: ProviderShadowAdmissionInsertionProjectionV2
}

ProviderShadowInitialAdmissionSourceV2 =
    the exact `initial_candidate_insert` member of
    ProviderShadowAdmissionSourceV2

ProviderShadowLinkedAdmissionFallbackAbsenceV2 = {
  child: true,
  execution_history: true,
  private_correlation: true,
  deterministic_request: true,
  deterministic_queue: true,
  fallback_progress_receipt: true
}

ProviderShadowLegacyFallbackFirstAttemptDescendantV2 = {
  decision_id: UUID,
  request_sequence: 1,
  logical_attempt: 1,
  request_role: "PRIMARY_LAUNCH" | "PRIMARY_DOWN",
  action_kind: "launch" | "down",
  planned_execution_kind: "api_request" | "legacy_direct_down",
  phase: "PRE_SUBMIT" | "REQUEST_BOUND" | "COMPLETE" |
         "ABANDONED_PRE_SUBMIT" | "REQUEST_ASSOCIATION_UNKNOWN",
  legacy_request_id: null | Text,
  invocation: ServeShadowAttemptInvocationV1,
  invocation_sha256: Sha256,
  provider_operation_id: null | Text,
  actual_outcome: null | ServeReplicaActionOutcomeV1,
  actual_outcome_sha256: null | Sha256,
  proposed_outcome: null | ServeReplicaActionOutcomeV1,
  proposed_outcome_sha256: null | Sha256,
  retry_decision: null | ServeShadowRetryDecisionV1,
  retry_decision_sha256: null | Sha256,
  pre_observation: null | ProviderLifecycleObservationV1,
  pre_observation_sha256: null | Sha256,
  post_observation: null | ProviderLifecycleObservationV1,
  post_observation_sha256: null | Sha256,
  legacy_effect_trace: null | LegacyProviderEffectTraceV1,
  legacy_effect_trace_sha256: null | Sha256,
  divergence_class: null | "MATCH" | "IDENTITY_MISMATCH" |
                    "PLACEMENT_MISMATCH" |
                    "SUBMISSION_CERTAINTY_MISMATCH" |
                    "OPERATION_ID_MISMATCH" | "RETRY_MISMATCH" |
                    "OBSERVATION_MISMATCH" | "TERMINAL_MISMATCH" |
                    "UNSUPPORTED_PROVIDER_PROFILE",
  admitted_at: UtcTimestamp,
  request_bound_at: null | UtcTimestamp,
  completed_at: null | UtcTimestamp,
  updated_at: UtcTimestamp
}

# The first child is parent-kind-matched and obeys the complete durable legacy
# phase/nullability CHECK. It can never be a cleanup child, a private outcome
# codec, or a later logical attempt; those shapes cannot witness first progress.

ProviderShadowFallbackTerminalParentSnapshotV2 =
    the same immutable identity/spec/plan/fallback fields as
    ProviderShadowCompletedParentProjectionV2, with
    execution_route: "LEGACY_CONTROLLER",
    phase: "ABANDONED_PRE_SUBMIT" | "AMBIGUOUS",
    legacy_projection/proposed_projection and their hashes either both complete
        or both null according to the durable parent CHECK,
    parity_class: "ABANDONED" | "AMBIGUOUS",
    completed_at: UtcTimestamp

ProviderShadowFallbackParentDescendantSnapshotV2 =
    ProviderShadowParentSnapshotV2 |
    ProviderShadowFallbackTerminalParentSnapshotV2

ProviderShadowLinkedAdmissionPrivateDescendantAbsenceV2 = {
  execution_history: true,
  private_correlation: true,
  deterministic_request: true,
  deterministic_queue: true
}

ProviderShadowLinkedAdmissionFallbackGcAbsenceV2 = {
  decision_id: UUID,
  service_name: Text,
  service_hash: UUID,
  service_incarnation: UUID,
  replica_id: NonnegativeInteger,
  replica_incarnation: UUID,
  desired_generation: PositiveInteger,
  coverage: true,
  parent: true,
  all_children: true,
  execution_history: true,
  private_correlation: true,
  deterministic_request: true,
  deterministic_queue: true,
  live_replica_links: true,
  cleanup_intent: true,
  reference:
      {reference_disposition: "released",
       released_reference: ProviderShadowReleasedReferenceSnapshotV2} |
      {reference_disposition: "absent", exact_reference_absent: true}
}

ProviderShadowLinkedAdmissionFallbackReceiptApplicabilityV2 = one of:
  {applicability: "retained_advanced_graph",
   progress_receipt:
       ProviderShadowLinkedAdmissionFallbackProgressReceiptV1,
   stored_parent: ProviderShadowFallbackParentDescendantSnapshotV2,
   stored_reference: ProviderShadowCandidateReferenceSnapshotV2 |
                     ProviderShadowReleasedReferenceSnapshotV2,
   first_legacy_attempt:
       null | ProviderShadowLegacyFallbackFirstAttemptDescendantV2,
   private_descendant_absence:
       ProviderShadowLinkedAdmissionPrivateDescendantAbsenceV2}
  {applicability: "typed_graph_gced",
   progress_receipt:
       ProviderShadowLinkedAdmissionFallbackProgressReceiptV1,
   graph_absence: ProviderShadowLinkedAdmissionFallbackGcAbsenceV2}

ProviderShadowLinkedAdmissionFallbackSourceV2 = one of:
  {fallback_disposition: "new_fallback",
   initial_admission_source: ProviderShadowInitialAdmissionSourceV2,
   operation_id: UUID,
   production_failure: ProviderShadowLinkedAdmissionPermanentFailureV2,
   locked_absence: ProviderShadowLinkedAdmissionFallbackAbsenceV2}
  {fallback_disposition: "stored_adoption",
   caller_initial_admission_source: ProviderShadowInitialAdmissionSourceV2,
   caller_operation_id: UUID,
   caller_production_failure:
       ProviderShadowLinkedAdmissionPermanentFailureV2,
   stored_parent: ProviderShadowRepresentedParentSnapshotV2,
       # exact original LEGACY_CONTROLLER/RUNNING fallback post-state
   stored_reference: ProviderShadowCandidateReferenceSnapshotV2,
       # exact original SHADOW_ACTIVE fallback post-state
   locked_absence: ProviderShadowLinkedAdmissionFallbackAbsenceV2,
   stored_receipt: ProviderShadowLinkedAdmissionFallbackReceiptV1}
  {fallback_disposition: "receipt_only_adoption",
   caller_initial_admission_source: ProviderShadowInitialAdmissionSourceV2,
   caller_operation_id: UUID,
   caller_production_failure:
       ProviderShadowLinkedAdmissionPermanentFailureV2,
   stored_receipt: ProviderShadowLinkedAdmissionFallbackReceiptV1,
   applicability:
       ProviderShadowLinkedAdmissionFallbackReceiptApplicabilityV2}

ProviderShadowLinkedAdmissionFallbackProjectionV2 = one of:
  {fallback_result: "NEWLY_COMMITTED" | "EXACT_ADOPTED_GRAPH",
   post_fallback_parent: ProviderShadowRepresentedParentSnapshotV2,
       # exact original LEGACY_CONTROLLER/RUNNING fallback projection
   post_fallback_reference: ProviderShadowCandidateReferenceSnapshotV2,
   fallback_receipt: ProviderShadowLinkedAdmissionFallbackReceiptV1}
  {fallback_result: "EXACT_ADOPTED_RECEIPT",
   post_fallback_parent: null,
   post_fallback_reference: null,
   fallback_receipt: ProviderShadowLinkedAdmissionFallbackReceiptV1}
  {fallback_result: "LOST_RACE",
   post_fallback_parent: null,
   post_fallback_reference: null,
   fallback_receipt: null}

ProviderShadowLinkedAdmissionFallbackInputV2 = {
  source: ProviderShadowLinkedAdmissionFallbackSourceV2,
  candidate_projection: ProviderShadowLinkedAdmissionFallbackProjectionV2
}

# For `new_fallback`, the validator reruns the complete enumerator and
# deterministic-first-failure selector against the initial source, requires the
# exact production failure and locked absences, and derives the permanent
# commitment, permanent receipt, and both state transitions. `committed_at` equals the source's
# database time; the operation/decision/request IDs and both adjacent canonical
# hashes must cross-equal. Retryable drift cannot inhabit this DTO. For
# graph `stored_adoption`, the validator hashes the caller's retained original
# source and failure, validates the stored parent's commitment, permanent
# receipt, exact original RUNNING/LEGACY_CONTROLLER parent and SHADOW_ACTIVE
# reference post-state, all-descendant absences, and progress-receipt absence,
# and returns
# `EXACT_ADOPTED_GRAPH` only on complete equality. That result still releases
# the decision-keyed idempotent same-cell signal; the first-child CAS prevents
# duplicate provider entry after an acknowledgement-lost signal. Once a legal legacy child,
# parent completion, reference release, or any other graph advancement occurs,
# graph adoption is no longer the right source. The store—not its caller—then
# selects `receipt_only_adoption` only after the permanent progress receipt
# cross-validates either the retained first transition descendant or the exact
# typed-GC absence proof. `LEGACY_PRE_SUBMIT` requires the named first child;
# `TERMINAL_NO_CALL_RELEASE` requires it null and the exact terminal parent /
# released-reference transition. The store validates later legacy rows through
# their typed relational readers without embedding an unbounded inventory in
# this DTO. Receipt-only adoption compares the same caller preimage directly
# with the permanent fallback receipt;
# it returns no parent/reference and never resurrects either. A different
# internally legal commitment is `LOST_RACE`; a malformed, hash-invalid,
# partial, or crossed stored state is corruption. Mutable cohort/API advancement
# after commit is irrelevant to adoption because it is not substituted for the retained root.
# Candidate projection fields are outputs, never evidence.

ProviderShadowAuthorizationSourceV2 = one of:
  {authorization_disposition: "candidate_authorization",
   history_before: ProviderShadowExecutionHistoryV1}
      # strict BOUND source; no authorization/projection fields are present
  {authorization_disposition: "stored_adoption",
   stored_authorized_history: ProviderShadowExecutionHistoryV1}
      # strict AUTHORIZED source written by an earlier candidate transaction

ProviderShadowAuthorizationProjectionV2 = {
  authorized_history: ProviderShadowExecutionHistoryV1
      # strict AUTHORIZED projection, including the builder-derived
      # dispatch membership, execution proof, lineage hash, and timestamp
}

ProviderShadowClaimedExecutionRepresentabilityInputV2 = {
  version: 2,
  boundary: "claimed_execution",
  dispatch_kind: "shadow_candidate",
  action_kind: "launch" | "down",
  candidate_service: ProviderShadowCandidateServiceSnapshotV2,
  reference: ProviderShadowCandidateReferenceSnapshotV2,
  stored_preflight: ProviderShadowCompletePreflightSourceV2,
  stored_parent: ProviderShadowRepresentedParentSnapshotV2,
  stored_child: ProviderShadowBoundChildProjectionV2,
  source: ProviderShadowAuthorizationSourceV2,
  resolved_cohort: ProviderAuthorityWorkerCohortV2,
  registration_set: ProviderAuthorityWorkerRegistrationSetV2,
  handoff_fence: ProviderResourceActionClaimHandoffFenceV2,
  accepted_membership: ProviderAuthorityWorkerAcceptedExecutionMembershipV2,
  api_instance: ProviderResourceActionApiInstanceSnapshotV2,
  request_claim: ProviderResourceActionRequestClaimSnapshotV2,
  current_progress: null | ProviderShadowLifecycleProgressV1,
  historical_shadow_origins: [ProviderShadowHistoricalOriginSourceV2],
  worker_attestation: ProviderAuthorityWorkerAttemptAttestationV1,
  database_now: UtcTimestamp,
  candidate_projection: ProviderShadowAuthorizationProjectionV2
}

ProviderShadowNextRepresentabilityBoundaryV2 = one of:
  {boundary_kind: "progress",
   next_progress: ProviderShadowLifecycleProgressV1,
   next_effect_trace: LegacyProviderEffectTraceV1,
   request_return: null}
  {boundary_kind: "handler_return",
   next_progress: null | ProviderShadowLifecycleProgressV1,
   next_effect_trace: LegacyProviderEffectTraceV1,
   request_return: ServeShadowCandidateRequestReturnV1}

ProviderShadowPreIoRepresentabilityInputV2 = {
  version: 2,
  boundary: "pre_io",
  dispatch_kind: "shadow_candidate",
  action_kind: "launch" | "down",
  candidate_service: ProviderShadowCandidateServiceSnapshotV2,
  reference: ProviderShadowCandidateReferenceSnapshotV2,
  stored_preflight: ProviderShadowCompletePreflightSourceV2,
  stored_parent: ProviderShadowRepresentedParentSnapshotV2,
  stored_child: ProviderShadowBoundChildProjectionV2,
  authorized_history: ProviderShadowExecutionHistoryV1,
  resolved_cohort: ProviderAuthorityWorkerCohortV2,
  registration_set: ProviderAuthorityWorkerRegistrationSetV2,
  handoff_fence: ProviderResourceActionClaimHandoffFenceV2,
  accepted_membership: ProviderAuthorityWorkerAcceptedExecutionMembershipV2,
  api_instance: ProviderResourceActionApiInstanceSnapshotV2,
  request_claim: ProviderResourceActionRequestClaimSnapshotV2,
  current_progress: null | ProviderShadowLifecycleProgressV1,
  historical_shadow_origins: [ProviderShadowHistoricalOriginSourceV2],
  worker_attestation: ProviderAuthorityWorkerAttemptAttestationV1,
  database_now: UtcTimestamp
}

ProviderShadowRequestTerminalSnapshotV2 = {
  request_id: UUID,
  request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
  request_finished_at: UtcTimestamp,
  request_execution_generation: 0 | 1,
  active_claim: false,
  request_worker_id: null,
  claim_token_sha256: null,
  controller_generation: null,
  claim_lease_expires_at: null,
  heartbeat_at: null,
  cancel_requested_at: null | UtcTimestamp,
  cancel_acknowledged_at: null | UtcTimestamp,
  handler_name: "serve_shadow_candidate_launch" |
                "serve_shadow_candidate_down",
  request_return: null | ServeShadowCandidateRequestReturnV1,
  request_return_sha256: null | Sha256
}

ProviderShadowGenerationOneTerminalRequestSourceV2 = {
  version: 2,
  request_id: UUID,
  status: "RUNNING",
  request_execution_generation: 1,
  authority_worker_instance_id: UUID,
  worker_instance_id: UUID,
  claim_token_sha256: Sha256,
  controller_generation: null,
  lease_expires_at: UtcTimestamp,
  heartbeat_at: UtcTimestamp,
  cancel_requested_at: null | UtcTimestamp,
  cancel_acknowledged_at: null,
  delivery_state: "claimed",
  claim_generation: 1,
  queue_priority: 0,
  schedule_type: "long",
  replay_policy: "NEVER",
  retryable: false,
  ignore_return_value: false,
  created_at: UtcTimestamp,
  updated_at: UtcTimestamp,
  request_input: ResourceActionRequestInputV1,
  request_input_sha256: Sha256,
  private_route: ResourceActionPrivateRouteV1
}

ProviderResourceActionGenerationOneTerminalRequestSourceV2 = {
  version: 2,
  request_id: UUID,
  status: "RUNNING",
  request_execution_generation: 1,
  authority_worker_instance_id: UUID,
  worker_instance_id: UUID,
  claim_token_sha256: Sha256,
  controller_generation: null,
  lease_expires_at: UtcTimestamp,
  heartbeat_at: UtcTimestamp,
  cancel_requested_at: null | UtcTimestamp,
  cancel_acknowledged_at: null,
  delivery_state: "claimed",
  claim_generation: 1,
  queue_priority: 0,
  schedule_type: "long",
  replay_policy: "NEVER",
  retryable: false,
  ignore_return_value: false,
  created_at: UtcTimestamp,
  updated_at: UtcTimestamp,
  request_input: ResourceActionRequestInputV1,
  request_input_sha256: Sha256,
  private_route: ResourceActionPrivateRouteV1
}

ProviderShadowAuthorityFenceClaimPreimageV2 = {
  request_before: ProviderShadowGenerationOneTerminalRequestSourceV2,
  history_before: ProviderShadowRawExecutionHistorySnapshotV2,
      # exact BOUND or AUTHORIZED; never SETTLED
  prior_lease_expires_at: UtcTimestamp
}

ProviderResourceActionAuthorityFenceClaimPreimageV2 = {
  request_before: ProviderResourceActionGenerationOneTerminalRequestSourceV2,
  action_id: UUID,
  attempt: PositiveInteger,
  handler_name: "serve_resource_action_launch" |
                "serve_resource_action_down",
  request_input: ResourceActionRequestInputV1,
  request_input_sha256: Sha256,
  locked_attempt: ProviderResourceActionRawAttemptSnapshotV2,
  prior_cancel_requested_at: null | UtcTimestamp,
  prior_lease_expires_at: UtcTimestamp
}

ProviderAuthorityFenceClaimPreimageV2 = one of:
  {claim_kind: "resource_action",
   action_claim: ProviderResourceActionAuthorityFenceClaimPreimageV2,
   shadow_claim: null}
  {claim_kind: "shadow_candidate",
   action_claim: null,
   shadow_claim: ProviderShadowAuthorityFenceClaimPreimageV2}

ProviderAuthorityFenceCommitmentClaimV2 = one of:
  {claim_kind: "resource_action",
   request_id: UUID,
   handler_name: "serve_resource_action_launch" |
                 "serve_resource_action_down",
   action_id: UUID,
   attempt: PositiveInteger,
   request_input_sha256: Sha256,
   execution_generation: 1,
   claim_owner_api_instance_id: UUID,
   claim_token_sha256: Sha256,
   prior_cancel_requested_at: null | UtcTimestamp,
   preterminal_attempt_sha256: Sha256}
  {claim_kind: "shadow_candidate",
   request_id: UUID,
   handler_name: "serve_shadow_candidate_launch" |
                 "serve_shadow_candidate_down",
   decision_id: UUID,
   request_sequence: PositiveInteger,
   request_role: "PRIMARY_LAUNCH" | "PRIMARY_DOWN",
   immutable_payload_sha256: Sha256,
   request_input_sha256: Sha256,
   execution_generation: 1,
   claim_owner_api_instance_id: UUID,
   claim_token_sha256: Sha256,
   prior_cancel_requested_at: null | UtcTimestamp,
   preterminal_history_sha256: Sha256}

# The two preterminal hashes have one closed domain. For an action claim,
# `preterminal_attempt_sha256` is SHA-256 of the canonical JSON bytes of the
# exact locked `ProviderResourceActionRawAttemptSnapshotV2` after its bounded
# outer-storage validation. For a shadow claim,
# `preterminal_history_sha256` is SHA-256 of the canonical JSON bytes of the
# exact locked `ProviderShadowRawExecutionHistorySnapshotV2` after the same
# outer-storage validation. Request creation/update/heartbeat/lease-expiry
# times and every terminal output are outside both snapshots; the independently
# retained cancellation intent is the only request-time semantic in this
# commitment. No strict cursor decode, normalization, or caller-selected
# subset may replace either raw snapshot hash.

ProviderAuthorityFenceCommitmentProjectionV2 = one of:
  {version: 2,
   fence_kind: "stale_owner",
   operation_id: UUID,  # origin_revoking_handoff_id
   origin_revoking_handoff_id: UUID,
   authority_worker_instance_id: UUID,
   lease_generation: PositiveInteger,
   prior_lease_revision: PositiveInteger,
   terminal_lease_revision: PositiveInteger,
   claims: SortedList<ProviderAuthorityFenceCommitmentClaimV2>}
  {version: 2,
   fence_kind: "cold_recovery",
   operation_id: UUID,  # recovery_id
   recovery_id: UUID,
   authority_worker_instance_id: UUID,
   pod_uid: UUID,
   prior_lease_state: "ACTIVE" | "REVOKED",
   lease_generation: PositiveInteger,
   prior_lease_revision: PositiveInteger,
   terminal_lease_revision: PositiveInteger,
   preserved_revocation_reason: null | "STALE_HANDOFF",
   preserved_revocation_owner_id: null | UUID,
   claims: SortedList<ProviderAuthorityFenceCommitmentClaimV2>}
  {version: 2,
   fence_kind: "process_supersession",
   operation_id: UUID,
   supersession_id: UUID,
   cohort_id: Text,
   authority_worker_instance_id: UUID,
   source_lease_generation: PositiveInteger,
   source_lease_revision: PositiveInteger,
   committed_lease_generation: PositiveInteger,
   committed_lease_revision: PositiveInteger,
   prior_api_instance_id: UUID,
   current_api_instance_id: UUID,
   prior_execution_owner_sha256: Sha256,
   current_execution_owner_sha256: Sha256,
   container_supersession_proof_sha256: Sha256,
   claims: SortedList<ProviderAuthorityFenceCommitmentClaimV2>}

# The sole fence-commitment projector strips operation/fence completion time,
# request created/updated/heartbeat/lease-expiry timestamps, completed fence
# outcomes, terminal requests, selectors/receipts, terminal events, and every
# hash of those outputs. It preserves cancellation intent because that changes
# terminal semantics. The resulting canonical projection—not the full
# preterminal or completed operation—is the commitment hash domain.

ProviderShadowAuthorityFenceOperationPreimageV2 = one of:
  {fence_kind: "stale_owner",
   operation:
       ProviderAuthorityWorkerStaleAuthorityFenceV2 with `request_claims`
       replaced by SortedList<ProviderAuthorityFenceClaimPreimageV2>}
  {fence_kind: "cold_recovery",
   operation:
       ProviderAuthorityWorkerColdRecoveryFenceV2 with `request_claims`
       replaced by SortedList<ProviderAuthorityFenceClaimPreimageV2>}
  {fence_kind: "process_supersession",
   operation:
       ProviderAuthorityWorkerProcessSupersessionV1 with `request_claims` and
       `request_claims_sha256` replaced by
       SortedList<ProviderAuthorityFenceClaimPreimageV2> and its Sha256}

# This is the complete locked, preterminal operation view. It contains no
# terminal request, action selector, shadow receipt, event, completed-claim
# hash, or other projector output. The final fence operation is derived from it
# by replacing every preimage claim with its completed V2 fence claim. Thus the
# commitment projector is acyclic even though completed fence rows embed receipts.

ProviderShadowTerminalHistoricalApiInstanceSnapshotV2 =
    the exact flat projection of one retained `authority-worker` API-instance
    row in owner phase `authority-bound-v2`, `authority-ready-v2`,
    `authority-rewarming-v2`, or `authority-draining-v2`. It preserves the
    immutable process/stable IDs, boot/owner hash, start/heartbeat/drain times,
    supported handlers/payload inventory, and pool generation, but imposes no
    current readiness or freshness requirement.

ProviderShadowTerminalHistoricalOwnerSourceV2 = {
  api_instance: ProviderShadowTerminalHistoricalApiInstanceSnapshotV2
}

ProviderShadowTerminalWinnerSourceV2 = one of:
  {winner_kind: "handler_return",
   generation_zero: null,
   generation_one: ProviderShadowGenerationOneTerminalRequestSourceV2,
   terminal_state: "SUCCEEDED",
   trusted_mode: "PRIVATE_HANDLER_RETURN",
   request_return: ServeShadowCandidateRequestReturnV1,
   fixed_failure_code: null,
   prior_cancel_requested_at: null}
  {winner_kind: "post_claim_failure",
   generation_zero: null,
   generation_one: ProviderShadowGenerationOneTerminalRequestSourceV2,
   terminal_state: "FAILED",
   trusted_mode: "PRIVATE_POST_CLAIM_FAILURE" |
                 "CLAIM_REAUTHORIZATION_FAILED",
   request_return: null,
   fixed_failure_code: "private_handler_failed" |
                       "provider_authority_reauthorization_failed",
   prior_cancel_requested_at: null}
  {winner_kind: "owner_acknowledged_cancellation",
   generation_zero: null,
   generation_one: ProviderShadowGenerationOneTerminalRequestSourceV2,
   terminal_state: "CANCELLED",
   trusted_mode: "OWNER_ACK_CANCEL",
   request_return: null,
   fixed_failure_code: null,
   prior_cancel_requested_at: UtcTimestamp}
  {winner_kind: "owner_quiesced_lease_loss",
   generation_zero: null,
   generation_one: ProviderShadowGenerationOneTerminalRequestSourceV2,
   terminal_state: "CANCELLED",
   trusted_mode: "OWNER_QUIESCED_LEASE_LOSS",
   request_return: null,
   fixed_failure_code: null,
   prior_cancel_requested_at: null | UtcTimestamp}
  {winner_kind: "terminal_before_claim_start",
   generation_zero: null | ProviderShadowGenerationZeroRequestQueueSnapshotV2,
   generation_one: null | ProviderShadowGenerationOneTerminalRequestSourceV2,
   terminal_state: "FAILED" | "CANCELLED",
   trusted_mode: "TERMINAL_BEFORE_CLAIM_START",
   request_return: null,
   fixed_failure_code: null | "private_request_failed_before_claim",
   prior_cancel_requested_at: null | UtcTimestamp}
  {winner_kind: "claim_start_not_representable",
   generation_zero: null,
   generation_one: ProviderShadowGenerationOneTerminalRequestSourceV2,
   terminal_state: "FAILED",
   trusted_mode: "CLAIM_START_NOT_REPRESENTABLE",
   request_return: null,
   fixed_failure_code: "provider_authority_not_representable_at_claim",
   prior_cancel_requested_at: null}
  {winner_kind: "authority_fence_cancellation",
   generation_zero: null,
   generation_one: ProviderShadowGenerationOneTerminalRequestSourceV2,
   terminal_state: "CANCELLED",
   trusted_mode: "STALE_OWNER_FENCE" | "COLD_RECOVERY_FENCE" |
                 "PROCESS_SUPERSESSION_FENCE",
   request_return: null,
   fixed_failure_code: null,
   prior_cancel_requested_at: null | UtcTimestamp,
   fence_operation_preimage:
       ProviderShadowAuthorityFenceOperationPreimageV2,
   fenced_claim_preimage: ProviderShadowAuthorityFenceClaimPreimageV2}

# The two nullable generation sources in terminal_before_claim_start are XOR.
# Handler return and post-claim failure require strict AUTHORIZED history.
# Claim-start-not-representable and terminal-before-claim-start require strict
# BOUND history. OWNER_ACK_CANCEL, OWNER_QUIESCED_LEASE_LOSS, and each typed
# fence accept BOUND or AUTHORIZED: BOUND deterministically derives
# TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START, while AUTHORIZED derives
# REQUEST_CANCELLED/SHADOW_EXECUTION. OWNER_ACK_CANCEL requires a byte-equal
# nonnull request cancellation intent. OWNER_QUIESCED_LEASE_LOSS and each typed
# fence preserve a null intent or acknowledge the exact nonnull one. The fence
# claim must be the unique byte-equal member of the enclosing typed operation;
# caller-selected terminal cause/disposition is not a field.

ProviderShadowTerminalCommitmentV1 = {
  version: 1,
  request_id: UUID,
  request_input_sha256: Sha256,
  immutable_payload_sha256: Sha256,
  handler_name: "serve_shadow_candidate_launch" |
                "serve_shadow_candidate_down",
  request_execution_generation: 0 | 1,
  authority_worker_instance_id: null | UUID,
  worker_instance_id: null | UUID,
  claim_token_sha256: null | Sha256,
  winner_kind: "handler_return" | "post_claim_failure" |
               "owner_acknowledged_cancellation" |
               "owner_quiesced_lease_loss" |
               "terminal_before_claim_start" |
               "claim_start_not_representable" |
               "authority_fence_cancellation",
  trusted_mode: "PRIVATE_HANDLER_RETURN" |
                "PRIVATE_POST_CLAIM_FAILURE" |
                "CLAIM_START_NOT_REPRESENTABLE" |
                "CLAIM_REAUTHORIZATION_FAILED" |
                "OWNER_ACK_CANCEL" | "OWNER_QUIESCED_LEASE_LOSS" |
                "TERMINAL_BEFORE_CLAIM_START" |
                "STALE_OWNER_FENCE" | "COLD_RECOVERY_FENCE" |
                "PROCESS_SUPERSESSION_FENCE",
  terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
  request_return_sha256: null | Sha256,
  fixed_failure_code: null | "private_handler_failed" |
                      "provider_authority_reauthorization_failed" |
                      "private_request_failed_before_claim" |
                      "provider_authority_not_representable_at_claim",
  prior_cancel_requested_at: null | UtcTimestamp,
  fence_operation_kind: null | "stale_owner" | "cold_recovery" |
                        "process_supersession",
  fence_operation_id: null | UUID,
  fence_operation_commitment:
      null | ProviderAuthorityFenceCommitmentProjectionV2,
  fence_operation_commitment_sha256: null | Sha256
}

# The production terminal builder derives this independently bounded canonical
# commitment from the trusted winner source. It omits database-owned finish
# time and transient lease/heartbeat deadlines, but preserves every value that
# distinguishes the caller's intended winner after request/evidence GC. Its
# closed validator enforces the exact winner/mode/state/nullability matrix. For
# a typed fence it stores and hashes the acyclic time-free commitment projection
# derived from the preterminal operation, never the completed operation that
# embeds this receipt.

ProviderShadowTerminalNewWriteSourceV2 = {
  terminalization_disposition: "new_terminal_write",
  candidate_service: ProviderShadowCandidateServiceSnapshotV2,
  reference: ProviderShadowCandidateReferenceSnapshotV2,
  stored_parent: ProviderShadowRepresentedParentSnapshotV2,
  stored_child: ProviderShadowBoundChildProjectionV2,
  committed_history: ProviderShadowRawExecutionHistorySnapshotV2,
      # exact BOUND or AUTHORIZED source; SETTLED rejects
  historical_shadow_origins: [ProviderShadowHistoricalOriginSourceV2],
  historical_owner: null | ProviderShadowTerminalHistoricalOwnerSourceV2,
      # null exactly for a generation-zero winner; required for generation one
  terminal_winner: ProviderShadowTerminalWinnerSourceV2,
  database_now: UtcTimestamp
}

ProviderShadowTerminalStoredAdoptionSourceV2 = {
  terminalization_disposition: "stored_adoption",
  caller_terminal_winner: ProviderShadowTerminalWinnerSourceV2,
  stored_terminal_history: ProviderShadowRequestTerminalHistoryV2,
  retained_terminal_request: null | ProviderShadowRequestTerminalSnapshotV2,
  stored_completed_fence_operation:
      null | ProviderAuthorityWorkerStaleAuthorityFenceV2 |
             ProviderAuthorityWorkerColdRecoveryFenceV2 |
             ProviderAuthorityWorkerProcessSupersessionV1
}

ProviderShadowTerminalizationSourceV2 =
    ProviderShadowTerminalNewWriteSourceV2 |
    ProviderShadowTerminalStoredAdoptionSourceV2

ProviderShadowTerminalizationProjectionV2 = one of:
  {terminalization_result: "NEWLY_TERMINALIZED",
   terminal_request: ProviderShadowRequestTerminalSnapshotV2,
   terminal_history: ProviderShadowRequestTerminalHistoryV2,
   completed_fence_operation:
       null | ProviderAuthorityWorkerStaleAuthorityFenceV2 |
              ProviderAuthorityWorkerColdRecoveryFenceV2 |
              ProviderAuthorityWorkerProcessSupersessionV1}
  {terminalization_result: "EXACT_ADOPTED",
   terminal_request: null | ProviderShadowRequestTerminalSnapshotV2,
   terminal_history: ProviderShadowRequestTerminalHistoryV2,
   completed_fence_operation:
       null | ProviderAuthorityWorkerStaleAuthorityFenceV2 |
              ProviderAuthorityWorkerColdRecoveryFenceV2 |
              ProviderAuthorityWorkerProcessSupersessionV1}
  {terminalization_result: "LOST_RACE",
   terminal_request: null,
   terminal_history: null,
   completed_fence_operation: null}

ProviderShadowTerminalizationRepresentabilityInputV2 = {
  version: 2,
  boundary: "terminalization",
  dispatch_kind: "shadow_candidate",
  action_kind: "launch" | "down",
  source: ProviderShadowTerminalizationSourceV2,
  candidate_projection: ProviderShadowTerminalizationProjectionV2
}

ProviderShadowCoverageSnapshotV2 = {
  decision_id: UUID,
  service_name: Text,
  service_hash: UUID,
  service_incarnation: UUID,
  replica_id: NonnegativeInteger,
  replica_incarnation: UUID,
  desired_generation: PositiveInteger,
  action_kind: "launch" | "down",
  normalizer_contract_version: 1,
  normalization_outcome: "REPRESENTABLE",
  not_representable_reason: null,
  worker_cohort_ref_id: UUID,
  candidate_epoch: UUID,
  qualification_policy_sha256: Sha256,
  qualification_binding_sha256: Sha256,
  admitted_at: UtcTimestamp
}

ProviderShadowHistoricalCandidateBindingV2 = {
  service_name: Text,
  service_hash: UUID,
  service_incarnation: UUID,
  candidate_epoch: UUID,
  qualification_policy_sha256: Sha256,
  qualification_binding_sha256: Sha256,
  admitted_at: UtcTimestamp
}

# This historical binding is projected byte-for-byte from immutable retained
# coverage. It deliberately has no current service mode, owner, lifecycle
# epoch, or elected-version field, so a completed settlement remains adoptable
# after a later shadow-to-authoritative promotion. It cannot authorize a new
# write or successor.

ProviderShadowSuccessorKeyAbsenceV2 = {
  child: true,
  execution_history: true,
  private_correlation: true,
  deterministic_request: true,
  deterministic_queue: true
}

ProviderShadowRetainedCapacityAllocationSnapshotV2 = {
  capacity_profile: ServeActionCapacityProfileV1,
  capacity_profile_sha256: Sha256,
  replica_is_spot: false,
  planned_capacity: 1,
  reserved_fill: false,
  is_zero_cost: false,
  paid_capacity_pool_key: null,
  cost_rebalance_for_replica_id: null,
  unknown_capacity_replacement: false
}

ProviderShadowSameParentSuccessorConstructionSourceV2 = {
  successor_disposition: "same_parent_new",
  successor_kind: "retry_same_plan" | "observe_same_plan",
  candidate_service: ProviderShadowCandidateServiceSnapshotV2,
  active_reference: ProviderShadowCandidateReferenceSnapshotV2,
  complete_preflight: ProviderShadowCompletePreflightSourceV2,
  registration_set: ProviderAuthorityWorkerRegistrationSetV2,
  handoff_fence: ProviderResourceActionClaimHandoffFenceV2,
  accepted_memberships:
      [ProviderAuthorityWorkerAcceptedExecutionMembershipV2],
  accepted_api_instances: [ProviderResourceActionApiInstanceSnapshotV2],
      # both arrays have exactly two index-aligned members
  deterministic_request_id: UUID,
  request_input: ResourceActionRequestInputV1,
  request_input_sha256: Sha256,
  locked_absence: ProviderShadowSuccessorKeyAbsenceV2
}

ProviderShadowPartialDownReplicaTransitionSourceV2 = {
  service_name: Text,
  service_hash: UUID,
  service_incarnation: UUID,
  replica_id: NonnegativeInteger,
  replica_incarnation: UUID,
  source_desired_generation: PositiveInteger,
  target_desired_generation: PositiveInteger,
      # exactly source_desired_generation + 1; overflow rejects
  source_launch_coverage_id: UUID,
  source_launch_sample_id: UUID,
  source_launch_status: "PROVISIONING" | "STARTING" | "READY" |
                        "NOT_READY" | "FAILED" | "FAILED_INITIAL_DELAY" |
                        "FAILED_PROBING" | "FAILED_PROVISION" |
                        "PREEMPTED" | "UNKNOWN",
  target_down_coverage_id_before: null,
  target_down_sample_id_before: null,
  target_cleanup_intent_before: null,
  retained_capacity_allocation:
      ProviderShadowRetainedCapacityAllocationSnapshotV2,
  retained_capacity_allocation_sha256: Sha256
}

# The retained-capacity pair is the complete typed, independently locked M4
# width-one allocation projection already attached to this replica. The hash is
# recomputed from the adjacent object; a hash-only source is invalid. Partial
# down preserves both bytes until down settlement; it never synthesizes,
# releases, or reallocates physical capacity while creating the cleanup root.

ProviderShadowPartialDownReplicaTransitionProjectionV2 = {
  service_name: Text,
  service_hash: UUID,
  service_incarnation: UUID,
  replica_id: NonnegativeInteger,
  replica_incarnation: UUID,
  desired_generation: PositiveInteger,
  replica_status: "SHUTTING_DOWN",
  down_shadow_coverage_id: UUID,
  down_shadow_sample_id: UUID,
  retained_capacity_allocation:
      ProviderShadowRetainedCapacityAllocationSnapshotV2,
  retained_capacity_allocation_sha256: Sha256
}

ProviderShadowPartialDownTargetConstructionSourceV2 = {
  successor_disposition: "partial_down_new",
  successor_kind: "partial_down",
  candidate_service: ProviderShadowCandidateServiceSnapshotV2,
  target_replica:
      ProviderShadowPartialDownReplicaTransitionSourceV2,
  target_reference_before: ProviderShadowPreparingReferenceSnapshotV2,
  target_complete_preflight:
      ProviderResourceActionPreflightRepresentabilityInputV2,
  registration_set: ProviderAuthorityWorkerRegistrationSetV2,
  handoff_fence: ProviderResourceActionClaimHandoffFenceV2,
  accepted_memberships:
      [ProviderAuthorityWorkerAcceptedExecutionMembershipV2],
  accepted_api_instances: [ProviderResourceActionApiInstanceSnapshotV2],
      # both arrays have exactly two index-aligned members
  deterministic_target_decision_id: UUID,
  deterministic_target_request_id: UUID,
  target_request_input: ResourceActionRequestInputV1,
  target_request_input_sha256: Sha256,
  target_absence: {
    coverage: true,
    parent: true,
    child: true,
    execution_history: true,
    private_correlation: true,
    deterministic_request: true,
    deterministic_queue: true,
    replica_down_links: true
  }
}

ProviderShadowPartialDownTargetInsertionProjectionV2 = {
  target_coverage: ProviderShadowCoverageSnapshotV2,
  target_replica:
      ProviderShadowPartialDownReplicaTransitionProjectionV2,
  target_admission: ProviderShadowAdmissionInsertionProjectionV2,
  source_partial_down_link: {
    partial_down_decision_id: UUID,
    partial_down_request_sequence: PositiveInteger,
    partial_down_basis_sha256: Sha256
  }
}

ProviderShadowPartialDownReplicaDescendantSnapshotV2 = one of:
  {replica_disposition: "present",
   service_name: Text,
   service_hash: UUID,
   service_incarnation: UUID,
   replica_id: NonnegativeInteger,
   replica_incarnation: UUID,
   desired_generation: PositiveInteger,
   row_disposition: "retained",
   replica_status: "SHUTTING_DOWN" | "FAILED_CLEANUP" | "UNKNOWN",
   down_shadow_coverage_id: UUID,
   down_shadow_sample_id: UUID,
   retained_capacity_allocation:
       ProviderShadowRetainedCapacityAllocationSnapshotV2,
   retained_capacity_allocation_sha256: Sha256}
  {replica_disposition: "absent_after_down",
   service_name: Text,
   service_hash: UUID,
   service_incarnation: UUID,
   replica_id: NonnegativeInteger,
   replica_incarnation: UUID,
   desired_generation: PositiveInteger,
   row_disposition: "removed",
   replica_status: null,
   down_shadow_coverage_id: UUID,
   down_shadow_sample_id: UUID,
   exact_replica_incarnation_absent: true,
   live_replica_links_absent: true,
   cleanup_intent_absent: true}

ProviderShadowStoredPartialDownTargetSourceV2 = {
  target_coverage: ProviderShadowCoverageSnapshotV2,
  target_replica: ProviderShadowPartialDownReplicaDescendantSnapshotV2,
  target_admission_descendant: ProviderShadowAdmissionStoredDescendantV2,
  reciprocal_target_source_binding: {
    source_decision_id: UUID,
    source_request_sequence: PositiveInteger,
    source_partial_down_basis_sha256: Sha256
  }
}

# The stored target is a legal descendant source, never the initial-insert
# output. Its immutable coverage/admission times and retained source cleanup
# basis reconstruct the original target insertion; its replica may have
# advanced through the closed down lifecycle or be proved absent after a
# completed removal, and its request may be authorized, terminal, settled, or
# GCed. The absent arm is validated against the completed target outcome,
# immutable coverage, reciprocal source-Q binding, original settlement
# commitment, and exact absence of the incarnation, live links, and cleanup
# intent; it never invents a `REMOVED` replica status or row. All surviving
# link/identity/capacity bytes remain equal.

ProviderShadowStoredSuccessorAdoptionSourceV2 = one of:
  {successor_disposition: "stored_successor_adoption",
   successor_kind: "retry_same_plan" | "observe_same_plan",
   stored_successor: ProviderShadowAdmissionStoredDescendantV2,
   stored_partial_down_target: null}
  {successor_disposition: "stored_successor_adoption",
   successor_kind: "partial_down",
   stored_successor: null,
   stored_partial_down_target:
       ProviderShadowStoredPartialDownTargetSourceV2}

ProviderShadowSettlementNewSuccessorSourceV2 =
    ProviderShadowSameParentSuccessorConstructionSourceV2 |
    ProviderShadowPartialDownTargetConstructionSourceV2

ProviderShadowSettlementSuccessorSourceV2 =
    ProviderShadowSettlementNewSuccessorSourceV2 |
    ProviderShadowStoredSuccessorAdoptionSourceV2

ProviderShadowSettlementSuccessorProjectionV2 = one of:
  {successor_kind: "retry_same_plan" | "observe_same_plan",
   same_parent_admission: ProviderShadowAdmissionInsertionProjectionV2,
   partial_down_target: null}
  {successor_kind: "partial_down",
   same_parent_admission: null,
   partial_down_target:
       ProviderShadowPartialDownTargetInsertionProjectionV2}

ProviderShadowSettlementCommitmentV1 = {
  version: 1,
  operation_id: UUID,
  decision_id: UUID,
  request_sequence: PositiveInteger,
  request_role: "PRIMARY_LAUNCH" | "PRIMARY_DOWN",
  terminal_history_sha256: Sha256,
  new_write_source_sha256: Sha256,
  settlement_projection_sha256: Sha256,
  successor_kind: null | "retry_same_plan" | "observe_same_plan" |
                  "partial_down",
  successor_decision_id: null | UUID,
  successor_request_sequence: null | PositiveInteger,
  settled_at: UtcTimestamp
}

# `new_write_source_sha256` names the canonical bytes of the complete original
# `ProviderShadowSettlementNewWriteSourceV2`, including operation ID and its one
# database time. `settlement_projection_sha256` names only the complete adjacent
# `ProviderShadowSettlementProjectionV2`; that projection never embeds this
# commitment, so the graph is acyclic. Successor identity is null exactly for a
# null kind and otherwise equals the projection's inserted child. This compact
# permanent commitment contains no mutable service/replica/API/request row and
# no raw provider cursor bytes. It is sufficient for a caller retaining its
# original source/projection to distinguish exact adoption from another legal
# winner after request and evidence GC.

ProviderShadowSettlementProjectionV2 = {
  version: 2,
  settlement_basis: "HANDLER_RETURN" | "REQUEST_FALLBACK",
  reduction_disposition: "S" | "R" | "U" | "B" | "Q" |
                         "P0" | "O" | "X",
  request_return: null | ServeShadowCandidateRequestReturnV1,
  request_return_sha256: null | Sha256,
  fallback_evidence: null | ServeShadowCandidateRequestFallbackEvidenceV1,
  actual_outcome: ServeShadowCandidateOutcomeV1,
  actual_outcome_sha256: Sha256,
  proposed_outcome: ServeShadowCandidateOutcomeV1,
  proposed_outcome_sha256: Sha256,
  retry_decision: ServeShadowRetryDecisionV1,
  retry_decision_sha256: Sha256,
  post_settlement_parent: ProviderShadowParentSnapshotV2,
  completed_child: ProviderShadowCompletedChildProjectionV2,
  candidate_settled_history: ProviderShadowRawExecutionHistorySnapshotV2,
  partial_down_basis: null | ServeShadowPartialLaunchCleanupBasisV1,
  successor_kind: null | "retry_same_plan" | "observe_same_plan" |
                  "partial_down",
  successor_projection:
      null | ProviderShadowSettlementSuccessorProjectionV2
}

ProviderShadowSettlementNewWriteSourceV2 = {
  settlement_disposition: "new_settlement",
  operation_id: UUID,
  candidate_service: ProviderShadowCandidateServiceSnapshotV2,
  active_reference: ProviderShadowCandidateReferenceSnapshotV2,
  locked_coverage: ProviderShadowCoverageSnapshotV2,
  locked_parent: ProviderShadowRepresentedParentSnapshotV2,
      # exact PRIVATE_API_REQUEST/RUNNING source
  locked_child: ProviderShadowBoundChildProjectionV2,
  locked_history: ProviderShadowRawExecutionHistorySnapshotV2,
      # exact terminal but not SETTLED source
  terminal_history: ProviderShadowRequestTerminalHistoryV2,
  retained_terminal_request: null | ProviderShadowRequestTerminalSnapshotV2,
  historical_shadow_origins: [ProviderShadowHistoricalOriginSourceV2],
  successor_source: null | ProviderShadowSettlementNewSuccessorSourceV2,
  database_now: UtcTimestamp
}

ProviderShadowSettlementStoredAdoptionSourceV2 = {
  settlement_disposition: "stored_adoption",
  caller_new_write_source: ProviderShadowSettlementNewWriteSourceV2,
  historical_candidate_binding: ProviderShadowHistoricalCandidateBindingV2,
  stored_reference: ProviderShadowCandidateReferenceSnapshotV2 |
                    ProviderShadowReleasedReferenceSnapshotV2,
  stored_coverage: ProviderShadowCoverageSnapshotV2,
  stored_parent: ProviderShadowParentSnapshotV2,
  stored_child: ProviderShadowCompletedChildProjectionV2,
  stored_history: ProviderShadowRawExecutionHistorySnapshotV2,
      # exact SETTLED source
  terminal_history: ProviderShadowRequestTerminalHistoryV2,
  retained_terminal_request: null | ProviderShadowRequestTerminalSnapshotV2,
  historical_shadow_origins: [ProviderShadowHistoricalOriginSourceV2],
  successor_source: null | ProviderShadowStoredSuccessorAdoptionSourceV2,
  stored_settlement_commitment: ProviderShadowSettlementCommitmentV1,
  stored_settlement_commitment_sha256: Sha256
}

ProviderShadowSettlementComponentGcAbsenceV2 = {
  decision_id: UUID,
  service_name: Text,
  service_hash: UUID,
  service_incarnation: UUID,
  replica_id: NonnegativeInteger,
  replica_incarnation: UUID,
  desired_generation: PositiveInteger,
  coverage: true,
  parent: true,
  all_children: true,
  all_execution_histories: true,
  private_correlations: true,
  deterministic_requests: true,
  deterministic_queues: true,
  live_replica_links: true,
  cleanup_intent: true,
  reference:
      {reference_disposition: "released",
       released_reference: ProviderShadowReleasedReferenceSnapshotV2} |
      {reference_disposition: "absent", exact_reference_absent: true}
}

ProviderShadowSettlementGcAbsenceV2 = {
  source_component: ProviderShadowSettlementComponentGcAbsenceV2,
  component_peer: one of:
      {component_relation: "ordinary",
       q_settlement_receipt: null,
       peer_absence: null}
      {component_relation: "outgoing_Q",
       q_settlement_receipt: ProviderShadowSettlementReceiptV1,
       peer_absence: ProviderShadowSettlementComponentGcAbsenceV2}
      {component_relation: "incoming_Q",
       q_settlement_receipt: ProviderShadowSettlementReceiptV1,
       peer_absence: ProviderShadowSettlementComponentGcAbsenceV2}
}

# Component relation is parent-graph-wide, not inferred from the replayed
# receipt. `outgoing_Q` binds the unique permanent Q settlement receipt in this
# parent to the absent primary-down peer; `incoming_Q` binds the unique
# reverse-indexed source receipt to this absent target. `ordinary` requires no
# incoming or outgoing Q receipt for any child in the parent. The store checks
# the partial unique source-parent index on `decision_id` and partial unique
# reverse-target index on `(successor_decision_id,
# successor_request_sequence)`, rather than scanning retry history, and rejects
# a second/crossed peer. Same-parent retry/observe children are covered by
# `all_children` in the one source component. Typed GC always deletes a Q pair
# atomically, so no arm permits one absent and one retained side.

ProviderShadowSettlementReceiptAdoptionSourceV2 = {
  settlement_disposition: "receipt_only_adoption",
  caller_new_write_source: ProviderShadowSettlementNewWriteSourceV2,
  stored_settlement_commitment: ProviderShadowSettlementCommitmentV1,
  stored_settlement_commitment_sha256: Sha256,
  graph_absence: ProviderShadowSettlementGcAbsenceV2
}

ProviderShadowSettlementSourceV2 =
    ProviderShadowSettlementNewWriteSourceV2 |
    ProviderShadowSettlementStoredAdoptionSourceV2 |
    ProviderShadowSettlementReceiptAdoptionSourceV2

ProviderShadowSettlementCandidateV2 = one of:
  {settlement_result:
       "NEWLY_SETTLED" | "EXACT_ADOPTED_GRAPH" |
       "EXACT_ADOPTED_RECEIPT",
   settlement_projection: ProviderShadowSettlementProjectionV2,
   settlement_commitment: ProviderShadowSettlementCommitmentV1,
   settlement_commitment_sha256: Sha256}
  {settlement_result: "LOST_RACE",
   settlement_projection: null,
   settlement_commitment: null,
   settlement_commitment_sha256: null}

# `new_settlement` may produce only `NEWLY_SETTLED`.
# `stored_adoption` may produce only `EXACT_ADOPTED_GRAPH` or `LOST_RACE`.
# `receipt_only_adoption` may produce only `EXACT_ADOPTED_RECEIPT` or
# `LOST_RACE`. A malformed, hash-invalid, partial, or crossed graph/commitment
# is corruption and cannot inhabit any candidate result.

ProviderShadowSettlementRepresentabilityInputV2 = {
  version: 2,
  boundary: "settlement",
  dispatch_kind: "shadow_candidate",
  action_kind: "launch" | "down",
  source: ProviderShadowSettlementSourceV2,
  candidate_projection: ProviderShadowSettlementCandidateV2
}

ProviderResourceActionDirectActionSnapshotV2 = {
  action_id: UUID,
  resource_identity: ResourceActionIdentityV1,
  action_revision: NonnegativeInteger,
  action_current_attempt: NonnegativeInteger,
  kernel_state: "READY" | "QUEUED" | "BLOCKED",
  action_last_result: null | ServeReplicaActionOutcomeV1,
  action_last_result_sha256: null | Sha256
}

ProviderResourceActionDirectNoEffectBuilderInputV2 = {
  version: 2,
  action: ProviderResourceActionDirectActionSnapshotV2,
  locked_predecessor: null | ProviderResourceActionReducerAttemptSnapshotV2,
  locked_current: null | ProviderResourceActionReducerAttemptSnapshotV2,
  request_row_disposition:
      "not_applicable" | "retained_terminal" | "garbage_collected",
  retained_terminal_request:
      null | ProviderResourceActionRequestTerminalSnapshotV2,
  cancelled_at: UtcTimestamp
}

ProviderResourceActionDirectTransitionRepresentabilityInputV2 = {
  version: 2,
  boundary: "owner_fenced_transition",
  dispatch_kind: "authoritative_action",
  action_kind: "launch",
  builder_input: ProviderResourceActionDirectNoEffectBuilderInputV2,
  candidate_outcome: ServeReplicaActionOutcomeV1
}

ProviderResourceActionRawAttemptSnapshotV2 = {
  action_id: UUID,
  attempt: PositiveInteger,
  request_id: UUID,
  request_terminal_snapshot:
      null | ProviderResourceActionRequestTerminalSnapshotV2,
  terminal_authority_selector:
      null | ProviderResourceActionAttemptTerminalAuthoritySelectorV2,
  request_input_sha256: Sha256,
  mutation_boundary: "NOT_STARTED" | "INTENT_COMMITTED" |
                     "SUBMITTED_OR_AMBIGUOUS" | "SETTLED",
  provider_io_boundary: "NOT_STARTED" | "INTENT_COMMITTED" |
                        "SUBMITTED_OR_AMBIGUOUS",
  provider_progress_revision: NonnegativeInteger,
  provider_progress_raw: null | CanonicalJsonObject,
  provider_progress_sha256: null | Sha256,
  provider_operation_id: null | Text,
  typed_outcome: null | ServeReplicaActionOutcomeV1,
  typed_outcome_sha256: null | Sha256,
  settled_at: null | UtcTimestamp,
  historical_authority: [  # 0..RESOURCE_ACTION_ATTEMPT_AUTHORITY_KEYS_MAX_V2
    ProviderResourceActionExecutionAuthorityLineageV2
  ]
}

ProviderResourceActionRawReducerHistoryProjectionV2 = {
  version: 2,
  action_id: UUID,
  action_kind: "launch" | "down",
  action_revision: NonnegativeInteger,
  action_current_attempt: PositiveInteger,
  action_last_result: null | ServeReplicaActionOutcomeV1,
  action_last_result_sha256: null | Sha256,
  locked_predecessor: null | ProviderResourceActionReducerAttemptSnapshotV2,
  locked_current: ProviderResourceActionRawAttemptSnapshotV2,
  launch_no_io_prefix: null | ServeLaunchNoIoPrefixV1,
  supersession_quiescence:
      null | ProviderLaunchSupersessionQuiescenceV1
}

ProviderResourceActionRawInvalidJournalClassifierInputV2 = {
  version: 2,
  action_kind: "launch" | "down",
  reducer_history: ProviderResourceActionRawReducerHistoryProjectionV2
}

ProviderResourceActionV2ReductionAuthorityContext = {
  version: 2,
  stored_spec: ServeReplicaActionSpecV2,
  resolved_cohort: ProviderAuthorityWorkerCohortV2,
  historical_authority: [  # 0..RESOURCE_ACTION_REDUCTION_AUTHORITY_KEYS_MAX_V2
    ProviderResourceActionExecutionAuthorityLineageV2
  ]
}

RESOURCE_ACTION_PROVIDER_EFFECTS_MAX_V2 = 5
RESOURCE_ACTION_ATTEMPT_AUTHORITY_KEYS_MAX_V2 =
    2 * RESOURCE_ACTION_PROVIDER_EFFECTS_MAX_V2 + 3  # 13
RESOURCE_ACTION_REDUCTION_AUTHORITY_KEYS_MAX_V2 =
    2 * RESOURCE_ACTION_ATTEMPT_AUTHORITY_KEYS_MAX_V2 + 2  # 28

ProviderResourceActionRawFallbackReductionInputV2 = {
  version: 2,
  action_kind: "launch" | "down",
  authority_context: ProviderResourceActionV2ReductionAuthorityContext,
  journal_classifier_input:
      ProviderResourceActionRawInvalidJournalClassifierInputV2
}

ProviderResourceActionRepresentabilityInputV2 =
  ProviderResourceActionPreflightRepresentabilityInputV2 |
  ProviderResourceActionAdmissionRepresentabilityInputV2 |
  ProviderResourceActionClaimedExecutionRepresentabilityInputV2 |
  ProviderResourceActionPreIoRepresentabilityInputV2 |
  ProviderShadowAdmissionRepresentabilityInputV2 |
  ProviderShadowClaimedExecutionRepresentabilityInputV2 |
  ProviderShadowPreIoRepresentabilityInputV2 |
  ProviderShadowTerminalizationRepresentabilityInputV2 |
  ProviderShadowSettlementRepresentabilityInputV2 |
  ProviderResourceActionDirectTransitionRepresentabilityInputV2

ProviderResourceActionRepresentabilityFixtureInputV2 = {
  version: 2,
  launch: {
    complete_preflight: ProviderResourceActionPreflightRepresentabilityInputV2,
    linked_admission: ProviderResourceActionAdmissionRepresentabilityInputV2,
    claimed_execution:
        ProviderResourceActionClaimedExecutionRepresentabilityInputV2,
    pre_io: ProviderResourceActionPreIoRepresentabilityInputV2
  },
  down: {
    complete_preflight: ProviderResourceActionPreflightRepresentabilityInputV2,
    linked_admission: ProviderResourceActionAdmissionRepresentabilityInputV2,
    claimed_execution:
        ProviderResourceActionClaimedExecutionRepresentabilityInputV2,
    pre_io: ProviderResourceActionPreIoRepresentabilityInputV2
  },
  down_cleanup_preflight_cases: [
    ProviderResourceActionPreflightRepresentabilityInputV2
  ],
  authoritative_history_cases: [
    ProviderResourceActionAdmissionRepresentabilityInputV2 |
    ProviderResourceActionClaimedExecutionRepresentabilityInputV2 |
    ProviderResourceActionPreIoRepresentabilityInputV2
  ],
  shadow: {
    launch: {
      complete_preflight: ProviderResourceActionPreflightRepresentabilityInputV2,
      linked_admission: ProviderShadowAdmissionRepresentabilityInputV2,
      claimed_execution: ProviderShadowClaimedExecutionRepresentabilityInputV2,
      pre_io: ProviderShadowPreIoRepresentabilityInputV2,
      terminalization: ProviderShadowTerminalizationRepresentabilityInputV2,
      settlement: ProviderShadowSettlementRepresentabilityInputV2
    },
    down: {
      complete_preflight: ProviderResourceActionPreflightRepresentabilityInputV2,
      linked_admission: ProviderShadowAdmissionRepresentabilityInputV2,
      claimed_execution: ProviderShadowClaimedExecutionRepresentabilityInputV2,
      pre_io: ProviderShadowPreIoRepresentabilityInputV2,
      terminalization: ProviderShadowTerminalizationRepresentabilityInputV2,
      settlement: ProviderShadowSettlementRepresentabilityInputV2
    },
    history_cases: [
      ProviderShadowAdmissionRepresentabilityInputV2 |
      ProviderShadowClaimedExecutionRepresentabilityInputV2 |
      ProviderShadowPreIoRepresentabilityInputV2 |
      ProviderShadowTerminalizationRepresentabilityInputV2 |
      ProviderShadowSettlementRepresentabilityInputV2
    ]
  },
  direct_transition_cases: [
    ProviderResourceActionDirectTransitionRepresentabilityInputV2
  ]
}
```

The twenty primary roots contain one complete-preflight, linked-admission,
claim-start, and immediate pre-I/O root per action kind and dispatch kind, plus
one shadow terminalization and settlement root per action kind.
Every authoritative admission/claim/pre-I/O spec has exactly
`binding_kind="authoritative_action"`; every shadow root has exactly
`binding_kind="shadow_candidate"` and is projected through its parent, primary
child, and one-to-one execution history rather than API006 action history. Every
linked-admission root carries the exact complete locked V2 registration set and
the exact null-nonterminal-handoff/completed-cold-recovery fence; the aligned
membership revision/hash alone is never enough to reconstruct either object.
Every
`accepted_memberships` array contains exactly the two
accepted, fresh-lease members in ascending Pod-UID order. At linked admission,
`accepted_api_instances` contains exactly two fresh ready snapshots in that same
order and `database_now` is the one PostgreSQL clock read from the locked
materialization transaction. Each API snapshot's process/stable-Pod/start/owner
hash is byte-equal to its membership lease execution owner, and its heartbeat is
fresh at that clock. All repeated action kinds, cohort
references, worker registrations, action or decision IDs, deterministic request
IDs, attempts or request sequences, execution generations, capsules, specs,
progress, and attestations
must be byte-equal to their enclosing typed source. Claim-start and pre-I/O
`resolved_cohort` is the exact locked V2 row named by the stored capsule's
compact `(cohort_id, cohort_identity_sha256)` reference; the accepted member
validates against it, and a common stable-identity projector requires the
V1-shaped API006 attempt attestation to be byte-equal to the V2 membership
identity on every immutable field. This evidence projection does not select
V1 ownership or permit a V1 live spec. The complete evaluator
constructs the preflight root only after native construction and before
serializing its candidate complete response. The locked authoritative
materialization transaction constructs the admission root from that exact
response, the full
locked `READY` action/spec/hash/identity/generation/revision/due-time/terminal
shape, and the attempt/request identity, full canonical request input, and hash
it is about to materialize. The action ID/kind/spec and all reducer-history
action fields must be byte-equal to that locked action. Before either row is
inserted, the admission enumerator calls the
same pure production
`project_provider_resource_action_post_materialization_v2(
ProviderResourceActionAdmissionRepresentabilityInputV2) ->
ProviderResourceActionPostMaterializationProjectionV2` used by the V2
materializer. From the locked pre-insert admission root, that projector
exact-simulates the sole successful insert transition: action revision plus
one, `kernel_state=QUEUED`, `action_current_attempt=next_attempt`, cleared next-
attempt time, and a nonterminal `locked_current` with the deterministic request
ID/input hash, null operation/outcome/settlement, both boundaries at
`NOT_STARTED`, and either the production-derived inherited retry seed at
revision one or a fresh null cursor at revision zero. It preserves the exact
settled immediate predecessor and rejects any different action/current/
predecessor shape. The full request input must hash to the declared input hash
and carry the same action/attempt/request/kind-specific private handler and
pristine queue state. The materializer constructs every deterministic action,
attempt, request, and queue column from that projection and byte-compares those
committed columns to it before returning success. Database-owned admitted /
updated timestamps are intentionally not projection fields and retain their
ordinary transaction semantics. The hypothetical projection remains pre-write
sizing evidence, never durable claim or execution authority.

The admission enumerator then derives a closed hypothetical pre-I/O root for
each accepted worker from that worker's exact aligned API snapshot, the one
`database_now`, and the projected post-materialization history and
evaluates every future progress/return/reducer case. Only the exact candidate
graph, resolved response cohort, projected history/retry seed, deterministic
request identity/hash, and code-owned response and renewal-successor
attestation profiles participate. The frozen request-generation profile is
exactly `PROVIDER_RESOURCE_ACTION_REQUEST_GENERATIONS_V2 = (1,)`: a
`ReplayPolicy.NEVER` request has one claim, and any recovery is attempt `n+1`
with a different request. No maximum-BIGINT or same-request replacement
generation is fabricated. This is an early
representability optimization for those exact two members, not authority for a
future replacement member. For each case both workers are checked and the
deterministic largest result (canonical length, then hash) is retained; either
worker being oversized rejects admission.

The private-shadow linked-admission evaluator uses the same two index-aligned
accepted memberships/API snapshots and one PostgreSQL `database_now`. Its sole
production projector takes the locked represented parent plus complete
preflight and derives the primary `private_api_request` child, deterministic
generation-zero request/input/hash, and `BOUND` history. Initial candidate
insert requires parent `PENDING_SELECTION/PENDING` and exact-simulates the one
transaction that commits parent `PRIVATE_API_REQUEST/RUNNING`, child
`REQUEST_BOUND`, request/queue/correlation,
`SHADOW_ACTIVE`, and history revision one. Retry candidate insert requires the
same already-`RUNNING` parent, exact settled immediate predecessor/retry
authorization, and the next contiguous sequence/logical attempt; it inserts the
new child/history/request/queue/correlation graph atomically without changing
parent identity. Initial and retry construction each starts from its independent
source arm; neither may use the candidate projection as evidence. The accepted
API snapshots are the exact class-14 rows locked and revalidated after the
complete class-10 prefix and before the new request/queue keys at classes
15-16. Their keys derive only from already-locked lease-owner scalars, so the
transaction never reaches backward.

Stored adoption starts instead from a legal stored descendant. The builder
reconstructs the immutable insertion projection and original database times
from its parent/child/history/reference and request descendant, then requires
those reconstructed insertion bytes to equal the candidate projection. It does
not compare a mutable descendant phase to the initial phase. The closed request
descendants are generation-zero bound/queued, generation-one authorized/running,
terminal with retained request, and settled terminal with the request already
GCed; the parent/child/history and immutable class-17 receipt cross-validate the
selected arm. Its exact historical-origin list is empty for a fresh initial
cursor and re-proves every prior-request origin carried by an inherited retry
cursor. The same adoption contract applies to initial admission and every
`R`/`U`/`P0`/`O`/`Q` successor admission. A partial or crossed descendant is
corruption, never permission to insert missing members. The
projected child role/kind/spec/invocation/request/hash must cross-equal the
parent. Its real row has exactly the projected decision/sequence/logical-
attempt/role, `private_api_request`, `REQUEST_BOUND`, request ID, invocation/
hash, null operation/outcome/retry/observation/effect/divergence fields,
`admitted_at=request_bound_at=database_now`, and null `completed_at`; the child
table has no revision field. The materializer constructs and byte-compares every
one of those columns, not only its invocation. The history has a wholly null authority/settlement bundle plus only
the exact empty or inherited retry seed. Before any private request is inserted,
the enumerator derives each accepted member's hypothetical dispatch membership,
shadow authority proof, authorized-history candidate, progress/return/fallback/
outcome domain, and terminal receipt from its aligned API snapshot. Either
member or any independently stored JSON child exceeding 65,536 bytes rejects
the private candidate before any private graph is written.

That rejection cannot strand the already counted slot. Retryable lock,
membership, freshness, or artifact drift leaves the parent
`PENDING_SELECTION/PENDING` and reference `PREPARING` and retries from fresh
evidence. A deterministic complete enumerator result of unbounded, oversized,
or unsupported is permanent. Under the same full 1-16 prefix, the fallback
projector proves the exact initial source and zero child/history/correlation/
deterministic-request/queue descendants, then atomically changes only parent to
`LEGACY_CONTROLLER/RUNNING/linked_admission_not_representable` and reference to
`SHADOW_ACTIVE`. It writes no private graph. Its result and original database
time are exact-adoptable from those two durable descendants. Only after commit
does the decision owner signal the one same-cell legacy worker; a crash before
signal is recovered from the stored full invocation under the ordinary proved-
no-`PRE_SUBMIT` legacy handshake. Once any private descendant exists, fallback
is permanently illegal. Thus no path signals both mutation owners or silently
releases capacity.

Every authoritative generation-one request claim, including one by a rolling
replacement or cold-recovery member selected before the request was claimed,
must next
run the `claimed_execution` boundary before invoking the handler and before any
lineage, attestation, progress, return, or result write. From the one
consolidated lock program it constructs the exact claim root and the candidate
Serve039 lineage. For `candidate_insert`, the sole production lineage builder
uses the root's one PostgreSQL `database_now` for `authorized_at` and every
same-transaction checked-at field; the root validator reruns that builder and
requires byte equality. For `stored_adoption`, it parses the immutable row,
validates that the stable action/request/generation/token key and canonical
hashes are exact, replays its proof at its stored `authorized_at`, and requires
every retained locked row to be a legal historical descendant. That predicate
may accept a retained revocation, handoff, reference release, or policy close
solely to validate immutable lineage; it grants no current execution. A second
predicate must independently prove the same live generation/token, exact-
current process owner, fresh accepted membership/lease/API instance, an
`ACTIVE/(OPEN | DRAINING)` policy, the exact already-bound `ACTION_ACTIVE`
reference, and no blocking handoff. `DRAINING` is accepted only when that
action/reference predates and remained byte-bound across the admission-state
CAS; it never admits or binds new work. It never remints timestamps,
compares the row to a fresh time-dependent candidate, or overwrites it. The evaluator
measures both separately stored lineage JSON children, the exact starting
attestation, and all code-owned legal before/after renewal-successor profiles.
Only after historical validation, representability, and that distinct current-
execution predicate all pass may the transaction exact-insert/adopt lineage and
let the handler run. Membership handoff/cold-recovery qualification evaluates the
same candidate-member profiles before accepting a replacement, and claim-start
is still the final per-generation barrier.

Claim-start rejection distinguishes whether authority already linearized. For
a candidate insert with no lineage, if the root is unbounded, oversized, or
drifted, the worker does not release first. While it still owns the exact
token/generation fence, one class-15, class-16, then class-17 transaction on the
same consolidated connection revalidates request and queue, writes the fixed
bounded `provider_authority_not_representable_at_claim` terminal error, and
clears the active-claim fields while preserving both the stable authority-
worker ID and process claim-owner ID in history. It atomically inserts/exact-
adopts the
`NO_SUCCESSFUL_CLAIM_START`/`CLAIM_START_NOT_REPRESENTABLE` terminal selector,
without invoking the handler or writing lineage/progress/return. If that CAS
loses, it writes nothing. This exact shape has no provider I/O and later reduces
through the no-I/O `P0` path even though its generation is positive. No
provider-originated or candidate-sized bytes enter the error.

For `stored_adoption`, the existing exact lineage is historical truth. A failed
current-successor or representability replay therefore never emits
`NO_SUCCESSFUL_CLAIM_START` and never deletes or overwrites lineage. Under the
same token/generation CAS it terminalizes `FAILED` with the fixed bounded
`provider_authority_reauthorization_failed` error and a
`LINEAGE`/`CLAIM_REAUTHORIZATION_FAILED` selector naming that row. Reduction
uses the actual retained journal and outcome evidence; it may classify no-I/O
only when those bytes prove it, and it never assumes `P0` from the rejection.
An unequal or unparseable stored lineage is corruption, not a reauthorization
failure: it writes no terminal selector or request state and blocks for
operator repair. In either branch a lost CAS writes nothing, and no rejected
path invokes the handler or performs new provider I/O.
The claimed handler separately constructs the `pre_io` root from the locked
stored rows, exact current membership, immutable lineage, current progress, and
current attestation immediately before its first intent/watermark commit. No
transport value, artifact, fixture, caller-supplied mapping, or prior boundary
root can replace fields at a later boundary. Strict fixture parsing uses the
same DTO validators but grants no live authority.

Every private-shadow generation-one claim runs the disjoint claimed-execution
root under the parent lock program before handler invocation. Its source and
candidate projection are disjoint. Candidate authorization requires the source
history to be strict `BOUND`; the sole builder uses the root's independently
loaded candidate-service row, active reference, byte-equal immutable complete
preflight retained in history, `database_now`, actual request claim, and selected
full registration set, handoff fence, accepted membership, and API snapshot to
derive the dispatch membership, authority
proof, lineage hash, authorization time, and complete `AUTHORIZED` history.
Only that history is output. The validator reruns the builder and the
transaction commits only the one `BOUND -> AUTHORIZED` CAS.
The root's canonical `historical_shadow_origins` list must equal every distinct
origin recursively reachable from the inherited/current cursor. Each origin is
proved by its retained completed predecessor child, strict settled history, and
permanent receipt; the builder cannot invent an older worker/effect claim.

Stored adoption instead supplies one strict persisted `AUTHORIZED` history as
source and output. It validates the proof at its stored authorization time
against the independent service/reference/preflight and stable request identity,
then separately proves current claim token/generation, ready process owner,
full current registration set/handoff fence, accepted lease/API membership,
and no blocking handoff. It never accepts root-
supplied membership/proof/lineage fields, remints authorization time, or compares
the stored row to a fresh time-dependent candidate. Action-shaped membership or
lineage values cross-reject at parse time.

A candidate-authorization representability failure leaves history `BOUND` and,
under the still-owned request/queue fence, terminalizes the request `FAILED`
with fixed bounded error and a `TERMINAL_BEFORE_CLAIM_START/
NO_SUCCESSFUL_CLAIM_START` shadow receipt fixed to `FAILED`, generation one,
the exact claim-owner pair, and null lineage. The terminal receipt has a null
current authority bundle; settlement then classifies the retained raw `BOUND`
journal through the exhaustive `P0`/`O`/`S`/`X` table. A fresh empty first
attempt is normally `P0`; an inherited valid nonterminal retry/observation seed
is normally `O`; a retained valid success is `S`; malformed progress is `X`.
The rejection itself never chooses `P0`. A failure while adopting an existing `AUTHORIZED` history never erases
it: it terminalizes with `REQUEST_FAILED/SHADOW_EXECUTION`, retains the lineage
hash, and reduces from the actual journal. A malformed/crossed history is
quarantined without terminal mutation. Lost-CAS paths write nothing.

Immediately before each next progress/intent or terminal return, the shadow
handler constructs `ProviderShadowPreIoRepresentabilityInputV2` from the locked
candidate service, active reference, history-retained complete preflight,
parent/child, same full registration-set/handoff/membership/API/request
authority, immutable authorized
history, current shadow progress, attestation, and one database time. The
dispatch membership, execution proof, and lineage hash are loaded only from the
authorized history and cross-validated against those independent current rows;
they are not parallel root fields. The same exact historical-origin extraction
also includes origins reachable from the next handler-return/no-effect branch.
It carries
no future result or scenario selector. The nonempty unique code constant
`PROVIDER_SHADOW_REPRESENTABILITY_SCENARIOS_V2`, its applicability classifier,
and fixed-signature production-builder dispatch are AST-inventoried against the
case inventory as one surface. For every scenario applicable to the exact root,
`build_provider_shadow_next_representability_boundary_v2()` synthesizes the
closed `ProviderShadowNextRepresentabilityBoundaryV2`: progress covers every
legal next cursor, while handler return carries only the now-known strict return
and final progress. It cannot invent the later database-owned terminal receipt,
fallback, reducer outcome, quiescence, or successor. Neither a
fixture nor a caller supplies the scenario ID or output. Live linked admission
enumerates all future reachable scenarios; claim-start repeats them for the
actual member; immediate pre-I/O/return classifies the actual code path and
reruns every remaining applicable output. The builder must reproduce every
candidate child byte and each JSON child is sized independently. Unknown,
inapplicable, or oversized bytes block before the next intent; a prior boundary
cannot authorize them.

Terminalization and settlement are separate representability boundaries because
their bytes do not exist while the request claim is live. After locking the
request, a new-write terminalizer constructs the root from the immutable route,
full locked preterminal request/queue state, independent candidate-service /
active-reference / running-parent / bound-child evidence, raw `BOUND` or
`AUTHORIZED` history, one trusted terminal-winner source, the independently
locked historical API-instance for every generation-one
winner, and one database
clock read. The production builder derives the finish time as the greatest of
that clock and every request terminal lower-bound field, derives the typed
permanent terminal commitment, terminal request, and receipt, and returns them
only as `candidate_projection`. The validator reruns the complete generation/
owner/disposition/cause/lineage/return/failure/fence matrix before class-17
insertion. The process ID comes from the locked request and historical API row;
the stable ID comes from that immutable API row's Pod UID/health owner fields
and must cross-equal history and any typed fence member. No class-5 lease lock
is taken by ordinary terminalization. They are not trusted merely because the winner
DTO repeats them. No caller supplies cause, disposition, finish time, receipt, or
terminal output as source. Handler/failure terminalization likewise verifies the
exact historical-origin list reachable from the retained cursor and strict
return; a receipt cannot bless an invented predecessor claim.

An unknown-commit retry uses the stored-adoption source: the caller's original
time-free trusted winner, the permanent typed receipt, and the terminal request
only when it still exists, plus the completed fence operation only when that
typed operation is retained. The builder hashes the caller winner into the same
typed commitment. Exact equality returns `EXACT_ADOPTED` and reuses the stored
finish time; a different internally legal commitment returns `LOST_RACE`; a
crossed receipt is corruption. After request and evidence GC the receipt alone
still distinguishes those results. Adoption never requires the service to
remain shadow, resurrects a graph, appends an event, or remints a timestamp.
Any nonnull completed-fence output is byte-equal to the stored operation; it is
never reconstructed from the receipt commitment.

Later, under the
class-1-through-10 reducer locks, the settlement builder consumes that immutable
receipt and retained terminal request, if present, to construct
`ProviderShadowSettlementCandidateV2`; the root validator reruns the raw-
journal classifier, strict return/fallback, literal S/R/U/B/Q/P0/O/X outcome, projections,
retry decision, `SETTLED` history, and optional successor graph byte-for-byte.
It also recomputes the exact canonical historical-origin set from the raw
history/return/quiescence and point-validates every supplied retained source;
`Q` cannot manufacture a predecessor effect claim.
Fixtures supply only source roots; neither candidate receipt nor settlement
projection is accepted unless the corresponding production builder reproduces
it exactly. Request GC after settlement changes only the nullable retained-
request input and cannot change the immutable receipt or projection.
The same atomic write inserts one permanent class-17 settlement commitment. It
hashes the complete original new-write source and the separate complete
settlement projection, records the operation/current/optional-successor
identities and original settle time, and is itself canonically hashed. It has no
FK to the deletable evidence graph. A store-owned, caller-unselectable
discovery/lock gate chooses mutable-graph adoption only for one complete
retained current/successor graph and chooses receipt-only adoption only for the
exact parent-wide ordinary, outgoing-Q, or incoming-Q GC proof. Both Q
components must be absent under the permanent reciprocal receipt/index
binding; no retained-peer arm exists. A partial, crossed, or unexpectedly retained graph is corruption and
cannot bypass validation through the receipt arm. Mutable-graph stored adoption
validates the receipt as well as every graph byte and returns only
`EXACT_ADOPTED_GRAPH` on complete equality. After evidence GC, receipt-only
adoption rebuilds the projection from the caller-retained original source,
hashes both, and returns only `EXACT_ADOPTED_RECEIPT` on complete commitment
equality. A different internally legal commitment returns the null-bearing
`LOST_RACE` arm; malformed/crossed bytes are corruption. A new write returns
only `NEWLY_SETTLED`. No source disposition can inhabit another success arm.
The receipt cannot reconstruct provider evidence or authorize a new successor.
For a new terminal write the parent/child/history types enforce the legal
running/request-bound/bound-or-authorized source. Receipt-only adoption needs
no mutable source descendant beyond its exact typed-GC absence proof. When the
terminal request is retained it must prove
the entire API007-defined claim triple under API008,
controller generation, lease expiry, and heartbeat were cleared. Cancellation
request/acknowledgement are both null or preserve the exact prior intent with a
nonnull acknowledgement equal to finish time; every crossed pair rejects.
The settlement projection's complete parent and child carry every stored
projection/outcome/retry/observation/effect/divergence JSON/hash pair, operation
ID, phase, and timestamp. They must equal the candidate actual/proposed
outcomes, fallback evidence, and
`SETTLED` history and are byte-compared to every column written by the atomic
reducer; a phase-only completion marker is insufficient. Settled replay accepts
only that exact complete graph, including after the request becomes absent.
The candidate projection carries actual and proposed outcomes separately and
both pairs byte-equal the completed child's corresponding columns. Actual is
derived from the strict handler return or terminal fallback and is the sole
mutation/retry authority; proposed is independently built by the frozen
comparison reducer. Their mismatch is retained as the exact bounded divergence
class and is promotion-blocking, never permission to choose the proposed state.
The stored `reduction_disposition` preserves the literal authoritative
classifier. Handler classes are `S/R/U/B/Q`; request-fallback classes are
`P0/O/S/X`. They are never normalized into one another. The following table is
the exhaustive settlement mapping; `RC` and `D` are the exact retry class and
delay from the handler `R` tuple, and "max" means
`logical_attempt == RESOURCE_ACTION_MAX_ATTEMPT_V1`:

| source | exact actual provider tuple | stored disposition | retry decision `(decision, class, delay)` | parent phase | successor |
|---|---|---|---|---|---|
| handler `S` | `S` | `S` | `(terminal, null, null)` | `COMPLETE` | none |
| handler `R`, below max | exact `R` | `R` | `(retry_same_plan, RC, D)` | `RUNNING` | `retry_same_plan` linked admission |
| handler `R`, max | same exact `R` | `R` | `(block, null, null)` | `COMPLETE` | none |
| handler `U`, below max | exact `U` | `U` | `(observe, observation_required, 60)` | `RUNNING` | `observe_same_plan` linked admission |
| handler `U`, max | same exact `U` | `U` | `(block, null, null)` | `COMPLETE` | none |
| handler `B` | exact `B` | `B` | `(block, null, null)` | `COMPLETE` | none |
| launch handler `Q` | exact `Q` | `Q` | `(terminal, null, null)` | `COMPLETE` | `partial_down` linked admission |
| fallback `P0`, below max | exact `P0` | `P0` | `(retry_same_plan, transient, 60)` | `RUNNING` | `retry_same_plan` linked admission |
| fallback `P0`, max | same exact `P0` | `P0` | `(block, null, null)` | `COMPLETE` | none |
| fallback `O`, below max | exact `O` | `O` | `(observe, observation_required, 60)` | `RUNNING` | `observe_same_plan` linked admission |
| fallback `O`, max | same exact `O` | `O` | `(block, null, null)` | `COMPLETE` | none |
| fallback `S` | exact `S` | `S` | `(terminal, null, null)` | `COMPLETE` | none |
| fallback `X` | exact `X` | `X` | `(block, null, null)` | `COMPLETE` | none |

`replan_new_generation` is unreachable in Serve039 shadow settlement. Attempt
exhaustion changes only the parent/successor/retry-decision projection; it never
rewrites the literal actual outcome or stored disposition. Proposed outcome is
comparison evidence only and cannot select any row in this table.
`post_settlement_parent` remains the exact `RUNNING` parent whenever a same-plan
retry/observation successor is created; it is the complete parent for terminal
`S`/`B`/`Q`, fallback `X`, or attempt exhaustion with no successor.
`successor_kind` is `retry_same_plan` exactly for below-max `R`/`P0`,
`observe_same_plan` exactly for below-max `U`/`O`, `partial_down` exactly for
`Q`, and null for `S`/`B`/`X` and exhausted `R`/`U`/`P0`/`O`; the successor-
projection is nonnull exactly for the three nonnull kinds and its child role/
kind/retry decision must match. Its facts never originate in that projection.
For a new settlement the outer root supplies either the source-only
`same_parent_new` arm or the source-only `partial_down_new` arm. Neither arm may
contain a completed child, settled history, post-settlement parent, target
coverage/parent, or any other value this transaction is about to write. The one
pure `project_shadow_settlement_with_successor_v2()` first derives the current
completed child, `SETTLED` history, literal outcome, and post-parent as local
values; only then may it pass that internal predecessor to the ordinary linked-
admission projector. That local value never becomes DTO evidence.

For retry/observe, the source declares the active reference, complete
preflight, full registration set/handoff fence, aligned memberships/API rows,
deterministic request input, and exact absence of every next-request key. The
combined projector emits the ordinary same-parent insertion graph. For `Q`,
the source instead declares the independently locked replica/capacity snapshot,
PREPARING target reference, complete down preflight/construction, full
authority set, deterministic target identities/input, and exact absence of
target coverage, parent, descendants, request, queue, correlation, and replica
down links. It internally derives the target PENDING_SELECTION parent, then the
final linked-admission graph, and emits one
`ProviderShadowPartialDownTargetInsertionProjectionV2` containing target
coverage, the exact replica-link/status/generation transition, final parent/
child/history/request/reference graph, and reciprocal source Q link. The
retained capacity allocation is byte-preserved, never released or reallocated
by admission.

Stored replay is disjoint. Its source supplies the caller-retained original
new-write root separately from the already-settled current graph, plus either
the complete stored same-parent successor or the complete
stored partial-down *descendant*, including immutable coverage, the current
present replica/capacity or exact post-removal absence proof, reciprocal links,
and a legal admission descendant whose
request/reference/history may already be authorized, terminal, settled,
released, or request-GCed. It never supplies the initial insertion projection
as source. The immutable target coverage/admission timestamps and source
cleanup basis reconstruct those original insertion bytes. The current service
may already be authoritative: stored replay uses only the mode-independent
historical candidate binding projected from retained coverage and cannot create
a new write. The builder hashes the caller root to distinguish its operation/
source from the stored winner, while current descendants independently prove
that winner's graph. It returns exact adoption only when caller source,
commitment, whole source/target graph, and all original database times reproduce
the candidate projection. A different internally legal caller commitment is a
lost race; a partial, crossed, or second target is corruption; no replay inserts
a missing member. Tests cover commit-before-ack followed by both target
advancement (including real replica-row removal) and source-service promotion
before exact adoption.
Once typed evidence GC removes that complete graph, the disjoint
`receipt_only_adoption` source carries the caller-retained original new-write
root and the permanent stored settlement commitment. The candidate carries the
caller-retained projection plus that byte-equal commitment. Recomputed source,
projection, terminal-history, successor-identity, operation, and time hashes
must all match; no current service/reference/request/evidence row is required.
This is exact acknowledgement recovery only, not graph resurrection.

Settlement cannot rely on an earlier source-only size pass for a newly created
graph. Before `R`, `U`, fallback `P0`/`O`, or any other legal retry writes a
successor, its sorted-lock transaction validates that complete source-only arm
and every future progress/return/fallback/outcome case for the derived child.
Before `Q` writes its normal primary-down target, the source+target sorted-union
transaction validates the complete target-construction arm in addition to the
shadow partial basis, cleanup rederivation, and complete down preflight. Only a
complete combined projection may commit source settlement plus successor graph;
not-representable or drift writes neither side. Exact lost-ack adoption uses the
stored-source arm and reruns the same projector against every committed byte.

The direct-transition root is constructed only inside the owner-fenced Serve
teardown transaction. Its validator reruns the sole production builder from
the exact locked input and requires the candidate outcome to be byte-equal; it
never accepts a proof kind, prefix, provider tuple, or outcome chosen by a
caller. The outer candidate must parse as the direct-no-effect basis and every
other outer basis rejects. The action snapshot, predecessor/current snapshots,
request-row disposition/retained terminal snapshot, prefix hashes, and one
database cancellation time cross-bind exactly as the parent direct builder
contract requires. Every materialized attempt also carries its immutable
terminal-authority selector; request-GC removes only the transient request
snapshot, never that selector. Lineage presence alone is not provider-I/O
evidence. It has no current worker/cohort field. The seven direct fixture roots are,
exactly: unmaterialized; terminal-request-unsettled at one link and maximum
count; retained-settled with a present request at one link and maximum count;
and retained-settled with a garbage-collected request at one link and maximum
count. Every materialized structural-maximum root uses terminal state
`CANCELLED`, null request return, and the corresponding longest legal fixed
error shape; `SUCCEEDED` is the equal-length terminal-state tie but sorts after
the manifest's explicit `CANCELLED` choice only as a negative/nonmaximal test,
while `FAILED` is shorter. Production transition tests separately exercise all
three legal terminal states and prove each classifies to the same structural
scenario and is no larger than its fixed maximum. A classifier derives that
scenario from the typed builder input and must match exactly one code-owned row.

The reducer-history projection is built only from the action, immediate
predecessor, current-attempt, and retained-request rows already required by the
boundary's lock program; it is not a caller-supplied history scan. Hash/null
fields are recomputed from their adjacent typed values. Its action revision and
each attempt's `request_input_sha256` are exact locked durable values, never
synthesized. A nonnull terminal-request snapshot additionally retains the
  exact execution generation, cleared null process worker, and kind-derived private
handler name; its terminal selector retains the pre-update claim worker, and
when a return exists the selector/lineage worker equals its nested terminal
attestation. At linked admission
for `next_attempt=1`, `action_current_attempt=0`, both locked attempt members,
both action-result members, `launch_no_io_prefix`, and
`supersession_quiescence` are null. For `next_attempt>1`,
`action_current_attempt=next_attempt-1`, `locked_predecessor.attempt` equals
that value, the predecessor is settled with its exact immutable outcome,
retained progress, and terminal-authority selector, and `locked_current` is
null. Its terminal-request snapshot is byte-equal when retained and null
exactly after legal settled-request GC; neither shape changes the selector or
historical authority.
At pre-I/O, `locked_current.attempt=attempt` and is byte-equal to the current
locked attempt; `locked_predecessor` is null exactly for attempt one and
otherwise names settled attempt `attempt-1` with the exact immutable values
used to materialize the current attempt. The top-level no-I/O prefix and
quiescence are null unless present in the exact action/latest or predecessor
outcome, in which case they are byte-equal. This supplies every known retry,
fallback, retained-request, no-I/O-prefix, quiescence, and historical-outcome
byte to `current` mode without loading unrelated older attempts; those older
immutable outcomes were size-gated when they were committed.

Every terminal attempt has exactly one immutable Serve039
`terminal_authority_selector`; a nonterminal attempt has none. While the API
request row exists, its terminal state/time/generation/input/handler must be
byte-equal to the selector, and its cleared process worker must be null. The
selector's stable authority-worker and process-owner IDs instead equal the
terminalizer's locked pre-update claim/lease pair and, when present, the
lineage/typed-return pair. After settled request GC, the transient
`request_terminal_snapshot` is null but the selector remains and is the sole
generation/worker/cause lookup. A `LINEAGE` selector names the exact matching
historical row; `NO_SUCCESSFUL_CLAIM_START` names none and is legal only for the
fixed claim-start rejection or any terminalization that won before successful
claim-start. Missing, crossed, mutable, or multiple selectors block reduction
and GC.

The selector is written by the generic central request-terminalization core on
the caller-owned connection, not only by the private handler. After
nonauthorizing discovery, every generation-one private terminal transaction
locks the claimed process API-instance row before request/queue and derives the
stable authority-worker ID from that row's canonical Pod UID; the route carries
no worker identity. Handler modes cross-check lineage/return, claim-start modes
cross-check their already-held lease/API context, owner-ack modes cross-check
the same-fence API mapping, and recovery modes cross-check the enclosing old-
owner lease/API evidence. Generation zero has both IDs null. A caller-supplied
stable ID or current-member lookup is never authority. A generation-zero path
whose class-15 reread observes a concurrent generation-one claim aborts without
mutation, releases, and restarts from class-14 owner discovery; it never locks
the API row backward or creates a null-owner generation-one receipt. The
opposite ordering commits either the generation-zero terminal receipt or the
generation-one claim. Its call-site
inventory covers typed completion, strict-codec failure, owner-acknowledged
kill/cancel, precondition/startup/recovery, leadership, reservation, and any
new terminal path. The expired-claim reaper is separately inventoried to skip
every claimed V2 private row and writes no selector. A terminal V2 request
without an exact selector rolls back
and fails closed. That core uses one PostgreSQL timestamp scalar for both the
request and selector finish time. Before the update it captures both the stable
authority-worker ID and process claim-owner ID in the selector whenever
execution generation is one, then clears the
complete API007-defined request claim triple under API008 plus controller-generation and heartbeat;
generation zero captures both IDs as null. Tests exercise the real API008 claim
constraint and cover handler success/failure, strict return encoding failure,
explicit cancellation, owner-quiesced lease loss, typed UID/process fences,
and generic expired-claim private skipping.

Historical authority is a closed projection, not an arbitrary list. The five-
effect schedule gives one attempt at most 13 unique keys: two origin keys per
effect plus one progress-envelope attestation, one terminal-selector, and one
typed-outcome key. The current/predecessor reduction union is at most 28 after
adding one action-last-result and one raw-invalid terminal-selector slot.
`extract_provider_resource_action_authority_keys_v2()` reads only those exact
typed slots. Every `LINEAGE` terminal selector contributes its named generation
whether or not the request row/progress remains parseable; a
`NO_SUCCESSFUL_CLAIM_START` selector contributes no key. The extractor
deduplicates by `(action_id, attempt,
request_execution_generation)`, and sorts by action UUID bytes, attempt, then
generation. Each attempt list equals its exact extractor result and the outer
`ProviderResourceActionV2ReductionAuthorityContext.historical_authority` equals
the sorted union across the bounded input. Missing, extra, duplicate, unsorted,
over-bound, crossed-request, or hash-unequal rows reject. Empty is legal for the
exact pristine nonterminal shape with null selector and no terminal/effect /
attestation evidence. Once terminal, empty is legal only when the immutable
selector proves `NO_SUCCESSFUL_CLAIM_START`, whether generation zero or the
sole generation one assigned before terminalization/claim-gate rejection; any
other named authority key requires its exact immutable row.

One primary history cannot stand for mutually exclusive locked states. The
ordered authoritative-history bank therefore contains full admission or
pre-I/O roots for every code-owned applicability-equivalence class, including
fresh attempt one, retry with its immediate settled predecessor in both
retained-request and legally garbage-collected-request shapes, and inherited
effect adoption into the later attempt's new sole-generation-one request,
maximum-attempt exhaustion,
every fallback journal class, and every distinct known prefix/quiescence shape.
`classify_provider_resource_action_history_scenario_v2()` derives the one
literal scenario from the root; it cannot rewrite history or accept a selector.
Its ordered code constant
`PROVIDER_RESOURCE_ACTION_HISTORY_SCENARIOS_V2` is a nonempty tuple of unique
literal scenario IDs. The fixture bank must classify to that tuple exactly in
order, with one root per row and no missing, duplicate, or extra root; repository
AST inventory freezes the classifier keys and tuple as one surface.
Live admission/pre-I/O evaluates only rows reachable from its exact root, and a
later retry is measured by its own later admission transaction.

Fallback `X` is valid stored output from invalid raw journal bytes, so it is
positive representability evidence rather than a malformed-output test. Its
linked-admission projector receives the normal full authoritative admission
root and first consumes
`project_provider_resource_action_post_materialization_v2()`. For each
sealed code-owned raw-invalid profile, a second production projector exact-
simulates only the reachable request-terminal/journal mutation from that post-
insert state and derives a complete
`ProviderResourceActionRawFallbackReductionInputV2`; no raw scenario is a top-
level fixture/live root. Its nested classifier input has one
authority: its action is the projected `QUEUED` action with incremented
revision/current attempt, it retains that projection's typed predecessor, and
it replaces only the projection's current snapshot with the sole raw current
attempt. The current attempt equals `action_current_attempt`; predecessor is
null exactly for attempt one and otherwise is the exact settled attempt-1; the
current request is terminal and unclaimed; action/request/kind/input hashes
and every prefix field cross-bind; and its raw progress/hash pair and terminal-
authority selector are exact. For linked-admission measurement, the outer
reduction input repeats the candidate spec/resolved cohort and constructs one
hypothetical immutable lineage plus `LINEAGE` selector for each accepted
member. Its API start/heartbeat, Pod IP, server version, boot nonce, ready-health
bytes, and `authorized_at` come only from the aligned locked snapshot and
`database_now`; compact cohort, action/kind, request/generation/worker, and input hashes
cross-bind byte-for-byte. Both accepted workers are projected independently. A
live reduction instead loads its immutable terminal selector and the exact
historical lineage it names; it never selects current accepted membership.
The journal classifier core consumes only the nested raw history, while the
real fallback reducer must consume the entire V2 reduction input through the
same explicit authority-context wrapper used by live persistence; neither may
recover a spec, cohort, or member ambiently. The production-profile test
inventory carries the exact bounded raw attempt bytes and hash. Each profile
either fails the production `ProviderLifecycleProgressV1` parser or parses
successfully and fails exactly one production hash, revision, action-context,
operation-ID, or watermark/progress invariant. The sealed profile tuple covers
every `invalid` classifier branch, with AST dispatch equality and runtime branch
instrumentation preventing an unmeasured branch. In every case the production
journal classifier returns exactly `invalid` and the real V2 fallback reducer
constructs the measured `X` outcome. The fixture cannot
name `X` or a raw profile directly. The nonempty unique
`PROVIDER_RESOURCE_ACTION_RAW_INVALID_JOURNAL_PROFILES_V2` tuple and its builder
dispatch are AST-inventoried as one surface. Linked admission evaluates all of
those profiles as future response states before request insertion; an actual
reduction still validates the exact bounded outcome before persistence.

The finite mutable-attestation domain is code-owned rather than inferred from
fixtures. `PROVIDER_AUTHORITY_ATTESTATION_SUCCESSOR_PROFILES_V2` is an ordered,
nonempty tuple with exactly these four literal profiles:
`current_before_after_null`, `current_before_after_closed`,
`maximal_mutable_before_after_null`, and
`maximal_mutable_before_after_closed`. The current profile preserves the exact
live Pod/ReplicaSet/Deployment resource versions and `observed_at`; the maximal
mutable profile substitutes only the resource-version leaves with exact legal
1,024-byte NFC text maxima. Canonical timestamps are fixed-width, so size mode
does not fabricate a future clock: it preserves the root's legal `observed_at`,
and a closed-after projector uses the boundary's exact nondecreasing database
time.
It never changes namespace/name/UID/owner, generation, template, image,
ServiceAccount, artifact, callable, or handler identity. All identity text is
already bounded by the production codecs: Kubernetes names are at most 253
UTF-8 bytes, other text/resource-version/UID leaves are at most 1,024 UTF-8
bytes, generations are signed-int64 positive integers, and timestamps have the
fixed canonical UTC representation.

For either before profile, the closed after profile is the unique legal
`ProviderAuthorityWorkerAttemptAttestationV1` successor: every identity field
is byte-equal to `before` and only `observed_at` may advance; the null profile
keeps `after=null`. Within one request execution generation no other transition
exists. Admission evaluates the current and maximal mutable profiles for each
of its two accepted members as an early optimization. Claim-start evaluates the
exact current live `before` plus both legal after states and the maximal legal
successor-before pair before persisting lineage or invoking the handler.
Handoff and cold-recovery qualification run the same candidate-member profiles
before accepting replacement membership. Immediate pre-I/O starts from the
persisted exact attestation and evaluates only its legal null/closed remainder.
The tuple, builder dispatch, and every profile consumer are one AST-inventoried
surface; no fixture may invent another before/after value or maximize an
arbitrary frozen field.

Each case ID maps to exactly one fixed-signature case projector and one
code-owned applicability predicate in the enumerator's sealed dispatch table;
they receive only the exact
`ProviderResourceActionRepresentabilityInputV2` selected by the case's
`dispatch_kind`, `action_kind`, and `boundary`, plus the enumerator-owned mode `current` or
`candidate_maximal`.
There is no artifact-supplied argument object, payload preimage, free-form
selector, artifact path, callable name, or code expression. The repository AST
inventory requires the dispatch keys and projected ordered case rows to be
identical to the applicability keys and the globally concatenated shard rows,
so adding or changing a legal variant requires a new explicit case ID and both
result rows. Thus the complete cross-dispatch case cardinality is exactly
`len(cases)` and the two result files jointly contain exactly twice that value;
the small manifest contains only refs and neither design repeats a manually
maintained number that can drift. The pure enumerator projects its actual
ordered `(case_id, dispatch_kind, action_kind, boundary, payload_kind)` tuple and
requires canonical equality with the index-ordered shard concatenation before measuring either a fixture or
live input. Applicability is derived only from the typed root. Common rows
apply at their named boundary. Handler and fallback rows apply only when the
exact kind-specific history and production journal classifier can reach them;
direct rows apply only to `owner_fenced_transition` and its one classified
legal builder-input shape. A down complete-preflight root applies exactly one cleanup-target
row: `completed_launch`, or the unique literal case returned
by `classify_provider_kubernetes_partial_cleanup_rederivation_input_v2()` after
the sole rederiver counts nonnull committed-object UIDs and the Pod's retained
server allocations, or the corresponding unique shadow-partial case returned
by the shadow-input classifier for a private-shadow `Q`. Mutually exclusive launch histories are not fabricated
from one live root, and their inapplicability is neither an error nor evidence
for that action.

The two referenced fixture inputs are descriptor-read and size/hash-verified.
The realistic fixture is evaluated only in `current` mode and the candidate-
maximal fixture only in `candidate_maximal` mode; each live boundary instead
evaluates both modes. Each contains the twenty primary roots plus four ordered
banks: the complete down-cleanup bank (completed launch, every literal legal
API006 partial-cleanup case, then every literal legal shadow-partial cleanup
case); the full authoritative-history bank; the full shadow-history bank; and
the seven direct-transition roots. Every cleanup member is a full typed preflight root and must replay the
sole cleanup rederiver and corrected down-capsule constructor byte-for-byte.
No artifact supplies a target override, history rewrite, proof-kind selector,
or hand-authored outcome. Across the twenty primary roots and all four banks, the
CI aggregator requires every global case to have at least one applicable
production input. For each case and mode it evaluates every
applicable root, rejects if any result is oversized, and emits the deterministic
maximum result by canonical byte length and then SHA-256. A missing case fails;
distinct completed/partial roots need not render byte-equal results. Golden
counts and hashes are CI evidence only; live preflight, admission, claim-start,
pre-I/O, and direct-transition checks substitute their exact typed roots and render both modes for every
applicable reachable case rather than trusting fixture byte counts. Linked
admission evaluates its own rows plus all descendant `pre_io` rows before
action-attempt or shadow-child/request insertion; immediate pre-I/O reruns the latter with the actual
claim generation, cursor, operation ID, member, and attestation. The owner-
fenced transition call runs before outcome/capacity/release writes. Across the
seven boundary kinds every case reachable by that immutable action, shadow graph, or transition is
evaluated; an unknown or empty/missing boundary slice, or an applicable
projector that cannot render, is a hard failure.
`current` preserves every known live byte; `candidate_maximal` preserves those
same bytes and substitutes only the declared finite maxima for values still
unknown at that boundary. Either oversize mode rejects.

The fully expanded case tuple contains separate payload rows, with both
`realistic` and `candidate_maximal` results, for each of these semantics:

- all phase-table rows: three `CREATE_INTENT` roles, one/two/three-slot
  `OBJECTS_PARTIAL`, `OBJECTS_EXACT` through both Pod edges, `HANDLE_INTENT`,
  `HANDLE_COMMITTED`, `RUNTIME_READY`, `JOB_INTENT`, `JOB_COMMITTED`,
  `JOB_RUNNING`, `ENDPOINT_RESOLVED`, and `SUCCEEDED`;
- all down rows: `TARGET_RESOLVED`; each legal delete-order
  `DELETE_INTENT`/`DELETE_PARTIAL` prefix; `ABSENCE_EXACT`;
  `HANDLE_REMOVE_INTENT` with exact-handle and already-absent inputs;
  `HANDLE_REMOVED` for both legal removal dispositions; and `SUCCEEDED`;
- launch and down V2 preflight request, complete response, each kind-matched
  not-representable reason, capsule, config, invocation, plan, and full action
  spec, including every eligible live worker/attempt-attestation projection;
- for each launch/down kind, the linked-admission canonical request input and,
  for each accepted member, its hypothetical claim's separately rendered
  `dispatch_membership` and `execution_authority` lineage children; the actual
  claimed-execution root's two children; and the immutable pre-I/O lineage's
  same two children. Each child has its own case row, byte count, and hash. The
  transient enclosing lineage DTO is never treated as one stored/wire JSON
  value and cannot hide an oversized child;
- for each launch/down kind, exactly the ordered
  `PROVIDER_RESOURCE_ACTION_TERMINAL_SELECTOR_PROFILES_V2`: handler-return /
  lineage / succeeded; request-failed / lineage / failed; request-cancelled /
  lineage / cancelled; claim-start-not-representable / no-successful-claim-
  start / failed; claim-reauthorization-failed / lineage / failed; and the four
  terminal-before-claim-start / no-successful-claim-start variants formed by
  `FAILED | CANCELLED` crossed only with generation-zero/both-IDs-null or
  generation-one/both-IDs-nonnull. No profile constructor accepts a free cause,
  disposition, state, generation, or worker. The tuple, applicability map,
  constructor dispatch, exhaustive parent state table, AST inventory, and both
  golden modes are one surface; an illegal pair is a negative parser test, not
  a representability row. These profiles are measured at linked admission and
  revalidated at reduction/direct transition before persistence or GC;
- for each private-shadow launch/down kind, the represented parent, primary
  child/invocation, canonical request input, `BOUND` history, and both accepted
  workers' hypothetical dispatch-membership, authority-proof, lineage-hash, and
  `AUTHORIZED` history children at linked admission; the actual selected
  worker's same children at claim-start and pre-I/O. Empty attempt-one and exact
  inherited-retry-seed histories are separate scenarios. Parent, child, and
  history row projections are typed scenario inputs only, never enclosing
  payload-result rows. Their actual spec/plan/invocation, membership, authority,
  progress, return, receipt, fallback, outcome, retry, observation, and effect-
  trace JSON leaves are each independently bounded and hashed. Every completed
  parent additionally emits two distinct `shadow_projection` payload rows: its
  stored actual/legacy projection and its stored proposed projection. The
  aggregate SQL join is never a payload row;
- every shadow launch/down phase and origin/evidence/no-effect substitution,
  including same-claim commit/adoption, later logical-attempt adoption, every
  E-only and legal `E* + N<i>` quiescence, handler `S` and every reachable
  `R`/`U`/`B`, launch-only `Q`, fallback `P0`/`O`/`S`/`X`, and maximum logical-
  attempt blocking. `Q` additionally measures the normalized successor edge,
  `ServeShadowPartialLaunchCleanupBasisV1`, shadow cleanup rederivation input,
  and the complete normal `PRIMARY_DOWN` preflight/admission graph; it never
  invents `LAUNCH_CLEANUP_DOWN`;
- the complete private-shadow terminal-receipt matrix: handler return;
  request failed; owner-acknowledged cancellation; owner-quiesced lease loss
  with absent/present prior intent; claim-start not representable; terminal-
  before-claim-start; and each stale-owner/cold-recovery/process-supersession
  fence, including mixed action+shadow batches at one and maximum inventory,
  with
  generation-zero/null-owner and generation-one/exact-owner shapes only where
  the parent matrix permits them, `SHADOW_EXECUTION` plus nonnull lineage exactly
  for an authorized history, and `NO_SUCCESSFUL_CLAIM_START` plus null lineage
  exactly before authorization. Each receipt has a separate
  `shadow_terminal_commitment` case containing the acyclic time-free fence
  commitment projection derived from the preterminal source;
  tests prove the completed fence operation never enters its own hash domain.
  Request-present, request-GCed, and later evidence-GCed exact adoption plus a
  different legal lost-race commitment are separate terminalization cases. Each
  receipt is paired with the strict handler return or raw-journal fallback, the resulting `ServeShadowCandidateOutcomeV1`,
  final execution-history `SETTLED` projection, retry decision, child `COMPLETE`
  projection, permanent `shadow_settlement_commitment`, and request-present/
  request-GC/evidence-GC retention shape;
- the resolved cohort, each eligible worker identity and attempt attestation,
  native V2 renderer input, each of the three rendered/validated request
  bodies, and every completed or legal partial cleanup target used by those
  enclosing rows;
- `call_not_entered` for sequences 0-4; CoreV1 422 for failing sequences 0-2;
  the indivisible cluster-row `rolled_back + not_found` tuple and the one typed
  `conflict_no_write + different_identity_conflict`/different-UUID exact
  handle; one Skylet `schema_rejected`/NotFound row plus same-key conflict in
  each of the `SUCCEEDED`, `FAILED`, and `BLOCKED` retained job states;
- reducer quiescence for every nonsuccessful phase-table row, including every
  E-only post-effect/read-only row and every legal `E* + N<i>` intent row, plus
  same-claim created/submitted/inserted commits, same-claim adoption, later-
  attempt adoption, and the explicit new-request generation zero-to-one reset
  across attempts;
- the parent's complete handler/outcome cross-field cases: domain success `S`;
  every revision-zero, nonintent, and current-intent error-category mapping to
  `R`/`U`/`B`, including maximal bounded code/message/retry leaves;
  supersession `Q` for E-only and E+N; the unique count-zero unmaterialized
  direct `CANCELLED_NO_EFFECT` basis plus the positive one-link and maximum-
  count terminal-request-unsettled and retained-settled bases, including
  retained request present/GC, all owned by `owner_fenced_transition`; request-
  terminal fallback `P0`, `O`, external-cursor `S`, and corruption `X`, for
  each compatible terminal request state and missing-return/fixed-failure reason;
  malformed nonnull return is a negative codec/quarantine case, not a fallback row.
  Direct fixtures cover only those legal basis/prefix pairs, immutable
  historical attempt outcomes, and the exact null revision-zero journal.
  Maximum-attempt fixtures
  also cover the parent's exact exhaustion reduction for handler `R`/`U` and
  fallback `P0`/`O`, including no max-plus-one request. Direct-teardown
  precedence is transition coverage over the same maximum-count direct
  outcome bytes, not a duplicate representability payload row.

The enumerator is not allowed to hand-author a value that the live reducer
cannot produce. Before freezing this tuple, the production progress module
must implement the parent-specified
`ServeReplicaActionDirectNoEffectCancellationV1`, direct-cancellation outcome,
and one closed authoritative outer outcome parser discriminating
handler/direct/fallback bases. The direct builder alone constructs the count-zero, appended, or
retained prefix under the exact proof variant and enforces direct-teardown
precedence. The existing pre-038 provider-result-only shadow codec remains
readable historical evidence, not an alias for this outer parser. Private P3
shadow remains disabled until the specified parent/child/history/progress,
terminalization, S/R/U/B/Q plus P0/O/S/X settlement, and same-inventory boundary
family are implemented and verified against actual shadow rows.

The same production module must support V2 immutable specs through the exact
`ProviderResourceActionV2ReductionAuthorityContext`, containing the stored V2
spec, resolved V2 cohort, and only the immutable historical-authority rows named
by the bounded progress/outcome/terminal slice. Frozen V1
parsing/validation remains unchanged. V2 reduction
validates the compact capsule reference against that context and compares each
V1-shaped API006 attestation with the stable worker projection frozen in its
named lineage row; it
never falls through the V1 spec parser or resolves a cohort ambiently. Public
V1 reducer wrappers and V2 authority-context wrappers share the same pure
reducer cores. The response projectors call only these production DTOs,
progress/no-effect builders, and reducer cores. They preserve every locked
root byte and synthesize only bounded future response leaves; every projected
value must round-trip its production parser.

The `journal_class=invalid` fallback is the one deliberate ordering exception
to attestation comparison: its V2 wrapper first validates the stored spec's
compact cohort reference against the exact resolved cohort, binds the terminal
selector execution generation and worker ID to its immutable lineage row when
one exists, and classifies the raw
journal before attempting domain progress parsing. Because invalid bytes may
contain no parseable attestation, that branch must not require a stable
attestation projection. Every non-invalid V2 journal continues to require the
normal V1-shaped-attestation-to-historical-lineage comparison. The linked-admission `X`
projector passes the full admission root, candidate lineage/context, and derived
post-materialization raw history through this same production wrapper.

All seven direct-cancellation payloads are owned by
`owner_fenced_transition` and measured from the exact locked production-builder
input before the one atomic outcome/capacity/release write. They are not
worker admission or pre-I/O descendants. A separate maximum-attempt direct-
precedence row is forbidden because it would serialize the same payload.
Likewise, the no-effect tuple has exactly five call-not-entered, three CoreV1
422, two cluster-row, and four Skylet semantic rows; splitting one literal
proof tuple or crossing schema rejection with an inapplicable durable job state
does not create a renderable case.

Only canonically valid renderable values appear in `cases`; every row therefore
has a byte count and hash. Negative parser/transition coverage--including
`SUCCEEDED` quiescence, E-only-with-N, intent-without-N, wrong-claim N,
retry-local resolution of an inherited intent, invalid handler/result cross-
combinations, immediate-link tamper, and crossed-predecessor inputs--remains an
exhaustive test inventory but is not miscounted as a representability payload.

The previously generated 375-row intermediate is rejected: it contains five
invalid direct basis/prefix Cartesian rows, three invalidly split/crossed no-
effect rows, a duplicate direct-precedence payload, and assumes one down root
can represent all mutually exclusive cleanup histories. It also fabricates
mutually exclusive reducer histories from one root, maps direct teardown onto
worker boundaries, and hand-authors fallback `X` without the raw-invalid
production classifier. Removing its known duplicate/impossible rows would
yield the current provisional 366 rows only before the required generated-byte
audit of the phase/E-only/E+N quiescence families. The accepted provisional
index records that derived count and exact `0..182` / `183..365` shard ranges;
it is not authority evidence because only three of seven boundary families are
implemented. No cardinality is final or may be copied as a separately
maintained assertion into a callable inventory, static manifest, fixture, or
golden. Final index/shard counts must be regenerated from the exact audited
production rows and this design re-reviewed if they change.

The `candidate_maximal` fixture is not a synthetic protocol-wide fill of every
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

The current rebased PR head does not yet contain the final V2 case index/two
shards, CI golden manifest/two result files/fixture set, or complete production
enumerator. The preserved continuation checkpoint has a provisional 366-row
set and only three of seven boundary families implemented. The existing
full-spec/preflight-envelope goldens and those partial rows are inputs to this
work, not a substitute for the fully expanded inventory and post-inventory
evidence above.
Because the native V2 config-access reference and V2 static-manifest inventory
change capsule and preflight bytes, final post-cutover goldens must be
regenerated and must still pass the unchanged 60,000-byte outer and
65,536-byte per-value budgets. Existing hashes cannot be relabeled as final V2
qualification evidence.
Authority therefore remains disabled until the cohort-bound case index and
both shards, the acyclic CI-only golden manifest, two result files, and two
exact fixture preimages, the
inventoried enumerator, and the complete-preflight, authoritative-admission,
immediate-pre-I/O, and owner-fenced-transition calls land and pass together.

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

In legacy-controller shadow, the expected cluster-record UUID is carried
through the real `sdk.down()` request, `core.down()`, and legacy backend
teardown. In selected Serve039 private shadow and in authority, the respective
private handler supplies it directly to the extracted fail-closed cluster
row/session seam. Every arm reloads the cluster row and checks the UUID after acquiring
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

Shadow has exactly one mutation owner per candidate. Before private selection,
the existing legacy launch/down thread mutates and the durable projection only
observes. For a represented Serve039 private candidate, the private handler is
the sole mutator and the legacy call is suppressed. The two arms are disjoint;
neither candidate performs both mutations.

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

Before the candidate window starts, the narrow represented path is routed
through the same pure object renderer and prebooted-runtime/Skylet seam. The
existing SafeThread remains the sole decision/admission/request owner and waits
for one real PR #1070 request; its claimed private
`serve_shadow_candidate_launch/down` handler is the sole provider-effect owner.
No SDK or parallel mutation path runs. Only
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
it is not a separately admitted logical down action. A retained legacy-
controller child stores `planned_execution_kind` exactly `api_request |
legacy_direct_down`. A represented primary child instead stores exactly
`private_api_request`, has one same-key Serve039 execution-history row, and uses
the kind-matched private handler; cleanup can never use that value. Every child
also stores the real request ID when returned, actual/proposed outcomes, retry
decision, pre/post observation, provider correlation, and bounded divergence.

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
`ResolvedProviderTargetV1`, lock/read the same-UUID global-user-state cluster
row and its full `ProviderKubernetesHandleV1` in class 2, and copy both typed
preimages and their hashes into `PriorLaunchBasisV1`; missing, hash-only, or
name-only evidence is not eligible.

## Eligibility and activation

Before a provider lifecycle profile is considered, Serve must project this
closed capacity prerequisite from the persisted service version, effective task
resources, and live replica rows:

```text
ServeActionCapacityProfileV1 = {
  version: 1,
  profile: "ordinary_ondemand_physical_width1_v1",
  pool: false,
  replica_unit: "physical_backend",
  planned_capacity: 1,
  node_count: 1,
  use_spot: false,
  accelerator: null,
  spot_placer: null,
  reserved_capacity_fill: false,
  cost_rebalance: false,
  dynamic_ondemand_fallback: false,
  base_ondemand_fallback_replicas: 0
}
```

An absent dynamic on-demand fallback normalizes to false and an absent base
fallback normalizes to zero. The predicate additionally requires every live or
provisionally admitted replica, and the immutable service-version spec that
created it, to project the same profile. Each such row has
exact-Boolean `ReplicaInfo.is_spot=false`, `planned_capacity=1`,
`reserved_fill=false`, `is_zero_cost=false`,
`paid_capacity_pool_key=null`,
`cost_rebalance_for_replica_id=null`, and
`unknown_capacity_replacement=false`. Paid placement has no independent
service flag, so `spot_placer=null` is the mandatory paid-capacity exclusion;
checking only `cost_rebalance=false` is insufficient. The physical replica-unit
check is independent of the scalar width check, so a capacity-aware logical
configuration remains ineligible even when its current GPU width happens to be
one.

This is a Serve admission prerequisite, not a fact inferred from successful
provider normalization and not a new field in `ProviderLifecyclePlanV1`.
`PrivateShadowActivationProofV2` and `AuthoritativePromotionProofV2` bind its
canonical bytes and the parent design's complete
`ServeServiceVersionSpecIdentityV1` for the elected version and each live
replica's creating version. Missing, null, non-Boolean, or true `is_spot` is
ineligible even when the current service version says `use_spot=false`. Serve checks it before
creating a `PREPARING` reference and rechecks it under the service lock
immediately before private-shadow or authoritative admission. A service update
that would leave the profile is rejected before version/spec commit while the
service is in `shadow` or `authoritative`; there is no silent demotion and no
legacy provider fallback after authority. A capacity-ineligible service remains
explicitly legacy and creates no preparation, reference, coverage, private
request, or action row. Paid-capacity, reserved-fill, cost-rebalance,
spot/fallback, accelerator, multi-node, and logical-width authority require a
later named capacity profile, a separately reviewed transaction/lock contract,
and a provider-profile update where the execution shape changes.

The parent design also owns the exact trust-source preimage below. This facet
constructs no weaker provider-local substitute:

```text
QualifiedResourceActionRolePodTemplateV1 = {
  version: 1,
  contract: "qualified_resource_action_role_pod_template_v1",
  template_json: Text
      # exact compact UTF-8 canonical JSON for the normalized raw
      # apps/v1 Deployment.spec.template value
}

QualifiedResourceActionRoleDeploymentV1 = {
  version: 1,
  role: "api" | "ordinary-executor" | "controller",
  namespace: Text,
  deployment_name: Text,
  deployment_uid: Text,
  generation: PositiveInteger,
  observed_generation: PositiveInteger,  # exactly generation
  desired_replicas: PositiveInteger,
  updated_replicas: PositiveInteger,      # exactly desired_replicas
  ready_replicas: PositiveInteger,        # exactly desired_replicas
  available_replicas: PositiveInteger,    # exactly desired_replicas
  unavailable_replicas: 0,
  pod_template: QualifiedResourceActionRolePodTemplateV1,
  pod_template_sha256: Sha256,
  oci_manifest_digest: "sha256:" + 64LowerHex,
  source_commit: 40LowerHex,
  artifact_inventory_sha256: Sha256
}

ResourceActionDeploymentInventoryV1 = {
  version: 1,
  contract: "resource_action_deployment_inventory_v1",
  deployments: [QualifiedResourceActionRoleDeploymentV1]
      # exactly API, ordinary-executor, controller in role order
}

ResourceActionRequiredCrashCanaryInventoryV1 = {
  version: 1,
  contract: "resource_action_crash_canary_inventory_v1",
  requirements: [{sequence: 1..20,
                  boundary_id: ResourceActionCrashCanaryBoundaryV1}]
      # exact ordered closed enum from parent fault categories 1..20
}

ResourceActionCandidateBindingV2 = {
  version: 2,
  qualification_policy_sha256: Sha256,
  schema_heads: AuthoritySchemaHeadsV2,
  deployment_inventory: ResourceActionDeploymentInventoryV1,
  deployment_inventory_sha256: Sha256,
  deployment_selection: ApprovedAuthorityDeploymentSelectionV1,
  deployment_selection_sha256: Sha256,
  selected_cohort: ApprovedAuthorityCohortArtifactV1,
  selected_cohort_sha256: Sha256,
  capacity_profile: ServeActionCapacityProfileV1,
  capacity_profile_sha256: Sha256,
  elected_version_identity: ServeServiceVersionSpecIdentityV1,
  elected_version_identity_sha256: Sha256,
  live_replica_identity_inventory: HashedCanonicalObjectV1,
  required_crash_canary_inventory:
      ResourceActionRequiredCrashCanaryInventoryV1,
  required_crash_canary_inventory_sha256: Sha256
}
```

The Pod-template wrapper is not an opaque caller hash. It canonically embeds
the normalized raw JSON at the live `apps/v1` Deployment's `/spec/template`:
exact `metadata/spec` top-level fields, only labels/annotations and optional
null `creationTimestamp` in metadata, null/absent annotations normalized to an
empty object, no controller-owned `pod-template-hash`, and the complete Pod spec.
The tree uses exact JSON types/NFC text/signed-int64 integers, bounded depth and
member count, no duplicate keys or aliases, and globally unique container names.
The parent design defines its exact canonical encoding and wrapper-hash rule;
YAML, typed-client defaults, ReplicaSet transforms, and Pod runtime fields are
not interchangeable preimages.

The deployment builder proves every current ready Pod has the role's exact
policy-approved artifact, but excludes mutable Pod names and UIDs from the
binding so same-template replacement does not reset qualification. Deployment
UID/generation/template/status, the exact selected cohort static seed/handler/
claim contracts, elected and live-replica version/capacity identities, and the
20-boundary checked-in crash requirement artifact are bound. Its repository
bytes use the existing exact canonical-payload-plus-one-LF artifact convention;
the typed canonical payload hash is bound. Every redundant hash is recomputed
and the candidate object's canonical SHA-256 is
`qualification_binding_sha256`. Missing policy projection or any partial,
drifted, mutable/tag-only, or caller-provided preimage fails closed before this
facet can prepare or dispatch provider work.

Delegated hashed inventories use the parent contract's strict recursive JSON
validator, including exact scalar/container types, signed-int64 integers,
depth/member bounds, and rejection of aliases and cycles. Candidate construction
retains an immutable canonical snapshot of its live-replica inventory;
serialization and every policy validation revalidate and compare against that
snapshot, so mutation rejects even after recomputing the delegated inner hash.

The exact policy payload is a qualification-time artifact because its final
M4 commit/digests do not yet exist. The chart defaults to an empty reference,
no projection, and closed loader failure; a later reviewed qualification change
adds exact canonical bytes and repo path/size/hash without itself activating
authority. The chart reserves the policy annotation names, volume name, and
the fixed path's complete normalized mount ancestry even while empty. Mount
paths are POSIX-cleaned before exact/ancestor/descendant comparison, including
trailing/repeated slashes and dot/parent segments, so configurable Pod extras
cannot pre-stage a competing trust source.
Old `--reuse-values` releases may lack either the whole `resourceActions` object
or only the new `qualificationPolicy` key; those two absent states alone resolve
to the exact empty path/zero-size/empty-hash triple and render no projection.
An explicit null/non-object, partial object, extra key, or drifted nonempty
reference still rejects, so backward compatibility never supplies authority.
The typed contracts, crash golden, exact loader, and projection boundary are
implemented prerequisites only. No provider preflight, admission, transition,
promotion, dispatch, or provider I/O consumes them in this tranche.

“No paid/reserved DML” applies only to M4 eligibility, proof, action admission,
and action reduction. An excluded decision releases every service/version/
replica SQL lock before it calls `LegacyServeReplicaMutationAdapter`; that
adapter retains the existing paid-capacity/reserved-fill DML and lock protocol.
The handoff and a concurrent capacity-lock holder are required race tests.

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
- no provider-specific work queue, action-execution lease, due scanner, or
  retry loop; a claimless registration-liveness lease may not schedule work.

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
deployed. The contract below is the first deployable
`ProviderLifecycleInvocationV1` inner wire shape and replaces that scaffold in
place. It remains byte-frozen inside historical `ServeReplicaActionSpecV1`.
The live M4 wrapper uses the separately versioned
`ProviderLifecycleInvocationV2` graph defined below; there is no
version-dispatching execution reader, backfill, or optional-execution-config
form.

The original P2a first-deployment gate reached API007/Serve034 and remains
historical evidence. The rebased M4 pre-migration gate keeps every API, worker,
and controller on one proven baseline image while the additive migrations
reach exact API008, Serve037, and global-user-state 028; it exact-reflects the
frozen Serve033/034 action catalog plus the unrelated Serve035 reserved-fill
and Serve036 controller-configuration additions and the Serve037 placement-
normalization/retirement tables, columns, PostgreSQL checks, and foreign keys.
In one consistent read-only PostgreSQL
snapshot it then requires zero Serve replica rows in `api_resource_actions`,
their attempts and correlated `api_requests`; zero rows in all six Serve033
sample, represented-attempt, coverage, coverage-attempt, worker-cohort, and
worker-cohort-reference tables; zero replica action/sample/coverage links; and
no service mode other than `legacy`. The pinned baseline cannot race this
snapshot because it has no resource-action writer. The exact query output,
schema heads, and baseline digest are retained as rollout evidence before API
rollout begins.

An unexpected/missing revision or table, nonzero row/link, nonlegacy service,
or mixed writer image aborts rollout before a new closed invocation reader or
writer runs.
Application code never rewrites, reinterprets, or purges an old row. If one is
found, the closed current invocation V1 remains frozen and the V2 action
envelope rejects it; the feature requires a separately versioned invocation or
reviewed offline canonical migration. A new invocation shape cannot masquerade
as V1. An encountered flattened row fails the closed parser and blocks
promotion/recovery. Canonical invocation, plan, and action-wrapper hashes and
their golden fixtures change; deterministic launch/down action UUIDs do not,
because they derive only from logical resource identity and action kind.

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
`ServeReplicaActionSpecV2.launch_cleanup_down_invocation(self)`. The method
first requires `ParentSpec.invocation` to be a primary launch and receives no
replacement target, workspace, basis, config, or other argument.
The byte-frozen V1 helper remains available only to the exact-034 historical
reader; no live M4 cleanup calls it or accepts its parent as provider authority.

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
  qualification_artifact: ProviderProjectedQualificationArtifactRefV1
}

ProviderProjectedQualificationArtifactRefV1 = {
  source: "helm_chart_configmap_v1",
  repo_path: "charts/skypilot/files/resource-action-qualifications/" + Text,
  mount_path:
    "/etc/skypilot/resource-action-authority/qualification.json",
  byte_size: PositiveInteger,
  sha256: Sha256
}

ProviderOCIImageQualificationArtifactV1 = {
  version: 1,
  requested_reference: DigestPinnedOCIReference,
  oci_manifest_digest: "sha256:" + 64LowerHex,
  oci_config_digest: "sha256:" + 64LowerHex,
  source_commit: 40LowerHex,
  platform: "linux/amd64"
}

ProviderRuntimeImageIdentityV1 = {
  raw_image_id: Text,
  runtime_image_id_scheme:
    "containerd" | "cri-o" | "docker-pullable" | "oci-reference",
  runtime_image_id_digest: "sha256:" + 64LowerHex,
  qualified_oci_manifest_digest: "sha256:" + 64LowerHex,
  qualified_oci_config_digest: "sha256:" + 64LowerHex,
  qualification_artifact_sha256: Sha256,
  runtime_id_contract:
    "qualified_oci_config_digest_v1" |
    "qualified_oci_manifest_digest_v1"
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

Runtime-image observation is a closed discriminated union, not an assumption
that manifest and config digests are interchangeable. `containerd://sha256:...`
and `cri-o://sha256:...` use
`qualified_oci_config_digest_v1` and must equal the qualification's OCI config
digest. `docker-pullable://<canonical-reference>@sha256:...` and the bare
canonical digest-pinned reference reported by current EKS use
`qualified_oci_manifest_digest_v1` (the latter has scheme `oci-reference`) and
must equal the qualification's OCI manifest digest. The checked-in
qualification artifact binds that manifest digest to the distinct config
digest. No parser may relabel a manifest digest as a config digest.

The qualification artifact is intentionally not an image-local repository
artifact. It is canonical JSON checked into the follow-up chart, packaged by
Helm, rendered byte-for-byte into an immutable cohort ConfigMap, and mounted at
the one fixed path above. The manifest and qualification keys are each mounted
as an individual read-only `subPath` file from immutable ConfigMaps; they
therefore appear as regular bind-mounted files rather than kubelet atomic-writer
symlink chains, and immutability means update propagation is neither expected
nor relied upon. The already-built candidate contains the verifier,
not its own future evidence. At startup the verifier opens the projected file
without following a caller-selected path, requires its exact size/hash and
canonical bytes plus exactly one final LF, parses exactly
`ProviderOCIImageQualificationArtifactV1`, and requires the document's requested reference, manifest
digest, config digest, source commit, and `linux/amd64` platform to equal the
manifest and running image qualification. `source_commit` must equal the
40-lowercase-hex release-build `sky.__commit__`; a template, unknown, `-dirty`,
short, uppercase, or otherwise nonrelease value fails bootstrap. The other three cohort references
remain installed-package `ProviderRepoArtifactRefV1` values and are resolved
descriptor-safely beneath the fixed package root. A manifest cannot substitute
one source kind for the other.

Live evidence on 2026-08-02 confirms why both variants are required. Revision
76 Pods reported raw `imageID`
`361913687221.dkr.ecr.us-east-1.amazonaws.com/skypilot-ha@sha256:b21f0e7cc39f62a21bc5887406f941d0b298d8fc277f0b5abb8b1f170c88b198`,
while registry inspection reported the distinct OCI config digest
`sha256:e1ea2aa540a0247c855de513a84a56d73eae9f40afaaad69ffcc52757e8061b6`.
That is an `oci-reference`/manifest observation, not config-digest evidence.

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

ProviderAuthorityWorkerPodTemplateReleaseInputsV1 = {
  version: 1,
  namespace: Text,
  helm_full_name: Text,
  cohort_suffix: DnsLabelMax42,
  cohort_id: Text,
  deployment_name: Text,
  service_account_name: Text,
  container_name: "skypilot-authority-worker",
  image: DigestPinnedOCIReference,
  image_pull_policy: "Always",
  command: [Text],
  args: [Text],
  health_port: DecimalPortText,
  preflight_port: DecimalPortText,
  manifest_config_map: {name: Text, key: "manifest.json",
                        mount_path:
                          "/etc/skypilot/resource-action-authority/manifest.json"},
  qualification_config_map: {name: Text, key: "qualification.json",
                             mount_path:
                               "/etc/skypilot/resource-action-authority/qualification.json"},
  auth_secret: {name: Text, key: Text,
                mount_path:
                  "/etc/skypilot/resource-action-authority/auth/tokens"},
  tls_secret: {name: Text, cert_key: Text, private_key_key: Text,
               ca_key: Text,
               cert_path:
                 "/etc/skypilot/resource-action-authority/tls/tls.crt",
               private_key_path:
                 "/etc/skypilot/resource-action-authority/tls/tls.key",
               ca_path:
                 "/etc/skypilot/resource-action-authority/tls/ca.crt"},
  database_secret: {name: Text, key: Text},
  downward_api_fields: [  # exact order
    {env: "SKYPILOT_POD_NAME", field_path: "metadata.name"},
    {env: "SKYPILOT_POD_NAMESPACE", field_path: "metadata.namespace"},
    {env: "SKYPILOT_POD_UID", field_path: "metadata.uid"}
  ],
  literal_env: [                                  # exact ascending closed set
    {name: "SKYPILOT_API_REQUEST_BACKEND", value: "postgres"},
    {name: "SKYPILOT_API_SERVER_ROLE", value: "authority-worker"},
    {name: "SKYPILOT_RELEASE_NAME", value: Text},
    {name: "SKYPILOT_RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE",
     value: "/etc/skypilot/resource-action-authority/auth/tokens"},
    {name: "SKYPILOT_STATE_DB_MIGRATION_MODE", value: "verify"}
  ],
  secret_env: [                                   # exact closed set
    {name: "SKYPILOT_DB_CONNECTION_URI", secret_name: Text, key: Text}
  ],
  resources: CanonicalKubernetesResourceRequirementsV1,
  image_pull_secrets: [Text],
  pod_labels: [{key: Text, value: Text}],
  pod_annotations_without_manifest_hash: [{key: Text, value: Text}],
  pod_security_context: CanonicalKubernetesPodSecurityContextV1,
  container_security_context: CanonicalKubernetesContainerSecurityContextV1,
  node_selector: [{key: Text, value: Text}],
  affinity: CanonicalKubernetesAffinityV1 | null,
  tolerations: [CanonicalKubernetesTolerationV1],
  topology_spread_constraints:
      [CanonicalKubernetesTopologySpreadConstraintV1],
  priority_class_name: Text | null,
  runtime_class_name: Text | null,
  scheduler_name: Text | null,
  termination_grace_period_seconds: PositiveInteger
}

ProviderAuthorityWorkerPodTemplateBindingV1 = {
  version: 1,
  contract: "authority_worker_pod_template_v1",
  projector_artifact_sha256: Sha256,
  release_inputs: ProviderAuthorityWorkerPodTemplateReleaseInputsV1,
  expected_template_sha256: Sha256,
  manifest_hash_annotation_json_pointer:
      "/metadata/annotations/skypilot.co~1resource-action-manifest-sha256",
  manifest_hash_placeholder: "$MANIFEST_SHA256"
}

ProviderAuthorityWorkerRuntimeCapacityV1 = {
  version: 1,
  worker_process_count: PositiveInteger,          # 1..16
  supervisor_sync_engine_namespaces:
      ["api-requests-control", "authority-preflight", "shared"],
  supervisor_max_connections_per_namespace: 1,
  supervisor_persistent_connection_budget: 3,
  child_sync_engine_namespaces: ["api-requests-control", "shared"],
  child_max_connections_per_namespace: 1,
  child_persistent_connection_budget: 2,
  pod_persistent_connection_ceiling: PositiveInteger
      # exactly 3 + 2 * worker_process_count
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
  pod_template_binding: ProviderAuthorityWorkerPodTemplateBindingV1,
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

# Additive M4 types; every unlisted key and field type is byte-for-byte the V1
# leaf above. V1 remains cleanup/history-only and is never reinterpreted.
ProviderAuthorityWorkerPodTemplateReleaseInputsV2 =
    ProviderAuthorityWorkerPodTemplateReleaseInputsV1 with exactly {
  version: 2,
  command: ["tini", "--"],
  args: ["python", "-m", "sky.server.server", "--deploy", "--host",
         "0.0.0.0", "--role", "authority-worker", "--role-health-port",
         health_port, "--authority-preflight-port", preflight_port],
  downward_api_fields: [
    {env: "SKYPILOT_POD_NAME", field_path: "metadata.name"},
    {env: "SKYPILOT_POD_NAMESPACE", field_path: "metadata.namespace"},
    {env: "SKYPILOT_POD_UID", field_path: "metadata.uid"},
    {env: "POD_IP", field_path: "status.podIP"}
  ],
  runtime_capacity: ProviderAuthorityWorkerRuntimeCapacityV1,
  runtime_capacity_sha256: Sha256,
  literal_env: [  # exact ascending closed V1 set plus these six bindings
    {name: "SKYPILOT_API_REQUEST_BACKEND", value: "postgres"},
    {name: "SKYPILOT_API_SERVER_ROLE", value: "authority-worker"},
    {name: "SKYPILOT_AUTHORITY_CHILD_MAX_CONNECTIONS_PER_NAMESPACE",
     value: DecimalIntegerText},
    {name: "SKYPILOT_AUTHORITY_CHILD_PERSISTENT_CONNECTION_BUDGET",
     value: DecimalIntegerText},
    {name: "SKYPILOT_AUTHORITY_POD_CONNECTION_CEILING",
     value: DecimalIntegerText},
    {name: "SKYPILOT_AUTHORITY_PROCESS_COUNT", value: DecimalIntegerText},
    {name: "SKYPILOT_AUTHORITY_SUPERVISOR_MAX_CONNECTIONS_PER_NAMESPACE",
     value: DecimalIntegerText},
    {name: "SKYPILOT_AUTHORITY_SUPERVISOR_PERSISTENT_CONNECTION_BUDGET",
     value: DecimalIntegerText},
    {name: "SKYPILOT_RELEASE_NAME", value: Text},
    {name: "SKYPILOT_RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE",
     value: "/etc/skypilot/resource-action-authority/auth/tokens"},
    {name: "SKYPILOT_STATE_DB_MIGRATION_MODE", value: "verify"}
  ]
}

ProviderAuthorityWorkerPodTemplateBindingV2 = {
  version: 2,
  contract: "authority_worker_pod_template_v2",
  projector_artifact_sha256: Sha256,
  release_inputs: ProviderAuthorityWorkerPodTemplateReleaseInputsV2,
  expected_template_sha256: Sha256,
  manifest_hash_annotation_json_pointer:
      "/metadata/annotations/skypilot.co~1resource-action-manifest-sha256",
  manifest_hash_placeholder: "$MANIFEST_SHA256"
}

ProviderAuthorityWorkerCohortManifestV2 = {
  version: 2,
  cohort_id: Text,
  namespace: Text,
  deployment_name: Text,
  service_account_name: Text,
  container_name: "skypilot-authority-worker",
  image: ProviderOCIImageQualificationV1,
  pod_template_contract: ProviderRepoArtifactRefV1,
  pod_template_binding: ProviderAuthorityWorkerPodTemplateBindingV2,
  artifact_inventory: ProviderRepoArtifactRefV1,
  callable_inventory: ProviderRepoArtifactRefV1,
  runtime_capacity: ProviderAuthorityWorkerRuntimeCapacityV1,
  runtime_capacity_sha256: Sha256,
  claim_contract: "frozen_action_cohort_join_v2",
  handler_allowlist: ["serve_shadow_candidate_launch",
                      "serve_shadow_candidate_down",
                      "serve_resource_action_launch",
                      "serve_resource_action_down"]
}

ProviderAuthorityWorkerCohortV2 = {
  version: 2,
  manifest: ProviderAuthorityWorkerCohortManifestV2,
  manifest_sha256: Sha256,
  deployment_uid: Text,
  service_account_uid: Text
}

The V2 manifest requires the exact new V2 Pod-template projector artifact and
binding; a V1 projector/binding or a V2 top-level wrapper around V1 release
inputs rejects. The manifest and release-input runtime-capacity objects and
hashes are byte-equal. Their six environment values are the canonical decimal
renderings of `N`, supervisor per-namespace limit `1`, supervisor derived budget
`3`, child per-namespace limit `1`, child derived budget `2`, and `3 + 2*N`;
every other value, missing/extra
entry, or `N` outside 1..16 rejects at chart rendering, manifest load, Pod-
template projection, bootstrap, and readiness. No runtime default or CPU-based
pool sizing exists. Its fixed `tini` parent makes Python termination end the main
container process, and the fourth downward field is the sole source of the
authority API row's `pod_ip`. The projector, chart, release preflight, static
manifest, live Pod-template normalizer, and post-build OCI qualification all
compare these exact V2 bytes before Serve039 owner binding or readiness. The
supervisor installs an immutable synchronous-engine allowlist of exactly
`api-requests-control`, `authority-preflight`, and normalized `shared`; each
spawned child independently installs exactly `api-requests-control` and
`shared`. Both call `db_utils.set_max_connections(1)` before any engine exists,
the preflight special factory remains `pool_size=1, max_overflow=0`, and a
process-local `db_utils.get_engine()` guard rejects an unlisted sync namespace,
any async engine, a changed limit, or late policy installation. Readiness
introspects all cache keys and QueuePools and rejects a pool whose size is not
one or whose overflow is nonzero. Real-PostgreSQL qualification holds every
allowed checkout simultaneously and observes exactly the persistent high-water
ceiling `3 + 2*N` at both `N=1` and `N=16`; hidden namespaces and a second
checkout are negative tests. Transient startup/migration advisory-lock
`NullPool` sessions require separately provisioned headroom and do not enter the
persistent ceiling.

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

ProviderAuthorityWorkerIdentityV2 = {
  version: 2,
  namespace: Text,
  pod_name: Text,
  pod_uid: UUID,
  pod_resource_version: Text,
  pod_service_account_name: Text,
  pod_controller_owner: ProviderKubernetesControllerOwnerV1,
  replica_set_name: Text,
  replica_set_uid: Text,
  replica_set_resource_version: Text,
  replica_set_controller_owner: ProviderKubernetesControllerOwnerV1,
  deployment_name: Text,
  deployment_uid: Text,
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

ProviderAuthorityWorkerRegistrationV2 = {
  version: 2,
  worker_instance_id: UUID,
  worker: ProviderAuthorityWorkerIdentityV2,
  pod_ready: true,
  registered_at: UtcTimestamp
}

RESOURCE_ACTION_WORKER_REGISTRATION_LEASE_RENEW_SECONDS_V1 = 20
RESOURCE_ACTION_WORKER_REGISTRATION_LEASE_TTL_SECONDS_V1 = 60
RESOURCE_ACTION_WORKER_FENCE_MAX_REQUEST_CLAIMS_V1 = 64
RESOURCE_ACTION_WORKER_FENCE_MAX_REQUEST_CLAIMS_JSON_BYTES_V1 = 24_576
RESOURCE_ACTION_WORKER_FENCE_MAX_CANONICAL_BYTES_V1 = 30_720
RESOURCE_ACTION_WORKER_COLD_FENCES_MAX_CANONICAL_BYTES_V1 = 65_536
RESOURCE_ACTION_WORKER_FENCE_MAX_REQUEST_CLAIMS_V2 = 16
RESOURCE_ACTION_WORKER_FENCE_MAX_REQUEST_CLAIMS_JSON_BYTES_V2 = 24_576
RESOURCE_ACTION_WORKER_FENCE_MAX_CANONICAL_BYTES_V2 = 30_720
RESOURCE_ACTION_WORKER_COLD_FENCES_MAX_CANONICAL_BYTES_V2 = 65_536
RESOURCE_ACTION_WORKER_PROCESS_SUPERSESSION_MAX_CANONICAL_BYTES_V1 = 65_536
AUTHORITY_WORKER_CONTAINER_SUPERSESSION_PROOF_MAX_AGE_SECONDS_V1 = 300

ProviderAuthorityWorkerLeaseV1 = {
  version: 1,
  worker_instance_id: UUID,
  generation: PositiveInteger,
  state: "ACTIVE" | "REVOKED",
  renewal_registration: ProviderAuthorityWorkerRegistrationV2,
  renewal_registration_sha256: Sha256,
  renewed_at: UtcTimestamp,
  expires_at: UtcTimestamp,
  revoked_at: null | UtcTimestamp,
  revocation_reason: null | "STALE_HANDOFF" | "CANDIDATE_ABANDONED" |
                            "COHORT_COLD_RECOVERY" |
                            "COHORT_REMOVAL",
  revocation_owner_id: null | UUID,
  last_operation_id: UUID,
  last_operation_kind: "INSERT" | "RENEW" | "REVOKE",
  revision: PositiveInteger
}

ProviderAuthorityWorkerExecutionOwnerV1 = {
  version: 1,
  authority_worker_instance_id: UUID,  # stable Pod UID / Serve lease key
  api_instance_id: UUID,               # fresh per start; unequal to stable ID
  pod_name: Text,
  pod_uid: UUID,
  pod_resource_version: Text,
  container_name: "skypilot-authority-worker",
  container_id: Text,
  container_restart_count: NonnegativeInteger,
  container_started_at: UtcTimestamp,
  observed_at: UtcTimestamp,
  api_instance_started_at: UtcTimestamp
}

# Every typed read requires
# authority_worker_instance_id == pod_uid != api_instance_id.

ProviderAuthorityWorkerLeaseV2 = ProviderAuthorityWorkerLeaseV1 + {
  version: 2,
  execution_owner: ProviderAuthorityWorkerExecutionOwnerV1,
  execution_owner_sha256: Sha256,
  execution_owner_api_instance_id: UUID,
  last_operation_kind: "INSERT" | "RENEW" | "REVOKE" |
                       "BIND_EXECUTION_OWNER" |
                       "SUPERSEDE_EXECUTION_OWNER"
}

ProviderAuthorityWorkerContainerSupersessionProofV1 = {
  version: 1,
  authority_worker_instance_id: UUID,
  pod_name: Text,
  pod_uid: UUID,
  prior_api_instance_id: UUID,
  current_api_instance_id: UUID,
  prior_container_id: Text,
  prior_container_restart_count: NonnegativeInteger,
  current_container_id: Text,
  current_container_restart_count: PositiveInteger,
  current_container_started_at: UtcTimestamp,
  pod_resource_version: Text,
  observed_at: UtcTimestamp
}

ProviderAuthorityWorkerDeploymentSnapshotV2 = {
  version: 2,
  deployment_name: Text,
  deployment_uid: Text,
  deployment_resource_version: Text,
  deployment_generation: PositiveInteger,
  deployment_observed_generation: PositiveInteger,
  pod_template_contract_sha256: Sha256,
  deployment_strategy: "RollingUpdate",
  deployment_max_surge: 0,
  deployment_max_unavailable: 1,
  deployment_spec_replicas: 2,
  deployment_status_replicas: 2,
  deployment_updated_replicas: 2,
  deployment_ready_replicas: 2,
  deployment_available_replicas: 2,
  deployment_unavailable_replicas: 0,
  observed_at: UtcTimestamp
}

ProviderAuthorityWorkerRegistrationSetV2 = {
  version: 2,
  cohort_identity_sha256: Sha256,
  revision: PositiveInteger,
  deployment_snapshot: null | ProviderAuthorityWorkerDeploymentSnapshotV2,
  workers: [ProviderAuthorityWorkerRegistrationV2]
}

ProviderAuthorityWorkerAcceptedExecutionMembershipV2 = {
  version: 2,
  registration: ProviderAuthorityWorkerRegistrationV2,
  registration_set_revision: PositiveInteger,
  registration_set_sha256: Sha256,
  lease: ProviderAuthorityWorkerLeaseV2
}

ProviderAuthorityWorkerStableIdentityProjectionV1 = {
  version: 1,
  namespace: Text,
  pod_name: Text,
  pod_uid: UUID,
  pod_service_account_name: Text,
  pod_controller_owner: ProviderKubernetesControllerOwnerV1,
  replica_set_name: Text,
  replica_set_uid: Text,
  replica_set_controller_owner: ProviderKubernetesControllerOwnerV1,
  deployment_name: Text,
  deployment_uid: Text,
  deployment_generation: PositiveInteger,
  deployment_observed_generation: PositiveInteger,
  pod_template_contract_sha256: Sha256,
  image: ProviderAuthorityWorkerImageV1,
  service_account_uid: Text,
  artifact_inventory_sha256: Sha256,
  callable_inventory_sha256: Sha256,
  handler_allowlist_sha256: Sha256
}

project_stable_worker_identity_v1(
  ProviderAuthorityWorkerIdentityV1 | ProviderAuthorityWorkerIdentityV2
) -> ProviderAuthorityWorkerStableIdentityProjectionV1

Every manifest parser recomputes the static-seed hash, the exact NUL-delimited
derivation preimage from the parent design, its full SHA-256, both derived
names, and service-account/name equality. It rejects a field-by-field valid but
crossed seed or derivation. The shipped V1 identity, registration, and
registration-set codecs above are immutable. V1 requires
each worker's Deployment generation, observed generation, and registration
status-observed generation to be equal; its spec/ready/available counts are
all two; and the Pod is Ready. A readable V1 set has one or two registrations
sorted by strictly ascending distinct Pod UID; only the exact two-member shape
could historically be `ACCEPTING`. Set validation additionally requires every
member to carry one identical tuple of Deployment resourceVersion, generation,
observed generation, and registration status-observed generation. Thus
registrations from different Deployment resourceVersions cannot form one V1
acceptance set. V1 registration and set
rows remain readable only to retire or audit historical rows; no Serve038
registry writer emits or rewrites them, and they cannot satisfy cohort
activation, `/readyz`, preflight, or an action claim. The byte-frozen V1
identity remains the execution-local before/after type inside the shipped
`ProviderAuthorityWorkerAttemptAttestationV1`; its Deployment resourceVersion
is same-process provenance only and grants no membership or readiness. Runtime
stable projection of that V1 identity fails closed unless its textual Pod UID
is one canonical UUID; history/retirement decoding does not require projection.

Because V1 predates registration leases, its sole live retirement program runs
before Serve038 at exact Serve037 through the frozen Serve034 action contract:
the shipped stale-`REGISTERING` edge plus
M4's cleanup-only accepted bridge. It writes only shipped lifecycle/revision /
`state_changed_at`, preserves the V1 registration bytes/hash and every
`RELEASED` or terminal carrier byte-for-byte, and uses the current-chart
tombstone removal plus surviving API exact-NotFound verifier to reach
`RETIRED`. The action catalog through Serve037 has no
`removal_authorized_at`, registration lease, handoff, or cold-recovery row.

Serve038 admits only exact numeric-V1 `RETIRED` history with null
`removal_authorized_at` and a nonnull truthful `retired_at == state_changed_at`.
Every V1
`REMOVAL_AUTHORIZED -> RETIRED` exact-NotFound edge must finish under exact
Serve037 before migration; any V1 nonterminal state, V1 terminal row with a
nonnull removal-authorization time or null/mismatched retirement time,
malformed version token, or mixed old writer
blocks 038 before DDL or stamping. Migration neither copies nor invents a V1
removal timestamp. V1 cannot be selected, registered, renewed, claimed, rolled
back, handed off, cold-recovered, or undergo another lifecycle edge after 038.

Serve038's replacement cohort CHECK requires exact numeric registration-set
version `2` for every `REGISTERING | ACCEPTING | DRAINING` row plus null
`removal_authorized_at` before removal; exact equality to `state_changed_at` in
`REMOVAL_AUTHORIZED`; and preservation through `RETIRED`, where nonnull
`retired_at == state_changed_at >= removal_authorized_at`. The sole exception is
an already-`RETIRED` exact numeric-V1 row whose `removal_authorized_at` is null
and whose nonnull `retired_at == state_changed_at`; all three remain immutable.
Every new V2 transition requires nonnull truthful removal and retirement times.
Version `3`, missing/null/string, numeric `1.0`/`2.0`, every post-038 V1 write,
and every V1 terminal row outside that exact grandfather shape fail the physical
and typed contracts.

Serve038 registry membership uses only V2. Every V2 registration proves a
current Ready Pod and its Pod -> ReplicaSet -> Deployment UID owner chain. Each
worker's image, ServiceAccount, owner chain, and template contract must equal
the cohort. Once the set-level snapshot is nonnull, its Deployment name, UID,
generation, observed generation, and template contract must equal each worker's
corresponding fields; the per-Pod identity contains no Deployment
resourceVersion. A `REGISTERING` set has one or two workers and a null snapshot.
On an exact active-policy-bound Serve039 or Serve040 fresh anchor, each
registration insert/append atomically
creates its generation/revision-one ACTIVE lease with a nonnull owner/hash/
normalized-process triple, locks the named bootstrap API row after that lease,
and changes it to exact bound phase with the owner hash. Lost acknowledgement
adopts the anchor, same-stable-identity lease lineage, and exact bound API phase /
boot identity together. Only a retained pre-039 lease may begin owner-null, and
it must complete typed `BIND_EXECUTION_OWNER` before activation; a new post-039
INSERT can never use that exception. Only after both anchor registrations and
their separate fresh ACTIVE leases exist does the activation transaction lock
them, then both rows named by their normalized current execution owners in
canonical process-ID order, rechecking owner JSON/scalar/API stable-Pod/start
equality, and construct the installed
registrations from the exact lease renewal-registration bytes after stable-
projection equality to those anchors. Each API row has its own process UUID,
binds `pod_uid` to the matching stable authority-worker/Pod UUID, and has a
fresh heartbeat plus exact bound phase and owner hash through commit;
`ready=true` is not required and cannot be published before acceptance. Before the short transaction, the API
verifier reads and hashes the final Deployment snapshot. The same no-I/O CAS
validates and installs that bounded fresh snapshot while changing the cohort to
`ACCEPTING`. Immediately before commit, fresh PostgreSQL time must still precede
both registration-lease and API-instance expiries and keep both registrations plus the snapshot
inside the fixed bound; otherwise activation rolls back and rereads.
`ACCEPTING` and `DRAINING` require exactly two workers and a nonnull snapshot.
That snapshot requires generation equal to observed generation,
status total/updated/ready/available all two, unavailable zero, and the frozen
template hash. Its resourceVersion may advance when status or a one-at-a-time
replacement changes without invalidating an otherwise current survivor. The
initial one-member V2 `REGISTERING` cohort and embedded set both start at
revision one. Every later legal V2 cohort write, including registration append,
activation, handoff or cold-recovery completion, rollback, and lifecycle-only
transitions, advances both revisions by exactly one and preserves their
equality; lease renewal and handoff-table-only transitions advance neither.
Unknown commit outcome adopts only the exact expected before/after revisions,
state, canonical bytes, and hashes. For an anchor insert/append it also requires
the immutable anchor's same-stable-identity lease lineage: at generation one,
the exact insert operation ID and registration bytes; afterward, an `ACTIVE`
generation/revision-equal descendant reached only by legal `RENEW`, initial
`BIND_EXECUTION_OWNER`, or retained `SUPERSEDE_EXECUTION_OWNER` transitions
with the same stable projection. Every owner change must resolve through the
exact process-supersession chain. A
registration's `worker_instance_id` is exactly the canonical UUID value of its
Pod UID. It is the stable Serve membership/lease identity and cannot be reused
by a replacement Pod UID; a non-UUID authority Pod UID fails startup before
registration. It is deliberately distinct from the request claim owner's
`SKYPILOT_API_SERVER_INSTANCE_ID`, which is a fresh random UUID for each Python/
container start, is unequal to the stable Pod UUID, and is inherited only by
that start's children. Authority-worker API-instance registration uses
`INSERT` plus exact adoption, never the generic upsert. A conflict adopts only
the same UUID/role/Pod name/UID/IP/version/supported-handler/supported-payload/
bootstrap-health bytes; the stored database `started_at` is read and becomes
the sole `api_instance_started_at` bound into the execution owner. Unequal
bytes hard-fail. Subsequent heartbeat advances only `heartbeat_at` while
repeating the closed state bytes. Only typed bind/supersede, completed initial
eager warm, current-owner pool-failure `ready -> rewarming`, successful full-
pool recovery `rewarming -> ready`, and permanent withdrawal may change phase,
readiness, draining time, owner hash, or pool generation; each uses the exact
phase matrix and CAS above. It never recreates a missing row. After any
owner bind, a missing row is a retention violation and causes fail-stop. A separate
lease row is authorized only for an exact member of
the current V2 `REGISTERING | ACCEPTING | DRAINING` set or the exact candidate
of the cohort's unique `OPEN | READY` handoff. Initial insertion and every
renewal lock cohort -> relevant nonterminal handoff -> lease -> its normalized
current execution-owner API row in the global order, recheck that authorization
and lifecycle, and require the renewing caller process UUID to equal the owner
JSON/scalar while the row repeats the same stable Pod, stored start identity,
role/inventories and remains fresh through commit. It then writes one fresh self-read
V2 registration/hash while advancing only generation, renewal time, expiry,
and lease-row revision. A normal renewal must preserve the complete execution
owner byte-for-byte; only the retained bind/supersession protocols below may
change it. The renewal registration must recompute the same worker
instance and stable identity as the authorizing registration, and its worker
`observed_at` must be no later than database `renewed_at` and within the fixed
five-minute bound. Lease existence confers no claim or effect authority before
accepted membership.
The only additional insertion authorization is for the exact two locked cold-
recovery candidates inside the same atomic membership CAS. They have no
registration lease before it and cannot renew unless that commit makes them
accepted members.
Leases are mutable rows separate from immutable registration-set bytes; renewal
cannot change a registration-set hash or an in-flight attempt attestation. An
accepted-membership object is valid only when its registration is byte-equal to
the member at the named current V2 set revision/hash and its same-instance lease
is fresh. For preflight, claim, and every effect,
the stable instance ID derived from the V1 attempt/preflight identity must equal
the accepted V2 stable instance ID, the attestation's canonical process UUID
must equal the exact-current execution-owner/request/queue/API chain, and
`project_stable_worker_identity_v1(v1_identity)` must be byte-equal to
`project_stable_worker_identity_v1(v2_registration.worker)`. Pod, ReplicaSet,
and Deployment resourceVersions plus `observed_at` are intentionally outside
that cross-version projection; each version still validates those fields in
its own live-read contract. No V1-only field can substitute for the V2 set and
fresh V2 lease whose execution owner is exact-current.

Serve039 adds nullable execution-owner/hash columns plus the normalized
`execution_owner_api_instance_id` scalar without changing the
shipped V1 lease codec. Historical V1 rows keep all three null. Before first V2
private admission, each accepted ACTIVE lease is upgraded under a zero-private-
request inventory to `ProviderAuthorityWorkerLeaseV2`: one cohort -> handoff ->
lease -> bootstrap API-instance transaction installs the exact current owner,
advances lease generation/revision by one, and records
`BIND_EXECUTION_OWNER`. Bind is an owner-changing renewal: an expired but still
`ACTIVE` source is legal, and the one commit uses
`GREATEST(clock_timestamp(), source.renewed_at, owner.container_started_at,
owner.observed_at, api_instance.started_at,
new_renewal_registration.worker.observed_at)` to write a fresh self-read
renewal registration, `renewed_at`, 60-second expiry, and the owner/hash/scalar
triple while changing the API row from bootstrap to bound. Immediately before
commit, the owner observation remains inside the fixed five-minute identity
bound, the API row remains inside its 20-second heartbeat window, and PostgreSQL
time precedes the new lease expiry; otherwise it rolls back and rereads.
Readiness and child warming require that fresh committed descendant. A post-039
new lease includes its owner at `INSERT` under the same bootstrap-row, operation-
time, phase-transition, and final freshness contract.
Normal `RENEW` and every revocation preserve the owner/hash/normalized-process-
scalar triple; owner replacement by
ordinary upsert/heartbeat is corruption. Claim SQL locks the stable member's
lease and requires `execution_owner.api_instance_id` to equal the process-
unique caller API row, while owner Pod UID/stable ID equals the accepted member.
Lineage, terminal receipts, and fences carry both identities. Thus a late old
process fails its request CAS even though the stable Pod remains accepted.

For a locked active private request `r`,
`private_request_terminal_lower_bound(r)` is exactly
`GREATEST(r.created_at, r.updated_at,
COALESCE(r.heartbeat_at, '-infinity'::timestamptz),
COALESCE(r.cancel_requested_at, '-infinity'::timestamptz))`. A multi-request
operation takes the greatest of those values, omitting the request term for an
empty inventory. Discovery snapshots every named column and the locked suffix
requires byte equality. All private finish, cancel-acknowledgement, and receipt
times are at least that bound even if the database clock moves backward.

A same-Pod Python restart is not repaired by lease expiry. The chart's fixed
`tini -- python ...` topology makes Python exit terminate the container; a
supported next process therefore has a new Kubernetes container ID and larger
restart count. It starts `/bootstrapz` only, mints a fresh API instance UUID,
starts no claimant/pool, and exact-reads its named current container from the
same Pod UID. If its API ID already equals the lease owner it may exact-adopt a
lost acknowledgement. If the API ID differs while container ID is unchanged,
it cannot prove quiescence and exits the container; two supervisors in one
container can never supersede one another. Otherwise it constructs the closed
container-supersession proof: same stable worker/Pod/name, different nonempty
container IDs, strictly larger restart count, current named container exactly
Running, current start time/Pod resourceVersion, and a fresh bounded
observation. Kubernetes' one-current-container-per-name invariant then proves
the prior container and all its child processes are gone; boot UUID or stale
heartbeat alone never does.
The proof and current owner are one observation:
`current_container_started_at <= observed_at`, both fields plus container ID,
restart count, Pod/resourceVersion and stable ID are byte-equal, and
`current_api_row.started_at == current_owner.api_instance_started_at`. The
proposed supersession time is no earlier than those three timestamps. At the
final locked precommit check, proof/owner observation is neither future nor more
than the fixed 300 seconds old, the current API row remains inside its fixed
20-second heartbeat window, and PostgreSQL time precedes the new lease expiry.
A boundary wait beyond any window rolls back and obtains new evidence.

Nonlocking discovery uses `LIMIT 17` over every active row owned by the prior
API UUID, without filtering by handler/route/correlation marker, generation,
status, or queue validity. It rejects rather than truncates the 17th row, enforces the
24,576-byte request-list and 65,536-byte enclosing supersession ceilings, and
then requires every row to closed-validate as a legal generation-one `PENDING |
RUNNING` claimed request/queue shape before constructing its arm-specific
terminal receipt. Generation two, `WAITING`, crossed/missing queue state, or a
partial private marker blocks the entire transition. The locked requery repeats the
same max-plus-one and byte checks before any commit. One
retryable transaction locks cohort and resolves the exact nonterminal
membership protocol; inserts the complete immutable process-supersession evidence;
locks the stable lease; locks prior and current API rows in UUID order; locks
all requests by request ID and then all queues by request ID; reconstructs the
inventory; and runs the common lineage -> action-selector -> shadow-history ->
event terminal batch. Every old request becomes `CANCELLED` under its original
token/generation/owner through the trusted `PROCESS_SUPERSESSION_FENCE` mode,
never requeued. An action claim with committed lineage gets exactly
`REQUEST_CANCELLED/LINEAGE/CANCELLED`; an action claim assigned generation one
before claim-start gets exactly
`TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START/CANCELLED`, and the
terminalizer never creates its missing lineage. A shadow claim with an
`AUTHORIZED` history gets exact generation-one
`REQUEST_CANCELLED/SHADOW_EXECUTION/CANCELLED` history with the stored lineage
hash; a still-`BOUND` claim gets
`TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START/CANCELLED` with a null
hash. The same
transaction advances lease
generation/revision exactly one, installs the current owner, records the reused
operation ID with `SUPERSEDE_EXECUTION_OWNER`, and leaves cohort/registration-
set revision unchanged. The prior API row may be stale or unready but must
equal the prior owner's process UUID, stable Pod identity, and stored start
time. The current row must be a fresh, unready, nondraining, bootstrap-only
`authority-worker` for the same Pod name/UID/IP, with the new UUID/start time,
the fixed private handler/payload inventory, and zero claims. Both rows,
owners, and the container proof cross-equal; malformed or crossed evidence
rejects. Supersession may consume an expired but still `ACTIVE` source and is
an owner-changing renewal. Its one proposed operation time is the greatest of
PostgreSQL time, source `renewed_at`, proof/current-owner container start and
observation, current API `started_at`, new renewal-registration worker
`observed_at`, and every affected request terminal lower
bound. Locked revalidation repeats all source/API/request bytes and bounds; the
same commit refreshes the self-read renewal registration, `renewed_at`, and
60-second expiry, installs the owner/hash/scalar triple, and uses that time for
the process row, request finishes, cancellation acknowledgements, and receipts.
It also changes the current API row from bootstrap to bound in that commit.
The supersession row requires committed generation/
revision equal source + one; both scalar API IDs to equal their embedded owners
and container proof; stable worker/Pod equality throughout; canonical request-
list hash; `completed_at` equal every request finish time; and exact receipt
hashes. Unknown outcome adopts only the
entire immutable row, terminal receipts, and current lease operation; any drift
blocks. Unique prior and current API-instance indexes prevent branches or boot
reuse. A lease whose last operation is `SUPERSEDE_EXECUTION_OWNER` resolves its
ID directly through unique `(cohort_id, operation_id)` and must equal that row's
committed counters, stable worker, current owner/hash/scalar, and time. After a
later renewal, the lease's current process scalar has at most one indexed
`current_api_instance_id` tip; if present that tip validates directly, and if
absent the process is the insert/bind owner. Unique prior/current indexes plus
the frozen writer induction reject branches, gaps, and reuse in O(1); readers
never walk an unbounded process chain. Only after commit/readback may the process warm all fixed children and
publish claim readiness.

Unknown INSERT/BIND/SUPERSEDE outcomes adopt the API phase as part of the joined
proof. Bootstrap plus unchanged source means uncommitted and permits full retry.
A commit requires the same UUID/immutable boot bytes, exact bound phase/stable
ID/execution-owner hash, unready/nondraining shape, only a legal fresh heartbeat
advance, and the exact lease operation; supersession additionally requires the
process row and every request/receipt/event. A documented same-owner legal lease
descendant may be adopted without rewriting the API row. Unequal phase/hash/
identity or partial evidence blocks, and retry never resets bound to bootstrap
or repeats terminal mutation.

The compatibility matrix is closed. Without a nonterminal handoff, the lease
must be an ACTIVE current `REGISTERING | ACCEPTING | DRAINING` member. During
`OPEN | READY`, only the ACTIVE survivor or the exact ACTIVE candidate lease may
supersede; the handoff anchors stay stable-Pod evidence and subsequent
acknowledgement/completion reads the lease's new exact-current owner. The
revoked stale member, unrelated Pod, and terminal candidate reject. Cohort
locking serializes cold recovery: supersession-first makes the recovery's
owner/lease snapshot drift and forces exact reread or UID-proof rejection;
cold-recovery-first revokes an old lease and makes supersession reject. A cold
candidate has no lease before commit and therefore cannot supersede; a restarted
bootstrap supplies a fresh API row to a newly constructed recovery input.

If a live supervisor loses request-lease authority, it first withdraws
readiness/stops claiming, signals and joins every owned future/process, and then
terminalizes each still-matching claim under the trusted
`OWNER_QUIESCED_LEASE_LOSS` mode. If database proof cannot commit, it exits the
container after killing the pool so the next container start uses the retained
supersession protocol. Lease time alone never permits a third party to close
the work. Process-supersession history is permanent while any lease, API row,
request, lineage, terminal receipt, action/shadow evidence, cohort/reference,
or rollout proof can name either owner.
Generic API-instance GC excludes `authority-worker`. A Serve-owned typed job
reciprocally retains the API-instance row while the
indexed normalized execution-owner scalar of any ACTIVE or REVOKED lease, any
active API request, action lineage or selector, either worker-ID column of any
retained `AUTHORIZED | SETTLED` shadow execution history, shadow terminal
history, or either scalar process-supersession ID names that API UUID. A `BOUND`
shadow history has null worker IDs. Mandatory normalized receipts cover
handoff/cold JSON entries, so GC never reverse-scans JSON and the generic layer
never imports/reflects Serve039. The job serializes on the exact class-13
`authority-worker-v2` cursor. Each finite epoch persists the high-water result
of the exact-role `ORDER BY instance_id DESC LIMIT 1` query, advances in UUID
order, and wraps only after reaching that bound; `MAX(uuid)` is forbidden
because PostgreSQL 14 does not provide it. At most 128 targets plus one epoch-
start and one epoch-completion cursor transaction run per pass. Rows inserted, newly eligible, newly rootless, or
temporarily locked behind the cursor are revisited in the next finite epoch.
Indexed root-absence checks are discovery-only prefilters, so a rooted first
page cannot starve later rows.

Each target transaction locks cursor then API row, requires `ready=false` and
`heartbeat_at <= clock_timestamp() - interval '5 minutes'`, repeats the exact
role/health predicate, and point-queries every indexed root. It retains a
changed, malformed, raced-to-rooted, or blocked row and deletes only an exact
rootless row; either ordinary result atomically advances the cursor with one
operation ID/revision/time. Unknown commit adopts that operation or a legal
successor, while database failure is closed. The cursor has no FK, is not a
root, and may name a deleted row. Every root-creating writer, including BIND and
supersession, locks the same API row and revalidates existence before commit;
read-only root probes acquire no earlier-class row lock. Root absence, expiry,
or loss of readiness alone is not deletion authority, and heartbeat never
recreates a missing row.
The request lookup uses the existing process-first active-claim index. Any
authority-worker-owned active row outside the exact four private shapes is a
retention root plus blocking corruption, never an ordinary-row filter miss.

`REGISTERING` permits no in-place member replacement. If the surviving API
verifier proves exact UID-qualified absence of either anchor in a one- or two-
member set before activation, one transaction locks cohort -> handoff slot ->
all leases -> references, proves the cohort was never accepted and has zero
handoff/reference/private/action/effect evidence, revokes every ACTIVE lease
with `COHORT_REMOVAL`, and moves directly to `REMOVAL_AUTHORIZED` using one
PostgreSQL timestamp. Every such lease has null `revocation_owner_id`, preserves
its nullable or nonnull execution-owner/hash/normalized-process-scalar triple
exactly, records
`last_operation_kind=REVOKE`, and has `revoked_at ==
cohort.removal_authorized_at`. Serve038 adds that nullable column to the shipped
cohort table; it is null before removal, set exactly once on entry to
`REMOVAL_AUTHORIZED`, and preserved through `RETIRED`, whose `state_changed_at`
and `retired_at` remain truthful. Lease expiry,
unready state, deletion timestamp, or name-only evidence cannot authorize this
edge. Exact Deployment/ServiceAccount NotFound later commits `RETIRED`, and a
fresh suffix/cohort is required.

```

One-at-a-time V2 membership replacement is a Serve038 PostgreSQL protocol, not
a blind rewrite of the registration-set JSON:

```text
ProviderAuthorityWorkerPodUidAbsenceProofV1 = one of:
  {version: 1,
   disposition: "not_found",
   namespace: Text,
   pod_name: Text,
   expected_absent_pod_uid: UUID,
   current_pod_uid: null,
   current_pod_resource_version: null,
   observed_at: UtcTimestamp}
  {version: 1,
   disposition: "same_name_different_uid",
   namespace: Text,
   pod_name: Text,
   expected_absent_pod_uid: UUID,
   current_pod_uid: UUID,  # unequal to expected_absent_pod_uid
   current_pod_resource_version: Text,
   observed_at: UtcTimestamp}

ProviderAuthorityWorkerStaleAuthorityFenceV1 = {
  version: 1,
  origin_revoking_handoff_id: UUID,
  stale_worker_instance_id: UUID,
  stale_lease_generation: PositiveInteger,
  prior_stale_lease_revision: PositiveInteger,
  revoked_stale_lease_revision: PositiveInteger,
  request_claims: SortedList<{  # 0..64; whole list <= 24,576 canonical bytes
    request_id: UUID,
    execution_generation: PositiveInteger,
    claim_token_sha256: Sha256,
    prior_lease_expires_at: UtcTimestamp,
    fenced_delivery_state: "queued"
  }>,
  fenced_at: UtcTimestamp
}

ProviderAuthorityWorkerColdRecoveryFenceV1 = {
  version: 1,
  recovery_id: UUID,
  worker_instance_id: UUID,
  pod_uid: UUID,
  prior_lease_state: "ACTIVE" | "REVOKED",
  lease_generation: PositiveInteger,
  prior_lease_revision: PositiveInteger,
  terminal_lease_revision: PositiveInteger,
  preserved_revocation_reason: null | "STALE_HANDOFF",
  preserved_revocation_owner_id: null | UUID,
  request_claims: SortedList<{  # 0..64; whole list <= 24,576 canonical bytes
    request_id: UUID,
    execution_generation: PositiveInteger,
    claim_token_sha256: Sha256,
    prior_lease_expires_at: UtcTimestamp,
    fenced_delivery_state: "queued"
  }>,
  fenced_at: UtcTimestamp
}

# V1 queued fences remain readable pre-039 history but cannot authorize M4.
# M4 uses terminal V2 fences so ReplayPolicy.NEVER is never overridden.
ProviderShadowExecutionHistoryV1 = {
  version: 1,
  decision_id: UUID,
  request_sequence: PositiveInteger,
  request_role: "PRIMARY_LAUNCH" | "PRIMARY_DOWN",
  request_id: UUID,
  handler_name: "serve_shadow_candidate_launch" |
                "serve_shadow_candidate_down",
  immutable_payload_sha256: Sha256,
  request_input_sha256: Sha256,
  preflight_request: ProviderAuthorityPreflightRequestV2,
  preflight_request_sha256: Sha256,
  preflight_response: ProviderAuthorityPreflightResponseV2,
  preflight_response_sha256: Sha256,
  phase: "BOUND" | "AUTHORIZED" | "SETTLED",
  request_execution_generation: null | 1,
  authority_worker_instance_id: null | UUID,
  worker_instance_id: null | UUID,
  claim_token_sha256: null | Sha256,
  dispatch_membership:
      null | ProviderShadowCandidateDispatchMembershipV2,
  dispatch_membership_sha256: null | Sha256,
  execution_authority: null | ProviderShadowExecutionAuthorityProofV2,
  execution_authority_sha256: null | Sha256,
  execution_authority_lineage_sha256: null | Sha256,
  authorized_at: null | UtcTimestamp,
  provider_io_boundary: "NOT_STARTED" | "INTENT_COMMITTED" |
                        "SUBMITTED_OR_AMBIGUOUS",
  provider_progress_revision: NonnegativeInteger,
  provider_progress: null | ProviderShadowLifecycleProgressV1,
  provider_progress_sha256: null | Sha256,
  provider_operation_id: null | Text,
  provider_effect_trace: LegacyProviderEffectTraceV1,
  provider_effect_trace_sha256: Sha256,
  request_return: null | ServeShadowCandidateRequestReturnV1,
  request_return_sha256: null | Sha256,
  terminal_history_sha256: null | Sha256,
  settlement_basis: null | "HANDLER_RETURN" | "REQUEST_FALLBACK",
  reduction_disposition: null | "S" | "R" | "U" | "B" | "Q" |
                               "P0" | "O" | "X",
  partial_down_decision_id: null | UUID,
  partial_down_request_sequence: null | PositiveInteger,
  partial_down_basis_sha256: null | Sha256,
  revision: PositiveInteger,
  created_at: UtcTimestamp,
  updated_at: UtcTimestamp,
  settled_at: null | UtcTimestamp
}

ProviderShadowRequestTerminalHistoryV2 = {
  version: 2,
  decision_id: UUID,
  request_sequence: PositiveInteger,
  request_role: "PRIMARY_LAUNCH" | "PRIMARY_DOWN",
  request_id: UUID,
  immutable_payload_sha256: Sha256,
  request_input_sha256: Sha256,
  handler_name: "serve_shadow_candidate_down" |
                "serve_shadow_candidate_launch",
  request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
  request_execution_generation: 0 | 1,
  authority_worker_instance_id: null | UUID,
  worker_instance_id: null | UUID,  # process-unique API claim owner
  authority_disposition: "NO_SUCCESSFUL_CLAIM_START" | "SHADOW_EXECUTION",
  execution_authority_lineage_sha256: null | Sha256,
  terminal_cause: "HANDLER_RETURN" | "REQUEST_FAILED" |
                  "REQUEST_CANCELLED" | "TERMINAL_BEFORE_CLAIM_START",
  terminal_winner: ProviderShadowTerminalCommitmentV1,
  terminal_winner_sha256: Sha256,
  request_return_sha256: null | Sha256,
  request_finished_at: UtcTimestamp
}

The history projection is the typed view of the one-to-one class-10 Serve039
row, not a second serialized enclosing value. Every JSON/hash child is parsed,
hashed, and bounded independently at 65,536 canonical UTF-8 bytes. `BOUND` has
the immutable nonnull complete preflight request/response pairs, the wholly
null authority and settlement bundles, the exact empty or inherited retry-seed
progress shape, and an empty provider-effect trace. `AUTHORIZED` has generation one, distinct
stable/process IDs, token hash, both proof/hash pairs, lineage hash, and
authorization time, with no settlement fields. `SETTLED` retains that complete
authority bundle or the wholly null pre-claim-start bundle and has the exact
terminal-receipt hash, basis, reduction, and settlement time. Handler basis has
the exact nonnull shadow return pair; fallback has it null. The three partial-
down fields are all nonnull exactly for launch `Q`, point to one normal
represented primary-down history, and otherwise are all null. Identity,
request, handler, immutable hashes, progress revision/hash, terminal receipt,
return, effect trace, and outcome cross-validate against the retained
parent/child/request. Every pre-I/O CAS appends the exact planned effect with a
null response before the call and every post-I/O CAS resolves only that same
entry; no later cursor can erase or reconstruct an earlier response. The strict
handler return binds `final_provider_effect_trace_sha256`, and settlement copies
that byte-equal trace into the completed child's `legacy_effect_trace` pair.
Crash-after-call-before-checkpoint therefore retains the truthful null-response
ambiguous entry rather than inventing a transport result;
action-shaped lineage, return, progress, or outcome values reject.
The named strict `ProviderShadowExecutionHistoryV1` reader covers `BOUND`,
`AUTHORIZED`, and non-`X` `SETTLED` rows. A literal `X` settled row uses the
layered raw snapshot above so its malformed bounded progress evidence survives;
all outer identity, effect-trace, receipt, return, and settlement fields remain
strict.

ProviderAuthorityWorkerTerminalFenceClaimV2 = one of:
  {claim_kind: "resource_action",
   request_id: UUID,
   handler_name: "serve_resource_action_down" |
                 "serve_resource_action_launch",
   action_id: UUID,
   attempt: PositiveInteger,
   request_input_sha256: Sha256,
   execution_generation: 1,
   claim_owner_api_instance_id: UUID,
   claim_token_sha256: Sha256,
   prior_lease_expires_at: UtcTimestamp,
   prior_cancel_requested_at: null | UtcTimestamp,
   fence_outcome: "TERMINAL_CANCELLED",
   request_finished_at: UtcTimestamp,
   terminal_selector_sha256: Sha256,
   shadow_terminal_history: null,
   shadow_terminal_history_sha256: null}
| {claim_kind: "shadow_candidate",
   request_id: UUID,
   handler_name: "serve_shadow_candidate_down" |
                 "serve_shadow_candidate_launch",
   decision_id: UUID,
   request_sequence: PositiveInteger,
   request_role: "PRIMARY_LAUNCH" | "PRIMARY_DOWN",
   immutable_payload_sha256: Sha256,
   execution_generation: 1,
   claim_owner_api_instance_id: UUID,
   claim_token_sha256: Sha256,
   prior_lease_expires_at: UtcTimestamp,
   prior_cancel_requested_at: null | UtcTimestamp,
   fence_outcome: "TERMINAL_CANCELLED",
   request_finished_at: UtcTimestamp,
   terminal_selector_sha256: null,
   shadow_terminal_history: ProviderShadowRequestTerminalHistoryV2,
   shadow_terminal_history_sha256: Sha256}

ProviderAuthorityWorkerStaleAuthorityFenceV2 = {
  version: 2,
  origin_revoking_handoff_id: UUID,
  stale_worker_instance_id: UUID,
  stale_lease_generation: PositiveInteger,
  prior_stale_lease_revision: PositiveInteger,
  revoked_stale_lease_revision: PositiveInteger,
  request_claims: SortedList<
      ProviderAuthorityWorkerTerminalFenceClaimV2>,  # 0..16, request-ID order
  fenced_at: UtcTimestamp
}

ProviderAuthorityWorkerColdRecoveryFenceV2 = {
  version: 2,
  recovery_id: UUID,
  worker_instance_id: UUID,
  pod_uid: UUID,
  prior_lease_state: "ACTIVE" | "REVOKED",
  lease_generation: PositiveInteger,
  prior_lease_revision: PositiveInteger,
  terminal_lease_revision: PositiveInteger,
  preserved_revocation_reason: null | "STALE_HANDOFF",
  preserved_revocation_owner_id: null | UUID,
  request_claims: SortedList<
      ProviderAuthorityWorkerTerminalFenceClaimV2>,  # 0..16, request-ID order
  fenced_at: UtcTimestamp
}

ProviderAuthorityWorkerProcessSupersessionV1 = {
  version: 1,
  supersession_id: UUID,
  cohort_id: Text,
  authority_worker_instance_id: UUID,
  operation_id: UUID,
  source_lease_generation: PositiveInteger,
  source_lease_revision: PositiveInteger,
  committed_lease_generation: PositiveInteger,
  committed_lease_revision: PositiveInteger,
  prior_api_instance_id: UUID,
  current_api_instance_id: UUID,
  prior_execution_owner: ProviderAuthorityWorkerExecutionOwnerV1,
  prior_execution_owner_sha256: Sha256,
  current_execution_owner: ProviderAuthorityWorkerExecutionOwnerV1,
  current_execution_owner_sha256: Sha256,
  container_supersession_proof:
      ProviderAuthorityWorkerContainerSupersessionProofV1,
  container_supersession_proof_sha256: Sha256,
  request_claims: SortedList<
      ProviderAuthorityWorkerTerminalFenceClaimV2>,  # 0..16, request-ID order
  request_claims_sha256: Sha256,
  completed_at: UtcTimestamp
}

ProviderAuthorityWorkerCandidateZeroEffectProofV1 = {
  version: 1,
  candidate_worker_instance_id: UUID,
  candidate_pod_uid: UUID,
  accepted_membership_count: 0,
  live_request_claim_count: 0,
  attempt_attestation_count: 0,
  provider_progress_count: 0,
  provider_operation_count: 0,
  provider_effect_count: 0,
  observed_at: UtcTimestamp
}

V1 queued fences are read-only pre-039 history and retain their V1 64/24,576/
30,720/65,536 bounds plus max-plus-one `LIMIT 65`; they cannot authorize a live
handoff, recovery, claim, or replay after Serve039. Every live Serve039 handoff
or cold-recovery fence uses the V2 constants: at most 16 request claims, at most
24,576 canonical bytes for the list, 30,720 for one complete fence, and 65,536
for the exact-two cold-fence array. V2 discovery queries `LIMIT 17` over every
active row owned by the fenced authority process with no handler/marker/
generation/status/queue filter, then requires every row to be exactly one of the
four private routes with the legal claim/queue shape. An ordinary-looking,
unmarked, crossed, or malformed row is blocking corruption and cannot disappear
through a filter. It sorts by
request UUID bytes, and rejects cardinality or any nested/enclosing canonical-
byte overflow before any handoff/recovery/lease/revocation write. The locked
requery uses the same max-plus-one rule and rolls the whole transaction back on
overflow or drift. Truncation, pagination, a hash-only substitute, and partial
fencing are forbidden; typed decoding rejects any retained over-limit list or
envelope. On overflow the verifier writes nothing, observes no new stale claim,
lets the per-request generic quiescence terminalizer reduce ordinary expired
claims, and retries fresh discovery; a cancellation-pending claim still requires
owner acknowledgement, a UID-qualified V2 recovery fence, or the container-
qualified process-supersession fence. Persistent overflow
is a recovery blocker, never permission to discard claims.
Checked-in generated maximal action-only, shadow-only, and mixed 16-entry
fixtures must fit all three V2 byte ceilings and exact-two enclosure, while
one-byte/text-max and 17-entry negatives reject. The manifest-bound fixed pool
and API-instance-serialized claim cap make overflow unreachable from valid live
state rather than a normal liveness mechanism.
The enclosing process-supersession DTO is separately limited to 65,536
canonical bytes and has its own maximal 16-entry action/shadow/mixed goldens;
its owner/proof objects do not consume the smaller request-list ceiling.

The V2 claim union is closed over the complete four-handler inventory. Each
entry claim-owner API UUID equals the enclosing stable worker lease's current
execution owner (or the supersession row's prior owner); that owner's stable
worker/Pod UUID equals the enclosing stale/cold/process worker. The action
selector or shadow history repeats both exact IDs, and `request_finished_at`
equals the enclosing stale/cold `fenced_at` or process-supersession
`completed_at`. Nonlocking discovery includes
the exact request, queue, private route/correlation, reconstructed immutable
request input, and any action lineage/selector or shadow-history candidate
needed to construct the complete entry before the earlier-class evidence row
is inserted. After locking all requests and then all queues, the batch core
reconstructs every byte and requires exact equality; a concurrent claim-start,
terminalization, cancellation-intent, or correlation change rolls the whole
handoff/recovery/process-supersession transaction back.

Every locked source is active and claimed at generation one with the entry's
claim-owner API UUID, token hash, lease expiry, cancel-request timestamp, null cancel-
acknowledgement, and claimed queue generation one. An action arm exact-binds
handler/kind/action/attempt/input, has no shadow object/hash, and hashes the
exact Serve039 terminal-selector row that the batch writes. A shadow arm exact-
binds handler/kind/decision/sequence/role/invocation hash, has a null action selector,
and embeds the exact `ProviderShadowRequestTerminalHistoryV2` row written by the
same batch, fixed to `CANCELLED`, generation one, and both nonnull stable/process
IDs. If the committed class-10 history nonlockingly read after locking the
class-15 request is `AUTHORIZED`, the receipt is exactly
`REQUEST_CANCELLED/SHADOW_EXECUTION` with its byte-equal nonnull execution-
lineage hash. If it is still `BOUND`, the receipt is exactly
`TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START` with a null lineage hash.
No other phase/disposition is legal; its hash is SHA-256 of that row's canonical
bytes. The
cancel-request timestamp is either null or nonnull and snapshotted exactly;
nonnull acknowledgement on any active source and null-request/non-null-ack are
blocking corruption. The
general DTO additionally requires generation zero with both IDs null or
generation one with both IDs nonnull and exactly the disposition/state/cause/
lineage-hash matrix in the parent design.
For every V2 handoff, cold-recovery, or process-supersession batch, a nonnull
`prior_cancel_requested_at` is preserved on the terminal request and
`cancel_acknowledged_at` is set to the enclosing `fenced_at`/`completed_at`
only after the UID/container proof establishes quiescence. If the prior field
is null, both cancellation fields remain null. A terminal row with a retained
unacknowledged intent is invalid. Each enclosing time includes every affected
`private_request_terminal_lower_bound`; same-owner acknowledgement uses exactly
`GREATEST(clock_timestamp(), private_request_terminal_lower_bound(request))`,
and an unclaimed generation-zero cancellation uses
`GREATEST(clock_timestamp(), created_at, updated_at)` for its equal requested,
acknowledged, terminal-updated, finished, and receipt times; exact replay
validates that equality.
No request,
action/attempt, or shadow decision/sequence key may repeat. The store first
key-share-locks all named action lineages in canonical lineage-key order, then
inserts/adopts action selectors in action/attempt order, then shadow histories
in decision/sequence order. Only after all class-17 receipts succeed does the
generic batch allocate terminal events. Request GC retains an action until its
attempt is settled and selector validates. It retains a shadow request until
its represented child is `COMPLETE`, same-key execution history is `SETTLED`,
and the exact terminal receipt, settlement basis, final progress, outcome/hash,
and copied return/hash relationships cross-validate; a fallback has a null
return pair and a handler basis has the exact nonnull pair. These are nonlocking
point reads after the class-15 request lock, never a backward class-10 lock. The
immutable handoff/cold/process evidence is an additional cross-check, never the sole
normal shadow completion record.

Same-process quiescence is also closed. `OWNER_ACK_CANCEL` is singleton-only and
requires prior cancellation intent; `OWNER_QUIESCED_LEASE_LOSS` is singleton-
only and permits prior intent or none. Both require the same locked generation-
one historical owner and write `CANCELLED`: action lineage maps to
`REQUEST_CANCELLED/LINEAGE`, pre-claim-start maps to
`TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START`, and shadow maps to the
corresponding `REQUEST_CANCELLED` or `TERMINAL_BEFORE_CLAIM_START` history.
Lineage is never fabricated. Prior intent is preserved and acknowledged at the
one finish time; without intent, legal only for lease loss, both cancellation
fields stay null. Terminal updated/finished/receipt/acknowledgement times are one
exact lower-bounded database scalar. No-intent lease-loss action reduction uses
the retained journal table (`P0`/`O`/`S`/`X`) and can create only attempt `n+1`,
never replay the request; pending intent still grants no provider evidence, and
shadow uses its distinct progress/history contract.

The parent terminal store's singleton/batch receipts are tagged
`NEWLY_TERMINALIZED | EXACT_ADOPTED`; the outer terminal result additionally
has `LOST_RACE`, while missing/crossed durable evidence is blocking
`PRIVATE_TERMINAL_CORRUPTION`. Retry first discovers the request plus its unique
selector/shadow history. A terminal generation-one request derives and locks the
historical process API row from that receipt at class 14 before locking the
request; generation zero starts at class 15. It requires no queue and first
closed-validates request/route/input/handler/state/generation/owner/cause/
lineage/cancellation/updated/finish/event bytes as one internally consistent
legal terminal winner, reusing persisted time and appending nothing. Equality to
the caller's original trusted context returns `EXACT_ADOPTED`; a different
closed legal winner for the same immutable request returns `LOST_RACE` with no
receipt or write. Only missing, crossed, or internally inconsistent evidence is
`PRIVATE_TERMINAL_CORRUPTION`; a valid owner acknowledgement, UID handoff/cold
fence, process supersession, or owner-lease-loss winner is not corruption.
Handoff/cold/process unknown outcomes adopt only the whole
immutable operation plus every terminal request, receipt, and event; partial
mixing is corruption. Commit-boundary tests for handler success/failure, owner
cancel, claim-start rejection, and one-request recovery prove one finish,
receipt, and event with no reminted timestamp. Handler-versus-owner-ack and
handler-versus-UID/process-fence tests cover the losing handler retry after both
acknowledged and unknown winner commits.
For an otherwise same active claim, newly nonnull unacknowledged cancellation
intent makes handler success/failure return typed `LOST_RACE`. While that exact
owner remains live, only its quiesced owner-ack/lease-loss path closes the intent;
after owner death, the typed UID-qualified handoff/cold fence or container-
qualified process-supersession fence may close it. Both intent/handler lock
orders are tested and intent is never reclassified as corruption or overwritten
by success.

serve_resource_action_worker_registration_leases = {
  cohort_id: Text REFERENCES serve_resource_action_worker_cohorts(cohort_id),
  worker_instance_id: UUID,
  pod_uid: UUID,
  generation: PositiveInteger,
  state: "ACTIVE" | "REVOKED",
  renewal_registration: ProviderAuthorityWorkerRegistrationV2,
  renewal_registration_sha256: Sha256,
  execution_owner: null | ProviderAuthorityWorkerExecutionOwnerV1,
  execution_owner_sha256: null | Sha256,
  execution_owner_api_instance_id: null | UUID,
  renewed_at: UtcTimestamp,
  expires_at: UtcTimestamp,
  revoked_at: null | UtcTimestamp,
  revocation_reason: null | "STALE_HANDOFF" | "CANDIDATE_ABANDONED" |
                            "COHORT_COLD_RECOVERY" |
                            "COHORT_REMOVAL",
  revocation_owner_id: null | UUID,
  last_operation_id: UUID,
  last_operation_kind: "INSERT" | "RENEW" | "REVOKE" |
                       "BIND_EXECUTION_OWNER" |
                       "SUPERSEDE_EXECUTION_OWNER",
  revision: PositiveInteger,
  PRIMARY KEY (cohort_id, worker_instance_id),
  UNIQUE (cohort_id, pod_uid),
  CHECK (worker_instance_id = pod_uid AND generation > 0 AND revision > 0 AND
         expires_at = renewed_at + INTERVAL '60 seconds' AND
         ((execution_owner IS NULL AND execution_owner_sha256 IS NULL AND
           execution_owner_api_instance_id IS NULL) OR
          (execution_owner IS NOT NULL AND
           execution_owner_sha256 IS NOT NULL AND
           execution_owner_api_instance_id IS NOT NULL AND
           CASE WHEN jsonb_typeof(execution_owner) = "object" THEN
             execution_owner_api_instance_id::text =
                 execution_owner ->> "api_instance_id" AND
             worker_instance_id::text =
                 execution_owner ->> "authority_worker_instance_id" AND
             pod_uid::text = execution_owner ->> "pod_uid" AND
             execution_owner_api_instance_id != worker_instance_id
           ELSE FALSE END IS TRUE)) AND
         (last_operation_kind NOT IN
              ("BIND_EXECUTION_OWNER", "SUPERSEDE_EXECUTION_OWNER") OR
          execution_owner IS NOT NULL) AND
         ((state = "ACTIVE" AND revision = generation AND revoked_at IS NULL AND
          revocation_reason IS NULL AND revocation_owner_id IS NULL AND
          ((generation = 1 AND last_operation_kind = "INSERT") OR
           (generation > 1 AND last_operation_kind IN
               ("RENEW", "BIND_EXECUTION_OWNER",
                "SUPERSEDE_EXECUTION_OWNER")))) OR
         (state = "REVOKED" AND revision = generation + 1 AND
          revoked_at >= renewed_at AND
          revoked_at IS NOT NULL AND revocation_reason IS NOT NULL AND
          last_operation_kind = "REVOKE" AND
          ((revocation_reason = "COHORT_REMOVAL" AND
            revocation_owner_id IS NULL) OR
           (revocation_reason IN ("STALE_HANDOFF", "CANDIDATE_ABANDONED",
                                  "COHORT_COLD_RECOVERY") AND
            revocation_owner_id IS NOT NULL))))),
  INDEX (cohort_id, expires_at) WHERE state = "ACTIVE",
  UNIQUE (execution_owner_api_instance_id)
    WHERE execution_owner_api_instance_id IS NOT NULL
}

serve_resource_action_worker_registration_handoffs = {
  cohort_id: Text REFERENCES serve_resource_action_worker_cohorts(cohort_id),
  handoff_id: UUID,
  predecessor_handoff_id: null | UUID,
  chain_sequence: PositiveInteger,
  stale_fence_disposition:
      "NEWLY_REVOKED" | "ADOPTED_ABANDONED_PREDECESSOR",
  source_cohort_revision: PositiveInteger,
  source_cohort_state: "ACCEPTING" | "DRAINING",
  source_registration_set_revision: PositiveInteger,
  source_registration_set: ProviderAuthorityWorkerRegistrationSetV2,
  source_registration_set_sha256: Sha256,
  stale_worker_instance_id: UUID,
  stale_pod_name: Text,
  stale_pod_uid: UUID,
  survivor_worker_instance_id: UUID,
  survivor_pod_uid: UUID,
  candidate_worker_instance_id: UUID,
  candidate_pod_name: Text,
  candidate_pod_uid: UUID,
  stale_uid_absence_proof: ProviderAuthorityWorkerPodUidAbsenceProofV1,
  stale_uid_absence_proof_sha256: Sha256,
  stale_authority_fence: ProviderAuthorityWorkerStaleAuthorityFenceV1 |
                         ProviderAuthorityWorkerStaleAuthorityFenceV2,
  stale_authority_fence_sha256: Sha256,
  candidate_registration: ProviderAuthorityWorkerRegistrationV2,
  candidate_registration_sha256: Sha256,
  survivor_registration: null | ProviderAuthorityWorkerRegistrationV2,
  survivor_registration_sha256: null | Sha256,
  handoff_state: "OPEN" | "READY" | "COMPLETED" | "ABANDONED",
  final_registration_set: null | ProviderAuthorityWorkerRegistrationSetV2,
  final_registration_set_sha256: null | Sha256,
  final_registration_set_revision: null | PositiveInteger,
  final_deployment_snapshot: null | ProviderAuthorityWorkerDeploymentSnapshotV2,
  final_deployment_snapshot_sha256: null | Sha256,
  committed_cohort_revision: null | PositiveInteger,
  candidate_absence_proof: null | ProviderAuthorityWorkerPodUidAbsenceProofV1,
  candidate_absence_proof_sha256: null | Sha256,
  survivor_absence_proof: null | ProviderAuthorityWorkerPodUidAbsenceProofV1,
  survivor_absence_proof_sha256: null | Sha256,
  candidate_zero_effect_proof:
      null | ProviderAuthorityWorkerCandidateZeroEffectProofV1,
  candidate_zero_effect_proof_sha256: null | Sha256,
  abandonment_reason: null | "candidate_absent_zero_effect" |
                              "both_members_lost_cold_recovery_required",
  revision: PositiveInteger,
  opened_at: UtcTimestamp,
  fenced_at: UtcTimestamp,
  survivor_acknowledged_at: null | UtcTimestamp,
  terminal_at: null | UtcTimestamp,
  PRIMARY KEY (cohort_id, handoff_id),
  FOREIGN KEY (cohort_id, predecessor_handoff_id) REFERENCES
      serve_resource_action_worker_registration_handoffs(cohort_id, handoff_id),
  CHECK (predecessor_handoff_id IS NULL OR
         predecessor_handoff_id != handoff_id),
  CHECK ((stale_fence_disposition = "NEWLY_REVOKED" AND
          predecessor_handoff_id IS NULL AND chain_sequence = 1) OR
         (stale_fence_disposition = "ADOPTED_ABANDONED_PREDECESSOR" AND
          predecessor_handoff_id IS NOT NULL AND chain_sequence > 1)),
  UNIQUE (cohort_id, predecessor_handoff_id)
      WHERE predecessor_handoff_id IS NOT NULL,
  UNIQUE (cohort_id, source_cohort_revision, chain_sequence),
  UNIQUE (cohort_id) WHERE handoff_state IN ("OPEN", "READY"),
  UNIQUE (cohort_id, candidate_pod_uid)
}

serve_resource_action_worker_registration_cold_recoveries = {
  cohort_id: Text REFERENCES serve_resource_action_worker_cohorts(cohort_id),
  recovery_id: UUID,
  source_cohort_revision: PositiveInteger,
  source_cohort_state: "ACCEPTING" | "DRAINING",
  source_registration_set_revision: PositiveInteger,
  source_registration_set: ProviderAuthorityWorkerRegistrationSetV2,
  source_registration_set_sha256: Sha256,
  old_uid_absence_proofs:
      SortedList<ProviderAuthorityWorkerPodUidAbsenceProofV1>,  # exact two
  old_uid_absence_proofs_sha256: Sha256,
  old_authority_fences:
      SortedList<ProviderAuthorityWorkerColdRecoveryFenceV1 |
                 ProviderAuthorityWorkerColdRecoveryFenceV2>,  # exact two/same version
  old_authority_fences_sha256: Sha256,
  final_registration_set: ProviderAuthorityWorkerRegistrationSetV2,
  final_registration_set_sha256: Sha256,
  final_registration_set_revision: PositiveInteger,
  final_deployment_snapshot: ProviderAuthorityWorkerDeploymentSnapshotV2,
  final_deployment_snapshot_sha256: Sha256,
  committed_cohort_revision: PositiveInteger,
  completed_at: UtcTimestamp,
  PRIMARY KEY (cohort_id, recovery_id),
  UNIQUE (cohort_id, source_cohort_revision)
}

serve_resource_action_worker_process_supersessions = {
  cohort_id: Text REFERENCES serve_resource_action_worker_cohorts(cohort_id),
  supersession_id: UUID,
  authority_worker_instance_id: UUID,
  operation_id: UUID,
  source_lease_generation: PositiveInteger,
  source_lease_revision: PositiveInteger,
  committed_lease_generation: PositiveInteger,
  committed_lease_revision: PositiveInteger,
  prior_api_instance_id: UUID,
  current_api_instance_id: UUID,
  prior_execution_owner: ProviderAuthorityWorkerExecutionOwnerV1,
  prior_execution_owner_sha256: Sha256,
  current_execution_owner: ProviderAuthorityWorkerExecutionOwnerV1,
  current_execution_owner_sha256: Sha256,
  container_supersession_proof:
      ProviderAuthorityWorkerContainerSupersessionProofV1,
  container_supersession_proof_sha256: Sha256,
  request_claims: SortedList<
      ProviderAuthorityWorkerTerminalFenceClaimV2>,
  request_claims_sha256: Sha256,
  completed_at: UtcTimestamp,
  PRIMARY KEY (cohort_id, supersession_id),
  UNIQUE (cohort_id, operation_id),
  UNIQUE (prior_api_instance_id),
  UNIQUE (current_api_instance_id),
  CHECK (supersession_id = operation_id AND
         source_lease_generation > 0 AND source_lease_revision > 0 AND
         source_lease_generation = source_lease_revision AND
         committed_lease_generation = source_lease_generation + 1 AND
         committed_lease_revision = source_lease_revision + 1 AND
         prior_api_instance_id != current_api_instance_id AND
         prior_api_instance_id != authority_worker_instance_id AND
         current_api_instance_id != authority_worker_instance_id AND
         jsonb_typeof(prior_execution_owner) = "object" AND
         jsonb_typeof(current_execution_owner) = "object" AND
         jsonb_typeof(container_supersession_proof) = "object" AND
         jsonb_typeof(request_claims) = "array" AND
         jsonb_array_length(request_claims) <= 16 AND
         CASE WHEN jsonb_typeof(prior_execution_owner) = "object" AND
                        jsonb_typeof(current_execution_owner) = "object" AND
                        jsonb_typeof(container_supersession_proof) = "object"
              THEN prior_api_instance_id::text =
                       prior_execution_owner ->> "api_instance_id" AND
                   current_api_instance_id::text =
                       current_execution_owner ->> "api_instance_id" AND
                   authority_worker_instance_id::text =
                       prior_execution_owner ->> "authority_worker_instance_id" AND
                   authority_worker_instance_id::text =
                       current_execution_owner ->> "authority_worker_instance_id" AND
                   authority_worker_instance_id::text =
                       prior_execution_owner ->> "pod_uid" AND
                   authority_worker_instance_id::text =
                       current_execution_owner ->> "pod_uid" AND
                   authority_worker_instance_id::text =
                       container_supersession_proof ->> "authority_worker_instance_id" AND
                   prior_api_instance_id::text =
                       container_supersession_proof ->> "prior_api_instance_id" AND
                   current_api_instance_id::text =
                       container_supersession_proof ->> "current_api_instance_id"
              ELSE FALSE END IS TRUE),
  INDEX (cohort_id, authority_worker_instance_id, completed_at)
}

serve_resource_action_api_instance_gc_cursors = {
  cursor_name: "authority-worker-v2",
  sweep_epoch: NonnegativeInteger,
  sweep_upper_bound_instance_id: null | UUID,
  after_instance_id: null | UUID,
  revision: PositiveInteger,
  last_operation_id: UUID,
  updated_at: UtcTimestamp,
  PRIMARY KEY (cursor_name),
  CHECK (cursor_name = "authority-worker-v2"),
  CHECK ((sweep_upper_bound_instance_id IS NULL AND
          after_instance_id IS NULL) OR
         (sweep_upper_bound_instance_id IS NOT NULL AND
          (after_instance_id IS NULL OR
           after_instance_id <= sweep_upper_bound_instance_id)))
}
```

The process row has exactly its restrictive cohort foreign key. That parent is
already locked at class 3 before the class-4 insert. Lease, API-instance,
request, queue, lineage, selector, and shadow-history relations are deliberately
not foreign keys: implicit parent locks would violate the later explicit class-
5/14/15/16/17 order. Typed validation cross-checks them under those locks.

The GC cursor has no foreign key and is the class-13 predecessor of its class-14
API target. Its UUIDs are keyset markers, not retained identities, and may name
a missing row. First use creates the exact singleton at epoch zero with null
markers, revision one, a caller-minted operation UUID, and PostgreSQL time;
concurrent creation adopts any valid scheduling successor. The row grants no
deletion authority.

The three Deployment rollout fields are closed wire members. Both maxima are
JSON integers: `Recreate`, crossed values, Boolean/numeric aliases, and
percentage strings reject. Live V2 uses the V2 PodTemplate binding; the V1
binding remains readable only as immutable pre-039 audit history and cannot
authorize a V2 candidate, lease, claim, or effect.
The physical JSONB columns remain additive unions only so retained pre-039 V1
history can still be decoded. A handoff/cold-recovery row has one fence version
throughout; mixed exact-two cold arrays reject. At Serve039 activation every
nonterminal handoff is absent and the exact full-table `EXISTS` audit under
policy/cohort locks proves zero request or delivery rows for all four private
handlers in every state. Its fixed 60-second statement timeout/read failure
blocks activation. No pre-039 pristine,
claimed, or terminal private row is grandfathered. All new handoff/recovery
writes use V2, and no V1 fence can authorize membership, terminalization,
claiming, reduction, or readiness. A retained terminal V1 row is audit-only.

Serve039 runs one explicit migration transaction. Initial reflection accepts
only exact 038 or exact complete 039 at an old stamp; partial 039 fails before
mutation. Exact 038 takes `ACCESS EXCLUSIVE` on the existing writer prefix:
policy epochs (class 2) -> cohorts (class 3) -> handoffs (class 4) -> leases
(class 5) -> cohort refs (class 6) -> shadow parents (class 9) -> shadow
children (class 10). It cannot lock
a not-yet-created 039 relation. Exact-complete-039 old-stamp adoption instead
uses one merged global schedule: policy epochs (class 2); cohorts (class 3);
handoffs and process-supersession history in canonical relation order (class
4); leases (class 5); refs (class 6); shadow parents (class 9); shadow children
and execution history in canonical relation order (class 10); the API-GC cursor
(class 13); then lineage,
action selectors, shadow terminal history, shadow admission-fallback history,
shadow admission-fallback-progress history, and shadow settlement history in canonical relation order
(class 17). It never appends an earlier-class new relation after the 038 prefix.
Under the retained schedule it re-
audits, performs owner-triple ALTER/CHECK/index, adds/backfills the shadow-parent
route plus fallback-evidence pair under the temporary
`LEGACY_CONTROLLER` server default and exact CHECK/NOT-NULL program, replaces
the child execution-kind CHECK, creates process, FK-free GC cursor, lineage, selector, shadow
execution history, shadow terminal history, then FK-free shadow admission-
fallback history, FK-free shadow admission-fallback-progress history, and
FK-free shadow settlement history with its source-parent and reverse-target
partial unique indexes as applicable, post-reflects,
and stamps. It
holds the schedule throughout. The process table has only its cohort FK and the
migration performs no API-lineage DDL; API008 remains an independent runtime
gate. Activation/old-image rollback uses the same timeout-bounded full-table
audit. Lock timeout is
fully transactional. An exact complete 039 catalog at an old stamp adopts only
when all nine new relations are empty and every lease owner/hash/scalar is null;
the parent columns/CHECK/default must be exact and every retained parent must be
ordinary `LEGACY_CONTROLLER` with its complete fallback triple null. Nonempty
or incompatible partial state fails. The controlled full-table private-
handler audit remains exclusively in the independent runtime activation/rollback
gate; migration and stamp adoption never query API-lineage state. Bidirectional
038-writer/DDL lock-order tests are mandatory.

Stacked M5a PR #1240 also ships the dormant PostgreSQL-only Serve040 revision
and one 039/040-aware image; it does not mutate the 039 catalog during the
rollback matrix. After every active authoritative service has rotated at 039
to the exact one-set `ROLLBACK_EVIDENCE_CLOSURE` policy, the database has zero
bound action/private-shadow work and zero services in shadow, and all roles /
cohorts attest that M5a image, the server-owned target gate may run 040. The
gated target call alone registers the typed one-shot Serve040
`on_version_apply` callback through `safe_alembic_upgrade()` and online
`env.py`; the private registration binds a server-minted operation ID plus the
exact `serve_db` 039 -> 040 step, and the revision rejects a missing, direct,
offline, wrong-step, or duplicate registration before mutation. The migration
takes the advisory lock, discovers and locks every controller-owner /
service fence in canonical class-1/2 order, takes class-2 policy-table `ACCESS
EXCLUSIVE`, locks every active closure-policy row/freezes its successor key, then takes
class-9 shadow-parent `ACCESS EXCLUSIVE`; revalidates that whole gate; exact-
reflects default-bearing 039; drops only the `execution_route` server default;
post-reflects the otherwise identical catalog; and leaves a closed one-shot
handoff in the current `MigrationContext` while retaining every lock. After the
revision function returns, Alembic's `HeadMaintainer` advances 039 -> 040; the
registered callback then runs before outer commit on that identical connection
and transaction. It requires the exact step/heads/handoff, re-reads actual
`008/040/028`, recomputes the post-catalog hash, and revalidates the locked
service/predecessor inventory. For each service in canonical order it constructs
its distinct closure-predecessor-bound catalog proof, supersedes the prelocked
closure row, and inserts/exact-adopts the closed one-set head-040
`SCHEMA_HEAD_ADVANCE` successor. It consumes the handoff and revalidates the
whole successor set without committing or releasing a lock; the outer Alembic
transaction commits the DDL, version row, and policies together only after
`env.py`, still inside `context.begin_transaction()`, asserts that the exact
registration ran once and the handoff was consumed. Updating the
predecessor before inserting the successor preserves
the one-ACTIVE/service unique constraint. No backward lock is acquired because every class-2 table/
row lock precedes class 9. Failure leaves complete 039 unchanged; an unknown
commit adopts only complete 040 plus the full closed successor set, and no
committed physical/policy-head mismatch is legal. The M5a image re-attests
every process/cohort at actual 040 and reopens only under
exact policy/physical-head equality. It never downgrades, re-adds the default,
changes a route value, or admits M4 after rollback closure. Tests cover both
the initial #1240 Serve039/default-present catalog and the final
Serve040/default-absent catalog, every migration/writer lock direction, failure
on each side of DDL/stamp/policy activation, and forward-only recovery.

One shared `serve038_worker_state_check_constraints()` SQLAlchemy factory owns
the physical worker-table constraints in fresh metadata and migration DDL; the
catalog inspector compares every named normalized expression. Its exact
families are:

- `serve038_worker_lease_closed_shape_ck`: closed state/counter/operation/owner,
  60-second TTL, revoke-time, ID equality, renewal-registration JSON-object and
  at-most-65,536-byte stored rendering, and lowercase-hash shape;
- `serve038_worker_handoff_scalar_lineage_ck`: closed enums, positive/equal
  source revisions, worker/Pod equalities and identity distinctness,
  `opened_at=fenced_at`, and disposition/predecessor/sequence shape;
- `serve038_worker_handoff_pairing_state_ck`: every JSON/hash pair, root/array
  type, DTO-specific stored-size bound, lowercase hash, and exact OPEN/READY/
  COMPLETED/two-ABANDONED revision/nullability shapes;
- `serve038_worker_handoff_terminal_revision_ck`: final/embedded/committed
  source+1 relations, final snapshot pairing, conditional survivor absence, and
  terminal/ack timestamp nullability; and
- `serve038_worker_cold_required_json_ck` plus
  `serve038_worker_cold_revision_shape_ck`: source enum, positive/equal source
  revisions, required JSON/hash/root/size shapes, exact-two absence/fence/final-
  worker arrays, and final/embedded/committed source+1 relations.

Serve039 does not rewrite the shipped Serve038 factory. Its migration replaces
only the lease operation CHECK with named
`serve039_worker_lease_execution_owner_ck`, preserving every old clause while
adding triple-null bounded JSON/hash/scalar owner columns and the two V2
operation kinds. The scalar must equal the decoded owner's API instance ID and
has a partial unique index. Null owner remains legal only for retained pre-039 history and the short
zero-private-row binding gate; every live V2 accepted/candidate lease requires
the exact nonnull V2 owner. Typed transitions require `BIND` only from null and
`SUPERSEDE` only with the matching immutable supersession row; ordinary
`RENEW`/`REVOKE` preserve the owner/hash/normalized-process-scalar triple.
Fresh/upgrade catalog tests compare the
normalized replacement expression and prove the Serve038 downgrade is refused.
The separately named replacement shadow-child CHECK admits exactly
`api_request | legacy_direct_down | private_api_request`; the private value
requires `PRIMARY_LAUNCH | PRIMARY_DOWN`, `REQUEST_BOUND | COMPLETE`, and a
nonnull request ID/bind time, while `LAUNCH_CLEANUP_DOWN` rejects it. The
execution-history FK and typed writer/reader enforce the cross-table one-to-one
existence rule: exactly one history for a private child and none for either
legacy execution kind.
The separate named `serve039_worker_process_supersession_ck` owns the process
row's positive/equal source counters, exact committed-source-plus-one
relations, distinct process IDs, JSON root and at-most-16 list shape, lowercase
hash shapes, and stored-rendering byte ceilings. Typed validation additionally
requires both owner objects, their scalar IDs, the stable lease/Pod ID, the
container proof, every receipt, and the one completion timestamp to cross-bind;
the locked stable-lease transition rejects a candidate API UUID already present
in either process-ID column, except exact adoption of this operation.

Every JSON predicate uses a two-valued `CASE`/`IS TRUE`, so missing, wrong-type,
or JSON-null data fails instead of passing CHECK through SQL NULL. SQL owns all
row-local shapes; typed codecs additionally recompute canonical hashes and byte
bounds, align nested workers/proofs, and validate cross-row lease/handoff/cohort
relations under locks.

All typed JSON values and their hashes are byte-checked on every transition;
the source, candidate identity/registration, and stale-UID proof are immutable
from insert, while candidate/survivor terminal absence evidence is immutable
after its one write.
Lease insert is exactly generation/revision one. An `ACTIVE` lease has a hash-
valid fresh self-read V2 renewal registration, null revocation fields,
`renewal_registration.registered_at == renewed_at`,
`renewal_registration.worker.observed_at <= renewed_at`, and
`expires_at == renewed_at + 60 seconds`. A normal renewal uses
`GREATEST(clock_timestamp(), source.renewed_at,
new_renewal_registration.worker.observed_at)` and renews every 20 seconds by CAS from its exact
generation/revision, advancing both by exactly one and replacing only the
registration/hash and times while preserving the execution-owner/hash/
normalized-process-scalar triple; neither
constant is configurable. Only the retained bind/process-supersession programs
change that owner and advance both counters once under their distinct operation
kind. Revocation
preserves generation, registration/hash and renewal/expiry times, advances
revision by exactly one, and writes database `revoked_at >= renewed_at` plus one
closed reason. `STALE_HANDOFF | CANDIDATE_ABANDONED` requires the exact handoff
ID as `revocation_owner_id`, `COHORT_COLD_RECOVERY` requires the recovery ID,
and `COHORT_REMOVAL` requires a null `revocation_owner_id`; all revocations
preserve the separate execution-owner/hash/normalized-process-scalar triple
byte-for-byte. `REVOKED` is
terminal and cannot renew.
Every joined handoff, cold-recovery, abandonment, or lifecycle transaction that
inserts, renews, or revokes a lease computes its single operation timestamp as
the `GREATEST` of PostgreSQL time, affected prior lease renewal times, every new
or renewed registration worker observation, prior cohort state time when that
state changes, every affected private-request terminal lower bound when requests
terminalize, and owner/proof/API start-observation terms when ownership changes.
Where global lock order requires immutable
class-4 handoff or class-3 cold-recovery evidence to be inserted before class-5
lease locks, the lease terms
come from complete nonlocking snapshots and the later class-5 suffix requires
each snapshot plus proposed registration/request byte, generation, revision,
and timestamp term to remain exact; drift rolls the
uncommitted evidence back. All rows written by that operation reuse the same
value. A backward wall-clock step therefore cannot violate
`revoked_at >= renewed_at`, including two-member cold recovery and cohort
removal.
The closed SQL and typed row invariant is: `ACTIVE` means
`revision == generation`, null revocation fields, and `INSERT` exactly at
generation one or one of the V2-legal `RENEW | BIND_EXECUTION_OWNER |
SUPERSEDE_EXECUTION_OWNER` operations thereafter; `REVOKED` means
`revision == generation + 1`, `REVOKE`, a nonnull revoke time/reason, and the
reason-specific owner shape above. Every malformed combination is rejected on
write and on typed read rather than normalized. `STALE_HANDOFF |
CANDIDATE_ABANDONED` owner IDs resolve to exact same-cohort immutable handoffs;
`COHORT_COLD_RECOVERY` owner IDs resolve to exact same-cohort immutable cold-
recovery rows. Neither owner row may be deleted while a lease names it. An unknown insert, renewal, or revocation result adopts only exact expected
`last_operation_id/kind`, state, generation, revision, registration/hash,
execution-owner/hash/normalized-process-scalar triple,
revocation fields, and server-timestamp relationships. The caller mints the
operation UUID before its first attempt and reuses it; it never guesses
`clock_timestamp()`. A later valid renewal may supersede an unknown renewal but
is reported as supersession rather than false adoption; insertion is then
proved by ancestry, while revocation is terminal. The stale-fence transaction
revokes the stale lease; valid abandonment revokes the candidate lease. Lease
existence or freshness without accepted set membership grants nothing.
The source registration-set revision equals its embedded revision and
`source_cohort_revision`; `source_cohort_state` equals the locked
`ACCEPTING | DRAINING` cohort state, and neither lifecycle state nor revision
may change while the handoff is `OPEN | READY`. Its workers are
exactly the named stale and survivor stable instances. Every authority worker
instance ID is byte-equal to its canonical Pod UID; its distinct currently
accepted process API-instance ID comes only from the exact lease execution
owner. Handoff/cold recovery lock those current process API rows and enumerate
claims by their IDs; they never treat the stable Pod UUID as a request owner. The
candidate is not either source member, the absence proof's
namespace/name/stale UID equal the stale registration, and the candidate and
survivor evidence equal their named IDs and UIDs. The stale-authority fence
contains the preserved lease generation, prior ACTIVE revision, and post-revoke
revision equal to prior + one, plus every locked live claim in request-ID order;
for `NEWLY_REVOKED` the lease records `STALE_HANDOFF`, this handoff ID, and
`revoked_at=fenced_at`, while the fence's origin ID is also this handoff. An
adopting handoff copies that fence byte-for-byte and the immutable lease retains
`revocation_owner_id == origin_revoking_handoff_id`, which resolves to the root
handoff. The new `predecessor_handoff_id` names the immediate chain tip and
equals the origin only at sequence two. Validation is O(1): exact-read the immediate
predecessor and directly read the same-cohort root named by the copied origin,
compare source/fence/sequence invariants, and rely on unique predecessor/
sequence induction; never walk an unbounded ancestor chain. For `NEWLY_REVOKED`, handoff `fenced_at` equals the
embedded fence time and lease `revoked_at`. For an adopting handoff the embedded
origin time stays old, while its new `opened_at=fenced_at` uses one fresh
database time only as evidence metadata. Raw wall-clock values may repeat or
regress and never order a chain; `chain_sequence` and state CASes define
causality.
`NEWLY_REVOKED` uses `ProviderAuthorityWorkerStaleAuthorityFenceV2` and changes
every listed request/queue claim to a terminal `CANCELLED` request under
`stale_authority_fence.fenced_at`; it never requeues a `ReplayPolicy.NEVER`
request. The transaction reconstructs each exact selector, stores its canonical
hash in the fence entry, clears the complete claim, deletes the delivery, and
later requires the selector row to hash byte-equal. The adopting branch makes
no second request mutation and instead validates that immutable terminal fence
plus zero current stale claims. In either branch no unlisted claim remains
owned by that instance. The reducer later settles each old attempt and alone
may create attempt `n+1`. A completed final set contains
exactly the two current ACTIVE lease renewal registrations in canonical order.
Each is ID-equal and stable-projection-equal to its immutable post-fence
survivor or candidate anchor; resourceVersions, `observed_at`, and
`registered_at` may advance only through the validated lease renewal contract.
The final set's embedded
revision equals `final_registration_set_revision`, and its hash equals the
stored final hash. `final_registration_set_revision ==
committed_cohort_revision == source_registration_set_revision + 1 ==
source_cohort_revision + 1`. Its snapshot is byte-equal to the separately
hashed final snapshot, and completion preserves `source_cohort_state`.
The OPEN insert has `opened_at == fenced_at`; later transition timestamps are
diagnostic metadata and need not compare monotonically across transactions.
The `OPEN -> READY -> COMPLETED` revision/state CASes, exact handoff ID/sequence
in the acknowledgement, and fresh bounded evidence establish causality.
Abandonment has null survivor time only when it leaves `OPEN`.
At `OPEN`, candidate registration/hash are byte-equal to the initial candidate
lease renewal-registration/hash; registration `registered_at`, lease
`renewed_at`, and row `opened_at=fenced_at` use the same database time, and the
lease is generation/revision one with `last_operation_kind=INSERT`. Candidate
abandonment revokes that lease with `CANDIDATE_ABANDONED`, this handoff ID,
`last_operation_kind=REVOKE`, and `lease.revoked_at == handoff.terminal_at`.
Lost-ack adoption verifies every immutable handoff equality plus the candidate
API row's exact bound phase, immutable boot/stable identity, and owner hash. It
accepts the candidate lease either at that exact generation/revision-one insert
(including the caller's operation ID) or as a valid same-stable-identity ACTIVE
descendant through only legal renewal/process-supersession transitions.
Bootstrap plus unchanged source proves an uncommitted attempt; partial bound /
lease/handoff evidence blocks. A terminally superseded handoff is reported as
such.
The only nonterminal enrichment is the one write-once survivor registration on
`OPEN -> READY`. `COMPLETED` requires all final fields, a byte-equal final
snapshot, and null abandonment fields; `OPEN -> ABANDONED` is exactly revision
two and `READY -> ABANDONED` exactly revision three and requires null final fields,
write-once candidate absence/zero-effect pairs, and a nonnull reason. The
survivor absence pair is nonnull exactly for
`both_members_lost_cold_recovery_required` and is an exact UID-qualified proof;
lease expiry or health state is insufficient. No other state
edge exists. Handoff rows are retained with registration history and cannot be
garbage-collected while a registration-lease owner, successor self-FK, cold-
recovery preserved owner, action, attempt, request, cohort reference, policy
epoch, or rollout evidence row can name them, their cohort, or their worker
instances. Every such owner ID must resolve by typed read to the exact same-
cohort handoff; the JSON cold-fence owner cannot bypass origin retention.

`NEWLY_REVOKED` requires null predecessor and `chain_sequence=1`. A chained
`ADOPTED_ABANDONED_PREDECESSOR` requires its nonnull predecessor to be the exact
immediately prior terminal same-cohort `NEWLY_REVOKED |
ADOPTED_ABANDONED_PREDECESSOR` handoff with reason
`candidate_absent_zero_effect`, an
identical source set, stale identity, stale-UID absence proof, and stale-
authority fence. The stale lease retains the recorded generation and exact
post-revoke revision, a locked scan finds zero current stale claims, the accepted survivor
and both its registration and API server-instance leases remain fresh, and the
new candidate is distinct. The new row has
`chain_sequence == predecessor.chain_sequence + 1`; under the cohort lock, the
typed writer requires that predecessor to be the greatest-sequence unadopted
terminal tip for the retained source/fence. The partial unique predecessor
index rejects a second adopter or an older-row branch, and the scoped
source-revision/sequence unique key rejects duplicate roots or positions. A predecessor with survivor absence, nonzero or
unknown candidate effect, changed membership, or any unequal fence cannot be
adopted. Chaining may repeat while the same survivor remains fresh; the
immediate self-FK, immutable insert, unique predecessor, and consecutive
positive sequence prohibit dangling, branching, cross-cohort, or cyclic
provenance without depending on wall-clock ordering. Full-set cold recovery is
reserved for exact absence of both accepted members.

The exact replacement sequence is:

1. `maxSurge=0,maxUnavailable=1` removes one old Pod and starts one candidate.
   The candidate serves `/livez` and `/bootstrapz` and becomes Kubernetes-ready
   on `/bootstrapz`, but keeps application `/readyz=false`, advertises no
   claimant, rejects action preflight, and cannot acquire an API request. It
   self-attests and submits only its own fresh V2 identity; before `OPEN` there
   is no candidate registration lease to orphan or renew.
2. The surviving API-role handoff verifier independently exact-GETs the stale
   Pod name and obtains one of the two proofs above; candidate-supplied absence
   is not trusted. `deletionTimestamp`, a terminating Pod, an expired lease, or
   a name-only observation is not absence. Exact HTTP NotFound or the same name
   with a different current UID is required. Nonlocking discovery also reads the
   bounded stale request-then-queue inventory and current lease snapshots needed
   for an optimistic closed fence.

   The PostgreSQL transaction validates the proof, locks the cohort/source V2
   set, and requires membership exactly stale plus survivor. From the complete
   discovered stale/survivor lease snapshots it computes the proposed logical
   operation time as `GREATEST(clock_timestamp(), stale.renewed_at,
   survivor.renewed_at,
   candidate_registration.worker.observed_at,
   candidate_execution_owner.container_started_at,
   candidate_execution_owner.observed_at, candidate_api_instance.started_at,
   <every discovered private_request_terminal_lower_bound>)`, omitting the last
   term for an empty inventory. For adoption it
   exact-reads and validates the immutable terminal predecessor/root before any
   later-class lock. With one database time it first inserts the complete
   `OPEN` handoff at class 4. It then visits stale, survivor, and absent candidate
   registration-lease keys in canonical order at class 5: locks both accepted
   rows, requires each locked row to remain byte-, generation-, and revision-
   equal to its discovered snapshot, requires the survivor ACTIVE and fresh,
   rechecks the candidate key
   absent before inserting its generation/revision-one lease, and requires the
   stale row ACTIVE for `NEWLY_REVOKED` or exact terminal `STALE_HANDOFF` for
   adoption. The new branch stages the stale revocation; adoption preserves it.

   Only then does it lock the stale lease's prior execution-owner API row, the
   survivor lease's current execution-owner API row, and the candidate's
   process-unique bootstrap API row in UUID order and require the survivor and
   candidate rows fresh. It revalidates the candidate's immutable boot/stable
   identity and stages bootstrap-to-bound with the inserted lease's exact owner
   hash in the same transaction; no commit can expose the candidate lease with a
   bootstrap/null-hash API row. It then locks all discovered request rows in
   request-ID order and only then all
   corresponding queue rows in request-ID order. It reruns the
   bounded stale inventory under those locks, requires exact equality/no
   unlisted claim, and invokes the one borrowed batch core. That core
   terminalizes all locked requests `CANCELLED`, deletes all locked queues,
   key-share-locks every named action lineage in canonical lineage-key order,
   inserts/exact-adopts action selectors in `(action_id, attempt)` order,
   inserts/exact-adopts shadow terminal histories in
   `(decision_id, request_sequence)` order, and only then allocates terminal
   events in request-ID order. An action arm with lineage uses
   `REQUEST_CANCELLED/LINEAGE`; an action arm without lineage uses
   `TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START`; a shadow arm writes
   exact `REQUEST_CANCELLED/SHADOW_EXECUTION` for an `AUTHORIZED` history or
   `TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START` for `BOUND`, with the
   corresponding nonnull/null lineage hash. The batch uses only the trusted
   `STALE_OWNER_FENCE` mode, never creates missing lineage, never calls the
   scalar terminalizer, and never requeues the old request. Any drift,
   including generic terminalization that does not hold the cohort lock, rolls
   back the earlier uncommitted handoff/lease/revocation; no earlier lock class
   is acquired after this suffix and no Kubernetes/provider I/O occurs under
   SQL locks. Immediately before commit, fresh PostgreSQL time must still be
   before survivor/candidate registration and API lease expiries, with every
   registration/Kubernetes proof inside its fixed bound; a wait past TTL rolls
   back. A lost result adopts
   the exact immutable handoff, the candidate API row's exact bound phase/boot /
   stable identity/owner hash, plus the candidate lease either at its exact
   generation/revision-one insert (including the caller's operation ID) or as a
   valid same-stable-identity ACTIVE descendant through only legal renewal/
   process-supersession transitions. Bootstrap plus unchanged source is an
   uncommitted attempt and permits a full retry; partial or unequal joined
   evidence blocks. A terminally
   superseded handoff is reported as such, not falsely adopted as `OPEN`. After commit, only that unique handoff
   candidate may renew; the lease still grants no claim or effect. No Kubernetes
   or provider I/O occurs while those SQL locks are held.
3. Nonlocking claim discovery grants nothing. The authority-claim transaction
   first locks the exact cohort and its nonterminal handoff, then the claimant
   registration lease, its normalized current execution-owner API row, request,
   and queue rows in the
   global order; opening a handoff takes the same prefix. This prevents a
   statement snapshot taken before `OPEN` from
   claiming after the fence. The claim query requires the claimant's stable
   authority-worker/Pod UUID to be in the currently accepted V2 set, that
   stable lease's execution owner to equal the caller's process API-instance
   UUID, both leases to be fresh against PostgreSQL `clock_timestamp()`, and
   that stable member not be the stale member of
   an `OPEN` or `READY` handoff. Therefore the fenced stale Pod and the
   not-yet-member candidate cannot claim; the accepted survivor may continue.
   `OPEN | READY` also rejects every new `PREPARING` cohort reference while
   already-bound work remains recoverable.
   The same predicates are rechecked when an existing claim renews and before
   every handler effect.
4. After exact-reading `(handoff_id, chain_sequence, revision=1, state=OPEN)`,
   the survivor independently rereads its complete live Pod -> ReplicaSet ->
   Deployment chain and compares the exact handoff source. One
   transaction locks cohort -> handoff -> survivor lease -> that lease's
   normalized current execution-owner API row. The acknowledging caller process
   UUID must equal the owner JSON/scalar; the row's stable Pod/start identity and
   freshness are revalidated. It renews the lease from the fresh V2 registration
   with one acknowledgement time equal to
   `GREATEST(clock_timestamp(), source_lease.renewed_at,
   fresh_registration.worker.observed_at)` while preserving the owner triple,
   writes the exact same registration/hash once as
   the survivor acknowledgement, and changes `OPEN -> READY` by CAS. The candidate cannot fabricate or
   proxy the survivor acknowledgement. The acknowledgement names that exact
   handoff ID/sequence; the state CAS and bounded fresh proof establish causal
   order without comparing wall clocks. If the survivor is absent or cannot
   attest after observing OPEN, this cohort cannot complete the handoff.
5. Only after observing exact `READY` does the candidate read the final
   Deployment snapshot. It must be the same frozen Deployment/template and
   report generation equal to observed generation, exactly two desired,
   updated, ready, and available Pods, and zero unavailable Pods. A final
   PostgreSQL transaction relocks the cohort, registration set, handoff, both
   registration leases, both current execution-owner API server-instance rows
   in process-UUID order, and a zero-current-stale-claim inventory in the
   global order; revalidates every retained terminal receipt named by the stale
   fence, the zero-claim result, fresh
   survivor/candidate leases, exact two-Pod owner chains, and absence proof;
   constructs the two final registrations from the exact current lease renewal
   registration bytes after stable-projection equality with the immutable
   handoff anchors;
   atomically replaces only the stale membership with the candidate, advances
   the cohort and V2 set revisions and sole set-level Deployment snapshot, and
   changes the handoff to `COMPLETED` with the byte-equal separately hashed
   final set and snapshot. It does not repeat the OPEN transaction's request
   mutations or recreate deleted queues. Immediately before
   commit, fresh PostgreSQL time must still precede both registration/API lease
   expiries and keep every Pod/Deployment/absence proof within its fixed bound;
   drift or a wait past TTL rolls back and retries.
6. The candidate exact-reads/adopts `COMPLETED`, eagerly warms every fixed child,
   then performs a bound-to-ready CAS that locks cohort -> any nonterminal
   handoff -> its accepted stable lease -> the lease's normalized current-owner
   API row. It requires caller process UUID == owner JSON/scalar, exact bound
   phase with `health_detail.execution_owner_sha256 ==
   lease.execution_owner_sha256`, stable Pod/start identity and freshness, and
   the stable candidate still in the matching accepted V2 set. Only that CAS or
   exact same-process adoption enables `/readyz`, preflight, and claims; a
   superseded process fails after warming. A lost acknowledgement at any
   CAS is recovered by exact `(cohort_id, handoff_id, revision, state, hashes)`
   readback; it is never repaired by replaying an insert or membership rewrite.
7. `OPEN` or `READY` may become `ABANDONED` only after the API verifier obtains
   an exact candidate-UID absence proof outside SQL. The no-I/O transaction
   locks cohort -> nonterminal handoff -> candidate registration lease, then
   the row named by that lease's normalized current execution owner, requiring
   owner JSON/scalar/API stable-Pod/start equality, then all candidate request rows in request-ID
   order, and then all corresponding queue rows in request-ID order if present. Every
   legal candidate claim, attempt, progress, provider-operation, and effect
   writer first needs that held prefix, so fail-closed scans of those later
   classes construct the closed zero-effect proof under the locks. Using one
   PostgreSQL timestamp, the CAS sets
   `candidate_zero_effect_proof.observed_at == handoff.terminal_at ==
   candidate_lease.revoked_at`, records `last_operation_kind=REVOKE`, writes both
   immutable proof/hash pairs, and terminally revokes the lease while changing
   the handoff to `ABANDONED`. An unknown result exact-reads this joined terminal
   evidence; no earlier zero-effect scan is replayable. The
   candidate may be deliberately terminated first, but a merely unhealthy or
   terminating candidate is insufficient. Once completed, its loss is a new
   replacement, never abandonment of the old handoff.
8. Loss of both previously accepted members before `COMPLETED` has no survivor
   acknowledgement path. The API verifier pre-reads exact UID-qualified
   survivor and candidate absence proofs, then the same locked transaction and
   database-time join constructs the candidate zero-effect proof and the handoff is
   retained as `ABANDONED` with
   `both_members_lost_cold_recovery_required`; recovery then uses the full-set
   same-cohort protocol below. Candidate self-attestation alone never
   reconstructs the old cohort's accepted membership.
9. Full-set cold recovery applies only to the same immutable cohort in
   `ACCEPTING | DRAINING` with exactly two V2 members and no `OPEN | READY`
   handoff. Any interrupted single-member handoff must first complete or reach
   its exact terminal `ABANDONED` proof. Two replacement Pods remain
   bootstrap-only and submit distinct fresh identities but have no registration
   leases or claims. The surviving API verifier independently proves exact UID-
   qualified absence of both accepted Pods, exact owner chains for both
   candidates, and an unchanged Deployment UID/generation/template/image/
   ServiceAccount with one fresh two-ready/two-available snapshot. Both
   candidate API-instance rows are fresh bootstrap rows at transaction entry and
   remain fresh through the atomic bound-phase commit.
10. Nonlocking discovery first reads both complete old lease snapshots and the bounded
    old request-then-queue inventories. One PostgreSQL transaction locks the
    cohort and empty nonterminal-handoff slot. It computes the proposed logical
    operation time as `GREATEST(clock_timestamp(), old_lease_1.renewed_at,
    old_lease_2.renewed_at,
    candidate_1_registration.worker.observed_at,
    candidate_2_registration.worker.observed_at,
    <both candidate execution-owner container start/observation and API start terms>,
    <every discovered private_request_terminal_lower_bound>)`, omitting the
    request term for an empty inventory. With that one database timestamp it
    fully constructs and inserts the immutable recovery row—source/proofs,
    optimistic fences, two candidate registrations, final set/snapshot, and
    terminal revisions—before any later-class lock. It then visits all old and
    absent candidate lease keys in canonical order at class 5, locks each old
    row, requires its bytes, generation, and revision to equal the discovered
    snapshot, and rechecks each candidate key absent before inserting both generation/
    revision-one ACTIVE leases, and stages each legal old transition. For an
    ACTIVE old lease its cold-recovery fence records the prior revision and
    revokes the lease at exactly revision + 1 with `COHORT_COLD_RECOVERY` and
    this recovery ID. An already-
    REVOKED old member is admissible only when its exact retained
    `STALE_HANDOFF` owner/fence proves the same member; its terminal lease bytes
    are preserved and its cold fence requires zero current claims. Any other
    prior state or uncertain evidence rejects.
    For ACTIVE, preserved-revocation fields are null, terminal lease revision is
    prior + 1, the claim list exhaustively covers request and queue mutations,
    and `fence.fenced_at == lease.revoked_at == recovery.completed_at`; the
    lease records `COHORT_COLD_RECOVERY`, this recovery ID, and
    `last_operation_kind=REVOKE`. For already REVOKED, terminal revision equals
    prior, preserved reason/owner equal the exact `STALE_HANDOFF` origin, all
    lease bytes/timestamps remain unchanged, claims are empty, and `fenced_at ==
    recovery.completed_at` is a fresh audit time rather than a second revoke.
11. Only after the evidence row and every class-5 change are staged does that
    transaction lock the two old leases' execution-owner API rows and the two
    candidate bootstrap API rows in UUID order, require both candidate rows
    fresh, revalidate both immutable boot/stable identities, and stage each
    bootstrap-to-bound transition with its inserted lease's exact owner hash.
    No commit can expose either candidate lease with a bootstrap/null-hash API
    row. It then locks every
    discovered old request row in request-ID order and only then every
    corresponding queue row in request-ID order. It reruns both bounded
    inventories and requires exact equality/no unlisted claim. A dedicated
    borrowed batch-terminalization core updates every request and deletes every
    queue, key-share-locks all named action lineages in canonical lineage-key
    order, inserts/exact-adopts all action selectors in canonical
    `(action_id, attempt)` order, inserts/exact-adopts all shadow terminal
    histories in `(decision_id, request_sequence)` order, and only after the
    entire class-17 set is valid allocates/emits all class-18 operational events
    in request-ID order. The batch uses only `COLD_RECOVERY_FENCE`, maps action
    arms with/without lineage to `REQUEST_CANCELLED/LINEAGE` or
    `TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START`, writes exact
    `REQUEST_CANCELLED` histories for shadow arms, never creates missing
    lineage, and never loops through the scalar terminalizer. It applies the V2
    terminal-cancelled fences atomically. Drift—including generic request terminalization that does not
    hold the cohort lock—rolls back every earlier uncommitted insert/update.
    Immediately before commit, fresh PostgreSQL time must still precede both
    candidate registration/API lease expiries and every candidate/absence/
    snapshot proof must remain in its fixed bound; a wait past TTL rolls back.
    It then updates the already-locked cohort/final V2 set by exactly one
    revision without changing lifecycle state and commits membership, claims,
    leases, and evidence atomically; no earlier class is acquired after the
    suffix. Each candidate registration `registered_at`, lease
    `renewed_at`, and recovery `completed_at` is that time, and each lease has
    `last_operation_kind=INSERT`. The final set is exactly the two candidates;
    old action specs, attempt identity/payload/progress, request payloads, and
    cohort references are unchanged, while each old request becomes terminal,
    its claim is cleared, its queue row is deleted, and its selector is appended
    exactly as fenced. Old attempts become reducer-eligible; only their later
    `n+1` requests can be claimed by the new members. No cross-cohort transfer,
    same-request requeue, or legacy route is created.
12. A lost result adopts only the exact `(cohort_id, recovery_id,
    source_revision, committed_revision, evidence hashes)` row plus matching
    cohort/final-set evidence, each candidate API row's exact bound phase/boot /
    stable identity/owner hash, and each candidate lease either at the recorded
    generation/revision-one insert or as a valid same-stable-identity ACTIVE
    descendant through only legal renewal/process-supersession transitions.
    Bootstrap rows plus unchanged source prove an uncommitted attempt and permit
    a full retry; partial or unequal joined evidence blocks. A
    later membership change is reported as
    supersession, never repaired by replay. Candidates enable `/readyz` and claims only after that
    exact read. Candidate loss before commit writes nothing; loss after commit
    is a new ordinary replacement or cold recovery. If the immutable Deployment
    UID/template no longer exists, cold recovery rejects. The chart must retain
    it while bound work exists; only a zero-bound-work retirement may create a
    new cohort.

The cold row's source cohort, set, and embedded-set revisions are equal. Its two
canonical source workers align one-for-one with the sorted UID-absence proofs
and cold fences; crossed worker/proof arrays reject. The final cohort/set/
embedded revisions all equal source + 1, and the final set is exactly the two
candidate lease registrations plus the separately byte-equal hashed snapshot.

Cold-recovery rows are immutable evidence inserted only while the exact cohort
row is locked; they are never independently row-locked and add no late lock
class after request/queue rows. Their unique source-revision key plus the cohort
CAS serializes insert/adoption. They are permanent membership history while any
registration lease owner, action, attempt, request, reference, policy, rollout
evidence, or registration history can name the recovery, cohort, or an old/new
worker instance.

```text

ProviderAuthorityWorkerAttemptAttestationV1 = {
  request_id: UUID,
  request_execution_generation: PositiveInteger,
  request_worker_id: Text,
  claimed_cursor_sha256: null | Sha256,
  before: ProviderAuthorityWorkerIdentityV1,
  after: null | ProviderAuthorityWorkerIdentityV1
}

In every Serve039/V2 use, `request_worker_id` remains the frozen V1 text field
but must be the one canonical lowercase hyphenated UUID rendering. Its parsed
UUID must equal the locked request and claimed queue `worker_instance_id`, the
lineage and terminal-selector process owner, the accepted lease's
`execution_owner_api_instance_id` and embedded `api_instance_id`, and the locked
API-instance row. This equation is rechecked at claim-start, every progress
checkpoint, immediately before every provider I/O, after each effect, and on
handler return. Separately, the stable projection of both `before` and nonnull
`after` equals the lease/registration `authority_worker_instance_id == pod_uid`.
The process UUID is never compared to or substituted for that stable Pod UUID;
a prior process after same-Pod supersession cannot attest for the successor.

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
  capsule. The target re-derived from the locked basis source, the preflight
  cleanup target, and the capsule cleanup target are byte-equal. The basis's
  `launch_cleanup_target_sha256`, preflight
  `cleanup_target_sha256`, capsule `cleanup_target_sha256`, and projected-subject
  `cleanup_target_sha256` all equal that recomputed input hash. No input includes
  or dereferences `down.execution_config`.

Execution-config/spec admission calls the kind-matched projector and requires
its complete canonical result to be byte-equal to the embedded policy subject.
It also requires every projector input to equal the corresponding invocation,
retained basis, and preflight field and to hash to the corresponding indexed
plan commitment. For launch, it additionally
compares the outer invocation field-for-field with the projected subject:
source, requested target, resources, topology, replica environment,
security-group scope, policy and managed-secret modes, retry flag, exact-resource
override, backend/optimizer, every boolean launch option, clone-disk value,
mount-blob value, and TLS-material value must equal the projector input or its
displayed fixed result. The invocation's execution-config field is compared to
the complete enclosing config but is not a projector input. For down, requested
target, workspace, cluster name, expected cluster-record UUID, complete prior
basis, purge/graceful flags, and timeout are bound to the plan hashes, cleanup
target, projected subject, and enclosing config; both basis and cleanup hashes are
recomputed from their complete typed preimages. A self-consistent subject/hash
graph with one changed target, retry flag, replica value, option, cleanup target,
cluster identity, or prior-basis hash is therefore invalid.

Every null, empty collection, and literal in
`ProviderKubernetesExecutionConfigV1` is semantic; execution may not replace it
with an ambient default. Every renderer, binding, normalization, inventory, and
runtime artifact has a retrievable repository path, byte size, and hash within
the exact approved executor-cohort image; no bare private hash is a preimage.
The action-aware candidate renderer is pure over the closed pre-object
`ProviderKubernetesRendererInputV1`, whose seed contains the policy-free
capsule fields, alongside the enclosing frozen resource identity and retained
source. No `objects` field exists at render time; the emitted bodies are later
appended to the unchanged seed and must compare byte-equal to the completed
capsule's three expectations. It does not call the generic config writer/bootstrap
path or rediscover user/workspace, image, CPU/GPU labels, RuntimeClass, storage,
queue, service account, SSH identity, port mode, pod config, mounts, or
credentials.
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
service-account UID, and image digest. Each Serve038 execution records the
byte-frozen `ProviderAuthorityWorkerAttemptAttestationV1` as execution-local
before/after provenance and separately requires current V2 membership. Using
downward-API
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
that same artifact. The stable instance ID derived from its V1 identity and its
stable identity projection must equal the V2 registration joined by the request
claim, while canonical `request_worker_id` equals the exact-current request/
queue/lineage/lease/API process-owner chain. That registration's separate lease
and current owner API row must still be fresh. The handler
claim-fenced-writes `claimed_cursor_sha256` and `before` after claim and before
the first mutating external effect. Immediately before
every CoreV1 or Skylet effect it
re-reads the full Pod -> ReplicaSet -> Deployment chain and requires every
identity field except the fresh `observed_at` to be byte-equal to `before`.
Immediately after every effect it re-reads the same chain: the first such read
fills `after` write-once, and later reads must equal that stored identity except
for a later `observed_at`. The before/after identity fields must equal each
other, and final success requires nonnull `after`. Within the generation-one
claim the attestation can change only from `after=null` to that one exact
post-effect identity. A new action attempt clears the attempt-scoped
attestation, binds its new generation-one claim to the carried cursor, and may
reconcile that cursor, but cannot repeat a committed effect. An unattested
worker rejects; a replacement Pod in the same frozen cohort can recover work
only after the old request is terminalized by the typed handoff/cold-recovery
fence, the reducer settles it, and a new attempt passes fresh membership claim
predicates. A
bootstrap-only candidate cannot recover work.

The values-level cohort `id` is only a version suffix `S`: a lowercase
DNS-label of at most 42 characters. Each authority installation has one
immutable operator-provisioned UUID `I`, unique among clusters/releases sharing
the central database. For release namespace `N` and rendered Helm full name
`F`, `manifest.cohort_id` is the database key
`"ra:" + I + ":" + sha256(N + "\\n" + F + "\\n" + S).hexdigest() + ":" + S`;
the fields are exact UTF-8 and each displayed `"\\n"` is byte `0x0A`.
Every database row and wire proof carries that full key, while rendered
resource names derive from `F` and `S`. The chart rejects a missing, changed,
or noncanonical installation UUID and any derived-key/name overflow. The
database primary key plus exact identity comparison makes duplicate-ID/hash
collision fail closed. Cross-release, cross-namespace, cross-installation, and
forced-collision tests cover this boundary. The full key names an immutable versioned Deployment, not a mutable
release-wide slot. Multiple cohort Deployments may run concurrently. The
action's frozen resolved cohort alone may claim it; a newer active cohort cannot
claim or reinterpret an older action. An old cohort remains deployed while any
nonterminal authoritative action or private shadow request/coverage attempt
references its resolved cohort identity. Template/image changes therefore
create a new cohort instead of invalidating in-flight launch or down recovery.

Each normal Serve038 live cohort values entry also requires the exact string
`manifestContract: provider_authority_worker_cohort_v2`. The chart deliberately
does not expose a numeric manifest-version selector: Helm normalizes integral
YAML/JSON numbers before template evaluation, so lexical `2` and `2.0` cannot be
distinguished reliably. Only that closed string contract may cause the chart to
render numeric `manifest.version=2` with
`claim_contract=frozen_action_cohort_join_v2`; missing, numeric, or alternate
discriminators fail closed. The one-time Serve034 `deselect` retirement phase
accepts only the previously shipped cohort values shape with this discriminator
absent and renders the byte-frozen numeric V1 manifest, V1 join contract, and
`Recreate` Deployment strategy. Supplying any discriminator in that phase is an
error, and `tombstone` renders no live cohort workload. The closed values schema
rejects the obsolete `manifestVersion` spelling. This is a values/render
boundary only; the persisted and wire manifest versions remain strict numeric
integers.

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
with an accepted V2 registration set in the Serve038 worker-cohort registry.
The shipped Serve033 V1 projection remains retirement/history-only. In the
suffix of a transaction already holding/revalidating owner -> service -> active
policy, PostgreSQL locks that cohort, rejects any nonterminal handoff, locks
both accepted registration leases in instance order, proves both leases fresh,
and inserts or exactly adopts the decision's nonexecuting
`PREPARING` retention reference, then releases every lock before the network
preflight. The authority-worker cohort created its registry identity during
rollout by self-attesting the same projected static manifest plus its live
Deployment and ServiceAccount UIDs. The first worker inserts `REGISTERING`; each
distinct ready Pod exactly adopts the immutable identity and appends its closed
V2 registration evidence. Only the typed two-worker V2 set plus its sole final
set-level Deployment snapshot changes it to `ACCEPTING`. Lost insert, append,
and promotion acknowledgements exact-read and adopt only the committed bytes
and revision. The action-specific
preflight response's `resolved_cohort` must be byte-equal to that registry
identity. Its V2 worker identity must be byte-equal to a
fresh member of the current accepted V2 set that is not blocked by an `OPEN` or
`READY` handoff. The reference authorizes no claim or I/O; it
only prevents retirement while prepared work is unadmitted.

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

That complete V1 graph and the `/v1/` transport are the byte-frozen Serve034
preflight/retirement baseline only. They cannot prepare or return evidence for
a live M4 action. M4 adds the following closed graph; a V1 request, manifest,
cohort, worker identity, capsule, or response never dispatches through it:

```text
ProviderLaunchPreflightSeedV2 = {
  version: 2,
  resource_identity: ProviderLifecyclePlanV2.resource_identity,
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

ProviderDownPreflightSeedV2 = {
  version: 2,
  resource_identity: ProviderLifecyclePlanV2.resource_identity,
  workspace: Text,
  requested_target: ProviderLocatorV1,
  prior_launch_basis: PriorLaunchBasisV2,
  prior_launch_basis_sha256: Sha256,
  cleanup_target: ProviderKubernetesCleanupTargetV1,
  cleanup_target_sha256: Sha256,
  context_mode: "in_cluster",
  config_projection: ProviderKubernetesConfigProjectionV1
}

ProviderLifecyclePreflightSeedV2 =
  ProviderLaunchPreflightSeedV2 | ProviderDownPreflightSeedV2

ProviderAuthorityPreflightRequestV2 = {
  version: 2,
  contract: "provider_kubernetes_preflight_v2",
  action_kind: "launch" | "down",
  nonce: UUID,
  seed: ProviderLifecyclePreflightSeedV2,
  expected_cohort_manifest: ProviderAuthorityWorkerCohortManifestV2,
  request_sha256: Sha256
}

ProviderLaunchAuthorityPreflightResponseV2 = {
  version: 2,
  contract: "provider_kubernetes_preflight_v2",
  action_kind: "launch",
  nonce: UUID,
  request_sha256: Sha256,
  disposition: "complete" | "not_representable",
  reason: null | ProviderLaunchNotRepresentableReasonV1,
  resolved_cohort: null | ProviderAuthorityWorkerCohortV2,
  execution_capsule: null | ProviderKubernetesExecutionCapsuleV2,
  executor_policy_proof: null | ProviderPolicyBoundaryProofV1,
  worker_identity: null | ProviderAuthorityWorkerIdentityV2
}

ProviderDownAuthorityPreflightResponseV2 = {
  version: 2,
  contract: "provider_kubernetes_preflight_v2",
  action_kind: "down",
  nonce: UUID,
  request_sha256: Sha256,
  disposition: "complete" | "not_representable",
  reason: null | ProviderDownNotRepresentableReasonV1,
  resolved_cohort: null | ProviderAuthorityWorkerCohortV2,
  execution_capsule: null | ProviderKubernetesDownExecutionCapsuleV2,
  executor_policy_proof: null | ProviderPolicyBoundaryProofV1,
  worker_identity: null | ProviderAuthorityWorkerIdentityV2
}

ProviderAuthorityPreflightResponseV2 =
  ProviderLaunchAuthorityPreflightResponseV2 |
  ProviderDownAuthorityPreflightResponseV2
```

The V1 leaf types intentionally reused inside V2 above do not encode the
cohort, worker membership, Deployment strategy, or enclosing protocol
version; their canonical behavior is unchanged. The additive V2 preflight
request/response parsers, serializer, `/v2/` endpoint, and exact bounded
transport goldens are an immediate activation gate. Until they are deployed,
the implementation must keep linked M4 authority disabled rather than route a
V2 action through the existing V1 transport.

V2 registration workers are canonically sorted by distinct Pod UID and have
distinct worker-instance IDs. `REGISTERING` requires one or two workers and a
null snapshot; `ACCEPTING` and `DRAINING` require exactly two and the sole
nonnull set-level snapshot. Every worker identity must match the row's complete
cohort, manifest/artifact/callable/handler hashes, Deployment UID/generation/
template, and ServiceAccount UID. No V2 worker identity stores the Deployment
resourceVersion. At initial `REGISTERING -> ACCEPTING`, a handoff or cold-
recovery final membership CAS, and `DRAINING -> ACCEPTING` rollback, every
installed registration's `registered_at` and embedded
worker `observed_at`, plus the installed snapshot's `observed_at`, are at or
before that transaction's fresh `clock_timestamp()` and no more than five
minutes old, and every installed member's separate lease expires after that
same timestamp. After that transition, renewable lease freshness and the live V1
stable-projection check gate preflight, claim, renewal, and effects; aging the
immutable registration evidence alone does not rewrite the set. Unknown, V1-
set, duplicate, unready, mixed, expired-lease, projection-mismatched, active-
handoff-stale, null-snapshot accepting, nonnull-snapshot registering, or status-
not-2/2 evidence cannot activate the cohort, answer preflight, or claim work.

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
immutable ConfigMap key mounted by individual read-only `subPath` at exactly
`/etc/skypilot/resource-action-authority/manifest.json` in controllers and the
matching worker cohort. It must be a nonsymlink regular bind-mounted file with
exact canonical bytes/size/hash; neither side performs a runtime ConfigMap GET.
A complete
response's `resolved_cohort.manifest` must be byte-equal to the request's
manifest, and its `manifest_sha256` must recompute from those bytes. The
authenticated V2 worker identity must be byte-equal to a registration in the
exact current accepted V2 set; that instance's separate lease must be fresh and
it cannot be the stale member of an `OPEN` or `READY` handoff. The accepted
set's final V2 Deployment snapshot, rather than a per-Pod Deployment
resourceVersion, supplies the rollout fence. The execution capsule's compact
`executor_cohort` ID and identity hash must equal the returned typed V2
cohort's recomputed ID and canonical hash. Any mismatch is not representable
and is never normalized away.

The installed `pod_template_contract` is the qualified pure builder/projector
implementation and closed schema; it is not treated as a generic expected
release template. V1 accepts exactly the installed source artifact
`sky/serve/resource_action_provider_preflight.py`: its descriptor-read source
bytes bind `projector_artifact_sha256`, while the closed release-input DTOs in
`sky/serve/resource_actions.py` bind the projector's schema. The runtime calls
that module's named pure projector only after the source reference resolves
byte-for-byte; an arbitrary same-shaped module or data file cannot select a
callable. The manifest's `pod_template_binding.release_inputs` is the trusted
complete release-specific preimage. It includes every Helm-varying Pod-template
byte: image/command/args/ports/resources, every literal/downward/Secret-backed
env entry, every release-specific Secret/ConfigMap name/key/path and
volume/mount, database credential reference, ServiceAccount name/pull-secret
settings, labels/annotations, security contexts, and all variable
scheduling/termination values. The v1 builder supplies the remaining closed
non-variable bytes: Pod token automount is exactly false; both Pod
`serviceAccount` fields equal the cohort ServiceAccount; one explicitly named
`kube-api-access` projected volume is mounted read-only at
`/var/run/secrets/kubernetes.io/serviceaccount`; and its sources are exactly a
3,607-second ServiceAccount token at `token`, `kube-root-ca.crt` key `ca.crt`
at `ca.crt`, and downward `metadata.namespace` at `namespace`. The cohort
ServiceAccount object itself retains explicit token automount true. This
suppresses the admission plugin's dynamically suffixed token volume without
removing the bootstrap observer's in-cluster credential. Unknown inputs,
environment names, volumes, mounts, containers, or template fields reject. The
builder artifact hash must equal
`projector_artifact_sha256`; it deterministically constructs the expected
PodTemplateSpec and recomputes `expected_template_sha256`.

The manifest hash has no recursive preimage. The builder places the literal
`$MANIFEST_SHA256` only at the one annotation JSON pointer and hashes that
placeholder template into `expected_template_sha256`. The static manifest
contains that binding but no computed manifest hash, so its canonical SHA-256
is then well-defined. Helm substitutes that final manifest SHA only into the
live Deployment annotation. Bootstrap requires the live annotation equal the
computed static-file hash, replaces exactly that value with the placeholder,
and then compares the rebuilt expected template to Deployment, ReplicaSet, and
Pod projections. The immutable manifest ConfigMap name is cohort-versioned but
not content-hash-derived. No other field may contain the placeholder or final
manifest hash.

The controller may use generated UIDs as preparation/execution evidence only
from this authenticated live response. The registry copy is a retention and
equality fence; it cannot populate a response or execution capsule. The
controller uses the returned UIDs only in the same bounded preparation cell.
It freezes only the compact cohort ID/hash reference into an admitted action
or private shadow record; the complete resolved V2 cohort remains in its
permanently retained registry row and transient V2 response. The response is
bound to its nonce, request hash, action kind, and expected manifest and cannot
be replayed for another preparation.

The live M4 endpoint is
`POST https://<full-name>-authority-preflight.<release-namespace>.svc:46583/internal/resource-actions/v2/kubernetes/preflight`.
The otherwise identical `/v1/` endpoint remains Serve034 retirement-only.
Here `<full-name>` is the Helm chart's rendered full name (for the current
test release, `skypilot-ha-authority-preflight.skypilot-ha.svc`).
It is served by a read-only extension of the authority executor's role-health
supervisor, not public FastAPI and not a request handler. The endpoint imports
only preflight construction and no action submit/session mutation callable.
Request and response are canonical JSON capped at 65,536 bytes; redirects are
disabled; connect timeout is one second and total timeout five seconds. The
controller may retry once after 100 ms with the identical nonce/body only for a
connection reset, timeout, or 502/503/504. It never retries a 2xx/4xx response.
Exhaustion, malformed bytes, nonce/hash mismatch, mixed cohort identity, or an
unequal capsule is not representable and never enqueues fallback work.

Transport authentication extends the existing token-ring module with the
purpose-specific strict parser below and constant-time comparison with a distinct
`SKYPILOT_RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE`. The purpose grammar
rejects the reserved `sky_` prefix used by every SkyPilot service-account API
token; LB-sync, controller-admin, and data-plane token bytes are compared from
their mounted rings and must be disjoint. A purpose-specific TLS
Secret supplies `tls.crt`, `tls.key`, and `ca.crt`, with a SAN for the exact
Service DNS. Authority workers mount cert/key/CA, controllers mount only CA
from that TLS Secret, and both separately mount/reread the purpose token ring.
Helm refuses authority enablement without
the two named existing Secrets. A ClusterIP Service selects only ready
authority-worker Pods, has no Ingress/LoadBalancer, and exposes only 46583. A
NetworkPolicy admits that port only from controller-role Pods in the release
namespace. P2a renders no controller egress policy: introducing the first
egress policy for that selector would silently isolate every other controller
destination. If a deployment already isolates controller egress, the cohort
stays disabled until its complete existing egress policy is explicitly
extended with the Service path and regression-tested.

The v1 HTTP/TLS edge is closed:

- P2a speaks HTTP/1.1 over TLS only. The server requires TLS 1.2 or newer and
  ALPN `http/1.1`. The leaf/key must match; the chain must terminate in the
  mounted purpose CA; and the currently valid leaf has `CA=false`,
  `serverAuth` EKU, and exactly one SAN: the full Service `dNSName`. CN
  fallback, wildcard/IP/URI/extra DNS SAN, plaintext, client-certificate mode,
  system CA, proxy, and hostname-disable paths reject. The client sends and
  verifies exact SNI/hostname equality. Of the TLS triple, controllers mount
  only `ca.crt`; they separately mount the purpose token ring. Workers mount
  `tls.crt`, `tls.key`, `ca.crt`, and the same purpose ring.
- The request is exactly `POST`, the literal path with no query/fragment,
  HTTP/1.1, and one each of `Host: <service-dns>:46583`,
  `Authorization: Bearer <ASCII-token>`,
  `Content-Type: application/json`, `Accept: application/json`,
  `Accept-Encoding: identity`, canonical decimal `Content-Length`, and
  `Connection: close`. `Content-Encoding`, `Transfer-Encoding`, `Expect`,
  `Proxy-Authorization`, `Cookie`, credentials outside Authorization,
  absolute-form targets, and duplicate headers reject. The request line is at
  most 2,048 bytes; at most 32 headers are allowed, each at most 8,192 bytes and
  32,768 bytes aggregate. Content length is 1..65,536; short or trailing body
  bytes reject. UTF-8 input must byte-equal canonical reserialization.
- The purpose ring file is at most 514 bytes and contains exactly one or two
  unique LF-delimited ASCII tokens, each 32..256 characters matching
  `[A-Za-z0-9._~+/=-]+` but not beginning `sky_`, with one final LF and no
  blank/CR/duplicate line. SkyPilot API service-account tokens always use that
  excluded prefix and only their hashes are retained centrally, so no chart
  Secret reference or reversible raw API-token inventory exists to compare.
  The server rereads it for every request and compares the candidate against all
  ring members without an early-exit timing branch. After bounded framing-header
  validation, ring validity and bearer comparison occur before reading any body
  byte; 401/503 closes without draining an unauthenticated body. Helm
  requires a Secret/key distinct from each configured LB-sync,
  controller-admin, and data-plane ring. Controller startup compares every
  mounted ring and rejects equal token bytes across trust domains; each client
  call rereads and repeats that comparison so a projected Secret rotation also
  fails closed.
- Status/body bytes are closed: 200 carries only the validated canonical typed
  response; every error is exactly
  `{"code":"<code>","version":1}`, where the sole mapping is 400
  `bad_request`, 401 `unauthorized`, 404 `not_found`, 405
  `method_not_allowed`, 408 `timeout`, 411 `length_required`, 413
  `body_too_large`, 415 `unsupported_media_type`, 431
  `headers_too_large`, and 503 `cohort_unavailable`. Unexpected exceptions
  become the fixed 503, never traceback. No branch echoes request bytes, token,
  filesystem, certificate, exception, or cohort detail.
- Every response is canonical JSON no larger than 65,536 bytes and has exactly
  one `Content-Type: application/json`, `Content-Length`,
  `Cache-Control: no-store`, `X-Content-Type-Options: nosniff`, and
  `Connection: close`. Only 401 adds `WWW-Authenticate: Bearer`; only 405
  adds `Allow: POST`. There is no `Server`, `Date`, compression, chunking,
  redirect, or free-form header. The client rejects duplicate/missing response
  headers, any content/transfer encoding, wrong/excess length, noncanonical
  body bytes, nonce/hash/cohort mismatch, or an invalid typed union.
- The listener uses a fixed eight-request worker pool and backlog 16, never an
  unbounded thread/process per connection and never more than one request per
  connection. Socket/header/body/evaluator/encode work has a five-second
  deadline. An admitted request without a worker/evaluator slot receives fixed
  503; a socket rejected before parsing is reset and falls under the client's
  one reset retry. Shutdown stops admission, clears `/bootstrapz`, drains only
  bounded in-flight work, and closes TLS.
- The client creates a fresh trust-isolated session for each attempt with
  `trust_env=false`, no proxy/netrc/cookie/system-CA state, the closed headers,
  exact DNS verification, no redirect, connect timeout at most one second, and
  wall-clock total at most five seconds. It retries once after 100 ms with
  byte-identical body/nonce only on reset, timeout, or 502/503/504; it never
  retries TLS/auth, any other 2xx/4xx/500, or a secondary token after 401.
- TLS files are opened descriptor-relatively from one Kubernetes projected
  Secret `..data` generation. A watcher fully validates leaf/key/CA, then
  atomically swaps the SSL context for new connections. Invalid/mixed rotation
  clears bootstrap and the local accepted snapshot and yields failed TLS/fixed
  503 rather than stale service. CA rotation is ordered: add old+new CA roots,
  swap the leaf/key, obtain two fresh worker registrations, then remove the old
  root. `Connection: close` bounds old-context lifetime. Token rotation uses
  the analogous two-member overlap then removal. Tests cover every intermediate
  generation, partial/invalid files, SAN drift, and recovery.

Before P2a deployment is called verified, a controller Pod using the mounted
CA/token must make the exact live call and receive the typed initial
`not_representable: preflight_unavailable_or_invalid`; unauthenticated,
wrong-purpose-token, wrong-CA, wrong-SAN, redirect, oversized, duplicate-header,
and concurrency-saturation canaries must receive only the closed failures.

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

The Serve adapter retains this already-shipped closed V1 wrapper unchanged for
pre-Serve038 history and exact-034 cleanup-only tooling:

```text
ServeReplicaActionSpecV1 = {
  version: 1,
  provider_plan: ProviderLifecyclePlanV1,
  invocation: ProviderLifecycleInvocationV1
}
```

V1 has no service-version identity or candidate/policy binding and therefore
cannot authorize M4 admission, dispatch, recovery, or provider I/O. Its exact
class, canonical bytes, hash goldens, and
`ServeReplicaActionSpecV1.from_value()` parser remain frozen; no field is added
and no V1 row is reinterpreted.

The parent design owns the following exact M4 values, reproduced here as the
provider-facet input contract:

```text
ServeServiceVersionSpecIdentityV1 = {
  version: 1,
  service_name: Text,
  service_incarnation: UUID,
  service_version: PositiveInteger,
  effective_service_config_sha256: Sha256,
  effective_task_config_sha256: Sha256,
  capacity_profile: ServeActionCapacityProfileV1,
  provider_profile: "pod_cluster_v1"
}

ShadowCandidateActionBindingV1 = {
  version: 1,
  binding_kind: "shadow_candidate",
  candidate_epoch: UUID,
  qualification_policy_sha256: Sha256,
  qualification_binding_sha256: Sha256
}

AuthoritativeActionPolicyBindingV1 = {
  version: 1,
  binding_kind: "authoritative_action",
  policy_epoch: UUID,
  policy_sha256: Sha256,
  authority_binding_sha256: Sha256
}

ServeReplicaActionAdmissionBindingV1 =
  ShadowCandidateActionBindingV1 | AuthoritativeActionPolicyBindingV1

ServeReplicaActionSpecV2 = {
  version: 2,
  service_version_spec_identity: ServeServiceVersionSpecIdentityV1,
  service_version_spec_identity_sha256: Sha256,
  admission_binding: ServeReplicaActionAdmissionBindingV1,
  provider_plan: ProviderLifecyclePlanV2,
  invocation: ProviderLifecycleInvocationV2
}
```

The compact live graph is exactly:

```text
ProviderAuthorityWorkerCohortReferenceV1 = {
  version: 1,
  cohort_id: Text,
  cohort_identity_sha256: Sha256
}

ProviderKubernetesExecutionCapsuleV2 =
  ProviderKubernetesExecutionCapsuleV1 with version 2 and
  executor_cohort: ProviderAuthorityWorkerCohortReferenceV1

ProviderKubernetesDownExecutionCapsuleV2 =
  ProviderKubernetesDownExecutionCapsuleV1 with version 2 and
  executor_cohort: ProviderAuthorityWorkerCohortReferenceV1

ProviderKubernetesExecutionConfigV2 =
  ProviderKubernetesExecutionConfigV1 with version 2 and the V2 launch capsule

ProviderKubernetesDownExecutionConfigV2 =
  ProviderKubernetesDownExecutionConfigV1 with version 2 and the V2 down capsule

ProviderLaunchLifecycleInvocationV2 =
  ProviderLaunchLifecycleInvocationV1 with version 2 and
  launch.execution_config: ProviderKubernetesExecutionConfigV2

ProviderDownInvocationV2 =
  ProviderDownInvocationV1 with
  prior_launch_basis: PriorLaunchBasisV2 and
  execution_config: ProviderKubernetesDownExecutionConfigV2

ProviderDownLifecycleInvocationV2 =
  ProviderDownLifecycleInvocationV1 with version 2 and
  down: ProviderDownInvocationV2

ProviderLifecycleInvocationV2 =
  ProviderLaunchLifecycleInvocationV2 | ProviderDownLifecycleInvocationV2

ProviderLifecyclePlanV2 = the version-2 closed plan binding only
ProviderLifecycleInvocationV2
```

This is deliberately not an edit to any V1 nested type. Only the additive V2
preflight response defined above may carry the complete V2 cohort transiently;
the persisted V2 capsule carries only the compact reference. A parsed reference
is non-authorizing. After the caller locks and parses the named permanent row as
`ProviderAuthorityWorkerCohortV2`, the authority module's sole typed resolver
`validate_locked_action_spec_cohort_v2()` recomputes that object's canonical
hash and proves exact ID/hash equality. The structural action module exposes no
scalar or V1-cohort convenience that can claim a lock or manufacture V2
authority. Every live V2 boundary repeats the typed resolution under its
documented lock order; no unlocked hash lookup or V1 fallback is permitted.

Every V2 payload member is immutable and covered by the complete action-spec
hash. The embedded identity hash is recomputed, and the existing plan/
invocation byte-equality, action-ID, action-kind, target, and
`request_payload_sha256 == invocation.sha256` invariants still apply. A private
shadow parent accepts only `ShadowCandidateActionBindingV1`, byte-equal to the
locked candidate/coverage tuple. An authoritative action accepts only
`AuthoritativeActionPolicyBindingV1`, byte-equal to its `ACTION_ACTIVE`
reference. Action admission requires the locked `ACTIVE/OPEN` policy row;
materialization, claim, provider-context, pre-I/O, and recovery instead require
the same frozen tuple under locked `ACTIVE/(OPEN | DRAINING)` current
execution. `CLOSED | SUPERSEDED` is accepted only by historical validation and
reduction, not execution. Its `policy_epoch` is an opaque
`uuid.UUID` in Python, canonical lowercase hyphenated UUID text in JSON, and
native PostgreSQL `UUID`; an integer, numeric string, sequence, policy
revision, or noncanonical spelling rejects.

The sole live provider parser is
`serve_replica_action_spec_from_value_v2()`. Manager admission, private request
materialization, claim resolution, provider-context loading, immediate pre-I/O
reauthorization, submit/observe, recovery, and reduction call it and reject V1
before creating or advancing a reference, request, attempt, watermark, or
provider effect. An optional version-dispatching inspection reader grants no
execution authority. Serve038's zero-action/shadow migration precondition
means no live V1 row needs a dual reader, backfill, or V2 reconstruction.

Every installed-package artifact path is normalized relative to the fixed
package root, contains no empty, dot, parent, absolute, or platform-separator
segment, and is opened descriptor-relative with no symlink traversal. Every
inventory entry must be a unique regular file with exact byte size and SHA-256;
missing, duplicate, extra, symlink, directory, device, path escape, or changed
bytes fail bootstrap. The separately projected qualification file uses its one
fixed mount path and the same nonsymlink/regular-file/size/hash checks, but is
not falsely included in the image-installed inventory.

`pod_template_contract` supplies the qualified pure builder/projector and
closed schema; `pod_template_binding.release_inputs` supplies every
release-specific value. Together they canonically construct/project every Pod-template-affecting
field: Deployment selector and template labels/annotations; every container's
name, digest image, pull policy, command, args, env name and literal/valueFrom
source, port, resource request/limit, security context, and volumeMount; all
init/ephemeral containers; volumes and projected item modes; ServiceAccount /
token automount/imagePullSecrets; startup/readiness/liveness probes; restart /
termination settings; Pod security context; DNS/host/network settings; node /
affinity/toleration/topology/scheduler/runtimeClass/priority fields. The live
Deployment projection, release-input hash, expected-template hash, and canonical
static manifest must all recompute exactly. An omitted field is represented by
the contract's explicit null/empty/default token, never ignored ambiently.

Bootstrap normalizes the live Deployment template through that exact projector,
requires the owning ReplicaSet template byte-equal after exactly one permitted
controller transform—adding one computed `pod-template-hash` label with the
same value to its selector and template—and requires no other RS drift. The
API-stored Deployment and ReplicaSet templates must each carry exactly null
`metadata.creationTimestamp`; the closed `api_default_values` rule verifies
and removes that field before comparison because the Pod projection below has
no ObjectMeta lifecycle field. The
observer first exact-validates Pod `apiVersion/kind/metadata.namespace`, its
identity, and the separately closed ReplicaSet ownerReference. It then
constructs exactly `{metadata:{labels,annotations}, spec}` rather than
comparing full-object TypeMeta/identity fields to a PodTemplateSpec, and removes
only scheduler-assigned `spec.nodeName`. Every other API/admission default is
made deterministic in the expected template: `serviceAccount`,
`schedulerName`, container termination-message fields, and probe
`successThreshold`/`timeoutSeconds` are explicit; Pod priority is zero,
preemption policy is `PreemptLowerPriority`, and the two unique final
`NoExecute` tolerations are, in order, `node.kubernetes.io/not-ready` and
`node.kubernetes.io/unreachable` with 300-second bounds. Release-input
tolerations using either reserved key with effect absent/null/empty (the
Kubernetes all-effects form) or `NoExecute` reject, so each fixed suffix entry
is unique. Every API-defaulted
spec/container field instead must equal the explicit default map in the
artifact. The artifact's canonical
`deployment_to_replicaset_rules`, `replicaset_to_pod_runtime_paths`, and
`api_default_values` are exhaustive closed maps; unknown ignored paths reject.
It separately recomputes the running
four-handler registry—names, entrypoint module/qualname, execution class,
replay/cancellation policy, aliases, and strict result codec—and requires it
byte/hash-equal to the callable/handler inventories in the manifest. A merely
matching Pod annotation or manifest hash cannot substitute for any of these
three live comparisons.

Unknown keys and floats are rejected, and the canonical object is bounded to
65,536 UTF-8 bytes. `provider_plan.validate_invocation(invocation)` must pass;
the plan and invocation derive the enclosing action ID, and
`provider_plan.request_payload_sha256` equals `invocation.sha256`. The shadow
parent's separately indexed `provider_plan` and hash are an exact byte-equal
copy of the wrapper member, and a primary child's invocation is an exact
byte-equal copy of the wrapper invocation. For primary down, the plan carries
only `prior_launch_basis_sha256` and `prior_cleanup_target_sha256`; each equals
the complete basis or capsule cleanup-target preimage in the invocation.
Admission validates the referenced retained source before accepting either. A
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
authority. It cannot appear in live `ServeReplicaActionSpecV2`, a primary child,
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
down golden fixtures must include the observed 1,039-byte `boltz-test` CA scalar,
three complete requested/semantic object bodies, the full kind-specific
principal/authorization inventory, and all 12 prerequisite role records. The
launch golden additionally includes the exact five-role endpoint projection,
six runtime artifacts, and both endpoint callers with their complete live
Deployment projections. Down goldens contain none of those launch-only
endpoint/runtime/job fields, and insertion of any one rejects. Tests separately
cover completed-launch down and every legal partial-launch down, including
maximal committed-cleanup and legal null-slot/null-handle shapes. They record
each full live `ServeReplicaActionSpecV2` byte length, require
it to be at most 60,000 bytes (preserving at least 5,536 bytes of rollout
headroom), and still enforce the absolute 65,536-byte parser bound. Failure is
`NOT_REPRESENTABLE`; no truncation, compression, or unverified external
hash-only lookup is allowed. Initial implementation measurement activated this
gate: a realistic completed-launch down spec was 72,567 bytes, and a legal
`HANDLE_COMMITTED` partial-launch down was 183,137 bytes (28,716-byte cursor,
27,607-byte quiescence, 70,057-byte basis, and 100,781-byte invocation). The
cause was structural duplication, not an unbounded leaf. The corrected wire
contract stores the full basis only in the invocation, the full cleanup target
only in the execution capsule, retains its hash in the basis, and retains plan
hashes for both; the partial basis additionally references its locked cursor/
quiescence preimages by exact source key, revision, and hash as specified above.
Authority remains disabled until launch, completed-down, every legal partial-
down, and realistic/candidate-maximal full-spec goldens prove the revised shape
is at most 60,000 bytes and capped preflight request/response goldens satisfy
their independent 65,536-byte transport limits.

The P2a Helm-derived complete static cohort makes the current frozen V1
representative launch spec exactly 60,851 bytes. That remains parseable but
deliberately fails this activation gate and cannot qualify the larger V2
envelope; P2a may deploy only dark and must not raise the 60,000-byte budget.
Before P2b linked represented admission, replace the
capsule's 5,241-byte complete cohort with a closed compact durable reference
containing only `version`, `cohort_id`, and `cohort_identity_sha256`. The
complete cohort is already permanently retained in
`serve_resource_action_worker_cohorts`. Admission must lock that row, recompute
the canonical identity hash, and require exact equality before it materializes
or dispatches a request. The measured 231-byte reference projects the frozen
V1 representative fixture to approximately 55,841 bytes. That estimate omits
the required V2 service-version identity/hash and admission binding and is not
qualification; checked-in exact realistic and candidate-maximal V2 goldens
must prove the unchanged 60,000-byte gate.

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

When an ordinary counted provider slot is available, the manager performs the
parent design's short service -> replica -> cohort -> handoff -> registration-
leases -> reference -> coverage -> optional-parent transaction. It takes no
paid-capacity, reserved-fill, or
logical-capacity lock. Before signaling it requires the same-ID `PREPARING`
reference and atomically writes `worker_cohort_ref_id=decision_id`; mismatch or
rollback leaves no approval. Reference activation is branch-specific below.
For a legacy-SDK represented or not-representable branch, that same transaction
changes the reference to `SHADOW_ACTIVE`, and it signals only after commit or
exact lost-commit readback. A not-representable approval carries coverage and
an unguessable process-local nonce but grants no cross-process replay authority.
The same-cell worker rechecks owner/cancel/scope fences and commits its
represented child or coverage-only attempt `PRE_SUBMIT` before SDK request
creation.

For the private represented branch, the capacity transaction deliberately
leaves the reference `PREPARING`, writes the parent as
`PENDING_SELECTION/PENDING`, and signals nothing. The narrow follow-up
materializer locks the complete graph and atomically creates/exact-adopts the
represented child, preflight-bearing `BOUND` history with empty effect trace,
deterministic request, queue delivery, private correlation, binds the child to
that ID and `REQUEST_BOUND`, changes the parent to
`PRIVATE_API_REQUEST/RUNNING`, and commits the sole `SHADOW_ACTIVE` transition.
Only its commit or exact lost-ack adoption
lets the SafeThread enter its wait state. A crash in between leaves no claimable
request and recovery can exact-adopt or safely retain `PREPARING`.
Permanent enumerator rejection instead exact-adopts the declared
`LEGACY_CONTROLLER/RUNNING` fallback transition after proving zero private
descendants, then signals the same-cell legacy worker; retryable drift leaves
selection pending.
Construction/start of a PREPARING worker is
not mutation enqueue; release of this gate is the enforceable provider
boundary.

Only the represented branch may instead atomically materialize the deterministic
private request and represented child, then wait for its single claimed handler.
The coverage-only branch always takes the same-cell legacy SDK path above; it
has no private request or cross-process executable input.

The live worker uses the same in-memory request object and retained identity
proof until SDK request admission, but object identity is not a distributed
authority. Across HTTP the
durable contract is the full stored `ServeReplicaActionSpecV2`, including the
service-version identity/hash, shadow-candidate or authoritative-policy
binding, frozen execution config, scope, invocation, and retrievable
references. Generic
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
- Cut the same-ID renderer body contract over atomically: add the exact
  three-body validator/artifacts and remove every minimal/fake
  `admissionDefaults` fixture and acceptance in the same change. Gate its first
  execution on the read-only zero-persisted-represented-launch preflight above;
  never implement dual `kubernetes_admitted_object_v1` shape acceptance.
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
The 16,384-byte CA scalar ceiling covers the 1,039-byte canonical DER base64
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

Further P1 launch-identity verification evidence on 2026-08-02: fixed
canonical byte-size/hash fixtures cover the closed input, context, request,
proof, and response chain. Focused tests cover exact scalar types and bounds,
constant-time capability/store validation, one-session/no-mutation behavior,
post-auth and explicit no-auth identity replacement, exact raw HTTP statuses,
OAuth nonredirecting 401 behavior, a 65,536-byte body/response cap, fail-closed
missing middleware state, and one byte-identical 100 ms retry only for reset,
timeout, or exact 503. Independent adversarial review accepted the corrected
full-value user-hash check and middleware boundary. This evidence does not
claim manager-side capability generation/discard, source projection, admission
binding, or any caller of the still-dark client.

Further P1 launch execution-config verification evidence on 2026-08-02: the
closed Kubernetes locator, content-addressed launch source and identity proof,
request-identity projector, execution capsule, nonrecursive policy subject,
complete launch execution config, and exact outer launch invocation are
implemented. The composition requires byte-equal scope, topology, workload,
source, policy, request body, resource, application-port, cohort, Skylet, and
object-plan projections; reserves every control-plane/Skylet port; and rejects
subtyped persisted wrappers, nested children, collection leaves, and crossed
capsule/config graphs. A six-file focused matrix passes all 397 tests against
PostgreSQL; targeted mypy is clean, pylint is 10.00/10, and independent
adversarial review reproduced the persisted-wrapper regressions and accepted
the tranche. This launch-only evidence does not claim provider I/O, preflight,
admission, manager integration, or authority.

Further P1 execution-contract verification evidence on 2026-08-02: immutable
down composition now includes both completed-launch and partial-launch bases,
the exact three-object cleanup target, a current down-only execution capsule
and policy subject, the complete down execution config, and the exact outer
down invocation. The reviewed launch/down tranche passed 239 pure cases and 41
PostgreSQL cases, including all 20 legal partial-down shapes and 40 frozen
launch/down spec goldens; every measured canonical spec remained below the
65,536-byte bound. The resolved-target tests require all three canonical roles
and reject the pre-release flattened wire.

Further P1 pure-renderer verification evidence on 2026-08-02: the five exact
LF-terminated artifacts, typed resolver wrappers, closed renderer input/seed,
17-binding set, exact three-body validator, pure request/admitted normalizers,
object-plan builder, and completed-capsule revalidation are implemented. The
60-case focused renderer/artifact/normalizer matrix passes; the complete
1,543-case `test_serve_resource_action*.py` matrix passes with the real
PostgreSQL test URL, including the atomically replaced launch/down body and
full-spec byte/hash goldens. The release-wheel test proves all five files are
embedded byte-for-byte. Descriptor traversal is bound to the opened regular
imported `sky` package and rejects symlinks, size/hash drift, or path
substitution. Runtime code-object checks plus source-AST tests close the staged
call graph, declared input accesses including zero-access entrypoints, extra
project helpers, executable imports, and forbidden ambient sources. The
validator scans each nested code-object identity exactly once and derives
source positions from `Instruction.positions.lineno`, with an exact-integer
`starts_line` fallback. Exact executable and AST seals pass on CPython 3.10.20,
3.11.15, 3.12.13, 3.13.14, 3.14.3, and the CI image's 3.14.6; this closes the
portability defect exposed by 3.14's boolean `starts_line` and repeated nested
`LOAD_CONST` references without widening the one-fingerprint-per-minor
allowlist. This
evidence is effect-free: it does not run preflight, create a provider client,
dispatch a private handler, persist a 201/409 result, or qualify an executor
cohort.

Further P1 progress and return-envelope verification evidence on 2026-08-02:
the pure Serve-owned API006 contract parses and validates launch/down cursors,
checks monotonic transitions and attempt/execution attestations, derives only a
lineage-valid retry seed, constructs supersession quiescence, and validates the
exact handler and fallback reductions. Its focused suite passed 72 cases, and
the generic PostgreSQL retry/materialization suite passed 40 real-database
cases, including predecessor locking and cross-generation attestation checks.
Strict encoders/decoders are registered only for
`sky.serve_resource_action_launch` and
`sky.serve_resource_action_down`; focused codec/name, PostgreSQL, and SQLite
runs verify that null, malformed, or hash-invalid returns terminalize failed
instead of persisting a successful null/untyped result. This evidence is for
immutable values and persistence behavior only: it does not implement or prove
runtime integration or live qualification of the implemented pure renderer
and normalizers, preflight, runtime admission/session, dispatcher, provider
I/O, shadow parity, or live authority.

First-deployment cutover evidence completed on 2026-08-02. The read-only
`boltz-test` baseline at Helm revision 57 had API004, upstream
request-classification Serve032, global-user-state 027, every ordinary role on
digest
`sha256:d05257c3018c570861104c6c0a509c92d29af93df2d167a58e50d6748a1590a1`,
zero services/replicas/clusters, and no resource-action tables. Revisions 58–60
performed the dark API -> ordinary executor -> controller upgrade to source
commit `a836825ef` and digest
`sha256:c5f1306f91c7fe2db151c34131ca4cd39be9beba3d21d170f5757996338f375e`.
Revision 61 returned all three ordinary roles to the compatible baseline image
with the current chart while retaining global-user-state 028, Serve033, and
API007. Revisions 62–64 repeated the staged upgrade and left the new digest as
the final state. The exact per-revision evidence is recorded under Deployment
and rollback. This proves that the unshipped feature-only Serve032 shape was
absent and that the guarded Serve033 migration handled the upstream catalog;
it does not prove this provider facet.

### P2a: preflight-only cohort bootstrap

- Add exactly six closed canonical envelopes: launch and down preflight seeds,
  the discriminated authority request, launch and down responses, and their
  discriminated response union. The request binds its action kind, fresh nonce,
  self-hash, expected manifest, and complete seed. A complete response must
  carry the kind-matched capsule and proof and exact returned
  cohort/manifest/worker identity; a not-representable response must carry
  only its closed reason. Every forbidden branch field is null, every hash is
  recomputed, and each canonical request/response is at most 65,536 bytes.
- Add the purpose-token-authenticated private HTTPS server and strict client at
  the exact Service DNS. The client trusts only the mounted purpose CA, follows
  no redirect, uses one-second connect/five-second total timeouts, and makes at
  most one byte-identical retry after 100 ms for reset, timeout, 502, 503, or
  504. It uses a fresh session with `trust_env=false`. The endpoint imports no
  submit, provider-mutation, execution-session, or request-store callable and
  uses the exact method/header/status/body/concurrency/rotation contract below.
- Split Kubernetes bootstrap readiness from queue-claim readiness. The role
  binds health and preflight listeners first; `/bootstrapz` becomes ready once
  the static manifest, TLS/token files, and local server are valid, and is the
  P2a Pod startup and readiness probe. `/readyz` remains false because P2a
  never starts a request executor. `/bootstrapz` also fails while the role drain
  marker exists. The preflight route returns 503 until this Pod has adopted an
  `ACCEPTING` cohort.
- The API-request `ServerInstanceLease` is deliberately not queue-ready in
  P2a. It remains `ready=false` with closed
  `health_detail={"phase":"preflight-only"}` after bootstrap and cohort
  acceptance; this runtime path has no call to `set_ready(true)`. Kubernetes
  readiness therefore comes only from `/bootstrapz` and cannot accidentally
  advertise a claim-capable authority executor.
- In a bootstrap coordinator separate from the endpoint, load and byte-verify
  the complete projected manifest and its installed-package artifact refs;
  byte-verify the separately projected post-build qualification artifact; read
  the live Pod -> ReplicaSet -> Deployment owner chain and ServiceAccount; take
  the registration timestamp from a public read-only PostgreSQL-clock seam
  before those reads; and use the existing Serve033 register / compare-and-swap
  lifecycle primitives. Both Pods first become `/bootstrapz`-Ready, so the
  Deployment can report spec/status-total/updated/ready/available replicas all
  exactly two and unavailable replicas zero
  before either typed registration is constructible. The first process that
  observes exact Deployment `2/2` creates `REGISTERING` with its own
  registration; the peer reads that row and appends its own registration only
  after observing the same generation/resourceVersion. The set is then sorted,
  either process rereads that same Deployment snapshot and promotes, and the
  peer polls and adopts that same
  `ACCEPTING` identity. Stale-version
  retries discard the observation, take a new database timestamp, and repeat
  the exact live proof. Competing one-entry insert conflict is expected: the
  loser exact-reads the winner, verifies the immutable identity/Deployment
  snapshot, merges only its own entry, and compare-and-swaps. A lost
  insert/append/promotion acknowledgement first adopts exact stored bytes and
  revision; it does not reobserve and demand timestamp equality. They never
  merge unequal identities. If the append-only
  registration cannot finish inside the five-minute freshness bound, the
  API-role abort path may fence a never-accepted row only after proving zero
  references/evidence/actions/proofs, authorize removal, verify exact NotFound,
  and retire it; operators then use a new immutable cohort ID.
- Do not call `executor.start()`, construct an authority claim config, or claim
  any PR #1070 queue row in P2a. The four private handlers remain fail closed;
  no manager calls the endpoint; no `PREPARING`, `SHADOW_ACTIVE`, action,
  request, or queue row is created; no workload/action-provider client,
  Kubernetes mutation, or provider effect is reachable. Only the dedicated
  bootstrap observer may GET its own Pod, owning ReplicaSet, exact Deployment,
  and ServiceAccount. After acceptance, the initial evaluator returns only typed
  `not_representable: preflight_unavailable_or_invalid`. Complete live target
  observation is P2b.
- Expand the Helm cohort value into a complete static-manifest input: immutable
  image reference and registry manifest digest, OCI config digest, qualification
  artifact reference, Pod-template contract reference, artifact-inventory
  reference, callable-inventory reference, exact deployment/service-account /
  container names, claim contract, and four-handler allowlist. Project the
  canonical manifest read-only and bind its hash into the Pod template.
- Qualify an image without circular evidence: first merge/build the dark P2a
  runtime and inspect its immutable registry manifest and OCI config; then land
  a follow-up checked-in, chart-packaged qualification artifact and values
  selecting that exact prior image. The current chart projects the immutable
  artifact bytes beside `manifest.json`, allowing the prior image to verify
  their size/hash without pretending the file was installed in that image.
  Never derive the OCI config digest from a manifest digest or a live CRI
  `imageID`. `authorityWorker.enabled` remains false until that follow-up
  evidence is reviewed.
- P2a does not claim rolling replacement recovery. If the accepted two-Pod set
  changes or any manifest/live identity drifts, the continuing coordinator
  clears its process-local accepted snapshot, the endpoint returns 503, and
  queue readiness remains false. Each accepted process has a five-minute local
  lease from the oldest attested database timestamp. Before expiry, a watchdog
  may compare-and-swap `ACCEPTING -> ACCEPTING` only by replacing its own
  Pod-UID registration and preserving the peer entry byte-for-byte. The sorted
  UID set, Deployment UID/generation, ServiceAccount UID, immutable identity,
  and non-temporal proof fields cannot change; only that caller's timestamps /
  Pod/ReplicaSet resourceVersions may advance; Deployment resourceVersion is
  frozen across the pair. A CAS loser rereads and reapplies only its own
  entry; a replacement UID rejects immediately and a survivor clears acceptance
  no later than peer freshness expiry. Database-clock failure, future/stale
  time, Pod replacement, Deployment resourceVersion change, owner /
  generation drift, projected-byte drift, or failed renewal clears the lease
  and returns 503. Immutable conflicts terminate the process; transient
  observation failure stays unavailable. P3 still requires a separately
  reviewed rolling-replacement protocol before claims.

### P2b: live shadow observation

- The native V2 seed/input, launch/down constructors, and sole cleanup-target
  rederiver are implemented. First finish the final V2 six-role artifact and
  callable inventories and fully expanded representability case inventory /
  enumerator plus CI-only post-inventory goldens. The exact
  repository call inventory must reject any V1 construction/conversion root or
  duplicate cleanup builder, and both content-addressed fixture sets must pass
  before complete preflight or represented admission is reachable.
- Gate entry to provider preparation on the exact
  `ordinary_ondemand_physical_width1_v1` Serve projection, then recheck its
  elected `ServeServiceVersionSpecIdentityV1` and each live replica's bound
  creating-version identity under the service/version locks at admission. A failed gate
  stays wholly legacy and creates no preparation or evidence graph.
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

- Add a narrow represented-only PostgreSQL admission primitive that locks
  owner -> service/replica -> cohort -> handoff -> both leases -> reference ->
  coverage -> parent -> represented attempt/history -> both API rows -> request
  -> queue and atomically materializes and binds the sole private PR #1070
  request/queue row plus `BOUND` history before changing
  `PREPARING -> SHADOW_ACTIVE`. It validates the exact service owner/epoch,
  active accepted cohort, immutable request body, deterministic request ID,
  and write-once compatibility association before queue visibility.
  `NOT_REPRESENTABLE` coverage remains same-cell legacy SDK work and cannot
  create a private request. Do not broaden the legacy SDK request binder.
  Within that transaction the represented child is inserted directly as
  committed `REQUEST_BOUND` with the deterministic request ID and bind
  timestamp; no private `PRE_SUBMIT` state exists. Claim SQL joins the exact parent/child, route sequence, request-ID
  equality, private correlation, and active reference; an unbound child is
  never claimable.
- Give the private payload a legal public Pydantic field with serialization
  alias `_skypilot_resource_action_authority_v1`, forbid extras, and serialize
  durable request JSON with aliases while handler kwargs use the public field
  name. The PostgreSQL claim predicate must observe exactly the underscore
  alias.
- Before claimant startup, install the complete Serve039 boundary: lease owner /
  hash/normalized-process columns, process supersessions, execution lineage,
  action selectors, one-to-one shadow execution histories, and shadow terminal
  histories; exact PostgreSQL target /
  old-stamp behavior; and the same-engine three-method terminal-store composition
  in every API/Uvicorn/controller/authority-supervisor and spawned-child root.
  Private claim and terminal predicates accept only generation zero or one,
  cancellation intent closes only through quiesced owner acknowledgement, and
  multi-request terminal work uses only homogeneous stale-owner, process-
  supersession, or cold-recovery batches with the exact 16/16/32 bounds and
  operation/fence partition contract.
- Make authority API registration INSERT-plus-exact-adoption with a per-boot
  nonce and database-owned start time; implement the closed bootstrap -> bound ->
  ready -> rewarming -> ready and owner-bound -> draining health transitions,
  with heartbeat-only CAS; and give Serve
  the sole typed GC for historical `authority-worker` rows. Bind every new post-
  039 lease/owner in the same commit as bootstrap -> bound, retain bound API
  identity in lost-ACK adoption, and revalidate row existence against concurrent
  GC before BIND or supersession can commit.
- Run one fixed eagerly warmed no-burst LONG process pool per authority
  supervisor, with distinct child PIDs and the manifest-frozen connection budget.
  Implement current-process-only normal RENEW and survivor acknowledgement,
  same-container rejection, `SUPERSEDE_EXECUTION_OWNER` from an exact Kubernetes
  container-incarnation proof, the process-fenced common terminal batch, and
  stable-Pod/process/API/request equality at claim-start, every progress and pre-
  I/O CAS, each effect, and return. No bootstrap or prior process can renew,
  acknowledge, warm to ready, claim, or attest for the current owner.
- Make bootstrap a permanent runtime phase: bind `/livez`, `/bootstrapz`, and
  the authenticated preflight transport; self-attest two distinct V2 worker
  registrations; persist the sole final Deployment snapshot; promote the
  cohort to `ACCEPTING`; complete static manifest/transport/principal/claim/
  RBAC readiness; then expose `/readyz`, resolve cohort-bound claim
  configuration, and start/advertise the existing request claimant.
  `/bootstrapz`, not `/readyz`, is the Kubernetes readiness probe. The
  preflight transport returns unavailable until the serving worker is a fresh
  member of the accepted V2 set. Target- and kind-specific
  preflight runs only after one manager creates its exact `PREPARING`
  reference; it cannot be a startup gate and is bound into admission and the
  one-request dispatch proof. Startup alone creates no
  action/reference/request/queue row and performs no provider effect.
- Add journal-before-I/O mutation boundary to action-correlated requests.
- Register strict request-result codecs for
  `serve_shadow_candidate_launch` and `serve_shadow_candidate_down`, matching
  the fail-closed authoritative-handler rule: null, default-encoded,
  wrong-kind, extra-key, or hash-invalid success payloads terminalize failed.
  Ordinary request codecs remain unchanged.
- Implement the specified one-to-one shadow execution-history store and closed
  `ProviderShadowLifecycleProgressV1` projection for partial object UIDs/specs,
  exact handle, runtime, job intent/ID, endpoint, operation IDs, strict return,
  fallback, and typed outcome under the represented child/private-request claim
  fences. It reuses only the explicitly closed pure substitutions and never
  fabricates an API006 action/attempt row. P3 cannot complete until that canonical
  store and its same-inventory linked-admission/claim/pre-I/O representability
  gates are implemented and verified.
- Carry the exact provider cursor from attempt `n` to `n+1`, clear/recompute
  only the attempt-scoped attestation envelope, and reject any crossed-boundary
  gap or regression.
- Invoke the in-server execution/core seam directly; the handler must not call
  `sdk.launch()` or `sdk.down()` and create a nested API request. It consumes
  the active execution claim's request ID and generation as the only request
  identity; SDK entrypoints are hard-failed in integration tests.
- Drive exact CoreV1 operations through one `KubernetesResourceActionSession`;
  bypass policy reapplication, generic bootstrap/config discovery, cached
  clients, and mutation helpers that rewrite bytes.
- Implement per-effect worker-chain revalidation, the purpose-authenticated TLS
  preflight endpoint, dark Helm versioned authority-worker Deployments/Service/
  RBAC/NetworkPolicy resources, active/frozen cohort claim join, and the closed
  handler claim filter.
- Implement the PostgreSQL `OPEN -> READY -> COMPLETED` worker-registration
  handoff and its `ABANDONED` branch exactly as specified above. Replacement
  candidates remain bootstrap-only and claimless; the old UID is fenced only
  after exact absence, the survivor must re-attest after that fence, and the
  final set-level Deployment snapshot and membership replacement commit
  atomically. Losing both accepted members uses the same-cohort full-set cold-
  recovery transaction so frozen work remains claimable.
- For authoritative actions, consume the parent design's one-request
  `PrivateDispatchReadinessProofV2` at claim-start and insert/exact-adopt its
  Serve039 lineage before handler invocation; every first/later progress
  watermark validates that immutable key. For private shadow, consume the
  disjoint dispatch membership/authority proof at claim-start and exact-adopt
  `BOUND -> AUTHORIZED` in the same-key execution history before handler
  invocation; every progress/pre-I/O CAS validates that immutable lineage. A
  shadow-activation proof or cached readiness result is never reusable dispatch
  authority.
- Implement Skylet submit/readback idempotency by action UUID plus its fsynced
  job/start-outbox and launcher run-token state machine; remove every SSH/
  generic-execute fallback from the candidate.
- Implement superseded partial-launch handoff to one normal down action with no
  hidden launch cleanup or alternate scheduler.
- Keep authoritative dispatch limited to synthetic/canary actions.

### P4: selected Serve authority

- Enable one service only while its locked service version, every live replica,
  and the promotion proof all satisfy
  `ordinary_ondemand_physical_width1_v1`, and only after the parent design's
  server-minted proof establishes at least 86,400 seconds, 100 clean represented
  launch graphs, 100 clean represented down graphs, zero divergence/blockers,
  and the complete crash/HA inventory. No caller or service setting may lower
  these floors.
- Require every crash boundary to have a durable pre-injection `STARTED`
  intent, no unresolved or tainting run, and at least one exact `PASS`; a later
  pass never erases `FAIL`/`ABANDONED` in the candidate epoch.
- Require the exact provider block in endpoint lookup, same-UID Pod IP refresh,
  and both warm-standby load-balancer slots' connectivity smoke.
- Preserve per-service fallback only for services that never promoted. Reject a
  shadow/authoritative service update that enables spot placement, on-demand
  fallback, paid or reserved capacity, cost-rebalance, accelerators, multi-node
  resources, or logical replica semantics; never demote or route an
  authoritative action to legacy.
- Keep the four named eligible-path transition seams throughout M4. Their
  already-authored stacked M5a child, not P4, deletes duplicate provider retry/
  observation ownership only after the exact-M4 post-authority gate passes.
- Before promotion, install the parent design's admission-closed authority-
  policy rotation machinery. The initial policy has one exact M4 deployment set
  named as both elected and rollback; an exact merged M5a set enters a two-set
  successor policy only after claim-disabled role/cohort attestation, the full
  16-selection mixed-compatibility suite, and an empty nonterminal inventory.

## Tests

P2a cannot be marked complete until one focused unit/PostgreSQL/Helm/live suite
proves all of the following together:

- liveness uses only `/livez`; both startup and readiness use
  `/bootstrapz`; `/readyz` stays false; the PostgreSQL server-instance row
  stays `ready=false`/phase `preflight-only`; monkeypatched
  `executor.start()`, claim-config construction, queue-claim SQL, request-store
  writes, provider client construction, and Kubernetes mutation hard-fail;
- two simultaneous one-entry inserts linearize as insert/conflict/exact-read /
  own-entry merge/CAS, one exact Deployment snapshot reaches `ACCEPTING`, and
  lost insert, append, promotion, and renewal acknowledgements adopt only exact
  stored bytes. Crash before/after each boundary, freshness expiry, abort /
  append/promotion/reference races, exact tombstone removal, and >1 watchdog
  interval cover own-entry-only renewal, peer-byte preservation, CAS loss,
  frozen Deployment resourceVersion, stale peer, replacement UID, clock
  failure, and local lease clearing. A >5-minute DRAINING fixture proves
  same-Pod `DRAINING -> DRAINING` renewal keeps frozen work dispatch-eligible
  without permitting a new reference;
- exact probes and the entire HTTP/TLS/auth/status/header/concurrency/rotation
  matrix above pass. A real controller Pod makes the typed initial
  not-representable call using its mounted purpose CA/ring, while wrong ring /
  CA/SAN/SNI, duplicate/framing/header/body/canonical errors, slow body,
  saturation, partial Secret generations, and old/new overlap take only their
  closed branches;
- PostgreSQL asserts zero request, queue, action, attempt, reference, coverage,
  and private-evidence rows; Kubernetes audit asserts only own-Pod, owner-RS,
  exact Deployment, and ServiceAccount GETs, with zero list/watch or mutation.
  RBAC authorization tests deny every other verb/resource and separately cover
  the exact conditional API tombstone GET grant;
- qualification uses the real registry manifest/config digests and both
  containerd/cri-o config-ID plus live EKS bare OCI-reference/manifest-ID
  branches. Missing/tampered/substituted/extra/symlink/path-escape artifacts,
  wrong `subPath` bytes, Pod/RS/Deployment projector drift, unexpected
  API-default/runtime fields, missing/extra release-specific Secret/ConfigMap /
  env/volume/scheduling input, manifest-placeholder misuse or self-hash
  mismatch, handler-registry drift, and every hash/size/canonical mismatch
  block bootstrap; and
- Helm schema/render/install/upgrade tests cover immutable installation ID,
  cross-release/namespace/installation/full-key collision handling, every
  required value/resource/name, worker/tombstone least privilege, and an
  active-selection-only render diff that changes only selector/manager
  resources while every cohort Deployment, ServiceAccount, ConfigMap,
  RoleBinding subject, and Pod template remains byte-identical.

Contract tests must cover:

- pre-release flattened resolved-target and invocation v1 bytes are rejected,
  the deployable resolved target contains exactly the three canonical object
  roles, the first-deployment preflight passes only with legacy service modes
  and absent/empty operational tables, and the cutover changes hashes/goldens
  without changing deterministic action UUIDs;
- the same-ID renderer cutover preflight passes only with zero persisted
  represented launch specs/actions/attempts/links, fails closed on every
  nonzero, missing, or indeterminate query result, and the resulting build
  accepts the exact three-body fixtures while rejecting every old minimal/fake
  `admissionDefaults` body; no tested build or parser accepts both shapes under
  `kubernetes_admitted_object_v1`;
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
- missing, unreadable, hash-drifted, or behavior-drifted candidate
  `outer_template`, `node_fragment`, `binding_schema`,
  `config_access_inventory`, or `admitted_object_normalization` artifacts
  return exactly
  `unrepresented_execution_config`, preserve shadow-only routing, and emit no
  action-owned provider bytes; realistic and candidate-maximal artifact goldens
  are required before that gate can open;
- config-inventory artifact tests parse the typed/raw canonical wrapper rather
  than `CanonicalJsonObject`, accept `api_group=""` only for the exact CoreV1
  object-session entries, and reject that empty literal at every other field;
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
- the shipped V1 identity/registration/set goldens retain their exact per-Pod
  Deployment resourceVersion bytes and remain readable for retirement/history,
  but V1 registration/set activation, `/readyz`, preflight authority, and claim
  authority reject. The V1 identity remains valid only as execution-local
  preflight/attempt provenance whose stable projection and derived instance ID
  exactly match current V2 membership; its Deployment resourceVersion alone
  never grants authority;
  V2 registration sets require ascending unique worker Pod UIDs and distinct
  instance IDs and reject both a permuted list and either duplicate. Their sole
  final Deployment snapshot is set-level, advances independently from per-Pod
  attestations, and rejects a crossed UID/generation/template, unequal
  generation/observed-generation, expired lease, or non-2/2 status without
  requiring or permitting per-Pod Deployment resourceVersions;
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
  action attempt binds its generation-one attestation to the carried cursor
  without replaying a committed effect;
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
- the exact-M4 cleanup-only/API008-Serve037-state028 pre-migration gate,
  which exact-reflects and preserves the frozen Serve034 release-ledger
  subcatalog plus the unrelated Serve035/036/037 additions, including the
  exact placement-normalization/retirement tables, columns, PostgreSQL checks,
  and foreign keys,
  implements accepted-V1 `ACCEPTING -> DRAINING -> REMOVAL_AUTHORIZED`,
  deselects every V1 cohort, uses current-chart tombstone removal followed by
  surviving-API NotFound verification to retire it, and proves zero work /
  nonterminal V1 state; any carrier blocks;
  old V1 append, renew, registration, `REGISTERING -> ACCEPTING`, and
  `DRAINING -> ACCEPTING` writes racing retirement either land
  before its locked CAS or affect zero rows, and the Serve038 V2-only
  nonterminal CHECK rejects missing/null/string/`2.0` and every stale V1 write;
  terminal-shape tests reject version `3`, missing/null/string, and numeric
  `1.0`/`2.0` outside the exact V1/V2 branches;
- Helm render/install/upgrade tests for dark-by-default immutable versioned
  two-replica authority cohorts, active-cohort preflight selection, frozen-
  cohort claim joins, `REGISTERING` plus two distinct V2 ready adopters and the
  required null snapshot before the final set-level snapshot and `ACCEPTING`,
  `/bootstrapz` -> `ACCEPTING` ->
  `/readyz`, `maxSurge=0`/`maxUnavailable=1`, and replacement preserving the
  fresh survivor while advancing only the set-level Deployment snapshot,
  digest pins,
  namespace-local Secrets/static-manifest mounts,
  release-namespace worker/Service selectors, two frozen LB Deployment
  selectors with distinct explicit ServiceAccounts and exact GET-only evidence,
  separate canary workload namespace, ClusterIP Service, NetworkPolicy, exact
  namespaced/cluster RBAC
  grants and forbidden verbs, plus the dark API -> ordinary executor ->
  controller rollout/current-chart compatible-image rollback, and separately
  gated versioned authority-cohort add/switch/drain/retirement tests while both
  cohorts remain claimable;
- handoff tests cover both exact stale-UID absence variants and reject a
  deletion timestamp, terminating same-UID Pod, expired lease, and name-only
  proof; every SQL lock/claim race; candidate bootstrap without preflight,
  `/readyz`, or claims; stale-claim generation fencing; post-fence survivor
  self-attestation and rejection of pre-fence, candidate-forged, stale, or lost
  acknowledgement; final snapshot drift; atomic stale-to-candidate membership
  replacement; exact lost-ack adoption at every CAS; candidate loss before and
  after `READY`; `ABANDONED` only with exact candidate absence and zero claim,
  attempt, progress, operation, and effect evidence; refusal to abandon after
  any such evidence; chained-candidate loss; and double accepted-member loss
  atomically cold-recovering two new members in the same cohort with both old
  claims fenced, plus exact lost-ack adoption and rejection of any changed
  Deployment UID/template;
- API006 -> API007 migration preserves every existing request, queue, action,
  attempt, and server-instance row while widening only the named role CHECK;
  downgrade rejects any remaining `authority-worker` instance; ordinary API007
  roles remain operational before Serve033 and exclude all four private names;
  authority startup against Serve032 or an incomplete private-handler inventory
  fails before a queue claim. API007 -> API008 migration tests separately prove
  the exact execution-quiescence columns, writer capability defaults, retained
  requests/claims, and fail-closed rejection of `unknown`/false API-instance
  attestations;
- Serve039 migration/constraint tests cover exact-038 writer/DDL contention,
  partial and old-stamp catalogs without an API-lineage migration dependency,
  literal merged old-stamp relation order and both class-4-process/class-10-
  shadow contention directions,
  fresh anchor/handoff/cold INSERT plus API bootstrap-to-bound atomicity, the
  retained pre-039 owner-null BIND exception, and every null/crossed/malformed
  owner JSON/hash/scalar, stable/process ID, start-time, and process-row proof;
- Serve038 migration tests start from the exact Serve037 placement-
  normalization/retirement catalog with both empty and valid nonempty ledgers,
  preserve every table/column/check/foreign key/row byte-for-byte, and race the
  Serve037 normalizer's `services -> version_specs -> replicas ->
  ephemeral_storage_cleanup_intents` table-lock program against the M4
  `services -> version_specs -> replicas` DDL prefix in both directions without
  a reverse acquisition or deadlock;
- authority API-instance tests cover insert lost-ACK and database-owned-start
  adoption, equal retry, forced UUID collision by unequal boot nonce or immutable
  inventory, deletion/no recreation, every legal and illegal health edge,
  heartbeat-only CAS, wrong owner hash, and bound-to-ready versus supersession.
  `ready -> rewarming` races claim-start and immediate pre-I/O in both lock
  orders; its generation increments exactly once, lost-ACK exact-adopts, and a
  stale generation cannot ABA-adopt. Initial warm failure, repeated rewarm
  failure, no draining recovery, and an unmarked ordinary-looking active owner
  that blocks rewarming are mandatory cases.
  Current-owner RENEW and survivor acknowledgement pass; bootstrap, prior,
  stale, wrong-phase, noncanonical process UUID, or crossed stable/process/API /
  request attestations reject at claim-start, progress, pre-I/O, effect, and
  return. Identity-proof cases include future time, the exact 300-second boundary
  and one tick beyond it, lock-wait expiry, and backward database clocks;
- process/runtime tests prove one eagerly warmed no-burst LONG pool with distinct
  child PIDs and frozen connection budgets; simultaneous supervisors and same-
  container replacement reject; a larger restart count/new container may
  supersede an expired-but-ACTIVE source; and lost-ACK, historical process-ID
  reuse, late-prior writes, and handoff/cold races preserve one current owner.
  An unmarked ordinary-looking active owner separately blocks supersession,
  handoff, cold recovery, claim-cap admission, and terminalization. Exact
  manifest widths `N=1` and `N=16` prove the physical `3 + 2*N` PostgreSQL
  high-water; zero/17, environment or manifest-hash drift, a hidden/duplicate
  engine namespace, and a wrong QueuePool size or overflow reject readiness.
  Mixed action/shadow/pending-cancellation inventories at 0/16/17 exercise the
  homogeneous process batch, both action selector branches, exact shadow history,
  one terminal `updated_at`/finish/ack/receipt time, and no missing-lineage insert;
- terminal-store composition tests cover every API, Uvicorn, controller,
  authority-supervisor, and spawned-child root; unequal database registration;
  generation-zero/one-only claim and cancellation predicates; mixed-mode/time /
  operation/fence batch rejection; exact whole-operation adoption at every
  commit boundary; no partially terminal sibling; and handler-versus-owner-ack /
  UID/process-fence winners in both lock orders, including losing-handler retry
  after an unknown winner commit and after request plus Serve-evidence GC.
  Receipt-only shadow adoption compares the typed permanent commitment without
  requiring current service/API/event state. Mixed action/shadow fence tests
  derive the time-free commitment projection from receipt-free claims, prove
  completed receipts/events/times/TTLs are outside its hash domain, and reject
  every recursive/crossed projection. OWNER_ACK_CANCEL,
  OWNER_QUIESCED_LEASE_LOSS, STALE_OWNER_FENCE, COLD_RECOVERY_FENCE, and
  PROCESS_SUPERSESSION_FENCE each cross `BOUND` and `AUTHORIZED` shadow history
  and derive the exact pre-claim versus execution receipt without a caller cause.
  Owner-quiesced lease-loss cases cross action /
  shadow, lineage/pre-claim-start, and null/pending intent, prove exact CANCELLED
  receipts/timestamps, and cover `P0`/`O`/`S`/`X` plus attempt-`n+1` without same-
  request replay. Serve-owned API-row GC
  separately tests fresh versus stale rootless bootstrap, ACTIVE/REVOKED lease,
  any active request including malformed state, lineage, selector, shadow, and
  both process-ID roots; exact cursor initialization, finite high-water epochs,
  128/129 leading rooted or raced-blocked rows, restart/wrap/lost-ACK, inserts
  before and after the cursor, a locked row, and a missing cursor target;
  generic-GC exclusion; heartbeat/delete and BIND or
  SUPERSEDE/delete in both lock orders with existence revalidation; and no row
  recreation;
- atomic route-specific binding—legacy shadow or authoritative admission may
  bind `SHADOW_ACTIVE|ACTION_ACTIVE`, while selected-private capacity admission
  leaves `PREPARING` and its one linked transaction performs the sole
  `SHADOW_ACTIVE` CAS—plus permanent fallback and exact adoption. Fallback
  cases cover new commit, every pre/post-commit cut, immediate graph adoption
  with progress-receipt absence and idempotent signaling, atomic progress-
  receipt insertion with the first legacy PRE_SUBMIT or terminal no-call
  release, retained-advanced and typed-GC receipt-only adoption after
  membership/API advancement, explicit rejection of caller-selected receipt-
  only at the unsignaled post-state, a different operation/source/failure lost
  race, hash/evidence crossing, missing/crossed progress receipt, and every
  partial descendant;
  active-cohort switches between preflight/admission; retirement between zero-
  reference discovery/admission; stale preparation owners; a nonterminal
  private shadow request; missing/unreadable/malformed references; rollback
  from `DRAINING` with the exact Deployment; and removal only after
  `REMOVAL_AUTHORIZED`, with the surviving API verifier retaining only the
  derived tombstone GETs until exact NotFound commits `RETIRED`;
- mixed action/legacy and launch/down width-one admission at the shared weighted
  cap, per-service down-cap enforcement, commit-before-signal recovery,
  release/admission and owner-handoff races, and exact loser/no-artifact and
  no-double-release assertions from the parent;
- profile and update/admission races prove that missing, non-Boolean, or true
  `ReplicaInfo.is_spot`, nonnull `spot_placer` (including
  a current scalar width of one and explicit `cost_rebalance=false`),
  `reserved_capacity_fill`, spot/on-demand fallback, accelerator or multi-node
  resources, logical replica mode, `planned_capacity != 1`,
  `reserved_fill=true`, `is_zero_cost=true`, an unknown-capacity replacement,
  or nonnull paid-pool/cost-rebalance replica attribution creates no
  preparation, coverage, request, or action artifact. Shadow and authoritative
  updates reject before spec commit, and action paths perform no DML against
  paid-capacity or reserved-fill tables; after all SQL locks release, the
  excluded adapter still performs its existing paid/reserved DML under a
  concurrent capacity-lock fixture;
- crash qualification commits `STARTED` before any fault, retains incomplete
  and tainting evidence, and cannot hide `FAIL`/`ABANDONED` with a later pass;
  the pre-M5a gate requires a fixed exact-M4 86,400-second window with at least
  100 clean launch and 100 clean down graphs and exact-zero eligible legacy
  route, unresolved crash intent, stale claim, duplicate effect, divergence,
  or blocker;
  claim-disabled exact-M5a staging, the complete 16-selection mixed suite, and
  the admission-closed policy rotation preserve all-M4 rollback and all-M5a re-
  upgrade without legacy routing. Policy physical/parser negatives reject zero
  or three deployment sets; omitted, duplicate, or outside-set selections; and
  elected/rollback hashes crossed between sets. Only the all-elected selection
  accrues soak qualification; mixed and all-rollback intervals do not;
  a distinct post-re-upgrade exact-M5a/Serve039 window repeats those duration/
  count/exact-zero gates and the complete crash/HA matrix before rollback
  closure. Serve040 tests then require permanent CLOSED closure policies and
  global zero shadow/bound work, acquire every owner/service/policy lock before
  the parent DDL lock, reject direct/offline/missing/wrong callback execution,
  prove the revision leaves a byte-exact pre/post-catalog handoff and every lock
  live, prove Alembic advances its version row before the same-connection /
  same-transaction callback reads actual 040 and builds each predecessor-bound
  proof, supersede-before-insert to preserve one ACTIVE policy, and
  accept only an atomic complete default-free catalog/stamp/successor set.
  Callback or post-run unconsumed-handoff failure rolls back DDL, version row,
  and policy writes together.
  Wrong reason/head, premature target, partial set, M4 artifact, or revision-one
  OPEN successor rejects; fresh 040 process/cohort attestations precede reopen,
  and a second final exact-M5a/Serve040 86,400-second/100+100 crash/HA window
  repeats every exact-zero gate;
- legacy-controller shadow records every provider candidate inside the exact
  capacity cohort around its one legacy high-level mutation; private represented
  shadow instead materializes one private request whose handler is the sole
  effect owner, captures the closed effect-body union with exactly three creates
  plus one typed job submission (or three UID-preconditioned deletes), never
  also invokes the legacy mutation, and rejects arbitrary job JSON or a
  missing/extra/mismatched effect; and
- absence of any new provider-work queue, action-execution lease, or domain
  retry scheduler. The registration-liveness lease above grants no execution
  authority and cannot become a second work scheduler.

The isolated HA smoke test kills API/controller/authority-worker pods at every
mutation/progress boundary and asserts one logical action, one request per
attempt, no duplicate object/job, exact partial-state adoption, both LB-slot
paths, and no false teardown completion.

## Deployment and rollback

Provider changes ship dark, then shadow, then per-service authoritative. The
blocking M4 migration job must first converge all three independent additive
heads—global-user-state 028, Serve039 after Serve038 first validates exact
Serve037, preserves the Serve033/034 action foundation plus the unrelated
Serve035/036/037 additions, including the placement-normalization/retirement
catalog and any valid nonempty normalization/retirement evidence without
rewriting it, and installs membership/policy state, and API005 for
legacy-only shadow; exact API008 (including the API006 progress substrate and
API007 role/claim foundation) is required before any private-handler shadow,
provider dispatch, or authority. API008 activation
uses the parent design's distinct server-owned proof, exact-reads the three
actual Alembic heads and accepted cohort under the transition transaction, and
requires the named PostgreSQL request storage/queue identifiers and
`execution_quiescence_capable=true` on every counted API-instance row, and
never trusts caller-supplied revision strings alone. There is
no cross-lineage Alembic dependency. No provider profile is enabled globally by
schema migration. The same merged M5a image later supports exact Serve039 and
Serve040, but 040 is reachable only through the parent design's server-gated
rollback-closure/head-advance protocol. Application rollback retains all three heads and
uses only a compatible image that preserves nonnull cluster-record UUIDs as
write-once commitments and preserves nonterminal shadow/action state. It does
not run provider compensation or schema down. After first authority, rollback
to a pre-action-aware image is unsupported.

An older additive-schema-tolerant image may be exercised only in the explicit
pre-owner dark rollback: a locked inventory must prove zero nonnull lease owner/
hash/normalized-process-scalar triples; zero process-supersession, lineage,
action-selector, shadow-terminal-history, shadow-admission-fallback-history,
shadow-admission-fallback-progress-history, and shadow-settlement-history rows;
zero request/delivery rows in
every state for all four private handlers; and no V2 candidate, dispatch,
action, activation, or admission evidence. The first post-039 lease INSERT/BIND
that stores an owner triple closes this window even with zero actions.
Thereafter, while the physical head remains 039, rollback is restricted to a
policy-approved Serve039/process-aware M4 or M5a image that preserves owner triples and process/terminal history, implements the
same cancellation, same-Pod supersession, handoff, and cold-recovery batches,
never derives/remints a process UUID from the stable Pod UID, closes admissions,
and drains/resumes the exact durable inventory without demotion or state loss.
A pre-039 image fails the startup/chart gate. After the 040 stamp, M4 is
durably absent from the one-set policy and only the exact 039/040-aware M5a
artifact or a qualified forward fix is eligible; no image may downgrade the
head or re-add the default.

The post-rotation rollback matrix binds an owner under the two-set successor,
walks all 16 approved M4/M5a role/cohort selections, rolls to the all-M4
rollback selection, same-Pod restarts mixed action/shadow/pending-cancel
inventories at 0/16/blocked-17, performs handoff and full cold recovery, then
re-upgrades to all-M5a at exact Serve039. Owner/process/lineage/selector/shadow bytes remain
stable, each request terminalizes once, and late old processes reject
throughout. Before that first rotation the one-set M4 policy permits no older
post-owner binary; an incident freezes admission and uses exact M4 or a forward
fix.

The normal M5a merge remains blocked until the canary's exact merged M4 digest
has held authority for at least 86,400 seconds with at least 100 clean
represented launch graphs and 100 clean represented down graphs, the complete
crash/HA matrix, and zero eligible legacy route, unresolved crash intent,
stale claim, duplicate provider effect, divergence, or blocker. This gate does
not rotate policy or weaken ownership; it only authorizes merging the already-
stacked cleanup source.

After authority, a future exact image is not silently added to that compatible
set. The parent M4 Serve038-backed policy-epoch protocol at live Serve039 first
attests the exact merged
artifact in claim-disabled role Pods and a new immutable cohort, moves the
canary policy `OPEN -> DRAINING -> CLOSED`, proves zero bound work, and
atomically activates a successor `OPEN` policy whose rollback set is exact M4,
whose elected set is exact M5a, and whose compatibility inventory is the full
16-selection Cartesian product. The staged M5a rollout, exact-M4 application
rollback, and exact-M5a re-upgrade never demote the service or invoke legacy
provider mutation. A fresh
post-re-upgrade exact-M5a/Serve039 window then runs for at least 86,400 seconds and at
least 100 clean represented launch graphs plus 100 clean represented down
graphs, with the full crash/HA matrix and zero eligible legacy route,
unresolved crash intent, stale claim, duplicate provider effect, divergence,
or blocker.
Only then does #1240 rotate every authoritative service to the permanently
closed one-set `ROLLBACK_EVIDENCE_CLOSURE` policy, require zero shadow services
and zero bound work globally, and authorize the distinct Serve040 transaction.
That transaction locks every owner/service and closure-policy row before
shadow-parent class 9, exact-reflects the default-bearing 039 catalog, drops
only the `execution_route='LEGACY_CONTROLLER'` server default, post-reflects,
and leaves the closed pre/post-catalog/predecessor handoff in the Alembic
context. Alembic then advances the version row to 040 and its registered
same-connection/same-transaction callback re-reads actual 040, builds each
predecessor-bound catalog proof, supersedes the closure row, and inserts the
closed head-advance successor before the outer commit. It commits the complete
catalog/version/policy set atomically; callback or unconsumed-handoff failure
rolls all three back, no physical-040/head-039 state is committed, and
acknowledgement-loss adopts only the complete 040 set. The same M5a image
restarts/re-attests every role/
cohort at actual head 040 and only then reopens. Retained 039 histories remain
readable but grant no new authority.
A second final exact-M5a/Serve040 window independently runs for at least 86,400
seconds, 100 clean launch graphs, 100 clean down graphs, the complete crash/HA
matrix, and zero eligible legacy route, unresolved crash intent, stale claim,
duplicate effect, divergence, or blocker.
No M4 rollback, schema downgrade, service demotion, or ownership
reversion exists after closure. Any later image change reuses
`COMPATIBLE_IMAGE_ROTATION` with byte-equal predecessor/actual/successor head
040 and a newly attested finite compatibility set; it never repeats the 039
closure/head-advance reasons.

The renderer-body change is also a same-v1 atomic cutover. Keep representation
and every provider route disabled while all potential readers/writers move to
the one image that simultaneously removes the minimal/fake
`admissionDefaults` acceptance and adds the exact three-body contract. With the
old image still pinned as the sole possible writer, run and retain the
consistent read-only zero-persisted-represented-launch preflight defined above;
only after every relevant role runs the new digest may representation resume.
The existing dark runtime and empty `boltz-test` graph are evidence that this
gate can pass, not permission to skip it. Any old row or mixed writer aborts the
cutover; v1 never has a dual-shape window. After a new-shape represented capsule
is persisted, rollback is limited to an image that reads only that same exact
shape and cannot restore the old fixture contract.

The dark binary/schema rollout and compatible-image rollback now have live
`boltz-test` evidence. `resourceActions.authorityWorker.enabled` nevertheless
remains `false`: no authority-worker resource, admitted provider session,
dispatcher, provider mutation, shadow sample, or M4 service was exercised.

For a dark action-aware image, upgrade API first, ordinary executors second,
and controllers last, explicitly pinning every untouched role at each
`helm upgrade --reuse-values`. Authority-worker cohorts are absent from this
sequence. Their future deployment is separately gated by renderer/runtime/
preflight and cohort-attestation evidence. The first additive migration stage
omits `--atomic`; repair or rollback uses the current chart and compatible
prior digests against retained additive heads, never native `helm rollback`.

The first P2a image is necessarily built and deployed with the authority role
disabled. Registry inspection of that immutable image supplies its distinct
OCI manifest and config digests; a follow-up reviewed change checks in the
qualification artifact and complete cohort values for that already-built
image. That follow-up chart packages and projects the exact qualification bytes
as a distinct immutable file; the prior runtime verifies the projected bytes
against `qualificationArtifact` instead of trying to open a repository file
that its image cannot contain. Only then may the preflight-only cohort be
rendered. Even then it starts no queue executor and cannot produce complete
preflight evidence or provider I/O.

### `boltz-test` dark rollout evidence (2026-08-02)

The new artifact used immutable tag `resource-actions-a836825ef`, source commit
`a836825ef9c219563bb2abc740707c825c26edc5`, and digest
`sha256:c5f1306f91c7fe2db151c34131ca4cd39be9beba3d21d170f5757996338f375e`
(`new`). The compatible baseline digest was
`sha256:d05257c3018c570861104c6c0a509c92d29af93df2d167a58e50d6748a1590a1`
(`old`). Deployment evidence remains scoped to that source commit if the PR is
subsequently rebased.

The later contract artifact used immutable tag `resource-actions-4f024b60f`,
exact merge commit `4f024b60f2fc71852fa8fb9747390f4d3917b03f`, and digest
`sha256:06c9e71c5744ea970c41402fb9c4934e6722a7b53271f6715231b4b275525d25`
(`renderer-contract`). In the rows below, `prior` means the compatible digest
running that ordinary role immediately before its staged replacement.

The pure-renderer artifact used immutable tag `resource-actions-0e894c2a5`,
exact merge commit `0e894c2a5d7186d15b10d62bbfdb8283201e4e63`, and digest
`sha256:b21f0e7cc39f62a21bc5887406f941d0b298d8fc277f0b5abb8b1f170c88b198`
(`renderer`).

| Revision | Purpose | API / executor / controller | Migration result | Heads after checkpoint |
|---|---|---|---|---|
| 57 | Observed baseline | old / old / old | prior rollout | 027 / Serve032 / API004 |
| 58 | Dark API/migration stage | new / old / old | succeeded, 10:05:51–10:06:56 UTC | 028 / Serve033 / API007 |
| 59 | Dark ordinary-executor stage | new / new / old | succeeded, 10:13:33–10:13:45 UTC | 028 / Serve033 / API007 |
| 60 | Dark controller stage | new / new / new | succeeded, 10:21:02–10:21:14 UTC | 028 / Serve033 / API007 |
| 61 | Current-chart compatible-image rollback | old / old / old | old migrator succeeded, 10:26:51–10:27:02 UTC | retained 028 / Serve033 / API007 |
| 62 | Re-upgrade API stage | new / old / old | succeeded, 10:36:15–10:36:26 UTC | retained 028 / Serve033 / API007 |
| 63 | Re-upgrade ordinary-executor stage | new / new / old | succeeded, 10:42:48–10:43:00 UTC | retained 028 / Serve033 / API007 |
| 64 | Re-upgrade controller/final stage | new / new / new | succeeded, 10:46:32–10:47:04 UTC | retained 028 / Serve033 / API007 |
| 71 | Renderer-contract API/migration stage | renderer-contract / prior / prior | succeeded, 16:14:43–16:15:51 UTC | retained 028 / Serve033 / API007 / capacity001 |
| 72 | Renderer-contract ordinary-executor stage | renderer-contract / renderer-contract / prior | succeeded, 16:23:29–16:23:40 UTC | retained 028 / Serve033 / API007 / capacity001 |
| 73 | Renderer-contract controller/final stage | renderer-contract / renderer-contract / renderer-contract | succeeded, 16:28:09–16:28:57 UTC | retained 028 / Serve033 / API007 / capacity001 |
| 74 | Pure-renderer API/migration stage | renderer / renderer-contract / renderer-contract | succeeded, 20:13:56–20:14:33 UTC | retained 028 / Serve033 / API007 / capacity001 |
| 75 | Pure-renderer ordinary-executor stage | renderer / renderer / renderer-contract | succeeded, 20:21:03–20:21:13 UTC | retained 028 / Serve033 / API007 / capacity001 |
| 76 | Pure-renderer controller/final stage | renderer / renderer / renderer | succeeded, 20:26:10–20:26:21 UTC | retained 028 / Serve033 / API007 / capacity001 |

Every revision 57--64 checkpoint converged the changed role to 2/2 at the
intended digest with zero restarts. All eight action/shadow/coverage/cohort
tables remained empty; services, replicas, and clusters remained zero; all 18
pre-existing requests were preserved while normal processing reduced
nonterminal requests from 9 to 6; final ungranted locks were zero; and no
authority-worker resource existed. The final state had two ready API endpoints
and no matching schema, migration, private-handler, or resource-action error in
role logs.

Karpenter/capacity churn generated 134 aggregated `FailedScheduling` events,
10 `Underutilized` evictions, two transient AWS-CNI
`FailedCreatePodSandBox` events, and 167 startup/readiness `Unhealthy` events
for the selected rollout objects between 10:05 and 10:49 UTC. Every affected
ordinary Deployment recovered to 2/2 with zero restarts. With zero actions and
provider I/O disabled, this is ordinary Deployment recovery evidence, not an
action crash-canary or provider-authority result.

At revision 73, all six active API, ordinary-executor, and controller Pods
reported the exact `renderer-contract` digest, were ready, and had zero
restarts. A role-log scan from the revision-71 start found zero traceback,
exception, critical, fatal, unhandled,
or error matches. PostgreSQL retained API007, Serve033, global-user-state 028,
and capacity001; the eight action-family tables and the service, replica, and
cluster tables all had zero rows. The authority-worker value remained
explicitly false and no matching workload resource existed. Karpenter churn
occurred during the rollout, but no mixed-version Pod remained at the final
checkpoint. This checkpoint starts no shadow, provider-I/O,
crash-canary, or M4 evidence window.

At the revision-76 stable checkpoint, all three ordinary Deployments reported
2/2 ready, updated, and available replicas. All six active Pods had zero
restarts, no deletion timestamp, the exact `renderer` image ID, and embedded
commit `0e894c2a5d7186d15b10d62bbfdb8283201e4e63`. Both API replicas byte-checked
the packaged config-access inventory at its frozen size and SHA256. PostgreSQL
retained API007, Serve033, global-user-state 028, and capacity001. The eight
action-family tables, correlated API requests, services, replicas, and
clusters all had zero rows. `resourceActions.authorityWorker.enabled=false`,
and the only Deployments were the ordinary API, executor, and controller
roles. A 20-minute role-log scan found no traceback, exception, error, fatal,
crash, or failed match after excluding the API CLI's expected local-server
startup message.

Karpenter capacity provisioning and consolidation caused transient pending,
surge, and terminating Pods during revisions 74--76. Each stage retained ready
old-role capacity until replacements were ready and then converged to exactly
two Pods at the intended digest. This is ordinary rolling-update recovery
evidence only. With no action, shadow, provider session, provider I/O, or
authority worker, revision 76 does not start an M3/M4 qualification window.

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
    installationId: ""       # immutable UUID, required before enablement
    activeCohort: ""
    cohorts: []
    # Each entry is:
    # - id: p2a-v1              # version suffix, not the database cohort key
    #   replicas: 2
    #   image: registry.example/repo@sha256:<OCI-manifest-digest>
    #   imagePullPolicy: Always
    #   ociConfigDigest: sha256:<OCI-config-digest>
    #   qualificationArtifact: {repoPath: ..., byteSize: ..., sha256: ...}
    #   podTemplateContract: {repoPath: ..., byteSize: ..., sha256: ...}
    #   artifactInventory: {repoPath: ..., byteSize: ..., sha256: ...}
    #   callableInventory: {repoPath: ..., byteSize: ..., sha256: ...}
    retirementTombstones: []    # [cohort suffix]; exact names derive from it
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

Every v1 cohort entry requires a lowercase DNS-label `id` suffix of at most 42
characters, `replicas: 2`, a digest-pinned
`image`, `imagePullPolicy: Always`, the distinct qualified OCI config digest,
one chart-packaged projected qualification reference, and the three exact
installed-package references shown above. The manifest
digest parsed from `image` and the supplied config digest are not
interchangeable. Helm derives the database key as
`"ra:" + installationId + ":" + sha256(namespace + "\\n" + fullName +
"\\n" + id).hexdigest() + ":" + id`, requires the immutable installation
UUID when enabled, requires `fullName` itself to be one lowercase DNS label,
and rejects name/key overflow. Serve034 persists a release ledger keyed by the
stable `(namespace, .Release.Name)` pair with a database-wide unique
installation UUID, immutable rendered full name, exact desired live-manifest
array/hash, tombstone array/hash, and revision. Its companion binding table has
primary key `(namespace, .Release.Name, cohort suffix)`, a unique full cohort
ID, and the complete immutable canonical manifest/hash. Changing either
identity, reusing a suffix with different bytes, or reusing the installation
UUID in another release fails closed even after all chart authority values are
cleared.
When enabled, `activeCohort` names exactly one suffix entry. Helm
renders one immutable, version-named authority-worker
Deployment and one immutable version-named ServiceAccount per entry, one
ClusterIP preflight Service selecting only the active cohort, purpose-token/TLS
projections, the canonical static cohort manifest as an immutable read-only
projected file, and authority-ingress NetworkPolicy. The manifest hash is a Pod
template annotation so an immutable cohort ID cannot silently retain Pods from
another manifest. The Deployment strategy is exact `Recreate` with no
rolling-update parameters. Every initial/final/watchdog Deployment observation
rejects any other strategy before registration or lease adoption, preserving
the two-Pod no-overlap invariant without adding a mutable strategy field to the
cohort manifest. It renders no additional queue, Ingress, LoadBalancer, or
mutation service, and the runtime needs no ConfigMap GET permission.

For PostgreSQL with the required pre-existing database Secret, the migration
Job is always a blocking pre-install/pre-upgrade hook even when proposed HA and
authority are disabled. It receives a closed canonical release proposal keyed
by namespace plus `.Release.Name`. Each live entry names only its suffix and a
fixed path beneath
`/etc/skypilot/resource-action-authority/release-preflight/`. A weight `-20`
immutable hook ConfigMap reuses the exact canonical `$manifestJson` bytes later
placed in the worker ConfigMap; the weight `-10` migration Job mounts each file
read-only, descriptor-reads and canonical-validates it, then atomically updates
the ledger before Helm may mutate workload objects. A fully cleared disabled
proposal uses empty installation/live/tombstone values but resolves an existing
ledger through the stable release key. It cannot hide a live cohort. Authority
remains unsupported with the chart-managed database-secret path; credential
topology changes occur only as a separate post-retirement operation.

The runtime does not infer activation from those compatibility environment
names. The chart always reserves its exclusive
`SKYPILOT_RESOURCE_ACTION_AUTHORITY_ENABLED` marker, and only exact text `true`
activates an authority path. It is emitted to API for every enabled inventory
(including tombstone-only retirement) and to controller only when a live
cohort's preflight credentials and manifest are mounted. An absent marker is
authoritative disabled even if an operator-defined legacy installation or
token-file environment value is present.

The current hook predicate still depends on the proposed PostgreSQL backend and
pre-existing Secret values. Consequently no cohort may be enabled until a
follow-up persists a stable retained release/database anchor before the ledger
commit, with no first-enable crash gap, and makes every later ordinary upgrade
resolve that anchor and run preflight using the pinned Secret even after values
are cleared or credential topology is changed. Missing/tampered anchors or
Secrets must stop the release before object deletion. `--no-hooks`, raw object
deletion, and uninstall are explicit administrator bypasses requiring a
separate break-glass/finalizer protocol. This is an enablement blocker, not a
dark-deployment blocker for a release that has never enabled authority.

First worker registration resolves the installation UUID from its full cohort
ID and locks the release row before the cohort row. It requires the release to
remain enabled and the complete manifest bytes to match both the current live
inventory and permanent suffix binding. A race therefore either registers
under the locked predecessor inventory before preflight audits every cohort
row, or waits and observes the new inventory; it cannot register in the gap
between preflight and Helm apply. `REMOVAL_AUTHORIZED` may remain live for the
first removal upgrade or appear as a tombstone for the deletion upgrade, but
absence from both inventories rejects.

The release-input schema above is also the complete environment contract: five
literal entries, one database-Secret entry, and four ordered downward-API
entries (the existing Pod name/namespace/UID plus `POD_IP=status.podIP`), with no additional resource-field or
ambient environment injection.
The runtime mints a fresh random `SKYPILOT_API_SERVER_INSTANCE_ID` internally
for this Python/container start after validating `SKYPILOT_POD_UID`; it never
derives one from the Pod UID and it is not a fifth downward-API entry. Child
processes inherit that exact value. Manifest, qualification, and TLS paths are fixed code
constants rather than extra environment variables.
Each retirement tombstone is a previously rendered cohort ID; the chart derives
its two fixed names and rejects arbitrary name overrides. Operational removal
requires the registry row to be `REMOVAL_AUTHORIZED` with those exact names and
UIDs before the tombstone-bearing upgrade.

Whenever authority-worker support is enabled, including a tombstone-only
release with no live cohort, the API Pod receives the immutable installation
UUID plus canonical compact JSON arrays of the sorted unique live suffixes and
sorted unique tombstone suffixes in exactly
`SKYPILOT_RESOURCE_ACTION_AUTHORITY_INSTALLATION_ID`,
`SKYPILOT_RESOURCE_ACTION_AUTHORITY_COHORT_SUFFIXES_JSON`, and
`SKYPILOT_RESOURCE_ACTION_AUTHORITY_RETIREMENT_TOMBSTONES_JSON`. These names
are reserved from operator extra environments. The surviving API role runs one
PostgreSQL advisory-lock singleton scoped by installation UUID, namespace, and
rendered Helm full name. It validates every central row back to that exact
release digest and inventory. A live `REGISTERING` row may take only the stale
zero-carrier authorization path and performs no Kubernetes read; a live
`REMOVAL_AUTHORIZED` row waits for an operator tombstone. Only an inventoried
tombstone may issue exact-name GETs for its bound Deployment and
ServiceAccount. Only HTTP 404 is absence; 403, transport failure, or a returned
object with different apiVersion/kind/namespace/name/UID leaves the row
unchanged and does not prevent later records in the bounded pass from being
checked. Two exact 404 results permit the fenced `RETIRED` transition. There is
no list, watch, delete, ambient kubeconfig, or cross-release fallback.

The API Pod's ServiceAccount token automount is enabled while this verifier is
configured, and a tombstone-scoped Role grants that existing API
ServiceAccount `get` on only the derived Deployment and ServiceAccount names.
With tombstones but no live cohort, Helm renders only the API verifier
environment/RBAC portion of this feature: no authority Deployment,
ServiceAccount, manifest/qualification ConfigMap, preflight Service,
NetworkPolicy, purpose-token Secret mount, or TLS Secret mount is synthesized.
After every matching central row is `RETIRED`, a later current-chart upgrade
may remove the tombstone suffix and therefore the corresponding GET grant.

Every authority object and Pod uses
`skypilot.co/authority-release-scope=trunc63(sha256(<namespace> + "\n" +
<rendered-full-name>))`. The Deployment, selector Service, topology-spread
constraint, and NetworkPolicy target combine that label with the authority role
and, where applicable, cohort suffix. NetworkPolicy ingress additionally
requires both `skypilot.co/role=controller` and the immutable
`app=<rendered-full-name>-controller` source label. Thus two Helm releases in
one namespace may reuse a cohort suffix without either Service selecting the
other's Pods or either NetworkPolicy admitting the other's controllers. A
two-release render guard requires target scope sets and source selectors to be
exact and disjoint.

`activeCohort` is consumed only by the selector Service and manager/controller
selection projection. It is not rendered into any cohort Deployment Pod
template, environment variable, annotation, volume, projected manifest, or
hash. An active-only values change therefore changes selection resources but
leaves every old and new cohort Deployment byte-identical; a Helm render-diff
golden enforces byte identity for every cohort Deployment, ServiceAccount,
per-cohort manifest/qualification ConfigMap, RoleBinding subject, and Pod
template across the switch. A frozen worker learns only its own full cohort ID
from its immutable manifest, never the current active suffix.

A full derived cohort ID permanently binds its static manifest plus Deployment and
ServiceAccount UIDs. An upgrade never changes those fields in place. The
shipped P2a Serve033 registration flow adds a
new cohort. Both Pods first become `/bootstrapz`-Ready. The first process that
then observes one exact Deployment generation/resourceVersion at
spec/status-total/updated/ready/available replicas all two and unavailable zero
inserts its own registration in a
`REGISTERING` Serve033 identity. The peer exact-reads that row and appends its
own registration only after observing the same Deployment snapshot; the pair
is sorted by Pod UID and no peer-Pod GET is needed. Only the typed two-worker /
same-Deployment-snapshot transaction changes it to `ACCEPTING`, after which the
cohort may be selected. New launch/down
preparations first create a `PREPARING` reference under that `ACCEPTING`
identity and then freeze the same resolved cohort returned by preflight.
In P2a, Kubernetes startup and readiness probes call `/bootstrapz` and prove
only local manifest/transport health; `/readyz` stays false, preflight stays
503 until `ACCEPTING`, and the role starts no executor. Both Pods therefore
become Kubernetes Ready before the typed registration path requires the
Deployment's two ready/available replicas. P3 changes `/readyz` to claim
readiness only after the accepted identity is resolved. `ACCEPTING` plus active
selection gates creation of new `PREPARING` references, not claims. Once P3 is
implemented, the existing queue claim predicate binds each worker's own
immutable cohort and an existing `SHADOW_ACTIVE` or `ACTION_ACTIVE` reference;
it remains enabled for that cohort in either `ACCEPTING` or `DRAINING`,
independent of later active selection, so activation has no readiness cycle and
frozen old work remains recoverable.

A crash cannot wedge a never-accepted row forever. After all stored
registration/observation timestamps are more than five minutes behind a fresh
PostgreSQL clock read, the API-role abort transaction may lock and change
`REGISTERING -> REMOVAL_AUTHORIZED` only if the row has no acceptance history
and exhaustive scans find zero cohort references, private requests/evidence,
action specs/attempts, or activation/promotion proofs. It locks the cohort row;
because every append, promotion, and reference admission locks that earlier
class first, all later-class carrier scans are deliberately nonlocking and do
not violate the global lock order. A carrier declaring the same full cohort key
blocks even when its remaining identity differs. The P2a stale-registration
audit uses a recursive JSONB cohort-ID locator over every current action,
request, shadow-parent, and shadow-child carrier, so a top-level/embedded or
parent/child disagreement cannot hide the target key. It is an existence audit:
target-located terminal, released, unknown-handler, malformed, and
hash-inconsistent rows all retain the cohort. State with no recognizable target
locator does not globally retain every unrelated P2a cohort; normalized
locators and complete typed/hash/terminal-graph validation are a P3 normal
`DRAINING` retirement gate. P2a persists no separate cohort-bearing
activation/promotion-proof row, so there is no additional carrier table to
scan. A concurrent append, promotion, or reference wins and makes abort reject.
The current chart
then removes only the exact bound Deployment/ServiceAccount; the API verifier
commits `RETIRED` after exact NotFound. Operators create a new suffix/full ID;
the aborted ID is never retried. Tests cover crash before insert, after insert,
after the second append, after promotion commit but before acknowledgement, and
abort racing each append/promotion/reference boundary.

Every locally adopted `ACCEPTING` or `DRAINING` identity expires after five
minutes unless a watchdog renews it. A same-cohort, same-state
`ACCEPTING -> ACCEPTING` or `DRAINING -> DRAINING` compare-and-swap may
replace only the caller's own Pod-UID registration while preserving the peer
entry byte-for-byte. The sorted UID set, Deployment UID/generation,
Deployment resourceVersion, ServiceAccount UID, manifest/image/artifact /
template/handler/claim identities, and all other non-temporal fields remain
identical; only that caller's observation/registration timestamps and
Pod/ReplicaSet resourceVersions may advance. CAS loss rereads and reapplies only the caller's
entry; a new UID rejects and a stale peer clears acceptance at peer expiry. The
watchdog uses PostgreSQL time, rejects future timestamps and
clock failure, rereads all projected bytes and the live owner chain, and clears
its local accepted snapshot before expiry on any drift or failed renewal.
Preflight then returns 503. DRAINING renewal cannot create new references; once
P3 exists it only keeps frozen old requests eligible for per-dispatch proof.
P2a treats Pod/Deployment replacement as a new cohort; claim readiness remains
false until P3's rolling protocol is implemented.

Before applying Serve038, the exact M4 image runs a cleanup-only entrypoint at
actual API008/Serve037/state028 with its migration ceiling held at 037 and all
executors/private routes disabled. After the current chart deselects each P2a
cohort, this no-DDL bridge supplies the missing typed accepted-V1 path:
cohort/reference locking, exact zero-carrier proof,
`ACCEPTING -> DRAINING -> REMOVAL_AUTHORIZED`, the frozen Serve034 tombstone
protocol at the exact Serve037 head, and lost-CAS adoption. The current-chart tombstone upgrade removes
only the exact Deployment/ServiceAccount, so every old Pod is gone before the
surviving API verifier proves both NotFound results and commits `RETIRED`. The gate proves zero action/reference work, zero
nonterminal V1 cohort, and zero live/fresh authority-worker server instance. A
racing V1 append/renew/registration or shipped `REGISTERING -> ACCEPTING` /
`DRAINING -> ACCEPTING` CAS linearizes on the cohort lock, landing
before the next bridge CAS or affecting zero later-lifecycle rows.
Serve038 installs the V2-only nonterminal CHECK above while holding the cohort
DDL lock; a stale old-binary V1 write therefore fails physically after the
migration. Every post-038 chart rejects an authority-worker artifact without
exact-038 capability, and rollback to a pre-038 P2a writer is unsupported.

M4's Serve038 authority-state migration preserves those V1 rows as retirement-
only history and registers a fresh V2 cohort suffix. The P2a version-1 static
manifest, `frozen_action_cohort_join_v1` claim contract, Deployment-snapshot
parser, and exact `Recreate` strategy remain readable only for that Serve034
cleanup/retirement history. The fresh Serve038 cohort uses a distinct
version-2 static manifest, exact claim contract
`frozen_action_cohort_join_v2`,
`ProviderAuthorityWorkerDeploymentSnapshotV2`, and exact `RollingUpdate`
strategy with integer `maxSurge=0` and integer `maxUnavailable=1`. V1 and V2
manifest/snapshot/claim parsers do not accept one another; a crossed version or
strategy rejects before cohort selection, dispatch proof minting, `/readyz`, or
claim advertisement. Each V2 worker first
serves `/livez` and `/bootstrapz`, binds the
authenticated preflight transport, self-attests its projected manifest/live
owner chain, and becomes Kubernetes-ready only on `/bootstrapz`; action
preflight remains unavailable until accepted V2 membership. The first inserts a
`REGISTERING` Serve038 identity with its own V2 registration; the peer exact-
reads/adopts that identity and compare-and-swap appends only its own distinct V2
anchor registration. Each insertion transaction also creates or exact-adopts
that member's ACTIVE lease, and later renewal follows the global lock order. A
lost acknowledgement adopts the immutable anchor plus its same-stable-
identity lease lineage: exact insert operation ID/bytes at generation one, or
a valid ACTIVE generation/revision-equal descendant through only legal renewal,
initial owner binding, or retained process-supersession transitions thereafter.
Both `REGISTERING` shapes keep `deployment_snapshot=null`.
Neither process reads or invents its peer Pod. A final set-level Deployment read
after both attestations and the typed V2 two-worker transaction replaces the
anchors with the exact current lease renewal-registration bytes, atomically
fills that field, and changes the cohort to `ACCEPTING`; the final resourceVersion is
not required to equal any per-Pod owner-chain observation because V2 stores it
only at set level.

The bounded 2026-08-03 runtime implementation reaches this initial membership
boundary and no farther. Its static entrypoint and the actual Helm
migration-hook release preflight accept only exact numeric V1 or V2 manifest
contracts. The hook preserves the frozen Serve034 V1 path and sends only exact
V2 manifests to an additive typed release-ledger writer. It descriptor-reads
and parses every projected manifest exactly once, using the same bytes for
numeric dispatch and typed durable preflight. The retained-row decoder checks
both raw inventory types and individual/combined 256-entry bounds before any
hashing, iteration, or manifest decode. Before registration,
the V2 store locks and fully validates the release row's typed canonical
uniform-version list, hashes, sorted/unique/bounded/disjoint inventories,
immutable identities, revision/timestamps, and permanent suffix binding.

The runtime selects the additive V2 coordinator only for an exact parsed V2
type. V2 performs bounded-time four-GET self/owner-chain projection, initial
insert/adoption, peer append, own-lease renewal, fresh final set-level
Deployment projection, and the existing PostgreSQL two-member activation
transaction. A shared stop gate linearizes every mutating store call and local
acceptance publication: a read-only observer/store read may outlive the bounded
join, but it cannot write or publish after `stop()` returns. Crossing this gate
first relies on transaction-local PostgreSQL
`statement_timeout=5000ms` and `lock_timeout=3000ms` before locking; the engine
also bounds pool checkout and connection establishment at 15 seconds each.
`stop()` then gives the whole gate 30 seconds and errs toward fail-stop if pool
plus connection setup exhaust that budget. If a DBAPI/network blackhole
defeats those graceful limits, this dedicated role invokes nonreturning
`os._exit(70)`, so OS connection close rolls back uncommitted work and stop
cannot return into a possible late commit/adoption; otherwise its ten-second
tail join leaves 20 seconds of the 60-second Pod grace for the remaining role
shutdown. The production role exact-type filters V2 out of the retained V1
evaluator and passes V2 only to the isolated, zero-queue locked trust reader.
It never starts the request executor, keeps the API-instance lease unready, and
advertises no claim. Without a production manager caller for the `PREPARING`
writer, live V2 requests still receive canonical typed 503; tests with the
exact durable trust join receive only typed unavailable. Membership acceptance
and that response authorize no action, request, effect, or provider call.
Atomic preparation/admission, claim readiness, handoff/cold-recovery runtime,
and retirement/rollback orchestration remain open gates.

Lost insert/append/promotion acknowledgement is resolved by exact row/revision
read, never blind replay. Static cohort/transport/principal/claim/RBAC checks
then enable `/readyz` and the existing claimant; target- and kind-specific
preflight remains per decision. Own renewal failure or manifest/owner drift
clears that process's `/readyz` and stops its new claims, claim renewals, and
effects. Peer expiry stops new `PREPARING` references but does not strand
already-bound work: a fresh accepted survivor may claim or renew that work
while the durable handoff proceeds. Subsequent
`maxSurge=0,maxUnavailable=1` replacement uses only the durable handoff protocol
above: bootstrap-only candidate, exact stale-UID absence, stale-instance claim
fence and `OPEN`, post-fence survivor acknowledgement and `READY`, then one
atomic accepted-membership/final-snapshot commit and `COMPLETED`. The survivor
does not attest a Deployment resourceVersion; the sole fresh set-level snapshot
does. `ABANDONED` requires candidate absence plus zero-effect proof, and loss
of both accepted members uses full-set same-cohort cold recovery. Template,
image, or Deployment UID change creates a new cohort only while retaining any
old bound-work cohort unchanged; it cannot transfer or strand frozen work.
New launch/down
preparations first create a `PREPARING` reference under that `ACCEPTING`
identity and then freeze the same resolved cohort returned by preflight.
Kubernetes `/bootstrapz` readiness during `REGISTERING` proves only the
self-attestation/health contract; `/readyz` remains false. `ACCEPTING` plus
active selection gates creation of new
`PREPARING` references, not claims. The existing queue claim predicate binds
each worker's own immutable cohort, stable accepted V2 registration membership
plus its fresh exact-current execution lease/API process membership, and
an existing `SHADOW_ACTIVE` or `ACTION_ACTIVE` reference. It excludes the stale
instance in any `OPEN` or `READY` handoff and every nonmember candidate. It
remains enabled for an otherwise valid cohort in either `ACCEPTING` or
`DRAINING`, independent of later active selection, so activation has no
readiness cycle and frozen old work remains recoverable.

Moving active selection away does not remove the old cohort. Every lifecycle
edge computes `GREATEST(clock_timestamp(), locked_prior.state_changed_at,
<every affected locked lease.renewed_at>)`, omitting the lease terms only when
the edge affects no lease, writes it to `state_changed_at`, and uses the same
value for removal authorization and lease revocation. This preserves the shipped
`state_changed_at >= created_at` CHECK under wall-clock regression. The typed
retirement helper first locks cohort -> nonterminal-handoff slot -> both
accepted registration leases, rejects
`OPEN | READY` and any currently accepted member with a terminal
`STALE_HANDOFF`-revoked lease, and commits `DRAINING`, which rejects new
preparation references while existing preparation, private-shadow, and action
references remain claimable. A separate exceptional
`ACCEPTING -> REMOVAL_AUTHORIZED` edge is legal only for unresolved terminal-
stale membership when one transaction continues through references in the
global order, proves the complete locked zero-non-`RELEASED`-reference/zero-work inventory and
the same fail-closed defensive scans used for removal, terminally revokes the
survivor, and sets `removal_authorized_at`. It permits no rollback, cold
recovery, or later handoff; no unmarked retirement-only `DRAINING` state exists.
After all
active references release, one
transaction locks cohort -> nonterminal handoffs -> every registration lease ->
references in the global order, rejects `OPEN | READY`, performs fail-closed
nonlocking defensive reads over authoritative action specs/attempts/requests,
Serve parent/child evidence, private shadow requests/coverage attempts, and V2
handoffs, terminally revokes every remaining ACTIVE registration lease, and
commits `REMOVAL_AUTHORIZED` atomically using one PostgreSQL timestamp. Every
`COHORT_REMOVAL` lease has null `revocation_owner_id`, preserves its execution-
owner/hash/normalized-process-scalar triple exactly, records
`last_operation_kind=REVOKE`, and
`revoked_at == cohort.removal_authorized_at`. The write-once removal time is
preserved when `RETIRED` later advances `state_changed_at`. Renewal rechecks lifecycle under the
cohort lock and rejects from that point forward. An `OPEN` or `READY` handoff is
an active reference and must reach `COMPLETED` or valid `ABANDONED` first. Unknown,
malformed, inaccessible, cross-identity,
or ambiguously decoded state counts as a reference. Only then may the current
chart remove the Deployment and ServiceAccount. The surviving API-role verifier
then obtains and hashes exact Deployment/ServiceAccount NotFound outside SQL; a
short no-I/O cohort -> handoff -> leases -> references transaction revalidates
`REMOVAL_AUTHORIZED`, exact tombstone names, zero non-`RELEASED` references,
byte-equal retained `RELEASED` history, and both proofs
before one CAS commits `RETIRED`, setting `retired_at == state_changed_at ==
GREATEST(clock_timestamp(), removal_authorized_at)` so wall-clock regression
cannot wedge the edge. Removal moves their exact names into
`authorityWorker.retirementTombstones`; the surviving API-role retirement
verifier keeps tombstone-scoped release-namespace GET permission, performs the
NotFound checks, then locks the stable release row before the cohort and
requires the exact suffix to remain tombstoned and absent from live inventory
before committing `RETIRED`. A concurrent rollback either makes the suffix
live first and rejects the stale NotFound result, or sees `RETIRED` and rejects
recreation. A later chart upgrade prunes the permission. The removed
worker/ServiceAccount is never the verifier. A
prepared-but-unadmitted decision therefore pins its old cohort, as do private
shadow requests and nonterminal actions. Before rollback, the API verifier reads
and hashes the exact Deployment, ServiceAccount, both Pod owner chains, and one
final snapshot. `DRAINING -> ACCEPTING` is a no-I/O transaction that locks
cohort -> handoff -> both registration leases -> both rows named by their
normalized current execution owners, rechecks owner JSON/scalar/API stable-Pod/
start equality, and replaces registration
evidence with one current V2 set containing the exact two current ACTIVE lease
renewal-registration bytes after stable equality to the membership anchors and
its sole final Deployment snapshot. Both registration and API server-instance
leases are fresh and the exact
renewal bytes and snapshot satisfy the common five-minute bound against that
transaction's one fresh PostgreSQL timestamp; it validates the pre-read exact
Deployment, ServiceAccount, qualified image, and manifest and rejects any
`OPEN | READY` handoff or accepted member with a terminal `STALE_HANDOFF` lease.
Immediately before commit, fresh PostgreSQL time rechecks both registration/API
expiries and all proof bounds; drift or a wait past TTL rolls back.
The source stays `DRAINING` until a chained handoff completes. In contrast,
`DRAINING -> REMOVAL_AUTHORIZED` remains legal under its exact zero-reference
proof over non-`RELEASED` rows: it revokes the survivor, retires the incomplete membership, and permits
no future handoff;
a retired ID is never recreated.

Claim filtering is by a closed server-owned handler allowlist plus frozen
cohort predicate in the existing queue query. For action requests the query
joins the existing action/current-attempt correlation, requires the action to
remain `QUEUED`, and matches the immutable spec's cohort ID/Deployment UID; for
private shadow-candidate requests it matches the
same closed cohort fields in their internal payload. A cohort admits only its
matching private shadow/resource-action handlers; ordinary normal executors
exclude them, and every cohort excludes unrelated public handlers. A
missing/mixed allowlist, unknown cohort, mutable cohort manifest, or handler
inventory blocks shadow-window collection and authority. This adds no request
class, queue row type, claim token, heartbeat, or request/action-execution
lease; the separate registration-liveness lease grants no work authority.

Before either branch, the SQL predicate requires the exact registered private
payload type `sky.server.requests.payloads:ResourceActionPrivateRequestBodyV1`,
format `pydantic-json`, version 1, an object-valued `payload_json` with
object length exactly one, and the sole underscore alias object. Wrong
type/format/version, nonobject JSON, public-name encoding, or an extra sibling
key rejects. Mutation tests cover each independently.

Action claims additionally require the same decision's `ACTION_ACTIVE`
reference; private shadow claims require its `SHADOW_ACTIVE` reference. A
`DRAINING` cohort does not invalidate either. A released, missing,
cross-decision, or cross-Deployment reference rejects the claim.

API-request revision 007 only admits `authority-worker` as a durable
`api_server_instances.role`; it changes no queue or request shape and preserves
ordinary API006 rows. General-role query construction excludes the four private
names without a Serve import or Serve relation. P2a's health/preflight
listeners and bootstrap coordinator resolve their preflight-only V1 identity
against Serve033, start before registration, and never start the executor. M4
authority startup additionally requires exact API008 with the built-in
PostgreSQL request storage/queue backend attestations and execution-quiescence
capability, plus the exact registered handler inventory,
and resolves its immutable V2 cohort identity and fresh leases against Serve038
plus its execution-authority lineage/head contract against the exact active
policy-bound Serve039, or policy-bound Serve040 after the gated M5a advance,
before P3 may call `executor.start()` or start queue workers. The role therefore
fails closed on an API007/Serve032 P2a deployment and on any live private/M4 deployment
without exact policy/physical-head equality, while ordinary API007/008 executors remain operational
through both staged Serve migrations.

P2a renders only the release-namespace self-attestation Role/Binding needed to
GET Pods and ReplicaSets for the worker's owner chain plus the exact versioned
Deployment and ServiceAccount by `resourceNames`. Runtime code further requires
the Pod name/namespace/UID from downward-API identity, follows only its
controller owner references, and rejects any object outside that chain. P2a
renders no ClusterRole/Binding, canary-namespace Role, access-review create,
Namespace/version/admission/LB/workload GET, or workload mutation permission.
Those broader resources below belong to P2b/P3 and remain absent while the
initial evaluator can return only not-representable. The sole conditional
exception is a tombstone-bearing removal upgrade: it adds a distinct Role /
Binding for the surviving API ServiceAccount, limited to GET the exact
tombstoned Deployment and ServiceAccount names so it can commit NotFound /
`RETIRED`; the next upgrade prunes that binding. It grants no Pod/ReplicaSet
or wildcard read.

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

- M4/provider authority is not reached. Keep the authority worker and every
  provider-authoritative route disabled until the remaining gates below are
  implemented and verified; immutable specs, progress, and codecs alone do not
  authorize provider I/O. Private shadow transition requires
  `PrivateShadowActivationProofV2`; every live authoritative dispatch must
  consume `PrivateDispatchReadinessProofV2` at claim-start and persist/exact-
  adopt Serve039 lineage; authority promotion separately requires
  `AuthoritativePromotionProofV2`. Private-shadow dispatch remains disabled
  until its now-specified one-to-one history/progress/return/reduction and same-
  inventory representability contracts are implemented and verified. The pure readiness predicate does not
  itself enable a route. The V2-only live parser, native V2 seed/input and
  launch/down constructors, and sole cleanup rederiver are implemented. The
  remaining code gate also includes the Serve039 lease-owner/process/lineage /
  selector/shadow-history migration and separate metadata, PostgreSQL-versus-
  SQLite target routing, exact API boot/health lifecycle and Serve-owned GC,
  fixed warmed supervisor/process pool, post-039 INSERT/BIND, current-owner
  RENEW/acknowledgement, process supersession, generation-only cancellation and
  terminal batches, stable/process attestation, V2 policy/candidate/proof family,
  same-engine connection-borrowing API006 seams, claim-start lineage and lost-
  ACK adoption, bounded historical reduction, and settled-replay validation.
  The representability code gate includes the authoritative direct-no-
  effect proof/outcome builder, expanded authoritative handler/direct/fallback
  parser, raw-invalid profile/classifier integration, shared exact post-
  materialization projector, final V2 artifact/callable inventories, and
  finite case inventory/enumerator plus acyclic CI-only goldens.
  Repository inventory must
  prove that admission,
  materialization, claim, provider-context, submit/observe, pre-I/O, recovery,
  and reducer boundaries reject V1 before any artifact or effect; V1 remains
  explicit history/cleanup-only parsing.
- Build an image from exact P2a merge
  `4c91d3345ccb5f19538c9f8376c5e7403f5644cc`, deploy it dark, and live-
  qualify it before enabling a cohort: the six closed wire envelopes, strict
  TLS/purpose-token transport,
  `/bootstrapz` versus `/readyz` split, complete projected manifest,
  Pod/ReplicaSet/Deployment/ServiceAccount observer, PostgreSQL-clock two-Pod
  registration, and post-build OCI qualification. The role starts no queue
  executor and its only accepted response remains typed not-representable. P2a
  includes same-Pod stable-registration refresh only; that is not Serve039
  current-process owner RENEW. Process-owner bind/supersession and the exact
  process/API/GC/concurrency suite remain P3 merge gates. The one-set M4 policy
  has only exact-M4 idempotent redeploy recovery; any new forward-fix binary
  first requires a qualified successor two-set policy. The distinct
  all-M5a -> all-M4 -> all-M5a binary rollback matrix runs under the later
  two-set successor. Pod-replacement /
  rolling registration.
- Before the first enabled P2a cohort, implement and live-verify the retained
  release/database anchor above across first-enable crashes, empty values,
  backend/Secret changes, missing Secret, rollback, and ordinary uninstall.
  Keep `resourceActions.authorityWorker.enabled=false` until that gate passes.
- The additive V2 capsules now use only the compact permanent-row reference,
  and the authority module owns the typed V2 cohort resolver. Exact structural
  full-spec
  goldens are 56,994 bytes for realistic launch, 56,977 for the alternate
  admitted launch binding, 45,045 for completed down, and 48,560 for the
  selected candidate-maximal partial down; candidate-maximal keeps admitted
  frozen inputs byte-exact and maximizes only declared runtime-derived
  evidence. Their respective SHA-256 values are
  `7d680f846c37326330903064bc210fb73a67e6b7625b1614b17ce9df6feea733`,
  `7392f6792ec560ce4a99884b9bc2dd6ac83a4a5925a936ace27de8fcf458891e`,
  `f638480d05f9283a52c7b1075ab2df9a1a3a8280890f9e01fa10053d3277c82d`,
  and
  `b66dabb27ec6f8cb7fff670bf8a1975228741ea80b8aea2c87cd822dd901c796`.
  A valid nested graph using the generic 1,024-byte workspace maximum
  renders 62,047 bytes and is rejected by the unchanged 60,000-byte outer
  qualification gate. The additive V2 preflight parser/transport is complete
  structural input. Before P2b linked admission, freeze the final V2
  inventories and prove every live boundary resolves its reference against the
  parsed locked V2 row, runs the applicable external-context validator, invokes
  the already-implemented sole cleanup rederiver, and passes the exact finite
  representability case tuple.
  Regenerate the full-spec and envelope goldens after binding the V2
  inventories; the baseline counts and hashes above cannot qualify that
  changed graph. The exact 60,851-byte frozen V1 launch remains history, not a V2
  qualification result; do not increase either budget.
- Exact inventory of existing providers that can propagate a stable
  cluster-record UUID/incarnation before launch; multi-node/compound launch is
  ineligible until all effects have one exact observable target contract.
- The current branch has no accepted five-artifact `pod_cluster_v1` renderer
  cohort or canary-qualified `kubernetes_admitted_object_v1` normalizer, so
  `unrepresented_execution_config` and shadow-only routing remain mandatory.
  Bind and live-qualify the five implemented pinned artifacts with
  realistic/candidate-maximal body goldens, prebooted runtime, action-keyed
  Skylet, mutation-trace, handle/endpoint, and observation fixtures against
  real Kubernetes. Then exercise the exact 12-role in-cluster
  principal/authorization/admission/network preflight plus both launch LB
  Deployment-to-ServiceAccount projections on the selected Boltz canary path,
  and verify the down preflight performs zero LB Deployment GETs.
- The code/fixture side of the atomic same-v1 body cutover is complete and has
  no dual-shape compatibility. Before accepting the renderer, retain the
  deployment gate: prove a zero persisted represented launch graph under the
  pinned old writer, deploy every reader/writer containing the exact-body
  validator, and repeat the zero-row checkpoint before representation resumes.
- Integrate the implemented provider renderer and admitted-object normalizer
  with the already-implemented launch/down invocation and execution-config
  contracts, complete preflight seeds, controller/executor dual policy-absence
  proof sequence, preparation/counted-slot gate, and exact partial
  UID-qualified adoption/deletion. Neither execution config currently has a
  runtime consumer capable of provider mutation.
- Implement the frozen server-URL normalizer above as the sole kubeconfig-
  server decomposition path and run its DNS/default-port/path/IPv4/IPv6/IDNA /
  percent-encoding golden matrix before enabling the live Kubernetes
  normalizer. The pure transport DTO accepts only its canonical output.
- After P2a, implement the complete live preflight evaluator and same-client runtime
  admission/session, including current-scope drift checks, artifact loading,
  principal/authorization proofs, and the exact request-handler dispatcher.
  The dispatcher must checkpoint the implemented API006 progress contract and
  operation IDs before/after each real effect; strict return codecs are a
  persistence fence, not a dispatcher.
- Integrate the implemented cursor validator, lineage-safe retry seed, exact
  reducer/fallback, and quiescence builder with the live handler and the
  owner-fenced retained-source admission transaction. Completed and partial
  basis parsers exist, but no provider-runtime path yet performs partial-launch
  cleanup admission, effect readback, or Serve projection.
- Implement and verify the closed effect-body trace, qualified
  manifest/config/CRI runtime identity, and the Skylet fsynced outbox/run-token/
  post-exec-handshake recovery state machine, including full retained-request
  reconstruction and equal-hash/unequal-byte conflict handling. Measure
  realistic and candidate-maximal renderer/preflight/runtime goldens below the
  bound before enabling any authority.
- The dark API -> ordinary executor -> controller rollout/current-chart
  rollback is complete. Still open are rendered and live verification of the
  dedicated authority-worker Helm
  versioned-cohort contract and the implemented Serve038 V2
  `REGISTERING`/two-ready-Pod/set-level-snapshot activation; runtime
  `OPEN`/`READY`/`COMPLETED`/`ABANDONED` replacement remains unimplemented,
  release-namespace worker/Service/RBAC/projections, two distinct frozen LB
  Deployments and explicit ServiceAccounts, separate canary workload namespace,
  purpose-specific TLS/token transport, exact same-client facade and
  RBAC/access-review matrices, controller-only preflight network path,
  frozen-cohort claim routing/retention, surviving-API tombstone verification,
  two-Pod attestation, candidate/stale/survivor fault injection, and a later
  authority-cohort rollout/rollback with nonterminal references pinned to their
  cohort.
- A measured complete-shadow window of at least 86,400 seconds, at least 100
  clean represented launch graphs and 100 clean represented down graphs, zero
  divergence/blockers, and the complete crash/HA inventory. A server policy
  may raise but never lower these floors.
