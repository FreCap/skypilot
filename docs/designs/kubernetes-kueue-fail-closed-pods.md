# Fail-closed Kueue management for Kubernetes pods

- Status: Incident root cause proven; Phase 1 merged; protocol-v2 response,
  adoption, post-wait admitted-only/Pod-UID continuity, and two-phase Kueue
  admission/scheduling deadlines implemented and unit-verified; final feature
  validation and cluster policy rollout pending
- Last updated: 2026-08-20
- Owners: SkyPilot Kubernetes and Serve

## Context

SkyPilot already supports Kueue's plain-Pod integration.  When
`kubernetes.kueue.local_queue_name` (or its `kubernetes.quota.queue` alias) is
configured, every node Pod receives the Kueue LocalQueue label and the
pod-group metadata needed for gang admission.

Before this design, that integration was best effort.  If the Kueue mutating
webhook is absent, excludes the workload namespace, or has plain-Pod integration
disabled, Kubernetes accepts the same Pod as an ordinary scheduler workload.
The Pod can then consume GPUs without a Kueue Workload, quota reservation,
priority, or preemption policy.  A low Kubernetes `PriorityClass` does not close
this gap: Kueue cannot reclaim quota from a workload it never managed.

The 2026-08-08 Phoenix incident is now attributed to this gap from retained EKS
audit records, Kueue Events, live cluster state, and an isolated reproduction:

- Four 32-GPU research PyTorchJobs were created between 01:08 and 01:21 EST
  (06:08--06:21 UTC).
  Kueue created their Workloads but created no Pods.  Repeated Workload Events
  said the `hyperpod` topology could not fit the pod sets because
  `nvidia.com/gpu` excluded 59--63 of 64 nodes.
- The inference placement had created 128 one-GPU SkyPilot head Pods.  At
  deletion time all 128 were Running on 16 H200 nodes.  None had a Kueue queue
  label, managed label, admission gate, or owning Workload.  They therefore
  consumed physical topology capacity without becoming Kueue preemption
  candidates.
- The inference Pods used Kubernetes priority `-1000` and either the default or
  GPU bin-pack scheduler.  That had allowed scheduler-level preemption in older
  flows, but it could not help here: the higher-priority research Pods did not
  exist until Kueue admission, while Kueue could not evict Pods it did not own.
- Simone's administrator identity deleted the 128 inference Pods from 05:13:39
  through 05:14:22 EST (10:13:39--10:14:22 UTC).  Kueue reserved quota and
  admitted all four research Workloads at 05:23:14 EST (10:23:14 UTC), and
  their Pods were created immediately.  This precedes the 06:23 EST Slack
  update that the jobs were running and the later 06:44 EST announcement that
  inference was temporarily disabled.
- The reported 26 "fully GPU-free" nodes were a monitoring blind spot.  The
  list included the 16 nodes occupied by those 128 SkyPilot Pods.  It counted
  managed-job GPU requests but omitted the direct inference Pods.

The causal mechanism was a control-plane inversion: Kueue topology-aware
scheduling correctly refused to admit research gangs into physically occupied
topology, but the occupants had bypassed Kueue, so Kueue's configured
lower-priority preemption could not reclaim them.  This design closes that
governance gap and makes future attribution conclusive: a strict SkyPilot Pod
either has synchronous Kueue admission attestation or fails closed.

Kueue's plain-Pod protocol gives us a strong admission boundary:

- a queue label selects the namespace-local LocalQueue;
- the webhook marks an accepted Pod with `kueue.x-k8s.io/managed=true`;
- the `kueue.x-k8s.io/admission` scheduling gate prevents scheduling until
  Kueue admits the generated Workload; and
- `kueue.x-k8s.io/priority-class` selects a WorkloadPriorityClass independently
  of Kubernetes Pod priority.

References:

- [Kueue plain Pods](https://kueue.sigs.k8s.io/v0.18/docs/tasks/run/plain_pods/)
- [Kueue WorkloadPriorityClass](https://kueue.sigs.k8s.io/docs/concepts/workload_priority_class/)
- [Kueue preemption](https://kueue.sigs.k8s.io/docs/concepts/preemption/)
- [Default LocalQueue](https://kueue.sigs.k8s.io/docs/tasks/manage/enforce_job_management/setup_default_local_queue/)

## Goals

- Automatically require Kueue management whenever an effective LocalQueue is
  configured, without requiring a second opt-in flag.
- Fail closed when the Kueue webhook or plain-Pod integration is missing or
  mis-scoped: an unverified Pod must not reach a GPU.
- Fail immediately, before any Pod operation, when the selected LocalQueue or
  its ClusterQueue is missing, inactive, unreconciled, namespace-ineligible,
  or cannot be verified.
- Make the effective queue a server-owned placement decision in required mode.
- Map SkyPilot's named resource `priority_class` to Kueue's
  WorkloadPriorityClass on the generic non-projected required path. Protocol-v2
  projected workers reject task-owned priority and use immutable server-owned
  projection admission.
- Preserve existing behavior for placements with no effective Kueue queue.
- Cover SkyServe replicas because they use the same Kubernetes Pod provisioning
  path as ordinary SkyPilot clusters.
- Keep Kueue quota waiting out of the ordinary Pod scheduling timeout. A
  required-Kueue Pod gets a bounded queue-admission phase followed by a fresh
  configured scheduling deadline after admission.

## Non-goals

- Installing Kueue or choosing ClusterQueue quota, cohort, namespace-selection,
  or preemption policy from SkyPilot.  The reusable infrastructure module may
  create a namespaced LocalQueue only when the operator explicitly names both
  that LocalQueue and its already-designed ClusterQueue.
- Defining a cluster's preemption or cohort policy.  SkyPilot can attest that a
  workload is governed; the cluster operator still owns who may preempt whom.
- Converting the SkyServe controller, managed-jobs controller, or other SkyPilot
  system controllers into Kueue workloads.  Their controller-launch paths
  intentionally remove queue configuration.
- Replacing Kueue's Workload controller with a SkyPilot-owned queue controller.
- Discovering or installing Kueue, or guessing which LocalQueue a placement
  should use when none is configured.

## Public contract

Operators select a Kubernetes LocalQueue at global, workspace, or context
scope.  A non-empty effective queue automatically activates strict management:

```yaml
kubernetes:
  context_configs:
    research-phx:
      namespace: rescluster-k8s-prod-east1-preemptible-inference
      kueue:
        local_queue_name: default
```

`kubernetes.quota.queue` remains an equivalent queue-name spelling.  Resolution
keeps the existing precedence: workspace over global, context over cloud, and
`quota.queue` over `kueue.local_queue_name` within one scope.

There is no second flag required for safety.  `require_managed: true` remains
available as an optional assertion: it enables strict mode and makes a missing
queue a configuration error.  `require_managed: false` cannot downgrade a
placement that has an effective queue; removing the queue is the explicit way
to disable Kueue for that placement.  This avoids recreating the incident when
an operator configures a queue but forgets the assertion flag.

When an effective queue is present, or `require_managed` is `true`:

1. A non-empty effective LocalQueue name is mandatory.  Resource rendering
   fails before Pod creation if it is absent.  At the final provisioning
   boundary, SkyPilot also reads that namespace-local object and requires its
   `Active` condition to be `True` before it lists, adopts, deletes, or creates
   any Pod.  A missing object, inactive or unreconciled condition, malformed
   response, RBAC denial, or Kubernetes API error fails provisioning
   immediately rather than leaving a gated Pod pending until
   `provision_timeout`.
2. `require_managed` and an API-server-configured effective queue are taken
   from API-server config, not a remote client's request override.  Workspace
   config is already wholly server-owned; global and context `require_managed`
   fields are additionally removed from client overrides.  If API-server
   config contains any queue or explicit strict placement, queue-name overrides
   are also stripped during the server merge.  On a server with no configured
   queue or strict placement, a legacy request-provided queue remains accepted,
   but it also activates gating and attestation rather than best-effort mode.
3. SkyPilot reasserts the queue, label-form pod-group name, exact group size,
   `retriable-in-group=false`, and optional WorkloadPriorityClass after all
   custom Pod configuration has been merged. It removes the competing
   annotation-form group identity.
4. SkyPilot removes any client-supplied `kueue.x-k8s.io/managed` label and
   Kueue managed finalizer, then pre-adds the Kueue admission scheduling gate
   before submitting the Pod. Protocol-v2 projected workers first strip every
   caller-supplied Kueue-prefixed label/annotation and scheduling gate, then
   install this one canonical request contract.
5. The immediate create response must contain `kueue.x-k8s.io/managed=true`,
   the exact queue, pod-group, and WorkloadPriorityClass labels; exact group
   count and `retriable-in-group=false` annotations; Kueue's eight-lowercase-
   hexadecimal `role-hash` annotation; and the Kueue admission scheduling
   gate. Admission-phase `podset`, `workload`, `local-queue`, and
   `cluster-queue` outputs must still be absent. Those values prove admission
   mutation ran against the intended closed contract.
6. A Pod that fails immediate create-response attestation is force-deleted and
   provisioning fails. If deletion itself fails at this pre-admission phase,
   the SkyPilot-added scheduling gate still prevents the unverified Pod from
   scheduling. An admitted, reused, or post-wait Pod is no longer protected by
   that premise: rejection still fails closed, and a materialized protocol-v2
   reserved-fill request forbids broad request-owned teardown or placement
   failover. If immediate absence cannot be confirmed, its durable replica
   owner performs exact cleanup under fresh authority.
7. Existing Pending or Running Pods found during resume/recovery are attested
   against their current Kueue lifecycle state. They require the same exact
   managed, queue, workload-priority, pod-group, group-count,
   `retriable-in-group=false`, and role-hash contract plus either the
   still-present admission gate or Kueue's managed finalizer after admission
   removes the gate. An admitted Pod must bind `podset` to its role hash;
   `workload=<pod-group>` is optional because it depends on topology-aware
   scheduling. The `local-queue=<queue>` and
   `cluster-queue=<preflight-cluster-queue>` outputs are mandatory and exact
   after admission. This requires Kueue's `AssignQueueLabelsForPods` feature
   and closes a LocalQueue-retargeting race between preflight and admission.
   With Kueue v0.19 implicit Topology Aware Scheduling, admission may also add
   `podset-unconstrained-topology="true"`; SkyPilot permits only that exact
   literal on an otherwise exact admitted, ungated Pod. It remains forbidden
   on the submitted or still-gated projection. Protocol v2 rejects every other
   unknown Kueue-prefixed metadata field and any scheduling gate other than
   Kueue admission or topology. Non-compliant Pods are deleted rather than
   adopted.
8. On the generic non-projected path, a resource-level `priority_class` is
   emitted as the official `kueue.x-k8s.io/priority-class` label. If that
   WorkloadPriorityClass does not exist, Kueue keeps the workload from being
   admitted; SkyPilot does not fall back to an unprioritized workload.
   Protocol-v2 projected workers instead reject task-owned priority and attest
   the immutable server-owned projection admission.
9. Required-Kueue provisioning has two sequential, bounded phases. First,
   SkyPilot waits up to the existing 24-hour Kubernetes queueing deadline for
   every exact Pod name and UID to lose the Kueue admission gate. While gated,
   each observation retains the managed finalizer and exact request labels and
   role hash; when the gate disappears it must also carry the exact PodSet
   binding and LocalQueue/ClusterQueue outputs. Every observation reuses the
   adoption attestation contract, so deletion, same-name replacement, identity
   mutation, or an ungated but incompletely admitted Pod fails closed. Time
   spent behind Kueue quota does not consume `provision_timeout`. After all Pods
   are admitted, SkyPilot starts a fresh `provision_timeout` clock for scheduler
   binding and then uses the existing unbounded container-initialization wait.
   Placements without required Kueue retain the existing single scheduling
   clock. The Pod lifecycle remains the runtime correctness boundary; reading
   Kueue Workloads is optional diagnostics and is not an RBAC prerequisite.
   Passive gated observations hold no service advisory lock, fleet guard, or
   process provider phase. Optional ephemeral-volume provisioning and the
   bounded Kubernetes bootstrap transaction (including Service/RBAC upserts)
   each run in a fresh short effect epoch; a stale launch cannot leak those
   provider objects before reaching Pod creation. The immediate create response
   is captured under another short effect epoch; after the passive watcher
   first observes all gates
   removed, it reacquires exact request/association and provider authority and
   repeats the full batch UID, finalizer, PodSet, and LocalQueue/ClusterQueue
   proof before handing off to scheduling. Database-only service updates,
   correctly fenced same-UID protocol-v2 recovery, and compatible v2
   materialization therefore remain able to make progress during the queue
   wait. The immutable physical-cluster capture deliberately remains active:
   another v2 caller may join it only with the same physical UID, while a
   tokenless legacy provider call against that same context fails busy until
   the capture retires. Isolated ambient work for another context remains
   independent. The same exact Pods are fresh-read under a later short epoch
   after Running before provisioning success is published.
   If the 24-hour admission deadline expires, protocol-v2 reserved fill emits
   a wire-safe typed `ReservedFillProviderPresentError` carrying the exact Pod
   name/UID set. It performs no request-owned deletion, absence publication,
   capacity failover, or legacy teardown proof. The durable association and
   replica pin remain authoritative until reconciliation freshly observes
   `PRESENT`, performs the single UID-fenced down, and proves `ABSENT`; an
   authority change at the timeout has the same fail-closed result. Generic
   required-Kueue launches retain the ordinary Kubernetes timeout error. The
   cleanup consumer is stacked PR #1608; #1607 must not deploy without that
   canonical adjudication path available in the rollout.

Kueue plain-Pod management does not cover Deployment-owned Pods with the same
create-response attestation boundary.  Required mode therefore rejects
SkyPilot's high-availability Deployment path.  SkyPilot system controllers are
unaffected because their launch context removes both the queue and the explicit
strict assertion.

The reusable EKS spoke-workspace module exposes the corresponding cluster-side
contract as an optional object on each partition:

```hcl
partitions = [{
  namespace = "inference"
  kueue = {
    cluster_queue_name = "inference-borrower"
  }
}]
```

`local_queue_name` is optional and defaults to the literal `default`.  Kueue
0.18's GA LocalQueue defaulting then injects the queue label for otherwise
unlabelled integrated Pods before validation, while the admission policy
requires both that exact label and the `managed=true` value normally added by
Kueue.  An ordinary client that omitted Kueue metadata is therefore auto-fixed,
or denied if the webhook/defaulting path is unavailable.  These writable labels
are not cryptographic admission provenance against a namespace writer that
deliberately forges both values; strict SkyPilot closes that threat by stripping
the managed label, adding the scheduling gate, and attesting the create
response.  A non-default name is supported as an explicit loss of the
queue-label auto-fix behavior, without weakening strict SkyPilot.

Supplying the object creates only the namespace-local LocalQueue, grants the
SkyPilot control-plane subject `get` on that exact LocalQueue, and installs a
fail-closed Pod admission rule requiring that exact queue label and managed
value.  Before creation, the module reads the named ClusterQueue
and accepts only a selector that is explicitly empty (all namespaces) or
consists solely of
`kubernetes.io/metadata.name=<partition namespace>`.  This deliberately
conservative subset is mechanically provable without guessing external
namespace labels.  It then waits for the LocalQueue's `Active=True` condition.

Although the ClusterQueue object name field accepts a DNS subdomain, Kueue
0.18's `AssignQueueLabelsForPods` controller emits the admitted
`cluster-queue-name` Pod label only when that name is also a DNS-1123 label.
Strict adoption requires that exact output to bind admission to preflight.
Both the infrastructure module and runtime preflight therefore reject dotted,
over-63-character, uppercase, underscore-containing, or otherwise non-label
ClusterQueue names before any Pod mutation, with an actionable error. This is
an explicit deployment naming prerequisite, not a post-admission retry.

The selector check is necessary because Kueue 0.18 defines LocalQueue
`Active=True` only in terms of the referenced ClusterQueue's active condition;
the scheduler checks the ClusterQueue namespace selector later.  A queue can
therefore look active while every Workload from its namespace remains
inadmissible.  `Active=True` alone is not the module readiness contract.

The module does not install Kueue, create or mutate a ClusterQueue, infer a
queue from cluster discovery, or grant this read to the workload Pod
ServiceAccount.  An absent object creates no Kueue resources, admission rules,
or RBAC, preserving existing callers.  A missing CRD or ClusterQueue,
inaccessible cluster, selector mismatch, inactive queue, or ownership conflict
fails the Terraform plan/apply rather than publishing a usable-looking
partition.

The resulting queue name remains explicit in server-owned workspace
configuration.  The control-plane and spoke stacks cannot safely auto-wire
that output: the control-plane identity is a prerequisite for creating each
spoke, so a reverse dependency would form a deployment cycle.  The Boltz
rollout will use the deterministic `default` name in both research contexts and
repeat it in the two inference workspaces; CI must check that equality.  If the
server setting is accidentally omitted, LocalQueue defaulting plus the managed
Pod admission policy still prevents an unmanaged Pod, but SkyPilot's gang
metadata and synchronous attestation require the explicit setting.

## Architecture and invariants

```text
effective queue or explicit strict assertion
        |
        v
render queue + derived strict mode + workload priority
        |
        v
preflight namespace-local LocalQueue
  - object exists
  - current-generation Active=True
  - target ClusterQueue exists and is current-generation Active=True
  - target ClusterQueue name is a DNS-1123 label (output-label capable)
  - target ClusterQueue selector matches the current Namespace
  - read is authorized
        |
        v
final Pod spec after custom pod_config
  - overwrite queue/group/count/retriable/priority metadata
  - remove forged managed/finalizer and competing group identity
  - protocol v2 strips all other Kueue metadata and gates
  - protocol v2 removes nodeName and installs its frozen server-owned scheduler
  - add the one Kueue admission scheduling gate
  - represented by kubernetes-python >=32.0.1 without typed-field loss
        |
        v
Kubernetes admission
        |
        +-- Kueue mutates Pod --> managed=true + role-hash --> attest
        |                         --> wait for admission
        |
        `-- Kueue does not mutate --> delete + error; scheduling gate stays shut
```

The following invariants hold whenever an effective queue exists or
`require_managed` is true:

- No newly created, unattested SkyPilot Pod can be scheduled.
- No Pod reconciliation or mutation begins unless the selected LocalQueue has
  been read successfully, both queue conditions are current and active, and the
  current Namespace still matches the target ClusterQueue selector.
- A configured queue cannot silently remain best effort because
  `require_managed` was omitted or set to false.
- A request cannot redirect a strict placement to another LocalQueue through
  either the `kueue.local_queue_name` or `quota.queue` spelling.  Stripping is
  performed before config merge, because ignoring only the resource-level
  override would still leave the request value in the merged config.
- A custom Pod spec cannot change the selected queue, pod-group identity, group
  cardinality, retriable policy, or WorkloadPriorityClass after server policy
  is resolved. A protocol-v2 projected worker has no second Kueue-prefixed
  metadata or scheduling-gate path. It also has no caller-selected scheduler or
  direct `nodeName` binding path: the candidate freezes the effective
  server-owned scheduler (defaulting to `default-scheduler`), final rendering
  installs that exact name, and removes `nodeName`.
- A caller cannot forge attestation by supplying the managed label in its Pod
  config.
- The immediate create response proves that Kueue admission mutation ran but
  not that quota was admitted. Required-Kueue provisioning first waits under
  the bounded queue-admission deadline while Kueue retains the scheduling gate,
  continuously binding every observation to the exact original Pod UID and
  adoption identity without holding service/fleet authority or the process
  provider phase. Once all gates are removed and exact admission outputs are
  present, one fresh bounded authority epoch repeats that exact batch proof;
  only then does the ordinary configured scheduling timeout begin from zero.
  It is not charged for time spent waiting on Kueue quota. The
  all-containers-Running observation returns each Pod's exact UID. Successful
   provisioning additionally requires a fresh post-wait GET of that same name
  and UID, still in `Running`, and attests its admitted-only identity. The fresh
  Pod must retain its exact projected scheduler and bind a non-empty
  `nodeName`; a fresh read of that exact Node must carry the immutable
  projection's exact accelerator label. A missing or same-name replacement
  Pod, mutated admission identity, still-gated or unbound post-wait object,
  alternate scheduler, or wrong accelerator Node fails closed.
- Immediate creation and later adoption are distinct attestation phases. The
  create response must have pre-admission metadata plus Kueue's role hash and
  admission gate and must remain unbound. Adoption accepts only that state or
  an admitted state with
  `podset=role-hash`, optional TAS workload identity, the mandatory exact
  local/cluster queue output pair, and Kueue's managed finalizer. Kueue v0.19's
  implicit-TAS admission output may additionally be the exact annotation
  `podset-unconstrained-topology="true"`; all other values, phases, and unknown
  Kueue metadata fail closed. A bound admitted adoption is accepted only after
  the exact bound Node passes the projected accelerator-label check; an
  admitted Pending Pod may remain unbound until the post-wait proof.
- Multi-node clusters remain one Kueue pod group, so partial admission cannot
  make a subset of the requested nodes run.
- Kueue preemption may delete a Pod.  SkyPilot/SkyServe recovery recreates the
  missing cluster/replica through the same gated, attested path.

The gate is deliberately added by SkyPilot rather than trusted to the webhook.
Kueue treats an already-present admission gate idempotently.  This reverses the
failure mode: a broken integration produces a visibly stuck, non-consuming Pod
that SkyPilot deletes, rather than an invisible GPU consumer.

## Cluster-side deployment contract

Strict SkyPilot support is only one half of the production guarantee.  Before a
shared inference placement is enabled, the cluster must provide:

1. Kueue plain-Pod integration for the inference namespace.
2. The configured LocalQueue in that namespace.  The reusable module defaults
   its name to `default`, activating Kueue's GA queue-label defaulting as defense
   in depth for older or non-SkyPilot clients that omit the label.
3. Permission for the Kubernetes identity used by SkyPilot provisioning to
   `get` `localqueues.kueue.x-k8s.io` in that namespace and exact `GET /apis`
   plus `GET /apis/` for served-version discovery.  Inability to perform these
   reads is intentionally a fail-closed launch error.
4. Exact-name `get` permission on the referenced ClusterQueue and current
   Namespace, so runtime preflight can catch queue-policy drift after Terraform
   apply without granting enumeration or mutation.
5. A fail-closed namespaced admission rule requiring the configured queue label
   and `managed=true` value on every direct Pod.  This protects older SkyPilot
   clients and ordinary direct Pod submitters that do not implement synchronous
   create-response attestation, including accidental webhook omission or
   mis-scoping.  Because Kubernetes labels are user-writable, strict SkyPilot's
   strip/gate/create-response attestation remains the boundary against a
   deliberate forgery.  The reusable module owns the Pod-specific rule
   automatically.
6. A lower Kueue workload priority for inference than research. Generic strict
   Pods may omit the WorkloadPriorityClass label and let Kueue derive workload
   priority from the Pod's Kubernetes PriorityClass. Protocol-v2 projected
   workers do not rely on that implicit fallback: the server-owned projection
   freezes an explicit WorkloadPriorityClass together with the admission
   queue. The Boltz rollout may give it the same reviewed `-1000` semantics as
   the admission-enforced Pod PriorityClass, but it is a distinct Kueue object
   and contract field.
7. One verified preemption domain:
   - preferably the same ClusterQueue with `withinClusterQueue: LowerPriority`;
     or
   - ClusterQueues in the same Cohort with reclaim/borrow preemption configured
     and tested.

The HyperPod task-governance add-on owns its generated Kueue objects.  Rollout
must use supported add-on inputs or separately owned objects that the add-on
will not overwrite; editing generated ClusterQueues in place is not a durable
solution.

SkyServe must also retain its shared-fleet safety contract: scale to zero when
idle, no unconditional nonzero fill floor, and no placement re-enable before
existing evidence or a separately authorized Kueue qualification proves the
preemption contract.

## Implementation phases

### Phase 1: SkyPilot fail-closed Pod path

- Add schema and precedence resolution for `kueue.require_managed`.
- Derive strict mode automatically from any non-empty effective queue; keep
  `require_managed: true` as a missing-queue assertion.
- Strip remote client overrides of the server-owned requirement.
- On servers containing a strict placement, strip queue-name overrides before
  merging request config with server config.
- Persist the resolved queue, strict flag, and WorkloadPriorityClass in the
  Kubernetes provider config.
- Preflight the selected LocalQueue and require `Active=True` before any Pod
  query, cleanup, adoption, or creation.
- Discover Kueue API v1beta2 with a v1beta1 fallback, then follow the LocalQueue
  reference to the ClusterQueue and validate current-generation activity plus
  the full Kubernetes Namespace selector on every launch.  This catches policy
  drift that occurs after the infrastructure apply.
- Require kubernetes-python >=32.0.1. Clients before 26.1 deserialize away
  `spec.schedulingGates` and DRA claim fields, while clients 26.1 through 31
  still deserialize away Pod-level `spec.resources`. Either loss can hide an
  admission mutation from the protocol-v2 whole-Pod accelerator contract. The
  dependency floor is global so strict attestation has one typed model path
  rather than a version-specific raw-JSON fallback; 32.0.0 remains unsupported
  because of its authentication regression.
- Gate, normalize, attest, and clean up direct Pods in the provisioner.
- Split required-Kueue waiting into an exact-Pod, bounded admission phase and a
  fresh ordinary scheduler-binding phase. Reuse the existing 24-hour queueing
  default for the first phase and the rendered `provision_timeout` for the
  second; do not make reserved-fill provisioning indefinite. Split protocol-v2
  provider authority at the same boundary: concrete create/read/mutation
  effects use short idempotent association/provider epochs, while passive
  admission and scheduling waits retain no service lock or provider phase.
- For projection protocol v2, expose an explicit provider protocol marker,
  strip every caller Kueue metadata/gate surface, and attest Kueue's
  phase-specific role-hash, PodSet, optional workload, mandatory exact queue
  outputs, and narrow response allowlists, including only v0.19's exact
  admitted implicit-TAS output. The all-containers-Running wait
  returns exact Pod UIDs; fresh-read and admitted-only reattest those same
  names and UIDs after the passive wait. Do not infer this strict contract from
  the accidental presence of individual projection fields.
- Reject the Deployment path in required mode.
- Extend config, request-sanitization, template, and provisioning unit tests.
- Update the Kueue example documentation.

### Phase 2: cluster policy rollout

- Add the optional per-partition Kueue contract to the reusable EKS spoke
  module.  It validates the existing ClusterQueue's namespace selector,
  provisions the LocalQueue, waits for `Active=True`, adds exact-name read RBAC
  and exact-queue/managed Pod admission, adds the runtime preflight's exact-name
  ClusterQueue/Namespace reads and both exact API-discovery-root spellings, and
  outputs the queue mapping.  The LocalQueue name defaults to `default` for
  Kueue webhook auto-fix behavior.
- Provision an operator-owned inference admission domain.  The referenced
  ClusterQueue must already select the inference namespace and implement the
  reviewed quota/cohort/preemption policy; its name must be a DNS-1123 label so
  Kueue can publish the exact admission output; the module must not infer it.
- Enable and verify Kueue's `AssignQueueLabelsForPods` feature. SkyPilot needs
  its exact LocalQueue/ClusterQueue output pair to bind admission to the queue
  preflight and fails closed when the pair is absent.
- Retain the existing admission-enforced `-1000` Kubernetes PriorityClass for
  Pods, and provision and freeze a distinct server-owned
  WorkloadPriorityClass with the reviewed lower-priority semantics for the
  initial Boltz rollout.
- Connect inference and research to a tested preemption domain.
- Expand queue-label admission to non-Pod workload kinds where each cluster's
  Kueue installation supports them.
- Configure the SkyPilot workspace/context with the inference LocalQueue;
  `require_managed: true` may be retained as an explicit assertion.
- Keep the shared research placement disabled until the rollout gates pass.

### Phase 3: activation and non-compute verification

- Inspect the existing managed inference and research evidence against the
  exact Pod/Workload contract before activation. If existing evidence cannot
  prove reclaim, keep activation closed until a separately authorized Kueue
  qualification supplies it; this rollout creates no GPU or BCL canary.
- Re-enable the intended SkyServe placement only after the immutable image,
  queue policy, and policy-plugin identity are proven, then observe ordinary
  traffic, preemption recovery, and scale-to-zero through existing workload.

## Deployment and fix forward

SkyPilot code can deploy before cluster activation because placements without
an effective queue remain unchanged.  Cluster resources and server-owned queue
config are deployed next, while the shared inference placement remains
disabled.  The expanded-runtime release and module RBAC must be deployed before
adding the queue to server-owned config; older module RBAC lacks the exact
ClusterQueue, Namespace, and API-discovery reads and will correctly fail closed.

For module-managed partitions, apply ordering is:

1. Provision or update the operator-owned ClusterQueue/preemption domain.
2. Disable new unmanaged inference placement and drain/down existing inference
   Pods.  The module's admission policy covers both creates and updates, so it
   must not be activated while legacy unlabeled Pods still need mutation.
3. Apply the spoke partition's `kueue` object.  Terraform creates the
   LocalQueue only after validating the ClusterQueue selector, waits for
   `Active=True`, and installs exact-name read RBAC plus exact-queue Pod
   admission.  Import an existing LocalQueue only if it already references the
   intended ClusterQueue; drain and replace rather than retarget it in place.
4. Verify the module output, `AssignQueueLabelsForPods`, and a direct
   authorization check.
5. Add the identical queue name to the server-owned inference workspace.
6. Verify the immutable version/Pod/Workload evidence without launching a
   synthetic workload, then re-enable shared placement only when the reclaim
   contract is already proven.

An unused LocalQueue does not affect existing Pods, but the exact-queue
admission binding deliberately does.  The drain in step 2 is therefore a hard
gate, not optional cleanup.  Reversing steps 3 and 5 is safe but unavailable,
because the runtime preflight fails closed until the queue becomes active.

Activation is per workspace/context.  Adding a queue (or enabling the explicit
assertion) on a context with existing ordinary Pods intentionally makes those
Pods ineligible for adoption.  Operators should drain or down old replicas
before activation, then let SkyServe recreate them under the new contract.

There is no supported demotion to unmanaged placement after activation. If a
defect appears, stop new shared inference placement or scale the service to
zero, preserve the Kueue objects and durable evidence, and deploy a corrected
full-fleet image or policy forward. Do not remove queue policy while strict
placement remains enabled: strict mode would safely stop new Pods, but the
service would be unavailable and continuously attempt recovery.

## Verification evidence and test plan

The protocol-v2 lifecycle hardening was verified on 2026-08-13 with a focused
`uv run --no-sync pytest` selection covering post-wait, required-Kueue,
adoption, and immediate-worker-attestation cases. It passed the wrong
ClusterQueue, still-gated, same-name replacement UID, and non-Running negative
regressions, in addition to the accepted lifecycle. The complete
`tests/unit_tests/kubernetes/test_provision.py` file subsequently exited
successfully. These results are new evidence and are not included in the
historical 197-test count below.

The reusable module and expanded runtime drift contract were verified on
2026-08-10:

- The EKS spoke module's native Terraform test suite passed 37/37 tests.  It
  covers no-op compatibility, default LocalQueue creation and readiness,
  ClusterQueue activity and selector rejection, exact managed-Pod admission,
  outputs, and partition validation.
- The shared Kubernetes RBAC module's native Terraform test suite passed 15/15
  tests.  It proves exact-name LocalQueue, ClusterQueue, and Namespace reads,
  exact `/apis` and `/apis/` discovery roots, no broad Kueue verbs, and no Kueue
  permission on the workload ServiceAccount.
- The focused Kueue runtime selection passed 39 tests, and the complete
  `tests/unit_tests/kubernetes/test_provision.py` file passed 197 tests.  The
  added cases cover API-version fallback, LocalQueue and ClusterQueue deletion,
  stale or inactive status, unreadable Namespace state, nil/mismatching/full
  selector semantics, and fail-before-Pod ordering.
- YAPF and isort completed, mypy reported no issues across 887 source files,
  and `git diff --check` passed.  The repository formatter's Pylint pass over
  the legacy provisioner test file continues to report that file's existing
  warnings; changed production modules have no new Pylint finding.

The LocalQueue preflight follow-up was verified on 2026-08-09:

- The focused required-Kueue regression selection and the complete
  `tests/unit_tests/kubernetes/test_provision.py` suite both exited
  successfully.
- Tests cover an active queue, a missing object, absent and false `Active`
  conditions, authorization and malformed-response failures, and ordering
  before the first Pod query.
- `format.sh` completed YAPF and isort, and mypy reported no issues across 887
  source files.  Pylint rated the changed production modules 10.00/10; its
  separate pass over the legacy provisioner test module reports that file's
  existing warnings.
- `git diff --check` passed.

Phase 1 automated verification completed on 2026-08-08:

- `bash format.sh --files ...` completed YAPF, isort, and mypy with no type
  errors across 884 source files.  Pylint passed for the changed production
  modules; invoking it on the legacy `tests/test_config.py` reports that
  file's existing warnings.
- The automatic-enforcement regression pass covered a queue with no assertion,
  a queue with `require_managed: false`, a request-provided queue on an
  otherwise queue-less server, a stale provider config with a false flag,
  missing-queue rejection, create-response attestation, existing-Pod adoption,
  Deployment rejection, rendering, and remote request sanitization.  The
  consolidated focused pytest invocation exited successfully.
- The affected config, Kubernetes provisioner, template-source, cloud render,
  and request-payload suites passed with eight pytest workers.  Three unrelated
  config tests were excluded because this development environment does not
  have the `rsync` executable they require.
- `git diff --check` passed.

Live Phoenix verification completed on 2026-08-08 through an administrator SSM
tunnel to the private EKS API.  The investigation made no changes to production
queues, nodes, GPU workloads, or priority classes.  Isolated tests used one
temporary namespace, separate ClusterQueues and a ResourceFlavor selecting one
CPU node.  The only scheduled test blockers requested 1--20 millicores.  A
large managed request retained an additional test scheduling gate and never
bound to a node.  All temporary namespaced and cluster-scoped resources were
deleted and verified absent afterward.

Three candidate causes were tested independently:

1. **Stopped or unhealthy Kueue admission.**  A ClusterQueue with
   `stopPolicy: Hold` produced `Active=False, reason=Stopped`; its Workload had
   `QuotaReserved=False, reason=Inadmissible` and message `ClusterQueue ... is
   inactive`.  Restoring `stopPolicy: None` admitted it.  Production did not
   have this signature: the controller recorded 8,227 successful leader-lease
   updates from 00:50 through 05:25 EST, with no gap above four seconds, and no
   controller restart or queue-spec write released the jobs.
2. **Nominal Kueue quota exhaustion.**  Two 10-millicore Pods behind a
   10-millicore test ClusterQueue produced `insufficient unused quota for cpu
   ... 10m more needed`; deleting the holder immediately admitted the waiter.
   Production did not have this signature.  The research ClusterQueue remained
   generation 1, Active, with 512 GPUs of nominal quota; the 304 admitted GPUs
   plus the four 32-GPU pending jobs totaled 432 GPUs.  Its Events instead
   named topology resource fit.
3. **Physical topology occupied by an unmanaged workload.**  With nominal
   quota available, a 20-millicore direct Pod made the only eligible test node
   too small for a managed request.  Kueue emitted `topology ... doesn't allow
   to fit ... excluded: resource "cpu"`, matching production.  The direct Pod
   had no queue label, managed label, gate, or Workload.  After it was removed,
   the identical managed request was admitted on its first fresh scheduling
   cycle.  Kueue kept the already-inadmissible copy in backoff for more than
   120 seconds, consistent with the delayed production retry after Pod cleanup.

Live inspection also confirmed Kueue v0.18.0 from the HyperPod task-governance
add-on v1.5.0, plain-Pod integration enabled, topology-aware scheduling enabled,
and the research ClusterQueue's `withinClusterQueue: LowerPriority` policy.  The
exact fail-closed wire contract was tested against that webhook: a Pod carrying
the queue label and a pre-added Kueue admission gate was accepted, received
`managed=true`, the Kueue finalizer, and exactly one admission gate, and created
a Workload.  An otherwise identical pre-gated Pod without a queue label received
no managed label, finalizer, or Workload and remained scheduling-gated.  This
validates both the positive attestation and safe negative path used by Phase 1.
The temporary attestation resources were also deleted and verified absent.  The
remaining production qualification below is a Phase 2/3 activation gate for
the new strict SkyPilot path and cross-priority eviction, not an incident-
attribution gate.

Automated tests must prove:

- schema acceptance and automatic strict behavior for queue-only global,
  workspace, context, and request-provided configurations;
- explicit false cannot downgrade a queued placement, while explicit true
  without a queue fails before Pod creation;
- missing, inactive, unreconciled, malformed, or unreadable LocalQueues fail
  before the first Pod query or mutation, while `Active=True` passes;
- precedence and absence-of-queue rejection;
- remote clients cannot override `require_managed`;
- remote clients cannot redirect queues on a server containing a configured
  queue or explicit strict placement, while queue-less servers preserve legacy
  request-provided queues and enforce them strictly;
- system-controller queue removal also disables strict mode;
- rendered strict Pods carry the queue, pod group, group count, gate, and
  `retriable-in-group=false`, and WorkloadPriorityClass;
- a Kueue-mutated create response succeeds only with the exact role-hash and
  pre-admission metadata phase;
- an unmutated, wrong-queue, malformed-role-hash, competing-group, unknown-
  metadata, or unknown-gate response is deleted and rejected;
- the AppArmor retry and terminating-Pod retry cannot bypass attestation;
- existing gated pre-admission Pods and ungated admitted Pods with
  `podset=role-hash` plus the exact local/cluster queue output pair are adopted;
  optional TAS workload is accepted only when exact; an ungated finalizer-only
  Pod, an admitted Pod without queue outputs, a label-only
  Pod, or any other phase mismatch is deleted rather than adopted; and
- post-wait success requires the exact UID observed with all containers
  Running, a fresh object still in `Running`, no admission gate, and the
  complete admitted PodSet/queue binding, exact projected scheduler, non-empty
  `nodeName`, and a fresh exact Node with the projected accelerator label; a
  gated object, same-name replacement, unbound/wrong-node object, alternate
  scheduler, or non-Running object is deleted and rejected; and
- a correctly gated Pod may wait longer than `provision_timeout`, admit before
  the queue deadline, and then receive the full fresh scheduling timeout;
- a non-Kueue unscheduled Pod retains the existing single timeout;
- an admitted but still-unbound Kueue Pod fails after the fresh configured
  scheduling timeout; and
- a Pod deleted, recreated under the same name, or mutated while awaiting
  Kueue admission fails closed; and
- required mode rejects Deployment-owned Pods.

The 2026-08-20 implementation verification ran the complete Kubernetes
provisioner unit-test module plus the Serve platform-projection and Kubernetes
cloud timeout suites. It covers a 16-second valid quota wait with a 15-second
configured scheduling timeout, a full fresh 15-second post-admission timeout,
the unchanged non-Kueue path, the bounded queue deadline, and fail-closed
deletion, recreation, finalizer loss, and queue-identity mutation.

Production non-compute inspection, with exact namespace, queue, and classes
substituted:

```bash
kubectl -n <inference-ns> get pod <replica-pod> -o yaml
kubectl -n <inference-ns> get workload -l kueue.x-k8s.io/queue-name=<queue> -o yaml
kubectl get clusterqueue <cluster-queue> -o yaml
```

The replica Pod must show `managed=true`, the expected queue, group, priority,
role-hash, and `podset=role-hash` binding, and no admission gate after
admission. Its Workload must show the expected LocalQueue, ClusterQueue,
priority, admitted quota, and full pod-group cardinality.

The production rollout does not create a GPU/BCL canary. Existing deployment
evidence, or a separately authorized Kueue-policy qualification, must show that
higher-priority research evicts enough low-priority inference Workloads and is
admitted without a Kueue restart, manual Pod deletion, or namespace drain.
SkyServe recreation must remain NotAdmitted while research holds quota and
must not thrash. Existing scale-to-zero evidence must show no inference Pods,
Workloads, or stuck Kueue finalizers remain.

## Open gates

- East1 live inspection on 2026-08-10 confirmed TAS disabled, plain-Pod
  integration enabled, an active research LocalQueue/ClusterQueue, no
  LocalQueue in the inference namespace, and no LocalQueue read permission for
  the SkyPilot provisioning subject.  The research ClusterQueue's namespace
  selector excludes the inference namespace, so pointing a new LocalQueue at
  it could report `Active=True` while all inference Workloads remain
  inadmissible; it is not an activation path.
- Current platform main reverted the attempted Phoenix TAS disable and keeps
  TAS enabled: disabling only the primary feature gate left dependent Kueue
  0.18 beta gates enabled and made the controller configuration invalid.  The
  checked-in and live research ClusterQueue still selects only
  `hyperpod-ns-research`.  AWS EKS metadata reports the managed task-governance
  add-on, so a standalone inference queue must use a separately owned name and
  must not overwrite an add-on-generated object.
- Each cluster needs an operator-owned inference ClusterQueue or other durable,
  supported preemption-domain change that admits the inference namespace.  Do
  not patch the HyperPod add-on's generated east1 ClusterQueue in place.
- The reusable module's direct-Pod admission rule covers SkyPilot pool Pods.
  Cluster-wide policy for other workload kinds remains owned by the Kueue
  installation; Phoenix already ships such a policy behind its own namespace
  label, while east1 needs an independently owned equivalent if non-Pod clients
  are admitted there.
- The research-over-inference reclaim contract must be proven from existing
  deployment evidence or a separately authorized Kueue-policy qualification
  before Phoenix is re-enabled for SkyServe. This feature rollout itself does
  not launch a capacity-consuming canary.

## Alternatives considered

### Require a separate opt-in boolean for queued Pods

Rejected.  Queue configuration already states that the workload belongs to
Kueue.  Making enforcement depend on a second boolean recreates a fail-open
operator-error path: forgetting the boolean leaves an apparently queued Pod
free to consume GPUs outside Kueue.  An explicit assertion remains useful for
detecting a missing queue, but it cannot weaken a queued placement.

### Rely only on Kubernetes PriorityClass

Rejected.  It may let the default scheduler preempt individual Pods, but Kueue
still cannot account for or govern a workload that bypasses its admission
boundary.  This is especially unsafe for gang workloads and quota reporting.

### Trust the queue label without attestation

Rejected.  This is the existing behavior: a broken or mis-scoped webhook leaves
the label in place while the ordinary scheduler runs the Pod.

### Check for a Workload after Pod creation

Rejected as the primary boundary.  It adds a controller race and leaves a
window in which an ungated Pod can schedule. Create-response attestation plus
a pre-added scheduling gate is synchronous and fail closed before admission.
A fresh post-wait Pod attestation then binds the admitted Pod to the exact
preflight ClusterQueue before provisioning can succeed. Workload checks remain
valuable operational verification but are not the runtime correctness
boundary.

### Let Kueue retain a gated Pod when the LocalQueue is missing

Rejected.  Kueue safely marks the generated Workload `Inadmissible`, but
SkyPilot would otherwise wait until `provision_timeout` (or forever for a
negative timeout) and obscure a deterministic configuration error.  A
read-only LocalQueue preflight preserves fail-closed behavior and returns the
actionable error before creating any object.  SkyPilot still does not create
the queue because its ClusterQueue, quota, cohort, and preemption policy are
operator-owned decisions.

### Make reserved-fill provisioning wait indefinitely

Rejected as the steady-state timeout contract. An indefinite ambient
provisioning timeout prevents legitimate Kueue queueing from failing early, but
also erases the scheduler-placement bound, affects non-Kueue pools, and can
retain a malformed or permanently blocked claim forever. The two-phase watcher
preserves a long but bounded Kueue admission window and starts the operator's
short scheduling timeout only after admission.

### Require cluster-wide `manageJobsWithoutQueueName`

Useful defense in depth, but not sufficient as SkyPilot's only guarantee.  It
is cluster-global/namespace-selector policy, can drift independently of
SkyPilot, and does not prove that a particular Pod was mutated.  A default
LocalQueue and admission policy are still recommended for non-SkyPilot clients.
