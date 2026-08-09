# Fail-closed Kueue management for Kubernetes pods

- Status: Incident root cause proven; Phase 1 automatic enforcement and LocalQueue preflight implemented; cluster rollout pending
- Last updated: 2026-08-09
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

- Four 32-GPU research PyTorchJobs were created between 06:08 and 06:21 UTC.
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
- Simone's administrator identity deleted the 128 inference Pods from 10:13:39
  through 10:14:22 UTC (06:13--06:14 Eastern daylight time).  Kueue reserved
  quota and admitted all four research Workloads at 10:23:14 UTC, and their Pods
  were created immediately.  This matches the 06:23 Slack update that the jobs
  were running; it precedes the later 06:44 announcement that inference was
  temporarily disabled.
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

- [Kueue plain Pods](https://kueue.sigs.k8s.io/v0.19/docs/tasks/run/plain_pods/)
- [Kueue WorkloadPriorityClass](https://kueue.sigs.k8s.io/docs/concepts/workload_priority_class/)
- [Kueue preemption](https://kueue.sigs.k8s.io/docs/concepts/preemption/)
- [Default LocalQueue](https://kueue.sigs.k8s.io/docs/tasks/manage/enforce_job_management/setup_default_local_queue/)

## Goals

- Automatically require Kueue management whenever an effective LocalQueue is
  configured, without requiring a second opt-in flag.
- Fail closed when the Kueue webhook or plain-Pod integration is missing or
  mis-scoped: an unverified Pod must not reach a GPU.
- Fail immediately, before any Pod operation, when the selected LocalQueue is
  missing, inactive, unreconciled, or cannot be verified.
- Make the effective queue a server-owned placement decision in required mode.
- Map SkyPilot's named resource `priority_class` to Kueue's
  WorkloadPriorityClass in required mode.
- Preserve existing behavior for placements with no effective Kueue queue.
- Cover SkyServe replicas because they use the same Kubernetes Pod provisioning
  path as ordinary SkyPilot clusters.

## Non-goals

- Installing Kueue, creating LocalQueues, or choosing ClusterQueue quotas from
  SkyPilot.
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
        local_queue_name: inference
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
3. SkyPilot reasserts the queue, pod-group name, group size, and optional
   WorkloadPriorityClass after all custom Pod configuration has been merged.
4. SkyPilot removes any client-supplied `kueue.x-k8s.io/managed` label and
   pre-adds the Kueue admission scheduling gate before submitting the Pod.
5. The create response must contain both
   `kueue.x-k8s.io/managed=true` and the expected queue label.  Those values
   prove that admission mutation ran against the intended queue.
6. A Pod that fails attestation is immediately force-deleted and provisioning
   fails.  If deletion itself fails, the SkyPilot-added scheduling gate still
   prevents the unverified Pod from scheduling.
7. Existing Pending or Running Pods found during resume/recovery are subject to
   the same attestation.  Non-compliant Pods are deleted rather than adopted.
8. A resource-level `priority_class` is emitted as the official
   `kueue.x-k8s.io/priority-class` label.  If that WorkloadPriorityClass does not
   exist, Kueue keeps the workload from being admitted; SkyPilot does not fall
   back to an unprioritized workload.

Kueue plain-Pod management does not cover Deployment-owned Pods with the same
create-response attestation boundary.  Required mode therefore rejects
SkyPilot's high-availability Deployment path.  SkyPilot system controllers are
unaffected because their launch context removes both the queue and the explicit
strict assertion.

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
  - Active=True
  - read is authorized
        |
        v
final Pod spec after custom pod_config
  - overwrite queue/group/priority metadata
  - remove forged managed label
  - add Kueue admission scheduling gate
        |
        v
Kubernetes admission
        |
        +-- Kueue mutates Pod --> managed=true --> attest --> wait for admission
        |
        `-- Kueue does not mutate --> delete + error; scheduling gate stays shut
```

The following invariants hold whenever an effective queue exists or
`require_managed` is true:

- No newly created, unattested SkyPilot Pod can be scheduled.
- No Pod reconciliation or mutation begins unless the selected LocalQueue has
  been read successfully and reports `Active=True`.
- A configured queue cannot silently remain best effort because
  `require_managed` was omitted or set to false.
- A request cannot redirect a strict placement to another LocalQueue through
  either the `kueue.local_queue_name` or `quota.queue` spelling.  Stripping is
  performed before config merge, because ignoring only the resource-level
  override would still leave the request value in the merged config.
- A custom Pod spec cannot change the selected queue, pod-group identity, group
  cardinality, or WorkloadPriorityClass after server policy is resolved.
- A caller cannot forge attestation by supplying the managed label in its Pod
  config.
- Successful provisioning implies that Kueue admission mutation ran.  It does
  not imply that the Workload has yet been admitted; normal provisioning waits
  while Kueue retains the scheduling gate.
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
2. The configured LocalQueue in that namespace.  A LocalQueue named `default`
   is recommended as defense in depth for older or non-SkyPilot clients that
   omit the queue label.
3. Permission for the Kubernetes identity used by SkyPilot provisioning to
   `get` `localqueues.kueue.x-k8s.io` in that namespace.  Inability to perform
   this read is intentionally a fail-closed launch error.
4. A low WorkloadPriorityClass for inference and a higher class for research.
5. One verified preemption domain:
   - preferably the same ClusterQueue with `withinClusterQueue: LowerPriority`;
     or
   - ClusterQueues in the same Cohort with reclaim/borrow preemption configured
     and tested.
6. An admission policy rejecting GPU Pods in the inference namespace when they
   omit the required Kueue queue metadata.  This protects the cluster from
   clients that predate SkyPilot's required mode.

The HyperPod task-governance add-on owns its generated Kueue objects.  Rollout
must use supported add-on inputs or separately owned objects that the add-on
will not overwrite; editing generated ClusterQueues in place is not a durable
solution.

SkyServe must also retain its shared-fleet safety contract: scale to zero when
idle, no unconditional nonzero fill floor, and no placement re-enable before
the Kueue preemption smoke test passes.

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
- Gate, normalize, attest, and clean up direct Pods in the provisioner.
- Reject the Deployment path in required mode.
- Extend config, request-sanitization, template, and provisioning unit tests.
- Update the Kueue example documentation.

### Phase 2: cluster policy rollout

- Provision the inference LocalQueue and WorkloadPriorityClass.
- Connect inference and research to a tested preemption domain.
- Add namespace-level defaulting/admission enforcement.
- Configure the SkyPilot workspace/context with the inference LocalQueue;
  `require_managed: true` may be retained as an explicit assertion.
- Keep the shared research placement disabled until the rollout gates pass.

### Phase 3: canary and activation

- Launch a one-node low-priority inference canary and inspect its Pod and
  generated Workload.
- Exercise queued multi-node admission and research-over-inference preemption.
- Re-enable one bounded SkyServe placement tier, observe it through scale-up,
  preemption recovery, and scale-to-zero, then expand.

## Deployment and rollback

SkyPilot code can deploy before cluster activation because placements without
an effective queue remain unchanged.  Cluster resources and server-owned queue
config are deployed next, while the shared inference placement remains
disabled.  The cluster rollout must grant the SkyPilot provisioning identity
read access to LocalQueues before adding the queue to server-owned config.

Activation is per workspace/context.  Adding a queue (or enabling the explicit
assertion) on a context with existing ordinary Pods intentionally makes those
Pods ineligible for adoption.  Operators should drain or down old replicas
before activation, then let SkyServe recreate them under the new contract.

Rollback order is the reverse:

1. Disable the shared inference placement or scale the service to zero.
2. Remove the queue from the server-owned placement if SkyPilot must
   temporarily return to unqueued behavior.  Setting `require_managed: false`
   alone does not bypass management for a queued placement.
3. Only then remove separately owned Kueue policy objects.

Do not remove Kueue policy while strict mode and the shared placement remain
enabled.  Strict mode would safely stop new Pods, but the service would be
unavailable and continuously attempt recovery.

## Verification evidence and test plan

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
   updates from 05:50 through 10:25 UTC, with no gap above four seconds, and no
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
remaining production smoke test below is a Phase 2/3 activation gate for the
new strict SkyPilot path and cross-priority eviction, not an incident-attribution
gate.

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
  WorkloadPriorityClass;
- a Kueue-mutated create response succeeds;
- an unmutated or wrong-queue response is deleted and rejected;
- the AppArmor retry and terminating-Pod retry cannot bypass attestation;
- existing non-compliant Pods are not adopted; and
- required mode rejects Deployment-owned Pods.

Production smoke test, with exact namespace, queue, and classes substituted:

```bash
kubectl -n <inference-ns> get pod <replica-pod> -o yaml
kubectl -n <inference-ns> get workload -l kueue.x-k8s.io/queue-name=<queue> -o yaml
kubectl get clusterqueue <cluster-queue> -o yaml
```

The replica Pod must show `managed=true`, the expected queue and priority
labels, and no admission gate after admission.  Its Workload must show the
expected LocalQueue, ClusterQueue, priority, admitted quota, and full pod-group
cardinality.

Then consume idle capacity with low-priority inference and launch a higher
priority four-node research gang workload.  The pass condition is that Kueue
evicts enough inference Workloads and admits the research workload without a
Kueue restart, manual Pod deletion, or namespace drain.  SkyServe may recreate
the evicted replica, but it must remain NotAdmitted while research holds quota
and must not thrash.  Finally scale the service to zero and verify that no
inference Pods, Workloads, or stuck Kueue finalizers remain.

## Open gates

- The live HyperPod-owned research ClusterQueue and its preemption policy have
  been confirmed.  The supported add-on customization surface for adding the
  inference queue/priority class without later reconciliation overwriting it
  must still be confirmed before Phase 2.
- The cluster-side LocalQueue, WorkloadPriorityClass, and admission policy need
  platform design approval before implementation.
- The research-over-inference preemption smoke test must pass before Phoenix is
  re-enabled for SkyServe.

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
window in which an ungated Pod can schedule.  Create-response attestation plus
a pre-added scheduling gate is synchronous and fail closed.  Workload checks
remain valuable operational verification.

### Let Kueue retain a gated Pod when the LocalQueue is missing

Rejected.  Kueue safely marks the generated Workload `Inadmissible`, but
SkyPilot would otherwise wait until `provision_timeout` (or forever for a
negative timeout) and obscure a deterministic configuration error.  A
read-only LocalQueue preflight preserves fail-closed behavior and returns the
actionable error before creating any object.  SkyPilot still does not create
the queue because its ClusterQueue, quota, cohort, and preemption policy are
operator-owned decisions.

### Require cluster-wide `manageJobsWithoutQueueName`

Useful defense in depth, but not sufficient as SkyPilot's only guarantee.  It
is cluster-global/namespace-selector policy, can drift independently of
SkyPilot, and does not prove that a particular Pod was mutated.  A default
LocalQueue and admission policy are still recommended for non-SkyPilot clients.
