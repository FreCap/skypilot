# Reserved GPU Fallback and Serve Worker Reconciliation

## Status

Approved for implementation on 2026-07-20. The first two milestones are independently shippable and are intentionally split into separate pull requests and deployments. Milestone 2 was added after live research-only validation on 2026-08-05 exposed an indefinite-wait regression.

Last updated 2026-08-11. Milestone 3 extends the indefinite-wait contract to an
explicit Kubernetes-only heterogeneous placement policy while ensuring one
unknown or occupied location cannot monopolize the initial launch wave.

Fable review 1 reshaped the design to use current-pod Karpenter Events rather than unproven pod conditions, and narrowed stale-worker cleanup to the confirmed launch-row race.

## Problem

Two independent lifecycle failures can make a SkyServe fleet appear stuck or over-provisioned.

The `prod_research_cluster_eks` reserved cluster exposes fixed GPU nodes, but its only Karpenter NodePool is CPU-only. GPU replicas can schedule while an existing matching slot is free. Once those slots are unavailable, Karpenter reports that `nvidia.com/gpu.product` has no known values. SkyPilot currently treats that deterministic incompatibility like a potentially recoverable scheduling delay and waits for the Kubernetes provisioning timeout before trying the next candidate.

Separately, a completed Serve launch worker may remain in controller memory after its replica row has disappeared from durable state. The refresher currently asserts that the row exists. That assertion aborts the entire refresh pass and prevents unrelated teardown reconciliation.

## Goals

When Kubernetes reports a deterministic GPU-product incompatibility, the placement attempt should fail immediately through the existing insufficient-resource path so Serve can try its next candidate. Ordinary scheduling contention and autoscaler progress must retain their current timeout behavior.

When durable replica state no longer contains a completed launch worker's replica, the Serve controller should discard that stale local launch bookkeeping and continue reconciling other replicas. Durable state remains authoritative.

Both changes must preserve existing mixed-version behavior, require no schema or API changes, and be independently reversible.

When a caller explicitly selects an indefinite Kubernetes provisioning wait,
occupied fixed GPU capacity must remain pending instead of taking the fast
fallback path. This behavior composes with the original goal: finite waits
still fail fast so heterogeneous services can try their next candidate.

When every candidate is a non-spot Kubernetes exact positive whole-GPU shape
and a non-pool service explicitly enables a placement policy, accept the
catalog and make that policy own exact-shape selection. Use fresh free-capacity
observations for wide admission when capacity is available, and spread bounded
zero/unknown scheduler probes across the configured locations so separate
clusters can make progress concurrently and wait for occupied slots to return,
including when an admitted probe waits indefinitely.

## Non-goals

This design does not add GPU capacity to the research cluster, modify its Karpenter NodePools, shorten the global Kubernetes provisioning timeout, or permanently disable the reserved cluster after one failed placement.

Milestones 0–2 do not change Serve demand calculation, reserved-capacity
budgeting, candidate ordering, rollout version election, or teardown commitment
semantics. Milestone 3 changes only the interpretation and balanced consumption
order of the existing demand-launch budget; it does not change demand targets,
reserved-capacity-fill broker grants, rollout election, or teardown commitment.

Kubernetes context aliases that resolve to the same physical cluster are out of
scope. The production research configuration has two distinct context names
backed by two distinct EKS clusters. Operators must not list aliases as separate
placement locations because the demand cache is keyed by context and exact
accelerator name.

SkyPilot Pools remain outside Milestone 3. An all-nonspot placement catalog is
still rejected for a pool.

It does not change missing-row teardown handling. Safely recovering a failed teardown without its durable replica identity requires a separate tombstone design; silently discarding such a worker could lose the last evidence of leaked infrastructure.

## Existing Flow

```text
Serve demand
    -> reserved-capacity budget
    -> Kubernetes candidate launch
       -> pod scheduling wait loop
       -> timeout or success
       -> Serve availability retry on launch failure

completed Serve launch worker
    -> refresh durable replica mapping
       -> placement evidence and result reconciliation
       -> assert replica row exists
```

The decisive incompatibility is emitted as a Kubernetes Event by Karpenter. The scheduler emits a separate generic GPU-insufficiency Event for the same pod, so the classifier must examine all matching Events rather than selecting only the newest or first failure.

The Serve worker refresher retrieves replica rows in one durable-store operation. Database errors propagate. A missing key in a successful result means the durable row does not exist, rather than representing a transient database failure.

## Milestone 0: Stale Serve Launch Worker Reconciliation

Partition missing launch rows immediately after the bulk read, before either the spot-placement evidence pass or completed-result reconciliation can dereference them.

For a completed launch worker whose replica row is absent, remove the corresponding launch-future entry, request-ID mapping, and launch-cancellation bookkeeping. Log the replica ID and continue the same refresh pass. Do not reconstruct a row, retry the launch, or record candidate evidence because the durable placement identity is no longer available.

The cleanup must be idempotent and must not mutate mappings while iterating their live views. A stale launch worker must not prevent a later normal launch or teardown worker in the same pass from being reconciled.

### Milestone 0 tests

Add a focused unit test with a spot placer enabled, a stale completed launch worker, another normally completed launch worker, and a normally completed teardown worker. Prove the stale entry is removed before placement-evidence handling and that the other workers continue through reconciliation. Existing launch-failure evidence, retry, and teardown behavior remain covered by the surrounding suite.

## Milestone 1: Deterministic Kubernetes GPU Fast-Fail

Classify a pod as deterministically incompatible only when all of the following hold:

1. The Event belongs to the UID of one of the current pending pods from this launch. Its latest occurrence timestamp is no earlier than the pod-creation attempt. Timestamp precedence is `series.last_observed_time`, `event_time`, `last_timestamp`, then `metadata.creation_timestamp`.
2. The Event reason is `FailedScheduling`, its type is `Warning`, and its reporting component or source component is `karpenter`.
3. The Event message is a single Karpenter diagnostic, not a semicolon-delimited multi-NodePool error, and contains `incompatible requirements`, `nvidia.com/gpu.product`, and `does not have known values`.

During the existing pod-scheduling wait loop, inspect all recent `FailedScheduling` Events for the current pending pod UIDs. A coalesced Event created before an adopted pending pod's current launch attempt qualifies only when its latest occurrence timestamp is fresh. Event timestamps and the cutoff are normalized to aware UTC; naive timestamps are interpreted as UTC before comparison. The same normalized snapshot also answers the existing Karpenter `FailedScheduling` autoscaling heuristic, making this store the sole `FailedScheduling` Events read in the scheduling loop. Definitive `TriggeredScaleUp` detection retains its separate reason-specific query.

The query path uses a process-local cache entry per `(context, namespace)` with a short monotonic-time TTL and per-entry single-flight locking. A caller increments the entry's pin count under the global cache lock before releasing that lock; eviction considers only unpinned entries. The caller releases its pin in a `finally` block. The global lock protects only entry lookup, pinning, bounded insertion, and least-recently-used eviction; it is never held during a Kubernetes call or while waiting for another entry's refresh. If the entry bound is full and every entry is pinned, diagnosis for a new uncached key is skipped for that iteration and the normal scheduling wait continues.

Remote `sky.launch` requests execute in separate API-worker processes, so the process-local layer is backed by a pod-local cross-process snapshot store on the pod's non-NFS temporary filesystem. The store has a fixed number of buckets selected by SHA-256 over a canonical JSON encoding of `(context, namespace)`, so all worker processes choose the same bucket. Each bucket holds a bounded set of exact identities, their process-comparable monotonic refresh times, normalized matches, and bounded least-recently-used metadata. A colliding identity may occupy a free slot or replace an expired least-recently-used identity; if the bucket is full of fresh identities, diagnosis for the new key is skipped rather than evicting a live entry and recreating per-launch polling.

Each bucket has a nonblocking file lock. The winner holds it across the Kubernetes read and a same-directory atomic replacement through one fixed staging filename; a crashed writer can therefore leave at most one staging file, which the next winner overwrites. Lock contention skips diagnosis for that iteration, allowing the next scheduling-loop tick to consume the winner's snapshot. A malformed, wrong-version, or future-dated snapshot observed under the lock is treated as an empty bucket, refreshed once by that lock holder, and repaired atomically. The in-process global lock is never held while attempting the file lock. This makes the Events read single-flight across concurrent API worker processes within one control-plane pod while retaining the process-local fast path. Multiple API replicas or overlapping rollout pods may each perform one refresh per TTL; cross-pod coordination is intentionally out of scope because the current deployment has one Recreate-strategy API pod.

Each refresh lists `reason=FailedScheduling` with `_request_timeout=kubernetes.API_TIMEOUT`, examines the complete response, and stores a bounded normalized representation of every Event needed by either consumer: the latest occurrence timestamp for the existing autoscaling heuristic, plus the current-pod UID, timestamp, and message for exact Karpenter GPU incompatibilities. Karpenter joins NodePool failures with a semicolon through its multi-error path; any such combined diagnostic is conservatively rejected, so a CPU-only NodePool cannot fast-fail a pod that also has a GPU-capable NodePool with a transient failure. Process-local namespace entries, shared buckets, and normalized Event matches all have fixed maxima. API failures store an empty negative result for the normal TTL, preventing retry bursts while preserving the existing timeout path. `_cluster_maybe_autoscaling()` consumes this snapshot and must not call `list_namespaced_event` independently.

If any Event matches the classifier, raise `KubernetesError(..., insufficent_resources=['GPUs'])` immediately. The backend already converts that exact classification into an availability failure. The existing Serve availability retry then benches that placement for the current launch attempt and evaluates the next candidate. The signal is scoped to the current launch; the existing placer TTL later re-probes the reserved cluster after capacity may have become free.

Generic `FailedScheduling`, temporary insufficient capacity, taints, topology delays, pending storage, image pulls, and an autoscaler's ordinary provisioning progress continue using the existing timeout. The implementation must not add per-launch Events API polling.

### Milestone 1 tests

Add focused unit tests for the exact HyperPod/Karpenter Event, a same-pod generic scheduler Event preceding it, a semicolon-delimited mixed-NodePool Event, a non-GPU incompatible requirement, an old or wrong-UID Event, old creation with a fresh coalesced occurrence, naive and aware timestamp normalization, Event API failure with negative caching, concurrent per-key single-flight, pin-safe entry eviction, entry and match bounds, and the normal scheduling path. Clear the process-local cache between two reads to prove the shared snapshot avoids a second Events call. Cover stable bucket selection, multiple colliding identities, a full-fresh collision skip, expired-entry replacement, malformed and future-dated snapshot repair, fixed staging-file atomic replacement, and shared-lock contention. Prove that concurrent launch loops using both the deterministic classifier and the Karpenter autoscaling heuristic make one total `list_namespaced_event` call per snapshot TTL. Prove that only the exact current single-diagnostic Karpenter GPU Event exits before the configured timeout and that the raised error retains the GPU insufficient-resource classification used by availability retry.

## Milestone 2: Explicit Indefinite Reserved-GPU Wait

`kubernetes.provision_timeout: -1` is the existing public contract for waiting
indefinitely on an unscheduled pod. The scheduling loop already preserves a
negative timeout in its deadline calculation, but Milestone 1 currently raises
the Karpenter GPU incompatibility before that contract can take effect.

This matters on `prod_research_cluster_eks`: fixed GPU nodes use
`nvidia.com/gpu.product`, while Karpenter manages CPU-only groups. A pending
A100-80GB pod can therefore have matching fixed nodes that are merely occupied
and simultaneously receive Karpenter's “GPU product has no known values” Event.
The Event proves only that Karpenter cannot add a matching node; it does not
prove that a fixed matching slot can never become free.

When the effective provisioning timeout is negative, skip the deterministic
Karpenter GPU fast-fail and retain the pod in the ordinary scheduling loop.
Finite zero or positive timeouts preserve Milestone 1 exactly: a matching Event
still raises the GPU-classified Kubernetes error immediately and Serve may try
its next candidate. No autoscaler-dialect override, node relabeling, schema
change, API change, or new capacity is introduced.

The live reproducer used server `1.1.1114` at
`28f24dd495378385f270ebce0c0c5b93dd733028`, exactly one
`A100-80GB:1`, and no cloud fallback. Kubernetes admitted the low-priority pod,
reported 33 nodes with insufficient GPU and 10 selector mismatches, and left it
unbound. The Milestone 1 shortcut removed it and retried. Earlier successful
reserved-pod waiting predates the shortcut's 2026-08-04 deployment.

### Milestone 2 tests

Add a focused scheduling-loop test whose first observation is an unbound
pending pod and whose later observation is the same pod bound to a node. With a
negative timeout, prove the Karpenter fast-fail classifier is never called and
the loop reaches the later scheduled observation. Retain the existing exact
Karpenter Event test as proof that finite-timeout fast failure and GPU
classification are unchanged.

## Milestone 3: Kubernetes-only Heterogeneous Placement

An ordinary `resources.any_of` catalog does not make the Serve placement policy
own a non-spot launch. The generic launch path may therefore choose one
accelerator shape and retry it in place instead of returning a typed capacity
failure to the policy. Before this milestone, enabling the policy did not solve
that problem because submission validation rejected a catalog with no spot
entry.

For a non-empty catalog whose entries are all non-spot Kubernetes locations
with exactly one positive whole-number accelerator shape, an explicitly enabled
placement policy is valid. CPU-only, fractional-GPU, and compound accelerator
shapes remain invalid because the shared free-GPU budget cannot account for
them. Fresh demand launches use the policy's active-location snapshot, persist
one exact selected location, and set the availability retry limit to one. A
typed capacity failure therefore benches that shape and lets a later
reconciliation select another active candidate. Batch launch decisions take
the same placement snapshot before selecting any of their shapes.

The existing capacity snapshot distinguishes measured free capacity from an
ambiguous zero before provider launch. A zero remains authoritative for an
ordinary fixed zero-cost tier in a mixed paid fallback. In an all-Kubernetes
placement catalog, however, zero cannot distinguish a truly unavailable pool
from scale-to-zero capacity or a slot expected to return, so it receives only
the bounded speculative allowance. A context with a configured Kubernetes
autoscaler receives the same bounded allowance so a pending pod can trigger
scale-up from zero. Missing observations also receive only that allowance.
Selection consumes speculative and measured allowances in balanced rounds
across exact locations instead of exhausting the first equal-cost location
before considering the next. With two Kubernetes contexts, an admission wave
of at least two backends therefore contains work for both contexts whenever
both have measured or speculative budget.

Balancing applies across the backends in one demand wave; it does not hedge one
logical replica. A one-replica wave has only one selected location, so with
``provision_timeout: -1`` that sole probe may remain pending indefinitely on an
unavailable context. Operators who want concurrent cluster initialization must
drive demand for at least two backends.

A zero-capacity probe enables a Kubernetes scheduling decision; it does not
alter priority or guarantee eviction. Milestone 3 does not rewrite task
priority configuration. The production inference class is intentionally
``-1000`` with ``preemptionPolicy: Never``: it cannot evict BCL occupants,
while higher-priority BCL pods can reclaim GPUs from these inference pods.
Verification of BCL reclamation must inspect the admitted pods' resolved
priorities and the scheduler's nomination or preemption Events.

`kubernetes.provision_timeout: -1` remains valid in this mode. It means an
individual admitted, unscheduled pod may wait indefinitely; it does not disable
other locations in the same demand wave. A scheduled/bound pod has already
acquired capacity and leaves the scheduling-timeout path, so container, image,
and model initialization remain governed by their existing startup/readiness
contracts rather than by `provision_timeout`. Finite timeouts still provide a
bounded escape for ambiguous unscheduled states, while definitive autoscaler
progress retains the existing deadline extension.

This milestone does not change the public `provision_timeout` default and does
not introduce a special 30-second default. For a fallback-oriented fleet,
omitting the setting retains the ordinary single-pod 10-second ambiguity
guard; the
autoscaler-aware path raises its initial detection window and extends it after
definitive scale-up evidence. For fixed reserved pools where an occupied slot
is expected to become available again, `-1` is the appropriate queueing policy.
Clusters whose GPU nodes can scale dynamically still use the infrastructure's
existing, deliberately longer autoscaler timeout policy. A 200-backend target
with 200 GPUs already measured free can be admitted in one 120/80-balanced
wave. If all 200 GPUs are occupied but reclaimable, admission starts with four
bounded probes per context (eight across two contexts) and grows in later
reconciliation waves as probes schedule; the 200-backend free-capacity test
does not claim instant reclamation of 200 occupied slots.

### Milestone 3 tests

Validate that an A100-80GB/H200 non-spot Kubernetes catalog is accepted when
the placement policy is enabled and the effective provisioning timeout is
finite, absent, or `-1`, while an all-on-demand non-Kubernetes catalog remains
invalid. Reject all-nonspot Kubernetes catalogs that are CPU-only, fractional,
or otherwise lack one exact whole-GPU shape per entry. At runtime, prove that a
fresh Kubernetes-only launch is pinned to one configured GPU shape, uses
placement-owned availability handling for finite attempts, and requests a batch
placement snapshot. Prove that equal measured or speculative budgets across two
contexts are selected in alternating rounds, so both contexts enter the
admission wave before either receives its second launch. For a 200-replica
measured budget split 120/80, prove the first 128 selections split 64/64 and the
final selections consume exactly 120/80. Retain the mixed-placement test proving
that an ordinary non-spot fallback keeps its existing retry behavior. Prove a
measured zero remains authoritative in that mixed fixed-pool mode, while an
all-Kubernetes catalog and a context with a configured autoscaler each receive
only the bounded probe allowance.

## Resulting Flow

```text
Serve demand
    -> Kubernetes candidate launch
       -> existing pod list/wait loop
       -> bounded shared FailedScheduling Event snapshot
          -> current Karpenter GPU incompatibility
             -> negative timeout: keep the admitted pod pending
             -> finite timeout: existing KubernetesError with GPUs insufficient
                -> existing Serve availability retry
                -> next candidate
          -> any other pending signal
             -> existing timeout behavior

Kubernetes-only heterogeneous Serve demand with placement policy
    -> measured-free locations get their exact budget
    -> measured-zero and unknown locations get bounded scheduler probes
    -> balanced budget selection puts distinct locations in the same launch wave
    -> policy selects and persists one exact GPU shape per physical attempt
       -> success: launch that shape
       -> finite typed capacity failure: bench it for later reconciliation
       -> indefinite unscheduled attempt: remain pending without blocking
          already-enqueued attempts for the other locations

completed Serve launch worker
    -> refresh durable replica mapping
       -> row exists: existing result reconciliation
       -> row absent: remove stale local launch bookkeeping and continue
```

## Alternatives

Reducing the Kubernetes provisioning timeout globally would make legitimate node provisioning and transient scheduling less reliable. It also would not distinguish a fixed GPU pool from a cluster whose autoscaler can add matching nodes.

Polling Kubernetes Events independently from every launch would multiply API traffic by the number of concurrent launches. A short-lived, bounded snapshot shared by context and namespace preserves fast diagnosis while bounding the aggregate query rate.

Adding a GPU Karpenter NodePool is an infrastructure capacity decision. SkyPilot still needs correct fallback when any reserved pool is exhausted or cannot expand.

Configuring the context as the generic autoscaler dialect requires every GPU
node to carry a durable `skypilot.co/accelerator` label. The live research nodes
instead use GPU Feature Discovery's `nvidia.com/gpu.product` labels. Relabeling
the shared cluster and maintaining replacement-node labels is a wider platform
rollout than honoring the existing negative-timeout contract.

Combining the milestones in one pull request would couple unrelated Serve and Kubernetes provisioning risks and make rollback less precise.

## Rollout and Verification

Each milestone is formatted, tested, reviewed, merged, and deployed independently. The full visible GitHub check rollup is the merge gate.

Deploy the exact generated release with Helm `--reuse-values`. Verify the live API image and commit identity before evaluating service behavior.

Pre-deployment read-only evidence on 2026-08-11 confirmed that the east and
Phoenix research contexts are distinct EKS clusters (not aliases), neither has
a SkyPilot ``kubernetes.autoscaler`` setting, and the affected east service
uses ``provision_timeout: -1`` with priority ``-1000`` / preemption policy
``Never``. At observation time, 73 low-priority inference GPU pods were running;
31 BCL GPU pods requested 200 GPUs at priority 0 with
``PreemptLowerPriority``. No workload PDB covered either set, and recent
scheduler Events recorded low-priority inference pods as preemption victims.
This proves the configured priority direction and supplies strong operational
evidence for BCL reclaim, but it is not a fresh end-to-end BCL canary.

For milestone 0, verify that controller refreshes no longer emit the missing-launch-row assertion and that teardown counts continue falling when a stale completed launch worker is encountered.

For milestone 1, verify from a controlled saturation case or equivalent production event evidence that a deterministic reserved-cluster GPU incompatibility exits quickly and the next candidate begins provisioning. Also verify that reserved placements still succeed when matching GPU slots are available.

For milestone 2, deploy a matching client/server commit and repeat the bounded
one-A100-80GB research-only probe. Require the same admitted pod UID to remain
unbound and Pending across the observation window, then remove only the exact
temporary service. Verify no AWS resource or GPU allocation occurred. A later
free-capacity acceptance must prove the waiting pod can proceed normally.

For milestone 3, submit a zero-replica A100-80GB/H200 research-only service with
the placement policy. With two contexts and unknown capacity, drive demand for
at least two backends and verify that queued exact placements alternate between
them. With a fresh zero-capacity observation, verify only the bounded Serve
probes are submitted. In a separately authorized priority canary, place a
low-priority inference replica on a free GPU, submit a higher-priority BCL
workload, and verify BCL reclaims that GPU while the inference replica is
redriven. Repeat with
`provision_timeout: -1` and verify both contexts receive their first admitted
probe before either receives a second; remove the temporary service and every
probe afterward. No ordinary on-demand resource is eligible in this test.

The control-plane deployment is independently verifiable without launching a
GPU workload: require the exact immutable image digest, successful migration
hook, API readiness, controller recovery, and unchanged existing-service
status. A live Milestone 3 GPU/preemption canary remains an explicit operational
gate because it exercises real scheduling and can affect live GPU workloads; do
not run it without separate workload and capacity authorization.

Rollback is a Helm rollback to the prior chart revision. No milestone changes
durable schema or API contracts.
