# Reserved GPU Fallback and Serve Worker Reconciliation

## Status

Approved for implementation on 2026-07-20. The two milestones are independently shippable and are intentionally split into separate pull requests and deployments.

Fable review 1 reshaped the design to use current-pod Karpenter Events rather than unproven pod conditions, and narrowed stale-worker cleanup to the confirmed launch-row race.

## Problem

Two independent lifecycle failures can make a SkyServe fleet appear stuck or over-provisioned.

The `prod_research_cluster_eks` reserved cluster exposes fixed GPU nodes, but its only Karpenter NodePool is CPU-only. GPU replicas can schedule while an existing matching slot is free. Once those slots are unavailable, Karpenter reports that `nvidia.com/gpu.product` has no known values. SkyPilot currently treats that deterministic incompatibility like a potentially recoverable scheduling delay and waits for the Kubernetes provisioning timeout before trying the next candidate.

Separately, a completed Serve launch worker may remain in controller memory after its replica row has disappeared from durable state. The refresher currently asserts that the row exists. That assertion aborts the entire refresh pass and prevents unrelated teardown reconciliation.

## Goals

When Kubernetes reports a deterministic GPU-product incompatibility, the placement attempt should fail immediately through the existing insufficient-resource path so Serve can try its next candidate. Ordinary scheduling contention and autoscaler progress must retain their current timeout behavior.

When durable replica state no longer contains a completed launch worker's replica, the Serve controller should discard that stale local launch bookkeeping and continue reconciling other replicas. Durable state remains authoritative.

Both changes must preserve existing mixed-version behavior, require no schema or API changes, and be independently reversible.

## Non-goals

This design does not add GPU capacity to the research cluster, modify its Karpenter NodePools, shorten the global Kubernetes provisioning timeout, or permanently disable the reserved cluster after one failed placement.

It does not change Serve demand calculation, reserved-capacity budgeting, candidate ordering, rollout version election, or teardown commitment semantics.

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

During the existing pod-scheduling wait loop, inspect all recent `FailedScheduling` Events for the current pending pod UIDs. A coalesced Event created before an adopted pending pod's current launch attempt qualifies only when its latest occurrence timestamp is fresh. Event timestamps and the cutoff are normalized to aware UTC; naive timestamps are interpreted as UTC before comparison.

The query path uses a cache entry per `(context, namespace)` with a short monotonic-time TTL and per-entry single-flight locking. A caller increments the entry's pin count under the global cache lock before releasing that lock; eviction considers only unpinned entries. The caller releases its pin in a `finally` block. The global lock protects only entry lookup, pinning, bounded insertion, and least-recently-used eviction; it is never held during a Kubernetes call or while waiting for another entry's refresh. If the entry bound is full and every entry is pinned, diagnosis for a new uncached key is skipped for that iteration and the normal scheduling wait continues.

Each refresh lists `reason=FailedScheduling` with `_request_timeout=kubernetes.API_TIMEOUT`, examines the complete response, and stores only a normalized mapping from matching pod UID to its latest qualifying occurrence timestamp and message. Karpenter joins NodePool failures with a semicolon through its multi-error path; any such combined diagnostic is conservatively rejected, so a CPU-only NodePool cannot fast-fail a pod that also has a GPU-capable NodePool with a transient failure. Both namespace entries and per-entry UID matches have fixed maxima. API failures store an empty negative result for the normal TTL, preventing retry bursts while preserving the existing timeout path.

If any Event matches the classifier, raise `KubernetesError(..., insufficent_resources=['GPUs'])` immediately. The backend already converts that exact classification into an availability failure. The existing Serve availability retry then benches that placement for the current launch attempt and evaluates the next candidate. The signal is scoped to the current launch; the existing placer TTL later re-probes the reserved cluster after capacity may have become free.

Generic `FailedScheduling`, temporary insufficient capacity, taints, topology delays, pending storage, image pulls, and an autoscaler's ordinary provisioning progress continue using the existing timeout. The implementation must not add per-launch Events API polling.

### Milestone 1 tests

Add focused unit tests for the exact HyperPod/Karpenter Event, a same-pod generic scheduler Event preceding it, a semicolon-delimited mixed-NodePool Event, a non-GPU incompatible requirement, an old or wrong-UID Event, old creation with a fresh coalesced occurrence, naive and aware timestamp normalization, Event API failure with negative caching, concurrent per-key single-flight, pin-safe entry eviction, entry and match bounds, and the normal scheduling path. Prove that only the exact current single-diagnostic Karpenter GPU Event exits before the configured timeout and that the raised error retains the GPU insufficient-resource classification used by availability retry.

## Resulting Flow

```text
Serve demand
    -> Kubernetes candidate launch
       -> existing pod list/wait loop
       -> bounded shared FailedScheduling Event snapshot
          -> current Karpenter GPU incompatibility
             -> existing KubernetesError with GPUs insufficient
             -> existing Serve availability retry
             -> next candidate
          -> any other pending signal
             -> existing timeout behavior

completed Serve launch worker
    -> refresh durable replica mapping
       -> row exists: existing result reconciliation
       -> row absent: remove stale local launch bookkeeping and continue
```

## Alternatives

Reducing the Kubernetes provisioning timeout globally would make legitimate node provisioning and transient scheduling less reliable. It also would not distinguish a fixed GPU pool from a cluster whose autoscaler can add matching nodes.

Polling Kubernetes Events independently from every launch would multiply API traffic by the number of concurrent launches. A short-lived, bounded snapshot shared by context and namespace preserves fast diagnosis while bounding the aggregate query rate.

Adding a GPU Karpenter NodePool is an infrastructure capacity decision. SkyPilot still needs correct fallback when any reserved pool is exhausted or cannot expand.

Combining the milestones in one pull request would couple unrelated Serve and Kubernetes provisioning risks and make rollback less precise.

## Rollout and Verification

Each milestone is formatted, tested, reviewed, merged, and deployed independently. The full visible GitHub check rollup is the merge gate.

Deploy the exact generated release with Helm `--reuse-values`. Verify the live API image and commit identity before evaluating service behavior.

For milestone 0, verify that controller refreshes no longer emit the missing-launch-row assertion and that teardown counts continue falling when a stale completed launch worker is encountered.

For milestone 1, verify from a controlled saturation case or equivalent production event evidence that a deterministic reserved-cluster GPU incompatibility exits quickly and the next candidate begins provisioning. Also verify that reserved placements still succeed when matching GPU slots are available.

Rollback is a Helm rollback to the prior chart revision. Neither milestone changes durable schema or API contracts.
