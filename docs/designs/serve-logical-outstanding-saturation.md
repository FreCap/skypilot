# Logical outstanding-work saturation

> Superseded by [Demand-aware logical scaling with bounded waves](serve-demand-aware-scaling-ramp.md).
> Saturation remains supported for compatibility, but it is no longer the
> recommended way to smooth rejection-driven demand for one-job-per-GPU
> services.

## Problem

SkyServe logical fleets currently map every outstanding unit directly to one
logical GPU target. Outstanding work deliberately includes in-flight, queued,
recently rejected, and unknown-occupancy safety demand. Production observation
of a long-running GPU service showed that this one-to-one mapping and the
default 20-minute downscale hold can retain hundreds of logical targets after a
burst even after current work has drained.

## Behavior contract

`target_concurrency_per_replica` remains the number of outstanding work units
the autoscaler assigns to one GPU before adding another logical GPU slot. In
logical mode, the target is:

```text
ceil((in_flight + queued + recently_rejected + unknown_floor)
     / target_concurrency_per_replica)
```

Recently rejected jobs remain deduplicated and retained by the existing reject
window. Unknown occupancy remains a fail-closed contribution. Logical
inventory, placement, and retirement targets remain GPU-slot counts.

The load balancer's asynchronous occupancy queue remains the execution gate.
Increasing the autoscaling saturation target does not increase simultaneous
model execution per GPU.

## Solution

Allow logical fleets to configure a positive integer
`target_concurrency_per_replica`. Divide both fresh outstanding work and the
stale-report arrival floor by that value when computing a logical GPU target.
Keep physical-backend autoscaling, rejection retention, request dispatch,
logical placement, and scale-down safety unchanged.

The initial consumer will configure two outstanding requests per GPU: one may
be executing while one waits. Its service policy will also choose a shorter
downscale delay independently; changing the generic default is out of scope.

## Compatibility and rollout

The existing value of one produces identical targets, so old specs and
persisted services retain their behavior. A policy update can change the knob
without a database migration; per-version state and the first-recompute snap
already protect live updates.

Release the SkyPilot change before applying any logical service spec with a
value greater than one. Validate on a test fleet before production. Rollback is
a service update restoring the value to one.

## Alternatives considered

- Removing rejected jobs loses demand that failed admission and is explicitly
  not part of this change.
- Shortening the rejection window changes a separate safety contract.
- Adding a moving-average autoscaler adds state and delays durable long-running
  work. Saturation is the smaller first step.
- Raising model execution concurrency risks GPU oversubscription and is a
  separate runtime decision.

## Test plan

- Validate positive integer logical saturation targets and reject invalid
  logical values.
- Verify fresh logical targets divide in-flight, queued, rejected, and unknown
  work while keeping rejected jobs in the numerator.
- Verify stale arrival floors use the same saturation divisor.
- Verify the value one retains existing behavior and physical-backend target
  math is unchanged.
- Run focused service-spec and concurrency-autoscaler unit tests and format the
  changed files.
