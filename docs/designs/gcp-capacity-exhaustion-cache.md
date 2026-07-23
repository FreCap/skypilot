# Extend the Short-Lived Capacity Exhaustion Cache to GCP

## Context

`sky/provision/capacity_cache.py` records short-lived hints so that a Spot demand
which just failed for physical capacity is not immediately retried against the
same exhausted pool. It is AWS-only today: every cache key is prefixed `aws:`,
`ResourceKey` carries an AWS `account`, and `_capacity_cache_key` /
`_quota_cooldown_key` in `sky/backends/cloud_vm_ray_backend.py` return `None`
for any non-AWS cloud. `_classify_capacity_error` likewise returns `None` unless
the cloud is AWS.

GCP receives no such protection, so every replica launch rediscovers the same
exhausted zones from scratch. Measured on `boltz-l4-fleet` over 24 hours:

- 216 GCP placement failures, all carrying
  `VM_MIN_COUNT_NOT_REACHED` plus `ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS`,
  with no quota codes mixed in.
- Only 16 distinct `(region, zone, instance_type)` shapes, concentrated in
  `asia-northeast3-a` (89), `asia-northeast3-b` (43) and `europe-west4-b` (28).
- Replaying those events against the existing `_CAPACITY_TTL_SECONDS` of 120s,
  178 of 216 attempts (82%) would have hit an active hint and been skipped.

Each avoided attempt saves a full GCP bulk-insert round trip plus the failover
bookkeeping behind it, and removes a corresponding burst of noise from the
replica logs.

This design depends on the outcome classification fix (PR #874), which taught
`_placement_outcome` to read every provider code rather than only the first.
That change is reporting-only and deliberately did not widen
`_CAPACITY_ERROR_CODES`, because that set gates cache suppression and needs the
separate treatment described here.

## Behavior Contract

1. A GCP Spot demand that fails with a recognized zonal capacity code records a
   hint keyed to the exact failing shape, and a subsequent identical demand is
   skipped while that hint is live.
2. Suppression is never broader than the exact shape that failed. A hint for one
   zone, machine type, accelerator set or node count must never suppress a
   different one.
3. Hints expire on the existing capacity TTL. No hint outlives its TTL, and a
   successful provision of the same shape clears it immediately.
4. Cache read or write failures never change provisioning behavior. Every cache
   interaction remains fail-open, matching the current AWS paths.
5. A quota denial continues to dominate a mixed batch, because it is regional
   and makes sibling-zone attempts for the same demand futile.
6. `VM_MIN_COUNT_NOT_REACHED` is treated as a neutral summary code, never on its
   own as evidence of capacity exhaustion.
7. Existing AWS behavior is unchanged, including its cache keys, TTLs and
   dashboard presentation.

## Design

### Classification

`_classify_capacity_error` currently requires that *every* code in the batch be
a known capacity or quota code, and returns `None` otherwise so that unknown
failures take the conservative failover path. GCP breaks that rule benignly: it
always emits `VM_MIN_COUNT_NOT_REACHED` as a summary alongside the code that
explains the cause. Treating it as unknown would mean GCP never classifies.

PR #874 already introduced the neutral set `_NEUTRAL_PLACEMENT_ERROR_CODES`,
currently `{'VM_MIN_COUNT_NOT_REACHED'}`, and applies it in `_placement_outcome`
by filtering neutral codes and then requiring every remaining code to be a
capacity code. `_classify_capacity_error` reuses that same set and discipline,
so an empty remainder stays unclassified and a genuinely unknown code keeps the
conservative path.

The all-remaining-must-match rule matters beyond GCP: the AWS provisioner
retries each subnet and appends one entry per distinct failure, so an aggregate
mixing capacity with an unrelated error must not be read as exhaustion.

The GCP capacity codes are `ZONE_RESOURCE_POOL_EXHAUSTED`,
`ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS`, `insufficientCapacity` and, for
TPU, `CapacityExceeded`. Two exclusions are deliberate. `UNSUPPORTED_OPERATION`
is observed on preemption during creation rather than an exhausted pool, so
suppressing future launches on it would be wrong. `RESOURCE_EXHAUSTED` is TPU
*quota* exhaustion in its only producer, `sky/provision/gcp/tpu_node.py`, and is
classified as quota rather than capacity.

One gap stays open. GCP also reports zonal exhaustion as the bare numeric
operation code 8, which `_provider_error_codes` stringifies to `'8'`. That token
is too collision-prone for a cross-provider set, so recognizing it requires
provider-scoped normalization rather than a raw set entry. Until that lands,
those failures take the conservative path and are simply not cached.

### Key identity

The AWS key is `(account, region, zone, instance_type, num_nodes)`. GCP maps
onto the same shape with the project in place of the account, with one required
addition.

On AWS the instance type fully determines the accelerator. On GCP that holds for
the GPU-bundled families (`a2`, `g2`), but not for N1, where accelerators are
attached separately. Keying GCP on the machine type alone would let an
`n1-standard-8` + T4 exhaustion suppress an `n1-standard-8` + V100 demand, which
violates contract point 2. The GCP key therefore includes a canonical
accelerator representation.

Today's `boltz-l4-fleet` traffic is entirely `a2-highgpu-1g` + `A100:1` and
`g2-standard-4` + `L4:1`, so the collision is not currently exercised, but it is
reachable as soon as an N1 shape enters the fleet.

Cache keys gain a cloud discriminator, so namespaces become `gcp:capacity_exhausted:v1:`
alongside the existing `aws:` ones. Because the capacity TTL is 120s, no
migration is needed: any key written under the old namespace expires within two
minutes of rollout.

The project identity is already available. `capacity_cache_account` is derived
from `cloud_user_identity`, which is fetched regardless of cloud, so the GCP
path needs no additional API call.

### Presentation

`active_service_observations` returns hints described as AWS-specific, and the
Serve placement page renders them under an "AWS launch suppression" heading. Both
become provider-neutral, with the provider carried per hint so the UI can label
each row. This is a visible API shape change on the placement endpoint and is
the main reason this work is separated from PR #874.

## Alternatives

**Do nothing.** The failover path already blocks an exhausted zone within a
single launch attempt, so this is a waste and noise problem rather than a
correctness one. Rejected because 82% of GCP capacity attempts are redundant,
and the volume distorts the placement page's failure reporting.

**Raise the TTL instead of adding GCP.** Does nothing for GCP, and the replay
shows no benefit from a longer window: 120s and 300s both suppress the same 178
attempts, because retries for a given shape cluster tightly.

**Rebuild the cache as a fully provider-agnostic component up front.** Larger
change with no additional near-term benefit, since AWS and GCP are the only
clouds emitting structured capacity codes on this path today. The cloud
discriminator introduced here leaves that refactor open.

**Key GCP hints on region rather than zone.** Suppresses far more attempts, but
violates contract point 2: GCP capacity is genuinely per zone, and a regional
hint would refuse zones that still have capacity.

## Milestones

1. Neutral-code filtering in `_classify_capacity_error`, plus GCP capacity codes.
   Unit tests only, no behavior change until the gates open.
2. Cloud discriminator and accelerator component in the cache key types.
3. Open the `_capacity_cache_key` and `_quota_cooldown_key` gates to GCP.
4. Provider-neutral observations in the placement API and the dashboard label.

Milestones 1 and 2 are independently landable and carry no behavior change,
which keeps the risky gate-opening step small.

## Rollout

The suppression gates ship behind a config flag defaulting to off, so the cache
can be enabled per deployment after the classification has been observed to be
correct in production. Because outcome classification landed first, the
placement history already labels GCP exhaustion as `capacity_failed`, so the
`capacity failed` counter can be compared before and after enabling the gates to
confirm suppression is not hiding real failures.

Rollback is a flag flip. Since hints expire in 120s, disabling the flag returns
behavior to the current state within two minutes with no cleanup.

## Test Plan

Unit:

- GCP batch of `VM_MIN_COUNT_NOT_REACHED` + `ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS`
  classifies as capacity.
- A batch of only neutral codes stays unclassified.
- An unknown code alongside a capacity code stays unclassified.
- A quota code in a mixed batch classifies as quota.
- Two GCP demands differing only in accelerators produce different keys.
- Two demands differing only in cloud produce different keys.
- Cache read and write failures leave the provisioning result unchanged.

Integration:

- With the flag off, GCP provisioning issues the same attempts as today.
- With the flag on, a forced capacity failure suppresses an immediate identical
  retry, and a differing accelerator shape is still attempted.
- A successful provision clears the hint for that shape.

Production validation:

- Replay the last 24h of `serve_placement_events` for a GCP-heavy service and
  confirm the predicted suppression count matches the 82% measured here.
- After enabling, confirm the `capacity_failed` count drops while `succeeded`
  does not, which would indicate suppression is skipping attempts that would
  have failed rather than attempts that would have succeeded.
