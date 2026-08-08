# Cross-service reserved fill for Boltz and preferred borrowers

## Problem

`boltz-l4-fleet` historically claimed the
`prod_research_cluster_eks` A100 and A100-80GB reserved-capacity group by
itself, so its opportunistic fill replicas could occupy every otherwise-free
slot. Other services could not participate unless they exposed the same
Kubernetes pool shapes and enabled the shared fill policy.

The production policy is:

- retain at least 10 brokered fill slots for `boltz-l4-fleet` when the pool and
  Boltz fill headroom can support them;
- give remaining brokered capacity to preferred borrowers such as
  `opendde-10c200s-v4` and `protenixv2-batch50-v2` while they can use it;
- split capacity equally between preferred borrowers when both are eligible;
- remain work-conserving, so capacity a preferred borrower cannot materialize
  may flow to another borrower or back to Boltz;
- keep normal demand placement separate from opportunistic fill. Demand-placed
  replicas on the reserved pool remain exempt from fill grants.

## Existing behavior

The reserved-fill broker already supports this policy:

1. Claims share a broker round only when their pool keys are identical. The
   key is the Kubernetes context plus the canonical accelerator-name set.
2. Floors are allocated first.
3. Remaining capacity is allocated by weighted water filling.
4. A claimant's share is capped by the fill headroom it can materialize. Any
   excess is redistributed to other claimants.

All participating services need one-GPU Kubernetes candidates for the same
set:

```yaml
cloud: kubernetes
region: prod_research_cluster_eks
accelerators: A100:1
```

and:

```yaml
cloud: kubernetes
region: prod_research_cluster_eks
accelerators: A100-80GB:1
```

Each borrower's image, service account, object-storage permissions, disk size,
and setup path must work on both research-cluster shapes before the full
service relies on them.

## Configuration contract

Configure Boltz as the floor holder:

```yaml
service:
  replica_policy:
    reserved_capacity_fill:
      floor_replicas: 10
      weight: 100
```

Configure each preferred borrower as:

```yaml
service:
  replica_policy:
    reserved_capacity_fill:
      floor_replicas: 0
      weight: 1000000
```

`1000000` is the supported maximum weight. The 10,000-to-1 ratio between a
preferred borrower and Boltz assigns realistic weighted remainder to the
borrower while it has headroom. If multiple preferred borrowers are eligible,
their equal weights split that remainder equally. Boltz keeps a positive
weight so any excess can flow back to it instead of sitting idle.

Do not add `weight: 0`. The allocator requires positive weights, and the
bounded ratio already expresses the intended preference while preserving
work-conserving redistribution.

## Headroom limitation and rollout

Fill is bounded by a service's effective fill cap:

```text
max_replicas - demand_target
```

A borrower at `max_replicas` therefore has no opportunistic fill headroom.
Adding Kubernetes candidates still permits ordinary demand placement onto free
reserved slots, but ordinary demand does not receive a broker grant and does
not itself force a peer's borrowed fill to drain.

Use this bounded rollout for each borrower:

1. Validate the borrower image and setup on both research-cluster GPU shapes.
2. Add both Kubernetes candidates and the preferred-borrower fill policy.
3. Confirm the new version is elected and applied while the existing serving
   fleet stays healthy.
4. Confirm the broker claim uses the same canonical pool key as Boltz.
5. Confirm the broker retains Boltz's floor and feeds newly free slots to an
   eligible preferred borrower.
6. Watch the first Kubernetes replica through provisioning and readiness.

A separate demand-aware preemption design would be needed for ordinary demand
to evict another service's already borrowed fill automatically. That behavior
is out of scope because it couples demand replacement, pending-placement
cancellation, and broker grants.

## Rolling-update progress

Logical concurrency rolling updates must be able to start when the latest
version has no replicas yet. Exact-card actuation revalidation uses the
complete active fleet as transitional supply evidence, including healthy old
versions, while committed latest-version capacity remains latest-only. This
keeps the two concerns separate:

- old replicas may prove that an adopted accelerator target is attributable,
  and busy old replicas remain protected from drain;
- running work on every version is reported as warm retention but does not pin
  the private desired-card map to its serving card or create same-card
  replacement authority; compatibility demand ownership selects the cold card;
- old replicas never satisfy the latest-version launch target;
- the latest version emits the existing fenced logical scale target;
- on a fresh, complete, non-downscale mixed-version tick, the supply-aware
  compatible placement may move only its per-card explicitly owned subset,
  even when a non-retiring old replica still backs the prior card. The old row
  protects active work and fences nonpreemptive retirement until
  latest-version READY coverage exists; it never selects the paid replacement
  card. Synthesized default-all and aggregate padding own no paid placement;
  a backed remainder retains only same-card rollout authority;
- old-version provenance remains tri-state. A complete map permits units absent
  from every generation to move only toward explicit ownership and gives an
  unproven remainder no paid authority; unknown provenance preserves the
  adopted map and publishes explicit zero paid authority. Preempted and
  scale-down rows never count as backing;
- a downscale hold keeps its adopted exact-card retry assignment instead of
  applying mixed-version reassignment;
- the logical reconciliation target and paid cold-launch authority are
  separate fields. A retained target is not spending authority;
- the compatibility allocator returns separate full reconciliation,
  explicit-reassignment, and paid-ownership maps. Latest-only aggregate
  minimums and headerless queued demand may buy the selected cheapest card;
  inferred in-flight overflow and generic padding may not. A mixed-version
  cross-card move still requires the explicit-reassignment subset;
- logical READY/provisioning supply uses the same committed-capacity value as
  shortage suppression. A persistently degraded zero-capacity row cannot steer
  paid placement after its bounded replacement timeout, while the explicitly
  marked bounded replacement remains committed to prevent recursive waves;
- placement returns explicit typed funding provenance and planned capacity.
  The manager debits paid authority only for a `PAID` launch result and never
  infers the debit from an appended `ReplicaInfo`; `ZERO_COST` demand placement
  does not consume paid authority;
- unmaterialized broker-reported free slots never back demand actuation and
  never create paid cold-launch authority for their accelerator;
- reserved-fill decisions remain the only path that materializes those free
  slots, and retain their existing zero-cost-only placement fence;
- configured scale-up waves bound the adopted latest-version target when
  enabled, while `max_replicas` remains the demand-target ceiling;
- rolling logical fleets drain old replicas incrementally as ready latest
  capacity covers the same accelerator target, with busy or unknown-card
  replicas protected and physical retirements capped per tick;
- physical-backend and blue-green updates still wait for complete
  latest-version exact-card coverage before draining old replicas;
- reserved-fill launches remain bounded by broker grants, exact-card free
  supply, demand-reserved claims, and the aggregate hard ceiling.

Without complete-fleet revalidation, a service with 57 ready old L4 replicas,
zero latest replicas, demand target 57, `max_replicas: 64`, and seven free A100
reserved slots revokes its exact-card target and emits no scale-up. The broker
correctly assigns the seven slots, but physical rebalancing cannot begin. The
rolling-progress fix restores the ordinary latest-version L4 surge first; fill
then independently consumes eligible reserved A100 headroom without bypassing
demand safety. If a reported research A100 slot disappears before launch, its
fill attempt is skipped or fails at that pinned zero-cost location. It must not
be retried as a paid cloud A100 merely to preserve exact-card rollout coverage.

## Failure and rollback behavior

- If a preferred claim expires or its Kubernetes candidates are not
  launchable, the remaining claimants redistribute the capacity.
- If the shared pool observation fails, no new fill is fed. Existing holdings
  remain protected by the broker's blackout behavior.
- Roll back a borrower by removing its Kubernetes candidates and fill policy.
- Roll back Boltz by restoring its prior `reserved_capacity_fill: true`
  setting. No database migration or state rewrite is involved.

## Tests

The broker regression covers the production policy with total capacity 100:

- Boltz floor 10, weight 100;
- OpenDDE and Protenix floor 0, weight 1,000,000;
- expected entitlement 10 to Boltz and 45 to each preferred borrower;
- when OpenDDE is capped at 30, expected entitlement 10 to Boltz, 30 to
  OpenDDE, and 60 to Protenix;
- when both preferred borrowers are capped, their unused capacity flows back
  to Boltz.

The rolling-progress regression covers the production deadlock shape:

- 57 ready old-version L4 replicas and zero latest-version replicas;
- logical demand target 57 with `max_replicas: 64`;
- an exact-card catalog containing L4, A100, and A100-80GB;
- seven free reserved A100-family slots assigned by the broker;
- a latest-version fenced logical scale target is emitted;
- the demand target and paid cold-launch authority remain L4-only;
- A100-family launches, if any, are separate zero-cost-only fill decisions;
- fresh, complete, explicitly-proven non-downscale compatibility placement can
  move an adopted old-backed unit to the allocator-owned replacement card
  before all old rows disappear;
- busy old-version A100-family replicas remain retained until their work
  completes without requiring a latest-version A100-family replacement for
  L4-compatible work;
- no old replica drains before compatible latest-version ready coverage exists;
- partial latest-version L4 coverage retires only the matching amount of idle
  old-version L4 capacity instead of waiting for the complete L4 target;
- latest-version capacity on an incompatible different card cannot retire old
  L4-only capacity;
- fill cannot exceed the broker grant or aggregate ceiling while demand surge
  is pending.

The paid-authority regression additionally covers the production-shaped mixed
card case: old A100 rows back adopted units, current compatible ownership puts
their replacements on L4, and the first fresh tick emits only L4 paid
authority. After the L4 wave commits, a second tick progresses retirement of
idle old A100 rows rather than deadlocking on an empty authority map. Stale or
incomplete input emits explicit zero paid authority; a downscale-held retry
retains its adopted exact card; 40 old L4-only units remain L4. Manager tests
prove that only typed `PAID` results debit authority and typed `ZERO_COST`
results do not. A fresh report with running old A100 work but no accepted
compatibility history keeps A100 as the same-card replacement; only an explicit
flexible profile permits the cross-card L4 replacement.

The existing suite also covers identical multi-accelerator pool keys,
overlapping-but-nonidentical pool rejection, floor scaling, weighted splits,
effective caps, feed conservation, grant damping, claim expiry, and epoch
fencing. The broker policy itself needs no product-code change; the autoscaler
change restores rolling progress after the participating service configuration
creates a new version.

Run:

```bash
pytest -q tests/unit_tests/test_reserved_fill_broker.py
pytest -q tests/unit_tests/test_reserved_capacity_fill.py
pytest -q tests/unit_tests/test_concurrency_autoscaler.py
bash format.sh --files sky/serve/autoscalers.py \
  tests/unit_tests/test_reserved_fill_broker.py \
  tests/unit_tests/test_reserved_capacity_fill.py \
  tests/unit_tests/test_concurrency_autoscaler.py
git diff --check
```

## Manual verification

After deployment, verify:

- every participating claim publishes the same pool key for
  `prod_research_cluster_eks` with `a100` and `a100-80gb`;
- Boltz's grant does not fall below 10 when total brokered capacity and Boltz
  fill headroom are both at least 10;
- preferred borrowers split the realistic weighted remainder while they have
  fill headroom;
- total feeds never exceed observed free slots;
- service versions are elected and applied without request rejections;
- existing replicas continue returning HTTP 200 during the rolling update;
- no Kubernetes image, IAM, object-storage, readiness, or disk failures appear
  in borrower launch logs.

The initial live verification for OpenDDE observed fresh claims with weights
100 and 1,000,000, grants of 10 to Boltz and 5 to OpenDDE, and the one newly
free slot fed to OpenDDE. This confirms the broker preserved the floor and
applied the preferred weight before replica actuation.
