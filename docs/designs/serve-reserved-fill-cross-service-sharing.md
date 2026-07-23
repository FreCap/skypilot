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

- old replicas may prove that an adopted accelerator target is attributable;
- old replicas never satisfy the latest-version launch target;
- the latest version emits the existing fenced logical scale target;
- configured scale-up waves bound the adopted latest-version target when
  enabled, while `max_replicas` remains the demand-target ceiling;
- old replicas drain only after ready latest capacity covers them, with busy
  replicas protected and logical retirements capped per tick;
- reserved-fill launches remain bounded by broker grants, exact-card free
  supply, demand-reserved claims, and the aggregate hard ceiling.

Without complete-fleet revalidation, a service with 57 ready old L4 replicas,
zero latest replicas, demand target 57, `max_replicas: 64`, and seven free A100
reserved slots revokes its exact-card target and emits no scale-up. The broker
correctly assigns the seven slots, but physical rebalancing cannot begin. The
rolling-progress fix restores the ordinary latest-version surge first; fill
then consumes eligible reserved headroom without bypassing demand safety.

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
- no old replica drains before latest-version ready coverage exists;
- fill cannot exceed the broker grant or aggregate ceiling while demand surge
  is pending.

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
