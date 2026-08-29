# SkyServe exact-card reservation-before-Spot production evidence

Observed: 2026-08-29 UTC

This bundle records the compact production evidence for the exact-card
compatible and statically disjoint `boltz-l4-fleet` edges. It does not claim
full flexible/mixed-card convergence. It intentionally contains no request
payloads, credentials, signed URLs, or raw logs.

## Qualified source and deployment

- Source: `boltz-bio/skypilot` `origin/improvements` commit
  `29f43f123e5903174493d7cb5150e93e9f33b359`.
- Closing changes: PR #1794 (exact-card reservation budgets), PR #1795
  (canonical accelerator identity), and PR #1796 (statically disjoint paid
  evidence).
- Release: `1.1.1565`, Helm revision 681, public API 93.
- Image:
  `sha256:970aede628a218d4553d422e491dd25eea042730c0e920973c0262d708d5298c`.
- Chart:
  `sha256:6c98751f4de1e1ae05199ec64dd634f014fc5e08060bb350808d960d335cae97`.
- Homogeneous roles: two Ready API Pods, two Ready controller Pods, and three
  Ready executor Pods on the exact image digest.
- `/api/health` reported version `1.1.1565`, API 93, build 9791, and exact
  commit `29f43f123e5903174493d7cb5150e93e9f33b359`.
- Helm `storage.enabled=false`; no SkyPilot PVC or EFS is present.

## Service identity and policy

- Service: `boltz-l4-fleet`, lifecycle 141, version 1.
- Service hash: `b519fa0f-37d9-4fee-9fd8-b575495ad88c`.
- Local pre-submit file SHA-256:
  `5e543c53d8295d9e71857899319b6efedff4c0b854a1c42a7b69742809a95d7a`.
- PostgreSQL `submitted_yaml_content` SHA-256:
  `edd774fea75ac822f5a51c10bd1f9e00f15368a592d5a10058813f4054fe5d06`.
- PostgreSQL rendered `yaml_content` SHA-256:
  `e9bc6a003a7a3659c5ffdfb2119daa7b93646ef8a9301d6a2b2cd9c567116eed`.
- The local input is sourced from provider-only `boltz-platform` commit
  `988931e2515a59c39d799eb80c66c1cb6d5784b7`; its checked-in service YAML and
  the local pre-submit file differ only by blank lines. That local commit
  touches only five files under `ml_models/providers/skypilot/boltz-2/` and is
  not represented as a remote platform-main merge.
- Policy: `min_replicas: 0`, fill floor 0, `utilization_gate: true`, paid cap
  100 physical GPU units, and Spot-only paid placement.
- Reserved cards: East A100/A100-80GB and PHX H200 through the existing
  scheduler configuration. Paid card for this proof: exact L4.

## Compatible reservation edge

The immediately preceding exact-card campaign on lifecycle 139 committed one
ordinary zero-cost admission for cluster
`boltz-l4-fleet-1-d878fbdc20`:

- authorization kind `ZERO_COST_ADMISSION`;
- no paid capacity pool key and no paid claim;
- Kubernetes context `prod_research_cluster_eks`;
- exact launched resource `A100:1`;
- one classified request and zero rejection;
- the replica reached `READY` and then entered normal zero-demand teardown.

This closes the compatible edge: a free exact A100 is committed before any
paid residual.

## Statically disjoint Spot edge

Run `qual-20260829T033859Z-6476e4cefe4d` submitted one authenticated exact-L4
request while the immutable worker projection contained only A100,
A100-80GB, and H200 reserved cards.

- PostgreSQL committed one exact L4 paid claim at priority 20, capacity-plan
  generation 742, demand generation 1834.
- Selected pool: AWS account `096766144388`, workspace `mt_hybrid`,
  `eu-south-2b`, `g6.2xlarge`, one L4, Spot.
- Replica association:
  `5e937c99-b421-500d-a730-b0f2b2421963`; cluster
  `boltz-l4-fleet-1-7f7c28867f`.
- EC2 instance `i-0a8d91515fc10f2ae` launched at 03:40:09 UTC and SkyServe
  reached `READY` in about three and a half minutes from provider launch.
- The client observed acknowledgement, verified the completion marker and
  result, reported completion, and returned `qualification_passed`.
- Marker digest:
  `sha256:c1de88d303ee4f58c41676c0cc3f44875c1631c703c683cac3d4ba3a1b82f8d7`.
- Result digest:
  `sha256:f59c730b40d0567ec2832196a0be6f7e6fa8414297b90f55afb81a94238ff317`.
- No ordinary on-demand or wrong-shape instance appeared. The launch-time
  Spot market price was $0.1402/hour; the larger request price was only the
  Spot ceiling.

This closes the disjoint edge: incompatible reserved supply contributes zero
commitment and cannot suppress a genuine exact-L4 Spot residual.

## Demand-driven drain and billing closure

- The controller began normal down at 03:46:33 UTC after demand returned to
  zero, removed the replica normally, and removed the paid claim. AWS records
  user-initiated termination at 03:46:57 UTC.
- EC2 instance `i-0a8d91515fc10f2ae` reached `terminated` at 03:52:37 UTC.
- One-time Spot request `sir-7nvfhtrk` closed with
  `instance-terminated-by-user` at 03:53:22 UTC.
- Delete-on-termination root volume `vol-05c66f5f69916044a` returned
  `InvalidVolume.NotFound`.
- PostgreSQL ended with zero live replicas and zero paid claims for the
  service. The service remained healthy in `NO_REPLICA` state.
- The three exact qualification scratch objects were deleted after the marker
  and result were verified.

At the +10-minute guard sample (04:02:14 UTC), service status remained
`NO_REPLICA` with zero replicas, paid claims, paid waiters, and zero-cost
actuation intents. EC2 still reported `terminated`, the Spot request remained
`closed`, and the volume remained absent.

This +10 sample does not by itself satisfy the design's separate +30 and full
stale/quiescence-horizon acceptance gate.

At the +30 sample (PostgreSQL time 04:24:37.712402 UTC), lifecycle 141 remained
`NO_REPLICA` with empty replica, paid-claim, paid-waiter, and zero-cost-intent
sets. Fresh capacity-plan generation 1238 targeted zero for A100, A100-80GB,
H200, and L4 and carried no paid launch authority. Demand generation 2928 and
both authoritative HA reporters were fresh and complete with zero queue,
in-flight work, and 60/300-second arrivals; one older reporter was expired
historical state and had no authority. AWS still reported the exact instance
terminated with no attached volume, the Spot request closed, and the root
volume absent.

The +30 sample is 22 minutes 23 seconds after the retained +10 exact-zero
sample. That exceeds the live/source 20-second API-instance stale interval,
70-second controller cutover quiescence interval, and 180-second reserved-pool
observation horizon. The latest predecessor `1.1.1564` heartbeat was at
03:34:54.737330 UTC; only current `1.1.1565` leases were fresh at the final
sample. This closes the configured stale/quiescence drain horizon as well as
the wall-clock +30 check.

No VM, active Spot request, root volume, replica, or paid claim remains as a
billing or launch authority.

## Idle telemetry evidence

After drain, `/serve/boltz-l4-fleet/demand` read directly from PostgreSQL and
reported two fresh, complete HA reporters with exact values:

- queue depth 0;
- async processing 0;
- HTTP in flight 0;
- total in flight 0;
- recent rejections 0.

Request history retained one classified successful request. The qualification
client did not create a protocol-covered async-ledger row, so a live nonzero
exact terminal-ledger/UI capture remains a full-design acceptance gate. It is
not placement or provider-billing authority.

The deployed dashboard bundle contains the separate `Async processing now`,
total-in-flight, queued, and exact terminal fields introduced by PR #1783; it
does not depend on contacting the service controller for the demand summary.

## Scale evidence reused by this qualification

The final correction did not change provider launch concurrency or teardown
machinery. Existing production evidence therefore remains applicable:

- one atomic wave reached 100 concurrently provider-`RUNNING` one-L4 GCP Spot
  VMs in 3 minutes 41.9 seconds and peaked at 117;
- lifecycle 137 reached exactly 100 GCP Spot L4 VMs, zero on-demand and zero
  wrong-shape instances, served 10,000/10,000 authenticated warm requests at
  first-attempt HTTP 200, and returned PostgreSQL/provider/disk state to zero;
- a separate mixed census proved 44 reserved plus 28 AWS Spot replicas Ready
  and serving concurrently, with zero on-demand.

## Scope audit

The closing source, service recreation, Helm deployment, and qualification
made no Kueue policy, Terraform, Terragrunt, KubeRay, IAM, EFS/PVC, or
`boltz-platform` application-code change. The local provider integration is
the only platform-owned runtime surface used by the qualification.

## External catalog gate

AWS Pricing and the hosted catalog leave four commercial G6/L4 regions absent:
Zurich, Sao Paulo, Hyderabad, and Malaysia. Zurich (`eu-central-2`) is the only
qualified candidate: it has a ready source patch, a compatible curated image,
and a real Spot launch/driver/workdir/teardown proof. Source support is not yet
merged or released. Draft catalog PR #191 is rebased onto catalog master
`69166fce3ece5b9dffe639d3e9ceca2ee1f89fa1` and contains exactly 1,127 Zurich
additions and no deletions. Its candidate VM catalog SHA-256 is
`2e0ca474d692a484ba60e39af45d62babd5492376394bb732ea7e9a5d2b5614b`.

Activation remains externally gated on upstream source PR #10587 approval,
merge, and release plus publisher-account Zurich opt-in attestation. The
catalog must remain draft and unpublished until those conditions hold; no
other missing commercial G6/L4 region currently passes all three evidence
gates. Sao Paulo has no compatible curated image or launch proof, Hyderabad
has images but no available opted-in account or launch proof, and Malaysia has
neither an image nor an opted-in account/launch proof. GovCloud remains outside
the commercial catalog scope and lacks the same complete evidence chain.
