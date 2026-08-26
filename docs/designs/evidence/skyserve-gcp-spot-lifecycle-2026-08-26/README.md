# SkyServe GCP Spot lifecycle qualification

Status: **complete**

Run date: 2026-08-26 UTC

Run ID: `2ca8e9a0-4a59-4acf-a0cb-f9ea4291d6f3`

Service incarnation: `d4ccc0a5-f2b4-4b72-bc01-079c979fd5d6`

## Scope

This bundle records the disposable paid-provider lifecycle gate for
`boltz-l4-fleet`. Acceptance required at least 100 physical GCP Spot VMs
concurrently `RUNNING`, with a hard cap of 120. Every VM had to be
`g2-standard-4` with exactly one L4, and ordinary On-Demand capacity had to
remain zero. Completion required normal SkyServe teardown to exact zero in
both PostgreSQL and the provider-native GCP inventory.

This run does not qualify reserved/Kueue placement, production model serving,
or the separate 10,000-terminal-request ledger.

## Immutable release

- PR: [#1744](https://github.com/boltz-bio/skypilot/pull/1744)
- Merge: `329f6f5a33bab85401fef59b023714b47fb1d5eb`
- Release/chart: `1.1.1513`
- Image:
  `sha256:837be5d44a58e167fd7aaa906d65c2681f6e5c6bbefd54d76cd0bf6ba24dfec1`
- Chart:
  `sha256:d84303e4eab868127949d068f45e93f87ea800c214af0be5445021b44f38d4bb`
- Helm revision 634 ran the qualification. Revision 635 retained the same
  release and restored the temporary service launch window to 100 and the
  successful-location TTL to 600 seconds.

All API, controller, and executor replicas used the exact image. Central state
remained PostgreSQL-only: `storage.enabled=false`, no PVC, and no EFS.

## Envelope

- GCP project: `boltz-498512`
- Market: Spot only
- Shape: `g2-standard-4`, one NVIDIA L4, one 50-GiB managed boot disk
- Service floor/ceiling and live paid cap: 120
- External guard cap: 120 VMs, GPUs, and disks
- Synthetic lifecycle stimulus: 10,000 stable IDs at concurrency 256
- No user or researcher traffic; only the bounded synthetic lifecycle stimulus

The service specification deliberately contained no Kubernetes/reserved,
AWS, On-Demand, Terraform, Kueue, or `boltz-platform` activation path.

## Timeline and result

| Event | UTC | Result |
|---|---|---|
| Fixed-120 update completed | 18:23:39.277 | Version 2 applied |
| Atomic paid wave committed | 18:25:12.183 | 120 exact replica/debit rows |
| First provider sample at acceptance | 18:28:54.100 | 100 `RUNNING`, 107 Spot total |
| First peak sample | 18:29:25.311 | 117/117 `RUNNING` |
| Normal down request completed | 18:30:04.135 | Controller-owned teardown |
| Provider `RUNNING=0` | 18:35:00.512 | 28 remaining objects were `STOPPING` |
| Exact all-state zero | 18:35:39.315 | Service/DB/VM/disk state absent |
| Last retained zero sample | 18:36:57.577 | Third consecutive exact-zero sample |

The first acceptance sample arrived 3 minutes 41.9 seconds after the atomic
commit and 5 minutes 14.8 seconds after the update. Fresh samples then observed
107, 110, 114, and 117 concurrently `RUNNING`. At peak, 80 VMs were in
`asia-northeast3` and 37 in `asia-south1`. Across all 98 guard samples there
was no On-Demand/non-Spot capacity and no invalid accelerator, machine, disk,
debit, or service-incarnation observation.

Normal teardown reached provider running-zero 4 minutes 56.4 seconds after
the down request completed and exact all-state zero after 5 minutes 35.2
seconds. No direct provider delete, database-row delete, or executor restart
was used. Independent PostgreSQL and GCP all-state censuses agreed with the
three final guard samples. A final census after Helm revision 635 again found
zero service rows, replicas, claims, waiters, associations, queue rows, pins,
cluster bookkeeping, GCP VMs, and attributable disks.

## Evidence integrity

The bounded operator artifacts had these SHA-256 digests:

- scale-zero service YAML:
  `ec1e4a1eaba6ce2c450e8501c2201845e5c3929291f99150ef5f4a021a9e94b6`
- fixed-120 service YAML:
  `a8e15e9ecc97929c7b28536de24814890fc749e16e5c45cbeca7b485455497fd`
- arm envelope:
  `763db1a93d800954470a79b37302f74c43f19efbcaf7980ad5758d4aeb178985`
- 98-sample PostgreSQL plus GCP-native audit:
  `940b7f1c739ac48549d569249b29b9adf7625a3823a71b6351fe1ead64b27065`
- final guard receipt:
  `e5befe4258ab22494a2aa525b3ce2af8ed718f7c02d1dbf503a2514d4504c388`

Raw provider object names and full minute-by-minute snapshots are intentionally
not checked into the design tree.

## Follow-up observation

The concurrent traffic writer changed prospective demand semantics five times
before the sixth fixed-floor transaction committed. For future fixed-floor
lifecycle qualifications, commit the floor before starting that writer.
Longer term, investigate a complete decision-output fingerprint containing the
effective target, exact-card allocation, compatibility, launch priority, and
waiter-fairness result. A telemetry change may avoid invalidating the wave only
when every output is unchanged; any priority or fairness change must reject.
Fresh reporter, route, supply, cap, ownership, and every decision-changing
demand transition remain fail-closed.
