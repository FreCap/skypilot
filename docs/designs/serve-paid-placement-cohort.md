# SkyServe Bounded Paid Placement Cohorts

_Created: 2026-07-22_

## Problem

SkyServe logical autoscaling can persist an entire missing-capacity wave before
any provider launch returns. Dynamic fallback placement deliberately selects
the cheapest active location for every paid launch until provider feedback
benches that exact location. A large target gap can therefore create hundreds
of PENDING replicas pinned to one unverified Spot zone.

Launch admission later starts only a bounded subset of those rows. When one
admitted launch proves the zone unavailable, the placer benches it and refresh
processing deletes every never-started PENDING sibling pinned there. The next
autoscaler tick sees the logical gap again and repeats the wave at another
location. READY capacity can remain stable while the dashboard's combined
PENDING, PROVISIONING, and STARTING count repeatedly grows and collapses.

A live 1,000-slot service exposed the failure mode with roughly 500 READY slots
and repeated approximately 500-slot provisioning waves. The behavior wastes
controller work and delays convergence even though most deleted rows never
reach a provider allocation attempt.

## Goals

Fresh paid demand placement must keep cheapest-first economics while bounding
how many unresolved launches one exact location can accumulate before provider
feedback exists. Once that bounded cohort is full, placement must spill to the
next-cheapest eligible active location. If every eligible paid location is
full, scale-up must make no durable progress and retry from current state on a
later reconciliation.

The bound must survive controller restart without new persistent state, remain
correct for exact-card targets, and avoid changing zero-cost capacity,
cost-rebalance, or recovery-re-drive semantics.

## Background

`SkyPilotReplicaManager._scale_up_to_logical_capacity_locked()` counts every
nonterminal PENDING row as committed logical capacity and loops until the full
target gap is persisted. `_launch_replica()` asks the spot placer for a location
before it writes each row. `DynamicFallbackSpotPlacer.select_next_location()`
selects the cheapest active candidate and has no in-wave provider feedback.
`_refresh_thread_pool()` later admits PENDING rows under the global launch
budget and invalidates never-started rows when their exact location is benched.

The dashboard's `Provisioning` history bucket contains PENDING, PROVISIONING,
and STARTING rows. A large cliff therefore primarily reports invalidated
logical launch intent, not an equivalent number of terminated READY machines.

Earlier commits `797335e6e4` and `801195b0f9` accumulated in-wave locations for
least-loaded placement. Commit `2950107ce3` intentionally removed that policy
because load spreading moved launches away from cheaper active capacity and
complicated failure fencing. This design does not restore least-loaded
placement.

The zero-cost path already uses small speculative allowances during capacity
measurement blackouts. That establishes the feedback-window pattern, but its
pool-level GPU accounting remains distinct from the exact-location paid bound.

## Behavior Contract

The paid placement cohort applies only to fresh demand launches selected by a
spot placer. It does not constrain:

- zero-cost-only reserved-capacity fill;
- recovery re-drives with an immutable persisted location;
- cost-rebalance replacements with an already selected location; or
- services without a spot placer.

An unresolved paid launch is a nonterminal replica at an active non-zero-cost
location with status PENDING or PROVISIONING. STARTING, READY, and NOT_READY
replicas prove that provider capacity was acquired and do not consume the
speculative window. Application setup and readiness duration are not provider
location-capacity feedback, and must not push subsequent demand to a more
expensive location after `sky.launch` succeeds. Terminal and teardown rows do
not consume the window. Legacy locations are resolved through the placer's
existing location-resolution contract before they are counted.

A recovery-pinned paid row that is still PENDING or PROVISIONING consumes the
same allowance as a fresh row. This is intentionally conservative: fresh
demand cannot speculate past unresolved durable work merely because that work
was reconstructed by a successor controller.

Each exact paid `Location` has an independent cohort allowance. Exact identity
uses the placer's location keys, including cloud, region, zone, accelerator
shape, Spot mode, and other launch-defining fields. The default allowance is
four, matching the existing small-probe convention. Operators may override it
with the positive-integer environment variable
`SKYPILOT_SERVE_PAID_LOCATION_LAUNCH_WINDOW`. Invalid or non-positive values
log a warning once per distinct value and fall back to four.

At the start of a physical or logical scale-up wave, the manager reconstructs
remaining allowances from the existing durable replica snapshot. Every paid
row persisted in the same wave debits its selected location immediately. No new
table, column, or process-local authority is introduced.

Location selection retains all eligible zero-cost candidates and only those
eligible paid candidates whose remaining allowance is positive. The existing
placer then applies zero-cost preference, exact-card restriction, active bench
state, and cheapest-first ordering. A successfully persisted paid row debits the
wave-local allowance before the next selection in the manager-locked wave.
Selection or fencing failures that write no row consume no allowance. Selecting
zero-cost capacity does not touch the paid allowance.

Unsatisfiable and saturated selections remain distinct. The manager first
computes the existing active location set for an exact accelerator override. If
that set is empty, it raises the existing configuration error. Only after that
validation does it filter paid candidates by remaining cohort allowance.

When otherwise-valid candidates are empty only because their paid cohorts are
full, the launch helper returns without allocating a replica ID, writing a row,
or registering a launch thread. Logical exact-card reconciliation marks that
card deferred for the current tick and continues with other deficient card
targets. It stops when every deficient card is satisfied or deferred. A generic
logical target stops after the first no-progress result. Physical batches may
continue to later overrides so one saturated card does not block an independent
card. Budget saturation must never convert a permanent exact-card
misconfiguration into silent retry.

On a later reconciliation, READY or terminal transitions naturally release
allowance when the durable snapshot is rebuilt. One success frees one cohort
slot; it does not permanently disable the bound for that location. Existing
benching, selection-time fencing, failure-wins aggregation, and queued-sibling
invalidation remain unchanged.

## Implementation

`sky/serve/replica_managers.py` will add a small wave-local paid-location
budget. Its builder reads the already-required replica snapshot and active
placer locations once, counts unresolved statuses, and returns remaining
allowance by resolved paid location. The selection helper intersects that map
with any exact accelerator override before delegating cost ordering to the
spot placer.

Both physical batch scale-up and logical scale-up pass one mutable budget
through the wave. The single-replica scale-up path builds the same budget from
durable state. Recovery and pinned-replacement call paths bypass it.

No change is required in `sky/serve/spot_placer.py`: the existing
`allowed_locations` interface already supports a caller-provided eligible set
without changing cost ordering or bench semantics.

## Alternatives Considered

Restoring least-loaded placement would distribute a wave, but it would undo the
explicit cheapest-first economic contract and revive snapshot and fencing
complexity removed by commit `2950107ce3`.

Persisting only as many total rows as the global launch-admission budget would
bound the graph but couple one service's placement behavior to controller
memory sizing and the number of configured services. It would also prevent
independent locations from receiving parallel capacity feedback.

Deriving the cohort as a share of the global budget has the same coupling and
can change one service's placement behavior when unrelated services are added.
A small fixed per-location default provides a stable feedback bound; the
environment override preserves operational tuning without making the global
pool authoritative for exact-location risk.

Keeping every queued sibling after a bench would avoid the visual cliff but
would admit stale, known-bad pins or require in-place row relocation. The
existing invalidation and immutable placement identity are safer and remain.

## Changed-Path-to-Test Matrix

| Changed production path or invariant | Test path | Expected proof |
| --- | --- | --- |
| Paid budget reconstruction counts PENDING and PROVISIONING by resolved exact location, but excludes STARTING, READY, NOT_READY, terminal, zero-cost, and benched locations | `tests/unit_tests/test_serve_replica_managers.py` | Remaining allowance is rebuilt correctly from durable rows after restart and provider-success feedback releases capacity before application readiness |
| Cheapest-first paid placement consumes at most the configured cohort at one exact location before spilling | `tests/unit_tests/test_serve_replica_managers.py` | A multi-location wave assigns the first cohort to the cheapest location and the next cohort to the next-cheapest location |
| All eligible paid locations at their allowance produce no row, ID increment, or launch thread | `tests/unit_tests/test_serve_replica_managers.py` | Selection returns no progress and leaves durable and process-local launch state unchanged |
| An exact accelerator override with no active matching location remains an error, while a matching but cohort-saturated location defers | `tests/unit_tests/test_serve_replica_managers.py` | Permanent configuration errors cannot become silent retries and transient saturation cannot crash a wave |
| Exact-card targets have independent eligibility and a saturated first card does not starve a later deficient card | `tests/unit_tests/test_serve_replica_managers.py` | Logical reconciliation continues across card targets and preserves exact accelerator shapes |
| Zero-cost selection, recovery pins, and cost-rebalance pins bypass the paid cohort | Existing reserved-capacity and recovery suites plus focused regressions | Existing contracts and call shapes remain unchanged |
| Environment override accepts positive integers and falls back safely for invalid values | `tests/unit_tests/test_serve_replica_managers.py` | Default, override, and invalid-input behavior are deterministic |
| Python style, typing, lint, and diff integrity | `format.sh`, focused pytest, mypy, pylint, and `git diff --check` | Changed paths satisfy repository gates |

## Manual Test Plan

On a staging service with at least two paid Spot locations, create a target gap
larger than twice the configured cohort and keep request demand above the
target. Confirm controller logs and replica rows show no more than the cohort
allowance in PENDING or PROVISIONING at one exact unverified location. Confirm
overflow uses the next-cheapest active location, a capacity failure still
benches and invalidates only its remaining PENDING siblings, and the next tick
reconstructs allowances from durable state.

Repeat with an exact-card target and with a mixed zero-cost plus paid service.
The exact-card service must not place on another accelerator, and zero-cost
capacity must retain its existing pool budget and preference behavior.

## Rollout and Rollback

The default activates on controller upgrade with no migration. The change is
fail-closed on speculative paid placement: an unexpected accounting problem
can delay new rows until the next reconciliation but cannot relocate a live or
recovered replica. Operators can raise the positive-integer window if a provider
reliably supports more parallel feedback.

Rollback is a controller image rollback. Existing rows remain valid because no
schema or persisted representation changes. Provider-side instance and billing
observability remains separate from logical-row history and should be checked
when evaluating production cost impact.
