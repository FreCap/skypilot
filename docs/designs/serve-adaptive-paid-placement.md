# SkyServe Adaptive Paid Placement

_Created: 2026-08-02_
_Last updated: 2026-08-28_

## Status

Proposed. This design extends the deployed global paid-capacity authority in
`serve-paid-placement-cohort.md`. The existing exact-pool depth ladder,
per-service envelope, per-card exploration frontier, request-priority protocol,
and PostgreSQL outcome transaction remain authoritative.

The per-service envelope in this document applies to non-planner callers.
Planner-bound protocol-v2 admission is superseded by
`serve-multi-pool-reserved-capacity-fill.md`: its committed paid launch target
is the sole aggregate authority, while the exact-pool depth, priority,
Spot-only, price-order, stale-plan, and hard-cap fences remain shared. Thus the
16-claim cold envelope below must not serialize a larger immutable planner
cohort.

The atomic paid-batch amendment in the parent design changes transaction
cardinality, not adaptive policy. One transaction may commit several accepted
members, but it evaluates the same cold/effective service limit, per-card
frontier, exact-pool depth, price order, and request priority once under the
service and sorted pool locks. The default maximum remains 16. A larger
qualification batch requires an explicit isolated operator profile, effective
service limit, summed pool headroom, frontier, process/global launch cap, and
aggregate long-worker capacity of at least the requested width; batching alone
never widens a service or pool.

The process/global launch cap in that qualification is enforced only by the
existing later P-reservation transaction. Phase A may use its current snapshot
to bound preparation, but it does not reserve P and may commit more
`SCHEDULED` rows than Phase B can start immediately.

The rollout is intentionally ordered. Milestone 1 makes genuine target-backed
launches react to durable provider feedback faster and lets proven pools widen
one service's unresolved-claim envelope within an operator cap. Milestone 2
activates the already-implemented delayed third failure domain. Milestone 3
adds shadow-only speculative decisions and accounting. Milestone 4 may launch
bounded speculative duplicates only after the cost and cleanup gates in this
document are proven.

## Motivation and evidence

Production launch history for the fixed 24-hour interval
`[2026-08-01T12:27:48Z, 2026-08-02T12:27:48Z)` contained 436 successful and 572
typed capacity-failed AWS/GCP paid placement outcomes. A conservative replay
used the clean subset whose lifecycle rows survived long enough to join to the
end-to-end `sky.launch` request duration: 27 successes and 61 typed failures.
In that subset, typed failures returned in roughly 10 seconds on AWS and 30
seconds on GCP, while successful launches commonly took 70 to 115 seconds.

The deployed policy admits at most 16 unresolved paid claims for one service.
After a launch worker finishes, the replica-manager refresher can wait up to 20
seconds to observe it, and the autoscaler can then wait another decision
interval before replacing a failed claim. A fixed two-request hedge produces
less than one expected success at the observed 43.3 percent aggregate success
rate and adds little throughput to a 50- or 100-machine wave.

Paired replay is directional rather than a capacity forecast. It assumes
independent attempts and therefore overstates same-region gains during a
correlated shortage. It predicts that widening a proven, target-backed service
window from 16 toward 24 matters much more than two fixed duplicates for large
waves. It also predicts that publishing typed failure immediately to the next
reconciliation removes a material controller-delay wave. Production canary
measurements, not the replay, decide each promotion.

## Goals

1. Preserve target fidelity: normal placement never creates more committed
   capacity than the autoscaler's current target.
2. Preserve the cold-start bound: an unproven service still has a 16-claim
   unresolved paid envelope and two cold pools of depth four.
3. Use recent durable success to increase target-backed launch throughput, with
   a default-off operator ceiling and no schema change.
4. Reconcile immediately after a durable typed capacity or quota outcome,
   without shortening every controller polling loop.
5. Use distinct provider-region failure domains before deep regional fan-out,
   then deepen productive pools instead of continually opening new regions.
6. If speculative duplicates are later enabled, keep their realized waste plus
   active reservations below 3 percent of attributable non-speculative service
   compute, fail closed on uncertain accounting, and clean up every loser across
   restart or ownership handoff.
7. Keep rollout and rollback independently controllable by environment flags.

## Non-goals

Milestones 1 and 2 do not overshoot an autoscaler target, change provider price
ordering, revoke unresolved claims, or infer capacity from error text. They do
not reduce global API-server launch-worker limits or bypass the existing final
launch fences.

This design does not claim that catalog estimates equal the provider invoice.
Speculative launch actuation stays disabled until an attributable billing
source and conservative reservation price are available. A 2.5 percent
authorization ledger is a guard band for a hard 3 percent measured ceiling,
not evidence by itself that the invoice stayed under that ceiling.

## Behavior contract

### 1. Evidence-aware target-backed service window

The existing
`SKYPILOT_SERVE_PAID_SERVICE_LAUNCH_WINDOW` remains the cold floor and defaults
to 16. A new
`SKYPILOT_SERVE_PAID_SERVICE_MAX_LAUNCH_WINDOW` is the rollout ceiling and also
defaults to 16, so new code is behaviorally disabled until configured. A
versioned JSON
`SKYPILOT_SERVE_PAID_SERVICE_LAUNCH_WINDOW_PROFILES` document may override the
ceiling and the delayed maximum exploration frontier for an exact
`(workspace, service_name, service_hash)` incarnation. The frontier field is
optional, so a Milestone 1 profile does not change placement breadth. An
invalid document or a profile that does not match all three fields grants no
override. This makes both the first 24-claim canary and the later third-frontier
canary specific to one incarnation instead of silently widening every
controller in the API-server Pod.

Dedicated Serve controllers receive both that exact profile document and the
global maximum exploration frontier from their API-server launch environment.
This preserves the rollout baseline of two pools for every unmatched service;
the code default of three cannot silently widen a remote controller.

For a PostgreSQL-backed service, one launch-budget snapshot computes:

```text
productive_limit = sum(
    admission_limit for each distinct eligible exact pool in the
    card's bounded productive frontier
    whose admission_state is active and whose unexpired last_success_at exists)

effective_service_limit = min(
    max(configured_cold_service_limit, configured_max_service_limit),
    max(configured_cold_service_limit, productive_limit))
```

For every accelerator card requested by the current batch, the productive
frontier contains existing service-owned pools first and then the cheapest
eligible pools, up to that card's effective exploration frontier. This matters
because a pool commonly releases its last claim in the same transaction that
raises its learned depth; requiring a still-unresolved service claim would make
the positive evidence disappear exactly when it became useful. The shared
exact-pool authority deliberately lets one service reuse another service's
recent genuine success in the same provider pool.

Unknown pool identities do not contribute positive evidence. Cooldown and
probe pools do not contribute. Expired success does not contribute. Duplicate
catalog aliases for one exact pool contribute once. Pools outside the current
batch's accelerator cards or bounded frontier do not contribute. The effective
limit can therefore increase only from PostgreSQL-backed launch success already
recognized by the existing pool authority, and a large catalog cannot widen the
service merely by containing many old successful regions.

The launch budget records the effective limit and passes that exact value into
the atomic batch transaction. The advisory snapshot is not authority. The
service-row lock recounts valid claims, adds the accepted policy-valid subset,
and clips or rejects members at the effective limit. It never admits every
prepared member merely because the caller requested a large wave. If a
controller restarts, it recomputes the same bound from durable claims and pool
evidence. Existing over-limit claims are retained and new claims stop until
they drain below the recomputed limit.

The autoscaler and logical target fence remain the source of demand. Raising
the envelope creates no additional scaling decision; it only lets more already
requested work be persisted before the next provider result. The global
launch/down worker admission budget still paces actual API mutations across
services.

The first production ceiling is 24. A ceiling of 32 or 64 requires a separate
promotion using controller RSS, API request queue age, provider throttling, and
cross-service latency evidence. At the current launch worker estimate, eight
additional live workers can consume roughly 1.6 to 2.4 GiB, so queued claims
and live workers must be reported separately.

The canary must also set the global Serve worker ceiling to at least 32 while
the API server continues guaranteeing at least that many long workers. The
24-claim incarnation profile then prevents the canary from consuming the last
eight global slots, while ordinary services remain at their 16-claim floor. A
24-claim service ceiling under a 16-worker global ceiling only prequeues work
and does not test the intended provider concurrency. A 24-worker global ceiling
would let the canary occupy every slot and is not an acceptable fairness gate.

### 2. Durable feedback wakeup

Each local launch worker writes its replica id to a process-local completion
queue and sets a coalescing event in a `finally` block when it exits. The
replica-manager refresher clears the event before draining the queue, joins each
notified worker before consulting `is_alive()`, and then waits for either the
event or the existing 20-second timeout. The queue preserves every completion
across event coalescing and clear races. Both signals are advisory and the
timeout remains the recovery fallback.

The refresher classifies the result and commits the existing batch outcome
transaction. Only after a transaction containing a typed capacity or quota
failure for an exact paid pool commits does it set a separate coalescing
autoscaler event. The autoscaler clears that event immediately before reading
durable state, then waits for either a later event or its ordinary decision
interval instead of sleeping unconditionally. Feedback that arrived before the
clear is consumed by that durable read; feedback arriving during the tick stays
set and makes the wait return immediately. Generic failures, unknown pool
identities, and pre-commit worker signals do not authorize failure-driven
replacement.

The ordering is:

```text
provider result
  -> local worker completion signal
  -> manager refresh under the existing manager lock
  -> atomic outcome commit closes exact pool and releases claim
  -> autoscaler wake signal
  -> fresh target calculation and ordinary fenced placement
```

Events carry no correctness state and may be lost on process death. Durable
PostgreSQL rows plus the existing periodic loops recover all missed work. Event
coalescing may combine a wave into one reconciliation. It must not create one
autoscaler run per failed replica.

### 3. Failure-domain progression

The normal frontier remains two pools per exact accelerator card. The delayed
maximum remains separately configured. Production first sets the exact canary
profile's maximum from two to three, retaining the process-global maximum of
two, the 30-second feedback delay, and the requirement that owned pools have no
headroom. A new pool is preferred only in a provider-region domain not already
represented by the owned frontier. The effective maximum is recomputed from the
exact service-incarnation profile in the same budget snapshot whose value is
passed to the atomic frontier claim.

No fourth pool is enabled in the first production rollout. Code for a fourth
domain may be added only with a durable residual-shortfall input, a minimum
residual of 48 single-slot machines or an equivalent exact-card capacity, and a
second feedback epoch after the third pool was claimed. A process-local count
or the age of an older primary is insufficient authority.

### 4. Speculative episode contract

A speculative claim is a duplicate launch not counted toward committed target
coverage until it wins. It is never represented as an ordinary target-backed
claim with an implicit convention. Before actuation, PostgreSQL must persist a
speculation episode containing at least:

- service name and incarnation hash;
- exact target generation and accelerator card;
- candidate replica ids and exact pool identities;
- state: `PLANNED`, `LAUNCHING`, `WON`, `CANCELLING`, `CLOSED`, or
  `ACCOUNTING_UNCERTAIN`;
- candidate-specific reservation amount and price source;
- winner, loser cleanup ownership, and timestamps;
- realized speculative waste and the billing reconciliation watermark.

At most one transaction can promote a successful candidate to winner for an
episode. A winner becomes ordinary committed capacity before any target-backed
replacement is suppressed. Every other candidate becomes a durable loser and
must be cancelled or terminated by the existing idempotent provider cleanup
path. Unknown request state, failed cancellation transport, controller restart,
or owner handoff retains cleanup ownership and blocks more speculation. A late
second success is excess capacity and stays off route until teardown is proven.

The raw episode size is:

```text
Hraw = min(shortfall, 4 * min(3, ceil(shortfall / 32)))
```

This yields at most 4, 8, and 12 candidates for the canary tiers. A candidate
must also have an independent eligible provider-region slot and pass the cost
ledger. Speculation is evaluated after 120 seconds of unresolved shortfall or
an explicit untyped retry condition, not at target publication time. Typed
capacity feedback continues through normal target-backed replacement and does
not by itself authorize a duplicate.

For candidate reservation `R`, accrued attributable non-speculative compute
`B`, realized speculative waste plus active reservations `S`, and authorization
rate 2.5 percent:

```text
allowed_candidates = floor((0.025 * B - S) / R)
H = min(Hraw, independent_slots, max(0, allowed_candidates))
```

`R` is at least five minutes at the greater of the candidate-specific p90
observed rate and a configured catalog ceiling. `B` excludes rejected requests,
unaccepted capacity, and speculative machines. A service with no accrued
non-speculative compute has zero speculative allowance. Missing, stale, or
ambiguous price or billing attribution moves the episode or ledger to
`ACCOUNTING_UNCERTAIN` and authorizes zero new candidates.

Shadow mode persists or reports the decision inputs but launches nothing. It is
the default for Milestone 3. Actuation begins with `H <= 4` for one isolated
service. Caps 8 and 12 are separate promotions.

The current replica manager marks its direct launch/down thread pools as a
deprecated mutation owner while resource actions are introduced. Milestone 3
must integrate episode actuation and cleanup with the authoritative durable
resource-action owner when that path is eligible. It must not add a second
long-lived cleanup protocol to the deprecated pool merely because that path is
convenient during design.

## Compatibility and failure handling

The adaptive target-backed window and feedback events require no schema change.
Old controllers continue enforcing 16. A current service has one fenced owner,
so a rolling binary transition cannot concurrently widen one service from an
old and new controller. The same service-owner fence prevents a singleton and
batch writer from interleaving fresh admission for that service. The batch adds
no row or request shape, so an image rollback is operational and may return
fresh admission to singleton speed while existing replica+claim pairs continue
through ordinary reservation, binding, serving, and cleanup. PostgreSQL
service-row and sorted exact-pool locks remain the authority across services.

Local SQLite retains the legacy local paid window and does not use adaptive
service limits or speculative accounting. This follows the central database
policy: any speculation tables and migrations target PostgreSQL only.

If the event-driven path raises, the supervised manager and autoscaler loops
retain their periodic fallback. If dynamic limit parsing is invalid, both the
floor and maximum fall back to 16. If maximum is configured below the floor, the
floor wins and a warning is emitted.

An image rollback retains all ordinary paid claims and pool evidence. A later
speculation milestone must be backward compatible with durable open episodes:
before any old binary is deployed, speculation is disabled and every episode
must reach `CLOSED`, or a cleanup-only compatibility worker must remain deployed.

## Milestones and rollout gates

### Milestone 1: target-backed throughput

Implement the evidence-aware service ceiling and durable feedback wakeup. Ship
with maximum service window 16. Unit tests and real-PostgreSQL tests must prove
the atomic envelope, conservative reconstruction, typed-outcome ordering, event
coalescing, and polling fallback.

Canary maximum 24 for `boltz-l4-fleet`. Promote only if all of these hold over a
natural scale-up wave:

- no service exceeds its computed effective limit;
- no exact pool exceeds its admission limit;
- controller RSS and API request queue age remain inside existing alert bounds;
- the API server advertises at least 32 guaranteed long workers, the global
  Serve worker ceiling is 32, and the canary owns no more than 24 live launches;
- typed worker completion to pool-close and next-decision latency materially
  improve;
- zero-cost, request priority, and unrelated services continue progressing;
- provider throttling and quota errors do not increase unexpectedly.

Rollback sets maximum back to 16. Existing claims drain naturally and are never
revoked.

### Milestone 2: third failure domain

Set the exact canary profile's maximum exploration frontier to three while
retaining the global maximum of two and the 30-second feedback delay. Verify no
other service opens a third pool, no fourth pool opens, every third pool is
target-backed, restart reconstructs three-pool ownership, and placement prefers
a new provider-region. Roll back by removing the profile field or restoring it
to two; existing third-pool claims drain naturally.

### Milestone 3: shadow speculation

Add the PostgreSQL episode and ledger migration, decision audit rows, billing
reconciliation watermark, dashboard diagnostics, and cleanup-only recovery.
Keep actuation disabled. Replay shadow decisions against actual provider invoice
attribution for at least one representative billing interval. The upper bound,
not the mean, must remain below 3 percent when every active reservation is
charged at its ceiling.

### Milestone 4: bounded speculation

Enable one isolated service with `H <= 4` and the 120-second trigger. Promote to
8 and then 12 only after each tier proves:

- measured realized waste plus active ceiling reservations stays below 3
  percent of attributable non-speculative compute;
- cancellation p99, orphan count, and accounting-uncertain episode count stay
  within explicit zero-or-bounded gates;
- no speculative loser enters routing;
- controller restart and ownership handoff close every episode;
- target-backed latency does not regress when the budget authorizes zero.

Any cost, cancellation, orphan, queue, ownership, or attribution breach disables
new speculation before changing the target-backed policy.

## Test plan

Automated coverage must include:

1. Pure configuration and effective-window tests for no owned pool, cold pools,
   duplicate aliases, successful pools, expired success, cooldown, probe,
   malformed identities, configured caps, and inherited overage.
2. Replica-manager physical and logical batches proving that the current target
   still bounds launches while a productive service may pass 16 up to its
   effective cap.
3. Real-PostgreSQL races in which two stale snapshots contend for the last
   dynamic service slot and exactly one claim commits.
4. A multi-member real-PostgreSQL batch whose prepared size exceeds the
   effective adaptive window, proving that the committed policy-valid subset
   plus existing claims stops exactly at the recomputed service limit, remains
   `SCHEDULED`, and creates no association/request/queue/pin before ordinary
   binding. Include a saturated middle member followed by an accepted member
   for a distinct logical slot and eligible pool.
5. Launch completion waking the manager, typed outcome commit waking the
   autoscaler only after persistence, generic failure not waking it, coalesced
   waves, restart fallback, and ownership-loss behavior.
6. Existing frontier, zero-cost, request-priority, rollout, recovery, and paid
   capacity suites unchanged.
7. For speculation, migration upgrade/downgrade ownership, winner races, late
   success, cancellation ambiguity, crash failpoints around winner commit,
   cleanup recovery, price staleness, reservation exhaustion, rejected-request
   exclusion, invoice reconciliation, and fail-closed mixed-version rollback.

Manual verification follows the milestone gates above and records exact image
digest, Helm revision, controller incarnation, environment values, PostgreSQL
invariants, API queue state, and before/after timing for the canary wave.
