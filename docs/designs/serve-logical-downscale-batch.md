# SkyServe logical downscale batch actuation

Status: accepted after Fable adversarial review

## Problem

A logical autoscaler tick can emit hundreds of
`LogicalScaleDownTarget` decisions carrying one immutable
`(version, reconcile_generation, target_capacity)` fence. The controller
currently invokes `scale_down_logically()` once per victim. Each invocation
acquires the replica-manager lock and logical-state lock and usually scans the
whole fleet.

The load balancer publishes a newer reconcile generation every 20 seconds. At
large fleet sizes, the per-victim lock waits and repeated full-fleet scans can
outlive that validity window. The remaining decisions are then stale, but the
controller continues walking the batch. Fresh ticks are delayed, accepted
retirements cannot get timely completion evidence, and recovery may reactivate
uncommitted victims. The service can remain far above its valid logical target.

## Goal and safety invariant

A logical scale-down wave selected from one generation must require one manager
lock acquisition and at most one full-fleet scan. The exact logical fence is
validated once before mutation. The wave either accepts a capacity-safe ordered
subset from one state snapshot, or rejects the entire stale wave before victim
mutation.

Every accepted victim retains the existing durable drain and later-generation
completion proof. Coverage, freshness, version, idleness, and controller
recovery fences remain unchanged.

## Scope

In scope:

- Batch consecutive logical scale-down decisions at the controller-to-manager
  boundary without changing decision order.
- Add one SkyPilot replica-manager batch implementation with one fence check
  and one authoritative fleet scan.
- Reuse the current retirement and capacity rules with an in-memory capacity
  ledger updated after each accepted victim.
- Preserve the single-victim API as a compatibility wrapper.
- Add controller and replica-manager regression tests plus batch summary logs.

Out of scope:

- Dashboard or history changes.
- Autoscaler target math, downscale delay, or rate-limit policy.
- Logical slot units or reserved-capacity-fill policy.
- Drain, completion, restart-adoption, or reactivation semantics.
- Logical or physical scale-up changes.
- Database schema or public API changes.
- Manual EC2 termination.
- The Boltz Platform image-pin pull request.

## Design

### Controller

Collect consecutive `LogicalScaleDownTarget` decisions whose version,
generation, and target are identical. Flush that batch before an incompatible
decision and at the end of the tick. Existing physical scale-up batching keeps
its current behavior, and all cross-operator ordering is preserved.

The manager call is:

```python
scale_down_logically_batch(
    replica_ids,
    target_capacity,
    version,
    reconcile_generation,
)
```

### Replica manager

The base class has a conservative loop fallback. `SkyPilotReplicaManager`
overrides it under one `@with_lock` acquisition. While holding
`_logical_state_lock`, it:

1. Validates logical mode, snapshot freshness, exact version and generation,
   current manager version, any pending version, and the published target.
2. Reads the fleet once and treats those rows as the authoritative actuation
   snapshot, including victim resolution and the durable-defer input. Missing
   and terminal victims are skipped from that snapshot without separate point
   reads.
3. Computes current-version committed and ready capacity while excluding
   already-retiring non-victims, matching the singleton implementation.
4. Processes requested victims in autoscaler order. It applies the existing
   not-yet-served committed-capacity proof or the served, known, idle,
   ready-capacity proof.
5. After accepting a victim through the existing termination or durable
   idle-defer path, it subtracts that victim's exact step-3 contribution from
   each ledger bucket. A ready-and-known current-version victim contributed
   `min(planned, observed)` to both committed and ready capacity. Any other
   current-version victim contributed planned width to committed capacity only.
   An outdated victim contributed zero. Skipped victims do not affect the
   ledger.
6. Defensively skips duplicate ids and victims that are already retiring,
   without changing the ledger.
7. Changes the ledger only after the durable acceptance action succeeds. If a
   per-victim persistence operation raises, the batch lets the error escape so
   the controller's existing outer-loop handler aborts the remainder. Earlier
   acceptances stay durable and individually fenced; a fresh tick retries the
   rest.

The logical-state lock prevents a newer LB generation from landing between the
shared proof and durable acceptance. The batch is the only `@with_lock` entry
and calls unlocked internals, because the manager lock is non-reentrant. The
critical section is shorter than the current serial wave because it removes
repeated manager-lock waits, victim point reads, and repeated fleet scans. The
normal ready-victim path performs no cloud API calls while holding the locks.
The preserved not-yet-served path may still interrupt and join an in-progress
launch, matching the singleton behavior.

### Observability

A stale wave produces one structured summary containing its fence and victim
count, not one line per victim. A completed wave logs requested, accepted, and
skipped counts.

## Alternatives

- Breaking the singleton loop on the first stale result bounds stale work but
  leaves valid waves with repeated lock acquisition and O(victims * fleet)
  scans.
- Removing the generation fence weakens safety.
- Increasing the LB interval or recovery timeout masks the actuation defect.
- Adding a scale-down rate limit changes policy and belongs in a separate
  change.

## Validation

- Controller grouping and operator-order tests.
- Stale batch rejection before reads or mutation.
- Missing or terminal victims skipped from one fleet scan without mutation.
- Exactly one fleet scan for a large valid wave.
- Cumulative capacity safety across mixed current, outdated, busy, unknown, and
  ineligible victims.
- Exact ledger contributions for degraded ready capacity, not-yet-served
  current-version capacity, and zero-contribution outdated victims.
- Duplicate and already-retiring victims skipped without ledger changes.
- A mid-wave persistence failure keeps earlier durable acceptances, does not
  decrement for the failed victim, and aborts the remainder for a fresh tick.
- Differential equivalence between the batch and sequential singleton behavior
  over a frozen fleet snapshot.
- Existing logical retirement and recovery suites.
- Production verification of exact deployed commit, Helm readiness, target
  convergence, absence of stale per-victim log storms, stable queue and
  rejection signals, and SkyPilot-to-AWS instance reconciliation.

## Rollout

Merge to `improvements` after all visible PR checks pass. Publish the exact
merge commit and upgrade the existing production Helm release directly with
`--reuse-values`. Do not create the Boltz Platform pin PR in this task. Monitor
for at least 15 stable minutes. Roll back to the prior Helm revision on API
health failure, demand regression, capacity below target, or retirement safety
errors.

During production monitoring, include LB sync latency because one batch holds
the logical-state lock across its durable acceptances. The removed repeated
fleet scans should make this substantially shorter than the current serial
wave, but a latency regression is a rollback signal.
