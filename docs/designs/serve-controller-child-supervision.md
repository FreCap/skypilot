# SkyServe controller-child supervision

## Status

Implementation and verification in progress. The dead-child-only supervision
contract was implemented after a production recovery loop on 2026-07-22. The
2026-07-23 extension removes a separate controller-start serialization failure
without changing the supervision policy. This design remains intentionally
separate from the proposed 40-second/one-veto scaling policy. Production
remains at 300 seconds and the compatibility default of two vetoes while these
changes are deployed and observed.

## Problem

The consolidated Serve parent supervises one controller child. It checks both
`multiprocessing.Process.is_alive()` and the child's authenticated lightweight
HTTP health endpoint. A live child that misses health for 60 seconds is treated
like a dead child and passed to `_respawn_controller`.

Large-fleet recovery can keep the HTTP endpoint unavailable for longer than 60
seconds while the child is making real progress. In production SkyPilot
1.1.667, `boltz-l4-fleet` recovered 434 tracked logical slots, 246 ready and 188
provisioning. The child logged sequential launch re-drive and load-balancer
sync, but health timed out. The parent repeatedly created a replacement:

| Event | Parent PID | Old child PID | Replacement PID |
|---|---:|---:|---:|
| first false replacement | 1425 | 1678 | 33122 |
| second false replacement | 1425 | 33122 | 47212 |
| third replacement started | 1425 | 47212 | 66728 |

`_respawn_controller` currently starts and readies the replacement before it
kills the old child. Both children therefore overlap with the same service
hash, parent PID, and pod IP launch authority. The child port fences HTTP
routing, but it is not part of replica-manager launch ownership. This violates
single-writer safety and repeatedly discards recovery progress.

Each real child reconstruction also rebuilds process-local autoscaler state.
During this incident, raw demand fell from 71 to 30 and then 1 while the
retained demand target stayed 356. The 300-second downscale window could not
complete because supervision kept reconstructing the child. This explains the
plateau without requiring a stale or wrong load balancer.

A second production failure on 2026-07-23 exposed excessive serialization in
controller startup. Every service parent selects a local controller port under
one pod-local file lock. The lock correctly closes the gap between observing a
free port and the child binding it, but it is currently held while waiting up
to 420 seconds for the controller to finish reconstruction and start listening.
One slow `boltz-l4-fleet` recovery held the lock while an OpenDDE controller
queued behind it; an older OpenDDE recovery then acquired it and repeated the
same wait. Unrelated `sky serve up` operations could not register even though
they needed different local ports.

## Load-balancer authority and target separation

The production Kubernetes Service selected HA slot `a`, whose durable cutover
generation was 42. Slot `a` was Ready. Slot `b` was unready and absent from the
Service selector. Ordinary demand sync does not carry the cutover generation.
It is authorized by the live pod UID and readiness plus agreement among the
reporter's slot, durable active slot, and Kubernetes Service-selected slot. An
unselected former-active slot, wrong pod UID, or unready reporter is not
authoritative. Adding a generation to each demand report is separate HA
protocol work and is not part of this emergency patch.

The purple reservation target is an independent fill overlay. It may remain
high intentionally when research-cluster capacity is free. It is not produced
by an LB and must never seed orange demand hysteresis. A restart baseline may
include only latest-version, nonterminal, non-retiring demand-origin capacity:

- ordinary demand launches count;
- demand launches placed on zero-cost capacity count;
- legacy rows without `reserved_fill` metadata count as demand;
- ready or provisioning rows with `reserved_fill=true` do not count;
- the fill target remains a separate overlay;
- losing a fill row may cause a paid launch only when independently computed
  adopted demand exceeds compatible surviving nonfailed capacity.

## Goals

- Keep at most one live controller child per service parent.
- Replace a child automatically only after process death is authoritative.
- Never reset productive recovery because an HTTP health endpoint is slow.
- Preserve immediate recovery after actual child exit.
- Preserve ownership, port-publication, teardown, and HA fences.
- Keep demand restart baseline and reserved fill strictly separate.
- Expose enough state to distinguish slow recovery, dead child, stale demand,
  and intentional reserved fill.
- Let independent controller children initialize concurrently without
  reintroducing a port-selection race.

## Non-goals

- Detecting every possible live-process deadlock automatically in this patch.
- Designing a general progress-heartbeat protocol.
- Changing demand calculation, downscale delay, veto budget, queue patience, or
  scale-up rate.
- Changing HA load-balancer promotion or Service routing.
- Changing replica launch or cleanup concurrency.
- Shortening the 420-second service registration budget.
- Adding a new public port allocator, database column, or Kubernetes Service.

## Behavior contract

### Replacement trigger

`Process.is_alive() is False` is the only automatic replacement trigger in this
patch. HTTP health remains an observability signal, not proof of death.

A live child that misses health:

- remains the only child;
- retains its published port and process-local state;
- is logged as `hold_live_child` with health-miss age and LB health;
- with a healthy external LB, leaves service status unchanged and does not
  advance the degraded-status failure counter;
- with an unhealthy external LB, follows the existing degraded-status
  accounting and may set `CONTROLLER_FAILED` after three failures and existing
  exponential backoff, but is still not killed or replaced;
- resumes healthy accounting when the same child answers again.

`CONTROLLER_FAILED` heals through the existing path only after both the same
live child responds and the external LB is healthy. Healing resets the service
to `REPLICA_INIT` so the controller can recompute replica-driven status.

This deliberately trades automatic recovery from a genuinely live deadlock for
single-writer correctness. Until an out-of-band progress protocol exists, an
operator or API-pod restart is the recovery path for a confirmed deadlock.

### No-overlap respawn

`_respawn_controller` accepts only a concrete, dead prior child. It checks
liveness before selecting a port or spawning. A live child, missing process
handle, or ambiguous liveness causes a bounded fail-closed result, with no new
process and no DB port write.

Before spawning a replacement, the parent must join or otherwise confirm the
prior process is reaped. If death or reaping cannot be confirmed, respawn fails
closed and retries later. The sequence is:

```text
observe is_alive == false
confirm/reap old child
select fresh port
spawn replacement
wait for replacement TCP readiness
CAS-publish replacement port for exact parent owner
```

There is never a state with two live child PIDs under one parent. Existing
owner-loss behavior remains unchanged: a stale parent and child exit without
destructive cleanup. A real child death still creates a new controller session
and resets process-local autoscaler state.

### Concurrent startup with a reserved socket

Initial startup and dead-child respawn use the same socket-handoff primitive:

```text
acquire pod-local compatibility lock
create an IPv4 stream socket
bind it to the controller host and first available controller port
spawn the controller child with that bound, non-listening socket
release the compatibility lock
wait for child TCP readiness
CAS-publish the ready owner tuple and port
close the parent reservation copy
```

Binding the real socket, rather than selecting and releasing a temporary one,
keeps the port exclusively reserved while the child reconstructs state. The
socket is intentionally not put into listening mode in the parent, so the
existing TCP readiness probe cannot succeed until the child has installed the
socket into Uvicorn's event loop. The child starts Uvicorn with the inherited
socket instead of asking Uvicorn to bind the host and port again.

The parent retains its descriptor copy as a reservation lease through the
readiness check and owner-fenced database publication. Without that lease, a
child could die between the parent's liveness check and TCP probe, another
service could reuse the just-freed port, and the first parent could mistake the
other controller's listener for its own. The retained duplicate prevents reuse
through the complete decision window. Once the exact owner tuple and port are
published, the parent closes its copy and the live child becomes the sole
socket owner. A later child death is handled by the existing supervision path.

A failed `Process.start()` closes the parent reservation before propagating the
error. A readiness or publication failure closes it in the caller's cleanup
path; `os._exit` also closes it at the process boundary. Readiness waiting,
owner publication, and all database work happen after the file lock is
released.

The file lock remains around reservation and spawn for rolling compatibility.
An older same-pod starter still holds that lock until its child binds. Once a
new starter releases it, the new child's bound socket makes the port unavailable
to old `find_free_port` callers. The socket is node-local and process-owned; it
adds no central allocator, durable lease, or cross-pod coordination.

### Demand-only reconstruction

Controller reconstruction restores a conservative demand actuation baseline
once, on the first fresh complete demand recompute. That baseline is the
demand-origin cohort defined above, excluding reserved-fill ready and
provisioning rows. The same cohort owns the 50-percent downscale allowance and
pending-retention floor.

Fill disappearance does not manufacture demand. Reserved capacity may satisfy
the adopted demand target and suppress duplicate launches, but the fill target,
fill-origin capacity, and reserved broker grants cannot be copied into the
demand baseline.

### Longer-term progress-aware supervision

A future change may restore automatic recovery for live deadlock only with an
out-of-band child phase/progress heartbeat that does not depend on the same HTTP
event loop. It must distinguish monotonic recovery progress from a stalled
phase and still kill and confirm death before spawning. Log-file timestamps,
replica count heuristics, or a larger static timeout are not authoritative.

## Observability

This patch adds parent-owned structured logs only. They expose parent PID,
child PID, `is_alive`, HTTP health state, health-miss start and age, external LB
health, and action (`hold_live_child`, `respawn_dead_child`, or
`refuse_live_respawn`). It adds no status schema or parent-child diagnostic
store.

When the child responds, existing autoscaler status and minute history continue
to keep these values separate:

- raw demand;
- adopted demand after hysteresis;
- downscale elapsed time and active deadline;
- demand-origin committed and provisioning capacity;
- fill target and fill-origin ready/provisioning capacity;
- effective capacity target;
- controller session where already exposed.

Recovery phase, completed-row progress, child start metadata, cross-process
respawn counters, and stalled-recovery alerting belong to the future
out-of-band heartbeat design. They are not promised by this patch.

## Compatibility and rollback

This patch adds no public service field and requires no schema migration. Old
and new service specs behave the same. Socket transfer uses Python
`multiprocessing`'s supported socket reduction on spawn platforms and ordinary
descriptor inheritance on fork platforms. The compatibility lock makes a
rolling mix with the prior selection implementation safe on the same pod.

A control-plane rollback restores live health-timeout replacement and the
long-held startup lock, so rollback is unsafe while a large recovery is in
progress. Verify one healthy child and no replacement attempt before rollback.

The service policy stays at 300 seconds/two vetoes throughout this rollout.
The 40-second/one-veto canary is a later, separately reviewed service update.

## Test plan

- Keep a live child HTTP-unresponsive for more than 60 and 300 fake-clock
  seconds. Assert no respawn, one child PID, and unchanged published port.
- Repeat with healthy and unhealthy external LB. Degraded reporting may differ,
  according to the exact contract above, but neither case may replace a live
  child.
- Kill a child. Assert one immediate replacement, fresh port publication, and
  no overlapping live PID.
- Pass a live child directly to `_respawn_controller`. Assert refusal occurs
  before port selection, spawn, DB publication, or kill.
- Pass no child handle to `_respawn_controller`. Assert the same fail-closed
  behavior; absence is not authoritative proof of process death.
- Simulate join/reap failure for a dead child. Assert fail-closed retry and no
  replacement.
- Reserve a controller socket. Assert a second bind to its host and port fails,
  and assert a TCP connection does not succeed until the socket is listening.
- Start a child through the socket-handoff helper. Assert a failed
  `Process.start()` closes the parent copy, while success returns a live
  reservation lease to the caller.
- Block controller readiness for one service while starting another. Assert the
  port-selection lock is released before either readiness wait and both
  services receive distinct reserved ports.
- Kill a child during readiness, then start another controller. Assert the
  first parent's retained socket prevents port reuse and no foreign listener
  can satisfy its readiness/publication path.
- Assert initial startup and respawn close the parent reservation only after
  successful owner-fenced port publication, and close it on every ordinary
  failure path.
- Run the handoff under both `fork` and `spawn` multiprocessing contexts where
  the platform supports them.
- Assert the controller passes the inherited socket to Uvicorn; the legacy
  no-socket path remains covered for direct controller tests.
- Set the unrelated LB/degraded-status retry deadline far in the future, then
  transition a concrete child from live to dead. Assert the first dead-child
  observation attempts exactly one immediate replacement. Only a failed
  respawn attempt may advance the separate respawn retry deadline.
- Repeat real child death and respawn. Assert recovery is idempotent and does
  not duplicate launches or terminations.
- Use a demand-baseline fixture containing demand ready/provisioning,
  reserved-fill ready/provisioning, demand-origin zero-cost, and legacy rows.
  Assert only demand-origin capacity seeds the restart target and both
  downscale budgets.
- Remove fill ready and provisioning rows. Assert no paid backfill unless
  adopted demand exceeds compatible surviving capacity.
- Assert the selected Ready slot `a` with the live UID may update demand, while
  unready/unselected slot `b`, a former-active unselected slot, and a wrong-UID
  report cannot mutate the demand snapshot or autoscaler target. Do not assert
  a per-report generation fence that ordinary demand sync does not carry.
- Run focused child-supervision, controller-respawn, recovery, concurrency
  autoscaler, reserved-fill, LB HA, and Serve service tests. Run formatting,
  mypy, pylint, Ruff, and the broader Serve unit-test slice.

## Rollout

1. Obtain a `PURSUE` verdict on this exact design.
2. Implement the smallest socket-reservation extension on the latest
   `origin/improvements` head without changing controller supervision policy.
3. Run focused tests and formatting, then review the exact tested commit.
4. Merge only after the full visible GitHub check rollup is green.
5. Build an immutable control-plane image from the merge commit and deploy with
   Helm `--reuse-values`.
6. Verify API version and commit, one child PID per parent, active LB slot and
   generation, and continued endpoint traffic.
7. Observe a complete `boltz-l4-fleet` recovery. Raw demand, adopted demand,
   fill target, ready capacity, and provisioning capacity must remain separate;
   no health miss may create a new child generation.
8. Keep the service policy unchanged until one healthy held-out window proves
   target and capacity convergence. Only then return to the separately reviewed
   downscale-policy canary.
