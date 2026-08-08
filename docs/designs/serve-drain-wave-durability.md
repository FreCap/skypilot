# Drain wave durability

- **Status:** designed, not implemented; current-code audit repeated on
  2026-08-08
- **Last updated:** 2026-08-08
- **Milestones:** W1 fresh-demand target gate, W2 bounded recovery hold, W3 wave counters
- **Hard prerequisites for the reclaim case:** Milestones 1 and 2 of
  `docs/designs/serve-drain-proof-across-lb-restarts.md` (mechanisms A, B, C)
- **Blocks:** enabling `reserved_capacity_fill.utilization_gate` on
  `protenixv2-hybrid-v1` (see Milestones)

Line numbers are at `13368520fe` on `feat/serve-reserved-fill-utilization-gate`.
Read them with `git show 13368520fe:sky/serve/<file>.py`. Note that
`serve-drain-proof-across-lb-restarts.md` and
`serve-reserved-fill-utilization-gate.md` cite the deployed SHA
`a0028d62c7be576a97937d8fe7471bfa7c019849` instead, so their line numbers do not
match this document's.

### Current-code audit (2026-08-08)

The implementation has moved substantially since the design snapshot, but the
three proposed milestones have not silently landed. Audited at
`0407c5a7daf65a375c55275b5ff4224f4dfc5154`:

- W1 is still absent from logical-target publication. The controller gates the
  separate `target_num_replicas` publication on
  `has_recomputed_with_fresh_data()` at `controller.py:4346-4358`, but publishes
  `logical_target_state` without that gate at `controller.py:4386-4398`.
- W2 is still absent. Recovery initializes a process-local 120-second deadline
  at `replica_managers.py:7690-7693`, then renews it when evidence remains
  unavailable or incoherent at `7735-7752`. The triage does not release a member
  when its durable drain budget expires.
- The original milestone-0 counters remain in
  `drain_observability.py:44-100`, but W3's `evidence_gap`, discarded-drain-time,
  recovery-hold, and stale-target counters are not present.

The historical line references below remain pinned to `13368520fe` so the
original proof is reproducible. The audit above is the authoritative status
check against the current branch.

**Headline, stated first because it is the whole argument.** A drain wave does
not need to become a durable object. Its members already are: seven
`logical_retirement_*` fields plus `drain_cap_seconds`, `drain_started_at` and
`logical_retirement_committed` are persisted on every victim row
(`replica_managers.py:1145-1171`), and the recovery path's *default* outcome
after a restart is adoption, which preserves every one of them
(`replica_managers.py:5791-5839`, whose own log line at `5835-5840` says it
preserves "their durable drain deadlines"). What destroys a wave is not the
ephemeral controller epoch. It is that the pass which owns the wave after a
restart decides on evidence it does not have, has no bounded outcome, and can be
handed a demand target computed from an empty window. This design fixes those
three things in about 60 lines, adds no durable state, no table, no migration,
and no new fencing mechanism. `SERVE_VERSION` stays `'030'`
(`sky/utils/db/migration_utils.py:54`).

A `serve_drain_waves` table was designed and rejected. The rejection is argued in
full under "Rejected alternatives", and the short version is that the only thing
a wave row can carry that the replica rows cannot is a durable *kill budget*, and
a durable kill budget is exactly the mechanism by which a resumed wave keeps
killing against intent formed before the restart. That is failure class 1.

## Problem

### What a wave is

A wave is one autoscaler tick's decision to retire a set of replicas. It is
created as a plain list: `LogicalScaleDownTarget` decisions emitted by
`_generate_logical_scaling_decisions` (`autoscalers.py:6290-6302` and
`6984-6996`), batched by the controller into one call at
`controller.py:3844-3865` using the *first* decision's fence, and accepted by
`ReplicaManager.scale_down_logically_batch` (`replica_managers.py:6187-6376`)
under one lock, one fleet read and one capacity ledger. There is no wave object,
no wave id and no durable wave record. The only thing the members share is the
fence tuple `(version, reconcile_generation, target_capacity,
target_capacity_by_accelerator, accelerator_shapes)`, stamped per row by
`_defer_scale_down_until_idle` (`replica_managers.py:5132-5142`).

There is no cap on wave size. Selection is bounded only by the capacity ledger
(`autoscalers.py:6924-6982`, mirrored at `replica_managers.py:6255-6298`), so
with 78 replicas and an autoscaler target of `{'A100-80GB': 1, 'L4': 1}` a single
tick legitimately emits about 76 victims and one batch accepts all of them. The
reserved-capacity reclaim is exactly this shape.

### Why it dissolves today

Two clocks, and the second fires far sooner than the first.

**The proof clock.** `serve-drain-proof-across-lb-restarts.md` establishes that
after a load balancer session change an already-off-route url is structurally
unreachable in all four acknowledgement sets, so `_ReplicaDrainTracker._seen`
(`replica_managers.py:812-850`) can never be re-established. A member's idle
proof can therefore only complete inside the LB session that acknowledged it.
Measured session lifetime is 3300 s (46 rollouts in 41.9 h). protenix's
`graceful_drain_seconds` is 7200 s, the schema maximum. 7200 / 3300 = 2.18, so a
member that needs its full budget outlives at least two sessions with certainty,
and its proof becomes unobtainable after the first roll. It then runs to the
deadline, `outdated_backend` is `False` for a same-version reclaim victim
(`replica_managers.py:6046-6051`), and control reaches
`_abort_logical_retirement(info, 'post-routing idle proof timed out')` at
`6072-6075`, which nulls `drain_started_at` at `5881`. The next tick re-selects
the same replica, because the ordering at `autoscalers.py:6899-6905` is
deterministic and its inputs are unchanged, and gives it a fresh 7200 s.

**The destruction clock.** The controller and every LB restart together: the
LB pod template pins `skypilot.co/controller-image-digest`, and on 2026-07-25 the
api-server pod was recreated at 11:57:47Z with every LB pod following between
11:59:28Z and 12:02:12Z. `ReplicaManager.__init__` mints a new epoch at
`replica_managers.py:2023`, every uncommitted member fails the compare at
`5170-5171`, and the recovery walk at `3032-3036` sweeps all of them into
`_recovering_logical_retirement_ids`. `_reconcile_recovering_logical_retirements`
(`5611-5840`) then decides on the first fresh version-matching snapshot, which
after a co-restart is blind two independent ways: `observed_slots_by_replica_id`
is empty because `total_slots_by_url` is only written by a probe round, and
`force_all_live_unknown` (`controller.py:1073`) marks every live replica unknown.
`_logical_ready_capacity` skips both cases (`replica_managers.py:5245-5254`, with
an in-tree comment that already names this), so ready capacity reads 0, the
shortfall branch at `5727` fires, and up to
`_LOGICAL_RETIREMENT_RECOVERY_MAX_REACTIVATIONS_PER_GENERATION = 20` (line 91,
enforced at `5774-5778`) members are reactivated per snapshot generation.
Generations advance once per LB sync, every 20 s
(`controller.py:1074-1076`). A 76-member wave is therefore fully undone in four
generations, about 80 s.

### The arithmetic

Per 3300 s cycle: 0 terminations, 76 discarded drains, 76 fresh 7200 s budgets
started. There is no number of cycles after which the wave completes. That is
what "the reclaim feature is effectively inert" means, and it is why
`serve-reserved-fill-utilization-gate.md:19-38` says not to enable the gate on
protenix yet.

### What actually dissolves the wave, precisely

Not the epoch. The recovery pass has two outcomes, and destruction is the
*exception*:

- **Adoption** (`replica_managers.py:5791-5839`) rewrites only the seven fence
  fields at `5806-5817`. It never touches `drain_cap_seconds`,
  `drain_started_at` or `wait_for_idle_before_termination`. A wave adopted after
  a restart keeps every member and every elapsed second.
- **Reactivation** (`5727-5789`) calls `_abort_logical_retirement` at `5760`,
  which nulls the drain at `5880-5881` and pops the tracker at `5891`.

The epoch merely routes the wave into that pass. The pass reactivates only when
it reads a shortfall, and after a co-restart it reads a shortfall because the
view is blind. **Remove the blind read and the same code adopts the whole wave
with its elapsed drain intact.** That is mechanism C of
`serve-drain-proof-across-lb-restarts.md`, and it is the single largest piece of
wave durability. This design does not re-derive it.

### What mechanism C does not supply

**(1) A bound on the hold, which C makes the normal outcome.** While a member is
in `_recovering_logical_retirement_ids` it is neither terminable nor readmittable:
`_refresh_wait_for_idle` continues at `6028-6036` and down admission continues at
`7200-7210`. The pass's own 120 s deadline is diagnostic and self-renewing
(`5654-5664` and `5670-5679` both log and then push the deadline out by another
`_LOGICAL_RETIREMENT_RECOVERY_TIMEOUT_SECONDS`), and two other gates return with
no bound at all: the pending-version gate at `5682-5684` returns silently, and
the reactivation-generation gate at `5687-5689` returns until a newer generation.
Three reachable paths hold a wave forever:

- The target is revoked every tick. `invalidate_logical_target()` is called
  whenever exact-card retirement must fail closed
  (`controller.py:3767-3772`, driven by `autoscalers.py:6275-6280`). Then
  `target_state is None`, gate A at `5651-5665` renews and returns, forever.
- A per-card shortfall that no victim can close. The reactivation loop skips any
  candidate whose card has no shortfall (`5754-5759`), so an L4 shortfall against
  76 A100 victims readmits nobody, consumes the generation at `5744-5745`, and
  returns. Every later generation repeats it. Held victims contribute 0 to
  `_logical_ready_capacity` because of the `is_scale_down` filter at `5236-5239`,
  so the shortfall does not close by itself.
- An aggregate shortfall with `target_by_accelerator` non-empty and every per-card
  shortfall zero. Same loop, same outcome.

C bounds only its own suppression (120 s, then today's behaviour resumes). It
does not touch these three, and by making the hold the normal post-restart
outcome instead of the exception it exercises them far more often.

**(2) A demand target that the autoscaler itself trusts.**
`_last_logical_target_state` is written unconditionally at
`autoscalers.py:6275-6289`, while only the SCALE_DOWN decisions are gated by
`_fresh_for_tick()` at `6293-6294`. `controller.py:3763-3766` publishes it with
no freshness test. After a controller restart the concurrency autoscaler's demand
window is empty and its target is `min_replicas`;
`_set_target_num_replicas_with_concurrency_logic` explicitly does *not* consume
its one-shot snap on a stale tick (`autoscalers.py:5946-5960`, and
`has_recomputed_with_fresh_data()` at `4238-4250` exists precisely so consumers
can tell). `_fresh_for_tick`'s docstring at `4265-4275` names the hazard
verbatim: "marrying a blind target to fresh-mode kills."

Today this is masked by accident. The recovery pass reads a blind capacity (0)
and may hold a rebuilt-blind target (2). The two errors point in opposite
directions and the blind capacity wins, so the outcome is reactivation, which is
the safe direction for capacity. **Mechanism C removes half of that mask.** With
the blind capacity read suppressed and then the view becoming sighted, a
rebuilt-blind target survives into a coverage check that passes:
`ready_covers_target` at `5215-5218` compares real capacity against
`min_replicas`. For an outdated-version member the bounded path at `6052-6071`
and `7224-7241` then completes the teardown with `require_victim_idle=False`,
that is with no idle proof at all. A mass teardown authorized by a demand
estimate the autoscaler refuses to act on is failure class 1, and shipping C
without W1 creates it.

## Correctness contract

Numbered and testable. "Member" means a replica row carrying an uncommitted
`logical_retirement_*` intent. New properties are marked NEW; the rest are
existing properties this design pins so they cannot be lost.

1. **NO DURABLE KILL AUTHORIZATION.** No durable record of wave intent exists.
   Every irreversible teardown re-derives its authorization from the currently
   published `_logical_target` and a current fleet snapshot, at all three
   consumers: `_refresh_wait_for_idle` (`replica_managers.py:6037-6043`),
   `_finish_logical_retirement` (`5899-5919`, which evaluates the state twice
   around the confirmation write), and down admission (`7231-7241`). *Test:* hold
   a wave of N members, raise the published target above what the remaining fleet
   covers, assert zero members reach `_terminate_replica`.

2. **POST-KILL COVERAGE.** At the instant a member is admitted, observed ready
   capacity excluding that member covers the live aggregate target and every live
   per-card target (`5203-5218`, `excluded_ids = {info.replica_id}`; every other
   off-route member already contributes 0 via the `is_scale_down` filter at
   `5236-5239`). Unchanged. Rules out LOST CAPACITY in the steady state.

3. **THE PUBLISHED TARGET IS BACKED BY A FRESH RECOMPUTE. (NEW, W1)**
   `_logical_target` is non-`None` only if the autoscaler's most recent recompute
   consumed a fresh demand report (`has_recomputed_with_fresh_data()`,
   `autoscalers.py:4238-4250`). Otherwise it is revoked and every member is
   `'wait'` (`replica_managers.py:5173-5176`). *Test:* an autoscaler with
   `_snap_target_on_next_recompute` still `True` publishes no target, and
   `_logical_retirement_state` returns `'wait'` for a member whose fence is
   otherwise current. Rules out LOST CAPACITY across a restart, including the
   no-idle-proof bounded path at `6052-6071`.

4. **THE RECOVERY HOLD NEVER OUTLIVES THE VICTIM'S DURABLE DRAIN BUDGET. (NEW,
   W2)** A member is released from `_recovering_logical_retirement_ids` as soon as
   `_remaining_drain_seconds(drain_started_at, drain_cap_seconds) <= 0`
   (`replica_managers.py:727-734`), or as soon as that budget cannot be
   evaluated. Release returns it to routing. *Test:* with no snapshot and no
   target ever arriving, a member whose wall-clock budget has elapsed is routing
   within one `_refresh_thread_pool` pass. Rules out STUCK CAPACITY.

5. **EVERY HELD MEMBER HAS EXACTLY TWO EXITS AND ONE OF THEM NEEDS NO EVIDENCE.
   (NEW, W2)** Adoption (needs a fresh, version-matching, generation-coherent,
   non-blind snapshot and a target) or budget-exhaustion release (needs only the
   wall clock). No third state exists and no gate can return without one of the
   two eventually applying. *Test:* enumerate the four evidence gates
   (`5651-5665`, `5668-5680`, `5682-5684`, `5687-5689`) and assert each one still
   terminates the hold at the budget.

6. **ADOPTION IS NON-DESTRUCTIVE.** Adoption rewrites only the seven fence fields
   (`5806-5817`) and never `drain_cap_seconds`, `drain_started_at` or
   `wait_for_idle_before_termination`. *Test:* pin field-level equality of those
   three across a simulated restart plus adoption. This is the property that
   makes a wave survive a restart, and it already holds; the test exists so a
   future change cannot silently remove it.

7. **A BLIND CAPACITY VIEW IS NOT A SHORTFALL.** Delegated in full to mechanism C
   of `serve-drain-proof-across-lb-restarts.md`. Hard dependency, not restated
   here.

8. **GENERATIONS ARE COMPARED ONLY INSIDE ONE EVIDENCE DOMAIN.** The per-process
   `_logical_controller_epoch` (`2023`, rotated at `4592`, compared at `5171` and
   `5695-5697`) is retained verbatim. It is the validity domain for
   `_reconcile_generation`, which is process-local and resets to 0
   (`controller.py:417-420`, incremented at `1074-1076`). Without it, incarnation
   B at generation 3 would read incarnation A's persisted
   `logical_retirement_generation = 2` as newer evidence at `5175`, match a stale
   `logical_retirement_confirmed_generation`, and run
   `_terminate_replica(..., in_flight_drain_cap_seconds=0)` at `5926-5930`: an
   irreversible zero-drain teardown of async work against a confirmation B never
   observed. Rules out DOUBLE KILL. *Test:* a member persisted at generation 5000
   under a dead epoch is not released by a fresh controller's snapshot at
   generation 3.

9. **ONE ACTOR.** Every replica write carries
   `expected_service_hash` and `expected_controller_owner = (controller_pid,
   controller_ip)` (`_db_fence_kwargs`, `replica_managers.py:2372-2381`), verified
   under `SELECT ... FOR UPDATE` by `_lock_service_owner_in_session`
   (`serve_state.py:2946-2967`), and `_persist_replica` raises on a `False`
   return. Because `logical_retirement_committed = True` is a *write* that
   precedes `t.start()` (`7250-7254`), a fenced-out incarnation can never start a
   teardown. Unchanged.

10. **IRREVERSIBILITY IS PER REPLICA.** `logical_retirement_committed`, written at
    `7250` immediately before the RUNNING persist, remains the sole durable
    admission boundary. Committed rows are exempt from abort on every path
    (`6022-6027`, `5632-5634`) and are detached from their selection by
    `_detach_committed_logical_retirement` (`5842-5856`). Unchanged.

11. **NO REDUNDANT SELECTION.** A replica that is terminal or already carries
    `is_scale_down is True` cannot be selected (`autoscalers.py:6894-6898`,
    `replica_managers.py:6304-6310`). A resumed incarnation therefore cannot
    re-select a replica a previous incarnation terminated, because terminal rows
    and deleted rows both fail that filter. Rules out the other half of DOUBLE
    KILL. Unchanged.

12. **ELAPSED DRAIN IS NEVER CARRIED ACROSS A ROUTING EPISODE. (NEW as an
    explicit non-goal)** Every abort returns the victim to routing, where it
    serves traffic again; a fresh full budget on re-selection is therefore the
    correct semantics. No credit, carry-over or decay of `drain_started_at`
    exists. *Test:* after an abort and re-selection, `drain_started_at` is `now`.
    The argument is under "Rejected alternatives"; the short version is that
    `_ensure_drain_started_at` (`701-724`) is shared by four call sites of which
    three end in a kill, so any credit consumed there is a deadline shortener on
    a killing path.

13. **NO NEW DURABLE STATE.** No new table, no new column, no new
    `replica_state` JSON key, no migration, no `SERVE_VERSION` bump, no
    `API_VERSION` bump, no change to the controller-to-LB wire. Rollback is a
    binary swap with no state to strand.

## Mechanism

### Durable state

None. That sentence is the design.

For completeness, the durable state a wave already has, all of it inside the
authoritative `replicas.replica_state` JSON blob (`serve_state.py:143-166`,
written at `2883-2907`, read via `_replica_from_state` at `2937-2943`):

| field | declared | role |
|---|---|---|
| `logical_retirement_version` | `replica_managers.py:1157` | selection fence |
| `logical_retirement_controller_epoch` | `1158` | evidence domain |
| `logical_retirement_generation` | `1159` | selection fence |
| `logical_retirement_target_capacity` | `1160` | selection fence |
| `logical_retirement_confirmed_generation` | `1161` | final proof stamp |
| `logical_retirement_bounded_deadline` | `1166` | rolling-update completion |
| `logical_retirement_committed` | `1171` | irreversibility |
| `drain_cap_seconds` | `1145` | budget size |
| `drain_started_at` | `1150` | wall-clock budget anchor, restart-durable |
| `wait_for_idle_before_termination` | `1154` | strict-drain marker |

That is a complete description of a wave member. The wave is the set of rows
sharing a fence. Nothing in the four failure classes requires more.

### Logic, W1: the published target must be backed by a fresh recompute

One edit, `controller.py:3763-3772`:

```python
target_state = decision_autoscaler.logical_target_state
if not decision_autoscaler.has_recomputed_with_fresh_data():
    # A target computed before the first fresh-data recompute is the
    # rebuilt-blind minimum (autoscalers.py:5946-5960 deliberately does not
    # consume the one-shot snap on a stale tick). Publishing it would let the
    # coverage check at replica_managers.py:5215-5218 pass against a demand
    # estimate the autoscaler itself refuses to act on, and the bounded
    # rolling-update path completes with no idle proof at all.
    self._replica_manager.invalidate_logical_target()
elif target_state is not None:
    self._replica_manager.publish_logical_target(*target_state)
elif decision_autoscaler.configured_accelerator_shapes:
    self._replica_manager.invalidate_logical_target()
```

`has_recomputed_with_fresh_data()` returns `True` unconditionally on the base
class (`autoscalers.py:1365-1372`: QPS and queue autoscalers recompute from
always-available signals, so their target is never the rebuilt-blind minimum) and
is overridden by `ConcurrencyAutoscaler` at `4238-4250` to return
`not self._snap_target_on_next_recompute`. That flag is set `True` on
construction (`4073`) and on rebuild (`6399`), and cleared only inside
`_set_target_num_replicas_with_concurrency_logic` at `6096-6102`, which is
reached only past the stale-tick early return at `5946`. So the predicate is
exactly "the target was computed on a tick whose demand report was fresh", which
is the property the coverage check needs.

Fail-closed in both directions. `_logical_target = None` makes
`_logical_retirement_state` return `'wait'` at `5173-5176`, and it makes
`_logical_target_fence_holds` reject a new batch in
`scale_down_logically_batch` (`6202-6212`), so a stale target neither authorizes
a teardown nor authorizes a new wave.

Bounded: the flag clears on the first fresh-data recompute, one decision interval
after the first LB sync.

### Logic, W2: the recovery hold never outlives the drain budget

One edit in the triage loop of `_reconcile_recovering_logical_retirements`
(`replica_managers.py:5626-5641`), which already runs on every pass before any
evidence gate:

```python
for replica_id in sorted(list(recovering_ids)):
    info = infos_by_id.get(replica_id)
    ...                                     # existing three discards, 5628-5640
    if self._logical_recovery_budget_exhausted(info):
        # The durable drain budget is the service's own statement of how long
        # it will hold this GPU off route. The recovery hold refuses both
        # teardown (7200-7210) and readmission (6028-6036), so it must not
        # outlive that budget: the four evidence gates below can all return
        # without acting, and two of them renew their own deadline forever
        # (5662-5664, 5677-5679).
        self._abort_logical_retirement(
            info,
            'recovery evidence did not arrive within the drain budget',
            reason_key=drain_observability.ABORT_REASON_EVIDENCE_GAP)
        recovering_ids.discard(replica_id)
        continue
    candidates.append(info)
```

with

```python
@staticmethod
def _logical_recovery_budget_exhausted(info: ReplicaInfo) -> bool:
    status = info.status_property
    drain_cap = getattr(status, 'drain_cap_seconds', None)
    started_at = getattr(status, 'drain_started_at', None)
    if (type(drain_cap) is not int or drain_cap <= 0 or
            not _is_valid_drain_started_at(started_at)):
        # An unevaluable budget cannot bound a hold. Release toward serving.
        return True
    return _remaining_drain_seconds(float(started_at), drain_cap) <= 0.0
```

Notes on the shape, each of which is load-bearing:

- **No new constant and no new clock.** `drain_started_at` is wall clock and
  restart-durable by construction (`_ensure_drain_started_at` at `701-724`, whose
  docstring says exactly this, and the field comment at `1146-1150`), so the bound
  survives any number of restarts. `_remaining_drain_seconds` (`727-734`) is
  already fail-closed against bounded clock skew.
- **Abort here rather than merely discard.** Discarding would hand the row to
  `_refresh_wait_for_idle` in the same `_refresh_thread_pool` pass (`6778`,
  `6782`, `6785`), where the still-mismatched epoch produces `'abort'` at `5171`
  anyway, but with the reason string `'the current target or controller fence
  changed'`, which `_classify_abort_reason` (`737-746`) maps to `fence_changed`.
  Aborting in the triage with an explicit key keeps the accounting honest.
- **`reason_key` is a new optional argument to `_abort_logical_retirement`**
  (`5858-5891`), defaulting to `None` and falling back to
  `_classify_abort_reason`. That preserves the existing property stated in that
  function's docstring, that a new caller cannot add an uncounted abort.
- **No pacing.** The 20-per-generation bound at line 91 exists so a speculative
  shortfall reading does not return the complete old fleet to routing. A
  budget-exhaustion release is not speculative: those rows have consumed the
  entire window the service configured. Pacing it would leave capacity stuck
  longer for no safety gain.
- **The unevaluable-budget case releases immediately.** In practice
  `drain_cap_seconds` is always set, by `_defer_scale_down_until_idle` at
  `5127-5128` and by `_register_wait_for_idle` at `5066-5070`. Rows predating the
  field also predate `logical_retirement_committed` and are handled by
  `_is_legacy_uncertain_logical_retirement` (`5454-5481`) on a separate path
  (`5483` onward), so they never reach this triage. Releasing an unevaluable row
  toward routing is the conservative direction and is deterministic.

The same edit also bounds the in-process version-update handoff, which reuses the
identical recovery set (`_handoff_logical_retirements_for_version_update`,
`4572-4606`, `recovering_ids.update(retiring_ids)` at `4598`). This design does
not try to distinguish a version handoff from a restart. Once the hold is
bounded, the distinction buys nothing: both end in adoption or in a
budget-bounded release.

Second, smaller edit: the pending-version gate at `5682-5684` returns with no log
and no deadline renewal, so a version update stalled on the manager lock leaves
the whole held population invisible. Give it the same log-and-renew treatment the
two gates above it already use (`5654-5664`, `5670-5679`). This is
observability only; W2's budget bound is what makes it safe.

### Logic, W3: counters

Extend the existing `drain_observability.DrainProofStats`
(`sky/serve/drain_observability.py:44-100`), which is already surfaced under
`drain_proof` in `/autoscaler/info` (`controller.py:4030-4038`). No parallel
metric surface, and the same bounded-key discipline (`_ABORT_REASONS` at
`36-41`).

- `ABORT_REASON_EVIDENCE_GAP = 'evidence_gap'`, added to `_ABORT_REASONS`. Today
  a co-restart abort and a genuine demand rebound are indistinguishable: the
  reactivation reason string at `5760-5762` is `'current ready capacity is below
  the recovered target'`, which matches none of the substrings at `737-746` and
  is counted as `other`. Set `evidence_gap` explicitly at the W2 release site and
  at mechanism C's past-grace reactivation.
- `drain_seconds_discarded_total`, computed inside `_abort_logical_retirement`
  from `time.time() - drain_started_at` **before** the null at `5881`. This is the
  metric the whole design exists to drive down, and it is the one number that
  distinguishes "the wave is progressing" from "the wave is on a treadmill".
- `recovery_holds_opened`, `recovery_holds_adopted`,
  `recovery_holds_released_by_budget`. The first two measure whether a wave
  survives a restart; the third is the W2 escape firing and should be near zero
  once A and C are deployed.
- `logical_target_suppressed_stale`, incremented when W1's new branch fires. If
  this is non-zero outside the first minute after a restart, the freshness
  predicate is behaving differently from this document's model.

All process-local, all reset with the controller, none touching the database,
matching the module docstring's stated contract at `21-24`.

### What is explicitly not changed

`_ReplicaDrainTracker` and its four acknowledgement sets (`749-850`);
`_seed_from_existing_report` (`785-809`); `update_lb_in_flight` (`2047-2073`);
`_logical_ready_capacity` (`5224-5259`) and
`_logical_ready_capacity_by_accelerator`; the coverage check at `5203-5218`;
`_logical_retirement_victim_is_idle`; `_is_committed_logical_retirement`
(`5353-5398`); `_is_recoverable_uncommitted_logical_retirement` (`5401-5452`);
`_is_legacy_uncertain_logical_retirement` (`5454-5481`);
`_defer_scale_down_until_idle` (`5099-5145`); `scale_down_logically_batch`
(`6187-6376`); the down-admission fence and commit write (`7189-7258`);
`_ensure_drain_started_at` (`701-724`); the per-process epoch and its rotation
(`2023`, `4592`); `lb_ha.py`; `lb_k8s.py`; every service-spec schema field.

## Restart behavior

All three cases assume mechanisms A and C are deployed, because that is the
required ship order. Where the behaviour differs without them, it is stated.

### Controller only

The api-server pod is recreated but the LB pods are not, which happens on a
controller-child respawn (`service.py:996-1019`) and on any api-server restart
that does not change the LB pod template digest.

1. The new parent CAS-claims `(controller_pid, controller_ip)` via
   `update_service_controller_pid_if_owner` (`service.py:1676-1684`).
2. `ReplicaManager.__init__` mints a new epoch (`2023`).
   `_recover_replica_operations` re-registers trackers from the durable
   wall-clock anchor (`3022-3032` into `_register_wait_for_idle` at `5058-5097`,
   deadline recomputed at `5074-5084`), and the epoch-mismatch branch at
   `3032-3036` puts every uncommitted member into the recovery hold.
3. W1: until the autoscaler's first fresh-data recompute, no target is published,
   so every member is `'wait'`. Bounded by one decision interval.
4. The LB is warm, so its next sync is not blind. The recovery pass adopts
   (`5791-5839`), re-fencing every member to the new epoch and
   `snapshot.generation` while preserving `drain_cap_seconds` and
   `drain_started_at`.
5. Release requires one strictly newer generation (`5704-5707`), about 20 s.
   Members then evaluate normally. Members already proven idle finish; the rest
   keep draining from where they were.

**Operator view:** `sky serve status` shows the same set of SHUTTING_DOWN
replicas before and after. `/autoscaler/info` shows
`drain_proof.recovery_holds_opened` incrementing by the wave size and
`recovery_holds_adopted` following it about 40 s later, with
`drain_seconds_discarded_total` unchanged. Total cost about 40 to 60 s of a
7200 s budget.

### LB only

An LB pod is evicted, OOMKilled or crash-looped without the controller
restarting.

1. The epoch is unchanged, so no member enters the recovery hold and the recovery
   pass does not run.
2. The new LB's first one or two syncs are blind. Mechanism C's blindness
   predicate suppresses the `'abort'` at `5216-5218`, and `_logical_retirement_state`
   returns `'wait'`. Members stay off route with their drains open.

   **Without C this is where the wave dies**, on a single blind tick, because
   `_refresh_wait_for_idle` acts on `'abort'` immediately at `6039-6043` for every
   member against the same snapshot.
3. The proof session is gone. With mechanism A the controller advertises the
   off-route urls and the fresh LB re-probes them, so first-party proof returns in
   roughly 40 to 70 s. Without A the proof is unobtainable for the remainder of
   that drain and each member runs to its 7200 s deadline and then aborts at
   `6072-6075`.
4. Elapsed drain is untouched throughout: nothing aborted.

**Operator view:** with A and C, a brief stall in terminations and then normal
progress. Without A, `drain_proof.deadline_expiry_without_proof` climbs and
`drain_seconds_discarded_total` climbs with it. That contrast is the whole reason
W3 ships early.

### Co-restart

The measured event: api-server pod at 11:57:47Z, LB pods 11:59:28Z to 12:02:12Z,
roughly hourly. The controller-to-LB gap reaches 4 m 25 s, so evidence can
legitimately take about 330 s to arrive.

- **t+0** Controller boots. Fresh epoch, trackers re-registered from wall clock,
  every uncommitted member into the recovery hold. Adoption and reactivation both
  require a snapshot, and there is none, so gate A at `5651-5665` renews and
  returns.
- **t+0 to one decision interval** W1 suppresses the target. Members are
  `'wait'`. Note this is *stricter* than today, which is the point: today the
  first tick can publish `min_replicas`.
- **t+100 s to t+270 s** LB pods finish rolling. Their first syncs are blind
  (`observed_slots` empty, `force_all_live_unknown` at `controller.py:1073`).
  Mechanism C suppresses the shortfall read, so the reactivation branch at `5727`
  does not fire. **The wave is not touched.**
- **t+150 s to t+330 s** Probe rounds repopulate `total_slots_by_url` and, with
  mechanism A, the off-route urls are back in `_occupancy_capable`. The view is
  sighted, the autoscaler has recomputed with fresh data, and the recovery pass
  adopts the whole wave in one loop.
- **+20 s** One strictly newer generation releases the members
  (`5704-5707`). Members whose proof arrived finish; the rest keep draining with
  their elapsed time intact.
- **Failure branch:** if evidence never arrives, W2 releases each member at its
  own drain budget rather than holding it forever, counted as `evidence_gap`.

Cost per co-restart: about 150 to 350 s of a 7200 s budget, and zero aborts.
Today: 76 aborts within about 80 s and the whole budget discarded.

**Operator view:** during the gap, replicas remain SHUTTING_DOWN and the
reserved-fill gate's cap appears stalled rather than releasing. That is the
correct fail-closed signal and it is exactly what the gate's actuation gate
produces (`reserved_capacity_allocation.py:238-244`, which holds the cap while
`holdings > cap`). `drain_proof.recovery_holds_released_by_budget` staying at 0
is how you tell a slow recovery from a wedged one.

## Relation to the drain-proof design

`docs/designs/serve-drain-proof-across-lb-restarts.md` fixes per-replica *proof*
and the blind-shortfall *read*. This design fixes the *bound* on the hold that
fix creates and the *target* it is evaluated against. They are orthogonal and
both are required.

**A (controller-advertised drain watchlist): composes, and is the completion
engine.** A is the only mechanism that lets a cold LB obtain first-party
occupancy for an already-off-route url. Without it, a wave that survives a roll
still cannot finish: every member runs to its 7200 s deadline and aborts. This
design does not touch A. A's `drain_proof_watchlist` walks durable row state
gated on `wait_for_idle_before_termination`, `logical_retirement_version` and
`sky_down_status`, all of which held members still carry unchanged, so it picks
them up with no modification. **Ship A first.**

**B (absence is never proof for an async-declared replica): untouched.** This
design does not modify `_ReplicaDrainTracker` (`749-850`),
`_seed_from_existing_report` (`785-809`), `update_lb_in_flight` (`2047-2073`),
`_lb_report_authority`, `_publish_ha_drain_view`, `lb_ha.py` or `lb_k8s.py`. The
idleness proof is not weakened by one term, and contract 12 removes the only
mechanism (drain credit) by which it could have been.

**C (a blind capacity view is not evidence of a shortfall): hard prerequisite,
consumed not re-derived.** C's `forced_all_live_unknown` snapshot field, its
`_logical_capacity_view_is_blind` predicate and its
`_LOGICAL_CAPACITY_BLIND_GRACE_SECONDS` ship exactly as C specifies. This design
adds nothing to them and relocates none of their call sites. The relationship
runs the other way: **C needs W2 and W1.** C makes the recovery hold the normal
post-restart outcome, which exercises three unbounded hold paths C does not
address (listed under "What mechanism C does not supply"), and C removes the
accidental mask on the blind demand target. W1 and W2 should land with or before
C.

**C's own stated limit is where W2 lands.** C's Risks section says "The
co-restart case is only partially solved, by design", because
`_refresh_wait_for_idle` still refuses authority for recovering ids
(`6028-6036`) and recovery still requires adoption plus a strictly newer
generation. C's open question 3 asks whether that release fence should be
shortened. This design's answer is no: the fence is correct and cheap (about
20 s), and the actual problem is that the hold in front of it has no floor.
W2 gives it one without touching the fence.

**Milestone 0 (already merged in this tree at `4f33fb9a46`): extended, not
duplicated.** Three counters and one abort reason are added to the existing
`DrainProofStats`.

**Nothing is superseded.** No mechanism, call site, constant or predicate from
the drain-proof design is replaced or relocated by this one.

**Reserved-fill utilization gate: consumer, not dependency.** Its release
governor (`reserved_capacity_allocation.py:196-250`) paces the *grant*, with a
300 s dwell, a 300 s step clock, a 25 % step fraction and a minimum step of 2
(`constants.py:619-628`). Its actuation gate at `238-244` turns a stuck drain
into a visibly stalled cap, which is the correct fail-closed behaviour. This
design does not unify wave pacing with that governor: the governor is opt-in and
per-claimant, while demand-driven target drops and rolling updates have no pacing
at all. See "Rejected alternatives".

## Rejected alternatives

**Rejected: a durable `serve_drain_waves` table (migration 031).** Designed in
full and rejected. It would need a new table on `Base.metadata`, a
Postgres-guarded create following `012_serve_replica_status_history.py`, a
`SERVE_VERSION` bump at `sky/utils/db/migration_utils.py:54`, a delete hook in
the child-deletion loop at `serve_state.py:2801-2807` or wave rows leak on
service delete, and its own CAS surface, because the owner-tuple fence at
`replica_managers.py:2372-2381` covers replica writes only. What it buys is
wave-level accounting, and by its own construction that accounting is audit-only:
authorization still comes from `_logical_retirement_state` at `5899` and `7231`,
so deleting the wave row would not prevent a single kill. Worse, the accounting
is *wrong* for the largest batches it would ever see, because victims with
`has_served == False` are killed inside the wave-creating pass, either by the
bulk delete at `6226-6253` or by `_terminate_replica(...,
in_flight_drain_cap_seconds=0)` at `6334-6338`, and never pass through
`_defer_scale_down_until_idle`, so they never become members. A 90-replica failed
logical launch would record 2 members and 0 terminations while destroying 88
replicas. A table that is audit-only and whose audit is wrong is not worth a
central migration in a handle-with-care file.

**Rejected: a durable kill budget ("intended 76, killed 31, 45 remain
authorized").** This is the only thing a wave row can carry that the replica rows
cannot, and it is the reason to reject the table rather than a reason to build
it. A budget is durable *intent*, and a resumed wave acting on intent formed
before a restart is precisely failure class 1: a 76-replica reclaim wave that
resumes against a pre-restart target can take the service to its floor and past
it. The design as it stands carries no intent forward at all, only identity and
evidence, so every irreversible step re-derives coverage from the current target
(contract 1). That is strictly safer, and it is free.

**Rejected: preserving `drain_started_at` across an abort, as a credit or
otherwise.** This is the change the brief suggests and it is the most dangerous
piece considered. Three reasons.

First, it does not fix what it claims to. For a same-version wave victim, elapsed
drain is not progress toward completion: completion requires an idle proof from
the LB, and the deadline expiring leads to `_abort_logical_retirement` at
`6072-6075`, not to a termination. Preserving elapsed time makes the next attempt
give up *sooner*, not finish sooner.

Second, the scoping cannot be enforced where it matters.
`_ensure_drain_started_at` (`701-724`) is the natural consumption site because it
owns the wall-clock anchor, and it has four callers: `4750` (launch-cancel
teardown), `4913` (the physical drain worker, whose deadline at `4922` bounds
`_wait_for_drain` and whose expiry force-terminates), `5074`
(`_register_wait_for_idle`) and `5129` (`_defer_scale_down_until_idle`, shared by
the logical and the plain strict-drain paths). Three of the four end in a kill.
Restricting who *writes* a credit does not restrict who *spends* it, and the
credit is a durable field that survives until consumed. A member credited after a
blind-window abort, returned to routing, given fresh async work, and then retired
by an unrelated `sky serve update` takes the outdated-version bounded path at
`6052-6071`, which calls `_finish_logical_retirement(require_victim_idle=False)`
and terminates with `in_flight_drain_cap_seconds=0`: live jobs killed on a
credit-shortened deadline with no idle proof. That violates the hard constraint
that killing real user jobs is worse than holding a GPU.

Third, the proposed invalidator is dark exactly when it is needed. Voiding the
credit on "observed with non-zero `in_flight_by_replica_id` since the abort"
requires LB reports, and every abort this design exists to survive originates in
an observability gap where the LB reports nothing. The correlation is adverse,
not neutral.

The correct statement is the one in contract 12: an abort returns the victim to
routing, where it serves again, so a fresh full budget is correct. The bug is
that aborts fire for reasons that are not real, and that is mechanism C's job.

**Rejected: replacing the per-process epoch with a durable lineage token**
(`f'lin:{service_hash}:{lifecycle_epoch}'`). The diagnosis is right and the
generation-floor companion is sound, but the value is small and the coupling is
fragile. Value: it saves the recovery detour, worth about 40 to 60 s per restart
out of a 3300 s cycle. Fragility: `services.lifecycle_epoch` advances on every
up, update, down and purge lock acquisition (`serve_state.py:81-87`), including a
failed apply, none of which restart the controller, so a fleet that runs apply on
a cadence would make its whole live wave foreign-lineage at the next restart and
silently degrade to today. Risk: the token's real payload is that it lets the
recovery hold be removed, and removing the hold removes the two guards at
`6028-6036` and `7200-7210` that stop `_refresh_wait_for_idle` from converting
elapsed wall time into readmission authority. Without those, a member whose
budget elapses during the post-restart evidence gap is aborted at `6072-6075`
having consulted no snapshot, no target and no capacity evidence at all. Keeping
the hold and bounding it (W2) gets the safety for one predicate and no new
identity.

**Rejected: seeding `_reconcile_generation` from a persisted floor at boot.** The
companion to the lineage token, and it is the piece that would make persisted
generation integers comparable across incarnations. It is correct in isolation
(the floor is a max over exactly the rows that carry those integers, and the
owner tuple is CAS-claimed before the controller child is spawned, so no
concurrent writer can beat the read), but it is only useful if the epoch is
being replaced, and it introduces a failure mode where a partial or filtered
read under-seeds the counter and a stale confirmation is read as newer evidence,
which is the zero-drain teardown in contract 8. Not worth it for a benefit the
adoption path already delivers by re-stamping
`logical_retirement_generation = snapshot.generation` at `5809`.

**Rejected: moving coverage failure from `'abort'` to `'wait'` with a single
readmission owner.** Structurally attractive: it separates the safe act
(readmit) from the unsafe one (destroy intent), and it would stop one blind tick
reverting an entire wave. But it concentrates all readmission authority in one
new function, and that function inherits the per-card skip at `5754-5759`, so a
shortfall on a card no victim carries readmits nobody and the victims are then
neither terminable nor readmittable until their drain deadline. Today the same
situation aborts every victim in one tick and capacity returns immediately. The
blind-tick problem it solves is mechanism C's job, and C solves it without moving
any readmission authority.

**Rejected: a pause/cancel split of `_abort_logical_retirement`.** Same insight,
same objection, plus the specification hazard that "pause" is both an action that
returns a victim to routing and a return value meaning "take no action", and the
two readings give opposite restart behaviour. If the epoch-mismatch line `5171`
returns the action, every member of every wave is readmitted once per restart,
including the ones sitting SCHEDULED-uncommitted behind the 64-way down budget
(`_MAX_CONCURRENT_DOWNS_PER_SERVICE` at line 105, enforced at `7191-7192`), which
are the members closest to completion.

**Rejected: capping or pacing wave size.** 76 victims in one tick is intended
behaviour: selection is bounded only by the capacity ledger
(`autoscalers.py:6924-6982`), and the only downstream limits bound teardowns, not
off-route replicas. A per-wave admission rate is defensible but it is a policy
change with its own argument, it belongs next to the reserved-fill release
governor (`reserved_capacity_allocation.py:196-250`) which already paces the
grant, and smuggling it into a durability fix would confound the measurement of
whether the durability fix worked.

**Rejected: a manager-side demand-freshness conjunct instead of W1's
controller-side gate.** Adding "the demand report is fresh" as a seventh conjunct
to `_logical_retirement_state` (`5146-5223`) would work, but it needs a
cross-thread read of autoscaler state from inside `_logical_state_lock`, on a
path that already does a full `serve_state.get_replica_infos` under that lock at
`5203`. The controller-side gate is three lines, sits where the target is already
computed and published, and fails closed through the existing
`invalidate_logical_target()` path.

## Risks

**The wave still does not complete without mechanism A.** W1 and W2 make the wave
survive every roll with its elapsed drain intact. They do not obtain a drain
proof for an off-route url behind a fresh LB, and without one every member runs
to its 7200 s deadline and aborts. Shipping W1 and W2 alone converts "the wave is
destroyed every 55 minutes" into "the wave survives and completes only for
members whose proof the current LB session can still see". That is strictly
better and it is measurably visible in `drain_seconds_discarded_total`, but it is
not done. This is the single most likely way this work lands and disappoints.

**W1 lengthens the post-restart hold, and its correctness rests on a predicate
that lives in the autoscaler.** If `has_recomputed_with_fresh_data()` ever fails
to flip for a service, that service's logical retirement freezes until W2's
budget bound releases it. The base class returns `True` unconditionally
(`autoscalers.py:1365-1372`) and the only override is
`ConcurrencyAutoscaler`'s at `4238-4250`, so the exposure is one class today, but
a future autoscaler that forgets the override inherits `True` and silently loses
the protection rather than freezing. Prefer that direction, and pin it with a
test that the base-class default is `True`.

**W2's release aborts the whole remaining wave at once.** When evidence has been
unavailable for a full `graceful_drain_seconds`, up to 76 replicas return to
routing in one pass, deliberately unpaced. That is the safe direction (serving
beats holding) and it is the same magnitude as today's reactivation spread over
four generations, but it is a large routing change and it will look alarming the
first time it fires. `recovery_holds_released_by_budget` should be alerted on,
not merely logged.

**W2 makes the treadmill bounded, not absent.** A member released at its budget
returns to routing and is re-selected on the next tick with a fresh 7200 s. Under
a genuine multi-hour evidence outage that is an infinite loop with a 2-hour
period. It is bounded, observable and strictly better than an infinite hold, and
the correct fix for the underlying condition is A, not more machinery here.

**No wave-level accounting exists, by construction.** "Did wave 7 finish", "how
many did it kill", "how many restarts has it survived" are unanswerable except by
inference from replica rows and the process-local counters, which reset with the
controller. That is the price of contract 13 and it is paid knowingly.

**`sky/serve/replica_managers.py` is on the repo CLAUDE.md handle-with-care
list.** W2 edits the triage loop of a 230-line recovery function and adds one
optional argument to the abort. The edits are small, but they are in the region
where every line already carries a comment explaining a subtle invariant, and the
triage loop runs before every evidence gate, so a bug there affects every held
member on every pass.

**No smoke coverage is possible.** The dominant event is a live LB roll
coincident with an api-server restart, which `tests/smoke_tests/test_sky_serve.py`
cannot reproduce. Validation is unit tests plus fleet observation against the
Milestone 0 and W3 counters, so a wrong bound would be discovered in production.

**W1's masked hazard existed before this design and is being un-masked by C, not
by W1.** If C ships before W1, the window between "the capacity view becomes
sighted" and "the autoscaler completes its first fresh-data recompute" is a live
mass-teardown window for outdated-version members, which take the bounded path
with no idle proof. That is the strongest reason W1 must not lag C.

## Milestones

Ordered by dependency. Each is independently shippable and independently
revertable. Total effort about 2 days, against weeks for a durable wave table.

**W1: fresh-demand gate on the published logical target. About 0.5 day.**
`sky/serve/controller.py:3763-3772` (one branch),
`sky/serve/drain_observability.py` (one counter). No dependency on A or C, safe
to ship first, and it closes a lost-capacity hazard that exists today under a
narrow race and becomes wide the moment C lands. Ship this before or with
mechanism C, never after.

**W2: bounded recovery hold. About 1 day.**
`sky/serve/replica_managers.py`: `_logical_recovery_budget_exhausted` (new,
about 10 lines), the triage-loop branch at `5626-5641`, the optional `reason_key`
on `_abort_logical_retirement` at `5858-5891`, and the log-and-renew fix for the
pending-version gate at `5682-5684`. No dependency on A or C. Ship with or before
C, because C makes the hold the normal outcome.

**W3: wave counters. About 0.5 day.** `sky/serve/drain_observability.py` plus the
increment sites. Not a gate on anything, but it is the only way to tell whether
any of this worked, and `drain_seconds_discarded_total` is the number that
settles it. Ship it first if you want the before-and-after comparison, which you
do.

**Ship order across both designs:** M0 (already merged) -> W3 -> W1 -> W2 ->
drain-proof M1 (A + B) -> drain-proof M2 (C). The two hard constraints are
inherited from the drain-proof design (C must not precede A) and added here
(W1 and W2 must not follow C).

**Which milestone unblocks the reserved-capacity utilization gate on protenix.**
`serve-reserved-fill-utilization-gate.md:19-38` already blocks enabling on
`protenixv2-hybrid-v1` until drain-proof M1 and M2 are deployed. This design adds
**W1 and W2 as a third precondition**, because the reclaim wave is the one that
spans hours and therefore spans restarts, and because C without W1 opens the
mass-teardown window described above. The full gate for
`reserved_capacity_fill.utilization_gate: true` on protenix is therefore: W1 and
W2 deployed, then drain-proof M1, then drain-proof M2, then one observation
window in which `drain_seconds_discarded_total` stays flat across at least three
control-plane deploys. W3 is not a gate but it is what makes that observation
window meaningful.

## Test plan

Logic only. No assertions on log or error message text. Extend these existing
files; no new test file is needed.

**`tests/unit_tests/test_serve_restart_bounded_drain_resume.py`** (has
`_make_manager` at line 10, `_bounded_precommit_info` at 39, `_restart_manager`
at 66, `test_probe3_shortfall_reactivation_still_works` at 148,
`test_same_total_exact_card_shift_reactivates_required_card` at 169):

- Contract 4: with no snapshot and no target ever supplied, a recovering member
  whose `drain_started_at` plus `drain_cap_seconds` is in the past is not in
  `_recovering_logical_retirement_ids` after one
  `_reconcile_recovering_logical_retirements`, and its `is_scale_down` is
  `False`.
- Contract 4, the unevaluable case: a recovering member with
  `drain_cap_seconds = None` is released on the first pass.
- Contract 5: for each of the four evidence gates, drive the pass with inputs
  that hit that gate and assert the member is still released once its budget
  elapses. Parametrize over the four gate conditions rather than writing four
  tests.
- Contract 4, negative: a recovering member with budget remaining is *not*
  released, on every gate.
- Contract 6: run `_restart_manager`, supply a fresh non-blind snapshot and
  target, and assert `drain_cap_seconds`, `drain_started_at` and
  `wait_for_idle_before_termination` are equal before and after adoption, and
  that `logical_retirement_controller_epoch` changed.
- Contract 8: a member persisted with `logical_retirement_generation = 5000`
  under a dead epoch is not released to admission by a fresh manager whose
  snapshot generation is 3.
- Extend `test_probe3_shortfall_reactivation_still_works` so it keeps asserting
  today's reactivation semantics when the budget is *not* exhausted, which pins
  that W2 did not change the shortfall path.
- The per-card strand, which motivates W2: an L4 card shortfall against
  A100-only recovering candidates readmits zero members on every generation, and
  the members are released only at their budget. Build it from
  `test_same_total_exact_card_shift_reactivates_required_card`'s fixtures.

**`tests/unit_tests/test_serve_replica_managers.py`** (already exercises
`_logical_retirement_state` and `_logical_ready_capacity`):

- Contract 3: with `_logical_target` `None`, `_logical_retirement_state` returns
  `'wait'` for a member whose fence is otherwise entirely current, and
  `scale_down_logically_batch` accepts zero victims.
- Contract 1: with a wave of N members held and a target the remaining fleet
  cannot cover, no member reaches `'safe'` at any of the three consumers.
- Contract 12: after `_abort_logical_retirement` and a re-selection through
  `_defer_scale_down_until_idle`, `drain_started_at` is `now`, not the prior
  value, and no credit-like field exists on the row.
- `_abort_logical_retirement` with an explicit `reason_key` records that key, and
  with `reason_key=None` still records `_classify_abort_reason`'s result.

**`tests/unit_tests/test_serve_controller.py`**:

- Contract 3, publish side: an autoscaler stub whose
  `has_recomputed_with_fresh_data()` is `False` results in
  `invalidate_logical_target()` and never `publish_logical_target()`, even when
  `logical_target_state` is a well-formed tuple.
- The same stub returning `True` publishes exactly the tuple.
- The existing exact-card revoke branch (`target_state is None` plus
  `configured_accelerator_shapes`) still invalidates, so W1 did not change it.

**`tests/unit_tests/test_serve_autoscaler.py`**:

- `has_recomputed_with_fresh_data()` is `True` on the base class without any
  report, and `False` on a freshly constructed `ConcurrencyAutoscaler` until a
  recompute consumes a fresh report. This is the pin for the risk that a future
  autoscaler silently loses W1's protection.

**`tests/unit_tests/test_serve_graceful_drain.py`** (`TestReplicaDrainTracker` at
line 211): unchanged. Assert nothing new. Its presence in this plan is a
statement that contract 12 and mechanism B leave the idleness proof untouched;
if any test in this file needs editing, the change has gone wrong.

**Smoke:** none. Stated in "Risks".

## Open questions

1. **Should W2's release be paced after all?** It is unpaced on the argument that
   a budget-exhaustion release is not speculative. If a 76-replica simultaneous
   return to routing turns out to destabilise the LB's routing view or the
   autoscaler's next tick, reusing the 20-per-generation bound at line 91 is a
   one-line change. Decide after the first time
   `recovery_holds_released_by_budget` fires in production.

2. **Is the drain budget the right horizon for the hold, or should it be
   shorter?** The budget is 7200 s on protenix, so a genuinely wedged LB can hold
   76 A100s for two hours before W2 fires. A shorter horizon would return capacity
   sooner but would destroy waves during long-but-legitimate evidence gaps, and
   the measured controller-to-LB gap already reaches 265 s. The budget is the
   service's own statement of how long it will hold a GPU off route, which is why
   it was chosen, but an operator may want a separate, shorter recovery horizon
   for the specific case where *no* snapshot has ever arrived in this
   incarnation.

3. **Does W1 need a companion on the scale-up side?** W1 suppresses the target
   for retirement purposes. `_last_logical_target_state` is also the fence for
   `scale_up_to_logical_capacity`, and this design does not change that path
   because growth on a blind target is the safe direction. Worth confirming that
   no scale-up path reads the suppressed target and interprets `None` as zero.

4. **Should `evidence_gap` be applied to mechanism C's past-grace reactivation
   too?** This design assumes yes, so that a blind-driven abort is never counted
   as `target_coverage`. That is a change inside C's call site, so it needs to be
   agreed with whoever lands C rather than assumed here.

5. **Is `_is_legacy_uncertain_logical_retirement`'s separate reconcile
   (`5454-5481`, driven from `5483` onward) also unbounded?** It was not audited
   for this design because those rows predate the commit bit and should not exist
   in production any more. If any remain, they need the same budget bound.

6. **Does the reclaim workstream ever retire outdated-version replicas?** Every
   argument here about the bounded no-idle-proof path at `6052-6071` assumes the
   reclaim retires current-version replicas only, so its members always take the
   abort branch at `6072-6075` rather than the completion branch. If a reclaim
   path can select an outdated member, that member is exposed to W1's hazard for
   the whole window and the two populations must be described separately.
