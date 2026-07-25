"""Process-local counters for the replica drain and retirement paths.

[boltz fork] Milestone 0 of docs/designs/serve-drain-proof-across-lb-restarts.md.
Counters only, no behavior change, so the model in that design can be
calibrated against production before and after the fix lands.

The question these answer is which of two very different costs actually
dominates when a load balancer restarts mid-drain:

- a drain that runs to its full ``graceful_drain_seconds`` because the
  restarted load balancer can never re-acknowledge an already-off-route
  replica (``deadline_expiry_without_proof``), or
- a whole retirement wave that is ABORTED and returned to routing because a
  blind capacity view reads as a shortfall (``logical_abort`` keyed by
  reason, plus ``blind_ready_capacity``).

The second is believed to dominate on the logical path, and no production
evidence has ever shown the first being paid there. Until these counters
exist, that remains a code reading rather than a measurement.

Deliberately process-local and unbounded-key-free: values are surfaced
through the controller's existing ``/autoscaler/info`` response, so they
reset with the controller and never touch the database.
"""
import collections
from typing import Any

# Bounded, closed set. Reasons are supplied by call sites in this repo, never
# by user input, but keying a Counter on an unbounded string would still be a
# slow leak in a long-lived controller.
ABORT_REASON_TARGET_COVERAGE = 'target_coverage'
ABORT_REASON_FENCE_CHANGED = 'fence_changed'
ABORT_REASON_IDLE_PROOF_TIMEOUT = 'idle_proof_timeout'
ABORT_REASON_OTHER = 'other'

_ABORT_REASONS = frozenset({
    ABORT_REASON_TARGET_COVERAGE,
    ABORT_REASON_FENCE_CHANGED,
    ABORT_REASON_IDLE_PROOF_TIMEOUT,
    ABORT_REASON_OTHER,
})


class DrainProofStats:
    """Counters for one service's drain and logical-retirement outcomes."""

    def __init__(self) -> None:
        # A strict idle wait that reached its deadline with no zero-occupancy
        # proof and fell back to a bounded graceful drain. This is the cost
        # the 7200s cap represents.
        self._deadline_expiry_without_proof = 0
        # A drain that completed because the load balancer proved the replica
        # idle. The ratio against the counter above is the headline number.
        self._proved_drained = 0
        # Logical retirements aborted, by reason. An abort returns the victim
        # to routing and discards its elapsed drain, so a wave that keeps
        # aborting makes no progress no matter how long it runs.
        self._logical_aborts: collections.Counter[str] = collections.Counter()
        # Bounded rolling-update retirements completed at their deadline
        # without an idle proof (replacement capacity already covered target).
        self._bounded_completions = 0
        # Rounds in which ready-capacity accounting had to skip at least one
        # same-version candidate because its occupancy was unobserved or
        # explicitly unknown. This is the blind view that drives
        # target_coverage aborts; skipped_replicas is the total across rounds.
        self._blind_capacity_rounds = 0
        self._blind_capacity_skipped_replicas = 0

    def record_deadline_expiry_without_proof(self) -> None:
        self._deadline_expiry_without_proof += 1

    def record_proved_drained(self) -> None:
        self._proved_drained += 1

    def record_logical_abort(self, reason: str) -> None:
        if reason not in _ABORT_REASONS:
            reason = ABORT_REASON_OTHER
        self._logical_aborts[reason] += 1

    def record_bounded_completion(self) -> None:
        self._bounded_completions += 1

    def record_blind_ready_capacity(self, skipped_replicas: int) -> None:
        if skipped_replicas <= 0:
            return
        self._blind_capacity_rounds += 1
        self._blind_capacity_skipped_replicas += skipped_replicas

    def snapshot(self) -> dict[str, Any]:
        return {
            'deadline_expiry_without_proof':
                self._deadline_expiry_without_proof,
            'proved_drained': self._proved_drained,
            'logical_aborts': dict(sorted(self._logical_aborts.items())),
            'logical_aborts_total': sum(self._logical_aborts.values()),
            'bounded_completions': self._bounded_completions,
            'blind_capacity_rounds': self._blind_capacity_rounds,
            'blind_capacity_skipped_replicas':
                self._blind_capacity_skipped_replicas,
        }
