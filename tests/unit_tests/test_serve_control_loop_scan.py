"""Regression tests for the canonical SkyServe mutation-admission surface.

Provider mutations are admitted by the PostgreSQL transactions in
``serve_state.reserve_replica_*_running_if_capacity``.  The controller utility
layer only derives the independent P and D limits; it must not grow a second
read-then-write admission predicate that can race the database transaction.
"""

from sky.utils import controller_utils


def test_launch_and_termination_limits_are_independent(monkeypatch):
    monkeypatch.setattr(controller_utils, '_get_request_parallelism',
                        lambda pool: 7 if pool else 5)

    assert controller_utils.get_serve_launch_limit(pool=False) == 5
    assert controller_utils.get_serve_termination_limit(
        pool=False) == (5 * controller_utils.SERVE_TERMINATIONS_PER_LAUNCH_SLOT)
    assert controller_utils.get_serve_launch_limit(pool=True) == 7
    assert controller_utils.get_serve_termination_limit(
        pool=True) == (7 * controller_utils.SERVE_TERMINATIONS_PER_LAUNCH_SLOT)


def test_no_read_then_write_mutation_admission_predicates():
    assert not hasattr(controller_utils, 'get_serve_mutation_counts')
    assert not hasattr(controller_utils, 'can_provision')
    assert not hasattr(controller_utils, 'can_terminate')
