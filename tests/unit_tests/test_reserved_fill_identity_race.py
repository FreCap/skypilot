"""Concurrent identity lookups must not starve the reserved-fill pool.

`get_kubernetes_physical_cluster_uid` stamps each lookup with a generation,
runs the (slow) `read_namespace` outside the cache lock, and on return
refuses to publish if a newer generation started meanwhile. The fence is
there so a launch-time check cannot accept an identity captured before a
context retarget.

The observation path pays for it: with several fill-enabled services polling
the same context, the loser of that race returns None even though its own
read succeeded, and `resolve_fill_pool_specs` drops the pool edge for the
cycle. Measured on the running controller, 2 of 4 concurrent lookups
returned None while every serial lookup succeeded, which switched fill off
for the whole fleet.
"""
# pylint: disable=protected-access
import concurrent.futures
import threading
from unittest import mock

from sky.serve import reserved_capacity

_CONTEXT = 'prod_research_cluster_eks'
_UID = '14de98b4-cb7b-4f82-beb7-6f754a96f1dd'


def _reset_cache():
    with reserved_capacity._PHYSICAL_CLUSTER_UID_CACHE_LOCK:
        reserved_capacity._PHYSICAL_CLUSTER_UID_CACHE.clear()
        reserved_capacity._PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS.clear()


def _core_api(uid=_UID, barrier=None):
    """A core_api whose read_namespace optionally synchronizes callers."""

    def _read_namespace(_name, **_kwargs):
        if barrier is not None:
            # Hold every concurrent reader inside the slow call, which is
            # exactly the window the generation stamp is taken outside of.
            barrier.wait(timeout=10)
        return mock.MagicMock(metadata=mock.MagicMock(uid=uid))

    api = mock.MagicMock()
    api.read_namespace.side_effect = _read_namespace
    return mock.MagicMock(return_value=api)


class TestSerialLookupsSucceed:

    def test_a_single_lookup_resolves(self):
        _reset_cache()
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               _core_api()):
            assert reserved_capacity.get_kubernetes_physical_cluster_uid(
                _CONTEXT) == _UID

    def test_repeated_forced_lookups_all_resolve(self):
        _reset_cache()
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               _core_api()):
            results = [
                reserved_capacity.get_kubernetes_physical_cluster_uid(
                    _CONTEXT, force_refresh=True) for _ in range(5)
            ]
        assert results == [_UID] * 5


class TestConcurrentObservationLookups:
    """The observation path must not lose its own successful read.

    The damaging interleaving is precise: the EARLIER lookup must also
    FINISH first, while the later one is still inside `read_namespace`.
    It then finds a newer generation stamped, an empty cache, and fails
    closed on a read that succeeded.
    """

    def test_an_early_finisher_does_not_discard_its_own_read(self):
        _reset_cache()
        first_may_finish = threading.Event()
        second_started = threading.Event()
        calls = []
        calls_lock = threading.Lock()

        def _read_namespace(_name, **_kwargs):
            with calls_lock:
                calls.append(1)
                nth = len(calls)
            if nth == 1:
                # Hold until the second lookup has stamped its generation.
                second_started.wait(timeout=10)
            else:
                second_started.set()
                # Let the first one return before this one publishes.
                first_may_finish.wait(timeout=10)
            return mock.MagicMock(metadata=mock.MagicMock(uid=_UID))

        api = mock.MagicMock()
        api.read_namespace.side_effect = _read_namespace
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               mock.MagicMock(return_value=api)):
            with concurrent.futures.ThreadPoolExecutor(2) as pool:
                first = pool.submit(
                    reserved_capacity.get_kubernetes_physical_cluster_uid,
                    _CONTEXT)
                second_started.wait(timeout=10)
                second = pool.submit(
                    reserved_capacity.get_kubernetes_physical_cluster_uid,
                    _CONTEXT)
                first_result = first.result(timeout=10)
                first_may_finish.set()
                second_result = second.result(timeout=10)

        # The first reader's own read succeeded against the same live
        # cluster; returning None takes the fill pool edge down for a cycle.
        assert first_result == _UID
        assert second_result == _UID

    def test_every_concurrent_observer_resolves(self):
        _reset_cache()
        workers = 4
        barrier = threading.Barrier(workers)
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               _core_api(barrier=barrier)):
            with concurrent.futures.ThreadPoolExecutor(workers) as pool:
                results = list(
                    pool.map(
                        lambda _: reserved_capacity.
                        get_kubernetes_physical_cluster_uid(_CONTEXT),
                        range(workers)))
        assert results == [_UID] * workers


class TestFailuresStillFailClosed:
    """Removing the starvation must not soften a real lookup failure."""

    def test_a_failed_read_returns_none(self):
        _reset_cache()
        api = mock.MagicMock()
        api.read_namespace.side_effect = RuntimeError('forbidden')
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               mock.MagicMock(return_value=api)):
            assert reserved_capacity.get_kubernetes_physical_cluster_uid(
                _CONTEXT) is None

    def test_an_empty_uid_returns_none(self):
        _reset_cache()
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               _core_api(uid='')):
            assert reserved_capacity.get_kubernetes_physical_cluster_uid(
                _CONTEXT) is None

    def test_a_failed_read_does_not_serve_an_expired_identity(self):
        _reset_cache()
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               _core_api()):
            assert reserved_capacity.get_kubernetes_physical_cluster_uid(
                _CONTEXT) == _UID
        api = mock.MagicMock()
        api.read_namespace.side_effect = RuntimeError('forbidden')
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               mock.MagicMock(return_value=api)):
            assert reserved_capacity.get_kubernetes_physical_cluster_uid(
                _CONTEXT, force_refresh=True) is None


class TestRetargetIsStillFenced:
    """A forced launch-time check must not accept a superseded identity."""

    def test_forced_refresh_reports_the_new_identity(self):
        _reset_cache()
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               _core_api(uid='old-uid')):
            assert reserved_capacity.get_kubernetes_physical_cluster_uid(
                _CONTEXT) == 'old-uid'
        # The context is retargeted at a different physical cluster.
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               _core_api(uid='new-uid')):
            assert reserved_capacity.get_kubernetes_physical_cluster_uid(
                _CONTEXT, force_refresh=True) == 'new-uid'
