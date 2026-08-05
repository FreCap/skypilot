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

from sky import exceptions
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
    """Uncontended observations and forced refreshes keep normal behavior."""

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

    def test_busy_retry_losing_generation_reports_its_successful_read(self):
        """Retirement waiting must not reintroduce observation starvation."""
        _reset_cache()
        first_waiting = threading.Event()
        allow_first_retry = threading.Event()
        second_read_started = threading.Event()
        allow_second_finish = threading.Event()
        calls = 0
        calls_lock = threading.Lock()
        busy = exceptions.KubernetesPhysicalClusterFenceBusyError(
            'captured', _CONTEXT, 7)

        def _read_namespace(_name, **_kwargs):
            nonlocal calls
            with calls_lock:
                calls += 1
                nth = calls
            if nth == 1:
                raise busy
            if nth == 2:
                # The newer lookup owns generation 2 but has not published.
                second_read_started.set()
                assert allow_second_finish.wait(timeout=10)
            return mock.MagicMock(metadata=mock.MagicMock(uid=_UID))

        def _wait_for_retirement(*_args):
            first_waiting.set()
            assert allow_first_retry.wait(timeout=10)
            return True

        api = mock.MagicMock()
        api.read_namespace.side_effect = _read_namespace
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               mock.MagicMock(return_value=api)), \
             mock.patch.object(
                 reserved_capacity.kubernetes,
                 'wait_for_physical_cluster_uid_fence_retirement',
                 side_effect=_wait_for_retirement) as wait_for_retirement:
            with concurrent.futures.ThreadPoolExecutor(2) as pool:
                first = pool.submit(
                    reserved_capacity.get_kubernetes_physical_cluster_uid,
                    _CONTEXT)
                assert first_waiting.wait(timeout=10)
                second = pool.submit(
                    reserved_capacity.get_kubernetes_physical_cluster_uid,
                    _CONTEXT)
                assert second_read_started.wait(timeout=10)
                allow_first_retry.set()
                first_result = first.result(timeout=10)
                allow_second_finish.set()
                second_result = second.result(timeout=10)

        assert first_result == _UID
        assert second_result == _UID
        wait_for_retirement.assert_called_once()


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


class TestLaunchFenceIsNotStarved:
    """The pre-launch guard reads this value; None refuses every fill launch.

    `_authorize_reserved_fill_launch` calls this with force_refresh=True and
    compares the result to the pinned pool UID, so a lookup that returns None
    is reported as `fill-physical-cluster-uid-mismatch`. On the deployed fleet
    that refused every fill launch while the broker was feeding 74-90 slots.
    """

    def test_a_forced_early_finisher_reports_its_own_read(self):
        # Same interleaving as the observation-path case: the earlier lookup
        # finishes first, while the later one is still inside read_namespace.
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
                second_started.wait(timeout=10)
            else:
                second_started.set()
                first_may_finish.wait(timeout=10)
            return mock.MagicMock(metadata=mock.MagicMock(uid=_UID))

        api = mock.MagicMock()
        api.read_namespace.side_effect = _read_namespace
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               mock.MagicMock(return_value=api)):
            with concurrent.futures.ThreadPoolExecutor(2) as pool:
                first = pool.submit(
                    reserved_capacity.get_kubernetes_physical_cluster_uid,
                    _CONTEXT,
                    force_refresh=True)
                second_started.wait(timeout=10)
                second = pool.submit(
                    reserved_capacity.get_kubernetes_physical_cluster_uid,
                    _CONTEXT,
                    force_refresh=True)
                first_result = first.result(timeout=10)
                first_may_finish.set()
                second_result = second.result(timeout=10)

        # A None here is what the guard turns into
        # 'fill-physical-cluster-uid-mismatch'.
        assert first_result == _UID
        assert second_result == _UID

    def test_the_guard_authorizes_a_matching_pin(self):
        # End to end through the comparison the guard actually performs.
        _reset_cache()
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               _core_api()):
            observed = reserved_capacity.get_kubernetes_physical_cluster_uid(
                _CONTEXT, force_refresh=True)
        pinned = _UID
        assert observed == pinned  # guard returns (True, 'authorized')

    def test_a_real_retarget_is_still_refused(self):
        # The fence must still catch a context pointed at another cluster.
        _reset_cache()
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               _core_api(uid='different-cluster-uid')):
            observed = reserved_capacity.get_kubernetes_physical_cluster_uid(
                _CONTEXT, force_refresh=True)
        assert observed != _UID  # guard returns the mismatch verdict

    def test_superseded_forced_busy_retry_reports_its_successful_read(self):
        """A successful post-retirement read must not starve the guard."""
        _reset_cache()
        first_waiting = threading.Event()
        allow_first_retry = threading.Event()
        second_read_started = threading.Event()
        allow_second_finish = threading.Event()
        calls = 0
        calls_lock = threading.Lock()
        busy = exceptions.KubernetesPhysicalClusterFenceBusyError(
            'captured', _CONTEXT, 11)

        def _read_namespace(_name, **_kwargs):
            nonlocal calls
            with calls_lock:
                calls += 1
                nth = calls
            if nth == 1:
                raise busy
            if nth == 2:
                second_read_started.set()
                assert allow_second_finish.wait(timeout=10)
            return mock.MagicMock(metadata=mock.MagicMock(uid=_UID))

        def _wait_for_retirement(*_args):
            first_waiting.set()
            assert allow_first_retry.wait(timeout=10)
            return True

        api = mock.MagicMock()
        api.read_namespace.side_effect = _read_namespace
        with mock.patch.object(reserved_capacity.kubernetes, 'core_api',
                               mock.MagicMock(return_value=api)), \
             mock.patch.object(
                 reserved_capacity.kubernetes,
                 'wait_for_physical_cluster_uid_fence_retirement',
                 side_effect=_wait_for_retirement) as wait_for_retirement:
            with concurrent.futures.ThreadPoolExecutor(2) as pool:
                first = pool.submit(
                    reserved_capacity.get_kubernetes_physical_cluster_uid,
                    _CONTEXT,
                    force_refresh=True)
                assert first_waiting.wait(timeout=10)
                second = pool.submit(
                    reserved_capacity.get_kubernetes_physical_cluster_uid,
                    _CONTEXT,
                    force_refresh=True)
                assert second_read_started.wait(timeout=10)
                allow_first_retry.set()
                first_result = first.result(timeout=10)
                allow_second_finish.set()
                second_result = second.result(timeout=10)

        assert first_result == _UID
        assert second_result == _UID
        wait_for_retirement.assert_called_once()
