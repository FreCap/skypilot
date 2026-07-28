"""Tests for external-only controller-child supervision."""
# pylint: disable=missing-class-docstring,protected-access
from unittest import mock

from sky.serve import constants
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import service

_SET_STATUS_METHOD = 'set_service_status_and_active_versions_if_owner'


class TestBackoff:

    def test_backoff_grows_and_caps(self):
        first = service._child_respawn_backoff_seconds(1)
        second = service._child_respawn_backoff_seconds(2)
        huge = service._child_respawn_backoff_seconds(1000)
        assert first == service._CHILD_RESPAWN_BACKOFF_BASE_SECONDS
        assert second == 2 * first
        assert huge == service._CHILD_RESPAWN_BACKOFF_CAP_SECONDS


class _FakeProc:

    def __init__(self, alive=True, error=None):
        self._alive = alive
        self._error = error
        self.pid = 123

    def is_alive(self):
        if self._error is not None:
            raise self._error
        return self._alive


class TestControllerReplacementTrigger:

    def test_live_child_is_never_replaced_for_elapsed_health_miss(self):
        process = _FakeProc(alive=True)
        for unused_elapsed_seconds in (60, 300, 3600):
            assert not service._controller_child_needs_respawn('svc', process)

    def test_dead_child_needs_respawn(self):
        assert service._controller_child_needs_respawn('svc',
                                                       _FakeProc(alive=False))

    def test_missing_child_handle_fails_closed(self):
        assert not service._controller_child_needs_respawn('svc', None)

    def test_liveness_error_fails_closed(self):
        assert not service._controller_child_needs_respawn(
            'svc', _FakeProc(error=RuntimeError('unknown')))

    def test_live_child_health_miss_is_graced_with_healthy_data_plane(self):
        assert service._controller_health_miss_is_graced(
            controller_responding=False,
            controller_needs_respawn=False,
            external_lb_healthy=True)

    def test_health_miss_is_not_graced_for_dead_child(self):
        assert not service._controller_health_miss_is_graced(
            controller_responding=False,
            controller_needs_respawn=True,
            external_lb_healthy=True)

    def test_health_miss_does_not_hide_external_lb_failure(self):
        assert not service._controller_health_miss_is_graced(
            controller_responding=False,
            controller_needs_respawn=False,
            external_lb_healthy=False)

    def test_healthy_child_does_not_need_grace(self):
        assert not service._controller_health_miss_is_graced(
            controller_responding=True,
            controller_needs_respawn=False,
            external_lb_healthy=True)


class TestControllerHealth:

    def test_bounded_health_check(self):
        response = mock.Mock(status_code=200)
        with mock.patch.object(service.serve_utils,
                               '_get_to_local_controller_with_retry',
                               return_value=response) as get:
            assert service._controller_child_responding('svc', 'incarnation-a',
                                                        '10.0.0.2', 20001)
        get.assert_called_once_with(
            'svc',
            ('incarnation-a', service.os.getpid(), '10.0.0.2', 20001),
            constants.CONTROLLER_HEALTH_ENDPOINT_PATH,
            # Keep the literal here so lowering the production read budget
            # cannot make the assertion change along with the implementation.
            timeout=(0.5, 5))

    def test_failed_health_check_is_unhealthy(self):
        with mock.patch.object(service.serve_utils,
                               '_get_to_local_controller_with_retry',
                               side_effect=TimeoutError):
            assert not service._controller_child_responding(
                'svc', 'incarnation-a', '10.0.0.2', 20001)


def _record(status):
    return {
        'status': status,
        'hash': 'incarnation-a',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
    }


class TestDegradedFlag:

    def test_flags_healthy_service(self):
        with mock.patch.object(
                service.serve_state,
                'get_service_from_name',
                return_value=_record(serve_state.ServiceStatus.READY)), \
             mock.patch.object(
                 service.serve_state,
                 _SET_STATUS_METHOD) as set_status:
            service._flag_service_degraded('svc', 'incarnation-a', 123,
                                           '10.0.0.1')
        set_status.assert_called_once_with(
            'svc',
            'incarnation-a',
            123,
            '10.0.0.1',
            serve_state.ServiceStatus.CONTROLLER_FAILED,
            expected_status=serve_state.ServiceStatus.READY)

    def test_never_overrides_teardown(self):
        for status in (serve_state.ServiceStatus.SHUTTING_DOWN,
                       serve_state.ServiceStatus.FAILED_CLEANUP):
            with mock.patch.object(service.serve_state,
                                   'get_service_from_name',
                                   return_value=_record(status)), \
                 mock.patch.object(
                     service.serve_state,
                     _SET_STATUS_METHOD
                 ) as set_status:
                service._flag_service_degraded('svc', 'incarnation-a', 123,
                                               '10.0.0.1')
            set_status.assert_not_called()

    def test_flag_is_idempotent(self):
        with mock.patch.object(
                service.serve_state,
                'get_service_from_name',
                return_value=_record(
                    serve_state.ServiceStatus.CONTROLLER_FAILED)), \
             mock.patch.object(
                 service.serve_state,
                 _SET_STATUS_METHOD) as set_status:
            service._flag_service_degraded('svc', 'incarnation-a', 123,
                                           '10.0.0.1')
        set_status.assert_not_called()

    def test_db_failure_is_contained(self):
        with mock.patch.object(service.serve_state,
                               'get_service_from_name',
                               side_effect=RuntimeError('db down')):
            service._flag_service_degraded('svc', 'incarnation-a', 123,
                                           '10.0.0.1')  # Must not raise.


class TestDegradedHeal:

    def test_heals_only_controller_failed(self):
        with mock.patch.object(
                service.serve_state,
                'get_service_from_name',
                return_value=_record(
                    serve_state.ServiceStatus.CONTROLLER_FAILED)), \
             mock.patch.object(
                 service.serve_state,
                 _SET_STATUS_METHOD,
                 return_value=True) as set_status:
            assert service._heal_service_degraded('svc', 'incarnation-a', 123,
                                                  '10.0.0.1')
        set_status.assert_called_once_with(
            'svc',
            'incarnation-a',
            123,
            '10.0.0.1',
            serve_state.ServiceStatus.REPLICA_INIT,
            expected_status=serve_state.ServiceStatus.CONTROLLER_FAILED)

    def test_noop_for_other_statuses_reports_complete(self):
        for status in (serve_state.ServiceStatus.READY,
                       serve_state.ServiceStatus.SHUTTING_DOWN,
                       serve_state.ServiceStatus.NO_REPLICA):
            with mock.patch.object(service.serve_state,
                                   'get_service_from_name',
                                   return_value=_record(status)), \
                 mock.patch.object(
                     service.serve_state,
                     _SET_STATUS_METHOD
                 ) as set_status:
                assert service._heal_service_degraded('svc', 'incarnation-a',
                                                      123, '10.0.0.1')
            set_status.assert_not_called()

    def test_db_failure_reports_incomplete_for_retry(self):
        # The caller must keep retrying the heal on subsequent healthy
        # ticks; giving up would leave the service stuck CONTROLLER_FAILED
        # (the replica-driven writer never overwrites that status).
        with mock.patch.object(service.serve_state,
                               'get_service_from_name',
                               side_effect=RuntimeError('db down')):
            assert not service._heal_service_degraded('svc', 'incarnation-a',
                                                      123, '10.0.0.1')
        with mock.patch.object(
                service.serve_state,
                'get_service_from_name',
                return_value=_record(
                    serve_state.ServiceStatus.CONTROLLER_FAILED)), \
             mock.patch.object(
                 service.serve_state,
                 _SET_STATUS_METHOD,
                 side_effect=RuntimeError('db down')):
            assert not service._heal_service_degraded('svc', 'incarnation-a',
                                                      123, '10.0.0.1')


class TestReplicaWriterGuard:

    def test_does_not_overwrite_controller_failed(self):
        with mock.patch.object(
                serve_utils.serve_state,
                'get_service_controller_owner',
                return_value=_record(
                    serve_state.ServiceStatus.CONTROLLER_FAILED)), \
             mock.patch.object(
                 serve_utils.serve_state,
                 _SET_STATUS_METHOD) as set_status:
            serve_utils.set_service_status_and_active_versions_from_replica(
                'svc', [], serve_utils.UpdateMode.ROLLING)
        set_status.assert_not_called()
