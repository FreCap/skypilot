"""Tests for external-only controller-child supervision."""
# pylint: disable=protected-access
from unittest import mock

from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import service


class TestBackoff:

    def test_backoff_grows_and_caps(self):
        first = service._child_respawn_backoff_seconds(1)
        second = service._child_respawn_backoff_seconds(2)
        huge = service._child_respawn_backoff_seconds(1000)
        assert first == service._CHILD_RESPAWN_BACKOFF_BASE_SECONDS
        assert second == 2 * first
        assert huge == service._CHILD_RESPAWN_BACKOFF_CAP_SECONDS


class TestControllerHealth:

    def test_bounded_health_check(self):
        response = mock.Mock(status_code=200)
        with mock.patch.object(service.serve_utils,
                               '_get_to_controller_with_retry',
                               return_value=response) as get:
            assert service._controller_child_responding('svc', 20001)
        get.assert_called_once_with('svc',
                                    20001,
                                    '/autoscaler/info',
                                    timeout=(0.5, 1.0))

    def test_failed_health_check_is_unhealthy(self):
        with mock.patch.object(service.serve_utils,
                               '_get_to_controller_with_retry',
                               side_effect=TimeoutError):
            assert not service._controller_child_responding('svc', 20001)


def _record(status):
    return {'status': status}


class TestDegradedFlag:

    def test_flags_healthy_service(self):
        with mock.patch.object(
                service.serve_state,
                'get_service_from_name',
                return_value=_record(serve_state.ServiceStatus.READY)), \
             mock.patch.object(
                 service.serve_state,
                 'set_service_status_and_active_versions') as set_status:
            service._flag_service_degraded('svc')
        set_status.assert_called_once_with(
            'svc', serve_state.ServiceStatus.CONTROLLER_FAILED)

    def test_never_overrides_teardown(self):
        for status in (serve_state.ServiceStatus.SHUTTING_DOWN,
                       serve_state.ServiceStatus.FAILED_CLEANUP):
            with mock.patch.object(service.serve_state,
                                   'get_service_from_name',
                                   return_value=_record(status)), \
                 mock.patch.object(
                     service.serve_state,
                     'set_service_status_and_active_versions') as set_status:
                service._flag_service_degraded('svc')
            set_status.assert_not_called()

    def test_flag_is_idempotent(self):
        with mock.patch.object(
                service.serve_state,
                'get_service_from_name',
                return_value=_record(
                    serve_state.ServiceStatus.CONTROLLER_FAILED)), \
             mock.patch.object(
                 service.serve_state,
                 'set_service_status_and_active_versions') as set_status:
            service._flag_service_degraded('svc')
        set_status.assert_not_called()

    def test_db_failure_is_contained(self):
        with mock.patch.object(service.serve_state,
                               'get_service_from_name',
                               side_effect=RuntimeError('db down')):
            service._flag_service_degraded('svc')  # Must not raise.


class TestDegradedHeal:

    def test_heals_only_controller_failed(self):
        with mock.patch.object(
                service.serve_state,
                'get_service_from_name',
                return_value=_record(
                    serve_state.ServiceStatus.CONTROLLER_FAILED)), \
             mock.patch.object(
                 service.serve_state,
                 'set_service_status_and_active_versions') as set_status:
            assert service._heal_service_degraded('svc')
        set_status.assert_called_once_with(
            'svc', serve_state.ServiceStatus.REPLICA_INIT)

    def test_noop_for_other_statuses_reports_complete(self):
        for status in (serve_state.ServiceStatus.READY,
                       serve_state.ServiceStatus.SHUTTING_DOWN,
                       serve_state.ServiceStatus.NO_REPLICA):
            with mock.patch.object(service.serve_state,
                                   'get_service_from_name',
                                   return_value=_record(status)), \
                 mock.patch.object(
                     service.serve_state,
                     'set_service_status_and_active_versions') as set_status:
                assert service._heal_service_degraded('svc')
            set_status.assert_not_called()

    def test_db_failure_reports_incomplete_for_retry(self):
        # The caller must keep retrying the heal on subsequent healthy
        # ticks; giving up would leave the service stuck CONTROLLER_FAILED
        # (the replica-driven writer never overwrites that status).
        with mock.patch.object(service.serve_state,
                               'get_service_from_name',
                               side_effect=RuntimeError('db down')):
            assert not service._heal_service_degraded('svc')
        with mock.patch.object(
                service.serve_state,
                'get_service_from_name',
                return_value=_record(
                    serve_state.ServiceStatus.CONTROLLER_FAILED)), \
             mock.patch.object(
                 service.serve_state,
                 'set_service_status_and_active_versions',
                 side_effect=RuntimeError('db down')):
            assert not service._heal_service_degraded('svc')


class TestReplicaWriterGuard:

    def test_does_not_overwrite_controller_failed(self):
        with mock.patch.object(
                serve_utils.serve_state,
                'get_service_from_name',
                return_value=_record(
                    serve_state.ServiceStatus.CONTROLLER_FAILED)), \
             mock.patch.object(
                 serve_utils.serve_state,
                 'set_service_status_and_active_versions') as set_status:
            serve_utils.set_service_status_and_active_versions_from_replica(
                'svc', [], serve_utils.UpdateMode.ROLLING)
        set_status.assert_not_called()
