"""Unit tests for sky.server.requests.precond module."""
import asyncio
import unittest
from unittest import mock

from sky import exceptions
from sky import execution
from sky.serve import constants as serve_constants
from sky.serve import serve_state
from sky.server.requests import preconditions
from sky.server.requests import request_names
from sky.server.requests import requests as api_requests
from sky.utils import status_lib


class TestPrecondition(unittest.IsolatedAsyncioTestCase):
    """Unit tests for Precondition class."""

    def setUp(self):
        self.request_id = 'test-request'

    @mock.patch('sky.server.requests.requests.set_request_failed_async')
    @mock.patch('sky.server.requests.requests.get_request_async')
    async def test_precondition_timeout(self, mock_get_request,
                                        mock_set_failed):
        """Test Precondition timeout behavior."""

        class Timeouted(preconditions.Precondition):

            async def check(self):
                return False, 'Still checking'

        mock_get_request.return_value = mock.MagicMock(
            status=api_requests.RequestStatus.PENDING)

        p = Timeouted(self.request_id, check_interval=0.01, timeout=0.02)
        result = await p

        self.assertFalse(result)
        mock_set_failed.assert_awaited_once()
        self.assertIsInstance(mock_set_failed.call_args[0][1],
                              exceptions.RequestCancelled)

    async def test_precondition_timeout_uses_monotonic_deadline(self):
        """Wall clock rollback must not extend the precondition timeout."""
        check_calls = 0

        class Pending(preconditions.Precondition):

            async def check(self):
                nonlocal check_calls
                check_calls += 1
                return False, 'Still checking'

        mock_get_request = mock.AsyncMock(return_value=mock.MagicMock(
            status=api_requests.RequestStatus.PENDING))
        mock_set_failed = mock.AsyncMock()
        mock_update_status = mock.AsyncMock()
        mock_sleep = mock.AsyncMock(side_effect=[
            None,
            AssertionError('wait exceeded its monotonic deadline'),
        ])
        mock_clock = mock.MagicMock()
        mock_clock.time.side_effect = [1000.0, 999.0, 998.0]
        mock_clock.monotonic.side_effect = [100.0, 100.5, 101.1]
        with mock.patch.object(api_requests, 'get_request_async',
                               mock_get_request), \
             mock.patch.object(api_requests, 'set_request_failed_async',
                               mock_set_failed), \
             mock.patch.object(api_requests, 'update_status_msg_async',
                               mock_update_status), \
             mock.patch.object(preconditions, 'time', mock_clock), \
             mock.patch.object(preconditions.asyncio, 'sleep', mock_sleep):
            result = await Pending(self.request_id, check_interval=1, timeout=1)

        self.assertFalse(result)
        mock_set_failed.assert_awaited_once()
        mock_get_request.assert_awaited_once()
        self.assertEqual(check_calls, 1)
        mock_sleep.assert_awaited_once_with(1)
        mock_clock.time.assert_not_called()
        self.assertEqual(mock_clock.monotonic.call_count, 3)

    @mock.patch('sky.server.requests.requests.get_request_async')
    async def test_precondition_without_timeout_skips_clock(
            self, mock_get_request):
        """An unlimited immediate wait should not read the deadline clock."""
        check_calls = 0

        class Ready(preconditions.Precondition):

            async def check(self):
                nonlocal check_calls
                check_calls += 1
                return True, None

        mock_get_request.return_value = mock.MagicMock(
            status=api_requests.RequestStatus.PENDING)
        mock_clock = mock.MagicMock()
        with mock.patch.object(preconditions, 'time', mock_clock), \
             mock.patch.object(preconditions.asyncio, 'sleep',
                               new_callable=mock.AsyncMock) as mock_sleep:
            result = await Ready(self.request_id, timeout=0)

        self.assertTrue(result)
        mock_get_request.assert_awaited_once()
        self.assertEqual(check_calls, 1)
        mock_sleep.assert_not_awaited()
        mock_clock.monotonic.assert_not_called()

    @mock.patch('sky.server.requests.requests.get_request_async')
    async def test_precondition_cancelled(self, mock_get_request):
        """Test Precondition cancellation behavior."""

        class Canceled(preconditions.Precondition):

            async def check(self):
                return False, 'Waiting'

        mock_get_request.return_value = mock.MagicMock(
            status=api_requests.RequestStatus.CANCELLED)

        p = Canceled(self.request_id)
        result = await p

        self.assertFalse(result)

    @mock.patch('sky.server.requests.requests.set_request_failed_async')
    @mock.patch('sky.server.requests.requests.get_request_async')
    async def test_precondition_check_exception(self, mock_get_request,
                                                mock_set_failed):
        """Test Precondition behavior when check raises exception."""

        class Errored(preconditions.Precondition):

            async def check(self):
                raise RuntimeError('Test error')

        mock_get_request.return_value = mock.MagicMock(
            status=api_requests.RequestStatus.PENDING)

        p = Errored(self.request_id)
        result = await p

        self.assertFalse(result)
        mock_set_failed.assert_awaited_once()

    @mock.patch('sky.server.requests.requests.set_request_failed_async')
    @mock.patch('sky.server.requests.requests.get_request_async')
    async def test_precondition_callback_exception_marks_request_failed(
            self, mock_get_request, mock_set_failed):
        """A failed queue insertion must not leave the request pending."""

        class Ready(preconditions.Precondition):

            async def check(self):
                await asyncio.sleep(0)
                return True, None

        mock_get_request.return_value = mock.MagicMock(
            status=api_requests.RequestStatus.PENDING)
        failed_enqueue = mock.AsyncMock(
            side_effect=RuntimeError('queue unavailable'))

        await Ready(self.request_id).wait_async(on_condition_met=failed_enqueue)

        mock_set_failed.assert_awaited_once()
        self.assertIsInstance(mock_set_failed.call_args.args[1], RuntimeError)

    @mock.patch('sky.server.requests.requests.set_request_failed_async')
    @mock.patch('sky.server.requests.requests.get_request_async')
    async def test_precondition_callback_cancellation_propagates(
            self, mock_get_request, mock_set_failed):
        """Server shutdown cancellation must not become a request failure."""

        class Ready(preconditions.Precondition):

            async def check(self):
                await asyncio.sleep(0)
                return True, None

        mock_get_request.return_value = mock.MagicMock(
            status=api_requests.RequestStatus.PENDING)
        cancelled_enqueue = mock.AsyncMock(side_effect=asyncio.CancelledError())

        with self.assertRaises(asyncio.CancelledError):
            await Ready(self.request_id
                       ).wait_async(on_condition_met=cancelled_enqueue)

        mock_set_failed.assert_not_awaited()


class TestClusterStartCompletePrecondition(unittest.IsolatedAsyncioTestCase):
    """Unit tests for ClusterStartCompletePrecondition class."""

    def setUp(self):
        """Set up test fixtures."""
        self.request_id = 'test-request'
        self.cluster_name = 'test-cluster'

    @mock.patch('sky.global_user_state.get_status_from_cluster_name_async',
                new_callable=mock.AsyncMock)
    @mock.patch('sky.server.requests.requests.get_request_tasks_async')
    async def test_cluster_up(self, mock_get_tasks, mock_get_status):
        """Test when cluster is UP."""
        mock_get_status.return_value = status_lib.ClusterStatus.UP
        mock_get_tasks.return_value = []

        p = preconditions.ClusterStartCompletePrecondition(
            self.request_id, self.cluster_name)
        met, msg = await p.check()

        self.assertTrue(met)
        self.assertIsNone(msg)
        # Should not check tasks when cluster is UP
        mock_get_tasks.assert_not_awaited()

    @mock.patch('sky.global_user_state.get_status_from_cluster_name_async',
                new_callable=mock.AsyncMock)
    @mock.patch('sky.server.requests.requests.get_request_tasks_async')
    async def test_cluster_not_found(self, mock_get_tasks, mock_get_status):
        """Test when cluster is not found and no tasks are running."""
        mock_get_status.return_value = None
        mock_get_tasks.return_value = []

        p = preconditions.ClusterStartCompletePrecondition(
            self.request_id, self.cluster_name)
        met, msg = await p.check()

        self.assertTrue(met)
        self.assertIsNone(msg)

    @mock.patch('sky.global_user_state.get_status_from_cluster_name_async',
                new_callable=mock.AsyncMock)
    @mock.patch('sky.server.requests.requests.get_request_tasks_async')
    async def test_cluster_starting(self, mock_get_tasks, mock_get_status):
        """Test when cluster is being started and there are tasks running."""
        mock_get_status.return_value = status_lib.ClusterStatus.INIT
        mock_get_tasks.return_value = [mock.MagicMock()]

        p = preconditions.ClusterStartCompletePrecondition(
            self.request_id, self.cluster_name)
        met, msg = await p.check()

        self.assertFalse(met)
        self.assertIn('Waiting for cluster', msg)

    @mock.patch('sky.global_user_state.get_status_from_cluster_name_async',
                new_callable=mock.AsyncMock)
    @mock.patch('sky.server.requests.requests.get_request_tasks_async')
    async def test_cluster_not_found_but_tasks_running(self, mock_get_tasks,
                                                       mock_get_status):
        """Test when cluster is not found but tasks are running."""
        mock_get_status.return_value = None
        mock_get_tasks.return_value = [mock.MagicMock()]

        p = preconditions.ClusterStartCompletePrecondition(
            self.request_id, self.cluster_name)
        met, _ = await p.check()

        self.assertFalse(met)


class TestServiceReplicaLaunchPrecondition(unittest.IsolatedAsyncioTestCase):
    """A persisted launch request must revalidate its Serve owner."""

    @mock.patch(
        'sky.serve.serve_state.get_service_replica_launch_authorization')
    async def test_exact_owner_is_authorized(self, mock_get_owner):
        mock_get_owner.return_value = {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'status': serve_state.ServiceStatus.READY,
        }
        condition = preconditions.ServiceReplicaLaunchPrecondition(
            'request-id', 'svc', 'incarnation-a', 123, '10.0.0.1')

        met, message = await condition.check()

        self.assertTrue(met)
        self.assertIsNone(message)

    @mock.patch(
        'sky.serve.serve_state.get_service_replica_launch_authorization')
    async def test_teardown_owner_is_rejected(self, mock_get_owner):
        mock_get_owner.return_value = {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'status': serve_state.ServiceStatus.SHUTTING_DOWN,
        }
        condition = preconditions.ServiceReplicaLaunchPrecondition(
            'request-id', 'svc', 'incarnation-a', 123, '10.0.0.1')

        with self.assertRaises(exceptions.RequestCancelled):
            await condition.check()

    @mock.patch(
        'sky.serve.serve_state.get_service_replica_launch_authorization')
    async def test_recovering_controller_failed_owner_is_authorized(
            self, mock_get_owner):
        mock_get_owner.return_value = {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'status': serve_state.ServiceStatus.CONTROLLER_FAILED,
        }
        condition = preconditions.ServiceReplicaLaunchPrecondition(
            'request-id', 'svc', 'incarnation-a', 123, '10.0.0.1')

        met, message = await condition.check()

        self.assertTrue(met)
        self.assertIsNone(message)

        launch_context = {
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'incarnation-a',
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.1',
        }
        execution._validate_service_replica_launch_fence(  # pylint: disable=protected-access
            launch_context)

    @mock.patch(
        'sky.serve.serve_state.get_service_replica_launch_authorization')
    async def test_quarantine_aware_version_and_owner_are_both_fenced(
            self, mock_get_authorization):
        mock_get_authorization.return_value = {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'status': serve_state.ServiceStatus.READY,
            # Version 2 was committed and quarantined; recovery elected v1.
            'launch_authorized_version': 1,
        }
        version_one = preconditions.ServiceReplicaLaunchPrecondition(
            'request-v1', 'svc', 'incarnation-a', 123, '10.0.0.1', 1)
        met, message = await version_one.check()
        self.assertTrue(met)
        self.assertIsNone(message)

        version_two = preconditions.ServiceReplicaLaunchPrecondition(
            'request-v2', 'svc', 'incarnation-a', 123, '10.0.0.1', 2)
        with self.assertRaises(exceptions.RequestCancelled):
            await version_two.check()

        stale_owner = preconditions.ServiceReplicaLaunchPrecondition(
            'request-stale', 'svc', 'incarnation-stale', 123, '10.0.0.1', 1)
        with self.assertRaises(exceptions.RequestCancelled):
            await stale_owner.check()

        self.assertEqual(mock_get_authorization.call_count, 3)
        mock_get_authorization.assert_called_with('svc')

    @mock.patch(
        'sky.serve.serve_state.get_service_replica_launch_authorization')
    async def test_legacy_serialized_fence_remains_owner_only(
            self, mock_get_authorization):
        """Old persisted v1 payloads omitted the generation field."""
        mock_get_authorization.return_value = {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'status': serve_state.ServiceStatus.READY,
            'launch_authorized_version': 2,
            'launch_version_required': False,
        }
        condition = preconditions.deserialize(
            'service-replica-launch.v1', {
                'check_interval': 1,
                'service_name': 'svc',
                'service_hash': 'incarnation-a',
                'controller_pid': 123,
                'controller_ip': '10.0.0.1',
            }, 'legacy-request')
        self.assertIsInstance(condition,
                              preconditions.ServiceReplicaLaunchPrecondition)
        self.assertIsNone(condition.service_version)

        met, message = await condition.check()

        self.assertTrue(met)
        self.assertIsNone(message)

    @mock.patch(
        'sky.serve.serve_state.get_service_replica_launch_authorization')
    async def test_legacy_fence_is_rejected_after_config_protocol_activation(
            self, mock_get_authorization):
        mock_get_authorization.return_value = {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'status': serve_state.ServiceStatus.READY,
            'launch_authorized_version': 2,
            'launch_version_required': True,
        }
        condition = preconditions.deserialize(
            'service-replica-launch.v1', {
                'check_interval': 1,
                'service_name': 'svc',
                'service_hash': 'incarnation-a',
                'controller_pid': 123,
                'controller_ip': '10.0.0.1',
            }, 'legacy-request')

        with self.assertRaises(exceptions.RequestCancelled):
            await condition.check()

    def test_new_serialized_fence_round_trips_generation(self):
        condition = preconditions.ServiceReplicaLaunchPrecondition(
            'request-id', 'svc', 'incarnation-a', 123, '10.0.0.1', 7)

        durable = preconditions.serialize(condition)

        self.assertIsNotNone(durable)
        assert durable is not None
        self.assertEqual(durable.payload['service_version'], 7)
        restored = preconditions.deserialize(durable.type_name, durable.payload,
                                             'request-id')
        self.assertIsInstance(restored,
                              preconditions.ServiceReplicaLaunchPrecondition)
        self.assertEqual(restored.service_version, 7)

    @mock.patch.object(serve_state,
                       'service_replica_launch_fence_holds',
                       return_value=True)
    async def test_excluded_profile_discriminator_round_trips_and_rechecks(
            self, mock_fence_holds):
        discriminator = {
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
                serve_constants.
                ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 7,
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY: '12345678-1234-4234-8234-123456789abc',
        }
        condition = preconditions.ServiceReplicaLaunchPrecondition(
            'request-id',
            'svc',
            'incarnation-a',
            123,
            '10.0.0.1',
            7,
            binding_excluded_launch_context=discriminator)

        durable = preconditions.serialize(condition)

        self.assertIsNotNone(durable)
        assert durable is not None
        self.assertEqual(durable.payload['binding_excluded_launch_context'],
                         discriminator)
        restored = preconditions.deserialize(durable.type_name, durable.payload,
                                             'request-id')
        self.assertIsInstance(restored,
                              preconditions.ServiceReplicaLaunchPrecondition)
        self.assertEqual(restored.binding_excluded_launch_context,
                         discriminator)

        met, message = await restored.check()

        self.assertTrue(met)
        self.assertIsNone(message)
        mock_fence_holds.assert_called_once_with(
            {
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'incarnation-a',
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 7,
                serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
                serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.1',
            }, discriminator)

    def test_malformed_serialized_excluded_profile_is_rejected(self):
        with self.assertRaisesRegex(ValueError,
                                    'excluded-profile discriminator'):
            preconditions.deserialize(
                'service-replica-launch.v1', {
                    'check_interval': 1,
                    'service_name': 'svc',
                    'service_hash': 'incarnation-a',
                    'controller_pid': 123,
                    'controller_ip': '10.0.0.1',
                    'binding_excluded_launch_context': {
                        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY: 'persisted-special.v1',
                    },
                }, 'request-id')

    def test_system_recovery_discriminator_must_name_its_queue_request(self):
        with self.assertRaisesRegex(ValueError, 'request identity'):
            preconditions.deserialize(
                'service-replica-launch.v1', {
                    'check_interval': 1,
                    'service_name': 'svc',
                    'service_hash': 'incarnation-a',
                    'controller_pid': 123,
                    'controller_ip': '10.0.0.1',
                    'binding_excluded_launch_context': {
                        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
                            serve_constants.
                            ORDINARY_LAUNCH_BINDING_EXCLUDED_SYSTEM_RECOVERY_PROFILE,
                        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 7,
                        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REQUEST_ID_KEY: 'different-request',
                        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_GENERATION_KEY: 7,
                    },
                }, 'request-id')

    @mock.patch('sky.execution.dag_utils.convert_entrypoint_to_dag')
    @mock.patch(
        'sky.serve.serve_state.get_service_replica_launch_authorization')
    def test_replayed_request_rechecks_fence_before_execution(
            self, mock_get_owner, mock_convert_dag):
        """Restart replay cannot bypass the in-memory precondition."""
        mock_get_owner.return_value = None
        launch_context = {
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'incarnation-a',
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.1',
        }

        with self.assertRaises(exceptions.RequestCancelled):
            execution._execute(  # pylint: disable=protected-access
                mock.MagicMock(),
                _request_name=request_names.AdminPolicyRequestName.
                CLUSTER_LAUNCH,
                _is_launched_by_sky_serve_controller=True,
                _extra_launch_context=launch_context)

        mock_convert_dag.assert_not_called()

    @mock.patch(
        'sky.serve.serve_state.get_service_replica_launch_authorization')
    def test_execution_uses_quarantine_aware_generation_and_owner_fence(
            self, mock_get_authorization):
        mock_get_authorization.return_value = {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'status': serve_state.ServiceStatus.READY,
            'launch_authorized_version': 1,
            'launch_version_required': True,
        }

        def launch_context(version, service_hash='incarnation-a'):
            return {
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: service_hash,
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: version,
                serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
                serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.1',
            }

        execution._validate_service_replica_launch_fence(  # pylint: disable=protected-access
            launch_context(1))
        with self.assertRaises(exceptions.RequestCancelled):
            execution._validate_service_replica_launch_fence(  # pylint: disable=protected-access
                launch_context(2))
        with self.assertRaises(exceptions.RequestCancelled):
            execution._validate_service_replica_launch_fence(  # pylint: disable=protected-access
                launch_context(1, service_hash='incarnation-stale'))

        legacy_context = launch_context(1)
        legacy_context.pop(
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY)
        with mock.patch(
                'sky.execution.dag_utils.convert_entrypoint_to_dag') as convert:
            with self.assertRaises(exceptions.RequestCancelled):
                execution._execute(  # pylint: disable=protected-access
                    mock.MagicMock(),
                    _request_name=request_names.AdminPolicyRequestName.
                    CLUSTER_LAUNCH,
                    _is_launched_by_sky_serve_controller=True,
                    _extra_launch_context=legacy_context)
            convert.assert_not_called()

        self.assertEqual(mock_get_authorization.call_count, 4)
        mock_get_authorization.assert_called_with('svc')

    @mock.patch('sky.execution.serve_utils.is_external_load_balancer_mode',
                return_value=False)
    @mock.patch('sky.execution.dag_utils.convert_entrypoint_to_dag',
                side_effect=RuntimeError('reached legacy execution'))
    def test_legacy_remote_db_request_skips_api_local_fence(
            self, mock_convert_dag, mock_external_mode):
        del mock_convert_dag, mock_external_mode
        with self.assertRaisesRegex(RuntimeError, 'reached legacy execution'):
            execution._execute(  # pylint: disable=protected-access
                mock.MagicMock(),
                _request_name=request_names.AdminPolicyRequestName.
                CLUSTER_LAUNCH,
                _is_launched_by_sky_serve_controller=True,
                _extra_launch_context={})


if __name__ == '__main__':
    unittest.main()
