"""Tests for sky.jobs.server.core."""
# pylint: disable=redefined-outer-name
from contextlib import ExitStack
import pickle
from types import SimpleNamespace
from unittest import mock

import pytest

from sky import backends
from sky import exceptions
from sky.jobs import runner as managed_job_runner
from sky.jobs.server import cancellation as jobs_cancellation
from sky.jobs.server import core as jobs_core
from sky.usage import usage_lib
from sky.utils import controller_utils


@pytest.fixture
def cancellation_gateway():
    handle = mock.MagicMock()
    handle.is_grpc_enabled_with_flag = True
    channel = object()
    handle.get_grpc_channel.return_value = channel
    backend = mock.MagicMock(spec=backends.CloudVmRayBackend)
    runner = mock.MagicMock()
    client = mock.MagicMock()
    client.cancel_managed_jobs.return_value = SimpleNamespace(
        message='Cancellation requested.')

    with ExitStack() as stack:
        accessible = stack.enter_context(
            mock.patch.object(jobs_core.backend_utils,
                              'is_controller_accessible',
                              return_value=handle))
        get_backend = stack.enter_context(
            mock.patch.object(jobs_core.backend_utils,
                              'get_backend_from_handle',
                              return_value=backend))
        invoke = stack.enter_context(
            mock.patch.object(jobs_core.backend_utils,
                              'invoke_skylet_with_retries',
                              side_effect=lambda operation: operation()))
        skylet_client = stack.enter_context(
            mock.patch.object(jobs_core.cloud_vm_ray_backend,
                              'SkyletClient',
                              return_value=client))
        current_runner = stack.enter_context(
            mock.patch.object(managed_job_runner,
                              'current',
                              return_value=runner))
        stack.enter_context(
            mock.patch.object(jobs_core.skypilot_config,
                              'get_active_workspace',
                              return_value='workspace-a'))
        stack.enter_context(
            mock.patch.object(jobs_core.common_utils,
                              'get_user_hash',
                              return_value='user-hash-a'))
        yield SimpleNamespace(
            handle=handle,
            channel=channel,
            backend=backend,
            runner=runner,
            client=client,
            accessible=accessible,
            get_backend=get_backend,
            invoke=invoke,
            skylet_client=skylet_client,
            current_runner=current_runner,
        )


def test_cancel_grpc_projects_job_ids_and_graceful_fields(cancellation_gateway):
    gateway = cancellation_gateway

    jobs_core.cancel(job_ids=[7, 9], graceful=True, graceful_timeout=23)

    gateway.accessible.assert_called_once_with(
        controller=controller_utils.Controllers.JOBS_CONTROLLER,
        stopped_message='All managed jobs should have finished.')
    gateway.get_backend.assert_called_once_with(gateway.handle)
    gateway.skylet_client.assert_called_once_with(gateway.channel)
    gateway.invoke.assert_called_once()
    gateway.client.cancel_managed_jobs.assert_called_once()
    request = gateway.client.cancel_managed_jobs.call_args.args[0]
    assert request.current_workspace == 'workspace-a'
    assert list(request.job_ids.ids) == [7, 9]
    assert request.graceful is True
    assert request.graceful_timeout == 23
    gateway.current_runner.assert_not_called()


@pytest.mark.parametrize(('kwargs', 'field', 'expected'), [
    ({
        'name': 'train'
    }, 'job_name', 'train'),
    ({
        'pool': 'workers'
    }, 'pool_name', 'workers'),
    ({
        'all': True
    }, 'user_hash', 'user-hash-a'),
    ({
        'all_users': True
    }, 'all_users', True),
])
def test_cancel_grpc_projects_selector(cancellation_gateway, kwargs, field,
                                       expected):
    gateway = cancellation_gateway

    jobs_core.cancel(**kwargs)

    request = gateway.client.cancel_managed_jobs.call_args.args[0]
    assert getattr(request, field) == expected


def test_cancel_rejects_stale_controller_graceful_without_legacy_runner(
        cancellation_gateway):
    gateway = cancellation_gateway
    gateway.client.cancel_managed_jobs.side_effect = (
        exceptions.SkyletMethodNotImplementedError())

    with pytest.raises(exceptions.NotSupportedError,
                       match='Please upgrade the jobs controller'):
        jobs_core.cancel(name='train', graceful=True, graceful_timeout=17)

    gateway.client.cancel_managed_jobs.assert_called_once()
    gateway.current_runner.assert_not_called()


def test_cancel_rejects_missing_selector(cancellation_gateway):
    with pytest.raises(ValueError, match='Can only specify one'):
        jobs_core.cancel()

    cancellation_gateway.get_backend.assert_not_called()
    cancellation_gateway.invoke.assert_not_called()


def test_cancel_non_graceful_falls_back_to_legacy_runner_when_grpc_missing(
        cancellation_gateway):
    gateway = cancellation_gateway
    gateway.client.cancel_managed_jobs.side_effect = (
        exceptions.SkyletMethodNotImplementedError())
    gateway.runner.cancel_managed_jobs.return_value = 'Legacy cancelled.'

    jobs_core.cancel(name='train')

    gateway.client.cancel_managed_jobs.assert_called_once()
    gateway.runner.cancel_managed_jobs.assert_called_once_with(
        handle=gateway.handle,
        backend=gateway.backend,
        all_users=False,
        all=False,
        job_ids=[],
        name='train',
        pool=None,
        graceful=False,
        graceful_timeout=None,
    )


def test_cancel_rejects_non_grpc_graceful_before_backend_lookup(
        cancellation_gateway):
    gateway = cancellation_gateway
    gateway.handle.is_grpc_enabled_with_flag = False

    with pytest.raises(exceptions.NotSupportedError,
                       match='Please upgrade the jobs controller'):
        jobs_core.cancel(name='train', graceful=True, graceful_timeout=17)

    gateway.get_backend.assert_not_called()
    gateway.invoke.assert_not_called()
    gateway.current_runner.assert_not_called()


def test_cancel_rejects_missing_grpc_output(cancellation_gateway):
    gateway = cancellation_gateway
    gateway.client.cancel_managed_jobs.return_value = SimpleNamespace(
        message=None)

    with pytest.raises(RuntimeError, match='produced no output'):
        jobs_core.cancel(name='train')


@pytest.mark.parametrize(('output', 'message'), [
    (None, 'produced no output'),
    ('Multiple jobs found with name train', 'specify the job ID'),
])
def test_cancel_rejects_invalid_legacy_output(cancellation_gateway, output,
                                              message):
    gateway = cancellation_gateway
    gateway.handle.is_grpc_enabled_with_flag = False
    gateway.runner.cancel_managed_jobs.return_value = output

    with pytest.raises(RuntimeError, match=message):
        jobs_core.cancel(name='train')


def test_cancel_facade_preserves_callable_and_pickle_identity():
    assert jobs_core.cancel is jobs_cancellation.cancel
    assert jobs_core.cancel.__module__ == jobs_core.__name__
    assert pickle.loads(pickle.dumps(jobs_core.cancel)) is jobs_core.cancel


def test_cancel_preserves_usage_entrypoint_attribution(cancellation_gateway):
    usage_message = usage_lib.messages.usage
    option_type = type(usage_lib.env_options.Options.DISABLE_LOGGING)
    with mock.patch.object(option_type, 'get', return_value=False), \
         mock.patch.object(usage_message, 'entrypoint', None), \
         mock.patch.object(usage_message, 'update_entrypoint') as update, \
         mock.patch.object(usage_lib, '_send_local_messages'):
        with pytest.raises(ValueError, match='Can only specify one'):
            jobs_core.cancel()

    update.assert_called_once_with('sky.jobs.server.core.cancel')
    cancellation_gateway.get_backend.assert_not_called()


def _forwarded_tail(tail):
    """Call ``jobs_core.tail_logs`` with ``tail`` (mocking out the controller
    restart / backend / runner) and return the ``tail`` value forwarded to
    ``tail_managed_job_logs``."""
    fake_backend = mock.MagicMock(spec=backends.CloudVmRayBackend)
    fake_runner = mock.MagicMock()
    fake_runner.tail_managed_job_logs.return_value = 0
    with mock.patch.object(jobs_core, '_maybe_restart_controller',
                           return_value=mock.MagicMock()), \
         mock.patch.object(jobs_core.backend_utils,
                           'get_backend_from_handle',
                           return_value=fake_backend), \
         mock.patch.object(jobs_core.managed_job_runner,
                           'current',
                           return_value=fake_runner):
        jobs_core.tail_logs(name=None,
                            job_id=1,
                            follow=False,
                            controller=False,
                            refresh=False,
                            tail=tail)
    fake_runner.tail_managed_job_logs.assert_called_once()
    return fake_runner.tail_managed_job_logs.call_args.kwargs['tail']


@pytest.mark.parametrize(
    ('given', 'expected'),
    [
        (0, None),  # dashboard download button's "all lines" sentinel
        (-1, None),  # `sky jobs logs --tail -1`
        (None, None),  # no tail -> all
        (200, 200),  # positive tail forwarded unchanged
        (5000, 5000),
    ])
def test_tail_logs_normalizes_non_positive_tail(given, expected):
    """A non-positive tail (0 / -1) means "all lines" and must be normalized
    to None before reaching the backward-seek tail reader (which asserts
    tail > 0). Otherwise the dashboard download (tail=0) raises
    AssertionError and produces an empty log."""
    assert _forwarded_tail(given) == expected


def test_associate_job_api_access_token_batches_ids(monkeypatch):
    persist = mock.Mock()
    revoke = mock.Mock()
    monkeypatch.setattr(jobs_core.managed_job_state, 'set_api_access_token_ids',
                        persist)
    monkeypatch.setattr(jobs_core.global_user_state,
                        'delete_service_account_token', revoke)

    jobs_core._associate_job_api_access_token(  # pylint: disable=protected-access
        [7, 8, 9], 'token-id')

    persist.assert_called_once_with([7, 8, 9], 'token-id')
    revoke.assert_not_called()


def test_associate_job_api_access_token_revokes_on_persist_failure(monkeypatch):
    persist_error = RuntimeError('managed jobs database unavailable')
    persist = mock.Mock(side_effect=persist_error)
    revoke = mock.Mock()
    monkeypatch.setattr(jobs_core.managed_job_state, 'set_api_access_token_ids',
                        persist)
    monkeypatch.setattr(jobs_core.global_user_state,
                        'delete_service_account_token', revoke)

    with pytest.raises(RuntimeError) as raised:
        jobs_core._associate_job_api_access_token(  # pylint: disable=protected-access
            [7, 8], 'token-id')

    assert raised.value is persist_error
    revoke.assert_called_once_with('token-id')


def test_associate_job_api_access_token_preserves_persist_failure_when_revoke_fails(
        monkeypatch, caplog):
    persist_error = RuntimeError('managed jobs database unavailable')
    persist = mock.Mock(side_effect=persist_error)
    revoke = mock.Mock(side_effect=RuntimeError('token database unavailable'))
    monkeypatch.setattr(jobs_core.managed_job_state, 'set_api_access_token_ids',
                        persist)
    monkeypatch.setattr(jobs_core.global_user_state,
                        'delete_service_account_token', revoke)

    with pytest.raises(RuntimeError) as raised:
        jobs_core._associate_job_api_access_token(  # pylint: disable=protected-access
            [7, 8], 'token-id')

    assert raised.value is persist_error
    assert 'Failed to revoke unassociated API access token token-id' in caplog.text
