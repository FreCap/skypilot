"""Unit tests for the IBM VPC provider's Cloud Functions client."""

import importlib.util
from pathlib import Path
import sys
import types
from unittest import mock

import pytest


@pytest.fixture(name='vpc_provider', scope='module')
def fixture_vpc_provider():
    """Loads the provider without importing Ray's legacy node provider."""
    monkeypatch = pytest.MonkeyPatch()
    provider_dir = (Path(__file__).parents[2] / 'sky' / 'skylet' / 'providers' /
                    'ibm')
    package_name = 'sky.skylet.providers.ibm'
    package = types.ModuleType(package_name)
    package.__path__ = [str(provider_dir)]
    monkeypatch.setitem(sys.modules, package_name, package)

    utils_name = f'{package_name}.utils'
    utils_spec = importlib.util.spec_from_file_location(
        utils_name, provider_dir / 'utils.py')
    assert utils_spec is not None and utils_spec.loader is not None
    utils_module = importlib.util.module_from_spec(utils_spec)
    monkeypatch.setitem(sys.modules, utils_name, utils_module)
    utils_spec.loader.exec_module(utils_module)

    provider_spec = importlib.util.spec_from_file_location(
        'ibm_vpc_provider_under_test', provider_dir / 'vpc_provider.py')
    assert provider_spec is not None and provider_spec.loader is not None
    provider_module = importlib.util.module_from_spec(provider_spec)
    provider_spec.loader.exec_module(provider_module)
    yield provider_module
    monkeypatch.undo()


class _RequestSentinel(Exception):
    """Stops a client method immediately after it issues its request."""


def _make_cleaner(vpc_provider):
    cleaner = vpc_provider.ClusterCleaner('resource-group', 'vpc-id', 'us-east')
    cleaner.get_headers = mock.MagicMock(return_value={})
    return cleaner


def test_create_namespace_preserves_response_until_status_is_checked(
        vpc_provider):
    cleaner = _make_cleaner(vpc_provider)
    list_response = mock.MagicMock()
    list_response.text = '{"total_count": 0, "namespaces": []}'
    create_response = mock.MagicMock(status_code=201)
    create_response.json.return_value = {'id': 'namespace-id'}

    with mock.patch.object(vpc_provider.requests, 'get',
                           return_value=list_response) as get_request, \
         mock.patch.object(vpc_provider.requests, 'post',
                           return_value=create_response) as post_request:
        namespace_id = cleaner.create_or_fetch_namespace()

    assert namespace_id == 'namespace-id'
    create_response.raise_for_status.assert_called_once_with()
    assert get_request.call_args.kwargs[
        'timeout'] == vpc_provider.DEFAULT_HTTP_TIMEOUT_SECONDS
    assert post_request.call_args.kwargs[
        'timeout'] == vpc_provider.DEFAULT_HTTP_TIMEOUT_SECONDS


def test_delete_missing_action_remains_idempotent(vpc_provider):
    cleaner = _make_cleaner(vpc_provider)
    response = mock.MagicMock(status_code=404, text='')

    with mock.patch.object(vpc_provider.requests,
                           'delete',
                           return_value=response):
        assert cleaner.delete_action('namespace-id') == {}

    response.raise_for_status.assert_not_called()


def test_invocation_timeout_exceeds_action_budget(vpc_provider):
    assert (vpc_provider.INVOKE_HTTP_TIMEOUT_SECONDS
            > vpc_provider.CLOUD_FUNCTION_ACTION_TIMEOUT_SECONDS)


def test_ibm_oauth_request_has_finite_timeout(vpc_provider):
    response = mock.MagicMock(text='{"access_token": "token"}')

    with mock.patch.object(vpc_provider.ibm,
                           'get_api_key',
                           return_value='api-key'), mock.patch.object(
                               vpc_provider.ibm.requests,
                               'post',
                               return_value=response) as request:
        assert vpc_provider.ibm.get_oauth_token() == 'token'

    response.raise_for_status.assert_called_once_with()
    assert request.call_args.kwargs[
        'timeout'] == vpc_provider.ibm.IAM_HTTP_TIMEOUT_SECONDS


@pytest.mark.parametrize(
    ('client_method', 'request_method', 'args', 'timeout_name'), [
        ('create_or_fetch_namespace', 'get',
         (), 'DEFAULT_HTTP_TIMEOUT_SECONDS'),
        ('_get_cloud_functions_actions', 'get',
         ('namespace-id',), 'DEFAULT_HTTP_TIMEOUT_SECONDS'),
        ('create_action', 'put',
         ('namespace-id',), 'DEFAULT_HTTP_TIMEOUT_SECONDS'),
        ('delete_action', 'delete',
         ('namespace-id',), 'DEFAULT_HTTP_TIMEOUT_SECONDS'),
        ('invoke_action', 'post',
         ('namespace-id',), 'INVOKE_HTTP_TIMEOUT_SECONDS'),
    ])
def test_cloud_function_requests_have_finite_timeout(vpc_provider,
                                                     client_method,
                                                     request_method, args,
                                                     timeout_name):
    cleaner = _make_cleaner(vpc_provider)

    with mock.patch.object(
            vpc_provider.ibm, 'get_api_key',
            return_value='api-key'), mock.patch.object(
                vpc_provider.requests,
                request_method,
                side_effect=_RequestSentinel) as request, pytest.raises(
                    _RequestSentinel):
        getattr(cleaner, client_method)(*args)

    assert request.call_args.kwargs['timeout'] == getattr(
        vpc_provider, timeout_name)
