"""Characterization tests for the SkyServe controller transport facade."""
# pylint: disable=protected-access
import pickle
from unittest import mock

import pytest

from sky.serve import constants
from sky.serve import controller_transport
from sky.serve import serve_state
from sky.serve import serve_utils


def _owner_record(**overrides) -> dict:
    record = {
        'hash': 'incarnation-a',
        'status': serve_state.ServiceStatus.READY,
        'controller_pid': 1234,
        'controller_port': 20001,
        'controller_ip': '10.0.0.7',
    }
    record.update(overrides)
    return record


def test_owner_fingerprint_contract_and_module_identity():
    assert serve_utils.make_controller_owner_fingerprint(
        'incarnation-a', 1234, None, 20001
    ) == 'afbd46e7eeca7a2ded8d33e61f86898d13f83789f29c11c061a8484a2bef3371'
    assert serve_utils.make_controller_owner_fingerprint(
        'incarnation-a', 1234, '2001:0db8::1', 20001
    ) == '59647565f7d42bbdfc79e32d0e376f75110e6fdef1104e8672aded3a745bdf4f'
    assert serve_utils.make_controller_owner_fingerprint.__module__ == (
        'sky.serve.serve_utils')
    assert serve_utils.ControllerOwnerError.__module__ == (
        'sky.serve.serve_utils')
    assert (serve_utils.make_controller_owner_fingerprint
            is controller_transport.make_controller_owner_fingerprint)
    assert (serve_utils.ControllerOwnerError
            is controller_transport.ControllerOwnerError)
    error = serve_utils.ControllerOwnerError('owner lost')
    restored = pickle.loads(pickle.dumps(error, protocol=5))
    assert type(restored) is serve_utils.ControllerOwnerError
    assert restored.args == ('owner lost',)


def test_remote_get_contract_uses_one_snapshot_and_one_http_call(
        monkeypatch, tmp_path):
    token_ring = tmp_path / 'admin.tokens'
    token_ring.write_text('admin-token\n', encoding='utf-8')
    monkeypatch.setenv(constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                       str(token_ring))
    monkeypatch.setenv('POD_IP', '10.0.0.5')
    response = mock.Mock(status_code=200)
    owner_read = mock.Mock(return_value=_owner_record())
    request = mock.Mock(return_value=response)

    with mock.patch.object(serve_utils.serve_state,
                           'get_service_controller_owner', owner_read), \
         mock.patch.object(serve_utils.requests, 'get', request):
        actual = serve_utils._get_to_controller_with_retry(
            'svc',
            'incarnation-a',
            '/autoscaler/info',
            headers={
                'X-Trace': 'trace-a',
                constants.CONTROLLER_OWNER_HEADER: 'caller-value',
            })

    assert actual is response
    owner_read.assert_called_once_with('svc')
    request.assert_called_once_with(
        'http://10.0.0.7:20001/autoscaler/info',
        timeout=serve_utils._CONTROLLER_HTTP_TIMEOUT_SECONDS,
        headers={
            'X-Trace': 'trace-a',
            constants.CONTROLLER_OWNER_HEADER:
                serve_utils.make_controller_owner_fingerprint(
                    'incarnation-a', 1234, '10.0.0.7', 20001),
            'Authorization': 'Bearer admin-token',
        })


def test_fixed_local_owner_skips_database_and_preserves_timeout(monkeypatch):
    monkeypatch.delenv(constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                       raising=False)
    monkeypatch.delenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, raising=False)
    response = mock.Mock(status_code=200)
    owner_read = mock.Mock()
    request = mock.Mock(return_value=response)
    owner = ('incarnation-a', 1234, '10.0.0.7', 20001)

    with mock.patch.object(serve_utils.serve_state,
                           'get_service_controller_owner', owner_read), \
         mock.patch.object(serve_utils.requests, 'get', request):
        actual = serve_utils._get_to_local_controller_with_retry('svc',
                                                                 owner,
                                                                 '/health',
                                                                 timeout=(0.25,
                                                                          0.5))

    assert actual is response
    owner_read.assert_not_called()
    request.assert_called_once_with(
        'http://localhost:20001/health',
        timeout=(0.25, 0.5),
        headers={
            constants.CONTROLLER_OWNER_HEADER:
                serve_utils.make_controller_owner_fingerprint(*owner)
        })


def test_placement_state_contract_validates_response_object():
    response = mock.Mock()
    response.json.return_value = {'replicas': []}
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response) as request:
        assert serve_utils.get_service_placement_state('svc',
                                                       'incarnation-a') == {
                                                           'replicas': []
                                                       }
    request.assert_called_once_with(
        'svc',
        'incarnation-a',
        constants.CONTROLLER_PLACEMENT_ENDPOINT_PATH,
        timeout=(1.0, 2.0))
    response.raise_for_status.assert_called_once_with()

    response.json.return_value = []
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response):
        with pytest.raises(ValueError,
                           match='Placement-state response must be an object'):
            serve_utils.get_service_placement_state('svc', 'incarnation-a')
