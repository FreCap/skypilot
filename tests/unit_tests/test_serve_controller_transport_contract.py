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
    response.json.return_value = {
        'available': False,
        'reason': 'controller_unavailable',
    }
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response) as request:
        assert serve_utils.get_service_placement_state(
            'svc', 'incarnation-a') == {
                'available': False,
                'reason': 'controller_unavailable',
            }
    request.assert_called_once_with(
        'svc',
        'incarnation-a',
        constants.CONTROLLER_PLACEMENT_ENDPOINT_PATH,
        params={
            'limit': constants.PLACEMENT_STATE_DEFAULT_PAGE_SIZE,
            'offset': 0,
            'include_paid_admission': True,
        },
        timeout=serve_utils._CONTROLLER_HTTP_TIMEOUT_SECONDS)
    response.raise_for_status.assert_called_once_with()

    response.json.return_value = []
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response):
        with pytest.raises(ValueError,
                           match='Placement-state response must be an object'):
            serve_utils.get_service_placement_state('svc', 'incarnation-a')

    response.json.return_value = {
        'available': True,
        'pagination_version': constants.PLACEMENT_STATE_PAGINATION_VERSION,
        'order_generation': 'a' * 64,
    }
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response):
        with pytest.raises(ValueError, match='must include locations'):
            serve_utils.get_service_placement_state('svc', 'incarnation-a')


def test_placement_state_pages_a_legacy_controller_response():
    response = mock.Mock()
    response.json.return_value = {
        'available': True,
        'enabled': True,
        'locations': [{
            'region': str(index)
        } for index in range(5)],
        'truncated': False,
    }
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response):
        payload = serve_utils.get_service_placement_state('svc',
                                                          'incarnation-a',
                                                          limit=2,
                                                          offset=2)

    assert [entry['region'] for entry in payload['locations']] == ['2', '3']
    assert payload['page_offset'] == 2
    assert payload['next_offset'] == 4
    assert payload['total_locations'] == 5
    assert payload['truncated'] is True
    assert payload['pagination_version'] == (
        constants.PLACEMENT_STATE_LEGACY_PAGINATION_VERSION)


def test_placement_state_accepts_an_exact_bounded_controller_page():
    response = mock.Mock()
    expected = {
        'available': True,
        'enabled': True,
        'pagination_version': constants.PLACEMENT_STATE_PAGINATION_VERSION,
        'order_generation': 'a' * 64,
        'page_offset': 100,
        'next_offset': None,
        'total_locations': 101,
        'locations': [{
            'region': 'last'
        }],
        'truncated': False,
    }
    response.json.return_value = expected
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response) as request:
        actual = serve_utils.get_service_placement_state(
            'svc',
            'incarnation-a',
            limit=1,
            offset=100,
            expected_order_generation=('a' * 64))

    assert actual is expected
    request.assert_called_once_with(
        'svc',
        'incarnation-a',
        constants.CONTROLLER_PLACEMENT_ENDPOINT_PATH,
        params={
            'limit': 1,
            'offset': 100,
            'include_paid_admission': True,
            'expected_order_generation': 'a' * 64,
        },
        timeout=serve_utils._CONTROLLER_HTTP_TIMEOUT_SECONDS)


def test_placement_state_accepts_previous_order_during_rolling_upgrade():
    response = mock.Mock()
    expected = {
        'available': True,
        'enabled': True,
        'pagination_version':
            constants.PLACEMENT_STATE_LEGACY_PAGINATION_VERSION,
        'page_offset': 1,
        'next_offset': None,
        'total_locations': 2,
        'locations': [{
            'region': 'legacy-second'
        }],
        'truncated': False,
    }
    response.json.return_value = expected
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response):
        actual = serve_utils.get_service_placement_state('svc',
                                                         'incarnation-a',
                                                         limit=1,
                                                         offset=1)

    assert actual is expected


@pytest.mark.parametrize('order_generation', [None, '', 'a' * 63, 'A' * 64])
def test_current_placement_page_requires_valid_order_generation(
        order_generation):
    response = mock.Mock()
    response.json.return_value = {
        'available': True,
        'enabled': True,
        'pagination_version': constants.PLACEMENT_STATE_PAGINATION_VERSION,
        'order_generation': order_generation,
        'page_offset': 0,
        'next_offset': None,
        'total_locations': 1,
        'locations': [{
            'region': 'only'
        }],
        'truncated': False,
    }
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response):
        with pytest.raises(ValueError, match='order generation'):
            serve_utils.get_service_placement_state('svc', 'incarnation-a')


def test_disabled_current_placement_page_needs_no_order_generation():
    response = mock.Mock()
    expected = {
        'available': True,
        'enabled': False,
        'pagination_version': constants.PLACEMENT_STATE_PAGINATION_VERSION,
        'page_offset': 0,
        'next_offset': None,
        'total_locations': 0,
        'locations': [],
        'truncated': False,
    }
    response.json.return_value = expected
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response):
        actual = serve_utils.get_service_placement_state('svc', 'incarnation-a')

    assert actual is expected


def test_catalog_order_change_response_is_validated_and_preserved():
    response = mock.Mock()
    expected = {
        'available': False,
        'reason': 'catalog_order_changed',
        'pagination_version': constants.PLACEMENT_STATE_PAGINATION_VERSION,
        'order_generation': 'b' * 64,
    }
    response.json.return_value = expected
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response):
        actual = serve_utils.get_service_placement_state(
            'svc',
            'incarnation-a',
            limit=1,
            offset=1,
            expected_order_generation='a' * 64)

    assert actual is expected

    response.json.return_value = {
        **expected,
        'order_generation': 'malformed',
    }
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response):
        with pytest.raises(ValueError, match='invalid generation'):
            serve_utils.get_service_placement_state(
                'svc',
                'incarnation-a',
                limit=1,
                offset=1,
                expected_order_generation='a' * 64)


def test_placement_state_rejects_a_mismatched_controller_page():
    response = mock.Mock()
    response.json.return_value = {
        'pagination_version': constants.PLACEMENT_STATE_PAGINATION_VERSION,
        'order_generation': 'a' * 64,
        'page_offset': 0,
        'next_offset': None,
        'total_locations': 1,
        'locations': [{
            'region': 'wrong-page'
        }],
    }
    with mock.patch.object(controller_transport,
                           '_get_to_controller_with_retry',
                           return_value=response):
        with pytest.raises(ValueError, match='page offset'):
            serve_utils.get_service_placement_state('svc',
                                                    'incarnation-a',
                                                    limit=1,
                                                    offset=1)
