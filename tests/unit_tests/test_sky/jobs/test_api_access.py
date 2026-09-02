from unittest import mock

from sky.jobs import api_access


def test_create_job_api_token_preserves_user_identity():
    token_data = {
        'token': 'sky_raw-token',
        'token_id': 'token-id',
        'token_hash': 'token-hash',
        'expires_at': 12345,
    }
    token_service = mock.MagicMock()
    token_service.create_token.return_value = token_data

    with mock.patch.object(api_access.token_service_lib, 'token_service',
                           token_service), mock.patch.object(
                               api_access.global_user_state,
                               'add_service_account_token') as add_token:
        token, token_id = api_access.create_job_api_token(
            'original-user', 'controller-37-deadbeef')

    assert (token, token_id) == ('sky_raw-token', 'token-id')
    token_service.create_token.assert_called_once_with(
        creator_user_id='original-user',
        service_account_user_id='original-user',
        token_name='managed-job-controller-37-deadbeef',
        expires_in_days=3)
    add_token.assert_called_once_with(
        token_id='token-id',
        token_name='managed-job-controller-37-deadbeef',
        token_hash='token-hash',
        creator_user_hash='original-user',
        service_account_user_id='original-user',
        expires_at=12345)
