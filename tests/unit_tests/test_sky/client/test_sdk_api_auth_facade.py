"""Characterization tests for the SDK API-authentication facade."""

import inspect
import pickle

import sky
from sky.client import sdk


def test_api_auth_public_facade_contract() -> None:
    assert sky.api_login is sdk.api_login
    assert not hasattr(sky, 'api_logout')
    assert sdk.api_login.__module__ == sdk.__name__
    assert sdk.api_logout.__module__ == sdk.__name__

    login_parameters = inspect.signature(sdk.api_login).parameters
    assert tuple(login_parameters) == (
        'endpoint',
        'relogin',
        'service_account_token',
        'no_browser',
    )
    assert all(parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
               for parameter in login_parameters.values())
    assert tuple(
        parameter.default for parameter in login_parameters.values()) == (
            None,
            False,
            None,
            False,
        )
    assert not inspect.signature(sdk.api_logout).parameters


def test_api_auth_public_functions_pickle_round_trip() -> None:
    assert pickle.loads(pickle.dumps(sdk.api_login)) is sdk.api_login
    assert pickle.loads(pickle.dumps(sdk.api_logout)) is sdk.api_logout
