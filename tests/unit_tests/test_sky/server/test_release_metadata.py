"""Tests for API-server release provenance."""

import asyncio
import datetime
from types import SimpleNamespace
from unittest import mock

import sky
from sky.schemas.api import responses
from sky.server import common
from sky.server import server


def _parse_iso8601(timestamp: str) -> datetime.datetime:
    parsed = datetime.datetime.fromisoformat(timestamp)
    assert parsed.tzinfo is not None
    return parsed


def test_source_commit_timestamp_is_timezone_aware():
    assert sky.__commit_timestamp__ is not None
    _parse_iso8601(sky.__commit_timestamp__)


def test_health_response_includes_release_timestamps():
    request = SimpleNamespace(
        state=SimpleNamespace(auth_user=None, anonymous_user=False))
    external_proxy_config = SimpleNamespace(enabled=False)

    with mock.patch.object(server.version_check,
                           'get_latest_version_for_current',
                           return_value=None), \
         mock.patch.object(server.common,
                           'get_skypilot_version_on_disk',
                           return_value=sky.__version__), \
         mock.patch.object(server.server_config,
                           'load_external_proxy_config',
                           return_value=external_proxy_config):
        response = asyncio.run(server.health(request))

    assert isinstance(response, responses.APIHealthResponse)
    assert response.commit_timestamp == sky.__commit_timestamp__
    assert response.deployment_timestamp == server._SERVER_STARTED_AT
    _parse_iso8601(response.deployment_timestamp)


def test_anonymous_orchestration_health_omits_release_metadata():
    request = SimpleNamespace(
        state=SimpleNamespace(auth_user=None, anonymous_user=True))

    with mock.patch.object(server.versions,
                           'get_remote_api_version',
                           return_value=None):
        response = asyncio.run(server.health(request))

    assert response.model_dump(exclude_unset=True) == {
        'status': common.ApiServerStatus.HEALTHY,
    }


def test_modern_anonymous_health_omits_release_metadata():
    request = SimpleNamespace(
        state=SimpleNamespace(auth_user=None, anonymous_user=True))
    external_proxy_config = SimpleNamespace(enabled=False)

    with mock.patch.object(server.versions,
                           'get_remote_api_version',
                           return_value=67), \
         mock.patch.object(server.version_check,
                           'get_latest_version_for_current',
                           return_value=None), \
         mock.patch.object(server.common,
                           'get_skypilot_version_on_disk',
                           return_value=sky.__version__), \
         mock.patch.object(server.server_config,
                           'load_external_proxy_config',
                           return_value=external_proxy_config):
        response = asyncio.run(server.health(request))

    serialized = response.model_dump(exclude_unset=True)
    assert serialized['status'] == common.ApiServerStatus.NEEDS_AUTH
    assert 'commit_timestamp' not in serialized
    assert 'deployment_timestamp' not in serialized


def test_missing_commit_timestamp_is_omitted():
    request = SimpleNamespace(
        state=SimpleNamespace(auth_user=None, anonymous_user=False))
    external_proxy_config = SimpleNamespace(enabled=False)

    with mock.patch.object(server.sky, '__commit_timestamp__', None), \
         mock.patch.object(server.version_check,
                           'get_latest_version_for_current',
                           return_value=None), \
         mock.patch.object(server.common,
                           'get_skypilot_version_on_disk',
                           return_value=sky.__version__), \
         mock.patch.object(server.server_config,
                           'load_external_proxy_config',
                           return_value=external_proxy_config):
        response = asyncio.run(server.health(request))

    serialized = response.model_dump(exclude_unset=True)
    assert 'commit_timestamp' not in serialized
    assert serialized['deployment_timestamp'] == server._SERVER_STARTED_AT
