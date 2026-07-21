"""Tests for the Kubernetes autodown event breadcrumb reader."""
import datetime
import inspect
import pickle
from typing import Any, get_type_hints, Optional
from unittest import mock

from sky.provision.kubernetes import autostop_events
from sky.provision.kubernetes import instance as k8s_instance

_PROVIDER_CONFIG = {'namespace': 'sky-ns', 'context': 'my-ctx'}


def test_autostop_event_instance_surface_is_stable():
    expected_parameters = {
        'emit_autostop_event_best_effort':
            ('provider_config', 'cluster_name_on_cloud'),
        'get_cluster_autostop_event':
            ('provider_config', 'cluster_name_on_cloud', 'since'),
    }
    expected_type_hints = {
        'emit_autostop_event_best_effort': {
            'provider_config': dict[str, Any],
            'cluster_name_on_cloud': str,
            'return': type(None),
        },
        'get_cluster_autostop_event': {
            'provider_config': dict[str, Any],
            'cluster_name_on_cloud': str,
            'since': float | None,
            'return': dict[str, Any] | None,
        },
    }

    assert k8s_instance.AUTOSTOP_EVENT_REASON == 'SkyPilotAutodown'
    for name, parameter_names in expected_parameters.items():
        symbol = getattr(k8s_instance, name)
        signature = inspect.signature(symbol)
        assert tuple(signature.parameters) == parameter_names
        assert get_type_hints(symbol) == expected_type_hints[name]
        assert symbol.__module__ == k8s_instance.__name__
        assert pickle.loads(pickle.dumps(symbol)) is symbol
        assert symbol is getattr(autostop_events, name)

    get_event_signature = inspect.signature(
        k8s_instance.get_cluster_autostop_event)
    assert get_event_signature.parameters['since'].default is None


def _make_event(name: str,
                reason: str = k8s_instance.AUTOSTOP_EVENT_REASON,
                message: str = 'autodowning',
                last_timestamp: Optional[datetime.datetime] = None):
    """Build a mock core/v1 Event with the fields the reader inspects."""
    event = mock.MagicMock()
    event.reason = reason
    event.message = message
    event.involved_object.name = name
    event.last_timestamp = last_timestamp
    event.event_time = None
    event.metadata.creation_timestamp = last_timestamp
    return event


def _patch_core_api(monkeypatch, events=None, raises=None):
    core_api_mock = mock.MagicMock()
    if raises is not None:
        core_api_mock.list_namespaced_event.side_effect = raises
    else:
        response = mock.MagicMock()
        response.items = events or []
        core_api_mock.list_namespaced_event.return_value = response
    monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                        lambda *args, **kwargs: core_api_mock)
    return core_api_mock


def test_emit_autostop_event_preserves_gateway_contract(monkeypatch):
    core_api_mock = _patch_core_api(monkeypatch)
    k8s_client = mock.MagicMock()
    monkeypatch.setattr(k8s_instance.kubernetes.kubernetes, 'client',
                        k8s_client)

    k8s_instance.emit_autostop_event_best_effort(_PROVIDER_CONFIG,
                                                 'my-cluster-abc')

    k8s_client.V1ObjectMeta.assert_called_once()
    metadata_kwargs = k8s_client.V1ObjectMeta.call_args.kwargs
    assert metadata_kwargs['namespace'] == 'sky-ns'
    assert metadata_kwargs['name'].startswith(
        'my-cluster-abc-head.skyautodown.')
    k8s_client.V1ObjectReference.assert_called_once_with(
        kind='Pod', name='my-cluster-abc-head', namespace='sky-ns')
    k8s_client.V1EventSource.assert_called_once_with(
        component='skypilot-skylet')
    event_kwargs = k8s_client.CoreV1Event.call_args.kwargs
    assert event_kwargs['reason'] == 'SkyPilotAutodown'
    assert event_kwargs['message'] == (
        'Cluster is autodowning after reaching its idle timeout.')
    assert event_kwargs['type'] == 'Normal'
    assert event_kwargs['first_timestamp'] == event_kwargs['last_timestamp']
    assert event_kwargs['first_timestamp'].tzinfo == datetime.timezone.utc
    core_api_mock.create_namespaced_event.assert_called_once_with(
        'sky-ns',
        k8s_client.CoreV1Event.return_value,
        _request_timeout=k8s_instance.kubernetes.API_TIMEOUT)


def test_emit_autostop_event_never_raises(monkeypatch):
    core_api_mock = _patch_core_api(monkeypatch)
    k8s_client = mock.MagicMock()
    k8s_client.CoreV1Event.side_effect = RuntimeError('bad event payload')
    monkeypatch.setattr(k8s_instance.kubernetes.kubernetes, 'client',
                        k8s_client)

    k8s_instance.emit_autostop_event_best_effort(_PROVIDER_CONFIG,
                                                 'my-cluster-abc')

    core_api_mock.create_namespaced_event.assert_not_called()


def test_autostop_event_matches_head_pod(monkeypatch):
    ts = datetime.datetime(2026, 6, 5, tzinfo=datetime.timezone.utc)
    core_api_mock = _patch_core_api(
        monkeypatch,
        events=[_make_event('my-cluster-abc-head', last_timestamp=ts)])

    result = k8s_instance.get_cluster_autostop_event(_PROVIDER_CONFIG,
                                                     'my-cluster-abc')

    assert result is not None
    assert result['reason'] == k8s_instance.AUTOSTOP_EVENT_REASON
    assert result['transitioned_at'] == int(ts.timestamp())
    core_api_mock.list_namespaced_event.assert_called_once_with(
        'sky-ns',
        field_selector='reason=SkyPilotAutodown',
        _request_timeout=k8s_instance.kubernetes.API_TIMEOUT)


def test_autostop_event_ignores_other_clusters(monkeypatch):
    ts = datetime.datetime(2026, 6, 5, tzinfo=datetime.timezone.utc)
    _patch_core_api(
        monkeypatch,
        events=[_make_event('other-cluster-head', last_timestamp=ts)])

    result = k8s_instance.get_cluster_autostop_event(_PROVIDER_CONFIG,
                                                     'my-cluster-abc')

    assert result is None


def test_autostop_event_ignores_prefix_sibling(monkeypatch):
    # A sibling cluster whose name shares this cluster's prefix must not match;
    # the head pod name is matched exactly, not by prefix.
    ts = datetime.datetime(2026, 6, 5, tzinfo=datetime.timezone.utc)
    _patch_core_api(
        monkeypatch,
        events=[_make_event('my-cluster-abc-2-head', last_timestamp=ts)])

    result = k8s_instance.get_cluster_autostop_event(_PROVIDER_CONFIG,
                                                     'my-cluster-abc')

    assert result is None


def test_autostop_event_ignores_stale_before_since(monkeypatch):
    # A breadcrumb from a previous incarnation (before the current launch) must
    # be ignored so a relaunched-then-torn-down cluster is not misattributed.
    stale = datetime.datetime(2026, 6, 5, 10, tzinfo=datetime.timezone.utc)
    _patch_core_api(
        monkeypatch,
        events=[_make_event('my-cluster-abc-head', last_timestamp=stale)])
    launched_at = datetime.datetime(2026,
                                    6,
                                    5,
                                    11,
                                    tzinfo=datetime.timezone.utc).timestamp()

    result = k8s_instance.get_cluster_autostop_event(_PROVIDER_CONFIG,
                                                     'my-cluster-abc',
                                                     since=launched_at)

    assert result is None


def test_autostop_event_kept_after_since(monkeypatch):
    fresh = datetime.datetime(2026, 6, 5, 12, tzinfo=datetime.timezone.utc)
    _patch_core_api(
        monkeypatch,
        events=[_make_event('my-cluster-abc-head', last_timestamp=fresh)])
    launched_at = datetime.datetime(2026,
                                    6,
                                    5,
                                    11,
                                    tzinfo=datetime.timezone.utc).timestamp()

    result = k8s_instance.get_cluster_autostop_event(_PROVIDER_CONFIG,
                                                     'my-cluster-abc',
                                                     since=launched_at)

    assert result is not None
    assert result['transitioned_at'] == int(fresh.timestamp())


def test_autostop_event_returns_latest(monkeypatch):
    older = datetime.datetime(2026, 6, 5, 10, tzinfo=datetime.timezone.utc)
    newer = datetime.datetime(2026, 6, 5, 12, tzinfo=datetime.timezone.utc)
    _patch_core_api(monkeypatch,
                    events=[
                        _make_event('my-cluster-abc-head',
                                    message='old',
                                    last_timestamp=older),
                        _make_event('my-cluster-abc-head',
                                    message='new',
                                    last_timestamp=newer),
                    ])

    result = k8s_instance.get_cluster_autostop_event(_PROVIDER_CONFIG,
                                                     'my-cluster-abc')

    assert result is not None
    assert result['message'] == 'new'
    assert result['transitioned_at'] == int(newer.timestamp())


def test_autostop_event_no_events(monkeypatch):
    _patch_core_api(monkeypatch, events=[])

    assert k8s_instance.get_cluster_autostop_event(_PROVIDER_CONFIG,
                                                   'my-cluster-abc') is None


def test_autostop_event_never_raises(monkeypatch):
    _patch_core_api(monkeypatch, raises=RuntimeError('k8s API down'))

    # Best-effort diagnostics: an API error resolves to None, not an exception.
    assert k8s_instance.get_cluster_autostop_event(_PROVIDER_CONFIG,
                                                   'my-cluster-abc') is None
