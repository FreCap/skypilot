"""Facade tests for diagnostic volume refresh projection wiring."""

from __future__ import annotations

import contextlib
import dataclasses
from unittest import mock

import pytest

from sky import global_user_state
from sky import provision
from sky.utils import status_lib
from sky.volumes import refresh_projection
from sky.volumes.server import core


@dataclasses.dataclass
class _Handle:
    name: str
    cloud: str = 'test-cloud'


def _volume(
    name: str,
    *,
    status: status_lib.VolumeStatus = status_lib.VolumeStatus.READY,
    error: str | None = None,
    usedby_pods: list[str] | None = None,
    usedby_clusters: list[str] | None = None,
) -> dict[str, object]:
    return {
        'name': name,
        'handle': _Handle(name),
        'status': status,
        'error_message': error,
        'usedby_pods': [] if usedby_pods is None else usedby_pods,
        'usedby_clusters': ([] if usedby_clusters is None else usedby_clusters),
    }


@dataclasses.dataclass
class _FacadeFakes:
    get_errors: mock.MagicMock
    get_usedby: mock.MagicMock
    map_usedby: mock.MagicMock
    get_latest: mock.MagicMock
    update_status: mock.MagicMock
    refresh_config: mock.MagicMock
    update_config: mock.MagicMock


def _install_facade_fakes(
    monkeypatch: pytest.MonkeyPatch,
    volumes: list[dict[str, object]],
    observed_usage: dict[str, tuple[list[str], list[str]]],
    events: list[str],
    *,
    errors: dict[str, str | None] | None = None,
    failed_volume_names: set[str] | None = None,
    missing_latest_names: set[str] | None = None,
) -> _FacadeFakes:
    errors = {} if errors is None else errors
    failed_volume_names = (set() if failed_volume_names is None else
                           failed_volume_names)
    missing_latest_names = (set() if missing_latest_names is None else
                            missing_latest_names)
    volumes_by_name = {volume['name']: volume for volume in volumes}

    monkeypatch.setattr(global_user_state, 'get_volumes',
                        mock.MagicMock(return_value=volumes))

    def _get_errors(cloud: str, configs: list[object]) -> dict[str, str | None]:
        del configs
        events.append(f'error-batch:{cloud}')
        return errors

    get_errors = mock.MagicMock(side_effect=_get_errors)
    monkeypatch.setattr(provision, 'get_all_volumes_errors', get_errors)

    def _get_usedby(cloud: str, configs: list[object]):
        del configs
        events.append(f'usedby-batch:{cloud}')
        return {}, {}, failed_volume_names

    get_usedby = mock.MagicMock(side_effect=_get_usedby)
    monkeypatch.setattr(provision, 'get_all_volumes_usedby', get_usedby)

    def _map_usedby(cloud: str, all_pods: dict[str, object],
                    all_clusters: dict[str, object],
                    handle: _Handle) -> tuple[list[str], list[str]]:
        del cloud, all_pods, all_clusters
        events.append(f'map:{handle.name}')
        return observed_usage[handle.name]

    map_usedby = mock.MagicMock(side_effect=_map_usedby)
    monkeypatch.setattr(provision, 'map_all_volumes_usedby', map_usedby)

    def _get_latest(name: str) -> dict[str, object] | None:
        events.append(f'latest:{name}')
        if name in missing_latest_names:
            return None
        return volumes_by_name[name]

    get_latest = mock.MagicMock(side_effect=_get_latest)
    monkeypatch.setattr(global_user_state, 'get_volume_by_name', get_latest)

    def _update_status(name: str, **kwargs: object) -> None:
        del kwargs
        events.append(f'write:{name}')

    update_status = mock.MagicMock(side_effect=_update_status)
    monkeypatch.setattr(global_user_state, 'update_volume_status',
                        update_status)

    def _refresh_config(cloud: str, handle: _Handle) -> tuple[bool, _Handle]:
        del cloud
        events.append(f'config:{handle.name}')
        return False, handle

    refresh_config = mock.MagicMock(side_effect=_refresh_config)
    monkeypatch.setattr(provision, 'refresh_volume_config', refresh_config)

    update_config = mock.MagicMock()
    monkeypatch.setattr(global_user_state, 'update_volume_config',
                        update_config)

    @contextlib.contextmanager
    def _lock(name: str):
        events.append(f'lock-enter:{name}')
        try:
            yield
        finally:
            events.append(f'lock-exit:{name}')

    monkeypatch.setattr(core, '_volume_lock', _lock)
    return _FacadeFakes(get_errors, get_usedby, map_usedby, get_latest,
                        update_status, refresh_config, update_config)


def test_refresh_batches_once_and_defers_projection_after_authoritative_work(
        monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    volumes = [_volume('volume-a'), _volume('volume-b')]
    pods_a = ['pod-a-2', 'pod-a-1']
    clusters_a = ['cluster-a']
    pods_b = ['pod-b']
    clusters_b = ['cluster-b-2', 'cluster-b-1']
    usage = {
        'volume-a': (pods_a, clusters_a),
        'volume-b': (pods_b, clusters_b),
    }
    fakes = _install_facade_fakes(monkeypatch,
                                  volumes,
                                  usage,
                                  events,
                                  errors={'volume-a': ''})
    real_compare = refresh_projection.compare_volume_refresh_projection
    compared_snapshots: list[refresh_projection.VolumeRefreshSnapshot] = []

    def _compare(
        snapshot: refresh_projection.VolumeRefreshSnapshot,
        authoritative: refresh_projection.VolumeRefreshProjection,
    ) -> refresh_projection.VolumeRefreshShadowOutcome:
        events.append('candidate')
        compared_snapshots.append(snapshot)
        return real_compare(snapshot, authoritative)

    monkeypatch.setattr(refresh_projection, 'compare_volume_refresh_projection',
                        _compare)
    warning = mock.MagicMock()
    monkeypatch.setattr(core.logger, 'warning', warning)

    core.volume_refresh()

    fakes.get_errors.assert_called_once()
    fakes.get_usedby.assert_called_once()
    assert fakes.map_usedby.call_count == 2
    assert fakes.get_latest.call_count == 2
    assert fakes.refresh_config.call_count == 2
    assert fakes.update_status.call_count == 2
    calls_by_name = {
        call.args[0]: call for call in fakes.update_status.call_args_list
    }
    assert calls_by_name['volume-a'].kwargs['usedby_pods'] is pods_a
    assert calls_by_name['volume-a'].kwargs['usedby_clusters'] is clusters_a
    assert calls_by_name['volume-b'].kwargs['usedby_pods'] is pods_b
    assert calls_by_name['volume-b'].kwargs['usedby_clusters'] is clusters_b
    first_candidate = events.index('candidate')
    authoritative_events = [
        index for index, event in enumerate(events)
        if event.startswith(('write:', 'config:', 'lock-exit:'))
    ]
    assert first_candidate > max(authoritative_events)
    assert events.count('candidate') == 2
    assert isinstance(compared_snapshots[0], refresh_projection.ObservedRefresh)
    assert compared_snapshots[0].observed_error == ''
    warning.assert_not_called()


def test_failed_usedby_fetch_defers_skip_without_per_volume_work(
        monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    volumes = [_volume('failed-volume')]
    fakes = _install_facade_fakes(monkeypatch,
                                  volumes, {'failed-volume': ([], [])},
                                  events,
                                  failed_volume_names={'failed-volume'})
    comparisons: list[tuple[refresh_projection.VolumeRefreshSnapshot,
                            refresh_projection.VolumeRefreshProjection]] = []
    real_compare = refresh_projection.compare_volume_refresh_projection

    def _compare(
        snapshot: refresh_projection.VolumeRefreshSnapshot,
        authoritative: refresh_projection.VolumeRefreshProjection,
    ) -> refresh_projection.VolumeRefreshShadowOutcome:
        comparisons.append((snapshot, authoritative))
        return real_compare(snapshot, authoritative)

    monkeypatch.setattr(refresh_projection, 'compare_volume_refresh_projection',
                        _compare)

    core.volume_refresh()

    assert len(comparisons) == 1
    assert isinstance(comparisons[0][0], refresh_projection.UsedByFetchFailed)
    assert isinstance(comparisons[0][1], refresh_projection.Skip)
    fakes.map_usedby.assert_not_called()
    fakes.get_latest.assert_not_called()
    fakes.update_status.assert_not_called()
    fakes.refresh_config.assert_not_called()
    assert not any(event.startswith('lock-') for event in events)


def test_missing_handle_and_missing_latest_row_keep_existing_skip_boundaries(
        monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    no_handle = _volume('no-handle')
    no_handle['handle'] = None
    missing_row = _volume('missing-row')
    fakes = _install_facade_fakes(monkeypatch, [no_handle, missing_row],
                                  {'missing-row': ([], [])},
                                  events,
                                  missing_latest_names={'missing-row'})
    compare = mock.MagicMock()
    monkeypatch.setattr(refresh_projection, 'compare_volume_refresh_projection',
                        compare)

    core.volume_refresh()

    fakes.get_errors.assert_called_once()
    fakes.get_usedby.assert_called_once()
    fakes.map_usedby.assert_called_once()
    fakes.get_latest.assert_called_once_with('missing-row')
    fakes.update_status.assert_not_called()
    fakes.refresh_config.assert_not_called()
    compare.assert_not_called()
    assert events.count('lock-enter:missing-row') == 1
    assert events.count('lock-exit:missing-row') == 1


@pytest.mark.parametrize(
    ('dimension', 'volume_count', 'usage_factory', 'expected_candidates'), [
        ('snapshots', 129, lambda _: ([], []), 128),
        ('references', 1, lambda _: (['pod'] * 4097, []), 0),
        ('identity-bytes', 1, lambda _: (['\u00e9' * 131073], []), 0),
    ])
def test_capture_budgets_never_interrupt_authoritative_work(
    monkeypatch: pytest.MonkeyPatch,
    dimension: str,
    volume_count: int,
    usage_factory,
    expected_candidates: int,
) -> None:
    del dimension
    events: list[str] = []
    volumes = [
        _volume(f'volume-{index}',
                status=status_lib.VolumeStatus.NOT_READY,
                error='old-error') for index in range(volume_count)
    ]
    usage = {
        f'volume-{index}': usage_factory(index) for index in range(volume_count)
    }
    fakes = _install_facade_fakes(monkeypatch, volumes, usage, events)
    real_compare = refresh_projection.compare_volume_refresh_projection
    compared_snapshots: list[refresh_projection.VolumeRefreshSnapshot] = []

    def _compare(
        snapshot: refresh_projection.VolumeRefreshSnapshot,
        authoritative: refresh_projection.VolumeRefreshProjection,
    ) -> refresh_projection.VolumeRefreshShadowOutcome:
        compared_snapshots.append(snapshot)
        return real_compare(snapshot, authoritative)

    monkeypatch.setattr(refresh_projection, 'compare_volume_refresh_projection',
                        _compare)
    warning = mock.MagicMock()
    monkeypatch.setattr(core.logger, 'warning', warning)

    core.volume_refresh()

    assert fakes.update_status.call_count == volume_count
    assert fakes.refresh_config.call_count == volume_count
    assert len(compared_snapshots) == expected_candidates
    assert all(
        isinstance(snapshot, refresh_projection.ObservedRefresh)
        for snapshot in compared_snapshots)
    warning.assert_called_once()
    warning_args = warning.call_args.args
    assert warning_args[1] == expected_candidates
    assert warning_args[2] == expected_candidates
    assert warning_args[6] == volume_count - expected_candidates


def test_mixed_shadow_outcomes_are_bounded_sanitized_and_continue(
        monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    volumes = [_volume(f'volume-{index}') for index in range(5)]
    usage = {
        f'volume-{index}': ([f'usage-secret-{index}'], []) for index in range(5)
    }
    fakes = _install_facade_fakes(
        monkeypatch,
        volumes,
        usage,
        events,
        errors={'volume-0': 'provider-payload-secret'})
    real_projector = refresh_projection.project_volume_refresh
    projector_calls = 0

    class _EqualityError:

        def __eq__(self, other: object) -> bool:
            del other
            raise RuntimeError('comparison-exception-secret')

    def _project(
        snapshot: refresh_projection.VolumeRefreshSnapshot,
    ) -> refresh_projection.VolumeRefreshProjection:
        nonlocal projector_calls
        call_index = projector_calls
        projector_calls += 1
        if call_index == 0:
            return refresh_projection.Skip()
        if call_index == 1:
            raise RuntimeError('projector-exception-secret')
        if call_index == 2:
            return _EqualityError()  # type: ignore[return-value]
        if call_index == 4:
            return refresh_projection.Skip()
        return real_projector(snapshot)

    monkeypatch.setattr(refresh_projection, 'project_volume_refresh', _project)
    warning = mock.MagicMock()
    monkeypatch.setattr(core.logger, 'warning', warning)

    core.volume_refresh()

    assert projector_calls == 5
    assert fakes.update_status.call_count == 5
    assert fakes.refresh_config.call_count == 5
    warning.assert_called_once()
    warning_args = warning.call_args.args
    rendered_warning = warning_args[0] % warning_args[1:]
    assert warning_args[1:7] == (5, 1, 2, 1, 1, 0)
    assert warning_args[7].split(',') == ['volume-0', 'volume-1', 'volume-2']
    for secret in ('provider-payload-secret', 'usage-secret',
                   'projector-exception-secret', 'comparison-exception-secret'):
        assert secret not in rendered_warning


def test_shadow_warning_failure_does_not_escape_after_authoritative_work(
        monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    volumes = [_volume('volume-a')]
    fakes = _install_facade_fakes(monkeypatch, volumes,
                                  {'volume-a': (['pod-a'], [])}, events)
    compare = mock.MagicMock(
        return_value=refresh_projection.VolumeRefreshShadowOutcome.MISMATCH)
    monkeypatch.setattr(refresh_projection, 'compare_volume_refresh_projection',
                        compare)
    warning = mock.MagicMock(side_effect=RuntimeError('logging unavailable'))
    monkeypatch.setattr(core.logger, 'warning', warning)

    core.volume_refresh()

    fakes.update_status.assert_called_once()
    fakes.refresh_config.assert_called_once()
    compare.assert_called_once()
    warning.assert_called_once()
