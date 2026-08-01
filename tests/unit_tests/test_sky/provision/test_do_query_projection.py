"""Tests for DigitalOcean query status projection."""

from __future__ import annotations

import copy
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from sky import provision
from sky.provision.do import instance as do_instance
from sky.provision.do import query_projection
from sky.utils import status_lib


class _RecordingInstances(dict[str, Any]):
    """Dictionary that records calls to its values view."""

    def __init__(self, instances: dict[str, Any]) -> None:
        super().__init__(instances)
        self.values_calls = 0

    def values(self):
        self.values_calls += 1
        return super().values()


class _RecordingRow:
    """Instance row that records field-access order."""

    def __init__(self, label: str, values: dict[str, Any],
                 accesses: list[tuple[str, str]]) -> None:
        self._label = label
        self._values = values
        self._accesses = accesses

    def __getitem__(self, key: str) -> Any:
        self._accesses.append((self._label, key))
        return self._values[key]


def _dict_key_error_message(value: Any) -> str:
    """Return this interpreter's ordinary unhashable-dict-key message."""
    mapping: dict[Any, None] = {}
    try:
        mapping[value] = None
    except TypeError as error:
        return str(error)
    raise AssertionError(f'{value!r} is unexpectedly hashable.')


def test_project_query_instances_empty_result():
    instances = _RecordingInstances({})

    result = query_projection.project_query_instances(instances,
                                                      status_lib.ClusterStatus)

    assert type(result) is dict
    assert not result
    assert instances.values_calls == 1


def test_project_query_instances_all_states_order_and_input_unchanged():
    instances = {
        'provider-key-new': {
            'name': 'node-new',
            'status': 'new'
        },
        'provider-key-archive': {
            'name': 'node-archive',
            'status': 'archive'
        },
        'provider-key-active': {
            'name': 'node-active',
            'status': 'active'
        },
        'provider-key-off': {
            'name': 'node-off',
            'status': 'off'
        },
    }
    original = copy.deepcopy(instances)

    result = query_projection.project_query_instances(instances,
                                                      status_lib.ClusterStatus)

    assert list(result.items()) == [
        ('node-new', (status_lib.ClusterStatus.INIT, None)),
        ('node-archive', (status_lib.ClusterStatus.INIT, None)),
        ('node-active', (status_lib.ClusterStatus.UP, None)),
        ('node-off', (status_lib.ClusterStatus.STOPPED, None)),
    ]
    assert instances == original


def test_project_query_instances_duplicate_names_keep_first_position_last_value(
):
    instances = {
        'provider-key-first': {
            'name': 'duplicate',
            'status': 'new'
        },
        'provider-key-middle': {
            'name': 'middle',
            'status': 'active'
        },
        'provider-key-last': {
            'name': 'duplicate',
            'status': 'off'
        },
    }

    result = query_projection.project_query_instances(instances,
                                                      status_lib.ClusterStatus)

    assert list(result.items()) == [
        ('duplicate', (status_lib.ClusterStatus.STOPPED, None)),
        ('middle', (status_lib.ClusterStatus.UP, None)),
    ]


def test_project_query_instances_reads_values_once_and_fields_in_legacy_order():
    accesses: list[tuple[str, str]] = []
    instances = _RecordingInstances({
        'provider-key-first': _RecordingRow('first', {
            'name': 'node-first',
            'status': 'active'
        }, accesses),
        'provider-key-second': _RecordingRow('second', {
            'name': 'node-second',
            'status': 'off'
        }, accesses),
    })

    result = query_projection.project_query_instances(instances,
                                                      status_lib.ClusterStatus)

    assert list(result.items()) == [
        ('node-first', (status_lib.ClusterStatus.UP, None)),
        ('node-second', (status_lib.ClusterStatus.STOPPED, None)),
    ]
    assert instances.values_calls == 1
    assert accesses == [
        ('first', 'status'),
        ('first', 'name'),
        ('second', 'status'),
        ('second', 'name'),
    ]


@pytest.mark.parametrize(
    ('row_values', 'expected_exception', 'expected_message',
     'expected_accesses'),
    [
        ({
            'name': 'node',
            'status': 'unknown'
        }, KeyError, "'unknown'", [('row', 'status')]),
        ({
            'name': 'node'
        }, KeyError, "'status'", [('row', 'status')]),
        ({
            'status': 'active'
        }, KeyError, "'name'", [('row', 'status'), ('row', 'name')]),
        ({
            'name': [],
            'status': 'active'
        }, TypeError, _dict_key_error_message([]), [('row', 'status'),
                                                    ('row', 'name')]),
    ],
    ids=('unknown-state', 'missing-status', 'missing-name', 'unhashable-name'),
)
def test_project_query_instances_failure_semantics_without_retry(
    row_values: dict[str, Any],
    expected_exception: type[Exception],
    expected_message: str,
    expected_accesses: list[tuple[str, str]],
):
    accesses: list[tuple[str, str]] = []
    instances = _RecordingInstances(
        {'provider-key': _RecordingRow('row', row_values, accesses)})

    with pytest.raises(expected_exception) as exception_info:
        query_projection.project_query_instances(instances,
                                                 status_lib.ClusterStatus)

    assert str(exception_info.value) == expected_message
    assert instances.values_calls == 1
    assert accesses == expected_accesses


def test_direct_query_calls_helper_and_projector_once_with_exact_objects(
        monkeypatch: pytest.MonkeyPatch):
    helper_result = {'provider-key': object()}
    projected_result = {'node': (status_lib.ClusterStatus.UP, None)}
    filter_instances = mock.Mock(return_value=helper_result)
    projector = mock.Mock(return_value=projected_result)
    monkeypatch.setattr(do_instance.utils, 'filter_instances', filter_instances)
    monkeypatch.setattr(do_instance.query_projection, 'project_query_instances',
                        projector)

    result = do_instance.query_instances(
        'display-name',
        'cloud-name',
        provider_config={'region': 'test-region'},
        non_terminated_only=False,
        retry_if_missing=True,
    )

    filter_instances.assert_called_once_with('cloud-name', status_filters=None)
    projector.assert_called_once_with(helper_result, status_lib.ClusterStatus)
    assert projector.call_args.args[0] is helper_result
    assert projector.call_args.args[1] is status_lib.ClusterStatus
    assert result is projected_result


def test_direct_query_rejects_missing_provider_config_before_calls(
        monkeypatch: pytest.MonkeyPatch):
    filter_instances = mock.Mock()
    projector = mock.Mock()
    monkeypatch.setattr(do_instance.utils, 'filter_instances', filter_instances)
    monkeypatch.setattr(do_instance.query_projection, 'project_query_instances',
                        projector)

    with pytest.raises(AssertionError) as exception_info:
        do_instance.query_instances('display-name',
                                    'cloud-name',
                                    provider_config=None)

    assert exception_info.value.args == (('cloud-name', None),)
    filter_instances.assert_not_called()
    projector.assert_not_called()


def test_direct_query_uses_current_instance_status_binding(
        monkeypatch: pytest.MonkeyPatch):
    replacement_init = object()
    replacement_status = SimpleNamespace(INIT=replacement_init,
                                         UP=object(),
                                         STOPPED=object())
    monkeypatch.setattr(do_instance, 'status_lib',
                        SimpleNamespace(ClusterStatus=replacement_status))
    filter_instances = mock.Mock(
        return_value={'provider-key': {
            'name': 'node',
            'status': 'new',
        }})
    monkeypatch.setattr(do_instance.utils, 'filter_instances', filter_instances)

    result = do_instance.query_instances(
        'display-name',
        'cloud-name',
        provider_config={'region': 'test-region'},
    )

    filter_instances.assert_called_once_with('cloud-name', status_filters=None)
    assert result == {'node': (replacement_init, None)}


def test_facade_query_preserves_projector_result_identity(
        monkeypatch: pytest.MonkeyPatch):
    helper_result = {'provider-key': object()}
    projected_result = {'node': (status_lib.ClusterStatus.STOPPED, None)}
    filter_instances = mock.Mock(return_value=helper_result)
    projector = mock.Mock(return_value=projected_result)
    monkeypatch.setattr(provision, '_registered_provisioners', {})
    monkeypatch.setattr(provision, '_registered_provisioner_bundles', {})
    monkeypatch.setattr(provision, '_legacy_mixed_owner_diagnostics', set())
    monkeypatch.setattr(do_instance.utils, 'filter_instances', filter_instances)
    monkeypatch.setattr(do_instance.query_projection, 'project_query_instances',
                        projector)

    result = provision.query_instances(
        'do',
        'display-name',
        'cloud-name',
        provider_config={'region': 'test-region'},
        non_terminated_only=False,
        retry_if_missing=True,
    )

    filter_instances.assert_called_once_with('cloud-name', status_filters=None)
    projector.assert_called_once_with(helper_result, status_lib.ClusterStatus)
    assert projector.call_args.args[1] is status_lib.ClusterStatus
    assert result is projected_result
