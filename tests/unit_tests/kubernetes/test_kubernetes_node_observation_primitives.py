"""Tests for bounded Kubernetes node observation primitives."""
# pylint: disable=protected-access

import concurrent.futures
import gzip
import io
import json
import threading
from unittest import mock

import pytest
import urllib3

from sky.provision.kubernetes import utils


class _Response:
    """Minimal urllib3-like byte response with observable ownership."""

    def __init__(self, payload: bytes) -> None:
        self._stream = io.BytesIO(payload)
        self._length = len(payload)
        self.closed_by_owner = False
        self.released = False
        self.ownership_events: list[str] = []

    @property
    def length_remaining(self) -> int:
        return self._length - self._stream.tell()

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        self.closed_by_owner = True
        self.ownership_events.append('close')

    def release_conn(self) -> None:
        self.released = True
        self.ownership_events.append('release')


class _UnknownLengthResponse(_Response):
    """Byte response whose EOF cannot be known without a physical read."""

    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self.read_sizes: list[int] = []
        self.physically_read_bytes = 0

    @property
    def length_remaining(self) -> int:
        raise AttributeError('response length is unknown')

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        data = super().read(size)
        self.physically_read_bytes += len(data)
        return data


def _node(name,
          *,
          cpu='4',
          memory='16Gi',
          allocatable_cpu='3900m',
          allocatable_memory='15Gi',
          ready=True,
          annotations=None,
          labels=None):
    return {
        'apiVersion': 'v1',
        'kind': 'Node',
        'metadata': {
            'name': name,
            'labels': labels if labels is not None else {
                'secret-label': 'must-not-be-retained',
            },
            'annotations': annotations or {
                'secret-annotation': 'must-not-be-retained',
            },
        },
        'status': {
            'capacity': {
                'cpu': cpu,
                'memory': memory,
            },
            'allocatable': {
                'cpu': allocatable_cpu,
                'memory': allocatable_memory,
            },
            'conditions': [{
                'type': 'Ready',
                'status': 'True' if ready else 'False',
            }],
        },
    }


def _payload(nodes, *, metadata=None) -> bytes:
    return json.dumps({
        'apiVersion': 'v1',
        'kind': 'NodeList',
        'metadata': metadata or {},
        'items': nodes,
    }).encode('utf-8')


def _read(payload,
          *,
          maximum_nodes=10,
          maximum_response_bytes=None,
          budget=None):
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    if maximum_response_bytes is None:
        maximum_response_bytes = len(payload)
    if budget is None:
        budget = utils.KubernetesObservationBudget(
            maximum_response_bytes=maximum_response_bytes,
            maximum_node_records=maximum_nodes)
    observation = utils.get_kubernetes_node_observation_uncached_bounded(
        core_api=core_api,
        maximum_nodes=maximum_nodes,
        maximum_response_bytes=maximum_response_bytes,
        budget=budget,
    )
    return observation, response, core_api, budget


def test_node_projection_streams_only_frozen_resources(monkeypatch):
    payload = _payload([
        _node('node-b', ready=False),
        _node('node-a', cpu='1000m', memory='8Gi'),
    ])
    monkeypatch.setattr(
        utils.V1Node,
        'from_dict',
        mock.MagicMock(side_effect=AssertionError('V1Node was retained')),
    )

    observation, response, core_api, budget = _read(payload)
    nodes = observation.node_resources

    assert [node.name for node in nodes] == ['node-b', 'node-a']
    nodes_by_name = {node.name: node for node in nodes}
    assert nodes_by_name['node-a'].is_ready
    assert nodes_by_name['node-a'].cpu_capacity == 1.0
    assert nodes_by_name['node-a'].memory_capacity_gb == 8.0
    assert nodes_by_name['node-a'].cpu_allocatable == 3.9
    assert nodes_by_name['node-a'].memory_allocatable_gb == 15.0
    assert nodes_by_name['node-a']._cpu_capacity_for_fitting == 1
    assert not nodes_by_name['node-b'].is_ready
    assert nodes_by_name['node-b'].cpu_capacity == 4.0
    assert nodes_by_name['node-b'].memory_capacity_gb == 16.0
    assert nodes_by_name['node-b'].cpu_allocatable == 3.9
    assert nodes_by_name['node-b'].memory_allocatable_gb == 15.0
    assert nodes_by_name['node-b']._cpu_capacity_for_fitting == 4.0
    assert all(not hasattr(node, 'labels') and not hasattr(node, 'annotations')
               for node in nodes)
    assert not observation.cpu_avoid_accelerator_label_keys
    assert 'must-not-be-retained' not in repr(observation)
    assert response.released
    assert not response.closed_by_owner
    assert response.ownership_events == ['release']
    assert budget.consumed_response_bytes == len(payload)
    assert budget.remaining_response_bytes == 0
    assert budget.consumed_node_records == 2
    core_api.list_node.assert_called_once_with(
        limit=11,
        _request_timeout=utils.kubernetes.API_TIMEOUT,
        _preload_content=False,
    )


@pytest.mark.parametrize(
    ('conditions', 'expected'),
    (
        ([{
            'type': 'MemoryPressure',
            'status': 'True'
        }], False),
        ([
            {
                'type': 'MemoryPressure',
                'status': 'True'
            },
            {
                'type': 'Ready',
                'status': 'True'
            },
            {
                'type': 'Ready',
                'status': 'False'
            },
        ], True),
        ([
            {
                'type': 'Ready',
                'status': 'False'
            },
            {
                'type': 'Ready',
                'status': 'True'
            },
        ], False),
        ([{
            'type': 'Ready',
            'status': 'true'
        }], False),
    ),
)
def test_legacy_and_streaming_readiness_share_transition(
        monkeypatch, conditions, expected):
    node = _node('node-a')
    node['status']['conditions'] = conditions
    transition = mock.MagicMock(
        wraps=utils._transition_kubernetes_node_readiness)
    monkeypatch.setattr(utils, '_transition_kubernetes_node_readiness',
                        transition)

    legacy_result = utils.V1Node.from_dict(node).is_ready()
    first_ready_index = next(
        (index for index, condition in enumerate(conditions)
         if condition['type'] == 'Ready'), None)
    expected_legacy_calls = (len(conditions) if first_ready_index is None else
                             first_ready_index + 1)
    assert transition.call_count == expected_legacy_calls

    transition.reset_mock()
    observation, _, _, _ = _read(_payload([node]))

    assert legacy_result is expected
    assert observation.node_resources[0].is_ready is expected
    assert transition.call_count == len(conditions)


def test_label_detection_preserves_priority_and_first_nonempty_match_order():
    payload = _payload([
        _node(
            'node-a',
            labels={
                # A lower-priority valid family occurs first in the provider
                # stream. Formatter priority, not arrival order, owns selection.
                utils.CoreWeaveLabelFormatter.LABEL_KEY: 'H100_NVLINK_80GB',
                # Empty matches do not decide the GKE formatter.
                utils.GKELabelFormatter.GPU_LABEL_KEY: '   ',
            }),
        _node(
            'node-b',
            labels={
                # The first nonempty GKE match is invalid and permanently
                # disqualifies GKE, even though a later GKE label is valid.
                utils.GKELabelFormatter.GPU_LABEL_KEY: 'not-a-gke-accelerator',
                utils.GFDLabelFormatter.LABEL_KEY: 'NVIDIA-L4',
            }),
        _node(
            'node-c',
            labels={
                utils.GKELabelFormatter.GPU_LABEL_KEY: 'nvidia-l4',
                # Karpenter outranks the earlier valid GFD and CoreWeave
                # matches in LABEL_FORMATTER_REGISTRY.
                utils.KarpenterLabelFormatter.LABEL_KEY: 'a100',
            }),
    ])

    observation, response, _, _ = _read(payload)

    assert observation.cpu_avoid_accelerator_label_keys == (
        utils.KarpenterLabelFormatter.LABEL_KEY,)
    assert [node.name for node in observation.node_resources
           ] == ['node-a', 'node-b', 'node-c']
    assert 'H100_NVLINK_80GB' not in repr(observation)
    assert 'not-a-gke-accelerator' not in repr(observation)
    assert response.ownership_events == ['release']


def test_label_detection_returns_all_keys_for_highest_priority_formatter():
    payload = _payload([
        _node('node-a',
              labels={
                  utils.GFDLabelFormatter.LABEL_KEY: 'NVIDIA-L4',
              }),
        _node('node-b',
              labels={
                  utils.GKELabelFormatter.GPU_LABEL_KEY: 'nvidia-l4',
              }),
    ])

    observation, _, _, _ = _read(payload)

    assert observation.cpu_avoid_accelerator_label_keys == (
        utils.GKELabelFormatter.GPU_LABEL_KEY,
        utils.GKELabelFormatter.TPU_LABEL_KEY,
    )


def test_derived_accelerator_label_key_tuple_is_count_bounded(monkeypatch):

    class OversizedFormatter:
        """Test formatter whose projected key set violates the bound."""

        @classmethod
        def match_label_key(cls, label_key):
            return label_key == 'example.com/accelerator'

        @classmethod
        def validate_label_value(cls, label_value):
            del label_value
            return True, ''

        @classmethod
        def get_label_keys(cls):
            return [
                f'example.com/accelerator-{index}'
                for index in range(utils._MAX_OBSERVED_ACCELERATOR_LABEL_KEYS +
                                   1)
            ]

    monkeypatch.setattr(utils, 'LABEL_FORMATTER_REGISTRY', [OversizedFormatter])
    payload = _payload([
        _node('node-a', labels={'example.com/accelerator': 'accelerator-value'})
    ])
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(utils.KubernetesObservationLimitError,
                       match='label keys exceed'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert response.ownership_events == ['close', 'release']


def test_exact_response_byte_bound_is_accepted():
    payload = _payload([_node('node-a')])
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with mock.patch.object(response, 'read', wraps=response.read) as read_mock:
        observation = utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert [node.name for node in observation.node_resources] == ['node-a']
    assert response.released
    assert budget.consumed_response_bytes == len(payload)
    assert budget.remaining_response_bytes == 0
    read_mock.assert_called_once_with(len(payload))


def test_gzip_response_uses_decoded_eof_and_preserves_byte_bound():
    node = _node(
        'node-a',
        annotations={'padding': 'x' * (utils.IJSON_BUFFER_SIZE + 1024)})
    payload = _payload([node])
    encoded_payload = gzip.compress(payload)
    assert len(encoded_payload) < utils.IJSON_BUFFER_SIZE < len(payload)
    response = urllib3.response.HTTPResponse(
        body=io.BytesIO(encoded_payload),
        headers={
            'content-encoding': 'gzip',
            'content-length': str(len(encoded_payload)),
        },
        preload_content=False,
        decode_content=True,
    )
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=1)

    with mock.patch.object(response, 'read', wraps=response.read) as read_mock:
        with mock.patch.object(response,
                               'release_conn',
                               wraps=response.release_conn) as release_mock:
            observation = (
                utils.get_kubernetes_node_observation_uncached_bounded(
                    core_api=core_api,
                    maximum_nodes=1,
                    maximum_response_bytes=len(payload),
                    budget=budget,
                ))

    assert [node.name for node in observation.node_resources] == ['node-a']
    assert budget.consumed_response_bytes == len(payload)
    assert read_mock.call_args_list[-1] == mock.call(1)
    assert read_mock.call_args_list.count(mock.call(1)) == 1
    release_mock.assert_called_once_with()


@pytest.mark.parametrize('response_headroom', (0, 100))
def test_exact_unknown_length_bound_uses_one_cached_eof_probe(
        response_headroom):
    payload = _payload([_node('node-a')])
    response = _UnknownLengthResponse(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    observation = utils.get_kubernetes_node_observation_uncached_bounded(
        core_api=core_api,
        maximum_nodes=10,
        maximum_response_bytes=len(payload) + response_headroom,
        budget=budget,
    )

    assert [node.name for node in observation.node_resources] == ['node-a']
    assert response.read_sizes == [len(payload), 1]
    assert response.physically_read_bytes == len(payload)
    assert response.ownership_events == ['release']
    assert budget.consumed_response_bytes == len(payload)


def test_unknown_length_oversized_sentinel_is_discarded_and_rejected():
    valid_payload = _payload([_node('node-a')])
    response = _UnknownLengthResponse(valid_payload + b'X')
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(valid_payload), maximum_node_records=10)

    with pytest.raises(utils.KubernetesObservationLimitError,
                       match='exceeds its accepted byte bound'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(valid_payload),
            budget=budget,
        )

    assert response.read_sizes == [len(valid_payload), 1]
    assert response.physically_read_bytes == len(valid_payload) + 1
    assert response.ownership_events == ['close', 'release']
    assert budget.consumed_response_bytes == len(valid_payload)


def test_nonempty_eof_probe_result_is_cached_without_a_second_probe():
    source = _UnknownLengthResponse(b'ab')
    budget = utils.KubernetesObservationBudget(maximum_response_bytes=1,
                                               maximum_node_records=1)
    reader = utils._KubernetesObservationReader(source,
                                                maximum_bytes=1,
                                                budget=budget)

    assert reader.read(1) == b'a'
    with pytest.raises(utils.KubernetesObservationLimitError,
                       match='exceeds its accepted byte bound'):
        reader.read(1)
    with pytest.raises(utils.KubernetesObservationLimitError,
                       match='already rejected'):
        reader.read(1)

    assert source.read_sizes == [1, 1]
    assert source.physically_read_bytes == 2


def test_erroring_eof_probe_is_cached_without_a_second_probe():
    read_sizes = []

    class ErroringProbeSource:
        """Return accepted content, then fail while performing the probe."""

        def read(self, size):
            read_sizes.append(size)
            if len(read_sizes) == 1:
                return b'a'
            raise OSError('probe failed after consuming its byte')

    budget = utils.KubernetesObservationBudget(maximum_response_bytes=1,
                                               maximum_node_records=1)
    reader = utils._KubernetesObservationReader(ErroringProbeSource(),
                                                maximum_bytes=1,
                                                budget=budget)

    assert reader.read(1) == b'a'
    with pytest.raises(OSError, match='probe failed'):
        reader.read(1)
    with pytest.raises(utils.KubernetesObservationLimitError,
                       match='already rejected'):
        reader.read(1)

    assert read_sizes == [1, 1]


def test_exact_aggregate_bound_with_looser_response_bound_is_accepted():
    payload = _payload([_node('node-a')])
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    observation, response, _, budget = _read(
        payload,
        maximum_response_bytes=len(payload) + 100,
        budget=budget,
    )

    assert [node.name for node in observation.node_resources] == ['node-a']
    assert response.ownership_events == ['release']
    assert budget.consumed_response_bytes == len(payload)
    assert budget.remaining_response_bytes == 0


@pytest.mark.parametrize('missing_capacity', ('cpu', 'memory'))
def test_node_projection_requires_capacity_fields(missing_capacity):
    node = _node('node-a')
    node['status']['capacity'].pop(missing_capacity)
    payload = _payload([node])
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(ValueError, match='resource fields are incomplete'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert response.closed_by_owner
    assert response.released
    assert response.ownership_events == ['close', 'release']


def test_node_projection_keeps_legacy_missing_allocatable_resource_defaults():
    node = _node('node-a')
    node['status']['allocatable'] = {}
    payload = _payload([node])

    observation, response, _, _ = _read(payload)
    nodes = observation.node_resources

    assert nodes[0].cpu_allocatable == 0
    assert nodes[0].memory_allocatable_gb == 0
    assert response.released
    assert not response.closed_by_owner


def test_node_projection_requires_allocatable_map():
    node = _node('node-a')
    node['status'].pop('allocatable')
    payload = _payload([node])
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(ValueError, match='resource fields are incomplete'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert response.ownership_events == ['close', 'release']


def test_dotted_keys_cannot_spoof_node_structure():
    spoofed_node = {
        'metadata.name': 'node-a',
        'status.capacity.cpu': '4',
        'status.capacity.memory': '16Gi',
        'status.allocatable.cpu': '3900m',
        'status.allocatable.memory': '15Gi',
        'status.conditions': [{
            'type': 'Ready',
            'status': 'True',
        }],
    }
    payload = _payload([spoofed_node])
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(ValueError, match='resource fields are incomplete'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert response.ownership_events == ['close', 'release']


@pytest.mark.parametrize(('container_depth', 'accepted'),
                         ((64, True), (65, False)))
def test_json_container_depth_bound_is_root_inclusive(container_depth,
                                                      accepted):
    nested_value: object = 0
    # The root object is the first container. Its `nested` value supplies the
    # remaining containers needed to hit the requested total depth exactly.
    for _ in range(container_depth - 1):
        nested_value = [nested_value]
    payload = json.dumps({
        'apiVersion': 'v1',
        'kind': 'NodeList',
        'metadata': {},
        'nested': nested_value,
        'items': [_node('node-a')],
    }).encode('utf-8')
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    if accepted:
        observation = utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )
        assert [node.name for node in observation.node_resources] == ['node-a']
        assert response.ownership_events == ['release']
    else:
        with pytest.raises(utils.KubernetesObservationLimitError,
                           match='container depth'):
            utils.get_kubernetes_node_observation_uncached_bounded(
                core_api=core_api,
                maximum_nodes=10,
                maximum_response_bytes=len(payload),
                budget=budget,
            )
        assert response.ownership_events == ['close', 'release']


def test_duplicate_node_names_are_rejected_without_reordering():
    payload = _payload([_node('node-a'), _node('node-a')])
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(ValueError, match='names must be unique'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert budget.consumed_node_records == 2
    assert response.ownership_events == ['close', 'release']


def test_parse_failure_survives_release_failure_and_closes_response(
        monkeypatch):
    node = _node('node-a')
    node['status']['capacity'].pop('cpu')
    payload = _payload([node])
    response = _Response(payload)
    release_conn = mock.MagicMock(side_effect=RuntimeError('release failed'))
    monkeypatch.setattr(response, 'release_conn', release_conn)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(ValueError, match='resource fields are incomplete'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    release_conn.assert_called_once_with()
    assert response.closed_by_owner


def test_release_failure_after_success_closes_response_and_propagates(
        monkeypatch):
    payload = _payload([_node('node-a')])
    response = _Response(payload)
    release_error = RuntimeError('release failed')
    release_conn = mock.MagicMock(side_effect=release_error)
    monkeypatch.setattr(response, 'release_conn', release_conn)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(RuntimeError, match='release failed') as exc_info:
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert exc_info.value is release_error
    release_conn.assert_called_once_with()
    assert response.closed_by_owner


@pytest.mark.parametrize('aggregate_is_tighter', (False, True))
def test_byte_overflow_fails_before_budget_excess_and_never_refunds(
        aggregate_is_tighter):
    payload = _payload([_node('node-a')])
    byte_limit = len(payload) - 1
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    per_response_limit = len(payload) if aggregate_is_tighter else byte_limit
    aggregate_limit = byte_limit if aggregate_is_tighter else len(payload)
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=aggregate_limit, maximum_node_records=10)

    with pytest.raises(utils.KubernetesObservationLimitError):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=per_response_limit,
            budget=budget,
        )

    assert budget.consumed_response_bytes == byte_limit
    assert budget.consumed_response_bytes <= aggregate_limit
    assert response.closed_by_owner
    assert response.released


def test_partial_invalid_response_charges_bytes_and_completed_nodes():
    complete_item = json.dumps(_node('node-a')).encode('utf-8')
    payload = b'{"items":[' + complete_item + b','
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload) + 100, maximum_node_records=10)
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response

    with pytest.raises(Exception):  # ijson backend-specific incomplete error
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload) + 100,
            budget=budget,
        )

    assert budget.consumed_response_bytes == len(payload)
    assert budget.consumed_node_records == 1
    assert response.closed_by_owner
    assert response.released


def test_node_count_bound_rejects_whole_response_without_refund():
    payload = _payload([_node('node-a'), _node('node-b')])
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response

    with pytest.raises(utils.KubernetesObservationLimitError,
                       match='count bound'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=1,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert budget.consumed_node_records == 1
    assert response.closed_by_owner
    assert response.released
    core_api.list_node.assert_called_once_with(
        limit=2,
        _request_timeout=utils.kubernetes.API_TIMEOUT,
        _preload_content=False,
    )


@pytest.mark.parametrize(
    'metadata',
    (
        {
            'continue': 'next-page',
        },
        {
            'remainingItemCount': 1,
        },
    ),
)
def test_pagination_is_rejected(metadata):
    payload = _payload([_node('node-a')], metadata=metadata)
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(utils.KubernetesObservationLimitError,
                       match='paginated'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert response.closed_by_owner
    assert response.released


@pytest.mark.parametrize('metadata', (None, 'metadata', 1, [], [
    {
        'continue': 'next-page'
    },
]))
def test_root_metadata_must_be_one_object(metadata):
    payload = json.dumps({
        'apiVersion': 'v1',
        'kind': 'NodeList',
        'metadata': metadata,
        'items': [_node('node-a')],
    }).encode('utf-8')
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(ValueError, match='metadata must be an object'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert response.ownership_events == ['close', 'release']


def test_root_metadata_is_required():
    payload = json.dumps({
        'apiVersion': 'v1',
        'kind': 'NodeList',
        'items': [_node('node-a')],
    }).encode('utf-8')
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(ValueError, match='collection is incomplete'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert response.ownership_events == ['close', 'release']


def test_root_metadata_must_not_be_repeated():
    node_json = json.dumps(_node('node-a'))
    payload = ('{"metadata":{},"metadata":{},"items":[' + node_json +
               ']}').encode('utf-8')
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(ValueError, match='metadata must not be repeated'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert response.ownership_events == ['close', 'release']


@pytest.mark.parametrize('pagination_field', ('continue', 'remainingItemCount'))
def test_nested_pagination_metadata_is_rejected(pagination_field):
    payload = _payload([_node('node-a')],
                       metadata={'extension': {
                           pagination_field: 'next-page'
                       }})
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(ValueError, match='must be direct fields'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert response.ownership_events == ['close', 'release']


@pytest.mark.parametrize(
    'node',
    (
        _node('a' * 254),
        _node('node-a', cpu='1' * 129),
        _node('node-a', annotations={'a' * 1025: 'value'}),
        _node('node-a', annotations={'key': 'a' * (256 * 1024 + 1)}),
    ),
)
def test_per_field_byte_bounds_are_enforced(node):
    payload = _payload([node])
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(utils.KubernetesObservationLimitError):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert response.closed_by_owner
    assert response.released


def test_condition_count_bound_is_enforced():
    node = _node('node-a')
    node['status']['conditions'] = [{
        'type': f'Condition{index}',
        'status': 'False',
    } for index in range(257)]
    payload = _payload([node])
    response = _Response(payload)
    core_api = mock.MagicMock()
    core_api.list_node.return_value = response
    budget = utils.KubernetesObservationBudget(
        maximum_response_bytes=len(payload), maximum_node_records=10)

    with pytest.raises(utils.KubernetesObservationLimitError,
                       match='conditions'):
        utils.get_kubernetes_node_observation_uncached_bounded(
            core_api=core_api,
            maximum_nodes=10,
            maximum_response_bytes=len(payload),
            budget=budget,
        )

    assert response.closed_by_owner
    assert response.released


def test_aggregate_budget_is_thread_safe_monotonic_and_never_exceeds_cap():
    budget = utils.KubernetesObservationBudget(maximum_response_bytes=1000,
                                               maximum_node_records=100)

    def charge(_):
        try:
            budget.consume_response_bytes(1)
            budget.consume_node_record()
            return True
        except utils.KubernetesObservationLimitError:
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        results = list(executor.map(charge, range(1000)))

    assert sum(results) == 100
    assert budget.consumed_response_bytes == 1000
    assert budget.remaining_response_bytes == 0
    assert budget.consumed_node_records == 100
    assert budget.remaining_node_records == 0

    consumed_before = budget.consumed_node_records
    with pytest.raises(utils.KubernetesObservationLimitError):
        budget.consume_node_record()
    assert budget.consumed_node_records == consumed_before


def test_failed_physical_read_permanently_charges_its_reservation():
    byte_cap = 10
    budget = utils.KubernetesObservationBudget(maximum_response_bytes=byte_cap,
                                               maximum_node_records=1)
    physically_read = 0

    class FailingSource:

        def read(self, size):
            nonlocal physically_read
            physically_read += size
            raise OSError('read failed after consuming bytes')

    reader = utils._KubernetesObservationReader(FailingSource(),
                                                maximum_bytes=byte_cap,
                                                budget=budget)
    with pytest.raises(OSError, match='after consuming'):
        reader.read(byte_cap)

    assert physically_read == byte_cap
    assert budget.consumed_response_bytes == byte_cap
    assert budget.remaining_response_bytes == 0

    next_source = mock.MagicMock()
    next_source.length_remaining = 1
    next_reader = utils._KubernetesObservationReader(next_source,
                                                     maximum_bytes=byte_cap,
                                                     budget=budget)
    with pytest.raises(utils.KubernetesObservationLimitError,
                       match='aggregate'):
        next_reader.read(byte_cap)
    next_source.read.assert_not_called()


def test_concurrent_physical_reads_do_not_hold_the_aggregate_lock():
    byte_cap = 20
    budget = utils.KubernetesObservationBudget(maximum_response_bytes=byte_cap,
                                               maximum_node_records=1)
    reads_started = threading.Barrier(2)

    class ConcurrentSource:

        def read(self, size):
            reads_started.wait(timeout=5)
            return b'x' * size

    readers = [
        utils._KubernetesObservationReader(ConcurrentSource(),
                                           maximum_bytes=10,
                                           budget=budget) for _ in range(2)
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda reader: reader.read(10), readers))

    assert results == [b'x' * 10, b'x' * 10]
    assert budget.consumed_response_bytes == byte_cap
    assert budget.remaining_response_bytes == 0


def test_reader_waits_for_inflight_reservation_before_declaring_exhaustion():
    byte_cap = 10
    budget = utils.KubernetesObservationBudget(maximum_response_bytes=byte_cap,
                                               maximum_node_records=1)
    first_read_started = threading.Event()
    allow_first_read_to_finish = threading.Event()
    second_source_called = threading.Event()

    class FirstSource:

        def read(self, size):
            del size
            first_read_started.set()
            assert allow_first_read_to_finish.wait(timeout=5)
            return b'a'

    class SecondSource:

        def read(self, size):
            del size
            second_source_called.set()
            return b'b'

    first_reader = utils._KubernetesObservationReader(FirstSource(),
                                                      maximum_bytes=byte_cap,
                                                      budget=budget)
    second_reader = utils._KubernetesObservationReader(SecondSource(),
                                                       maximum_bytes=byte_cap,
                                                       budget=budget)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        first_result = executor.submit(first_reader.read, byte_cap)
        assert first_read_started.wait(timeout=5)
        second_result = executor.submit(second_reader.read, byte_cap)
        assert not second_source_called.wait(timeout=0.1)
        allow_first_read_to_finish.set()
        assert first_result.result(timeout=5) == b'a'
        assert second_result.result(timeout=5) == b'b'

    assert budget.consumed_response_bytes == 2
    assert budget.remaining_response_bytes == byte_cap - 2


def test_concurrent_streams_never_physically_read_past_aggregate_byte_cap():
    byte_cap = 100
    budget = utils.KubernetesObservationBudget(maximum_response_bytes=byte_cap,
                                               maximum_node_records=10)
    tracker_lock = threading.Lock()
    physically_read = 0

    class CountingResponse(_Response):

        def read(self, size=-1):
            nonlocal physically_read
            data = super().read(size)
            with tracker_lock:
                physically_read += len(data)
            return data

    def read_context(context_name):
        payload = _payload([_node(context_name)])
        response = CountingResponse(payload)
        core_api = mock.MagicMock()
        core_api.list_node.return_value = response
        try:
            utils.get_kubernetes_node_observation_uncached_bounded(
                core_api=core_api,
                maximum_nodes=10,
                maximum_response_bytes=len(payload),
                budget=budget,
            )
        except utils.KubernetesObservationLimitError:
            return response
        raise AssertionError('oversized concurrent response was accepted')

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(read_context, ('node-a', 'node-b')))

    assert physically_read == byte_cap
    assert budget.consumed_response_bytes == byte_cap
    assert budget.remaining_response_bytes == 0
    assert all(response.closed_by_owner and response.released
               for response in responses)
