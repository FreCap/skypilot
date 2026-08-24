"""Proving physical Pod absence so settled cleanups stop being retained.

A protocol-v2 cleanup whose durable cluster record vanished used to be kept
forever and re-driven every 15 minutes, because the fence could not prove the
provider was clean. These tests pin the replacement behavior: read the physical
cluster, and only retain rows that are genuinely unresolved.
"""
# pylint: disable=protected-access
import concurrent.futures
import contextlib
import io
import json
import threading
import types
from unittest import mock

import pytest

from sky import exceptions
from sky.serve import replica_managers
from sky.serve import reserved_capacity

_CONTEXT = 'prod_research_cluster_eks'
_UID = 'physical-uid'
_CLUSTER = 'boltz-l4-fleet-39149-0698237f22'

Presence = reserved_capacity.PhysicalReplicaPresence


def _fence() -> reserved_capacity.ProtocolV2CleanupFence:
    return reserved_capacity.ProtocolV2CleanupFence(kubernetes_context=_CONTEXT,
                                                    physical_cluster_uid=_UID)


def _pod(*, annotation: str | None, label: str | None):
    metadata = types.SimpleNamespace(
        annotations=({
            'skypilot-cluster-name': annotation
        } if annotation is not None else {}),
        labels=({
            'skypilot-cluster-name': label
        } if label is not None else {}),
    )
    return types.SimpleNamespace(metadata=metadata)


@pytest.fixture(autouse=True)
def _clear_snapshot_cache(monkeypatch):
    reserved_capacity._physical_presence_snapshots.clear()
    reserved_capacity._physical_presence_reads.clear()
    monkeypatch.setattr(replica_managers.kueue_lane_observer,
                        'project_exact_pod_absence_after_teardown',
                        lambda *_args: False)
    yield
    reserved_capacity._physical_presence_snapshots.clear()
    reserved_capacity._physical_presence_reads.clear()


def _patch_pods(monkeypatch, pods, *, raises=None):
    """Stub the fenced all-namespace Pod list, returning the call counter."""
    calls = {'n': 0}

    def _read(fence, *, cluster_name_on_cloud=None):
        calls['n'] += 1
        assert fence.kubernetes_context == _CONTEXT
        if raises is not None:
            raise raises
        annotated = set()
        on_cloud = set()
        fully_annotated = True
        for pod in pods:
            annotation = pod.metadata.annotations.get('skypilot-cluster-name')
            label = pod.metadata.labels.get('skypilot-cluster-name')
            if label:
                on_cloud.add(label)
            if annotation:
                annotated.add(annotation)
            else:
                fully_annotated = False
        return frozenset(annotated), frozenset(on_cloud), fully_annotated

    monkeypatch.setattr(reserved_capacity, '_read_physical_replica_names',
                        _read)
    return calls


class _RawResponse:
    """Small urllib3-response stand-in for streaming parser tests."""

    def __init__(self, payload):
        if not isinstance(payload, bytes):
            payload = json.dumps(payload).encode()
        self._body = io.BytesIO(payload)
        self.released = False
        self.closed = False
        self.actions = []
        self.read_sizes = []

    def read(self, size=-1):
        self.read_sizes.append(size)
        return self._body.read(size)

    def release_conn(self):
        self.actions.append('release')
        self.released = True

    def close(self):
        self.actions.append('close')
        self.closed = True


def _metadata_item(*, annotation, label):
    annotations = ({} if annotation is None else {
        'skypilot-cluster-name': annotation
    })
    labels = ({} if label is None else {'skypilot-cluster-name': label})
    return {
        'apiVersion': 'meta.k8s.io/v1',
        'kind': 'PartialObjectMetadata',
        'metadata': {
            'annotations': annotations,
            'labels': labels,
        },
    }


def _metadata_list(items, *, continuation=''):
    return {
        'apiVersion': 'meta.k8s.io/v1',
        'kind': 'PartialObjectMetadataList',
        'metadata': {
            'continue': continuation,
        },
        'items': items,
    }


def _raw_metadata_list_with_items_fields(items_fields):
    return ('{"apiVersion":"meta.k8s.io/v1",'
            '"kind":"PartialObjectMetadataList","metadata":{},' + items_fields +
            '}').encode()


def _patch_raw_api(monkeypatch, responses):
    client = mock.Mock()
    client.call_api.side_effect = responses
    monkeypatch.setattr(reserved_capacity.kubernetes, 'api_client',
                        lambda context: client)
    monkeypatch.setattr(reserved_capacity.kubernetes,
                        'physical_cluster_uid_fence',
                        lambda context, uid, wait_for_initializer=False:
                        contextlib.nullcontext())
    monkeypatch.setattr(reserved_capacity.kubernetes,
                        'raise_if_api_call_deadline_exceeded', lambda: None)
    return client


def test_absent_when_no_pod_claims_the_cluster(monkeypatch):
    """The provisioning that never happened leaves nothing to clean up."""
    _patch_pods(monkeypatch, [
        _pod(annotation='boltz-l4-fleet-1-abc', label='boltz-l4-fleet-1-abc-d')
    ])
    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER) is Presence.ABSENT


def test_present_when_annotation_matches(monkeypatch):
    _patch_pods(monkeypatch,
                [_pod(annotation=_CLUSTER, label=f'{_CLUSTER}-b673d4fd')])
    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER) is Presence.PRESENT


def test_present_when_only_the_on_cloud_label_matches(monkeypatch):
    """A Pod predating the annotation still proves ownership by prefix."""
    _patch_pods(monkeypatch,
                [_pod(annotation=None, label=f'{_CLUSTER}-b673d4fd')])
    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER) is Presence.PRESENT


def test_unproven_when_a_pod_lacks_the_ownership_annotation(monkeypatch):
    """A shortened on-cloud name can hide the prefix, so absence is unsafe."""
    _patch_pods(monkeypatch,
                [_pod(annotation=None, label='truncated-9f3a21c4')])
    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER) is Presence.UNPROVEN


def test_unproven_when_the_provider_read_fails(monkeypatch):
    _patch_pods(monkeypatch, [], raises=RuntimeError('api down'))
    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER) is Presence.UNPROVEN


def test_unproven_when_the_identity_fence_conflicts(monkeypatch):
    """A retargeted kubeconfig must never be read as a clean provider."""
    _patch_pods(monkeypatch, [],
                raises=exceptions.KubernetesPhysicalClusterIdentityError(
                    'conflicting expectation'))
    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER) is Presence.UNPROVEN


def test_one_provider_read_serves_a_whole_drain_sweep(monkeypatch):
    """Hundreds of retiring replicas must not issue hundreds of Pod lists."""
    calls = _patch_pods(monkeypatch,
                        [_pod(annotation='other', label='other-1')])
    for index in range(50):
        assert reserved_capacity.probe_physical_replica_presence(
            _fence(), f'replica-{index}') is Presence.ABSENT
    assert calls['n'] == 1


def test_exact_evidence_bypasses_an_older_cached_snapshot(monkeypatch):
    calls = _patch_pods(monkeypatch,
                        [_pod(annotation='other', label='other-1')])
    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER) is Presence.ABSENT
    observed_after = reserved_capacity.time.monotonic()
    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER, observed_after=observed_after) is Presence.ABSENT
    assert calls['n'] == 2


def test_snapshot_expires_so_new_pods_are_observed(monkeypatch):
    calls = _patch_pods(monkeypatch,
                        [_pod(annotation='other', label='other-1')])
    reserved_capacity.probe_physical_replica_presence(_fence(), _CLUSTER)
    cached = next(iter(reserved_capacity._physical_presence_snapshots.values()))
    stale = (cached.completed_at +
             reserved_capacity._PHYSICAL_PRESENCE_SNAPSHOT_TTL_SECONDS + 1)
    reserved_capacity.probe_physical_replica_presence(_fence(),
                                                      _CLUSTER,
                                                      now=stale)
    assert calls['n'] == 2


def test_exact_lookup_streams_partial_metadata_and_paginates(monkeypatch):
    """Known handles request only their exact label and retain tiny fields."""
    on_cloud_name = 'shortened-9f3a21c4'
    responses = [
        _RawResponse(
            _metadata_list(
                [_metadata_item(annotation='other', label=on_cloud_name)],
                continuation='next-page')),
        _RawResponse(
            _metadata_list(
                [_metadata_item(annotation=None, label=on_cloud_name)])),
    ]
    client = _patch_raw_api(monkeypatch, responses)

    snapshot = reserved_capacity._read_physical_replica_names(
        _fence(), cluster_name_on_cloud=on_cloud_name)

    assert snapshot == (frozenset({'other'}), frozenset({on_cloud_name}), False)
    assert client.call_api.call_count == 2
    first = client.call_api.call_args_list[0]
    assert ('labelSelector', f'skypilot-cluster-name={on_cloud_name}'
           ) in first.kwargs['query_params']
    assert ('limit', reserved_capacity._PHYSICAL_PRESENCE_PAGE_SIZE
           ) in first.kwargs['query_params']
    assert first.kwargs['header_params']['Accept'] == (
        reserved_capacity._PARTIAL_OBJECT_METADATA_LIST_ACCEPT)
    assert first.kwargs['_preload_content'] is False
    assert (
        'continue',
        'next-page') in client.call_api.call_args_list[1].kwargs['query_params']
    assert all(response.actions == ['release'] for response in responses)


def test_empty_partial_metadata_null_items_proves_absence(monkeypatch):
    """Kubernetes encodes an empty nil Items slice as JSON null."""
    response = _RawResponse(_metadata_list(None))
    _patch_raw_api(monkeypatch, [response])

    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER) is Presence.ABSENT
    assert response.actions == ['release']


def test_exact_selector_hit_is_present_even_if_logical_prefix_was_shortened(
        monkeypatch):
    on_cloud_name = 'shortened-9f3a21c4'
    _patch_raw_api(monkeypatch, [
        _RawResponse(
            _metadata_list(
                [_metadata_item(annotation=None, label=on_cloud_name)]))
    ])

    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER,
        cluster_name_on_cloud=on_cloud_name) is Presence.PRESENT


def test_exact_selector_violation_cannot_prove_absence(monkeypatch):
    """A server/proxy ignoring the selector is never negative evidence."""
    _patch_raw_api(monkeypatch, [
        _RawResponse(
            _metadata_list(
                [_metadata_item(annotation='other', label='wrong-label')]))
    ])

    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER,
        cluster_name_on_cloud='expected-label') is Presence.UNPROVEN


@pytest.mark.parametrize('failure', [
    'wrong-kind', 'missing-items', 'duplicate-null', 'null-scalar',
    'duplicate-array', 'scalar-array-element', 'truncated-array',
    'repeated-page', 'byte-limit', 'item-limit'
])
def test_incomplete_or_unbounded_metadata_read_cannot_prove_absence(
        monkeypatch, failure):
    one_item = _metadata_item(annotation='other', label='other-on-cloud')
    if failure == 'wrong-kind':
        payload = _metadata_list([one_item])
        payload['kind'] = 'PodList'
        responses = [_RawResponse(payload)]
    elif failure == 'missing-items':
        payload = _metadata_list([])
        payload.pop('items')
        responses = [_RawResponse(payload)]
    elif failure == 'duplicate-null':
        responses = [
            _RawResponse(
                _raw_metadata_list_with_items_fields(
                    '"items":null,"items":null'))
        ]
    elif failure == 'null-scalar':
        responses = [
            _RawResponse(
                _raw_metadata_list_with_items_fields('"items":null,"items":42'))
        ]
    elif failure == 'duplicate-array':
        responses = [
            _RawResponse(
                _raw_metadata_list_with_items_fields('"items":[],"items":[]'))
        ]
    elif failure == 'scalar-array-element':
        responses = [
            _RawResponse(_raw_metadata_list_with_items_fields('"items":[null]'))
        ]
    elif failure == 'truncated-array':
        responses = [
            _RawResponse(
                _raw_metadata_list_with_items_fields('"items":[')[:-1])
        ]
    elif failure == 'repeated-page':
        responses = [
            _RawResponse(_metadata_list([one_item], continuation='same')),
            _RawResponse(_metadata_list([one_item], continuation='same')),
        ]
    elif failure == 'byte-limit':
        monkeypatch.setattr(reserved_capacity,
                            '_PHYSICAL_PRESENCE_MAX_RESPONSE_BYTES', 10)
        responses = [_RawResponse(_metadata_list([one_item]))]
    else:
        monkeypatch.setattr(reserved_capacity, '_PHYSICAL_PRESENCE_MAX_ITEMS',
                            1)
        responses = [
            _RawResponse(_metadata_list([one_item, one_item])),
        ]
    _patch_raw_api(monkeypatch, responses)

    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER) is Presence.UNPROVEN
    assert responses[-1].actions == ['close', 'release']
    if failure == 'byte-limit':
        assert max(responses[-1].read_sizes) <= 11


def test_observation_completion_is_sampled_after_provider_read(monkeypatch):
    _patch_pods(monkeypatch, [])
    monotonic = mock.Mock(side_effect=[10.0, 20.0])
    monkeypatch.setattr(reserved_capacity.time, 'monotonic', monotonic)

    assert reserved_capacity.probe_physical_replica_presence(
        _fence(), _CLUSTER, now=5.0) is Presence.ABSENT

    observation = next(
        iter(reserved_capacity._physical_presence_snapshots.values()))
    assert observation.started_at == 10.0
    assert observation.completed_at == 20.0


class _WaitObservedEvent:
    """Event wrapper that makes entering the singleflight wait observable."""

    def __init__(self, event, waiter_entered):
        self._event = event
        self._waiter_entered = waiter_entered

    def wait(self):
        self._waiter_entered.set()
        return self._event.wait()

    def set(self):
        return self._event.set()


def _observe_singleflight_wait(waiter_entered):
    with reserved_capacity._physical_presence_lock:
        read = next(iter(reserved_capacity._physical_presence_reads.values()))
        read.ready = _WaitObservedEvent(read.ready, waiter_entered)


def test_concurrent_current_callers_share_one_provider_read(monkeypatch):
    read_started = threading.Event()
    release_read = threading.Event()
    waiter_entered = threading.Event()
    calls = 0

    def _read(fence):
        del fence
        nonlocal calls
        calls += 1
        read_started.set()
        assert release_read.wait(timeout=5)
        return frozenset(), frozenset(), True

    monkeypatch.setattr(reserved_capacity, '_read_physical_replica_names',
                        _read)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        owner = executor.submit(
            reserved_capacity.probe_physical_replica_presence, _fence(),
            _CLUSTER)
        assert read_started.wait(timeout=5)
        _observe_singleflight_wait(waiter_entered)
        waiter = executor.submit(
            reserved_capacity.probe_physical_replica_presence, _fence(),
            'another-cluster')
        try:
            assert waiter_entered.wait(timeout=5)
        finally:
            release_read.set()
        assert owner.result(timeout=5) is Presence.ABSENT
        assert waiter.result(timeout=5) is Presence.ABSENT
    assert calls == 1


def test_post_teardown_caller_never_joins_a_pre_teardown_read(monkeypatch):
    first_started = threading.Event()
    release_first = threading.Event()
    waiter_entered = threading.Event()
    calls = 0

    def _read(fence):
        del fence
        nonlocal calls
        calls += 1
        if calls == 1:
            first_started.set()
            assert release_first.wait(timeout=5)
            return frozenset(), frozenset(), True
        return frozenset({_CLUSTER}), frozenset(), True

    monkeypatch.setattr(reserved_capacity, '_read_physical_replica_names',
                        _read)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        older = executor.submit(
            reserved_capacity.probe_physical_replica_presence, _fence(),
            'other-cluster')
        assert first_started.wait(timeout=5)
        observed_after = reserved_capacity.time.monotonic()
        _observe_singleflight_wait(waiter_entered)
        causal = executor.submit(
            reserved_capacity.probe_physical_replica_presence,
            _fence(),
            _CLUSTER,
            observed_after=observed_after)
        try:
            assert waiter_entered.wait(timeout=5)
        finally:
            release_first.set()
        assert older.result(timeout=5) is Presence.ABSENT
        assert causal.result(timeout=5) is Presence.PRESENT
    assert calls == 2


class _Recorder:
    """Minimal stand-in exposing only what the cleanup funnel touches."""

    def __init__(self):
        self._service_name = 'svc'
        self.finished = []

    def _handle_sky_down_finish(self, info, format_exc):
        self.finished.append((info, format_exc))


def _info():
    return types.SimpleNamespace(
        replica_id=39149,
        replica_record_id=('00000000-0000-0000-0000-000000000001'),
        cluster_name=_CLUSTER)


def _prove(recorder, info):
    return replica_managers.SkyPilotReplicaManager._prove_cleanup_complete(
        recorder, info, 'the durable cluster record is absent')


def test_proven_absent_cleanup_finishes_instead_of_being_retained(monkeypatch):
    """The 368-row pileup drains: absence settles the row as cleaned up."""
    monkeypatch.setattr(reserved_capacity, 'parse_protocol_v2_cleanup_fence',
                        lambda info: _fence())
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        lambda fence, name: Presence.ABSENT)
    recorder = _Recorder()
    assert _prove(recorder, _info()) is True
    assert recorder.finished == [(mock.ANY, None)]


@pytest.mark.parametrize('presence', [Presence.PRESENT, Presence.UNPROVEN])
def test_unsettled_cleanup_is_still_retained(monkeypatch, presence):
    """A live or unreadable Pod keeps the durable row and its retry."""
    monkeypatch.setattr(reserved_capacity, 'parse_protocol_v2_cleanup_fence',
                        lambda info: _fence())
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        lambda fence, name: presence)
    recorder = _Recorder()
    assert _prove(recorder, _info()) is False
    assert not recorder.finished


def test_non_fenced_replica_keeps_legacy_handling(monkeypatch):
    """Ordinary (non reserved-fill) rows never consult the physical prover."""
    monkeypatch.setattr(reserved_capacity, 'parse_protocol_v2_cleanup_fence',
                        lambda info: None)
    recorder = _Recorder()
    assert _prove(recorder, _info()) is False
    assert not recorder.finished


def test_malformed_identity_is_never_settled(monkeypatch):
    """The row the fence exists to protect must stay retained."""

    def _raise(info):
        raise exceptions.KubernetesPhysicalClusterIdentityError('malformed')

    monkeypatch.setattr(reserved_capacity, 'parse_protocol_v2_cleanup_fence',
                        _raise)
    recorder = _Recorder()
    assert _prove(recorder, _info()) is False
    assert not recorder.finished
