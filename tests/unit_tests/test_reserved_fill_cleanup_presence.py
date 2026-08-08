"""Proving physical Pod absence so settled cleanups stop being retained.

A protocol-v2 cleanup whose durable cluster record vanished used to be kept
forever and re-driven every 15 minutes, because the fence could not prove the
provider was clean. These tests pin the replacement behavior: read the physical
cluster, and only retain rows that are genuinely unresolved.
"""
# pylint: disable=protected-access
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
def _clear_snapshot_cache():
    reserved_capacity._physical_presence_snapshots.clear()
    yield
    reserved_capacity._physical_presence_snapshots.clear()


def _patch_pods(monkeypatch, pods, *, raises=None):
    """Stub the fenced all-namespace Pod list, returning the call counter."""
    calls = {'n': 0}

    def _read(fence):
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
            _fence(), f'replica-{index}', now=100.0) is Presence.ABSENT
    assert calls['n'] == 1


def test_snapshot_expires_so_new_pods_are_observed(monkeypatch):
    calls = _patch_pods(monkeypatch,
                        [_pod(annotation='other', label='other-1')])
    reserved_capacity.probe_physical_replica_presence(_fence(),
                                                      _CLUSTER,
                                                      now=100.0)
    stale = 100.0 + reserved_capacity._PHYSICAL_PRESENCE_SNAPSHOT_TTL_SECONDS + 1
    reserved_capacity.probe_physical_replica_presence(_fence(),
                                                      _CLUSTER,
                                                      now=stale)
    assert calls['n'] == 2


class _Recorder:
    """Minimal stand-in exposing only what the cleanup funnel touches."""

    def __init__(self):
        self.finished = []

    def _handle_sky_down_finish(self, info, format_exc):
        self.finished.append((info, format_exc))


def _info():
    return types.SimpleNamespace(replica_id=39149, cluster_name=_CLUSTER)


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
