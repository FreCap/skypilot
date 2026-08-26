"""Provider-authority phase coverage for service status serialization."""
# pylint: disable=protected-access,missing-class-docstring
import contextlib
import threading
import types
from unittest import mock

import pytest

from sky import exceptions
from sky.serve import provider_phase
from sky.serve import serve_state
from sky.serve import serve_utils

_V2 = provider_phase.ProviderPhaseMode.V2_FENCED
_AMBIENT = provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
_PHYSICAL_KEY = ('phx-context', 'physical-uid')


class _PhaseHarness:
    """Deterministic root/child phase model for status fanout tests."""

    def __init__(self, timed_out_mode=None):
        self.timed_out_mode = timed_out_mode
        self.attempts = []
        self.exits = []
        self.join_modes = []
        self.active_mode = None
        self.active_children = 0
        self._lock = threading.Lock()
        self._local = threading.local()

    @contextlib.contextmanager
    def phase(self, mode):
        self.attempts.append(mode)
        if mode is self.timed_out_mode:
            raise exceptions.ProviderPhaseTimeoutError('deterministic busy')
        with self._lock:
            assert self.active_mode is None
            self.active_mode = mode
        admission = types.SimpleNamespace(mode=mode)
        try:
            yield admission
        finally:
            with self._lock:
                # The root may not leave until every fanout child has joined.
                assert self.active_children == 0
                assert self.active_mode is mode
                self.active_mode = None
            self.exits.append(mode)

    @contextlib.contextmanager
    def join(self, admission):
        with self._lock:
            assert self.active_mode is admission.mode
            self.active_children += 1
        self.join_modes.append(admission.mode)
        prior_mode = getattr(self._local, 'mode', None)
        self._local.mode = admission.mode
        try:
            yield admission
        finally:
            if prior_mode is None:
                del self._local.mode
            else:
                self._local.mode = prior_mode
            with self._lock:
                self.active_children -= 1

    def current_mode(self):
        return getattr(self._local, 'mode', self.active_mode)


def _replica(replica_id, name, expected_mode, harness, serialized):
    info = types.SimpleNamespace(replica_id=replica_id,
                                 cluster_name=name,
                                 planned_capacity=1)

    def _to_info_dict(*, with_handle, with_url, cluster_record, rate_cache):
        del cluster_record, rate_cache
        if with_url:
            assert harness.current_mode() is expected_mode
            serialized.append((expected_mode, name))
        return {
            'replica_id': replica_id,
            'name': name,
            'status': serve_state.ReplicaStatus.READY,
            'endpoint': f'http://{name}:8080' if with_url else 'must-strip',
            'handle': f'handle-{name}' if with_handle else None,
            'launched_at': 1,
            'provider_identity_uncertain': False,
            'cloud': 'Kubernetes' if expected_mode is _V2 else 'AWS',
            'region': 'phx-context' if expected_mode is _V2 else 'us-east-1',
            'infra': f'infra-{name}',
            'hourly_cost': 1.0,
            'resources_str': 'H200:1',
        }

    info.to_info_dict = mock.Mock(side_effect=_to_info_dict)
    return info


def _prepared(name, v2_infos, ordinary_infos):
    infos = [*v2_infos, *ordinary_infos]
    prepared = serve_utils._PreparedServiceStatus(
        record={
            'name': name,
            'pool': False
        },
        pool=False,
        include_replica_info=True,
        replica_infos=infos,
        cluster_records={
            info.cluster_name: {
                'name': info.cluster_name,
                'handle': f'handle-{info.cluster_name}',
                'launched_at': 1,
            } for info in infos
        },
        ordinary_infos=list(ordinary_infos),
    )
    if v2_infos:
        prepared.fenced_groups[_PHYSICAL_KEY] = list(v2_infos)
        prepared.validated_handles.update({
            info.replica_id: f'handle-{info.cluster_name}' for info in v2_infos
        })
    return prepared


def _decode(payload):
    return serve_utils.unpickle_service_status(payload)


class TestStatusProviderPhaseFanout:

    def test_bad_replica_serializer_does_not_black_out_healthy_peer(self):
        harness = _PhaseHarness()
        serialized = []
        bad = _replica(1, 'bad', _AMBIENT, harness, serialized)
        bad.replica_record_id = 'record-bad'
        bad.version = 1
        bad.is_spot = False
        bad.to_info_dict.side_effect = RuntimeError('corrupt presentation row')
        healthy = _replica(2, 'healthy', _AMBIENT, harness, serialized)
        prepared = _prepared('svc', [], [bad, healthy])

        with harness.phase(_AMBIENT) as admission, mock.patch.object(
                provider_phase, 'join_provider_phase',
                side_effect=harness.join):
            results = serve_utils._serialize_ordinary_status_partition(
                [(prepared, bad), (prepared, healthy)], admission=admission)
        records = {record['name']: record for _, _, record in results}

        assert records['bad']['status'] is serve_state.ReplicaStatus.UNKNOWN
        assert records['bad']['provider_identity_uncertain'] is True
        assert records['bad']['endpoint'] is None
        assert records['healthy']['status'] is serve_state.ReplicaStatus.READY
        assert records['healthy']['endpoint'] == 'http://healthy:8080'

    def test_bad_service_preparation_does_not_black_out_healthy_service(self):
        prepared = _prepared('healthy', [], [])

        def _prepare(name, **_kwargs):
            if name == 'bad':
                raise RuntimeError('corrupt service snapshot')
            return prepared

        with mock.patch.object(serve_utils,
                               '_prepare_service_status',
                               side_effect=_prepare):
            statuses = _decode(
                serve_utils.get_service_status_pickled(['bad', 'healthy'],
                                                       pool=False))

        assert [status['name'] for status in statuses] == ['healthy']
        assert statuses[0]['replica_info'] == []

    def test_global_v2_then_ambient_fanout_joins_and_proves_pool_once(self):
        harness = _PhaseHarness()
        serialized = []
        v2_z = _replica(1, 'v2-z', _V2, harness, serialized)
        ordinary_z = _replica(2, 'ordinary-z', _AMBIENT, harness, serialized)
        v2_a = _replica(1, 'v2-a', _V2, harness, serialized)
        ordinary_a = _replica(2, 'ordinary-a', _AMBIENT, harness, serialized)
        prepared = {
            'svc-z': _prepared('svc-z', [v2_z], [ordinary_z]),
            'svc-a': _prepared('svc-a', [v2_a], [ordinary_a]),
        }
        proofs = []

        @contextlib.contextmanager
        def _physical_fence(info, handle):
            assert harness.current_mode() is _V2
            proofs.append((info.cluster_name, handle))
            yield

        def _prepare(name, **kwargs):
            assert kwargs['pool'] is False
            return prepared[name]

        with mock.patch.object(serve_utils,
                               '_prepare_service_status',
                               side_effect=_prepare) as prepare, \
             mock.patch.object(serve_utils.provider_phase,
                               'provider_phase',
                               side_effect=harness.phase), \
             mock.patch.object(serve_utils.provider_phase,
                               'join_provider_phase',
                               side_effect=harness.join), \
             mock.patch('sky.serve.reserved_capacity.'
                        'protocol_v2_provider_fence',
                        side_effect=_physical_fence):
            statuses = _decode(
                serve_utils.get_service_status_pickled(['svc-z', 'svc-a'],
                                                       pool=False))

        assert [status['name'] for status in statuses] == ['svc-a', 'svc-z']
        assert harness.attempts == [_V2, _AMBIENT]
        assert harness.exits == [_V2, _AMBIENT]
        assert harness.join_modes.count(_V2) == 1
        assert harness.join_modes.count(_AMBIENT) == 2
        assert [mode for mode, _ in serialized[:2]] == [_V2, _V2]
        assert all(mode is _AMBIENT for mode, _ in serialized[2:])
        # Both services share one physical pool and therefore one outer proof.
        assert proofs == [('v2-z', 'handle-v2-z')]
        assert prepare.call_count == 2

    def test_conflicting_uids_for_one_context_fail_both_closed(self):
        harness = _PhaseHarness()
        serialized = []
        first = _replica(1, 'first', _V2, harness, serialized)
        second = _replica(2, 'second', _V2, harness, serialized)
        prepared = _prepared('svc', [first], [])
        second_key = (_PHYSICAL_KEY[0], 'replacement-uid')
        prepared.replica_infos.append(second)
        prepared.cluster_records[second.cluster_name] = {
            'name': second.cluster_name,
            'handle': 'handle-second',
            'launched_at': 1,
        }
        prepared.fenced_groups[second_key] = [second]
        prepared.validated_handles[second.replica_id] = 'handle-second'

        with mock.patch.object(serve_utils,
                               '_prepare_service_status',
                               return_value=prepared), \
             mock.patch.object(serve_utils.provider_phase,
                               'provider_phase') as phase, \
             mock.patch('sky.serve.reserved_capacity.'
                        'protocol_v2_provider_fence') as physical_fence:
            status = _decode(
                serve_utils.get_service_status_pickled(['svc'], pool=False))[0]

        assert [item['status'] for item in status['replica_info']] == [
            serve_state.ReplicaStatus.UNKNOWN,
            serve_state.ReplicaStatus.UNKNOWN,
        ]
        assert all(item['provider_identity_uncertain']
                   for item in status['replica_info'])
        assert all(item['endpoint'] is None for item in status['replica_info'])
        # Durable placement metadata remains; neither contradictory target is
        # entered or selected as the scheduling-dependent winner.
        assert all(
            item['cloud'] == 'Kubernetes' for item in status['replica_info'])
        phase.assert_not_called()
        physical_fence.assert_not_called()

    @pytest.mark.parametrize('timed_out_mode', [_V2, _AMBIENT])
    def test_phase_timeout_keeps_partition_as_strict_unknown(
            self, timed_out_mode):
        harness = _PhaseHarness(timed_out_mode)
        serialized = []
        v2 = _replica(1, 'v2', _V2, harness, serialized)
        ordinary = _replica(2, 'ordinary', _AMBIENT, harness, serialized)
        prepared = _prepared('svc', [v2], [ordinary])
        proofs = []

        @contextlib.contextmanager
        def _physical_fence(info, handle):
            del handle
            assert harness.current_mode() is _V2
            proofs.append(info.replica_id)
            yield

        with mock.patch.object(serve_utils,
                               '_prepare_service_status',
                               return_value=prepared), \
             mock.patch.object(serve_utils.provider_phase,
                               'provider_phase',
                               side_effect=harness.phase), \
             mock.patch.object(serve_utils.provider_phase,
                               'join_provider_phase',
                               side_effect=harness.join), \
             mock.patch('sky.serve.reserved_capacity.'
                        'protocol_v2_provider_fence',
                        side_effect=_physical_fence):
            status = _decode(
                serve_utils.get_service_status_pickled(['svc'], pool=False))[0]

        assert harness.attempts == [_V2, _AMBIENT]
        records = {item['name']: item for item in status['replica_info']}
        unknown_name = 'v2' if timed_out_mode is _V2 else 'ordinary'
        known_name = 'ordinary' if timed_out_mode is _V2 else 'v2'
        unknown = records[unknown_name]
        assert unknown['status'] is serve_state.ReplicaStatus.UNKNOWN
        assert unknown['provider_identity_uncertain'] is True
        assert unknown['endpoint'] is None
        assert unknown['handle'] is None
        assert unknown['launched_at'] is None
        for field in serve_utils._PROVIDER_STATUS_FIELDS:
            assert field not in unknown
        assert records[known_name]['status'] is serve_state.ReplicaStatus.READY
        assert records[known_name]['endpoint'] is not None
        assert proofs == ([] if timed_out_mode is _V2 else [1])

    def test_standalone_status_runs_synchronous_v2_before_ordinary(self):
        harness = _PhaseHarness()
        serialized = []
        v2 = _replica(1, 'v2', _V2, harness, serialized)
        ordinary = _replica(2, 'ordinary', _AMBIENT, harness, serialized)
        prepared = _prepared('svc', [v2], [ordinary])

        @contextlib.contextmanager
        def _physical_fence(info, handle):
            del info, handle
            assert harness.current_mode() is _V2
            yield

        with mock.patch.object(serve_utils,
                               '_prepare_service_status',
                               return_value=prepared), \
             mock.patch.object(serve_utils.provider_phase,
                               'provider_phase',
                               side_effect=harness.phase), \
             mock.patch.object(serve_utils.provider_phase,
                               'join_provider_phase') as join, \
             mock.patch('sky.serve.reserved_capacity.'
                        'protocol_v2_provider_fence',
                        side_effect=_physical_fence):
            status = serve_utils._get_service_status('svc', pool=False)

        assert status is not None
        assert harness.attempts == [_V2, _AMBIENT]
        assert serialized == [(_V2, 'v2'), (_AMBIENT, 'ordinary')]
        join.assert_not_called()
