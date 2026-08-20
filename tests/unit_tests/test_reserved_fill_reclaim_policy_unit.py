"""Non-PostgreSQL tests for terminal reserved-fill reclaim enforcement."""
# pylint: disable=protected-access,unexpected-keyword-arg

import contextlib
import time
import types
from unittest import mock

import pytest

from sky import exceptions
from sky.backends import cloud_vm_ray_backend as backend
from sky.provision import common as provision_common
from sky.serve import constants
from sky.serve import kubernetes_identity
from sky.serve import pool_capacity_observation
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker as broker
from sky.serve import reserved_fill_reclaim_attestation as reclaim
from sky.serve import reserved_fill_reclaim_proofs
from sky.serve import serve_state

_FLEET_DIGEST = 'a' * 64
_INVENTORY_DIGEST = 'b' * 64
_OTHER_FLEET_DIGEST = 'c' * 64
_OTHER_INVENTORY_DIGEST = 'd' * 64
_GATE_GENERATION = 11


def _worker_projection() -> dict[str, object]:
    return {
        'projection_version': 2,
        'candidate_id': 'kubernetes-0000',
        'kubernetes_context': 'phx-context',
        'namespace': 'inference',
        'service_account_name': 'inference-sa',
        'scheduler_name': 'default-scheduler',
        'priority_class_name': 'inference-low',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'kueue_admission': {
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        },
        'pod_identity_role_arn':
            ('arn:aws:iam::123456789012:role/inference-worker'),
        'accelerator_name': 'H200',
        'accelerator_count': 1,
        'accelerator_scheduling': {
            'label_key': 'nvidia.com/gpu.product',
            'label_values': ['NVIDIA-H200'],
            'resource_key': 'nvidia.com/gpu',
        },
        'cache': {
            'kind': 'none',
        },
    }


_WORKER_PROJECTION = _worker_projection()
_WORKER_PROJECTION_DIGEST = kubernetes_identity.worker_projection_sha256(
    _WORKER_PROJECTION)


def _projected_admission() -> reclaim.ReclaimProjectedAdmission:
    return reclaim.projected_admission_from_worker_projection(
        _WORKER_PROJECTION, worker_projection_sha256=_WORKER_PROJECTION_DIGEST)


def test_projected_admission_exposes_frozen_scheduler() -> None:
    assert _projected_admission().scheduler_name == 'default-scheduler'


def test_projected_admission_exposes_frozen_pod_identity_contract() -> None:
    assert (_projected_admission().pod_identity_role_arn ==
            'arn:aws:iam::123456789012:role/inference-worker')

    identity_free_projection = {
        **_WORKER_PROJECTION,
        'pod_identity_role_arn': None,
    }
    identity_free_digest = kubernetes_identity.worker_projection_sha256(
        identity_free_projection)
    admission = reclaim.projected_admission_from_worker_projection(
        identity_free_projection, worker_projection_sha256=identity_free_digest)

    assert admission.pod_identity_role_arn is None


def test_projected_admission_exposes_frozen_accelerator_scheduling() -> None:
    assert _projected_admission().accelerator_scheduling == (
        reclaim.ReclaimAcceleratorScheduling(label_key='nvidia.com/gpu.product',
                                             label_values=('NVIDIA-H200',),
                                             resource_key='nvidia.com/gpu'))


@pytest.fixture(autouse=True)
def _valid_reclaim_gate_guard(monkeypatch):
    monkeypatch.setattr(serve_state,
                        'reserved_fill_reclaim_gate_authority_guard_is_valid',
                        lambda _: True)
    monkeypatch.setattr(serve_state, 'service_replica_launch_fence_snapshot',
                        _launch_snapshot)
    monkeypatch.setattr(reserved_fill_reclaim_proofs,
                        'provider_proof_reference_holds_in_connection',
                        lambda *_args, **_kwargs: True)


def _identity(
        *,
        fleet_digest: str = _FLEET_DIGEST,
        inventory_digest: str = _INVENTORY_DIGEST
) -> reclaim.ReclaimPolicyIdentity:
    return reclaim.ReclaimPolicyIdentity(
        fleet_bundle_sha256=fleet_digest,
        policy_revision='policy-v1',
        provider_inventory_sha256=inventory_digest)


def _launch_context(
        *,
        policy_bound: bool = True,
        fleet_digest: str = _FLEET_DIGEST,
        inventory_digest: str = _INVENTORY_DIGEST) -> dict[str, object]:
    pool_key = broker.make_pool_key('phx-context',
                                    'H200',
                                    protocol_version=broker.PROTOCOL_V2,
                                    physical_cluster_uid='physical-uid')
    context: dict[str, object] = {
        constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
        constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'incarnation-a',
        constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 3,
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.1',
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [_WORKER_PROJECTION],
    }
    policy_kwargs = {}
    if policy_bound:
        policy_kwargs = {
            'reconciliation_gate_generation': _GATE_GENERATION,
            'reclaim_fleet_bundle_sha256': fleet_digest,
            'reclaim_policy_revision': 'policy-v1',
            'reclaim_provider_inventory_sha256': inventory_digest,
            'worker_projection_sha256': _WORKER_PROJECTION_DIGEST,
        }
    context.update(
        reserved_capacity.make_protocol_v2_launch_fence(
            pool_key=pool_key,
            service_generation=7,
            service_version=3,
            physical_cluster_uid='physical-uid',
            kubernetes_context='phx-context',
            accelerator='H200',
            accelerator_count=1,
            **policy_kwargs))
    return context


def _legacy_launch_context() -> dict[str, object]:
    return {
        constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
        constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'incarnation-a',
        constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 3,
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.1',
        constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
        constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 7,
        constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY: '11111111-1111-4111-8111-111111111111',
    }


def _provisioner(context: dict[str, object]):
    provisioner = object.__new__(backend.RetryingVmProvisioner)
    provisioner._workload_type = 'service'
    provisioner._extra_launch_context = context
    return provisioner


def _durable_replica(context: dict[str, object]):
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    if fence is None:
        return types.SimpleNamespace(reserved_fill=True)
    return types.SimpleNamespace(
        _version=18,
        _VERSION=18,
        reserved_fill=True,
        version=fence.service_version,
        reserved_fill_pool_key=fence.pool_key,
        reserved_fill_service_generation=fence.service_generation,
        reserved_fill_physical_cluster_uid=fence.physical_cluster_uid,
        reserved_fill_kubernetes_context=fence.kubernetes_context,
        reserved_fill_reconciliation_gate_generation=(
            fence.reconciliation_gate_generation),
        reserved_fill_reclaim_fleet_bundle_sha256=(
            fence.reclaim_fleet_bundle_sha256),
        reserved_fill_reclaim_policy_revision=fence.reclaim_policy_revision,
        reserved_fill_reclaim_provider_inventory_sha256=(
            fence.reclaim_provider_inventory_sha256),
        reserved_fill_worker_projection_sha256=(fence.worker_projection_sha256),
        reserved_fill_allocation_generation=3,
        reserved_fill_allocation_input_sha256='c' * 64,
        reserved_fill_allocation_claim_generation=fence.service_generation,
        reserved_fill_observation_generation=5,
        reserved_fill_observation_sequence=0,
        reserved_fill_intent_idempotency_key='d' * 64,
        zero_cost_admission_sequence=9,
        is_zero_cost=True,
        resources_override={
            'cloud': 'Kubernetes',
            'region': fence.kubernetes_context,
            'accelerators': {
                fence.accelerator: fence.accelerator_count
            },
        },
        location={
            'cloud': 'Kubernetes',
            'region': fence.kubernetes_context,
            'accelerators': {
                fence.accelerator: fence.accelerator_count
            },
        })


def _launch_snapshot(
    context: dict[str,
                  object]) -> serve_state.ServiceReplicaLaunchFenceSnapshot:
    return serve_state.ServiceReplicaLaunchFenceSnapshot(
        _durable_replica(context))


def _scope(context: dict[str, object]) -> reclaim.ReclaimLaunchScope:
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None
    return reclaim.ReclaimLaunchScope(
        service_name='svc',
        service_version=fence.service_version,
        pool_key=fence.pool_key,
        service_generation=fence.service_generation,
        physical_cluster_uid=fence.physical_cluster_uid,
        kubernetes_context=fence.kubernetes_context,
        accelerator=fence.accelerator,
        accelerator_count=fence.accelerator_count,
        projected_admission=_projected_admission())


def _launch_authorization(
    scope: reclaim.ReclaimLaunchScope,
    *,
    identity: reclaim.ReclaimPolicyIdentity | None = None,
    gate_generation: int = _GATE_GENERATION,
    completed_monotonic: float | None = None,
) -> reclaim.ReclaimLaunchAuthorization:
    effective_identity = identity or _identity()
    effective_completed = (time.monotonic() if completed_monotonic is None else
                           completed_monotonic)
    reference = reclaim.ReclaimProviderProofReference(
        receipt_nonce='a' * 64,
        proof_sha256='8' * 64,
        identity=effective_identity,
        gate_generation=gate_generation,
        kubernetes_context=scope.kubernetes_context,
        completed_monotonic=effective_completed)
    return reclaim.ReclaimLaunchAuthorization(
        identity=effective_identity,
        gate_generation=gate_generation,
        scope=scope,
        provider_proof_reference=reference,
        completed_monotonic=effective_completed)


def _activation_receipt(
    identity: reclaim.ReclaimPolicyIdentity | None = None,
) -> reclaim.ReclaimActivationReceipt:
    return reclaim.ReclaimActivationReceipt(
        identity=identity or _identity(),
        claim_scope_count=0,
        claim_scope_sha256='e' * 64,
        evidence_sha256='f' * 64,
        writer_image_digest=f'sha256:{"1" * 64}',
        writer_deployment_generation='deployment-generation-1',
        writer_deployment_uid='deployment-uid-1',
        writer_pod_inventory_count=1,
        writer_pod_inventory_sha256='2' * 64)


def _gate(*,
          sequenced: bool,
          identity: reclaim.ReclaimPolicyIdentity | None = None):
    if sequenced:
        gate_identity = identity or _identity()
        return pool_capacity_observation.ReconciliationGate(
            state=(pool_capacity_observation.ReconciliationGateState.
                   SEQUENCED_ACTIVE),
            generation=_GATE_GENERATION,
            reclaim_policy_identity=gate_identity,
            reclaim_activation_receipt=_activation_receipt(gate_identity),
            reclaim_authorized_at=1000.0)
    return pool_capacity_observation.ReconciliationGate(
        state=pool_capacity_observation.ReconciliationGateState.LEGACY_ACTIVE,
        generation=0)


def _install_gate(monkeypatch, gate, *, claim_generation: int = 7) -> None:
    repository = types.SimpleNamespace(read_reconciliation_gate=mock.Mock(
        return_value=gate))
    monkeypatch.setattr(pool_capacity_observation,
                        'PoolCapacityObservationRepository',
                        lambda *_args, **_kwargs: repository)
    monkeypatch.setattr(
        serve_state, 'get_service_status_snapshot', lambda _service_name: {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'version': 3,
        })
    monkeypatch.setattr(
        serve_state, 'get_placement_projection_record',
        lambda _service_name, _version:
        (True, None, None, [_WORKER_PROJECTION]))
    identity = gate.reclaim_policy_identity
    sequenced_active = gate.sequenced_active
    state = getattr(
        gate, 'state',
        (pool_capacity_observation.ReconciliationGateState.SEQUENCED_ACTIVE
         if sequenced_active else
         pool_capacity_observation.ReconciliationGateState.LEGACY_ACTIVE))
    authority = {
        'reconciliation_gate_state': state.value,
        'protocol_version': broker.PROTOCOL_V2,
        'reconciliation_gate_generation': gate.generation,
        'reclaim_fleet_bundle_sha256':
            (None if identity is None else identity.fleet_bundle_sha256),
        'reclaim_policy_revision':
            (None if identity is None else identity.policy_revision),
        'reclaim_provider_inventory_sha256':
            (None if identity is None else identity.provider_inventory_sha256),
    }
    pool_key = broker.make_pool_key('phx-context',
                                    'H200',
                                    protocol_version=broker.PROTOCOL_V2,
                                    physical_cluster_uid='physical-uid')
    sequenced_rows = [
        {
            'claim_generation': claim_generation,
        },
        {
            'hash': 'incarnation-a',
            'resource_scope': 'incarnation-a',
            'current_version': 3,
        },
        {
            'worker_placement_projections': [_WORKER_PROJECTION],
        },
        {
            'claim_set_state':
                serve_state.RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2,
            'service_version': 3,
            'generation': claim_generation,
            'edge_count': 1,
        },
        [{
            'pool_key': pool_key,
            'access_context': 'phx-context',
            'physical_cluster_uid': 'physical-uid',
            'gpus_per_replica': 1,
            'service_generation': claim_generation,
            'accelerator_names': ['h200'],
            'worker_projection_sha256_by_accelerator': {
                'h200': _WORKER_PROJECTION_DIGEST,
            },
        }],
    ]

    class _Result:
        """Minimal SQLAlchemy result stub for the authority snapshot."""

        def __init__(self, value):
            self._value = value

        def mappings(self):
            return self

        def one_or_none(self):
            assert not isinstance(self._value, list)
            return self._value

        def all(self):
            assert isinstance(self._value, list)
            return self._value

    class _Connection:
        """Ordered row source for the authority snapshot."""

        def __init__(self):
            self._rows = [authority]
            if sequenced_active:
                self._rows.extend(sequenced_rows)

        def execute(self, _statement):
            assert self._rows
            return _Result(self._rows.pop(0))

    @contextlib.contextmanager
    def _begin():
        yield _Connection()

    engine = types.SimpleNamespace(
        dialect=types.SimpleNamespace(name='postgresql'), begin=_begin)
    monkeypatch.setattr(serve_state._db_manager, 'get_engine', lambda: engine)


@pytest.mark.parametrize('committed', [True, False])
def test_new_claim_generation_retains_only_exact_committed_intent(
        monkeypatch, committed):
    context = _launch_context()
    snapshot = _launch_snapshot(context)
    intent_match = mock.Mock(return_value=committed)
    monkeypatch.setattr(serve_state.zero_cost_actuation,
                        'committed_intent_matches_replica_in_connection',
                        intent_match)
    _install_gate(monkeypatch, _gate(sequenced=True), claim_generation=8)

    holds = serve_state.reserved_fill_reclaim_launch_authority_holds(
        _scope(context), _launch_authorization(_scope(context)), context,
        snapshot)

    assert holds is committed
    intent_match.assert_called_once()
    assert intent_match.call_args.kwargs == {
        'service_name': 'svc',
        'service_hash': 'incarnation-a',
        'replica_info': snapshot.durable_replica_info,
    }


class _Policy(reclaim.ReservedFillReclaimPolicy):
    """Typed deployment policy with explicit callbacks for one test edge."""

    def __init__(self,
                 authorize_launch=None,
                 *,
                 identity: reclaim.ReclaimPolicyIdentity | None = None,
                 authorize_claim_set=None):
        self._identity = identity or _identity()
        self._authorize_launch = authorize_launch
        self._authorize_claim_set = authorize_claim_set

    def policy_identity(self) -> reclaim.ReclaimPolicyIdentity:
        return self._identity

    def enforcement_contract(self) -> reclaim.ReclaimEnforcementContract:
        return (reclaim.ReclaimEnforcementContract.
                GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2)

    def attest_activation(self, claimed_contexts, *, writer_image_digest,
                          deadline_monotonic):
        del claimed_contexts, writer_image_digest, deadline_monotonic
        raise NotImplementedError

    def authorize_claim_set(self, scope, *, expected_identity,
                            expected_gate_generation, deadline_monotonic):
        if self._authorize_claim_set is None:
            raise NotImplementedError
        return self._authorize_claim_set(
            scope,
            expected_identity=expected_identity,
            expected_gate_generation=expected_gate_generation,
            deadline_monotonic=deadline_monotonic)

    def authorize_launch(self, scope, *, expected_identity,
                         expected_gate_generation, deadline_monotonic):
        if self._authorize_launch is None:
            raise NotImplementedError
        return self._authorize_launch(
            scope,
            expected_identity=expected_identity,
            expected_gate_generation=expected_gate_generation,
            deadline_monotonic=deadline_monotonic)


def test_observation_policy_identity_mismatch_precedes_provider_read(
        monkeypatch):
    repository = types.SimpleNamespace(read_reconciliation_gate=mock.Mock(
        return_value=_gate(sequenced=True)))
    provider_read = mock.Mock()
    monkeypatch.setattr(reserved_capacity, 'query_pool_capacity_target',
                        provider_read)
    monkeypatch.setattr(
        reclaim, 'require_unique_policy', lambda: _Policy(identity=_identity(
            fleet_digest=_OTHER_FLEET_DIGEST,
            inventory_digest=_OTHER_INVENTORY_DIGEST)))

    with pytest.raises(reclaim.ReclaimAttestationError, match='does not match'):
        reserved_capacity._query_pool_capacity_target_with_reclaim_policy(
            repository, mock.sentinel.target,
            time.monotonic() + 5)

    provider_read.assert_not_called()


def test_policy_complete_launch_fence_round_trip():
    context = _launch_context()

    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)

    assert fence is not None
    assert fence.policy_bound
    assert fence.reconciliation_gate_generation == _GATE_GENERATION
    assert fence.reclaim_fleet_bundle_sha256 == _FLEET_DIGEST
    assert fence.reclaim_policy_revision == 'policy-v1'
    assert fence.reclaim_provider_inventory_sha256 == _INVENTORY_DIGEST
    assert set(context).issuperset(constants.RESERVED_FILL_LAUNCH_FENCE_KEYS)


@pytest.mark.parametrize(
    ('mutation', 'message'),
    [
        (lambda context: context.pop(
            constants.RESERVED_FILL_LAUNCH_RECLAIM_POLICY_REVISION_KEY),
         'incomplete'),
        (lambda context: context.__setitem__(
            constants.RESERVED_FILL_LAUNCH_RECLAIM_FLEET_BUNDLE_SHA256_KEY, 'A'
            * 64), 'identity is invalid'),
        (lambda context: context.__setitem__(
            constants.RESERVED_FILL_LAUNCH_GATE_GENERATION_KEY, True),
         'identity is invalid'),
    ],
)
def test_partial_or_malformed_policy_identity_is_rejected(mutation, message):
    context = _launch_context()
    mutation(context)

    with pytest.raises(ValueError, match=message):
        reserved_capacity.parse_protocol_v2_launch_fence(context)


def test_partial_policy_identity_is_rejected_by_fence_builder():
    pool_key = broker.make_pool_key('phx-context',
                                    'H200',
                                    protocol_version=broker.PROTOCOL_V2,
                                    physical_cluster_uid='physical-uid')

    with pytest.raises(ValueError, match='complete or absent'):
        reserved_capacity.make_protocol_v2_launch_fence(
            pool_key=pool_key,
            service_generation=7,
            service_version=3,
            physical_cluster_uid='physical-uid',
            kubernetes_context='phx-context',
            accelerator='H200',
            accelerator_count=1,
            reconciliation_gate_generation=_GATE_GENERATION)


@pytest.mark.parametrize(
    ('field', 'replacement'),
    [
        ('version', 4),
        ('reserved_fill_pool_key',
         broker.make_pool_key('phx-context',
                              'H200',
                              protocol_version=broker.PROTOCOL_V2,
                              physical_cluster_uid='replacement-uid')),
        ('reserved_fill_service_generation', 8),
        ('reserved_fill_physical_cluster_uid', 'replacement-uid'),
        ('reserved_fill_kubernetes_context', 'other-context'),
        ('reserved_fill_reconciliation_gate_generation', 12),
        ('reserved_fill_reclaim_fleet_bundle_sha256', _OTHER_FLEET_DIGEST),
        ('reserved_fill_reclaim_policy_revision', 'policy-v2'),
        ('reserved_fill_reclaim_provider_inventory_sha256',
         _OTHER_INVENTORY_DIGEST),
        ('reserved_fill_worker_projection_sha256', 'f' * 64),
    ],
)
def test_every_durable_policy_fence_field_must_match_request(
        field, replacement):
    context = _launch_context()
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None
    durable = _durable_replica(context)
    setattr(durable, field, replacement)
    if field == 'reserved_fill_kubernetes_context':
        durable.resources_override['region'] = replacement
        durable.location['region'] = replacement

    with pytest.raises(ValueError):
        reserved_capacity.validate_protocol_v2_launch_fence_against_replica(
            fence, durable)


@pytest.mark.parametrize(
    ('mapping_name', 'field', 'replacement'),
    [
        ('resources_override', 'region', 'other-context'),
        ('resources_override', 'accelerators', {
            'H200': 2
        }),
        ('location', 'region', 'other-context'),
        ('location', 'accelerators', {
            'H200': 2
        }),
    ],
)
def test_durable_location_and_shape_must_match_request(mapping_name, field,
                                                       replacement):
    context = _launch_context()
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None
    durable = _durable_replica(context)
    getattr(durable, mapping_name)[field] = replacement

    with pytest.raises(ValueError):
        reserved_capacity.validate_protocol_v2_launch_fence_against_replica(
            fence, durable)


@pytest.mark.parametrize(
    ('field', 'replacement'),
    [
        ('zero_cost_admission_sequence', None),
        ('zero_cost_admission_sequence', 0),
        ('reserved_fill_allocation_generation', None),
        ('reserved_fill_allocation_input_sha256', None),
        ('reserved_fill_allocation_claim_generation', 8),
        ('reserved_fill_observation_generation', 0),
        ('reserved_fill_observation_sequence', -1),
        ('reserved_fill_intent_idempotency_key', None),
        ('is_zero_cost', False),
    ],
)
def test_policy_bound_terminal_fence_requires_admitted_allocation_provenance(
        field, replacement):
    context = _launch_context()
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None
    durable = _durable_replica(context)
    setattr(durable, field, replacement)

    with pytest.raises(ValueError, match='admitted allocation provenance'):
        reserved_capacity.validate_protocol_v2_launch_fence_against_replica(
            fence, durable)


def test_base_v2_fence_is_accepted_only_while_gate_is_legacy(monkeypatch):
    context = _launch_context(policy_bound=False)
    provisioner = _provisioner(context)
    global_guard = mock.Mock(return_value=contextlib.nullcontext())
    owner_entries = []

    @contextlib.contextmanager
    def _owner_guard():
        owner_entries.append('enter')
        yield _launch_snapshot(context)

    monkeypatch.setattr(serve_state,
                        'reserved_fill_reclaim_gate_authority_guard',
                        global_guard)
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        _owner_guard)
    require_policy = mock.Mock()
    monkeypatch.setattr(reclaim, 'require_unique_policy', require_policy)

    _install_gate(monkeypatch, _gate(sequenced=False))
    with provisioner._service_replica_launch_provider_guard():
        owner_entries.append('provider')

    assert owner_entries == ['enter', 'provider']
    require_policy.assert_not_called()

    owner_entries.clear()
    _install_gate(monkeypatch, _gate(sequenced=True))
    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='authority changed'):
        with provisioner._service_replica_launch_provider_guard():
            owner_entries.append('provider')

    assert owner_entries == ['enter']
    require_policy.assert_not_called()


def test_legacy_fill_request_cannot_bypass_sequenced_global_gate(monkeypatch):
    context = _legacy_launch_context()
    provisioner = _provisioner(context)
    global_entries = []
    snapshot = _launch_snapshot(context)

    @contextlib.contextmanager
    def _owner_guard():
        yield snapshot

    @contextlib.contextmanager
    def _global_guard(*, shared):
        assert shared
        global_entries.append('enter')
        yield object()

    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        _owner_guard)
    monkeypatch.setattr(serve_state,
                        'reserved_fill_reclaim_gate_authority_guard',
                        _global_guard)
    require_policy = mock.Mock()
    monkeypatch.setattr(reclaim, 'require_unique_policy', require_policy)

    _install_gate(monkeypatch, _gate(sequenced=False))
    with provisioner._service_replica_launch_provider_guard():
        global_entries.append('provider')
    assert global_entries == ['enter', 'provider']

    global_entries.clear()
    _install_gate(monkeypatch, _gate(sequenced=True))
    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='authority changed'):
        with provisioner._service_replica_launch_provider_guard():
            global_entries.append('provider')
    assert global_entries == ['enter']
    require_policy.assert_not_called()


def test_ordinary_zero_cost_replica_does_not_enter_reclaim_gate(monkeypatch):
    context = _legacy_launch_context()
    provisioner = _provisioner(context)
    ordinary_zero_cost = types.SimpleNamespace(reserved_fill=False,
                                               is_zero_cost=True)
    snapshot = serve_state.ServiceReplicaLaunchFenceSnapshot(ordinary_zero_cost)
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        lambda: contextlib.nullcontext(snapshot))
    global_guard = mock.Mock()
    require_policy = mock.Mock()
    monkeypatch.setattr(serve_state,
                        'reserved_fill_reclaim_gate_authority_guard',
                        global_guard)
    monkeypatch.setattr(reclaim, 'require_unique_policy', require_policy)
    provider_ran = False

    with provisioner._service_replica_launch_provider_guard():
        provider_ran = True

    assert provider_ran
    global_guard.assert_not_called()
    require_policy.assert_not_called()


def test_durable_row_mismatch_fails_before_policy_or_global_guard(monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    durable = _durable_replica(context)
    durable.reserved_fill_service_generation += 1
    snapshot = serve_state.ServiceReplicaLaunchFenceSnapshot(durable)
    owner_guard = mock.Mock(return_value=contextlib.nullcontext(snapshot))
    require_policy = mock.Mock()
    global_guard = mock.Mock()
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        owner_guard)
    monkeypatch.setattr(reclaim, 'require_unique_policy', require_policy)
    monkeypatch.setattr(serve_state,
                        'reserved_fill_reclaim_gate_authority_guard',
                        global_guard)

    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='durable replica row'):
        with provisioner._service_replica_launch_provider_guard():
            pytest.fail('provider body must not run')

    owner_guard.assert_called_once_with()
    require_policy.assert_not_called()
    global_guard.assert_not_called()


def test_terminal_policy_identity_mismatch_precedes_ticket_global_and_provider(
        monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    authorize_launch = mock.Mock()
    policy = _Policy(authorize_launch,
                     identity=_identity(
                         fleet_digest=_OTHER_FLEET_DIGEST,
                         inventory_digest=_OTHER_INVENTORY_DIGEST))
    owner_guard = mock.Mock(
        return_value=contextlib.nullcontext(_launch_snapshot(context)))
    global_guard = mock.Mock(return_value=contextlib.nullcontext())
    monkeypatch.setattr(reclaim, 'require_unique_policy', lambda: policy)
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        owner_guard)
    monkeypatch.setattr(serve_state,
                        'reserved_fill_reclaim_gate_authority_guard',
                        global_guard)
    provider_ran = False

    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='policy refused'):
        with provisioner._service_replica_launch_provider_guard():
            provider_ran = True

    assert not provider_ran
    owner_guard.assert_called_once_with()
    authorize_launch.assert_not_called()
    global_guard.assert_not_called()


def test_service_guard_precedes_policy_authorization_and_global_guard(
        monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    expected_scope = _scope(context)
    events = []

    def _authorize(scope, *, expected_identity, expected_gate_generation,
                   deadline_monotonic):
        events.append('policy')
        assert deadline_monotonic > time.monotonic()
        assert scope == expected_scope
        assert expected_identity == _identity()
        assert expected_gate_generation == _GATE_GENERATION
        return _launch_authorization(scope)

    @contextlib.contextmanager
    def _global_guard(*, shared):
        assert shared
        events.append('global-enter')
        yield
        events.append('global-exit')

    @contextlib.contextmanager
    def _owner_guard():
        events.append('service-enter')
        yield _launch_snapshot(context)
        events.append('service-exit')

    monkeypatch.setattr(reclaim, 'require_unique_policy',
                        lambda: _Policy(_authorize))
    monkeypatch.setattr(serve_state,
                        'reserved_fill_reclaim_gate_authority_guard',
                        _global_guard)
    monkeypatch.setattr(
        serve_state, 'reserved_fill_reclaim_launch_authority_holds',
        lambda scope, authorization, launch_context, launch_snapshot: events.
        append('recheck') or (scope == expected_scope and authorization is
                              not None and launch_snapshot is not None))
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        _owner_guard)

    with provisioner._service_replica_launch_provider_guard():
        events.append('provider')

    assert events == [
        'service-enter', 'policy', 'global-enter', 'recheck', 'provider',
        'global-exit', 'service-exit'
    ]


def test_failed_service_authority_mints_no_policy_ticket_or_global_guard(
        monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    require_policy = mock.Mock()
    global_guard = mock.Mock()

    class _FailedOwnerGuard:

        def __enter__(self):
            raise exceptions.ServeReplicaLaunchFenceError('service superseded')

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        _FailedOwnerGuard)
    monkeypatch.setattr(reclaim, 'require_unique_policy', require_policy)
    monkeypatch.setattr(serve_state,
                        'reserved_fill_reclaim_gate_authority_guard',
                        global_guard)

    with pytest.raises(exceptions.ServeReplicaLaunchFenceError,
                       match='service superseded'):
        with provisioner._service_replica_launch_provider_guard():
            pytest.fail('provider body must not run')

    require_policy.assert_not_called()
    global_guard.assert_not_called()


@pytest.mark.parametrize('ticket_kind', ['missing-policy', 'stale', 'mismatch'])
def test_invalid_policy_ticket_fails_before_any_guard_or_provider(
        monkeypatch, ticket_kind):
    context = _launch_context()
    provisioner = _provisioner(context)
    expected_scope = _scope(context)
    global_guard = mock.Mock(return_value=contextlib.nullcontext())
    owner_guard = mock.Mock(
        return_value=contextlib.nullcontext(_launch_snapshot(context)))
    if ticket_kind == 'missing-policy':
        policy_result = mock.Mock(
            side_effect=reclaim.ReclaimAttestationError('no policy'))
    elif ticket_kind == 'stale':
        ticket = _launch_authorization(
            expected_scope,
            completed_monotonic=(time.monotonic() -
                                 reclaim.AUTHORIZATION_MAX_AGE_SECONDS - 1))
        policy_result = mock.Mock(
            return_value=_Policy(lambda _scope, **_kwargs: ticket))
    else:
        ticket = _launch_authorization(
            expected_scope,
            identity=_identity(fleet_digest=_OTHER_FLEET_DIGEST,
                               inventory_digest=_OTHER_INVENTORY_DIGEST))
        policy_result = mock.Mock(
            return_value=_Policy(lambda _scope, **_kwargs: ticket))
    monkeypatch.setattr(reclaim, 'require_unique_policy', policy_result)
    monkeypatch.setattr(serve_state,
                        'reserved_fill_reclaim_gate_authority_guard',
                        global_guard)
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        owner_guard)
    provider_ran = False

    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='policy refused'):
        with provisioner._service_replica_launch_provider_guard():
            provider_ran = True

    assert not provider_ran
    global_guard.assert_not_called()
    owner_guard.assert_called_once_with()


def test_tampered_fence_identity_fails_locked_gate_recheck_before_provider(
        monkeypatch):
    context = _launch_context(fleet_digest=_OTHER_FLEET_DIGEST,
                              inventory_digest=_OTHER_INVENTORY_DIGEST)
    provisioner = _provisioner(context)
    tampered_identity = _identity(fleet_digest=_OTHER_FLEET_DIGEST,
                                  inventory_digest=_OTHER_INVENTORY_DIGEST)

    def _authorize(scope, *, expected_identity, expected_gate_generation,
                   deadline_monotonic):
        assert deadline_monotonic > time.monotonic()
        assert expected_identity == tampered_identity
        return _launch_authorization(scope,
                                     identity=tampered_identity,
                                     gate_generation=expected_gate_generation)

    monkeypatch.setattr(reclaim, 'require_unique_policy',
                        lambda: _Policy(_authorize, identity=tampered_identity))
    monkeypatch.setattr(
        serve_state, 'reserved_fill_reclaim_gate_authority_guard',
        lambda *, shared: contextlib.nullcontext() if shared else None)
    owner_guard = mock.Mock(
        return_value=contextlib.nullcontext(_launch_snapshot(context)))
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        owner_guard)
    _install_gate(monkeypatch, _gate(sequenced=True, identity=_identity()))
    provider_ran = False

    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='authority'):
        with provisioner._service_replica_launch_provider_guard():
            provider_ran = True

    assert not provider_ran
    owner_guard.assert_called_once_with()


def test_missing_terminal_provider_receipt_fails_before_provider(monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)

    def _authorize(scope, **_kwargs):
        return _launch_authorization(scope)

    monkeypatch.setattr(reclaim, 'require_unique_policy',
                        lambda: _Policy(_authorize))
    monkeypatch.setattr(
        serve_state, 'reserved_fill_reclaim_gate_authority_guard',
        lambda *, shared: contextlib.nullcontext() if shared else None)
    owner_guard = mock.Mock(
        return_value=contextlib.nullcontext(_launch_snapshot(context)))
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        owner_guard)
    _install_gate(monkeypatch, _gate(sequenced=True))
    receipt_guard = mock.Mock(return_value=False)
    monkeypatch.setattr(reserved_fill_reclaim_proofs,
                        'provider_proof_reference_holds_in_connection',
                        receipt_guard)
    provider_ran = False

    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='authority'):
        with provisioner._service_replica_launch_provider_guard():
            provider_ran = True

    assert not provider_ran
    receipt_guard.assert_called_once()
    owner_guard.assert_called_once_with()


def test_provider_exception_classification_is_preserved(monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    expected_scope = _scope(context)

    def _authorize(scope, **_kwargs):
        return _launch_authorization(scope)

    monkeypatch.setattr(reclaim, 'require_unique_policy',
                        lambda: _Policy(_authorize))
    monkeypatch.setattr(
        serve_state, 'reserved_fill_reclaim_gate_authority_guard',
        lambda *, shared: contextlib.nullcontext() if shared else None)
    monkeypatch.setattr(
        serve_state, 'reserved_fill_reclaim_launch_authority_holds',
        lambda scope, authorization, launch_context, launch_snapshot: scope ==
        expected_scope and authorization is not None and launch_snapshot is
        not None)
    monkeypatch.setattr(
        provisioner, '_service_replica_launch_provider_owner_guard',
        lambda: contextlib.nullcontext(_launch_snapshot(context)))
    provider_error = provision_common.ProvisionerError(
        'provider capacity classification')

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        with provisioner._service_replica_launch_provider_guard():
            raise provider_error

    assert exc_info.value is provider_error


@pytest.mark.parametrize('loss_point', ['before', 'after'])
def test_lost_global_guard_never_reports_an_authorized_provider_effect(
        monkeypatch, loss_point):
    context = _launch_context()
    provisioner = _provisioner(context)
    expected_scope = _scope(context)

    monkeypatch.setattr(
        reclaim, 'require_unique_policy',
        lambda: _Policy(lambda scope, **_kwargs: _launch_authorization(scope)))
    monkeypatch.setattr(
        serve_state, 'reserved_fill_reclaim_gate_authority_guard',
        lambda *, shared: contextlib.nullcontext(object()) if shared else None)
    validity = ([False] if loss_point == 'before' else [True, False])
    monkeypatch.setattr(serve_state,
                        'reserved_fill_reclaim_gate_authority_guard_is_valid',
                        lambda _: validity.pop(0))
    monkeypatch.setattr(
        serve_state, 'reserved_fill_reclaim_launch_authority_holds',
        lambda scope, authorization, launch_context, launch_snapshot: scope ==
        expected_scope and authorization is not None and launch_snapshot is
        not None)
    owner_guard = mock.Mock(
        return_value=contextlib.nullcontext(_launch_snapshot(context)))
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        owner_guard)
    provider_ran = False

    with pytest.raises(exceptions.ReservedFillLaunchFenceError, match='guard'):
        with provisioner._service_replica_launch_provider_guard():
            provider_ran = True

    assert provider_ran is (loss_point == 'after')
    owner_guard.assert_called_once_with()


def _claim_edge() -> dict[str, object]:
    pool_key = broker.make_pool_key('phx-context',
                                    'H200',
                                    protocol_version=broker.PROTOCOL_V2,
                                    physical_cluster_uid='physical-uid')
    return {
        'service_name': 'svc',
        'pool_key': pool_key,
        'legacy_pool_key': broker.make_pool_key('phx-context', 'H200'),
        'pool_position': 0,
        'access_context': 'phx-context',
        'physical_cluster_uid': 'physical-uid',
        'accelerator_names': ['H200'],
        'service_generation': 7,
        'weight': 1.0,
        'floor_replicas': 0,
        'gpus_per_replica': 1,
        'holdings_fill': 0,
        'effective_cap': 3,
        'launchable': 1,
        'heartbeat_ts': 1000.0,
    }


def _replace_claim_set() -> int | None:
    return broker.replace_claim_set('svc',
                                    semantic_hash='semantic-v1',
                                    global_headroom=3,
                                    utilization_ceiling=3,
                                    utilization_state={},
                                    edges=[_claim_edge()],
                                    expected_service_hash='incarnation-a',
                                    expected_controller_owner=(123, '10.0.0.1'))


@pytest.mark.parametrize('failure_kind', ['missing-identity', 'missing-policy'])
def test_sequenced_claim_without_complete_policy_fails_before_broker_lock(
        monkeypatch, failure_kind):
    if failure_kind == 'missing-identity':
        gate = types.SimpleNamespace(sequenced_active=True,
                                     generation=_GATE_GENERATION,
                                     reclaim_policy_identity=None)
        require_policy = mock.Mock()
    else:
        gate = _gate(sequenced=True)
        require_policy = mock.Mock(
            side_effect=reclaim.ReclaimAttestationError('no policy'))
    _install_gate(monkeypatch, gate)
    monkeypatch.setattr(reclaim, 'require_unique_policy', require_policy)
    get_lock = mock.Mock()
    monkeypatch.setattr(broker.locks, 'get_lock', get_lock)

    assert _replace_claim_set() is None
    get_lock.assert_not_called()
    if failure_kind == 'missing-identity':
        require_policy.assert_not_called()
    else:
        require_policy.assert_called_once_with()


def test_sequenced_claim_policy_identity_mismatch_precedes_ticket_and_lock(
        monkeypatch):
    _install_gate(monkeypatch, _gate(sequenced=True))
    authorize_claim_set = mock.Mock()
    policy = _Policy(identity=_identity(
        fleet_digest=_OTHER_FLEET_DIGEST,
        inventory_digest=_OTHER_INVENTORY_DIGEST),
                     authorize_claim_set=authorize_claim_set)
    monkeypatch.setattr(reclaim, 'require_unique_policy', lambda: policy)
    get_lock = mock.Mock()
    monkeypatch.setattr(broker.locks, 'get_lock', get_lock)

    assert _replace_claim_set() is None
    authorize_claim_set.assert_not_called()
    get_lock.assert_not_called()


def test_sequenced_claim_policy_callback_precedes_broker_lock(monkeypatch):
    gate = _gate(sequenced=True)
    _install_gate(monkeypatch, gate)
    events = []

    def _authorize(scope, *, expected_identity, expected_gate_generation,
                   deadline_monotonic):
        events.append('policy')
        assert deadline_monotonic > time.monotonic()
        assert expected_identity == _identity()
        assert expected_gate_generation == _GATE_GENERATION
        return reclaim.ReclaimClaimAuthorization(
            identity=expected_identity,
            gate_generation=expected_gate_generation,
            scope=scope,
            completed_monotonic=time.monotonic())

    class _RecordingLock:
        """Record the first instant at which broker authority is held."""

        def acquire(self, *, blocking):
            assert blocking

            @contextlib.contextmanager
            def _acquired():
                events.append('broker-lock-enter')
                yield
                events.append('broker-lock-exit')

            return _acquired()

    def _get_lock(*args, **kwargs):
        del args, kwargs
        events.append('broker-lock-create')
        return _RecordingLock()

    def _persist(*args, **kwargs):
        del args
        events.append('persist')
        assert kwargs['reclaim_claim_scope'].service_name == 'svc'
        assert isinstance(kwargs['reclaim_claim_authorization'],
                          reclaim.ReclaimClaimAuthorization)
        return 7

    monkeypatch.setattr(reclaim, 'require_unique_policy',
                        lambda: _Policy(authorize_claim_set=_authorize))
    monkeypatch.setattr(broker.locks, 'get_lock', _get_lock)
    monkeypatch.setattr(broker, 'get_protocol_version',
                        lambda: broker.PROTOCOL_V2)
    monkeypatch.setattr(broker, '_prune_claims', lambda *_args: [])
    monkeypatch.setattr(broker, '_claim_rows', lambda *_args: [])
    monkeypatch.setattr(serve_state, 'replace_reserved_fill_claim_set',
                        _persist)

    assert _replace_claim_set() == 7
    assert events == [
        'policy', 'broker-lock-create', 'broker-lock-enter', 'persist',
        'broker-lock-exit'
    ]
