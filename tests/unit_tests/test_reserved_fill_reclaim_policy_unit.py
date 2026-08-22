"""Non-PostgreSQL tests for terminal reserved-fill reclaim enforcement."""
# pylint: disable=protected-access,unexpected-keyword-arg

import concurrent.futures
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


def _install_gate(monkeypatch,
                  gate,
                  *,
                  claim_generation: int = 7,
                  edge_rows: list[dict[str, object]] | None = None) -> None:
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
    monkeypatch.setattr(serve_state, 'get_placement_catalog',
                        lambda _service_name, _version: {'num_nodes': 1})
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
    if edge_rows is None:
        edge_rows = [{
            'pool_key': pool_key,
            'access_context': 'phx-context',
            'physical_cluster_uid': 'physical-uid',
            'gpus_per_replica': 1,
            'service_generation': claim_generation,
            'accelerator_names': ['h200'],
            'worker_projection_sha256_by_accelerator': {
                'h200': _WORKER_PROJECTION_DIGEST,
            },
        }]
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
            'edge_count': len(edge_rows),
        },
        edge_rows,
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

    def renew_provider_proofs(self, *, expected_identity,
                              expected_gate_generation, deadline_monotonic):
        del expected_identity, expected_gate_generation, deadline_monotonic
        raise NotImplementedError

    def authorize_launch(self, scope, *, expected_identity,
                         expected_gate_generation, deadline_monotonic):
        if self._authorize_launch is None:
            raise NotImplementedError
        return self._authorize_launch(
            scope,
            expected_identity=expected_identity,
            expected_gate_generation=expected_gate_generation,
            deadline_monotonic=deadline_monotonic)


def _install_launch_policy(monkeypatch, *, callback=None, identity=None):
    calls = []

    def _authorize(scope, *, expected_identity, expected_gate_generation,
                   deadline_monotonic):
        calls.append((scope, expected_identity, expected_gate_generation,
                      deadline_monotonic))
        if callback is not None:
            return callback(scope,
                            expected_identity=expected_identity,
                            expected_gate_generation=expected_gate_generation,
                            deadline_monotonic=deadline_monotonic)
        return _launch_authorization(scope,
                                     identity=expected_identity,
                                     gate_generation=expected_gate_generation)

    policy = _Policy(authorize_launch=_authorize, identity=identity)
    monkeypatch.setattr(reclaim, 'require_unique_policy', lambda: policy)
    return calls


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


def test_committed_provider_guard_uses_frozen_handoff_fresh_proof_and_uid(
        monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    events = []

    @contextlib.contextmanager
    def _owner_guard():
        events.append('service-enter')
        yield _launch_snapshot(context)
        events.append('service-exit')

    @contextlib.contextmanager
    def _physical_guard(kubernetes_context, physical_cluster_uid):
        assert kubernetes_context == 'phx-context'
        assert physical_cluster_uid == 'physical-uid'
        events.append('physical-enter')
        yield
        events.append('physical-exit')

    def _committed_authority(scope, authorization, launch_context,
                             launch_snapshot):
        assert launch_context is context
        assert launch_snapshot == _launch_snapshot(context)
        assert scope == _scope(context)
        assert authorization.scope == scope
        assert authorization.identity == _identity()
        assert authorization.gate_generation == _GATE_GENERATION
        events.append('committed-authority')
        return True

    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        _owner_guard)
    monkeypatch.setattr(serve_state,
                        'reserved_fill_committed_launch_authority_holds',
                        _committed_authority)
    monkeypatch.setattr(backend.kubernetes_adaptor,
                        'physical_cluster_uid_fence', _physical_guard)
    policy_calls = _install_launch_policy(monkeypatch)
    monkeypatch.setattr(
        serve_state, 'reserved_fill_reclaim_gate_authority_guard',
        mock.Mock(side_effect=AssertionError('postcommit global gate')))

    with provisioner._service_replica_launch_provider_guard():
        events.append('provider')

    assert events == [
        'service-enter',
        'committed-authority',
        'physical-enter',
        'provider',
        'physical-exit',
        'service-exit',
    ]
    assert len(policy_calls) == 1
    scope, identity, generation, deadline = policy_calls[0]
    assert scope == _scope(context)
    assert identity == _identity()
    assert generation == _GATE_GENERATION
    assert deadline > time.monotonic()


def test_committed_provider_guard_pauses_typed_proof_unavailability_before_effect(
        monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    committed = mock.Mock(return_value=True)
    physical = mock.Mock()

    def _unavailable(*_args, **_kwargs):
        raise reclaim.ReclaimProviderProofUnavailableError(
            'proof renewer has not published yet')

    _install_launch_policy(monkeypatch, callback=_unavailable)
    monkeypatch.setattr(serve_state,
                        'reserved_fill_committed_launch_authority_holds',
                        committed)
    monkeypatch.setattr(backend.kubernetes_adaptor,
                        'physical_cluster_uid_fence', physical)

    with pytest.raises(
            exceptions.ReservedFillProviderProofPausedError) as error:
        with provisioner._reserved_fill_committed_provider_guard(
                _launch_snapshot(context)):
            pytest.fail('provider body must not run')

    assert error.value.retry_wait_seconds == 3
    committed.assert_not_called()
    physical.assert_not_called()


@pytest.mark.parametrize(
    'attestation_error',
    (reclaim.ReclaimAttestationError('malformed proof'),
     reclaim.ReclaimProviderNonconformanceError('provider mismatch')))
def test_committed_provider_guard_keeps_permanent_attestation_terminal(
        monkeypatch, attestation_error):
    context = _launch_context()
    provisioner = _provisioner(context)
    committed = mock.Mock(return_value=True)
    physical = mock.Mock()

    def _refused(*_args, **_kwargs):
        raise attestation_error

    _install_launch_policy(monkeypatch, callback=_refused)
    monkeypatch.setattr(serve_state,
                        'reserved_fill_committed_launch_authority_holds',
                        committed)
    monkeypatch.setattr(backend.kubernetes_adaptor,
                        'physical_cluster_uid_fence', physical)

    with pytest.raises(exceptions.ReservedFillLaunchFenceError) as error:
        with provisioner._reserved_fill_committed_provider_guard(
                _launch_snapshot(context)):
            pytest.fail('provider body must not run')

    assert not isinstance(error.value, exceptions.ExecutionRetryableError)
    committed.assert_not_called()
    physical.assert_not_called()


def test_committed_provider_guard_rejects_proof_pause_after_effect_started(
        monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    _install_launch_policy(monkeypatch)
    monkeypatch.setattr(serve_state,
                        'reserved_fill_committed_launch_authority_holds',
                        lambda *_args: True)
    monkeypatch.setattr(backend.kubernetes_adaptor,
                        'physical_cluster_uid_fence',
                        lambda *_args: contextlib.nullcontext())
    late_pause = exceptions.ReservedFillProviderProofPausedError(
        'late pause', 'must fail closed', retry_wait_seconds=3)

    with pytest.raises(exceptions.ReservedFillLaunchFenceError) as error:
        with provisioner._reserved_fill_committed_provider_guard(
                _launch_snapshot(context)):
            raise late_pause

    assert error.value.__cause__ is late_pause
    assert not isinstance(error.value, exceptions.ExecutionRetryableError)


def test_mutable_successor_state_cannot_revoke_committed_provider_epochs(
        monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    snapshot = _launch_snapshot(context)
    authority = mock.Mock(return_value=True)
    physical_entries = []

    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        lambda: contextlib.nullcontext(snapshot))
    monkeypatch.setattr(serve_state,
                        'reserved_fill_committed_launch_authority_holds',
                        authority)

    @contextlib.contextmanager
    def _physical_guard(kubernetes_context, physical_cluster_uid):
        physical_entries.append((kubernetes_context, physical_cluster_uid))
        yield

    monkeypatch.setattr(backend.kubernetes_adaptor,
                        'physical_cluster_uid_fence', _physical_guard)
    policy_calls = _install_launch_policy(monkeypatch)
    # These are the old mutable successor authorities. A post-create
    # with-guard/pass seam must never consult any of them.  Fresh external
    # facts are still attested against the original frozen scope.
    monkeypatch.setattr(
        serve_state, 'reserved_fill_reclaim_gate_authority_guard',
        mock.Mock(side_effect=AssertionError('successor gate consulted')))
    with provisioner._service_replica_launch_provider_guard():
        pass
    # Model a later Kubernetes readiness/update provider epoch after mutable
    # allocation, claim, observation, and policy publications have advanced.
    with provisioner._service_replica_launch_provider_guard():
        pass

    assert authority.call_count == 2
    assert all(
        call.args[0] == _scope(context) for call in authority.call_args_list)
    assert [call[0] for call in policy_calls] == [_scope(context)] * 2
    assert [call[2] for call in policy_calls] == [_GATE_GENERATION] * 2
    assert physical_entries == [('phx-context', 'physical-uid')] * 2


def test_physical_uid_retarget_fails_before_provider_effect(monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    snapshot = _launch_snapshot(context)
    provider_ran = False

    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        lambda: contextlib.nullcontext(snapshot))
    monkeypatch.setattr(serve_state,
                        'reserved_fill_committed_launch_authority_holds',
                        lambda *_args: True)
    _install_launch_policy(monkeypatch)

    @contextlib.contextmanager
    def _retargeted_physical_guard(_context, _uid):
        if _context is not None:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'context alias was retargeted')
        yield  # pragma: no cover

    monkeypatch.setattr(backend.kubernetes_adaptor,
                        'physical_cluster_uid_fence',
                        _retargeted_physical_guard)

    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='committed reserved-fill handoff'):
        with provisioner._service_replica_launch_provider_guard():
            provider_ran = True

    assert not provider_ran


def test_lost_committed_service_authority_fails_before_physical_effect(
        monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    snapshot = _launch_snapshot(context)
    physical_guard = mock.Mock()

    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        lambda: contextlib.nullcontext(snapshot))
    monkeypatch.setattr(serve_state,
                        'reserved_fill_committed_launch_authority_holds',
                        lambda *_args: False)
    _install_launch_policy(monkeypatch)
    monkeypatch.setattr(backend.kubernetes_adaptor,
                        'physical_cluster_uid_fence', physical_guard)

    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='committed intent no longer authorizes'):
        with provisioner._service_replica_launch_provider_guard():
            pytest.fail('provider body must not run')

    physical_guard.assert_not_called()


def test_ordinary_zero_cost_replica_needs_no_reserved_fill_handoff(monkeypatch):
    context = _legacy_launch_context()
    provisioner = _provisioner(context)
    ordinary_zero_cost = types.SimpleNamespace(reserved_fill=False,
                                               is_zero_cost=True)
    snapshot = serve_state.ServiceReplicaLaunchFenceSnapshot(ordinary_zero_cost)
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        lambda: contextlib.nullcontext(snapshot))
    committed = mock.Mock()
    physical = mock.Mock()
    monkeypatch.setattr(serve_state,
                        'reserved_fill_committed_launch_authority_holds',
                        committed)
    monkeypatch.setattr(backend.kubernetes_adaptor,
                        'physical_cluster_uid_fence', physical)

    with provisioner._service_replica_launch_provider_guard():
        pass

    committed.assert_not_called()
    physical.assert_not_called()


def test_provider_exception_classification_is_preserved(monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    monkeypatch.setattr(
        provisioner, '_service_replica_launch_provider_owner_guard',
        lambda: contextlib.nullcontext(_launch_snapshot(context)))
    monkeypatch.setattr(serve_state,
                        'reserved_fill_committed_launch_authority_holds',
                        lambda *_args: True)
    _install_launch_policy(monkeypatch)
    monkeypatch.setattr(backend.kubernetes_adaptor,
                        'physical_cluster_uid_fence',
                        lambda *_args: contextlib.nullcontext())
    provider_error = provision_common.ProvisionerError(
        'provider capacity classification')

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        with provisioner._service_replica_launch_provider_guard():
            raise provider_error

    assert exc_info.value is provider_error


def test_committed_provider_guard_rejects_current_plugin_identity_rotation(
        monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    snapshot = _launch_snapshot(context)
    provider_ran = False
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        lambda: contextlib.nullcontext(snapshot))
    monkeypatch.setattr(
        reclaim, 'require_unique_policy',
        lambda: _Policy(authorize_launch=lambda *_args, **_kwargs: pytest.fail(
            'identity mismatch must precede external proof'),
                        identity=_identity(fleet_digest=_OTHER_FLEET_DIGEST)))

    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='Deployment reclaim policy refused'):
        with provisioner._service_replica_launch_provider_guard():
            provider_ran = True

    assert not provider_ran


def test_committed_provider_guard_rejects_expired_fresh_proof_before_effect(
        monkeypatch):
    context = _launch_context()
    provisioner = _provisioner(context)
    snapshot = _launch_snapshot(context)
    provider_ran = False
    committed = mock.Mock(return_value=True)
    monkeypatch.setattr(provisioner,
                        '_service_replica_launch_provider_owner_guard',
                        lambda: contextlib.nullcontext(snapshot))
    monkeypatch.setattr(serve_state,
                        'reserved_fill_committed_launch_authority_holds',
                        committed)

    def _expired(scope, *, expected_identity, expected_gate_generation,
                 deadline_monotonic):
        del deadline_monotonic
        return _launch_authorization(
            scope,
            identity=expected_identity,
            gate_generation=expected_gate_generation,
            completed_monotonic=(time.monotonic() -
                                 reclaim.AUTHORIZATION_MAX_AGE_SECONDS))

    _install_launch_policy(monkeypatch, callback=_expired)

    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='Deployment reclaim policy refused'):
        with provisioner._service_replica_launch_provider_guard():
            provider_ran = True

    assert not provider_ran
    committed.assert_not_called()


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


def _replace_claim_set(claim_authorization_executor=None,) -> int | None:
    return broker.replace_claim_set(
        'svc',
        semantic_hash='semantic-v1',
        global_headroom=3,
        utilization_ceiling=3,
        utilization_state={},
        edges=[_claim_edge()],
        expected_service_hash='incarnation-a',
        expected_controller_owner=(123, '10.0.0.1'),
        claim_authorization_executor=(claim_authorization_executor))


@pytest.mark.parametrize('failure_kind', ['missing-identity', 'missing-policy'])
def test_sequenced_claim_without_complete_policy_fails_before_broker_lock(
        monkeypatch, failure_kind):
    if failure_kind == 'missing-identity':
        gate = types.SimpleNamespace(sequenced_active=True,
                                     generation=_GATE_GENERATION,
                                     reclaim_policy_identity=None)
        authorize = mock.Mock()
    else:
        gate = _gate(sequenced=True)
        authorize = mock.Mock(
            side_effect=reclaim.ReclaimAttestationError('no policy'))
    _install_gate(monkeypatch, gate)
    monkeypatch.setattr(broker, '_authorize_reclaim_claim_set_in_boundary',
                        authorize)
    get_lock = mock.Mock()
    monkeypatch.setattr(broker.locks, 'get_lock', get_lock)

    assert _replace_claim_set(mock.Mock()) is None
    get_lock.assert_not_called()
    if failure_kind == 'missing-identity':
        authorize.assert_not_called()
    else:
        authorize.assert_called_once()


def test_sequenced_claim_policy_identity_mismatch_precedes_ticket_and_lock(
        monkeypatch):
    _install_gate(monkeypatch, _gate(sequenced=True))
    authorize = mock.Mock(
        side_effect=reclaim.ReclaimAttestationError('identity mismatch'))
    monkeypatch.setattr(broker, '_authorize_reclaim_claim_set_in_boundary',
                        authorize)
    get_lock = mock.Mock()
    monkeypatch.setattr(broker.locks, 'get_lock', get_lock)

    assert _replace_claim_set(mock.Mock()) is None
    authorize.assert_called_once()
    get_lock.assert_not_called()


def test_sequenced_claim_policy_callback_precedes_broker_lock(monkeypatch):
    gate = _gate(sequenced=True)
    _install_gate(monkeypatch, gate)
    events = []

    def _authorize(_executor, scope, expected_identity,
                   expected_gate_generation):
        events.append('policy')
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

    monkeypatch.setattr(broker, '_authorize_reclaim_claim_set_in_boundary',
                        _authorize)
    monkeypatch.setattr(broker.locks, 'get_lock', _get_lock)
    monkeypatch.setattr(broker, 'get_protocol_version',
                        lambda: broker.PROTOCOL_V2)
    monkeypatch.setattr(broker, '_prune_claims', lambda *_args: [])
    monkeypatch.setattr(broker, '_claim_rows', lambda *_args: [])
    monkeypatch.setattr(serve_state, 'replace_reserved_fill_claim_set',
                        _persist)

    assert _replace_claim_set(mock.Mock()) == 7
    assert events == [
        'policy', 'broker-lock-create', 'broker-lock-enter', 'persist',
        'broker-lock-exit'
    ]


def test_claim_boundary_timeout_cancels_drains_and_rejects_late_success():
    executor = mock.Mock()
    executor.max_workers = 1
    future = executor.submit.return_value
    future.done.return_value = False
    future.result.side_effect = (concurrent.futures.TimeoutError(),
                                 'late-success')
    with pytest.raises(reclaim.ReclaimAttestationError,
                       match='bounded process lifetime'):
        broker._execute_claim_authorization_in_boundary(executor, int, ('7',),
                                                        5)

    executor.submit.assert_called_once_with(int, '7')
    future.request_cancel.assert_called_once_with()
    assert future.result.call_count == 2
    assert future.result.call_args_list[0].kwargs['timeout'] <= 5
    assert future.result.call_args_list[1].kwargs['timeout'] == (
        broker._RECLAIM_CLAIM_BOUNDARY_DRAIN_TIMEOUT_SECONDS)


def test_claim_boundary_missing_drain_proof_poison_is_controller_terminal():
    callback = mock.Mock()
    executor = mock.Mock()
    executor.max_workers = 1
    executor.poisoned = False
    future = executor.submit.return_value
    future.done.return_value = False
    future.result.side_effect = concurrent.futures.TimeoutError()

    def _poison(error):
        executor.poisoned = True
        callback(error)

    executor._poison.side_effect = _poison
    with pytest.raises(broker.request_process.AmbiguousBoundaryError,
                       match='cannot prove its process family absent') as info:
        broker._execute_claim_authorization_in_boundary(executor, int, ('7',),
                                                        5)

    future.request_cancel.assert_called_once_with()
    executor._poison.assert_called_once_with(info.value)
    callback.assert_called_once_with(info.value)
    assert executor.poisoned
    assert isinstance(info.value.__cause__,
                      broker.request_process.BoundaryExecutionError)
    assert 'without a process-family drain result' in str(info.value.__cause__)


def test_claim_boundary_unreleased_lane_poison_is_controller_terminal(
        monkeypatch):
    executor = mock.Mock()
    executor.max_workers = 1
    executor.poisoned = False
    executor.has_idle_workers.return_value = False
    executor.submit.return_value.result.return_value = 'late-success'
    monkeypatch.setattr(broker, '_RECLAIM_CLAIM_BOUNDARY_DRAIN_TIMEOUT_SECONDS',
                        0)

    with pytest.raises(broker.request_process.AmbiguousBoundaryError) as info:
        broker._execute_claim_authorization_in_boundary(executor, str,
                                                        ('value',), 5)

    assert isinstance(info.value.__cause__,
                      broker.request_process.BoundaryExecutionError)
    executor._poison.assert_called_once_with(info.value)


def test_claim_boundary_requires_one_finite_lane():
    executor = mock.Mock()
    executor.max_workers = None

    with pytest.raises(ValueError, match='one finite owned lane'):
        broker._execute_claim_authorization_in_boundary(executor, str,
                                                        ('value',), 5)

    executor.submit.assert_not_called()


def test_claim_boundary_ambiguity_propagates_before_broker_lock(monkeypatch):
    _install_gate(monkeypatch, _gate(sequenced=True))
    ambiguity = broker.request_process.AmbiguousBoundaryError(
        'unproven claim family')
    monkeypatch.setattr(broker, '_authorize_reclaim_claim_set_in_boundary',
                        mock.Mock(side_effect=ambiguity))
    get_lock = mock.Mock()
    monkeypatch.setattr(broker.locks, 'get_lock', get_lock)

    with pytest.raises(broker.request_process.AmbiguousBoundaryError):
        _replace_claim_set(mock.Mock())

    get_lock.assert_not_called()
