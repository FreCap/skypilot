"""Tests for the Serve-owned durable Kueue admission callback."""
# pylint: disable=protected-access

import contextlib
import dataclasses
import datetime
import types
from unittest import mock
import uuid

import pytest

from sky.provision import common as provision_common
from sky.serve import constants as serve_constants
from sky.serve import kueue_lane_lineage
from sky.serve import kueue_lane_observer
from sky.serve import ordinary_launch_binding
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_reclaim_attestation

_INTENT_KEY = 'a' * 64
_WORKER_PROJECTION_SHA256 = 'b' * 64
_REPLICA_RECORD_ID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_READ_STARTED_AT = datetime.datetime(2026,
                                     8,
                                     21,
                                     12,
                                     tzinfo=datetime.timezone.utc)


def _fence() -> reserved_capacity.ProtocolV2LaunchFence:
    pool_key = reserved_capacity_broker.make_pool_key(
        'phx-context',
        'H200',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='physical-uid')
    return reserved_capacity.ProtocolV2LaunchFence(
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        pool_key=pool_key,
        service_generation=7,
        service_version=3,
        physical_cluster_uid='physical-uid',
        kubernetes_context='phx-context',
        accelerator='h200',
        accelerator_count=1,
        reconciliation_gate_generation=11,
        reclaim_fleet_bundle_sha256='c' * 64,
        reclaim_policy_revision='policy-v2',
        reclaim_provider_inventory_sha256='d' * 64,
        worker_projection_sha256=_WORKER_PROJECTION_SHA256)


def _authority() -> kueue_lane_observer._ObservationAuthority:
    return kueue_lane_observer._ObservationAuthority(
        service_name='svc',
        service_hash='service-hash',
        service_lifecycle_epoch=2,
        service_version=3,
        intent_key=_INTENT_KEY,
        replica_id=5,
        replica_record_id=_REPLICA_RECORD_ID,
        association_id=uuid.UUID('22222222-2222-4222-8222-222222222222'),
        provider_cluster_generation=4,
        fence=_fence())


def _identity(**overrides) -> provision_common.KueuePodAdmissionIdentity:
    values = {
        'intent_key': _INTENT_KEY,
        'replica_record_uuid': str(_REPLICA_RECORD_ID),
        'pool_physical_uid': 'physical-uid',
        'worker_projection_sha256': _WORKER_PROJECTION_SHA256,
    }
    values.update(overrides)
    return provision_common.KueuePodAdmissionIdentity(**values)


def _observation(
    state: provision_common.KueuePodAdmissionState,
) -> provision_common.KueuePodAdmissionObservation:
    admitted = state is provision_common.KueuePodAdmissionState.POLICY_ADMITTED
    receipt = provision_common.KueuePodAdmissionReceipt(
        state=state,
        namespace='inference',
        pod_name='svc-replica-head',
        pod_uid='33333333-3333-4333-8333-333333333333',
        pod_phase='Running' if admitted else 'Pending',
        scheduling_gates=(),
        cluster_name_on_cloud='svc-replica',
        kueue_managed_finalizer='kueue.x-k8s.io/managed',
        local_queue_name='skypilot',
        cluster_queue_name='skypilot',
        admission_local_queue_name='skypilot' if admitted else None,
        admission_cluster_queue_name='skypilot' if admitted else None,
        workload_priority_class_name='skypilot-low',
        pod_group_name='svc-replica-head',
        pod_group_total_count=1,
        role_hash='0123abcd',
        podset='0123abcd' if admitted else None,
        workload_name='svc-replica-head' if admitted else None,
        unconstrained_topology='true' if admitted else None,
        priority_class_name='skypilot-low',
        priority_value=-1000,
        preemption_policy='Never',
        scheduler_name='default-scheduler',
        service_account_name='inference-worker',
        accelerator='h200',
        accelerator_label_key='nvidia.com/gpu.product',
        accelerator_label_values=('NVIDIA-H200',),
        accelerator_resource_key='nvidia.com/gpu',
        accelerator_count=1,
        identity=_identity())
    return provision_common.KueuePodAdmissionObservation(receipt)


def _transaction_engine(events: list[str]):

    @contextlib.contextmanager
    def begin():
        events.append('transaction-enter')
        try:
            yield mock.sentinel.connection
        except BaseException:
            events.append('transaction-rollback')
            raise
        else:
            events.append('transaction-commit')

    return types.SimpleNamespace(begin=begin)


def test_observation_clock_token_is_sampled_on_a_closed_short_connection():
    events: list[str] = []
    connection = mock.Mock()
    connection.execute.return_value.scalar_one.return_value = _READ_STARTED_AT

    @contextlib.contextmanager
    def connect():
        events.append('clock-connect')
        try:
            yield connection
        finally:
            events.append('clock-close')

    observer = kueue_lane_observer._DurableKueuePodAdmissionObserver(
        _authority())
    with mock.patch.object(kueue_lane_observer.serve_state_schema,
                           'get_database_engine',
                           return_value=types.SimpleNamespace(connect=connect)):
        assert observer.begin_observation() == _READ_STARTED_AT

    assert events == ['clock-connect', 'clock-close']
    connection.execute.assert_called_once()


def test_admitted_receipt_commits_durable_state():
    events: list[str] = []
    authority = _authority()
    observation = _observation(
        provision_common.KueuePodAdmissionState.POLICY_ADMITTED)
    repository = mock.MagicMock()

    def persist(*_args, **_kwargs):
        assert events == ['transaction-enter']
        events.append('persist-admitted')

    repository.observe_policy_admitted_in_connection.side_effect = persist
    with mock.patch.object(
            kueue_lane_observer.serve_state_schema,
            'get_database_engine',
            return_value=_transaction_engine(events)), \
         mock.patch.object(
             kueue_lane_observer,
             '_lock_and_validate_materialization',
             return_value=(repository, mock.sentinel.identity,
                           mock.sentinel.admission)):
        kueue_lane_observer._observe(authority, observation, _READ_STARTED_AT)

    assert events == [
        'transaction-enter', 'persist-admitted', 'transaction-commit'
    ]
    repository.observe_policy_admitted_in_connection.assert_called_once()
    assert (repository.observe_policy_admitted_in_connection.call_args.
            kwargs['association_id'] == authority.association_id)
    assert (repository.observe_policy_admitted_in_connection.call_args.
            kwargs['provider_read_started_at'] == _READ_STARTED_AT)
    repository.observe_pod_waiting_in_connection.assert_not_called()


def test_waiting_receipt_commits_durable_state():
    events: list[str] = []
    authority = _authority()
    observation = _observation(
        provision_common.KueuePodAdmissionState.POD_WAITING)
    repository = mock.MagicMock()
    repository.observe_pod_waiting_in_connection.side_effect = (
        lambda *_args, **_kwargs: events.append('persist-waiting'))
    with mock.patch.object(
            kueue_lane_observer.serve_state_schema,
            'get_database_engine',
            return_value=_transaction_engine(events)), \
         mock.patch.object(
             kueue_lane_observer,
             '_lock_and_validate_materialization',
             return_value=(repository, mock.sentinel.identity,
                           mock.sentinel.admission)):
        kueue_lane_observer._observe(authority, observation, _READ_STARTED_AT)

    assert events == [
        'transaction-enter', 'persist-waiting', 'transaction-commit'
    ]
    repository.observe_pod_waiting_in_connection.assert_called_once()
    assert (repository.observe_pod_waiting_in_connection.call_args.
            kwargs['association_id'] == authority.association_id)
    assert (repository.observe_pod_waiting_in_connection.call_args.
            kwargs['provider_read_started_at'] == _READ_STARTED_AT)
    repository.observe_policy_admitted_in_connection.assert_not_called()


@pytest.mark.parametrize('mutation', ['identity', 'accelerator', 'count'])
def test_identity_or_shape_mismatch_fails_before_durable_state(mutation):
    authority = _authority()
    observation = _observation(
        provision_common.KueuePodAdmissionState.POLICY_ADMITTED)
    receipt = observation.receipt
    if mutation == 'identity':
        receipt = dataclasses.replace(receipt,
                                      identity=_identity(intent_key='e' * 64))
    elif mutation == 'accelerator':
        receipt = dataclasses.replace(receipt, accelerator='a100')
    else:
        receipt = dataclasses.replace(receipt, accelerator_count=2)
    observation = provision_common.KueuePodAdmissionObservation(receipt)

    with mock.patch.object(
            kueue_lane_observer.serve_state_schema,
            'get_database_engine') as get_database_engine, \
         mock.patch.object(
             kueue_lane_observer,
             '_lock_and_validate_materialization') as validate, \
         pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                       match='identity or shape'):
        kueue_lane_observer._observe(authority, observation, _READ_STARTED_AT)

    get_database_engine.assert_not_called()
    validate.assert_not_called()


@pytest.mark.parametrize(
    ('admission_mode', 'expects_runtime'),
    [(reserved_fill_reclaim_attestation.ReclaimAdmissionMode.KUEUE, True),
     (reserved_fill_reclaim_attestation.ReclaimAdmissionMode.
      KUBERNETES_SCHEDULER, False)])
def test_runtime_is_derived_only_for_projected_kueue_admission(
        admission_mode, expects_runtime):
    fence = _fence()
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
        authorization_reference=f'reserved-fill:{_INTENT_KEY}',
        authorization_generation=7,
        authorization_payload={'intent_key': _INTENT_KEY})
    bound = ordinary_launch_binding.BoundNonPoolLaunchContext(
        association_id=uuid.UUID('22222222-2222-4222-8222-222222222222'),
        request_id='request-id',
        service_name='svc',
        replica_id=5,
        replica_record_id=_REPLICA_RECORD_ID,
        launch_generation=4,
        input_digest='f' * 64,
        profile=profile,
        capability_cohort_epoch=1,
        capability_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))
    launch_context = {
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'service-hash',
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 3,
        ordinary_launch_binding.LIFECYCLE_EPOCH_KEY: 2,
        serve_constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [{}],
    }
    admission = types.SimpleNamespace(admission_mode=admission_mode)
    durable_admission = types.SimpleNamespace(
        state=kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
        pod_namespace='inference',
        pod_name='svc-replica-head',
        pod_uid='persisted-pod-uid')
    transaction_events: list[str] = []

    with mock.patch.object(
            kueue_lane_observer.ordinary_launch_binding,
            'parse_bound_non_pool_launch_context',
            return_value=bound), \
         mock.patch.object(
             kueue_lane_observer.reserved_capacity,
             'require_reclaim_worker_projection',
             return_value=({}, admission)), \
         mock.patch.object(
             kueue_lane_observer.serve_state_schema,
             'get_database_engine',
             return_value=_transaction_engine(transaction_events)), \
         mock.patch.object(
             kueue_lane_observer,
             '_lock_and_validate_materialization',
             return_value=(mock.sentinel.repository, mock.sentinel.identity,
                           durable_admission)) as validate:
        runtime = kueue_lane_observer.runtime_for_reserved_fill_launch(
            launch_context, fence)

    if not expects_runtime:
        assert runtime is None
        validate.assert_not_called()
        assert transaction_events == []
        return
    assert runtime is not None
    assert runtime.identity == _identity()
    assert runtime.accelerator == 'h200'
    assert runtime.persisted_pod_identity == (
        provision_common.KueuePersistedPodIdentity(namespace='inference',
                                                   pod_name='svc-replica-head',
                                                   pod_uid='persisted-pod-uid'))
    assert callable(runtime.observer)
    assert callable(runtime.observer.begin_observation)
    validate.assert_called_once()
    assert transaction_events == ['transaction-enter', 'transaction-commit']


def test_intent_pending_runtime_has_no_persisted_pod_identity():
    admission = types.SimpleNamespace(
        state=kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING,
        pod_namespace=None,
        pod_name=None,
        pod_uid=None)

    assert (
        kueue_lane_observer._persisted_pod_identity_from_admission(admission)
        is None)
