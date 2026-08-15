"""Characterization tests for SkyServe's versioned replica record."""
# pylint: disable=protected-access
import copy
import dataclasses
import logging
import pickle
from typing import Any
from unittest import mock

import pytest

from sky import backends
from sky import clouds
from sky import exceptions
from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.serve import replica_info
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import spot_placer
from sky.serve import system_recovery_state
from sky.utils import common_utils

_RECONCILIATION_GATE_GENERATION = 29
_RECLAIM_FLEET_BUNDLE_SHA256 = 'c' * 64
_RECLAIM_POLICY_REVISION = 'kueue-reclaim-v1'
_RECLAIM_PROVIDER_INVENTORY_SHA256 = 'd' * 64
_WORKER_PROJECTION_SHA256 = 'e' * 64
_RECLAIM_ATTRIBUTION_FIELDS = (
    'reserved_fill_reconciliation_gate_generation',
    'reserved_fill_reclaim_fleet_bundle_sha256',
    'reserved_fill_reclaim_policy_revision',
    'reserved_fill_reclaim_provider_inventory_sha256',
    'reserved_fill_worker_projection_sha256',
)
_LIVE_LEGACY_TOP_BASE = frozenset((
    'replica_info_version',
    'replica_id',
    'cluster_name',
    'version',
    'replica_port',
    'created_at',
    'first_not_ready_time',
    'first_consecutive_failure_time',
    'status_property',
    'is_spot',
    'location',
    'resources_override',
    'reserved_fill',
    'cost_rebalance_for_replica_id',
))
_LIVE_LEGACY_TOP_CAPACITY = frozenset((
    'planned_capacity',
    'unknown_capacity_replacement',
    'logical_bridge_capacity_verified',
))
_LIVE_LEGACY_TOP_ECONOMIC = frozenset((
    'is_zero_cost',
    'paid_capacity_pool_key',
))
_LIVE_LEGACY_TOP_RECOVERY = frozenset((
    'replica_record_id',
    'system_recovery_launch_intent',
    'system_recovery_disposition',
    'launch_request_id',
    'service_job_id',
    'candidate_ready_observed_at',
    'ordinary_release_not_before',
    'system_recovery_revision',
    'system_recovery',
    'system_recovery_quarantine',
))
_LIVE_LEGACY_TOP_FILL_IDENTITY_V13 = frozenset((
    'reserved_fill_pool_key',
    'reserved_fill_service_generation',
    'reserved_fill_physical_cluster_uid',
))
_LIVE_LEGACY_TOP_FILL_IDENTITY_V14 = frozenset((
    *_LIVE_LEGACY_TOP_FILL_IDENTITY_V13,
    'reserved_fill_kubernetes_context',
))
_LIVE_LEGACY_STATUS_BASE = frozenset((
    'sky_launch_status',
    'user_app_failed',
    'service_ready_now',
    'first_ready_time',
    'sky_down_status',
    'is_scale_down',
    'preempted',
    'purged',
    'failed_spot_availability',
    'drain_cap_seconds',
    'wait_for_idle_before_termination',
))
_LIVE_LEGACY_STATUS_CURRENT = frozenset((
    *_LIVE_LEGACY_STATUS_BASE,
    'drain_started_at',
    'logical_retirement_version',
    'logical_retirement_controller_epoch',
    'logical_retirement_generation',
    'logical_retirement_target_capacity',
    'logical_retirement_confirmed_generation',
    'logical_retirement_bounded_deadline',
    'logical_retirement_committed',
))
_LIVE_LEGACY_TOP_CAPACITY_SHAPE = (_LIVE_LEGACY_TOP_BASE |
                                   _LIVE_LEGACY_TOP_CAPACITY)
_LIVE_LEGACY_TOP_ECONOMIC_SHAPE = (_LIVE_LEGACY_TOP_CAPACITY_SHAPE |
                                   _LIVE_LEGACY_TOP_ECONOMIC)
_LIVE_LEGACY_TOP_RECOVERY_SHAPE = (_LIVE_LEGACY_TOP_ECONOMIC_SHAPE |
                                   _LIVE_LEGACY_TOP_RECOVERY)
_LIVE_LEGACY_CENSUS_CASES = (
    pytest.param(3,
                 _LIVE_LEGACY_TOP_BASE,
                 _LIVE_LEGACY_STATUS_BASE,
                 id='v3-base'),
    pytest.param(6,
                 _LIVE_LEGACY_TOP_CAPACITY_SHAPE,
                 _LIVE_LEGACY_STATUS_CURRENT,
                 id='v6-capacity'),
    pytest.param(7,
                 _LIVE_LEGACY_TOP_BASE,
                 _LIVE_LEGACY_STATUS_BASE,
                 id='v7-base'),
    pytest.param(12,
                 _LIVE_LEGACY_TOP_ECONOMIC_SHAPE,
                 _LIVE_LEGACY_STATUS_CURRENT,
                 id='v12-economic'),
    pytest.param(13,
                 _LIVE_LEGACY_TOP_RECOVERY_SHAPE,
                 _LIVE_LEGACY_STATUS_CURRENT,
                 id='v13-no-fill-identity'),
    pytest.param(13,
                 _LIVE_LEGACY_TOP_RECOVERY_SHAPE |
                 _LIVE_LEGACY_TOP_FILL_IDENTITY_V13,
                 _LIVE_LEGACY_STATUS_CURRENT,
                 id='v13-location-derived-context'),
    pytest.param(13,
                 _LIVE_LEGACY_TOP_RECOVERY_SHAPE |
                 _LIVE_LEGACY_TOP_FILL_IDENTITY_V14,
                 _LIVE_LEGACY_STATUS_CURRENT,
                 id='v13-explicit-context'),
    pytest.param(14,
                 _LIVE_LEGACY_TOP_RECOVERY_SHAPE |
                 _LIVE_LEGACY_TOP_FILL_IDENTITY_V14,
                 _LIVE_LEGACY_STATUS_CURRENT,
                 id='v14-explicit-context'),
)


def _replica() -> replica_managers.ReplicaInfo:
    location = spot_placer.Location(cloud=clouds.AWS(),
                                    region='us-east-1',
                                    zone='us-east-1a')
    replica = replica_managers.ReplicaInfo(
        replica_id=7,
        cluster_name='svc-7',
        replica_port='8080',
        is_spot=True,
        location=location,
        version=3,
        resources_override={
            'cloud': clouds.AWS(),
            'region': 'us-east-1',
            'image_id': {
                None: 'global-image',
                'us-east-1': 'regional-image',
            },
        },
        planned_capacity=4,
        unknown_capacity_replacement=True)
    replica.created_at = 100.0
    replica.first_not_ready_time = 200.0
    replica.first_consecutive_failure_time = 210.0
    replica.logical_bridge_capacity_verified = True
    replica.reserved_fill = True
    replica.is_zero_cost = True
    replica.cost_rebalance_for_replica_id = 2
    replica.paid_capacity_pool_key = 'aws|us-east-1|a100'
    replica.status_property = replica_managers.ReplicaStatusProperty(
        sky_launch_status=common_utils.ProcessStatus.SUCCEEDED,
        service_ready_now=True,
        first_ready_time=150.0,
        drain_cap_seconds=120,
        drain_started_at=300.0,
        wait_for_idle_before_termination=True,
        logical_retirement_version=3,
        logical_retirement_controller_epoch='owner-a',
        logical_retirement_generation=11,
        logical_retirement_target_capacity=8,
        logical_retirement_confirmed_generation=10,
        logical_retirement_bounded_deadline=True,
        logical_retirement_committed=True)
    return replica


def _protocol_v2_replica() -> replica_managers.ReplicaInfo:
    replica = _replica()
    context = 'phx-context'
    physical_uid = 'physical-uid'
    replica.location = spot_placer.Location(cloud=clouds.Kubernetes(),
                                            region=context,
                                            zone=None,
                                            accelerators={
                                                'H200': 1
                                            },
                                            use_spot=False).to_pickleable()
    replica.resources_override = {
        'cloud': clouds.Kubernetes(),
        'region': context,
        'accelerators': {
            'H200': 1,
        },
    }
    replica.reserved_fill_pool_key = reserved_capacity_broker.make_pool_key(
        context,
        'H200',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=physical_uid)
    replica.reserved_fill_service_generation = 7
    replica.reserved_fill_physical_cluster_uid = physical_uid
    replica.reserved_fill_kubernetes_context = context
    return replica


def _attributed_protocol_v2_replica() -> replica_managers.ReplicaInfo:
    replica = _protocol_v2_replica()
    replica.reserved_fill_allocation_generation = 5
    replica.reserved_fill_allocation_input_sha256 = 'a' * 64
    replica.reserved_fill_allocation_claim_generation = 11
    replica.reserved_fill_reconciliation_gate_generation = (
        _RECONCILIATION_GATE_GENERATION)
    replica.reserved_fill_reclaim_fleet_bundle_sha256 = (
        _RECLAIM_FLEET_BUNDLE_SHA256)
    replica.reserved_fill_reclaim_policy_revision = _RECLAIM_POLICY_REVISION
    replica.reserved_fill_reclaim_provider_inventory_sha256 = (
        _RECLAIM_PROVIDER_INVENTORY_SHA256)
    replica.reserved_fill_worker_projection_sha256 = (_WORKER_PROJECTION_SHA256)
    replica.reserved_fill_observation_generation = 13
    replica.reserved_fill_observation_sequence = 17
    replica.reserved_fill_intent_idempotency_key = 'b' * 64
    replica.zero_cost_admission_sequence = 19
    replica.zero_cost_materialization_sequence = 13
    return replica


def _live_legacy_state(version: int, top_fields: frozenset[str],
                       status_fields: frozenset[str]) -> dict[str, Any]:
    """Build one value-bearing state with an exact live-census key shape."""
    state = _protocol_v2_replica().to_storage_dict()
    state['replica_info_version'] = version
    state['status_property'] = {
        field: state['status_property'][field] for field in status_fields
    }
    return {field: copy.deepcopy(state[field]) for field in top_fields}


def _golden_v7_state() -> dict[str, Any]:
    """Literal v1.1.1276 fixture, independent of the current serializer."""
    return {
        'replica_info_version': 7,
        'replica_id': '41',
        'cluster_name': 9001,
        'version': '5',
        'replica_port': 8081,
        'created_at': 101.25,
        'first_not_ready_time': None,
        'first_consecutive_failure_time': 109.5,
        'status_property': {
            'sky_launch_status': 'SUCCEEDED',
            'user_app_failed': 0,
            'service_ready_now': 1,
            'first_ready_time': 105.0,
            'sky_down_status': None,
            'is_scale_down': 0,
            'preempted': False,
            'purged': False,
            'failed_spot_availability': 0,
            'drain_cap_seconds': 45,
            'wait_for_idle_before_termination': 1,
        },
        'is_spot': 0,
        'location': {
            'cloud': 'Kubernetes',
            'region': 'phx-context',
            'zone': None,
            'accelerators': {
                'H200': 1,
            },
            'use_spot': False,
            'image_id': None,
            'container_image': None,
            'disk_tier': None,
            'ephemeral_storage': None,
            'instance_type': None,
        },
        'resources_override': {
            'cloud': 'Kubernetes',
            'region': 'phx-context',
            'accelerators': {
                'H200': 1,
            },
            'image_id': {
                'null': 'global-image',
                'phx-context': 'regional-image',
            },
        },
        'reserved_fill': True,
        'cost_rebalance_for_replica_id': 2,
    }


def _golden_v13_i3_state() -> dict[str, Any]:
    """Literal value-bearing v13/I3 fixture with the complete recovery R."""
    state = _golden_v7_state()
    state['replica_info_version'] = 13
    state.update({
        'planned_capacity': 4,
        'unknown_capacity_replacement': True,
        'logical_bridge_capacity_verified': True,
        'is_zero_cost': True,
        'paid_capacity_pool_key': 'paid-pool',
        'replica_record_id': '12345678-1234-5678-9234-567812345678',
        'system_recovery_launch_intent': None,
        'system_recovery_disposition': 'ORDINARY',
        'launch_request_id': None,
        'service_job_id': None,
        'candidate_ready_observed_at': None,
        'ordinary_release_not_before': None,
        'system_recovery_revision': 0,
        'system_recovery': None,
        'system_recovery_quarantine': None,
        'reserved_fill_pool_key': '["v2", "physical-uid", "h200"]',
        'reserved_fill_service_generation': 7,
        'reserved_fill_physical_cluster_uid': 'physical-uid',
    })
    status = state['status_property']
    status.update({
        'drain_started_at': 0.0,
        'logical_retirement_version': 4,
        'logical_retirement_controller_epoch': 'controller-epoch',
        'logical_retirement_generation': 11,
        'logical_retirement_target_capacity': 5,
        'logical_retirement_confirmed_generation': 10,
        'logical_retirement_bounded_deadline': 1,
        'logical_retirement_committed': 'true',
    })
    return state


def _protocol_v2_handle(
        context: str = 'phx-context') -> backends.CloudVmRayResourceHandle:
    handle = mock.Mock(spec=backends.CloudVmRayResourceHandle)
    handle.cluster_name = 'svc-7'
    handle.launched_resources = mock.Mock(cloud=clouds.Kubernetes(),
                                          region=context)
    return handle


def _status_field_names() -> tuple[str, ...]:
    return tuple(
        field.name
        for field in dataclasses.fields(replica_managers.ReplicaStatusProperty))


def _assert_materialized_legacy_status_defaults(
        status: replica_managers.ReplicaStatusProperty) -> None:
    expected = dict(vars(replica_managers.ReplicaStatusProperty()))
    expected['logical_retirement_committed'] = None
    assert vars(status) == expected


def test_constructor_owns_complete_explicit_interface():
    replica = _replica()

    assert set(vars(replica)) == {
        '_version', *replica_info._REPLICA_INFO_OWNED_FIELDS
    }
    assert set(vars(replica.status_property)) == set(_status_field_names())


@pytest.mark.parametrize(('updates', 'expected'), [
    ({}, serve_state.ReplicaStatus.PENDING),
    ({
        'sky_launch_status': common_utils.ProcessStatus.RUNNING
    }, serve_state.ReplicaStatus.PROVISIONING),
    ({
        'sky_launch_status': common_utils.ProcessStatus.SUCCEEDED,
        'service_ready_now': True,
        'first_ready_time': 10.0,
    }, serve_state.ReplicaStatus.READY),
    ({
        'sky_launch_status': common_utils.ProcessStatus.SUCCEEDED,
        'first_ready_time': 10.0,
    }, serve_state.ReplicaStatus.NOT_READY),
    ({
        'sky_launch_status': common_utils.ProcessStatus.SUCCEEDED,
        'sky_down_status': common_utils.ProcessStatus.RUNNING,
    }, serve_state.ReplicaStatus.SHUTTING_DOWN),
    ({
        'sky_launch_status': common_utils.ProcessStatus.SUCCEEDED,
        'sky_down_status': common_utils.ProcessStatus.FAILED,
    }, serve_state.ReplicaStatus.FAILED_CLEANUP),
])
def test_status_projection_contract(updates, expected):
    status = replica_managers.ReplicaStatusProperty()
    for name, value in updates.items():
        setattr(status, name, value)
    assert status.to_replica_status() is expected


def test_replica_status_projection_is_side_effect_free(caplog):
    sentinel_id = 987654321
    sentinel_payload = 'status-projection-payload-sentinel'
    replica = _replica()
    replica.replica_id = sentinel_id
    replica.cluster_name = sentinel_payload
    replica.status_property.sky_launch_status = (
        common_utils.ProcessStatus.RUNNING)
    replica.status_property.sky_down_status = (
        common_utils.ProcessStatus.SUCCEEDED)

    with caplog.at_level(logging.ERROR):
        assert replica.status is serve_state.ReplicaStatus.UNKNOWN

    assert str(sentinel_id) not in caplog.text
    assert sentinel_payload not in caplog.text
    assert not caplog.records


def test_storage_round_trip_is_lossless_and_does_not_mutate_source():
    replica = _replica()
    assert replica.resources_override is not None
    cloud_before = replica.resources_override['cloud']
    image_id_before = copy.deepcopy(replica.resources_override['image_id'])

    state = replica.to_storage_dict()
    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    assert state['replica_info_version'] == 18
    assert set(state) == set(replica_info._REPLICA_INFO_STORAGE_FIELDS)
    assert replica.resources_override['cloud'] is cloud_before
    assert replica.resources_override['image_id'] == image_id_before
    assert restored.to_storage_dict() == state
    assert restored.location == replica.location
    assert restored.resources_override['image_id'] == {
        None: 'global-image',
        'us-east-1': 'regional-image',
    }
    assert restored.status is serve_state.ReplicaStatus.READY


def test_legacy_pickle_migration_materializes_zero_cost_provenance():
    legacy_state = dict(vars(_replica()))
    legacy_state['_version'] = 10
    legacy_state.pop('is_zero_cost')
    restored = replica_managers.ReplicaInfo.__new__(
        replica_managers.ReplicaInfo)

    restored.__setstate__(legacy_state)

    assert vars(restored)['is_zero_cost'] is False


def test_legacy_pickle_migration_materializes_logical_capacity_fields():
    legacy_state = copy.deepcopy(vars(_replica()))
    legacy_state['_version'] = 8
    legacy_state.pop('unknown_capacity_replacement')
    legacy_state.pop('logical_bridge_capacity_verified')
    restored = replica_managers.ReplicaInfo.__new__(
        replica_managers.ReplicaInfo)

    restored.__setstate__(legacy_state)

    assert vars(restored)['unknown_capacity_replacement'] is False
    assert vars(restored)['logical_bridge_capacity_verified'] is False


def test_legacy_pickle_migration_materializes_every_status_field():
    legacy_state = copy.deepcopy(vars(_replica()))
    legacy_state['_version'] = 13
    vars(legacy_state['status_property']).clear()
    restored = replica_managers.ReplicaInfo.__new__(
        replica_managers.ReplicaInfo)

    restored.__setstate__(legacy_state)

    assert set(vars(restored.status_property)) == set(_status_field_names())
    _assert_materialized_legacy_status_defaults(restored.status_property)


def test_current_record_requires_explicit_zero_cost_provenance():
    replica = _replica()
    del replica.is_zero_cost

    with pytest.raises(AttributeError, match='is_zero_cost'):
        replica.to_storage_dict()


@pytest.mark.parametrize('field', replica_info._REPLICA_INFO_OWNED_FIELDS)
def test_current_record_requires_complete_owned_interface(field):
    replica = _replica()
    delattr(replica, field)

    with pytest.raises(AttributeError, match=field):
        replica.to_storage_dict()


@pytest.mark.parametrize('field', _status_field_names())
def test_current_record_requires_every_status_field(field):
    replica = _replica()
    vars(replica.status_property).pop(field)

    with pytest.raises(AttributeError, match=field):
        replica.to_storage_dict()


@pytest.mark.parametrize('field', replica_info._REPLICA_INFO_OWNED_FIELDS)
def test_current_pickle_requires_complete_owned_interface(field):
    current_state = copy.deepcopy(vars(_replica()))
    current_state.pop(field)
    restored = replica_managers.ReplicaInfo.__new__(
        replica_managers.ReplicaInfo)

    with pytest.raises(AttributeError, match=field):
        restored.__setstate__(current_state)


@pytest.mark.parametrize('marker', [0, 1, 'yes'])
def test_storage_rejects_non_boolean_reserved_fill_marker(marker):
    replica = _replica()
    setattr(replica, 'reserved_fill', marker)
    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='must be a boolean'):
        replica.to_storage_dict()

    state = _replica().to_storage_dict()
    state['reserved_fill'] = marker
    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='must be a boolean'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


def test_protocol_v2_storage_round_trip_preserves_strict_cleanup_authority():
    state = _protocol_v2_replica().to_storage_dict()
    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    fence = reserved_capacity.parse_protocol_v2_cleanup_fence(restored)
    assert fence == reserved_capacity.ProtocolV2CleanupFence(
        kubernetes_context='phx-context', physical_cluster_uid='physical-uid')


def test_typed_fill_allocation_attribution_round_trips_json_and_pickle():
    replica = _attributed_protocol_v2_replica()
    state = replica.to_storage_dict()

    from_json = replica_managers.ReplicaInfo.from_storage_dict(state)
    from_pickle = pickle.loads(pickle.dumps(replica, protocol=5))

    expected = {
        'reserved_fill_allocation_generation': 5,
        'reserved_fill_allocation_input_sha256': 'a' * 64,
        'reserved_fill_allocation_claim_generation': 11,
        'reserved_fill_reconciliation_gate_generation': _RECONCILIATION_GATE_GENERATION,
        'reserved_fill_reclaim_fleet_bundle_sha256': _RECLAIM_FLEET_BUNDLE_SHA256,
        'reserved_fill_reclaim_policy_revision': _RECLAIM_POLICY_REVISION,
        'reserved_fill_reclaim_provider_inventory_sha256': _RECLAIM_PROVIDER_INVENTORY_SHA256,
        'reserved_fill_worker_projection_sha256': _WORKER_PROJECTION_SHA256,
        'reserved_fill_observation_generation': 13,
        'reserved_fill_observation_sequence': 17,
        'reserved_fill_intent_idempotency_key': 'b' * 64,
        'zero_cost_admission_sequence': 19,
        'zero_cost_materialization_sequence': 13,
    }
    assert {field: state[field] for field in expected} == expected
    assert {field: getattr(from_json, field) for field in expected} == expected
    assert {field: getattr(from_pickle, field) for field in expected} == expected


def test_first_typed_fill_round_trips_zero_observation_sequence():
    replica = _attributed_protocol_v2_replica()
    replica.reserved_fill_observation_sequence = 0

    state = replica.to_storage_dict()
    from_json = replica_managers.ReplicaInfo.from_storage_dict(state)
    from_pickle = pickle.loads(pickle.dumps(replica, protocol=5))

    assert state['reserved_fill_observation_sequence'] == 0
    assert from_json.reserved_fill_observation_sequence == 0
    assert from_pickle.reserved_fill_observation_sequence == 0


def test_v17_collision_materializes_absent_allocation_attribution():
    state = _protocol_v2_replica().to_storage_dict()
    state['replica_info_version'] = 17
    for field in replica_info.V17_COLLISION_OPTIONAL_STORAGE_FIELDS:
        state.pop(field)

    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    assert all(
        getattr(restored, field) is None
        for field in replica_info.V17_COLLISION_OPTIONAL_STORAGE_FIELDS)
    assert restored.to_storage_dict()['replica_info_version'] == 18


def test_complete_v17_shape_preserves_all_attribution():
    state = _attributed_protocol_v2_replica().to_storage_dict()
    state['replica_info_version'] = 17

    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    assert restored.reserved_fill_allocation_generation == 5
    assert restored.reserved_fill_allocation_input_sha256 == 'a' * 64
    assert restored.reserved_fill_allocation_claim_generation == 11
    assert restored.reserved_fill_observation_generation == 13
    assert restored.reserved_fill_observation_sequence == 17
    assert restored.reserved_fill_intent_idempotency_key == 'b' * 64
    assert restored.reserved_fill_reconciliation_gate_generation == 29
    assert (restored.reserved_fill_reclaim_fleet_bundle_sha256 ==
            _RECLAIM_FLEET_BUNDLE_SHA256)

    rewritten = restored.to_storage_dict()
    assert rewritten['replica_info_version'] == 18
    decoded_again = replica_managers.ReplicaInfo.from_storage_dict(rewritten)
    assert decoded_again.reserved_fill_allocation_generation == 5
    assert (decoded_again.reserved_fill_reclaim_policy_revision ==
            _RECLAIM_POLICY_REVISION)


def test_v17_collision_rejects_partially_missing_attribution():
    state = _attributed_protocol_v2_replica().to_storage_dict()
    state['replica_info_version'] = 17
    state.pop('reserved_fill_reclaim_policy_revision')

    with pytest.raises(ValueError, match='partially missing attribution'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize(
    ('field', 'value', 'message'),
    [
        ('reserved_fill_allocation_generation', True, 'positive integer'),
        ('reserved_fill_allocation_input_sha256', 'A' * 64, 'SHA-256'),
        ('reserved_fill_allocation_claim_generation', 0, 'positive integer'),
        ('reserved_fill_reconciliation_gate_generation', True,
         'positive integer'),
        ('reserved_fill_reclaim_fleet_bundle_sha256', 'C' * 64, 'SHA-256'),
        ('reserved_fill_reclaim_policy_revision', '', 'nonempty text'),
        ('reserved_fill_reclaim_provider_inventory_sha256', 'd' * 63,
         'SHA-256'),
        ('reserved_fill_worker_projection_sha256', 'E' * 64, 'SHA-256'),
        ('reserved_fill_observation_generation', 1.0, 'positive integer'),
        ('reserved_fill_observation_sequence', -1, 'nonnegative integer'),
        ('reserved_fill_intent_idempotency_key', None, 'must be complete'),
    ],
)
def test_typed_fill_allocation_attribution_rejects_malformed_fields(
        field, value, message):
    state = _attributed_protocol_v2_replica().to_storage_dict()
    state[field] = value

    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match=message):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize('field', _RECLAIM_ATTRIBUTION_FIELDS)
def test_typed_fill_reclaim_identity_rejects_partial_bundle(field):
    state = _attributed_protocol_v2_replica().to_storage_dict()
    state[field] = None

    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='must be complete'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


def test_non_fill_row_cannot_claim_allocation_attribution():
    replica = _attributed_protocol_v2_replica()
    replica.reserved_fill = False

    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='requires a fill row'):
        replica.to_storage_dict()


def test_zero_cost_admission_sequence_supports_ordinary_rows_and_is_typed():
    ordinary = _replica()
    ordinary.reserved_fill = False
    ordinary.zero_cost_admission_sequence = 18
    restored = replica_managers.ReplicaInfo.from_storage_dict(
        ordinary.to_storage_dict())
    assert restored.zero_cost_admission_sequence == 18

    legacy = _protocol_v2_replica()
    legacy.zero_cost_admission_sequence = 19
    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='requires complete'):
        legacy.to_storage_dict()

    attributed = _attributed_protocol_v2_replica()
    attributed.zero_cost_admission_sequence = True
    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='positive integer'):
        attributed.to_storage_dict()

    paid = _replica()
    paid.reserved_fill = False
    paid.is_zero_cost = False
    paid.zero_cost_admission_sequence = 20
    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='requires a zero-cost row'):
        paid.to_storage_dict()


@pytest.mark.parametrize('terminal_launch_status', [
    common_utils.ProcessStatus.INTERRUPTED,
    common_utils.ProcessStatus.FAILED,
])
def test_zero_cost_materialization_sequence_survives_terminal_cleanup_status(
        terminal_launch_status):
    ordinary = _replica()
    ordinary.reserved_fill = False
    ordinary.zero_cost_admission_sequence = 18
    ordinary.zero_cost_materialization_sequence = 7
    ordinary.status_property.sky_launch_status = terminal_launch_status

    restored = replica_managers.ReplicaInfo.from_storage_dict(
        ordinary.to_storage_dict())

    assert restored.status_property.sky_launch_status == terminal_launch_status
    assert restored.zero_cost_materialization_sequence == 7


def test_deserialized_malformed_v2_tuple_fails_closed_at_cleanup_parser():
    state = _protocol_v2_replica().to_storage_dict()
    state['resources_override']['region'] = 'replacement-context'
    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='resource pin is malformed'):
        reserved_capacity.parse_protocol_v2_cleanup_fence(restored)


def test_protocol_v2_endpoint_resolution_enters_exact_physical_fence():
    replica = _protocol_v2_replica()
    handle = _protocol_v2_handle()
    cluster_record = {'name': 'svc-7', 'handle': handle}
    uid_fence = mock.MagicMock()
    uid_fence.return_value.__enter__.return_value = None

    with mock.patch.object(kubernetes_adaptor,
                           'physical_cluster_uid_fence', uid_fence), \
         mock.patch.object(replica_managers.backend_utils,
                           'get_endpoints',
                           return_value={8080: '10.0.0.1:8080'}) as endpoint:
        assert replica._resolve_url(  # pylint: disable=protected-access
            cluster_record=cluster_record,
            handle=handle) == ('http://10.0.0.1:8080')

    uid_fence.assert_called_once_with('phx-context', 'physical-uid')
    endpoint.assert_called_once_with('svc-7',
                                     8080,
                                     cluster_record=cluster_record)


def test_protocol_v2_endpoint_uid_mismatch_precedes_provider_call():
    replica = _protocol_v2_replica()
    handle = _protocol_v2_handle()
    cluster_record = {'name': 'svc-7', 'handle': handle}
    uid_fence = mock.MagicMock()
    uid_fence.return_value.__enter__.side_effect = (
        exceptions.KubernetesPhysicalClusterIdentityError('UID mismatch'))

    with mock.patch.object(kubernetes_adaptor,
                           'physical_cluster_uid_fence', uid_fence), \
         mock.patch.object(replica_managers.backend_utils,
                           'get_endpoints') as endpoint, \
         pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='UID mismatch'):
        replica._resolve_url(  # pylint: disable=protected-access
            cluster_record=cluster_record,
            handle=handle)

    endpoint.assert_not_called()


def test_protocol_v2_endpoint_rejects_retargeted_handle_before_uid_lookup():
    replica = _protocol_v2_replica()
    handle = _protocol_v2_handle(context='replacement-context')
    cluster_record = {'name': 'svc-7', 'handle': handle}

    with mock.patch.object(
            kubernetes_adaptor,
            'physical_cluster_uid_fence') as uid_fence, \
         mock.patch.object(replica_managers.backend_utils,
                           'get_endpoints') as endpoint, \
         pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='durable replica handle'):
        replica._resolve_url(  # pylint: disable=protected-access
            cluster_record=cluster_record,
            handle=handle)

    uid_fence.assert_not_called()
    endpoint.assert_not_called()


def test_pool_probe_propagates_explicit_provider_phase_admission():
    replica = _protocol_v2_replica()
    handle = _protocol_v2_handle()
    backend = mock.Mock()
    backend.get_job_status.return_value = {
        1: replica_info.job_lib.JobStatus.SUCCEEDED
    }
    provider_fence = mock.MagicMock()
    provider_fence.return_value.__enter__.return_value = None
    admission = mock.sentinel.provider_phase_admission

    with mock.patch.object(replica_info.global_user_state,
                           'get_handle_from_cluster_name',
                           return_value=handle), \
         mock.patch.object(replica_info.backend_utils,
                           'check_cluster_available',
                           return_value=handle), \
         mock.patch.object(replica_info.backend_utils,
                           'get_backend_from_handle',
                           return_value=backend), \
         mock.patch.object(reserved_capacity,
                           'protocol_v2_provider_fence',
                           provider_fence):
        _, ready, _ = replica.probe_pool(provider_phase_admission=admission)

    assert ready
    assert provider_fence.call_args_list == [
        mock.call(replica, handle, phase_admission=admission),
        mock.call(replica, handle, phase_admission=admission),
    ]


def test_golden_v7_reader_pins_v1_1_1276_materialization():
    state = _golden_v7_state()

    restored = replica_managers.ReplicaInfo.from_storage_dict(state)
    canonical = restored.to_storage_dict()

    assert {
        field: canonical[field]
        for field in ('replica_info_version', 'replica_id', 'cluster_name',
                      'version', 'replica_port', 'created_at', 'is_spot',
                      'planned_capacity', 'unknown_capacity_replacement',
                      'logical_bridge_capacity_verified', 'reserved_fill',
                      'is_zero_cost', 'cost_rebalance_for_replica_id',
                      'paid_capacity_pool_key', 'replica_record_id')
    } == {
        'replica_info_version': 18,
        'replica_id': 41,
        'cluster_name': '9001',
        'version': 5,
        'replica_port': '8081',
        'created_at': 101.25,
        'is_spot': False,
        'planned_capacity': 1,
        'unknown_capacity_replacement': False,
        'logical_bridge_capacity_verified': False,
        'reserved_fill': True,
        'is_zero_cost': False,
        'cost_rebalance_for_replica_id': 2,
        'paid_capacity_pool_key': None,
        'replica_record_id': '6b04d5d4-2013-50e8-b67e-d18f5875a5e8',
    }
    assert canonical['resources_override'] == {
        'cloud': 'Kubernetes',
        'region': 'phx-context',
        'accelerators': {
            'H200': 1,
        },
        'image_id': [[None, 'global-image'], ['phx-context', 'regional-image']],
    }
    assert canonical['status_property'] == {
        'sky_launch_status': 'SUCCEEDED',
        'user_app_failed': False,
        'service_ready_now': True,
        'first_ready_time': 105.0,
        'sky_down_status': None,
        'is_scale_down': False,
        'preempted': False,
        'purged': False,
        'failed_spot_availability': False,
        'drain_cap_seconds': 45,
        'drain_started_at': None,
        'wait_for_idle_before_termination': True,
        'logical_retirement_version': None,
        'logical_retirement_controller_epoch': None,
        'logical_retirement_generation': None,
        'logical_retirement_target_capacity': None,
        'logical_retirement_confirmed_generation': None,
        'logical_retirement_bounded_deadline': False,
        'logical_retirement_committed': None,
    }
    assert {
        field: canonical[field]
        for field in replica_info.V13_ADDITIVE_STORAGE_FIELDS
    } == {
        'replica_record_id': '6b04d5d4-2013-50e8-b67e-d18f5875a5e8',
        'system_recovery_launch_intent': None,
        'system_recovery_disposition': 'ORDINARY',
        'launch_request_id': None,
        'service_job_id': None,
        'candidate_ready_observed_at': None,
        'ordinary_release_not_before': None,
        'system_recovery_revision': 0,
        'system_recovery': None,
        'system_recovery_quarantine': None,
    }
    assert all(canonical[field] is None
               for field in replica_info.V17_COLLISION_OPTIONAL_STORAGE_FIELDS)


def test_golden_v13_i3_reader_pins_recovery_and_status_semantics():
    state = _golden_v13_i3_state()

    restored = replica_managers.ReplicaInfo.from_storage_dict(state)
    canonical = restored.to_storage_dict()

    assert canonical['replica_info_version'] == 18
    assert canonical['replica_record_id'] == (
        '12345678-1234-5678-9234-567812345678')
    assert {
        field: canonical[field]
        for field in replica_info.SYSTEM_RECOVERY_STORAGE_FIELDS
    } == {
        'system_recovery_launch_intent': None,
        'system_recovery_disposition': 'ORDINARY',
        'launch_request_id': None,
        'service_job_id': None,
        'candidate_ready_observed_at': None,
        'ordinary_release_not_before': None,
        'system_recovery_revision': 0,
        'system_recovery': None,
        'system_recovery_quarantine': None,
    }
    assert canonical['reserved_fill'] is True
    assert canonical['is_zero_cost'] is True
    assert canonical['reserved_fill_pool_key'] == (
        '["v2", "physical-uid", "h200"]')
    assert canonical['reserved_fill_service_generation'] == 7
    assert canonical['reserved_fill_physical_cluster_uid'] == 'physical-uid'
    assert canonical['reserved_fill_kubernetes_context'] is None
    assert canonical['status_property']['drain_started_at'] is None
    assert canonical['status_property'][
        'logical_retirement_bounded_deadline'] is False
    assert canonical['status_property']['logical_retirement_committed'] is None
    assert reserved_capacity.parse_protocol_v2_cleanup_fence(
        restored) == reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='phx-context',
            physical_cluster_uid='physical-uid')
    assert all(canonical[field] is None
               for field in replica_info.V17_COLLISION_OPTIONAL_STORAGE_FIELDS)


@pytest.mark.parametrize(('version', 'top_fields', 'status_fields'),
                         _LIVE_LEGACY_CENSUS_CASES)
def test_live_legacy_json_shapes_materialize_canonical_v18(
        version, top_fields, status_fields):
    state = _live_legacy_state(version, top_fields, status_fields)

    restored = replica_managers.ReplicaInfo.from_storage_dict(state)
    restored_again = replica_managers.ReplicaInfo.from_storage_dict(state)
    rewritten = restored.to_storage_dict()

    assert restored._version == 18
    assert set(vars(restored)) == {
        '_version', *replica_info._REPLICA_INFO_OWNED_FIELDS
    }
    assert set(vars(restored.status_property)) == set(_status_field_names())
    assert rewritten['replica_info_version'] == 18
    assert set(rewritten) == set(replica_info._REPLICA_INFO_STORAGE_FIELDS)
    assert set(rewritten['status_property']) == set(_status_field_names())
    for field in top_fields - {'replica_info_version', 'status_property'}:
        assert rewritten[field] == state[field]
    for field in status_fields:
        assert rewritten['status_property'][field] == state['status_property'][
            field]
    for field, default in replica_info._REPLICA_INFO_LEGACY_DEFAULTS.items():
        if field not in top_fields:
            assert getattr(restored, field) == default
    for field, default in (
            replica_info._REPLICA_STATUS_PROPERTY_LEGACY_DEFAULTS.items()):
        if field not in status_fields:
            assert getattr(restored.status_property, field) == default
    for field in replica_info.V17_COLLISION_OPTIONAL_STORAGE_FIELDS:
        assert getattr(restored, field) is None

    if version < 13:
        assert (restored.system_recovery_disposition ==
                system_recovery_state.SystemRecoveryDisposition.ORDINARY)
        assert restored.replica_record_id == restored_again.replica_record_id
    else:
        assert restored.replica_record_id == state['replica_record_id']


@pytest.mark.parametrize(
    ('version', 'top_fields', 'expected_context'),
    [
        pytest.param(13,
                     _LIVE_LEGACY_TOP_RECOVERY_SHAPE,
                     None,
                     id='v13-no-fill-identity'),
        pytest.param(13,
                     _LIVE_LEGACY_TOP_RECOVERY_SHAPE |
                     _LIVE_LEGACY_TOP_FILL_IDENTITY_V13,
                     'phx-context',
                     id='v13-location-derived-context'),
        pytest.param(13,
                     _LIVE_LEGACY_TOP_RECOVERY_SHAPE |
                     _LIVE_LEGACY_TOP_FILL_IDENTITY_V14,
                     'phx-context',
                     id='v13-explicit-context'),
        pytest.param(14,
                     _LIVE_LEGACY_TOP_RECOVERY_SHAPE |
                     _LIVE_LEGACY_TOP_FILL_IDENTITY_V14,
                     'phx-context',
                     id='v14-explicit-context'),
    ],
)
def test_live_legacy_reserved_fill_rows_preserve_cleanup_fence(
        version, top_fields, expected_context):
    state = _live_legacy_state(version, top_fields, _LIVE_LEGACY_STATUS_CURRENT)
    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    assert restored.reserved_fill is True
    assert restored.is_zero_cost is True
    fence = reserved_capacity.parse_protocol_v2_cleanup_fence(restored)
    if expected_context is None:
        assert fence is None
    else:
        assert fence == reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context=expected_context,
            physical_cluster_uid='physical-uid')
    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='Policy-bound reserved-fill launch'):
        replica_info.validate_reserved_fill_allocation_attribution(
            restored, require_policy_bound_admission=True)


@pytest.mark.parametrize(('version', 'top_fields', 'status_fields'),
                         _LIVE_LEGACY_CENSUS_CASES)
@pytest.mark.parametrize('malformation', ['missing', 'unexpected'])
def test_live_legacy_json_rejects_unsanctioned_top_level_shape(
        version, top_fields, status_fields, malformation):
    state = _live_legacy_state(version, top_fields, status_fields)
    if malformation == 'missing':
        state.pop('cluster_name')
    else:
        state['unexpected_legacy_field'] = None

    with pytest.raises(ValueError, match='invalid top-level shape'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize(('version', 'top_fields', 'status_fields'),
                         _LIVE_LEGACY_CENSUS_CASES)
@pytest.mark.parametrize('malformation', ['missing', 'unexpected'])
def test_live_legacy_json_rejects_unsanctioned_status_shape(
        version, top_fields, status_fields, malformation):
    state = _live_legacy_state(version, top_fields, status_fields)
    if malformation == 'missing':
        state['status_property'].pop('sky_launch_status')
    else:
        state['status_property']['unexpected_legacy_field'] = None

    with pytest.raises(ValueError, match='invalid status_property shape'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize('version',
                         [-1, 0, 1, 2, 4, 5, 8, 9, 10, 11, 15, 16, 19, 99])
def test_legacy_json_rejects_unsanctioned_and_future_versions(version):
    state = _live_legacy_state(3, _LIVE_LEGACY_TOP_BASE,
                               _LIVE_LEGACY_STATUS_BASE)
    state['replica_info_version'] = version

    with pytest.raises(ValueError, match='Unsupported ReplicaInfo'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize('version', [True, False, 13.0, '13', None])
def test_legacy_json_rejects_non_integer_versions(version):
    state = _live_legacy_state(13, _LIVE_LEGACY_TOP_RECOVERY_SHAPE,
                               _LIVE_LEGACY_STATUS_CURRENT)
    state['replica_info_version'] = version

    with pytest.raises(ValueError, match='version must be an integer'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize('value', [0, 1, 'true', None])
def test_legacy_json_requires_exact_reserved_fill_boolean(value):
    state = _golden_v7_state()
    state['reserved_fill'] = value

    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='reserved_fill marker must be a boolean'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize('value', [0, 1, 'true', None])
def test_legacy_json_requires_exact_zero_cost_boolean(value):
    state = _golden_v13_i3_state()
    state['is_zero_cost'] = value

    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='is_zero_cost must be an exact boolean'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize('value', [True, 0, -1, '4', None])
def test_legacy_json_requires_positive_integer_planned_capacity(value):
    state = _golden_v13_i3_state()
    state['planned_capacity'] = value

    with pytest.raises(ValueError,
                       match='planned_capacity must be a positive integer'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


def test_legacy_json_rejects_malformed_nested_image_id():
    state = _golden_v7_state()
    resources_override = state['resources_override']
    assert isinstance(resources_override, dict)
    resources_override['image_id'] = [['missing-image-value']]

    with pytest.raises(ValueError, match='Invalid replica image_id'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize('version', [13, 14])
def test_legacy_json_malformed_recovery_bundle_is_quarantined(version):
    top_fields = (_LIVE_LEGACY_TOP_RECOVERY_SHAPE |
                  _LIVE_LEGACY_TOP_FILL_IDENTITY_V14)
    state = _live_legacy_state(version, top_fields, _LIVE_LEGACY_STATUS_CURRENT)
    state['system_recovery_disposition'] = 'not-a-disposition'

    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    assert restored._version == 18
    assert restored.system_recovery_quarantine is not None
    assert (restored.system_recovery_quarantine.reason ==
            system_recovery_state.RecoveryQuarantineReason.MALFORMED_V13_BUNDLE)
    assert not restored.is_ready
    assert restored.to_storage_dict()['replica_info_version'] == 18


def test_v17_collision_requires_exact_status_shape():
    state = _replica().to_storage_dict()
    state['replica_info_version'] = 17
    state['status_property'].pop('logical_retirement_committed')
    with pytest.raises(ValueError, match='logical_retirement_committed'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize('field', replica_info._REPLICA_INFO_OWNED_FIELDS)
def test_current_json_requires_complete_owned_interface(field):
    state = _replica().to_storage_dict()
    state.pop(field)

    with pytest.raises(ValueError, match=field):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize('field', _status_field_names())
def test_current_json_requires_every_status_field(field):
    state = _replica().to_storage_dict()
    state['status_property'].pop(field)

    with pytest.raises(ValueError, match=field):
        replica_managers.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize('nested', [False, True])
def test_current_json_rejects_unknown_fields(nested):
    state = _replica().to_storage_dict()
    target = state['status_property'] if nested else state
    target['unknown_legacy_field'] = 'not-owned'

    with pytest.raises(ValueError, match='unexpected fields'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


def test_v17_collision_rejects_missing_noncollision_field():
    state = _replica().to_storage_dict()
    state['replica_info_version'] = 17
    state.pop('planned_capacity')

    with pytest.raises(ValueError, match='planned_capacity'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


def test_public_class_and_pickle_identity_remain_stable():
    assert replica_managers.ReplicaInfo is replica_info.ReplicaInfo
    assert (replica_managers.ReplicaStatusProperty
            is replica_info.ReplicaStatusProperty)
    assert replica_managers._NOT_PROVIDED is replica_info._NOT_PROVIDED
    assert replica_managers.ReplicaInfo.__module__ == (
        'sky.serve.replica_managers')
    assert replica_managers.ReplicaStatusProperty.__module__ == (
        'sky.serve.replica_managers')

    replica = _replica()
    restored = pickle.loads(pickle.dumps(replica, protocol=5))
    assert type(restored) is replica_managers.ReplicaInfo
    assert type(
        restored.status_property) is (replica_managers.ReplicaStatusProperty)
    assert restored.to_storage_dict() == replica.to_storage_dict()


def test_info_projection_reuses_one_cluster_record_and_endpoint_lookup():
    replica = _replica()
    handle = mock.MagicMock()
    handle.launched_resources.cloud = clouds.AWS()
    handle.launched_resources.region = 'us-east-1'
    handle.launched_resources.infra.formatted_str.return_value = (
        'aws (us-east-1)')
    handle.launched_nodes = 1
    cluster_record = {'handle': handle, 'launched_at': 90.0}

    with mock.patch.object(replica, 'handle',
                           return_value=handle) as handle_read, \
         mock.patch.object(replica_managers.global_user_state,
                           'get_cluster_from_name') as cluster_read, \
         mock.patch.object(replica_managers.backend_utils,
                           'get_endpoints',
                           return_value={8080: '1.2.3.4:8080'}) as endpoint, \
         mock.patch.object(
             replica_managers.resources_utils,
             'get_readable_resources_repr',
             return_value=('1x A100', '1x A100 (full)')) as resource_repr, \
         mock.patch.object(replica_managers.estimated_spend,
                           'estimate_hourly_cost',
                           return_value=(2.5, None)) as estimate:
        result = replica.to_info_dict(with_handle=False,
                                      cluster_record=cluster_record)

    cluster_read.assert_not_called()
    handle_read.assert_called_once_with(cluster_record)
    endpoint.assert_called_once_with('svc-7',
                                     8080,
                                     cluster_record=cluster_record)
    resource_repr.assert_called_once_with(handle, simplified_only=False)
    estimate.assert_called_once_with(handle.launched_resources, 1, None)
    assert result['endpoint'] == 'http://1.2.3.4:8080'
    assert result['hourly_cost'] == 2.5
    assert result['resources_str_full'] == '1x A100 (full)'
    assert result['time_to_ready_seconds'] == 50.0


def test_probe_contains_input_and_transport_failures():
    replica = _replica()
    client = mock.Mock()
    client.get.side_effect = ValueError('invalid user header')

    with mock.patch.object(replica_managers.replica_tls,
                           'probe_client',
                           return_value=client):
        actual, ready, probe_time = replica.probe(
            readiness_path='/health',
            post_data=None,
            timeout=7,
            headers={'X-User': 'value'},
            resolved_url='https://replica.example')

    assert actual is replica
    assert ready is False
    assert isinstance(probe_time, float)
    client.get.assert_called_once_with('https://replica.example/health',
                                       headers={'X-User': 'value'},
                                       timeout=7)


def test_probe_reports_exact_start_immediately_before_transport_call():
    replica = _replica()
    events = []
    response = mock.Mock(status_code=200)

    def _get(*_args, **_kwargs):
        events.append(('request', None))
        return response

    client = mock.Mock()
    client.get.side_effect = _get

    with mock.patch.object(replica_managers.replica_tls,
                           'probe_client',
                           return_value=client):
        _, ready, _ = replica.probe(
            readiness_path='/health',
            post_data=None,
            timeout=7,
            headers=None,
            resolved_url='https://replica.example',
            request_started_callback=lambda started_at: events.append(
                ('start', started_at)))

    assert ready
    assert [event for event, _ in events] == ['start', 'request']
    assert isinstance(events[0][1], float)
