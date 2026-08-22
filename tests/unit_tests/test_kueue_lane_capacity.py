"""Unit tests for the minimal Kueue admission capacity projection."""
import datetime
from types import SimpleNamespace
import uuid

import pytest

from sky.serve import kueue_lane_capacity
from sky.serve import kueue_lane_lineage

_NOW = datetime.datetime(2026, 8, 22, tzinfo=datetime.timezone.utc)
_SERVICE_HASH = 'service-hash'
_RECORD_ID = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')


class _ScalarResult:

    def scalar_one(self):
        return _NOW


class _Connection:

    def execute(self, _statement):
        return _ScalarResult()


class _Repository:

    def __init__(self, rows):
        self._rows = rows

    def lock_service_admissions_in_connection(self, _connection, *_args):
        return tuple(self._rows)


def _row(state, **overrides):
    values = {
        'service_name': 'svc',
        'service_hash': _SERVICE_HASH,
        'service_lifecycle_epoch': 2,
        'service_version': 3,
        'pool_key': 'pool-a',
        'pool_epoch': 7,
        'physical_cluster_uid': 'cluster-a',
        'kubernetes_context': 'phx',
        'accelerator': 'h200',
        'accelerator_count': 8,
        'worker_projection_sha256': 'b' * 64,
        'capacity_unit': 'physical',
        'planned_capacity': 1,
        'state': state.value if hasattr(state, 'value') else str(state),
        'intent_idempotency_key': 'intent-1',
        'replica_id': 7,
        'replica_record_id': _RECORD_ID,
        'valid_until': _NOW + datetime.timedelta(seconds=10),
        'replacement_surge_units': 0,
        'replacement_compatibility_sha256': None,
    }
    values.update(overrides)
    identity = kueue_lane_lineage.KueueAdmissionIdentity(
        service_name=values['service_name'],
        service_hash=values['service_hash'],
        service_lifecycle_epoch=values['service_lifecycle_epoch'],
        service_version=values['service_version'],
        pool_key=values['pool_key'],
        pool_epoch=values['pool_epoch'],
        physical_cluster_uid=values['physical_cluster_uid'],
        kubernetes_context=values['kubernetes_context'],
        accelerator=values['accelerator'],
        accelerator_count=values['accelerator_count'],
        worker_projection_sha256=values['worker_projection_sha256'])
    values.setdefault('unresolved_domain_sha256',
                      identity.unresolved_domain_sha256)
    return SimpleNamespace(**values)


def _intent_row(row):
    intent = {
        field: getattr(row, field)
        for field in ('intent_idempotency_key', 'service_name', 'service_hash',
                      'service_lifecycle_epoch', 'service_version', 'pool_key',
                      'pool_epoch', 'physical_cluster_uid',
                      'kubernetes_context', 'accelerator', 'accelerator_count',
                      'worker_projection_sha256', 'capacity_unit',
                      'planned_capacity', 'valid_until')
    }
    intent['state'] = 'GRANTED'
    return intent


def _project(monkeypatch,
             rows,
             *,
             live=None,
             live_intents=None,
             expected_kueue=True,
             include_intent=True):
    monkeypatch.setattr(kueue_lane_lineage, 'KueueAdmissionRepository',
                        lambda: _Repository(rows))
    source = rows[0] if rows else _row(
        kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING)
    intent = _intent_row(source)

    def _resolve(_connection, intent_row):
        if not expected_kueue:
            return None
        return kueue_lane_lineage.KueueAdmissionIdentity(
            service_name=intent_row['service_name'],
            service_hash=intent_row['service_hash'],
            service_lifecycle_epoch=intent_row['service_lifecycle_epoch'],
            service_version=intent_row['service_version'],
            pool_key=intent_row['pool_key'],
            pool_epoch=intent_row['pool_epoch'],
            physical_cluster_uid=intent_row['physical_cluster_uid'],
            kubernetes_context=intent_row['kubernetes_context'],
            accelerator=intent_row['accelerator'],
            accelerator_count=intent_row['accelerator_count'],
            worker_projection_sha256=intent_row['worker_projection_sha256'])

    monkeypatch.setattr(
        kueue_lane_capacity.zero_cost_actuation,
        'kueue_admission_identity_for_locked_intent_in_connection', _resolve)
    monkeypatch.setattr(
        kueue_lane_lineage, 'validate_admission_intent_identity',
        lambda _row, intent_row: _resolve(_Connection(), intent_row))
    return kueue_lane_capacity.lock_capacity_projection_in_connection(
        _Connection(),
        service_name='svc',
        service_hash=_SERVICE_HASH,
        service_lifecycle_epoch=2,
        service_version=3,
        accounting_cards={'h200'},
        locked_intent_rows=((intent,) if include_intent else ()),
        planned_capacity_by_intent_key={'intent-1': 1},
        capacity_unit_by_intent_key={'intent-1': 'physical'},
        live_replica_record_ids=({(7, _RECORD_ID)} if live is None else live),
        provider_present_replica_record_ids=set(),
        live_intent_keys=({'intent-1'}
                          if live_intents is None else live_intents))


def test_non_kueue_intent_has_no_override(monkeypatch):
    projection = _project(monkeypatch, [], expected_kueue=False)

    assert projection.demand_supply_for_intent('intent-1') is None
    assert projection.assigned_gpu_for_intent('intent-1') is None
    assert not projection.has_unknown


def test_missing_expected_kueue_admission_is_unknown(monkeypatch):
    projection = _project(monkeypatch, [])

    assert projection.demand_supply_for_intent('intent-1') is False
    assert projection.assigned_gpu_for_intent('intent-1') is True
    assert projection.unknown_shapes == {('h200', 8)}


def test_locked_pending_intent_is_live_without_process_replica(monkeypatch):
    projection = _project(monkeypatch, [], live_intents=set())

    assert projection.assigned_gpu_for_intent('intent-1') is True
    assert projection.unknown_shapes == {('h200', 8)}


def test_missing_live_intent_identity_is_unbounded_unknown(monkeypatch):
    projection = _project(monkeypatch, [], include_intent=False)

    assert projection.assigned_gpu_for_intent('intent-1') is True
    assert projection.unbounded_unknown


def test_fresh_exact_waiting_is_neither_supply_nor_assigned(monkeypatch):
    projection = _project(
        monkeypatch, [_row(kueue_lane_lineage.KueueAdmissionState.POD_WAITING)])

    assert projection.demand_supply_for_intent('intent-1') is False
    assert projection.assigned_gpu_for_intent('intent-1') is False
    assert projection.fresh_waiting_replica_record_ids == {(7, _RECORD_ID)}
    assert not projection.has_unknown


def test_policy_admitted_is_planned_supply_and_assigned(monkeypatch):
    projection = _project(
        monkeypatch,
        [_row(kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED)])

    assert projection.demand_supply_for_intent('intent-1') is True
    assert projection.assigned_gpu_for_intent('intent-1') is True
    assert projection.admitted_replica_record_ids == {(7, _RECORD_ID)}
    assert not projection.has_unknown


def test_live_admitted_predecessor_version_remains_supply(monkeypatch):
    projection = _project(monkeypatch, [
        _row(kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
             service_version=2)
    ])

    assert projection.demand_supply_for_intent('intent-1') is True
    assert projection.assigned_gpu_for_intent('intent-1') is True
    assert not projection.has_unknown


@pytest.mark.parametrize('row', [
    _row(kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING),
    _row(kueue_lane_lineage.KueueAdmissionState.POD_WAITING, valid_until=_NOW),
    _row('NOT_A_STATE'),
    _row(kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
         service_version=4),
])
def test_unknown_is_conservatively_assigned_and_blocks_paid(monkeypatch, row):
    projection = _project(monkeypatch, [row])

    assert projection.demand_supply_for_intent('intent-1') is False
    assert projection.assigned_gpu_for_intent('intent-1') is True
    assert projection.has_unknown
    assert projection.unknown_shapes == {('h200', 8)}


@pytest.mark.parametrize('state', [
    kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
    kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
])
def test_missing_exact_replica_is_unknown(monkeypatch, state):
    projection = _project(monkeypatch, [_row(state)], live=set())

    assert projection.assigned_gpu_for_intent('intent-1') is True
    assert projection.has_unknown


def test_duplicate_intent_rows_are_rejected(monkeypatch):
    row = _row(kueue_lane_lineage.KueueAdmissionState.POD_WAITING)

    with pytest.raises(kueue_lane_capacity.KueueAdmissionCapacityConflict,
                       match='one-to-one'):
        _project(monkeypatch, [row, row])


@pytest.mark.parametrize(
    ('debit', 'candidate', 'is_kueue', 'paid', 'allowed', 'uses_surge'),
    [
        (6, 2, True, 0, True, False),
        (8, 1, True, 1, True, True),
        # One eight-GPU Pod is still exactly one physical surge token.
        (8, 8, True, 8, True, True),
        (9, 1, True, 8, False, False),
        (8, 1, True, 0, False, False),
        (8, 1, False, 1, False, False),
        # Logical headroom reduces how much paid capacity must be replaced.
        (6, 8, True, 6, True, True),
        (6, 8, True, 5, False, False),
    ])
def test_fixed_physical_replacement_surge(debit, candidate, is_kueue, paid,
                                          allowed, uses_surge):
    decision = kueue_lane_capacity.decide_zero_cost_replacement_surge(
        max_capacity=8,
        physical_capacity_debit=debit,
        candidate_capacity=candidate,
        candidate_is_kueue=is_kueue,
        compatible_live_paid_capacity=paid)

    assert decision.allowed is allowed
    assert decision.uses_surge is uses_surge


def test_active_surge_lease_cannot_chain_after_partial_cleanup():
    decision = kueue_lane_capacity.decide_zero_cost_replacement_surge(
        max_capacity=20,
        physical_capacity_debit=20,
        candidate_capacity=1,
        candidate_is_kueue=True,
        compatible_live_paid_capacity=20,
        surge_lease_active=True)

    assert not decision.allowed
    assert not decision.uses_surge


def test_replacement_digest_is_exact_shape_and_projection_bound():
    kwargs = {
        'service_hash': _SERVICE_HASH,
        'service_lifecycle_epoch': 2,
        'service_version': 3,
        'capacity_unit': 'logical',
        'accelerator': 'h200',
        'accelerator_count': 8,
        'worker_projection_sha256': 'b' * 64,
    }
    digest = kueue_lane_capacity.replacement_compatibility_sha256(**kwargs)

    assert digest != kueue_lane_capacity.replacement_compatibility_sha256(**{
        **kwargs, 'accelerator': 'l4'
    })
    assert digest != kueue_lane_capacity.replacement_compatibility_sha256(**{
        **kwargs, 'accelerator_count': 1
    })
    assert digest != kueue_lane_capacity.replacement_compatibility_sha256(**{
        **kwargs, 'worker_projection_sha256': 'c' * 64
    })


def test_valid_surge_lease_is_projected_without_mutable_profiles(monkeypatch):
    digest = kueue_lane_capacity.replacement_compatibility_sha256(
        service_hash=_SERVICE_HASH,
        service_lifecycle_epoch=2,
        service_version=3,
        capacity_unit='physical',
        accelerator='h200',
        accelerator_count=8,
        worker_projection_sha256='b' * 64)
    projection = _project(monkeypatch, [
        _row(kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
             replacement_surge_units=1,
             replacement_compatibility_sha256=digest)
    ])

    assert projection.replacement_surge_shapes == {('h200', 8)}
    assert projection.replacement_surge_intent_keys == {'intent-1'}


def test_mismatched_surge_digest_is_exact_shape_unknown(monkeypatch):
    projection = _project(monkeypatch, [
        _row(kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
             replacement_surge_units=1,
             replacement_compatibility_sha256='c' * 64)
    ])

    assert projection.has_unknown
    assert not projection.unbounded_unknown
    assert projection.unknown_shapes == {('h200', 8)}
    # Positive durable lease remains a conservation barrier despite the bad
    # compatibility receipt.
    assert projection.replacement_surge_shapes == {('h200', 8)}
