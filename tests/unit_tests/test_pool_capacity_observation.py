"""Unit contracts for immutable physical-pool observations."""

# pylint: disable=protected-access

import dataclasses
import importlib
import json
import uuid

import pytest
import sqlalchemy

from sky.serve import pool_capacity_observation as observation
from sky.serve import pool_capacity_observation_schema as observation_schema
from sky.serve import reserved_fill_reclaim_attestation as reclaim_attestation
from sky.serve import serve_state_schema


def _pool_key(*names: str) -> str:
    encoded_names: str | list[str] = (names[0]
                                      if len(names) == 1 else list(names))
    return json.dumps(['v2', 'physical-uid', encoded_names])


def _receipt(
    identity: reclaim_attestation.ReclaimPolicyIdentity,
) -> reclaim_attestation.ReclaimActivationReceipt:
    scope_count, scope_sha256 = reclaim_attestation.claim_scope_projection(())
    return reclaim_attestation.ReclaimActivationReceipt(
        identity=identity,
        claim_scope_count=scope_count,
        claim_scope_sha256=scope_sha256,
        evidence_sha256='c' * 64,
        writer_image_digest='sha256:' + 'd' * 64,
        writer_deployment_generation='17',
        writer_deployment_uid='deployment-uid',
        writer_pod_inventory_count=3,
        writer_pod_inventory_sha256='e' * 64)


def _completed_row(
        payload: observation.PoolCapacityPayload) -> dict[str, object]:
    pool_key = _pool_key('a100-80gb', 'h200')
    generation = 7
    row_key = observation._row_key(pool_key, generation)
    lease_token = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
    values: dict[str, object] = {
        'context': row_key,
        'snapshot_time': 100.0,
        'completed_at': 120.0,
        'availability': None,
        'pool_key': pool_key,
        'physical_cluster_uid': 'physical-uid',
        'accelerator_names': ['a100-80gb', 'h200'],
        'access_context': 'research-east',
        'observation_generation': generation,
        'lease_token': lease_token,
        'lease_expires_at': 145.0,
        'observation_sequence': 19,
        'ordinary_admission_sequence': 17,
        'materialization_sequence': 13,
        'observation_status': ('SUCCESS' if isinstance(
            payload, observation.PoolCapacitySuccess) else 'BLACKOUT'),
        'payload': payload.canonical_value(),
        'observed_at': 100.0,
        'valid_until': 280.0,
        'published_at': 120.0,
    }
    values['payload_sha256'] = observation._authority_sha256(
        row_key=row_key,
        pool_key=pool_key,
        physical_cluster_uid='physical-uid',
        accelerator_names=('a100-80gb', 'h200'),
        access_context='research-east',
        observation_generation=generation,
        lease_token=lease_token,
        lease_expires_at=145.0,
        observation_sequence=19,
        ordinary_admission_sequence=17,
        materialization_sequence=13,
        payload=payload,
        observed_at=100.0,
        completed_at=120.0,
        valid_until=280.0,
        published_at=120.0,
    )
    return values


def test_success_payload_is_canonical_exact_and_immutable() -> None:
    payload = observation.PoolCapacitySuccess.from_counts(
        5, {
            'H200': 2,
            'A100-80GB': 3,
        })

    assert payload.free_gpus_by_accelerator == (('a100-80gb', 3), ('h200', 2))
    assert payload.present_accelerator_names == ('a100-80gb', 'h200')
    assert payload.canonical_value() == {
        'kind': 'success',
        'free_gpus': 5,
        'free_gpus_by_accelerator': {
            'a100-80gb': 3,
            'h200': 2,
        },
        'present_accelerator_names': ['a100-80gb', 'h200'],
    }
    with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
        payload.free_gpus = 6  # type: ignore[misc]


@pytest.mark.parametrize(
    'free_gpus, counts',
    [
        (True, {
            'a100': 1
        }),
        (1, {
            'a100': True
        }),
        (1, {
            'A100': 1,
            'a100': 0
        }),
        (2, {
            'a100': 1
        }),
        (0, {}),
    ],
)
def test_success_payload_rejects_ambiguous_counts(
        free_gpus: object, counts: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        observation.PoolCapacitySuccess.from_counts(  # type: ignore[arg-type]
            free_gpus, counts)


def test_success_payload_preserves_absent_vs_present_zero() -> None:
    absent = observation.PoolCapacitySuccess.from_counts(
        0, {
            'a100': 0,
            'h200': 0,
        }, present_accelerator_names=())
    full = observation.PoolCapacitySuccess.from_counts(
        0, {
            'a100': 0,
            'h200': 0,
        }, present_accelerator_names=('a100', 'h200'))

    assert not absent.present_accelerator_names
    assert full.present_accelerator_names == ('a100', 'h200')
    assert absent.canonical_value() != full.canonical_value()


def test_success_payload_converts_raw_gpus_with_one_pure_width_boundary(
) -> None:
    payload = observation.PoolCapacitySuccess.from_counts(
        19, {
            'a100': 9,
            'h200': 10,
        })

    assert payload.slot_counts(8) == (('a100', 1), ('h200', 1))
    with pytest.raises(ValueError, match='positive integer'):
        payload.slot_counts(True)


def test_success_decoder_rejects_non_list_presence_shape() -> None:
    raw = {
        'kind': 'success',
        'free_gpus': 0,
        'free_gpus_by_accelerator': {
            'a100': 0,
        },
        'present_accelerator_names': 'a100',
    }

    assert observation._decode_payload(  # pylint: disable=protected-access
        raw, observation_schema.SUCCESS) is None


def test_blackout_payload_is_closed_and_bounded() -> None:
    payload = observation.PoolCapacityBlackout(
        observation.PoolCapacityBlackoutReason.PERMISSION_DENIED,
        'pods/list is forbidden')
    assert payload.canonical_value() == {
        'kind': 'blackout',
        'reason': 'PERMISSION_DENIED',
        'detail': 'pods/list is forbidden',
    }
    with pytest.raises(ValueError):
        observation.PoolCapacityBlackout(  # type: ignore[arg-type]
            'PERMISSION_DENIED')
    with pytest.raises(ValueError):
        observation.PoolCapacityBlackout(
            observation.PoolCapacityBlackoutReason.PROVIDER_ERROR, 'x' * 4097)


def test_completed_decode_verifies_full_legacy_and_authority_payload() -> None:
    payload = observation.PoolCapacitySuccess.from_counts(
        3, {
            'a100-80gb': 1,
            'h200': 2,
        })
    values = _completed_row(payload)

    decoded = observation._decode_completed_row(
        values)  # type: ignore[arg-type]
    assert decoded is not None
    assert decoded.payload == payload
    assert decoded.is_authoritative_at(120.0)
    assert decoded.is_authoritative_at(280.0)
    assert not decoded.is_authoritative_at(280.001)

    # A legacy writer changes only its old projection.  The stored authority
    # digest no longer matches, so the row fails closed instead of preserving
    # stale capacity under new identity fields.
    tampered = dict(values)
    tampered['availability'] = '{"a100-80gb": 99}'
    assert observation._decode_completed_row(  # type: ignore[arg-type]
        tampered) is None

    tampered = dict(values)
    tampered['physical_cluster_uid'] = 'retargeted-cluster'
    assert observation._decode_completed_row(  # type: ignore[arg-type]
        tampered) is None

    tampered = dict(values)
    tampered['payload'] = {
        **payload.canonical_value(),
        'free_gpus': 4,
    }
    assert observation._decode_completed_row(  # type: ignore[arg-type]
        tampered) is None


def test_blackout_is_typed_completed_evidence_but_never_authority() -> None:
    blackout = observation.PoolCapacityBlackout(
        observation.PoolCapacityBlackoutReason.TIMEOUT)
    decoded = observation._decode_completed_row(  # type: ignore[arg-type]
        _completed_row(blackout))
    assert decoded is not None
    assert decoded.payload == blackout
    assert not decoded.is_authoritative_at(120.0)


def test_pool_key_and_payload_identity_must_match_exactly() -> None:
    key = _pool_key('A100-80GB', 'h200')
    assert observation._parse_physical_pool_key(key) == ('physical-uid',
                                                         ('a100-80gb', 'h200'))
    with pytest.raises(ValueError):
        observation._parse_physical_pool_key(
            json.dumps(['context-alias', 'a100']))
    with pytest.raises(ValueError):
        observation._parse_physical_pool_key(
            json.dumps(['v2', 'physical-uid', ['A100', 'a100']]))


def test_postgresql_catalog_isolated_from_shared_sqlite_metadata() -> None:
    legacy_columns = {
        column.name
        for column in serve_state_schema.demand_capacity_observations_table.c
    }
    authority_columns = {
        column.name
        for column in observation_schema.demand_capacity_observations_v2_table.c
    }
    assert legacy_columns == {
        'context',
        'snapshot_time',
        'completed_at',
        'availability',
    }
    assert {
        'pool_key', 'observation_generation', 'lease_token', 'payload',
        'payload_sha256', 'valid_until'
    } <= authority_columns
    assert (observation_schema.demand_capacity_observations_v2_table.metadata
            is not serve_state_schema.Base.metadata)
    assert (observation_schema.reserved_fill_round_observation_table.metadata
            is observation_schema.metadata)
    assert (observation_schema.reserved_fill_service_allocation_table.metadata
            is observation_schema.metadata)
    assert 'observation_generation' not in (
        serve_state_schema.reserved_fill_rounds_table.c)
    assert 'allocation_generation' not in (
        serve_state_schema.reserved_fill_service_claim_sets_table.c)

    sqlite = sqlalchemy.create_engine('sqlite:///:memory:')
    with pytest.raises(ValueError, match='PostgreSQL-only'):
        observation.PoolCapacityObservationRepository(sqlite)


def test_observation_authority_is_composed_into_forward_serve044() -> None:
    revision = importlib.import_module(
        'sky.schemas.db.serve_state.044_reserved_fill_reconciliation')
    assert revision.revision == '044'
    assert revision.down_revision == '043'
    assert hasattr(revision, '_install_observation_authority')


def test_reclaim_identity_is_a_self_contained_serve045_successor() -> None:
    revision = importlib.import_module(
        'sky.schemas.db.serve_state.045_reserved_fill_reclaim_policy')

    assert revision.revision == '045'
    assert revision.down_revision == '044'
    assert not hasattr(revision, 'observation_schema')
    assert {
        'reclaim_fleet_bundle_sha256',
        'reclaim_policy_revision',
        'reclaim_provider_inventory_sha256',
        'reclaim_claim_scope_count',
        'reclaim_claim_scope_sha256',
        'reclaim_evidence_sha256',
        'reclaim_authorized_at',
        'image_digest',
        'deployment_generation',
        'deployment_uid',
        'pod_inventory_count',
        'pod_inventory_sha256',
    } <= {
        column.name
        for column in observation_schema.protocol_state_sequence_table.c
    }


def test_worker_projection_claim_binding_is_serve046_successor() -> None:
    revision = importlib.import_module(
        'sky.schemas.db.serve_state.046_reserved_fill_worker_projection')

    assert revision.revision == '046'
    assert revision.down_revision == '045'


def test_reconciliation_gate_snapshot_is_typed_and_immutable() -> None:
    gate = observation.ReconciliationGate(
        state=observation.ReconciliationGateState.LEGACY_ACTIVE, generation=0)

    assert not gate.sequenced_active
    with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
        gate.generation = 1  # type: ignore[misc]


def test_reconciliation_gate_state_and_identity_are_one_closed_tuple() -> None:
    identity = reclaim_attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256='a' * 64,
        policy_revision='policy-v1',
        provider_inventory_sha256='b' * 64)

    active = observation.ReconciliationGate(
        state=observation.ReconciliationGateState.SEQUENCED_ACTIVE,
        generation=1,
        reclaim_policy_identity=identity,
        reclaim_activation_receipt=_receipt(identity),
        reclaim_authorized_at=100.0)
    assert active.sequenced_active
    with pytest.raises(ValueError, match='legacy.*cannot carry'):
        observation.ReconciliationGate(
            state=observation.ReconciliationGateState.LEGACY_ACTIVE,
            generation=0,
            reclaim_policy_identity=identity)
    with pytest.raises(ValueError, match='sequenced.*requires'):
        observation.ReconciliationGate(
            state=observation.ReconciliationGateState.SEQUENCED_ACTIVE,
            generation=1)


def test_reconciliation_gate_decoder_rejects_partial_active_identity() -> None:
    row = {
        'reconciliation_gate_state': 'SEQUENCED_ACTIVE',
        'reconciliation_gate_generation': 1,
        'reclaim_fleet_bundle_sha256': 'a' * 64,
        'reclaim_policy_revision': None,
        'reclaim_provider_inventory_sha256': 'b' * 64,
        'reclaim_claim_scope_count': 0,
        'reclaim_claim_scope_sha256': 'c' * 64,
        'reclaim_evidence_sha256': 'd' * 64,
        'reclaim_authorized_at': 100.0,
        'image_digest': 'sha256:' + 'e' * 64,
        'deployment_generation': '17',
        'deployment_uid': 'deployment-uid',
        'pod_inventory_count': 3,
        'pod_inventory_sha256': 'f' * 64,
    }

    with pytest.raises(observation.ObservationRepositoryCorruptionError,
                       match='authorization is malformed'):
        observation._decode_reconciliation_gate(row)  # type: ignore[arg-type]
