"""Typed SkyServe placement-contract and persistence compatibility tests."""

import base64
import hashlib
import inspect
import logging
import pickle
import types
from typing import Any
from unittest import mock
import uuid
import zlib

import pytest
from spot_placer_test_utils import make_location
import sqlalchemy
from sqlalchemy import orm
import test_serve_resource_actions as resource_action_fixtures
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

import sky
from sky.container_images import demand_state
from sky.serve import constants as serve_constants
from sky.serve import ephemeral_storage_contract
from sky.serve import placement_contract_normalization
from sky.serve import placement_policy
from sky.serve import serve_state
from sky.serve import service_spec
from sky.serve import spot_placer


@pytest.fixture(autouse=True)
def _exact_normalizer_release_commit(monkeypatch):
    monkeypatch.setattr(sky, '__commit__', 'a' * 40)


# Generated with pickle protocol 4 from the unmodified v1.1.1132 release at
# ab5ec55b89a8c576e20e6ea27cf240e88134bb64.  Keeping the real preceding-
# release artifact catches qualified-class and state-layout incompatibilities
# that a same-binary synthetic state cannot.  It was generated with Python
# 3.11.13, pickle protocol 4, and zlib level 9 from a SkyServiceSpec with the
# exact fields returned by _base_spec_kwargs() plus max_replicas=8,
# target_concurrency_per_replica=1, dynamic_fallback_per_gpu, and
# graceful_drain_async_occupancy=True.
_V1_1_1132_PER_GPU_SPEC_ZLIB_B64 = (
    'eNptVNtu1DAQLdACQlxEJbZVC7SgCsFL+RCkfekHjCb2JGvVsYMvS4OEBG+tlDfC/zL2'
    'Nsluu3mIojnHM+MzZ/J799/Z3k5+upm/bM89uSXltxIEviHRdy8vLtuLVeQiBf72X/'
    '70v/rP3StwhFIZ8h4aDIu+e/J1QajT1wyUUUGhBkkaW/AkrJG+//agO1o7FlRNNoYJ'
    'ftidAhnZWGUCNM4WxIkCV+dMI+kR59AF+MCJ6vs5dlewo+AUFprvETBED8JK8v2c+2'
    'a0xqsVI4c+DAfase8CxaUtyzHtvHu9In2P5APwOxIH36TgQlULwCUqjYXSKrT9dXey'
    'FciaqlKR7G+6A6gcCiojq+RQmbVaJ3ch9K0RYIWIDRqRCryAmuOOGq0E8rV3UiTfa'
    'og87d5ucKBoAYUgTQ6DdTzEbh9MrMEuybHYS+WVNVz9MTTWhdTHAQR0FfF9Gx4yuS'
    'FXbvEW4p5FdI64rQ0Kz/r9wImBr/8TA+dPHEEmYJX0+wh0xZIEkqO0MroVcZLjbBzL'
    '2CZodlEe/hrvGFBiE9SS2Hc1GgmcUNU5HcPvskBeIJsiNsBlaLOb4zvghsLp/L2zys'
    'q1+vtT/YGZlZrKSvvDbCk8g+jJg7YVl9JTzWuegKO8lhIENiiSiUqldTaysKyXowI1'
    'myLlecaTSxJiwEwImn3vSHIhVi93OG0fL6skl/o+AtkarBU7zNwKV6LWaQcYPeVl8H'
    'QfWpfmOTvb8spqdq3ru8Mh38hNzqiamH4NsbnVYuPXMO8OszrbsRlrw/Ne3VSZiu/J'
    'pVtG9vjT6uzVvJRsDFOFBYQFC7ewWjL0KZmUk8U8mpL3MTqaGMM/ZBjEFhVuYnH+H8'
    'SKFvA=')

# Generated from unmodified v1.1.247 at
# de1a52ff83ff628f8413f315deb89a2adf4238c7, the exact parent release of the
# logical-marker change.  This is the real historical physical/per-GPU state:
# neither the marker nor versioned placement fields existed yet.  It was
# generated with Python 3.11.13, pickle protocol 4, and zlib level 9 from the
# same exact constructor fields documented for the preceding-release fixture.
_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64 = (
    'eNptUstuFDEQDCRAhHiIHDZRLoQLgkv4EKS97Ae0euzeXWs99uDHwhyQQOIAkm8Z/pca'
    'L/vIYw6Wp6tdXV3dP07+/jo+ql+ZxFV/HSWspZ5GCcVO1FBezVb9bBOZjYGb4ePP4f'
    'vwobymIKyNkxip47QcyrNPS2E73iZknEmGLWmx3FMU5Z2Ow+dH5fLgWTKt+Jz28ONy'
    'ReJ0541L1AXfCIgSqoNpl3QMDttQTCBq73OcbOAgKRhuLPpInHIk5bXEYQrdQFv+ts'
    'mooXfbB/1Od8Nq5efzHe20vNkkfckSE+HMguA5LQIrmWd0Gti4g/y3dyGOvVPklcod'
    'O9UPv8tLahEP0lmjGNKPxkhVto2cljNyuSW/lgA/1iYa70D+lDofUqwKEoeFQFKHOU'
    'jYPq4K/kOQpHIIgqq3UjCOc/zUsWtS3LEyqae5sbYapTxaDdKwheCx3ecoi5DmxDUh'
    'WfgaRIsbTUPo7GC6WAYtYdR4Sbp33Bp077S07DTN2drRY6BXMDvKfWhvw7S8wDZ6rI'
    'SFo2EoF1u+Xe7Y1qLL4+rlLirG3G+v3rRckPZf3cPYhKxnTZtOjVugT5TugTzB1dtq'
    'dB06WXGLtKS0hHFLbzWg96PDIMvJrAWajM1B9hnbHa118kOtDn9yc/0PKFJszg==')


def _base_spec_kwargs() -> dict[str, Any]:
    return {
        'readiness_path': '/health',
        'initial_delay_seconds': 1,
        'readiness_timeout_seconds': 2,
        'endpoint_probe_interval_seconds': 3,
        'lb_stream_timeout_seconds': 4,
        'min_replicas': 0,
    }


def _spec(spot_placer_name: str | None = None) -> service_spec.SkyServiceSpec:
    kwargs = _base_spec_kwargs()
    if spot_placer_name == placement_policy.CAPACITY_AWARE_SPOT_PLACER:
        kwargs.update({
            'max_replicas': 8,
            'target_concurrency_per_replica': 1,
            'graceful_drain_async_occupancy': True,
        })
    kwargs['spot_placer'] = spot_placer_name
    return service_spec.SkyServiceSpec(**kwargs)


def _restore(state: dict[str, Any]) -> service_spec.SkyServiceSpec:
    restored = service_spec.SkyServiceSpec.__new__(service_spec.SkyServiceSpec)
    restored.__setstate__(state)
    return restored


def _raw_spec_pickle(state: dict[str, Any], protocol: int = 4) -> bytes:
    return placement_contract_normalization._serialize_raw_state(
        state, protocol)


def _explicit_v2_payload(spot_placer_name: str | None = None,
                         protocol: int = 4) -> bytes:
    spec = _spec(spot_placer_name)
    state = dict(spec.__dict__)
    state.update(spec.placement_contract.persisted_fields())
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)
    return _raw_spec_pickle(state, protocol=protocol)


def _normalizer_work(payload: bytes,
                     version: int,
                     yaml_content: str | None = 'service: {}'):
    row = {
        'service_name': 'svc',
        'version': version,
        'spec': payload,
        'yaml_content': yaml_content,
        'submitted_yaml_content': None,
        'created_at': 1.0,
        'created_by': 'test',
        'quarantined_at': None,
        'quarantine_reason': None,
        'retired_yaml_content': None,
        'retired_at': None,
        'retirement_reason': None,
        'retirement_run_id': None,
        'placement_catalog': None,
        'controller_config': None,
        'controller_config_digest': None,
        'controller_config_snapshot_id': None,
        'controller_applied_at': None,
    }
    analysis, classification = (
        placement_contract_normalization._classify_version_row(row))
    return placement_contract_normalization._RowWork(
        row,
        dict(row),
        analysis,
        classification,
        dependency_facts={
            'service_present': True,
            'service_current_version': 3,
            'service_hash': 'current-hash',
            'service_lifecycle_epoch': 7,
            'service_resource_scope': 'current-scope',
            'service_status': 'READY',
            'service_active': False,
            'replica_count': 0,
            'unknown_version_replica_count': 0,
            'cleanup_intent_count': 0,
            'quarantined': False,
            'controller_applied': False,
            'retired': False,
        })


def _api_pod_identity() -> (placement_contract_normalization._ApiPodIdentity):
    return placement_contract_normalization._canonical_api_pod_identity(
        'pod-a', uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'))


def _retirement_service_row(current_version: int = 2) -> dict[str, Any]:
    return {
        'name': 'svc',
        'current_version': current_version,
        'active_versions': f'[{current_version}]',
        'hash': 'current-hash',
        'lifecycle_epoch': 7,
        'resource_scope': 'current-hash',
        'workspace': 'workspace',
        'status': 'READY',
        'pool': 0,
        'resource_action_mode': 'legacy',
        'resource_action_mode_changed_at': None,
    }


def _zero_target_cleanup_yaml(resource_scope: str,
                              storage_generation: str) -> str:
    scope_id = ephemeral_storage_contract.canonical_ephemeral_storage_scope_id(
        resource_scope, storage_generation)
    metadata_key = serve_constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY
    return f"""\
_metadata:
  {metadata_key}:
    resource_scope: {resource_scope}
    scope_id: {scope_id}
    storage_generation: {storage_generation}
    storage_mounts: []
service: {{}}
"""


def _attach_cleanup_intent_inputs(
    rows: list[placement_contract_normalization._RowWork],
    service_rows: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach unique typed cleanup YAML and return exact intent preimages."""
    intents: list[dict[str, Any]] = []
    candidates = [
        row for row in rows
        if row.classification is placement_contract_normalization.
        Classification.HISTORICAL_PHYSICAL_PER_GPU
    ]
    for row in candidates:
        service_name, version = row.identity
        service = service_rows[service_name]
        resource_scope = service['resource_scope']
        storage_generation = f'generation-{version}'
        yaml_content = _zero_target_cleanup_yaml(resource_scope,
                                                 storage_generation)
        row.original['yaml_content'] = yaml_content
        row.result['yaml_content'] = yaml_content
        intents.append({
            'service_name': service_name,
            'resource_scope': resource_scope,
            'storage_generation': storage_generation,
            'yaml_content': yaml_content,
            'pool': 0,
            'lifecycle_epoch': service['lifecycle_epoch'],
            'provisional': 1,
            'created_at': 0.5,
        })
    intent_counts: dict[str, int] = {}
    for intent in intents:
        service_name = intent['service_name']
        intent_counts[service_name] = intent_counts.get(service_name, 0) + 1
    for row in rows:
        row.dependency_facts['cleanup_intent_count'] = intent_counts.get(
            row.identity[0], 0)
        row.dependency_facts['service_resource_scope'] = service_rows[
            row.identity[0]]['resource_scope']
    return intents


def _build_test_cleanup_plan(
    rows: list[placement_contract_normalization._RowWork],
    service_rows: dict[str, dict[str, Any]],
) -> placement_contract_normalization._CleanupIntentPlan:
    intents = _attach_cleanup_intent_inputs(rows, service_rows)
    return placement_contract_normalization._build_cleanup_intent_plan(
        intents, rows, service_rows, row_bound=max(1, len(intents)))


def _test_predecessor_receipt_evidence(
    service_names: set[str],
) -> placement_contract_normalization._PredecessorReceiptEvidence:
    approved_commit = 'b' * 40
    approved_digest = placement_contract_normalization._canonical_json_sha256(
        [approved_commit])
    freeze_input_digest = 'c' * 64
    freeze_binding_digest = (
        placement_contract_normalization._canonical_json_sha256({
            'approved_loaded_image_commit_sha256': approved_digest,
            'operator_freeze_evidence_input_sha256': freeze_input_digest,
        }))
    facts = {
        'predecessor_receipt_schema':
            placement_contract_normalization._PREDECESSOR_RECEIPT_SCHEMA,
        'predecessor_receipt_inventory_count': len(service_names),
        'predecessor_receipt_inventory_sha256':
            placement_contract_normalization._canonical_json_sha256(
                sorted(service_names)),
        'approved_loaded_image_commit_count': 1,
        'approved_loaded_image_commit_sha256': approved_digest,
        'operator_freeze_evidence_input_sha256': freeze_input_digest,
        'operator_freeze_approved_commit_binding_sha256': freeze_binding_digest,
        'predecessor_receipts_complete': True,
    }
    return placement_contract_normalization._PredecessorReceiptEvidence(
        frozenset(service_names), facts)


def _single_cleanup_plan_inputs() -> tuple[
    list[dict[str, Any]],
    list[placement_contract_normalization._RowWork],
    dict[str, dict[str, Any]],
]:
    historical_payload = zlib.decompress(
        base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64))
    candidate = _normalizer_work(historical_payload, 1)
    successor = _normalizer_work(_explicit_v2_payload(), 2)
    rows = [candidate, successor]
    service_rows = {'svc': _retirement_service_row()}
    intents = _attach_cleanup_intent_inputs(rows, service_rows)
    return intents, rows, service_rows


@pytest.mark.parametrize(('policy_name', 'pool', 'expected'), [
    (None, False, (placement_policy.ENGINE_NONE,
                   placement_policy.REPLICA_UNIT_PHYSICAL_BACKEND,
                   placement_policy.CATALOG_MODE_NOT_APPLICABLE,
                   placement_policy.COST_UNIT_NOT_APPLICABLE,
                   placement_policy.RESERVED_FILL_MODE_NOT_APPLICABLE,
                   placement_policy.WORKLOAD_KIND_SERVICE)),
    (None, True, (placement_policy.ENGINE_NONE,
                  placement_policy.REPLICA_UNIT_PHYSICAL_BACKEND,
                  placement_policy.CATALOG_MODE_NOT_APPLICABLE,
                  placement_policy.COST_UNIT_NOT_APPLICABLE,
                  placement_policy.RESERVED_FILL_MODE_NOT_APPLICABLE,
                  placement_policy.WORKLOAD_KIND_POOL)),
    (placement_policy.SPOT_HEDGE_PLACER, False,
     (placement_policy.ENGINE_DYNAMIC_FALLBACK,
      placement_policy.REPLICA_UNIT_PHYSICAL_BACKEND,
      placement_policy.CATALOG_MODE_CONFIGURED_SHAPES,
      placement_policy.COST_UNIT_MACHINE_HOUR,
      placement_policy.RESERVED_FILL_MODE_CONFIGURED_SHAPE,
      placement_policy.WORKLOAD_KIND_SERVICE)),
    (placement_policy.SPOT_HEDGE_PLACER, True,
     (placement_policy.ENGINE_DYNAMIC_FALLBACK,
      placement_policy.REPLICA_UNIT_PHYSICAL_BACKEND,
      placement_policy.CATALOG_MODE_CONFIGURED_SHAPES,
      placement_policy.COST_UNIT_MACHINE_HOUR,
      placement_policy.RESERVED_FILL_MODE_CONFIGURED_SHAPE,
      placement_policy.WORKLOAD_KIND_POOL)),
    (placement_policy.CAPACITY_AWARE_SPOT_PLACER, False,
     (placement_policy.ENGINE_DYNAMIC_FALLBACK,
      placement_policy.REPLICA_UNIT_LOGICAL,
      placement_policy.CATALOG_MODE_WHOLE_GPU_SHAPES,
      placement_policy.COST_UNIT_GPU_SLOT_HOUR,
      placement_policy.RESERVED_FILL_MODE_SINGLE_GPU_BACKEND,
      placement_policy.WORKLOAD_KIND_SERVICE)),
])
def test_fresh_contract_matrix(policy_name, pool, expected):
    contract = placement_policy.resolve_fresh_contract(policy_name, pool)

    assert (contract.engine, contract.replica_unit, contract.catalog_mode,
            contract.cost_unit, contract.reserved_fill_mode,
            contract.workload_kind) == expected


@pytest.mark.parametrize(('policy_name', 'pool', 'expand'), [
    (placement_policy.SPOT_HEDGE_PLACER, False, False),
    (placement_policy.SPOT_HEDGE_PLACER, True, False),
    (placement_policy.CAPACITY_AWARE_SPOT_PLACER, False, True),
])
def test_one_engine_factory_propagates_exact_contract(policy_name, pool,
                                                      expand):
    contract = placement_policy.resolve_fresh_contract(policy_name, pool)
    spec = types.SimpleNamespace(placement_contract=contract)
    task = types.SimpleNamespace(resources=[mock.sentinel.resources],
                                 num_nodes=1)
    catalog = spot_placer.PlacementCatalog(tuple(), num_nodes=1)
    with mock.patch.object(spot_placer.PlacementCatalog,
                           'from_task',
                           return_value=catalog) as build:
        assert spot_placer.SpotPlacer.build_catalog(spec, task) is catalog
    assert build.call_args.kwargs['expand_accelerator_counts'] is expand

    placer = spot_placer.SpotPlacer.from_task(spec,
                                              task,
                                              placement_catalog=catalog)

    assert type(placer) is spot_placer.DynamicFallbackSpotPlacer
    assert placer.placement_contract == contract


def test_historical_contract_uses_one_engine_and_per_gpu_ranking():
    contract = placement_policy.resolve_legacy_contract(
        placement_policy.CAPACITY_AWARE_SPOT_PLACER,
        pool=False,
        uses_logical_replicas=False)
    spec = types.SimpleNamespace(placement_contract=contract)
    task = types.SimpleNamespace(resources=[mock.sentinel.resources],
                                 num_nodes=1)
    one_gpu = make_location('one', accelerators={'L4': 1}, cloud_name='AWS')
    four_gpu = make_location('four', accelerators={'L4': 4}, cloud_name='AWS')
    catalog = spot_placer.PlacementCatalog(((four_gpu, 0.6), (one_gpu, 0.2)),
                                           num_nodes=1)
    with mock.patch.object(spot_placer.PlacementCatalog,
                           'from_task',
                           return_value=catalog) as build:
        assert spot_placer.SpotPlacer.build_catalog(spec, task) is catalog
    assert build.call_args.kwargs['expand_accelerator_counts'] is True

    placer = spot_placer.SpotPlacer.from_task(spec,
                                              task,
                                              placement_catalog=catalog)

    assert type(placer) is spot_placer.DynamicFallbackSpotPlacer
    assert placer.placement_contract == contract
    assert not placer.placement_contract.uses_logical_replicas
    assert placer.select_next_location() == four_gpu


def test_disabled_contract_constructs_no_catalog_or_engine():
    contract = placement_policy.resolve_fresh_contract(None, pool=False)
    spec = types.SimpleNamespace(placement_contract=contract)
    task = mock.Mock()
    with mock.patch.object(spot_placer.PlacementCatalog, 'from_task') as build:
        assert spot_placer.SpotPlacer.build_catalog(spec, task) is None
        assert spot_placer.SpotPlacer.from_task(spec, task) is None
    build.assert_not_called()


def test_logical_policy_rejects_pool_contract():
    with pytest.raises(ValueError, match='not supported for pools'):
        placement_policy.resolve_fresh_contract(
            placement_policy.CAPACITY_AWARE_SPOT_PLACER, pool=True)


def test_empty_pool_mapping_is_explicitly_a_pool():
    contract = placement_policy.resolve_fresh_contract(None, pool={})

    assert contract.workload_kind == placement_policy.WORKLOAD_KIND_POOL
    parsed = service_spec.SkyServiceSpec.from_yaml_config({
        'pool': {},
        'workers': 1,
    })
    assert parsed.pool
    assert parsed.__dict__[placement_policy.POOL_FIELD] == {'workers': 1}
    assert parsed.placement_contract.workload_kind == (
        placement_policy.WORKLOAD_KIND_POOL)
    assert parsed.to_yaml_config()['pool']['workers'] == 1
    with pytest.raises(ValueError, match='not supported for pool'):
        service_spec.SkyServiceSpec(
            **_base_spec_kwargs(),
            max_replicas=8,
            target_concurrency_per_replica=1,
            graceful_drain_async_occupancy=True,
            spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER,
            pool={})


@pytest.mark.parametrize('policy_name', [
    'DYNAMIC_FALLBACK_PER_GPU',
    'Dynamic_Fallback_Per_GPU',
])
def test_case_insensitive_service_policy_is_persisted_canonically(policy_name):
    parsed = service_spec.SkyServiceSpec.from_yaml_config({
        'replica_policy': {
            'min_replicas': 0,
            'max_replicas': 8,
            'target_concurrency_per_replica': 1,
            'spot_placer': policy_name,
        },
        'graceful_drain_async_occupancy': True,
    })

    assert parsed.spot_placer == placement_policy.CAPACITY_AWARE_SPOT_PLACER
    assert parsed.to_yaml_config()['replica_policy']['spot_placer'] == (
        placement_policy.CAPACITY_AWARE_SPOT_PLACER)


def test_pool_policy_is_nullable_and_case_insensitive():
    disabled = service_spec.SkyServiceSpec.from_yaml_config({
        'pool': {
            'workers': 1,
            'spot_placer': None,
        },
    })
    enabled = service_spec.SkyServiceSpec.from_yaml_config({
        'pool': {
            'workers': 1,
            'spot_placer': 'DYNAMIC_FALLBACK',
        },
    })

    assert disabled.spot_placer is None
    assert 'spot_placer' not in disabled.to_yaml_config()['pool']
    assert enabled.spot_placer == placement_policy.SPOT_HEDGE_PLACER
    assert enabled.to_yaml_config()['pool']['spot_placer'] == (
        placement_policy.SPOT_HEDGE_PLACER)
    persisted_driver = dict(enabled.__dict__[placement_policy.POOL_FIELD])
    assert enabled.to_yaml_config() == enabled.to_yaml_config()
    assert enabled.__dict__[placement_policy.POOL_FIELD] == persisted_driver


def test_pool_policy_copy_override_round_trips_without_stale_nested_driver():
    enabled = service_spec.SkyServiceSpec.from_yaml_config({
        'pool': {
            'workers': 3,
            'spot_placer': 'DYNAMIC_FALLBACK',
        },
    })

    disabled = enabled.copy(spot_placer=None)
    rendered = disabled.to_yaml_config()
    reparsed = service_spec.SkyServiceSpec.from_yaml_config(rendered)

    assert disabled.spot_placer is None
    assert 'spot_placer' not in disabled.__dict__[placement_policy.POOL_FIELD]
    assert 'spot_placer' not in rendered['pool']
    assert not reparsed.placement_contract.enabled
    assert reparsed.placement_contract == disabled.placement_contract


def test_service_policy_is_explicitly_nullable():
    parsed = service_spec.SkyServiceSpec.from_yaml_config({
        'replica_policy': {
            'min_replicas': 0,
            'spot_placer': None,
        },
    })

    assert parsed.spot_placer is None
    assert not parsed.placement_contract.enabled
    assert 'spot_placer' not in parsed.to_yaml_config()['replica_policy']


@pytest.mark.parametrize('pool', ['pool', [], 1])
def test_invalid_pool_representation_fails_before_construction(pool):
    with pytest.raises(ValueError, match='_pool must be'):
        service_spec.SkyServiceSpec(**_base_spec_kwargs(), pool=pool)


@pytest.mark.parametrize('invalid_token', [None, object()])
def test_public_constructor_cannot_inject_historical_contract(invalid_token):
    assert 'placement_contract' not in inspect.signature(
        service_spec.SkyServiceSpec).parameters
    legacy = placement_policy.resolve_legacy_contract(
        placement_policy.CAPACITY_AWARE_SPOT_PLACER,
        pool=False,
        uses_logical_replicas=False)

    with pytest.raises(ValueError, match='internal to SkyServiceSpec.copy'):
        service_spec.SkyServiceSpec(
            **_base_spec_kwargs(),
            spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER,
            _preserved_placement_contract=legacy,
            _placement_contract_copy_token=invalid_token)


def test_current_spec_persists_only_primitive_v2_contract_fields():
    spec = _spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER)

    assert spec.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 2
    assert placement_policy.ROLLBACK_REPLICA_UNIT_FIELD not in spec.__dict__
    assert all(not isinstance(value, placement_policy.PlacementContract)
               for value in spec.__dict__.values())
    assert spec.placement_contract.uses_logical_replicas
    rendered = spec.to_yaml_config()
    assert '_placement_' not in repr(rendered)
    assert '_uses_logical_replicas' not in repr(rendered)


@pytest.mark.parametrize('version', [True, False, 1.0, '1', 0, 3])
def test_contract_writer_rejects_non_exact_versions(version):
    with pytest.raises(TypeError):
        _spec().placement_contract.persisted_fields(version)


def test_preceding_release_per_gpu_pickle_survives_without_removed_class():
    packed = base64.b64decode(_V1_1_1132_PER_GPU_SPEC_ZLIB_B64)
    assert hashlib.sha256(packed).hexdigest() == (
        'abb9674919110ad253fc1e879317375e4f9def218e54d2f136f94c0d72cbeb61')
    serialized = zlib.decompress(packed)
    assert hashlib.sha256(serialized).hexdigest() == (
        '140dc73ce72a3f9f2fef9e1850ba4ea9b8ebd262f31709247caaf039ff70672c')
    assert b'dynamic_fallback_per_gpu' in serialized
    assert b'sky.serve.spot_placer' not in serialized
    assert b'CapacityAwareDynamicFallbackSpotPlacer' not in serialized

    restored = pickle.loads(serialized)

    expected = placement_policy.resolve_fresh_contract(
        placement_policy.CAPACITY_AWARE_SPOT_PLACER, pool=False)
    assert restored.placement_contract == expected
    assert restored.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 2
    assert (placement_policy.ROLLBACK_REPLICA_UNIT_FIELD
            not in restored.__dict__)
    copied = restored.copy()
    assert copied.placement_contract == expected
    assert pickle.loads(pickle.dumps(copied,
                                     protocol=4)).placement_contract == expected


def test_real_pre_marker_pickle_preserves_physical_per_gpu_contract():
    packed = base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64)
    assert hashlib.sha256(packed).hexdigest() == (
        'e340c072d53a82c19f66ac30f8c01f91ff030904eb43e8ba3a8cb8393d97b71a')
    serialized = zlib.decompress(packed)
    assert hashlib.sha256(serialized).hexdigest() == (
        'ce661c241b19fce70d7b6caa13e2f1b46d184a44ea98109e6e7e95025dd228d0')
    assert b'_uses_logical_replicas' not in serialized
    assert b'_placement_' not in serialized
    assert b'sky.serve.spot_placer' not in serialized
    assert b'CapacityAwareDynamicFallbackSpotPlacer' not in serialized

    restored = pickle.loads(serialized)

    contract = restored.placement_contract
    assert contract.is_legacy_physical_per_gpu
    assert contract.reserved_fill_mode == (
        placement_policy.RESERVED_FILL_MODE_SINGLE_GPU_BACKEND)
    copied = restored.copy()
    assert copied.placement_contract == restored.placement_contract
    with pytest.raises(ValueError, match='mirror-free v2'):
        pickle.dumps(copied, protocol=4)
    with pytest.raises(ValueError, match='serialization requires'):
        pickle.dumps(restored, protocol=4)


def test_raw_normalizer_accepts_exact_release_fixtures():
    logical = placement_contract_normalization.analyze_spec_pickle(
        zlib.decompress(base64.b64decode(_V1_1_1132_PER_GPU_SPEC_ZLIB_B64)))
    assert logical.classification is (
        placement_contract_normalization.Classification.FIELDLESS_SUPPORTED)
    assert logical.changed
    assert logical.source_protocol == 4
    assert logical.contract_projection is not None
    assert logical.contract_projection['replica_unit'] == 'logical'
    assert logical.contract_projection['version'] == 2
    assert logical.result_bytes is not None
    normalized = pickle.loads(logical.result_bytes)
    assert normalized.placement_contract.uses_logical_replicas
    assert normalized.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 2
    assert (placement_policy.ROLLBACK_REPLICA_UNIT_FIELD
            not in normalized.__dict__)

    historical = placement_contract_normalization.analyze_spec_pickle(
        zlib.decompress(
            base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64)))
    assert historical.classification is (
        placement_contract_normalization.Classification.
        HISTORICAL_PHYSICAL_PER_GPU)
    assert not historical.changed
    assert historical.result_bytes is not None
    assert historical.source_sha256 == historical.result_sha256


@pytest.mark.parametrize(('policy_name', 'pool', 'logical_marker'), [
    (None, False, False),
    (None, {
        'workers': 1
    }, False),
    (placement_policy.SPOT_HEDGE_PLACER, False, False),
    (placement_policy.SPOT_HEDGE_PLACER, {
        'workers': 1
    }, False),
    (placement_policy.CAPACITY_AWARE_SPOT_PLACER, False, True),
])
def test_raw_normalizer_materializes_all_supported_fieldless_contracts(
        policy_name, pool, logical_marker):
    state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state[placement_policy.POLICY_NAME_FIELD] = policy_name
    state[placement_policy.POOL_FIELD] = pool
    state[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] = logical_marker
    state['_normalizer_unrelated_sentinel'] = ({'ordered': [1, 2]},)
    payload = _raw_spec_pickle(state, protocol=5)

    analysis = placement_contract_normalization.analyze_spec_pickle(payload)

    assert analysis.classification is (
        placement_contract_normalization.Classification.FIELDLESS_SUPPORTED)
    assert analysis.source_protocol == 5
    assert analysis.changed
    assert analysis.result_bytes is not None
    normalized_state = pickle.loads(analysis.result_bytes).__dict__
    assert normalized_state['_normalizer_unrelated_sentinel'] == ({
        'ordered': [1, 2]
    },)
    assert normalized_state[placement_policy.CONTRACT_VERSION_FIELD] == 2
    assert (placement_policy.ROLLBACK_REPLICA_UNIT_FIELD
            not in normalized_state)


@pytest.mark.parametrize('protocol', [4, 5])
def test_raw_normalizer_preserves_none_placeholder(protocol):
    payload = pickle.dumps(None, protocol=protocol)

    analysis = placement_contract_normalization.analyze_spec_pickle(payload)

    assert analysis.classification is (
        placement_contract_normalization.Classification.PLACEHOLDER)
    assert not analysis.changed
    assert analysis.result_bytes == payload


@pytest.mark.parametrize('version', [1, 2])
def test_raw_normalizer_converges_explicit_contract_to_v2(version):
    state = dict(_spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER).__dict__)
    state[placement_policy.CONTRACT_VERSION_FIELD] = version
    if version == 1:
        state[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] = True
    else:
        state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)
    payload = _raw_spec_pickle(state)

    analysis = placement_contract_normalization.analyze_spec_pickle(payload)

    expected = (placement_contract_normalization.Classification.EXPLICIT_V1
                if version == 1 else
                placement_contract_normalization.Classification.EXPLICIT_V2)
    assert analysis.classification is expected
    assert analysis.changed is (version == 1)
    assert analysis.result_bytes is not None
    normalized = pickle.loads(analysis.result_bytes)
    assert normalized.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 2
    assert (placement_policy.ROLLBACK_REPLICA_UNIT_FIELD
            not in normalized.__dict__)
    if version == 2:
        assert analysis.result_bytes == payload


@pytest.mark.parametrize('payload', [
    pickle.dumps('not-a-spec', protocol=4),
    pickle.dumps(None, protocol=3),
    pickle.dumps(None, protocol=0),
    b'not-a-pickle',
])
def test_raw_normalizer_reports_non_spec_and_protocol_blockers(payload):
    analysis = placement_contract_normalization.analyze_spec_pickle(payload)

    assert analysis.blocked
    assert analysis.blocker_reason
    assert analysis.result_bytes is None


def test_raw_normalizer_reports_partial_and_nested_spec_blockers():
    partial = dict(_spec().__dict__)
    partial.pop(placement_policy.CONTRACT_COST_UNIT_FIELD)
    partial_analysis = placement_contract_normalization.analyze_spec_pickle(
        _raw_spec_pickle(partial))
    assert partial_analysis.blocked
    assert 'Partial placement contract' in partial_analysis.blocker_reason

    outer = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        outer.pop(field)
    outer['_nested_spec'] = _spec()
    nested_analysis = placement_contract_normalization.analyze_spec_pickle(
        _raw_spec_pickle(outer))
    assert nested_analysis.blocked
    assert 'exactly one top-level' in nested_analysis.blocker_reason


def test_raw_normalizer_row_classifies_null_spec_instead_of_crashing():
    analysis, classification = (
        placement_contract_normalization._classify_version_row({
            'spec': None,
            'yaml_content': None,
            'retired_at': None,
            'retired_yaml_content': None,
            'retirement_reason': None,
            'retirement_run_id': None,
        }))

    assert classification is (
        placement_contract_normalization.Classification.BLOCKER)
    assert analysis.blocked
    assert analysis.blocker_reason == 'Persisted spec payload is not bytes.'


def test_raw_normalizer_requires_exact_protocol_4_retirement_shape():
    run_id = uuid.uuid4()
    row = {
        'spec': pickle.dumps(None, protocol=5),
        'yaml_content': None,
        'retired_yaml_content': 'service: {}',
        'retired_at': 1.0,
        'retirement_reason':
            (placement_contract_normalization._RETIREMENT_REASON),
        'retirement_run_id': run_id,
    }

    analysis, classification = (
        placement_contract_normalization._classify_version_row(row))

    assert analysis.blocked
    assert classification is (
        placement_contract_normalization.Classification.BLOCKER)

    row['spec'] = pickle.dumps(None, protocol=4)
    analysis, classification = (
        placement_contract_normalization._classify_version_row(row))
    assert not analysis.blocked
    assert classification is (
        placement_contract_normalization.Classification.RETIRED)


def test_retirement_uses_cross_incarnation_target_demand(monkeypatch):
    observed = []

    def old_hash_target_evidence(service_name, version):
        observed.append((service_name, version))
        return demand_state.LiveServiceVersionDemandEvidence(count=1,
                                                             digest='a' * 64)

    monkeypatch.setattr(
        demand_state,
        'get_live_service_version_demand_evidence_any_incarnation',
        old_hash_target_evidence)

    evidence = placement_contract_normalization._image_demand_evidence('svc', 1)
    no_requests = placement_contract_normalization._ExternalEvidence(
        count=0, digest='0' * 64)

    assert observed == [('svc', 1)]
    assert evidence.count == 1
    # No current service hash is accepted by this boundary, so an old-hash,
    # target-scoped owner cannot be hidden by a same-name recreation.
    assert evidence.digest == 'a' * 64
    historical_payload = zlib.decompress(
        base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64))
    candidate = _normalizer_work(historical_payload, 1)
    successor = _normalizer_work(_explicit_v2_payload(), 2)
    service_rows = {'svc': _retirement_service_row()}
    rows = [candidate, successor]
    cleanup_plan = _build_test_cleanup_plan(rows, service_rows)
    receipt_evidence = _test_predecessor_receipt_evidence(set(service_rows))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='live container-image demand'):
        placement_contract_normalization._prepare_retirement_rows(
            rows, service_rows, uuid.uuid4(), 10.0, {('svc', 1): evidence},
            {('svc', 1): no_requests}, no_requests, no_requests,
            _api_pod_identity(), no_requests, cleanup_plan, receipt_evidence)


def test_controller_process_evidence_matches_only_exact_service_argv(
        monkeypatch):
    target = placement_contract_normalization._ProcessTarget(
        'svc', 'incarnation', 7)

    class FakeProcess:

        def __init__(self, pid, cmdline):
            self.pid = pid
            self._cmdline = cmdline

        def status(self):
            return 'running'

        def cmdline(self):
            return self._cmdline

        def create_time(self):
            return 100.0 + self.pid

        def ppid(self):
            return 10

    processes = [
        FakeProcess(1, [
            'python', '-m', 'sky.serve.service', '--service-name', 'svc',
            '--workspace', 'workspace', '--service-incarnation', 'incarnation'
        ]),
        FakeProcess(2, [
            'sky.serve.controller --service-name svc '
            '--service-incarnation incarnation', ''
        ]),
        # A malformed unrelated compatibility process is outside this exact
        # target and must not make retirement of svc impossible.
        FakeProcess(
            3,
            ['python', '-m', 'sky.serve.service', '--service-name', 'other']),
        FakeProcess(4, ['sky.serve.service', '--service-name', 'svc-prefix']),
    ]
    monkeypatch.setattr(placement_contract_normalization.psutil, 'process_iter',
                        lambda: iter(processes))

    evidence = (
        placement_contract_normalization._serve_controller_process_evidence(
            frozenset({target}), 'pod-a'))

    assert evidence.count == 2
    assert len(evidence.digest) == 64
    assert evidence.digest != (
        placement_contract_normalization._serve_controller_process_evidence(
            frozenset({target}), 'pod-b').digest)


@pytest.mark.parametrize(
    'cmdline', [[
        'python', '-m', 'sky.serve.service', '--service-name=svc',
        '--service-incarnation', 'incarnation'
    ],
                [
                    'python', '-m', 'sky.serve.service', '--service-name',
                    'svc', '--service-incarnation', 'wrong'
                ],
                [
                    'sky.serve.controller --service-name svc '
                    '--service-incarnation incarnation', 'trailing'
                ]])
def test_controller_process_evidence_rejects_malformed_target(
        monkeypatch, cmdline):
    target = placement_contract_normalization._ProcessTarget(
        'svc', 'incarnation', 7)

    class FakeProcess:
        pid = 1

        def status(self):
            return 'running'

        def cmdline(self):
            return cmdline

        def create_time(self):
            return 1.0

        def ppid(self):
            return 0

    monkeypatch.setattr(placement_contract_normalization.psutil, 'process_iter',
                        lambda: iter([FakeProcess()]))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker):
        placement_contract_normalization._serve_controller_process_evidence(
            frozenset({target}), 'pod-a')


def test_controller_process_evidence_access_and_overflow_block(monkeypatch):
    target = placement_contract_normalization._ProcessTarget(
        'svc', 'incarnation', 7)

    class AccessDeniedProcess:
        pid = 1

        def status(self):
            raise placement_contract_normalization.psutil.AccessDenied(pid=1)

    monkeypatch.setattr(placement_contract_normalization.psutil, 'process_iter',
                        lambda: iter([AccessDeniedProcess()]))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='cannot read every'):
        placement_contract_normalization._serve_controller_process_evidence(
            frozenset({target}), 'pod-a')

    monkeypatch.setattr(placement_contract_normalization,
                        '_MAX_PROCESS_EVIDENCE_ROWS', 1)

    class ZombieProcess:

        def status(self):
            return placement_contract_normalization.psutil.STATUS_ZOMBIE

    monkeypatch.setattr(
        placement_contract_normalization.psutil, 'process_iter',
        lambda: iter([ZombieProcess(), ZombieProcess()]))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='explicit row bound'):
        placement_contract_normalization._serve_controller_process_evidence(
            frozenset({target}), 'pod-a')


def test_process_evidence_rejects_nonzero_malformed_and_drift():
    zero = placement_contract_normalization._ExternalEvidence(0, '0' * 64)
    nonzero = placement_contract_normalization._ExternalEvidence(1, '0' * 64)
    drifted = placement_contract_normalization._ExternalEvidence(0, '1' * 64)

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='not quiescent'):
        placement_contract_normalization._require_stable_zero_evidence(
            zero, nonzero, 'processes')
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='changed during apply'):
        placement_contract_normalization._require_stable_zero_evidence(
            zero, drifted, 'processes')
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='malformed evidence'):
        placement_contract_normalization._validate_external_evidence(
            placement_contract_normalization._ExternalEvidence(0, 'BAD'),
            'processes')


def _resource_action_row(raw_spec, *, domain='serve', resource_type='replica'):
    actions = resource_action_fixtures.actions
    typed = actions.ServeReplicaActionSpecV1.from_value(raw_spec)
    invocation = typed.invocation
    identity = invocation.resource_identity
    return {
        'action_id': typed.action_id,
        'domain': domain,
        'resource_type': resource_type,
        'resource_identity': identity.action_identity(invocation.action_kind
                                                     ).resource_identity,
        'desired_generation': identity.desired_generation,
        'action_type': invocation.action_kind.value,
        'immutable_spec': typed.canonical_value(),
        'immutable_spec_sha256': typed.sha256,
    }


def test_resource_action_evidence_matches_exact_launch_source_any_incarnation():
    target = placement_contract_normalization._ResourceActionTarget(
        'svc', 3, 'different-current-hash')
    launch_row = _resource_action_row(resource_action_fixtures._launch_spec())
    down_row = _resource_action_row(resource_action_fixtures._down_spec())

    launch_evidence = (
        placement_contract_normalization._resource_action_evidence_from_rows(
            [('api_resource_actions', launch_row),
             ('api_resource_actions', down_row)], frozenset({target})))

    assert launch_evidence[('svc', 3)].count == 1
    assert len(launch_evidence[('svc', 3)].digest) == 64
    unrelated = placement_contract_normalization._ResourceActionTarget(
        'svc', 2, 'different-current-hash')
    assert placement_contract_normalization._resource_action_evidence_from_rows(
        [('api_resource_actions', down_row)],
        frozenset({unrelated}))[('svc', 2)].count == 0


def test_resource_action_evidence_rejects_malformed_possible_serve_root():
    target = placement_contract_normalization._ResourceActionTarget(
        'svc', 3, 'current-hash')
    corrupted_outer = _resource_action_row(
        resource_action_fixtures._launch_spec(), domain='other')
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='differs from its typed spec'):
        placement_contract_normalization._resource_action_evidence_from_rows(
            [('api_resource_actions', corrupted_outer)], frozenset({target}))

    malformed = dict(corrupted_outer,
                     domain='serve',
                     immutable_spec={'invalid': True},
                     immutable_spec_sha256='0' * 64)
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='unparseable possible Serve'):
        placement_contract_normalization._resource_action_evidence_from_rows(
            [('api_resource_actions', malformed)], frozenset({target}))


def test_resource_action_evidence_validates_and_ignores_nonserve_root():
    target = placement_contract_normalization._ResourceActionTarget(
        'svc', 3, 'current-hash')
    immutable_spec = {'version': 1, 'foreign': 'contract'}
    foreign = {
        'action_id': uuid.uuid4(),
        'domain': 'foreign',
        'resource_type': 'worker',
        'resource_identity': 'worker-a',
        'desired_generation': 1,
        'action_type': 'launch',
        'immutable_spec': immutable_spec,
        'immutable_spec_sha256':
            (resource_action_fixtures.kernel_actions.canonical_sha256(
                immutable_spec)),
    }

    evidence = (
        placement_contract_normalization._resource_action_evidence_from_rows(
            [('api_resource_actions', foreign)], frozenset({target})))
    empty = (
        placement_contract_normalization._resource_action_evidence_from_rows(
            [], frozenset({target})))

    assert evidence[('svc', 3)].count == 0
    assert evidence[('svc', 3)].digest != empty[('svc', 3)].digest


def test_sole_recreate_api_pod_requires_current_registry_identity(monkeypatch):
    instance_id = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
    monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'all')
    monkeypatch.setenv('SKYPILOT_POD_UID', 'pod-a')
    monkeypatch.setenv('SKYPILOT_API_SERVER_INSTANCE_ID', str(instance_id))
    monkeypatch.delenv('SKYPILOT_ROLLING_UPDATE_ENABLED', raising=False)
    row = {
        'instance_id': instance_id,
        'role': 'all',
        'pod_uid': 'pod-a',
        'ready': True,
        'draining_at': None,
    }
    monkeypatch.setattr(placement_contract_normalization,
                        '_fresh_api_instances', lambda _engine: [row])

    identity = placement_contract_normalization._require_sole_recreate_api_pod(
        mock.Mock())

    assert identity.pod_uid == 'pod-a'
    assert identity.instance_id == instance_id
    assert len(identity.digest) == 64

    # A second fresh registry member blocks even if it is already draining;
    # it may still host a controller process in another pod.
    second = dict(row,
                  instance_id=uuid.uuid4(),
                  pod_uid='pod-b',
                  ready=False,
                  draining_at=1.0)
    monkeypatch.setattr(placement_contract_normalization,
                        '_fresh_api_instances', lambda _engine: [row, second])
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='exactly one fresh registered'):
        placement_contract_normalization._require_sole_recreate_api_pod(
            mock.Mock())


def test_sole_recreate_api_pod_rejects_wrong_instance_and_rolling(monkeypatch):
    instance_id = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
    monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'all')
    monkeypatch.setenv('SKYPILOT_POD_UID', 'pod-a')
    monkeypatch.setenv('SKYPILOT_API_SERVER_INSTANCE_ID', str(instance_id))
    monkeypatch.setattr(
        placement_contract_normalization, '_fresh_api_instances',
        lambda _engine: [{
            'instance_id': uuid.uuid4(),
            'role': 'all',
            'pod_uid': 'pod-a',
            'ready': True,
            'draining_at': None,
        }])
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='does not match'):
        placement_contract_normalization._require_sole_recreate_api_pod(
            mock.Mock())

    monkeypatch.setenv('SKYPILOT_ROLLING_UPDATE_ENABLED', 'true')
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='Recreate'):
        placement_contract_normalization._require_sole_recreate_api_pod(
            mock.Mock())


def test_legacy_controller_evidence_covers_every_server_identity(monkeypatch):
    first_inventory = {
        'ordinary-cluster': ('UP', 1),
        'sky-serve-controller-old-server': ('DOWN', 2),
        'sky-serve-controller-current-server': ('UP', 3),
    }
    monkeypatch.setattr(placement_contract_normalization.global_user_state,
                        'get_cluster_status_fields_by_prefix',
                        lambda _prefix, *, row_limit: first_inventory)

    first = (placement_contract_normalization.
             _legacy_serve_controller_cluster_evidence())

    assert first.count == 2
    monkeypatch.setattr(
        placement_contract_normalization.global_user_state,
        'get_cluster_status_fields_by_prefix',
        lambda _prefix, *, row_limit: dict(
            reversed(tuple(first_inventory.items()))))
    second = (placement_contract_normalization.
              _legacy_serve_controller_cluster_evidence())
    assert second == first


def test_cleanup_plan_rejects_scope_under_wrong_metadata_key():
    intents, rows, service_rows = _single_cleanup_plan_inputs()
    intents[0]['yaml_content'] = intents[0]['yaml_content'].replace(
        '_metadata:', 'metadata:', 1)

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='wrong Task metadata field'):
        placement_contract_normalization._build_cleanup_intent_plan(
            intents, rows, service_rows, row_bound=1)


@pytest.mark.parametrize(('field', 'value', 'match'), [
    ('pool', False, 'exact non-pool parent bit'),
    ('provisional', False, 'invalid provisional bit'),
])
def test_cleanup_plan_rejects_false_lookalike_integer_fields(
        field, value, match):
    intents, rows, service_rows = _single_cleanup_plan_inputs()
    intents[0][field] = value

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match=match):
        placement_contract_normalization._build_cleanup_intent_plan(
            intents, rows, service_rows, row_bound=1)


def test_cleanup_plan_rejects_yaml_scope_mismatch():
    intents, rows, service_rows = _single_cleanup_plan_inputs()
    intents[0]['yaml_content'] = _zero_target_cleanup_yaml(
        'different-scope', intents[0]['storage_generation'])

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='YAML scope disagrees'):
        placement_contract_normalization._build_cleanup_intent_plan(
            intents, rows, service_rows, row_bound=1)


def test_cleanup_plan_rejects_duplicate_yaml_version_mapping():
    intents, rows, service_rows = _single_cleanup_plan_inputs()
    duplicate_yaml = intents[0]['yaml_content']
    rows[1].original['yaml_content'] = duplicate_yaml
    rows[1].result['yaml_content'] = duplicate_yaml

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='generation is reused'):
        placement_contract_normalization._build_cleanup_intent_plan(
            intents, rows, service_rows, row_bound=1)


def test_cleanup_plan_rejects_generation_reused_by_different_version_yaml():
    intents, rows, service_rows = _single_cleanup_plan_inputs()
    reused_generation_yaml = intents[0][
        'yaml_content'] + 'envs:\n  TEST: value\n'
    rows[1].original['yaml_content'] = reused_generation_yaml
    rows[1].result['yaml_content'] = reused_generation_yaml

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='generation|multiple live'):
        placement_contract_normalization._build_cleanup_intent_plan(
            intents, rows, service_rows, row_bound=1)


def test_cleanup_plan_rejects_future_lifecycle_epoch():
    intents, rows, service_rows = _single_cleanup_plan_inputs()
    intents[0]['lifecycle_epoch'] = service_rows['svc']['lifecycle_epoch'] + 1

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='future lifecycle epoch'):
        placement_contract_normalization._build_cleanup_intent_plan(
            intents, rows, service_rows, row_bound=1)


def test_cleanup_plan_rejects_nonzero_deletion_target():
    intents, rows, service_rows = _single_cleanup_plan_inputs()
    intents[0]['yaml_content'] += 'workdir: /owned/workdir\n'

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='nonzero deletion target'):
        placement_contract_normalization._build_cleanup_intent_plan(
            intents, rows, service_rows, row_bound=1)


def test_multirow_retirement_proves_a_surviving_successor():
    historical_payload = zlib.decompress(
        base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64))
    historical_one = _normalizer_work(historical_payload, 1)
    historical_two = _normalizer_work(historical_payload, 2)
    successor = _normalizer_work(_explicit_v2_payload(), 3)
    rows = [historical_one, historical_two, successor]
    service_rows = {'svc': _retirement_service_row(current_version=3)}
    cleanup_plan = _build_test_cleanup_plan(rows, service_rows)
    receipt_evidence = _test_predecessor_receipt_evidence(set(service_rows))
    historical_generations = {
        ephemeral_storage_contract.parse_ephemeral_storage_scope(
            row.original['yaml_content']).storage_generation
        for row in (historical_one, historical_two)
    }
    assert historical_generations == {'generation-1', 'generation-2'}
    no_demand = placement_contract_normalization._ExternalEvidence(count=0,
                                                                   digest='0' *
                                                                   64)

    affected = placement_contract_normalization._prepare_retirement_rows(
        rows, service_rows, uuid.uuid4(), 10.0, {
            ('svc', 1): no_demand,
            ('svc', 2): no_demand,
        }, {
            ('svc', 1): no_demand,
            ('svc', 2): no_demand,
        }, no_demand, no_demand, _api_pod_identity(), no_demand, cleanup_plan,
        receipt_evidence)

    assert affected == {'svc'}
    assert historical_one.dependency_facts[
        'strictly_newer_committed_version'] == 3
    assert historical_two.dependency_facts[
        'strictly_newer_committed_version'] == 3
    assert historical_one.dependency_facts['recovery_version'] == 3
    assert historical_two.dependency_facts['recovery_version'] == 3
    assert historical_one.dependency_facts['process_quiescence_count'] == 0
    assert historical_one.dependency_facts[
        'process_quiescence_sha256'] == '0' * 64
    assert historical_one.dependency_facts[
        'serve_consolidation_mode_proved'] is True
    assert historical_one.dependency_facts['parent_non_pool_proved'] is True
    assert historical_one.dependency_facts[
        'resource_action_mode_legacy_inert'] is True
    ledger_entry = {
        'version': 1,
        'dependency_facts': historical_one.dependency_facts,
    }
    assert (
        placement_contract_normalization._retirement_ledger_facts_are_complete(
            ledger_entry, protocol=2))
    for field, contradictory_value in (
        ('service_pool', 1),
        ('service_resource_action_mode', 'shadow'),
        ('service_resource_action_mode_changed_at', 1.0),
    ):
        tampered_facts = dict(historical_one.dependency_facts)
        tampered_facts[field] = contradictory_value
        assert not (placement_contract_normalization.
                    _retirement_ledger_facts_are_complete(
                        {
                            'version': 1,
                            'dependency_facts': tampered_facts,
                        },
                        protocol=2))
    for field, false_lookalike in (
        ('replica_count', False),
        ('cleanup_candidate_deletion_target_count', False),
        ('cleanup_intent_deletion_target_count', False),
        ('cleanup_intent_count', True),
        ('cleanup_intent_inventory_count', True),
        ('cleanup_intent_adopted_count', False),
        ('cleanup_match_inventory_count', True),
        ('cleanup_candidate_match_count', True),
        ('predecessor_receipt_inventory_count', False),
        ('approved_loaded_image_commit_count', True),
    ):
        tampered_facts = dict(historical_one.dependency_facts)
        tampered_facts[field] = false_lookalike
        assert not (placement_contract_normalization.
                    _retirement_ledger_facts_are_complete(
                        {
                            'version': 1,
                            'dependency_facts': tampered_facts,
                        },
                        protocol=2)), field
    assert historical_one.result['yaml_content'] is None
    assert historical_two.result['yaml_content'] is None
    assert successor.result['yaml_content'] == 'service: {}'


@pytest.mark.parametrize(('column', 'value', 'fact', 'fact_value', 'match'), [
    ('placement_catalog', {}, None, None, 'placement catalog activation'),
    (None, None, 'cleanup_intent_count', 2, 'service-wide intent inventory'),
    (None, None, 'replica_count', 1, 'owns replica rows'),
    (None, None, 'unknown_version_replica_count', 1,
     'NULL or orphan-version replica rows'),
    ('controller_config', b'{}', None, None,
     'incomplete staged controller configuration'),
])
def test_retirement_rejects_catalog_cleanup_and_bridge_dependencies(
        column, value, fact, fact_value, match):
    historical_payload = zlib.decompress(
        base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64))
    candidate = _normalizer_work(historical_payload, 1)
    successor = _normalizer_work(_explicit_v2_payload(), 2)
    if column is not None:
        candidate.original[column] = value
        candidate.result[column] = value
    service_rows = {'svc': _retirement_service_row()}
    rows = [candidate, successor]
    cleanup_plan = _build_test_cleanup_plan(rows, service_rows)
    receipt_evidence = _test_predecessor_receipt_evidence(set(service_rows))
    if fact is not None:
        candidate.dependency_facts[fact] = fact_value
    no_evidence = placement_contract_normalization._ExternalEvidence(
        count=0, digest='0' * 64)

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match=match):
        placement_contract_normalization._prepare_retirement_rows(
            rows, service_rows, uuid.uuid4(), 10.0, {('svc', 1): no_evidence},
            {('svc', 1): no_evidence}, no_evidence, no_evidence,
            _api_pod_identity(), no_evidence, cleanup_plan, receipt_evidence)


def test_retirement_rejects_same_service_placeholder_reservation():
    historical_payload = zlib.decompress(
        base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64))
    candidate = _normalizer_work(historical_payload, 1)
    successor = _normalizer_work(_explicit_v2_payload(), 2)
    placeholder = _normalizer_work(pickle.dumps(None, protocol=4),
                                   3,
                                   yaml_content=None)
    service_rows = {'svc': _retirement_service_row()}
    rows = [candidate, successor, placeholder]
    cleanup_plan = _build_test_cleanup_plan(rows, service_rows)
    receipt_evidence = _test_predecessor_receipt_evidence(set(service_rows))
    no_evidence = placement_contract_normalization._ExternalEvidence(
        count=0, digest='0' * 64)

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='placeholder or reservation'):
        placement_contract_normalization._prepare_retirement_rows(
            rows, service_rows, uuid.uuid4(), 10.0, {('svc', 1): no_evidence},
            {('svc', 1): no_evidence}, no_evidence, no_evidence,
            _api_pod_identity(), no_evidence, cleanup_plan, receipt_evidence)


@pytest.mark.parametrize(('parent_delta', 'match'), [
    ({
        'pool': 1
    }, 'exact non-pool parent'),
    ({
        'resource_action_mode': 'shadow',
        'resource_action_mode_changed_at': 1.0,
    }, 'inert legacy default'),
    ({
        'resource_action_mode': 'authoritative',
        'resource_action_mode_changed_at': 1.0,
    }, 'inert legacy default'),
])
def test_retirement_rejects_pool_or_active_resource_action_parent(
        parent_delta, match):
    historical_payload = zlib.decompress(
        base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64))
    candidate = _normalizer_work(historical_payload, 1)
    successor = _normalizer_work(_explicit_v2_payload(), 2)
    service = _retirement_service_row()
    service.update(parent_delta)
    service_rows = {'svc': service}
    rows = [candidate, successor]
    receipt_evidence = _test_predecessor_receipt_evidence(set(service_rows))
    no_evidence = placement_contract_normalization._ExternalEvidence(
        count=0, digest='0' * 64)

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match=match):
        cleanup_plan = _build_test_cleanup_plan(rows, service_rows)
        placement_contract_normalization._prepare_retirement_rows(
            rows, service_rows, uuid.uuid4(), 10.0, {('svc', 1): no_evidence},
            {('svc', 1): no_evidence}, no_evidence, no_evidence,
            _api_pod_identity(), no_evidence, cleanup_plan, receipt_evidence)


def test_terminal_service_normalizes_without_unloadable_receipt():
    state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    row = _normalizer_work(_raw_spec_pickle(state), 1)

    affected = placement_contract_normalization._prepare_supported_rows(
        [row], {'svc': {
            'status': 'SHUTTING_DOWN',
        }})

    assert row.outcome == 'changed'
    assert affected == set()


def test_supported_apply_converts_fieldless_and_explicit_v1_to_v2():
    fieldless_state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        fieldless_state.pop(field)
    fieldless = _normalizer_work(_raw_spec_pickle(fieldless_state), 1)
    v1_spec = _spec()
    explicit_v1_state = dict(v1_spec.__dict__)
    explicit_v1_state.update(
        v1_spec.placement_contract._legacy_v1_persisted_fields())
    explicit_v1_state[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] = False
    explicit_v1 = _normalizer_work(_raw_spec_pickle(explicit_v1_state), 2)

    affected = placement_contract_normalization._prepare_supported_rows(
        [fieldless, explicit_v1], {
            'svc': {
                'status': 'READY',
                'current_version': 2,
                'pool': 0,
                'logical_replica_semantics': 0,
            }
        })

    assert affected == {'svc'}
    for row in (fieldless, explicit_v1):
        assert row.outcome == 'changed'
        state = pickle.loads(row.result['spec']).__dict__
        assert state[placement_policy.CONTRACT_VERSION_FIELD] == 2
        assert placement_policy.ROLLBACK_REPLICA_UNIT_FIELD not in state
        assert row.dependency_facts['receipt_current_version'] == 2
        assert row.dependency_facts['receipt_recovery_version'] == 2
        assert row.dependency_facts['receipt_loadable_result_proved'] is True
        assert row.dependency_facts['receipt_parent_contract_proved'] is True


def test_supported_receipt_rejects_parent_logical_fence_mismatch():
    fieldless_state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        fieldless_state.pop(field)
    fieldless = _normalizer_work(_raw_spec_pickle(fieldless_state), 1)

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='durable logical-replica fence'):
        placement_contract_normalization._prepare_supported_rows(
            [fieldless], {
                'svc': {
                    'status': 'READY',
                    'current_version': 1,
                    'pool': 0,
                    'logical_replica_semantics': 1,
                }
            })


def test_supported_receipt_rejects_recovery_logical_fence_mismatch():
    fieldless_state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        fieldless_state.pop(field)
    fieldless = _normalizer_work(_raw_spec_pickle(fieldless_state), 1)
    recovery = _normalizer_work(_explicit_v2_payload(), 2)
    recovery.original['controller_applied_at'] = 1.0
    recovery.result['controller_applied_at'] = 1.0
    current = _normalizer_work(
        _explicit_v2_payload(placement_policy.CAPACITY_AWARE_SPOT_PLACER), 3)
    current.original['quarantined_at'] = 2.0
    current.result['quarantined_at'] = 2.0

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='quarantine-aware recovery target disagrees'):
        placement_contract_normalization._prepare_supported_rows(
            [fieldless, recovery, current], {
                'svc': {
                    'status': 'READY',
                    'current_version': 3,
                    'pool': 0,
                    'logical_replica_semantics': 1,
                }
            })


@pytest.mark.parametrize('unloadable_current', ['placeholder', 'historical'])
def test_supported_apply_rejects_unloadable_current_receipt_target(
        unloadable_current):
    fieldless_state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        fieldless_state.pop(field)
    fieldless = _normalizer_work(_raw_spec_pickle(fieldless_state), 1)
    if unloadable_current == 'placeholder':
        current = _normalizer_work(pickle.dumps(None, protocol=4),
                                   2,
                                   yaml_content=None)
    else:
        current = _normalizer_work(
            zlib.decompress(
                base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64)), 2)

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='current_version is not a surviving committed'):
        placement_contract_normalization._prepare_supported_rows(
            [fieldless, current],
            {'svc': {
                'status': 'READY',
                'current_version': 2,
            }})


def test_supported_apply_rejects_historical_recovery_receipt_target():
    fieldless_state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        fieldless_state.pop(field)
    fieldless = _normalizer_work(_raw_spec_pickle(fieldless_state), 1)
    historical = _normalizer_work(
        zlib.decompress(
            base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64)), 2)
    historical.original['controller_applied_at'] = 1.0
    historical.result['controller_applied_at'] = 1.0
    current = _normalizer_work(_explicit_v2_payload(), 3)
    quarantined = _normalizer_work(_explicit_v2_payload(), 4)
    quarantined.original['quarantined_at'] = 2.0
    quarantined.result['quarantined_at'] = 2.0

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='quarantine-aware recovery version'):
        placement_contract_normalization._prepare_supported_rows(
            [fieldless, historical, current, quarantined],
            {'svc': {
                'status': 'READY',
                'current_version': 3,
            }})


def test_normalization_ledger_records_current_parent_identity():
    row = _normalizer_work(_explicit_v2_payload(), 1)
    session = mock.Mock()
    run_id = uuid.uuid4()

    placement_contract_normalization._insert_ledger(
        session, [row],
        run_id=run_id,
        mode=placement_contract_normalization.ApplyMode.SUPPORTED,
        row_bound=1,
        started_at=1.0,
        completed_at=2.0,
        freeze_evidence_sha256='f' * 64,
        pre_digest='a' * 64,
        post_digest='b' * 64)

    ledger_values = session.execute.call_args_list[1].args[1]
    assert ledger_values[0]['service_hash'] == 'current-hash'
    assert ledger_values[0]['service_lifecycle_epoch'] == 7


def test_apply_rejects_prior_ledger_drift():
    mismatch = ({
        'service_name': 'svc',
        'version': 1,
        'last_completed_at': 1.0,
    },)

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='Prior normalization ledger does not match'):
        placement_contract_normalization._require_prior_ledger_consistency(
            mismatch)

    placement_contract_normalization._require_prior_ledger_consistency(({
        'service_name': 'svc',
        'version': 2,
        'reason': 'untracked_current_row',
    },))
    placement_contract_normalization._require_prior_ledger_consistency(({
        'service_name': 'svc',
        'version': 3,
        'reason': 'tracked_row_absent_from_current_inventory',
    },))


def test_postgres_inventory_blocks_live_parent_workload_kind_mismatch(
        empty_postgres):
    engine = empty_postgres
    serve_state.Base.metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name='svc',
            workspace='workspace',
            status='READY',
            current_version=1,
            active_versions='[1]',
            pool=1,
            hash='service-hash',
            lifecycle_epoch=1,
            resource_scope='service-hash'))
        connection.execute(serve_state.version_specs_table.insert().values(
            service_name='svc',
            version=1,
            spec=_explicit_v2_payload(),
            yaml_content='service: {}',
            created_at=1.0,
            created_by='test'))

    result = placement_contract_normalization.run_operator(engine=engine,
                                                           mode=None,
                                                           row_bound=10)

    assert result.classification_counts == {'blocker': 1}
    assert result.blockers[0]['reason'].startswith(
        'Persisted contract workload kind disagrees with its live parent')


def test_postgres_operator_apply_rerun_new_row_and_cas_rollback(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    serve_state.Base.metadata.create_all(engine)
    fieldless_state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        fieldless_state.pop(field)
    fieldless_payload = _raw_spec_pickle(fieldless_state)
    no_evidence = placement_contract_normalization._ExternalEvidence(
        count=0, digest='0' * 64)

    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name='svc',
            workspace='workspace',
            status='READY',
            current_version=1,
            active_versions='[1]',
            hash='service-hash',
            lifecycle_epoch=1,
            resource_scope='service-hash'))
        connection.execute(serve_state.version_specs_table.insert().values(
            service_name='svc',
            version=1,
            spec=fieldless_payload,
            yaml_content='service: {}',
            created_at=1.0,
            created_by='test'))

    run = placement_contract_normalization.run_operator(
        engine=engine,
        mode=placement_contract_normalization.ApplyMode.SUPPORTED,
        row_bound=10,
        freeze_evidence_sha256='f' * 64,
        request_evidence_getter=lambda _engine: no_evidence,
        api_pod_checker=lambda _engine: _api_pod_identity())
    assert run.changed_rows == 1
    assert run.prior_ledger_mismatches == ()
    with engine.connect() as connection:
        normalized_payload = connection.execute(
            sqlalchemy.select(serve_state.version_specs_table.c.spec).where(
                serve_state.version_specs_table.c.service_name == 'svc',
                serve_state.version_specs_table.c.version == 1)).scalar_one()
    normalized_state = pickle.loads(normalized_payload).__dict__
    assert normalized_state[placement_policy.CONTRACT_VERSION_FIELD] == 2
    assert placement_policy.ROLLBACK_REPLICA_UNIT_FIELD not in normalized_state

    # A mutable, non-spec operational column may legitimately change after a
    # manifest.  Prior consistency fences only the exact result spec for the
    # same service incarnation; the next run records a fresh row snapshot.
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.update().where(
            serve_state.version_specs_table.c.service_name == 'svc',
            serve_state.version_specs_table.c.version == 1).values(
                placement_catalog={'candidate': 'updated'}))
    rerun = placement_contract_normalization.run_operator(
        engine=engine,
        mode=placement_contract_normalization.ApplyMode.SUPPORTED,
        row_bound=10,
        freeze_evidence_sha256='e' * 64,
        request_evidence_getter=lambda _engine: no_evidence,
        api_pod_checker=lambda _engine: _api_pod_identity())
    assert rerun.changed_rows == 0
    assert rerun.prior_ledger_mismatches == ()

    # A fieldless row committed after the prior inventory is not grandfathered
    # by that manifest: the next run classifies and normalizes it.  Force its
    # CAS to miss once and prove the ledger plus all writes roll back together.
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.insert().values(
            service_name='svc',
            version=2,
            spec=fieldless_payload,
            yaml_content='service: {}',
            created_at=2.0,
            created_by='test'))
        connection.execute(serve_state.services_table.update().where(
            serve_state.services_table.c.name == 'svc').values(
                current_version=2, active_versions='[2]'))
    untracked_dry_run = placement_contract_normalization.run_operator(
        engine=engine, mode=None, row_bound=10)
    assert [
        mismatch['reason']
        for mismatch in untracked_dry_run.prior_ledger_mismatches
    ] == ['untracked_current_row']
    original_cas = placement_contract_normalization._cas_version_result

    def force_changed_row_cas_miss(session, row):
        if row.outcome == 'changed':
            session.execute(serve_state.version_specs_table.update().where(
                serve_state.version_specs_table.c.service_name ==
                row.identity[0], serve_state.version_specs_table.c.version ==
                row.identity[1]).values(spec=_explicit_v2_payload()))
        original_cas(session, row)

    monkeypatch.setattr(placement_contract_normalization, '_cas_version_result',
                        force_changed_row_cas_miss)
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='Version CAS failed'):
        placement_contract_normalization.run_operator(
            engine=engine,
            mode=placement_contract_normalization.ApplyMode.SUPPORTED,
            row_bound=10,
            freeze_evidence_sha256='d' * 64,
            request_evidence_getter=lambda _engine: no_evidence,
            api_pod_checker=lambda _engine: _api_pod_identity())
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state.placement_normalization_runs_table)).scalar_one(
                ) == 2
        assert connection.execute(
            sqlalchemy.select(serve_state.version_specs_table.c.spec).where(
                serve_state.version_specs_table.c.service_name == 'svc',
                serve_state.version_specs_table.c.version ==
                2)).scalar_one() == fieldless_payload

    monkeypatch.setattr(placement_contract_normalization, '_cas_version_result',
                        original_cas)
    new_row_run = placement_contract_normalization.run_operator(
        engine=engine,
        mode=placement_contract_normalization.ApplyMode.SUPPORTED,
        row_bound=10,
        freeze_evidence_sha256='c' * 64,
        request_evidence_getter=lambda _engine: no_evidence,
        api_pod_checker=lambda _engine: _api_pod_identity())
    assert new_row_run.changed_rows == 1
    assert new_row_run.run_id is not None
    assert [
        mismatch['reason'] for mismatch in new_row_run.prior_ledger_mismatches
    ] == ['untracked_current_row']
    with engine.connect() as connection:
        ledger_row = connection.execute(
            sqlalchemy.select(
                serve_state.placement_normalization_rows_table.c.classification,
                serve_state.placement_normalization_rows_table.c.outcome,
            ).where(
                serve_state.placement_normalization_rows_table.c.run_id ==
                uuid.UUID(new_row_run.run_id),
                serve_state.placement_normalization_rows_table.c.version ==
                2)).one()
    assert tuple(ledger_row) == ('fieldless_supported', 'changed')

    # New explicit-v2 and uncommitted placeholder rows need no rewrite, but
    # they are still captured by the next complete fleet manifest.
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.insert(), [{
            'service_name': 'svc',
            'version': 3,
            'spec': pickle.dumps(None, protocol=4),
            'yaml_content': None,
            'created_at': 3.0,
            'created_by': 'test',
        }, {
            'service_name': 'svc',
            'version': 4,
            'spec': _explicit_v2_payload(),
            'yaml_content': 'service: {}',
            'created_at': 4.0,
            'created_by': 'test',
        }])
    manifest_dry_run = placement_contract_normalization.run_operator(
        engine=engine, mode=None, row_bound=10)
    assert [
        mismatch['reason']
        for mismatch in manifest_dry_run.prior_ledger_mismatches
    ] == ['untracked_current_row', 'untracked_current_row']
    manifest_run = placement_contract_normalization.run_operator(
        engine=engine,
        mode=placement_contract_normalization.ApplyMode.SUPPORTED,
        row_bound=10,
        freeze_evidence_sha256='b' * 64,
        request_evidence_getter=lambda _engine: no_evidence,
        api_pod_checker=lambda _engine: _api_pod_identity())
    assert manifest_run.changed_rows == 0
    assert manifest_run.run_id is not None
    assert [
        mismatch['reason'] for mismatch in manifest_run.prior_ledger_mismatches
    ] == ['untracked_current_row', 'untracked_current_row']
    with engine.connect() as connection:
        new_entries = connection.execute(
            sqlalchemy.select(
                serve_state.placement_normalization_rows_table.c.version,
                serve_state.placement_normalization_rows_table.c.classification,
                serve_state.placement_normalization_rows_table.c.outcome,
            ).where(
                serve_state.placement_normalization_rows_table.c.run_id ==
                uuid.UUID(manifest_run.run_id),
                serve_state.placement_normalization_rows_table.c.version.in_([
                    3, 4
                ])).order_by(serve_state.placement_normalization_rows_table.c.
                             version)).all()
    assert [tuple(entry) for entry in new_entries] == [
        (3, 'placeholder', 'unchanged'),
        (4, 'explicit_v2', 'unchanged'),
    ]
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.delete().where(
            serve_state.version_specs_table.c.service_name == 'svc',
            serve_state.version_specs_table.c.version == 4))
    removed_row_dry_run = placement_contract_normalization.run_operator(
        engine=engine, mode=None, row_bound=10)
    assert [
        mismatch['reason']
        for mismatch in removed_row_dry_run.prior_ledger_mismatches
    ] == ['tracked_row_absent_from_current_inventory']
    removed_row_manifest = placement_contract_normalization.run_operator(
        engine=engine,
        mode=placement_contract_normalization.ApplyMode.SUPPORTED,
        row_bound=10,
        freeze_evidence_sha256='a' * 64,
        request_evidence_getter=lambda _engine: no_evidence,
        api_pod_checker=lambda _engine: _api_pod_identity())
    assert [
        mismatch['reason']
        for mismatch in removed_row_manifest.prior_ledger_mismatches
    ] == ['tracked_row_absent_from_current_inventory']
    assert placement_contract_normalization.run_operator(
        engine=engine, mode=None, row_bound=10).prior_ledger_mismatches == ()


def test_postgres_operator_retires_only_historical_row_and_keeps_high_watermark(
        empty_postgres):
    engine = empty_postgres
    serve_state.Base.metadata.create_all(engine)
    historical_payload = zlib.decompress(
        base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64))
    successor_payload = _explicit_v2_payload()
    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name='svc',
            workspace='workspace',
            status='READY',
            current_version=2,
            active_versions='[2]',
            hash='service-hash',
            lifecycle_epoch=7,
            resource_scope='service-hash'))
        connection.execute(serve_state.version_specs_table.insert(), [{
            'service_name': 'svc',
            'version': 1,
            'spec': historical_payload,
            'yaml_content': 'service: {}',
            'created_at': 1.0,
            'created_by': 'test',
        }, {
            'service_name': 'svc',
            'version': 2,
            'spec': successor_payload,
            'yaml_content': 'service: {}',
            'created_at': 2.0,
            'created_by': 'test',
        }])

    no_evidence = placement_contract_normalization._ExternalEvidence(
        count=0, digest='0' * 64)

    for replica_id, replica_version in ((1, None), (2, 999)):
        with engine.begin() as connection:
            connection.execute(serve_state.replicas_table.insert().values(
                service_name='svc',
                replica_id=replica_id,
                version=replica_version))
        with orm.Session(engine) as session:
            inventory, _ = placement_contract_normalization._scan_inventory(
                session, row_bound=10)
        historical = next(
            row for row in inventory if row.identity == ('svc', 1))
        assert historical.dependency_facts['unknown_version_replica_count'] == 1
        with engine.begin() as connection:
            connection.execute(serve_state.replicas_table.delete().where(
                serve_state.replicas_table.c.service_name == 'svc',
                serve_state.replicas_table.c.replica_id == replica_id))

    def no_resource_actions(_engine, targets):
        return {
            (target.service_name, target.version): no_evidence
            for target in targets
        }

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='consolidation mode'):
        placement_contract_normalization.run_operator(
            engine=engine,
            mode=(placement_contract_normalization.ApplyMode.
                  RETIRE_TERMINAL_HISTORICAL),
            row_bound=10,
            freeze_evidence_sha256='f' * 64,
            consolidation_mode_checker=lambda: False)

    common_retirement_kwargs = {
        'engine': engine,
        'mode': (placement_contract_normalization.ApplyMode.
                 RETIRE_TERMINAL_HISTORICAL),
        'row_bound': 10,
        'freeze_evidence_sha256': 'f' * 64,
        'image_evidence_getter': lambda _name, _version: no_evidence,
        'request_evidence_getter': lambda _engine: no_evidence,
        'process_evidence_getter': lambda _targets, _pod_uid: no_evidence,
        'resource_action_evidence_getter': no_resource_actions,
        'api_pod_checker': lambda _engine: _api_pod_identity(),
        'controller_hold_checker': lambda: True,
        'consolidation_mode_checker': lambda: True,
    }
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='cluster evidence is not quiescent'):
        placement_contract_normalization.run_operator(
            **common_retirement_kwargs,
            legacy_controller_evidence_getter=lambda:
            (placement_contract_normalization._ExternalEvidence(
                count=1, digest='1' * 64)))

    changing_legacy_evidence = iter((
        no_evidence,
        placement_contract_normalization._ExternalEvidence(count=0,
                                                           digest='1' * 64),
    ))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='cluster evidence changed during apply'):
        placement_contract_normalization.run_operator(
            **common_retirement_kwargs,
            legacy_controller_evidence_getter=lambda: next(
                changing_legacy_evidence))

    timestamps = iter((10.0, 11.0, 12.0))
    result = placement_contract_normalization.run_operator(
        **common_retirement_kwargs,
        legacy_controller_evidence_getter=lambda: no_evidence,
        now=lambda: next(timestamps))

    assert result.changed_rows == 0
    assert result.retired_rows == 1
    assert result.run_id is not None
    run_id = uuid.UUID(result.run_id)
    with engine.connect() as connection:
        versions = connection.execute(
            sqlalchemy.select(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name == 'svc').
            order_by(
                serve_state.version_specs_table.c.version)).mappings().all()
        service = connection.execute(
            sqlalchemy.select(serve_state.services_table).where(
                serve_state.services_table.c.name == 'svc')).mappings().one()
        ledger = connection.execute(
            sqlalchemy.select(
                serve_state.placement_normalization_rows_table.c.version,
                serve_state.placement_normalization_rows_table.c.classification,
                serve_state.placement_normalization_rows_table.c.outcome,
            ).where(serve_state.placement_normalization_rows_table.c.run_id ==
                    run_id).order_by(
                        serve_state.placement_normalization_rows_table.c.version
                    )).all()

    assert [row['version'] for row in versions] == [1, 2]
    retired, successor = versions
    assert bytes(retired['spec']) == pickle.dumps(None, protocol=4)
    assert retired['yaml_content'] is None
    assert retired['retired_yaml_content'] == 'service: {}'
    assert retired['retired_at'] == 11.0
    assert retired['retirement_run_id'] == run_id
    assert bytes(successor['spec']) == successor_payload
    assert successor['yaml_content'] == 'service: {}'
    assert service['current_version'] == 2
    assert service['placement_normalization_requested_run_id'] == run_id
    assert service['placement_normalization_loaded_run_id'] is None
    assert [tuple(row) for row in ledger] == [
        (1, 'historical_physical_per_gpu', 'retired'),
        (2, 'explicit_v2', 'unchanged'),
    ]
    dry_run = placement_contract_normalization.run_operator(engine=engine,
                                                            mode=None,
                                                            row_bound=10)
    assert dry_run.classification_counts == {
        'retired': 1,
        'explicit_v2': 1,
    }
    assert dry_run.changed_rows == 0
    assert dry_run.blockers == ()


def test_ledger_manifest_verifies_complete_pre_and_post_inventory():
    row = _normalizer_work(_explicit_v2_payload(), 1)
    original_columns = placement_contract_normalization._column_sha256s(
        row.original)
    result_columns = placement_contract_normalization._column_sha256s(
        row.result)
    original_row_digest = placement_contract_normalization._row_sha256(
        row.original)
    result_row_digest = placement_contract_normalization._row_sha256(row.result)
    run_id = uuid.uuid4()
    entry = {
        'run_id': run_id,
        'service_name': 'svc',
        'version': 1,
        'classification': row.classification.value,
        'outcome': 'unchanged',
        'original_spec_sha256': row.analysis.source_sha256,
        'result_spec_sha256': row.analysis.source_sha256,
        'original_row_sha256': original_row_digest,
        'result_row_sha256': result_row_digest,
        'original_column_sha256s': original_columns,
        'result_column_sha256s': result_columns,
        'service_hash': 'current-hash',
        'service_lifecycle_epoch': 7,
        'dependency_facts': row.dependency_facts,
    }
    run = {
        'run_id': run_id,
        'mode': 'apply_supported',
        'normalizer_version': f'1:{"a" * 40}',
        'schema_revision': '037',
        'release_version': 'test-release',
        'started_at': 1.0,
        'completed_at': 2.0,
        'row_count': 1,
        'row_bound': 1,
        'classification_counts': {
            row.classification.value: 1,
        },
        'pre_inventory_sha256': placement_contract_normalization._sha256(
            f'[["svc",1,"{original_row_digest}"]]'.encode()),
        'post_inventory_sha256': placement_contract_normalization._sha256(
            f'[["svc",1,"{result_row_digest}"]]'.encode()),
        'freeze_evidence_sha256': 'e' * 64,
    }

    assert placement_contract_normalization._ledger_manifest_mismatches(
        run, [entry]) == []

    entry['outcome'] = 'changed'
    tamper_reasons = {
        issue['reason'] for issue in placement_contract_normalization.
        _ledger_manifest_mismatches(run, [entry])
    }
    assert 'invalid_classification_outcome' in tamper_reasons
    assert 'spec_digest_outcome_mismatch' in tamper_reasons
    entry['outcome'] = 'unchanged'

    entry['result_row_sha256'] = 'f' * 64
    reasons = {
        issue['reason'] for issue in placement_contract_normalization.
        _ledger_manifest_mismatches(run, [entry])
    }
    assert 'invalid_result_column_inventory' in reasons
    assert 'post_inventory_digest_mismatch' in reasons


def test_postimage_verification_rereads_every_version_column(monkeypatch):
    expected = _normalizer_work(_explicit_v2_payload(), 1)
    observed = _normalizer_work(expected.original['spec'], 1)
    digest = placement_contract_normalization._fleet_sha256([expected],
                                                            result=True)
    monkeypatch.setattr(placement_contract_normalization, '_scan_inventory',
                        lambda _session, _bound: ([observed], {}))

    placement_contract_normalization._verify_version_postimages(
        mock.Mock(), [expected], 1, digest)

    observed.original['created_by'] = 'unexpected-trigger-write'
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='postimages do not match'):
        placement_contract_normalization._verify_version_postimages(
            mock.Mock(), [expected], 1, digest)


def test_fieldless_per_gpu_spec_materializes_historical_physical_contract():
    state = dict(_spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER).__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)

    restored = _restore(state)

    assert restored.replica_unit == 'physical_backend'
    assert restored.placement_contract.is_legacy_physical_per_gpu
    assert restored.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 1
    assert restored.__dict__[
        placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] is False
    copied = restored.copy()
    assert copied.placement_contract == restored.placement_contract
    with pytest.raises(ValueError, match='mirror-free v2'):
        pickle.dumps(copied, protocol=4)


def test_historical_copy_preserves_pre_validation_scaling_values():
    state = dict(_spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER).__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)
    state['_target_utilization_percentage'] = 0
    legacy = _restore(state)

    copied = legacy.copy()
    assert copied.target_utilization_percentage == 0
    with pytest.raises(ValueError, match='mirror-free v2'):
        pickle.dumps(copied, protocol=4)


def test_pre_placement_spec_materializes_explicit_disabled_service_contract():
    state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)
    state.pop(placement_policy.POLICY_NAME_FIELD)
    state.pop(placement_policy.POOL_FIELD)

    restored = _restore(state)

    assert not restored.placement_contract.enabled
    assert restored.placement_contract.workload_kind == 'service'
    assert restored.__dict__[placement_policy.POLICY_NAME_FIELD] is None
    assert restored.__dict__[placement_policy.POOL_FIELD] is False
    assert restored.copy().placement_contract == restored.placement_contract


@pytest.mark.parametrize('corrupt', [
    'partial',
    'boolean_version',
    'unknown_version',
    'unknown_dimension',
    'invalid_tuple',
    'missing_mirror',
    'mirror_mismatch',
    'missing_policy',
    'missing_pool',
    'invalid_logical_pool_tuple',
    'invalid_pool',
    'policy_mismatch',
])
def test_v1_contract_corruption_fails_loudly(corrupt):
    state = dict(_spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER).__dict__)
    state[placement_policy.CONTRACT_VERSION_FIELD] = (
        placement_policy.PLACEMENT_CONTRACT_VERSION_V1)
    state[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] = True
    if corrupt == 'partial':
        state.pop(placement_policy.CONTRACT_COST_UNIT_FIELD)
    elif corrupt == 'boolean_version':
        state[placement_policy.CONTRACT_VERSION_FIELD] = True
    elif corrupt == 'unknown_version':
        state[placement_policy.CONTRACT_VERSION_FIELD] = 99
    elif corrupt == 'unknown_dimension':
        state[placement_policy.CONTRACT_RESERVED_FILL_MODE_FIELD] = 'mystery'
    elif corrupt == 'invalid_tuple':
        state[placement_policy.CONTRACT_CATALOG_MODE_FIELD] = (
            placement_policy.CATALOG_MODE_CONFIGURED_SHAPES)
    elif corrupt == 'missing_mirror':
        state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD)
    elif corrupt == 'mirror_mismatch':
        state[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] = False
    elif corrupt == 'missing_policy':
        state.pop(placement_policy.POLICY_NAME_FIELD)
    elif corrupt == 'missing_pool':
        state.pop(placement_policy.POOL_FIELD)
    elif corrupt == 'invalid_logical_pool_tuple':
        state[placement_policy.CONTRACT_WORKLOAD_KIND_FIELD] = (
            placement_policy.WORKLOAD_KIND_POOL)
    elif corrupt == 'invalid_pool':
        state[placement_policy.POOL_FIELD] = 'pool'
    elif corrupt == 'policy_mismatch':
        state[placement_policy.POLICY_NAME_FIELD] = (
            placement_policy.SPOT_HEDGE_PLACER)

    with pytest.raises(ValueError):
        _restore(state)


def test_valid_contract_workload_mismatch_hits_pool_fence():
    state = dict(_spec().__dict__)
    state[placement_policy.CONTRACT_WORKLOAD_KIND_FIELD] = (
        placement_policy.WORKLOAD_KIND_POOL)

    with pytest.raises(ValueError, match='workload kind disagrees with _pool'):
        _restore(state)


def test_fieldless_invalid_pool_representation_fails_loudly():
    state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state[placement_policy.POOL_FIELD] = []

    with pytest.raises(ValueError, match='_pool must be'):
        _restore(state)


def test_fieldless_empty_pool_mapping_preserves_preceding_service_semantics():
    state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state[placement_policy.POOL_FIELD] = {}

    restored = _restore(state)

    assert not restored.pool
    assert restored.placement_contract.workload_kind == (
        placement_policy.WORKLOAD_KIND_SERVICE)
    assert restored.__dict__[placement_policy.POOL_FIELD] is False


@pytest.mark.parametrize('pool_driver', [{}, True])
def test_versioned_pool_requires_truthy_rollback_driver(pool_driver):
    pool_spec = service_spec.SkyServiceSpec.from_yaml_config({
        'pool': {},
        'workers': 1,
    })
    state = dict(pool_spec.__dict__)
    state[placement_policy.POOL_FIELD] = pool_driver

    with pytest.raises(ValueError, match='rollback-compatible pool'):
        _restore(state)


def test_decode_boundary_emits_bounded_structured_events(caplog):
    legacy_state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        legacy_state.pop(field)

    with caplog.at_level(logging.WARNING, logger='sky.serve.service_spec'):
        restored = _restore(legacy_state)
        # Typed hot-path access must not emit another decode event.
        assert not restored.placement_contract.enabled
    assert caplog.text.count('event=skyserve_placement_contract_decode') == 1
    assert 'outcome=legacy_materialized' in caplog.text
    assert 'source=fieldless' in caplog.text

    caplog.clear()
    explicit_v1_state = dict(_spec().__dict__)
    explicit_v1_state[placement_policy.CONTRACT_VERSION_FIELD] = (
        placement_policy.PLACEMENT_CONTRACT_VERSION_V1)
    explicit_v1_state[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] = False
    with caplog.at_level(logging.WARNING, logger='sky.serve.service_spec'):
        restored_v1 = _restore(explicit_v1_state)
    assert restored_v1.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 2
    assert (placement_policy.ROLLBACK_REPLICA_UNIT_FIELD
            not in restored_v1.__dict__)
    assert caplog.text.count('event=skyserve_placement_contract_decode') == 1
    assert 'outcome=legacy_materialized' in caplog.text
    assert 'source=v1' in caplog.text

    caplog.clear()
    rejected_state = dict(_spec().__dict__)
    rejected_state.pop(placement_policy.CONTRACT_COST_UNIT_FIELD)
    with caplog.at_level(logging.ERROR, logger='sky.serve.service_spec'), \
         pytest.raises(ValueError):
        _restore(rejected_state)
    assert caplog.text.count('event=skyserve_placement_contract_decode') == 1
    assert 'outcome=rejected' in caplog.text
    assert 'contract_fields_present=6' in caplog.text


def test_transition_reader_accepts_v2_and_copy_stays_v2():
    original = _spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER)
    state = dict(original.__dict__)
    state.update(original.placement_contract.persisted_fields())
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)

    restored = _restore(state)

    assert restored.placement_contract == original.placement_contract
    assert placement_policy.ROLLBACK_REPLICA_UNIT_FIELD not in restored.__dict__
    copied = restored.copy()
    assert copied.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 2
    assert (placement_policy.ROLLBACK_REPLICA_UNIT_FIELD not in copied.__dict__)


def test_v2_rejects_rollback_mirror():
    original = _spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER)
    state = dict(original.__dict__)
    state.update(original.placement_contract.persisted_fields())
    state[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] = True

    with pytest.raises(ValueError, match='v2 must not contain'):
        _restore(state)


def test_v2_writer_rejects_transition_only_historical_contract():
    legacy = placement_policy.resolve_legacy_contract(
        placement_policy.CAPACITY_AWARE_SPOT_PLACER,
        pool=False,
        uses_logical_replicas=False)

    with pytest.raises(ValueError, match='v2 cannot encode'):
        legacy.persisted_fields()


def test_v2_reader_rejects_transition_only_historical_contract():
    state = dict(_spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER).__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)
    historical = _restore(state)
    v2_state = dict(historical.__dict__)
    v2_state[placement_policy.CONTRACT_VERSION_FIELD] = (
        placement_policy.PLACEMENT_CONTRACT_VERSION_V2)
    v2_state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD)

    with pytest.raises(ValueError, match='v2 cannot encode'):
        _restore(v2_state)


def test_contract_driver_override_resolves_fresh_semantics():
    state = dict(_spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER).__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)
    legacy = _restore(state)
    assert legacy.placement_contract.is_legacy_physical_per_gpu

    updated = legacy.copy(
        spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER)

    assert updated.uses_logical_replicas
    assert not updated.placement_contract.is_legacy_physical_per_gpu


def test_logical_copy_cannot_change_workload_kind_to_pool():
    logical = _spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER)

    with pytest.raises(ValueError, match='not supported for pool'):
        logical.copy(pool=True)


def test_per_gpu_subclass_and_dynamic_registry_are_removed():
    assert not hasattr(spot_placer, 'CapacityAwareDynamicFallbackSpotPlacer')
    assert not hasattr(spot_placer, 'SPOT_PLACERS')
    assert not hasattr(spot_placer, 'DEFAULT_SPOT_PLACER')
