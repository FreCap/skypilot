"""Typed SkyServe placement-contract and persistence compatibility tests."""

import base64
import hashlib
import inspect
import logging
import pickle
import types
from typing import Any
from unittest import mock
import zlib

import pytest
from spot_placer_test_utils import make_location

from sky.serve import placement_policy
from sky.serve import service_spec
from sky.serve import spot_placer

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


def test_transition_spec_persists_only_primitive_v1_contract_fields():
    spec = _spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER)

    assert spec.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 1
    assert spec.__dict__[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] is True
    assert all(not isinstance(value, placement_policy.PlacementContract)
               for value in spec.__dict__.values())
    assert spec.placement_contract.uses_logical_replicas
    rendered = spec.to_yaml_config()
    assert '_placement_' not in repr(rendered)
    assert '_uses_logical_replicas' not in repr(rendered)


@pytest.mark.parametrize('version', [True, False, 1.0, '1', 0, 3])
def test_contract_writer_rejects_non_exact_versions(version):
    with pytest.raises(ValueError, match='Unsupported placement contract'):
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
    assert restored.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 1
    assert restored.__dict__[
        placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] is True
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
    assert copied.placement_contract == contract
    assert copied.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 1
    assert copied.__dict__[
        placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] is False
    assert pickle.loads(pickle.dumps(copied,
                                     protocol=4)).placement_contract == contract


def test_fieldless_per_gpu_spec_materializes_historical_physical_contract():
    state = dict(_spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER).__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD)

    restored = _restore(state)

    assert restored.replica_unit == 'physical_backend'
    assert restored.placement_contract.is_legacy_physical_per_gpu
    assert restored.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 1
    assert restored.__dict__[
        placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] is False
    copied = restored.copy()
    assert copied.placement_contract == restored.placement_contract
    assert copied.spot_placer == placement_policy.CAPACITY_AWARE_SPOT_PLACER


def test_historical_copy_preserves_pre_validation_scaling_values():
    state = dict(_spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER).__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD)
    state['_target_utilization_percentage'] = 0
    legacy = _restore(state)

    copied = legacy.copy()

    assert copied.placement_contract.is_legacy_physical_per_gpu
    assert copied.target_utilization_percentage == 0


def test_pre_placement_spec_materializes_explicit_disabled_service_contract():
    state = dict(_spec().__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD)
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

    caplog.clear()
    rejected_state = dict(_spec().__dict__)
    rejected_state.pop(placement_policy.CONTRACT_COST_UNIT_FIELD)
    with caplog.at_level(logging.ERROR, logger='sky.serve.service_spec'), \
         pytest.raises(ValueError):
        _restore(rejected_state)
    assert caplog.text.count('event=skyserve_placement_contract_decode') == 1
    assert 'outcome=rejected' in caplog.text
    assert 'contract_fields_present=6' in caplog.text


def test_transition_reader_accepts_v2_and_copy_downgrades_to_v1():
    original = _spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER)
    state = dict(original.__dict__)
    state.update(
        original.placement_contract.persisted_fields(
            placement_policy.PLACEMENT_CONTRACT_VERSION_CLEANUP))
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD)

    restored = _restore(state)

    assert restored.placement_contract == original.placement_contract
    assert placement_policy.ROLLBACK_REPLICA_UNIT_FIELD not in restored.__dict__
    copied = restored.copy()
    assert copied.__dict__[placement_policy.CONTRACT_VERSION_FIELD] == 1
    assert copied.__dict__[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] is True


def test_v2_rejects_rollback_mirror():
    original = _spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER)
    state = dict(original.__dict__)
    state.update(
        original.placement_contract.persisted_fields(
            placement_policy.PLACEMENT_CONTRACT_VERSION_CLEANUP))

    with pytest.raises(ValueError, match='v2 must not contain'):
        _restore(state)


def test_v2_writer_rejects_transition_only_historical_contract():
    legacy = placement_policy.resolve_legacy_contract(
        placement_policy.CAPACITY_AWARE_SPOT_PLACER,
        pool=False,
        uses_logical_replicas=False)

    with pytest.raises(ValueError, match='v2 cannot encode'):
        legacy.persisted_fields(
            placement_policy.PLACEMENT_CONTRACT_VERSION_CLEANUP)


def test_v2_reader_rejects_transition_only_historical_contract():
    state = dict(_spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER).__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD)
    historical = _restore(state)
    v2_state = dict(historical.__dict__)
    v2_state[placement_policy.CONTRACT_VERSION_FIELD] = (
        placement_policy.PLACEMENT_CONTRACT_VERSION_CLEANUP)
    v2_state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD)

    with pytest.raises(ValueError, match='v2 cannot encode'):
        _restore(v2_state)


def test_contract_driver_override_resolves_fresh_semantics():
    state = dict(_spec(placement_policy.CAPACITY_AWARE_SPOT_PLACER).__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD)
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
