"""Explicit placement policy contracts for SkyServe.

This module is intentionally dependency-neutral: it owns the public policy
names and their typed runtime meaning, but imports neither service specs nor
placement engines.
"""

from collections.abc import Mapping
import dataclasses
from typing import Any

SPOT_HEDGE_PLACER = 'dynamic_fallback'
CAPACITY_AWARE_SPOT_PLACER = 'dynamic_fallback_per_gpu'
SUPPORTED_SPOT_PLACERS = (SPOT_HEDGE_PLACER, CAPACITY_AWARE_SPOT_PLACER)

ENGINE_NONE = 'none'
ENGINE_DYNAMIC_FALLBACK = 'dynamic_fallback'

REPLICA_UNIT_PHYSICAL_BACKEND = 'physical_backend'
REPLICA_UNIT_LOGICAL = 'logical'

CATALOG_MODE_NOT_APPLICABLE = 'not_applicable'
CATALOG_MODE_CONFIGURED_SHAPES = 'configured_shapes'
CATALOG_MODE_WHOLE_GPU_SHAPES = 'whole_gpu_shapes'

COST_UNIT_NOT_APPLICABLE = 'not_applicable'
COST_UNIT_MACHINE_HOUR = 'machine_hour'
COST_UNIT_GPU_SLOT_HOUR = 'gpu_slot_hour'

RESERVED_FILL_MODE_NOT_APPLICABLE = 'not_applicable'
RESERVED_FILL_MODE_CONFIGURED_SHAPE = 'configured_shape'
RESERVED_FILL_MODE_SINGLE_GPU_BACKEND = 'single_gpu_backend'

WORKLOAD_KIND_SERVICE = 'service'
WORKLOAD_KIND_POOL = 'pool'

PLACEMENT_CONTRACT_VERSION_V1 = 1
PLACEMENT_CONTRACT_VERSION_V2 = 2

CONTRACT_VERSION_FIELD = '_placement_contract_version'
CONTRACT_ENGINE_FIELD = '_placement_engine'
CONTRACT_REPLICA_UNIT_FIELD = '_placement_replica_unit'
CONTRACT_CATALOG_MODE_FIELD = '_placement_catalog_mode'
CONTRACT_COST_UNIT_FIELD = '_placement_cost_unit'
CONTRACT_RESERVED_FILL_MODE_FIELD = '_placement_reserved_fill_mode'
CONTRACT_WORKLOAD_KIND_FIELD = '_placement_workload_kind'
CONTRACT_FIELDS = (
    CONTRACT_VERSION_FIELD,
    CONTRACT_ENGINE_FIELD,
    CONTRACT_REPLICA_UNIT_FIELD,
    CONTRACT_CATALOG_MODE_FIELD,
    CONTRACT_COST_UNIT_FIELD,
    CONTRACT_RESERVED_FILL_MODE_FIELD,
    CONTRACT_WORKLOAD_KIND_FIELD,
)
ROLLBACK_REPLICA_UNIT_FIELD = '_uses_logical_replicas'
POLICY_NAME_FIELD = '_spot_placer'
POOL_FIELD = '_pool'


@dataclasses.dataclass(frozen=True)
class PlacementContract:
    """Complete runtime meaning of a SkyServe placement policy."""

    engine: str
    replica_unit: str
    catalog_mode: str
    cost_unit: str
    reserved_fill_mode: str
    workload_kind: str

    def __post_init__(self) -> None:
        fields = {
            'engine': (self.engine, {ENGINE_NONE, ENGINE_DYNAMIC_FALLBACK}),
            'replica_unit':
                (self.replica_unit,
                 {REPLICA_UNIT_PHYSICAL_BACKEND, REPLICA_UNIT_LOGICAL}),
            'catalog_mode': (self.catalog_mode, {
                CATALOG_MODE_NOT_APPLICABLE, CATALOG_MODE_CONFIGURED_SHAPES,
                CATALOG_MODE_WHOLE_GPU_SHAPES
            }),
            'cost_unit': (self.cost_unit, {
                COST_UNIT_NOT_APPLICABLE, COST_UNIT_MACHINE_HOUR,
                COST_UNIT_GPU_SLOT_HOUR
            }),
            'reserved_fill_mode': (self.reserved_fill_mode, {
                RESERVED_FILL_MODE_NOT_APPLICABLE,
                RESERVED_FILL_MODE_CONFIGURED_SHAPE,
                RESERVED_FILL_MODE_SINGLE_GPU_BACKEND,
            }),
            'workload_kind': (self.workload_kind,
                              {WORKLOAD_KIND_SERVICE, WORKLOAD_KIND_POOL}),
        }
        for name, (value, allowed) in fields.items():
            if not isinstance(value, str) or value not in allowed:
                raise ValueError(f'Invalid placement contract {name}: '
                                 f'{value!r}. Expected one of '
                                 f'{sorted(allowed)!r}.')

        no_engine = (ENGINE_NONE, REPLICA_UNIT_PHYSICAL_BACKEND,
                     CATALOG_MODE_NOT_APPLICABLE, COST_UNIT_NOT_APPLICABLE,
                     RESERVED_FILL_MODE_NOT_APPLICABLE)
        physical_dynamic = (ENGINE_DYNAMIC_FALLBACK,
                            REPLICA_UNIT_PHYSICAL_BACKEND,
                            CATALOG_MODE_CONFIGURED_SHAPES,
                            COST_UNIT_MACHINE_HOUR,
                            RESERVED_FILL_MODE_CONFIGURED_SHAPE)
        logical_per_gpu = (ENGINE_DYNAMIC_FALLBACK, REPLICA_UNIT_LOGICAL,
                           CATALOG_MODE_WHOLE_GPU_SHAPES,
                           COST_UNIT_GPU_SLOT_HOUR,
                           RESERVED_FILL_MODE_SINGLE_GPU_BACKEND)
        legacy_physical_per_gpu = (ENGINE_DYNAMIC_FALLBACK,
                                   REPLICA_UNIT_PHYSICAL_BACKEND,
                                   CATALOG_MODE_WHOLE_GPU_SHAPES,
                                   COST_UNIT_GPU_SLOT_HOUR,
                                   RESERVED_FILL_MODE_SINGLE_GPU_BACKEND)
        behavior = (self.engine, self.replica_unit, self.catalog_mode,
                    self.cost_unit, self.reserved_fill_mode)
        allowed_behaviors = {
            no_engine, physical_dynamic, logical_per_gpu,
            legacy_physical_per_gpu
        }
        if behavior not in allowed_behaviors:
            raise ValueError('Invalid placement contract behavior tuple: '
                             f'{behavior!r}.')
        if (self.workload_kind == WORKLOAD_KIND_POOL and
                behavior not in {no_engine, physical_dynamic}):
            raise ValueError('Pools require a physical placement contract; '
                             f'got {behavior!r}.')

    @property
    def enabled(self) -> bool:
        return self.engine != ENGINE_NONE

    @property
    def uses_logical_replicas(self) -> bool:
        return self.replica_unit == REPLICA_UNIT_LOGICAL

    @property
    def expand_accelerator_counts(self) -> bool:
        return self.catalog_mode == CATALOG_MODE_WHOLE_GPU_SHAPES

    @property
    def requires_single_gpu_reserved_fill(self) -> bool:
        return (
            self.reserved_fill_mode == RESERVED_FILL_MODE_SINGLE_GPU_BACKEND)

    @property
    def is_legacy_physical_per_gpu(self) -> bool:
        return (self.engine == ENGINE_DYNAMIC_FALLBACK and
                self.replica_unit == REPLICA_UNIT_PHYSICAL_BACKEND and
                self.catalog_mode == CATALOG_MODE_WHOLE_GPU_SHAPES and
                self.cost_unit == COST_UNIT_GPU_SLOT_HOUR and
                self.reserved_fill_mode
                == RESERVED_FILL_MODE_SINGLE_GPU_BACKEND)

    def normalize_hourly_cost(self, hourly_cost: float,
                              accelerator_slots: float) -> float:
        """Normalize a machine-hour price for this contract's cost unit."""
        if self.cost_unit == COST_UNIT_MACHINE_HOUR:
            return hourly_cost
        if self.cost_unit == COST_UNIT_GPU_SLOT_HOUR:
            slots = accelerator_slots if accelerator_slots > 0 else 1.0
            return hourly_cost / slots
        raise ValueError('A placement-disabled contract has no cost order.')

    def _persisted_fields_for_version(self, version: int) -> dict[str, Any]:
        if (version == PLACEMENT_CONTRACT_VERSION_V2 and
                self.is_legacy_physical_per_gpu):
            raise ValueError('Placement contract v2 cannot encode the '
                             'transition-only historical physical/per-GPU '
                             'contract.')
        return {
            CONTRACT_VERSION_FIELD: version,
            CONTRACT_ENGINE_FIELD: self.engine,
            CONTRACT_REPLICA_UNIT_FIELD: self.replica_unit,
            CONTRACT_CATALOG_MODE_FIELD: self.catalog_mode,
            CONTRACT_COST_UNIT_FIELD: self.cost_unit,
            CONTRACT_RESERVED_FILL_MODE_FIELD: self.reserved_fill_mode,
            CONTRACT_WORKLOAD_KIND_FIELD: self.workload_kind,
        }

    def persisted_fields(self) -> dict[str, Any]:
        """Return the sole current, mirror-free persistence representation."""
        return self._persisted_fields_for_version(PLACEMENT_CONTRACT_VERSION_V2)

    def _legacy_v1_persisted_fields(self) -> dict[str, Any]:
        """Encode the read-only legacy v1 compatibility representation."""
        return self._persisted_fields_for_version(PLACEMENT_CONTRACT_VERSION_V1)


def workload_kind_from_pool(pool: Any) -> str:
    """Resolve the supported persisted pool representation explicitly."""
    if pool is None or pool is False:
        return WORKLOAD_KIND_SERVICE
    if pool is True or isinstance(pool, dict):
        return WORKLOAD_KIND_POOL
    raise ValueError('_pool must be a boolean, pool configuration, or None; '
                     f'got {pool!r}.')


def legacy_workload_kind_from_pool(pool: Any) -> str:
    """Resolve a fieldless pool value with the preceding reader's semantics."""
    workload_kind_from_pool(pool)
    if pool is True or (isinstance(pool, dict) and bool(pool)):
        return WORKLOAD_KIND_POOL
    return WORKLOAD_KIND_SERVICE


def is_pool_workload(pool: Any) -> bool:
    return workload_kind_from_pool(pool) == WORKLOAD_KIND_POOL


def canonicalize_policy_name(spot_placer: Any) -> str | None:
    """Return the canonical persisted spelling of a public policy name."""
    if spot_placer is None:
        return None
    if not isinstance(spot_placer, str):
        raise ValueError('spot_placer must be a string or None; got '
                         f'{spot_placer!r}.')
    return spot_placer.lower()


def resolve_fresh_contract(spot_placer: str | None,
                           pool: Any) -> PlacementContract:
    """Resolve a newly submitted explicit policy."""
    spot_placer = canonicalize_policy_name(spot_placer)
    workload_kind = workload_kind_from_pool(pool)
    is_pool = workload_kind == WORKLOAD_KIND_POOL
    if spot_placer is None:
        return PlacementContract(ENGINE_NONE, REPLICA_UNIT_PHYSICAL_BACKEND,
                                 CATALOG_MODE_NOT_APPLICABLE,
                                 COST_UNIT_NOT_APPLICABLE,
                                 RESERVED_FILL_MODE_NOT_APPLICABLE,
                                 workload_kind)
    if spot_placer == SPOT_HEDGE_PLACER:
        return PlacementContract(ENGINE_DYNAMIC_FALLBACK,
                                 REPLICA_UNIT_PHYSICAL_BACKEND,
                                 CATALOG_MODE_CONFIGURED_SHAPES,
                                 COST_UNIT_MACHINE_HOUR,
                                 RESERVED_FILL_MODE_CONFIGURED_SHAPE,
                                 workload_kind)
    if spot_placer == CAPACITY_AWARE_SPOT_PLACER:
        if is_pool:
            raise ValueError('dynamic_fallback_per_gpu is not supported for '
                             'pools, which count physical workers.')
        return PlacementContract(ENGINE_DYNAMIC_FALLBACK, REPLICA_UNIT_LOGICAL,
                                 CATALOG_MODE_WHOLE_GPU_SHAPES,
                                 COST_UNIT_GPU_SLOT_HOUR,
                                 RESERVED_FILL_MODE_SINGLE_GPU_BACKEND,
                                 workload_kind)
    raise ValueError(f'Unsupported spot placer: {spot_placer!r}.')


def resolve_legacy_contract(spot_placer: str | None, pool: Any,
                            uses_logical_replicas: bool) -> PlacementContract:
    """Resolve a fieldless persisted spec under its historical marker."""
    spot_placer = canonicalize_policy_name(spot_placer)
    if not isinstance(uses_logical_replicas, bool):
        raise ValueError('Legacy logical replica marker must be a boolean; '
                         f'got {uses_logical_replicas!r}.')
    legacy_pool = (legacy_workload_kind_from_pool(pool) == WORKLOAD_KIND_POOL)
    if uses_logical_replicas:
        contract = resolve_fresh_contract(spot_placer, legacy_pool)
        if not contract.uses_logical_replicas:
            raise ValueError('Legacy logical replica marker conflicts with '
                             f'spot placer {spot_placer!r}.')
        return contract
    if spot_placer == CAPACITY_AWARE_SPOT_PLACER:
        if legacy_pool:
            raise ValueError('A legacy per-GPU physical contract cannot be a '
                             'pool.')
        return PlacementContract(ENGINE_DYNAMIC_FALLBACK,
                                 REPLICA_UNIT_PHYSICAL_BACKEND,
                                 CATALOG_MODE_WHOLE_GPU_SHAPES,
                                 COST_UNIT_GPU_SLOT_HOUR,
                                 RESERVED_FILL_MODE_SINGLE_GPU_BACKEND,
                                 WORKLOAD_KIND_SERVICE)
    return resolve_fresh_contract(spot_placer, legacy_pool)


def _validate_policy_mapping(contract: PlacementContract,
                             spot_placer: str | None, *,
                             allow_legacy_per_gpu: bool) -> None:
    if contract.workload_kind == WORKLOAD_KIND_POOL:
        pool = True
    else:
        pool = False
    fresh = resolve_fresh_contract(spot_placer, pool)
    if contract == fresh:
        return
    if (allow_legacy_per_gpu and spot_placer == CAPACITY_AWARE_SPOT_PLACER and
            contract.is_legacy_physical_per_gpu and not pool):
        return
    raise ValueError(
        'Persisted spot placer does not match its placement '
        f'contract: policy={spot_placer!r}, contract={contract!r}.')


def decode_contract_state(
        state: Mapping[str, Any]) -> tuple[PlacementContract, int | None]:
    """Decode a SkyServiceSpec state without silently repairing corruption.

    Returns the contract and its persisted schema version.  ``None`` identifies
    an all-contract-fields-absent legacy state.
    """
    present = tuple(field in state for field in CONTRACT_FIELDS)
    if not any(present):
        marker = state.get(ROLLBACK_REPLICA_UNIT_FIELD, False)
        if not isinstance(marker, bool):
            raise ValueError('Legacy logical replica marker must be a boolean; '
                             f'got {marker!r}.')
        spot_placer = state.get(POLICY_NAME_FIELD)
        pool = state.get(POOL_FIELD, False)
        legacy_workload_kind_from_pool(pool)
        return resolve_legacy_contract(spot_placer, pool, marker), None
    if not all(present):
        missing = [
            field for field, is_present in zip(CONTRACT_FIELDS, present)
            if not is_present
        ]
        raise ValueError('Partial placement contract state; missing fields: '
                         f'{missing!r}.')
    if POLICY_NAME_FIELD not in state or POOL_FIELD not in state:
        raise ValueError('Versioned placement contract requires explicit '
                         '_spot_placer and _pool fields.')

    version = state[CONTRACT_VERSION_FIELD]
    if not isinstance(version, int) or isinstance(version, bool):
        raise ValueError('Placement contract version must be an integer; got '
                         f'{version!r}.')
    if version not in (PLACEMENT_CONTRACT_VERSION_V1,
                       PLACEMENT_CONTRACT_VERSION_V2):
        raise ValueError(f'Unsupported placement contract version: '
                         f'{version!r}.')
    values = [state[field] for field in CONTRACT_FIELDS[1:]]
    contract = PlacementContract(*values)
    pool = state[POOL_FIELD]
    workload_kind = workload_kind_from_pool(pool)
    if (workload_kind == WORKLOAD_KIND_POOL and
        (not isinstance(pool, dict) or not pool)):
        raise ValueError('Versioned pool placement contract requires a '
                         'non-empty rollback-compatible pool configuration; '
                         f'got {pool!r}.')
    if contract.workload_kind != workload_kind:
        raise ValueError('Placement contract workload kind disagrees with '
                         f'_pool: {contract.workload_kind!r} versus {pool!r}.')
    if (version == PLACEMENT_CONTRACT_VERSION_V2 and
            contract.is_legacy_physical_per_gpu):
        raise ValueError('Placement contract v2 cannot encode the '
                         'transition-only historical physical/per-GPU '
                         'contract.')
    _validate_policy_mapping(
        contract,
        state[POLICY_NAME_FIELD],
        allow_legacy_per_gpu=(version == PLACEMENT_CONTRACT_VERSION_V1))

    has_mirror = ROLLBACK_REPLICA_UNIT_FIELD in state
    if version == PLACEMENT_CONTRACT_VERSION_V1:
        if not has_mirror:
            raise ValueError('Placement contract v1 requires the rollback '
                             'logical replica marker.')
        marker = state[ROLLBACK_REPLICA_UNIT_FIELD]
        if not isinstance(marker, bool):
            raise ValueError('Rollback logical replica marker must be a '
                             f'boolean; got {marker!r}.')
        if marker != contract.uses_logical_replicas:
            raise ValueError('Rollback logical replica marker disagrees with '
                             'the placement contract replica unit.')
    elif has_mirror:
        raise ValueError('Placement contract v2 must not contain the rollback '
                         'logical replica marker.')
    return contract, version
