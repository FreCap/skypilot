"""Service specification for SkyServe."""
import json
import math
import os
import textwrap
from typing import Any

from sky import serve
from sky import sky_logging
from sky.serve import constants
from sky.serve import load_balancing_policies as lb_policies
from sky.serve import placement_policy
from sky.serve import serve_utils
from sky.utils import common_utils
from sky.utils import schemas
from sky.utils import ux_utils
from sky.utils import yaml_utils

logger = sky_logging.init_logger(__name__)

_PLACEMENT_CONTRACT_COPY_TOKEN = object()
_PLACEMENT_DECODE_EVENT = 'skyserve_placement_contract_decode'


def _canonical_pool_driver(
    pool: bool | dict[str, Any] | None,
    min_replicas: int,
    max_replicas: int | None,
) -> bool | dict[str, Any] | None:
    """Return a rollback-readable persisted pool driver."""
    if not placement_policy.is_pool_workload(pool):
        return pool
    if isinstance(pool, dict) and pool:
        canonical_pool = dict(pool)
        # The policy name has its own persisted primitive field.  Keeping a
        # second, unnormalized copy in _pool lets the two drivers disagree and
        # causes explicit null to leak back into canonical YAML.
        canonical_pool.pop('spot_placer', None)
        if canonical_pool:
            return canonical_pool
    if max_replicas is None:
        return {'workers': min_replicas}
    return {
        'min_workers': min_replicas,
        'max_workers': max_replicas,
    }


class SkyServiceSpec:
    """SkyServe service specification."""

    def __init__(
        self,
        readiness_path: str,
        initial_delay_seconds: int,
        readiness_timeout_seconds: int,
        endpoint_probe_interval_seconds: int,
        lb_stream_timeout_seconds: int,
        min_replicas: int,
        lb_retriable_status_codes: list[int] | None = None,
        lb_max_retries: int | None = None,
        lb_retry_initial_backoff_seconds: float | None = None,
        lb_request_queue: dict[str, Any] | None = None,
        # New services use two warm LB slots by default. Old persisted specs
        # are explicitly backfilled to False in __setstate__ below.
        lb_high_availability: bool = True,
        max_replicas: int | None = None,
        min_replicas_by_accelerator: dict[str, int] | None = None,
        num_overprovision: int | None = None,
        ports: str | None = None,
        target_qps_per_replica: float | dict[str, float] | None = None,
        target_concurrency_per_replica: float | None = None,
        target_utilization_percentage: int | None = None,
        expected_request_duration_seconds: float | None = None,
        initial_provision_lead_time_seconds: float | str | None = None,
        adaptive_demand_estimation: bool | None = None,
        max_scale_up_rate_percentage: int | None = None,
        scale_up_rate_min_replicas: int | None = None,
        scale_up_rate_period_seconds: int | None = None,
        adaptive_scale_up: dict[str, Any] | None = None,
        max_scale_down_rate_percentage: int | None = None,
        reserved_capacity_fill: bool | dict[str, Any] | None = None,
        cost_rebalance: bool | dict[str, Any] | None = None,
        post_data: dict[str, Any] | None = None,
        tls_credential: serve_utils.TLSCredential | None = None,
        readiness_headers: dict[str, str] | None = None,
        dynamic_ondemand_fallback: bool | None = None,
        base_ondemand_fallback_replicas: int | None = None,
        spot_placer: str | None = None,
        upscale_delay_seconds: int | None = None,
        downscale_delay_seconds: int | None = None,
        load_balancing_policy: str | None = None,
        pool: bool | dict[str, Any] | None = None,
        queue_length_threshold: int | None = None,
        consecutive_failure_threshold_timeout: int | None = None,
        graceful_drain_seconds: int | None = None,
        graceful_drain_async_occupancy: bool | None = None,
        _preserved_placement_contract: (placement_policy.PlacementContract |
                                        None) = None,
        _placement_contract_copy_token: object | None = None,
    ) -> None:
        spot_placer = placement_policy.canonicalize_policy_name(spot_placer)
        is_pool = placement_policy.is_pool_workload(pool)
        if is_pool:
            # For pools, max_replicas should never be specified directly by the
            # user. It should only be set via max_workers in the pool config.
            # However, if queue_length_threshold is set, that means max_replicas
            # was set internally from max_workers, so we allow it
            unsupported_fields = [
                'num_overprovision',
                'target_qps_per_replica',
                'target_concurrency_per_replica',
                'target_utilization_percentage',
                'expected_request_duration_seconds',
                'initial_provision_lead_time_seconds',
                'adaptive_demand_estimation',
                'max_scale_up_rate_percentage',
                'scale_up_rate_min_replicas',
                'scale_up_rate_period_seconds',
                'adaptive_scale_up',
                'max_scale_down_rate_percentage',
                'base_ondemand_fallback_replicas',
                'dynamic_ondemand_fallback',
                'load_balancing_policy',
                'ports',
                'post_data',
                'tls_credential',
                'readiness_headers',
            ]
            # Only restrict delay fields if autoscaling is not enabled
            # Autoscaling is enabled when max_replicas (from max_workers) is set
            if max_replicas is None:
                unsupported_fields.extend([
                    'upscale_delay_seconds',
                    'downscale_delay_seconds',
                ])

            for unsupported_field in unsupported_fields:
                if locals()[unsupported_field] is not None:
                    with ux_utils.print_exception_no_traceback():
                        error_msg = (
                            f'{unsupported_field} is not supported for pool.')
                        raise ValueError(error_msg)

            # Validate queue_length_threshold if provided
            if queue_length_threshold is not None:
                if queue_length_threshold <= 0:
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError('queue_length_threshold must be > 0. '
                                         f'Got: {queue_length_threshold}')
                # If queue_length_threshold is set, max_workers (max_replicas)
                # must also be set.
                if max_replicas is None:
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            'max_workers must be set when '
                            'queue_length_threshold is specified for pool '
                            'autoscaling.')

        if max_replicas is not None and max_replicas < min_replicas:
            with ux_utils.print_exception_no_traceback():
                raise ValueError('max_replicas must be greater than or '
                                 'equal to min_replicas. Found: '
                                 f'min_replicas={min_replicas}, '
                                 f'max_replicas={max_replicas}')

        accelerator_floors = dict(min_replicas_by_accelerator or {})
        normalized_floor_names: set[str] = set()
        for accelerator, floor in accelerator_floors.items():
            normalized = accelerator.casefold()
            if normalized in normalized_floor_names:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'min_replicas_by_accelerator contains duplicate exact '
                        f'accelerator names ignoring case: {accelerator!r}.')
            normalized_floor_names.add(normalized)
            if isinstance(floor,
                          bool) or not isinstance(floor, int) or floor < 0:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'min_replicas_by_accelerator values must be integers '
                        f'>= 0. Got {accelerator!r}: {floor!r}.')
        effective_max = max_replicas if max_replicas is not None else min_replicas
        if sum(accelerator_floors.values()) > effective_max:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'The sum of min_replicas_by_accelerator must not exceed '
                    f'max_replicas ({effective_max}). Got: '
                    f'{sum(accelerator_floors.values())}.')
        if (accelerator_floors and
                not isinstance(target_qps_per_replica, dict) and
                target_concurrency_per_replica is None):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'min_replicas_by_accelerator requires either dict type '
                    'target_qps_per_replica or '
                    'target_concurrency_per_replica so SkyServe can size and '
                    'launch each exact accelerator independently.')
        if (accelerator_floors and
                load_balancing_policy != 'instance_aware_least_load'):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'min_replicas_by_accelerator requires '
                    'load_balancing_policy: instance_aware_least_load.')

        # The two demand knobs select different autoscalers (request-rate vs
        # concurrency); allowing both would make the sizing model ambiguous.
        if (target_qps_per_replica is not None and
                target_concurrency_per_replica is not None):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'target_qps_per_replica and '
                    'target_concurrency_per_replica are mutually exclusive. '
                    'Please set only one of them.')

        # Object form implies enabled even when empty (an all-defaults
        # object is the same opt-in as a plain `true`).
        reserved_fill_enabled = (isinstance(reserved_capacity_fill, dict) or
                                 bool(reserved_capacity_fill))
        normalized_reserved_fill: bool | dict[str, Any] | None
        if isinstance(reserved_capacity_fill, dict):
            # The YAML path is schema-validated; enforce ranges here too so
            # programmatic construction cannot feed bad knobs to the
            # autoscaler.
            fill_floor = reserved_capacity_fill.get('floor_replicas', 0)
            if not isinstance(fill_floor, int) or fill_floor < 0:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'reserved_capacity_fill.floor_replicas must be an '
                        f'integer >= 0. Got: {fill_floor}')
            fill_weight = reserved_capacity_fill.get('weight', 1)
            # isfinite: float('inf') passes a plain > 0 check (and NaN
            # passes a plain <= 0 rejection), and either poisons the
            # broker's weighted water-fill (inf/inf -> NaN in rounding)
            # every round for the whole pool while the claim stays live.
            if (not isinstance(fill_weight,
                               (int, float)) or isinstance(fill_weight, bool) or
                    fill_weight <= 0 or not math.isfinite(fill_weight)):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError('reserved_capacity_fill.weight must be '
                                     f'a finite number > 0. Got: {fill_weight}')
            # Finite is not enough: 1e308 passes isfinite yet overflows the
            # broker's weighted water-fill (remaining*weight / sum(weights)
            # -> inf -> NaN in rounding). The documented bound keeps every
            # sane priority ratio expressible while staying far from float
            # overflow; the broker additionally clamps out-of-bound DB rows
            # so a poisoned row cannot crash rounds either.
            if fill_weight > constants.RESERVED_FILL_MAX_WEIGHT:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'reserved_capacity_fill.weight must not exceed '
                        f'{constants.RESERVED_FILL_MAX_WEIGHT:g}. '
                        f'Got: {fill_weight}')
            utilization_gate = reserved_capacity_fill.get(
                'utilization_gate', True)
            if not isinstance(utilization_gate, bool):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'reserved_capacity_fill.utilization_gate must be a '
                        f'boolean. Got: {utilization_gate!r}')
            # A floor above max_replicas can never be materialized: the fill
            # target is clamped to max_replicas, so the excess would sit as
            # a permanent phantom claim on the broker (absorbing entitlement
            # and feed the service never launches). The dynamic
            # demand-pressure clamp (effective_cap in the broker claim) is
            # the real guard; this is the cheap spec-time misconfiguration
            # catch for the explicit-max case.
            if max_replicas is not None and fill_floor > max_replicas:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'reserved_capacity_fill.floor_replicas must not '
                        'exceed max_replicas. Got: '
                        f'floor_replicas={fill_floor}, '
                        f'max_replicas={max_replicas}')
        if reserved_fill_enabled:
            # Fill launches land as NON-spot replicas on zero-cost
            # locations, indistinguishable (via is_spot) from paid
            # on-demand fallback capacity: FallbackRequestRateAutoscaler
            # would count them toward the fallback quota and kill them
            # as excess on-demand. Enforced at the CONSTRUCTOR so
            # programmatic construction cannot bypass the YAML-path
            # check.
            if (dynamic_ondemand_fallback or base_ondemand_fallback_replicas):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'reserved_capacity_fill is not supported with '
                        'on-demand fallback (dynamic_ondemand_fallback / '
                        'base_ondemand_fallback_replicas).')

        if cost_rebalance is not None:
            if not isinstance(cost_rebalance, (bool, dict)):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'cost_rebalance must be a boolean or object. Got: '
                        f'{cost_rebalance!r}')
            if isinstance(cost_rebalance, dict):
                min_savings = cost_rebalance.get('min_savings_fraction', 0.3)
                if (not isinstance(min_savings, (int, float)) or
                        isinstance(min_savings, bool) or
                        not math.isfinite(min_savings) or min_savings <= 0 or
                        min_savings > 1):
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            'cost_rebalance.min_savings_fraction must be a '
                            'finite number in (0, 1]. Got: '
                            f'{min_savings!r}')
                max_parallel = cost_rebalance.get('max_parallel_replacements',
                                                  1)
                if (not isinstance(max_parallel, int) or
                        isinstance(max_parallel, bool) or max_parallel < 1):
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            'cost_rebalance.max_parallel_replacements must be '
                            f'an integer >= 1. Got: {max_parallel!r}')
                stabilization = cost_rebalance.get('stabilization_seconds', 300)
                if (not isinstance(stabilization, (int, float)) or
                        isinstance(stabilization, bool) or
                        not math.isfinite(stabilization) or stabilization < 0):
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            'cost_rebalance.stabilization_seconds must be a '
                            f'finite number >= 0. Got: {stabilization!r}')
            if cost_rebalance is not False and spot_placer is None:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError('cost_rebalance requires spot_placer so '
                                     'candidate locations can be selected.')

        if target_concurrency_per_replica is not None:
            # Zero (or negative) per-GPU capacity would make every replica
            # capacity 0 and feed divisions in the concurrency autoscaler.
            if target_concurrency_per_replica <= 0:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'target_concurrency_per_replica must be > 0. '
                        f'Got: {target_concurrency_per_replica}')

        if _preserved_placement_contract is None:
            if _placement_contract_copy_token is not None:
                raise ValueError('Placement contract copy token cannot be '
                                 'used without a preserved contract.')
            resolved_placement_contract = (
                placement_policy.resolve_fresh_contract(spot_placer, pool))
            preserves_legacy_placement_contract = False
        else:
            if _placement_contract_copy_token is not (
                    _PLACEMENT_CONTRACT_COPY_TOKEN):
                raise ValueError('Preserved placement contracts are internal '
                                 'to SkyServiceSpec.copy().')
            # Internal copies of a committed version carry the complete
            # contract.  Validate it against both public drivers instead of
            # reconstructing through a different policy name.
            placement_state = _preserved_placement_contract.persisted_fields(
                placement_policy.PLACEMENT_CONTRACT_VERSION_TRANSITION)
            placement_state.update({
                placement_policy.POLICY_NAME_FIELD: spot_placer,
                placement_policy.POOL_FIELD: pool,
                placement_policy.ROLLBACK_REPLICA_UNIT_FIELD:
                    _preserved_placement_contract.uses_logical_replicas,
            })
            resolved_placement_contract, _ = (
                placement_policy.decode_contract_state(placement_state))
            preserves_legacy_placement_contract = (
                resolved_placement_contract.is_legacy_physical_per_gpu)
        uses_logical_replicas = (
            resolved_placement_contract.uses_logical_replicas)

        def _validate_percentage(name: str, value: int | None) -> None:
            if preserves_legacy_placement_contract:
                return
            if (value is not None and
                (not isinstance(value, int) or isinstance(value, bool) or
                 value < 1 or value > 100)):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        f'{name} must be an integer between 1 and 100. '
                        f'Got: {value!r}')

        _validate_percentage('target_utilization_percentage',
                             target_utilization_percentage)
        _validate_percentage('max_scale_up_rate_percentage',
                             max_scale_up_rate_percentage)
        _validate_percentage('max_scale_down_rate_percentage',
                             max_scale_down_rate_percentage)
        if (not preserves_legacy_placement_contract and
                expected_request_duration_seconds is not None and
            (not isinstance(expected_request_duration_seconds, (int, float)) or
             isinstance(expected_request_duration_seconds, bool) or
             not math.isfinite(expected_request_duration_seconds) or
             expected_request_duration_seconds <= 0)):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'expected_request_duration_seconds must be a finite '
                    'number > 0. Got: '
                    f'{expected_request_duration_seconds!r}')
        if (not preserves_legacy_placement_contract and
                adaptive_demand_estimation is not None and
                not isinstance(adaptive_demand_estimation, bool)):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'adaptive_demand_estimation must be a boolean. Got: '
                    f'{adaptive_demand_estimation!r}')
        if (not preserves_legacy_placement_contract and
                initial_provision_lead_time_seconds is not None and
                initial_provision_lead_time_seconds
                != constants.AUTOSCALER_PROVISION_LEAD_AUTO and
            (not isinstance(initial_provision_lead_time_seconds,
                            (int, float)) or
             isinstance(initial_provision_lead_time_seconds, bool) or
             not math.isfinite(initial_provision_lead_time_seconds) or
             initial_provision_lead_time_seconds < 0)):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'initial_provision_lead_time_seconds must be a finite '
                    'number >= 0 or '
                    f'{constants.AUTOSCALER_PROVISION_LEAD_AUTO!r}. Got: '
                    f'{initial_provision_lead_time_seconds!r}')
        for name, value in (
            ('scale_up_rate_min_replicas', scale_up_rate_min_replicas),
            ('scale_up_rate_period_seconds', scale_up_rate_period_seconds),
        ):
            if (not preserves_legacy_placement_contract and
                    value is not None and
                (not isinstance(value, int) or isinstance(value, bool) or
                 value <= 0)):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(f'{name} must be a positive integer. '
                                     f'Got: {value!r}')
        scale_up_rate_fields = (
            max_scale_up_rate_percentage,
            scale_up_rate_min_replicas,
            scale_up_rate_period_seconds,
        )
        if (not preserves_legacy_placement_contract and
                any(value is not None for value in scale_up_rate_fields) and
                not all(value is not None for value in scale_up_rate_fields)):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'max_scale_up_rate_percentage, '
                    'scale_up_rate_min_replicas, and '
                    'scale_up_rate_period_seconds must be set together.')
        if (not preserves_legacy_placement_contract and
                adaptive_scale_up is not None):
            if not isinstance(adaptive_scale_up, dict):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError('adaptive_scale_up must be an object. '
                                     f'Got: {adaptive_scale_up!r}')
            if not all(value is not None for value in scale_up_rate_fields):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'adaptive_scale_up requires max_scale_up_rate_'
                        'percentage, scale_up_rate_min_replicas, and '
                        'scale_up_rate_period_seconds.')
            adaptive_defaults = {
                'max_scale_up_rate_percentage': 100,
                'scale_up_rate_min_replicas': 50,
                'pressure_observations': 2,
                'hold_seconds': 120,
            }
            adaptive_defaults.update(adaptive_scale_up)
            _validate_percentage(
                'adaptive_scale_up.max_scale_up_rate_percentage',
                adaptive_defaults['max_scale_up_rate_percentage'])
            for field in ('scale_up_rate_min_replicas',
                          'pressure_observations'):
                value = adaptive_defaults[field]
                if (not isinstance(value, int) or isinstance(value, bool) or
                        value <= 0):
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            f'adaptive_scale_up.{field} must be a positive '
                            f'integer. Got: {value!r}')
            hold_seconds = adaptive_defaults['hold_seconds']
            if (not isinstance(hold_seconds, (int, float)) or
                    isinstance(hold_seconds, bool) or hold_seconds <= 0 or
                    not math.isfinite(hold_seconds)):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'adaptive_scale_up.hold_seconds must be a finite '
                        f'number > 0. Got: {hold_seconds!r}')
            adaptive_scale_up = adaptive_defaults

        logical_scaling_fields = {
            'target_utilization_percentage': target_utilization_percentage,
            'expected_request_duration_seconds': expected_request_duration_seconds,
            'initial_provision_lead_time_seconds': initial_provision_lead_time_seconds,
            'adaptive_demand_estimation': adaptive_demand_estimation,
            'max_scale_up_rate_percentage': max_scale_up_rate_percentage,
            'scale_up_rate_min_replicas': scale_up_rate_min_replicas,
            'scale_up_rate_period_seconds': scale_up_rate_period_seconds,
            'adaptive_scale_up': adaptive_scale_up,
            'max_scale_down_rate_percentage': max_scale_down_rate_percentage,
        }
        explicitly_set_logical_fields = [
            name for name, value in logical_scaling_fields.items()
            if value is not None
        ]
        if (explicitly_set_logical_fields and not uses_logical_replicas and
                not preserves_legacy_placement_contract):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    f'{", ".join(explicitly_set_logical_fields)} require '
                    'logical replicas with spot_placer: '
                    f'{placement_policy.CAPACITY_AWARE_SPOT_PLACER}.')
        if uses_logical_replicas:
            if (not isinstance(target_concurrency_per_replica, int) or
                    isinstance(target_concurrency_per_replica, bool)):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'dynamic_fallback_per_gpu requires '
                        'target_concurrency_per_replica to be a positive '
                        'integer so logical GPU targets remain whole slots. '
                        f'Got: {target_concurrency_per_replica!r}')
            if graceful_drain_async_occupancy is not True:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'dynamic_fallback_per_gpu requires '
                        'graceful_drain_async_occupancy: true so scale-down '
                        'can prove that no asynchronous work is running.')
            # Reserved fill is safe for logical fleets when every Kubernetes
            # fill shape is one GPU, so one broker slot is one logical slot.
            # The task-level validator enforces that resource invariant (the
            # service spec alone does not carry the task's resource shapes).

        if target_qps_per_replica is not None:
            if max_replicas is None:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError('max_replicas must be set where '
                                     'target_qps_per_replica is set.')
        elif target_concurrency_per_replica is not None:
            if max_replicas is None:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError('max_replicas must be set where '
                                     'target_concurrency_per_replica is set.')
        else:
            # Allow different min/max replicas for pools with queue-length
            # autoscaling
            if (not is_pool and max_replicas is not None and
                    max_replicas != min_replicas):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'Detected different min_replicas and max_replicas '
                        'while neither target_qps_per_replica nor '
                        'target_concurrency_per_replica is set. To enable '
                        'autoscaling, please set one of them.')

        if not readiness_path.startswith('/'):
            with ux_utils.print_exception_no_traceback():
                raise ValueError('readiness_path must start with a slash (/). '
                                 f'Got: {readiness_path}')

        # Add the check for unknown load balancing policies
        if (load_balancing_policy is not None and
                load_balancing_policy not in serve.LB_POLICIES):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    f'Unknown load balancing policy: {load_balancing_policy}. '
                    f'Available policies: {list(serve.LB_POLICIES.keys())}')

        if target_concurrency_per_replica is not None:
            # The concurrency autoscaler sizes on the LB's in-flight gauges;
            # a policy that does not track per-replica load (e.g.
            # round_robin) would leave the autoscaler blind, so reject at
            # spec load rather than at runtime. `make_policy_name` resolves
            # None to the default policy (least_load), which does track.
            resolved_policy = lb_policies.LoadBalancingPolicy.make_policy_name(
                load_balancing_policy)
            policy_cls = lb_policies.LB_POLICIES.get(resolved_policy)
            if policy_cls is None or not issubclass(
                    policy_cls, lb_policies.LeastLoadPolicy):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'target_concurrency_per_replica requires a '
                        'load-tracking load balancing policy (e.g. '
                        'least_load or instance_aware_least_load). '
                        f'Got: {resolved_policy}')

        if graceful_drain_seconds is not None and (
                not isinstance(graceful_drain_seconds, int) or
                isinstance(graceful_drain_seconds, bool) or
                graceful_drain_seconds < 0 or graceful_drain_seconds
                > constants.LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS):
            # The upper bound keeps the drain within the window the LB
            # retains (and reports) a retiring replica's unknown async
            # occupancy; a longer cap could end early once that retention
            # expires.
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'graceful_drain_seconds must be an integer between 0 and '
                    f'{constants.LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS}. '
                    f'Got: {graceful_drain_seconds!r}')
        if lb_request_queue is not None:
            queue_defaults: dict[str, Any] = {
                'min_size': constants.LB_REQUEST_QUEUE_MIN_SIZE,
                'size_per_replica': constants.LB_REQUEST_QUEUE_SIZE_PER_REPLICA,
                'max_size': constants.LB_REQUEST_QUEUE_MAX_SIZE,
                'max_concurrency_per_replica':
                    constants.LB_REQUEST_QUEUE_CONCURRENCY_PER_REPLICA,
                'max_concurrency': constants.LB_REQUEST_QUEUE_MAX_CONCURRENCY,
                'timeout_seconds': constants.LB_REQUEST_QUEUE_TIMEOUT_SECONDS,
                'max_request_body_bytes':
                    constants.LB_REQUEST_QUEUE_MAX_BODY_BYTES,
                'use_async_occupancy': False,
            }
            queue_defaults.update(lb_request_queue)
            if (queue_defaults['use_async_occupancy'] and
                    'max_concurrency_per_replica' not in lb_request_queue):
                # Async occupancy contributes each replica's probed slots, so
                # an implicit value of one would silently disable multi-worker
                # replicas. The fleet-wide cap remains the safety ceiling;
                # callers can still opt into a stricter per-replica cap.
                queue_defaults['max_concurrency_per_replica'] = (
                    queue_defaults['max_concurrency'])
            for field in ('min_size', 'size_per_replica', 'max_size',
                          'max_concurrency_per_replica', 'max_concurrency',
                          'max_request_body_bytes'):
                value = queue_defaults[field]
                minimum = 0 if field in ('min_size', 'size_per_replica') else 1
                if (not isinstance(value, int) or isinstance(value, bool) or
                        value < minimum):
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            f'load_balancer.request_queue.{field} must be an '
                            f'integer >= {minimum}. Got: {value!r}')
            field_maximums = {
                'min_size': constants.LB_REQUEST_QUEUE_MAX_SIZE_LIMIT,
                'size_per_replica': constants.LB_REQUEST_QUEUE_MAX_SIZE_LIMIT,
                'max_size': constants.LB_REQUEST_QUEUE_MAX_SIZE_LIMIT,
                'max_concurrency_per_replica':
                    constants.LB_REQUEST_QUEUE_MAX_CONCURRENCY_LIMIT,
                'max_concurrency':
                    constants.LB_REQUEST_QUEUE_MAX_CONCURRENCY_LIMIT,
                'max_request_body_bytes':
                    constants.LB_REQUEST_QUEUE_MAX_BODY_BYTES_LIMIT,
            }
            for field, maximum in field_maximums.items():
                value = queue_defaults[field]
                if value > maximum:
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            f'load_balancer.request_queue.{field} must be <= '
                            f'{maximum}. Got: {value}')
            timeout_seconds = queue_defaults['timeout_seconds']
            if (not isinstance(timeout_seconds, (int, float)) or
                    isinstance(timeout_seconds, bool) or timeout_seconds <= 0 or
                    not math.isfinite(timeout_seconds)):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'load_balancer.request_queue.timeout_seconds must be '
                        f'a finite number > 0. Got: {timeout_seconds!r}')
            thresholds: Any = queue_defaults.get('timeout_seconds_by_priority',
                                                 [])
            if not isinstance(thresholds, list):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'load_balancer.request_queue.timeout_seconds_by_'
                        f'priority must be a list. Got: {thresholds!r}')
            normalized_thresholds: list[dict[str, int | float]] = []
            previous_priority = -1
            for threshold in thresholds:
                if not isinstance(threshold, dict):
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            'load_balancer.request_queue.timeout_seconds_by_'
                            'priority entries must be objects.')
                min_priority = threshold.get('min_priority')
                threshold_timeout = threshold.get('timeout_seconds')
                if (not isinstance(min_priority, int) or
                        isinstance(min_priority, bool) or
                        not 0 <= min_priority <= 100 or
                        min_priority <= previous_priority):
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            'load_balancer.request_queue.timeout_seconds_by_'
                            'priority min_priority values must be unique, '
                            'strictly increasing integers from 0 to 100. '
                            f'Got: {min_priority!r}')
                if (not isinstance(threshold_timeout, (int, float)) or
                        isinstance(threshold_timeout, bool) or
                        threshold_timeout <= 0 or
                        not math.isfinite(threshold_timeout)):
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            'load_balancer.request_queue.timeout_seconds_by_'
                            'priority timeout_seconds must be a finite '
                            f'number > 0. Got: {threshold_timeout!r}')
                normalized_thresholds.append({
                    'min_priority': min_priority,
                    'timeout_seconds': threshold_timeout,
                })
                previous_priority = min_priority
            queue_defaults['timeout_seconds_by_priority'] = (
                normalized_thresholds)
            use_async_occupancy = queue_defaults['use_async_occupancy']
            if not isinstance(use_async_occupancy, bool):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'load_balancer.request_queue.use_async_occupancy must '
                        f'be a boolean. Got: {use_async_occupancy!r}')
            if queue_defaults['min_size'] > queue_defaults['max_size']:
                min_size = queue_defaults['min_size']
                max_size = queue_defaults['max_size']
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'load_balancer.request_queue.min_size must not exceed '
                        f'max_size. Got: min_size={min_size}, '
                        f'max_size={max_size}')
            body_memory = (queue_defaults['max_concurrency'] *
                           queue_defaults['max_request_body_bytes'])
            if (body_memory
                    > constants.LB_REQUEST_QUEUE_BODY_MEMORY_BUDGET_BYTES):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'load_balancer.request_queue max_concurrency * '
                        'max_request_body_bytes must not exceed the '
                        f'{constants.LB_REQUEST_QUEUE_BODY_MEMORY_BUDGET_BYTES}'
                        f'-byte buffering budget. Got: {body_memory}')
            lb_request_queue = queue_defaults
        self._readiness_path: str = readiness_path
        self._initial_delay_seconds: int = initial_delay_seconds
        self._readiness_timeout_seconds: int = readiness_timeout_seconds
        self._endpoint_probe_interval_seconds: int = (
            endpoint_probe_interval_seconds)
        self._lb_stream_timeout_seconds: int = lb_stream_timeout_seconds
        self._lb_retriable_status_codes: list[int] | None = (
            lb_retriable_status_codes)
        self._lb_max_retries: int | None = lb_max_retries
        self._lb_retry_initial_backoff_seconds: float | None = (
            lb_retry_initial_backoff_seconds)
        self._lb_request_queue: dict[str, Any] | None = lb_request_queue
        self._lb_high_availability = bool(lb_high_availability)
        # YAML parsing sets this marker after construction. A missing field is
        # a creation default, not an instruction to migrate an existing
        # service during its next ordinary update.
        self._lb_high_availability_specified = False
        self._graceful_drain_seconds: int | None = graceful_drain_seconds
        # Declares fast-ack work whose lifetime outlives its HTTP envelope.
        # The LB must treat a missing occupancy sample as unknown from the
        # first request, rather than inferring capability from a successful
        # probe (which cannot protect a never-probed replica).
        self._graceful_drain_async_occupancy: bool | None = (
            graceful_drain_async_occupancy)
        self._min_replicas: int = min_replicas
        self._max_replicas: int | None = max_replicas
        self._min_replicas_by_accelerator = accelerator_floors
        self._num_overprovision: int | None = num_overprovision
        self._ports: str | None = ports
        self._target_qps_per_replica: float | dict[
            str, float] | None = target_qps_per_replica
        # Per-GPU target concurrency: replica capacity = knob * gpu_count.
        self._target_concurrency_per_replica: float | None = (
            target_concurrency_per_replica)
        self._target_utilization_percentage: int | None = (
            target_utilization_percentage)
        self._expected_request_duration_seconds: float | None = (
            expected_request_duration_seconds)
        self._initial_provision_lead_time_seconds: float | str | None = (
            initial_provision_lead_time_seconds)
        self._adaptive_demand_estimation: bool | None = (
            adaptive_demand_estimation)
        self._max_scale_up_rate_percentage: int | None = (
            max_scale_up_rate_percentage)
        self._scale_up_rate_min_replicas: int | None = (
            scale_up_rate_min_replicas)
        self._scale_up_rate_period_seconds: int | None = (
            scale_up_rate_period_seconds)
        self._adaptive_scale_up: dict[str, Any] | None = adaptive_scale_up
        self._max_scale_down_rate_percentage: int | None = (
            max_scale_down_rate_percentage)
        # Persist primitive placement dimensions, never the runtime dataclass,
        # so the preceding server can ignore the new fields during rollback.
        # Contract v1 dual-writes its historical logical marker; runtime policy
        # reads placement_contract instead of the mirror.
        self.__dict__.update(
            resolved_placement_contract.persisted_fields(
                placement_policy.PLACEMENT_CONTRACT_VERSION_TRANSITION))
        self._uses_logical_replicas: bool = uses_logical_replicas
        # Opt-in: allow scaling up onto free reserved (zero-cost) capacity.
        # Absent/False means no behavior change. Bool form or object form
        # ({floor_replicas, weight}); object form implies enabled.
        # Normalize the utilization policy into the persisted representation.
        # Newly constructed/parsed enabled specs default to activity-backed
        # fill. __setstate__ separately maps old representations that lack the
        # key to explicit False, so a controller restart does not silently
        # change an existing service before an intentional update.
        if isinstance(reserved_capacity_fill, dict):
            normalized_reserved_fill = dict(reserved_capacity_fill)
            normalized_reserved_fill.setdefault('utilization_gate', True)
        elif reserved_fill_enabled:
            normalized_reserved_fill = {'utilization_gate': True}
        else:
            normalized_reserved_fill = reserved_capacity_fill
        self._reserved_capacity_fill: bool | dict[
            str, Any] | None = normalized_reserved_fill
        # Absent/None is the default policy: enabled when a placer supplies a
        # candidate catalog. False is the durable opt-out for both newly
        # parsed and pre-existing persisted service specs.
        self._cost_rebalance: bool | dict[str, Any] | None = cost_rebalance
        self._post_data: dict[str, Any] | None = post_data
        self._tls_credential: serve_utils.TLSCredential | None = (
            tls_credential)
        self._readiness_headers: dict[str, str] | None = readiness_headers
        self._dynamic_ondemand_fallback: bool | None = dynamic_ondemand_fallback
        self._base_ondemand_fallback_replicas: int | None = base_ondemand_fallback_replicas
        self._spot_placer: str | None = spot_placer
        self._upscale_delay_seconds: int | None = upscale_delay_seconds
        self._downscale_delay_seconds: int | None = downscale_delay_seconds
        self._load_balancing_policy: str | None = load_balancing_policy
        self._pool: bool | dict[str, Any] | None = _canonical_pool_driver(
            pool, min_replicas, max_replicas)
        self._queue_length_threshold: int | None = queue_length_threshold
        self._consecutive_failure_threshold_timeout: int | None = (
            consecutive_failure_threshold_timeout)

        self._use_ondemand_fallback: bool = (
            self.dynamic_ondemand_fallback is not None and
            self.dynamic_ondemand_fallback) or (
                self.base_ondemand_fallback_replicas is not None and
                self.base_ondemand_fallback_replicas > 0)

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Set state from pickled state, for backward compatibility."""
        try:
            resolved_contract, contract_version = (
                placement_policy.decode_contract_state(state))
        except (TypeError, ValueError):
            present_fields = sum(
                field in state for field in placement_policy.CONTRACT_FIELDS)
            logger.error(
                f'event={_PLACEMENT_DECODE_EVENT} outcome=rejected '
                f'contract_fields_present={present_fields} '
                f'policy_present={placement_policy.POLICY_NAME_FIELD in state} '
                f'pool_present={placement_policy.POOL_FIELD in state} '
                f'mirror_present='
                f'{placement_policy.ROLLBACK_REPLICA_UNIT_FIELD in state}')
            raise
        if contract_version is None:
            logger.warning(f'event={_PLACEMENT_DECODE_EVENT} '
                           'outcome=legacy_materialized '
                           f'replica_unit={resolved_contract.replica_unit} '
                           f'workload_kind={resolved_contract.workload_kind}')
            # Materialize legacy state in memory.  The authoritative persisted
            # version bytes remain untouched; a deliberate new copy is a v1
            # transition write with the rollback marker.
            state.setdefault(placement_policy.POLICY_NAME_FIELD, None)
            state.setdefault(placement_policy.POOL_FIELD, False)
            legacy_pool = state[placement_policy.POOL_FIELD]
            if resolved_contract.workload_kind == (
                    placement_policy.WORKLOAD_KIND_POOL):
                state[placement_policy.POOL_FIELD] = _canonical_pool_driver(
                    legacy_pool, state.get('_min_replicas', 0),
                    state.get('_max_replicas'))
            elif isinstance(legacy_pool, dict) and not legacy_pool:
                # Preserve the preceding reader's explicit meaning for an
                # empty legacy mapping: it was a service, not a pool.
                state[placement_policy.POOL_FIELD] = False
            state.update(
                resolved_contract.persisted_fields(
                    placement_policy.PLACEMENT_CONTRACT_VERSION_TRANSITION))
            state[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] = (
                resolved_contract.uses_logical_replicas)
        # These fields were added after earlier releases had already persisted
        # SkyServiceSpec objects in the serve DB.
        state.setdefault('_endpoint_probe_interval_seconds',
                         constants.DEFAULT_ENDPOINT_PROBE_INTERVAL_SECONDS)
        state.setdefault('_lb_stream_timeout_seconds',
                         constants.DEFAULT_LB_STREAM_TIMEOUT)
        state.setdefault('_lb_retriable_status_codes', None)
        state.setdefault('_lb_max_retries', None)
        state.setdefault('_lb_retry_initial_backoff_seconds', None)
        state.setdefault('_lb_request_queue', None)
        state.setdefault('_lb_high_availability', False)
        state.setdefault('_lb_high_availability_specified', False)
        state.setdefault('_consecutive_failure_threshold_timeout', None)
        state.setdefault('_pool', False)
        # Added with the concurrency autoscaler; old DB rows predate it.
        state.setdefault('_target_concurrency_per_replica', None)
        state.setdefault('_min_replicas_by_accelerator', {})
        state.setdefault('_target_utilization_percentage', None)
        state.setdefault('_expected_request_duration_seconds', None)
        state.setdefault('_initial_provision_lead_time_seconds', None)
        state.setdefault('_adaptive_demand_estimation', None)
        state.setdefault('_max_scale_up_rate_percentage', None)
        state.setdefault('_scale_up_rate_min_replicas', None)
        state.setdefault('_scale_up_rate_period_seconds', None)
        state.setdefault('_adaptive_scale_up', None)
        # Old persisted specs predate bounded downscale and preserve the
        # previous unlimited target adoption rather than silently changing on
        # an API-server restart. Newly parsed specs default to 50 via the
        # property below.
        state.setdefault('_max_scale_down_rate_percentage', 100)
        # Added with reserved-capacity fill; old DB rows predate it. M5 made
        # utilization gating the default for newly parsed enabled specs, but
        # old persisted bool/object forms did not make that choice. Normalize
        # those missing-key forms to the explicit legacy opt-out so restart is
        # behavior-preserving; an intentional service update reparses omitted
        # utilization_gate as True.
        if '_reserved_capacity_fill' not in state:
            state['_reserved_capacity_fill'] = None
        else:
            reserved_fill = state['_reserved_capacity_fill']
            if isinstance(reserved_fill, dict):
                reserved_fill = dict(reserved_fill)
                reserved_fill.setdefault('utilization_gate', False)
                state['_reserved_capacity_fill'] = reserved_fill
            elif reserved_fill:
                state['_reserved_capacity_fill'] = {'utilization_gate': False}
        state.setdefault('_cost_rebalance', None)
        # Added with the in-flight-aware graceful drain; old DB rows
        # predate it (None -> default drain semantics).
        state.setdefault('_graceful_drain_seconds', None)
        state.setdefault('_graceful_drain_async_occupancy', None)
        self.__dict__.update(state)

    @staticmethod
    def from_yaml_config(config: dict[str, Any]) -> 'SkyServiceSpec':
        common_utils.validate_schema(config, schemas.get_service_schema(),
                                     'Invalid service YAML: ')
        if 'replicas' in config and 'replica_policy' in config:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'Cannot specify both `replicas` and `replica_policy` in '
                    'the service YAML. Please use one of them.')

        service_config: dict[str, Any] = {}

        readiness_section = config.get('readiness_probe', '/')
        if isinstance(readiness_section, str):
            service_config['readiness_path'] = readiness_section
            initial_delay_seconds = None
            post_data = None
            readiness_timeout_seconds = None
            endpoint_probe_interval_seconds = None
            consecutive_failure_threshold_timeout = None
            readiness_headers = None
        else:
            service_config['readiness_path'] = readiness_section['path']
            initial_delay_seconds = readiness_section.get(
                'initial_delay_seconds', None)
            post_data = readiness_section.get('post_data', None)
            readiness_timeout_seconds = readiness_section.get(
                'timeout_seconds', None)
            endpoint_probe_interval_seconds = readiness_section.get(
                'endpoint_probe_interval_seconds', None)
            consecutive_failure_threshold_timeout = readiness_section.get(
                'consecutive_failure_threshold_timeout', None)
            readiness_headers = readiness_section.get('headers', None)
        if initial_delay_seconds is None:
            initial_delay_seconds = constants.DEFAULT_INITIAL_DELAY_SECONDS
        service_config['initial_delay_seconds'] = initial_delay_seconds
        if readiness_timeout_seconds is None:
            readiness_timeout_seconds = (
                constants.DEFAULT_READINESS_PROBE_TIMEOUT_SECONDS)
        service_config['readiness_timeout_seconds'] = readiness_timeout_seconds
        if endpoint_probe_interval_seconds is None:
            endpoint_probe_interval_seconds = (
                constants.DEFAULT_ENDPOINT_PROBE_INTERVAL_SECONDS)
        service_config['endpoint_probe_interval_seconds'] = (
            endpoint_probe_interval_seconds)
        service_config['consecutive_failure_threshold_timeout'] = (
            consecutive_failure_threshold_timeout)
        service_config['graceful_drain_seconds'] = config.get(
            'graceful_drain_seconds', None)
        service_config['graceful_drain_async_occupancy'] = config.get(
            'graceful_drain_async_occupancy', None)
        pool_config = config.get('pool', None)
        load_balancer_section = config.get('load_balancer', None)
        lb_stream_timeout_seconds = None
        if load_balancer_section is not None:
            lb_stream_timeout_seconds = load_balancer_section.get(
                'stream_timeout_seconds', None)
        if lb_stream_timeout_seconds is None:
            lb_stream_timeout_seconds = constants.DEFAULT_LB_STREAM_TIMEOUT
        service_config['lb_stream_timeout_seconds'] = lb_stream_timeout_seconds
        lb_retriable_status_codes = None
        if load_balancer_section is not None:
            lb_retriable_status_codes = load_balancer_section.get(
                'retriable_status_codes', None)
        service_config['lb_retriable_status_codes'] = lb_retriable_status_codes
        if load_balancer_section is not None:
            service_config['lb_max_retries'] = load_balancer_section.get(
                'max_retries', None)
            service_config['lb_retry_initial_backoff_seconds'] = (
                load_balancer_section.get('retry_initial_backoff_seconds',
                                          None))
            service_config['lb_request_queue'] = load_balancer_section.get(
                'request_queue', None)
            if 'high_availability' in load_balancer_section:
                logger.warning(
                    'load_balancer.high_availability is ignored and will be '
                    'removed. Load balancer high availability is always on '
                    'for services and never applies to pools, which have no '
                    'inference endpoint. Drop the field from the service '
                    'YAML. An existing service keeps its durable load '
                    'balancer mode until an explicit migration.')
        # The load balancer topology is derived, not chosen: warm-standby
        # for services, single-slot for pools. With a single slot the
        # controller marks every live replica occupancy-unknown during the
        # maxSurge overlap of each rollout (force_all_live_unknown is
        # unconditionally true when HA is off), and the logical retirement
        # gate then aborts an in-progress drain wave and returns its victims
        # to routing. Two slots remove that term. This does NOT make drain
        # proof survive a load balancer restart: a rollout replaces both
        # slots, so the tracker's acknowledgement resets either way (see
        # docs/designs/serve-drain-proof-across-lb-restarts.md).
        service_config['lb_high_availability'] = pool_config is None
        if isinstance(post_data, str):
            try:
                post_data = json.loads(post_data)
            except json.JSONDecodeError as e:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'Invalid JSON string for `post_data` in the '
                        '`readiness_probe` section of your service YAML.'
                    ) from e
        service_config['post_data'] = post_data
        service_config['readiness_headers'] = readiness_headers

        ports = config.get('ports', None)
        if ports is not None:
            assert isinstance(ports, int)
            if not 1 <= ports <= 65535:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError('Port must be between 1 and 65535.')
        service_config['ports'] = str(ports) if ports is not None else None

        if pool_config is not None:
            service_config['pool'] = pool_config

        policy_section = config.get('replica_policy', None)
        if policy_section is not None and pool_config is not None:
            with ux_utils.print_exception_no_traceback():
                raise ValueError('Cannot specify `replica_policy` for cluster '
                                 'pool. Only `workers: <num>` or `min_workers: '
                                 '<num> max_workers: <num>` is supported '
                                 'for pool now.')

        simplified_policy_section = config.get('replicas', None)
        workers_config = config.get('workers', None)
        if simplified_policy_section is not None and workers_config is not None:
            with ux_utils.print_exception_no_traceback():
                raise ValueError('Cannot specify both `replicas` and `workers`.'
                                 ' Please use one of them.')
        if (simplified_policy_section is not None and pool_config is not None):
            with ux_utils.print_exception_no_traceback():
                raise ValueError('Cannot specify `replicas` for pool. '
                                 'Please use `workers` instead.')
        if simplified_policy_section is None:
            simplified_policy_section = workers_config

        # Parse pool config if it's a dict (for autoscaling support)
        queue_length_threshold = None
        pool_min_workers = None
        pool_max_workers = None
        pool_upscale_delay = None
        pool_downscale_delay = None
        pool_spot_placer = None
        if pool_config is not None and isinstance(pool_config, dict):
            queue_length_threshold = pool_config.get('queue_length_threshold',
                                                     None)
            pool_min_workers = pool_config.get('min_workers', None)
            pool_max_workers = pool_config.get('max_workers', None)
            pool_upscale_delay = pool_config.get('upscale_delay_seconds', None)
            pool_downscale_delay = pool_config.get('downscale_delay_seconds',
                                                   None)
            pool_spot_placer = pool_config.get('spot_placer', None)
            workers_config = pool_config.get('workers', workers_config)
            # Validate: one of workers or max_workers and min_workers must be
            # set.
            if (pool_min_workers is None and pool_max_workers is None and
                    workers_config is None):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'One of workers, or both min_workers and max_workers'
                        ' must be set for pool autoscaling.')
            # Validate: if queue_length_threshold is set, max_workers must also
            # be set
            if queue_length_threshold is not None and pool_max_workers is None:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'max_workers must be set when queue_length_threshold '
                        'is specified for pool autoscaling.')
            # Validate: if min_workers is set, max_workers must also be set
            if pool_min_workers is not None and pool_max_workers is None:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'max_workers must be set when min_workers is '
                        'specified for pool autoscaling.')
            # Validate: min_workers <= max_workers when both are set
            if pool_min_workers is not None and pool_max_workers is not None:
                if pool_min_workers > pool_max_workers:
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            f'min_workers ({pool_min_workers}) must be <= '
                            f'max_workers ({pool_max_workers}) for pool '
                            'autoscaling.')
        if policy_section is None or simplified_policy_section is not None:
            if simplified_policy_section is not None:
                min_replicas = simplified_policy_section
            elif workers_config is not None:
                # Use workers_config from pool dict if available
                min_replicas = workers_config
            else:
                min_replicas = constants.DEFAULT_MIN_REPLICAS
            # For pools with autoscaling set the relevant config values.
            if pool_config is not None and pool_max_workers is not None:
                if queue_length_threshold is None:
                    queue_length_threshold = (
                        constants.AUTOSCALER_DEFAULT_QUEUE_LENGTH_THRESHOLD)
                    logger.info(
                        'Set default queue_length_threshold='
                        f'{queue_length_threshold} for pool with max_workers='
                        f'{pool_max_workers}')
                min_replicas = (pool_min_workers if pool_min_workers is not None
                                else min_replicas)
            service_config['min_replicas'] = min_replicas
            service_config['min_replicas_by_accelerator'] = None
            service_config['max_replicas'] = pool_max_workers
            service_config['upscale_delay_seconds'] = pool_upscale_delay
            service_config['downscale_delay_seconds'] = pool_downscale_delay
            service_config['num_overprovision'] = None
            service_config['target_qps_per_replica'] = None
            service_config['target_concurrency_per_replica'] = None
            service_config['target_utilization_percentage'] = None
            service_config['expected_request_duration_seconds'] = None
            service_config['initial_provision_lead_time_seconds'] = None
            service_config['adaptive_demand_estimation'] = None
            service_config['max_scale_up_rate_percentage'] = None
            service_config['scale_up_rate_min_replicas'] = None
            service_config['scale_up_rate_period_seconds'] = None
            service_config['adaptive_scale_up'] = None
            service_config['max_scale_down_rate_percentage'] = None
            service_config['reserved_capacity_fill'] = None
            service_config['cost_rebalance'] = None
            service_config['spot_placer'] = pool_spot_placer
        else:
            service_config['min_replicas'] = policy_section['min_replicas']
            service_config['min_replicas_by_accelerator'] = policy_section.get(
                'min_replicas_by_accelerator', None)
            service_config['max_replicas'] = policy_section.get(
                'max_replicas', None)
            service_config['num_overprovision'] = policy_section.get(
                'num_overprovision', None)
            service_config['target_qps_per_replica'] = policy_section.get(
                'target_qps_per_replica', None)
            service_config['target_concurrency_per_replica'] = (
                policy_section.get('target_concurrency_per_replica', None))
            for field in ('target_utilization_percentage',
                          'expected_request_duration_seconds',
                          'initial_provision_lead_time_seconds',
                          'adaptive_demand_estimation',
                          'max_scale_up_rate_percentage',
                          'scale_up_rate_min_replicas',
                          'scale_up_rate_period_seconds', 'adaptive_scale_up',
                          'max_scale_down_rate_percentage'):
                service_config[field] = policy_section.get(field, None)
            service_config['reserved_capacity_fill'] = policy_section.get(
                'reserved_capacity_fill', None)
            service_config['cost_rebalance'] = policy_section.get(
                'cost_rebalance', None)
            service_config['upscale_delay_seconds'] = policy_section.get(
                'upscale_delay_seconds', None)
            service_config['downscale_delay_seconds'] = policy_section.get(
                'downscale_delay_seconds', None)
            service_config[
                'base_ondemand_fallback_replicas'] = policy_section.get(
                    'base_ondemand_fallback_replicas', None)
            service_config['dynamic_ondemand_fallback'] = policy_section.get(
                'dynamic_ondemand_fallback', None)
            service_config['spot_placer'] = policy_section.get(
                'spot_placer', None)

        # Set queue_length_threshold from pool config
        service_config['queue_length_threshold'] = queue_length_threshold

        service_config['load_balancing_policy'] = config.get(
            'load_balancing_policy', None)

        # Validate instance-aware settings
        target_qps_per_replica = service_config['target_qps_per_replica']
        target_concurrency_per_replica = service_config.get(
            'target_concurrency_per_replica', None)
        load_balancing_policy = service_config['load_balancing_policy']
        accelerator_floors = service_config.get(
            'min_replicas_by_accelerator') or {}

        if (accelerator_floors and
                load_balancing_policy != 'instance_aware_least_load'):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'min_replicas_by_accelerator requires '
                    'load_balancing_policy: instance_aware_least_load.')

        if isinstance(target_qps_per_replica, dict):
            if load_balancing_policy != 'instance_aware_least_load':
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'When using dict type target_qps_per_replica, '
                        'load_balancing_policy must be '
                        '"instance_aware_least_load".')
            # On-demand fallback routes to FallbackRequestRateAutoscaler,
            # which sizes with the float-only request-rate math and rejects
            # dict targets at runtime (after the service is already up).
            if (service_config.get('dynamic_ondemand_fallback') or
                    service_config.get('base_ondemand_fallback_replicas')):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'Dict type target_qps_per_replica is not supported '
                        'with on-demand fallback '
                        '(dynamic_ondemand_fallback / '
                        'base_ondemand_fallback_replicas).')

        if target_concurrency_per_replica is not None:
            # Same trap as the dict-qps case above: on-demand fallback
            # selects FallbackRequestRateAutoscaler in from_spec, which
            # would silently ignore the concurrency knob (or, with the
            # knob checked first, silently drop the user's spot-safety
            # fallback config). Reject at load instead of at runtime.
            if (service_config.get('dynamic_ondemand_fallback') or
                    service_config.get('base_ondemand_fallback_replicas')):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'target_concurrency_per_replica is not supported '
                        'with on-demand fallback '
                        '(dynamic_ondemand_fallback / '
                        'base_ondemand_fallback_replicas).')

        if load_balancing_policy == 'instance_aware_least_load':
            # The per-GPU concurrency knob carries its own shape-aware
            # capacity model (knob * gpu_count), so the QPS dict is only
            # mandatory when it is the sizing signal.
            if (target_concurrency_per_replica is None and
                    not isinstance(target_qps_per_replica, dict)):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'When using "instance_aware_least_load" policy, '
                        'target_qps_per_replica must be a '
                        'dict mapping GPU types to QPS values.')

        tls_section = config.get('tls', None)
        if tls_section is not None:
            service_config['tls_credential'] = serve_utils.TLSCredential(
                keyfile=tls_section.get('keyfile', None),
                certfile=tls_section.get('certfile', None),
            )

        # No YAML can assert a load balancer mode any more, so a parsed spec
        # never carries one. An existing service therefore keeps its durable
        # mode across unrelated updates; moving it onto warm-standby stays an
        # explicit migration. A brand new service has no durable mode and
        # takes the derived value above.
        spec = SkyServiceSpec(**service_config)
        return spec

    @staticmethod
    def from_yaml_str(yaml_str: str) -> 'SkyServiceSpec':
        config = yaml_utils.safe_load(yaml_str)

        if isinstance(config, str):
            with ux_utils.print_exception_no_traceback():
                raise ValueError('YAML loaded as str, not as dict. '
                                 f'Is it correct? content:\n{yaml_str}')

        if config is None:
            config = {}

        if 'service' not in config:
            with ux_utils.print_exception_no_traceback():
                raise ValueError('Service YAML must have a "service" section. '
                                 f'Is it correct? content:\n{yaml_str}')

        return SkyServiceSpec.from_yaml_config(config['service'])

    @staticmethod
    def from_yaml(yaml_path: str) -> 'SkyServiceSpec':
        with open(os.path.expanduser(yaml_path), encoding='utf-8') as f:
            yaml_content = f.read()
        return SkyServiceSpec.from_yaml_str(yaml_content)

    def to_yaml_config(self) -> dict[str, Any]:
        config: dict[str, Any] = {}

        def add_if_not_none(section: str,
                            key: str | None,
                            value: Any,
                            no_empty: bool = False):
            if no_empty and not value:
                return
            if value is not None:
                if key is None:
                    config[section] = value
                else:
                    if section not in config:
                        config[section] = dict()
                    config[section][key] = value

        # Rendering must not mutate the persisted rollback driver when nested
        # fields are added below.
        rendered_pool = (dict(self._pool)
                         if isinstance(self._pool, dict) else self._pool)
        add_if_not_none('pool', None, rendered_pool)
        # Emitted before the pool early-return: the field is service-level
        # and must survive serialization for pools too (their retirement
        # uses the same bounded drain, just without the in-flight gauge).
        add_if_not_none('graceful_drain_seconds', None,
                        self.graceful_drain_seconds)
        add_if_not_none('graceful_drain_async_occupancy', None,
                        self.graceful_drain_async_occupancy)

        if self.pool:
            add_if_not_none('pool', 'spot_placer', self.spot_placer)
            if self.max_replicas is not None:
                add_if_not_none('pool', 'max_workers', self.max_replicas)
                add_if_not_none('pool', 'queue_length_threshold',
                                self.queue_length_threshold)
                add_if_not_none('pool', 'min_workers', self.min_replicas)
                add_if_not_none('pool', 'upscale_delay_seconds',
                                self.upscale_delay_seconds)
                add_if_not_none('pool', 'downscale_delay_seconds',
                                self.downscale_delay_seconds)
            else:
                add_if_not_none('pool', 'workers', self.min_replicas)
            return config

        add_if_not_none('readiness_probe', 'path', self.readiness_path)
        add_if_not_none('readiness_probe', 'initial_delay_seconds',
                        self.initial_delay_seconds)
        add_if_not_none('readiness_probe', 'post_data', self.post_data)
        add_if_not_none('readiness_probe', 'timeout_seconds',
                        self.readiness_timeout_seconds)
        # Omit default-valued newer fields to preserve compatibility with
        # older controllers that do not recognize them during serve update.
        if (self.endpoint_probe_interval_seconds
                != constants.DEFAULT_ENDPOINT_PROBE_INTERVAL_SECONDS):
            add_if_not_none('readiness_probe',
                            'endpoint_probe_interval_seconds',
                            self.endpoint_probe_interval_seconds)
        add_if_not_none('readiness_probe',
                        'consecutive_failure_threshold_timeout',
                        self.consecutive_failure_threshold_timeout)
        if (self.lb_stream_timeout_seconds
                != constants.DEFAULT_LB_STREAM_TIMEOUT):
            add_if_not_none('load_balancer', 'stream_timeout_seconds',
                            self.lb_stream_timeout_seconds)
        add_if_not_none('load_balancer', 'retriable_status_codes',
                        self.lb_retriable_status_codes)
        add_if_not_none('load_balancer', 'max_retries', self.lb_max_retries)
        add_if_not_none('load_balancer', 'retry_initial_backoff_seconds',
                        self.lb_retry_initial_backoff_seconds)
        add_if_not_none('load_balancer', 'request_queue', self.lb_request_queue)
        # high_availability is deliberately not emitted. Both sides derive the
        # same mode from pool-ness, so the round trip needs no carrier, and
        # emitting one would only re-trigger the ignored-field warning. An
        # older server parsing this config derives the identical value from
        # the field's absence.
        add_if_not_none('readiness_probe', 'headers', self._readiness_headers)
        add_if_not_none('replica_policy', 'min_replicas', self.min_replicas)
        add_if_not_none('replica_policy',
                        'min_replicas_by_accelerator',
                        self.min_replicas_by_accelerator,
                        no_empty=True)
        add_if_not_none('replica_policy', 'max_replicas', self.max_replicas)
        add_if_not_none('replica_policy', 'num_overprovision',
                        self.num_overprovision)
        add_if_not_none('replica_policy', 'target_qps_per_replica',
                        self.target_qps_per_replica)
        add_if_not_none('replica_policy', 'target_concurrency_per_replica',
                        self.target_concurrency_per_replica)
        replica_policy_values = (
            ('target_utilization_percentage',
             self._target_utilization_percentage),
            ('expected_request_duration_seconds',
             self._expected_request_duration_seconds),
            ('initial_provision_lead_time_seconds',
             self._initial_provision_lead_time_seconds),
            ('adaptive_demand_estimation', self._adaptive_demand_estimation),
            ('max_scale_up_rate_percentage',
             self._max_scale_up_rate_percentage),
            ('scale_up_rate_min_replicas', self._scale_up_rate_min_replicas),
            ('scale_up_rate_period_seconds',
             self._scale_up_rate_period_seconds),
            ('adaptive_scale_up', self._adaptive_scale_up),
            ('max_scale_down_rate_percentage',
             self._max_scale_down_rate_percentage),
        )
        for field, value in replica_policy_values:
            add_if_not_none('replica_policy', field, value)
        # no_empty omits the disabled None/False forms. Enabled fill always
        # serializes as an object so its utilization policy is unambiguous
        # across server versions.
        reserved_fill_config: bool | dict[str, Any] | None = (
            self._reserved_capacity_fill)
        if isinstance(self._reserved_capacity_fill, dict):
            fill_obj: dict[str, Any] = {}
            if self.reserved_fill_floor_replicas != 0:
                fill_obj['floor_replicas'] = self.reserved_fill_floor_replicas
            if self.reserved_fill_weight != 1.0:
                fill_obj['weight'] = self.reserved_fill_weight
            # Always serialize the policy explicitly. A new client may send
            # this YAML to a pre-M5 server, whose default is False, so omitting
            # True would silently change a gated service back to static fill.
            # Keeping False explicit is equally important for new servers.
            fill_obj['utilization_gate'] = self.reserved_fill_utilization_gate
            reserved_fill_config = fill_obj
        add_if_not_none('replica_policy',
                        'reserved_capacity_fill',
                        reserved_fill_config,
                        no_empty=True)
        add_if_not_none('replica_policy', 'cost_rebalance',
                        self._cost_rebalance)
        add_if_not_none('replica_policy', 'dynamic_ondemand_fallback',
                        self.dynamic_ondemand_fallback)
        add_if_not_none('replica_policy', 'base_ondemand_fallback_replicas',
                        self.base_ondemand_fallback_replicas)
        add_if_not_none('replica_policy', 'spot_placer', self.spot_placer)
        add_if_not_none('replica_policy', 'upscale_delay_seconds',
                        self.upscale_delay_seconds)
        add_if_not_none('replica_policy', 'downscale_delay_seconds',
                        self.downscale_delay_seconds)
        add_if_not_none('load_balancing_policy', None,
                        self._load_balancing_policy)
        add_if_not_none('ports', None, int(self.ports) if self.ports else None)
        if self.tls_credential is not None:
            add_if_not_none('tls', 'keyfile', self.tls_credential.keyfile)
            add_if_not_none('tls', 'certfile', self.tls_credential.certfile)
        return config

    def probe_str(self):
        if self.post_data is None:
            method = f'GET {self.readiness_path}'
        else:
            method = f'POST {self.readiness_path} {json.dumps(self.post_data)}'
        headers = ('' if self.readiness_headers is None else
                   ' with custom headers')
        return f'{method}{headers}'

    def spot_policy_str(self) -> str:
        policy_strs: list[str] = []
        if (self.dynamic_ondemand_fallback is not None and
                self.dynamic_ondemand_fallback):
            if self.spot_placer is not None:
                if self.spot_placer == placement_policy.SPOT_HEDGE_PLACER:
                    return 'SpotHedge'
            policy_strs.append('Dynamic on-demand fallback')
            if self.base_ondemand_fallback_replicas is not None:
                policy_strs.append(
                    f'with {self.base_ondemand_fallback_replicas}'
                    'base on-demand replicas')
        else:
            if self.base_ondemand_fallback_replicas is not None:
                plural = (''
                          if self.base_ondemand_fallback_replicas == 1 else 's')
                policy_strs.append('Static spot mixture with '
                                   f'{self.base_ondemand_fallback_replicas} '
                                   f'base on-demand replica{plural}')
        if self.spot_placer is not None:
            if not policy_strs:
                policy_strs.append('Spot placement')
            policy_strs.append(f'with {self.spot_placer} placer')
        if not policy_strs:
            return 'No spot policy'
        return ' '.join(policy_strs)

    def autoscaling_policy_str(self):
        if self.pool:
            if self.queue_length_threshold is not None:
                # Autoscaling pool
                max_plural = '' if self.max_replicas == 1 else 's'
                min_plural = '' if self.min_replicas == 1 else 's'
                return (f'Autoscaling from {self.min_replicas} to '
                        f'{self.max_replicas} worker{max_plural} '
                        f'(queue threshold: {self.queue_length_threshold})')
            # Fixed-size pool
            return f'Fixed-size ({self.min_replicas} workers)'
        # TODO(MaoZiming): Update policy_str
        noun = 'worker' if self.pool else 'replica'
        min_plural = '' if self.min_replicas == 1 else 's'
        if self.max_replicas == self.min_replicas or self.max_replicas is None:
            return f'Fixed {self.min_replicas} {noun}{min_plural}'
        # TODO(tian): Refactor to contain more information
        max_plural = '' if self.max_replicas == 1 else 's'
        overprovision_str = ''
        if self.num_overprovision is not None:
            overprovision_str = (
                f' with {self.num_overprovision} overprovisioned replicas')
        # This runs on every service record build (serve_state.get_service),
        # so it must render (not assert) for every valid autoscaling spec.
        if self.target_concurrency_per_replica is not None:
            utilization = self.target_utilization_percentage
            return (
                f'Autoscaling from {self.min_replicas} to {self.max_replicas} '
                f'{noun}{max_plural}{overprovision_str} '
                '(target_concurrency_per_replica: '
                f'{self.target_concurrency_per_replica} per GPU, '
                f'target utilization: {utilization}%)')
        # Already checked in __init__: a non-fixed, non-pool service without
        # the concurrency knob must carry the QPS knob.
        assert self.target_qps_per_replica is not None
        return (f'Autoscaling from {self.min_replicas} to {self.max_replicas} '
                f'{noun}{max_plural}{overprovision_str} (target QPS per '
                f'{noun}: {self.target_qps_per_replica})')

    def set_ports(self, ports: str) -> None:
        self._ports = ports

    def tls_str(self):
        if self.tls_credential is None:
            return 'No TLS Enabled'
        return (f'Keyfile: {self.tls_credential.keyfile}, '
                f'Certfile: {self.tls_credential.certfile}')

    def __repr__(self) -> str:
        if self.pool:
            return textwrap.dedent(f"""\
                Worker policy:  {self.autoscaling_policy_str()}
            """)
        return textwrap.dedent(f"""\
            Readiness probe method:           {self.probe_str()}
            Readiness initial delay seconds:  {self.initial_delay_seconds}
            Readiness probe timeout seconds:  {self.readiness_timeout_seconds}
            Replica autoscaling policy:       {self.autoscaling_policy_str()}
            TLS Certificates:                 {self.tls_str()}
            Spot Policy:                      {self.spot_policy_str()}
            Load Balancing Policy:            {self.load_balancing_policy}
        """)

    @property
    def readiness_path(self) -> str:
        return self._readiness_path

    @property
    def initial_delay_seconds(self) -> int:
        return self._initial_delay_seconds

    @property
    def readiness_timeout_seconds(self) -> int:
        return self._readiness_timeout_seconds

    @property
    def endpoint_probe_interval_seconds(self) -> int:
        return self._endpoint_probe_interval_seconds

    @property
    def lb_stream_timeout_seconds(self) -> int:
        return self._lb_stream_timeout_seconds

    @property
    def lb_retriable_status_codes(self) -> list[int] | None:
        return self._lb_retriable_status_codes

    @property
    def graceful_drain_seconds(self) -> int | None:
        return self._graceful_drain_seconds

    @property
    def graceful_drain_async_occupancy(self) -> bool | None:
        return self._graceful_drain_async_occupancy

    @property
    def lb_max_retries(self) -> int | None:
        return self._lb_max_retries

    @property
    def lb_retry_initial_backoff_seconds(self) -> float | None:
        return self._lb_retry_initial_backoff_seconds

    @property
    def lb_request_queue(self) -> dict[str, Any] | None:
        return self._lb_request_queue

    @property
    def lb_high_availability(self) -> bool:
        """Whether the service uses two controller-fenced LB slots."""
        return self._lb_high_availability

    @property
    def lb_high_availability_specified(self) -> bool:
        """Whether YAML explicitly selected the HA mode."""
        return self._lb_high_availability_specified

    @property
    def min_replicas(self) -> int:
        return self._min_replicas

    @property
    def min_replicas_by_accelerator(self) -> dict[str, int]:
        return dict(self._min_replicas_by_accelerator)

    @property
    def max_replicas(self) -> int | None:
        # If None, treated as having the same value of min_replicas.
        return self._max_replicas

    @property
    def num_overprovision(self) -> int | None:
        return self._num_overprovision

    @property
    def ports(self) -> str | None:
        return self._ports

    @property
    def target_qps_per_replica(self) -> float | dict[str, float] | None:
        return self._target_qps_per_replica

    @property
    def target_concurrency_per_replica(self) -> float | None:
        # Per GPU: replica capacity = knob * gpu_count. __setstate__
        # materializes the field for specs unpickled from old DB rows.
        return self._target_concurrency_per_replica

    @property
    def target_utilization_percentage(self) -> int:
        value = self._target_utilization_percentage
        return 100 if value is None else value

    @property
    def expected_request_duration_seconds(self) -> float | None:
        return self._expected_request_duration_seconds

    @property
    def initial_provision_lead_time_seconds(self) -> float | str | None:
        """Configured seed: a number, the 'auto' sentinel, or unset."""
        return self._initial_provision_lead_time_seconds

    @property
    def adaptive_demand_estimation(self) -> bool:
        # Default-on: measuring the workload is the correct behavior, and an
        # explicit false is the only way to keep static configured estimates.
        return self._adaptive_demand_estimation is not False

    @property
    def max_scale_up_rate_percentage(self) -> int | None:
        return self._max_scale_up_rate_percentage

    @property
    def scale_up_rate_min_replicas(self) -> int | None:
        return self._scale_up_rate_min_replicas

    @property
    def scale_up_rate_period_seconds(self) -> int | None:
        return self._scale_up_rate_period_seconds

    @property
    def adaptive_scale_up(self) -> dict[str, Any] | None:
        value = self._adaptive_scale_up
        return dict(value) if value is not None else None

    @property
    def max_scale_down_rate_percentage(self) -> int:
        value = self._max_scale_down_rate_percentage
        return 50 if value is None else value

    @property
    def replica_unit(self) -> str:
        return self.placement_contract.replica_unit

    @property
    def placement_contract(self) -> placement_policy.PlacementContract:
        contract, version = placement_policy.decode_contract_state(
            self.__dict__)
        if version is None:
            raise ValueError(
                'Runtime SkyServiceSpec is missing its materialized '
                'placement contract.')
        return contract

    @property
    def uses_logical_replicas(self) -> bool:
        return self.placement_contract.uses_logical_replicas

    @property
    def reserved_capacity_fill(self) -> bool:
        # Opt-in flag; absent (None) collapses to False so callers never
        # need to distinguish unset from disabled. The object form implies
        # enabled even when empty (all-defaults object == plain True).
        if isinstance(self._reserved_capacity_fill, dict):
            return True
        return bool(self._reserved_capacity_fill)

    @property
    def reserved_fill_floor_replicas(self) -> int:
        # Minimum number of fill replicas to keep; only the object form can
        # set it, everything else means no floor.
        if isinstance(self._reserved_capacity_fill, dict):
            return int(self._reserved_capacity_fill.get('floor_replicas', 0))
        return 0

    @property
    def reserved_fill_weight(self) -> float:
        # Relative weight of this service when brokering shared reserved
        # capacity; only the object form can set it.
        if isinstance(self._reserved_capacity_fill, dict):
            return float(self._reserved_capacity_fill.get('weight', 1.0))
        return 1.0

    @property
    def reserved_fill_utilization_gate(self) -> bool:
        # Reserved fill is activity-backed by default: an idle claimant
        # eventually releases its whole fill entitlement. The object form
        # provides a durable explicit opt-out for services whose reservation
        # must remain static even without observable utilization.
        if not self.reserved_capacity_fill:
            return False
        if isinstance(self._reserved_capacity_fill, dict):
            return bool(
                self._reserved_capacity_fill.get('utilization_gate', False))
        return True

    @property
    def cost_rebalance(self) -> bool:
        return (not self.pool and self.placement_contract.enabled and
                self._cost_rebalance is not False)

    @property
    def cost_rebalance_min_savings_fraction(self) -> float:
        if not isinstance(self._cost_rebalance, dict):
            return 0.3
        return float(self._cost_rebalance.get('min_savings_fraction', 0.3))

    @property
    def cost_rebalance_max_parallel_replacements(self) -> int:
        if not isinstance(self._cost_rebalance, dict):
            return 1
        return int(self._cost_rebalance.get('max_parallel_replacements', 1))

    @property
    def cost_rebalance_stabilization_seconds(self) -> float:
        if not isinstance(self._cost_rebalance, dict):
            return 300.0
        return float(self._cost_rebalance.get('stabilization_seconds', 300))

    @property
    def post_data(self) -> dict[str, Any] | None:
        return self._post_data

    @property
    def tls_credential(self) -> serve_utils.TLSCredential | None:
        return self._tls_credential

    @tls_credential.setter
    def tls_credential(self, value: serve_utils.TLSCredential | None) -> None:
        self._tls_credential = value

    @property
    def readiness_headers(self) -> dict[str, str] | None:
        return self._readiness_headers

    @property
    def base_ondemand_fallback_replicas(self) -> int | None:
        return self._base_ondemand_fallback_replicas

    @property
    def dynamic_ondemand_fallback(self) -> bool | None:
        return self._dynamic_ondemand_fallback

    @property
    def spot_placer(self) -> str | None:
        return self._spot_placer

    @property
    def upscale_delay_seconds(self) -> int | None:
        return self._upscale_delay_seconds

    @property
    def downscale_delay_seconds(self) -> int | None:
        return self._downscale_delay_seconds

    @property
    def use_ondemand_fallback(self) -> bool:
        return self._use_ondemand_fallback

    @property
    def load_balancing_policy(self) -> str:
        return lb_policies.LoadBalancingPolicy.make_policy_name(
            self._load_balancing_policy)

    @property
    def pool(self) -> bool:
        return placement_policy.is_pool_workload(self._pool)

    @property
    def queue_length_threshold(self) -> int | None:
        return self._queue_length_threshold

    @property
    def consecutive_failure_threshold_timeout(self) -> int | None:
        return self._consecutive_failure_threshold_timeout

    def copy(self, **override) -> 'SkyServiceSpec':
        placement_driver_overridden = ('spot_placer' in override or
                                       'pool' in override)
        copied_spot_placer = override.pop('spot_placer', self._spot_placer)
        copied_pool = override.pop('pool', self._pool)
        copied_placement_contract = (None if placement_driver_overridden else
                                     self.placement_contract)
        copied = SkyServiceSpec(
            readiness_path=override.pop('readiness_path', self._readiness_path),
            initial_delay_seconds=override.pop('initial_delay_seconds',
                                               self._initial_delay_seconds),
            readiness_timeout_seconds=override.pop(
                'readiness_timeout_seconds', self._readiness_timeout_seconds),
            endpoint_probe_interval_seconds=override.pop(
                'endpoint_probe_interval_seconds',
                self._endpoint_probe_interval_seconds),
            lb_stream_timeout_seconds=override.pop(
                'lb_stream_timeout_seconds', self._lb_stream_timeout_seconds),
            lb_retriable_status_codes=override.pop(
                'lb_retriable_status_codes', self._lb_retriable_status_codes),
            lb_max_retries=override.pop('lb_max_retries', self._lb_max_retries),
            lb_retry_initial_backoff_seconds=override.pop(
                'lb_retry_initial_backoff_seconds',
                self._lb_retry_initial_backoff_seconds),
            lb_request_queue=override.pop('lb_request_queue',
                                          self._lb_request_queue),
            lb_high_availability=override.pop('lb_high_availability',
                                              self._lb_high_availability),
            graceful_drain_seconds=override.pop('graceful_drain_seconds',
                                                self._graceful_drain_seconds),
            graceful_drain_async_occupancy=override.pop(
                'graceful_drain_async_occupancy',
                self._graceful_drain_async_occupancy),
            min_replicas=override.pop('min_replicas', self._min_replicas),
            min_replicas_by_accelerator=override.pop(
                'min_replicas_by_accelerator',
                self._min_replicas_by_accelerator),
            max_replicas=override.pop('max_replicas', self._max_replicas),
            num_overprovision=override.pop('num_overprovision',
                                           self._num_overprovision),
            ports=override.pop('ports', self._ports),
            target_qps_per_replica=override.pop('target_qps_per_replica',
                                                self._target_qps_per_replica),
            target_concurrency_per_replica=override.pop(
                'target_concurrency_per_replica',
                self._target_concurrency_per_replica),
            target_utilization_percentage=override.pop(
                'target_utilization_percentage',
                self._target_utilization_percentage),
            expected_request_duration_seconds=override.pop(
                'expected_request_duration_seconds',
                self._expected_request_duration_seconds),
            initial_provision_lead_time_seconds=override.pop(
                'initial_provision_lead_time_seconds',
                self._initial_provision_lead_time_seconds),
            adaptive_demand_estimation=override.pop(
                'adaptive_demand_estimation', self._adaptive_demand_estimation),
            max_scale_up_rate_percentage=override.pop(
                'max_scale_up_rate_percentage',
                self._max_scale_up_rate_percentage),
            scale_up_rate_min_replicas=override.pop(
                'scale_up_rate_min_replicas', self._scale_up_rate_min_replicas),
            scale_up_rate_period_seconds=override.pop(
                'scale_up_rate_period_seconds',
                self._scale_up_rate_period_seconds),
            adaptive_scale_up=override.pop('adaptive_scale_up',
                                           self._adaptive_scale_up),
            max_scale_down_rate_percentage=override.pop(
                'max_scale_down_rate_percentage',
                self._max_scale_down_rate_percentage),
            reserved_capacity_fill=override.pop('reserved_capacity_fill',
                                                self._reserved_capacity_fill),
            cost_rebalance=override.pop('cost_rebalance', self._cost_rebalance),
            post_data=override.pop('post_data', self._post_data),
            tls_credential=override.pop('tls_credential', self._tls_credential),
            readiness_headers=override.pop('readiness_headers',
                                           self._readiness_headers),
            dynamic_ondemand_fallback=override.pop(
                'dynamic_ondemand_fallback', self._dynamic_ondemand_fallback),
            base_ondemand_fallback_replicas=override.pop(
                'base_ondemand_fallback_replicas',
                self._base_ondemand_fallback_replicas),
            spot_placer=copied_spot_placer,
            upscale_delay_seconds=override.pop('upscale_delay_seconds',
                                               self._upscale_delay_seconds),
            downscale_delay_seconds=override.pop('downscale_delay_seconds',
                                                 self._downscale_delay_seconds),
            load_balancing_policy=override.pop('load_balancing_policy',
                                               self._load_balancing_policy),
            pool=copied_pool,
            queue_length_threshold=override.pop('queue_length_threshold',
                                                self._queue_length_threshold),
            consecutive_failure_threshold_timeout=override.pop(
                'consecutive_failure_threshold_timeout',
                self._consecutive_failure_threshold_timeout),
            _preserved_placement_contract=copied_placement_contract,
            _placement_contract_copy_token=(None if copied_placement_contract
                                            is None else
                                            _PLACEMENT_CONTRACT_COPY_TOKEN),
        )
        copied._lb_high_availability_specified = (  # pylint: disable=protected-access
            self._lb_high_availability_specified)
        return copied
