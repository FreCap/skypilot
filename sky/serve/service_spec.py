"""Service specification for SkyServe."""
import json
import math
import os
import textwrap
from typing import Any, Dict, List, Optional, Union

from sky import serve
from sky import sky_logging
from sky.serve import constants
from sky.serve import load_balancing_policies as lb_policies
from sky.serve import serve_utils
from sky.serve import spot_placer as spot_placer_lib
from sky.utils import common_utils
from sky.utils import schemas
from sky.utils import ux_utils
from sky.utils import yaml_utils

logger = sky_logging.init_logger(__name__)


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
        lb_retriable_status_codes: Optional[List[int]] = None,
        lb_max_retries: Optional[int] = None,
        lb_retry_initial_backoff_seconds: Optional[float] = None,
        max_replicas: Optional[int] = None,
        num_overprovision: Optional[int] = None,
        ports: Optional[str] = None,
        target_qps_per_replica: Optional[Union[float, Dict[str, float]]] = None,
        target_concurrency_per_replica: Optional[float] = None,
        reserved_capacity_fill: Optional[Union[bool, Dict[str, Any]]] = None,
        post_data: Optional[Dict[str, Any]] = None,
        tls_credential: Optional[serve_utils.TLSCredential] = None,
        readiness_headers: Optional[Dict[str, str]] = None,
        dynamic_ondemand_fallback: Optional[bool] = None,
        base_ondemand_fallback_replicas: Optional[int] = None,
        spot_placer: Optional[str] = None,
        upscale_delay_seconds: Optional[int] = None,
        downscale_delay_seconds: Optional[int] = None,
        load_balancing_policy: Optional[str] = None,
        pool: Optional[bool] = None,
        queue_length_threshold: Optional[int] = None,
        consecutive_failure_threshold_timeout: Optional[int] = None,
        graceful_drain_seconds: Optional[int] = None,
        graceful_drain_async_occupancy: Optional[bool] = None,
    ) -> None:
        if pool:
            # For pools, max_replicas should never be specified directly by the
            # user. It should only be set via max_workers in the pool config.
            # However, if queue_length_threshold is set, that means max_replicas
            # was set internally from max_workers, so we allow it
            unsupported_fields = [
                'num_overprovision',
                'target_qps_per_replica',
                'target_concurrency_per_replica',
                'base_ondemand_fallback_replicas',
                'dynamic_ondemand_fallback',
                'spot_placer',
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

        if target_concurrency_per_replica is not None:
            # Zero (or negative) per-GPU capacity would make every replica
            # capacity 0 and feed divisions in the concurrency autoscaler.
            if target_concurrency_per_replica <= 0:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'target_concurrency_per_replica must be > 0. '
                        f'Got: {target_concurrency_per_replica}')

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
            if (not pool and max_replicas is not None and
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
                graceful_drain_seconds < 0 or graceful_drain_seconds >
                constants.LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS):
            # The upper bound keeps the drain within the window the LB
            # retains (and reports) a retiring replica's unknown async
            # occupancy; a longer cap could end early once that retention
            # expires.
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'graceful_drain_seconds must be an integer between 0 and '
                    f'{constants.LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS}. '
                    f'Got: {graceful_drain_seconds!r}')
        self._readiness_path: str = readiness_path
        self._initial_delay_seconds: int = initial_delay_seconds
        self._readiness_timeout_seconds: int = readiness_timeout_seconds
        self._endpoint_probe_interval_seconds: int = (
            endpoint_probe_interval_seconds)
        self._lb_stream_timeout_seconds: int = lb_stream_timeout_seconds
        self._lb_retriable_status_codes: Optional[List[int]] = (
            lb_retriable_status_codes)
        self._lb_max_retries: Optional[int] = lb_max_retries
        self._lb_retry_initial_backoff_seconds: Optional[float] = (
            lb_retry_initial_backoff_seconds)
        self._graceful_drain_seconds: Optional[int] = graceful_drain_seconds
        # Declares fast-ack work whose lifetime outlives its HTTP envelope.
        # The LB must treat a missing occupancy sample as unknown from the
        # first request, rather than inferring capability from a successful
        # probe (which cannot protect a never-probed replica).
        self._graceful_drain_async_occupancy: Optional[bool] = (
            graceful_drain_async_occupancy)
        self._min_replicas: int = min_replicas
        self._max_replicas: Optional[int] = max_replicas
        self._num_overprovision: Optional[int] = num_overprovision
        self._ports: Optional[str] = ports
        self._target_qps_per_replica: Optional[Union[float, Dict[
            str, float]]] = target_qps_per_replica
        # Per-GPU target concurrency: replica capacity = knob * gpu_count.
        self._target_concurrency_per_replica: Optional[float] = (
            target_concurrency_per_replica)
        # Opt-in: allow scaling up onto free reserved (zero-cost) capacity.
        # Absent/False means no behavior change. Bool form or object form
        # ({floor_replicas, weight}); object form implies enabled.
        self._reserved_capacity_fill: Optional[Union[bool, Dict[
            str, Any]]] = reserved_capacity_fill
        self._post_data: Optional[Dict[str, Any]] = post_data
        self._tls_credential: Optional[serve_utils.TLSCredential] = (
            tls_credential)
        self._readiness_headers: Optional[Dict[str, str]] = readiness_headers
        self._dynamic_ondemand_fallback: Optional[
            bool] = dynamic_ondemand_fallback
        self._base_ondemand_fallback_replicas: Optional[
            int] = base_ondemand_fallback_replicas
        self._spot_placer: Optional[str] = spot_placer
        self._upscale_delay_seconds: Optional[int] = upscale_delay_seconds
        self._downscale_delay_seconds: Optional[int] = downscale_delay_seconds
        self._load_balancing_policy: Optional[str] = load_balancing_policy
        self._pool: Optional[bool] = pool
        self._queue_length_threshold: Optional[int] = queue_length_threshold
        self._consecutive_failure_threshold_timeout: Optional[int] = (
            consecutive_failure_threshold_timeout)

        self._use_ondemand_fallback: bool = (
            self.dynamic_ondemand_fallback is not None and
            self.dynamic_ondemand_fallback) or (
                self.base_ondemand_fallback_replicas is not None and
                self.base_ondemand_fallback_replicas > 0)

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """Set state from pickled state, for backward compatibility."""
        # These fields were added after earlier releases had already persisted
        # SkyServiceSpec objects in the serve DB.
        state.setdefault('_endpoint_probe_interval_seconds',
                         constants.DEFAULT_ENDPOINT_PROBE_INTERVAL_SECONDS)
        state.setdefault('_lb_stream_timeout_seconds',
                         constants.DEFAULT_LB_STREAM_TIMEOUT)
        state.setdefault('_lb_retriable_status_codes', None)
        state.setdefault('_lb_max_retries', None)
        state.setdefault('_lb_retry_initial_backoff_seconds', None)
        state.setdefault('_consecutive_failure_threshold_timeout', None)
        # Added with the concurrency autoscaler; old DB rows predate it.
        state.setdefault('_target_concurrency_per_replica', None)
        # Added with reserved-capacity fill; old DB rows predate it.
        state.setdefault('_reserved_capacity_fill', None)
        # Added with the in-flight-aware graceful drain; old DB rows
        # predate it (None -> default drain semantics).
        state.setdefault('_graceful_drain_seconds', None)
        state.setdefault('_graceful_drain_async_occupancy', None)
        self.__dict__.update(state)

    @staticmethod
    def from_yaml_config(config: Dict[str, Any]) -> 'SkyServiceSpec':
        common_utils.validate_schema(config, schemas.get_service_schema(),
                                     'Invalid service YAML: ')
        if 'replicas' in config and 'replica_policy' in config:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'Cannot specify both `replicas` and `replica_policy` in '
                    'the service YAML. Please use one of them.')

        service_config: Dict[str, Any] = {}

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

        pool_config = config.get('pool', None)
        if pool_config is not None:
            service_config['pool'] = pool_config

        policy_section = config.get('replica_policy', None)
        if policy_section is not None and pool_config:
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
        if simplified_policy_section is not None and pool_config:
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
        if pool_config is not None and isinstance(pool_config, dict):
            queue_length_threshold = pool_config.get('queue_length_threshold',
                                                     None)
            pool_min_workers = pool_config.get('min_workers', None)
            pool_max_workers = pool_config.get('max_workers', None)
            pool_upscale_delay = pool_config.get('upscale_delay_seconds', None)
            pool_downscale_delay = pool_config.get('downscale_delay_seconds',
                                                   None)
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
            service_config['max_replicas'] = pool_max_workers
            service_config['upscale_delay_seconds'] = pool_upscale_delay
            service_config['downscale_delay_seconds'] = pool_downscale_delay
            service_config['num_overprovision'] = None
            service_config['target_qps_per_replica'] = None
            service_config['target_concurrency_per_replica'] = None
            service_config['reserved_capacity_fill'] = None
        else:
            service_config['min_replicas'] = policy_section['min_replicas']
            service_config['max_replicas'] = policy_section.get(
                'max_replicas', None)
            service_config['num_overprovision'] = policy_section.get(
                'num_overprovision', None)
            service_config['target_qps_per_replica'] = policy_section.get(
                'target_qps_per_replica', None)
            service_config['target_concurrency_per_replica'] = (
                policy_section.get('target_concurrency_per_replica', None))
            service_config['reserved_capacity_fill'] = policy_section.get(
                'reserved_capacity_fill', None)
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

        return SkyServiceSpec(**service_config)

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
        with open(os.path.expanduser(yaml_path), 'r', encoding='utf-8') as f:
            yaml_content = f.read()
        return SkyServiceSpec.from_yaml_str(yaml_content)

    def to_yaml_config(self) -> Dict[str, Any]:
        config: Dict[str, Any] = {}

        def add_if_not_none(section: str,
                            key: Optional[str],
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

        add_if_not_none('pool', None, self._pool)
        # Emitted before the pool early-return: the field is service-level
        # and must survive serialization for pools too (their retirement
        # uses the same bounded drain, just without the in-flight gauge).
        add_if_not_none('graceful_drain_seconds', None,
                        self.graceful_drain_seconds)
        add_if_not_none('graceful_drain_async_occupancy', None,
                        self.graceful_drain_async_occupancy)

        if self.pool:
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
        if (self.endpoint_probe_interval_seconds !=
                constants.DEFAULT_ENDPOINT_PROBE_INTERVAL_SECONDS):
            add_if_not_none('readiness_probe',
                            'endpoint_probe_interval_seconds',
                            self.endpoint_probe_interval_seconds)
        add_if_not_none('readiness_probe',
                        'consecutive_failure_threshold_timeout',
                        self.consecutive_failure_threshold_timeout)
        if (self.lb_stream_timeout_seconds !=
                constants.DEFAULT_LB_STREAM_TIMEOUT):
            add_if_not_none('load_balancer', 'stream_timeout_seconds',
                            self.lb_stream_timeout_seconds)
        add_if_not_none('load_balancer', 'retriable_status_codes',
                        self.lb_retriable_status_codes)
        add_if_not_none('load_balancer', 'max_retries', self.lb_max_retries)
        add_if_not_none('load_balancer', 'retry_initial_backoff_seconds',
                        self.lb_retry_initial_backoff_seconds)
        add_if_not_none('readiness_probe', 'headers', self._readiness_headers)
        add_if_not_none('replica_policy', 'min_replicas', self.min_replicas)
        add_if_not_none('replica_policy', 'max_replicas', self.max_replicas)
        add_if_not_none('replica_policy', 'num_overprovision',
                        self.num_overprovision)
        add_if_not_none('replica_policy', 'target_qps_per_replica',
                        self.target_qps_per_replica)
        add_if_not_none('replica_policy', 'target_concurrency_per_replica',
                        self.target_concurrency_per_replica)
        # no_empty: omit both None and False so older controllers never see
        # the field unless the user opted in. Canonicalize: an object form
        # carrying only default knobs collapses to the plain bool form.
        reserved_fill_config: Optional[Union[bool, Dict[str, Any]]] = (
            self._reserved_capacity_fill)
        if isinstance(self._reserved_capacity_fill, dict):
            fill_obj: Dict[str, Any] = {}
            if self.reserved_fill_floor_replicas != 0:
                fill_obj['floor_replicas'] = self.reserved_fill_floor_replicas
            if self.reserved_fill_weight != 1.0:
                fill_obj['weight'] = self.reserved_fill_weight
            reserved_fill_config = fill_obj if fill_obj else True
        add_if_not_none('replica_policy',
                        'reserved_capacity_fill',
                        reserved_fill_config,
                        no_empty=True)
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
        policy_strs: List[str] = []
        if (self.dynamic_ondemand_fallback is not None and
                self.dynamic_ondemand_fallback):
            if self.spot_placer is not None:
                if self.spot_placer == spot_placer_lib.SPOT_HEDGE_PLACER:
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
            return (
                f'Autoscaling from {self.min_replicas} to {self.max_replicas} '
                f'{noun}{max_plural}{overprovision_str} '
                '(target_concurrency_per_replica: '
                f'{self.target_concurrency_per_replica} per GPU)')
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
    def lb_retriable_status_codes(self) -> Optional[List[int]]:
        return self._lb_retriable_status_codes

    @property
    def graceful_drain_seconds(self) -> Optional[int]:
        return self._graceful_drain_seconds

    @property
    def graceful_drain_async_occupancy(self) -> Optional[bool]:
        return self._graceful_drain_async_occupancy

    @property
    def lb_max_retries(self) -> Optional[int]:
        return self._lb_max_retries

    @property
    def lb_retry_initial_backoff_seconds(self) -> Optional[float]:
        return self._lb_retry_initial_backoff_seconds

    @property
    def min_replicas(self) -> int:
        return self._min_replicas

    @property
    def max_replicas(self) -> Optional[int]:
        # If None, treated as having the same value of min_replicas.
        return self._max_replicas

    @property
    def num_overprovision(self) -> Optional[int]:
        return self._num_overprovision

    @property
    def ports(self) -> Optional[str]:
        return self._ports

    @property
    def target_qps_per_replica(
            self) -> Optional[Union[float, Dict[str, float]]]:
        return self._target_qps_per_replica

    @property
    def target_concurrency_per_replica(self) -> Optional[float]:
        # Per GPU: replica capacity = knob * gpu_count. Guarded with getattr
        # semantics via __setstate__ for specs unpickled from old DB rows.
        return self._target_concurrency_per_replica

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
    def post_data(self) -> Optional[Dict[str, Any]]:
        return self._post_data

    @property
    def tls_credential(self) -> Optional[serve_utils.TLSCredential]:
        return self._tls_credential

    @tls_credential.setter
    def tls_credential(self,
                       value: Optional[serve_utils.TLSCredential]) -> None:
        self._tls_credential = value

    @property
    def readiness_headers(self) -> Optional[Dict[str, str]]:
        return self._readiness_headers

    @property
    def base_ondemand_fallback_replicas(self) -> Optional[int]:
        return self._base_ondemand_fallback_replicas

    @property
    def dynamic_ondemand_fallback(self) -> Optional[bool]:
        return self._dynamic_ondemand_fallback

    @property
    def spot_placer(self) -> Optional[str]:
        return self._spot_placer

    @property
    def upscale_delay_seconds(self) -> Optional[int]:
        return self._upscale_delay_seconds

    @property
    def downscale_delay_seconds(self) -> Optional[int]:
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
        # This can happen for backward compatibility.
        if not hasattr(self, '_pool'):
            return False
        return bool(self._pool)

    @property
    def queue_length_threshold(self) -> Optional[int]:
        return self._queue_length_threshold

    @property
    def consecutive_failure_threshold_timeout(self) -> Optional[int]:
        return self._consecutive_failure_threshold_timeout

    def copy(self, **override) -> 'SkyServiceSpec':
        return SkyServiceSpec(
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
            graceful_drain_seconds=override.pop('graceful_drain_seconds',
                                                self._graceful_drain_seconds),
            graceful_drain_async_occupancy=override.pop(
                'graceful_drain_async_occupancy',
                self._graceful_drain_async_occupancy),
            min_replicas=override.pop('min_replicas', self._min_replicas),
            max_replicas=override.pop('max_replicas', self._max_replicas),
            num_overprovision=override.pop('num_overprovision',
                                           self._num_overprovision),
            ports=override.pop('ports', self._ports),
            target_qps_per_replica=override.pop('target_qps_per_replica',
                                                self._target_qps_per_replica),
            target_concurrency_per_replica=override.pop(
                'target_concurrency_per_replica',
                self._target_concurrency_per_replica),
            reserved_capacity_fill=override.pop('reserved_capacity_fill',
                                                self._reserved_capacity_fill),
            post_data=override.pop('post_data', self._post_data),
            tls_credential=override.pop('tls_credential', self._tls_credential),
            readiness_headers=override.pop('readiness_headers',
                                           self._readiness_headers),
            dynamic_ondemand_fallback=override.pop(
                'dynamic_ondemand_fallback', self._dynamic_ondemand_fallback),
            base_ondemand_fallback_replicas=override.pop(
                'base_ondemand_fallback_replicas',
                self._base_ondemand_fallback_replicas),
            spot_placer=override.pop('spot_placer', self._spot_placer),
            upscale_delay_seconds=override.pop('upscale_delay_seconds',
                                               self._upscale_delay_seconds),
            downscale_delay_seconds=override.pop('downscale_delay_seconds',
                                                 self._downscale_delay_seconds),
            load_balancing_policy=override.pop('load_balancing_policy',
                                               self._load_balancing_policy),
            pool=override.pop('pool', self._pool),
            queue_length_threshold=override.pop('queue_length_threshold',
                                                self._queue_length_threshold),
            consecutive_failure_threshold_timeout=override.pop(
                'consecutive_failure_threshold_timeout',
                self._consecutive_failure_threshold_timeout),
        )
