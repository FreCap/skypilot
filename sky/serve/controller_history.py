"""Best-effort history persistence for the SkyServe controller."""

# This module's functions bind directly as methods on SkyServeController.
# pylint: disable=protected-access

import asyncio
import logging
import time
import types
from typing import Any

from sky.serve import serve_history
from sky.utils import common_utils

# Preserve the historical logger identity used by these controller methods.
logger = logging.getLogger('sky.serve.controller')


class _UnsupportedRequestClassificationVersion(ValueError):
    """A newer LB protocol that this controller must not acknowledge."""


async def _persist_request_history(self: Any, request_data: dict[str,
                                                                 Any]) -> bool:
    """Persist history without allowing observability to fail sync."""
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, self._record_request_history,
                                          request_data)
    except ValueError as e:
        # A malformed snapshot cannot become valid by retrying. Drop it
        # with an acknowledgement so a mixed-version or corrupted LB
        # cannot hammer the controller every sync forever.
        logger.warning('Dropping invalid load balancer request history for '
                       f'{self._service_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return True
    except Exception as e:  # pylint: disable=broad-except
        # Request history is observability, not control-plane state.
        # Keep routing and autoscaling available while asking the LB to
        # retry only its bounded cumulative counters.
        logger.warning('Failed to persist load balancer request history for '
                       f'{self._service_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return False


def _record_request_history(self: Any, request_data: dict[str, Any]) -> bool:
    """Persist one live LB process's cumulative minute counters."""
    request_history = request_data.get('request_history')
    if request_history is None:
        return True
    service_hash = self._service_hash
    if service_hash is None:
        # Compatibility for direct/legacy controller construction without
        # an incarnation fence. Do not create history that could leak into
        # a later same-name service.
        return True
    lb_session_id = request_data.get('lb_session_id')
    process_session_id = request_data.get('request_history_session_id')
    if (not isinstance(lb_session_id, str) or not lb_session_id or
            not isinstance(process_session_id, str) or
            len(process_session_id) != 32 or
            any(character not in '0123456789abcdef'
                for character in process_session_id)):
        raise ValueError('Invalid request history reporter session.')
    reporter_session_id = f'{lb_session_id}:{process_session_id}'
    serve_history.record_request_activity(
        self._service_name,
        service_hash,
        reporter_session_id,
        request_history,
    )
    return True


async def _persist_request_classification_history(
        self: Any, request_data: dict[str, Any]) -> bool:
    """Persist terminal classifications independently from arrival history."""
    if request_data.get('request_classification_history') is None:
        return True
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(
            None, self._record_request_classification_history, request_data)
    except _UnsupportedRequestClassificationVersion as e:
        # A future LB must retain and retry its snapshot until a capable
        # controller is rolled out. A boolean acknowledgement would otherwise
        # make the newer data irrecoverable.
        logger.warning('Cannot accept load balancer request classification '
                       f'history for {self._service_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return False
    except ValueError as e:
        # A malformed cumulative snapshot cannot become valid by retrying.
        logger.warning('Dropping invalid load balancer request '
                       f'classification history for {self._service_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return True
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Failed to persist load balancer request '
                       f'classification history for {self._service_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return False


def _record_request_classification_history(
        self: Any, request_data: dict[str, Any]) -> bool:
    """Persist one LB process's cumulative terminal-classification pairs."""
    classification_history = request_data.get('request_classification_history')
    if classification_history is None:
        return True
    declared_version = (classification_history.get('classification_version')
                        if isinstance(classification_history, dict) else None)
    if (isinstance(declared_version, int) and
            not isinstance(declared_version, bool) and declared_version
            > serve_history.REQUEST_CLASSIFICATION_PROTOCOL_VERSION):
        raise _UnsupportedRequestClassificationVersion(
            'Request classification protocol version '
            f'{declared_version} is newer than supported version '
            f'{serve_history.REQUEST_CLASSIFICATION_PROTOCOL_VERSION}.')
    service_hash = self._service_hash
    if service_hash is None:
        return True
    lb_session_id = request_data.get('lb_session_id')
    process_session_id = request_data.get('request_history_session_id')
    if (not isinstance(lb_session_id, str) or not lb_session_id or
            not isinstance(process_session_id, str) or
            len(process_session_id) != 32 or
            any(character not in '0123456789abcdef'
                for character in process_session_id)):
        raise ValueError('Invalid request classification reporter session.')
    reporter_session_id = f'{lb_session_id}:{process_session_id}'
    serve_history.record_request_classification(
        self._service_name,
        service_hash,
        reporter_session_id,
        classification_history,
        request_history=request_data.get('request_history'),
    )
    return True


async def _persist_response_time_history(self: Any,
                                         request_data: dict[str, Any]) -> bool:
    """Accept legacy HTTP histograms during a mixed-version rollout."""
    if request_data.get('response_time_history') is None:
        return True
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None,
                                          self._record_response_time_history,
                                          request_data)
    except ValueError as e:
        logger.warning('Dropping invalid legacy load balancer response-time '
                       f'history for {self._service_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return True
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Failed to persist legacy load balancer '
                       f'response-time history for '
                       f'{self._service_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return False


def _record_response_time_history(self: Any, request_data: dict[str,
                                                                Any]) -> bool:
    """Persist one legacy LB process's cumulative HTTP histograms."""
    response_time_history = request_data.get('response_time_history')
    if response_time_history is None:
        return True
    service_hash = self._service_hash
    if service_hash is None:
        return True
    lb_session_id = request_data.get('lb_session_id')
    process_session_id = request_data.get('request_history_session_id')
    if (not isinstance(lb_session_id, str) or not lb_session_id or
            not isinstance(process_session_id, str) or
            len(process_session_id) != 32 or
            any(character not in '0123456789abcdef'
                for character in process_session_id)):
        raise ValueError('Invalid response history reporter session.')
    reporter_session_id = f'{lb_session_id}:{process_session_id}'
    serve_history.record_response_times(
        self._service_name,
        service_hash,
        reporter_session_id,
        response_time_history,
    )
    return True


async def _persist_prediction_time_history(
        self: Any, request_data: dict[str, Any]) -> bool:
    """Persist prediction histograms without allowing them to fail sync."""
    if request_data.get('prediction_time_history') is None:
        return True
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None,
                                          self._record_prediction_time_history,
                                          request_data)
    except ValueError as e:
        # A malformed bounded snapshot cannot become valid by retrying.
        logger.warning('Dropping invalid load balancer prediction-time '
                       f'history for {self._service_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return True
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Failed to persist load balancer prediction-time '
                       f'history for {self._service_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return False


def _record_prediction_time_history(self: Any, request_data: dict[str,
                                                                  Any]) -> bool:
    """Persist one live LB process's cumulative prediction histograms."""
    prediction_time_history = request_data.get('prediction_time_history')
    if prediction_time_history is None:
        return True
    service_hash = self._service_hash
    if service_hash is None:
        return True
    lb_session_id = request_data.get('lb_session_id')
    process_session_id = request_data.get('request_history_session_id')
    if (not isinstance(lb_session_id, str) or not lb_session_id or
            not isinstance(process_session_id, str) or
            len(process_session_id) != 32 or
            any(character not in '0123456789abcdef'
                for character in process_session_id)):
        raise ValueError('Invalid prediction history reporter session.')
    reporter_session_id = f'{lb_session_id}:{process_session_id}'
    serve_history.record_prediction_times(
        self._service_name,
        service_hash,
        reporter_session_id,
        prediction_time_history,
    )
    return True


async def _persist_autoscaler_history(
    self: Any,
    replica_counts: dict[str, int | str],
    capacity_hint: dict[str, Any],
) -> None:
    """Persist controller gauges without allowing history to fail sync."""
    loop = asyncio.get_running_loop()
    observed_at = time.time()
    try:
        await loop.run_in_executor(None, self._record_autoscaler_history,
                                   replica_counts, capacity_hint, observed_at)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Failed to persist autoscaler history for '
                       f'{self._service_name!r}: '
                       f'{common_utils.format_exception(e)}')


def _record_autoscaler_history(
    self: Any,
    replica_counts: dict[str, Any],
    capacity_hint: dict[str, Any],
    timestamp: float | None = None,
) -> int:
    """Persist one minute observation from already-computed sync state."""
    service_hash = self._service_hash
    if service_hash is None:
        return 0
    replica_unit = replica_counts.get('replica_unit')
    ready_capacity = replica_counts.get('ready_replicas')
    total_capacity = replica_counts.get('total_replicas')
    provisioning_capacity = capacity_hint.get('provisioning_replicas')
    if (not isinstance(replica_unit, str) or
            replica_unit not in {'physical_backend', 'logical_slot'} or
            not isinstance(ready_capacity, int) or
            not isinstance(total_capacity, int) or
            not isinstance(provisioning_capacity, int)):
        return 0

    autoscaler_info = self._autoscaler.info()
    demand_target = self._autoscaler.get_final_target_num_replicas()
    fill_target = autoscaler_info.get('fill_target')
    if not isinstance(fill_target, int) or isinstance(fill_target, bool):
        fill_target = 0
    capacity_target = max(demand_target, fill_target)
    peak_in_flight = autoscaler_info.get('in_flight_total')
    peak_queue_depth = autoscaler_info.get('queue_depth')
    if not isinstance(peak_in_flight, int) or isinstance(peak_in_flight, bool):
        peak_in_flight = None
    if not isinstance(peak_queue_depth, int) or isinstance(
            peak_queue_depth, bool):
        peak_queue_depth = None
    accelerator_breakdown = self._get_accelerator_history_breakdown(
        replica_counts, fill_target)
    return serve_history.record_autoscaler_snapshot(
        self._service_name,
        service_hash,
        self._history_session_id,
        version=self._applied_version,
        replica_unit=replica_unit,
        demand_target=demand_target,
        capacity_target=capacity_target,
        ready_capacity=ready_capacity,
        provisioning_capacity=provisioning_capacity,
        total_capacity=total_capacity,
        peak_in_flight=peak_in_flight,
        peak_queue_depth=peak_queue_depth,
        accelerator_breakdown=accelerator_breakdown,
        timestamp=timestamp,
        required_source_mode='LEGACY_CONTROLLER',
    )


def _get_accelerator_history_breakdown(
        self: Any, replica_counts: dict[str, Any],
        aggregate_fill_target: int) -> dict[str, Any] | None:
    """Build one complete exact-card observation, or mark unavailable."""
    shapes = self._autoscaler.configured_accelerator_shapes
    if not isinstance(shapes, dict) or not shapes:
        return None
    if not self._autoscaler.has_recomputed_with_fresh_data():
        return None
    configured = list(shapes)
    demand_target = self._autoscaler.target_num_replicas_by_accelerator
    if (not isinstance(demand_target, dict) or sum(demand_target.values())
            != self._autoscaler.target_num_replicas):
        # An aggregate fallback or mixed-version report cannot be
        # reconstructed as exact-card zeroes.
        return None

    def mapping(field: str) -> dict[str, int]:
        raw = replica_counts.get(field, {})
        return raw if isinstance(raw, dict) else {}

    fill_target = mapping('fill_target_by_accelerator')
    if sum(fill_target.values()) != aggregate_fill_target:
        # A broker grant can briefly outlive the exact physical supply
        # observation that attributed it. Preserve the aggregate target,
        # but do not publish an invented exact-card history overlay.
        return None

    return {
        'capacity_semantics_version':
            serve_history.ACCELERATOR_BREAKDOWN_CAPACITY_SEMANTICS_VERSION,
        'configured_accelerators': configured,
        'min_replicas': dict(self._autoscaler.min_replicas_by_accelerator),
        'demand_target': dict(demand_target),
        'warm_retention_target': dict(
            self._autoscaler.warm_retention_target_by_accelerator),
        'cold_launch_authority': dict(
            self._autoscaler.cold_launch_authority_by_accelerator),
        'ready_capacity': mapping('ready_replicas_by_accelerator'),
        'provisioning_capacity':
            mapping('provisioning_replicas_by_accelerator'),
        'total_capacity': mapping('total_replicas_by_accelerator'),
        'zero_cost_ready_capacity':
            mapping('zero_cost_ready_replicas_by_accelerator'),
        'fill_target': fill_target,
        'free_reserved_slots': mapping('free_reserved_slots_by_accelerator'),
    }


_CONTROLLER_GLOBAL_METHOD_NAMES = (
    '_persist_request_history',
    '_record_request_history',
    '_persist_response_time_history',
    '_record_response_time_history',
    '_persist_prediction_time_history',
    '_record_prediction_time_history',
    '_persist_autoscaler_history',
    '_record_autoscaler_history',
    '_get_accelerator_history_breakdown',
)

for _method_name in (
        *_CONTROLLER_GLOBAL_METHOD_NAMES,
        '_persist_request_classification_history',
        '_record_request_classification_history',
):
    _history_method = globals()[_method_name]
    _history_method.__module__ = 'sky.serve.controller'
    _history_method.__qualname__ = f'SkyServeController.{_method_name}'


def _bind_controller_globals(controller_globals: dict[str, Any]) -> None:
    """Bind extracted legacy methods to their historical module globals."""
    for method_name in _CONTROLLER_GLOBAL_METHOD_NAMES:
        method = globals()[method_name]
        rebound_method = types.FunctionType(
            method.__code__,
            controller_globals,
            method.__name__,
            method.__defaults__,
            method.__closure__,
        )
        rebound_method.__kwdefaults__ = (None if method.__kwdefaults__ is None
                                         else method.__kwdefaults__.copy())
        rebound_method.__annotations__ = method.__annotations__.copy()
        rebound_method.__module__ = method.__module__
        rebound_method.__qualname__ = method.__qualname__
        rebound_method.__dict__.update(method.__dict__)
        type_params = getattr(method, '__type_params__', None)
        if type_params is not None:
            setattr(rebound_method, '__type_params__', type_params)
        globals()[method_name] = rebound_method
