"""User interface with the SkyServe."""
import base64
import bisect
import collections
from collections.abc import Callable
from collections.abc import Iterator
import concurrent.futures
import contextvars
import dataclasses
import datetime
import enum
import hashlib
import ipaddress
import json
import math
import os
import pathlib
import pickle
import re
import shlex
import shutil
import threading
import time
import traceback
import typing
from typing import Any, TextIO
import uuid

import colorama
import filelock

from sky import backends
from sky import exceptions
from sky import global_user_state
from sky import resources as resources_lib
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.client import sdk
from sky.container_images import task_utils as container_image_task_utils
from sky.jobs import state as managed_job_state
from sky.serve import auth_tokens
from sky.serve import constants
from sky.serve import serve_state
from sky.serve import spot_placer
from sky.server import constants as server_constants
from sky.server.requests import request_names
from sky.skylet import constants as skylet_constants
from sky.skylet import job_lib
from sky.utils import annotations
from sky.utils import command_runner
from sky.utils import common_utils
from sky.utils import controller_utils
from sky.utils import debug_dump_helpers
from sky.utils import locks
from sky.utils import log_utils
from sky.utils import message_utils
from sky.utils import resources_utils
from sky.utils import status_lib
from sky.utils import subprocess_utils
from sky.utils import ux_utils
from sky.utils import yaml_utils
from sky.utils.db import db_utils

if typing.TYPE_CHECKING:
    import fastapi
    import psutil
    import requests

    import sky
    from sky.serve import replica_managers
    from sky.serve import service_spec as service_spec_lib
    WorkerHandle = backends.CloudVmRayResourceHandle | None
else:
    psutil = adaptors_common.LazyImport('psutil')
    requests = adaptors_common.LazyImport('requests')
    WorkerHandle = Any

logger = sky_logging.init_logger(__name__)


def get_provider_configs_for_handles(
        handles_by_key: 'typing.Mapping[Any, Any]'
) -> dict[Any, dict[str, Any]]:
    """Fetch provider configs once per unique cluster YAML path.

    Multiple logical replicas can share the same physical cluster and thus the
    same ``cluster_yaml``.  Serve hot paths only need the parsed ``provider``
    block, so reuse one batched YAML read per unique path and fan the result
    back out to every caller key.
    """
    yaml_paths: list[str] = []
    keys_by_yaml: dict[str, list[Any]] = collections.defaultdict(list)
    for key, handle in handles_by_key.items():
        cluster_yaml = getattr(handle, 'cluster_yaml', None)
        if not isinstance(cluster_yaml, str):
            continue
        if cluster_yaml not in keys_by_yaml:
            yaml_paths.append(cluster_yaml)
        keys_by_yaml[cluster_yaml].append(key)

    if not yaml_paths:
        return {}

    yaml_configs = global_user_state.get_cluster_yaml_dict_multiple(yaml_paths)
    provider_configs_by_yaml = {
        yaml_path: config['provider']
        for yaml_path, config in zip(yaml_paths, yaml_configs, strict=True)
    }
    provider_configs: dict[Any, dict[str, Any]] = {}
    for yaml_path, keys in keys_by_yaml.items():
        provider_config = provider_configs_by_yaml[yaml_path]
        for key in keys:
            provider_configs[key] = provider_config
    return provider_configs


# Keep the established serve_utils import and pickle identities while the
# security-sensitive implementation lives in its own low-state module.
AuthTokenConfigurationError = auth_tokens.AuthTokenConfigurationError
is_lb_data_plane_auth_enabled = auth_tokens.is_lb_data_plane_auth_enabled
validate_controller_auth_token_isolation = (
    auth_tokens.validate_controller_auth_token_isolation)
get_lb_sync_auth_tokens = auth_tokens.get_lb_sync_auth_tokens
get_controller_admin_auth_tokens = auth_tokens.get_controller_admin_auth_tokens
get_lb_auth_tokens = auth_tokens.get_lb_auth_tokens
for _auth_token_symbol in (
        AuthTokenConfigurationError,
        is_lb_data_plane_auth_enabled,
        validate_controller_auth_token_isolation,
        get_lb_sync_auth_tokens,
        get_controller_admin_auth_tokens,
        get_lb_auth_tokens,
):
    _auth_token_symbol.__module__ = __name__
del _auth_token_symbol

# Retry settings for cross-pod controller HTTP calls. The DB row update of
# `controller_ip` is atomic w.r.t. controller readiness (sky.serve.service
# only flips DB after _wait_for_controller_ready), so the only failure modes
# left are: (1) DB read replica lag right after a recovery; (2) brief network
# blips between pods.
# Intermediate retries log at DEBUG to avoid spamming WARN every refresh tick
# while the controller is intentionally absent (CONTROLLER_INIT /
# SHUTTING_DOWN / FAILED_CLEANUP); the final-attempt failure logs once at
# WARN.
# A single attempt with a tight connect timeout keeps `sky jobs pool status`
# responsive even when one of N pools' controllers is unreachable.
_CONTROLLER_HTTP_RETRY_ATTEMPTS = 1
_CONTROLLER_HTTP_RETRY_BACKOFF_SECONDS = 0.5
_LAUNCH_QUIESCE_MAX_CANCEL_ROUNDS = 3
# (connect_timeout, read_timeout). Connect timeout matters most: when the
# controller pod is dead/unreachable, kernel ECONNREFUSED is instant on
# loopback but cross-pod TCP can hang for 30s+ if the remote pod silently
# drops SYN (e.g. NetworkPolicy, pod terminating mid-flight). Without an
# explicit timeout, `requests` waits forever and `sky jobs pool status`
# appears to hang. Read timeout is generous because /autoscaler/info on a
# busy controller can take a moment.
_CONTROLLER_HTTP_TIMEOUT_SECONDS = (1.0, 10.0)

# Bound on the per-call thread pool used by `get_service_status_pickled` to
# fan out across services/pools. The per-service work is dominated by I/O
# (controller HTTP + DB reads), so threads parallelize well. Capped low so a
# 100-pool deployment doesn't open 100 simultaneous DB connections or
# trigger memory pressure on big pools.
_STATUS_FANOUT_MAX_WORKERS = 8


class ControllerOwnerError(RuntimeError):
    """The intended service incarnation has no safe controller target."""


@dataclasses.dataclass(frozen=True)
class _PurgeResult:
    """Outcome of an immediate purge attempt."""

    completed: bool
    message: str | None = None


_ControllerOwner = tuple[str, int, str | None, int]


def make_controller_owner_fingerprint(service_hash: object,
                                      controller_pid: object,
                                      controller_ip: object,
                                      controller_port: object) -> str:
    """Return a stable fingerprint for one exact controller owner tuple."""
    if not isinstance(service_hash, str) or not service_hash:
        raise ControllerOwnerError('Controller service hash is missing.')
    if (not isinstance(controller_pid, int) or
            isinstance(controller_pid, bool) or controller_pid <= 0):
        raise ControllerOwnerError(
            'Controller parent PID is missing or invalid.')
    if (not isinstance(controller_port, int) or
            isinstance(controller_port, bool) or
            not 1 <= controller_port <= 65535):
        raise ControllerOwnerError('Controller port is missing or invalid.')
    normalized_ip: str | None = None
    if controller_ip is not None:
        if not isinstance(controller_ip, str) or not controller_ip:
            raise ControllerOwnerError('Controller IP is invalid.')
        try:
            normalized_ip = str(ipaddress.ip_address(controller_ip))
        except ValueError as e:
            raise ControllerOwnerError('Controller IP is invalid.') from e
    payload = json.dumps(
        [service_hash, controller_pid, normalized_ip, controller_port],
        separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _get_controller_url(service_name: str,
                        expected_service_hash: str) -> tuple[str, str]:
    """Resolve and fence the controller HTTP URL.

    In single-pod (or daemon == controller pod) deployments the IP read from
    DB either matches our own POD_IP or is None — in both cases we fall back
    to localhost. In HA where the request handler runs on a different pod
    than the controller process, we route via the controller's pod IP from DB.
    The address and owner fingerprint come from one narrow, atomic row read.
    """
    record = serve_state.get_service_controller_owner(service_name)
    if record is None:
        raise ControllerOwnerError(
            f'Controller owner for {service_name!r} is missing.')
    service_hash = record.get('hash')
    if service_hash != expected_service_hash:
        raise ControllerOwnerError(
            f'Service {service_name!r} was replaced while the controller '
            'request was in flight.')
    service_status = record.get('status')
    if (not isinstance(service_status, serve_state.ServiceStatus) or
            service_status in (serve_state.ServiceStatus.SHUTTING_DOWN,
                               serve_state.ServiceStatus.FAILED_CLEANUP)):
        raise ControllerOwnerError(
            f'Controller owner for {service_name!r} is not routable.')
    controller_pid = record.get('controller_pid')
    controller_port = record.get('controller_port')
    controller_ip = record.get('controller_ip')
    owner_fingerprint = make_controller_owner_fingerprint(
        service_hash, typing.cast(int, controller_pid), controller_ip,
        typing.cast(int, controller_port))
    self_ip = os.environ.get('POD_IP')
    normalized_self_ip = None
    if self_ip is not None:
        try:
            normalized_self_ip = str(ipaddress.ip_address(self_ip))
        except ValueError:
            pass
    normalized_controller_ip = None
    if controller_ip is not None:
        normalized_controller_ip = str(ipaddress.ip_address(controller_ip))
    if (normalized_controller_ip is None or
            normalized_controller_ip == normalized_self_ip):
        url = f'http://localhost:{controller_port}'
    else:
        host = (f'[{normalized_controller_ip}]' if ':'
                in normalized_controller_ip else normalized_controller_ip)
        url = f'http://{host}:{controller_port}'
    logger.debug(f'_get_controller_url for {service_name}: url={url} '
                 f'self_ip={self_ip} controller_ip={controller_ip}')
    return url, owner_fingerprint


def _get_local_controller_url(owner: _ControllerOwner) -> tuple[str, str]:
    """Resolve a specifically supervised local child without consulting DB."""
    service_hash, controller_pid, controller_ip, controller_port = owner
    owner_fingerprint = make_controller_owner_fingerprint(
        service_hash, controller_pid, controller_ip, controller_port)
    return f'http://localhost:{controller_port}', owner_fingerprint


def _request_to_controller_with_retry(method: str,
                                      service_name: str,
                                      expected_service_hash: str,
                                      path: str,
                                      *,
                                      fixed_controller_owner: _ControllerOwner |
                                      None = None,
                                      **kwargs):
    """HTTP `method` to the controller with bounded retry on ConnectionError.
    """
    request_fn = getattr(requests, method)
    # Force a bounded timeout.
    if 'timeout' not in kwargs:
        kwargs['timeout'] = _CONTROLLER_HTTP_TIMEOUT_SECONDS
    # Controller callers use the admin ring, independent of the credential the
    # LB uses for sync. During rotation, retry with the next overlap credential
    # only after a 401: transport failures retry the SAME credential, while a
    # 403/5xx is an application result and must not replay the request.
    admin_tokens = get_controller_admin_auth_tokens()
    owner_header_lower = constants.CONTROLLER_OWNER_HEADER.lower()
    base_headers = {
        name: value
        for name, value in dict(kwargs.pop('headers', {}) or {}).items()
        if str(name).lower() != owner_header_lower
    }
    caller_supplied_auth = any(
        str(name).lower() == 'authorization' for name in base_headers)
    token_index = 0
    for attempt in range(_CONTROLLER_HTTP_RETRY_ATTEMPTS):
        if fixed_controller_owner is None:
            controller_url, owner_fingerprint = _get_controller_url(
                service_name, expected_service_hash)
        else:
            if fixed_controller_owner[0] != expected_service_hash:
                raise ControllerOwnerError(
                    'Fixed controller owner does not match the intended '
                    'service incarnation.')
            controller_url, owner_fingerprint = _get_local_controller_url(
                fixed_controller_owner)
        url = controller_url + path
        try:
            while True:
                headers = dict(base_headers)
                headers[constants.CONTROLLER_OWNER_HEADER] = owner_fingerprint
                if admin_tokens and not caller_supplied_auth:
                    headers['Authorization'] = (
                        f'Bearer {admin_tokens[token_index]}')
                request_kwargs = dict(kwargs)
                if headers:
                    request_kwargs['headers'] = headers
                response = request_fn(url, **request_kwargs)
                if (response.status_code == 401 and not caller_supplied_auth and
                        token_index + 1 < len(admin_tokens)):
                    token_index += 1
                    continue
                return response
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout):
            if attempt < _CONTROLLER_HTTP_RETRY_ATTEMPTS - 1:
                logger.debug(
                    f'Connection to controller {url} failed '
                    f'(attempt {attempt + 1}/'
                    f'{_CONTROLLER_HTTP_RETRY_ATTEMPTS}); retrying after '
                    f'{_CONTROLLER_HTTP_RETRY_BACKOFF_SECONDS}s.')
                time.sleep(_CONTROLLER_HTTP_RETRY_BACKOFF_SECONDS)
                continue
            logger.warning(
                f'Connection to controller {url} failed after '
                f'{_CONTROLLER_HTTP_RETRY_ATTEMPTS} attempts. '
                'Controller may be down, restarting, or mid-HA-flip.')
            raise


def _post_to_controller_with_retry(service_name: str,
                                   expected_service_hash: str, path: str,
                                   **kwargs):
    return _request_to_controller_with_retry('post', service_name,
                                             expected_service_hash, path,
                                             **kwargs)


def _get_to_controller_with_retry(service_name: str, expected_service_hash: str,
                                  path: str, **kwargs):
    return _request_to_controller_with_retry('get', service_name,
                                             expected_service_hash, path,
                                             **kwargs)


def get_service_placement_state(service_name: str,
                                expected_service_hash: str) -> dict[str, Any]:
    """Read one controller's in-memory placer state with bounded I/O."""
    response = _get_to_controller_with_retry(
        service_name,
        expected_service_hash,
        constants.CONTROLLER_PLACEMENT_ENDPOINT_PATH,
        timeout=(1.0, 2.0))
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError('Placement-state response must be an object.')
    return payload


def _get_to_local_controller_with_retry(service_name: str,
                                        controller_owner: _ControllerOwner,
                                        path: str, **kwargs):
    return _request_to_controller_with_retry(
        'get',
        service_name,
        controller_owner[0],
        path,
        fixed_controller_owner=controller_owner,
        **kwargs)


# NOTE(dev): We assume log are print with the hint 'sky api logs -l'. Be careful
# when changing UX as this assumption is used to expand some log files while
# ignoring others.
_SKYPILOT_LOG_HINT = r'.*sky api logs -l'
_SKYPILOT_PROVISION_API_LOG_PATTERN = (
    fr'{_SKYPILOT_LOG_HINT} (.*/provision\.log)')
# New hint pattern for provision logs
_SKYPILOT_PROVISION_LOG_CMD_PATTERN = r'.*sky logs --provision\s+(\S+)'
_SKYPILOT_LOG_PATTERN = fr'{_SKYPILOT_LOG_HINT} (.*\.log)'

# TODO(tian): Find all existing replica id and print here.
_FAILED_TO_FIND_REPLICA_MSG = (
    f'{colorama.Fore.RED}Failed to find replica '
    '{replica_id}. Please use `sky serve status [SERVICE_NAME]`'
    f' to check all valid replica id.{colorama.Style.RESET_ALL}')
# Max number of replicas to show in `sky serve status` by default.
# If user wants to see all replicas, use `sky serve status --all`.
_REPLICA_TRUNC_NUM = 10


class ServiceComponent(enum.Enum):
    CONTROLLER = 'controller'
    LOAD_BALANCER = 'load_balancer'
    REPLICA = 'replica'


@dataclasses.dataclass
class ServiceComponentTarget:
    """Represents a target service component with an optional replica ID.
    """
    component: ServiceComponent
    replica_id: int | None = None

    def __init__(self,
                 component: str | ServiceComponent,
                 replica_id: int | None = None):
        if isinstance(component, str):
            component = ServiceComponent(component)
        self.component = component
        self.replica_id = replica_id

    def __post_init__(self):
        """Validate that replica_id is only provided for REPLICA component."""
        if (self.component == ServiceComponent.REPLICA) != (self.replica_id
                                                            is None):
            raise ValueError(
                'replica_id must be specified if and only if component is '
                'REPLICA.')

    def __hash__(self) -> int:
        return hash((self.component, self.replica_id))

    def __str__(self) -> str:
        if self.component == ServiceComponent.REPLICA:
            return f'{self.component.value}-{self.replica_id}'
        return self.component.value


class UserSignal(enum.Enum):
    """User signal to send to controller.

    User can send signal to controller by writing to a file. The controller
    will read the file and handle the signal.
    """
    # Stop the controller, load balancer and all replicas.
    TERMINATE = 'terminate'

    # TODO(tian): Add more signals, such as pause.

    def error_type(self) -> type[Exception]:
        """Get the error corresponding to the signal."""
        return _SIGNAL_TO_ERROR[self]


class UpdateMode(enum.Enum):
    """Update mode for updating a service."""
    ROLLING = 'rolling'
    BLUE_GREEN = 'blue_green'


@dataclasses.dataclass
class TLSCredential:
    """TLS credential for the service."""
    keyfile: str
    certfile: str


DEFAULT_UPDATE_MODE = UpdateMode.ROLLING

_SIGNAL_TO_ERROR = {
    UserSignal.TERMINATE: exceptions.ServeUserTerminatedError,
}


class RequestsAggregator:
    """Base class for request aggregator."""

    def add(self, request: 'fastapi.Request') -> None:
        """Add a request to the request aggregator."""
        raise NotImplementedError

    def add_rejection(self) -> None:
        """Record one terminal load-balancer rejection."""
        raise NotImplementedError

    def clear(self) -> None:
        """Clear all current request aggregator."""
        raise NotImplementedError

    def drain(self) -> dict[str, Any]:
        """Atomically take the current report batch out of the aggregator.

        New samples added after this method returns belong to the next batch.
        The caller must restore the returned batch if delivery fails.
        """
        raise NotImplementedError

    def restore(self, batch: dict[str, Any]) -> None:
        """Restore a previously drained batch after failed delivery."""
        raise NotImplementedError

    def to_dict(self) -> dict[str, Any]:
        """Convert the aggregator to a dict."""
        raise NotImplementedError

    def request_history_snapshot(self) -> dict[str, Any] | None:
        """Return request-history counters awaiting acknowledgement."""
        raise NotImplementedError

    def mark_request_history_accepted(self,
                                      snapshot: dict[str, Any] | None) -> None:
        """Mark a request-history snapshot as durably accepted."""
        raise NotImplementedError

    def add_prediction_time(self, duration_seconds: float,
                            outcome: str) -> None:
        """Record one completed prediction."""
        raise NotImplementedError

    def prediction_time_history_snapshot(self) -> dict[str, Any] | None:
        """Return prediction-time counters awaiting acknowledgement."""
        raise NotImplementedError

    def mark_prediction_time_history_accepted(
            self, snapshot: dict[str, Any] | None) -> None:
        """Mark a prediction-time snapshot as durably accepted."""
        raise NotImplementedError

    def __repr__(self) -> str:
        raise NotImplementedError


class RequestTimestamp(RequestsAggregator):
    """RequestTimestamp: Aggregates request timestamps.

    This is useful for QPS-based autoscaling.
    """

    def __init__(self) -> None:
        # Bounded: the batch is retained across a failed controller sync (so
        # load signal is not dropped), but a persistent failure must not grow it
        # without limit -- maxlen keeps only the most recent samples (ample for
        # QPS autoscaling). See constants.LB_REQUEST_TIMESTAMP_CAP.
        self.timestamps: collections.deque[float] = collections.deque(
            maxlen=constants.LB_REQUEST_TIMESTAMP_CAP)
        self.compatibility_profiles: collections.deque[dict[str, Any]] = (
            collections.deque(maxlen=constants.LB_REQUEST_TIMESTAMP_CAP))
        # Exact arrival counters are reported independently from the lossy,
        # bounded raw timestamp batch used by autoscaling. Counts remain in
        # memory through the current hour so another request in an already
        # acknowledged minute advances the same cumulative counter.
        self._request_history: dict[int, int] = {}
        self._acknowledged_request_history: dict[int, int] = {}
        self._rejection_history: dict[int, int] = {}
        self._acknowledged_rejection_history: dict[int, int] = {}
        self._prediction_time_history: dict[int, dict[str, list[int]]] = {}
        self._acknowledged_prediction_time_history: dict[int,
                                                         dict[str,
                                                              list[int]]] = {}
        # Pruning rebuilds both bounded history dictionaries. Keep that work on
        # minute boundaries (and controller snapshots), never on every request.
        self._last_pruned_request_history_bucket: int | None = None

    def add(self, request: 'fastapi.Request') -> None:
        """Add a request to the request aggregator."""
        timestamp = time.time()
        self.timestamps.append(timestamp)
        compatible = getattr(request, '_skyserve_compatible_accelerators', None)
        self.compatibility_profiles.append({
            'timestamp': timestamp,
            'priority': int(
                getattr(request, '_skyserve_request_priority',
                        constants.LB_REQUEST_PRIORITY_MIN)),
            # None distinguishes a legacy omitted-catalog request from an
            # explicit canonical set; an empty list is never valid.
            'compatible_accelerators':
                (list(compatible) if compatible is not None else None),
        })
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        bucket_start = int(timestamp // bucket_seconds) * bucket_seconds
        self._request_history[bucket_start] = (
            self._request_history.get(bucket_start, 0) + 1)
        if bucket_start != self._last_pruned_request_history_bucket:
            self._prune_request_history(bucket_start)

    def add_rejection(self) -> None:
        """Record one terminal 503 in its completion-minute bucket."""
        timestamp = time.time()
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        bucket_start = int(timestamp // bucket_seconds) * bucket_seconds
        self._rejection_history[bucket_start] = (
            self._rejection_history.get(bucket_start, 0) + 1)
        if bucket_start != self._last_pruned_request_history_bucket:
            self._prune_request_history(bucket_start)

    def add_prediction_time(self, duration_seconds: float,
                            outcome: str) -> None:
        """Record one completed prediction in its observation minute."""
        timestamp = time.time()
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        bucket_start = int(timestamp // bucket_seconds) * bucket_seconds
        if outcome not in constants.LB_PREDICTION_TIME_OUTCOMES:
            raise ValueError(f'Unsupported prediction outcome: {outcome!r}.')
        if (not isinstance(duration_seconds, (int, float)) or
                isinstance(duration_seconds, bool) or
                not math.isfinite(duration_seconds)):
            raise ValueError('Prediction duration must be finite.')
        duration_seconds = max(0.0, float(duration_seconds))
        duration_bucket = bisect.bisect_left(
            constants.LB_PREDICTION_TIME_BUCKET_UPPER_BOUNDS_SECONDS,
            duration_seconds)
        outcome_counts = self._prediction_time_history.setdefault(
            bucket_start, {})
        counts = outcome_counts.setdefault(
            outcome, [0] * constants.LB_PREDICTION_TIME_BUCKET_COUNT)
        counts[duration_bucket] += 1
        if bucket_start != self._last_pruned_request_history_bucket:
            self._prune_request_history(bucket_start)

    def clear(self) -> None:
        """Clear all current request aggregator."""
        self.timestamps.clear()
        self.compatibility_profiles.clear()
        self._request_history.clear()
        self._acknowledged_request_history.clear()
        self._rejection_history.clear()
        self._acknowledged_rejection_history.clear()
        self._prediction_time_history.clear()
        self._acknowledged_prediction_time_history.clear()
        self._last_pruned_request_history_bucket = None

    def _prune_request_history(self, newest_bucket: int) -> None:
        oldest_bucket = (newest_bucket -
                         (constants.LB_REQUEST_HISTORY_MAX_BUCKETS - 1) *
                         constants.LB_REQUEST_HISTORY_BUCKET_SECONDS)
        self._request_history = {
            bucket: count
            for bucket, count in self._request_history.items()
            if bucket >= oldest_bucket
        }
        self._acknowledged_request_history = {
            bucket: count
            for bucket, count in self._acknowledged_request_history.items()
            if bucket >= oldest_bucket
        }
        self._rejection_history = {
            bucket: count
            for bucket, count in self._rejection_history.items()
            if bucket >= oldest_bucket
        }
        self._acknowledged_rejection_history = {
            bucket: count
            for bucket, count in self._acknowledged_rejection_history.items()
            if bucket >= oldest_bucket
        }
        self._prediction_time_history = {
            bucket: counts
            for bucket, counts in self._prediction_time_history.items()
            if bucket >= oldest_bucket
        }
        self._acknowledged_prediction_time_history = {
            bucket: counts
            for bucket, counts in
            self._acknowledged_prediction_time_history.items()
            if bucket >= oldest_bucket
        }
        self._last_pruned_request_history_bucket = newest_bucket

    def request_history_snapshot(self) -> dict[str, Any] | None:
        """Return counters changed since their last durable acknowledgement."""
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        newest_bucket = int(time.time() // bucket_seconds) * bucket_seconds
        self._prune_request_history(newest_bucket)
        bucket_starts = sorted(
            set(self._request_history) | set(self._rejection_history))
        buckets = []
        for bucket in bucket_starts:
            request_count = self._request_history.get(bucket, 0)
            rejected_count = self._rejection_history.get(bucket, 0)
            if (request_count <= self._acknowledged_request_history.get(
                    bucket, 0) and
                    rejected_count <= self._acknowledged_rejection_history.get(
                        bucket, 0)):
                continue
            bucket_payload = {
                'bucket_start': bucket,
                'request_count': request_count,
                'rejected_count': rejected_count,
            }
            buckets.append(bucket_payload)
        if not buckets:
            return None
        return {
            'bucket_seconds': constants.LB_REQUEST_HISTORY_BUCKET_SECONDS,
            'buckets': buckets,
        }

    def mark_request_history_accepted(self,
                                      snapshot: dict[str, Any] | None) -> None:
        """Acknowledge only counts present in an accepted snapshot.

        Requests arriving while the snapshot is in flight increment the live
        counter beyond the acknowledged value and are therefore sent on the
        next sync.
        """
        if snapshot is None:
            return
        for bucket in snapshot.get('buckets', []):
            bucket_start = bucket.get('bucket_start')
            request_count = bucket.get('request_count')
            rejected_count = bucket.get('rejected_count', 0)
            current_count = self._request_history.get(bucket_start)
            if current_count is not None:
                accepted_count = min(current_count, request_count)
                self._acknowledged_request_history[bucket_start] = max(
                    accepted_count,
                    self._acknowledged_request_history.get(bucket_start, 0))
            current_rejected = self._rejection_history.get(bucket_start)
            if current_rejected is not None:
                accepted_rejected = min(current_rejected, rejected_count)
                self._acknowledged_rejection_history[bucket_start] = max(
                    accepted_rejected,
                    self._acknowledged_rejection_history.get(bucket_start, 0))

    @staticmethod
    def _prediction_counts_advance(
            current: dict[str, list[int]],
            acknowledged: dict[str, list[int]] | None) -> bool:
        if acknowledged is None:
            return any(sum(counts) for counts in current.values())
        for outcome, counts in current.items():
            accepted = acknowledged.get(outcome, [])
            if any(count > (accepted[index] if index < len(accepted) else 0)
                   for index, count in enumerate(counts)):
                return True
        return False

    def prediction_time_history_snapshot(self) -> dict[str, Any] | None:
        """Return prediction histograms changed since durable acceptance."""
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        newest_bucket = int(time.time() // bucket_seconds) * bucket_seconds
        self._prune_request_history(newest_bucket)
        buckets = []
        for bucket_start in sorted(self._prediction_time_history):
            outcome_counts = self._prediction_time_history[bucket_start]
            acknowledged = self._acknowledged_prediction_time_history.get(
                bucket_start)
            if not self._prediction_counts_advance(outcome_counts,
                                                   acknowledged):
                continue
            buckets.append({
                'bucket_start': bucket_start,
                'outcome_counts': {
                    outcome: list(counts)
                    for outcome, counts in outcome_counts.items()
                    if any(counts)
                },
            })
        if not buckets:
            return None
        return {
            'bucket_seconds': constants.LB_REQUEST_HISTORY_BUCKET_SECONDS,
            'histogram_version': constants.LB_PREDICTION_TIME_HISTOGRAM_VERSION,
            'buckets': buckets,
        }

    def mark_prediction_time_history_accepted(
            self, snapshot: dict[str, Any] | None) -> None:
        """Acknowledge only histogram counts present in one accepted report."""
        if snapshot is None:
            return
        for bucket in snapshot.get('buckets', []):
            bucket_start = bucket.get('bucket_start')
            live = self._prediction_time_history.get(bucket_start)
            reported = bucket.get('outcome_counts')
            if live is None or not isinstance(reported, dict):
                continue
            acknowledged = self._acknowledged_prediction_time_history.setdefault(
                bucket_start, {})
            for outcome, reported_counts in reported.items():
                live_counts = live.get(outcome)
                if live_counts is None or not isinstance(reported_counts, list):
                    continue
                accepted = acknowledged.setdefault(
                    outcome, [0] * constants.LB_PREDICTION_TIME_BUCKET_COUNT)
                for index, reported_count in enumerate(reported_counts):
                    if index >= len(live_counts) or index >= len(accepted):
                        break
                    accepted[index] = max(
                        accepted[index], min(live_counts[index],
                                             reported_count))

    def drain(self) -> dict[str, Any]:
        """Take the current timestamps, leaving later arrivals untouched."""
        batch = self.to_dict()
        self.timestamps.clear()
        self.compatibility_profiles.clear()
        return batch

    def restore(self, batch: dict[str, Any]) -> None:
        """Merge a failed batch back ahead of any arrivals made in-flight.

        Extending oldest-to-newest also preserves the deque's bounded behavior:
        if the combined batches exceed the cap, only the newest timestamps are
        retained.
        """
        drained = batch.get('timestamps', [])
        drained_profiles = batch.get('compatibility_profiles', [])
        if not drained and not drained_profiles:
            return
        current = list(self.timestamps)
        current_profiles = list(self.compatibility_profiles)
        self.timestamps.clear()
        self.compatibility_profiles.clear()
        self.timestamps.extend(drained)
        self.timestamps.extend(current)
        self.compatibility_profiles.extend(drained_profiles)
        self.compatibility_profiles.extend(current_profiles)

    def to_dict(self) -> dict[str, Any]:
        """Convert the aggregator to a dict."""
        grouped_profiles: dict[tuple[int, frozenset[str]], dict[str, Any]] = {}
        for profile in self.compatibility_profiles:
            accelerators = profile.get('compatible_accelerators')
            priority = profile.get('priority')
            timestamp = profile.get('timestamp')
            count = profile.get('count', 1)
            if (not isinstance(accelerators, list) or not accelerators or
                    not isinstance(priority, int) or
                    not isinstance(timestamp, (int, float)) or
                    not isinstance(count, int) or count < 1):
                # Legacy omitted-catalog samples remain visible to aggregate
                # timestamp scaling but cannot be safely assigned to a card.
                continue
            key = (priority, frozenset(accelerators))
            grouped = grouped_profiles.get(key)
            if grouped is None:
                grouped_profiles[key] = {
                    'timestamp': timestamp,
                    'priority': priority,
                    'compatible_accelerators': list(accelerators),
                    'count': count,
                }
            else:
                grouped['timestamp'] = max(grouped['timestamp'], timestamp)
                grouped['count'] += count
        return {
            'timestamps': list(self.timestamps),
            'compatibility_profiles': list(grouped_profiles.values()),
        }

    def __repr__(self) -> str:
        return f'RequestTimestamp(timestamps={list(self.timestamps)})'


def get_service_filelock_path(pool: str) -> str:
    # Request serialization must not use an inode inside the canonical service
    # directory. Incarnation teardown deletes that directory; a waiter creating
    # ``<service>/pool.lock`` afterward would bypass the old lock inode and
    # silently lose serialization with the operation already in flight.
    digest = hashlib.sha256(pool.encode('utf-8')).hexdigest()
    path = (pathlib.Path(locks.SKY_LOCKS_DIR) /
            f'.skyserve-request-{digest}.lock').expanduser().absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


class ServiceLifecycleLock:
    """Advisory/file lock paired with a durable monotonically increasing token.

    Mutual exclusion handles the normal case; the epoch handles silent
    PostgreSQL session loss.  Resource mutations additionally use
    incarnation-scoped identities, while authoritative DB commits validate
    this token under a row lock.
    """

    def __init__(self, service_name: str, lock: locks.DistributedLock) -> None:
        self.service_name = service_name
        self.lock = lock
        self.epoch: int | None = None

    def acquire(self) -> 'ServiceLifecycleLock':
        self.lock.acquire()
        try:
            if isinstance(self.lock, locks.PostgresLock):
                self.epoch = self.lock.run_in_lock_session(
                    lambda connection:
                    serve_state.claim_service_lifecycle_epoch(
                        self.service_name, connection))
            else:
                self.epoch = serve_state.claim_service_lifecycle_epoch(
                    self.service_name)
            if not self.session_is_valid():
                raise RuntimeError('Lifecycle lock session was lost while '
                                   f'claiming {self.service_name!r}.')
        except BaseException:
            # Executor cancellation is delivered as KeyboardInterrupt.  If it
            # lands while claiming the fencing epoch, release the already-held
            # advisory lock before the worker is reused.
            self.lock.release()
            raise
        return self

    def release(self) -> None:
        self.lock.release()

    def session_is_valid(self) -> bool:
        if isinstance(self.lock, locks.PostgresLock):
            return self.lock.is_session_alive()
        return self.lock.is_locked()

    def __enter__(self) -> 'ServiceLifecycleLock':
        return self.acquire()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


def get_service_lifecycle_lock(service_name: str) -> ServiceLifecycleLock:
    """Return the cross-pod lock serializing destructive service lifecycles.

    The lock ID is outside the service working directory: deleting or
    quarantining that directory must never create a fresh lock inode. In
    PostgreSQL deployments this resolves to an advisory lock shared by every
    API pod; local/SQLite deployments use the runtime-global lock directory.
    """
    # The PostgreSQL epoch claim executes raw SQL on the advisory-lock session,
    # so ensure migration 008 has created its table before acquiring that
    # session and attempting the claim.
    serve_state.ensure_tables_initialized()
    # Generic lock auto-detection intentionally falls back to a local FileLock
    # when DB initialization raises. That is acceptable for best-effort
    # callers, but unsafe here: a transient PostgreSQL/config outage would let
    # multiple HA pods concurrently destroy the same name. Detect explicitly
    # and fail closed instead.
    engine = global_user_state.initialize_and_get_db()
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        lock_type = 'postgres'
    elif engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        lock_type = 'filelock'
    else:
        raise RuntimeError('Unsupported database dialect for service '
                           f'lifecycle lock: {engine.dialect.name!r}.')
    digest = hashlib.sha256(service_name.encode('utf-8')).hexdigest()
    lock = locks.get_lock(f'skyserve-lifecycle-{digest}', lock_type=lock_type)
    return ServiceLifecycleLock(service_name, lock)


def lifecycle_lock_is_valid(lock: ServiceLifecycleLock) -> bool:
    """Whether a lifecycle lease still owns both session and durable token."""
    if lock.epoch is None or not lock.session_is_valid():
        return False
    try:
        return serve_state.service_lifecycle_epoch_matches(
            lock.service_name, lock.epoch)
    except Exception:  # pylint: disable=broad-except
        # A DB outage makes the durable fence unverifiable.  Destructive work
        # must stop rather than degrading to a process-local assumption.
        return False


def get_service_lifecycle_epoch(lock: ServiceLifecycleLock) -> int:
    """Return an acquired lifecycle lease's durable token."""
    if lock.epoch is None:
        raise RuntimeError('Service lifecycle lock has not been acquired.')
    return lock.epoch


def advance_service_lifecycle_epoch(lock: ServiceLifecycleLock) -> int:
    """Fence an in-flight lifecycle operation while retaining its name lock."""
    if not lifecycle_lock_is_valid(lock):
        raise RuntimeError('Cannot advance a lost service lifecycle lock.')
    if isinstance(lock.lock, locks.PostgresLock):
        epoch = lock.lock.run_in_lock_session(
            lambda connection: serve_state.claim_service_lifecycle_epoch(
                lock.service_name, connection))
    else:
        epoch = serve_state.claim_service_lifecycle_epoch(lock.service_name)
    lock.epoch = epoch
    if not lock.session_is_valid():
        raise RuntimeError('Lifecycle lock session was lost while fencing '
                           f'{lock.service_name!r}.')
    return epoch


def remove_service_directory(service_dir: str) -> None:
    """Remove one already-fenced incarnation directory.

    New service directories are derived from the durable resource scope, so
    they can be deleted after the service row is removed without any rename or
    canonical-path TOCTOU.  A legacy directory is also safe here: every
    successor created by this version uses a scoped path and therefore cannot
    occupy the old name-only location.
    """
    try:
        if os.path.islink(service_dir):
            os.unlink(service_dir)
        else:
            shutil.rmtree(service_dir)
    except FileNotFoundError:
        pass


def _validate_consolidation_mode_config(current_is_consolidation_mode: bool,
                                        pool: bool) -> None:
    """Validate the consolidation mode config."""
    # Check whether the consolidation mode config is changed.
    controller = controller_utils.get_controller_for_pool(pool).value
    if current_is_consolidation_mode:
        controller_cn = controller.cluster_name
        if global_user_state.cluster_with_name_exists(controller_cn):
            logger.warning(
                f'{colorama.Fore.RED}Consolidation mode for '
                f'{controller.controller_type} is enabled, but the controller '
                f'cluster {controller_cn} is still running. Please terminate '
                'the controller cluster first.'
                f'{colorama.Style.RESET_ALL}')
    else:
        noun = 'pool' if pool else 'service'
        num_services = serve_state.get_num_services(pool=pool)
        if num_services:
            logger.warning(
                f'{colorama.Fore.RED}Consolidation mode for '
                f'{controller.controller_type} is disabled, but there are '
                f'still {num_services} {noun}s running. Please terminate '
                f'those {noun}s first.{colorama.Style.RESET_ALL}')


def _pool_consolidation_extra_validator(arg: bool) -> None:
    """Warn about leftover pools when switching to non-consolidated mode.

    Passed as extra_validator to controller_utils.is_jobs_consolidation_mode
    from the pool branch of is_consolidation_mode. Skipped when consolidation
    is on because the jobs validator already warns about the shared
    controller cluster in that case.
    """
    if not arg:
        _validate_consolidation_mode_config(arg, pool=True)


@annotations.lru_cache(scope='request', maxsize=1)
def is_consolidation_mode(pool: bool = False) -> bool:
    if pool:
        # INVARIANT: pool consolidation state must match managed jobs —
        # pool operations run on the jobs controller. Route both readers
        # through controller_utils.is_jobs_consolidation_mode so they
        # cannot diverge. Pool adds one extra validator (leftover pools)
        # because the jobs validator only knows about leftover jobs.
        return controller_utils.is_jobs_consolidation_mode(
            extra_validator=_pool_consolidation_extra_validator)
    # The external-only Helm topology necessarily runs service controllers in
    # the API pod. Treat its explicit capability signal as authoritative so
    # enabling the chart cannot accidentally launch an obsolete, billable
    # dedicated controller VM because an old persisted config omitted the
    # consolidation flag.
    if (os.environ.get(constants.EXTERNAL_LB_ENABLED_ENV_VAR,
                       '').lower() == 'true'):
        return True
    # Serve (pool=False) otherwise runs on its own controller cluster,
    # independent of the jobs controller, and keeps a config-driven
    # consolidation flag for compatibility outside the Helm topology.
    if os.environ.get(skylet_constants.OVERRIDE_CONSOLIDATION_MODE) is not None:
        # if we are in the serve controller, we must always be in
        # consolidation mode.
        return True
    consolidation_mode = skypilot_config.get_nested(
        ('serve', 'controller', 'consolidation_mode'), default_value=False)
    # We should only do this check on API server, as the controller will not
    # have related config and will always seemingly disabled for consolidation
    # mode. Check #6611 for more details.
    if os.environ.get(skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER) is not None:
        _validate_consolidation_mode_config(consolidation_mode, pool)
    return consolidation_mode


def is_external_load_balancer_mode() -> bool:
    """Whether the external-LB platform capability is enabled.

    Helm injects the same explicit capability into the API pod and every
    generated LB pod. Consolidated controller children inherit the API pod's
    environment. No persisted or per-service config participates, so all
    processes in the topology necessarily agree. False/unset means service
    startup is unsupported (pools remain valid because they have no inference
    endpoint); it no longer selects an in-pod implementation.
    """
    return (os.environ.get(constants.EXTERNAL_LB_ENABLED_ENV_VAR,
                           '').lower() == 'true')


def replica_tls_mode() -> str:
    """Encryption mode for the load-balancer-to-replica hop.

    Read from the same Helm-injected environment as the external-LB capability
    flag, and for the same reason: the controller (which mints and injects the
    key material) and the load balancer (which pins it) must never disagree, or
    the LB would dial https at a plaintext replica, or verify against material
    the replica was never given.
    """
    mode = os.environ.get(constants.REPLICA_TLS_MODE_ENV_VAR,
                          '').strip().lower()
    if not mode:
        return constants.REPLICA_TLS_MODE_OFF
    if mode not in constants.REPLICA_TLS_MODES:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                f'{constants.REPLICA_TLS_MODE_ENV_VAR}={mode!r} is not one of '
                f'{", ".join(constants.REPLICA_TLS_MODES)}.')
    return mode


def ha_recovery_for_consolidation_mode(pool: bool,
                                       still_leader: Callable[[], bool] |
                                       None = None):
    """Recovery logic for HA mode.

    Args:
        pool: Whether to recover pools (True) or services (False).
        still_leader: Optional probe returning whether this pod still holds
            the consolidation leader lock. Re-checked before every recovery
            launch: the leader lock is session-scoped (a PG advisory lock),
            so it can be silently lost mid-sweep (RDS failover, idle
            timeout), at which point another pod may already be running its
            own recovery. Launching more controllers here without
            leadership would split-brain, so the sweep aborts instead. None
            means the caller has no revocable leadership concept (e.g. a
            single-pod filelock deployment).
    """
    # No setup recovery is needed in consolidation mode, as the API server
    # already has all runtime installed. Directly start jobs recovery here.
    # Refers to sky/templates/kubernetes-ray.yml.j2 for more details.
    runner = command_runner.LocalProcessCommandRunner()
    noun = 'pool' if pool else 'serve'
    capnoun = noun.capitalize()
    prefix = f'{noun}_'
    # Snapshot the set of in-flight _start service names once per iteration
    # so we don't walk /proc N times for N services. This also gives all
    # services a consistent view (no torn read where service A is checked
    # before service B's _start spawns, and B is checked after).
    in_flight_service_incarnations = (
        _snapshot_in_flight_start_service_incarnations())
    with open(skylet_constants.HA_PERSISTENT_RECOVERY_LOG_PATH.format(prefix),
              'w',
              encoding='utf-8') as f:
        start = time.time()
        f.write(f'Starting HA recovery at {datetime.datetime.now()}\n')
        # Snapshot only the mode this sweep can recover. Pools never own
        # external LBs, so the service-mode snapshot is also the complete set
        # of live LB owners used by reconciliation below.
        service_names = serve_state.get_glob_service_names(None, pool=pool)
        # One slim query for the whole sweep instead of a per-service joined
        # read (which deserializes the latest spec N times). The snapshot
        # carries every field this loop consumes, including the latest
        # version's raw yaml for placeholder detection.
        liveness_snapshots = {
            record['name']: record
            for record in serve_state.get_service_liveness_snapshots(pool=pool)
        }
        committed_version_candidates = [
            service_name for service_name in service_names
            if ((svc := liveness_snapshots.get(service_name)) is None or
                svc.get('yaml_content') is None)
        ]
        latest_committed_versions = serve_state.get_latest_committed_versions(
            committed_version_candidates)
        raw_identities = serve_state.get_service_mode_and_hashes([
            service_name for service_name in committed_version_candidates
            if service_name not in latest_committed_versions
        ])
        for service_name in service_names:
            svc = liveness_snapshots.get(service_name)
            # A row with no version_specs row is invisible to the joined
            # snapshot query.  A row whose latest version is a NULL-yaml
            # placeholder is visible, but is equally unbootable when it has
            # no earlier committed version.  Retire both shapes atomically;
            # mark_unrecoverable_service_for_cleanup rechecks the absence of
            # committed yaml in the same transaction as the terminal fence.
            needs_committed_version_check = (svc is None or
                                             ('yaml_content' in svc and
                                              svc['yaml_content'] is None))
            if needs_committed_version_check:
                committed_version = latest_committed_versions.get(service_name)
                if committed_version is None:
                    raw_identity = raw_identities.get(service_name)
                    if (raw_identity is not None and raw_identity[0] == pool and
                            isinstance(raw_identity[1], str) and
                            raw_identity[1]):
                        retired = (
                            serve_state.mark_unrecoverable_service_for_cleanup(
                                service_name, raw_identity[1], pool))
                        if retired:
                            f.write(
                                f'{capnoun} {service_name} has no committed '
                                'version; retired its unusable recovery '
                                'script and marked it for purge.\n')
                    continue
            if svc is None:
                # The row disappeared or changed mode between the name and
                # joined-status snapshots. It is no longer ours to recover in
                # this sweep.
                continue
            controller_pid = svc['controller_pid']
            controller_ip = svc.get('controller_ip')
            status_dbg = svc.get('status')
            f.write(f'ha_recovery candidate {service_name}: '
                    f'pid={controller_pid} ip={controller_ip} '
                    f'status={status_dbg}\n')
            if controller_pid is not None:
                try:
                    alive = _controller_process_alive(
                        controller_pid,
                        service_name,
                        svc.get('hash'),
                        allow_legacy=(svc.get('resource_scope') is None))
                except Exception as e:  # pylint: disable=broad-except
                    # _controller_process_alive may raise if psutil fails
                    # (transient AccessDenied / cmdline read race / etc).
                    # Treating "raised" as "dead" would replace a possibly-
                    # alive controller every iteration that hits the
                    # exception, churning pid/ip/port and disrupting
                    # in-flight requests. Be conservative: skip this round.
                    f.write(f'Error checking controller pid {controller_pid}'
                            f' for {noun} {service_name}: {e}\n')
                    continue
                if alive:
                    f.write(f'Controller pid {controller_pid} for '
                            f'{noun} {service_name} is still running. '
                            'Skipping recovery.\n')
                    continue

            # Defense in depth: even if DB controller_pid is stale (e.g. an
            # older _start hadn't yet pre-claimed it, or pre-claim is
            # disabled / lost), still skip the recovery launch if any
            # `python -m sky.serve.service --service-name <name>` is
            # already running on this pod (snapshot taken once at the
            # top of the iteration). Otherwise the daemon's ~20s
            # iteration repeatedly fires recovery during the 0-60s
            # controller boot window, piling up multiple _start instances.
            service_hash = svc.get('hash')
            resource_scope = svc.get('resource_scope')
            exact_start_running = ((service_name, service_hash)
                                   in in_flight_service_incarnations)
            legacy_start_running = (resource_scope is None and
                                    (service_name, None)
                                    in in_flight_service_incarnations)
            if exact_start_running or legacy_start_running:
                f.write(f'{capnoun} {service_name}: _start process already '
                        f'running on this pod; skipping recovery this '
                        f'round.\n')
                continue

            script = serve_state.get_ha_recovery_script(service_name)
            if script is None:
                f.write(f'{capnoun} {service_name}\'s recovery script does '
                        'not exist. Skipping recovery.\n')
                continue
            # Fence right before the launch: the leader-lock session may have
            # died since the caller's top-of-iteration probe (this sweep can
            # take a while with many services). Without leadership, another
            # pod may already be recovering the same services — abort the
            # sweep instead of racing it.
            if still_leader is not None and not still_leader():
                msg = ('Consolidation leader lock session lost mid-recovery; '
                       'aborting the rest of this recovery sweep.')
                f.write(msg + '\n')
                logger.error(msg)
                break
            # Recreate the service working directory before running the
            # recovery script. It lives on pod-local storage (emptyDir), so a
            # pod REPLACEMENT (rolling update, reschedule) wipes it while the
            # durable service row and recovery script survive in the DB. The
            # stored script redirects its output into this directory; without
            # the mkdir the redirect fails and the script dies instantly —
            # recovery then retries every ~20s forever and the service stays
            # headless (measured live: a 224-replica spot fleet sat
            # CONTROLLER_FAILED for hours while its replicas kept billing).
            # _start itself re-derives everything else from the DB on
            # recovery, so the empty directory is all that is needed.
            try:
                os.makedirs(os.path.expanduser(
                    generate_remote_service_dir_name(
                        service_name, svc.get('resource_scope'))),
                            exist_ok=True)
            except OSError as e:
                f.write(f'Failed to recreate the service dir for '
                        f'{service_name}: {e}\n')
            rc, out, err = runner.run(script, require_outputs=True)
            if rc:
                f.write(f'Recovery script returned {rc}. '
                        f'Output: {out}\nError: {err}\n')
            f.write(f'{capnoun} {service_name} completed recovery at '
                    f'{datetime.datetime.now()}\n')
        # Reap external LB objects whose owning service no longer exists. Only
        # for services (pools have no LB). No-op outside external-LB +
        # in-cluster mode. Lazy import: lb_k8s imports serve_utils at module
        # level, so a top-level import here would be circular.
        if not pool:
            from sky.serve import (  # pylint: disable=import-outside-toplevel  # noqa: E501
                lb_k8s)
            try:
                lb_k8s.reconcile_lb_objects(set(service_names))
            except Exception as e:  # pylint: disable=broad-except
                # Reconcile is best-effort cleanup; never let it abort recovery.
                f.write(f'Failed to reconcile external LB objects: {e}\n')
        f.write(f'HA recovery completed at {datetime.datetime.now()}\n')
        f.write(f'Total recovery time: {time.time() - start} seconds\n')


def _controller_process_alive(pid: int,
                              service_name: str,
                              service_incarnation: str | None = None,
                              allow_legacy: bool = True) -> bool:
    """Check exact local controller identity, not pod-local PID alone."""
    try:
        process = psutil.Process(pid)
        cmdline = process.cmdline()
        if not process.is_running():
            return False
        try:
            name_idx = cmdline.index('--service-name')
        except ValueError:
            return False
        if name_idx + 1 >= len(cmdline) or cmdline[name_idx +
                                                   1] != service_name:
            return False
        try:
            incarnation_idx = cmdline.index('--service-incarnation')
        except ValueError:
            return allow_legacy
        if incarnation_idx + 1 >= len(cmdline):
            return False
        return (service_incarnation is not None and
                cmdline[incarnation_idx + 1] == service_incarnation)
    except psutil.NoSuchProcess:
        return False


def _snapshot_in_flight_start_service_incarnations(
) -> set[tuple[str, str | None]]:
    """Return active ``(service name, requested incarnation)`` processes.

    Used by ha_recovery_for_consolidation_mode to deduplicate recovery
    launches: while a previously-spawned _start is still in its 0-60s
    boot window waiting for the controller subprocess to bind, DB
    controller_pid may still point at the dead previous instance.
    Re-firing the recovery script in that window causes pile-up
    (multiple _start instances racing on the same service).

    Snapshotting once per daemon iteration (rather than per-service)
    gives O(processes + N services) instead of O(processes * N), and
    also a consistent view (all services see the same set).

    Zombies are excluded — a `_start` process that died but hasn't been
    reaped (pods without a proper init process) would otherwise
    permanently block recovery for that service.

    Matching is on the argv LIST (not a joined string), so
    `--service-name pool-a` does not falsely match `--service-name pool-abc`.
    """
    in_flight: set[tuple[str, str | None]] = set()
    for proc in psutil.process_iter(['cmdline', 'status']):
        try:
            if proc.info.get('status') == psutil.STATUS_ZOMBIE:
                continue
            cmdline = proc.info.get('cmdline') or []
            if 'sky.serve.service' not in ' '.join(cmdline):
                continue
            try:
                idx = cmdline.index('--service-name')
            except ValueError:
                continue
            if idx + 1 < len(cmdline):
                service_name = cmdline[idx + 1]
                incarnation = None
                try:
                    incarnation_idx = cmdline.index('--service-incarnation')
                except ValueError:
                    pass
                else:
                    if incarnation_idx + 1 < len(cmdline):
                        incarnation = cmdline[incarnation_idx + 1]
                in_flight.add((service_name, incarnation))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return in_flight


def _start_in_flight(service_name: str) -> bool:
    """Thin wrapper around the process snapshot for
    one-off checks (e.g. tests, ad-hoc callers).

    The hot path in `ha_recovery_for_consolidation_mode` calls the
    snapshot helper directly and reuses the set across services.
    """
    return any(name == service_name
               for name, _ in _snapshot_in_flight_start_service_incarnations())


def validate_external_lb_service_spec(
        service_spec: 'service_spec_lib.SkyServiceSpec') -> None:
    """Validate service fields implemented by the external-only LB.

    Task-level TLS used to terminate inside the in-pod load balancer. The
    external LB intentionally serves HTTP behind the platform ingress, so
    accepting that field would persist ``tls_encrypted=True`` and advertise an
    HTTPS endpoint that does not exist.
    """
    if service_spec.tls_credential is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'Task-level service.tls_credential is not supported by the '
                'external SkyServe load balancer. Terminate TLS at the '
                'platform ingress/load balancer and remove tls_credential '
                'from the service specification.')


def snapshot_service_container_images(
    task: 'sky.Task',
    workspace: str | None = None,
) -> str | None:
    """Pins every candidate in one service version to one artifact.

    Managed selectors may retain different distribution profiles for
    placement, but their content identity must converge. Explicit direct
    selectors must also converge. Legacy image_id candidates remain outside
    the catalog and retain their historical heterogeneous behavior. The
    rewritten task YAML is the durable per-version snapshot.
    """
    try:
        artifact_ids = container_image_task_utils.snapshot_task_container_images(
            [task], workspace)
    except ValueError:
        with ux_utils.print_exception_no_traceback():
            raise
    return next(iter(artifact_ids), None)


def resolve_service_workspace(
    service_name: str,
    service_record: dict[str, Any],
    requested_workspace: str | None = None,
    *,
    trusted_recovery_hint: bool = False,
) -> str:
    """Returns and, when necessary, safely backfills a service workspace.

    Existing service rows predate durable workspace storage. Replica cluster
    rows are authoritative evidence because every launch already persisted its
    active workspace. A controller's service-scoped recovery script is also a
    valid hint when no replica has ever been launched; an ordinary update
    request is not.
    """
    stored_workspace = service_record.get('workspace')
    if isinstance(stored_workspace, str) and stored_workspace:
        if (requested_workspace is not None and
                requested_workspace != stored_workspace):
            raise RuntimeError(
                f'Service {service_name!r} belongs to workspace '
                f'{stored_workspace!r}, not {requested_workspace!r}.')
        return stored_workspace

    expected_service_hash = service_record.get('hash')
    if (not isinstance(expected_service_hash, str) or
            not expected_service_hash):
        raise RuntimeError(
            f'Cannot safely recover legacy service {service_name!r} without '
            'a durable incarnation hash.')

    replica_infos = serve_state.get_replica_infos(service_name)
    cluster_names = list(
        dict.fromkeys(
            info.cluster_name
            for info in replica_infos
            if isinstance(info.cluster_name, str) and info.cluster_name))
    cluster_records = global_user_state.get_clusters_from_names(cluster_names)
    evidenced_workspaces = {
        record['workspace']
        for record in cluster_records.values()
        if record is not None and isinstance(record.get('workspace'), str) and
        record['workspace']
    }
    if len(evidenced_workspaces) > 1:
        raise RuntimeError(
            f'Cannot safely recover legacy service {service_name!r}: its '
            'replica clusters belong to multiple workspaces.')

    inferred_workspace = next(iter(evidenced_workspaces), None)
    if inferred_workspace is not None:
        if (requested_workspace is not None and
                requested_workspace != inferred_workspace):
            raise RuntimeError(
                f'Service {service_name!r} replica evidence belongs to '
                f'workspace {inferred_workspace!r}, not '
                f'{requested_workspace!r}.')
        workspace = inferred_workspace
    elif (trusted_recovery_hint and isinstance(requested_workspace, str) and
          requested_workspace):
        workspace = requested_workspace
    else:
        raise RuntimeError(
            f'Cannot safely recover legacy service {service_name!r} without '
            'a durable workspace or replica-cluster workspace evidence. '
            'Recreate it in the intended workspace.')

    if not serve_state.set_service_workspace_if_owner(service_name, workspace,
                                                      expected_service_hash):
        refreshed = serve_state.get_service_from_name(service_name)
        if (refreshed is None or
                refreshed.get('hash') != expected_service_hash or
                refreshed.get('workspace') != workspace):
            raise RuntimeError(
                f'Lost ownership while backfilling workspace for service '
                f'{service_name!r}.')
    logger.warning(f'Backfilled legacy service {service_name!r} workspace '
                   f'to {workspace!r}.')
    return workspace


def validate_logical_replica_task(
        task: 'sky.Task',
        service_spec: 'service_spec_lib.SkyServiceSpec | None' = None) -> None:
    """Reject topologies without a defined logical-replica contract."""
    if service_spec is None:
        service_spec = task.service
    if (service_spec is not None and
            getattr(service_spec, 'uses_logical_replicas', False) is True and
            task.num_nodes != 1):
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'dynamic_fallback_per_gpu currently supports only single-node '
                'services. Multi-node replica routing does not yet define a '
                'safe logical capacity contract.')


def resolve_replica_ingress_port(task: 'sky.Task', pool: bool) -> str:
    """Resolve the one ingress port accepted by Serve validation and launch."""
    if task.service is None:
        raise RuntimeError('Service or pool section not found.')
    if pool:
        if (task.service.ports is not None or any(
                resources.ports is not None for resources in task.resources)):
            raise ValueError('Cannot specify ports in a pool.')
        return '-'
    if task.service.ports is not None:
        return task.service.ports

    inferred_ports: set[int] = set()
    for resources in task.resources:
        ports = list(resources_utils.port_ranges_to_set(resources.ports))
        if len(ports) != 1:
            raise ValueError(
                'To open multiple ports on the replica, please set the '
                '`service.ports` field to specify a main service port. '
                'Must only specify one port in resources otherwise. '
                'Each replica will use the port specified as application '
                f'ingress port. Got {ports!r}.')
        inferred_ports.add(ports[0])
    if len(inferred_ports) != 1:
        raise ValueError('Got multiple ports in different resources: '
                         f'{sorted(inferred_ports)!r}. Please specify the '
                         'same port instead.')
    return str(next(iter(inferred_ports)))


def validate_service_task(task: 'sky.Task', pool: bool) -> None:
    """Validate the task for Sky Serve.

    Args:
        task: sky.Task to validate

    Raises:
        ValueError: if the arguments are invalid.
        RuntimeError: if the task.serve is not found.
    """
    spot_resources: list[sky.Resources] = [
        resource for resource in task.resources if resource.use_spot
    ]
    has_spot_placer = (task.service is not None and
                       task.service.spot_placer is not None)
    # A spot placer may manage a heterogeneous set that mixes spot cloud
    # entries with non-spot reserved-capacity entries (e.g. a zero-cost
    # Kubernetes pool): each launch is pinned to its location's spot-ness.
    # Without a placer, mixing stays unsupported.
    if (len(spot_resources) not in [0, len(task.resources)] and
            not has_spot_placer):
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'Resources must either all use spot or none use spot. '
                'To use on-demand and spot instances together, '
                'use `dynamic_ondemand_fallback`, set '
                'base_ondemand_fallback_replicas, or configure a '
                '`spot_placer` to manage a mixed set.')

    field_name = 'service' if not pool else 'pool'
    if task.service is None:
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(f'{field_name.capitalize()} section not found.')

    if pool != task.service.pool:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'{field_name.capitalize()} section in the YAML '
                             f'file does not match the pool argument. '
                             f'To fix, add a valid `{field_name}` field.')

    validate_logical_replica_task(task)

    # Validate that pools do not use ordered resources
    if pool and isinstance(task.resources, list):
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'Ordered resources are not supported for pools. '
                'Use `any_of` instead, or specify a single resource.')

    policy_description = ('on-demand'
                          if task.service.dynamic_ondemand_fallback else 'spot')
    for resource in list(task.resources):
        if resource.job_recovery is not None:
            sys_name = 'SkyServe' if not pool else 'Pool'
            with ux_utils.print_exception_no_traceback():
                raise ValueError(f'job_recovery is disabled for {sys_name}. '
                                 f'{sys_name} will replenish preempted spot '
                                 f'with {policy_description} instances.')

    # Reserved fill supports one Kubernetes context per service. Multiple
    # accelerator names in that context form one brokered capacity group,
    # provided they use the same GPU count per backend. Zero-cost-ness is not
    # fully knowable client-side, so all Kubernetes entries are the safe
    # conservative candidate set.
    if task.service.reserved_capacity_fill:
        pool_shapes: dict[tuple[str | None, str], int] = {}
        for requested_resources in task.resources:
            if str(requested_resources.cloud).lower() != 'kubernetes':
                continue
            accelerators = requested_resources.accelerators or {}
            if not accelerators:
                continue
            gpu_name, gpu_count = next(iter(accelerators.items()))
            is_numeric = (not isinstance(gpu_count, bool) and
                          isinstance(gpu_count, (int, float)))
            is_finite = is_numeric and math.isfinite(float(gpu_count))
            is_whole = is_finite and float(gpu_count).is_integer()
            if (task.service.uses_logical_replicas and
                (not is_whole or float(gpu_count) != 1.0)):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'dynamic_fallback_per_gpu with '
                        'reserved_capacity_fill requires one-GPU Kubernetes '
                        'fill shapes so broker slots equal logical slots. '
                        f'Got {gpu_name}:{gpu_count!r}.')
            if not is_whole or float(gpu_count) < 1:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'reserved_capacity_fill requires each Kubernetes GPU '
                        'count to be a positive whole number. '
                        f'Got {gpu_name}:{gpu_count!r}.')
            key = (requested_resources.region, gpu_name.lower())
            pool_shapes[key] = max(pool_shapes.get(key, 1), int(gpu_count))
        contexts = {context for context, _ in pool_shapes}
        gpu_counts = set(pool_shapes.values())
        if len(contexts) > 1 or len(gpu_counts) > 1:
            shapes = sorted(pool_shapes.items(), key=repr)
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'reserved_capacity_fill requires one Kubernetes context '
                    'and one GPU count per backend; the resources span '
                    f'{shapes}.')
        if (task.service.uses_logical_replicas and gpu_counts and
                gpu_counts != {1}):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'dynamic_fallback_per_gpu with reserved_capacity_fill '
                    'requires one-GPU Kubernetes fill shapes so broker slots '
                    'equal logical slots.')

    # Validate the placer contract without enumerating providers. The final
    # policy-mutated task gets one complete catalog immediately before its
    # immutable service version is committed.
    spot_placer.SpotPlacer.validate_task(task.service, task)

    replica_ingress_port = resolve_replica_ingress_port(task, pool)
    for requested_resources in task.resources:
        if (task.service.use_ondemand_fallback and
                not requested_resources.use_spot):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    '`use_ondemand_fallback` is only supported '
                    'for spot resources. Please explicitly specify '
                    '`use_spot: true` in resources for on-demand fallback.')
        if (task.service.spot_placer is not None and
                not requested_resources.use_spot and not spot_resources):
            # Non-spot entries are fine under a placer as the reserved
            # zero-cost tier of a mixed set — but a placer over a set with
            # NO spot entry at all is a misconfiguration.
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    '`spot_placer` requires at least one spot resource. '
                    'Please specify `use_spot: true` on the cloud entries.')
    if not pool and task.service.ports is None:
        task.service.set_ports(replica_ingress_port)


def generate_service_name(pool: bool = False):
    noun = 'pool' if pool else 'service'
    return f'sky-{noun}-{uuid.uuid4().hex[:4]}'


def _resource_scope_tag(resource_scope: str, length: int = 20) -> str:
    """Filesystem/cloud-safe digest for an incarnation resource scope."""
    return hashlib.sha256(resource_scope.encode('utf-8')).hexdigest()[:length]


def generate_ephemeral_storage_scope_id(resource_scope: str,
                                        storage_generation: str) -> str:
    """Return one version generation's compact bucket/path namespace."""
    # Keep close to the historical 8-character file-mount run ID so generated
    # bucket names remain within provider limits. The prefix distinguishes a
    # Serve-owned namespace from an arbitrary user suffix.
    identity = json.dumps([resource_scope, storage_generation],
                          separators=(',', ':'))
    return f'sv{_resource_scope_tag(identity, length=10)}'


def ephemeral_storage_identity_matches_scope(storage: Any,
                                             scope_id: str) -> bool:
    """Whether a storage object's bucket/subpath carries ``scope_id``."""
    suffix = f'-{scope_id}'
    name = getattr(storage, 'name', None)
    if isinstance(name, str) and name.endswith(suffix):
        return True
    source = getattr(storage, 'source', None)
    if isinstance(source, str):
        # Covers provider URI shapes (bucket in netloc for S3/GCS/R2, path
        # segment for Azure/COS/OCI) without treating a substring inside a
        # larger identifier as ownership.
        source_without_query = source.split('?', 1)[0].rstrip('/')
        if any(
                segment.endswith(suffix)
                for segment in source_without_query.split('/')):
            return True
    bucket_sub_path = getattr(storage, '_bucket_sub_path', None)
    if isinstance(bucket_sub_path, str):
        scoped_prefix = f'job-{scope_id}'
        normalized = bucket_sub_path.strip('/')
        if (normalized == scoped_prefix or
                normalized.startswith(f'{scoped_prefix}/') or
                f'/{scoped_prefix}/' in f'/{normalized}/'):
            return True
    return False


def generate_remote_service_dir_name(service_name: str,
                                     resource_scope: str | None = None) -> str:
    legacy_name = service_name.replace('-', '_')
    if resource_scope is None:
        # Compatibility only for rows created before resource_scope existed.
        # New incarnations never use this lossy name.
        return os.path.join(constants.SKYSERVE_METADATA_DIR, legacy_name)
    # The readable prefix is deliberately non-authoritative: validation has
    # historically admitted names whose normalized forms collide (`svc-a`,
    # `svc_a`, `Svc.A`).  Hash the exact original spelling as well as the
    # incarnation so the path identity remains injective across both service
    # names and same-name successors.
    readable_name = re.sub(r'[^A-Za-z0-9]+', '_', service_name).strip('_')
    if not readable_name:
        readable_name = 'service'
    name_tag = _resource_scope_tag(service_name, length=16)
    scope_tag = _resource_scope_tag(resource_scope)
    scoped_name = f'{readable_name}_name_{name_tag}_inc_{scope_tag}'
    return os.path.join(constants.SKYSERVE_METADATA_DIR, scoped_name)


def generate_remote_tmp_task_yaml_file_name(service_name: str,
                                            resource_scope: str | None = None
                                           ) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'task.yaml.tmp')


def generate_remote_tmp_submitted_task_yaml_file_name(service_name: str,
                                                      resource_scope: str |
                                                      None = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    return os.path.join(dir_name, 'submitted_task.yaml.tmp')


def generate_task_yaml_file_name(service_name: str,
                                 version: int,
                                 expand_user: bool = True,
                                 resource_scope: str | None = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    if expand_user:
        dir_name = os.path.expanduser(dir_name)
    return os.path.join(dir_name, f'task_v{version}.yaml')


def generate_submitted_task_yaml_file_name(
        service_name: str,
        version: int,
        expand_user: bool = True,
        resource_scope: str | None = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    if expand_user:
        dir_name = os.path.expanduser(dir_name)
    return os.path.join(dir_name, f'submitted_task_v{version}.yaml')


def generate_remote_config_yaml_file_name(service_name: str,
                                          resource_scope: str | None = None
                                         ) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'config.yaml')


def generate_remote_controller_log_file_name(service_name: str,
                                             resource_scope: str | None = None
                                            ) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'controller.log')


def generate_remote_batch_controller_log_file_name(service_name: str,
                                                   resource_scope: str |
                                                   None = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'batch_controller.log')


def generate_replica_launch_log_file_name(
        service_name: str,
        replica_id: int,
        resource_scope: str | None = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    dir_name = os.path.expanduser(dir_name)
    return os.path.join(dir_name, f'replica_{replica_id}_launch.log')


def generate_replica_log_file_name(service_name: str,
                                   replica_id: int,
                                   resource_scope: str | None = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    dir_name = os.path.expanduser(dir_name)
    return os.path.join(dir_name, f'replica_{replica_id}.log')


def generate_replica_cluster_name(service_name: str,
                                  replica_id: int,
                                  resource_scope: str | None = None) -> str:
    # NOTE(dev): This format is used in sky/serve/service.py::_cleanup, for
    # checking replica cluster existence. Be careful when changing it.
    if resource_scope is None:
        return f'{service_name}-{replica_id}'
    identity = json.dumps([service_name, resource_scope], separators=(',', ':'))
    scope_tag = _resource_scope_tag(identity, length=10)
    suffix = f'-{replica_id}-{scope_tag}'
    # Keep Kubernetes/cloud-derived names within the common 63-character
    # ceiling even when the user service name itself occupies that budget.
    prefix = service_name[:63 - len(suffix)].rstrip('-')
    if not prefix:
        prefix = 'skyserve'
    return f'{prefix}{suffix}'


def set_service_status_and_active_versions_from_replica(
    service_name: str,
    replica_infos: list['replica_managers.ReplicaInfo'],
    update_mode: UpdateMode,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> None:
    record = serve_state.get_service_controller_owner(service_name,
                                                      require_version=True)
    if record is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'The service is up-ed in an old version and does not '
                'support update. Please `sky serve down` '
                'it first and relaunch the service.')
    record_hash = record.get('hash')
    if (expected_service_hash is not None and
            record_hash != expected_service_hash):
        logger.debug(f'Refusing replica-driven status write from stale '
                     f'incarnation {expected_service_hash!r} for '
                     f'{service_name!r}; current incarnation is '
                     f'{record_hash!r}.')
        return
    record_owner = (record.get('controller_pid'), record.get('controller_ip'))
    if (expected_controller_owner is not None and
            record_owner != expected_controller_owner):
        logger.debug(f'Refusing replica-driven status write from stale '
                     f'controller {expected_controller_owner!r} for '
                     f'{service_name!r}; current controller is '
                     f'{record_owner!r}.')
        return
    observed_status = record['status']
    if observed_status in serve_state.ServiceStatus.terminal_statuses():
        # A controller child can briefly keep probing after its parent has
        # durably entered teardown or marked it failed. Terminal state belongs
        # to the parent/finalizer and must never be healed by replica probes.
        return

    ready_replicas = list(filter(lambda info: info.is_ready, replica_infos))
    if update_mode == UpdateMode.ROLLING:
        active_versions = sorted(
            list(set(info.version for info in ready_replicas)))
    else:
        chosen_version = get_latest_version_with_min_replicas(
            service_name, replica_infos)
        active_versions = [chosen_version] if chosen_version is not None else []
    # Compute the service status from ALL replicas, not just the ready ones:
    # `from_replica_statuses` needs the full set to ever return FAILED (some
    # replica failed, none ready) or REPLICA_INIT (replicas exist, none ready
    # or failed). Fed only ready replicas, it can only return READY or
    # NO_REPLICA, so a service whose replicas all failed would show the
    # benign-looking NO_REPLICA. `active_versions` above intentionally stays
    # on the ready replicas (the versions actually serving traffic).
    service_hash = (expected_service_hash
                    if expected_service_hash is not None else record_hash)
    if not isinstance(service_hash, str) or not service_hash:
        logger.warning(f'Refusing replica-driven status write for '
                       f'{service_name!r} without a durable incarnation.')
        return
    updated = serve_state.set_service_status_and_active_versions_if_owner(
        service_name,
        service_hash, (expected_controller_owner[0] if expected_controller_owner
                       is not None else record.get('controller_pid')),
        (expected_controller_owner[1] if expected_controller_owner is not None
         else record.get('controller_ip')),
        serve_state.ServiceStatus.from_replica_statuses(
            [info.status for info in replica_infos]),
        active_versions=active_versions,
        expected_status=observed_status)
    if not updated:
        logger.debug(f'Skipped stale replica-driven status write for '
                     f'{service_name!r}; owner or status changed.')


def update_service_status(pool: bool) -> None:
    noun = 'pool' if pool else 'serve'
    capnoun = noun.capitalize()
    records = serve_state.get_service_liveness_snapshots(pool=pool)
    terminal_statuses = set(serve_state.ServiceStatus.terminal_statuses())
    for record in records:
        service_name = record['name']
        service_status = record['status']
        if service_status in terminal_statuses:
            # Finalization and recovery own terminal states.  In particular,
            # never erase FAILED_CLEANUP by rewriting it CONTROLLER_FAILED.
            continue

        logger.info(f'Update {noun} status for {service_name!r} '
                    f'with status {service_status}')

        controller_pid = record['controller_pid']
        if controller_pid is None:
            logger.info(f'{capnoun} {service_name!r} controller pid is None. '
                        f'Unexpected status {service_status}. Set to failure.')
        elif controller_pid < 0:
            # Backwards compatibility: this service was submitted when ray was
            # still used for controller process management. We set the
            # value_to_replace_existing_entries to -1 to indicate historical
            # services.
            # TODO(tian): Remove before 0.13.0.
            controller_job_id = record['controller_job_id']
            assert controller_job_id is not None
            controller_status = job_lib.get_status(controller_job_id)
            if (controller_status is not None and
                    not controller_status.is_terminal()):
                continue
            logger.info(f'Updating {noun} {service_name!r} in old version. '
                        f'SkyPilot job status: {controller_status}. '
                        'Set to failure.')
        else:
            if _controller_process_alive(
                    controller_pid,
                    service_name,
                    record.get('hash'),
                    allow_legacy=record.get('resource_scope') is None):
                # The controller is still running.
                continue
            logger.info(f'{capnoun} {service_name!r} controller pid '
                        f'{controller_pid} is not alive. Set to failure.')

        # If controller job is not running, set it as controller failed.
        serve_state.set_service_status_and_active_versions_if_owner(
            service_name,
            record['hash'],
            record['controller_pid'],
            record['controller_ip'],
            serve_state.ServiceStatus.CONTROLLER_FAILED,
            expected_status=service_status)


def update_service_encoded(service_name: str,
                           version: int,
                           mode: str,
                           pool: bool,
                           expected_service_hash: str | None = None,
                           expected_lifecycle_epoch: int | None = None,
                           has_submitted_yaml: bool = False) -> str:
    noun = 'pool' if pool else 'service'
    capnoun = noun.capitalize()
    # Only existence and the incarnation hash are consumed here; skip the
    # YAML render and the fleet-sized replica serialization.
    service_status = _get_service_status(service_name,
                                         pool=pool,
                                         with_replica_info=False,
                                         with_yaml=False,
                                         status_snapshot_only=True)
    if service_status is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'{capnoun} {service_name!r} does not exist.')
    service_hash = service_status['hash']
    if (expected_service_hash is not None and
            service_hash != expected_service_hash):
        raise RuntimeError(f'{capnoun} {service_name!r} was replaced before '
                           'the update was submitted.')
    request_body = {
        'version': version,
        'mode': mode,
    }
    if expected_service_hash is not None:
        request_body['service_hash'] = expected_service_hash
    if expected_lifecycle_epoch is not None:
        request_body['lifecycle_epoch'] = expected_lifecycle_epoch
    if has_submitted_yaml:
        request_body['has_submitted_yaml'] = True
    resp = _post_to_controller_with_retry(
        service_name,
        service_hash,
        '/controller/update_service',
        json=request_body,
        # Keep the compatibility timeout for controllers predating the
        # commit-then-reconcile protocol, whose handler may still wait behind
        # a slow replica-manager probe round.
        timeout=(_CONTROLLER_HTTP_TIMEOUT_SECONDS[0],
                 constants.UPDATE_SERVICE_TIMEOUT_SECONDS))
    if resp.status_code == 404:
        with ux_utils.print_exception_no_traceback():
            # This only happens for services since pool is added after the
            # update feature is introduced.
            raise ValueError(
                'The service is up-ed in an old version and does not '
                'support update. Please `sky serve down` '
                'it first and relaunch the service. ')
    elif resp.status_code == 400:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Client error during {noun} update: {resp.text}')
    elif resp.status_code == 409:
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(f'Stale {noun} update rejected: {resp.text}')
    elif resp.status_code == 500:
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(
                f'Server error during {noun} update: {resp.text}')
    elif resp.status_code != 200:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Failed to update {noun}: {resp.text}')

    service_msg = resp.json()['message']
    return message_utils.encode_payload(service_msg)


def set_load_balancer_high_availability_encoded(
        service_name: str, enabled: bool, expected_service_hash: str,
        expected_lifecycle_epoch: int) -> None:
    """Submit a lifecycle-fenced, LB-only topology transition."""
    service_status = _get_service_status(service_name,
                                         pool=False,
                                         with_replica_info=False,
                                         with_yaml=False,
                                         status_snapshot_only=True)
    if service_status is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Service {service_name!r} does not exist.')
    service_hash = service_status['hash']
    if service_hash != expected_service_hash:
        raise RuntimeError(f'Service {service_name!r} was replaced before '
                           'the load balancer update was submitted.')
    resp = _post_to_controller_with_retry(
        service_name,
        service_hash,
        '/controller/set_load_balancer_high_availability',
        json={
            'enabled': enabled,
            'service_hash': expected_service_hash,
            'lifecycle_epoch': expected_lifecycle_epoch,
        },
        timeout=(_CONTROLLER_HTTP_TIMEOUT_SECONDS[0],
                 constants.UPDATE_SERVICE_TIMEOUT_SECONDS))
    if resp.status_code == 404:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'The service controller does not support load balancer '
                'topology updates. Upgrade the API server and retry.')
    if resp.status_code == 400:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                f'Invalid load balancer topology update: {resp.text}')
    if resp.status_code == 409:
        raise RuntimeError(
            f'Stale load balancer topology update rejected: {resp.text}')
    if resp.status_code == 500:
        raise RuntimeError(f'Load balancer topology update failed: {resp.text}')
    if resp.status_code != 200:
        raise RuntimeError(
            f'Failed to update the load balancer topology: {resp.text}')


def terminate_replica(service_name: str, replica_id: int, purge: bool) -> str:
    # TODO(tian): Currently pool does not support terminating replica.
    # Only existence and the incarnation hash are consumed here; avoid the
    # full service-row read and the fleet-sized replica serialization.
    service_status = _get_service_status(service_name,
                                         pool=False,
                                         with_replica_info=False,
                                         with_yaml=False,
                                         status_snapshot_only=True)
    if service_status is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Service {service_name!r} does not exist.')
    replica_info = serve_state.get_replica_info_from_id(service_name,
                                                        replica_id)
    if replica_info is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                f'Replica {replica_id} for service {service_name} does not '
                'exist.')

    resp = _post_to_controller_with_retry(
        service_name,
        service_status['hash'],
        '/controller/terminate_replica',
        json={
            'replica_id': replica_id,
            'purge': purge,
        },
        timeout=(_CONTROLLER_HTTP_TIMEOUT_SECONDS[0],
                 constants.TERMINATE_REPLICA_TIMEOUT_SECONDS))

    try:
        body = resp.json()
    except ValueError:
        body = {}
    # HTTPException responses (e.g. 400/404 validation errors) use FastAPI's
    # default {'detail': ...} shape; the controller's generic error handler
    # and success responses use {'message': ...}.
    message: str = str(body.get('message') or body.get('detail') or resp.text)
    if resp.status_code != 200:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Failed to terminate replica {replica_id} '
                             f'in {service_name}. Reason:\n{message}')
    return message


def get_yaml_content(service_name: str,
                     version: int,
                     resource_scope: str | None = None) -> str:
    yaml_content = serve_state.get_yaml_content(service_name, version)
    if yaml_content is not None:
        return yaml_content
    # Backward compatibility for old service records that
    # does not dump the yaml content to version database.
    # TODO(tian): Remove this after 2 minor releases, i.e. 0.13.0.
    if resource_scope is None:
        record = serve_state.get_service_from_name(service_name)
        resource_scope = record.get('resource_scope') if record else None
    latest_yaml_path = generate_task_yaml_file_name(
        service_name, version, resource_scope=resource_scope)
    with open(latest_yaml_path, encoding='utf-8') as f:
        return f.read()


def _set_replica_status_aggregates(record: dict[str, Any],
                                   status_counts: dict[str, int],
                                   capacity_counts: dict[str, int]) -> None:
    """Attach public replica capacity and physical backend aggregates."""
    failed_statuses = {
        status.value for status in serve_state.ReplicaStatus.failed_statuses()
    }
    physical_failed = sum(count for status, count in status_counts.items()
                          if status in failed_statuses)
    capacity_failed = sum(count for status, count in capacity_counts.items()
                          if status in failed_statuses)
    logical = bool(record.get('logical_replica_semantics'))
    record.update({
        'replica_unit': ('logical_slot' if logical else 'physical_backend'),
        'ready_replicas': capacity_counts.get(
            serve_state.ReplicaStatus.READY.value, 0),
        'total_replicas': sum(capacity_counts.values()) - capacity_failed,
        'failed_replicas': capacity_failed,
        'physical_ready_replicas': status_counts.get(
            serve_state.ReplicaStatus.READY.value, 0),
        'physical_total_replicas': sum(status_counts.values()) -
                                   physical_failed,
        'physical_failed_replicas': physical_failed,
    })


def _get_service_status(
        service_name: str,
        pool: bool,
        with_replica_info: bool = True,
        with_replica_counts: bool = False,
        with_yaml: bool = True,
        with_target_num_replicas: bool = False,
        status_snapshot_only: bool = False) -> dict[str, Any] | None:
    """Get the status dict of the service.

    Args:
        service_name: The name of the service.
        with_replica_info: Whether to include the information of all replicas.
        with_replica_counts: Whether to include a per-status replica count
            histogram (``replica_status_counts``). Cheaper than
            ``with_replica_info`` but not free (one pass over the replica
            rows), so internal callers that only need the service row
            should leave both off.
        with_yaml: Whether to include the rendered YAML (``pool_yaml`` for
            pools, secret-redacted ``service_yaml`` for services). Liveness
            callers can skip the parse/dump work when they only need
            controller metadata.
        with_target_num_replicas: Whether to fetch autoscaler info
            (``target_num_replicas`` and request stats) from the controller.
            This is an HTTP round-trip to the controller process, so it is
            opt-in: control and liveness paths (HA recovery, termination,
            registration polling) must never block on a possibly-dead
            controller's connect timeout for fields they do not read. Only
            user-facing status rendering should pass True.
        status_snapshot_only: Whether to read only lifecycle fields from the
            services table. Callers must opt in explicitly because some
            YAML-free lifecycle paths still inspect latest-version metadata.

    Returns:
        A dictionary describing the status of the service if the service exists.
        Otherwise, return None.
    """
    if status_snapshot_only:
        if (with_replica_info or with_replica_counts or with_yaml or
                with_target_num_replicas):
            raise ValueError('A status-only snapshot cannot include service '
                             'enrichment.')
        record = serve_state.get_service_status_snapshot(service_name,
                                                         require_version=True)
    else:
        record = serve_state.get_service_from_name(service_name)
    if record is None:
        return None
    if record['pool'] != pool:
        return None

    if record['pool'] and with_yaml:
        record['pool_yaml'] = ''
        version = record['version']
        try:
            yaml_content = get_yaml_content(service_name, version,
                                            record.get('resource_scope'))
            raw_yaml_config = yaml_utils.read_yaml_str(yaml_content)
        except Exception as e:  # pylint: disable=broad-except
            # If this is a consolidation mode running without an PVC, the file
            # might lost after an API server update (restart). In such case, we
            # don't want it to crash the command. Fall back to an empty string.
            logger.error(f'Failed to read YAML for service {service_name} '
                         f'with version {version}: {e}')
            record['pool_yaml'] = ''
        else:
            original_config = raw_yaml_config.get('_user_specified_yaml')
            if original_config is None:
                # Fall back to old display format.
                original_config = raw_yaml_config
                original_config.pop('run', None)
                svc: dict[str, Any] = original_config.pop('service')
                if svc is not None:
                    svc.pop('pool', None)  # Remove pool from service config
                    original_config['pool'] = svc  # Add pool to root config
            else:
                original_config = yaml_utils.safe_load(original_config)
            record['pool_yaml'] = yaml_utils.dump_yaml_str(original_config)
    elif not record['pool'] and with_yaml:
        # Services get a display-only copy of their YAML (e.g. for the
        # dashboard service page). Unlike ``pool_yaml``, which the batch
        # coordinator parses back into a launchable task, this is for
        # humans, so secrets are redacted.
        record['service_yaml'] = ''
        version = record['version']
        try:
            # The latest-version join already fetched the YAML; only fall
            # back to a storage read for old rows that predate YAML-in-DB.
            stored_yaml = record.get('yaml_content')
            if stored_yaml is None:
                stored_yaml = get_yaml_content(service_name, version,
                                               record.get('resource_scope'))
            raw_yaml_config = yaml_utils.read_yaml_str(stored_yaml)
            original_yaml = raw_yaml_config.get('_user_specified_yaml')
            if original_yaml is None:
                # Old records without the embedded user YAML: show the
                # rendered task YAML instead.
                original_yaml = stored_yaml
            record['service_yaml'] = debug_dump_helpers.redact_task_yaml(
                str(original_yaml))
        except Exception as e:  # pylint: disable=broad-except
            # Same rationale as the pool branch: a lost or malformed YAML
            # must not fail the whole status query.
            logger.error(f'Failed to read YAML for service {service_name} '
                         f'with version {version}: {e}')
            record['service_yaml'] = ''

    if with_target_num_replicas:
        record['target_num_replicas'] = 0
        try:
            resp = _get_to_controller_with_retry(service_name, record['hash'],
                                                 '/autoscaler/info')
            autoscaler_info = resp.json()
            record['target_num_replicas'] = autoscaler_info[
                'target_num_replicas']
            request_field_map = {
                'min_replicas_by_accelerator': 'min_replicas_by_accelerator',
                'target_num_replicas_by_accelerator': 'target_num_replicas_by_accelerator',
                'demand_target_by_accelerator': 'demand_target_by_accelerator',
                'warm_retention_target_by_accelerator': 'warm_retention_target_by_accelerator',
                'cold_launch_authority_by_accelerator': 'cold_launch_authority_by_accelerator',
                'ready_replicas_by_accelerator': 'ready_replicas_by_accelerator',
                'provisioning_replicas_by_accelerator': 'provisioning_replicas_by_accelerator',
                'total_replicas_by_accelerator': 'total_replicas_by_accelerator',
                'zero_cost_ready_replicas_by_accelerator': 'zero_cost_ready_replicas_by_accelerator',
                'fill_target_by_accelerator': 'fill_target_by_accelerator',
                'free_reserved_slots_by_accelerator': 'free_reserved_slots_by_accelerator',
                'fill_target': 'fill_target',
                'fill_free_slots': 'fill_free_slots',
                'recent_request_count': 'recent_request_count',
                'request_window_seconds': 'request_window_seconds',
                'requests_per_second': 'requests_per_second',
                'observed_ready_replicas': 'ready_replicas',
                'in_flight_requests': 'in_flight_total',
                'request_queue_depth': 'queue_depth',
                'rejected_requests': 'rejected_in_window',
                'recent_rejected_requests': 'rejected_in_recent_window',
                'rejected_concurrency': 'rejected_concurrency',
                'raw_target_num_replicas': 'raw_target_num_replicas',
                'committed_capacity': 'committed_capacity',
                'target_utilization_percentage': 'target_utilization_percentage',
                'latest_scale_up_wave_at': 'latest_scale_up_wave_at',
                'request_stats_age_seconds': 'report_age_seconds',
                'committed_version': 'committed_version',
                'applied_version': 'applied_version',
                'update_apply_pending': 'update_apply_pending',
                'update_apply_lag_seconds': 'update_apply_lag_seconds',
                'update_apply_error': 'update_apply_error',
                'update_apply_failures': 'update_apply_failures',
                'quarantined_version': 'quarantined_version',
                'quarantined_at': 'quarantined_at',
                'quarantine_reason': 'quarantine_reason',
            }
            for record_field, autoscaler_field in request_field_map.items():
                if autoscaler_field in autoscaler_info:
                    record[record_field] = autoscaler_info[autoscaler_field]
        except requests.exceptions.RequestException:
            record['target_num_replicas'] = None
        except Exception as e:  # pylint: disable=broad-except
            record['target_num_replicas'] = None
            logger.error(f'Failed to get autoscaler info for {service_name}: '
                         f'{common_utils.format_exception(e)}\n'
                         f'Traceback: {traceback.format_exc()}')

    if with_replica_counts and not with_replica_info:
        # Summary mode: give callers (the dashboard header, list views)
        # enough to render without the expensive per-replica
        # `to_info_dict` serialization below. Physical services use one
        # indexed GROUP BY. Logical services scan only the compact JSON state
        # needed for persisted widths; neither path resolves cluster records,
        # endpoints, handles, or cloud/cluster APIs.
        logical = bool(record.get('logical_replica_semantics'))
        if logical:
            status_counts, capacity_counts = (
                serve_state.get_replica_status_and_capacity_counts(service_name)
            )
        else:
            status_counts = serve_state.get_replica_status_counts(service_name)
            capacity_counts = status_counts
        record['replica_status_counts'] = status_counts
        _set_replica_status_aggregates(record, status_counts, capacity_counts)

    if with_replica_info:
        replica_infos = serve_state.get_replica_infos(service_name)
        full_status_counts: collections.defaultdict[str, int] = (
            collections.defaultdict(int))
        full_capacity_counts: collections.defaultdict[str, int] = (
            collections.defaultdict(int))
        logical = bool(record.get('logical_replica_semantics'))
        for info in replica_infos:
            status = info.status.value
            full_status_counts[status] += 1
            planned_capacity = getattr(info, 'planned_capacity', 1)
            if (not isinstance(planned_capacity, int) or
                    isinstance(planned_capacity, bool) or planned_capacity < 1):
                planned_capacity = 1
            full_capacity_counts[status] += planned_capacity if logical else 1
        _set_replica_status_aggregates(record, dict(full_status_counts),
                                       dict(full_capacity_counts))
        # Pre-fetch cluster records in one batched DB query instead of
        # letting each to_info_dict() do its own. With a long failure
        # history this was an N+1.
        cluster_names = [info.cluster_name for info in replica_infos]
        cluster_records = global_user_state.get_clusters_from_names(
            cluster_names)
        rate_cache: dict[str, float] = {}
        record['replica_info'] = [
            info.to_info_dict(
                with_handle=True,
                with_url=not pool,
                cluster_record=cluster_records[info.cluster_name],
                rate_cache=rate_cache,
            ) for info in replica_infos
        ]
        if pool:
            record['job_status_counts'] = (
                managed_job_state.get_nonterminal_job_status_counts_by_pool(
                    service_name))
            # Fetch all nonterminal job ids in the pool in a single query,
            # grouped by current_cluster_name. Avoids the N+1 pattern of
            # (1 + len(replicas)) per-pool queries against a job_info table
            # that may contain tens of thousands of finished rows.
            jobs_by_cluster = (
                managed_job_state.get_nonterminal_job_ids_by_pool_grouped(
                    service_name))
            # Pool-level jobs (e.g. batch coordinators) span every worker.
            # They have pool set but no cluster_name, so they live under the
            # None bucket of the grouped result. Note: the prior per-call
            # implementation passed cluster_name=None to a function that
            # treated None as "no filter" rather than "IS NULL", so it
            # accidentally returned every nonterminal job in the pool and
            # surfaced unrelated replicas' jobs as `used_by` on each READY
            # worker. The grouped query lets us implement the intended
            # semantic exactly.
            pool_level_job_ids = list(jobs_by_cluster.get(None, []))
            for replica_info in record['replica_info']:
                job_ids = list(jobs_by_cluster.get(replica_info['name'], []))
                # Show pool-level jobs on READY workers only.
                if (replica_info.get('status') ==
                        serve_state.ReplicaStatus.READY):
                    job_ids = list(dict.fromkeys(pool_level_job_ids + job_ids))
                replica_info['used_by'] = job_ids
    observed_ready = record.get('observed_ready_replicas')
    if ('ready_replicas' in record and isinstance(observed_ready, int) and
            not isinstance(observed_ready, bool) and observed_ready >= 0):
        record['ready_replicas'] = observed_ready
    return record


def resolve_target_qps_for_gpu_shape(
        gpu_type: str, gpu_count: int,
        target_qps_per_replica: dict[str, float]) -> float | None:
    """Per-REPLICA target QPS for a replica with `gpu_count` x `gpu_type`.

    Key semantics (backward compatible with the count-blind matcher):
      1. An exact shape key ('L4:4') is a per-replica value, used as-is.
      2. A bare type key ('L4') is a per-GPU value, multiplied by the
         replica's GPU count.
      3. A count-suffixed key of the same type but different count
         ('L4:1' for an L4:8 replica) is normalized to per-GPU
         (value / key count) and multiplied by the replica's count.
    Returns None when nothing matches (caller picks its fallback).

    NOTE: the per-GPU semantics of (2) and (3) assume ONE model instance
    per GPU (each server pinned to a distinct device). For a model that
    needs k GPUs per instance, per-GPU scaling would overcount capacity
    by k: declare an exact per-replica shape key instead, e.g. a 2-GPU
    model on L4:8 machines serving 4 instances at 0.1 qps each ->
    {'L4:8': 0.4}.
    """
    exact_key = f'{gpu_type}:{gpu_count}'
    if exact_key in target_qps_per_replica:
        return target_qps_per_replica[exact_key]
    if gpu_type in target_qps_per_replica:
        return target_qps_per_replica[gpu_type] * gpu_count
    for key, value in target_qps_per_replica.items():
        base, _, count_str = key.partition(':')
        if base == gpu_type and count_str.isdigit() and int(count_str) > 0:
            return value / int(count_str) * gpu_count
    return None


def get_service_status_pickled(
        service_names: list[str] | None,
        pool: bool,
        summary_only: bool = False,
        include_target_num_replicas: bool | None = None
) -> list[dict[str, str]]:
    if service_names is None:
        # Get all names for the requested mode only.
        service_names = serve_state.get_glob_service_names(None, pool=pool)
    if not service_names:
        return []
    if include_target_num_replicas is None:
        include_target_num_replicas = not summary_only
    # Fan out across services. Each `_get_service_status` is dominated by
    # I/O (controller HTTP + DB reads) so threads parallelize well; the
    # cap on max_workers keeps memory and DB-connection pressure bounded.
    # Each task gets a fresh `Context.copy()` because the same Context
    # can't be entered from multiple threads (Context.run raises
    # RuntimeError otherwise) — but the values (request_id / user_id)
    # are inherited so log redirection still works inside workers.
    # `ex.map` preserves the existing failure contract (first failure
    # aborts the whole call).
    parent_ctx = contextvars.copy_context()

    def _run_in_context(name: str) -> dict[str, Any] | None:
        kwargs = {
            'pool': pool,
            'with_replica_info': not summary_only,
            'with_replica_counts': summary_only,
            'with_target_num_replicas': include_target_num_replicas,
        }
        # Service summaries are metadata-only dashboard snapshots. Avoid
        # parsing, redacting, and dumping one YAML document per service on
        # every poll. Pool summaries deliberately keep YAML because pool
        # lifecycle consumers parse it back into a launchable task.
        if summary_only and not pool:
            kwargs['with_yaml'] = False
        return parent_ctx.copy().run(_get_service_status, name, **kwargs)

    max_workers = min(len(service_names), _STATUS_FANOUT_MAX_WORKERS)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        statuses = list(ex.map(_run_in_context, service_names))
    live_statuses = sorted(
        (status for status in statuses if status is not None),
        key=lambda status: status['name'])
    for status in live_statuses:
        # The rendered YAML carries plaintext secrets (replicas need it
        # launchable), so it never leaves the controller for services:
        # clients get the redacted `service_yaml` instead. Pools keep it
        # because the batch coordinator and worker-count updates parse
        # `yaml_content`/`pool_yaml` back into a launchable task.
        if not status.get('pool'):
            status.pop('yaml_content', None)
    return [{
        k: base64.b64encode(pickle.dumps(v)).decode('utf-8')
        for k, v in s.items()
    }
            for s in live_statuses]


# TODO (kyuds): remove when serve codegen is removed
def get_service_status_encoded(
        service_names: list[str] | None,
        pool: bool,
        summary_only: bool = False,
        include_target_num_replicas: bool | None = None) -> str:
    # We have to use payload_type here to avoid the issue of
    # message_utils.decode_payload() not being able to correctly decode the
    # message with <sky-payload> tags.
    service_statuses = get_service_status_pickled(
        service_names,
        pool,
        summary_only=summary_only,
        include_target_num_replicas=include_target_num_replicas)
    return message_utils.encode_payload(service_statuses,
                                        payload_type='service_status')


def unpickle_service_status(
        payload: list[dict[str, str]]) -> list[dict[str, Any]]:
    service_statuses: list[dict[str, Any]] = []
    for service_status in payload:
        if not isinstance(service_status, dict):
            raise ValueError(f'Invalid service status: {service_status}')
        service_statuses.append({
            k: pickle.loads(base64.b64decode(v))
            for k, v in service_status.items()
        })
    return service_statuses


# TODO (kyuds): remove when serve codegen is removed
def load_service_status(payload: str) -> list[dict[str, Any]]:
    try:
        service_statuses_encoded = message_utils.decode_payload(
            payload, payload_type='service_status')
    except ValueError as e:
        if 'Invalid payload string' in str(e):
            # Backward compatibility for serve controller started before #4660
            # where the payload type is not added.
            service_statuses_encoded = message_utils.decode_payload(payload)
        else:
            raise
    return unpickle_service_status(service_statuses_encoded)


# TODO (kyuds): remove when serve codegen is removed
def add_version_encoded(service_name: str) -> str:
    new_version = serve_state.add_version(service_name)
    return message_utils.encode_payload(new_version)


# TODO (kyuds): remove when serve codegen is removed
def load_version_string(payload: str) -> str:
    return message_utils.decode_payload(payload)


def get_ready_replicas(
    service_name: str,
    replicas: list['replica_managers.ReplicaInfo'] | None = None
) -> list['replica_managers.ReplicaInfo']:
    logger.info(f'Get number of replicas for pool {service_name!r}')
    if replicas is None:
        replicas = serve_state.get_replica_infos(service_name)
    return [
        info for info in replicas
        if info.status == serve_state.ReplicaStatus.READY
    ]


def _get_pool_cluster_records(
    replicas: list['replica_managers.ReplicaInfo']
) -> dict[str, dict[str, Any] | None]:
    return global_user_state.get_clusters_from_names(
        [replica_info.cluster_name for replica_info in replicas])


def _task_fits(task_resources: 'resources_lib.Resources',
               free_resources: 'resources_lib.Resources') -> bool:
    """Check if the task resources fit in the free resources."""
    if not task_resources.less_demanding_than(free_resources,
                                              check_cloud=False):
        return False
    if task_resources.cpus is not None:
        if (free_resources.cpus is None or
                task_resources.cpus > free_resources.cpus):
            return False
    if task_resources.memory is not None:
        if (free_resources.memory is None or
                task_resources.memory > free_resources.memory):
            return False
    return True


def _is_empty_resource(resource: 'resources_lib.Resources') -> bool:
    # Returns True if this resource object does not specify any resources.
    return (resource.cpus is None and resource.memory is None and
            resource.accelerators is None)


def get_free_worker_resources(
    pool: str,
    replicas: list['replica_managers.ReplicaInfo'] | None = None,
    cluster_records: dict[str, dict[str, Any] | None] | None = None,
    resolved_handles: dict[str, WorkerHandle] | None = None,
) -> dict[str, resources_lib.Resources | None] | None:
    """Get free resources for each worker in a pool.

    Args:
        pool: Pool name (service name)
        replicas: Optional replica snapshot to reuse; fetched from the state
            store when not provided.
        cluster_records: Optional cluster-table snapshot keyed by worker name.
            When provided, the free-resource walk reuses it instead of issuing
            a second batched cluster read.
        resolved_handles: Optional output mapping keyed by worker name. When
            provided, each worker's resolved handle is stored for reuse by the
            caller's post-selection bookkeeping.

    Returns:
        Dictionary mapping cluster_name (worker) to free Resources object (or
        None if worker is not available or has no free resources).
    """

    free_resources: dict[str, resources_lib.Resources | None] = {}
    if replicas is None:
        replicas = serve_state.get_replica_infos(pool)
    used_resources_by_cluster = (
        managed_job_state.get_pool_worker_used_resources_by_cluster(pool))
    if used_resources_by_cluster is None:
        logger.warning('Failed to get used resources for pool '
                       f'{pool!r}; disabling resource-aware scheduling')
        return None

    # Snapshot every worker's cluster record in one batched read; the
    # per-replica ``handle()`` fallback would issue one cluster-table read
    # per worker on every scheduling attempt.
    if cluster_records is None:
        cluster_records = _get_pool_cluster_records(replicas)
    for replica_info in replicas:
        cluster_name = replica_info.cluster_name

        # Get cluster handle
        cluster_record = cluster_records.get(cluster_name)
        handle = (None if cluster_record is None else
                  replica_info.handle(cluster_record))
        if resolved_handles is not None:
            resolved_handles[cluster_name] = handle
        if handle is None or handle.launched_resources is None:
            free_resources[cluster_name] = None
            continue

        total_resources = handle.launched_resources

        used_resources = used_resources_by_cluster.get(cluster_name)
        if used_resources is None:
            free_resources[cluster_name] = total_resources
            continue

        if _is_empty_resource(used_resources):
            # At least one job on this worker has no explicit resource request.
            # Treat the worker as fully occupied for resource-aware placement.
            logger.debug('Some jobs on cluster '
                         f'{cluster_name!r} have no resources specified. '
                         'Skipping resource-aware scheduling for cluster '
                         f'{cluster_name!r}')
            free_resources[cluster_name] = resources_lib.Resources()
        else:
            # Calculate free resources using - operator
            free = total_resources - used_resources
            free_resources[cluster_name] = free

    return free_resources


def get_next_cluster_name(
    service_name: str,
    job_id: int,
    task_resources: typing.Union['resources_lib.Resources',
                                 set['resources_lib.Resources'],
                                 list['resources_lib.Resources']] | None = None
) -> str | None:
    """Get the next available cluster name from replicas with sufficient
    resources.

    Args:
        service_name: The name of the service.
        job_id: Job ID to associate with the acquired cluster.
        task_resources: Optional task resource requirements. If provided, will
                check if resources fit in free worker resources. Can be
                a single Resources object or a set/list of Resources objects.

    Returns:
        The cluster name if a suitable replica is found, None otherwise.
    """
    # Check if service exists
    service_status = _get_service_status(service_name,
                                         pool=True,
                                         with_replica_info=False,
                                         with_yaml=False,
                                         status_snapshot_only=True)
    if service_status is None:
        logger.error(f'Service {service_name!r} does not exist.')
        return None
    if not service_status['pool']:
        logger.error(f'Service {service_name!r} is not a pool.')
        return None

    with filelock.FileLock(get_service_filelock_path(service_name)):
        logger.debug(f'Get next cluster name for pool {service_name!r}')
        # Read the replica set once and share the snapshot between readiness
        # filtering and free-resource accounting so the scheduling decision
        # is made against a single consistent view of the fleet.
        replicas = serve_state.get_replica_infos(service_name)
        ready_replicas = get_ready_replicas(service_name, replicas=replicas)

        logger.debug(f'Ready replicas: {ready_replicas!r}')

        idle_replicas: list[replica_managers.ReplicaInfo] = []
        # cluster_name -> the task resource option that fit on that worker.
        chosen_resources: dict[str, resources_lib.Resources] = {}

        # If task_resources is provided, use resource-aware scheduling
        # Normalize task_resources to a list
        if isinstance(task_resources, resources_lib.Resources):
            task_resources_list = [task_resources]
        elif isinstance(task_resources, (set, list)):
            task_resources_list = list(task_resources)
        else:
            task_resources_list = []

        # We should do resource aware scheduling if:
        # 1. There are task resources.
        # 2. The first task resource has some resources listed.
        # 3. There are free resources.
        # 4. Any free resource has some resources listed.
        resource_aware = len(task_resources_list) > 0
        resource_aware = (resource_aware and
                          not _is_empty_resource(task_resources_list[0]))

        free_resources = None
        cluster_records = None
        resolved_handles: dict[str, WorkerHandle] | None = None
        if resource_aware:
            cluster_records = _get_pool_cluster_records(replicas)
            resolved_handles = {}
            free_resources = get_free_worker_resources(
                service_name,
                replicas=replicas,
                cluster_records=cluster_records,
                resolved_handles=resolved_handles)
            logger.debug(f'Free resources: {free_resources!r}')
            resource_aware = free_resources is not None
        if resource_aware and free_resources is not None:
            for free_resource in free_resources.values():
                if free_resource is not None and not _is_empty_resource(
                        free_resource):
                    break
            else:
                resource_aware = False

        if resource_aware:
            logger.debug('Doing resource aware scheduling')
            for replica_info in ready_replicas:
                cluster_name = replica_info.cluster_name
                assert free_resources is not None
                free_resources_on_worker = free_resources.get(cluster_name)
                logger.debug(f'Free resources for cluster {cluster_name!r}: '
                             f'{free_resources_on_worker!r}')

                # Skip if worker has no free resources available
                if free_resources_on_worker is None:
                    logger.debug(f'Worker {cluster_name!r} has no free '
                                 'resources')
                    continue

                # Check if any of the task resource options fit, remembering
                # which option fit so the selection below does not have to
                # recompute it.
                for task_res in task_resources_list:
                    logger.debug(f'Task resources: {task_res!r}')
                    if _task_fits(task_res, free_resources_on_worker):
                        logger.debug(f'Task resources {task_res!r} fits'
                                     ' in free resources '
                                     f'{free_resources_on_worker!r}')
                        chosen_resources[cluster_name] = task_res
                        idle_replicas.append(replica_info)
                        break
                    else:
                        logger.debug(f'Task resources {task_res!r} does not fit'
                                     ' in free resources '
                                     f'{free_resources_on_worker!r}')
        # Also fall back to resource unaware scheduling if no idle replicas are
        # found. This might be because our launched resources were improperly
        # set. If that's the case then jobs will fail to schedule in a resource
        # aware way because one of the resources will be `None` so we can just
        # fallback to 1 job per replica. If we are truly resource bottlenecked
        # then we will see that there are jobs running on the replica and will
        # not schedule another.
        if len(idle_replicas) == 0:
            logger.debug('Falling back to resource unaware scheduling')
            jobs_per_replica = (
                managed_job_state.get_nonterminal_job_counts_by_pool(
                    service_name))
            # Fall back to resource unaware scheduling if no task resources
            # are provided.
            for replica_info in ready_replicas:
                if jobs_per_replica.get(replica_info.cluster_name, 0) == 0:
                    idle_replicas.append(replica_info)

        if not idle_replicas:
            logger.info(f'No idle replicas found for pool {service_name!r}')
            return None

        # Select the first idle replica.
        replica_info = idle_replicas[0]
        logger.info(f'Selected replica {replica_info.replica_id} with cluster '
                    f'{replica_info.cluster_name!r} for job {job_id!r} in pool '
                    f'{service_name!r}')

        # If job has heterogeneous resources (any_of/ordered), update
        # full_resources to the specific resource that was selected for this
        # worker. This must happen before releasing the filelock to ensure
        # atomicity with the scheduling decision.
        if resource_aware and len(task_resources_list) > 1:
            chosen_res = chosen_resources.get(replica_info.cluster_name)
            if chosen_res is not None:
                logger.debug(f'Updating full_resources for job {job_id!r} '
                             f'to selected resource: {chosen_res!r}')
                managed_job_state.update_job_full_resources(
                    job_id, chosen_res.to_yaml_config())

        managed_job_state.set_current_cluster_name(job_id,
                                                   replica_info.cluster_name)

        # Set infrastructure info for sorting/filtering
        if cluster_records is None:
            handle = replica_info.handle()
        else:
            handle = None
            if resolved_handles is not None:
                handle = resolved_handles.get(replica_info.cluster_name)
            if handle is None:
                cluster_record = cluster_records.get(replica_info.cluster_name)
                handle = (None if cluster_record is None else
                          replica_info.handle(cluster_record))
        if handle is not None and handle.launched_resources is not None:
            lr = handle.launched_resources
            managed_job_state.set_job_infra(
                job_id,
                cloud=str(lr.cloud) if lr.cloud is not None else None,
                region=lr.region,
                zone=lr.zone,
            )

        return replica_info.cluster_name


def _purge_ownership_failure(service_name: str, detail: str) -> str:
    return (f'{colorama.Fore.YELLOW}failed service {service_name!r} could not '
            'be purged because its service incarnation changed during '
            f'teardown ({detail}); retry against the current service state.'
            f'{colorama.Style.RESET_ALL}')


def quiesce_service_replica_launch_requests(
    service_name: str,
    replica_infos: list['replica_managers.ReplicaInfo'],
    continue_guard: Callable[[], bool] | None = None,
) -> bool:
    """Cancel and await every active launch backed by replica inventory.

    ``sdk.api_cancel`` only schedules a cancellation request.  Teardown may
    remove replica/service rows only after that cancellation request itself
    has completed and a fresh status query proves that no launch request for
    any incarnation-scoped replica cluster remains active.  The caller must
    first stop the controller child (or receive its teardown acknowledgement),
    so no producer can enqueue a new launch after this barrier begins.

    Returns False on any transport/status/ownership uncertainty.  Callers then
    retain the durable service and replica rows for a later retry.
    """

    def _guard_allows() -> bool:
        if continue_guard is None:
            return True
        try:
            return continue_guard()
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to verify service ownership while '
                           'quiescing replica launches: '
                           f'{common_utils.format_exception(e)}')
            return False

    launch_request_name = (server_constants.REQUEST_NAME_PREFIX +
                           request_names.RequestName.CLUSTER_LAUNCH.value)
    cluster_names = {info.cluster_name for info in replica_infos}

    def _active_launch_request_ids() -> set[str]:
        if not cluster_names:
            return set()
        # The service is already durably terminal, so the controller cannot
        # schedule more launches. A launch request racing this snapshot is
        # caught by the next cancellation round. Return only the three small
        # fields needed for that convergence proof. This is bounded by the
        # API's active queue rather than by retained replica history (2,159
        # stale rows previously meant 2,159 HTTP requests per round).
        active_requests = sdk.api_status(
            all_status=False, fields=['request_id', 'name', 'cluster_name'])
        return {
            request.request_id
            for request in active_requests
            if request.name == launch_request_name and
            request.cluster_name in cluster_names
        }

    try:
        # A completed cancellation request makes the target terminal before it
        # returns. The caller has already published SHUTTING_DOWN, and both the
        # scheduler precondition and persisted execution entrypoint reject any
        # launch row that appears after this scan.
        cancel_rounds = 0
        while True:
            if not _guard_allows():
                return False
            active_request_ids = _active_launch_request_ids()
            if not active_request_ids:
                return True

            if cancel_rounds >= _LAUNCH_QUIESCE_MAX_CANCEL_ROUNDS:
                logger.error('Replica launch requests remained active after '
                             f'cancellation for {service_name!r}: '
                             f'{sorted(active_request_ids)}')
                return False
            cancel_request_id = sdk.api_cancel(sorted(active_request_ids),
                                               all_users=True,
                                               silent=True)
            sdk.stream_and_get(cancel_request_id)
            cancel_rounds += 1
    except Exception as e:  # pylint: disable=broad-except
        logger.error('Failed to quiesce replica launch requests for '
                     f'{service_name!r}: '
                     f'{common_utils.format_exception(e)}')
        return False


def get_existing_replica_cluster_names(
    replica_infos: list['replica_managers.ReplicaInfo'],) -> set[str]:
    """Return one batched snapshot of replica names in the cluster table."""
    cluster_names = list(
        dict.fromkeys(info.cluster_name for info in replica_infos))
    if not cluster_names:
        return set()
    return set(
        global_user_state.get_cluster_status_fields(cluster_names).keys())


def _terminate_failed_services(service_name: str,
                               expected_service_hash: str | None,
                               service_status: serve_state.ServiceStatus | None,
                               pool: bool = False) -> _PurgeResult:
    """Terminate service in failed status.

    Failed-status services may still have a parent or recovering controller,
    so a file signal alone is not authoritative. Claim durable SHUTTING_DOWN,
    wait for an exact-owner child-teardown acknowledgement (or atomically claim
    a proven orphan), then terminate any remaining replicas and conditionally
    remove the exact service incarnation. Clusters that fail to terminate are
    reported as a potential resource leak.

    Returns:
        A structured completion result and optional failure message.
    """
    if not expected_service_hash:
        return _PurgeResult(
            False,
            _purge_ownership_failure(service_name,
                                     'missing durable service hash'))
    lifecycle_lock = get_service_lifecycle_lock(service_name)
    # Kept in the outer helper's compatibility signature for existing callers;
    # cleanup behavior is now fully determined by durable DB state.
    del service_status
    with lifecycle_lock:
        message = _terminate_failed_services_locked(service_name,
                                                    expected_service_hash, pool,
                                                    lifecycle_lock)
    return _PurgeResult(message is None, message)


def _terminate_failed_services_locked(
        service_name: str, expected_service_hash: str, pool: bool,
        lifecycle_lock: ServiceLifecycleLock) -> str | None:
    """Locked implementation of failed-service purge."""

    def _still_owns() -> bool:
        return (lifecycle_lock_is_valid(lifecycle_lock) and
                serve_state.service_owner_matches(service_name,
                                                  expected_service_hash))

    lifecycle_epoch = get_service_lifecycle_epoch(lifecycle_lock)

    if not _still_owns():
        return _purge_ownership_failure(service_name,
                                        'ownership lost before cleanup')
    if not serve_state.set_service_status_and_active_versions_if_hash(
            service_name,
            expected_service_hash,
            serve_state.ServiceStatus.SHUTTING_DOWN,
            expected_lifecycle_epoch=lifecycle_epoch):
        return _purge_ownership_failure(
            service_name, 'could not claim durable teardown state')

    # A CONTROLLER_FAILED row may still have a live parent/child (for example,
    # a transient LB failure). The parent polls durable SHUTTING_DOWN at the
    # top of every tick, kills and joins its child, then clears controller_port
    # before waiting for this same lifecycle lock. Do not down name-reused
    # replica clusters until that acknowledgement arrives.
    owner = serve_state.get_service_controller_owner(service_name,
                                                     include_lb_state=True)
    if owner is None or owner.get('hash') != expected_service_hash:
        return _purge_ownership_failure(service_name,
                                        'owner disappeared before teardown')
    resource_scope = owner.get('resource_scope')
    high_availability = bool(owner.get('lb_ha_enabled'))
    if owner.get('controller_port') != constants.CONTROLLER_TEARDOWN_ACK_PORT:
        recovery_script = serve_state.get_ha_recovery_script(service_name)
        if recovery_script is None:
            # Legacy orphan/FAILED_CLEANUP rows may have no parent left to
            # write the new acknowledgement. Absence of the recovery script
            # is durable proof that no controller can be (re)spawned.
            claimed = serve_state.claim_orphaned_service_teardown(
                service_name,
                expected_service_hash,
                owner.get('controller_pid'),
                owner.get('controller_ip'),
                os.getpid(),
                os.environ.get('POD_IP'),
                expected_lifecycle_epoch=lifecycle_epoch)
        elif serve_state.get_latest_committed_version(service_name) is None:
            # Old partial-registration rows can retain a recovery script but
            # no committed yaml. Such a script can never boot a controller;
            # atomically consume it while claiming teardown so purge does not
            # wait forever for an impossible acknowledgement.
            claimed = serve_state.claim_unrecoverable_service_teardown(
                service_name,
                expected_service_hash,
                owner.get('controller_pid'),
                owner.get('controller_ip'),
                os.getpid(),
                os.environ.get('POD_IP'),
                expected_lifecycle_epoch=lifecycle_epoch)
        else:
            claimed = None
        if claimed is False:
            return _purge_ownership_failure(
                service_name, 'orphan teardown claim lost ownership')

    owner_ack_deadline = time.monotonic() + 10
    while True:
        owner = serve_state.get_service_controller_owner(service_name)
        if owner is None or owner.get('hash') != expected_service_hash:
            return _purge_ownership_failure(
                service_name, 'ownership changed while awaiting controller')
        if (owner.get('controller_port') ==
                constants.CONTROLLER_TEARDOWN_ACK_PORT):
            break
        remaining = owner_ack_deadline - time.monotonic()
        if remaining <= 0:
            return (f'{colorama.Fore.YELLOW}failed service '
                    f'{service_name!r} could not be purged because its '
                    'controller has not yet acknowledged durable teardown; '
                    'cleanup remains scheduled and can be retried.'
                    f'{colorama.Style.RESET_ALL}')
        time.sleep(min(0.2, remaining))

    if not _still_owns():
        return _purge_ownership_failure(
            service_name, 'lifecycle lock or ownership lost after controller '
            'acknowledgement')

    replica_infos = serve_state.get_replica_infos(service_name)
    if not quiesce_service_replica_launch_requests(
            service_name, replica_infos, continue_guard=_still_owns):
        return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                'could not be purged because its replica launch requests '
                'could not be quiesced; durable cleanup inventory was '
                f'retained for retry.{colorama.Style.RESET_ALL}')

    # Fence the public data plane before *any* replica teardown.  The LB keeps
    # its last coherent routing view when controller sync stops, so reversing
    # this order accepts requests for clusters already being destroyed.  A
    # failed delete retains the exact row/name and aborts all cloud teardown.
    from sky.serve import lb_k8s  # pylint: disable=import-outside-toplevel
    if not pool:
        try:
            api_deployment_uid = lb_k8s.get_api_deployment_owner_uid(
                require_runtime=True)
            if resource_scope is None:
                lb_k8s.delete_lb_objects(
                    service_name,
                    expected_service_hash=expected_service_hash,
                    require_runtime=True,
                    expected_api_deployment_uid=api_deployment_uid,
                    high_availability=high_availability)
            else:
                lb_k8s.delete_lb_objects(
                    service_name,
                    expected_service_hash=expected_service_hash,
                    resource_scope=resource_scope,
                    require_runtime=True,
                    expected_api_deployment_uid=api_deployment_uid,
                    high_availability=high_availability)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                f'Failed to delete external LB objects for failed service '
                f'{service_name!r}; retaining purge state for retry: '
                f'{common_utils.format_exception(e)}')
            return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                    'could not be purged because its external load balancer '
                    'could not be deleted; retry purge after fixing '
                    f'Kubernetes access.{colorama.Style.RESET_ALL}')

    if not _still_owns():
        return _purge_ownership_failure(
            service_name, 'ownership lost after load balancer cleanup')

    remaining_replica_clusters: list[str] = []
    # The controller is dead (CONTROLLER_FAILED / FAILED_CLEANUP / zombie
    # SHUTTING_DOWN), so no down thread will ever run for these replicas:
    # terminate their clusters here, BEFORE dropping the DB rows. Deleting
    # the rows first (the old behavior) permanently orphaned any cluster
    # that still existed -- nothing referenced it anymore, so it kept
    # billing until manually downed.
    try:
        existing_cluster_names = get_existing_replica_cluster_names(
            replica_infos)
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Failed to prove replica cluster inventory for failed '
                     f'service {service_name!r}: '
                     f'{common_utils.format_exception(e)}')
        return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                'could not be purged because its replica cluster inventory '
                'could not be verified; durable cleanup inventory was '
                f'retained for retry.{colorama.Style.RESET_ALL}')
    if not _still_owns():
        return _purge_ownership_failure(
            service_name, 'ownership lost after cluster inventory snapshot')
    to_terminate = [
        info for info in replica_infos
        if info.cluster_name in existing_cluster_names
    ]
    if to_terminate:
        # Imported here to break the circular dependency: replica_managers
        # imports serve_utils at module load.
        # pylint: disable=import-outside-toplevel
        from sky.serve import replica_managers

        # DistributedLock's PostgreSQL liveness probe uses the lock-owning
        # connection.  Multiple replica workers must not use that connection
        # concurrently: psycopg connections/cursors are not a thread-safe
        # ownership oracle.  Serialize guard probes while keeping the actual
        # cluster termination requests parallel.
        ownership_guard_lock = threading.Lock()

        def _worker_still_owns() -> bool:
            with ownership_guard_lock:
                return _still_owns()

        def _terminate_replica_cluster(
                info: 'replica_managers.ReplicaInfo') -> str | None:
            # Reuse the normal replica down path (sdk.down with retries);
            # logs go to the replica's log file like a regular teardown.
            log_file_name = generate_replica_log_file_name(
                service_name, info.replica_id, resource_scope)
            try:
                replica_managers.terminate_cluster(
                    info.cluster_name,
                    log_file_name,
                    continue_guard=(_worker_still_owns))
                return None
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'Failed to terminate replica cluster '
                             f'{info.cluster_name} of failed service '
                             f'{service_name!r}: '
                             f'{common_utils.format_exception(e)}')
                return info.cluster_name

        termination_failures = subprocess_utils.run_in_parallel(
            _terminate_replica_cluster, to_terminate)
        remaining_replica_clusters = [
            f'{name!r}' for name in termination_failures if name is not None
        ]

    if not _still_owns():
        return _purge_ownership_failure(service_name,
                                        'ownership lost after replica cleanup')

    if remaining_replica_clusters:
        # Keep every durable row/file and the name even though new replica
        # names are incarnation-scoped.  This preserves an authoritative,
        # retryable inventory for any billable cluster that survived down.
        if not serve_state.set_service_status_and_active_versions_if_owner(
                service_name,
                expected_service_hash,
                owner.get('controller_pid'),
                owner.get('controller_ip'),
                serve_state.ServiceStatus.FAILED_CLEANUP,
                expected_status=serve_state.ServiceStatus.SHUTTING_DOWN,
                expected_lifecycle_epoch=lifecycle_epoch):
            return _purge_ownership_failure(
                service_name, 'ownership lost while retaining failed cleanup')
        remaining_identity = ', '.join(remaining_replica_clusters)
        return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                'could not be purged because some replica clusters could not '
                'be terminated. The service name and cleanup metadata remain '
                'reserved; retry purge after checking: '
                f'{remaining_identity}{colorama.Style.RESET_ALL}')

    # Version rows may already have been retired while this service was live;
    # consume the separate durable generation manifests only after every
    # replica is confirmed gone and before the final DB removal.
    # Imported here to break the serve_utils <-> service dependency cycle.
    # pylint: disable=import-outside-toplevel
    from sky.serve import service as service_lib
    if not service_lib.cleanup_storage_intents(service_name, resource_scope,
                                               _still_owns):
        return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                'could not be purged because scoped storage cleanup failed; '
                'durable cleanup inventory was retained for retry.'
                f'{colorama.Style.RESET_ALL}')

    service_dir = os.path.expanduser(
        generate_remote_service_dir_name(service_name, resource_scope))
    # A legacy name-only directory has no injective owner identity. Keep it;
    # new scoped directories are safe to delete after the final DB CAS.
    remove_directory = resource_scope is not None
    # Claim the exact incarnation + lifecycle epoch first inside one
    # transaction, then remove every name-keyed child row.  All later
    # filesystem work targets the old incarnation's disjoint path.
    removed = serve_state.remove_service_completely(
        service_name,
        expected_service_hash,
        expected_lifecycle_epoch=lifecycle_epoch)
    if not removed:
        return _purge_ownership_failure(
            service_name, 'final database compare-and-delete '
            'lost ownership')
    if remove_directory:
        remove_service_directory(service_dir)
    return None


def _terminate_orphaned_service_children(service_name: str,
                                         expected_pool: bool) -> _PurgeResult:
    """Purge child-only replica/storage inventory under the name fence."""
    message = _terminate_orphaned_service_children_impl(service_name,
                                                        expected_pool)
    return _PurgeResult(message is None, message)


def _terminate_orphaned_service_children_impl(
        service_name: str, expected_pool: bool) -> str | None:
    """Implementation returning a diagnostic for an incomplete purge."""
    lifecycle_lock = get_service_lifecycle_lock(service_name)
    with lifecycle_lock:
        lifecycle_epoch = get_service_lifecycle_epoch(lifecycle_lock)

        child_pool = serve_state.get_orphaned_service_child_mode(service_name)
        if child_pool is None:
            return (f'{colorama.Fore.YELLOW}orphaned name {service_name!r} '
                    'could not be purged because its service/pool mode is '
                    'ambiguous; durable child inventory was retained for '
                    f'manual inspection.{colorama.Style.RESET_ALL}')
        if child_pool != expected_pool:
            expected_noun = 'pool' if expected_pool else 'service'
            actual_noun = 'pool' if child_pool else 'service'
            return (f'{colorama.Fore.YELLOW}orphaned name {service_name!r} '
                    f'belongs to a {actual_noun}, not a {expected_noun}; no '
                    f'children were changed.{colorama.Style.RESET_ALL}')

        def _still_orphaned() -> bool:
            return (lifecycle_lock_is_valid(lifecycle_lock) and
                    serve_state.get_service_mode_and_hash(service_name) is None
                    and
                    serve_state.get_orphaned_service_child_mode(service_name)
                    == expected_pool)

        if not _still_orphaned():
            return _purge_ownership_failure(
                service_name, 'a service row appeared before orphan cleanup')

        replica_infos = serve_state.get_replica_infos(service_name)
        if not quiesce_service_replica_launch_requests(
                service_name, replica_infos, continue_guard=_still_orphaned):
            return (f'{colorama.Fore.YELLOW}orphaned service '
                    f'{service_name!r} could not be purged because its replica '
                    'launch requests could not be quiesced; durable child '
                    f'inventory was retained.{colorama.Style.RESET_ALL}')

        # Imported here to break replica_managers -> serve_utils and
        # service -> replica_managers dependency cycles.
        # pylint: disable=import-outside-toplevel
        from sky.serve import lb_k8s
        from sky.serve import replica_managers
        from sky.serve import service as service_lib

        intents = serve_state.get_ephemeral_storage_cleanup_intents(
            service_name)
        resource_scopes = sorted({
            intent['resource_scope']
            for intent in intents
            if isinstance(intent.get('resource_scope'), str)
        })
        api_deployment_uid: str | None = None
        if resource_scopes and not expected_pool:
            try:
                api_deployment_uid = lb_k8s.get_api_deployment_owner_uid(
                    require_runtime=True)
            except Exception as e:  # pylint: disable=broad-except
                return (f'{colorama.Fore.YELLOW}orphaned service '
                        f'{service_name!r} could not be purged because the '
                        'current API Deployment owner could not be verified: '
                        f'{common_utils.format_exception(e)}.'
                        f'{colorama.Style.RESET_ALL}')
        for resource_scope in resource_scopes:
            if expected_pool:
                break
            if not _still_orphaned():
                return _purge_ownership_failure(
                    service_name, 'ownership lost before orphan LB cleanup')
            try:
                lb_k8s.delete_lb_objects(
                    service_name,
                    expected_service_hash=resource_scope,
                    resource_scope=resource_scope,
                    require_runtime=True,
                    expected_api_deployment_uid=api_deployment_uid,
                    high_availability=True)
            except Exception as e:  # pylint: disable=broad-except
                return (f'{colorama.Fore.YELLOW}orphaned service '
                        f'{service_name!r} could not be purged because scoped '
                        f'load balancer cleanup failed: '
                        f'{common_utils.format_exception(e)}.'
                        f'{colorama.Style.RESET_ALL}')

        try:
            existing_cluster_names = get_existing_replica_cluster_names(
                replica_infos)
        except Exception as e:  # pylint: disable=broad-except
            return (f'{colorama.Fore.YELLOW}orphaned service '
                    f'{service_name!r} could not be purged because its '
                    'replica cluster inventory could not be verified: '
                    f'{common_utils.format_exception(e)}.'
                    f'{colorama.Style.RESET_ALL}')
        if not _still_orphaned():
            return _purge_ownership_failure(
                service_name,
                'ownership lost after orphan cluster inventory snapshot')
        to_terminate = [
            info for info in replica_infos
            if info.cluster_name in existing_cluster_names
        ]
        termination_failures = []
        for info in to_terminate:
            if not _still_orphaned():
                return _purge_ownership_failure(
                    service_name,
                    'ownership lost before orphan replica cleanup')
            try:
                replica_managers.terminate_cluster(
                    info.cluster_name,
                    generate_replica_log_file_name(service_name,
                                                   info.replica_id),
                    continue_guard=_still_orphaned)
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'Failed to terminate orphan replica cluster '
                             f'{info.cluster_name!r}: '
                             f'{common_utils.format_exception(e)}')
                termination_failures.append(info.cluster_name)
        if termination_failures:
            return (f'{colorama.Fore.YELLOW}orphaned service '
                    f'{service_name!r} could not be purged because replica '
                    'cluster termination failed; retry after checking: '
                    f'{", ".join(sorted(termination_failures))}.'
                    f'{colorama.Style.RESET_ALL}')

        for resource_scope in resource_scopes:
            if not service_lib.cleanup_storage_intents(
                    service_name, resource_scope, _still_orphaned):
                return (
                    f'{colorama.Fore.YELLOW}orphaned service '
                    f'{service_name!r} could not be purged because scoped '
                    'storage cleanup failed; durable inventory was retained.'
                    f'{colorama.Style.RESET_ALL}')

        if not serve_state.remove_orphaned_service_children(
                service_name, lifecycle_epoch):
            return _purge_ownership_failure(
                service_name, 'ownership lost during orphan metadata removal')
    return None


def terminate_services(service_names: list[str] | None, purge: bool,
                       pool: bool) -> str:
    noun = 'pool' if pool else 'service'
    capnoun = noun.capitalize()
    requested_service_names = service_names
    service_names = serve_state.get_glob_service_names(service_names, pool=pool)
    if purge:
        service_names = sorted(
            set(service_names) | set(
                serve_state.get_orphaned_service_child_names(
                    requested_service_names)))
    terminated_service_names: list[str] = []
    messages: list[str] = []

    for service_name in service_names:
        service_status = _get_service_status(service_name,
                                             pool=pool,
                                             with_replica_info=False,
                                             with_yaml=False)
        if service_status is None:
            # `_get_service_status` returns None for two distinct cases: a
            # healthy service of the *other* mode (its `pool` flag != the
            # requested `pool`), and a `services` row with no `version_specs`
            # row -- an orphan stranded by an interrupted first-run
            # registration, invisible to the latest-version inner join.
            # `add_service` now writes both rows atomically, but a row
            # stranded before that fix can still exist, and no normal path
            # can recover or remove it (HA recovery and plain `down` both
            # skip a None status). With --purge, clean such an orphan up
            # directly -- but only when the raw row belongs to the requested
            # mode, so a serve `down --purge` never removes a jobs-pool's row
            # (or vice versa).
            if purge:
                raw_identity = serve_state.get_service_mode_and_hash(
                    service_name)
                if raw_identity is not None and raw_identity[0] == pool:
                    result = _terminate_failed_services(service_name,
                                                        raw_identity[1],
                                                        None,
                                                        pool=pool)
                    if result.message is not None:
                        messages.append(result.message)
                    if result.completed:
                        terminated_service_names.append(f'{service_name!r}')
                elif raw_identity is None:
                    result = _terminate_orphaned_service_children(
                        service_name, pool)
                    if result.message is not None:
                        messages.append(result.message)
                    if result.completed:
                        terminated_service_names.append(f'{service_name!r}')
            continue
        if (service_status is not None and service_status['status']
                == serve_state.ServiceStatus.SHUTTING_DOWN):
            if purge:
                # Resume exact-owner cleanup for a zombie or a prior
                # fail-closed purge attempt. The first purge durably CASes the
                # row to SHUTTING_DOWN before touching replicas/LB/files; any
                # later failure deliberately keeps that row retryable here.
                result = _terminate_failed_services(
                    service_name,
                    service_status.get('hash'),
                    serve_state.ServiceStatus.SHUTTING_DOWN,
                    pool=pool)
                if result.message is not None:
                    messages.append(result.message)
                if result.completed:
                    terminated_service_names.append(service_name)
            # Without --purge, treat as already scheduled to terminate.
            continue
        if pool:
            nonterminal_job_ids = (
                managed_job_state.get_nonterminal_job_ids_by_pool(service_name))
            if nonterminal_job_ids:
                nonterminal_job_ids_str = ','.join(
                    str(job_id) for job_id in nonterminal_job_ids)
                num_nonterminal_jobs = len(nonterminal_job_ids)
                messages.append(
                    f'{colorama.Fore.YELLOW}{capnoun} {service_name!r} has '
                    f'{num_nonterminal_jobs} nonterminal jobs: '
                    f'{nonterminal_job_ids_str}. To terminate the {noun}, '
                    f'please run `sky jobs cancel --pool {service_name}` to '
                    'cancel all jobs in the pool first.'
                    f'{colorama.Style.RESET_ALL}')
                continue
        purge_cmd = (f'sky jobs pool down {service_name} --purge'
                     if pool else f'sky serve down {service_name} --purge')
        if (service_status['status']
                in serve_state.ServiceStatus.failed_statuses()):
            failed_status = service_status['status']
            if purge:
                result = _terminate_failed_services(service_name,
                                                    service_status.get('hash'),
                                                    failed_status,
                                                    pool=pool)
                if result.message is not None:
                    messages.append(result.message)
                if result.completed:
                    terminated_service_names.append(f'{service_name!r}')
            else:
                messages.append(
                    f'{colorama.Fore.YELLOW}{capnoun} {service_name!r} is in '
                    f'failed status ({failed_status}). Skipping '
                    'its termination as it could lead to a resource leak. '
                    f'(Use `{purge_cmd}` to forcefully terminate the {noun}.)'
                    f'{colorama.Style.RESET_ALL}')
                # Don't add to terminated_service_names since it's not
                # actually terminated.
                continue
        else:
            # Send the terminate signal to controller.
            expected_service_hash = service_status.get('hash')
            lifecycle_lock = get_service_lifecycle_lock(service_name)
            with lifecycle_lock:
                # Re-read under the same distributed lifecycle fence used by
                # update/apply. This runs on the controller for named and
                # ``--all`` calls alike, so no client-side lock topology can
                # re-open the race.
                current = serve_state.get_service_controller_owner(service_name)
                marked_for_teardown = (
                    lifecycle_lock_is_valid(lifecycle_lock) and
                    isinstance(expected_service_hash, str) and
                    bool(expected_service_hash) and current is not None and
                    current.get('hash') == expected_service_hash and
                    current['status']
                    not in serve_state.ServiceStatus.terminal_statuses() and
                    serve_state.set_service_status_and_active_versions_if_hash(
                        service_name,
                        expected_service_hash,
                        serve_state.ServiceStatus.SHUTTING_DOWN,
                        expected_lifecycle_epoch=get_service_lifecycle_epoch(
                            lifecycle_lock)))
            if not marked_for_teardown:
                messages.append(
                    f'{colorama.Fore.YELLOW}{capnoun} {service_name!r} '
                    'changed incarnation before termination could be '
                    f'signaled; retry the command.{colorama.Style.RESET_ALL}')
                continue
            signal_file = pathlib.Path(
                constants.SIGNAL_FILE_PATH.format(service_name)).expanduser()
            # Make sure parent directory exists.
            signal_file.parent.mkdir(parents=True, exist_ok=True)
            # Filelock is needed to prevent race condition between signal
            # check/removal and signal writing.
            with filelock.FileLock(str(signal_file) + '.lock'):
                with signal_file.open(mode='w', encoding='utf-8') as f:
                    json.dump(
                        {
                            'signal': UserSignal.TERMINATE.value,
                            'service_hash': expected_service_hash,
                        },
                        f,
                        separators=(',', ':'))
                    f.flush()
        # Failed-service purge branches append only after confirming the row
        # was removed; normal signal-based down always reaches this point.
        if (service_status is None or
            (service_status['status']
             not in serve_state.ServiceStatus.failed_statuses() and
             service_status['status']
             != serve_state.ServiceStatus.SHUTTING_DOWN) or not purge):
            terminated_service_names.append(f'{service_name!r}')
    if not terminated_service_names:
        messages.append(f'No {noun} to terminate.')
    else:
        identity_str = f'{capnoun} {terminated_service_names[0]} is'
        if len(terminated_service_names) > 1:
            terminated_service_names_str = ', '.join(terminated_service_names)
            identity_str = f'{capnoun}s {terminated_service_names_str} are'
        messages.append(f'{identity_str} scheduled to be terminated.')
    return '\n'.join(messages)


def wait_service_registration(
        service_name: str,
        job_id: int,
        pool: bool,
        expected_resource_scope: str | None = None) -> str:
    """Util function to call at the end of `sky.serve.up()`.

    This function will:
        (1) Check the name duplication by job id of the controller. If
            the job id is not the same as the database record, this
            means another service is already taken that name. See
            sky/serve/api.py::up for more details.
        (2) Wait for the load balancer port to be assigned and return.

    Returns:
        Encoded load balancer port assigned to the service.
    """

    # TODO (kyuds): when codegen is fully deprecated, return the lb port
    # as an int directly instead of encoding it.
    def _controller_log_path(record: dict[str, Any] | None = None) -> str:
        resource_scope = (record.get('resource_scope')
                          if record is not None else expected_resource_scope)
        return os.path.expanduser(
            generate_remote_controller_log_file_name(service_name,
                                                     resource_scope))

    deadline = (time.monotonic() + constants.CONTROLLER_SETUP_TIMEOUT_SECONDS)
    setup_completed = False
    noun = 'pool' if pool else 'service'
    while True:
        # Only do this check for non-consolidation mode as consolidation mode
        # has no setup process.
        if not is_consolidation_mode(pool):
            job_status = job_lib.get_status(job_id)
            if job_status is not None and job_status.is_terminal():
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        f'The controller job for the {noun} {service_name!r} '
                        f'reached terminal status {job_status.value} before '
                        'registration completed.')
            if job_status != job_lib.JobStatus.RUNNING:
                # Wait for the controller process to finish setting up. It
                # can be slow if a lot cloud dependencies are being installed.
                if time.monotonic() > deadline:
                    with ux_utils.print_exception_no_traceback():
                        raise RuntimeError(
                            f'Failed to start the controller process for '
                            f'the {noun} {service_name!r} within '
                            f'{constants.CONTROLLER_SETUP_TIMEOUT_SECONDS}'
                            f' seconds.')
                # No need to check the service status as the controller process
                # is still setting up.
                time.sleep(1)
                continue

        if not setup_completed:
            setup_completed = True
            # Give service registration its own full timeout budget.
            deadline = (time.monotonic() +
                        constants.SERVICE_REGISTER_TIMEOUT_SECONDS)

        record = _get_service_status(service_name,
                                     pool=pool,
                                     with_replica_info=False,
                                     with_yaml=False,
                                     status_snapshot_only=True)
        if record is not None:
            if (expected_resource_scope is not None and
                    record.get('resource_scope') != expected_resource_scope):
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        f'The {noun} {service_name!r} changed incarnation '
                        'during registration; refusing to accept a same-name '
                        'replacement that reused the controller job id.')
            if job_id != record['controller_job_id']:
                if pool:
                    command_to_run = 'sky jobs pool apply --pool'
                else:
                    command_to_run = 'sky serve update'
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        f'The {noun} {service_name!r} is already running. '
                        f'Please specify a different name for your {noun}. '
                        f'To update an existing {noun}, run: {command_to_run}'
                        f' {service_name} <new-{noun}-yaml>')
            status = record['status']
            if status in serve_state.ServiceStatus.terminal_statuses():
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        f'The {noun} {service_name!r} entered terminal status '
                        f'{status.value} during registration; cleanup is in '
                        'progress or required.')
            lb_port = record['load_balancer_port']
            if lb_port is not None:
                return message_utils.encode_payload(lb_port)
        else:
            controller_log_path = _controller_log_path()
            if os.path.exists(controller_log_path):
                with open(controller_log_path, encoding='utf-8') as f:
                    log_content = f.read()
                if (constants.MAX_NUMBER_OF_SERVICES_REACHED_ERROR
                        in log_content):
                    with ux_utils.print_exception_no_traceback():
                        raise RuntimeError(
                            controller_utils.get_max_services_error_message(
                                pool))
        if time.monotonic() > deadline:
            # Print the controller log to help user debug.
            controller_log_path = _controller_log_path(record)
            try:
                with open(controller_log_path, encoding='utf-8') as f:
                    log_content = f.read()
            except FileNotFoundError:
                log_content = (f'Controller log {controller_log_path!r} '
                               'not found.')
            with ux_utils.print_exception_no_traceback():
                raise ValueError(f'Failed to register service {service_name!r} '
                                 'on the SkyServe controller. '
                                 f'Reason:\n{log_content}')
        time.sleep(1)


def load_service_initialization_result(payload: str) -> int:
    return message_utils.decode_payload(payload)


def _get_service_log_owner_record(service_name: str,
                                  pool: bool) -> dict[str, Any] | None:
    """Read the slim service row used by log/liveness helpers.

    These paths only need status and resource-scope metadata, so they must
    stay off the joined latest-spec read and never parse YAML.
    """
    record = serve_state.get_service_controller_owner(service_name,
                                                      require_version=True)
    if record is None or record.get('pool') != pool:
        return None
    return record


def _get_healthy_service_log_owner_record(
        service_name: str,
        pool: bool) -> tuple[dict[str, Any] | None, str | None]:
    """Return one slim owner snapshot or the user-facing health error."""
    service_record = _get_service_log_owner_record(service_name, pool)
    capnoun = 'Service' if not pool else 'Pool'
    if service_record is None:
        return None, f'{capnoun} {service_name!r} does not exist.'
    if service_record['status'] == serve_state.ServiceStatus.CONTROLLER_INIT:
        return (None, f'{capnoun} {service_name!r} is still initializing its '
                'controller. Please try again later.')
    return service_record, None


def _check_service_status_healthy(service_name: str, pool: bool) -> str | None:
    _, msg = _get_healthy_service_log_owner_record(service_name, pool)
    return msg


def get_latest_version_with_min_replicas(
        service_name: str,
        replica_infos: list['replica_managers.ReplicaInfo']) -> int | None:
    # Find the latest version with at least min_replicas replicas.
    version2count: collections.defaultdict[int,
                                           int] = collections.defaultdict(int)
    for info in replica_infos:
        if info.is_ready:
            version2count[info.version] += 1

    active_versions = sorted(version2count.keys(), reverse=True)
    specs_by_version = serve_state.get_specs(service_name, active_versions)
    for version in active_versions:
        spec = specs_by_version.get(version)
        if (spec is not None and version2count[version] >= spec.min_replicas):
            return version
    # Use the oldest version if no version has enough replicas.
    return active_versions[-1] if active_versions else None


def _process_line(
        line: str,
        cluster_name: str,
        stop_on_eof: bool = False,
        streamed_provision_log_paths: set | None = None) -> Iterator[str]:
    # The line might be directing users to view logs, like
    # `✓ Cluster launched: new-http.  View logs at: *.log`
    # We should tail the detailed logs for user.
    def cluster_is_up() -> bool:
        status = global_user_state.get_status_from_cluster_name(cluster_name)
        return status in (status_lib.ClusterStatus.UP,
                          status_lib.ClusterStatus.AUTOSTOPPING)

    provision_api_log_prompt = re.match(_SKYPILOT_PROVISION_API_LOG_PATTERN,
                                        line)
    provision_log_cmd_prompt = re.match(_SKYPILOT_PROVISION_LOG_CMD_PATTERN,
                                        line)
    log_prompt = re.match(_SKYPILOT_LOG_PATTERN, line)

    def _stream_provision_path(p: pathlib.Path) -> Iterator[str]:
        # Check if this provision log has already been streamed to avoid
        # duplicate expansion. When a Kubernetes cluster needs to pull a Docker
        # image, rich spinner updates can produce hundreds of lines matching
        # _SKYPILOT_PROVISION_LOG_CMD_PATTERN (e.g., "Launching (1 pod(s)
        # pending due to Pulling)... View logs: sky logs --provision ...").
        # Without this check, the same provision log would be expanded hundreds
        # of times, creating huge log files (30M+) and making users think the
        # system is stuck in an infinite loop.
        if streamed_provision_log_paths is not None:
            resolved_path = str(p.resolve())
            if resolved_path in streamed_provision_log_paths:
                return
            streamed_provision_log_paths.add(resolved_path)

        try:
            with open(p, newline='', encoding='utf-8') as f:
                # Exit if >10s without new content to avoid hanging when INIT
                yield from log_utils.follow_logs(f,
                                                 should_stop=cluster_is_up,
                                                 stop_on_eof=stop_on_eof,
                                                 idle_timeout_seconds=10)
        except FileNotFoundError:
            # Fall back cleanly if the hinted path doesn't exist
            yield line
            yield (f'{colorama.Fore.YELLOW}{colorama.Style.BRIGHT}'
                   f'Try to expand log file {p} but not found. Skipping...'
                   f'{colorama.Style.RESET_ALL}')
        return

    if provision_api_log_prompt is not None:
        rel_path = provision_api_log_prompt.group(1)
        nested_log_path = pathlib.Path(
            skylet_constants.SKY_LOGS_DIRECTORY).expanduser().joinpath(
                rel_path).resolve()
        yield from _stream_provision_path(nested_log_path)
        return

    if provision_log_cmd_prompt is not None:
        # Resolve provision log via cluster table first, then history.
        log_path_str = global_user_state.get_cluster_provision_log_path(
            cluster_name)
        if not log_path_str:
            log_path_str = (
                global_user_state.get_cluster_history_provision_log_path(
                    cluster_name))
        if not log_path_str:
            yield line
            return
        yield from _stream_provision_path(
            pathlib.Path(log_path_str).expanduser().resolve())
        return

    if log_prompt is not None:
        # Now we skip other logs (file sync logs) since we lack
        # utility to determine when these log files are finished
        # writing.
        # TODO(tian): We should not skip these logs since there are
        # small chance that error will happen in file sync. Need to
        # find a better way to do this.
        return

    yield line


def _follow_logs_with_provision_expanding(
    file: TextIO,
    cluster_name: str,
    *,
    should_stop: Callable[[], bool],
    stop_on_eof: bool = False,
    idle_timeout_seconds: int | None = None,
) -> Iterator[str]:
    """Follows logs and expands any provision.log references found.

    Args:
        file: Log file to read from.
        cluster_name: Name of the cluster being launched.
        should_stop: Callback that returns True when streaming should stop.
        stop_on_eof: If True, stop when reaching end of file.
        idle_timeout_seconds: If set, stop after these many seconds without
            new content.

    Yields:
        Log lines, including expanded content from referenced provision logs.
    """
    streamed_provision_log_paths: set = set()

    def process_line(line: str) -> Iterator[str]:
        yield from _process_line(
            line,
            cluster_name,
            stop_on_eof=stop_on_eof,
            streamed_provision_log_paths=streamed_provision_log_paths)

    return log_utils.follow_logs(file,
                                 should_stop=should_stop,
                                 stop_on_eof=stop_on_eof,
                                 process_line=process_line,
                                 idle_timeout_seconds=idle_timeout_seconds)


def _capped_follow_logs_with_provision_expanding(
    log_list: list[str],
    cluster_name: str,
    *,
    line_cap: int = 100,
) -> Iterator[str]:
    """Follows logs and expands any provision.log references found.

    Args:
        log_list: List of Log Lines to read from.
        cluster_name: Name of the cluster being launched.
        line_cap: Number of last lines to return

    Yields:
        Log lines, including expanded content from referenced provision logs.
    """
    all_lines: collections.deque[str] = collections.deque(maxlen=line_cap)
    streamed_provision_log_paths: set = set()

    for line in log_list:
        for processed in _process_line(
                line=line,
                cluster_name=cluster_name,
                stop_on_eof=False,
                streamed_provision_log_paths=streamed_provision_log_paths):
            all_lines.append(processed)

    yield from all_lines


def stream_replica_logs(service_name: str, replica_id: int, follow: bool,
                        tail: int | None, pool: bool) -> str:
    record, msg = _get_healthy_service_log_owner_record(service_name, pool=pool)
    if msg is not None:
        return msg
    assert record is not None
    repnoun = 'worker' if pool else 'replica'
    caprepnoun = repnoun.capitalize()
    print(f'{colorama.Fore.YELLOW}Start streaming logs for launching process '
          f'of {repnoun} {replica_id}.{colorama.Style.RESET_ALL}')
    resource_scope = record.get('resource_scope')
    log_file_name = generate_replica_log_file_name(service_name, replica_id,
                                                   resource_scope)
    # The replica_<id>.log file is the post-mortem archive: it's only
    # populated on the teardown path (terminate_cluster's redirect_log,
    # or _download_and_stream_logs writing launch_log + ssh'd job logs
    # into it). A 0-byte file on disk is a teardown-race remnant — e.g.
    # `terminate_cluster` was invoked on a replica that never came up, so
    # `ctx.redirect_log` created the file but no log lines were written;
    # or `_download_and_stream_logs` opened with mode='w' and crashed
    # before writing. If we trust `os.path.exists` alone, we commit to
    # the (empty) main log and silently drop the launch log fallback,
    # making `sky jobs pool logs` return empty for an alive replica.
    if (os.path.exists(log_file_name) and os.path.getsize(log_file_name) > 0):
        if tail is not None:
            lines = common_utils.read_last_n_lines(log_file_name, tail)
            for line in lines:
                if not line.endswith('\n'):
                    line += '\n'
                print(line, end='', flush=True)
        else:
            with open(log_file_name, encoding='utf-8') as f:
                print(f.read(), flush=True)
        return ''

    launch_log_file_name = generate_replica_launch_log_file_name(
        service_name, replica_id, resource_scope)
    if not os.path.exists(launch_log_file_name):
        return (f'{colorama.Fore.RED}{caprepnoun} {replica_id} doesn\'t exist.'
                f'{colorama.Style.RESET_ALL}')

    matching_info = serve_state.get_replica_info_from_id(
        service_name, replica_id)
    recorded_cluster_name = (getattr(matching_info, 'cluster_name', None)
                             if matching_info is not None else None)
    replica_cluster_name = (recorded_cluster_name if isinstance(
        recorded_cluster_name, str) else generate_replica_cluster_name(
            service_name, replica_id, resource_scope))

    def _get_replica_status() -> serve_state.ReplicaStatus:
        # Single-row lookup: this runs on every poll of the follow loop
        # below, so scanning (and unpickling) every replica of the service
        # per poll is O(replicas) wasted work at fleet scale.
        info = serve_state.get_replica_info_from_id(service_name, replica_id)
        if info is not None:
            return info.status
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                _FAILED_TO_FIND_REPLICA_MSG.format(replica_id=replica_id))

    replica_provisioned = (
        lambda: _get_replica_status() != serve_state.ReplicaStatus.PROVISIONING)

    # Handle launch logs based on number parameter
    final_lines_to_print = []
    if tail is not None:
        static_lines = common_utils.read_last_n_lines(launch_log_file_name,
                                                      tail)
        lines = list(
            _capped_follow_logs_with_provision_expanding(
                log_list=static_lines,
                cluster_name=replica_cluster_name,
                line_cap=tail,
            ))
        final_lines_to_print += lines
    else:
        with open(launch_log_file_name, newline='', encoding='utf-8') as f:
            for line in _follow_logs_with_provision_expanding(
                    f,
                    replica_cluster_name,
                    should_stop=replica_provisioned,
                    stop_on_eof=not follow,
            ):
                print(line, end='', flush=True)

    if (not follow and
            _get_replica_status() == serve_state.ReplicaStatus.PROVISIONING):
        # Early exit if not following the logs.
        if tail is not None:
            for line in final_lines_to_print:
                if not line.endswith('\n'):
                    line += '\n'
                print(line, end='', flush=True)
        return ''

    backend = backends.CloudVmRayBackend()
    handle = global_user_state.get_handle_from_cluster_name(
        replica_cluster_name)
    if handle is None:
        if tail is not None:
            for line in final_lines_to_print:
                if not line.endswith('\n'):
                    line += '\n'
                print(line, end='', flush=True)
        return _FAILED_TO_FIND_REPLICA_MSG.format(replica_id=replica_id)
    assert isinstance(handle, backends.CloudVmRayResourceHandle), handle

    # Notify user here to make sure user won't think the log is finished.
    print(f'{colorama.Fore.YELLOW}Start streaming logs for task job '
          f'of {repnoun} {replica_id}...{colorama.Style.RESET_ALL}')

    # Always tail the latest logs, which represent user setup & run.
    if tail is None:
        returncode = backend.tail_logs(handle, job_id=None, follow=follow)
        if returncode != 0:
            return (f'{colorama.Fore.RED}Failed to stream logs for {repnoun} '
                    f'{replica_id}.{colorama.Style.RESET_ALL}')
    elif not follow and tail > 0:
        final = backend.tail_logs(handle,
                                  job_id=None,
                                  follow=follow,
                                  tail=tail,
                                  stream_logs=False,
                                  require_outputs=True,
                                  process_stream=True)
        if isinstance(final, int) or (final[0] != 0 and final[0] != 101):
            if tail is not None:
                for line in final_lines_to_print:
                    if not line.endswith('\n'):
                        line += '\n'
                    print(line, end='', flush=True)
            return (f'{colorama.Fore.RED}Failed to stream logs for replica '
                    f'{replica_id}.{colorama.Style.RESET_ALL}')
        final_lines_to_print += final[1].splitlines()
        for line in final_lines_to_print[-tail:]:
            if not line.endswith('\n'):
                line += '\n'
            print(line, end='', flush=True)
    return ''


def stream_serve_process_logs(service_name: str, stream_controller: bool,
                              follow: bool, tail: int | None,
                              pool: bool) -> str:
    record, msg = _get_healthy_service_log_owner_record(service_name, pool)
    if msg is not None:
        return msg
    assert record is not None
    if not stream_controller:
        if pool:
            return 'Pools do not have a load balancer.'
        # Lazy import avoids the lb_k8s -> serve_utils module cycle. External-
        # only SkyServe writes LB output to the Kubernetes Pod log, never the
        # legacy controller-local load_balancer.log file.
        from sky.serve import lb_k8s  # pylint: disable=import-outside-toplevel
        return lb_k8s.stream_lb_logs(service_name, follow, tail)
    resource_scope = record.get('resource_scope')
    log_file = generate_remote_controller_log_file_name(service_name,
                                                        resource_scope)

    def _service_is_terminal() -> bool:
        record = _get_service_log_owner_record(service_name, pool)
        if record is None:
            return True
        return record['status'] in serve_state.ServiceStatus.failed_statuses()

    if tail is not None:
        lines = common_utils.read_last_n_lines(os.path.expanduser(log_file),
                                               tail)
        for line in lines:
            if not line.endswith('\n'):
                line += '\n'
            print(line, end='', flush=True)
    else:
        with open(os.path.expanduser(log_file), newline='',
                  encoding='utf-8') as f:
            for line in log_utils.follow_logs(
                    f,
                    should_stop=_service_is_terminal,
                    stop_on_eof=not follow,
            ):
                print(line, end='', flush=True)
    return ''


# ================== Table Formatter for `sky serve status` ==================


def _get_replicas(service_record: dict[str, Any]) -> str:
    ready = service_record.get('ready_replicas')
    total = service_record.get('total_replicas')
    if (isinstance(ready, int) and not isinstance(ready, bool) and
            ready >= 0 and isinstance(total, int) and
            not isinstance(total, bool) and total >= 0):
        return f'{ready}/{total}'
    ready_replica_num, total_replica_num = 0, 0
    for info in service_record['replica_info']:
        if info['status'] == serve_state.ReplicaStatus.READY:
            ready_replica_num += 1
        # TODO(MaoZiming): add a column showing failed replicas number.
        if info['status'] not in serve_state.ReplicaStatus.failed_statuses():
            total_replica_num += 1
    return f'{ready_replica_num}/{total_replica_num}'


def format_service_table(service_records: list[dict[str, Any]], show_all: bool,
                         pool: bool) -> str:
    noun = 'pool' if pool else 'service'
    if not service_records:
        return f'No existing {noun}s.'

    service_columns = [
        'NAME', 'VERSION', 'UPTIME', 'STATUS',
        'REPLICAS' if not pool else 'WORKERS'
    ]
    if not pool:
        service_columns.append('ENDPOINT')
    if show_all:
        service_columns.extend([
            'AUTOSCALING_POLICY', 'LOAD_BALANCING_POLICY', 'REQUESTED_RESOURCES'
        ])
        if pool:
            # Remove the load balancing policy column for pools.
            service_columns.pop(-2)
    service_table = log_utils.create_table(service_columns)

    replica_infos: list[dict[str, Any]] = []
    for record in service_records:
        for replica in record['replica_info']:
            replica['service_name'] = record['name']
            replica_infos.append(replica)

        service_name = record['name']
        version = ','.join(
            str(v) for v in record['active_versions']
        ) if 'active_versions' in record and record['active_versions'] else '-'
        uptime = log_utils.readable_time_duration(record['uptime'],
                                                  absolute=True)
        service_status = record['status']
        status_str = service_status.colored_str()
        replicas = _get_replicas(record)
        endpoint = record['endpoint']
        if endpoint is None:
            endpoint = '-'
        policy = record['policy']
        requested_resources_str = record['requested_resources_str']
        load_balancing_policy = record['load_balancing_policy']

        service_values = [
            service_name,
            version,
            uptime,
            status_str,
            replicas,
        ]
        if not pool:
            service_values.append(endpoint)
        if show_all:
            service_values.extend(
                [policy, load_balancing_policy, requested_resources_str])
            if pool:
                service_values.pop(-2)
        service_table.add_row(service_values)

    replica_table = _format_replica_table(replica_infos, show_all, pool)
    replica_noun = 'Pool Workers' if pool else 'Service Replicas'
    return (f'{service_table}\n'
            f'\n{colorama.Fore.CYAN}{colorama.Style.BRIGHT}'
            f'{replica_noun}{colorama.Style.RESET_ALL}\n'
            f'{replica_table}')


def _format_replica_table(replica_records: list[dict[str, Any]], show_all: bool,
                          pool: bool) -> str:
    noun = 'worker' if pool else 'replica'
    if not replica_records:
        return f'No existing {noun}s.'

    replica_columns = [
        'POOL_NAME' if pool else 'SERVICE_NAME', 'ID', 'VERSION', 'ENDPOINT',
        'LAUNCHED', 'INFRA', 'RESOURCES', 'STATUS'
    ]
    if pool:
        replica_columns.append('USED_BY')
        # Remove the endpoint column for pool workers.
        replica_columns.pop(3)
    replica_table = log_utils.create_table(replica_columns)

    truncate_hint = ''
    if not show_all:
        if len(replica_records) > _REPLICA_TRUNC_NUM:
            truncate_hint = f'\n... (use --all to show all {noun}s)'
        replica_records = replica_records[:_REPLICA_TRUNC_NUM]

    for record in replica_records:
        endpoint = record.get('endpoint', '-')
        service_name = record['service_name']
        replica_id = record['replica_id']
        version = (record['version'] if 'version' in record else '-')
        replica_endpoint = endpoint if endpoint else '-'
        launched_at = log_utils.readable_time_duration(record['launched_at'])
        infra = '-'
        resources_str = '-'
        replica_status = record['status']
        status_str = replica_status.colored_str()
        used_by = record.get('used_by', None)
        if used_by is None:
            used_by_str = '-'
        elif isinstance(used_by, str):
            used_by_str = used_by
        else:
            if len(used_by) > 2:
                used_by_str = (
                    f'{used_by[0]}, {used_by[1]}, +{len(used_by) - 2}'
                    ' more')
            elif len(used_by) == 2:
                used_by_str = f'{used_by[0]}, {used_by[1]}'
            elif len(used_by) == 1:
                used_by_str = str(used_by[0])
            else:
                used_by_str = '-'

        # Prefer pre-computed string fields from the server (new servers
        # ship these alongside or instead of a pickled handle to keep wire
        # payload small). Fall back to computing them locally from
        # ``record['handle']`` for back-compat with old servers.
        infra_pre = record.get('infra')
        if infra_pre is not None:
            infra = infra_pre
        if show_all:
            resources_pre = (record.get('resources_str_full') or
                             record.get('resources_str'))
        else:
            resources_pre = record.get('resources_str')
        if resources_pre is not None:
            resources_str = resources_pre

        if infra_pre is None or resources_pre is None:
            replica_handle: backends.CloudVmRayResourceHandle | None = record.get(
                'handle')
            if (replica_handle is not None and
                    replica_handle.launched_resources is not None):
                if infra_pre is None:
                    infra = (
                        replica_handle.launched_resources.infra.formatted_str())
                if resources_pre is None:
                    simplified = not show_all
                    resources_str_simple, resources_str_full = (
                        resources_utils.get_readable_resources_repr(
                            replica_handle, simplified_only=simplified))
                    if simplified:
                        resources_str = resources_str_simple
                    else:
                        assert resources_str_full is not None
                        resources_str = resources_str_full

        replica_values = [
            service_name,
            replica_id,
            version,
            replica_endpoint,
            launched_at,
            infra,
            resources_str,
            status_str,
        ]
        if pool:
            replica_values.append(used_by_str)
            replica_values.pop(3)
        replica_table.add_row(replica_values)

    return f'{replica_table}{truncate_hint}'


# =========================== CodeGen for Sky Serve ===========================
# TODO (kyuds): deprecate and remove serve codegen entirely.


# TODO(tian): Use REST API instead of SSH in the future. This codegen pattern
# is to reuse the authentication of ssh. If we want to use REST API, we need
# to implement some authentication mechanism.
class ServeCodeGen:
    """Code generator for SkyServe.

    Usage:
      >> code = ServeCodeGen.get_service_status(service_name)
    """

    # TODO(zhwu): When any API is changed, we should update the
    # constants.SERVE_VERSION.
    _PREFIX = [
        'from sky.serve import serve_state',
        'from sky.serve import serve_utils',
        'from sky.serve import constants',
        'serve_version = constants.SERVE_VERSION',
    ]

    @classmethod
    def get_service_status(
            cls,
            service_names: list[str] | None,
            pool: bool,
            summary_only: bool = False,
            include_target_num_replicas: bool | None = None) -> str:
        # summary_only is only forwarded to controllers whose lib version
        # understands it (v6+); older controllers just return the full
        # payload — a graceful degradation, never an error.
        code = [
            f'kwargs={{}} if serve_version < 3 else {{"pool": {pool}}}',
            ('kwargs.update({"summary_only": '
             f'{summary_only}}}) if serve_version >= 6 else None'),
            ('kwargs.update({"include_target_num_replicas": '
             f'{include_target_num_replicas}}}) if serve_version >= 7 else '
             'None') if include_target_num_replicas is not None else None,
            f'msg = serve_utils.get_service_status_encoded({service_names!r}, '
            '**kwargs)', 'print(msg, end="", flush=True)'
        ]
        return cls._build([line for line in code if line is not None])

    @classmethod
    def add_version(cls, service_name: str) -> str:
        code = [
            f'msg = serve_utils.add_version_encoded({service_name!r})',
            'print(msg, end="", flush=True)'
        ]
        return cls._build(code)

    @classmethod
    def terminate_services(cls, service_names: list[str] | None, purge: bool,
                           pool: bool) -> str:
        code = [
            f'kwargs={{}} if serve_version < 3 else {{"pool": {pool}}}',
            f'msg = serve_utils.terminate_services({service_names!r}, '
            f'purge={purge}, **kwargs)', 'print(msg, end="", flush=True)'
        ]
        return cls._build(code)

    @classmethod
    def terminate_replica(cls, service_name: str, replica_id: int,
                          purge: bool) -> str:
        code = [
            f'(lambda: print(serve_utils.terminate_replica({service_name!r}, '
            f'{replica_id}, {purge}), end="", flush=True) '
            'if getattr(constants, "SERVE_VERSION", 0) >= 2 else '
            f'exec("raise RuntimeError('
            f'{constants.TERMINATE_REPLICA_VERSION_MISMATCH_ERROR!r})"))()'
        ]
        return cls._build(code)

    @classmethod
    def wait_service_registration(cls, service_name: str, job_id: int,
                                  pool: bool) -> str:
        code = [
            f'kwargs={{}} if serve_version < 4 else {{"pool": {pool}}}',
            'msg = serve_utils.wait_service_registration('
            f'{service_name!r}, {job_id}, **kwargs)',
            'print(msg, end="", flush=True)'
        ]
        cmd = cls._build(code)
        # When running in consolidation mode, the codegen subprocess inherits
        # SKYPILOT_GLOBAL_CONFIG pointing to the client override config, which
        # lacks serve.controller.consolidation_mode=true. The subprocess would
        # then read the server config from the client override path and
        # incorrectly conclude it is NOT in consolidation mode, causing a
        # 300-second CONTROLLER_SETUP_TIMEOUT. Bake OVERRIDE_CONSOLIDATION_MODE
        # into the shell command so the subprocess always sees the correct mode.
        if is_consolidation_mode(pool):
            cmd = (f'export {skylet_constants.OVERRIDE_CONSOLIDATION_MODE}'
                   f'=true; {cmd}')
        return cmd

    @classmethod
    def stream_replica_logs(cls, service_name: str, replica_id: int,
                            follow: bool, tail: int | None, pool: bool) -> str:
        code = [
            f'kwargs={{}} if serve_version < 5 else {{"pool": {pool}}}',
            'msg = serve_utils.stream_replica_logs('
            f'{service_name!r}, {replica_id!r}, follow={follow}, tail={tail}, '
            '**kwargs)', 'print(msg, flush=True)'
        ]
        return cls._build(code)

    @classmethod
    def stream_serve_process_logs(cls, service_name: str,
                                  stream_controller: bool, follow: bool,
                                  tail: int | None, pool: bool) -> str:
        code = [
            f'kwargs={{}} if serve_version < 5 else {{"pool": {pool}}}',
            f'msg = serve_utils.stream_serve_process_logs({service_name!r}, '
            f'{stream_controller}, follow={follow}, tail={tail}, **kwargs)',
            'print(msg, flush=True)'
        ]
        return cls._build(code)

    @classmethod
    def update_service(cls, service_name: str, version: int, mode: str,
                       pool: bool) -> str:
        code = [
            f'kwargs={{}} if serve_version < 3 else {{"pool": {pool}}}',
            f'msg = serve_utils.update_service_encoded({service_name!r}, '
            f'{version}, mode={mode!r}, **kwargs)',
            'print(msg, end="", flush=True)',
        ]
        return cls._build(code)

    @classmethod
    def _build(cls, code: list[str]) -> str:
        code = cls._PREFIX + code
        generated_code = '; '.join(code)
        # Use the local user id to make sure the operation goes to the correct
        # user.
        return (f'export {skylet_constants.USER_ID_ENV_VAR}='
                f'"{common_utils.get_user_hash()}"; '
                f'{skylet_constants.SKY_PYTHON_CMD} '
                f'-u -c {shlex.quote(generated_code)}')
