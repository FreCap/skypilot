"""User interface with the SkyServe."""
import base64
import collections
import concurrent.futures
import contextvars
import dataclasses
import datetime
import enum
import hashlib
import ipaddress
import json
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
from typing import (Any, Callable, DefaultDict, Deque, Dict, Iterator, List,
                    Optional, Set, TextIO, Tuple, Type, Union)
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
from sky.jobs import state as managed_job_state
from sky.serve import constants
from sky.serve import serve_state
from sky.serve import spot_placer
from sky.skylet import constants as skylet_constants
from sky.skylet import job_lib
from sky.utils import annotations
from sky.utils import command_runner
from sky.utils import common_utils
from sky.utils import controller_utils
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
else:
    psutil = adaptors_common.LazyImport('psutil')
    requests = adaptors_common.LazyImport('requests')

logger = sky_logging.init_logger(__name__)

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


class AuthTokenConfigurationError(ValueError):
    """A required Serve auth ring is absent or cannot be parsed safely."""


class ControllerOwnerError(RuntimeError):
    """The intended service incarnation has no safe controller target."""


_AUTH_TOKEN_PATTERN = re.compile(r'[A-Za-z0-9._~+/=-]+')

_ControllerOwner = Tuple[str, int, Optional[str], int]


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
    normalized_ip: Optional[str] = None
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
                        expected_service_hash: str) -> Tuple[str, str]:
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


def _get_local_controller_url(owner: _ControllerOwner) -> Tuple[str, str]:
    """Resolve a specifically supervised local child without consulting DB."""
    service_hash, controller_pid, controller_ip, controller_port = owner
    owner_fingerprint = make_controller_owner_fingerprint(
        service_hash, controller_pid, controller_ip, controller_port)
    return f'http://localhost:{controller_port}', owner_fingerprint


def _request_to_controller_with_retry(
        method: str,
        service_name: str,
        expected_service_hash: str,
        path: str,
        *,
        fixed_controller_owner: Optional[_ControllerOwner] = None,
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
    replica_id: Optional[int] = None

    def __init__(self,
                 component: Union[str, ServiceComponent],
                 replica_id: Optional[int] = None):
        if isinstance(component, str):
            component = ServiceComponent(component)
        self.component = component
        self.replica_id = replica_id

    def __post_init__(self):
        """Validate that replica_id is only provided for REPLICA component."""
        if (self.component
                == ServiceComponent.REPLICA) != (self.replica_id is None):
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

    def error_type(self) -> Type[Exception]:
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

    def clear(self) -> None:
        """Clear all current request aggregator."""
        raise NotImplementedError

    def drain(self) -> Dict[str, Any]:
        """Atomically take the current report batch out of the aggregator.

        New samples added after this method returns belong to the next batch.
        The caller must restore the returned batch if delivery fails.
        """
        raise NotImplementedError

    def restore(self, batch: Dict[str, Any]) -> None:
        """Restore a previously drained batch after failed delivery."""
        raise NotImplementedError

    def to_dict(self) -> Dict[str, Any]:
        """Convert the aggregator to a dict."""
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
        self.timestamps: 'collections.deque[float]' = collections.deque(
            maxlen=constants.LB_REQUEST_TIMESTAMP_CAP)

    def add(self, request: 'fastapi.Request') -> None:
        """Add a request to the request aggregator."""
        del request  # unused
        self.timestamps.append(time.time())

    def clear(self) -> None:
        """Clear all current request aggregator."""
        self.timestamps.clear()

    def drain(self) -> Dict[str, Any]:
        """Take the current timestamps, leaving later arrivals untouched."""
        batch = self.to_dict()
        self.timestamps.clear()
        return batch

    def restore(self, batch: Dict[str, Any]) -> None:
        """Merge a failed batch back ahead of any arrivals made in-flight.

        Extending oldest-to-newest also preserves the deque's bounded behavior:
        if the combined batches exceed the cap, only the newest timestamps are
        retained.
        """
        drained = batch.get('timestamps', [])
        if not drained:
            return
        current = list(self.timestamps)
        self.timestamps.clear()
        self.timestamps.extend(drained)
        self.timestamps.extend(current)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the aggregator to a dict."""
        return {'timestamps': list(self.timestamps)}

    def __repr__(self) -> str:
        return f'RequestTimestamp(timestamps={list(self.timestamps)})'


def get_service_filelock_path(pool: str) -> str:
    # Request serialization must not use an inode inside the canonical service
    # directory. Teardown atomically quarantines that directory; a waiter
    # creating ``<service>/pool.lock`` afterward would both bypass the old
    # lock inode and prevent a failed teardown from restoring its directory.
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
        self.epoch: Optional[int] = None

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
        except Exception:
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


def quarantine_service_directory(service_dir: str,
                                 service_hash: str) -> List[str]:
    """Atomically move a canonical working directory to hash-owned storage.

    The deterministic sibling path makes teardown retryable after a process
    death. It is never reused by another incarnation because its name derives
    from the durable service hash.
    """
    digest = hashlib.sha256(service_hash.encode('utf-8')).hexdigest()[:20]
    quarantine_dir = f'{service_dir}.teardown-{digest}'
    parent_dir = os.path.dirname(quarantine_dir)
    retry_prefix = f'{os.path.basename(quarantine_dir)}-retry-'
    existing_quarantines = []
    if os.path.lexists(quarantine_dir):
        existing_quarantines.append(quarantine_dir)
    try:
        existing_quarantines.extend(
            os.path.join(parent_dir, entry)
            for entry in sorted(os.listdir(parent_dir))
            if entry.startswith(retry_prefix) and
            os.path.lexists(os.path.join(parent_dir, entry)))
    except FileNotFoundError:
        pass
    if existing_quarantines:
        if os.path.lexists(service_dir):
            # Expected after a crash between quarantine and DB CAS: HA startup
            # recreates the canonical directory before resuming teardown.
            # Move that new same-incarnation directory too, so nothing at the
            # canonical path can be deleted after the name is released.
            retry_dir = f'{quarantine_dir}-retry-{uuid.uuid4().hex}'
            os.rename(service_dir, retry_dir)
            return existing_quarantines + [retry_dir]
        return existing_quarantines
    try:
        os.rename(service_dir, quarantine_dir)
    except FileNotFoundError:
        return []
    return [quarantine_dir]


def remove_quarantined_service_directory(
        quarantine_dirs: Optional[List[str]]) -> None:
    """Remove only a hash-owned quarantine, never the canonical path."""
    if not quarantine_dirs:
        return
    for quarantine_dir in quarantine_dirs:
        try:
            if os.path.islink(quarantine_dir):
                os.unlink(quarantine_dir)
            else:
                shutil.rmtree(quarantine_dir)
        except FileNotFoundError:
            pass


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
        all_services = [
            svc for svc in serve_state.get_services() if svc['pool'] == pool
        ]
        if all_services:
            logger.warning(
                f'{colorama.Fore.RED}Consolidation mode for '
                f'{controller.controller_type} is disabled, but there are '
                f'still {len(all_services)} {noun}s running. Please terminate '
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


def is_lb_data_plane_auth_enabled() -> bool:
    """Whether inference requests require the LB-only bearer credential.

    New charts inject an explicit capability value. If it is absent during a
    mixed-version rollout, preserve the legacy behavior by treating configured
    data-plane token material as enabled. An explicit false is authoritative
    even while stale Secret files are being removed from an existing pod.
    """
    configured = os.environ.get(constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR)
    if configured is None:
        return bool(
            os.environ.get(constants.LB_AUTH_TOKENS_FILE_ENV_VAR) or
            os.environ.get(constants.LB_AUTH_TOKEN_ENV_VAR))
    if configured == 'true':
        return True
    if configured == 'false':
        return False
    raise AuthTokenConfigurationError(
        f'{constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR} must be exactly '
        '"true" or "false".')


def _get_auth_tokens(file_env_var: str,
                     legacy_token_env_var: Optional[str],
                     ring_name: str,
                     required: bool = False) -> Tuple[str, ...]:
    """Read a newline-delimited bearer-token ring without caching it.

    The configured file is authoritative and is read fresh on every call so a
    projected Secret rotation is live. A final newline is accepted; blank
    lines, whitespace-bearing/non-ASCII tokens, an empty file, and I/O/UTF-8
    errors are rejected instead of silently falling back to the legacy env
    token. When a legacy singleton env name is supplied, it is consulted only
    when no file is configured. Callers can omit that fallback for trust
    domains where sharing the legacy credential would be unsafe.
    """
    token_file = os.environ.get(file_env_var)
    if token_file:
        try:
            contents = pathlib.Path(token_file).expanduser().read_text(
                encoding='utf-8')
        except (OSError, UnicodeError) as e:
            raise AuthTokenConfigurationError(
                f'Cannot read {ring_name} token ring from {token_file!r}: '
                f'{common_utils.format_exception(e)}') from e
        tokens = tuple(contents.splitlines())
        if not tokens:
            raise AuthTokenConfigurationError(
                f'{ring_name} token ring {token_file!r} is empty.')
        for token in tokens:
            if _AUTH_TOKEN_PATTERN.fullmatch(token) is None:
                raise AuthTokenConfigurationError(
                    f'{ring_name} token ring {token_file!r} contains an '
                    'empty or malformed token.')
        return tokens

    legacy_token = (os.environ.get(legacy_token_env_var)
                    if legacy_token_env_var is not None else None)
    if legacy_token:
        if _AUTH_TOKEN_PATTERN.fullmatch(legacy_token) is None:
            assert legacy_token_env_var is not None
            raise AuthTokenConfigurationError(
                f'{legacy_token_env_var} contains a malformed token.')
        return (legacy_token,)
    if required:
        if legacy_token_env_var is not None:
            missing_sources = (f'neither {file_env_var} nor '
                               f'{legacy_token_env_var} is configured')
        else:
            missing_sources = f'{file_env_var} is not configured'
        raise AuthTokenConfigurationError(
            f'{ring_name} authentication is required, but '
            f'{missing_sources}.')
    return ()


def _get_controller_auth_token_rings(
        sync_required: bool = False,
        admin_required: bool = False
) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    """Read both controller rings and reject any cross-domain credential.

    Each ring may contain multiple credentials for an overlap rotation within
    that trust domain. A credential may never appear in both rings: otherwise
    an external LB holding the sync ring could invoke destructive controller
    administration routes. Both files are read on every call so an unsafe
    Secret rotation fails closed immediately, not only at process startup.

    The legacy singleton remains an admin-only fallback. In particular, it is
    never returned as an LB-sync credential.
    """
    sync_tokens = _get_auth_tokens(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                                   None,
                                   'load-balancer sync',
                                   required=sync_required)
    admin_tokens = _get_auth_tokens(
        constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
        constants.CONTROLLER_AUTH_TOKEN_ENV_VAR,
        'controller admin',
        required=admin_required)
    if not set(sync_tokens).isdisjoint(admin_tokens):
        raise AuthTokenConfigurationError(
            'Load-balancer sync and controller-admin token rings must be '
            'disjoint.')
    return sync_tokens, admin_tokens


def validate_controller_auth_token_isolation(required: bool = False) -> None:
    """Validate the controller trust-domain boundary without exposing tokens."""
    _get_controller_auth_token_rings(sync_required=required,
                                     admin_required=required)


def get_lb_sync_auth_tokens(required: bool = False) -> Tuple[str, ...]:
    """Credentials accepted on, and presented to, the LB sync endpoint."""
    sync_tokens, _ = _get_controller_auth_token_rings(sync_required=required)
    return sync_tokens


def get_controller_admin_auth_tokens(required: bool = False) -> Tuple[str, ...]:
    """Credentials accepted by trusted controller administration callers."""
    _, admin_tokens = _get_controller_auth_token_rings(admin_required=required)
    return admin_tokens


def get_lb_auth_tokens(required: bool = False) -> Tuple[str, ...]:
    """Credentials accepted by the external LB inference data plane."""
    return _get_auth_tokens(constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
                            constants.LB_AUTH_TOKEN_ENV_VAR,
                            'load-balancer data plane', required)


def ha_recovery_for_consolidation_mode(pool: bool,
                                       still_leader: Optional[Callable[
                                           [], bool]] = None):
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
        # Snapshot every service name known to the DB. In external-LB mode this
        # is the set of live services used below to reap orphaned LB objects
        # (LBs whose owning service row is gone). It is a superset (also
        # includes pools), which is safe: reconcile only deletes LBs NOT in the
        # set, and pools own no LB.
        all_service_names = serve_state.get_glob_service_names(None)
        for service_name in all_service_names:
            svc = _get_service_status(service_name,
                                      pool=pool,
                                      with_replica_info=False)
            if svc is None:
                # A raw service row without committed YAML is invisible to the
                # latest-version join and its recovery script cannot possibly
                # boot. Retire that script atomically instead of retrying an
                # immortal partial registration forever.
                raw_identity = serve_state.get_service_mode_and_hash(
                    service_name)
                if (raw_identity is not None and raw_identity[0] == pool and
                        isinstance(raw_identity[1], str) and raw_identity[1]):
                    retired = (
                        serve_state.mark_unrecoverable_service_for_cleanup(
                            service_name, raw_identity[1], pool))
                    if retired:
                        f.write(f'{capnoun} {service_name} has no committed '
                                'version; retired its unusable recovery '
                                'script and marked it for purge.\n')
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
                lb_k8s.reconcile_lb_objects(set(all_service_names))
            except Exception as e:  # pylint: disable=broad-except
                # Reconcile is best-effort cleanup; never let it abort recovery.
                f.write(f'Failed to reconcile external LB objects: {e}\n')
        f.write(f'HA recovery completed at {datetime.datetime.now()}\n')
        f.write(f'Total recovery time: {time.time() - start} seconds\n')


def _controller_process_alive(pid: int,
                              service_name: str,
                              service_incarnation: Optional[str] = None,
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
) -> Set[Tuple[str, Optional[str]]]:
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
    in_flight: Set[Tuple[str, Optional[str]]] = set()
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


def validate_service_task(task: 'sky.Task', pool: bool) -> None:
    """Validate the task for Sky Serve.

    Args:
        task: sky.Task to validate

    Raises:
        ValueError: if the arguments are invalid.
        RuntimeError: if the task.serve is not found.
    """
    spot_resources: List['sky.Resources'] = [
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

    # Try to create a spot placer from the task yaml. Check if the task yaml
    # is valid for spot placer.
    spot_placer.SpotPlacer.from_task(task.service, task)

    # [boltz fork] Reserved-capacity fill v1 arbitrates exactly ONE
    # Kubernetes (context, GPU) pool per service; the broker cycle rejects
    # multi-pool claims at runtime, but that surfaces only as a controller
    # error log -- reject at submit time too. Zero-cost-ness is not fully
    # knowable client-side (it needs per-location pricing on the
    # controller), so ALL Kubernetes entries are treated as candidate pool
    # shapes: the conservative superset (a Kubernetes entry is the
    # zero-cost tier in every supported fill topology). The runtime guard
    # stays as the backstop for specs that slip past (e.g. older clients).
    if task.service.reserved_capacity_fill:
        pool_shapes = set()
        for requested_resources in task.resources:
            if str(requested_resources.cloud).lower() != 'kubernetes':
                continue
            accelerators = requested_resources.accelerators or {}
            if not accelerators:
                continue
            gpu_name = next(iter(accelerators))
            pool_shapes.add((requested_resources.region, gpu_name.lower()))
        if len(pool_shapes) > 1:
            # key=repr: a shape's context may be None (no context pinned),
            # which does not order against strings.
            shapes = sorted(pool_shapes, key=repr)
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'reserved_capacity_fill supports exactly one Kubernetes '
                    '(context, GPU) pool per service; the resources span '
                    f'{shapes}. Keep a single Kubernetes '
                    'shape, or disable reserved_capacity_fill.')

    replica_ingress_port: Optional[int] = int(
        task.service.ports) if (task.service.ports is not None) else None
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
            requested_ports = list(
                resources_utils.port_ranges_to_set(requested_resources.ports))
            if len(requested_ports) != 1:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'To open multiple ports on the replica, please set the '
                        '`service.ports` field to specify a main service port. '
                        'Must only specify one port in resources otherwise. '
                        'Each replica will use the port specified as '
                        'application ingress port.')
            service_port = requested_ports[0]
            if replica_ingress_port is None:
                replica_ingress_port = service_port
            elif service_port != replica_ingress_port:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        f'Got multiple ports: {service_port} and '
                        f'{replica_ingress_port} in different resources. '
                        'Please specify the same port instead.')
        if pool:
            if (task.service.ports is not None or
                    requested_resources.ports is not None):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError('Cannot specify ports in a pool.')


def generate_service_name(pool: bool = False):
    noun = 'pool' if pool else 'service'
    return f'sky-{noun}-{uuid.uuid4().hex[:4]}'


def _resource_scope_tag(resource_scope: str, length: int = 20) -> str:
    """Filesystem/cloud-safe digest for an incarnation resource scope."""
    return hashlib.sha256(resource_scope.encode('utf-8')).hexdigest()[:length]


def generate_remote_service_dir_name(service_name: str,
                                     resource_scope: Optional[str] = None
                                    ) -> str:
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
                                            resource_scope: Optional[str] = None
                                           ) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'task.yaml.tmp')


def generate_task_yaml_file_name(service_name: str,
                                 version: int,
                                 expand_user: bool = True,
                                 resource_scope: Optional[str] = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    if expand_user:
        dir_name = os.path.expanduser(dir_name)
    return os.path.join(dir_name, f'task_v{version}.yaml')


def generate_remote_config_yaml_file_name(service_name: str,
                                          resource_scope: Optional[str] = None
                                         ) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'config.yaml')


def generate_remote_controller_log_file_name(
        service_name: str, resource_scope: Optional[str] = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'controller.log')


def generate_remote_batch_controller_log_file_name(
        service_name: str, resource_scope: Optional[str] = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'batch_controller.log')


def generate_replica_launch_log_file_name(
        service_name: str,
        replica_id: int,
        resource_scope: Optional[str] = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    dir_name = os.path.expanduser(dir_name)
    return os.path.join(dir_name, f'replica_{replica_id}_launch.log')


def generate_replica_log_file_name(service_name: str,
                                   replica_id: int,
                                   resource_scope: Optional[str] = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    dir_name = os.path.expanduser(dir_name)
    return os.path.join(dir_name, f'replica_{replica_id}.log')


def generate_replica_cluster_name(service_name: str,
                                  replica_id: int,
                                  resource_scope: Optional[str] = None) -> str:
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
    replica_infos: List['replica_managers.ReplicaInfo'],
    update_mode: UpdateMode,
    expected_service_hash: Optional[str] = None,
    expected_controller_owner: Optional[Tuple[Optional[int],
                                              Optional[str]]] = None
) -> None:
    record = serve_state.get_service_from_name(service_name)
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
    service_names = serve_state.get_glob_service_names(None)
    for service_name in service_names:
        record = _get_service_status(service_name,
                                     pool=pool,
                                     with_replica_info=False)
        if record is None:
            continue
        service_status = record['status']
        if service_status == serve_state.ServiceStatus.SHUTTING_DOWN:
            # Skip services that is shutting down.
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


def update_service_encoded(
        service_name: str,
        version: int,
        mode: str,
        pool: bool,
        expected_service_hash: Optional[str] = None,
        expected_lifecycle_epoch: Optional[int] = None) -> str:
    noun = 'pool' if pool else 'service'
    capnoun = noun.capitalize()
    service_status = _get_service_status(service_name, pool=pool)
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
    resp = _post_to_controller_with_retry(
        service_name,
        service_hash,
        '/controller/update_service',
        json=request_body,
        # See UPDATE_SERVICE_TIMEOUT_SECONDS: the handler may wait on the
        # replica-manager lock behind a slow probe round, so the default 10s
        # read timeout would spuriously fail the update. If even this
        # expires, the update still applies server-side once the lock frees.
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


def terminate_replica(service_name: str, replica_id: int, purge: bool) -> str:
    # TODO(tian): Currently pool does not support terminating replica.
    service_status = _get_service_status(service_name, pool=False)
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

    resp = _post_to_controller_with_retry(service_name,
                                          service_status['hash'],
                                          '/controller/terminate_replica',
                                          json={
                                              'replica_id': replica_id,
                                              'purge': purge,
                                          })

    message: str = resp.json()['message']
    if resp.status_code != 200:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Failed to terminate replica {replica_id} '
                             f'in {service_name}. Reason:\n{message}')
    return message


def get_yaml_content(service_name: str,
                     version: int,
                     resource_scope: Optional[str] = None) -> str:
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
    with open(latest_yaml_path, 'r', encoding='utf-8') as f:
        return f.read()


def _get_service_status(
        service_name: str,
        pool: bool,
        with_replica_info: bool = True,
        with_replica_counts: bool = False) -> Optional[Dict[str, Any]]:
    """Get the status dict of the service.

    Args:
        service_name: The name of the service.
        with_replica_info: Whether to include the information of all replicas.
        with_replica_counts: Whether to include a per-status replica count
            histogram (``replica_status_counts``). Cheaper than
            ``with_replica_info`` but not free (one pass over the replica
            rows), so internal callers that only need the service row
            should leave both off.

    Returns:
        A dictionary describing the status of the service if the service exists.
        Otherwise, return None.
    """
    record = serve_state.get_service_from_name(service_name)
    if record is None:
        return None
    if record['pool'] != pool:
        return None

    record['pool_yaml'] = ''
    if record['pool']:
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
                svc: Dict[str, Any] = original_config.pop('service')
                if svc is not None:
                    svc.pop('pool', None)  # Remove pool from service config
                    original_config['pool'] = svc  # Add pool to root config
            else:
                original_config = yaml_utils.safe_load(original_config)
            record['pool_yaml'] = yaml_utils.dump_yaml_str(original_config)

    record['target_num_replicas'] = 0
    try:
        resp = _get_to_controller_with_retry(service_name, record['hash'],
                                             '/autoscaler/info')
        record['target_num_replicas'] = resp.json()['target_num_replicas']
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
        # `to_info_dict` serialization below — a status histogram costs
        # one unpickle pass over the rows, no cluster-record joins, no
        # URL resolution. At fleet scale (hundreds of replicas) this is
        # the difference between a snappy summary and a 30s+ full query.
        status_counts: DefaultDict[str, int] = collections.defaultdict(int)
        for info in serve_state.get_replica_infos(service_name):
            status_counts[info.status.value] += 1
        record['replica_status_counts'] = dict(status_counts)

    if with_replica_info:
        replica_infos = serve_state.get_replica_infos(service_name)
        # Pre-fetch cluster records in one batched DB query instead of
        # letting each to_info_dict() do its own. With a long failure
        # history this was an N+1.
        cluster_names = [info.cluster_name for info in replica_infos]
        cluster_records = global_user_state.get_clusters_from_names(
            cluster_names)
        record['replica_info'] = [
            info.to_info_dict(
                with_handle=True,
                with_url=not pool,
                cluster_record=cluster_records[info.cluster_name],
            ) for info in replica_infos
        ]
        if pool:
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
    return record


def resolve_target_qps_for_gpu_shape(
        gpu_type: str, gpu_count: int,
        target_qps_per_replica: Dict[str, float]) -> Optional[float]:
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
        service_names: Optional[List[str]],
        pool: bool,
        summary_only: bool = False) -> List[Dict[str, str]]:
    if service_names is None:
        # Get all service names
        service_names = serve_state.get_glob_service_names(None)
    if not service_names:
        return []
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

    def _run_in_context(name: str) -> Optional[Dict[str, Any]]:
        return parent_ctx.copy().run(_get_service_status,
                                     name,
                                     pool=pool,
                                     with_replica_info=not summary_only,
                                     with_replica_counts=summary_only)

    max_workers = min(len(service_names), _STATUS_FANOUT_MAX_WORKERS)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        statuses = list(ex.map(_run_in_context, service_names))
    service_statuses: List[Dict[str, str]] = [{
        k: base64.b64encode(pickle.dumps(v)).decode('utf-8')
        for k, v in s.items()
    }
                                              for s in statuses
                                              if s is not None]
    return sorted(service_statuses, key=lambda x: x['name'])


# TODO (kyuds): remove when serve codegen is removed
def get_service_status_encoded(service_names: Optional[List[str]],
                               pool: bool,
                               summary_only: bool = False) -> str:
    # We have to use payload_type here to avoid the issue of
    # message_utils.decode_payload() not being able to correctly decode the
    # message with <sky-payload> tags.
    service_statuses = get_service_status_pickled(service_names,
                                                  pool,
                                                  summary_only=summary_only)
    return message_utils.encode_payload(service_statuses,
                                        payload_type='service_status')


def unpickle_service_status(
        payload: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    service_statuses: List[Dict[str, Any]] = []
    for service_status in payload:
        if not isinstance(service_status, dict):
            raise ValueError(f'Invalid service status: {service_status}')
        service_statuses.append({
            k: pickle.loads(base64.b64decode(v))
            for k, v in service_status.items()
        })
    return service_statuses


# TODO (kyuds): remove when serve codegen is removed
def load_service_status(payload: str) -> List[Dict[str, Any]]:
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
        service_name: str) -> List['replica_managers.ReplicaInfo']:
    logger.info(f'Get number of replicas for pool {service_name!r}')
    return [
        info for info in serve_state.get_replica_infos(service_name)
        if info.status == serve_state.ReplicaStatus.READY
    ]


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
        pool: str) -> Optional[Dict[str, Optional[resources_lib.Resources]]]:
    """Get free resources for each worker in a pool.

    Args:
        pool: Pool name (service name)

    Returns:
        Dictionary mapping cluster_name (worker) to free Resources object (or
        None if worker is not available or has no free resources).
    """

    free_resources: Dict[str, Optional[resources_lib.Resources]] = {}
    replicas = serve_state.get_replica_infos(pool)

    for replica_info in replicas:
        cluster_name = replica_info.cluster_name

        # Get cluster handle
        handle = replica_info.handle()
        if handle is None or handle.launched_resources is None:
            free_resources[cluster_name] = None
            continue

        total_resources = handle.launched_resources

        # Get job IDs running on this worker
        job_ids = managed_job_state.get_nonterminal_job_ids_by_pool(
            pool, cluster_name)

        if len(job_ids) == 0:
            free_resources[cluster_name] = total_resources
            continue

        # Get used resources
        # TODO(lloyd): We should batch the database calls here so that we
        # make a single call to get all the used resources for all the jobs.
        used_resources = managed_job_state.get_pool_worker_used_resources(
            set(job_ids))
        if used_resources is None:
            # We failed to get the used resources. We should return None since
            # we can't make any guarantees about what resources are being used.
            logger.warning(
                f'Failed to get used resources for cluster {cluster_name!r}')
            return None

        if _is_empty_resource(used_resources):
            # We encountered a job that has no resources specified. We
            # will not consider it for resource-aware scheduling so it must
            # be scheduled on its own. To do this we will set the free
            # worker resources to nothing by returning an empty resource
            # object.
            logger.debug(f'Job {job_ids} has no resources specified. '
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
    task_resources: Optional[typing.Union[
        'resources_lib.Resources', typing.Set['resources_lib.Resources'],
        typing.List['resources_lib.Resources']]] = None
) -> Optional[str]:
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
                                         with_replica_info=False)
    if service_status is None:
        logger.error(f'Service {service_name!r} does not exist.')
        return None
    if not service_status['pool']:
        logger.error(f'Service {service_name!r} is not a pool.')
        return None

    with filelock.FileLock(get_service_filelock_path(service_name)):
        free_resources = get_free_worker_resources(service_name)
        logger.debug(f'Free resources: {free_resources!r}')
        logger.debug(f'Get next cluster name for pool {service_name!r}')
        ready_replicas = get_ready_replicas(service_name)

        logger.debug(f'Ready replicas: {ready_replicas!r}')

        idle_replicas: List['replica_managers.ReplicaInfo'] = []

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
        resource_aware = resource_aware and free_resources is not None
        if free_resources is not None:
            for free_resource in free_resources.values():
                if free_resource is not None and not _is_empty_resource(
                        free_resource):
                    break
            else:
                resource_aware = False
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

                # Check if any of the task resource options fit
                fits = False
                for task_res in task_resources_list:
                    logger.debug(f'Task resources: {task_res!r}')
                    if _task_fits(task_res, free_resources_on_worker):
                        logger.debug(f'Task resources {task_res!r} fits'
                                     ' in free resources '
                                     f'{free_resources_on_worker!r}')
                        fits = True
                        break
                    else:
                        logger.debug(f'Task resources {task_res!r} does not fit'
                                     ' in free resources '
                                     f'{free_resources_on_worker!r}')
                if fits:
                    idle_replicas.append(replica_info)
        # Also fall back to resource unaware scheduling if no idle replicas are
        # found. This might be because our launched resources were improperly
        # set. If that's the case then jobs will fail to schedule in a resource
        # aware way because one of the resources will be `None` so we can just
        # fallback to 1 job per replica. If we are truly resource bottlenecked
        # then we will see that there are jobs running on the replica and will
        # not schedule another.
        if len(idle_replicas) == 0:
            logger.debug('Falling back to resource unaware scheduling')
            # Fall back to resource unaware scheduling if no task resources
            # are provided.
            for replica_info in ready_replicas:
                jobs_on_replica = (
                    managed_job_state.get_nonterminal_job_ids_by_pool(
                        service_name, replica_info.cluster_name))
                if not jobs_on_replica:
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
            assert free_resources is not None
            free_resources_on_worker = free_resources.get(
                replica_info.cluster_name)
            if free_resources_on_worker is not None:
                # Find which task resource fits on this worker
                for task_res in task_resources_list:
                    if _task_fits(task_res, free_resources_on_worker):
                        # Update full_resources in database to this specific
                        # resource
                        logger.debug(
                            f'Updating full_resources for job {job_id!r} '
                            f'to selected resource: {task_res!r}')
                        managed_job_state.update_job_full_resources(
                            job_id, task_res.to_yaml_config())
                        break

        managed_job_state.set_current_cluster_name(job_id,
                                                   replica_info.cluster_name)

        # Set infrastructure info for sorting/filtering
        handle = replica_info.handle()
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


def _terminate_failed_services(service_name: str,
                               expected_service_hash: Optional[str],
                               service_status: Optional[
                                   serve_state.ServiceStatus],
                               pool: bool = False) -> Optional[str]:
    """Terminate service in failed status.

    Failed-status services may still have a parent or recovering controller,
    so a file signal alone is not authoritative. Claim durable SHUTTING_DOWN,
    wait for an exact-owner child-teardown acknowledgement (or atomically claim
    a proven orphan), then terminate any remaining replicas and conditionally
    remove the exact service incarnation. Clusters that fail to terminate are
    reported as a potential resource leak.

    Returns:
        A message indicating potential resource leak (if any). If no
        resource leak is detected, return None.
    """
    if not expected_service_hash:
        return _purge_ownership_failure(service_name,
                                        'missing durable service hash')
    lifecycle_lock = get_service_lifecycle_lock(service_name)
    # Kept in the outer helper's compatibility signature for existing callers;
    # cleanup behavior is now fully determined by durable DB state.
    del service_status
    with lifecycle_lock:
        return _terminate_failed_services_locked(service_name,
                                                 expected_service_hash, pool,
                                                 lifecycle_lock)


def _terminate_failed_services_locked(
        service_name: str, expected_service_hash: str, pool: bool,
        lifecycle_lock: ServiceLifecycleLock) -> Optional[str]:
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
    owner = serve_state.get_service_controller_owner(service_name)
    if owner is None or owner.get('hash') != expected_service_hash:
        return _purge_ownership_failure(service_name,
                                        'owner disappeared before teardown')
    resource_scope = owner.get('resource_scope')
    if owner.get('controller_port') != constants.CONTROLLER_TEARDOWN_ACK_PORT:
        recovery_script = serve_state.get_ha_recovery_script(service_name)
        if recovery_script is None:
            # Legacy orphan/FAILED_CLEANUP rows may have no parent left to
            # acknowledge teardown. Absence of the recovery script is durable
            # proof that no controller can be (re)spawned for this row.
            claimed = serve_state.claim_orphaned_service_teardown(
                service_name, expected_service_hash,
                owner.get('controller_pid'), owner.get('controller_ip'),
                os.getpid(), os.environ.get('POD_IP'),
                expected_lifecycle_epoch=lifecycle_epoch)
        elif serve_state.get_latest_committed_version(service_name) is None:
            # A partial-registration row may retain a script but no committed
            # YAML. The script can never boot, so consume it atomically while
            # taking teardown ownership rather than waiting forever.
            claimed = serve_state.claim_unrecoverable_service_teardown(
                service_name, expected_service_hash,
                owner.get('controller_pid'), owner.get('controller_ip'),
                os.getpid(), os.environ.get('POD_IP'))
        else:
            claimed = None
        if claimed is False:
            return _purge_ownership_failure(
                service_name, 'orphan teardown claim lost ownership')

    owner_ack_deadline = time.time() + 10
    while True:
        owner = serve_state.get_service_controller_owner(service_name)
        if owner is None or owner.get('hash') != expected_service_hash:
            return _purge_ownership_failure(
                service_name, 'ownership changed while awaiting controller')
        if (owner.get('controller_port') ==
                constants.CONTROLLER_TEARDOWN_ACK_PORT):
            break
        if time.time() >= owner_ack_deadline:
            return (f'{colorama.Fore.YELLOW}failed service '
                    f'{service_name!r} could not be purged because its '
                    'controller has not yet acknowledged durable teardown; '
                    'cleanup remains scheduled and can be retried.'
                    f'{colorama.Style.RESET_ALL}')
        time.sleep(0.2)

    if not _still_owns():
        return _purge_ownership_failure(
            service_name, 'lifecycle lock or ownership lost after controller '
            'acknowledgement')

    # Fence the public data plane before *any* replica teardown.  The LB keeps
    # its last coherent routing view when controller sync stops, so reversing
    # this order accepts requests for clusters already being destroyed.  A
    # failed delete retains the exact row/name and aborts all cloud teardown.
    from sky.serve import lb_k8s  # pylint: disable=import-outside-toplevel
    if not pool:
        try:
            if resource_scope is None:
                lb_k8s.delete_lb_objects(
                    service_name,
                    expected_service_hash=expected_service_hash,
                    require_runtime=True)
            else:
                lb_k8s.delete_lb_objects(
                    service_name,
                    expected_service_hash=expected_service_hash,
                    resource_scope=resource_scope,
                    require_runtime=True)
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

    remaining_replica_clusters: List[str] = []
    replica_infos = serve_state.get_replica_infos(service_name)
    # The controller is dead (CONTROLLER_FAILED / FAILED_CLEANUP / zombie
    # SHUTTING_DOWN), so no down thread will ever run for these replicas:
    # terminate their clusters here, BEFORE dropping the DB rows. Deleting
    # the rows first (the old behavior) permanently orphaned any cluster
    # that still existed -- nothing referenced it anymore, so it kept
    # billing until manually downed.
    to_terminate = [
        info for info in replica_infos
        if global_user_state.cluster_with_name_exists(info.cluster_name)
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
                info: 'replica_managers.ReplicaInfo') -> Optional[str]:
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


def terminate_services(service_names: Optional[List[str]], purge: bool,
                       pool: bool) -> str:
    noun = 'pool' if pool else 'service'
    capnoun = noun.capitalize()
    service_names = serve_state.get_glob_service_names(service_names)
    terminated_service_names: List[str] = []
    messages: List[str] = []

    def _purge_completed(message: Optional[str]) -> bool:
        """Whether a failed-service purge removed its service row."""
        # Every fail-closed condition (owner/lock loss, cluster-down failure,
        # controller acknowledgement timeout, or LB deletion failure) retains
        # the exact row/name and marks that case explicitly. Only a completed
        # atomic removal counts as done.
        return message is None or 'could not be purged because' not in message

    for service_name in service_names:
        service_status = _get_service_status(service_name,
                                             pool=pool,
                                             with_replica_info=False)
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
                    message = _terminate_failed_services(service_name,
                                                         raw_identity[1],
                                                         None,
                                                         pool=pool)
                    if message is not None:
                        messages.append(message)
                    if _purge_completed(message):
                        terminated_service_names.append(f'{service_name!r}')
            continue
        if (service_status is not None and service_status['status']
                == serve_state.ServiceStatus.SHUTTING_DOWN):
            if purge:
                # Resume exact-owner cleanup for a zombie or a prior
                # fail-closed purge attempt. The first purge durably CASes the
                # row to SHUTTING_DOWN before touching replicas/LB/files; any
                # later failure deliberately keeps that row retryable here.
                message = _terminate_failed_services(
                    service_name,
                    service_status.get('hash'),
                    serve_state.ServiceStatus.SHUTTING_DOWN,
                    pool=pool)
                if message is not None:
                    messages.append(message)
                if _purge_completed(message):
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
                message = _terminate_failed_services(service_name,
                                                     service_status.get('hash'),
                                                     failed_status,
                                                     pool=pool)
                if message is not None:
                    messages.append(message)
                if _purge_completed(message):
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
            marked_for_teardown = (
                isinstance(expected_service_hash, str) and
                bool(expected_service_hash) and
                serve_state.set_service_status_and_active_versions_if_hash(
                    service_name, expected_service_hash,
                    serve_state.ServiceStatus.SHUTTING_DOWN))
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
             service_status['status'] != serve_state.ServiceStatus.SHUTTING_DOWN
            ) or not purge):
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


def wait_service_registration(service_name: str, job_id: int,
                              pool: bool) -> str:
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
    start_time = time.time()
    setup_completed = False
    noun = 'pool' if pool else 'service'
    while True:
        # Only do this check for non-consolidation mode as consolidation mode
        # has no setup process.
        if not is_consolidation_mode(pool):
            job_status = job_lib.get_status(job_id)
            if job_status is None or job_status < job_lib.JobStatus.RUNNING:
                # Wait for the controller process to finish setting up. It
                # can be slow if a lot cloud dependencies are being installed.
                if (time.time() - start_time >
                        constants.CONTROLLER_SETUP_TIMEOUT_SECONDS):
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
            # Reset the start time to wait for the service to be registered.
            start_time = time.time()

        record = _get_service_status(service_name,
                                     pool=pool,
                                     with_replica_info=False)
        if record is not None:
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
            controller_log_path = os.path.expanduser(
                generate_remote_controller_log_file_name(service_name))
            if os.path.exists(controller_log_path):
                with open(controller_log_path, 'r', encoding='utf-8') as f:
                    log_content = f.read()
                if (constants.MAX_NUMBER_OF_SERVICES_REACHED_ERROR
                        in log_content):
                    with ux_utils.print_exception_no_traceback():
                        raise RuntimeError(
                            controller_utils.get_max_services_error_message(
                                pool))
        elapsed = time.time() - start_time
        if elapsed > constants.SERVICE_REGISTER_TIMEOUT_SECONDS:
            # Print the controller log to help user debug.
            resource_scope = (record.get('resource_scope')
                              if record is not None else None)
            controller_log_path = (generate_remote_controller_log_file_name(
                service_name, resource_scope))
            with open(os.path.expanduser(controller_log_path),
                      'r',
                      encoding='utf-8') as f:
                log_content = f.read()
            with ux_utils.print_exception_no_traceback():
                raise ValueError(f'Failed to register service {service_name!r} '
                                 'on the SkyServe controller. '
                                 f'Reason:\n{log_content}')
        time.sleep(1)


def load_service_initialization_result(payload: str) -> int:
    return message_utils.decode_payload(payload)


def _check_service_status_healthy(service_name: str,
                                  pool: bool) -> Optional[str]:
    service_record = _get_service_status(service_name,
                                         pool,
                                         with_replica_info=False)
    capnoun = 'Service' if not pool else 'Pool'
    if service_record is None:
        return f'{capnoun} {service_name!r} does not exist.'
    if service_record['status'] == serve_state.ServiceStatus.CONTROLLER_INIT:
        return (f'{capnoun} {service_name!r} is still initializing its '
                'controller. Please try again later.')
    return None


def get_latest_version_with_min_replicas(
        service_name: str,
        replica_infos: List['replica_managers.ReplicaInfo']) -> Optional[int]:
    # Find the latest version with at least min_replicas replicas.
    version2count: DefaultDict[int, int] = collections.defaultdict(int)
    for info in replica_infos:
        if info.is_ready:
            version2count[info.version] += 1

    active_versions = sorted(version2count.keys(), reverse=True)
    for version in active_versions:
        spec = serve_state.get_spec(service_name, version)
        if (spec is not None and version2count[version] >= spec.min_replicas):
            return version
    # Use the oldest version if no version has enough replicas.
    return active_versions[-1] if active_versions else None


def _process_line(
        line: str,
        cluster_name: str,
        stop_on_eof: bool = False,
        streamed_provision_log_paths: Optional[set] = None) -> Iterator[str]:
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
            with open(p, 'r', newline='', encoding='utf-8') as f:
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
    idle_timeout_seconds: Optional[int] = None,
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
    log_list: List[str],
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
    all_lines: Deque[str] = collections.deque(maxlen=line_cap)
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
                        tail: Optional[int], pool: bool) -> str:
    msg = _check_service_status_healthy(service_name, pool=pool)
    if msg is not None:
        return msg
    repnoun = 'worker' if pool else 'replica'
    caprepnoun = repnoun.capitalize()
    print(f'{colorama.Fore.YELLOW}Start streaming logs for launching process '
          f'of {repnoun} {replica_id}.{colorama.Style.RESET_ALL}')
    record = serve_state.get_service_from_name(service_name)
    resource_scope = record.get('resource_scope') if record else None
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
            with open(log_file_name, 'r', encoding='utf-8') as f:
                print(f.read(), flush=True)
        return ''

    launch_log_file_name = generate_replica_launch_log_file_name(
        service_name, replica_id, resource_scope)
    if not os.path.exists(launch_log_file_name):
        return (f'{colorama.Fore.RED}{caprepnoun} {replica_id} doesn\'t exist.'
                f'{colorama.Style.RESET_ALL}')

    replica_infos = serve_state.get_replica_infos(service_name)
    matching_info = next(
        (info for info in replica_infos if info.replica_id == replica_id), None)
    recorded_cluster_name = (getattr(matching_info, 'cluster_name', None)
                             if matching_info is not None else None)
    replica_cluster_name = (recorded_cluster_name if isinstance(
        recorded_cluster_name, str) else generate_replica_cluster_name(
            service_name, replica_id, resource_scope))

    def _get_replica_status() -> serve_state.ReplicaStatus:
        for info in serve_state.get_replica_infos(service_name):
            if info.replica_id == replica_id:
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
        with open(launch_log_file_name, 'r', newline='', encoding='utf-8') as f:
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
                              follow: bool, tail: Optional[int],
                              pool: bool) -> str:
    msg = _check_service_status_healthy(service_name, pool)
    if msg is not None:
        return msg
    if not stream_controller:
        if pool:
            return 'Pools do not have a load balancer.'
        # Lazy import avoids the lb_k8s -> serve_utils module cycle. External-
        # only SkyServe writes LB output to the Kubernetes Pod log, never the
        # legacy controller-local load_balancer.log file.
        from sky.serve import lb_k8s  # pylint: disable=import-outside-toplevel
        return lb_k8s.stream_lb_logs(service_name, follow, tail)
    record = serve_state.get_service_from_name(service_name)
    resource_scope = record.get('resource_scope') if record else None
    log_file = generate_remote_controller_log_file_name(service_name,
                                                        resource_scope)

    def _service_is_terminal() -> bool:
        record = _get_service_status(service_name,
                                     pool,
                                     with_replica_info=False)
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
        with open(os.path.expanduser(log_file),
                  'r',
                  newline='',
                  encoding='utf-8') as f:
            for line in log_utils.follow_logs(
                    f,
                    should_stop=_service_is_terminal,
                    stop_on_eof=not follow,
            ):
                print(line, end='', flush=True)
    return ''


# ================== Table Formatter for `sky serve status` ==================


def _get_replicas(service_record: Dict[str, Any]) -> str:
    ready_replica_num, total_replica_num = 0, 0
    for info in service_record['replica_info']:
        if info['status'] == serve_state.ReplicaStatus.READY:
            ready_replica_num += 1
        # TODO(MaoZiming): add a column showing failed replicas number.
        if info['status'] not in serve_state.ReplicaStatus.failed_statuses():
            total_replica_num += 1
    return f'{ready_replica_num}/{total_replica_num}'


def format_service_table(service_records: List[Dict[str, Any]], show_all: bool,
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

    replica_infos: List[Dict[str, Any]] = []
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


def _format_replica_table(replica_records: List[Dict[str, Any]], show_all: bool,
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
            replica_handle: Optional[
                'backends.CloudVmRayResourceHandle'] = record.get('handle')
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
    def get_service_status(cls,
                           service_names: Optional[List[str]],
                           pool: bool,
                           summary_only: bool = False) -> str:
        # summary_only is only forwarded to controllers whose lib version
        # understands it (v6+); older controllers just return the full
        # payload — a graceful degradation, never an error.
        code = [
            f'kwargs={{}} if serve_version < 3 else {{"pool": {pool}}}',
            ('kwargs.update({"summary_only": '
             f'{summary_only}}}) if serve_version >= 6 else None'),
            f'msg = serve_utils.get_service_status_encoded({service_names!r}, '
            '**kwargs)', 'print(msg, end="", flush=True)'
        ]
        return cls._build(code)

    @classmethod
    def add_version(cls, service_name: str) -> str:
        code = [
            f'msg = serve_utils.add_version_encoded({service_name!r})',
            'print(msg, end="", flush=True)'
        ]
        return cls._build(code)

    @classmethod
    def terminate_services(cls, service_names: Optional[List[str]], purge: bool,
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
                            follow: bool, tail: Optional[int],
                            pool: bool) -> str:
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
                                  tail: Optional[int], pool: bool) -> str:
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
    def _build(cls, code: List[str]) -> str:
        code = cls._PREFIX + code
        generated_code = '; '.join(code)
        # Use the local user id to make sure the operation goes to the correct
        # user.
        return (f'export {skylet_constants.USER_ID_ENV_VAR}='
                f'"{common_utils.get_user_hash()}"; '
                f'{skylet_constants.SKY_PYTHON_CMD} '
                f'-u -c {shlex.quote(generated_code)}')
