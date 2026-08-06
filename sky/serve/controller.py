"""SkyServeController: the central controller of SkyServe.

Responsible for autoscaling and replica management.
"""
import asyncio
from collections.abc import Callable
import contextlib
import contextvars
import functools
import hashlib
import hmac
import logging
import math
import os
import re
import signal
import socket
import threading
import time
import traceback
from typing import Any, NamedTuple
import uuid

import colorama
import fastapi
from fastapi import responses
import filelock
import uvicorn

from sky import exceptions
from sky import global_user_state
from sky import serve
from sky import sky_logging
from sky import skypilot_config
from sky import task as task_lib
from sky.serve import auth_tokens
from sky.serve import autoscalers
from sky.serve import constants as serve_constants
from sky.serve import controller_history
from sky.serve import lb_ha
from sky.serve import lb_ha_observability as lb_ha_obs
from sky.serve import lb_k8s
from sky.serve import paid_capacity
from sky.serve import provider_phase
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.serve import system_recovery_route_lease
from sky.serve import system_recovery_state
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import context as sky_context
from sky.utils import context_utils
from sky.utils import thread_utils
from sky.utils import ux_utils
from sky.utils.db import db_utils

logger = sky_logging.init_logger(__name__)
# Keep the historical controller-module patch surface for tests and plugins.
serve_history = controller_history.serve_history


def _catalog_missing_task_contexts(
        yaml_content: str, placement_catalog: dict[str, Any]) -> set[str]:
    """Kubernetes contexts the task declares that the catalog does not carry.

    Compared on the context name alone. A catalog entry pins cloud, region and
    a purchase model; the question here is only whether the enumeration ever
    considered this context, so anything narrower would report a spurious
    mismatch on an unrelated field.

    Best effort by construction: a task or catalog this cannot parse yields an
    empty set, which preserves the previous reuse behavior rather than failing
    an update on a parsing difference.
    """
    try:
        task = task_lib.Task.from_yaml_str(yaml_content)
    except Exception:  # pylint: disable=broad-except
        return set()

    declared: set[str] = set()
    for resources in (task.resources or []):
        cloud = getattr(resources, 'cloud', None)
        if cloud is None or str(cloud).lower() != 'kubernetes':
            continue
        region = getattr(resources, 'region', None)
        if isinstance(region, str) and region:
            declared.add(region)
    if not declared:
        return set()

    present: set[str] = set()

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            cloud = node.get('cloud')
            region = node.get('region')
            if (isinstance(cloud, str) and cloud.lower() == 'kubernetes' and
                    isinstance(region, str) and region):
                present.add(region)
            for value in node.values():
                _walk(value)
        elif isinstance(node, list):
            for value in node:
                _walk(value)

    _walk(placement_catalog)
    return declared - present


def _uses_current_request_classification_protocol(
        request_data: dict[str, Any]) -> bool:
    """Whether one LB payload declares the controller's current protocol."""
    classification_history = request_data.get('request_classification_history')
    if not isinstance(classification_history, dict):
        return False
    version = classification_history.get('classification_version')
    return (isinstance(version, int) and not isinstance(version, bool) and
            version == serve_history.REQUEST_CLASSIFICATION_PROTOCOL_VERSION)


class _PreparedLoadBalancerReport(NamedTuple):
    authority: tuple[bool, bool, bool]
    effective_request_data: dict[str, Any]
    ha_enabled: bool


class _PreparedControllerConfig(NamedTuple):
    """Validated, immutable inputs for one config-aware update generation."""

    config: Any
    service_name: str
    live_path: str
    staged_path: str
    recovery_script: str
    version: int
    snapshot_id: str
    source_digest: str
    durable_bytes: bytes
    durable_digest: str
    source_is_staged: bool
    source_is_live: bool
    legacy_snapshot: tuple[bytes, str, str] | None


def _make_auth_dependency(*,
                          sync: bool = False,
                          required: bool = False) -> Callable:
    """Build a dependency for one purpose-specific controller token ring.

    Rings are read fresh for every request, so mounted Secret rotations do not
    require a controller restart. The sync endpoint accepts only the LB-sync
    ring; administrative/status endpoints accept only the admin ring. A broken
    required ring returns 503 (fail closed without claiming the caller merely
    supplied a bad credential); a present but nonmatching credential returns
    401.
    """

    async def _verify(authorization: str | None = fastapi.Header(None)) -> None:
        getter = (serve_utils.get_lb_sync_auth_tokens
                  if sync else serve_utils.get_controller_admin_auth_tokens)
        try:
            expected_tokens = getter(required=required)
        except serve_utils.AuthTokenConfigurationError as e:
            logger.error('Controller authentication is unavailable: %s', e)
            raise fastapi.HTTPException(
                status_code=503,
                detail='Controller authentication is unavailable.') from e
        if not expected_tokens:
            return
        # isascii() guards hmac.compare_digest, which raises TypeError on a
        # non-ASCII str -- a malformed header must be a clean 401, not a 500.
        if authorization is None or not authorization.isascii():
            raise fastapi.HTTPException(status_code=401, detail='Unauthorized.')
        authorized = False
        for expected_token in expected_tokens:
            # Evaluate every ring member instead of short-circuiting on the
            # first match, keeping request timing independent of token order.
            authorized |= hmac.compare_digest(authorization,
                                              f'Bearer {expected_token}')
        if not authorized:
            raise fastapi.HTTPException(status_code=401, detail='Unauthorized.')

    return _verify


def _make_controller_owner_dependency(
        controller_owner_fingerprint: str) -> Callable:
    """Fence every child request to the exact controller owner tuple."""

    async def _verify(requested_owner: str | None = fastapi.Header(
        None, alias=serve_constants.CONTROLLER_OWNER_HEADER)) -> None:
        if requested_owner != controller_owner_fingerprint:
            raise fastapi.HTTPException(
                status_code=409, detail='Controller owner identity mismatch.')

    return _verify


def _validate_terminate_replica_payload(request_data: Any) -> tuple[int, bool]:
    """Validate the destructive replica-termination request payload."""
    if not isinstance(request_data, dict):
        raise fastapi.HTTPException(
            status_code=400,
            detail='Replica termination payload must be an object.')
    replica_id = request_data.get('replica_id')
    # bool is a subclass of int, so isinstance(True, int) is unsafe here.
    if type(replica_id) is not int:
        raise fastapi.HTTPException(status_code=400,
                                    detail='Replica ID must be an integer.')
    purge = request_data.get('purge')
    if type(purge) is not bool:
        raise fastapi.HTTPException(status_code=400,
                                    detail='Purge must be a boolean.')
    return replica_id, purge


async def _read_terminate_replica_payload(
        request: fastapi.Request) -> tuple[int, bool]:
    """Read and validate a replica-termination request body."""
    try:
        request_data = await request.json()
    except ValueError as e:
        raise fastapi.HTTPException(
            status_code=400,
            detail='Replica termination payload must be valid JSON.') from e
    return _validate_terminate_replica_payload(request_data)


def _get_replica_info_for_termination(
        service_name: str, replica_id: int) -> replica_managers.ReplicaInfo:
    """Return the requested replica or a stable client-facing 404."""
    replica_info = serve_state.get_replica_info_from_id(service_name,
                                                        replica_id)
    if replica_info is None:
        raise fastapi.HTTPException(
            status_code=404, detail=f'Replica {replica_id} does not exist.')
    return replica_info


def _terminate_replica_sync(service_name: str,
                            replica_manager: replica_managers.ReplicaManager,
                            replica_id: int, purge: bool) -> fastapi.Response:
    """Durably schedule one replica teardown off the controller event loop."""
    replica_info = _get_replica_info_for_termination(service_name, replica_id)
    replica_status = replica_info.status

    if replica_status == serve_state.ReplicaStatus.SHUTTING_DOWN:
        return responses.JSONResponse(
            status_code=409,
            content={
                'message':
                    f'Replica {replica_id} of service {service_name!r} is '
                    'already in the process of terminating. Skip terminating '
                    'now.'
            })

    if (replica_status in serve_state.ReplicaStatus.failed_statuses() and
            not purge):
        return responses.JSONResponse(
            status_code=409,
            content={
                'message': f'{colorama.Fore.YELLOW}Replica {replica_id} of '
                           f'service {service_name!r} is in failed status '
                           f'({replica_info.status}). Skipping its termination '
                           'as it could lead to a resource leak. '
                           f'(Use `sky serve down {service_name!r} '
                           f'--replica-id {replica_id} --purge` to forcefully '
                           f'terminate the replica.){colorama.Style.RESET_ALL}'
            })

    # This may wait behind a fleet-wide recovery/probe/placement lock. Keep it
    # on the executor thread, but do not acknowledge until scale_down() has
    # durably scheduled the owner-fenced teardown.
    replica_manager.scale_down(replica_id, purge=purge)

    action = 'terminated' if not purge else 'purged'
    message = (f'{colorama.Fore.GREEN}Replica {replica_id} of service '
               f'{service_name!r} is scheduled to be '
               f'{action}.{colorama.Style.RESET_ALL}\n'
               f'Please use {ux_utils.BOLD}sky serve status '
               f'{service_name}{ux_utils.RESET_BOLD} '
               f'to check the latest status.')
    return responses.JSONResponse(status_code=200, content={'message': message})


def _read_declared_submitted_yaml(request_data: dict[str, Any],
                                  service_name: str, version: int,
                                  resource_scope: str | None) -> str | None:
    """Read only the submitted YAML declared by this update request."""
    if request_data.get('has_submitted_yaml') is not True:
        return None
    path = serve_utils.generate_submitted_task_yaml_file_name(
        service_name, version, resource_scope=resource_scope)
    try:
        with open(path, encoding='utf-8') as f:
            return f.read()
    except OSError as e:
        logger.warning(
            'Submitted YAML declared for service %r version %s '
            'is unavailable at %s: %s', service_name, version, path, e)
        return None


class AutoscalerInfoFilter(logging.Filter):

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not ('GET' in message and '200' in message and
                    '/autoscaler/info' in message)


class _PendingServiceUpdate(NamedTuple):
    version: int
    service: Any
    update_mode: serve_utils.UpdateMode
    committed_at: float
    prepared_config: _PreparedControllerConfig | None


_UPDATE_RETRY_BACKOFF_SECONDS = 5


class DeterministicServiceUpdateError(ValueError):
    """An immutable committed spec cannot be applied by this controller."""


class ServiceUpdateRequiresRecoveryError(RuntimeError):
    """A config/runtime transition began and cannot be retried in process."""


class SkyServeController:
    """SkyServeController: control everything about replica.

    This class is responsible for:
        - Starting and terminating the replica monitor and autoscaler.
        - Providing the HTTP Server API for SkyServe to communicate with.
    """

    def __init__(self,
                 service_name: str,
                 service_spec: serve.SkyServiceSpec,
                 version: int,
                 host: str,
                 port: int,
                 controller_owner_fingerprint: str,
                 resource_scope: str | None = None,
                 service_hash: str | None = None,
                 controller_pid: int | None = None,
                 controller_ip: str | None = None,
                 enforce_launch_fence: bool = False) -> None:
        self._service_name = service_name
        self._resource_scope = resource_scope
        self._service_hash = service_hash
        self._history_session_id = uuid.uuid4().hex
        self._controller_owner = ((controller_pid,
                                   controller_ip) if service_hash is not None or
                                  controller_pid is not None or
                                  controller_ip is not None else None)
        if (service_hash is not None and
                serve_state.service_uses_logical_replica_semantics(service_name)
                and getattr(service_spec, 'uses_logical_replicas',
                            False) is not True):
            raise RuntimeError(
                'Refusing to recover a service whose durable logical-replica '
                'activation fence disagrees with its latest specification.')
        # Serialize durable update commits. The live manager/autoscaler
        # transition happens on the reconciler below: a fleet-wide probe can
        # hold the replica-manager lock for minutes, but a second update must
        # still be able to commit while the first transition waits for it.
        self._update_lock = threading.Lock()
        self._update_condition = threading.Condition()
        self._pending_update: _PendingServiceUpdate | None = None
        self._applying_update: _PendingServiceUpdate | None = None
        # Recovery boots the latest applicable version. Preserve the highest
        # committed version separately so a quarantined candidate remains
        # visible without becoming runtime authority.
        self._committed_version = (
            serve_state.get_latest_committed_version(service_name) or version)
        self._applied_version = version
        latest_quarantine = serve_state.get_latest_quarantined_version(
            service_name)
        self._quarantined_version = (latest_quarantine['version']
                                     if latest_quarantine is not None else None)
        self._quarantined_at = (latest_quarantine['quarantined_at']
                                if latest_quarantine is not None else None)
        self._quarantine_reason = (latest_quarantine['quarantine_reason']
                                   if latest_quarantine is not None else None)
        # Publish the autoscaler catalog, routing spec, and applied version as
        # one compatibility epoch.  LB demand ingestion takes the same lock so
        # an old-version report can never be validated before an update and
        # collected against the new exact-card catalog after it.
        self._routing_state_lock = threading.RLock()
        self._update_apply_error: str | None = None
        self._update_apply_failures = 0
        self._update_recovery_required = False
        self._update_reconciler_stop = threading.Event()
        # Serialize every autoscaler actuation epoch with a controller-config
        # transition. If a transition fails after publishing any new runtime
        # state, its catch path raises the irreversible stop fence before this
        # lock is released; an old or partially updated decision can therefore
        # never resume scaling while the parent prepares a fresh child.
        self._actuation_epoch_lock = threading.RLock()
        self._actuation_stop = threading.Event()
        # Serialize LB snapshots while resolving a cold replica cache in the
        # threadpool. Concurrent LB Pods can overlap during a rollout; without
        # this lock they would duplicate the fleet-wide endpoint work and race
        # to replace the shared routing/translation caches. Create the asyncio
        # lock lazily inside the running server loop: on Python 3.9 eager lock
        # construction fails if an earlier asyncio.run() closed the thread's
        # current loop.
        self._lb_sync_lock: asyncio.Lock | None = None
        self._lb_role_lock: asyncio.Lock | None = None
        self._lb_demand_lock: asyncio.Lock | None = None
        durable_lb_state = (serve_state.get_lb_cutover_state(service_name)
                            if service_hash is not None else None)
        self._lb_ha_enabled = (
            durable_lb_state.enabled if durable_lb_state is not None else
            getattr(service_spec, 'lb_high_availability', False) is True)
        self._lb_session_ledger = (lb_ha.LbSessionLedger(
            serve_constants.LB_ROLE_REPORT_MAX_AGE_SECONDS,
            serve_constants.LB_PROMOTION_OCCUPANCY_MAX_AGE_SECONDS)
                                   if self._lb_ha_enabled else None)
        self._lb_expected_occupancy_urls: set[str] = set()
        # An empty set means "synchronous service" only after one complete
        # routing sync. Before that, it means "contract unknown" and must not
        # make promotion vacuously safe after a controller restart.
        self._lb_occupancy_contract_known = False
        self._lb_last_demand_snapshot = (
            serve_state.get_lb_last_demand_snapshot(service_name)
            if self._lb_ha_enabled else None)
        self._lb_demand_handoff = lb_ha.DemandHandoff(
            serve_constants.LB_DEMAND_HANDOFF_SECONDS)
        self._lb_drain_timeout_seconds = (
            lb_k8s.lb_termination_grace_period_seconds(
                service_spec.lb_stream_timeout_seconds,
                service_spec.graceful_drain_seconds)
            if self._lb_ha_enabled else 0)
        self._controller_owner_fingerprint = controller_owner_fingerprint
        self._is_pool = service_spec.pool
        self._replica_manager: replica_managers.ReplicaManager = (
            replica_managers.SkyPilotReplicaManager(
                service_name=service_name,
                spec=service_spec,
                version=version,
                resource_scope=resource_scope,
                service_hash=service_hash,
                controller_pid=controller_pid,
                controller_ip=controller_ip,
                enforce_launch_fence=enforce_launch_fence))
        # Pass `version` so a controller rebuilt on restart/respawn starts the
        # autoscaler at the recovered latest version (matching the replica
        # manager above), not INITIAL_VERSION. Otherwise a service updated past
        # v1 would have its autoscaler treat every live replica as outdated and
        # churn replicas forever after any restart.
        self._autoscaler: autoscalers.Autoscaler = (
            autoscalers.Autoscaler.from_spec(service_name, service_spec,
                                             version))
        self._autoscaler.set_spot_placer(self._replica_manager.spot_placer)
        try:
            placement_policy_states = (
                serve_state.get_service_placement_policy_states(service_name))
        except Exception as e:  # pylint: disable=broad-except
            # This state is an economic optimization fence, not part of the
            # serving data path. Starting with no elapsed stabilization is the
            # conservative fallback: it cannot authorize an early replacement
            # and lets a recovering controller become healthy while the next
            # successful write re-establishes durable state.
            logger.warning(
                'Could not restore cost-rebalance stabilization; restarting '
                'the candidate window: '
                f'{common_utils.format_exception(e)}')
            placement_policy_states = None
        self._autoscaler.load_cost_rebalance_state(
            None if placement_policy_states is
            None else placement_policy_states['cost_rebalance_state'])
        self._configure_instance_aware_accelerators(service_spec)
        # [boltz fork] Reserved-capacity fill poller lifecycle: started
        # from run() when the service booted with the flag on, and
        # lazily from update_service when an update enables the flag on
        # a live service (idempotent -- at most one poller thread; a
        # flag toggled OFF leaves the thread alive but dormant, see
        # poller_loop). The poller is the only component allowed to
        # issue the expensive cluster-wide realtime free-GPU query.
        self._reserved_capacity_fill_enabled: bool = bool(
            getattr(service_spec, 'reserved_capacity_fill', False))
        self._reserved_capacity_poller_started: bool = False
        # update_service handlers run in FastAPI's threadpool, so two
        # concurrent fill-enabling updates could both observe the
        # started flag as False; the lock makes start-once atomic.
        self._reserved_capacity_poller_lock = threading.Lock()
        # Seed the zero-cost location set synchronously, before run()
        # starts the autoscaler thread: a respawned controller's
        # autoscaler boots with empty fill state (from_spec above; there
        # is no cross-process dump/load) and its first decision tick can
        # beat the first poll by a lot (per-location cost warm-up + the
        # cluster-wide realtime query). Without the seed, a QPS-family
        # autoscaler's first tick computes target=min_replicas from its
        # empty window and, with zero_cost_count=0, suppression cannot
        # shelter the live fill fleet -- the whole fill surplus would be
        # mass-terminated. Seeding grants NO free slots (snapshot time
        # stays None), so no new fill launches until the first real poll.
        self._seed_fill_zero_cost_locations(self._autoscaler)
        self._host = host
        self._port = port
        # [boltz fork] Cache of replica_id -> (url, gpu_type, gpu_count)
        # for the
        # load_balancer_sync response. Both fields require a cluster handle
        # fetch (and, for the url, an endpoint query) and are fixed for a
        # replica's lifetime once it is READY, so they are resolved at most
        # once per replica. The cache is rebuilt from the currently active
        # replicas on every sync, which prunes replicas that are no longer
        # READY; a replica that recovers with a new endpoint is thus
        # re-resolved.
        self._lb_replica_cache: dict[int, tuple[str, str, int]] = {}
        self._lb_replica_cache_record_ids: dict[int, str] = {}
        # Superset of _lb_replica_cache for url -> replica_id translation
        # of the LB's in-flight report: keeps entries for replicas that
        # left READY but are still nonterminal, so a probe-blipped
        # replica's running job stays attributed to it (see
        # _get_lb_replica_info / _translate_in_flight).
        self._lb_translation_cache: dict[int, tuple[str, str, int]] = {}
        self._lb_translation_cache_record_ids: dict[int, str] = {}
        # Monotonic generation for complete LB demand/capacity reports. It is
        # intentionally process-local: after restart logical scale-down stays
        # disabled until the new controller consumes a fresh report.
        self._reconcile_generation = 0
        # Immutable routing configuration shipped to the external load
        # balancer. Stored in-memory and updated only after the controller's
        # live autoscaler / replica-manager state transitions, so syncs never
        # advertise a newer routing policy than the runtime has actually
        # applied.
        self._routing_spec = self._build_routing_spec(service_spec)
        # Refreshed only by autoscaler/LB-sync paths that already hold a full
        # replica snapshot. Status polling reads this without new DB/API work.
        self._replica_counts_snapshot: dict[str, int | str] | None = None
        self._app = fastapi.FastAPI(lifespan=self.lifespan)
        if not self._mark_controller_applied_version(version):
            raise RuntimeError(
                f'Could not durably mark recovered service version {version} '
                'as controller-applied under the current ownership fence.')

    @contextlib.asynccontextmanager
    async def lifespan(self, _: fastapi.FastAPI):
        if auth_tokens.is_resource_action_authority_enabled():
            # Refuse to publish controller routes if the private authority
            # credential overlaps any other ring mounted in this process.
            # Reads remain fresh in the request client so later projected
            # Secret rotations keep the same fail-closed boundary.
            auth_tokens.validate_resource_action_preflight_auth_token_isolation(
                required=True)
        uvicorn_access_logger = logging.getLogger('uvicorn.access')
        for handler in uvicorn_access_logger.handlers:
            handler.setFormatter(sky_logging.FORMATTER)
            handler.addFilter(AutoscalerInfoFilter())
        yield

    def _seed_fill_zero_cost_locations(
            self, autoscaler: autoscalers.Autoscaler) -> None:
        """Seed centralized zero-cost identities without provider calls.

        The complete version catalog was materialized before the controller
        child started. The seed grants no free slots and records no snapshot
        time; it only protects the known fill fleet from the first autoscaler
        tick. An already-populated set (e.g. loaded from a dump) is never
        overwritten -- see Autoscaler.seed_zero_cost_locations.
        """
        if not autoscaler.reserved_capacity_fill:
            return
        placer = self._replica_manager.spot_placer
        if placer is None:
            return
        try:
            autoscaler.seed_zero_cost_locations([
                location.to_pickleable()
                for location in placer.zero_cost_locations()
            ])
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to seed cataloged zero-cost locations '
                           '(best-effort; will rely on the first successful '
                           'poll instead): '
                           f'{common_utils.format_exception(e)}')

    def _persist_cost_rebalance_state(
            self, autoscaler: autoscalers.Autoscaler) -> bool:
        """Persist stabilization before authorizing economic scale-up."""
        if not autoscaler.cost_rebalance_state_dirty:
            return True
        service_hash = getattr(self, '_service_hash', None)
        if service_hash is None:
            autoscaler.mark_cost_rebalance_state_persisted()
            return True
        try:
            persisted = serve_state.set_service_cost_rebalance_state(
                self._service_name, service_hash,
                getattr(self, '_controller_owner', None),
                autoscaler.dump_cost_rebalance_state())
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(
                'Could not persist cost-rebalance stabilization; suppressing '
                'new economic replacements for this tick: '
                f'{common_utils.format_exception(e)}')
            return False
        if persisted:
            autoscaler.mark_cost_rebalance_state_persisted()
        return persisted

    def _get_lb_replica_info(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        async_occupancy_by_version: dict[int, bool | None] | None = None,
    ) -> tuple[dict[str, dict[str, str]], int]:
        """Build the url -> replica info mapping for load_balancer_sync.

        [boltz fork] Resolving a replica's url and gpu_type is expensive, so
        cluster records and provider configs for newly-READY replicas are each
        fetched in one batched lookup. The resulting endpoint/accelerator data
        is cached for the replica's lifetime. Ordinary warm-cache rows perform
        neither lookup. Protocol-v2 reserved-fill rows re-read their durable
        handle and re-prove the physical Kubernetes identity on every sync;
        rows sharing one physical pool reuse one UID fence/read for the round.
        A brand-new replica whose gpu_type cannot be resolved yet is reported
        as 'unknown' until it is.

        `replica_infos` is fetched once by the caller and shared with the
        capacity-hint computation, so the async sync handler issues no
        extra replica-list DB reads.

        Returns the (url -> info) mapping and the number of identity-verified
        READY, active replicas seen -- which can exceed len(mapping) when a
        verified replica's endpoint is transiently unresolvable this round.
        The load balancer uses that count to tell an authoritative zero (no
        verified READY replicas) apart from a spurious empty map (verified
        replicas exist but none resolved), so a physical-cluster retarget
        explicitly clears old routes instead of preserving them as transient.
        """
        runtime_snapshot = serve_state.get_service_runtime_snapshot(
            self._service_name, require_version=True)
        assert runtime_snapshot is not None, ('No service record found for '
                                              f'{self._service_name}')
        service_hash = getattr(self, '_service_hash', None)
        controller_owner = getattr(self, '_controller_owner', None)
        if (service_hash is not None and
            (runtime_snapshot.get('hash') != service_hash or
             (runtime_snapshot.get('controller_pid'),
              runtime_snapshot.get('controller_ip')) != controller_owner)):
            raise RuntimeError('Controller ownership changed while building '
                               'the load balancer routing snapshot.')
        active_versions = set(runtime_snapshot['active_versions'])
        replica_cache: dict[int, tuple[str, str, int]] = {}
        replica_cache_record_ids: dict[int, str] = {}
        replica_info: dict[str, dict[str, str]] = {}
        resolved_url_sources: dict[str, replica_managers.ReplicaInfo] = {}
        ready_infos = [
            info for info in replica_infos
            if (info.status == serve_state.ReplicaStatus.READY and
                info.version in active_versions and
                self._replica_manager.system_recovery_allows_routing(info))
        ]

        def _is_recovery_capable(info: 'replica_managers.ReplicaInfo') -> bool:
            return (info.system_recovery_disposition ==
                    system_recovery_state.SystemRecoveryDisposition.CAPABLE)

        def _retire_unverified_route(info: 'replica_managers.ReplicaInfo',
                                     error: Exception) -> None:
            if _is_recovery_capable(info):
                self._replica_manager.retire_system_recovery_route(info)
            logger.error(
                'Withholding READY replica %s because its physical '
                'Kubernetes identity could not be verified: %s',
                info.replica_id, common_utils.format_exception(error))

        def _cached_route(
            info: 'replica_managers.ReplicaInfo'
        ) -> tuple[str, str, int] | None:
            cached = self._lb_replica_cache.get(info.replica_id)
            if getattr(self, '_lb_replica_cache_record_ids',
                       {}).get(info.replica_id) != info.replica_record_id:
                return None
            return cached

        # Strictly classify every candidate before deciding that a warm cache
        # can avoid its durable-handle lookup. A malformed row must never
        # degrade to the ordinary/legacy path.
        cleanup_fences: dict[int, reserved_capacity.ProtocolV2CleanupFence |
                             None] = {}
        identity_rejected_ids: set[int] = set()
        for info in ready_infos:
            try:
                cleanup_fences[id(info)] = (
                    reserved_capacity.parse_protocol_v2_cleanup_fence(info))
            except exceptions.KubernetesPhysicalClusterIdentityError as e:
                identity_rejected_ids.add(id(info))
                _retire_unverified_route(info, e)

        cluster_lookup_infos = [
            info for info in ready_infos
            if id(info) not in identity_rejected_ids and (cleanup_fences[id(
                info)] is not None or _cached_route(info) is None)
        ]
        cluster_names = list(
            dict.fromkeys(info.cluster_name for info in cluster_lookup_infos))
        cluster_records: dict[str, dict[str, Any] | None] = {}
        if cluster_names:
            cluster_records = global_user_state.get_clusters_from_names(
                cluster_names)

        # get_endpoints normally reads and parses each cluster YAML to obtain
        # its provider config. That is another fleet-sized DB N+1. Reuse the
        # records above to collect the YAML paths, then fetch all YAMLs in one
        # query before resolving endpoints.
        handles: dict[int, Any] = {}
        for info in cluster_lookup_infos:
            cluster_record = cluster_records.get(info.cluster_name)
            if cluster_record is None:
                continue
            if cleanup_fences[id(info)] is None:
                handle = info.handle(cluster_record)
            else:
                # A v2 row must validate the exact durable handle instead of
                # letting a convenience accessor assert or fetch by name.
                handle = (cluster_record.get('handle') if isinstance(
                    cluster_record, dict) else None)
            handles[id(info)] = handle
        uncached_handles = {
            info.replica_id: handles[id(info)]
            for info in cluster_lookup_infos
            if _cached_route(info) is None and id(info) in handles
        }
        provider_configs = serve_utils.get_provider_configs_for_handles(
            uncached_handles)

        # First resolve/cache candidates under their physical-cluster fence.
        # The emission pass below retains the caller's original row order so
        # URL-collision handling remains deterministic even though pool fences
        # are batched.
        route_candidates: dict[int, tuple[str, str, int]] = {}

        def _resolve_route_candidate(
                info: 'replica_managers.ReplicaInfo') -> None:
            is_capable = _is_recovery_capable(info)
            cached = _cached_route(info)
            if cached is None:
                cluster_record = cluster_records.get(info.cluster_name)
                if cluster_record is None:
                    if is_capable:
                        self._replica_manager.retire_system_recovery_route(info)
                    logger.warning(f'Replica {info.replica_id} is READY but '
                                   'its cluster record is not available yet; '
                                   'skipping for this sync.')
                    return
                handle = handles.get(id(info))
                url = info._resolve_url(  # pylint: disable=protected-access
                    cluster_record=cluster_record,
                    handle=handle,
                    provider_config=provider_configs.get(info.replica_id))
                if url is None:
                    # A replica can be READY while its endpoint is briefly
                    # unresolvable (e.g. the cluster record has no head IP
                    # mid-recovery). Skip it for this sync instead of
                    # crashing the whole load_balancer_sync — it is simply
                    # not routable this round and will be re-resolved on
                    # the next sync.
                    if is_capable:
                        self._replica_manager.retire_system_recovery_route(info)
                    logger.warning(f'Replica {info.replica_id} is READY but '
                                   'its endpoint is not resolvable yet; '
                                   'skipping for this sync.')
                    return
                # gpu_type/gpu_count are used by instance-aware load
                # balancing policies. They derive from the replica's
                # launched accelerators, which are fixed for the replica's
                # lifetime.
                gpu_type = 'unknown'
                gpu_count = 1
                if handle is not None:
                    accelerators = handle.launched_resources.accelerators
                    if accelerators:
                        gpu_type = list(accelerators.keys())[0]
                        try:
                            gpu_count = max(1, int(accelerators[gpu_type]))
                        except (TypeError, ValueError):
                            gpu_count = 1
                cached = (url, gpu_type, gpu_count)
            route_candidates[id(info)] = cached

        verified_ready_count = 0
        v2_groups: dict[tuple[str, str], list[tuple[
            replica_managers.ReplicaInfo,
            contextlib.AbstractContextManager[None]]]] = {}
        legacy_infos: list[replica_managers.ReplicaInfo] = []
        for info in ready_infos:
            if id(info) in identity_rejected_ids:
                continue
            cleanup_fence = cleanup_fences[id(info)]
            if cleanup_fence is None:
                legacy_infos.append(info)
                continue
            try:
                # Construction performs the row/handle checks synchronously;
                # entering one representative below performs the shared
                # physical UID read for the whole pool.
                provider_fence = reserved_capacity.protocol_v2_provider_fence(
                    info, handle=handles.get(id(info)))
            except exceptions.KubernetesPhysicalClusterIdentityError as e:
                identity_rejected_ids.add(id(info))
                _retire_unverified_route(info, e)
                continue
            key = (cleanup_fence.kubernetes_context,
                   cleanup_fence.physical_cluster_uid)
            v2_groups.setdefault(key, []).append((info, provider_fence))

        if v2_groups:
            # Resolve every physically fenced group before opening an ambient
            # provider phase.  A timeout escapes before the local candidate
            # maps below are published to any warm routing cache.
            with provider_phase.provider_phase(
                    provider_phase.ProviderPhaseMode.V2_FENCED):
                for grouped_infos in v2_groups.values():
                    try:
                        # Every row in this group has already validated its
                        # own durable handle. Keeping the representative fence
                        # open makes all endpoint reads use the same captured
                        # physical target.
                        with grouped_infos[0][1]:
                            for info, _ in grouped_infos:
                                _resolve_route_candidate(info)
                    except exceptions.KubernetesPhysicalClusterIdentityError as e:
                        # Treat the group as one coherent snapshot: a provider
                        # fence failure invalidates candidates resolved earlier
                        # in the same scope as well as any warm cached routes.
                        for info, _ in grouped_infos:
                            route_candidates.pop(id(info), None)
                            identity_rejected_ids.add(id(info))
                            _retire_unverified_route(info, e)
                        continue
                    verified_ready_count += len(grouped_infos)

        if legacy_infos:
            with provider_phase.provider_phase(
                    provider_phase.ProviderPhaseMode.AMBIENT_LEGACY):
                for info in legacy_infos:
                    verified_ready_count += 1
                    _resolve_route_candidate(info)

        for info in ready_infos:
            cached = route_candidates.get(id(info))
            if cached is None:
                continue
            is_capable = _is_recovery_capable(info)
            url, gpu_type, gpu_count = cached
            try:
                normalized_url = (
                    system_recovery_route_lease.normalize_route_url(url))
            except system_recovery_route_lease.RouteLeaseError:
                if is_capable:
                    self._replica_manager.retire_system_recovery_route(info)
                    logger.warning(
                        'Recovery-capable replica %s has an '
                        'invalid route URL; withholding it.', info.replica_id)
                else:
                    logger.warning(
                        'Replica %s has an invalid route URL; '
                        'withholding it.', info.replica_id)
                continue
            url = normalized_url
            cached = (url, gpu_type, gpu_count)
            route_marker = None
            if is_capable:
                route_marker = (
                    self._replica_manager.system_recovery_route_marker(
                        info, url))
            replica_cache[info.replica_id] = cached
            replica_cache_record_ids[info.replica_id] = info.replica_record_id
            prior_info = resolved_url_sources.get(url)
            if prior_info is not None:
                for source_info in (prior_info, info):
                    if (source_info.system_recovery_disposition ==
                            system_recovery_state.SystemRecoveryDisposition.
                            CAPABLE):
                        self._replica_manager.retire_system_recovery_route(
                            source_info)
                # Keep the transport key present so this coherent snapshot is
                # not mistaken for a spurious empty resolution.  New LBs parse
                # the closed fence field and remove any retained client/lease.
                replica_info[url] = {
                    serve_constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                        serve_constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
                }
                logger.error(
                    'Replica route collision for normalized URL %s '
                    '(replicas %s and %s); fencing the URL.', url,
                    prior_info.replica_id, info.replica_id)
                continue
            resolved_url_sources[url] = info
            if is_capable and route_marker is None:
                # A capable row without its exact process-local marker must
                # never fall through as an ordinary outage-retained route.
                replica_info[url] = {
                    serve_constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                        serve_constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
                }
                continue
            replica_info[url] = {
                'gpu_type': gpu_type,
                'gpu_count': str(gpu_count),
            }
            if route_marker is not None:
                replica_info[url].update({
                    serve_constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY:
                        serve_constants.
                        SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_VERSION,
                    serve_constants.SYSTEM_RECOVERY_ROUTE_REPLICA_ID_KEY:
                        route_marker.replica_id,
                    serve_constants.SYSTEM_RECOVERY_ROUTE_TOKEN_KEY:
                        route_marker.route_token,
                })
            is_zero_cost = info.is_zero_cost
            if type(is_zero_cost) is bool:
                # Placement-cost provenance is independent from the launch
                # reason: an ordinary demand launch may land on free reserved
                # capacity and should receive the same economic tie-break.
                replica_info[url]['is_zero_cost'] = ('true' if is_zero_cost else
                                                     'false')
            async_occupancy = ((async_occupancy_by_version or
                                {}).get(info.version))
            if async_occupancy is not None:
                # Per-replica (not latest-service) declaration: during a
                # rolling update old and new versions may have different
                # fast-ack contracts. Emit explicit false as a tri-state
                # protocol: omission means old controller / preserve prior LB
                # knowledge, while false starts a two-phase disable that keeps
                # old work visible until a generation-valid idle sample.
                replica_info[url]['async_occupancy'] = (
                    'true' if async_occupancy else 'false')
        # The translation cache retains entries for replicas that left
        # READY but are still alive: the LB's in-flight snapshot is taken
        # against ITS last routing view, so a replica probe-blipped out
        # of READY mid-job would otherwise become untranslatable -- its
        # in-flight unit would vanish from the autoscaler's outstanding
        # sum and, worse, the replica would read as idle and become a
        # scale-down victim while an hour-long job still runs on it.
        # Terminal replicas (SHUTTING_DOWN included) are pruned so the
        # cache stays bounded AND so a retiring replica's in-flight stops
        # counting toward the autoscaler's outstanding-work sum: its
        # requests are pinned to it and cannot be re-routed, so counting
        # them as demand would launch phantom replacement capacity for
        # the whole drain window. The retirement drain itself does not
        # need translation -- it matches the LB's raw url-keyed report
        # against the replica's own url (see _ReplicaDrainTracker).
        nonterminal_records = {
            info.replica_id: info.replica_record_id
            for info in replica_infos
            if not info.is_terminal
        }
        translation_cache = {
            replica_id: cached
            for replica_id, cached in self._lb_translation_cache.items()
            if (replica_id in nonterminal_records and
                getattr(self, '_lb_translation_cache_record_ids', {}).get(
                    replica_id) == nonterminal_records[replica_id])
        }
        translation_cache_record_ids = {
            replica_id: nonterminal_records[replica_id]
            for replica_id in translation_cache
        }
        translation_cache.update(replica_cache)
        translation_cache_record_ids.update(replica_cache_record_ids)
        self._lb_translation_cache = translation_cache
        self._lb_translation_cache_record_ids = translation_cache_record_ids
        # Replacing the cache with this sync's active replicas prunes the
        # replicas that are no longer READY.
        self._lb_replica_cache = replica_cache
        self._lb_replica_cache_record_ids = replica_cache_record_ids
        return replica_info, verified_ready_count

    def _url_to_replica_id_map(self) -> dict[str, int]:
        """Invert the translation cache (url -> replica id)."""
        return {
            url: replica_id
            for replica_id, (url, _, _) in self._lb_translation_cache.items()
        }

    def _translate_in_flight(
            self,
            in_flight_by_url: dict[str, int] | None) -> dict[int, int] | None:
        """Translate the LB's url-keyed in-flight gauge to replica ids.

        [boltz fork] The LB only knows replicas by url; the autoscaler
        only knows them by id. `_lb_translation_cache` (replica_id ->
        (url, gpu_type, gpu_count), rebuilt by `_get_lb_replica_info` on
        every sync, so call that first) provides the inversion. It
        deliberately includes still-alive replicas that left READY, so a
        probe-blipped replica's running job stays attributed instead of
        vanishing (which would both shrink the outstanding sum and make
        the replica read as an idle scale-down victim). A url the cache
        does not know (the replica went terminal) is dropped: there is
        no live replica id to attribute the work to.

        None passes through: it means the LB (old version, or a policy
        that cannot track in-flight) sent no gauge, which the autoscaler
        must distinguish from an empty fleet.
        """
        if in_flight_by_url is None:
            return None
        url_to_replica_id = self._url_to_replica_id_map()
        in_flight_by_replica_id: dict[int, int] = {}
        for url, count in in_flight_by_url.items():
            replica_id = url_to_replica_id.get(url)
            if replica_id is not None:
                in_flight_by_replica_id[replica_id] = int(count)
        return in_flight_by_replica_id

    def _translate_observed_slots(
            self, slots_by_url: dict[str, int] | None) -> dict[int, int]:
        """Translate a generation-valid LB slot map to durable replica IDs."""
        if slots_by_url is None:
            return {}
        url_to_replica_id = self._url_to_replica_id_map()
        return {
            url_to_replica_id[url]: max(0, int(slots))
            for url, slots in slots_by_url.items()
            if url in url_to_replica_id
        }

    def _logical_bridge_capacity_candidates(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        logical_versions: set[int],
        observed_slots: dict[int, int],
    ) -> dict[int, int]:
        """Return fresh physical-bridge widths bounded by launched GPUs."""
        if getattr(self._autoscaler, 'replica_unit', None) != 'logical':
            return {}
        candidates: dict[int, int] = {}
        for info in replica_infos:
            if info.version in logical_versions or info.is_terminal:
                continue
            observed = observed_slots.get(info.replica_id)
            if observed is None or observed < 1:
                continue
            current = getattr(info, 'planned_capacity', 1)
            if (isinstance(current, bool) or not isinstance(current, int) or
                    current < 1):
                current = 1
            durable_bound = (current if bool(
                getattr(info, 'logical_bridge_capacity_verified', False)) else
                             1)
            cached = self._lb_translation_cache.get(info.replica_id)
            if cached is None:
                observed_slots[info.replica_id] = min(observed, durable_bound)
                continue
            gpu_type, gpu_count = cached[1], cached[2]
            if (gpu_type == 'unknown' or isinstance(gpu_count, bool) or
                    not isinstance(gpu_count, int) or gpu_count < 1):
                observed_slots[info.replica_id] = min(observed, durable_bound)
                continue
            verified = min(observed, gpu_count)
            # Pass only state transitions to the manager. This keeps the LB
            # sync hot path free of a redundant DB read/write per heartbeat.
            if (not bool(
                    getattr(info, 'logical_bridge_capacity_verified', False)) or
                    verified > current):
                candidates[info.replica_id] = verified
            # Reporter-controlled values must never reach the autoscaler above
            # the independently resolved hardware bound.
            observed_slots[info.replica_id] = verified
        return candidates

    def _unknown_async_replica_ids(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        async_occupancy_by_version: dict[int, bool | None],
        occupancy_sampled_urls: list[str] | None,
        unknown_in_flight_urls: list[str] | None,
        force_all_live_unknown: bool = False,
    ) -> set[int]:
        """Resolve the fail-closed async occupancy set for one LB report.

        An envelope count (including explicit zero) does not prove anything
        about fast-ack work. A declared async replica is known only when the
        LB says its numeric entry includes a validity-filtered occupancy
        sample. Old LBs omit that proof, and a first sync necessarily precedes
        application of the controller's declaration, so both remain unknown.
        """
        url_to_replica_id = self._url_to_replica_id_map()
        sampled_replica_ids: set[int] = set()
        if not force_all_live_unknown:
            sampled_replica_ids = {
                url_to_replica_id[url]
                for url in (occupancy_sampled_urls or [])
                if url in url_to_replica_id
            }
        unknown_replica_ids = {
            url_to_replica_id[url]
            for url in (unknown_in_flight_urls or [])
            if url in url_to_replica_id
        }
        live_infos = [
            info for info in replica_infos
            if info.status in (serve_state.ReplicaStatus.READY,
                               serve_state.ReplicaStatus.NOT_READY)
        ]
        if force_all_live_unknown:
            # Two maxSurge LBs publish last-writer-wins gauges. A sampled zero
            # from either cannot prove that the other accepted no work, so the
            # short overlap/grace fails closed for every live replica,
            # including legacy services without a declaration.
            unknown_replica_ids.update(info.replica_id for info in live_infos)
        else:
            unknown_replica_ids.update(
                info.replica_id
                for info in live_infos
                if async_occupancy_by_version.get(info.version, False) and
                info.replica_id not in sampled_replica_ids)
        return unknown_replica_ids

    def _lb_report_authority(self,
                             session_id: str | None) -> tuple[bool, bool, bool]:
        """Return ``(live member, demand, drain)`` report authority.

        The sole Ready, non-terminating Pod sees all new Service traffic, so it
        may continue feeding demand while an old terminating Pod finishes a
        long stream. Idleness and drain completion are service-wide claims:
        they remain authoritative only when the reporter is the sole live Pod,
        including terminating Pods. Both decisions use one Kubernetes list.
        """
        try:
            pod_authority = lb_k8s.get_lb_pod_authority(self._service_name)
        except Exception as e:  # pylint: disable=broad-except
            # The lifecycle helper already converts Kubernetes API failures to
            # None. Keep this boundary defensive too: report validation must
            # fail closed even if an unexpected adapter error escapes it.
            logger.warning('Failed to validate the live load balancer Pod '
                           f'set: {common_utils.format_exception(e)}')
            return False, False, False
        if session_id is None or pod_authority is None:
            return False, False, False
        reporter_is_live = session_id in pod_authority.live_uids
        if pod_authority.slot_by_uid is not None:
            reporter_slot = pod_authority.slot_by_uid.get(session_id)
            try:
                state = serve_state.get_lb_cutover_state(self._service_name)
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('Failed to read HA load balancer authority: '
                               f'{common_utils.format_exception(e)}')
                return False, False, False
            coherent = (state is not None and state.enabled and
                        state.active_slot is not None and
                        state.active_slot == pod_authority.selected_slot)
            active_slot = state.active_slot if state is not None else None
            reporter_ready = (reporter_is_live and session_id
                              in pod_authority.ready_nonterminating_uids)
            legacy_selected = (state is not None and state.phase
                               in (lb_ha.LbCutoverPhase.MIGRATING,
                                   lb_ha.LbCutoverPhase.ROLLING_BACK) and
                               pod_authority.selected_slot is None and
                               pod_authority.legacy_uids is not None and
                               session_id in pod_authority.legacy_uids)
            live_slot_uids = (set(pod_authority.slot_by_uid) &
                              pod_authority.live_uids)
            legacy_drain_authoritative = (
                legacy_selected and
                pod_authority.legacy_uids == {session_id} and
                state is not None and
                (state.phase is lb_ha.LbCutoverPhase.MIGRATING or
                 not live_slot_uids))
            demand_authoritative = reporter_ready and (
                (coherent and reporter_slot == active_slot) or legacy_selected)
            # HA drain authority is service-wide and comes from the bounded
            # ACTIVE+DRAINING session ledger, never a single Pod report.
            return (reporter_is_live, demand_authoritative,
                    legacy_drain_authoritative)
        demand_authoritative = (pod_authority.ready_nonterminating_uids == {
            session_id
        })
        drain_authoritative = pod_authority.live_uids == {session_id}
        return reporter_is_live, demand_authoritative, drain_authoritative

    @staticmethod
    def _lb_drain_report_view(
        request_data: dict[str, Any],
        report_is_authoritative: bool,
    ) -> tuple[dict[str, int] | None, list[str] | None]:
        """Return a raw drain view that only the sole live LB can prove."""
        in_flight = request_data.get('in_flight')
        routing_urls = request_data.get('routing_urls')
        if not report_is_authoritative:
            # Never publish any field from a non-authoritative reporter. In
            # particular, replacing a trusted report with a synthetic blocking
            # report would still let a different service's/rollout Pod mutate
            # this service's drain session and freshness timestamp.
            return None, None
        return in_flight, routing_urls

    async def _confirm_logical_bridge_capacities(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        logical_versions: set[int],
        observed_slots: dict[int, int],
    ) -> None:
        """Bound and durably confirm physical-bridge logical capacities."""
        bridge_candidates = self._logical_bridge_capacity_candidates(
            replica_infos, logical_versions, observed_slots)
        if not bridge_candidates:
            return
        confirmer = getattr(self._replica_manager,
                            'confirm_logical_bridge_capacities', None)
        if not callable(confirmer):
            return
        loop = asyncio.get_running_loop()
        try:
            confirmed = await loop.run_in_executor(None, confirmer,
                                                   bridge_candidates)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to persist verified logical bridge '
                           'capacity; retaining conservative width one: '
                           f'{common_utils.format_exception(e)}')
            return
        infos_by_id = {info.replica_id: info for info in replica_infos}
        for replica_id, capacity in confirmed.items():
            info = infos_by_id.get(replica_id)
            if info is not None:
                info.planned_capacity = capacity
                info.logical_bridge_capacity_verified = True

    async def _ingest_load_balancer_report(
        self,
        request_data: dict[str, Any],
        replica_infos: list['replica_managers.ReplicaInfo'],
        async_occupancy_by_version: dict[int, bool | None],
        authority: tuple[bool, bool, bool] | None = None,
        logical_versions: set[int] | None = None,
    ) -> bool:
        """Apply the independently authorized parts of one external LB report.

        A sole Ready reporter keeps scale-up demand live during maxSurge. Until
        every older Pod is gone, its zero gauges cannot prove service-wide
        idleness: all live replicas remain occupancy-unknown and the replica
        manager receives a controller-generated blocking drain view. A wrong
        UID, multiple Ready Pods, or a failed lookup cannot mutate either
        subsystem.
        """
        if authority is None:
            loop = asyncio.get_running_loop()
            authority = await loop.run_in_executor(
                None, self._lb_report_authority,
                request_data.get('lb_session_id'))
        observed_slots: dict[int, int] = {}
        if authority[1]:
            observed_slots = self._translate_observed_slots(
                request_data.get('total_slots_by_url'))
            if logical_versions is not None:
                await self._confirm_logical_bridge_capacities(
                    replica_infos, logical_versions, observed_slots)
        return self._apply_load_balancer_report(
            request_data,
            replica_infos,
            async_occupancy_by_version,
            authority,
            observed_slots,
        )

    def _apply_load_balancer_report(
        self,
        request_data: dict[str, Any],
        replica_infos: list['replica_managers.ReplicaInfo'],
        async_occupancy_by_version: dict[int, bool | None],
        authority: tuple[bool, bool, bool],
        observed_slots: dict[int, int],
    ) -> bool:
        """Synchronously mutate runtime state from one prepared LB report."""
        accepted, effective_request_data, ha_enabled = (
            self._prepare_load_balancer_report(request_data, authority))
        if not accepted:
            return False
        applied = self._apply_prepared_load_balancer_report(
            request_data, effective_request_data, replica_infos,
            async_occupancy_by_version, authority, observed_slots, ha_enabled)
        if not applied:
            return False
        return self._apply_load_balancer_drain_report(request_data, authority,
                                                      ha_enabled)

    def _prepare_load_balancer_report(
        self,
        request_data: dict[str, Any],
        authority: tuple[bool, bool, bool],
    ) -> tuple[bool, dict[str, Any], bool]:
        """Linearize HA demand-handoff state before runtime ingestion.

        The HA role channel snapshots ``_lb_last_demand_snapshot`` while
        beginning a cutover and mutates ``_lb_demand_handoff`` while
        recovering or rolling one back.  The async sync handler serializes
        this short head phase with those rare demand transitions, then
        releases the demand lock before the potentially contended
        autoscaler/replica-manager phase.
        """
        (reporter_is_live, demand_authoritative, _) = authority
        ha_enabled = getattr(self, '_lb_ha_enabled', False)
        if not reporter_is_live:
            logger.warning('Ignoring non-authoritative load balancer demand '
                           'and drain report for service '
                           f'{self._service_name!r}.')
            return False, request_data, ha_enabled

        effective_request_data = request_data
        if demand_authoritative:
            if ha_enabled:
                state = serve_state.get_lb_cutover_state(self._service_name)
                if (state is not None and
                        state.phase is not lb_ha.LbCutoverPhase.PREPARING):
                    self._restore_lb_demand_handoff(state.generation)
                    complete_report = self._lb_demand_report_is_complete(
                        request_data)
                    handoff = getattr(self, '_lb_demand_handoff', None)
                    if handoff is not None:
                        if (complete_report and
                                handoff.complete_report_at is None and
                                handoff.generation == state.generation):
                            fence = self._lb_cutover_fence()
                            if fence is not None:
                                service_hash, owner, lifecycle_epoch = fence
                                complete_at = (
                                    serve_state.mark_lb_demand_handoff_complete(
                                        self._service_name, service_hash, owner,
                                        lifecycle_epoch, state.generation))
                                handoff.restore(handoff.generation,
                                                handoff.snapshot, complete_at)
                        handoff_generation = handoff.generation
                        effective_request_data = handoff.apply(
                            state.generation, request_data, complete_report)
                        if (handoff_generation is not None and
                                handoff.generation is None):
                            fence = self._lb_cutover_fence()
                            if fence is not None:
                                service_hash, owner, lifecycle_epoch = fence
                                serve_state.clear_lb_demand_handoff(
                                    self._service_name, service_hash, owner,
                                    lifecycle_epoch, handoff_generation)
                demand_snapshot = lb_ha.DemandSnapshot.from_request(
                    request_data)
                self._lb_last_demand_snapshot = demand_snapshot
                if (state is not None and state.active_slot is not None and
                        state.phase in (lb_ha.LbCutoverPhase.STABLE,
                                        lb_ha.LbCutoverPhase.DRAINING)):
                    fence = self._lb_cutover_fence()
                    if fence is not None:
                        service_hash, owner, lifecycle_epoch = fence
                        serve_state.record_lb_active_demand_snapshot(
                            self._service_name, service_hash, owner,
                            lifecycle_epoch, state.active_slot,
                            state.generation, demand_snapshot)
        return True, effective_request_data, ha_enabled

    def _apply_prepared_load_balancer_report(
        self,
        request_data: dict[str, Any],
        effective_request_data: dict[str, Any],
        replica_infos: list['replica_managers.ReplicaInfo'],
        async_occupancy_by_version: dict[int, bool | None],
        authority: tuple[bool, bool, bool],
        observed_slots: dict[int, int],
        ha_enabled: bool,
    ) -> bool:
        """Apply prepared demand and drain state without touching HA handoff."""
        (_, demand_authoritative, drain_authoritative) = authority
        if not demand_authoritative:
            # Either genuine Pod may refresh routing during the two-Ready
            # maxSurge window, but it may not mutate demand state.
            return True

        # Parse reporter-controlled demand only after its dedicated gate.
        # Besides preventing state mutation, this keeps a stale/wrong Pod from
        # making the controller reject a useful routing response with a
        # malformed demand-only field.
        request_aggregator: dict[str, Any] = effective_request_data.get(
            'request_aggregator', {})
        timestamps: list[int] = request_aggregator.get('timestamps', [])
        compatibility_profiles = request_aggregator.get(
            'compatibility_profiles', [])
        queued_compatibility_profiles = effective_request_data.get(
            'queued_requests_by_compatibility', [])
        rejected_compatibility_profiles = effective_request_data.get(
            'rejected_requests_by_compatibility', [])
        logger.info(f'Received {len(timestamps)} inflight requests.')
        translated_in_flight = self._translate_in_flight(
            effective_request_data.get('in_flight'))
        unknown_replica_ids = self._unknown_async_replica_ids(
            replica_infos,
            async_occupancy_by_version,
            effective_request_data.get('occupancy_sampled_urls', []),
            effective_request_data.get('unknown_in_flight_urls', []),
            force_all_live_unknown=(not drain_authoritative and not ha_enabled))
        self._reconcile_generation = getattr(self, '_reconcile_generation',
                                             0) + 1
        reconcile_generation = self._reconcile_generation
        # Validate the reporter epoch and ingest its exact-card gauges under
        # the same lock used to publish a new catalog/version. The report is
        # therefore either wholly old-epoch or wholly new-epoch.
        with self._routing_state_lock:
            compatibility_demand_complete = (
                self._compatibility_demand_report_is_complete(request_data))
            self._autoscaler.collect_request_information({
                'timestamps': timestamps,
                'compatibility_profiles': compatibility_profiles,
                'queued_requests_by_compatibility': queued_compatibility_profiles,
                'rejected_requests_by_compatibility': rejected_compatibility_profiles,
                'compatibility_demand_complete': compatibility_demand_complete,
                'in_flight_by_replica_id': translated_in_flight,
                'unknown_in_flight_replica_ids': list(unknown_replica_ids),
                'observed_slots_by_replica_id': observed_slots,
                # During maxSurge overlap, no LB can prove service-wide
                # async occupancy. Keep those backends drain-busy, but do
                # not age the degraded-capacity replacement timer: the old
                # Pod may simply be finishing a long stream. Replacement
                # becomes eligible only from a sole-live authoritative
                # reporter's real probe miss.
                'unknown_capacity_replica_ids': list(unknown_replica_ids if (
                    drain_authoritative or ha_enabled) else ()),
                'reconcile_generation': reconcile_generation,
                'queue_depth': effective_request_data.get('queue_depth'),
                'queue_depth_by_priority':
                    effective_request_data.get('queue_depth_by_priority'),
                'rejected_in_window':
                    effective_request_data.get('rejected_in_window'),
                'rejected_in_recent_window':
                    effective_request_data.get('rejected_in_recent_window'),
                'rejected_in_window_by_priority': effective_request_data.get(
                    'rejected_in_window_by_priority'),
                'rejected_in_recent_window_by_priority':
                    effective_request_data.get(
                        'rejected_in_recent_window_by_priority'),
                # Measured request durations. The same snapshot the
                # controller persists for history also lets the
                # autoscaler supersede its configured duration estimate.
                'prediction_time_history':
                    request_data.get('prediction_time_history'),
                'unique_job_arrivals_60s':
                    effective_request_data.get('unique_job_arrivals_60s'),
                'unique_job_arrivals_300s':
                    effective_request_data.get('unique_job_arrivals_300s'),
                'headerless_arrivals_60s':
                    effective_request_data.get('headerless_arrivals_60s'),
                'headerless_arrivals_300s':
                    effective_request_data.get('headerless_arrivals_300s'),
                'offered_arrival_tracking_saturated':
                    effective_request_data.get(
                        'offered_arrival_tracking_saturated'),
                'pressure_report_is_floored':
                    effective_request_data.get('pressure_report_is_floored'),
            })
            if (translated_in_flight is not None and getattr(
                    self._autoscaler, 'replica_unit', None) == 'logical'):
                self._replica_manager.update_logical_reconcile_snapshot(
                    version=self._autoscaler.latest_version,
                    generation=reconcile_generation,
                    observed_slots_by_replica_id=observed_slots,
                    in_flight_by_replica_id=translated_in_flight,
                    unknown_replica_ids=unknown_replica_ids)
        return True

    def _apply_load_balancer_drain_report(
        self,
        request_data: dict[str, Any],
        authority: tuple[bool, bool, bool],
        ha_enabled: bool,
    ) -> bool:
        """Publish a validated report's drain view under fresh authority."""
        (reporter_is_live, demand_authoritative,
         drain_authoritative) = authority
        if not reporter_is_live:
            return False
        if not demand_authoritative and not drain_authoritative:
            # A merely live overlap Pod may refresh routing, but it must not
            # overwrite a previously trusted drain snapshot.
            return True
        if ha_enabled and not drain_authoritative:
            # The role channel owns the service-wide ACTIVE+DRAINING view.
            return True
        if drain_authoritative:
            drain_in_flight, drain_routing_urls = self._lb_drain_report_view(
                request_data, report_is_authoritative=True)
            unknown_urls = request_data.get('unknown_in_flight_urls')
            draining_urls = request_data.get('draining_urls')
        else:
            # This is the legitimate sole Ready Pod, but another live Pod may
            # still own streams. Invalidate an older clean proof without
            # trusting this overlap reporter's process-local drain fields.
            drain_in_flight, drain_routing_urls = {}, None
            unknown_urls, draining_urls = [], []
        self._replica_manager.update_lb_in_flight(
            drain_in_flight, drain_routing_urls, unknown_urls, draining_urls,
            request_data.get('lb_session_id'))
        return True

    async def _handle_load_balancer_sync(
            self, request_data: dict[str, Any]) -> fastapi.Response:
        """Validate LB membership before disclosing confidential routing."""
        # Every DB read below (ownership fences, replica/spec snapshot) runs
        # in the executor: on a large replica table these are the sync
        # handler's blocking calls, and the FastAPI event loop must stay free
        # so controller liveness and ownership probes remain responsive.
        loop = asyncio.get_running_loop()
        if not await loop.run_in_executor(None, self._owns_current_service):
            return fastapi.Response(status_code=503)
        lb_sync_lock = self._lb_sync_lock
        if lb_sync_lock is None:
            lb_sync_lock = asyncio.Lock()
            self._lb_sync_lock = lb_sync_lock
        async with lb_sync_lock:
            authority = await loop.run_in_executor(
                None, self._lb_report_authority,
                request_data.get('lb_session_id'))
            if not authority[0]:
                # The sync token authenticates the shared LB workload, not
                # membership in this service. Do not reveal replica URLs,
                # capacity, or routing policy to another service's Pod.
                return fastapi.Response(status_code=503)
            with self._routing_state_lock:
                replica_snapshot_version = self._applied_version
            (replica_infos, async_occupancy_by_version,
             logical_versions) = (await loop.run_in_executor(
                 None, self._snapshot_replica_occupancy))
            # Cold endpoint resolution is proportional to the READY fleet and
            # may take tens of seconds. Keep it off the FastAPI event loop so
            # controller liveness and ownership probes remain responsive.
            try:
                lb_replica_info, num_ready = await loop.run_in_executor(
                    None, self._get_lb_replica_info, replica_infos,
                    async_occupancy_by_version)
            except exceptions.ProviderPhaseTimeoutError as error:
                # No routing/cache state is published until
                # _get_lb_replica_info completes both provider phases.  A
                # timeout therefore fails the whole sync closed instead of
                # returning a successful partial map.
                logger.warning(
                    'Load balancer route synchronization timed '
                    'out waiting for provider authority: %s',
                    common_utils.format_exception(error))
                return fastapi.Response(status_code=503)
            if isinstance(lb_replica_info, dict):
                self._lb_expected_occupancy_urls = {
                    url for url, info in lb_replica_info.items()
                    if str(info.get('async_occupancy', '')).lower() == 'true'
                }
                self._lb_occupancy_contract_known = True
            # History is incarnation-scoped and never changes runtime state,
            # so it may finish before the final ownership fence even if this
            # controller loses the service mid-write. Autoscaler history reads
            # the previously applied authoritative demand snapshot; the next
            # frequent sync persists the report prepared below.
            observed_slots: dict[int, int] = {}
            if authority[1]:
                observed_slots = self._translate_observed_slots(
                    request_data.get('total_slots_by_url'))
                if logical_versions is not None:
                    await self._confirm_logical_bridge_capacities(
                        replica_infos, logical_versions, observed_slots)
            # Replica aggregation includes the cached reserved-capacity
            # observation read. Keep that PostgreSQL read off the FastAPI
            # event loop with the other load-balancer sync reads.
            replica_counts = await loop.run_in_executor(
                None, self._get_replica_counts, replica_infos)
            history_capacity_hint = self._get_capacity_hint(
                replica_infos, logical_versions, replica_counts=replica_counts)
            ((request_history_accepted,
              request_classification_history_accepted),
             response_time_history_accepted, prediction_time_history_accepted,
             _) = await asyncio.gather(
                 self._persist_request_histories(request_data),
                 self._persist_response_time_history(request_data),
                 self._persist_prediction_time_history(request_data),
                 self._persist_autoscaler_history(replica_counts,
                                                  history_capacity_hint),
             )
            # HA cutover promotion snapshots the last active demand report.
            # Serialize only demand-handoff mutations; ordinary role
            # heartbeats do not take this lock and must remain responsive even
            # when PostgreSQL/Kubernetes fencing for a sync is slow.
            demand_lock = getattr(self, '_lb_demand_lock', None)
            if demand_lock is None:
                demand_lock = asyncio.Lock()
                self._lb_demand_lock = demand_lock
            async with demand_lock:
                head_operation = loop.run_in_executor(
                    None, self._prepare_authoritative_load_balancer_report,
                    request_data)
                prepared = await self._await_executor_operation(
                    head_operation, 'Load balancer report preparation')
                if prepared is None:
                    return fastapi.Response(status_code=503)

            if prepared.authority[1] and not authority[1]:
                # Authority can legitimately move while the earlier replica
                # and history snapshots are prepared. Preserve the newly
                # authoritative report's capacity observation; durable bridge
                # confirmation remains best-effort and conservative on error.
                observed_slots = self._translate_observed_slots(
                    request_data.get('total_slots_by_url'))
                if logical_versions is not None:
                    await self._confirm_logical_bridge_capacities(
                        replica_infos, logical_versions, observed_slots)

            if not await loop.run_in_executor(None, self._owns_current_service):
                return fastapi.Response(status_code=503)
            deferred_sync_cancellation: asyncio.CancelledError | None = None

            async def complete_sync_phase(operation: 'asyncio.Future[Any]',
                                          description: str) -> Any:
                """Keep a post-tail safety phase atomic across cancellation."""
                nonlocal deferred_sync_cancellation
                try:
                    result, cancellation = (await
                                            self._complete_executor_operation(
                                                operation, description))
                except Exception as e:  # pylint: disable=broad-except
                    deferred_cancellation = deferred_sync_cancellation
                    if deferred_cancellation is None:
                        raise
                    logger.warning(
                        f'{description} failed after its request was '
                        f'cancelled: {common_utils.format_exception(e)}')
                    raise deferred_cancellation from e  # pylint: disable=raising-bad-type
                if (cancellation is not None and
                        deferred_sync_cancellation is None):
                    deferred_sync_cancellation = cancellation
                return result

            def raise_deferred_sync_cancellation() -> None:
                if deferred_sync_cancellation is not None:
                    raise deferred_sync_cancellation  # pylint: disable=raising-bad-type

            tail_operation = loop.run_in_executor(
                None,
                self._apply_prepared_load_balancer_report,
                request_data,
                prepared.effective_request_data,
                replica_infos,
                async_occupancy_by_version,
                prepared.authority,
                observed_slots,
                prepared.ha_enabled,
            )
            accepted = await complete_sync_phase(
                tail_operation, 'Load balancer runtime report ingestion')
            if not accepted:
                raise_deferred_sync_cancellation()
                return fastapi.Response(status_code=503)
            if prepared.ha_enabled and not prepared.authority[2]:
                # Steady HA slot reports never own drain state; the fast role
                # channel publishes the ACTIVE+DRAINING aggregate. Revalidate
                # disclosure off the role lock so normal heartbeats are not
                # queued behind duplicate owner/Kubernetes reads.
                drain_operation = loop.run_in_executor(
                    None, self._load_balancer_disclosure_is_authorized,
                    request_data.get('lb_session_id'))
                drain_accepted = await complete_sync_phase(
                    drain_operation, 'Load balancer disclosure validation')
            else:
                # Legacy-selected and non-HA reporters can own drain state.
                # Order their publication after any role transition that ran
                # while the runtime tail waited on manager locks.
                role_lock = getattr(self, '_lb_role_lock', None)
                if role_lock is None:
                    role_lock = asyncio.Lock()
                    self._lb_role_lock = role_lock
                lock_operation = asyncio.create_task(role_lock.acquire())
                acquired = await complete_sync_phase(
                    lock_operation, 'Load balancer drain lock acquisition')
                assert acquired
                try:
                    drain_operation = loop.run_in_executor(
                        None,
                        self._apply_authoritative_load_balancer_drain_report,
                        request_data)
                    drain_accepted = await complete_sync_phase(
                        drain_operation,
                        'Load balancer drain report publication')
                finally:
                    role_lock.release()
            raise_deferred_sync_cancellation()
            if not drain_accepted:
                return fastapi.Response(status_code=503)
            self._replica_counts_snapshot = replica_counts
            capacity_hint = self._get_capacity_hint(
                replica_infos, logical_versions, replica_counts=replica_counts)
            # Snapshot the routing contract and its version atomically.  The
            # load balancer only echoes this version after applying the same
            # response's routing spec and route set.
            with self._routing_state_lock:
                service_version = self._applied_version
                routing_spec = (self._get_routing_spec() if service_version
                                == replica_snapshot_version else None)
            response_content = {
                'replica_info': lb_replica_info,
                'num_ready_replicas': num_ready,
                'routing_spec': routing_spec,
                'capacity_hint': capacity_hint,
                'request_history_accepted': request_history_accepted,
                'request_classification_history_accepted': request_classification_history_accepted,
                'response_time_history_accepted': response_time_history_accepted,
                'prediction_time_history_accepted': prediction_time_history_accepted,
                # Additive protocol negotiation for mixed-version rollouts.
                # A new LB only relies exclusively on the replaceable queue
                # gauge after a controller positively advertises support.
                'queued_compatibility_demand_supported': True,
            }
            # Additive in every mode so the next demand report can prove that
            # its exact-card gauges were admitted under the current catalog.
            # Old LBs ignore this field and remain compatibility-incomplete.
            response_content['service_version'] = service_version
            return responses.JSONResponse(content=response_content,
                                          status_code=200)

    def _lb_cutover_fence(
        self,) -> tuple[str, tuple[int | None, str | None], int] | None:
        """Return the current incarnation/owner/epoch fence or fail closed."""
        owner = serve_state.get_service_controller_owner(self._service_name,
                                                         include_lb_state=True)
        if (owner is None or not owner.get('lb_ha_enabled') or
                not owner.get('hash') or owner.get('lifecycle_epoch') is None):
            return None
        expected_owner = self._controller_owner
        actual_owner = (owner.get('controller_pid'), owner.get('controller_ip'))
        if expected_owner is None or actual_owner != expected_owner:
            return None
        return (str(owner['hash']), actual_owner, int(owner['lifecycle_epoch']))

    def _lb_promotion_report_is_current(self, request_data: dict[str,
                                                                 Any]) -> bool:
        if not getattr(self, '_lb_occupancy_contract_known', False):
            return False
        routing_version = request_data.get('routing_version')
        if (not isinstance(routing_version, int) or
                isinstance(routing_version, bool) or
                routing_version != self._applied_version):
            return False
        sample_generations = request_data.get('occupancy_sample_generation', {})
        sample_ages = request_data.get('occupancy_sample_age_seconds', {})
        if not isinstance(sample_generations, dict) or not isinstance(
                sample_ages, dict):
            return False
        return lb_ha.occupancy_samples_are_promotable(
            self._lb_expected_occupancy_urls, sample_generations, sample_ages,
            serve_constants.LB_PROMOTION_OCCUPANCY_MAX_AGE_SECONDS)

    @staticmethod
    def _lb_demand_report_is_complete(request_data: dict[str, Any]) -> bool:
        """Whether an authoritative report can age out old demand gauges.

        Occupancy samples need not cover every backend. Missing samples are
        represented by the current report's unknown set and remain protected
        individually; they must not preserve stale queue and rejection gauges
        for the whole fleet indefinitely. Requiring every field keeps a mixed
        rollout with an older load balancer fail closed.
        """
        in_flight = request_data.get('in_flight')
        if not isinstance(in_flight, dict):
            return False
        if any(not isinstance(url, str) or not isinstance(count, int) or
               isinstance(count, bool) or count < 0
               for url, count in in_flight.items()):
            return False
        for field in ('queue_depth', 'rejected_in_window',
                      'rejected_in_recent_window'):
            value = request_data.get(field)
            if (not isinstance(value, int) or isinstance(value, bool) or
                    value < 0):
                return False
        unknown_urls = request_data.get('unknown_in_flight_urls')
        queued_compatibility_profiles = request_data.get(
            'queued_requests_by_compatibility')
        rejected_compatibility_profiles = request_data.get(
            'rejected_requests_by_compatibility')
        return (isinstance(unknown_urls, list) and
                all(isinstance(url, str) for url in unknown_urls) and
                isinstance(queued_compatibility_profiles, list) and all(
                    lb_ha.CompatibilityDemand.from_dict(
                        profile, require_timestamp=False) is not None
                    for profile in queued_compatibility_profiles) and
                isinstance(rejected_compatibility_profiles, list) and all(
                    lb_ha.CompatibilityDemand.from_dict(
                        profile, require_timestamp=False) is not None
                    for profile in rejected_compatibility_profiles))

    def _compatibility_demand_report_is_complete(
            self, request_data: dict[str, Any]) -> bool:
        """Whether all replaceable exact-card demand gauges are present."""
        routing_version = request_data.get('routing_version')
        return (isinstance(routing_version, int) and
                not isinstance(routing_version, bool) and
                routing_version == self._applied_version and
                self._lb_demand_report_is_complete(request_data))

    def _prepare_authoritative_load_balancer_report(
            self,
            request_data: dict[str, Any]) -> _PreparedLoadBalancerReport | None:
        """Refresh fences and prepare one demand-transition-serialized report."""
        if not self._owns_current_service():
            return None
        authority = self._lb_report_authority(request_data.get('lb_session_id'))
        if not authority[0]:
            return None
        accepted, effective_request_data, ha_enabled = (
            self._prepare_load_balancer_report(request_data, authority))
        if not accepted:
            return None
        return _PreparedLoadBalancerReport(authority, effective_request_data,
                                           ha_enabled)

    def _apply_authoritative_load_balancer_drain_report(
            self, request_data: dict[str, Any]) -> bool:
        """Re-fence and publish drain state immediately before disclosure."""
        if not self._owns_current_service():
            return False
        authority = self._lb_report_authority(request_data.get('lb_session_id'))
        if not authority[0]:
            return False
        return self._apply_load_balancer_drain_report(
            request_data, authority, getattr(self, '_lb_ha_enabled', False))

    def _load_balancer_disclosure_is_authorized(self,
                                                session_id: str | None) -> bool:
        """Revalidate ownership and live Pod membership before disclosure."""
        return (self._owns_current_service() and
                self._lb_report_authority(session_id)[0])

    @staticmethod
    async def _complete_executor_operation(
            operation: 'asyncio.Future[Any]',
            description: str) -> tuple[Any, asyncio.CancelledError | None]:
        """Finish an executor mutation and report deferred cancellation."""
        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                result = await asyncio.shield(operation)
            except asyncio.CancelledError as e:  # noqa: ASYNC103
                # Cancelling an asyncio wrapper cannot stop a running executor
                # function. Even repeated cancellation must not release the
                # corresponding lock while that worker mutates shared state.
                if operation.cancelled():
                    raise
                cancellation = e
            except Exception as e:  # pylint: disable=broad-except
                deferred_cancellation = cancellation
                if deferred_cancellation is None:
                    raise
                assert isinstance(deferred_cancellation, asyncio.CancelledError)
                logger.warning(
                    f'{description} failed after its request was cancelled: '
                    f'{common_utils.format_exception(e)}')
                raise deferred_cancellation from e  # pylint: disable=raising-bad-type
            else:
                return result, cancellation

    @classmethod
    async def _await_executor_operation(cls, operation: 'asyncio.Future[Any]',
                                        description: str) -> Any:
        """Keep an uncancellable executor mutation inside its async lock."""
        result, cancellation = await cls._complete_executor_operation(
            operation, description)
        if cancellation is not None:
            raise cancellation
        return result

    def _restore_lb_demand_handoff(self, generation: int) -> None:
        handoff = self._lb_demand_handoff
        if handoff.generation == generation:
            return
        durable_generation, snapshot, complete_at = (
            serve_state.get_lb_demand_handoff(self._service_name))
        handoff.restore(durable_generation, snapshot, complete_at)

    def _publish_ha_drain_view(self,
                               authority: lb_k8s.LbPodAuthority,
                               state: lb_ha.LbCutoverState,
                               legacy_selected: bool = False) -> None:
        """Publish one service-wide ACTIVE+DRAINING drain snapshot."""
        if (legacy_selected and state.phase is lb_ha.LbCutoverPhase.MIGRATING):
            # The selected legacy Pod publishes the only authoritative drain
            # view through the regular sync path. Idle warm slots must not
            # overwrite it before migration.
            return
        ledger = self._lb_session_ledger
        if ledger is None or authority.slot_by_uid is None:
            return
        ledger.discard_dead(authority.live_uids)
        stream_owner_slots = {state.active_slot}
        if (state.phase is lb_ha.LbCutoverPhase.DRAINING and
                state.pending_slot is not None):
            stream_owner_slots.add(state.pending_slot)
        stream_owner_ids = {
            session_id for session_id, slot in authority.slot_by_uid.items()
            if session_id in authority.live_uids and slot in stream_owner_slots
        }
        legacy_stream_owner_ids = ((authority.legacy_uids or set()) &
                                   authority.live_uids)
        if (state.phase is lb_ha.LbCutoverPhase.ROLLING_BACK and
                not legacy_selected):
            # A terminating migration tail can still own streams when an
            # immediate rollback enters ROLLING_BACK but cannot yet create or
            # select the replacement legacy Deployment. A fresh unselected
            # legacy candidate has never received traffic and is excluded.
            legacy_stream_owner_ids &= authority.terminating_uids or set()
        if legacy_stream_owner_ids:
            # A legacy Pod remains a possible stream owner after the migration
            # selector moves and throughout its termination grace. During
            # rollback, it becomes a possible owner as soon as the selector
            # moves back. Legacy processes do not use the role ledger, so
            # including their UID deliberately makes the aggregate incomplete
            # and blocks backend drain decisions until the topology settles.
            stream_owner_ids.update(legacy_stream_owner_ids)
        report = ledger.aggregate(stream_owner_ids)
        if report.complete:
            in_flight = report.in_flight
            routing_urls = report.routing_urls
            unknown_urls = report.unknown_urls
            draining_urls = report.draining_urls
        else:
            # A missing stream-owner report is not evidence of idleness.
            in_flight, routing_urls, unknown_urls, draining_urls = {}, None, [], []
        self._replica_manager.update_lb_in_flight(
            in_flight, routing_urls, unknown_urls, draining_urls,
            f'ha-generation-{state.generation}')

    def _rollback_active_slot_is_drained(self, authority: lb_k8s.LbPodAuthority,
                                         state: lb_ha.LbCutoverState) -> bool:
        """Return whether rollback may retire the formerly active slot."""
        if (state.phase is not lb_ha.LbCutoverPhase.ROLLING_BACK or
                state.active_slot is None or authority.slot_by_uid is None):
            return False
        active_ids = {
            session_id for session_id, slot in authority.slot_by_uid.items()
            if session_id in authority.live_uids and slot is state.active_slot
        }
        if not active_ids:
            return True
        ledger = self._lb_session_ledger
        if ledger is None:
            return False
        ledger.discard_dead(authority.live_uids)
        report = ledger.aggregate(active_ids,
                                  required_applied_role=lb_ha.LbRole.DRAINING,
                                  required_applied_generation=state.generation)
        # Only process-local work belongs to the retiring LB. Replica-global
        # async occupancy continues to be sampled by the selected legacy LB.
        return report.complete and report.local_in_flight == 0

    def _finish_ha_drain_if_safe(
            self, authority: lb_k8s.LbPodAuthority, state: lb_ha.LbCutoverState,
            fence: tuple[str, tuple[int | None, str | None], int]) -> bool:
        if (state.phase is not lb_ha.LbCutoverPhase.DRAINING or
                state.active_slot is None or state.pending_slot is None or
                authority.slot_by_uid is None):
            return False
        draining_ids = {
            session_id for session_id, slot in authority.slot_by_uid.items()
            if session_id in authority.live_uids and slot == state.pending_slot
        }
        clean = not draining_ids
        if draining_ids and self._lb_session_ledger is not None:
            report = self._lb_session_ledger.aggregate(
                draining_ids,
                required_applied_role=lb_ha.LbRole.DRAINING,
                required_applied_generation=state.generation)
            # Backend async occupancy is replica-global and continues to be
            # sampled by the new active. It is not work owned by the former
            # LB process and must not pin DRAINING forever. The process-local
            # admission count includes unqueued and queued dispatches as well
            # as returned streams. Requiring the LB to acknowledge DRAINING
            # also closes the admission window between the selector move and
            # the first role response applied by the former active.
            clean = report.complete and report.local_in_flight == 0
        drain_started_at = state.drain_started_at
        timed_out = (drain_started_at is not None and
                     time.time() - drain_started_at >= getattr(
                         self, '_lb_drain_timeout_seconds',
                         serve_constants.LB_DRAIN_CLOSE_GRACE_SECONDS))
        if timed_out and not clean:
            logger.warning('Finishing HA LB drain after the bounded Pod '
                           f'termination budget for {self._service_name!r}.')
            clean = True
        if not clean:
            return False
        service_hash, owner, lifecycle_epoch = fence
        return serve_state.finish_lb_cutover_drain(
            self._service_name, service_hash, owner, lifecycle_epoch,
            state.active_slot, state.pending_slot, state.generation)

    @staticmethod
    def _lb_ha_rollout_evidence(
            authority: lb_k8s.LbPodAuthority, state: lb_ha.LbCutoverState,
            desired_runtime_revision: str | None) -> dict[str, Any]:
        """Return role-channel evidence that both slots share one revision."""
        revisions = authority.revision_by_uid or {}
        slot_by_uid = authority.slot_by_uid or {}
        slots: dict[str, dict[str, Any]] = {}
        all_revisions: set[str] = set()
        for slot in lb_ha.LbSlot:
            ready_ids = {
                uid for uid in authority.ready_nonterminating_uids
                if slot_by_uid.get(uid) is slot
            }
            slot_revisions = sorted({
                revision for uid in ready_ids
                if (revision := revisions.get(uid)) is not None
            })
            all_revisions.update(slot_revisions)
            slots[slot.value] = {
                'ready': bool(ready_ids),
                'revisions': slot_revisions,
            }
        converged = (desired_runtime_revision is not None and
                     state.phase is lb_ha.LbCutoverPhase.STABLE and
                     all(slot['ready'] and len(slot['revisions']) == 1
                         for slot in slots.values()) and
                     all_revisions == {desired_runtime_revision})
        return {
            'phase': state.phase.value,
            'selected_slot': (state.active_slot.value
                              if state.active_slot is not None else None),
            'generation': state.generation,
            'desired_revision': desired_runtime_revision,
            'slots': slots,
            'slots_converged': converged,
        }

    async def _handle_load_balancer_role(
            self, request_data: dict[str, Any]) -> fastapi.Response:
        """Ingest a fast HA report and advance the recoverable cutover saga."""
        trace = lb_ha_obs.RoleRequestTrace()

        def role_response(
                outcome: lb_ha_obs.LbRoleOutcome,
                status_code: int,
                content: dict[str, Any] | None = None
        ) -> responses.JSONResponse:
            response_content = dict(content or {})
            response_content['outcome'] = outcome.value
            response_content['observability'] = trace.snapshot()
            return responses.JSONResponse(content=response_content,
                                          status_code=status_code)

        if not self._lb_ha_enabled:
            return role_response(
                lb_ha_obs.LbRoleOutcome.LEGACY_MODE, 200, {
                    'role': lb_ha.LbRole.ACTIVE.value,
                    'generation': 0,
                    'selected_slot': None,
                    'promotable': True,
                })
        session_id = request_data.get('lb_session_id')
        slot = lb_ha.parse_slot(request_data.get('lb_slot'))
        if not isinstance(session_id, str) or slot is None:
            return role_response(lb_ha_obs.LbRoleOutcome.INVALID_REPORT, 503)
        loop = asyncio.get_running_loop()
        if not await trace.run_in_executor(loop, 'postgresql_owner_read',
                                           self._owns_current_service):
            return role_response(lb_ha_obs.LbRoleOutcome.CONTROLLER_NOT_OWNER,
                                 503)
        role_lock = self._lb_role_lock
        if role_lock is None:
            role_lock = asyncio.Lock()
            self._lb_role_lock = role_lock
        demand_lock = getattr(self, '_lb_demand_lock', None)
        if demand_lock is None:
            demand_lock = asyncio.Lock()
            self._lb_demand_lock = demand_lock
        demand_transition_cancellation: asyncio.CancelledError | None = None
        demand_transition_invalidated_generation: int | None = None

        async def run_demand_transition(phase: str, function: Callable, *args:
                                        Any) -> Any:
            nonlocal demand_transition_cancellation
            operation = asyncio.create_task(
                trace.run_in_executor(loop, phase, function, *args))
            try:
                result, cancellation = await self._complete_executor_operation(
                    operation, f'Load balancer {phase}')
            except Exception as e:  # pylint: disable=broad-except
                deferred_cancellation = demand_transition_cancellation
                if deferred_cancellation is None:
                    raise
                demand_transition_cancellation = None
                logger.warning(
                    'Load balancer transition failed after its request was '
                    f'cancelled: {common_utils.format_exception(e)}')
                raise deferred_cancellation from e  # pylint: disable=raising-bad-type
            if cancellation is not None:
                demand_transition_cancellation = cancellation
            return result

        def invalidate_demand_transition(
                transition_state: lb_ha.LbCutoverState) -> None:
            """Block clean drain proof until a post-transition role report."""
            nonlocal demand_transition_invalidated_generation
            if (demand_transition_invalidated_generation ==
                    transition_state.generation):
                return
            self._replica_manager.update_lb_in_flight(
                {}, None, [], [],
                f'ha-transition-{transition_state.generation}')
            demand_transition_invalidated_generation = (
                transition_state.generation)

        def finish_demand_transition(
                transition_state: lb_ha.LbCutoverState) -> None:
            """Publish a safe drain view before propagating cancellation."""
            nonlocal demand_transition_cancellation
            if demand_transition_cancellation is not None:
                # The caller will not apply this role response, and an
                # executor mutation may already have moved the selector.
                # Invalidate any prior clean proof until a later role report
                # publishes a view sampled after the transition.
                invalidate_demand_transition(transition_state)
                cancellation = demand_transition_cancellation
                demand_transition_cancellation = None
                raise cancellation  # pylint: disable=raising-bad-type

        lock_wait_started_at = time.monotonic()
        async with role_lock:
            trace.lock_acquired(lock_wait_started_at)
            authority = await trace.run_in_executor(loop,
                                                    'kubernetes_pod_authority',
                                                    lb_k8s.get_lb_pod_authority,
                                                    self._service_name)
            if authority is None or authority.slot_by_uid is None:
                return role_response(
                    lb_ha_obs.LbRoleOutcome.POD_AUTHORITY_UNAVAILABLE, 503)
            if (session_id not in authority.live_uids or
                    authority.slot_by_uid.get(session_id) is not slot):
                return role_response(
                    lb_ha_obs.LbRoleOutcome.POD_NOT_AUTHORITATIVE, 503)
            fence = await trace.run_in_executor(loop, 'postgresql_fence_read',
                                                self._lb_cutover_fence)
            state = await trace.run_in_executor(
                loop, 'postgresql_cutover_state_read',
                serve_state.get_lb_cutover_state, self._service_name)
            if (fence is None or state is None or not state.enabled or
                    state.active_slot is None):
                return role_response(
                    lb_ha_obs.LbRoleOutcome.CUTOVER_STATE_UNAVAILABLE, 503)
            promotable = self._lb_promotion_report_is_current(request_data)
            role = state.role_for(slot)
            ledger = self._lb_session_ledger
            if ledger is None or not ledger.update(
                    session_id, slot, role, state.generation, request_data):
                return role_response(lb_ha_obs.LbRoleOutcome.REPORT_REJECTED,
                                     503)
            try:
                routing: (lb_k8s.LbServiceRouting |
                          lb_k8s.LbServiceTransitionRouting)
                if state.phase in (lb_ha.LbCutoverPhase.MIGRATING,
                                   lb_ha.LbCutoverPhase.ROLLING_BACK):
                    routing = await trace.run_in_executor(
                        loop, 'kubernetes_service_routing_read',
                        lb_k8s.get_lb_service_transition_routing,
                        self._service_name)
                else:
                    routing = await trace.run_in_executor(
                        loop, 'kubernetes_service_routing_read',
                        lb_k8s.get_lb_service_routing, self._service_name)
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('Cannot reconcile HA LB Service routing: '
                               f'{common_utils.format_exception(e)}')
                return role_response(
                    lb_ha_obs.LbRoleOutcome.ROUTING_UNAVAILABLE, 503)

            service_hash, expected_owner, lifecycle_epoch = fence
            transition_legacy_selected = bool(
                state.phase in (lb_ha.LbCutoverPhase.MIGRATING,
                                lb_ha.LbCutoverPhase.ROLLING_BACK) and
                getattr(routing, 'legacy_selected', False))
            ready_by_slot = {
                candidate_slot: {
                    uid
                    for uid in authority.ready_nonterminating_uids
                    if authority.slot_by_uid.get(uid) is candidate_slot
                } for candidate_slot in lb_ha.LbSlot
            }
            if state.phase is lb_ha.LbCutoverPhase.MIGRATING:
                assert state.active_slot is lb_ha.LbSlot.A
                if transition_legacy_selected:
                    if (slot is lb_ha.LbSlot.A and
                            session_id in ready_by_slot[lb_ha.LbSlot.A] and
                            bool(ready_by_slot[lb_ha.LbSlot.B]) and promotable):
                        async with demand_lock:
                            invalidate_demand_transition(state)
                            patched = await run_demand_transition(
                                'kubernetes_selector_patch',
                                lb_k8s.patch_lb_service_migration_to_slot,
                                self._service_name, service_hash,
                                expected_owner, lifecycle_epoch)
                            if patched:
                                # The old routing snapshot still says legacy,
                                # but the resourceVersion-fenced patch is enough
                                # to block drain decisions immediately.
                                transition_legacy_selected = False
                            finish_demand_transition(state)
                elif (routing.active_slot is lb_ha.LbSlot.A and
                      routing.generation == 1):
                    async with demand_lock:
                        invalidate_demand_transition(state)
                        migrated = await run_demand_transition(
                            'postgresql_cutover_write',
                            serve_state.finish_lb_ha_migration,
                            self._service_name, service_hash, expected_owner,
                            lifecycle_epoch)
                        finish_demand_transition(state)
                    if migrated:
                        state = await trace.run_in_executor(
                            loop, 'postgresql_cutover_state_read',
                            serve_state.get_lb_cutover_state,
                            self._service_name)
                        assert state is not None
                        # Do not wait for foreground Deployment deletion while
                        # holding the role lock. The terminating legacy Pod may
                        # drain for the full grace period; blocking here makes
                        # both HA slots time out their role heartbeats. Stable
                        # parent-process supervision owns idempotent obsolete-
                        # topology cleanup and retries it on every pass.

            rollback_view_published = False
            if state.phase is lb_ha.LbCutoverPhase.ROLLING_BACK:
                rollback_active_slot = state.active_slot
                assert rollback_active_slot is not None
                legacy_ready = bool((authority.legacy_uids or set()) &
                                    authority.ready_nonterminating_uids)
                if (not transition_legacy_selected and legacy_ready and
                        routing.active_slot is rollback_active_slot and
                        routing.generation == state.generation):
                    async with demand_lock:
                        invalidate_demand_transition(state)
                        patched = await run_demand_transition(
                            'kubernetes_selector_patch',
                            lb_k8s.patch_lb_service_rollback_to_legacy,
                            self._service_name, service_hash, expected_owner,
                            lifecycle_epoch, rollback_active_slot,
                            state.generation)
                        if patched:
                            transition_legacy_selected = True
                        finish_demand_transition(state)
                if transition_legacy_selected:
                    # Publish a blocking drain view before the database leaves
                    # HA mode. The legacy Pod may already be accepting new
                    # streams, while the former active slot may still be
                    # finishing old ones.
                    self._publish_ha_drain_view(authority, state, True)
                    rollback_view_published = True
                    slot_drained = await trace.run_in_executor(
                        loop, 'drain_evidence_read',
                        self._rollback_active_slot_is_drained, authority, state)
                    rolled_back = False
                    if slot_drained:
                        async with demand_lock:
                            invalidate_demand_transition(state)
                            rolled_back = await run_demand_transition(
                                'postgresql_cutover_write',
                                serve_state.finish_lb_ha_rollback,
                                self._service_name, service_hash,
                                expected_owner, lifecycle_epoch,
                                rollback_active_slot, state.generation)
                            if rolled_back:
                                self._lb_ha_enabled = False
                                self._lb_session_ledger = None
                                self._lb_last_demand_snapshot = None
                            finish_demand_transition(state)
                    if rolled_back:
                        # As above, the parent supervisor deletes obsolete HA
                        # slots outside this role lock. Return the committed
                        # role response immediately so remaining slots keep a
                        # fresh controller heartbeat throughout their drain.
                        return role_response(
                            lb_ha_obs.LbRoleOutcome.SUCCESS, 200, {
                                'role': lb_ha.LbRole.DRAINING.value,
                                'generation': state.generation,
                                'selected_slot': None,
                                'promotable': False,
                                'phase':
                                    lb_ha.LbCutoverPhase.ROLLING_BACK.value,
                            })

            if state.phase is lb_ha.LbCutoverPhase.STABLE:
                stable_active_slot = state.active_slot
                assert stable_active_slot is not None
                if (routing.active_slot is not stable_active_slot or
                        routing.generation != state.generation):
                    return role_response(
                        lb_ha_obs.LbRoleOutcome.ROUTING_NOT_CONVERGED, 503)
                target = stable_active_slot.other
                selected_ready = bool(ready_by_slot[stable_active_slot])
                target_ready = session_id in ready_by_slot[target]
                planned_upgrade = False
                if selected_ready and target_ready:
                    revisions = authority.revision_by_uid or {}
                    target_revision = revisions.get(session_id)
                    desired_revision = routing.desired_runtime_revision
                    active_revisions = {
                        revisions.get(uid)
                        for uid in ready_by_slot[stable_active_slot]
                    }
                    planned_upgrade = (desired_revision is not None and
                                       target_revision == desired_revision and
                                       desired_revision not in active_revisions)
                if (slot is target and target_ready and promotable and
                    (not selected_ready or planned_upgrade)):
                    async with demand_lock:
                        invalidate_demand_transition(state)
                        demand_snapshot = self._lb_last_demand_snapshot
                        next_state = await run_demand_transition(
                            'postgresql_cutover_write',
                            serve_state.begin_lb_cutover, self._service_name,
                            service_hash, expected_owner, lifecycle_epoch,
                            stable_active_slot, state.generation, target,
                            demand_snapshot)
                        if next_state is not None:
                            self._lb_demand_handoff.begin(
                                next_state.generation, demand_snapshot)
                            state = next_state
                        finish_demand_transition(state)

            if state.phase is lb_ha.LbCutoverPhase.PREPARING:
                assert state.pending_slot is not None
                assert state.active_slot is not None
                target = state.pending_slot
                preparing_active_slot = state.active_slot
                # Crash recovery: the selector moved but the DB commit did not.
                if (routing.active_slot is target and
                        routing.generation == state.generation):
                    # This request discovered an already inconsistent
                    # topology. Block drain proof before even queueing on a
                    # concurrent sync/transition; cancellation while waiting
                    # for the demand lock must remain fail closed.
                    invalidate_demand_transition(state)
                    async with demand_lock:
                        committed = await run_demand_transition(
                            'postgresql_cutover_write',
                            serve_state.commit_lb_cutover, self._service_name,
                            service_hash, expected_owner, lifecycle_epoch,
                            preparing_active_slot, target, state.generation)
                        finish_demand_transition(state)
                    if not committed:
                        # The selector already routes to the target. If the
                        # fenced database CAS cannot record DRAINING, neither
                        # the old PREPARING snapshot nor a role response can
                        # safely describe all possible stream owners.
                        invalidate_demand_transition(state)
                        return role_response(
                            lb_ha_obs.LbRoleOutcome.TRANSITION_INCONSISTENT,
                            503)
                    if committed:
                        state = await trace.run_in_executor(
                            loop, 'postgresql_cutover_state_read',
                            serve_state.get_lb_cutover_state,
                            self._service_name)
                        assert state is not None
                elif (routing.active_slot is preparing_active_slot and
                      routing.generation == state.generation - 1):
                    target_ready = session_id in ready_by_slot[target]
                    armed_generation = request_data.get('armed_generation')
                    if (slot is target and target_ready and promotable and
                            armed_generation == state.generation):
                        async with demand_lock:
                            invalidate_demand_transition(state)
                            patched = await run_demand_transition(
                                'kubernetes_selector_patch',
                                lb_k8s.patch_lb_service_active_slot,
                                self._service_name, service_hash,
                                expected_owner, lifecycle_epoch,
                                preparing_active_slot, state.generation - 1,
                                target, state.generation)
                            committed = False
                            if patched:
                                committed = await run_demand_transition(
                                    'postgresql_cutover_write',
                                    serve_state.commit_lb_cutover,
                                    self._service_name, service_hash,
                                    expected_owner, lifecycle_epoch,
                                    preparing_active_slot, target,
                                    state.generation)
                            finish_demand_transition(state)
                        if patched and not committed:
                            # Do not overwrite the fail-closed view with a
                            # PREPARING aggregate that omits the now-selected
                            # target.
                            invalidate_demand_transition(state)
                            return role_response(
                                lb_ha_obs.LbRoleOutcome.TRANSITION_INCONSISTENT,
                                503)
                        if committed:
                            state = await trace.run_in_executor(
                                loop, 'postgresql_cutover_state_read',
                                serve_state.get_lb_cutover_state,
                                self._service_name)
                            assert state is not None
                    elif not ready_by_slot[target]:
                        async with demand_lock:
                            invalidate_demand_transition(state)
                            advanced = await run_demand_transition(
                                'kubernetes_selector_patch',
                                lb_k8s.patch_lb_service_aborted_generation,
                                self._service_name, service_hash,
                                expected_owner, lifecycle_epoch,
                                preparing_active_slot, target, state.generation)
                            aborted = False
                            if advanced:
                                aborted = await run_demand_transition(
                                    'postgresql_cutover_write',
                                    serve_state.abort_lb_cutover_preparation,
                                    self._service_name, service_hash,
                                    expected_owner, lifecycle_epoch,
                                    preparing_active_slot, target,
                                    state.generation)
                            if aborted:
                                self._lb_demand_handoff.restore(
                                    None, None, None)
                            finish_demand_transition(state)
                        if aborted:
                            state = await trace.run_in_executor(
                                loop, 'postgresql_cutover_state_read',
                                serve_state.get_lb_cutover_state,
                                self._service_name)
                            assert state is not None
                elif (routing.active_slot is preparing_active_slot and
                      routing.generation == state.generation):
                    # Crash recovery after the Service generation was
                    # advanced but before the database abort committed.
                    async with demand_lock:
                        invalidate_demand_transition(state)
                        aborted = await run_demand_transition(
                            'postgresql_cutover_write',
                            serve_state.abort_lb_cutover_preparation,
                            self._service_name, service_hash, expected_owner,
                            lifecycle_epoch, preparing_active_slot, target,
                            state.generation)
                        if aborted:
                            self._lb_demand_handoff.restore(None, None, None)
                        finish_demand_transition(state)
                    if aborted:
                        state = await trace.run_in_executor(
                            loop, 'postgresql_cutover_state_read',
                            serve_state.get_lb_cutover_state,
                            self._service_name)
                        assert state is not None
                else:
                    return role_response(
                        lb_ha_obs.LbRoleOutcome.TRANSITION_INCONSISTENT, 503)

            legacy_selected = bool(
                state.phase in (lb_ha.LbCutoverPhase.MIGRATING,
                                lb_ha.LbCutoverPhase.ROLLING_BACK) and
                transition_legacy_selected)
            if not rollback_view_published:
                self._publish_ha_drain_view(authority, state, legacy_selected)
            if state.phase is lb_ha.LbCutoverPhase.DRAINING:
                finished = await trace.run_in_executor(
                    loop, 'drain_evidence_write', self._finish_ha_drain_if_safe,
                    authority, state, fence)
                if finished:
                    state = await trace.run_in_executor(
                        loop, 'postgresql_cutover_state_read',
                        serve_state.get_lb_cutover_state, self._service_name)
                    assert state is not None
            assert state.active_slot is not None
            role = state.role_for(slot)
            if (state.phase is lb_ha.LbCutoverPhase.ROLLING_BACK and
                    legacy_selected and slot is state.active_slot):
                # Once traffic has moved to legacy, stop admission on the
                # former active before waiting for its process-local count.
                role = lb_ha.LbRole.DRAINING
            return role_response(
                lb_ha_obs.LbRoleOutcome.SUCCESS, 200, {
                    'role': role.value,
                    'generation': state.generation,
                    'selected_slot': state.active_slot.value,
                    'promotable': promotable,
                    'phase': state.phase.value,
                    'ha_rollout': self._lb_ha_rollout_evidence(
                        authority, state, routing.desired_runtime_revision),
                })

    async def _handle_load_balancer_request_history_sync(
            self, request_data: dict[str, Any]) -> fastapi.Response:
        """Persist a draining LB's history without reporting demand."""
        loop = asyncio.get_running_loop()
        if not await loop.run_in_executor(None, self._owns_current_service):
            return fastapi.Response(status_code=503)
        authority = await loop.run_in_executor(
            None, self._lb_report_authority, request_data.get('lb_session_id'))
        if not authority[0]:
            return fastapi.Response(status_code=503)
        ((request_accepted, classification_accepted), response_time_accepted,
         prediction_time_accepted) = await asyncio.gather(
             self._persist_request_histories(request_data),
             self._persist_response_time_history(request_data),
             self._persist_prediction_time_history(request_data),
         )
        return responses.JSONResponse(content={
            'request_history_accepted': request_accepted,
            'request_classification_history_accepted': classification_accepted,
            'response_time_history_accepted': response_time_accepted,
            'prediction_time_history_accepted': prediction_time_accepted,
        },
                                      status_code=200)

    async def _persist_request_histories(
            self, request_data: dict[str, Any]) -> tuple[bool, bool]:
        """Persist arrivals only after current-v1 support is durable."""
        if _uses_current_request_classification_protocol(request_data):
            classification_accepted = (
                await
                self._persist_request_classification_history(request_data))
            if not classification_accepted:
                # Do not expose positive arrival rows without the paired
                # support fields. The load balancer retains both snapshots and
                # retries the classification transaction first.
                return False, False
            request_accepted = await self._persist_request_history(request_data)
            return request_accepted, True

        # Legacy and future-version payloads keep independent acknowledgement:
        # their attempt history remains useful even when this controller cannot
        # understand the classification envelope.
        request_accepted, classification_accepted = await asyncio.gather(
            self._persist_request_history(request_data),
            self._persist_request_classification_history(request_data),
        )
        return request_accepted, classification_accepted

    # These functions intentionally bind as methods on the controller facade.
    # pylint: disable=protected-access
    _persist_request_history = controller_history._persist_request_history
    _record_request_history = controller_history._record_request_history
    _persist_request_classification_history = (
        controller_history._persist_request_classification_history)
    _record_request_classification_history = (
        controller_history._record_request_classification_history)
    _persist_response_time_history = (
        controller_history._persist_response_time_history)
    _record_response_time_history = (
        controller_history._record_response_time_history)
    _persist_prediction_time_history = (
        controller_history._persist_prediction_time_history)
    _record_prediction_time_history = (
        controller_history._record_prediction_time_history)
    _persist_autoscaler_history = controller_history._persist_autoscaler_history
    _record_autoscaler_history = controller_history._record_autoscaler_history
    _get_accelerator_history_breakdown = (
        controller_history._get_accelerator_history_breakdown)

    # pylint: enable=protected-access

    def _owns_current_service(self) -> bool:
        """Whether this controller parent still owns the exact DB row."""
        service_hash = getattr(self, '_service_hash', None)
        controller_owner = getattr(self, '_controller_owner', None)
        if service_hash is None:
            # Compatibility for direct/legacy controller construction.
            return True
        owner = serve_state.get_service_controller_owner(self._service_name)
        return (owner is not None and owner.get('hash') == service_hash and
                (owner.get('controller_pid'), owner.get('controller_ip'))
                == controller_owner)

    def _snapshot_replica_occupancy(
        self
    ) -> tuple[list['replica_managers.ReplicaInfo'], dict[int, bool | None],
               set[int]]:
        """Read the replica rows and per-version async-occupancy flags.

        Blocking DB reads; callers on the event loop must run this in an
        executor.
        """
        replica_infos = serve_state.get_replica_infos(self._service_name)
        replica_versions = sorted({info.version for info in replica_infos})
        version_specs = serve_state.get_specs(self._service_name,
                                              replica_versions)
        async_occupancy_by_version: dict[int, bool | None] = {
            replica_version: None if version_specs.get(replica_version)
                             is None else getattr(
                                 version_specs[replica_version],
                                 'graceful_drain_async_occupancy',
                                 None) for replica_version in replica_versions
        }
        logical_versions = {
            replica_version for replica_version in replica_versions
            if getattr(version_specs.get(replica_version),
                       'uses_logical_replicas', False) is True
        }
        return replica_infos, async_occupancy_by_version, logical_versions

    def _get_capacity_hint(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        logical_versions: set[int] | None = None,
        replica_counts: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build the capacity_hint block of the sync response.

        [boltz fork] Computed from the replica_infos list the handler
        already fetched for `_get_lb_replica_info` -- no extra DB reads.

        - provisioning_replicas: latest-version, nonterminal, not-yet-
          ready replicas (capacity that will serve soon; lets the data
          plane hold spill decisions for capacity already on the way).
        - target_num_replicas: the autoscaler's current demand target. While
          the autoscaler's target may still be the rebuilt-blind minimum,
          report max(target, latest demand-owned nonterminal count) instead:
          a routine controller restart must not tell the platform the traffic
          fleet wants to shrink, while reserved fill must not delay spill or
          backfill as if it were paid demand intent. The floor keys on
          has_recomputed_with_fresh_data(), not has_fresh_demand_report(): the
          sync handler feeds the report BEFORE building this hint, so the very
          first post-restart sync is already "fresh" while the target stays
          min_replicas until the autoscaler thread's next decision tick
          consumes the snap.
        - max_replicas: the configured autoscaling ceiling. It changes only
          on service updates, so the external load balancer can retain the
          last synced value while the control plane is temporarily down.
        """
        latest_version = self._autoscaler.latest_version
        num_provisioning = 0
        num_latest_demand_nonterminal = 0
        logical = getattr(self._autoscaler, 'replica_unit', None) == 'logical'
        if logical_versions is None:
            logical_versions = {latest_version} if logical else set()
        for info in replica_infos:
            if info.version != latest_version or info.is_terminal:
                continue
            if (logical and getattr(getattr(info, 'status_property', None),
                                    'is_scale_down', False) is True):
                continue
            width = int(getattr(info, 'planned_capacity', 1)) if logical else 1
            # reserved_fill is launch-origin attribution. The dedicated
            # ready/provisioning fields still describe all usable capacity,
            # but opportunistic fill cannot raise this traffic-intent floor.
            # Legacy rows missing the additive flag remain demand-owned,
            # matching the autoscaler's restart baseline.
            if not getattr(info, 'reserved_fill', False):
                num_latest_demand_nonterminal += width
            if not info.is_ready:
                num_provisioning += width
        target = self._autoscaler.get_final_target_num_replicas()
        if not self._autoscaler.has_recomputed_with_fresh_data():
            target = max(target, num_latest_demand_nonterminal)
        hint: dict[str, Any] = {
            'replica_unit': ('logical_slot' if logical else 'physical_backend'),
            'provisioning_replicas': num_provisioning,
            'target_num_replicas': target,
            'max_replicas': self._autoscaler.max_replicas,
            'configured_max_replicas': self._autoscaler.max_replicas,
        }
        if replica_counts is None:
            replica_counts = self._get_replica_counts(replica_infos)
        hint.update({
            key: value
            for key, value in replica_counts.items()
            if key != 'replica_unit'
        })
        min_by_accelerator = getattr(self._autoscaler,
                                     'min_replicas_by_accelerator', {})
        demand_by_accelerator = getattr(self._autoscaler,
                                        'target_num_replicas_by_accelerator',
                                        {})
        if (isinstance(self._autoscaler, autoscalers.ConcurrencyAutoscaler) and
                not self._autoscaler.has_recomputed_with_fresh_data()):
            demand_by_accelerator = {}
        if isinstance(min_by_accelerator, dict) and min_by_accelerator:
            hint['min_replicas_by_accelerator'] = dict(min_by_accelerator)
        if isinstance(demand_by_accelerator, dict) and demand_by_accelerator:
            hint['target_num_replicas_by_accelerator'] = dict(
                demand_by_accelerator)
            hint['demand_target_by_accelerator'] = dict(demand_by_accelerator)
        if (isinstance(self._autoscaler, autoscalers.ConcurrencyAutoscaler) and
                self._autoscaler.has_recomputed_with_fresh_data()):
            hint['warm_retention_target_by_accelerator'] = dict(
                getattr(self._autoscaler,
                        'warm_retention_target_by_accelerator', {}))
            hint['cold_launch_authority_by_accelerator'] = dict(
                getattr(self._autoscaler,
                        'cold_launch_authority_by_accelerator', {}))
        if logical:
            planned_capacity_by_url = {
                cached[0]: int(getattr(info, 'planned_capacity', 1))
                for info in replica_infos
                if (info.version in logical_versions or bool(
                    getattr(info, 'logical_bridge_capacity_verified', False))
                   ) and not info.is_terminal
                for cached in [self._lb_translation_cache.get(info.replica_id)]
                if cached is not None
            }
            hint['planned_capacity_by_url'] = planned_capacity_by_url
            hint['logical_replica_urls'] = sorted(planned_capacity_by_url)
        return hint

    def _get_replica_counts(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> dict[str, Any]:
        """Return logical capacity and physical backend status aggregates."""
        autoscaler = getattr(self, '_autoscaler', None)
        logical = getattr(autoscaler, 'replica_unit', None) == 'logical'

        ready = total = failed = 0
        physical_ready = physical_total = physical_failed = 0
        ready_by_accelerator: dict[str, int] = {}
        provisioning_by_accelerator: dict[str, int] = {}
        total_by_accelerator: dict[str, int] = {}
        zero_cost_ready_by_accelerator: dict[str, int] = {}
        zero_cost_total_by_accelerator: dict[str, int] = {}
        zero_cost_location_classifier = getattr(
            autoscaler, 'is_replica_on_zero_cost_location', None)
        failed_statuses = serve_state.ReplicaStatus.failed_statuses()
        committed_unready_statuses = {
            serve_state.ReplicaStatus.PENDING,
            serve_state.ReplicaStatus.PROVISIONING,
            serve_state.ReplicaStatus.STARTING,
            serve_state.ReplicaStatus.NOT_READY,
        }
        for info in replica_infos:
            status = info.status
            # Pre-activation bridge rows deserialize with planned_capacity=1;
            # every logical version keeps its selected width. This lets a
            # rolling activation count both generations without spec queries.
            planned_capacity = getattr(info, 'planned_capacity', 1)
            width = (planned_capacity
                     if logical and isinstance(planned_capacity, int) and
                     not isinstance(planned_capacity, bool) and
                     planned_capacity > 0 else 1)
            cached = self._lb_translation_cache.get(info.replica_id)
            accelerator = cached[1] if cached is not None else 'unknown'
            if accelerator == 'unknown':
                resources = getattr(getattr(info, 'handle', None),
                                    'launched_resources', None)
                accelerators = getattr(resources, 'accelerators', None)
                if not accelerators:
                    accelerators = (getattr(info, 'resources_override', None) or
                                    {}).get('accelerators')
                if isinstance(accelerators, dict) and accelerators:
                    accelerator = next(iter(accelerators))
            known_accelerator = accelerator != 'unknown'
            is_zero_cost = info.is_zero_cost is True
            if callable(zero_cost_location_classifier):
                classified = zero_cost_location_classifier(info)
                # Loose mocks used by callers may synthesize arbitrary
                # attributes. Only the classifier's real boolean contract is
                # accepted; persisted provenance remains a valid positive
                # signal for builds/configurations without a location match.
                if type(classified) is bool:
                    is_zero_cost = is_zero_cost or classified
            if status == serve_state.ReplicaStatus.READY:
                capacity_getter = getattr(autoscaler,
                                          'get_ready_replica_capacity', None)
                observed_ready = (capacity_getter(info) if logical and
                                  callable(capacity_getter) else width)
                ready += max(0, int(observed_ready))
                physical_ready += 1
                if known_accelerator:
                    ready_by_accelerator[accelerator] = (
                        ready_by_accelerator.get(accelerator, 0) + width)
                if known_accelerator and is_zero_cost:
                    zero_cost_ready_by_accelerator[accelerator] = (
                        zero_cost_ready_by_accelerator.get(accelerator, 0) +
                        width)
            if status in failed_statuses:
                failed += width
                physical_failed += 1
            else:
                total += width
                physical_total += 1
                if known_accelerator:
                    total_by_accelerator[accelerator] = (
                        total_by_accelerator.get(accelerator, 0) + width)
                if known_accelerator and is_zero_cost:
                    zero_cost_total_by_accelerator[accelerator] = (
                        zero_cost_total_by_accelerator.get(accelerator, 0) +
                        width)
                if (known_accelerator and status in committed_unready_statuses):
                    provisioning_by_accelerator[accelerator] = (
                        provisioning_by_accelerator.get(accelerator, 0) + width)

        counts: dict[str, Any] = {
            'ready_replicas': ready,
            'total_replicas': total,
            'failed_replicas': failed,
            'physical_ready_replicas': physical_ready,
            'physical_total_replicas': physical_total,
            'physical_failed_replicas': physical_failed,
        }
        for key, value in {
                'ready_replicas_by_accelerator': ready_by_accelerator,
                'provisioning_replicas_by_accelerator': provisioning_by_accelerator,
                'total_replicas_by_accelerator': total_by_accelerator,
                'zero_cost_ready_replicas_by_accelerator': zero_cost_ready_by_accelerator,
                'zero_cost_total_replicas_by_accelerator': zero_cost_total_by_accelerator,
        }.items():
            if value:
                counts[key] = value
        free_reserved = self._get_free_reserved_slots_by_accelerator()
        if free_reserved:
            counts['free_reserved_slots_by_accelerator'] = free_reserved
        fill_target = self._get_fill_target_by_accelerator(
            zero_cost_total_by_accelerator, free_reserved)
        if fill_target:
            counts['fill_target_by_accelerator'] = fill_target
        demand_target = getattr(autoscaler,
                                'target_num_replicas_by_accelerator', {})
        if (isinstance(autoscaler, autoscalers.ConcurrencyAutoscaler) and
                not autoscaler.has_recomputed_with_fresh_data()):
            demand_target = {}
        if isinstance(demand_target, dict) and demand_target:
            counts['demand_target_by_accelerator'] = dict(demand_target)
        if (isinstance(autoscaler, autoscalers.ConcurrencyAutoscaler) and
                autoscaler.has_recomputed_with_fresh_data()):
            counts['warm_retention_target_by_accelerator'] = dict(
                getattr(autoscaler, 'warm_retention_target_by_accelerator', {}))
            counts['cold_launch_authority_by_accelerator'] = dict(
                getattr(autoscaler, 'cold_launch_authority_by_accelerator', {}))
        counts['replica_unit'] = ('logical_slot'
                                  if logical else 'physical_backend')
        return counts

    def _get_free_reserved_slots_by_accelerator(self) -> dict[str, int]:
        """Return fresh observed physical supply for cataloged free cards."""
        placer = getattr(getattr(self, '_replica_manager', None), 'spot_placer',
                         None)
        # LB sync is latency-sensitive and consumes both immutable catalog
        # identities and observations refreshed by the background poller.
        getter = getattr(placer, 'zero_cost_locations', None)
        if not callable(getter):
            return {}
        locations = getter()  # pylint: disable=not-callable
        if not isinstance(locations, list) or not locations:
            return {}
        shapes = reserved_capacity.zero_cost_pool_shapes(locations)
        observations = reserved_capacity.get_cached_free_gpus_by_pool(locations)
        canonical_by_name = {
            str(card).casefold(): str(card) for location in locations
            for card in (location.accelerators or {})
        }
        free_by_accelerator: dict[str, int] = {}
        for (context, normalized_card), per_replica in shapes.items():
            observation = observations.get((context, normalized_card))
            if observation is None or observation.free_gpus is None:
                continue
            card = canonical_by_name.get(normalized_card, normalized_card)
            free_by_accelerator[card] = (
                free_by_accelerator.get(card, 0) +
                max(0, observation.free_gpus) // max(1, per_replica))
        return free_by_accelerator

    def _get_fill_target_by_accelerator(
        self,
        zero_cost_total: dict[str, int],
        free_reserved: dict[str, int],
    ) -> dict[str, int]:
        """Return a fully attributable aggregate fill overlay by exact card."""
        if getattr(self._autoscaler, 'reserved_capacity_fill',
                   False) is not True:
            return {}
        raw_aggregate_target = getattr(self._autoscaler, '_fill_target', 0)
        aggregate_target = (max(0, raw_aggregate_target)
                            if isinstance(raw_aggregate_target, int) and
                            not isinstance(raw_aggregate_target, bool) else 0)
        card_order: list[str] = []
        seen: set[str] = set()
        demand_target = getattr(self._autoscaler,
                                'target_num_replicas_by_accelerator', {})
        if not isinstance(demand_target, dict):
            demand_target = {}
        for mapping in (demand_target, zero_cost_total, free_reserved):
            for card in mapping:
                if card.casefold() in seen:
                    continue
                seen.add(card.casefold())
                card_order.append(card)
        result: dict[str, int] = {}
        remaining = aggregate_target
        # Existing reserved replicas are the stable allocation. New fill is
        # then projected only onto exact cards with fresh physical supply.
        for source in (zero_cost_total, free_reserved):
            for card in card_order:
                if remaining <= 0:
                    break
                allocated = min(remaining, max(0, int(source.get(card, 0))))
                if allocated <= 0:
                    continue
                result[card] = result.get(card, 0) + allocated
                remaining -= allocated
        if remaining > 0:
            # A broker grant may remain visible for one poll after the exact
            # free observation becomes stale. The aggregate remains valid, but
            # its exact card is unavailable and must not be guessed.
            return {}
        return result

    def _configured_accelerators(self, service_spec: Any) -> list[str]:
        """Return configured exact accelerator IDs in service resource order."""
        yaml_content = getattr(getattr(self, '_replica_manager', None),
                               'yaml_content', None)
        if not isinstance(yaml_content, str):
            # Direct controller unit tests replace ReplicaManager with a loose
            # mock. A real manager always owns the committed YAML string.
            return []
        task = replica_managers.load_task_with_service_spec(
            yaml_content, service_spec)
        configured: list[str] = []
        seen: set[str] = set()
        counts_by_accelerator: dict[str, set[int]] = {}
        for resources in task.resources:
            for accelerator, raw_count in (resources.accelerators or
                                           {}).items():
                normalized = accelerator.casefold()
                try:
                    count = int(raw_count)
                except (TypeError, ValueError):
                    count = 0
                counts_by_accelerator.setdefault(normalized, set()).add(count)
                if normalized in seen:
                    continue
                seen.add(normalized)
                configured.append(accelerator)
        floors = getattr(service_spec, 'min_replicas_by_accelerator', {})
        configured_by_name = {name.casefold(): name for name in configured}
        unknown_floors = [
            name for name in floors if name.casefold() not in configured_by_name
        ]
        if unknown_floors:
            raise ValueError(
                'min_replicas_by_accelerator contains accelerators not '
                f'configured by the service resources: {unknown_floors}.')
        ambiguous = {
            name: sorted(counts)
            for name, counts in counts_by_accelerator.items()
            if len(counts) > 1 or not counts or min(counts) < 1
        }
        # A larger legacy any_of service remains valid but cannot encode its
        # default-all set in the bounded version-1 header. Withhold the
        # capability instead of breaking that existing service. Floors still
        # require an unambiguous shape for every exact card they target.
        if len(configured) > serve_constants.LB_REQUEST_ACCELERATORS_MAX_ITEMS:
            ambiguous_floors = {
                name: counts
                for name, counts in ambiguous.items()
                if name in {floor.casefold() for floor in floors}
            }
            if ambiguous_floors:
                raise ValueError(
                    'SkyServe per-card floors require one positive GPU count '
                    'shape per accelerator; found ambiguous floor shapes '
                    f'{ambiguous_floors}.')
            return []
        if ambiguous:
            raise ValueError(
                'SkyServe exact-card compatibility requires one positive GPU '
                'count shape per accelerator; found ambiguous shapes '
                f'{ambiguous}.')
        placer = getattr(getattr(self, '_replica_manager', None), 'spot_placer',
                         None)
        if len(configured) > 1 and not isinstance(task.resources,
                                                  list) and placer is None:
            if floors:
                raise ValueError(
                    'SkyServe per-card floors without a placement policy '
                    'require an ordered accelerator resource list so cold '
                    'scale-up is deterministic.')
            # A resources.any_of set has no user-defined order. Withhold the
            # request capability instead of turning hash iteration into a
            # cold-card policy. Existing aggregate any_of behavior remains.
            return []

        # First establish a deterministic service fallback. Ordered/list
        # resources retain their user order; unordered any_of resources use
        # the instance-aware QPS key order and then exact lexical order.
        if not isinstance(task.resources, list):
            qps_order: dict[str, int] = {}
            target_qps = getattr(service_spec, 'target_qps_per_replica', {})
            if isinstance(target_qps, dict):
                for key in target_qps:
                    card = key.partition(':')[0].casefold()
                    qps_order.setdefault(card, len(qps_order))
            configured.sort(key=lambda card: (qps_order.get(
                card.casefold(), len(qps_order)), card.casefold()))

        # The complete version catalog contains the nominal per-machine price
        # of every configured shape. Include temporarily benched locations:
        # transient
        # availability may delay an exact-card cold launch, but must never
        # promote a more expensive compatible card into its place. An
        # unavailable prices preserve the deterministic service fallback.
        if placer is not None:
            configured_by_name = {card.casefold(): card for card in configured}
            paid_costs: dict[str, float] = {}
            unpriced_cards: set[str] = set()
            try:
                known_locations = placer.known_locations()
            except Exception:  # pylint: disable=broad-except
                known_locations = []
            for location in known_locations:
                accelerators = location.accelerators or {}
                if len(accelerators) != 1:
                    continue
                raw_card = next(iter(accelerators))
                card = configured_by_name.get(str(raw_card).casefold())
                if card is None:
                    continue
                try:
                    hourly_cost = float(placer.cost_per_hour(location))
                except Exception:  # pylint: disable=broad-except
                    unpriced_cards.add(card)
                    continue
                if not math.isfinite(hourly_cost) or hourly_cost < 0:
                    unpriced_cards.add(card)
                    continue
                if hourly_cost == 0:
                    continue
                paid_costs[card] = min(hourly_cost,
                                       paid_costs.get(card, float('inf')))
            if (not unpriced_cards and
                    all(card in paid_costs for card in configured)):
                fallback_order = {
                    card: index for index, card in enumerate(configured)
                }
                configured.sort(
                    key=lambda card: (paid_costs[card], fallback_order[card]))
        return configured

    def _configured_accelerator_shapes(self,
                                       service_spec: Any) -> dict[str, int]:
        """Return canonical exact-card GPU counts from active task resources."""
        configured = self._configured_accelerators(service_spec)
        if not configured:
            return {}
        yaml_content = getattr(getattr(self, '_replica_manager', None),
                               'yaml_content', None)
        if not isinstance(yaml_content, str):
            return {}
        task = replica_managers.load_task_with_service_spec(
            yaml_content, service_spec)
        configured_by_name = {card.casefold(): card for card in configured}
        counts_by_name: dict[str, int] = {}
        for resources in task.resources:
            for accelerator, raw_count in (resources.accelerators or
                                           {}).items():
                card = configured_by_name.get(accelerator.casefold())
                if card is not None:
                    counts_by_name[card.casefold()] = int(raw_count)
        return {card: counts_by_name[card.casefold()] for card in configured}

    def _configure_instance_aware_accelerators(self, service_spec: Any) -> None:
        """Feed task-authoritative exact shapes to the compatible autoscaler."""
        compatible_autoscaler = self._autoscaler
        if not isinstance(compatible_autoscaler,
                          (autoscalers.InstanceAwareRequestRateAutoscaler,
                           autoscalers.ConcurrencyAutoscaler)):
            return
        shapes = self._accelerator_shapes_for_compatibility(
            compatible_autoscaler, service_spec)
        # Empty is an explicit policy downgrade, not a no-op. It clears an
        # in-place ConcurrencyAutoscaler's prior card catalog before the next
        # decision tick can act on stale compatibility state.
        compatible_autoscaler.set_configured_accelerator_shapes(shapes)

    def _accelerator_shapes_for_compatibility(
            self, candidate_autoscaler: autoscalers.Autoscaler,
            service_spec: Any) -> dict[str, int]:
        """Resolve the exact catalog for one autoscaler/spec transition."""
        if (getattr(service_spec, 'load_balancing_policy',
                    None) != 'instance_aware_least_load' or
                not isinstance(candidate_autoscaler,
                               (autoscalers.InstanceAwareRequestRateAutoscaler,
                                autoscalers.ConcurrencyAutoscaler))):
            return {}
        return self._configured_accelerator_shapes(service_spec)

    def _supports_exact_accelerator_compatibility(
            self,
            service_spec: Any,
            candidate_autoscaler: autoscalers.Autoscaler | None = None) -> bool:
        """Whether routing and autoscaling share the exact-card contract."""
        compatible_autoscaler = (getattr(self, '_autoscaler', None)
                                 if candidate_autoscaler is None else
                                 candidate_autoscaler)
        return (getattr(service_spec, 'load_balancing_policy',
                        None) == 'instance_aware_least_load' and
                isinstance(compatible_autoscaler,
                           (autoscalers.InstanceAwareRequestRateAutoscaler,
                            autoscalers.ConcurrencyAutoscaler)))

    def _build_routing_spec(
        self,
        service_spec: Any,
        candidate_autoscaler: autoscalers.Autoscaler | None = None
    ) -> dict[str, Any] | None:
        """Build the immutable routing config shipped on LB syncs."""
        if service_spec is None:
            return None
        target_qps = service_spec.target_qps_per_replica
        retriable_status_codes = service_spec.lb_retriable_status_codes
        configured_accelerators = (
            self._configured_accelerators(service_spec)
            if self._supports_exact_accelerator_compatibility(
                service_spec, candidate_autoscaler) else [])
        routing_spec = {
            # `load_balancing_policy` resolves None to the default policy
            # name, so the LB always receives a concrete policy to build.
            'load_balancing_policy_name': service_spec.load_balancing_policy,
            'target_qps_per_replica': (
                dict(target_qps) if isinstance(target_qps, dict) else target_qps
            ),
            # Lets an instance-aware LB weight replicas per-GPU when the
            # service sizes on concurrency (no QPS dict to weight by) --
            # and clear stale QPS weights after an update switches modes.
            'target_concurrency_per_replica': getattr(
                service_spec, 'target_concurrency_per_replica', None),
            'stream_timeout_seconds': service_spec.lb_stream_timeout_seconds,
            'retriable_status_codes':
                (list(retriable_status_codes)
                 if retriable_status_codes is not None else None),
            'max_retries': service_spec.lb_max_retries,
            'retry_initial_backoff_seconds':
                (service_spec.lb_retry_initial_backoff_seconds),
            'request_queue': getattr(service_spec, 'lb_request_queue', None),
        }
        if configured_accelerators:
            routing_spec.update({
                'request_accelerator_compatibility_version':
                    serve_constants.LB_REQUEST_ACCELERATORS_VERSION,
                'configured_accelerators': configured_accelerators,
            })
        return routing_spec

    def _get_routing_spec(self) -> dict[str, Any] | None:
        """Return the routing spec for the load_balancer_sync response.

        [boltz fork] The external load balancer fetches its routing
        configuration -- load-balancing policy, per-replica target QPS, and
        stream timeout -- over the sync channel instead of static launch
        args, so a `sky serve update` that only changes these fields reaches
        a running LB without re-rolling it. The source of truth is the
        controller's in-memory runtime state, not a per-sync DB reread: the
        update handler commits the new version row before transitioning the
        live autoscaler/replica-manager, so reading the DB here can expose a
        newer routing spec than the runtime has actually applied. Keeping an
        immutable in-memory snapshot removes that mismatch window and avoids
        steady-state DB reads on the hottest serve control-plane path.
        """
        return getattr(self, '_routing_spec', None)

    def _load_service_for_update(self, version: int, yaml_content: str) -> Any:
        """Parse a new update or reuse an exact legacy committed spec.

        An old dynamic_fallback_per_gpu YAML can be invalid under today's
        automatically activated logical contract. A lost-response retry must
        therefore be recognized before parsing. The transactional commit still
        rechecks immutability and lifecycle ownership.
        """
        if serve_state.get_yaml_content(self._service_name,
                                        version) == yaml_content:
            persisted = serve_state.get_spec(self._service_name, version)
            if persisted is None:
                raise RuntimeError(
                    f'Service version {version} has committed YAML but its '
                    'authoritative specification is missing.')
            return persisted
        return serve.SkyServiceSpec.from_yaml_str(yaml_content)

    def _prepare_controller_config_update(
            self, version: int, expected_digest: str,
            snapshot_id: str) -> _PreparedControllerConfig:
        """Validate a staged config and bind it to the HA recovery script."""
        live_path = serve_utils.generate_versioned_config_yaml_file_name(
            self._service_name, version, self._resource_scope)
        staged_path = serve_utils.generate_staged_config_yaml_file_name(
            self._service_name,
            version,
            self._resource_scope,
            snapshot_id=snapshot_id)
        recovery_script = serve_state.get_ha_recovery_script(self._service_name)
        if recovery_script is None:
            raise RuntimeError('Service HA recovery script is missing; '
                               'refusing a non-durable config refresh.')
        committed_yaml = serve_state.get_yaml_content(self._service_name,
                                                      version)
        committed_snapshot = (serve_state.get_version_controller_config(
            self._service_name, version)
                              if committed_yaml is not None else None)
        try:
            config_bytes = serve_utils.secure_staged_controller_config(
                staged_path, expected_digest)
        except FileNotFoundError:
            config_bytes = None
        if config_bytes is not None:
            if committed_yaml is None:
                serve_utils.write_config_snapshot_receipt(
                    staged_path, version, snapshot_id, expected_digest)
                durable_bytes = serve_utils.sanitize_ha_recovery_config_bytes(
                    config_bytes)
                durable_digest = hashlib.sha256(durable_bytes).hexdigest()
                source_is_staged = True
                source_is_live = False
            else:
                if committed_snapshot is None:
                    raise RuntimeError('Committed controller config snapshot '
                                       'is unavailable for retry.')
                (durable_bytes, durable_digest,
                 committed_snapshot_id) = committed_snapshot
                if committed_snapshot_id != snapshot_id:
                    raise RuntimeError(
                        'Committed controller config snapshot ID does not '
                        'match the API-server submission.')
                # Never mint or overwrite a receipt after the immutable row is
                # committed. A retry may upload different raw bytes using the
                # same nonce and safe projection; those bytes must never gain
                # authority over the committed version. Preserve raw fields
                # only when the pre-commit receipt still proves exact identity.
                receipt = serve_utils.get_config_snapshot_receipt(staged_path)
                receipt_matches = (
                    receipt is not None and receipt['version'] == version and
                    receipt['snapshot_id'] == snapshot_id and
                    receipt['source_digest'] == expected_digest and
                    hashlib.sha256(config_bytes).hexdigest() == expected_digest)
                if (receipt_matches and hashlib.sha256(
                        serve_utils.sanitize_ha_recovery_config_bytes(
                            config_bytes)).hexdigest() == durable_digest):
                    source_is_staged = True
                else:
                    config_bytes = durable_bytes
                    source_is_staged = False
                source_is_live = False
        else:
            # os.replace consumes the staged file. A lost 200 response must be
            # retryable after that point, but absence before the version commit
            # is never valid and must not silently admit the old live config.
            if committed_yaml is None:
                raise RuntimeError('Staged controller config snapshot is '
                                   'missing before version commit.')
            if committed_snapshot is None:
                raise RuntimeError('Committed controller config snapshot is '
                                   'unavailable for retry.')
            (durable_bytes, durable_digest,
             committed_snapshot_id) = committed_snapshot
            if committed_snapshot_id != snapshot_id:
                raise RuntimeError(
                    'Committed controller config snapshot ID does not match '
                    'the API-server submission.')
            live_receipt = serve_utils.get_config_snapshot_receipt(live_path)
            if (live_receipt is not None and
                    live_receipt['snapshot_id'] == snapshot_id and
                    live_receipt['source_digest'] != expected_digest):
                raise RuntimeError(
                    'Committed raw controller config receipt does not match '
                    'the retried API-server digest.')
            live_bytes = serve_utils.read_verified_controller_config(
                live_path, version, snapshot_id, expected_digest)
            if (live_bytes is not None and hashlib.sha256(
                    serve_utils.sanitize_ha_recovery_config_bytes(
                        live_bytes)).hexdigest() == durable_digest):
                # Preserve exact raw fields on a same-pod lost-response retry.
                # Full child/pod recovery removes the receipt and deliberately
                # falls back to the DB-bound safe projection below.
                config_bytes = live_bytes
                source_is_live = True
            else:
                # The random nonce identifies the already-committed request
                # after a pod loss. Do not persist the source digest: stripped
                # low-entropy secrets would otherwise gain an offline verifier.
                config_bytes = durable_bytes
                source_is_live = False
            source_is_staged = False

        recovery_script = (serve_utils.strip_legacy_ha_recovery_config_payload(
            recovery_script, live_path))

        expected_workspace = self._replica_manager.workspace
        try:
            config = serve_utils.parse_and_validate_version_controller_config(
                config_bytes, expected_workspace,
                'staged Serve controller config')
        except Exception as e:  # pylint: disable=broad-except
            raise RuntimeError('Staged controller config snapshot is invalid: '
                               f'{common_utils.format_exception(e)}') from None
        try:
            serve_utils.parse_and_validate_version_controller_config(
                durable_bytes, expected_workspace,
                'durable Serve controller config')
        except Exception as e:  # pylint: disable=broad-except
            raise RuntimeError('Durable controller config snapshot is invalid: '
                               f'{common_utils.format_exception(e)}') from None
        legacy_snapshot = None
        protocol_active = (serve_state.get_version_controller_config(
            self._service_name, self._applied_version) is not None)
        if (source_is_staged and committed_yaml is None and
                not protocol_active):
            current_config_path = os.environ.get(
                skypilot_config.ENV_VAR_SKYPILOT_CONFIG,
                serve_utils.generate_remote_config_yaml_file_name(
                    self._service_name, self._resource_scope))
            expanded_current_path = os.path.expanduser(current_config_path)
            try:
                with open(expanded_current_path, 'rb') as live_config_file:
                    legacy_bytes = (
                        serve_utils.sanitize_ha_recovery_config_bytes(
                            live_config_file.read()))
            except OSError:
                recovery_version = serve_state.get_recovery_version_spec(
                    self._service_name)
                committed_legacy = (serve_state.get_version_controller_config(
                    self._service_name, recovery_version[0])
                                    if recovery_version is not None else None)
                if committed_legacy is None:
                    raise RuntimeError(
                        'Current controller config is unavailable for '
                        'legacy-version recovery backfill.') from None
                legacy_bytes = committed_legacy[0]
            try:
                serve_utils.parse_and_validate_version_controller_config(
                    legacy_bytes, expected_workspace,
                    'legacy Serve controller config')
            except Exception as e:  # pylint: disable=broad-except
                raise RuntimeError(
                    'Legacy controller config snapshot is invalid: '
                    f'{common_utils.format_exception(e)}') from None
            legacy_digest = hashlib.sha256(legacy_bytes).hexdigest()
            # Historical versions all used the one frozen controller config.
            # The safe digest is a stable, non-secret migration identity.
            legacy_snapshot = (legacy_bytes, legacy_digest, legacy_digest)
        return _PreparedControllerConfig(config, self._service_name, live_path,
                                         staged_path, recovery_script, version,
                                         snapshot_id, expected_digest,
                                         durable_bytes, durable_digest,
                                         source_is_staged, source_is_live,
                                         legacy_snapshot)

    @staticmethod
    def _run_with_prepared_config(prepared: _PreparedControllerConfig,
                                  callback: Callable[[], Any]) -> Any:
        """Run admission under a request-local config without publishing it."""

        def _run() -> Any:
            sky_context.initialize()
            with skypilot_config.replace_skypilot_config_in_memory(
                    prepared.config):
                return callback()

        return contextvars.Context().run(_run)

    @staticmethod
    def _discard_prepared_controller_config(
            prepared: _PreparedControllerConfig | None) -> None:
        if prepared is not None and prepared.source_is_staged:
            serve_utils.remove_staged_controller_config(prepared.staged_path)

    @staticmethod
    def _schedule_supervised_recovery() -> None:
        """Terminate this child after its caller finishes durable writes."""

        def _terminate_for_recovery() -> None:
            time.sleep(0.1)
            os.kill(os.getpid(), signal.SIGTERM)

        threading.Thread(target=_terminate_for_recovery, daemon=True).start()

    def _get_actuation_epoch_lock(self) -> threading.RLock:
        lock = getattr(self, '_actuation_epoch_lock', None)
        if lock is None:
            # Compatibility for tests and embedders that bypass __init__.
            lock = threading.RLock()
            self._actuation_epoch_lock = lock
        return lock

    def _get_actuation_stop(self) -> threading.Event:
        stop_event = getattr(self, '_actuation_stop', None)
        if stop_event is None:
            # Compatibility for tests and embedders that bypass __init__.
            stop_event = threading.Event()
            self._actuation_stop = stop_event
        return stop_event

    def _get_update_reconciler_stop(self) -> threading.Event:
        stop_event = getattr(self, '_update_reconciler_stop', None)
        if stop_event is None:
            # Compatibility for tests and embedders that bypass __init__.
            stop_event = threading.Event()
            self._update_reconciler_stop = stop_event
        return stop_event

    def _fence_actuation_for_update_recovery(self) -> None:
        """Irreversibly stop this partial child from changing fleet state."""
        self._update_recovery_required = True
        self._get_actuation_stop().set()
        update_reconciler_stop = getattr(self, '_update_reconciler_stop', None)
        if update_reconciler_stop is not None:
            update_reconciler_stop.set()
        self._replica_manager.fence_launches_for_update_recovery()

    def _mark_controller_applied_version(self, version: int) -> bool:
        """Persist an exact recovery baseline under this controller owner."""
        if self._service_hash is None:
            return True
        return serve_state.mark_version_controller_applied(
            self._service_name,
            version,
            self._service_hash,
            expected_controller_owner=self._controller_owner)

    @staticmethod
    def _install_controller_config(prepared: _PreparedControllerConfig) -> None:
        """Publish a config only after its version and recovery are durable."""
        live_path = os.path.expanduser(prepared.live_path)

        def _install_globally() -> None:
            # An empty Context guarantees the already-validated config is
            # atomically published to the process-global snapshot.
            with filelock.FileLock(
                    skypilot_config.get_skypilot_config_lock_path()):
                installed_bytes: bytes | None
                if prepared.source_is_staged:
                    installed_bytes = (
                        serve_utils.promote_staged_controller_config(
                            prepared.live_path, prepared.staged_path,
                            prepared.version, prepared.snapshot_id,
                            prepared.source_digest))
                elif prepared.source_is_live:
                    installed_bytes = (
                        serve_utils.read_verified_controller_config(
                            prepared.live_path, prepared.version,
                            prepared.snapshot_id, prepared.source_digest))
                    if installed_bytes is None:
                        raise RuntimeError('Committed raw controller config '
                                           'changed before retry install.')
                else:
                    installed_bytes = (
                        serve_utils.restore_version_controller_config(
                            prepared.service_name,
                            prepared.version,
                            prepared.live_path,
                            prepared.staged_path,
                            expected_workspace=(prepared.config.get_nested(
                                keys=('active_workspace',),
                                default_value=None))))
                    if installed_bytes is None:
                        raise RuntimeError('Committed controller config is '
                                           'unavailable for installation.')
                expected_installed_digest = (
                    prepared.source_digest if prepared.source_is_staged or
                    prepared.source_is_live else prepared.durable_digest)
                if (hashlib.sha256(installed_bytes).hexdigest()
                        != expected_installed_digest):
                    raise RuntimeError(
                        'Controller config snapshot changed between admission '
                        'and installation.')
                skypilot_config.install_internal_config_snapshot(
                    prepared.config, live_path)
                expected_workspace = prepared.config.get_nested(
                    keys=('active_workspace',), default_value=None)
                if skypilot_config.get_active_workspace() != expected_workspace:
                    raise RuntimeError(
                        'Installed controller config changed the durable '
                        'service workspace.')

        contextvars.Context().run(_install_globally)

    def _transition_load_balancer_mode(
            self,
            enable_ha: bool,
            target_spec: serve.SkyServiceSpec,
            expected_service_hash: str | None = None,
            expected_lifecycle_epoch: int | None = None) -> None:
        """Run an explicit stable-Service-preserving HA migration/rollback."""
        state = serve_state.get_lb_cutover_state(self._service_name)
        if state is None:
            raise RuntimeError('Service LB cutover state is missing.')
        owner_record = serve_state.get_service_controller_owner(
            self._service_name, include_lb_state=True)
        actual_owner = ((owner_record.get('controller_pid'),
                         owner_record.get('controller_ip'))
                        if owner_record is not None else None)
        if (owner_record is None or not owner_record.get('hash') or
                owner_record.get('lifecycle_epoch') is None or
                actual_owner != self._controller_owner):
            raise RuntimeError('Service ownership changed before LB migration.')
        if (expected_service_hash is not None and
                owner_record['hash'] != expected_service_hash):
            raise RuntimeError(
                'Service incarnation changed before LB migration.')
        if (expected_lifecycle_epoch is not None and
                owner_record.get('lifecycle_epoch')
                != expected_lifecycle_epoch):
            raise RuntimeError('Service lifecycle changed before LB migration.')
        service_hash = str(owner_record['hash'])
        assert actual_owner is not None
        owner = actual_owner
        lifecycle_epoch = int(owner_record['lifecycle_epoch'])
        resuming = False
        if enable_ha and state.enabled:
            if state.phase is lb_ha.LbCutoverPhase.STABLE:
                self._lb_ha_enabled = True
                return
            if state.phase is not lb_ha.LbCutoverPhase.MIGRATING:
                raise RuntimeError(
                    'Load balancer mode cannot change while another cutover '
                    f'is {state.phase.value}.')
            resuming = True
        elif not enable_ha and not state.enabled:
            if state.phase is not lb_ha.LbCutoverPhase.STABLE:
                raise RuntimeError(
                    'Disabled load balancer HA has an invalid non-stable '
                    f'cutover phase {state.phase.value}.')
            self._lb_ha_enabled = False
            return
        elif not enable_ha and state.phase is lb_ha.LbCutoverPhase.ROLLING_BACK:
            resuming = True
        elif state.phase is not lb_ha.LbCutoverPhase.STABLE:
            raise RuntimeError('Load balancer mode cannot change while another '
                               f'cutover is {state.phase.value}.')
        if enable_ha:
            lb_k8s.require_lb_ha_runtime()
        if enable_ha and not resuming:
            started = serve_state.begin_lb_ha_migration(self._service_name,
                                                        service_hash, owner,
                                                        lifecycle_epoch)
            if started:
                self._lb_session_ledger = lb_ha.LbSessionLedger(
                    serve_constants.LB_ROLE_REPORT_MAX_AGE_SECONDS,
                    serve_constants.LB_PROMOTION_OCCUPANCY_MAX_AGE_SECONDS)
                self._lb_occupancy_contract_known = False
                self._lb_last_demand_snapshot = None
        elif not enable_ha and not resuming:
            if state.active_slot is None:
                raise RuntimeError('HA rollback has no committed active slot.')
            started = serve_state.begin_lb_ha_rollback(self._service_name,
                                                       service_hash, owner,
                                                       lifecycle_epoch,
                                                       state.active_slot,
                                                       state.generation)
        else:
            started = True
        if not started:
            raise RuntimeError('LB mode transition lost its durable CAS fence.')
        if enable_ha:
            self._lb_ha_enabled = True
            if self._lb_session_ledger is None:
                self._lb_session_ledger = lb_ha.LbSessionLedger(
                    serve_constants.LB_ROLE_REPORT_MAX_AGE_SECONDS,
                    serve_constants.LB_PROMOTION_OCCUPANCY_MAX_AGE_SECONDS)
                self._lb_occupancy_contract_known = False
                self._lb_last_demand_snapshot = None
        termination_grace = lb_k8s.lb_termination_grace_period_seconds(
            target_spec.lb_stream_timeout_seconds,
            target_spec.graceful_drain_seconds)
        lb_k8s.prepare_lb_mode_transition(
            self._service_name,
            termination_grace,
            service_hash,
            enable_ha,
            continue_guard=self._owns_current_service,
            resource_scope=self._resource_scope)
        deadline = (time.monotonic() +
                    serve_constants.LB_DEPLOYMENT_READY_TIMEOUT_SECONDS)
        while time.monotonic() < deadline:
            current = serve_state.get_lb_cutover_state(self._service_name)
            if (current is not None and current.enabled == enable_ha and
                    current.phase is lb_ha.LbCutoverPhase.STABLE):
                self._lb_ha_enabled = enable_ha
                self._lb_drain_timeout_seconds = termination_grace
                if not enable_ha:
                    self._lb_session_ledger = None
                    self._lb_last_demand_snapshot = None
                return
            if not self._owns_current_service():
                raise RuntimeError('Service ownership changed during LB mode '
                                   'transition.')
            time.sleep(1)
        raise RuntimeError('Timed out waiting for the stable load balancer '
                           'selector mode transition to commit.')

    def _set_load_balancer_high_availability(
            self, enabled: bool, expected_service_hash: str,
            expected_lifecycle_epoch: int) -> None:
        """Apply one fenced LB-only topology update."""
        target_spec = serve_state.get_spec(self._service_name,
                                           self._committed_version)
        if target_spec is None:
            raise RuntimeError('Current service spec is missing.')
        self._transition_load_balancer_mode(
            enabled,
            target_spec,
            expected_service_hash=expected_service_hash,
            expected_lifecycle_epoch=expected_lifecycle_epoch)

    def _commit_service_update(
        self,
        version: int,
        service: Any,
        yaml_content: str,
        update_mode: serve_utils.UpdateMode,
        requested_service_hash: str | None,
        lifecycle_epoch: int | None,
        submitted_yaml_content: str | None = None,
        prepared_config: _PreparedControllerConfig | None = None
    ) -> fastapi.Response:
        """Durably accept one immutable version and schedule its apply."""
        authoritative_retry_service = None
        persisted_yaml = serve_state.get_yaml_content(self._service_name,
                                                      version)
        if persisted_yaml == yaml_content:
            authoritative_retry_service = serve_state.get_spec(
                self._service_name, version)
        validation_service = authoritative_retry_service or service
        if (authoritative_retry_service is None and
                isinstance(validation_service, serve.SkyServiceSpec) and
                not validation_service.lb_high_availability_specified):
            # Default-on applies when a service is created. An existing
            # service whose YAML predates this field inherits its durable mode
            # until a dedicated selector migration is requested explicitly.
            service = service.copy(lb_high_availability=self._lb_ha_enabled)
            validation_service = service
        if (authoritative_retry_service is None and
                isinstance(validation_service, serve.SkyServiceSpec) and
                validation_service.lb_high_availability_specified and
                validation_service.lb_high_availability != self._lb_ha_enabled):
            self._transition_load_balancer_mode(
                validation_service.lb_high_availability,
                validation_service,
                expected_service_hash=requested_service_hash,
                expected_lifecycle_epoch=lifecycle_epoch)
        current_autoscaler = getattr(self, '_autoscaler', None)
        if (authoritative_retry_service is None and getattr(
                current_autoscaler, 'replica_unit', None) == 'logical' and
                getattr(validation_service, 'replica_unit',
                        'physical_backend') != 'logical'):
            self._discard_prepared_controller_config(prepared_config)
            return responses.JSONResponse(content={
                'message': 'An existing dynamic_fallback_per_gpu service '
                           'cannot switch in place to physical-backend replica '
                           'semantics. Create a new service for that migration.'
            },
                                          status_code=400)
        if (getattr(validation_service, 'replica_unit',
                    'physical_backend') == 'logical' and
                update_mode == serve_utils.UpdateMode.BLUE_GREEN):
            self._discard_prepared_controller_config(prepared_config)
            return responses.JSONResponse(content={
                'message': 'dynamic_fallback_per_gpu services currently '
                           'require rolling updates. Blue-green activation is '
                           'based on physical backend counts and cannot '
                           'preserve the per-GPU capacity target.'
            },
                                          status_code=400)
        placement_catalog = serve_state.get_placement_catalog(
            self._service_name, version)
        if (placement_catalog is not None and
                getattr(validation_service, 'spot_placer', None) is not None):
            # A catalog reused from an earlier commit enumerates the locations
            # that existed when it was built. Adding a Kubernetes context to a
            # service afterwards therefore has no effect: the context never
            # enters the catalog, the placer never lists it as zero-cost, and
            # the reserved-fill broker never claims a pool for it. That failure
            # is silent -- the spec, the workspace and the cluster all look
            # correct while the capacity is simply never used. Observed in
            # production with a spec-declared context absent from six
            # consecutive versions' catalogs. Rebuild rather than inherit an
            # enumeration the spec has outgrown.
            missing = _catalog_missing_task_contexts(yaml_content,
                                                     placement_catalog)
            if missing:
                logger.info(
                    f'Rebuilding the placement catalog for version {version}: '
                    f'the inherited catalog is missing Kubernetes context(s) '
                    f'{sorted(missing)} that the task declares.')
                placement_catalog = None
        needs_catalog = (getattr(validation_service, 'spot_placer', None)
                         is not None and placement_catalog is None)
        needs_logical_validation = (
            authoritative_retry_service is None and
            getattr(validation_service, 'uses_logical_replicas', False) is True)
        if needs_catalog or needs_logical_validation:
            try:
                if needs_catalog:
                    update_task = (replica_managers.load_task_with_service_spec(
                        yaml_content, validation_service))
                else:
                    update_task = task_lib.Task.from_yaml_str(yaml_content)
                if (needs_logical_validation and update_task.num_nodes != 1):
                    raise ValueError(
                        'dynamic_fallback_per_gpu currently supports only '
                        'single-node services. Multi-node replica routing '
                        'does not yet define a safe logical capacity contract.')
                if needs_catalog:

                    def _build_catalog() -> Any:
                        return spot_placer.SpotPlacer.build_catalog(
                            validation_service,
                            update_task,
                            workspace=self._replica_manager.workspace)

                    if prepared_config is None:
                        built_catalog = _build_catalog()
                    else:
                        built_catalog = self._run_with_prepared_config(
                            prepared_config, _build_catalog)
                    assert built_catalog is not None
                    placement_catalog = built_catalog.to_dict()
                    # A freshly built catalog that still omits a declared
                    # context means the context is unreachable or not allowed
                    # in this service's workspace, not that the catalog is
                    # stale. Say so: the alternative is a service that runs
                    # indefinitely without the capacity its spec asks for.
                    still_missing = _catalog_missing_task_contexts(
                        yaml_content, placement_catalog)
                    if still_missing:
                        logger.error(
                            'Placement catalog for version '
                            f'{version} omits Kubernetes context(s) '
                            f'{sorted(still_missing)} declared by the task. '
                            'Reserved fill will not claim a pool for them. '
                            'Check that each context is reachable and allowed '
                            'in workspace '
                            f'{self._replica_manager.workspace!r}.')
            except (ValueError, RuntimeError) as e:
                self._discard_prepared_controller_config(prepared_config)
                return responses.JSONResponse(content={'message': str(e)},
                                              status_code=400)
        catalog_kwargs: dict[str, Any] = ({
            'placement_catalog': placement_catalog
        } if placement_catalog is not None else {})
        recovery_kwargs: dict[str, Any] = ({
            # The script is service-global and points at the latest committed
            # generation. A delayed retry of an older immutable version must
            # compare only that version's snapshot, never rewrite or compare
            # the newer service-global recovery pointer.
            **({
                'ha_recovery_script': prepared_config.recovery_script,
                'legacy_controller_config_snapshot': (prepared_config.legacy_snapshot),
                'legacy_controller_applied_version': (self._applied_version if prepared_config.legacy_snapshot is not None else None),
            } if authoritative_retry_service is None else {}),
            'controller_config': prepared_config.durable_bytes,
            'controller_config_digest': prepared_config.durable_digest,
            'controller_config_snapshot_id': prepared_config.snapshot_id,
        } if prepared_config is not None else {})
        result = serve_state.add_or_update_version(
            self._service_name,
            version,
            service,
            yaml_content,
            submitted_yaml_content=submitted_yaml_content,
            expected_service_hash=(requested_service_hash or
                                   self._service_hash),
            expected_lifecycle_epoch=lifecycle_epoch,
            expected_controller_owner=self._controller_owner,
            **catalog_kwargs,
            **recovery_kwargs)
        if result not in (serve_state.VersionCommitResult.COMMITTED,
                          serve_state.VersionCommitResult.IDEMPOTENT_RETRY):
            self._discard_prepared_controller_config(prepared_config)
        if result is serve_state.VersionCommitResult.REJECTED:
            return responses.JSONResponse(content={
                'message': 'Service lifecycle ownership changed or entered '
                           'terminal status before the update was committed.'
            },
                                          status_code=409)
        if result is serve_state.VersionCommitResult.SEMANTIC_CONFLICT:
            return responses.JSONResponse(content={
                'message': 'An existing dynamic_fallback_per_gpu service '
                           'cannot switch in place to physical-backend replica '
                           'semantics. Create a new service for that migration.'
            },
                                          status_code=400)
        if result is serve_state.VersionCommitResult.LB_HA_CONFLICT:
            return responses.JSONResponse(content={
                'message':
                    'load_balancer.high_availability changed concurrently '
                    'with this update. Re-read service status and retry the '
                    'explicit migration or rollback.'
            },
                                          status_code=400)
        if result is serve_state.VersionCommitResult.CONTENT_CONFLICT:
            return responses.JSONResponse(content={
                'message': f'Service version {version} was already committed '
                           'with different content. Re-run the update to '
                           'allocate a new version.'
            },
                                          status_code=409)
        if result is serve_state.VersionCommitResult.STALE_VERSION:
            return responses.JSONResponse(content={
                'message': f'Service version {version} was superseded by a '
                           'newer committed version before it could commit. '
                           'Re-run the update to allocate a new version.'
            },
                                          status_code=409)
        if result is serve_state.VersionCommitResult.IDEMPOTENT_RETRY:
            # The caller reconstructed this YAML with today's hidden defaults,
            # which may differ from an older stored pickled spec (notably a
            # pre-activation dynamic_fallback_per_gpu version). Apply only the
            # immutable authoritative bytes already committed for this version.
            persisted_service = (authoritative_retry_service or
                                 serve_state.get_spec(self._service_name,
                                                      version))
            if persisted_service is None:
                return responses.JSONResponse(content={
                    'message': f'Service version {version} was committed but '
                               'its authoritative specification is missing.'
                },
                                              status_code=409)
            service = persisted_service
            # A committed retry is an acknowledgement only. Reinstalling or
            # re-enqueueing it can roll the process policy back after a newer
            # version has applied, or resurrect a quarantined generation.
            self._discard_retry_stage_if_unused(version, prepared_config)
            content = {'message': 'Success'}
            if prepared_config is not None:
                content['config_snapshot_id'] = prepared_config.snapshot_id
            content.update(self._get_update_status())
            return responses.JSONResponse(content=content, status_code=200)

        logger.info(f'Committed update to version {version}: {service}')
        self._record_committed_update(version, service, update_mode,
                                      prepared_config)
        content = {'message': 'Success'}
        if prepared_config is not None:
            content['config_snapshot_id'] = prepared_config.snapshot_id
        content.update(self._get_update_status())
        return responses.JSONResponse(content=content, status_code=200)

    def _discard_retry_stage_if_unused(
            self, version: int,
            prepared_config: _PreparedControllerConfig | None) -> None:
        if prepared_config is None:
            return
        with self._update_condition:
            stage_in_use = any(
                update is not None and update.version == version
                for update in (self._pending_update,
                               getattr(self, '_applying_update', None)))
        if not stage_in_use:
            serve_utils.remove_staged_controller_config(
                prepared_config.staged_path)

    def _record_committed_update(
            self,
            version: int,
            service: Any,
            update_mode: serve_utils.UpdateMode,
            prepared_config: _PreparedControllerConfig | None = None) -> None:
        """Wake the reconciler after the update's durable commit."""
        update = _PendingServiceUpdate(version, service, update_mode,
                                       time.time(), prepared_config)
        scheduled = False
        discarded: _PendingServiceUpdate | None = None
        with self._update_condition:
            self._committed_version = max(self._committed_version, version)
            if version > self._applied_version:
                pending = self._pending_update
                if pending is None or version > pending.version:
                    # Coalesce versions that commit before the worker starts.
                    # This matches controller recovery, which also boots only
                    # the newest committed version.
                    self._pending_update = update
                    if pending is not getattr(self, '_applying_update', None):
                        discarded = pending
                    scheduled = True
                    self._update_apply_error = None
                    self._update_apply_failures = 0
            self._update_condition.notify()
        if discarded is not None:
            self._discard_prepared_controller_config(discarded.prepared_config)
        if not scheduled:
            self._discard_prepared_controller_config(prepared_config)
        if scheduled:
            # Publish this before the reconciler waits on the manager lock.
            # Large stale scale-up batches use the signal to yield to the newer
            # version.
            self._replica_manager.notify_version_pending(version)

    def _get_update_status(self) -> dict[str, Any]:
        """Return committed-versus-applied update visibility."""
        with self._update_condition:
            pending = self._pending_update
            apply_lag = (None if pending is None else max(
                0, int(time.time() - pending.committed_at)))
            return {
                'committed_version': self._committed_version,
                'applied_version': self._applied_version,
                'update_apply_pending': pending is not None,
                'update_apply_lag_seconds': apply_lag,
                'update_apply_error': self._update_apply_error,
                'update_apply_failures': self._update_apply_failures,
                'quarantined_version': self._quarantined_version,
                'quarantined_at': self._quarantined_at,
                'quarantine_reason': self._quarantine_reason,
            }

    def _update_still_authorized(self) -> bool:
        """Whether this controller still owns a nonterminal service."""
        owner = serve_state.get_service_controller_owner(self._service_name)
        if owner is None or owner['status'] in (
                serve_state.ServiceStatus.terminal_statuses()):
            return False
        if (self._service_hash is not None and
                owner['hash'] != self._service_hash):
            return False
        if self._controller_owner is not None:
            current_owner = (owner['controller_pid'], owner['controller_ip'])
            if current_owner != self._controller_owner:
                return False
        return True

    def _drop_pending_update(self, update: _PendingServiceUpdate) -> None:
        with self._update_condition:
            if self._pending_update is update:
                self._pending_update = None
        self._replica_manager.clear_pending_version(update.version)
        self._discard_prepared_controller_config(update.prepared_config)

    def _record_retryable_update_failure(self,
                                         update: _PendingServiceUpdate,
                                         error: Exception,
                                         wait: bool,
                                         context: str = 'apply') -> bool:
        """Record a retryable failure and preserve the pending-version fence."""
        exception_str = common_utils.format_exception(error)
        with self._update_condition:
            retry_same_update = self._pending_update is update
            if retry_same_update:
                self._update_apply_error = exception_str
                self._update_apply_failures += 1
        # _apply_service_update clears its pending-version signal in a finally
        # block. Re-publish it while this durable version waits for a retry.
        self._replica_manager.notify_version_pending(update.version)
        retry_message = ('will retry'
                         if retry_same_update else 'was superseded')
        logger.error(f'Failed to {context} committed service version '
                     f'{update.version}; {retry_message}: {exception_str}')
        with ux_utils.enable_traceback():
            logger.error(f'  Traceback: {traceback.format_exc()}')
        if retry_same_update and wait:
            with self._update_condition:
                self._update_condition.wait_for(
                    lambda: self._pending_update is not update,
                    timeout=_UPDATE_RETRY_BACKOFF_SECONDS)
        return not retry_same_update

    def _reconcile_pending_update_once(self, wait: bool = False) -> bool:
        """Apply one pending update; optionally wait through retry backoff."""
        if getattr(self, '_update_recovery_required', False):
            return True
        with self._update_condition:
            while (self._pending_update is None or
                   self._pending_update.version <= self._applied_version):
                if not wait:
                    return True
                self._update_condition.wait()
            update = self._pending_update
            self._applying_update = update

        try:
            return self._reconcile_selected_update(update, wait)
        finally:
            with self._update_condition:
                if self._applying_update is update:
                    self._applying_update = None
                discard_prepared = self._pending_update is not update
                self._update_condition.notify_all()
            if discard_prepared:
                self._discard_prepared_controller_config(update.prepared_config)

    def _reconcile_selected_update(self, update: _PendingServiceUpdate,
                                   wait: bool) -> bool:
        """Apply one update already fenced as the active reconciliation."""
        try:
            if not self._update_still_authorized():
                logger.info(
                    f'Dropping committed service version {update.version}: '
                    'the controller no longer owns a live service.')
                self._drop_pending_update(update)
                return True
            self._apply_service_update(update.version, update.service,
                                       update.update_mode,
                                       update.prepared_config)
        except DeterministicServiceUpdateError as e:
            exception_str = common_utils.format_exception(e)
            quarantined_at = time.time()
            try:
                quarantined = serve_state.quarantine_version(
                    self._service_name,
                    update.version,
                    exception_str,
                    quarantined_at=quarantined_at,
                    expected_service_hash=self._service_hash,
                    expected_controller_owner=self._controller_owner)
            except Exception as quarantine_error:  # pylint: disable=broad-except
                return self._record_retryable_update_failure(
                    update, quarantine_error, wait, context='quarantine')
            if not quarantined:
                # Do not fail open until the rejection is durable. A missing
                # or uncommitted row is treated like a transient apply failure.
                return self._record_retryable_update_failure(
                    update,
                    RuntimeError(
                        'The committed version row could not be quarantined.'),
                    wait,
                    context='quarantine')
            with self._update_condition:
                if self._pending_update is update:
                    self._pending_update = None
                    self._update_apply_error = exception_str
                    self._update_apply_failures = 1
                self._quarantined_version = update.version
                self._quarantined_at = quarantined_at
                self._quarantine_reason = exception_str
                self._update_condition.notify()
            self._replica_manager.clear_pending_version(update.version)
            logger.error(
                f'Quarantined committed service version {update.version}; '
                f'continuing applied version {self._applied_version}: '
                f'{exception_str}')
            return True
        except ServiceUpdateRequiresRecoveryError as e:
            # Keep the pending-version launch fence asserted until the parent
            # replaces this child. The raw stage may have been consumed and
            # manager/autoscaler state may be partial, so an in-process retry
            # is not an admissible recovery mechanism.
            exception_str = common_utils.format_exception(e)
            with self._update_condition:
                self._update_apply_error = exception_str
                self._update_apply_failures += 1
            logger.error(f'Committed service version {update.version} '
                         'requires supervised controller recovery: '
                         f'{exception_str}')
            return True
        except Exception as e:  # pylint: disable=broad-except
            return self._record_retryable_update_failure(update, e, wait)

        with self._update_condition:
            self._applied_version = max(self._applied_version, update.version)
            if self._pending_update is update:
                self._pending_update = None
            self._update_apply_error = None
            self._update_apply_failures = 0
        logger.info(f'Applied committed service version {update.version} '
                    f'after {time.time() - update.committed_at:.1f}s.')
        return True

    def _run_update_reconciler(self) -> None:
        """Continuously converge runtime state to the newest committed spec."""
        stop_event = getattr(self, '_update_reconciler_stop', None)
        while (not getattr(self, '_update_recovery_required', False) and
               (stop_event is None or not stop_event.is_set())):
            self._reconcile_pending_update_once(wait=True)

    def _run_orphaned_config_stage_sweeper(self) -> None:
        """Remove expired raw stages that no update handler can reconcile."""
        stop_event = self._update_reconciler_stop
        while not stop_event.is_set():
            try:
                # The update endpoint holds this same lock from raw-stage
                # admission through its commit/cleanup finally block. GC can
                # therefore never unlink a stage being consumed by this child.
                with self._update_lock:
                    removed_versions = (
                        serve_utils.gc_orphaned_staged_controller_configs(
                            self._service_name, self._resource_scope))
                if removed_versions:
                    logger.info(
                        'Removed expired uncommitted controller config stages '
                        f'for service {self._service_name!r}, versions '
                        f'{removed_versions}.')
            except Exception as e:  # pylint: disable=broad-except
                # Database uncertainty must preserve the raw stage. Retry on
                # the next bounded sweep instead of weakening commit safety.
                logger.warning(
                    'Could not reconcile orphaned controller config stages '
                    f'for service {self._service_name!r}: '
                    f'{common_utils.format_exception(e)}')
            if stop_event.wait(serve_constants.
                               ORPHANED_CONFIG_STAGE_SWEEP_INTERVAL_SECONDS):
                return

    def _quarantine_unrecoverable_rollout(
        self,
        failure: autoscalers.UnrecoverableRolloutFailure,
    ) -> bool:
        """Durably reject a never-ready runtime before controller recovery."""
        with self._routing_state_lock:
            if failure.version != self._applied_version:
                logger.info(
                    f'Ignoring stale rollout-failure signal for version '
                    f'{failure.version}; applied version is '
                    f'{self._applied_version}.')
                return False
        quarantined_at = time.time()
        try:
            quarantined = serve_state.quarantine_version(
                self._service_name,
                failure.version,
                failure.reason,
                quarantined_at=quarantined_at,
                expected_service_hash=self._service_hash,
                expected_controller_owner=self._controller_owner)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                f'Failed to quarantine unrecoverable service version '
                f'{failure.version}: {common_utils.format_exception(e)}')
            return False
        if not quarantined:
            logger.error(f'Refusing to fail open from service version '
                         f'{failure.version}: its quarantine write lost the '
                         'controller ownership fence.')
            return False
        with self._update_condition:
            self._quarantined_version = failure.version
            self._quarantined_at = quarantined_at
            self._quarantine_reason = failure.reason
            self._update_apply_error = failure.reason
            self._update_apply_failures = 1
        self._fence_actuation_for_update_recovery()
        logger.error(
            f'Quarantined never-ready service version {failure.version}; '
            'terminating this controller child so recovery can elect the '
            f'proven active runtime: {failure.reason}')
        return True

    def _apply_service_update(
            self,
            version: int,
            service: Any,
            update_mode: serve_utils.UpdateMode,
            prepared_config: _PreparedControllerConfig | None = None) -> None:
        """Apply a persisted update to the live controller state."""
        try:

            def _preflight() -> Any:
                if (getattr(self._autoscaler, 'replica_unit', None) == 'logical'
                        and getattr(service, 'uses_logical_replicas',
                                    False) is not True):
                    raise ValueError(
                        'Refusing to apply a physical-backend version after '
                        'logical replica semantics were activated.')
                return replica_managers.validate_service_update_preflight(
                    self._service_name,
                    version,
                    service,
                    workspace=self._replica_manager.workspace)

            if prepared_config is None:
                candidate_spot_placer = _preflight()
            else:
                candidate_spot_placer = self._run_with_prepared_config(
                    prepared_config, _preflight)
        except (AssertionError, RuntimeError, TypeError, ValueError) as e:
            raise DeterministicServiceUpdateError(
                f'Version {version} failed deterministic launch preflight: '
                f'{common_utils.format_exception(e)}') from e
        # add_or_update_version commits before this method runs.  Announce the
        # new version without acquiring the replica-manager lock: a large
        # placer-backed scale-up batch may currently hold that lock while
        # enqueueing hundreds of replicas from the superseded version.  The
        # signal lets that batch yield promptly so update_version can acquire
        # the lock and make the durable version live.
        self._replica_manager.notify_version_pending(version)
        config_transition_started = False
        runtime_transition_started = False

        def _install_matching_config() -> None:
            nonlocal config_transition_started
            config_transition_started = True
            assert prepared_config is not None
            self._install_controller_config(prepared_config)

        actuation_epoch_lock = self._get_actuation_epoch_lock()
        actuation_epoch_lock.acquire()
        try:
            runtime_transition_started = True
            if prepared_config is None:
                self._replica_manager.update_version(
                    version,
                    service,
                    update_mode=update_mode,
                    new_spot_placer=(candidate_spot_placer))
            else:
                self._replica_manager.update_version(
                    version,
                    service,
                    update_mode=update_mode,
                    new_spot_placer=(candidate_spot_placer),
                    install_config=_install_matching_config)
            new_autoscaler = autoscalers.Autoscaler.from_spec(
                self._service_name, service)
            accelerator_shapes = self._accelerator_shapes_for_compatibility(
                new_autoscaler, service)
            replace_autoscaler = not isinstance(self._autoscaler,
                                                type(new_autoscaler))
            if replace_autoscaler:
                logger.info('Autoscaler type changed to '
                            f'{type(new_autoscaler)}, updating autoscaler.')
            # Build against the candidate type before mutating the retained live
            # autoscaler.  Publication below then contains no fallible catalog or
            # task parsing between the runtime transition and its version fence.
            new_routing_spec = self._build_routing_spec(service, new_autoscaler)
            reserved_capacity_fill_enabled = bool(
                getattr(service, 'reserved_capacity_fill', False))
            with self._routing_state_lock:
                if replace_autoscaler:
                    old_autoscaler = self._autoscaler
                    # Snapshot, restore, initialize, and publish while LB demand
                    # ingestion is excluded by this routing-epoch lock. The old
                    # autoscaler's own state lock makes its dump internally
                    # coherent; keeping the controller lock through publication
                    # also prevents a later authoritative report from landing on
                    # the old object and being lost from the replacement.
                    new_autoscaler.load_dynamic_states(
                        old_autoscaler.dump_dynamic_states())
                    if isinstance(
                            new_autoscaler,
                        (autoscalers.InstanceAwareRequestRateAutoscaler,
                         autoscalers.ConcurrencyAutoscaler)):
                        new_autoscaler.update_version_and_accelerator_shapes(
                            version, service, update_mode, accelerator_shapes)
                    else:
                        new_autoscaler.update_version(version,
                                                      service,
                                                      update_mode=update_mode)
                    # Seed BEFORE publishing: the replacement cannot take a
                    # decision tick with an empty zero-cost location set.
                    self._seed_fill_zero_cost_locations(new_autoscaler)
                    self._autoscaler = new_autoscaler
                else:
                    if isinstance(
                            self._autoscaler,
                        (autoscalers.InstanceAwareRequestRateAutoscaler,
                         autoscalers.ConcurrencyAutoscaler)):
                        self._autoscaler.update_version_and_accelerator_shapes(
                            version, service, update_mode, accelerator_shapes)
                    else:
                        self._autoscaler.update_version(version,
                                                        service,
                                                        update_mode=update_mode)
                self._reserved_capacity_fill_enabled = (
                    reserved_capacity_fill_enabled)
                self._routing_spec = new_routing_spec
                # This assignment is part of the same critical section as the
                # exact-card catalog and routing spec.  The sync handler and demand
                # ingestion take this lock, so no response/report can observe a
                # mixed epoch.
                self._applied_version = max(self._applied_version, version)
            if not self._mark_controller_applied_version(version):
                raise RuntimeError(
                    f'Could not durably mark service version {version} as '
                    'controller-applied under the current ownership fence.')
        except Exception as e:  # pylint: disable=broad-except
            if config_transition_started or runtime_transition_started:
                # The immutable raw stage may already have been consumed and
                # manager state may have advanced. In-process retry cannot
                # prove a coherent rollback, so delegate to quarantine-aware
                # supervised recovery before another launch is admitted.
                # The parent keeps the same durable owner tuple while it
                # respawns this child, so ownership checks alone cannot close
                # this window.  Fence queued, in-flight, and future manager
                # launches before arranging termination; correctness must not
                # depend on how quickly SIGTERM is delivered.
                self._fence_actuation_for_update_recovery()
                self._schedule_supervised_recovery()
                raise ServiceUpdateRequiresRecoveryError(
                    f'Version {version} failed after its controller runtime '
                    f'transition began: '
                    f'{common_utils.format_exception(e)}') from e
            raise
        finally:
            if not getattr(self, '_update_recovery_required', False):
                self._replica_manager.clear_pending_version(version)
            actuation_epoch_lock.release()
        if reserved_capacity_fill_enabled:
            # An update can enable fill on a live service: give
            # the (retained or replaced) autoscaler the location
            # set so suppression works immediately (no-op when
            # already populated), and make sure the poller
            # exists -- without it fill would sit half-active
            # (flag on, no free-slot feed) until a respawn.
            self._seed_fill_zero_cost_locations(self._autoscaler)
            try:
                self._start_reserved_capacity_poller_if_needed()
            except Exception as e:  # pylint: disable=broad-except
                # The policy/version transition is already complete.  Poller
                # startup is recoverable on the next update or controller
                # restart and must not strand a committed version as pending.
                logger.error('Failed to start the reserved-capacity poller: '
                             f'{common_utils.format_exception(e)}')

    def _start_reserved_capacity_poller_if_needed(self) -> None:
        """Start the reserved-capacity poller (idempotent).

        Called from run() (boot-enabled flag) and from update_service
        (flag enabled on a live service). Getters, not the live objects:
        update_service can replace self._autoscaler and the poller must
        feed the current one. The zero-cost location seeding that
        protects the pre-first-poll window is handled separately by
        _seed_fill_zero_cost_locations (at construction and on update).
        """
        placer = self._replica_manager.spot_placer
        if placer is None:
            # The flag without a spot placer is inert (the placer defines
            # the zero-cost location set fill draws from): say so
            # instead of silently never filling. NOTE: the placer is
            # built once at ReplicaManager construction from the BOOT
            # spec -- an update that INTRODUCES the placer (adds the
            # any_of set / spot_placer field) cannot activate fill until
            # the next controller respawn; this is a pre-existing
            # property of the placer machinery, not of fill.
            logger.warning(
                'reserved_capacity_fill is enabled but the service has no '
                'spot placer (no any_of location set); fill is inactive '
                'until the controller is respawned with a placer-bearing '
                'spec.')
            return
        with self._reserved_capacity_poller_lock:
            if self._reserved_capacity_poller_started:
                return
            self._reserved_capacity_poller_started = True
        thread_utils.start_supervised_thread(
            lambda: reserved_capacity.poller_loop(
                lambda: self._autoscaler,
                lambda: self._replica_manager.spot_placer,
                self._service_name,
                self._service_hash,
                self._controller_owner,
                stop_event=self._get_actuation_stop(),
                actuation_epoch_lock=self._get_actuation_epoch_lock()),
            'reserved-capacity-poller',
            stop_event=self._get_actuation_stop())

    def _run_autoscaler(self):
        logger.info('Starting autoscaler.')
        stop_event = self._get_actuation_stop()
        while not stop_event.is_set():
            # Clear before reading durable replica and placement state. Typed
            # feedback that arrived before this point is consumed by this tick;
            # feedback that arrives during the tick leaves the signal set and
            # makes the interval wait below return immediately.
            actuation_epoch_lock = self._get_actuation_epoch_lock()
            actuation_epoch_lock.acquire()
            try:
                if stop_event.is_set():
                    return
                self._replica_manager.clear_scale_reconciliation_signal()
                replica_infos = serve_state.get_replica_infos(
                    self._service_name)
                self._replica_counts_snapshot = self._get_replica_counts(
                    replica_infos)
                # Use the active versions set by replica manager to make
                # sure we only scale down the outdated replicas that are
                # not used by the load balancer.
                runtime_snapshot = serve_state.get_service_runtime_snapshot(
                    self._service_name, require_version=True)
                assert runtime_snapshot is not None, (
                    'No service record found for '
                    f'{self._service_name}')
                active_versions = runtime_snapshot['active_versions']
                # Keep the exact autoscaler instance/version that produced
                # this tick. A concurrent update may replace or mutate
                # `self._autoscaler` before actuation; the manager's expected
                # version fence must carry the producer's version, not the
                # newly published one.
                decision_autoscaler = self._autoscaler
                decision_version = decision_autoscaler.latest_version
                decision_autoscaler.set_spot_placer(
                    self._replica_manager.spot_placer)
                if isinstance(decision_autoscaler,
                              (autoscalers.InstanceAwareRequestRateAutoscaler,
                               autoscalers.ConcurrencyAutoscaler)):
                    decision_autoscaler.set_free_reserved_slots_by_accelerator(
                        self._get_free_reserved_slots_by_accelerator())

                # Autoscaler now extracts GPU type info directly from
                # replica_infos in generate_scaling_decisions method
                # for better decoupling.
                scaling_options = decision_autoscaler.generate_scaling_decisions(
                    replica_infos, active_versions)
                target_num_replicas = None
                if (decision_autoscaler.has_recomputed_with_fresh_data()
                        is True):
                    demand_target = (
                        decision_autoscaler.get_final_target_num_replicas())
                    fill_target = 0
                    if (getattr(decision_autoscaler, 'reserved_capacity_fill',
                                False) is True):
                        fill_target = getattr(decision_autoscaler,
                                              '_fill_target', 0)
                    if (type(fill_target) is not int or  # pylint: disable=unidiomatic-typecheck
                            fill_target < 0):
                        fill_target = 0
                    target_num_replicas = max(demand_target, fill_target)
                self._replica_manager.publish_target_num_replicas(
                    target_num_replicas, expected_version=decision_version)
                if not self._persist_cost_rebalance_state(decision_autoscaler):
                    logger.warning(
                        'Suppressing new cost-rebalance replacements because '
                        'the controller no longer owns durable stabilization '
                        'state.')
                    scaling_options = [
                        option for option in scaling_options
                        if not (option.operator == autoscalers.
                                AutoscalerDecisionOperator.SCALE_UP and
                                option.reason == autoscalers.
                                AutoscalerDecisionReason.COST_REBALANCE)
                    ]
                rollout_failure = (
                    decision_autoscaler.unrecoverable_rollout_failure)
                if isinstance(rollout_failure,
                              autoscalers.UnrecoverableRolloutFailure):
                    if self._quarantine_unrecoverable_rollout(rollout_failure):
                        # In-process rollback would require reversing manager,
                        # autoscaler, routing, and launch-fence mutations. A
                        # hard child exit delegates that transition to the
                        # existing parent supervisor, whose recovery election
                        # reads the durable quarantine and active-version
                        # fallback before constructing any runtime objects.
                        os._exit(1)  # pylint: disable=protected-access
                    self._replica_manager.wait_for_scale_reconciliation(
                        self._autoscaler.get_decision_interval())
                    continue
                if (isinstance(decision_autoscaler,
                               autoscalers.ConcurrencyAutoscaler) and
                        decision_autoscaler.replica_unit == 'logical'):
                    target_state = decision_autoscaler.logical_target_state
                    if target_state is not None:
                        self._replica_manager.publish_logical_target(
                            *target_state)
                    elif decision_autoscaler.configured_accelerator_shapes:
                        # Exact-card retirement must fail closed while the LB
                        # compatibility report is incomplete. Explicitly
                        # revoke an earlier generation as well as suppressing
                        # this tick's aggregate-only intent.
                        self._replica_manager.invalidate_logical_target()
                # Batch consecutive SCALE_UP decisions into ONE
                # replica-manager call: each scale_up acquires the manager
                # lock, which the readiness-probe round holds for tens of
                # seconds per round on large fleets — per-decision calls
                # trickle through the gaps between rounds (measured live: a
                # 1000-target fleet enqueued only ~100 launches per several
                # minutes while the launch budget sat idle). One lock
                # acquisition per tick's upscale restores the launch budget
                # as the intended pacing mechanism. Decision ORDER is
                # preserved: scale-downs still execute at their original
                # position relative to the upscale batches around them.
                pending_scale_up: list[dict[str, Any] | None] = []
                pending_logical_scale_down: list[
                    autoscalers.LogicalScaleDownTarget] = []

                # The closure is only called within the same outer-loop
                # iteration that (re)binds pending_scale_up, so capturing the
                # loop-scoped list is intentional (B023 false positive).
                def _flush_scale_up(
                    expected_version: int = decision_version,
                    producer_autoscaler: autoscalers.
                    Autoscaler = decision_autoscaler,
                ) -> None:
                    if not pending_scale_up:  # noqa: B023
                        return
                    aggregate_priority = (
                        producer_autoscaler.current_launch_priority())
                    if not isinstance(
                            producer_autoscaler,
                            autoscalers.InstanceAwareRequestRateAutoscaler):
                        self._replica_manager.scale_up_batch(
                            list(pending_scale_up),  # noqa: B023
                            expected_version=expected_version,
                            launch_priority=aggregate_priority)
                        pending_scale_up.clear()  # noqa: B023
                        return

                    # QPS exact-card decisions are ordinary physical
                    # overrides, not one LogicalScaleTarget. Preserve decision
                    # order while splitting consecutive card runs so an
                    # A100-only high-priority request cannot promote L4 claims.
                    card_batches: list[tuple[str | None,
                                             list[dict[str, Any] | None]]] = []
                    for resources_override in pending_scale_up:  # noqa: B023
                        card = None
                        accelerators = (resources_override or
                                        {}).get('accelerators')
                        if isinstance(accelerators,
                                      dict) and len(accelerators) == 1:
                            card = str(next(iter(accelerators)))
                        if not card_batches or card_batches[-1][0] != card:
                            card_batches.append((card, []))
                        card_batches[-1][1].append(resources_override)
                    targeted_cards = [
                        card for card, _ in card_batches if card is not None
                    ]
                    priorities_by_card = (
                        producer_autoscaler.
                        current_launch_priorities_by_accelerator(targeted_cards)
                    )
                    for card, resources_overrides in card_batches:
                        launch_priority = (
                            aggregate_priority
                            if card is None else priorities_by_card.get(
                                card, serve_constants.LB_REQUEST_PRIORITY_MIN))
                        self._replica_manager.scale_up_batch(
                            resources_overrides,
                            expected_version=expected_version,
                            launch_priority=launch_priority)
                    pending_scale_up.clear()  # noqa: B023

                def _flush_logical_scale_down() -> None:
                    if not pending_logical_scale_down:  # noqa: B023
                        return
                    first = pending_logical_scale_down[0]  # noqa: B023
                    exact_target_kwargs: dict[str, Any] = {}
                    if (first.target_capacity_by_accelerator or
                            first.accelerator_shapes):
                        exact_target_kwargs = {
                            'target_capacity_by_accelerator':
                                first.target_capacity_by_accelerator,
                            'accelerator_shapes': first.accelerator_shapes,
                        }
                    self._replica_manager.scale_down_logically_batch(
                        [
                            target.replica_id for target in  # noqa: B023
                            pending_logical_scale_down  # noqa: B023
                        ],  # noqa: B023
                        first.target_capacity,
                        first.version,
                        first.reconcile_generation,
                        **exact_target_kwargs)
                    pending_logical_scale_down.clear()  # noqa: B023

                for scaling_option in scaling_options:
                    logger.info(f'Scaling option received: {scaling_option}')
                    if (scaling_option.operator ==
                            autoscalers.AutoscalerDecisionOperator.SCALE_UP):
                        _flush_logical_scale_down()
                        if isinstance(scaling_option.target,
                                      autoscalers.LogicalScaleTarget):
                            _flush_scale_up()
                            logical_target = scaling_option.target
                            replacement_kwargs: dict[str, Any] = {}
                            if logical_target.replace_unknown_replica_ids:
                                replacement_kwargs[
                                    'replace_unknown_replica_ids'] = (
                                        logical_target.
                                        replace_unknown_replica_ids)
                            if logical_target.launch_budget is not None:
                                replacement_kwargs['launch_budget'] = (
                                    logical_target.launch_budget)
                            replacement_kwargs['launch_priority'] = (
                                logical_target.launch_priority)
                            if (logical_target.launch_priority_by_accelerator):
                                replacement_kwargs[
                                    'launch_priority_by_accelerator'] = dict(
                                        logical_target.
                                        launch_priority_by_accelerator)
                            if (logical_target.
                                    cold_launch_authority_by_accelerator
                                    is not None):
                                replacement_kwargs[
                                    'cold_launch_authority_by_accelerator'] = (
                                        dict(
                                            logical_target.
                                            cold_launch_authority_by_accelerator
                                        ))
                            if logical_target.target_capacity_by_accelerator:
                                replacement_kwargs[
                                    'target_capacity_by_accelerator'] = dict(
                                        logical_target.
                                        target_capacity_by_accelerator)
                                replacement_kwargs['accelerator_shapes'] = dict(
                                    logical_target.accelerator_shapes)
                            self._replica_manager.scale_up_to_logical_capacity(
                                logical_target.target_capacity,
                                logical_target.version,
                                logical_target.reconcile_generation,
                                **replacement_kwargs)
                        else:
                            assert (scaling_option.target is None or isinstance(
                                scaling_option.target, dict)), scaling_option
                            pending_scale_up.append(scaling_option.target)
                    else:
                        _flush_scale_up()
                        if isinstance(scaling_option.target,
                                      autoscalers.LogicalScaleDownTarget):
                            logical_scale_down_target = scaling_option.target
                            if pending_logical_scale_down:
                                first = pending_logical_scale_down[0]
                                if ((logical_scale_down_target.version,
                                     logical_scale_down_target.
                                     reconcile_generation,
                                     logical_scale_down_target.target_capacity,
                                     logical_scale_down_target.
                                     target_capacity_by_accelerator,
                                     logical_scale_down_target.
                                     accelerator_shapes) != (
                                         first.version,
                                         first.reconcile_generation,
                                         first.target_capacity,
                                         first.target_capacity_by_accelerator,
                                         first.accelerator_shapes)):
                                    _flush_logical_scale_down()
                            pending_logical_scale_down.append(
                                logical_scale_down_target)
                        else:
                            _flush_logical_scale_down()
                            assert isinstance(scaling_option.target,
                                              int), scaling_option
                            self._replica_manager.scale_down(
                                scaling_option.target,
                                wait_for_idle=(
                                    scaling_option.reason == autoscalers.
                                    AutoscalerDecisionReason.COST_REBALANCE),
                                expected_version=decision_version)
                _flush_scale_up()
                _flush_logical_scale_down()
            except Exception as e:  # pylint: disable=broad-except
                # No matter what error happens, we should keep the
                # monitor running.
                logger.error('Error in autoscaler: '
                             f'{common_utils.format_exception(e)}')
                with ux_utils.enable_traceback():
                    logger.error(f'  Traceback: {traceback.format_exc()}')
            finally:
                actuation_epoch_lock.release()
            if stop_event.is_set():
                return
            self._replica_manager.wait_for_scale_reconciliation(
                self._autoscaler.get_decision_interval())

    def run(self, controller_socket: socket.socket | None = None) -> None:

        # Every non-pool service uses the external LB topology. Refuse to boot
        # an externally reachable controller with either auth boundary absent;
        # pools have no LB and retain optional localhost/admin auth.
        auth_required = not getattr(self, '_is_pool', True)
        if auth_required:
            serve_utils.get_lb_sync_auth_tokens(required=True)
            serve_utils.get_controller_admin_auth_tokens(required=True)

        admin_auth_dependency = fastapi.Depends(
            _make_auth_dependency(required=auth_required))
        sync_auth_dependency = fastapi.Depends(
            _make_auth_dependency(sync=True, required=auth_required))
        controller_owner_dependency = fastapi.Depends(
            _make_controller_owner_dependency(
                self._controller_owner_fingerprint))

        @self._app.get(
            serve_constants.CONTROLLER_HEALTH_ENDPOINT_PATH,
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        async def get_controller_health() -> fastapi.Response:
            # Keep child liveness independent from fleet size.  In particular,
            # autoscaler.info() serializes every replica and can legitimately
            # exceed the supervisor's one-second read budget at fleet scale.
            return responses.JSONResponse(content={'status': 'ok'},
                                          status_code=200)

        @self._app.get(
            serve_constants.CONTROLLER_UPDATE_CAPABILITIES_ENDPOINT_PATH,
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        async def get_update_capabilities() -> fastapi.Response:
            return responses.JSONResponse(content={
                'config_snapshot_protocol_version':
                    serve_constants.
                    SERVE_UPDATE_CONFIG_SNAPSHOT_PROTOCOL_VERSION,
            },
                                          status_code=200)

        @self._app.get(
            serve_constants.CONTROLLER_PLACEMENT_ENDPOINT_PATH,
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        def get_placement_state() -> fastapi.Response:
            placer = self._replica_manager.spot_placer
            if placer is None:
                content = {
                    'available': True,
                    'enabled': False,
                    'locations': [],
                    'truncated': False,
                }
            else:
                replica_infos = serve_state.get_replica_infos(
                    self._service_name)
                try:
                    budget = paid_capacity.build_launch_budget(
                        placer,
                        workspace=getattr(self._replica_manager, '_workspace',
                                          constants.SKYPILOT_DEFAULT_WORKSPACE),
                        existing_replica_infos=replica_infos,
                        globally_managed=getattr(self, '_service_hash',
                                                 None) is not None,
                        service_name=self._service_name,
                        service_hash=getattr(self, '_service_hash', None))
                    paid_admission = (
                        paid_capacity.admission_snapshot_by_location(budget))
                except Exception as e:  # pylint: disable=broad-except
                    # Admission shown here is explicitly advisory. Keep exact
                    # location and bench diagnostics available during a
                    # database outage; the launch path still requires its
                    # transactional claim and therefore remains fail closed.
                    logger.warning(
                        'Could not build advisory paid-capacity placement '
                        'state: '
                        f'{common_utils.format_exception(e)}')
                    paid_admission = None
                content = placer.placement_snapshot(
                    paid_admission_by_location=paid_admission)
            return responses.JSONResponse(content=content, status_code=200)

        @self._app.get(
            '/autoscaler/info',
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        # Deliberately sync so FastAPI runs fleet-wide status serialization in
        # its worker pool instead of blocking health and control-plane traffic.
        def get_autoscaler_info() -> fastapi.Response:
            info = self._autoscaler.info()
            counts = self._replica_counts_snapshot
            if counts is not None:
                info.update(counts)
            info.update(self._get_update_status())
            try:
                info['drain_proof'] = (
                    self._replica_manager.drain_proof_stats_snapshot())
            except Exception as e:  # pylint: disable=broad-except
                # Diagnostics must never take down the endpoint the
                # supervisor and the dashboard both read.
                logger.warning('Could not snapshot drain-proof counters: '
                               f'{common_utils.format_exception(e)}')
            return responses.JSONResponse(content=info, status_code=200)

        @self._app.post(
            serve_constants.LB_CONTROLLER_SYNC_PATH,
            dependencies=[sync_auth_dependency, controller_owner_dependency])
        async def load_balancer_sync(
                request: fastapi.Request) -> fastapi.Response:
            request_data = await request.json()
            return await self._handle_load_balancer_sync(request_data)

        @self._app.post(
            serve_constants.LB_CONTROLLER_SYSTEM_RECOVERY_LEASE_PATH,
            dependencies=[sync_auth_dependency, controller_owner_dependency])
        async def load_balancer_system_recovery_route_lease(
        ) -> fastapi.Response:
            # Constant-time in-memory projection: endpoint resolution, fleet
            # rows, cloud APIs, and application state are deliberately absent.
            return responses.JSONResponse(content=(
                self._replica_manager.system_recovery_route_lease_snapshot()),
                                          status_code=200)

        @self._app.post(
            serve_constants.LB_CONTROLLER_ROLE_PATH,
            dependencies=[sync_auth_dependency, controller_owner_dependency])
        async def load_balancer_role(
                request: fastapi.Request) -> fastapi.Response:
            request_data = await request.json()
            return await self._handle_load_balancer_role(request_data)

        @self._app.post(
            serve_constants.LB_CONTROLLER_HISTORY_SYNC_PATH,
            dependencies=[sync_auth_dependency, controller_owner_dependency])
        async def load_balancer_request_history_sync(
                request: fastapi.Request) -> fastapi.Response:
            request_data = await request.json()
            return await self._handle_load_balancer_request_history_sync(
                request_data)

        # Deliberately a sync handler: parsing and committing the task YAML can
        # perform blocking file/DB I/O. Runtime application happens on the
        # reconciler, so this lock covers only the short durable commit.
        def _serialize_update(
            handler: Callable[..., fastapi.Response]
        ) -> Callable[..., fastapi.Response]:

            @functools.wraps(handler)
            def _wrapped(*args: Any, **kwargs: Any) -> fastapi.Response:
                with self._update_lock:
                    return handler(*args, **kwargs)

            return _wrapped

        @self._app.post(
            '/controller/update_service',
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        @self._app.post(
            serve_constants.CONTROLLER_CONFIG_UPDATE_ENDPOINT_PATH,
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        @_serialize_update
        def update_service(
            http_request: fastapi.Request,
            request_data: dict[str, Any] = fastapi.Body(...)
        ) -> fastapi.Response:
            version = request_data.get('version', None)
            snapshot_id = request_data.get('config_snapshot_id')
            has_config_snapshot = request_data.get('has_config_snapshot', False)
            is_config_endpoint = (http_request.url.path == serve_constants.
                                  CONTROLLER_CONFIG_UPDATE_ENDPOINT_PATH)
            try:
                if is_config_endpoint != (has_config_snapshot is True):
                    return responses.JSONResponse(content={
                        'message': 'The atomic config-update endpoint and '
                                   'snapshot body must be used together.'
                    },
                                                  status_code=400)
                if (not is_config_endpoint and
                        serve_utils.is_consolidation_mode(self._is_pool)):
                    return responses.JSONResponse(content={
                        'message': 'Legacy updates are disabled for a '
                                   'controller running the atomic config '
                                   'refresh protocol. Quiesce updates until '
                                   'all API pods and controllers are rolled.'
                    },
                                                  status_code=409)
                if version is None:
                    return responses.JSONResponse(
                        content={'message': 'Error: version is not specified.'},
                        status_code=400)
                if type(version) is not int or version <= 0:  # pylint: disable=unidiomatic-typecheck
                    return responses.JSONResponse(content={
                        'message': 'Version must be a positive integer.'
                    },
                                                  status_code=400)
                update_mode_str = request_data.get(
                    'mode', serve_utils.DEFAULT_UPDATE_MODE.value)
                update_mode = serve_utils.UpdateMode(update_mode_str)
                logger.info(f'Update to new version {version} with '
                            f'update_mode {update_mode}.')
                # The yaml with the name latest_task_yaml will be synced
                # See sky/serve/core.py::update
                latest_task_yaml = serve_utils.generate_task_yaml_file_name(
                    self._service_name,
                    version,
                    resource_scope=self._resource_scope)
                with open(latest_task_yaml, encoding='utf-8') as f:
                    yaml_content = f.read()
                submitted_yaml_content = _read_declared_submitted_yaml(
                    request_data, self._service_name, version,
                    self._resource_scope)
                requested_service_hash = request_data.get('service_hash')
                lifecycle_epoch = request_data.get('lifecycle_epoch')
                if (requested_service_hash is not None and
                        requested_service_hash != self._service_hash):
                    return responses.JSONResponse(content={
                        'message': 'Service incarnation changed before '
                                   'the update was committed.'
                    },
                                                  status_code=409)
                prepared_config = None
                if has_config_snapshot:
                    if (not isinstance(lifecycle_epoch, int) or
                            isinstance(lifecycle_epoch, bool)):
                        return responses.JSONResponse(content={
                            'message': 'A config refresh requires a fenced '
                                       'service lifecycle epoch.'
                        },
                                                      status_code=409)
                    expected_digest = request_data.get('config_snapshot_digest')
                    if (not isinstance(expected_digest, str) or re.fullmatch(
                            r'[0-9a-f]{64}', expected_digest) is None or
                            not isinstance(snapshot_id, str) or
                            re.fullmatch(r'[0-9a-f]{64}', snapshot_id) is None):
                        return responses.JSONResponse(content={
                            'message': 'A valid config snapshot digest and '
                                       'ID are required.'
                        },
                                                      status_code=400)
                    prepared_config = self._prepare_controller_config_update(
                        version, expected_digest, snapshot_id)
                service = self._load_service_for_update(version, yaml_content)
                return self._commit_service_update(version, service,
                                                   yaml_content, update_mode,
                                                   requested_service_hash,
                                                   lifecycle_epoch,
                                                   submitted_yaml_content,
                                                   prepared_config)
            except Exception as e:  # pylint: disable=broad-except
                exception_str = common_utils.format_exception(e)
                logger.error(f'Error in update_service: {exception_str}')
                return responses.JSONResponse(content={
                    'message': 'Error',
                    'exception': exception_str,
                    'traceback': traceback.format_exc()
                },
                                              status_code=500)
            finally:
                if (is_config_endpoint and isinstance(version, int) and
                        not isinstance(version, bool) and
                        isinstance(snapshot_id, str) and
                        re.fullmatch(r'[0-9a-f]{64}', snapshot_id) is not None):
                    try:
                        committed = serve_state.get_yaml_content(
                            self._service_name, version) is not None
                    except Exception:  # pylint: disable=broad-except
                        committed = True
                    if not committed:
                        staged_path = (
                            serve_utils.generate_staged_config_yaml_file_name(
                                self._service_name,
                                version,
                                self._resource_scope,
                                snapshot_id=snapshot_id))
                        serve_utils.remove_staged_controller_config(staged_path)

        @self._app.post(
            serve_constants.CONTROLLER_CONFIG_CLEANUP_ENDPOINT_PATH,
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        @_serialize_update
        def cleanup_staged_update_config(request_data: dict[
            str, Any] = fastapi.Body(...)) -> fastapi.Response:
            """Clean an ambiguous raw stage after all update handlers drain."""
            version = request_data.get('version')
            lifecycle_epoch = request_data.get('expected_lifecycle_epoch')
            snapshot_id = request_data.get('config_snapshot_id')
            if (not isinstance(version, int) or isinstance(version, bool) or
                    not isinstance(lifecycle_epoch, int) or
                    isinstance(lifecycle_epoch, bool) or
                    not isinstance(snapshot_id, str) or
                    re.fullmatch(r'[0-9a-f]{64}', snapshot_id) is None):
                return responses.JSONResponse(content={
                    'message': 'Version, lifecycle epoch, and config snapshot '
                               'ID are required.'
                },
                                              status_code=400)
            owner = serve_state.get_service_controller_owner(self._service_name)
            current_owner = ((owner.get('controller_pid'),
                              owner.get('controller_ip'))
                             if owner is not None else None)
            if (owner is None or owner.get('hash') != self._service_hash or
                    owner.get('lifecycle_epoch') != lifecycle_epoch or
                    current_owner != self._controller_owner):
                return responses.JSONResponse(content={
                    'message': 'Service lifecycle ownership changed before '
                               'staged config cleanup.'
                },
                                              status_code=409)
            removed = (serve_utils.remove_uncommitted_staged_controller_config(
                self._service_name, version, self._resource_scope, snapshot_id))
            return responses.JSONResponse(content={'removed': removed},
                                          status_code=200)

        @self._app.post(
            '/controller/set_load_balancer_high_availability',
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        @_serialize_update
        def set_load_balancer_high_availability(request_data: dict[
            str, Any] = fastapi.Body(...)) -> fastapi.Response:
            try:
                enabled = request_data.get('enabled')
                if not isinstance(enabled, bool):
                    return responses.JSONResponse(
                        content={'message': 'enabled must be a boolean.'},
                        status_code=400)
                expected_service_hash = request_data.get('service_hash')
                expected_lifecycle_epoch = request_data.get('lifecycle_epoch')
                if (not isinstance(expected_service_hash, str) or
                        not expected_service_hash or
                        not isinstance(expected_lifecycle_epoch, int) or
                        isinstance(expected_lifecycle_epoch, bool)):
                    return responses.JSONResponse(content={
                        'message': 'Service incarnation and lifecycle '
                                   'fences are required.'
                    },
                                                  status_code=400)
                self._set_load_balancer_high_availability(
                    enabled, expected_service_hash, expected_lifecycle_epoch)
                return responses.JSONResponse(
                    content={
                        'message': 'Load balancer high availability is '
                                   f'{"enabled" if enabled else "disabled"}.'
                    })
            except RuntimeError as e:
                return responses.JSONResponse(content={'message': str(e)},
                                              status_code=409)
            except Exception as e:  # pylint: disable=broad-except
                exception_str = common_utils.format_exception(e)
                logger.error('Error changing load balancer high '
                             f'availability: {exception_str}')
                return responses.JSONResponse(content={
                    'message': 'Error',
                    'exception': exception_str,
                    'traceback': traceback.format_exc()
                },
                                              status_code=500)

        terminate_replica_lock = asyncio.Lock()

        @self._app.post(
            '/controller/terminate_replica',
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        async def terminate_replica(
                request: fastapi.Request) -> fastapi.Response:
            replica_id, purge = await _read_terminate_replica_payload(request)
            # Preserve the route's prior serialized validation semantics while
            # letting health and LB sync proceed during manager-lock waits.
            async with terminate_replica_lock:
                loop = asyncio.get_running_loop()
                terminate = functools.partial(_terminate_replica_sync,
                                              self._service_name,
                                              self._replica_manager, replica_id,
                                              purge)
                operation = loop.run_in_executor(None, terminate)
                try:
                    return await asyncio.shield(operation)
                except asyncio.CancelledError:
                    # Executor work cannot be cancelled after it starts. Keep
                    # duplicate admission serialized until its durable outcome
                    # is known, even if the caller disconnects meanwhile.
                    try:
                        await operation
                    except Exception as e:  # pylint: disable=broad-except
                        logger.warning(
                            'Replica termination failed after its request '
                            f'was cancelled: {common_utils.format_exception(e)}'
                        )
                    raise

        @self._app.exception_handler(Exception)
        async def validation_exception_handler(
                request: fastapi.Request, exc: Exception) -> fastapi.Response:
            with ux_utils.enable_traceback():
                logger.error(f'Error in controller: {exc!r}')
            return responses.JSONResponse(
                status_code=500,
                content={
                    'message':
                        (f'Failed method {request.method} at URL {request.url}.'
                         f' Exception message is {exc!r}.')
                },
            )

        # A committed update is the API success boundary. Apply it on a
        # controller-owned worker so fleet-wide replica locks cannot make the
        # request time out or prevent a later update from committing.
        thread_utils.start_supervised_thread(
            self._run_update_reconciler,
            'service-update-reconciler',
            stop_event=self._get_update_reconciler_stop())

        # Raw policy-admitted snapshots can contain credentials. A pod-local
        # controller survives API-request crashes, so it owns periodic cleanup
        # of expired NULL-yaml stages in addition to endpoint-local finally
        # cleanup. The first sweep runs immediately on every child start.
        thread_utils.start_supervised_thread(
            self._run_orphaned_config_stage_sweeper,
            'controller-config-stage-sweeper',
            stop_event=self._get_update_reconciler_stop())

        # Supervised so a BaseException escaping the autoscaler loop (or the
        # loop returning) does not silently stop all scaling decisions while
        # the controller keeps serving HTTP -- it is restarted instead.
        thread_utils.start_supervised_thread(
            self._run_autoscaler,
            'autoscaler',
            stop_event=self._get_actuation_stop())

        if self._reserved_capacity_fill_enabled:
            self._start_reserved_capacity_poller_if_needed()

        logger.info('SkyServe Controller started on '
                    f'http://{self._host}:{self._port}. PID: {os.getpid()}')

        try:
            if controller_socket is None:
                uvicorn.run(self._app, host=self._host, port=self._port)
            else:
                config = uvicorn.Config(self._app,
                                        host=self._host,
                                        port=self._port)
                uvicorn.Server(config).run(sockets=[controller_socket])
        except BaseException:  # pylint: disable=broad-except
            # The finally below hard-exits, which would otherwise swallow the
            # propagating exception -- log it so a crash-looping controller
            # leaves a post-mortem trace.
            logger.error('SkyServe Controller uvicorn server raised:\n'
                         f'{traceback.format_exc()}')
            raise
        finally:
            # If uvicorn.run() ever returns (a clean shutdown, a child-only
            # SIGINT raising KeyboardInterrupt, or any other exit), the HTTP
            # control plane is dead but the supervised control-loop threads
            # (autoscaler, replica refresher/prober/status-fetcher) are
            # non-daemon and loop forever, so the interpreter cannot exit and
            # the process lingers. The parent `_start` watchdog respawns the
            # controller only when `controller_process.is_alive()` is False,
            # so a lingering process is never respawned and the service is
            # stuck with no working controller. Hard-exit so the parent
            # observes the death and respawns on a fresh port.
            logger.error('SkyServe Controller uvicorn server exited; '
                         'terminating the subprocess so the parent can '
                         'respawn the controller.')
            os._exit(1)  # pylint: disable=protected-access


# TODO(tian): Probably we should support service that will stop the VM in
# specific time period.
def run_controller(service_name: str,
                   service_spec: serve.SkyServiceSpec,
                   version: int,
                   controller_host: str,
                   controller_port: int,
                   controller_owner_fingerprint: str,
                   resource_scope: str | None = None,
                   service_hash: str | None = None,
                   controller_pid: int | None = None,
                   controller_ip: str | None = None,
                   enforce_launch_fence: bool = False,
                   controller_socket: socket.socket | None = None):
    db_utils.set_postgres_connection_metrics_process_role('serve-controller')
    os.environ[constants.OVERRIDE_CONSOLIDATION_MODE] = 'true'
    # Hijack sys.stdout/stderr to be context aware.
    context_utils.hijack_sys_attrs()
    controller = SkyServeController(service_name, service_spec, version,
                                    controller_host, controller_port,
                                    controller_owner_fingerprint,
                                    resource_scope, service_hash,
                                    controller_pid, controller_ip,
                                    enforce_launch_fence)
    if controller_socket is None:
        controller.run()
    else:
        controller.run(controller_socket)
