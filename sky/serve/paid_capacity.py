"""Global admission control for fresh paid SkyServe capacity.

Autoscalers decide how much capacity a service needs. Spot placers decide
which provider location is cheapest and currently usable. This module owns the
cross-service limit on unresolved, genuine demand launches into one exact paid
provider pool.
"""
import collections
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
import dataclasses
import enum
import functools
import hashlib
import json
import math
import os
import re
import threading
import time
import typing
from typing import Any
import uuid

from sky import clouds
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.serve import constants
from sky.serve import spot_placer
from sky.utils import common_utils

if typing.TYPE_CHECKING:
    from sky.serve import capacity_admission
    from sky.serve import replica_managers

logger = sky_logging.init_logger(__name__)
serve_state = adaptors_common.LazyImport('sky.serve.serve_state')
replica_info_lib = adaptors_common.LazyImport('sky.serve.replica_info')

_BASE_LIMIT_DEFAULT = 4
_LEGACY_LOCAL_LIMIT_DEFAULT = 4
_MAX_LIMIT_DEFAULT = 480
_EXPLORATION_FRONTIER_DEFAULT = 2
_MAX_EXPLORATION_FRONTIER_DEFAULT = 3
_EXPLORATION_FEEDBACK_DELAY_SECONDS_DEFAULT = 30
_BASE_LIMIT_ENV_VAR = 'SKYPILOT_SERVE_PAID_LOCATION_LAUNCH_WINDOW'
_MAX_LIMIT_ENV_VAR = 'SKYPILOT_SERVE_PAID_LOCATION_MAX_LAUNCH_WINDOW'
_SERVICE_LIMIT_DEFAULT = 16
_SERVICE_LIMIT_ENV_VAR = 'SKYPILOT_SERVE_PAID_SERVICE_LAUNCH_WINDOW'
_SERVICE_MAX_LIMIT_ENV_VAR = ('SKYPILOT_SERVE_PAID_SERVICE_MAX_LAUNCH_WINDOW')
_SERVICE_LIMIT_PROFILES_ENV_VAR = (
    'SKYPILOT_SERVE_PAID_SERVICE_LAUNCH_WINDOW_PROFILES')
_SERVICE_LIMIT_PROFILES_VERSION = 1
_EXPLORATION_FRONTIER_ENV_VAR = (
    'SKYPILOT_SERVE_PAID_LOCATION_EXPLORATION_FRONTIER')
_MAX_EXPLORATION_FRONTIER_ENV_VAR = (
    'SKYPILOT_SERVE_PAID_LOCATION_MAX_EXPLORATION_FRONTIER')
_EXPLORATION_FEEDBACK_DELAY_SECONDS_ENV_VAR = (
    'SKYPILOT_SERVE_PAID_LOCATION_EXPLORATION_FEEDBACK_DELAY_SECONDS')
_SUCCESS_TTL_SECONDS_DEFAULT = 10 * 60
_SUCCESS_TTL_SECONDS_ENV_VAR = (
    'SKYPILOT_SERVE_PAID_LOCATION_SUCCESS_TTL_SECONDS')
_WAITER_TTL_SECONDS_DEFAULT = 45
_WAITER_TTL_SECONDS_ENV_VAR = (
    'SKYPILOT_SERVE_PAID_LOCATION_WAITER_TTL_SECONDS')
_FAILURE_COOLDOWN_SECONDS_DEFAULT = 10 * 60
_FAILURE_COOLDOWN_SECONDS_ENV_VAR = (
    'SKYPILOT_SERVE_PAID_LOCATION_FAILURE_COOLDOWN_SECONDS')
_ADMISSION_SUMMARY_LOG_MIN_INTERVAL_SECONDS = 30
_ADMISSION_SUMMARY_LOG_INTERVAL_SECONDS = 5 * 60
_LEGACY_POOL_KEY_VERSION = 1
_POOL_KEY_VERSION = 2
_AWS_ACCOUNT_ID_RE = re.compile(r'[0-9]{12}')
_SHA256_RE = re.compile(r'[0-9a-f]{64}')
_MAX_EXACT_SHAPE_INTEGER = (1 << 63) - 1
MAX_PREPARED_LAUNCH_SPECS = 512
_UNRESOLVED_STATUS_VALUES = frozenset({'PENDING', 'PROVISIONING'})
_admission_summary_log_lock = threading.Lock()
_admission_summary_log_signature: tuple[Any, ...] | None = None
_admission_summary_logged_at = 0.0
_UNSET_POOL_KEY = object()
FrontierKey = tuple[str, ...]


class ClaimResult(enum.Enum):
    """Result of atomically persisting one paid-capacity claim."""

    ACQUIRED = 'acquired'
    SATURATED = 'saturated'
    SERVICE_SATURATED = 'service_saturated'
    FEEDBACK_PENDING = 'feedback_pending'
    HIGHER_PRIORITY_WAITING = 'higher_priority_waiting'
    OWNERSHIP_LOST = 'ownership_lost'
    LEGACY_LOCAL = 'legacy_local'


class PaidGPUAttributionError(ValueError):
    """A paid replica has no exact, self-consistent physical GPU width."""


@dataclasses.dataclass(frozen=True, kw_only=True)
class PhysicalBackendShape:
    """Exact provider shape shared by pool identity and planner authority."""

    accelerator: str | None
    gpu_units_per_node: int
    num_nodes: int

    def __post_init__(self) -> None:
        cpu = self.accelerator is None
        if ((not cpu and
             (not isinstance(self.accelerator, str) or not self.accelerator or
              self.accelerator != self.accelerator.casefold())) or
                type(self.gpu_units_per_node) is not int or  # pylint: disable=unidiomatic-typecheck
                self.gpu_units_per_node < 0
                or self.gpu_units_per_node > _MAX_EXACT_SHAPE_INTEGER or
            (cpu != (self.gpu_units_per_node == 0))
                or type(self.num_nodes) is not int or  # pylint: disable=unidiomatic-typecheck
                not 1 <= self.num_nodes <= _MAX_EXACT_SHAPE_INTEGER):
            raise PaidGPUAttributionError(
                'Physical backend shape is malformed.')
        if (self.gpu_units_per_node > 0 and self.gpu_units_per_node
                > _MAX_EXACT_SHAPE_INTEGER // self.num_nodes):
            raise PaidGPUAttributionError(
                'Physical backend shape exceeds exact accounting range.')

    @property
    def total_gpu_units(self) -> int:
        return self.gpu_units_per_node * self.num_nodes


def freeze_paid_launch_payload(value: Mapping[str, Any]) -> bytes:
    """Freeze one JSON object used by a provider-free launch specification."""
    if not isinstance(value, Mapping):
        raise TypeError('Paid launch payload must be a mapping.')
    try:
        return json.dumps(dict(value),
                          sort_keys=True,
                          separators=(',', ':'),
                          allow_nan=False).encode('utf-8')
    except (TypeError, ValueError) as error:
        raise ValueError(
            'Paid launch payload must be canonical JSON.') from error


def thaw_paid_launch_payload(value: bytes) -> dict[str, Any]:
    """Decode and verify one canonical provider-free launch payload."""
    if type(value) is not bytes or not value:  # pylint: disable=unidiomatic-typecheck
        raise ValueError('Paid launch payload must be nonempty bytes.')
    try:
        decoded = json.loads(value.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError('Paid launch payload is not valid JSON.') from error
    if not isinstance(decoded, dict):
        raise ValueError('Paid launch payload must encode one object.')
    if freeze_paid_launch_payload(decoded) != value:
        raise ValueError('Paid launch payload is not canonical JSON.')
    return decoded


def paid_launch_payload_sha256(value: Mapping[str, Any]) -> str:
    """Hash one canonical JSON authority payload."""
    return hashlib.sha256(freeze_paid_launch_payload(value)).hexdigest()


def decode_paid_launch_resources_override(value: bytes) -> dict[str, Any]:
    """Decode one canonical immutable resources override."""
    decoded = spot_placer.decode_resources_override(
        thaw_paid_launch_payload(value))
    if not isinstance(decoded, dict):
        raise ValueError('Paid launch resources override is malformed.')
    return decoded


@dataclasses.dataclass(frozen=True, kw_only=True)
class PaidLaunchVersionAuthority:
    """Immutable controller authority read from one elected version row."""

    service_spec: bytes
    service_spec_sha256: str
    controller_config: bytes
    controller_config_digest: str
    controller_config_snapshot_id: str

    def __post_init__(self) -> None:
        if (type(self.service_spec) is not bytes or not self.service_spec or
                type(self.service_spec_sha256) is not str or
                _SHA256_RE.fullmatch(self.service_spec_sha256) is None or
                hashlib.sha256(self.service_spec).hexdigest()
                != self.service_spec_sha256 or
                type(self.controller_config) is not bytes or
                type(self.controller_config_digest) is not str or
                _SHA256_RE.fullmatch(self.controller_config_digest) is None or
                hashlib.sha256(self.controller_config).hexdigest()
                != self.controller_config_digest or
                type(self.controller_config_snapshot_id) is not str or
                _SHA256_RE.fullmatch(
                    self.controller_config_snapshot_id) is None):
            raise ValueError('Paid launch version authority is malformed.')


@dataclasses.dataclass(frozen=True, kw_only=True)
class PaidLaunchCatalogEvidence:
    """Immutable elected-version and catalog facts for one Spot template."""

    placement_catalog_sha256: str
    catalog_rank: int
    exploration_round: int
    slot_within_pool_window: int
    version_authority: PaidLaunchVersionAuthority

    def __post_init__(self) -> None:
        if (type(self.placement_catalog_sha256) is not str or
                _SHA256_RE.fullmatch(self.placement_catalog_sha256) is None):
            raise ValueError('Paid launch version evidence has a bad digest.')
        if not isinstance(self.version_authority, PaidLaunchVersionAuthority):
            raise ValueError('Paid launch has no elected-version authority.')
        integer_fields = (self.catalog_rank, self.exploration_round,
                          self.slot_within_pool_window)
        if any(
                type(value) is not int or value < 0  # pylint: disable=unidiomatic-typecheck
                for value in integer_fields):
            raise ValueError('Paid launch catalog ordering is malformed.')


@dataclasses.dataclass(frozen=True, kw_only=True)
class PaidLaunchSpec:
    """Deeply immutable provider-free candidate for atomic paid admission.

    Every structured value is canonical JSON bytes or an immutable tuple.  In
    particular, this boundary deliberately cannot retain ``ReplicaInfo``,
    ``Location``, a callback, a launch worker, or mutable controller state.
    Plan and demand authority are also absent: PostgreSQL derives those from
    its final locked snapshot when it accepts a sparse subset.
    """

    ordinal: int
    service_name: str
    service_hash: str
    service_lifecycle_epoch: int
    service_version: int
    replica_id: int
    replica_record_id: str
    cluster_name_seed: str
    worker_construction: bytes
    provider_account: str | None
    cloud: str
    workspace: str
    region: str
    zone: str | None
    instance_type: str | None
    pool_key: str
    frontier_key: FrontierKey
    accelerator: str
    gpu_units_per_node: int
    num_nodes: int
    resources_override: bytes
    catalog_evidence: PaidLaunchCatalogEvidence

    def __post_init__(self) -> None:
        nonempty_strings = (
            self.service_name,
            self.service_hash,
            self.cluster_name_seed,
            self.cloud,
            self.workspace,
            self.region,
            self.pool_key,
            self.accelerator,
        )
        if any(
                type(value) is not str or not value
                for value in nonempty_strings):
            raise ValueError('Paid launch identities must be nonempty strings.')
        if any(
                type(value) is not int or value < minimum  # pylint: disable=unidiomatic-typecheck
                for value, minimum in ((self.ordinal,
                                        0), (self.service_lifecycle_epoch,
                                             1), (self.service_version,
                                                  1), (self.replica_id, 1),
                                       (self.gpu_units_per_node,
                                        1), (self.num_nodes, 1))):
            raise ValueError('Paid launch integer identities are malformed.')
        if self.gpu_units_per_node > (_MAX_EXACT_SHAPE_INTEGER //
                                      self.num_nodes):
            raise ValueError('Paid launch physical shape is too large.')
        try:
            record_id = str(uuid.UUID(self.replica_record_id))
        except (AttributeError, TypeError, ValueError) as error:
            raise ValueError(
                'Paid launch record identity must be a UUID string.') from error
        if record_id != self.replica_record_id:
            raise ValueError('Paid launch record UUID must be canonical.')
        if (self.zone is not None and
            (type(self.zone) is not str or not self.zone)):
            raise ValueError('Paid launch zone must be nonempty or absent.')
        if (self.provider_account is not None and
            (type(self.provider_account) is not str or
             not self.provider_account)):
            raise ValueError('Paid launch provider account is malformed.')
        if (self.cloud != self.cloud.casefold() or
                self.accelerator != self.accelerator.casefold()):
            raise ValueError('Paid launch cloud/card names must be folded.')
        if (type(self.frontier_key) is not tuple or any(
                type(card) is not str or not card or card != card.casefold()
                for card in self.frontier_key) or
                self.frontier_key != tuple(sorted(set(self.frontier_key)))):
            raise ValueError('Paid launch frontier identity is noncanonical.')
        for payload in (self.worker_construction, self.resources_override):
            thaw_paid_launch_payload(payload)
        if not isinstance(self.catalog_evidence, PaidLaunchCatalogEvidence):
            raise ValueError('Paid launch has no typed version evidence.')

        pool = pool_key_payload(self.pool_key)
        if pool is None or pool.get('use_spot') is not True:
            raise ValueError('Paid launch pool must be canonical Spot.')
        accelerators = pool.get('accelerators')
        if accelerators != [[self.accelerator, self.gpu_units_per_node]]:
            raise ValueError('Paid launch pool and accelerator shape disagree.')
        if (pool.get('cloud') != self.cloud or
                pool.get('workspace') != self.workspace or
                pool.get('region') != self.region or
                pool.get('zone') != self.zone or
                pool.get('instance_type') != self.instance_type or
                pool.get('num_nodes') != self.num_nodes):
            raise ValueError('Paid launch pool and location disagree.')
        provider_identity = pool.get('provider_identity')
        pool_account = (provider_identity.get('aws_account_id') if isinstance(
            provider_identity, dict) else None)
        if pool_account != self.provider_account:
            raise ValueError('Paid launch pool and provider account disagree.')

    @property
    def physical_gpu_units(self) -> int:
        return self.gpu_units_per_node * self.num_nodes

    def persistence_spec(
        self,
        *,
        priority: int,
        frontier_limit: int,
        replica_port: str,
        planned_capacity: int,
        created_at: float | None,
    ) -> 'PaidClaimPersistenceSpec':
        """Materialize the legacy SQL adapter without provider resolution.

        The immutable specification remains the cross-lock boundary.  This
        short-lived mutable form exists only inside the repository adapter,
        where the final locked planner derives and installs claim authority.
        """
        if (type(priority) is not int or type(frontier_limit) is not int or  # pylint: disable=unidiomatic-typecheck
                frontier_limit < 1):
            raise ValueError('Paid persistence policy inputs are malformed.')
        state = build_pristine_paid_replica_state(
            self,
            replica_port=replica_port,
            planned_capacity=planned_capacity,
            created_at=created_at)
        info = replica_info_lib.ReplicaInfo.from_storage_dict(state)
        location = info.get_spot_location()
        if (location is None or location.to_pickleable() != info.location or
                info.replica_id != self.replica_id or
                info.replica_record_id != self.replica_record_id or
                info.version != self.service_version or
                info.cluster_name != self.cluster_name_seed or
                info.paid_capacity_pool_key != self.pool_key or
                info.is_spot is not True or info.is_zero_cost is not False or
                info.reserved_fill is not False):
            raise ValueError('Paid launch initial replica state is malformed.')
        return PaidClaimPersistenceSpec(candidate=PaidClaimCandidate(
            replica_id=self.replica_id,
            replica_info=info,
            location=location,
            priority=priority,
            capacity_plan_claim=None),
                                        pool_key=self.pool_key,
                                        frontier_key=self.frontier_key,
                                        frontier_limit=frontier_limit)


def build_pristine_paid_replica_state(
    spec: PaidLaunchSpec,
    *,
    replica_port: str,
    planned_capacity: int,
    created_at: float | None,
) -> dict[str, Any]:
    """Construct the complete initial paid row from its sole typed seed."""
    if not isinstance(spec, PaidLaunchSpec):
        raise TypeError('Paid launch spec is malformed.')
    if type(replica_port) is not str or not replica_port:  # pylint: disable=unidiomatic-typecheck
        raise ValueError('Paid launch replica port is malformed.')
    if type(planned_capacity) is not int or planned_capacity < 1:  # pylint: disable=unidiomatic-typecheck
        raise ValueError('Paid launch planned capacity is malformed.')
    if created_at is not None and (  # pylint: disable=unidiomatic-typecheck
            type(created_at) is not float or not math.isfinite(created_at) or
            created_at <= 0):
        raise ValueError('Paid launch creation time is malformed.')
    resources_override = decode_paid_launch_resources_override(
        spec.resources_override)
    location = spot_placer.Location.from_resources_override(resources_override)
    if location is None:
        raise ValueError('Paid launch has no exact location.')
    info = replica_info_lib.ReplicaInfo(replica_id=spec.replica_id,
                                        cluster_name=spec.cluster_name_seed,
                                        replica_port=replica_port,
                                        is_spot=True,
                                        location=location,
                                        version=spec.service_version,
                                        resources_override=resources_override,
                                        planned_capacity=planned_capacity)
    # The prepared seed has no clock authority. PostgreSQL supplies the exact
    # accepted timestamp after it locks paid capacity and before row insertion.
    info.created_at = created_at
    info.replica_record_id = spec.replica_record_id
    info.is_zero_cost = False
    info.paid_capacity_pool_key = spec.pool_key
    return typing.cast(dict[str, Any], info.to_storage_dict())


@dataclasses.dataclass(frozen=True, kw_only=True)
class PaidLaunchReceiptMember:
    """One exact accepted replica/claim identity read from PostgreSQL."""

    replica_id: int
    replica_record_id: str
    pool_key: str
    priority: int
    accelerator: str
    plan_units: int
    physical_gpu_units: int

    def __post_init__(self) -> None:
        if (type(self.replica_id) is not int or self.replica_id < 1 or  # pylint: disable=unidiomatic-typecheck
                type(self.replica_record_id) is not str
                or type(self.pool_key) is not str
                or pool_key_payload(self.pool_key) is None
                or type(self.priority) is not int  # pylint: disable=unidiomatic-typecheck
                or not constants.LB_REQUEST_PRIORITY_MIN <= self.priority <=
                constants.LB_REQUEST_PRIORITY_MAX
                or type(self.accelerator) is not str or not self.accelerator
                or self.accelerator != self.accelerator.casefold()
                or type(self.plan_units) is not int or self.plan_units < 1 or  # pylint: disable=unidiomatic-typecheck
                type(self.physical_gpu_units) is not int or  # pylint: disable=unidiomatic-typecheck
                self.physical_gpu_units < 1):
            raise ValueError('Paid launch receipt member is malformed.')
        try:
            record_id = str(uuid.UUID(self.replica_record_id))
        except (TypeError, ValueError) as error:
            raise ValueError('Paid launch receipt record ID is not a UUID.') \
                from error
        if record_id != self.replica_record_id:
            raise ValueError('Paid launch receipt record UUID is noncanonical.')


@dataclasses.dataclass(frozen=True, kw_only=True)
class PaidLaunchReceipt:
    """Sparse committed subset returned by atomic plan/claim admission."""

    service_name: str
    service_hash: str
    service_lifecycle_epoch: int
    service_version: int
    capacity_plan_generation: int
    capacity_plan_sha256: str
    capacity_unit: str
    members: tuple[PaidLaunchReceiptMember, ...]

    def __post_init__(self) -> None:
        if (type(self.service_name) is not str or not self.service_name or
                type(self.service_hash) is not str or not self.service_hash or
                type(self.service_lifecycle_epoch) is not int or  # pylint: disable=unidiomatic-typecheck
                self.service_lifecycle_epoch < 1
                or type(self.service_version) is not int or  # pylint: disable=unidiomatic-typecheck
                self.service_version < 1 or
                type(self.capacity_plan_generation) is not int or  # pylint: disable=unidiomatic-typecheck
                self.capacity_plan_generation < 1
                or type(self.capacity_plan_sha256) is not str
                or _SHA256_RE.fullmatch(self.capacity_plan_sha256) is None
                or self.capacity_unit not in ('logical-gpu', 'physical-backend')
                or type(self.members) is not tuple
                or any(not isinstance(member, PaidLaunchReceiptMember)
                       for member in self.members)):
            raise ValueError('Paid launch receipt is malformed.')
        identities = tuple((member.replica_id, member.replica_record_id)
                           for member in self.members)
        if len(identities) != len(set(identities)):
            raise ValueError('Paid launch receipt members must be unique.')


@dataclasses.dataclass(frozen=True)
class PaidClaimCandidate:
    """One frozen exact-location member of a paid admission wave."""

    replica_id: int
    replica_info: 'replica_managers.ReplicaInfo'
    location: spot_placer.Location
    priority: int
    capacity_plan_claim: Mapping[str, Any] | None = None


@dataclasses.dataclass(frozen=True)
class PaidClaimBatchMemberResult:
    """Authoritative Phase-A result for one exact replica record."""

    replica_id: int
    replica_record_id: str
    claim_result: ClaimResult


@dataclasses.dataclass(frozen=True)
class PaidClaimBatchResult:
    """Ordered results from one atomic paid replica/claim transaction."""

    members: tuple[PaidClaimBatchMemberResult, ...]

    @property
    def committed_members(self) -> tuple[PaidClaimBatchMemberResult, ...]:
        return tuple(member for member in self.members
                     if member.claim_result is ClaimResult.ACQUIRED)


@dataclasses.dataclass(frozen=True)
class PaidClaimPersistenceSpec:
    """Exact database-facing policy inputs for one frozen candidate."""

    candidate: PaidClaimCandidate
    pool_key: str
    frontier_key: FrontierKey
    frontier_limit: int


class LaunchOutcome(enum.Enum):
    """Capacity evidence from one completed paid launch."""

    SUCCESS = 'success'
    CAPACITY_FAILURE = 'capacity_failure'
    QUOTA_FAILURE = 'quota_failure'
    OTHER_FAILURE = 'other_failure'


@dataclasses.dataclass(frozen=True)
class CompletedLaunchPersistence:
    """Committed result of one completed launch wave."""

    ownership_valid: bool
    applied_pool_keys: frozenset[str] = frozenset()


@dataclasses.dataclass
class LaunchBudget:
    """One wave's advisory headroom and exact pool identity."""

    remaining_by_location: dict[spot_placer.Location, int]
    pool_key_by_location: dict[spot_placer.Location, str]
    states_by_pool_key: dict[str, dict[str, Any]]
    globally_managed: bool
    priority_deferred_pool_keys: set[str] = dataclasses.field(
        default_factory=set)
    service_remaining: int | None = None
    service_claim_limit: int | None = None
    frontier_limit: int | None = None
    frontier_key_by_location: dict[spot_placer.Location,
                                   FrontierKey] = (dataclasses.field(
                                       default_factory=dict))
    owned_pool_keys_by_frontier: dict[FrontierKey,
                                      set[str]] = (dataclasses.field(
                                          default_factory=dict))
    unknown_owned_pool_keys: set[str] = dataclasses.field(default_factory=set)
    oldest_claimed_at_by_frontier: dict[FrontierKey,
                                        float] = (dataclasses.field(
                                            default_factory=dict))
    oldest_unknown_claimed_at: float | None = None
    feedback_deferred_frontiers: set[FrontierKey] = dataclasses.field(
        default_factory=set)
    stop_sequence: int = 0
    max_frontier_limit: int | None = None
    frontier_feedback_delay_seconds: int | None = None
    newest_claimed_at_by_pool_key: dict[str, float] = dataclasses.field(
        default_factory=dict)
    unknown_claim_age_pool_keys: set[str] = dataclasses.field(
        default_factory=set)
    frontier_limit_overrides: dict[FrontierKey, int] = dataclasses.field(
        default_factory=dict)
    max_live_paid_gpu_units: int | None = None
    live_paid_gpu_units: int | None = None
    paid_gpu_units_remaining: int | None = None
    plan_bound_cohort: 'PlanBoundAdmissionCohort | None' = None


@dataclasses.dataclass(frozen=True)
class RampUpdate:
    """Pure adaptive-limit transition produced from provider feedback."""

    current_limit: int
    successes_since_resize: int
    expired: bool
    failed: bool


@dataclasses.dataclass(frozen=True)
class AdmissionLimit:
    """Effective admission bound for one exact paid-capacity pool."""

    limit: int
    state: str
    cooldown_until: float | None


@dataclasses.dataclass(frozen=True)
class ServiceLimitProfile:
    """One exact service-incarnation adaptive-window override."""

    workspace: str
    service_name: str
    service_hash: str
    max_launch_window: int
    max_exploration_frontier: int | None = None


@dataclasses.dataclass(frozen=True)
class PlanBoundAdmissionTarget:
    """One exact-card projection of uncommitted capacity-plan authority."""

    frontier_key: FrontierKey
    remaining_plan_units: int
    physical_backend_width: int
    claim_units_per_backend: int
    backend_claim_count: int
    frontier_limit: int

    def __post_init__(self) -> None:
        if (not isinstance(self.frontier_key, tuple) or not self.frontier_key or
                not all(
                    isinstance(card, str) and card
                    for card in self.frontier_key) or
                type(self.remaining_plan_units) is not int or
                self.remaining_plan_units <= 0 or
                type(self.physical_backend_width) is not int or
                self.physical_backend_width <= 0 or
                type(self.claim_units_per_backend) is not int or
                self.claim_units_per_backend <= 0 or
                type(self.backend_claim_count) is not int or
                self.backend_claim_count <= 0 or
                type(self.frontier_limit) is not int or
                self.frontier_limit <= 0 or self.backend_claim_count *
                self.claim_units_per_backend != self.remaining_plan_units):
            raise ValueError('Plan-bound paid target is malformed.')


@dataclasses.dataclass(frozen=True)
class PlanBoundAdmissionCohort:
    """Deterministic Phase-A cohort derived from one immutable paid plan.

    The capacity plan remains the sole aggregate purchase authority. Exact
    pool limits are independent failure-containment bounds, so the cohort
    opens only the minimum cost-ordered pool frontier needed to hold the
    plan-authorized whole backends under those existing bounds.
    """

    capacity_plan_generation: int
    capacity_plan_sha256: str
    targets: tuple[PlanBoundAdmissionTarget, ...]

    def __post_init__(self) -> None:
        if (type(self.capacity_plan_generation) is not int or
                self.capacity_plan_generation <= 0 or
                not isinstance(self.capacity_plan_sha256, str) or
                _SHA256_RE.fullmatch(self.capacity_plan_sha256) is None or
                any(not isinstance(target, PlanBoundAdmissionTarget)
                    for target in self.targets)):
            raise ValueError('Plan-bound paid cohort identity is malformed.')
        keys = tuple(target.frontier_key for target in self.targets)
        if len(set(keys)) != len(keys) or keys != tuple(sorted(keys)):
            raise ValueError(
                'Plan-bound paid cohort targets are not canonical.')

    @property
    def backend_claim_count(self) -> int:
        return sum(target.backend_claim_count for target in self.targets)

    def frontier_limits(self) -> dict[FrontierKey, int]:
        return {
            target.frontier_key: target.frontier_limit
            for target in self.targets
        }


@functools.cache
def _parse_positive_int(raw_value: str | None, default: int,
                        variable: str) -> int:
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        value = 0
    if value <= 0:
        logger.warning(
            f'Invalid {variable} value {raw_value!r}; using {default}.')
        return default
    return value


def base_limit() -> int:
    """Return the first unresolved paid-capacity cohort size."""
    return _parse_positive_int(os.environ.get(_BASE_LIMIT_ENV_VAR),
                               _BASE_LIMIT_DEFAULT, _BASE_LIMIT_ENV_VAR)


def legacy_local_limit() -> int:
    """Return the pre-global per-service window for local SQLite."""
    return _parse_positive_int(os.environ.get(_BASE_LIMIT_ENV_VAR),
                               _LEGACY_LOCAL_LIMIT_DEFAULT, _BASE_LIMIT_ENV_VAR)


def max_limit() -> int:
    """Return the largest adaptive unresolved paid-capacity cohort."""
    configured = _parse_positive_int(os.environ.get(_MAX_LIMIT_ENV_VAR),
                                     _MAX_LIMIT_DEFAULT, _MAX_LIMIT_ENV_VAR)
    return max(base_limit(), configured)


def service_limit() -> int:
    """Return one service's cross-pool unresolved paid-claim envelope."""
    return _parse_positive_int(os.environ.get(_SERVICE_LIMIT_ENV_VAR),
                               _SERVICE_LIMIT_DEFAULT, _SERVICE_LIMIT_ENV_VAR)


@functools.cache
def _parse_service_limit_profiles(
        raw_value: str | None) -> tuple[ServiceLimitProfile, ...]:
    """Parse exact-incarnation adaptive-window profiles, failing closed."""
    if not raw_value:
        return ()
    try:
        document = json.loads(raw_value)
        if (not isinstance(document, dict) or
                set(document) != {'version', 'profiles'} or
                type(document['version']) is not int or  # pylint: disable=unidiomatic-typecheck
                document['version'] != _SERVICE_LIMIT_PROFILES_VERSION
                or not isinstance(document['profiles'], list)):
            raise ValueError('document has invalid fields or version')
        profiles = []
        identities = set()
        for value in document['profiles']:
            required_fields = {
                'workspace', 'service_name', 'service_hash', 'max_launch_window'
            }
            allowed_fields = required_fields | {'max_exploration_frontier'}
            if (not isinstance(value, dict) or
                    not required_fields.issubset(value) or
                    not set(value).issubset(allowed_fields)):
                raise ValueError('profile has invalid fields')
            workspace = value['workspace']
            service_name = value['service_name']
            service_hash = value['service_hash']
            max_launch_window = value['max_launch_window']
            profile_max_frontier = value.get('max_exploration_frontier')
            if (not isinstance(workspace, str) or not workspace or
                    not isinstance(service_name, str) or not service_name or
                    not isinstance(service_hash, str) or not service_hash or
                    type(max_launch_window) is not int or  # pylint: disable=unidiomatic-typecheck
                    max_launch_window <= 0 or
                (profile_max_frontier is not None and
                 (type(profile_max_frontier) is not int or  # pylint: disable=unidiomatic-typecheck
                  profile_max_frontier <= 0))):
                raise ValueError('profile contains invalid values')
            identity = (workspace, service_name, service_hash)
            if identity in identities:
                raise ValueError('profile identities must be unique')
            identities.add(identity)
            profiles.append(
                ServiceLimitProfile(
                    workspace=workspace,
                    service_name=service_name,
                    service_hash=service_hash,
                    max_launch_window=max_launch_window,
                    max_exploration_frontier=(profile_max_frontier)))
        return tuple(profiles)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        logger.warning('Ignoring invalid paid service launch-window profile '
                       f'document: {error}.')
        return ()


def _matching_service_profile(
        workspace: str | None, service_name: str | None,
        service_hash: str | None) -> ServiceLimitProfile | None:
    if not workspace or not service_name or not service_hash:
        return None
    profiles = _parse_service_limit_profiles(
        os.environ.get(_SERVICE_LIMIT_PROFILES_ENV_VAR))
    return next(
        (profile for profile in profiles
         if (profile.workspace, profile.service_name,
             profile.service_hash) == (workspace, service_name, service_hash)),
        None)


def max_service_limit(*,
                      workspace: str | None = None,
                      service_name: str | None = None,
                      service_hash: str | None = None) -> int:
    """Return the effective adaptive ceiling for one service incarnation."""
    floor = service_limit()
    configured = _parse_positive_int(os.environ.get(_SERVICE_MAX_LIMIT_ENV_VAR),
                                     _SERVICE_LIMIT_DEFAULT,
                                     _SERVICE_MAX_LIMIT_ENV_VAR)
    profile = _matching_service_profile(workspace, service_name, service_hash)
    if profile is not None:
        configured = profile.max_launch_window
    if configured < floor:
        _warn_service_max_below_floor(floor, configured)
    return max(floor, configured)


@functools.cache
def _warn_service_max_below_floor(floor: int, configured: int) -> None:
    logger.warning(
        'Paid service maximum launch window '
        f'{configured} is below the cold launch window {floor}; using '
        f'{floor}.')


def exploration_frontier() -> int:
    """Return the number of paid pools one service/card may explore."""
    return _parse_positive_int(os.environ.get(_EXPLORATION_FRONTIER_ENV_VAR),
                               _EXPLORATION_FRONTIER_DEFAULT,
                               _EXPLORATION_FRONTIER_ENV_VAR)


def max_exploration_frontier() -> int:
    """Return the largest delayed-feedback exploration frontier."""
    configured = _parse_positive_int(
        os.environ.get(_MAX_EXPLORATION_FRONTIER_ENV_VAR),
        _MAX_EXPLORATION_FRONTIER_DEFAULT, _MAX_EXPLORATION_FRONTIER_ENV_VAR)
    return max(exploration_frontier(), configured)


def max_service_exploration_frontier(*,
                                     workspace: str | None = None,
                                     service_name: str | None = None,
                                     service_hash: str | None = None) -> int:
    """Return one incarnation's delayed-feedback frontier ceiling."""
    configured = max_exploration_frontier()
    profile = _matching_service_profile(workspace, service_name, service_hash)
    if (profile is not None and profile.max_exploration_frontier is not None):
        configured = profile.max_exploration_frontier
    return max(exploration_frontier(), configured)


def exploration_feedback_delay_seconds() -> int:
    """Return how old every unresolved claim must be before expansion."""
    return _parse_positive_int(
        os.environ.get(_EXPLORATION_FEEDBACK_DELAY_SECONDS_ENV_VAR),
        _EXPLORATION_FEEDBACK_DELAY_SECONDS_DEFAULT,
        _EXPLORATION_FEEDBACK_DELAY_SECONDS_ENV_VAR)


def success_ttl_seconds() -> int:
    """Return how long successful feedback keeps an expanded cohort."""
    return _parse_positive_int(os.environ.get(_SUCCESS_TTL_SECONDS_ENV_VAR),
                               _SUCCESS_TTL_SECONDS_DEFAULT,
                               _SUCCESS_TTL_SECONDS_ENV_VAR)


def waiter_ttl_seconds() -> int:
    """Return how long a service's priority heartbeat remains eligible."""
    return _parse_positive_int(os.environ.get(_WAITER_TTL_SECONDS_ENV_VAR),
                               _WAITER_TTL_SECONDS_DEFAULT,
                               _WAITER_TTL_SECONDS_ENV_VAR)


def failure_cooldown_seconds() -> int:
    """Return how long typed capacity failure closes an exact paid pool."""
    return _parse_positive_int(
        os.environ.get(_FAILURE_COOLDOWN_SECONDS_ENV_VAR),
        _FAILURE_COOLDOWN_SECONDS_DEFAULT, _FAILURE_COOLDOWN_SECONDS_ENV_VAR)


def limit_ladder(bootstrap_limit: int, ceiling_limit: int) -> tuple[int, ...]:
    """Return every valid persisted adaptive-limit rung."""
    bootstrap_limit = max(1, int(bootstrap_limit))
    ceiling_limit = max(bootstrap_limit, int(ceiling_limit))
    values = [bootstrap_limit]
    while values[-1] < ceiling_limit:
        next_value = min(ceiling_limit, values[-1] * 2)
        if next_value == values[-1]:
            break
        values.append(next_value)
    return tuple(values)


def effective_limit(
    current_limit: int,
    last_success_at: float | None,
    *,
    bootstrap_limit: int,
    ceiling_limit: int,
    now: float,
    ttl_seconds: float,
) -> tuple[int, bool]:
    """Clamp and expire one persisted adaptive limit."""
    ladder = limit_ladder(bootstrap_limit, ceiling_limit)
    if int(current_limit) not in ladder:
        # Revision 027 used a 60/120/240/480 ladder. Conservatively reset old
        # rungs when the configured bootstrap changes instead of preserving an
        # unearned cohort. The failure/probe path bypasses this helper while
        # current_limit=1 is its intentional marker.
        return bootstrap_limit, True
    effective = int(current_limit)
    has_positive_evidence = (effective > bootstrap_limit or
                             last_success_at is not None)
    expired = (has_positive_evidence and (last_success_at is None or
                                          now - last_success_at >= ttl_seconds))
    return (bootstrap_limit if expired else effective), expired


def effective_admission_limit(
    current_limit: int,
    last_success_at: float | None,
    last_failure_at: float | None,
    *,
    bootstrap_limit: int,
    ceiling_limit: int,
    now: float,
    success_ttl: float,
    failure_cooldown: float,
) -> AdmissionLimit:
    """Return normal, cooldown-closed, or one-probe pool admission."""
    if last_failure_at is not None:
        cooldown_until = last_failure_at + failure_cooldown
        if now < cooldown_until:
            return AdmissionLimit(limit=0,
                                  state='cooldown',
                                  cooldown_until=cooldown_until)
        return AdmissionLimit(limit=1,
                              state='probe',
                              cooldown_until=cooldown_until)
    effective, _ = effective_limit(current_limit,
                                   last_success_at,
                                   bootstrap_limit=bootstrap_limit,
                                   ceiling_limit=ceiling_limit,
                                   now=now,
                                   ttl_seconds=success_ttl)
    return AdmissionLimit(limit=effective, state='active', cooldown_until=None)


def record_outcomes(
    current_limit: int,
    successes_since_resize: int,
    last_success_at: float | None,
    outcomes: Iterable[LaunchOutcome],
    *,
    bootstrap_limit: int,
    ceiling_limit: int,
    now: float,
    ttl_seconds: float,
) -> RampUpdate:
    """Apply genuine launch feedback to one exact pool's adaptive limit."""
    completed = list(outcomes)
    if not completed:
        raise ValueError('At least one paid-capacity outcome is required.')
    if (LaunchOutcome.CAPACITY_FAILURE in completed or
            LaunchOutcome.QUOTA_FAILURE in completed):
        return RampUpdate(current_limit=bootstrap_limit,
                          successes_since_resize=0,
                          expired=False,
                          failed=True)
    successful = sum(outcome == LaunchOutcome.SUCCESS for outcome in completed)
    if successful == 0:
        return RampUpdate(current_limit=int(current_limit),
                          successes_since_resize=max(
                              0, int(successes_since_resize)),
                          expired=False,
                          failed=False)

    current_limit, expired = effective_limit(current_limit,
                                             last_success_at,
                                             bootstrap_limit=bootstrap_limit,
                                             ceiling_limit=ceiling_limit,
                                             now=now,
                                             ttl_seconds=ttl_seconds)
    success_count = (0 if expired else max(0, int(successes_since_resize)))
    success_count += successful
    while success_count >= current_limit and current_limit < ceiling_limit:
        success_count -= current_limit
        current_limit = min(ceiling_limit, current_limit * 2)
    if current_limit >= ceiling_limit:
        success_count = min(success_count, ceiling_limit - 1)
    return RampUpdate(current_limit=current_limit,
                      successes_since_resize=success_count,
                      expired=expired,
                      failed=False)


def _normalized_accelerators(
    accelerators: Mapping[str, int | float] | None
) -> list[list[str | int | float]]:
    if not accelerators:
        return []
    normalized = []
    for name, count in sorted(accelerators.items(),
                              key=lambda item: item[0].casefold()):
        normalized_count: int | float = count
        if isinstance(count, float) and count.is_integer():
            normalized_count = int(count)
        normalized.append([str(name).casefold(), normalized_count])
    return normalized


def _active_aws_account_id_for_workspace(workspace: str,
                                         cloud: clouds.Cloud) -> str:
    """Resolve the account that one workspace will use for AWS effects."""
    if not isinstance(workspace, str) or not workspace:
        raise ValueError('workspace must be nonempty.')
    if not isinstance(cloud, clouds.AWS):
        raise ValueError('AWS account resolution requires an AWS cloud.')
    with skypilot_config.local_active_workspace_ctx(workspace):
        active_identity = cloud.get_active_user_identity()
    account_id = (active_identity[1] if isinstance(active_identity,
                                                   (list, tuple)) and
                  len(active_identity) >= 2 else None)
    if (not isinstance(account_id, str) or
            _AWS_ACCOUNT_ID_RE.fullmatch(account_id) is None):
        raise ValueError('Active AWS account identity is unavailable.')
    return account_id


def _provider_identity_for_location(
    location: spot_placer.Location,
    *,
    workspace: str,
    aws_account_id: str | None = None,
) -> dict[str, str] | None:
    if not isinstance(location.cloud, clouds.AWS):
        return None
    account_id = aws_account_id
    if (not isinstance(account_id, str) or
            _AWS_ACCOUNT_ID_RE.fullmatch(account_id) is None):
        raise ValueError('AWS paid pool requires one exact account ID.')
    return {'aws_account_id': account_id}


def _active_aws_account_id_for_locations(
        locations: Iterable[spot_placer.Location], *,
        workspace: str) -> str | None:
    """Resolve one workspace account once for a placement/budget snapshot."""
    aws_clouds = [
        location.cloud
        for location in locations
        if isinstance(location.cloud, clouds.AWS)
    ]
    if not aws_clouds:
        return None
    return _active_aws_account_id_for_workspace(workspace, aws_clouds[0])


def resolve_aws_account_id_for_locations(
        locations: Iterable[spot_placer.Location], *,
        workspace: str) -> str | None:
    """Resolve AWS identity before entering launch-admission locks.

    This is the provider-I/O seam for immutable paid launch preparation.  A
    caller must invoke it before taking a ReplicaManager, routing, or database
    lock and then pass the returned scalar into provider-free preparation.
    """
    return _active_aws_account_id_for_locations(locations, workspace=workspace)


def pool_key(location: spot_placer.Location,
             *,
             workspace: str,
             num_nodes: int,
             aws_account_id: str | None = None) -> str:
    """Build a stable identity for one exact provider capacity pool."""
    payload = {
        # Account scope is an AWS provider requirement.  Keep every other
        # provider on the existing v1 identity so this AWS-only safety change
        # neither resets its admission history nor widens its rollout surface.
        'version': (_POOL_KEY_VERSION if isinstance(location.cloud, clouds.AWS)
                    else _LEGACY_POOL_KEY_VERSION),
        'workspace': workspace,
        'cloud': str(location.cloud).casefold(),
        'region': location.region,
        'zone': location.zone,
        'instance_type': location.instance_type,
        'accelerators': _normalized_accelerators(location.accelerators),
        'use_spot': location.use_spot,
        'num_nodes': num_nodes,
    }
    if isinstance(location.cloud, clouds.AWS):
        payload['provider_identity'] = _provider_identity_for_location(
            location, workspace=workspace, aws_account_id=aws_account_id)
    return json.dumps(payload, sort_keys=True, separators=(',', ':'))


def _legacy_pool_key(location: spot_placer.Location, *, workspace: str,
                     num_nodes: int) -> str:
    """Build the account-unscoped v1 key used only for legacy settlement.

    A replica which predates account-scoped paid admission has no durable fact
    proving which AWS account owns a possibly-created provider object.  Its
    recovery claim must preserve that uncertainty instead of freezing whatever
    account happens to be active after restart.  Cohort-11 provider effects
    reject this legacy key, while the claim can still conservatively account
    for the unresolved replica.
    """
    payload = {
        'version': _LEGACY_POOL_KEY_VERSION,
        'workspace': workspace,
        'cloud': str(location.cloud).casefold(),
        'region': location.region,
        'zone': location.zone,
        'instance_type': location.instance_type,
        'accelerators': _normalized_accelerators(location.accelerators),
        'use_spot': location.use_spot,
        'num_nodes': num_nodes,
    }
    return json.dumps(payload, sort_keys=True, separators=(',', ':'))


def frontier_key(location: spot_placer.Location) -> FrontierKey:
    """Return one model-only exploration identity for a paid location."""
    accelerators = location.accelerators
    if not accelerators:
        return ()
    return tuple(
        sorted((str(name).casefold() for name in accelerators),
               key=str.casefold))


def _pool_key_payload(key: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(key)
    except (TypeError, ValueError):
        return None
    legacy_fields = {
        'version', 'workspace', 'cloud', 'region', 'zone', 'instance_type',
        'accelerators', 'use_spot', 'num_nodes'
    }
    current_fields = legacy_fields | {'provider_identity'}
    if (not isinstance(payload, dict) or
        (payload.get('version') == _LEGACY_POOL_KEY_VERSION and
         set(payload) != legacy_fields) or
        (payload.get('version') == _POOL_KEY_VERSION and
         set(payload) != current_fields) or payload.get('version')
            not in (_LEGACY_POOL_KEY_VERSION, _POOL_KEY_VERSION) or
            type(payload.get('version')) is not int or  # pylint: disable=unidiomatic-typecheck
            not isinstance(payload.get('workspace'), str)
            or not payload['workspace']
            or not isinstance(payload.get('cloud'), str) or not payload['cloud']
            or not isinstance(payload.get('region'), str)
            or not payload['region'] or
        (payload.get('zone') is not None and
         (not isinstance(payload['zone'], str) or not payload['zone'])) or
        (payload.get('instance_type') is not None and
         (not isinstance(payload['instance_type'], str) or
          not payload['instance_type']))
            or type(payload.get('use_spot')) is not bool or  # pylint: disable=unidiomatic-typecheck
            type(payload.get('num_nodes')) is not int or  # pylint: disable=unidiomatic-typecheck
            payload['num_nodes'] <= 0
            or payload['num_nodes'] > _MAX_EXACT_SHAPE_INTEGER
            or not isinstance(payload.get('accelerators'), list)):
        return None
    if payload['version'] == _POOL_KEY_VERSION:
        provider_identity = payload['provider_identity']
        if (payload['cloud'] != 'aws' or
                not isinstance(provider_identity, dict) or
                set(provider_identity) != {'aws_account_id'} or
                not isinstance(provider_identity['aws_account_id'], str) or
                _AWS_ACCOUNT_ID_RE.fullmatch(
                    provider_identity['aws_account_id']) is None):
            return None
    names = []
    for accelerator in payload['accelerators']:
        if not isinstance(accelerator, list) or len(accelerator) != 2:
            return None
        count = accelerator[1]
        finite_count = type(count) is int  # pylint: disable=unidiomatic-typecheck
        if isinstance(count, float):
            finite_count = math.isfinite(count)
        if (not isinstance(accelerator[0], str) or not accelerator[0] or
                accelerator[0] != accelerator[0].casefold() or
                not finite_count or count <= 0 or
                count > _MAX_EXACT_SHAPE_INTEGER):
            return None
        names.append(accelerator[0])
    if len(names) != len(set(names)) or names != sorted(names,
                                                        key=str.casefold):
        return None
    if json.dumps(payload, sort_keys=True, separators=(',', ':')) != key:
        return None
    return payload


def pool_key_payload(key: str) -> dict[str, Any] | None:
    """Decode one canonical exact paid provider-pool identity."""
    payload = _pool_key_payload(key)
    return None if payload is None else dict(payload)


def _frozen_aws_account_id_from_replica_infos(
        replica_infos: Iterable['replica_managers.ReplicaInfo'], *,
        workspace: str) -> str | None:
    """Recover one already-committed account without provider I/O."""
    account_ids: set[str] = set()
    for info in replica_infos:
        key = getattr(info, 'paid_capacity_pool_key', None)
        if not isinstance(key, str):
            continue
        payload = _pool_key_payload(key)
        if (payload is None or payload['version'] != _POOL_KEY_VERSION or
                payload['workspace'] != workspace or payload['cloud'] != 'aws'):
            continue
        provider_identity = payload['provider_identity']
        assert isinstance(provider_identity, dict)
        account_ids.add(provider_identity['aws_account_id'])
    if len(account_ids) != 1:
        return None
    return next(iter(account_ids))


def frontier_key_from_pool_key(key: str) -> FrontierKey | None:
    """Recover a card frontier identity from one versioned exact pool key."""
    payload = _pool_key_payload(key)
    if payload is None:
        return None
    return tuple(accelerator[0] for accelerator in payload['accelerators'])


def _legacy_local_remaining(
    placer: spot_placer.SpotPlacer,
    paid_locations: Iterable[spot_placer.Location],
    existing_replica_infos: list['replica_managers.ReplicaInfo'],
) -> dict[spot_placer.Location, int]:
    remaining = {location: legacy_local_limit() for location in paid_locations}
    for info in existing_replica_infos:
        if info.status.value not in _UNRESOLVED_STATUS_VALUES:
            continue
        replica_location = info.get_spot_location()
        if replica_location is None:
            continue
        # Local SQLite has no cross-service claim identity to protect. Mirror
        # the operational rollout resolver so an ambiguous pre-instance-type
        # row still debits the cheapest compatible pool it would launch on.
        resolved = placer.resolve_location(replica_location,
                                           allow_ambiguous_legacy_shape=True)
        if resolved in remaining:
            remaining[resolved] = max(0, remaining[resolved] - 1)
    return remaining


def central_authority_available() -> bool:
    """Whether this process has the PostgreSQL shared-authority backend."""
    return (serve_state.get_database_engine().dialect.name == 'postgresql')


def _log_admission_summary(states: dict[str, dict[str, Any]],
                           *,
                           service_claims: int,
                           service_claim_limit: int,
                           max_live_paid_gpu_units: int | None = None,
                           live_paid_gpu_units: int | None = None) -> None:
    """Log one bounded shared-admission summary on transition or interval."""
    if not states:
        return
    state_counts = collections.Counter(
        str(state.get('admission_state', 'active'))
        for state in states.values())
    overage_pools = sum(
        int(state.get('legacy_overage', 0)) > 0 for state in states.values())
    saturated_pools = sum(
        int(state.get('remaining', 0)) == 0 for state in states.values())
    service_remaining = max(0, service_claim_limit - service_claims)
    paid_gpu_units_remaining = None
    if (max_live_paid_gpu_units is not None and
            live_paid_gpu_units is not None):
        paid_gpu_units_remaining = max(
            0, max_live_paid_gpu_units - live_paid_gpu_units)
    signature = (len(states), tuple(sorted(state_counts.items())), overage_pools
                 > 0, service_remaining == 0, max_live_paid_gpu_units,
                 live_paid_gpu_units, paid_gpu_units_remaining)
    observed_at = time.monotonic()
    global _admission_summary_log_signature
    global _admission_summary_logged_at
    with _admission_summary_log_lock:
        elapsed = observed_at - _admission_summary_logged_at
        if (_admission_summary_log_signature is not None and
                elapsed < _ADMISSION_SUMMARY_LOG_MIN_INTERVAL_SECONDS):
            return
        if (signature == _admission_summary_log_signature and
                elapsed < _ADMISSION_SUMMARY_LOG_INTERVAL_SECONDS):
            return
        _admission_summary_log_signature = signature
        _admission_summary_logged_at = observed_at
    logger.info(
        'Global paid-capacity admission: '
        f'pools={len(states)}, states={dict(sorted(state_counts.items()))}, '
        f'active_claims={sum(int(state.get("active_claims", 0)) for state in states.values())}, '
        f'admission_limit={sum(int(state.get("admission_limit", 0)) for state in states.values())}, '
        f'remaining={sum(int(state.get("remaining", 0)) for state in states.values())}, '
        f'saturated_pools={saturated_pools}, '
        f'legacy_overage_claims={sum(int(state.get("legacy_overage", 0)) for state in states.values())}, '
        f'service_claims={service_claims}, '
        f'service_limit={service_claim_limit}, '
        f'max_live_paid_gpu_units={max_live_paid_gpu_units}, '
        f'live_paid_gpu_units={live_paid_gpu_units}, '
        f'paid_gpu_units_remaining={paid_gpu_units_remaining}, '
        f'service_remaining={service_remaining}.')


def _service_claim_count(
        existing_replica_infos: Iterable['replica_managers.ReplicaInfo']
) -> int:
    """Count this service's unresolved rows with an exact paid claim."""
    return sum(info.status.value in _UNRESOLVED_STATUS_VALUES and
               isinstance(info.paid_capacity_pool_key, str)
               for info in existing_replica_infos)


def _exact_whole_gpu_shape(
    accelerators: Any,
    *,
    field: str,
) -> tuple[str, int]:
    """Decode one exact whole-GPU accelerator shape."""
    if not isinstance(accelerators, Mapping) or len(accelerators) != 1:
        raise PaidGPUAttributionError(
            f'{field} must contain one exact accelerator shape.')
    card, count = next(iter(accelerators.items()))
    valid_count = (
        type(count) is int and 1 <= count <=  # pylint: disable=unidiomatic-typecheck
        _MAX_EXACT_SHAPE_INTEGER)
    if isinstance(count, float):
        valid_count = (math.isfinite(count) and
                       1 <= count <= _MAX_EXACT_SHAPE_INTEGER and
                       count.is_integer())
    if not isinstance(card, str) or not card or not valid_count:
        raise PaidGPUAttributionError(
            f'{field} must contain one positive whole-GPU shape.')
    return card.casefold(), int(count)


def _paid_pool_gpu_shape(pool_key_value: Any) -> PhysicalBackendShape:
    """Decode the billing shape from one canonical exact paid pool."""
    if not isinstance(pool_key_value, str):
        raise PaidGPUAttributionError(
            'Paid replica has no exact provider pool identity.')
    try:
        payload = _pool_key_payload(pool_key_value)
    except (OverflowError, ValueError) as error:
        raise PaidGPUAttributionError(
            'Paid replica provider pool identity is malformed.') from error
    if payload is None or len(payload['accelerators']) > 1:
        raise PaidGPUAttributionError(
            'Paid replica provider pool identity is malformed.')
    card = None
    count = 0
    if payload['accelerators']:
        raw_card, raw_count = payload['accelerators'][0]
        card, count = _exact_whole_gpu_shape({raw_card: raw_count},
                                             field='paid provider pool')
    num_nodes = payload['num_nodes']
    # _pool_key_payload() already requires an exact positive integer. Keep the
    # check local so this function remains fail-closed if that codec changes.
    if type(num_nodes) is not int or num_nodes < 1:  # pylint: disable=unidiomatic-typecheck
        raise PaidGPUAttributionError(
            'Paid replica provider pool has malformed node cardinality.')
    return PhysicalBackendShape(accelerator=card,
                                gpu_units_per_node=count,
                                num_nodes=num_nodes)


def _cross_check_replica_gpu_shape(resource: Any, *, field: str,
                                   expected: tuple[str, int] | None) -> None:
    """Require any persisted duplicate shape to match the paid pool."""
    if resource is None:
        return
    if not isinstance(resource, Mapping):
        raise PaidGPUAttributionError(f'Paid replica {field} is malformed.')
    if 'accelerators' not in resource:
        return
    if expected is None:
        accelerators = resource.get('accelerators')
        if accelerators is None or accelerators == {}:
            return
        raise PaidGPUAttributionError(
            f'Paid replica {field} disagrees with its CPU-only provider pool.')
    observed = _exact_whole_gpu_shape(resource.get('accelerators'),
                                      field=f'paid replica {field}')
    if observed != expected:
        raise PaidGPUAttributionError(
            f'Paid replica {field} disagrees with its provider pool.')


def paid_replica_cleanup_proven(
    replica: Any,
    *,
    sky_down_status_value: Any,
) -> bool:
    """Cross-check the JSON cleanup copy against its relational authority."""
    if not isinstance(replica, Mapping):
        raise PaidGPUAttributionError('Paid replica state is malformed.')
    status = replica.get('status_property')
    if not isinstance(status, Mapping):
        raise PaidGPUAttributionError(
            'Paid replica cleanup attribution is malformed.')
    if status.get('sky_down_status') != sky_down_status_value:
        raise PaidGPUAttributionError(
            'Paid replica cleanup scalar contradicts ReplicaInfo state.')
    return (sky_down_status_value == common_utils.ProcessStatus.SUCCEEDED.value)


def validate_paid_replica_relational_copies(
    replica: Any,
    *,
    pool_key_value: Any,
) -> bool:
    """Validate paid/zero-cost JSON copies after cleanup remains unproven.

    Returns whether the authoritative relational pool scalar classifies this
    row as paid. A matched durable cleanup proof should be checked first: once
    provider cleanup is complete, stale historical billing-shape copies do not
    retain phantom paid capacity.
    """
    if not isinstance(replica, Mapping):
        raise PaidGPUAttributionError('Paid replica state is malformed.')
    persisted_pool_key = replica.get('paid_capacity_pool_key')
    is_zero_cost = replica.get('is_zero_cost')
    if persisted_pool_key != pool_key_value:
        raise PaidGPUAttributionError(
            'Paid replica row and JSON name different provider pools.')
    relationally_paid = pool_key_value is not None
    if ((relationally_paid and is_zero_cost is True) or
        (not relationally_paid and is_zero_cost is False)):
        raise PaidGPUAttributionError(
            'Paid replica pool contradicts zero-cost attribution.')
    return relationally_paid


def paid_replica_gpu_units(
    replica: Any,
    *,
    pool_key_value: Any = _UNSET_POOL_KEY,
) -> int:
    """Return one paid replica's exact physical GPU debit.

    ``planned_capacity`` deliberately remains the service's capacity unit: it
    is one for a physical-backend service and the logical slot width for a
    logical service. Billing width instead comes from the canonical paid pool,
    whose ``num_nodes`` is not duplicated by ReplicaInfo location overrides.
    The duplicated per-node shapes are consistency checks only.
    """
    if isinstance(replica, Mapping):
        persisted_pool_key = replica.get('paid_capacity_pool_key')
        location = replica.get('location')
        resources_override = replica.get('resources_override')
        is_zero_cost = replica.get('is_zero_cost')
    else:
        persisted_pool_key = getattr(replica, 'paid_capacity_pool_key', None)
        location = getattr(replica, 'location', None)
        resources_override = getattr(replica, 'resources_override', None)
        is_zero_cost = getattr(replica, 'is_zero_cost', None)
    if is_zero_cost is True:
        raise PaidGPUAttributionError(
            'Paid provider pool contradicts zero-cost replica attribution.')
    if pool_key_value is _UNSET_POOL_KEY:
        pool_key_value = persisted_pool_key
    elif (persisted_pool_key is not None and
          persisted_pool_key != pool_key_value):
        raise PaidGPUAttributionError(
            'Paid replica row and claim name different provider pools.')
    shape = _paid_pool_gpu_shape(pool_key_value)
    expected = (None if shape.accelerator is None else
                (shape.accelerator, shape.gpu_units_per_node))
    _cross_check_replica_gpu_shape(location,
                                   field='location',
                                   expected=expected)
    _cross_check_replica_gpu_shape(resources_override,
                                   field='resources_override',
                                   expected=expected)
    return shape.total_gpu_units


def paid_pool_gpu_units(pool_key_value: Any) -> int:
    """Return the exact total GPU debit for one canonical provider pool."""
    return _paid_pool_gpu_shape(pool_key_value).total_gpu_units


def paid_pool_gpu_shape(pool_key_value: Any) -> PhysicalBackendShape:
    """Return the canonical typed physical shape of one provider pool."""
    return _paid_pool_gpu_shape(pool_key_value)


def _live_paid_gpu_units(
        existing_replica_infos: Iterable['replica_managers.ReplicaInfo']
) -> int:
    """Sum paid GPU units whose provider cleanup is not durably complete."""
    total = 0
    for info in existing_replica_infos:
        down_status = info.status_property.sky_down_status
        down_status_value = getattr(down_status, 'value', down_status)
        if (info.is_zero_cost is not True and down_status_value
                != common_utils.ProcessStatus.SUCCEEDED.value):
            total += paid_replica_gpu_units(info)
    return total


def _budget_location_gpu_units(budget: LaunchBudget,
                               location: spot_placer.Location) -> int | None:
    """Resolve one advisory location's exact total-backend GPU debit."""
    pool = budget.pool_key_by_location.get(location)
    if pool is None:
        return None
    try:
        return paid_pool_gpu_units(pool)
    except PaidGPUAttributionError:
        return None


def _evidence_aware_service_limit(
    *,
    paid_locations: Iterable[spot_placer.Location],
    states_by_pool_key: Mapping[str, Mapping[str, Any]],
    pool_key_by_location: Mapping[spot_placer.Location, str],
    frontier_key_by_location: Mapping[spot_placer.Location, FrontierKey],
    owned_pool_keys_by_frontier: Mapping[FrontierKey, set[str]],
    unknown_owned_pool_keys: set[str],
    requested_frontier_keys: set[FrontierKey] | None,
    floor: int,
    ceiling: int,
    frontier_ceiling: int | None = None,
) -> int:
    """Return a bounded service envelope backed by durable pool success."""
    floor = max(1, int(floor))
    ceiling = max(floor, int(ceiling))
    if ceiling == floor:
        return floor

    ordered_candidates: dict[FrontierKey,
                             list[str]] = (collections.defaultdict(list))
    for location in paid_locations:
        frontier = frontier_key_by_location.get(location,
                                                frontier_key(location))
        if (requested_frontier_keys is not None and
                frontier not in requested_frontier_keys):
            continue
        pool = pool_key_by_location.get(location)
        if pool is not None and pool not in ordered_candidates[frontier]:
            ordered_candidates[frontier].append(pool)

    productive_limit = 0
    base_frontier = exploration_frontier()
    maximum_frontier = max(
        base_frontier,
        max_exploration_frontier()
        if frontier_ceiling is None else frontier_ceiling)
    for frontier, candidates in ordered_candidates.items():
        owned = (set(owned_pool_keys_by_frontier.get(frontier, set())) |
                 unknown_owned_pool_keys)
        frontier_width = min(maximum_frontier, max(base_frontier, len(owned)))
        # Opaque claims consume every card frontier and contribute no positive
        # evidence. Known owned pools follow in candidate cost order, then the
        # cheapest eligible unowned pools fill the remaining bounded frontier.
        opaque_pools = set(unknown_owned_pool_keys)
        known_owned_pools = set(owned_pool_keys_by_frontier.get(
            frontier, set()))
        ordered_pools = sorted(opaque_pools)
        ordered_pools.extend(
            pool for pool in candidates
            if pool in known_owned_pools and pool not in ordered_pools)
        ordered_pools.extend(pool for pool in sorted(known_owned_pools)
                             if pool not in ordered_pools)
        ordered_pools.extend(
            pool for pool in candidates if pool not in ordered_pools)
        for pool in ordered_pools[:frontier_width]:
            if pool in opaque_pools:
                continue
            state = states_by_pool_key.get(pool)
            if (state is None or state.get('admission_state') != 'active' or
                    state.get('last_success_at') is None):
                continue
            admission_limit = state.get('admission_limit')
            if (type(admission_limit) is int and  # pylint: disable=unidiomatic-typecheck
                    admission_limit > 0):
                productive_limit += admission_limit
                if productive_limit >= ceiling:
                    return ceiling
    return min(ceiling, max(floor, productive_limit))


def _plan_bound_admission_cohort(
    *,
    authority: 'capacity_admission.PaidLaunchAuthority',
    service_name: str | None,
    service_hash: str | None,
    paid_locations: Sequence[spot_placer.Location],
    remaining_by_location: Mapping[spot_placer.Location, int],
    pool_key_by_location: Mapping[spot_placer.Location, str],
    frontier_key_by_location: Mapping[spot_placer.Location, FrontierKey],
    owned_pool_keys_by_frontier: Mapping[FrontierKey, set[str]],
    unknown_owned_pool_keys: set[str],
    requested_frontier_keys: set[FrontierKey] | None,
    claimed_plan_units_by_accelerator: Mapping[str, int],
) -> PlanBoundAdmissionCohort:
    """Project one plan target onto the smallest safe provider frontier.

    Pool limits remain failure-containment bounds.  They must not be summed
    into a second service-wide demand authority: a large immutable plan may
    instead use more exact pools in the placer's canonical price order.
    """
    if (authority.service_name != service_name or
            authority.service_hash != service_hash):
        raise ValueError(
            'Paid plan authority does not match the service incarnation.')
    raw_targets = authority.remaining_launch_capacity()
    canonical_claimed: dict[str, int] = {}
    for raw_card, raw_units in claimed_plan_units_by_accelerator.items():
        if (not isinstance(raw_card, str) or not raw_card or
                type(raw_units) is not int or raw_units < 0):
            raise ValueError(
                'Paid plan committed-debit projection is malformed.')
        card = raw_card.casefold()
        canonical_claimed[card] = canonical_claimed.get(card, 0) + raw_units
    targets_by_frontier: dict[FrontierKey, int] = {}
    for raw_card, raw_units in raw_targets.items():
        if (not isinstance(raw_card, str) or not raw_card or raw_card == '*' or
                type(raw_units) is not int or raw_units < 0):
            raise ValueError('Paid plan launch target is malformed.')
        card = raw_card.casefold()
        claimed_units = canonical_claimed.pop(card, 0)
        remaining_units = raw_units - claimed_units
        if remaining_units < 0:
            raise ValueError('Paid plan committed debits exceed its target.')
        if remaining_units == 0:
            continue
        target_frontier: FrontierKey = (card,)
        if target_frontier in targets_by_frontier:
            raise ValueError('Paid plan launch target repeats a card.')
        if (requested_frontier_keys is not None and
                target_frontier not in requested_frontier_keys):
            continue
        targets_by_frontier[target_frontier] = remaining_units
    if any(units > 0 for units in canonical_claimed.values()):
        raise ValueError('Paid plan has debits outside its target cards.')

    if unknown_owned_pool_keys and targets_by_frontier:
        # An opaque unresolved claim cannot be assigned to one card frontier.
        # It remains charged by the hard service GPU cap, but no new provider
        # exposure is safe until its exact pool identity is recovered.
        raise ValueError(
            'Plan-bound paid cohort has an opaque owned provider pool.')

    distinct_locations_by_frontier: dict[
        FrontierKey, list[spot_placer.Location]] = collections.defaultdict(list)
    seen_pool_keys_by_frontier: dict[FrontierKey,
                                     set[str]] = collections.defaultdict(set)
    for location in paid_locations:
        if location.use_spot is not True:
            continue
        location_frontier = frontier_key_by_location.get(
            location, frontier_key(location))
        if location_frontier not in targets_by_frontier:
            continue
        pool = pool_key_by_location.get(location)
        if pool is None:
            continue
        try:
            authority_shape = authority.backend_shape(location_frontier[0])
            pool_shape = paid_pool_gpu_shape(pool)
        except Exception:  # pylint: disable=broad-except
            continue
        if pool_shape != authority_shape:
            continue
        if pool in seen_pool_keys_by_frontier[location_frontier]:
            continue
        seen_pool_keys_by_frontier[location_frontier].add(pool)
        distinct_locations_by_frontier[location_frontier].append(location)

    targets: list[PlanBoundAdmissionTarget] = []
    default_frontier = exploration_frontier()
    for target_frontier, target_units in sorted(targets_by_frontier.items()):
        locations = distinct_locations_by_frontier.get(target_frontier, [])
        try:
            backend_shape = authority.backend_shape(target_frontier[0])
            claim_units = authority.claim_units_per_backend(target_frontier[0])
        except Exception as error:  # pylint: disable=broad-except
            logger.warning('Deferring malformed plan-bound paid card %s: %s',
                           target_frontier[0],
                           common_utils.format_exception(error))
            continue
        if not locations:
            logger.info(
                'Deferring plan-bound paid card %s: no Spot candidate '
                'has physical backend width %d.', target_frontier[0],
                backend_shape.total_gpu_units)
            continue
        if target_units % claim_units != 0:
            logger.warning(
                'Deferring malformed plan-bound paid card %s: '
                'target %d is not quantized to claim width %d.',
                target_frontier[0], target_units, claim_units)
            continue
        target_claims = target_units // claim_units
        if target_claims <= 0:
            continue

        owned = set(owned_pool_keys_by_frontier.get(target_frontier, set()))
        remaining_claims = target_claims
        # Existing owned pools consume frontier exposure whether or not they
        # have current headroom.  Reuse their exact-pool headroom first, then
        # add only the cheapest unowned pools required for the residual.
        for location in locations:
            pool = pool_key_by_location[location]
            if pool not in owned:
                continue
            remaining_claims = max(
                0, remaining_claims -
                max(0, int(remaining_by_location.get(location, 0))))
        new_pool_count = 0
        for location in locations:
            if remaining_claims <= 0:
                break
            pool = pool_key_by_location[location]
            if pool in owned:
                continue
            remaining = max(0, int(remaining_by_location.get(location, 0)))
            if remaining <= 0:
                continue
            new_pool_count += 1
            remaining_claims = max(0, remaining_claims - remaining)

        targets.append(
            PlanBoundAdmissionTarget(
                frontier_key=target_frontier,
                remaining_plan_units=target_units,
                physical_backend_width=(backend_shape.total_gpu_units),
                claim_units_per_backend=claim_units,
                backend_claim_count=target_claims,
                frontier_limit=max(default_frontier,
                                   len(owned) + new_pool_count)))

    return PlanBoundAdmissionCohort(
        capacity_plan_generation=authority.generation,
        capacity_plan_sha256=authority.content_sha256,
        targets=tuple(targets))


def build_launch_budget(
    placer: spot_placer.SpotPlacer,
    *,
    workspace: str,
    existing_replica_infos: list['replica_managers.ReplicaInfo'],
    globally_managed: bool,
    service_name: str | None = None,
    service_hash: str | None = None,
    requested_frontier_keys: set[FrontierKey] | None = None,
    max_live_paid_gpu_units: int | None = None,
    allow_provider_identity_lookup: bool = True,
    paid_launch_authority:
    'capacity_admission.PaidLaunchAuthority | None' = None,
) -> LaunchBudget:
    """Read one advisory shared-capacity snapshot for all active paid pools."""
    if (max_live_paid_gpu_units is not None and
        (isinstance(max_live_paid_gpu_units, bool) or
         not isinstance(max_live_paid_gpu_units, int) or
         max_live_paid_gpu_units < 0)):
        raise ValueError('max_live_paid_gpu_units must be an integer >= 0.')
    zero_cost = set(placer.zero_cost_locations())
    paid_locations = [
        location for location in placer.ranked_active_locations()
        if location not in zero_cost
    ]
    if paid_launch_authority is not None:
        # Planner purchase authority is prospective Spot-only. On-demand
        # locations must not affect either cohort sizing or later selection.
        paid_locations = [
            location for location in paid_locations if location.use_spot is True
        ]
    paid_gpu_attribution_complete = True
    try:
        live_paid_gpu_units = _live_paid_gpu_units(existing_replica_infos)
    except PaidGPUAttributionError as error:
        paid_gpu_attribution_complete = False
        live_paid_gpu_units = None
        logger.warning('Disabling fresh paid capacity because a live replica '
                       'has no exact physical GPU debit: '
                       f'{common_utils.format_exception(error)}')
    central_available = globally_managed and central_authority_available()
    if paid_launch_authority is not None:
        identity_matches = (
            isinstance(service_name, str) and bool(service_name) and
            isinstance(service_hash, str) and bool(service_hash) and
            paid_launch_authority.service_name == service_name and
            paid_launch_authority.service_hash == service_hash)
        if not central_available or not identity_matches:
            # A committed planner target can only be spent through the shared
            # PostgreSQL Phase-A transaction for its exact service
            # incarnation.  It must never degrade to the process-local legacy
            # window when central authority or identity evidence is absent.
            logger.warning('Disabling planner-bound paid capacity: exact '
                           'PostgreSQL service authority is unavailable.')
            keys = {
                location: _legacy_pool_key(
                    location, workspace=workspace,
                    num_nodes=placer.num_nodes) for location in paid_locations
            }
            return LaunchBudget(
                remaining_by_location={
                    location: 0 for location in paid_locations
                },
                pool_key_by_location=keys,
                states_by_pool_key={},
                globally_managed=central_available,
                service_remaining=0,
                service_claim_limit=0,
                max_live_paid_gpu_units=max_live_paid_gpu_units,
                live_paid_gpu_units=live_paid_gpu_units,
                paid_gpu_units_remaining=(0 if max_live_paid_gpu_units
                                          is not None else None))
    if not central_available:
        # Preserve the pre-account-scope local policy exactly.  It neither has
        # PostgreSQL authority to freeze an account nor needs provider identity
        # to enforce its process-local unresolved-launch window.
        keys = {
            location: _legacy_pool_key(
                location, workspace=workspace,
                num_nodes=placer.num_nodes) for location in paid_locations
        }
        if max_live_paid_gpu_units is not None:
            # A configured hard cost bound requires the shared PostgreSQL
            # service lock. Never silently widen it to the legacy local
            # unresolved-launch window.
            return LaunchBudget(remaining_by_location={
                location: 0 for location in paid_locations
            },
                                pool_key_by_location=keys,
                                states_by_pool_key={},
                                globally_managed=False,
                                service_remaining=0,
                                max_live_paid_gpu_units=max_live_paid_gpu_units,
                                live_paid_gpu_units=live_paid_gpu_units,
                                paid_gpu_units_remaining=0)
        return LaunchBudget(remaining_by_location=_legacy_local_remaining(
            placer, paid_locations, existing_replica_infos),
                            pool_key_by_location=keys,
                            states_by_pool_key={},
                            globally_managed=False,
                            live_paid_gpu_units=live_paid_gpu_units)

    aws_account_id = None
    if allow_provider_identity_lookup:
        try:
            aws_account_id = _active_aws_account_id_for_locations(
                paid_locations, workspace=workspace)
        except Exception as error:  # pylint: disable=broad-except
            # Paid AWS identity is a hard correctness scope, but its transient
            # unavailability must not suppress zero-cost Kubernetes fill or other
            # paid providers in the same heterogeneous service.
            logger.warning('Disabling AWS paid candidates because the exact '
                           'workspace account is unavailable: '
                           f'{common_utils.format_exception(error)}')
            paid_locations = [
                location for location in paid_locations
                if not isinstance(location.cloud, clouds.AWS)
            ]
    else:
        # Dashboard/status rendering is advisory and must remain provider-free.
        # Reuse an exact account already frozen in a live replica, or omit AWS
        # admission from that read snapshot until the launch path freezes one.
        aws_account_id = _frozen_aws_account_id_from_replica_infos(
            existing_replica_infos, workspace=workspace)
        if aws_account_id is None:
            paid_locations = [
                location for location in paid_locations
                if not isinstance(location.cloud, clouds.AWS)
            ]
    keys = {
        location: pool_key(
            location,
            workspace=workspace,
            num_nodes=placer.num_nodes,
            aws_account_id=aws_account_id) for location in paid_locations
    }

    states = serve_state.get_paid_capacity_pool_states(
        list(keys.values()),
        base_limit=base_limit(),
        max_limit=max_limit(),
        now=None,
        success_ttl_seconds=success_ttl_seconds(),
        failure_cooldown_seconds=failure_cooldown_seconds())
    remaining = {
        location: int(states[key]['remaining'])
        for location, key in keys.items()
    }
    service_claims = _service_claim_count(existing_replica_infos)
    frontier_keys = {
        location: frontier_key(location) for location in paid_locations
    }
    owned_by_frontier: dict[FrontierKey,
                            set[str]] = collections.defaultdict(set)
    oldest_by_frontier: dict[FrontierKey, float] = {}
    newest_by_pool_key: dict[str, float] = {}
    unknown_claim_age_pool_keys = set()
    unknown_owned_pool_keys = set()
    oldest_unknown_claimed_at = None
    for info in existing_replica_infos:
        if info.status.value not in _UNRESOLVED_STATUS_VALUES:
            continue
        key = info.paid_capacity_pool_key
        if not isinstance(key, str):
            continue
        claimed_at = info.created_at
        normalized_claimed_at = None
        if (isinstance(claimed_at, (int, float)) and
                not isinstance(claimed_at, bool)):
            candidate_claimed_at = float(claimed_at)
            if math.isfinite(
                    candidate_claimed_at) and candidate_claimed_at >= 0:
                normalized_claimed_at = candidate_claimed_at
        if normalized_claimed_at is None:
            unknown_claim_age_pool_keys.add(key)
        else:
            newest_by_pool_key[key] = max(
                normalized_claimed_at,
                newest_by_pool_key.get(key, normalized_claimed_at))
        parsed_frontier = frontier_key_from_pool_key(key)
        if parsed_frontier is None:
            unknown_owned_pool_keys.add(key)
            if normalized_claimed_at is not None:
                oldest_unknown_claimed_at = min(
                    normalized_claimed_at,
                    oldest_unknown_claimed_at if oldest_unknown_claimed_at
                    is not None else normalized_claimed_at)
            continue
        owned_by_frontier[parsed_frontier].add(key)
        if normalized_claimed_at is not None:
            oldest_by_frontier[parsed_frontier] = min(
                normalized_claimed_at,
                oldest_by_frontier.get(parsed_frontier, normalized_claimed_at))
    configured_frontier = exploration_frontier()
    configured_max_frontier = max_service_exploration_frontier(
        workspace=workspace,
        service_name=service_name,
        service_hash=service_hash)
    cohort = None
    frontier_limit_overrides: dict[FrontierKey, int] = {}
    if paid_launch_authority is None:
        service_claim_limit = _evidence_aware_service_limit(
            paid_locations=paid_locations,
            states_by_pool_key=states,
            pool_key_by_location=keys,
            frontier_key_by_location=frontier_keys,
            owned_pool_keys_by_frontier=owned_by_frontier,
            unknown_owned_pool_keys=unknown_owned_pool_keys,
            requested_frontier_keys=requested_frontier_keys,
            floor=service_limit(),
            ceiling=max_service_limit(workspace=workspace,
                                      service_name=service_name,
                                      service_hash=service_hash),
            frontier_ceiling=configured_max_frontier)
        service_remaining = max(0, service_claim_limit - service_claims)
    else:
        try:
            claimed_units = serve_state.get_paid_capacity_plan_claimed_units(
                paid_launch_authority.service_name,
                paid_launch_authority.service_hash,
                paid_launch_authority.generation,
                paid_launch_authority.content_sha256)
            cohort = _plan_bound_admission_cohort(
                authority=paid_launch_authority,
                service_name=service_name,
                service_hash=service_hash,
                paid_locations=paid_locations,
                remaining_by_location=remaining,
                pool_key_by_location=keys,
                frontier_key_by_location=frontier_keys,
                owned_pool_keys_by_frontier=owned_by_frontier,
                unknown_owned_pool_keys=unknown_owned_pool_keys,
                requested_frontier_keys=requested_frontier_keys,
                claimed_plan_units_by_accelerator=claimed_units)
            target_width_by_frontier = {
                target.frontier_key: target.physical_backend_width
                for target in cohort.targets
            }
            filtered_remaining = {
                location: (
                    location_remaining if target_width_by_frontier.get(
                        frontier_keys[location]) == paid_pool_gpu_units(
                            keys[location]) else 0
                ) for location, location_remaining in remaining.items()
            }
        except Exception as error:  # pylint: disable=broad-except
            # The immutable plan, exact physical width, and durable debit
            # ledger must agree before even preparing provider candidates.
            logger.warning('Disabling planner-bound paid cohort: %s',
                           common_utils.format_exception(error))
            remaining = {location: 0 for location in remaining}
            service_claim_limit = max(1, service_claims)
            service_remaining = 0
        else:
            service_remaining = cohort.backend_claim_count
            service_claim_limit = max(1, service_claims + service_remaining)
            frontier_limit_overrides = cohort.frontier_limits()
            remaining = filtered_remaining
            if frontier_limit_overrides:
                configured_max_frontier = max(
                    configured_frontier, *frontier_limit_overrides.values())
    paid_gpu_units_remaining = None
    if max_live_paid_gpu_units is not None:
        if paid_gpu_attribution_complete:
            assert live_paid_gpu_units is not None
            paid_gpu_units_remaining = max(
                0, max_live_paid_gpu_units - live_paid_gpu_units)
        else:
            # Advisory reads must not guess a legacy/malformed row's node
            # cardinality. Preserve zero-cost placement, but close the paid
            # service envelope until exact attribution is repaired.
            paid_gpu_units_remaining = 0
            service_remaining = 0
    _log_admission_summary(states,
                           service_claims=service_claims,
                           service_claim_limit=service_claim_limit,
                           max_live_paid_gpu_units=max_live_paid_gpu_units,
                           live_paid_gpu_units=live_paid_gpu_units)
    return LaunchBudget(
        remaining_by_location=remaining,
        pool_key_by_location=keys,
        states_by_pool_key=states,
        globally_managed=True,
        service_remaining=service_remaining,
        service_claim_limit=service_claim_limit,
        frontier_limit=configured_frontier,
        max_frontier_limit=configured_max_frontier,
        frontier_feedback_delay_seconds=(exploration_feedback_delay_seconds()),
        frontier_key_by_location=frontier_keys,
        owned_pool_keys_by_frontier=dict(owned_by_frontier),
        unknown_owned_pool_keys=unknown_owned_pool_keys,
        oldest_claimed_at_by_frontier=oldest_by_frontier,
        oldest_unknown_claimed_at=oldest_unknown_claimed_at,
        newest_claimed_at_by_pool_key=newest_by_pool_key,
        unknown_claim_age_pool_keys=unknown_claim_age_pool_keys,
        frontier_limit_overrides=frontier_limit_overrides,
        max_live_paid_gpu_units=max_live_paid_gpu_units,
        live_paid_gpu_units=live_paid_gpu_units,
        paid_gpu_units_remaining=paid_gpu_units_remaining,
        plan_bound_cohort=cohort)


def _owned_pool_keys(budget: LaunchBudget, key: FrontierKey) -> set[str]:
    return (budget.owned_pool_keys_by_frontier.get(key, set()) |
            budget.unknown_owned_pool_keys)


def _effective_frontier_limit(budget: LaunchBudget,
                              key: FrontierKey) -> int | None:
    """Return this card's current advisory and restart-safe frontier."""
    if budget.frontier_limit is None:
        return None
    base = max(1, int(budget.frontier_limit))
    maximum = max(base, int(budget.max_frontier_limit or base))
    # A replacement controller must reuse an already-owned third pool without
    # opening a fourth.  Clamp legacy over-wide ownership to the configured
    # maximum while continuing to admit claims into those existing pools.
    restored = min(maximum, max(base, len(_owned_pool_keys(budget, key))))
    override = budget.frontier_limit_overrides.get(key, restored)
    return min(maximum, max(restored, int(override)))


def _frontier_limits_by_key(budget: LaunchBudget) -> dict[FrontierKey, int]:
    """Return effective per-card limits for atomic waiter reconciliation."""
    keys = (set(budget.owned_pool_keys_by_frontier) |
            set(budget.frontier_key_by_location.values()) |
            set(budget.frontier_limit_overrides))
    result = {}
    for key in keys:
        limit = _effective_frontier_limit(budget, key)
        if limit is not None:
            result[key] = limit
    return result


def _youngest_unresolved_claim_age_seconds(budget: LaunchBudget,
                                           key: FrontierKey) -> float | None:
    """Return the age of the newest unresolved claim in an owned cohort."""
    owned = _owned_pool_keys(budget, key)
    if not owned:
        return None
    if owned & budget.unknown_claim_age_pool_keys:
        return None
    claimed_at = []
    for pool in owned:
        timestamp = budget.newest_claimed_at_by_pool_key.get(pool)
        if timestamp is None:
            return None
        claimed_at.append(timestamp)
    return max(0.0, time.time() - max(claimed_at))


def _owned_pool_has_headroom(budget: LaunchBudget, key: FrontierKey) -> bool:
    owned = _owned_pool_keys(budget, key)
    return any(
        budget.remaining_by_location.get(location, 0) > 0 and
        budget.pool_key_by_location.get(location) in owned
        for location in budget.remaining_by_location)


def _defer_frontier(budget: LaunchBudget, key: FrontierKey) -> None:
    """Mark and log one card that cannot open another paid pool this wave."""
    if key in budget.feedback_deferred_frontiers:
        return
    budget.feedback_deferred_frontiers.add(key)
    oldest_candidates = [
        value for value in (budget.oldest_claimed_at_by_frontier.get(key),
                            budget.oldest_unknown_claimed_at)
        if value is not None
    ]
    age_text = 'unknown'
    if oldest_candidates:
        age_text = str(max(0, int(time.time() - min(oldest_candidates))))
    youngest_age = _youngest_unresolved_claim_age_seconds(budget, key)
    youngest_age_text = ('unknown' if youngest_age is None else str(
        max(0, int(youngest_age))))
    card = ','.join(key) if key else 'cpu'
    card_limit = _effective_frontier_limit(budget, key)
    logger.info(
        'Paid-capacity exploration frontier awaiting feedback: '
        f'card={card}, owned_pools={len(_owned_pool_keys(budget, key))}, '
        f'limit={card_limit}, '
        f'oldest_unresolved_claim_age_seconds={age_text}, '
        f'youngest_unresolved_claim_age_seconds={youngest_age_text}.')


def _record_selection_stop(budget: LaunchBudget) -> None:
    """Record one paid path that made no progress in this wave."""
    budget.stop_sequence += 1


def select_location(
    placer: spot_placer.SpotPlacer,
    budget: LaunchBudget,
    *,
    skip_zero_cost_preference: bool = False,
    allowed_locations: set[spot_placer.Location] | None = None,
) -> spot_placer.Location | None:
    """Select the cheapest location that still has advisory paid headroom."""
    active = [
        location for location in placer.active_locations()
        if allowed_locations is None or location in allowed_locations
    ]
    if not active:
        selection_kwargs: dict[str, Any] = {}
        if skip_zero_cost_preference:
            selection_kwargs['skip_zero_cost_preference'] = True
        if allowed_locations is not None:
            selection_kwargs['allowed_locations'] = set()
        selected = placer.select_next_location(**selection_kwargs)
        if selected is None:
            _record_selection_stop(budget)
        return selected
    zero_cost = set(placer.zero_cost_locations())
    active_paid = [location for location in active if location not in zero_cost]
    available_paid = set()
    for location in active_paid:
        if (budget.remaining_by_location.get(location, 0) <= 0 or
            (budget.service_remaining is not None and
             budget.service_remaining <= 0)):
            continue
        if budget.paid_gpu_units_remaining is not None:
            gpu_units = _budget_location_gpu_units(budget, location)
            if (gpu_units is None or
                    budget.paid_gpu_units_remaining < gpu_units):
                continue
        available_paid.add(location)
    eligible_paid = available_paid
    expansion_candidates: set[spot_placer.Location] = set()
    if budget.frontier_limit is not None:
        eligible_paid = set()
        blocked_by_frontier: dict[FrontierKey, set[spot_placer.Location]] = (
            collections.defaultdict(set))
        for location in available_paid:
            key = budget.frontier_key_by_location.get(location,
                                                      frontier_key(location))
            if key in budget.feedback_deferred_frontiers:
                continue
            pool = budget.pool_key_by_location.get(location)
            owned = _owned_pool_keys(budget, key)
            effective_frontier = _effective_frontier_limit(budget, key)
            assert effective_frontier is not None
            if pool in owned or len(owned) < effective_frontier:
                eligible_paid.add(location)
            else:
                blocked_by_frontier[key].add(location)
        for key, blocked_locations in blocked_by_frontier.items():
            if any(
                    budget.frontier_key_by_location.get(
                        location, frontier_key(location)) == key
                    for location in eligible_paid):
                continue
            current_frontier = _effective_frontier_limit(budget, key)
            assert current_frontier is not None
            maximum_frontier = max(
                current_frontier,
                int(budget.max_frontier_limit or current_frontier))
            delay = budget.frontier_feedback_delay_seconds
            youngest_age = _youngest_unresolved_claim_age_seconds(budget, key)
            can_expand = (key not in budget.frontier_limit_overrides and
                          current_frontier < maximum_frontier and
                          delay is not None and youngest_age is not None and
                          youngest_age >= delay and
                          not budget.unknown_owned_pool_keys and
                          not _owned_pool_has_headroom(budget, key))
            if can_expand:
                # The frontier is a concurrency bound, not a price policy.
                # Once every owned exact pool has exhausted its bounded
                # unresolved allowance, widen to the cheapest eligible exact
                # pool.  Provider/region diversity follows only when it is
                # actually cheaper or cheaper pools are inactive/cooling down;
                # it must not override the placer's canonical cost order.
                expansion_candidates.update(blocked_locations)
                continue
            if key not in budget.feedback_deferred_frontiers:
                _defer_frontier(budget, key)
    if skip_zero_cost_preference and active_paid and not eligible_paid:
        if expansion_candidates:
            eligible_paid = expansion_candidates
        else:
            _record_selection_stop(budget)
            return None
    else:
        eligible_paid |= expansion_candidates
    candidates = eligible_paid | {
        location for location in active if location in zero_cost
    }
    if not candidates:
        _record_selection_stop(budget)
        return None
    selected = placer.select_next_location(
        skip_zero_cost_preference=skip_zero_cost_preference,
        allowed_locations=candidates)
    if selected is None:
        _record_selection_stop(budget)
        return None
    if selected in zero_cost:
        return selected
    selected_key = budget.pool_key_by_location.get(selected)
    if selected_key in budget.priority_deferred_pool_keys:
        _record_selection_stop(budget)
        return None
    if selected in expansion_candidates:
        key = budget.frontier_key_by_location.get(selected,
                                                  frontier_key(selected))
        previous_limit = _effective_frontier_limit(budget, key)
        assert previous_limit is not None
        maximum_frontier = max(previous_limit,
                               int(budget.max_frontier_limit or previous_limit))
        expanded_limit = min(maximum_frontier, previous_limit + 1)
        budget.frontier_limit_overrides[key] = expanded_limit
        youngest_age = _youngest_unresolved_claim_age_seconds(budget, key)
        card = ','.join(key) if key else 'cpu'
        logger.info('Paid-capacity exploration frontier expanded after delayed '
                    f'feedback: card={card}, from_limit={previous_limit}, '
                    f'to_limit={expanded_limit}, '
                    'youngest_unresolved_claim_age_seconds='
                    f'{max(0, int(youngest_age or 0))}, '
                    f'candidate_cloud={str(selected.cloud).casefold()}, '
                    f'candidate_region={selected.region}.')
    return selected


def admission_snapshot_by_location(
        budget: LaunchBudget) -> dict[spot_placer.Location, dict[str, Any]]:
    """Return bounded, display-only admission state for active paid pools."""
    snapshot = {}
    frontier_details: dict[FrontierKey, tuple[int | None, int | None,
                                              int | None, set[str]]] = {}
    for location, remaining in budget.remaining_by_location.items():
        key = budget.pool_key_by_location.get(location)
        frontier = budget.frontier_key_by_location.get(location,
                                                       frontier_key(location))
        details = frontier_details.get(frontier)
        if details is None:
            effective_frontier = _effective_frontier_limit(budget, frontier)
            maximum_frontier = None
            if effective_frontier is not None:
                maximum_frontier = max(
                    effective_frontier,
                    int(budget.max_frontier_limit or effective_frontier))
            youngest_age = _youngest_unresolved_claim_age_seconds(
                budget, frontier)
            youngest_age_seconds = (None if youngest_age is None else max(
                0, int(youngest_age)))
            owned_pool_keys = _owned_pool_keys(budget, frontier)
            details = (effective_frontier, maximum_frontier,
                       youngest_age_seconds, owned_pool_keys)
            frontier_details[frontier] = details
        (effective_frontier, maximum_frontier, youngest_age_seconds,
         owned_pool_keys) = details
        state = budget.states_by_pool_key.get(key,
                                              {}) if key is not None else {}
        raw_state = str(state.get('admission_state', 'active'))
        if raw_state == 'active':
            admission_state = 'open' if remaining > 0 else 'saturated'
        elif raw_state in ('cooldown', 'probe'):
            admission_state = raw_state
        else:
            admission_state = 'open' if remaining > 0 else 'saturated'
        snapshot[location] = {
            'state': admission_state,
            'pool_remaining': max(0, int(remaining)),
            'service_remaining': budget.service_remaining,
            'cooldown_until': state.get('cooldown_until'),
            'frontier_limit': effective_frontier,
            'frontier_max_limit': maximum_frontier,
            'frontier_owned': key is not None and key in owned_pool_keys,
            'frontier_owned_pool_count': len(owned_pool_keys),
            'youngest_unresolved_claim_age_seconds': youngest_age_seconds,
        }
    return snapshot


def defer_for_priority(budget: LaunchBudget | None,
                       location: spot_placer.Location | None) -> None:
    """Stop this wave at a priority-deferred pool without enabling spill."""
    if budget is None or location is None:
        return
    key = budget.pool_key_by_location.get(location)
    if key is not None:
        budget.priority_deferred_pool_keys.add(key)
        _record_selection_stop(budget)


def defer_for_feedback(budget: LaunchBudget | None,
                       location: spot_placer.Location | None) -> None:
    """Stop this wave from opening another pool for one accelerator card."""
    if budget is None or location is None:
        return
    key = budget.frontier_key_by_location.get(location, frontier_key(location))
    _defer_frontier(budget, key)
    _record_selection_stop(budget)


def debit(budget: LaunchBudget | None,
          location: spot_placer.Location | None) -> None:
    """Debit a claim accepted after the advisory snapshot was read."""
    if budget is None or location not in budget.remaining_by_location:
        return
    key = budget.pool_key_by_location.get(location)
    aliases = [
        candidate for candidate in budget.remaining_by_location
        if candidate == location or
        (key is not None and budget.pool_key_by_location.get(candidate) == key)
    ]
    for candidate in aliases:
        remaining = budget.remaining_by_location[candidate]
        if remaining > 0:
            budget.remaining_by_location[candidate] = remaining - 1
    if budget.service_remaining is not None and budget.service_remaining > 0:
        budget.service_remaining -= 1
    if (budget.paid_gpu_units_remaining is not None and
            budget.paid_gpu_units_remaining > 0):
        gpu_units = _budget_location_gpu_units(budget, location)
        budget.paid_gpu_units_remaining = (0 if gpu_units is None else max(
            0, budget.paid_gpu_units_remaining - gpu_units))
    if budget.frontier_limit is not None and key is not None:
        frontier = budget.frontier_key_by_location.get(location,
                                                       frontier_key(location))
        budget.owned_pool_keys_by_frontier.setdefault(frontier, set()).add(key)
        claimed_at = time.time()
        budget.newest_claimed_at_by_pool_key[key] = max(
            claimed_at,
            budget.newest_claimed_at_by_pool_key.get(key, claimed_at))
        budget.oldest_claimed_at_by_frontier.setdefault(frontier, claimed_at)


def exhaust(budget: LaunchBudget | None,
            location: spot_placer.Location | None) -> None:
    """Stop this wave from repeatedly racing a saturated exact pool."""
    if budget is None or location not in budget.remaining_by_location:
        return
    key = budget.pool_key_by_location.get(location)
    for candidate in budget.remaining_by_location:
        if (candidate == location or
            (key is not None and
             budget.pool_key_by_location.get(candidate) == key)):
            budget.remaining_by_location[candidate] = 0


def exhaust_service(budget: LaunchBudget | None) -> None:
    """Stop a wave after the authoritative per-service envelope is full."""
    if budget is not None:
        exhausted = False
        if budget.service_remaining is not None:
            budget.service_remaining = 0
            exhausted = True
        if budget.paid_gpu_units_remaining is not None:
            budget.paid_gpu_units_remaining = 0
            exhausted = True
        if exhausted:
            _record_selection_stop(budget)


def service_exhausted(budget: LaunchBudget | None) -> bool:
    """Whether fresh paid placement has no service-envelope headroom."""
    return (budget is not None and
            ((budget.service_remaining is not None and
              budget.service_remaining <= 0) or
             (budget.paid_gpu_units_remaining is not None and
              budget.paid_gpu_units_remaining <= 0)))


def try_persist_claim_batch(
    *,
    service_name: str,
    service_hash: str | None,
    controller_owner: tuple[int | None, str | None] | None,
    candidates: Sequence[PaidClaimCandidate],
    budget: LaunchBudget,
) -> PaidClaimBatchResult:
    """Atomically persist one ordered subset of paid replica claims."""
    candidates = tuple(candidates)
    if not candidates:
        return PaidClaimBatchResult(())

    identities = []
    for candidate in candidates:
        if (type(candidate.priority) is not int or  # pylint: disable=unidiomatic-typecheck
                not constants.LB_REQUEST_PRIORITY_MIN <= candidate.priority <=
                constants.LB_REQUEST_PRIORITY_MAX):
            raise ValueError('Paid claim priority must be exact and in range.')
        identities.append(
            (candidate.replica_id, candidate.replica_info.replica_record_id))
    if len(set(identities)) != len(identities):
        raise ValueError('Paid claim batch candidate identities must be '
                         'unique.')
    if len({replica_id for replica_id, _ in identities}) != len(identities):
        raise ValueError('Paid claim batch replica IDs must be unique.')
    if len({record_id for _, record_id in identities}) != len(identities):
        raise ValueError('Paid claim batch record identities must be unique.')

    if not budget.globally_managed or service_hash is None:
        has_capacity_plan_claim = any(candidate.capacity_plan_claim is not None
                                      for candidate in candidates)
        result = (ClaimResult.SERVICE_SATURATED
                  if budget.max_live_paid_gpu_units is not None or
                  has_capacity_plan_claim else ClaimResult.LEGACY_LOCAL)
        return PaidClaimBatchResult(
            tuple(
                PaidClaimBatchMemberResult(replica_id, replica_record_id,
                                           result)
                for replica_id, replica_record_id in identities))

    persistence_specs = []
    for candidate in candidates:
        try:
            key = budget.pool_key_by_location[candidate.location]
        except KeyError as error:
            raise ValueError('Paid claim candidate location is absent from '
                             'its frozen launch budget.') from error
        candidate_frontier = budget.frontier_key_by_location.get(
            candidate.location, frontier_key(candidate.location))
        effective_frontier = _effective_frontier_limit(budget,
                                                       candidate_frontier)
        if effective_frontier is None:
            effective_frontier = exploration_frontier()
        persistence_specs.append(
            PaidClaimPersistenceSpec(candidate=candidate,
                                     pool_key=key,
                                     frontier_key=candidate_frontier,
                                     frontier_limit=effective_frontier))

    results = serve_state.try_add_replicas_with_paid_capacity_claims(
        service_name,
        service_hash,
        persistence_specs,
        base_limit=base_limit(),
        max_limit=max_limit(),
        service_limit=(budget.service_claim_limit if budget.service_claim_limit
                       is not None else service_limit()),
        max_live_paid_gpu_units=budget.max_live_paid_gpu_units,
        now=None,
        success_ttl_seconds=success_ttl_seconds(),
        failure_cooldown_seconds=failure_cooldown_seconds(),
        waiter_ttl_seconds=waiter_ttl_seconds(),
        frontier_default_limit=(budget.frontier_limit if budget.frontier_limit
                                is not None else exploration_frontier()),
        frontier_limits_by_key=_frontier_limits_by_key(budget),
        expected_controller_owner=controller_owner,
    )
    members = []
    for spec, result_value in zip(persistence_specs, results, strict=True):
        result = ClaimResult(result_value)
        candidate = spec.candidate
        if result is ClaimResult.ACQUIRED:
            # Publish caller-visible provenance only after the whole batch
            # transaction is durable.
            candidate.replica_info.paid_capacity_pool_key = spec.pool_key
        members.append(
            PaidClaimBatchMemberResult(
                replica_id=candidate.replica_id,
                replica_record_id=candidate.replica_info.replica_record_id,
                claim_result=result))
    return PaidClaimBatchResult(tuple(members))


def try_persist_claim(
    *,
    service_name: str,
    service_hash: str | None,
    controller_owner: tuple[int | None, str | None] | None,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    location: spot_placer.Location,
    budget: LaunchBudget,
    priority: int,
    capacity_plan_claim: Mapping[str, Any] | None = None,
) -> ClaimResult:
    """Persist one claim through the canonical paid batch transaction."""
    candidate = PaidClaimCandidate(replica_id=replica_id,
                                   replica_info=replica_info,
                                   location=location,
                                   priority=priority,
                                   capacity_plan_claim=capacity_plan_claim)
    return try_persist_claim_batch(service_name=service_name,
                                   service_hash=service_hash,
                                   controller_owner=controller_owner,
                                   candidates=(candidate,),
                                   budget=budget).members[0].claim_result


def adopt_existing_claims(
    *,
    service_name: str,
    service_hash: str | None,
    controller_owner: tuple[int | None, str | None] | None,
    workspace: str,
    placer: spot_placer.SpotPlacer | None,
    replica_infos: list['replica_managers.ReplicaInfo'],
    priority: int,
) -> bool:
    """Adopt unresolved legacy rows before recovery re-drives their launches."""
    if service_hash is None or not central_authority_available():
        return True
    # Recovery starts before the controller HTTP port binds. The centralized
    # version catalog is already complete, so this cannot resolve providers.
    zero_cost = set(
        placer.zero_cost_locations()) if placer is not None else set()
    claims = []
    for info in replica_infos:
        if (info.status.value not in _UNRESOLVED_STATUS_VALUES or
                info.reserved_fill or info.is_zero_cost is True or
                info.cost_rebalance_for_replica_id is not None):
            continue
        existing_key = info.paid_capacity_pool_key
        if isinstance(existing_key, str):
            claims.append((info.replica_id, existing_key, priority, info))
            continue
        if placer is None:
            continue
        replica_location = info.get_spot_location()
        if replica_location is None:
            continue
        location = placer.resolve_location(replica_location)
        if location is None or location in zero_cost:
            continue
        if isinstance(location.cloud, clouds.AWS):
            # This row existed before it had a durable paid-pool identity.
            # Never infer its provider account from ambient restart
            # credentials: the unresolved launch may have effected a
            # different account.  Retain an account-unscoped v1 claim for
            # conservative settlement; cohort-11 AWS provider effects require
            # v2 and therefore fail closed.
            key = _legacy_pool_key(location,
                                   workspace=workspace,
                                   num_nodes=placer.num_nodes)
        else:
            key = pool_key(location,
                           workspace=workspace,
                           num_nodes=placer.num_nodes)
        claims.append((info.replica_id, key, priority, info))
    return serve_state.adopt_paid_capacity_claims(
        service_name,
        service_hash,
        claims,
        base_limit=base_limit(),
        now=None,
        expected_controller_owner=controller_owner)


def persist_completed_launches(
    *,
    service_name: str,
    service_hash: str | None,
    controller_owner: tuple[int | None, str | None] | None,
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    outcomes: dict[int, LaunchOutcome],
) -> CompletedLaunchPersistence | None:
    """Persist completed rows and feed claimed outcomes into the ramp."""
    if service_hash is None or not central_authority_available():
        return None
    applied_pool_keys: set[str] = set()
    ownership_valid = (
        serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            service_name,
            service_hash,
            replica_infos,
            outcomes,
            base_limit=base_limit(),
            max_limit=max_limit(),
            now=None,
            success_ttl_seconds=success_ttl_seconds(),
            failure_cooldown_seconds=failure_cooldown_seconds(),
            expected_controller_owner=controller_owner,
            applied_outcome_pool_keys=applied_pool_keys))
    return CompletedLaunchPersistence(
        ownership_valid=ownership_valid,
        applied_pool_keys=frozenset(applied_pool_keys))
