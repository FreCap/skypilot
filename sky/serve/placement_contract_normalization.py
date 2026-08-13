"""Raw persisted-placement contract normalization primitives.

The database operator built on this module is intentionally separate from
ordinary SkyServiceSpec unpickling.  These helpers capture the exact top-level
state without running compatibility hooks, prove that the source pickle is
reproducible byte-for-byte, and permit only the documented placement-key
delta.
"""

import argparse
import collections
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
import contextlib
import copyreg
import dataclasses
import datetime
import enum
import hashlib
import io
import json
import math
import os
import pickle
import time
from typing import Any, TYPE_CHECKING
import uuid

import psutil
import sqlalchemy
from sqlalchemy import orm

import sky
from sky.adaptors import common as adaptors_common
from sky.container_images import demand_state
from sky.serve import ephemeral_storage_contract
from sky.serve import maintenance
from sky.serve import placement_normalization_authority
from sky.serve import placement_normalization_identity
from sky.serve import placement_normalization_manifest
from sky.serve import placement_policy
from sky.serve import resource_action_state_schema
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import service_spec
from sky.server import constants as server_constants
from sky.server.requests import postgres_schema as request_postgres_schema
from sky.server.requests import request_names
from sky.server.requests import requests as requests_lib
from sky.utils import common as sky_common
from sky.utils.db import db_utils

if TYPE_CHECKING:
    from sky.serve import resource_actions as serve_resource_actions_types

serve_resource_actions = adaptors_common.LazyImport(
    'sky.serve.resource_actions')
kernel_resource_actions = adaptors_common.LazyImport(
    'sky.server.requests.resource_actions')
request_postgres = adaptors_common.LazyImport('sky.server.requests.postgres')
global_user_state = adaptors_common.LazyImport('sky.global_user_state')

_SPEC_MODULE = 'sky.serve.service_spec'
_SPEC_NAME = 'SkyServiceSpec'
_SUPPORTED_PROTOCOLS = frozenset({4, 5})
_SCHEMA_REVISION = '037'
_ADVISORY_LOCK_NAME = 'skyserve-placement-contract-normalization-v1'
_LOCK_TIMEOUT_MS = 5000
_STATEMENT_TIMEOUT_MS = 60000
_MAX_INVENTORY_ROWS = placement_normalization_manifest.MAX_INVENTORY_ROWS
_MAX_ACTIVE_SERVE_REQUEST_EVIDENCE_ROWS = 10000
_MAX_PROCESS_EVIDENCE_ROWS = 10000
_MAX_RESOURCE_ACTION_EVIDENCE_ROWS = 10000
_MAX_LEGACY_CONTROLLER_EVIDENCE_ROWS = 10000
_PROCESS_EVIDENCE_SCHEMA = 'skyserve-process-quiescence-v1'
_RESOURCE_ACTION_EVIDENCE_SCHEMA = 'skyserve-resource-action-roots-v1'
_API_POD_EVIDENCE_SCHEMA = 'skyserve-sole-api-pod-v1'
_LEGACY_CONTROLLER_EVIDENCE_SCHEMA = (
    'skyserve-legacy-controller-cluster-absence-v2')
_CLEANUP_PROOF_SCHEMA_V2 = 'skyserve-ephemeral-storage-retirement-v2'
_CLEANUP_PROOF_SCHEMA = 'skyserve-ephemeral-storage-retirement-v3'
_CLEANUP_CREATED_AT_MODE_FINITE = 'finite'
_CLEANUP_CREATED_AT_MODE_LEGACY_PREFIX_NULL = 'legacy_prefix_null'
_CLEANUP_PROTOCOL_V3_ONLY_FIELDS = frozenset({
    'cleanup_version_timestamp_service_count',
    'cleanup_version_timestamp_inventory_count',
    'cleanup_version_timestamp_matched_intent_count',
    'cleanup_legacy_null_version_timestamp_count',
    'cleanup_timestamp_boundary_count',
    'cleanup_version_timestamp_inventory_sha256',
    'cleanup_candidate_version_created_at_mode',
    'cleanup_candidate_legacy_timestamp_boundary_version',
    'cleanup_timestamp_proof_sha256',
})
_STALE_PLACEHOLDER_PROOF_SCHEMA = (
    placement_normalization_manifest.STALE_PLACEHOLDER_PROOF_SCHEMA)
_STALE_PLACEHOLDER_EVIDENCE_FACT = (
    placement_normalization_manifest.STALE_PLACEHOLDER_EVIDENCE_FACT)
_SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT = (
    placement_normalization_manifest.SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT)
_STALE_PLACEHOLDER_EVIDENCE_FIELDS = (
    placement_normalization_manifest.STALE_PLACEHOLDER_EVIDENCE_FIELDS)
_SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FIELDS = (
    placement_normalization_manifest.SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FIELDS
)
_STALE_PLACEHOLDER_NULL_COLUMNS = (
    placement_normalization_manifest.STALE_PLACEHOLDER_NULL_COLUMNS)
_PREDECESSOR_RECEIPT_SCHEMA = 'skyserve-predecessor-receipt-inventory-v1'
# This operator authenticates and runs against exactly Serve040.  Keep its
# service projection frozen at that revision so later canonical metadata
# columns are never emitted into SQL against the historical schema.
_REVISION_040_SERVICE_COLUMN_NAMES = (
    'name',
    'workspace',
    'controller_job_id',
    'controller_port',
    'load_balancer_port',
    'status',
    'uptime',
    'policy',
    'auto_restart',
    'requested_resources',
    'requested_resources_str',
    'current_version',
    'active_versions',
    'load_balancing_policy',
    'tls_encrypted',
    'pool',
    'controller_pid',
    'hash',
    'resource_action_mode',
    'resource_action_mode_changed_at',
    'resource_action_candidate_epoch',
    'resource_action_candidate_policy_sha256',
    'resource_action_candidate_binding_sha256',
    'lifecycle_epoch',
    'resource_scope',
    'entrypoint',
    'controller_ip',
    'placement_normalization_requested_run_id',
    'placement_normalization_loaded_run_id',
    'placement_normalization_loaded_image_commit',
    'placement_normalization_loaded_controller_pid',
    'placement_normalization_loaded_controller_ip',
    'placement_normalization_loaded_boot_id',
    'placement_normalization_loaded_at',
    'logical_replica_semantics',
    'lb_ha_enabled',
    'lb_active_slot',
    'lb_cutover_generation',
    'lb_pending_slot',
    'lb_cutover_phase',
    'lb_drain_started_at',
    'lb_demand_handoff_generation',
    'lb_demand_handoff_snapshot',
    'lb_demand_handoff_complete_at',
    'lb_last_demand_snapshot',
    'spot_placement_state',
    'cost_rebalance_state',
)
_POD_UID_ENV_VAR = 'SKYPILOT_POD_UID'
_ROLLING_UPDATE_ENV_VAR = 'SKYPILOT_ROLLING_UPDATE_ENABLED'
_RETIREMENT_REASON = (
    'transition-only physical/per-GPU contract retired after locked '
    'dependency proof')
_RETIRED_SPEC_BYTES = pickle.dumps(None, protocol=4)
_ALLOWED_LEGACY_DELTA_FIELDS = frozenset({
    placement_policy.POLICY_NAME_FIELD,
    placement_policy.POOL_FIELD,
    placement_policy.ROLLBACK_REPLICA_UNIT_FIELD,
    *placement_policy.CONTRACT_FIELDS,
})


class Classification(enum.Enum):
    """Exhaustive raw-pickle classification used by the operator ledger."""

    PLACEHOLDER = 'placeholder'
    EXPLICIT_V1 = 'explicit_v1'
    EXPLICIT_V2 = 'explicit_v2'
    FIELDLESS_SUPPORTED = 'fieldless_supported'
    HISTORICAL_PHYSICAL_PER_GPU = 'historical_physical_per_gpu'
    RETIRED = 'retired'
    BLOCKER = 'blocker'


_LOADABLE_RESULT_OUTCOMES = frozenset({
    (Classification.FIELDLESS_SUPPORTED, 'changed'),
    (Classification.EXPLICIT_V1, 'changed'),
    (Classification.EXPLICIT_V2, 'unchanged'),
})
_SUPPORTED_NORMALIZATION_SOURCES = frozenset({
    Classification.FIELDLESS_SUPPORTED,
    Classification.EXPLICIT_V1,
})


def _revision_040_service_columns() -> tuple[sqlalchemy.Column[Any], ...]:
    """Return the canonical service projection owned by Serve040."""
    return tuple(serve_state.services_table.c[name]
                 for name in _REVISION_040_SERVICE_COLUMN_NAMES)


class ApplyMode(enum.Enum):
    """Explicit write phases; absence means dry-run."""

    SUPPORTED = 'apply_supported'
    RETIRE_TERMINAL_HISTORICAL = 'retire_terminal_historical'


class NormalizationBlocker(ValueError):
    """A persisted payload cannot be normalized without guessing."""


@dataclasses.dataclass(frozen=True)
class RawSpecAnalysis:
    """Bounded analysis result; raw bytes are retained only in process."""

    classification: Classification
    source_sha256: str
    result_sha256: str | None
    source_protocol: int | None
    changed: bool
    contract_projection: dict[str, Any] | None
    blocker_reason: str | None = None
    result_bytes: bytes | None = dataclasses.field(default=None, repr=False)

    @property
    def blocked(self) -> bool:
        return self.classification is Classification.BLOCKER


@dataclasses.dataclass(frozen=True)
class OperatorResult:
    """Secret-free result emitted by the operator CLI."""

    mode: str
    dry_run: bool
    row_count: int
    classification_counts: dict[str, int]
    pre_inventory_sha256: str
    post_inventory_sha256: str
    changed_rows: int
    retired_rows: int
    blockers: tuple[dict[str, Any], ...]
    run_id: str | None
    prior_ledger_mismatches: tuple[dict[str, Any], ...]

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


class _RawSpecCapture:
    """Proxy used only while bypassing SkyServiceSpec.__setstate__."""

    def __init__(self, captures: list['_RawSpecCapture']) -> None:
        self._captures = captures
        self.state: dict[str, Any] | None = None
        captures.append(self)

    def __setstate__(self, state: Any) -> None:
        if not isinstance(state, dict):
            raise NormalizationBlocker(
                'SkyServiceSpec top-level pickle state is not a mapping.')
        self.state = state


def _capture_class(captures: list[_RawSpecCapture]) -> type[_RawSpecCapture]:
    """Build a per-load capture class so nested captures are countable."""

    class Capture(_RawSpecCapture):

        def __new__(cls) -> 'Capture':
            instance = super().__new__(cls)
            _RawSpecCapture.__init__(instance, captures)
            return instance

        def __init__(self) -> None:  # pylint: disable=super-init-not-called
            # NEWOBJ bypasses __init__; initialization happens in __new__.
            pass

    return Capture


class _RawSpecUnpickler(pickle.Unpickler):
    """Replace only the exact persisted SkyServiceSpec qualified class."""

    def __init__(self, stream: io.BytesIO,
                 captures: list[_RawSpecCapture]) -> None:
        super().__init__(stream)
        self._capture_type = _capture_class(captures)

    def find_class(self, module: str, name: str) -> Any:
        if (module, name) == (_SPEC_MODULE, _SPEC_NAME):
            return self._capture_type
        return super().find_class(module, name)


class _RawStatePickler(pickle.Pickler):
    """Serialize a supplied raw __dict__ without calling __getstate__."""

    def reducer_override(self, obj: Any) -> Any:
        if isinstance(obj, service_spec.SkyServiceSpec):
            return (
                copyreg.__newobj__,  # type: ignore[attr-defined]
                (service_spec.SkyServiceSpec,),
                obj.__dict__)
        return NotImplemented


def _source_protocol(payload: bytes) -> int:
    if len(payload) < 2 or payload[0] != pickle.PROTO[0]:
        raise NormalizationBlocker(
            'Persisted SkyServiceSpec must use an explicit pickle protocol.')
    protocol = payload[1]
    if protocol not in _SUPPORTED_PROTOCOLS:
        raise NormalizationBlocker(
            f'Unsupported persisted pickle protocol: {protocol!r}.')
    return protocol


def _capture_payload(
    payload: bytes,) -> tuple[dict[str, Any] | None, int, bool]:
    protocol = _source_protocol(payload)
    stream = io.BytesIO(payload)
    captures: list[_RawSpecCapture] = []
    try:
        root = _RawSpecUnpickler(stream, captures).load()
    except NormalizationBlocker:
        raise
    except Exception as exc:
        raise NormalizationBlocker(
            f'Persisted payload cannot be decoded: {type(exc).__name__}.') \
            from exc
    if stream.read(1):
        raise NormalizationBlocker('Persisted pickle has trailing bytes.')
    if root is None and not captures:
        return None, protocol, True
    if len(captures) != 1 or root is not captures[0]:
        raise NormalizationBlocker(
            'Persisted payload must contain exactly one top-level '
            'SkyServiceSpec.')
    state = captures[0].state
    if state is None:
        raise NormalizationBlocker('SkyServiceSpec pickle has no state.')
    return state, protocol, False


def _serialize_raw_state(state: dict[str, Any], protocol: int) -> bytes:
    raw_spec = service_spec.SkyServiceSpec.__new__(service_spec.SkyServiceSpec)
    raw_spec.__dict__ = state
    stream = io.BytesIO()
    _RawStatePickler(stream, protocol=protocol).dump(raw_spec)
    return stream.getvalue()


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _contract_projection(state: dict[str, Any],
                         contract: placement_policy.PlacementContract,
                         version: int | None) -> dict[str, Any]:
    return {
        'version': version,
        'policy': state.get(placement_policy.POLICY_NAME_FIELD),
        'engine': contract.engine,
        'replica_unit': contract.replica_unit,
        'catalog_mode': contract.catalog_mode,
        'cost_unit': contract.cost_unit,
        'reserved_fill_mode': contract.reserved_fill_mode,
        'workload_kind': contract.workload_kind,
        'rollback_uses_logical_replicas': state.get(
            placement_policy.ROLLBACK_REPLICA_UNIT_FIELD),
    }


def _unaffected_projection(state: dict[str, Any]) -> bytes:
    unaffected = tuple((key, value)
                       for key, value in state.items()
                       if key not in _ALLOWED_LEGACY_DELTA_FIELDS)
    try:
        return pickle.dumps(unaffected, protocol=4)
    except Exception as exc:
        raise NormalizationBlocker(
            'Unrelated raw state is not deterministically projectable: '
            f'{type(exc).__name__}.') from exc


def _analyze(payload: bytes) -> RawSpecAnalysis:
    source_digest = _sha256(payload)
    state, protocol, placeholder = _capture_payload(payload)
    if placeholder:
        return RawSpecAnalysis(Classification.PLACEHOLDER,
                               source_digest,
                               source_digest,
                               protocol,
                               False,
                               None,
                               result_bytes=payload)
    assert state is not None

    reproduced = _serialize_raw_state(state, protocol)
    if reproduced != payload:
        raise NormalizationBlocker(
            'Raw source projection does not reproduce the source bytes.')

    try:
        contract, version = placement_policy.decode_contract_state(state)
    except (TypeError, ValueError) as exc:
        raise NormalizationBlocker(str(exc)) from exc
    projection = _contract_projection(state, contract, version)
    if version == placement_policy.PLACEMENT_CONTRACT_VERSION_V2:
        return RawSpecAnalysis(Classification.EXPLICIT_V2,
                               source_digest,
                               source_digest,
                               protocol,
                               False,
                               projection,
                               result_bytes=payload)
    if contract.is_legacy_physical_per_gpu:
        return RawSpecAnalysis(Classification.HISTORICAL_PHYSICAL_PER_GPU,
                               source_digest,
                               source_digest,
                               protocol,
                               False,
                               projection,
                               result_bytes=payload)

    if version == placement_policy.PLACEMENT_CONTRACT_VERSION_V1:
        source_classification = Classification.EXPLICIT_V1
    else:
        assert version is None
        source_classification = Classification.FIELDLESS_SUPPORTED

    before_unaffected = _unaffected_projection(state)
    normalized_state = dict(state)
    if version is None:
        materialized, materialized_version = (
            service_spec.materialize_legacy_placement_contract_state(
                normalized_state))
        if materialized_version is not None or materialized != contract:
            raise NormalizationBlocker(
                'Legacy materializer did not preserve the decoded contract.')
    normalized_state.update(contract.persisted_fields())
    normalized_state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)

    unexpected_key_delta = (set(state) ^ set(normalized_state)) - (
        _ALLOWED_LEGACY_DELTA_FIELDS)
    if unexpected_key_delta:
        raise NormalizationBlocker(
            'Legacy materialization added or removed unrelated top-level '
            'state.')
    if _unaffected_projection(normalized_state) != before_unaffected:
        raise NormalizationBlocker(
            'Legacy materialization changed unrelated ordered state.')

    result = _serialize_raw_state(normalized_state, protocol=4)
    result_state, result_protocol, result_placeholder = _capture_payload(result)
    if result_placeholder or result_protocol != 4 or result_state is None:
        raise NormalizationBlocker(
            'Normalized result is not a protocol-4 SkyServiceSpec.')
    if _serialize_raw_state(result_state, 4) != result:
        raise NormalizationBlocker(
            'Normalized result is not byte-for-byte reproducible.')
    if _unaffected_projection(result_state) != before_unaffected:
        raise NormalizationBlocker(
            'Normalized result changed unrelated ordered state.')
    result_contract, result_version = (
        placement_policy.decode_contract_state(result_state))
    if (result_contract != contract or
            result_version != placement_policy.PLACEMENT_CONTRACT_VERSION_V2):
        raise NormalizationBlocker(
            'Normalized result does not encode the expected v2 contract.')
    return RawSpecAnalysis(source_classification,
                           source_digest,
                           _sha256(result),
                           protocol,
                           True,
                           _contract_projection(result_state, result_contract,
                                                result_version),
                           result_bytes=result)


def analyze_spec_pickle(payload: Any) -> RawSpecAnalysis:
    """Classify one persisted payload without throwing on corrupt input."""
    if isinstance(payload, memoryview):
        payload = payload.tobytes()
    elif isinstance(payload, bytearray):
        payload = bytes(payload)
    if not isinstance(payload, bytes):
        return RawSpecAnalysis(
            Classification.BLOCKER,
            _sha256(b''),
            None,
            None,
            False,
            None,
            blocker_reason='Persisted spec payload is not bytes.')
    try:
        return _analyze(payload)
    except NormalizationBlocker as exc:
        return RawSpecAnalysis(Classification.BLOCKER,
                               _sha256(payload),
                               None,
                               None,
                               False,
                               None,
                               blocker_reason=str(exc))


@dataclasses.dataclass
class _RowWork:
    """One captured version row and its planned exact postimage."""

    original: dict[str, Any]
    result: dict[str, Any]
    analysis: RawSpecAnalysis
    classification: Classification
    outcome: str = 'unchanged'
    dependency_facts: dict[str, Any] = dataclasses.field(default_factory=dict)
    manifest_classification: (
        placement_normalization_manifest.ManifestClassification | None) = None

    @property
    def identity(self) -> tuple[str, int]:
        return (str(self.original['service_name']),
                int(self.original['version']))

    @property
    def ledger_classification(self) -> str:
        """Return the contextual durable classification for this run."""
        if self.manifest_classification is not None:
            return self.manifest_classification.value
        return self.classification.value


@dataclasses.dataclass
class _CleanupVersionTimestampProof:
    """One live version's explicit intent timestamp comparison contract."""

    mode: str
    created_at: float | None
    boundary_version: int | None
    boundary_created_at: float | None

    @property
    def intent_created_at_upper_bound(self) -> float:
        if self.mode == _CLEANUP_CREATED_AT_MODE_FINITE:
            assert self.created_at is not None
            return self.created_at
        assert self.mode == _CLEANUP_CREATED_AT_MODE_LEGACY_PREFIX_NULL
        assert self.boundary_created_at is not None
        return self.boundary_created_at


@dataclasses.dataclass
class _CleanupIntentWork:
    """One exact cleanup-intent preimage and its planned postimage."""

    original: dict[str, Any]
    result: dict[str, Any]
    matched_version: tuple[str, int]
    projection_sha256: str
    matched_version_timestamp: _CleanupVersionTimestampProof

    @property
    def identity(self) -> tuple[str, str, str]:
        return (self.original['service_name'], self.original['resource_scope'],
                self.original['storage_generation'])

    @property
    def adopted(self) -> bool:
        return self.original['provisional'] == 1


@dataclasses.dataclass(frozen=True)
class _CleanupIntentPlan:
    """Complete validated cleanup-intent repair plan for one transaction."""

    rows: tuple[_CleanupIntentWork, ...]
    candidate_facts: Mapping[tuple[str, int], Mapping[str, Any]]
    global_facts: Mapping[str, Any]


@dataclasses.dataclass(frozen=True)
class _PredecessorReceiptEvidence:
    """Typed completed-receipt inventory that may be atomically replaced."""

    service_names: frozenset[str]
    facts: Mapping[str, Any]


@dataclasses.dataclass(frozen=True)
class _ExternalEvidence:
    """Bounded count and canonical digest from an external authority."""

    count: int
    digest: str


@dataclasses.dataclass(frozen=True)
class _ApiPodIdentity:
    """Canonical identity of the sole fresh API registry member."""

    pod_uid: str
    instance_id: uuid.UUID
    digest: str


@dataclasses.dataclass(frozen=True, order=True)
class _ProcessTarget:
    """Exact live parent identity whose absence retirement must prove."""

    service_name: str
    service_hash: str
    lifecycle_epoch: int


@dataclasses.dataclass(frozen=True, order=True)
class _ResourceActionTarget:
    """Historical version identity for retained resource-action evidence."""

    service_name: str
    version: int
    service_hash: str


@dataclasses.dataclass(frozen=True, order=True)
class _ProcessFact:
    """Nonsecret same-pod process identity included in quiescence evidence."""

    pid: int
    ppid: int
    create_time: float
    status: str
    kind: str
    service_name: str
    service_hash: str


def _as_bytes(value: Any) -> bytes:
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, bytes):
        return value
    raise NormalizationBlocker('Persisted spec payload is not bytes.')


def _json_default(value: Any) -> str:
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(f'Unsupported canonical inventory value: {type(value)!r}.')


def _value_sha256(value: Any) -> str:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytes):
        payload = b'bytes\0' + value
    else:
        payload = b'json\0' + json.dumps(
            value,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
            default=_json_default,
        ).encode()
    return _sha256(payload)


def _column_sha256s(row: Mapping[str, Any]) -> dict[str, str]:
    return {
        str(column): _value_sha256(value)
        for column, value in sorted(row.items())
    }


def _row_sha256(row: Mapping[str, Any]) -> str:
    encoded = json.dumps(_column_sha256s(row),
                         sort_keys=True,
                         separators=(',', ':')).encode()
    return _sha256(encoded)


def _canonical_json_sha256(value: Any) -> str:
    return _sha256(
        json.dumps(value,
                   sort_keys=True,
                   separators=(',', ':'),
                   default=_json_default).encode())


def _cleanup_intent_inventory_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    inventory = [
        (row['service_name'], row['resource_scope'], row['storage_generation'],
         _row_sha256(row))
        for row in sorted(rows,
                          key=lambda item: (item['service_name'], item[
                              'resource_scope'], item['storage_generation']))
    ]
    return _canonical_json_sha256(inventory)


def _cleanup_intent_fleet_sha256(rows: Sequence[_CleanupIntentWork], *,
                                 result: bool) -> str:
    projected_rows = [work.result if result else work.original for work in rows]
    return _cleanup_intent_inventory_sha256(projected_rows)


def _fleet_sha256(rows: list[_RowWork], *, result: bool) -> str:
    inventory = [(work.identity[0], work.identity[1],
                  _row_sha256(work.result if result else work.original))
                 for work in sorted(rows, key=lambda item: item.identity)]
    return _sha256(json.dumps(inventory, separators=(',', ':')).encode())


def _blocking_analysis(payload: bytes,
                       reason: str,
                       protocol: int | None = None) -> RawSpecAnalysis:
    return RawSpecAnalysis(Classification.BLOCKER,
                           _sha256(payload),
                           None,
                           protocol,
                           False,
                           None,
                           blocker_reason=reason)


def _classify_version_row(
        row: dict[str, Any]) -> tuple[RawSpecAnalysis, Classification]:
    raw_payload = row.get('spec')
    analysis = analyze_spec_pickle(raw_payload)
    if analysis.blocked and not isinstance(raw_payload,
                                           (bytes, bytearray, memoryview)):
        return analysis, Classification.BLOCKER
    payload = _as_bytes(raw_payload)
    retirement_values = (
        row.get('retired_at'),
        row.get('retired_yaml_content'),
        row.get('retirement_reason'),
        row.get('retirement_run_id'),
    )
    retired = row.get('retired_at') is not None
    if retired:
        retired_at = row.get('retired_at')
        complete = (row.get('yaml_content') is None and
                    isinstance(row.get('retired_yaml_content'), str) and
                    row.get('retirement_reason') == _RETIREMENT_REASON and
                    isinstance(row.get('retirement_run_id'), uuid.UUID) and
                    isinstance(retired_at, (int, float)) and
                    not isinstance(retired_at, bool) and
                    math.isfinite(retired_at) and retired_at >= 0 and
                    payload == _RETIRED_SPEC_BYTES)
        if (not complete or
                analysis.classification is not Classification.PLACEHOLDER):
            blocked = _blocking_analysis(
                payload, 'Retired version row is not the exact complete '
                'protocol-4 retirement representation.',
                analysis.source_protocol)
            return blocked, Classification.BLOCKER
        return analysis, Classification.RETIRED
    if any(value is not None for value in retirement_values[1:]):
        blocked = _blocking_analysis(
            payload, 'Live version row contains partial retirement fields.',
            analysis.source_protocol)
        return blocked, Classification.BLOCKER
    if analysis.blocked:
        return analysis, Classification.BLOCKER
    if analysis.classification is Classification.PLACEHOLDER:
        if row.get('yaml_content') is not None:
            blocked = _blocking_analysis(
                payload, 'A committed YAML row has a pickled-None spec.',
                analysis.source_protocol)
            return blocked, Classification.BLOCKER
        return analysis, Classification.PLACEHOLDER
    if row.get('yaml_content') is None:
        blocked = _blocking_analysis(
            payload, 'A non-placeholder spec has no committed YAML.',
            analysis.source_protocol)
        return blocked, Classification.BLOCKER
    return analysis, analysis.classification


def _parse_active_versions(value: Any) -> list[int]:
    try:
        decoded = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError) as exc:
        raise NormalizationBlocker(
            'Service active_versions is not valid JSON.') from exc
    if (not isinstance(decoded, list) or any(
            type(version) is not int or version < 1 for version in decoded)):
        raise NormalizationBlocker(
            'Service active_versions is not a positive integer list.')
    return decoded


def _parent_workload_kind(service: Mapping[str, Any]) -> str:
    """Decode the exact database parent kind without truthiness fallback."""
    pool = service.get('pool')
    if type(pool) is not int or pool not in (0, 1):
        raise NormalizationBlocker(
            'Live parent service has an invalid persisted pool discriminator.')
    return (placement_policy.WORKLOAD_KIND_POOL
            if pool == 1 else placement_policy.WORKLOAD_KIND_SERVICE)


def _require_contract_matches_parent(analysis: RawSpecAnalysis,
                                     service: Mapping[str, Any]) -> None:
    """Require one decoded version contract to match its live DB parent."""
    projection = analysis.contract_projection
    if not isinstance(projection, dict):
        raise NormalizationBlocker(
            'Committed contract has no validated contract projection.')
    expected_kind = _parent_workload_kind(service)
    if projection.get('workload_kind') != expected_kind:
        raise NormalizationBlocker(
            'Persisted contract workload kind disagrees with its live parent '
            f'service kind: {projection.get("workload_kind")!r} versus '
            f'{expected_kind!r}.')


def _scan_inventory(
        session: orm.Session,
        row_bound: int) -> tuple[list[_RowWork], dict[str, dict[str, Any]]]:
    version_table = serve_state.version_specs_table
    frozen_columns = [
        version_table.c[column]
        for column in placement_normalization_manifest.VERSION_SPEC_COLUMNS
    ]
    version_rows = [
        dict(row) for row in session.execute(
            sqlalchemy.select(*frozen_columns).order_by(
                version_table.c.service_name, version_table.c.version).limit(
                    row_bound + 1)).mappings().all()
    ]
    if len(version_rows) > row_bound:
        raise NormalizationBlocker(
            f'Inventory row count {len(version_rows)} exceeds explicit bound '
            f'{row_bound}.')
    service_names = sorted({str(row['service_name']) for row in version_rows})
    service_rows: dict[str, dict[str, Any]] = {}
    replica_counts: dict[tuple[str, int], int] = {}
    unknown_version_replica_counts: dict[str, int] = {}
    cleanup_counts: dict[str, int] = {}
    if service_names:
        service_rows = {
            str(row['name']): dict(row) for row in session.execute(
                sqlalchemy.select(*_revision_040_service_columns()).where(
                    serve_state.services_table.c.name.in_(
                        service_names))).mappings().all()
        }
        replica_counts = {
            (str(row.service_name), int(row.version)): int(row.replica_count)
            for row in session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.service_name,
                    serve_state.replicas_table.c.version,
                    sqlalchemy.func.count().label(  # pylint: disable=not-callable
                        'replica_count')).where(
                            serve_state.replicas_table.c.service_name.in_(
                                service_names),
                            serve_state.replicas_table.c.version.isnot(None)).
                group_by(serve_state.replicas_table.c.service_name,
                         serve_state.replicas_table.c.version)).all()
        }
        known_replica_version = serve_state.version_specs_table.alias(
            'placement_normalization_known_replica_version')
        replica_inventory = serve_state.replicas_table.outerjoin(
            known_replica_version,
            sqlalchemy.and_(
                known_replica_version.c.service_name ==
                serve_state.replicas_table.c.service_name,
                known_replica_version.c.version ==
                serve_state.replicas_table.c.version,
            ))
        unknown_version_replica_counts = {
            str(row.service_name): int(row.replica_count)
            for row in session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.service_name,
                    sqlalchemy.func.count().label(  # pylint: disable=not-callable
                        'replica_count')).select_from(replica_inventory).where(
                            serve_state.replicas_table.c.service_name.in_(
                                service_names),
                            sqlalchemy.or_(
                                serve_state.replicas_table.c.version.is_(None),
                                known_replica_version.c.service_name.is_(None)))
                .group_by(serve_state.replicas_table.c.service_name)).all()
        }
        cleanup_counts = {
            str(row.service_name): int(row.cleanup_count)
            for row in session.execute(
                sqlalchemy.select(
                    serve_state.ephemeral_storage_cleanup_intents_table.c.
                    service_name,
                    sqlalchemy.func.count().label(  # pylint: disable=not-callable
                        'cleanup_count')).where(
                            serve_state.ephemeral_storage_cleanup_intents_table.
                            c.service_name.in_(service_names)).
                group_by(serve_state.ephemeral_storage_cleanup_intents_table.c.
                         service_name)).all()
        }
    work_rows: list[_RowWork] = []
    for row in version_rows:
        analysis, classification = _classify_version_row(row)
        name = str(row['service_name'])
        version = int(row['version'])
        service = service_rows.get(name)
        if (service is not None and classification
                not in (Classification.BLOCKER, Classification.PLACEHOLDER,
                        Classification.RETIRED)):
            try:
                _require_contract_matches_parent(analysis, service)
            except NormalizationBlocker as exc:
                payload = _as_bytes(row['spec'])
                analysis = _blocking_analysis(payload, str(exc),
                                              analysis.source_protocol)
                classification = Classification.BLOCKER
        active_versions = ([] if service is None else _parse_active_versions(
            service['active_versions']))
        facts = {
            'service_present': service is not None,
            'service_current_version':
                (None if service is None else service['current_version']),
            'service_hash': None if service is None else service.get('hash'),
            'service_lifecycle_epoch':
                (None if service is None else service.get('lifecycle_epoch')),
            'service_resource_scope':
                (None if service is None else service.get('resource_scope')),
            'service_pool': None if service is None else service.get('pool'),
            'service_logical_replica_semantics':
                (None if service is None else
                 service.get('logical_replica_semantics')),
            'service_resource_action_mode': (
                None if service is None else service.get('resource_action_mode')
            ),
            'service_resource_action_mode_changed_at':
                (None if service is None else
                 service.get('resource_action_mode_changed_at')),
            'service_status':
                (None if service is None else service.get('status')),
            'service_active': version in active_versions,
            'replica_count': replica_counts.get((name, version), 0),
            'unknown_version_replica_count': unknown_version_replica_counts.get(
                name, 0),
            'cleanup_intent_count': cleanup_counts.get(name, 0),
            'quarantined': row.get('quarantined_at') is not None,
            'controller_applied': row.get('controller_applied_at') is not None,
            'retired': classification is Classification.RETIRED,
        }
        work_rows.append(
            _RowWork(row,
                     dict(row),
                     analysis,
                     classification,
                     dependency_facts=facts))
    return work_rows, service_rows


def _active_serve_request_evidence(
        engine: sqlalchemy.engine.Engine) -> _ExternalEvidence:
    table = request_postgres_schema.REQUESTS
    mutation_names = tuple(
        server_constants.REQUEST_NAME_PREFIX + name.value for name in (
            request_names.RequestName.SERVE_UP,
            request_names.RequestName.SERVE_UPDATE,
            request_names.RequestName.SERVE_LB_HIGH_AVAILABILITY,
            request_names.RequestName.SERVE_DOWN,
            request_names.RequestName.SERVE_TERMINATE_REPLICA,
        ))
    active_statuses = tuple(
        status.value for status in requests_lib.RequestStatus.active_statuses())
    with _reader_database_authority(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                table.c.request_id, table.c.name, table.c.status,
                table.c.execution_generation).where(
                    table.c.name.in_(mutation_names),
                    table.c.status.in_(active_statuses)).order_by(
                        table.c.request_id).limit(
                            _MAX_ACTIVE_SERVE_REQUEST_EVIDENCE_ROWS + 1)).all()
    if len(rows) > _MAX_ACTIVE_SERVE_REQUEST_EVIDENCE_ROWS:
        raise NormalizationBlocker(
            'Active Serve request evidence exceeds its explicit row bound.')
    canonical = json.dumps([(str(row.request_id), str(row.name), str(
        row.status), int(row.execution_generation)) for row in rows],
                           separators=(',', ':')).encode()
    return _ExternalEvidence(len(rows), _sha256(canonical))


def _image_demand_evidence(service_name: str,
                           version: int) -> _ExternalEvidence:
    evidence = (
        demand_state.get_live_service_version_demand_evidence_any_incarnation(
            service_name, version))
    return _ExternalEvidence(evidence.count, evidence.digest)


def _serve_controllers_are_local() -> bool:
    """Whether every service parent/controller runs in this API pod."""
    return serve_utils.is_consolidation_mode(pool=False)


def _legacy_serve_controller_cluster_evidence() -> _ExternalEvidence:
    """Inventory every legacy remote Serve-controller authority record."""
    prefix = sky_common.SKY_SERVE_CONTROLLER_PREFIX
    if (not isinstance(prefix, str) or not prefix or
            any(character.isspace() for character in prefix)):
        raise NormalizationBlocker(
            'Legacy Serve-controller cluster prefix is invalid.')
    try:
        raw_inventory = global_user_state.get_cluster_status_fields_by_prefix(
            prefix, row_limit=_MAX_LEGACY_CONTROLLER_EVIDENCE_ROWS)
    except (TypeError, ValueError) as exc:
        raise NormalizationBlocker(
            'Legacy Serve-controller inventory could not be bounded.') from exc
    if not isinstance(raw_inventory, dict):
        raise NormalizationBlocker(
            'Legacy Serve-controller inventory returned an invalid result.')
    records: list[tuple[str, str | None, int | None]] = []
    for name, status_fields in raw_inventory.items():
        if not isinstance(name, str):
            raise NormalizationBlocker(
                'Legacy Serve-controller inventory has a non-string name.')
        if not name.startswith(prefix):
            continue
        if (not isinstance(status_fields, tuple) or len(status_fields) != 2 or
                status_fields[0] is not None and
                not isinstance(status_fields[0], str) or
                status_fields[1] is not None and
                type(status_fields[1]) is not int):
            raise NormalizationBlocker(
                'Legacy Serve-controller inventory has invalid status fields.')
        records.append((name, status_fields[0], status_fields[1]))
    records.sort()
    canonical = json.dumps(
        {
            'schema': _LEGACY_CONTROLLER_EVIDENCE_SCHEMA,
            'cluster_name_prefix': prefix,
            'records': records,
        },
        sort_keys=True,
        separators=(',', ':')).encode()
    return _ExternalEvidence(len(records), _sha256(canonical))


def _fresh_api_instances(
        engine: sqlalchemy.engine.Engine) -> list[dict[str, Any]]:
    """Read a bounded database-clock snapshot of every fresh API instance."""
    table = request_postgres_schema.SERVER_INSTANCES
    with _reader_database_authority(engine) as session:
        return [
            dict(row) for row in session.execute(
                sqlalchemy.select(
                    table.c.instance_id,
                    table.c.role,
                    table.c.pod_uid,
                    table.c.ready,
                    table.c.draining_at,
                ).where(
                    table.c.heartbeat_at >= sqlalchemy.func.clock_timestamp() -
                    datetime.timedelta(
                        seconds=request_postgres.INSTANCE_STALE_AFTER_SECONDS),
                ).order_by(table.c.instance_id).limit(2)).mappings().all()
        ]


def _canonical_api_pod_identity(pod_uid: str,
                                instance_id: uuid.UUID) -> _ApiPodIdentity:
    canonical = json.dumps(
        {
            'schema': _API_POD_EVIDENCE_SCHEMA,
            'pod_uid': pod_uid,
            'instance_id': str(instance_id),
            'role': 'all',
            'ready': True,
            'draining': False,
        },
        sort_keys=True,
        separators=(',', ':')).encode()
    return _ApiPodIdentity(pod_uid, instance_id, _sha256(canonical))


def _require_sole_recreate_api_pod(
        engine: sqlalchemy.engine.Engine) -> _ApiPodIdentity:
    """Prove this is the sole fresh all-role pod in a Recreate rollout."""
    role = os.environ.get(request_postgres.SERVER_ROLE_ENV_VAR)
    if role != 'all':
        raise NormalizationBlocker(
            'Normalization apply requires the explicit all-role API pod.')
    rolling = os.environ.get(_ROLLING_UPDATE_ENV_VAR)
    if rolling not in (None, 'false'):
        raise NormalizationBlocker(
            'Normalization apply requires a Recreate API deployment.')
    pod_uid = os.environ.get(_POD_UID_ENV_VAR)
    if (pod_uid is None or not pod_uid or pod_uid.strip() != pod_uid or
            any(character.isspace() for character in pod_uid)):
        raise NormalizationBlocker(
            'Normalization apply requires the current nonempty pod UID.')
    raw_instance_id = os.environ.get(
        request_postgres.SERVER_INSTANCE_ID_ENV_VAR)
    try:
        instance_id = uuid.UUID(raw_instance_id) if raw_instance_id else None
    except (ValueError, AttributeError) as exc:
        raise NormalizationBlocker(
            'Normalization apply requires a valid current API instance ID.') \
            from exc
    if instance_id is None or str(instance_id) != raw_instance_id:
        raise NormalizationBlocker(
            'Normalization apply requires a canonical current API instance '
            'ID.')
    rows = _fresh_api_instances(engine)
    if len(rows) != 1:
        raise NormalizationBlocker(
            'Normalization apply requires exactly one fresh registered API '
            'instance.')
    row = rows[0]
    if (row.get('role') != 'all' or row.get('pod_uid') != pod_uid or
            row.get('ready') is not True or
            row.get('draining_at') is not None or
            row.get('instance_id') != instance_id):
        raise NormalizationBlocker(
            'The sole fresh API instance does not match this all-role pod.')
    return _canonical_api_pod_identity(pod_uid, instance_id)


def _target_by_name(
        targets: frozenset[_ProcessTarget]) -> dict[str, _ProcessTarget]:
    by_name: dict[str, _ProcessTarget] = {}
    for target in targets:
        if (not isinstance(target, _ProcessTarget) or not target.service_name or
                not target.service_hash or
                type(target.lifecycle_epoch) is not int or
                target.lifecycle_epoch < 1 or
                any(character.isspace() for character in target.service_name) or
                any(character.isspace() for character in target.service_hash) or
                target.service_name in by_name):
            raise NormalizationBlocker(
                'Process evidence contains an invalid target identity.')
        by_name[target.service_name] = target
    return by_name


def _mentions_target(arguments: Sequence[str],
                     target_names: frozenset[str]) -> bool:
    for argument in arguments:
        tokens = argument.split(' ')
        for token in tokens:
            if token in target_names:
                return True
            if any(token.endswith(f'={name}') for name in target_names):
                return True
    return False


def _unique_process_flag(arguments: Sequence[str], flag: str) -> str:
    if any(argument.startswith(f'{flag}=') for argument in arguments):
        raise NormalizationBlocker(
            'Serve process identity uses a noncanonical flag form.')
    indices = [
        index for index, argument in enumerate(arguments) if argument == flag
    ]
    if (len(indices) != 1 or indices[0] + 1 >= len(arguments) or
            not arguments[indices[0] + 1]):
        raise NormalizationBlocker(
            'Serve process identity has a missing or duplicate flag.')
    return arguments[indices[0] + 1]


def _service_parent_target(
        cmdline: Sequence[str],
        targets_by_name: Mapping[str, _ProcessTarget]) -> _ProcessTarget | None:
    module = 'sky.serve.service'
    module_indices = [
        index for index in range(len(cmdline) - 1)
        if cmdline[index] == '-m' and cmdline[index + 1] == module
    ]
    module_mentioned = any(module in argument for argument in cmdline)
    target_names = frozenset(targets_by_name)
    if len(module_indices) != 1:
        if module_mentioned and _mentions_target(cmdline, target_names):
            raise NormalizationBlocker(
                'A target Serve parent has a malformed module invocation.')
        return None
    try:
        service_name = _unique_process_flag(cmdline, '--service-name')
        service_hash = _unique_process_flag(cmdline, '--service-incarnation')
    except NormalizationBlocker:
        if _mentions_target(cmdline, target_names):
            raise
        return None
    target = targets_by_name.get(service_name)
    if target is None:
        return None
    if service_hash != target.service_hash:
        raise NormalizationBlocker(
            'A target Serve parent has the wrong service incarnation.')
    return target


def _controller_title_target(
        cmdline: Sequence[str],
        targets_by_name: Mapping[str, _ProcessTarget]) -> _ProcessTarget | None:
    prefix = 'sky.serve.controller'
    if not cmdline or not cmdline[0].startswith(prefix):
        return None
    if any(argument for argument in cmdline[1:]):
        raise NormalizationBlocker(
            'A Serve controller title has nonempty trailing argv entries.')
    tokens = cmdline[0].split(' ')
    if (len(tokens) != 5 or tokens[0] != prefix or
            tokens[1] != '--service-name' or
            tokens[3] != '--service-incarnation' or not tokens[2] or
            not tokens[4]):
        if _mentions_target(cmdline, frozenset(targets_by_name)):
            raise NormalizationBlocker(
                'A target Serve controller has a malformed process title.')
        return None
    target = targets_by_name.get(tokens[2])
    if target is None:
        return None
    if tokens[4] != target.service_hash:
        raise NormalizationBlocker(
            'A target Serve controller has the wrong service incarnation.')
    return target


def _serve_controller_process_evidence(targets: frozenset[_ProcessTarget],
                                       pod_uid: str) -> _ExternalEvidence:
    """Return exact same-pod Serve parent/controller process evidence."""
    targets_by_name = _target_by_name(targets)
    if (not isinstance(pod_uid, str) or not pod_uid or
            any(character.isspace() for character in pod_uid)):
        raise NormalizationBlocker('Process evidence has an invalid pod UID.')
    facts: set[_ProcessFact] = set()
    scanned = 0
    try:
        processes = iter(psutil.process_iter())
    except psutil.AccessDenied as exc:
        raise NormalizationBlocker(
            'Process evidence cannot enumerate every same-pod process.') \
            from exc
    except psutil.Error as exc:
        raise NormalizationBlocker(
            'Process evidence failed to enumerate same-pod processes.') \
            from exc
    while True:
        try:
            process = next(processes)
        except StopIteration:
            break
        except psutil.AccessDenied as exc:
            raise NormalizationBlocker(
                'Process evidence cannot enumerate every same-pod process.') \
                from exc
        except psutil.Error as exc:
            raise NormalizationBlocker(
                'Process evidence failed to enumerate same-pod processes.') \
                from exc
        scanned += 1
        if scanned > _MAX_PROCESS_EVIDENCE_ROWS:
            raise NormalizationBlocker(
                'Process evidence exceeds its explicit row bound.')
        try:
            status = process.status()
            if status == psutil.STATUS_ZOMBIE:
                continue
            cmdline = process.cmdline()
            create_time = process.create_time()
            ppid = process.ppid()
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
        except psutil.AccessDenied as exc:
            raise NormalizationBlocker(
                'Process evidence cannot read every same-pod process.') from exc
        except psutil.Error as exc:
            raise NormalizationBlocker(
                'Process evidence failed while reading same-pod processes.') \
                from exc
        pid = process.pid
        if (type(pid) is not int or pid <= 0 or type(ppid) is not int or
                ppid < 0 or not isinstance(status, str) or not status or
                not isinstance(cmdline, (list, tuple)) or
                any(not isinstance(argument, str) for argument in cmdline) or
                not isinstance(create_time,
                               (int, float)) or isinstance(create_time, bool) or
                not math.isfinite(create_time) or create_time < 0):
            raise NormalizationBlocker(
                'Process evidence returned malformed process metadata.')
        parent_target = _service_parent_target(cmdline, targets_by_name)
        controller_target = _controller_title_target(cmdline, targets_by_name)
        if parent_target is not None and controller_target is not None:
            raise NormalizationBlocker(
                'One process ambiguously matches two Serve process kinds.')
        target = parent_target or controller_target
        if target is None:
            continue
        facts.add(
            _ProcessFact(pid=pid,
                         ppid=ppid,
                         create_time=float(create_time),
                         status=status,
                         kind=('service_parent'
                               if parent_target is not None else 'controller'),
                         service_name=target.service_name,
                         service_hash=target.service_hash))
    canonical = json.dumps(
        {
            'schema': _PROCESS_EVIDENCE_SCHEMA,
            'pod_uid': pod_uid,
            'targets':
                [dataclasses.asdict(target) for target in sorted(targets)],
            'facts': [dataclasses.asdict(fact) for fact in sorted(facts)],
        },
        sort_keys=True,
        separators=(',', ':')).encode()
    return _ExternalEvidence(len(facts), _sha256(canonical))


def _resource_action_root_rows(
    engine: sqlalchemy.engine.Engine,) -> list[tuple[str, dict[str, Any]]]:
    """Read a single explicitly bounded typed-launch root inventory."""
    actions = request_postgres_schema.RESOURCE_ACTIONS
    samples = resource_action_state_schema.SHADOW_SAMPLES
    with _reader_database_authority(engine) as session:
        action_rows = [
            dict(row) for row in session.execute(
                sqlalchemy.select(
                    actions.c.action_id,
                    actions.c.domain,
                    actions.c.resource_type,
                    actions.c.resource_identity,
                    actions.c.desired_generation,
                    actions.c.action_type,
                    actions.c.immutable_spec,
                    actions.c.immutable_spec_sha256,
                ).order_by(actions.c.action_id).limit(
                    _MAX_RESOURCE_ACTION_EVIDENCE_ROWS + 1)).mappings().all()
        ]
        if len(action_rows) > _MAX_RESOURCE_ACTION_EVIDENCE_ROWS:
            raise NormalizationBlocker(
                'Resource-action evidence exceeds its explicit row bound.')
        remaining = _MAX_RESOURCE_ACTION_EVIDENCE_ROWS - len(action_rows)
        sample_rows = [
            dict(row) for row in session.execute(
                sqlalchemy.select(
                    samples.c.would_be_action_id,
                    samples.c.service_name,
                    samples.c.service_hash,
                    samples.c.service_incarnation,
                    samples.c.replica_id,
                    samples.c.replica_incarnation,
                    samples.c.desired_generation,
                    samples.c.action_type,
                    samples.c.resource_identity,
                    samples.c.immutable_spec,
                    samples.c.immutable_spec_sha256,
                ).order_by(samples.c.would_be_action_id).limit(
                    remaining + 1)).mappings().all()
        ]
    if len(sample_rows) > remaining:
        raise NormalizationBlocker(
            'Resource-action evidence exceeds its explicit row bound.')
    return (
        [('api_resource_actions', row) for row in action_rows] +
        [('serve_resource_action_shadow_samples', row) for row in sample_rows])


def _validate_resource_action_outer_row(
        store: str, row: Mapping[str, Any],
        spec: 'serve_resource_actions_types.ServeReplicaActionSpecV1') -> str:
    """Validate the indexed root identity around one typed immutable spec."""
    invocation = spec.invocation
    identity = invocation.resource_identity
    expected_resource_identity = identity.action_identity(
        invocation.action_kind).resource_identity
    if store == 'api_resource_actions':
        row_id = row.get('action_id')
        if (row_id != spec.action_id or row.get('domain') != 'serve' or
                row.get('resource_type') != 'replica' or
                row.get('resource_identity') != expected_resource_identity or
                row.get('desired_generation') != identity.desired_generation or
                row.get('action_type') != invocation.action_kind.value):
            raise NormalizationBlocker(
                'Resource-action root identity differs from its typed spec.')
    elif store == 'serve_resource_action_shadow_samples':
        row_id = row.get('would_be_action_id')
        if (row_id != spec.action_id or
                row.get('service_hash') != identity.service_hash or
                row.get('service_incarnation') != identity.service_incarnation
                or row.get('replica_id') != identity.replica_id or
                row.get('replica_incarnation') != identity.replica_incarnation
                or
                row.get('desired_generation') != identity.desired_generation or
                row.get('action_type') != invocation.action_kind.value or
                row.get('resource_identity') != expected_resource_identity):
            raise NormalizationBlocker(
                'Shadow-action root identity differs from its typed spec.')
        launch = invocation.launch
        if (launch is not None and
                row.get('service_name') != launch.source.content.service_name):
            raise NormalizationBlocker(
                'Shadow-action service name differs from its launch source.')
    else:
        raise NormalizationBlocker(
            'Resource-action evidence has an unknown root store.')
    if not isinstance(row_id, uuid.UUID):
        raise NormalizationBlocker(
            'Resource-action evidence has a malformed root ID.')
    return str(row_id)


def _possibly_serve_action_wrapper(value: Any) -> bool:
    return (isinstance(value, Mapping) and
            ('provider_plan' in value or 'invocation' in value))


def _validate_nonserve_resource_action_row(row: Mapping[str, Any]) -> str:
    """Validate enough generic storage shape before ignoring a foreign row."""
    action_id = row.get('action_id')
    domain = row.get('domain')
    resource_type = row.get('resource_type')
    resource_identity = row.get('resource_identity')
    desired_generation = row.get('desired_generation')
    action_type = row.get('action_type')
    digest = row.get('immutable_spec_sha256')
    try:
        observed_digest = kernel_resource_actions.canonical_sha256(
            row.get('immutable_spec'))
    except (TypeError, ValueError) as exc:
        raise NormalizationBlocker(
            'Non-Serve resource-action root has an invalid immutable spec.') \
            from exc
    if (not isinstance(action_id, uuid.UUID) or not isinstance(domain, str) or
            not domain or not isinstance(resource_type, str) or
            not resource_type or not isinstance(resource_identity, str) or
            not resource_identity or type(desired_generation) is not int or
            desired_generation < 1 or action_type not in ('launch', 'down') or
            digest != observed_digest):
        raise NormalizationBlocker(
            'Non-Serve resource-action root has malformed indexed state.')
    return str(action_id)


def _resource_action_inventory_fact(
    store: str,
    row: Mapping[str, Any],
    *,
    row_id: str,
    contract: str,
    immutable_spec_sha256: str,
    action_kind: str,
) -> dict[str, Any]:
    fact = {
        'store': store,
        'row_id': row_id,
        'contract': contract,
        'immutable_spec_sha256': immutable_spec_sha256,
        'action_kind': action_kind,
        'resource_identity': row.get('resource_identity'),
        'desired_generation': row.get('desired_generation'),
    }
    if store == 'api_resource_actions':
        fact.update({
            'domain': row.get('domain'),
            'resource_type': row.get('resource_type'),
        })
    elif store == 'serve_resource_action_shadow_samples':
        fact.update({
            'service_name': row.get('service_name'),
            'service_hash': row.get('service_hash'),
            'service_incarnation': str(row.get('service_incarnation')),
            'replica_id': row.get('replica_id'),
            'replica_incarnation': str(row.get('replica_incarnation')),
        })
    else:
        raise NormalizationBlocker(
            'Resource-action inventory has an unknown root store.')
    return fact


def _resource_action_evidence_from_rows(
    rows: Sequence[tuple[str, Mapping[str, Any]]],
    targets: frozenset[_ResourceActionTarget],
) -> dict[tuple[str, int], _ExternalEvidence]:
    """Typed-parse root rows and build exact per-version zero-root proofs."""
    target_by_identity: dict[tuple[str, int], _ResourceActionTarget] = {}
    facts: dict[tuple[str, int], list[dict[str, Any]]] = {}
    inventory_facts: list[dict[str, Any]] = []
    for target in targets:
        if (not isinstance(target, _ResourceActionTarget) or
                not target.service_name or type(target.version) is not int or
                target.version < 1 or not target.service_hash or
                any(character.isspace() for character in target.service_name) or
                any(character.isspace() for character in target.service_hash) or
            (target.service_name, target.version) in target_by_identity):
            raise NormalizationBlocker(
                'Resource-action evidence has an invalid target identity.')
        identity = (target.service_name, target.version)
        target_by_identity[identity] = target
        facts[identity] = []
    for store, row in rows:
        try:
            spec = serve_resource_actions.ServeReplicaActionSpecV1.from_value(
                row.get('immutable_spec'))
        except (TypeError, ValueError) as exc:
            claims_serve_replica = (row.get('domain') == 'serve' and
                                    row.get('resource_type') == 'replica')
            if (store == 'serve_resource_action_shadow_samples' or
                    claims_serve_replica or
                    _possibly_serve_action_wrapper(row.get('immutable_spec'))):
                raise NormalizationBlocker(
                    f'{store} contains an unparseable possible Serve '
                    'immutable spec.') from exc
            row_id = _validate_nonserve_resource_action_row(row)
            inventory_facts.append(
                _resource_action_inventory_fact(
                    store,
                    row,
                    row_id=row_id,
                    contract='nonserve',
                    immutable_spec_sha256=row['immutable_spec_sha256'],
                    action_kind=row['action_type']))
            continue
        if row.get('immutable_spec_sha256') != spec.sha256:
            raise NormalizationBlocker(
                f'{store} immutable spec digest does not match its preimage.')
        row_id = _validate_resource_action_outer_row(store, row, spec)
        inventory_facts.append(
            _resource_action_inventory_fact(
                store,
                row,
                row_id=row_id,
                contract='serve_replica_v1',
                immutable_spec_sha256=spec.sha256,
                action_kind=spec.invocation.action_kind.value))
        launch = spec.invocation.launch
        if launch is None:
            continue
        source = launch.source.content
        matched_target = target_by_identity.get(
            (source.service_name, source.service_version))
        if matched_target is not None:
            facts[(matched_target.service_name,
                   matched_target.version)].append({
                       'store': store,
                       'row_id': row_id,
                       'immutable_spec_sha256': spec.sha256,
                       'action_kind': spec.invocation.action_kind.value,
                       'match_kind': 'exact_launch_source',
                   })
    evidence: dict[tuple[str, int], _ExternalEvidence] = {}
    inventory_facts.sort(key=lambda fact: (fact['store'], fact['row_id']))
    for identity, target in sorted(target_by_identity.items()):
        target_facts = sorted(facts[identity],
                              key=lambda fact: (fact['store'], fact['row_id']))
        canonical = json.dumps(
            {
                'schema': _RESOURCE_ACTION_EVIDENCE_SCHEMA,
                'target': dataclasses.asdict(target),
                'root_inventory': inventory_facts,
                'facts': target_facts,
            },
            sort_keys=True,
            separators=(',', ':')).encode()
        evidence[identity] = _ExternalEvidence(len(target_facts),
                                               _sha256(canonical))
    return evidence


def _resource_action_evidence(
    engine: sqlalchemy.engine.Engine,
    targets: frozenset[_ResourceActionTarget],
) -> dict[tuple[str, int], _ExternalEvidence]:
    if not targets:
        return {}
    return _resource_action_evidence_from_rows(
        _resource_action_root_rows(engine), targets)


def _validate_external_evidence_map(
    evidence: Mapping[tuple[str, int], _ExternalEvidence],
    expected_identities: frozenset[tuple[str, int]],
    source: str,
) -> dict[tuple[str, int], _ExternalEvidence]:
    if (not isinstance(evidence, Mapping) or
            set(evidence) != set(expected_identities)):
        raise NormalizationBlocker(
            f'{source} returned an incomplete evidence map.')
    return {
        identity: _validate_external_evidence(value, source)
        for identity, value in evidence.items()
    }


def _validate_external_evidence(evidence: _ExternalEvidence,
                                source: str) -> _ExternalEvidence:
    if (not isinstance(evidence, _ExternalEvidence) or
            type(evidence.count) is not int or evidence.count < 0 or
            not isinstance(evidence.digest, str) or
            len(evidence.digest) != 64 or
            any(character not in '0123456789abcdef'
                for character in evidence.digest)):
        raise NormalizationBlocker(f'{source} returned malformed evidence.')
    return evidence


def _validate_api_pod_identity(identity: _ApiPodIdentity,
                               source: str) -> _ApiPodIdentity:
    if (not isinstance(identity, _ApiPodIdentity) or
            not isinstance(identity.pod_uid, str) or not identity.pod_uid or
            any(character.isspace() for character in identity.pod_uid) or
            not isinstance(identity.instance_id, uuid.UUID) or
            not isinstance(identity.digest, str) or
            len(identity.digest) != 64 or
            any(character not in '0123456789abcdef'
                for character in identity.digest)):
        raise NormalizationBlocker(f'{source} returned malformed identity.')
    if identity != _canonical_api_pod_identity(identity.pod_uid,
                                               identity.instance_id):
        raise NormalizationBlocker(
            f'{source} returned a noncanonical identity digest.')
    return identity


def _require_stable_api_pod(before: _ApiPodIdentity,
                            after: _ApiPodIdentity) -> None:
    if after != before:
        raise NormalizationBlocker(
            'Sole Recreate API pod identity changed during apply.')


def _require_stable_zero_evidence(before: _ExternalEvidence,
                                  after: _ExternalEvidence,
                                  source: str) -> None:
    if before.count or after.count:
        raise NormalizationBlocker(f'{source} is not quiescent.')
    if after.digest != before.digest:
        raise NormalizationBlocker(f'{source} changed during apply.')


def _require_stable_zero_evidence_map(
    before: Mapping[tuple[str, int], _ExternalEvidence],
    after: Mapping[tuple[str, int], _ExternalEvidence],
    source: str,
) -> None:
    if set(before) != set(after):
        raise NormalizationBlocker(f'{source} target inventory changed.')
    for identity in sorted(before):
        _require_stable_zero_evidence(before[identity], after[identity], source)


def _retirement_evidence_targets(
    rows: Sequence[_RowWork],
) -> tuple[frozenset[_ProcessTarget], frozenset[_ResourceActionTarget]]:
    """Build explicit external-proof identities from locked owner facts."""
    candidate_services = frozenset(
        row.identity[0]
        for row in rows
        if row.classification is Classification.HISTORICAL_PHYSICAL_PER_GPU)
    process_targets: set[_ProcessTarget] = set()
    action_targets: set[_ResourceActionTarget] = set()
    for row in rows:
        requires_version_evidence = (
            row.classification is Classification.HISTORICAL_PHYSICAL_PER_GPU or
            row.identity[0] in candidate_services and
            row.classification is Classification.PLACEHOLDER)
        if not requires_version_evidence:
            continue
        service_hash = row.dependency_facts.get('service_hash')
        lifecycle_epoch = row.dependency_facts.get('service_lifecycle_epoch')
        if (not isinstance(service_hash, str) or not service_hash or
                any(character.isspace() for character in service_hash) or
                type(lifecycle_epoch) is not int or lifecycle_epoch < 1):
            raise NormalizationBlocker(
                'Historical candidate has no exact current owner identity.')
        process_targets.add(
            _ProcessTarget(row.identity[0], service_hash, lifecycle_epoch))
        action_targets.add(
            _ResourceActionTarget(row.identity[0], row.identity[1],
                                  service_hash))
    return frozenset(process_targets), frozenset(action_targets)


def _retirement_version_evidence_rows(
        rows: Sequence[_RowWork]) -> tuple[_RowWork, ...]:
    """Return candidates plus every same-service raw placeholder."""
    candidate_services = frozenset(
        row.identity[0]
        for row in rows
        if row.classification is Classification.HISTORICAL_PHYSICAL_PER_GPU)
    return tuple(
        row for row in rows
        if row.classification is Classification.HISTORICAL_PHYSICAL_PER_GPU or
        row.identity[0] in candidate_services and
        row.classification is Classification.PLACEHOLDER)


def _read_cleanup_intent_inventory(session: orm.Session,
                                   service_names: frozenset[str],
                                   row_bound: int) -> list[dict[str, Any]]:
    """Read a complete, stably ordered candidate-service intent inventory."""
    if not service_names:
        return []
    table = serve_state.ephemeral_storage_cleanup_intents_table
    raw_rows = session.execute(
        sqlalchemy.select(table).where(
            table.c.service_name.in_(sorted(service_names))).order_by(
                table.c.service_name, table.c.resource_scope,
                table.c.storage_generation).limit(row_bound +
                                                  1)).mappings().all()
    if len(raw_rows) > row_bound:
        raise NormalizationBlocker(
            'Cleanup-intent inventory exceeds its explicit row bound.')
    return [dict(row) for row in raw_rows]


def _finite_nonnegative(value: Any) -> bool:
    return (not isinstance(value, bool) and isinstance(value, (int, float)) and
            math.isfinite(float(value)) and value >= 0)


def _build_cleanup_version_timestamp_proofs(
    rows: Sequence[_RowWork],
    candidate_services: frozenset[str],
    row_bound: int,
) -> tuple[dict[tuple[str, int], _CleanupVersionTimestampProof], int]:
    """Prove each candidate service's complete live timestamp topology."""
    live_rows_by_service: dict[str,
                               list[_RowWork]] = (collections.defaultdict(list))
    seen_identities: set[tuple[str, int]] = set()
    for row in rows:
        raw_service_name = row.original.get('service_name')
        if raw_service_name not in candidate_services:
            continue
        if (type(row.original.get('yaml_content')) is not str or
                row.original.get('retired_at') is not None):
            continue
        raw_version = row.original.get('version')
        if (type(raw_service_name) is not str or not raw_service_name or
                type(raw_version) is not int or raw_version < 1):
            raise NormalizationBlocker(
                'Live version has an invalid timestamp-proof identity.')
        identity = (raw_service_name, raw_version)
        if identity in seen_identities:
            raise NormalizationBlocker(
                'Timestamp-proof inventory has a duplicate live version.')
        seen_identities.add(identity)
        live_rows_by_service[raw_service_name].append(row)

    if set(live_rows_by_service) != set(candidate_services):
        raise NormalizationBlocker(
            'Timestamp proof has no live version for a candidate service.')
    if len(seen_identities) > row_bound:
        raise NormalizationBlocker(
            'Timestamp-proof inventory exceeds its explicit row bound.')

    proofs: dict[tuple[str, int], _CleanupVersionTimestampProof] = {}
    boundary_count = 0
    for service_name in sorted(candidate_services):
        service_rows = sorted(live_rows_by_service[service_name],
                              key=lambda item: item.identity[1])
        first_finite_index: int | None = None
        normalized_created_at: list[float | None] = []
        for index, row in enumerate(service_rows):
            created_at = row.original.get('created_at')
            if created_at is None:
                if first_finite_index is not None:
                    raise NormalizationBlocker(
                        'Live version has a NULL creation timestamp after '
                        'the finite timestamp boundary.')
                normalized_created_at.append(None)
                continue
            if not _finite_nonnegative(created_at):
                raise NormalizationBlocker(
                    'Live version has an invalid creation timestamp.')
            if first_finite_index is None:
                first_finite_index = index
            normalized_created_at.append(float(created_at))

        null_prefix_count = next(
            (index for index, value in enumerate(normalized_created_at)
             if value is not None), len(normalized_created_at))
        boundary_version: int | None = None
        boundary_created_at: float | None = None
        if null_prefix_count:
            if first_finite_index is None:
                raise NormalizationBlocker(
                    'Legacy NULL timestamp prefix has no finite boundary.')
            boundary_row = service_rows[first_finite_index]
            boundary_version = boundary_row.identity[1]
            boundary_created_at = normalized_created_at[first_finite_index]
            assert boundary_created_at is not None
            boundary_count += 1

        for index, row in enumerate(service_rows):
            created_at = normalized_created_at[index]
            if index < null_prefix_count:
                mode = _CLEANUP_CREATED_AT_MODE_LEGACY_PREFIX_NULL
                assert boundary_version is not None
                assert boundary_created_at is not None
                if row.identity[1] >= boundary_version:
                    raise NormalizationBlocker(
                        'Legacy NULL timestamp is not a strict version '
                        'prefix.')
                proof = _CleanupVersionTimestampProof(
                    mode=mode,
                    created_at=None,
                    boundary_version=boundary_version,
                    boundary_created_at=boundary_created_at)
            else:
                assert created_at is not None
                proof = _CleanupVersionTimestampProof(
                    mode=_CLEANUP_CREATED_AT_MODE_FINITE,
                    created_at=created_at,
                    boundary_version=None,
                    boundary_created_at=None)
            proofs[row.identity] = proof
    return proofs, boundary_count


def _cleanup_timestamp_proof_binding_sha256(
    facts: Mapping[str, Any],
    candidate_identity: tuple[str, int],
) -> str:
    """Bind one candidate to the complete timestamp inventory proof."""
    service_name, version = candidate_identity
    return _canonical_json_sha256({
        'cleanup_contract_schema': facts.get('cleanup_contract_schema'),
        'cleanup_version_timestamp_service_count':
            facts.get('cleanup_version_timestamp_service_count'),
        'cleanup_version_timestamp_inventory_count':
            facts.get('cleanup_version_timestamp_inventory_count'),
        'cleanup_version_timestamp_matched_intent_count':
            facts.get('cleanup_version_timestamp_matched_intent_count'),
        'cleanup_legacy_null_version_timestamp_count':
            facts.get('cleanup_legacy_null_version_timestamp_count'),
        'cleanup_timestamp_boundary_count':
            facts.get('cleanup_timestamp_boundary_count'),
        'cleanup_version_timestamp_inventory_sha256':
            facts.get('cleanup_version_timestamp_inventory_sha256'),
        'cleanup_candidate_service_name_sha256': _sha256(service_name.encode()),
        'cleanup_candidate_version': version,
        'cleanup_candidate_version_created_at_mode':
            facts.get('cleanup_candidate_version_created_at_mode'),
        'cleanup_candidate_legacy_timestamp_boundary_version':
            facts.get('cleanup_candidate_legacy_timestamp_boundary_version'),
        'cleanup_intent_key_sha256': facts.get('cleanup_intent_key_sha256'),
    })


def _zero_target_projection(yaml_content: str, source: str) -> tuple[Any, str]:
    try:
        projection = (ephemeral_storage_contract.
                      require_zero_deletion_target_projection(yaml_content))
    except ephemeral_storage_contract.EphemeralStorageContractError as exc:
        raise NormalizationBlocker(
            f'{source} does not have an exact zero-target cleanup contract: '
            f'{exc}') from exc
    projection_dict = projection.to_dict()
    return projection, _canonical_json_sha256(projection_dict)


def _build_cleanup_intent_plan(
    raw_intents: Sequence[Mapping[str, Any]],
    rows: Sequence[_RowWork],
    service_rows: Mapping[str, Mapping[str, Any]],
    row_bound: int,
) -> _CleanupIntentPlan:
    """Build the all-or-nothing typed cleanup adoption and match proof."""
    if len(raw_intents) > row_bound:
        raise NormalizationBlocker(
            'Cleanup-intent inventory exceeds its explicit row bound.')
    candidates = [
        row for row in rows
        if row.classification is Classification.HISTORICAL_PHYSICAL_PER_GPU
    ]
    candidate_services = frozenset(row.identity[0] for row in candidates)
    timestamp_proofs, timestamp_boundary_count = (
        _build_cleanup_version_timestamp_proofs(rows, candidate_services,
                                                row_bound))
    expected_columns = {
        column.name for column in
        serve_state.ephemeral_storage_cleanup_intents_table.columns
    }
    live_versions_by_yaml: dict[tuple[str, str], list[_RowWork]] = (
        collections.defaultdict(list))
    live_versions_by_generation: dict[tuple[str, str, str], _RowWork] = {}
    for row in rows:
        yaml_content = row.original.get('yaml_content')
        if (row.identity[0] in candidate_services and
                isinstance(yaml_content, str) and
                row.original.get('retired_at') is None):
            live_versions_by_yaml[(row.identity[0], yaml_content)].append(row)
            try:
                scope = (ephemeral_storage_contract.
                         parse_ephemeral_storage_scope(yaml_content))
            except ephemeral_storage_contract.EphemeralStorageContractError as exc:
                raise NormalizationBlocker(
                    'Live version has a malformed cleanup scope: '
                    f'{exc}') from exc
            if scope is None:
                continue
            parent = service_rows.get(row.identity[0])
            if (parent is None or scope.resource_scope != parent.get('hash') or
                    scope.resource_scope != parent.get('resource_scope')):
                raise NormalizationBlocker(
                    'Live version cleanup scope disagrees with its parent.')
            generation_key = (row.identity[0], scope.resource_scope,
                              scope.storage_generation)
            if generation_key in live_versions_by_generation:
                raise NormalizationBlocker(
                    'A cleanup generation is reused by multiple live '
                    'versions.')
            live_versions_by_generation[generation_key] = row

    work_rows: list[_CleanupIntentWork] = []
    match_inventory: list[dict[str, Any]] = []
    intent_by_candidate_yaml: dict[tuple[str, str],
                                   list[_CleanupIntentWork]] = (
                                       collections.defaultdict(list))
    intent_by_version: dict[tuple[str, int], _CleanupIntentWork] = {}
    seen_intent_identities: set[tuple[str, str, str]] = set()
    seen_generations: set[tuple[str, str]] = set()
    seen_versions: set[tuple[str, int]] = set()
    for raw_intent in raw_intents:
        original = dict(raw_intent)
        if set(original) != expected_columns:
            raise NormalizationBlocker(
                'Cleanup-intent inventory has an incomplete row projection.')
        service_name = original.get('service_name')
        resource_scope = original.get('resource_scope')
        storage_generation = original.get('storage_generation')
        yaml_content = original.get('yaml_content')
        if (type(service_name) is not str or not service_name or
                type(resource_scope) is not str or not resource_scope or
                type(storage_generation) is not str or not storage_generation or
                type(yaml_content) is not str):
            raise NormalizationBlocker(
                'Cleanup-intent identity and YAML fields must be exact '
                'nonempty strings.')
        if service_name not in candidate_services:
            raise NormalizationBlocker(
                'Cleanup-intent inventory contains an unexpected service.')
        identity = (service_name, resource_scope, storage_generation)
        if identity in seen_intent_identities:
            raise NormalizationBlocker(
                'Cleanup-intent inventory contains a duplicate identity.')
        seen_intent_identities.add(identity)
        generation_identity = (service_name, storage_generation)
        if generation_identity in seen_generations:
            raise NormalizationBlocker(
                'A cleanup generation is reused within one service.')
        seen_generations.add(generation_identity)

        parent = service_rows.get(service_name)
        if parent is None:
            raise NormalizationBlocker(
                'Cleanup intent has no live parent service.')
        parent_hash = parent.get('hash')
        parent_scope = parent.get('resource_scope')
        parent_pool = parent.get('pool')
        parent_epoch = parent.get('lifecycle_epoch')
        if (type(parent_hash) is not str or not parent_hash or
                type(parent_scope) is not str or not parent_scope or
                parent_hash != parent_scope or resource_scope != parent_scope):
            raise NormalizationBlocker(
                'Cleanup intent does not match the exact parent resource '
                'scope and service incarnation.')
        pool = original.get('pool')
        lifecycle_epoch = original.get('lifecycle_epoch')
        provisional = original.get('provisional')
        created_at = original.get('created_at')
        if (type(parent_pool) is not int or parent_pool != 0 or
                type(pool) is not int or pool != parent_pool):
            raise NormalizationBlocker(
                'Cleanup intent does not have the exact non-pool parent bit.')
        if (type(parent_epoch) is not int or parent_epoch < 1 or
                type(lifecycle_epoch) is not int or lifecycle_epoch < 1 or
                lifecycle_epoch > parent_epoch):
            raise NormalizationBlocker(
                'Cleanup intent has an invalid or future lifecycle epoch.')
        if type(provisional) is not int or provisional not in (0, 1):
            raise NormalizationBlocker(
                'Cleanup intent has an invalid provisional bit.')
        if not _finite_nonnegative(created_at):
            raise NormalizationBlocker(
                'Cleanup intent has an invalid creation timestamp.')

        intent_projection, projection_sha256 = _zero_target_projection(
            yaml_content, 'Cleanup-intent YAML')
        scope = intent_projection.scope
        if (scope.resource_scope != resource_scope or
                scope.storage_generation != storage_generation):
            raise NormalizationBlocker(
                'Cleanup-intent YAML scope disagrees with its row identity.')
        matching_versions = live_versions_by_yaml.get(
            (service_name, yaml_content), [])
        if len(matching_versions) != 1:
            raise NormalizationBlocker(
                'Cleanup intent does not map byte-for-byte to exactly one '
                'live committed version YAML.')
        matched = matching_versions[0]
        generation_match = live_versions_by_generation.get(identity)
        if generation_match is not matched:
            raise NormalizationBlocker(
                'Cleanup generation does not identify its unique matched '
                'version.')
        matched_timestamp = timestamp_proofs.get(matched.identity)
        if matched_timestamp is None:
            raise NormalizationBlocker(
                'Matched version has no complete timestamp proof.')
        assert isinstance(created_at, (int, float))
        if float(created_at) > matched_timestamp.intent_created_at_upper_bound:
            if (matched_timestamp.mode ==
                    _CLEANUP_CREATED_AT_MODE_LEGACY_PREFIX_NULL):
                raise NormalizationBlocker(
                    'Cleanup intent was created after the legacy timestamp '
                    'boundary.')
            raise NormalizationBlocker(
                'Cleanup intent was created after its matched version.')
        matched_projection, matched_projection_sha256 = (
            _zero_target_projection(yaml_content, 'Matched version YAML'))
        if (matched_projection.scope != scope or
                matched_projection_sha256 != projection_sha256):
            raise NormalizationBlocker(
                'Cleanup intent and matched version have different scopes.')
        if matched.identity in seen_versions:
            raise NormalizationBlocker(
                'Multiple cleanup intents map to one retained version.')
        seen_versions.add(matched.identity)

        result = dict(original)
        result['provisional'] = 0
        work = _CleanupIntentWork(original, result, matched.identity,
                                  projection_sha256, matched_timestamp)
        work_rows.append(work)
        intent_by_candidate_yaml[(service_name, yaml_content)].append(work)
        intent_by_version[matched.identity] = work
        key_sha256 = _canonical_json_sha256(list(identity))
        yaml_sha256 = _sha256(yaml_content.encode())
        match_inventory.append({
            'intent_key_sha256': key_sha256,
            'matched_service_name_sha256': _sha256(service_name.encode()),
            'matched_version': matched.identity[1],
            'yaml_sha256': yaml_sha256,
            'zero_target_projection_sha256': projection_sha256,
        })

    timestamp_inventory: list[list[Any]] = []
    legacy_null_timestamp_count = 0
    for version_identity in sorted(timestamp_proofs):
        proof = timestamp_proofs[version_identity]
        match = intent_by_version.get(version_identity)
        matched_intent_key_sha256 = None
        matched_intent_created_at = None
        if match is not None:
            matched_intent_key_sha256 = _canonical_json_sha256(
                list(match.identity))
            raw_intent_created_at = match.original['created_at']
            assert isinstance(raw_intent_created_at, (int, float))
            matched_intent_created_at = float(raw_intent_created_at)
        if proof.mode == _CLEANUP_CREATED_AT_MODE_LEGACY_PREFIX_NULL:
            legacy_null_timestamp_count += 1
        timestamp_inventory.append([
            _sha256(version_identity[0].encode()),
            version_identity[1],
            'committed',
            proof.mode,
            proof.created_at,
            proof.boundary_version,
            proof.boundary_created_at,
            matched_intent_key_sha256,
            matched_intent_created_at,
        ])

    candidate_facts: dict[tuple[str, int], dict[str, Any]] = {}
    for candidate in candidates:
        yaml_content = candidate.original.get('yaml_content')
        if not isinstance(yaml_content, str):
            raise NormalizationBlocker(
                'Historical candidate has no committed compiled YAML.')
        matches = intent_by_candidate_yaml.get(
            (candidate.identity[0], yaml_content), [])
        if len(matches) != 1:
            raise NormalizationBlocker(
                'Historical candidate does not map to exactly one cleanup '
                'intent.')
        candidate_projection, candidate_projection_sha256 = (
            _zero_target_projection(yaml_content, 'Historical candidate YAML'))
        match = matches[0]
        parent = service_rows[candidate.identity[0]]
        if (candidate_projection.scope.resource_scope != parent.get('hash') or
                candidate_projection.scope.resource_scope
                != parent.get('resource_scope')):
            raise NormalizationBlocker(
                'Historical candidate YAML does not match its parent scope.')
        intent_yaml = match.original['yaml_content']
        candidate_yaml_sha256 = _sha256(yaml_content.encode())
        intent_yaml_sha256 = _sha256(intent_yaml.encode())
        matched_timestamp = match.matched_version_timestamp
        candidate_facts[candidate.identity] = {
            'cleanup_candidate_match_count': 1,
            'cleanup_intent_key_sha256': _canonical_json_sha256(
                list(match.identity)),
            'cleanup_candidate_yaml_sha256': candidate_yaml_sha256,
            'cleanup_intent_yaml_sha256': intent_yaml_sha256,
            'cleanup_candidate_zero_target_projection_sha256': candidate_projection_sha256,
            'cleanup_intent_zero_target_projection_sha256':
                match.projection_sha256,
            'cleanup_candidate_deletion_target_count': 0,
            'cleanup_intent_deletion_target_count': 0,
            'cleanup_candidate_version_created_at_mode': matched_timestamp.mode,
            'cleanup_candidate_legacy_timestamp_boundary_version':
                matched_timestamp.boundary_version,
            'retired_yaml_preserved': candidate_yaml_sha256 ==
                                      intent_yaml_sha256,
            'current_cleanup_reader_inventory_preserved': True,
            'v1_1_1135_cleanup_omission_lossless': True,
        }

    if set(candidate_facts) != {candidate.identity for candidate in candidates}:
        raise NormalizationBlocker(
            'Cleanup proof does not cover every historical candidate.')
    ordered_work = tuple(sorted(work_rows, key=lambda work: work.identity))
    pre_digest = _cleanup_intent_fleet_sha256(ordered_work, result=False)
    post_digest = _cleanup_intent_fleet_sha256(ordered_work, result=True)
    adopted_count = sum(work.adopted for work in ordered_work)
    if (pre_digest == post_digest) != (adopted_count == 0):
        raise NormalizationBlocker(
            'Cleanup-intent plan digest does not match its provisional delta.')
    global_facts = {
        'cleanup_contract_schema': _CLEANUP_PROOF_SCHEMA,
        'cleanup_intent_inventory_count': len(ordered_work),
        'cleanup_intent_pre_inventory_sha256': pre_digest,
        'cleanup_intent_post_inventory_sha256': post_digest,
        'cleanup_intent_adopted_count': adopted_count,
        'cleanup_match_inventory_count': len(match_inventory),
        'cleanup_match_inventory_sha256': _canonical_json_sha256(
            sorted(match_inventory,
                   key=lambda item: (item['matched_service_name_sha256'], item[
                       'matched_version'], item['intent_key_sha256']))),
        'cleanup_version_timestamp_service_count': len(candidate_services),
        'cleanup_version_timestamp_inventory_count': len(timestamp_inventory),
        'cleanup_version_timestamp_matched_intent_count':
            len(intent_by_version),
        'cleanup_legacy_null_version_timestamp_count': legacy_null_timestamp_count,
        'cleanup_timestamp_boundary_count': timestamp_boundary_count,
        'cleanup_version_timestamp_inventory_sha256':
            _canonical_json_sha256(timestamp_inventory),
    }
    for candidate_identity, facts in candidate_facts.items():
        combined_facts = {**global_facts, **facts}
        facts['cleanup_timestamp_proof_sha256'] = (
            _cleanup_timestamp_proof_binding_sha256(combined_facts,
                                                    candidate_identity))
    return _CleanupIntentPlan(ordered_work, candidate_facts, global_facts)


def _select_recovery_version(
        rows: list[_RowWork],
        excluded: frozenset[tuple[str, int]] = frozenset(),
) -> int | None:
    committed = [
        row for row in rows if row.identity not in excluded and
        row.result.get('yaml_content') is not None
    ]
    applicable = [
        row.identity[1]
        for row in committed
        if row.result.get('quarantined_at') is None
    ]
    quarantined = [
        row.identity[1]
        for row in rows
        if row.result.get('quarantined_at') is not None
    ]
    applied = [
        row.identity[1]
        for row in committed
        if row.result.get('quarantined_at') is None and
        row.result.get('controller_applied_at') is not None
    ]
    latest_applicable = max(applicable, default=None)
    latest_quarantined = max(quarantined, default=None)
    latest_applied = max(applied, default=None)
    if (latest_quarantined is not None and
        (latest_applicable is None or latest_applicable < latest_quarantined)):
        return latest_applied
    return latest_applicable


def _validate_approved_loaded_image_commits(
        values: Sequence[str]) -> frozenset[str]:
    commits = frozenset(values)
    if (not commits or len(commits) != len(values) or any(
            type(commit) is not str or len(commit) != 40 or any(
                character not in '0123456789abcdef'
                for character in commit)
            for commit in commits)):
        raise ValueError(
            'Historical retirement requires a nonempty, duplicate-free set '
            'of exact 40-hex approved loaded image commits.')
    return commits


def _completed_run_inventory(
        session: orm.Session, run_id: uuid.UUID,
        row_bound: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    runs = serve_state.placement_normalization_runs_table
    ledger = serve_state.placement_normalization_rows_table
    run = session.execute(
        sqlalchemy.select(runs).where(
            runs.c.run_id == run_id)).mappings().one_or_none()
    if run is None:
        raise NormalizationBlocker(
            'Predecessor receipt references an absent run manifest.')
    entries = session.execute(
        sqlalchemy.select(ledger).where(ledger.c.run_id == run_id).order_by(
            ledger.c.service_name,
            ledger.c.version).limit(row_bound + 1)).mappings().all()
    if len(entries) > row_bound:
        raise NormalizationBlocker(
            'Predecessor receipt ledger exceeds its explicit row bound.')
    run_dict = dict(run)
    entry_dicts = [dict(entry) for entry in entries]
    mismatches = placement_normalization_manifest.manifest_mismatches(
        run_dict, entry_dicts)
    if mismatches:
        raise NormalizationBlocker(
            'Predecessor receipt run manifest is invalid; first mismatch is '
            f'{mismatches[0]!r}.')
    return run_dict, entry_dicts


def _terminal_current_inventory_mismatches(
    session: orm.Session,
    run: Mapping[str, Any],
    entries: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    """Bind one terminal manifest to its current candidate-service rows."""
    if not placement_normalization_manifest.is_terminal_protocol4_manifest(
            run, entries):
        return ()
    candidate_services = sorted({
        str(entry['service_name'])
        for entry in entries
        if entry.get('classification') == (
            placement_normalization_manifest.ManifestClassification.
            HISTORICAL_PHYSICAL_PER_GPU.value) and
        entry.get('outcome') == 'retired'
    })
    version_table = serve_state.version_specs_table
    frozen_columns = [
        version_table.c[column]
        for column in placement_normalization_manifest.VERSION_SPEC_COLUMNS
    ]
    current_rows = [
        dict(row) for row in session.execute(
            sqlalchemy.select(*frozen_columns).where(
                version_table.c.service_name.in_(candidate_services)).order_by(
                    version_table.c.service_name,
                    version_table.c.version).limit(_MAX_INVENTORY_ROWS +
                                                   1)).mappings().all()
    ]
    if len(current_rows) > _MAX_INVENTORY_ROWS:
        return ({
            'run_id': str(run.get('run_id')),
            'reason': 'current_candidate_inventory_exceeds_bound',
        },)
    current_service_hashes = {
        service_name: placement_normalization_manifest.ServiceHashObservation(
            False, None) for service_name in candidate_services
    }
    service_table = serve_state.services_table
    for row in session.execute(
            sqlalchemy.select(service_table.c.name, service_table.c.hash).where(
                service_table.c.name.in_(candidate_services))).all():
        current_service_hashes[str(row.name)] = (
            placement_normalization_manifest.ServiceHashObservation(
                True, row.hash))
    current_classifications = {
        (str(row['service_name']), int(row['version'])): analyze_spec_pickle(
            row['spec']).classification.value for row in current_rows
    }
    return tuple(
        placement_normalization_manifest.current_inventory_mismatches(
            run, entries, current_rows, current_service_hashes,
            current_classifications))


def _require_no_terminal_protocol4_manifest(session: orm.Session) -> None:
    """Refuse every write after the durable stale-placeholder generation."""
    runs = serve_state.placement_normalization_runs_table
    latest_run_id = session.execute(
        sqlalchemy.select(runs.c.run_id).order_by(
            runs.c.completed_at.desc(),
            runs.c.run_id.desc()).limit(1)).scalar_one_or_none()
    if latest_run_id is None:
        return
    run, entries = _completed_run_inventory(session, latest_run_id,
                                            _MAX_INVENTORY_ROWS)
    if not placement_normalization_manifest.is_terminal_protocol4_manifest(
            run, entries):
        return
    live_mismatches = _terminal_current_inventory_mismatches(
        session, run, entries)
    if live_mismatches:
        raise NormalizationBlocker(
            'Protocol-4 terminal inventory proof no longer matches live '
            f'state; first mismatch is {live_mismatches[0]!r}.')
    raise NormalizationBlocker(
        'Protocol-4 stale-placeholder retirement is terminal; later '
        'normalization writes are forbidden.')


def _predecessor_receipt_evidence(
    session: orm.Session,
    rows: Sequence[_RowWork],
    service_rows: Mapping[str, Mapping[str, Any]],
    approved_loaded_image_commits: frozenset[str],
    row_bound: int,
    freeze_evidence_sha256: str,
) -> _PredecessorReceiptEvidence:
    """Validate every nonempty receipt tuple as completed immutable evidence."""
    table = serve_state.services_table
    receipt_columns = (
        table.c.placement_normalization_requested_run_id,
        table.c.placement_normalization_loaded_run_id,
        table.c.placement_normalization_loaded_image_commit,
        table.c.placement_normalization_loaded_controller_pid,
        table.c.placement_normalization_loaded_controller_ip,
        table.c.placement_normalization_loaded_boot_id,
        table.c.placement_normalization_loaded_at,
    )
    receipt_services = session.execute(
        sqlalchemy.select(*_revision_040_service_columns()).where(
            sqlalchemy.or_(
                *(column.isnot(None) for column in receipt_columns))).order_by(
                    table.c.name).limit(row_bound + 1)).mappings().all()
    if len(receipt_services) > row_bound:
        raise NormalizationBlocker(
            'Predecessor receipt inventory exceeds its explicit row bound.')
    versions_by_service: dict[str,
                              list[_RowWork]] = collections.defaultdict(list)
    for row in rows:
        versions_by_service[row.identity[0]].append(row)
    validated_manifests: dict[uuid.UUID, tuple[dict[str, Any],
                                               list[dict[str, Any]]]] = {}
    inventory: list[dict[str, Any]] = []
    names: set[str] = set()
    for raw_service in receipt_services:
        service = dict(raw_service)
        service_name = service.get('name')
        if (type(service_name) is not str or not service_name or
                service_name in names):
            raise NormalizationBlocker(
                'Predecessor receipt inventory has an invalid or duplicate '
                'service name.')
        names.add(service_name)
        if service_rows.get(service_name) != service:
            raise NormalizationBlocker(
                'Predecessor receipt service is absent from the locked '
                'version inventory or changed during its scan.')
        requested_run_id = service.get(
            'placement_normalization_requested_run_id')
        loaded_run_id = service.get('placement_normalization_loaded_run_id')
        if not isinstance(requested_run_id, uuid.UUID):
            raise NormalizationBlocker(
                'Predecessor receipt has loaded state without a requested '
                'run UUID.')
        if loaded_run_id != requested_run_id:
            raise NormalizationBlocker(
                'Predecessor receipt is pending or belongs to another run.')
        service_hash = service.get('hash')
        current_version = service.get('current_version')
        if (type(service_hash) is not str or not service_hash or
                type(current_version) is not int or current_version < 1):
            raise NormalizationBlocker(
                'Predecessor receipt has an invalid service incarnation or '
                'current version.')
        service_versions = versions_by_service.get(service_name, [])
        recovery_version = _select_recovery_version(service_versions)
        if recovery_version is None:
            raise NormalizationBlocker(
                'Predecessor receipt has no live recovery version.')
        receipt_row = session.execute(
            serve_state._placement_normalization_receipt_query(  # pylint: disable=protected-access
                service_name,
                recovery_version,
                current_version,
                service_hash,
                (service.get('controller_pid'), service.get('controller_ip')),
                require_ledger=False)).mappings().one_or_none()
        if receipt_row is None:
            raise NormalizationBlocker(
                'Predecessor receipt lost its current/recovery version '
                'snapshot.')
        receipt = dict(receipt_row)
        try:
            manifest_completed_at = (
                serve_state._validate_placement_normalization_run_manifest(  # pylint: disable=protected-access
                    receipt, requested_run_id))
            serve_state._validate_raw_explicit_placement_contract(  # pylint: disable=protected-access
                receipt,
                'recovery',
                require_cleanup_contract=True)
            serve_state._validate_raw_explicit_placement_contract(  # pylint: disable=protected-access
                receipt,
                'current',
                require_cleanup_contract=True)
            serve_state._validate_placement_normalization_loaded_receipt(  # pylint: disable=protected-access
                receipt, requested_run_id, manifest_completed_at)
            serve_state._validate_placement_normalization_completed_service_incarnation(  # pylint: disable=protected-access
                receipt, service_hash)
            for prefix in ('recovery', 'current'):
                serve_state._validate_placement_normalization_completed_ledger_result(  # pylint: disable=protected-access
                    receipt, prefix, service_hash, manifest_completed_at)
        except RuntimeError as exc:
            raise NormalizationBlocker(
                f'Predecessor receipt validation failed: {exc}') from exc
        loaded_commit = receipt['loaded_image_commit']
        loaded_at = receipt['loaded_at']
        if loaded_commit not in approved_loaded_image_commits:
            raise NormalizationBlocker(
                'Predecessor receipt was loaded by an unapproved image '
                'commit.')
        if requested_run_id not in validated_manifests:
            validated_manifests[requested_run_id] = _completed_run_inventory(
                session, requested_run_id, row_bound)
        manifest, _ = validated_manifests[requested_run_id]
        inventory.append({
            'service_name_sha256': _sha256(service_name.encode()),
            'service_hash_sha256': _sha256(service_hash.encode()),
            'requested_run_id': str(requested_run_id),
            'loaded_run_id': str(loaded_run_id),
            'loaded_image_commit': loaded_commit,
            'loaded_controller_pid': receipt['loaded_controller_pid'],
            'loaded_controller_ip_sha256':
                (None if receipt['loaded_controller_ip'] is None else _sha256(
                    receipt['loaded_controller_ip'].encode())),
            'loaded_boot_id': receipt['loaded_boot_id'],
            'loaded_at': float(loaded_at),
            'normalizer_version': manifest['normalizer_version'],
            'current_version': current_version,
            'recovery_version': recovery_version,
        })
    approved_commit_sha256 = _canonical_json_sha256(
        sorted(approved_loaded_image_commits))
    freeze_binding_sha256 = _canonical_json_sha256({
        'approved_loaded_image_commit_sha256': approved_commit_sha256,
        'operator_freeze_evidence_input_sha256': freeze_evidence_sha256,
    })
    facts = {
        'predecessor_receipt_schema': _PREDECESSOR_RECEIPT_SCHEMA,
        'predecessor_receipt_inventory_count': len(inventory),
        'predecessor_receipt_inventory_sha256':
            _canonical_json_sha256(inventory),
        'approved_loaded_image_commit_count':
            len(approved_loaded_image_commits),
        'approved_loaded_image_commit_sha256': approved_commit_sha256,
        'operator_freeze_evidence_input_sha256': freeze_evidence_sha256,
        'operator_freeze_approved_commit_binding_sha256': freeze_binding_sha256,
        'predecessor_receipts_complete': True,
    }
    return _PredecessorReceiptEvidence(frozenset(names), facts)


def _validate_successor_controller_config(row: _RowWork,
                                          workspace: Any) -> None:
    values = [
        row.result.get('controller_config'),
        row.result.get('controller_config_digest'),
        row.result.get('controller_config_snapshot_id'),
    ]
    if any(value is None for value in values):
        raise NormalizationBlocker(
            'Recovery successor has an incomplete controller-config tuple.')
    config = _as_bytes(values[0])
    digest = values[1]
    snapshot_id = values[2]
    if (not isinstance(digest, str) or len(digest) != 64 or
            any(character not in '0123456789abcdef' for character in digest) or
            _sha256(config) != digest):
        raise NormalizationBlocker(
            'Recovery successor controller-config digest is invalid.')
    if (not isinstance(snapshot_id, str) or len(snapshot_id) != 64 or any(
            character not in '0123456789abcdef' for character in snapshot_id)):
        raise NormalizationBlocker(
            'Recovery successor controller-config snapshot ID is invalid.')
    if not isinstance(workspace, str) or not workspace:
        raise NormalizationBlocker(
            'Service workspace is unavailable for controller-config proof.')
    try:
        serve_utils.parse_and_validate_version_controller_config(
            config, workspace, 'placement normalization retirement proof')
    except Exception as exc:
        raise NormalizationBlocker(
            'Recovery successor controller config failed validation.') from exc


def _prepare_stale_placeholder_proofs(
    rows: list[_RowWork],
    service_rows: Mapping[str, Mapping[str, Any]],
    image_evidence: Mapping[tuple[str, int], _ExternalEvidence],
    resource_action_evidence: Mapping[tuple[str, int], _ExternalEvidence],
    retiring_identities: frozenset[tuple[str, int]],
) -> dict[str, dict[str, Any]]:
    """Prove and ledger exact stale placeholders for candidate services."""
    candidate_services = sorted(
        {identity[0] for identity in retiring_identities})
    staged_evidence: list[tuple[_RowWork, dict[str, Any]]] = []
    summaries: dict[str, dict[str, Any]] = {}
    for service_name in candidate_services:
        service = service_rows.get(service_name)
        if service is None:
            raise NormalizationBlocker(
                'Stale-placeholder proof requires a live parent service.')
        current_version = service.get('current_version')
        if type(current_version) is not int or current_version < 1:
            raise NormalizationBlocker(
                'Stale-placeholder proof requires a positive current version.')
        service_versions = [
            row for row in rows if row.identity[0] == service_name
        ]
        current_rows = [
            row for row in service_versions
            if row.identity[1] == current_version and
            row.identity not in retiring_identities and
            row.classification is Classification.EXPLICIT_V2 and row.result.get(
                'yaml_content') is not None and row.result.get('retired_at') is
            None and row.result.get('quarantined_at') is None
        ]
        if len(current_rows) != 1:
            raise NormalizationBlocker(
                'Stale-placeholder proof requires one surviving committed '
                'explicit-v2 current version.')

        placeholder_rows = sorted(
            (row for row in service_versions
             if row.classification is Classification.PLACEHOLDER),
            key=lambda row: row.identity)
        service_evidence: list[dict[str, Any]] = []
        for row in placeholder_rows:
            identity = row.identity
            if (row.analysis.classification is not Classification.PLACEHOLDER or
                    row.outcome != 'unchanged' or
                    row.manifest_classification is not None or
                    _as_bytes(row.original['spec']) != _RETIRED_SPEC_BYTES or
                    _as_bytes(row.result['spec']) != _RETIRED_SPEC_BYTES):
                raise NormalizationBlocker(
                    'Historical service placeholder is not the canonical '
                    'protocol-4 None representation.')
            if current_version <= identity[1]:
                raise NormalizationBlocker(
                    'Historical service has a fillable placeholder without a '
                    'strictly newer committed version.')
            if (set(_STALE_PLACEHOLDER_NULL_COLUMNS) - set(row.original) or
                    set(_STALE_PLACEHOLDER_NULL_COLUMNS) - set(row.result) or
                    any(row.original[column] is not None or
                        row.result[column] is not None
                        for column in _STALE_PLACEHOLDER_NULL_COLUMNS) or
                    _row_sha256(row.original) != _row_sha256(row.result)):
                raise NormalizationBlocker(
                    'Historical service placeholder has live or staged state.')
            if (row.dependency_facts.get('service_current_version')
                    != current_version or
                    row.dependency_facts.get('service_active') is not False or
                    type(row.dependency_facts.get('replica_count')) is not int
                    or row.dependency_facts['replica_count'] != 0 or type(
                        row.dependency_facts.get(
                            'unknown_version_replica_count')) is not int or
                    row.dependency_facts['unknown_version_replica_count'] != 0):
                raise NormalizationBlocker(
                    'Historical service placeholder is current, active, or '
                    'replica-owning.')
            image = image_evidence.get(identity)
            action = resource_action_evidence.get(identity)
            if image is None or action is None:
                raise NormalizationBlocker(
                    'Stale-placeholder external evidence is incomplete.')
            image = _validate_external_evidence(
                image, 'Stale-placeholder container-image demand')
            action = _validate_external_evidence(
                action, 'Stale-placeholder resource-action root')
            if image.count != 0:
                raise NormalizationBlocker(
                    'Historical service placeholder has live container-image '
                    'demand.')
            if action.count != 0:
                raise NormalizationBlocker(
                    'Historical service placeholder has a typed '
                    'resource-action launch root.')
            evidence = {
                'schema': _STALE_PLACEHOLDER_PROOF_SCHEMA,
                'service_name_sha256': _sha256(service_name.encode()),
                'version': identity[1],
                'original_row_sha256': _row_sha256(row.original),
                'strictly_newer_committed_version': current_version,
                'image_demand_count': image.count,
                'image_demand_sha256': image.digest,
                'resource_action_root_count': action.count,
                'resource_action_root_sha256': action.digest,
                'state_clean': True,
                'fill_stale_proved': True,
            }
            assert set(evidence) == _STALE_PLACEHOLDER_EVIDENCE_FIELDS
            service_evidence.append(evidence)
            staged_evidence.append((row, evidence))

        summary = {
            'schema': _STALE_PLACEHOLDER_PROOF_SCHEMA,
            'service_name_sha256': _sha256(service_name.encode()),
            'current_version': current_version,
            'placeholder_count': len(service_evidence),
            'image_demand_count': 0,
            'resource_action_root_count': 0,
            'inventory_sha256':
                (placement_normalization_manifest.
                 stale_placeholder_inventory_sha256(service_name,
                                                    current_version,
                                                    service_evidence)),
            'fill_stale_proved': True,
        }
        assert set(summary) == (_SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FIELDS)
        summaries[service_name] = summary

    for row, evidence in staged_evidence:
        row.dependency_facts[_STALE_PLACEHOLDER_EVIDENCE_FACT] = evidence
        row.manifest_classification = (placement_normalization_manifest.
                                       ManifestClassification.STALE_PLACEHOLDER)
    return summaries


def _retirement_dependency_facts(
    candidate: _RowWork,
    all_rows: list[_RowWork],
    service_rows: dict[str, dict[str, Any]],
    image_evidence: _ExternalEvidence,
    resource_action_evidence: _ExternalEvidence,
    legacy_controller_evidence: _ExternalEvidence,
    retiring_identities: frozenset[tuple[str, int]],
    process_evidence: _ExternalEvidence,
    api_pod_identity: _ApiPodIdentity,
    request_evidence: _ExternalEvidence,
    cleanup_plan: _CleanupIntentPlan,
    receipt_evidence: _PredecessorReceiptEvidence,
    stale_placeholder_proof: Mapping[str, Any],
) -> dict[str, Any]:
    name, version = candidate.identity
    service = service_rows.get(name)
    if service is None:
        raise NormalizationBlocker(
            'Historical retirement requires a live parent service row for '
            'the locked recovery and ownership proof.')
    if _parent_workload_kind(service) != placement_policy.WORKLOAD_KIND_SERVICE:
        raise NormalizationBlocker(
            'Historical retirement requires an exact non-pool parent service.')
    if (service.get('resource_action_mode') != 'legacy' or
            service.get('resource_action_mode_changed_at') is not None):
        raise NormalizationBlocker(
            'Historical retirement requires the live parent resource-action '
            'mode to remain at the inert legacy default.')
    service_versions = [row for row in all_rows if row.identity[0] == name]
    config_fields = ('controller_config', 'controller_config_digest',
                     'controller_config_snapshot_id')
    incomplete_config_versions = [
        row for row in service_versions
        if any(row.result.get(field) is not None for field in config_fields) and
        not all(row.result.get(field) is not None for field in config_fields)
    ]
    if incomplete_config_versions:
        raise NormalizationBlocker(
            'Historical service still has an incomplete staged controller '
            'configuration generation.')
    newer = [
        row for row in service_versions
        if row.identity[1] > version and row.identity not in retiring_identities
        and row.result.get('yaml_content') is not None
    ]
    if not newer:
        raise NormalizationBlocker(
            'Historical retirement requires a strictly newer committed '
            'successor.')
    current_version = service.get('current_version')
    if type(current_version) is not int or current_version <= version:
        raise NormalizationBlocker(
            'Service current_version has not advanced past the historical '
            'version.')
    current_rows = [
        row for row in service_versions
        if row.identity[1] == current_version and row.identity not in
        retiring_identities and row.result.get('yaml_content') is not None
    ]
    if len(current_rows) != 1:
        raise NormalizationBlocker(
            'Service current_version does not identify a surviving committed '
            'version.')
    active_versions = _parse_active_versions(service.get('active_versions'))
    if version in active_versions:
        raise NormalizationBlocker('Historical version is still active.')
    if candidate.dependency_facts['replica_count']:
        raise NormalizationBlocker(
            'Historical version still owns replica rows.')
    if candidate.dependency_facts['unknown_version_replica_count']:
        raise NormalizationBlocker(
            'Historical service still has NULL or orphan-version replica '
            'rows.')
    service_cleanup_count = sum(
        intent.identity[0] == name for intent in cleanup_plan.rows)
    if (candidate.dependency_facts['cleanup_intent_count']
            != service_cleanup_count):
        raise NormalizationBlocker(
            'Typed cleanup proof does not cover the service-wide intent '
            'inventory.')
    cleanup_facts = cleanup_plan.candidate_facts.get(candidate.identity)
    if cleanup_facts is None:
        raise NormalizationBlocker(
            'Historical candidate has no typed cleanup-intent match proof.')
    if (_service_requires_normalization_receipt(service) and
            name not in receipt_evidence.service_names):
        raise NormalizationBlocker(
            'Live historical service has no completed predecessor load '
            'receipt.')
    if candidate.original.get('placement_catalog') is not None:
        raise NormalizationBlocker(
            'Historical version still has a placement catalog activation.')
    if candidate.original.get('quarantined_at') is not None:
        raise NormalizationBlocker(
            'A quarantined historical version cannot be retired in place.')
    if candidate.original.get('controller_applied_at') is not None:
        raise NormalizationBlocker(
            'Historical version has a controller-applied receipt.')
    recovery_version = _select_recovery_version(service_versions,
                                                retiring_identities)
    if recovery_version is None or recovery_version <= version:
        raise NormalizationBlocker(
            'Quarantine-aware recovery does not select a newer successor.')
    recovery_row = next(
        row for row in service_versions if row.identity[1] == recovery_version)
    config_protocol_active = any(
        any(
            row.result.get(field) is not None
            for field in ('controller_config', 'controller_config_digest',
                          'controller_config_snapshot_id'))
        for row in service_versions)
    if config_protocol_active:
        _validate_successor_controller_config(recovery_row,
                                              service.get('workspace'))
    if image_evidence.count != 0:
        raise NormalizationBlocker(
            'Historical version still owns live container-image demand.')
    if resource_action_evidence.count != 0:
        raise NormalizationBlocker(
            'Historical version still has a typed resource-action launch '
            'root.')
    if legacy_controller_evidence.count != 0:
        raise NormalizationBlocker(
            'The legacy remote Serve-controller cluster still exists.')
    if request_evidence.count != 0:
        raise NormalizationBlocker(
            'Historical retirement has an active Serve mutation request.')
    if process_evidence.count != 0:
        raise NormalizationBlocker(
            'Historical retirement has a live Serve controller process.')
    return {
        **candidate.dependency_facts,
        'service_pool': service.get('pool'),
        'service_resource_action_mode': service.get('resource_action_mode'),
        'service_resource_action_mode_changed_at':
            service.get('resource_action_mode_changed_at'),
        'strictly_newer_committed_version': min(row.identity[1] for row in newer
                                               ),
        'recovery_version': recovery_version,
        'config_protocol_active': config_protocol_active,
        'recovery_config_valid': config_protocol_active,
        **cleanup_plan.global_facts,
        **cleanup_facts,
        **receipt_evidence.facts,
        'predecessor_receipt_requirement_satisfied': True,
        'image_demand_count': image_evidence.count,
        'image_demand_sha256': image_evidence.digest,
        'resource_action_root_count': resource_action_evidence.count,
        'resource_action_root_sha256': resource_action_evidence.digest,
        'legacy_controller_cluster_count': legacy_controller_evidence.count,
        'legacy_controller_cluster_sha256': legacy_controller_evidence.digest,
        'legacy_controller_cluster_absent': True,
        'serve_consolidation_mode_proved': True,
        'parent_non_pool_proved': True,
        'resource_action_mode_legacy_inert': True,
        'placement_catalog_absent': True,
        'cleanup_dependency_typed': True,
        'bridge_replica_dependency_absent': True,
        'unversioned_replica_dependency_absent': True,
        _SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT:
            dict(stale_placeholder_proof),
        'incomplete_staged_config_dependency_absent': True,
        'serve_mutation_request_count': request_evidence.count,
        'serve_mutation_request_sha256': request_evidence.digest,
        'process_quiescence_count': process_evidence.count,
        'process_quiescence_sha256': process_evidence.digest,
        'process_quiescence_pod_uid': api_pod_identity.pod_uid,
        'sole_api_instance_id': str(api_pod_identity.instance_id),
        'sole_api_pod_sha256': api_pod_identity.digest,
        'sole_recreate_api_pod_proved': True,
        'controller_hold_required': True,
    }


def _service_requires_normalization_receipt(
        service: dict[str, Any] | None) -> bool:
    if service is None:
        return False
    status_value = service.get('status')
    try:
        status = serve_state.ServiceStatus(status_value)
    except (TypeError, ValueError) as exc:
        raise NormalizationBlocker(
            'Affected service has an unknown persisted status.') from exc
    return status not in serve_state.ServiceStatus.terminal_statuses()


def _has_loadable_result(row: _RowWork) -> bool:
    return (row.classification, row.outcome) in _LOADABLE_RESULT_OUTCOMES


def _validate_supported_receipt_target(
        service_name: str, service: dict[str, Any],
        service_versions: list[_RowWork]) -> tuple[int, int]:
    """Prove the requested run can be acknowledged by the controller."""
    current_version = service.get('current_version')
    if type(current_version) is not int or current_version < 1:
        raise NormalizationBlocker(
            'Affected service current_version is not a positive integer.')
    current_rows = [
        row for row in service_versions
        if row.identity == (service_name, current_version) and row.result.get(
            'yaml_content') is not None and row.result.get('retired_at') is None
    ]
    if len(current_rows) != 1 or not _has_loadable_result(current_rows[0]):
        raise NormalizationBlocker(
            'Affected service current_version is not a surviving committed '
            'explicit-result row.')

    recovery_version = _select_recovery_version(service_versions)
    recovery_rows = [
        row for row in service_versions
        if recovery_version is not None and row.identity == (service_name,
                                                             recovery_version)
        and row.result.get('yaml_content') is not None and row.result.get(
            'retired_at') is None and row.result.get('quarantined_at') is None
    ]
    if len(recovery_rows) != 1 or not _has_loadable_result(recovery_rows[0]):
        raise NormalizationBlocker(
            'Affected service quarantine-aware recovery version is not a '
            'nonquarantined committed explicit-result row.')
    expected_kind = _parent_workload_kind(service)
    logical_semantics = service.get('logical_replica_semantics')
    if (type(logical_semantics) is not int or logical_semantics not in (0, 1)):
        raise NormalizationBlocker(
            'Affected service has an invalid durable logical-replica fence.')
    expected_replica_unit = (placement_policy.REPLICA_UNIT_LOGICAL
                             if logical_semantics == 1 else
                             placement_policy.REPLICA_UNIT_PHYSICAL_BACKEND)
    for label, row in (('current', current_rows[0]),
                       ('quarantine-aware recovery', recovery_rows[0])):
        projection = row.analysis.contract_projection
        if not isinstance(projection, dict):
            raise NormalizationBlocker(
                f'Affected service {label} target has no contract projection.')
        if projection.get('workload_kind') != expected_kind:
            raise NormalizationBlocker(
                f'Affected service {label} target disagrees with the live '
                'parent pool discriminator.')
        if projection.get('replica_unit') != expected_replica_unit:
            raise NormalizationBlocker(
                f'Affected service {label} target disagrees with the durable '
                'logical-replica fence.')
    assert recovery_version is not None
    return current_version, recovery_version


def _prepare_supported_rows(
        rows: list[_RowWork], service_rows: dict[str, dict[str,
                                                           Any]]) -> set[str]:
    candidate_services: set[str] = set()
    for row in rows:
        if row.classification not in _SUPPORTED_NORMALIZATION_SOURCES:
            continue
        if row.analysis.result_bytes is None:
            raise NormalizationBlocker(
                'Supported normalization has no result payload.')
        row.result['spec'] = row.analysis.result_bytes
        row.outcome = 'changed'
        service = service_rows.get(row.identity[0])
        if _service_requires_normalization_receipt(service):
            candidate_services.add(row.identity[0])

    affected_services: set[str] = set()
    for service_name in sorted(candidate_services):
        service = service_rows[service_name]
        service_versions = [
            row for row in rows if row.identity[0] == service_name
        ]
        current_version, recovery_version = (_validate_supported_receipt_target(
            service_name, service, service_versions))
        for row in service_versions:
            if row.classification not in _SUPPORTED_NORMALIZATION_SOURCES:
                continue
            row.dependency_facts.update({
                'receipt_current_version': current_version,
                'receipt_recovery_version': recovery_version,
                'receipt_loadable_result_proved': True,
                'receipt_parent_contract_proved': True,
            })
        affected_services.add(service_name)
    return affected_services


def _prepare_retirement_rows(
        rows: list[_RowWork], service_rows: dict[str,
                                                 dict[str,
                                                      Any]], run_id: uuid.UUID,
        retired_at: float, image_evidence: dict[tuple[str, int],
                                                _ExternalEvidence],
        resource_action_evidence: dict[tuple[str, int], _ExternalEvidence],
        legacy_controller_evidence: _ExternalEvidence,
        process_evidence: _ExternalEvidence, api_pod_identity: _ApiPodIdentity,
        request_evidence: _ExternalEvidence, cleanup_plan: _CleanupIntentPlan,
        receipt_evidence: _PredecessorReceiptEvidence) -> set[str]:
    affected_services: set[str] = set()
    retiring_identities = frozenset(
        row.identity
        for row in rows
        if row.classification is Classification.HISTORICAL_PHYSICAL_PER_GPU)
    if any(row.classification in _SUPPORTED_NORMALIZATION_SOURCES
           for row in rows):
        raise NormalizationBlocker(
            'Supported fieldless or explicit-v1 rows remain; run '
            '--apply-supported before historical retirement.')
    stale_placeholder_proofs = _prepare_stale_placeholder_proofs(
        rows, service_rows, image_evidence, resource_action_evidence,
        retiring_identities)
    dependency_facts: dict[tuple[str, int], dict[str, Any]] = {}
    for row in rows:
        if row.classification is not Classification.HISTORICAL_PHYSICAL_PER_GPU:
            continue
        identity = row.identity
        evidence = image_evidence.get(identity)
        action_evidence = resource_action_evidence.get(identity)
        stale_placeholder_proof = stale_placeholder_proofs.get(identity[0])
        if (evidence is None or action_evidence is None or
                stale_placeholder_proof is None):
            raise NormalizationBlocker(
                'Historical candidate changed after external preflight.')
        dependency_facts[identity] = _retirement_dependency_facts(
            row, rows, service_rows, evidence, action_evidence,
            legacy_controller_evidence, retiring_identities, process_evidence,
            api_pod_identity, request_evidence, cleanup_plan, receipt_evidence,
            stale_placeholder_proof)
    # Mutate only after every candidate has proved safety against the final
    # post-retirement fleet.  Otherwise an earlier candidate could cite a
    # later candidate that this same transaction also retires as its survivor.
    for row in rows:
        if row.identity not in retiring_identities:
            continue
        identity = row.identity
        row.dependency_facts = dependency_facts[identity]
        row.result.update({
            'retired_yaml_content': row.original['yaml_content'],
            'yaml_content': None,
            'spec': _RETIRED_SPEC_BYTES,
            'retired_at': retired_at,
            'retirement_reason': _RETIREMENT_REASON,
            'retirement_run_id': run_id,
        })
        row.outcome = 'retired'
        service = service_rows.get(identity[0])
        if _service_requires_normalization_receipt(service):
            affected_services.add(identity[0])
    return affected_services


def _cas_service_receipt_request(session: orm.Session, service_name: str,
                                 service: dict[str,
                                               Any], run_id: uuid.UUID) -> None:
    predicates = [serve_state.services_table.c.name == service_name]
    for column_name in ('hash', 'lifecycle_epoch'):
        column = serve_state.services_table.c[column_name]
        value = service.get(column_name)
        predicates.append(
            column.is_(None) if value is None else column == value)
    result = session.execute(serve_state.services_table.update().where(
        *predicates).values(placement_normalization_requested_run_id=run_id,
                            placement_normalization_loaded_run_id=None,
                            placement_normalization_loaded_image_commit=None,
                            placement_normalization_loaded_controller_pid=None,
                            placement_normalization_loaded_controller_ip=None,
                            placement_normalization_loaded_boot_id=None,
                            placement_normalization_loaded_at=None))
    if result.rowcount != 1:
        raise NormalizationBlocker(
            f'Service owner CAS failed for {service_name!r}.')


def _cas_version_result(session: orm.Session, row: _RowWork) -> None:
    table = serve_state.version_specs_table
    predicates = [
        table.c.service_name == row.identity[0],
        table.c.version == row.identity[1],
        table.c.spec == _as_bytes(row.original['spec']),
    ]
    if row.outcome == 'changed':
        values = {'spec': row.result['spec']}
    elif row.outcome == 'retired':
        predicates.extend((
            table.c.yaml_content == row.original['yaml_content'],
            table.c.retired_at.is_(None),
            table.c.retired_yaml_content.is_(None),
            table.c.retirement_reason.is_(None),
            table.c.retirement_run_id.is_(None),
            table.c.quarantined_at.is_(None),
        ))
        values = {
            'spec': row.result['spec'],
            'yaml_content': None,
            'retired_yaml_content': row.result['retired_yaml_content'],
            'retired_at': row.result['retired_at'],
            'retirement_reason': row.result['retirement_reason'],
            'retirement_run_id': row.result['retirement_run_id'],
        }
    else:
        return
    changed = session.execute(
        table.update().where(*predicates).values(**values)).rowcount
    if changed != 1:
        raise NormalizationBlocker(f'Version CAS failed for {row.identity!r}.')


def _cas_cleanup_intent_results(session: orm.Session,
                                plan: _CleanupIntentPlan) -> None:
    """Adopt exactly the planned provisional intents by full-row CAS."""
    table = serve_state.ephemeral_storage_cleanup_intents_table
    changed_count = 0
    for work in plan.rows:
        if not work.adopted:
            continue
        predicates = []
        for column in table.columns:
            value = work.original[column.name]
            predicates.append(
                column.is_(None) if value is None else column == value)
        changed = session.execute(
            table.update().where(*predicates).values(provisional=0)).rowcount
        if changed != 1:
            raise NormalizationBlocker(
                f'Cleanup-intent CAS failed for {work.identity!r}.')
        changed_count += 1
    if changed_count != plan.global_facts['cleanup_intent_adopted_count']:
        raise NormalizationBlocker(
            'Cleanup-intent CAS count disagrees with the frozen repair plan.')


def _verify_cleanup_intent_postimages(session: orm.Session,
                                      plan: _CleanupIntentPlan,
                                      row_bound: int) -> None:
    service_names = frozenset(work.identity[0] for work in plan.rows)
    observed_rows = _read_cleanup_intent_inventory(session, service_names,
                                                   row_bound)
    observed_by_identity = {
        (row['service_name'], row['resource_scope'], row['storage_generation']):
            _row_sha256(row) for row in observed_rows
    }
    expected_by_identity = {
        work.identity: _row_sha256(work.result) for work in plan.rows
    }
    if observed_by_identity != expected_by_identity:
        raise NormalizationBlocker(
            'Locked cleanup-intent postimages do not match the frozen plan.')
    if (_cleanup_intent_inventory_sha256(observed_rows)
            != plan.global_facts['cleanup_intent_post_inventory_sha256']):
        raise NormalizationBlocker(
            'Locked cleanup-intent postimage digest does not match the '
            'retirement ledger.')


def _insert_ledger(session: orm.Session, rows: list[_RowWork], *,
                   run_id: uuid.UUID, mode: ApplyMode, row_bound: int,
                   started_at: float, completed_at: float,
                   freeze_evidence_sha256: str, pre_digest: str,
                   post_digest: str) -> None:
    counts = collections.Counter(row.ledger_classification for row in rows)
    session.execute(
        serve_state.placement_normalization_runs_table.insert().values(
            run_id=run_id,
            mode=mode.value,
            normalizer_version=(
                placement_normalization_identity.format_normalizer_identity(
                    sky.__commit__)),
            schema_revision=_SCHEMA_REVISION,
            release_version=sky.__version__,
            started_at=started_at,
            completed_at=completed_at,
            row_bound=row_bound,
            row_count=len(rows),
            classification_counts=dict(counts),
            pre_inventory_sha256=pre_digest,
            post_inventory_sha256=post_digest,
            freeze_evidence_sha256=freeze_evidence_sha256))
    values = []
    for row in rows:
        original_columns = _column_sha256s(row.original)
        result_columns = _column_sha256s(row.result)
        result_spec = _as_bytes(row.result['spec'])
        # These exact owner facts are copied into dependency_facts at scan
        # time; NULL means there was no live parent row.
        service_hash = row.dependency_facts.get('service_hash')
        lifecycle_epoch = row.dependency_facts.get('service_lifecycle_epoch')
        values.append({
            'run_id': run_id,
            'service_name': row.identity[0],
            'version': row.identity[1],
            'classification': row.ledger_classification,
            'outcome': row.outcome,
            'original_spec_sha256': row.analysis.source_sha256,
            'result_spec_sha256': _sha256(result_spec),
            'original_row_sha256': _row_sha256(row.original),
            'result_row_sha256': _row_sha256(row.result),
            'original_column_sha256s': original_columns,
            'result_column_sha256s': result_columns,
            'contract_projection': row.analysis.contract_projection,
            'service_hash': service_hash,
            'service_lifecycle_epoch': lifecycle_epoch,
            'dependency_facts': row.dependency_facts,
        })
    if values:
        session.execute(serve_state.placement_normalization_rows_table.insert(),
                        values)


def _verify_inserted_ledger(session: orm.Session, run_id: uuid.UUID,
                            row_bound: int) -> None:
    """Validate the complete planned manifest before any transaction commits."""
    run = session.execute(
        sqlalchemy.select(serve_state.placement_normalization_runs_table).where(
            serve_state.placement_normalization_runs_table.c.run_id ==
            run_id)).mappings().one()
    entries = session.execute(
        sqlalchemy.select(serve_state.placement_normalization_rows_table).where(
            serve_state.placement_normalization_rows_table.c.run_id ==
            run_id).order_by(
                serve_state.placement_normalization_rows_table.c.service_name,
                serve_state.placement_normalization_rows_table.c.version).limit(
                    row_bound + 1)).mappings().all()
    if len(entries) > row_bound:
        raise NormalizationBlocker(
            'Inserted normalization ledger exceeds its explicit row bound.')
    mismatches = placement_normalization_manifest.manifest_mismatches(
        dict(run), [dict(entry) for entry in entries])
    if mismatches:
        raise NormalizationBlocker(
            'Inserted normalization ledger is invalid; first mismatch is '
            f'{mismatches[0]!r}.')


def _is_sha256(value: Any) -> bool:
    return (isinstance(value, str) and len(value) == 64 and
            all(character in '0123456789abcdef' for character in value))


def _frozen_version_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Project a live row onto the immutable protocol-4 column contract."""
    try:
        return {
            column: row[column]
            for column in placement_normalization_manifest.VERSION_SPEC_COLUMNS
        }
    except KeyError as exc:
        raise NormalizationBlocker(
            f'Live version row lacks frozen column {exc.args[0]!r}.') from exc


def _is_post_terminal_explicit_v2(work: _RowWork, completed_at: float) -> bool:
    created_at = work.original.get('created_at')
    return (work.classification is Classification.EXPLICIT_V2 and
            isinstance(work.original.get('yaml_content'), str) and
            work.original.get('quarantined_at') is None and
            work.original.get('retired_at') is None and
            isinstance(created_at,
                       (int, float)) and not isinstance(created_at, bool) and
            math.isfinite(created_at) and created_at > completed_at)


def _is_post_terminal_placeholder(work: _RowWork) -> bool:
    return (work.classification is Classification.PLACEHOLDER and
            work.original.get('created_at') is None and
            _as_bytes(work.original['spec']) == _RETIRED_SPEC_BYTES and all(
                work.original.get(column) is None
                for column in _STALE_PLACEHOLDER_NULL_COLUMNS))


def _terminal_fleet_mismatches(
    session: orm.Session,
    run: Mapping[str, Any],
    entries: Sequence[Mapping[str, Any]],
    rows: Sequence[_RowWork],
) -> tuple[dict[str, Any], ...]:
    """Validate post-terminal rows outside retired candidate services."""
    mismatches = list(
        _terminal_current_inventory_mismatches(session, run, entries))
    candidate_services = {
        str(manifest_entry['service_name'])
        for manifest_entry in entries
        if manifest_entry.get('classification') == (
            placement_normalization_manifest.ManifestClassification.
            HISTORICAL_PHYSICAL_PER_GPU.value) and
        manifest_entry.get('outcome') == 'retired'
    }
    manifest_by_identity = {
        (str(manifest_entry['service_name']), int(manifest_entry['version'])): manifest_entry
        for manifest_entry in entries
        if str(manifest_entry['service_name']) not in candidate_services
    }
    high_waters: dict[tuple[str, Any], int] = {}
    for entry in manifest_by_identity.values():
        boundary = (str(entry['service_name']), entry.get('service_hash'))
        high_waters[boundary] = max(high_waters.get(boundary, 0),
                                    int(entry['version']))

    relevant_names = sorted({
        *(str(entry['service_name']) for entry in entries),
        *(work.identity[0] for work in rows),
    })
    current_hash_observations = {
        name: placement_normalization_manifest.ServiceHashObservation(
            False, None) for name in relevant_names
    }
    service_table = serve_state.services_table
    if relevant_names:
        for row in session.execute(
                sqlalchemy.select(
                    service_table.c.name, service_table.c.hash).where(
                        service_table.c.name.in_(relevant_names))).all():
            current_hash_observations[str(row.name)] = (
                placement_normalization_manifest.ServiceHashObservation(
                    True, row.hash))

    current_hashes: dict[str, str | None] = {}
    invalid_hash_services: set[str] = set()
    for service_name, observation in current_hash_observations.items():
        try:
            current_hashes[service_name] = observation.validated_value()
        except ValueError:
            mismatches.append({
                'service_name': service_name,
                'service_hash': observation.value,
                'reason': 'invalid_current_parent_hash_observation',
            })
            invalid_hash_services.add(service_name)

    completed_at = float(run['completed_at'])
    current_by_identity = {work.identity: work for work in rows}
    for work in rows:
        service_name, version = work.identity
        if service_name in candidate_services:
            continue
        if service_name in invalid_hash_services:
            continue
        service_hash = current_hashes.get(service_name)
        tracked_entry = manifest_by_identity.get(work.identity)
        if service_hash is None:
            mismatches.append({
                'service_name': service_name,
                'version': version,
                'service_hash': service_hash,
                'reason': 'current_row_without_parent_service',
            })
            continue
        if (tracked_entry is not None and
                tracked_entry.get('service_hash') == service_hash):
            if tracked_entry.get('classification') == (
                    placement_normalization_manifest.ManifestClassification.
                    PLACEHOLDER.value):
                frozen_row = _frozen_version_row(work.original)
                unchanged = (_is_post_terminal_placeholder(work) and
                             _row_sha256(frozen_row)
                             == tracked_entry.get('result_row_sha256'))
                if not (unchanged or
                        _is_post_terminal_explicit_v2(work, completed_at)):
                    mismatches.append({
                        'service_name': service_name,
                        'version': version,
                        'service_hash': service_hash,
                        'reason': 'invalid_terminal_placeholder_transition',
                    })
            elif (work.analysis.source_sha256
                  != tracked_entry.get('result_spec_sha256')):
                mismatches.append({
                    'service_name': service_name,
                    'version': version,
                    'service_hash': service_hash,
                    'reason': 'tracked_result_spec_drift',
                })
            continue

        boundary = (service_name, service_hash)
        high_water = high_waters.get(boundary)
        if high_water is not None:
            allowed = (version > high_water and
                       (_is_post_terminal_placeholder(work) or
                        _is_post_terminal_explicit_v2(work, completed_at)))
            reason = 'invalid_same_incarnation_post_terminal_row'
        else:
            allowed = _is_post_terminal_explicit_v2(work, completed_at)
            reason = 'invalid_new_incarnation_version_row'
        if not allowed:
            mismatches.append({
                'service_name': service_name,
                'version': version,
                'service_hash': service_hash,
                'reason': reason,
            })

    for identity, tracked_entry in sorted(manifest_by_identity.items()):
        if identity[0] in invalid_hash_services:
            continue
        current = current_by_identity.get(identity)
        if current is not None and current_hashes.get(
                identity[0]) == tracked_entry.get('service_hash'):
            continue
        if (current is None and current_hashes.get(
                identity[0]) == tracked_entry.get('service_hash') and
                tracked_entry.get('service_hash') is not None):
            mismatches.append({
                'service_name': identity[0],
                'version': identity[1],
                'service_hash': tracked_entry.get('service_hash'),
                'reason': 'tracked_row_absent_from_current_inventory',
            })
    return tuple(mismatches)


def _prior_ledger_mismatches(
        session: orm.Session,
        rows: list[_RowWork]) -> tuple[dict[str, Any], ...]:
    ledger = serve_state.placement_normalization_rows_table
    runs = serve_state.placement_normalization_runs_table
    latest_run = session.execute(
        sqlalchemy.select(runs).order_by(
            runs.c.completed_at.desc(),
            runs.c.run_id.desc()).limit(1)).mappings().first()
    mismatches: list[dict[str, Any]] = []
    latest_manifest_identities: set[tuple[str, int, Any, Any]] | None = None
    latest_manifest_rows: dict[tuple[str, int], tuple[Any, Any]] | None = None
    if latest_run is not None:
        ledger_entries = [
            dict(row) for row in session.execute(
                sqlalchemy.select(ledger).where(
                    ledger.c.run_id == latest_run['run_id']).order_by(
                        ledger.c.service_name, ledger.c.version).limit(
                            _MAX_INVENTORY_ROWS + 1)).mappings().all()
        ]
        latest_run_dict = dict(latest_run)
        mismatches.extend(
            placement_normalization_manifest.manifest_mismatches(
                latest_run_dict, ledger_entries))
        if mismatches:
            return tuple(mismatches)
        if placement_normalization_manifest.is_terminal_protocol4_manifest(
                latest_run_dict, ledger_entries):
            return _terminal_fleet_mismatches(session, latest_run_dict,
                                              ledger_entries, rows)
        latest_manifest_identities = {
            (str(entry['service_name']), int(entry['version']),
             entry.get('service_hash'), entry.get('service_lifecycle_epoch'))
            for entry in ledger_entries
            if isinstance(entry.get('service_name'), str) and
            type(entry.get('version')) is int
        }
        latest_manifest_rows = {
            (identity[0], identity[1]): (identity[2], identity[3])
            for identity in latest_manifest_identities
        }
    current_services = serve_state.services_table.alias(
        'placement_normalization_current_service')
    current_versions = serve_state.version_specs_table.alias(
        'placement_normalization_current_version')
    ranked = sqlalchemy.select(
        ledger.c.service_name,
        ledger.c.version,
        ledger.c.service_hash,
        ledger.c.service_lifecycle_epoch,
        ledger.c.result_spec_sha256,
        runs.c.completed_at,
        sqlalchemy.func.row_number().over(
            partition_by=(ledger.c.service_name, ledger.c.version,
                          ledger.c.service_hash,
                          ledger.c.service_lifecycle_epoch),
            order_by=(runs.c.completed_at.desc(),
                      ledger.c.run_id.desc())).label('row_rank'),
    ).select_from(
        ledger.join(runs, ledger.c.run_id == runs.c.run_id).join(
            current_versions,
            sqlalchemy.and_(
                current_versions.c.service_name == ledger.c.service_name,
                current_versions.c.version == ledger.c.version)).outerjoin(
                    current_services,
                    current_services.c.name == ledger.c.service_name)).where(
                        ledger.c.service_hash.is_not_distinct_from(
                            current_services.c.hash),
                        ledger.c.service_lifecycle_epoch.is_not_distinct_from(
                            current_services.c.lifecycle_epoch)).subquery()
    latest = {
        (str(row.service_name), int(row.version), row.service_hash, row.service_lifecycle_epoch):
            (float(row.completed_at), str(row.result_spec_sha256))
        for row in session.execute(
            sqlalchemy.select(ranked).where(ranked.c.row_rank == 1)).all()
    }
    current_row_identities = {work.identity for work in rows}
    for work in rows:
        service_hash = work.dependency_facts.get('service_hash')
        lifecycle_epoch = work.dependency_facts.get('service_lifecycle_epoch')
        identity = (*work.identity, service_hash, lifecycle_epoch)
        if (latest_manifest_identities is not None and
                identity not in latest_manifest_identities):
            mismatches.append({
                'service_name': work.identity[0],
                'version': work.identity[1],
                'service_hash': service_hash,
                'service_lifecycle_epoch': lifecycle_epoch,
                'reason': 'untracked_current_row',
            })
        prior = latest.get(identity)
        if prior is not None and prior[1] != work.analysis.source_sha256:
            mismatches.append({
                'service_name': work.identity[0],
                'version': work.identity[1],
                'service_hash': service_hash,
                'service_lifecycle_epoch': lifecycle_epoch,
                'last_completed_at': prior[0],
                'reason': 'tracked_result_spec_drift',
            })
    if latest_manifest_rows is not None:
        for service_name, version in sorted(
                set(latest_manifest_rows) - current_row_identities):
            service_hash, lifecycle_epoch = latest_manifest_rows[(service_name,
                                                                  version)]
            mismatches.append({
                'service_name': service_name,
                'version': version,
                'service_hash': service_hash,
                'service_lifecycle_epoch': lifecycle_epoch,
                'reason': 'tracked_row_absent_from_current_inventory',
            })
    return tuple(mismatches)


def _require_prior_ledger_consistency(
        mismatches: tuple[dict[str, Any], ...]) -> None:
    nonblocking_reasons = frozenset({
        'untracked_current_row',
        'tracked_row_absent_from_current_inventory',
    })
    blocking = tuple(mismatch for mismatch in mismatches
                     if mismatch.get('reason') not in nonblocking_reasons)
    if blocking:
        raise NormalizationBlocker(
            'Prior normalization ledger does not match the locked inventory; '
            f'first mismatch is {blocking[0]!r}.')


def _validate_operator_inputs(
    mode: ApplyMode | None,
    row_bound: int,
    freeze_evidence_sha256: str | None,
    approved_loaded_image_commits: Sequence[str],
) -> frozenset[str]:
    if mode is not None and not isinstance(mode, ApplyMode):
        raise ValueError('mode must be an ApplyMode value or None.')
    if (type(row_bound) is not int or
            not 1 <= row_bound <= _MAX_INVENTORY_ROWS):
        raise ValueError(
            f'row_bound must be an integer in [1, {_MAX_INVENTORY_ROWS}].')
    if mode is not ApplyMode.RETIRE_TERMINAL_HISTORICAL:
        if approved_loaded_image_commits:
            raise ValueError('Approved loaded image commits are accepted only '
                             'for historical retirement.')
        approved_commits: frozenset[str] = frozenset()
    else:
        approved_commits = _validate_approved_loaded_image_commits(
            approved_loaded_image_commits)
    if mode is None:
        return approved_commits
    if (not isinstance(freeze_evidence_sha256, str) or
            len(freeze_evidence_sha256) != 64 or
            any(character not in '0123456789abcdef'
                for character in freeze_evidence_sha256)):
        raise ValueError(
            'Apply mode requires a lowercase SHA-256 freeze evidence digest.')
    # Refuse a dirty checkout, tag, or abbreviated revision before acquiring
    # any external evidence or database lock.  Every durable writer identity
    # must be attributable to one exact released image commit.
    placement_normalization_identity.format_normalizer_identity(sky.__commit__)
    return approved_commits


def _read_timestamp(now: Callable[[], float], label: str) -> float:
    value = now()
    if (not isinstance(value, (int, float)) or isinstance(value, bool) or
            not math.isfinite(value) or value < 0):
        raise NormalizationBlocker(f'{label} timestamp is invalid.')
    return float(value)


@dataclasses.dataclass(frozen=True)
class _WriterDatabaseAuthorityLease:
    """One canonical database authority owned by one physical connection."""

    connection: sqlalchemy.engine.Connection
    authority: (placement_normalization_authority.
                PlacementNormalizationDatabaseAuthority)


@contextlib.contextmanager
def _reader_database_authority(
    engine: sqlalchemy.engine.Engine,) -> Iterator[orm.Session]:
    """Bind one preliminary read transaction to the canonical DB schema."""
    with orm.Session(engine) as session, session.begin():
        try:
            authority = (placement_normalization_authority.
                         assert_reader_database_authority(session.connection()))
            placement_normalization_authority.bind_session_to_authority(
                session, authority)
        except (placement_normalization_authority.
                PlacementNormalizationAuthorityError,
                sqlalchemy.exc.SQLAlchemyError) as exc:
            raise NormalizationBlocker(
                'Placement-normalization database read authority is absent '
                'or invalid.') from exc
        yield session


def _acquire_writer_session_lock(
    connection: sqlalchemy.engine.Connection,
) -> (placement_normalization_authority.PlacementNormalizationDatabaseAuthority
     ):
    """Acquire and prove one typed cross-protocol database authority."""
    busy = False
    try:
        with connection.begin():
            acquired = connection.execute(
                sqlalchemy.text('SELECT pg_catalog.pg_try_advisory_lock('
                                'pg_catalog.hashtextextended(:name, 0))'), {
                                    'name': _ADVISORY_LOCK_NAME
                                }).scalar_one()
            if acquired is not True:
                busy = True
                raise NormalizationBlocker(
                    'Another placement-normalization writer or schema '
                    'migration owns the advisory authority.')
            authority = (placement_normalization_authority.
                         assert_writer_database_authority(
                             connection, _ADVISORY_LOCK_NAME))
    except NormalizationBlocker:
        if not busy and not connection.closed:
            # The acquisition query or the active exact-session probe may
            # have changed a backend-level lock before the client observed
            # its result. Invalidating is the only safe pool cleanup for an
            # uncertain session-level side effect.
            connection.invalidate()
        raise
    except (placement_normalization_authority.
            PlacementNormalizationAuthorityError,
            sqlalchemy.exc.SQLAlchemyError) as exc:
        if not connection.closed:
            connection.invalidate()
        raise NormalizationBlocker(
            'Placement-normalization database write authority is absent '
            'or invalid.') from exc
    except BaseException:
        if not connection.closed:
            connection.invalidate()
        raise
    return authority


@contextlib.contextmanager
def _writer_database_authority(
    engine: sqlalchemy.engine.Engine,
) -> Iterator[_WriterDatabaseAuthorityLease]:
    """Own one exact writer session from acquisition through final release."""
    connection = engine.connect().execution_options(
        isolation_level='SERIALIZABLE')
    lease_owned = False
    try:
        # The operator is rare and its session-level lock must never survive
        # on a pooled backend.  Detaching makes close() physically terminate
        # this DBAPI connection even if an asynchronous exception lands in the
        # narrow caller boundary immediately after lock acquisition.
        connection.detach()
        authority = _acquire_writer_session_lock(connection)
        lease_owned = True
        yield _WriterDatabaseAuthorityLease(connection, authority)
    finally:
        try:
            if lease_owned and not connection.closed:
                with connection.begin():
                    (placement_normalization_authority.
                     release_writer_session_lock(connection,
                                                 _ADVISORY_LOCK_NAME))
            elif not connection.closed:
                # Acquisition was attempted but ownership was not durably
                # observed by this frame.  Invalidate the detached backend;
                # it is never eligible to re-enter QueuePool.
                connection.invalidate()
        except BaseException:
            if not connection.closed:
                # Never return a backend with an uncertain or reentrant
                # session-level authority hold to any subsequent caller.
                connection.invalidate()
            raise
        finally:
            connection.close()


def _acquire_writer_table_locks(session: orm.Session, schema: str) -> None:
    bind = session.get_bind()
    quote = bind.dialect.identifier_preparer.quote
    relations = ', '.join(f'{quote(schema)}.{quote(relation)}'
                          for relation in ('services', 'version_specs',
                                           'replicas',
                                           'ephemeral_storage_cleanup_intents'))
    session.execute(
        sqlalchemy.text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT_MS}ms'"))
    session.execute(
        sqlalchemy.text(
            f"SET LOCAL statement_timeout = '{_STATEMENT_TIMEOUT_MS}ms'"))
    session.execute(
        sqlalchemy.text(f'LOCK TABLE {relations} IN SHARE ROW EXCLUSIVE MODE'))


def _verify_version_postimages(session: orm.Session,
                               expected_rows: list[_RowWork], row_bound: int,
                               expected_digest: str) -> None:
    observed_rows, _ = _scan_inventory(session, row_bound)
    expected_by_identity = {
        row.identity: _row_sha256(row.result) for row in expected_rows
    }
    observed_by_identity = {
        row.identity: _row_sha256(row.original) for row in observed_rows
    }
    if observed_by_identity != expected_by_identity:
        raise NormalizationBlocker(
            'Locked version postimages do not match the planned fleet.')
    if _fleet_sha256(observed_rows, result=False) != expected_digest:
        raise NormalizationBlocker(
            'Locked version postimage fleet digest does not match the '
            'manifest.')


def _verify_service_receipts(session: orm.Session, service_names: set[str],
                             run_id: uuid.UUID) -> None:
    if not service_names:
        return
    table = serve_state.services_table
    rows = session.execute(
        sqlalchemy.select(
            table.c.name,
            table.c.placement_normalization_requested_run_id,
            table.c.placement_normalization_loaded_run_id,
            table.c.placement_normalization_loaded_image_commit,
            table.c.placement_normalization_loaded_controller_pid,
            table.c.placement_normalization_loaded_controller_ip,
            table.c.placement_normalization_loaded_boot_id,
            table.c.placement_normalization_loaded_at,
        ).where(table.c.name.in_(sorted(service_names)))).all()
    expected_cleared = (None,) * 6
    observed = {
        str(row.name): (row.placement_normalization_requested_run_id,
                        row.placement_normalization_loaded_run_id,
                        row.placement_normalization_loaded_image_commit,
                        row.placement_normalization_loaded_controller_pid,
                        row.placement_normalization_loaded_controller_ip,
                        row.placement_normalization_loaded_boot_id,
                        row.placement_normalization_loaded_at) for row in rows
    }
    if set(observed) != service_names or any(
            values[0] != run_id or values[1:] != expected_cleared
            for values in observed.values()):
        raise NormalizationBlocker(
            'Service normalization receipt postimages are incomplete.')


def run_operator(
    *,
    engine: sqlalchemy.engine.Engine | None = None,
    mode: ApplyMode | None = None,
    row_bound: int,
    freeze_evidence_sha256: str | None = None,
    approved_loaded_image_commits: Sequence[str] = (),
    image_evidence_getter: Callable[[str, int],
                                    _ExternalEvidence] = _image_demand_evidence,
    request_evidence_getter: Callable[
        [sqlalchemy.engine.Engine],
        _ExternalEvidence] = _active_serve_request_evidence,
    process_evidence_getter: Callable[
        [frozenset[_ProcessTarget], str],
        _ExternalEvidence] = _serve_controller_process_evidence,
    resource_action_evidence_getter: Callable[
        [sqlalchemy.engine.Engine, frozenset[_ResourceActionTarget]],
        Mapping[tuple[str, int],
                _ExternalEvidence]] = _resource_action_evidence,
    api_pod_checker: Callable[[sqlalchemy.engine.Engine],
                              _ApiPodIdentity] = _require_sole_recreate_api_pod,
    controller_hold_checker: Callable[
        [], bool] = maintenance.is_controller_hold_active,
    consolidation_mode_checker: Callable[[],
                                         bool] = _serve_controllers_are_local,
    legacy_controller_evidence_getter: Callable[
        [], _ExternalEvidence] = _legacy_serve_controller_cluster_evidence,
    now: Callable[[], float] = time.time,
) -> OperatorResult:
    """Dry-run or atomically apply one explicit normalization phase."""
    approved_commits = _validate_operator_inputs(mode, row_bound,
                                                 freeze_evidence_sha256,
                                                 approved_loaded_image_commits)
    if engine is None:
        engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'Placement normalization is supported only on PostgreSQL.')

    if mode is not None:
        # This read-only fence deliberately precedes every external evidence
        # getter.  It is repeated under the writer locks below to close two
        # concurrent first-retirement attempts.
        with _reader_database_authority(engine) as session:
            _require_no_terminal_protocol4_manifest(session)

    if mode is None:
        with _reader_database_authority(engine) as session:
            rows, service_rows = _scan_inventory(session, row_bound)
            prior_mismatches = _prior_ledger_mismatches(session, rows)
        _prepare_supported_rows(rows, service_rows)
        blockers = tuple({
            'service_name': row.identity[0],
            'version': row.identity[1],
            'reason': row.analysis.blocker_reason,
        } for row in rows if row.classification is Classification.BLOCKER)
        counts = collections.Counter(row.classification.value for row in rows)
        return OperatorResult(mode='dry_run',
                              dry_run=True,
                              row_count=len(rows),
                              classification_counts=dict(counts),
                              pre_inventory_sha256=_fleet_sha256(rows,
                                                                 result=False),
                              post_inventory_sha256=_fleet_sha256(rows,
                                                                  result=True),
                              changed_rows=sum(
                                  row.outcome == 'changed' for row in rows),
                              retired_rows=0,
                              blockers=blockers,
                              run_id=None,
                              prior_ledger_mismatches=prior_mismatches)

    assert freeze_evidence_sha256 is not None
    if mode is ApplyMode.RETIRE_TERMINAL_HISTORICAL:
        if consolidation_mode_checker() is not True:
            raise NormalizationBlocker(
                'Historical retirement requires Serve consolidation mode so '
                'every controller process is local to the fenced API pod.')
        if controller_hold_checker() is not True:
            raise NormalizationBlocker(
                'Historical retirement requires the explicit Serve controller '
                'hold to be active.')
    api_pod_before = _validate_api_pod_identity(
        api_pod_checker(engine), 'Sole Recreate API pod preflight')
    request_before = _validate_external_evidence(
        request_evidence_getter(engine), 'Serve request preflight')
    if request_before.count:
        raise NormalizationBlocker(
            'Active Serve mutation requests remain during the freeze.')

    preflight_images: dict[tuple[str, int], _ExternalEvidence] = {}
    preflight_candidate_identities: set[tuple[str, int]] = set()
    preflight_version_evidence_identities: set[tuple[str, int]] = set()
    preflight_process_targets: frozenset[_ProcessTarget] = frozenset()
    preflight_action_targets: frozenset[_ResourceActionTarget] = frozenset()
    resource_actions_before: dict[tuple[str, int], _ExternalEvidence] = {}
    process_before: _ExternalEvidence | None = None
    legacy_controller_before: _ExternalEvidence | None = None
    if mode is ApplyMode.RETIRE_TERMINAL_HISTORICAL:
        legacy_controller_before = _validate_external_evidence(
            legacy_controller_evidence_getter(),
            'Legacy Serve-controller cluster preflight')
        _require_stable_zero_evidence(
            legacy_controller_before, legacy_controller_before,
            'Legacy Serve-controller cluster evidence')
        with _reader_database_authority(engine) as session:
            preflight_rows, _ = _scan_inventory(session, row_bound)
        (preflight_process_targets, preflight_action_targets
        ) = _retirement_evidence_targets(preflight_rows)
        for row in _retirement_version_evidence_rows(preflight_rows):
            if row.classification is (
                    Classification.HISTORICAL_PHYSICAL_PER_GPU):
                preflight_candidate_identities.add(row.identity)
            preflight_version_evidence_identities.add(row.identity)
            preflight_images[row.identity] = _validate_external_evidence(
                image_evidence_getter(row.identity[0], row.identity[1]),
                'Container-image demand preflight')
            if preflight_images[row.identity].count:
                raise NormalizationBlocker(
                    'Historical candidate or same-service placeholder has '
                    'live container-image demand.')
        process_before = _validate_external_evidence(
            process_evidence_getter(preflight_process_targets,
                                    api_pod_before.pod_uid),
            'Serve controller process preflight')
        _require_stable_zero_evidence(process_before, process_before,
                                      'Serve controller process evidence')
        resource_actions_before = _validate_external_evidence_map(
            resource_action_evidence_getter(engine, preflight_action_targets),
            frozenset(preflight_version_evidence_identities),
            'Serve resource-action root preflight')
        _require_stable_zero_evidence_map(
            resource_actions_before, resource_actions_before,
            'Serve resource-action root evidence')

    started_at = _read_timestamp(now, 'Normalization start')
    run_id = uuid.uuid4()
    with _writer_database_authority(engine) as authority_lease:
        connection = authority_lease.connection
        database_schema = authority_lease.authority.schema
        with orm.Session(bind=connection) as session, session.begin():
            placement_normalization_authority.bind_session_to_authority(
                session, authority_lease.authority)
            _acquire_writer_table_locks(session, database_schema)
            _require_no_terminal_protocol4_manifest(session)
            rows, service_rows = _scan_inventory(session, row_bound)
            api_pod_locked = _validate_api_pod_identity(
                api_pod_checker(engine),
                'Sole Recreate API pod locked preflight')
            _require_stable_api_pod(api_pod_before, api_pod_locked)
            blocking_rows = [
                row for row in rows
                if row.classification is Classification.BLOCKER
            ]
            if blocking_rows:
                first = blocking_rows[0]
                raise NormalizationBlocker(
                    f'Inventory contains {len(blocking_rows)} blocker(s); '
                    'first '
                    f'is {first.identity!r}: '
                    f'{first.analysis.blocker_reason}')
            prior_mismatches = _prior_ledger_mismatches(session, rows)
            _require_prior_ledger_consistency(prior_mismatches)
            pre_digest = _fleet_sha256(rows, result=False)
            manifest_freeze_evidence_sha256 = freeze_evidence_sha256
            if mode is ApplyMode.SUPPORTED:
                affected_services = _prepare_supported_rows(rows, service_rows)
                cleanup_plan: _CleanupIntentPlan | None = None
            else:
                if consolidation_mode_checker() is not True:
                    raise NormalizationBlocker(
                        'Serve consolidation mode ended before historical '
                        'retirement acquired its writer locks.')
                if controller_hold_checker() is not True:
                    raise NormalizationBlocker(
                        'Serve controller hold ended before historical '
                        'retirement acquired its writer locks.')
                locked_identities = {
                    row.identity for row in rows if row.classification is
                    Classification.HISTORICAL_PHYSICAL_PER_GPU
                }
                if locked_identities != preflight_candidate_identities:
                    raise NormalizationBlocker(
                        'Historical candidates changed after external '
                        'preflight.')
                locked_version_evidence_identities = {
                    row.identity
                    for row in _retirement_version_evidence_rows(rows)
                }
                if (locked_version_evidence_identities
                        != preflight_version_evidence_identities):
                    raise NormalizationBlocker(
                        'Historical candidate placeholder inventory changed '
                        'after external preflight.')
                (locked_process_targets,
                 locked_action_targets) = _retirement_evidence_targets(rows)
                if (locked_process_targets != preflight_process_targets or
                        locked_action_targets != preflight_action_targets):
                    raise NormalizationBlocker(
                        'Historical external-proof targets changed after '
                        'preflight.')
                assert process_before is not None
                process_locked = _validate_external_evidence(
                    process_evidence_getter(locked_process_targets,
                                            api_pod_locked.pod_uid),
                    'Serve controller process locked preflight')
                _require_stable_zero_evidence(
                    process_before, process_locked,
                    'Serve controller process evidence')
                images_locked: dict[tuple[str, int], _ExternalEvidence] = {}
                for identity in sorted(locked_version_evidence_identities):
                    locked_image = _validate_external_evidence(
                        image_evidence_getter(identity[0], identity[1]),
                        'Container-image demand locked preflight')
                    _require_stable_zero_evidence(
                        preflight_images[identity], locked_image,
                        'Container-image demand evidence')
                    images_locked[identity] = locked_image
                resource_actions_locked = _validate_external_evidence_map(
                    resource_action_evidence_getter(engine,
                                                    locked_action_targets),
                    frozenset(locked_version_evidence_identities),
                    'Serve resource-action root locked preflight')
                _require_stable_zero_evidence_map(
                    resource_actions_before, resource_actions_locked,
                    'Serve resource-action root evidence')
                assert legacy_controller_before is not None
                legacy_controller_locked = _validate_external_evidence(
                    legacy_controller_evidence_getter(),
                    'Legacy Serve-controller cluster locked preflight')
                _require_stable_zero_evidence(
                    legacy_controller_before, legacy_controller_locked,
                    'Legacy Serve-controller cluster evidence')
                candidate_services = frozenset(
                    identity[0] for identity in locked_identities)
                raw_cleanup_intents = _read_cleanup_intent_inventory(
                    session, candidate_services, row_bound)
                cleanup_plan = _build_cleanup_intent_plan(
                    raw_cleanup_intents, rows, service_rows, row_bound)
                receipt_evidence = _predecessor_receipt_evidence(
                    session, rows, service_rows, approved_commits, row_bound,
                    freeze_evidence_sha256)
                manifest_freeze_evidence_sha256 = receipt_evidence.facts[
                    'operator_freeze_approved_commit_binding_sha256']
                affected_services = _prepare_retirement_rows(
                    rows, service_rows, run_id,
                    _read_timestamp(now, 'Historical retirement'),
                    images_locked, resource_actions_locked,
                    legacy_controller_locked, process_locked, api_pod_locked,
                    request_before, cleanup_plan, receipt_evidence)
            post_digest = _fleet_sha256(rows, result=True)
            completed_at = _read_timestamp(now, 'Normalization completion')
            if completed_at < started_at:
                raise NormalizationBlocker(
                    'Normalization completion precedes its start.')
            _insert_ledger(
                session,
                rows,
                run_id=run_id,
                mode=mode,
                row_bound=row_bound,
                started_at=started_at,
                completed_at=completed_at,
                freeze_evidence_sha256=manifest_freeze_evidence_sha256,
                pre_digest=pre_digest,
                post_digest=post_digest)
            _verify_inserted_ledger(session, run_id, row_bound)
            if cleanup_plan is not None:
                _cas_cleanup_intent_results(session, cleanup_plan)
            for row in rows:
                _cas_version_result(session, row)
            receipt_services: set[str] = set()
            for service_name in sorted(affected_services):
                service = service_rows.get(service_name)
                if service is not None:
                    _cas_service_receipt_request(session, service_name, service,
                                                 run_id)
                    receipt_services.add(service_name)
            _verify_version_postimages(session, rows, row_bound, post_digest)
            if cleanup_plan is not None:
                _verify_cleanup_intent_postimages(session, cleanup_plan,
                                                  row_bound)
            _verify_service_receipts(session, receipt_services, run_id)
            request_after = _validate_external_evidence(
                request_evidence_getter(engine), 'Serve request postflight')
            if request_after.count or request_after.digest != request_before.digest:
                raise NormalizationBlocker(
                    'Serve mutation request evidence changed during apply.')
            if mode is ApplyMode.RETIRE_TERMINAL_HISTORICAL:
                if consolidation_mode_checker() is not True:
                    raise NormalizationBlocker(
                        'Serve consolidation mode ended during historical '
                        'retirement.')
                if controller_hold_checker() is not True:
                    raise NormalizationBlocker(
                        'Serve controller hold ended during historical '
                        'retirement.')
                for identity, before in preflight_images.items():
                    after = _validate_external_evidence(
                        image_evidence_getter(identity[0], identity[1]),
                        'Container-image demand postflight')
                    if after.count or after.digest != before.digest:
                        raise NormalizationBlocker(
                            'Container-image demand evidence changed during '
                            'historical retirement.')
                resource_actions_after = _validate_external_evidence_map(
                    resource_action_evidence_getter(engine,
                                                    preflight_action_targets),
                    frozenset(preflight_version_evidence_identities),
                    'Serve resource-action root postflight')
                _require_stable_zero_evidence_map(
                    resource_actions_before, resource_actions_after,
                    'Serve resource-action root evidence')
                assert process_before is not None
                process_after = _validate_external_evidence(
                    process_evidence_getter(preflight_process_targets,
                                            api_pod_locked.pod_uid),
                    'Serve controller process postflight')
                _require_stable_zero_evidence(
                    process_before, process_after,
                    'Serve controller process evidence')
                assert legacy_controller_before is not None
                legacy_controller_after = _validate_external_evidence(
                    legacy_controller_evidence_getter(),
                    'Legacy Serve-controller cluster postflight')
                _require_stable_zero_evidence(
                    legacy_controller_before, legacy_controller_after,
                    'Legacy Serve-controller cluster evidence')
            api_pod_after = _validate_api_pod_identity(
                api_pod_checker(engine), 'Sole Recreate API pod postflight')
            _require_stable_api_pod(api_pod_before, api_pod_after)
            try:
                placement_normalization_authority.reassert_writer_session_lock(
                    connection, _ADVISORY_LOCK_NAME)
            except (placement_normalization_authority.
                    PlacementNormalizationAuthorityError) as exc:
                raise NormalizationBlocker(
                    'Placement-normalization writer lost its database '
                    'authority before commit.') from exc

    counts = collections.Counter(row.classification.value for row in rows)
    return OperatorResult(
        mode=mode.value,
        dry_run=False,
        row_count=len(rows),
        classification_counts=dict(counts),
        pre_inventory_sha256=pre_digest,
        post_inventory_sha256=post_digest,
        changed_rows=sum(row.outcome == 'changed' for row in rows),
        retired_rows=sum(row.outcome == 'retired' for row in rows),
        blockers=(),
        run_id=str(run_id),
        prior_ledger_mismatches=prior_mismatches)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Inspect or normalize persisted SkyServe placement '
        'contracts.')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--apply-supported', action='store_true')
    mode.add_argument('--retire-terminal-historical', action='store_true')
    parser.add_argument('--max-rows', type=int, required=True)
    parser.add_argument('--freeze-evidence-sha256')
    parser.add_argument('--approved-loaded-image-commit',
                        action='append',
                        default=[])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    mode = None
    if args.apply_supported:
        mode = ApplyMode.SUPPORTED
    elif args.retire_terminal_historical:
        mode = ApplyMode.RETIRE_TERMINAL_HISTORICAL
    result = run_operator(
        mode=mode,
        row_bound=args.max_rows,
        freeze_evidence_sha256=args.freeze_evidence_sha256,
        approved_loaded_image_commits=(args.approved_loaded_image_commit))
    print(json.dumps(result.as_dict(), sort_keys=True, separators=(',', ':')))


if __name__ == '__main__':
    main()
