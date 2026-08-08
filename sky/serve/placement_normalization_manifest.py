"""Pure validation for persisted placement-normalization manifests.

This module owns the durable manifest grammar shared by the offline writer and
the runtime receipt reader.  It deliberately imports neither database state
nor the normalization operator, so reading a completed receipt cannot acquire
their transitive dependencies or silently use a different validator.
"""

import collections
from collections.abc import Mapping
from collections.abc import Sequence
import dataclasses
import enum
import hashlib
import json
import math
import pickle
from typing import Any
import uuid

from sky.serve import placement_normalization_identity

__all__ = [
    'MAX_INVENTORY_ROWS',
    'ManifestClassification',
    'PlacementNormalizationManifestError',
    'PROTOCOL_V4_ONLY_FACT_FIELDS',
    'ServiceHashObservation',
    'SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT',
    'SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FIELDS',
    'STALE_PLACEHOLDER_EVIDENCE_FACT',
    'STALE_PLACEHOLDER_EVIDENCE_FIELDS',
    'STALE_PLACEHOLDER_NULL_COLUMNS',
    'STALE_PLACEHOLDER_PROOF_SCHEMA',
    'VERSION_SPEC_COLUMNS',
    'current_inventory_mismatches',
    'is_terminal_protocol4_manifest',
    'manifest_mismatches',
    'stale_placeholder_inventory_sha256',
    'validate_completed_manifest',
    'validate_current_inventory',
]


class ManifestClassification(enum.Enum):
    """Persisted row classifications, including contextual protocol-4 state."""

    PLACEHOLDER = 'placeholder'
    EXPLICIT_V1 = 'explicit_v1'
    EXPLICIT_V2 = 'explicit_v2'
    FIELDLESS_SUPPORTED = 'fieldless_supported'
    HISTORICAL_PHYSICAL_PER_GPU = 'historical_physical_per_gpu'
    STALE_PLACEHOLDER = 'stale_placeholder'
    RETIRED = 'retired'
    BLOCKER = 'blocker'


class PlacementNormalizationManifestError(ValueError):
    """A completed placement-normalization manifest is not self-consistent."""

    def __init__(self, mismatches: Sequence[Mapping[str, Any]]) -> None:
        self.mismatches = tuple(dict(mismatch) for mismatch in mismatches)
        first = self.mismatches[0] if self.mismatches else None
        super().__init__(
            'Placement-normalization manifest validation failed; first '
            f'mismatch is {first!r}.')


@dataclasses.dataclass(frozen=True)
class ServiceHashObservation:
    """One explicit live-parent observation for a candidate service.

    ``present=False, value=None`` means the parent row was absent.  Keeping
    presence separate from the raw value prevents a present SQL-NULL hash from
    being mistaken for a completed service teardown.
    """

    present: bool
    value: object

    def validated_value(self) -> str | None:
        """Return a valid hash/absence value or reject a malformed shape."""
        if type(self.present) is not bool:
            raise ValueError('Parent presence must be an exact boolean.')
        if not self.present:
            if self.value is not None:
                raise ValueError('An absent parent cannot carry a hash value.')
            return None
        value = self.value
        if type(value) is not str:
            raise ValueError(
                'A present parent must carry a nonempty opaque hash.')
        value_str = str(value)
        if (not value_str or
                any(character.isspace() for character in value_str)):
            raise ValueError(
                'A present parent must carry a nonempty opaque hash.')
        return value_str


_SCHEMA_REVISION = '037'
_MAX_INVENTORY_ROWS = 100000
_PREDECESSOR_RECEIPT_SCHEMA = 'skyserve-predecessor-receipt-inventory-v1'
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

_STALE_PLACEHOLDER_PROOF_SCHEMA = 'skyserve-stale-placeholder-retirement-v1'
_STALE_PLACEHOLDER_EVIDENCE_FACT = 'stale_placeholder_evidence'
_SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT = (
    'same_service_stale_placeholder_proof')
_LEGACY_PLACEHOLDER_ABSENCE_FACT = (
    'same_service_placeholder_dependency_absent')
_PROTOCOL_V4_ONLY_FACT_FIELDS = frozenset({
    _STALE_PLACEHOLDER_EVIDENCE_FACT,
    _SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT,
})
_STALE_PLACEHOLDER_EVIDENCE_FIELDS = frozenset({
    'schema',
    'service_name_sha256',
    'version',
    'original_row_sha256',
    'strictly_newer_committed_version',
    'image_demand_count',
    'image_demand_sha256',
    'resource_action_root_count',
    'resource_action_root_sha256',
    'state_clean',
    'fill_stale_proved',
})
_SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FIELDS = frozenset({
    'schema',
    'service_name_sha256',
    'current_version',
    'placeholder_count',
    'image_demand_count',
    'resource_action_root_count',
    'inventory_sha256',
    'fill_stale_proved',
})

# This is the immutable revision-038 version_specs inventory carried by
# protocol 4.  Later live-schema additions must not alter the persisted proof.
_VERSION_SPEC_COLUMN_NAMES = (
    'service_name',
    'version',
    'spec',
    'yaml_content',
    'submitted_yaml_content',
    'created_at',
    'created_by',
    'quarantined_at',
    'quarantine_reason',
    'retired_yaml_content',
    'retired_at',
    'retirement_reason',
    'retirement_run_id',
    'placement_catalog',
    'controller_config',
    'controller_config_digest',
    'controller_config_snapshot_id',
    'controller_applied_at',
    'resource_action_spec_identity',
    'resource_action_spec_identity_sha256',
)
_VERSION_SPEC_COLUMNS = frozenset(_VERSION_SPEC_COLUMN_NAMES)
_STALE_PLACEHOLDER_NULL_COLUMNS = frozenset({
    'yaml_content',
    'submitted_yaml_content',
    'placement_catalog',
    'controller_config',
    'controller_config_digest',
    'controller_config_snapshot_id',
    'controller_applied_at',
    'quarantined_at',
    'quarantine_reason',
    'retired_yaml_content',
    'retired_at',
    'retirement_reason',
    'retirement_run_id',
    'resource_action_spec_identity',
    'resource_action_spec_identity_sha256',
})
_RETIREMENT_COLUMNS = frozenset({
    'spec',
    'yaml_content',
    'retired_yaml_content',
    'retired_at',
    'retirement_reason',
    'retirement_run_id',
})
_RETIREMENT_REASON = (
    'transition-only physical/per-GPU contract retired after locked '
    'dependency proof')
_RETIRED_SPEC_BYTES = pickle.dumps(None, protocol=4)

_CONTRACT_PROJECTION_FIELDS = frozenset({
    'version',
    'policy',
    'engine',
    'replica_unit',
    'catalog_mode',
    'cost_unit',
    'reserved_fill_mode',
    'workload_kind',
    'rollback_uses_logical_replicas',
})
_VALID_SERVICE_V2_PROJECTIONS = frozenset({
    (None, 'none', 'physical_backend', 'not_applicable', 'not_applicable',
     'not_applicable'),
    ('dynamic_fallback', 'dynamic_fallback', 'physical_backend',
     'configured_shapes', 'machine_hour', 'configured_shape'),
    ('dynamic_fallback_per_gpu', 'dynamic_fallback', 'logical',
     'whole_gpu_shapes', 'gpu_slot_hour', 'single_gpu_backend'),
})
_HISTORICAL_PROJECTION_BEHAVIOR = (
    'dynamic_fallback_per_gpu',
    'dynamic_fallback',
    'physical_backend',
    'whole_gpu_shapes',
    'gpu_slot_hour',
    'single_gpu_backend',
)

# Public construction constants are immutable aliases of the persisted
# protocol contract; writers should not duplicate these spellings.
MAX_INVENTORY_ROWS = _MAX_INVENTORY_ROWS
VERSION_SPEC_COLUMNS = _VERSION_SPEC_COLUMN_NAMES
STALE_PLACEHOLDER_PROOF_SCHEMA = _STALE_PLACEHOLDER_PROOF_SCHEMA
STALE_PLACEHOLDER_EVIDENCE_FACT = _STALE_PLACEHOLDER_EVIDENCE_FACT
STALE_PLACEHOLDER_EVIDENCE_FIELDS = _STALE_PLACEHOLDER_EVIDENCE_FIELDS
SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT = (
    _SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT)
SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FIELDS = (
    _SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FIELDS)
PROTOCOL_V4_ONLY_FACT_FIELDS = _PROTOCOL_V4_ONLY_FACT_FIELDS
STALE_PLACEHOLDER_NULL_COLUMNS = _STALE_PLACEHOLDER_NULL_COLUMNS


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _json_default(value: Any) -> str:
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(f'Unsupported canonical inventory value: {type(value)!r}.')


def _canonical_json_sha256(value: Any) -> str:
    return _sha256(
        json.dumps(value,
                   sort_keys=True,
                   separators=(',', ':'),
                   default=_json_default).encode())


def _value_sha256(value: Any) -> str:
    if isinstance(value, memoryview):
        value = value.tobytes()
    elif isinstance(value, bytearray):
        value = bytes(value)
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
    return _sha256(
        json.dumps(_column_sha256s(row), sort_keys=True,
                   separators=(',', ':')).encode())


def _prehashed_row_sha256(value: Any) -> str | None:
    if (not isinstance(value, dict) or
            any(not isinstance(column, str) or not _is_sha256(digest)
                for column, digest in value.items())):
        return None
    return _sha256(
        json.dumps(value, sort_keys=True, separators=(',', ':')).encode())


def _is_sha256(value: Any) -> bool:
    return (isinstance(value, str) and len(value) == 64 and
            all(character in '0123456789abcdef' for character in value))


def _cleanup_timestamp_proof_binding_sha256(
    facts: Mapping[str, Any],
    candidate_identity: tuple[str, int],
) -> str:
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


def _stale_placeholder_inventory_sha256(
    service_name: str,
    current_version: int,
    evidence_rows: Sequence[Mapping[str, Any]],
) -> str:
    return _canonical_json_sha256({
        'schema': _STALE_PLACEHOLDER_PROOF_SCHEMA,
        'service_name_sha256': _sha256(service_name.encode()),
        'current_version': current_version,
        'placeholders': list(evidence_rows),
    })


def stale_placeholder_inventory_sha256(
    service_name: str,
    current_version: int,
    evidence_rows: Sequence[Mapping[str, Any]],
) -> str:
    """Return the canonical digest for one complete stale-row inventory."""
    return _stale_placeholder_inventory_sha256(service_name, current_version,
                                               evidence_rows)


def _retirement_ledger_v1_facts_are_complete(entry: Mapping[str, Any]) -> bool:
    """Validate the frozen protocol-1 retirement-fact contract."""
    facts = entry.get('dependency_facts')
    version = entry.get('version')
    if not isinstance(facts, dict) or type(version) is not int:
        return False
    zero_counts = (
        'replica_count',
        'unknown_version_replica_count',
        'cleanup_intent_count',
        'image_demand_count',
        'resource_action_root_count',
        'legacy_controller_cluster_count',
        'serve_mutation_request_count',
        'process_quiescence_count',
    )
    true_proofs = (
        'service_present',
        'serve_consolidation_mode_proved',
        'parent_non_pool_proved',
        'resource_action_mode_legacy_inert',
        'placement_catalog_absent',
        'cleanup_dependency_absent',
        'bridge_replica_dependency_absent',
        'unversioned_replica_dependency_absent',
        'same_service_placeholder_dependency_absent',
        'incomplete_staged_config_dependency_absent',
        'legacy_controller_cluster_absent',
        'sole_recreate_api_pod_proved',
        'controller_hold_required',
    )
    false_facts = ('service_active', 'quarantined', 'controller_applied')
    digest_facts = (
        'image_demand_sha256',
        'resource_action_root_sha256',
        'legacy_controller_cluster_sha256',
        'serve_mutation_request_sha256',
        'process_quiescence_sha256',
        'sole_api_pod_sha256',
    )
    storage = facts.get('storage_ownership')
    storage_fields = {
        'file_mounts', 'storage_mounts', 'volumes', 'volume_mounts', 'workdir',
        'ephemeral_storage_scope'
    }
    pod_uid = facts.get('process_quiescence_pod_uid')
    instance_id = facts.get('sole_api_instance_id')
    try:
        parsed_instance_id = uuid.UUID(instance_id)
    except (AttributeError, TypeError, ValueError):
        return False
    return (all(facts.get(field) == 0 for field in zero_counts) and
            all(facts.get(field) is True for field in true_proofs) and
            all(facts.get(field) is False for field in false_facts) and
            all(_is_sha256(facts.get(field)) for field in digest_facts) and
            type(facts.get('service_pool')) is int and
            facts['service_pool'] == 0 and
            facts.get('service_resource_action_mode') == 'legacy' and
            'service_resource_action_mode_changed_at' in facts and
            facts['service_resource_action_mode_changed_at'] is None and
            isinstance(storage, dict) and set(storage) == storage_fields and
            all(value is False for value in storage.values()) and
            type(facts.get('service_current_version')) is int and
            facts['service_current_version'] > version and
            type(facts.get('strictly_newer_committed_version')) is int and
            facts['strictly_newer_committed_version'] > version and
            type(facts.get('recovery_version')) is int and
            facts['recovery_version'] > version and
            type(facts.get('service_lifecycle_epoch')) is int and
            facts['service_lifecycle_epoch'] > 0 and
            isinstance(facts.get('service_hash'), str) and
            bool(facts['service_hash']) and
            not any(character.isspace() for character in facts['service_hash'])
            and isinstance(pod_uid, str) and bool(pod_uid) and
            not any(character.isspace() for character in pod_uid) and
            str(parsed_instance_id) == instance_id and
            type(facts.get('config_protocol_active')) is bool and
            facts.get('recovery_config_valid')
            is facts.get('config_protocol_active'))


def _retirement_ledger_typed_cleanup_facts_are_complete_for_protocol(
        entry: Mapping[str, Any], protocol: int) -> bool:
    """Validate facts shared by typed cleanup retirement protocols."""
    facts = entry.get('dependency_facts')
    version = entry.get('version')
    if not isinstance(facts, dict) or type(version) is not int:
        return False
    if protocol in (placement_normalization_identity.PROTOCOL_V2,
                    placement_normalization_identity.PROTOCOL_V3):
        if facts.get(_LEGACY_PLACEHOLDER_ABSENCE_FACT) is not True:
            return False
    elif protocol == placement_normalization_identity.PROTOCOL_V4:
        if _LEGACY_PLACEHOLDER_ABSENCE_FACT in facts:
            return False
    else:
        return False
    zero_counts = (
        'replica_count',
        'unknown_version_replica_count',
        'image_demand_count',
        'resource_action_root_count',
        'legacy_controller_cluster_count',
        'serve_mutation_request_count',
        'process_quiescence_count',
        'cleanup_candidate_deletion_target_count',
        'cleanup_intent_deletion_target_count',
    )
    true_proofs = (
        'service_present',
        'serve_consolidation_mode_proved',
        'parent_non_pool_proved',
        'resource_action_mode_legacy_inert',
        'placement_catalog_absent',
        'cleanup_dependency_typed',
        'bridge_replica_dependency_absent',
        'unversioned_replica_dependency_absent',
        'incomplete_staged_config_dependency_absent',
        'legacy_controller_cluster_absent',
        'sole_recreate_api_pod_proved',
        'controller_hold_required',
        'retired_yaml_preserved',
        'current_cleanup_reader_inventory_preserved',
        'v1_1_1135_cleanup_omission_lossless',
        'predecessor_receipts_complete',
        'predecessor_receipt_requirement_satisfied',
    )
    false_facts = ('service_active', 'quarantined', 'controller_applied')
    digest_facts = (
        'image_demand_sha256',
        'resource_action_root_sha256',
        'legacy_controller_cluster_sha256',
        'serve_mutation_request_sha256',
        'process_quiescence_sha256',
        'sole_api_pod_sha256',
        'cleanup_intent_pre_inventory_sha256',
        'cleanup_intent_post_inventory_sha256',
        'cleanup_match_inventory_sha256',
        'cleanup_intent_key_sha256',
        'cleanup_candidate_yaml_sha256',
        'cleanup_intent_yaml_sha256',
        'cleanup_candidate_zero_target_projection_sha256',
        'cleanup_intent_zero_target_projection_sha256',
        'predecessor_receipt_inventory_sha256',
        'approved_loaded_image_commit_sha256',
        'operator_freeze_evidence_input_sha256',
        'operator_freeze_approved_commit_binding_sha256',
    )
    pod_uid = facts.get('process_quiescence_pod_uid')
    instance_id = facts.get('sole_api_instance_id')
    try:
        parsed_instance_id = uuid.UUID(instance_id)
    except (AttributeError, TypeError, ValueError):
        return False
    cleanup_count = facts.get('cleanup_intent_count')
    inventory_count = facts.get('cleanup_intent_inventory_count')
    adopted_count = facts.get('cleanup_intent_adopted_count')
    match_count = facts.get('cleanup_match_inventory_count')
    receipt_count = facts.get('predecessor_receipt_inventory_count')
    approved_count = facts.get('approved_loaded_image_commit_count')
    cleanup_digest_delta_valid = ((adopted_count == 0) == (
        facts.get('cleanup_intent_pre_inventory_sha256') == facts.get(
            'cleanup_intent_post_inventory_sha256')))
    expected_freeze_binding = _canonical_json_sha256({
        'approved_loaded_image_commit_sha256':
            facts.get('approved_loaded_image_commit_sha256'),
        'operator_freeze_evidence_input_sha256':
            facts.get('operator_freeze_evidence_input_sha256'),
    })
    return (
        all(
            type(facts.get(field)) is int and facts[field] == 0
            for field in zero_counts) and
        all(facts.get(field) is True for field in true_proofs) and
        all(facts.get(field) is False for field in false_facts) and
        all(_is_sha256(facts.get(field)) for field in digest_facts) and
        facts.get('predecessor_receipt_schema') == _PREDECESSOR_RECEIPT_SCHEMA
        and type(cleanup_count) is int and type(inventory_count) is int and
        inventory_count >= 1 and 1 <= cleanup_count <= inventory_count and
        type(adopted_count) is int and 0 <= adopted_count <= inventory_count and
        type(match_count) is int and match_count == inventory_count and
        type(facts.get('cleanup_candidate_match_count')) is int and
        facts['cleanup_candidate_match_count'] == 1 and
        cleanup_digest_delta_valid and
        facts.get('cleanup_candidate_yaml_sha256')
        == facts.get('cleanup_intent_yaml_sha256') and
        facts.get('cleanup_candidate_zero_target_projection_sha256')
        == facts.get('cleanup_intent_zero_target_projection_sha256') and
        type(receipt_count) is int and receipt_count >= 0 and
        type(approved_count) is int and approved_count >= 1 and
        facts.get('operator_freeze_approved_commit_binding_sha256')
        == expected_freeze_binding and type(
            facts.get('service_pool')) is int and facts['service_pool'] == 0 and
        facts.get('service_resource_action_mode') == 'legacy' and
        'service_resource_action_mode_changed_at' in facts and
        facts['service_resource_action_mode_changed_at'] is None and
        type(facts.get('service_current_version')) is int and
        facts['service_current_version'] > version and
        type(facts.get('strictly_newer_committed_version')) is int and
        facts['strictly_newer_committed_version'] > version and
        type(facts.get('recovery_version')) is int and
        facts['recovery_version'] > version and
        type(facts.get('service_lifecycle_epoch')) is int and
        facts['service_lifecycle_epoch'] > 0 and isinstance(
            facts.get('service_hash'), str) and facts['service_hash'] != '' and
        not any(character.isspace() for character in facts['service_hash']) and
        isinstance(pod_uid, str) and pod_uid != '' and
        not any(character.isspace() for character in pod_uid) and
        str(parsed_instance_id) == instance_id and type(
            facts.get('config_protocol_active')) is bool and
        facts.get('recovery_config_valid')
        is facts.get('config_protocol_active'))


def _retirement_ledger_typed_cleanup_facts_are_complete(
        entry: Mapping[str, Any]) -> bool:
    """Validate the frozen protocol-2/3 typed cleanup fact contract."""
    return _retirement_ledger_typed_cleanup_facts_are_complete_for_protocol(
        entry, placement_normalization_identity.PROTOCOL_V3)


def _retirement_ledger_v2_facts_are_complete(entry: Mapping[str, Any]) -> bool:
    """Validate the frozen all-finite protocol-2 retirement evidence."""
    facts = entry.get('dependency_facts')
    original_column_sha256s = entry.get('original_column_sha256s')
    return (isinstance(facts, dict) and
            isinstance(original_column_sha256s, dict) and
            _is_sha256(original_column_sha256s.get('created_at')) and
            original_column_sha256s['created_at'] != _value_sha256(None) and
            facts.get('cleanup_contract_schema') == _CLEANUP_PROOF_SCHEMA_V2 and
            not any(field in facts
                    for field in _CLEANUP_PROTOCOL_V3_ONLY_FIELDS) and
            _retirement_ledger_typed_cleanup_facts_are_complete(entry))


def _retirement_ledger_timestamp_facts_are_complete(entry: Mapping[str, Any],
                                                    protocol: int) -> bool:
    """Validate facts shared by timestamp-bound protocols 3 and 4."""
    facts = entry.get('dependency_facts')
    service_name = entry.get('service_name')
    version = entry.get('version')
    original_column_sha256s = entry.get('original_column_sha256s')
    if (not isinstance(facts, dict) or type(service_name) is not str or
            not service_name or type(version) is not int or version < 1 or
            not isinstance(original_column_sha256s, dict) or
            not _is_sha256(original_column_sha256s.get('created_at')) or
            facts.get('cleanup_contract_schema') != _CLEANUP_PROOF_SCHEMA or
            not _retirement_ledger_typed_cleanup_facts_are_complete_for_protocol(
                entry, protocol)):
        return False

    service_count = facts.get('cleanup_version_timestamp_service_count')
    inventory_count = facts.get('cleanup_version_timestamp_inventory_count')
    matched_count = facts.get('cleanup_version_timestamp_matched_intent_count')
    legacy_null_count = facts.get('cleanup_legacy_null_version_timestamp_count')
    boundary_count = facts.get('cleanup_timestamp_boundary_count')
    mode = facts.get('cleanup_candidate_version_created_at_mode')
    boundary_version = facts.get(
        'cleanup_candidate_legacy_timestamp_boundary_version')
    if (type(service_count) is not int or service_count < 1 or
            type(inventory_count) is not int or
            not service_count <= inventory_count <= _MAX_INVENTORY_ROWS or
            type(matched_count) is not int or
            matched_count != facts.get('cleanup_match_inventory_count') or
            matched_count != facts.get('cleanup_intent_inventory_count') or
            not 1 <= matched_count <= inventory_count or
            type(legacy_null_count) is not int or
            not 0 <= legacy_null_count < inventory_count or
            type(boundary_count) is not int or
            not 0 <= boundary_count <= service_count or
            boundary_count > legacy_null_count or
            boundary_count > inventory_count - legacy_null_count or
        ((legacy_null_count == 0) != (boundary_count == 0)) or not _is_sha256(
            facts.get('cleanup_version_timestamp_inventory_sha256')) or
            not _is_sha256(facts.get('cleanup_timestamp_proof_sha256'))):
        return False

    if mode == _CLEANUP_CREATED_AT_MODE_FINITE:
        if (boundary_version is not None or
                original_column_sha256s['created_at'] == _value_sha256(None)):
            return False
    elif mode == _CLEANUP_CREATED_AT_MODE_LEGACY_PREFIX_NULL:
        current_version = facts.get('service_current_version')
        if (legacy_null_count < 1 or boundary_count < 1 or
                original_column_sha256s['created_at'] != _value_sha256(None) or
                type(boundary_version) is not int or
                not version < boundary_version or
                type(current_version) is not int or
                boundary_version > current_version):
            return False
    else:
        return False

    return facts['cleanup_timestamp_proof_sha256'] == (
        _cleanup_timestamp_proof_binding_sha256(facts, (service_name, version)))


def _retirement_ledger_v3_facts_are_complete(entry: Mapping[str, Any]) -> bool:
    """Validate timestamp-bound protocol-3 retirement evidence."""
    facts = entry.get('dependency_facts')
    service_name = entry.get('service_name')
    version = entry.get('version')
    original_column_sha256s = entry.get('original_column_sha256s')
    if (not isinstance(facts, dict) or type(service_name) is not str or
            not service_name or type(version) is not int or version < 1 or
            not isinstance(original_column_sha256s, dict) or
            not _is_sha256(original_column_sha256s.get('created_at')) or
            facts.get('cleanup_contract_schema') != _CLEANUP_PROOF_SCHEMA or
            not _retirement_ledger_typed_cleanup_facts_are_complete(entry)):
        return False

    service_count = facts.get('cleanup_version_timestamp_service_count')
    inventory_count = facts.get('cleanup_version_timestamp_inventory_count')
    matched_count = facts.get('cleanup_version_timestamp_matched_intent_count')
    legacy_null_count = facts.get('cleanup_legacy_null_version_timestamp_count')
    boundary_count = facts.get('cleanup_timestamp_boundary_count')
    mode = facts.get('cleanup_candidate_version_created_at_mode')
    boundary_version = facts.get(
        'cleanup_candidate_legacy_timestamp_boundary_version')
    if (type(service_count) is not int or service_count < 1 or
            type(inventory_count) is not int or
            not service_count <= inventory_count <= _MAX_INVENTORY_ROWS or
            type(matched_count) is not int or
            matched_count != facts.get('cleanup_match_inventory_count') or
            matched_count != facts.get('cleanup_intent_inventory_count') or
            not 1 <= matched_count <= inventory_count or
            type(legacy_null_count) is not int or
            not 0 <= legacy_null_count < inventory_count or
            type(boundary_count) is not int or
            not 0 <= boundary_count <= service_count or
            boundary_count > legacy_null_count or
            boundary_count > inventory_count - legacy_null_count or
        ((legacy_null_count == 0) != (boundary_count == 0)) or not _is_sha256(
            facts.get('cleanup_version_timestamp_inventory_sha256')) or
            not _is_sha256(facts.get('cleanup_timestamp_proof_sha256'))):
        return False

    if mode == _CLEANUP_CREATED_AT_MODE_FINITE:
        if (boundary_version is not None or
                original_column_sha256s['created_at'] == _value_sha256(None)):
            return False
    elif mode == _CLEANUP_CREATED_AT_MODE_LEGACY_PREFIX_NULL:
        current_version = facts.get('service_current_version')
        if (legacy_null_count < 1 or boundary_count < 1 or
                original_column_sha256s['created_at'] != _value_sha256(None) or
                type(boundary_version) is not int or
                not version < boundary_version or
                type(current_version) is not int or
                boundary_version > current_version):
            return False
    else:
        return False

    return facts['cleanup_timestamp_proof_sha256'] == (
        _cleanup_timestamp_proof_binding_sha256(facts, (service_name, version)))


def _has_exact_protocol4_column_inventories(entry: Mapping[str, Any]) -> bool:
    original = entry.get('original_column_sha256s')
    result = entry.get('result_column_sha256s')
    service_name = entry.get('service_name')
    version = entry.get('version')
    return (isinstance(original, dict) and isinstance(result, dict) and
            set(original) == _VERSION_SPEC_COLUMNS and
            set(result) == _VERSION_SPEC_COLUMNS and
            type(service_name) is str and service_name != '' and
            type(version) is int and version > 0 and
            original['service_name'] == _value_sha256(service_name) and
            result['service_name'] == _value_sha256(service_name) and
            original['version'] == _value_sha256(version) and
            result['version'] == _value_sha256(version))


def _valid_explicit_v2_service_projection(value: Any) -> bool:
    if not isinstance(value, dict) or set(value) != _CONTRACT_PROJECTION_FIELDS:
        return False
    behavior = (value.get('policy'), value.get('engine'),
                value.get('replica_unit'), value.get('catalog_mode'),
                value.get('cost_unit'), value.get('reserved_fill_mode'))
    return (type(value.get('version')) is int and value['version'] == 2 and
            value.get('workload_kind') == 'service' and
            value.get('rollback_uses_logical_replicas') is None and
            behavior in _VALID_SERVICE_V2_PROJECTIONS)


def _valid_protocol4_projection(classification: object, outcome: object,
                                value: Any) -> bool:
    if classification in (ManifestClassification.PLACEHOLDER.value,
                          ManifestClassification.STALE_PLACEHOLDER.value,
                          ManifestClassification.RETIRED.value):
        return value is None
    if not isinstance(value, dict) or set(value) != _CONTRACT_PROJECTION_FIELDS:
        return False
    version = value.get('version')
    workload_kind = value.get('workload_kind')
    rollback = value.get('rollback_uses_logical_replicas')
    behavior = (value.get('policy'), value.get('engine'),
                value.get('replica_unit'), value.get('catalog_mode'),
                value.get('cost_unit'), value.get('reserved_fill_mode'))
    if classification == ManifestClassification.EXPLICIT_V2.value:
        return (type(version) is int and version == 2 and
                workload_kind in ('service', 'pool') and rollback is None and
                behavior in _VALID_SERVICE_V2_PROJECTIONS and
                not (workload_kind == 'pool' and
                     behavior[0] == 'dynamic_fallback_per_gpu'))
    if classification == (
            ManifestClassification.HISTORICAL_PHYSICAL_PER_GPU.value):
        return ((version is None or type(version) is int and version == 1) and
                workload_kind == 'service' and
                (rollback is None or rollback is False) and
                behavior == _HISTORICAL_PROJECTION_BEHAVIOR)
    if classification in (ManifestClassification.EXPLICIT_V1.value,
                          ManifestClassification.FIELDLESS_SUPPORTED.value):
        # Changed supported rows persist the projection recaptured from their
        # normalized explicit-v2 result, not the legacy source projection.
        return (outcome == 'changed' and _valid_protocol4_projection(
            ManifestClassification.EXPLICIT_V2.value, 'unchanged', value))
    return False


def _canonical_none_markers(entry: Mapping[str, Any]) -> tuple[bool, ...]:
    original = entry.get('original_column_sha256s')
    result = entry.get('result_column_sha256s')
    original_spec_column = (original.get('spec')
                            if isinstance(original, dict) else None)
    result_spec_column = (result.get('spec')
                          if isinstance(result, dict) else None)
    canonical_spec = _sha256(_RETIRED_SPEC_BYTES)
    canonical_column = _value_sha256(_RETIRED_SPEC_BYTES)
    return (
        entry.get('original_spec_sha256') == canonical_spec,
        entry.get('result_spec_sha256') == canonical_spec,
        original_spec_column == canonical_column,
        result_spec_column == canonical_column,
    )


def _is_exact_stale_placeholder_entry(entry: Mapping[str, Any],
                                      retired_entry: Mapping[str, Any],
                                      current_version: int) -> bool:
    facts = entry.get('dependency_facts')
    retired_facts = retired_entry.get('dependency_facts')
    evidence = (facts.get(_STALE_PLACEHOLDER_EVIDENCE_FACT) if isinstance(
        facts, dict) else None)
    original = entry.get('original_column_sha256s')
    result = entry.get('result_column_sha256s')
    service_name = entry.get('service_name')
    version = entry.get('version')
    null_sha256 = _value_sha256(None)
    if (not _has_exact_protocol4_column_inventories(entry) or
            type(service_name) is not str or type(version) is not int or
            entry.get('classification')
            != ManifestClassification.STALE_PLACEHOLDER.value or
            entry.get('outcome') != 'unchanged' or
            entry.get('contract_projection') is not None or
            not all(_canonical_none_markers(entry)) or
            not isinstance(original, dict) or not isinstance(result, dict) or
            original != result or entry.get('original_row_sha256')
            != entry.get('result_row_sha256') or not isinstance(facts, dict) or
            not isinstance(retired_facts, dict) or
            _SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT in facts or
            not isinstance(evidence, dict) or
            set(evidence) != _STALE_PLACEHOLDER_EVIDENCE_FIELDS or
            any(original[column] != null_sha256
                for column in _STALE_PLACEHOLDER_NULL_COLUMNS)):
        return False
    if (facts.get('service_present') is not True or
            type(facts.get('service_pool')) is not int or
            facts['service_pool'] != 0 or
            facts.get('service_active') is not False or
            facts.get('quarantined') is not False or
            facts.get('controller_applied') is not False or
            facts.get('retired') is not False or
            type(facts.get('replica_count')) is not int or
            facts['replica_count'] != 0 or
            type(facts.get('unknown_version_replica_count')) is not int or
            facts['unknown_version_replica_count'] != 0 or
            facts.get('service_current_version') != current_version or
            facts.get('service_resource_action_mode') != 'legacy' or
            'service_resource_action_mode_changed_at' not in facts or
            facts.get('service_resource_action_mode_changed_at') is not None or
            facts.get('service_hash') != retired_facts.get('service_hash') or
            facts.get('service_lifecycle_epoch')
            != retired_facts.get('service_lifecycle_epoch')):
        return False
    return (evidence.get('schema') == _STALE_PLACEHOLDER_PROOF_SCHEMA and
            evidence.get('service_name_sha256') == _sha256(
                service_name.encode()) and
            evidence.get('version') == version and
            evidence.get('original_row_sha256')
            == entry.get('original_row_sha256') and
            evidence.get('strictly_newer_committed_version') == current_version
            and current_version > version and
            type(evidence.get('image_demand_count')) is int and
            evidence['image_demand_count'] == 0 and
            _is_sha256(evidence.get('image_demand_sha256')) and
            type(evidence.get('resource_action_root_count')) is int and
            evidence['resource_action_root_count'] == 0 and
            _is_sha256(evidence.get('resource_action_root_sha256')) and
            evidence.get('state_clean') is True and
            evidence.get('fill_stale_proved') is True)


def _is_exact_retired_unchanged_tombstone(entry: Mapping[str, Any]) -> bool:
    original = entry.get('original_column_sha256s')
    result = entry.get('result_column_sha256s')
    facts = entry.get('dependency_facts')
    null_sha256 = _value_sha256(None)
    return (
        _has_exact_protocol4_column_inventories(entry) and
        entry.get('classification') == ManifestClassification.RETIRED.value and
        entry.get('outcome') == 'unchanged' and
        entry.get('contract_projection') is None and
        all(_canonical_none_markers(entry)) and isinstance(original, dict) and
        isinstance(result, dict) and original == result and
        entry.get('original_row_sha256') == entry.get('result_row_sha256') and
        original['yaml_content'] == null_sha256 and
        original['retired_yaml_content'] != null_sha256 and
        original['retired_at'] != null_sha256 and
        original['retirement_reason'] == _value_sha256(_RETIREMENT_REASON) and
        original['retirement_run_id'] != null_sha256 and
        isinstance(facts, dict) and facts.get('retired') is True)


def _is_exact_protocol4_retirement_candidate(entry: Mapping[str, Any]) -> bool:
    original = entry.get('original_column_sha256s')
    result = entry.get('result_column_sha256s')
    facts = entry.get('dependency_facts')
    null_sha256 = _value_sha256(None)
    if (not _has_exact_protocol4_column_inventories(entry) or
            entry.get('classification')
            != ManifestClassification.HISTORICAL_PHYSICAL_PER_GPU.value or
            entry.get('outcome') != 'retired' or
            not isinstance(original, dict) or not isinstance(result, dict) or
            any(_canonical_none_markers(entry)[::2]) or
            not all(_canonical_none_markers(entry)[1::2]) or
            not isinstance(facts, dict) or facts.get('retired') is not False):
        return False
    unchanged_columns = _VERSION_SPEC_COLUMNS - _RETIREMENT_COLUMNS
    return (all(
        original[column] == result[column] for column in unchanged_columns) and
            original['yaml_content'] != null_sha256 and
            result['yaml_content'] == null_sha256 and
            original['retired_yaml_content'] == null_sha256 and
            result['retired_yaml_content'] == original['yaml_content'] and
            original['retired_at'] == null_sha256 and
            result['retired_at'] != null_sha256 and
            original['retirement_reason'] == null_sha256 and
            result['retirement_reason'] == _value_sha256(_RETIREMENT_REASON) and
            original['retirement_run_id'] == null_sha256 and
            result['retirement_run_id'] == _value_sha256(entry.get('run_id'))
            and original['quarantined_at'] == null_sha256 and
            result['quarantined_at'] == null_sha256 and
            original['controller_applied_at'] == null_sha256 and
            result['controller_applied_at'] == null_sha256)


def _is_exact_protocol4_current_entry(entry: Mapping[str, Any],
                                      retired_entry: Mapping[str, Any],
                                      current_version: int) -> bool:
    original = entry.get('original_column_sha256s')
    result = entry.get('result_column_sha256s')
    facts = entry.get('dependency_facts')
    retired_facts = retired_entry.get('dependency_facts')
    null_sha256 = _value_sha256(None)
    markers = _canonical_none_markers(entry)
    return (
        _has_exact_protocol4_column_inventories(entry) and
        entry.get('version') == current_version and entry.get('classification')
        == ManifestClassification.EXPLICIT_V2.value and
        entry.get('outcome') == 'unchanged' and
        not any(markers) and _valid_explicit_v2_service_projection(
            entry.get('contract_projection')) and isinstance(original, dict) and
        isinstance(result, dict) and original == result and
        entry.get('original_row_sha256') == entry.get('result_row_sha256') and
        entry.get('original_spec_sha256') == entry.get('result_spec_sha256') and
        original['yaml_content'] != null_sha256 and
        original['retired_yaml_content'] == null_sha256 and
        original['retired_at'] == null_sha256 and
        original['retirement_reason'] == null_sha256 and
        original['retirement_run_id'] == null_sha256 and
        original['quarantined_at'] == null_sha256 and
        isinstance(facts, dict) and isinstance(retired_facts, dict) and
        facts.get('service_present') is True and
        type(facts.get('service_pool')) is int and
        facts['service_pool'] == 0 and
        facts.get('service_current_version') == current_version and
        facts.get('quarantined') is False and facts.get('retired') is False and
        facts.get('service_hash') == retired_facts.get('service_hash') and
        facts.get('service_lifecycle_epoch')
        == retired_facts.get('service_lifecycle_epoch'))


def _retirement_ledger_v4_facts_are_complete(entry: Mapping[str, Any]) -> bool:
    facts = entry.get('dependency_facts')
    service_name = entry.get('service_name')
    summary = (facts.get(_SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT)
               if isinstance(facts, dict) else None)
    if (not isinstance(facts, dict) or type(service_name) is not str or
            not service_name or _STALE_PLACEHOLDER_EVIDENCE_FACT in facts or
            _LEGACY_PLACEHOLDER_ABSENCE_FACT in facts or
            not isinstance(summary, dict) or
            set(summary) != _SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FIELDS or
            not _is_exact_protocol4_retirement_candidate(entry)):
        return False
    current_version = summary.get('current_version')
    placeholder_count = summary.get('placeholder_count')
    return (summary.get('schema') == _STALE_PLACEHOLDER_PROOF_SCHEMA and
            summary.get('service_name_sha256') == _sha256(
                service_name.encode()) and type(current_version) is int and
            current_version > 0 and type(placeholder_count) is int and
            0 <= placeholder_count <= _MAX_INVENTORY_ROWS and
            type(summary.get('image_demand_count')) is int and
            summary['image_demand_count'] == 0 and
            type(summary.get('resource_action_root_count')) is int and
            summary['resource_action_root_count'] == 0 and
            _is_sha256(summary.get('inventory_sha256')) and
            summary.get('fill_stale_proved') is True and
            _retirement_ledger_timestamp_facts_are_complete(
                entry, placement_normalization_identity.PROTOCOL_V4))


def _retirement_ledger_facts_are_complete(entry: Mapping[str, Any],
                                          protocol: int) -> bool:
    if protocol == placement_normalization_identity.PROTOCOL_V1:
        return _retirement_ledger_v1_facts_are_complete(entry)
    if protocol == placement_normalization_identity.PROTOCOL_V2:
        return _retirement_ledger_v2_facts_are_complete(entry)
    if protocol == placement_normalization_identity.PROTOCOL_V3:
        return _retirement_ledger_v3_facts_are_complete(entry)
    if protocol == placement_normalization_identity.PROTOCOL_V4:
        return _retirement_ledger_v4_facts_are_complete(entry)
    return False


def _retirement_ledger_v4_stale_placeholders_are_complete(
    retired_entry: Mapping[str, Any],
    entries: Sequence[Mapping[str, Any]],
) -> bool:
    if not _retirement_ledger_v4_facts_are_complete(retired_entry):
        return False
    service_name = retired_entry.get('service_name')
    retired_facts = retired_entry.get('dependency_facts')
    assert isinstance(service_name, str)
    assert isinstance(retired_facts, dict)
    summary = retired_facts[_SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT]
    assert isinstance(summary, dict)
    current_version = summary['current_version']
    assert isinstance(current_version, int)

    same_service_entries = [
        entry for entry in entries if entry.get('service_name') == service_name
    ]
    stale_entries = [
        entry for entry in same_service_entries if entry.get('classification')
        == ManifestClassification.STALE_PLACEHOLDER.value
    ]
    if (any(
            entry.get('classification') ==
            ManifestClassification.PLACEHOLDER.value
            for entry in same_service_entries) or
            len(stale_entries) != summary['placeholder_count']):
        return False

    current_entries = [
        entry for entry in same_service_entries
        if entry.get('version') == current_version
    ]
    if len(current_entries) != 1 or not _is_exact_protocol4_current_entry(
            current_entries[0], retired_entry, current_version):
        return False

    evidence_rows: list[Mapping[str, Any]] = []
    image_count = 0
    action_count = 0
    for entry in sorted(stale_entries,
                        key=lambda item: int(item.get('version', -1))):
        if not _is_exact_stale_placeholder_entry(entry, retired_entry,
                                                 current_version):
            return False
        facts = entry['dependency_facts']
        assert isinstance(facts, dict)
        evidence = facts[_STALE_PLACEHOLDER_EVIDENCE_FACT]
        assert isinstance(evidence, dict)
        image_count += evidence['image_demand_count']
        action_count += evidence['resource_action_root_count']
        evidence_rows.append(evidence)

    # Classification strings are not sufficient proof.  Every unchanged row
    # carrying any canonical-None marker must have all four bound markers and
    # one of the two exact durable shapes.  In particular it can never be
    # relabeled explicit-v2, nor become a fake tombstone by substituting one
    # side-column hash.
    for entry in same_service_entries:
        if (entry.get('outcome') == 'unchanged' and
                any(_canonical_none_markers(entry)) and
                not (_is_exact_stale_placeholder_entry(entry, retired_entry,
                                                       current_version) or
                     _is_exact_retired_unchanged_tombstone(entry))):
            return False

    return (retired_facts.get('service_current_version') == current_version and
            image_count == summary['image_demand_count'] == 0 and
            action_count == summary['resource_action_root_count'] == 0 and
            summary['inventory_sha256'] == _stale_placeholder_inventory_sha256(
                service_name, current_version, evidence_rows))


def manifest_mismatches(
        run: Mapping[str, Any],
        entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return every self-consistency failure in a completed full manifest."""
    run_id = str(run.get('run_id'))
    issues: list[dict[str, Any]] = []

    def add(reason: str, entry: Mapping[str, Any] | None = None) -> None:
        issue: dict[str, Any] = {'run_id': run_id, 'reason': reason}
        if entry is not None:
            issue.update({
                'service_name': entry.get('service_name'),
                'version': entry.get('version'),
            })
        issues.append(issue)

    row_count = run.get('row_count')
    row_bound = run.get('row_bound')
    raw_run_id = run.get('run_id')
    mode = run.get('mode')
    started_at = run.get('started_at')
    completed_at = run.get('completed_at')
    normalizer_version = run.get('normalizer_version')
    normalizer_identity: (
        placement_normalization_identity.PlacementNormalizationIdentity |
        None) = None
    parsed_mode: str | None = None
    if not isinstance(raw_run_id, uuid.UUID):
        add('invalid_run_id')
    try:
        parsed_mode = placement_normalization_identity.parse_manifest_mode(mode)
    except placement_normalization_identity.PlacementNormalizationIdentityError:
        add('invalid_run_mode')
    try:
        normalizer_identity = (placement_normalization_identity.
                               parse_normalizer_identity(normalizer_version))
    except placement_normalization_identity.PlacementNormalizationIdentityError:
        add('invalid_normalizer_version')
    allowed_outcomes: frozenset[tuple[str, str]] = frozenset()
    if normalizer_identity is not None and parsed_mode is not None:
        allowed_outcomes = (
            placement_normalization_identity.allowed_manifest_outcomes(
                normalizer_identity, parsed_mode))
    if run.get('schema_revision') != _SCHEMA_REVISION:
        add('invalid_schema_revision')
    if not isinstance(run.get('release_version'),
                      str) or not run.get('release_version'):
        add('invalid_release_version')
    if (not isinstance(started_at,
                       (int, float)) or isinstance(started_at, bool) or
            not math.isfinite(started_at) or started_at < 0 or
            not isinstance(completed_at,
                           (int, float)) or isinstance(completed_at, bool) or
            not math.isfinite(completed_at) or completed_at < started_at):
        add('invalid_run_times')
    if not all(
            _is_sha256(run.get(field))
            for field in ('pre_inventory_sha256', 'post_inventory_sha256',
                          'freeze_evidence_sha256')):
        add('invalid_run_digests')
    if (type(row_count) is not int or type(row_bound) is not int or
            not 0 <= row_count <= row_bound <= _MAX_INVENTORY_ROWS):
        add('invalid_run_row_bound')
    if type(row_count) is not int or row_count != len(entries):
        add('incomplete_run_inventory')

    counts: collections.Counter[str] = collections.Counter()
    pre_inventory: list[tuple[str, int, str]] = []
    post_inventory: list[tuple[str, int, str]] = []
    identities: set[tuple[str, int]] = set()
    for entry in entries:
        service_name = entry.get('service_name')
        version = entry.get('version')
        if (not isinstance(service_name, str) or not service_name or
                type(version) is not int or version < 1 or
            (service_name, version) in identities):
            add('invalid_or_duplicate_row_identity', entry)
            continue
        identities.add((service_name, version))
        classification = entry.get('classification')
        outcome = entry.get('outcome')
        if (classification, outcome) not in allowed_outcomes:
            add('invalid_classification_outcome', entry)
        if entry.get('run_id') != raw_run_id:
            add('row_run_id_mismatch', entry)
        if not isinstance(classification, str) or not classification:
            add('invalid_row_classification', entry)
        else:
            counts[classification] += 1
        original_spec_digest = entry.get('original_spec_sha256')
        result_spec_digest = entry.get('result_spec_sha256')
        if not (_is_sha256(original_spec_digest) and
                _is_sha256(result_spec_digest)):
            add('invalid_spec_digests', entry)
        elif ((outcome == 'unchanged')
              != (original_spec_digest == result_spec_digest)):
            add('spec_digest_outcome_mismatch', entry)
        if (outcome == 'retired' and
                result_spec_digest != _sha256(_RETIRED_SPEC_BYTES)):
            add('invalid_retired_result_digest', entry)
        original_row_digest = entry.get('original_row_sha256')
        result_row_digest = entry.get('result_row_sha256')
        if (_prehashed_row_sha256(entry.get('original_column_sha256s'))
                != original_row_digest):
            add('invalid_original_column_inventory', entry)
        if (_prehashed_row_sha256(entry.get('result_column_sha256s'))
                != result_row_digest):
            add('invalid_result_column_inventory', entry)
        if isinstance(original_row_digest, str):
            pre_inventory.append((service_name, version, original_row_digest))
        if isinstance(result_row_digest, str):
            post_inventory.append((service_name, version, result_row_digest))
        facts = entry.get('dependency_facts')
        service_hash = entry.get('service_hash')
        if not (service_hash is None or
                type(service_hash) is str and service_hash != '' and
                not any(character.isspace() for character in service_hash)):
            add('invalid_service_hash', entry)
        if (not isinstance(facts, dict) or
                facts.get('service_hash') != service_hash or
                facts.get('service_lifecycle_epoch')
                != entry.get('service_lifecycle_epoch')):
            add('owner_facts_do_not_match_columns', entry)
        if normalizer_identity is not None and isinstance(facts, dict):
            protocol = normalizer_identity.protocol
            has_stale_evidence = _STALE_PLACEHOLDER_EVIDENCE_FACT in facts
            has_stale_summary = (_SAME_SERVICE_STALE_PLACEHOLDER_PROOF_FACT
                                 in facts)
            if (protocol != placement_normalization_identity.PROTOCOL_V4 and
                (has_stale_evidence or has_stale_summary)):
                add('later_protocol_placeholder_facts', entry)
            if protocol == placement_normalization_identity.PROTOCOL_V4:
                if not _has_exact_protocol4_column_inventories(entry):
                    add('invalid_v4_version_column_inventory', entry)
                if not _valid_protocol4_projection(
                        classification, outcome,
                        entry.get('contract_projection')):
                    add('invalid_v4_contract_projection', entry)
                if _LEGACY_PLACEHOLDER_ABSENCE_FACT in facts:
                    add('legacy_placeholder_absence_fact_in_v4', entry)
                if classification == (
                        ManifestClassification.STALE_PLACEHOLDER.value):
                    if (outcome != 'unchanged' or not has_stale_evidence or
                            has_stale_summary):
                        add('invalid_v4_stale_placeholder_fact_scope', entry)
                elif outcome == 'retired':
                    if has_stale_evidence or not has_stale_summary:
                        add('invalid_v4_stale_placeholder_fact_scope', entry)
                elif has_stale_evidence or has_stale_summary:
                    add('invalid_v4_stale_placeholder_fact_scope', entry)
        if (outcome == 'retired' and
            (normalizer_identity is None or
             not _retirement_ledger_facts_are_complete(
                 entry, normalizer_identity.protocol))):
            add('incomplete_retirement_dependency_facts', entry)
        if (outcome == 'retired' and normalizer_identity is not None and
                normalizer_identity.protocol
                in (placement_normalization_identity.PROTOCOL_V2,
                    placement_normalization_identity.PROTOCOL_V3,
                    placement_normalization_identity.PROTOCOL_V4) and
                isinstance(facts, dict) and
                facts.get('operator_freeze_approved_commit_binding_sha256')
                != run.get('freeze_evidence_sha256')):
            add('retirement_freeze_commit_binding_mismatch', entry)

    if (normalizer_identity is not None and normalizer_identity.protocol
            == placement_normalization_identity.PROTOCOL_V4 and parsed_mode ==
            placement_normalization_identity.RETIRE_TERMINAL_HISTORICAL_MODE):
        retired_entries = [
            entry for entry in entries if entry.get('outcome') == 'retired'
        ]
        candidate_services = {
            entry.get('service_name') for entry in retired_entries
        }
        for entry in entries:
            if (entry.get('classification')
                    == ManifestClassification.STALE_PLACEHOLDER.value and
                    entry.get('service_name') not in candidate_services):
                add('orphan_v4_stale_placeholder', entry)
        for entry in retired_entries:
            if not _retirement_ledger_v4_stale_placeholders_are_complete(
                    entry, entries):
                add('incomplete_v4_stale_placeholder_inventory', entry)

    if run.get('classification_counts') != dict(counts):
        add('classification_counts_do_not_match_inventory')
    pre_digest = _sha256(
        json.dumps(sorted(pre_inventory), separators=(',', ':')).encode())
    post_digest = _sha256(
        json.dumps(sorted(post_inventory), separators=(',', ':')).encode())
    if run.get('pre_inventory_sha256') != pre_digest:
        add('pre_inventory_digest_mismatch')
    if run.get('post_inventory_sha256') != post_digest:
        add('post_inventory_digest_mismatch')
    return issues


def validate_completed_manifest(run: Mapping[str, Any],
                                entries: Sequence[Mapping[str, Any]]) -> None:
    """Raise if a persisted completed manifest is not complete and valid."""
    mismatches = manifest_mismatches(run, entries)
    if mismatches:
        raise PlacementNormalizationManifestError(mismatches)


def is_terminal_protocol4_manifest(
        run: Mapping[str, Any], entries: Sequence[Mapping[str, Any]]) -> bool:
    """Whether a valid manifest is the terminal historical retirement run."""
    validate_completed_manifest(run, entries)
    identity = placement_normalization_identity.parse_normalizer_identity(
        run.get('normalizer_version'))
    mode = placement_normalization_identity.parse_manifest_mode(run.get('mode'))
    return (
        identity.protocol == placement_normalization_identity.PROTOCOL_V4 and
        mode == placement_normalization_identity.RETIRE_TERMINAL_HISTORICAL_MODE
        and any(
            entry.get('classification') ==
            ManifestClassification.HISTORICAL_PHYSICAL_PER_GPU.value and
            entry.get('outcome') == 'retired' for entry in entries))


def _raw_spec_bytes(row: Mapping[str, Any]) -> bytes | None:
    value = row.get('spec')
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    return value if isinstance(value, bytes) else None


def _is_raw_unretired_placeholder(row: Mapping[str, Any], *,
                                  fillable: bool) -> bool:
    return (set(row) == _VERSION_SPEC_COLUMNS and
            _raw_spec_bytes(row) == _RETIRED_SPEC_BYTES and all(
                row.get(column) is None
                for column in _STALE_PLACEHOLDER_NULL_COLUMNS) and
            (not fillable or row.get('created_at') is None))


def _is_raw_post_terminal_explicit_v2(row: Mapping[str, Any],
                                      completed_at: float) -> bool:
    created_at = row.get('created_at')
    return (set(row) == _VERSION_SPEC_COLUMNS and
            _raw_spec_bytes(row) not in (None, _RETIRED_SPEC_BYTES) and
            isinstance(row.get('yaml_content'), str) and
            row.get('quarantined_at') is None and
            row.get('retired_yaml_content') is None and
            row.get('retired_at') is None and
            row.get('retirement_reason') is None and
            row.get('retirement_run_id') is None and
            isinstance(created_at,
                       (int, float)) and not isinstance(created_at, bool) and
            math.isfinite(created_at) and created_at > completed_at)


def current_inventory_mismatches(
    run: Mapping[str, Any],
    entries: Sequence[Mapping[str, Any]],
    current_rows: Sequence[Mapping[str, Any]],
    current_service_hashes: Mapping[str, ServiceHashObservation],
    current_classifications: Mapping[tuple[str, int], str],
) -> list[dict[str, Any]]:
    """Validate live rows against a terminal protocol-4 manifest.

    ``current_rows`` must be a bounded query selecting exactly
    :data:`VERSION_SPEC_COLUMNS` for every retired candidate service (and no
    unrelated service). ``current_service_hashes`` must contain one explicit
    :class:`ServiceHashObservation` per candidate, distinguishing a deleted
    parent from a present parent whose hash value is malformed.
    ``current_classifications`` is produced by the operator's exact raw scanner
    and must cover every current row identity. Lifecycle epochs are
    intentionally absent: an unchanged service hash is the durable incarnation
    identity for immutable version bytes.
    """
    issues = manifest_mismatches(run, entries)
    if issues:
        return issues
    if not is_terminal_protocol4_manifest(run, entries):
        return []

    run_id = str(run.get('run_id'))

    def add(reason: str,
            service_name: object = None,
            version: object = None) -> None:
        issue: dict[str, Any] = {'run_id': run_id, 'reason': reason}
        if service_name is not None:
            issue['service_name'] = service_name
        if version is not None:
            issue['version'] = version
        issues.append(issue)

    completed_at = run['completed_at']
    assert isinstance(completed_at, (int, float))
    candidate_services = {
        entry['service_name']
        for entry in entries
        if entry.get('classification') == ManifestClassification.
        HISTORICAL_PHYSICAL_PER_GPU.value and entry.get('outcome') == 'retired'
    }
    manifest_by_identity = {
        (entry['service_name'], entry['version']): entry
        for entry in entries
        if entry['service_name'] in candidate_services
    }
    terminal_hashes: dict[str, str | None] = {}
    high_waters: dict[tuple[str, str | None], int] = {}
    proved_current_versions: dict[tuple[str, str | None], int] = {}
    for entry in entries:
        service_name = entry['service_name']
        if service_name not in candidate_services:
            continue
        version = entry['version']
        service_hash = entry.get('service_hash')
        prior_hash = terminal_hashes.get(service_name, service_hash)
        if service_name in terminal_hashes and prior_hash != service_hash:
            add('terminal_manifest_mixed_service_incarnations', service_name,
                version)
            continue
        terminal_hashes[service_name] = service_hash
        boundary = (service_name, service_hash)
        high_waters[boundary] = max(high_waters.get(boundary, 0), version)
        facts = entry.get('dependency_facts')
        proved_current = (facts.get('service_current_version') if isinstance(
            facts, dict) else None)
        if type(proved_current) is int:
            proved_current_versions[boundary] = max(
                proved_current_versions.get(boundary, 0), proved_current)

    current_by_identity: dict[tuple[str, int], Mapping[str, Any]] = {}
    for row in current_rows:
        service_name = row.get('service_name')
        version = row.get('version')
        if (set(row) != _VERSION_SPEC_COLUMNS or
                not isinstance(service_name, str) or not service_name or
                type(version) is not int or version < 1 or
            (service_name, version) in current_by_identity):
            add('invalid_or_duplicate_current_version_row', service_name,
                version)
            continue
        if service_name not in candidate_services:
            add('unexpected_current_inventory_service', service_name, version)
            continue
        current_by_identity[(service_name, version)] = row
        classification = current_classifications.get((service_name, version))
        if type(classification) is not str:
            add('missing_current_row_classification', service_name, version)
    unexpected_observations = set(current_service_hashes) - candidate_services
    for service_name in sorted(unexpected_observations,
                               key=lambda value:
                               (type(value).__name__, repr(value))):
        add('unexpected_current_parent_hash_observation', service_name)
    normalized_hashes: dict[str, str | None] = {}
    invalid_hash_services: set[str] = set()
    for service_name in candidate_services:
        observation = current_service_hashes.get(service_name)
        if observation is None:
            add('missing_current_parent_hash_observation', service_name)
            invalid_hash_services.add(service_name)
            continue
        if type(observation) is not ServiceHashObservation:
            add('invalid_current_parent_hash_observation', service_name)
            invalid_hash_services.add(service_name)
            continue
        try:
            normalized_hashes[service_name] = observation.validated_value()
        except ValueError:
            add('invalid_current_parent_hash_observation', service_name)
            invalid_hash_services.add(service_name)

    stale_entries = {
        (entry['service_name'], entry['version']): entry
        for entry in entries
        if entry.get('classification') ==
        ManifestClassification.STALE_PLACEHOLDER.value
    }
    for identity, stale_entry in stale_entries.items():
        service_name, version = identity
        if service_name in invalid_hash_services:
            continue
        terminal_hash = stale_entry.get('service_hash')
        current_hash = normalized_hashes.get(service_name)
        current_row = current_by_identity.get(identity)
        if current_hash == terminal_hash and terminal_hash is not None:
            if current_row is None:
                add('manifested_stale_row_missing', service_name, version)
                continue
            if (current_classifications.get(identity)
                    != ManifestClassification.PLACEHOLDER.value or
                    not _is_raw_unretired_placeholder(current_row,
                                                      fillable=False) or
                    _column_sha256s(current_row)
                    != stale_entry.get('result_column_sha256s') or
                    _row_sha256(current_row)
                    != stale_entry.get('result_row_sha256')):
                add('manifested_stale_row_drift', service_name, version)
        elif current_hash is None:
            # Teardown may delete immutable live rows without deleting their
            # terminal audit record.  An orphan still present has no parent
            # incarnation to which it can be safely associated.
            if current_row is not None:
                add('orphaned_terminal_row_after_parent_deletion', service_name,
                    version)
        elif current_row is not None and (
                current_classifications.get(identity)
                != ManifestClassification.EXPLICIT_V2.value or
                not _is_raw_post_terminal_explicit_v2(current_row,
                                                      completed_at)):
            add('old_stale_placeholder_survives_recreation', service_name,
                version)

    for candidate_entry in entries:
        if (candidate_entry.get('classification')
                != ManifestClassification.HISTORICAL_PHYSICAL_PER_GPU.value or
                candidate_entry.get('outcome') != 'retired'):
            continue
        service_name = candidate_entry['service_name']
        version = candidate_entry['version']
        terminal_hash = candidate_entry.get('service_hash')
        if (service_name not in invalid_hash_services and
                normalized_hashes.get(service_name) == terminal_hash and
                terminal_hash is not None and
            (service_name, version) not in current_by_identity):
            add('retired_candidate_row_missing', service_name, version)

    # The surviving committed current row is the durable reason every stale
    # placeholder below it can never be filled.  Keep that exact row present
    # for as long as the proved service incarnation itself remains present;
    # otherwise a parent could retain the terminal hash while losing the
    # high-water postimage on which the stale proof depends.
    for boundary, current_version in proved_current_versions.items():
        service_name, terminal_hash = boundary
        if (service_name not in invalid_hash_services and
                terminal_hash is not None and
                normalized_hashes.get(service_name) == terminal_hash and
            (service_name, current_version) not in current_by_identity):
            add('manifested_current_row_missing', service_name, current_version)

    for identity, row in current_by_identity.items():
        service_name, version = identity
        if service_name in invalid_hash_services:
            continue
        classification = current_classifications.get(identity)
        current_hash = normalized_hashes.get(service_name)
        terminal_hash = terminal_hashes.get(service_name)
        manifest_entry = manifest_by_identity.get(identity)

        if current_hash is None:
            if manifest_entry is None or identity not in stale_entries:
                add('current_row_without_parent_service', service_name, version)
            continue

        same_incarnation = (service_name in terminal_hashes and
                            terminal_hash is not None and
                            current_hash == terminal_hash)
        if not same_incarnation:
            if (classification == ManifestClassification.EXPLICIT_V2.value and
                    _is_raw_post_terminal_explicit_v2(row, completed_at)):
                continue
            if identity not in stale_entries:
                add('invalid_new_incarnation_version_row', service_name,
                    version)
            continue

        assert terminal_hash is not None
        boundary = (service_name, terminal_hash)
        high_water = high_waters.get(boundary, 0)
        proved_current = proved_current_versions.get(boundary, 0)
        if manifest_entry is None:
            above_boundary = version > max(high_water, proved_current)
            if (classification == ManifestClassification.PLACEHOLDER.value and
                    above_boundary and
                    _is_raw_unretired_placeholder(row, fillable=True)):
                continue
            if (classification == ManifestClassification.EXPLICIT_V2.value and
                    above_boundary and
                    _is_raw_post_terminal_explicit_v2(row, completed_at)):
                continue
            add('invalid_same_incarnation_post_terminal_row', service_name,
                version)
            continue

        manifest_classification = manifest_entry.get('classification')
        if manifest_classification == (
                ManifestClassification.STALE_PLACEHOLDER.value):
            # Exact stale validation was performed above.
            continue
        if manifest_classification == ManifestClassification.PLACEHOLDER.value:
            if (classification == ManifestClassification.PLACEHOLDER.value and
                    _is_raw_unretired_placeholder(row, fillable=True) and
                    _column_sha256s(row)
                    == manifest_entry.get('result_column_sha256s') and
                    _row_sha256(row)
                    == manifest_entry.get('result_row_sha256')):
                continue
            if (classification == ManifestClassification.EXPLICIT_V2.value and
                    _is_raw_post_terminal_explicit_v2(row, completed_at)):
                continue
            add('invalid_terminal_placeholder_transition', service_name,
                version)
            continue

        if (manifest_entry.get('outcome') == 'retired' or
                manifest_classification
                == ManifestClassification.RETIRED.value):
            # Raw pickle analysis reports canonical None as ``placeholder``;
            # the exact frozen postimage below supplies the contextual
            # tombstone classification without polluting the raw enum.
            result_columns = manifest_entry.get('result_column_sha256s')
            current_columns = _column_sha256s(row)
            if (classification != ManifestClassification.PLACEHOLDER.value or
                    not isinstance(result_columns, dict) or
                    any(current_columns[column] != result_columns[column]
                        for column in _RETIREMENT_COLUMNS)):
                add('retired_terminal_row_drift', service_name, version)
            continue

        current_spec = _raw_spec_bytes(row)
        if (current_spec is None or _sha256(current_spec)
                != manifest_entry.get('result_spec_sha256')):
            add('tracked_terminal_spec_drift', service_name, version)

    return issues


def validate_current_inventory(
    run: Mapping[str, Any],
    entries: Sequence[Mapping[str, Any]],
    current_rows: Sequence[Mapping[str, Any]],
    current_service_hashes: Mapping[str, ServiceHashObservation],
    current_classifications: Mapping[tuple[str, int], str],
) -> None:
    """Raise if live state violates a terminal protocol-4 manifest."""
    mismatches = current_inventory_mismatches(run, entries, current_rows,
                                              current_service_hashes,
                                              current_classifications)
    if mismatches:
        raise PlacementNormalizationManifestError(mismatches)
