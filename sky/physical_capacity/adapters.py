"""Pure mapping-version-1 adapters for bounded capacity evidence scans.

The adapters normalize only scalar rows supplied by ``SourceReader``.  They do
not call workload getters, deserialize resource/blob columns, import a cloud
provider, or perform a network operation.  Current registry hashes remain
legacy association evidence because source and registry writes were not
atomic.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
import dataclasses
from typing import Any
import uuid

from sky.jobs import naming as managed_job_naming
from sky.jobs import status_types as managed_job_statuses
from sky.physical_capacity import canonical
from sky.physical_capacity import contracts
from sky.physical_capacity import models
from sky.physical_capacity import source_queries
from sky.serve import controller_transport
from sky.serve import serve_statuses
from sky.utils import status_lib

_SERVE_PRESENT_STATUSES = frozenset({
    serve_statuses.ReplicaStatus.PENDING,
    serve_statuses.ReplicaStatus.PROVISIONING,
    serve_statuses.ReplicaStatus.STARTING,
    serve_statuses.ReplicaStatus.READY,
    serve_statuses.ReplicaStatus.NOT_READY,
})
_SERVE_ABSENT_STATUSES = frozenset({
    serve_statuses.ReplicaStatus.SHUTTING_DOWN,
    serve_statuses.ReplicaStatus.FAILED,
    serve_statuses.ReplicaStatus.FAILED_INITIAL_DELAY,
    serve_statuses.ReplicaStatus.FAILED_PROBING,
    serve_statuses.ReplicaStatus.FAILED_PROVISION,
    serve_statuses.ReplicaStatus.FAILED_CLEANUP,
    serve_statuses.ReplicaStatus.PREEMPTED,
})
_SERVE_RETIRING_STATUSES = frozenset({
    serve_statuses.ServiceStatus.SHUTTING_DOWN,
    serve_statuses.ServiceStatus.FAILED_CLEANUP,
})
_MANAGED_PROCESSING_STATUSES = frozenset(
    managed_job_statuses.ManagedJobStatus.processing_statuses())
_MANAGED_TERMINAL_STATUSES = frozenset(
    managed_job_statuses.ManagedJobStatus.terminal_statuses())
_REGISTRY_OBSERVED_STATES = {
    'INIT': contracts.EvidenceObservedState.PROVISIONING,
    'UP': contracts.EvidenceObservedState.UP,
    'STOPPED': contracts.EvidenceObservedState.STOPPED,
    'AUTOSTOPPING': contracts.EvidenceObservedState.PARTIAL,
}


@dataclasses.dataclass
class _CandidateDraft:
    """Mutable candidate kept only until duplicate hashes are resolved."""

    adapter: str
    discriminator_name: str
    discriminator_value: int
    group_hash: str
    workspace: str
    expected_attribution: tuple[str, str, int]
    desired_state: contracts.EvidenceDesiredState
    association_status: contracts.EvidenceAssociationStatus
    observed_state: contracts.EvidenceObservedState
    cluster_hash: str | None
    conflicted: bool
    scalar_placement_hash: str | None = None

    def mark_duplicate(self) -> None:
        self.association_status = (
            contracts.EvidenceAssociationStatus.REGISTRY_UNSAFE)
        self.observed_state = contracts.EvidenceObservedState.UNKNOWN
        self.cluster_hash = None
        self.scalar_placement_hash = None
        self.conflicted = True

    def to_record(self) -> contracts.AllocationCandidateEvidenceRecord:
        payload: dict[str, object] = {
            'adapter': self.adapter,
            'mapping_version': contracts.MAPPING_VERSION,
            'group_source_incarnation_hash': self.group_hash,
            self.discriminator_name: self.discriminator_value,
        }
        if self.cluster_hash is not None:
            payload['cluster_hash'] = self.cluster_hash
            identity_confidence = (contracts.EvidenceIdentityConfidence.LEGACY)
        else:
            payload['association_status'] = self.association_status.value
            identity_confidence = (contracts.EvidenceIdentityConfidence.UNKNOWN)
        source_hash = canonical.canonical_hash(
            payload, domain=canonical.CanonicalDomain.SOURCE_INCARNATION)
        return contracts.AllocationCandidateEvidenceRecord(
            source_incarnation_hash=source_hash,
            group_source_incarnation_hash=self.group_hash,
            identity_confidence=identity_confidence,
            association_status=self.association_status,
            desired_state=self.desired_state,
            observed_state=self.observed_state,
            scalar_placement_hash=self.scalar_placement_hash)


@dataclasses.dataclass(frozen=True)
class _RegistryAssociation:
    """Closed result of one exact current-registry point lookup."""

    status: contracts.EvidenceAssociationStatus
    cluster_hash: str | None
    observed_state: contracts.EvidenceObservedState
    conflicted: bool


def _bounded_optional_string(value: object,
                             *,
                             field: str,
                             allow_empty: bool = False) -> str | None:
    if value is None:
        return None
    return canonical.validate_bounded_string(
        value,
        max_bytes=canonical.MAX_CANONICAL_STRING_BYTES,
        field=field,
        allow_empty=allow_empty)


def _valid_positive_integer(value: object) -> bool:
    return type(value) is int and value > 0


def _valid_nonnegative_integer(value: object) -> bool:
    return type(value) is int and value >= 0


def _parse_uuid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError):
        return None


def _row_value(row: Mapping[str, Any], key: str) -> Any:
    if key not in row:
        raise source_queries.SourceDecodeError(
            f'Bounded source row omitted required scalar {key!r}.')
    return row[key]


def _validate_attribution(
    row: Mapping[str, Any],
    expected: tuple[str, str, int],
) -> bool:
    actual = (_row_value(row, 'workload_type'), _row_value(row, 'workload_id'),
              _row_value(row, 'workload_task_id'))
    if actual == (None, None, None):
        return True
    return actual == expected


def _registry_association(
    reader: source_queries.SourceReader,
    *,
    cluster_name: object,
    workspace: str,
    expected_attribution: tuple[str, str, int],
) -> _RegistryAssociation:
    if not isinstance(cluster_name, str) or not cluster_name:
        return _RegistryAssociation(
            contracts.EvidenceAssociationStatus.SOURCE_MALFORMED, None,
            contracts.EvidenceObservedState.UNKNOWN, True)
    try:
        canonical.validate_bounded_string(
            cluster_name,
            max_bytes=canonical.MAX_SOURCE_KEY_BYTES,
            field='Registry cluster name')
    except ValueError:
        return _RegistryAssociation(
            contracts.EvidenceAssociationStatus.SOURCE_MALFORMED, None,
            contracts.EvidenceObservedState.UNKNOWN, True)

    row = reader.cluster(cluster_name)
    if row is None:
        return _RegistryAssociation(
            contracts.EvidenceAssociationStatus.REGISTRY_MISSING, None,
            contracts.EvidenceObservedState.UNKNOWN, False)
    if _row_value(row, 'name') != cluster_name:
        raise source_queries.SourceDecodeError(
            'Registry point lookup returned a different cluster name.')

    registry_workspace = _row_value(row, 'workspace')
    if (isinstance(registry_workspace, str) and
            registry_workspace != workspace):
        raise source_queries.SourceConflictError(
            'Registry row belongs to another workspace.')

    raw_hash = _row_value(row, 'cluster_hash')
    try:
        cluster_hash = _bounded_optional_string(raw_hash,
                                                field='Registry cluster_hash')
    except ValueError:
        cluster_hash = None
    safe = (cluster_hash is not None and bool(cluster_hash) and
            type(_row_value(row, 'is_managed')) is int and
            _row_value(row, 'is_managed') == 1 and
            registry_workspace == workspace and
            _validate_attribution(row, expected_attribution))
    if not safe:
        return _RegistryAssociation(
            contracts.EvidenceAssociationStatus.REGISTRY_UNSAFE, None,
            contracts.EvidenceObservedState.UNKNOWN, True)

    raw_status = _row_value(row, 'status')
    try:
        registry_status = status_lib.ClusterStatus(raw_status)
    except (TypeError, ValueError):
        registry_status = None
    observed = (_REGISTRY_OBSERVED_STATES.get(registry_status.value)
                if registry_status is not None else None)
    status_conflict = registry_status is None
    return _RegistryAssociation(
        contracts.EvidenceAssociationStatus.REGISTRY_HASH, cluster_hash,
        observed or contracts.EvidenceObservedState.UNKNOWN, status_conflict)


def _scalar_placement(
    reader: source_queries.SourceReader,
    *,
    cluster_hash: str,
    workspace: str,
    expected_attribution: tuple[str, str, int],
) -> tuple[str | None, bool]:
    row = reader.cluster_history(cluster_hash)
    if row is None:
        return None, False
    if _row_value(row, 'cluster_hash') != cluster_hash:
        raise source_queries.SourceDecodeError(
            'History point lookup returned a different cluster hash.')
    history_workspace = _row_value(row, 'workspace')
    if (isinstance(history_workspace, str) and history_workspace != workspace):
        raise source_queries.SourceConflictError(
            'History row belongs to another workspace.')
    attribution_conflict = not _validate_attribution(row, expected_attribution)
    if attribution_conflict:
        return None, True
    if (history_workspace != workspace or
            type(_row_value(row, 'is_managed')) is not int or
            _row_value(row, 'is_managed') != 1):
        return None, False

    normalized: dict[str, str | int | bool | None] = {}
    for source_field, payload_field in (('cloud', 'provider'),
                                        ('region', 'region'), ('zone', 'zone')):
        raw_value = _row_value(row, source_field)
        try:
            value = _bounded_optional_string(raw_value,
                                             field=f'History {source_field}')
        except ValueError:
            return None, False
        if value == '':
            return None, False
        normalized[payload_field] = value
    node_count = _row_value(row, 'num_nodes')
    if node_count is not None and not _valid_positive_integer(node_count):
        return None, False
    if normalized['provider'] is None and node_count is None:
        return None, False
    payload: dict[str, object] = {
        'mapping_version': contracts.MAPPING_VERSION,
        'evidence_kind': 'registry_history_scalars',
        **normalized,
        'node_count': node_count,
        'shape_known': False,
    }
    return (canonical.canonical_hash(
        payload, domain=canonical.CanonicalDomain.PHYSICAL_SPEC), False)


def _retain_record(reader: source_queries.SourceReader,
                   records: list[contracts.EvidenceRecord],
                   record: contracts.EvidenceRecord) -> None:
    encoded = canonical.canonical_json_bytes(
        record.to_payload(), domain=canonical.CanonicalDomain.EVIDENCE_RECORD)
    reader.retain_canonical_bytes(encoded)
    records.append(record)


def _add_group(findings: contracts.FindingCounts,
               record: contracts.GroupEvidenceRecord, *,
               conflicted: bool) -> None:
    findings.increment(contracts.FindingKey.SELECTORS_PRESENT)
    confidence_key = {
        contracts.EvidenceGroupConfidence.EXACT:
            contracts.FindingKey.GROUPS_EXACT,
        contracts.EvidenceGroupConfidence.LEGACY:
            contracts.FindingKey.GROUPS_LEGACY,
        contracts.EvidenceGroupConfidence.UNKNOWN:
            contracts.FindingKey.GROUPS_UNKNOWN,
    }[record.confidence]
    findings.increment(confidence_key)
    if conflicted:
        findings.increment(contracts.FindingKey.SOURCE_CONFLICT)


def _serve_group(
    selector: contracts.ServeSourceSelector,
    row: Mapping[str, Any],
) -> tuple[contracts.GroupEvidenceRecord, serve_statuses.ServiceStatus | None,
           bool]:
    if _row_value(row, 'name') != selector.service_name:
        raise source_queries.SelectorMismatchError(
            'Serve row key does not match its selector.')
    workspace = _row_value(row, 'workspace')
    if workspace is None:
        raise source_queries.SourceDecodeError(
            'Selected Serve workspace must not be NULL.')
    if not isinstance(workspace, str) or workspace != selector.workspace:
        raise source_queries.SelectorMismatchError(
            'Serve row workspace does not match its selector.')
    expected_pool = int(
        selector.source_kind is models.ProjectionSourceKind.SERVE_POOL)
    if (type(_row_value(row, 'pool')) is not int or
            _row_value(row, 'pool') != expected_pool):
        raise source_queries.SelectorMismatchError(
            'Serve row kind does not match its selector.')

    raw_hash = _row_value(row, 'hash')
    raw_scope = _row_value(row, 'resource_scope')
    identity_malformed = False
    try:
        service_hash = _bounded_optional_string(raw_hash,
                                                field='Serve service hash',
                                                allow_empty=True)
        resource_scope = _bounded_optional_string(raw_scope,
                                                  field='Serve resource scope',
                                                  allow_empty=True)
    except ValueError:
        service_hash = None
        resource_scope = None
        identity_malformed = True

    if identity_malformed:
        source_payload: dict[str, object] = {
            'adapter': 'serve',
            'mapping_version': contracts.MAPPING_VERSION,
            'workload_kind': selector.source_kind.value,
            'workspace': selector.workspace,
            'service_name': selector.service_name,
            'source_identity_status': 'source_malformed',
        }
    else:
        source_payload = {
            'adapter': 'serve',
            'mapping_version': contracts.MAPPING_VERSION,
            'workload_kind': selector.source_kind.value,
            'workspace': selector.workspace,
            'service_name': selector.service_name,
            'service_hash': service_hash,
            'resource_scope': resource_scope,
        }
    group_hash = canonical.canonical_hash(
        source_payload, domain=canonical.CanonicalDomain.SOURCE_INCARNATION)

    lifecycle_epoch = _row_value(row, 'lifecycle_epoch')
    lifecycle_malformed = (lifecycle_epoch is not None and
                           not _valid_positive_integer(lifecycle_epoch))
    controller_pid = _row_value(row, 'controller_pid')
    controller_port = _row_value(row, 'controller_port')
    controller_ip = _row_value(row, 'controller_ip')
    tuple_missing = (controller_pid is None and controller_port is None and
                     controller_ip is None)
    tuple_valid = False
    tuple_malformed = False
    if not tuple_missing:
        if controller_pid is None or controller_port is None:
            tuple_malformed = True
        else:
            try:
                controller_transport.make_controller_owner_fingerprint(
                    service_hash if service_hash else 'legacy', controller_pid,
                    controller_ip, controller_port)
                tuple_valid = True
            except controller_transport.ControllerOwnerError:
                tuple_malformed = True

    scope_conflict = (resource_scope is not None and
                      resource_scope != service_hash)
    exact = (not identity_malformed and bool(service_hash) and
             resource_scope == service_hash and
             _valid_positive_integer(lifecycle_epoch) and tuple_valid)
    fence_conflict = (identity_malformed or lifecycle_malformed or
                      tuple_malformed or scope_conflict)
    if exact:
        confidence = contracts.EvidenceGroupConfidence.EXACT
    elif fence_conflict:
        confidence = contracts.EvidenceGroupConfidence.UNKNOWN
    else:
        confidence = contracts.EvidenceGroupConfidence.LEGACY

    raw_status = _row_value(row, 'status')
    try:
        service_status = serve_statuses.ServiceStatus(raw_status)
    except (TypeError, ValueError):
        service_status = None
    status_conflict = service_status is None
    if service_status in _SERVE_RETIRING_STATUSES:
        lifecycle = contracts.EvidenceLifecycle.RETIRING
        status_class = contracts.EvidenceStatusClass.ABSENT
    elif service_status is not None:
        lifecycle = contracts.EvidenceLifecycle.ACTIVE
        status_class = contracts.EvidenceStatusClass.PRESENT
    else:
        lifecycle = contracts.EvidenceLifecycle.UNKNOWN
        status_class = contracts.EvidenceStatusClass.UNKNOWN
    return (contracts.GroupEvidenceRecord(group_hash, confidence, lifecycle,
                                          status_class), service_status,
            fence_conflict or status_conflict)


def _serve_desired_state(
    parent_status: serve_statuses.ServiceStatus | None,
    raw_replica_status: object,
) -> tuple[contracts.EvidenceDesiredState, bool, serve_statuses.ReplicaStatus |
           None]:
    try:
        replica_status = serve_statuses.ReplicaStatus(raw_replica_status)
    except (TypeError, ValueError):
        replica_status = None
    replica_conflict = replica_status is None
    if parent_status in _SERVE_RETIRING_STATUSES:
        return (contracts.EvidenceDesiredState.ABSENT, replica_conflict,
                replica_status)
    if parent_status is None:
        return (contracts.EvidenceDesiredState.UNKNOWN, replica_conflict,
                replica_status)
    if replica_status in _SERVE_PRESENT_STATUSES:
        desired = contracts.EvidenceDesiredState.PRESENT
    elif replica_status in _SERVE_ABSENT_STATUSES:
        desired = contracts.EvidenceDesiredState.ABSENT
    else:
        desired = contracts.EvidenceDesiredState.UNKNOWN
    return desired, replica_conflict, replica_status


def _serve_candidate(
    selector: contracts.ServeSourceSelector,
    group_hash: str,
    parent_status: serve_statuses.ServiceStatus | None,
    row: Mapping[str, Any],
    reader: source_queries.SourceReader,
) -> _CandidateDraft | None:
    if _row_value(row, 'service_name') != selector.service_name:
        raise source_queries.SourceDecodeError(
            'Serve replica key does not match its selected service.')
    replica_id = _row_value(row, 'replica_id')
    if not _valid_nonnegative_integer(replica_id):
        raise source_queries.SourceDecodeError(
            'Serve replica_id must be a non-negative integer.')
    desired, status_conflict, replica_status = _serve_desired_state(
        parent_status, _row_value(row, 'status'))
    cluster_name = _row_value(row, 'cluster_name')
    if (replica_status is serve_statuses.ReplicaStatus.PENDING and
        (cluster_name is None or cluster_name == '')):
        return None
    version = _row_value(row, 'version')
    workload_type = ('pool' if selector.source_kind
                     is models.ProjectionSourceKind.SERVE_POOL else 'service')
    expected_attribution = (workload_type, selector.service_name, version)
    if not _valid_positive_integer(version):
        association = _RegistryAssociation(
            contracts.EvidenceAssociationStatus.SOURCE_MALFORMED, None,
            contracts.EvidenceObservedState.UNKNOWN, True)
    else:
        association = _registry_association(
            reader,
            cluster_name=cluster_name,
            workspace=selector.workspace,
            expected_attribution=expected_attribution)
    return _CandidateDraft(adapter='serve',
                           discriminator_name='replica_id',
                           discriminator_value=replica_id,
                           group_hash=group_hash,
                           workspace=selector.workspace,
                           expected_attribution=expected_attribution,
                           desired_state=desired,
                           association_status=association.status,
                           observed_state=association.observed_state,
                           cluster_hash=association.cluster_hash,
                           conflicted=status_conflict or association.conflicted)


def _managed_status(
    raw_status: object,
) -> tuple[managed_job_statuses.ManagedJobStatus | None,
           contracts.EvidenceLifecycle, contracts.EvidenceStatusClass, bool]:
    try:
        status = managed_job_statuses.ManagedJobStatus(raw_status)
    except (TypeError, ValueError):
        status = None
    if status in _MANAGED_PROCESSING_STATUSES:
        return (status, contracts.EvidenceLifecycle.ACTIVE,
                contracts.EvidenceStatusClass.PRESENT, False)
    if (status in _MANAGED_TERMINAL_STATUSES or
            status is managed_job_statuses.ManagedJobStatus.CANCELLING):
        return (status, contracts.EvidenceLifecycle.RETIRING,
                contracts.EvidenceStatusClass.ABSENT, False)
    return (status, contracts.EvidenceLifecycle.UNKNOWN,
            contracts.EvidenceStatusClass.UNKNOWN, status is None)


def _managed_group(
    selector: contracts.ManagedJobTaskSelector,
    job_info: Mapping[str, Any],
    task: Mapping[str, Any],
    *,
    controller_instance_id: str,
    controller_generation: int,
) -> tuple[contracts.GroupEvidenceRecord,
           managed_job_statuses.ManagedJobStatus | None, bool]:
    if _row_value(job_info, 'spot_job_id') != selector.spot_job_id:
        raise source_queries.SelectorMismatchError(
            'Managed job row key does not match its selector.')
    if (_row_value(task, 'spot_job_id') != selector.spot_job_id or
            _row_value(task, 'task_id') != selector.task_id):
        raise source_queries.SelectorMismatchError(
            'Managed task row key does not match its selector.')
    spot_row_id = _row_value(task, 'job_id')
    if not _valid_positive_integer(spot_row_id):
        raise source_queries.SourceDecodeError(
            'Managed spot.job_id must be a positive integer.')

    workspace = _row_value(job_info, 'workspace')
    null_workspace = workspace is None
    if null_workspace:
        if selector.workspace != 'default':
            raise source_queries.SelectorMismatchError(
                'NULL managed workspace maps only to default.')
        workspace = 'default'
    elif not isinstance(workspace, str) or workspace != selector.workspace:
        raise source_queries.SelectorMismatchError(
            'Managed row workspace does not match its selector.')
    if type(_row_value(job_info, 'is_batch')) is not bool:
        raise source_queries.SourceDecodeError(
            'Managed job_info.is_batch must be a non-null Boolean.')

    status, lifecycle, status_class, status_conflict = _managed_status(
        _row_value(task, 'status'))
    raw_instance = _row_value(job_info, 'controller_instance_id')
    raw_generation = _row_value(job_info, 'controller_generation')
    if raw_instance is None and raw_generation is None:
        confidence = contracts.EvidenceGroupConfidence.LEGACY
        fence_conflict = False
    else:
        parsed_instance = _parse_uuid(raw_instance)
        valid_pair = (parsed_instance is not None and
                      _valid_positive_integer(raw_generation))
        nonterminal = status in _MANAGED_PROCESSING_STATUSES
        current_pair = (parsed_instance == controller_instance_id and
                        raw_generation == controller_generation)
        if valid_pair and (not nonterminal or current_pair):
            confidence = contracts.EvidenceGroupConfidence.EXACT
            fence_conflict = False
        else:
            confidence = contracts.EvidenceGroupConfidence.UNKNOWN
            fence_conflict = True
    if null_workspace and confidence is contracts.EvidenceGroupConfidence.EXACT:
        confidence = contracts.EvidenceGroupConfidence.LEGACY

    group_payload = {
        'adapter': 'managed_jobs',
        'mapping_version': contracts.MAPPING_VERSION,
        'workload_kind': 'managed_job_task',
        'workspace': selector.workspace,
        'spot_job_id': selector.spot_job_id,
        'task_id': selector.task_id,
        'spot_row_id': spot_row_id,
    }
    group_hash = canonical.canonical_hash(
        group_payload, domain=canonical.CanonicalDomain.SOURCE_INCARNATION)
    return (contracts.GroupEvidenceRecord(group_hash, confidence, lifecycle,
                                          status_class), status,
            fence_conflict or status_conflict)


def _managed_desired_state(
    status: managed_job_statuses.ManagedJobStatus | None,
) -> contracts.EvidenceDesiredState:
    if status in _MANAGED_PROCESSING_STATUSES:
        return contracts.EvidenceDesiredState.PRESENT
    if (status in _MANAGED_TERMINAL_STATUSES or
            status is managed_job_statuses.ManagedJobStatus.CANCELLING):
        return contracts.EvidenceDesiredState.ABSENT
    return contracts.EvidenceDesiredState.UNKNOWN


def _managed_candidate(
    selector: contracts.ManagedJobTaskSelector,
    group_hash: str,
    status: managed_job_statuses.ManagedJobStatus | None,
    task: Mapping[str, Any],
    reader: source_queries.SourceReader,
) -> tuple[_CandidateDraft | None, bool]:
    task_name = _row_value(task, 'task_name')
    malformed_name = False
    try:
        if not isinstance(task_name, str) or not task_name:
            raise ValueError('missing task name')
        cluster_name = managed_job_naming.generate_managed_job_cluster_name(
            task_name, selector.spot_job_id)
        canonical.validate_bounded_string(
            cluster_name,
            max_bytes=canonical.MAX_SOURCE_KEY_BYTES,
            field='Derived managed cluster name')
    except (TypeError, ValueError, UnicodeError):
        cluster_name = None
        malformed_name = True
    expected_attribution = ('managed_job', str(selector.spot_job_id),
                            selector.task_id)
    if malformed_name:
        association = _RegistryAssociation(
            contracts.EvidenceAssociationStatus.SOURCE_MALFORMED, None,
            contracts.EvidenceObservedState.UNKNOWN, True)
    else:
        association = _registry_association(
            reader,
            cluster_name=cluster_name,
            workspace=selector.workspace,
            expected_attribution=expected_attribution)
    if (status is managed_job_statuses.ManagedJobStatus.PENDING and
            association.status
            is contracts.EvidenceAssociationStatus.REGISTRY_MISSING):
        return None, True
    spot_row_id = _row_value(task, 'job_id')
    return (_CandidateDraft(adapter='managed_jobs',
                            discriminator_name='spot_row_id',
                            discriminator_value=spot_row_id,
                            group_hash=group_hash,
                            workspace=selector.workspace,
                            expected_attribution=expected_attribution,
                            desired_state=_managed_desired_state(status),
                            association_status=association.status,
                            observed_state=association.observed_state,
                            cluster_hash=association.cluster_hash,
                            conflicted=(association.conflicted or
                                        status is None)), False)


def _validate_pool_service(selector: contracts.ServeSourceSelector,
                           row: Mapping[str, Any]) -> None:
    if (_row_value(row, 'name') != selector.service_name or
            _row_value(row, 'workspace') != selector.workspace or
            type(_row_value(row, 'pool')) is not int or
            _row_value(row, 'pool') != 1):
        raise source_queries.SelectorMismatchError(
            'Diagnostic pool row contradicts its configured selector.')


def _pool_link_unambiguous(
    reader: source_queries.SourceReader,
    *,
    workspace: str,
    pool_name: object,
    cluster_name: object,
    pool_selectors: Mapping[str, contracts.ServeSourceSelector],
) -> bool:
    if (not isinstance(pool_name, str) or not pool_name or
            not isinstance(cluster_name, str) or not cluster_name):
        return False
    selector = pool_selectors.get(pool_name)
    if selector is None or selector.workspace != workspace:
        return False
    service = reader.service(pool_name)
    if service is None:
        return False
    _validate_pool_service(selector, service)
    matches: list[Mapping[str, Any]] = []
    for replica in reader.replicas(pool_name):
        if (_row_value(replica, 'service_name') != pool_name or
                not _valid_nonnegative_integer(_row_value(
                    replica, 'replica_id'))):
            raise source_queries.SourceDecodeError(
                'Diagnostic pool replica has an invalid primary key.')
        if _row_value(replica, 'cluster_name') == cluster_name:
            matches.append(replica)
    if len(matches) != 1:
        return False
    version = _row_value(matches[0], 'version')
    if not _valid_positive_integer(version):
        return False
    association = _registry_association(reader,
                                        cluster_name=cluster_name,
                                        workspace=workspace,
                                        expected_attribution=('pool', pool_name,
                                                              version))
    return (association.status
            is contracts.EvidenceAssociationStatus.REGISTRY_HASH)


def _finalize_candidates(
    reader: source_queries.SourceReader,
    candidates: list[_CandidateDraft],
    records: list[contracts.EvidenceRecord],
    findings: contracts.FindingCounts,
) -> None:
    by_hash: dict[str, list[_CandidateDraft]] = {}
    for candidate in candidates:
        if candidate.cluster_hash is not None:
            by_hash.setdefault(candidate.cluster_hash, []).append(candidate)
    for duplicates in by_hash.values():
        if len(duplicates) > 1:
            for duplicate in duplicates:
                duplicate.mark_duplicate()

    for candidate in candidates:
        reader.check_deadline()
        if candidate.cluster_hash is not None:
            placement_hash, placement_conflict = _scalar_placement(
                reader,
                cluster_hash=candidate.cluster_hash,
                workspace=candidate.workspace,
                expected_attribution=candidate.expected_attribution)
            candidate.scalar_placement_hash = placement_hash
            candidate.conflicted = candidate.conflicted or placement_conflict
        record = candidate.to_record()
        _retain_record(reader, records, record)
        findings.increment(contracts.FindingKey.ALLOCATION_CANDIDATES)
        findings.increment(contracts.FindingKey.IDENTITY_GAP)
        findings.increment(contracts.FindingKey.SELECTED_SPEC_GAP)
        if record.identity_confidence is (
                contracts.EvidenceIdentityConfidence.LEGACY):
            findings.increment(contracts.FindingKey.ALLOCATIONS_LEGACY)
        else:
            findings.increment(contracts.FindingKey.ALLOCATIONS_UNKNOWN)
        desired_key = {
            contracts.EvidenceDesiredState.PRESENT:
                contracts.FindingKey.DESIRED_PRESENT,
            contracts.EvidenceDesiredState.ABSENT:
                contracts.FindingKey.DESIRED_ABSENT,
            contracts.EvidenceDesiredState.UNKNOWN:
                contracts.FindingKey.DESIRED_UNKNOWN,
        }[record.desired_state]
        findings.increment(desired_key)
        if record.scalar_placement_hash is not None:
            findings.increment(contracts.FindingKey.SCALAR_PLACEMENT_KNOWN)
        if candidate.conflicted:
            findings.increment(contracts.FindingKey.SOURCE_CONFLICT)


def scan_partition(
    partition: contracts.SourcePartition,
    selectors: Sequence[contracts.SourceSelector],
    dependency_selectors: Sequence[contracts.SourceSelector],
    reader: source_queries.SourceReader,
    *,
    controller_instance_id: str,
    controller_generation: int,
) -> contracts.PartitionEvidenceResult:
    """Normalize one typed-selector partition into ephemeral evidence DTOs."""
    if not isinstance(partition, contracts.SourcePartition):
        raise TypeError('partition must be a SourcePartition.')
    normalized_instance = _parse_uuid(controller_instance_id)
    if normalized_instance is None or not _valid_positive_integer(
            controller_generation):
        raise ValueError(
            'Current controller identity must be a UUID/generation.')
    if len(set(selectors)) != len(selectors):
        raise source_queries.SelectorMismatchError(
            'Partition selectors must not contain duplicates.')
    for selector in selectors:
        if contracts.selector_partition(selector) != partition:
            raise source_queries.SelectorMismatchError(
                'Primary selector does not belong to this partition.')
    if not selectors:
        raise source_queries.SelectorMismatchError(
            'A source partition must contain a primary selector.')

    findings = contracts.FindingCounts()
    records: list[contracts.EvidenceRecord] = []
    candidates: list[_CandidateDraft] = []
    pool_selectors = {
        selector.service_name: selector
        for selector in dependency_selectors
        if isinstance(selector, contracts.ServeSourceSelector) and
        selector.source_kind is models.ProjectionSourceKind.SERVE_POOL and
        selector.workspace == partition.workspace
    }

    if partition.source_kind in (models.ProjectionSourceKind.SERVE_SERVICE,
                                 models.ProjectionSourceKind.SERVE_POOL):
        for untyped_selector in selectors:
            reader.check_deadline()
            if not isinstance(untyped_selector, contracts.ServeSourceSelector):
                raise source_queries.SelectorMismatchError(
                    'Serve partition contains a managed-job selector.')
            selector = untyped_selector
            service = reader.service(selector.service_name)
            if service is None:
                findings.increment(contracts.FindingKey.SELECTORS_MISSING)
                continue
            group, parent_status, group_conflict = _serve_group(
                selector, service)
            _retain_record(reader, records, group)
            _add_group(findings, group, conflicted=group_conflict)
            for replica in reader.replicas(selector.service_name):
                candidate = _serve_candidate(selector,
                                             group.source_incarnation_hash,
                                             parent_status, replica, reader)
                if candidate is None:
                    findings.increment(contracts.FindingKey.NO_CLUSTER_YET)
                else:
                    candidates.append(candidate)
    elif partition.source_kind is models.ProjectionSourceKind.MANAGED_JOB_TASK:
        for untyped_selector in selectors:
            reader.check_deadline()
            if not isinstance(untyped_selector,
                              contracts.ManagedJobTaskSelector):
                raise source_queries.SelectorMismatchError(
                    'Managed partition contains a Serve selector.')
            selector = untyped_selector
            job_info = reader.job_info(selector.spot_job_id)
            tasks = reader.spot_tasks(selector.spot_job_id, selector.task_id)
            if len(tasks) > 1:
                raise source_queries.SourceConflictError(
                    'Managed logical task key has duplicate rows.')
            if job_info is None or not tasks:
                findings.increment(contracts.FindingKey.SELECTORS_MISSING)
                continue
            task = tasks[0]
            group, status, group_conflict = _managed_group(
                selector,
                job_info,
                task,
                controller_instance_id=normalized_instance,
                controller_generation=controller_generation)
            _retain_record(reader, records, group)
            _add_group(findings, group, conflicted=group_conflict)
            pool = _row_value(job_info, 'pool')
            is_batch = _row_value(job_info, 'is_batch')
            current_cluster_name = _row_value(job_info, 'current_cluster_name')
            if pool is not None:
                if not is_batch and current_cluster_name is not None:
                    findings.increment(
                        contracts.FindingKey.POOL_ASSIGNMENT_UNFENCED)
                    if not _pool_link_unambiguous(
                            reader,
                            workspace=selector.workspace,
                            pool_name=pool,
                            cluster_name=current_cluster_name,
                            pool_selectors=pool_selectors):
                        findings.increment(
                            contracts.FindingKey.POOL_ASSIGNMENT_AMBIGUOUS)
                continue
            candidate, no_cluster_yet = _managed_candidate(
                selector, group.source_incarnation_hash, status, task, reader)
            if no_cluster_yet:
                findings.increment(contracts.FindingKey.NO_CLUSTER_YET)
            elif candidate is not None:
                candidates.append(candidate)
    else:
        raise source_queries.SelectorMismatchError(
            'Unknown source partition kind.')

    _finalize_candidates(reader, candidates, records, findings)
    findings.source_rows = reader.source_rows
    result = contracts.PartitionEvidenceResult(tuple(records), findings,
                                               reader.rows_seen)
    result.validate(len(selectors))
    reader.check_deadline()
    return result


def scan_serve_partition(
    partition: contracts.SourcePartition,
    selectors: Sequence[contracts.ServeSourceSelector],
    reader: source_queries.SourceReader,
    *,
    controller_instance_id: str,
    controller_generation: int,
) -> contracts.PartitionEvidenceResult:
    """Typed convenience wrapper for a Serve service or pool partition."""
    return scan_partition(partition,
                          selectors,
                          selectors,
                          reader,
                          controller_instance_id=controller_instance_id,
                          controller_generation=controller_generation)


def scan_managed_job_partition(
    partition: contracts.SourcePartition,
    selectors: Sequence[contracts.ManagedJobTaskSelector],
    dependency_selectors: Sequence[contracts.SourceSelector],
    reader: source_queries.SourceReader,
    *,
    controller_instance_id: str,
    controller_generation: int,
) -> contracts.PartitionEvidenceResult:
    """Typed convenience wrapper including same-workspace pool dependencies."""
    return scan_partition(partition,
                          selectors,
                          dependency_selectors,
                          reader,
                          controller_instance_id=controller_instance_id,
                          controller_generation=controller_generation)
