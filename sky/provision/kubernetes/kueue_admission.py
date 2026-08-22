"""Pure CoreV1 Pod classification for reserved-fill Kueue admission.

This module performs no Kubernetes RPCs and imports neither SkyServe nor its
database.  Callers read one exact Pod, classify it here, and pass the typed
observation to the runtime callback that owns durable state.
"""

from collections.abc import Mapping
import dataclasses
import re
from typing import Any, NoReturn, TYPE_CHECKING

from sky.provision import common
from sky.provision import constants as provision_constants
from sky.provision.kubernetes import constants
from sky.provision.kubernetes import pod_spec as pod_spec_lib

if TYPE_CHECKING:
    from kubernetes.client import V1Pod


@dataclasses.dataclass(frozen=True)
class KueuePodAdmissionExpectation:
    """Hash-bound static worker facts plus one dynamic lane identity."""

    namespace: str
    cluster_name_on_cloud: str
    local_queue_name: str
    cluster_queue_name: str
    workload_priority_class_name: str | None
    pod_group_total_count: int
    priority_class_name: str | None
    priority_value: int | None
    preemption_policy: str | None
    service_account_name: str
    scheduler_name: str
    accelerator: str
    accelerator_label_key: str
    accelerator_label_values: tuple[str, ...]
    accelerator_resource_key: str
    accelerator_count: int
    identity: common.KueuePodAdmissionIdentity

    def __post_init__(self) -> None:
        for field_name in ('namespace', 'cluster_name_on_cloud',
                           'local_queue_name', 'cluster_queue_name',
                           'service_account_name', 'scheduler_name',
                           'accelerator', 'accelerator_label_key',
                           'accelerator_resource_key'):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise ValueError(f'{field_name} must be a nonempty string.')
        if (not isinstance(self.workload_priority_class_name, str) or
                not self.workload_priority_class_name):
            raise ValueError('A Kueue lane requires a nonempty '
                             'workload_priority_class_name.')
        if (not isinstance(self.priority_class_name, str) or
                not self.priority_class_name):
            raise ValueError('A Kueue lane requires a nonempty '
                             'priority_class_name.')
        if (type(self.priority_value) is not int or
                self.priority_value < -2147483648 or
                self.priority_value > 1000000000):
            raise ValueError('priority_value must be a Kubernetes priority '
                             'integer.')
        if self.preemption_policy not in ('Never', 'PreemptLowerPriority'):
            raise ValueError('preemption_policy must be Never, '
                             'or PreemptLowerPriority.')
        if (type(self.pod_group_total_count) is not int or
                self.pod_group_total_count < 1):
            raise ValueError('pod_group_total_count must be positive.')
        if (type(self.accelerator_count) is not int or
                self.accelerator_count < 1):
            raise ValueError('accelerator_count must be positive.')
        values = self.accelerator_label_values
        if (not isinstance(values, tuple) or not values or any(
                not isinstance(value, str) or not value for value in values) or
                len(set(values)) != len(values)):
            raise ValueError('accelerator_label_values must be a nonempty '
                             'unique tuple of strings.')
        if not isinstance(self.identity, common.KueuePodAdmissionIdentity):
            raise TypeError('identity must be KueuePodAdmissionIdentity.')


class KueuePodAdmissionClassificationError(ValueError):
    """One exact CoreV1 Pod does not satisfy the expected lane contract."""

    def __init__(self, identity_name: str, actual: object,
                 expected: object) -> None:
        super().__init__(f'{identity_name}: {actual!r}; expected {expected!r}')
        self.identity_name = identity_name
        self.actual = actual
        self.expected = expected


def identity_annotations(
        identity: common.KueuePodAdmissionIdentity) -> dict[str, str]:
    """Return the closed dynamic annotation set for one durable intent."""
    if not isinstance(identity, common.KueuePodAdmissionIdentity):
        raise TypeError('identity must be KueuePodAdmissionIdentity.')
    return {
        constants.RESERVED_FILL_INTENT_KEY_ANNOTATION: identity.intent_key,
        constants.RESERVED_FILL_REPLICA_RECORD_UUID_ANNOTATION:
            identity.replica_record_uuid,
        constants.RESERVED_FILL_POOL_PHYSICAL_UID_ANNOTATION:
            identity.pool_physical_uid,
        constants.RESERVED_FILL_WORKER_PROJECTION_SHA256_ANNOTATION:
            identity.worker_projection_sha256,
    }


def install_dynamic_identity_annotations(
        pod: dict[str,
                  Any], identity: common.KueuePodAdmissionIdentity) -> None:
    """Install identity after static projection rendering, rejecting clashes.

    The annotation values intentionally do not alter the static worker
    projection digest.  Instead the already-computed digest is one of the
    dynamic values and the classifier below requires that exact value.
    """
    if not isinstance(pod, dict):
        raise TypeError('Pod must be a mutable mapping.')
    metadata = pod.setdefault('metadata', {})
    if not isinstance(metadata, dict):
        raise ValueError('Pod metadata must be a mutable mapping.')
    annotations = metadata.setdefault('annotations', {})
    if not isinstance(annotations, dict):
        raise ValueError('Pod annotations must be a mutable mapping.')
    collision = sorted(constants.RESERVED_FILL_IDENTITY_ANNOTATION_KEYS &
                       set(annotations))
    if collision:
        raise ValueError('Caller Pod metadata collides with server-owned '
                         f'reserved-fill annotations: {collision!r}.')
    annotations.update(identity_annotations(identity))


def _field(value: object, yaml_name: str, api_name: str | None = None) -> Any:
    if isinstance(value, Mapping):
        if yaml_name in value:
            return value[yaml_name]
        if api_name is not None:
            return value.get(api_name)
        return None
    if api_name is not None and hasattr(value, api_name):
        return getattr(value, api_name)
    return getattr(value, yaml_name, None)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _reject(identity_name: str, actual: object, expected: object) -> NoReturn:
    raise KueuePodAdmissionClassificationError(identity_name, actual, expected)


def _require_equal(identity_name: str, actual: object,
                   expected: object) -> None:
    if actual != expected:
        _reject(identity_name, actual, expected)


def _scheduling_gate_names(pod_spec: object) -> list[object]:
    gates = _field(pod_spec, 'schedulingGates', 'scheduling_gates')
    if not isinstance(gates, (list, tuple)):
        return []
    return [_field(gate, 'name') for gate in gates]


def _affinity_contract_matches(
        pod_spec: object, expectation: KueuePodAdmissionExpectation) -> bool:
    affinity = _field(pod_spec, 'affinity')
    node_affinity = _field(affinity, 'nodeAffinity', 'node_affinity')
    required = _field(node_affinity,
                      'requiredDuringSchedulingIgnoredDuringExecution',
                      'required_during_scheduling_ignored_during_execution')
    terms = _field(required, 'nodeSelectorTerms', 'node_selector_terms')
    if not isinstance(terms, (list, tuple)) or not terms:
        return False
    for term in terms:
        expressions = _field(term, 'matchExpressions', 'match_expressions')
        if not isinstance(expressions, (list, tuple)):
            return False
        matches = [
            expression for expression in expressions
            if _field(expression, 'key') == expectation.accelerator_label_key
        ]
        if (len(matches) != 1 or _field(matches[0], 'operator') != 'In' or
                tuple(_field(matches[0], 'values') or
                      ()) != expectation.accelerator_label_values):
            return False
    return True


def classify_pod(
    pod: 'V1Pod',
    expectation: KueuePodAdmissionExpectation,
    *,
    expected_pod_name: str,
    expected_pod_uid: str,
) -> common.KueuePodAdmissionObservation:
    """Classify one already-read CoreV1 Pod without any external reads."""
    if not isinstance(expectation, KueuePodAdmissionExpectation):
        raise TypeError('expectation must be KueuePodAdmissionExpectation.')
    if not isinstance(expected_pod_name, str) or not expected_pod_name:
        raise ValueError('expected_pod_name must be nonempty.')
    if not isinstance(expected_pod_uid, str) or not expected_pod_uid:
        raise ValueError('expected_pod_uid must be nonempty.')

    metadata = _field(pod, 'metadata')
    pod_spec = _field(pod, 'spec')
    status = _field(pod, 'status')
    name = _field(metadata, 'name')
    namespace = _field(metadata, 'namespace')
    uid = _field(metadata, 'uid')
    deletion_timestamp = _field(metadata, 'deletionTimestamp',
                                'deletion_timestamp')
    phase = _field(status, 'phase')
    exact_identity = {
        'namespace': namespace,
        'name': name,
        'uid': uid,
        'deletion_timestamp': deletion_timestamp,
        'phase': phase,
    }
    expected_identity = {
        'namespace': expectation.namespace,
        'name': expected_pod_name,
        'uid': expected_pod_uid,
        'deletion_timestamp': None,
        'phase': 'Pending or Running',
    }
    if (namespace != expectation.namespace or name != expected_pod_name or
            uid != expected_pod_uid or deletion_timestamp is not None or
            phase not in ('Pending', 'Running')):
        _reject('Kueue lane Pod identity', exact_identity, expected_identity)

    labels = _mapping(_field(metadata, 'labels'))
    annotations = _mapping(_field(metadata, 'annotations'))
    _require_equal('SkyPilot cluster identity',
                   labels.get(provision_constants.TAG_SKYPILOT_CLUSTER_NAME),
                   expectation.cluster_name_on_cloud)

    expected_annotations = identity_annotations(expectation.identity)
    actual_annotations = {
        key: annotations.get(key)
        for key in constants.RESERVED_FILL_IDENTITY_ANNOTATION_KEYS
    }
    unknown_identity_annotations = sorted(
        key for key in annotations
        if key.startswith(constants.RESERVED_FILL_IDENTITY_ANNOTATION_PREFIX)
        and key not in constants.RESERVED_FILL_IDENTITY_ANNOTATION_KEYS)
    if (actual_annotations != expected_annotations or
            unknown_identity_annotations):
        _reject(
            'reserved-fill Pod annotations', {
                'identity': actual_annotations,
                'unknown': unknown_identity_annotations,
            }, {
                'identity': expected_annotations,
                'unknown': [],
            })

    priority_class_name = _field(pod_spec, 'priorityClassName',
                                 'priority_class_name')
    _require_equal('priority class', priority_class_name,
                   expectation.priority_class_name)
    if expectation.priority_class_name is not None:
        _require_equal('numeric priority', _field(pod_spec, 'priority'),
                       expectation.priority_value)
        _require_equal(
            'preemption policy',
            _field(pod_spec, 'preemptionPolicy', 'preemption_policy'),
            expectation.preemption_policy)
    _require_equal(
        'service account',
        _field(pod_spec, 'serviceAccountName', 'service_account_name'),
        expectation.service_account_name)
    _require_equal('scheduler',
                   _field(pod_spec, 'schedulerName', 'scheduler_name'),
                   expectation.scheduler_name)

    try:
        accelerator_contract = pod_spec_lib.enforce_projected_accelerator_contract(
            pod_spec,
            expectation.accelerator_resource_key,
            expectation.accelerator_count,
            rewrite=False)
    except pod_spec_lib.ProjectedAcceleratorContractError as error:
        _reject(
            'accelerator scheduling contract', str(error), {
                'label_key': expectation.accelerator_label_key,
                'label_values': list(expectation.accelerator_label_values),
                'resource_key': expectation.accelerator_resource_key,
                'accelerator_count': expectation.accelerator_count,
            })
    if (not accelerator_contract.matches or
            not _affinity_contract_matches(pod_spec, expectation)):
        _reject(
            'accelerator scheduling contract', {
                'ray_node_container_count':
                    accelerator_contract.ray_node_container_count,
                'resource_contract_matches':
                    accelerator_contract.ray_node_resource_contract_matches,
                'unexpected_accelerator_resources':
                    accelerator_contract.unexpected_accelerator_resources,
                'dynamic_resource_claims':
                    accelerator_contract.dynamic_resource_claims,
                'affinity_contract_matches': _affinity_contract_matches(
                    pod_spec, expectation),
            }, {
                'label_key': expectation.accelerator_label_key,
                'label_values': list(expectation.accelerator_label_values),
                'resource_key': expectation.accelerator_resource_key,
                'accelerator_count': expectation.accelerator_count,
            })

    managed_value = labels.get(constants.KUEUE_MANAGED_KEY)
    queue = labels.get(constants.KUEUE_QUEUE_LABEL)
    workload_priority = labels.get(
        constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL)
    pod_group = labels.get(constants.KUEUE_POD_GROUP_LABEL)
    pod_group_total = annotations.get(
        constants.KUEUE_POD_GROUP_TOTAL_COUNT_ANNOTATION)
    retriable = annotations.get(constants.KUEUE_RETRIABLE_IN_GROUP_ANNOTATION)
    competing_group = annotations.get(constants.KUEUE_POD_GROUP_LABEL)
    role_hash = annotations.get(constants.KUEUE_ROLE_HASH_ANNOTATION)
    role_hash_valid = bool(
        isinstance(role_hash, str) and re.fullmatch(r'[0-9a-f]{8}', role_hash))
    _require_equal('Kueue managed label', managed_value,
                   constants.KUEUE_MANAGED_VALUE)
    _require_equal('Kueue queue label', queue, expectation.local_queue_name)
    _require_equal('Kueue workload priority label', workload_priority,
                   expectation.workload_priority_class_name)
    _require_equal('Kueue Pod group label', pod_group,
                   expectation.cluster_name_on_cloud)
    _require_equal('Kueue Pod group count annotation', pod_group_total,
                   str(expectation.pod_group_total_count))
    _require_equal('Kueue retriable annotation', retriable, 'false')
    _require_equal('competing Kueue Pod group annotation', competing_group,
                   None)
    if not role_hash_valid:
        _reject('Kueue role hash annotation', role_hash,
                '8 lowercase hexadecimal characters')

    finalizers = _field(metadata, 'finalizers')
    has_managed_finalizer = bool(
        isinstance(finalizers, (list, tuple)) and
        constants.KUEUE_MANAGED_FINALIZER in finalizers)
    if not has_managed_finalizer:
        _reject('Kueue managed finalizer', finalizers,
                constants.KUEUE_MANAGED_FINALIZER)

    gate_names = _scheduling_gate_names(pod_spec)
    allowed_gates = {
        constants.KUEUE_ADMISSION_SCHEDULING_GATE,
        constants.KUEUE_TOPOLOGY_SCHEDULING_GATE,
    }
    gates_are_exact = (all(
        isinstance(name, str) and name in allowed_gates for name in gate_names)
                       and len(set(gate_names)) == len(gate_names))
    if not gates_are_exact:
        _reject('Kueue scheduling gates', gate_names,
                f'unique members of {sorted(allowed_gates)!r}')
    has_admission_gate = constants.KUEUE_ADMISSION_SCHEDULING_GATE in gate_names

    allowed_labels = {
        constants.KUEUE_MANAGED_KEY,
        constants.KUEUE_QUEUE_LABEL,
        constants.KUEUE_POD_GROUP_LABEL,
        constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL,
        constants.KUEUE_CLUSTER_QUEUE_LABEL,
        constants.KUEUE_LOCAL_QUEUE_LABEL,
        constants.KUEUE_PODSET_LABEL,
    }
    allowed_annotations = {
        constants.KUEUE_POD_GROUP_TOTAL_COUNT_ANNOTATION,
        constants.KUEUE_RETRIABLE_IN_GROUP_ANNOTATION,
        constants.KUEUE_ROLE_HASH_ANNOTATION,
        constants.KUEUE_WORKLOAD_ANNOTATION,
        constants.KUEUE_PODSET_UNCONSTRAINED_TOPOLOGY_ANNOTATION,
    }
    unexpected_kueue_labels = sorted(
        key for key in labels
        if key.startswith(constants.KUEUE_METADATA_PREFIX) and
        key not in allowed_labels)
    unexpected_kueue_annotations = sorted(
        key for key in annotations
        if key.startswith(constants.KUEUE_METADATA_PREFIX) and
        key not in allowed_annotations)
    if unexpected_kueue_labels or unexpected_kueue_annotations:
        _reject(
            'unexpected Kueue metadata', {
                'labels': unexpected_kueue_labels,
                'annotations': unexpected_kueue_annotations,
            }, {
                'labels': [],
                'annotations': [],
            })

    podset = labels.get(constants.KUEUE_PODSET_LABEL)
    workload = annotations.get(constants.KUEUE_WORKLOAD_ANNOTATION)
    local_queue = labels.get(constants.KUEUE_LOCAL_QUEUE_LABEL)
    cluster_queue = labels.get(constants.KUEUE_CLUSTER_QUEUE_LABEL)
    topology = annotations.get(
        constants.KUEUE_PODSET_UNCONSTRAINED_TOPOLOGY_ANNOTATION)
    admitted_values = (podset, workload, local_queue, cluster_queue)
    if has_admission_gate:
        if any(value is not None
               for value in admitted_values) or topology is not None:
            _reject(
                'gated Kueue admission outputs', {
                    'podset': podset,
                    'workload': workload,
                    'local_queue': local_queue,
                    'cluster_queue': cluster_queue,
                    'unconstrained_topology': topology,
                }, 'all absent while admission-gated')
        _require_equal('pre-admission node binding',
                       _field(pod_spec, 'nodeName', 'node_name'), None)
        state = common.KueuePodAdmissionState.POD_WAITING
    else:
        admitted_exact = bool(
            podset == role_hash and
            workload in (None, expectation.cluster_name_on_cloud) and
            local_queue == expectation.local_queue_name and
            cluster_queue == expectation.cluster_queue_name and
            topology in (None, 'true'))
        if not admitted_exact:
            _reject(
                'admitted Kueue outputs', {
                    'podset': podset,
                    'workload': workload,
                    'local_queue': local_queue,
                    'cluster_queue': cluster_queue,
                    'unconstrained_topology': topology,
                }, {
                    'podset': role_hash,
                    'workload': f'null or {expectation.cluster_name_on_cloud!r}',
                    'local_queue': expectation.local_queue_name,
                    'cluster_queue': expectation.cluster_queue_name,
                    'unconstrained_topology': 'null or "true"',
                })
        state = common.KueuePodAdmissionState.POLICY_ADMITTED

    assert isinstance(namespace, str)
    assert isinstance(name, str)
    assert isinstance(uid, str)
    assert isinstance(phase, str)
    assert isinstance(role_hash, str)
    receipt = common.KueuePodAdmissionReceipt(
        state=state,
        namespace=namespace,
        pod_name=name,
        pod_uid=uid,
        pod_phase=phase,
        scheduling_gates=tuple(sorted(str(gate) for gate in gate_names)),
        cluster_name_on_cloud=expectation.cluster_name_on_cloud,
        kueue_managed_finalizer=constants.KUEUE_MANAGED_FINALIZER,
        local_queue_name=expectation.local_queue_name,
        cluster_queue_name=expectation.cluster_queue_name,
        admission_local_queue_name=local_queue,
        admission_cluster_queue_name=cluster_queue,
        workload_priority_class_name=(expectation.workload_priority_class_name),
        pod_group_name=expectation.cluster_name_on_cloud,
        pod_group_total_count=expectation.pod_group_total_count,
        role_hash=role_hash,
        podset=podset,
        workload_name=workload,
        unconstrained_topology=topology,
        priority_class_name=expectation.priority_class_name,
        priority_value=expectation.priority_value,
        preemption_policy=expectation.preemption_policy,
        scheduler_name=expectation.scheduler_name,
        service_account_name=expectation.service_account_name,
        accelerator=expectation.accelerator,
        accelerator_label_key=expectation.accelerator_label_key,
        accelerator_label_values=expectation.accelerator_label_values,
        accelerator_resource_key=expectation.accelerator_resource_key,
        accelerator_count=expectation.accelerator_count,
        identity=expectation.identity)
    return common.KueuePodAdmissionObservation(receipt=receipt)
