"""Pure native-V2 construction of Kubernetes cleanup targets.

The transaction-owning adapters provide exact retained launch evidence and a
same-UUID cluster-row observation.  This module only validates those typed
preimages and reconstructs the immutable V1 provider leaf consumed by the V2
action graph.  It deliberately performs no database, clock, configuration, or
Kubernetes access.
"""

from __future__ import annotations

import dataclasses
import datetime
import re
from typing import Any, ClassVar, TypeAlias
import unicodedata
import uuid

from sky.serve import resource_action_progress
from sky.serve import resource_actions
from sky.server.requests import resource_actions as kernel_actions

_MAX_SHORT_TEXT_BYTES = 253
_MAX_POSTGRES_BIGINT = 2**63 - 1
_UTC_TIMESTAMP_RE = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:'
                               r'[0-9]{2}\.[0-9]{6}Z$')


class _CompositeCanonicalContract:
    """Canonical helpers for bounded-child, potentially large composites.

    A candidate-maximal partial input is larger than the generic 65,536-byte
    wire-object ceiling because it intentionally combines separately bounded
    basis, progress, quiescence, and plan preimages.  These inputs are
    transient and never transported or persisted as one value, so they do not
    inherit the wire-bound ``CanonicalContract``.  Each child is still parsed
    by its closed bounded contract and the aggregate round-trips canonically.
    """

    def canonical_value(self) -> dict[str, object]:
        raise NotImplementedError

    @property
    def canonical_bytes(self) -> bytes:
        return resource_actions.canonical_json_bytes(self.canonical_value())


def _closed_composite_object(value: object, *, name: str,
                             keys: frozenset[str]) -> dict[str, Any]:
    if type(value) is not dict:
        raise TypeError(f'{name} must be an object.')
    if any(type(key) is not str for key in value):
        raise TypeError(f'{name} keys must be text.')
    if set(value) != keys:
        raise ValueError(f'{name} has unknown or missing fields.')
    return value


def _version_two(value: object, *, name: str) -> int:
    if type(value) is not int or value != 2:
        raise ValueError(f'{name} must be integer 2.')
    return value


def _positive_integer(value: object, *, name: str) -> int:
    if (type(value) is not int or value <= 0 or value > _MAX_POSTGRES_BIGINT):
        raise ValueError(f'{name} must be a positive signed-int64 integer.')
    return value


def _short_text(value: object, *, name: str) -> str:
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    try:
        size = len(value.encode('utf-8'))
    except UnicodeEncodeError as error:
        raise ValueError(f'{name} must be valid UTF-8 text.') from error
    if (size == 0 or size > _MAX_SHORT_TEXT_BYTES or '\x00' in value or
            unicodedata.normalize('NFC', value) != value):
        raise ValueError(
            f'{name} must be 1..{_MAX_SHORT_TEXT_BYTES} canonical UTF-8 bytes.')
    return value


def _timestamp(value: object, *, name: str) -> str:
    if type(value) is not str or _UTC_TIMESTAMP_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be canonical UTC timestamp text.')
    try:
        datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError as error:
        raise ValueError(f'{name} must be a valid UTC timestamp.') from error
    return value


def _uuid(value: object, *, name: str) -> uuid.UUID:
    if type(value) is uuid.UUID:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be a UUID or canonical UUID text.')
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ValueError(f'{name} must be a UUID.') from error
    if str(parsed) != value:
        raise ValueError(f'{name} must be lowercase hyphenated UUID text.')
    return parsed


def _cluster_row_disposition(
    value: object,
) -> resource_actions.ProviderKubernetesClusterRowDispositionV1:
    if type(value) is (
            resource_actions.ProviderKubernetesClusterRowDispositionV1):
        return value
    if type(value) is not str:
        raise TypeError('cleanup cluster-row disposition must be text.')
    try:
        return resource_actions.ProviderKubernetesClusterRowDispositionV1(value)
    except ValueError as error:
        raise ValueError(
            'cleanup cluster-row disposition is unsupported.') from error


def _source_object_plans(
    value: object,
) -> tuple[resource_actions.ProviderKubernetesObjectPlanV1, ...]:
    if (type(value) is not tuple or len(value) != len(
            resource_actions.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1) or any(
                type(item)
                is not resource_actions.ProviderKubernetesObjectPlanV1
                for item in value)):
        raise ValueError('cleanup rederivation requires exactly three typed '
                         'source object plans.')
    expected = tuple(
        (entry.plan_sequence, entry.role)
        for entry in resource_actions.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1)
    actual = tuple((item.sequence, item.role) for item in value)
    if actual != expected:
        raise ValueError('cleanup rederivation source plans have invalid '
                         'sequence or role order.')
    return value


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesCleanupClusterRowObservationV2(
        _CompositeCanonicalContract):
    """Preparation-frozen exact same-UUID cluster-row observation."""

    version: int
    cluster_name: str
    cluster_record_uuid: uuid.UUID
    disposition: resource_actions.ProviderKubernetesClusterRowDispositionV1
    handle: resource_actions.ProviderKubernetesHandleV1 | None
    observed_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'cluster_name', 'cluster_record_uuid', 'disposition',
        'handle', 'observed_at'
    })

    def __post_init__(self) -> None:
        _version_two(self.version,
                     name='cleanup cluster-row observation version')
        object.__setattr__(
            self, 'cluster_name',
            _short_text(self.cluster_name,
                        name='cleanup cluster-row observation cluster_name'))
        if type(self.cluster_record_uuid) is not uuid.UUID:
            raise TypeError('cleanup cluster-row observation UUID has an '
                            'invalid type.')
        disposition = _cluster_row_disposition(self.disposition)
        object.__setattr__(self, 'disposition', disposition)
        if (self.handle is not None and type(self.handle)
                is not resource_actions.ProviderKubernetesHandleV1):
            raise TypeError('cleanup cluster-row observation handle has an '
                            'invalid type.')
        if ((disposition is resource_actions.
             ProviderKubernetesClusterRowDispositionV1.EXACT_HANDLE)
                != (self.handle is not None)):
            raise ValueError('exact_handle requires a handle; not_found '
                             'requires null.')
        if self.handle is not None and (
                self.handle.cluster_name != self.cluster_name or
                self.handle.cluster_record_uuid != self.cluster_record_uuid):
            raise ValueError('cleanup cluster-row handle differs from its '
                             'same-UUID row identity.')
        object.__setattr__(
            self, 'observed_at',
            _timestamp(self.observed_at,
                       name='cleanup cluster-row observation observed_at'))

    @classmethod
    def from_value(
        cls,
        value: object,
    ) -> ProviderKubernetesCleanupClusterRowObservationV2:
        raw = _closed_composite_object(
            value, name='cleanup cluster-row observation V2', keys=cls._KEYS)
        return cls(
            version=raw['version'],
            cluster_name=raw['cluster_name'],
            cluster_record_uuid=_uuid(
                raw['cluster_record_uuid'],
                name='cleanup cluster-row observation cluster_record_uuid'),
            disposition=raw['disposition'],
            handle=(None if raw['handle'] is None else
                    resource_actions.ProviderKubernetesHandleV1.from_value(
                        raw['handle'])),
            observed_at=raw['observed_at'])

    def canonical_value(self) -> dict[str, object]:
        return {
            'version': 2,
            'cluster_name': self.cluster_name,
            'cluster_record_uuid': str(self.cluster_record_uuid),
            'disposition': self.disposition.value,
            'handle':
                (None if self.handle is None else self.handle.canonical_value()
                ),
            'observed_at': self.observed_at,
        }


def _validate_cluster_row_target_binding(
    cluster_row: ProviderKubernetesCleanupClusterRowObservationV2,
    basis: resource_actions.PriorLaunchBasisV1,
) -> None:
    target = basis.launch_requested_target
    if (cluster_row.cluster_name != target.sky_cluster_name or
            cluster_row.cluster_record_uuid != target.sky_cluster_record_uuid):
        raise ValueError('cleanup cluster-row observation differs from the '
                         'prior launch target.')
    handle = cluster_row.handle
    if handle is not None:
        handle.validate_requested_target(target)
        handle.validate_workspace_identity(basis.launch_workspace_identity)
        if handle.launched_resources_sha256 != basis.launch_resources.sha256:
            raise ValueError('cleanup cluster-row handle resources differ '
                             'from the prior launch basis.')


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesCompletedCleanupRederivationInputV2(
        _CompositeCanonicalContract):
    """Closed preimages for cleanup after one completed launch."""

    version: int
    source: str
    basis: resource_actions.CompletedLaunchBasisV1
    source_object_plans: tuple[resource_actions.ProviderKubernetesObjectPlanV1,
                               ...]
    cluster_row: ProviderKubernetesCleanupClusterRowObservationV2

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'source', 'basis', 'source_object_plans', 'cluster_row'})

    def __post_init__(self) -> None:
        _version_two(self.version,
                     name='completed cleanup rederivation input version')
        if type(self.source) is not str:
            raise TypeError('completed cleanup rederivation source must be '
                            'text.')
        if self.source != 'completed_launch':
            raise ValueError('completed cleanup rederivation source is '
                             'unsupported.')
        if type(self.basis) is not resource_actions.CompletedLaunchBasisV1:
            raise TypeError('completed cleanup rederivation basis has an '
                            'invalid type.')
        object.__setattr__(self, 'source_object_plans',
                           _source_object_plans(self.source_object_plans))
        if type(self.cluster_row
               ) is not ProviderKubernetesCleanupClusterRowObservationV2:
            raise TypeError('completed cleanup rederivation cluster row has '
                            'an invalid type.')
        _validate_cluster_row_target_binding(self.cluster_row, self.basis)
        if (self.cluster_row.disposition is not resource_actions.
                ProviderKubernetesClusterRowDispositionV1.EXACT_HANDLE or
                self.cluster_row.handle is None or
                self.cluster_row.handle.canonical_bytes
                != self.basis.launch_handle.canonical_bytes):
            raise ValueError('completed cleanup rederivation requires the '
                             'byte-equal prior launch handle.')

    @classmethod
    def from_value(
        cls,
        value: object,
    ) -> ProviderKubernetesCompletedCleanupRederivationInputV2:
        raw = _closed_composite_object(
            value,
            name='completed cleanup rederivation input V2',
            keys=cls._KEYS)
        raw_plans = raw['source_object_plans']
        if type(raw_plans) is not list or len(raw_plans) != 3:
            raise ValueError('completed cleanup rederivation requires exactly '
                             'three source plans.')
        return cls(
            version=raw['version'],
            source=raw['source'],
            basis=resource_actions.CompletedLaunchBasisV1.from_value(
                raw['basis']),
            source_object_plans=tuple(
                resource_actions.ProviderKubernetesObjectPlanV1.from_value(item)
                for item in raw_plans),
            cluster_row=ProviderKubernetesCleanupClusterRowObservationV2.
            from_value(raw['cluster_row']))

    def canonical_value(self) -> dict[str, object]:
        return {
            'version': 2,
            'source': 'completed_launch',
            'basis': self.basis.canonical_value(),
            'source_object_plans': [
                item.canonical_value() for item in self.source_object_plans
            ],
            'cluster_row': self.cluster_row.canonical_value(),
        }


def _validate_quiescence_effects(
    cursor: resource_action_progress.ProviderLaunchProgressV1,
    quiescence: resource_action_progress.ProviderLaunchSupersessionQuiescenceV1,
    *,
    launch_action_id: uuid.UUID,
    launch_attempt: int,
) -> None:
    committed = tuple(
        resource_action_progress.ProviderLaunchEffectQuiescenceV1.
        from_committed(effect) for effect in cursor.committed_effects)
    if (len(quiescence.effects) < len(committed) or
            any(expected.canonical_bytes != actual.canonical_bytes
                for expected, actual in zip(committed, quiescence.effects))):
        raise ValueError('partial cleanup quiescence differs from the cursor '
                         'committed-effect prefix.')
    if not cursor.is_intent:
        if len(quiescence.effects) != len(committed):
            raise ValueError('non-intent partial cleanup cursor has an extra '
                             'quiescence effect.')
        return
    if len(quiescence.effects) != len(committed) + 1:
        raise ValueError('intent partial cleanup cursor requires one exact '
                         'no-effect quiescence entry.')
    if cursor.intent_origin is None:
        raise ValueError('intent partial cleanup cursor has no origin.')
    sequence = cursor.current_intent_sequence
    if sequence is None:
        raise ValueError('intent partial cleanup cursor has no sequence.')
    no_effect = quiescence.effects[-1].canonical_value()
    if (no_effect['effect_sequence'] != sequence or no_effect['intent_origin']
            != cursor.intent_origin.canonical_value() or
            no_effect['resolution_origin']
            != cursor.intent_origin.canonical_value()):
        raise ValueError('partial cleanup no-effect quiescence differs from '
                         'the exact current intent.')
    cursor.intent_origin.validate_action(launch_action_id)
    if cursor.intent_origin.launch_attempt != launch_attempt:
        raise ValueError('partial cleanup current intent belongs to another '
                         'launch attempt.')


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesPartialCleanupRederivationInputV2(
        _CompositeCanonicalContract):
    """Closed retained API006 preimages for partial-launch cleanup."""

    version: int
    source: str
    basis: resource_actions.PartialLaunchCleanupBasisV1
    source_object_plans: tuple[resource_actions.ProviderKubernetesObjectPlanV1,
                               ...]
    source_progress: resource_action_progress.ProviderLifecycleProgressV1
    source_progress_revision: int
    source_quiescence: resource_action_progress.ProviderLaunchSupersessionQuiescenceV1
    cluster_row: ProviderKubernetesCleanupClusterRowObservationV2

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'source', 'basis', 'source_object_plans', 'source_progress',
        'source_progress_revision', 'source_quiescence', 'cluster_row'
    })

    def __post_init__(self) -> None:
        _version_two(self.version,
                     name='partial cleanup rederivation input version')
        if type(self.source) is not str:
            raise TypeError('partial cleanup rederivation source must be '
                            'text.')
        if self.source != 'partial_launch_cleanup':
            raise ValueError('partial cleanup rederivation source is '
                             'unsupported.')
        if type(self.basis) is not resource_actions.PartialLaunchCleanupBasisV1:
            raise TypeError('partial cleanup rederivation basis has an '
                            'invalid type.')
        object.__setattr__(self, 'source_object_plans',
                           _source_object_plans(self.source_object_plans))
        if type(self.source_progress
               ) is not resource_action_progress.ProviderLifecycleProgressV1:
            raise TypeError('partial cleanup source progress has an invalid '
                            'type.')
        # Reparse to reject a directly constructed cursor that skipped its
        # closed literal-phase validation.
        progress = resource_action_progress.ProviderLifecycleProgressV1.from_value(
            self.source_progress.canonical_value())
        object.__setattr__(self, 'source_progress', progress)
        revision = _positive_integer(
            self.source_progress_revision,
            name='partial cleanup source progress revision')
        object.__setattr__(self, 'source_progress_revision', revision)
        if type(self.source_quiescence) is not (
                resource_action_progress.ProviderLaunchSupersessionQuiescenceV1
        ):
            raise TypeError('partial cleanup source quiescence has an invalid '
                            'type.')
        quiescence = resource_action_progress.ProviderLaunchSupersessionQuiescenceV1.from_value(
            self.source_quiescence.canonical_value())
        object.__setattr__(self, 'source_quiescence', quiescence)
        if type(self.cluster_row
               ) is not ProviderKubernetesCleanupClusterRowObservationV2:
            raise TypeError('partial cleanup rederivation cluster row has an '
                            'invalid type.')
        _validate_cluster_row_target_binding(self.cluster_row, self.basis)
        self._validate_retained_source()

    def _validate_retained_source(self) -> None:
        basis = self.basis
        progress = self.source_progress
        if type(progress.cursor
               ) is not resource_action_progress.ProviderLaunchProgressV1:
            raise ValueError('partial cleanup source progress must be launch '
                             'progress.')
        cursor = progress.cursor
        if cursor.phase is resource_action_progress.LaunchProgressPhaseV1.SUCCEEDED:
            raise ValueError('successful launch progress requires a completed '
                             'cleanup basis.')
        if (cursor.sha256 != basis.launch_provider_cursor_sha256 or
                self.source_progress_revision
                != basis.launch_provider_progress_revision):
            raise ValueError('partial cleanup progress bytes or revision '
                             'differ from the retained basis.')
        quiescence = self.source_quiescence
        expected_request_id = uuid.UUID(
            kernel_actions.request_id_for_attempt(basis.launch_action_id,
                                                  basis.launch_attempt))
        if (quiescence.sha256 != basis.launch_quiescence_sha256 or
                quiescence.launch_action_id != basis.launch_action_id or
                quiescence.launch_attempt != basis.launch_attempt or
                quiescence.request_id != expected_request_id or
                quiescence.launch_provider_cursor_sha256 != cursor.sha256):
            raise ValueError('partial cleanup quiescence differs from the '
                             'retained launch source.')
        _validate_quiescence_effects(cursor,
                                     quiescence,
                                     launch_action_id=basis.launch_action_id,
                                     launch_attempt=basis.launch_attempt)
        self._validate_committed_effect_bindings(cursor)
        source_target = (cursor.known_objects if cursor.known_objects
                         is not None else cursor.resolved_target)
        if (source_target is None or source_target.requested_target_sha256
                != basis.launch_requested_target.sha256):
            raise ValueError('partial cleanup cursor target differs from the '
                             'retained launch target.')
        row_handle = self.cluster_row.handle
        if row_handle is not None and (cursor.handle is None or
                                       row_handle.canonical_bytes
                                       != cursor.handle.canonical_bytes):
            raise ValueError('partial cleanup exact cluster-row handle differs '
                             'from the launch cursor handle.')

    def _validate_committed_effect_bindings(
        self,
        cursor: resource_action_progress.ProviderLaunchProgressV1,
    ) -> None:
        # The closed cursor reparse above proves the literal effect
        # kind/phase, contiguous prefix, origin ordering, and object-at-commit
        # bindings.  This outer-source check additionally binds every origin
        # to the retained action and every object effect to its immutable plan.
        for sequence, effect in enumerate(cursor.committed_effects):
            expected_role = (self.source_object_plans[sequence].role
                             if sequence < 3 else None)
            if (effect.effect_sequence != sequence or
                    effect.role is not expected_role):
                raise ValueError('partial cleanup committed effect sequence '
                                 'or role differs from its source plan.')
            effect.validate_action(self.basis.launch_action_id)
            if sequence < 3:
                effect_value = effect.canonical_value()
                plan = self.source_object_plans[sequence]
                if (effect_value['request_body_sha256']
                        != plan.request_body_sha256 or
                        effect_value['requested_semantic_sha256']
                        != plan.requested_semantic_sha256):
                    raise ValueError('partial cleanup committed effect differs '
                                     'from its immutable source plan.')

    @classmethod
    def from_value(
        cls,
        value: object,
    ) -> ProviderKubernetesPartialCleanupRederivationInputV2:
        raw = _closed_composite_object(
            value, name='partial cleanup rederivation input V2', keys=cls._KEYS)
        raw_plans = raw['source_object_plans']
        if type(raw_plans) is not list or len(raw_plans) != 3:
            raise ValueError('partial cleanup rederivation requires exactly '
                             'three source plans.')
        return cls(
            version=raw['version'],
            source=raw['source'],
            basis=resource_actions.PartialLaunchCleanupBasisV1.from_value(
                raw['basis']),
            source_object_plans=tuple(
                resource_actions.ProviderKubernetesObjectPlanV1.from_value(item)
                for item in raw_plans),
            source_progress=(
                resource_action_progress.ProviderLifecycleProgressV1.from_value(
                    raw['source_progress'])),
            source_progress_revision=raw['source_progress_revision'],
            source_quiescence=(
                resource_action_progress.ProviderLaunchSupersessionQuiescenceV1.
                from_value(raw['source_quiescence'])),
            cluster_row=ProviderKubernetesCleanupClusterRowObservationV2.
            from_value(raw['cluster_row']))

    def canonical_value(self) -> dict[str, object]:
        return {
            'version': 2,
            'source': 'partial_launch_cleanup',
            'basis': self.basis.canonical_value(),
            'source_object_plans': [
                item.canonical_value() for item in self.source_object_plans
            ],
            'source_progress': self.source_progress.canonical_value(),
            'source_progress_revision': self.source_progress_revision,
            'source_quiescence': self.source_quiescence.canonical_value(),
            'cluster_row': self.cluster_row.canonical_value(),
        }


ProviderKubernetesCleanupRederivationInputV2: TypeAlias = (
    ProviderKubernetesCompletedCleanupRederivationInputV2 |
    ProviderKubernetesPartialCleanupRederivationInputV2)


def validate_provider_kubernetes_cleanup_target_binding_v2(
    basis: resource_actions.PriorLaunchBasisV1,
    cleanup_target: resource_actions.ProviderKubernetesCleanupTargetV1,
) -> None:
    """Bind one V2 cleanup-target leaf to its exact retained launch basis."""

    if type(basis) not in (resource_actions.CompletedLaunchBasisV1,
                           resource_actions.PartialLaunchCleanupBasisV1):
        raise TypeError('prior launch cleanup binding basis has an invalid '
                        'type.')
    if type(cleanup_target
           ) is not resource_actions.ProviderKubernetesCleanupTargetV1:
        raise TypeError('prior launch cleanup binding target has an invalid '
                        'type.')
    cleanup_target.validate_requested_target(basis.launch_requested_target)
    if cleanup_target.basis_kind is not basis.basis_kind:
        raise ValueError('cleanup target basis kind does not match its prior '
                         'launch basis.')
    if cleanup_target.sha256 != basis.launch_cleanup_target_sha256:
        raise ValueError('cleanup target hash does not match its prior launch '
                         'basis commitment.')
    if type(basis) is resource_actions.CompletedLaunchBasisV1:
        if (cleanup_target.handle is None or
                cleanup_target.handle.canonical_bytes
                != basis.launch_handle.canonical_bytes):
            raise ValueError('completed cleanup target handle is not '
                             'byte-equal to its prior launch handle.')
        resolved_objects = basis.launch_resolved_target.kubernetes_objects
        for resolved, cleanup_object in zip(resolved_objects,
                                            cleanup_target.objects):
            plan = cleanup_object.plan
            if (resolved.role is not cleanup_object.role or
                    resolved.kind is not plan.kind or
                    resolved.namespace != plan.namespace or
                    resolved.name != plan.name or
                    resolved.uid != cleanup_object.committed_uid or
                    resolved.observed_semantic_sha256
                    != plan.requested_semantic_sha256 or
                    tuple(item.canonical_bytes
                          for item in resolved.server_allocations) != tuple(
                              item.canonical_bytes for item in
                              cleanup_object.committed_server_allocations)):
                raise ValueError('completed cleanup target object evidence is '
                                 'not byte-equal to its prior launch source.')
        pod = cleanup_target.objects[2]
        if (basis.launch_resolved_target.provider_resource_id
                != f'pod/{pod.plan.name}' or
                basis.launch_resolved_target.workload_uid != pod.committed_uid):
            raise ValueError('completed cleanup target Pod identity does not '
                             'match its prior launch source.')


def _cleanup_object_from_resolved(
    plan: resource_actions.ProviderKubernetesObjectPlanV1,
    resolved: resource_actions.ProviderKubernetesResolvedObjectV1 | None,
) -> resource_actions.ProviderKubernetesCleanupObjectV1:
    if resolved is None:
        return resource_actions.ProviderKubernetesCleanupObjectV1(
            sequence=plan.sequence,
            role=plan.role,
            plan=plan,
            committed_uid=None,
            committed_server_allocations=())
    if (resolved.role is not plan.role or resolved.kind is not plan.kind or
            resolved.namespace != plan.namespace or
            resolved.name != plan.name or resolved.observed_semantic_sha256
            != plan.requested_semantic_sha256):
        raise ValueError('cleanup source object evidence differs from its '
                         'immutable source plan.')
    return resource_actions.ProviderKubernetesCleanupObjectV1(
        sequence=plan.sequence,
        role=plan.role,
        plan=plan,
        committed_uid=resolved.uid,
        committed_server_allocations=resolved.server_allocations)


def _completed_cleanup_objects(
    value: ProviderKubernetesCompletedCleanupRederivationInputV2,
) -> tuple[resource_actions.ProviderKubernetesCleanupObjectV1, ...]:
    resolved = value.basis.launch_resolved_target.kubernetes_objects
    return tuple(
        _cleanup_object_from_resolved(plan, source_object)
        for plan, source_object in zip(value.source_object_plans, resolved))


def _partial_cleanup_objects(
    value: ProviderKubernetesPartialCleanupRederivationInputV2,
) -> tuple[resource_actions.ProviderKubernetesCleanupObjectV1, ...]:
    cursor = value.source_progress.cursor
    if type(cursor) is not resource_action_progress.ProviderLaunchProgressV1:
        raise ValueError('partial cleanup source progress must be launch '
                         'progress.')
    if cursor.known_objects is not None:
        resolved = tuple(
            slot.object for slot in cursor.known_objects.kubernetes_objects)
    else:
        if cursor.resolved_target is None:
            raise ValueError('partial cleanup launch cursor has no resolved '
                             'target evidence.')
        resolved = cursor.resolved_target.kubernetes_objects
    objects = tuple(
        _cleanup_object_from_resolved(plan, source_object)
        for plan, source_object in zip(value.source_object_plans, resolved))
    pod_node_allocation = bool(objects[2].committed_server_allocations)
    _ = resource_actions.ProviderPartialLaunchCleanupLegalShapeV1(
        case_id='native_v2_rederived',
        launch_phase=cursor.phase.value,
        committed_object_count=sum(
            item.committed_uid is not None for item in objects),
        pod_node_allocation=pod_node_allocation,
        cluster_row_disposition=value.cluster_row.disposition)
    return objects


def rederive_provider_kubernetes_cleanup_target_v2(
    value: ProviderKubernetesCleanupRederivationInputV2,
) -> resource_actions.ProviderKubernetesCleanupTargetV1:
    """Construct the sole cleanup target from retained typed preimages."""

    if type(value) is ProviderKubernetesCompletedCleanupRederivationInputV2:
        objects = _completed_cleanup_objects(value)
    elif type(value) is ProviderKubernetesPartialCleanupRederivationInputV2:
        objects = _partial_cleanup_objects(value)
    else:
        raise TypeError('cleanup rederivation input has an invalid type.')
    basis = value.basis
    cluster_row = value.cluster_row
    cleanup_target = resource_actions.ProviderKubernetesCleanupTargetV1(
        version=1,
        basis_kind=basis.basis_kind,
        requested_target_sha256=basis.launch_requested_target.sha256,
        cluster_name=cluster_row.cluster_name,
        cluster_record_uuid=cluster_row.cluster_record_uuid,
        objects=objects,
        cluster_row_disposition=cluster_row.disposition,
        handle=cluster_row.handle,
        observed_at=cluster_row.observed_at)
    validate_provider_kubernetes_cleanup_target_binding_v2(
        basis, cleanup_target)
    return cleanup_target
