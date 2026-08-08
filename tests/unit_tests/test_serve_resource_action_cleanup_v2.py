"""Pure native-V2 Kubernetes cleanup-target construction tests."""
# pylint: disable=protected-access

import ast
import copy
import dataclasses
import inspect
import pathlib
import uuid

import pytest
import test_serve_resource_action_down_execution_config as down_fixtures

from sky.serve import resource_action_cleanup_v2 as cleanup_v2
from sky.serve import resource_action_progress as progress
from sky.serve import resource_actions as actions
from sky.server.requests import resource_actions as kernel_actions


def _cluster_row(
    target: actions.ProviderKubernetesCleanupTargetV1,
) -> cleanup_v2.ProviderKubernetesCleanupClusterRowObservationV2:
    return cleanup_v2.ProviderKubernetesCleanupClusterRowObservationV2(
        version=2,
        cluster_name=target.cluster_name,
        cluster_record_uuid=target.cluster_record_uuid,
        disposition=target.cluster_row_disposition,
        handle=target.handle,
        observed_at=target.observed_at)


def _completed_input(
) -> tuple[cleanup_v2.ProviderKubernetesCompletedCleanupRederivationInputV2,
           actions.ProviderKubernetesCleanupTargetV1]:
    basis = actions.CompletedLaunchBasisV1.from_value(
        down_fixtures.completed_basis_payload())
    expected = actions.ProviderKubernetesCleanupTargetV1.from_value(
        down_fixtures._cleanup_target())
    value = cleanup_v2.ProviderKubernetesCompletedCleanupRederivationInputV2(
        version=2,
        source='completed_launch',
        basis=basis,
        source_object_plans=tuple(item.plan for item in expected.objects),
        cluster_row=_cluster_row(expected))
    return value, expected


def _partial_input(
    case: actions.ProviderPartialLaunchCleanupLegalShapeV1,
    *,
    fixture_member: str = 'realistic',
) -> tuple[cleanup_v2.ProviderKubernetesPartialCleanupRederivationInputV2,
           actions.ProviderKubernetesCleanupTargetV1]:
    basis, expected, cursor, quiescence = (
        down_fixtures._partial_source_for_case(case,
                                               fixture_member=fixture_member))
    value = cleanup_v2.ProviderKubernetesPartialCleanupRederivationInputV2(
        version=2,
        source='partial_launch_cleanup',
        basis=basis,
        source_object_plans=tuple(item.plan for item in expected.objects),
        source_progress=progress.ProviderLifecycleProgressV1(
            version=1, cursor=cursor, worker_attestation=None),
        source_progress_revision=basis.launch_provider_progress_revision,
        source_quiescence=quiescence,
        cluster_row=_cluster_row(expected))
    return value, expected


def test_completed_cleanup_is_rederived_byte_exactly_from_source_evidence(
) -> None:
    value, expected = _completed_input()

    actual = cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2(value)

    assert actual.canonical_bytes == expected.canonical_bytes
    cleanup_v2.validate_provider_kubernetes_cleanup_target_binding_v2(
        value.basis, actual)


@pytest.mark.parametrize('case',
                         down_fixtures._PARTIAL_DOWN_CASES,
                         ids=lambda case: case.case_id)
def test_all_legal_partial_cleanup_shapes_are_rederived_byte_exactly(
    case: actions.ProviderPartialLaunchCleanupLegalShapeV1,) -> None:
    value, expected = _partial_input(case)

    actual = cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2(value)

    assert actual.canonical_bytes == expected.canonical_bytes
    cleanup_v2.validate_provider_kubernetes_cleanup_target_binding_v2(
        value.basis, actual)


def test_candidate_maximal_partial_cleanup_is_rederived_byte_exactly() -> None:
    case = next(case for case in down_fixtures._PARTIAL_DOWN_CASES
                if case.case_id == 'endpoint_resolved_exact_handle')
    value, expected = _partial_input(case, fixture_member='candidate_maximal')

    actual = cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2(value)

    assert actual.canonical_bytes == expected.canonical_bytes


def test_cleanup_rederivation_inputs_are_closed_canonical_composites() -> None:
    completed, _ = _completed_input()
    parsed_completed = type(completed).from_value(completed.canonical_value())
    assert parsed_completed.canonical_bytes == completed.canonical_bytes
    assert type(parsed_completed.cluster_row).from_value(
        parsed_completed.cluster_row.canonical_value()).canonical_bytes == (
            parsed_completed.cluster_row.canonical_bytes)

    case = next(case for case in down_fixtures._PARTIAL_DOWN_CASES
                if case.case_id == 'endpoint_resolved_exact_handle')
    partial, _ = _partial_input(case, fixture_member='candidate_maximal')
    # This transient composite deliberately exceeds the generic wire-object
    # ceiling while each durable child remains independently bounded.
    assert len(partial.canonical_bytes) > 65_536
    parsed_partial = type(partial).from_value(partial.canonical_value())
    assert parsed_partial.canonical_bytes == partial.canonical_bytes


def test_completed_rederivation_rejects_source_plan_evidence_drift() -> None:
    value, _ = _completed_input()
    raw = value.source_object_plans[0].canonical_value()
    raw['requested_semantic']['metadata']['labels']['drift'] = 'changed'
    raw['requested_semantic_sha256'] = actions.canonical_sha256(
        raw['requested_semantic'])
    crossed_plan = actions.ProviderKubernetesObjectPlanV1.from_value(raw)
    crossed = cleanup_v2.ProviderKubernetesCompletedCleanupRederivationInputV2(
        version=2,
        source='completed_launch',
        basis=value.basis,
        source_object_plans=(crossed_plan,) + value.source_object_plans[1:],
        cluster_row=value.cluster_row)

    with pytest.raises(ValueError, match='differs from its immutable'):
        cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2(crossed)


def test_partial_rederivation_rejects_revision_and_cluster_row_handle_drift(
) -> None:
    case = next(case for case in down_fixtures._PARTIAL_DOWN_CASES
                if case.case_id == 'endpoint_resolved_exact_handle')
    value, _ = _partial_input(case)
    with pytest.raises(ValueError, match='progress bytes or revision'):
        cleanup_v2.ProviderKubernetesPartialCleanupRederivationInputV2(
            version=2,
            source='partial_launch_cleanup',
            basis=value.basis,
            source_object_plans=value.source_object_plans,
            source_progress=value.source_progress,
            source_progress_revision=value.source_progress_revision + 1,
            source_quiescence=value.source_quiescence,
            cluster_row=value.cluster_row)

    _, other_target = _partial_input(case, fixture_member='candidate_maximal')
    crossed_row = cleanup_v2.ProviderKubernetesCleanupClusterRowObservationV2(
        version=2,
        cluster_name=value.cluster_row.cluster_name,
        cluster_record_uuid=value.cluster_row.cluster_record_uuid,
        disposition=value.cluster_row.disposition,
        handle=other_target.handle,
        observed_at=value.cluster_row.observed_at)
    with pytest.raises(ValueError, match='differs from the launch cursor'):
        cleanup_v2.ProviderKubernetesPartialCleanupRederivationInputV2(
            version=2,
            source='partial_launch_cleanup',
            basis=value.basis,
            source_object_plans=value.source_object_plans,
            source_progress=value.source_progress,
            source_progress_revision=value.source_progress_revision,
            source_quiescence=value.source_quiescence,
            cluster_row=crossed_row)


@pytest.mark.parametrize('field,crossed', [('effect_sequence', 1),
                                           ('role', 'head_service')])
def test_partial_input_reparses_cursor_effect_sequence_and_role(
    field: str,
    crossed: object,
) -> None:
    case = next(case for case in down_fixtures._PARTIAL_DOWN_CASES
                if case.case_id == 'objects_partial_1_not_found')
    value, _ = _partial_input(case)
    cursor = value.source_progress.cursor
    assert type(cursor) is progress.ProviderLaunchProgressV1
    raw = cursor.canonical_value()
    raw['committed_effects'][0][field] = crossed
    forged_cursor = dataclasses.replace(
        cursor, value=actions.CanonicalJsonObject.from_value(raw))
    forged_progress = progress.ProviderLifecycleProgressV1(
        version=1, cursor=forged_cursor, worker_attestation=None)

    with pytest.raises(ValueError):
        cleanup_v2.ProviderKubernetesPartialCleanupRederivationInputV2(
            version=2,
            source='partial_launch_cleanup',
            basis=value.basis,
            source_object_plans=value.source_object_plans,
            source_progress=forged_progress,
            source_progress_revision=value.source_progress_revision,
            source_quiescence=value.source_quiescence,
            cluster_row=value.cluster_row)


def test_partial_input_binds_every_committed_effect_origin_to_source_action(
) -> None:
    case = next(case for case in down_fixtures._PARTIAL_DOWN_CASES
                if case.case_id == 'objects_partial_1_not_found')
    value, _ = _partial_input(case)
    cursor = value.source_progress.cursor
    assert type(cursor) is progress.ProviderLaunchProgressV1
    raw = cursor.canonical_value()
    other_action_id = uuid.UUID('aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee')
    other_request_id = kernel_actions.request_id_for_attempt(other_action_id, 1)
    for origin_field in ('intent_origin', 'evidence_commit_origin'):
        origin = raw['committed_effects'][0][origin_field]
        origin['request_id'] = other_request_id
        origin['worker_attestation']['request_id'] = other_request_id
        origin['worker_attestation_sha256'] = actions.canonical_sha256(
            origin['worker_attestation'])
    crossed_cursor = progress.ProviderLaunchProgressV1.from_value(raw)
    crossed_progress = progress.ProviderLifecycleProgressV1(
        version=1, cursor=crossed_cursor, worker_attestation=None)
    old_quiescence = value.source_quiescence
    crossed_quiescence = progress.ProviderLaunchSupersessionQuiescenceV1(
        launch_action_id=value.basis.launch_action_id,
        launch_attempt=value.basis.launch_attempt,
        request_id=old_quiescence.request_id,
        handler_terminal_result_sha256=(
            old_quiescence.handler_terminal_result_sha256),
        launch_provider_cursor_sha256=crossed_cursor.sha256,
        effects=tuple(
            progress.ProviderLaunchEffectQuiescenceV1.from_committed(effect)
            for effect in crossed_cursor.committed_effects),
        settled_at=old_quiescence.settled_at)
    basis_value = copy.deepcopy(value.basis.canonical_value())
    basis_value['launch_provider_cursor_sha256'] = crossed_cursor.sha256
    basis_value['launch_quiescence_sha256'] = crossed_quiescence.sha256
    crossed_basis = actions.PartialLaunchCleanupBasisV1.from_value(basis_value)

    with pytest.raises(ValueError, match='request ID differs'):
        cleanup_v2.ProviderKubernetesPartialCleanupRederivationInputV2(
            version=2,
            source='partial_launch_cleanup',
            basis=crossed_basis,
            source_object_plans=value.source_object_plans,
            source_progress=crossed_progress,
            source_progress_revision=value.source_progress_revision,
            source_quiescence=crossed_quiescence,
            cluster_row=value.cluster_row)


def test_rederivation_boundary_has_no_io_or_ambient_access_dependencies(
) -> None:
    source_path = pathlib.Path(inspect.getsourcefile(cleanup_v2) or '')
    tree = ast.parse(source_path.read_text(encoding='utf-8'))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.module is not None
            imports.add(node.module)
    assert imports == {
        '__future__',
        'dataclasses',
        'datetime',
        're',
        'typing',
        'unicodedata',
        'uuid',
        'sky.serve',
        'sky.server.requests',
    }
    forbidden_clock_calls = {'now', 'today', 'utcnow', 'time'}
    assert not any(
        isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and
        node.func.attr in forbidden_clock_calls for node in ast.walk(tree))
    signature = inspect.signature(
        cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2)
    assert tuple(signature.parameters) == ('value',)
    assert 'cleanup_target' not in signature.parameters

    value, expected = _completed_input()
    assert cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2(
        value).canonical_bytes == expected.canonical_bytes
