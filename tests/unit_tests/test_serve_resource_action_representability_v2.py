"""Finite API006 provider representability inventory tests."""
# pylint: disable=protected-access

import ast
import copy
import hashlib
import json
import pathlib

import pytest
import test_serve_resource_action_v2_identity as v2_fixtures

from sky.serve import resource_action_provider_inventory_v2 as inventory_v2
from sky.serve import resource_action_representability as representability
from sky.serve import resource_actions as actions

_ROOT = pathlib.Path(__file__).parents[2]
_CASE_PATH = (_ROOT / 'sky/serve/resource_action_artifacts/'
              'provider_authority_v2/representability_case_inventory.json')
_EXPECTED_RAW_SIZE = 733
_EXPECTED_RAW_SHA256 = (
    'f2c6a375eb70dd06068b85ce2689a72f9918333849a9ca434a17f3c600817abe')
_EXPECTED_INDEX_CANONICAL_SHA256 = (
    'e997725e41137567c541a0faeaa33528153fd3a0174d533b99f4738aab1eb0a6')
_EXPECTED_COMBINED_CASE_SET_SHA256 = (
    'a38edd1044ea4e2407852cea51e5f1589fd01a4a6ab53a450a477a562f4d8006')
_EXPECTED_SHARDS = (
    (37_417,
     'fd377eb0050ff97a3dd12c1389420e1075af97a1aa83d3c17c0633bb957f980c'),
    (37_991,
     'fe0a12faacc2f3373c3b2854ff4a16f9610d3a6f135b43f445ae26b73fb93d2b'),
)


def _inventory(
) -> representability.ProviderResourceActionRepresentabilityCaseInventoryV2:
    raw = _CASE_PATH.read_bytes()
    assert raw.endswith(b'\n') and not raw.endswith(b'\n\n')
    value = json.loads(raw[:-1])
    assert actions.canonical_json_bytes(value) == raw[:-1]
    return inventory_v2.load_provider_resource_action_representability_inventory_v2(
        raw)


def test_case_inventory_has_exact_canonical_lf_preimage_and_budget() -> None:
    raw = _CASE_PATH.read_bytes()
    inventory = _inventory()

    assert len(raw) == _EXPECTED_RAW_SIZE
    assert hashlib.sha256(raw).hexdigest() == _EXPECTED_RAW_SHA256
    assert hashlib.sha256(
        raw[:-1]).hexdigest() == _EXPECTED_INDEX_CANONICAL_SHA256
    assert inventory.sha256 == _EXPECTED_COMBINED_CASE_SET_SHA256
    assert len(raw) <= 65_536
    index = (
        representability.
        ProviderResourceActionRepresentabilityCaseInventoryIndexV2.from_value(
            json.loads(raw)))
    assert index.contract == (
        'provider_resource_action_representability_case_inventory_index_v2')
    assert len(index.shards) == 2
    for ordinal, (descriptor, (expected_size, expected_sha256)) in enumerate(
            zip(index.shards, _EXPECTED_SHARDS)):
        assert descriptor.ordinal == ordinal
        assert set(descriptor.canonical_value()) == {
            'ordinal', 'first_case_sequence', 'last_case_sequence',
            'case_count', 'artifact'
        }
        assert set(descriptor.artifact.canonical_value()) == {
            'repo_path', 'byte_size', 'sha256'
        }
        shard = _ROOT / descriptor.artifact.repo_path
        contents = shard.read_bytes()
        assert len(contents) == expected_size
        assert hashlib.sha256(contents).hexdigest() == expected_sha256
        assert len(contents) == descriptor.artifact.byte_size
        assert expected_sha256 == descriptor.artifact.sha256
        assert len(contents) <= 65_536
        shard_value = json.loads(contents)
        assert set(shard_value) == {
            'version', 'contract', 'profile', 'ordinal', 'cases'
        }
        assert shard_value['contract'] == (
            'provider_resource_action_representability_case_inventory_shard_v2')
        assert shard_value['ordinal'] == ordinal
    assert len(inventory.cases) == 366
    assert inventory.canonical_bytes == (
        representability.
        PROVIDER_RESOURCE_ACTION_REPRESENTABILITY_CASE_INVENTORY_V2.
        canonical_bytes)


def test_case_inventory_is_fully_expanded_and_matches_literal_dispatch(
) -> None:
    inventory = _inventory()
    source = ast.parse((_ROOT / 'sky/serve/'
                        'resource_action_representability.py').read_text())
    assignment = next(
        node for node in source.body
        if isinstance(node, ast.AnnAssign) and isinstance(
            node.target, ast.Name) and node.target.id == '_CASE_PROJECTORS_V2')
    assert isinstance(assignment.value, ast.Dict)
    literal_dispatch_keys = tuple(
        ast.literal_eval(key) for key in assignment.value.keys)

    assert literal_dispatch_keys == tuple(
        case.case_id for case in inventory.cases)
    assert tuple(representability._CASE_PROJECTORS_V2) == literal_dispatch_keys
    assert tuple(case.sequence for case in inventory.cases) == tuple(
        range(len(inventory.cases)))
    assert len({case.case_id for case in inventory.cases
               }) == len(inventory.cases)
    assert not any(
        marker in case.case_id
        for case in inventory.cases
        for marker in ('regex', 'range', 'wildcard', 'all_enum', 'cartesian'))
    representability._validate_provider_resource_action_representability_dispatch_v2(
    )


def test_case_inventory_covers_closed_reasons_phases_and_partial_shapes(
) -> None:
    cases = _inventory().cases
    case_ids = {case.case_id for case in cases}
    launch_reason_prefix = (
        'launch.complete_preflight.preflight_response.not_representable.')
    down_reason_prefix = (
        'down.complete_preflight.preflight_response.not_representable.')

    assert {
        case_id.removeprefix(launch_reason_prefix)
        for case_id in case_ids
        if case_id.startswith(launch_reason_prefix)
    } == {
        reason.value
        for reason in actions.ProviderLaunchNotRepresentableReasonV1
    }
    assert {
        case_id.removeprefix(down_reason_prefix)
        for case_id in case_ids
        if case_id.startswith(down_reason_prefix)
    } == {
        reason.value for reason in actions.ProviderDownNotRepresentableReasonV1
    }
    assert {
        case_id.removeprefix('down.complete_preflight.cleanup_target.partial.')
        for case_id in case_ids
        if case_id.startswith('down.complete_preflight.cleanup_target.partial.')
    } == {
        shape.case_id for shape in
        actions.enumerate_provider_partial_launch_cleanup_legal_shapes_v1()
    }

    for role in ('head_ssh_service', 'head_service', 'head_pod'):
        assert f'launch.pre_io.progress.create_intent.{role}' in case_ids
    for phase in ('handle_intent', 'handle_committed', 'runtime_ready',
                  'job_intent', 'job_committed', 'job_running',
                  'endpoint_resolved', 'succeeded'):
        assert f'launch.pre_io.progress.{phase}' in case_ids
    for phase in ('target_resolved', 'delete_intent', 'delete_partial',
                  'absence_exact', 'handle_remove_intent', 'handle_removed',
                  'succeeded'):
        assert any(
            case_id.startswith(f'down.pre_io.progress.{phase}')
            for case_id in case_ids)
    for sequence in range(5):
        assert (f'launch.pre_io.no_effect.call_not_entered.effect_{sequence}'
                in case_ids)
        assert (f'launch.pre_io.quiescence.e_plus_n.effect_{sequence}'
                in case_ids)


def test_inventory_parser_rejects_open_or_crossed_case_sets() -> None:
    original = _inventory().canonical_value()

    unknown = copy.deepcopy(original)
    unknown['cases'][0]['selector'] = 'all enum values'
    with pytest.raises(ValueError, match='unknown or missing'):
        (representability.ProviderResourceActionRepresentabilityCaseInventoryV2.
         from_value(unknown))

    noncontiguous = copy.deepcopy(original)
    noncontiguous['cases'][1]['sequence'] = 9
    with pytest.raises(ValueError, match='contiguous'):
        (representability.ProviderResourceActionRepresentabilityCaseInventoryV2.
         from_value(noncontiguous))

    duplicate = copy.deepcopy(original)
    duplicate['cases'][1]['case_id'] = duplicate['cases'][0]['case_id']
    with pytest.raises(ValueError, match='not unique'):
        (representability.ProviderResourceActionRepresentabilityCaseInventoryV2.
         from_value(duplicate))

    placeholder = copy.deepcopy(original)
    placeholder['cases'][0]['case_id'] = 'launch.pre_io.progress.all_enum'
    with pytest.raises(ValueError, match='placeholder'):
        (representability.ProviderResourceActionRepresentabilityCaseInventoryV2.
         from_value(placeholder))


def test_structural_v2_with_v1_renderer_inventory_fails_closed() -> None:
    spec = actions.serve_replica_action_spec_from_value_v2(
        v2_fixtures._v2_launch_spec())
    capsule = spec.invocation.require_launch().execution_config.capsule
    assert capsule.config_projection.config_access_inventory.repo_path.endswith(
        'kubernetes_renderer_v1/config_access_inventory.json')

    with pytest.raises(representability.
                       ProviderResourceActionRepresentabilityUnavailableError,
                       match='native V2 config-access'):
        representability._validate_native_v2_config_reference(capsule)


def test_response_origin_cases_fail_closed_until_final_builders_land() -> None:
    # Bypass construction only to isolate the sealed dispatch gate.  No such
    # value can cross the public parser or authorize a live boundary.
    root = object.__new__(
        representability.ProviderResourceActionPreIoRepresentabilityInputV2)
    case_id = 'launch.pre_io.progress.create_intent.head_ssh_service'

    with pytest.raises(representability.
                       ProviderResourceActionRepresentabilityUnavailableError,
                       match='response-origin'):
        representability._project_provider_resource_action_representability_case_v2(
            root, representability.ProviderResourceActionRepresentabilityModeV2.
            CURRENT, case_id)


def test_final_golden_graph_is_not_fabricated_before_final_artifacts() -> None:
    artifact_root = _CASE_PATH.parent
    assert not (artifact_root / 'representability_goldens.json').exists()
    assert not (artifact_root / 'representability/realistic.json').exists()
    assert not (artifact_root /
                'representability/candidate_maximal.json').exists()
    manifest = (_ROOT / 'sky/setup_files/MANIFEST.in').read_text()
    assert ('recursive-include sky/serve/resource_action_artifacts/'
            'provider_authority_v2 *.json') in manifest
