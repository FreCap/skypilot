"""Exact packaged-data contract for the effect-free Kubernetes renderer."""

import hashlib
import json
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ARTIFACT_ROOT = (_REPO_ROOT / 'sky' / 'serve' / 'resource_action_artifacts' /
                  'kubernetes_renderer_v1')
_EXPECTED_ARTIFACTS = {
    'admitted_object_normalization.json':
        (3033,
         '3ab35d775ff1324587c1c10854d5de8572ce127a8541dc08d85349be06e8f850'),
    'binding_schema.json':
        (4520,
         '2c64a3ed8ee6ac3108fbf13d509ef348c73937d60473b5f697b24ee077611aef'),
    'config_access_inventory.json':
        (23710,
         '19901e8e0491a4e9f957f7ff2a1244fc1baff132c37015c9e8e726af2d538f13'),
    'node_fragment.json':
        (1632,
         '2000b68c74ccb6710e43b03963cf31f40c35ec879743977a3e3ba6ff3baa43db'),
    'outer_template.json':
        (972, '769039b9c25956833032fb670148797c3ba74cd5a12253faf1e99443a27444b8'
        ),
}


def _load_artifact(name: str) -> dict[str, Any]:
    raw = (_ARTIFACT_ROOT / name).read_bytes()
    value = json.loads(raw)
    canonical = json.dumps(value,
                           allow_nan=False,
                           ensure_ascii=False,
                           separators=(',', ':'),
                           sort_keys=True).encode('utf-8') + b'\n'
    assert raw == canonical
    expected_size, expected_sha256 = _EXPECTED_ARTIFACTS[name]
    assert len(raw) == expected_size
    assert hashlib.sha256(raw).hexdigest() == expected_sha256
    return value


def _pointer_get(value: Any, pointer: str) -> Any:
    assert pointer.startswith('/')
    current = value
    for raw_token in pointer[1:].split('/'):
        token = raw_token.replace('~1', '/').replace('~0', '~')
        if isinstance(current, list):
            current = current[int(token)]
        else:
            current = current[token]
    return current


def _collect_markers(value: Any, pointer: str = '') -> list[tuple[str, str]]:
    if type(value) is dict:
        if set(value) == {'$binding'}:
            return [(pointer, value['$binding'])]
        markers = []
        for key, child in value.items():
            escaped = key.replace('~', '~0').replace('/', '~1')
            markers.extend(_collect_markers(child, f'{pointer}/{escaped}'))
        return markers
    if type(value) is list:
        markers = []
        for index, child in enumerate(value):
            markers.extend(_collect_markers(child, f'{pointer}/{index}'))
        return markers
    return []


def test_renderer_artifact_set_is_exact_canonical_json_lf() -> None:
    assert {path.name for path in _ARTIFACT_ROOT.iterdir()
           } == set(_EXPECTED_ARTIFACTS)
    for name in sorted(_EXPECTED_ARTIFACTS):
        _load_artifact(name)


def test_binding_targets_are_the_complete_template_marker_set() -> None:
    binding_schema = _load_artifact('binding_schema.json')
    artifacts = {
        'outer_template': _load_artifact('outer_template.json'),
        'node_fragment': _load_artifact('node_fragment.json'),
    }
    actual_targets = []
    for binding in binding_schema['bindings']:
        for target in binding['targets']:
            expected_marker = {'$binding': binding['name']}
            assert _pointer_get(artifacts[target['artifact_role']],
                                target['pointer']) == expected_marker
            actual_targets.append(
                (target['artifact_role'], target['pointer'], binding['name']))
    discovered_targets = []
    for artifact_role, value in artifacts.items():
        discovered_targets.extend(
            (artifact_role, pointer, binding_name)
            for pointer, binding_name in _collect_markers(value))
    assert sorted(actual_targets) == sorted(discovered_targets)
    assert [entry['name'] for entry in binding_schema['bindings']
           ] == sorted(entry['name'] for entry in binding_schema['bindings'])
    assert len(binding_schema['bindings']) == 17


def test_config_inventory_has_exact_closed_cardinalities() -> None:
    inventory = _load_artifact('config_access_inventory.json')
    assert len(inventory['artifact_roles']) == 5
    assert len(inventory['entrypoints']) == 11
    assert len(inventory['call_graph']) == 11
    assert len(inventory['input_access']) == 51
    assert len(inventory['transient_flow']) == 7
    operations = inventory['provider_operations']
    assert operations['renderer'] == []
    assert operations['normalizer'] == []
    assert len(operations['object_session']) == 15
    assert len(operations['preflight_contracts']) == 2
    assert all(operation['api_group'] == ''
               for operation in operations['object_session'])
