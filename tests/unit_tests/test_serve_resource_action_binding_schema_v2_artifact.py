"""Exact packaged-data boundary for the native-V2 binding schema."""

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from sky.serve import resource_action_renderer
from sky.serve import resource_action_renderer_v2

_REPO_ROOT = Path(__file__).resolve().parents[2]
_V1_PATH = (_REPO_ROOT / 'sky' / 'serve' / 'resource_action_artifacts' /
            'kubernetes_renderer_v1' / 'binding_schema.json')
_V2_PATH = (_REPO_ROOT / 'sky' / 'serve' / 'resource_action_artifacts' /
            'kubernetes_renderer_v2' / 'binding_schema.json')
_EXPECTED_V2_SIZE = 4520
_EXPECTED_V2_SHA256 = (
    'ac1095af06d9fee228f0671bfe86941eb02a8e10282ea8d138fbd89e6006fd5c')


def _load_canonical_lf(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw = path.read_bytes()
    value = json.loads(raw)
    assert type(value) is dict
    canonical_lf = json.dumps(value,
                              allow_nan=False,
                              ensure_ascii=False,
                              separators=(',', ':'),
                              sort_keys=True).encode('utf-8') + b'\n'
    assert raw == canonical_lf
    return raw, value


def test_binding_schema_v2_has_exact_fixed_bytes() -> None:
    raw, schema = _load_canonical_lf(_V2_PATH)

    assert len(raw) == _EXPECTED_V2_SIZE
    assert hashlib.sha256(raw).hexdigest() == _EXPECTED_V2_SHA256
    assert raw.endswith(b'\n')
    assert not raw.endswith(b'\n\n')
    assert schema['schema'] == (
        'skypilot.serve.prebooted-direct-pod.bindings.v2')
    assert schema['input_contract'] == 'validated_launch_spec_v2'


def test_binding_schema_v2_changes_only_the_two_version_fields() -> None:
    _, v1 = _load_canonical_lf(_V1_PATH)
    _, v2 = _load_canonical_lf(_V2_PATH)

    expected_v2 = dict(v1)
    expected_v2['schema'] = ('skypilot.serve.prebooted-direct-pod.bindings.v2')
    expected_v2['input_contract'] = 'validated_launch_spec_v2'
    assert v2 == expected_v2
    assert {key for key in v1 if v1[key] != v2[key]
           } == {'schema', 'input_contract'}

    v1_bindings = v1['bindings']
    v2_bindings = v2['bindings']
    assert len(v1_bindings) == len(v2_bindings) == 17
    assert v2_bindings == v1_bindings
    assert [row['name'] for row in v2_bindings
           ] == sorted(row['name'] for row in v2_bindings)
    for v1_row, v2_row in zip(v1_bindings, v2_bindings):
        assert v2_row['source_pointer'] == v1_row['source_pointer']
        assert v2_row['transform'] == v1_row['transform']
        assert v2_row['targets'] == v1_row['targets']


def test_binding_schema_versions_reject_each_other() -> None:
    _, v1 = _load_canonical_lf(_V1_PATH)
    _, v2 = _load_canonical_lf(_V2_PATH)

    resource_action_renderer.ProviderKubernetesBindingSchemaArtifactV1.from_value(
        v1)
    resource_action_renderer_v2.ProviderKubernetesBindingSchemaArtifactV2(v2)
    with pytest.raises(ValueError, match='bindings.v1 contract'):
        resource_action_renderer.ProviderKubernetesBindingSchemaArtifactV1.from_value(
            v2)
    with pytest.raises(ValueError, match='V2 binding schema is not exact'):
        resource_action_renderer_v2.ProviderKubernetesBindingSchemaArtifactV2(
            v1)
