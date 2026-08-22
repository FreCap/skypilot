"""Characterization tests for built-in Kubernetes template composition."""

import hashlib
import pathlib
from unittest import mock

import jinja2
from jinja2 import meta
import pytest
import yaml

import sky
from sky.utils import common_utils
from sky.utils import kubernetes_template_source
from sky.utils import yaml_utils

_TEMPLATE_DIR = pathlib.Path(sky.__file__).parent / 'templates'
_MONOLITH_PATH = _TEMPLATE_DIR / 'kubernetes-ray.yml.j2'
_OUTER_PATH = _TEMPLATE_DIR / 'kubernetes-ray-outer.yml.j2'
_NODE_CONFIG_PATH = (_TEMPLATE_DIR / 'kubernetes-ray-node-config.yml.j2')

_SOURCE_MARKER_LINE = ('{{ skypilot_kubernetes_node_config_fragment_v1 }}\n')

_MONOLITH_SIZE = 90617
_MONOLITH_SHA256 = (
    'f1f0b602b8ffe4948d9c655cadf216238ee9c26d71b788d73eed0f5da4078676')
_OUTER_SIZE = 16400
_OUTER_SHA256 = (
    'b49f33288ccaf0356b212b1863819e1126aa4d17c5501961dcfc66d87f118707')
_NODE_CONFIG_SIZE = 74267
_NODE_CONFIG_SHA256 = (
    '47b7c26047f9c2a2b8f756f618e6726b4c7508018354e6098554fdf612b5f4ba')
_BINDING_NAMES_SHA256 = (
    '4bcea22d9ef512686ad19920fc5f533c9db2f013abe5458c4773bf3617336b1a')

_MARKER_ERROR = ('Built-in Kubernetes template outer marker count mismatch.')
_OUTER_ERROR = ('Built-in Kubernetes template outer source identity mismatch.')
_NODE_CONFIG_ERROR = (
    'Built-in Kubernetes template node-config source identity mismatch.')
_RECOMPOSED_ERROR = (
    'Built-in Kubernetes template recomposed source identity mismatch.')
_INVALID_UTF8_ERROR = 'Built-in Kubernetes template source is not valid UTF-8.'


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _read_sources() -> tuple[str, str, str]:
    return (_MONOLITH_PATH.read_text(encoding='utf-8'),
            _OUTER_PATH.read_text(encoding='utf-8'),
            _NODE_CONFIG_PATH.read_text(encoding='utf-8'))


def _assert_composer_error(expected_message: str, outer_text: str,
                           node_config_text: str) -> None:
    with pytest.raises(ValueError) as exc_info:
        kubernetes_template_source.compose_builtin_kubernetes_template_source(
            outer_text, node_config_text)
    assert str(exc_info.value) == expected_message


def _same_length_drift(value: str) -> str:
    assert value
    replacement = '!' if value[0] != '!' else '#'
    assert len(replacement.encode('utf-8')) == len(value[0].encode('utf-8'))
    return replacement + value[1:]


def _install_synthetic_contract(monkeypatch: pytest.MonkeyPatch,
                                outer_text: str, node_config_text: str) -> str:
    # Intentional white-box coverage: these private identities are rollback
    # fences.  Rebinding them lets a small fixture prove cross-boundary Jinja
    # and YAML behavior without introducing a second production composer.
    recomposed = outer_text.replace(_SOURCE_MARKER_LINE, node_config_text)
    identities = {
        '_BUILTIN_KUBERNETES_OUTER_SIZE': len(outer_text.encode('utf-8')),
        '_BUILTIN_KUBERNETES_OUTER_SHA256': _sha256_bytes(
            outer_text.encode('utf-8')),
        '_BUILTIN_KUBERNETES_NODE_CONFIG_SIZE': len(
            node_config_text.encode('utf-8')),
        '_BUILTIN_KUBERNETES_NODE_CONFIG_SHA256': _sha256_bytes(
            node_config_text.encode('utf-8')),
        '_BUILTIN_KUBERNETES_MONOLITH_SIZE': len(recomposed.encode('utf-8')),
        '_BUILTIN_KUBERNETES_MONOLITH_SHA256': _sha256_bytes(
            recomposed.encode('utf-8')),
    }
    for name, value in identities.items():
        monkeypatch.setattr(kubernetes_template_source, name, value)
    return recomposed


def test_physical_sources_recompose_exact_monolith() -> None:
    monolith = _MONOLITH_PATH.read_bytes()
    outer = _OUTER_PATH.read_bytes()
    node_config = _NODE_CONFIG_PATH.read_bytes()

    assert len(monolith) == _MONOLITH_SIZE
    assert _sha256_bytes(monolith) == _MONOLITH_SHA256
    assert len(outer) == _OUTER_SIZE
    assert _sha256_bytes(outer) == _OUTER_SHA256
    assert len(node_config) == _NODE_CONFIG_SIZE
    assert _sha256_bytes(node_config) == _NODE_CONFIG_SHA256
    assert outer.count(_SOURCE_MARKER_LINE.encode('utf-8')) == 1

    recomposed = (
        kubernetes_template_source.compose_builtin_kubernetes_template_source(
            outer.decode('utf-8'), node_config.decode('utf-8')))
    assert recomposed.encode('utf-8') == monolith


def test_fragment_binding_names_are_closed() -> None:
    fragment = _NODE_CONFIG_PATH.read_text(encoding='utf-8')
    environment = jinja2.Environment()
    syntax_tree = environment.parse(fragment)
    binding_names = sorted(
        meta.find_undeclared_variables(syntax_tree) - set(environment.globals))
    canonical_names = ('\n'.join(binding_names) + '\n').encode('utf-8')

    assert len(binding_names) == 76
    assert _sha256_bytes(canonical_names) == _BINDING_NAMES_SHA256


@pytest.mark.parametrize('marker_count', [0, 2])
def test_composer_rejects_marker_count_mismatch(marker_count: int) -> None:
    _, outer, node_config = _read_sources()
    replacement = _SOURCE_MARKER_LINE * marker_count
    invalid_outer = outer.replace(_SOURCE_MARKER_LINE, replacement)

    _assert_composer_error(_MARKER_ERROR, invalid_outer, node_config)


def test_composer_rejects_outer_source_drift() -> None:
    _, outer, node_config = _read_sources()

    _assert_composer_error(_OUTER_ERROR, _same_length_drift(outer), node_config)


def test_composer_rejects_node_config_source_drift() -> None:
    _, outer, node_config = _read_sources()

    _assert_composer_error(_NODE_CONFIG_ERROR, outer,
                           _same_length_drift(node_config))


@pytest.mark.parametrize('invalid_source', ['outer', 'node-config'])
def test_composer_rejects_non_utf8_source(invalid_source: str) -> None:
    _, outer, node_config = _read_sources()
    if invalid_source == 'outer':
        outer = outer + '\ud800'
    else:
        node_config = node_config + '\ud800'

    _assert_composer_error(_INVALID_UTF8_ERROR, outer, node_config)


def test_composer_rejects_recomposed_source_drift(
        monkeypatch: pytest.MonkeyPatch) -> None:
    _, outer, node_config = _read_sources()
    monkeypatch.setattr(kubernetes_template_source,
                        '_BUILTIN_KUBERNETES_MONOLITH_SHA256', '0' * 64)

    _assert_composer_error(_RECOMPOSED_ERROR, outer, node_config)


@pytest.mark.parametrize('template_ref_factory', [
    lambda: 'kubernetes-ray.yml.j2',
    lambda: str(_MONOLITH_PATH),
])
def test_fill_template_builtin_dispatches_through_composer(
        template_ref_factory, monkeypatch: pytest.MonkeyPatch,
        tmp_path: pathlib.Path) -> None:
    monolith, outer, node_config = _read_sources()
    composer = mock.Mock(wraps=kubernetes_template_source.
                         compose_builtin_kubernetes_template_source)
    captured_sources: list[str] = []
    source_reads: list[pathlib.Path] = []
    real_open = open

    def tracking_open(path, *args, **kwargs):
        resolved_path = pathlib.Path(path).resolve()
        if resolved_path in {
                _MONOLITH_PATH.resolve(),
                _OUTER_PATH.resolve(),
                _NODE_CONFIG_PATH.resolve(),
        }:
            source_reads.append(resolved_path)
        return real_open(path, *args, **kwargs)

    class _CapturingTemplate:

        def __init__(self, source: str):
            captured_sources.append(source)

        def render(self, **_variables) -> str:
            return 'rendered-by-test'

    monkeypatch.setattr(kubernetes_template_source,
                        'compose_builtin_kubernetes_template_source', composer)
    monkeypatch.setattr(common_utils.jinja2, 'Template', _CapturingTemplate)
    monkeypatch.setattr(common_utils, 'open', tracking_open, raising=False)
    output_path = tmp_path / 'cluster.yml'

    common_utils.fill_template(template_ref_factory(), {'unused': object()},
                               str(output_path))

    composer.assert_called_once_with(outer, node_config)
    assert captured_sources == [monolith]
    assert source_reads == [_OUTER_PATH.resolve(), _NODE_CONFIG_PATH.resolve()]
    assert output_path.read_text(encoding='utf-8') == 'rendered-by-test'


@pytest.mark.parametrize('invalid_path', [_OUTER_PATH, _NODE_CONFIG_PATH])
def test_fill_template_rejects_non_utf8_physical_source(
        invalid_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
        tmp_path: pathlib.Path) -> None:
    real_open = open

    class _InvalidUtf8Reader:

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'\xff'

    def invalid_utf8_open(path, *args, **kwargs):
        if pathlib.Path(path).resolve() == invalid_path.resolve():
            return _InvalidUtf8Reader()
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(common_utils, 'open', invalid_utf8_open, raising=False)

    with pytest.raises(ValueError) as exc_info:
        common_utils.fill_template('kubernetes-ray.yml.j2', {},
                                   str(tmp_path / 'cluster.yml'))
    assert str(exc_info.value) == _INVALID_UTF8_ERROR


def test_fill_template_does_not_intercept_same_basename_plugin(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    plugin_dir = tmp_path / 'plugin'
    plugin_dir.mkdir()
    plugin_template = plugin_dir / 'kubernetes-ray.yml.j2'
    plugin_template.write_text('plugin={{ value }}', encoding='utf-8')
    composer = mock.Mock(side_effect=AssertionError('composer intercepted'))
    monkeypatch.setattr(kubernetes_template_source,
                        'compose_builtin_kubernetes_template_source', composer)
    output_path = tmp_path / 'plugin.yml'

    common_utils.fill_template(str(plugin_template), {'value': 'preserved'},
                               str(output_path))

    composer.assert_not_called()
    assert output_path.read_text(encoding='utf-8') == 'plugin=preserved'


def test_fill_template_preserves_other_template_behavior(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    other_template = tmp_path / 'other-ray.yml.j2'
    other_template.write_text(
        'values:{% for value in values %} {{ value }}{'
        '% endfor %}',
        encoding='utf-8')
    composer = mock.Mock(side_effect=AssertionError('composer intercepted'))
    monkeypatch.setattr(kubernetes_template_source,
                        'compose_builtin_kubernetes_template_source', composer)
    output_path = tmp_path / 'other.yml'

    common_utils.fill_template(str(other_template), {'values': [1, 2]},
                               str(output_path))

    composer.assert_not_called()
    assert output_path.read_text(encoding='utf-8') == 'values: 1 2'


def test_fill_template_preserves_bare_non_kubernetes_template(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    template_name = 'aws-ray.yml.j2'
    expected_source = (_TEMPLATE_DIR /
                       template_name).read_text(encoding='utf-8')
    captured_sources: list[str] = []

    class _CapturingTemplate:

        def __init__(self, source: str):
            captured_sources.append(source)

        def render(self, **_variables) -> str:
            return 'bare-template-preserved'

    composer = mock.Mock(side_effect=AssertionError('composer intercepted'))
    monkeypatch.setattr(kubernetes_template_source,
                        'compose_builtin_kubernetes_template_source', composer)
    monkeypatch.setattr(common_utils.jinja2, 'Template', _CapturingTemplate)
    output_path = tmp_path / 'aws.yml'

    common_utils.fill_template(template_name, {}, str(output_path))

    composer.assert_not_called()
    assert captured_sources == [expected_source]
    assert output_path.read_text(
        encoding='utf-8') == ('bare-template-preserved')


def test_composed_source_preserves_cross_boundary_jinja_and_yaml_semantics(
        monkeypatch: pytest.MonkeyPatch) -> None:
    outer = ("{% set role = 'head' %}\n" + _SOURCE_MARKER_LINE +
             'consumer: *pod\n')
    node_config = ('pod: &pod\n'
                   '  metadata:\n'
                   '    name: "{{ cluster_name }}-{{ role }}"\n')
    expected_source = _install_synthetic_contract(monkeypatch, outer,
                                                  node_config)

    composed_source = (
        kubernetes_template_source.compose_builtin_kubernetes_template_source(
            outer, node_config))
    rendered = jinja2.Template(composed_source).render(cluster_name='sky')
    parsed = yaml_utils.safe_load(rendered)

    assert composed_source == expected_source
    assert parsed['pod']['metadata']['name'] == 'sky-head'
    assert parsed['consumer'] is parsed['pod']


def test_composed_source_preserves_jinja_and_yaml_error_coordinates(
        monkeypatch: pytest.MonkeyPatch) -> None:
    jinja_outer = 'prefix: true\n' + _SOURCE_MARKER_LINE + 'suffix: true\n'
    invalid_jinja_fragment = 'pod:\n  image: {{ broken\n'
    jinja_oracle = _install_synthetic_contract(monkeypatch, jinja_outer,
                                               invalid_jinja_fragment)
    jinja_composed = (
        kubernetes_template_source.compose_builtin_kubernetes_template_source(
            jinja_outer, invalid_jinja_fragment))

    with pytest.raises(jinja2.TemplateSyntaxError) as oracle_jinja_error:
        jinja2.Environment().parse(jinja_oracle)
    with pytest.raises(jinja2.TemplateSyntaxError) as composed_jinja_error:
        jinja2.Environment().parse(jinja_composed)
    assert type(composed_jinja_error.value) is type(oracle_jinja_error.value)
    assert str(composed_jinja_error.value) == str(oracle_jinja_error.value)
    assert composed_jinja_error.value.lineno == oracle_jinja_error.value.lineno

    yaml_outer = 'prefix: true\n' + _SOURCE_MARKER_LINE + 'suffix: true\n'
    invalid_yaml_fragment = 'pod:\n  image: [unterminated\n'
    yaml_oracle = _install_synthetic_contract(monkeypatch, yaml_outer,
                                              invalid_yaml_fragment)
    yaml_composed = (
        kubernetes_template_source.compose_builtin_kubernetes_template_source(
            yaml_outer, invalid_yaml_fragment))

    with pytest.raises(yaml.parser.ParserError) as oracle_yaml_error:
        yaml_utils.safe_load(yaml_oracle)
    with pytest.raises(yaml.parser.ParserError) as composed_yaml_error:
        yaml_utils.safe_load(yaml_composed)
    assert type(composed_yaml_error.value) is type(oracle_yaml_error.value)
    assert str(composed_yaml_error.value) == str(oracle_yaml_error.value)
    assert (composed_yaml_error.value.problem_mark.line ==
            oracle_yaml_error.value.problem_mark.line)
    assert (composed_yaml_error.value.problem_mark.column ==
            oracle_yaml_error.value.problem_mark.column)
