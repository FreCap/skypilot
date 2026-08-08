"""Exact intermediate V2 config-access artifact and source closure."""

# pylint: disable=protected-access

import ast
import copy
import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest
import test_serve_resource_action_renderer_v2 as renderer_fixtures

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_cleanup_v2 as cleanup_v2
from sky.serve import resource_action_preflight_v2 as preflight_v2
from sky.serve import resource_action_provider_artifacts as provider_artifacts
from sky.serve import resource_action_renderer_v2 as renderer_v2
from sky.serve import resource_action_representability as representability
from sky.serve import resource_actions as actions

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ARTIFACT_PATH = (_REPO_ROOT / 'sky' / 'serve' / 'resource_action_artifacts' /
                  'kubernetes_renderer_v2' / 'config_access_inventory.json')
_V1_ARTIFACT_PATH = (_REPO_ROOT / 'sky' / 'serve' /
                     'resource_action_artifacts' / 'kubernetes_renderer_v1' /
                     'config_access_inventory.json')
_RAW_SIZE = 64_527
_RAW_SHA256 = 'a532de512e33448adec38707651dde7a82c742d512eeec746f42e01c60665ad1'
_CANONICAL_SHA256 = (
    '1fc41b7eabaafa7375f8690302424502579ea9d317107d628ca6fe8c54e560d2')
_INVENTORIED_AST_SHA256 = (
    '505c190e2174a1efeb9bb8fb82c38f9641dac388ff291e9f2588e95a78e9a005')

_ENTRYPOINTS = (
    ('launch_capsule_constructor', 'sky.serve.resource_action_renderer_v2.'
     'construct_provider_kubernetes_execution_capsule_v2'),
    ('down_capsule_constructor', 'sky.serve.resource_action_renderer_v2.'
     'construct_provider_kubernetes_down_execution_capsule_v2'),
    ('cleanup_target_rederiver', 'sky.serve.resource_action_cleanup_v2.'
     'rederive_provider_kubernetes_cleanup_target_v2'),
    ('representability_enumerator',
     'sky.serve.resource_action_representability.'
     'enumerate_provider_resource_action_representability_v2'),
)

_MODULES = {
    renderer_v2.__name__: Path(renderer_v2.__file__),
    cleanup_v2.__name__: Path(cleanup_v2.__file__),
    representability.__name__: Path(representability.__file__),
    provider_artifacts.__name__: Path(provider_artifacts.__file__),
    authority.__name__: Path(authority.__file__),
    preflight_v2.__name__: Path(preflight_v2.__file__),
    actions.__name__: Path(actions.__file__),
}
_MODULE_ALIASES = {
    renderer_v2.__name__: {
        'authority': authority.__name__,
        'cleanup_v2': cleanup_v2.__name__,
        'provider_artifacts': provider_artifacts.__name__,
    },
    cleanup_v2.__name__: {},
    representability.__name__: {
        'actions': actions.__name__,
        'cleanup_v2': cleanup_v2.__name__,
        'preflight_v2': preflight_v2.__name__,
        'renderer_v2': renderer_v2.__name__,
    },
    provider_artifacts.__name__: {},
    authority.__name__: {},
    preflight_v2.__name__: {},
    actions.__name__: {},
}
_TERMINAL_ATTRIBUTES = frozenset({
    'canonical_bytes',
    'sha256',
    'value',
    'isascii',
    'lower',
    'encode',
    'validate_action',
    'validate_requested_target',
    'validate_workspace_identity',
    'items',
    'values',
    'keys',
    'removeprefix',
    'rsplit',
    'split',
    'endswith',
})


def _artifact() -> tuple[bytes, dict[str, Any]]:
    raw = _ARTIFACT_PATH.read_bytes()
    value = json.loads(raw)
    assert type(value) is dict
    return raw, value


def _source_nodes() -> tuple[dict[str, ast.AST], dict[str, str]]:
    nodes: dict[str, ast.AST] = {}
    owning_modules: dict[str, str] = {}
    for module_name, path in _MODULES.items():
        module = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        for node in module.body:
            if type(node) is ast.FunctionDef:
                qualified_name = f'{module_name}.{node.name}'
                nodes[qualified_name] = node
                owning_modules[qualified_name] = module_name
            elif type(node) is ast.ClassDef:
                for child in node.body:
                    if type(child) is ast.FunctionDef:
                        qualified_name = (
                            f'{module_name}.{node.name}.{child.name}')
                        nodes[qualified_name] = child
                        owning_modules[qualified_name] = module_name
    return nodes, owning_modules


def _expression_path(
    expression: ast.AST,
    aliases: dict[str, tuple[str, ...]],
) -> tuple[tuple[str, ...], bool, bool] | None:
    if type(expression) is ast.Name:
        path = aliases.get(expression.id)
        return None if path is None else (path, False, True)
    if type(expression) is ast.Attribute:
        parent = _expression_path(expression.value, aliases)
        if parent is None:
            return None
        path, _, aliasable = parent
        if expression.attr in _TERMINAL_ATTRIBUTES:
            return path, True, False
        return path + (expression.attr,), True, aliasable
    if type(expression) is ast.Subscript:
        parent = _expression_path(expression.value, aliases)
        if parent is None:
            return None
        path, _, aliasable = parent
        index = expression.slice
        token = (str(index.value) if type(index) is ast.Constant and
                 type(index.value) in (int, str) else '*')
        return path + (token,), True, aliasable
    if (type(expression) is ast.Call and
            type(expression.func) is ast.Attribute):
        parent = _expression_path(expression.func.value, aliases)
        if parent is None:
            return None
        path, _, _ = parent
        if expression.func.attr == 'get':
            if (expression.args and type(expression.args[0]) is ast.Constant and
                    type(expression.args[0].value) is str):
                return path + (expression.args[0].value,), True, False
            return path, True, False
        if expression.func.attr == 'canonical_value':
            return path, True, False
        if expression.func.attr in _TERMINAL_ATTRIBUTES:
            return path, True, False
    if (type(expression) is ast.Call and type(expression.func) is ast.Name and
            expression.func.id == 'getattr' and
            len(expression.args) in (2, 3) and not expression.keywords and
            type(expression.args[1]) is ast.Constant and
            type(expression.args[1].value) is str):
        parent = _expression_path(expression.args[0], aliases)
        if parent is not None:
            return (parent[0] + (expression.args[1].value,), True, False)
    return None


def _parameter_pointers(
    function: ast.FunctionDef,
    parameter: str,
) -> tuple[str, ...]:
    aliases: dict[str, tuple[str, ...]] = {parameter: ()}
    parent_by_id = {
        id(child): parent for parent in ast.walk(function)
        for child in ast.iter_child_nodes(parent)
    }
    alias_rhs_ids: set[int] = set()
    assignments = sorted((cast(ast.Assign | ast.AnnAssign, node)
                          for node in ast.walk(function)
                          if type(node) in (ast.Assign, ast.AnnAssign)),
                         key=lambda node: (node.lineno, node.col_offset))
    for assignment in assignments:
        targets = (assignment.targets if isinstance(assignment, ast.Assign) else
                   (assignment.target,))
        value = assignment.value
        if value is None or type(value) is ast.Call:
            continue
        resolved = _expression_path(value, aliases)
        if resolved is None or not resolved[2]:
            continue
        path = resolved[0]
        for target in targets:
            if type(target) is not ast.Name:
                continue
            aliases[target.id] = path
            if any(
                    type(node) is ast.Name and type(node.ctx) is ast.Load and
                    node.id == target.id and (node.lineno, node.col_offset) > (
                        assignment.lineno, assignment.col_offset)
                    for node in ast.walk(function)):
                alias_rhs_ids.add(id(value))

    pointers: set[tuple[str, ...]] = set()
    for node in ast.walk(function):
        if type(node) not in (ast.Name, ast.Attribute, ast.Subscript, ast.Call):
            continue
        if type(node) is ast.Name and type(node.ctx) is not ast.Load:
            continue
        resolved = _expression_path(node, aliases)
        if resolved is None or not resolved[1] or id(node) in alias_rhs_ids:
            continue
        parent = parent_by_id.get(id(node))
        if ((type(parent) in (ast.Attribute, ast.Subscript) and
             cast(ast.Attribute | ast.Subscript, parent).value is node) or
            (type(parent) is ast.Call and parent.func is node) or
            (type(parent) is ast.Call and type(parent.func) is ast.Attribute and
             parent.func.value is node) or
            (type(parent) is ast.Call and type(parent.func) is ast.Name and
             parent.func.id == 'getattr' and parent.args and
             parent.args[0] is node)):
            continue
        pointers.add(resolved[0])
    return tuple('/' if not pointer else '/' + '/'.join(pointer)
                 for pointer in sorted(pointers))


def _inventoried_ast_sha256(
    call_graph: list[dict[str, Any]],
    nodes: dict[str, ast.AST],
) -> str:

    def _stable_dump(node: ast.AST) -> str:
        # Python 3.14 changed ast.dump() to hide empty fields by default and
        # added empty ``type_params`` fields to definitions.  Preserve the
        # pre-3.14 structural preimage across every supported interpreter.
        dump_kwargs: dict[str, Any] = {
            'annotate_fields': True,
            'include_attributes': False,
            'show_empty': True,
        }
        try:
            dumped = ast.dump(  # pylint: disable=unexpected-keyword-arg
                node, **dump_kwargs)
        except TypeError:
            dumped = ast.dump(node,
                              annotate_fields=True,
                              include_attributes=False)
        return dumped.replace(', type_params=[]', '')

    preimage = bytearray()
    for row in call_graph:
        qualified_name = row['caller']
        preimage.extend(qualified_name.encode('utf-8'))
        preimage.extend(b'\0')
        preimage.extend(_stable_dump(nodes[qualified_name]).encode('utf-8'))
        preimage.extend(b'\0')
    return hashlib.sha256(preimage).hexdigest()


def _source_callees(
    caller: str,
    function: ast.FunctionDef,
    module_name: str,
    inventoried_names: set[str],
) -> tuple[str, ...]:
    calls: list[tuple[int, int, str]] = []
    for node in ast.walk(function):
        if type(node) is not ast.Call:
            continue
        qualified_name: str | None = None
        candidate: str | None
        if type(node.func) is ast.Name:
            candidate = f'{module_name}.{node.func.id}'
            if candidate in inventoried_names:
                qualified_name = candidate
            elif (caller == _ENTRYPOINTS[3][1] and node.func.id == 'projector'):
                qualified_name = ('sky.serve.resource_action_representability.'
                                  '_CaseProjectorV2.__call__')
        elif (type(node.func) is ast.Attribute and
              type(node.func.value) is ast.Name):
            target_module = _MODULE_ALIASES[module_name].get(node.func.value.id)
            candidate = (None if target_module is None else
                         f'{target_module}.{node.func.attr}')
            if candidate in inventoried_names:
                qualified_name = candidate
        if qualified_name is not None:
            calls.append((node.lineno, node.col_offset, qualified_name))
    ordered = []
    for _, _, qualified_name in sorted(calls):
        if qualified_name not in ordered:
            ordered.append(qualified_name)
    return tuple(ordered)


def test_v2_config_inventory_bytes_parser_and_schema_are_exact() -> None:
    raw, value = _artifact()
    canonical = actions.canonical_json_bytes(value)
    assert len(raw) == _RAW_SIZE < 65_536
    assert raw == canonical + b'\n'
    assert hashlib.sha256(raw).hexdigest() == _RAW_SHA256
    assert hashlib.sha256(canonical).hexdigest() == _CANONICAL_SHA256
    assert set(value) == {
        'schema', 'artifact_roles', 'entrypoints', 'call_graph', 'input_access',
        'transient_flow', 'provider_operations', 'forbidden_sources'
    }
    parsed = renderer_v2.ProviderKubernetesConfigAccessInventoryV2.from_value(
        value)
    assert parsed.canonical_bytes == canonical

    changed = copy.deepcopy(value)
    changed['forbidden_sources'] = changed['forbidden_sources'][:-1]
    with pytest.raises(ValueError, match='not exact'):
        renderer_v2.ProviderKubernetesConfigAccessInventoryV2.from_value(
            changed)


def test_v2_inventory_has_four_roots_and_closed_reachable_call_graph() -> None:
    _, value = _artifact()
    assert value['entrypoints'] == [{
        'sequence': sequence,
        'role': role,
        'qualified_name': qualified_name,
    } for sequence, (role, qualified_name) in enumerate(_ENTRYPOINTS)]
    graph = value['call_graph']
    assert len(graph) == 35
    assert [row['sequence'] for row in graph] == list(range(len(graph)))
    callers = tuple(row['caller'] for row in graph)
    assert len(callers) == len(set(callers))
    assert callers[:4] == tuple(item[1] for item in _ENTRYPOINTS)
    assert all(
        len(row) == 3 and set(row) == {'sequence', 'caller', 'callees'}
        for row in graph)
    assert all(
        len(row['callees']) == len(set(row['callees'])) and
        set(row['callees']).issubset(callers) for row in graph)

    by_caller = {row['caller']: tuple(row['callees']) for row in graph}
    reached = set()
    pending = [item[1] for item in _ENTRYPOINTS]
    while pending:
        caller = pending.pop(0)
        if caller in reached:
            continue
        reached.add(caller)
        pending.extend(by_caller[caller])
    assert reached == set(callers)
    encoded = json.dumps(value, sort_keys=True)
    assert 'validated_launch_spec_v1' not in encoded
    assert 'sky.serve.resource_action_renderer.' not in encoded


def test_v2_inventory_call_graph_and_parameter_access_match_source_ast(
) -> None:
    _, value = _artifact()
    nodes, owning_modules = _source_nodes()
    graph = value['call_graph']
    inventoried_names = {row['caller'] for row in graph}
    assert inventoried_names.issubset(nodes)
    for row in graph:
        caller = row['caller']
        function = nodes[caller]
        assert type(function) is ast.FunctionDef
        assert _source_callees(caller, function, owning_modules[caller],
                               inventoried_names) == tuple(row['callees'])

    expected_rows = []
    for row in graph:
        consumer = row['caller']
        function = nodes[consumer]
        assert type(function) is ast.FunctionDef
        arguments = [
            *function.args.posonlyargs, *function.args.args,
            *function.args.kwonlyargs
        ]
        for argument in arguments:
            contract = (
                ast.unparse(argument.annotation)
                if argument.annotation is not None else
                ('_CaseProjectorV2' if argument.arg == 'self' else 'untyped'))
            expected_rows.append((consumer, argument.arg, contract,
                                  _parameter_pointers(function, argument.arg)))
    actual_rows = []
    for sequence, row in enumerate(value['input_access']):
        assert row['sequence'] == sequence
        assert set(row) == {
            'sequence', 'consumer', 'parameter', 'contract', 'accesses'
        }
        pointers = []
        for access_sequence, access in enumerate(row['accesses']):
            assert access['sequence'] == access_sequence
            assert set(access) == {
                'sequence', 'pointer', 'disposition', 'use', 'binding_names'
            }
            assert access['pointer'].startswith('/')
            assert access['disposition'] in {'embedded', 'content_addressed'}
            assert access['binding_names'] == sorted(access['binding_names'])
            pointers.append(access['pointer'])
        actual_rows.append((row['consumer'], row['parameter'], row['contract'],
                            tuple(pointers)))
    assert actual_rows == expected_rows
    assert len(actual_rows) == 71
    assert sum(not row['accesses'] for row in value['input_access']) == 25
    assert _inventoried_ast_sha256(graph, nodes) == _INVENTORIED_AST_SHA256


def test_v2_inventory_leaf_transient_and_effect_closure_is_exact() -> None:
    _, value = _artifact()
    roles = value['artifact_roles']
    assert [row['sequence'] for row in roles] == list(range(5))
    assert [row['role'] for row in roles] == [
        'outer_template', 'node_fragment', 'binding_schema',
        'config_access_inventory', 'admitted_object_normalization'
    ]
    assert roles[2]['schema_id'].endswith('.bindings.v2')
    assert roles[3]['schema_id'].endswith('.config-access-inventory.v2')
    assert all(
        row['consumers'] and len(row['consumers']) == len(set(row['consumers']))
        for row in roles)

    transients = value['transient_flow']
    assert len(transients) == 18
    assert [row['sequence'] for row in transients] == list(range(18))
    assert len({row['name'] for row in transients}) == len(transients)
    graph_names = {row['caller'] for row in value['call_graph']}
    assert all(row['producer'] in graph_names and row['consumers'] and
               set(row['consumers']).issubset(graph_names)
               for row in transients)

    v1_operations = json.loads(
        _V1_ARTIFACT_PATH.read_text(encoding='utf-8'))['provider_operations']
    assert value['provider_operations'] == v1_operations
    operations = value['provider_operations']
    assert operations['renderer'] == []
    assert operations['normalizer'] == []
    assert len(operations['object_session']) == 15
    assert len(operations['preflight_contracts']) == 2
    assert all(operation['api_group'] == ''
               for operation in operations['object_session'])
    assert value['forbidden_sources'] == sorted(value['forbidden_sources'])
    assert {
        'caller_supplied_cleanup_target', 'database', 'environment',
        'provider_discovery', 'secret', 'skypilot_config', 'system_clock'
    }.issubset(value['forbidden_sources'])


def test_runtime_validator_pins_actual_four_function_roots(
        monkeypatch: pytest.MonkeyPatch) -> None:
    _, value = _artifact()
    inventory = renderer_v2.ProviderKubernetesConfigAccessInventoryV2(value)
    resolved_inventory = object.__new__(
        renderer_v2.ResolvedProviderKubernetesConfigAccessInventoryArtifactV2)
    object.__setattr__(resolved_inventory, 'inventory', inventory)
    resolved = object.__new__(
        renderer_v2.ResolvedProviderKubernetesRendererArtifactSetV2)
    object.__setattr__(resolved, 'config_access_inventory', resolved_inventory)
    renderer_v2._validate_config_access_inventory_v2(resolved)

    monkeypatch.setattr(
        representability,
        'enumerate_provider_resource_action_representability_v2',
        lambda unused: None)
    with pytest.raises(ValueError, match='exact four native roots'):
        renderer_v2._validate_config_access_inventory_v2(resolved)


def test_native_v2_launch_renderer_output_is_byte_deterministic(
        monkeypatch: pytest.MonkeyPatch) -> None:
    _, inventory_value = _artifact()
    monkeypatch.setattr(renderer_fixtures, '_config_inventory_value',
                        lambda: copy.deepcopy(inventory_value))
    raw_input = renderer_fixtures._renderer_input_raw()
    expected_objects = raw_input.pop('_expected_objects')
    resolved_artifacts = renderer_fixtures._resolved_artifacts()
    monkeypatch.setattr(renderer_v2, '_read_renderer_artifacts_v2',
                        lambda unused: resolved_artifacts)
    cohort = renderer_fixtures._cohort()

    first = renderer_v2.construct_provider_kubernetes_execution_capsule_v2(
        renderer_v2.ProviderKubernetesRendererInputV2.from_value(
            copy.deepcopy(raw_input)), cohort)
    second = renderer_v2.construct_provider_kubernetes_execution_capsule_v2(
        renderer_v2.ProviderKubernetesRendererInputV2.from_value(
            copy.deepcopy(raw_input)), cohort)

    assert first.canonical_bytes == second.canonical_bytes
    assert [item.canonical_value() for item in first.objects
           ] == expected_objects
