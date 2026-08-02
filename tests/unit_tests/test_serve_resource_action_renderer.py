"""Focused tests for the effect-free Kubernetes renderer pipeline."""

# pylint: disable=exec-used,import-outside-toplevel,protected-access
# pylint: disable=undefined-variable

import ast
import copy
import dataclasses
import hashlib
import inspect
import multiprocessing
import operator
import os
from pathlib import Path
import sys
import types
from typing import Any

import pytest
import test_serve_resource_action_launch_execution_config as fixtures

from sky.serve import resource_action_provider_artifacts as provider_artifacts
from sky.serve import resource_action_renderer as renderer
from sky.serve import resource_actions as actions

_ARTIFACT_ROOT = (Path(__file__).resolve().parents[2] / 'sky' / 'serve' /
                  'resource_action_artifacts' / 'kubernetes_renderer_v1')
_PUBLIC_RENDERER_ENTRYPOINTS = {
    'assemble_and_revalidate_provider_kubernetes_execution_capsule_v1',
    'build_provider_kubernetes_object_plans_v1',
    'construct_provider_kubernetes_execution_capsule_v1',
    'render_provider_kubernetes_objects_v1',
    'resolve_provider_kubernetes_bindings_v1',
    'resolve_provider_kubernetes_renderer_artifacts_v1',
    'validate_kubernetes_serve_three_object_body_v1',
    'validate_provider_kubernetes_config_access_inventory_v1',
    'validate_provider_kubernetes_renderer_input_v1',
}
_BINDING_NAMES = (
    'head_labels',
    'head_name',
    'head_pod_labels',
    'head_pod_name',
    'head_service_selector',
    'head_ssh_labels',
    'head_ssh_name',
    'image_pull_policy',
    'original_user',
    'pod_cpu_limit',
    'pod_cpu_request',
    'pod_memory_limit',
    'pod_memory_request',
    'replica_id_text',
    'target_namespace',
    'workload_image',
    'workload_service_account',
)
_EXPECTED_MODULE_AST_SHA256_BY_PYTHON = {
    (3, 10): {
        'resource_action_renderer.py': '6a1932c7d4941224cf3a0e02848129f245172c7aefc5b4ab5282528c848be9d3',
        'resource_action_provider_artifacts.py': '88301f01d2ccab44ba6bb71c2f0dfb243a8d05a011a8d34e833dbf9e6252310e',
    },
    (3, 11): {
        'resource_action_renderer.py': '6a1932c7d4941224cf3a0e02848129f245172c7aefc5b4ab5282528c848be9d3',
        'resource_action_provider_artifacts.py': '88301f01d2ccab44ba6bb71c2f0dfb243a8d05a011a8d34e833dbf9e6252310e',
    },
    (3, 12): {
        'resource_action_renderer.py': '50bcf3bba5d37e854d0a8f5d4db3b5c998679e885d3daa356895e4c8cf8ef4a0',
        'resource_action_provider_artifacts.py': 'e3e23e6f3eedf06221d1e303d34eeb0720cdfcaf7a88a75046223a868b1b6e8d',
    },
    (3, 13): {
        'resource_action_renderer.py': 'abe742fe165f081ddce34c0b9fa5a6650abaa32970477858d2191cd579492555',
        'resource_action_provider_artifacts.py': 'e3c0907281fad6db8c66ee129f3470fbcaba8f0324ed291a8fd35c7aeaa0f989',
    },
    (3, 14): {
        'resource_action_renderer.py': 'abe742fe165f081ddce34c0b9fa5a6650abaa32970477858d2191cd579492555',
        'resource_action_provider_artifacts.py': 'e3c0907281fad6db8c66ee129f3470fbcaba8f0324ed291a8fd35c7aeaa0f989',
    },
}
_PYTHON_MINOR = (sys.version_info.major, sys.version_info.minor)
_EXPECTED_MODULE_AST_SHA256 = (
    _EXPECTED_MODULE_AST_SHA256_BY_PYTHON[_PYTHON_MINOR])


def _renderer_input_raw() -> dict:
    seed = fixtures._capsule_raw()
    del seed['objects']
    target = fixtures._target()
    return {
        'version': 1,
        'contract': 'validated_launch_spec_v1',
        'resource_identity': fixtures._resource_identity(),
        'sky_cluster_name': target['sky_cluster_name'],
        'sky_cluster_record_uuid': target['sky_cluster_record_uuid'],
        'name_basis': target['kubernetes']['name_basis'],
        'seed': seed,
        'retained_source': fixtures._content_source(),
    }


def _renderer_input() -> actions.ProviderKubernetesRendererInputV1:
    return actions.ProviderKubernetesRendererInputV1.from_value(
        _renderer_input_raw())


def _resolved_artifacts(
) -> renderer.ResolvedProviderKubernetesRendererArtifactSetV1:
    renderer_input = _renderer_input()
    return renderer.resolve_provider_kubernetes_renderer_artifacts_v1(
        renderer_input)


def _fake_distribution(tmp_path: Path) -> tuple[Path, Path]:
    fake_sky = tmp_path / 'sky'
    fake_artifact_root = (fake_sky / 'serve' / 'resource_action_artifacts' /
                          'kubernetes_renderer_v1')
    fake_artifact_root.mkdir(parents=True)
    (fake_sky / '__init__.py').write_text('', encoding='utf-8')
    for source in _ARTIFACT_ROOT.iterdir():
        (fake_artifact_root / source.name).write_bytes(source.read_bytes())
    return fake_sky, fake_artifact_root


def _ast_functions(path: Path) -> tuple[ast.Module, dict[str, ast.FunctionDef]]:
    module = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    functions = {
        node.name: node for node in module.body if type(node) is ast.FunctionDef
    }
    return module, functions


def _expression_pointer(
    expression: ast.AST,
    aliases: dict[str, tuple[str, ...]],
) -> tuple[str, ...] | None:
    if type(expression) is ast.Name:
        return aliases.get(expression.id)
    if type(expression) is ast.Attribute:
        parent = _expression_pointer(expression.value, aliases)
        return None if parent is None else parent + (expression.attr,)
    if type(expression) is ast.Subscript:
        parent = _expression_pointer(expression.value, aliases)
        if parent is None:
            return None
        index = expression.slice
        if type(index) is ast.Constant and type(index.value) in (int, str):
            token = str(index.value)
        else:
            token = '*'
        return parent + (token,)
    if (type(expression) is ast.Call and type(expression.func) is ast.Name and
            expression.func.id == 'getattr' and
            len(expression.args) in (2, 3) and not expression.keywords and
            type(expression.args[1]) is ast.Constant and
            type(expression.args[1].value) is str):
        parent = _expression_pointer(expression.args[0], aliases)
        if parent is not None:
            return parent + (expression.args[1].value,)
    return None


def _function_renderer_input_pointers(
        function: ast.FunctionDef) -> set[tuple[str, ...]]:
    aliases: dict[str, tuple[str, ...]] = {'renderer_input': ()}
    parent_by_node = {
        id(child): parent for parent in ast.walk(function)
        for child in ast.iter_child_nodes(parent)
    }
    assignments = sorted((node for node in ast.walk(function)
                          if type(node) in (ast.Assign, ast.AnnAssign)),
                         key=lambda node: (node.lineno, node.col_offset))
    alias_rhs_nodes: set[int] = set()
    for assignment in assignments:
        if type(assignment) is ast.Assign:
            targets = assignment.targets
            value = assignment.value
        else:
            targets = (assignment.target,)
            value = assignment.value
        if value is None:
            continue
        pointer = _expression_pointer(value, aliases)
        if pointer is None:
            continue
        for target in targets:
            if type(target) is ast.Name:
                aliases[target.id] = pointer
                if any(
                        type(node) is ast.Name and
                        type(node.ctx) is ast.Load and node.id == target.id and
                    (node.lineno, node.col_offset) > (assignment.lineno,
                                                      assignment.col_offset)
                        for node in ast.walk(function)):
                    alias_rhs_nodes.add(id(value))

    pointers = set()
    for node in ast.walk(function):
        if type(node) not in (ast.Name, ast.Attribute, ast.Subscript, ast.Call):
            continue
        if type(node) is ast.Name and type(node.ctx) is not ast.Load:
            continue
        pointer = _expression_pointer(node, aliases)
        if pointer is None or not pointer or id(node) in alias_rhs_nodes:
            continue
        parent = parent_by_node.get(id(node))
        if ((type(parent) in (ast.Attribute, ast.Subscript) and
             parent.value is node) or
            (type(parent) is ast.Call and type(parent.func) is ast.Name and
             parent.func.id == 'getattr' and parent.args and
             parent.args[0] is node)):
            continue
        if pointer[-1] in ('canonical_bytes', 'canonical_value'):
            pointer = pointer[:-1]
        if pointer:
            pointers.add(pointer)
    return pointers


def _pointer_prefix_matches(left: tuple[str, ...], right: tuple[str,
                                                                ...]) -> bool:
    for left_token, right_token in zip(left, right):
        if (left_token != right_token and left_token != '*' and
                right_token != '*'):
            return False
    return True


def _expected_pointer_covers_actual(expected: tuple[str, ...],
                                    actual: tuple[str, ...]) -> bool:
    return (len(expected) <= len(actual) and
            _pointer_prefix_matches(expected, actual))


def _module_ast_sha256(module: ast.Module) -> str:
    preimage = ast.dump(module, annotate_fields=True,
                        include_attributes=False).encode('utf-8')
    return hashlib.sha256(preimage).hexdigest()


def _function_with_insertion(function: Any, insertion: str) -> Any:
    source = inspect.getsource(function)
    marker = '    renderer = renderer_input.seed.renderer\n'
    if source.count(marker) != 1:
        raise AssertionError('resolver mutation marker is not exact.')
    mutated_source = source.replace(marker, insertion + marker)
    namespace = dict(function.__globals__)
    exec(compile(mutated_source, '<renderer-inventory-drift>', 'exec'),
         namespace)
    return namespace[function.__name__]


def _resolve_artifacts_in_child(send_connection: Any,
                                renderer_input: Any) -> None:
    try:
        renderer.resolve_provider_kubernetes_renderer_artifacts_v1(
            renderer_input)
    except ValueError as error:
        send_connection.send(('ValueError', str(error)))
    else:
        send_connection.send(('success', ''))
    finally:
        send_connection.close()


def _assert_resolver_fails_without_blocking(renderer_input: Any) -> None:
    context = multiprocessing.get_context('fork')
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(target=_resolve_artifacts_in_child,
                              args=(send_connection, renderer_input),
                              daemon=True)
    process.start()
    send_connection.close()
    try:
        process.join(timeout=3)
        if process.is_alive():
            process.terminate()
            process.join(timeout=3)
            pytest.fail('descriptor-safe resolver blocked on a FIFO leaf.')
        assert process.exitcode == 0
        assert receive_connection.poll()
        outcome, message = receive_connection.recv()
    finally:
        receive_connection.close()
        if process.is_alive():
            process.terminate()
            process.join(timeout=3)
        process.close()
    assert outcome == 'ValueError', message


def _validated_pipeline(
) -> tuple[actions.ProviderKubernetesRendererInputV1,
           renderer.ResolvedProviderKubernetesRendererArtifactSetV1, tuple[
               actions.ValidatedKubernetesServeThreeObjectBodyV1, ...], tuple[
                   actions.ProviderKubernetesRequestNormalizationV1, ...]]:
    renderer_input = _renderer_input()
    resolved = renderer.resolve_provider_kubernetes_renderer_artifacts_v1(
        renderer_input)
    rendered = renderer.render_provider_kubernetes_objects_v1(
        renderer_input, resolved)
    validated = renderer.validate_kubernetes_serve_three_object_body_v1(
        renderer_input, rendered)
    normalizations = tuple(
        provider_artifacts.normalize_kubernetes_request_object_v1(
            body.role, body, resolved.admitted_object_normalization)
        for body in validated)
    return renderer_input, resolved, validated, normalizations


def test_renderer_has_only_the_nine_inventoried_public_entrypoints() -> None:
    public_functions = {
        name: function
        for name, function in inspect.getmembers(renderer, inspect.isfunction)
        if not name.startswith('_')
    }
    assert set(public_functions) == _PUBLIC_RENDERER_ENTRYPOINTS
    assert all(function.__module__ == renderer.__name__
               for function in public_functions.values())


def test_source_ast_matches_inventory_calls_accesses_and_imports() -> None:
    renderer_module, renderer_functions = _ast_functions(Path(
        renderer.__file__))
    _, normalizer_functions = _ast_functions(Path(provider_artifacts.__file__))
    actual_imports = set()
    for node in renderer_module.body:
        if type(node) is ast.Import:
            actual_imports.update(
                ('import', alias.name, alias.asname) for alias in node.names)
        elif type(node) is ast.ImportFrom:
            actual_imports.update(
                ('from', node.module, alias.name, alias.asname, node.level)
                for alias in node.names)
    assert actual_imports == {
        ('from', '__future__', 'annotations', None, 0),
        ('import', 'dataclasses', None),
        ('import', 'dis', None),
        ('import', 'hashlib', None),
        ('import', 'json', None),
        ('import', 'marshal', None),
        ('import', 'os', None),
        ('import', 're', None),
        ('import', 'stat', None),
        ('import', 'sys', None),
        ('import', 'types', None),
        ('from', 'typing', 'Any', None, 0),
        ('from', 'typing', 'ClassVar', None, 0),
        ('from', 'typing', 'TypeVar', None, 0),
        ('import', 'sky', 'sky_package'),
        ('from', 'sky.serve', 'resource_action_provider_artifacts',
         'provider_artifacts', 0),
        ('from', 'sky.serve', 'resource_actions', 'actions', 0),
    }

    inventory = (_resolved_artifacts().config_access_inventory.inventory.
                 canonical_value())
    qualified_names = tuple(
        entry['qualified_name'] for entry in inventory['entrypoints'])
    qualified_by_simple = {
        qualified_name.rsplit('.', 1)[1]: qualified_name
        for qualified_name in qualified_names
    }
    function_nodes: dict[str, ast.FunctionDef] = {}
    for qualified_name in qualified_names:
        simple_name = qualified_name.rsplit('.', 1)[1]
        source_functions = (normalizer_functions if qualified_name.startswith(
            'sky.serve.resource_action_provider_artifacts.') else
                            renderer_functions)
        function_nodes[qualified_name] = source_functions[simple_name]

    for graph_entry in inventory['call_graph']:
        calls = []
        for node in ast.walk(function_nodes[graph_entry['caller']]):
            if type(node) is not ast.Call:
                continue
            qualified_name = None
            if type(node.func) is ast.Name:
                qualified_name = qualified_by_simple.get(node.func.id)
            elif (type(node.func) is ast.Attribute and
                  type(node.func.value) is ast.Name and
                  node.func.value.id == 'provider_artifacts'):
                qualified_name = qualified_by_simple.get(node.func.attr)
            if qualified_name is not None:
                calls.append((node.lineno, node.col_offset, qualified_name))
        assert tuple(item[2] for item in sorted(calls)) == tuple(
            graph_entry['callees'])

    expected_accesses: dict[str, list[tuple[str, ...]]] = {}
    for access in inventory['input_access']:
        expected_accesses.setdefault(access['consumer'], []).append(
            tuple(access['source_pointer'][1:].split('/')))
    renderer_entrypoints = tuple(
        qualified_name for qualified_name in qualified_names
        if qualified_name.startswith('sky.serve.resource_action_renderer.'))
    for consumer in renderer_entrypoints:
        expected_pointers = expected_accesses.get(consumer, [])
        actual_pointers = _function_renderer_input_pointers(
            function_nodes[consumer])
        assert all(
            any(
                _expected_pointer_covers_actual(expected, actual)
                for actual in actual_pointers)
            for expected in expected_pointers)
        assert all(
            any(
                _expected_pointer_covers_actual(expected, actual)
                for expected in expected_pointers)
            for actual in actual_pointers)

    assert _module_ast_sha256(renderer_module) == (
        _EXPECTED_MODULE_AST_SHA256['resource_action_renderer.py'])
    provider_module, _ = _ast_functions(Path(provider_artifacts.__file__))
    assert _module_ast_sha256(provider_module) == (
        _EXPECTED_MODULE_AST_SHA256['resource_action_provider_artifacts.py'])


def test_resolve_inventory_and_bindings_are_exact() -> None:
    renderer_input = _renderer_input()
    assert (
        renderer.validate_provider_kubernetes_renderer_input_v1(renderer_input)
        is renderer_input)
    resolved = renderer.resolve_provider_kubernetes_renderer_artifacts_v1(
        renderer_input)
    assert renderer.validate_provider_kubernetes_config_access_inventory_v1(
        resolved) is None

    expected_refs = renderer_input.seed.renderer
    assert resolved.outer_template.artifact_ref == expected_refs.outer_template
    assert resolved.node_fragment.artifact_ref == expected_refs.node_fragment
    assert resolved.binding_schema.artifact_ref == expected_refs.binding_schema
    assert (resolved.config_access_inventory.artifact_ref ==
            expected_refs.config_access_inventory)
    assert (resolved.admitted_object_normalization.artifact_ref ==
            expected_refs.admitted_object_normalization)

    bindings = renderer.resolve_provider_kubernetes_bindings_v1(
        renderer_input, resolved)
    assert tuple(
        binding.name for binding in bindings.bindings) == _BINDING_NAMES
    by_name = {
        binding.name: binding.value.canonical_value()
        for binding in bindings.bindings
    }
    assert by_name['replica_id_text'] == '7'
    assert by_name['original_user'] == 'effective@example.com'
    assert by_name['target_namespace'] == 'serve-canary'
    assert by_name['workload_service_account'] == 'serve-workload'
    assert set(by_name['head_service_selector']) == {
        'component',
        'skypilot-cluster-name',
        'skypilot.co/cluster-record-uuid',
        'skypilot.co/serve-replica-incarnation',
    }


def test_inventory_deduplicates_repeated_nested_code_loads(
        monkeypatch: pytest.MonkeyPatch) -> None:
    resolved = _resolved_artifacts()
    get_instructions = renderer.dis.get_instructions

    def repeated_nested_code_loads(code_object: types.CodeType):
        for instruction in get_instructions(code_object):
            yield instruction
            if (instruction.opname == 'LOAD_CONST' and
                    type(instruction.argval) is types.CodeType):
                yield instruction

    monkeypatch.setattr(renderer.dis, 'get_instructions',
                        repeated_nested_code_loads)
    assert renderer.validate_provider_kubernetes_config_access_inventory_v1(
        resolved) is None


def test_render_normalize_plan_and_assemble_match_frozen_capsule() -> None:
    renderer_input, resolved, validated, normalizations = _validated_pipeline()
    expected_capsule = fixtures._capsule()
    assert tuple(body.role for body in validated) == tuple(
        plan.role for plan in expected_capsule.objects)
    assert tuple(body.body.canonical_bytes for body in validated) == tuple(
        plan.request_body.canonical_bytes for plan in expected_capsule.objects)

    plans = renderer.build_provider_kubernetes_object_plans_v1(
        validated, normalizations, resolved.admitted_object_normalization)
    assert tuple(plan.canonical_bytes for plan in plans) == tuple(
        plan.canonical_bytes for plan in expected_capsule.objects)
    assert all(
        plan.normalization_profile.canonical_bytes ==
        resolved.admitted_object_normalization.artifact_ref.canonical_bytes
        for plan in plans)

    assembled = (
        renderer.
        assemble_and_revalidate_provider_kubernetes_execution_capsule_v1(
            renderer_input, plans))
    assert assembled.canonical_bytes == expected_capsule.canonical_bytes
    constructed = renderer.construct_provider_kubernetes_execution_capsule_v1(
        renderer_input)
    assert constructed.canonical_bytes == expected_capsule.canonical_bytes


def test_input_validator_rejects_self_consistent_wrong_user_projection(
) -> None:
    raw = _renderer_input_raw()
    raw['seed']['request_identity']['cleaned_user'] = 'otheruser'
    for topology_object in raw['seed']['topology']['mutable_objects']:
        for label in topology_object['labels']:
            if label['key'] == 'skypilot-user':
                label['value'] = 'otheruser'
    renderer_input = actions.ProviderKubernetesRendererInputV1.from_value(raw)
    with pytest.raises(ValueError, match='explicit-user projection'):
        renderer.validate_provider_kubernetes_renderer_input_v1(renderer_input)


def test_input_validator_rejects_non_ascii_original_user() -> None:
    raw = _renderer_input_raw()
    raw['seed']['request_identity']['original_user'] = 'éffective@example.com'
    raw['seed']['request_identity']['cleaned_user'] = 'ffectiveexamplecom'
    for topology_object in raw['seed']['topology']['mutable_objects']:
        for label in topology_object['labels']:
            if label['key'] == 'skypilot-user':
                label['value'] = 'ffectiveexamplecom'
    renderer_input = actions.ProviderKubernetesRendererInputV1.from_value(raw)
    with pytest.raises(ValueError, match='must be ASCII'):
        renderer.validate_provider_kubernetes_renderer_input_v1(renderer_input)


def test_body_validator_rejects_contextual_dynamic_drift() -> None:
    renderer_input = _renderer_input()
    resolved = renderer.resolve_provider_kubernetes_renderer_artifacts_v1(
        renderer_input)
    rendered = list(
        renderer.render_provider_kubernetes_objects_v1(renderer_input,
                                                       resolved))
    pod = rendered[2].canonical_value()
    pod['metadata']['annotations']['skypilot-user'] = 'another@example.com'
    rendered[2] = actions.CanonicalJsonObject.from_value(pod)
    with pytest.raises(ValueError, match='annotation does not match'):
        renderer.validate_kubernetes_serve_three_object_body_v1(
            renderer_input, tuple(rendered))


def test_contextual_body_validation_selects_replica_environment_by_name(
) -> None:
    source = inspect.getsource(
        renderer.validate_kubernetes_serve_three_object_body_v1)
    assert "container['env'][0]" not in source
    assert "entry['name'] == 'SKYPILOT_SERVE_REPLICA_ID'" in source


def test_plan_builder_rejects_crossed_normalization_semantics() -> None:
    _, resolved, validated, normalizations = _validated_pipeline()
    crossed = list(normalizations)
    crossed[0] = dataclasses.replace(crossed[0],
                                     requested_semantic=validated[1].body)
    with pytest.raises(ValueError, match='semantic does not match'):
        renderer.build_provider_kubernetes_object_plans_v1(
            validated, tuple(crossed), resolved.admitted_object_normalization)


def test_resolver_rejects_role_path_drift() -> None:
    raw = _renderer_input_raw()
    raw['seed']['renderer']['outer_template']['repo_path'] = (
        'sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
        'renamed_outer_template.json')
    renderer_input = actions.ProviderKubernetesRendererInputV1.from_value(raw)
    with pytest.raises(ValueError, match='path is not exact'):
        renderer.resolve_provider_kubernetes_renderer_artifacts_v1(
            renderer_input)


def test_resolver_reads_from_imported_distribution_and_rejects_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_sky, fake_artifact_root = _fake_distribution(tmp_path)
    monkeypatch.setattr(renderer.sky_package, '__file__',
                        str(fake_sky / '__init__.py'))

    renderer_input = _renderer_input()
    renderer.resolve_provider_kubernetes_renderer_artifacts_v1(renderer_input)

    victim = fake_artifact_root / 'outer_template.json'
    original = tmp_path / 'outer_template.json'
    original.write_bytes(victim.read_bytes())
    victim.unlink()
    victim.symlink_to(original)
    with pytest.raises(ValueError,
                       match='descriptor-safe renderer artifact resolution'):
        renderer.resolve_provider_kubernetes_renderer_artifacts_v1(
            renderer_input)


@pytest.mark.parametrize('mutation', ('missing', 'symlink'))
def test_resolver_rejects_nonregular_imported_package_initializer(
    mutation: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_sky, _ = _fake_distribution(tmp_path)
    package_init = fake_sky / '__init__.py'
    package_init.unlink()
    if mutation == 'symlink':
        external_init = tmp_path / 'external_init.py'
        external_init.write_text('', encoding='utf-8')
        package_init.symlink_to(external_init)
    monkeypatch.setattr(renderer.sky_package, '__file__', str(package_init))
    with pytest.raises(ValueError,
                       match='descriptor-safe renderer artifact resolution'):
        renderer.resolve_provider_kubernetes_renderer_artifacts_v1(
            _renderer_input())


def test_resolver_rejects_fifo_package_initializer_without_blocking(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_sky, _ = _fake_distribution(tmp_path)
    package_init = fake_sky / '__init__.py'
    package_init.unlink()
    os.mkfifo(package_init)
    monkeypatch.setattr(renderer.sky_package, '__file__', str(package_init))

    _assert_resolver_fails_without_blocking(_renderer_input())


def test_resolver_rejects_fifo_artifact_without_blocking(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_sky, fake_artifact_root = _fake_distribution(tmp_path)
    victim = fake_artifact_root / 'outer_template.json'
    victim.unlink()
    os.mkfifo(victim)
    monkeypatch.setattr(renderer.sky_package, '__file__',
                        str(fake_sky / '__init__.py'))

    _assert_resolver_fails_without_blocking(_renderer_input())


def test_inventory_verifier_rejects_undeclared_project_call(
        monkeypatch: pytest.MonkeyPatch) -> None:
    resolved_artifacts = _resolved_artifacts()
    original = renderer.render_provider_kubernetes_objects_v1

    def call_drift(renderer_input: Any, resolved: Any) -> Any:
        bindings = resolve_provider_kubernetes_bindings_v1(
            renderer_input, resolved)
        actions.project_provider_kubernetes_request_identity_v1(
            renderer_input.seed.request_identity.original_user,
            renderer_input.name_basis)
        return bindings

    call_drift.__globals__['resolve_provider_kubernetes_bindings_v1'] = (
        renderer.resolve_provider_kubernetes_bindings_v1)
    call_drift.__name__ = original.__name__
    call_drift.__module__ = renderer.__name__
    monkeypatch.setattr(renderer, original.__name__, call_drift)
    with pytest.raises(ValueError, match='undeclared project callee'):
        renderer.validate_provider_kubernetes_config_access_inventory_v1(
            resolved_artifacts)


def test_inventory_verifier_rejects_undeclared_input_access(
        monkeypatch: pytest.MonkeyPatch) -> None:
    resolved_artifacts = _resolved_artifacts()
    original = renderer.validate_provider_kubernetes_renderer_input_v1

    def access_drift(renderer_input: Any) -> Any:
        _ = renderer_input.seed.objects
        return renderer_input

    access_drift.__name__ = original.__name__
    access_drift.__module__ = renderer.__name__
    monkeypatch.setattr(renderer, original.__name__, access_drift)
    with pytest.raises(ValueError, match='access shape has drifted'):
        renderer.validate_provider_kubernetes_config_access_inventory_v1(
            resolved_artifacts)


@pytest.mark.parametrize(
    ('insertion', 'expected_pointer', 'error_match'),
    (("    _ = getattr(renderer_input, 'resource_identity')\n",
      ('resource_identity',), 'dynamic attribute access'),
     ("    if renderer_input.seed is None:\n"
      "        raise ValueError('drift')\n",
      ('seed',), 'executable access shape has drifted'),
     ("    _ = renderer_input.seed.renderer\n",
      ('seed', 'renderer'), 'executable access shape has drifted')))
def test_inventory_verifier_rejects_hidden_renderer_input_access(
    insertion: str,
    expected_pointer: tuple[str, ...],
    error_match: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolved_artifacts = _resolved_artifacts()
    original = renderer.resolve_provider_kubernetes_renderer_artifacts_v1
    drifted = _function_with_insertion(original, insertion)
    drifted_source = ast.parse(
        inspect.getsource(original).replace(
            '    renderer = renderer_input.seed.renderer\n',
            insertion + '    renderer = renderer_input.seed.renderer\n'))
    drifted_function = drifted_source.body[0]
    assert type(drifted_function) is ast.FunctionDef
    assert expected_pointer in _function_renderer_input_pointers(
        drifted_function)
    monkeypatch.setattr(renderer, original.__name__, drifted)
    with pytest.raises(ValueError, match=error_match):
        renderer.validate_provider_kubernetes_config_access_inventory_v1(
            resolved_artifacts)


def test_inventory_verifier_rejects_same_name_ambient_file_read(
        monkeypatch: pytest.MonkeyPatch) -> None:
    resolved_artifacts = _resolved_artifacts()
    original = renderer.resolve_provider_kubernetes_renderer_artifacts_v1
    drifted = _function_with_insertion(
        original, "    ambient_fd = os.open('/etc/passwd', read_flags)\n"
        "    os.read(ambient_fd, 1)\n"
        "    os.close(ambient_fd)\n")
    monkeypatch.setattr(renderer, original.__name__, drifted)
    with pytest.raises(ValueError, match='executable access shape has drifted'):
        renderer.validate_provider_kubernetes_config_access_inventory_v1(
            resolved_artifacts)


def test_executable_fingerprint_table_is_deeply_immutable() -> None:
    fingerprint_table = (
        renderer._EXPECTED_INVENTORIED_EXECUTABLE_SHA256_BY_PYTHON)
    assert type(fingerprint_table) is tuple
    assert all(
        type(row) is tuple and type(row[0]) is tuple and type(row[1]) is str
        for row in fingerprint_table)
    with pytest.raises(TypeError):
        operator.setitem(fingerprint_table, 0, fingerprint_table[0])
    with pytest.raises(TypeError):
        operator.setitem(fingerprint_table[0], 1, fingerprint_table[0][1])


def test_module_ast_seal_rejects_top_level_ambient_source() -> None:
    renderer_path = Path(renderer.__file__)
    source = renderer_path.read_text(encoding='utf-8')
    drifted_module = ast.parse(source +
                               "\n_ambient = os.getenv('SKYPILOT_CONFIG')\n",
                               filename=str(renderer_path))
    assert _module_ast_sha256(drifted_module) != (
        _EXPECTED_MODULE_AST_SHA256['resource_action_renderer.py'])


def test_inventory_verifier_rejects_forbidden_environment_source(
        monkeypatch: pytest.MonkeyPatch) -> None:
    resolved_artifacts = _resolved_artifacts()
    original = renderer.resolve_provider_kubernetes_bindings_v1

    def environment_drift(renderer_input: Any, resolved: Any) -> Any:
        del renderer_input, resolved
        return os.getenv('SKYPILOT_CONFIG')

    environment_drift.__name__ = original.__name__
    environment_drift.__module__ = renderer.__name__
    monkeypatch.setattr(renderer, original.__name__, environment_drift)
    with pytest.raises(ValueError, match='forbidden module source os'):
        renderer.validate_provider_kubernetes_config_access_inventory_v1(
            resolved_artifacts)


def test_inventory_verifier_rejects_executable_import(
        monkeypatch: pytest.MonkeyPatch) -> None:
    resolved_artifacts = _resolved_artifacts()
    original = renderer.assemble_and_revalidate_provider_kubernetes_execution_capsule_v1

    def import_drift(renderer_input: Any, plans: Any) -> Any:
        import subprocess
        del renderer_input, plans
        return subprocess

    import_drift.__name__ = original.__name__
    import_drift.__module__ = renderer.__name__
    monkeypatch.setattr(renderer, original.__name__, import_drift)
    with pytest.raises(ValueError, match='executable import'):
        renderer.validate_provider_kubernetes_config_access_inventory_v1(
            resolved_artifacts)


def test_renderer_does_not_mutate_its_input() -> None:
    renderer_input = _renderer_input()
    before = copy.deepcopy(renderer_input.canonical_value())
    renderer.construct_provider_kubernetes_execution_capsule_v1(renderer_input)
    assert renderer_input.canonical_value() == before
