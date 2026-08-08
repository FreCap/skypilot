"""Closed six-role and callable evidence for provider authority V2."""

# pylint: disable=protected-access

import ast
import copy
import dataclasses
import hashlib
import inspect
import json
import os
import pathlib
import shutil
import sys
import types

import pytest

from sky.serve import resource_action_provider_inventory_v2 as inventory_v2
from sky.serve import resource_actions as actions
from sky.server.requests import registry as request_registry

_ROOT = pathlib.Path(
    'sky/serve/resource_action_artifacts/provider_authority_v2')
_ARTIFACT_INVENTORY = _ROOT / 'artifact_inventory.json'
_CALLABLE_INVENTORY = _ROOT / 'callable_inventory.json'


def _canonical_file(value: object) -> bytes:
    return actions.canonical_json_bytes(value) + b'\n'


def _reference(path: pathlib.Path) -> actions.ProviderRepoArtifactRefV1:
    contents = path.read_bytes()
    return actions.ProviderRepoArtifactRefV1(
        repo_path=path.as_posix(),
        byte_size=len(contents),
        sha256=hashlib.sha256(contents).hexdigest())


def _copy_role_graph(destination: pathlib.Path) -> dict:
    value = json.loads(_ARTIFACT_INVENTORY.read_bytes())
    for row in value['artifacts']:
        target = destination / row['repo_path']
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(row['repo_path'], target)
        if row['role'] == 'representability_case_inventory':
            index = json.loads(pathlib.Path(row['repo_path']).read_bytes())
            for descriptor in index['shards']:
                shard_path = descriptor['artifact']['repo_path']
                shard_target = destination / shard_path
                shard_target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(shard_path, shard_target)
    return value


def test_checked_in_v2_inventories_match_installed_bytes_and_callables(
) -> None:
    artifact_contents = _ARTIFACT_INVENTORY.read_bytes()
    callable_contents = _CALLABLE_INVENTORY.read_bytes()
    references = inventory_v2.validate_provider_authority_artifact_inventory_v2(
        artifact_contents)
    inventory_v2.validate_provider_authority_callable_inventory_v2(
        callable_contents)
    assert len(references) == 6
    assert (inventory_v2.project_provider_authority_artifact_inventory_v2().
            canonical_bytes == artifact_contents[:-1])
    assert (inventory_v2.project_provider_authority_callable_inventory_v2().
            canonical_bytes == callable_contents[:-1])
    installed_references = (
        inventory_v2.validate_installed_provider_authority_inventories_v2(
            _reference(_ARTIFACT_INVENTORY), _reference(_CALLABLE_INVENTORY)))
    assert installed_references == references


@pytest.mark.parametrize('artifact_path,callable_path', [
    (_CALLABLE_INVENTORY, _CALLABLE_INVENTORY),
    (_ARTIFACT_INVENTORY, _ARTIFACT_INVENTORY),
])
def test_installed_top_level_inventory_paths_are_role_exact(
        artifact_path: pathlib.Path, callable_path: pathlib.Path) -> None:
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error,
                       match='path'):
        inventory_v2.validate_installed_provider_authority_inventories_v2(
            _reference(artifact_path), _reference(callable_path))


def test_v2_inventory_roles_paths_and_pure_roots_are_exact() -> None:
    artifacts = json.loads(_ARTIFACT_INVENTORY.read_bytes())['artifacts']
    assert [(row['role'], row['repo_path']) for row in artifacts
           ] == list(inventory_v2._ARTIFACT_ROLE_PATHS)
    callables = json.loads(_CALLABLE_INVENTORY.read_bytes())
    assert [row['name'] for row in callables['handlers']
           ] == list(actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1)
    assert callables['pure_entrypoints'] == [{
        'role': 'launch_capsule_constructor',
        'module': 'sky.serve.resource_action_renderer_v2',
        'qualname': 'construct_provider_kubernetes_execution_capsule_v2',
    }, {
        'role': 'down_capsule_constructor',
        'module': 'sky.serve.resource_action_renderer_v2',
        'qualname': 'construct_provider_kubernetes_down_execution_capsule_v2',
    }, {
        'role': 'cleanup_target_rederiver',
        'module': 'sky.serve.resource_action_cleanup_v2',
        'qualname': 'rederive_provider_kubernetes_cleanup_target_v2',
    }, {
        'role': 'representability_enumerator',
        'module': 'sky.serve.resource_action_representability',
        'qualname': 'enumerate_provider_resource_action_representability_v2',
    }]


@pytest.mark.parametrize('mutation', [
    lambda value: value.update({'version': 1}),
    lambda value: value.update({'contract': 'crossed'}),
    lambda value: value.update({'extra': None}),
    lambda value: value['artifacts'].pop(),
    lambda value: value['artifacts'].append(copy.deepcopy(value['artifacts'][0])
                                           ),
    lambda value: value['artifacts'].__setitem__(
        slice(0, 2), list(reversed(value['artifacts'][:2]))),
    lambda value: value['artifacts'][0].update({'role': 'node_fragment'}),
    lambda value: value['artifacts'][0].update(
        {'repo_path': value['artifacts'][1]['repo_path']}),
    lambda value: value['artifacts'][0].update({'byte_size': 1}),
    lambda value: value['artifacts'][0].update({'sha256': '0' * 64}),
    lambda value: value['artifacts'][0].update({'extra': None}),
])
def test_artifact_inventory_rejects_every_closed_binding_mutation(
        mutation) -> None:
    value = json.loads(_ARTIFACT_INVENTORY.read_bytes())
    mutation(value)
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error):
        inventory_v2.validate_provider_authority_artifact_inventory_v2(
            _canonical_file(value))


@pytest.mark.parametrize('row_index', range(6))
@pytest.mark.parametrize('field', ['role', 'repo_path', 'byte_size', 'sha256'])
def test_each_artifact_role_rejects_each_bound_field(row_index: int,
                                                     field: str) -> None:
    value = json.loads(_ARTIFACT_INVENTORY.read_bytes())
    row = value['artifacts'][row_index]
    mutations = {
        'role': 'crossed_role',
        'repo_path': 'sky/serve/resource_action_artifacts/crossed.json',
        'byte_size': row['byte_size'] + 1,
        'sha256': '0' * 64,
    }
    row[field] = mutations[field]
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error):
        inventory_v2.validate_provider_authority_artifact_inventory_v2(
            _canonical_file(value))


@pytest.mark.parametrize('variant', [
    lambda body: body[:-1],
    lambda body: body + b'\n',
    lambda body: body[:-1] + b'\r\n',
    lambda body: json.dumps(json.loads(body), indent=2).encode() + b'\n',
    lambda body: body.replace(b'{', b'{"version":2,', 1),
])
@pytest.mark.parametrize('path', [_ARTIFACT_INVENTORY, _CALLABLE_INVENTORY])
def test_v2_inventories_reject_noncanonical_or_duplicate_bytes(
        path: pathlib.Path, variant) -> None:
    validator = (inventory_v2.validate_provider_authority_artifact_inventory_v2
                 if path == _ARTIFACT_INVENTORY else
                 inventory_v2.validate_provider_authority_callable_inventory_v2)
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error):
        validator(variant(path.read_bytes()))


@pytest.mark.parametrize('mutation', [
    lambda value: value.update({'version': 1}),
    lambda value: value.update({'contract': 'crossed'}),
    lambda value: value.update({'extra': None}),
    lambda value: value['handlers'].pop(),
    lambda value: value['handlers'].append(copy.deepcopy(value['handlers'][0])),
    lambda value: value['handlers'].__setitem__(
        slice(0, 2), list(reversed(value['handlers'][:2]))),
    lambda value: value['handlers'][0].update({'module': 'crossed.module'}),
    lambda value: value['handlers'][0].update({'extra': None}),
    lambda value: value['handlers'][0]['result_codec'].update({'extra': None}),
    lambda value: value['handlers'][0]['result_codec']['encoder'].update(
        {'extra': None}),
    lambda value: value['pure_entrypoints'].pop(),
    lambda value: value['pure_entrypoints'].append(
        copy.deepcopy(value['pure_entrypoints'][0])),
    lambda value: value['pure_entrypoints'].__setitem__(
        slice(0, 2), list(reversed(value['pure_entrypoints'][:2]))),
    lambda value: value['pure_entrypoints'][0].update({'role': 'crossed'}),
    lambda value: value['pure_entrypoints'][0].update({'module': 'crossed'}),
    lambda value: value['pure_entrypoints'][0].update({'qualname': 'crossed'}),
    lambda value: value['pure_entrypoints'][0].update({'extra': None}),
])
def test_callable_inventory_rejects_every_closed_shape_and_identity_mutation(
        mutation) -> None:
    value = json.loads(_CALLABLE_INVENTORY.read_bytes())
    mutation(value)
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error):
        inventory_v2.validate_provider_authority_callable_inventory_v2(
            _canonical_file(value))


@pytest.mark.parametrize('handler_index', range(4))
@pytest.mark.parametrize('field', [
    'name', 'module', 'qualname', 'execution_class', 'claim_scope',
    'replay_policy', 'cancellation_policy', 'aliases', 'encoder.mode',
    'encoder.module', 'encoder.qualname', 'decoder.mode', 'decoder.module',
    'decoder.qualname', 'strict_return_value'
])
def test_callable_inventory_rejects_every_handler_and_codec_field(
        handler_index: int, field: str) -> None:
    value = json.loads(_CALLABLE_INVENTORY.read_bytes())
    row = value['handlers'][handler_index]
    if field == 'aliases':
        row[field] = ['old-name']
    elif field == 'strict_return_value':
        row['result_codec'][field] = not row['result_codec'][field]
    elif '.' in field:
        codec_name, key = field.split('.')
        row['result_codec'][codec_name][key] = 'crossed'
    else:
        row[field] = 'crossed'
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error):
        inventory_v2.validate_provider_authority_callable_inventory_v2(
            _canonical_file(value))


@pytest.mark.parametrize('root_index', range(4))
@pytest.mark.parametrize('field', ['role', 'module', 'qualname'])
def test_each_pure_root_rejects_each_bound_field(root_index: int,
                                                 field: str) -> None:
    value = json.loads(_CALLABLE_INVENTORY.read_bytes())
    value['pure_entrypoints'][root_index][field] = 'crossed'
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error):
        inventory_v2.validate_provider_authority_callable_inventory_v2(
            _canonical_file(value))


def test_callable_inventory_detects_registry_and_pure_root_drift(
        monkeypatch: pytest.MonkeyPatch) -> None:
    registrations = request_registry.registered_handlers()
    authority_registration = next(
        registration for registration in registrations if registration.name ==
        actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1[0])
    extra = dataclasses.replace(authority_registration,
                                name='serve_resource_action_extra')
    monkeypatch.setattr(request_registry, 'registered_handlers', lambda:
                        (*registrations, extra))
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error,
                       match='closed four-name'):
        inventory_v2.project_provider_authority_callable_inventory_v2()
    monkeypatch.undo()

    roots = inventory_v2._PURE_ENTRYPOINTS
    monkeypatch.setattr(inventory_v2, '_PURE_ENTRYPOINTS',
                        (*roots[:-1],
                         (roots[-1][0], roots[0][1], roots[0][2], roots[0][3])))
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error,
                       match='differs from actual callables'):
        inventory_v2.validate_provider_authority_callable_inventory_v2(
            _CALLABLE_INVENTORY.read_bytes())


def test_changed_leaf_and_crossed_v1_renderer_evidence_fail_closed(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _copy_role_graph(tmp_path)
    changed = tmp_path / value['artifacts'][0]['repo_path']
    contents = bytearray(changed.read_bytes())
    contents[0] ^= 1
    changed.write_bytes(contents)
    monkeypatch.setattr(inventory_v2, '_distribution_root',
                        lambda: str(tmp_path))
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error,
                       match='content address'):
        inventory_v2.validate_provider_authority_artifact_inventory_v2(
            _ARTIFACT_INVENTORY.read_bytes())

    value = _copy_role_graph(tmp_path)
    for role, v1_name in (('binding_schema', 'binding_schema.json'),
                          ('config_access_inventory',
                           'config_access_inventory.json')):
        row = next(item for item in value['artifacts'] if item['role'] == role)
        crossed = pathlib.Path(
            'sky/serve/resource_action_artifacts/kubernetes_renderer_v1'
        ) / v1_name
        target = tmp_path / row['repo_path']
        target.write_bytes(crossed.read_bytes())
        row['byte_size'] = target.stat().st_size
        row['sha256'] = hashlib.sha256(target.read_bytes()).hexdigest()
        with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error,
                           match='schema|contract'):
            inventory_v2.validate_provider_authority_artifact_inventory_v2(
                _canonical_file(value))
        value = _copy_role_graph(tmp_path)


def test_case_artifact_and_code_dispatch_cannot_drift_together(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _copy_role_graph(tmp_path)
    row = next(item for item in value['artifacts']
               if item['role'] == 'representability_case_inventory')
    target = tmp_path / row['repo_path']
    case_index = json.loads(target.read_bytes())
    descriptor = case_index['shards'][0]
    shard_target = tmp_path / descriptor['artifact']['repo_path']
    case_shard = json.loads(shard_target.read_bytes())
    case_shard['cases'][0]['case_id'] = 'crossed.case'
    shard_contents = _canonical_file(case_shard)
    shard_target.write_bytes(shard_contents)
    descriptor['artifact']['byte_size'] = len(shard_contents)
    descriptor['artifact']['sha256'] = hashlib.sha256(
        shard_contents).hexdigest()
    index_contents = _canonical_file(case_index)
    target.write_bytes(index_contents)
    row['byte_size'] = len(index_contents)
    row['sha256'] = hashlib.sha256(index_contents).hexdigest()
    monkeypatch.setattr(inventory_v2, '_distribution_root',
                        lambda: str(tmp_path))
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error,
                       match='differs from code dispatch'):
        inventory_v2.validate_provider_authority_artifact_inventory_v2(
            _canonical_file(value))


def test_descriptor_reader_rejects_symlink_component_and_fifo_leaf(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    symlink_root = tmp_path / 'symlink-root'
    value = _copy_role_graph(symlink_root)
    first_path = pathlib.PurePosixPath(value['artifacts'][0]['repo_path'])
    real_sky = symlink_root / 'real-sky'
    (symlink_root / 'sky').rename(real_sky)
    (symlink_root / 'sky').symlink_to(real_sky, target_is_directory=True)
    monkeypatch.setattr(inventory_v2, '_distribution_root',
                        lambda: str(symlink_root))
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error,
                       match='resolved safely'):
        inventory_v2.validate_provider_authority_artifact_inventory_v2(
            _ARTIFACT_INVENTORY.read_bytes())

    monkeypatch.undo()
    fifo_root = tmp_path / 'fifo-root'
    value = _copy_role_graph(fifo_root)
    fifo = fifo_root / first_path
    fifo.unlink()
    os.mkfifo(fifo)
    monkeypatch.setattr(inventory_v2, '_distribution_root',
                        lambda: str(fifo_root))
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error,
                       match='bounded regular file'):
        inventory_v2.validate_provider_authority_artifact_inventory_v2(
            _ARTIFACT_INVENTORY.read_bytes())


def test_artifact_graph_opens_the_fixed_distribution_root_once(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _copy_role_graph(tmp_path)
    monkeypatch.setattr(inventory_v2, '_distribution_root',
                        lambda: str(tmp_path))
    original_open = os.open
    root_opens = 0

    def _counted_open(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal root_opens
        if dir_fd is None and path == str(tmp_path):
            root_opens += 1
        if dir_fd is None:
            return original_open(path, flags, mode)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, 'open', _counted_open)
    inventory_v2.validate_provider_authority_artifact_inventory_v2(
        _ARTIFACT_INVENTORY.read_bytes())
    assert root_opens == 1


def test_callable_identity_never_imports_or_invokes_module_descriptors(
        monkeypatch: pytest.MonkeyPatch) -> None:
    descriptor_accesses: list[str] = []

    class DescriptorTrapModule(types.ModuleType):

        def __getattribute__(self, name: str):
            descriptor_accesses.append(name)
            return super().__getattribute__(name)

    trap = DescriptorTrapModule('authority_descriptor_trap')

    def trapped() -> None:
        raise AssertionError('callable identity must never invoke the root')

    trapped.__module__ = 'authority_descriptor_trap'
    trapped.__qualname__ = 'trapped'
    monkeypatch.setitem(sys.modules, 'authority_descriptor_trap', trap)
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error,
                       match='already loaded and exact'):
        inventory_v2._importable_callable_identity(trapped)
    assert not descriptor_accesses

    registration_accesses: list[str] = []

    class RegistrationTrap:

        def __getattribute__(self, name: str):
            registration_accesses.append(name)
            raise AssertionError('open registry rows must not be inspected')

    monkeypatch.setattr(request_registry, 'registered_handlers', lambda:
                        (RegistrationTrap(),))
    with pytest.raises(inventory_v2.ProviderAuthorityInventoryV2Error,
                       match='open row type'):
        inventory_v2.project_provider_authority_callable_inventory_v2()
    assert not registration_accesses


def test_inventory_module_has_no_v1_top_level_construction_authority() -> None:
    source = inspect.getsource(inventory_v2)
    tree = ast.parse(source)
    forbidden = {
        'construct_provider_kubernetes_execution_capsule_v1',
        'assemble_and_revalidate_provider_kubernetes_execution_capsule_v1',
        'ProviderKubernetesExecutionCapsuleSeedV1',
        'ProviderKubernetesRendererInputV1',
        'ProviderKubernetesExecutionCapsuleV1',
        'ProviderKubernetesDownExecutionCapsuleV1',
    }
    identifiers = {
        node.id for node in ast.walk(tree) if isinstance(node, ast.Name)
    } | {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }
    assert forbidden.isdisjoint(identifiers)
    assert not any(
        isinstance(node, ast.Name) and node.id == 'importlib'
        for node in ast.walk(tree))
    assert not any(
        isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and
        node.func.id == 'getattr' for node in ast.walk(tree))
    callable_bytes = _CALLABLE_INVENTORY.read_bytes()
    assert b'resource_action_renderer.construct_' not in callable_bytes
    assert b'_capsule_v1' not in callable_bytes
