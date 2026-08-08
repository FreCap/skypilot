"""Pure installed-inventory evidence for provider authority V2.

The cohort-bound artifact inventory names exactly six immutable JSON files.
The callable inventory independently binds the four private request handlers,
their strict result codecs, and the four pure construction roots.  This module
projects those facts from installed bytes and actual importable callables; it
does not construct a capsule, evaluate preflight, access a database, or invoke
provider I/O.
"""

from __future__ import annotations

from collections.abc import Iterator
import contextlib
import hashlib
import json
import os
import stat
import sys
import types
from typing import Any

import sky as sky_package
from sky.serve import resource_action_cleanup_v2 as cleanup_v2
from sky.serve import resource_action_renderer_v2 as renderer_v2
from sky.serve import resource_action_representability as representability_v2
from sky.serve import resource_actions as actions
from sky.server import constants as server_constants
from sky.server.requests import registry as request_registry
from sky.server.requests.serializers import decoders
from sky.server.requests.serializers import encoders

_ARTIFACT_INVENTORY_CONTRACT = 'provider_authority_artifact_inventory_v2'
_CALLABLE_INVENTORY_CONTRACT = 'provider_authority_callable_inventory_v2'
_ARTIFACT_INVENTORY_REPO_PATH = (
    'sky/serve/resource_action_artifacts/provider_authority_v2/'
    'artifact_inventory.json')
_CALLABLE_INVENTORY_REPO_PATH = (
    'sky/serve/resource_action_artifacts/provider_authority_v2/'
    'callable_inventory.json')


def _literal_module_function(module: types.ModuleType, *, module_name: str,
                             qualname: str) -> types.FunctionType:
    """Resolve one fixed top-level function without invoking descriptors."""

    if type(module) is not types.ModuleType:
        raise RuntimeError('Authority inventory module is not exact.')
    namespace = vars(module)
    if namespace.get('__name__') != module_name or '.' in qualname:
        raise RuntimeError('Authority inventory module identity is invalid.')
    function = namespace.get(qualname)
    if (type(function) is not types.FunctionType or
            function.__module__ != module_name or
            function.__qualname__ != qualname):
        raise RuntimeError('Authority inventory function identity is invalid.')
    return function


_ARTIFACT_ROLE_PATHS: tuple[tuple[str, str], ...] = (
    ('outer_template',
     'sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
     'outer_template.json'),
    ('node_fragment',
     'sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
     'node_fragment.json'),
    ('binding_schema',
     'sky/serve/resource_action_artifacts/kubernetes_renderer_v2/'
     'binding_schema.json'),
    ('config_access_inventory',
     'sky/serve/resource_action_artifacts/kubernetes_renderer_v2/'
     'config_access_inventory.json'),
    ('admitted_object_normalization',
     'sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
     'admitted_object_normalization.json'),
    ('representability_case_inventory',
     'sky/serve/resource_action_artifacts/provider_authority_v2/'
     'representability_case_inventory.json'),
)
_PURE_ENTRYPOINTS: tuple[tuple[str, str, str, types.FunctionType], ...] = (
    ('launch_capsule_constructor', 'sky.serve.resource_action_renderer_v2',
     'construct_provider_kubernetes_execution_capsule_v2',
     _literal_module_function(
         renderer_v2,
         module_name='sky.serve.resource_action_renderer_v2',
         qualname='construct_provider_kubernetes_execution_capsule_v2')),
    ('down_capsule_constructor', 'sky.serve.resource_action_renderer_v2',
     'construct_provider_kubernetes_down_execution_capsule_v2',
     _literal_module_function(
         renderer_v2,
         module_name='sky.serve.resource_action_renderer_v2',
         qualname='construct_provider_kubernetes_down_execution_capsule_v2')),
    ('cleanup_target_rederiver', 'sky.serve.resource_action_cleanup_v2',
     'rederive_provider_kubernetes_cleanup_target_v2',
     _literal_module_function(
         cleanup_v2,
         module_name='sky.serve.resource_action_cleanup_v2',
         qualname='rederive_provider_kubernetes_cleanup_target_v2')),
    ('representability_enumerator',
     'sky.serve.resource_action_representability',
     'enumerate_provider_resource_action_representability_v2',
     _literal_module_function(
         representability_v2,
         module_name='sky.serve.resource_action_representability',
         qualname='enumerate_provider_resource_action_representability_v2')),
)
if type(request_registry) is not types.ModuleType:
    raise RuntimeError('Authority request registry module is not exact.')
_HANDLER_REGISTRATION_TYPE = vars(request_registry).get('HandlerRegistration')
if type(_HANDLER_REGISTRATION_TYPE) is not type:
    raise RuntimeError('Authority handler-registration type is not exact.')
_MAX_STATIC_JSON_BYTES = 65_536
_READ_CHUNK_BYTES = 64 * 1024
_INVENTORY_ARTIFACT_KEYS = frozenset(
    {'role', 'repo_path', 'byte_size', 'sha256'})
_CALLABLE_HANDLER_KEYS = frozenset({
    'name', 'module', 'qualname', 'execution_class', 'claim_scope',
    'replay_policy', 'cancellation_policy', 'aliases', 'result_codec'
})
_RESULT_CODEC_KEYS = frozenset({'encoder', 'decoder', 'strict_return_value'})
_CODEC_IDENTITY_KEYS = frozenset({'mode', 'module', 'qualname'})
_PURE_ENTRYPOINT_KEYS = frozenset({'role', 'module', 'qualname'})


class ProviderAuthorityInventoryV2Error(ValueError):
    """Installed V2 inventory evidence failed closed validation."""


def _validate_literal_representability_dispatch() -> None:
    validator = _literal_module_function(
        representability_v2,
        module_name='sky.serve.resource_action_representability',
        qualname=
        '_validate_provider_resource_action_representability_dispatch_v2')
    try:
        validator()
    except (TypeError, ValueError) as error:
        raise ProviderAuthorityInventoryV2Error(
            'Representability dispatch is not literal and closed.') from error


def _closed_dictionary(value: Any, *, name: str,
                       keys: frozenset[str]) -> dict[str, Any]:
    if type(value) is not dict:
        raise ProviderAuthorityInventoryV2Error(
            f'{name} must be an exact JSON object.')
    if any(type(key) is not str for key in value) or set(value) != keys:
        raise ProviderAuthorityInventoryV2Error(
            f'{name} has unknown or missing fields.')
    return value


def _json_without_duplicates(contents: bytes, *, name: str) -> Any:

    def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ProviderAuthorityInventoryV2Error(
                    f'{name} contains a duplicate JSON key.')
            result[key] = value
        return result

    def _forbid_float(_: str) -> Any:
        raise ProviderAuthorityInventoryV2Error(
            f'{name} contains a floating-point value.')

    try:
        value = json.loads(contents.decode('utf-8'),
                           object_pairs_hook=_object,
                           parse_float=_forbid_float,
                           parse_constant=_forbid_float)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderAuthorityInventoryV2Error(
            f'{name} is not valid canonical UTF-8 JSON.') from error
    try:
        canonical = actions.canonical_json_bytes(value)
    except (TypeError, ValueError) as error:
        raise ProviderAuthorityInventoryV2Error(
            f'{name} is not bounded canonical JSON.') from error
    if canonical != contents:
        raise ProviderAuthorityInventoryV2Error(
            f'{name} bytes are not canonical JSON.')
    return value


def _parse_canonical_json_file(contents: bytes, *, name: str) -> Any:
    if type(contents) is not bytes:
        raise TypeError(f'{name} contents must be bytes.')
    if (not contents.endswith(b'\n') or contents.endswith(b'\n\n') or
            b'\r' in contents or len(contents) > _MAX_STATIC_JSON_BYTES):
        raise ProviderAuthorityInventoryV2Error(
            f'{name} must be bounded canonical JSON with exactly one final LF.')
    return _json_without_duplicates(contents[:-1], name=name)


def _distribution_root() -> str:
    if type(sky_package) is not types.ModuleType:
        raise ProviderAuthorityInventoryV2Error(
            'The imported SkyPilot package module is not exact.')
    package_init = vars(sky_package).get('__file__')
    if (type(package_init) is not str or not os.path.isabs(package_init) or
            os.path.basename(package_init) != '__init__.py'):
        raise ProviderAuthorityInventoryV2Error(
            'The imported SkyPilot package location is not fixed.')
    package_directory = os.path.dirname(package_init)
    if os.path.basename(package_directory) != 'sky':
        raise ProviderAuthorityInventoryV2Error(
            'The imported SkyPilot package is not top-level.')
    return os.path.dirname(package_directory)


@contextlib.contextmanager
def _open_distribution_root_descriptor() -> Iterator[int]:
    """Open the fixed package root once for one artifact-graph traversal."""

    required_flags = ('O_CLOEXEC', 'O_DIRECTORY', 'O_NOFOLLOW', 'O_NONBLOCK')
    if any(not hasattr(os, flag) for flag in required_flags):
        raise ProviderAuthorityInventoryV2Error(
            'Descriptor-safe installed artifact resolution is unsupported.')
    directory_flags = (os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY |
                       os.O_NOFOLLOW)
    try:
        root_descriptor = os.open(_distribution_root(), directory_flags)
    except OSError as error:
        raise ProviderAuthorityInventoryV2Error(
            'Installed distribution root cannot be opened safely.') from error
    try:
        yield root_descriptor
    finally:
        os.close(root_descriptor)


def _read_installed_repo_path_from_root(root_descriptor: int, repo_path: str, *,
                                        maximum_bytes: int) -> bytes:
    # Parsing through the shared reference contract also proves a relative,
    # normalized POSIX path before any component is opened.
    try:
        sentinel = actions.ProviderRepoArtifactRefV1(repo_path=repo_path,
                                                     byte_size=1,
                                                     sha256='0' * 64)
    except (TypeError, ValueError) as error:
        raise ProviderAuthorityInventoryV2Error(
            'Installed artifact repository path is invalid.') from error
    if sentinel.repo_path != repo_path:
        raise ProviderAuthorityInventoryV2Error(
            'Installed artifact repository path is not canonical.')
    if (type(maximum_bytes) is not int or maximum_bytes < 1 or
            maximum_bytes > _MAX_STATIC_JSON_BYTES):
        raise ValueError('Installed artifact byte limit is invalid.')
    if type(root_descriptor) is not int or root_descriptor < 0:
        raise TypeError('Installed distribution root descriptor is invalid.')
    directory_flags = (os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY |
                       os.O_NOFOLLOW)
    read_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
    current_descriptor = root_descriptor
    try:
        components = repo_path.split('/')
        for component in components[:-1]:
            next_descriptor = os.open(component,
                                      directory_flags,
                                      dir_fd=current_descriptor)
            if current_descriptor != root_descriptor:
                os.close(current_descriptor)
            current_descriptor = next_descriptor
        file_descriptor = -1
        try:
            file_descriptor = os.open(components[-1],
                                      read_flags,
                                      dir_fd=current_descriptor)
        except OSError as error:
            raise ProviderAuthorityInventoryV2Error(
                'Installed artifact is not a bounded regular file.') from error
        try:
            before = os.fstat(file_descriptor)
            if (not stat.S_ISREG(before.st_mode) or before.st_size < 1 or
                    before.st_size > maximum_bytes):
                raise ProviderAuthorityInventoryV2Error(
                    'Installed artifact is not a bounded regular file.')
            contents = bytearray()
            remaining = before.st_size
            while remaining:
                chunk = os.read(file_descriptor,
                                min(_READ_CHUNK_BYTES, remaining))
                if not chunk:
                    raise ProviderAuthorityInventoryV2Error(
                        'Installed artifact changed while read.')
                contents.extend(chunk)
                remaining -= len(chunk)
            if os.read(file_descriptor, 1):
                raise ProviderAuthorityInventoryV2Error(
                    'Installed artifact grew while read.')
            after = os.fstat(file_descriptor)
            stable = (before.st_dev, before.st_ino, before.st_size,
                      before.st_mtime_ns, before.st_ctime_ns)
            if stable != (after.st_dev, after.st_ino, after.st_size,
                          after.st_mtime_ns, after.st_ctime_ns):
                raise ProviderAuthorityInventoryV2Error(
                    'Installed artifact changed while read.')
            return bytes(contents)
        finally:
            os.close(file_descriptor)
    except OSError as error:
        raise ProviderAuthorityInventoryV2Error(
            'Installed artifact cannot be resolved safely.') from error
    finally:
        if current_descriptor != root_descriptor:
            os.close(current_descriptor)


def _read_installed_repo_path(repo_path: str, *, maximum_bytes: int) -> bytes:
    with _open_distribution_root_descriptor() as root_descriptor:
        return _read_installed_repo_path_from_root(root_descriptor,
                                                   repo_path,
                                                   maximum_bytes=maximum_bytes)


def _read_installed_artifact(
    reference: actions.ProviderRepoArtifactRefV1,) -> bytes:
    with _open_distribution_root_descriptor() as root_descriptor:
        return _read_installed_artifact_from_root(root_descriptor, reference)


def _read_installed_artifact_from_root(
    root_descriptor: int,
    reference: actions.ProviderRepoArtifactRefV1,
) -> bytes:
    if type(reference) is not actions.ProviderRepoArtifactRefV1:
        raise TypeError('Installed artifact reference has an invalid type.')
    if reference.byte_size > _MAX_STATIC_JSON_BYTES:
        raise ProviderAuthorityInventoryV2Error(
            'Installed artifact exceeds its fixed byte bound.')
    contents = _read_installed_repo_path_from_root(
        root_descriptor, reference.repo_path, maximum_bytes=reference.byte_size)
    if (len(contents) != reference.byte_size or
            hashlib.sha256(contents).hexdigest() != reference.sha256):
        raise ProviderAuthorityInventoryV2Error(
            'Installed artifact differs from its content address.')
    return contents


def _artifact_row(role: str,
                  repo_path: str,
                  contents: bytes,
                  *,
                  root_descriptor: int | None = None) -> dict[str, Any]:
    value = _parse_canonical_json_file(contents, name=f'{role} artifact')
    if type(value) is not dict:
        raise ProviderAuthorityInventoryV2Error(
            f'{role} artifact must be an exact JSON object.')
    expected_schemas = {
        'outer_template': 'skypilot.serve.prebooted-direct-pod.outer-template.v1',
        'node_fragment': 'skypilot.serve.prebooted-direct-pod.node-fragment.v1',
        'binding_schema': 'skypilot.serve.prebooted-direct-pod.bindings.v2',
        'config_access_inventory': 'skypilot.serve.prebooted-direct-pod.config-access-inventory.v2',
        'admitted_object_normalization': 'skypilot.kubernetes.admitted-object-normalization.v1',
    }
    expected_schema = expected_schemas.get(role)
    if expected_schema is not None and value.get('schema') != expected_schema:
        raise ProviderAuthorityInventoryV2Error(
            f'{role} artifact has an unsupported schema.')
    try:
        if role == 'binding_schema':
            renderer_v2.ProviderKubernetesBindingSchemaArtifactV2(value)
        elif role == 'config_access_inventory':
            renderer_v2.ProviderKubernetesConfigAccessInventoryV2(value)
        elif role == 'representability_case_inventory':
            inventory = load_provider_resource_action_representability_inventory_v2(
                contents, root_descriptor=root_descriptor)
            code_inventory = (
                representability_v2.
                PROVIDER_RESOURCE_ACTION_REPRESENTABILITY_CASE_INVENTORY_V2)
            if inventory.canonical_bytes != code_inventory.canonical_bytes:
                raise ProviderAuthorityInventoryV2Error(
                    'Representability case artifact differs from code dispatch.'
                )
            _validate_literal_representability_dispatch()
    except ProviderAuthorityInventoryV2Error:
        raise
    except (TypeError, ValueError) as error:
        raise ProviderAuthorityInventoryV2Error(
            f'{role} artifact contract is invalid.') from error
    return {
        'role': role,
        'repo_path': repo_path,
        'byte_size': len(contents),
        'sha256': hashlib.sha256(contents).hexdigest(),
    }


def load_provider_resource_action_representability_inventory_v2(
    index_contents: bytes,
    *,
    root_descriptor: int | None = None,
) -> representability_v2.ProviderResourceActionRepresentabilityCaseInventoryV2:
    """Load the fixed two-shard inventory through descriptor-safe reads."""

    if root_descriptor is None:
        with _open_distribution_root_descriptor() as opened_root:
            return load_provider_resource_action_representability_inventory_v2(
                index_contents, root_descriptor=opened_root)
    if type(root_descriptor) is not int or root_descriptor < 0:
        raise TypeError('Representability root descriptor is invalid.')

    value = _parse_canonical_json_file(
        index_contents, name='representability case inventory index')
    try:
        index = (representability_v2.
                 ProviderResourceActionRepresentabilityCaseInventoryIndexV2.
                 from_value(value))
    except (TypeError, ValueError) as error:
        raise ProviderAuthorityInventoryV2Error(
            'representability_case_inventory artifact contract is invalid.'
        ) from error

    cases: list[
        representability_v2.ProviderResourceActionRepresentabilityCaseV2] = []
    for descriptor in index.shards:
        contents = _read_installed_artifact_from_root(root_descriptor,
                                                      descriptor.artifact)
        shard_value = _parse_canonical_json_file(
            contents, name=f'representability case shard {descriptor.ordinal}')
        try:
            shard = (representability_v2.
                     ProviderResourceActionRepresentabilityCaseInventoryShardV2.
                     from_value(shard_value))
        except (TypeError, ValueError) as error:
            raise ProviderAuthorityInventoryV2Error(
                'Representability shard contract is invalid.') from error
        if (shard.ordinal != descriptor.ordinal or
                shard.cases[0].sequence != descriptor.first_case_sequence or
                shard.cases[-1].sequence != descriptor.last_case_sequence or
                len(shard.cases) != descriptor.case_count or
                len(shard.canonical_bytes) + 1
                != descriptor.artifact.byte_size):
            raise ProviderAuthorityInventoryV2Error(
                'Representability shard range differs from its descriptor.')
        cases.extend(shard.cases)

    try:
        return (representability_v2.
                ProviderResourceActionRepresentabilityCaseInventoryV2(
                    version=2,
                    contract=index.contract,
                    profile=index.profile,
                    cases=tuple(cases)))
    except (TypeError, ValueError) as error:
        raise ProviderAuthorityInventoryV2Error(
            'Representability shards do not form one closed inventory.'
        ) from error


def project_provider_authority_artifact_inventory_v2(
) -> actions.CanonicalJsonObject:
    """Project the exact six ordered roles from descriptor-read bytes."""

    rows = []
    with _open_distribution_root_descriptor() as root_descriptor:
        for role, repo_path in _ARTIFACT_ROLE_PATHS:
            contents = _read_installed_repo_path_from_root(
                root_descriptor,
                repo_path,
                maximum_bytes=_MAX_STATIC_JSON_BYTES)
            rows.append(
                _artifact_row(role,
                              repo_path,
                              contents,
                              root_descriptor=root_descriptor))
    return actions.CanonicalJsonObject({
        'version': 2,
        'contract': _ARTIFACT_INVENTORY_CONTRACT,
        'artifacts': rows,
    })


def validate_provider_authority_artifact_inventory_v2(
    contents: bytes,
    *,
    root_descriptor: int | None = None,
) -> tuple[actions.ProviderRepoArtifactRefV1, ...]:
    """Require one canonical six-role inventory to equal installed bytes."""

    if root_descriptor is None:
        with _open_distribution_root_descriptor() as opened_root:
            return validate_provider_authority_artifact_inventory_v2(
                contents, root_descriptor=opened_root)
    if type(root_descriptor) is not int or root_descriptor < 0:
        raise TypeError('Provider artifact root descriptor is invalid.')

    raw = _parse_canonical_json_file(contents,
                                     name='provider artifact inventory V2')
    value = _closed_dictionary(raw,
                               name='provider artifact inventory V2',
                               keys=frozenset(
                                   {'version', 'contract', 'artifacts'}))
    if (type(value['version']) is not int or value['version'] != 2 or
            value['contract'] != _ARTIFACT_INVENTORY_CONTRACT):
        raise ProviderAuthorityInventoryV2Error(
            'Provider artifact inventory V2 contract is unsupported.')
    rows = value['artifacts']
    if type(rows) is not list or len(rows) != len(_ARTIFACT_ROLE_PATHS):
        raise ProviderAuthorityInventoryV2Error(
            'Provider artifact inventory V2 must have exactly six roles.')
    references: list[actions.ProviderRepoArtifactRefV1] = []
    projected_rows = []
    for row, (expected_role, expected_path) in zip(rows, _ARTIFACT_ROLE_PATHS):
        parsed = _closed_dictionary(row,
                                    name='provider artifact inventory V2 row',
                                    keys=_INVENTORY_ARTIFACT_KEYS)
        if parsed['role'] != expected_role:
            raise ProviderAuthorityInventoryV2Error(
                'Provider artifact inventory V2 role order is invalid.')
        try:
            reference = actions.ProviderRepoArtifactRefV1.from_value({
                key: parsed[key] for key in ('repo_path', 'byte_size', 'sha256')
            })
        except (TypeError, ValueError) as error:
            raise ProviderAuthorityInventoryV2Error(
                'Provider artifact inventory V2 reference is invalid.'
            ) from error
        if reference.repo_path != expected_path:
            raise ProviderAuthorityInventoryV2Error(
                'Provider artifact inventory V2 role path is invalid.')
        installed = _read_installed_artifact_from_root(root_descriptor,
                                                       reference)
        projected_rows.append(
            _artifact_row(expected_role,
                          expected_path,
                          installed,
                          root_descriptor=root_descriptor))
        references.append(reference)
    if (len({row['role'] for row in rows}) != len(rows) or len(
        {reference.repo_path for reference in references}) != len(references)):
        raise ProviderAuthorityInventoryV2Error(
            'Provider artifact inventory V2 roles and paths must be distinct.')
    projected = actions.CanonicalJsonObject({
        'version': 2,
        'contract': _ARTIFACT_INVENTORY_CONTRACT,
        'artifacts': projected_rows,
    })
    if projected.canonical_bytes != contents[:-1]:
        raise ProviderAuthorityInventoryV2Error(
            'Provider artifact inventory V2 differs from installed bytes.')
    return tuple(references)


def _importable_callable_identity(callable_value: Any,
                                  *,
                                  mode: str | None = None) -> dict[str, str]:
    if type(callable_value) is not types.FunctionType:
        raise ProviderAuthorityInventoryV2Error(
            'Authority callable identity is not an exact Python function.')
    module = callable_value.__module__
    qualname = callable_value.__qualname__
    if (type(module) is not str or type(qualname) is not str or not module or
            not qualname or '<locals>' in qualname or '.' in qualname):
        raise ProviderAuthorityInventoryV2Error(
            'Authority callable identity is not stable and importable.')
    loaded_module = sys.modules.get(module)
    if type(loaded_module) is not types.ModuleType:
        raise ProviderAuthorityInventoryV2Error(
            'Authority callable module is not already loaded and exact.')
    resolved = vars(loaded_module).get(qualname)
    if resolved is not callable_value:
        raise ProviderAuthorityInventoryV2Error(
            'Authority callable identity resolves to a different object.')
    identity = {'module': module, 'qualname': qualname}
    if mode is not None:
        identity = {'mode': mode, **identity}
    return identity


def project_provider_authority_callable_inventory_v2(
) -> actions.CanonicalJsonObject:
    """Project the actual four handlers/codecs and four pure roots."""

    expected_names = actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1
    registered = request_registry.registered_handlers()
    if type(registered) is not tuple or any(
            type(registration) is not _HANDLER_REGISTRATION_TYPE
            for registration in registered):
        raise ProviderAuthorityInventoryV2Error(
            'Actual authority handler registry returned an open row type.')
    authority_registrations = tuple(
        registration for registration in registered if registration.claim_scope
        is request_registry.HandlerClaimScope.RESOURCE_ACTION_AUTHORITY)
    registrations_by_name = {
        registration.name: registration
        for registration in authority_registrations
    }
    if (len(authority_registrations) != len(expected_names) or
            set(registrations_by_name) != set(expected_names)):
        raise ProviderAuthorityInventoryV2Error(
            'Actual authority handler registry is not the closed four-name '
            'inventory.')
    handler_rows = []
    for handler_name in expected_names:
        try:
            registration = request_registry.resolve_handler(handler_name)
        except (TypeError, ValueError) as error:
            raise ProviderAuthorityInventoryV2Error(
                'Authority handler cannot be resolved from the registry.'
            ) from error
        if registration is not registrations_by_name[handler_name]:
            raise ProviderAuthorityInventoryV2Error(
                'Authority handler resolution differs from the actual '
                'runtime registry inventory.')
        handler_identity = _importable_callable_identity(registration.func)
        full_request_name = server_constants.REQUEST_NAME_PREFIX + handler_name
        encoder = encoders.get_encoder(full_request_name)
        decoder = decoders.get_decoder(full_request_name)
        handler_rows.append({
            'name': registration.name,
            **handler_identity,
            'execution_class': registration.execution_class.value,
            'claim_scope': registration.claim_scope.value,
            'replay_policy': registration.replay_policy.value,
            'cancellation_policy': registration.cancellation_policy.value,
            'aliases': list(registration.aliases),
            'result_codec': {
                'encoder': _importable_callable_identity(
                    encoder,
                    mode=('default' if encoder is encoders.default_encoder else
                          'registered')),
                'decoder': _importable_callable_identity(
                    decoder,
                    mode=('default'
                          if decoder is decoders.default_decode_handler else
                          'registered')),
                'strict_return_value':
                    encoders.requires_strict_return_value(full_request_name),
            },
        })
    pure_rows = []
    _validate_literal_representability_dispatch()
    for role, expected_module, expected_qualname, entrypoint in _PURE_ENTRYPOINTS:
        identity = _importable_callable_identity(entrypoint)
        if identity != {
                'module': expected_module,
                'qualname': expected_qualname,
        }:
            raise ProviderAuthorityInventoryV2Error(
                'Authority pure-root literal identity drifted.')
        pure_rows.append({
            'role': role,
            **identity,
        })
    return actions.CanonicalJsonObject({
        'version': 2,
        'contract': _CALLABLE_INVENTORY_CONTRACT,
        'handlers': handler_rows,
        'pure_entrypoints': pure_rows,
    })


def _validate_text_fields(value: dict[str, Any], fields: tuple[str, ...], *,
                          name: str) -> None:
    if any(
            type(value[field]) is not str or not value[field]
            for field in fields):
        raise ProviderAuthorityInventoryV2Error(
            f'{name} text identity is invalid.')


def validate_provider_authority_callable_inventory_v2(contents: bytes) -> None:
    """Require canonical bytes equal the actual registry/import projection."""

    raw = _parse_canonical_json_file(contents,
                                     name='provider callable inventory V2')
    value = _closed_dictionary(
        raw,
        name='provider callable inventory V2',
        keys=frozenset({'version', 'contract', 'handlers', 'pure_entrypoints'}))
    if (type(value['version']) is not int or value['version'] != 2 or
            value['contract'] != _CALLABLE_INVENTORY_CONTRACT):
        raise ProviderAuthorityInventoryV2Error(
            'Provider callable inventory V2 contract is unsupported.')
    handlers = value['handlers']
    expected_names = list(
        actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1)
    if type(handlers) is not list or len(handlers) != len(expected_names):
        raise ProviderAuthorityInventoryV2Error(
            'Provider callable inventory V2 must have exactly four handlers.')
    for row, expected_name in zip(handlers, expected_names):
        parsed = _closed_dictionary(row,
                                    name='provider callable V2 handler',
                                    keys=_CALLABLE_HANDLER_KEYS)
        if parsed['name'] != expected_name:
            raise ProviderAuthorityInventoryV2Error(
                'Provider callable inventory V2 handler order is invalid.')
        _validate_text_fields(
            parsed, ('name', 'module', 'qualname', 'execution_class',
                     'claim_scope', 'replay_policy', 'cancellation_policy'),
            name='provider callable V2 handler')
        aliases = parsed['aliases']
        if type(aliases) is not list or any(
                type(alias) is not str or not alias for alias in aliases):
            raise ProviderAuthorityInventoryV2Error(
                'Provider callable inventory V2 aliases are invalid.')
        result_codec = _closed_dictionary(
            parsed['result_codec'],
            name='provider callable V2 result codec',
            keys=_RESULT_CODEC_KEYS)
        if type(result_codec['strict_return_value']) is not bool:
            raise ProviderAuthorityInventoryV2Error(
                'Provider callable inventory V2 strict codec flag is invalid.')
        for field in ('encoder', 'decoder'):
            codec = _closed_dictionary(result_codec[field],
                                       name=f'provider callable V2 {field}',
                                       keys=_CODEC_IDENTITY_KEYS)
            _validate_text_fields(codec, ('mode', 'module', 'qualname'),
                                  name=f'provider callable V2 {field}')
            if codec['mode'] not in ('default', 'registered'):
                raise ProviderAuthorityInventoryV2Error(
                    'Provider callable inventory V2 codec mode is invalid.')
    pure_rows = value['pure_entrypoints']
    if type(pure_rows) is not list or len(pure_rows) != len(_PURE_ENTRYPOINTS):
        raise ProviderAuthorityInventoryV2Error(
            'Provider callable inventory V2 must have exactly four pure roots.')
    for row, (expected_role, _, _, _) in zip(pure_rows, _PURE_ENTRYPOINTS):
        parsed = _closed_dictionary(row,
                                    name='provider callable V2 pure root',
                                    keys=_PURE_ENTRYPOINT_KEYS)
        _validate_text_fields(parsed, ('role', 'module', 'qualname'),
                              name='provider callable V2 pure root')
        if parsed['role'] != expected_role:
            raise ProviderAuthorityInventoryV2Error(
                'Provider callable inventory V2 pure-root order is invalid.')
    if (len({row['name'] for row in handlers}) != len(handlers) or
            len({row['role'] for row in pure_rows}) != len(pure_rows)):
        raise ProviderAuthorityInventoryV2Error(
            'Provider callable inventory V2 identities must be distinct.')
    projected = project_provider_authority_callable_inventory_v2()
    if projected.canonical_bytes != contents[:-1]:
        raise ProviderAuthorityInventoryV2Error(
            'Provider callable inventory V2 differs from actual callables.')


def validate_installed_provider_authority_inventories_v2(
    artifact_inventory: actions.ProviderRepoArtifactRefV1,
    callable_inventory: actions.ProviderRepoArtifactRefV1,
) -> tuple[actions.ProviderRepoArtifactRefV1, ...]:
    """Descriptor-read and validate both cohort-bound V2 inventories."""

    if (type(artifact_inventory) is not actions.ProviderRepoArtifactRefV1 or
            type(callable_inventory) is not actions.ProviderRepoArtifactRefV1):
        raise TypeError('Installed V2 inventory reference has an invalid type.')
    if artifact_inventory.repo_path != _ARTIFACT_INVENTORY_REPO_PATH:
        raise ProviderAuthorityInventoryV2Error(
            'Installed V2 artifact inventory path is invalid.')
    if callable_inventory.repo_path != _CALLABLE_INVENTORY_REPO_PATH:
        raise ProviderAuthorityInventoryV2Error(
            'Installed V2 callable inventory path is invalid.')
    if artifact_inventory.repo_path == callable_inventory.repo_path:
        raise ProviderAuthorityInventoryV2Error(
            'Installed V2 inventory paths must be distinct.')
    with _open_distribution_root_descriptor() as root_descriptor:
        artifact_contents = _read_installed_artifact_from_root(
            root_descriptor, artifact_inventory)
        callable_contents = _read_installed_artifact_from_root(
            root_descriptor, callable_inventory)
        references = validate_provider_authority_artifact_inventory_v2(
            artifact_contents, root_descriptor=root_descriptor)
    validate_provider_authority_callable_inventory_v2(callable_contents)
    return references


__all__ = [
    'ProviderAuthorityInventoryV2Error',
    'project_provider_authority_artifact_inventory_v2',
    'project_provider_authority_callable_inventory_v2',
    'validate_installed_provider_authority_inventories_v2',
    'validate_provider_authority_artifact_inventory_v2',
    'validate_provider_authority_callable_inventory_v2',
]
